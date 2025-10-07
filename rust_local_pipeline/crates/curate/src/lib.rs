use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef, Int16Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::Schema;
use arrow::ipc::reader::FileReader as IpcReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct CurateStats {
    pub files_written: u32,
    pub rows_written: u64,
    pub snapshot_dir: PathBuf,
    pub commit_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CurateConfig {
    pub slug: String,
    pub ingest_date: String,
    pub snapshot_date: String,
    pub storage_root: PathBuf,
    pub silver_dir: String,
    pub gold_dir: String,
    pub manifests_dir: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitFile {
    path: String,
    rows: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommitJson {
    dataset: String,
    snapshot_date: String,
    files: Vec<CommitFile>,
}

pub async fn write_gold_snapshot(cfg: CurateConfig) -> Result<CurateStats> {
    // Locate Silver IPC
    let silver_path = cfg
        .storage_root
        .join(&cfg.silver_dir)
        .join(&cfg.slug)
        .join(format!("ingest_date={}", cfg.ingest_date))
        .join("part-000000.arrow");
    if !silver_path.exists() {
        return Err(anyhow!("Silver file not found: {}", silver_path.display()));
    }

    // Prepare snapshot output dir
    let snapshot_dir = cfg
        .storage_root
        .join(&cfg.gold_dir)
        .join(&cfg.slug)
        .join(format!("snapshot_date={}", cfg.snapshot_date));
    create_dir_all(&snapshot_dir)
        .with_context(|| format!("mkdir -p {}", snapshot_dir.display()))?;

    // Read Silver IPC
    let f = File::open(&silver_path).with_context(|| format!("open {}", silver_path.display()))?;
    let reader = IpcReader::try_new(f, None)?;
    let schema: Arc<Schema> = reader.schema();

    // Column indexes needed for partitioning
    let year_idx = schema
        .index_of("year_mutation")
        .context("missing 'year_mutation' in Silver")?;
    let dept_idx = schema
        .index_of("code_departement")
        .context("missing 'code_departement' in Silver")?;

    // Partition buffers: key -> Vec<RecordBatch>
    type Key = (i16, String);
    let mut parts: HashMap<Key, Vec<RecordBatch>> = HashMap::new();

    for maybe_batch in reader {
        let batch = maybe_batch?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        let year_arr = batch
            .column(year_idx)
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| anyhow!("year_mutation must be Int16"))?;
        let dept_arr = batch
            .column(dept_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("code_departement must be Utf8"))?;

        // Build index vectors per partition in this batch
        let mut idx_map: HashMap<Key, Vec<u32>> = HashMap::new();
        for row in 0..n {
            let year = year_arr.value(row);
            let dept = if dept_arr.is_null(row) || dept_arr.value(row).trim().is_empty() {
                "UNK".to_string()
            } else {
                dept_arr.value(row).trim().to_string()
            };
            idx_map.entry((year, dept)).or_default().push(row as u32);
        }

        // For each key, slice the batch by indices and push to partition buffers
        for (key, rows) in idx_map.into_iter() {
            let indices = UInt32Array::from(rows);
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                let arr = batch.column(c);
                let taken = take(arr.as_ref(), &indices, None)
                    .with_context(|| "arrow::compute::take failed")?;
                cols.push(taken);
            }
            let part_batch = RecordBatch::try_new(schema.clone(), cols)?;
            parts.entry(key).or_default().push(part_batch);
        }
    }

    // Writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_dictionary_enabled(true)
        .build();

    // Write one Parquet file per partition
    let mut files: Vec<CommitFile> = Vec::new();
    let mut files_written = 0u32;
    let mut rows_written = 0u64;

    for ((year, dept), batches) in parts.into_iter() {
        // directory: .../year=YYYY/dept=CC/
        let dir = snapshot_dir
            .join(format!("year={}", year))
            .join(format!("dept={}", dept));
        create_dir_all(&dir)?;
        let file_path = dir.join("part-000000.parquet");
        let mut file =
            File::create(&file_path).with_context(|| format!("create {}", file_path.display()))?;
        let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), Some(props.clone()))?;

        let mut part_rows = 0u64;
        for b in batches {
            part_rows += b.num_rows() as u64;
            writer.write(&b)?;
        }
        writer.close()?;
        // Ensure flushed to disk
        file.flush()?;

        files.push(CommitFile {
            path: file_path.to_string_lossy().to_string(),
            rows: part_rows,
        });
        files_written += 1;
        rows_written += part_rows;
    }

    // Write commit.json
    let commit_dir = cfg
        .storage_root
        .join(&cfg.manifests_dir)
        .join(&cfg.slug)
        .join(format!("snapshot_date={}", cfg.snapshot_date));
    create_dir_all(&commit_dir)?;
    let commit_path = commit_dir.join("commit.json");
    let commit = CommitJson {
        dataset: cfg.slug.clone(),
        snapshot_date: cfg.snapshot_date.clone(),
        files,
    };
    {
        let mut out = File::create(&commit_path)?;
        let json = serde_json::to_string_pretty(&commit)?;
        out.write_all(json.as_bytes())?;
        out.flush()?;
    }

    // Update latest.json atomically
    let latest_dir = cfg.storage_root.join(&cfg.manifests_dir).join(&cfg.slug);
    create_dir_all(&latest_dir)?;
    let latest_tmp = latest_dir.join("latest.json.tmp");
    let latest_path = latest_dir.join("latest.json");
    {
        let mut out = File::create(&latest_tmp)?;
        let json = format!(r#"{{"snapshot_date":"{}"}}"#, cfg.snapshot_date);
        out.write_all(json.as_bytes())?;
        out.flush()?;
    }
    // best-effort atomic replace
    std::fs::rename(&latest_tmp, &latest_path)
        .or_else(|_| std::fs::copy(&latest_tmp, &latest_path).map(|_| ()))?;

    Ok(CurateStats {
        files_written,
        rows_written,
        snapshot_dir,
        commit_path,
    })
}
