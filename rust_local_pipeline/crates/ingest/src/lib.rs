use anyhow::{anyhow, Context, Result};
use arrow::ipc::writer::FileWriter;
use arrow::datatypes::{DataType, Field, Schema};
use csv_async::{AsyncReaderBuilder, StringRecord};
use futures::StreamExt;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array, Int64Builder, RecordBatch, StringArray, StringBuilder};
use tokio::io::BufReader;

#[derive(Debug, Clone)]
pub enum SourceKind {
    LocalFile,
}

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub slug: String,
    pub source: SourceKind,
    pub ingest_date: String, // "YYYY-MM-DD"
    pub storage_root: PathBuf, // e.g., "./data"
    pub bronze_dir: String,    // e.g., "bronze"
}

#[derive(Debug, Default, Clone)]
pub struct IngestStats {
    pub rows_in: u64,
    pub bytes_in: u64,
    pub parts_written: u32,
    pub out_path: PathBuf,
}

const BATCH_SIZE: usize = 65_536;

pub async fn ingest_dataset(cfg: IngestConfig, source_path: &Path) -> Result<IngestStats> {
    if !source_path.exists() {
        return Err(anyhow!("source file not found: {}", source_path.display()));
    }

    let out_dir = cfg
        .storage_root
        .join(&cfg.bronze_dir)
        .join(&cfg.slug)
        .join(format!("ingest_date={}", cfg.ingest_date));
    create_dir_all(&out_dir).with_context(|| format!("mkdir -p {}", out_dir.display()))?;

    let out_path = out_dir.join("part-000000.arrow");

    let f = tokio::fs::File::open(source_path).await?;
    let meta = f.metadata().await?;
    let bytes_in = meta.len() as u64;
    let mut rdr = AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .flexible(true)
        .create_reader(BufReader::new(f));

    // Read header (column names)
    let headers: StringRecord = rdr.headers().await?.clone();
    if headers.is_empty() {
        return Err(anyhow!("empty CSV header in {}", source_path.display()));
    }
    let src_col_count = headers.len();

    // Build Arrow schema: all source columns Utf8 (nullable) + lineage
    let mut fields: Vec<Field> = headers
        .iter()
        .map(|name| Field::new(name.trim(), DataType::Utf8, true))
        .collect();
    fields.push(Field::new("ingest_date", DataType::Utf8, false));
    fields.push(Field::new("source_file", DataType::Utf8, false));
    fields.push(Field::new("row_number", DataType::Int64, false));
    let schema = Arc::new(Schema::new(fields));

    // IPC writer (sync file)
    let out_file = File::create(&out_path)
        .with_context(|| format!("create {}", out_path.display()))?;
    let mut writer = FileWriter::try_new(out_file, &schema)?;

    // Batch builders
    let mut col_builders: Vec<StringBuilder> =
        (0..src_col_count).map(|_| StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8)).collect();
    let mut ingest_date_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12);
    let mut source_file_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 32);
    let mut rownum_builder = Int64Builder::with_capacity(BATCH_SIZE);

    let mut rows_in: u64 = 0;
    let source_file_str = source_path.display().to_string();

    let mut records = rdr.records();

    while let Some(rec_res) = records.next().await {
        let rec = rec_res?;
        rows_in += 1;

        for (i, bldr) in col_builders.iter_mut().enumerate() {
            let val_opt = rec.get(i).map(|s| s.trim());
            if let Some(v) = val_opt {
                if v.is_empty() {
                    bldr.append_null();
                } else {
                    bldr.append_value(v);
                }
            } else {
                bldr.append_null();
            }
        }

        ingest_date_builder.append_value(&cfg.ingest_date);
        source_file_builder.append_value(&source_file_str);
        rownum_builder.append_value(rows_in as i64);

        // Flush when batch full
        if (rows_in as usize) % BATCH_SIZE == 0 {
            write_batch(
                &schema,
                &mut writer,
                &mut col_builders,
                &mut ingest_date_builder,
                &mut source_file_builder,
                &mut rownum_builder,
            )?;
        }
    }

    if (rows_in as usize) % BATCH_SIZE != 0 {
        write_batch(
            &schema,
            &mut writer,
            &mut col_builders,
            &mut ingest_date_builder,
            &mut source_file_builder,
            &mut rownum_builder,
        )?;
    }

    writer.finish()?;

    Ok(IngestStats {
        rows_in,
        bytes_in,
        parts_written: 1,
        out_path,
    })
}

#[allow(clippy::too_many_arguments)]
fn write_batch(
    schema: &Arc<Schema>,
    writer: &mut FileWriter<File>,
    col_builders: &mut [StringBuilder],
    ingest_date_builder: &mut StringBuilder,
    source_file_builder: &mut StringBuilder,
    rownum_builder: &mut Int64Builder,
) -> Result<()> {
    // Build Arrow arrays for this batch
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for b in col_builders.iter_mut() {
        let arr: StringArray = b.finish();
        cols.push(Arc::new(arr) as ArrayRef);
    }
    let ingest_arr: StringArray = ingest_date_builder.finish();
    let source_arr: StringArray = source_file_builder.finish();
    let rownum_arr: Int64Array = rownum_builder.finish();
    cols.push(Arc::new(ingest_arr) as ArrayRef);
    cols.push(Arc::new(source_arr) as ArrayRef);
    cols.push(Arc::new(rownum_arr) as ArrayRef);

    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    writer.write(&batch)?;

    for b in col_builders.iter_mut() {
        *b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 8);
    }
    *ingest_date_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 12);
    *source_file_builder = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 32);
    *rownum_builder = Int64Builder::with_capacity(BATCH_SIZE);

    Ok(())
}
