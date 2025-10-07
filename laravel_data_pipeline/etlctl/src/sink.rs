use chrono::Utc;
use percent_encoding::percent_decode_str;
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use crate::types::ArrowLikeSchema;
pub(crate) use crate::util::{has_any_parquet, polars_dtype_from_str};

/// Résout le chemin **local** d'un URI file://
pub fn local_path_from_file_uri(uri: &str) -> anyhow::Result<PathBuf> {
    let uri = uri.strip_prefix("file://").unwrap_or(uri);
    println!("local_path_from_file_uri: {}", uri);
    // on accepte les espaces encodés
    let decoded = percent_decode_str(uri).decode_utf8()?;
    Ok(PathBuf::from(decoded.as_ref()))
}

/// Racine d'une "table" sur disque: {warehouse}/{namespace}/{name}
pub fn table_root(warehouse_uri: &str, namespace: &str, name: &str) -> anyhow::Result<PathBuf> {
    let mut root = local_path_from_file_uri(warehouse_uri)?;
    root.push(namespace);
    root.push(name);
    Ok(root)
}

/// Assure {root}/data existe
pub fn ensure_table_dirs(root: &Path) -> anyhow::Result<PathBuf> {
    let data = root.join("data");
    fs::create_dir_all(&data)?;
    Ok(data)
}

/// Ecrit un DataFrame en Parquet, retourne un snapshot_id horodaté
pub fn write_parquet_snapshot(mut df: DataFrame, table_root: &Path) -> anyhow::Result<(PathBuf, String)> {
    println!("Table root: {:?}", table_root);
    println!("DataFrame shape: {:?}", df.shape());
    let data_dir = ensure_table_dirs(table_root)?;
    let ts = Utc::now().format("%Y%m%dT%H%M%S%3fZ").to_string();
    let file = data_dir.canonicalize()?.join(format!("part-{}.parquet", ts));
    let f = std::fs::File::create(&file)?;
    println!("Writing Parquet snapshot to {:?}", file);
    let writer = ParquetWriter::new(f)
        .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(3)?)))
        .with_statistics(StatisticsOptions {
        min_value: true,
        max_value: true,
        distinct_count: false,
        null_count: true,
    });
    writer.finish(&mut df)?;
    Ok((file, ts))
}


/// Lit tous les Parquet d'une table en LazyFrame
pub fn scan_parquet_table_sync(table_root: &Path) -> anyhow::Result<LazyFrame> {
    let data = table_root.join("data");
    if !data.exists() {
        anyhow::bail!("table has no data dir: {:?}", data);
    }
    if !has_any_parquet(&data)? {
        anyhow::bail!("table data dir empty: {:?}", data); // cas contrôlé
    }
    let glob = data.join("*.parquet");
    LazyFrame::scan_parquet(
        PlPath::new(glob.to_string_lossy().as_ref()),
        Default::default(),
    )
        .map_err(|e| anyhow::anyhow!(e.to_string()))
}


pub fn empty_lazyframe_with_schema(schema: &ArrowLikeSchema) -> anyhow::Result<LazyFrame> {
    use polars::prelude::*;
    let mut cols: Vec<Column> = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let dt = polars_dtype_from_str(&f.r#type)?;
        // 0 lignes, dtype imposé
        let  name = PlSmallStr::from_str(&f.name);
        cols.push(Column::full_null(name, 0, &dt));
    }
    let df = DataFrame::new(cols)?;
    Ok(df.lazy())
}

/// Scan parquet, ou retourne un LF vide typé si aucun fichier n'est présent.
pub fn scan_parquet_table_or_empty(table_root: &Path, schema: &ArrowLikeSchema) -> anyhow::Result<LazyFrame> {
    use polars::prelude::*;
    let data_dir = table_root.join("data");
    if !data_dir.exists() {
        return empty_lazyframe_with_schema(schema);
    }
    let has_parquet = std::fs::read_dir(&data_dir)
        .map(|it| it.filter_map(Result::ok).any(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet")))
        .unwrap_or(false);
    if !has_parquet {
        return empty_lazyframe_with_schema(schema);
    }
    let glob = data_dir.join("*.parquet");
    LazyFrame::scan_parquet(
        PlPath::new(glob.to_string_lossy().as_ref()),
        Default::default(),
    )
        .map_err(|e| anyhow::anyhow!(e.to_string()))
}