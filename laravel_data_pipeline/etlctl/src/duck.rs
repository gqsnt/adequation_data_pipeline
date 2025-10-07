// src/duck.rs
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ffi::OsString;

fn sanitize_ident(s: &str) -> String {
    // ident DuckDB simple: alnum + underscore
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' { out.push(ch); }
        else { out.push('_'); }
    }
    if out.is_empty() { "_".into() } else { out }
}

fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

fn duckdb_bin() -> OsString {
    std::env::var_os("DUCKDB_BIN").unwrap_or_else(|| OsString::from("duckdb"))
}

fn run_duckdb_sql(db_path: &Path, sql: &str) -> anyhow::Result<()> {
    let bin = duckdb_bin();

    // Use `-c "<sql>"` so the CLI executes and exits.
    // We capture stdout/stderr to surface errors nicely.
    let output = Command::new(bin)
        .arg(db_path.as_os_str())
        .arg("-c")
        .arg(sql)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!(
            "duckdb cli failed (code {:?})\nSTDERR:\n{}\nSTDOUT:\n{}",
            output.status.code(),
            stderr,
            stdout
        );
    }
    Ok(())
}

/// Enregistre/rafraîchit une vue DuckDB qui pointe vers un glob Parquet.
/// - base_dir: répertoire où stocker le .duckdb (ex: /data/duckdb)
/// - schema: généralement ton `namespace`
/// - view_name: nom logique (ex: "gold_tx" ou "silver")
/// - parquet_glob: `/warehouse/{namespace}/{dataset}/data/*.parquet`
///
/// Requiert que la commande `duckdb` soit installée dans l'image (ou DUCKDB_BIN pointant vers le binaire).
pub fn register_parquet_view(
    base_dir: &str,
    schema: &str,
    view_name: &str,
    parquet_glob: &Path,
) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(base_dir)?;
    let db_path = Path::new(base_dir).join(format!("{}.duckdb", sanitize_ident(schema)));

    let schema_ident = sanitize_ident(schema);
    let view_ident = sanitize_ident(view_name);
    let glob_sql = escape_single_quotes(&parquet_glob.to_string_lossy());

    // DuckDB 1.x sait lire Parquet nativement via `read_parquet`.
    // On crée le schéma s’il n’existe pas, puis on (re)crée la vue.
    let sql = format!(
        r#"
        CREATE SCHEMA IF NOT EXISTS "{schema}";
        CREATE OR REPLACE VIEW "{schema}"."{view}" AS
        SELECT * FROM read_parquet('{glob}');
        "#,
        schema = schema_ident,
        view = view_ident,
        glob = glob_sql,
    );

    run_duckdb_sql(&db_path, &sql)?;
    Ok(db_path)
}
