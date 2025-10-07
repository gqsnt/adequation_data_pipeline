use anyhow::Result;
use std::path::PathBuf;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "pipeline", version, about = "Local DVF pipeline CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// CSV -> Bronze IPC
    Ingest {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        source: PathBuf,
        #[arg(long, value_name = "YYYY-MM-DD")]
        ingest_date: String,
        #[arg(long, default_value = "./data")]
        root: PathBuf,
    },
    /// Bronze IPC -> Silver IPC (+ Rejects)
    Validate {
        #[arg(long)]
        dataset: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        ingest_date: String,
        #[arg(long, default_value = "./data")]
        root: PathBuf,
    },
    /// Silver IPC -> Gold Parquet + manifests
    Curate {
        #[arg(long)]
        dataset: String,
        /// Which Silver ingest to use
        #[arg(long, value_name = "YYYY-MM-DD")]
        ingest_date: String,
        /// Where to place the Gold snapshot
        #[arg(long, value_name = "YYYY-MM-DD")]
        snapshot_date: String,
        #[arg(long, default_value = "./data")]
        root: PathBuf,
    },
    /// Update a DuckDB catalog file with views that point to the latest Gold snapshot.
    DuckdbRefresh {
        #[arg(long)]
        dataset: String,
        /// Optionally force a specific snapshot date (YYYY-MM-DD). Defaults to manifests/<slug>/latest.json
        #[arg(long)]
        snapshot_date: Option<String>,
        /// Path to .duckdb file used by Metabase.
        #[arg(long, default_value = "./metabase/warehouse.duckdb")]
        db: PathBuf,
        /// Root data directory (where gold/ and manifests/ live).
        #[arg(long, default_value = "./data")]
        root: PathBuf,
    },

}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let start = std::time::Instant::now();
    match cli.cmd {
        Commands::Ingest { dataset, source, ingest_date, root } => {
            let cfg = ingest::IngestConfig {
                slug: dataset,
                source: ingest::SourceKind::LocalFile,
                ingest_date,
                storage_root: root,
                bronze_dir: "bronze".to_string(),
            };
            let stats = ingest::ingest_dataset(cfg, &source.as_path()).await?;
            println!(
                "INGEST OK rows_in={} bytes_in={} out={}",
                stats.rows_in, stats.bytes_in, stats.out_path.display()
            );
        }
        Commands::Validate { dataset, ingest_date, root } => {
            let cfg = validate::ValidateConfig {
                slug: dataset,
                ingest_date,
                storage_root: root,
                bronze_dir: "bronze".to_string(),
                silver_dir: "silver".to_string(),
                rejects_dir: "rejects".to_string(),
            };
            let st = validate::validate_dataset(cfg).await?;
            println!(
                "VALIDATE OK rows_in={} rows_out={} rejects={} silver={} rejects={}",
                st.rows_in, st.rows_out, st.rejects, st.silver_out.display(), st.rejects_out.display()
            );
        }
        Commands::Curate { dataset, ingest_date, snapshot_date, root } => {
            let cfg = curate::CurateConfig {
                slug: dataset,
                ingest_date,
                snapshot_date,
                storage_root: root,
                silver_dir: "silver".to_string(),
                gold_dir: "gold".to_string(),
                manifests_dir: "manifests".to_string(),
            };
            let st = curate::write_gold_snapshot(cfg).await?;
            println!(
                "CURATE OK files_written={} rows_written={} snapshot_dir={} commit={}",
                st.files_written,
                st.rows_written,
                st.snapshot_dir.display(),
                st.commit_path.display()
            );
        }
        Commands::DuckdbRefresh { dataset, snapshot_date, db, root } => {
            let cfg = duckdb_catalog::RefreshCfg {
                slug: dataset,
                storage_root: root,
                manifests_dir: "manifests".to_string(),
                gold_dir: "gold".to_string(),
                snapshot_date,
                duckdb_path: db,
            };
            duckdb_catalog::refresh_duckdb(cfg)?;
            println!("DUCKDB REFRESH OK");
        }
    }
    let duration_pretty = humantime::format_duration(start.elapsed());
    println!("DONE in {}", duration_pretty);
    Ok(())
}
