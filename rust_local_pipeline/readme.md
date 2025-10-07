# DVF Local Pipeline (Rust · Arrow · Parquet · DuckDB)

A small, local pipeline to process the **DVF** CSV using a simple **Bronze → Silver → Gold** flow and a single CLI.

* **Ingest**: CSV → **Bronze** (Arrow IPC)
* **Validate**: Bronze → **Silver** (typed; **dedup** + rejects)
* **Curate**: Silver → **Gold** (partitioned Parquet + manifests)
* **DuckDB Refresh**: creates views over the latest Gold snapshot

---

## Requirements

* Rust toolchain
* DuckDB CLI in `PATH` (for `duckdb-refresh`)
* CSV at `samples/dvf.csv`

Optional:

* Docker (for the provided Metabase image)

---

## Folder Basics

```
samples/dvf.csv     # input CSV
crates/cli          # CLI entrypoint (binary: `pipeline`)
crates/ingest       # CSV → Bronze IPC
crates/validate     # Bronze → Silver IPC (+ rejects, dedup)
crates/curate       # Silver → Gold Parquet (+ manifests)
crates/duckdb-catalog  # DuckDB views over Gold
data/               # output root (created by the CLI)
```

---

## Quick Start (4 commands)

From the repo root (adjust dates if needed):

```bash
# 1) CSV -> Bronze
cargo run --release -p cli -- ingest \
  --dataset dvf \
  --source ./samples/dvf.csv \
  --ingest-date 2025-10-02 \
  --root ./data

# 2) Bronze -> Silver (typed, dedup, rejects)
cargo run --release -p cli -- validate \
  --dataset dvf \
  --ingest-date 2025-10-02 \
  --root ./data

# 3) Silver -> Gold (Parquet + manifests)
cargo run --release -p cli -- curate \
  --dataset dvf \
  --ingest-date 2025-10-02 \
  --snapshot-date 2025-10-02 \
  --root ./data

# 4) Create/refresh DuckDB views over latest Gold
cargo run -p cli -- duckdb-refresh \
  --dataset dvf \
  --root ./data \
  --db ./metabase/warehouse.duckdb
```

> Tip: the same commands are in `run.sh`.

---

## What Each Step Does (in one line)

* **Ingest**: reads CSV (comma, header) → Arrow IPC + lineage columns.
* **Validate**: enforces types and rules, **deduplicates** using a BLAKE3 key, writes **Silver** + **Rejects**.
* **Curate**: writes **Parquet** partitioned by `year_mutation` and `code_departement`, updates manifests.
* **DuckDB Refresh**: (re)creates convenient views (e.g., `gold.dvf_latest`, `gold.dvf_price_metrics_yoy`).

---

## Outputs (where to look)

```
data/
├─ bronze/dvf/ingest_date=YYYY-MM-DD/part-000000.arrow
├─ silver/dvf/ingest_date=YYYY-MM-DD/part-000000.arrow
├─ rejects/dvf/ingest_date=YYYY-MM-DD/part-000000.arrow
├─ gold/dvf/snapshot_date=YYYY-MM-DD/year=YYYY/dept=CC/part-000000.parquet
└─ manifests/dvf/
   ├─ snapshot_date=YYYY-MM-DD/commit.json
   └─ latest.json
```

---

## Optional: Metabase (DuckDB)

Build and run the provided image:

```bash
docker build -t dvf-metabase -f DockerFile .
docker run --rm -p 3000:3000 \
  -v "$PWD/metabase":/home/metabase \
  -v "$PWD/data":/home/data \
  dvf-metabase
```

Point Metabase to `./metabase/warehouse.duckdb` (or `/home/metabase/warehouse.duckdb` inside the container).

---

## Troubleshooting (quick)

* **DuckDB not found** → install DuckDB CLI or add it to `PATH`.
* **No Gold data** → ensure `curate` ran and `manifests/dvf/latest.json` exists.
* **Empty views** → re-run `duckdb-refresh` after a successful `curate`.
