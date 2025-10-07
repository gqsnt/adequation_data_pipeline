# DVF Backend Bench (Rust + Laravel)

Minimal, reproducible benchmark of two backends that ingest and query the **DVF** dataset from a shared CSV:

* **Rust**: Axum + Polars (CSV → Parquet partitioned by `year/dep`) + DuckDB for querying.
* **Laravel**: PHP/Laravel + Eloquent (CSV → PostgreSQL via ORM-only bulk insert).
* **Endpoints are identical on both stacks** (no `/api` prefix in Laravel).

---

## Repository Layout

```
/
├─ samples/
│  └─ dvf.csv                 # shared input CSV (place file here) https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees
├─ rust/
│  └─ src/main.rs             # Axum + Polars + DuckDB service
└─ laravel/
   ├─ app/Http/Controllers/DvfController.php
   ├─ database/migrations/*_create_dvfs_table.php
   └─ routes/web.php          # routes without /api prefix
```

---

## CSV Location (single source of truth)

* The CSV lives at **`samples/dvf.csv`**.
* **Laravel** resolves the path with: `base_path('samples/dvf.csv')`. 
* **Rust** should read from the same file; set `CSV_PATH` in `rust/src/main.rs` to:

  ```rust
  const CSV_PATH: &str = "../samples/dvf.csv"; // path relative to rust/ directory
  ```

---

## Laravel (ORM-only path)

### Setup

```bash
cd laravel
composer i
cp .env.example .env
# Configure DB_* (PostgreSQL recommended)
php artisan key:generate
php artisan migrate
```

### Run

```bash
php artisan serve --host=0.0.0.0 --port=8000
```

### Ingest (benchmark ~10000k rows)

```bash
curl "http://localhost:8000/dvf/ingest?n=100000"
```

### Example queries (same shape as Rust)

```bash
curl "http://localhost:8000/dvf/filter?dep=75&from=2023-01-01&to=2023-12-31"
curl "http://localhost:8000/dvf/agg-commune?dep=75&year=2023"
curl "http://localhost:8000/dvf/topn?dep=75&year=2023&n=20"
curl "http://localhost:8000/dvf/geo-bbox?min_lon=2.20&min_lat=48.80&max_lon=2.42&max_lat=48.92&year=2023"
```

**Notes**

* Ingestion uses **Eloquent bulk `insert()`** with batching (no `COPY`, no jobs/queues).
* Default batch size in controller: **5,000** rows/transaction.

---

## Rust (Polars + Parquet + DuckDB)

### Build & Run

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh # if Rust not installed
cd rust
cargo build --release
./target/release/<binary-name>   # listens on 0.0.0.0:8080
```

### Ingest (benchmark all rows)

```bash
curl "http://localhost:8080/dvf/ingest"
```

### Example queries

```bash
curl "http://localhost:8080/dvf/filter?dep=75&from=2023-01-01&to=2023-12-31"
curl "http://localhost:8080/dvf/agg-commune?dep=75&year=2023"
curl "http://localhost:8080/dvf/topn?dep=75&year=2023&n=20"
curl "http://localhost:8080/dvf/geo-bbox?min_lon=2.20&min_lat=48.80&max_lon=2.42&max_lat=48.92&year=2023"
```

**Notes**

* Polars handles type coercions and computes `prix_m2`; Parquet files are partitioned by `year` and `code_departement`.
* DuckDB exposes a `dvf` view over Parquet (`hive_partitioning=true`).

---

## What to Measure
* **Ingestion speed**
    * Laravel: **5,000** rows/s (`n=100000`).
    * Rust: **200,000** rows/s (`n=0`).

---

## Endpoint Summary (same for both stacks)

| Endpoint           | Purpose                                      | Key params                                                             |
| ------------------ | -------------------------------------------- | ---------------------------------------------------------------------- |
| `/dvf/ingest`      | Trigger ingestion (bounded by `n`)           | `n` (rows to process; `0` = unlimited)                                 |
| `/dvf/filter`      | Filter by department and date range          | `dep`, `from`, `to`, `page`, `per_page`                                |
| `/dvf/agg-commune` | Aggregate per commune (avg/median `prix_m2`) | `dep`, `year`                                                          |
| `/dvf/topn`        | Top-N communes by average `prix_m2`          | `dep`, `year`, `n`                                                     |
| `/dvf/geo-bbox`    | Bounding-box query (optional `year`)         | `min_lon`, `min_lat`, `max_lon`, `max_lat`, `year`, `page`, `per_page` |

All parameters and responses are aligned, enabling fair comparison.
