# laravel-data-pipeline

> ⚠️ **Minimal preview**: this repo boots the UI + APIs and the Rust worker, but the **“Run pipeline” flow is not working yet**. You can create Projects/Sources/Datasets and **infer schemas**, but end-to-end executes are WIP.

---

## What’s inside

* **Laravel + Sail** (PHP 8.4, Vite) — UI & APIs
* **Postgres 17** — app DB
* **Redis 7** — queues/cache
* **Rust worker (Axum)** — `/infer_schema`, `/run` endpoints (WIP)
* **DuckDB** — used by worker for Parquet views
* **Metabase** — points to DuckDB for quick charts

**Ports:** Web `8080`, Vite `5173`, Worker `8081`, Postgres `5432`, Redis `6379`, Metabase `3000`
**Warehouse:** `./datas/warehouse` (mounted in web+worker)
**Samples:** `./datas/samples` (read-only in worker)

---

## Quick start (dev)

```bash
# 0) Clone and enter the repo, then:
cp web/.env.example web/.env
# Make sure DB_*, REDIS_* match docker-compose (defaults: app/secret/app)

# 1) Up all services
docker compose up -d --build

# 2) Install & init Laravel (inside container)
docker exec -it laravel-app bash -lc '
  composer install &&
  php artisan key:generate &&
  php artisan migrate &&
  npm ci &&
  npm run dev
'
# UI: http://localhost:8080  (Vite HMR on 5173)
# Metabase: http://localhost:3000
```

---

## What works today

* **Projects**

    * Create project (`slug`, `warehouse_uri`, `namespace`)
* **Sources (CSV)**

    * CRUD + **Infer schema** (calls worker `/infer_schema`)
    * Bronze dataset auto-created/updated from inferred schema
* **Silver**

    * Seed from Source → edit fields + PK
* **Gold**

    * Create/update/delete one or many Gold datasets
* **Pipelines**

    * Create/update/delete (attach mappings)

---

## Not working (WIP)

* **Runs** (Project → Runs → “New Run”): the call path to the worker exists but full execute/append/metrics is **not reliable yet**. Treat it as a stub for now.
* **Mappings**: create/edit/delete exists but ui is not functional. 
* **Metabase**: you can connect to DuckDB but there’s no data yet (runs are WIP).
---

## Worker API (local)

* `POST http://localhost:8081/infer_schema`

  ```json
  {
    "uri": "file:///samples/dvf.csv",
    "source_config": { "Csv": { "delimiter": ",", "has_header": true, "encoding": "utf-8" } },
    "limit": 500
  }
  ```
* `POST http://localhost:8081/run` *(present but not stable yet)*

The worker writes Parquet snapshots under `./datas/warehouse/{namespace}/{dataset}/data/*.parquet`
and (for Gold) registers a DuckDB view in `./duckdb-data/{namespace}.duckdb`.

---

## Compose overview (short)

* **laravel**: mounts `./web` and `./datas/warehouse`
* **worker**: mounts `./datas/warehouse`, `./datas/samples` (ro), `duckdb-data`
* **pgsql**, **redis**: persisted via named volumes
* **metabase**: reads DuckDB + Warehouse (ro)

---

## Troubleshooting

* UI is up but assets 404 → rerun `npm run dev` inside `laravel-app`.
* “Sync schema” does nothing → check worker on `http://localhost:8081/infer_schema`.
* Metabase empty → ensure there’s Parquet under `datas/warehouse/...` (runs are WIP).

---

