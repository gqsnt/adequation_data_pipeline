cargo run --release -p cli -- ingest   --dataset dvf   --source ./samples/dvf.csv   --ingest-date 2025-10-02   --root ./data;
cargo run --release -p cli -- validate   --dataset dvf   --ingest-date 2025-10-02   --root ./data;
cargo run --release -p cli -- curate   --dataset dvf   --ingest-date 2025-10-02   --snapshot-date 2025-10-02   --root ./data;
cargo run -p cli -- duckdb-refresh   --dataset dvf   --root ./data   --db ./metabase/warehouse.duckdb;