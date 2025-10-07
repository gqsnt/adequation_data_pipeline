use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs::{read_to_string, write};
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Deserialize)]
struct Latest {
    snapshot_date: String,
}

#[derive(Debug, Clone)]
pub struct RefreshCfg {
    pub slug: String,
    pub storage_root: PathBuf,
    pub manifests_dir: String,
    pub gold_dir: String,
    pub snapshot_date: Option<String>,
    pub duckdb_path: PathBuf,
}


pub fn refresh_duckdb(cfg: RefreshCfg) -> Result<()> {

    let snapshot = match cfg.snapshot_date {
        Some(s) => s,
        None => {
            let latest_path = cfg
                .storage_root
                .join(&cfg.manifests_dir)
                .join(&cfg.slug)
                .join("latest.json");
            let txt = read_to_string(&latest_path)
                .with_context(|| format!("read {}", latest_path.display()))?;
            let latest: Latest = serde_json::from_str(&txt)?;
            latest.snapshot_date
        }
    };

    // 2) Build glob to Parquet files
    let parquet_glob_path = cfg
        .storage_root
        .join(&cfg.gold_dir)
        .join(&cfg.slug)
        .join(format!("snapshot_date={}", snapshot))
        .join("**")
        .join("*.parquet");

    let parquet_glob_norm = normalize_for_duckdb_path(&parquet_glob_path.to_string_lossy());
    let parquet_glob_sql = escape_single_quotes(&parquet_glob_norm);

    let ref_depts_path = cfg
        .storage_root
        .join(&cfg.gold_dir)
        .join("ref")
        .join("departements.parquet");
    let ref_depts_norm = normalize_for_duckdb_path(&ref_depts_path.to_string_lossy());
    let ref_depts_sql = escape_single_quotes(&ref_depts_norm);


    let schema_name = cfg.gold_dir; // e.g. "gold"
    let view_latest = format!("{}_latest", cfg.slug);
    let view_transaction_latest = format!("{}_transaction_latest", cfg.slug);
    let view_residential_latest = format!("{}_residential_latest", cfg.slug);
    let view_price_metrics_yoy = format!("{}_price_metrics_yoy", cfg.slug);
    let view_monthly_price = format!("{}_monthly_price", cfg.slug);
    let view_summary = format!("{}_summary", cfg.slug);
    let view_price_metrics = format!("{}_price_metrics", cfg.slug);
    let view_tiles_gh64 = format!("{}_tiles_gh64", cfg.slug);
    let view_points = format!("{}_points", cfg.slug); // geometry points (from latest)
    let view_points_ll = format!("{}_points_ll", cfg.slug); // lon/lat projection
    let view_dept_join = format!("{}_by_dept", cfg.slug); // choropleth-friendly agg

    let sql = format!(
        r#"
PRAGMA disable_progress_bar;
INSTALL spatial; LOAD spatial;

CREATE SCHEMA IF NOT EXISTS ref;
CREATE OR REPLACE VIEW ref.departements AS
SELECT * FROM read_parquet('{ref_depts}');

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE VIEW {schema}.{view_latest} AS
SELECT * EXCLUDE (year, dept)
FROM read_parquet('{glob}', union_by_name=true);



-- 1) Table de PRÉSENTATION
CREATE OR REPLACE VIEW {schema}.{view_transaction_latest} AS
SELECT
  id_mutation, numero_disposition, nature_mutation, date_mutation,
  valeur_fonciere, surface_reelle_bati, nombre_pieces_principales,
  code_type_local, UPPER(COALESCE(type_local,'INCONNU')) AS type_local,
  code_postal, code_commune, UPPER(nom_commune) AS nom_commune,
  COALESCE(code_departement,'UNK') AS code_departement,
  longitude, latitude, geohash6,
  prix_m2,
  CAST(EXTRACT(YEAR FROM date_mutation) AS SMALLINT) AS year_mutation,
  DATE_TRUNC('month', date_mutation)                  AS month_start
FROM {schema}.{view_latest};


-- 2) Points géométriques & projection lat/lon
CREATE OR REPLACE VIEW {schema}.{view_points} AS
SELECT t.*, CAST(ST_Point(longitude, latitude) AS GEOMETRY) AS geom
FROM {schema}.{view_transaction_latest} t
WHERE longitude IS NOT NULL AND latitude IS NOT NULL;

CREATE OR REPLACE VIEW {schema}.{view_points_ll} AS
SELECT
  id_mutation, date_mutation, year_mutation, month_start,
  code_departement, type_local, valeur_fonciere, prix_m2,
  latitude, longitude
FROM {schema}.{view_points};

-- 3) Résidentiel propre + winsorisation p01–p99 (stabilité €/m²)
CREATE OR REPLACE VIEW {schema}.{view_residential_latest} AS
WITH base AS (
  SELECT *
  FROM {schema}.{view_transaction_latest}
  WHERE UPPER(type_local) IN ('APPARTEMENT','MAISON')
    AND surface_reelle_bati IS NOT NULL AND surface_reelle_bati > 9
    AND prix_m2 IS NOT NULL AND prix_m2 > 0
),
q AS (
  SELECT
    year_mutation, code_departement, type_local,
    quantile_cont(prix_m2, 0.01) AS p01,
    quantile_cont(prix_m2, 0.99) AS p99
  FROM base
  GROUP BY 1,2,3
)
SELECT
  b.*,
  GREATEST(LEAST(b.prix_m2, q.p99), q.p01) AS prix_m2_winsor
FROM base b
JOIN q USING (year_mutation, code_departement, type_local);

-- 4) Agrégats prêts à l’emploi
CREATE OR REPLACE VIEW {schema}.{view_summary} AS
SELECT year_mutation, code_departement, COUNT(*)::BIGINT AS n_rows
FROM {schema}.{view_transaction_latest}
GROUP BY 1,2 ORDER BY 1,2;


CREATE OR REPLACE VIEW {schema}.{view_price_metrics} AS
SELECT
  year_mutation, code_departement, type_local,
  COUNT(*)::BIGINT                AS n,
  median(valeur_fonciere)::DOUBLE AS median_valeur_fonciere,
  median(prix_m2)::DOUBLE         AS median_prix_m2
FROM {schema}.{view_residential_latest}
GROUP BY 1,2,3;

-- 5) Série mensuelle (résidentiel propre)
CREATE OR REPLACE VIEW {schema}.{view_monthly_price} AS
SELECT
  month_start, code_departement, type_local,
  COUNT(*)::BIGINT            AS n,
  median(prix_m2_winsor)::DOUBLE AS median_prix_m2_w
FROM {schema}.{view_residential_latest}
GROUP BY 1,2,3;

-- 6) YoY par département/type (résidentiel)
CREATE OR REPLACE VIEW {schema}.{view_price_metrics_yoy} AS
WITH y AS (
  SELECT
    year_mutation, code_departement, type_local,
    median(prix_m2_winsor)::DOUBLE AS med_p2
  FROM {schema}.{view_residential_latest}
  GROUP BY 1,2,3
),
lagged AS (
  SELECT
    y.*,
    LAG(med_p2) OVER (
      PARTITION BY code_departement, type_local
      ORDER BY year_mutation
    ) AS med_p2_prev
  FROM y
)
SELECT
  year_mutation, code_departement, type_local, med_p2,
  CASE WHEN med_p2_prev IS NULL OR med_p2_prev=0 THEN NULL
       ELSE (med_p2/med_p2_prev - 1.0)
  END AS yoy_med_p2
FROM lagged;

-- 7) Grille geohash6 (heatmap)
CREATE OR REPLACE VIEW {schema}.{view_tiles_gh64} AS
SELECT
  year_mutation, geohash6,
  COUNT(*)::BIGINT           AS n,
  median(prix_m2_winsor)::DOUBLE AS median_prix_m2_w
FROM {schema}.{view_residential_latest}
WHERE geohash6 IS NOT NULL
GROUP BY 1,2;

-- 8) Choroplèthe département (jointure polygones)
CREATE OR REPLACE VIEW {schema}.{view_dept_join} AS
SELECT
  d.code_departement, d.nom_departement,
  COUNT(*)::BIGINT            AS n,
  median(r.prix_m2_winsor)::DOUBLE AS median_prix_m2_w
FROM ref.departements d
JOIN {schema}.{view_points} p
  ON ST_Intersects(d.geom, p.geom)
JOIN {schema}.{view_residential_latest} r
  ON r.id_mutation = p.id_mutation
GROUP BY 1,2;

"#,
        schema = schema_name,
        view_latest = view_latest,
        view_summary = view_summary,
        view_price_metrics = view_price_metrics,
        view_tiles_gh64 = view_tiles_gh64,
        view_points = view_points,
        view_points_ll = view_points_ll,
        view_dept_join = view_dept_join,
        glob = parquet_glob_sql,
        ref_depts = ref_depts_sql,
    );

    let init_sql = cfg.duckdb_path.with_extension("init.sql");
    write(&init_sql, sql.as_bytes()).with_context(|| format!("write {}", init_sql.display()))?;

    let status = Command::new("duckdb")
        .arg(&cfg.duckdb_path)
        .arg("-init")
        .arg(&init_sql)
        .status()
        .map_err(|e| anyhow!("failed to spawn duckdb CLI: {e}. Is DuckDB installed?"))?;

    if !status.success() {
        return Err(anyhow!("duckdb CLI returned non-zero status"));
    }

    Ok(())
}

fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}
fn normalize_for_duckdb_path(s: &str) -> String {
    // DuckDB accepts forward slashes on all platforms
    s.replace('\\', "/")
}
