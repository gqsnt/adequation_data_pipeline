use crate::duck::register_parquet_view;
use crate::sink::*;
use crate::types::*;
use crate::util::*;
use axum::Json;
use polars::prelude::*;
use polars::prelude::{col, lit, Expr};
use polars::prelude::{concat_str, DataType::String as DtString};
use std::path::{Path, PathBuf};

#[test]
pub fn test_run() {
    let start = std::time::Instant::now();
    // get file test.json from project root
    let file = std::fs::read_to_string("test.json").unwrap();
    let req: RunReq = serde_json::from_str(&file).unwrap();
    let resp = run_pipeline_sync(req);
    println!("Done in {:?}", start.elapsed());
    println!("resp: {:?}", resp);
}

#[test]
pub fn test_infer() {
    let start = std::time::Instant::now();
    // get file test_infer.json from project root
    let req: InferSchemaReq = InferSchemaReq {
        uri: "file://..\\datas\\samples\\dvf.csv".into(),
        source_config: SourceCfg::Csv {
            delimiter: ",".into(),
            has_header: true,
            encoding: "utf-8".into(),
        },
        limit: 1000,
    };
    let resp = infer_schema_sync(req).unwrap();
    println!("resp: {:?}", resp);
    println!("Done in {:?}", start.elapsed());
}

pub async fn infer_schema(
    Json(req): Json<InferSchemaReq>,
) -> Result<Json<InferSchemaResp>, (axum::http::StatusCode, String)> {
    let result = tokio::task::spawn_blocking(move || infer_schema_sync(req))
        .await
        .map_err(to_http)??;
    Ok(Json(result))
}

pub fn infer_schema_sync(
    req: InferSchemaReq,
) -> Result<InferSchemaResp, (axum::http::StatusCode, String)> {
    let ldf = match &req.source_config {
        SourceCfg::Csv {
            delimiter,
            has_header,
            encoding,
        } => scan_csv_lazy_sync(&req.uri, delimiter, *has_header, encoding).map_err(to_http)?,
        SourceCfg::Parquet => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "Parquet schema inference not implemented".into(),
            ));
        }
    };
    let df = ldf.limit(req.limit as IdxSize).collect().map_err(to_http)?;
    let schema = infer_arrow_like_schema(&df);
    Ok(InferSchemaResp { schema })
}

pub async fn run_pipeline(
    Json(req): Json<RunReq>,
) -> Result<Json<RunResp>, (axum::http::StatusCode, String)> {
    let result = tokio::task::spawn_blocking(move || run_pipeline_sync(req))
        .await
        .map_err(to_http)?
        .map_err(to_http)?;
    Ok(Json(result))
}
fn run_pipeline_sync(req: RunReq) -> anyhow::Result<RunResp> {
    // 0) Validate mapping targets
    validate_mapping_against_schema(req.datasets.1.schema(), &req.mapping)?;

    // 1) Roots
    let ori_root = table_root(
        &req.project.warehouse_uri,
        &req.project.namespace,
        req.datasets.0.name(),
    )?;
    let dest_root = table_root(
        &req.project.warehouse_uri,
        &req.project.namespace,
        req.datasets.1.name(),
    )?;

    println!("ori_root: {:?}", ori_root);
    println!("dest_root: {:?}", dest_root);

    // 2) Build bronze LF (CSV -> stage to Parquet)
    let (mut bronze_lf_src, ori_rows, _bronze_snapshot_opt) = build_source_lazy(&req, &ori_root)?;

    // 3) Single enhanced plan on top of bronze
    let (with_targets, filter_pred, target_cols, source_cols, violation_exprs) =
        build_enhanced_plan(&mut bronze_lf_src, &req.mapping, req.datasets.1.schema())?;

    let primary_key = req.datasets.1.primary_key().clone();
    let use_hashed_pk = !primary_key.is_empty();

    // base valid rows
    let mut dest_valid_lf = with_targets.clone().filter(filter_pred.clone());

    // add `_pk_hash` and include it in select if PK exists
    if use_hashed_pk {
        let pk_complete = primary_key
            .iter()
            .fold(lit(true), |acc, k| acc.and(col(k).is_not_null()));
        dest_valid_lf = dest_valid_lf.filter(pk_complete);
        let pk_e = pk_hash_expr(&primary_key)
            .expect("pk_hash_expr must be Some when primary_key is non-empty")
            .alias("_pk_hash");
        // select target columns + the internal key
        let mut select_cols = target_cols.clone();
        select_cols.push(col("_pk_hash"));
        dest_valid_lf = dest_valid_lf.with_columns([pk_e]).select(select_cols);
    } else {
        // original target-only selection
        dest_valid_lf = dest_valid_lf.select(target_cols.clone());
    }
    let dedup_cols: Vec<String> = if use_hashed_pk {
        vec!["_pk_hash".into()]
    } else {
        primary_key.clone()
    };

    let dest_df = {
        // Eager: collect, dedup, anti-join
        let mut df = dest_valid_lf.collect()?;
        if !dedup_cols.is_empty() {
            df = df.unique_stable(Some(&dedup_cols), UniqueKeepStrategy::First, None)?;
            let rehash_from = if use_hashed_pk {
                Some(primary_key.as_slice())
            } else {
                None
            };
            df = append_unique_against_existing(df, &dest_root, &dedup_cols, rehash_from)?;
        }
        df
    };

    let dest_rows = dest_df.height() as i64;

    // Compute DQ summary / invalid samples BEFORE deciding to write,
    // so we can return early when there are no new rows.
    let dq_summary = aggregate_dq_summary(&with_targets, &violation_exprs)?;
    let samples = collect_invalid_samples(
        &with_targets,
        &filter_pred,
        &violation_exprs,
        &source_cols,
        1000,
    )?;

    // If there are no new rows after anti-join, skip writing a new file.
    if dest_rows == 0 {
        return Ok(RunResp {
            snapshot: String::new(),
            ori_rows,
            dest_rows,
            rejected_rows: ori_rows - dest_rows,
            error_samples: samples,
            dq_summary,
            logs: vec![
                "stage: read source".into(),
                "stage: apply mapping".into(),
                "stage: apply dedup".into(),
                "stage: append unique (no new rows)".into(),
                "stage: compute DQ summary".into(),
            ],
        });
    }

    // 6) Write dest snapshot (eager) — drop the internal `_pk_hash` before persisting
    let mut to_write = dest_df.clone();
    if use_hashed_pk {
        let _ = to_write.drop_in_place("_pk_hash");
    }
    let (_, dest_snapshot) = write_parquet_snapshot(to_write, &dest_root)?;

    // store only the single-key column if we used it; otherwise the original PK columns
    let _ = write_keys_index(&dest_df, &dest_root, &dedup_cols)?;

    // 10) DuckDB gold view (if applicable)
    if matches!(req.datasets.1, DataSet::Gold(_)) {
        if let Ok(base) = std::env::var("DUCKDB_BASE_DIR") {
            let glob = dest_root.join("data").join("*.parquet");
            let _ =
                register_parquet_view(&base, &req.project.namespace, req.datasets.1.name(), &glob);
        }
    }

    Ok(RunResp {
        snapshot: dest_snapshot,
        ori_rows,
        dest_rows,
        rejected_rows: ori_rows - dest_rows,
        error_samples: samples,
        dq_summary,
        logs: vec![
            "stage: read source".into(),
            "stage: apply mapping".into(),
            "stage: apply dedup".into(),
            "stage: append unique".into(),
            "stage: write dest".into(),
            "stage: write keys index".into(),
            "stage: compute DQ summary".into(),
        ],
    })
}

fn scan_csv_lazy_sync(
    uri: &str,
    delimiter: &String,
    has_header: bool,
    encoding: &str,
) -> anyhow::Result<LazyFrame> {
    use polars::prelude::{CsvEncoding, LazyCsvReader};

    let path = local_path_from_file_uri(uri)?;
    println!("scan_csv_lazy: {:?}", path);
    println!("Current dir: {:?}", std::env::current_dir()?);
    let mut reader = LazyCsvReader::new(PlPath::from_str(path.to_string_lossy().as_ref()))
        .with_has_header(has_header)
        .with_infer_schema_length(Some(1_000))
        .with_ignore_errors(true);

    if !delimiter.is_empty() {
        reader = reader.with_separator(delimiter.as_bytes()[0]);
    }

    // Nouvel ajout: mapping encoding
    let enc = match encoding.to_lowercase().as_str() {
        "utf-8" | "utf8" => CsvEncoding::Utf8,
        _ => CsvEncoding::LossyUtf8, // MVP: on “répare” silencieusement
    };
    reader = reader.with_encoding(enc);
    Ok(reader.finish()?)
}

/// Applique le mapping (filters + projections) → DataFrame Silver

fn to_http<E: std::fmt::Display>(e: E) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::BAD_REQUEST, e.to_string())
}

fn keys_index_root(silver_root: &Path) -> PathBuf {
    silver_root.join("keys_index")
}

fn write_keys_index(df: &DataFrame, silver_root: &Path, keys: &[String]) -> anyhow::Result<String> {
    if keys.is_empty() {
        return Ok(String::new());
    }
    let mut only_keys = df
        .clone()
        .lazy()
        .select(
            keys.iter()
                .map(|k| col(k).cast(DtString).alias(k))
                .collect::<Vec<_>>(),
        )
        .collect()?;
    only_keys = only_keys.unique_stable(None, UniqueKeepStrategy::First, None)?;
    let ki_root = keys_index_root(silver_root);
    Ok(write_parquet_snapshot(only_keys, &ki_root)?.1)
}

fn scan_keys_index(silver_root: &Path, keys: &[String]) -> anyhow::Result<LazyFrame> {
    let root = keys_index_root(silver_root);
    let ldf = scan_parquet_table_sync(&root)?;
    // ensure same column order as keys (avoid schema shuffle)
    Ok(ldf.select(keys.iter().map(col).collect::<Vec<_>>()))
}

fn append_unique_against_existing(
    new_df: DataFrame,
    silver_root: &Path,
    keys: &[String],
    rehash_from: Option<&[String]>,
) -> anyhow::Result<DataFrame> {
    if keys.is_empty() {
        return Ok(new_df);
    }

    // 1) Préférence index de clés si présent ET non vide
    let ki_data = keys_index_root(silver_root).join("data");
    if ki_data.exists() && has_any_parquet(&ki_data).unwrap_or(false) {
        let existing_keys = scan_keys_index(silver_root, keys)?; // schéma cohérent
        let existing_keys = existing_keys.select(
            keys.iter().map(|k| col(k).cast(DtString).alias(k)).collect::<Vec<_>>()
        );
        let left_on: Vec<Expr> = keys.iter().map(|k| col(k).cast(DtString)).collect();
        let right_on: Vec<Expr> = keys.iter().map(|k| col(k).cast(DtString)).collect();
        return Ok(new_df.lazy()
            .join(existing_keys, left_on, right_on, JoinArgs::new(JoinType::Anti))
            .collect()?);
    }

    // 2) Sinon, table Silver si non vide ; sinon "rien d'existant" => tout passe
    let silver_data = silver_root.join("data");
    if silver_data.exists() && crate::sink::has_any_parquet(&silver_data).unwrap_or(false) {
        let ldf = scan_parquet_table_sync(silver_root)?;
        let existing_keys = if keys.len() == 1 && keys[0] == "_pk_hash" {
            if let Some(orig) = rehash_from {
                let pk = pk_hash_expr(&orig.to_vec())
                    .expect("pk_hash_expr must exist when rehash_from provided")
                    .alias("_pk_hash");
                ldf.with_columns([pk]).select([col("_pk_hash")])
            } else {
                // pas de moyen de reconstruire : ne pas filtrer
                return Ok(new_df);
            }
        } else {
            ldf.select(keys.iter().map(col).collect::<Vec<_>>())
        };

        let existing_keys = existing_keys.select(
            keys.iter().map(|k| col(k).cast(DtString).alias(k)).collect::<Vec<_>>()
        );
        let left_on: Vec<Expr> = keys.iter().map(|k| col(k).cast(DtString)).collect();
        let right_on: Vec<Expr> = keys.iter().map(|k| col(k).cast(DtString)).collect();

        return Ok(new_df.lazy()
            .join(existing_keys, left_on, right_on, JoinArgs::new(JoinType::Anti))
            .collect()?);
    }

    // 3) Aucune donnée existante : premier write, pas d’anti-join
    Ok(new_df)
}

// --- IR vs Target Schema validation ----------------------------------------
fn validate_mapping_against_schema(
    schema: &ArrowLikeSchema,
    mapping: &MappingCfg,
) -> anyhow::Result<()> {
    use std::collections::HashSet;
    let target_fields: HashSet<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    let mapped_targets: HashSet<&str> = mapping
        .transforms
        .columns
        .iter()
        .map(|c| c.target.as_str())
        .collect();

    // must cover exactly the target schema columns (no missing; extras allowed? -> here: no extras)
    for f in &schema.fields {
        if !mapped_targets.contains(f.name.as_str()) {
            anyhow::bail!("mapping missing target column '{}'", f.name);
        }
    }
    for t in &mapped_targets {
        if !target_fields.contains(t) {
            anyhow::bail!("mapping defines extra target column '{}'", t);
        }
    }

    Ok(())
}

// --- DQ helpers -------------------------------------------------------------

fn lit_from_json(v: &serde_json::Value) -> Expr {
    use serde_json::Value::*;
    match v {
        Null => lit(NULL),
        Bool(b) => lit(*b),
        Number(n) => {
            if let Some(i) = n.as_i64() {
                lit(i)
            } else if let Some(f) = n.as_f64() {
                lit(f)
            } else {
                lit(n.to_string())
            }
        }
        String(s) => lit(s.as_str()),
        _ => lit(v.to_string()),
    }
}

fn build_dq_violation_expr(
    rule: &crate::types::DqRule,
    target_exprs: &std::collections::HashMap<String, Expr>,
) -> anyhow::Result<Expr> {
    let left = target_exprs
        .get(&rule.column)
        .ok_or_else(|| {
            anyhow::anyhow!(format!(
                "DQ rule references unknown target column '{}'",
                rule.column
            ))
        })?
        .clone();

    let left_cpy = left.clone(); // used for is_not_null
    let out = match rule.op.as_str() {
        ">" => left.gt(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::from(0)),
        )),
        ">=" => left.gt_eq(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::from(0)),
        )),
        "<" => left.lt(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::from(0)),
        )),
        "<=" => left.lt_eq(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::from(0)),
        )),
        "==" => left.eq(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::Null),
        )),
        "!=" => left.neq(lit_from_json(
            &rule.value.clone().unwrap_or(serde_json::Value::Null),
        )),
        "is_not_null" => left.is_null().not(), // violation==false -> we invert below; we want "violation" expr, so:
        "is_null" => left.is_null(),
        _ => anyhow::bail!("unsupported DQ op '{}'", rule.op),
    };

    // For "is_not_null", a violation is actually (left.is_null())
    let violation = if rule.op == "is_not_null" {
        left_cpy.is_null()
    } else {
        // For normal compare, violation means NOT(condition)
        out.not()
    };

    Ok(violation)
}

fn dq_code(col: &str, op: &str) -> String {
    format!("DQ_{}_{}", col, op)
}

fn build_source_lazy(
    req: &RunReq,
    ori_root: &std::path::Path,
) -> anyhow::Result<(LazyFrame, i64, Option<String>)> {
    match &req.datasets.0 {
        DataSet::Bronze { uri, source, inner } => match source {
            SourceCfg::Csv {
                delimiter,
                has_header,
                encoding,
            } => {
                // Read CSV lazily
                let mut staged = scan_csv_lazy_sync(uri, delimiter, *has_header, encoding)?;
                // Enforce the configured *Bronze* schema before writing Parquet
                let enforced = enforce_lazyframe_to_schema(&mut staged, &inner.schema)?;
                let ori_rows = crate::util::lazy_count_rows(&enforced)?;
                // Stream write enforced types to Parquet and get a robust row count
                let (bronze_path, bronze_snapshot) =
                    write_parquet_snapshot(enforced.collect().unwrap(), ori_root)?;
                let staged_lf = LazyFrame::scan_parquet(
                    PlPath::new(bronze_path.to_string_lossy().as_ref()),
                    Default::default(),
                )?;
                Ok((staged_lf, ori_rows, Some(bronze_snapshot)))
            }
            SourceCfg::Parquet => anyhow::bail!("Bronze Parquet not implemented"),
        },
        DataSet::Silver(ds) => {
            let lf = scan_parquet_table_or_empty(
                &table_root(&req.project.warehouse_uri, &req.project.namespace, &ds.name)?,
                &ds.schema,
            )?;
            // cheap count if you need ori_rows:
            let ori_rows = crate::util::lazy_count_rows(&lf)?;
            Ok((lf, ori_rows, None))
        }
        DataSet::Gold(_) => anyhow::bail!("Gold as source not supported"),
    }
}

fn build_enhanced_plan(
    bronze_lf: &mut LazyFrame,
    mapping: &MappingCfg,
    target_schema: &ArrowLikeSchema,
) -> anyhow::Result<(LazyFrame, Expr, Vec<Expr>, Vec<Expr>, Vec<(String, Expr)>)> {
    use polars::prelude::*;
    // Map target name -> declared dtype
    let mut dtype_map = std::collections::HashMap::<&str, DataType>::new();
    for f in &target_schema.fields {
        dtype_map.insert(f.name.as_str(), polars_dtype_from_str(&f.r#type)?);
    }
    // Build target columns as Expr = (clean -> cast -> alias target)
    let mut target_cols_exprs: Vec<Expr> = Vec::with_capacity(mapping.transforms.columns.len());
    // Track parse-fail flags for non-string targets (so we can reject + report)
    let mut parse_fail_violations: Vec<(String, Expr)> = Vec::new();

    for c in &mapping.transforms.columns {
        let raw = crate::expr::build_expr(&c.expr)?;
        // treat "" as null before casting (common in CSVs)
        let cleaned = when(
            raw.clone()
                .cast(DataType::String)
                .str()
                .len_chars()
                .eq(lit(0)),
        )
        .then(lit(NULL))
        .otherwise(raw);
        let dt = dtype_map.get(c.target.as_str()).ok_or_else(|| {
            anyhow::anyhow!(format!("unknown target column '{}' in schema", c.target))
        })?;
        let casted = coerce_expr_to_dtype(cleaned.clone(), dt).alias(&c.target);
        target_cols_exprs.push(casted);
        if !matches!(dt, DataType::String) {
            let had_content = cleaned
                .clone()
                .cast(DataType::String)
                .str()
                .len_chars()
                .gt(lit(0));
            let parse_failed = had_content.and(col(&c.target).is_null());
            parse_fail_violations.push((format!("PARSE_FAIL_{}", c.target), parse_failed));
        }
    }
    let with_targets = bronze_lf.clone().with_columns(target_cols_exprs.clone());

    // filter predicate
    let mut pred: Option<Expr> = None;
    for f in &mapping.transforms.filters {
        let e = crate::expr::build_expr(f)?;
        pred = Some(match pred {
            Some(p) => p.and(e),
            None => e,
        });
    }
    // Enforce "no parse failures" as part of acceptance predicate
    let mut no_parse_fail = lit(true);
    for (_, pf) in &parse_fail_violations {
        no_parse_fail = no_parse_fail.and(pf.clone().not());
    }
    let filter_pred = pred
        .unwrap_or(polars::prelude::lit(true))
        .and(no_parse_fail);

    // DQ violation flags built on target columns (now real, properly typed cols)
    let mut target_name_to_expr = std::collections::HashMap::<String, Expr>::new();
    for c in &mapping.transforms.columns {
        target_name_to_expr.insert(c.target.clone(), col(&c.target));
    }
    let mut violation_exprs: Vec<(String, Expr)> = mapping
        .dq_rules
        .iter()
        .map(|r| {
            let e = crate::etl::build_dq_violation_expr(r, &target_name_to_expr)?; // reuse your helper
            Ok((crate::etl::dq_code(&r.column, &r.op), e))
        })
        .collect::<anyhow::Result<_>>()?;
    violation_exprs.extend(parse_fail_violations);
    // names for selects
    let target_cols = mapping
        .transforms
        .columns
        .iter()
        .map(|c| col(&c.target))
        .collect::<Vec<_>>();
    let source_cols = bronze_lf
        .collect_schema()?
        .iter_names()
        .map(|n| col(n.clone()))
        .collect::<Vec<_>>();

    Ok((
        with_targets,
        filter_pred,
        target_cols,
        source_cols,
        violation_exprs,
    ))
}

fn aggregate_dq_summary(
    with_targets: &LazyFrame,
    violations: &[(String, Expr)],
) -> anyhow::Result<Vec<crate::types::DqSummaryItem>> {
    use polars::prelude::*;
    if violations.is_empty() {
        return Ok(vec![]);
    }
    let sums = with_targets
        .clone()
        .select(
            violations
                .iter()
                .map(|(code, e)| e.clone().cast(DataType::Int64).sum().alias(code))
                .collect::<Vec<_>>(),
        )
        .collect()?;

    let checked_rows = crate::util::lazy_count_rows(with_targets)?;

    let mut out = Vec::with_capacity(violations.len());
    for (code, _) in violations {
        let v = sums
            .column(code)?
            .get(0)
            .map(|av| match av {
                AnyValue::Int64(i) => i,
                AnyValue::UInt64(u) => u as i64,
                AnyValue::Int32(i) => i as i64,
                _ => 0,
            })
            .unwrap_or(0);
        out.push(crate::types::DqSummaryItem {
            rule_code: code.clone(),
            violations: v,
            checked_rows,
        });
    }
    Ok(out)
}
fn collect_invalid_samples(
    with_targets: &LazyFrame,
    filter_pred: &Expr,
    violations: &[(String, Expr)],
    source_cols: &[Expr],
    limit_n: usize,
) -> anyhow::Result<Vec<crate::types::ErrorSample>> {
    use polars::prelude::*;
    // invalid = !filter OR any(violation)
    let mut invalid = filter_pred.clone().not();
    for (_, e) in violations {
        invalid = invalid.or(e.clone());
    }

    // You can add reason-flag columns here if you want per-row code inspection:
    // let reason_cols = violations.iter().map(|(c,e)| e.clone().alias(c)).collect::<Vec<_>>();

    let df = with_targets
        .clone()
        .filter(invalid)
        .select(source_cols.to_vec())
        .limit(limit_n as IdxSize)
        .collect()?;

    // convert to samples (same as before)
    let mut samples = Vec::with_capacity(df.height());
    let cols = df.get_columns();
    for i in 0..df.height() {
        let mut obj = serde_json::Map::new();
        for s in cols {
            let any = unsafe { s.get_unchecked(i) };
            obj.insert(s.name().to_string(), crate::util::anyvalue_to_json(&any));
        }
        samples.push(crate::types::ErrorSample {
            reason_code: "INVALID".into(), // optional: compute first violation true to set more precise code
            message: "filters failed or DQ violation".into(),
            row_no: Some(i as i64),
            source_values: serde_json::Value::Object(obj),
        });
    }
    Ok(samples)
}

fn pk_hash_expr(keys: &[String]) -> Option<Expr> {
    if keys.is_empty() {
        return None;
    }
    // Length-prefixed + NULL-safe representation: "<len>:<val>|..." to avoid collisions.
    let parts: Vec<Expr> = keys
        .iter()
        .flat_map(|k| {
            let s = col(k).cast(DtString);
            let s_nn = s.clone().fill_null(lit("<NULL>"));
            let len = s_nn.clone().str().len_chars().cast(DtString);
            [
                len,      // "<len>"
                lit(":"), // ":"
                s_nn,     // "<val or <NULL>>"
                lit("|"), // field sep
            ]
        })
        .collect();
    Some(concat_str(parts, "", false))
}
