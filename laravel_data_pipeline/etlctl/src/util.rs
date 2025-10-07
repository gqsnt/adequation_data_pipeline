use polars::prelude::*;
use std::collections::HashMap;

pub fn infer_arrow_like_schema(df: &DataFrame) -> crate::types::ArrowLikeSchema {
    let mut fields = Vec::new();
    for (name, dtype) in df.get_columns().iter().map(|s| (s.name(), s.dtype())) {
        fields.push(crate::types::Field {
            name: name.to_string(),
            r#type: dtype_to_str(dtype),
            nullable: true,
        })
    }
    crate::types::ArrowLikeSchema { fields }
}

pub fn lazy_count_rows(lf: &LazyFrame) -> anyhow::Result<i64> {
    // `len()` in a projection returns a single scalar (row count).
    // If a Polars version returns a column, we still read index 0.
    let df = lf.clone().select([len().alias("__n")]).collect()?;
    let s = df.column("__n")?;
    // Try common integer views; fall back to parsing AnyValue.
    if let Ok(ca) = s.i64() {
        Ok(ca.get(0).unwrap_or(0))
    } else if let Ok(ca) = s.u64() {
        Ok(ca.get(0).unwrap_or(0) as i64)
    } else if let Ok(ca) = s.i32() {
        Ok(ca.get(0).map(|v| v as i64).unwrap_or(0))
    } else if let Ok(av) = s.get(0) {
        // very defensive fallback
        Ok(av.to_string().parse::<i64>().unwrap_or(0))
    } else {
        Ok(0)
    }
}

fn dtype_to_str(dt: &DataType) -> String {
    use DataType::*;
    match dt {
        Int64 => "i64".to_string(),
        Int32 => "i32".to_string(),
        Float64 => "f64".to_string(),
        Float32 => "f32".to_string(),
        String => "str".to_string(),
        Boolean => "bool".to_string(),
        Date => "date".to_string(),
        Datetime(_, _) => "datetime".to_string(),
        _ => format!("{dt:?}"),
    }
}

/// Conversion AnyValue -> JSON
pub fn anyvalue_to_json(any: &AnyValue) -> serde_json::Value {
    match any {
        AnyValue::Null => serde_json::Value::Null,
        AnyValue::Boolean(b) => serde_json::Value::Bool(*b),
        AnyValue::Int64(i) => serde_json::Value::from(*i),
        AnyValue::Int32(i) => serde_json::Value::from(*i),
        AnyValue::UInt64(i) => serde_json::Value::from(*i),
        AnyValue::UInt32(i) => serde_json::Value::from(*i),
        AnyValue::Float64(f) => serde_json::json!(*f),
        AnyValue::Float32(f) => serde_json::json!(*f),
        AnyValue::String(s) => serde_json::Value::String(s.to_string()),
        _ => serde_json::Value::String(any.to_string()),
    }
}

/// Collecte un échantillon (n) depuis un LazyFrame -> Vec<Map>
pub fn collect_sample(
    ldf: LazyFrame,
    n: usize,
) -> anyhow::Result<Vec<HashMap<String, serde_json::Value>>> {
    let df = ldf.limit(n as IdxSize).collect()?;
    let mut out = Vec::with_capacity(df.height());
    let cols = df.get_columns();

    for i in 0..df.height() {
        let mut map = HashMap::new();
        for s in cols {
            let any = unsafe { s.get_unchecked(i) };
            map.insert(s.name().to_string(), anyvalue_to_json(&any));
        }
        out.push(map);
    }
    Ok(out)
}

pub fn polars_dtype_from_str(s: &str) -> anyhow::Result<DataType> {
    use polars::prelude::DataType::*;
    Ok(match s {
        "i64" => Int64,
        "i32" => Int32,
        "f64" => Float64,
        "f32" => Float32,
        "str" | "utf8" => String,
        "bool" => Boolean,
        "date" | "date32" => Date,
        "datetime" => Datetime(TimeUnit::Microseconds, None),
        other => anyhow::bail!("unsupported dtype in target schema: {other}"),
    })
}

pub fn coerce_expr_to_dtype(expr: Expr, dt: &DataType) -> Expr {
    use polars::prelude::{DataType::*, StrptimeOptions};
    match dt {
        Date => expr.cast(String).str().to_date(StrptimeOptions {
            format: None,
            strict: false,
            exact: true,
            cache: true,
        }),
        _ => expr.cast(dt.clone()),
    }
}

/// Build a projection that returns the `ldf` with columns *exactly* as described
/// in `schema` (order + dtype). Missing columns are filled with NULLs and cast.
/// For non-string targets, empty strings are treated as NULL before cast.
pub fn enforce_lazyframe_to_schema(
    ldf: &mut LazyFrame,
    schema: &crate::types::ArrowLikeSchema,
) -> anyhow::Result<LazyFrame> {
    use polars::prelude::*;
    let have = ldf
        .collect_schema()?
        .iter_names()
        .cloned()
        .collect::<std::collections::HashSet<_>>();

    let mut exprs: Vec<Expr> = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let dt = polars_dtype_from_str(&f.r#type)?;
        let base = if have.contains(&PlSmallStr::from_str(&f.name)) {
            col(&f.name)
        } else {
            lit(NULL)
        };
        // Treat "" as NULL for non-string targets, then cast.
        let cleaned = if matches!(dt, DataType::String) {
            base
        } else {
            when(base.clone().cast(DataType::String).eq(lit("")))
                .then(lit(NULL))
                .otherwise(base)
        };
        exprs.push(coerce_expr_to_dtype(cleaned, &dt).alias(&f.name));
    }
    Ok(ldf.clone().select(exprs))
}



pub fn has_any_parquet(dir: &std::path::Path) -> std::io::Result<bool> {
    if !dir.exists() { return Ok(false); }
    let mut it = std::fs::read_dir(dir)?;
    while let Some(Ok(e)) = it.next() {
        let p = e.path();
        if p.extension().map_or(false, |ext| ext == "parquet") {
            return Ok(true);
        }
    }
    Ok(false)
}