use crate::types::ExprIR;
use polars::prelude::{*, col as col_, lit as lit_};

pub fn build_expr(e: &ExprIR) -> PolarsResult<Expr> {
    use ExprIR::*;
    Ok(match e {
        Col { col } => col_(col),
        Lit { lit } => match lit {
            serde_json::Value::Null => lit_(NULL),
            serde_json::Value::Bool(b) => lit_(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { lit_(i) }
                else if let Some(f) = n.as_f64() { lit_(f) }
                else { lit_(n.to_string()) }
            }
            serde_json::Value::String(s) => lit_(s.as_str()),
            v => lit_(v.to_string()),
        },
        // Call-style node
        Call { fn_, args, to, fmt, len, pred, then, r#else } => {
            match fn_.as_str() {
                // Binaires arithmétiques/comparaisons
                "+" | "-" | "*" | "/" | "==" | "!=" | ">" | ">=" | "<" | "<=" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    let b = build_expr(args.get(1).ok_or_else(err_missing)?)?;
                    match fn_.as_str() {
                        "+" => a + b,
                        "-" => a - b,
                        "*" => a * b,
                        "/" => a / b,
                        "==" => a.eq(b),
                        "!=" => a.neq(b),
                        ">"  => a.gt(b),
                        ">=" => a.gt_eq(b),
                        "<"  => a.lt(b),
                        "<=" => a.lt_eq(b),
                        _    => unreachable!()
                    }
                }

                // cast(to)
                "cast" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    match to.as_deref() {
                        Some("i64")   => a.cast(DataType::Int64),
                        Some("f64")   => a.cast(DataType::Float64),
                        Some("utf8")  => a.cast(DataType::String),
                        Some("str")   => a.cast(DataType::String),
                        Some("date32")|Some("date") => a.cast(DataType::Date),
                        Some(other) => return Err(PolarsError::ComputeError(
                            format!("unsupported cast to {}", other).into()
                        )),
                        None => return Err(PolarsError::ComputeError("cast: missing 'to'".into())),
                    }
                }

                // to_date(fmt)
                "to_date" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    let fmt = fmt.clone().unwrap_or("%Y-%m-%d".into());
                    a.str().to_date(StrptimeOptions {
                        format: Some(fmt.into()),
                        strict: false,
                        exact: true,
                        cache: true,
                    })
                }

                // zfill(len)
                "zfill" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    let n = len.unwrap_or(2);
                    a.cast(DataType::String).str().zfill(lit(n as i64))
                }

                // when / then / else
                "when" => {
                    let p = build_expr(pred.as_ref()
                        .ok_or_else(|| PolarsError::ComputeError("when: missing pred".into()))?)?;
                    let t = build_expr(then.as_ref()
                        .ok_or_else(|| PolarsError::ComputeError("when: missing then".into()))?)?;
                    let el = match r#else {
                        Some(x) => build_expr(x)?,
                        None => lit(NULL),
                    };
                    when(p).then(t).otherwise(el)
                }

                // is_not_null / is_null
                "is_not_null" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    a.is_not_null()
                }
                "is_null" => {
                    let a = build_expr(args.first().ok_or_else(err_missing)?)?;
                    a.is_null()
                }

                _ => return Err(PolarsError::ComputeError(format!("unsupported fn {}", fn_).into()))
            }
        }
    })
}

fn err_missing() -> PolarsError { PolarsError::ComputeError("missing arg".into()) }
