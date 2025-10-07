

use axum::{routing::post, Router};
use std::net::SocketAddr;
use anyhow::Context;
use tracing_subscriber::{EnvFilter, fmt};
use etlctl::etl::{infer_schema,  run_pipeline};


fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::from_default_env()
        .add_directive("tower_http=info".parse().context("parse error")?)
        .add_directive("laravel_data_pipeline_worker=info".parse().context("parse error")?)
        .add_directive("polars=warn".parse().context("parse error")?);
    fmt().with_env_filter(filter).compact().init();




    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    tracing::info!("Worker listening on http://{}", bind);
    start_server(&bind)?;

    Ok(())
}

#[tokio::main]
async fn start_server(bind_addr: &str) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/infer_schema", post(infer_schema))
        .route("/run", post(run_pipeline));
    let addr: SocketAddr = bind_addr.parse().expect("invalid BIND_ADDR");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    Ok(axum::serve(listener, app).await?)
}

