//! Minimal dataset descriptor loader (to be expanded).
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetDescriptor {
    pub slug: String,
    pub schema_version: u32,
    pub format: String,
}

pub fn load_descriptor(_slug: &str) -> Result<DatasetDescriptor> {
    // Placeholder: will read config/datasets/{slug}.toml later.
    Ok(DatasetDescriptor {
        slug: "dvf".to_string(),
        schema_version: 1,
        format: "csv".to_string(),
    })
}
