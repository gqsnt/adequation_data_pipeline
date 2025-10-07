use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone)]
pub struct InferSchemaReq {
    pub uri: String,
    pub source_config: SourceCfg,
    #[serde(default = "default_preview_limit")]
    pub limit: usize,
}
fn default_preview_limit() -> usize {
    200
}


#[derive(Debug, Deserialize, Clone, Serialize)]
pub enum SourceCfg {
    Csv {
        #[serde(default = "default_delim")]
        delimiter: String,
        #[serde(default = "default_true")]
        has_header: bool,
        #[serde(default = "default_utf8")]
        encoding: String,
    },
    Parquet,
}

fn default_delim() -> String {
    ",".into()
}
fn default_true() -> bool {
    true
}
fn default_utf8() -> String {
    "utf-8".into()
}

#[derive(Debug, Deserialize)]
pub struct RunReq {
    pub project: ProjectCfg,
    pub datasets: (DataSet, DataSet),
    pub mapping: MappingCfg,
}

#[derive(Debug, Deserialize)]
pub struct ProjectCfg {
    pub namespace: String,
    pub warehouse_uri: String, // file:///warehouse
}

#[derive(Debug, Deserialize, Clone)]
pub struct MappingCfg {
    pub transforms: MappingIR,
    #[serde(default)]
    pub dq_rules: Vec<DqRule>,
}

#[derive(Debug, Deserialize)]
pub enum DataSet {
    Bronze {
        uri: String,
        source: SourceCfg,
        inner: InnerDataset,
    },
    Silver(InnerDataset),
    Gold(InnerDataset),
}

impl DataSet {
    pub fn name(&self) -> &str {
        match self {
            DataSet::Bronze { inner, .. } => &inner.name,
            DataSet::Silver(inner) => &inner.name,
            DataSet::Gold(inner) => &inner.name,
        }
    }

    pub fn primary_key(&self) -> &Vec<String> {
        match self {
            DataSet::Bronze { inner, .. } => &inner.primary_key,
            DataSet::Silver(inner) => &inner.primary_key,
            DataSet::Gold(inner) => &inner.primary_key,
        }
    }

    pub fn schema(&self) -> &ArrowLikeSchema {
        match self {
            DataSet::Bronze { inner, .. } => &inner.schema,
            DataSet::Silver(inner) => &inner.schema,
            DataSet::Gold(inner) => &inner.schema,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct InnerDataset {
    pub name: String, // obligatoire pour Silver/Gold, optionnel pour Bronze (si absent, run() infère le schéma)
    pub primary_key: Vec<String>,
    pub schema: ArrowLikeSchema,
}

#[derive(Debug, Serialize)]
pub struct InferSchemaResp {
    pub schema: ArrowLikeSchema,
    //pub rows: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ArrowLikeSchema {
    pub fields: Vec<Field>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Field {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DqRule {
    pub column: String,
    pub op: String, // ">", ">=", "==", "is_not_null", etc
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MappingIR {
    pub columns: Vec<TargetColumn>,
    #[serde(default)]
    pub filters: Vec<ExprIR>,
}
#[derive(Debug, Deserialize, Clone)]
pub struct TargetColumn {
    pub target: String,
    pub expr: ExprIR,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum ExprIR {
    Col {
        col: String,
    },
    Lit {
        lit: serde_json::Value,
    },
    Call {
        fn_: String,
        #[serde(default)]
        args: Vec<ExprIR>,
        #[serde(default)]
        to: Option<String>,
        #[serde(default)]
        fmt: Option<String>,
        #[serde(default)]
        len: Option<usize>,
        #[serde(default)]
        pred: Option<Box<ExprIR>>,
        #[serde(default)]
        then: Option<Box<ExprIR>>,
        #[serde(default)]
        r#else: Option<Box<ExprIR>>,
    },
}

/// --- Responses ---

#[derive(Debug, Serialize)]
pub struct RunResp {
    pub snapshot: String,
    pub ori_rows: i64,
    pub dest_rows: i64,
    pub rejected_rows: i64,
    #[serde(default)]
    pub error_samples: Vec<ErrorSample>,
    #[serde(default)]
    pub dq_summary: Vec<DqSummaryItem>,
    #[serde(default)]
    pub logs: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct DqSummaryItem {
    pub rule_code: String, // e.g., DQ_valeur_fonciere_>
    pub violations: i64,
    pub checked_rows: i64,
}

#[derive(Debug, Serialize)]
pub struct ErrorSample {
    pub reason_code: String,
    pub message: String,
    pub row_no: Option<i64>,
    pub source_values: serde_json::Value,
}
