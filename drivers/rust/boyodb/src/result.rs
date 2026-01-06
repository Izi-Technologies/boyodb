use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Result of a query execution.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    /// List of rows as maps.
    pub rows: Vec<HashMap<String, Value>>,

    /// Column names.
    pub columns: Vec<String>,

    /// Number of segments scanned.
    pub segments_scanned: i32,

    /// Bytes skipped due to pruning.
    pub data_skipped_bytes: u64,
}

impl QueryResult {
    /// Number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Check if result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Iterate over rows.
    pub fn iter(&self) -> impl Iterator<Item = &HashMap<String, Value>> {
        self.rows.iter()
    }
}

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Database name.
    pub database: String,

    /// Table name.
    pub name: String,

    /// Schema JSON (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_json: Option<String>,
}

/// Internal response structure.
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    pub status: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub ipc_base64: Option<String>,
    #[serde(default)]
    pub ipc_len: Option<u64>,
    #[serde(default)]
    pub ipc_streaming: Option<bool>,
    #[serde(skip)]
    pub ipc_bytes: Option<Vec<u8>>,
    #[serde(default)]
    pub segments_scanned: Option<i32>,
    #[serde(default)]
    pub data_skipped_bytes: Option<u64>,
    #[serde(default)]
    pub databases: Option<Vec<String>>,
    #[serde(default)]
    pub tables: Option<Vec<TableInfo>>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub explain_plan: Option<Value>,
    #[serde(default)]
    pub metrics: Option<Value>,
    #[serde(default)]
    pub prepared_id: Option<String>,
}
