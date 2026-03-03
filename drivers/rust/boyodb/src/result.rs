use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;

/// Result of a query execution.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    /// List of rows as maps (for backward compatibility).
    pub rows: Vec<HashMap<String, Value>>,

    /// Column names.
    pub columns: Vec<String>,

    /// Arrow RecordBatches containing the result data.
    pub batches: Vec<RecordBatch>,

    /// Schema of the result (if available).
    pub schema: Option<SchemaRef>,

    /// Total number of rows across all batches.
    pub row_count: usize,

    /// Number of segments scanned.
    pub segments_scanned: i32,

    /// Bytes skipped due to pruning.
    pub data_skipped_bytes: u64,
}

impl QueryResult {
    /// Check if result is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Iterate over rows as HashMaps.
    pub fn iter(&self) -> impl Iterator<Item = &HashMap<String, Value>> {
        self.rows.iter()
    }

    /// Get the schema of the result.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Get all record batches.
    pub fn record_batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Concatenate all batches into a single RecordBatch.
    pub fn concat_batches(&self) -> Option<RecordBatch> {
        if self.batches.is_empty() {
            return None;
        }
        if self.batches.len() == 1 {
            return Some(self.batches[0].clone());
        }

        // Use arrow concat
        let schema = self.batches[0].schema();
        arrow::compute::concat_batches(&schema, &self.batches).ok()
    }

    /// Get a column by name from the first batch.
    pub fn column(&self, name: &str) -> Option<&arrow_array::ArrayRef> {
        if let Some(batch) = self.batches.first() {
            let idx = batch.schema().index_of(name).ok()?;
            Some(batch.column(idx))
        } else {
            None
        }
    }
}

/// Parse Arrow IPC data into QueryResult.
pub(crate) fn parse_arrow_ipc(data: &[u8], result: &mut QueryResult) {
    if data.is_empty() {
        return;
    }

    let cursor = Cursor::new(data);
    let reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("Failed to create Arrow IPC reader: {}", e);
            return;
        }
    };

    let schema = reader.schema();
    result.schema = Some(schema.clone());
    result.columns = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut batches = Vec::new();
    let mut total_rows = 0;

    for batch_result in reader {
        match batch_result {
            Ok(batch) => {
                total_rows += batch.num_rows();

                // Convert to HashMap rows for backward compatibility
                for row_idx in 0..batch.num_rows() {
                    let mut row = HashMap::new();
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        let value = array_value_to_json(col, row_idx);
                        row.insert(field.name().clone(), value);
                    }
                    result.rows.push(row);
                }

                batches.push(batch);
            }
            Err(e) => {
                tracing::warn!("Failed to read Arrow batch: {}", e);
                break;
            }
        }
    }

    result.batches = batches;
    result.row_count = total_rows;
}

/// Convert an Arrow array value at a given index to JSON.
fn array_value_to_json(array: &arrow_array::ArrayRef, idx: usize) -> Value {
    use arrow_array::*;

    if array.is_null(idx) {
        return Value::Null;
    }

    match array.data_type() {
        arrow_schema::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(idx))
        }
        arrow_schema::DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        arrow_schema::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        arrow_schema::DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        arrow_schema::DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        arrow_schema::DataType::Timestamp(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Value::Number(arr.value(idx).into())
        }
        arrow_schema::DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        _ => {
            // Fallback: use debug format
            Value::String(format!("{:?}", array.slice(idx, 1)))
        }
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
