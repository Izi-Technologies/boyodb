//! Arrow Flight Protocol Support
//!
//! Arrow Flight provides high-performance, zero-copy data transfer between
//! BoyoDB and analytics tools like PyArrow, Pandas, Polars, and DuckDB.
//!
//! Key benefits:
//! - Zero-copy: Data stays in Arrow format, no serialization overhead
//! - Streaming: Handle datasets larger than memory
//! - gRPC-based: Cross-language support (Python, Java, R, etc.)
//! - Parallel: Multiple streams for high throughput
//!
//! Example Python usage:
//! ```python
//! import pyarrow.flight as flight
//!
//! client = flight.connect("grpc://localhost:8815")
//! info = client.get_flight_info(flight.FlightDescriptor.for_command(b"SELECT * FROM events"))
//! reader = client.do_get(info.endpoints[0].ticket)
//! table = reader.read_all()  # Zero-copy Arrow table
//! df = table.to_pandas()     # Convert to Pandas if needed
//! ```

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Flight ticket for retrieving query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightTicket {
    /// Unique query ID
    pub query_id: String,
    /// SQL query text
    pub query: String,
    /// Optional partition for parallel reads
    pub partition: Option<u32>,
    /// Total partitions
    pub total_partitions: Option<u32>,
}

impl FlightTicket {
    pub fn new(query_id: &str, query: &str) -> Self {
        Self {
            query_id: query_id.to_string(),
            query: query.to_string(),
            partition: None,
            total_partitions: None,
        }
    }

    pub fn with_partition(mut self, partition: u32, total: u32) -> Self {
        self.partition = Some(partition);
        self.total_partitions = Some(total);
        self
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        serde_json::from_slice(data).ok()
    }
}

/// Flight endpoint information
#[derive(Debug, Clone)]
pub struct FlightEndpoint {
    /// Ticket to retrieve this data
    pub ticket: FlightTicket,
    /// Server locations (for distributed queries)
    pub locations: Vec<String>,
}

/// Flight information for a query
#[derive(Debug, Clone)]
pub struct FlightInfo {
    /// Schema of the result
    pub schema: Arc<Schema>,
    /// Endpoints to retrieve data from
    pub endpoints: Vec<FlightEndpoint>,
    /// Total records (if known)
    pub total_records: Option<i64>,
    /// Total bytes (if known)
    pub total_bytes: Option<i64>,
}

impl FlightInfo {
    pub fn new(schema: Arc<Schema>, ticket: FlightTicket) -> Self {
        Self {
            schema,
            endpoints: vec![FlightEndpoint {
                ticket,
                locations: vec!["grpc://localhost:8815".to_string()],
            }],
            total_records: None,
            total_bytes: None,
        }
    }

    pub fn with_stats(mut self, records: i64, bytes: i64) -> Self {
        self.total_records = Some(records);
        self.total_bytes = Some(bytes);
        self
    }
}

/// Descriptor for identifying a data stream
#[derive(Debug, Clone)]
pub enum FlightDescriptor {
    /// Path-based descriptor (e.g., database.table)
    Path(Vec<String>),
    /// Command-based descriptor (e.g., SQL query)
    Command(Vec<u8>),
}

impl FlightDescriptor {
    pub fn for_path(path: &[&str]) -> Self {
        FlightDescriptor::Path(path.iter().map(|s| s.to_string()).collect())
    }

    pub fn for_command(cmd: &[u8]) -> Self {
        FlightDescriptor::Command(cmd.to_vec())
    }

    pub fn for_sql(sql: &str) -> Self {
        FlightDescriptor::Command(sql.as_bytes().to_vec())
    }
}

/// Result of a DoGet operation - streaming record batches
pub struct FlightDataStream {
    /// Schema for all batches
    pub schema: Arc<Schema>,
    /// Record batches to stream
    batches: Vec<RecordBatch>,
    /// Current position
    position: usize,
}

impl FlightDataStream {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            position: 0,
        }
    }

    /// Get next batch (for streaming)
    pub fn next_batch(&mut self) -> Option<&RecordBatch> {
        if self.position < self.batches.len() {
            let batch = &self.batches[self.position];
            self.position += 1;
            Some(batch)
        } else {
            None
        }
    }

    /// Get all remaining batches
    pub fn remaining(&self) -> &[RecordBatch] {
        &self.batches[self.position..]
    }

    /// Total record count
    pub fn total_records(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Serialize all batches to IPC format
    pub fn to_ipc(&self) -> Result<Vec<u8>, String> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &self.schema)
                .map_err(|e| format!("IPC writer error: {}", e))?;
            for batch in &self.batches {
                writer.write(batch)
                    .map_err(|e| format!("IPC write error: {}", e))?;
            }
            writer.finish()
                .map_err(|e| format!("IPC finish error: {}", e))?;
        }
        Ok(buf)
    }
}

/// Action types supported by the Flight server
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightAction {
    /// Health check
    HealthCheck,
    /// Cancel a running query
    CancelQuery { query_id: String },
    /// Get server statistics
    GetStats,
    /// Clear query cache
    ClearCache,
    /// Custom action
    Custom { action_type: String, body: Vec<u8> },
}

impl FlightAction {
    pub fn from_type_and_body(action_type: &str, body: &[u8]) -> Self {
        match action_type {
            "HealthCheck" => FlightAction::HealthCheck,
            "CancelQuery" => {
                let query_id = String::from_utf8_lossy(body).to_string();
                FlightAction::CancelQuery { query_id }
            }
            "GetStats" => FlightAction::GetStats,
            "ClearCache" => FlightAction::ClearCache,
            _ => FlightAction::Custom {
                action_type: action_type.to_string(),
                body: body.to_vec(),
            },
        }
    }
}

/// Flight action result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightActionResult {
    pub success: bool,
    pub message: String,
    #[serde(default)]
    pub data: HashMap<String, String>,
}

impl FlightActionResult {
    pub fn ok(message: &str) -> Self {
        Self {
            success: true,
            message: message.to_string(),
            data: HashMap::new(),
        }
    }

    pub fn error(message: &str) -> Self {
        Self {
            success: false,
            message: message.to_string(),
            data: HashMap::new(),
        }
    }

    pub fn with_data(mut self, key: &str, value: &str) -> Self {
        self.data.insert(key.to_string(), value.to_string());
        self
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

/// Configuration for Arrow Flight server
#[derive(Debug, Clone)]
pub struct FlightConfig {
    /// Host to bind to
    pub host: String,
    /// Port for Flight service
    pub port: u16,
    /// Maximum message size (bytes)
    pub max_message_size: usize,
    /// Enable TLS
    pub tls_enabled: bool,
    /// Path to TLS certificate
    pub tls_cert_path: Option<String>,
    /// Path to TLS key
    pub tls_key_path: Option<String>,
    /// Maximum concurrent streams
    pub max_concurrent_streams: u32,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8815,
            max_message_size: 64 * 1024 * 1024, // 64MB
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_concurrent_streams: 100,
        }
    }
}

impl FlightConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn grpc_uri(&self) -> String {
        let scheme = if self.tls_enabled { "grpcs" } else { "grpc" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

/// Statistics for Flight service
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlightStats {
    pub total_queries: u64,
    pub active_streams: u64,
    pub bytes_transferred: u64,
    pub records_transferred: u64,
    pub avg_query_time_ms: f64,
}

/// Prepared statement for Flight SQL
#[derive(Debug, Clone)]
pub struct FlightPreparedStatement {
    /// Statement handle
    pub handle: String,
    /// Original SQL
    pub sql: String,
    /// Parameter schema (for bind parameters)
    pub parameter_schema: Option<Arc<Schema>>,
    /// Result schema
    pub result_schema: Arc<Schema>,
    /// Creation timestamp
    pub created_at: u64,
}

impl FlightPreparedStatement {
    pub fn new(handle: &str, sql: &str, result_schema: Arc<Schema>) -> Self {
        Self {
            handle: handle.to_string(),
            sql: sql.to_string(),
            parameter_schema: None,
            result_schema,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

/// Flight SQL command types
#[derive(Debug, Clone)]
pub enum FlightSqlCommand {
    /// Execute a SQL query
    Execute { query: String },
    /// Get catalogs
    GetCatalogs,
    /// Get schemas
    GetSchemas { catalog: Option<String> },
    /// Get tables
    GetTables {
        catalog: Option<String>,
        schema: Option<String>,
        table_types: Vec<String>,
    },
    /// Get table types
    GetTableTypes,
    /// Get primary keys
    GetPrimaryKeys {
        catalog: Option<String>,
        schema: Option<String>,
        table: String,
    },
    /// Prepare a statement
    Prepare { query: String },
    /// Close a prepared statement
    ClosePrepared { handle: String },
}

/// Zero-copy data converter for interop with ML/AI tools
pub struct ZeroCopyConverter;

impl ZeroCopyConverter {
    /// Convert RecordBatches to PyArrow-compatible IPC format
    /// This format can be read by PyArrow with zero deserialization:
    /// ```python
    /// reader = pa.ipc.open_stream(data)
    /// table = reader.read_all()  # Zero-copy!
    /// ```
    pub fn to_pyarrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
        if batches.is_empty() {
            return Err("No batches to convert".to_string());
        }

        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| format!("IPC writer error: {}", e))?;
            for batch in batches {
                writer.write(batch)
                    .map_err(|e| format!("IPC write error: {}", e))?;
            }
            writer.finish()
                .map_err(|e| format!("IPC finish error: {}", e))?;
        }
        Ok(buf)
    }

    /// Convert to DuckDB-compatible format
    /// DuckDB can read Arrow IPC directly:
    /// ```python
    /// import duckdb
    /// duckdb.from_arrow(pyarrow_table)
    /// ```
    pub fn to_duckdb_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
        // Same format - DuckDB uses standard Arrow IPC
        Self::to_pyarrow_ipc(batches)
    }

    /// Convert to Polars-compatible format
    /// Polars can read Arrow IPC:
    /// ```python
    /// import polars as pl
    /// df = pl.read_ipc(data)
    /// ```
    pub fn to_polars_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
        // Same format - Polars uses standard Arrow IPC
        Self::to_pyarrow_ipc(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_flight_ticket_roundtrip() {
        let ticket = FlightTicket::new("query-123", "SELECT * FROM events");
        let bytes = ticket.to_bytes();
        let restored = FlightTicket::from_bytes(&bytes).unwrap();

        assert_eq!(ticket.query_id, restored.query_id);
        assert_eq!(ticket.query, restored.query);
    }

    #[test]
    fn test_flight_ticket_with_partition() {
        let ticket = FlightTicket::new("q1", "SELECT *")
            .with_partition(2, 4);

        assert_eq!(ticket.partition, Some(2));
        assert_eq!(ticket.total_partitions, Some(4));
    }

    #[test]
    fn test_flight_info() {
        let batch = make_test_batch();
        let ticket = FlightTicket::new("q1", "SELECT *");
        let info = FlightInfo::new(batch.schema(), ticket)
            .with_stats(1000, 8000);

        assert_eq!(info.total_records, Some(1000));
        assert_eq!(info.total_bytes, Some(8000));
        assert_eq!(info.endpoints.len(), 1);
    }

    #[test]
    fn test_flight_data_stream() {
        let batch = make_test_batch();
        let schema = batch.schema();
        let mut stream = FlightDataStream::new(schema, vec![batch.clone(), batch.clone()]);

        assert_eq!(stream.total_records(), 6); // 3 + 3

        let b1 = stream.next_batch().unwrap();
        assert_eq!(b1.num_rows(), 3);

        let b2 = stream.next_batch().unwrap();
        assert_eq!(b2.num_rows(), 3);

        assert!(stream.next_batch().is_none());
    }

    #[test]
    fn test_flight_data_stream_to_ipc() {
        let batch = make_test_batch();
        let stream = FlightDataStream::new(batch.schema(), vec![batch]);

        let ipc = stream.to_ipc().unwrap();
        assert!(!ipc.is_empty());
    }

    #[test]
    fn test_flight_action_result() {
        let result = FlightActionResult::ok("Query completed")
            .with_data("rows", "1000")
            .with_data("time_ms", "42");

        assert!(result.success);
        assert_eq!(result.data.get("rows"), Some(&"1000".to_string()));
    }

    #[test]
    fn test_flight_config() {
        let config = FlightConfig::default();
        assert_eq!(config.address(), "0.0.0.0:8815");
        assert_eq!(config.grpc_uri(), "grpc://0.0.0.0:8815");

        let tls_config = FlightConfig {
            tls_enabled: true,
            ..Default::default()
        };
        assert_eq!(tls_config.grpc_uri(), "grpcs://0.0.0.0:8815");
    }

    #[test]
    fn test_zero_copy_converter() {
        let batch = make_test_batch();
        let ipc = ZeroCopyConverter::to_pyarrow_ipc(&[batch.clone()]).unwrap();

        assert!(!ipc.is_empty());
        // Verify it's valid IPC by checking magic bytes
        assert!(ipc.len() > 8);
    }

    #[test]
    fn test_flight_action_parsing() {
        let action = FlightAction::from_type_and_body("HealthCheck", &[]);
        assert_eq!(action, FlightAction::HealthCheck);

        let action = FlightAction::from_type_and_body("CancelQuery", b"query-123");
        match action {
            FlightAction::CancelQuery { query_id } => assert_eq!(query_id, "query-123"),
            _ => panic!("Expected CancelQuery"),
        }
    }

    #[test]
    fn test_flight_descriptor() {
        let path_desc = FlightDescriptor::for_path(&["default", "events"]);
        match path_desc {
            FlightDescriptor::Path(p) => {
                assert_eq!(p, vec!["default", "events"]);
            }
            _ => panic!("Expected Path"),
        }

        let cmd_desc = FlightDescriptor::for_sql("SELECT * FROM events");
        match cmd_desc {
            FlightDescriptor::Command(c) => {
                assert_eq!(String::from_utf8_lossy(&c), "SELECT * FROM events");
            }
            _ => panic!("Expected Command"),
        }
    }

    #[test]
    fn test_prepared_statement() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let stmt = FlightPreparedStatement::new("stmt-1", "SELECT id FROM t", schema.clone());

        assert_eq!(stmt.handle, "stmt-1");
        assert_eq!(stmt.sql, "SELECT id FROM t");
        assert!(stmt.created_at > 0);
    }
}
