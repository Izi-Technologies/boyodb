//! Change Data Capture (CDC)
//!
//! Native CDC with Debezium-compatible output format for real-time data streaming.
//! Captures INSERT, UPDATE, DELETE events and publishes them to sinks.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// CDC event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CdcEventType {
    /// Row created
    Create,
    /// Row updated
    Update,
    /// Row deleted
    Delete,
    /// Initial snapshot read
    Read,
    /// Table truncated
    Truncate,
    /// Schema change
    Schema,
}

impl CdcEventType {
    pub fn op_code(&self) -> &'static str {
        match self {
            CdcEventType::Create => "c",
            CdcEventType::Update => "u",
            CdcEventType::Delete => "d",
            CdcEventType::Read => "r",
            CdcEventType::Truncate => "t",
            CdcEventType::Schema => "s",
        }
    }
}

/// CDC event (Debezium-compatible format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Event schema (Debezium envelope)
    pub schema: Option<CdcSchema>,
    /// Event payload
    pub payload: CdcPayload,
}

/// CDC schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSchema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<CdcSchemaField>,
    pub optional: bool,
    pub name: String,
}

/// Schema field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSchemaField {
    #[serde(rename = "type")]
    pub field_type: String,
    pub optional: bool,
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<CdcSchemaField>>,
}

/// CDC payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcPayload {
    /// Row data before change (for UPDATE/DELETE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<HashMap<String, serde_json::Value>>,
    /// Row data after change (for INSERT/UPDATE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<HashMap<String, serde_json::Value>>,
    /// Source metadata
    pub source: CdcSource,
    /// Operation type
    pub op: String,
    /// Transaction metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<CdcTransaction>,
    /// Timestamp in milliseconds
    pub ts_ms: i64,
}

/// Source metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSource {
    /// Connector version
    pub version: String,
    /// Connector name
    pub connector: String,
    /// Server name
    pub name: String,
    /// Timestamp in milliseconds
    pub ts_ms: i64,
    /// Snapshot status
    pub snapshot: String,
    /// Database name
    pub db: String,
    /// Schema name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Transaction ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txid: Option<u64>,
    /// Log sequence number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

/// Transaction metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcTransaction {
    /// Transaction ID
    pub id: String,
    /// Total order within transaction
    pub total_order: u64,
    /// Data collection order
    pub data_collection_order: u64,
}

/// CDC connector configuration
#[derive(Debug, Clone)]
pub struct CdcConnectorConfig {
    /// Connector name
    pub name: String,
    /// Server name (used in source metadata)
    pub server_name: String,
    /// Tables to capture (database.table patterns)
    pub tables: Vec<String>,
    /// Include schema in events
    pub include_schema: bool,
    /// Snapshot mode
    pub snapshot_mode: SnapshotMode,
    /// Output format
    pub output_format: OutputFormat,
    /// Tombstone on delete (emit null for delete key)
    pub tombstones_on_delete: bool,
    /// Decimal handling mode
    pub decimal_handling: DecimalHandling,
}

/// Snapshot mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Take initial snapshot
    Initial,
    /// No snapshot, start from current position
    Never,
    /// Snapshot only when no offset exists
    WhenNeeded,
    /// Schema only, no data snapshot
    SchemaOnly,
}

/// Output format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// Debezium JSON envelope
    Debezium,
    /// Simple JSON (no envelope)
    Json,
    /// Avro
    Avro,
}

/// Decimal handling mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalHandling {
    /// Precise decimal
    Precise,
    /// Convert to double
    Double,
    /// Convert to string
    String,
}

impl Default for CdcConnectorConfig {
    fn default() -> Self {
        Self {
            name: "boyodb-connector".into(),
            server_name: "boyodb".into(),
            tables: vec!["*".into()],
            include_schema: true,
            snapshot_mode: SnapshotMode::Initial,
            output_format: OutputFormat::Debezium,
            tombstones_on_delete: true,
            decimal_handling: DecimalHandling::Precise,
        }
    }
}

/// CDC sink trait
pub trait CdcSink: Send + Sync {
    /// Send event to sink
    fn send(&self, event: &CdcEvent) -> Result<(), CdcError>;
    /// Flush pending events
    fn flush(&self) -> Result<(), CdcError>;
    /// Close the sink
    fn close(&self) -> Result<(), CdcError>;
}

/// In-memory CDC sink (for testing/debugging)
pub struct MemorySink {
    events: RwLock<VecDeque<CdcEvent>>,
    max_events: usize,
}

impl MemorySink {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(max_events)),
            max_events,
        }
    }

    pub fn events(&self) -> Vec<CdcEvent> {
        self.events.read().iter().cloned().collect()
    }

    pub fn drain(&self) -> Vec<CdcEvent> {
        self.events.write().drain(..).collect()
    }
}

impl CdcSink for MemorySink {
    fn send(&self, event: &CdcEvent) -> Result<(), CdcError> {
        let mut events = self.events.write();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event.clone());
        Ok(())
    }

    fn flush(&self) -> Result<(), CdcError> {
        Ok(())
    }

    fn close(&self) -> Result<(), CdcError> {
        Ok(())
    }
}

/// CDC connector
pub struct CdcConnector {
    /// Connector configuration
    config: CdcConnectorConfig,
    /// Registered sinks
    sinks: RwLock<Vec<Arc<dyn CdcSink>>>,
    /// Current LSN position
    lsn: RwLock<u64>,
    /// Running state
    running: RwLock<bool>,
    /// Statistics
    stats: RwLock<CdcStats>,
}

/// CDC statistics
#[derive(Debug, Default, Clone)]
pub struct CdcStats {
    pub events_captured: u64,
    pub events_sent: u64,
    pub events_failed: u64,
    pub creates: u64,
    pub updates: u64,
    pub deletes: u64,
    pub truncates: u64,
    pub last_event_ts_ms: i64,
}

impl CdcConnector {
    pub fn new(config: CdcConnectorConfig) -> Self {
        Self {
            config,
            sinks: RwLock::new(Vec::new()),
            lsn: RwLock::new(0),
            running: RwLock::new(false),
            stats: RwLock::new(CdcStats::default()),
        }
    }

    /// Register a sink
    pub fn add_sink(&self, sink: Arc<dyn CdcSink>) {
        self.sinks.write().push(sink);
    }

    /// Start the connector
    pub fn start(&self) {
        *self.running.write() = true;
    }

    /// Stop the connector
    pub fn stop(&self) {
        *self.running.write() = false;
    }

    /// Check if connector is running
    pub fn is_running(&self) -> bool {
        *self.running.read()
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        *self.lsn.read()
    }

    /// Get statistics
    pub fn stats(&self) -> CdcStats {
        self.stats.read().clone()
    }

    /// Emit a change event
    pub fn emit_change(
        &self,
        event_type: CdcEventType,
        database: &str,
        table: &str,
        before: Option<HashMap<String, serde_json::Value>>,
        after: Option<HashMap<String, serde_json::Value>>,
        transaction_id: Option<u64>,
    ) -> Result<(), CdcError> {
        if !self.is_running() {
            return Err(CdcError::ConnectorNotRunning);
        }

        // Check if table is included
        if !self.should_capture(database, table) {
            return Ok(());
        }

        // Increment LSN
        let lsn = {
            let mut lsn = self.lsn.write();
            *lsn += 1;
            *lsn
        };

        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Build event
        let event = CdcEvent {
            schema: if self.config.include_schema {
                Some(self.build_schema(event_type, table))
            } else {
                None
            },
            payload: CdcPayload {
                before,
                after,
                source: CdcSource {
                    version: "0.1.0".into(),
                    connector: "boyodb".into(),
                    name: self.config.server_name.clone(),
                    ts_ms,
                    snapshot: "false".into(),
                    db: database.into(),
                    schema: None,
                    table: table.into(),
                    txid: transaction_id,
                    lsn: Some(lsn),
                },
                op: event_type.op_code().into(),
                transaction: transaction_id.map(|id| CdcTransaction {
                    id: format!("{}", id),
                    total_order: lsn,
                    data_collection_order: 1,
                }),
                ts_ms,
            },
        };

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.events_captured += 1;
            stats.last_event_ts_ms = ts_ms;
            match event_type {
                CdcEventType::Create => stats.creates += 1,
                CdcEventType::Update => stats.updates += 1,
                CdcEventType::Delete => stats.deletes += 1,
                CdcEventType::Truncate => stats.truncates += 1,
                _ => {}
            }
        }

        // Send to sinks
        let sinks = self.sinks.read();
        for sink in sinks.iter() {
            match sink.send(&event) {
                Ok(_) => {
                    self.stats.write().events_sent += 1;
                }
                Err(_) => {
                    self.stats.write().events_failed += 1;
                }
            }
        }

        // Send tombstone for deletes if configured
        if event_type == CdcEventType::Delete && self.config.tombstones_on_delete {
            let tombstone = CdcEvent {
                schema: None,
                payload: CdcPayload {
                    before: None,
                    after: None,
                    source: CdcSource {
                        version: "0.1.0".into(),
                        connector: "boyodb".into(),
                        name: self.config.server_name.clone(),
                        ts_ms,
                        snapshot: "false".into(),
                        db: database.into(),
                        schema: None,
                        table: table.into(),
                        txid: transaction_id,
                        lsn: Some(lsn),
                    },
                    op: "d".into(),
                    transaction: None,
                    ts_ms,
                },
            };

            for sink in sinks.iter() {
                let _ = sink.send(&tombstone);
            }
        }

        Ok(())
    }

    /// Flush all sinks
    pub fn flush(&self) -> Result<(), CdcError> {
        let sinks = self.sinks.read();
        for sink in sinks.iter() {
            sink.flush()?;
        }
        Ok(())
    }

    fn should_capture(&self, database: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", database, table);

        for pattern in &self.config.tables {
            if pattern == "*" {
                return true;
            }
            if pattern == &full_name {
                return true;
            }
            if pattern.ends_with(".*") {
                let db_prefix = &pattern[..pattern.len() - 2];
                if database == db_prefix {
                    return true;
                }
            }
        }

        false
    }

    fn build_schema(&self, _event_type: CdcEventType, _table: &str) -> CdcSchema {
        CdcSchema {
            schema_type: "struct".into(),
            fields: vec![
                CdcSchemaField {
                    field_type: "struct".into(),
                    optional: true,
                    field: "before".into(),
                    name: None,
                    fields: None,
                },
                CdcSchemaField {
                    field_type: "struct".into(),
                    optional: true,
                    field: "after".into(),
                    name: None,
                    fields: None,
                },
                CdcSchemaField {
                    field_type: "struct".into(),
                    optional: false,
                    field: "source".into(),
                    name: Some("io.debezium.connector.boyodb.Source".into()),
                    fields: None,
                },
                CdcSchemaField {
                    field_type: "string".into(),
                    optional: false,
                    field: "op".into(),
                    name: None,
                    fields: None,
                },
                CdcSchemaField {
                    field_type: "int64".into(),
                    optional: true,
                    field: "ts_ms".into(),
                    name: None,
                    fields: None,
                },
            ],
            optional: false,
            name: "io.debezium.connector.boyodb.Envelope".into(),
        }
    }
}

/// CDC error
#[derive(Debug, Clone)]
pub enum CdcError {
    ConnectorNotRunning,
    SinkError(String),
    SerializationError(String),
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectorNotRunning => write!(f, "CDC connector is not running"),
            Self::SinkError(s) => write!(f, "Sink error: {}", s),
            Self::SerializationError(s) => write!(f, "Serialization error: {}", s),
        }
    }
}

impl std::error::Error for CdcError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_connector() {
        let config = CdcConnectorConfig::default();
        let connector = CdcConnector::new(config);

        let sink = Arc::new(MemorySink::new(100));
        connector.add_sink(sink.clone());
        connector.start();

        // Emit events
        let mut row = HashMap::new();
        row.insert("id".into(), serde_json::json!(1));
        row.insert("name".into(), serde_json::json!("Alice"));

        connector
            .emit_change(
                CdcEventType::Create,
                "mydb",
                "users",
                None,
                Some(row.clone()),
                Some(1),
            )
            .unwrap();

        connector
            .emit_change(
                CdcEventType::Update,
                "mydb",
                "users",
                Some(row.clone()),
                Some({
                    let mut new_row = row.clone();
                    new_row.insert("name".into(), serde_json::json!("Alice Smith"));
                    new_row
                }),
                Some(2),
            )
            .unwrap();

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload.op, "c");
        assert_eq!(events[1].payload.op, "u");
    }

    #[test]
    fn test_table_filtering() {
        let config = CdcConnectorConfig {
            tables: vec!["mydb.*".into()],
            ..Default::default()
        };
        let connector = CdcConnector::new(config);

        let sink = Arc::new(MemorySink::new(100));
        connector.add_sink(sink.clone());
        connector.start();

        // This should be captured
        connector
            .emit_change(
                CdcEventType::Create,
                "mydb",
                "users",
                None,
                Some(HashMap::new()),
                None,
            )
            .unwrap();

        // This should not be captured
        connector
            .emit_change(
                CdcEventType::Create,
                "otherdb",
                "users",
                None,
                Some(HashMap::new()),
                None,
            )
            .unwrap();

        let events = sink.events();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_cdc_stats() {
        let connector = CdcConnector::new(CdcConnectorConfig::default());
        let sink = Arc::new(MemorySink::new(100));
        connector.add_sink(sink);
        connector.start();

        for i in 0..5 {
            connector
                .emit_change(
                    CdcEventType::Create,
                    "db",
                    "table",
                    None,
                    Some(HashMap::new()),
                    Some(i),
                )
                .unwrap();
        }

        let stats = connector.stats();
        assert_eq!(stats.events_captured, 5);
        assert_eq!(stats.creates, 5);
    }
}
