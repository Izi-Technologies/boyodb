//! CDC to Data Lakes
//!
//! Direct CDC to Delta Lake/Iceberg format with schema evolution in CDC streams.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// CDC Event Types
// ============================================================================

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Snapshot,
    SchemaChange,
}

/// CDC event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Unique event ID
    pub event_id: String,
    /// Source database
    pub database: String,
    /// Source table
    pub table: String,
    /// Operation type
    pub operation: CdcOperation,
    /// Event timestamp (source)
    pub timestamp: u64,
    /// Transaction ID
    pub transaction_id: Option<String>,
    /// Log sequence number
    pub lsn: u64,
    /// Before image (for updates/deletes)
    pub before: Option<HashMap<String, CdcValue>>,
    /// After image (for inserts/updates)
    pub after: Option<HashMap<String, CdcValue>>,
    /// Schema version
    pub schema_version: u32,
    /// Schema definition
    pub schema: Option<CdcSchema>,
}

/// CDC value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CdcValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal { value: String, precision: u32, scale: u32 },
    Date(i32),
    Timestamp(i64),
    TimestampTz(i64, String),
    Uuid(String),
    Json(String),
    Array(Vec<CdcValue>),
    Map(HashMap<String, CdcValue>),
}

/// CDC schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcSchema {
    pub version: u32,
    pub columns: Vec<CdcColumn>,
    pub primary_key: Vec<String>,
}

/// CDC column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcColumn {
    pub name: String,
    pub data_type: CdcDataType,
    pub nullable: bool,
    pub default: Option<String>,
}

/// CDC data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CdcDataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u32, scale: u32 },
    String,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Uuid,
    Json,
    Array(Box<CdcDataType>),
    Map { key: Box<CdcDataType>, value: Box<CdcDataType> },
    Struct(Vec<CdcColumn>),
}

// ============================================================================
// Lake Sink Configuration
// ============================================================================

/// Target lake format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LakeFormat {
    DeltaLake,
    Iceberg,
    Hudi,
}

/// Lake sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakeSinkConfig {
    /// Sink name
    pub name: String,
    /// Target format
    pub format: LakeFormat,
    /// Storage location
    pub storage: LakeStorage,
    /// Table name in lake
    pub table_name: String,
    /// Partition columns
    pub partition_by: Vec<String>,
    /// Enable schema evolution
    pub schema_evolution: bool,
    /// Merge on columns (for upserts)
    pub merge_keys: Vec<String>,
    /// Write mode
    pub write_mode: WriteMode,
    /// Batch settings
    pub batch: BatchConfig,
    /// Compaction settings
    pub compaction: CompactionConfig,
}

/// Lake storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LakeStorage {
    S3 {
        bucket: String,
        prefix: String,
        region: String,
    },
    Gcs {
        bucket: String,
        prefix: String,
    },
    Azure {
        container: String,
        prefix: String,
    },
    Local {
        path: String,
    },
}

/// Write mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteMode {
    /// Append only
    Append,
    /// Merge based on primary key
    Merge,
    /// Replace partitions
    Overwrite,
}

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size in records
    pub max_records: usize,
    /// Maximum batch size in bytes
    pub max_bytes: usize,
    /// Maximum batch age in milliseconds
    pub max_age_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_records: 10000,
            max_bytes: 100 * 1024 * 1024, // 100MB
            max_age_ms: 60000,            // 1 minute
        }
    }
}

/// Compaction configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Enable auto-compaction
    pub enabled: bool,
    /// Minimum number of files to trigger compaction
    pub min_files: usize,
    /// Target file size in bytes
    pub target_file_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_files: 10,
            target_file_size: 128 * 1024 * 1024, // 128MB
        }
    }
}

// ============================================================================
// Delta Lake Writer
// ============================================================================

/// Delta Lake table writer
pub struct DeltaLakeWriter {
    config: LakeSinkConfig,
    /// Current schema
    schema: RwLock<Option<CdcSchema>>,
    /// Transaction log version
    version: AtomicU64,
    /// Pending records
    pending: RwLock<Vec<CdcEvent>>,
    /// Statistics
    stats: LakeWriterStats,
}

impl DeltaLakeWriter {
    pub fn new(config: LakeSinkConfig) -> Self {
        Self {
            config,
            schema: RwLock::new(None),
            version: AtomicU64::new(0),
            pending: RwLock::new(Vec::new()),
            stats: LakeWriterStats::default(),
        }
    }

    /// Write CDC events to Delta Lake
    pub fn write(&self, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError> {
        let mut pending = self.pending.write();

        for event in events {
            // Handle schema changes
            if let Some(new_schema) = &event.schema {
                self.handle_schema_change(new_schema)?;
            }

            pending.push(event);
        }

        // Check if we should flush
        let should_flush = pending.len() >= self.config.batch.max_records
            || self.estimate_size(&pending) >= self.config.batch.max_bytes;

        if should_flush {
            drop(pending);
            return self.flush();
        }

        Ok(WriteResult {
            records_written: 0,
            bytes_written: 0,
            files_created: 0,
            version: self.version.load(Ordering::Relaxed),
        })
    }

    /// Flush pending records
    pub fn flush(&self) -> Result<WriteResult, LakeError> {
        let events: Vec<CdcEvent> = self.pending.write().drain(..).collect();

        if events.is_empty() {
            return Ok(WriteResult {
                records_written: 0,
                bytes_written: 0,
                files_created: 0,
                version: self.version.load(Ordering::Relaxed),
            });
        }

        let records_written = events.len();

        // Group by partition
        let partitioned = self.partition_events(&events);

        let mut files_created = 0;
        let mut bytes_written = 0;

        for (_partition_key, (partition, partition_events)) in partitioned {
            // Generate Parquet file content
            let file_content = self.generate_parquet(&partition_events)?;
            bytes_written += file_content.len();

            // Generate Delta log entry
            let add_file = DeltaAddFile {
                path: self.generate_file_path(&partition),
                partition_values: partition.clone(),
                size: file_content.len() as i64,
                modification_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                data_change: true,
                stats: Some(self.compute_stats(&partition_events)),
            };

            // Write commit (simplified)
            self.write_commit(vec![DeltaAction::Add(add_file)])?;
            files_created += 1;
        }

        self.version.fetch_add(1, Ordering::Relaxed);
        self.stats.records_written.fetch_add(records_written as u64, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(bytes_written as u64, Ordering::Relaxed);

        Ok(WriteResult {
            records_written,
            bytes_written,
            files_created,
            version: self.version.load(Ordering::Relaxed),
        })
    }

    fn handle_schema_change(&self, new_schema: &CdcSchema) -> Result<(), LakeError> {
        let mut current = self.schema.write();

        if let Some(current_schema) = current.as_ref() {
            if !self.config.schema_evolution {
                // Verify schemas match
                if !schemas_compatible(current_schema, new_schema) {
                    return Err(LakeError::SchemaEvolutionDisabled);
                }
            }

            // Generate schema evolution metadata action
            let metadata = DeltaMetadata {
                schema_string: serde_json::to_string(new_schema).unwrap_or_default(),
                partition_columns: self.config.partition_by.clone(),
                configuration: HashMap::new(),
            };

            self.write_commit(vec![DeltaAction::Metadata(metadata)])?;
        }

        *current = Some(new_schema.clone());
        Ok(())
    }

    fn partition_events(&self, events: &[CdcEvent]) -> HashMap<String, (HashMap<String, String>, Vec<CdcEvent>)> {
        let mut partitioned: HashMap<String, (HashMap<String, String>, Vec<CdcEvent>)> = HashMap::new();

        for event in events {
            let partition_values: HashMap<String, String> = self
                .config
                .partition_by
                .iter()
                .filter_map(|col| {
                    event.after.as_ref().and_then(|after| {
                        after.get(col).map(|v| (col.clone(), cdc_value_to_string(v)))
                    })
                })
                .collect();

            // Create a string key from partition values for hashing
            let partition_key = partition_values
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("/");

            partitioned
                .entry(partition_key)
                .or_insert_with(|| (partition_values, Vec::new()))
                .1
                .push(event.clone());
        }

        partitioned
    }

    fn generate_file_path(&self, partition: &HashMap<String, String>) -> String {
        let mut path = String::new();

        for (key, value) in partition {
            path.push_str(&format!("{}={}/", key, value));
        }

        path.push_str(&format!(
            "part-{:05}-{}.parquet",
            self.version.load(Ordering::Relaxed),
            generate_id()
        ));

        path
    }

    fn generate_parquet(&self, _events: &[CdcEvent]) -> Result<Vec<u8>, LakeError> {
        // Simplified - in production would use arrow/parquet crate
        Ok(vec![0u8; 1024]) // Placeholder
    }

    fn compute_stats(&self, events: &[CdcEvent]) -> String {
        let stats = DeltaStats {
            num_records: events.len() as i64,
            min_values: HashMap::new(),
            max_values: HashMap::new(),
            null_count: HashMap::new(),
        };
        serde_json::to_string(&stats).unwrap_or_default()
    }

    fn write_commit(&self, actions: Vec<DeltaAction>) -> Result<(), LakeError> {
        let commit = DeltaCommit {
            version: self.version.load(Ordering::Relaxed),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            actions,
        };

        // Serialize and write (simplified)
        let _json = serde_json::to_string(&commit).map_err(|e| LakeError::SerializationError(e.to_string()))?;

        Ok(())
    }

    fn estimate_size(&self, events: &[CdcEvent]) -> usize {
        events.len() * 256 // Rough estimate
    }

    /// Get writer statistics
    pub fn stats(&self) -> LakeWriterStatsSnapshot {
        LakeWriterStatsSnapshot {
            records_written: self.stats.records_written.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            current_version: self.version.load(Ordering::Relaxed),
            pending_records: self.pending.read().len(),
        }
    }
}

// Delta Lake structures
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaCommit {
    version: u64,
    timestamp: i64,
    actions: Vec<DeltaAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DeltaAction {
    Add(DeltaAddFile),
    Remove(DeltaRemoveFile),
    Metadata(DeltaMetadata),
    Protocol(DeltaProtocol),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaAddFile {
    path: String,
    partition_values: HashMap<String, String>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    stats: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaRemoveFile {
    path: String,
    deletion_timestamp: Option<i64>,
    data_change: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaMetadata {
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaProtocol {
    min_reader_version: i32,
    min_writer_version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaStats {
    num_records: i64,
    min_values: HashMap<String, String>,
    max_values: HashMap<String, String>,
    null_count: HashMap<String, i64>,
}

// ============================================================================
// Iceberg Writer
// ============================================================================

/// Iceberg table writer
pub struct IcebergWriter {
    config: LakeSinkConfig,
    /// Current schema
    schema: RwLock<Option<CdcSchema>>,
    /// Current snapshot ID
    snapshot_id: AtomicU64,
    /// Pending records
    pending: RwLock<Vec<CdcEvent>>,
    /// Statistics
    stats: LakeWriterStats,
}

impl IcebergWriter {
    pub fn new(config: LakeSinkConfig) -> Self {
        Self {
            config,
            schema: RwLock::new(None),
            snapshot_id: AtomicU64::new(0),
            pending: RwLock::new(Vec::new()),
            stats: LakeWriterStats::default(),
        }
    }

    /// Write CDC events to Iceberg
    pub fn write(&self, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError> {
        let mut pending = self.pending.write();

        for event in events {
            if let Some(new_schema) = &event.schema {
                self.handle_schema_change(new_schema)?;
            }
            pending.push(event);
        }

        let should_flush = pending.len() >= self.config.batch.max_records;

        if should_flush {
            drop(pending);
            return self.flush();
        }

        Ok(WriteResult {
            records_written: 0,
            bytes_written: 0,
            files_created: 0,
            version: self.snapshot_id.load(Ordering::Relaxed),
        })
    }

    /// Flush pending records
    pub fn flush(&self) -> Result<WriteResult, LakeError> {
        let events: Vec<CdcEvent> = self.pending.write().drain(..).collect();

        if events.is_empty() {
            return Ok(WriteResult {
                records_written: 0,
                bytes_written: 0,
                files_created: 0,
                version: self.snapshot_id.load(Ordering::Relaxed),
            });
        }

        let records_written = events.len();

        // Write data files
        let data_files = self.write_data_files(&events)?;
        let files_created = data_files.len();
        let bytes_written: usize = data_files.iter().map(|f| f.file_size_in_bytes as usize).sum();

        // Create new snapshot
        let new_snapshot_id = self.snapshot_id.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot = IcebergSnapshot {
            snapshot_id: new_snapshot_id as i64,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            summary: IcebergSummary {
                operation: "append".to_string(),
                added_data_files: files_created as i32,
                added_records: records_written as i64,
            },
            manifest_list: format!("manifests/snap-{}.avro", new_snapshot_id),
        };

        // Write manifest (simplified)
        self.write_manifest(&snapshot, &data_files)?;

        self.stats.records_written.fetch_add(records_written as u64, Ordering::Relaxed);
        self.stats.bytes_written.fetch_add(bytes_written as u64, Ordering::Relaxed);

        Ok(WriteResult {
            records_written,
            bytes_written,
            files_created,
            version: new_snapshot_id,
        })
    }

    fn handle_schema_change(&self, new_schema: &CdcSchema) -> Result<(), LakeError> {
        if !self.config.schema_evolution {
            if let Some(current) = self.schema.read().as_ref() {
                if !schemas_compatible(current, new_schema) {
                    return Err(LakeError::SchemaEvolutionDisabled);
                }
            }
        }

        *self.schema.write() = Some(new_schema.clone());
        Ok(())
    }

    fn write_data_files(&self, _events: &[CdcEvent]) -> Result<Vec<IcebergDataFile>, LakeError> {
        // Simplified - would write actual Parquet files
        Ok(vec![IcebergDataFile {
            file_path: format!("data/{}.parquet", generate_id()),
            file_format: "PARQUET".to_string(),
            record_count: _events.len() as i64,
            file_size_in_bytes: 1024,
            column_sizes: HashMap::new(),
            null_value_counts: HashMap::new(),
        }])
    }

    fn write_manifest(
        &self,
        _snapshot: &IcebergSnapshot,
        _files: &[IcebergDataFile],
    ) -> Result<(), LakeError> {
        // Simplified - would write Avro manifest
        Ok(())
    }

    /// Get writer statistics
    pub fn stats(&self) -> LakeWriterStatsSnapshot {
        LakeWriterStatsSnapshot {
            records_written: self.stats.records_written.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            current_version: self.snapshot_id.load(Ordering::Relaxed),
            pending_records: self.pending.read().len(),
        }
    }
}

// Iceberg structures
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergSnapshot {
    snapshot_id: i64,
    timestamp_ms: i64,
    summary: IcebergSummary,
    manifest_list: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergSummary {
    operation: String,
    added_data_files: i32,
    added_records: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IcebergDataFile {
    file_path: String,
    file_format: String,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
}

// ============================================================================
// CDC Lake Sink Manager
// ============================================================================

/// Unified CDC to Lake sink manager
pub struct CdcLakeSinkManager {
    /// Active sinks
    sinks: RwLock<HashMap<String, Arc<dyn LakeSink + Send + Sync>>>,
    /// Schema registry
    schemas: RwLock<HashMap<String, Vec<CdcSchema>>>,
}

/// Lake sink trait
pub trait LakeSink {
    fn write(&self, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError>;
    fn flush(&self) -> Result<WriteResult, LakeError>;
    fn stats(&self) -> LakeWriterStatsSnapshot;
}

impl LakeSink for DeltaLakeWriter {
    fn write(&self, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError> {
        self.write(events)
    }

    fn flush(&self) -> Result<WriteResult, LakeError> {
        self.flush()
    }

    fn stats(&self) -> LakeWriterStatsSnapshot {
        self.stats()
    }
}

impl LakeSink for IcebergWriter {
    fn write(&self, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError> {
        self.write(events)
    }

    fn flush(&self) -> Result<WriteResult, LakeError> {
        self.flush()
    }

    fn stats(&self) -> LakeWriterStatsSnapshot {
        self.stats()
    }
}

impl CdcLakeSinkManager {
    pub fn new() -> Self {
        Self {
            sinks: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new sink
    pub fn create_sink(&self, config: LakeSinkConfig) -> Result<(), LakeError> {
        let name = config.name.clone();

        let sink: Arc<dyn LakeSink + Send + Sync> = match config.format {
            LakeFormat::DeltaLake => Arc::new(DeltaLakeWriter::new(config)),
            LakeFormat::Iceberg => Arc::new(IcebergWriter::new(config)),
            LakeFormat::Hudi => {
                return Err(LakeError::UnsupportedFormat("Hudi".to_string()));
            }
        };

        self.sinks.write().insert(name, sink);
        Ok(())
    }

    /// Write events to a sink
    pub fn write(&self, sink_name: &str, events: Vec<CdcEvent>) -> Result<WriteResult, LakeError> {
        let sinks = self.sinks.read();
        let sink = sinks
            .get(sink_name)
            .ok_or_else(|| LakeError::SinkNotFound(sink_name.to_string()))?;
        sink.write(events)
    }

    /// Flush a sink
    pub fn flush(&self, sink_name: &str) -> Result<WriteResult, LakeError> {
        let sinks = self.sinks.read();
        let sink = sinks
            .get(sink_name)
            .ok_or_else(|| LakeError::SinkNotFound(sink_name.to_string()))?;
        sink.flush()
    }

    /// Flush all sinks
    pub fn flush_all(&self) -> Vec<(String, Result<WriteResult, LakeError>)> {
        let sinks = self.sinks.read();
        sinks
            .iter()
            .map(|(name, sink)| (name.clone(), sink.flush()))
            .collect()
    }

    /// Get sink statistics
    pub fn stats(&self, sink_name: &str) -> Option<LakeWriterStatsSnapshot> {
        self.sinks.read().get(sink_name).map(|s| s.stats())
    }

    /// Register schema for a table
    pub fn register_schema(&self, table_key: &str, schema: CdcSchema) {
        self.schemas
            .write()
            .entry(table_key.to_string())
            .or_insert_with(Vec::new)
            .push(schema);
    }

    /// Get schema history for a table
    pub fn get_schema_history(&self, table_key: &str) -> Vec<CdcSchema> {
        self.schemas
            .read()
            .get(table_key)
            .cloned()
            .unwrap_or_default()
    }

    /// List all sinks
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
    }

    /// Remove a sink
    pub fn remove_sink(&self, sink_name: &str) -> bool {
        self.sinks.write().remove(sink_name).is_some()
    }
}

impl Default for CdcLakeSinkManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Results and Errors
// ============================================================================

/// Write result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub records_written: usize,
    pub bytes_written: usize,
    pub files_created: usize,
    pub version: u64,
}

/// Lake writer statistics
#[derive(Default)]
struct LakeWriterStats {
    records_written: AtomicU64,
    bytes_written: AtomicU64,
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakeWriterStatsSnapshot {
    pub records_written: u64,
    pub bytes_written: u64,
    pub current_version: u64,
    pub pending_records: usize,
}

/// Lake errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LakeError {
    SinkNotFound(String),
    UnsupportedFormat(String),
    SchemaEvolutionDisabled,
    SchemaIncompatible(String),
    StorageError(String),
    SerializationError(String),
    WriteError(String),
}

impl std::fmt::Display for LakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LakeError::SinkNotFound(n) => write!(f, "Sink not found: {}", n),
            LakeError::UnsupportedFormat(fmt) => write!(f, "Unsupported format: {}", fmt),
            LakeError::SchemaEvolutionDisabled => write!(f, "Schema evolution disabled"),
            LakeError::SchemaIncompatible(msg) => write!(f, "Schema incompatible: {}", msg),
            LakeError::StorageError(e) => write!(f, "Storage error: {}", e),
            LakeError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            LakeError::WriteError(e) => write!(f, "Write error: {}", e),
        }
    }
}

impl std::error::Error for LakeError {}

// Helper functions
fn schemas_compatible(old: &CdcSchema, new: &CdcSchema) -> bool {
    // Check that all old columns exist in new schema (backward compatible)
    for old_col in &old.columns {
        if !new.columns.iter().any(|c| c.name == old_col.name) {
            return false;
        }
    }
    true
}

fn cdc_value_to_string(value: &CdcValue) -> String {
    match value {
        CdcValue::Null => "null".to_string(),
        CdcValue::Bool(b) => b.to_string(),
        CdcValue::Int32(i) => i.to_string(),
        CdcValue::Int64(i) => i.to_string(),
        CdcValue::Float32(f) => f.to_string(),
        CdcValue::Float64(f) => f.to_string(),
        CdcValue::String(s) => s.clone(),
        CdcValue::Date(d) => d.to_string(),
        CdcValue::Timestamp(t) => t.to_string(),
        _ => "complex".to_string(),
    }
}

fn generate_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("{:016x}", hasher.finish())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_writer() {
        let config = LakeSinkConfig {
            name: "test_sink".to_string(),
            format: LakeFormat::DeltaLake,
            storage: LakeStorage::Local {
                path: "/tmp/delta".to_string(),
            },
            table_name: "events".to_string(),
            partition_by: vec!["date".to_string()],
            schema_evolution: true,
            merge_keys: vec!["id".to_string()],
            write_mode: WriteMode::Append,
            batch: BatchConfig::default(),
            compaction: CompactionConfig::default(),
        };

        let writer = DeltaLakeWriter::new(config);

        let event = CdcEvent {
            event_id: "evt1".to_string(),
            database: "test".to_string(),
            table: "events".to_string(),
            operation: CdcOperation::Insert,
            timestamp: 1000,
            transaction_id: None,
            lsn: 1,
            before: None,
            after: Some(HashMap::from([
                ("id".to_string(), CdcValue::Int64(1)),
                ("date".to_string(), CdcValue::String("2025-01-01".to_string())),
            ])),
            schema_version: 1,
            schema: None,
        };

        let result = writer.write(vec![event]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_iceberg_writer() {
        let config = LakeSinkConfig {
            name: "test_iceberg".to_string(),
            format: LakeFormat::Iceberg,
            storage: LakeStorage::Local {
                path: "/tmp/iceberg".to_string(),
            },
            table_name: "events".to_string(),
            partition_by: vec![],
            schema_evolution: true,
            merge_keys: vec![],
            write_mode: WriteMode::Append,
            batch: BatchConfig {
                max_records: 2,
                ..Default::default()
            },
            compaction: CompactionConfig::default(),
        };

        let writer = IcebergWriter::new(config);

        let events: Vec<CdcEvent> = (0..3)
            .map(|i| CdcEvent {
                event_id: format!("evt{}", i),
                database: "test".to_string(),
                table: "events".to_string(),
                operation: CdcOperation::Insert,
                timestamp: 1000 + i as u64,
                transaction_id: None,
                lsn: i as u64,
                before: None,
                after: Some(HashMap::from([("id".to_string(), CdcValue::Int64(i))])),
                schema_version: 1,
                schema: None,
            })
            .collect();

        let result = writer.write(events);
        assert!(result.is_ok());

        // Should have flushed due to batch size
        let stats = writer.stats();
        assert!(stats.records_written > 0);
    }

    #[test]
    fn test_sink_manager() {
        let manager = CdcLakeSinkManager::new();

        let config = LakeSinkConfig {
            name: "my_sink".to_string(),
            format: LakeFormat::DeltaLake,
            storage: LakeStorage::Local {
                path: "/tmp/test".to_string(),
            },
            table_name: "test".to_string(),
            partition_by: vec![],
            schema_evolution: true,
            merge_keys: vec![],
            write_mode: WriteMode::Append,
            batch: BatchConfig::default(),
            compaction: CompactionConfig::default(),
        };

        assert!(manager.create_sink(config).is_ok());
        assert!(manager.list_sinks().contains(&"my_sink".to_string()));
    }
}
