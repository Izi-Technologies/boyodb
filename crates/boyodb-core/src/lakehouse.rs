//! Lakehouse Format Support - Delta Lake & Apache Iceberg Integration
//!
//! Provides native read/write support for open table formats:
//! - Delta Lake: ACID transactions, time travel, schema evolution
//! - Apache Iceberg: Hidden partitioning, schema evolution, branching

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Common Types
// ============================================================================

/// Error type for lakehouse operations
#[derive(Debug, Clone)]
pub enum LakehouseError {
    /// Table not found
    TableNotFound(String),
    /// Version not found
    VersionNotFound(i64),
    /// Schema mismatch
    SchemaMismatch(String),
    /// Conflict during commit
    CommitConflict(String),
    /// I/O error
    IoError(String),
    /// Invalid table format
    InvalidFormat(String),
    /// Snapshot not found
    SnapshotNotFound(i64),
    /// Partition error
    PartitionError(String),
    /// Metadata error
    MetadataError(String),
}

impl std::fmt::Display for LakehouseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "table not found: {}", t),
            Self::VersionNotFound(v) => write!(f, "version not found: {}", v),
            Self::SchemaMismatch(msg) => write!(f, "schema mismatch: {}", msg),
            Self::CommitConflict(msg) => write!(f, "commit conflict: {}", msg),
            Self::IoError(msg) => write!(f, "I/O error: {}", msg),
            Self::InvalidFormat(msg) => write!(f, "invalid format: {}", msg),
            Self::SnapshotNotFound(id) => write!(f, "snapshot not found: {}", id),
            Self::PartitionError(msg) => write!(f, "partition error: {}", msg),
            Self::MetadataError(msg) => write!(f, "metadata error: {}", msg),
        }
    }
}

impl std::error::Error for LakehouseError {}

/// Supported table formats
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableFormat {
    Delta,
    Iceberg,
}

/// Data file format
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
    Orc,
    Avro,
}

/// Column type for lakehouse schemas
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Decimal { precision: u8, scale: u8 },
    Uuid,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Struct(Vec<(String, ColumnType)>),
}

/// Column definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub id: i32,
    pub name: String,
    pub data_type: ColumnType,
    pub required: bool,
    pub doc: Option<String>,
}

/// Table schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub schema_id: i32,
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            schema_id: 0,
            columns,
        }
    }

    pub fn with_id(mut self, id: i32) -> Self {
        self.schema_id = id;
        self
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

// ============================================================================
// Delta Lake Support
// ============================================================================

/// Delta Lake table configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaConfig {
    /// Table location (path or URI)
    pub location: String,
    /// Checkpoint interval
    pub checkpoint_interval: u64,
    /// Enable deletion vectors
    pub deletion_vectors_enabled: bool,
    /// Data retention in hours
    pub log_retention_hours: u64,
    /// Deleted file retention in hours
    pub deleted_file_retention_hours: u64,
    /// Enable auto-optimize
    pub auto_optimize: bool,
    /// Target file size in bytes
    pub target_file_size: u64,
}

impl Default for DeltaConfig {
    fn default() -> Self {
        Self {
            location: String::new(),
            checkpoint_interval: 10,
            deletion_vectors_enabled: true,
            log_retention_hours: 168, // 7 days
            deleted_file_retention_hours: 24,
            auto_optimize: true,
            target_file_size: 128 * 1024 * 1024, // 128 MB
        }
    }
}

/// Delta Lake action types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaAction {
    /// Add a new file
    Add(AddFileAction),
    /// Remove a file
    Remove(RemoveFileAction),
    /// Metadata change
    Metadata(MetadataAction),
    /// Protocol version
    Protocol(ProtocolAction),
    /// Transaction commit info
    CommitInfo(CommitInfoAction),
    /// CDC (Change Data Capture)
    Cdc(CdcFileAction),
    /// Domain metadata (user-defined)
    DomainMetadata(DomainMetadataAction),
}

/// Add file action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddFileAction {
    pub path: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub modification_time: i64,
    pub data_change: bool,
    pub stats: Option<String>, // JSON stats
    pub tags: Option<HashMap<String, String>>,
    pub deletion_vector: Option<DeletionVector>,
    pub base_row_id: Option<i64>,
    pub default_row_commit_version: Option<i64>,
}

/// Remove file action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoveFileAction {
    pub path: String,
    pub deletion_timestamp: Option<i64>,
    pub data_change: bool,
    pub extended_file_metadata: bool,
    pub partition_values: Option<HashMap<String, String>>,
    pub size: Option<i64>,
    pub deletion_vector: Option<DeletionVector>,
}

/// Deletion vector for row-level deletes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeletionVector {
    pub storage_type: String,
    pub path_or_inline_dv: String,
    pub offset: Option<i32>,
    pub size_in_bytes: i32,
    pub cardinality: i64,
}

/// Metadata action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataAction {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub format: FileFormatSpec,
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    pub created_time: Option<i64>,
    pub configuration: HashMap<String, String>,
}

/// File format specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileFormatSpec {
    pub provider: String,
    pub options: HashMap<String, String>,
}

/// Protocol action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProtocolAction {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Option<Vec<String>>,
    pub writer_features: Option<Vec<String>>,
}

/// Commit info action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitInfoAction {
    pub version: Option<i64>,
    pub timestamp: i64,
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub operation: String,
    pub operation_parameters: HashMap<String, String>,
    pub job: Option<JobInfo>,
    pub notebook: Option<NotebookInfo>,
    pub cluster_id: Option<String>,
    pub read_version: Option<i64>,
    pub isolation_level: Option<String>,
    pub is_blind_append: Option<bool>,
    pub operation_metrics: Option<HashMap<String, String>>,
    pub engine_info: Option<String>,
}

/// Job information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: String,
    pub job_name: Option<String>,
    pub run_id: Option<String>,
    pub job_owner_name: Option<String>,
    pub trigger_type: Option<String>,
}

/// Notebook information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotebookInfo {
    pub notebook_id: String,
}

/// CDC file action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcFileAction {
    pub path: String,
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub tags: Option<HashMap<String, String>>,
}

/// Domain metadata action
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainMetadataAction {
    pub domain: String,
    pub configuration: String,
    pub removed: bool,
}

/// Delta Lake transaction log
#[derive(Debug)]
pub struct DeltaLog {
    config: DeltaConfig,
    version: i64,
    actions: Vec<DeltaAction>,
    schema: Option<Schema>,
    partition_columns: Vec<String>,
}

impl DeltaLog {
    pub fn new(config: DeltaConfig) -> Self {
        Self {
            config,
            version: -1,
            actions: Vec::new(),
            schema: None,
            partition_columns: Vec::new(),
        }
    }

    /// Get current version
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Get current schema
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    /// Start a new transaction
    pub fn start_transaction(&self) -> DeltaTransaction {
        DeltaTransaction {
            version: self.version,
            actions: Vec::new(),
            app_id: None,
            read_predicates: Vec::new(),
        }
    }

    /// List active files
    pub fn active_files(&self) -> Vec<&AddFileAction> {
        self.actions
            .iter()
            .filter_map(|a| match a {
                DeltaAction::Add(add) => Some(add),
                _ => None,
            })
            .collect()
    }

    /// Commit a transaction
    pub fn commit(&mut self, txn: DeltaTransaction) -> Result<i64, LakehouseError> {
        // Optimistic concurrency check
        if txn.version != self.version {
            return Err(LakehouseError::CommitConflict(format!(
                "expected version {} but found {}",
                txn.version, self.version
            )));
        }

        self.version += 1;
        self.actions.extend(txn.actions);

        Ok(self.version)
    }

    /// Time travel to a specific version
    pub fn time_travel(&self, version: i64) -> Result<DeltaSnapshot, LakehouseError> {
        if version > self.version || version < 0 {
            return Err(LakehouseError::VersionNotFound(version));
        }

        // Build snapshot at version
        let mut files = Vec::new();
        let mut schema = None;

        for action in &self.actions {
            match action {
                DeltaAction::Add(add) => files.push(add.clone()),
                DeltaAction::Remove(remove) => {
                    files.retain(|f| f.path != remove.path);
                }
                DeltaAction::Metadata(meta) => {
                    // Parse schema from metadata
                    schema = Some(meta.clone());
                }
                _ => {}
            }
        }

        Ok(DeltaSnapshot {
            version,
            files,
            metadata: schema,
        })
    }

    /// Optimize the table (compaction)
    pub fn optimize(&mut self, predicate: Option<&str>) -> Result<OptimizeResult, LakehouseError> {
        let files_before = self.active_files().len();

        // Collect small files
        let small_files: Vec<_> = self.active_files()
            .iter()
            .filter(|f| (f.size as u64) < self.config.target_file_size / 4)
            .map(|f| f.path.clone())
            .collect();

        if small_files.is_empty() {
            return Ok(OptimizeResult {
                files_added: 0,
                files_removed: 0,
                bytes_added: 0,
                bytes_removed: 0,
            });
        }

        // In a real implementation, we would merge these files
        // For now, return a simulated result
        Ok(OptimizeResult {
            files_added: 1,
            files_removed: small_files.len(),
            bytes_added: self.config.target_file_size as i64,
            bytes_removed: small_files.len() as i64 * (self.config.target_file_size as i64 / 8),
        })
    }

    /// Vacuum old files
    pub fn vacuum(&mut self, retention_hours: Option<u64>) -> Result<VacuumResult, LakehouseError> {
        let retention = retention_hours.unwrap_or(self.config.deleted_file_retention_hours);
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - (retention as i64 * 3600 * 1000);

        let mut files_deleted = 0;
        let mut bytes_freed = 0;

        // Find removed files past retention
        for action in &self.actions {
            if let DeltaAction::Remove(remove) = action {
                if let Some(ts) = remove.deletion_timestamp {
                    if ts < cutoff {
                        files_deleted += 1;
                        bytes_freed += remove.size.unwrap_or(0);
                    }
                }
            }
        }

        Ok(VacuumResult {
            files_deleted,
            bytes_freed,
        })
    }
}

/// Delta Lake transaction
#[derive(Debug)]
pub struct DeltaTransaction {
    version: i64,
    actions: Vec<DeltaAction>,
    app_id: Option<String>,
    read_predicates: Vec<String>,
}

impl DeltaTransaction {
    /// Add a file
    pub fn add_file(&mut self, action: AddFileAction) {
        self.actions.push(DeltaAction::Add(action));
    }

    /// Remove a file
    pub fn remove_file(&mut self, action: RemoveFileAction) {
        self.actions.push(DeltaAction::Remove(action));
    }

    /// Update metadata
    pub fn update_metadata(&mut self, action: MetadataAction) {
        self.actions.push(DeltaAction::Metadata(action));
    }

    /// Set commit info
    pub fn set_commit_info(&mut self, operation: &str, params: HashMap<String, String>) {
        self.actions.push(DeltaAction::CommitInfo(CommitInfoAction {
            version: None,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            user_id: None,
            user_name: None,
            operation: operation.to_string(),
            operation_parameters: params,
            job: None,
            notebook: None,
            cluster_id: None,
            read_version: Some(self.version),
            isolation_level: Some("Serializable".to_string()),
            is_blind_append: Some(true),
            operation_metrics: None,
            engine_info: Some("BoyoDB".to_string()),
        }));
    }
}

/// Delta Lake snapshot
#[derive(Debug)]
pub struct DeltaSnapshot {
    pub version: i64,
    pub files: Vec<AddFileAction>,
    pub metadata: Option<MetadataAction>,
}

/// Optimize result
#[derive(Debug)]
pub struct OptimizeResult {
    pub files_added: usize,
    pub files_removed: usize,
    pub bytes_added: i64,
    pub bytes_removed: i64,
}

/// Vacuum result
#[derive(Debug)]
pub struct VacuumResult {
    pub files_deleted: usize,
    pub bytes_freed: i64,
}

// ============================================================================
// Apache Iceberg Support
// ============================================================================

/// Iceberg table configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Table location
    pub location: String,
    /// Catalog type
    pub catalog_type: CatalogType,
    /// Catalog URI
    pub catalog_uri: Option<String>,
    /// Warehouse path
    pub warehouse: Option<String>,
    /// Format version (1 or 2)
    pub format_version: i32,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            location: String::new(),
            catalog_type: CatalogType::Hadoop,
            catalog_uri: None,
            warehouse: None,
            format_version: 2,
        }
    }
}

/// Iceberg catalog types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogType {
    Hadoop,
    Hive,
    Rest,
    Glue,
    Nessie,
    Jdbc,
}

/// Iceberg partition field
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: PartitionTransform,
}

/// Partition transforms
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
    Void,
}

impl PartitionTransform {
    /// Apply transform to a value
    pub fn apply(&self, value: &PartitionValue) -> PartitionValue {
        match (self, value) {
            (PartitionTransform::Identity, v) => v.clone(),
            (PartitionTransform::Year, PartitionValue::Timestamp(ts)) => {
                // Extract year from timestamp (milliseconds since epoch)
                let secs = *ts / 1000;
                let days = secs / 86400;
                let years = days / 365; // Simplified
                PartitionValue::Int(1970 + years as i32)
            }
            (PartitionTransform::Month, PartitionValue::Timestamp(ts)) => {
                let secs = *ts / 1000;
                let days = secs / 86400;
                let months = days / 30; // Simplified
                PartitionValue::Int(months as i32)
            }
            (PartitionTransform::Day, PartitionValue::Timestamp(ts)) => {
                let secs = *ts / 1000;
                let days = secs / 86400;
                PartitionValue::Int(days as i32)
            }
            (PartitionTransform::Hour, PartitionValue::Timestamp(ts)) => {
                let secs = *ts / 1000;
                let hours = secs / 3600;
                PartitionValue::Int(hours as i32)
            }
            (PartitionTransform::Bucket(n), PartitionValue::String(s)) => {
                // Simple hash bucket
                let hash = s.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));
                PartitionValue::Int((hash % n) as i32)
            }
            (PartitionTransform::Bucket(n), PartitionValue::Int(i)) => {
                PartitionValue::Int(((*i as u32) % n) as i32)
            }
            (PartitionTransform::Truncate(w), PartitionValue::String(s)) => {
                PartitionValue::String(s.chars().take(*w as usize).collect())
            }
            (PartitionTransform::Truncate(w), PartitionValue::Int(i)) => {
                PartitionValue::Int(i - (i % (*w as i32)))
            }
            (PartitionTransform::Void, _) => PartitionValue::Null,
            _ => PartitionValue::Null,
        }
    }
}

/// Partition value
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PartitionValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    Timestamp(i64),
}

/// Partition spec
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

/// Sort order
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

/// Sort field
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortField {
    pub transform: PartitionTransform,
    pub source_id: i32,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

/// Sort direction
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Null ordering
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullOrder {
    First,
    Last,
}

/// Iceberg snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
    pub schema_id: Option<i32>,
}

/// Manifest file entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub status: ManifestEntryStatus,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub data_file: DataFile,
}

/// Manifest entry status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestEntryStatus {
    Existing,
    Added,
    Deleted,
}

/// Data file
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataFile {
    pub content: DataFileContent,
    pub file_path: String,
    pub file_format: FileFormat,
    pub partition: HashMap<String, PartitionValue>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: Option<HashMap<i32, i64>>,
    pub value_counts: Option<HashMap<i32, i64>>,
    pub null_value_counts: Option<HashMap<i32, i64>>,
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    pub key_metadata: Option<Vec<u8>>,
    pub split_offsets: Option<Vec<i64>>,
    pub equality_ids: Option<Vec<i32>>,
    pub sort_order_id: Option<i32>,
}

/// Data file content type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataFileContent {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

/// Iceberg table metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IcebergMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub current_schema_id: i32,
    pub schemas: Vec<Schema>,
    pub default_spec_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub last_partition_id: i32,
    pub default_sort_order_id: i32,
    pub sort_orders: Vec<SortOrder>,
    pub properties: HashMap<String, String>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<IcebergSnapshot>,
    pub snapshot_log: Vec<SnapshotLogEntry>,
    pub metadata_log: Vec<MetadataLogEntry>,
    pub refs: HashMap<String, SnapshotRef>,
}

/// Snapshot log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    pub timestamp_ms: i64,
    pub snapshot_id: i64,
}

/// Metadata log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    pub timestamp_ms: i64,
    pub metadata_file: String,
}

/// Snapshot reference (branch/tag)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    pub ref_type: RefType,
    pub min_snapshots_to_keep: Option<i32>,
    pub max_snapshot_age_ms: Option<i64>,
    pub max_ref_age_ms: Option<i64>,
}

/// Reference type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefType {
    Branch,
    Tag,
}

/// Iceberg table handle
#[derive(Debug)]
pub struct IcebergTable {
    config: IcebergConfig,
    metadata: IcebergMetadata,
}

impl IcebergTable {
    /// Create a new Iceberg table
    pub fn create(config: IcebergConfig, schema: Schema, partition_spec: Option<PartitionSpec>) -> Self {
        let table_uuid = format!("{:x}", std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos());

        let default_spec = partition_spec.unwrap_or(PartitionSpec {
            spec_id: 0,
            fields: Vec::new(),
        });

        let metadata = IcebergMetadata {
            format_version: config.format_version,
            table_uuid,
            location: config.location.clone(),
            last_sequence_number: 0,
            last_updated_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            last_column_id: schema.columns.iter().map(|c| c.id).max().unwrap_or(0),
            current_schema_id: schema.schema_id,
            schemas: vec![schema],
            default_spec_id: default_spec.spec_id,
            partition_specs: vec![default_spec],
            last_partition_id: 0,
            default_sort_order_id: 0,
            sort_orders: vec![SortOrder {
                order_id: 0,
                fields: Vec::new(),
            }],
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: Vec::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            refs: HashMap::new(),
        };

        Self { config, metadata }
    }

    /// Get current schema
    pub fn schema(&self) -> Option<&Schema> {
        self.metadata.schemas.iter().find(|s| s.schema_id == self.metadata.current_schema_id)
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&IcebergSnapshot> {
        self.metadata.current_snapshot_id.and_then(|id| {
            self.metadata.snapshots.iter().find(|s| s.snapshot_id == id)
        })
    }

    /// Start a new transaction
    pub fn new_transaction(&self) -> IcebergTransaction<'_> {
        IcebergTransaction {
            table: self,
            data_files: Vec::new(),
            delete_files: Vec::new(),
            schema_update: None,
            partition_spec_update: None,
            properties_update: HashMap::new(),
        }
    }

    /// Get snapshot by ID
    pub fn snapshot(&self, snapshot_id: i64) -> Option<&IcebergSnapshot> {
        self.metadata.snapshots.iter().find(|s| s.snapshot_id == snapshot_id)
    }

    /// Get snapshot history
    pub fn history(&self) -> &[SnapshotLogEntry] {
        &self.metadata.snapshot_log
    }

    /// Create a branch
    pub fn create_branch(&mut self, name: &str, snapshot_id: Option<i64>) -> Result<(), LakehouseError> {
        let snap_id = snapshot_id
            .or(self.metadata.current_snapshot_id)
            .ok_or_else(|| LakehouseError::SnapshotNotFound(0))?;

        self.metadata.refs.insert(name.to_string(), SnapshotRef {
            snapshot_id: snap_id,
            ref_type: RefType::Branch,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });

        Ok(())
    }

    /// Create a tag
    pub fn create_tag(&mut self, name: &str, snapshot_id: i64) -> Result<(), LakehouseError> {
        if !self.metadata.snapshots.iter().any(|s| s.snapshot_id == snapshot_id) {
            return Err(LakehouseError::SnapshotNotFound(snapshot_id));
        }

        self.metadata.refs.insert(name.to_string(), SnapshotRef {
            snapshot_id,
            ref_type: RefType::Tag,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });

        Ok(())
    }

    /// Evolve schema (add column)
    pub fn add_column(&mut self, column: Column) -> Result<(), LakehouseError> {
        let current_schema = self.metadata.schemas
            .iter_mut()
            .find(|s| s.schema_id == self.metadata.current_schema_id)
            .ok_or_else(|| LakehouseError::SchemaMismatch("current schema not found".into()))?;

        if current_schema.columns.iter().any(|c| c.name == column.name) {
            return Err(LakehouseError::SchemaMismatch(format!(
                "column {} already exists", column.name
            )));
        }

        current_schema.columns.push(column);
        self.metadata.last_column_id += 1;
        self.metadata.last_updated_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Ok(())
    }

    /// Cherry-pick snapshot
    pub fn cherry_pick(&mut self, snapshot_id: i64) -> Result<i64, LakehouseError> {
        let _source = self.metadata.snapshots
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
            .ok_or(LakehouseError::SnapshotNotFound(snapshot_id))?
            .clone();

        // Create new snapshot with same manifest list
        let new_snapshot_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let new_snapshot = IcebergSnapshot {
            snapshot_id: new_snapshot_id,
            parent_snapshot_id: self.metadata.current_snapshot_id,
            sequence_number: self.metadata.last_sequence_number + 1,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            manifest_list: _source.manifest_list.clone(),
            summary: {
                let mut s = _source.summary.clone();
                s.insert("operation".to_string(), "cherry-pick".to_string());
                s.insert("cherry-picked-snapshot-id".to_string(), snapshot_id.to_string());
                s
            },
            schema_id: _source.schema_id,
        };

        self.metadata.last_sequence_number += 1;
        self.metadata.current_snapshot_id = Some(new_snapshot_id);
        self.metadata.snapshots.push(new_snapshot);

        Ok(new_snapshot_id)
    }

    /// Expire snapshots older than the given timestamp
    pub fn expire_snapshots(&mut self, older_than_ms: i64) -> usize {
        let current_id = self.metadata.current_snapshot_id;
        let before_count = self.metadata.snapshots.len();

        self.metadata.snapshots.retain(|s| {
            Some(s.snapshot_id) == current_id || s.timestamp_ms >= older_than_ms
        });

        before_count - self.metadata.snapshots.len()
    }
}

/// Iceberg transaction
pub struct IcebergTransaction<'a> {
    table: &'a IcebergTable,
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
    schema_update: Option<Schema>,
    partition_spec_update: Option<PartitionSpec>,
    properties_update: HashMap<String, String>,
}

impl<'a> IcebergTransaction<'a> {
    /// Add a data file
    pub fn add_data_file(&mut self, file: DataFile) {
        self.data_files.push(file);
    }

    /// Add a delete file
    pub fn add_delete_file(&mut self, file: DataFile) {
        self.delete_files.push(file);
    }

    /// Update schema
    pub fn update_schema(&mut self, schema: Schema) {
        self.schema_update = Some(schema);
    }

    /// Update partition spec
    pub fn update_partition_spec(&mut self, spec: PartitionSpec) {
        self.partition_spec_update = Some(spec);
    }

    /// Set table property
    pub fn set_property(&mut self, key: &str, value: &str) {
        self.properties_update.insert(key.to_string(), value.to_string());
    }

    /// Get summary of pending changes
    pub fn summary(&self) -> HashMap<String, String> {
        let mut s = HashMap::new();
        s.insert("added-data-files".to_string(), self.data_files.len().to_string());
        s.insert("added-delete-files".to_string(), self.delete_files.len().to_string());
        s.insert("added-records".to_string(),
            self.data_files.iter().map(|f| f.record_count).sum::<i64>().to_string());
        s.insert("added-files-size".to_string(),
            self.data_files.iter().map(|f| f.file_size_in_bytes).sum::<i64>().to_string());
        s
    }
}

// ============================================================================
// Lakehouse Manager
// ============================================================================

/// Lakehouse manager for Delta Lake and Iceberg tables
#[derive(Debug)]
pub struct LakehouseManager {
    delta_tables: HashMap<String, DeltaLog>,
    iceberg_tables: HashMap<String, IcebergTable>,
}

impl LakehouseManager {
    pub fn new() -> Self {
        Self {
            delta_tables: HashMap::new(),
            iceberg_tables: HashMap::new(),
        }
    }

    /// Register a Delta Lake table
    pub fn register_delta(&mut self, name: &str, config: DeltaConfig) {
        self.delta_tables.insert(name.to_string(), DeltaLog::new(config));
    }

    /// Register an Iceberg table
    pub fn register_iceberg(&mut self, name: &str, table: IcebergTable) {
        self.iceberg_tables.insert(name.to_string(), table);
    }

    /// Get a Delta table
    pub fn get_delta(&self, name: &str) -> Option<&DeltaLog> {
        self.delta_tables.get(name)
    }

    /// Get a mutable Delta table
    pub fn get_delta_mut(&mut self, name: &str) -> Option<&mut DeltaLog> {
        self.delta_tables.get_mut(name)
    }

    /// Get an Iceberg table
    pub fn get_iceberg(&self, name: &str) -> Option<&IcebergTable> {
        self.iceberg_tables.get(name)
    }

    /// Get a mutable Iceberg table
    pub fn get_iceberg_mut(&mut self, name: &str) -> Option<&mut IcebergTable> {
        self.iceberg_tables.get_mut(name)
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<(&str, TableFormat)> {
        let mut result = Vec::new();
        for name in self.delta_tables.keys() {
            result.push((name.as_str(), TableFormat::Delta));
        }
        for name in self.iceberg_tables.keys() {
            result.push((name.as_str(), TableFormat::Iceberg));
        }
        result
    }
}

impl Default for LakehouseManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_transaction() {
        let config = DeltaConfig {
            location: "/test/delta".to_string(),
            ..Default::default()
        };

        let mut log = DeltaLog::new(config);
        let mut txn = log.start_transaction();

        txn.add_file(AddFileAction {
            path: "part-0001.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1024,
            modification_time: 0,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        });

        let version = log.commit(txn).unwrap();
        assert_eq!(version, 0);
        assert_eq!(log.active_files().len(), 1);
    }

    #[test]
    fn test_iceberg_partition_transforms() {
        let ts = 1704067200000i64; // 2024-01-01 00:00:00 UTC

        let year = PartitionTransform::Year.apply(&PartitionValue::Timestamp(ts));
        assert!(matches!(year, PartitionValue::Int(2024)));

        let bucket = PartitionTransform::Bucket(10).apply(&PartitionValue::String("hello".into()));
        assert!(matches!(bucket, PartitionValue::Int(i) if i < 10));

        let truncate = PartitionTransform::Truncate(3).apply(&PartitionValue::String("hello".into()));
        assert_eq!(truncate, PartitionValue::String("hel".into()));
    }

    #[test]
    fn test_iceberg_table_branches() {
        let config = IcebergConfig {
            location: "/test/iceberg".to_string(),
            ..Default::default()
        };

        let schema = Schema::new(vec![
            Column {
                id: 1,
                name: "id".to_string(),
                data_type: ColumnType::Int64,
                required: true,
                doc: None,
            }
        ]);

        let mut table = IcebergTable::create(config, schema, None);

        // Add a snapshot first
        table.metadata.snapshots.push(IcebergSnapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 0,
            manifest_list: "manifest.avro".to_string(),
            summary: HashMap::new(),
            schema_id: Some(0),
        });
        table.metadata.current_snapshot_id = Some(1);

        // Create branch
        table.create_branch("feature-x", None).unwrap();
        assert!(table.metadata.refs.contains_key("feature-x"));

        // Create tag
        table.create_tag("v1.0", 1).unwrap();
        let tag = table.metadata.refs.get("v1.0").unwrap();
        assert_eq!(tag.ref_type, RefType::Tag);
    }
}
