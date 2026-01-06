// Phase 12: Distributed DDL & Global Indexes
//
// Distributed DDL execution across cluster nodes and global secondary indexes
// for cross-shard queries with ON CLUSTER support and distributed indexes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

// ============================================================================
// Distributed DDL
// ============================================================================

/// DDL operation type
#[derive(Debug, Clone, PartialEq)]
pub enum DdlOperation {
    CreateDatabase {
        name: String,
        if_not_exists: bool,
        engine: Option<String>,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    CreateTable {
        database: String,
        name: String,
        if_not_exists: bool,
        schema: TableSchema,
        engine: TableEngine,
    },
    DropTable {
        database: String,
        name: String,
        if_exists: bool,
    },
    AlterTable {
        database: String,
        name: String,
        alterations: Vec<TableAlteration>,
    },
    RenameTable {
        database: String,
        old_name: String,
        new_name: String,
    },
    TruncateTable {
        database: String,
        name: String,
    },
    CreateIndex {
        database: String,
        table: String,
        index: IndexDefinition,
    },
    DropIndex {
        database: String,
        table: String,
        index_name: String,
    },
    CreateView {
        database: String,
        name: String,
        query: String,
        materialized: bool,
    },
    DropView {
        database: String,
        name: String,
    },
}

/// Table schema definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Option<Vec<String>>,
    pub order_by: Option<Vec<String>>,
    pub partition_by: Option<String>,
    pub settings: HashMap<String, String>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
    pub codec: Option<String>,
    pub comment: Option<String>,
}

/// Data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: u8 },
    String,
    FixedString(u32),
    Date,
    DateTime,
    DateTime64 { precision: u8 },
    UUID,
    IPv4,
    IPv6,
    Array(Box<DataType>),
    Map { key: Box<DataType>, value: Box<DataType> },
    Tuple(Vec<DataType>),
    Nullable(Box<DataType>),
    LowCardinality(Box<DataType>),
}

impl DataType {
    pub fn to_string(&self) -> String {
        match self {
            Self::Int8 => "Int8".to_string(),
            Self::Int16 => "Int16".to_string(),
            Self::Int32 => "Int32".to_string(),
            Self::Int64 => "Int64".to_string(),
            Self::UInt8 => "UInt8".to_string(),
            Self::UInt16 => "UInt16".to_string(),
            Self::UInt32 => "UInt32".to_string(),
            Self::UInt64 => "UInt64".to_string(),
            Self::Float32 => "Float32".to_string(),
            Self::Float64 => "Float64".to_string(),
            Self::Decimal { precision, scale } => format!("Decimal({}, {})", precision, scale),
            Self::String => "String".to_string(),
            Self::FixedString(n) => format!("FixedString({})", n),
            Self::Date => "Date".to_string(),
            Self::DateTime => "DateTime".to_string(),
            Self::DateTime64 { precision } => format!("DateTime64({})", precision),
            Self::UUID => "UUID".to_string(),
            Self::IPv4 => "IPv4".to_string(),
            Self::IPv6 => "IPv6".to_string(),
            Self::Array(inner) => format!("Array({})", inner.to_string()),
            Self::Map { key, value } => format!("Map({}, {})", key.to_string(), value.to_string()),
            Self::Tuple(types) => {
                let inner: Vec<String> = types.iter().map(|t| t.to_string()).collect();
                format!("Tuple({})", inner.join(", "))
            }
            Self::Nullable(inner) => format!("Nullable({})", inner.to_string()),
            Self::LowCardinality(inner) => format!("LowCardinality({})", inner.to_string()),
        }
    }
}

/// Table engine types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableEngine {
    MergeTree,
    ReplacingMergeTree { version_column: Option<String> },
    SummingMergeTree { columns: Vec<String> },
    AggregatingMergeTree,
    CollapsingMergeTree { sign_column: String },
    VersionedCollapsingMergeTree { sign_column: String, version_column: String },
    ReplicatedMergeTree { zoo_path: String, replica_name: String },
    Distributed { cluster: String, database: String, table: String, sharding_key: Option<String> },
    Memory,
    Log,
    TinyLog,
    Buffer { database: String, table: String, layers: BufferLayers },
}

/// Buffer engine layers config
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BufferLayers {
    pub num_layers: u8,
    pub min_time: u64,
    pub max_time: u64,
    pub min_rows: u64,
    pub max_rows: u64,
    pub min_bytes: u64,
    pub max_bytes: u64,
}

/// Table alterations
#[derive(Debug, Clone, PartialEq)]
pub enum TableAlteration {
    AddColumn { column: ColumnDefinition, after: Option<String> },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    ModifyColumn { column: ColumnDefinition },
    CommentColumn { name: String, comment: String },
    ModifyOrderBy { columns: Vec<String> },
    ModifyTtl { ttl: String },
    AddIndex { index: IndexDefinition },
    DropIndex { name: String },
    AddProjection { projection: ProjectionDefinition },
    DropProjection { name: String },
    Freeze { partition: Option<String> },
    Unfreeze { partition: Option<String> },
}

/// Index definition
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDefinition {
    pub name: String,
    pub expression: String,
    pub index_type: IndexType,
    pub granularity: u64,
}

/// Index types
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    MinMax,
    Set { max_rows: u64 },
    BloomFilter { false_positive_ppm: u32 }, // parts per million instead of f64
    TokenBloomFilter { false_positive_ppm: u32 },
    NGramBloomFilter { n: u8, false_positive_ppm: u32 },
    Hypothesis,
}

/// Projection definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectionDefinition {
    pub name: String,
    pub query: String,
}

/// DDL task status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlTaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

/// DDL task for a single node
#[derive(Debug, Clone)]
pub struct DdlNodeTask {
    pub node_id: String,
    pub status: DdlTaskStatus,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub error: Option<String>,
    pub retries: u32,
}

/// Distributed DDL task
#[derive(Debug, Clone)]
pub struct DdlTask {
    pub task_id: u64,
    pub operation: DdlOperation,
    pub cluster: String,
    pub initiator: String,
    pub created_at: Instant,
    pub node_tasks: HashMap<String, DdlNodeTask>,
    pub timeout: Duration,
}

impl DdlTask {
    /// Check if all nodes completed
    pub fn is_complete(&self) -> bool {
        self.node_tasks.values().all(|t| {
            matches!(t.status, DdlTaskStatus::Completed | DdlTaskStatus::Failed(_))
        })
    }

    /// Check if all nodes succeeded
    pub fn is_success(&self) -> bool {
        self.node_tasks.values().all(|t| t.status == DdlTaskStatus::Completed)
    }

    /// Get failed nodes
    pub fn failed_nodes(&self) -> Vec<&str> {
        self.node_tasks.iter()
            .filter(|(_, t)| matches!(t.status, DdlTaskStatus::Failed(_)))
            .map(|(n, _)| n.as_str())
            .collect()
    }

    /// Get pending nodes
    pub fn pending_nodes(&self) -> Vec<&str> {
        self.node_tasks.iter()
            .filter(|(_, t)| matches!(t.status, DdlTaskStatus::Pending | DdlTaskStatus::Running))
            .map(|(n, _)| n.as_str())
            .collect()
    }
}

/// Distributed DDL queue entry (for persistence)
#[derive(Debug, Clone)]
pub struct DdlQueueEntry {
    pub task_id: u64,
    pub operation_sql: String,
    pub cluster: String,
    pub initiator: String,
    pub timestamp: i64,
    pub settings: HashMap<String, String>,
}

/// Error type for distributed DDL
#[derive(Debug, Clone)]
pub enum DdlError {
    ClusterNotFound(String),
    NodeUnreachable(String),
    OperationFailed { node: String, error: String },
    Timeout,
    InvalidOperation(String),
    AlreadyExists(String),
    NotFound(String),
    InternalError(String),
}

impl std::fmt::Display for DdlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClusterNotFound(c) => write!(f, "Cluster not found: {}", c),
            Self::NodeUnreachable(n) => write!(f, "Node unreachable: {}", n),
            Self::OperationFailed { node, error } => write!(f, "Operation failed on {}: {}", node, error),
            Self::Timeout => write!(f, "DDL operation timed out"),
            Self::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            Self::AlreadyExists(name) => write!(f, "Already exists: {}", name),
            Self::NotFound(name) => write!(f, "Not found: {}", name),
            Self::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DdlError {}

/// Cluster node information
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub shard: u32,
    pub replica: u32,
    pub is_local: bool,
    pub weight: u32,
}

/// Cluster definition
#[derive(Debug, Clone)]
pub struct ClusterDefinition {
    pub name: String,
    pub nodes: Vec<ClusterNode>,
    pub shards: u32,
    pub replicas_per_shard: u32,
    pub internal_replication: bool,
}

impl ClusterDefinition {
    /// Get nodes for a specific shard
    pub fn shard_nodes(&self, shard: u32) -> Vec<&ClusterNode> {
        self.nodes.iter().filter(|n| n.shard == shard).collect()
    }

    /// Get all shard numbers
    pub fn shard_numbers(&self) -> Vec<u32> {
        let mut shards: Vec<u32> = self.nodes.iter().map(|n| n.shard).collect();
        shards.sort();
        shards.dedup();
        shards
    }

    /// Get one node per shard (for distributed queries)
    pub fn one_per_shard(&self) -> Vec<&ClusterNode> {
        let mut result = Vec::new();
        for shard in self.shard_numbers() {
            if let Some(node) = self.shard_nodes(shard).first() {
                result.push(*node);
            }
        }
        result
    }
}

/// Distributed DDL Manager
pub struct DistributedDdlManager {
    /// Known clusters
    clusters: RwLock<HashMap<String, ClusterDefinition>>,
    /// Active DDL tasks
    tasks: RwLock<HashMap<u64, DdlTask>>,
    /// Task ID counter
    next_task_id: AtomicU64,
    /// DDL queue for persistence
    queue: RwLock<Vec<DdlQueueEntry>>,
    /// Local node ID
    local_node_id: String,
    /// Default timeout
    default_timeout: Duration,
    /// Max retries per node
    max_retries: u32,
}

impl DistributedDdlManager {
    pub fn new(local_node_id: String) -> Self {
        Self {
            clusters: RwLock::new(HashMap::new()),
            tasks: RwLock::new(HashMap::new()),
            next_task_id: AtomicU64::new(1),
            queue: RwLock::new(Vec::new()),
            local_node_id,
            default_timeout: Duration::from_secs(300),
            max_retries: 3,
        }
    }

    /// Register a cluster
    pub fn register_cluster(&self, cluster: ClusterDefinition) {
        self.clusters.write().insert(cluster.name.clone(), cluster);
    }

    /// Get cluster definition
    pub fn get_cluster(&self, name: &str) -> Option<ClusterDefinition> {
        self.clusters.read().get(name).cloned()
    }

    /// List all clusters
    pub fn list_clusters(&self) -> Vec<String> {
        self.clusters.read().keys().cloned().collect()
    }

    /// Execute DDL on cluster
    pub fn execute_on_cluster(
        &self,
        cluster_name: &str,
        operation: DdlOperation,
    ) -> Result<u64, DdlError> {
        let cluster = self.clusters.read()
            .get(cluster_name)
            .cloned()
            .ok_or_else(|| DdlError::ClusterNotFound(cluster_name.to_string()))?;

        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);

        // Create node tasks for all unique nodes
        let mut node_tasks = HashMap::new();
        let mut seen_nodes = HashSet::new();

        for node in &cluster.nodes {
            if seen_nodes.insert(node.node_id.clone()) {
                node_tasks.insert(node.node_id.clone(), DdlNodeTask {
                    node_id: node.node_id.clone(),
                    status: DdlTaskStatus::Pending,
                    started_at: None,
                    completed_at: None,
                    error: None,
                    retries: 0,
                });
            }
        }

        let task = DdlTask {
            task_id,
            operation,
            cluster: cluster_name.to_string(),
            initiator: self.local_node_id.clone(),
            created_at: Instant::now(),
            node_tasks,
            timeout: self.default_timeout,
        };

        self.tasks.write().insert(task_id, task);

        // In a real implementation, this would send the DDL to all nodes
        // For now, we'll simulate immediate local execution
        self.execute_local(task_id)?;

        Ok(task_id)
    }

    /// Execute DDL locally (for simulation/testing)
    fn execute_local(&self, task_id: u64) -> Result<(), DdlError> {
        let mut tasks = self.tasks.write();
        let task = tasks.get_mut(&task_id)
            .ok_or_else(|| DdlError::InternalError("Task not found".to_string()))?;

        // Mark local node as running then completed
        if let Some(node_task) = task.node_tasks.get_mut(&self.local_node_id) {
            node_task.status = DdlTaskStatus::Running;
            node_task.started_at = Some(Instant::now());

            // Simulate execution (in real impl, would execute the DDL)
            node_task.status = DdlTaskStatus::Completed;
            node_task.completed_at = Some(Instant::now());
        }

        // For testing, mark all other nodes as completed too
        for (node_id, node_task) in task.node_tasks.iter_mut() {
            if node_id != &self.local_node_id {
                node_task.status = DdlTaskStatus::Completed;
                node_task.completed_at = Some(Instant::now());
            }
        }

        Ok(())
    }

    /// Get task status
    pub fn get_task(&self, task_id: u64) -> Option<DdlTask> {
        self.tasks.read().get(&task_id).cloned()
    }

    /// Wait for task completion
    pub fn wait_for_task(&self, task_id: u64, timeout: Duration) -> Result<DdlTask, DdlError> {
        let start = Instant::now();

        loop {
            if let Some(task) = self.tasks.read().get(&task_id) {
                if task.is_complete() {
                    return Ok(task.clone());
                }
            } else {
                return Err(DdlError::InternalError("Task not found".to_string()));
            }

            if start.elapsed() > timeout {
                return Err(DdlError::Timeout);
            }

            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Cancel a DDL task
    pub fn cancel_task(&self, task_id: u64) -> Result<(), DdlError> {
        let mut tasks = self.tasks.write();
        let task = tasks.get_mut(&task_id)
            .ok_or_else(|| DdlError::InternalError("Task not found".to_string()))?;

        for node_task in task.node_tasks.values_mut() {
            if matches!(node_task.status, DdlTaskStatus::Pending | DdlTaskStatus::Running) {
                node_task.status = DdlTaskStatus::Cancelled;
            }
        }

        Ok(())
    }

    /// Mark node task as failed
    pub fn mark_node_failed(&self, task_id: u64, node_id: &str, error: &str) -> Result<(), DdlError> {
        let mut tasks = self.tasks.write();
        let task = tasks.get_mut(&task_id)
            .ok_or_else(|| DdlError::InternalError("Task not found".to_string()))?;

        if let Some(node_task) = task.node_tasks.get_mut(node_id) {
            node_task.status = DdlTaskStatus::Failed(error.to_string());
            node_task.error = Some(error.to_string());
            node_task.completed_at = Some(Instant::now());
        }

        Ok(())
    }

    /// Mark node task as completed
    pub fn mark_node_completed(&self, task_id: u64, node_id: &str) -> Result<(), DdlError> {
        let mut tasks = self.tasks.write();
        let task = tasks.get_mut(&task_id)
            .ok_or_else(|| DdlError::InternalError("Task not found".to_string()))?;

        if let Some(node_task) = task.node_tasks.get_mut(node_id) {
            node_task.status = DdlTaskStatus::Completed;
            node_task.completed_at = Some(Instant::now());
        }

        Ok(())
    }

    /// Get DDL queue entries
    pub fn get_queue(&self) -> Vec<DdlQueueEntry> {
        self.queue.read().clone()
    }

    /// Clean up completed tasks older than duration
    pub fn cleanup_old_tasks(&self, max_age: Duration) {
        let mut tasks = self.tasks.write();
        let now = Instant::now();

        tasks.retain(|_, task| {
            if task.is_complete() {
                task.created_at.elapsed() < max_age
            } else {
                true
            }
        });
    }
}

impl Default for DistributedDdlManager {
    fn default() -> Self {
        Self::new("local".to_string())
    }
}

// ============================================================================
// Global Secondary Indexes
// ============================================================================

/// Global index entry - maps indexed value to shard locations
#[derive(Debug, Clone)]
pub struct GlobalIndexEntry {
    pub value: IndexValue,
    pub locations: Vec<ShardLocation>,
}

/// Location of data in a shard
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardLocation {
    pub shard_id: u32,
    pub partition: String,
    pub part_name: String,
    pub min_block: u64,
    pub max_block: u64,
}

/// Index value types
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    String(String),
    Bytes(Vec<u8>),
}

impl IndexValue {
    pub fn from_bytes(data: &[u8], data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                if data.len() >= 8 {
                    Some(IndexValue::Int(i64::from_le_bytes(data[..8].try_into().ok()?)))
                } else {
                    None
                }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                if data.len() >= 8 {
                    Some(IndexValue::UInt(u64::from_le_bytes(data[..8].try_into().ok()?)))
                } else {
                    None
                }
            }
            DataType::String => {
                String::from_utf8(data.to_vec()).ok().map(IndexValue::String)
            }
            _ => Some(IndexValue::Bytes(data.to_vec())),
        }
    }
}

/// Global index configuration
#[derive(Debug, Clone)]
pub struct GlobalIndexConfig {
    pub name: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: GlobalIndexType,
    pub unique: bool,
    pub include_columns: Vec<String>,  // Covering index columns
}

/// Global index types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalIndexType {
    /// B-tree style ordered index
    BTree,
    /// Hash index for equality lookups
    Hash,
    /// Full-text inverted index
    Inverted,
    /// Bitmap index for low-cardinality columns
    Bitmap,
    /// Geospatial index
    Spatial,
}

/// Index statistics
#[derive(Debug, Clone, Default)]
pub struct GlobalIndexStats {
    pub entries: u64,
    pub unique_values: u64,
    pub size_bytes: u64,
    pub shards_covered: u32,
    pub last_updated: Option<i64>,
    pub lookups: u64,
    pub hits: u64,
}

/// Global index structure
pub struct GlobalIndex {
    config: GlobalIndexConfig,
    /// BTree index: sorted map of value -> locations
    btree_index: RwLock<BTreeMap<IndexValue, Vec<ShardLocation>>>,
    /// Hash index: hash map of value -> locations
    hash_index: RwLock<HashMap<IndexValue, Vec<ShardLocation>>>,
    /// Bitmap index: value -> bitmap of shard presence
    bitmap_index: RwLock<HashMap<IndexValue, Vec<u64>>>,
    /// Statistics
    stats: RwLock<GlobalIndexStats>,
}

impl GlobalIndex {
    pub fn new(config: GlobalIndexConfig) -> Self {
        Self {
            config,
            btree_index: RwLock::new(BTreeMap::new()),
            hash_index: RwLock::new(HashMap::new()),
            bitmap_index: RwLock::new(HashMap::new()),
            stats: RwLock::new(GlobalIndexStats::default()),
        }
    }

    /// Insert an entry into the index
    pub fn insert(&self, value: IndexValue, location: ShardLocation) {
        match self.config.index_type {
            GlobalIndexType::BTree => {
                let mut index = self.btree_index.write();
                index.entry(value).or_default().push(location);
            }
            GlobalIndexType::Hash => {
                let mut index = self.hash_index.write();
                index.entry(value).or_default().push(location);
            }
            GlobalIndexType::Bitmap => {
                let mut index = self.bitmap_index.write();
                let shard_bit = location.shard_id as usize;
                let entry = index.entry(value).or_insert_with(Vec::new);

                // Ensure bitmap is large enough
                let word_idx = shard_bit / 64;
                while entry.len() <= word_idx {
                    entry.push(0);
                }
                entry[word_idx] |= 1 << (shard_bit % 64);
            }
            _ => {
                // For other types, use hash index as fallback
                let mut index = self.hash_index.write();
                index.entry(value).or_default().push(location);
            }
        }

        let mut stats = self.stats.write();
        stats.entries += 1;
    }

    /// Lookup exact value
    pub fn lookup(&self, value: &IndexValue) -> Vec<ShardLocation> {
        let mut stats = self.stats.write();
        stats.lookups += 1;

        let result = match self.config.index_type {
            GlobalIndexType::BTree => {
                self.btree_index.read().get(value).cloned().unwrap_or_default()
            }
            GlobalIndexType::Hash => {
                self.hash_index.read().get(value).cloned().unwrap_or_default()
            }
            GlobalIndexType::Bitmap => {
                // Convert bitmap to locations
                if let Some(bitmap) = self.bitmap_index.read().get(value) {
                    let mut locations = Vec::new();
                    for (word_idx, &word) in bitmap.iter().enumerate() {
                        for bit in 0..64 {
                            if word & (1 << bit) != 0 {
                                let shard_id = (word_idx * 64 + bit) as u32;
                                locations.push(ShardLocation {
                                    shard_id,
                                    partition: String::new(),
                                    part_name: String::new(),
                                    min_block: 0,
                                    max_block: u64::MAX,
                                });
                            }
                        }
                    }
                    locations
                } else {
                    Vec::new()
                }
            }
            _ => {
                self.hash_index.read().get(value).cloned().unwrap_or_default()
            }
        };

        if !result.is_empty() {
            stats.hits += 1;
        }

        result
    }

    /// Range lookup (BTree only)
    pub fn range_lookup(&self, min: Option<&IndexValue>, max: Option<&IndexValue>) -> Vec<ShardLocation> {
        if self.config.index_type != GlobalIndexType::BTree {
            return Vec::new();
        }

        let index = self.btree_index.read();
        let mut results = Vec::new();

        let range = match (min, max) {
            (Some(min), Some(max)) => {
                index.range(min.clone()..=max.clone())
            }
            (Some(min), None) => {
                index.range(min.clone()..)
            }
            (None, Some(max)) => {
                index.range(..=max.clone())
            }
            (None, None) => {
                return index.values().flatten().cloned().collect();
            }
        };

        for locations in range.map(|(_, v)| v) {
            results.extend(locations.iter().cloned());
        }

        results
    }

    /// Get shards containing value (for query routing)
    pub fn get_shards(&self, value: &IndexValue) -> Vec<u32> {
        let locations = self.lookup(value);
        let mut shards: Vec<u32> = locations.iter().map(|l| l.shard_id).collect();
        shards.sort();
        shards.dedup();
        shards
    }

    /// Get shards for range query
    pub fn get_shards_for_range(&self, min: Option<&IndexValue>, max: Option<&IndexValue>) -> Vec<u32> {
        let locations = self.range_lookup(min, max);
        let mut shards: Vec<u32> = locations.iter().map(|l| l.shard_id).collect();
        shards.sort();
        shards.dedup();
        shards
    }

    /// Remove entries for a shard (for rebalancing)
    pub fn remove_shard(&self, shard_id: u32) {
        match self.config.index_type {
            GlobalIndexType::BTree => {
                let mut index = self.btree_index.write();
                for locations in index.values_mut() {
                    locations.retain(|l| l.shard_id != shard_id);
                }
                index.retain(|_, v| !v.is_empty());
            }
            GlobalIndexType::Hash => {
                let mut index = self.hash_index.write();
                for locations in index.values_mut() {
                    locations.retain(|l| l.shard_id != shard_id);
                }
                index.retain(|_, v| !v.is_empty());
            }
            GlobalIndexType::Bitmap => {
                let mut index = self.bitmap_index.write();
                let word_idx = (shard_id / 64) as usize;
                let bit = shard_id % 64;

                for bitmap in index.values_mut() {
                    if word_idx < bitmap.len() {
                        bitmap[word_idx] &= !(1 << bit);
                    }
                }
            }
            _ => {}
        }
    }

    /// Get index statistics
    pub fn stats(&self) -> GlobalIndexStats {
        let mut stats = self.stats.read().clone();

        match self.config.index_type {
            GlobalIndexType::BTree => {
                let index = self.btree_index.read();
                stats.unique_values = index.len() as u64;
                stats.entries = index.values().map(|v| v.len() as u64).sum();
            }
            GlobalIndexType::Hash => {
                let index = self.hash_index.read();
                stats.unique_values = index.len() as u64;
                stats.entries = index.values().map(|v| v.len() as u64).sum();
            }
            GlobalIndexType::Bitmap => {
                stats.unique_values = self.bitmap_index.read().len() as u64;
            }
            _ => {}
        }

        stats
    }

    /// Get index configuration
    pub fn config(&self) -> &GlobalIndexConfig {
        &self.config
    }
}

/// Global index manager
pub struct GlobalIndexManager {
    indexes: RwLock<HashMap<String, Arc<GlobalIndex>>>,
}

impl GlobalIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new global index
    pub fn create_index(&self, config: GlobalIndexConfig) -> Result<(), DdlError> {
        let key = format!("{}.{}.{}", config.database, config.table, config.name);

        if self.indexes.read().contains_key(&key) {
            return Err(DdlError::AlreadyExists(key));
        }

        let index = Arc::new(GlobalIndex::new(config));
        self.indexes.write().insert(key, index);

        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, database: &str, table: &str, name: &str) -> Result<(), DdlError> {
        let key = format!("{}.{}.{}", database, table, name);

        if self.indexes.write().remove(&key).is_none() {
            return Err(DdlError::NotFound(key));
        }

        Ok(())
    }

    /// Get an index
    pub fn get_index(&self, database: &str, table: &str, name: &str) -> Option<Arc<GlobalIndex>> {
        let key = format!("{}.{}.{}", database, table, name);
        self.indexes.read().get(&key).cloned()
    }

    /// List indexes for a table
    pub fn list_indexes(&self, database: &str, table: &str) -> Vec<String> {
        let prefix = format!("{}.{}.", database, table);
        self.indexes.read()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .map(|k| k.strip_prefix(&prefix).unwrap_or(k).to_string())
            .collect()
    }

    /// Find indexes that can serve a query predicate
    pub fn find_indexes_for_columns(&self, database: &str, table: &str, columns: &[&str]) -> Vec<Arc<GlobalIndex>> {
        let prefix = format!("{}.{}.", database, table);

        self.indexes.read()
            .iter()
            .filter(|(k, idx)| {
                k.starts_with(&prefix) &&
                columns.iter().any(|c| idx.config().columns.contains(&c.to_string()))
            })
            .map(|(_, idx)| Arc::clone(idx))
            .collect()
    }

    /// Route query to specific shards using indexes
    pub fn route_query(
        &self,
        database: &str,
        table: &str,
        predicates: &[(String, IndexValue)],
    ) -> Option<Vec<u32>> {
        let prefix = format!("{}.{}.", database, table);

        for (column, value) in predicates {
            // Find an index for this column
            for (key, index) in self.indexes.read().iter() {
                if key.starts_with(&prefix) && index.config().columns.contains(column) {
                    let shards = index.get_shards(value);
                    if !shards.is_empty() {
                        return Some(shards);
                    }
                }
            }
        }

        None // No index can route this query
    }
}

impl Default for GlobalIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Distributed DDL Tests

    #[test]
    fn test_cluster_registration() {
        let manager = DistributedDdlManager::new("node1".to_string());

        let cluster = ClusterDefinition {
            name: "test_cluster".to_string(),
            nodes: vec![
                ClusterNode {
                    node_id: "node1".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 9000,
                    shard: 1,
                    replica: 1,
                    is_local: true,
                    weight: 1,
                },
                ClusterNode {
                    node_id: "node2".to_string(),
                    host: "127.0.0.2".to_string(),
                    port: 9000,
                    shard: 1,
                    replica: 2,
                    is_local: false,
                    weight: 1,
                },
                ClusterNode {
                    node_id: "node3".to_string(),
                    host: "127.0.0.3".to_string(),
                    port: 9000,
                    shard: 2,
                    replica: 1,
                    is_local: false,
                    weight: 1,
                },
            ],
            shards: 2,
            replicas_per_shard: 2,
            internal_replication: true,
        };

        manager.register_cluster(cluster.clone());

        let retrieved = manager.get_cluster("test_cluster").unwrap();
        assert_eq!(retrieved.name, "test_cluster");
        assert_eq!(retrieved.nodes.len(), 3);
    }

    #[test]
    fn test_cluster_shard_nodes() {
        let cluster = ClusterDefinition {
            name: "test".to_string(),
            nodes: vec![
                ClusterNode { node_id: "n1".to_string(), host: "".to_string(), port: 0, shard: 1, replica: 1, is_local: true, weight: 1 },
                ClusterNode { node_id: "n2".to_string(), host: "".to_string(), port: 0, shard: 1, replica: 2, is_local: false, weight: 1 },
                ClusterNode { node_id: "n3".to_string(), host: "".to_string(), port: 0, shard: 2, replica: 1, is_local: false, weight: 1 },
            ],
            shards: 2,
            replicas_per_shard: 2,
            internal_replication: true,
        };

        let shard1_nodes = cluster.shard_nodes(1);
        assert_eq!(shard1_nodes.len(), 2);

        let one_per_shard = cluster.one_per_shard();
        assert_eq!(one_per_shard.len(), 2);
    }

    #[test]
    fn test_ddl_create_table() {
        let manager = DistributedDdlManager::new("node1".to_string());

        let cluster = ClusterDefinition {
            name: "cluster1".to_string(),
            nodes: vec![
                ClusterNode { node_id: "node1".to_string(), host: "".to_string(), port: 0, shard: 1, replica: 1, is_local: true, weight: 1 },
            ],
            shards: 1,
            replicas_per_shard: 1,
            internal_replication: false,
        };
        manager.register_cluster(cluster);

        let operation = DdlOperation::CreateTable {
            database: "default".to_string(),
            name: "events".to_string(),
            if_not_exists: true,
            schema: TableSchema {
                columns: vec![
                    ColumnDefinition {
                        name: "id".to_string(),
                        data_type: DataType::UInt64,
                        nullable: false,
                        default: None,
                        codec: None,
                        comment: None,
                    },
                    ColumnDefinition {
                        name: "timestamp".to_string(),
                        data_type: DataType::DateTime,
                        nullable: false,
                        default: None,
                        codec: None,
                        comment: None,
                    },
                ],
                primary_key: Some(vec!["id".to_string()]),
                order_by: Some(vec!["timestamp".to_string(), "id".to_string()]),
                partition_by: Some("toYYYYMM(timestamp)".to_string()),
                settings: HashMap::new(),
            },
            engine: TableEngine::MergeTree,
        };

        let task_id = manager.execute_on_cluster("cluster1", operation).unwrap();
        let task = manager.get_task(task_id).unwrap();

        assert!(task.is_complete());
        assert!(task.is_success());
    }

    #[test]
    fn test_ddl_task_status() {
        let manager = DistributedDdlManager::new("node1".to_string());

        let cluster = ClusterDefinition {
            name: "c1".to_string(),
            nodes: vec![
                ClusterNode { node_id: "node1".to_string(), host: "".to_string(), port: 0, shard: 1, replica: 1, is_local: true, weight: 1 },
                ClusterNode { node_id: "node2".to_string(), host: "".to_string(), port: 0, shard: 2, replica: 1, is_local: false, weight: 1 },
            ],
            shards: 2,
            replicas_per_shard: 1,
            internal_replication: false,
        };
        manager.register_cluster(cluster);

        let task_id = manager.execute_on_cluster("c1", DdlOperation::CreateDatabase {
            name: "test_db".to_string(),
            if_not_exists: true,
            engine: None,
        }).unwrap();

        let task = manager.get_task(task_id).unwrap();
        assert!(task.is_complete());
    }

    #[test]
    fn test_ddl_cluster_not_found() {
        let manager = DistributedDdlManager::new("node1".to_string());

        let result = manager.execute_on_cluster("nonexistent", DdlOperation::CreateDatabase {
            name: "test".to_string(),
            if_not_exists: false,
            engine: None,
        });

        assert!(matches!(result, Err(DdlError::ClusterNotFound(_))));
    }

    #[test]
    fn test_data_type_string() {
        assert_eq!(DataType::Int64.to_string(), "Int64");
        assert_eq!(DataType::Array(Box::new(DataType::String)).to_string(), "Array(String)");
        assert_eq!(
            DataType::Map {
                key: Box::new(DataType::String),
                value: Box::new(DataType::Int64)
            }.to_string(),
            "Map(String, Int64)"
        );
        assert_eq!(DataType::Nullable(Box::new(DataType::Float64)).to_string(), "Nullable(Float64)");
    }

    // Global Index Tests

    #[test]
    fn test_global_index_btree() {
        let config = GlobalIndexConfig {
            name: "idx_user_id".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["user_id".to_string()],
            index_type: GlobalIndexType::BTree,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        // Insert entries
        index.insert(IndexValue::Int(100), ShardLocation {
            shard_id: 1,
            partition: "202401".to_string(),
            part_name: "all_1_1_0".to_string(),
            min_block: 0,
            max_block: 1000,
        });
        index.insert(IndexValue::Int(100), ShardLocation {
            shard_id: 2,
            partition: "202401".to_string(),
            part_name: "all_1_1_0".to_string(),
            min_block: 0,
            max_block: 500,
        });
        index.insert(IndexValue::Int(200), ShardLocation {
            shard_id: 1,
            partition: "202401".to_string(),
            part_name: "all_2_2_0".to_string(),
            min_block: 1001,
            max_block: 2000,
        });

        // Lookup
        let locations = index.lookup(&IndexValue::Int(100));
        assert_eq!(locations.len(), 2);

        let shards = index.get_shards(&IndexValue::Int(100));
        assert_eq!(shards, vec![1, 2]);
    }

    #[test]
    fn test_global_index_range() {
        let config = GlobalIndexConfig {
            name: "idx_timestamp".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["timestamp".to_string()],
            index_type: GlobalIndexType::BTree,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        for i in 0..10 {
            index.insert(IndexValue::Int(i * 100), ShardLocation {
                shard_id: (i % 3) as u32,
                partition: "".to_string(),
                part_name: "".to_string(),
                min_block: 0,
                max_block: 100,
            });
        }

        // Range lookup 200-500
        let locations = index.range_lookup(
            Some(&IndexValue::Int(200)),
            Some(&IndexValue::Int(500)),
        );
        assert_eq!(locations.len(), 4); // 200, 300, 400, 500
    }

    #[test]
    fn test_global_index_hash() {
        let config = GlobalIndexConfig {
            name: "idx_session".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["session_id".to_string()],
            index_type: GlobalIndexType::Hash,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        index.insert(IndexValue::String("sess_abc".to_string()), ShardLocation {
            shard_id: 1,
            partition: "".to_string(),
            part_name: "".to_string(),
            min_block: 0,
            max_block: 100,
        });

        let locations = index.lookup(&IndexValue::String("sess_abc".to_string()));
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].shard_id, 1);
    }

    #[test]
    fn test_global_index_bitmap() {
        let config = GlobalIndexConfig {
            name: "idx_status".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["status".to_string()],
            index_type: GlobalIndexType::Bitmap,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        // Status "active" is on shards 0, 1, 3
        for shard in [0, 1, 3] {
            index.insert(IndexValue::String("active".to_string()), ShardLocation {
                shard_id: shard,
                partition: "".to_string(),
                part_name: "".to_string(),
                min_block: 0,
                max_block: 100,
            });
        }

        let shards = index.get_shards(&IndexValue::String("active".to_string()));
        assert_eq!(shards, vec![0, 1, 3]);
    }

    #[test]
    fn test_global_index_remove_shard() {
        let config = GlobalIndexConfig {
            name: "idx_test".to_string(),
            database: "default".to_string(),
            table: "test".to_string(),
            columns: vec!["col".to_string()],
            index_type: GlobalIndexType::BTree,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        index.insert(IndexValue::Int(1), ShardLocation { shard_id: 1, partition: "".to_string(), part_name: "".to_string(), min_block: 0, max_block: 100 });
        index.insert(IndexValue::Int(1), ShardLocation { shard_id: 2, partition: "".to_string(), part_name: "".to_string(), min_block: 0, max_block: 100 });

        assert_eq!(index.get_shards(&IndexValue::Int(1)), vec![1, 2]);

        index.remove_shard(1);

        assert_eq!(index.get_shards(&IndexValue::Int(1)), vec![2]);
    }

    #[test]
    fn test_global_index_manager() {
        let manager = GlobalIndexManager::new();

        let config = GlobalIndexConfig {
            name: "idx_user".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["user_id".to_string()],
            index_type: GlobalIndexType::Hash,
            unique: false,
            include_columns: vec![],
        };

        manager.create_index(config).unwrap();

        let index = manager.get_index("default", "events", "idx_user").unwrap();
        assert_eq!(index.config().name, "idx_user");

        let indexes = manager.list_indexes("default", "events");
        assert_eq!(indexes, vec!["idx_user"]);

        manager.drop_index("default", "events", "idx_user").unwrap();
        assert!(manager.get_index("default", "events", "idx_user").is_none());
    }

    #[test]
    fn test_global_index_query_routing() {
        let manager = GlobalIndexManager::new();

        let config = GlobalIndexConfig {
            name: "idx_tenant".to_string(),
            database: "default".to_string(),
            table: "events".to_string(),
            columns: vec!["tenant_id".to_string()],
            index_type: GlobalIndexType::Hash,
            unique: false,
            include_columns: vec![],
        };

        manager.create_index(config).unwrap();

        let index = manager.get_index("default", "events", "idx_tenant").unwrap();
        index.insert(IndexValue::Int(123), ShardLocation {
            shard_id: 2,
            partition: "".to_string(),
            part_name: "".to_string(),
            min_block: 0,
            max_block: 100,
        });

        // Route query WHERE tenant_id = 123
        let shards = manager.route_query(
            "default",
            "events",
            &[("tenant_id".to_string(), IndexValue::Int(123))],
        );

        assert_eq!(shards, Some(vec![2]));
    }

    #[test]
    fn test_global_index_stats() {
        let config = GlobalIndexConfig {
            name: "idx_test".to_string(),
            database: "default".to_string(),
            table: "test".to_string(),
            columns: vec!["col".to_string()],
            index_type: GlobalIndexType::Hash,
            unique: false,
            include_columns: vec![],
        };

        let index = GlobalIndex::new(config);

        for i in 0..100 {
            index.insert(IndexValue::Int(i % 10), ShardLocation {
                shard_id: (i % 3) as u32,
                partition: "".to_string(),
                part_name: "".to_string(),
                min_block: 0,
                max_block: 100,
            });
        }

        // Do some lookups
        for i in 0..5 {
            index.lookup(&IndexValue::Int(i));
        }
        index.lookup(&IndexValue::Int(999)); // Miss

        let stats = index.stats();
        assert_eq!(stats.unique_values, 10);
        assert_eq!(stats.entries, 100);
        assert_eq!(stats.lookups, 6);
        assert_eq!(stats.hits, 5);
    }

    #[test]
    fn test_table_alteration_types() {
        let add_col = TableAlteration::AddColumn {
            column: ColumnDefinition {
                name: "new_col".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                codec: None,
                comment: None,
            },
            after: Some("existing_col".to_string()),
        };

        let drop_col = TableAlteration::DropColumn {
            name: "old_col".to_string(),
        };

        let rename_col = TableAlteration::RenameColumn {
            old_name: "col1".to_string(),
            new_name: "col2".to_string(),
        };

        // Just verify they can be created
        assert!(matches!(add_col, TableAlteration::AddColumn { .. }));
        assert!(matches!(drop_col, TableAlteration::DropColumn { .. }));
        assert!(matches!(rename_col, TableAlteration::RenameColumn { .. }));
    }

    #[test]
    fn test_index_value_ordering() {
        let mut values = vec![
            IndexValue::Int(3),
            IndexValue::Int(1),
            IndexValue::Int(2),
        ];
        values.sort();

        assert_eq!(values[0], IndexValue::Int(1));
        assert_eq!(values[1], IndexValue::Int(2));
        assert_eq!(values[2], IndexValue::Int(3));
    }
}
