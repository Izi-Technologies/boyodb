//! Real-Time & Streaming Module
//!
//! This module provides real-time data processing capabilities:
//! - Kafka/Pulsar connectors for stream ingestion
//! - Incremental materialized views for real-time aggregations
//! - Change Data Capture (CDC) for database synchronization
//! - Exactly-once ingestion with deduplication

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Stream Connector Abstraction (Kafka/Pulsar)
// ============================================================================

/// Message from a stream source
#[derive(Debug, Clone)]
pub struct StreamMessage {
    /// Topic/subject the message came from
    pub topic: String,
    /// Partition/shard ID
    pub partition: u32,
    /// Offset within the partition
    pub offset: u64,
    /// Message key (for partitioning)
    pub key: Option<Vec<u8>>,
    /// Message payload
    pub payload: Vec<u8>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
    /// Message headers/properties
    pub headers: HashMap<String, String>,
}

/// Offset tracking for exactly-once semantics
#[derive(Debug, Clone, Default)]
pub struct ConsumerOffsets {
    /// Map of topic -> partition -> offset
    offsets: HashMap<String, HashMap<u32, u64>>,
}

impl ConsumerOffsets {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(topic).and_then(|p| p.get(&partition).copied())
    }

    pub fn set(&mut self, topic: &str, partition: u32, offset: u64) {
        self.offsets
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    pub fn commit(&self) -> Vec<(String, u32, u64)> {
        let mut result = Vec::new();
        for (topic, partitions) in &self.offsets {
            for (&partition, &offset) in partitions {
                result.push((topic.clone(), partition, offset));
            }
        }
        result
    }
}

/// Stream source configuration
#[derive(Debug, Clone)]
pub struct StreamSourceConfig {
    /// Bootstrap servers (comma-separated)
    pub bootstrap_servers: String,
    /// Consumer group ID
    pub group_id: String,
    /// Topics to subscribe to
    pub topics: Vec<String>,
    /// Auto offset reset policy
    pub auto_offset_reset: OffsetReset,
    /// Maximum messages per poll
    pub max_poll_records: usize,
    /// Poll timeout in milliseconds
    pub poll_timeout_ms: u64,
    /// Enable auto-commit
    pub enable_auto_commit: bool,
    /// Security protocol
    pub security_protocol: SecurityProtocol,
    /// SASL mechanism (if using SASL)
    pub sasl_mechanism: Option<String>,
    /// SASL username
    pub sasl_username: Option<String>,
    /// SASL password
    pub sasl_password: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    Earliest,
    Latest,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Default for StreamSourceConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "boyodb-consumer".to_string(),
            topics: vec![],
            auto_offset_reset: OffsetReset::Latest,
            max_poll_records: 500,
            poll_timeout_ms: 100,
            enable_auto_commit: false,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }
}

/// Abstract stream consumer trait
pub trait StreamConsumer: Send + Sync {
    /// Poll for new messages
    fn poll(&mut self) -> Result<Vec<StreamMessage>, StreamError>;

    /// Commit offsets
    fn commit(&mut self, offsets: &ConsumerOffsets) -> Result<(), StreamError>;

    /// Pause consumption on specific partitions
    fn pause(&mut self, topic: &str, partitions: &[u32]) -> Result<(), StreamError>;

    /// Resume consumption on specific partitions
    fn resume(&mut self, topic: &str, partitions: &[u32]) -> Result<(), StreamError>;

    /// Seek to specific offset
    fn seek(&mut self, topic: &str, partition: u32, offset: u64) -> Result<(), StreamError>;

    /// Get current assignment
    fn assignment(&self) -> Vec<(String, u32)>;

    /// Close the consumer
    fn close(&mut self) -> Result<(), StreamError>;
}

/// Stream producer trait
pub trait StreamProducer: Send + Sync {
    /// Send a message
    fn send(&mut self, message: StreamMessage) -> Result<(), StreamError>;

    /// Send a batch of messages
    fn send_batch(&mut self, messages: Vec<StreamMessage>) -> Result<(), StreamError>;

    /// Flush pending messages
    fn flush(&mut self) -> Result<(), StreamError>;

    /// Close the producer
    fn close(&mut self) -> Result<(), StreamError>;
}

#[derive(Debug)]
pub enum StreamError {
    ConnectionFailed(String),
    AuthenticationFailed(String),
    SerializationError(String),
    DeserializationError(String),
    OffsetOutOfRange(String),
    TopicNotFound(String),
    PartitionNotFound(String),
    Timeout(String),
    ConsumerClosed,
    ProducerClosed,
    Other(String),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::ConnectionFailed(s) => write!(f, "Connection failed: {}", s),
            StreamError::AuthenticationFailed(s) => write!(f, "Authentication failed: {}", s),
            StreamError::SerializationError(s) => write!(f, "Serialization error: {}", s),
            StreamError::DeserializationError(s) => write!(f, "Deserialization error: {}", s),
            StreamError::OffsetOutOfRange(s) => write!(f, "Offset out of range: {}", s),
            StreamError::TopicNotFound(s) => write!(f, "Topic not found: {}", s),
            StreamError::PartitionNotFound(s) => write!(f, "Partition not found: {}", s),
            StreamError::Timeout(s) => write!(f, "Timeout: {}", s),
            StreamError::ConsumerClosed => write!(f, "Consumer is closed"),
            StreamError::ProducerClosed => write!(f, "Producer is closed"),
            StreamError::Other(s) => write!(f, "Stream error: {}", s),
        }
    }
}

impl std::error::Error for StreamError {}

/// Kafka-compatible consumer implementation (protocol simulation)
pub struct KafkaConsumer {
    config: StreamSourceConfig,
    offsets: ConsumerOffsets,
    assignment: Vec<(String, u32)>,
    paused: HashSet<(String, u32)>,
    buffer: VecDeque<StreamMessage>,
    closed: bool,
}

impl KafkaConsumer {
    pub fn new(config: StreamSourceConfig) -> Self {
        // Build initial assignment from topics (assuming 1 partition each for simplicity)
        let assignment: Vec<_> = config.topics.iter()
            .map(|t| (t.clone(), 0u32))
            .collect();

        Self {
            config,
            offsets: ConsumerOffsets::new(),
            assignment,
            paused: HashSet::new(),
            buffer: VecDeque::new(),
            closed: false,
        }
    }

    /// Inject messages for testing
    pub fn inject_messages(&mut self, messages: Vec<StreamMessage>) {
        self.buffer.extend(messages);
    }
}

impl StreamConsumer for KafkaConsumer {
    fn poll(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        if self.closed {
            return Err(StreamError::ConsumerClosed);
        }

        let mut result = Vec::new();
        let max = self.config.max_poll_records;
        let buffer_len = self.buffer.len();
        let mut checked = 0;

        while result.len() < max && checked < buffer_len {
            if let Some(msg) = self.buffer.pop_front() {
                checked += 1;
                // Skip paused partitions
                if self.paused.contains(&(msg.topic.clone(), msg.partition)) {
                    self.buffer.push_back(msg);
                    continue;
                }

                // Update offset tracking
                self.offsets.set(&msg.topic, msg.partition, msg.offset + 1);
                result.push(msg);
            } else {
                break;
            }
        }

        Ok(result)
    }

    fn commit(&mut self, offsets: &ConsumerOffsets) -> Result<(), StreamError> {
        if self.closed {
            return Err(StreamError::ConsumerClosed);
        }

        // In a real implementation, this would commit to Kafka
        for (topic, partition, offset) in offsets.commit() {
            self.offsets.set(&topic, partition, offset);
        }
        Ok(())
    }

    fn pause(&mut self, topic: &str, partitions: &[u32]) -> Result<(), StreamError> {
        for &p in partitions {
            self.paused.insert((topic.to_string(), p));
        }
        Ok(())
    }

    fn resume(&mut self, topic: &str, partitions: &[u32]) -> Result<(), StreamError> {
        for &p in partitions {
            self.paused.remove(&(topic.to_string(), p));
        }
        Ok(())
    }

    fn seek(&mut self, topic: &str, partition: u32, offset: u64) -> Result<(), StreamError> {
        self.offsets.set(topic, partition, offset);
        // In real implementation, would also seek in the buffer
        Ok(())
    }

    fn assignment(&self) -> Vec<(String, u32)> {
        self.assignment.clone()
    }

    fn close(&mut self) -> Result<(), StreamError> {
        self.closed = true;
        Ok(())
    }
}

/// Pulsar-compatible consumer (protocol simulation)
pub struct PulsarConsumer {
    service_url: String,
    subscription: String,
    topics: Vec<String>,
    buffer: VecDeque<StreamMessage>,
    message_ids: HashMap<String, u64>, // topic -> last message ID
    closed: bool,
}

impl PulsarConsumer {
    pub fn new(service_url: &str, subscription: &str, topics: Vec<String>) -> Self {
        Self {
            service_url: service_url.to_string(),
            subscription: subscription.to_string(),
            topics,
            buffer: VecDeque::new(),
            message_ids: HashMap::new(),
            closed: false,
        }
    }

    pub fn inject_messages(&mut self, messages: Vec<StreamMessage>) {
        self.buffer.extend(messages);
    }
}

impl StreamConsumer for PulsarConsumer {
    fn poll(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        if self.closed {
            return Err(StreamError::ConsumerClosed);
        }

        let mut result = Vec::new();
        while let Some(msg) = self.buffer.pop_front() {
            self.message_ids.insert(msg.topic.clone(), msg.offset);
            result.push(msg);
            if result.len() >= 500 {
                break;
            }
        }
        Ok(result)
    }

    fn commit(&mut self, _offsets: &ConsumerOffsets) -> Result<(), StreamError> {
        // Pulsar uses message acknowledgment instead of offset commits
        Ok(())
    }

    fn pause(&mut self, _topic: &str, _partitions: &[u32]) -> Result<(), StreamError> {
        Ok(())
    }

    fn resume(&mut self, _topic: &str, _partitions: &[u32]) -> Result<(), StreamError> {
        Ok(())
    }

    fn seek(&mut self, topic: &str, _partition: u32, offset: u64) -> Result<(), StreamError> {
        self.message_ids.insert(topic.to_string(), offset);
        Ok(())
    }

    fn assignment(&self) -> Vec<(String, u32)> {
        self.topics.iter().map(|t| (t.clone(), 0)).collect()
    }

    fn close(&mut self) -> Result<(), StreamError> {
        self.closed = true;
        Ok(())
    }
}

// ============================================================================
// Incremental Materialized Views
// ============================================================================

/// Aggregation function for materialized views
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
    First,
    Last,
}

/// Aggregate state that can be incrementally updated
#[derive(Debug, Clone)]
pub enum AggregateState {
    Count(i64),
    Sum(f64),
    Min(Option<f64>),
    Max(Option<f64>),
    Avg { sum: f64, count: i64 },
    CountDistinct(HashSet<String>),
    First(Option<f64>),
    Last(Option<f64>),
}

impl AggregateState {
    pub fn new(func: &AggregateFunction) -> Self {
        match func {
            AggregateFunction::Count => AggregateState::Count(0),
            AggregateFunction::Sum => AggregateState::Sum(0.0),
            AggregateFunction::Min => AggregateState::Min(None),
            AggregateFunction::Max => AggregateState::Max(None),
            AggregateFunction::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            AggregateFunction::CountDistinct => AggregateState::CountDistinct(HashSet::new()),
            AggregateFunction::First => AggregateState::First(None),
            AggregateFunction::Last => AggregateState::Last(None),
        }
    }

    /// Apply an INSERT delta
    pub fn apply_insert(&mut self, value: f64, distinct_key: Option<&str>) {
        match self {
            AggregateState::Count(c) => *c += 1,
            AggregateState::Sum(s) => *s += value,
            AggregateState::Min(m) => {
                *m = Some(m.map_or(value, |v| v.min(value)));
            }
            AggregateState::Max(m) => {
                *m = Some(m.map_or(value, |v| v.max(value)));
            }
            AggregateState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AggregateState::CountDistinct(set) => {
                if let Some(key) = distinct_key {
                    set.insert(key.to_string());
                }
            }
            AggregateState::First(f) => {
                if f.is_none() {
                    *f = Some(value);
                }
            }
            AggregateState::Last(l) => {
                *l = Some(value);
            }
        }
    }

    /// Apply a DELETE delta (for some aggregates)
    pub fn apply_delete(&mut self, value: f64, _distinct_key: Option<&str>) {
        match self {
            AggregateState::Count(c) => *c = (*c - 1).max(0),
            AggregateState::Sum(s) => *s -= value,
            AggregateState::Avg { sum, count } => {
                *sum -= value;
                *count = (*count - 1).max(0);
            }
            // Min/Max/First/Last cannot be efficiently decremented
            // CountDistinct would need reference counting
            _ => {}
        }
    }

    /// Get the current value
    pub fn value(&self) -> Option<f64> {
        match self {
            AggregateState::Count(c) => Some(*c as f64),
            AggregateState::Sum(s) => Some(*s),
            AggregateState::Min(m) => *m,
            AggregateState::Max(m) => *m,
            AggregateState::Avg { sum, count } => {
                if *count > 0 {
                    Some(*sum / *count as f64)
                } else {
                    None
                }
            }
            AggregateState::CountDistinct(set) => Some(set.len() as f64),
            AggregateState::First(f) => *f,
            AggregateState::Last(l) => *l,
        }
    }
}

/// Column definition for materialized view
#[derive(Debug, Clone)]
pub struct MaterializedColumn {
    /// Output column name
    pub name: String,
    /// Source column name (for aggregates)
    pub source_column: Option<String>,
    /// Aggregate function (None for group-by columns)
    pub aggregate: Option<AggregateFunction>,
}

/// Materialized view definition
#[derive(Debug, Clone)]
pub struct MaterializedViewDef {
    /// View name
    pub name: String,
    /// Source table
    pub source_table: String,
    /// Group by columns
    pub group_by: Vec<String>,
    /// Output columns (aggregates)
    pub columns: Vec<MaterializedColumn>,
    /// Filter expression (WHERE clause)
    pub filter: Option<String>,
    /// Refresh mode
    pub refresh_mode: RefreshMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshMode {
    /// Update on every insert
    Immediate,
    /// Update periodically
    Periodic,
    /// Update on demand
    Manual,
}

/// Row delta for incremental updates
#[derive(Debug, Clone)]
pub struct RowDelta {
    /// Operation type
    pub op: DeltaOp,
    /// Column values (name -> value)
    pub values: HashMap<String, DeltaValue>,
    /// Timestamp of the change
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOp {
    Insert,
    Delete,
    Update,
}

#[derive(Debug, Clone)]
pub enum DeltaValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl DeltaValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DeltaValue::Int(i) => Some(*i as f64),
            DeltaValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            DeltaValue::String(s) => Some(s.clone()),
            DeltaValue::Int(i) => Some(i.to_string()),
            DeltaValue::Float(f) => Some(f.to_string()),
            DeltaValue::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
}

/// Incremental materialized view engine
pub struct MaterializedView {
    /// View definition
    pub def: MaterializedViewDef,
    /// Current state: group_key -> column_name -> aggregate_state
    state: HashMap<String, HashMap<String, AggregateState>>,
    /// Last refresh timestamp
    last_refresh: u64,
    /// Pending deltas (for periodic refresh)
    pending_deltas: Vec<RowDelta>,
}

impl MaterializedView {
    pub fn new(def: MaterializedViewDef) -> Self {
        Self {
            def,
            state: HashMap::new(),
            last_refresh: 0,
            pending_deltas: Vec::new(),
        }
    }

    /// Build group key from row values
    fn build_group_key(&self, values: &HashMap<String, DeltaValue>) -> String {
        let mut parts = Vec::new();
        for col in &self.def.group_by {
            let val = values.get(col)
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "NULL".to_string());
            parts.push(val);
        }
        parts.join("|")
    }

    /// Apply a single delta immediately
    pub fn apply_delta(&mut self, delta: &RowDelta) {
        if self.def.refresh_mode == RefreshMode::Periodic {
            self.pending_deltas.push(delta.clone());
            return;
        }

        self.apply_delta_internal(delta);
    }

    fn apply_delta_internal(&mut self, delta: &RowDelta) {
        let group_key = self.build_group_key(&delta.values);

        // Get or create group state
        let group_state = self.state.entry(group_key).or_insert_with(|| {
            let mut cols = HashMap::new();
            for col in &self.def.columns {
                if let Some(ref agg) = col.aggregate {
                    cols.insert(col.name.clone(), AggregateState::new(agg));
                }
            }
            cols
        });

        // Apply delta to each aggregate
        for col in &self.def.columns {
            if let Some(ref agg) = col.aggregate {
                if let Some(state) = group_state.get_mut(&col.name) {
                    let value = col.source_column.as_ref()
                        .and_then(|src| delta.values.get(src))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);

                    let distinct_key = col.source_column.as_ref()
                        .and_then(|src| delta.values.get(src))
                        .and_then(|v| v.as_string());

                    match delta.op {
                        DeltaOp::Insert => state.apply_insert(value, distinct_key.as_deref()),
                        DeltaOp::Delete => state.apply_delete(value, distinct_key.as_deref()),
                        DeltaOp::Update => {
                            // Update = Delete old + Insert new (simplified)
                            state.apply_insert(value, distinct_key.as_deref());
                        }
                    }
                }
            }
        }
    }

    /// Refresh periodic view (apply pending deltas)
    pub fn refresh(&mut self) {
        let deltas: Vec<_> = self.pending_deltas.drain(..).collect();
        for delta in deltas {
            self.apply_delta_internal(&delta);
        }
        self.last_refresh = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Query the materialized view
    pub fn query(&self) -> Vec<HashMap<String, Option<f64>>> {
        let mut results = Vec::new();

        for (group_key, aggregates) in &self.state {
            let mut row = HashMap::new();

            // Parse group key back to columns
            let parts: Vec<_> = group_key.split('|').collect();
            for (i, col) in self.def.group_by.iter().enumerate() {
                if let Some(part) = parts.get(i) {
                    if let Ok(v) = part.parse::<f64>() {
                        row.insert(col.clone(), Some(v));
                    }
                }
            }

            // Add aggregate values
            for (name, state) in aggregates {
                row.insert(name.clone(), state.value());
            }

            results.push(row);
        }

        results
    }

    /// Get aggregate value for a specific group
    pub fn get(&self, group_key: &str, column: &str) -> Option<f64> {
        self.state.get(group_key)
            .and_then(|g| g.get(column))
            .and_then(|s| s.value())
    }
}

// ============================================================================
// Change Data Capture (CDC)
// ============================================================================

/// CDC event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcEventType {
    Insert,
    Update,
    Delete,
    Truncate,
    SchemaChange,
}

/// CDC event representing a change in the source database
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// Unique sequence number
    pub sequence: u64,
    /// Event type
    pub event_type: CdcEventType,
    /// Source database
    pub database: String,
    /// Source table
    pub table: String,
    /// Primary key values
    pub primary_key: HashMap<String, DeltaValue>,
    /// Before image (for update/delete)
    pub before: Option<HashMap<String, DeltaValue>>,
    /// After image (for insert/update)
    pub after: Option<HashMap<String, DeltaValue>>,
    /// Transaction ID
    pub transaction_id: Option<String>,
    /// Event timestamp
    pub timestamp: u64,
    /// Commit timestamp (for transactional CDC)
    pub commit_timestamp: Option<u64>,
}

/// CDC checkpoint for resumption
#[derive(Debug, Clone)]
pub struct CdcCheckpoint {
    /// Last processed sequence number
    pub sequence: u64,
    /// Last processed transaction ID
    pub transaction_id: Option<String>,
    /// Checkpoint timestamp
    pub timestamp: u64,
    /// Source-specific position (e.g., binlog position)
    pub source_position: Option<String>,
}

/// CDC source configuration
#[derive(Debug, Clone)]
pub struct CdcSourceConfig {
    /// Source type
    pub source_type: CdcSourceType,
    /// Connection string
    pub connection: String,
    /// Tables to capture
    pub tables: Vec<String>,
    /// Include before image in updates
    pub include_before_image: bool,
    /// Snapshot on initial start
    pub snapshot_on_start: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcSourceType {
    /// MySQL binlog
    MySqlBinlog,
    /// PostgreSQL logical replication
    PostgresLogical,
    /// MongoDB oplog
    MongoOplog,
    /// Generic WAL-based
    WalBased,
}

/// CDC capture engine
pub struct CdcCapture {
    config: CdcSourceConfig,
    /// Current checkpoint
    checkpoint: CdcCheckpoint,
    /// Event buffer
    buffer: VecDeque<CdcEvent>,
    /// Transaction buffer (for atomic commits)
    transaction_buffer: HashMap<String, Vec<CdcEvent>>,
    /// Next sequence number
    next_sequence: u64,
}

impl CdcCapture {
    pub fn new(config: CdcSourceConfig) -> Self {
        Self {
            config,
            checkpoint: CdcCheckpoint {
                sequence: 0,
                transaction_id: None,
                timestamp: 0,
                source_position: None,
            },
            buffer: VecDeque::new(),
            transaction_buffer: HashMap::new(),
            next_sequence: 1,
        }
    }

    /// Start capturing (would connect to source in real implementation)
    pub fn start(&mut self) -> Result<(), CdcError> {
        // In real implementation, would connect to source database
        // and start reading change events
        Ok(())
    }

    /// Emit a CDC event (used by source connectors)
    pub fn emit(&mut self, mut event: CdcEvent) -> u64 {
        event.sequence = self.next_sequence;
        self.next_sequence += 1;

        if let Some(ref txn_id) = event.transaction_id {
            // Buffer transactional events until commit
            self.transaction_buffer
                .entry(txn_id.clone())
                .or_default()
                .push(event);
        } else {
            self.buffer.push_back(event);
        }

        self.next_sequence - 1
    }

    /// Commit a transaction (flush buffered events)
    pub fn commit_transaction(&mut self, transaction_id: &str) {
        if let Some(events) = self.transaction_buffer.remove(transaction_id) {
            for event in events {
                self.buffer.push_back(event);
            }
        }
    }

    /// Rollback a transaction (discard buffered events)
    pub fn rollback_transaction(&mut self, transaction_id: &str) {
        self.transaction_buffer.remove(transaction_id);
    }

    /// Poll for new events
    pub fn poll(&mut self, max_events: usize) -> Vec<CdcEvent> {
        let mut events = Vec::new();
        while events.len() < max_events {
            if let Some(event) = self.buffer.pop_front() {
                self.checkpoint.sequence = event.sequence;
                self.checkpoint.timestamp = event.timestamp;
                self.checkpoint.transaction_id = event.transaction_id.clone();
                events.push(event);
            } else {
                break;
            }
        }
        events
    }

    /// Get current checkpoint
    pub fn checkpoint(&self) -> &CdcCheckpoint {
        &self.checkpoint
    }

    /// Resume from checkpoint
    pub fn resume_from(&mut self, checkpoint: CdcCheckpoint) {
        self.next_sequence = checkpoint.sequence + 1;
        self.checkpoint = checkpoint;
    }
}

#[derive(Debug)]
pub enum CdcError {
    ConnectionFailed(String),
    SnapshotFailed(String),
    ParseError(String),
    CheckpointError(String),
    Other(String),
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcError::ConnectionFailed(s) => write!(f, "CDC connection failed: {}", s),
            CdcError::SnapshotFailed(s) => write!(f, "CDC snapshot failed: {}", s),
            CdcError::ParseError(s) => write!(f, "CDC parse error: {}", s),
            CdcError::CheckpointError(s) => write!(f, "CDC checkpoint error: {}", s),
            CdcError::Other(s) => write!(f, "CDC error: {}", s),
        }
    }
}

impl std::error::Error for CdcError {}

// ============================================================================
// Exactly-Once Ingestion
// ============================================================================

/// Deduplication key extracted from messages
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct DeduplicationKey {
    /// Source identifier (topic, table, etc.)
    pub source: String,
    /// Unique key within source
    pub key: String,
}

/// Ingestion transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionState {
    /// Transaction started
    Started,
    /// Data written but not committed
    Prepared,
    /// Transaction committed
    Committed,
    /// Transaction aborted
    Aborted,
}

/// Ingestion transaction for exactly-once semantics
#[derive(Debug)]
pub struct IngestionTransaction {
    /// Unique transaction ID
    pub id: String,
    /// Transaction state
    pub state: IngestionState,
    /// Source offsets included in this transaction
    pub source_offsets: ConsumerOffsets,
    /// Records to be written
    pub records: Vec<IngestRecord>,
    /// Start timestamp
    pub start_time: u64,
    /// Deduplication keys for this transaction
    pub dedup_keys: Vec<DeduplicationKey>,
}

/// Record to be ingested
#[derive(Debug, Clone)]
pub struct IngestRecord {
    /// Target table
    pub table: String,
    /// Column values
    pub values: HashMap<String, DeltaValue>,
    /// Deduplication key
    pub dedup_key: Option<DeduplicationKey>,
}

/// Exactly-once ingestion engine
pub struct ExactlyOnceIngestor {
    /// Active transactions
    transactions: HashMap<String, IngestionTransaction>,
    /// Deduplication index (key -> transaction_id)
    dedup_index: HashMap<DeduplicationKey, String>,
    /// Committed dedup keys (with expiry timestamp)
    committed_keys: HashMap<DeduplicationKey, u64>,
    /// Dedup key TTL (milliseconds)
    dedup_ttl_ms: u64,
    /// Transaction timeout (milliseconds)
    transaction_timeout_ms: u64,
    /// Next transaction ID
    next_txn_id: u64,
}

impl ExactlyOnceIngestor {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
            dedup_index: HashMap::new(),
            committed_keys: HashMap::new(),
            dedup_ttl_ms: 24 * 60 * 60 * 1000, // 24 hours
            transaction_timeout_ms: 60 * 1000, // 1 minute
            next_txn_id: 1,
        }
    }

    pub fn with_dedup_ttl(mut self, ttl_ms: u64) -> Self {
        self.dedup_ttl_ms = ttl_ms;
        self
    }

    pub fn with_transaction_timeout(mut self, timeout_ms: u64) -> Self {
        self.transaction_timeout_ms = timeout_ms;
        self
    }

    /// Begin a new ingestion transaction
    pub fn begin(&mut self) -> String {
        let id = format!("txn_{}", self.next_txn_id);
        self.next_txn_id += 1;

        let txn = IngestionTransaction {
            id: id.clone(),
            state: IngestionState::Started,
            source_offsets: ConsumerOffsets::new(),
            records: Vec::new(),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            dedup_keys: Vec::new(),
        };

        self.transactions.insert(id.clone(), txn);
        id
    }

    /// Add a record to the transaction
    pub fn add_record(&mut self, txn_id: &str, record: IngestRecord) -> Result<(), IngestionError> {
        let txn = self.transactions.get_mut(txn_id)
            .ok_or(IngestionError::TransactionNotFound)?;

        if txn.state != IngestionState::Started {
            return Err(IngestionError::InvalidState);
        }

        // Check for duplicate
        if let Some(ref key) = record.dedup_key {
            // Check against committed keys
            if self.committed_keys.contains_key(key) {
                return Err(IngestionError::DuplicateRecord(key.clone()));
            }

            // Check against in-flight transactions
            if let Some(other_txn) = self.dedup_index.get(key) {
                if other_txn != txn_id {
                    return Err(IngestionError::DuplicateRecord(key.clone()));
                }
            }

            self.dedup_index.insert(key.clone(), txn_id.to_string());
            txn.dedup_keys.push(key.clone());
        }

        txn.records.push(record);
        Ok(())
    }

    /// Set source offsets for the transaction
    pub fn set_offsets(&mut self, txn_id: &str, offsets: ConsumerOffsets) -> Result<(), IngestionError> {
        let txn = self.transactions.get_mut(txn_id)
            .ok_or(IngestionError::TransactionNotFound)?;

        txn.source_offsets = offsets;
        Ok(())
    }

    /// Prepare transaction (write to WAL but don't commit)
    pub fn prepare(&mut self, txn_id: &str) -> Result<(), IngestionError> {
        let txn = self.transactions.get_mut(txn_id)
            .ok_or(IngestionError::TransactionNotFound)?;

        if txn.state != IngestionState::Started {
            return Err(IngestionError::InvalidState);
        }

        // In real implementation, would write to WAL here
        txn.state = IngestionState::Prepared;
        Ok(())
    }

    /// Commit transaction
    pub fn commit(&mut self, txn_id: &str) -> Result<Vec<IngestRecord>, IngestionError> {
        let txn = self.transactions.get_mut(txn_id)
            .ok_or(IngestionError::TransactionNotFound)?;

        if txn.state != IngestionState::Started && txn.state != IngestionState::Prepared {
            return Err(IngestionError::InvalidState);
        }

        txn.state = IngestionState::Committed;

        // Move dedup keys to committed set
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expiry = now + self.dedup_ttl_ms;

        for key in &txn.dedup_keys {
            self.dedup_index.remove(key);
            self.committed_keys.insert(key.clone(), expiry);
        }

        // Return records for actual insertion
        let records = txn.records.clone();

        // Clean up transaction
        self.transactions.remove(txn_id);

        Ok(records)
    }

    /// Abort transaction
    pub fn abort(&mut self, txn_id: &str) -> Result<(), IngestionError> {
        let txn = self.transactions.get_mut(txn_id)
            .ok_or(IngestionError::TransactionNotFound)?;

        txn.state = IngestionState::Aborted;

        // Remove dedup keys
        for key in &txn.dedup_keys {
            self.dedup_index.remove(key);
        }

        self.transactions.remove(txn_id);
        Ok(())
    }

    /// Clean up expired dedup keys and timed-out transactions
    pub fn cleanup(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Clean up expired dedup keys
        self.committed_keys.retain(|_, expiry| *expiry > now);

        // Clean up timed-out transactions
        let timeout_threshold = now.saturating_sub(self.transaction_timeout_ms);
        let timed_out: Vec<_> = self.transactions.iter()
            .filter(|(_, txn)| txn.start_time < timeout_threshold && txn.state == IngestionState::Started)
            .map(|(id, _)| id.clone())
            .collect();

        for txn_id in timed_out {
            let _ = self.abort(&txn_id);
        }
    }

    /// Check if a key has already been committed
    pub fn is_duplicate(&self, key: &DeduplicationKey) -> bool {
        self.committed_keys.contains_key(key) || self.dedup_index.contains_key(key)
    }

    /// Get transaction state
    pub fn transaction_state(&self, txn_id: &str) -> Option<IngestionState> {
        self.transactions.get(txn_id).map(|t| t.state)
    }
}

impl Default for ExactlyOnceIngestor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum IngestionError {
    TransactionNotFound,
    InvalidState,
    DuplicateRecord(DeduplicationKey),
    WriteError(String),
    Other(String),
}

impl std::fmt::Display for IngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestionError::TransactionNotFound => write!(f, "Transaction not found"),
            IngestionError::InvalidState => write!(f, "Invalid transaction state"),
            IngestionError::DuplicateRecord(k) => write!(f, "Duplicate record: {:?}", k),
            IngestionError::WriteError(s) => write!(f, "Write error: {}", s),
            IngestionError::Other(s) => write!(f, "Ingestion error: {}", s),
        }
    }
}

impl std::error::Error for IngestionError {}

// ============================================================================
// Stream Processing Pipeline
// ============================================================================

/// Stream processing stage
pub trait StreamStage: Send + Sync {
    /// Process a batch of messages
    fn process(&mut self, messages: Vec<StreamMessage>) -> Result<Vec<StreamMessage>, StreamError>;

    /// Flush any buffered state
    fn flush(&mut self) -> Result<Vec<StreamMessage>, StreamError>;
}

/// Filter stage - drops messages not matching predicate
pub struct FilterStage<F>
where
    F: Fn(&StreamMessage) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> FilterStage<F>
where
    F: Fn(&StreamMessage) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> StreamStage for FilterStage<F>
where
    F: Fn(&StreamMessage) -> bool + Send + Sync,
{
    fn process(&mut self, messages: Vec<StreamMessage>) -> Result<Vec<StreamMessage>, StreamError> {
        Ok(messages.into_iter().filter(|m| (self.predicate)(m)).collect())
    }

    fn flush(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        Ok(vec![])
    }
}

/// Transform stage - maps messages
pub struct TransformStage<F>
where
    F: Fn(StreamMessage) -> StreamMessage + Send + Sync,
{
    transform: F,
}

impl<F> TransformStage<F>
where
    F: Fn(StreamMessage) -> StreamMessage + Send + Sync,
{
    pub fn new(transform: F) -> Self {
        Self { transform }
    }
}

impl<F> StreamStage for TransformStage<F>
where
    F: Fn(StreamMessage) -> StreamMessage + Send + Sync,
{
    fn process(&mut self, messages: Vec<StreamMessage>) -> Result<Vec<StreamMessage>, StreamError> {
        Ok(messages.into_iter().map(|m| (self.transform)(m)).collect())
    }

    fn flush(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        Ok(vec![])
    }
}

/// Windowed aggregation stage
pub struct WindowAggregateStage {
    /// Window size in milliseconds
    window_size_ms: u64,
    /// Window slide in milliseconds (for sliding windows)
    slide_ms: u64,
    /// Key extractor function name
    key_column: String,
    /// Value column to aggregate
    value_column: String,
    /// Aggregate function
    aggregate: AggregateFunction,
    /// Window buffers: window_start -> key -> aggregate_state
    windows: HashMap<u64, HashMap<String, AggregateState>>,
    /// Watermark (latest event time seen)
    watermark: u64,
}

impl WindowAggregateStage {
    pub fn tumbling(window_size_ms: u64, key_column: &str, value_column: &str, aggregate: AggregateFunction) -> Self {
        Self {
            window_size_ms,
            slide_ms: window_size_ms, // Tumbling = slide equals size
            key_column: key_column.to_string(),
            value_column: value_column.to_string(),
            aggregate,
            windows: HashMap::new(),
            watermark: 0,
        }
    }

    pub fn sliding(window_size_ms: u64, slide_ms: u64, key_column: &str, value_column: &str, aggregate: AggregateFunction) -> Self {
        Self {
            window_size_ms,
            slide_ms,
            key_column: key_column.to_string(),
            value_column: value_column.to_string(),
            aggregate,
            windows: HashMap::new(),
            watermark: 0,
        }
    }

    fn window_start(&self, timestamp: u64) -> u64 {
        (timestamp / self.slide_ms) * self.slide_ms
    }

    fn emit_closed_windows(&mut self) -> Vec<StreamMessage> {
        let mut result = Vec::new();
        let closed_threshold = self.watermark.saturating_sub(self.window_size_ms);

        let closed_windows: Vec<_> = self.windows.keys()
            .filter(|&&start| start + self.window_size_ms <= closed_threshold)
            .copied()
            .collect();

        for window_start in closed_windows {
            if let Some(aggregates) = self.windows.remove(&window_start) {
                for (key, state) in aggregates {
                    if let Some(value) = state.value() {
                        let mut headers = HashMap::new();
                        headers.insert("window_start".to_string(), window_start.to_string());
                        headers.insert("window_end".to_string(), (window_start + self.window_size_ms).to_string());
                        headers.insert("key".to_string(), key);
                        headers.insert("value".to_string(), value.to_string());

                        result.push(StreamMessage {
                            topic: "aggregates".to_string(),
                            partition: 0,
                            offset: 0,
                            key: None,
                            payload: value.to_be_bytes().to_vec(),
                            timestamp: window_start + self.window_size_ms,
                            headers,
                        });
                    }
                }
            }
        }

        result
    }
}

impl StreamStage for WindowAggregateStage {
    fn process(&mut self, messages: Vec<StreamMessage>) -> Result<Vec<StreamMessage>, StreamError> {
        for msg in messages {
            // Update watermark
            if msg.timestamp > self.watermark {
                self.watermark = msg.timestamp;
            }

            // Extract key and value from headers (simplified)
            let key = msg.headers.get(&self.key_column)
                .cloned()
                .unwrap_or_else(|| "default".to_string());

            let value: f64 = msg.headers.get(&self.value_column)
                .and_then(|v| v.parse().ok())
                .unwrap_or(1.0); // Default to 1 for count

            // Determine which windows this message belongs to
            let window_start = self.window_start(msg.timestamp);

            // For sliding windows, message may belong to multiple windows
            let mut ws = window_start;
            while ws + self.window_size_ms > msg.timestamp {
                let window_aggs = self.windows.entry(ws).or_default();
                let state = window_aggs.entry(key.clone())
                    .or_insert_with(|| AggregateState::new(&self.aggregate));
                state.apply_insert(value, Some(&key));

                if ws < self.slide_ms {
                    break;
                }
                ws -= self.slide_ms;
            }
        }

        // Emit closed windows
        Ok(self.emit_closed_windows())
    }

    fn flush(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        // Force close all windows
        self.watermark = u64::MAX;
        Ok(self.emit_closed_windows())
    }
}

/// Stream processing pipeline
pub struct StreamPipeline {
    stages: Vec<Box<dyn StreamStage>>,
}

impl StreamPipeline {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    pub fn add_stage<S: StreamStage + 'static>(mut self, stage: S) -> Self {
        self.stages.push(Box::new(stage));
        self
    }

    pub fn process(&mut self, messages: Vec<StreamMessage>) -> Result<Vec<StreamMessage>, StreamError> {
        let mut current = messages;
        for stage in &mut self.stages {
            current = stage.process(current)?;
        }
        Ok(current)
    }

    pub fn flush(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        let mut result = Vec::new();
        for stage in &mut self.stages {
            let flushed = stage.flush()?;
            result.extend(flushed);
        }
        Ok(result)
    }
}

impl Default for StreamPipeline {
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

    #[test]
    fn test_consumer_offsets() {
        let mut offsets = ConsumerOffsets::new();
        offsets.set("topic1", 0, 100);
        offsets.set("topic1", 1, 200);
        offsets.set("topic2", 0, 50);

        assert_eq!(offsets.get("topic1", 0), Some(100));
        assert_eq!(offsets.get("topic1", 1), Some(200));
        assert_eq!(offsets.get("topic2", 0), Some(50));
        assert_eq!(offsets.get("topic3", 0), None);

        let committed = offsets.commit();
        assert_eq!(committed.len(), 3);
    }

    #[test]
    fn test_kafka_consumer() {
        let config = StreamSourceConfig {
            topics: vec!["test-topic".to_string()],
            ..Default::default()
        };

        let mut consumer = KafkaConsumer::new(config);

        // Inject test messages
        consumer.inject_messages(vec![
            StreamMessage {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 0,
                key: Some(b"key1".to_vec()),
                payload: b"value1".to_vec(),
                timestamp: 1000,
                headers: HashMap::new(),
            },
            StreamMessage {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 1,
                key: Some(b"key2".to_vec()),
                payload: b"value2".to_vec(),
                timestamp: 1001,
                headers: HashMap::new(),
            },
        ]);

        let messages = consumer.poll().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].offset, 0);
        assert_eq!(messages[1].offset, 1);
    }

    #[test]
    fn test_kafka_consumer_pause_resume() {
        let config = StreamSourceConfig {
            topics: vec!["test-topic".to_string()],
            ..Default::default()
        };

        let mut consumer = KafkaConsumer::new(config);

        consumer.inject_messages(vec![
            StreamMessage {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 0,
                key: None,
                payload: b"value1".to_vec(),
                timestamp: 1000,
                headers: HashMap::new(),
            },
        ]);

        // Pause partition 0
        consumer.pause("test-topic", &[0]).unwrap();

        let messages = consumer.poll().unwrap();
        assert_eq!(messages.len(), 0); // Should not receive paused partition

        // Resume
        consumer.resume("test-topic", &[0]).unwrap();

        let messages = consumer.poll().unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[test]
    fn test_pulsar_consumer() {
        let mut consumer = PulsarConsumer::new(
            "pulsar://localhost:6650",
            "test-subscription",
            vec!["test-topic".to_string()],
        );

        consumer.inject_messages(vec![
            StreamMessage {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 0,
                key: None,
                payload: b"pulsar-msg".to_vec(),
                timestamp: 1000,
                headers: HashMap::new(),
            },
        ]);

        let messages = consumer.poll().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"pulsar-msg");
    }

    #[test]
    fn test_materialized_view_count() {
        let def = MaterializedViewDef {
            name: "user_counts".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["user_id".to_string()],
            columns: vec![
                MaterializedColumn {
                    name: "event_count".to_string(),
                    source_column: None,
                    aggregate: Some(AggregateFunction::Count),
                },
            ],
            filter: None,
            refresh_mode: RefreshMode::Immediate,
        };

        let mut view = MaterializedView::new(def);

        // Insert events for user1
        for _ in 0..5 {
            let mut values = HashMap::new();
            values.insert("user_id".to_string(), DeltaValue::String("user1".to_string()));
            view.apply_delta(&RowDelta {
                op: DeltaOp::Insert,
                values,
                timestamp: 1000,
            });
        }

        // Insert events for user2
        for _ in 0..3 {
            let mut values = HashMap::new();
            values.insert("user_id".to_string(), DeltaValue::String("user2".to_string()));
            view.apply_delta(&RowDelta {
                op: DeltaOp::Insert,
                values,
                timestamp: 1001,
            });
        }

        assert_eq!(view.get("user1", "event_count"), Some(5.0));
        assert_eq!(view.get("user2", "event_count"), Some(3.0));
    }

    #[test]
    fn test_materialized_view_sum() {
        let def = MaterializedViewDef {
            name: "revenue".to_string(),
            source_table: "orders".to_string(),
            group_by: vec!["product".to_string()],
            columns: vec![
                MaterializedColumn {
                    name: "total_revenue".to_string(),
                    source_column: Some("amount".to_string()),
                    aggregate: Some(AggregateFunction::Sum),
                },
            ],
            filter: None,
            refresh_mode: RefreshMode::Immediate,
        };

        let mut view = MaterializedView::new(def);

        let amounts = [100.0, 200.0, 150.0];
        for amount in amounts {
            let mut values = HashMap::new();
            values.insert("product".to_string(), DeltaValue::String("widget".to_string()));
            values.insert("amount".to_string(), DeltaValue::Float(amount));
            view.apply_delta(&RowDelta {
                op: DeltaOp::Insert,
                values,
                timestamp: 1000,
            });
        }

        assert_eq!(view.get("widget", "total_revenue"), Some(450.0));
    }

    #[test]
    fn test_materialized_view_avg() {
        let def = MaterializedViewDef {
            name: "avg_latency".to_string(),
            source_table: "requests".to_string(),
            group_by: vec!["endpoint".to_string()],
            columns: vec![
                MaterializedColumn {
                    name: "avg_latency_ms".to_string(),
                    source_column: Some("latency".to_string()),
                    aggregate: Some(AggregateFunction::Avg),
                },
            ],
            filter: None,
            refresh_mode: RefreshMode::Immediate,
        };

        let mut view = MaterializedView::new(def);

        let latencies = [10.0, 20.0, 30.0, 40.0];
        for latency in latencies {
            let mut values = HashMap::new();
            values.insert("endpoint".to_string(), DeltaValue::String("/api/users".to_string()));
            values.insert("latency".to_string(), DeltaValue::Float(latency));
            view.apply_delta(&RowDelta {
                op: DeltaOp::Insert,
                values,
                timestamp: 1000,
            });
        }

        assert_eq!(view.get("/api/users", "avg_latency_ms"), Some(25.0));
    }

    #[test]
    fn test_materialized_view_periodic_refresh() {
        let def = MaterializedViewDef {
            name: "periodic_count".to_string(),
            source_table: "events".to_string(),
            group_by: vec!["category".to_string()],
            columns: vec![
                MaterializedColumn {
                    name: "count".to_string(),
                    source_column: None,
                    aggregate: Some(AggregateFunction::Count),
                },
            ],
            filter: None,
            refresh_mode: RefreshMode::Periodic,
        };

        let mut view = MaterializedView::new(def);

        // Add deltas (should be buffered)
        for _ in 0..5 {
            let mut values = HashMap::new();
            values.insert("category".to_string(), DeltaValue::String("A".to_string()));
            view.apply_delta(&RowDelta {
                op: DeltaOp::Insert,
                values,
                timestamp: 1000,
            });
        }

        // Before refresh, should be empty
        assert_eq!(view.get("A", "count"), None);

        // Refresh
        view.refresh();

        // After refresh, should have count
        assert_eq!(view.get("A", "count"), Some(5.0));
    }

    #[test]
    fn test_cdc_capture() {
        let config = CdcSourceConfig {
            source_type: CdcSourceType::PostgresLogical,
            connection: "postgres://localhost/test".to_string(),
            tables: vec!["users".to_string()],
            include_before_image: true,
            snapshot_on_start: false,
        };

        let mut cdc = CdcCapture::new(config);
        cdc.start().unwrap();

        // Emit an insert event
        let mut after = HashMap::new();
        after.insert("id".to_string(), DeltaValue::Int(1));
        after.insert("name".to_string(), DeltaValue::String("Alice".to_string()));

        let mut pk = HashMap::new();
        pk.insert("id".to_string(), DeltaValue::Int(1));

        let seq = cdc.emit(CdcEvent {
            sequence: 0, // Will be assigned
            event_type: CdcEventType::Insert,
            database: "test".to_string(),
            table: "users".to_string(),
            primary_key: pk,
            before: None,
            after: Some(after),
            transaction_id: None,
            timestamp: 1000,
            commit_timestamp: None,
        });

        assert_eq!(seq, 1);

        let events = cdc.poll(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, CdcEventType::Insert);
        assert_eq!(events[0].sequence, 1);
    }

    #[test]
    fn test_cdc_transactional() {
        let config = CdcSourceConfig {
            source_type: CdcSourceType::MySqlBinlog,
            connection: "mysql://localhost/test".to_string(),
            tables: vec!["orders".to_string()],
            include_before_image: false,
            snapshot_on_start: false,
        };

        let mut cdc = CdcCapture::new(config);

        // Emit events in a transaction
        let mut pk = HashMap::new();
        pk.insert("id".to_string(), DeltaValue::Int(1));

        cdc.emit(CdcEvent {
            sequence: 0,
            event_type: CdcEventType::Insert,
            database: "test".to_string(),
            table: "orders".to_string(),
            primary_key: pk.clone(),
            before: None,
            after: Some(pk.clone()),
            transaction_id: Some("txn1".to_string()),
            timestamp: 1000,
            commit_timestamp: None,
        });

        // Before commit, poll returns nothing
        let events = cdc.poll(10);
        assert_eq!(events.len(), 0);

        // Commit transaction
        cdc.commit_transaction("txn1");

        // After commit, poll returns events
        let events = cdc.poll(10);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_cdc_rollback() {
        let config = CdcSourceConfig {
            source_type: CdcSourceType::MySqlBinlog,
            connection: "mysql://localhost/test".to_string(),
            tables: vec!["orders".to_string()],
            include_before_image: false,
            snapshot_on_start: false,
        };

        let mut cdc = CdcCapture::new(config);

        let mut pk = HashMap::new();
        pk.insert("id".to_string(), DeltaValue::Int(1));

        cdc.emit(CdcEvent {
            sequence: 0,
            event_type: CdcEventType::Insert,
            database: "test".to_string(),
            table: "orders".to_string(),
            primary_key: pk.clone(),
            before: None,
            after: Some(pk),
            transaction_id: Some("txn1".to_string()),
            timestamp: 1000,
            commit_timestamp: None,
        });

        // Rollback transaction
        cdc.rollback_transaction("txn1");

        // After rollback, poll returns nothing
        let events = cdc.poll(10);
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_exactly_once_ingestor() {
        let mut ingestor = ExactlyOnceIngestor::new();

        let txn_id = ingestor.begin();

        let key = DeduplicationKey {
            source: "kafka".to_string(),
            key: "msg-123".to_string(),
        };

        let record = IngestRecord {
            table: "events".to_string(),
            values: {
                let mut m = HashMap::new();
                m.insert("id".to_string(), DeltaValue::Int(1));
                m
            },
            dedup_key: Some(key.clone()),
        };

        ingestor.add_record(&txn_id, record).unwrap();

        let records = ingestor.commit(&txn_id).unwrap();
        assert_eq!(records.len(), 1);

        // Try to add same key again - should fail
        let txn_id2 = ingestor.begin();
        let record2 = IngestRecord {
            table: "events".to_string(),
            values: HashMap::new(),
            dedup_key: Some(key.clone()),
        };

        let result = ingestor.add_record(&txn_id2, record2);
        assert!(matches!(result, Err(IngestionError::DuplicateRecord(_))));
    }

    #[test]
    fn test_exactly_once_abort() {
        let mut ingestor = ExactlyOnceIngestor::new();

        let txn_id = ingestor.begin();

        let key = DeduplicationKey {
            source: "kafka".to_string(),
            key: "msg-456".to_string(),
        };

        let record = IngestRecord {
            table: "events".to_string(),
            values: HashMap::new(),
            dedup_key: Some(key.clone()),
        };

        ingestor.add_record(&txn_id, record).unwrap();
        ingestor.abort(&txn_id).unwrap();

        // After abort, key should be available again
        let txn_id2 = ingestor.begin();
        let record2 = IngestRecord {
            table: "events".to_string(),
            values: HashMap::new(),
            dedup_key: Some(key),
        };

        assert!(ingestor.add_record(&txn_id2, record2).is_ok());
    }

    #[test]
    fn test_exactly_once_prepare_commit() {
        let mut ingestor = ExactlyOnceIngestor::new();

        let txn_id = ingestor.begin();

        let record = IngestRecord {
            table: "events".to_string(),
            values: HashMap::new(),
            dedup_key: None,
        };

        ingestor.add_record(&txn_id, record).unwrap();

        assert_eq!(ingestor.transaction_state(&txn_id), Some(IngestionState::Started));

        ingestor.prepare(&txn_id).unwrap();
        assert_eq!(ingestor.transaction_state(&txn_id), Some(IngestionState::Prepared));

        ingestor.commit(&txn_id).unwrap();
        assert_eq!(ingestor.transaction_state(&txn_id), None); // Transaction removed
    }

    #[test]
    fn test_stream_pipeline_filter() {
        let mut pipeline = StreamPipeline::new()
            .add_stage(FilterStage::new(|m: &StreamMessage| m.timestamp > 1000));

        let messages = vec![
            StreamMessage {
                topic: "test".to_string(),
                partition: 0,
                offset: 0,
                key: None,
                payload: vec![],
                timestamp: 500,
                headers: HashMap::new(),
            },
            StreamMessage {
                topic: "test".to_string(),
                partition: 0,
                offset: 1,
                key: None,
                payload: vec![],
                timestamp: 1500,
                headers: HashMap::new(),
            },
        ];

        let result = pipeline.process(messages).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp, 1500);
    }

    #[test]
    fn test_stream_pipeline_transform() {
        let mut pipeline = StreamPipeline::new()
            .add_stage(TransformStage::new(|mut m: StreamMessage| {
                m.headers.insert("processed".to_string(), "true".to_string());
                m
            }));

        let messages = vec![
            StreamMessage {
                topic: "test".to_string(),
                partition: 0,
                offset: 0,
                key: None,
                payload: vec![],
                timestamp: 1000,
                headers: HashMap::new(),
            },
        ];

        let result = pipeline.process(messages).unwrap();
        assert_eq!(result[0].headers.get("processed"), Some(&"true".to_string()));
    }

    #[test]
    fn test_window_aggregate_tumbling() {
        let mut stage = WindowAggregateStage::tumbling(
            1000, // 1 second windows
            "user",
            "value",
            AggregateFunction::Sum,
        );

        let mut messages = Vec::new();

        // Messages in first window (0-1000)
        for i in 0..3 {
            let mut headers = HashMap::new();
            headers.insert("user".to_string(), "alice".to_string());
            headers.insert("value".to_string(), "10".to_string());
            messages.push(StreamMessage {
                topic: "events".to_string(),
                partition: 0,
                offset: i,
                key: None,
                payload: vec![],
                timestamp: 100 + i * 100,
                headers,
            });
        }

        // Message that advances watermark past first window
        let mut headers = HashMap::new();
        headers.insert("user".to_string(), "alice".to_string());
        headers.insert("value".to_string(), "10".to_string());
        messages.push(StreamMessage {
            topic: "events".to_string(),
            partition: 0,
            offset: 10,
            key: None,
            payload: vec![],
            timestamp: 3000, // Advances watermark
            headers,
        });

        let result = stage.process(messages).unwrap();

        // Should emit the closed window (0-1000)
        assert!(!result.is_empty());

        // Check that sum is correct (3 * 10 = 30)
        let sum_str = result[0].headers.get("value").unwrap();
        let sum: f64 = sum_str.parse().unwrap();
        assert_eq!(sum, 30.0);
    }

    #[test]
    fn test_aggregate_state_count_distinct() {
        let mut state = AggregateState::new(&AggregateFunction::CountDistinct);

        state.apply_insert(0.0, Some("a"));
        state.apply_insert(0.0, Some("b"));
        state.apply_insert(0.0, Some("a")); // Duplicate
        state.apply_insert(0.0, Some("c"));

        assert_eq!(state.value(), Some(3.0)); // a, b, c
    }

    #[test]
    fn test_aggregate_state_min_max() {
        let mut min_state = AggregateState::new(&AggregateFunction::Min);
        let mut max_state = AggregateState::new(&AggregateFunction::Max);

        for v in [5.0, 2.0, 8.0, 1.0, 9.0] {
            min_state.apply_insert(v, None);
            max_state.apply_insert(v, None);
        }

        assert_eq!(min_state.value(), Some(1.0));
        assert_eq!(max_state.value(), Some(9.0));
    }

    #[test]
    fn test_deduplication_cleanup() {
        let mut ingestor = ExactlyOnceIngestor::new()
            .with_dedup_ttl(100); // 100ms TTL

        let txn_id = ingestor.begin();

        let key = DeduplicationKey {
            source: "test".to_string(),
            key: "cleanup-test".to_string(),
        };

        let record = IngestRecord {
            table: "events".to_string(),
            values: HashMap::new(),
            dedup_key: Some(key.clone()),
        };

        ingestor.add_record(&txn_id, record).unwrap();
        ingestor.commit(&txn_id).unwrap();

        // Key is duplicate immediately after commit
        assert!(ingestor.is_duplicate(&key));

        // Note: cleanup() would remove expired keys, but we can't easily test
        // time-based expiration without mocking SystemTime
    }
}
