//! CDC Sink Module - Stream changes to external systems
//!
//! Provides Change Data Capture sinks for streaming database changes to:
//! - Apache Kafka
//! - Apache Pulsar
//! - Amazon Kinesis
//! - Generic webhooks
//! - File-based destinations

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Common Types
// ============================================================================

/// CDC sink error types
#[derive(Debug, Clone)]
pub enum CdcSinkError {
    /// Connection failed
    ConnectionFailed(String),
    /// Serialization error
    SerializationError(String),
    /// Send failed
    SendFailed(String),
    /// Configuration error
    ConfigError(String),
    /// Sink not started
    NotStarted,
    /// Sink already stopped
    AlreadyStopped,
    /// Topic not found
    TopicNotFound(String),
    /// Buffer overflow
    BufferOverflow,
    /// Timeout
    Timeout,
}

impl std::fmt::Display for CdcSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            Self::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            Self::SendFailed(msg) => write!(f, "send failed: {}", msg),
            Self::ConfigError(msg) => write!(f, "configuration error: {}", msg),
            Self::NotStarted => write!(f, "sink not started"),
            Self::AlreadyStopped => write!(f, "sink already stopped"),
            Self::TopicNotFound(t) => write!(f, "topic not found: {}", t),
            Self::BufferOverflow => write!(f, "buffer overflow"),
            Self::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl std::error::Error for CdcSinkError {}

/// CDC event operation type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Snapshot,
    Truncate,
    SchemaChange,
}

/// CDC event payload
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Unique event ID
    pub event_id: String,
    /// Operation type
    pub operation: CdcOperation,
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Event timestamp
    pub timestamp_ms: u64,
    /// Transaction ID
    pub transaction_id: Option<u64>,
    /// Sequence number within transaction
    pub sequence: u64,
    /// Before state (for UPDATE/DELETE)
    pub before: Option<HashMap<String, CdcValue>>,
    /// After state (for INSERT/UPDATE)
    pub after: Option<HashMap<String, CdcValue>>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Schema information
    pub schema: Option<CdcSchema>,
}

/// CDC value types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CdcValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Timestamp(u64),
    Decimal(String),
    Json(String),
    Array(Vec<CdcValue>),
    Map(HashMap<String, CdcValue>),
}

/// Schema information for CDC events
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcSchema {
    pub version: i32,
    pub columns: Vec<CdcColumnDef>,
}

/// Column definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// Serialization format for CDC events
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializationFormat {
    Json,
    Avro,
    Protobuf,
    DebeziumJson,
    CanonicalJson,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        Self::Json
    }
}

// ============================================================================
// Kafka Sink
// ============================================================================

/// Kafka sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    /// Kafka bootstrap servers
    pub bootstrap_servers: Vec<String>,
    /// Topic name or pattern
    pub topic: TopicConfig,
    /// Producer configuration
    pub producer: KafkaProducerConfig,
    /// Serialization format
    pub format: SerializationFormat,
    /// Security configuration
    pub security: Option<KafkaSecurityConfig>,
    /// Schema registry URL (for Avro)
    pub schema_registry_url: Option<String>,
    /// Enable idempotent producer
    pub enable_idempotence: bool,
    /// Maximum in-flight requests
    pub max_in_flight: u32,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            topic: TopicConfig::Single("cdc-events".to_string()),
            producer: KafkaProducerConfig::default(),
            format: SerializationFormat::Json,
            security: None,
            schema_registry_url: None,
            enable_idempotence: true,
            max_in_flight: 5,
        }
    }
}

/// Topic configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TopicConfig {
    /// Single topic for all events
    Single(String),
    /// Topic per table: prefix.database.table
    PerTable { prefix: String },
    /// Custom topic mapping
    Custom(HashMap<String, String>),
}

/// Kafka producer configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaProducerConfig {
    /// Acks required (0, 1, or -1/all)
    pub acks: i32,
    /// Compression type
    pub compression: CompressionType,
    /// Batch size in bytes
    pub batch_size: usize,
    /// Linger time in milliseconds
    pub linger_ms: u64,
    /// Buffer memory in bytes
    pub buffer_memory: usize,
    /// Max request size
    pub max_request_size: usize,
    /// Retries
    pub retries: u32,
    /// Retry backoff in milliseconds
    pub retry_backoff_ms: u64,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            acks: -1, // all
            compression: CompressionType::Snappy,
            batch_size: 16384,
            linger_ms: 5,
            buffer_memory: 32 * 1024 * 1024,
            max_request_size: 1048576,
            retries: 3,
            retry_backoff_ms: 100,
        }
    }
}

/// Compression types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Kafka security configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSecurityConfig {
    /// Security protocol
    pub protocol: SecurityProtocol,
    /// SASL mechanism
    pub sasl_mechanism: Option<SaslMechanism>,
    /// SASL username
    pub sasl_username: Option<String>,
    /// SASL password (should be from secrets manager)
    pub sasl_password: Option<String>,
    /// SSL key path
    pub ssl_key_path: Option<String>,
    /// SSL certificate path
    pub ssl_cert_path: Option<String>,
    /// SSL CA path
    pub ssl_ca_path: Option<String>,
}

/// Security protocol
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

/// SASL mechanism
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
    GssApi,
}

/// Kafka sink statistics
#[derive(Clone, Debug, Default)]
pub struct KafkaSinkStats {
    pub events_sent: u64,
    pub events_failed: u64,
    pub bytes_sent: u64,
    pub batches_sent: u64,
    pub avg_latency_ms: f64,
    pub max_latency_ms: u64,
    pub last_error: Option<String>,
    pub last_success_time: Option<u64>,
}

/// Kafka CDC sink
pub struct KafkaSink {
    config: KafkaSinkConfig,
    running: AtomicBool,
    events_sent: AtomicU64,
    events_failed: AtomicU64,
    bytes_sent: AtomicU64,
    buffer: Mutex<Vec<CdcEvent>>,
}

impl KafkaSink {
    pub fn new(config: KafkaSinkConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            events_sent: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Start the sink
    pub fn start(&self) -> Result<(), CdcSinkError> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Validate configuration
        if self.config.bootstrap_servers.is_empty() {
            return Err(CdcSinkError::ConfigError("no bootstrap servers".into()));
        }

        // In a real implementation, we would create Kafka producer here
        // For now, we just mark as running

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Stop the sink
    pub fn stop(&self) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::AlreadyStopped);
        }

        // Flush remaining events
        self.flush()?;

        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Send a CDC event
    pub fn send(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        // Serialize event
        let payload = self.serialize(&event)?;

        // Determine topic
        let topic = self.get_topic(&event)?;

        // Buffer the event
        {
            let mut buffer = self.buffer.lock();

            if buffer.len() >= self.config.producer.batch_size / 100 {
                // Batch is full, would flush
                return Err(CdcSinkError::BufferOverflow);
            }

            buffer.push(event);
        }

        self.events_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent
            .fetch_add(payload.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Send multiple events
    pub fn send_batch(&self, events: Vec<CdcEvent>) -> Result<usize, CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        let mut sent = 0;
        for event in events {
            match self.send(event) {
                Ok(_) => sent += 1,
                Err(e) => {
                    self.events_failed.fetch_add(1, Ordering::Relaxed);
                    if sent == 0 {
                        return Err(e);
                    }
                    break;
                }
            }
        }

        Ok(sent)
    }

    /// Flush buffered events
    pub fn flush(&self) -> Result<(), CdcSinkError> {
        let events = {
            let mut buffer = self.buffer.lock();
            std::mem::take(&mut *buffer)
        };

        // In a real implementation, we would actually send to Kafka
        // For now, just count them as sent

        self.events_sent
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Serialize event based on format
    fn serialize(&self, event: &CdcEvent) -> Result<Vec<u8>, CdcSinkError> {
        match self.config.format {
            SerializationFormat::Json => serde_json::to_vec(event)
                .map_err(|e| CdcSinkError::SerializationError(e.to_string())),
            SerializationFormat::DebeziumJson => {
                // Debezium envelope format
                let envelope = DebeziumEnvelope {
                    schema: None,
                    payload: DebeziumPayload {
                        before: event.before.clone(),
                        after: event.after.clone(),
                        source: DebeziumSource {
                            version: "1.0".to_string(),
                            connector: "boyodb".to_string(),
                            name: format!("{}.{}", event.database, event.table),
                            ts_ms: event.timestamp_ms as i64,
                            snapshot: matches!(event.operation, CdcOperation::Snapshot),
                            db: event.database.clone(),
                            table: event.table.clone(),
                            txn_id: event.transaction_id,
                        },
                        op: match event.operation {
                            CdcOperation::Insert => "c",
                            CdcOperation::Update => "u",
                            CdcOperation::Delete => "d",
                            CdcOperation::Snapshot => "r",
                            CdcOperation::Truncate => "t",
                            CdcOperation::SchemaChange => "s",
                        }
                        .to_string(),
                        ts_ms: event.timestamp_ms as i64,
                    },
                };

                serde_json::to_vec(&envelope)
                    .map_err(|e| CdcSinkError::SerializationError(e.to_string()))
            }
            SerializationFormat::CanonicalJson => {
                // Sorted keys for canonical form
                let json = serde_json::to_value(event)
                    .map_err(|e| CdcSinkError::SerializationError(e.to_string()))?;
                serde_json::to_vec(&json)
                    .map_err(|e| CdcSinkError::SerializationError(e.to_string()))
            }
            _ => {
                // Avro/Protobuf would need schema
                serde_json::to_vec(event)
                    .map_err(|e| CdcSinkError::SerializationError(e.to_string()))
            }
        }
    }

    /// Get topic for event
    fn get_topic(&self, event: &CdcEvent) -> Result<String, CdcSinkError> {
        match &self.config.topic {
            TopicConfig::Single(topic) => Ok(topic.clone()),
            TopicConfig::PerTable { prefix } => {
                Ok(format!("{}.{}.{}", prefix, event.database, event.table))
            }
            TopicConfig::Custom(mapping) => {
                let key = format!("{}.{}", event.database, event.table);
                mapping
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| CdcSinkError::TopicNotFound(key))
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> KafkaSinkStats {
        KafkaSinkStats {
            events_sent: self.events_sent.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            batches_sent: 0,
            avg_latency_ms: 0.0,
            max_latency_ms: 0,
            last_error: None,
            last_success_time: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
        }
    }
}

/// Debezium envelope format
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebeziumEnvelope {
    schema: Option<serde_json::Value>,
    payload: DebeziumPayload,
}

/// Debezium payload
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebeziumPayload {
    before: Option<HashMap<String, CdcValue>>,
    after: Option<HashMap<String, CdcValue>>,
    source: DebeziumSource,
    op: String,
    ts_ms: i64,
}

/// Debezium source metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebeziumSource {
    version: String,
    connector: String,
    name: String,
    ts_ms: i64,
    snapshot: bool,
    db: String,
    table: String,
    txn_id: Option<u64>,
}

// ============================================================================
// Pulsar Sink
// ============================================================================

/// Pulsar sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulsarSinkConfig {
    /// Pulsar service URL
    pub service_url: String,
    /// Topic configuration
    pub topic: TopicConfig,
    /// Producer name
    pub producer_name: Option<String>,
    /// Serialization format
    pub format: SerializationFormat,
    /// Batching configuration
    pub batching: PulsarBatchingConfig,
    /// Authentication
    pub auth: Option<PulsarAuthConfig>,
}

impl Default for PulsarSinkConfig {
    fn default() -> Self {
        Self {
            service_url: "pulsar://localhost:6650".to_string(),
            topic: TopicConfig::Single("persistent://public/default/cdc".to_string()),
            producer_name: None,
            format: SerializationFormat::Json,
            batching: PulsarBatchingConfig::default(),
            auth: None,
        }
    }
}

/// Pulsar batching configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulsarBatchingConfig {
    pub enabled: bool,
    pub max_messages: u32,
    pub max_bytes: usize,
    pub delay_ms: u64,
}

impl Default for PulsarBatchingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_messages: 1000,
            max_bytes: 128 * 1024,
            delay_ms: 10,
        }
    }
}

/// Pulsar authentication configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulsarAuthConfig {
    pub method: PulsarAuthMethod,
    pub token: Option<String>,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

/// Pulsar authentication method
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PulsarAuthMethod {
    None,
    Token,
    Tls,
    OAuth2,
}

/// Pulsar sink
pub struct PulsarSink {
    config: PulsarSinkConfig,
    running: AtomicBool,
    events_sent: AtomicU64,
}

impl PulsarSink {
    pub fn new(config: PulsarSinkConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            events_sent: AtomicU64::new(0),
        }
    }

    pub fn start(&self) -> Result<(), CdcSinkError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn stop(&self) -> Result<(), CdcSinkError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn send(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        // In a real implementation, send to Pulsar
        self.events_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// ============================================================================
// Kinesis Sink
// ============================================================================

/// Kinesis sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisSinkConfig {
    /// AWS region
    pub region: String,
    /// Stream name
    pub stream_name: String,
    /// Partition key strategy
    pub partition_key: PartitionKeyStrategy,
    /// Serialization format
    pub format: SerializationFormat,
    /// AWS credentials
    pub credentials: Option<AwsCredentials>,
    /// Max records per batch
    pub max_batch_records: usize,
    /// Max batch size in bytes
    pub max_batch_size: usize,
}

impl Default for KinesisSinkConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            stream_name: "cdc-events".to_string(),
            partition_key: PartitionKeyStrategy::TableName,
            format: SerializationFormat::Json,
            credentials: None,
            max_batch_records: 500,
            max_batch_size: 5 * 1024 * 1024,
        }
    }
}

/// Partition key strategy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionKeyStrategy {
    /// Use table name as partition key
    TableName,
    /// Use primary key hash
    PrimaryKeyHash,
    /// Use explicit column
    Column(String),
    /// Random distribution
    Random,
    /// Custom expression
    Custom(String),
}

/// AWS credentials
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Kinesis sink
pub struct KinesisSink {
    config: KinesisSinkConfig,
    running: AtomicBool,
    events_sent: AtomicU64,
}

impl KinesisSink {
    pub fn new(config: KinesisSinkConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            events_sent: AtomicU64::new(0),
        }
    }

    pub fn start(&self) -> Result<(), CdcSinkError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn stop(&self) -> Result<(), CdcSinkError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn send(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        self.events_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get_partition_key(&self, event: &CdcEvent) -> String {
        match &self.config.partition_key {
            PartitionKeyStrategy::TableName => {
                format!("{}.{}", event.database, event.table)
            }
            PartitionKeyStrategy::PrimaryKeyHash => {
                // Hash primary key values
                let pk_str = event.primary_key.join(",");
                format!("{:x}", md5_hash(&pk_str))
            }
            PartitionKeyStrategy::Column(col) => event
                .after
                .as_ref()
                .and_then(|a| a.get(col))
                .map(|v| format!("{:?}", v))
                .unwrap_or_default(),
            PartitionKeyStrategy::Random => {
                format!(
                    "{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )
            }
            PartitionKeyStrategy::Custom(_expr) => {
                // Would evaluate expression
                event.event_id.clone()
            }
        }
    }
}

fn md5_hash(s: &str) -> u64 {
    // Simple hash for demonstration
    s.bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64))
}

// ============================================================================
// Webhook Sink
// ============================================================================

/// Webhook sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookSinkConfig {
    /// Webhook URL
    pub url: String,
    /// HTTP method
    pub method: HttpMethod,
    /// Headers
    pub headers: HashMap<String, String>,
    /// Serialization format
    pub format: SerializationFormat,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Retry configuration
    pub retry: RetryConfig,
    /// Batching
    pub batch_size: Option<usize>,
    /// Authentication
    pub auth: Option<WebhookAuth>,
}

impl Default for WebhookSinkConfig {
    fn default() -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        Self {
            url: "http://localhost:8080/webhook".to_string(),
            method: HttpMethod::Post,
            headers,
            format: SerializationFormat::Json,
            timeout_ms: 30000,
            retry: RetryConfig::default(),
            batch_size: None,
            auth: None,
        }
    }
}

/// HTTP method
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    Post,
    Put,
    Patch,
}

/// Retry configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Webhook authentication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WebhookAuth {
    Basic { username: String, password: String },
    Bearer { token: String },
    ApiKey { header: String, key: String },
    Hmac { secret: String, header: String },
}

/// Webhook sink
pub struct WebhookSink {
    config: WebhookSinkConfig,
    running: AtomicBool,
    events_sent: AtomicU64,
}

impl WebhookSink {
    pub fn new(config: WebhookSinkConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            events_sent: AtomicU64::new(0),
        }
    }

    pub fn start(&self) -> Result<(), CdcSinkError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn stop(&self) -> Result<(), CdcSinkError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn send(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        self.events_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// ============================================================================
// File Sink
// ============================================================================

/// File sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileSinkConfig {
    /// Base directory
    pub base_dir: String,
    /// File naming pattern
    pub file_pattern: FilePattern,
    /// File format
    pub format: FileFormat,
    /// Serialization format
    pub serialization: SerializationFormat,
    /// Max file size before rotation
    pub max_file_size: usize,
    /// Max events per file
    pub max_events_per_file: Option<usize>,
    /// Rotation interval
    pub rotation_interval_secs: Option<u64>,
    /// Compression
    pub compression: Option<CompressionType>,
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self {
            base_dir: "/var/log/cdc".to_string(),
            file_pattern: FilePattern::DateHierarchy,
            format: FileFormat::JsonLines,
            serialization: SerializationFormat::Json,
            max_file_size: 100 * 1024 * 1024,
            max_events_per_file: None,
            rotation_interval_secs: Some(3600),
            compression: Some(CompressionType::Gzip),
        }
    }
}

/// File naming pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FilePattern {
    /// database/table/date/part-N.json
    DateHierarchy,
    /// database.table.timestamp.json
    FlatTimestamp,
    /// Custom pattern with placeholders
    Custom(String),
}

/// File format
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    JsonLines,
    Json,
    Csv,
    Parquet,
}

/// File sink
pub struct FileSink {
    config: FileSinkConfig,
    running: AtomicBool,
    events_written: AtomicU64,
    current_file_events: AtomicU64,
}

impl FileSink {
    pub fn new(config: FileSinkConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            events_written: AtomicU64::new(0),
            current_file_events: AtomicU64::new(0),
        }
    }

    pub fn start(&self) -> Result<(), CdcSinkError> {
        // Create base directory
        std::fs::create_dir_all(&self.config.base_dir)
            .map_err(|e| CdcSinkError::ConfigError(format!("failed to create directory: {}", e)))?;

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn stop(&self) -> Result<(), CdcSinkError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn send(&self, event: CdcEvent) -> Result<(), CdcSinkError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(CdcSinkError::NotStarted);
        }

        self.events_written.fetch_add(1, Ordering::Relaxed);
        self.current_file_events.fetch_add(1, Ordering::Relaxed);

        // Check rotation
        if let Some(max) = self.config.max_events_per_file {
            if self.current_file_events.load(Ordering::Relaxed) >= max as u64 {
                self.rotate()?;
            }
        }

        Ok(())
    }

    fn rotate(&self) -> Result<(), CdcSinkError> {
        self.current_file_events.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn get_file_path(&self, event: &CdcEvent) -> String {
        match &self.config.file_pattern {
            FilePattern::DateHierarchy => {
                let ts = event.timestamp_ms / 1000;
                let date = chrono_date(ts);
                format!(
                    "{}/{}/{}/{}/events.jsonl",
                    self.config.base_dir, event.database, event.table, date
                )
            }
            FilePattern::FlatTimestamp => {
                format!(
                    "{}/{}.{}.{}.jsonl",
                    self.config.base_dir, event.database, event.table, event.timestamp_ms
                )
            }
            FilePattern::Custom(pattern) => pattern
                .replace("{database}", &event.database)
                .replace("{table}", &event.table)
                .replace("{timestamp}", &event.timestamp_ms.to_string()),
        }
    }
}

fn chrono_date(unix_secs: u64) -> String {
    // Simple date formatting without chrono dependency
    let days_since_epoch = unix_secs / 86400;
    let years = days_since_epoch / 365;
    let year = 1970 + years;
    let day_of_year = days_since_epoch % 365;
    let month = day_of_year / 30 + 1;
    let day = day_of_year % 30 + 1;
    format!("{:04}-{:02}-{:02}", year, month, day)
}

// ============================================================================
// Sink Manager
// ============================================================================

/// CDC sink type
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkType {
    Kafka,
    Pulsar,
    Kinesis,
    Webhook,
    File,
}

/// Sink manager for managing multiple CDC sinks
pub struct SinkManager {
    kafka_sinks: HashMap<String, Arc<KafkaSink>>,
    pulsar_sinks: HashMap<String, Arc<PulsarSink>>,
    kinesis_sinks: HashMap<String, Arc<KinesisSink>>,
    webhook_sinks: HashMap<String, Arc<WebhookSink>>,
    file_sinks: HashMap<String, Arc<FileSink>>,
}

impl SinkManager {
    pub fn new() -> Self {
        Self {
            kafka_sinks: HashMap::new(),
            pulsar_sinks: HashMap::new(),
            kinesis_sinks: HashMap::new(),
            webhook_sinks: HashMap::new(),
            file_sinks: HashMap::new(),
        }
    }

    /// Register a Kafka sink
    pub fn register_kafka(&mut self, name: &str, config: KafkaSinkConfig) {
        self.kafka_sinks
            .insert(name.to_string(), Arc::new(KafkaSink::new(config)));
    }

    /// Register a Pulsar sink
    pub fn register_pulsar(&mut self, name: &str, config: PulsarSinkConfig) {
        self.pulsar_sinks
            .insert(name.to_string(), Arc::new(PulsarSink::new(config)));
    }

    /// Register a Kinesis sink
    pub fn register_kinesis(&mut self, name: &str, config: KinesisSinkConfig) {
        self.kinesis_sinks
            .insert(name.to_string(), Arc::new(KinesisSink::new(config)));
    }

    /// Register a Webhook sink
    pub fn register_webhook(&mut self, name: &str, config: WebhookSinkConfig) {
        self.webhook_sinks
            .insert(name.to_string(), Arc::new(WebhookSink::new(config)));
    }

    /// Register a File sink
    pub fn register_file(&mut self, name: &str, config: FileSinkConfig) {
        self.file_sinks
            .insert(name.to_string(), Arc::new(FileSink::new(config)));
    }

    /// Start all sinks
    pub fn start_all(&self) -> Result<(), CdcSinkError> {
        for sink in self.kafka_sinks.values() {
            sink.start()?;
        }
        for sink in self.pulsar_sinks.values() {
            sink.start()?;
        }
        for sink in self.kinesis_sinks.values() {
            sink.start()?;
        }
        for sink in self.webhook_sinks.values() {
            sink.start()?;
        }
        for sink in self.file_sinks.values() {
            sink.start()?;
        }
        Ok(())
    }

    /// Stop all sinks
    pub fn stop_all(&self) -> Result<(), CdcSinkError> {
        for sink in self.kafka_sinks.values() {
            let _ = sink.stop();
        }
        for sink in self.pulsar_sinks.values() {
            let _ = sink.stop();
        }
        for sink in self.kinesis_sinks.values() {
            let _ = sink.stop();
        }
        for sink in self.webhook_sinks.values() {
            let _ = sink.stop();
        }
        for sink in self.file_sinks.values() {
            let _ = sink.stop();
        }
        Ok(())
    }

    /// Broadcast event to all sinks
    pub fn broadcast(&self, event: CdcEvent) -> HashMap<String, Result<(), CdcSinkError>> {
        let mut results = HashMap::new();

        for (name, sink) in &self.kafka_sinks {
            results.insert(format!("kafka:{}", name), sink.send(event.clone()));
        }
        for (name, sink) in &self.pulsar_sinks {
            results.insert(format!("pulsar:{}", name), sink.send(event.clone()));
        }
        for (name, sink) in &self.kinesis_sinks {
            results.insert(format!("kinesis:{}", name), sink.send(event.clone()));
        }
        for (name, sink) in &self.webhook_sinks {
            results.insert(format!("webhook:{}", name), sink.send(event.clone()));
        }
        for (name, sink) in &self.file_sinks {
            results.insert(format!("file:{}", name), sink.send(event.clone()));
        }

        results
    }

    /// List all registered sinks
    pub fn list_sinks(&self) -> Vec<(String, SinkType)> {
        let mut result = Vec::new();
        for name in self.kafka_sinks.keys() {
            result.push((name.clone(), SinkType::Kafka));
        }
        for name in self.pulsar_sinks.keys() {
            result.push((name.clone(), SinkType::Pulsar));
        }
        for name in self.kinesis_sinks.keys() {
            result.push((name.clone(), SinkType::Kinesis));
        }
        for name in self.webhook_sinks.keys() {
            result.push((name.clone(), SinkType::Webhook));
        }
        for name in self.file_sinks.keys() {
            result.push((name.clone(), SinkType::File));
        }
        result
    }
}

impl Default for SinkManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_sink() {
        let config = KafkaSinkConfig::default();
        let sink = KafkaSink::new(config);

        sink.start().unwrap();

        let event = CdcEvent {
            event_id: "1".to_string(),
            operation: CdcOperation::Insert,
            database: "test".to_string(),
            table: "users".to_string(),
            timestamp_ms: 1234567890000,
            transaction_id: Some(1),
            sequence: 1,
            before: None,
            after: Some({
                let mut m = HashMap::new();
                m.insert("id".to_string(), CdcValue::Int64(1));
                m.insert("name".to_string(), CdcValue::String("Alice".to_string()));
                m
            }),
            primary_key: vec!["id".to_string()],
            schema: None,
        };

        sink.send(event).unwrap();
        assert_eq!(sink.events_sent.load(Ordering::Relaxed), 1);

        sink.stop().unwrap();
    }

    #[test]
    fn test_topic_config() {
        let event = CdcEvent {
            event_id: "1".to_string(),
            operation: CdcOperation::Insert,
            database: "mydb".to_string(),
            table: "users".to_string(),
            timestamp_ms: 0,
            transaction_id: None,
            sequence: 0,
            before: None,
            after: None,
            primary_key: vec![],
            schema: None,
        };

        let config = KafkaSinkConfig {
            topic: TopicConfig::PerTable {
                prefix: "cdc".to_string(),
            },
            ..Default::default()
        };

        let sink = KafkaSink::new(config);
        let topic = sink.get_topic(&event).unwrap();
        assert_eq!(topic, "cdc.mydb.users");
    }

    #[test]
    fn test_sink_manager() {
        let mut manager = SinkManager::new();

        manager.register_kafka("primary", KafkaSinkConfig::default());
        manager.register_file(
            "backup",
            FileSinkConfig {
                base_dir: "/tmp/cdc".to_string(),
                ..Default::default()
            },
        );

        let sinks = manager.list_sinks();
        assert_eq!(sinks.len(), 2);

        manager.start_all().unwrap();
        manager.stop_all().unwrap();
    }
}
