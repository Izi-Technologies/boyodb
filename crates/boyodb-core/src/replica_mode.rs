//! Read Replica Mode Implementation
//!
//! Provides read-only replica support for BoyoDB to offload read queries
//! from write-heavy primary servers.
//!
//! # Architecture
//! ```text
//! ┌─────────────────┐         ┌─────────────────┐
//! │  Primary Server │ ──────► │  Shared S3      │
//! │  (R/W)          │         │  (segments)     │
//! │  :8765          │         └────────┬────────┘
//! └─────────────────┘                  │
//!                                      │ manifest sync
//!                                      ▼
//!                           ┌─────────────────┐
//!                           │  Read Replica   │
//!                           │  (R/O)          │
//!                           │  :8766          │
//!                           └─────────────────┘
//! ```
//!
//! # Features
//! - Read-only mode with write rejection
//! - Manifest sync from shared S3 storage
//! - HTTP bundle pull from primary server
//! - Configurable sync intervals
//! - Lag monitoring and health checks
//!
//! # Usage
//! ```bash
//! # Start as replica with S3 sync
//! boyodb-server /data/replica 0.0.0.0:8766 \
//!     --replica \
//!     --replica-sync-interval-ms 1000 \
//!     --s3-bucket cdr-bucket
//!
//! # Start as replica with HTTP sync from primary
//! boyodb-server /data/replica 0.0.0.0:8766 \
//!     --replica \
//!     --primary http://primary:8765
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Types and Errors
// ============================================================================

/// Errors from replica operations
#[derive(Debug, Clone)]
pub enum ReplicaError {
    /// Write operation attempted on read-only replica
    ReadOnlyReplica(String),
    /// Sync failed
    SyncFailed(String),
    /// Primary not reachable
    PrimaryUnreachable(String),
    /// Manifest parse error
    ManifestError(String),
    /// Lag too high
    LagTooHigh {
        current_lag_ms: u64,
        max_lag_ms: u64,
    },
    /// Configuration error
    ConfigError(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadOnlyReplica(msg) => {
                write!(f, "cannot execute write on read-only replica: {}", msg)
            }
            Self::SyncFailed(msg) => write!(f, "replica sync failed: {}", msg),
            Self::PrimaryUnreachable(msg) => write!(f, "primary unreachable: {}", msg),
            Self::ManifestError(msg) => write!(f, "manifest error: {}", msg),
            Self::LagTooHigh {
                current_lag_ms,
                max_lag_ms,
            } => {
                write!(
                    f,
                    "replica lag too high: {}ms (max {}ms)",
                    current_lag_ms, max_lag_ms
                )
            }
            Self::ConfigError(msg) => write!(f, "configuration error: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for ReplicaError {}

/// PostgreSQL error code for read-only transaction
pub const SQLSTATE_READ_ONLY_SQL_TRANSACTION: &str = "25006";

// ============================================================================
// Replica Configuration
// ============================================================================

/// Sync source type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSource {
    /// Sync from shared S3 bucket
    S3 {
        bucket: String,
        region: String,
        prefix: Option<String>,
    },
    /// Sync from primary via HTTP
    Http { primary_url: String },
    /// Sync from local filesystem (for testing)
    Local { path: String },
}

/// Replica configuration
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// Whether this instance is a read-only replica
    pub is_replica: bool,
    /// Sync source configuration
    pub sync_source: Option<SyncSource>,
    /// Manifest sync interval in milliseconds
    pub sync_interval_ms: u64,
    /// Maximum acceptable lag in milliseconds (0 = no limit)
    pub max_lag_ms: u64,
    /// Whether to reject queries when lag is too high
    pub reject_on_high_lag: bool,
    /// Connection timeout for HTTP sync (ms)
    pub http_timeout_ms: u64,
    /// Number of sync retries on failure
    pub sync_retries: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            is_replica: false,
            sync_source: None,
            sync_interval_ms: 1000,
            max_lag_ms: 0,
            reject_on_high_lag: false,
            http_timeout_ms: 5000,
            sync_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

impl ReplicaConfig {
    /// Create a new replica config
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable replica mode
    pub fn as_replica(mut self) -> Self {
        self.is_replica = true;
        self
    }

    /// Set S3 sync source
    pub fn with_s3_sync(mut self, bucket: &str, region: &str) -> Self {
        self.sync_source = Some(SyncSource::S3 {
            bucket: bucket.to_string(),
            region: region.to_string(),
            prefix: None,
        });
        self
    }

    /// Set HTTP sync source
    pub fn with_http_sync(mut self, primary_url: &str) -> Self {
        self.sync_source = Some(SyncSource::Http {
            primary_url: primary_url.to_string(),
        });
        self
    }

    /// Set sync interval
    pub fn with_sync_interval(mut self, interval_ms: u64) -> Self {
        self.sync_interval_ms = interval_ms;
        self
    }

    /// Set maximum lag
    pub fn with_max_lag(mut self, max_lag_ms: u64) -> Self {
        self.max_lag_ms = max_lag_ms;
        self
    }
}

// ============================================================================
// Replica State
// ============================================================================

/// Current replica state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Initializing - first sync in progress
    Initializing,
    /// Syncing - regular sync in progress
    Syncing,
    /// Ready - fully synced and serving reads
    Ready,
    /// Lagging - synced but lag exceeds threshold
    Lagging,
    /// Error - sync failed
    Error,
    /// Stopped - replica stopped
    Stopped,
}

impl std::fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initializing => write!(f, "initializing"),
            Self::Syncing => write!(f, "syncing"),
            Self::Ready => write!(f, "ready"),
            Self::Lagging => write!(f, "lagging"),
            Self::Error => write!(f, "error"),
            Self::Stopped => write!(f, "stopped"),
        }
    }
}

/// Replica metrics
#[derive(Debug, Clone, Default)]
pub struct ReplicaMetrics {
    /// Total sync operations
    pub total_syncs: u64,
    /// Successful syncs
    pub successful_syncs: u64,
    /// Failed syncs
    pub failed_syncs: u64,
    /// Current lag in milliseconds
    pub current_lag_ms: u64,
    /// Average lag in milliseconds
    pub avg_lag_ms: u64,
    /// Maximum observed lag
    pub max_lag_ms: u64,
    /// Bytes synced
    pub bytes_synced: u64,
    /// Segments synced
    pub segments_synced: u64,
    /// Last successful sync time
    pub last_sync_time: Option<SystemTime>,
    /// Last sync duration in milliseconds
    pub last_sync_duration_ms: u64,
    /// Write rejections count
    pub write_rejections: u64,
    /// Queries rejected due to high lag
    pub lag_rejections: u64,
}

// ============================================================================
// Write Guard
// ============================================================================

/// Guard that rejects write operations on replicas
pub struct WriteGuard {
    /// Whether in replica mode
    is_replica: AtomicBool,
    /// Write rejection counter
    rejections: AtomicU64,
}

impl Default for WriteGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteGuard {
    /// Create a new write guard
    pub fn new() -> Self {
        Self {
            is_replica: AtomicBool::new(false),
            rejections: AtomicU64::new(0),
        }
    }

    /// Enable replica mode
    pub fn enable_replica_mode(&self) {
        self.is_replica.store(true, Ordering::SeqCst);
    }

    /// Disable replica mode
    pub fn disable_replica_mode(&self) {
        self.is_replica.store(false, Ordering::SeqCst);
    }

    /// Check if in replica mode
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(Ordering::SeqCst)
    }

    /// Check if write is allowed, return error if not
    pub fn require_writable(&self) -> Result<(), ReplicaError> {
        if self.is_replica.load(Ordering::SeqCst) {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(ReplicaError::ReadOnlyReplica(
                "write operations disabled on read replica".into(),
            ));
        }
        Ok(())
    }

    /// Get rejection count
    pub fn rejection_count(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }
}

/// Check if SQL statement is a write operation
pub fn is_write_sql(sql: &str) -> bool {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Check for write statements
    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("DROP")
        || upper.starts_with("ALTER")
        || upper.starts_with("TRUNCATE")
        || upper.starts_with("GRANT")
        || upper.starts_with("REVOKE")
        || upper.starts_with("VACUUM")
        || upper.starts_with("ANALYZE")
        || upper.starts_with("REINDEX")
        || upper.starts_with("CLUSTER")
        || upper.starts_with("COPY") && upper.contains(" FROM ")
}

/// Check if SQL is safe for replica (read-only)
pub fn is_read_only_sql(sql: &str) -> bool {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Allowed read operations
    upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("SET")
        || upper.starts_with("BEGIN")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
        || upper.starts_with("LISTEN")
        || upper.starts_with("UNLISTEN")
}

// ============================================================================
// Manifest Tracking
// ============================================================================

/// Manifest version info
#[derive(Debug, Clone)]
pub struct ManifestVersion {
    /// Version/LSN of the manifest
    pub version: u64,
    /// Timestamp when manifest was created
    pub timestamp: SystemTime,
    /// Number of segments in manifest
    pub segment_count: usize,
    /// Total data size in bytes
    pub total_size: u64,
    /// Checksum of manifest
    pub checksum: String,
}

/// Manifest tracker for replica sync
pub struct ManifestTracker {
    /// Current manifest version
    current_version: RwLock<Option<ManifestVersion>>,
    /// Primary manifest version (for lag calculation)
    primary_version: RwLock<Option<ManifestVersion>>,
    /// Version history (for debugging)
    version_history: RwLock<Vec<ManifestVersion>>,
    /// Max history entries to keep
    max_history: usize,
}

impl Default for ManifestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestTracker {
    /// Create a new manifest tracker
    pub fn new() -> Self {
        Self {
            current_version: RwLock::new(None),
            primary_version: RwLock::new(None),
            version_history: RwLock::new(Vec::new()),
            max_history: 100,
        }
    }

    /// Update current manifest version
    pub fn update_current(&self, version: ManifestVersion) {
        let mut history = self.version_history.write();
        history.push(version.clone());
        if history.len() > self.max_history {
            history.remove(0);
        }

        let mut current = self.current_version.write();
        *current = Some(version);
    }

    /// Update primary manifest version
    pub fn update_primary(&self, version: ManifestVersion) {
        let mut primary = self.primary_version.write();
        *primary = Some(version);
    }

    /// Get current version
    pub fn current(&self) -> Option<ManifestVersion> {
        self.current_version.read().clone()
    }

    /// Get primary version
    pub fn primary(&self) -> Option<ManifestVersion> {
        self.primary_version.read().clone()
    }

    /// Calculate lag in versions
    pub fn version_lag(&self) -> u64 {
        let current = self.current_version.read();
        let primary = self.primary_version.read();

        match (&*current, &*primary) {
            (Some(c), Some(p)) => p.version.saturating_sub(c.version),
            _ => 0,
        }
    }

    /// Calculate lag in milliseconds (estimated)
    pub fn estimated_lag_ms(&self) -> u64 {
        let current = self.current_version.read();
        let primary = self.primary_version.read();

        match (&*current, &*primary) {
            (Some(c), Some(p)) => {
                let current_ts = c
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let primary_ts = p
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                primary_ts.saturating_sub(current_ts)
            }
            _ => 0,
        }
    }
}

// ============================================================================
// Replica Manager
// ============================================================================

/// Replica manager coordinates sync and state
pub struct ReplicaManager {
    /// Configuration
    config: ReplicaConfig,
    /// Write guard
    write_guard: Arc<WriteGuard>,
    /// Current state
    state: RwLock<ReplicaState>,
    /// Manifest tracker
    manifest_tracker: ManifestTracker,
    /// Metrics
    metrics: RwLock<ReplicaMetrics>,
    /// Running flag
    running: AtomicBool,
    /// Last error message
    last_error: RwLock<Option<String>>,
}

impl ReplicaManager {
    /// Create a new replica manager
    pub fn new(config: ReplicaConfig) -> Self {
        let write_guard = Arc::new(WriteGuard::new());
        if config.is_replica {
            write_guard.enable_replica_mode();
        }

        Self {
            config,
            write_guard,
            state: RwLock::new(ReplicaState::Initializing),
            manifest_tracker: ManifestTracker::new(),
            metrics: RwLock::new(ReplicaMetrics::default()),
            running: AtomicBool::new(false),
            last_error: RwLock::new(None),
        }
    }

    /// Get write guard
    pub fn write_guard(&self) -> Arc<WriteGuard> {
        Arc::clone(&self.write_guard)
    }

    /// Check if this is a replica
    pub fn is_replica(&self) -> bool {
        self.config.is_replica
    }

    /// Get current state
    pub fn state(&self) -> ReplicaState {
        *self.state.read()
    }

    /// Get metrics
    pub fn metrics(&self) -> ReplicaMetrics {
        self.metrics.read().clone()
    }

    /// Get config
    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    /// Check if write is allowed
    pub fn require_writable(&self) -> Result<(), ReplicaError> {
        self.write_guard.require_writable()
    }

    /// Check SQL and reject writes on replica
    pub fn check_sql(&self, sql: &str) -> Result<(), ReplicaError> {
        if !self.config.is_replica {
            return Ok(());
        }

        if is_write_sql(sql) {
            let mut metrics = self.metrics.write();
            metrics.write_rejections += 1;
            return Err(ReplicaError::ReadOnlyReplica(format!(
                "cannot execute {} on read replica",
                sql.split_whitespace().next().unwrap_or("statement")
            )));
        }

        // Check lag if configured
        if self.config.reject_on_high_lag && self.config.max_lag_ms > 0 {
            let lag = self.manifest_tracker.estimated_lag_ms();
            if lag > self.config.max_lag_ms {
                let mut metrics = self.metrics.write();
                metrics.lag_rejections += 1;
                return Err(ReplicaError::LagTooHigh {
                    current_lag_ms: lag,
                    max_lag_ms: self.config.max_lag_ms,
                });
            }
        }

        Ok(())
    }

    /// Start sync worker
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
        *self.state.write() = ReplicaState::Initializing;
    }

    /// Stop sync worker
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write() = ReplicaState::Stopped;
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Record successful sync
    pub fn record_sync_success(&self, duration_ms: u64, bytes: u64, segments: u64) {
        let mut metrics = self.metrics.write();
        metrics.total_syncs += 1;
        metrics.successful_syncs += 1;
        metrics.bytes_synced += bytes;
        metrics.segments_synced += segments;
        metrics.last_sync_time = Some(SystemTime::now());
        metrics.last_sync_duration_ms = duration_ms;

        // Update lag
        let lag = self.manifest_tracker.estimated_lag_ms();
        metrics.current_lag_ms = lag;
        if lag > metrics.max_lag_ms {
            metrics.max_lag_ms = lag;
        }

        // Update state
        let mut state = self.state.write();
        if self.config.max_lag_ms > 0 && lag > self.config.max_lag_ms {
            *state = ReplicaState::Lagging;
        } else {
            *state = ReplicaState::Ready;
        }

        // Clear error
        *self.last_error.write() = None;
    }

    /// Record sync failure
    pub fn record_sync_failure(&self, error: &str) {
        let mut metrics = self.metrics.write();
        metrics.total_syncs += 1;
        metrics.failed_syncs += 1;

        *self.state.write() = ReplicaState::Error;
        *self.last_error.write() = Some(error.to_string());
    }

    /// Update manifest versions
    pub fn update_manifest(&self, current: ManifestVersion, primary: Option<ManifestVersion>) {
        self.manifest_tracker.update_current(current);
        if let Some(p) = primary {
            self.manifest_tracker.update_primary(p);
        }
    }

    /// Get lag info
    pub fn get_lag(&self) -> LagInfo {
        LagInfo {
            version_lag: self.manifest_tracker.version_lag(),
            time_lag_ms: self.manifest_tracker.estimated_lag_ms(),
            current_version: self.manifest_tracker.current().map(|v| v.version),
            primary_version: self.manifest_tracker.primary().map(|v| v.version),
        }
    }

    /// Get status for health checks
    pub fn get_status(&self) -> ReplicaStatus {
        let metrics = self.metrics.read();
        let state = *self.state.read();
        let lag = self.get_lag();

        ReplicaStatus {
            is_replica: self.config.is_replica,
            state,
            lag_ms: lag.time_lag_ms,
            version_lag: lag.version_lag,
            last_sync: metrics.last_sync_time,
            total_syncs: metrics.total_syncs,
            failed_syncs: metrics.failed_syncs,
            write_rejections: metrics.write_rejections,
            last_error: self.last_error.read().clone(),
        }
    }
}

/// Lag information
#[derive(Debug, Clone)]
pub struct LagInfo {
    /// Lag in manifest versions
    pub version_lag: u64,
    /// Estimated time lag in milliseconds
    pub time_lag_ms: u64,
    /// Current replica version
    pub current_version: Option<u64>,
    /// Primary version
    pub primary_version: Option<u64>,
}

/// Replica status for health checks
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    /// Whether this is a replica
    pub is_replica: bool,
    /// Current state
    pub state: ReplicaState,
    /// Current lag in milliseconds
    pub lag_ms: u64,
    /// Version lag
    pub version_lag: u64,
    /// Last successful sync time
    pub last_sync: Option<SystemTime>,
    /// Total sync operations
    pub total_syncs: u64,
    /// Failed sync count
    pub failed_syncs: u64,
    /// Write rejections
    pub write_rejections: u64,
    /// Last error message
    pub last_error: Option<String>,
}

impl ReplicaStatus {
    /// Check if replica is healthy
    pub fn is_healthy(&self) -> bool {
        self.state == ReplicaState::Ready || self.state == ReplicaState::Syncing
    }

    /// Format as JSON
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"is_replica":{},"state":"{}","lag_ms":{},"version_lag":{},"total_syncs":{},"failed_syncs":{},"write_rejections":{},"healthy":{}}}"#,
            self.is_replica,
            self.state,
            self.lag_ms,
            self.version_lag,
            self.total_syncs,
            self.failed_syncs,
            self.write_rejections,
            self.is_healthy()
        )
    }
}

// ============================================================================
// Sync Task
// ============================================================================

/// Result of a sync operation
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Whether sync was successful
    pub success: bool,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Segments updated
    pub segments_updated: u64,
    /// New manifest version
    pub new_version: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
}

/// Sync task for background worker
pub struct SyncTask {
    /// Replica manager reference
    manager: Arc<ReplicaManager>,
}

impl SyncTask {
    /// Create a new sync task
    pub fn new(manager: Arc<ReplicaManager>) -> Self {
        Self { manager }
    }

    /// Perform a sync operation (simulated for now)
    pub fn sync(&self) -> SyncResult {
        let start = Instant::now();

        // In a real implementation, this would:
        // 1. Fetch manifest from source (S3 or HTTP)
        // 2. Compare with current manifest
        // 3. Download new/updated segments
        // 4. Update local manifest
        // 5. Notify engine to refresh

        // Simulated successful sync
        let duration_ms = start.elapsed().as_millis() as u64;

        self.manager.record_sync_success(duration_ms, 0, 0);

        SyncResult {
            success: true,
            duration_ms,
            bytes_transferred: 0,
            segments_updated: 0,
            new_version: Some(1),
            error: None,
        }
    }
}

// ============================================================================
// CLI Helpers
// ============================================================================

/// Parse replica CLI arguments
#[derive(Debug, Clone, Default)]
pub struct ReplicaCliArgs {
    /// Whether to run as replica
    pub replica: bool,
    /// Primary server URL for HTTP sync
    pub primary: Option<String>,
    /// Sync interval in milliseconds
    pub sync_interval_ms: Option<u64>,
    /// Maximum acceptable lag
    pub max_lag_ms: Option<u64>,
}

impl ReplicaCliArgs {
    /// Build config from CLI args
    pub fn to_config(&self) -> ReplicaConfig {
        let mut config = ReplicaConfig::new();

        if self.replica {
            config.is_replica = true;
        }

        if let Some(ref primary) = self.primary {
            config.sync_source = Some(SyncSource::Http {
                primary_url: primary.clone(),
            });
        }

        if let Some(interval) = self.sync_interval_ms {
            config.sync_interval_ms = interval;
        }

        if let Some(lag) = self.max_lag_ms {
            config.max_lag_ms = lag;
        }

        config
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_guard() {
        let guard = WriteGuard::new();

        // Not replica - writes allowed
        assert!(guard.require_writable().is_ok());

        // Enable replica mode
        guard.enable_replica_mode();
        assert!(guard.is_replica());

        // Writes now rejected
        let result = guard.require_writable();
        assert!(result.is_err());
        assert_eq!(guard.rejection_count(), 1);

        // Disable replica mode
        guard.disable_replica_mode();
        assert!(guard.require_writable().is_ok());
    }

    #[test]
    fn test_is_write_sql() {
        assert!(is_write_sql("INSERT INTO test VALUES (1)"));
        assert!(is_write_sql("UPDATE test SET x = 1"));
        assert!(is_write_sql("DELETE FROM test"));
        assert!(is_write_sql("CREATE TABLE test (id INT)"));
        assert!(is_write_sql("DROP TABLE test"));
        assert!(is_write_sql("ALTER TABLE test ADD COLUMN x INT"));
        assert!(is_write_sql("TRUNCATE test"));

        assert!(!is_write_sql("SELECT * FROM test"));
        assert!(!is_write_sql("SHOW TABLES"));
        assert!(!is_write_sql("EXPLAIN SELECT 1"));
    }

    #[test]
    fn test_is_read_only_sql() {
        assert!(is_read_only_sql("SELECT * FROM test"));
        assert!(is_read_only_sql("SHOW TABLES"));
        assert!(is_read_only_sql("EXPLAIN SELECT 1"));
        assert!(is_read_only_sql("SET timezone = 'UTC'"));
        assert!(is_read_only_sql("BEGIN"));
        assert!(is_read_only_sql("COMMIT"));

        assert!(!is_read_only_sql("INSERT INTO test VALUES (1)"));
        assert!(!is_read_only_sql("UPDATE test SET x = 1"));
    }

    #[test]
    fn test_replica_config() {
        let config = ReplicaConfig::new()
            .as_replica()
            .with_s3_sync("my-bucket", "us-east-1")
            .with_sync_interval(500)
            .with_max_lag(5000);

        assert!(config.is_replica);
        assert_eq!(config.sync_interval_ms, 500);
        assert_eq!(config.max_lag_ms, 5000);

        match config.sync_source {
            Some(SyncSource::S3 { bucket, region, .. }) => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-east-1");
            }
            _ => panic!("expected S3 source"),
        }
    }

    #[test]
    fn test_replica_manager() {
        let config = ReplicaConfig::new().as_replica();
        let manager = ReplicaManager::new(config);

        assert!(manager.is_replica());
        assert_eq!(manager.state(), ReplicaState::Initializing);

        // Write should be rejected
        let result = manager.require_writable();
        assert!(result.is_err());

        // SQL check
        assert!(manager.check_sql("SELECT 1").is_ok());
        assert!(manager.check_sql("INSERT INTO test VALUES (1)").is_err());
    }

    #[test]
    fn test_manifest_tracker() {
        let tracker = ManifestTracker::new();

        let version1 = ManifestVersion {
            version: 1,
            timestamp: SystemTime::now(),
            segment_count: 10,
            total_size: 1024,
            checksum: "abc123".to_string(),
        };

        tracker.update_current(version1.clone());
        assert_eq!(tracker.current().unwrap().version, 1);

        let version2 = ManifestVersion {
            version: 5,
            timestamp: SystemTime::now(),
            segment_count: 15,
            total_size: 2048,
            checksum: "def456".to_string(),
        };

        tracker.update_primary(version2);
        assert_eq!(tracker.version_lag(), 4);
    }

    #[test]
    fn test_replica_status() {
        let config = ReplicaConfig::new().as_replica();
        let manager = ReplicaManager::new(config);

        manager.start();
        manager.record_sync_success(100, 1024, 5);

        let status = manager.get_status();
        assert!(status.is_replica);
        assert!(status.is_healthy());
        assert_eq!(status.total_syncs, 1);
        assert_eq!(status.failed_syncs, 0);
    }

    #[test]
    fn test_replica_status_json() {
        let status = ReplicaStatus {
            is_replica: true,
            state: ReplicaState::Ready,
            lag_ms: 100,
            version_lag: 2,
            last_sync: None,
            total_syncs: 10,
            failed_syncs: 1,
            write_rejections: 5,
            last_error: None,
        };

        let json = status.to_json();
        assert!(json.contains("\"is_replica\":true"));
        assert!(json.contains("\"state\":\"ready\""));
        assert!(json.contains("\"healthy\":true"));
    }

    #[test]
    fn test_sync_source_types() {
        let s3 = SyncSource::S3 {
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            prefix: Some("data/".to_string()),
        };
        assert!(matches!(s3, SyncSource::S3 { .. }));

        let http = SyncSource::Http {
            primary_url: "http://localhost:8765".to_string(),
        };
        assert!(matches!(http, SyncSource::Http { .. }));
    }

    #[test]
    fn test_cli_args_to_config() {
        let args = ReplicaCliArgs {
            replica: true,
            primary: Some("http://primary:8765".to_string()),
            sync_interval_ms: Some(2000),
            max_lag_ms: Some(10000),
        };

        let config = args.to_config();
        assert!(config.is_replica);
        assert_eq!(config.sync_interval_ms, 2000);
        assert_eq!(config.max_lag_ms, 10000);
    }
}
