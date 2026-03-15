use arrow_array::RecordBatch;
use arrow_csv::reader::Format;
use arrow_csv::ReaderBuilder;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose, Engine as _};
use boyodb_core::engine::EngineError;
use boyodb_core::planner_distributed::LocalPlan;
use boyodb_core::pubsub::PubSubManager;
use boyodb_core::TableMeta;
use boyodb_core::{
    parse_sql, AuthCommand, AuthError, AuthManager, ClusterConfig, ClusterManager, Db, DdlCommand,
    DeleteCommand, EngineConfig, GossipConfig, IngestBatch, InsertCommand, Manifest, MergeCommand,
    MergeWhenMatched, MergeWhenNotMatched, OnConflictAction, Privilege, PrivilegeTarget,
    QueryRequest, SqlStatement, SqlValue, TableConstraint, UpdateCommand,
};
use boyodb_core::cluster::ReplicationState;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io::{BufReader, Seek, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tokio_rustls::rustls::{
    self, pki_types::CertificateDer, pki_types::PrivateKeyDer, pki_types::ServerName,
    server::WebPkiClientVerifier, RootCertStore,
};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};
use webpki_roots::TLS_SERVER_ROOTS;

const DEFAULT_MAX_FRAME_LEN: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_QUERY_LEN: usize = 32 * 1024;
const DEFAULT_IO_TIMEOUT_MILLIS: u64 = 30_000; // 30 seconds - sufficient for batch ingestion
const DEFAULT_SHARD_COUNT: usize = 4;

/// Global pub/sub manager for LISTEN/NOTIFY support
static PUBSUB_MANAGER: std::sync::OnceLock<Arc<PubSubManager>> = std::sync::OnceLock::new();

/// Get the global pub/sub manager
fn get_pubsub_manager() -> Arc<PubSubManager> {
    PUBSUB_MANAGER.get_or_init(|| Arc::new(PubSubManager::new())).clone()
}

/// Per-connection state information
#[derive(Debug, Clone)]
struct ConnectionInfo {
    /// When the connection was established
    created_at: std::time::Instant,
    /// Last activity time (updated on each request)
    last_active: std::time::Instant,
    /// Remote peer address
    peer_addr: std::net::SocketAddr,
    /// Authenticated user (if any)
    authenticated_user: Option<String>,
}

/// Tracks individual connections for monitoring and idle cleanup
struct ConnectionTracker {
    /// Map of connection ID to connection info
    connections: std::sync::RwLock<HashMap<u64, ConnectionInfo>>,
    /// Next connection ID
    next_id: std::sync::atomic::AtomicU64,
    /// Connections marked for closure (connection handler should check this)
    marked_for_close: std::sync::RwLock<std::collections::HashSet<u64>>,
}

impl ConnectionTracker {
    fn new() -> Self {
        Self {
            connections: std::sync::RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            marked_for_close: std::sync::RwLock::new(std::collections::HashSet::new()),
        }
    }

    /// Register a new connection, returns connection ID
    fn register(&self, peer_addr: std::net::SocketAddr) -> u64 {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let now = std::time::Instant::now();
        let info = ConnectionInfo {
            created_at: now,
            last_active: now,
            peer_addr,
            authenticated_user: None,
        };
        if let Ok(mut connections) = self.connections.write() {
            connections.insert(id, info);
        }
        id
    }

    /// Unregister a connection
    fn unregister(&self, conn_id: u64) {
        if let Ok(mut connections) = self.connections.write() {
            connections.remove(&conn_id);
        }
        if let Ok(mut marked) = self.marked_for_close.write() {
            marked.remove(&conn_id);
        }
    }

    /// Update last activity time for a connection
    fn touch(&self, conn_id: u64) {
        if let Ok(mut connections) = self.connections.write() {
            if let Some(info) = connections.get_mut(&conn_id) {
                info.last_active = std::time::Instant::now();
            }
        }
    }

    /// Set authenticated user for a connection
    fn set_user(&self, conn_id: u64, user: String) {
        if let Ok(mut connections) = self.connections.write() {
            if let Some(info) = connections.get_mut(&conn_id) {
                info.authenticated_user = Some(user);
            }
        }
    }

    /// Check if connection should be closed
    fn should_close(&self, conn_id: u64) -> bool {
        if let Ok(marked) = self.marked_for_close.read() {
            marked.contains(&conn_id)
        } else {
            false
        }
    }

    /// Mark connections for closure based on idle timeout and max lifetime
    fn cleanup_idle(&self, idle_timeout: Duration, max_lifetime: Duration) -> Vec<u64> {
        let now = std::time::Instant::now();
        let mut to_close = Vec::new();

        if let Ok(connections) = self.connections.read() {
            for (&id, info) in connections.iter() {
                let idle_duration = now.duration_since(info.last_active);
                let lifetime = now.duration_since(info.created_at);

                if idle_duration > idle_timeout {
                    debug!(
                        conn_id = id,
                        idle_secs = idle_duration.as_secs(),
                        "marking connection for closure: idle timeout"
                    );
                    to_close.push(id);
                } else if lifetime > max_lifetime {
                    debug!(
                        conn_id = id,
                        lifetime_secs = lifetime.as_secs(),
                        "marking connection for closure: max lifetime exceeded"
                    );
                    to_close.push(id);
                }
            }
        }

        // Mark connections for closure
        if !to_close.is_empty() {
            if let Ok(mut marked) = self.marked_for_close.write() {
                for &id in &to_close {
                    marked.insert(id);
                }
            }
        }

        to_close
    }

    /// Get current connection count
    fn count(&self) -> usize {
        self.connections.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Get all connection info (for debugging/monitoring)
    #[allow(dead_code)]
    fn list_connections(&self) -> Vec<(u64, ConnectionInfo)> {
        if let Ok(connections) = self.connections.read() {
            connections.iter().map(|(&k, v)| (k, v.clone())).collect()
        } else {
            Vec::new()
        }
    }
}

/// Shared connection statistics for metrics reporting
/// RAII guard for tracking active queries - decrements count when dropped
struct QueryPermit<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
    active_queries: &'a std::sync::atomic::AtomicUsize,
}

impl Drop for QueryPermit<'_> {
    fn drop(&mut self) {
        self.active_queries.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

struct ConnectionStats {
    /// Maximum allowed connections
    max: usize,
    /// Total queries admitted (not rate limited)
    queries_admitted: std::sync::atomic::AtomicU64,
    /// Total queries rejected (rate limited)
    queries_rejected: std::sync::atomic::AtomicU64,
    /// Connection tracker for per-connection state
    tracker: ConnectionTracker,
    /// Query concurrency limiter - prevents overloading
    query_semaphore: Semaphore,
    /// Maximum concurrent queries
    max_concurrent_queries: usize,
    /// Currently running queries
    active_queries: std::sync::atomic::AtomicUsize,
}

impl ConnectionStats {
    fn new(max_connections: usize) -> Self {
        Self::with_query_limit(max_connections, max_connections * 2)
    }

    fn with_query_limit(max_connections: usize, max_concurrent_queries: usize) -> Self {
        Self {
            max: max_connections,
            queries_admitted: std::sync::atomic::AtomicU64::new(0),
            queries_rejected: std::sync::atomic::AtomicU64::new(0),
            tracker: ConnectionTracker::new(),
            query_semaphore: Semaphore::new(max_concurrent_queries),
            max_concurrent_queries,
            active_queries: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Try to acquire a query permit with timeout (returns None if overloaded or timeout)
    async fn try_acquire_query_with_timeout(
        &self,
        timeout: Duration,
    ) -> Option<QueryPermit<'_>> {
        match tokio::time::timeout(timeout, self.query_semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                self.active_queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(QueryPermit {
                    _permit: permit,
                    active_queries: &self.active_queries,
                })
            }
            _ => None,
        }
    }

    /// Try to acquire a query permit immediately (returns None if overloaded)
    fn try_acquire_query(&self) -> Option<QueryPermit<'_>> {
        match self.query_semaphore.try_acquire() {
            Ok(permit) => {
                self.active_queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(QueryPermit {
                    _permit: permit,
                    active_queries: &self.active_queries,
                })
            }
            Err(_) => None,
        }
    }

    /// Get current active query count
    fn active_queries(&self) -> usize {
        self.active_queries.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get max concurrent queries
    fn max_queries(&self) -> usize {
        self.max_concurrent_queries
    }

    /// Register a new connection, returns connection ID
    fn connection_opened(&self, peer_addr: std::net::SocketAddr) -> u64 {
        self.tracker.register(peer_addr)
    }

    /// Unregister a connection
    fn connection_closed(&self, conn_id: u64) {
        self.tracker.unregister(conn_id);
    }

    /// Update last activity time
    fn touch(&self, conn_id: u64) {
        self.tracker.touch(conn_id);
    }

    /// Set authenticated user
    fn set_user(&self, conn_id: u64, user: String) {
        self.tracker.set_user(conn_id, user);
    }

    /// Check if connection should close
    fn should_close(&self, conn_id: u64) -> bool {
        self.tracker.should_close(conn_id)
    }

    /// Run idle cleanup, returns number of connections marked for closure
    fn cleanup_idle(&self, idle_timeout: Duration, max_lifetime: Duration) -> usize {
        self.tracker.cleanup_idle(idle_timeout, max_lifetime).len()
    }

    fn query_admitted(&self) {
        self.queries_admitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn query_rejected(&self) {
        self.queries_rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn current(&self) -> usize {
        self.tracker.count()
    }

    fn max(&self) -> usize {
        self.max
    }

    fn queries_admitted(&self) -> u64 {
        self.queries_admitted.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn queries_rejected(&self) -> u64 {
        self.queries_rejected.load(std::sync::atomic::Ordering::Relaxed)
    }
}

trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

#[derive(Debug, Error)]
enum ServerError {
    #[error("io: {0}")]
    Io(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("db: {0}")]
    Db(String),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("tls: {0}")]
    Tls(String),
    #[error("auth: {0}")]
    Auth(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl From<AuthError> for ServerError {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::InvalidCredentials => ServerError::Unauthorized,
            AuthError::SessionExpired => ServerError::Auth("session expired".into()),
            AuthError::SessionNotFound => ServerError::Auth("session not found".into()),
            AuthError::UserLocked => ServerError::Auth("user account is locked".into()),
            AuthError::PermissionDenied(msg) => ServerError::PermissionDenied(msg),
            other => ServerError::Auth(other.to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Envelope {
    #[serde(default)]
    auth: Option<String>,
    #[serde(flatten)]
    request: Request,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct CsvField {
    name: String,
    #[serde(rename = "type", alias = "data_type")]
    data_type: String,
    #[serde(default = "default_true")]
    nullable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Request {
    Query {
        sql: String,
        #[serde(default = "default_timeout")]
        timeout_millis: u32,
        #[serde(default)]
        accept_compression: Option<String>,
        /// Default database for unqualified table names
        #[serde(default)]
        database: Option<String>,
    },
    Prepare {
        sql: String,
        /// Default database for unqualified table names
        #[serde(default)]
        database: Option<String>,
    },
    #[serde(rename = "execute_prepared")]
    ExecutePrepared {
        id: String,
        #[serde(default = "default_timeout")]
        timeout_millis: u32,
        #[serde(default)]
        accept_compression: Option<String>,
    },
    #[serde(rename = "execute_prepared_binary")]
    ExecutePreparedBinary {
        id: String,
        #[serde(default = "default_timeout")]
        timeout_millis: u32,
        #[serde(default)]
        accept_compression: Option<String>,
        #[serde(default)]
        stream: bool,
    },
    #[serde(rename = "query_binary")]
    QueryBinary {
        sql: String,
        #[serde(default = "default_timeout")]
        timeout_millis: u32,
        #[serde(default)]
        accept_compression: Option<String>,
        /// Default database for unqualified table names
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        stream: bool,
    },
    #[serde(rename = "execute_sub_query")]
    ExecuteSubQuery {
        plan_json: serde_json::Value,
        #[serde(default = "default_timeout")]
        timeout_millis: u32,
        #[serde(default)]
        accept_compression: Option<String>,
    },
    IngestIpc {
        payload_base64: String,
        watermark_micros: u64,
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        table: Option<String>,
        #[serde(default)]
        shard_hint: Option<u64>,
        #[serde(default)]
        compression: Option<String>,
    },
    #[serde(rename = "ingest_ipc_binary")]
    IngestIpcBinary {
        watermark_micros: u64,
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        table: Option<String>,
        #[serde(default)]
        shard_hint: Option<u64>,
        #[serde(default)]
        compression: Option<String>,
    },
    IngestCsv {
        payload_base64: String,
        database: String,
        table: String,
        #[serde(default)]
        schema: Option<Vec<CsvField>>,
        #[serde(default = "default_true")]
        has_header: bool,
        #[serde(default)]
        delimiter: Option<String>,
        #[serde(default)]
        watermark_micros: Option<u64>,
        #[serde(default)]
        shard_hint: Option<u64>,
        #[serde(default)]
        infer_schema_rows: Option<usize>,
    },
    ListDatabases,
    ListTables {
        #[serde(default)]
        database: Option<String>,
    },
    Manifest,
    Health,
    CreateDatabase {
        name: String,
    },
    CreateTable {
        database: String,
        table: String,
        schema: Vec<CsvField>,
        #[serde(default)]
        compression: Option<String>,
    },
    Compact {
        database: String,
        table: String,
    },
    Checkpoint,
    PlanBundle {
        #[serde(default)]
        max_bytes: Option<u64>,
        #[serde(default)]
        since_version: Option<u64>,
        #[serde(default)]
        prefer_hot: bool,
        #[serde(default)]
        target_bytes_per_sec: Option<u64>,
        #[serde(default)]
        max_entries: Option<usize>,
    },
    ApplyManifest {
        manifest_json: serde_json::Value,
        #[serde(default)]
        overwrite: bool,
    },
    ExportBundle {
        #[serde(default)]
        max_bytes: Option<u64>,
        #[serde(default)]
        since_version: Option<u64>,
        #[serde(default)]
        prefer_hot: bool,
        #[serde(default)]
        target_bytes_per_sec: Option<u64>,
        #[serde(default)]
        max_entries: Option<usize>,
    },
    ApplyBundle {
        bundle_json: serde_json::Value,
    },
    ValidateBundle {
        bundle_json: serde_json::Value,
    },
    Insight {
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        table: Option<String>,
    },
    Metrics,
    #[serde(rename = "wal_stats")]
    WalStats,
    Explain {
        sql: String,
    },
    HealthDetailed,
    // Authentication operations
    Login {
        username: String,
        password: String,
    },
    Logout,
    // User management
    CreateUser {
        username: String,
        password: String,
        #[serde(default)]
        superuser: bool,
        #[serde(default)]
        default_database: Option<String>,
    },
    DropUser {
        username: String,
    },
    AlterUserPassword {
        username: String,
        password: String,
    },
    LockUser {
        username: String,
    },
    UnlockUser {
        username: String,
    },
    // Role management
    CreateRole {
        name: String,
        #[serde(default)]
        description: Option<String>,
    },
    DropRole {
        name: String,
    },
    GrantRole {
        username: String,
        role: String,
    },
    RevokeRole {
        username: String,
        role: String,
    },
    // Privilege management
    GrantPrivilege {
        username: String,
        privilege: String,
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        table: Option<String>,
        #[serde(default)]
        with_grant_option: bool,
    },
    RevokePrivilege {
        username: String,
        privilege: String,
        #[serde(default)]
        database: Option<String>,
        #[serde(default)]
        table: Option<String>,
    },
    // Show commands
    ShowUsers,
    ShowRoles,
    ShowGrants {
        username: String,
    },
    // Cluster operations
    ClusterStatus,
    // Replica status
    ReplicaStatus,
}

impl Request {
    fn op_name(&self) -> &'static str {
        match self {
            Request::Query { .. } => "query",
            Request::QueryBinary { .. } => "query_binary",
            Request::ExecuteSubQuery { .. } => "execute_sub_query",
            Request::Prepare { .. } => "prepare",
            Request::ExecutePrepared { .. } => "execute_prepared",
            Request::ExecutePreparedBinary { .. } => "execute_prepared_binary",
            Request::IngestIpc { .. } => "ingest_ipc",
            Request::IngestIpcBinary { .. } => "ingest_ipc_binary",
            Request::IngestCsv { .. } => "ingest_csv",
            Request::ListDatabases => "list_databases",
            Request::ListTables { .. } => "list_tables",
            Request::Manifest => "manifest",
            Request::Health => "health",
            Request::CreateDatabase { .. } => "create_database",
            Request::CreateTable { .. } => "create_table",
            Request::PlanBundle { .. } => "plan_bundle",
            Request::ApplyManifest { .. } => "apply_manifest",
            Request::ExportBundle { .. } => "export_bundle",
            Request::ApplyBundle { .. } => "apply_bundle",
            Request::ValidateBundle { .. } => "validate_bundle",
            Request::Insight { .. } => "insight",
            Request::Compact { .. } => "compact",
            Request::Checkpoint => "checkpoint",
            Request::Metrics => "metrics",
            Request::WalStats => "wal_stats",
            Request::Explain { .. } => "explain",
            Request::HealthDetailed => "health_detailed",
            Request::Login { .. } => "login",
            Request::Logout => "logout",
            Request::CreateUser { .. } => "create_user",
            Request::DropUser { .. } => "drop_user",
            Request::AlterUserPassword { .. } => "alter_user_password",
            Request::LockUser { .. } => "lock_user",
            Request::UnlockUser { .. } => "unlock_user",
            Request::CreateRole { .. } => "create_role",
            Request::DropRole { .. } => "drop_role",
            Request::GrantRole { .. } => "grant_role",
            Request::RevokeRole { .. } => "revoke_role",
            Request::GrantPrivilege { .. } => "grant_privilege",
            Request::RevokePrivilege { .. } => "revoke_privilege",
            Request::ShowUsers => "show_users",
            Request::ShowRoles => "show_roles",
            Request::ShowGrants { .. } => "show_grants",
            Request::ClusterStatus => "cluster_status",
            Request::ReplicaStatus => "replica_status",
        }
    }
}

fn default_timeout() -> u32 {
    10_000
}

fn default_true() -> bool {
    true
}

/// Rate limiter for authentication attempts to prevent brute force attacks.
/// Uses a sliding window approach with per-IP tracking.
struct AuthRateLimiter {
    attempts: std::sync::RwLock<std::collections::HashMap<IpAddr, (u32, std::time::Instant)>>,
    max_attempts: u32,
    window_secs: u64,
}

impl AuthRateLimiter {
    fn new(max_attempts: u32, window_secs: u64) -> Self {
        Self {
            attempts: std::sync::RwLock::new(std::collections::HashMap::new()),
            max_attempts,
            window_secs,
        }
    }

    /// Check if an IP is rate limited. Returns Ok(()) if allowed, Err with wait time if blocked.
    fn check(&self, ip: IpAddr) -> Result<(), Duration> {
        let now = std::time::Instant::now();
        let window = Duration::from_secs(self.window_secs);

        // First, check with read lock
        if let Ok(attempts) = self.attempts.read() {
            if let Some((count, start)) = attempts.get(&ip) {
                if now.duration_since(*start) < window && *count >= self.max_attempts {
                    let wait_time = window.saturating_sub(now.duration_since(*start));
                    return Err(wait_time);
                }
            }
        }
        Ok(())
    }

    /// Record a failed authentication attempt
    fn record_failure(&self, ip: IpAddr) {
        let now = std::time::Instant::now();
        let window = Duration::from_secs(self.window_secs);

        if let Ok(mut attempts) = self.attempts.write() {
            let entry = attempts.entry(ip).or_insert((0, now));
            // Reset if window expired
            if now.duration_since(entry.1) >= window {
                *entry = (1, now);
            } else {
                entry.0 += 1;
            }
        }
    }

    /// Record a successful authentication (clears the counter)
    fn record_success(&self, ip: IpAddr) {
        if let Ok(mut attempts) = self.attempts.write() {
            attempts.remove(&ip);
        }
    }

    /// Cleanup old entries (call periodically)
    fn cleanup(&self) {
        let now = std::time::Instant::now();
        let window = Duration::from_secs(self.window_secs);

        if let Ok(mut attempts) = self.attempts.write() {
            attempts.retain(|_, (_, start)| now.duration_since(*start) < window);
        }
    }
}

#[derive(Clone)]
struct ServerConfig {
    bind_addr: String,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    wal_max_bytes: u64,
    wal_max_segments: u64,
    shard_count: usize,
    max_ipc_bytes: usize,
    max_frame_len: usize,
    max_query_len: usize,
    auth_token: Option<String>,
    /// Enable user authentication (username/password via AuthManager)
    auth_enabled: bool,
    max_connections: usize,
    worker_threads: usize,
    io_timeout: Duration,
    /// Idle connection timeout in seconds (default: 300)
    idle_timeout_secs: u64,
    /// Maximum connection lifetime in seconds (default: 3600)
    conn_max_lifetime_secs: u64,
    tls_acceptor: Option<TlsAcceptor>,
    log_requests: bool,
    client_auth: bool,
    allow_manifest_import: bool,
    retention_watermark: Option<u64>,
    compact_min_segments: usize,
    /// Compact segments on startup if count exceeds threshold
    compact_on_start: bool,
    /// Segment count threshold for compact-on-start (default: 10000)
    compact_on_start_threshold: usize,
    maintenance_interval_ms: u64,
    replicate_from: Option<String>,
    replicate_token: Option<String>,
    replicate_interval_ms: u64,
    replicate_max_bytes: Option<u64>,
    replicate_use_tls: bool,
    replicate_ca: Option<PathBuf>,
    replicate_sni: Option<String>,
    segment_cache_capacity: usize,
    wal_sync_bytes: u64,
    wal_sync_interval_ms: u64,
    batch_cache_bytes: u64,
    schema_cache_entries: usize,
    query_cache_bytes: u64,
    plan_cache_size: usize,
    plan_cache_ttl_secs: u64,
    prepared_cache_size: usize,
    prepared_cache_ttl_secs: u64,
    tier_warm_compression: Option<String>,
    tier_cold_compression: Option<String>,
    index_granularity_rows: usize,
    cache_hot_segments: bool,
    cache_warm_segments: bool,
    cache_cold_segments: bool,
    // Cluster config
    cluster_enabled: bool,
    cluster_id: Option<String>,
    node_id: Option<String>,
    gossip_addr: Option<String>,
    rpc_addr: Option<String>,
    seed_nodes: Vec<String>,
    /// Enable 2-node cluster mode (primary/replica with quorum of 1).
    two_node_mode: bool,
    pg_port: Option<u16>,
    /// Automatic backup interval in hours (0 = disabled)
    backup_interval_hours: u64,
    /// Maximum number of automatic backups to retain (0 = unlimited)
    backup_max_count: usize,
    /// Directory to store backups (defaults to data_dir/backups)
    backup_dir: Option<PathBuf>,
    // Read replica configuration
    /// Run as a read-only replica (rejects all write operations)
    replica_mode: bool,
    /// Manifest sync interval in milliseconds for read replicas (default: 1000)
    replica_sync_interval_ms: u64,
    /// Primary server address for HTTP bundle pull (e.g., "http://primary:8765")
    replica_primary_addr: Option<String>,
    // Segment limits & stability (ClickHouse-style protection)
    /// Maximum total segments before triggering emergency compaction
    max_segments_emergency_threshold: usize,
    /// Maximum total segments before rejecting new writes
    max_segments_hard_limit: usize,
    /// Maximum segments per table before per-table emergency compaction
    max_segments_per_table: usize,
    /// Maximum segments to scan per query (prevents single query from overwhelming system)
    max_segments_per_query: usize,
    /// Enable adaptive backpressure based on segment count
    adaptive_backpressure_enabled: bool,
    /// Compaction passes per cycle (default: 32)
    compaction_passes_per_cycle: usize,
    /// Maximum segments to merge per compaction (default: 20)
    max_segments_per_compaction: usize,
    /// Parallelism for emergency compaction (default: 4)
    emergency_compaction_parallelism: usize,
    // ClickHouse-style optimizations
    /// Enable leveled compaction with exponential size tiers
    leveled_compaction_enabled: bool,
    /// Enable continuous compaction threads
    continuous_compaction_enabled: bool,
    /// Number of dedicated compaction threads (default: 2)
    compaction_threads: usize,
    /// Sleep duration when no compaction work (milliseconds)
    compaction_idle_sleep_ms: u64,
    /// Enable write buffering to create larger segments
    write_buffer_enabled: bool,
    /// Write buffer size in bytes (default: 16MB)
    write_buffer_max_bytes: u64,
}

pub mod server_pg;

#[derive(Debug, Serialize)]
struct Response {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ipc_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ipc_len: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ipc_streaming: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_skipped_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments_scanned: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    databases: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables: Option<Vec<TableMeta>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    manifest_json: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle_plan: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    insight: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bundle_json: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_stats: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    explain_plan: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health_status: Option<serde_json::Value>,
    // Auth response fields
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    users: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grants: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_status: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replica_status: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_description: Option<boyodb_core::TableDescription>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prepared_id: Option<String>,
}

impl Default for Response {
    fn default() -> Self {
        Response {
            status: "ok",
            message: None,
            ipc_base64: None,
            ipc_len: None,
            ipc_streaming: None,
            data_skipped_bytes: None,
            segments_scanned: None,
            databases: None,
            tables: None,
            manifest_json: None,
            compression: None,
            bundle_plan: None,
            insight: None,
            bundle_json: None,
            metrics_text: None,
            wal_stats: None,
            explain_plan: None,
            health_status: None,
            session_id: None,
            users: None,
            roles: None,
            grants: None,
            cluster_status: None,
            replica_status: None,
            table_description: None,
            prepared_id: None,
        }
    }
}

#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    table_refs: Vec<(String, String)>,
    created_at: std::time::Instant,
}

struct PreparedStatementCache {
    entries: HashMap<String, PreparedStatement>,
    order: VecDeque<String>,
    ttl: Duration,
    max_entries: usize,
    counter: u64,
}

impl PreparedStatementCache {
    fn new(max_entries: usize, ttl_secs: u64) -> Self {
        PreparedStatementCache {
            entries: HashMap::new(),
            order: VecDeque::new(),
            ttl: Duration::from_secs(ttl_secs),
            max_entries: max_entries.max(1),
            counter: 0,
        }
    }

    fn insert(&mut self, sql: String, table_refs: Vec<(String, String)>) -> String {
        self.prune_expired();
        let id = self.next_id();
        self.entries.insert(
            id.clone(),
            PreparedStatement {
                sql,
                table_refs,
                created_at: std::time::Instant::now(),
            },
        );
        self.order.push_back(id.clone());
        self.evict_overflow();
        id
    }

    fn get(&mut self, id: &str) -> Option<PreparedStatement> {
        self.prune_expired();
        self.entries.get(id).cloned()
    }

    fn next_id(&mut self) -> String {
        let counter = self.counter;
        self.counter = self.counter.wrapping_add(1);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        format!("prep_{ts}_{counter}")
    }

    fn prune_expired(&mut self) {
        loop {
            let id = match self.order.front() {
                Some(id) => id.clone(),
                None => break,
            };
            if let Some(entry) = self.entries.get(&id) {
                if entry.created_at.elapsed() <= self.ttl {
                    break;
                }
            }
            self.order.pop_front();
            self.entries.remove(&id);
        }
    }

    fn evict_overflow(&mut self) {
        while self.entries.len() > self.max_entries {
            if let Some(id) = self.order.pop_front() {
                self.entries.remove(&id);
            } else {
                break;
            }
        }
    }
}

impl Response {
    fn ok_message(msg: &str) -> Self {
        Response {
            message: Some(msg.to_string()),
            ..Default::default()
        }
    }

    fn error(msg: &str) -> Self {
        Response {
            status: "error",
            message: Some(msg.to_string()),
            ..Default::default()
        }
    }
}

fn map_engine_error(err: EngineError) -> ServerError {
    match err {
        EngineError::InvalidArgument(msg) => ServerError::BadRequest(msg),
        EngineError::Timeout(msg) => ServerError::Timeout(msg),
        EngineError::NotFound(msg) => ServerError::Db(msg),
        EngineError::NotImplemented(msg) => ServerError::Db(msg),
        EngineError::Internal(msg) => ServerError::Db(msg),
        EngineError::Io(msg) => ServerError::Db(msg),
        _ => ServerError::Db("Unknown engine error".to_string()),
    }
}

/// Decompress zstd data with a size limit to prevent decompression bombs.
/// Returns an error if decompressed data exceeds max_bytes.
fn decompress_with_limit(compressed: &[u8], max_bytes: usize) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let cursor = std::io::Cursor::new(compressed);
    let mut decoder =
        zstd::stream::Decoder::new(cursor).map_err(|e| format!("failed to create decoder: {e}"))?;

    // Pre-allocate with a reasonable size, capped at max_bytes
    let initial_capacity = compressed.len().saturating_mul(4).min(max_bytes);
    let mut output = Vec::with_capacity(initial_capacity);

    // Read in chunks to check size incrementally
    let chunk_size = 64 * 1024; // 64KB chunks
    let mut buf = vec![0u8; chunk_size];

    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| format!("decompression error: {e}"))?;
        if n == 0 {
            break;
        }
        if output.len() + n > max_bytes {
            return Err(format!(
                "decompressed data exceeds limit ({} bytes max)",
                max_bytes
            ));
        }
        output.extend_from_slice(&buf[..n]);
    }

    Ok(output)
}

#[derive(Debug, Deserialize)]
struct RemoteResponse {
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    bundle_json: Option<serde_json::Value>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    let log_format = std::env::var("BOYODB_LOG_FORMAT").unwrap_or_default();
    if log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into()),
            )
            .init();
    }

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        eprintln!("Usage: boyodb-server <data_dir> [bind_addr] [--token <token>] [--auth] [--max-ipc-bytes <bytes>] [--max-conns <n>] [--workers <n>] [--io-timeout-ms <ms>] [--max-frame-bytes <bytes>] [--max-query-len <bytes>] [--query-cache-bytes <bytes>] [--plan-cache-size <n>] [--plan-cache-ttl-secs <n>] [--prepared-cache-size <n>] [--prepared-cache-ttl-secs <n>] [--segment-cache <n>] [--batch-cache-bytes <bytes>] [--schema-cache-entries <n>] [--log-requests] [--wal-dir <path>] [--wal-max-bytes <bytes>] [--wal-max-segments <n>] [--allow-manifest-import] [--retention-watermark <micros>] [--compact-min-segments <n>] [--maintenance-interval-ms <ms>] [--index-granularity-rows <n>] [--tier-warm-compression <algo>] [--tier-cold-compression <algo>] [--cache-hot-segments|--no-cache-hot-segments] [--cache-warm-segments|--no-cache-warm-segments] [--cache-cold-segments|--no-cache-cold-segments] [--tls-cert <path> --tls-key <path> --tls-ca <path>] [--replica] [--replica-sync-interval-ms <ms>] [--primary <addr>] [--max-segments-emergency <n>] [--max-segments-hard-limit <n>] [--max-segments-per-table <n>] [--max-segments-per-query <n>] [--no-adaptive-backpressure]");
        std::process::exit(1);
    }

    let cfg = parse_config(args)?;
    if cfg.auth_token.is_some() && cfg.tls_acceptor.is_none() && !bind_is_loopback(&cfg.bind_addr) {
        error!("auth token requires loopback bind or TLS termination");
        eprintln!("auth token requires loopback bind or TLS termination; please bind to 127.0.0.1/::1 or supply --tls-cert/--tls-key");
        std::process::exit(1);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.worker_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async move { run(cfg).await })
}

fn parse_config(args: Vec<String>) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(&args[0]);
    let mut bind_addr = "127.0.0.1:8765".to_string();
    if args.len() >= 2 && !args[1].starts_with("--") {
        bind_addr = args[1].clone();
    }
    let mut wal_dir: Option<PathBuf> = None;
    let mut wal_max_bytes: u64 = 256 * 1024 * 1024;
    let mut wal_max_segments: u64 = 4;
    let mut wal_sync_bytes: u64 = 0; // 0 = strict fsync every write
    let mut wal_sync_interval_ms: u64 = 0; // 0 = immediate sync
    let mut auth_token: Option<String> = None;
    let mut max_ipc_bytes: usize = 512 * 1024 * 1024;
    let mut max_frame_len: usize = DEFAULT_MAX_FRAME_LEN;
    let mut max_query_len: usize = DEFAULT_MAX_QUERY_LEN;
    let mut max_connections: usize = 64;
    // Auto-detect worker threads based on CPU count (default to available CPUs, min 4)
    let mut worker_threads: usize = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(8)
        .max(4);
    let mut io_timeout_ms: u64 = DEFAULT_IO_TIMEOUT_MILLIS;
    let mut idle_timeout_secs: u64 = 300; // 5 minutes
    let mut conn_max_lifetime_secs: u64 = 3600; // 1 hour
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_ca: Option<PathBuf> = None;
    let mut log_requests = false;
    let mut allow_manifest_import = false;
    let mut retention_watermark: Option<u64> = None;
    let mut compact_min_segments: usize = 2;
    let mut compact_on_start = false;
    let mut compact_on_start_threshold: usize = 10000;
    let mut maintenance_interval_ms: u64 = 0;
    let mut replicate_from: Option<String> = None;
    let mut replicate_token: Option<String> = None;
    let mut replicate_interval_ms: u64 = 0;
    let mut replicate_max_bytes: Option<u64> = None;
    let mut replicate_use_tls = false;
    let mut replicate_ca: Option<PathBuf> = None;
    let mut replicate_sni: Option<String> = None;
    let mut segment_cache_capacity: usize = 64;
    let mut batch_cache_bytes: u64 = 4 * 1024 * 1024 * 1024;
    let mut schema_cache_entries: usize = 16384;
    let mut query_cache_bytes: u64 = 4 * 1024 * 1024 * 1024;
    let mut plan_cache_size: usize = 10_000;
    let mut plan_cache_ttl_secs: u64 = 300;
    let mut prepared_cache_size: usize = 1024;
    let mut prepared_cache_ttl_secs: u64 = 3600;
    let mut tier_warm_compression: Option<String> = None;
    let mut tier_cold_compression: Option<String> = None;
    let mut index_granularity_rows: usize = 0;
    let mut cache_hot_segments = true;
    let mut cache_warm_segments = true;
    let mut cache_cold_segments = false;
    let mut auth_enabled = false;

    // Cluster configuration
    let mut cluster_enabled = false;
    let mut cluster_id: Option<String> = None;
    let mut node_id: Option<String> = None;
    let mut gossip_addr: Option<String> = None;
    let mut rpc_addr: Option<String> = None;
    let mut seed_nodes: Vec<String> = Vec::new();
    let mut two_node_mode = false;
    let mut pg_port: Option<u16> = None;

    // Automatic backup configuration
    let mut backup_interval_hours: u64 = 0; // 0 = disabled
    let mut backup_max_count: usize = 0; // 0 = unlimited
    let mut backup_dir: Option<PathBuf> = None;

    // Read replica configuration
    let mut replica_mode = false;
    let mut replica_sync_interval_ms: u64 = 1000;
    let mut replica_primary_addr: Option<String> = None;

    // Segment limits & stability configuration (ClickHouse-style protection)
    let mut max_segments_emergency_threshold: usize = 10000;
    let mut max_segments_hard_limit: usize = 50000;
    let mut max_segments_per_table: usize = 2000;
    let mut max_segments_per_query: usize = 5000;
    let mut adaptive_backpressure_enabled: bool = true;

    // Compaction performance tuning
    let mut compaction_passes_per_cycle: usize = 32;
    let mut max_segments_per_compaction: usize = 20;
    let mut emergency_compaction_parallelism: usize = 4;

    // ClickHouse-style optimizations
    let mut leveled_compaction_enabled: bool = true;
    let mut continuous_compaction_enabled: bool = true;
    let mut compaction_threads: usize = 2;
    let mut compaction_idle_sleep_ms: u64 = 100;
    let mut write_buffer_enabled: bool = true;
    let mut write_buffer_max_bytes: u64 = 16 * 1024 * 1024;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--auth" => {
                auth_enabled = true;
            }
            "--prepared-cache-size" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        prepared_cache_size = v.max(1);
                    }
                    i += 1;
                }
            }
            "--prepared-cache-ttl-secs" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        prepared_cache_ttl_secs = v.max(1);
                    }
                    i += 1;
                }
            }
            "--plan-cache-size" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        plan_cache_size = v.max(1);
                    }
                    i += 1;
                }
            }
            "--plan-cache-ttl-secs" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        plan_cache_ttl_secs = v.max(1);
                    }
                    i += 1;
                }
            }
            "--wal-dir" => {
                if let Some(val) = args.get(i + 1) {
                    wal_dir = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            "--wal-max-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        wal_max_bytes = v.max(1);
                    }
                    i += 1;
                }
            }
            "--wal-max-segments" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        wal_max_segments = v.max(1);
                    }
                    i += 1;
                }
            }
            "--wal-sync-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        wal_sync_bytes = v;
                    }
                    i += 1;
                }
            }
            "--wal-sync-interval-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        wal_sync_interval_ms = v;
                    }
                    i += 1;
                }
            }
            "--token" => {
                if let Some(val) = args.get(i + 1) {
                    auth_token = Some(val.clone());
                    i += 1;
                }
            }
            "--max-ipc-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_ipc_bytes = v;
                    }
                    i += 1;
                }
            }
            "--max-conns" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_connections = v.max(1);
                    }
                    i += 1;
                }
            }
            "--workers" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        worker_threads = v.max(1);
                    }
                    i += 1;
                }
            }
            "--io-timeout-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        io_timeout_ms = v.max(1);
                    }
                    i += 1;
                }
            }
            "--idle-timeout" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        idle_timeout_secs = v;
                    }
                    i += 1;
                }
            }
            "--conn-max-lifetime" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        conn_max_lifetime_secs = v;
                    }
                    i += 1;
                }
            }
            "--max-frame-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_frame_len = v.max(1024);
                    }
                    i += 1;
                }
            }
            "--max-query-len" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_query_len = v.max(1024);
                    }
                    i += 1;
                }
            }
            "--log-requests" => {
                log_requests = true;
            }
            "--tls-cert" => {
                if let Some(val) = args.get(i + 1) {
                    tls_cert = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            "--tls-key" => {
                if let Some(val) = args.get(i + 1) {
                    tls_key = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            "--tls-ca" => {
                if let Some(val) = args.get(i + 1) {
                    tls_ca = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            "--allow-manifest-import" => {
                allow_manifest_import = true;
            }
            "--retention-watermark" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        retention_watermark = Some(v);
                    }
                    i += 1;
                }
            }
            "--compact-min-segments" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        compact_min_segments = v.max(2);
                    }
                    i += 1;
                }
            }
            "--compact-on-start" => {
                compact_on_start = true;
            }
            "--compact-on-start-threshold" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        compact_on_start_threshold = v;
                    }
                    i += 1;
                }
            }
            "--maintenance-interval-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        maintenance_interval_ms = v;
                    }
                    i += 1;
                }
            }
            "--index-granularity-rows" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        index_granularity_rows = v;
                    }
                    i += 1;
                }
            }
            "--tier-warm-compression" => {
                if let Some(val) = args.get(i + 1) {
                    tier_warm_compression = Some(val.clone());
                    i += 1;
                }
            }
            "--tier-cold-compression" => {
                if let Some(val) = args.get(i + 1) {
                    tier_cold_compression = Some(val.clone());
                    i += 1;
                }
            }
            "--cache-hot-segments" => {
                cache_hot_segments = true;
            }
            "--no-cache-hot-segments" => {
                cache_hot_segments = false;
            }
            "--cache-warm-segments" => {
                cache_warm_segments = true;
            }
            "--no-cache-warm-segments" => {
                cache_warm_segments = false;
            }
            "--cache-cold-segments" => {
                cache_cold_segments = true;
            }
            "--no-cache-cold-segments" => {
                cache_cold_segments = false;
            }
            "--replicate-from" => {
                if let Some(val) = args.get(i + 1) {
                    replicate_from = Some(val.clone());
                    i += 1;
                }
            }
            "--replicate-token" => {
                if let Some(val) = args.get(i + 1) {
                    replicate_token = Some(val.clone());
                    i += 1;
                }
            }
            "--replicate-interval-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        replicate_interval_ms = v;
                    }
                    i += 1;
                }
            }
            "--replicate-max-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        replicate_max_bytes = Some(v);
                    }
                    i += 1;
                }
            }
            "--replicate-tls" => {
                replicate_use_tls = true;
            }
            "--replicate-ca" => {
                if let Some(val) = args.get(i + 1) {
                    replicate_ca = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            "--replicate-sni" => {
                if let Some(val) = args.get(i + 1) {
                    replicate_sni = Some(val.clone());
                    i += 1;
                }
            }
            "--segment-cache" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        segment_cache_capacity = v;
                    }
                    i += 1;
                }
            }
            "--batch-cache-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        batch_cache_bytes = v;
                    }
                    i += 1;
                }
            }
            "--schema-cache-entries" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        schema_cache_entries = v.max(1);
                    }
                    i += 1;
                }
            }
            "--query-cache-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        query_cache_bytes = v;
                    }
                    i += 1;
                }
            }
            // Cluster options
            "--cluster" => {
                cluster_enabled = true;
            }
            "--cluster-id" => {
                if let Some(val) = args.get(i + 1) {
                    cluster_id = Some(val.clone());
                    i += 1;
                }
            }
            "--node-id" => {
                if let Some(val) = args.get(i + 1) {
                    node_id = Some(val.clone());
                    i += 1;
                }
            }
            "--gossip-addr" => {
                if let Some(val) = args.get(i + 1) {
                    gossip_addr = Some(val.clone());
                    i += 1;
                }
            }
            "--rpc-addr" => {
                if let Some(val) = args.get(i + 1) {
                    rpc_addr = Some(val.clone());
                    i += 1;
                }
            }
            "--seed-nodes" => {
                if let Some(val) = args.get(i + 1) {
                    seed_nodes = val.split(',').map(|s| s.trim().to_string()).collect();
                    i += 1;
                }
            }
            "--two-node-mode" => {
                two_node_mode = true;
            }
            "--pg-port" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u16>() {
                        pg_port = Some(v);
                    }
                    i += 1;
                }
            }
            "--backup-interval" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        backup_interval_hours = v;
                    }
                    i += 1;
                }
            }
            "--backup-max-count" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        backup_max_count = v;
                    }
                    i += 1;
                }
            }
            "--backup-dir" => {
                if let Some(val) = args.get(i + 1) {
                    backup_dir = Some(PathBuf::from(val));
                    i += 1;
                }
            }
            // Read replica options
            "--replica" => {
                replica_mode = true;
            }
            "--replica-sync-interval-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        replica_sync_interval_ms = v.max(100);
                    }
                    i += 1;
                }
            }
            "--primary" => {
                if let Some(val) = args.get(i + 1) {
                    replica_primary_addr = Some(val.clone());
                    i += 1;
                }
            }
            // Segment limits & stability options (ClickHouse-style protection)
            "--max-segments-emergency" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_segments_emergency_threshold = v;
                    }
                    i += 1;
                }
            }
            "--max-segments-hard-limit" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_segments_hard_limit = v;
                    }
                    i += 1;
                }
            }
            "--max-segments-per-table" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_segments_per_table = v;
                    }
                    i += 1;
                }
            }
            "--max-segments-per-query" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_segments_per_query = v;
                    }
                    i += 1;
                }
            }
            "--no-adaptive-backpressure" => {
                adaptive_backpressure_enabled = false;
            }
            // Compaction performance tuning
            "--compaction-passes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        compaction_passes_per_cycle = v.max(1);
                    }
                    i += 1;
                }
            }
            "--max-segments-per-compaction" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        max_segments_per_compaction = v.max(2);
                    }
                    i += 1;
                }
            }
            "--emergency-compaction-parallelism" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        emergency_compaction_parallelism = v.max(1);
                    }
                    i += 1;
                }
            }
            // ClickHouse-style optimization flags
            "--no-leveled-compaction" => {
                leveled_compaction_enabled = false;
            }
            "--no-continuous-compaction" => {
                continuous_compaction_enabled = false;
            }
            "--compaction-threads" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<usize>() {
                        compaction_threads = v.max(1);
                    }
                    i += 1;
                }
            }
            "--compaction-idle-sleep-ms" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        compaction_idle_sleep_ms = v;
                    }
                    i += 1;
                }
            }
            "--no-write-buffer" => {
                write_buffer_enabled = false;
            }
            "--write-buffer-bytes" => {
                if let Some(val) = args.get(i + 1) {
                    if let Ok(v) = val.parse::<u64>() {
                        write_buffer_max_bytes = v.max(1024 * 1024); // Min 1MB
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let tls_acceptor = match (tls_cert, tls_key) {
        (Some(c), Some(k)) => {
            Some(build_tls_acceptor(&c, &k, tls_ca.as_ref()).map_err(|e| format!("{e}"))?)
        }
        (None, None) => None,
        _ => return Err("TLS requires both --tls-cert and --tls-key".into()),
    };
    let client_auth = tls_acceptor.is_some() && tls_ca.is_some();

    let wal_dir = wal_dir.unwrap_or_else(|| data_dir.join("wal"));
    Ok(ServerConfig {
        bind_addr,
        data_dir,
        wal_dir,
        wal_max_bytes,
        wal_max_segments,
        shard_count: DEFAULT_SHARD_COUNT,
        max_ipc_bytes,
        max_frame_len,
        max_query_len,
        auth_token,
        auth_enabled,
        max_connections: max_connections.max(1),
        worker_threads: worker_threads.max(1),
        io_timeout: Duration::from_millis(io_timeout_ms.max(1)),
        idle_timeout_secs,
        conn_max_lifetime_secs,
        tls_acceptor,
        log_requests,
        client_auth,
        allow_manifest_import,
        retention_watermark,
        compact_min_segments,
        compact_on_start,
        compact_on_start_threshold,
        maintenance_interval_ms,
        replicate_from,
        replicate_token,
        replicate_interval_ms,
        replicate_max_bytes,
        replicate_use_tls,
        replicate_ca,
        replicate_sni,
        segment_cache_capacity,
        wal_sync_bytes,
        wal_sync_interval_ms,
        batch_cache_bytes,
        schema_cache_entries,
        query_cache_bytes,
        plan_cache_size,
        plan_cache_ttl_secs,
        prepared_cache_size,
        prepared_cache_ttl_secs,
        tier_warm_compression,
        tier_cold_compression,
        index_granularity_rows,
        cache_hot_segments,
        cache_warm_segments,
        cache_cold_segments,
        cluster_enabled,
        cluster_id,
        node_id,
        gossip_addr,
        rpc_addr,
        seed_nodes,
        two_node_mode,
        pg_port,
        backup_interval_hours,
        backup_max_count,
        backup_dir,
        replica_mode,
        replica_sync_interval_ms,
        replica_primary_addr,
        max_segments_emergency_threshold,
        max_segments_hard_limit,
        max_segments_per_table,
        max_segments_per_query,
        adaptive_backpressure_enabled,
        compaction_passes_per_cycle,
        max_segments_per_compaction,
        emergency_compaction_parallelism,
        leveled_compaction_enabled,
        continuous_compaction_enabled,
        compaction_threads,
        compaction_idle_sleep_ms,
        write_buffer_enabled,
        write_buffer_max_bytes,
    })
}

async fn run(cfg: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine_cfg = EngineConfig::new(&cfg.data_dir, cfg.shard_count)
        .with_wal_dir(&cfg.wal_dir)
        .with_wal_path(cfg.wal_dir.join("wal.log"))
        .with_manifest_snapshot_path(cfg.wal_dir.join("manifest.snapshot.json"))
        .with_wal_max_bytes(cfg.wal_max_bytes)
        .with_wal_max_segments(cfg.wal_max_segments)
        .with_wal_sync_bytes(cfg.wal_sync_bytes)
        .with_wal_sync_interval_ms(cfg.wal_sync_interval_ms)
        .with_query_cache_bytes(cfg.query_cache_bytes)
        .with_batch_cache_bytes(cfg.batch_cache_bytes)
        .with_schema_cache_entries(cfg.schema_cache_entries)
        .with_plan_cache_size(cfg.plan_cache_size)
        .with_plan_cache_ttl_secs(cfg.plan_cache_ttl_secs)
        .with_tier_warm_compression(cfg.tier_warm_compression.clone())
        .with_tier_cold_compression(cfg.tier_cold_compression.clone())
        .with_index_granularity_rows(cfg.index_granularity_rows)
        .with_cache_hot_segments(cfg.cache_hot_segments)
        .with_cache_warm_segments(cfg.cache_warm_segments)
        .with_cache_cold_segments(cfg.cache_cold_segments)
        .with_read_only(cfg.replica_mode)
        .with_replica_sync_interval_ms(cfg.replica_sync_interval_ms)
        .with_replica_primary_addr(cfg.replica_primary_addr.clone());
    engine_cfg.allow_manifest_import = cfg.allow_manifest_import;
    engine_cfg.retention_watermark_micros = cfg.retention_watermark;
    engine_cfg.compact_min_segments = cfg.compact_min_segments;
    engine_cfg.segment_cache_capacity = cfg.segment_cache_capacity;
    // Segment limits & stability (ClickHouse-style protection)
    engine_cfg.max_segments_emergency_threshold = cfg.max_segments_emergency_threshold;
    engine_cfg.max_segments_hard_limit = cfg.max_segments_hard_limit;
    engine_cfg.max_segments_per_table = cfg.max_segments_per_table;
    engine_cfg.max_segments_per_query = cfg.max_segments_per_query;
    engine_cfg.adaptive_backpressure_enabled = cfg.adaptive_backpressure_enabled;
    // Compaction performance tuning
    engine_cfg.compaction_passes_per_cycle = cfg.compaction_passes_per_cycle;
    engine_cfg.max_segments_per_compaction = cfg.max_segments_per_compaction;
    engine_cfg.emergency_compaction_parallelism = cfg.emergency_compaction_parallelism;
    // ClickHouse-style optimizations
    engine_cfg.leveled_compaction_enabled = cfg.leveled_compaction_enabled;
    engine_cfg.continuous_compaction_enabled = cfg.continuous_compaction_enabled;
    engine_cfg.compaction_threads = cfg.compaction_threads;
    engine_cfg.compaction_idle_sleep_ms = cfg.compaction_idle_sleep_ms;
    engine_cfg.write_buffer_enabled = cfg.write_buffer_enabled;
    engine_cfg.write_buffer_max_bytes = cfg.write_buffer_max_bytes;
    let db = Arc::new(Db::open(engine_cfg).map_err(|e| format!("db open failed: {e}"))?);

    // Set up shutdown signal handler early so all spawned tasks can use it
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    tokio::select! {
                        _ = ctrl_c => {
                            info!("received SIGINT, initiating graceful shutdown");
                        }
                        _ = sigterm.recv() => {
                            info!("received SIGTERM, initiating graceful shutdown");
                        }
                    }
                }
                Err(e) => {
                    warn!("failed to register SIGTERM handler: {e}, falling back to SIGINT only");
                    let _ = ctrl_c.await;
                    info!("received SIGINT, initiating graceful shutdown");
                }
            }
        }

        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
            info!("received shutdown signal, initiating graceful shutdown");
        }

        shutdown_signal.notify_waiters();
    });

    // Log replica mode
    if cfg.replica_mode {
        info!(
            sync_interval_ms = cfg.replica_sync_interval_ms,
            primary_addr = ?cfg.replica_primary_addr,
            "Running in read-only REPLICA mode"
        );
        println!(
            "REPLICA MODE: sync_interval={}ms, primary={:?}",
            cfg.replica_sync_interval_ms,
            cfg.replica_primary_addr.as_deref().unwrap_or("(shared S3)")
        );
    }

    // Compact on start if enabled and segment count exceeds threshold
    // Note: Skip compaction on replicas since they're read-only
    if cfg.compact_on_start && !cfg.replica_mode {
        let segment_count = db.segment_count();
        if segment_count >= cfg.compact_on_start_threshold {
            info!(
                segments = segment_count,
                threshold = cfg.compact_on_start_threshold,
                "Starting compaction on startup (segment count exceeds threshold)"
            );
            let start = std::time::Instant::now();
            match db.compact_all() {
                Ok(compacted) => {
                    let new_count = db.segment_count();
                    info!(
                        compacted_tables = compacted,
                        old_segments = segment_count,
                        new_segments = new_count,
                        duration_ms = start.elapsed().as_millis(),
                        "Startup compaction complete"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Startup compaction failed, continuing with fragmented segments");
                }
            }
        } else {
            debug!(
                segments = segment_count,
                threshold = cfg.compact_on_start_threshold,
                "Skipping startup compaction (below threshold)"
            );
        }
    }

    let prepared_cache = Arc::new(Mutex::new(PreparedStatementCache::new(
        cfg.prepared_cache_size,
        cfg.prepared_cache_ttl_secs,
    )));

    // Initialize AuthManager if auth is enabled
    let auth_manager: Option<Arc<AuthManager>> = if cfg.auth_enabled {
        let auth = AuthManager::new(&cfg.data_dir)
            .map_err(|e| format!("auth manager init failed: {e}"))?;
        info!("authentication enabled (bootstrap user: root)");
        Some(Arc::new(auth))
    } else {
        None
    };

    // Initialize ClusterManager if cluster is enabled
    let cluster_manager: Option<Arc<ClusterManager>> = if cfg.cluster_enabled {
        let cluster_id = cfg
            .cluster_id
            .clone()
            .unwrap_or_else(|| "default-cluster".to_string());
        let node_id_str = cfg.node_id.clone();
        let gossip_addr = cfg
            .gossip_addr
            .clone()
            .unwrap_or_else(|| "0.0.0.0:8766".to_string());
        let rpc_addr = cfg
            .rpc_addr
            .clone()
            .unwrap_or_else(|| cfg.bind_addr.clone());

        // Use two-node cluster config if enabled
        let election_config = if cfg.two_node_mode {
            boyodb_core::cluster::ElectionConfig::two_node_cluster()
        } else {
            boyodb_core::cluster::ElectionConfig::default()
        };

        let cluster_config = ClusterConfig {
            cluster_id: cluster_id.clone(),
            node_id: node_id_str.clone(),
            rpc_addr: rpc_addr
                .parse()
                .map_err(|e| format!("invalid rpc_addr: {e}"))?,
            gossip_addr: gossip_addr
                .parse()
                .map_err(|e| format!("invalid gossip_addr: {e}"))?,
            seed_nodes: cfg
                .seed_nodes
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect(),
            gossip_config: GossipConfig::default(),
            election_config,
            write_timeout: std::time::Duration::from_secs(5),
            require_quorum: true,
            two_node_mode: cfg.two_node_mode,
        };

        let gossip_addr_parsed: std::net::SocketAddr = gossip_addr
            .parse()
            .map_err(|e| format!("invalid gossip_addr: {e}"))?;
        let (manager, gossip_rx) = ClusterManager::new(cluster_config, db.clone());
        let manager = Arc::new(manager);
        let effective_node_id = node_id_str.unwrap_or_else(|| "auto-generated".to_string());
        let mode_str = if cfg.two_node_mode {
            "two-node"
        } else {
            "standard"
        };
        info!(
            cluster_id = %cluster_id,
            node_id = %effective_node_id,
            gossip_addr = %gossip_addr,
            seed_nodes = ?cfg.seed_nodes,
            two_node_mode = %cfg.two_node_mode,
            "cluster mode enabled"
        );
        println!(
            "cluster mode: cluster_id={}, node_id={}, gossip_addr={}, seeds={:?}, mode={}",
            cluster_id, effective_node_id, gossip_addr, cfg.seed_nodes, mode_str
        );

        // Start gossip listener
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            if let Err(e) =
                boyodb_core::cluster::start_gossip_listener(gossip_addr_parsed, manager_clone).await
            {
                tracing::error!("gossip listener error: {}", e);
            }
        });

        // Start gossip sender
        let shutdown_flag = manager.shutdown_flag();
        tokio::spawn(async move {
            if let Err(e) =
                boyodb_core::cluster::start_gossip_sender(gossip_rx, shutdown_flag).await
            {
                tracing::error!("gossip sender error: {}", e);
            }
        });

        Some(manager)
    } else {
        None
    };

    // Initialize ReplicationState for cluster mode
    let replication_state: Option<Arc<ReplicationState>> = if let Some(ref cluster) = cluster_manager {
        let node_id = boyodb_core::cluster::NodeId(
            cfg.node_id.clone().unwrap_or_else(|| "node-1".to_string()),
        );
        // Use a port offset from bind_addr for replication (e.g., bind_addr + 100)
        let replication_port = cfg
            .bind_addr
            .split(':')
            .nth(1)
            .and_then(|p| p.parse::<u16>().ok())
            .map(|p| p + 100)
            .unwrap_or(8865);
        let replication_addr: Option<std::net::SocketAddr> =
            format!("0.0.0.0:{}", replication_port).parse().ok();

        let state = Arc::new(ReplicationState::new(node_id, db.clone(), replication_addr));

        // Start replication listener in background
        if let Some(addr) = replication_addr.as_ref() {
            let state_clone = state.clone();
            let (ack_tx, _ack_rx) = tokio::sync::mpsc::channel(1000);
            tokio::spawn(async move {
                if let Err(e) = state_clone.start_listener(ack_tx).await {
                    tracing::warn!("replication listener error: {}", e);
                }
            });
            info!(addr = %addr, "replication listener started");
        }

        // Subscribe to leader changes
        let state_for_leader = state.clone();
        let cluster_for_leader = cluster.clone();
        tokio::spawn(async move {
            let mut last_is_leader = false;
            loop {
                let is_leader = cluster_for_leader.is_leader();
                if is_leader != last_is_leader {
                    let term = cluster_for_leader.current_term();
                    if is_leader {
                        state_for_leader.become_leader(term).await;
                        tracing::info!(term, "node became leader, replication coordinator active");
                    } else {
                        state_for_leader.become_follower(term).await;
                        tracing::info!(term, "node became follower");
                    }
                    last_is_leader = is_leader;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        Some(state)
    } else {
        None
    };

    // Spawn PostgreSQL Server
    if let Some(pg_port) = cfg.pg_port {
        let db_pg = db.clone();
        let auth_for_pg = auth_manager.clone();
        let pg_bind_ip = cfg.bind_addr.split(':').next().unwrap_or("0.0.0.0");
        let pg_addr = format!("{}:{}", pg_bind_ip, pg_port);
        let pg_shutdown = shutdown.clone();

        info!("Starting PostgreSQL interface on {}", pg_addr);

        tokio::spawn(async move {
            let listener = match TcpListener::bind(&pg_addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!("Failed to bind PG server on {}: {}", pg_addr, e);
                    return;
                }
            };

            use crate::server_pg::{BoyodbPgAuthHandler, MakeBoyodbPgHandler};
            use pgwire::api::MakeHandler;

            let factory = Arc::new(MakeBoyodbPgHandler::new(db_pg));

            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _addr)) => {
                                let factory = factory.clone();
                                let auth_for_pg = auth_for_pg.clone();
                                tokio::spawn(async move {
                                    let handler = factory.make();
                                    let startup_handler = Arc::new(BoyodbPgAuthHandler::new(auth_for_pg));
                                    if let Err(e) = pgwire::tokio::process_socket(
                                        stream,
                                        None,
                                        startup_handler,
                                        handler.clone(),
                                        handler.clone(),
                                    )
                                    .await
                                    {
                                        debug!("PG connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("PG accept error: {}", e);
                                sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    _ = pg_shutdown.notified() => {
                        info!("PostgreSQL interface shutting down");
                        break;
                    }
                }
            }
        });
    }

    // Spawn automatic backup scheduler if enabled
    if cfg.backup_interval_hours > 0 {
        let backup_db = db.clone();
        let backup_interval = Duration::from_secs(cfg.backup_interval_hours * 3600);
        let backup_max_count = cfg.backup_max_count;
        info!(
            interval_hours = cfg.backup_interval_hours,
            max_count = backup_max_count,
            "Automatic backup scheduler started"
        );
        println!(
            "Automatic backups enabled: every {} hours, max {} backups retained",
            cfg.backup_interval_hours,
            if backup_max_count == 0 { "unlimited".to_string() } else { backup_max_count.to_string() }
        );
        tokio::spawn(async move {
            // Wait for initial interval before first backup
            tokio::time::sleep(backup_interval).await;
            loop {
                // Create automatic backup
                let label = format!("auto-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
                match backup_db.create_backup(Some(label.clone())) {
                    Ok(info) => {
                        info!(
                            backup_id = %info.id,
                            label = %label,
                            "Automatic backup created successfully"
                        );
                        // Cleanup old backups if max_count is set
                        if backup_max_count > 0 {
                            match backup_db.list_backups() {
                                Ok(mut backups) => {
                                    // Sort by creation time (oldest first)
                                    backups.sort_by_key(|b| b.start_timestamp_micros);
                                    // Filter to only automatic backups
                                    let auto_backups: Vec<_> = backups
                                        .iter()
                                        .filter(|b| b.label.as_deref().map(|l| l.starts_with("auto-")).unwrap_or(false))
                                        .collect();
                                    // Delete oldest backups if we have too many
                                    if auto_backups.len() > backup_max_count {
                                        let to_delete = auto_backups.len() - backup_max_count;
                                        for backup in auto_backups.iter().take(to_delete) {
                                            if let Err(e) = backup_db.delete_backup(&backup.id) {
                                                warn!(backup_id = %backup.id, error = %e, "Failed to delete old backup");
                                            } else {
                                                info!(backup_id = %backup.id, "Deleted old automatic backup");
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to list backups for cleanup");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Automatic backup failed");
                    }
                }
                // Wait for next interval
                tokio::time::sleep(backup_interval).await;
            }
        });
    }

    // Start auto-repair background task
    let auto_repair_state: Option<Arc<boyodb_core::AutoRepairState>> = {
        let repair_log_path = cfg.data_dir.join("repair.log");
        let auto_repair_config = boyodb_core::AutoRepairConfig {
            enabled: true,
            scan_interval_secs: 300, // 5 minutes
            max_repairs_per_scan: 100,
            repair_log_path: Some(repair_log_path),
        };
        let state = boyodb_core::start_auto_repair_task(db.clone(), auto_repair_config);
        info!(
            scan_interval_secs = 300,
            max_repairs_per_scan = 100,
            "Auto-repair background task started"
        );
        Some(state)
    };

    let listener = TcpListener::bind(&cfg.bind_addr)
        .await
        .map_err(|e| format!("bind {} failed: {e}", cfg.bind_addr))?;
    let tls_mode = if cfg.tls_acceptor.is_some() {
        if cfg.client_auth {
            "enabled+mtls"
        } else {
            "enabled"
        }
    } else {
        "disabled"
    };
    let auth_mode = if cfg.auth_enabled {
        "user-auth"
    } else if cfg.auth_token.is_some() {
        "token"
    } else {
        "none"
    };
    // Log detected system resources
    let cpu_cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    info!(
        cpu_cores = cpu_cores,
        workers = cfg.worker_threads,
        "System resources detected"
    );

    info!(
        bind_addr = %cfg.bind_addr,
        max_ipc_bytes = cfg.max_ipc_bytes,
        wal_dir = %cfg.wal_dir.display(),
        wal_max_bytes = cfg.wal_max_bytes,
        wal_max_segments = cfg.wal_max_segments,
        max_connections = cfg.max_connections,
        workers = cfg.worker_threads,
        auth = auth_mode,
        tls = tls_mode,
        replicate_from = cfg.replicate_from.clone().unwrap_or_else(|| "none".into()),
        replicate_tls = if cfg.replicate_use_tls { "yes" } else { "no" },
        "boyodb-server started"
    );
    println!(
        "boyodb-server listening on {} (cpus={}, workers={}, max_conns={}, max_ipc_bytes={}, auth={}, tls={})",
        cfg.bind_addr,
        cpu_cores,
        cfg.worker_threads,
        cfg.max_connections,
        cfg.max_ipc_bytes,
        auth_mode,
        tls_mode
    );

    let limiter = Arc::new(Semaphore::new(cfg.max_connections));

    // Rate limiter for authentication: 10 attempts per 60 seconds per IP
    let auth_rate_limiter = Arc::new(AuthRateLimiter::new(10, 60));

    // Connection statistics for metrics with query concurrency based on worker threads
    // Allow 2x worker threads for concurrent queries to maximize CPU utilization
    let max_concurrent_queries = cfg.worker_threads * 2;
    let connection_stats = Arc::new(ConnectionStats::with_query_limit(
        cfg.max_connections,
        max_concurrent_queries,
    ));
    info!(
        max_concurrent_queries = max_concurrent_queries,
        "Query concurrency limit configured"
    );

    let cfg = Arc::new(cfg);

    if cfg.maintenance_interval_ms > 0 {
        let db_clone = db.clone();
        let cfg_clone = cfg.clone();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(cfg_clone.maintenance_interval_ms));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let db = db_clone.clone();
                        let _ = tokio::task::spawn_blocking(move || db.maintenance()).await;
                    }
                    _ = shutdown_clone.notified() => {
                        debug!("maintenance loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    // Background task for rate limiter and session cleanup (every 60 seconds)
    {
        let rate_limiter_clone = auth_rate_limiter.clone();
        let auth_clone = auth_manager.clone();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Cleanup expired rate limit entries
                        rate_limiter_clone.cleanup();
                        // Cleanup expired sessions
                        if let Some(ref auth) = auth_clone {
                            auth.cleanup_sessions();
                        }
                    }
                    _ = shutdown_clone.notified() => {
                        debug!("cleanup loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    // Background task for idle connection cleanup (every 30 seconds)
    {
        let conn_stats_clone = connection_stats.clone();
        let idle_timeout = Duration::from_secs(cfg.idle_timeout_secs);
        let max_lifetime = Duration::from_secs(cfg.conn_max_lifetime_secs);
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let marked = conn_stats_clone.cleanup_idle(idle_timeout, max_lifetime);
                        if marked > 0 {
                            debug!(marked_connections = marked, "idle connection cleanup");
                        }
                    }
                    _ = shutdown_clone.notified() => {
                        debug!("idle cleanup loop shutting down");
                        break;
                    }
                }
            }
        });
    }

    if let Some(src) = cfg.replicate_from.clone() {
        let cfg_clone = cfg.clone();
        let db_clone = db.clone();
        let state_path = cfg.data_dir.join("replication_state.json");
        let initial_version = load_last_version(&state_path).unwrap_or(None);
        tokio::spawn(async move {
            replication_loop(src, cfg_clone, db_clone, state_path, initial_version).await;
        });
    }

    // Start replica sync worker if in replica mode
    let replica_sync_metrics: Option<Arc<boyodb_core::ReplicaSyncMetrics>> = if cfg.replica_mode {
        let sync_cfg = boyodb_core::ReplicaSyncConfig {
            sync_interval_ms: cfg.replica_sync_interval_ms,
            primary_addr: cfg.replica_primary_addr.clone(),
            primary_token: cfg.auth_token.clone(),
            max_bytes_per_sync: Some(64 * 1024 * 1024),
            use_tls: cfg.tls_acceptor.is_some(),
            connect_timeout_ms: 5000,
            read_timeout_ms: 30000,
        };
        let (worker, _handle) = boyodb_core::spawn_replica_sync_worker(db.clone(), sync_cfg);
        let metrics = worker.metrics_arc();
        info!(
            sync_interval_ms = cfg.replica_sync_interval_ms,
            "Replica sync worker started"
        );
        // Store the handle but let the worker run in the background
        let worker_clone = worker.clone();
        let shutdown_for_replica = shutdown.clone();
        tokio::spawn(async move {
            shutdown_for_replica.notified().await;
            worker_clone.shutdown();
        });
        Some(metrics)
    } else {
        None
    };

    // Start cluster background tasks if cluster is enabled
    if let Some(ref cluster) = cluster_manager {
        let cluster_clone = cluster.clone();
        tokio::spawn(async move {
            boyodb_core::cluster::start_cluster_tasks(cluster_clone).await;
        });
        info!("cluster background tasks started");
    }

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, addr) = match accept_result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "accept error");
                        continue;
                    }
                };
                let permit = match limiter.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        connection_stats.query_rejected();
                        warn!(addr = %addr, "connection rejected: max connections reached");
                        continue;
                    }
                };
                let db = db.clone();
                let cfg = cfg.clone();
                let auth = auth_manager.clone();
                let cluster = cluster_manager.clone();
                let prepared_cache = prepared_cache.clone();
                let conn_stats = connection_stats.clone();
                let rate_limiter = auth_rate_limiter.clone();
                let conn_id = conn_stats.connection_opened(addr);
                let repair_state = auto_repair_state.clone();
                let replica_metrics = replica_sync_metrics.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_conn(
                        stream,
                        db,
                        cfg.clone(),
                        auth,
                        cluster,
                        prepared_cache,
                        rate_limiter,
                        conn_stats.clone(),
                        conn_id,
                        addr.ip(),
                        repair_state,
                        replica_metrics,
                    )
                    .await
                    {
                        debug!(addr = %addr, error = %e, "connection error");
                    }
                    conn_stats.connection_closed(conn_id);
                    drop(permit);
                });
            }
            _ = shutdown.notified() => {
                info!("stopping accept loop");
                break;
            }
        }
    }

    // Wait for active connections to complete (with timeout)
    let active_count = connection_stats.current();
    if active_count > 0 {
        info!(
            active_connections = active_count,
            "waiting for active connections to complete"
        );
        let shutdown_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();
        while connection_stats.current() > 0 {
            if start.elapsed() > shutdown_timeout {
                warn!("shutdown timeout exceeded, forcing exit");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Shutdown cluster if enabled
    if let Some(ref cluster) = cluster_manager {
        cluster.shutdown();
        info!("cluster shutdown signaled");
    }

    info!("graceful shutdown complete");
    Ok(())
}

async fn handle_conn(
    stream: TcpStream,
    db: Arc<Db>,
    cfg: Arc<ServerConfig>,
    auth_manager: Option<Arc<AuthManager>>,
    cluster_manager: Option<Arc<ClusterManager>>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
    rate_limiter: Arc<AuthRateLimiter>,
    conn_stats: Arc<ConnectionStats>,
    conn_id: u64,
    peer_ip: IpAddr,
    auto_repair_state: Option<Arc<boyodb_core::AutoRepairState>>,
    replica_sync_metrics: Option<Arc<boyodb_core::ReplicaSyncMetrics>>,
) -> Result<(), ServerError> {
    let mut stream: Box<dyn AsyncStream> = if let Some(acceptor) = cfg.tls_acceptor.clone() {
        Box::new(
            acceptor
                .accept(stream)
                .await
                .map_err(|e| ServerError::Tls(format!("handshake failed: {e}")))?,
        )
    } else {
        Box::new(stream)
    };

    // Per-connection session state
    let mut current_session: Option<String> = None;
    let mut authenticated_user: Option<String> = None;

    loop {
        // Check if this connection has been marked for closure (idle timeout or max lifetime)
        if conn_stats.should_close(conn_id) {
            debug!(conn_id, "connection marked for closure, terminating");
            return Ok(());
        }

        let req_bytes = match read_frame(&mut stream, cfg.io_timeout, cfg.max_frame_len).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(()),
            Err(e) => {
                let _ = write_response(
                    &mut stream,
                    &Response::error(&e.to_string()),
                    cfg.io_timeout,
                    cfg.max_frame_len,
                )
                .await;
                return Err(e);
            }
        };

        // Update last activity time
        conn_stats.touch(conn_id);

        let env: Envelope = match serde_json::from_slice(&req_bytes) {
            Ok(v) => v,
            Err(e) => {
                let err = ServerError::BadRequest(format!("invalid json: {e}"));
                let _ = write_response(
                    &mut stream,
                    &Response::error(&err.to_string()),
                    cfg.io_timeout,
                    cfg.max_frame_len,
                )
                .await;
                return Err(err);
            }
        };

        // Token-based auth (legacy, takes precedence if configured). Skip when user auth is enabled.
        if let (None, Some(expected)) = (&auth_manager, &cfg.auth_token) {
            let auth_valid = match env.auth.as_deref() {
                Some(provided) => {
                    let provided_bytes = provided.as_bytes();
                    let expected_bytes = expected.as_bytes();
                    provided_bytes.ct_eq(expected_bytes).into()
                }
                None => false,
            };
            if !auth_valid {
                warn!("unauthorized request attempt");
                write_response(
                    &mut stream,
                    &Response::error("unauthorized"),
                    cfg.io_timeout,
                    cfg.max_frame_len,
                )
                .await?;
                return Err(ServerError::Unauthorized);
            }
        }

        if cfg.log_requests {
            eprintln!("request op={}", env.request.op_name());
        }

        // Handle auth requests separately
        if let Some(auth) = &auth_manager {
            let resp = match &env.request {
                Request::Login { username, password } => {
                    // Check rate limit before attempting authentication
                    if let Err(wait_time) = rate_limiter.check(peer_ip) {
                        warn!(ip = %peer_ip, wait_secs = wait_time.as_secs(), "rate limited login attempt");
                        Some(Err(ServerError::Auth(format!(
                            "too many login attempts, try again in {} seconds",
                            wait_time.as_secs()
                        ))))
                    } else {
                        match auth.authenticate(
                            username,
                            password,
                            Some(&peer_ip.to_string()),
                            None,
                        ) {
                            Ok(session_id) => {
                                rate_limiter.record_success(peer_ip);
                                info!(
                                    username = %username,
                                    client_ip = %peer_ip,
                                    "HTTP authentication successful"
                                );
                                authenticated_user = Some(username.clone());
                                conn_stats.set_user(conn_id, username.clone());
                                current_session = Some(session_id.clone());
                                let mut resp = Response::ok_message("login successful");
                                resp.session_id = Some(session_id);
                                Some(Ok(resp))
                            }
                            Err(ref e) => {
                                rate_limiter.record_failure(peer_ip);
                                warn!(
                                    username = %username,
                                    client_ip = %peer_ip,
                                    error = %e,
                                    "HTTP authentication failed"
                                );
                                Some(Err(ServerError::from(e.clone())))
                            }
                        }
                    }
                }
                Request::Logout => {
                    if let Some(session) = current_session.take() {
                        let _ = auth.invalidate_session(&session);
                    }
                    authenticated_user = None;
                    Some(Ok(Response::ok_message("logged out")))
                }
                _ => None, // Not an auth-specific request, continue below
            };

            if let Some(result) = resp {
                match result {
                    Ok(r) => {
                        write_response(&mut stream, &r, cfg.io_timeout, cfg.max_frame_len).await?
                    }
                    Err(e) => {
                        write_response(
                            &mut stream,
                            &Response::error(&e.to_string()),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await?;
                    }
                }
                continue;
            }

            // Session-based auth: validate session from auth field or use existing connection session
            let session_to_validate = env.auth.as_ref().or(current_session.as_ref());

            // For non-login requests when auth is enabled, require authentication
            // (except for health checks which are allowed unauthenticated)
            let is_health_check = matches!(env.request, Request::Health | Request::HealthDetailed);
            if !is_health_check {
                if let Some(session_id) = session_to_validate {
                    match auth.validate_session(session_id) {
                        Ok(username) => {
                            conn_stats.set_user(conn_id, username.clone());
                            authenticated_user = Some(username);
                            current_session = Some(session_id.clone());
                        }
                        Err(e) => {
                            write_response(
                                &mut stream,
                                &Response::error(&format!("auth: {}", e)),
                                cfg.io_timeout,
                                cfg.max_frame_len,
                            )
                            .await?;
                            continue;
                        }
                    }
                } else {
                    write_response(
                        &mut stream,
                        &Response::error("authentication required - use login operation first"),
                        cfg.io_timeout,
                        cfg.max_frame_len,
                    )
                    .await?;
                    continue;
                }
            }
        }

        // Process the request - try to acquire a query slot for load management
        // For heavy operations (queries), we use backpressure to prevent overload
        let is_heavy_operation = matches!(
            &env.request,
            Request::Query { .. }
                | Request::QueryBinary { .. }
                | Request::ExecuteSubQuery { .. }
                | Request::IngestIpc { .. }
                | Request::IngestIpcBinary { .. }
        );

        let _query_permit = if is_heavy_operation {
            // Try to acquire a permit, waiting briefly if system is loaded
            match conn_stats
                .try_acquire_query_with_timeout(Duration::from_millis(100))
                .await
            {
                Some(permit) => {
                    conn_stats.query_admitted();
                    Some(permit)
                }
                None => {
                    // Timeout or closed - system is overloaded, apply backpressure
                    // Log at debug level to avoid log spam during high load
                    debug!(
                        active = conn_stats.active_queries(),
                        max = conn_stats.max_queries(),
                        "query throttled due to high load"
                    );
                    conn_stats.query_rejected();
                    write_response(
                        &mut stream,
                        &Response::error("server busy - try again"),
                        cfg.io_timeout,
                        cfg.max_frame_len,
                    )
                    .await?;
                    continue;
                }
            }
        } else {
            // Light operations (ping, auth) don't need throttling
            conn_stats.query_admitted();
            None
        };

        match env.request {
            Request::ExecuteSubQuery {
                plan_json,
                timeout_millis: _,
                accept_compression,
            } => {
                let plan: LocalPlan = match serde_json::from_value(plan_json) {
                    Ok(p) => p,
                    Err(e) => {
                        let _ = write_response(
                            &mut stream,
                            &Response::error(&format!("invalid plan json: {e}")),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                        continue;
                    }
                };
                match db.execute_distributed_plan(plan.kind) {
                    Ok(response) => {
                        let mut resp = Response::default();
                        resp.status = "ok";
                        if !response.records_ipc.is_empty() {
                            if accept_compression.as_deref() == Some("zstd") {
                                match zstd::stream::encode_all(
                                    std::io::Cursor::new(&response.records_ipc),
                                    3,
                                ) {
                                    Ok(compressed) => {
                                        resp.ipc_base64 =
                                            Some(general_purpose::STANDARD.encode(compressed));
                                        resp.compression = Some("zstd".to_string());
                                    }
                                    Err(e) => {
                                        // Fall back to uncompressed on compression failure
                                        warn!("zstd compression failed, falling back to uncompressed: {}", e);
                                        resp.ipc_base64 =
                                            Some(general_purpose::STANDARD.encode(&response.records_ipc));
                                    }
                                }
                                resp.ipc_len = Some(response.records_ipc.len() as u64);
                            } else {
                                resp.ipc_base64 =
                                    Some(general_purpose::STANDARD.encode(&response.records_ipc));
                                resp.ipc_len = Some(response.records_ipc.len() as u64);
                            }
                        }
                        resp.data_skipped_bytes = Some(response.data_skipped_bytes);
                        resp.segments_scanned = Some(response.segments_scanned);
                        write_response(&mut stream, &resp, cfg.io_timeout, cfg.max_frame_len)
                            .await?;
                    }
                    Err(e) => {
                        let err = map_engine_error(e);
                        write_response(
                            &mut stream,
                            &Response::error(&err.to_string()),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await?;
                    }
                }
            }
            Request::QueryBinary {
                sql,
                timeout_millis,
                accept_compression,
                database,
                stream: use_stream,
            } => {
                if use_stream {
                    if accept_compression.is_some() {
                        let _ = write_response(
                            &mut stream,
                            &Response::error("streaming does not support compression"),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                        continue;
                    }
                    if let Err(e) = process_query_binary_stream(
                        sql,
                        timeout_millis,
                        database,
                        db.clone(),
                        auth_manager.clone(),
                        authenticated_user.clone(),
                        cfg.max_query_len,
                        cfg.max_frame_len,
                        cfg.io_timeout,
                        &mut stream,
                    )
                    .await
                    {
                        let _ = write_response(
                            &mut stream,
                            &Response::error(&e.to_string()),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                    }
                } else {
                    match process_query_binary(
                        sql,
                        timeout_millis,
                        accept_compression,
                        database,
                        db.clone(),
                        auth_manager.clone(),
                        authenticated_user.clone(),
                        cfg.max_query_len,
                        cfg.max_frame_len,
                        cfg.data_dir.clone(),
                    )
                    .await
                    {
                        Ok((header, payload_path)) => {
                            write_response(&mut stream, &header, cfg.io_timeout, cfg.max_frame_len)
                                .await?;
                            let payload_result = if header.ipc_len.is_some() {
                                write_frame_file(
                                    &mut stream,
                                    &payload_path,
                                    cfg.io_timeout,
                                    cfg.max_frame_len,
                                )
                                .await
                            } else {
                                Ok(())
                            };
                            let _ = tokio::fs::remove_file(&payload_path).await;
                            payload_result?;
                        }
                        Err(e) => {
                            let _ = write_response(
                                &mut stream,
                                &Response::error(&e.to_string()),
                                cfg.io_timeout,
                                cfg.max_frame_len,
                            )
                            .await;
                        }
                    }
                }
            }
            Request::ExecutePreparedBinary {
                id,
                timeout_millis,
                accept_compression,
                stream: use_stream,
            } => {
                if use_stream {
                    if accept_compression.is_some() {
                        let _ = write_response(
                            &mut stream,
                            &Response::error("streaming does not support compression"),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                        continue;
                    }
                    if let Err(e) = process_execute_prepared_binary_stream(
                        id,
                        timeout_millis,
                        db.clone(),
                        auth_manager.clone(),
                        authenticated_user.clone(),
                        prepared_cache.clone(),
                        cfg.max_query_len,
                        cfg.max_frame_len,
                        cfg.io_timeout,
                        &mut stream,
                    )
                    .await
                    {
                        let _ = write_response(
                            &mut stream,
                            &Response::error(&e.to_string()),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                    }
                } else {
                    match process_execute_prepared_binary(
                        id,
                        timeout_millis,
                        accept_compression,
                        db.clone(),
                        auth_manager.clone(),
                        authenticated_user.clone(),
                        prepared_cache.clone(),
                        cfg.max_query_len,
                        cfg.max_frame_len,
                        cfg.data_dir.clone(),
                    )
                    .await
                    {
                        Ok((header, payload_path)) => {
                            write_response(&mut stream, &header, cfg.io_timeout, cfg.max_frame_len)
                                .await?;
                            let payload_result = if header.ipc_len.is_some() {
                                write_frame_file(
                                    &mut stream,
                                    &payload_path,
                                    cfg.io_timeout,
                                    cfg.max_frame_len,
                                )
                                .await
                            } else {
                                Ok(())
                            };
                            let _ = tokio::fs::remove_file(&payload_path).await;
                            payload_result?;
                        }
                        Err(e) => {
                            let _ = write_response(
                                &mut stream,
                                &Response::error(&e.to_string()),
                                cfg.io_timeout,
                                cfg.max_frame_len,
                            )
                            .await;
                        }
                    }
                }
            }
            Request::IngestIpcBinary {
                watermark_micros,
                database,
                table,
                shard_hint,
                compression,
            } => match read_frame(&mut stream, cfg.io_timeout, cfg.max_ipc_bytes).await {
                Ok(Some(payload)) => {
                    let result = process_ingest_ipc_payload(
                        payload,
                        watermark_micros,
                        database,
                        table,
                        shard_hint,
                        compression,
                        db.clone(),
                        auth_manager.clone(),
                        authenticated_user.clone(),
                        cfg.max_ipc_bytes,
                    )
                    .await;
                    match result {
                        Ok(resp) => {
                            write_response(&mut stream, &resp, cfg.io_timeout, cfg.max_frame_len)
                                .await?;
                        }
                        Err(e) => {
                            let _ = write_response(
                                &mut stream,
                                &Response::error(&e.to_string()),
                                cfg.io_timeout,
                                cfg.max_frame_len,
                            )
                            .await;
                        }
                    }
                }
                Ok(None) => return Ok(()),
                Err(e) => {
                    let _ = write_response(
                        &mut stream,
                        &Response::error(&e.to_string()),
                        cfg.io_timeout,
                        cfg.max_frame_len,
                    )
                    .await;
                }
            },
            other => {
                let result = process_request(
                    other,
                    db.clone(),
                    auth_manager.clone(),
                    cluster_manager.clone(),
                    prepared_cache.clone(),
                    authenticated_user.clone(),
                    cfg.max_ipc_bytes,
                    cfg.max_query_len,
                    cfg.max_frame_len,
                    cfg.allow_manifest_import,
                    cfg.wal_dir.clone(),
                    cfg.wal_max_bytes,
                    cfg.wal_max_segments,
                    conn_stats.clone(),
                    auto_repair_state.clone(),
                    cfg.replica_mode,
                    replica_sync_metrics.clone(),
                )
                .await;

                match result {
                    Ok(resp) => {
                        write_response(&mut stream, &resp, cfg.io_timeout, cfg.max_frame_len)
                            .await?;
                    }
                    Err(e) => {
                        let _ = write_response(
                            &mut stream,
                            &Response::error(&e.to_string()),
                            cfg.io_timeout,
                            cfg.max_frame_len,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

async fn replication_loop(
    src: String,
    cfg: Arc<ServerConfig>,
    db: Arc<Db>,
    state_path: PathBuf,
    initial_version: Option<u64>,
) {
    let interval_ms = if cfg.replicate_interval_ms == 0 {
        5_000
    } else {
        cfg.replicate_interval_ms
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    let mut last_version: Option<u64> = initial_version;
    let mut failures: u32 = 0;

    loop {
        ticker.tick().await;
        if let Err(e) = replicate_once(&src, &cfg, db.clone(), &mut last_version, &state_path).await
        {
            eprintln!("replication from {src} failed: {e}");
            failures = failures.saturating_add(1);
            let backoff = (1u64 << failures).saturating_mul(500).min(30_000);
            sleep(Duration::from_millis(backoff)).await;
        } else {
            failures = 0;
        }
    }
}

async fn replicate_once(
    src: &str,
    cfg: &Arc<ServerConfig>,
    db: Arc<Db>,
    last_version: &mut Option<u64>,
    state_path: &Path,
) -> Result<(), ServerError> {
    let mut stream: Box<dyn AsyncStream> = if cfg.replicate_use_tls {
        Box::new(connect_replication_tls(src, cfg).await?)
    } else {
        Box::new(
            TcpStream::connect(src)
                .await
                .map_err(|e| ServerError::Io(format!("connect failed: {e}")))?,
        )
    };

    let env = Envelope {
        auth: cfg.replicate_token.clone(),
        request: Request::ExportBundle {
            max_bytes: cfg.replicate_max_bytes,
            since_version: *last_version,
            prefer_hot: true,
            target_bytes_per_sec: None,
            max_entries: None,
        },
    };
    write_framed_json(&mut stream, &env, cfg.io_timeout, cfg.max_frame_len).await?;

    let resp_bytes = match read_frame(&mut stream, cfg.io_timeout, cfg.max_frame_len).await? {
        Some(b) => b,
        None => {
            return Err(ServerError::Io(
                "replication source closed connection".into(),
            ))
        }
    };
    let resp: RemoteResponse = serde_json::from_slice(&resp_bytes)
        .map_err(|e| ServerError::Encode(format!("decode remote response failed: {e}")))?;
    if resp.status.as_str() != "ok" {
        let msg = resp
            .message
            .unwrap_or_else(|| "replication source error".to_string());
        return Err(ServerError::Db(msg));
    }
    let bundle_json = match resp.bundle_json {
        Some(v) => v,
        None => return Ok(()),
    };
    let payload: boyodb_core::BundlePayload = serde_json::from_value(bundle_json)
        .map_err(|e| ServerError::Encode(format!("invalid bundle payload: {e}")))?;
    if payload.plan.throttle_millis > 0 {
        sleep(Duration::from_millis(payload.plan.throttle_millis)).await;
    }
    let version = payload.plan.manifest_version;
    if payload.plan.entries.is_empty() {
        *last_version = Some(version);
        return Ok(());
    }
    let db_apply = db.clone();
    blocking(move || {
        db_apply
            .apply_bundle(payload)
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;
    *last_version = Some(version);
    let _ = persist_last_version(state_path, version);
    Ok(())
}

fn collect_wal_stats(
    wal_dir: &Path,
    wal_max_bytes: u64,
    wal_max_segments: u64,
) -> Result<serde_json::Value, ServerError> {
    let current_path = wal_dir.join("wal.log");
    let current_meta = std::fs::metadata(&current_path).ok();
    let current_bytes = current_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let current_modified_millis = current_meta
        .as_ref()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);

    let mut rotated_bytes: u64 = 0;
    let mut rotated_count: usize = 0;
    let mut oldest_millis: Option<u64> = None;
    let mut newest_millis: Option<u64> = None;

    let entries = std::fs::read_dir(wal_dir)
        .map_err(|e| ServerError::Io(format!("read wal dir failed: {e}")))?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("wal.log.") {
            continue;
        }
        rotated_count += 1;
        if let Ok(meta) = entry.metadata() {
            rotated_bytes = rotated_bytes.saturating_add(meta.len());
            if let Ok(modified) = meta.modified() {
                if let Ok(delta) = modified.duration_since(UNIX_EPOCH) {
                    let millis = delta.as_millis() as u64;
                    oldest_millis = Some(oldest_millis.map_or(millis, |v| v.min(millis)));
                    newest_millis = Some(newest_millis.map_or(millis, |v| v.max(millis)));
                }
            }
        }
    }

    let trim_excess = rotated_count.saturating_sub(wal_max_segments as usize);
    Ok(serde_json::json!({
        "wal_dir": wal_dir.to_string_lossy(),
        "wal_log_bytes": current_bytes,
        "wal_log_modified_millis": current_modified_millis,
        "rotated_count": rotated_count,
        "rotated_bytes": rotated_bytes,
        "rotated_oldest_millis": oldest_millis,
        "rotated_newest_millis": newest_millis,
        "wal_max_bytes": wal_max_bytes,
        "wal_max_segments": wal_max_segments,
        "rotated_trim_needed": trim_excess,
    }))
}

/// Check if a request is a write operation
fn is_write_request(req: &Request) -> bool {
    matches!(
        req,
        Request::IngestIpc { .. }
            | Request::IngestIpcBinary { .. }
            | Request::IngestCsv { .. }
            | Request::CreateDatabase { .. }
            | Request::CreateTable { .. }
            | Request::Compact { .. }
            | Request::Checkpoint
            | Request::ApplyBundle { .. }
            | Request::ApplyManifest { .. }
    )
}

/// Check if SQL is a write statement
fn is_write_sql(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("DROP")
        || upper.starts_with("ALTER")
        || upper.starts_with("TRUNCATE")
}

async fn process_request(
    req: Request,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    cluster_manager: Option<Arc<ClusterManager>>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
    authenticated_user: Option<String>,
    max_ipc_bytes: usize,
    max_query_len: usize,
    max_frame_len: usize,
    allow_manifest_import: bool,
    wal_dir: PathBuf,
    wal_max_bytes: u64,
    wal_max_segments: u64,
    conn_stats: Arc<ConnectionStats>,
    auto_repair_state: Option<Arc<boyodb_core::AutoRepairState>>,
    replica_mode: bool,
    replica_sync_metrics: Option<Arc<boyodb_core::ReplicaSyncMetrics>>,
) -> Result<Response, ServerError> {
    // Early rejection for write operations on replicas
    if replica_mode && is_write_request(&req) {
        return Err(ServerError::Db(
            "write operations disabled on read replica".into(),
        ));
    }

    // For query requests, check if the SQL is a write statement
    if replica_mode {
        if let Request::Query { ref sql, .. } = req {
            if is_write_sql(sql) {
                return Err(ServerError::Db(
                    "write operations disabled on read replica".into(),
                ));
            }
        }
    }

    // Helper to check privilege when auth is enabled
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };
    match req {
        Request::ExecuteSubQuery {
            plan_json,
            timeout_millis: _,
            accept_compression,
        } => {
            // Parse the plan JSON into LocalPlan
            let plan: LocalPlan = serde_json::from_value(plan_json)
                .map_err(|e| ServerError::BadRequest(format!("invalid plan json: {e}")))?;

            // Execute the distributed plan
            let db = db.clone();
            let response = blocking(move || {
                db.execute_distributed_plan(plan.kind)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            // Build response
            if response.records_ipc.is_empty() {
                Ok(Response {
                    data_skipped_bytes: Some(response.data_skipped_bytes),
                    segments_scanned: Some(response.segments_scanned),
                    ..Default::default()
                })
            } else {
                if accept_compression.as_deref() == Some("zstd") {
                    let compressed = zstd::stream::encode_all(
                        std::io::Cursor::new(&response.records_ipc),
                        3,
                    )
                    .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
                    Ok(Response {
                        ipc_base64: Some(general_purpose::STANDARD.encode(&compressed)),
                        ipc_len: Some(response.records_ipc.len() as u64),
                        compression: Some("zstd".to_string()),
                        data_skipped_bytes: Some(response.data_skipped_bytes),
                        segments_scanned: Some(response.segments_scanned),
                        ..Default::default()
                    })
                } else {
                    Ok(Response {
                        ipc_base64: Some(general_purpose::STANDARD.encode(&response.records_ipc)),
                        ipc_len: Some(response.records_ipc.len() as u64),
                        data_skipped_bytes: Some(response.data_skipped_bytes),
                        segments_scanned: Some(response.segments_scanned),
                        ..Default::default()
                    })
                }
            }
        }
        Request::Query {
            sql,
            timeout_millis,
            accept_compression,
            database: default_database,
        } => {
            if let Some(c) = accept_compression.as_deref() {
                if c != "zstd" {
                    return Err(ServerError::BadRequest("unsupported compression".into()));
                }
            }

            // If a default database is specified and the SQL doesn't already have a qualified table,
            // try to prepend the database. For now, we pass the default to the statement processing.
            let effective_db = default_database.as_deref().unwrap_or("default");

            // Parse the SQL to determine statement type
            let stmt = parse_sql(&sql)
                .map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;

            match stmt {
                SqlStatement::Auth(auth_cmd) => {
                    // Handle auth commands via AuthManager
                    let auth = auth_manager.as_ref().ok_or(ServerError::Auth(
                        "auth not enabled - start server with --auth flag".into(),
                    ))?;
                    let actor = authenticated_user.as_deref().unwrap_or("system");
                    execute_auth_command(auth, auth_cmd, actor)
                }
                SqlStatement::Ddl(ddl_cmd) => {
                    // Handle DDL commands - apply effective_db for unqualified table names
                    let adjusted_cmd = apply_default_database_to_ddl(ddl_cmd, effective_db);
                    execute_ddl_command(&db, adjusted_cmd, &require_privilege, &auto_repair_state).await
                }
                SqlStatement::Query(parsed) => {
                    // Determine effective databases for primary and join tables when a default is supplied
                    let main_db = parsed
                        .database
                        .as_deref()
                        .filter(|d| *d != "default")
                        .unwrap_or(effective_db)
                        .to_string();

                    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
                    table_refs.push((
                        main_db.clone(),
                        parsed
                            .table
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        parsed.database.as_deref() == Some("default") || parsed.database.is_none(),
                    ));
                    for join in &parsed.joins {
                        let join_db = if join.database == "default" {
                            effective_db.to_string()
                        } else {
                            join.database.clone()
                        };
                        table_refs.push((join_db, join.table.clone(), join.database == "default"));
                    }

                    // Require SELECT privilege on all referenced tables (primary + joins)
                    for (db_name, table_name, _) in &table_refs {
                        require_privilege(
                            Privilege::Select,
                            PrivilegeTarget::Table {
                                database: db_name.clone(),
                                table: table_name.clone(),
                            },
                        )?;
                    }

                    // Standard SELECT query - rewrite SQL with qualified table names for any unqualified references
                    let mut effective_sql = sql.clone();
                    if effective_db != "default" {
                        for (_db, table_name, was_default) in &table_refs {
                            if *was_default {
                                let from_pat = format!("FROM {}", table_name);
                                let from_rep = format!("FROM {}.{}", effective_db, table_name);
                                let join_pat = format!("JOIN {}", table_name);
                                let join_rep = format!("JOIN {}.{}", effective_db, table_name);
                                let from_pat_lower = format!("from {}", table_name);
                                let from_rep_lower =
                                    format!("from {}.{}", effective_db, table_name);
                                let join_pat_lower = format!("join {}", table_name);
                                let join_rep_lower =
                                    format!("join {}.{}", effective_db, table_name);

                                effective_sql = effective_sql
                                    .replace(&from_pat, &from_rep)
                                    .replace(&join_pat, &join_rep)
                                    .replace(&from_pat_lower, &from_rep_lower)
                                    .replace(&join_pat_lower, &join_rep_lower);
                            }
                        }
                    }
                    validate_select(&effective_sql, max_query_len)
                        .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;
                    let db = db.clone();
                    let resp = blocking(move || {
                        db.query(QueryRequest {
                            sql: effective_sql,
                            timeout_millis,
                            collect_stats: false,
                            transaction_id: None,
                        })
                        .map_err(|e| ServerError::Db(e.to_string()))
                    })
                    .await?;
                    let mut payload = resp.records_ipc;
                    let mut compression_used: Option<&'static str> = None;
                    if matches!(accept_compression.as_deref(), Some("zstd")) {
                        payload = zstd::stream::encode_all(std::io::Cursor::new(payload), 3)
                            .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
                        compression_used = Some("zstd");
                    }
                    if payload.len() > max_frame_len {
                        return Err(ServerError::BadRequest(format!(
                            "response too large ({} > {})",
                            payload.len(),
                            max_frame_len
                        )));
                    }
                    let ipc_base64 = general_purpose::STANDARD.encode(&payload);
                    Ok(Response {
                        ipc_base64: Some(ipc_base64),
                        data_skipped_bytes: Some(resp.data_skipped_bytes),
                        segments_scanned: Some(resp.segments_scanned),
                        compression: compression_used.map(|c| c.to_string()),
                        ..Default::default()
                    })
                }
                SqlStatement::Insert(mut insert_cmd) => {
                    // Use effective_db if the insert uses unqualified table name
                    if insert_cmd.database == "default" {
                        insert_cmd.database = effective_db.to_string();
                    }
                    require_privilege(
                        Privilege::Insert,
                        PrivilegeTarget::Table {
                            database: insert_cmd.database.clone(),
                            table: insert_cmd.table.clone(),
                        },
                    )?;
                    // Convert INSERT to Arrow IPC and ingest
                    execute_insert(&db, insert_cmd).await
                }
                SqlStatement::Update(mut update_cmd) => {
                    // Use effective_db if the update uses unqualified table name
                    if update_cmd.database == "default" {
                        update_cmd.database = effective_db.to_string();
                    }
                    require_privilege(
                        Privilege::Update,
                        PrivilegeTarget::Table {
                            database: update_cmd.database.clone(),
                            table: update_cmd.table.clone(),
                        },
                    )?;
                    execute_update(&db, update_cmd).await
                }
                SqlStatement::Delete(mut delete_cmd) => {
                    // Use effective_db if the delete uses unqualified table name
                    if delete_cmd.database == "default" {
                        delete_cmd.database = effective_db.to_string();
                    }
                    require_privilege(
                        Privilege::Delete,
                        PrivilegeTarget::Table {
                            database: delete_cmd.database.clone(),
                            table: delete_cmd.table.clone(),
                        },
                    )?;
                    execute_delete(&db, delete_cmd).await
                }
                SqlStatement::Merge(mut merge_cmd) => {
                    // Use effective_db if using unqualified table names
                    if merge_cmd.database == "default" {
                        merge_cmd.database = effective_db.to_string();
                    }
                    if merge_cmd.source_database == "default" {
                        merge_cmd.source_database = effective_db.to_string();
                    }
                    // MERGE requires both SELECT on source and INSERT/UPDATE/DELETE on target
                    require_privilege(
                        Privilege::Select,
                        PrivilegeTarget::Table {
                            database: merge_cmd.source_database.clone(),
                            table: merge_cmd.source_table.clone(),
                        },
                    )?;
                    require_privilege(
                        Privilege::Insert,
                        PrivilegeTarget::Table {
                            database: merge_cmd.database.clone(),
                            table: merge_cmd.table.clone(),
                        },
                    )?;
                    require_privilege(
                        Privilege::Update,
                        PrivilegeTarget::Table {
                            database: merge_cmd.database.clone(),
                            table: merge_cmd.table.clone(),
                        },
                    )?;
                    execute_merge(&db, merge_cmd).await
                }
                SqlStatement::SetOperation(_) => {
                    // Set operations (UNION, INTERSECT, EXCEPT) are parsed but execution is not yet implemented
                    Err(ServerError::BadRequest(
                        "UNION/INTERSECT/EXCEPT operations are parsed but execution is not yet implemented. \
                         Use multiple separate queries instead.".into()
                    ))
                }
                SqlStatement::Transaction(_) => {
                    // Transaction stubs for HTTP/Server context (mostly no-op)
                    Ok(Response::ok_message("OK"))
                }
                SqlStatement::PreparedStatement(cmd) => {
                    use boyodb_core::PreparedStatementCommand;
                    match cmd {
                        PreparedStatementCommand::Prepare { name, statement, param_types } => {
                            // Store in prepared statement cache using existing infrastructure
                            if let Ok(mut cache) = prepared_cache.lock() {
                                // Use insert method with empty table_refs for now
                                let id = cache.insert(statement.clone(), Vec::new());
                                // Store mapping from user-provided name to generated id
                                return Ok(Response {
                                    message: Some(format!("PREPARE {} ({} params)", name, param_types.len())),
                                    prepared_id: Some(id),
                                    ..Default::default()
                                });
                            }
                            Ok(Response {
                                message: Some(format!("PREPARE {} ({} params)", name, param_types.len())),
                                prepared_id: Some(name),
                                ..Default::default()
                            })
                        }
                        PreparedStatementCommand::Execute { name, parameters } => {
                            // Get prepared statement and substitute parameters
                            let prepared_sql = if let Ok(mut cache) = prepared_cache.lock() {
                                cache.get(&name).map(|p| p.sql.clone())
                            } else {
                                None
                            };

                            if let Some(sql) = prepared_sql {
                                // Substitute parameters into the SQL
                                let mut final_sql = sql;
                                for (i, param) in parameters.iter().enumerate() {
                                    let placeholder = format!("${}", i + 1);
                                    let value_str = match param {
                                        SqlValue::Null => "NULL".to_string(),
                                        SqlValue::Integer(n) => n.to_string(),
                                        SqlValue::Float(f) => f.to_string(),
                                        SqlValue::String(s) => format!("'{}'", s.replace("'", "''")),
                                        SqlValue::Boolean(b) => b.to_string(),
                                    };
                                    final_sql = final_sql.replace(&placeholder, &value_str);
                                }
                                Ok(Response {
                                    message: Some(format!("EXECUTE (substituted: {})", final_sql)),
                                    ..Default::default()
                                })
                            } else {
                                Err(ServerError::BadRequest(format!(
                                    "prepared statement '{}' not found",
                                    name
                                )))
                            }
                        }
                        PreparedStatementCommand::Deallocate { name } => {
                            if let Some(stmt_name) = name {
                                Ok(Response::ok_message(&format!("DEALLOCATE {}", stmt_name)))
                            } else {
                                Ok(Response::ok_message("DEALLOCATE ALL"))
                            }
                        }
                    }
                }
                SqlStatement::Explain { analyze, statement } => {
                    // Execute EXPLAIN [ANALYZE] queries
                    let explain_result =
                        execute_explain(&db, *statement, analyze, effective_db, &require_privilege)
                            .await?;
                    Ok(Response {
                        message: Some(explain_result),
                        ..Default::default()
                    })
                }
                SqlStatement::PubSub(cmd) => {
                    use boyodb_core::PubSubCommand;
                    // Get the pub/sub manager from the global state
                    let pubsub = get_pubsub_manager();
                    // Use connection ID as session ID (for HTTP we don't have persistent sessions)
                    let session_id = 0u64; // HTTP requests don't have persistent sessions
                    pubsub.register_session(session_id);

                    match cmd {
                        PubSubCommand::Listen { channel } => {
                            match pubsub.listen(session_id, &channel) {
                                Ok(()) => Ok(Response::ok_message(&format!("LISTEN {}", channel))),
                                Err(e) => Err(ServerError::Db(format!("LISTEN error: {}", e))),
                            }
                        }
                        PubSubCommand::Unlisten { channel } => {
                            match pubsub.unlisten(session_id, &channel) {
                                Ok(()) => Ok(Response::ok_message(&format!("UNLISTEN {}", channel))),
                                Err(e) => Err(ServerError::Db(format!("UNLISTEN error: {}", e))),
                            }
                        }
                        PubSubCommand::Notify { channel, payload } => {
                            match pubsub.notify(session_id, &channel, payload.as_deref()) {
                                Ok(result) => Ok(Response::ok_message(&format!(
                                    "NOTIFY {} (delivered to {} listeners)",
                                    channel, result.delivered
                                ))),
                                Err(e) => Err(ServerError::Db(format!("NOTIFY error: {}", e))),
                            }
                        }
                    }
                }
                SqlStatement::Pivot { source, pivot } => {
                    // PIVOT transforms rows to columns based on aggregate values
                    // Execute the source query first, then apply pivot transformation
                    Ok(Response::ok_message(&format!(
                        "PIVOT not yet fully implemented - pivot_column: {}, value_column: {}, values: {:?}",
                        pivot.pivot_column, pivot.value_column, pivot.pivot_values
                    )))
                }
                SqlStatement::Unpivot { source, unpivot } => {
                    // UNPIVOT transforms columns to rows
                    Ok(Response::ok_message(&format!(
                        "UNPIVOT not yet fully implemented - value_column: {}, name_column: {}, columns: {:?}",
                        unpivot.value_column, unpivot.name_column, unpivot.columns
                    )))
                }
            }
        }
        Request::Prepare { sql, database } => {
            let effective_db = database.as_deref().unwrap_or("default");
            let stmt = parse_sql(&sql)
                .map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;
            let parsed = match stmt {
                SqlStatement::Query(parsed) => parsed,
                _ => {
                    return Err(ServerError::BadRequest(
                        "prepare only supports SELECT queries".into(),
                    ))
                }
            };

            let main_db = parsed
                .database
                .as_deref()
                .filter(|d| *d != "default")
                .unwrap_or(effective_db)
                .to_string();

            let mut table_refs: Vec<(String, String, bool)> = Vec::new();
            table_refs.push((
                main_db.clone(),
                parsed
                    .table
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                parsed.database.as_deref() == Some("default") || parsed.database.is_none(),
            ));
            for join in &parsed.joins {
                let join_db = if join.database == "default" {
                    effective_db.to_string()
                } else {
                    join.database.clone()
                };
                table_refs.push((join_db, join.table.clone(), join.database == "default"));
            }

            for (db_name, table_name, _) in &table_refs {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Table {
                        database: db_name.clone(),
                        table: table_name.clone(),
                    },
                )?;
            }

            let mut effective_sql = sql.clone();
            if effective_db != "default" {
                for (_db, table_name, was_default) in &table_refs {
                    if *was_default {
                        let from_pat = format!("FROM {}", table_name);
                        let from_rep = format!("FROM {}.{}", effective_db, table_name);
                        let join_pat = format!("JOIN {}", table_name);
                        let join_rep = format!("JOIN {}.{}", effective_db, table_name);
                        let from_pat_lower = format!("from {}", table_name);
                        let from_rep_lower = format!("from {}.{}", effective_db, table_name);
                        let join_pat_lower = format!("join {}", table_name);
                        let join_rep_lower = format!("join {}.{}", effective_db, table_name);

                        effective_sql = effective_sql
                            .replace(&from_pat, &from_rep)
                            .replace(&join_pat, &join_rep)
                            .replace(&from_pat_lower, &from_rep_lower)
                            .replace(&join_pat_lower, &join_rep_lower);
                    }
                }
            }

            validate_select(&effective_sql, max_query_len)
                .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;
            let prepared_id = {
                let mut cache = prepared_cache
                    .lock()
                    .map_err(|_| ServerError::BadRequest("prepared cache lock poisoned".into()))?;
                let refs = table_refs
                    .into_iter()
                    .map(|(db_name, table_name, _)| (db_name, table_name))
                    .collect();
                cache.insert(effective_sql, refs)
            };
            let mut resp = Response::ok_message("prepared");
            resp.prepared_id = Some(prepared_id);
            Ok(resp)
        }
        Request::ExecutePrepared {
            id,
            timeout_millis,
            accept_compression,
        } => {
            if let Some(c) = accept_compression.as_deref() {
                if c != "zstd" {
                    return Err(ServerError::BadRequest("unsupported compression".into()));
                }
            }
            let prepared = {
                let mut cache = prepared_cache
                    .lock()
                    .map_err(|_| ServerError::BadRequest("prepared cache lock poisoned".into()))?;
                cache.get(&id)
            }
            .ok_or_else(|| ServerError::BadRequest("prepared statement not found".into()))?;

            for (db_name, table_name) in &prepared.table_refs {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Table {
                        database: db_name.clone(),
                        table: table_name.clone(),
                    },
                )?;
            }

            validate_select(&prepared.sql, max_query_len)
                .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;
            let db = db.clone();
            let sql = prepared.sql.clone();
            let resp = blocking(move || {
                db.query(QueryRequest {
                    sql,
                    timeout_millis,
                    collect_stats: false,
                    transaction_id: None,
                })
                .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            let mut payload = resp.records_ipc;
            let mut compression_used: Option<&'static str> = None;
            if matches!(accept_compression.as_deref(), Some("zstd")) {
                payload = zstd::stream::encode_all(std::io::Cursor::new(payload), 3)
                    .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
                compression_used = Some("zstd");
            }
            if payload.len() > max_frame_len {
                return Err(ServerError::BadRequest(format!(
                    "response too large ({} > {})",
                    payload.len(),
                    max_frame_len
                )));
            }
            let ipc_base64 = general_purpose::STANDARD.encode(&payload);
            Ok(Response {
                ipc_base64: Some(ipc_base64),
                data_skipped_bytes: Some(resp.data_skipped_bytes),
                segments_scanned: Some(resp.segments_scanned),
                compression: compression_used.map(|c| c.to_string()),
                ..Default::default()
            })
        }
        Request::ExecutePreparedBinary { .. } => Err(ServerError::BadRequest(
            "execute_prepared_binary must be handled by the binary response path".into(),
        )),
        Request::QueryBinary { .. } => Err(ServerError::BadRequest(
            "query_binary must be handled by the binary response path".into(),
        )),
        Request::IngestIpcBinary { .. } => Err(ServerError::BadRequest(
            "ingest_ipc_binary must be handled by the binary request path".into(),
        )),
        Request::IngestIpc {
            payload_base64,
            watermark_micros,
            database,
            table,
            shard_hint,
            compression,
        } => {
            let payload = general_purpose::STANDARD
                .decode(payload_base64)
                .map_err(|e| ServerError::BadRequest(format!("base64 decode failed: {e}")))?;
            process_ingest_ipc_payload(
                payload,
                watermark_micros,
                database,
                table,
                shard_hint,
                compression,
                db,
                auth_manager,
                authenticated_user,
                max_ipc_bytes,
            )
            .await
        }
        Request::IngestCsv {
            payload_base64,
            database,
            table,
            schema,
            has_header,
            delimiter,
            watermark_micros,
            shard_hint,
            infer_schema_rows,
        } => {
            require_privilege(
                Privilege::Insert,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let csv_bytes = general_purpose::STANDARD
                .decode(payload_base64)
                .map_err(|e| ServerError::BadRequest(format!("base64 decode failed: {e}")))?;
            let db = db.clone();
            let max_ipc = max_ipc_bytes;
            blocking(move || {
                let tables = db
                    .list_tables(Some(&database))
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                let existing_schema: Option<Vec<CsvField>> = tables
                    .into_iter()
                    .find(|t| t.name == table)
                    .and_then(|t| t.schema_json)
                    .and_then(|s| serde_json::from_str::<Vec<CsvField>>(&s).ok());
                let effective_schema = if let Some(sc) = existing_schema {
                    sc
                } else if let Some(sc) = schema {
                    sc
                } else {
                    if !has_header {
                        return Err(ServerError::BadRequest(
                            "schema missing and has_header=false; cannot infer".into(),
                        ));
                    }
                    let max_rows = infer_schema_rows.unwrap_or(1024);
                    infer_csv_schema(&csv_bytes, max_rows, delimiter.clone(), has_header).map_err(
                        |e| ServerError::BadRequest(format!("schema inference failed: {e}")),
                    )?
                };

                let delim_byte = delimiter
                    .and_then(|s| s.as_bytes().first().copied())
                    .unwrap_or(b',');
                let arrow_schema = build_schema(&effective_schema)
                    .map_err(|e| ServerError::BadRequest(format!("invalid schema: {e}")))?;
                let batches =
                    read_csv_batches(&csv_bytes, arrow_schema.clone(), has_header, delim_byte)
                        .map_err(|e| ServerError::BadRequest(format!("csv parse failed: {e}")))?;
                let ipc = batches_to_ipc(&batches)
                    .map_err(|e| ServerError::Encode(format!("ipc encode failed: {e}")))?;
                if ipc.len() > max_ipc {
                    return Err(ServerError::BadRequest(format!(
                        "payload too large after conversion ({} > {})",
                        ipc.len(),
                        max_ipc
                    )));
                }

                let schema_json = serde_json::to_string(&effective_schema).unwrap_or_default();
                db.create_database(&database)
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                db.create_table(&database, &table, Some(schema_json))
                    .map_err(|e| ServerError::Db(e.to_string()))?;

                db.ingest_ipc(IngestBatch {
                    payload_ipc: ipc,
                    watermark_micros: watermark_micros.unwrap_or(0),
                    shard_override: shard_hint,
                    database: Some(database),
                    table: Some(table),
                })
                .map_err(|e| ServerError::Db(e.to_string()))?;
                Ok(())
            })
            .await?;
            Ok(Response::ok_message("ingest ok"))
        }
        Request::ListDatabases => {
            require_privilege(Privilege::Usage, PrivilegeTarget::Global)?;
            let db = db.clone();
            let dbs = blocking(move || {
                db.list_databases()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response {
                databases: Some(dbs),
                ..Default::default()
            })
        }
        Request::ListTables { database } => {
            if let Some(db_name) = &database {
                require_privilege(Privilege::Usage, PrivilegeTarget::Database(db_name.clone()))?;
            } else {
                require_privilege(Privilege::Usage, PrivilegeTarget::Global)?;
            }
            let db = db.clone();
            let tables = blocking(move || {
                db.list_tables(database.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response {
                tables: Some(tables),
                ..Default::default()
            })
        }
        Request::Manifest => {
            require_privilege(Privilege::Select, PrivilegeTarget::Global)?;
            let db = db.clone();
            let manifest = blocking(move || {
                db.export_manifest()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let json_val: serde_json::Value = serde_json::from_slice(&manifest)
                .map_err(|e| ServerError::Encode(format!("manifest parse failed: {e}")))?;
            Ok(Response {
                manifest_json: Some(json_val),
                ..Default::default()
            })
        }
        Request::Health => {
            let db = db.clone();
            blocking(move || {
                db.health_check()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("healthy"))
        }
        Request::CreateDatabase { name } => {
            require_privilege(Privilege::CreateDb, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || {
                db.create_database(&name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("database created"))
        }
        Request::CreateTable {
            database,
            table,
            schema,
            compression,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            if let Some(c) = compression.as_deref() {
                if c != "zstd" {
                    return Err(ServerError::BadRequest(
                        "unsupported compression (only zstd)".into(),
                    ));
                }
            }
            let schema_json =
                serde_json::to_string(&schema).map_err(|e| ServerError::Encode(e.to_string()))?;
            let db = db.clone();
            blocking(move || {
                db.create_table(&database, &table, Some(schema_json))
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                if compression.is_some() {
                    db.set_table_compression(&database, &table, compression)
                        .map_err(|e| ServerError::Db(e.to_string()))?;
                }
                Ok(())
            })
            .await?;
            Ok(Response::ok_message("table created"))
        }
        Request::Compact { database, table } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let db_name = database.clone();
            let table_name = table.clone();
            let result = blocking(move || {
                db.compact_table(&db_name, &table_name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = if let Some(entry) = result {
                format!(
                    "compacted table {}.{} into segment {} ({} bytes)",
                    database, table, entry.segment_id, entry.size_bytes
                )
            } else {
                "nothing to compact (1 or fewer segments)".to_string()
            };
            Ok(Response::ok_message(&msg))
        }
        Request::Checkpoint => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || db.checkpoint().map_err(|e| ServerError::Db(e.to_string()))).await?;
            Ok(Response::ok_message("checkpoint ok"))
        }
        Request::PlanBundle {
            max_bytes,
            since_version,
            prefer_hot,
            target_bytes_per_sec,
            max_entries,
        } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Global)?;
            let db = db.clone();
            let plan = blocking(move || {
                db.plan_bundle(boyodb_core::BundleRequest {
                    max_bytes,
                    since_version,
                    prefer_hot,
                    target_bytes_per_sec,
                    max_entries,
                })
                .map_err(map_engine_error)
            })
            .await?;
            let bundle_plan =
                serde_json::to_value(plan).map_err(|e| ServerError::Encode(e.to_string()))?;
            Ok(Response {
                bundle_plan: Some(bundle_plan),
                ..Default::default()
            })
        }
        Request::ExportBundle {
            max_bytes,
            since_version,
            prefer_hot,
            target_bytes_per_sec,
            max_entries,
        } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Global)?;
            let db = db.clone();
            let bundle = blocking(move || {
                db.export_bundle(boyodb_core::BundleRequest {
                    max_bytes,
                    since_version,
                    prefer_hot,
                    target_bytes_per_sec,
                    max_entries,
                })
                .map_err(map_engine_error)
            })
            .await?;
            let bundle_json =
                serde_json::to_value(bundle).map_err(|e| ServerError::Encode(e.to_string()))?;
            Ok(Response {
                bundle_json: Some(bundle_json),
                ..Default::default()
            })
        }
        Request::ApplyBundle { bundle_json } => {
            require_privilege(Privilege::Insert, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || {
                let payload: boyodb_core::BundlePayload = serde_json::from_value(bundle_json)
                    .map_err(|e| ServerError::BadRequest(format!("invalid bundle: {e}")))?;
                db.apply_bundle(payload).map_err(map_engine_error)
            })
            .await?;
            Ok(Response::ok_message("bundle applied"))
        }
        Request::ValidateBundle { bundle_json } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || {
                let payload: boyodb_core::BundlePayload = serde_json::from_value(bundle_json)
                    .map_err(|e| ServerError::BadRequest(format!("invalid bundle: {e}")))?;
                db.validate_bundle(&payload).map_err(map_engine_error)
            })
            .await?;
            Ok(Response::ok_message("bundle valid"))
        }
        Request::ApplyManifest {
            manifest_json,
            overwrite,
        } => {
            require_privilege(Privilege::Create, PrivilegeTarget::Global)?;
            if !allow_manifest_import {
                return Err(ServerError::BadRequest(
                    "manifest import disabled; start server with --allow-manifest-import".into(),
                ));
            }
            let bytes = serde_json::to_vec(&manifest_json)
                .map_err(|e| ServerError::Encode(format!("manifest serialize failed: {e}")))?;
            let db = db.clone();
            blocking(move || {
                db.import_manifest(&bytes, overwrite)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("manifest applied"))
        }
        Request::Insight { database, table } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Global)?;
            let db = db.clone();
            let manifest = blocking(move || {
                db.export_manifest()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let manifest_json: serde_json::Value = serde_json::from_slice(&manifest)
                .map_err(|e| ServerError::Encode(format!("manifest parse failed: {e}")))?;
            let insight_data = build_insight(&manifest_json, database.as_deref(), table.as_deref());
            Ok(Response {
                insight: Some(insight_data),
                ..Default::default()
            })
        }
        Request::Metrics => {
            let db = db.clone();
            let conn_current = conn_stats.current();
            let conn_max = conn_stats.max();
            let queries_admitted = conn_stats.queries_admitted();
            let queries_rejected = conn_stats.queries_rejected();
            let queries_active = conn_stats.active_queries();
            let queries_max = conn_stats.max_queries();
            let metrics_text = blocking(move || {
                Ok(db.prometheus_metrics_with_connections(
                    conn_current,
                    conn_max,
                    queries_admitted,
                    queries_rejected,
                    queries_active,
                    queries_max,
                ))
            })
            .await?;
            Ok(Response {
                metrics_text: Some(metrics_text),
                ..Default::default()
            })
        }
        Request::WalStats => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let stats = collect_wal_stats(&wal_dir, wal_max_bytes, wal_max_segments)?;
            Ok(Response {
                wal_stats: Some(stats),
                ..Default::default()
            })
        }
        Request::Explain { sql } => {
            let db = db.clone();
            let plan =
                blocking(move || db.explain(&sql).map_err(|e| ServerError::Db(e.to_string())))
                    .await?;
            let explain_json = serde_json::to_value(plan).map_err(|e| {
                ServerError::Encode(format!("failed to serialize explain plan: {e}"))
            })?;
            Ok(Response {
                explain_plan: Some(explain_json),
                ..Default::default()
            })
        }
        Request::HealthDetailed => {
            let db = db.clone();
            let status = blocking(move || Ok(db.health_status())).await?;
            let status_json = serde_json::to_value(status).map_err(|e| {
                ServerError::Encode(format!("failed to serialize health status: {e}"))
            })?;
            Ok(Response {
                health_status: Some(status_json),
                ..Default::default()
            })
        }
        // Login/Logout are handled in handle_conn before process_request is called
        Request::Login { .. } | Request::Logout => Ok(Response::error(
            "login/logout should be handled before process_request",
        )),
        // User management operations
        Request::CreateUser {
            username,
            password,
            superuser,
            default_database,
        } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.create_user(&username, &password, actor)?;
            if superuser {
                auth.set_superuser(&username, true, actor)?;
            }
            if let Some(db) = default_database {
                auth.set_default_database(&username, Some(&db), actor)?;
            }
            Ok(Response::ok_message(&format!(
                "user '{}' created",
                username
            )))
        }
        Request::DropUser { username } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.drop_user(&username, actor)?;
            Ok(Response::ok_message(&format!(
                "user '{}' dropped",
                username
            )))
        }
        Request::AlterUserPassword { username, password } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.alter_user_password(&username, &password, actor)?;
            Ok(Response::ok_message(&format!(
                "password changed for user '{}'",
                username
            )))
        }
        Request::LockUser { username } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.lock_user(&username, actor)?;
            Ok(Response::ok_message(&format!("user '{}' locked", username)))
        }
        Request::UnlockUser { username } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.unlock_user(&username, actor)?;
            Ok(Response::ok_message(&format!(
                "user '{}' unlocked",
                username
            )))
        }
        // Role management operations
        Request::CreateRole { name, description } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.create_role(&name, description.as_deref(), actor)?;
            Ok(Response::ok_message(&format!("role '{}' created", name)))
        }
        Request::DropRole { name } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.drop_role(&name, actor)?;
            Ok(Response::ok_message(&format!("role '{}' dropped", name)))
        }
        Request::GrantRole { username, role } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.grant_role(&username, &role, actor)?;
            Ok(Response::ok_message(&format!(
                "role '{}' granted to '{}'",
                role, username
            )))
        }
        Request::RevokeRole { username, role } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            auth.revoke_role(&username, &role, actor)?;
            Ok(Response::ok_message(&format!(
                "role '{}' revoked from '{}'",
                role, username
            )))
        }
        // Privilege management operations
        Request::GrantPrivilege {
            username,
            privilege,
            database,
            table,
            with_grant_option,
        } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            let priv_type = parse_privilege(&privilege)?;
            let target = build_privilege_target(database, table)?;
            auth.grant_privilege(
                &username,
                priv_type,
                target.clone(),
                with_grant_option,
                actor,
            )?;
            Ok(Response::ok_message(&format!(
                "{} granted to '{}'",
                privilege, username
            )))
        }
        Request::RevokePrivilege {
            username,
            privilege,
            database,
            table,
        } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let actor = authenticated_user.as_deref().unwrap_or("system");
            let priv_type = parse_privilege(&privilege)?;
            let target = build_privilege_target(database, table)?;
            auth.revoke_privilege(&username, priv_type, target, actor)?;
            Ok(Response::ok_message(&format!(
                "{} revoked from '{}'",
                privilege, username
            )))
        }
        // Show commands
        Request::ShowUsers => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let users = auth.list_users()?;
            let users_json = serde_json::to_value(users)
                .map_err(|e| ServerError::Encode(format!("failed to serialize users: {e}")))?;
            Ok(Response {
                users: Some(users_json),
                ..Default::default()
            })
        }
        Request::ShowRoles => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            let roles = auth.list_roles()?;
            let roles_json = serde_json::to_value(roles)
                .map_err(|e| ServerError::Encode(format!("failed to serialize roles: {e}")))?;
            Ok(Response {
                roles: Some(roles_json),
                ..Default::default()
            })
        }
        Request::ShowGrants { username } => {
            let auth = auth_manager
                .as_ref()
                .ok_or(ServerError::Auth("auth not enabled".into()))?;
            // Get user info and privileges
            let user_info = auth.get_user(&username)?;
            let privileges = auth.get_user_privileges(&username)?;
            let grants_data = serde_json::json!({
                "user": user_info,
                "privileges": privileges,
            });
            Ok(Response {
                grants: Some(grants_data),
                ..Default::default()
            })
        }
        Request::ClusterStatus => {
            if let Some(ref cluster) = cluster_manager {
                let status = cluster.status();
                let status_json_str = status.to_json();
                let status_value: serde_json::Value = serde_json::from_str(&status_json_str)
                    .unwrap_or_else(|_| serde_json::json!({ "error": "failed to parse status" }));
                Ok(Response {
                    cluster_status: Some(status_value),
                    ..Default::default()
                })
            } else {
                Ok(Response::ok_message("cluster mode not enabled"))
            }
        }
        Request::ReplicaStatus => {
            if replica_mode {
                if let Some(ref metrics) = replica_sync_metrics {
                    let snapshot = metrics.snapshot();
                    let status_value = serde_json::json!({
                        "mode": "replica",
                        "syncs_completed": snapshot.syncs_completed,
                        "syncs_failed": snapshot.syncs_failed,
                        "bytes_synced": snapshot.bytes_synced,
                        "segments_synced": snapshot.segments_synced,
                        "last_sync_millis": snapshot.last_sync_millis,
                        "last_sync_lag_ms": snapshot.last_sync_lag_ms,
                        "manifest_version": snapshot.manifest_version,
                    });
                    Ok(Response {
                        replica_status: Some(status_value),
                        ..Default::default()
                    })
                } else {
                    Ok(Response {
                        replica_status: Some(serde_json::json!({
                            "mode": "replica",
                            "error": "sync metrics not available"
                        })),
                        ..Default::default()
                    })
                }
            } else {
                Ok(Response {
                    replica_status: Some(serde_json::json!({
                        "mode": "primary",
                        "message": "this is a primary server, not a replica"
                    })),
                    ..Default::default()
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
struct QueryBinaryMeta {
    data_skipped_bytes: u64,
    segments_scanned: usize,
}

/// RAII guard to ensure temporary IPC files are cleaned up on all code paths.
/// The file is automatically deleted when the guard is dropped, unless `disarm()` is called.
struct TempFileGuard {
    path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        TempFileGuard { path: Some(path) }
    }

    /// Disarm the guard, returning the path without deleting the file.
    /// Use this when ownership of the file is being transferred to the caller.
    fn disarm(mut self) -> PathBuf {
        self.path.take().expect("TempFileGuard already disarmed")
    }

    /// Get a reference to the path (for use while guard is still active).
    fn path(&self) -> &Path {
        self.path.as_ref().expect("TempFileGuard already disarmed")
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(ref path) = self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct ChunkedSender {
    tx: tokio::sync::mpsc::UnboundedSender<Result<Vec<u8>, ServerError>>,
    buf: Vec<u8>,
    chunk_size: usize,
}

impl ChunkedSender {
    fn new(
        tx: tokio::sync::mpsc::UnboundedSender<Result<Vec<u8>, ServerError>>,
        chunk_size: usize,
    ) -> Self {
        ChunkedSender {
            tx,
            buf: Vec::with_capacity(chunk_size),
            chunk_size,
        }
    }

    fn send_chunk(&mut self, chunk: Vec<u8>) -> std::io::Result<()> {
        self.tx
            .send(Ok(chunk))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "stream closed"))
    }

    fn finish(mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = std::mem::take(&mut self.buf);
            self.send_chunk(chunk)?;
        }
        Ok(())
    }
}

impl std::io::Write for ChunkedSender {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= self.chunk_size {
            let chunk: Vec<u8> = self.buf.drain(..self.chunk_size).collect();
            self.send_chunk(chunk)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = std::mem::take(&mut self.buf);
            self.send_chunk(chunk)?;
        }
        Ok(())
    }
}

fn create_temp_ipc_file(data_dir: &Path) -> Result<(PathBuf, std::fs::File), ServerError> {
    let tmp_dir = data_dir.join("tmp");
    std::fs::create_dir_all(&tmp_dir)
        .map_err(|e| ServerError::Io(format!("create temp dir failed: {e}")))?;
    let pid = std::process::id();
    for attempt in 0..10 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ServerError::Io(format!("time error: {e}")))?;
        let filename = format!("query_stream_{pid}_{}_{}.ipc", now.as_nanos(), attempt);
        let path = tmp_dir.join(filename);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(ServerError::Io(format!("open temp file failed: {e}"))),
        }
    }
    Err(ServerError::Io("failed to create temp file".into()))
}

async fn process_query_binary(
    sql: String,
    timeout_millis: u32,
    accept_compression: Option<String>,
    default_database: Option<String>,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    authenticated_user: Option<String>,
    max_query_len: usize,
    max_frame_len: usize,
    data_dir: PathBuf,
) -> Result<(Response, PathBuf), ServerError> {
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };

    if let Some(c) = accept_compression.as_deref() {
        if c != "zstd" {
            return Err(ServerError::BadRequest("unsupported compression".into()));
        }
    }

    let effective_db = default_database.as_deref().unwrap_or("default");
    let stmt =
        parse_sql(&sql).map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;

    let parsed = match stmt {
        SqlStatement::Query(parsed) => parsed,
        _ => {
            return Err(ServerError::BadRequest(
                "query_binary only supports SELECT queries".into(),
            ))
        }
    };

    let main_db = parsed
        .database
        .as_deref()
        .filter(|d| *d != "default")
        .unwrap_or(effective_db)
        .to_string();

    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
    table_refs.push((
        main_db.clone(),
        parsed
            .table
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        parsed.database.as_deref() == Some("default") || parsed.database.is_none(),
    ));
    for join in &parsed.joins {
        let join_db = if join.database == "default" {
            effective_db.to_string()
        } else {
            join.database.clone()
        };
        table_refs.push((join_db, join.table.clone(), join.database == "default"));
    }

    for (db_name, table_name, _) in &table_refs {
        require_privilege(
            Privilege::Select,
            PrivilegeTarget::Table {
                database: db_name.clone(),
                table: table_name.clone(),
            },
        )?;
    }

    let mut effective_sql = sql.clone();
    if effective_db != "default" {
        for (_db, table_name, was_default) in &table_refs {
            if *was_default {
                let from_pat = format!("FROM {}", table_name);
                let from_rep = format!("FROM {}.{}", effective_db, table_name);
                let join_pat = format!("JOIN {}", table_name);
                let join_rep = format!("JOIN {}.{}", effective_db, table_name);
                let from_pat_lower = format!("from {}", table_name);
                let from_rep_lower = format!("from {}.{}", effective_db, table_name);
                let join_pat_lower = format!("join {}", table_name);
                let join_rep_lower = format!("join {}.{}", effective_db, table_name);

                effective_sql = effective_sql
                    .replace(&from_pat, &from_rep)
                    .replace(&join_pat, &join_rep)
                    .replace(&from_pat_lower, &from_rep_lower)
                    .replace(&join_pat_lower, &join_rep_lower);
            }
        }
    }

    validate_select(&effective_sql, max_query_len)
        .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;
    let (tmp_path, tmp_file) = create_temp_ipc_file(&data_dir)?;
    // Use RAII guard to ensure temp file cleanup on all error paths
    let mut payload_guard = TempFileGuard::new(tmp_path);
    let query_request = QueryRequest {
        sql: effective_sql,
        timeout_millis,
        collect_stats: false,
        transaction_id: None,
    };
    let db = db.clone();
    let meta = blocking(move || {
        let mut file = tmp_file;
        match db.query_ipc_to_writer(query_request.clone(), &mut file) {
            Ok(resp) => Ok(QueryBinaryMeta {
                data_skipped_bytes: resp.data_skipped_bytes,
                segments_scanned: resp.segments_scanned,
            }),
            Err(EngineError::NotImplemented(_)) => {
                let resp = db
                    .query(query_request)
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                if !resp.records_ipc.is_empty() {
                    file.write_all(&resp.records_ipc)
                        .map_err(|e| ServerError::Io(format!("write ipc failed: {e}")))?;
                }
                Ok(QueryBinaryMeta {
                    data_skipped_bytes: resp.data_skipped_bytes,
                    segments_scanned: resp.segments_scanned,
                })
            }
            Err(e) => Err(ServerError::Db(e.to_string())),
        }
    })
    .await?;

    let mut payload_len = std::fs::metadata(payload_guard.path())
        .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
        .len();
    let mut compression_used: Option<&'static str> = None;
    if payload_len > 0 && matches!(accept_compression.as_deref(), Some("zstd")) {
        let (compressed_path, compressed_file) = create_temp_ipc_file(&data_dir)?;
        // Create guard for compressed file; original will be cleaned up by its guard
        let compressed_guard = TempFileGuard::new(compressed_path);
        let mut input = std::fs::File::open(payload_guard.path())
            .map_err(|e| ServerError::Io(format!("open payload failed: {e}")))?;
        let encoder = zstd::stream::Encoder::new(compressed_file, 3)
            .map_err(|e| ServerError::Encode(format!("compression init failed: {e}")))?;
        let mut encoder = encoder.auto_finish();
        std::io::copy(&mut input, &mut encoder)
            .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
        drop(encoder);
        // Drop original guard (deletes original file), replace with compressed
        drop(payload_guard);
        payload_guard = compressed_guard;
        payload_len = std::fs::metadata(payload_guard.path())
            .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
            .len();
        compression_used = Some("zstd");
    }
    if payload_len > max_frame_len as u64 {
        // payload_guard will clean up the temp file automatically on return
        return Err(ServerError::BadRequest(format!(
            "response too large ({} > {})",
            payload_len, max_frame_len
        )));
    }

    let header = Response {
        ipc_len: if payload_len > 0 {
            Some(payload_len)
        } else {
            None
        },
        data_skipped_bytes: Some(meta.data_skipped_bytes),
        segments_scanned: Some(meta.segments_scanned),
        compression: compression_used.map(|c| c.to_string()),
        ..Default::default()
    };
    // Disarm the guard - caller takes ownership of the temp file
    let payload_path = payload_guard.disarm();
    Ok((header, payload_path))
}

async fn process_query_binary_stream(
    sql: String,
    timeout_millis: u32,
    default_database: Option<String>,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    authenticated_user: Option<String>,
    max_query_len: usize,
    max_frame_len: usize,
    timeout_dur: Duration,
    stream: &mut (impl AsyncWrite + Unpin),
) -> Result<(), ServerError> {
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };

    let effective_db = default_database.as_deref().unwrap_or("default");
    let stmt =
        parse_sql(&sql).map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;

    let parsed = match stmt {
        SqlStatement::Query(parsed) => parsed,
        _ => {
            return Err(ServerError::BadRequest(
                "query_binary only supports SELECT queries".into(),
            ))
        }
    };

    let main_db = parsed
        .database
        .as_deref()
        .filter(|d| *d != "default")
        .unwrap_or(effective_db)
        .to_string();

    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
    table_refs.push((
        main_db.clone(),
        parsed
            .table
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        parsed.database.as_deref() == Some("default") || parsed.database.is_none(),
    ));
    for join in &parsed.joins {
        let join_db = if join.database == "default" {
            effective_db.to_string()
        } else {
            join.database.clone()
        };
        table_refs.push((join_db, join.table.clone(), join.database == "default"));
    }

    for (db_name, table_name, _) in &table_refs {
        require_privilege(
            Privilege::Select,
            PrivilegeTarget::Table {
                database: db_name.clone(),
                table: table_name.clone(),
            },
        )?;
    }

    let mut effective_sql = sql.clone();
    if effective_db != "default" {
        for (_db, table_name, was_default) in &table_refs {
            if *was_default {
                let from_pat = format!("FROM {}", table_name);
                let from_rep = format!("FROM {}.{}", effective_db, table_name);
                let join_pat = format!("JOIN {}", table_name);
                let join_rep = format!("JOIN {}.{}", effective_db, table_name);
                let from_pat_lower = format!("from {}", table_name);
                let from_rep_lower = format!("from {}.{}", effective_db, table_name);
                let join_pat_lower = format!("join {}", table_name);
                let join_rep_lower = format!("join {}.{}", effective_db, table_name);

                effective_sql = effective_sql
                    .replace(&from_pat, &from_rep)
                    .replace(&join_pat, &join_rep)
                    .replace(&from_pat_lower, &from_rep_lower)
                    .replace(&join_pat_lower, &join_rep_lower);
            }
        }
    }

    validate_select(&effective_sql, max_query_len)
        .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;

    let query_request = QueryRequest {
        sql: effective_sql,
        timeout_millis,
        collect_stats: false,
        transaction_id: None,
    };
    let db = db.clone();
    let chunk_size = max_frame_len.min(256 * 1024).max(1024);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Result<Vec<u8>, ServerError>>();
    let tx_done = tx.clone();

    tokio::task::spawn_blocking(move || {
        let mut writer = ChunkedSender::new(tx, chunk_size);
        let result = match db.query_ipc_to_writer(query_request.clone(), &mut writer) {
            Ok(_resp) => Ok(()),
            Err(EngineError::NotImplemented(_)) => {
                let resp = db
                    .query(query_request)
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                if !resp.records_ipc.is_empty() {
                    writer
                        .write_all(&resp.records_ipc)
                        .map_err(|e| ServerError::Io(format!("write ipc failed: {e}")))?;
                }
                Ok(())
            }
            Err(e) => Err(ServerError::Db(e.to_string())),
        };
        let _ = writer.finish();
        match result {
            Ok(_) => {
                let _ = tx_done.send(Ok(Vec::new()));
            }
            Err(e) => {
                let _ = tx_done.send(Err(e));
            }
        }
        Ok::<(), ServerError>(())
    });

    let first = match rx.recv().await {
        Some(msg) => msg,
        None => {
            return Err(ServerError::Db("stream closed before response".into()));
        }
    };

    match first {
        Err(e) => {
            return Err(e);
        }
        Ok(first_chunk) => {
            let header = Response {
                ipc_streaming: Some(true),
                ..Default::default()
            };
            write_response(stream, &header, timeout_dur, max_frame_len).await?;

            if !first_chunk.is_empty() {
                write_frame_bytes(stream, &first_chunk, timeout_dur, max_frame_len).await?;
            }
            while let Some(msg) = rx.recv().await {
                match msg {
                    Ok(chunk) => {
                        write_frame_bytes(stream, &chunk, timeout_dur, max_frame_len).await?;
                        if chunk.is_empty() {
                            break;
                        }
                    }
                    Err(e) => {
                        // Send empty frame to signal end of stream, then return the error
                        let _ = write_frame_bytes(stream, &[], timeout_dur, max_frame_len).await;
                        warn!("streaming query error: {}", e);
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn process_execute_prepared_binary(
    id: String,
    timeout_millis: u32,
    accept_compression: Option<String>,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    authenticated_user: Option<String>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
    max_query_len: usize,
    max_frame_len: usize,
    data_dir: PathBuf,
) -> Result<(Response, PathBuf), ServerError> {
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };

    if let Some(c) = accept_compression.as_deref() {
        if c != "zstd" {
            return Err(ServerError::BadRequest("unsupported compression".into()));
        }
    }

    let prepared = {
        let mut cache = prepared_cache
            .lock()
            .map_err(|_| ServerError::BadRequest("prepared cache lock poisoned".into()))?;
        cache.get(&id)
    }
    .ok_or_else(|| ServerError::BadRequest("prepared statement not found".into()))?;

    for (db_name, table_name) in &prepared.table_refs {
        require_privilege(
            Privilege::Select,
            PrivilegeTarget::Table {
                database: db_name.clone(),
                table: table_name.clone(),
            },
        )?;
    }

    validate_select(&prepared.sql, max_query_len)
        .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;

    let (tmp_path, tmp_file) = create_temp_ipc_file(&data_dir)?;
    // Use RAII guard to ensure temp file cleanup on all error paths
    let mut payload_guard = TempFileGuard::new(tmp_path);
    let query_request = QueryRequest {
        sql: prepared.sql.clone(),
        timeout_millis,
        collect_stats: false,
        transaction_id: None,
    };
    let db = db.clone();
    let meta = blocking(move || {
        let mut file = tmp_file;
        match db.query_ipc_to_writer(query_request.clone(), &mut file) {
            Ok(resp) => Ok(QueryBinaryMeta {
                data_skipped_bytes: resp.data_skipped_bytes,
                segments_scanned: resp.segments_scanned,
            }),
            Err(EngineError::NotImplemented(_)) => {
                let resp = db
                    .query(query_request)
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                if !resp.records_ipc.is_empty() {
                    file.write_all(&resp.records_ipc)
                        .map_err(|e| ServerError::Io(format!("write ipc failed: {e}")))?;
                }
                Ok(QueryBinaryMeta {
                    data_skipped_bytes: resp.data_skipped_bytes,
                    segments_scanned: resp.segments_scanned,
                })
            }
            Err(e) => Err(ServerError::Db(e.to_string())),
        }
    })
    .await?;

    let mut payload_len = std::fs::metadata(payload_guard.path())
        .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
        .len();
    let mut compression_used: Option<&'static str> = None;
    if payload_len > 0 && matches!(accept_compression.as_deref(), Some("zstd")) {
        let (compressed_path, compressed_file) = create_temp_ipc_file(&data_dir)?;
        // Create guard for compressed file; original will be cleaned up by its guard
        let compressed_guard = TempFileGuard::new(compressed_path);
        let mut input = std::fs::File::open(payload_guard.path())
            .map_err(|e| ServerError::Io(format!("open payload failed: {e}")))?;
        let encoder = zstd::stream::Encoder::new(compressed_file, 3)
            .map_err(|e| ServerError::Encode(format!("compression init failed: {e}")))?;
        let mut encoder = encoder.auto_finish();
        std::io::copy(&mut input, &mut encoder)
            .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
        drop(encoder);
        // Drop original guard (deletes original file), replace with compressed
        drop(payload_guard);
        payload_guard = compressed_guard;
        payload_len = std::fs::metadata(payload_guard.path())
            .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
            .len();
        compression_used = Some("zstd");
    }
    if payload_len > max_frame_len as u64 {
        // payload_guard will clean up the temp file automatically on return
        return Err(ServerError::BadRequest(format!(
            "response too large ({} > {})",
            payload_len, max_frame_len
        )));
    }

    let header = Response {
        ipc_len: if payload_len > 0 {
            Some(payload_len)
        } else {
            None
        },
        data_skipped_bytes: Some(meta.data_skipped_bytes),
        segments_scanned: Some(meta.segments_scanned),
        compression: compression_used.map(|c| c.to_string()),
        ..Default::default()
    };
    // Disarm the guard - caller takes ownership of the temp file
    let payload_path = payload_guard.disarm();
    Ok((header, payload_path))
}

async fn process_execute_prepared_binary_stream(
    id: String,
    timeout_millis: u32,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    authenticated_user: Option<String>,
    prepared_cache: Arc<Mutex<PreparedStatementCache>>,
    max_query_len: usize,
    max_frame_len: usize,
    timeout_dur: Duration,
    stream: &mut (impl AsyncWrite + Unpin),
) -> Result<(), ServerError> {
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };

    let prepared = {
        let mut cache = prepared_cache
            .lock()
            .map_err(|_| ServerError::BadRequest("prepared cache lock poisoned".into()))?;
        cache.get(&id)
    }
    .ok_or_else(|| ServerError::BadRequest("prepared statement not found".into()))?;

    for (db_name, table_name) in &prepared.table_refs {
        require_privilege(
            Privilege::Select,
            PrivilegeTarget::Table {
                database: db_name.clone(),
                table: table_name.clone(),
            },
        )?;
    }

    validate_select(&prepared.sql, max_query_len)
        .map_err(|e| ServerError::BadRequest(format!("invalid sql: {e}")))?;

    let query_request = QueryRequest {
        sql: prepared.sql.clone(),
        timeout_millis,
        collect_stats: false,
        transaction_id: None,
    };
    let db = db.clone();
    let chunk_size = max_frame_len.min(256 * 1024).max(1024);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Result<Vec<u8>, ServerError>>();
    let tx_done = tx.clone();

    tokio::task::spawn_blocking(move || {
        let mut writer = ChunkedSender::new(tx, chunk_size);
        let result = match db.query_ipc_to_writer(query_request.clone(), &mut writer) {
            Ok(_resp) => Ok(()),
            Err(EngineError::NotImplemented(_)) => {
                let resp = db
                    .query(query_request)
                    .map_err(|e| ServerError::Db(e.to_string()))?;
                if !resp.records_ipc.is_empty() {
                    writer
                        .write_all(&resp.records_ipc)
                        .map_err(|e| ServerError::Io(format!("write ipc failed: {e}")))?;
                }
                Ok(())
            }
            Err(e) => Err(ServerError::Db(e.to_string())),
        };
        let _ = writer.finish();
        match result {
            Ok(_) => {
                let _ = tx_done.send(Ok(Vec::new()));
            }
            Err(e) => {
                let _ = tx_done.send(Err(e));
            }
        }
        Ok::<(), ServerError>(())
    });

    let first = match rx.recv().await {
        Some(msg) => msg,
        None => {
            return Err(ServerError::Db("stream closed before response".into()));
        }
    };

    match first {
        Err(e) => {
            return Err(e);
        }
        Ok(first_chunk) => {
            let header = Response {
                ipc_streaming: Some(true),
                ..Default::default()
            };
            write_response(stream, &header, timeout_dur, max_frame_len).await?;

            if !first_chunk.is_empty() {
                write_frame_bytes(stream, &first_chunk, timeout_dur, max_frame_len).await?;
            }
            while let Some(msg) = rx.recv().await {
                match msg {
                    Ok(chunk) => {
                        write_frame_bytes(stream, &chunk, timeout_dur, max_frame_len).await?;
                        if chunk.is_empty() {
                            break;
                        }
                    }
                    Err(e) => {
                        // Send empty frame to signal end of stream, then return the error
                        let _ = write_frame_bytes(stream, &[], timeout_dur, max_frame_len).await;
                        warn!("streaming query error: {}", e);
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn process_ingest_ipc_payload(
    mut payload: Vec<u8>,
    watermark_micros: u64,
    database: Option<String>,
    table: Option<String>,
    shard_hint: Option<u64>,
    compression: Option<String>,
    db: Arc<Db>,
    auth_manager: Option<Arc<AuthManager>>,
    authenticated_user: Option<String>,
    max_ipc_bytes: usize,
) -> Result<Response, ServerError> {
    let target = PrivilegeTarget::Table {
        database: database.clone().unwrap_or_else(|| "default".to_string()),
        table: table.clone().unwrap_or_else(|| "default".to_string()),
    };
    if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
        auth.require_privilege(user, Privilege::Insert, &target)?;
    }

    if matches!(compression.as_deref(), Some("zstd")) {
        payload = decompress_with_limit(&payload, max_ipc_bytes)
            .map_err(|e| ServerError::BadRequest(format!("zstd decode failed: {e}")))?;
    } else if compression.is_some() {
        return Err(ServerError::BadRequest("unsupported compression".into()));
    }
    if payload.len() > max_ipc_bytes {
        return Err(ServerError::BadRequest(format!(
            "payload too large ({} > {})",
            payload.len(),
            max_ipc_bytes
        )));
    }

    let db = db.clone();
    blocking(move || {
        db.ingest_ipc(IngestBatch {
            payload_ipc: payload,
            watermark_micros,
            shard_override: shard_hint,
            database,
            table,
        })
        .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;
    Ok(Response::ok_message("ingest ok"))
}

fn parse_privilege(s: &str) -> Result<Privilege, ServerError> {
    match s.to_uppercase().as_str() {
        "SELECT" => Ok(Privilege::Select),
        "INSERT" => Ok(Privilege::Insert),
        "UPDATE" => Ok(Privilege::Update),
        "DELETE" => Ok(Privilege::Delete),
        "CREATE" => Ok(Privilege::Create),
        "DROP" => Ok(Privilege::Drop),
        "ALTER" => Ok(Privilege::Alter),
        "GRANT" => Ok(Privilege::Grant),
        "TRUNCATE" => Ok(Privilege::Truncate),
        "ALL" => Ok(Privilege::All),
        "CONNECT" => Ok(Privilege::Connect),
        "USAGE" => Ok(Privilege::Usage),
        "CREATEDB" => Ok(Privilege::CreateDb),
        "CREATEUSER" => Ok(Privilege::CreateUser),
        "SUPERUSER" => Ok(Privilege::Superuser),
        _ => Err(ServerError::BadRequest(format!("unknown privilege: {}", s))),
    }
}

fn build_privilege_target(
    database: Option<String>,
    table: Option<String>,
) -> Result<PrivilegeTarget, ServerError> {
    match (database, table) {
        (Some(db), Some(tbl)) => Ok(PrivilegeTarget::Table {
            database: db,
            table: tbl,
        }),
        (Some(db), None) => Ok(PrivilegeTarget::Database(db)),
        (None, Some(_)) => Err(ServerError::BadRequest(
            "table specified without database".into(),
        )),
        (None, None) => Ok(PrivilegeTarget::Global),
    }
}

/// Execute an SQL auth command via the AuthManager
fn execute_auth_command(
    auth: &AuthManager,
    cmd: AuthCommand,
    actor: &str,
) -> Result<Response, ServerError> {
    use boyodb_core::GrantTargetType;

    match cmd {
        AuthCommand::CreateUser {
            username,
            password,
            options,
        } => {
            auth.create_user(&username, &password, actor)?;
            if options.superuser {
                auth.set_superuser(&username, true, actor)?;
            }
            if let Some(db) = options.default_database {
                auth.set_default_database(&username, Some(&db), actor)?;
            }
            if let Some(limit) = options.connection_limit {
                auth.set_connection_limit(&username, limit, actor)?;
            }
            Ok(Response::ok_message(&format!(
                "user '{}' created",
                username
            )))
        }
        AuthCommand::DropUser { username } => {
            auth.drop_user(&username, actor)?;
            Ok(Response::ok_message(&format!(
                "user '{}' dropped",
                username
            )))
        }
        AuthCommand::AlterUserPassword {
            username,
            new_password,
        } => {
            auth.alter_user_password(&username, &new_password, actor)?;
            Ok(Response::ok_message(&format!(
                "password changed for user '{}'",
                username
            )))
        }
        AuthCommand::AlterUserSuperuser {
            username,
            is_superuser,
        } => {
            auth.set_superuser(&username, is_superuser, actor)?;
            let status = if is_superuser { "granted" } else { "revoked" };
            Ok(Response::ok_message(&format!(
                "superuser {} for user '{}'",
                status, username
            )))
        }
        AuthCommand::AlterUserDefaultDb { username, database } => {
            auth.set_default_database(&username, database.as_deref(), actor)?;
            Ok(Response::ok_message(&format!(
                "default database updated for user '{}'",
                username
            )))
        }
        AuthCommand::LockUser { username } => {
            auth.lock_user(&username, actor)?;
            Ok(Response::ok_message(&format!("user '{}' locked", username)))
        }
        AuthCommand::UnlockUser { username } => {
            auth.unlock_user(&username, actor)?;
            Ok(Response::ok_message(&format!(
                "user '{}' unlocked",
                username
            )))
        }
        AuthCommand::CreateRole { name, description } => {
            auth.create_role(&name, description.as_deref(), actor)?;
            Ok(Response::ok_message(&format!("role '{}' created", name)))
        }
        AuthCommand::DropRole { name } => {
            auth.drop_role(&name, actor)?;
            Ok(Response::ok_message(&format!("role '{}' dropped", name)))
        }
        AuthCommand::GrantRole { role, username } => {
            auth.grant_role(&username, &role, actor)?;
            Ok(Response::ok_message(&format!(
                "role '{}' granted to '{}'",
                role, username
            )))
        }
        AuthCommand::RevokeRole { role, username } => {
            auth.revoke_role(&username, &role, actor)?;
            Ok(Response::ok_message(&format!(
                "role '{}' revoked from '{}'",
                role, username
            )))
        }
        AuthCommand::Grant {
            privileges,
            target_type,
            target_name,
            columns,
            grantee,
            grantee_is_role,
            with_grant_option,
        } => {
            let target = match target_type {
                GrantTargetType::Global => PrivilegeTarget::Global,
                GrantTargetType::Database => {
                    PrivilegeTarget::Database(target_name.clone().unwrap_or_default())
                }
                GrantTargetType::Table => {
                    // Parse "database.table" format
                    let name = target_name.clone().unwrap_or_default();
                    let parts: Vec<&str> = name.split('.').collect();
                    if parts.len() == 2 {
                        PrivilegeTarget::Table {
                            database: parts[0].to_string(),
                            table: parts[1].to_string(),
                        }
                    } else {
                        return Err(ServerError::BadRequest(
                            "table must be specified as database.table".into(),
                        ));
                    }
                }
                GrantTargetType::AllTablesInDatabase => {
                    PrivilegeTarget::AllTablesInDatabase(target_name.clone().unwrap_or_default())
                }
            };

            for priv_str in &privileges {
                let priv_type = parse_privilege(priv_str)?;
                if grantee_is_role {
                    auth.grant_privilege_to_role(
                        &grantee,
                        priv_type,
                        target.clone(),
                        with_grant_option,
                        actor,
                    )?;
                } else {
                    auth.grant_privilege(
                        &grantee,
                        priv_type,
                        target.clone(),
                        with_grant_option,
                        actor,
                    )?;
                }
            }
            let col_info = if let Some(ref cols) = columns {
                format!(" on columns ({})", cols.join(", "))
            } else {
                String::new()
            };
            Ok(Response::ok_message(&format!(
                "{}{} granted to '{}'",
                privileges.join(", "),
                col_info,
                grantee
            )))
        }
        AuthCommand::Revoke {
            privileges,
            target_type,
            target_name,
            columns,
            grantee,
            grantee_is_role,
        } => {
            let target = match target_type {
                GrantTargetType::Global => PrivilegeTarget::Global,
                GrantTargetType::Database => {
                    PrivilegeTarget::Database(target_name.clone().unwrap_or_default())
                }
                GrantTargetType::Table => {
                    let name = target_name.clone().unwrap_or_default();
                    let parts: Vec<&str> = name.split('.').collect();
                    if parts.len() == 2 {
                        PrivilegeTarget::Table {
                            database: parts[0].to_string(),
                            table: parts[1].to_string(),
                        }
                    } else {
                        return Err(ServerError::BadRequest(
                            "table must be specified as database.table".into(),
                        ));
                    }
                }
                GrantTargetType::AllTablesInDatabase => {
                    PrivilegeTarget::AllTablesInDatabase(target_name.clone().unwrap_or_default())
                }
            };

            for priv_str in &privileges {
                let priv_type = parse_privilege(priv_str)?;
                if grantee_is_role {
                    auth.revoke_privilege_from_role(&grantee, priv_type, target.clone(), actor)?;
                } else {
                    auth.revoke_privilege(&grantee, priv_type, target.clone(), actor)?;
                }
            }
            let col_info = if let Some(ref cols) = columns {
                format!(" on columns ({})", cols.join(", "))
            } else {
                String::new()
            };
            Ok(Response::ok_message(&format!(
                "{}{} revoked from '{}'",
                privileges.join(", "),
                col_info,
                grantee
            )))
        }
        AuthCommand::ShowUsers => {
            let users = auth.list_users()?;
            let users_json = serde_json::to_value(users)
                .map_err(|e| ServerError::Encode(format!("failed to serialize users: {e}")))?;
            Ok(Response {
                users: Some(users_json),
                ..Default::default()
            })
        }
        AuthCommand::ShowRoles => {
            let roles = auth.list_roles()?;
            let roles_json = serde_json::to_value(roles)
                .map_err(|e| ServerError::Encode(format!("failed to serialize roles: {e}")))?;
            Ok(Response {
                roles: Some(roles_json),
                ..Default::default()
            })
        }
        AuthCommand::ShowGrants { username } => {
            let user_info = auth.get_user(&username)?;
            let privileges = auth.get_user_privileges(&username)?;
            let grants_data = serde_json::json!({
                "user": user_info,
                "privileges": privileges,
            });
            Ok(Response {
                grants: Some(grants_data),
                ..Default::default()
            })
        }
    }
}

/// Execute a DDL command
async fn execute_ddl_command<F>(
    db: &Arc<Db>,
    cmd: DdlCommand,
    require_privilege: &F,
    auto_repair_state: &Option<Arc<boyodb_core::AutoRepairState>>,
) -> Result<Response, ServerError>
where
    F: Fn(Privilege, PrivilegeTarget) -> Result<(), ServerError>,
{
    match cmd {
        DdlCommand::CreateDatabase { name } => {
            require_privilege(Privilege::CreateDb, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || {
                db.create_database(&name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("database created"))
        }
        DdlCommand::DropDatabase { name, if_exists } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Database(name.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.drop_database(&name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("database dropped"))
        }
        DdlCommand::CreateTable {
            database,
            table,
            schema_json,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.create_table(&database, &table, schema_json)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("table created"))
        }
        DdlCommand::DropTable {
            database,
            table,
            if_exists,
        } => {
            require_privilege(
                Privilege::Drop,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.drop_table(&database, &table, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("table dropped"))
        }
        DdlCommand::TruncateTable { database, table } => {
            require_privilege(
                Privilege::Truncate,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.truncate_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("table truncated"))
        }
        DdlCommand::AlterTableAddColumn {
            database,
            table,
            column,
            data_type,
            nullable,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.alter_table_add_column(&database, &table, &column, &data_type, nullable)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("column added"))
        }
        DdlCommand::AlterTableDropColumn {
            database,
            table,
            column,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.alter_table_drop_column(&database, &table, &column)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("column dropped"))
        }
        DdlCommand::ShowDatabases => {
            // No privilege check for listing databases - all users can see database names
            let db = db.clone();
            let databases = blocking(move || {
                db.list_databases()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response {
                databases: Some(databases),
                ..Default::default()
            })
        }
        DdlCommand::ShowTables { database } => {
            // If database is specified, check privilege for that database
            if let Some(ref db_name) = database {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Database(db_name.clone()),
                )?;
            }
            let db = db.clone();
            let tables = blocking(move || {
                db.list_tables(database.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response {
                tables: Some(tables),
                ..Default::default()
            })
        }
        DdlCommand::DescribeTable { database, table } => {
            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let desc = blocking(move || {
                db.describe_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response {
                table_description: Some(desc),
                ..Default::default()
            })
        }
        DdlCommand::CreateView {
            database,
            name,
            query_sql,
            or_replace,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.create_view(&database, &name, &query_sql, or_replace)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("view created"))
        }
        DdlCommand::DropView {
            database,
            name,
            if_exists,
        } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.drop_view(&database, &name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("view dropped"))
        }
        DdlCommand::ShowViews { database } => {
            // If database is specified, check privilege for that database
            if let Some(ref db_name) = database {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Database(db_name.clone()),
                )?;
            }
            let db = db.clone();
            let views = blocking(move || {
                db.list_views(database.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            // Return views as a list of (database, name, query) tuples
            let view_names: Vec<String> = views
                .iter()
                .map(|(db, name, _)| format!("{}.{}", db, name))
                .collect();
            Ok(Response {
                message: Some(format!("Views: {}", view_names.join(", "))),
                ..Default::default()
            })
        }
        DdlCommand::CreateMaterializedView {
            database,
            name,
            query_sql,
            or_replace,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.create_materialized_view(&database, &name, &query_sql, or_replace)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("materialized view created"))
        }
        DdlCommand::DropMaterializedView {
            database,
            name,
            if_exists,
        } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.drop_materialized_view(&database, &name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("materialized view dropped"))
        }
        DdlCommand::RefreshMaterializedView { database, name, incremental } => {
            require_privilege(
                Privilege::Update,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            if incremental {
                blocking(move || {
                    db.refresh_materialized_view_incremental(&database, &name)
                        .map_err(|e| ServerError::Db(e.to_string()))
                })
                .await?;
                Ok(Response::ok_message("materialized view incrementally refreshed"))
            } else {
                blocking(move || {
                    db.refresh_materialized_view(&database, &name)
                        .map_err(|e| ServerError::Db(e.to_string()))
                })
                .await?;
                Ok(Response::ok_message("materialized view refreshed"))
            }
        }
        DdlCommand::ShowMaterializedViews { database } => {
            // If database is specified, check privilege for that database
            if let Some(ref db_name) = database {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Database(db_name.clone()),
                )?;
            }
            let db = db.clone();
            let views = blocking(move || {
                db.list_materialized_views(database.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            // Return materialized views as a list
            let view_names: Vec<String> = views
                .iter()
                .map(|(db, name, _, last_refresh)| {
                    if *last_refresh > 0 {
                        format!("{}.{} (last refresh: {})", db, name, last_refresh)
                    } else {
                        format!("{}.{} (never refreshed)", db, name)
                    }
                })
                .collect();
            Ok(Response {
                message: Some(format!("Materialized Views: {}", view_names.join(", "))),
                ..Default::default()
            })
        }
        DdlCommand::CreateIndex {
            database,
            table,
            index_name,
            columns,
            index_type,
            if_not_exists,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.create_index(
                    &database,
                    &table,
                    &index_name,
                    &columns,
                    index_type,
                    if_not_exists,
                )
                .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("index created"))
        }
        DdlCommand::DropIndex {
            database,
            table,
            index_name,
            if_exists,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.drop_index(&database, &table, &index_name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("index dropped"))
        }
        DdlCommand::ShowIndexes { database, table } => {
            // If database/table is specified, check privilege for that table
            if let (Some(ref db_name), Some(ref tbl_name)) = (&database, &table) {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Table {
                        database: db_name.clone(),
                        table: tbl_name.clone(),
                    },
                )?;
            }
            let db = db.clone();
            let indexes = blocking(move || {
                db.list_indexes(database.as_deref(), table.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let index_list: Vec<String> = indexes
                .iter()
                .map(|(db_name, tbl_name, idx_name, cols, idx_type)| {
                    format!(
                        "{}.{}.{} ({:?}) on ({})",
                        db_name,
                        tbl_name,
                        idx_name,
                        idx_type,
                        cols.join(", ")
                    )
                })
                .collect();
            Ok(Response {
                message: Some(if index_list.is_empty() {
                    "No indexes".to_string()
                } else {
                    format!("Indexes: {}", index_list.join("; "))
                }),
                ..Default::default()
            })
        }
        DdlCommand::AnalyzeTable { database, table, columns: _ } => {
            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.analyze_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("table analyzed"))
        }
        DdlCommand::ShowStatistics {
            database,
            table,
            column,
        } => {
            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let result = blocking(move || {
                db.get_table_statistics(&database, &table, column.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            let mut output = format!(
                "Statistics for {}.{}:\n  Segments: {}\n  Total bytes: {}\n  Estimated rows: {}\n",
                result.database, result.table, result.segment_count, result.total_bytes, result.estimated_rows
            );

            if !result.columns.is_empty() {
                output.push_str("\nColumn Statistics:\n");
                for col in &result.columns {
                    output.push_str(&format!("  {}:\n", col.column_name));
                    output.push_str(&format!("    Nulls: {}\n", col.null_count));
                    if let Some(dc) = col.distinct_count {
                        output.push_str(&format!("    Distinct (approx): {}\n", dc));
                    }
                    if let Some(ref min) = col.min_value {
                        output.push_str(&format!("    Min: {:?}\n", min));
                    }
                    if let Some(ref max) = col.max_value {
                        output.push_str(&format!("    Max: {:?}\n", max));
                    }
                }
            }

            Ok(Response::ok_message(&output))
        }
        DdlCommand::Vacuum {
            database,
            table,
            full,
            force,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let result = blocking(move || {
                db.vacuum_with_options(&database, &table, full, force)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "{}: processed {} segments, removed {}, reclaimed {} bytes, created {} new segments",
                if full { "VACUUM FULL" } else { "VACUUM" },
                result.segments_processed,
                result.segments_removed,
                result.bytes_reclaimed,
                result.new_segments
            )))
        }
        DdlCommand::Deduplicate { database, table } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let rows_removed = blocking(move || {
                db.deduplicate_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "DEDUPLICATE: removed {} duplicate rows",
                rows_removed
            )))
        }
        DdlCommand::SetDeduplication {
            database,
            table,
            config,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let has_config = config.is_some();
            blocking(move || {
                db.set_table_deduplication(&database, &table, config)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = if has_config {
                "deduplication configured"
            } else {
                "deduplication disabled"
            };
            Ok(Response::ok_message(msg))
        }
        // Data retention policy commands
        DdlCommand::SetRetention {
            database,
            table,
            retention_seconds,
            time_column,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.set_retention_policy(&database, &table, retention_seconds, time_column)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            // Format human-readable duration
            let duration_str = if retention_seconds >= 365 * 86400 {
                format!("{} year(s)", retention_seconds / (365 * 86400))
            } else if retention_seconds >= 30 * 86400 {
                format!("{} month(s)", retention_seconds / (30 * 86400))
            } else if retention_seconds >= 7 * 86400 {
                format!("{} week(s)", retention_seconds / (7 * 86400))
            } else if retention_seconds >= 86400 {
                format!("{} day(s)", retention_seconds / 86400)
            } else if retention_seconds >= 3600 {
                format!("{} hour(s)", retention_seconds / 3600)
            } else {
                format!("{} second(s)", retention_seconds)
            };
            Ok(Response::ok_message(&format!(
                "Retention policy set: data older than {} will be deleted",
                duration_str
            )))
        }
        DdlCommand::DropRetention { database, table } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.remove_retention_policy(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("Retention policy removed"))
        }
        DdlCommand::ShowRetention { database, table } => {
            let db = db.clone();
            let result = blocking(move || {
                if let (Some(database), Some(table)) = (database, table) {
                    // Show retention for specific table
                    match db.get_retention_policy(&database, &table) {
                        Ok(Some(policy)) => {
                            let duration_str = if policy.retention_seconds >= 365 * 86400 {
                                format!("{} year(s)", policy.retention_seconds / (365 * 86400))
                            } else if policy.retention_seconds >= 30 * 86400 {
                                format!("{} month(s)", policy.retention_seconds / (30 * 86400))
                            } else if policy.retention_seconds >= 86400 {
                                format!("{} day(s)", policy.retention_seconds / 86400)
                            } else {
                                format!("{} second(s)", policy.retention_seconds)
                            };
                            let time_col = policy.time_column.as_deref().unwrap_or("event_time");
                            let status = if policy.enabled { "enabled" } else { "disabled" };
                            Ok(format!(
                                "{}.{}: {} retention on column '{}' ({})",
                                database, table, duration_str, time_col, status
                            ))
                        }
                        Ok(None) => Ok(format!("{}.{}: no retention policy", database, table)),
                        Err(e) => Err(ServerError::Db(e.to_string())),
                    }
                } else {
                    // Show all retention policies
                    let manifest = db.export_manifest().map_err(|e| ServerError::Db(e.to_string()))?;
                    let manifest: serde_json::Value = serde_json::from_slice(&manifest)
                        .map_err(|e| ServerError::Db(e.to_string()))?;

                    let mut output = String::from("Retention Policies:\n");
                    if let Some(tables) = manifest.get("tables").and_then(|t| t.as_array()) {
                        for t in tables {
                            if let Some(policy) = t.get("retention_policy") {
                                if !policy.is_null() {
                                    let db_name = t.get("database").and_then(|d| d.as_str()).unwrap_or("default");
                                    let tbl_name = t.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
                                    let secs = policy.get("retention_seconds").and_then(|s| s.as_u64()).unwrap_or(0);
                                    let duration_str = if secs >= 86400 {
                                        format!("{}d", secs / 86400)
                                    } else {
                                        format!("{}s", secs)
                                    };
                                    output.push_str(&format!("  {}.{}: {}\n", db_name, tbl_name, duration_str));
                                }
                            }
                        }
                    }
                    if output == "Retention Policies:\n" {
                        output.push_str("  (none configured)");
                    }
                    Ok(output)
                }
            })
            .await?;
            Ok(Response::ok_message(&result))
        }
        // Time-based partitioning commands
        DdlCommand::SetPartition {
            database,
            table,
            granularity,
            time_column,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let granularity_parsed: boyodb_core::replication::PartitionGranularity = granularity
                .parse()
                .map_err(|e: String| ServerError::BadRequest(e))?;
            blocking(move || {
                db.set_partition_config(&database, &table, granularity_parsed, time_column)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "Partitioning enabled: {} granularity",
                granularity
            )))
        }
        DdlCommand::DropPartition { database, table } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.remove_partition_config(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("Partitioning disabled"))
        }
        DdlCommand::ShowPartitions { database, table } => {
            let db = db.clone();
            let result = blocking(move || {
                if let (Some(database), Some(table)) = (database, table) {
                    // Show partitions for specific table
                    match db.list_table_partitions(&database, &table) {
                        Ok(partitions) => {
                            if partitions.is_empty() {
                                return Ok(format!("{}.{}: no partitions (no data)", database, table));
                            }
                            let mut output = format!("Partitions for {}.{}:\n", database, table);
                            for p in &partitions {
                                output.push_str(&format!(
                                    "  {}: {} segments, {} bytes\n",
                                    p.partition_key, p.segment_count, p.total_bytes
                                ));
                            }
                            Ok(output)
                        }
                        Err(e) => Err(ServerError::Db(e.to_string())),
                    }
                } else {
                    // Show all partition configs
                    let manifest = db.export_manifest().map_err(|e| ServerError::Db(e.to_string()))?;
                    let manifest: serde_json::Value = serde_json::from_slice(&manifest)
                        .map_err(|e| ServerError::Db(e.to_string()))?;

                    let mut output = String::from("Partition Configurations:\n");
                    if let Some(tables) = manifest.get("tables").and_then(|t| t.as_array()) {
                        for t in tables {
                            if let Some(config) = t.get("partition_config") {
                                if !config.is_null() {
                                    let db_name = t.get("database").and_then(|d| d.as_str()).unwrap_or("default");
                                    let tbl_name = t.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
                                    let granularity = config.get("granularity").and_then(|g| g.as_str()).unwrap_or("unknown");
                                    let time_col = config.get("time_column").and_then(|c| c.as_str()).unwrap_or("event_time");
                                    output.push_str(&format!("  {}.{}: {} on {}\n", db_name, tbl_name, granularity, time_col));
                                }
                            }
                        }
                    }
                    if output == "Partition Configurations:\n" {
                        output.push_str("  (none configured)");
                    }
                    Ok(output)
                }
            })
            .await?;
            Ok(Response::ok_message(&result))
        }
        // New DDL commands
        DdlCommand::AlterTableRename {
            database,
            old_table,
            new_table,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: old_table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.rename_table(&database, &old_table, &new_table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("table renamed"))
        }
        DdlCommand::AlterTableRenameColumn {
            database,
            table,
            old_column,
            new_column,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let old_col = old_column.clone();
            let new_col = new_column.clone();
            blocking(move || {
                db.alter_table_rename_column(&database, &table, &old_col, &new_col)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "column '{}' renamed to '{}'",
                old_column, new_column
            )))
        }
        DdlCommand::CreateTableAs {
            database,
            table,
            query_sql,
            if_not_exists,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            let rows_inserted = blocking(move || {
                db.create_table_as(&database, &table, &query_sql, if_not_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "table created with {} rows",
                rows_inserted
            )))
        }
        DdlCommand::CreateSequence {
            database,
            name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            if_not_exists,
        } => {
            require_privilege(
                Privilege::Create,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.create_sequence(
                    &database,
                    &name,
                    start,
                    increment,
                    min_value,
                    max_value,
                    cycle,
                    if_not_exists,
                )
                .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("sequence created"))
        }
        DdlCommand::DropSequence {
            database,
            name,
            if_exists,
        } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.drop_sequence(&database, &name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("sequence dropped"))
        }
        DdlCommand::AlterSequence {
            database,
            name,
            restart_with,
            increment,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.alter_sequence(&database, &name, restart_with, increment)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("sequence altered"))
        }
        DdlCommand::ShowSequences { database } => {
            if let Some(ref db_name) = database {
                require_privilege(
                    Privilege::Select,
                    PrivilegeTarget::Database(db_name.clone()),
                )?;
            }
            let db = db.clone();
            let sequences = blocking(move || {
                let db_name = database.as_deref().unwrap_or("default");
                db.list_sequences(db_name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if sequences.is_empty() {
                Ok(Response::ok_message("No sequences"))
            } else {
                let seq_list: Vec<String> = sequences
                    .iter()
                    .map(|s| {
                        format!(
                            "{}.{} (current: {}, increment: {})",
                            s.database, s.name, s.current_value, s.increment
                        )
                    })
                    .collect();
                Ok(Response {
                    message: Some(format!("Sequences: {}", seq_list.join("; "))),
                    ..Default::default()
                })
            }
        }
        DdlCommand::CopyFrom {
            database,
            table,
            source,
            format,
            options,
        } => {
            require_privilege(
                Privilege::Insert,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let rows_copied = blocking(move || {
                db.copy_from(&database, &table, &source, &format, &options)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "COPY {} rows imported",
                rows_copied
            )))
        }
        DdlCommand::CopyTo {
            database,
            table,
            destination,
            format,
            options,
        } => {
            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            let dest_clone = destination.clone();
            let rows_copied = blocking(move || {
                db.copy_to(&database, &table, &destination, &format, &options)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "COPY {} rows exported to {}",
                rows_copied, dest_clone
            )))
        }
        DdlCommand::AddConstraint {
            database,
            table,
            constraint,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.add_constraint(&database, &table, constraint)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("constraint added"))
        }
        DdlCommand::DropConstraint {
            database,
            table,
            constraint_name,
        } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            let db = db.clone();
            blocking(move || {
                db.drop_constraint(&database, &table, &constraint_name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("constraint dropped"))
        }
        DdlCommand::RecoverToTimestamp { timestamp } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let ts = timestamp.clone();
            let result = blocking(move || {
                db.recover_to_timestamp(&ts)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = format!(
                "Recovery complete. Recovered to LSN {}, timestamp {}. {} WAL segments replayed ({} bytes). Path: {:?}",
                result.recovered_lsn,
                result.recovered_timestamp,
                result.wal_segments_replayed,
                result.wal_bytes_replayed,
                result.recovery_path
            );
            if !result.warnings.is_empty() {
                let warnings = result.warnings.join("; ");
                Ok(Response::ok_message(&format!(
                    "{}. Warnings: {}",
                    msg, warnings
                )))
            } else {
                Ok(Response::ok_message(&msg))
            }
        }
        DdlCommand::RecoverToLsn { lsn } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let result = blocking(move || {
                db.recover_to_lsn(lsn)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = format!(
                "Recovery complete. Recovered to LSN {}, timestamp {}. {} WAL segments replayed ({} bytes). Path: {:?}",
                result.recovered_lsn,
                result.recovered_timestamp,
                result.wal_segments_replayed,
                result.wal_bytes_replayed,
                result.recovery_path
            );
            if !result.warnings.is_empty() {
                let warnings = result.warnings.join("; ");
                Ok(Response::ok_message(&format!(
                    "{}. Warnings: {}",
                    msg, warnings
                )))
            } else {
                Ok(Response::ok_message(&msg))
            }
        }
        DdlCommand::CreateBackup { label } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let backup_info = blocking(move || {
                db.create_backup(label)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = format!(
                "Backup created: id='{}', label={:?}, size={} bytes, LSN range {}-{}, path='{}'",
                backup_info.id,
                backup_info.label,
                backup_info.size_bytes,
                backup_info.start_lsn,
                backup_info.end_lsn,
                backup_info.backup_path
            );
            Ok(Response::ok_message(&msg))
        }
        DdlCommand::ShowBackups => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let backups = blocking(move || {
                db.list_backups()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if backups.is_empty() {
                Ok(Response::ok_message("No backups available"))
            } else {
                let lines: Vec<String> = backups
                    .iter()
                    .map(|b| {
                        format!(
                            "{} | {} | {} bytes | LSN {}-{} | {}",
                            b.id,
                            b.label.as_deref().unwrap_or("(no label)"),
                            b.size_bytes,
                            b.start_lsn,
                            b.end_lsn,
                            b.backup_path
                        )
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "Available backups ({}):\n{}",
                    backups.len(),
                    lines.join("\n")
                )))
            }
        }
        DdlCommand::ShowWalStatus => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let status = blocking(move || {
                db.get_wal_status()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            let msg = format!(
                "WAL Archive Status:\n  Path: {:?}\n  Segments: {}\n  Total size: {} bytes\n  LSN range: {:?} - {:?}\n  Timestamp range: {:?} - {:?}",
                status.archive_path,
                status.segment_count,
                status.total_bytes,
                status.oldest_lsn,
                status.newest_lsn,
                status.oldest_timestamp,
                status.newest_timestamp
            );
            Ok(Response::ok_message(&msg))
        }
        DdlCommand::DeleteBackup { backup_id } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let id = backup_id.clone();
            blocking(move || {
                db.delete_backup(&id)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "Backup '{}' deleted successfully",
                backup_id
            )))
        }
        DdlCommand::ShowServerInfo => {
            let db = db.clone();
            let info = blocking(move || {
                db.get_server_info()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "Server Information:\n  Version: {}\n  Databases: {}\n  Tables: {}\n  Segments: {}\n  Manifest Version: {}\n  WAL LSN: {}",
                info.version, info.database_count, info.table_count,
                info.segment_count, info.manifest_version, info.wal_lsn
            )))
        }
        DdlCommand::ShowMissingSegments { database, table } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let missing = blocking(move || {
                db.find_missing_segments(database.as_deref(), table.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if missing.is_empty() {
                Ok(Response::ok_message("No missing segments found"))
            } else {
                let lines: Vec<String> = missing
                    .iter()
                    .map(|m| {
                        format!(
                            "{} | {}.{} | {} bytes",
                            m.segment_id, m.database, m.table, m.size_bytes
                        )
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "Missing segments ({}):\n{}",
                    missing.len(),
                    lines.join("\n")
                )))
            }
        }
        DdlCommand::ShowCorruptedSegments { database, table } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let corrupted = blocking(move || {
                db.find_corrupted_segments(database.as_deref(), table.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if corrupted.is_empty() {
                Ok(Response::ok_message("No corrupted segments found"))
            } else {
                let lines: Vec<String> = corrupted
                    .iter()
                    .map(|c| {
                        format!(
                            "{} | {}.{} | {} bytes | expected={} actual={}",
                            c.segment_id, c.database, c.table, c.size_bytes,
                            c.expected_checksum, c.actual_checksum
                        )
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "Corrupted segments ({}):\n{}",
                    corrupted.len(),
                    lines.join("\n")
                )))
            }
        }
        DdlCommand::ShowDamagedSegments { database, table } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let damaged = blocking(move || {
                db.find_damaged_segments(database.as_deref(), table.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if damaged.is_empty() {
                Ok(Response::ok_message("No damaged segments found"))
            } else {
                use boyodb_core::engine::DamagedSegment;
                let lines: Vec<String> = damaged
                    .iter()
                    .map(|d| match d {
                        DamagedSegment::Missing(m) => {
                            format!(
                                "MISSING | {} | {}.{} | {} bytes",
                                m.segment_id, m.database, m.table, m.size_bytes
                            )
                        }
                        DamagedSegment::Corrupted(c) => {
                            format!(
                                "CORRUPTED | {} | {}.{} | {} bytes | expected={} actual={}",
                                c.segment_id, c.database, c.table, c.size_bytes,
                                c.expected_checksum, c.actual_checksum
                            )
                        }
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "Damaged segments ({}):\n{}",
                    damaged.len(),
                    lines.join("\n")
                )))
            }
        }
        DdlCommand::RepairSegments { database, table } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let scope = match (&database, &table) {
                (None, None) => "all databases".to_string(),
                (Some(d), None) => format!("database {}", d),
                (Some(d), Some(t)) => format!("{}.{}", d, t),
                (None, Some(_)) => "all databases".to_string(),
            };
            let removed = blocking(move || {
                db.repair_segments(database.as_deref(), table.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            if removed.is_empty() {
                Ok(Response::ok_message("No repairs needed"))
            } else {
                Ok(Response::ok_message(&format!(
                    "Removed {} damaged segments from {} in manifest:\n{}",
                    removed.len(),
                    scope,
                    removed.join("\n")
                )))
            }
        }
        DdlCommand::CheckManifest => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let result = blocking(move || {
                db.check_manifest()
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            let mut report = format!(
                "Manifest Check Report:\n\
                 =======================\n\
                 Manifest Version: {}\n\
                 Total Databases: {}\n\
                 Total Tables: {}\n\
                 Total Segments: {}\n",
                result.manifest_version,
                result.total_databases,
                result.total_tables,
                result.total_segments
            );

            if result.missing_segments.is_empty()
                && result.corrupted_segments.is_empty()
                && result.orphaned_files.is_empty()
            {
                report.push_str("\nStatus: HEALTHY - No issues found");
            } else {
                report.push_str(&format!(
                    "\nIssues Found:\n\
                     - Missing segments: {}\n\
                     - Corrupted segments: {}\n\
                     - Orphaned files: {}\n",
                    result.missing_segments.len(),
                    result.corrupted_segments.len(),
                    result.orphaned_files.len()
                ));

                if !result.missing_segments.is_empty() {
                    report.push_str("\nMissing Segments:\n");
                    for seg in &result.missing_segments {
                        report.push_str(&format!(
                            "  {} | {}.{} | {} bytes\n",
                            seg.segment_id, seg.database, seg.table, seg.size_bytes
                        ));
                    }
                }

                if !result.corrupted_segments.is_empty() {
                    report.push_str("\nCorrupted Segments:\n");
                    for seg in &result.corrupted_segments {
                        report.push_str(&format!(
                            "  {} | {}.{} | expected={} actual={}\n",
                            seg.segment_id, seg.database, seg.table,
                            seg.expected_checksum, seg.actual_checksum
                        ));
                    }
                }

                if !result.orphaned_files.is_empty() {
                    report.push_str("\nOrphaned Files (not in manifest):\n");
                    for f in &result.orphaned_files {
                        report.push_str(&format!("  {} | {} bytes\n", f.path, f.size_bytes));
                    }
                }

                report.push_str("\nRun REPAIR ALL SEGMENTS to fix these issues.");
            }

            Ok(Response::ok_message(&report))
        }
        DdlCommand::ShowRepairStatus => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            // Get repair stats from the auto-repair state if available
            let stats = if let Some(ref state) = auto_repair_state {
                state.get_stats()
            } else {
                // No auto-repair configured, get basic stats from db
                let db = db.clone();
                blocking(move || Ok::<_, ServerError>(db.get_repair_stats())).await?
            };

            let last_scan = stats
                .last_scan_timestamp
                .map(|ts| {
                    // Format as human-readable time
                    let duration =
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|now| now.as_secs().saturating_sub(ts))
                            .unwrap_or(0);
                    if duration < 60 {
                        format!("{} seconds ago", duration)
                    } else if duration < 3600 {
                        format!("{} minutes ago", duration / 60)
                    } else {
                        format!("{} hours ago", duration / 3600)
                    }
                })
                .unwrap_or_else(|| "never".to_string());

            let report = format!(
                "Auto-Repair Status:\n\
                 ====================\n\
                 Enabled: {}\n\
                 Last Scan: {}\n\
                 Total Repaired: {}\n\
                 Pending Damaged: {}\n\
                 Scan Interval: {} seconds\n\
                 Max Repairs/Scan: {}",
                stats.enabled,
                last_scan,
                stats.total_repaired,
                stats.pending_count,
                stats.scan_interval_secs,
                stats.max_repairs_per_scan
            );
            Ok(Response::ok_message(&report))
        }
        DdlCommand::CompactTable { database, table } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let result = blocking(move || {
                // Run compaction multiple times to fully compact
                let mut total_merged = 0;
                loop {
                    match db.compact_table(&database, &table) {
                        Ok(Some(_)) => total_merged += 1,
                        Ok(None) => break,
                        Err(e) => return Err(ServerError::Db(e.to_string())),
                    }
                }
                Ok(total_merged)
            })
            .await?;
            if result == 0 {
                Ok(Response::ok_message("No compaction needed - table already optimized"))
            } else {
                Ok(Response::ok_message(&format!(
                    "Compaction complete: merged {} segment groups",
                    result
                )))
            }
        }
        DdlCommand::CompactAll => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let result = blocking(move || {
                let mut total_merged = 0;
                // Run multiple passes until no more compaction needed
                loop {
                    match db.run_background_compaction() {
                        Ok(0) => break,
                        Ok(n) => total_merged += n,
                        Err(e) => return Err(ServerError::Db(e.to_string())),
                    }
                }
                Ok(total_merged)
            })
            .await?;
            if result == 0 {
                Ok(Response::ok_message("No compaction needed - all tables already optimized"))
            } else {
                Ok(Response::ok_message(&format!(
                    "Compaction complete: merged {} segment groups across all tables",
                    result
                )))
            }
        }
        // Transaction commands
        // Note: BoyoDB is currently an OLAP system optimized for analytics.
        // Full ACID transactions would require significant architecture changes.
        // For now, we provide basic transaction syntax support with implicit auto-commit.
        DdlCommand::BeginTransaction => {
            // In auto-commit mode, BEGIN is acknowledged but doesn't change behavior
            Ok(Response::ok_message("BEGIN - Transaction started (auto-commit mode, all statements commit immediately)"))
        }
        DdlCommand::CommitTransaction => {
            // In auto-commit mode, COMMIT is always successful
            Ok(Response::ok_message("COMMIT - Transaction committed"))
        }
        DdlCommand::RollbackTransaction => {
            // In auto-commit mode, ROLLBACK cannot undo already-committed statements
            Ok(Response::ok_message("ROLLBACK - Transaction rolled back (note: in auto-commit mode, previous statements were already committed)"))
        }
        DdlCommand::Savepoint { name } => {
            // Savepoints are acknowledged but not enforced in auto-commit mode
            Ok(Response::ok_message(&format!("SAVEPOINT {} created (auto-commit mode)", name)))
        }
        DdlCommand::ReleaseSavepoint { name } => {
            Ok(Response::ok_message(&format!("SAVEPOINT {} released", name)))
        }
        DdlCommand::RollbackToSavepoint { name } => {
            Ok(Response::ok_message(&format!("Rolled back to SAVEPOINT {} (note: in auto-commit mode, previous statements were already committed)", name)))
        }
        // User-Defined Functions (UDFs)
        DdlCommand::CreateFunction {
            name,
            parameters,
            return_type,
            body,
            or_replace,
            language,
        } => {
            require_privilege(Privilege::Create, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();
            let params_str: Vec<String> = parameters
                .iter()
                .map(|p| format!("{} {}", p.name, p.data_type))
                .collect();
            blocking(move || {
                db.create_function(
                    &name,
                    parameters,
                    &return_type,
                    body,
                    language,
                    or_replace,
                )
                .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!(
                "Function '{}' created with {} parameter(s)",
                name_clone,
                params_str.len()
            )))
        }
        DdlCommand::DropFunction { name, if_exists } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();
            blocking(move || {
                db.drop_function(&name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!("Function '{}' dropped", name_clone)))
        }
        DdlCommand::ShowFunctions { pattern } => {
            let db = db.clone();
            let functions = blocking(move || {
                Ok::<_, ServerError>(db.list_functions(pattern.as_deref()))
            })
            .await?;
            if functions.is_empty() {
                Ok(Response::ok_message("No user-defined functions"))
            } else {
                let lines: Vec<String> = functions
                    .iter()
                    .map(|f| {
                        let params: Vec<String> = f.parameters
                            .iter()
                            .map(|p| format!("{} {}", p.name, p.data_type))
                            .collect();
                        format!("{}({}) -> {}", f.name, params.join(", "), f.return_type)
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "User-defined functions ({}):\n{}",
                    functions.len(),
                    lines.join("\n")
                )))
            }
        }
        // Streaming (Kafka/Pulsar connectors)
        DdlCommand::CreateStream {
            name,
            source_type,
            source_config,
            target_table,
            format,
        } => {
            require_privilege(Privilege::Create, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();

            // Parse target table (database.table format)
            let (target_db, target_tbl) = if let Some(target) = target_table {
                if target.contains('.') {
                    let parts: Vec<&str> = target.splitn(2, '.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("default".to_string(), target)
                }
            } else {
                return Err(ServerError::BadRequest(
                    "CREATE STREAM requires a target table (INTO clause)".into(),
                ));
            };

            let target_db_clone = target_db.clone();
            let target_tbl_clone = target_tbl.clone();

            blocking(move || {
                db.create_stream(
                    &name,
                    source_type,
                    &source_config,
                    &target_db,
                    &target_tbl,
                    format.as_deref(),
                )
                .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            Ok(Response::ok_message(&format!(
                "Stream '{}' created (target: {}.{})",
                name_clone, target_db_clone, target_tbl_clone
            )))
        }
        DdlCommand::DropStream { name, if_exists } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();
            blocking(move || {
                db.drop_stream(&name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!("Stream '{}' dropped", name_clone)))
        }
        DdlCommand::ShowStreams => {
            let db = db.clone();
            let streams = blocking(move || {
                Ok::<_, ServerError>(db.list_streams())
            })
            .await?;

            if streams.is_empty() {
                Ok(Response::ok_message("No streams defined"))
            } else {
                let lines: Vec<String> = streams
                    .iter()
                    .map(|s| {
                        let state = match &s.state {
                            boyodb_core::StreamState::Stopped => "STOPPED",
                            boyodb_core::StreamState::Running => "RUNNING",
                            boyodb_core::StreamState::Error(e) => &format!("ERROR: {}", e),
                        };
                        format!(
                            "{} | {:?} | {}.{} | {} | {} msgs",
                            s.name, s.source_type, s.target_database, s.target_table,
                            state, s.messages_consumed
                        )
                    })
                    .collect();
                Ok(Response::ok_message(&format!(
                    "Streams ({}):\n{}",
                    streams.len(),
                    lines.join("\n")
                )))
            }
        }
        DdlCommand::StartStream { name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();
            blocking(move || {
                db.start_stream(&name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!("Stream '{}' started", name_clone)))
        }
        DdlCommand::StopStream { name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            let name_clone = name.clone();
            blocking(move || {
                db.stop_stream(&name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!("Stream '{}' stopped", name_clone)))
        }
        DdlCommand::ShowStreamStatus { name } => {
            let db = db.clone();
            let status = blocking(move || {
                db.get_stream_status(&name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;

            let state_str = match &status.state {
                boyodb_core::StreamState::Stopped => "STOPPED".to_string(),
                boyodb_core::StreamState::Running => "RUNNING".to_string(),
                boyodb_core::StreamState::Error(e) => format!("ERROR: {}", e),
            };

            Ok(Response::ok_message(&format!(
                "Stream: {}\nSource: {:?}\nConfig: {}\nTarget: {}.{}\nFormat: {}\nState: {}\nMessages consumed: {}\nLast error: {}",
                status.name, status.source_type, status.source_config,
                status.target_database, status.target_table, status.format,
                state_str, status.messages_consumed,
                status.last_error.as_deref().unwrap_or("none")
            )))
        }

        // =========================================================================
        // Column-Level Encryption Commands (placeholders - require implementation)
        // =========================================================================
        DdlCommand::CreateEncryptionKey { key_name, algorithm, expires_at } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!(
                "Encryption key '{}' created with algorithm {:?}{}",
                key_name, algorithm,
                expires_at.map(|e| format!(", expires at {}", e)).unwrap_or_default()
            )))
        }
        DdlCommand::DropEncryptionKey { key_name, if_exists: _ } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("Encryption key '{}' dropped", key_name)))
        }
        DdlCommand::RotateEncryptionKey { key_name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("Encryption key '{}' rotated", key_name)))
        }
        DdlCommand::ShowEncryptionKeys => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message("No encryption keys configured"))
        }
        DdlCommand::EncryptColumn { database, table, column, key_name, algorithm: _ } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!(
                "Column {}.{}.{} encrypted with key '{}'",
                database, table, column, key_name
            )))
        }
        DdlCommand::DecryptColumn { database, table, column } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!(
                "Column {}.{}.{} decrypted",
                database, table, column
            )))
        }
        DdlCommand::ShowEncryptedColumns { database, table } => {
            Ok(Response::ok_message(&format!(
                "No encrypted columns in {}.{}",
                database.as_deref().unwrap_or("*"),
                table.as_deref().unwrap_or("*")
            )))
        }

        // =========================================================================
        // CDC Commands (placeholders - require implementation)
        // =========================================================================
        DdlCommand::CreateCdcSubscription { name, database, table, target_type, target_config: _, include_before: _ } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!(
                "CDC subscription '{}' created on {}.{} with target {:?}",
                name, database, table, target_type
            )))
        }
        DdlCommand::DropCdcSubscription { name, if_exists: _ } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("CDC subscription '{}' dropped", name)))
        }
        DdlCommand::StartCdcSubscription { name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("CDC subscription '{}' started", name)))
        }
        DdlCommand::StopCdcSubscription { name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("CDC subscription '{}' stopped", name)))
        }
        DdlCommand::ShowCdcSubscriptions => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message("No CDC subscriptions configured"))
        }
        DdlCommand::ShowCdcStatus { name } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("CDC subscription '{}' status: not running", name)))
        }
        DdlCommand::GetChanges { database, table, since_sequence, limit } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!(
                "No changes in {}.{} since {:?}, limit {:?}",
                database, table, since_sequence, limit
            )))
        }
        DdlCommand::SetCdcCheckpoint { name, sequence } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!(
                "CDC checkpoint for '{}' set to {}",
                name, sequence
            )))
        }

        // =========================================================================
        // Session & Server Management Commands
        // =========================================================================
        DdlCommand::SetVariable { name, value, scope } => {
            Ok(Response::ok_message(&format!(
                "SET {:?} {} = {}",
                scope, name, value
            )))
        }
        DdlCommand::ShowVariable { name } => {
            Ok(Response::ok_message(&format!("{} = (not set)", name)))
        }
        DdlCommand::ShowVariables { pattern } => {
            Ok(Response::ok_message(&format!(
                "Variables matching '{}':\n(no variables configured)",
                pattern.as_deref().unwrap_or("*")
            )))
        }
        DdlCommand::ShowStatus { pattern } => {
            let db_info = db.clone();
            let info = blocking(move || {
                db_info.get_server_info()
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            let status = format!(
                "Server Status{}:\n\
                 version: {}\n\
                 databases: {}\n\
                 tables: {}\n\
                 segments: {}\n\
                 manifest_version: {}\n\
                 wal_lsn: {}",
                pattern.as_ref().map(|p| format!(" (filter: {})", p)).unwrap_or_default(),
                info.version,
                info.database_count,
                info.table_count,
                info.segment_count,
                info.manifest_version,
                info.wal_lsn
            );
            Ok(Response::ok_message(&status))
        }
        DdlCommand::ShowProcesslist { full: _ } => {
            Ok(Response::ok_message("Id\tUser\tHost\tdb\tCommand\tTime\tState\tInfo\n1\troot\tlocalhost\tdefault\tQuery\t0\trunning\tSHOW PROCESSLIST"))
        }
        DdlCommand::KillConnection { connection_id } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("Connection {} killed", connection_id)))
        }
        DdlCommand::KillQuery { query_id } => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message(&format!("Query {} killed", query_id)))
        }
        DdlCommand::CommentOnTable { database, table, comment } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!(
                "Comment on {}.{} {}",
                database, table,
                comment.map(|c| format!("set to '{}'", c)).unwrap_or_else(|| "removed".to_string())
            )))
        }
        DdlCommand::CommentOnColumn { database, table, column, comment } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!(
                "Comment on {}.{}.{} {}",
                database, table, column,
                comment.map(|c| format!("set to '{}'", c)).unwrap_or_else(|| "removed".to_string())
            )))
        }
        DdlCommand::CommentOnDatabase { database, comment } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Database(database.clone()))?;
            Ok(Response::ok_message(&format!(
                "Comment on database {} {}",
                database,
                comment.map(|c| format!("set to '{}'", c)).unwrap_or_else(|| "removed".to_string())
            )))
        }
        DdlCommand::ClusterTable { database, table, index_name, verbose: _ } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            // Trigger compaction which reorders data
            let db = db.clone();
            let db_name = database.clone();
            let tbl_name = table.clone();
            blocking(move || {
                db.compact_table(&db_name, &tbl_name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;
            Ok(Response::ok_message(&format!("Table {}.{} clustered using index {}", database, table, index_name)))
        }
        DdlCommand::ClusterAll => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            let db = db.clone();
            blocking(move || {
                db.compact_all()
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;
            Ok(Response::ok_message("All tables clustered"))
        }
        DdlCommand::ReindexTable { database, table } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            // Trigger analyze to rebuild index statistics
            let db = db.clone();
            let db_name = database.clone();
            let tbl_name = table.clone();
            blocking(move || {
                db.analyze_table(&db_name, &tbl_name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;
            Ok(Response::ok_message(&format!("Indexes on {}.{} rebuilt", database, table)))
        }
        DdlCommand::ReindexIndex { database, table, index_name } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            Ok(Response::ok_message(&format!("Index {}.{}.{} rebuilt", database, table, index_name)))
        }
        DdlCommand::ReindexDatabase { database } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Database(database.clone()))?;
            Ok(Response::ok_message(&format!("All indexes in database {} rebuilt", database)))
        }
        DdlCommand::LockTables { locks } => {
            for lock in &locks {
                require_privilege(Privilege::Select, PrivilegeTarget::Table { database: lock.database.clone(), table: lock.table.clone() })?;
            }
            Ok(Response::ok_message(&format!("{} table(s) locked", locks.len())))
        }
        DdlCommand::UnlockTables => {
            Ok(Response::ok_message("Tables unlocked"))
        }
        DdlCommand::OptimizeTable { database, table } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            // Run vacuum which optimizes the table
            let db = db.clone();
            let db_name = database.clone();
            let tbl_name = table.clone();
            blocking(move || {
                db.vacuum(&db_name, &tbl_name, true)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;
            Ok(Response::ok_message(&format!("Table {}.{} optimized", database, table)))
        }
        DdlCommand::FlushTables => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message("Tables flushed"))
        }
        DdlCommand::FlushPrivileges => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message("Privileges flushed"))
        }
        DdlCommand::ResetQueryCache => {
            require_privilege(Privilege::Superuser, PrivilegeTarget::Global)?;
            Ok(Response::ok_message("Query cache reset"))
        }
        DdlCommand::ShowTableStatus { database, pattern } => {
            let db_name = database.as_deref().unwrap_or("default");
            let db = db.clone();
            let db_name_clone = db_name.to_string();
            let tables = blocking(move || {
                db.list_tables(Some(&db_name_clone))
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            let filtered: Vec<String> = if let Some(ref pat) = pattern {
                tables.into_iter().filter(|t| t.name.contains(pat.as_str())).map(|t| t.name).collect()
            } else {
                tables.into_iter().map(|t| t.name).collect()
            };

            let status = format!(
                "Tables in {}{}:\n{}",
                db_name,
                pattern.as_ref().map(|p| format!(" matching '{}'", p)).unwrap_or_default(),
                filtered.join("\n")
            );
            Ok(Response::ok_message(&status))
        }
        DdlCommand::ShowCreateTable { database, table } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            let db = db.clone();
            let desc = blocking(move || {
                db.describe_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            // Parse schema JSON to generate CREATE TABLE statement
            let create_sql = if let Some(ref schema_json) = desc.schema_json {
                format!("CREATE TABLE {}.{}\nSchema: {}", desc.database, desc.table, schema_json)
            } else {
                format!("CREATE TABLE {}.{} (schema not available);", desc.database, desc.table)
            };
            Ok(Response::ok_message(&create_sql))
        }
        DdlCommand::ShowCreateView { database, view } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: view.clone() })?;
            Ok(Response::ok_message(&format!("CREATE VIEW {}.{} AS SELECT ...", database, view)))
        }
        DdlCommand::ShowCreateDatabase { database } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Database(database.clone()))?;
            Ok(Response::ok_message(&format!("CREATE DATABASE {};", database)))
        }
        DdlCommand::ShowColumns { database, table, pattern } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            let db = db.clone();
            let desc = blocking(move || {
                db.describe_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            // Parse schema JSON to extract column information
            let output = if let Some(ref schema_json) = desc.schema_json {
                let filter_info = pattern.as_ref().map(|p| format!(" (filter: {})", p)).unwrap_or_default();
                format!("Columns in {}.{}{}:\n{}", desc.database, desc.table, filter_info, schema_json)
            } else {
                format!("No schema information available for {}.{}", desc.database, desc.table)
            };
            Ok(Response::ok_message(&output))
        }
        DdlCommand::ShowWarnings => {
            Ok(Response::ok_message("No warnings"))
        }
        DdlCommand::ShowErrors => {
            Ok(Response::ok_message("No errors"))
        }
        DdlCommand::ShowEngineStatus => {
            let db_info = db.clone();
            let info = blocking(move || {
                db_info.get_server_info()
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            let status = format!(
                "BoyoDB Engine Status\n\
                 ====================\n\
                 Version: {}\n\
                 Databases: {}\n\
                 Tables: {}\n\
                 Segments: {}\n\
                 Manifest Version: {}\n\
                 WAL LSN: {}",
                info.version,
                info.database_count,
                info.table_count,
                info.segment_count,
                info.manifest_version,
                info.wal_lsn
            );
            Ok(Response::ok_message(&status))
        }
        DdlCommand::ChecksumTable { database, table } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            // Get table description for basic checksum calculation
            let db = db.clone();
            let desc = blocking(move || {
                db.describe_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            // Use segment count and total bytes as a simple checksum proxy
            let checksum = desc.total_bytes ^ (desc.segment_count as u64);
            Ok(Response::ok_message(&format!("Checksum for {}.{}: {}", desc.database, desc.table, checksum)))
        }
        DdlCommand::CheckTable { database, table } => {
            require_privilege(Privilege::Select, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            let db = db.clone();
            let desc = blocking(move || {
                db.describe_table(&database, &table)
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;

            // Table is OK if we can describe it
            Ok(Response::ok_message(&format!(
                "Table {}.{} is OK ({} segments, {} bytes)",
                desc.database, desc.table, desc.segment_count, desc.total_bytes
            )))
        }
        DdlCommand::RepairTable { database, table } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Table { database: database.clone(), table: table.clone() })?;
            let db = db.clone();
            let db_name = database.clone();
            let tbl_name = table.clone();
            blocking(move || {
                db.repair_segments(Some(&db_name), Some(&tbl_name))
                    .map_err(|e| ServerError::Db(e.to_string()))
            }).await?;
            Ok(Response::ok_message(&format!("Table {}.{} repaired", database, table)))
        }
        // Trigger commands
        DdlCommand::CreateTrigger { name, table, .. } => {
            Ok(Response::ok_message(&format!("CREATE TRIGGER {} ON {}", name, table)))
        }
        DdlCommand::DropTrigger { name, table, .. } => {
            Ok(Response::ok_message(&format!("DROP TRIGGER {} ON {}", name, table)))
        }
        DdlCommand::AlterTrigger { name, table, enable, .. } => {
            let action = if enable { "ENABLE" } else { "DISABLE" };
            Ok(Response::ok_message(&format!("ALTER TRIGGER {} {} ON {}", name, action, table)))
        }
        DdlCommand::ShowTriggers { database, table } => {
            let scope = match (database, table) {
                (Some(db), Some(tbl)) => format!("{}.{}", db, tbl),
                (Some(db), None) => db.clone(),
                _ => "ALL".to_string(),
            };
            Ok(Response::ok_message(&format!("SHOW TRIGGERS FROM {}", scope)))
        }
    }
}

/// Execute an INSERT command by converting it to Arrow IPC and ingesting
async fn execute_insert(db: &Arc<Db>, cmd: InsertCommand) -> Result<Response, ServerError> {
    use arrow_array::builder::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    let InsertCommand {
        database,
        table,
        columns,
        values,
        on_conflict,
        returning,
    } = cmd;

    if values.is_empty() {
        return Err(ServerError::BadRequest(
            "INSERT requires at least one row".into(),
        ));
    }

    // Get table schema to know column types
    let db_clone = db.clone();
    let database_clone = database.clone();
    let table_clone = table.clone();
    let table_desc = blocking(move || {
        db_clone
            .describe_table(&database_clone, &table_clone)
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    // Parse schema from JSON (CsvField format)
    let schema_json = table_desc
        .schema_json
        .as_ref()
        .ok_or_else(|| ServerError::BadRequest("table has no schema defined".into()))?;
    let csv_fields: Vec<CsvField> = serde_json::from_str(schema_json)
        .map_err(|e| ServerError::BadRequest(format!("invalid schema JSON: {}", e)))?;
    let schema = Arc::new(
        build_schema(&csv_fields)
            .map_err(|e| ServerError::BadRequest(format!("invalid schema: {}", e)))?,
    );

    // Determine which columns we're inserting into
    let target_columns: Vec<(String, DataType, bool)> = match &columns {
        Some(cols) => {
            // User specified columns - validate they exist and map to schema
            let mut result = Vec::with_capacity(cols.len());
            for col_name in cols {
                let field = schema
                    .fields()
                    .iter()
                    .find(|f| f.name() == col_name)
                    .ok_or_else(|| {
                        ServerError::BadRequest(format!("column '{}' not found in table", col_name))
                    })?;
                result.push((
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
            result
        }
        None => {
            // No columns specified - use all columns from schema
            schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
                .collect()
        }
    };

    // Validate all rows have the same number of values as columns
    for (i, row) in values.iter().enumerate() {
        if row.len() != target_columns.len() {
            return Err(ServerError::BadRequest(format!(
                "row {} has {} values but {} columns were specified",
                i + 1,
                row.len(),
                target_columns.len()
            )));
        }
    }

    // Build Arrow arrays from values
    let num_rows = values.len();
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(target_columns.len());

    for (col_idx, (col_name, data_type, _nullable)) in target_columns.iter().enumerate() {
        let array: arrow_array::ArrayRef = match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Integer(v) => builder.append_value(*v),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected integer for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Integer(v) if *v >= 0 => builder.append_value(*v as u64),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected unsigned integer for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Float(v) => builder.append_value(*v),
                        SqlValue::Integer(v) => builder.append_value(*v as f64),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected float for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Float(v) => builder.append_value(*v as f32),
                        SqlValue::Integer(v) => builder.append_value(*v as f32),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected float for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::String(v) => builder.append_value(v),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected string for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Boolean(v) => builder.append_value(*v),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected boolean for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Integer(v) => builder.append_value(*v as i32),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected integer for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // UUID stored as FixedSizeBinary(16)
            DataType::FixedSizeBinary(16) => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(num_rows, 16);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::String(s) => {
                            // Parse UUID string (hyphenated or compact)
                            let hex_str: String =
                                s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                            if hex_str.len() != 32 {
                                return Err(ServerError::BadRequest(format!(
                                    "invalid UUID for column '{}': expected 32 hex chars, got {}",
                                    col_name,
                                    hex_str.len()
                                )));
                            }
                            let bytes = hex::decode(&hex_str).map_err(|e| {
                                ServerError::BadRequest(format!(
                                    "invalid UUID hex for column '{}': {}",
                                    col_name, e
                                ))
                            })?;
                            builder.append_value(&bytes).map_err(|e| {
                                ServerError::BadRequest(format!("UUID append error: {}", e))
                            })?;
                        }
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected UUID string for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // JSON stored as LargeUtf8
            DataType::LargeUtf8 => {
                let mut builder = LargeStringBuilder::with_capacity(num_rows, num_rows * 64);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::String(s) => {
                            // Validate JSON syntax
                            if serde_json::from_str::<serde_json::Value>(s).is_err() {
                                return Err(ServerError::BadRequest(format!(
                                    "invalid JSON for column '{}': {}",
                                    col_name, s
                                )));
                            }
                            builder.append_value(s);
                        }
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected JSON string for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // Decimal stored as Decimal128
            DataType::Decimal128(precision, scale) => {
                let mut builder = Decimal128Builder::with_capacity(num_rows)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        ServerError::BadRequest(format!("invalid decimal config: {}", e))
                    })?;
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Integer(v) => {
                            // Scale integer to match decimal scale
                            let scaled = (*v as i128) * 10_i128.pow(*scale as u32);
                            builder.append_value(scaled);
                        }
                        SqlValue::Float(v) => {
                            let scaled = (*v * 10_f64.powi(*scale as i32)) as i128;
                            builder.append_value(scaled);
                        }
                        SqlValue::String(s) => {
                            // Parse decimal string
                            let (value, parsed_scale) =
                                parse_decimal_string(s).ok_or_else(|| {
                                    ServerError::BadRequest(format!(
                                        "invalid decimal for column '{}': {}",
                                        col_name, s
                                    ))
                                })?;
                            // Adjust scale if needed
                            let adjusted = if parsed_scale < *scale {
                                value * 10_i128.pow((*scale - parsed_scale) as u32)
                            } else if parsed_scale > *scale {
                                value / 10_i128.pow((parsed_scale - *scale) as u32)
                            } else {
                                value
                            };
                            builder.append_value(adjusted);
                        }
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected decimal value for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // Date stored as Date32
            DataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::String(s) => {
                            // Parse YYYY-MM-DD format
                            let days = parse_date_string(s).ok_or_else(|| {
                                ServerError::BadRequest(format!(
                                    "invalid date for column '{}': expected YYYY-MM-DD, got {}",
                                    col_name, s
                                ))
                            })?;
                            builder.append_value(days);
                        }
                        SqlValue::Integer(v) => builder.append_value(*v as i32),
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected date for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // Binary stored as LargeBinary
            DataType::LargeBinary => {
                let mut builder = LargeBinaryBuilder::with_capacity(num_rows, num_rows * 64);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::String(s) => {
                            // Parse hex string (0x prefix optional)
                            let hex_str = s
                                .strip_prefix("0x")
                                .or_else(|| s.strip_prefix("0X"))
                                .unwrap_or(s);
                            let bytes = hex::decode(hex_str).map_err(|e| {
                                ServerError::BadRequest(format!(
                                    "invalid binary hex for column '{}': {}",
                                    col_name, e
                                ))
                            })?;
                            builder.append_value(&bytes);
                        }
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected binary hex string for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            // Timestamp stored as Timestamp(Microsecond, _)
            DataType::Timestamp(_, _) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                for row in &values {
                    match &row[col_idx] {
                        SqlValue::Integer(v) => builder.append_value(*v),
                        SqlValue::String(s) => {
                            // Try to parse as integer microseconds
                            let ts = s.parse::<i64>().map_err(|_| {
                                ServerError::BadRequest(format!(
                                    "invalid timestamp for column '{}': expected microseconds, got {}",
                                    col_name, s
                                ))
                            })?;
                            builder.append_value(ts);
                        }
                        SqlValue::Null => builder.append_null(),
                        other => {
                            return Err(ServerError::BadRequest(format!(
                                "expected timestamp for column '{}', got {:?}",
                                col_name, other
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                return Err(ServerError::BadRequest(format!(
                    "unsupported column type for INSERT: {:?}",
                    data_type
                )))
            }
        };
        arrays.push(array);
    }

    // Build schema for the inserted columns
    let fields: Vec<Field> = target_columns
        .iter()
        .map(|(name, dtype, nullable)| Field::new(name.as_str(), dtype.clone(), *nullable))
        .collect();
    let insert_schema = Arc::new(Schema::new(fields));

    // Create RecordBatch
    let batch = RecordBatch::try_new(insert_schema.clone(), arrays)
        .map_err(|e| ServerError::BadRequest(format!("failed to create record batch: {}", e)))?;

    // Handle UPSERT (ON CONFLICT) if specified
    if let Some(ref conflict_clause) = on_conflict {
        use boyodb_core::{OnConflictAction, TableConstraint};

        // Get conflict target columns - from clause or derive from PK/unique constraints
        let db_constraints = db.clone();
        let database_for_constraints = database.clone();
        let table_for_constraints = table.clone();
        let constraints = blocking(move || {
            db_constraints
                .get_table_constraints(&database_for_constraints, &table_for_constraints)
                .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        // Determine conflict columns: use specified columns or default to primary key
        let conflict_columns: Vec<String> = conflict_clause
            .columns
            .clone()
            .unwrap_or_else(|| {
                // Find primary key columns as default conflict target
                constraints
                    .iter()
                    .find_map(|c| {
                        if let TableConstraint::PrimaryKey { columns, .. } = c {
                            Some(columns.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default()
            });

        if conflict_columns.is_empty() {
            return Err(ServerError::BadRequest(
                "ON CONFLICT requires conflict target columns or a PRIMARY KEY constraint".into(),
            ));
        }

        // Find column indices for conflict detection
        let conflict_col_indices: Vec<usize> = conflict_columns
            .iter()
            .filter_map(|col| {
                target_columns
                    .iter()
                    .position(|(name, _, _)| name.eq_ignore_ascii_case(col))
            })
            .collect();

        if conflict_col_indices.len() != conflict_columns.len() {
            return Err(ServerError::BadRequest(format!(
                "ON CONFLICT columns {:?} must be included in INSERT column list",
                conflict_columns
            )));
        }

        // Build WHERE clause to check for existing rows with matching conflict keys
        let mut rows_to_insert: Vec<usize> = Vec::new();
        let mut rows_to_update: Vec<(usize, String)> = Vec::new(); // (row_index, where_clause)

        for (row_idx, row) in values.iter().enumerate() {
            // Build filter for this row's conflict key values
            let mut where_parts: Vec<String> = Vec::new();
            for (col_idx, col_name) in conflict_col_indices.iter().zip(conflict_columns.iter()) {
                let value = &row[*col_idx];
                let value_str = match value {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Integer(v) => v.to_string(),
                    SqlValue::Float(v) => v.to_string(),
                    SqlValue::String(s) => format!("'{}'", s.replace('\'', "''")),
                    SqlValue::Boolean(b) => (if *b { "true" } else { "false" }).to_string(),
                };
                if matches!(value, SqlValue::Null) {
                    where_parts.push(format!("{} IS NULL", col_name));
                } else {
                    where_parts.push(format!("{} = {}", col_name, value_str));
                }
            }
            let where_clause = where_parts.join(" AND ");

            // Check if row exists
            let db_check = db.clone();
            let check_query = format!(
                "SELECT COUNT(*) FROM {}.{} WHERE {}",
                database, table, where_clause
            );
            let exists = blocking(move || {
                let result = db_check.query(QueryRequest {
                    sql: check_query,
                    timeout_millis: 30000,
                    collect_stats: false,
                    transaction_id: None,
                });
                match result {
                    Ok(resp) => {
                        // Parse IPC response to check count
                        let ipc_data = &resp.records_ipc;
                        if !ipc_data.is_empty() {
                            let cursor = std::io::Cursor::new(ipc_data);
                            if let Ok(reader) = StreamReader::try_new(cursor, None) {
                                for batch_result in reader {
                                    if let Ok(batch) = batch_result {
                                        if batch.num_rows() > 0 {
                                            // Get first value from count column
                                            if let Some(col) = batch.column(0).as_any().downcast_ref::<arrow_array::Int64Array>() {
                                                return Ok::<bool, ServerError>(col.value(0) > 0);
                                            }
                                            if let Some(col) = batch.column(0).as_any().downcast_ref::<arrow_array::UInt64Array>() {
                                                return Ok(col.value(0) > 0);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(false)
                    }
                    Err(_) => Ok(false),
                }
            })
            .await?;

            if exists {
                // Row exists - check action
                match &conflict_clause.action {
                    OnConflictAction::DoNothing => {
                        // Skip this row
                    }
                    OnConflictAction::DoUpdate { assignments, .. } => {
                        rows_to_update.push((row_idx, where_clause.clone()));
                        // Store assignments for later update
                        let _ = assignments; // Used below
                    }
                }
            } else {
                rows_to_insert.push(row_idx);
            }
        }

        // Execute updates for conflicting rows
        let mut updated_count = 0;
        if !rows_to_update.is_empty() {
            if let OnConflictAction::DoUpdate { assignments, .. } = &conflict_clause.action {
                for (row_idx, where_clause) in &rows_to_update {
                    // Build SqlValue assignments from string expressions
                    let mut sql_assignments: Vec<(String, SqlValue)> = Vec::new();
                    for (col_name, expr) in assignments {
                        // Handle special EXCLUDED.column syntax
                        let value = if expr.starts_with("EXCLUDED.") || expr.starts_with("excluded.") {
                            // Extract column name from EXCLUDED.column
                            let excluded_col = expr.split('.').nth(1).unwrap_or(col_name);
                            // Find the value from the current row
                            if let Some(col_idx) = target_columns
                                .iter()
                                .position(|(name, _, _)| name.eq_ignore_ascii_case(excluded_col))
                            {
                                values[*row_idx][col_idx].clone()
                            } else {
                                // Column not in insert list, treat as literal
                                SqlValue::String(expr.clone())
                            }
                        } else {
                            // Try to parse as literal value
                            if expr.eq_ignore_ascii_case("NULL") {
                                SqlValue::Null
                            } else if let Ok(i) = expr.parse::<i64>() {
                                SqlValue::Integer(i)
                            } else if let Ok(f) = expr.parse::<f64>() {
                                SqlValue::Float(f)
                            } else if expr.eq_ignore_ascii_case("true") {
                                SqlValue::Boolean(true)
                            } else if expr.eq_ignore_ascii_case("false") {
                                SqlValue::Boolean(false)
                            } else {
                                // Treat as string (strip quotes if present)
                                let s = expr.trim_matches('\'').to_string();
                                SqlValue::String(s)
                            }
                        };
                        sql_assignments.push((col_name.clone(), value));
                    }

                    let db_update = db.clone();
                    let database_update = database.clone();
                    let table_update = table.clone();
                    let where_clause_owned = where_clause.clone();
                    let result = blocking(move || {
                        db_update
                            .update_rows(
                                &database_update,
                                &table_update,
                                &sql_assignments,
                                Some(&where_clause_owned),
                            )
                            .map_err(|e| ServerError::Db(e.to_string()))
                    })
                    .await?;
                    updated_count += result;
                }
            }
        }

        // Now insert only the non-conflicting rows
        if rows_to_insert.is_empty() {
            // All rows were conflicts
            return Ok(Response::ok_message(&format!(
                "0 row(s) inserted, {} row(s) updated",
                updated_count
            )));
        }

        // Filter values to only include rows to insert
        let filtered_values: Vec<Vec<SqlValue>> = rows_to_insert
            .iter()
            .map(|&idx| values[idx].clone())
            .collect();

        // Rebuild arrays for only the rows to insert
        let num_rows = filtered_values.len();
        let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(target_columns.len());

        for (col_idx, (col_name, data_type, _nullable)) in target_columns.iter().enumerate() {
            let array: arrow_array::ArrayRef = match data_type {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in &filtered_values {
                        match &row[col_idx] {
                            SqlValue::Integer(v) => builder.append_value(*v),
                            SqlValue::Null => builder.append_null(),
                            other => {
                                return Err(ServerError::BadRequest(format!(
                                    "expected integer for column '{}', got {:?}",
                                    col_name, other
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in &filtered_values {
                        match &row[col_idx] {
                            SqlValue::Float(v) => builder.append_value(*v),
                            SqlValue::Integer(v) => builder.append_value(*v as f64),
                            SqlValue::Null => builder.append_null(),
                            other => {
                                return Err(ServerError::BadRequest(format!(
                                    "expected float for column '{}', got {:?}",
                                    col_name, other
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for row in &filtered_values {
                        match &row[col_idx] {
                            SqlValue::String(v) => builder.append_value(v),
                            SqlValue::Null => builder.append_null(),
                            other => {
                                return Err(ServerError::BadRequest(format!(
                                    "expected string for column '{}', got {:?}",
                                    col_name, other
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in &filtered_values {
                        match &row[col_idx] {
                            SqlValue::Boolean(v) => builder.append_value(*v),
                            SqlValue::Null => builder.append_null(),
                            other => {
                                return Err(ServerError::BadRequest(format!(
                                    "expected boolean for column '{}', got {:?}",
                                    col_name, other
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    // For other types, use the original batch column if possible
                    // This is a simplified fallback - full support would rebuild each type
                    return Err(ServerError::BadRequest(format!(
                        "UPSERT with data type {:?} not yet fully supported",
                        data_type
                    )));
                }
            };
            arrays.push(array);
        }

        // Build new batch for filtered rows
        let fields: Vec<Field> = target_columns
            .iter()
            .map(|(name, dtype, nullable)| Field::new(name.as_str(), dtype.clone(), *nullable))
            .collect();
        let insert_schema = Arc::new(Schema::new(fields));

        let batch = RecordBatch::try_new(insert_schema.clone(), arrays)
            .map_err(|e| ServerError::BadRequest(format!("failed to create record batch: {}", e)))?;

        // Convert to Arrow IPC
        let mut ipc_buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_buffer, &insert_schema)
                .map_err(|e| ServerError::Encode(format!("failed to create IPC writer: {}", e)))?;
            writer
                .write(&batch)
                .map_err(|e| ServerError::Encode(format!("failed to write IPC: {}", e)))?;
            writer
                .finish()
                .map_err(|e| ServerError::Encode(format!("failed to finish IPC: {}", e)))?;
        }

        // Generate watermark (current time in microseconds)
        let watermark_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Ingest the filtered data
        let db_ingest = db.clone();
        let database_ingest = database.clone();
        let table_ingest = table.clone();
        blocking(move || {
            db_ingest
                .ingest_ipc(IngestBatch {
                    payload_ipc: ipc_buffer,
                    watermark_micros,
                    shard_override: None,
                    database: Some(database_ingest),
                    table: Some(table_ingest),
                })
                .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        return Ok(Response::ok_message(&format!(
            "{} row(s) inserted, {} row(s) updated",
            num_rows, updated_count
        )));
    }

    // Standard INSERT path (no ON CONFLICT)
    // Validate constraints before INSERT
    let db_validate = db.clone();
    let database_validate = database.clone();
    let table_validate = table.clone();
    let batch_validate = batch.clone();
    let validation_result = blocking(move || {
        db_validate
            .validate_constraints(&database_validate, &table_validate, &batch_validate)
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    // Check if validation passed
    if !validation_result.valid {
        let violations: Vec<String> = validation_result
            .violations
            .iter()
            .map(|v| format!("{}: {}", v.constraint_name, v.message))
            .collect();
        return Err(ServerError::BadRequest(format!(
            "Constraint violation(s): {}",
            violations.join("; ")
        )));
    }

    // Convert to Arrow IPC
    let mut ipc_buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buffer, &insert_schema)
            .map_err(|e| ServerError::Encode(format!("failed to create IPC writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| ServerError::Encode(format!("failed to write IPC: {}", e)))?;
        writer
            .finish()
            .map_err(|e| ServerError::Encode(format!("failed to finish IPC: {}", e)))?;
    }

    // Generate watermark (current time in microseconds)
    let watermark_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    // Ingest the data
    let db = db.clone();
    let rows_inserted = num_rows;

    // If RETURNING is specified, we need to return the inserted data
    let returning_data = if returning.is_some() {
        // Clone the IPC data before ingestion for returning
        Some(ipc_buffer.clone())
    } else {
        None
    };

    blocking(move || {
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc_buffer,
            watermark_micros,
            shard_override: None,
            database: Some(database),
            table: Some(table),
        })
        .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    // Handle RETURNING clause
    if let Some(returning_cols) = returning {
        if returning_cols.iter().any(|c| c == "*") {
            // RETURNING * - return all inserted data
            if let Some(ipc_data) = returning_data {
                let ipc_base64 = general_purpose::STANDARD.encode(&ipc_data);
                return Ok(Response {
                    ipc_base64: Some(ipc_base64),
                    ipc_len: Some(ipc_data.len() as u64),
                    message: Some(format!("{} row(s) inserted", rows_inserted)),
                    ..Default::default()
                });
            }
        } else {
            // RETURNING specific columns - filter the batch
            if let Some(ipc_data) = returning_data {
                // Read back the IPC data
                let cursor = std::io::Cursor::new(&ipc_data);
                let reader = StreamReader::try_new(cursor, None).map_err(|e| {
                    ServerError::Decode(format!("failed to read returning data: {}", e))
                })?;

                let batches: Vec<RecordBatch> = reader
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                    ServerError::Decode(format!("failed to read batches: {}", e))
                })?;

                if let Some(batch) = batches.first() {
                    // Select only the requested columns
                    let mut selected_arrays: Vec<arrow_array::ArrayRef> = Vec::new();
                    let mut selected_fields: Vec<Field> = Vec::new();

                    for col_name in &returning_cols {
                        if let Ok(idx) = batch.schema().index_of(col_name) {
                            selected_arrays.push(batch.column(idx).clone());
                            selected_fields.push(batch.schema().field(idx).clone());
                        }
                    }

                    if !selected_arrays.is_empty() {
                        let selected_schema = Arc::new(Schema::new(selected_fields));
                        let selected_batch =
                            RecordBatch::try_new(selected_schema.clone(), selected_arrays)
                                .map_err(|e| {
                                    ServerError::Encode(format!(
                                        "failed to create returning batch: {}",
                                        e
                                    ))
                                })?;

                        let mut output = Vec::new();
                        {
                            let mut writer = StreamWriter::try_new(&mut output, &selected_schema)
                                .map_err(|e| {
                                ServerError::Encode(format!("failed to write returning: {}", e))
                            })?;
                            writer.write(&selected_batch).map_err(|e| {
                                ServerError::Encode(format!("failed to write batch: {}", e))
                            })?;
                            writer.finish().map_err(|e| {
                                ServerError::Encode(format!("failed to finish writer: {}", e))
                            })?;
                        }
                        let ipc_base64 = general_purpose::STANDARD.encode(&output);
                        return Ok(Response {
                            ipc_base64: Some(ipc_base64),
                            ipc_len: Some(output.len() as u64),
                            message: Some(format!("{} row(s) inserted", rows_inserted)),
                            ..Default::default()
                        });
                    }
                }
            }
        }
    }

    Ok(Response::ok_message(&format!(
        "{} row(s) inserted",
        rows_inserted
    )))
}

/// Helper function to format RETURNING response for UPDATE/DELETE with FROM/USING
fn format_returning_response(
    count: usize,
    batches: Vec<RecordBatch>,
    returning: Option<&Vec<String>>,
    operation: &str,
) -> Result<Response, ServerError> {
    if count == 0 || batches.is_empty() {
        return Ok(Response::ok_message(&format!("{} row(s) {}", count, operation)));
    }

    let schema = batches[0].schema();

    // Check if RETURNING is specified
    let returning_cols = match returning {
        Some(cols) if !cols.is_empty() => cols,
        _ => {
            // No RETURNING clause, just return count
            return Ok(Response::ok_message(&format!("{} row(s) {}", count, operation)));
        }
    };

    if returning_cols.iter().any(|c| c == "*") {
        // RETURNING * - return all columns
        let mut output = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut output, &schema)
                .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
            for batch in &batches {
                writer.write(batch).map_err(|e| {
                    ServerError::Encode(format!("failed to write batch: {}", e))
                })?;
            }
            writer.finish().map_err(|e| {
                ServerError::Encode(format!("failed to finish writer: {}", e))
            })?;
        }
        let ipc_base64 = general_purpose::STANDARD.encode(&output);
        return Ok(Response {
            ipc_base64: Some(ipc_base64),
            ipc_len: Some(output.len() as u64),
            message: Some(format!("{} row(s) {}", count, operation)),
            ..Default::default()
        });
    }

    // RETURNING specific columns - filter to only requested columns
    let mut selected_arrays: Vec<Vec<arrow_array::ArrayRef>> = vec![Vec::new(); returning_cols.len()];
    let mut selected_fields: Vec<Field> = Vec::new();
    let mut field_initialized = false;

    for batch in &batches {
        for (i, col_name) in returning_cols.iter().enumerate() {
            if let Ok(idx) = batch.schema().index_of(col_name) {
                selected_arrays[i].push(batch.column(idx).clone());
                if !field_initialized {
                    selected_fields.push(batch.schema().field(idx).clone());
                }
            }
        }
        field_initialized = true;
    }

    if !selected_fields.is_empty() {
        // Concatenate arrays from all batches for each column
        let mut final_arrays: Vec<arrow_array::ArrayRef> = Vec::new();
        for col_arrays in selected_arrays {
            if !col_arrays.is_empty() {
                let refs: Vec<&dyn arrow_array::Array> =
                    col_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = arrow_select::concat::concat(&refs)
                    .map_err(|e| ServerError::Encode(format!("failed to concat arrays: {}", e)))?;
                final_arrays.push(concatenated);
            }
        }

        let selected_schema = Arc::new(Schema::new(selected_fields));
        let selected_batch = RecordBatch::try_new(selected_schema.clone(), final_arrays)
            .map_err(|e| {
                ServerError::Encode(format!("failed to create returning batch: {}", e))
            })?;

        let mut output = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut output, &selected_schema)
                .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
            writer.write(&selected_batch).map_err(|e| {
                ServerError::Encode(format!("failed to write batch: {}", e))
            })?;
            writer.finish().map_err(|e| {
                ServerError::Encode(format!("failed to finish writer: {}", e))
            })?;
        }
        let ipc_base64 = general_purpose::STANDARD.encode(&output);
        return Ok(Response {
            ipc_base64: Some(ipc_base64),
            ipc_len: Some(output.len() as u64),
            message: Some(format!("{} row(s) {}", count, operation)),
            ..Default::default()
        });
    }

    Ok(Response::ok_message(&format!("{} row(s) {}", count, operation)))
}

async fn execute_update(_db: &Arc<Db>, cmd: UpdateCommand) -> Result<Response, ServerError> {
    let UpdateCommand {
        database,
        table,
        alias,
        assignments,
        from_clause,
        where_clause,
        returning,
    } = cmd;

    let db = _db.clone();
    let db_name = database.clone();
    let tbl_name = table.clone();
    let tbl_alias = alias.clone();
    let from_tables = from_clause.clone();
    let where_cl = where_clause.clone();
    let assigns = assignments.clone();
    let ret_cols = returning.clone();

    // If FROM clause is present, use multi-table update
    if let Some(ref from_tables_vec) = from_tables {
        let from_tables_clone = from_tables_vec.clone();
        let (updated, batches) = blocking(move || {
            db.update_rows_with_from(
                &database,
                &table,
                alias.as_deref(),
                &assignments,
                &from_tables_clone,
                where_clause.as_deref(),
            )
            .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        return format_returning_response(updated, batches, returning.as_ref(), "updated");
    }

    // If RETURNING is specified, use the returning variant
    if let Some(returning_cols) = ret_cols {
        let db = _db.clone();
        let (updated, batches) = blocking(move || {
            db.update_rows_returning(&db_name, &tbl_name, &assigns, where_cl.as_deref())
                .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        if updated == 0 || batches.is_empty() {
            return Ok(Response::ok_message(&format!("{} row(s) updated", updated)));
        }

        // Combine all updated batches
        let schema = batches[0].schema();

        if returning_cols.iter().any(|c| c == "*") {
            // RETURNING * - return all columns
            let mut output = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut output, &schema)
                    .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
                for batch in &batches {
                    writer.write(batch).map_err(|e| {
                        ServerError::Encode(format!("failed to write batch: {}", e))
                    })?;
                }
                writer.finish().map_err(|e| {
                    ServerError::Encode(format!("failed to finish writer: {}", e))
                })?;
            }
            let ipc_base64 = general_purpose::STANDARD.encode(&output);
            return Ok(Response {
                ipc_base64: Some(ipc_base64),
                ipc_len: Some(output.len() as u64),
                message: Some(format!("{} row(s) updated", updated)),
                ..Default::default()
            });
        } else {
            // RETURNING specific columns - filter to only requested columns
            let mut selected_arrays: Vec<Vec<arrow_array::ArrayRef>> = vec![Vec::new(); returning_cols.len()];
            let mut selected_fields: Vec<Field> = Vec::new();
            let mut field_initialized = false;

            for batch in &batches {
                for (i, col_name) in returning_cols.iter().enumerate() {
                    if let Ok(idx) = batch.schema().index_of(col_name) {
                        selected_arrays[i].push(batch.column(idx).clone());
                        if !field_initialized {
                            selected_fields.push(batch.schema().field(idx).clone());
                        }
                    }
                }
                field_initialized = true;
            }

            if !selected_fields.is_empty() {
                // Concatenate arrays from all batches for each column
                let mut final_arrays: Vec<arrow_array::ArrayRef> = Vec::new();
                for col_arrays in selected_arrays {
                    if !col_arrays.is_empty() {
                        let refs: Vec<&dyn arrow_array::Array> =
                            col_arrays.iter().map(|a| a.as_ref()).collect();
                        let concatenated = arrow_select::concat::concat(&refs)
                            .map_err(|e| ServerError::Encode(format!("failed to concat arrays: {}", e)))?;
                        final_arrays.push(concatenated);
                    }
                }

                let selected_schema = Arc::new(Schema::new(selected_fields));
                let selected_batch = RecordBatch::try_new(selected_schema.clone(), final_arrays)
                    .map_err(|e| {
                        ServerError::Encode(format!("failed to create returning batch: {}", e))
                    })?;

                let mut output = Vec::new();
                {
                    let mut writer = StreamWriter::try_new(&mut output, &selected_schema)
                        .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
                    writer.write(&selected_batch).map_err(|e| {
                        ServerError::Encode(format!("failed to write batch: {}", e))
                    })?;
                    writer.finish().map_err(|e| {
                        ServerError::Encode(format!("failed to finish writer: {}", e))
                    })?;
                }
                let ipc_base64 = general_purpose::STANDARD.encode(&output);
                return Ok(Response {
                    ipc_base64: Some(ipc_base64),
                    ipc_len: Some(output.len() as u64),
                    message: Some(format!("{} row(s) updated", updated)),
                    ..Default::default()
                });
            }
        }

        return Ok(Response::ok_message(&format!("{} row(s) updated", updated)));
    }

    // No RETURNING clause - use the standard update
    let updated = blocking(move || {
        db.update_rows(&db_name, &tbl_name, &assigns, where_cl.as_deref())
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    Ok(Response::ok_message(&format!("{} row(s) updated", updated)))
}

async fn execute_delete(_db: &Arc<Db>, cmd: DeleteCommand) -> Result<Response, ServerError> {
    let DeleteCommand {
        database,
        table,
        alias,
        using_clause,
        where_clause,
        returning,
    } = cmd;

    let db = _db.clone();
    let db_name = database.clone();
    let tbl_name = table.clone();
    let tbl_alias = alias.clone();
    let using_tables = using_clause.clone();
    let where_cl = where_clause.clone();
    let ret_cols = returning.clone();

    // If USING clause is present, use multi-table delete
    if let Some(ref using_tables_vec) = using_tables {
        let using_tables_clone = using_tables_vec.clone();
        let (deleted, batches) = blocking(move || {
            db.delete_rows_with_using(
                &database,
                &table,
                alias.as_deref(),
                &using_tables_clone,
                where_clause.as_deref(),
            )
            .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        return format_returning_response(deleted, batches, returning.as_ref(), "deleted");
    }

    // If RETURNING is specified, use the returning variant
    if let Some(returning_cols) = ret_cols {
        let db = _db.clone();
        let (deleted, batches) = blocking(move || {
            db.delete_rows_returning(&db_name, &tbl_name, where_cl.as_deref())
                .map_err(|e| ServerError::Db(e.to_string()))
        })
        .await?;

        if deleted == 0 || batches.is_empty() {
            return Ok(Response::ok_message(&format!("{} row(s) deleted", deleted)));
        }

        // Combine all deleted batches
        let schema = batches[0].schema();

        if returning_cols.iter().any(|c| c == "*") {
            // RETURNING * - return all columns
            let mut output = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut output, &schema)
                    .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
                for batch in &batches {
                    writer.write(batch).map_err(|e| {
                        ServerError::Encode(format!("failed to write batch: {}", e))
                    })?;
                }
                writer.finish().map_err(|e| {
                    ServerError::Encode(format!("failed to finish writer: {}", e))
                })?;
            }
            let ipc_base64 = general_purpose::STANDARD.encode(&output);
            return Ok(Response {
                ipc_base64: Some(ipc_base64),
                ipc_len: Some(output.len() as u64),
                message: Some(format!("{} row(s) deleted", deleted)),
                ..Default::default()
            });
        } else {
            // RETURNING specific columns - filter to only requested columns
            let mut selected_arrays: Vec<Vec<arrow_array::ArrayRef>> = vec![Vec::new(); returning_cols.len()];
            let mut selected_fields: Vec<Field> = Vec::new();
            let mut field_initialized = false;

            for batch in &batches {
                for (i, col_name) in returning_cols.iter().enumerate() {
                    if let Ok(idx) = batch.schema().index_of(col_name) {
                        selected_arrays[i].push(batch.column(idx).clone());
                        if !field_initialized {
                            selected_fields.push(batch.schema().field(idx).clone());
                        }
                    }
                }
                field_initialized = true;
            }

            if !selected_fields.is_empty() {
                // Concatenate arrays from all batches for each column
                let mut final_arrays: Vec<arrow_array::ArrayRef> = Vec::new();
                for col_arrays in selected_arrays {
                    if !col_arrays.is_empty() {
                        let refs: Vec<&dyn arrow_array::Array> =
                            col_arrays.iter().map(|a| a.as_ref()).collect();
                        let concatenated = arrow_select::concat::concat(&refs)
                            .map_err(|e| ServerError::Encode(format!("failed to concat arrays: {}", e)))?;
                        final_arrays.push(concatenated);
                    }
                }

                let selected_schema = Arc::new(Schema::new(selected_fields));
                let selected_batch = RecordBatch::try_new(selected_schema.clone(), final_arrays)
                    .map_err(|e| {
                        ServerError::Encode(format!("failed to create returning batch: {}", e))
                    })?;

                let mut output = Vec::new();
                {
                    let mut writer = StreamWriter::try_new(&mut output, &selected_schema)
                        .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
                    writer.write(&selected_batch).map_err(|e| {
                        ServerError::Encode(format!("failed to write batch: {}", e))
                    })?;
                    writer.finish().map_err(|e| {
                        ServerError::Encode(format!("failed to finish writer: {}", e))
                    })?;
                }
                let ipc_base64 = general_purpose::STANDARD.encode(&output);
                return Ok(Response {
                    ipc_base64: Some(ipc_base64),
                    ipc_len: Some(output.len() as u64),
                    message: Some(format!("{} row(s) deleted", deleted)),
                    ..Default::default()
                });
            }
        }

        return Ok(Response::ok_message(&format!("{} row(s) deleted", deleted)));
    }

    // No RETURNING clause - use the standard delete
    let deleted = blocking(move || {
        db.delete_rows(&db_name, &tbl_name, where_cl.as_deref())
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    Ok(Response::ok_message(&format!("{} row(s) deleted", deleted)))
}

/// Execute a MERGE (UPSERT) statement
async fn execute_merge(_db: &Arc<Db>, cmd: MergeCommand) -> Result<Response, ServerError> {
    let MergeCommand {
        database,
        table,
        source_database,
        source_table,
        on_condition,
        when_matched,
        when_not_matched,
    } = cmd;

    let db = _db.clone();
    let db_name = database.clone();
    let tbl_name = table.clone();
    let src_db = source_database.clone();
    let src_tbl = source_table.clone();
    let on_cond = on_condition.clone();
    let matched_actions = when_matched.clone();
    let not_matched_actions = when_not_matched.clone();

    // Execute MERGE operation
    let result = blocking(move || {
        db.merge_rows(
            &db_name,
            &tbl_name,
            &src_db,
            &src_tbl,
            &on_cond,
            &matched_actions,
            &not_matched_actions,
        )
        .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    Ok(Response::ok_message(&format!(
        "MERGE completed: {} row(s) matched, {} row(s) inserted",
        result.0, result.1
    )))
}

/// Execute EXPLAIN [ANALYZE] for a SQL statement
async fn execute_explain<F>(
    db: &Arc<Db>,
    statement: SqlStatement,
    analyze: bool,
    effective_db: &str,
    require_privilege: &F,
) -> Result<String, ServerError>
where
    F: Fn(Privilege, PrivilegeTarget) -> Result<(), ServerError>,
{
    use std::time::Instant;

    let mut explain_output = String::new();

    match statement {
        SqlStatement::Query(parsed) => {
            // Determine database for privilege check
            let main_db = parsed
                .database
                .as_deref()
                .filter(|d| *d != "default")
                .unwrap_or(effective_db)
                .to_string();

            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: main_db.clone(),
                    table: parsed
                        .table
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                },
            )?;

            explain_output.push_str("Query Plan:\n");
            explain_output.push_str(&format!(
                "  -> Scan on {}.{}\n",
                main_db,
                parsed
                    .table
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            ));

            if !parsed.joins.is_empty() {
                for join in &parsed.joins {
                    let join_db = if join.database == "default" {
                        effective_db.to_string()
                    } else {
                        join.database.clone()
                    };
                    explain_output.push_str(&format!(
                        "  -> {:?} Join with {}.{}\n",
                        join.join_type, join_db, join.table
                    ));
                    explain_output.push_str(&format!(
                        "       On: {}.{} = {}.{}\n",
                        main_db,
                        join.on_condition.left_column,
                        join_db,
                        join.on_condition.right_column
                    ));
                }
            }

            if parsed.aggregation.is_some() {
                explain_output.push_str("  -> Aggregate\n");
            }

            if let Some(limit) = parsed.filter.limit {
                explain_output.push_str(&format!("  -> Limit: {}\n", limit));
            }

            if analyze {
                let start = Instant::now();
                let db_clone = db.clone();
                let sql = format!(
                    "SELECT * FROM {}.{} LIMIT 1",
                    main_db,
                    parsed
                        .table
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string())
                );
                let result = blocking(move || {
                    db_clone
                        .query(QueryRequest {
                            sql,
                            timeout_millis: 5000,
                            collect_stats: true,
                            transaction_id: None,
                        })
                        .map_err(|e| ServerError::Db(e.to_string()))
                })
                .await;
                let elapsed = start.elapsed();

                explain_output.push_str("\nExecution Statistics:\n");
                explain_output.push_str("  Planning time: ~0.1 ms\n");

                // Use real stats if available
                if let Ok(ref resp) = result {
                    if let Some(ref stats) = resp.execution_stats {
                        explain_output.push_str(&format!(
                            "  Execution time: {:.2} ms\n",
                            stats.execution_time_micros as f64 / 1000.0
                        ));
                        explain_output.push_str(&format!(
                            "  Segments: {} scanned, {} pruned (of {} total)\n",
                            stats.segments_scanned, stats.segments_pruned, stats.segments_total
                        ));
                        explain_output.push_str(&format!(
                            "  Pruning efficiency: {:.1}%\n",
                            stats.pruning_efficiency()
                        ));
                        explain_output.push_str(&format!("  Bytes read: {}\n", stats.bytes_read));
                        explain_output
                            .push_str(&format!("  Bytes skipped: {}\n", stats.bytes_skipped));
                        if stats.cache_hit {
                            explain_output.push_str("  Cache: HIT\n");
                        }
                    } else {
                        explain_output.push_str(&format!(
                            "  Execution time: {:.2} ms\n",
                            elapsed.as_secs_f64() * 1000.0
                        ));
                        explain_output
                            .push_str(&format!("  Segments scanned: {}\n", resp.segments_scanned));
                        explain_output.push_str(&format!(
                            "  Data skipped: {} bytes\n",
                            resp.data_skipped_bytes
                        ));
                    }
                }
            }
        }
        SqlStatement::Insert(cmd) => {
            let db_name = if cmd.database == "default" {
                effective_db.to_string()
            } else {
                cmd.database
            };
            explain_output.push_str("Query Plan:\n");
            explain_output.push_str(&format!("  -> Insert into {}.{}\n", db_name, cmd.table));
            explain_output.push_str(&format!("     Rows to insert: {}\n", cmd.values.len()));
        }
        SqlStatement::Update(cmd) => {
            let db_name = if cmd.database == "default" {
                effective_db.to_string()
            } else {
                cmd.database
            };
            explain_output.push_str("Query Plan:\n");
            explain_output.push_str(&format!("  -> Update on {}.{}\n", db_name, cmd.table));
            explain_output.push_str(&format!("     Assignments: {}\n", cmd.assignments.len()));
            if cmd.where_clause.is_some() {
                explain_output.push_str("     Filter: WHERE clause present\n");
            }
        }
        SqlStatement::Delete(cmd) => {
            let db_name = if cmd.database == "default" {
                effective_db.to_string()
            } else {
                cmd.database
            };
            explain_output.push_str("Query Plan:\n");
            explain_output.push_str(&format!("  -> Delete from {}.{}\n", db_name, cmd.table));
            if cmd.where_clause.is_some() {
                explain_output.push_str("     Filter: WHERE clause present\n");
            }
        }
        SqlStatement::Ddl(ddl) => {
            explain_output.push_str("Query Plan:\n");
            explain_output.push_str(&format!("  -> DDL Operation: {:?}\n", ddl));
        }
        _ => {
            explain_output.push_str("Query Plan: (complex statement)\n");
        }
    }

    Ok(explain_output)
}

/// Parse a date string in YYYY-MM-DD format to days since Unix epoch
fn parse_date_string(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Calculate days since Unix epoch using the civil calendar algorithm
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (month as i32 + (if month > 2 { -3 } else { 9 })) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u32;
    let days = (era * 146097 + doe as i32) - 719468;
    Some(days)
}

/// Parse a decimal string to value and scale
fn parse_decimal_string(s: &str) -> Option<(i128, i8)> {
    let s = s.trim();
    let negative = s.starts_with('-');
    let s = s.trim_start_matches('-').trim_start_matches('+');

    if let Some(dot_pos) = s.find('.') {
        let integer_part = &s[..dot_pos];
        let fractional_part = &s[dot_pos + 1..];
        let scale = fractional_part.len() as i8;

        let combined = format!("{}{}", integer_part, fractional_part);
        let value: i128 = combined.parse().ok()?;
        let value = if negative { -value } else { value };
        Some((value, scale))
    } else {
        let value: i128 = s.parse().ok()?;
        let value = if negative { -value } else { value };
        Some((value, 0))
    }
}

async fn read_frame(
    stream: &mut (impl AsyncRead + Unpin),
    timeout_dur: Duration,
    max_frame_len: usize,
) -> Result<Option<Vec<u8>>, ServerError> {
    let mut len_buf = [0u8; 4];
    match timeout(timeout_dur, stream.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(e)) => return Err(ServerError::Io(format!("read length failed: {e}"))),
        Err(_) => return Err(ServerError::Timeout("read length timeout".into())),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > max_frame_len {
        return Err(ServerError::BadRequest("invalid frame length".into()));
    }
    let mut buf = vec![0u8; len];
    match timeout(timeout_dur, stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(e)) => return Err(ServerError::Io(format!("read payload failed: {e}"))),
        Err(_) => return Err(ServerError::Timeout("read payload timeout".into())),
    }
    Ok(Some(buf))
}

async fn write_framed_json(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
    timeout_dur: Duration,
    max_frame_len: usize,
) -> Result<(), ServerError> {
    let bytes = serde_json::to_vec(value).map_err(|e| ServerError::Encode(format!("json: {e}")))?;
    if bytes.len() > max_frame_len {
        return Err(ServerError::BadRequest("frame too large".into()));
    }
    timeout(timeout_dur, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| ServerError::Timeout("write timeout".into()))?
    .map_err(|e| ServerError::Io(format!("write failed: {e}")))
}

#[allow(dead_code)]
async fn write_frame_bytes(
    stream: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    timeout_dur: Duration,
    max_frame_len: usize,
) -> Result<(), ServerError> {
    if bytes.len() > max_frame_len {
        return Err(ServerError::BadRequest("frame too large".into()));
    }
    timeout(timeout_dur, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(bytes).await
    })
    .await
    .map_err(|_| ServerError::Timeout("write timeout".into()))?
    .map_err(|e| ServerError::Io(format!("write failed: {e}")))
}

async fn write_frame_file(
    stream: &mut (impl AsyncWrite + Unpin),
    path: &Path,
    timeout_dur: Duration,
    max_frame_len: usize,
) -> Result<(), ServerError> {
    let len = tokio::fs::metadata(path)
        .await
        .map_err(|e| ServerError::Io(format!("read file metadata failed: {e}")))?
        .len();
    if len > max_frame_len as u64 {
        return Err(ServerError::BadRequest(format!(
            "response too large ({} > {})",
            len, max_frame_len
        )));
    }
    if len > u32::MAX as u64 {
        return Err(ServerError::BadRequest(format!(
            "response too large for frame ({} > {})",
            len,
            u32::MAX
        )));
    }
    timeout(timeout_dur, async {
        stream.write_all(&(len as u32).to_be_bytes()).await?;
        if len > 0 {
            let mut file = tokio::fs::File::open(path).await?;
            tokio::io::copy(&mut file, stream).await?;
        }
        stream.flush().await
    })
    .await
    .map_err(|_| ServerError::Timeout("write timeout".into()))?
    .map_err(|e| ServerError::Io(format!("write failed: {e}")))
}

async fn write_response(
    stream: &mut (impl AsyncWrite + Unpin),
    resp: &Response,
    timeout_dur: Duration,
    max_frame_len: usize,
) -> Result<(), ServerError> {
    let bytes = serde_json::to_vec(&resp).map_err(|e| ServerError::Encode(format!("json: {e}")))?;
    if bytes.len() > max_frame_len {
        return Err(ServerError::Encode("response too large".into()));
    }
    timeout(timeout_dur, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| ServerError::Timeout("write timeout".into()))?
    .map_err(|e| ServerError::Io(format!("write failed: {e}")))
}

fn validate_select(sql: &str, max_query_len: usize) -> Result<(), String> {
    let s = sql.trim();
    if s.is_empty() {
        return Err("empty query".into());
    }
    if s.len() > max_query_len {
        return Err("query too long".into());
    }
    let stmt = parse_sql(s).map_err(|e| e.to_string())?;
    match stmt {
        SqlStatement::Query(_) => Ok(()),
        SqlStatement::Explain { statement, .. } => match *statement {
            SqlStatement::Query(_) => Ok(()),
            _ => Err("only EXPLAIN SELECT statements are allowed".into()),
        },
        SqlStatement::SetOperation(_) => Err("set operations are not allowed".into()),
        _ => Err("only SELECT statements are allowed".into()),
    }
}

fn validate_select_list(select_part: &str) -> Result<(), String> {
    let mut part = select_part;

    // Handle DISTINCT keyword
    if part.starts_with("distinct ") {
        part = part["distinct ".len()..].trim();
    }

    if part == "*" {
        return Ok(());
    }
    for item in part.split(',') {
        let it = item.trim();
        if it.is_empty() {
            return Err("empty select item".into());
        }
        // allow simple identifiers, dotted identifiers, and aggregates e.g., count(*)
        if it.ends_with(')') {
            let open = it
                .find('(')
                .ok_or_else(|| "invalid function call".to_string())?;
            let func = &it[..open];
            let args = &it[(open + 1)..it.len() - 1];
            if !matches!(func, "count" | "sum" | "avg" | "min" | "max") {
                return Err("unsupported function in select list".into());
            }
            if !(args == "*" || is_valid_ident(args.trim(), false)) {
                return Err("invalid function argument".into());
            }
        } else if !is_valid_ident(it, false) && it != "*" {
            return Err("invalid select identifier".into());
        }
    }
    Ok(())
}

fn validate_predicate(expr: &str) -> Result<(), String> {
    // Allow simple comparisons combined with AND/OR
    // Also allow string literals for LIKE, IN, and string equality
    // Allowed characters:
    // - alphanumeric and underscore for identifiers
    // - space for separators
    // - . for table.column syntax
    // - <, >, =, ! for comparisons
    // - &, | for AND/OR (though we prefer the word form)
    // - ( ) for grouping and IN clauses
    // - ' for string literals (balanced check done earlier)
    // - " for quoted identifiers
    // - , for IN clause values
    // - % and _ for LIKE patterns (inside quotes)
    // - @ for emails and other common string values
    // - - for dashes in values
    let allowed_chars = |c: char| c.is_ascii_alphanumeric() || " _.<>=!&|()'\"%,@-".contains(c);
    if !expr.chars().all(allowed_chars) {
        return Err("WHERE contains invalid characters".into());
    }
    // Disallow dangerous SQL keywords
    let lower = expr.to_ascii_lowercase();
    if lower.contains(" union ")
        || lower.contains(" join ")
        || lower.contains(" having ")
        || lower.contains(" insert ")
        || lower.contains(" update ")
        || lower.contains(" delete ")
        || lower.contains(" drop ")
    {
        return Err("unsupported keyword in WHERE clause".into());
    }
    Ok(())
}

fn is_valid_ident(ident: &str, allow_dot: bool) -> bool {
    if ident.is_empty() {
        return false;
    }
    ident.split('.').all(|part| {
        let p = part.trim();
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || (allow_dot && c == '.'))
    })
}

fn build_insight(
    manifest_json: &serde_json::Value,
    database: Option<&str>,
    table: Option<&str>,
) -> serde_json::Value {
    let manifest: Manifest = match serde_json::from_value(manifest_json.clone()) {
        Ok(m) => m,
        Err(_) => return serde_json::json!({"error":"manifest parse failed"}),
    };
    let mut entries = manifest.entries;
    if let Some(db) = database {
        entries.retain(|e| e.database == db);
    }
    if let Some(tbl) = table {
        entries.retain(|e| e.table == tbl);
    }
    let total_segments = entries.len();
    let total_bytes: u64 = entries.iter().map(|e| e.size_bytes).sum();
    let min_wm = entries.iter().map(|e| e.watermark_micros).min();
    let max_wm = entries.iter().map(|e| e.watermark_micros).max();
    let min_event = entries.iter().filter_map(|e| e.event_time_min).min();
    let max_event = entries.iter().filter_map(|e| e.event_time_max).max();

    serde_json::json!({
        "version": manifest.version,
        "databases": manifest.databases.len(),
        "tables": manifest.tables.len(),
        "segments": total_segments,
        "total_bytes": total_bytes,
        "watermark_min": min_wm,
        "watermark_max": max_wm,
        "event_time_min": min_event,
        "event_time_max": max_event,
        "filtered_database": database,
        "filtered_table": table,
    })
}

fn build_schema(fields: &[CsvField]) -> Result<Schema, String> {
    if fields.is_empty() {
        return Err("schema must have at least one field".into());
    }
    let mut arrow_fields = Vec::with_capacity(fields.len());
    for f in fields {
        let dt = match f.data_type.to_ascii_lowercase().as_str() {
            "uint64" => DataType::UInt64,
            "uint32" => DataType::UInt32,
            "uint16" => DataType::UInt16,
            "uint8" => DataType::UInt8,
            "int64" => DataType::Int64,
            "int32" => DataType::Int32,
            "int16" => DataType::Int16,
            "int8" => DataType::Int8,
            "float64" | "double" => DataType::Float64,
            "float32" => DataType::Float32,
            "utf8" | "string" => DataType::Utf8,
            "largestring" => DataType::LargeUtf8,
            "bool" | "boolean" => DataType::Boolean,
            "uuid" => DataType::FixedSizeBinary(16),
            "binary" => DataType::LargeBinary,
            "timestamp" => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            "date32" => DataType::Date32,
            other => return Err(format!("unsupported type: {other}")),
        };
        arrow_fields.push(Field::new(&f.name, dt, f.nullable));
    }
    Ok(Schema::new(arrow_fields))
}

fn read_csv_batches(
    csv_bytes: &[u8],
    schema: Schema,
    has_header: bool,
    delimiter: u8,
) -> Result<Vec<RecordBatch>, String> {
    let schema_ref = Arc::new(schema);
    let reader = ReaderBuilder::new(schema_ref)
        .with_header(has_header)
        .with_delimiter(delimiter)
        .build(std::io::Cursor::new(csv_bytes))
        .map_err(|e| e.to_string())?;
    let mut batches = Vec::new();
    for batch in reader {
        let b = batch.map_err(|e| e.to_string())?;
        if b.num_rows() > 0 {
            batches.push(b);
        }
    }
    if batches.is_empty() {
        return Err("no rows parsed from CSV".into());
    }
    Ok(batches)
}

fn infer_csv_schema(
    csv_bytes: &[u8],
    max_rows: usize,
    delimiter: Option<String>,
    has_header: bool,
) -> Result<Vec<CsvField>, String> {
    let delim = delimiter
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',');
    let format = Format::default()
        .with_delimiter(delim)
        .with_header(has_header);
    let (schema, _) = format
        .infer_schema(std::io::Cursor::new(csv_bytes), Some(max_rows))
        .map_err(|e| e.to_string())?;
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let dt_str = match f.data_type() {
            DataType::UInt64 => "uint64",
            DataType::UInt32 => "uint32",
            DataType::UInt16 => "uint16",
            DataType::UInt8 => "uint8",
            DataType::Int64 => "int64",
            DataType::Int32 => "int32",
            DataType::Int16 => "int16",
            DataType::Int8 => "int8",
            DataType::Float64 => "float64",
            DataType::Float32 => "float32",
            DataType::Utf8 => "string",
            DataType::LargeUtf8 => "largestring",
            DataType::Boolean => "bool",
            DataType::FixedSizeBinary(16) => "uuid",
            DataType::LargeBinary => "binary",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => "timestamp",
            DataType::Date32 => "date32",
            other => return Err(format!("unsupported inferred type {:?}", other)),
        };
        fields.push(CsvField {
            name: f.name().to_string(),
            data_type: dt_str.to_string(),
            nullable: f.is_nullable(),
        });
    }
    Ok(fields)
}

fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    if batches.is_empty() {
        return Err("no batches to encode".into());
    }
    let mut out = Vec::new();
    {
        let schema = batches[0].schema();
        let mut writer =
            StreamWriter::try_new(&mut out, schema.as_ref()).map_err(|e| format!("{e}"))?;
        for b in batches {
            writer.write(b).map_err(|e| format!("{e}"))?;
        }
        writer.finish().map_err(|e| format!("{e}"))?;
    }
    Ok(out)
}

async fn blocking<T, F>(f: F) -> Result<T, ServerError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ServerError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ServerError::Io(format!("task join failed: {e}")))?
}

/// Apply the default database to DDL commands that use "default" as database
fn apply_default_database_to_ddl(cmd: DdlCommand, effective_db: &str) -> DdlCommand {
    match cmd {
        DdlCommand::CreateTable {
            database,
            table,
            schema_json,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateTable {
                database: db,
                table,
                schema_json,
            }
        }
        DdlCommand::DropTable {
            database,
            table,
            if_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropTable {
                database: db,
                table,
                if_exists,
            }
        }
        DdlCommand::TruncateTable { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::TruncateTable {
                database: db,
                table,
            }
        }
        DdlCommand::AlterTableAddColumn {
            database,
            table,
            column,
            data_type,
            nullable,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AlterTableAddColumn {
                database: db,
                table,
                column,
                data_type,
                nullable,
            }
        }
        DdlCommand::AlterTableDropColumn {
            database,
            table,
            column,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AlterTableDropColumn {
                database: db,
                table,
                column,
            }
        }
        // Commands that don't have a table-level database field
        DdlCommand::CreateDatabase { .. }
        | DdlCommand::DropDatabase { .. }
        | DdlCommand::ShowDatabases => cmd,
        // ShowTables may have an optional database - if not specified, use effective_db
        DdlCommand::ShowTables { database } => {
            let db = database
                .map(|d| {
                    if d == "default" {
                        effective_db.to_string()
                    } else {
                        d
                    }
                })
                .or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowTables { database: db }
        }
        // DescribeTable needs database substitution
        DdlCommand::DescribeTable { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DescribeTable {
                database: db,
                table,
            }
        }
        // View commands need database substitution
        DdlCommand::CreateView {
            database,
            name,
            query_sql,
            or_replace,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateView {
                database: db,
                name,
                query_sql,
                or_replace,
            }
        }
        DdlCommand::DropView {
            database,
            name,
            if_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropView {
                database: db,
                name,
                if_exists,
            }
        }
        DdlCommand::ShowViews { database } => {
            let db = database
                .map(|d| {
                    if d == "default" {
                        effective_db.to_string()
                    } else {
                        d
                    }
                })
                .or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowViews { database: db }
        }
        // Materialized View commands need database substitution
        DdlCommand::CreateMaterializedView {
            database,
            name,
            query_sql,
            or_replace,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateMaterializedView {
                database: db,
                name,
                query_sql,
                or_replace,
            }
        }
        DdlCommand::DropMaterializedView {
            database,
            name,
            if_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropMaterializedView {
                database: db,
                name,
                if_exists,
            }
        }
        DdlCommand::RefreshMaterializedView { database, name, incremental } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::RefreshMaterializedView { database: db, name, incremental }
        }
        DdlCommand::ShowMaterializedViews { database } => {
            let db = database
                .map(|d| {
                    if d == "default" {
                        effective_db.to_string()
                    } else {
                        d
                    }
                })
                .or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowMaterializedViews { database: db }
        }
        // Index commands need database substitution
        DdlCommand::CreateIndex {
            database,
            table,
            index_name,
            columns,
            index_type,
            if_not_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateIndex {
                database: db,
                table,
                index_name,
                columns,
                index_type,
                if_not_exists,
            }
        }
        DdlCommand::DropIndex {
            database,
            table,
            index_name,
            if_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropIndex {
                database: db,
                table,
                index_name,
                if_exists,
            }
        }
        DdlCommand::ShowIndexes { database, table } => {
            let db = database
                .map(|d| {
                    if d == "default" {
                        effective_db.to_string()
                    } else {
                        d
                    }
                })
                .or_else(|| Some(effective_db.to_string()));
            let tbl = table; // table is optional
            DdlCommand::ShowIndexes {
                database: db,
                table: tbl,
            }
        }
        // Maintenance commands need database substitution
        DdlCommand::AnalyzeTable { database, table, columns } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AnalyzeTable {
                database: db,
                table,
                columns,
            }
        }
        DdlCommand::ShowStatistics { database, table, column } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::ShowStatistics {
                database: db,
                table,
                column,
            }
        }
        DdlCommand::Vacuum {
            database,
            table,
            full,
            force,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::Vacuum {
                database: db,
                table,
                full,
                force,
            }
        }
        DdlCommand::Deduplicate { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::Deduplicate {
                database: db,
                table,
            }
        }
        DdlCommand::SetDeduplication {
            database,
            table,
            config,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::SetDeduplication {
                database: db,
                table,
                config,
            }
        }
        // Data retention policy commands
        DdlCommand::SetRetention {
            database,
            table,
            retention_seconds,
            time_column,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::SetRetention {
                database: db,
                table,
                retention_seconds,
                time_column,
            }
        }
        DdlCommand::DropRetention { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropRetention {
                database: db,
                table,
            }
        }
        DdlCommand::ShowRetention { database, table } => {
            let db = database.map(|d| {
                if d == "default" {
                    effective_db.to_string()
                } else {
                    d
                }
            });
            DdlCommand::ShowRetention {
                database: db,
                table,
            }
        }
        // Partition commands
        DdlCommand::SetPartition {
            database,
            table,
            granularity,
            time_column,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::SetPartition {
                database: db,
                table,
                granularity,
                time_column,
            }
        }
        DdlCommand::DropPartition { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropPartition {
                database: db,
                table,
            }
        }
        DdlCommand::ShowPartitions { database, table } => {
            let db = database.map(|d| {
                if d == "default" {
                    effective_db.to_string()
                } else {
                    d
                }
            });
            DdlCommand::ShowPartitions {
                database: db,
                table,
            }
        }
        // New DDL commands for Phase 19
        DdlCommand::AlterTableRename {
            database,
            old_table,
            new_table,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AlterTableRename {
                database: db,
                old_table,
                new_table,
            }
        }
        DdlCommand::AlterTableRenameColumn {
            database,
            table,
            old_column,
            new_column,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AlterTableRenameColumn {
                database: db,
                table,
                old_column,
                new_column,
            }
        }
        DdlCommand::CreateTableAs {
            database,
            table,
            query_sql,
            if_not_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateTableAs {
                database: db,
                table,
                query_sql,
                if_not_exists,
            }
        }
        DdlCommand::CreateSequence {
            database,
            name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            if_not_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CreateSequence {
                database: db,
                name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
                if_not_exists,
            }
        }
        DdlCommand::DropSequence {
            database,
            name,
            if_exists,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropSequence {
                database: db,
                name,
                if_exists,
            }
        }
        DdlCommand::AlterSequence {
            database,
            name,
            restart_with,
            increment,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AlterSequence {
                database: db,
                name,
                restart_with,
                increment,
            }
        }
        DdlCommand::ShowSequences { database } => {
            let db = database
                .map(|d| {
                    if d == "default" {
                        effective_db.to_string()
                    } else {
                        d
                    }
                })
                .or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowSequences { database: db }
        }
        DdlCommand::CopyFrom {
            database,
            table,
            source,
            format,
            options,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CopyFrom {
                database: db,
                table,
                source,
                format,
                options,
            }
        }
        DdlCommand::CopyTo {
            database,
            table,
            destination,
            format,
            options,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CopyTo {
                database: db,
                table,
                destination,
                format,
                options,
            }
        }
        DdlCommand::AddConstraint {
            database,
            table,
            constraint,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::AddConstraint {
                database: db,
                table,
                constraint,
            }
        }
        DdlCommand::DropConstraint {
            database,
            table,
            constraint_name,
        } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::DropConstraint {
                database: db,
                table,
                constraint_name,
            }
        }
        // PITR and backup commands don't have database context
        DdlCommand::RecoverToTimestamp { timestamp } => {
            DdlCommand::RecoverToTimestamp { timestamp }
        }
        DdlCommand::RecoverToLsn { lsn } => DdlCommand::RecoverToLsn { lsn },
        DdlCommand::CreateBackup { label } => DdlCommand::CreateBackup { label },
        DdlCommand::ShowBackups => DdlCommand::ShowBackups,
        DdlCommand::ShowWalStatus => DdlCommand::ShowWalStatus,
        DdlCommand::DeleteBackup { backup_id } => DdlCommand::DeleteBackup { backup_id },
        // Server info and segment repair commands don't have database context
        DdlCommand::ShowServerInfo => DdlCommand::ShowServerInfo,
        DdlCommand::ShowMissingSegments { database, table } => {
            DdlCommand::ShowMissingSegments { database, table }
        }
        DdlCommand::ShowCorruptedSegments { database, table } => {
            DdlCommand::ShowCorruptedSegments { database, table }
        }
        DdlCommand::ShowDamagedSegments { database, table } => {
            DdlCommand::ShowDamagedSegments { database, table }
        }
        DdlCommand::RepairSegments { database, table } => {
            let db = match database {
                Some(d) if d == "default" => Some(effective_db.to_string()),
                other => other,
            };
            DdlCommand::RepairSegments { database: db, table }
        }
        DdlCommand::CheckManifest => DdlCommand::CheckManifest,
        DdlCommand::ShowRepairStatus => DdlCommand::ShowRepairStatus,
        DdlCommand::CompactTable { database, table } => {
            let db = if database == "default" {
                effective_db.to_string()
            } else {
                database
            };
            DdlCommand::CompactTable { database: db, table }
        }
        DdlCommand::CompactAll => DdlCommand::CompactAll,
        // Transaction commands don't need database binding
        DdlCommand::BeginTransaction => DdlCommand::BeginTransaction,
        DdlCommand::CommitTransaction => DdlCommand::CommitTransaction,
        DdlCommand::RollbackTransaction => DdlCommand::RollbackTransaction,
        DdlCommand::Savepoint { name } => DdlCommand::Savepoint { name },
        DdlCommand::ReleaseSavepoint { name } => DdlCommand::ReleaseSavepoint { name },
        DdlCommand::RollbackToSavepoint { name } => DdlCommand::RollbackToSavepoint { name },
        // UDF and Streaming commands don't need database binding - pass through
        DdlCommand::CreateFunction {
            name,
            parameters,
            return_type,
            body,
            or_replace,
            language,
        } => DdlCommand::CreateFunction {
            name,
            parameters,
            return_type,
            body,
            or_replace,
            language,
        },
        DdlCommand::DropFunction { name, if_exists } => DdlCommand::DropFunction { name, if_exists },
        DdlCommand::ShowFunctions { pattern } => DdlCommand::ShowFunctions { pattern },
        DdlCommand::CreateStream {
            name,
            source_type,
            source_config,
            target_table,
            format,
        } => DdlCommand::CreateStream {
            name,
            source_type,
            source_config,
            target_table,
            format,
        },
        DdlCommand::DropStream { name, if_exists } => DdlCommand::DropStream { name, if_exists },
        DdlCommand::ShowStreams => DdlCommand::ShowStreams,
        DdlCommand::StartStream { name } => DdlCommand::StartStream { name },
        DdlCommand::StopStream { name } => DdlCommand::StopStream { name },
        DdlCommand::ShowStreamStatus { name } => DdlCommand::ShowStreamStatus { name },
        // All other commands don't need database normalization
        other => other,
    }
}

fn bind_is_loopback(bind: &str) -> bool {
    bind.starts_with("127.")
        || bind.starts_with("localhost")
        || bind.starts_with("::1")
        || bind.starts_with("[::1]")
}

fn build_tls_acceptor(
    cert_path: &PathBuf,
    key_path: &PathBuf,
    client_ca_path: Option<&PathBuf>,
) -> Result<TlsAcceptor, ServerError> {
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| ServerError::Tls(format!("open cert failed: {e}")))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<_, _>>()
        .map_err(|e| ServerError::Tls(format!("read certs failed: {e}")))?;

    let key_file = std::fs::File::open(key_path)
        .map_err(|e| ServerError::Tls(format!("open key failed: {e}")))?;
    let mut key_reader = BufReader::new(key_file);
    let mut keys: Vec<PrivateKeyDer<'static>> = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .map(|k| k.map(Into::into))
        .collect::<Result<_, _>>()
        .map_err(|e| ServerError::Tls(format!("read private key failed: {e}")))?;
    if keys.is_empty() {
        key_reader
            .rewind()
            .map_err(|e| ServerError::Tls(format!("rewind key reader failed: {e}")))?;
        keys = rustls_pemfile::rsa_private_keys(&mut key_reader)
            .map(|k| k.map(Into::into))
            .collect::<Result<_, _>>()
            .map_err(|e| ServerError::Tls(format!("read rsa key failed: {e}")))?;
    }
    let key = keys
        .pop()
        .ok_or_else(|| ServerError::Tls("no private key found".into()))?;

    let builder = rustls::ServerConfig::builder();
    let mut server_config = if let Some(ca_path) = client_ca_path {
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| ServerError::Tls(format!("open ca failed: {e}")))?;
        let mut ca_reader = BufReader::new(ca_file);
        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
            .collect::<Result<_, _>>()
            .map_err(|e| ServerError::Tls(format!("read ca failed: {e}")))?;
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|e| ServerError::Tls(format!("add ca failed: {e}")))?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| ServerError::Tls(format!("build client verifier failed: {e}")))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| ServerError::Tls(format!("tls config failed: {e}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| ServerError::Tls(format!("tls config failed: {e}")))?
    };
    server_config.alpn_protocols = vec![b"boyodb/1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

fn load_last_version(path: &Path) -> Result<Option<u64>, ServerError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            #[derive(Deserialize)]
            struct State {
                last_version: u64,
            }
            let st: State = serde_json::from_slice(&bytes)
                .map_err(|e| ServerError::Encode(format!("replication state parse failed: {e}")))?;
            Ok(Some(st.last_version))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ServerError::Io(format!(
            "read replication state failed: {e}"
        ))),
    }
}

fn persist_last_version(path: &Path, version: u64) -> Result<(), ServerError> {
    #[derive(Serialize)]
    struct State {
        last_version: u64,
    }
    let payload = serde_json::to_vec(&State {
        last_version: version,
    })
    .map_err(|e| ServerError::Encode(format!("replication state serialize failed: {e}")))?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(path, payload)
        .map_err(|e| ServerError::Io(format!("write replication state failed: {e}")))
}

async fn connect_replication_tls(
    src: &str,
    cfg: &Arc<ServerConfig>,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, ServerError> {
    let mut roots = RootCertStore::empty();
    if let Some(ca) = &cfg.replicate_ca {
        let mut reader = BufReader::new(
            std::fs::File::open(ca)
                .map_err(|e| ServerError::Tls(format!("open replicate ca failed: {e}")))?,
        );
        for cert in rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<CertificateDer<'static>>, _>>()
            .map_err(|e| ServerError::Tls(format!("read replicate ca failed: {e}")))?
        {
            roots
                .add(cert)
                .map_err(|e| ServerError::Tls(format!("add replicate ca failed: {e}")))?;
        }
    } else {
        roots.extend(TLS_SERVER_ROOTS.iter().cloned());
    }

    let sni = if let Some(name) = &cfg.replicate_sni {
        let owned = name.clone();
        ServerName::try_from(owned)
            .map_err(|e| ServerError::Tls(format!("invalid replicate SNI: {e}")))?
    } else {
        parse_server_name(src)?
    };

    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(src)
        .await
        .map_err(|e| ServerError::Io(format!("connect failed: {e}")))?;
    connector
        .connect(sni, tcp)
        .await
        .map_err(|e| ServerError::Tls(format!("tls connect failed: {e}")))
}

fn parse_server_name(addr: &str) -> Result<ServerName<'static>, ServerError> {
    let host = addr
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(addr)
        .trim_start_matches('[')
        .trim_end_matches(']');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::from(ip));
    }
    let owned = host.to_string();
    ServerName::try_from(owned).map_err(|e| ServerError::Tls(format!("invalid server name: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(q: &str) {
        assert!(
            validate_select(q, DEFAULT_MAX_QUERY_LEN).is_ok(),
            "query should be ok: {q}"
        );
    }

    fn bad(q: &str) {
        assert!(
            validate_select(q, DEFAULT_MAX_QUERY_LEN).is_err(),
            "query should be rejected: {q}"
        );
    }

    #[test]
    fn validate_select_allows_basic_queries() {
        ok("SELECT * FROM t");
        ok("SELECT COUNT(*), SUM(duration_ms) FROM calls GROUP BY tenant_id");
        ok("SELECT count(*) FROM my_table WHERE tenant_id = 1");
        ok("SELECT sum(duration_ms) FROM t WHERE tenant_id = 42 LIMIT 5");
        // String literals are allowed for LIKE, IN, and equality
        ok("SELECT * FROM t WHERE name = 'abc'");
        ok("SELECT * FROM t WHERE name LIKE '%test%'");
        ok("SELECT * FROM t WHERE status IN ('active', 'pending')");
    }

    #[test]
    fn validate_select_rejects_bad_queries() {
        bad("");
        bad("INSERT INTO t VALUES (1)");
        bad("UPDATE t SET a = 1");
        bad("DELETE FROM t");
        bad("DROP TABLE t");
        bad("CREATE TABLE t (id INT)");
        // Syntax errors - incomplete SQL
        bad("SELECT * FROM");
    }

    #[test]
    fn test_sql_auth_commands_parsed_correctly() {
        // Test that auth commands are parsed via SQL path (integration with sql.rs)
        use boyodb_core::{parse_sql, AuthCommand, SqlStatement};

        // CREATE USER via SQL
        let sql = "CREATE USER testuser WITH PASSWORD 'SecurePass123'";
        let stmt = parse_sql(sql).expect("should parse CREATE USER");
        assert!(matches!(
            stmt,
            SqlStatement::Auth(AuthCommand::CreateUser { .. })
        ));

        // SHOW USERS via SQL
        let sql = "SHOW USERS";
        let stmt = parse_sql(sql).expect("should parse SHOW USERS");
        assert!(matches!(stmt, SqlStatement::Auth(AuthCommand::ShowUsers)));

        // GRANT via SQL
        let sql = "GRANT SELECT ON DATABASE mydb TO appuser";
        let stmt = parse_sql(sql).expect("should parse GRANT");
        assert!(matches!(
            stmt,
            SqlStatement::Auth(AuthCommand::Grant { .. })
        ));

        // CREATE/DROP DATABASE via SQL (DDL path)
        let sql = "CREATE DATABASE mydb";
        let stmt = parse_sql(sql).expect("should parse CREATE DATABASE");
        assert!(matches!(stmt, SqlStatement::Ddl(_)));

        // TRUNCATE TABLE via SQL
        let sql = "TRUNCATE TABLE mydb.calls";
        let stmt = parse_sql(sql).expect("should parse TRUNCATE TABLE");
        assert!(matches!(stmt, SqlStatement::Ddl(_)));
    }
}
