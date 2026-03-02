use arrow_array::RecordBatch;
use arrow_csv::reader::Format;
use arrow_csv::ReaderBuilder;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose, Engine as _};
use boyodb_core::TableMeta;
use boyodb_core::{
    parse_sql, AuthCommand, AuthError, AuthManager, ClusterConfig, ClusterManager, Db, DdlCommand,
    DeleteCommand, EngineConfig, GossipConfig, IngestBatch, InsertCommand, Manifest, Privilege,
    PrivilegeTarget, QueryRequest, SqlStatement, SqlValue, UpdateCommand,
};
use boyodb_core::planner_distributed::LocalPlan;
use boyodb_core::engine::EngineError;
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
    tls_acceptor: Option<TlsAcceptor>,
    log_requests: bool,
    client_auth: bool,
    allow_manifest_import: bool,
    retention_watermark: Option<u64>,
    compact_min_segments: usize,
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
            match self.entries.get(&id) {
                Some(entry) => {
                    if entry.created_at.elapsed() <= self.ttl {
                        break;
                    }
                }
                None => {}
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
    let mut decoder = zstd::stream::Decoder::new(cursor)
        .map_err(|e| format!("failed to create decoder: {e}"))?;

    // Pre-allocate with a reasonable size, capped at max_bytes
    let initial_capacity = compressed.len().saturating_mul(4).min(max_bytes);
    let mut output = Vec::with_capacity(initial_capacity);

    // Read in chunks to check size incrementally
    let chunk_size = 64 * 1024; // 64KB chunks
    let mut buf = vec![0u8; chunk_size];

    loop {
        let n = decoder.read(&mut buf).map_err(|e| format!("decompression error: {e}"))?;
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
        eprintln!("Usage: boyodb-server <data_dir> [bind_addr] [--token <token>] [--auth] [--max-ipc-bytes <bytes>] [--max-conns <n>] [--workers <n>] [--io-timeout-ms <ms>] [--max-frame-bytes <bytes>] [--max-query-len <bytes>] [--query-cache-bytes <bytes>] [--plan-cache-size <n>] [--plan-cache-ttl-secs <n>] [--prepared-cache-size <n>] [--prepared-cache-ttl-secs <n>] [--segment-cache <n>] [--batch-cache-bytes <bytes>] [--schema-cache-entries <n>] [--log-requests] [--wal-dir <path>] [--wal-max-bytes <bytes>] [--wal-max-segments <n>] [--allow-manifest-import] [--retention-watermark <micros>] [--compact-min-segments <n>] [--maintenance-interval-ms <ms>] [--index-granularity-rows <n>] [--tier-warm-compression <algo>] [--tier-cold-compression <algo>] [--cache-hot-segments|--no-cache-hot-segments] [--cache-warm-segments|--no-cache-warm-segments] [--cache-cold-segments|--no-cache-cold-segments] [--tls-cert <path> --tls-key <path> --tls-ca <path>]");
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
    let mut worker_threads: usize = 8;
    let mut io_timeout_ms: u64 = DEFAULT_IO_TIMEOUT_MILLIS;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_ca: Option<PathBuf> = None;
    let mut log_requests = false;
    let mut allow_manifest_import = false;
    let mut retention_watermark: Option<u64> = None;
    let mut compact_min_segments: usize = 2;
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
        tls_acceptor,
        log_requests,
        client_auth,
        allow_manifest_import,
        retention_watermark,
        compact_min_segments,
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
        .with_cache_cold_segments(cfg.cache_cold_segments);
    engine_cfg.allow_manifest_import = cfg.allow_manifest_import;
    engine_cfg.retention_watermark_micros = cfg.retention_watermark;
    engine_cfg.compact_min_segments = cfg.compact_min_segments;
    engine_cfg.segment_cache_capacity = cfg.segment_cache_capacity;
    let db = Arc::new(Db::open(engine_cfg).map_err(|e| format!("db open failed: {e}"))?);
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
            rpc_addr: rpc_addr.parse().map_err(|e| format!("invalid rpc_addr: {e}"))?,
            gossip_addr: gossip_addr.parse().map_err(|e| format!("invalid gossip_addr: {e}"))?,
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
        let mode_str = if cfg.two_node_mode { "two-node" } else { "standard" };
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
                boyodb_core::cluster::start_gossip_sender(gossip_rx, shutdown_flag)
                    .await
            {
                tracing::error!("gossip sender error: {}", e);
            }
        });

        Some(manager)
    } else {
        None
    };

    // Spawn PostgreSQL Server
    if let Some(pg_port) = cfg.pg_port {
        let db_pg = db.clone();
        let auth_for_pg = auth_manager.clone();
        let pg_bind_ip = cfg.bind_addr.split(':').next().unwrap_or("0.0.0.0");
        let pg_addr = format!("{}:{}", pg_bind_ip, pg_port);
        
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
            // Try referencing commonly available path
            // If tokio module is not found in api, try top level or just assume impl
            
            // Actually, we called process_socket(stream, None, factory, factory, factory)
            // This function signature is likely:
            // process_socket(socket, ssl, startup_maker, query_maker, extended_maker)
            
            // Let's assume the function is in `pgwire::api`? Or `pgwire::messages`?
            // "could not find tokio in api" -> pgwire::api::tokio doesn't exist.
            // Maybe pgwire::api::process_socket exists?
            
            // To be safe, I'll use full path if I can guess.
            // But better: check valid paths.
            // I'll try `pgwire::api::process_socket`.
            
            let factory = Arc::new(MakeBoyodbPgHandler::new(db_pg));
            
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        let factory = factory.clone();
                        let auth_for_pg = auth_for_pg.clone();
                        tokio::spawn(async move {
                            // Using fully qualified path to avoid import error if possible, or try common one
                            // pgwire 0.24 usually exposes it at root or api logic.
                            // pgwire 0.20 uses api::process_socket and single factory
                            // pgwire 0.20 tokio::process_socket requires 3 handlers (Startup, Simple, Extended)
                            // We use the same handler instance for all.
                            let handler = factory.make();
                            // Probe Md5PasswordAuthStartupHandler
                            // use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
                            // let startup_handler = Arc::new(Md5PasswordAuthStartupHandler::new(factory.clone()));
                            // Use Cleartext Auth
                            // Use Cleartext Auth
                            let startup_handler = Arc::new(BoyodbPgAuthHandler::new(auth_for_pg));
                            if let Err(e) = pgwire::tokio::process_socket(
                                stream,
                                None,
                                startup_handler,
                                handler.clone(),
                                handler.clone()
                            ).await {
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
        });
    }

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
        "boyodb-server listening on {} (max_ipc_bytes={}, wal_dir={}, wal_max_bytes={}, wal_max_segments={}, max_conns={}, workers={}, auth={}, tls={}, replicate_from={}, replicate_tls={})",
        cfg.bind_addr,
        cfg.max_ipc_bytes,
        cfg.wal_dir.display(),
        cfg.wal_max_bytes,
        cfg.wal_max_segments,
        cfg.max_connections,
        cfg.worker_threads,
        auth_mode,
        tls_mode,
        cfg.replicate_from.clone().unwrap_or_else(|| "none".into()),
        if cfg.replicate_use_tls { "yes" } else { "no" }
    );

    let limiter = Arc::new(Semaphore::new(cfg.max_connections));

    // Rate limiter for authentication: 10 attempts per 60 seconds per IP
    let auth_rate_limiter = Arc::new(AuthRateLimiter::new(10, 60));

    let cfg = Arc::new(cfg);

    // Track active connections for graceful shutdown
    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Set up shutdown signal handler
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_clone = shutdown.clone();
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

        shutdown_clone.notify_waiters();
    });

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

    if let Some(src) = cfg.replicate_from.clone() {
        let cfg_clone = cfg.clone();
        let db_clone = db.clone();
        let state_path = cfg.data_dir.join("replication_state.json");
        let initial_version = load_last_version(&state_path).unwrap_or(None);
        tokio::spawn(async move {
            replication_loop(src, cfg_clone, db_clone, state_path, initial_version).await;
        });
    }

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
                        warn!(addr = %addr, "connection rejected: max connections reached");
                        continue;
                    }
                };
                let db = db.clone();
                let cfg = cfg.clone();
                let auth = auth_manager.clone();
                let cluster = cluster_manager.clone();
                let prepared_cache = prepared_cache.clone();
                let active = active_connections.clone();
                let rate_limiter = auth_rate_limiter.clone();
                active.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                tokio::spawn(async move {
                    if let Err(e) = handle_conn(
                        stream,
                        db,
                        cfg.clone(),
                        auth,
                        cluster,
                        prepared_cache,
                        rate_limiter,
                        addr.ip(),
                    )
                    .await
                    {
                        debug!(addr = %addr, error = %e, "connection error");
                    }
                    active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
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
    let active_count = active_connections.load(std::sync::atomic::Ordering::SeqCst);
    if active_count > 0 {
        info!(
            active_connections = active_count,
            "waiting for active connections to complete"
        );
        let shutdown_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();
        while active_connections.load(std::sync::atomic::Ordering::SeqCst) > 0 {
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
    peer_ip: IpAddr,
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
                        match auth.authenticate(username, password, Some(&peer_ip.to_string()), None) {
                            Ok(session_id) => {
                                rate_limiter.record_success(peer_ip);
                                info!(
                                    username = %username,
                                    client_ip = %peer_ip,
                                    "HTTP authentication successful"
                                );
                                authenticated_user = Some(username.clone());
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

        // Process the request
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
                                let compressed = zstd::stream::encode_all(
                                    std::io::Cursor::new(&response.records_ipc),
                                    3,
                                )
                                .unwrap_or_default();
                                resp.ipc_base64 = Some(general_purpose::STANDARD.encode(compressed));
                                resp.compression = Some("zstd".to_string());
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
) -> Result<Response, ServerError> {
    // Helper to check privilege when auth is enabled
    let require_privilege =
        |priv_type: Privilege, target: PrivilegeTarget| -> Result<(), ServerError> {
            if let (Some(auth), Some(user)) = (&auth_manager, &authenticated_user) {
                auth.require_privilege(user, priv_type, &target)?;
            }
            Ok(())
        };
    match req {
        Request::ExecuteSubQuery { .. } => todo!(),
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
                    execute_ddl_command(&db, adjusted_cmd, &require_privilege).await
                }
                SqlStatement::Query(parsed) => {
                    // Determine effective databases for primary and join tables when a default is supplied
                    let main_db = parsed.database.as_deref()
                        .filter(|d| *d != "default")
                        .unwrap_or(effective_db)
                        .to_string();

                    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
                    table_refs.push((main_db.clone(), parsed.table.clone().unwrap_or_else(|| "unknown".to_string()), parsed.database.as_deref() == Some("default") || parsed.database.is_none()));
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
                    let db = db.clone();
                    let resp = blocking(move || {
                        db.query(QueryRequest {
                            sql: effective_sql,
                            timeout_millis,
                            collect_stats: false,
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
                SqlStatement::Explain { analyze, statement } => {
                    // Execute EXPLAIN [ANALYZE] queries
                    let explain_result = execute_explain(&db, *statement, analyze, effective_db, &require_privilege).await?;
                    Ok(Response {
                        message: Some(explain_result),
                        ..Default::default()
                    })
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

            let main_db = parsed.database.as_deref()
                .filter(|d| *d != "default")
                .unwrap_or(effective_db)
                .to_string();

            let mut table_refs: Vec<(String, String, bool)> = Vec::new();
            table_refs.push((main_db.clone(), parsed.table.clone().unwrap_or_else(|| "unknown".to_string()), parsed.database.as_deref() == Some("default") || parsed.database.is_none()));
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
                db.apply_bundle(payload)
                    .map_err(map_engine_error)
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
            let metrics_text = blocking(move || Ok(db.prometheus_metrics())).await?;
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
    }
}

#[derive(Clone, Copy)]
struct QueryBinaryMeta {
    data_skipped_bytes: u64,
    segments_scanned: usize,
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
    let stmt = parse_sql(&sql).map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;

    let parsed = match stmt {
        SqlStatement::Query(parsed) => parsed,
        _ => {
            return Err(ServerError::BadRequest(
                "query_binary only supports SELECT queries".into(),
            ))
        }
    };

    let main_db = parsed.database.as_deref()
        .filter(|d| *d != "default")
        .unwrap_or(effective_db)
        .to_string();

    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
    table_refs.push((main_db.clone(), parsed.table.clone().unwrap_or_else(|| "unknown".to_string()), parsed.database.as_deref() == Some("default") || parsed.database.is_none()));
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
    let query_request = QueryRequest {
        sql: effective_sql,
        timeout_millis,
        collect_stats: false,
    };
    let db = db.clone();
    let meta = match blocking(move || {
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
    .await
    {
        Ok(meta) => meta,
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    };

    let mut payload_path = tmp_path;
    let mut payload_len = std::fs::metadata(&payload_path)
        .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
        .len();
    let mut compression_used: Option<&'static str> = None;
    if payload_len > 0 && matches!(accept_compression.as_deref(), Some("zstd")) {
        let (compressed_path, compressed_file) = create_temp_ipc_file(&data_dir)?;
        let mut input = std::fs::File::open(&payload_path)
            .map_err(|e| ServerError::Io(format!("open payload failed: {e}")))?;
        let encoder = zstd::stream::Encoder::new(compressed_file, 3)
            .map_err(|e| ServerError::Encode(format!("compression init failed: {e}")))?;
        let mut encoder = encoder.auto_finish();
        std::io::copy(&mut input, &mut encoder)
            .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
        drop(encoder);
        let _ = std::fs::remove_file(&payload_path);
        payload_path = compressed_path;
        payload_len = std::fs::metadata(&payload_path)
            .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
            .len();
        compression_used = Some("zstd");
    }
    if payload_len > max_frame_len as u64 {
        return Err(ServerError::BadRequest(format!(
            "response too large ({} > {})",
            payload_len,
            max_frame_len
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
    let stmt = parse_sql(&sql).map_err(|e| ServerError::BadRequest(format!("SQL parse error: {e}")))?;

    let parsed = match stmt {
        SqlStatement::Query(parsed) => parsed,
        _ => {
            return Err(ServerError::BadRequest(
                "query_binary only supports SELECT queries".into(),
            ))
        }
    };

    let main_db = parsed.database.as_deref()
        .filter(|d| *d != "default")
        .unwrap_or(effective_db)
        .to_string();

    let mut table_refs: Vec<(String, String, bool)> = Vec::new();
    table_refs.push((main_db.clone(), parsed.table.clone().unwrap_or_else(|| "unknown".to_string()), parsed.database.as_deref() == Some("default") || parsed.database.is_none()));
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
    };
    let db = db.clone();
    let chunk_size = max_frame_len.min(256 * 1024).max(1024);
    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<Vec<u8>, ServerError>>();
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
                        let _ = write_frame_bytes(stream, &[], timeout_dur, max_frame_len).await;
                        let _ = e;
                        return Ok(());
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
    let query_request = QueryRequest {
        sql: prepared.sql.clone(),
        timeout_millis,
        collect_stats: false,
    };
    let db = db.clone();
    let meta = match blocking(move || {
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
    .await
    {
        Ok(meta) => meta,
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
    };

    let mut payload_path = tmp_path;
    let mut payload_len = std::fs::metadata(&payload_path)
        .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
        .len();
    let mut compression_used: Option<&'static str> = None;
    if payload_len > 0 && matches!(accept_compression.as_deref(), Some("zstd")) {
        let (compressed_path, compressed_file) = create_temp_ipc_file(&data_dir)?;
        let mut input = std::fs::File::open(&payload_path)
            .map_err(|e| ServerError::Io(format!("open payload failed: {e}")))?;
        let encoder = zstd::stream::Encoder::new(compressed_file, 3)
            .map_err(|e| ServerError::Encode(format!("compression init failed: {e}")))?;
        let mut encoder = encoder.auto_finish();
        std::io::copy(&mut input, &mut encoder)
            .map_err(|e| ServerError::Encode(format!("compression failed: {e}")))?;
        drop(encoder);
        let _ = std::fs::remove_file(&payload_path);
        payload_path = compressed_path;
        payload_len = std::fs::metadata(&payload_path)
            .map_err(|e| ServerError::Io(format!("read payload metadata failed: {e}")))?
            .len();
        compression_used = Some("zstd");
    }
    if payload_len > max_frame_len as u64 {
        return Err(ServerError::BadRequest(format!(
            "response too large ({} > {})",
            payload_len,
            max_frame_len
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
    };
    let db = db.clone();
    let chunk_size = max_frame_len.min(256 * 1024).max(1024);
    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<Vec<u8>, ServerError>>();
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
                        let _ = write_frame_bytes(stream, &[], timeout_dur, max_frame_len).await;
                        let _ = e;
                        return Ok(());
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
            Ok(Response::ok_message(&format!(
                "{} granted to '{}'",
                privileges.join(", "),
                grantee
            )))
        }
        AuthCommand::Revoke {
            privileges,
            target_type,
            target_name,
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
                    auth.revoke_privilege_from_role(
                        &grantee,
                        priv_type,
                        target.clone(),
                        actor,
                    )?;
                } else {
                    auth.revoke_privilege(&grantee, priv_type, target.clone(), actor)?;
                }
            }
            Ok(Response::ok_message(&format!(
                "{} revoked from '{}'",
                privileges.join(", "),
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
                require_privilege(Privilege::Select, PrivilegeTarget::Database(db_name.clone()))?;
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
            require_privilege(
                Privilege::Drop,
                PrivilegeTarget::Database(database.clone()),
            )?;
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
                require_privilege(Privilege::Select, PrivilegeTarget::Database(db_name.clone()))?;
            }
            let db = db.clone();
            let views = blocking(move || {
                db.list_views(database.as_deref())
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            // Return views as a list of (database, name, query) tuples
            let view_names: Vec<String> = views.iter().map(|(db, name, _)| format!("{}.{}", db, name)).collect();
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
            require_privilege(
                Privilege::Drop,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.drop_materialized_view(&database, &name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("materialized view dropped"))
        }
        DdlCommand::RefreshMaterializedView { database, name } => {
            require_privilege(
                Privilege::Update,
                PrivilegeTarget::Database(database.clone()),
            )?;
            let db = db.clone();
            blocking(move || {
                db.refresh_materialized_view(&database, &name)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("materialized view refreshed"))
        }
        DdlCommand::ShowMaterializedViews { database } => {
            // If database is specified, check privilege for that database
            if let Some(ref db_name) = database {
                require_privilege(Privilege::Select, PrivilegeTarget::Database(db_name.clone()))?;
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
                db.create_index(&database, &table, &index_name, &columns, index_type, if_not_exists)
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
                    format!("{}.{}.{} ({:?}) on ({})", db_name, tbl_name, idx_name, idx_type, cols.join(", "))
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
        DdlCommand::AnalyzeTable { database, table } => {
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
        DdlCommand::Vacuum {
            database,
            table,
            full,
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
                db.vacuum(&database, &table, full)
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
        // New DDL commands
        DdlCommand::AlterTableRename { database, old_table, new_table } => {
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
        DdlCommand::AlterTableRenameColumn { database, table, old_column, new_column } => {
            require_privilege(
                Privilege::Alter,
                PrivilegeTarget::Table {
                    database: database.clone(),
                    table: table.clone(),
                },
            )?;
            // TODO: Implement column rename in engine
            Err(ServerError::NotImplemented(format!(
                "ALTER TABLE RENAME COLUMN is not yet implemented (rename {} to {})",
                old_column, new_column
            )))
        }
        DdlCommand::CreateTableAs { database, table, query_sql, if_not_exists } => {
            require_privilege(Privilege::Create, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            let rows_inserted = blocking(move || {
                db.create_table_as(&database, &table, &query_sql, if_not_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message(&format!("table created with {} rows", rows_inserted)))
        }
        DdlCommand::CreateSequence { database, name, start, increment, min_value, max_value, cycle, if_not_exists } => {
            require_privilege(Privilege::Create, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.create_sequence(&database, &name, start, increment, min_value, max_value, cycle, if_not_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("sequence created"))
        }
        DdlCommand::DropSequence { database, name, if_exists } => {
            require_privilege(Privilege::Drop, PrivilegeTarget::Database(database.clone()))?;
            let db = db.clone();
            blocking(move || {
                db.drop_sequence(&database, &name, if_exists)
                    .map_err(|e| ServerError::Db(e.to_string()))
            })
            .await?;
            Ok(Response::ok_message("sequence dropped"))
        }
        DdlCommand::AlterSequence { database, name, restart_with, increment } => {
            require_privilege(Privilege::Alter, PrivilegeTarget::Database(database.clone()))?;
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
                require_privilege(Privilege::Select, PrivilegeTarget::Database(db_name.clone()))?;
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
                    .map(|s| format!("{}.{} (current: {}, increment: {})", s.database, s.name, s.current_value, s.increment))
                    .collect();
                Ok(Response {
                    message: Some(format!("Sequences: {}", seq_list.join("; "))),
                    ..Default::default()
                })
            }
        }
        DdlCommand::CopyFrom { database, table, source, format, options } => {
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
            Ok(Response::ok_message(&format!("COPY {} rows imported", rows_copied)))
        }
        DdlCommand::CopyTo { database, table, destination, format, options } => {
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
            Ok(Response::ok_message(&format!("COPY {} rows exported to {}", rows_copied, dest_clone)))
        }
        DdlCommand::AddConstraint { database, table, constraint } => {
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
        DdlCommand::DropConstraint { database, table, constraint_name } => {
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
        on_conflict: _on_conflict, // TODO: implement UPSERT logic
        returning,
    } = cmd;

    if values.is_empty() {
        return Err(ServerError::BadRequest("INSERT requires at least one row".into()));
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
    let schema_json = table_desc.schema_json.as_ref().ok_or_else(|| {
        ServerError::BadRequest("table has no schema defined".into())
    })?;
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
                            let hex_str: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                            if hex_str.len() != 32 {
                                return Err(ServerError::BadRequest(format!(
                                    "invalid UUID for column '{}': expected 32 hex chars, got {}",
                                    col_name, hex_str.len()
                                )));
                            }
                            let bytes = hex::decode(&hex_str).map_err(|e| {
                                ServerError::BadRequest(format!("invalid UUID hex for column '{}': {}", col_name, e))
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
                    .map_err(|e| ServerError::BadRequest(format!("invalid decimal config: {}", e)))?;
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
                            let (value, parsed_scale) = parse_decimal_string(s).ok_or_else(|| {
                                ServerError::BadRequest(format!("invalid decimal for column '{}': {}", col_name, s))
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
                            let hex_str = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
                            let bytes = hex::decode(hex_str).map_err(|e| {
                                ServerError::BadRequest(format!("invalid binary hex for column '{}': {}", col_name, e))
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
                let reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| ServerError::Decode(format!("failed to read returning data: {}", e)))?;

                let batches: Vec<RecordBatch> = reader
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| ServerError::Decode(format!("failed to read batches: {}", e)))?;

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
                        let selected_batch = RecordBatch::try_new(selected_schema.clone(), selected_arrays)
                            .map_err(|e| ServerError::Encode(format!("failed to create returning batch: {}", e)))?;

                        let mut output = Vec::new();
                        {
                            let mut writer = StreamWriter::try_new(&mut output, &selected_schema)
                                .map_err(|e| ServerError::Encode(format!("failed to write returning: {}", e)))?;
                            writer.write(&selected_batch)
                                .map_err(|e| ServerError::Encode(format!("failed to write batch: {}", e)))?;
                            writer.finish()
                                .map_err(|e| ServerError::Encode(format!("failed to finish writer: {}", e)))?;
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

    Ok(Response::ok_message(&format!("{} row(s) inserted", rows_inserted)))
}

async fn execute_update(_db: &Arc<Db>, cmd: UpdateCommand) -> Result<Response, ServerError> {
    let UpdateCommand {
        database,
        table,
        assignments,
        where_clause,
        returning,
    } = cmd;

    let db = _db.clone();
    let db_name = database.clone();
    let tbl_name = table.clone();
    let where_cl = where_clause.clone();
    let ret_cols = returning.clone();

    let updated = blocking(move || {
        db.update_rows(&database, &table, &assignments, where_clause.as_deref())
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    // Handle RETURNING clause for UPDATE
    // Note: For full RETURNING support, we would need to capture the modified rows
    // before and after the update. For now, we'll return a count message.
    // TODO: Implement full RETURNING support by returning the modified rows
    if ret_cols.is_some() {
        // Future: Query the updated rows and return them
        // For now, return a structured message indicating the update count
        Ok(Response::ok_message(&format!(
            "{} row(s) updated (RETURNING clause support is limited)",
            updated
        )))
    } else {
        Ok(Response::ok_message(&format!(
            "{} row(s) updated",
            updated
        )))
    }
}

async fn execute_delete(_db: &Arc<Db>, cmd: DeleteCommand) -> Result<Response, ServerError> {
    let DeleteCommand {
        database,
        table,
        where_clause,
        returning,
    } = cmd;

    let db = _db.clone();
    let ret_cols = returning.clone();

    let deleted = blocking(move || {
        db.delete_rows(&database, &table, where_clause.as_deref())
            .map_err(|e| ServerError::Db(e.to_string()))
    })
    .await?;

    // Handle RETURNING clause for DELETE
    // Note: For full RETURNING support, we would need to capture the deleted rows
    // before deletion. For now, we'll return a count message.
    // TODO: Implement full RETURNING support by returning the deleted rows
    if ret_cols.is_some() {
        Ok(Response::ok_message(&format!(
            "{} row(s) deleted (RETURNING clause support is limited)",
            deleted
        )))
    } else {
        Ok(Response::ok_message(&format!(
            "{} row(s) deleted",
            deleted
        )))
    }
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
            let main_db = parsed.database.as_deref()
                .filter(|d| *d != "default")
                .unwrap_or(effective_db)
                .to_string();

            require_privilege(
                Privilege::Select,
                PrivilegeTarget::Table {
                    database: main_db.clone(),
                    table: parsed.table.clone().unwrap_or_else(|| "unknown".to_string()),
                },
            )?;

            explain_output.push_str(&format!("Query Plan:\n"));
            explain_output.push_str(&format!("  -> Scan on {}.{}\n", main_db, parsed.table.clone().unwrap_or_else(|| "unknown".to_string())));

            if !parsed.joins.is_empty() {
                for join in &parsed.joins {
                    let join_db = if join.database == "default" {
                        effective_db.to_string()
                    } else {
                        join.database.clone()
                    };
                    explain_output.push_str(&format!("  -> {:?} Join with {}.{}\n", join.join_type, join_db, join.table));
                    explain_output.push_str(&format!("       On: {}.{} = {}.{}\n",
                        main_db, join.on_condition.left_column,
                        join_db, join.on_condition.right_column));
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
                let sql = format!("SELECT * FROM {}.{} LIMIT 1", main_db, parsed.table.clone().unwrap_or_else(|| "unknown".to_string()));
                let result = blocking(move || {
                    db_clone.query(QueryRequest {
                        sql,
                        timeout_millis: 5000,
                        collect_stats: true,
                    })
                    .map_err(|e| ServerError::Db(e.to_string()))
                }).await;
                let elapsed = start.elapsed();

                explain_output.push_str("\nExecution Statistics:\n");
                explain_output.push_str("  Planning time: ~0.1 ms\n");

                // Use real stats if available
                if let Ok(ref resp) = result {
                    if let Some(ref stats) = resp.execution_stats {
                        explain_output.push_str(&format!("  Execution time: {:.2} ms\n", stats.execution_time_micros as f64 / 1000.0));
                        explain_output.push_str(&format!("  Segments: {} scanned, {} pruned (of {} total)\n",
                            stats.segments_scanned, stats.segments_pruned, stats.segments_total));
                        explain_output.push_str(&format!("  Pruning efficiency: {:.1}%\n", stats.pruning_efficiency()));
                        explain_output.push_str(&format!("  Bytes read: {}\n", stats.bytes_read));
                        explain_output.push_str(&format!("  Bytes skipped: {}\n", stats.bytes_skipped));
                        if stats.cache_hit {
                            explain_output.push_str("  Cache: HIT\n");
                        }
                    } else {
                        explain_output.push_str(&format!("  Execution time: {:.2} ms\n", elapsed.as_secs_f64() * 1000.0));
                        explain_output.push_str(&format!("  Segments scanned: {}\n", resp.segments_scanned));
                        explain_output.push_str(&format!("  Data skipped: {} bytes\n", resp.data_skipped_bytes));
                    }
                }
            }
        }
        SqlStatement::Insert(cmd) => {
            let db_name = if cmd.database == "default" { effective_db.to_string() } else { cmd.database };
            explain_output.push_str(&format!("Query Plan:\n"));
            explain_output.push_str(&format!("  -> Insert into {}.{}\n", db_name, cmd.table));
            explain_output.push_str(&format!("     Rows to insert: {}\n", cmd.values.len()));
        }
        SqlStatement::Update(cmd) => {
            let db_name = if cmd.database == "default" { effective_db.to_string() } else { cmd.database };
            explain_output.push_str(&format!("Query Plan:\n"));
            explain_output.push_str(&format!("  -> Update on {}.{}\n", db_name, cmd.table));
            explain_output.push_str(&format!("     Assignments: {}\n", cmd.assignments.len()));
            if cmd.where_clause.is_some() {
                explain_output.push_str(&format!("     Filter: WHERE clause present\n"));
            }
        }
        SqlStatement::Delete(cmd) => {
            let db_name = if cmd.database == "default" { effective_db.to_string() } else { cmd.database };
            explain_output.push_str(&format!("Query Plan:\n"));
            explain_output.push_str(&format!("  -> Delete from {}.{}\n", db_name, cmd.table));
            if cmd.where_clause.is_some() {
                explain_output.push_str(&format!("     Filter: WHERE clause present\n"));
            }
        }
        SqlStatement::Ddl(ddl) => {
            explain_output.push_str(&format!("Query Plan:\n"));
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

    if month < 1 || month > 12 || day < 1 || day > 31 {
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
    let allowed_chars = |c: char| {
        c.is_ascii_alphanumeric()
            || " _.<>=!&|()'\"%,@-".contains(c)
    };
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
        DdlCommand::CreateTable { database, table, schema_json } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateTable { database: db, table, schema_json }
        }
        DdlCommand::DropTable { database, table, if_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropTable { database: db, table, if_exists }
        }
        DdlCommand::TruncateTable { database, table } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::TruncateTable { database: db, table }
        }
        DdlCommand::AlterTableAddColumn { database, table, column, data_type, nullable } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AlterTableAddColumn { database: db, table, column, data_type, nullable }
        }
        DdlCommand::AlterTableDropColumn { database, table, column } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AlterTableDropColumn { database: db, table, column }
        }
        // Commands that don't have a table-level database field
        DdlCommand::CreateDatabase { .. } |
        DdlCommand::DropDatabase { .. } |
        DdlCommand::ShowDatabases => cmd,
        // ShowTables may have an optional database - if not specified, use effective_db
        DdlCommand::ShowTables { database } => {
            let db = database.map(|d| {
                if d == "default" { effective_db.to_string() } else { d }
            }).or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowTables { database: db }
        }
        // DescribeTable needs database substitution
        DdlCommand::DescribeTable { database, table } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DescribeTable { database: db, table }
        }
        // View commands need database substitution
        DdlCommand::CreateView { database, name, query_sql, or_replace } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateView { database: db, name, query_sql, or_replace }
        }
        DdlCommand::DropView { database, name, if_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropView { database: db, name, if_exists }
        }
        DdlCommand::ShowViews { database } => {
            let db = database.map(|d| {
                if d == "default" { effective_db.to_string() } else { d }
            }).or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowViews { database: db }
        }
        // Materialized View commands need database substitution
        DdlCommand::CreateMaterializedView { database, name, query_sql, or_replace } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateMaterializedView { database: db, name, query_sql, or_replace }
        }
        DdlCommand::DropMaterializedView { database, name, if_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropMaterializedView { database: db, name, if_exists }
        }
        DdlCommand::RefreshMaterializedView { database, name } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::RefreshMaterializedView { database: db, name }
        }
        DdlCommand::ShowMaterializedViews { database } => {
            let db = database.map(|d| {
                if d == "default" { effective_db.to_string() } else { d }
            }).or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowMaterializedViews { database: db }
        }
        // Index commands need database substitution
        DdlCommand::CreateIndex { database, table, index_name, columns, index_type, if_not_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateIndex { database: db, table, index_name, columns, index_type, if_not_exists }
        }
        DdlCommand::DropIndex { database, table, index_name, if_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropIndex { database: db, table, index_name, if_exists }
        }
        DdlCommand::ShowIndexes { database, table } => {
            let db = database.map(|d| {
                if d == "default" { effective_db.to_string() } else { d }
            }).or_else(|| Some(effective_db.to_string()));
            let tbl = table; // table is optional
            DdlCommand::ShowIndexes { database: db, table: tbl }
        }
        // Maintenance commands need database substitution
        DdlCommand::AnalyzeTable { database, table } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AnalyzeTable { database: db, table }
        }
        DdlCommand::Vacuum { database, table, full } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::Vacuum { database: db, table, full }
        }
        DdlCommand::Deduplicate { database, table } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::Deduplicate { database: db, table }
        }
        DdlCommand::SetDeduplication { database, table, config } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::SetDeduplication { database: db, table, config }
        }
        // New DDL commands for Phase 19
        DdlCommand::AlterTableRename { database, old_table, new_table } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AlterTableRename { database: db, old_table, new_table }
        }
        DdlCommand::AlterTableRenameColumn { database, table, old_column, new_column } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AlterTableRenameColumn { database: db, table, old_column, new_column }
        }
        DdlCommand::CreateTableAs { database, table, query_sql, if_not_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateTableAs { database: db, table, query_sql, if_not_exists }
        }
        DdlCommand::CreateSequence { database, name, start, increment, min_value, max_value, cycle, if_not_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CreateSequence { database: db, name, start, increment, min_value, max_value, cycle, if_not_exists }
        }
        DdlCommand::DropSequence { database, name, if_exists } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropSequence { database: db, name, if_exists }
        }
        DdlCommand::AlterSequence { database, name, restart_with, increment } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AlterSequence { database: db, name, restart_with, increment }
        }
        DdlCommand::ShowSequences { database } => {
            let db = database.map(|d| {
                if d == "default" { effective_db.to_string() } else { d }
            }).or_else(|| Some(effective_db.to_string()));
            DdlCommand::ShowSequences { database: db }
        }
        DdlCommand::CopyFrom { database, table, source, format, options } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CopyFrom { database: db, table, source, format, options }
        }
        DdlCommand::CopyTo { database, table, destination, format, options } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::CopyTo { database: db, table, destination, format, options }
        }
        DdlCommand::AddConstraint { database, table, constraint } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::AddConstraint { database: db, table, constraint }
        }
        DdlCommand::DropConstraint { database, table, constraint_name } => {
            let db = if database == "default" { effective_db.to_string() } else { database };
            DdlCommand::DropConstraint { database: db, table, constraint_name }
        }
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
