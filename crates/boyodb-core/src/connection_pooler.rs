//! Built-in Connection Pooler (PgBouncer-like)
//!
//! This module provides a PostgreSQL-compatible connection pooler similar to PgBouncer.
//! It supports multiple pooling modes, connection limits, and automatic health checking.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

// ============================================================================
// Password Authentication Utilities
// ============================================================================

/// Compute MD5 hash of input bytes
fn md5_hash(data: &[u8]) -> [u8; 16] {
    // Simple MD5 implementation (RFC 1321)
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];

    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    // Pad message
    let orig_len_bits = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&orig_len_bits.to_le_bytes());

    // Process blocks
    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    for chunk in msg.chunks(64) {
        let mut m = [0u32; 16];
        for (i, word) in chunk.chunks(4).enumerate() {
            m[i] = u32::from_le_bytes([word[0], word[1], word[2], word[3]]);
        }

        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };

            let f = f.wrapping_add(a).wrapping_add(K[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&a0.to_le_bytes());
    result[4..8].copy_from_slice(&b0.to_le_bytes());
    result[8..12].copy_from_slice(&c0.to_le_bytes());
    result[12..16].copy_from_slice(&d0.to_le_bytes());
    result
}

/// Convert bytes to hex string
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Compute PostgreSQL MD5 password hash
/// Format: "md5" + md5(md5(password + user) + salt)
pub fn compute_md5_password(password: &str, user: &str, salt: &[u8; 4]) -> String {
    // First hash: md5(password + user)
    let mut first_input = password.as_bytes().to_vec();
    first_input.extend_from_slice(user.as_bytes());
    let first_hash = md5_hash(&first_input);
    let first_hex = bytes_to_hex(&first_hash);

    // Second hash: md5(first_hex + salt)
    let mut second_input = first_hex.as_bytes().to_vec();
    second_input.extend_from_slice(salt);
    let second_hash = md5_hash(&second_input);

    format!("md5{}", bytes_to_hex(&second_hash))
}

/// User credentials stored in the pooler
#[derive(Debug, Clone)]
pub struct UserCredentials {
    /// Username
    pub username: String,
    /// Password (plain text for verification)
    password: String,
    /// Auth type required
    pub auth_type: AuthType,
}

impl UserCredentials {
    pub fn new(username: &str, password: &str, auth_type: AuthType) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            auth_type,
        }
    }

    /// Verify a plain text password
    pub fn verify_plain(&self, password: &str) -> bool {
        self.password == password
    }

    /// Verify an MD5 password response
    pub fn verify_md5(&self, response: &str, salt: &[u8; 4]) -> bool {
        let expected = compute_md5_password(&self.password, &self.username, salt);
        response == expected
    }
}

/// Pooling mode determines how server connections are assigned to clients
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMode {
    /// Connection is returned to pool after each transaction
    Transaction,
    /// Connection is held for the entire client session
    Session,
    /// Connection is returned after each statement (most aggressive)
    Statement,
}

impl Default for PoolMode {
    fn default() -> Self {
        PoolMode::Transaction
    }
}

/// Authentication type for backend connections
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthType {
    /// Trust (no password)
    Trust,
    /// MD5 password authentication
    Md5,
    /// SCRAM-SHA-256
    ScramSha256,
    /// Plain text password (not recommended)
    Plain,
}

impl Default for AuthType {
    fn default() -> Self {
        AuthType::ScramSha256
    }
}

/// Configuration for a database pool
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database name as seen by clients
    pub name: String,
    /// Actual database name on server (defaults to name)
    pub dbname: Option<String>,
    /// Backend host
    pub host: String,
    /// Backend port
    pub port: u16,
    /// Default user for connections
    pub user: Option<String>,
    /// Password for connections
    pub password: Option<String>,
    /// Authentication type
    pub auth_type: AuthType,
    /// Maximum connections to this database
    pub max_db_connections: usize,
    /// Minimum pool size (idle connections to maintain)
    pub min_pool_size: usize,
    /// Reserve pool size for additional burst capacity
    pub reserve_pool_size: usize,
    /// Pool mode for this database
    pub pool_mode: PoolMode,
    /// Maximum client connections waiting for a server
    pub max_client_wait: usize,
    /// Connection timeout in seconds
    pub connect_timeout: Duration,
    /// Query timeout (0 = no timeout)
    pub query_timeout: Duration,
    /// Idle timeout for server connections
    pub server_idle_timeout: Duration,
    /// How long to wait for a connection from pool
    pub pool_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            dbname: None,
            host: "localhost".to_string(),
            port: 5432,
            user: None,
            password: None,
            auth_type: AuthType::default(),
            max_db_connections: 100,
            min_pool_size: 5,
            reserve_pool_size: 5,
            pool_mode: PoolMode::default(),
            max_client_wait: 100,
            connect_timeout: Duration::from_secs(10),
            query_timeout: Duration::ZERO,
            server_idle_timeout: Duration::from_secs(600),
            pool_timeout: Duration::from_secs(30),
        }
    }
}

/// Global pooler configuration
#[derive(Debug, Clone)]
pub struct PoolerConfig {
    /// Listen address
    pub listen_addr: String,
    /// Listen port
    pub listen_port: u16,
    /// Maximum total client connections
    pub max_client_conn: usize,
    /// Default pool mode
    pub default_pool_mode: PoolMode,
    /// Default pool size per database/user pair
    pub default_pool_size: usize,
    /// Enable admin console
    pub admin_users: Vec<String>,
    /// Stats period (how often to log stats)
    pub stats_period: Duration,
    /// Log connections
    pub log_connections: bool,
    /// Log disconnections
    pub log_disconnections: bool,
    /// Log pooler errors
    pub log_pooler_errors: bool,
    /// Server reset query (executed when connection returned to pool)
    pub server_reset_query: String,
    /// Server check query (health check)
    pub server_check_query: String,
    /// Server check delay
    pub server_check_delay: Duration,
    /// TCP keepalive
    pub tcp_keepalive: Duration,
    /// TCP keepalive count
    pub tcp_keepcnt: u32,
    /// TCP keepalive interval
    pub tcp_keepintvl: Duration,
    /// Application name prefix
    pub application_name_add_host: bool,
    /// Disable pipelining
    pub disable_pipelining: bool,
}

impl Default for PoolerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".to_string(),
            listen_port: 6432,
            max_client_conn: 1000,
            default_pool_mode: PoolMode::Transaction,
            default_pool_size: 20,
            admin_users: vec!["pgbouncer".to_string()],
            stats_period: Duration::from_secs(60),
            log_connections: true,
            log_disconnections: true,
            log_pooler_errors: true,
            server_reset_query: "DISCARD ALL".to_string(),
            server_check_query: "SELECT 1".to_string(),
            server_check_delay: Duration::from_secs(30),
            tcp_keepalive: Duration::from_secs(30),
            tcp_keepcnt: 3,
            tcp_keepintvl: Duration::from_secs(10),
            application_name_add_host: true,
            disable_pipelining: false,
        }
    }
}

/// State of a server connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    /// Connection is being established
    Connecting,
    /// Logged in and available
    Idle,
    /// Currently executing a query
    Active,
    /// In a transaction
    InTransaction,
    /// Connection is being tested
    Testing,
    /// Connection is marked for removal
    Closing,
}

/// Represents a backend server connection
pub struct ServerConnection {
    /// Unique connection ID
    pub id: u64,
    /// Database this connection is for
    pub database: String,
    /// User this connection is authenticated as
    pub user: String,
    /// Backend host
    pub host: String,
    /// Backend port
    pub port: u16,
    /// Current state
    pub state: ServerState,
    /// When connection was created
    pub connect_time: Instant,
    /// When last request started
    pub request_time: Option<Instant>,
    /// Last query executed
    pub last_query: Option<String>,
    /// Total queries executed on this connection
    pub query_count: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Client currently using this connection (if any)
    pub client_id: Option<u64>,
    /// Last activity time
    pub last_activity: Instant,
    /// TCP stream to backend (None in simulation mode)
    stream: Option<TcpStream>,
    /// Backend process ID
    pub backend_pid: Option<u32>,
    /// Backend secret key (for cancel requests)
    pub backend_key: Option<u32>,
}

impl std::fmt::Debug for ServerConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConnection")
            .field("id", &self.id)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("state", &self.state)
            .field("client_id", &self.client_id)
            .field("backend_pid", &self.backend_pid)
            .finish()
    }
}

impl ServerConnection {
    pub fn new(id: u64, database: String, user: String, host: String, port: u16) -> Self {
        let now = Instant::now();
        Self {
            id,
            database,
            user,
            host,
            port,
            state: ServerState::Connecting,
            connect_time: now,
            request_time: None,
            last_query: None,
            query_count: 0,
            bytes_sent: 0,
            bytes_received: 0,
            client_id: None,
            last_activity: now,
            stream: None,
            backend_pid: None,
            backend_key: None,
        }
    }

    /// Create a connected server connection with TCP stream
    pub fn with_stream(mut self, stream: TcpStream) -> Self {
        self.stream = Some(stream);
        self.state = ServerState::Idle;
        self
    }

    /// Set backend process info
    pub fn set_backend_info(&mut self, pid: u32, key: u32) {
        self.backend_pid = Some(pid);
        self.backend_key = Some(key);
    }

    pub fn is_idle(&self) -> bool {
        self.state == ServerState::Idle && self.client_id.is_none()
    }

    pub fn idle_duration(&self) -> Duration {
        self.last_activity.elapsed()
    }

    pub fn age(&self) -> Duration {
        self.connect_time.elapsed()
    }

    /// Check if connection is healthy by sending a simple query
    pub fn health_check(&mut self, query: &str) -> Result<(), PoolerError> {
        if let Some(ref mut stream) = self.stream {
            // Send simple query
            let query_msg = Self::build_simple_query(query);
            stream.write_all(&query_msg).map_err(|e| {
                PoolerError::ServerConnectionFailed(format!("health check write failed: {}", e))
            })?;

            // Read response (simplified - just check we get something)
            let mut buf = [0u8; 1024];
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            match stream.read(&mut buf) {
                Ok(0) => Err(PoolerError::ServerConnectionFailed(
                    "connection closed".to_string(),
                )),
                Ok(_) => {
                    self.last_activity = Instant::now();
                    Ok(())
                }
                Err(e) => Err(PoolerError::ServerConnectionFailed(format!(
                    "health check read failed: {}",
                    e
                ))),
            }
        } else {
            // Simulation mode - always healthy
            self.last_activity = Instant::now();
            Ok(())
        }
    }

    /// Build a PostgreSQL simple query message
    fn build_simple_query(query: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'Q'); // Query message type
        let len = (query.len() + 5) as u32; // 4 bytes length + query + null terminator
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(query.as_bytes());
        msg.push(0); // Null terminator
        msg
    }

    /// Execute the server reset query
    pub fn reset(&mut self, reset_query: &str) -> Result<(), PoolerError> {
        if !reset_query.is_empty() {
            self.health_check(reset_query)?;
        }
        self.state = ServerState::Idle;
        self.client_id = None;
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Check if this connection has a live TCP stream
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }
}

/// State of a client connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Waiting for authentication
    Authenticating,
    /// Waiting for server connection
    WaitingForServer,
    /// Connected and idle
    Idle,
    /// Executing a query
    Active,
    /// In a transaction
    InTransaction,
    /// Disconnecting
    Disconnecting,
}

/// Represents a client connection
#[derive(Debug)]
pub struct ClientConnection {
    /// Unique client ID
    pub id: u64,
    /// Target database
    pub database: String,
    /// Authenticated user
    pub user: String,
    /// Client address
    pub addr: String,
    /// Current state
    pub state: ClientState,
    /// When connected
    pub connect_time: Instant,
    /// When current request started
    pub request_time: Option<Instant>,
    /// Server connection assigned (if any)
    pub server_id: Option<u64>,
    /// Total queries executed
    pub query_count: u64,
    /// Total bytes sent to client
    pub bytes_sent: u64,
    /// Total bytes received from client
    pub bytes_received: u64,
    /// Last activity time
    pub last_activity: Instant,
    /// Application name
    pub application_name: Option<String>,
}

impl ClientConnection {
    pub fn new(id: u64, database: String, user: String, addr: String) -> Self {
        let now = Instant::now();
        Self {
            id,
            database,
            user,
            addr,
            state: ClientState::Authenticating,
            connect_time: now,
            request_time: None,
            server_id: None,
            query_count: 0,
            bytes_sent: 0,
            bytes_received: 0,
            last_activity: now,
            application_name: None,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, ClientState::Active | ClientState::InTransaction)
    }

    pub fn wait_duration(&self) -> Duration {
        self.request_time.map_or(Duration::ZERO, |t| t.elapsed())
    }
}

/// Pool for a specific database/user pair
#[derive(Debug)]
pub struct ConnectionPool {
    /// Database name
    pub database: String,
    /// User name
    pub user: String,
    /// Pool configuration
    pub config: DatabaseConfig,
    /// Idle server connections
    idle_servers: VecDeque<u64>,
    /// Active server connections
    active_servers: Vec<u64>,
    /// Clients waiting for a connection
    waiting_clients: VecDeque<u64>,
    /// Pool statistics
    pub stats: PoolStats,
}

impl ConnectionPool {
    pub fn new(database: String, user: String, config: DatabaseConfig) -> Self {
        Self {
            database,
            user,
            config,
            idle_servers: VecDeque::new(),
            active_servers: Vec::new(),
            waiting_clients: VecDeque::new(),
            stats: PoolStats::default(),
        }
    }

    /// Get an idle connection from the pool
    pub fn get_connection(&mut self) -> Option<u64> {
        if let Some(server_id) = self.idle_servers.pop_front() {
            self.active_servers.push(server_id);
            self.stats.pool_hits += 1;
            Some(server_id)
        } else {
            None
        }
    }

    /// Return a connection to the pool
    pub fn return_connection(&mut self, server_id: u64) {
        if let Some(pos) = self.active_servers.iter().position(|&id| id == server_id) {
            self.active_servers.swap_remove(pos);
            self.idle_servers.push_back(server_id);
        }
    }

    /// Add a waiting client
    pub fn add_waiting_client(&mut self, client_id: u64) -> Result<(), PoolerError> {
        if self.waiting_clients.len() >= self.config.max_client_wait {
            return Err(PoolerError::TooManyWaitingClients);
        }
        self.waiting_clients.push_back(client_id);
        Ok(())
    }

    /// Get next waiting client
    pub fn get_waiting_client(&mut self) -> Option<u64> {
        self.waiting_clients.pop_front()
    }

    /// Check if we need more connections
    pub fn needs_more_connections(&self) -> bool {
        let total = self.idle_servers.len() + self.active_servers.len();
        total < self.config.max_db_connections
            && (self.idle_servers.is_empty() || !self.waiting_clients.is_empty())
    }

    /// Current idle count
    pub fn idle_count(&self) -> usize {
        self.idle_servers.len()
    }

    /// Current active count
    pub fn active_count(&self) -> usize {
        self.active_servers.len()
    }

    /// Waiting clients count
    pub fn waiting_count(&self) -> usize {
        self.waiting_clients.len()
    }
}

/// Statistics for a pool
#[derive(Debug, Default, Clone)]
pub struct PoolStats {
    /// Total queries executed
    pub total_queries: u64,
    /// Total bytes sent
    pub total_bytes_sent: u64,
    /// Total bytes received
    pub total_bytes_received: u64,
    /// Total time spent in transactions (ms)
    pub total_xact_time: u64,
    /// Total query time (ms)
    pub total_query_time: u64,
    /// Total wait time for clients (ms)
    pub total_wait_time: u64,
    /// Pool hits (got connection from pool)
    pub pool_hits: u64,
    /// Pool misses (had to create new connection)
    pub pool_misses: u64,
    /// Max wait time observed (ms)
    pub max_wait: u64,
}

/// Global pooler statistics
#[derive(Debug, Default)]
pub struct PoolerStats {
    /// Total client connections ever
    pub total_clients: AtomicU64,
    /// Current client count
    pub current_clients: AtomicUsize,
    /// Total server connections ever
    pub total_servers: AtomicU64,
    /// Current server count
    pub current_servers: AtomicUsize,
    /// Total queries
    pub total_queries: AtomicU64,
    /// Total bytes sent
    pub total_bytes_sent: AtomicU64,
    /// Total bytes received
    pub total_bytes_received: AtomicU64,
    /// Login failures
    pub login_failures: AtomicU64,
}

impl PoolerStats {
    pub fn snapshot(&self) -> PoolerStatsSnapshot {
        PoolerStatsSnapshot {
            total_clients: self.total_clients.load(Ordering::Relaxed),
            current_clients: self.current_clients.load(Ordering::Relaxed),
            total_servers: self.total_servers.load(Ordering::Relaxed),
            current_servers: self.current_servers.load(Ordering::Relaxed),
            total_queries: self.total_queries.load(Ordering::Relaxed),
            total_bytes_sent: self.total_bytes_sent.load(Ordering::Relaxed),
            total_bytes_received: self.total_bytes_received.load(Ordering::Relaxed),
            login_failures: self.login_failures.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of pooler stats
#[derive(Debug, Clone)]
pub struct PoolerStatsSnapshot {
    pub total_clients: u64,
    pub current_clients: usize,
    pub total_servers: u64,
    pub current_servers: usize,
    pub total_queries: u64,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
    pub login_failures: u64,
}

/// Pooler errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolerError {
    /// Too many clients connected
    TooManyClients,
    /// Database not configured
    UnknownDatabase(String),
    /// User not authorized
    AuthenticationFailed(String),
    /// Connection timeout
    ConnectionTimeout,
    /// Pool timeout (no available connections)
    PoolTimeout,
    /// Too many clients waiting
    TooManyWaitingClients,
    /// Server connection failed
    ServerConnectionFailed(String),
    /// Query execution failed
    QueryFailed(String),
    /// Configuration error
    ConfigError(String),
    /// Pool is shutting down
    ShuttingDown,
    /// Database is paused
    DatabasePaused(String),
    /// Reload failed
    ReloadFailed(String),
}

impl std::fmt::Display for PoolerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyClients => write!(f, "too many clients"),
            Self::UnknownDatabase(db) => write!(f, "unknown database: {}", db),
            Self::AuthenticationFailed(msg) => write!(f, "authentication failed: {}", msg),
            Self::ConnectionTimeout => write!(f, "connection timeout"),
            Self::PoolTimeout => write!(f, "pool timeout"),
            Self::TooManyWaitingClients => write!(f, "too many waiting clients"),
            Self::ServerConnectionFailed(msg) => write!(f, "server connection failed: {}", msg),
            Self::QueryFailed(msg) => write!(f, "query failed: {}", msg),
            Self::ConfigError(msg) => write!(f, "configuration error: {}", msg),
            Self::ShuttingDown => write!(f, "pooler is shutting down"),
            Self::DatabasePaused(db) => write!(f, "database is paused: {}", db),
            Self::ReloadFailed(msg) => write!(f, "reload failed: {}", msg),
        }
    }
}

impl std::error::Error for PoolerError {}

/// Database pause state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabasePauseState {
    /// Database is active and accepting connections
    Active,
    /// Database is paused - no new connections, existing continue
    Paused,
    /// Database is being killed - all connections closed
    Killing,
}

/// Connection pooler manager
pub struct ConnectionPooler {
    /// Global configuration
    config: RwLock<PoolerConfig>,
    /// Database configurations
    databases: RwLock<HashMap<String, DatabaseConfig>>,
    /// User credentials (key: "database:user")
    user_credentials: RwLock<HashMap<String, UserCredentials>>,
    /// Connection pools (key: database:user)
    pools: RwLock<HashMap<String, Mutex<ConnectionPool>>>,
    /// Server connections
    servers: RwLock<HashMap<u64, Mutex<ServerConnection>>>,
    /// Client connections
    clients: RwLock<HashMap<u64, Mutex<ClientConnection>>>,
    /// Authentication salt per client (for MD5 auth)
    auth_salts: RwLock<HashMap<u64, [u8; 4]>>,
    /// Paused databases
    paused_databases: RwLock<HashMap<String, DatabasePauseState>>,
    /// Next server ID
    next_server_id: AtomicU64,
    /// Next client ID
    next_client_id: AtomicU64,
    /// Global stats
    stats: PoolerStats,
    /// Shutdown flag
    shutdown: AtomicUsize,
    /// Enable simulation mode (no actual TCP connections)
    simulation_mode: bool,
    /// Config file path for RELOAD
    config_file_path: RwLock<Option<String>>,
}

impl ConnectionPooler {
    pub fn new(config: PoolerConfig) -> Self {
        Self {
            config: RwLock::new(config),
            databases: RwLock::new(HashMap::new()),
            user_credentials: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            auth_salts: RwLock::new(HashMap::new()),
            paused_databases: RwLock::new(HashMap::new()),
            next_server_id: AtomicU64::new(1),
            next_client_id: AtomicU64::new(1),
            stats: PoolerStats::default(),
            shutdown: AtomicUsize::new(0),
            simulation_mode: true, // Default to simulation for testing
            config_file_path: RwLock::new(None),
        }
    }

    /// Create pooler with real TCP connections enabled
    pub fn new_with_connections(config: PoolerConfig) -> Self {
        let mut pooler = Self::new(config);
        pooler.simulation_mode = false;
        pooler
    }

    /// Set config file path for RELOAD command
    pub fn set_config_file(&self, path: &str) {
        let mut config_path = self.config_file_path.write();
        *config_path = Some(path.to_string());
    }

    /// Get the current configuration (clone)
    pub fn get_config(&self) -> PoolerConfig {
        self.config.read().clone()
    }

    /// Check if a database is paused
    pub fn is_database_paused(&self, database: &str) -> bool {
        let paused = self.paused_databases.read();
        matches!(
            paused.get(database),
            Some(DatabasePauseState::Paused) | Some(DatabasePauseState::Killing)
        )
    }

    /// Get database pause state
    pub fn get_database_state(&self, database: &str) -> DatabasePauseState {
        let paused = self.paused_databases.read();
        paused
            .get(database)
            .copied()
            .unwrap_or(DatabasePauseState::Active)
    }

    /// Add user credentials
    pub fn add_user(&self, database: &str, credentials: UserCredentials) {
        let key = format!("{}:{}", database, credentials.username);
        let mut creds = self.user_credentials.write();
        creds.insert(key, credentials);
    }

    /// Generate a random salt for MD5 authentication
    fn generate_salt(&self) -> [u8; 4] {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let seed = now.as_nanos() as u32;
        // Simple PRNG for salt generation
        let a = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        a.to_le_bytes()
    }

    /// Get authentication salt for a client (generates if not exists)
    pub fn get_auth_salt(&self, client_id: u64) -> [u8; 4] {
        {
            let salts = self.auth_salts.read();
            if let Some(salt) = salts.get(&client_id) {
                return *salt;
            }
        }

        let salt = self.generate_salt();
        let mut salts = self.auth_salts.write();
        salts.insert(client_id, salt);
        salt
    }

    /// Add or update database configuration
    pub fn add_database(&self, config: DatabaseConfig) -> Result<(), PoolerError> {
        if config.name.is_empty() {
            return Err(PoolerError::ConfigError("database name required".into()));
        }

        let mut databases = self.databases.write();
        databases.insert(config.name.clone(), config);
        Ok(())
    }

    /// Remove a database configuration
    pub fn remove_database(&self, name: &str) -> Result<(), PoolerError> {
        let mut databases = self.databases.write();
        databases
            .remove(name)
            .ok_or_else(|| PoolerError::UnknownDatabase(name.to_string()))?;
        Ok(())
    }

    /// Get database configuration
    pub fn get_database(&self, name: &str) -> Option<DatabaseConfig> {
        let databases = self.databases.read();
        databases.get(name).cloned()
    }

    /// Accept a new client connection
    pub fn accept_client(
        &self,
        database: &str,
        user: &str,
        addr: &str,
    ) -> Result<u64, PoolerError> {
        if self.shutdown.load(Ordering::Relaxed) > 0 {
            return Err(PoolerError::ShuttingDown);
        }

        // Check if database is paused
        if self.is_database_paused(database) {
            return Err(PoolerError::DatabasePaused(database.to_string()));
        }

        // Atomically increment and check client count to prevent race condition
        let max_client_conn = self.config.read().max_client_conn;
        let prev_count = self.stats.current_clients.fetch_add(1, Ordering::SeqCst);
        if prev_count >= max_client_conn {
            // Exceeded limit, rollback the increment
            self.stats.current_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(PoolerError::TooManyClients);
        }

        // Verify database exists
        {
            let databases = self.databases.read();
            if !databases.contains_key(database) {
                // Rollback client count on error
                self.stats.current_clients.fetch_sub(1, Ordering::SeqCst);
                return Err(PoolerError::UnknownDatabase(database.to_string()));
            }
        }

        let client_id = self.next_client_id.fetch_add(1, Ordering::SeqCst);
        let client = ClientConnection::new(
            client_id,
            database.to_string(),
            user.to_string(),
            addr.to_string(),
        );

        {
            let mut clients = self.clients.write();
            clients.insert(client_id, Mutex::new(client));
        }

        self.stats.total_clients.fetch_add(1, Ordering::Relaxed);

        Ok(client_id)
    }

    /// Complete client authentication with plain text password
    pub fn authenticate_client(
        &self,
        client_id: u64,
        password: Option<&str>,
    ) -> Result<(), PoolerError> {
        let (database, user) = {
            let clients = self.clients.read();
            let client_lock = clients
                .get(&client_id)
                .ok_or(PoolerError::AuthenticationFailed(
                    "client not found".to_string(),
                ))?;
            let client = client_lock.lock();
            (client.database.clone(), client.user.clone())
        };

        // Look up credentials
        let key = format!("{}:{}", database, user);
        let creds = self.user_credentials.read();

        // Check if credentials are configured
        if let Some(user_creds) = creds.get(&key) {
            match user_creds.auth_type {
                AuthType::Trust => {
                    // No password verification needed
                }
                AuthType::Plain => {
                    // Verify plain text password
                    let password = password.ok_or_else(|| {
                        self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
                        PoolerError::AuthenticationFailed("password required".to_string())
                    })?;

                    if !user_creds.verify_plain(password) {
                        self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
                        return Err(PoolerError::AuthenticationFailed(
                            "invalid password".to_string(),
                        ));
                    }
                }
                AuthType::Md5 | AuthType::ScramSha256 => {
                    // For MD5/SCRAM, use authenticate_client_md5 instead
                    let password = password.ok_or_else(|| {
                        self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
                        PoolerError::AuthenticationFailed("password required".to_string())
                    })?;

                    // Fall back to plain verification if not using challenge-response
                    if !user_creds.verify_plain(password) {
                        self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
                        return Err(PoolerError::AuthenticationFailed(
                            "invalid password".to_string(),
                        ));
                    }
                }
            }
        }
        // If no credentials configured, allow (for backwards compatibility in tests)

        drop(creds);

        // Mark client as authenticated
        let clients = self.clients.read();
        if let Some(client_lock) = clients.get(&client_id) {
            let mut client = client_lock.lock();
            client.state = ClientState::Idle;
        }

        // Clean up auth salt
        let mut salts = self.auth_salts.write();
        salts.remove(&client_id);

        Ok(())
    }

    /// Complete client authentication with MD5 response
    pub fn authenticate_client_md5(
        &self,
        client_id: u64,
        md5_response: &str,
    ) -> Result<(), PoolerError> {
        let (database, user) = {
            let clients = self.clients.read();
            let client_lock = clients
                .get(&client_id)
                .ok_or(PoolerError::AuthenticationFailed(
                    "client not found".to_string(),
                ))?;
            let client = client_lock.lock();
            (client.database.clone(), client.user.clone())
        };

        // Get the salt used for this client
        let salt = {
            let salts = self.auth_salts.read();
            *salts.get(&client_id).ok_or_else(|| {
                PoolerError::AuthenticationFailed("no auth salt found".to_string())
            })?
        };

        // Look up credentials
        let key = format!("{}:{}", database, user);
        let creds = self.user_credentials.read();

        if let Some(user_creds) = creds.get(&key) {
            if !user_creds.verify_md5(md5_response, &salt) {
                self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
                return Err(PoolerError::AuthenticationFailed(
                    "MD5 authentication failed".to_string(),
                ));
            }
        } else {
            self.stats.login_failures.fetch_add(1, Ordering::Relaxed);
            return Err(PoolerError::AuthenticationFailed(
                "user not found".to_string(),
            ));
        }

        drop(creds);

        // Mark client as authenticated
        let clients = self.clients.read();
        if let Some(client_lock) = clients.get(&client_id) {
            let mut client = client_lock.lock();
            client.state = ClientState::Idle;
        }

        // Clean up auth salt
        let mut salts = self.auth_salts.write();
        salts.remove(&client_id);

        Ok(())
    }

    /// Request a server connection for a client
    pub fn get_server_connection(&self, client_id: u64) -> Result<u64, PoolerError> {
        let (database, user) = {
            let clients = self.clients.read();
            let client_lock = clients
                .get(&client_id)
                .ok_or(PoolerError::ConnectionTimeout)?;
            let client = client_lock.lock();
            (client.database.clone(), client.user.clone())
        };

        let pool_key = format!("{}:{}", database, user);

        // Ensure pool exists
        self.ensure_pool(&database, &user)?;

        // Try to get from pool
        {
            let pools = self.pools.read();
            if let Some(pool_lock) = pools.get(&pool_key) {
                let mut pool = pool_lock.lock();
                if let Some(server_id) = pool.get_connection() {
                    // Assign server to client
                    self.assign_server_to_client(server_id, client_id)?;
                    return Ok(server_id);
                }
            }
        }

        // No idle connection, create new one if possible
        let config = self
            .get_database(&database)
            .ok_or(PoolerError::UnknownDatabase(database.clone()))?;

        // Check if we can create more connections
        {
            let pools = self.pools.read();
            if let Some(pool_lock) = pools.get(&pool_key) {
                let pool = pool_lock.lock();
                let total = pool.idle_count() + pool.active_count();
                if total >= config.max_db_connections {
                    // Need to wait
                    return Err(PoolerError::PoolTimeout);
                }
            }
        }

        // Create new server connection
        let server_id = self.create_server_connection(&config, &user)?;

        // Add to pool's active list
        {
            let pools = self.pools.read();
            if let Some(pool_lock) = pools.get(&pool_key) {
                let mut pool = pool_lock.lock();
                pool.active_servers.push(server_id);
                pool.stats.pool_misses += 1;
            }
        }

        // Assign to client
        self.assign_server_to_client(server_id, client_id)?;

        Ok(server_id)
    }

    /// Ensure a pool exists for database/user pair
    fn ensure_pool(&self, database: &str, user: &str) -> Result<(), PoolerError> {
        let pool_key = format!("{}:{}", database, user);

        {
            let pools = self.pools.read();
            if pools.contains_key(&pool_key) {
                return Ok(());
            }
        }

        let config = self
            .get_database(database)
            .ok_or_else(|| PoolerError::UnknownDatabase(database.to_string()))?;

        let pool = ConnectionPool::new(database.to_string(), user.to_string(), config);

        let mut pools = self.pools.write();
        pools.insert(pool_key, Mutex::new(pool));

        Ok(())
    }

    /// Create a new server connection
    fn create_server_connection(
        &self,
        config: &DatabaseConfig,
        user: &str,
    ) -> Result<u64, PoolerError> {
        let server_id = self.next_server_id.fetch_add(1, Ordering::SeqCst);

        let user_str = config.user.as_deref().unwrap_or(user);
        let mut server = ServerConnection::new(
            server_id,
            config.name.clone(),
            user_str.to_string(),
            config.host.clone(),
            config.port,
        );

        if self.simulation_mode {
            // Simulation mode - no actual TCP connection
            server.state = ServerState::Idle;
        } else {
            // Establish actual TCP connection to backend
            let addr = format!("{}:{}", config.host, config.port);
            let stream = TcpStream::connect_timeout(
                &addr.parse().map_err(|e| {
                    PoolerError::ServerConnectionFailed(format!("invalid address: {}", e))
                })?,
                config.connect_timeout,
            )
            .map_err(|e| {
                PoolerError::ServerConnectionFailed(format!("connection failed: {}", e))
            })?;

            // Set TCP options
            stream.set_nodelay(true).ok();
            stream.set_read_timeout(Some(config.query_timeout)).ok();

            // Perform PostgreSQL startup handshake
            let (pid, key) = self.pg_startup_handshake(
                &stream,
                &config.name,
                user_str,
                config.password.as_deref(),
            )?;

            server.set_backend_info(pid, key);
            server = server.with_stream(stream);
        }

        {
            let mut servers = self.servers.write();
            servers.insert(server_id, Mutex::new(server));
        }

        self.stats.total_servers.fetch_add(1, Ordering::Relaxed);
        self.stats.current_servers.fetch_add(1, Ordering::Relaxed);

        Ok(server_id)
    }

    /// Perform PostgreSQL startup message handshake
    fn pg_startup_handshake(
        &self,
        stream: &TcpStream,
        database: &str,
        user: &str,
        password: Option<&str>,
    ) -> Result<(u32, u32), PoolerError> {
        let mut stream = stream;

        // Build startup message
        let mut startup = Vec::new();
        // Protocol version 3.0
        startup.extend_from_slice(&196608u32.to_be_bytes());
        // user parameter
        startup.extend_from_slice(b"user\0");
        startup.extend_from_slice(user.as_bytes());
        startup.push(0);
        // database parameter
        startup.extend_from_slice(b"database\0");
        startup.extend_from_slice(database.as_bytes());
        startup.push(0);
        // application_name
        startup.extend_from_slice(b"application_name\0");
        startup.extend_from_slice(b"boyodb-pooler\0");
        // terminator
        startup.push(0);

        // Length prefix (includes length field itself)
        let len = (startup.len() + 4) as u32;
        let mut msg = len.to_be_bytes().to_vec();
        msg.extend(startup);

        stream.write_all(&msg).map_err(|e| {
            PoolerError::ServerConnectionFailed(format!("startup write failed: {}", e))
        })?;

        // Read response
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).map_err(|e| {
            PoolerError::ServerConnectionFailed(format!("startup read failed: {}", e))
        })?;

        let msg_type = header[0] as char;
        let msg_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

        match msg_type {
            'R' => {
                // Authentication request
                let mut body = vec![0u8; msg_len - 4];
                stream.read_exact(&mut body).map_err(|e| {
                    PoolerError::ServerConnectionFailed(format!("auth read failed: {}", e))
                })?;

                let auth_type = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);

                match auth_type {
                    0 => {
                        // AuthenticationOk - no password needed
                    }
                    3 => {
                        // CleartextPassword
                        let password = password.ok_or_else(|| {
                            PoolerError::AuthenticationFailed("password required".to_string())
                        })?;
                        self.send_password(stream, password)?;
                    }
                    5 => {
                        // MD5Password
                        let password = password.ok_or_else(|| {
                            PoolerError::AuthenticationFailed("password required".to_string())
                        })?;
                        let salt: [u8; 4] = [body[4], body[5], body[6], body[7]];
                        let md5_pass = compute_md5_password(password, user, &salt);
                        self.send_password(stream, &md5_pass)?;
                    }
                    _ => {
                        return Err(PoolerError::AuthenticationFailed(format!(
                            "unsupported auth type: {}",
                            auth_type
                        )));
                    }
                }
            }
            'E' => {
                // Error response
                return Err(PoolerError::ServerConnectionFailed(
                    "server error during startup".to_string(),
                ));
            }
            _ => {
                return Err(PoolerError::ServerConnectionFailed(format!(
                    "unexpected message type: {}",
                    msg_type
                )));
            }
        }

        // Read messages until ReadyForQuery
        let (mut backend_pid, mut backend_key) = (0u32, 0u32);
        loop {
            let mut header = [0u8; 5];
            stream
                .read_exact(&mut header)
                .map_err(|e| PoolerError::ServerConnectionFailed(format!("read failed: {}", e)))?;

            let msg_type = header[0] as char;
            let msg_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

            let mut body = vec![0u8; msg_len - 4];
            if !body.is_empty() {
                stream.read_exact(&mut body).map_err(|e| {
                    PoolerError::ServerConnectionFailed(format!("body read failed: {}", e))
                })?;
            }

            match msg_type {
                'R' => {
                    // Another auth message (should be AuthenticationOk)
                    let auth_type = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                    if auth_type != 0 {
                        return Err(PoolerError::AuthenticationFailed(
                            "authentication failed".to_string(),
                        ));
                    }
                }
                'K' => {
                    // BackendKeyData
                    backend_pid = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                    backend_key = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                }
                'Z' => {
                    // ReadyForQuery - connection is ready
                    return Ok((backend_pid, backend_key));
                }
                'S' | 'N' => {
                    // ParameterStatus or NoticeResponse - ignore
                }
                'E' => {
                    // Error
                    return Err(PoolerError::ServerConnectionFailed(
                        "server error".to_string(),
                    ));
                }
                _ => {
                    // Ignore other messages during startup
                }
            }
        }
    }

    /// Send password message to server
    fn send_password(&self, stream: &TcpStream, password: &str) -> Result<(), PoolerError> {
        let mut stream = stream;
        let mut msg = Vec::new();
        msg.push(b'p'); // PasswordMessage
        let len = (password.len() + 5) as u32; // 4 + password + null
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(password.as_bytes());
        msg.push(0);

        stream.write_all(&msg).map_err(|e| {
            PoolerError::AuthenticationFailed(format!("password send failed: {}", e))
        })?;

        Ok(())
    }

    /// Assign a server connection to a client
    fn assign_server_to_client(&self, server_id: u64, client_id: u64) -> Result<(), PoolerError> {
        // Update server
        {
            let servers = self.servers.read();
            if let Some(server_lock) = servers.get(&server_id) {
                let mut server = server_lock.lock();
                server.client_id = Some(client_id);
                server.state = ServerState::Active;
                server.last_activity = Instant::now();
            }
        }

        // Update client
        {
            let clients = self.clients.read();
            if let Some(client_lock) = clients.get(&client_id) {
                let mut client = client_lock.lock();
                client.server_id = Some(server_id);
                client.state = ClientState::Active;
                client.last_activity = Instant::now();
            }
        }

        Ok(())
    }

    /// Release a server connection back to the pool
    pub fn release_server_connection(
        &self,
        client_id: u64,
        transaction_complete: bool,
    ) -> Result<(), PoolerError> {
        let (database, user, server_id) = {
            let clients = self.clients.read();
            let client_lock = clients
                .get(&client_id)
                .ok_or(PoolerError::ConnectionTimeout)?;
            let mut client = client_lock.lock();

            let server_id = client.server_id.take();
            client.state = ClientState::Idle;

            (client.database.clone(), client.user.clone(), server_id)
        };

        let Some(server_id) = server_id else {
            return Ok(()); // No server to release
        };

        // Check pool mode to decide if we should return connection
        let config = self.get_database(&database);
        let should_return = match config.map(|c| c.pool_mode) {
            Some(PoolMode::Transaction) => transaction_complete,
            Some(PoolMode::Statement) => true,
            Some(PoolMode::Session) => false,
            None => transaction_complete,
        };

        if should_return {
            // Update server state
            {
                let servers = self.servers.read();
                if let Some(server_lock) = servers.get(&server_id) {
                    let mut server = server_lock.lock();
                    server.client_id = None;
                    server.state = ServerState::Idle;
                    server.last_activity = Instant::now();
                }
            }

            // Return to pool
            let pool_key = format!("{}:{}", database, user);
            let pools = self.pools.read();
            if let Some(pool_lock) = pools.get(&pool_key) {
                let mut pool = pool_lock.lock();
                pool.return_connection(server_id);
            }
        }

        Ok(())
    }

    /// Record query execution
    pub fn record_query(&self, client_id: u64, query: &str, bytes_sent: u64, bytes_received: u64) {
        // Update client stats
        {
            let clients = self.clients.read();
            if let Some(client_lock) = clients.get(&client_id) {
                let mut client = client_lock.lock();
                client.query_count += 1;
                client.bytes_sent += bytes_sent;
                client.bytes_received += bytes_received;
                client.last_activity = Instant::now();

                // Update server if assigned
                if let Some(server_id) = client.server_id {
                    let servers = self.servers.read();
                    if let Some(server_lock) = servers.get(&server_id) {
                        let mut server = server_lock.lock();
                        server.query_count += 1;
                        server.bytes_sent += bytes_sent;
                        server.bytes_received += bytes_received;
                        server.last_query = Some(query.to_string());
                        server.last_activity = Instant::now();
                    }
                }
            }
        }

        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_bytes_sent
            .fetch_add(bytes_sent, Ordering::Relaxed);
        self.stats
            .total_bytes_received
            .fetch_add(bytes_received, Ordering::Relaxed);
    }

    /// Disconnect a client
    pub fn disconnect_client(&self, client_id: u64) -> Result<(), PoolerError> {
        // First release any server connection
        let _ = self.release_server_connection(client_id, true);

        // Remove client
        {
            let mut clients = self.clients.write();
            clients.remove(&client_id);
        }

        // Clean up auth salt to prevent memory leak
        {
            let mut salts = self.auth_salts.write();
            salts.remove(&client_id);
        }

        self.stats.current_clients.fetch_sub(1, Ordering::Relaxed);

        Ok(())
    }

    /// Close a server connection
    pub fn close_server_connection(&self, server_id: u64) -> Result<(), PoolerError> {
        // Get server info
        let (database, user) = {
            let servers = self.servers.read();
            let server_lock = servers
                .get(&server_id)
                .ok_or(PoolerError::ConnectionTimeout)?;
            let server = server_lock.lock();
            (server.database.clone(), server.user.clone())
        };

        // Remove from pool
        let pool_key = format!("{}:{}", database, user);
        {
            let pools = self.pools.read();
            if let Some(pool_lock) = pools.get(&pool_key) {
                let mut pool = pool_lock.lock();
                // Remove from idle or active
                pool.idle_servers.retain(|&id| id != server_id);
                pool.active_servers.retain(|&id| id != server_id);
            }
        }

        // Remove server
        {
            let mut servers = self.servers.write();
            servers.remove(&server_id);
        }

        self.stats.current_servers.fetch_sub(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get global statistics
    pub fn get_stats(&self) -> PoolerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get pool statistics for a database/user pair
    pub fn get_pool_stats(&self, database: &str, user: &str) -> Option<PoolStats> {
        let pool_key = format!("{}:{}", database, user);
        let pools = self.pools.read();
        pools.get(&pool_key).map(|p| {
            let pool = p.lock();
            pool.stats.clone()
        })
    }

    /// List all pools
    pub fn list_pools(&self) -> Vec<PoolSummary> {
        let pools = self.pools.read();
        pools
            .iter()
            .map(|(key, pool_lock)| {
                let pool = pool_lock.lock();
                PoolSummary {
                    database: pool.database.clone(),
                    user: pool.user.clone(),
                    idle_count: pool.idle_count(),
                    active_count: pool.active_count(),
                    waiting_count: pool.waiting_count(),
                    pool_mode: pool.config.pool_mode,
                }
            })
            .collect()
    }

    /// List all clients
    pub fn list_clients(&self) -> Vec<ClientSummary> {
        let clients = self.clients.read();
        clients
            .values()
            .map(|lock| {
                let client = lock.lock();
                ClientSummary {
                    id: client.id,
                    database: client.database.clone(),
                    user: client.user.clone(),
                    addr: client.addr.clone(),
                    state: client.state,
                    server_id: client.server_id,
                    connect_time_secs: client.connect_time.elapsed().as_secs(),
                }
            })
            .collect()
    }

    /// List all servers
    pub fn list_servers(&self) -> Vec<ServerSummary> {
        let servers = self.servers.read();
        servers
            .values()
            .map(|lock| {
                let server = lock.lock();
                ServerSummary {
                    id: server.id,
                    database: server.database.clone(),
                    user: server.user.clone(),
                    host: server.host.clone(),
                    port: server.port,
                    state: server.state,
                    client_id: server.client_id,
                    connect_time_secs: server.connect_time.elapsed().as_secs(),
                    query_count: server.query_count,
                }
            })
            .collect()
    }

    /// Initiate shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::SeqCst);
    }

    /// Check if shutdown is in progress
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed) > 0
    }

    /// Pause a database (stop accepting new connections)
    ///
    /// When paused:
    /// - No new client connections to this database will be accepted
    /// - Existing connections continue to work
    /// - Server connections remain in the pool
    pub fn pause_database(&self, database: &str) -> Result<(), PoolerError> {
        // Verify database exists
        {
            let databases = self.databases.read();
            if !databases.contains_key(database) {
                return Err(PoolerError::UnknownDatabase(database.to_string()));
            }
        }

        // Set pause state
        let mut paused = self.paused_databases.write();
        paused.insert(database.to_string(), DatabasePauseState::Paused);

        Ok(())
    }

    /// Resume a database (start accepting new connections again)
    pub fn resume_database(&self, database: &str) -> Result<(), PoolerError> {
        // Verify database exists
        {
            let databases = self.databases.read();
            if !databases.contains_key(database) {
                return Err(PoolerError::UnknownDatabase(database.to_string()));
            }
        }

        // Remove pause state
        let mut paused = self.paused_databases.write();
        paused.remove(database);

        Ok(())
    }

    /// Wait for all current queries to finish on a paused database
    /// Returns the number of active connections still running
    pub fn wait_database(&self, database: &str) -> Result<usize, PoolerError> {
        // Verify database is paused
        {
            let paused = self.paused_databases.read();
            if !matches!(paused.get(database), Some(DatabasePauseState::Paused)) {
                return Err(PoolerError::ConfigError(
                    "database must be paused before waiting".to_string(),
                ));
            }
        }

        // Count active connections for this database
        let servers = self.servers.read();
        let active_count = servers
            .values()
            .filter_map(|lock| {
                let server = lock.lock();
                if server.database == database && server.client_id.is_some() {
                    Some(())
                } else {
                    None
                }
            })
            .count();

        Ok(active_count)
    }

    /// Enable a database (set to active state)
    pub fn enable_database(&self, database: &str) -> Result<(), PoolerError> {
        self.resume_database(database)
    }

    /// Disable a database (same as pause)
    pub fn disable_database(&self, database: &str) -> Result<(), PoolerError> {
        self.pause_database(database)
    }

    /// Kill all connections for a database
    pub fn kill_database(&self, database: &str) -> Result<usize, PoolerError> {
        let mut killed = 0;

        // Find and close all server connections for this database
        let server_ids: Vec<u64> = {
            let servers = self.servers.read();
            servers
                .iter()
                .filter_map(|(&id, lock)| {
                    let server = lock.lock();
                    if server.database == database {
                        Some(id)
                    } else {
                        None
                    }
                })
                .collect()
        };

        for server_id in server_ids {
            if self.close_server_connection(server_id).is_ok() {
                killed += 1;
            }
        }

        Ok(killed)
    }

    /// Reload configuration from file
    ///
    /// This reloads runtime-changeable parameters without restarting the pooler.
    /// The following can be changed at runtime:
    /// - default_pool_size
    /// - max_client_conn
    /// - log_connections, log_disconnections, log_pooler_errors
    /// - server_reset_query, server_check_query
    /// - stats_period
    ///
    /// Changes to listen_addr/port require a restart.
    pub fn reload_config(&self) -> Result<(), PoolerError> {
        let config_path = {
            let path = self.config_file_path.read();
            path.clone()
        };

        let Some(path) = config_path else {
            // No config file set, nothing to reload
            return Ok(());
        };

        // Read and parse config file
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| PoolerError::ReloadFailed(format!("failed to read config file: {}", e)))?;

        // Parse the INI-style config (PgBouncer-compatible format)
        let new_config = Self::parse_config(&contents)?;

        // Apply changes to runtime config
        {
            let mut config = self.config.write();
            // Only update runtime-changeable parameters
            config.default_pool_size = new_config.default_pool_size;
            config.max_client_conn = new_config.max_client_conn;
            config.log_connections = new_config.log_connections;
            config.log_disconnections = new_config.log_disconnections;
            config.log_pooler_errors = new_config.log_pooler_errors;
            config.server_reset_query = new_config.server_reset_query;
            config.server_check_query = new_config.server_check_query;
            config.stats_period = new_config.stats_period;
        }

        Ok(())
    }

    /// Parse PgBouncer-style INI configuration
    fn parse_config(contents: &str) -> Result<PoolerConfig, PoolerError> {
        let mut config = PoolerConfig::default();

        for line in contents.lines() {
            let line = line.trim();

            // Skip comments and empty lines
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            // Skip section headers
            if line.starts_with('[') {
                continue;
            }

            // Parse key = value
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0].trim().to_lowercase();
            let value = parts[1].trim();

            match key.as_str() {
                "listen_addr" => config.listen_addr = value.to_string(),
                "listen_port" => {
                    config.listen_port = value
                        .parse()
                        .map_err(|_| PoolerError::ConfigError("invalid listen_port".to_string()))?;
                }
                "max_client_conn" => {
                    config.max_client_conn = value.parse().map_err(|_| {
                        PoolerError::ConfigError("invalid max_client_conn".to_string())
                    })?;
                }
                "default_pool_size" => {
                    config.default_pool_size = value.parse().map_err(|_| {
                        PoolerError::ConfigError("invalid default_pool_size".to_string())
                    })?;
                }
                "default_pool_mode" | "pool_mode" => {
                    config.default_pool_mode = match value.to_lowercase().as_str() {
                        "transaction" => PoolMode::Transaction,
                        "session" => PoolMode::Session,
                        "statement" => PoolMode::Statement,
                        _ => {
                            return Err(PoolerError::ConfigError(format!(
                                "invalid pool_mode: {}",
                                value
                            )))
                        }
                    };
                }
                "log_connections" => {
                    config.log_connections = value.parse().unwrap_or(value == "1");
                }
                "log_disconnections" => {
                    config.log_disconnections = value.parse().unwrap_or(value == "1");
                }
                "log_pooler_errors" => {
                    config.log_pooler_errors = value.parse().unwrap_or(value == "1");
                }
                "server_reset_query" => {
                    config.server_reset_query = value.to_string();
                }
                "server_check_query" => {
                    config.server_check_query = value.to_string();
                }
                "stats_period" => {
                    let secs: u64 = value.parse().map_err(|_| {
                        PoolerError::ConfigError("invalid stats_period".to_string())
                    })?;
                    config.stats_period = Duration::from_secs(secs);
                }
                "admin_users" => {
                    config.admin_users = value.split(',').map(|s| s.trim().to_string()).collect();
                }
                _ => {
                    // Ignore unknown parameters for forward compatibility
                }
            }
        }

        Ok(config)
    }

    /// Reload database configurations from file
    pub fn reload_databases(&self, contents: &str) -> Result<usize, PoolerError> {
        let mut added = 0;

        for line in contents.lines() {
            let line = line.trim();

            // Skip comments, empty lines, and section headers (except [databases])
            if line.is_empty()
                || line.starts_with('#')
                || line.starts_with(';')
                || (line.starts_with('[') && !line.eq_ignore_ascii_case("[databases]"))
            {
                continue;
            }

            // Skip the [databases] header itself
            if line.eq_ignore_ascii_case("[databases]") {
                continue;
            }

            // Parse: dbname = host=... port=... ...
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }

            let db_name = parts[0].trim();
            let params = parts[1].trim();

            let mut db_config = DatabaseConfig {
                name: db_name.to_string(),
                ..Default::default()
            };

            // Parse connection string parameters
            for param in params.split_whitespace() {
                if let Some((key, value)) = param.split_once('=') {
                    match key {
                        "host" => db_config.host = value.to_string(),
                        "port" => {
                            db_config.port = value.parse().unwrap_or(5432);
                        }
                        "dbname" => db_config.dbname = Some(value.to_string()),
                        "user" => db_config.user = Some(value.to_string()),
                        "password" => db_config.password = Some(value.to_string()),
                        "pool_size" => {
                            db_config.max_db_connections = value.parse().unwrap_or(100);
                        }
                        "pool_mode" => {
                            db_config.pool_mode = match value {
                                "transaction" => PoolMode::Transaction,
                                "session" => PoolMode::Session,
                                "statement" => PoolMode::Statement,
                                _ => PoolMode::Transaction,
                            };
                        }
                        _ => {}
                    }
                }
            }

            if self.add_database(db_config).is_ok() {
                added += 1;
            }
        }

        Ok(added)
    }
}

/// Summary of a pool
#[derive(Debug, Clone)]
pub struct PoolSummary {
    pub database: String,
    pub user: String,
    pub idle_count: usize,
    pub active_count: usize,
    pub waiting_count: usize,
    pub pool_mode: PoolMode,
}

/// Summary of a client
#[derive(Debug, Clone)]
pub struct ClientSummary {
    pub id: u64,
    pub database: String,
    pub user: String,
    pub addr: String,
    pub state: ClientState,
    pub server_id: Option<u64>,
    pub connect_time_secs: u64,
}

/// Summary of a server
#[derive(Debug, Clone)]
pub struct ServerSummary {
    pub id: u64,
    pub database: String,
    pub user: String,
    pub host: String,
    pub port: u16,
    pub state: ServerState,
    pub client_id: Option<u64>,
    pub connect_time_secs: u64,
    pub query_count: u64,
}

/// Admin command handler (SHOW commands like PgBouncer)
pub struct AdminCommands;

impl AdminCommands {
    /// Execute an admin command
    pub fn execute(pooler: &ConnectionPooler, command: &str) -> Result<AdminResult, PoolerError> {
        let upper = command.trim().to_uppercase();

        if upper.starts_with("SHOW ") {
            let target = upper.strip_prefix("SHOW ").unwrap().trim();
            match target {
                "STATS" => Ok(AdminResult::Stats(pooler.get_stats())),
                "POOLS" => Ok(AdminResult::Pools(pooler.list_pools())),
                "CLIENTS" => Ok(AdminResult::Clients(pooler.list_clients())),
                "SERVERS" => Ok(AdminResult::Servers(pooler.list_servers())),
                "CONFIG" => Ok(AdminResult::Config(pooler.get_config())),
                "DATABASES" => {
                    let dbs = pooler.databases.read();
                    let names: Vec<String> = dbs.keys().cloned().collect();
                    Ok(AdminResult::Databases(names))
                }
                "PAUSED" => {
                    let paused = pooler.paused_databases.read();
                    let paused_dbs: Vec<String> = paused
                        .iter()
                        .filter(|(_, state)| matches!(state, DatabasePauseState::Paused))
                        .map(|(name, _)| name.clone())
                        .collect();
                    Ok(AdminResult::PausedDatabases(paused_dbs))
                }
                _ => Err(PoolerError::QueryFailed(format!(
                    "unknown show target: {}",
                    target
                ))),
            }
        } else if upper.starts_with("PAUSE ") {
            let db = command.trim()[6..].trim();
            pooler.pause_database(db)?;
            Ok(AdminResult::Ok)
        } else if upper.starts_with("RESUME ") {
            let db = command.trim()[7..].trim();
            pooler.resume_database(db)?;
            Ok(AdminResult::Ok)
        } else if upper.starts_with("ENABLE ") {
            let db = command.trim()[7..].trim();
            pooler.enable_database(db)?;
            Ok(AdminResult::Ok)
        } else if upper.starts_with("DISABLE ") {
            let db = command.trim()[8..].trim();
            pooler.disable_database(db)?;
            Ok(AdminResult::Ok)
        } else if upper.starts_with("WAIT ") {
            let db = command.trim()[5..].trim();
            let active = pooler.wait_database(db)?;
            Ok(AdminResult::Waiting(active))
        } else if upper.starts_with("KILL ") {
            let db = command.trim()[5..].trim();
            let killed = pooler.kill_database(db)?;
            Ok(AdminResult::Killed(killed))
        } else if upper == "SHUTDOWN" {
            pooler.shutdown();
            Ok(AdminResult::Ok)
        } else if upper == "RELOAD" {
            pooler.reload_config()?;
            Ok(AdminResult::Ok)
        } else if upper.starts_with("SET ") {
            // SET parameter = value
            let rest = command.trim()[4..].trim();
            Self::handle_set_command(pooler, rest)?;
            Ok(AdminResult::Ok)
        } else {
            Err(PoolerError::QueryFailed(format!(
                "unknown command: {}",
                command
            )))
        }
    }

    /// Handle SET parameter = value commands
    fn handle_set_command(pooler: &ConnectionPooler, param_value: &str) -> Result<(), PoolerError> {
        let parts: Vec<&str> = param_value.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(PoolerError::QueryFailed(
                "SET requires parameter = value".to_string(),
            ));
        }

        let param = parts[0].trim().to_lowercase();
        let value = parts[1].trim().trim_matches(|c| c == '\'' || c == '"');

        let mut config = pooler.config.write();
        match param.as_str() {
            "default_pool_size" => {
                config.default_pool_size = value.parse().map_err(|_| {
                    PoolerError::ConfigError("invalid default_pool_size value".to_string())
                })?;
            }
            "max_client_conn" => {
                config.max_client_conn = value.parse().map_err(|_| {
                    PoolerError::ConfigError("invalid max_client_conn value".to_string())
                })?;
            }
            "log_connections" => {
                config.log_connections = value.parse().map_err(|_| {
                    PoolerError::ConfigError("invalid log_connections value".to_string())
                })?;
            }
            "log_disconnections" => {
                config.log_disconnections = value.parse().map_err(|_| {
                    PoolerError::ConfigError("invalid log_disconnections value".to_string())
                })?;
            }
            "log_pooler_errors" => {
                config.log_pooler_errors = value.parse().map_err(|_| {
                    PoolerError::ConfigError("invalid log_pooler_errors value".to_string())
                })?;
            }
            "server_reset_query" => {
                config.server_reset_query = value.to_string();
            }
            "server_check_query" => {
                config.server_check_query = value.to_string();
            }
            _ => {
                return Err(PoolerError::ConfigError(format!(
                    "unknown parameter: {}",
                    param
                )));
            }
        }

        Ok(())
    }
}

/// Result of an admin command
#[derive(Debug)]
pub enum AdminResult {
    Ok,
    Stats(PoolerStatsSnapshot),
    Pools(Vec<PoolSummary>),
    Clients(Vec<ClientSummary>),
    Servers(Vec<ServerSummary>),
    Config(PoolerConfig),
    Databases(Vec<String>),
    PausedDatabases(Vec<String>),
    Killed(usize),
    Waiting(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_mode_default() {
        assert_eq!(PoolMode::default(), PoolMode::Transaction);
    }

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.max_db_connections, 100);
        assert_eq!(config.min_pool_size, 5);
    }

    #[test]
    fn test_pooler_config_default() {
        let config = PoolerConfig::default();
        assert_eq!(config.listen_port, 6432);
        assert_eq!(config.max_client_conn, 1000);
        assert_eq!(config.default_pool_size, 20);
    }

    #[test]
    fn test_server_connection_new() {
        let server = ServerConnection::new(
            1,
            "testdb".to_string(),
            "testuser".to_string(),
            "localhost".to_string(),
            5432,
        );
        assert_eq!(server.id, 1);
        assert_eq!(server.database, "testdb");
        assert_eq!(server.state, ServerState::Connecting);
        assert!(server.client_id.is_none());
    }

    #[test]
    fn test_client_connection_new() {
        let client = ClientConnection::new(
            1,
            "testdb".to_string(),
            "testuser".to_string(),
            "127.0.0.1".to_string(),
        );
        assert_eq!(client.id, 1);
        assert_eq!(client.database, "testdb");
        assert_eq!(client.state, ClientState::Authenticating);
        assert!(client.server_id.is_none());
    }

    #[test]
    fn test_connection_pool_basic() {
        let config = DatabaseConfig {
            name: "testdb".to_string(),
            max_db_connections: 10,
            max_client_wait: 5,
            ..Default::default()
        };
        let mut pool = ConnectionPool::new("testdb".to_string(), "testuser".to_string(), config);

        // Initially empty
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.active_count(), 0);

        // Add an idle connection
        pool.idle_servers.push_back(1);
        assert_eq!(pool.idle_count(), 1);

        // Get connection
        let conn = pool.get_connection();
        assert_eq!(conn, Some(1));
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.stats.pool_hits, 1);

        // Return connection
        pool.return_connection(1);
        assert_eq!(pool.idle_count(), 1);
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_pooler_add_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            ..Default::default()
        };

        assert!(pooler.add_database(db_config).is_ok());
        assert!(pooler.get_database("testdb").is_some());
    }

    #[test]
    fn test_pooler_add_database_empty_name() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig::default();

        assert!(matches!(
            pooler.add_database(db_config),
            Err(PoolerError::ConfigError(_))
        ));
    }

    #[test]
    fn test_pooler_accept_client() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        assert_eq!(client_id, 1);

        let stats = pooler.get_stats();
        assert_eq!(stats.current_clients, 1);
        assert_eq!(stats.total_clients, 1);
    }

    #[test]
    fn test_pooler_unknown_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = pooler.accept_client("unknown", "user1", "127.0.0.1");
        assert!(matches!(result, Err(PoolerError::UnknownDatabase(_))));
    }

    #[test]
    fn test_pooler_authenticate_client() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        assert!(pooler
            .authenticate_client(client_id, Some("password"))
            .is_ok());
    }

    #[test]
    fn test_pooler_get_server_connection() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();

        let server_id = pooler.get_server_connection(client_id).unwrap();
        assert_eq!(server_id, 1);

        let stats = pooler.get_stats();
        assert_eq!(stats.current_servers, 1);
    }

    #[test]
    fn test_pooler_release_connection() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            pool_mode: PoolMode::Transaction,
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();
        pooler.get_server_connection(client_id).unwrap();

        // Release connection (transaction complete)
        pooler.release_server_connection(client_id, true).unwrap();

        // Pool should have idle connection
        let pools = pooler.list_pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].idle_count, 1);
        assert_eq!(pools[0].active_count, 0);
    }

    #[test]
    fn test_pooler_record_query() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();
        pooler.get_server_connection(client_id).unwrap();

        pooler.record_query(client_id, "SELECT 1", 100, 50);

        let stats = pooler.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.total_bytes_sent, 100);
        assert_eq!(stats.total_bytes_received, 50);
    }

    #[test]
    fn test_pooler_disconnect_client() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();

        assert_eq!(pooler.get_stats().current_clients, 1);

        pooler.disconnect_client(client_id).unwrap();

        assert_eq!(pooler.get_stats().current_clients, 0);
    }

    #[test]
    fn test_pooler_shutdown() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        assert!(!pooler.is_shutting_down());
        pooler.shutdown();
        assert!(pooler.is_shutting_down());

        // Should reject new clients
        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let result = pooler.accept_client("testdb", "user1", "127.0.0.1");
        assert!(matches!(result, Err(PoolerError::ShuttingDown)));
    }

    #[test]
    fn test_pooler_list_operations() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();
        pooler.get_server_connection(client_id).unwrap();

        let pools = pooler.list_pools();
        assert_eq!(pools.len(), 1);

        let clients = pooler.list_clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].database, "testdb");

        let servers = pooler.list_servers();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].database, "testdb");
    }

    #[test]
    fn test_admin_commands_show_stats() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = AdminCommands::execute(&pooler, "SHOW STATS").unwrap();
        match result {
            AdminResult::Stats(stats) => {
                assert_eq!(stats.current_clients, 0);
            }
            _ => panic!("expected Stats result"),
        }
    }

    #[test]
    fn test_admin_commands_show_pools() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = AdminCommands::execute(&pooler, "SHOW POOLS").unwrap();
        match result {
            AdminResult::Pools(pools) => {
                assert!(pools.is_empty());
            }
            _ => panic!("expected Pools result"),
        }
    }

    #[test]
    fn test_admin_commands_show_config() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = AdminCommands::execute(&pooler, "SHOW CONFIG").unwrap();
        match result {
            AdminResult::Config(config) => {
                assert_eq!(config.listen_port, 6432);
            }
            _ => panic!("expected Config result"),
        }
    }

    #[test]
    fn test_admin_commands_unknown() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = AdminCommands::execute(&pooler, "UNKNOWN COMMAND");
        assert!(matches!(result, Err(PoolerError::QueryFailed(_))));
    }

    #[test]
    fn test_pool_modes() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        // Test session mode
        let db_config = DatabaseConfig {
            name: "session_db".to_string(),
            pool_mode: PoolMode::Session,
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        let client_id = pooler
            .accept_client("session_db", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();
        pooler.get_server_connection(client_id).unwrap();

        // Release with transaction complete - should NOT return to pool in session mode
        pooler.release_server_connection(client_id, true).unwrap();

        // Client should still have server assigned
        let clients = pooler.list_clients();
        assert_eq!(clients.len(), 1);
        // In session mode, server_id should remain None after release but connection stays active
    }

    #[test]
    fn test_pool_waiting_clients() {
        let config = DatabaseConfig {
            name: "testdb".to_string(),
            max_client_wait: 2,
            ..Default::default()
        };
        let mut pool = ConnectionPool::new("testdb".to_string(), "testuser".to_string(), config);

        // Add waiting clients
        assert!(pool.add_waiting_client(1).is_ok());
        assert!(pool.add_waiting_client(2).is_ok());
        assert_eq!(pool.waiting_count(), 2);

        // Third should fail
        assert!(matches!(
            pool.add_waiting_client(3),
            Err(PoolerError::TooManyWaitingClients)
        ));

        // Get waiting clients
        assert_eq!(pool.get_waiting_client(), Some(1));
        assert_eq!(pool.get_waiting_client(), Some(2));
        assert_eq!(pool.get_waiting_client(), None);
    }

    #[test]
    fn test_kill_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        // Create some connections
        let client1 = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client1, None).unwrap();
        pooler.get_server_connection(client1).unwrap();

        let client2 = pooler
            .accept_client("testdb", "user2", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client2, None).unwrap();
        pooler.get_server_connection(client2).unwrap();

        assert_eq!(pooler.get_stats().current_servers, 2);

        // Kill all connections for the database
        let killed = pooler.kill_database("testdb").unwrap();
        assert_eq!(killed, 2);
        assert_eq!(pooler.get_stats().current_servers, 0);
    }

    #[test]
    fn test_connection_pool_needs_more() {
        let config = DatabaseConfig {
            name: "testdb".to_string(),
            max_db_connections: 10,
            ..Default::default()
        };
        let mut pool = ConnectionPool::new("testdb".to_string(), "testuser".to_string(), config);

        // Empty pool needs connections
        assert!(pool.needs_more_connections());

        // Add some idle connections
        pool.idle_servers.push_back(1);
        pool.idle_servers.push_back(2);

        // Has idle connections, no waiting clients - doesn't need more
        assert!(!pool.needs_more_connections());

        // Add waiting client - now needs more
        pool.waiting_clients.push_back(100);
        assert!(pool.needs_more_connections());
    }

    #[test]
    fn test_pooler_error_display() {
        assert_eq!(PoolerError::TooManyClients.to_string(), "too many clients");
        assert_eq!(
            PoolerError::UnknownDatabase("test".to_string()).to_string(),
            "unknown database: test"
        );
        assert_eq!(
            PoolerError::ConnectionTimeout.to_string(),
            "connection timeout"
        );
        assert_eq!(PoolerError::PoolTimeout.to_string(), "pool timeout");
        assert_eq!(
            PoolerError::ShuttingDown.to_string(),
            "pooler is shutting down"
        );
    }

    #[test]
    fn test_server_connection_helpers() {
        let mut server = ServerConnection::new(
            1,
            "testdb".to_string(),
            "user".to_string(),
            "localhost".to_string(),
            5432,
        );

        // Initially connecting, not idle
        assert!(!server.is_idle());

        // Set to idle
        server.state = ServerState::Idle;
        assert!(server.is_idle());

        // Assign client
        server.client_id = Some(1);
        assert!(!server.is_idle());

        // Check age and idle duration
        assert!(server.age().as_millis() < 100);
        assert!(server.idle_duration().as_millis() < 100);
    }

    #[test]
    fn test_client_connection_helpers() {
        let mut client = ClientConnection::new(
            1,
            "testdb".to_string(),
            "user".to_string(),
            "127.0.0.1".to_string(),
        );

        // Initially authenticating, not active
        assert!(!client.is_active());

        // Set to active
        client.state = ClientState::Active;
        assert!(client.is_active());

        // In transaction is also active
        client.state = ClientState::InTransaction;
        assert!(client.is_active());

        // Idle is not active
        client.state = ClientState::Idle;
        assert!(!client.is_active());

        // Wait duration without request time
        assert_eq!(client.wait_duration(), Duration::ZERO);

        // With request time
        client.request_time = Some(Instant::now());
        assert!(client.wait_duration().as_millis() < 100);
    }

    #[test]
    fn test_pause_resume_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        // Database should not be paused initially
        assert!(!pooler.is_database_paused("testdb"));
        assert_eq!(
            pooler.get_database_state("testdb"),
            DatabasePauseState::Active
        );

        // Pause database
        pooler.pause_database("testdb").unwrap();
        assert!(pooler.is_database_paused("testdb"));
        assert_eq!(
            pooler.get_database_state("testdb"),
            DatabasePauseState::Paused
        );

        // Should reject new connections to paused database
        let result = pooler.accept_client("testdb", "user1", "127.0.0.1");
        assert!(matches!(result, Err(PoolerError::DatabasePaused(_))));

        // Resume database
        pooler.resume_database("testdb").unwrap();
        assert!(!pooler.is_database_paused("testdb"));

        // Should accept connections again
        let client_id = pooler.accept_client("testdb", "user1", "127.0.0.1");
        assert!(client_id.is_ok());
    }

    #[test]
    fn test_pause_unknown_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let result = pooler.pause_database("nonexistent");
        assert!(matches!(result, Err(PoolerError::UnknownDatabase(_))));
    }

    #[test]
    fn test_wait_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        // Create connection
        let client_id = pooler
            .accept_client("testdb", "user1", "127.0.0.1")
            .unwrap();
        pooler.authenticate_client(client_id, None).unwrap();
        pooler.get_server_connection(client_id).unwrap();

        // Cannot wait on non-paused database
        let result = pooler.wait_database("testdb");
        assert!(matches!(result, Err(PoolerError::ConfigError(_))));

        // Pause and wait
        pooler.pause_database("testdb").unwrap();
        let active = pooler.wait_database("testdb").unwrap();
        assert_eq!(active, 1); // One active connection
    }

    #[test]
    fn test_admin_command_pause_resume() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        // PAUSE command
        let result = AdminCommands::execute(&pooler, "PAUSE testdb").unwrap();
        assert!(matches!(result, AdminResult::Ok));
        assert!(pooler.is_database_paused("testdb"));

        // SHOW PAUSED command
        let result = AdminCommands::execute(&pooler, "SHOW PAUSED").unwrap();
        match result {
            AdminResult::PausedDatabases(dbs) => {
                assert_eq!(dbs.len(), 1);
                assert_eq!(dbs[0], "testdb");
            }
            _ => panic!("expected PausedDatabases result"),
        }

        // RESUME command
        let result = AdminCommands::execute(&pooler, "RESUME testdb").unwrap();
        assert!(matches!(result, AdminResult::Ok));
        assert!(!pooler.is_database_paused("testdb"));
    }

    #[test]
    fn test_admin_command_set() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        // SET default_pool_size
        let result = AdminCommands::execute(&pooler, "SET default_pool_size = 50").unwrap();
        assert!(matches!(result, AdminResult::Ok));
        assert_eq!(pooler.get_config().default_pool_size, 50);

        // SET max_client_conn
        let result = AdminCommands::execute(&pooler, "SET max_client_conn = 500").unwrap();
        assert!(matches!(result, AdminResult::Ok));
        assert_eq!(pooler.get_config().max_client_conn, 500);

        // SET server_reset_query
        let result =
            AdminCommands::execute(&pooler, "SET server_reset_query = 'RESET ALL'").unwrap();
        assert!(matches!(result, AdminResult::Ok));
        assert_eq!(pooler.get_config().server_reset_query, "RESET ALL");
    }

    #[test]
    fn test_admin_command_set_invalid() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        // Invalid parameter
        let result = AdminCommands::execute(&pooler, "SET unknown_param = value");
        assert!(matches!(result, Err(PoolerError::ConfigError(_))));

        // Invalid value
        let result = AdminCommands::execute(&pooler, "SET max_client_conn = not_a_number");
        assert!(matches!(result, Err(PoolerError::ConfigError(_))));
    }

    #[test]
    fn test_parse_config() {
        let config_str = r#"
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
max_client_conn = 500
default_pool_size = 25
pool_mode = transaction
log_connections = true
server_reset_query = DISCARD ALL
stats_period = 120
admin_users = admin1, admin2
"#;

        let config = ConnectionPooler::parse_config(config_str).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1");
        assert_eq!(config.listen_port, 6432);
        assert_eq!(config.max_client_conn, 500);
        assert_eq!(config.default_pool_size, 25);
        assert_eq!(config.default_pool_mode, PoolMode::Transaction);
        assert!(config.log_connections);
        assert_eq!(config.server_reset_query, "DISCARD ALL");
        assert_eq!(config.stats_period, Duration::from_secs(120));
        assert_eq!(config.admin_users, vec!["admin1", "admin2"]);
    }

    #[test]
    fn test_reload_databases() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config_str = r#"
[databases]
db1 = host=localhost port=5432 dbname=mydb1 user=user1
db2 = host=127.0.0.1 port=5433 pool_size=50 pool_mode=session
"#;

        let added = pooler.reload_databases(db_config_str).unwrap();
        assert_eq!(added, 2);

        let db1 = pooler.get_database("db1").unwrap();
        assert_eq!(db1.host, "localhost");
        assert_eq!(db1.port, 5432);
        assert_eq!(db1.dbname, Some("mydb1".to_string()));

        let db2 = pooler.get_database("db2").unwrap();
        assert_eq!(db2.host, "127.0.0.1");
        assert_eq!(db2.port, 5433);
        assert_eq!(db2.max_db_connections, 50);
        assert_eq!(db2.pool_mode, PoolMode::Session);
    }

    #[test]
    fn test_enable_disable_database() {
        let pooler = ConnectionPooler::new(PoolerConfig::default());

        let db_config = DatabaseConfig {
            name: "testdb".to_string(),
            ..Default::default()
        };
        pooler.add_database(db_config).unwrap();

        // DISABLE is same as PAUSE
        pooler.disable_database("testdb").unwrap();
        assert!(pooler.is_database_paused("testdb"));

        // ENABLE is same as RESUME
        pooler.enable_database("testdb").unwrap();
        assert!(!pooler.is_database_paused("testdb"));
    }

    #[test]
    fn test_database_paused_error_display() {
        assert_eq!(
            PoolerError::DatabasePaused("test".to_string()).to_string(),
            "database is paused: test"
        );
        assert_eq!(
            PoolerError::ReloadFailed("file not found".to_string()).to_string(),
            "reload failed: file not found"
        );
    }
}
