//! Connection Management Module
//!
//! PostgreSQL-compatible connection pooling and management features.
//!
//! # Features
//! - Connection pooling with multiplexing (PgBouncer-style)
//! - Idle connection timeouts
//! - Max connections per user/database
//! - Connection limits and quotas
//! - Connection lifecycle management
//!
//! # Example
//! ```sql
//! -- Set connection timeout
//! SET idle_in_transaction_session_timeout = '5min';
//!
//! -- View connections
//! SELECT * FROM pg_stat_activity;
//!
//! -- Terminate a connection
//! SELECT pg_terminate_backend(pid);
//! ```

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;

// ============================================================================
// Types and Errors
// ============================================================================

/// Connection ID type
pub type ConnectionId = u64;

/// Process ID (PostgreSQL-style backend PID)
pub type BackendPid = u32;

/// Errors from connection management
#[derive(Debug, Clone)]
pub enum ConnectionError {
    /// Too many connections
    TooManyConnections { current: usize, max: usize },
    /// Too many connections for user
    TooManyUserConnections {
        user: String,
        current: usize,
        max: usize,
    },
    /// Too many connections for database
    TooManyDatabaseConnections {
        database: String,
        current: usize,
        max: usize,
    },
    /// Connection not found
    ConnectionNotFound(ConnectionId),
    /// Connection timeout
    ConnectionTimeout(ConnectionId),
    /// Pool exhausted
    PoolExhausted { pool_name: String },
    /// Authentication failed
    AuthenticationFailed(String),
    /// Connection refused
    ConnectionRefused(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyConnections { current, max } => {
                write!(
                    f,
                    "too many connections: {} (max {})",
                    current, max
                )
            }
            Self::TooManyUserConnections { user, current, max } => {
                write!(
                    f,
                    "too many connections for user '{}': {} (max {})",
                    user, current, max
                )
            }
            Self::TooManyDatabaseConnections {
                database,
                current,
                max,
            } => {
                write!(
                    f,
                    "too many connections for database '{}': {} (max {})",
                    database, current, max
                )
            }
            Self::ConnectionNotFound(id) => write!(f, "connection {} not found", id),
            Self::ConnectionTimeout(id) => write!(f, "connection {} timed out", id),
            Self::PoolExhausted { pool_name } => write!(f, "connection pool '{}' exhausted", pool_name),
            Self::AuthenticationFailed(msg) => write!(f, "authentication failed: {}", msg),
            Self::ConnectionRefused(msg) => write!(f, "connection refused: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for ConnectionError {}

// ============================================================================
// Connection State
// ============================================================================

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is being established
    Connecting,
    /// Connection is idle (available)
    Idle,
    /// Connection is active (executing query)
    Active,
    /// Connection is idle in transaction
    IdleInTransaction,
    /// Connection is idle in aborted transaction
    IdleInTransactionAborted,
    /// Connection is being closed
    Closing,
    /// Connection is closed
    Closed,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connecting => write!(f, "connecting"),
            Self::Idle => write!(f, "idle"),
            Self::Active => write!(f, "active"),
            Self::IdleInTransaction => write!(f, "idle in transaction"),
            Self::IdleInTransactionAborted => write!(f, "idle in transaction (aborted)"),
            Self::Closing => write!(f, "closing"),
            Self::Closed => write!(f, "closed"),
        }
    }
}

/// Wait event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitEventType {
    /// No wait
    None,
    /// Waiting on client
    Client,
    /// Waiting on lock
    Lock,
    /// Waiting on I/O
    IO,
    /// Waiting on IPC
    IPC,
    /// Waiting on timeout
    Timeout,
    /// Waiting on extension
    Extension,
}

impl std::fmt::Display for WaitEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::Client => write!(f, "Client"),
            Self::Lock => write!(f, "Lock"),
            Self::IO => write!(f, "IO"),
            Self::IPC => write!(f, "IPC"),
            Self::Timeout => write!(f, "Timeout"),
            Self::Extension => write!(f, "Extension"),
        }
    }
}

// ============================================================================
// Connection Info
// ============================================================================

/// Information about a connection (pg_stat_activity compatible)
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Connection ID
    pub id: ConnectionId,
    /// Backend process ID
    pub pid: BackendPid,
    /// Database name
    pub database: String,
    /// Username
    pub username: String,
    /// Application name
    pub application_name: String,
    /// Client address
    pub client_addr: Option<SocketAddr>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Client port
    pub client_port: Option<u16>,
    /// Backend start time
    pub backend_start: SystemTime,
    /// Transaction start time
    pub xact_start: Option<SystemTime>,
    /// Query start time
    pub query_start: Option<SystemTime>,
    /// State change time
    pub state_change: SystemTime,
    /// Current state
    pub state: ConnectionState,
    /// Wait event type
    pub wait_event_type: WaitEventType,
    /// Wait event
    pub wait_event: Option<String>,
    /// Current query
    pub query: Option<String>,
    /// Backend type
    pub backend_type: String,
}

impl ConnectionInfo {
    /// Create new connection info
    pub fn new(
        id: ConnectionId,
        pid: BackendPid,
        database: &str,
        username: &str,
        client_addr: Option<SocketAddr>,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            pid,
            database: database.to_string(),
            username: username.to_string(),
            application_name: String::new(),
            client_addr,
            client_hostname: None,
            client_port: client_addr.map(|a| a.port()),
            backend_start: now,
            xact_start: None,
            query_start: None,
            state_change: now,
            state: ConnectionState::Connecting,
            wait_event_type: WaitEventType::None,
            wait_event: None,
            query: None,
            backend_type: "client backend".to_string(),
        }
    }

    /// Set application name
    pub fn with_application_name(mut self, name: &str) -> Self {
        self.application_name = name.to_string();
        self
    }

    /// Update state
    pub fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
        self.state_change = SystemTime::now();
    }

    /// Start query
    pub fn start_query(&mut self, query: &str) {
        self.query = Some(query.to_string());
        self.query_start = Some(SystemTime::now());
        self.state = ConnectionState::Active;
        self.state_change = SystemTime::now();
    }

    /// End query
    pub fn end_query(&mut self) {
        self.query = None;
        self.query_start = None;
        self.state = ConnectionState::Idle;
        self.state_change = SystemTime::now();
    }

    /// Start transaction
    pub fn start_transaction(&mut self) {
        self.xact_start = Some(SystemTime::now());
    }

    /// End transaction
    pub fn end_transaction(&mut self, aborted: bool) {
        self.xact_start = None;
        if aborted {
            self.state = ConnectionState::IdleInTransactionAborted;
        } else {
            self.state = ConnectionState::Idle;
        }
        self.state_change = SystemTime::now();
    }

    /// Get query duration in milliseconds
    pub fn query_duration_ms(&self) -> Option<u64> {
        self.query_start.map(|start| {
            SystemTime::now()
                .duration_since(start)
                .unwrap_or_default()
                .as_millis() as u64
        })
    }

    /// Get transaction duration in milliseconds
    pub fn transaction_duration_ms(&self) -> Option<u64> {
        self.xact_start.map(|start| {
            SystemTime::now()
                .duration_since(start)
                .unwrap_or_default()
                .as_millis() as u64
        })
    }

    /// Get idle time in milliseconds (time since last activity)
    pub fn idle_time_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(self.state_change)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

// ============================================================================
// Connection Limits
// ============================================================================

/// Connection limits configuration
#[derive(Debug, Clone)]
pub struct ConnectionLimits {
    /// Maximum total connections
    pub max_connections: usize,
    /// Maximum connections per user (0 = unlimited)
    pub max_connections_per_user: usize,
    /// Maximum connections per database (0 = unlimited)
    pub max_connections_per_database: usize,
    /// Superuser reserved connections
    pub superuser_reserved_connections: usize,
    /// Idle session timeout in milliseconds (0 = disabled)
    pub idle_session_timeout_ms: u64,
    /// Idle in transaction timeout in milliseconds (0 = disabled)
    pub idle_in_transaction_timeout_ms: u64,
    /// Statement timeout in milliseconds (0 = disabled)
    pub statement_timeout_ms: u64,
    /// Lock timeout in milliseconds (0 = disabled)
    pub lock_timeout_ms: u64,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_connections_per_user: 0,
            max_connections_per_database: 0,
            superuser_reserved_connections: 3,
            idle_session_timeout_ms: 0,
            idle_in_transaction_timeout_ms: 0,
            statement_timeout_ms: 0,
            lock_timeout_ms: 0,
        }
    }
}

// ============================================================================
// Connection Pool
// ============================================================================

/// Pool mode for connection multiplexing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMode {
    /// One server connection per client (no pooling)
    Session,
    /// Connection released after transaction
    Transaction,
    /// Connection released after each statement
    Statement,
}

impl Default for PoolMode {
    fn default() -> Self {
        Self::Transaction
    }
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Pool name
    pub name: String,
    /// Target database
    pub database: String,
    /// Pool mode
    pub mode: PoolMode,
    /// Minimum pool size
    pub min_size: usize,
    /// Maximum pool size
    pub max_size: usize,
    /// Connection acquire timeout in milliseconds
    pub acquire_timeout_ms: u64,
    /// Idle connection timeout in milliseconds
    pub idle_timeout_ms: u64,
    /// Maximum connection lifetime in milliseconds
    pub max_lifetime_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            database: "default".to_string(),
            mode: PoolMode::Transaction,
            min_size: 5,
            max_size: 20,
            acquire_timeout_ms: 30000,
            idle_timeout_ms: 600000,
            max_lifetime_ms: 3600000,
        }
    }
}

/// Pooled connection wrapper
#[derive(Debug)]
pub struct PooledConnection {
    /// Connection ID
    pub id: ConnectionId,
    /// Pool name
    pub pool_name: String,
    /// Checkout time
    pub checkout_time: Instant,
    /// Last use time
    pub last_use_time: Instant,
    /// Created time
    pub created_time: Instant,
    /// Number of times used
    pub use_count: u64,
}

impl PooledConnection {
    fn new(id: ConnectionId, pool_name: &str) -> Self {
        let now = Instant::now();
        Self {
            id,
            pool_name: pool_name.to_string(),
            checkout_time: now,
            last_use_time: now,
            created_time: now,
            use_count: 0,
        }
    }

    fn touch(&mut self) {
        self.last_use_time = Instant::now();
        self.use_count += 1;
    }

    fn age_ms(&self) -> u64 {
        self.created_time.elapsed().as_millis() as u64
    }

    fn idle_ms(&self) -> u64 {
        self.last_use_time.elapsed().as_millis() as u64
    }
}

/// Connection pool
pub struct ConnectionPool {
    /// Configuration
    config: PoolConfig,
    /// Available connections
    available: Mutex<VecDeque<PooledConnection>>,
    /// In-use connections count
    in_use: AtomicUsize,
    /// Total connections created
    total_created: AtomicU64,
    /// Total checkouts
    total_checkouts: AtomicU64,
    /// Total checkins
    total_checkins: AtomicU64,
    /// Checkout timeouts
    checkout_timeouts: AtomicU64,
    /// Next connection ID
    next_id: AtomicU64,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            available: Mutex::new(VecDeque::new()),
            in_use: AtomicUsize::new(0),
            total_created: AtomicU64::new(0),
            total_checkouts: AtomicU64::new(0),
            total_checkins: AtomicU64::new(0),
            checkout_timeouts: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
        }
    }

    /// Get pool name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get current pool size
    pub fn size(&self) -> usize {
        self.available.lock().len() + self.in_use.load(Ordering::Relaxed)
    }

    /// Get available connections
    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    /// Get in-use connections
    pub fn in_use_count(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Checkout a connection from the pool
    pub fn checkout(&self) -> Result<PooledConnection, ConnectionError> {
        let mut available = self.available.lock();

        // Try to get an existing connection
        if let Some(mut conn) = available.pop_front() {
            // Check if connection is too old
            if self.config.max_lifetime_ms > 0 && conn.age_ms() > self.config.max_lifetime_ms {
                // Connection expired, create new one
                drop(conn);
            } else {
                conn.touch();
                self.in_use.fetch_add(1, Ordering::Relaxed);
                self.total_checkouts.fetch_add(1, Ordering::Relaxed);
                return Ok(conn);
            }
        }

        // Check if we can create a new connection
        let current_size = available.len() + self.in_use.load(Ordering::Relaxed);
        if current_size >= self.config.max_size {
            self.checkout_timeouts.fetch_add(1, Ordering::Relaxed);
            return Err(ConnectionError::PoolExhausted {
                pool_name: self.config.name.clone(),
            });
        }

        // Create new connection
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut conn = PooledConnection::new(id, &self.config.name);
        conn.touch();

        self.total_created.fetch_add(1, Ordering::Relaxed);
        self.in_use.fetch_add(1, Ordering::Relaxed);
        self.total_checkouts.fetch_add(1, Ordering::Relaxed);

        Ok(conn)
    }

    /// Return a connection to the pool
    pub fn checkin(&self, conn: PooledConnection) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        self.total_checkins.fetch_add(1, Ordering::Relaxed);

        // Check if connection should be kept
        if self.config.max_lifetime_ms > 0 && conn.age_ms() > self.config.max_lifetime_ms {
            // Connection expired, don't return to pool
            return;
        }

        let mut available = self.available.lock();

        // Check pool size
        if available.len() < self.config.max_size {
            available.push_back(conn);
        }
    }

    /// Clean up idle connections
    pub fn cleanup(&self) -> usize {
        let mut available = self.available.lock();
        let min_size = self.config.min_size;
        let idle_timeout = self.config.idle_timeout_ms;

        let mut removed = 0;
        while available.len() > min_size {
            if let Some(conn) = available.front() {
                if idle_timeout > 0 && conn.idle_ms() > idle_timeout {
                    available.pop_front();
                    removed += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        removed
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let available = self.available.lock();
        PoolStats {
            name: self.config.name.clone(),
            mode: self.config.mode,
            size: available.len() + self.in_use.load(Ordering::Relaxed),
            available: available.len(),
            in_use: self.in_use.load(Ordering::Relaxed),
            max_size: self.config.max_size,
            min_size: self.config.min_size,
            total_created: self.total_created.load(Ordering::Relaxed),
            total_checkouts: self.total_checkouts.load(Ordering::Relaxed),
            total_checkins: self.total_checkins.load(Ordering::Relaxed),
            checkout_timeouts: self.checkout_timeouts.load(Ordering::Relaxed),
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Pool name
    pub name: String,
    /// Pool mode
    pub mode: PoolMode,
    /// Current size
    pub size: usize,
    /// Available connections
    pub available: usize,
    /// In-use connections
    pub in_use: usize,
    /// Maximum size
    pub max_size: usize,
    /// Minimum size
    pub min_size: usize,
    /// Total connections created
    pub total_created: u64,
    /// Total checkouts
    pub total_checkouts: u64,
    /// Total checkins
    pub total_checkins: u64,
    /// Checkout timeouts
    pub checkout_timeouts: u64,
}

// ============================================================================
// Connection Manager
// ============================================================================

/// Connection manager statistics
#[derive(Debug, Clone, Default)]
pub struct ConnectionManagerStats {
    /// Total connections created
    pub total_connections: u64,
    /// Current active connections
    pub active_connections: usize,
    /// Peak connections
    pub peak_connections: usize,
    /// Connections rejected (limit exceeded)
    pub rejected_connections: u64,
    /// Connections timed out
    pub timed_out_connections: u64,
    /// Connections terminated
    pub terminated_connections: u64,
}

/// Connection manager
pub struct ConnectionManager {
    /// Connection limits
    limits: ConnectionLimits,
    /// Active connections
    connections: RwLock<HashMap<ConnectionId, ConnectionInfo>>,
    /// PID to connection ID mapping
    pid_to_id: RwLock<HashMap<BackendPid, ConnectionId>>,
    /// Connections per user
    user_connections: RwLock<HashMap<String, usize>>,
    /// Connections per database
    database_connections: RwLock<HashMap<String, usize>>,
    /// Connection pools
    pools: RwLock<HashMap<String, Arc<ConnectionPool>>>,
    /// Next connection ID
    next_id: AtomicU64,
    /// Next PID
    next_pid: AtomicU64,
    /// Statistics
    stats: RwLock<ConnectionManagerStats>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new(ConnectionLimits::default())
    }
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(limits: ConnectionLimits) -> Self {
        Self {
            limits,
            connections: RwLock::new(HashMap::new()),
            pid_to_id: RwLock::new(HashMap::new()),
            user_connections: RwLock::new(HashMap::new()),
            database_connections: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            next_pid: AtomicU64::new(1000),
            stats: RwLock::new(ConnectionManagerStats::default()),
        }
    }

    /// Create a connection pool
    pub fn create_pool(&self, config: PoolConfig) -> Arc<ConnectionPool> {
        let pool = Arc::new(ConnectionPool::new(config.clone()));
        self.pools.write().insert(config.name.clone(), Arc::clone(&pool));
        pool
    }

    /// Get a connection pool
    pub fn get_pool(&self, name: &str) -> Option<Arc<ConnectionPool>> {
        self.pools.read().get(name).cloned()
    }

    /// Register a new connection
    pub fn register_connection(
        &self,
        database: &str,
        username: &str,
        client_addr: Option<SocketAddr>,
        is_superuser: bool,
    ) -> Result<ConnectionInfo, ConnectionError> {
        // Check limits
        let connections = self.connections.read();
        let current = connections.len();
        drop(connections);

        // Check total limit (accounting for superuser reserved)
        let effective_max = if is_superuser {
            self.limits.max_connections
        } else {
            self.limits.max_connections.saturating_sub(self.limits.superuser_reserved_connections)
        };

        if current >= effective_max {
            let mut stats = self.stats.write();
            stats.rejected_connections += 1;
            return Err(ConnectionError::TooManyConnections {
                current,
                max: effective_max,
            });
        }

        // Check per-user limit
        if self.limits.max_connections_per_user > 0 {
            let user_conns = self.user_connections.read();
            let user_count = user_conns.get(username).copied().unwrap_or(0);
            if user_count >= self.limits.max_connections_per_user {
                let mut stats = self.stats.write();
                stats.rejected_connections += 1;
                return Err(ConnectionError::TooManyUserConnections {
                    user: username.to_string(),
                    current: user_count,
                    max: self.limits.max_connections_per_user,
                });
            }
        }

        // Check per-database limit
        if self.limits.max_connections_per_database > 0 {
            let db_conns = self.database_connections.read();
            let db_count = db_conns.get(database).copied().unwrap_or(0);
            if db_count >= self.limits.max_connections_per_database {
                let mut stats = self.stats.write();
                stats.rejected_connections += 1;
                return Err(ConnectionError::TooManyDatabaseConnections {
                    database: database.to_string(),
                    current: db_count,
                    max: self.limits.max_connections_per_database,
                });
            }
        }

        // Create connection
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed) as BackendPid;
        let mut info = ConnectionInfo::new(id, pid, database, username, client_addr);
        info.set_state(ConnectionState::Idle);

        // Register
        {
            let mut connections = self.connections.write();
            connections.insert(id, info.clone());
        }
        {
            let mut pid_map = self.pid_to_id.write();
            pid_map.insert(pid, id);
        }
        {
            let mut user_conns = self.user_connections.write();
            *user_conns.entry(username.to_string()).or_insert(0) += 1;
        }
        {
            let mut db_conns = self.database_connections.write();
            *db_conns.entry(database.to_string()).or_insert(0) += 1;
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_connections += 1;
            stats.active_connections = self.connections.read().len();
            if stats.active_connections > stats.peak_connections {
                stats.peak_connections = stats.active_connections;
            }
        }

        Ok(info)
    }

    /// Unregister a connection
    pub fn unregister_connection(&self, id: ConnectionId) -> Option<ConnectionInfo> {
        let info = {
            let mut connections = self.connections.write();
            connections.remove(&id)
        };

        if let Some(ref info) = info {
            // Remove PID mapping
            {
                let mut pid_map = self.pid_to_id.write();
                pid_map.remove(&info.pid);
            }
            // Decrement user count
            {
                let mut user_conns = self.user_connections.write();
                if let Some(count) = user_conns.get_mut(&info.username) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        user_conns.remove(&info.username);
                    }
                }
            }
            // Decrement database count
            {
                let mut db_conns = self.database_connections.write();
                if let Some(count) = db_conns.get_mut(&info.database) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        db_conns.remove(&info.database);
                    }
                }
            }
            // Update stats
            {
                let mut stats = self.stats.write();
                stats.active_connections = self.connections.read().len();
            }
        }

        info
    }

    /// Get connection by ID
    pub fn get_connection(&self, id: ConnectionId) -> Option<ConnectionInfo> {
        self.connections.read().get(&id).cloned()
    }

    /// Get connection by PID
    pub fn get_connection_by_pid(&self, pid: BackendPid) -> Option<ConnectionInfo> {
        let pid_map = self.pid_to_id.read();
        if let Some(id) = pid_map.get(&pid) {
            self.connections.read().get(id).cloned()
        } else {
            None
        }
    }

    /// Get all connections (pg_stat_activity)
    pub fn get_all_connections(&self) -> Vec<ConnectionInfo> {
        self.connections.read().values().cloned().collect()
    }

    /// Update connection info
    pub fn update_connection<F>(&self, id: ConnectionId, f: F) -> bool
    where
        F: FnOnce(&mut ConnectionInfo),
    {
        let mut connections = self.connections.write();
        if let Some(info) = connections.get_mut(&id) {
            f(info);
            true
        } else {
            false
        }
    }

    /// Terminate a connection (pg_terminate_backend)
    pub fn terminate_backend(&self, pid: BackendPid) -> bool {
        let id = {
            let pid_map = self.pid_to_id.read();
            pid_map.get(&pid).copied()
        };

        if let Some(id) = id {
            // Mark as closing
            self.update_connection(id, |info| {
                info.set_state(ConnectionState::Closing);
            });

            // Actually terminate
            if self.unregister_connection(id).is_some() {
                let mut stats = self.stats.write();
                stats.terminated_connections += 1;
                return true;
            }
        }

        false
    }

    /// Cancel a query (pg_cancel_backend)
    pub fn cancel_backend(&self, pid: BackendPid) -> bool {
        let id = {
            let pid_map = self.pid_to_id.read();
            pid_map.get(&pid).copied()
        };

        if let Some(id) = id {
            return self.update_connection(id, |info| {
                info.end_query();
            });
        }

        false
    }

    /// Terminate idle connections exceeding timeout
    pub fn cleanup_idle_connections(&self) -> usize {
        if self.limits.idle_session_timeout_ms == 0
            && self.limits.idle_in_transaction_timeout_ms == 0
        {
            return 0;
        }

        let connections_to_terminate: Vec<ConnectionId> = {
            let connections = self.connections.read();
            connections
                .iter()
                .filter(|(_, info)| {
                    let idle_ms = info.idle_time_ms();

                    // Check idle in transaction timeout
                    if self.limits.idle_in_transaction_timeout_ms > 0
                        && matches!(
                            info.state,
                            ConnectionState::IdleInTransaction
                                | ConnectionState::IdleInTransactionAborted
                        )
                        && idle_ms > self.limits.idle_in_transaction_timeout_ms
                    {
                        return true;
                    }

                    // Check idle session timeout
                    if self.limits.idle_session_timeout_ms > 0
                        && info.state == ConnectionState::Idle
                        && idle_ms > self.limits.idle_session_timeout_ms
                    {
                        return true;
                    }

                    false
                })
                .map(|(id, _)| *id)
                .collect()
        };

        let count = connections_to_terminate.len();
        for id in connections_to_terminate {
            if let Some(info) = self.unregister_connection(id) {
                let mut stats = self.stats.write();
                stats.timed_out_connections += 1;
            }
        }

        count
    }

    /// Get connection manager statistics
    pub fn stats(&self) -> ConnectionManagerStats {
        self.stats.read().clone()
    }

    /// Get connection count
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }

    /// Get connections per user
    pub fn connections_by_user(&self) -> HashMap<String, usize> {
        self.user_connections.read().clone()
    }

    /// Get connections per database
    pub fn connections_by_database(&self) -> HashMap<String, usize> {
        self.database_connections.read().clone()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_info() {
        let mut info = ConnectionInfo::new(1, 1000, "testdb", "testuser", None);

        assert_eq!(info.state, ConnectionState::Connecting);
        info.set_state(ConnectionState::Idle);
        assert_eq!(info.state, ConnectionState::Idle);

        info.start_query("SELECT 1");
        assert_eq!(info.state, ConnectionState::Active);
        assert!(info.query.is_some());

        info.end_query();
        assert_eq!(info.state, ConnectionState::Idle);
        assert!(info.query.is_none());
    }

    #[test]
    fn test_connection_manager() {
        let manager = ConnectionManager::new(ConnectionLimits {
            max_connections: 10,
            ..Default::default()
        });

        // Register connections
        let conn1 = manager
            .register_connection("db1", "user1", None, false)
            .unwrap();
        let conn2 = manager
            .register_connection("db1", "user2", None, false)
            .unwrap();

        assert_eq!(manager.connection_count(), 2);
        assert!(conn1.id != conn2.id);
        assert!(conn1.pid != conn2.pid);

        // Get connection
        let info = manager.get_connection(conn1.id).unwrap();
        assert_eq!(info.database, "db1");

        // Unregister
        manager.unregister_connection(conn1.id);
        assert_eq!(manager.connection_count(), 1);
    }

    #[test]
    fn test_connection_limits() {
        let manager = ConnectionManager::new(ConnectionLimits {
            max_connections: 5,
            superuser_reserved_connections: 3,
            ..Default::default()
        });

        manager
            .register_connection("db", "user", None, false)
            .unwrap();
        manager
            .register_connection("db", "user", None, false)
            .unwrap();

        // Third should fail
        let result = manager.register_connection("db", "user", None, false);
        assert!(matches!(result, Err(ConnectionError::TooManyConnections { .. })));
    }

    #[test]
    fn test_per_user_limit() {
        let manager = ConnectionManager::new(ConnectionLimits {
            max_connections: 100,
            max_connections_per_user: 2,
            ..Default::default()
        });

        manager
            .register_connection("db", "user1", None, false)
            .unwrap();
        manager
            .register_connection("db", "user1", None, false)
            .unwrap();

        // Third for same user should fail
        let result = manager.register_connection("db", "user1", None, false);
        assert!(matches!(
            result,
            Err(ConnectionError::TooManyUserConnections { .. })
        ));

        // Different user should work
        manager
            .register_connection("db", "user2", None, false)
            .unwrap();
    }

    #[test]
    fn test_terminate_backend() {
        let manager = ConnectionManager::new(ConnectionLimits::default());

        let conn = manager
            .register_connection("db", "user", None, false)
            .unwrap();
        assert_eq!(manager.connection_count(), 1);

        let terminated = manager.terminate_backend(conn.pid);
        assert!(terminated);
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_connection_pool() {
        let pool = ConnectionPool::new(PoolConfig {
            name: "test_pool".to_string(),
            max_size: 5,
            ..Default::default()
        });

        // Checkout
        let conn1 = pool.checkout().unwrap();
        assert_eq!(pool.in_use_count(), 1);

        let conn2 = pool.checkout().unwrap();
        assert_eq!(pool.in_use_count(), 2);

        // Checkin
        pool.checkin(conn1);
        assert_eq!(pool.in_use_count(), 1);
        assert_eq!(pool.available_count(), 1);

        pool.checkin(conn2);
        assert_eq!(pool.in_use_count(), 0);
        assert_eq!(pool.available_count(), 2);

        // Checkout reuses
        let conn3 = pool.checkout().unwrap();
        assert_eq!(pool.available_count(), 1);
    }

    #[test]
    fn test_pool_exhausted() {
        let pool = ConnectionPool::new(PoolConfig {
            name: "small_pool".to_string(),
            max_size: 2,
            ..Default::default()
        });

        let _conn1 = pool.checkout().unwrap();
        let _conn2 = pool.checkout().unwrap();

        // Third should fail
        let result = pool.checkout();
        assert!(matches!(result, Err(ConnectionError::PoolExhausted { .. })));
    }

    #[test]
    fn test_pool_stats() {
        let pool = ConnectionPool::new(PoolConfig::default());

        let conn = pool.checkout().unwrap();
        pool.checkin(conn);

        let stats = pool.stats();
        assert_eq!(stats.total_checkouts, 1);
        assert_eq!(stats.total_checkins, 1);
        assert_eq!(stats.total_created, 1);
    }

    #[test]
    fn test_connection_state_display() {
        assert_eq!(format!("{}", ConnectionState::Idle), "idle");
        assert_eq!(format!("{}", ConnectionState::Active), "active");
        assert_eq!(
            format!("{}", ConnectionState::IdleInTransaction),
            "idle in transaction"
        );
    }

    #[test]
    fn test_connections_by_database() {
        let manager = ConnectionManager::new(ConnectionLimits::default());

        manager
            .register_connection("db1", "user", None, false)
            .unwrap();
        manager
            .register_connection("db1", "user", None, false)
            .unwrap();
        manager
            .register_connection("db2", "user", None, false)
            .unwrap();

        let by_db = manager.connections_by_database();
        assert_eq!(by_db.get("db1"), Some(&2));
        assert_eq!(by_db.get("db2"), Some(&1));
    }
}
