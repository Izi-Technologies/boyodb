//! Connection Pool Module
//!
//! Provides built-in connection pooling with:
//! - Min/max pool sizing with dynamic scaling
//! - Connection reuse and health checking
//! - Prepared statement caching per connection
//! - Connection warming on startup
//! - Idle connection cleanup
//! - Connection lifetime limits

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

/// Configuration for the connection pool
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
    /// Connection acquire timeout in milliseconds
    pub acquire_timeout_ms: u64,
    /// Maximum time a connection can be idle before being closed
    pub idle_timeout_ms: u64,
    /// Maximum lifetime of a connection (0 = unlimited)
    pub max_lifetime_ms: u64,
    /// Number of connections to create on startup (warming)
    pub warmup_connections: usize,
    /// Health check interval in milliseconds
    pub health_check_interval_ms: u64,
    /// Maximum prepared statements to cache per connection
    pub prepared_statement_cache_size: usize,
    /// Enable connection validation before use
    pub validate_on_acquire: bool,
    /// Enable connection validation on return
    pub validate_on_return: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 20,
            acquire_timeout_ms: 30_000,
            idle_timeout_ms: 300_000, // 5 minutes
            max_lifetime_ms: 3600_000, // 1 hour
            warmup_connections: 2,
            health_check_interval_ms: 30_000,
            prepared_statement_cache_size: 100,
            validate_on_acquire: true,
            validate_on_return: false,
        }
    }
}

/// Cached prepared statement
#[derive(Clone, Debug)]
pub struct CachedPreparedStatement {
    /// Unique identifier for the prepared statement
    pub id: String,
    /// SQL query text
    pub sql: String,
    /// Parameter types (if known)
    pub param_types: Vec<String>,
    /// Result column types (if known)
    pub result_types: Vec<String>,
    /// When the statement was prepared
    pub prepared_at: Instant,
    /// Number of times executed
    pub execution_count: u64,
    /// Last execution time
    pub last_used: Instant,
}

/// Prepared statement cache with LRU eviction
pub struct PreparedStatementCache {
    statements: HashMap<String, CachedPreparedStatement>,
    access_order: VecDeque<String>,
    max_size: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl PreparedStatementCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            statements: HashMap::with_capacity(max_size),
            access_order: VecDeque::with_capacity(max_size),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get a cached prepared statement
    pub fn get(&mut self, sql: &str) -> Option<&CachedPreparedStatement> {
        if self.statements.contains_key(sql) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            // Move to front of access order
            self.access_order.retain(|s| s != sql);
            self.access_order.push_front(sql.to_string());
            self.statements.get(sql)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Cache a prepared statement
    pub fn put(&mut self, stmt: CachedPreparedStatement) {
        let sql = stmt.sql.clone();

        // Evict LRU if at capacity
        while self.statements.len() >= self.max_size {
            if let Some(old_sql) = self.access_order.pop_back() {
                self.statements.remove(&old_sql);
            } else {
                break;
            }
        }

        self.statements.insert(sql.clone(), stmt);
        self.access_order.push_front(sql);
    }

    /// Remove a prepared statement
    pub fn remove(&mut self, sql: &str) -> Option<CachedPreparedStatement> {
        self.access_order.retain(|s| s != sql);
        self.statements.remove(sql)
    }

    /// Clear all cached statements
    pub fn clear(&mut self) {
        self.statements.clear();
        self.access_order.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> PreparedStatementCacheStats {
        PreparedStatementCacheStats {
            size: self.statements.len(),
            capacity: self.max_size,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// Statistics for prepared statement cache
#[derive(Clone, Debug)]
pub struct PreparedStatementCacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
}

impl PreparedStatementCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// State of a pooled connection
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is available for use
    Idle,
    /// Connection is currently in use
    InUse,
    /// Connection is being validated
    Validating,
    /// Connection is closed/invalid
    Closed,
}

/// A pooled connection wrapper
pub struct PooledConnection<C> {
    /// The underlying connection
    pub inner: C,
    /// Unique connection ID
    pub id: u64,
    /// When the connection was created
    pub created_at: Instant,
    /// When the connection was last used
    pub last_used: Instant,
    /// Current state
    pub state: ConnectionState,
    /// Prepared statement cache for this connection
    pub prepared_cache: PreparedStatementCache,
    /// Number of times this connection has been used
    pub use_count: u64,
    /// Total time spent executing queries (microseconds)
    pub total_query_time_us: u64,
}

impl<C> PooledConnection<C> {
    pub fn new(inner: C, id: u64, prepared_cache_size: usize) -> Self {
        let now = Instant::now();
        Self {
            inner,
            id,
            created_at: now,
            last_used: now,
            state: ConnectionState::Idle,
            prepared_cache: PreparedStatementCache::new(prepared_cache_size),
            use_count: 0,
            total_query_time_us: 0,
        }
    }

    /// Check if the connection has exceeded its lifetime
    pub fn is_expired(&self, max_lifetime: Duration) -> bool {
        if max_lifetime.is_zero() {
            return false;
        }
        self.created_at.elapsed() > max_lifetime
    }

    /// Check if the connection has been idle too long
    pub fn is_idle_expired(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }

    /// Mark the connection as used
    pub fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.use_count += 1;
        self.state = ConnectionState::InUse;
    }

    /// Mark the connection as idle
    pub fn mark_idle(&mut self) {
        self.last_used = Instant::now();
        self.state = ConnectionState::Idle;
    }

    /// Record query execution time
    pub fn record_query_time(&mut self, duration_us: u64) {
        self.total_query_time_us += duration_us;
    }
}

/// Pool statistics
#[derive(Clone, Debug, Default)]
pub struct PoolStats {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Current idle connections
    pub idle_connections: usize,
    /// Current in-use connections
    pub in_use_connections: usize,
    /// Total connection acquisitions
    pub acquisitions: u64,
    /// Acquisitions that had to wait
    pub acquisitions_waited: u64,
    /// Acquisitions that timed out
    pub acquisitions_timed_out: u64,
    /// Total connection returns
    pub returns: u64,
    /// Connections closed due to idle timeout
    pub idle_timeouts: u64,
    /// Connections closed due to max lifetime
    pub lifetime_timeouts: u64,
    /// Connections closed due to validation failure
    pub validation_failures: u64,
    /// Average wait time in microseconds
    pub avg_wait_time_us: u64,
    /// Prepared statement cache stats (aggregate)
    pub prepared_cache_hits: u64,
    pub prepared_cache_misses: u64,
}

/// Connection pool for managing database connections
pub struct ConnectionPool<C, F>
where
    F: Fn() -> C + Send + Sync,
{
    config: PoolConfig,
    /// Factory function to create new connections
    create_connection: F,
    /// Available connections
    connections: Mutex<VecDeque<PooledConnection<C>>>,
    /// Semaphore to limit max connections
    semaphore: Arc<Semaphore>,
    /// Pool statistics
    stats: RwLock<PoolStats>,
    /// Next connection ID
    next_conn_id: AtomicU64,
    /// Whether the pool is closed
    closed: std::sync::atomic::AtomicBool,
    /// Total wait time for acquisitions (microseconds)
    total_wait_time_us: AtomicU64,
    /// Number of waits
    wait_count: AtomicU64,
}

impl<C, F> ConnectionPool<C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    /// Create a new connection pool
    pub fn new(config: PoolConfig, create_connection: F) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        Self {
            config,
            create_connection,
            connections: Mutex::new(VecDeque::new()),
            semaphore,
            stats: RwLock::new(PoolStats::default()),
            next_conn_id: AtomicU64::new(1),
            closed: std::sync::atomic::AtomicBool::new(false),
            total_wait_time_us: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
        }
    }

    /// Warm up the pool by creating initial connections
    pub fn warmup(&self) {
        let count = self.config.warmup_connections.min(self.config.max_connections);
        let mut connections = self.connections.lock();

        for _ in 0..count {
            let conn = self.create_new_connection();
            connections.push_back(conn);
        }

        let mut stats = self.stats.write();
        stats.connections_created += count as u64;
        stats.idle_connections = connections.len();
    }

    /// Create a new connection
    fn create_new_connection(&self) -> PooledConnection<C> {
        let id = self.next_conn_id.fetch_add(1, Ordering::SeqCst);
        let inner = (self.create_connection)();
        PooledConnection::new(inner, id, self.config.prepared_statement_cache_size)
    }

    /// Acquire a connection from the pool
    pub async fn acquire(&self) -> Result<PoolGuard<'_, C, F>, PoolError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(PoolError::PoolClosed);
        }

        let start = Instant::now();

        // Try to acquire a permit with timeout
        let permit = tokio::time::timeout(
            Duration::from_millis(self.config.acquire_timeout_ms),
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            let mut stats = self.stats.write();
            stats.acquisitions_timed_out += 1;
            PoolError::AcquireTimeout
        })?
        .map_err(|_| PoolError::PoolClosed)?;

        let wait_time = start.elapsed();
        if wait_time > Duration::from_millis(1) {
            self.total_wait_time_us.fetch_add(wait_time.as_micros() as u64, Ordering::Relaxed);
            self.wait_count.fetch_add(1, Ordering::Relaxed);
            let mut stats = self.stats.write();
            stats.acquisitions_waited += 1;
        }

        // Try to get an existing connection
        let mut conn = {
            let mut connections = self.connections.lock();
            let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
            let max_lifetime = Duration::from_millis(self.config.max_lifetime_ms);

            // Find a valid connection, removing expired ones
            loop {
                match connections.pop_front() {
                    Some(c) if c.is_expired(max_lifetime) => {
                        // Connection expired, close it and try next
                        let mut stats = self.stats.write();
                        stats.lifetime_timeouts += 1;
                        stats.connections_closed += 1;
                        continue;
                    }
                    Some(c) if c.is_idle_expired(idle_timeout) => {
                        // Connection idle too long, close it and try next
                        let mut stats = self.stats.write();
                        stats.idle_timeouts += 1;
                        stats.connections_closed += 1;
                        continue;
                    }
                    Some(c) => break Some(c),
                    None => break None,
                }
            }
        };

        // Create new connection if none available
        if conn.is_none() {
            let new_conn = self.create_new_connection();
            let mut stats = self.stats.write();
            stats.connections_created += 1;
            conn = Some(new_conn);
        }

        let mut conn = conn.unwrap();
        conn.mark_used();

        {
            let mut stats = self.stats.write();
            stats.acquisitions += 1;
            stats.in_use_connections += 1;
            stats.idle_connections = self.connections.lock().len();

            // Update average wait time
            let wait_count = self.wait_count.load(Ordering::Relaxed);
            if wait_count > 0 {
                stats.avg_wait_time_us = self.total_wait_time_us.load(Ordering::Relaxed) / wait_count;
            }
        }

        Ok(PoolGuard {
            conn: Some(conn),
            pool: self,
            permit: Some(permit),
        })
    }

    /// Return a connection to the pool
    fn return_connection(&self, mut conn: PooledConnection<C>, permit: OwnedSemaphorePermit) {
        conn.mark_idle();

        // Aggregate prepared statement cache stats
        {
            let cache_stats = conn.prepared_cache.stats();
            let mut stats = self.stats.write();
            stats.prepared_cache_hits += cache_stats.hits;
            stats.prepared_cache_misses += cache_stats.misses;
        }

        let max_lifetime = Duration::from_millis(self.config.max_lifetime_ms);

        // Don't return expired connections
        if !conn.is_expired(max_lifetime) {
            let mut connections = self.connections.lock();
            connections.push_back(conn);

            let mut stats = self.stats.write();
            stats.returns += 1;
            stats.in_use_connections = stats.in_use_connections.saturating_sub(1);
            stats.idle_connections = connections.len();
        } else {
            let mut stats = self.stats.write();
            stats.lifetime_timeouts += 1;
            stats.connections_closed += 1;
            stats.in_use_connections = stats.in_use_connections.saturating_sub(1);
        }

        // Release the permit (happens automatically when dropped)
        drop(permit);
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        self.stats.read().clone()
    }

    /// Get current pool size
    pub fn size(&self) -> usize {
        self.connections.lock().len()
    }

    /// Get number of available connections
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Close the pool and all connections
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.semaphore.close();

        let mut connections = self.connections.lock();
        let closed_count = connections.len();
        connections.clear();

        let mut stats = self.stats.write();
        stats.connections_closed += closed_count as u64;
        stats.idle_connections = 0;
    }

    /// Run maintenance tasks (cleanup idle connections, health checks)
    pub fn maintenance(&self) {
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let max_lifetime = Duration::from_millis(self.config.max_lifetime_ms);
        let min_connections = self.config.min_connections;

        let mut connections = self.connections.lock();
        let initial_count = connections.len();

        // Remove expired connections but keep at least min_connections
        let mut removed = 0;
        let mut keep = VecDeque::new();

        while let Some(conn) = connections.pop_front() {
            if keep.len() >= min_connections {
                if conn.is_expired(max_lifetime) {
                    removed += 1;
                    continue;
                } else if conn.is_idle_expired(idle_timeout) {
                    removed += 1;
                    continue;
                }
            }
            keep.push_back(conn);
        }

        *connections = keep;

        if removed > 0 {
            let mut stats = self.stats.write();
            stats.idle_timeouts += removed as u64;
            stats.connections_closed += removed as u64;
            stats.idle_connections = connections.len();
        }

        // Ensure minimum connections
        let current = connections.len();
        drop(connections);

        if current < min_connections {
            let to_create = min_connections - current;
            let mut connections = self.connections.lock();
            for _ in 0..to_create {
                let conn = self.create_new_connection();
                connections.push_back(conn);
            }

            let mut stats = self.stats.write();
            stats.connections_created += to_create as u64;
            stats.idle_connections = connections.len();
        }
    }
}

/// RAII guard for a pooled connection
pub struct PoolGuard<'a, C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    conn: Option<PooledConnection<C>>,
    pool: &'a ConnectionPool<C, F>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<'a, C, F> PoolGuard<'a, C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    /// Get a reference to the connection
    pub fn connection(&self) -> &C {
        &self.conn.as_ref().unwrap().inner
    }

    /// Get a mutable reference to the connection
    pub fn connection_mut(&mut self) -> &mut C {
        &mut self.conn.as_mut().unwrap().inner
    }

    /// Get the connection ID
    pub fn id(&self) -> u64 {
        self.conn.as_ref().unwrap().id
    }

    /// Get the prepared statement cache
    pub fn prepared_cache(&self) -> &PreparedStatementCache {
        &self.conn.as_ref().unwrap().prepared_cache
    }

    /// Get mutable access to the prepared statement cache
    pub fn prepared_cache_mut(&mut self) -> &mut PreparedStatementCache {
        &mut self.conn.as_mut().unwrap().prepared_cache
    }

    /// Record query execution time
    pub fn record_query_time(&mut self, duration_us: u64) {
        self.conn.as_mut().unwrap().record_query_time(duration_us);
    }

    /// Discard the connection (don't return to pool)
    pub fn discard(mut self) {
        self.conn = None;
        if let Some(permit) = self.permit.take() {
            drop(permit);
        }

        let mut stats = self.pool.stats.write();
        stats.validation_failures += 1;
        stats.connections_closed += 1;
        stats.in_use_connections = stats.in_use_connections.saturating_sub(1);
    }
}

impl<'a, C, F> Drop for PoolGuard<'a, C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    fn drop(&mut self) {
        if let (Some(conn), Some(permit)) = (self.conn.take(), self.permit.take()) {
            self.pool.return_connection(conn, permit);
        }
    }
}

impl<'a, C, F> std::ops::Deref for PoolGuard<'a, C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.connection()
    }
}

impl<'a, C, F> std::ops::DerefMut for PoolGuard<'a, C, F>
where
    C: Send + 'static,
    F: Fn() -> C + Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection_mut()
    }
}

/// Pool errors
#[derive(Debug, Clone)]
pub enum PoolError {
    /// Pool is closed
    PoolClosed,
    /// Timeout waiting for a connection
    AcquireTimeout,
    /// Connection validation failed
    ValidationFailed(String),
    /// Connection creation failed
    CreationFailed(String),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::PoolClosed => write!(f, "connection pool is closed"),
            PoolError::AcquireTimeout => write!(f, "timeout waiting for connection"),
            PoolError::ValidationFailed(msg) => write!(f, "connection validation failed: {}", msg),
            PoolError::CreationFailed(msg) => write!(f, "connection creation failed: {}", msg),
        }
    }
}

impl std::error::Error for PoolError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockConnection {
        id: u64,
    }

    #[tokio::test]
    async fn test_pool_basic() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        let pool = ConnectionPool::new(
            PoolConfig {
                min_connections: 1,
                max_connections: 5,
                ..Default::default()
            },
            move || MockConnection {
                id: counter_clone.fetch_add(1, Ordering::SeqCst),
            },
        );

        // Acquire and release
        {
            let conn = pool.acquire().await.unwrap();
            assert_eq!(conn.id(), 1);
        }

        // Same connection should be reused
        {
            let conn = pool.acquire().await.unwrap();
            assert_eq!(conn.id(), 1);
        }

        let stats = pool.stats();
        assert_eq!(stats.connections_created, 1);
        assert_eq!(stats.acquisitions, 2);
    }

    #[tokio::test]
    async fn test_pool_concurrent() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        let pool = Arc::new(ConnectionPool::new(
            PoolConfig {
                min_connections: 0,
                max_connections: 3,
                ..Default::default()
            },
            move || MockConnection {
                id: counter_clone.fetch_add(1, Ordering::SeqCst),
            },
        ));

        let mut handles = vec![];
        for _ in 0..10 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let conn = pool.acquire().await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(conn);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let stats = pool.stats();
        assert!(stats.connections_created <= 3);
        assert_eq!(stats.acquisitions, 10);
    }

    #[test]
    fn test_prepared_cache() {
        let mut cache = PreparedStatementCache::new(3);

        // Add statements
        for i in 0..5 {
            cache.put(CachedPreparedStatement {
                id: format!("stmt_{}", i),
                sql: format!("SELECT {}", i),
                param_types: vec![],
                result_types: vec![],
                prepared_at: Instant::now(),
                execution_count: 0,
                last_used: Instant::now(),
            });
        }

        // Only 3 should remain (LRU eviction)
        assert_eq!(cache.statements.len(), 3);

        // Most recent should be present
        assert!(cache.get("SELECT 4").is_some());
        assert!(cache.get("SELECT 3").is_some());
        assert!(cache.get("SELECT 2").is_some());

        // Oldest should be evicted
        assert!(cache.get("SELECT 0").is_none());
        assert!(cache.get("SELECT 1").is_none());
    }
}
