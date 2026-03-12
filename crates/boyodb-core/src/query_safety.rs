//! Query Safety Module
//!
//! PostgreSQL-compatible query timeout and cancellation features.
//!
//! # Features
//! - Statement timeouts (SET statement_timeout)
//! - Lock timeouts (SET lock_timeout)
//! - Idle in transaction timeouts
//! - Query cancellation (pg_cancel_backend)
//! - Query termination (pg_terminate_backend)
//! - Long-running query detection
//!
//! # Example
//! ```sql
//! -- Set statement timeout (5 seconds)
//! SET statement_timeout = '5s';
//!
//! -- Set lock timeout (10 seconds)
//! SET lock_timeout = '10s';
//!
//! -- Cancel a running query
//! SELECT pg_cancel_backend(12345);
//!
//! -- Terminate a connection
//! SELECT pg_terminate_backend(12345);
//!
//! -- Find long-running queries
//! SELECT pid, query, now() - query_start AS duration
//! FROM pg_stat_activity
//! WHERE state = 'active' AND query_start < now() - interval '5 minutes';
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Types and Errors
// ============================================================================

/// Query ID type
pub type QueryId = u64;

/// Backend PID type
pub type BackendPid = u32;

/// Errors from query safety operations
#[derive(Debug, Clone)]
pub enum QuerySafetyError {
    /// Query timeout
    StatementTimeout {
        query_id: QueryId,
        timeout_ms: u64,
        elapsed_ms: u64,
    },
    /// Lock timeout
    LockTimeout {
        query_id: QueryId,
        timeout_ms: u64,
        lock_type: String,
    },
    /// Query cancelled by user
    QueryCancelled { query_id: QueryId, reason: String },
    /// Backend terminated
    BackendTerminated { pid: BackendPid, reason: String },
    /// Query not found
    QueryNotFound(QueryId),
    /// Backend not found
    BackendNotFound(BackendPid),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for QuerySafetyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StatementTimeout {
                query_id,
                timeout_ms,
                elapsed_ms,
            } => {
                write!(
                    f,
                    "canceling statement due to statement timeout (query {}, {}ms elapsed, {}ms limit)",
                    query_id, elapsed_ms, timeout_ms
                )
            }
            Self::LockTimeout {
                query_id,
                timeout_ms,
                lock_type,
            } => {
                write!(
                    f,
                    "canceling statement due to lock timeout ({} lock, {}ms)",
                    lock_type, timeout_ms
                )
            }
            Self::QueryCancelled { query_id, reason } => {
                write!(f, "canceling statement due to user request: {}", reason)
            }
            Self::BackendTerminated { pid, reason } => {
                write!(f, "terminating connection due to administrator command: {}", reason)
            }
            Self::QueryNotFound(id) => write!(f, "query {} not found", id),
            Self::BackendNotFound(pid) => write!(f, "backend {} not found", pid),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for QuerySafetyError {}

/// PostgreSQL error codes for timeouts
pub mod sqlstate {
    pub const QUERY_CANCELED: &str = "57014";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const STATEMENT_TIMEOUT: &str = "57014";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const IDLE_IN_TRANSACTION_TIMEOUT: &str = "25P03";
}

// ============================================================================
// Timeout Configuration
// ============================================================================

/// Timeout configuration for a session
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Statement timeout in milliseconds (0 = disabled)
    pub statement_timeout_ms: u64,
    /// Lock timeout in milliseconds (0 = disabled)
    pub lock_timeout_ms: u64,
    /// Idle in transaction timeout in milliseconds (0 = disabled)
    pub idle_in_transaction_timeout_ms: u64,
    /// Lock wait timeout in milliseconds (0 = disabled)
    pub lock_wait_timeout_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            statement_timeout_ms: 0,
            lock_timeout_ms: 0,
            idle_in_transaction_timeout_ms: 0,
            lock_wait_timeout_ms: 0,
        }
    }
}

impl TimeoutConfig {
    /// Create with statement timeout
    pub fn with_statement_timeout(mut self, timeout_ms: u64) -> Self {
        self.statement_timeout_ms = timeout_ms;
        self
    }

    /// Create with lock timeout
    pub fn with_lock_timeout(mut self, timeout_ms: u64) -> Self {
        self.lock_timeout_ms = timeout_ms;
        self
    }

    /// Parse timeout from PostgreSQL format (e.g., "5s", "10min", "1h")
    pub fn parse_timeout(value: &str) -> Option<u64> {
        let value = value.trim().to_lowercase();

        // Handle "0" or "off"
        if value == "0" || value == "off" {
            return Some(0);
        }

        // Try to parse with unit suffix
        let (num_str, multiplier) = if let Some(num) = value.strip_suffix("ms") {
            (num, 1u64)
        } else if let Some(num) = value.strip_suffix("s") {
            (num, 1000)
        } else if let Some(num) = value.strip_suffix("min") {
            (num, 60 * 1000)
        } else if let Some(num) = value.strip_suffix("h") {
            (num, 60 * 60 * 1000)
        } else if let Some(num) = value.strip_suffix("d") {
            (num, 24 * 60 * 60 * 1000)
        } else {
            // Assume milliseconds
            (value.as_str(), 1)
        };

        num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
    }

    /// Format timeout as PostgreSQL string
    pub fn format_timeout(timeout_ms: u64) -> String {
        if timeout_ms == 0 {
            "0".to_string()
        } else if timeout_ms % (24 * 60 * 60 * 1000) == 0 {
            format!("{}d", timeout_ms / (24 * 60 * 60 * 1000))
        } else if timeout_ms % (60 * 60 * 1000) == 0 {
            format!("{}h", timeout_ms / (60 * 60 * 1000))
        } else if timeout_ms % (60 * 1000) == 0 {
            format!("{}min", timeout_ms / (60 * 1000))
        } else if timeout_ms % 1000 == 0 {
            format!("{}s", timeout_ms / 1000)
        } else {
            format!("{}ms", timeout_ms)
        }
    }
}

// ============================================================================
// Query Context
// ============================================================================

/// State of a running query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Query is running
    Running,
    /// Query is waiting for lock
    WaitingLock,
    /// Query is cancelled
    Cancelled,
    /// Query completed successfully
    Completed,
    /// Query failed with error
    Failed,
}

/// Running query context
#[derive(Debug)]
pub struct QueryContext {
    /// Query ID
    pub query_id: QueryId,
    /// Backend PID
    pub backend_pid: BackendPid,
    /// SQL text
    pub sql: String,
    /// Start time
    pub start_time: Instant,
    /// System start time
    pub start_system_time: SystemTime,
    /// Current state
    pub state: QueryState,
    /// Timeout configuration
    pub timeout_config: TimeoutConfig,
    /// Cancel flag
    cancel_requested: Arc<AtomicBool>,
    /// Lock wait start time (if waiting)
    pub lock_wait_start: Option<Instant>,
    /// Error message (if failed)
    pub error: Option<String>,
}

impl QueryContext {
    /// Create a new query context
    pub fn new(
        query_id: QueryId,
        backend_pid: BackendPid,
        sql: &str,
        timeout_config: TimeoutConfig,
    ) -> Self {
        Self {
            query_id,
            backend_pid,
            sql: sql.to_string(),
            start_time: Instant::now(),
            start_system_time: SystemTime::now(),
            state: QueryState::Running,
            timeout_config,
            cancel_requested: Arc::new(AtomicBool::new(false)),
            lock_wait_start: None,
            error: None,
        }
    }

    /// Get elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Check if statement timeout exceeded
    pub fn check_statement_timeout(&self) -> Option<QuerySafetyError> {
        let timeout = self.timeout_config.statement_timeout_ms;
        if timeout > 0 {
            let elapsed = self.elapsed_ms();
            if elapsed >= timeout {
                return Some(QuerySafetyError::StatementTimeout {
                    query_id: self.query_id,
                    timeout_ms: timeout,
                    elapsed_ms: elapsed,
                });
            }
        }
        None
    }

    /// Check if lock timeout exceeded
    pub fn check_lock_timeout(&self) -> Option<QuerySafetyError> {
        if let Some(lock_start) = self.lock_wait_start {
            let timeout = self.timeout_config.lock_timeout_ms;
            if timeout > 0 {
                let elapsed = lock_start.elapsed().as_millis() as u64;
                if elapsed >= timeout {
                    return Some(QuerySafetyError::LockTimeout {
                        query_id: self.query_id,
                        timeout_ms: timeout,
                        lock_type: "unknown".to_string(),
                    });
                }
            }
        }
        None
    }

    /// Check if cancellation was requested
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::SeqCst)
    }

    /// Request cancellation
    pub fn request_cancel(&self) {
        self.cancel_requested.store(true, Ordering::SeqCst);
    }

    /// Get cancel flag for sharing
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel_requested)
    }

    /// Start waiting for lock
    pub fn start_lock_wait(&mut self) {
        self.state = QueryState::WaitingLock;
        self.lock_wait_start = Some(Instant::now());
    }

    /// End lock wait
    pub fn end_lock_wait(&mut self) {
        self.state = QueryState::Running;
        self.lock_wait_start = None;
    }

    /// Mark as cancelled
    pub fn mark_cancelled(&mut self, reason: &str) {
        self.state = QueryState::Cancelled;
        self.error = Some(reason.to_string());
    }

    /// Mark as completed
    pub fn mark_completed(&mut self) {
        self.state = QueryState::Completed;
    }

    /// Mark as failed
    pub fn mark_failed(&mut self, error: &str) {
        self.state = QueryState::Failed;
        self.error = Some(error.to_string());
    }

    /// Check all timeouts and cancellation
    pub fn check(&self) -> Result<(), QuerySafetyError> {
        // Check cancellation first
        if self.is_cancel_requested() {
            return Err(QuerySafetyError::QueryCancelled {
                query_id: self.query_id,
                reason: "user request".to_string(),
            });
        }

        // Check statement timeout
        if let Some(err) = self.check_statement_timeout() {
            return Err(err);
        }

        // Check lock timeout
        if let Some(err) = self.check_lock_timeout() {
            return Err(err);
        }

        Ok(())
    }
}

// ============================================================================
// Query Manager
// ============================================================================

/// Query manager statistics
#[derive(Debug, Clone, Default)]
pub struct QueryManagerStats {
    /// Total queries started
    pub total_queries: u64,
    /// Queries completed successfully
    pub completed_queries: u64,
    /// Queries cancelled
    pub cancelled_queries: u64,
    /// Queries timed out
    pub timed_out_queries: u64,
    /// Queries failed
    pub failed_queries: u64,
    /// Currently running queries
    pub running_queries: usize,
    /// Queries waiting for locks
    pub waiting_queries: usize,
}

/// Query manager for tracking and managing running queries
pub struct QueryManager {
    /// Running queries
    queries: RwLock<HashMap<QueryId, QueryContext>>,
    /// PID to query ID mapping (for pg_cancel_backend)
    pid_queries: RwLock<HashMap<BackendPid, Vec<QueryId>>>,
    /// Next query ID
    next_query_id: AtomicU64,
    /// Default timeout configuration
    default_timeout: RwLock<TimeoutConfig>,
    /// Statistics
    stats: RwLock<QueryManagerStats>,
    /// Long query threshold (ms) for alerting
    long_query_threshold_ms: AtomicU64,
}

impl Default for QueryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryManager {
    /// Create a new query manager
    pub fn new() -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            pid_queries: RwLock::new(HashMap::new()),
            next_query_id: AtomicU64::new(1),
            default_timeout: RwLock::new(TimeoutConfig::default()),
            stats: RwLock::new(QueryManagerStats::default()),
            long_query_threshold_ms: AtomicU64::new(60000), // 1 minute default
        }
    }

    /// Set default timeout configuration
    pub fn set_default_timeout(&self, config: TimeoutConfig) {
        *self.default_timeout.write().unwrap() = config;
    }

    /// Get default timeout configuration
    pub fn get_default_timeout(&self) -> TimeoutConfig {
        self.default_timeout.read().unwrap().clone()
    }

    /// Set long query threshold
    pub fn set_long_query_threshold(&self, threshold_ms: u64) {
        self.long_query_threshold_ms.store(threshold_ms, Ordering::Relaxed);
    }

    /// Register a new query
    pub fn register_query(
        &self,
        backend_pid: BackendPid,
        sql: &str,
        timeout_config: Option<TimeoutConfig>,
    ) -> QueryId {
        let query_id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let config = timeout_config.unwrap_or_else(|| self.get_default_timeout());
        let context = QueryContext::new(query_id, backend_pid, sql, config);

        {
            let mut queries = self.queries.write().unwrap();
            queries.insert(query_id, context);
        }

        {
            let mut pid_queries = self.pid_queries.write().unwrap();
            pid_queries
                .entry(backend_pid)
                .or_insert_with(Vec::new)
                .push(query_id);
        }

        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
            stats.running_queries += 1;
        }

        query_id
    }

    /// Complete a query
    pub fn complete_query(&self, query_id: QueryId, success: bool, error: Option<&str>) {
        let mut queries = self.queries.write().unwrap();
        if let Some(context) = queries.get_mut(&query_id) {
            if success {
                context.mark_completed();
            } else if let Some(err) = error {
                context.mark_failed(err);
            }
        }

        // Remove from tracking
        if let Some(context) = queries.remove(&query_id) {
            let mut pid_queries = self.pid_queries.write().unwrap();
            if let Some(query_ids) = pid_queries.get_mut(&context.backend_pid) {
                query_ids.retain(|&id| id != query_id);
                if query_ids.is_empty() {
                    pid_queries.remove(&context.backend_pid);
                }
            }

            let mut stats = self.stats.write().unwrap();
            stats.running_queries = stats.running_queries.saturating_sub(1);
            if success {
                stats.completed_queries += 1;
            } else {
                stats.failed_queries += 1;
            }
        }
    }

    /// Get query context
    pub fn get_query(&self, query_id: QueryId) -> Option<QueryContext> {
        self.queries.read().unwrap().get(&query_id).map(|ctx| QueryContext {
            query_id: ctx.query_id,
            backend_pid: ctx.backend_pid,
            sql: ctx.sql.clone(),
            start_time: ctx.start_time,
            start_system_time: ctx.start_system_time,
            state: ctx.state,
            timeout_config: ctx.timeout_config.clone(),
            cancel_requested: Arc::clone(&ctx.cancel_requested),
            lock_wait_start: ctx.lock_wait_start,
            error: ctx.error.clone(),
        })
    }

    /// Check query timeouts and cancellation
    pub fn check_query(&self, query_id: QueryId) -> Result<(), QuerySafetyError> {
        let queries = self.queries.read().unwrap();
        if let Some(context) = queries.get(&query_id) {
            context.check()
        } else {
            Err(QuerySafetyError::QueryNotFound(query_id))
        }
    }

    /// Cancel a query by ID
    pub fn cancel_query(&self, query_id: QueryId) -> Result<(), QuerySafetyError> {
        let queries = self.queries.read().unwrap();
        if let Some(context) = queries.get(&query_id) {
            context.request_cancel();

            let mut stats = self.stats.write().unwrap();
            stats.cancelled_queries += 1;

            Ok(())
        } else {
            Err(QuerySafetyError::QueryNotFound(query_id))
        }
    }

    /// Cancel all queries for a backend (pg_cancel_backend)
    pub fn pg_cancel_backend(&self, pid: BackendPid) -> bool {
        let pid_queries = self.pid_queries.read().unwrap();
        if let Some(query_ids) = pid_queries.get(&pid) {
            let queries = self.queries.read().unwrap();
            for &query_id in query_ids {
                if let Some(context) = queries.get(&query_id) {
                    context.request_cancel();
                }
            }

            let mut stats = self.stats.write().unwrap();
            stats.cancelled_queries += query_ids.len() as u64;

            true
        } else {
            false
        }
    }

    /// Terminate a backend (pg_terminate_backend)
    pub fn pg_terminate_backend(&self, pid: BackendPid) -> bool {
        // First cancel all queries
        self.pg_cancel_backend(pid);

        // Remove from tracking
        let query_ids: Vec<QueryId> = {
            let mut pid_queries = self.pid_queries.write().unwrap();
            pid_queries.remove(&pid).unwrap_or_default()
        };

        let mut queries = self.queries.write().unwrap();
        for query_id in &query_ids {
            queries.remove(query_id);
        }

        if !query_ids.is_empty() {
            let mut stats = self.stats.write().unwrap();
            stats.running_queries = stats.running_queries.saturating_sub(query_ids.len());
        }

        !query_ids.is_empty()
    }

    /// Get all running queries
    pub fn get_running_queries(&self) -> Vec<QueryInfo> {
        let queries = self.queries.read().unwrap();
        queries
            .values()
            .filter(|ctx| ctx.state == QueryState::Running || ctx.state == QueryState::WaitingLock)
            .map(|ctx| QueryInfo {
                query_id: ctx.query_id,
                backend_pid: ctx.backend_pid,
                sql: ctx.sql.clone(),
                start_time: ctx.start_system_time,
                elapsed_ms: ctx.elapsed_ms(),
                state: ctx.state,
                waiting_for_lock: ctx.lock_wait_start.is_some(),
            })
            .collect()
    }

    /// Get long-running queries (exceeding threshold)
    pub fn get_long_running_queries(&self) -> Vec<QueryInfo> {
        let threshold = self.long_query_threshold_ms.load(Ordering::Relaxed);
        let queries = self.queries.read().unwrap();
        queries
            .values()
            .filter(|ctx| {
                (ctx.state == QueryState::Running || ctx.state == QueryState::WaitingLock)
                    && ctx.elapsed_ms() >= threshold
            })
            .map(|ctx| QueryInfo {
                query_id: ctx.query_id,
                backend_pid: ctx.backend_pid,
                sql: ctx.sql.clone(),
                start_time: ctx.start_system_time,
                elapsed_ms: ctx.elapsed_ms(),
                state: ctx.state,
                waiting_for_lock: ctx.lock_wait_start.is_some(),
            })
            .collect()
    }

    /// Check all queries for timeouts
    pub fn check_all_timeouts(&self) -> Vec<(QueryId, QuerySafetyError)> {
        let queries = self.queries.read().unwrap();
        let mut timed_out = Vec::new();

        for (query_id, context) in queries.iter() {
            if let Err(err) = context.check() {
                timed_out.push((*query_id, err));
            }
        }

        if !timed_out.is_empty() {
            let mut stats = self.stats.write().unwrap();
            stats.timed_out_queries += timed_out.len() as u64;
        }

        timed_out
    }

    /// Start lock wait for a query
    pub fn start_lock_wait(&self, query_id: QueryId) {
        let mut queries = self.queries.write().unwrap();
        if let Some(context) = queries.get_mut(&query_id) {
            context.start_lock_wait();

            let mut stats = self.stats.write().unwrap();
            stats.waiting_queries += 1;
        }
    }

    /// End lock wait for a query
    pub fn end_lock_wait(&self, query_id: QueryId) {
        let mut queries = self.queries.write().unwrap();
        if let Some(context) = queries.get_mut(&query_id) {
            context.end_lock_wait();

            let mut stats = self.stats.write().unwrap();
            stats.waiting_queries = stats.waiting_queries.saturating_sub(1);
        }
    }

    /// Get statistics
    pub fn stats(&self) -> QueryManagerStats {
        self.stats.read().unwrap().clone()
    }
}

/// Query info for external use
#[derive(Debug, Clone)]
pub struct QueryInfo {
    /// Query ID
    pub query_id: QueryId,
    /// Backend PID
    pub backend_pid: BackendPid,
    /// SQL text
    pub sql: String,
    /// Start time
    pub start_time: SystemTime,
    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Current state
    pub state: QueryState,
    /// Whether waiting for lock
    pub waiting_for_lock: bool,
}

impl QueryInfo {
    /// Format as pg_stat_activity row
    pub fn to_stat_row(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}ms\t{:?}",
            self.backend_pid,
            self.sql.chars().take(100).collect::<String>(),
            self.start_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            self.elapsed_ms,
            self.state
        )
    }
}

// ============================================================================
// Query Timeout Guard
// ============================================================================

/// RAII guard for query timeout checking
pub struct QueryTimeoutGuard {
    query_id: QueryId,
    manager: Arc<QueryManager>,
    check_interval: Duration,
    last_check: Instant,
}

impl QueryTimeoutGuard {
    /// Create a new timeout guard
    pub fn new(query_id: QueryId, manager: Arc<QueryManager>, check_interval_ms: u64) -> Self {
        Self {
            query_id,
            manager,
            check_interval: Duration::from_millis(check_interval_ms),
            last_check: Instant::now(),
        }
    }

    /// Check if timeout or cancellation occurred
    pub fn check(&mut self) -> Result<(), QuerySafetyError> {
        if self.last_check.elapsed() >= self.check_interval {
            self.last_check = Instant::now();
            self.manager.check_query(self.query_id)
        } else {
            Ok(())
        }
    }

    /// Get cancel flag for async checking
    pub fn cancel_flag(&self) -> Option<Arc<AtomicBool>> {
        self.manager.get_query(self.query_id).map(|ctx| ctx.cancel_flag())
    }
}

impl Drop for QueryTimeoutGuard {
    fn drop(&mut self) {
        // Query completed - will be marked as complete by caller
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_timeout_parsing() {
        assert_eq!(TimeoutConfig::parse_timeout("0"), Some(0));
        assert_eq!(TimeoutConfig::parse_timeout("off"), Some(0));
        assert_eq!(TimeoutConfig::parse_timeout("100"), Some(100));
        assert_eq!(TimeoutConfig::parse_timeout("100ms"), Some(100));
        assert_eq!(TimeoutConfig::parse_timeout("5s"), Some(5000));
        assert_eq!(TimeoutConfig::parse_timeout("2min"), Some(120000));
        assert_eq!(TimeoutConfig::parse_timeout("1h"), Some(3600000));
        assert_eq!(TimeoutConfig::parse_timeout("1d"), Some(86400000));
    }

    #[test]
    fn test_timeout_formatting() {
        assert_eq!(TimeoutConfig::format_timeout(0), "0");
        assert_eq!(TimeoutConfig::format_timeout(100), "100ms");
        assert_eq!(TimeoutConfig::format_timeout(5000), "5s");
        assert_eq!(TimeoutConfig::format_timeout(120000), "2min");
        assert_eq!(TimeoutConfig::format_timeout(3600000), "1h");
        assert_eq!(TimeoutConfig::format_timeout(86400000), "1d");
    }

    #[test]
    fn test_query_context() {
        let config = TimeoutConfig::default().with_statement_timeout(100);
        let ctx = QueryContext::new(1, 1000, "SELECT 1", config);

        assert_eq!(ctx.state, QueryState::Running);
        assert!(!ctx.is_cancel_requested());

        // Should not timeout immediately
        assert!(ctx.check_statement_timeout().is_none());
    }

    #[test]
    fn test_query_cancellation() {
        let config = TimeoutConfig::default();
        let ctx = QueryContext::new(1, 1000, "SELECT 1", config);

        // Request cancellation
        ctx.request_cancel();
        assert!(ctx.is_cancel_requested());

        // Check should return error
        let result = ctx.check();
        assert!(matches!(result, Err(QuerySafetyError::QueryCancelled { .. })));
    }

    #[test]
    fn test_query_manager() {
        let manager = QueryManager::new();

        // Register query
        let query_id = manager.register_query(1000, "SELECT 1", None);
        assert!(query_id > 0);

        // Get query
        let ctx = manager.get_query(query_id);
        assert!(ctx.is_some());
        assert_eq!(ctx.unwrap().backend_pid, 1000);

        // Complete query
        manager.complete_query(query_id, true, None);
        assert!(manager.get_query(query_id).is_none());
    }

    #[test]
    fn test_pg_cancel_backend() {
        let manager = QueryManager::new();

        let query_id = manager.register_query(1000, "SELECT pg_sleep(100)", None);

        // Cancel by PID
        let result = manager.pg_cancel_backend(1000);
        assert!(result);

        // Query should be marked for cancellation
        let ctx = manager.get_query(query_id).unwrap();
        assert!(ctx.is_cancel_requested());
    }

    #[test]
    fn test_pg_terminate_backend() {
        let manager = QueryManager::new();

        manager.register_query(1000, "SELECT 1", None);
        manager.register_query(1000, "SELECT 2", None);

        assert_eq!(manager.get_running_queries().len(), 2);

        // Terminate
        let result = manager.pg_terminate_backend(1000);
        assert!(result);

        // All queries for that PID should be removed
        assert_eq!(manager.get_running_queries().len(), 0);
    }

    #[test]
    fn test_statement_timeout() {
        let manager = QueryManager::new();
        let config = TimeoutConfig::default().with_statement_timeout(50);

        let query_id = manager.register_query(1000, "SELECT pg_sleep(10)", Some(config));

        // Wait for timeout
        thread::sleep(Duration::from_millis(100));

        // Check should return timeout error
        let result = manager.check_query(query_id);
        assert!(matches!(result, Err(QuerySafetyError::StatementTimeout { .. })));
    }

    #[test]
    fn test_long_running_queries() {
        let manager = QueryManager::new();
        manager.set_long_query_threshold(50);

        manager.register_query(1000, "SELECT 1", None);

        // Initially not long-running
        assert_eq!(manager.get_long_running_queries().len(), 0);

        // Wait
        thread::sleep(Duration::from_millis(100));

        // Now should be long-running
        assert_eq!(manager.get_long_running_queries().len(), 1);
    }

    #[test]
    fn test_lock_wait() {
        let manager = QueryManager::new();
        let config = TimeoutConfig::default().with_lock_timeout(50);

        let query_id = manager.register_query(1000, "UPDATE t SET x = 1", Some(config));
        manager.start_lock_wait(query_id);

        // Wait for timeout
        thread::sleep(Duration::from_millis(100));

        // Check should return lock timeout
        let result = manager.check_query(query_id);
        assert!(matches!(result, Err(QuerySafetyError::LockTimeout { .. })));
    }

    #[test]
    fn test_stats() {
        let manager = QueryManager::new();

        let q1 = manager.register_query(1000, "SELECT 1", None);
        let q2 = manager.register_query(1001, "SELECT 2", None);

        let stats = manager.stats();
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.running_queries, 2);

        manager.complete_query(q1, true, None);
        let stats = manager.stats();
        assert_eq!(stats.completed_queries, 1);
        assert_eq!(stats.running_queries, 1);

        manager.complete_query(q2, false, Some("error"));
        let stats = manager.stats();
        assert_eq!(stats.failed_queries, 1);
    }

    #[test]
    fn test_check_all_timeouts() {
        let manager = QueryManager::new();
        let config = TimeoutConfig::default().with_statement_timeout(50);

        manager.register_query(1000, "SELECT 1", Some(config.clone()));
        manager.register_query(1001, "SELECT 2", Some(config));

        thread::sleep(Duration::from_millis(100));

        let timed_out = manager.check_all_timeouts();
        assert_eq!(timed_out.len(), 2);
    }
}
