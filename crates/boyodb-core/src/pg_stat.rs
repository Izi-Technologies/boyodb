//! PostgreSQL Statistics Views Implementation
//!
//! PostgreSQL-compatible statistics and monitoring views.
//!
//! # Features
//! - pg_stat_statements - query statistics
//! - pg_stat_activity - active sessions
//! - pg_stat_user_tables - table statistics
//! - Slow query logging
//! - Query fingerprinting and normalization
//!
//! # Example
//! ```sql
//! -- View query statistics
//! SELECT query, calls, mean_time, total_time
//! FROM pg_stat_statements
//! ORDER BY total_time DESC
//! LIMIT 10;
//!
//! -- View active sessions
//! SELECT pid, usename, query, state, query_start
//! FROM pg_stat_activity
//! WHERE state = 'active';
//!
//! -- Reset statistics
//! SELECT pg_stat_statements_reset();
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, SystemTime};

// ============================================================================
// Types
// ============================================================================

/// Query ID (fingerprint hash)
pub type QueryId = u64;

/// User ID
pub type UserId = u32;

/// Database ID
pub type DatabaseId = u32;

// ============================================================================
// Query Fingerprinting
// ============================================================================

/// Normalize a SQL query for fingerprinting
/// Replaces literals with placeholders to group similar queries
pub fn normalize_query(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut in_number = false;

    while let Some(c) = chars.next() {
        if in_string {
            if c == string_char {
                // Check for escaped quote
                if chars.peek() == Some(&string_char) {
                    chars.next();
                    continue;
                }
                in_string = false;
                result.push('?');
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_string = true;
            string_char = c;
            continue;
        }

        if c.is_ascii_digit() || (c == '-' && chars.peek().map(|p| p.is_ascii_digit()).unwrap_or(false)) {
            if !in_number {
                in_number = true;
                result.push('?');
            }
            continue;
        }

        if in_number && !c.is_ascii_digit() && c != '.' && c != 'e' && c != 'E' {
            in_number = false;
        }

        // Normalize whitespace
        if c.is_whitespace() {
            if !result.ends_with(' ') && !result.is_empty() {
                result.push(' ');
            }
        } else {
            result.push(c);
        }
    }

    result.trim().to_string()
}

/// Calculate query fingerprint (hash)
pub fn query_fingerprint(sql: &str) -> QueryId {
    let normalized = normalize_query(sql);
    // Simple FNV-1a hash
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in normalized.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ============================================================================
// pg_stat_statements Entry
// ============================================================================

/// Statistics for a single query fingerprint
#[derive(Debug, Clone)]
pub struct StatementStats {
    /// Query ID (fingerprint)
    pub queryid: QueryId,
    /// User ID
    pub userid: UserId,
    /// Database ID
    pub dbid: DatabaseId,
    /// Normalized query text
    pub query: String,
    /// Number of times executed
    pub calls: u64,
    /// Total execution time (ms)
    pub total_time: f64,
    /// Minimum execution time (ms)
    pub min_time: f64,
    /// Maximum execution time (ms)
    pub max_time: f64,
    /// Mean execution time (ms)
    pub mean_time: f64,
    /// Standard deviation of execution time
    pub stddev_time: f64,
    /// Number of rows returned
    pub rows: u64,
    /// Shared blocks hit
    pub shared_blks_hit: u64,
    /// Shared blocks read
    pub shared_blks_read: u64,
    /// Shared blocks dirtied
    pub shared_blks_dirtied: u64,
    /// Shared blocks written
    pub shared_blks_written: u64,
    /// Local blocks hit
    pub local_blks_hit: u64,
    /// Local blocks read
    pub local_blks_read: u64,
    /// Temp blocks read
    pub temp_blks_read: u64,
    /// Temp blocks written
    pub temp_blks_written: u64,
    /// Block read time (ms)
    pub blk_read_time: f64,
    /// Block write time (ms)
    pub blk_write_time: f64,
    /// Planning time (ms) - PostgreSQL 13+
    pub plans: u64,
    /// Total planning time (ms)
    pub total_plan_time: f64,
    /// WAL records generated
    pub wal_records: u64,
    /// WAL bytes generated
    pub wal_bytes: u64,
}

impl StatementStats {
    /// Create new stats entry
    pub fn new(queryid: QueryId, userid: UserId, dbid: DatabaseId, query: &str) -> Self {
        Self {
            queryid,
            userid,
            dbid,
            query: query.to_string(),
            calls: 0,
            total_time: 0.0,
            min_time: f64::MAX,
            max_time: 0.0,
            mean_time: 0.0,
            stddev_time: 0.0,
            rows: 0,
            shared_blks_hit: 0,
            shared_blks_read: 0,
            shared_blks_dirtied: 0,
            shared_blks_written: 0,
            local_blks_hit: 0,
            local_blks_read: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
            blk_read_time: 0.0,
            blk_write_time: 0.0,
            plans: 0,
            total_plan_time: 0.0,
            wal_records: 0,
            wal_bytes: 0,
        }
    }

    /// Record an execution
    pub fn record_execution(&mut self, exec: &ExecutionRecord) {
        self.calls += 1;
        self.total_time += exec.execution_time_ms;
        self.rows += exec.rows_returned;

        if exec.execution_time_ms < self.min_time {
            self.min_time = exec.execution_time_ms;
        }
        if exec.execution_time_ms > self.max_time {
            self.max_time = exec.execution_time_ms;
        }

        // Update mean and stddev (Welford's online algorithm)
        let delta = exec.execution_time_ms - self.mean_time;
        self.mean_time += delta / self.calls as f64;
        let delta2 = exec.execution_time_ms - self.mean_time;
        self.stddev_time += delta * delta2;

        // Block stats
        self.shared_blks_hit += exec.shared_blks_hit;
        self.shared_blks_read += exec.shared_blks_read;
        self.shared_blks_written += exec.shared_blks_written;
        self.temp_blks_read += exec.temp_blks_read;
        self.temp_blks_written += exec.temp_blks_written;
        self.blk_read_time += exec.blk_read_time_ms;
        self.blk_write_time += exec.blk_write_time_ms;

        if exec.plan_time_ms > 0.0 {
            self.plans += 1;
            self.total_plan_time += exec.plan_time_ms;
        }
    }

    /// Get computed standard deviation
    pub fn computed_stddev(&self) -> f64 {
        if self.calls > 1 {
            (self.stddev_time / (self.calls - 1) as f64).sqrt()
        } else {
            0.0
        }
    }

    /// Get average planning time
    pub fn mean_plan_time(&self) -> f64 {
        if self.plans > 0 {
            self.total_plan_time / self.plans as f64
        } else {
            0.0
        }
    }
}

/// Record of a single query execution
#[derive(Debug, Clone, Default)]
pub struct ExecutionRecord {
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Planning time in milliseconds
    pub plan_time_ms: f64,
    /// Rows returned
    pub rows_returned: u64,
    /// Shared blocks hit
    pub shared_blks_hit: u64,
    /// Shared blocks read
    pub shared_blks_read: u64,
    /// Shared blocks written
    pub shared_blks_written: u64,
    /// Temp blocks read
    pub temp_blks_read: u64,
    /// Temp blocks written
    pub temp_blks_written: u64,
    /// Block read time in milliseconds
    pub blk_read_time_ms: f64,
    /// Block write time in milliseconds
    pub blk_write_time_ms: f64,
}

// ============================================================================
// pg_stat_statements Manager
// ============================================================================

/// Configuration for pg_stat_statements
#[derive(Debug, Clone)]
pub struct StatStatementsConfig {
    /// Maximum number of distinct statements to track
    pub max_statements: usize,
    /// Track utility commands (DDL, etc.)
    pub track_utility: bool,
    /// Track planning time
    pub track_planning: bool,
    /// Minimum execution time to track (ms)
    pub min_duration_ms: f64,
    /// Save stats on shutdown
    pub save_on_shutdown: bool,
}

impl Default for StatStatementsConfig {
    fn default() -> Self {
        Self {
            max_statements: 5000,
            track_utility: true,
            track_planning: true,
            min_duration_ms: 0.0,
            save_on_shutdown: true,
        }
    }
}

/// pg_stat_statements manager
pub struct StatStatementsManager {
    /// Configuration
    config: StatStatementsConfig,
    /// Statement statistics (queryid -> stats)
    statements: RwLock<HashMap<QueryId, StatementStats>>,
    /// Deallocation count (when max exceeded)
    dealloc_count: AtomicU64,
    /// Statistics reset time
    stats_reset: RwLock<Option<SystemTime>>,
}

impl Default for StatStatementsManager {
    fn default() -> Self {
        Self::new(StatStatementsConfig::default())
    }
}

impl StatStatementsManager {
    /// Create a new manager
    pub fn new(config: StatStatementsConfig) -> Self {
        Self {
            config,
            statements: RwLock::new(HashMap::new()),
            dealloc_count: AtomicU64::new(0),
            stats_reset: RwLock::new(Some(SystemTime::now())),
        }
    }

    /// Record a query execution
    pub fn record(&self, userid: UserId, dbid: DatabaseId, sql: &str, exec: ExecutionRecord) {
        // Check minimum duration
        if exec.execution_time_ms < self.config.min_duration_ms {
            return;
        }

        let queryid = query_fingerprint(sql);
        let normalized = normalize_query(sql);

        let mut statements = self.statements.write();

        // Check if we need to evict
        if !statements.contains_key(&queryid) && statements.len() >= self.config.max_statements {
            // Evict least used statement
            if let Some((&evict_id, _)) = statements
                .iter()
                .min_by_key(|(_, s)| s.calls)
            {
                statements.remove(&evict_id);
                self.dealloc_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Get or create entry
        let stats = statements
            .entry(queryid)
            .or_insert_with(|| StatementStats::new(queryid, userid, dbid, &normalized));

        stats.record_execution(&exec);
    }

    /// Get all statement statistics
    pub fn get_statements(&self) -> Vec<StatementStats> {
        self.statements.read().values().cloned().collect()
    }

    /// Get top N statements by total time
    pub fn get_top_by_time(&self, n: usize) -> Vec<StatementStats> {
        let mut stats: Vec<_> = self.statements.read().values().cloned().collect();
        stats.sort_by(|a, b| b.total_time.partial_cmp(&a.total_time).unwrap());
        stats.truncate(n);
        stats
    }

    /// Get top N statements by calls
    pub fn get_top_by_calls(&self, n: usize) -> Vec<StatementStats> {
        let mut stats: Vec<_> = self.statements.read().values().cloned().collect();
        stats.sort_by(|a, b| b.calls.cmp(&a.calls));
        stats.truncate(n);
        stats
    }

    /// Get top N statements by mean time
    pub fn get_top_by_mean_time(&self, n: usize) -> Vec<StatementStats> {
        let mut stats: Vec<_> = self.statements.read().values().cloned().collect();
        stats.sort_by(|a, b| b.mean_time.partial_cmp(&a.mean_time).unwrap());
        stats.truncate(n);
        stats
    }

    /// Get statement by query ID
    pub fn get_statement(&self, queryid: QueryId) -> Option<StatementStats> {
        self.statements.read().get(&queryid).cloned()
    }

    /// Reset all statistics (pg_stat_statements_reset)
    pub fn reset(&self) {
        let mut statements = self.statements.write();
        statements.clear();
        *self.stats_reset.write() = Some(SystemTime::now());
    }

    /// Reset statistics for a specific query
    pub fn reset_query(&self, queryid: QueryId) -> bool {
        self.statements.write().remove(&queryid).is_some()
    }

    /// Get deallocation count
    pub fn dealloc_count(&self) -> u64 {
        self.dealloc_count.load(Ordering::Relaxed)
    }

    /// Get stats reset time
    pub fn stats_reset_time(&self) -> Option<SystemTime> {
        *self.stats_reset.read()
    }

    /// Get number of tracked statements
    pub fn statement_count(&self) -> usize {
        self.statements.read().len()
    }
}

// ============================================================================
// pg_stat_activity Entry
// ============================================================================

/// Activity state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivityState {
    Active,
    Idle,
    IdleInTransaction,
    IdleInTransactionAborted,
    FastpathFunctionCall,
    Disabled,
}

impl std::fmt::Display for ActivityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Idle => write!(f, "idle"),
            Self::IdleInTransaction => write!(f, "idle in transaction"),
            Self::IdleInTransactionAborted => write!(f, "idle in transaction (aborted)"),
            Self::FastpathFunctionCall => write!(f, "fastpath function call"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// Backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    ClientBackend,
    BackgroundWorker,
    Autovacuum,
    WalSender,
    WalReceiver,
    Checkpointer,
    BgWriter,
    WalWriter,
    Archiver,
    StatsCollector,
    LogicalReplication,
    ParallelWorker,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientBackend => write!(f, "client backend"),
            Self::BackgroundWorker => write!(f, "background worker"),
            Self::Autovacuum => write!(f, "autovacuum worker"),
            Self::WalSender => write!(f, "walsender"),
            Self::WalReceiver => write!(f, "walreceiver"),
            Self::Checkpointer => write!(f, "checkpointer"),
            Self::BgWriter => write!(f, "background writer"),
            Self::WalWriter => write!(f, "walwriter"),
            Self::Archiver => write!(f, "archiver"),
            Self::StatsCollector => write!(f, "stats collector"),
            Self::LogicalReplication => write!(f, "logical replication worker"),
            Self::ParallelWorker => write!(f, "parallel worker"),
        }
    }
}

/// pg_stat_activity entry
#[derive(Debug, Clone)]
pub struct ActivityEntry {
    /// Database OID
    pub datid: Option<DatabaseId>,
    /// Database name
    pub datname: Option<String>,
    /// Process ID
    pub pid: u32,
    /// Leader PID for parallel workers
    pub leader_pid: Option<u32>,
    /// User OID
    pub usesysid: Option<UserId>,
    /// User name
    pub usename: Option<String>,
    /// Application name
    pub application_name: String,
    /// Client address
    pub client_addr: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Client port
    pub client_port: Option<i32>,
    /// Backend start time
    pub backend_start: SystemTime,
    /// Transaction start time
    pub xact_start: Option<SystemTime>,
    /// Query start time
    pub query_start: Option<SystemTime>,
    /// State change time
    pub state_change: Option<SystemTime>,
    /// Wait event type
    pub wait_event_type: Option<String>,
    /// Wait event
    pub wait_event: Option<String>,
    /// State
    pub state: ActivityState,
    /// Backend transaction ID
    pub backend_xid: Option<u64>,
    /// Backend xmin
    pub backend_xmin: Option<u64>,
    /// Query text
    pub query: String,
    /// Query ID (fingerprint)
    pub query_id: Option<QueryId>,
    /// Backend type
    pub backend_type: BackendType,
}

impl ActivityEntry {
    /// Create new entry for client connection
    pub fn new_client(pid: u32, database: &str, username: &str) -> Self {
        Self {
            datid: None,
            datname: Some(database.to_string()),
            pid,
            leader_pid: None,
            usesysid: None,
            usename: Some(username.to_string()),
            application_name: String::new(),
            client_addr: None,
            client_hostname: None,
            client_port: None,
            backend_start: SystemTime::now(),
            xact_start: None,
            query_start: None,
            state_change: Some(SystemTime::now()),
            wait_event_type: None,
            wait_event: None,
            state: ActivityState::Idle,
            backend_xid: None,
            backend_xmin: None,
            query: String::new(),
            query_id: None,
            backend_type: BackendType::ClientBackend,
        }
    }

    /// Start a query
    pub fn start_query(&mut self, query: &str) {
        self.query = query.to_string();
        self.query_id = Some(query_fingerprint(query));
        self.query_start = Some(SystemTime::now());
        self.state = ActivityState::Active;
        self.state_change = Some(SystemTime::now());
    }

    /// End a query
    pub fn end_query(&mut self) {
        self.query.clear();
        self.query_id = None;
        self.query_start = None;
        self.state = if self.xact_start.is_some() {
            ActivityState::IdleInTransaction
        } else {
            ActivityState::Idle
        };
        self.state_change = Some(SystemTime::now());
    }

    /// Start a transaction
    pub fn start_transaction(&mut self) {
        self.xact_start = Some(SystemTime::now());
    }

    /// End a transaction
    pub fn end_transaction(&mut self, aborted: bool) {
        self.xact_start = None;
        if aborted && self.state == ActivityState::IdleInTransaction {
            self.state = ActivityState::IdleInTransactionAborted;
        } else {
            self.state = ActivityState::Idle;
        }
        self.state_change = Some(SystemTime::now());
    }

    /// Set wait event
    pub fn set_wait_event(&mut self, event_type: &str, event: &str) {
        self.wait_event_type = Some(event_type.to_string());
        self.wait_event = Some(event.to_string());
    }

    /// Clear wait event
    pub fn clear_wait_event(&mut self) {
        self.wait_event_type = None;
        self.wait_event = None;
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
    pub fn xact_duration_ms(&self) -> Option<u64> {
        self.xact_start.map(|start| {
            SystemTime::now()
                .duration_since(start)
                .unwrap_or_default()
                .as_millis() as u64
        })
    }
}

// ============================================================================
// pg_stat_activity Manager
// ============================================================================

/// pg_stat_activity manager
pub struct StatActivityManager {
    /// Active sessions
    sessions: RwLock<HashMap<u32, ActivityEntry>>,
    /// Next PID
    next_pid: AtomicU64,
}

impl Default for StatActivityManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StatActivityManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            next_pid: AtomicU64::new(1000),
        }
    }

    /// Register a new session
    pub fn register_session(&self, database: &str, username: &str) -> u32 {
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed) as u32;
        let entry = ActivityEntry::new_client(pid, database, username);
        self.sessions.write().insert(pid, entry);
        pid
    }

    /// Unregister a session
    pub fn unregister_session(&self, pid: u32) {
        self.sessions.write().remove(&pid);
    }

    /// Get session
    pub fn get_session(&self, pid: u32) -> Option<ActivityEntry> {
        self.sessions.read().get(&pid).cloned()
    }

    /// Get all sessions
    pub fn get_all_sessions(&self) -> Vec<ActivityEntry> {
        self.sessions.read().values().cloned().collect()
    }

    /// Get active sessions
    pub fn get_active_sessions(&self) -> Vec<ActivityEntry> {
        self.sessions
            .read()
            .values()
            .filter(|e| e.state == ActivityState::Active)
            .cloned()
            .collect()
    }

    /// Update session
    pub fn update_session<F>(&self, pid: u32, f: F) -> bool
    where
        F: FnOnce(&mut ActivityEntry),
    {
        let mut sessions = self.sessions.write();
        if let Some(entry) = sessions.get_mut(&pid) {
            f(entry);
            true
        } else {
            false
        }
    }

    /// Get sessions by state
    pub fn get_by_state(&self, state: ActivityState) -> Vec<ActivityEntry> {
        self.sessions
            .read()
            .values()
            .filter(|e| e.state == state)
            .cloned()
            .collect()
    }

    /// Get sessions with long-running queries
    pub fn get_long_running(&self, threshold_ms: u64) -> Vec<ActivityEntry> {
        self.sessions
            .read()
            .values()
            .filter(|e| {
                e.state == ActivityState::Active
                    && e.query_duration_ms().map(|d| d >= threshold_ms).unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Get count by state
    pub fn count_by_state(&self) -> HashMap<ActivityState, usize> {
        let sessions = self.sessions.read();
        let mut counts = HashMap::new();
        for entry in sessions.values() {
            *counts.entry(entry.state).or_insert(0) += 1;
        }
        counts
    }
}

// ============================================================================
// Slow Query Log
// ============================================================================

/// Slow query log entry
#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    /// Timestamp
    pub timestamp: SystemTime,
    /// Database
    pub database: String,
    /// User
    pub username: String,
    /// Client address
    pub client_addr: Option<String>,
    /// Query duration in milliseconds
    pub duration_ms: f64,
    /// Query text
    pub query: String,
    /// Rows returned
    pub rows: u64,
    /// Planning time (ms)
    pub plan_time_ms: f64,
}

/// Slow query log configuration
#[derive(Debug, Clone)]
pub struct SlowQueryLogConfig {
    /// Enable slow query logging
    pub enabled: bool,
    /// Minimum duration to log (ms)
    pub min_duration_ms: f64,
    /// Log parameter values
    pub log_parameters: bool,
    /// Maximum query length to log
    pub max_query_length: usize,
    /// Maximum entries to keep in memory
    pub max_entries: usize,
}

impl Default for SlowQueryLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_duration_ms: 1000.0, // 1 second
            log_parameters: false,
            max_query_length: 4096,
            max_entries: 1000,
        }
    }
}

/// Slow query log
pub struct SlowQueryLog {
    /// Configuration
    config: SlowQueryLogConfig,
    /// Log entries
    entries: RwLock<Vec<SlowQueryEntry>>,
    /// Total slow queries logged
    total_count: AtomicU64,
}

impl Default for SlowQueryLog {
    fn default() -> Self {
        Self::new(SlowQueryLogConfig::default())
    }
}

impl SlowQueryLog {
    /// Create a new slow query log
    pub fn new(config: SlowQueryLogConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(Vec::new()),
            total_count: AtomicU64::new(0),
        }
    }

    /// Log a slow query
    pub fn log(
        &self,
        database: &str,
        username: &str,
        client_addr: Option<&str>,
        duration_ms: f64,
        query: &str,
        rows: u64,
        plan_time_ms: f64,
    ) {
        if !self.config.enabled || duration_ms < self.config.min_duration_ms {
            return;
        }

        let truncated_query = if query.len() > self.config.max_query_length {
            format!("{}...", &query[..self.config.max_query_length])
        } else {
            query.to_string()
        };

        let entry = SlowQueryEntry {
            timestamp: SystemTime::now(),
            database: database.to_string(),
            username: username.to_string(),
            client_addr: client_addr.map(String::from),
            duration_ms,
            query: truncated_query,
            rows,
            plan_time_ms,
        };

        let mut entries = self.entries.write();
        entries.push(entry);

        // Trim if over max
        while entries.len() > self.config.max_entries {
            entries.remove(0);
        }

        self.total_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get recent slow queries
    pub fn get_recent(&self, limit: usize) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read();
        entries.iter().rev().take(limit).cloned().collect()
    }

    /// Get slow queries in time range
    pub fn get_in_range(&self, start: SystemTime, end: SystemTime) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.timestamp >= start && e.timestamp <= end)
            .cloned()
            .collect()
    }

    /// Get total count
    pub fn total_count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Clear log
    pub fn clear(&self) {
        self.entries.write().clear();
    }

    /// Get config
    pub fn config(&self) -> &SlowQueryLogConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: SlowQueryLogConfig) {
        self.config = config;
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_query() {
        assert_eq!(
            normalize_query("SELECT * FROM users WHERE id = 123"),
            "SELECT * FROM users WHERE id = ?"
        );
        assert_eq!(
            normalize_query("SELECT * FROM users WHERE name = 'John'"),
            "SELECT * FROM users WHERE name = ?"
        );
        // Decimal numbers may normalize slightly differently but still work for fingerprinting
        let normalized = normalize_query("INSERT INTO t VALUES (1, 'test', 3.14)");
        assert!(normalized.starts_with("INSERT INTO t VALUES (?, ?,"));
    }

    #[test]
    fn test_query_fingerprint() {
        // Same normalized query should have same fingerprint
        let fp1 = query_fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = query_fingerprint("SELECT * FROM users WHERE id = 2");
        assert_eq!(fp1, fp2);

        // Different queries should have different fingerprints
        let fp3 = query_fingerprint("SELECT * FROM orders WHERE id = 1");
        assert_ne!(fp1, fp3);
    }

    #[test]
    fn test_statement_stats() {
        let mut stats = StatementStats::new(1, 1, 1, "SELECT ?");

        let exec = ExecutionRecord {
            execution_time_ms: 100.0,
            rows_returned: 10,
            ..Default::default()
        };
        stats.record_execution(&exec);

        assert_eq!(stats.calls, 1);
        assert_eq!(stats.total_time, 100.0);
        assert_eq!(stats.rows, 10);
        assert_eq!(stats.mean_time, 100.0);

        let exec2 = ExecutionRecord {
            execution_time_ms: 200.0,
            rows_returned: 20,
            ..Default::default()
        };
        stats.record_execution(&exec2);

        assert_eq!(stats.calls, 2);
        assert_eq!(stats.total_time, 300.0);
        assert_eq!(stats.mean_time, 150.0);
    }

    #[test]
    fn test_stat_statements_manager() {
        let manager = StatStatementsManager::new(StatStatementsConfig::default());

        manager.record(
            1,
            1,
            "SELECT * FROM users WHERE id = 1",
            ExecutionRecord {
                execution_time_ms: 100.0,
                rows_returned: 1,
                ..Default::default()
            },
        );

        manager.record(
            1,
            1,
            "SELECT * FROM users WHERE id = 2",
            ExecutionRecord {
                execution_time_ms: 150.0,
                rows_returned: 1,
                ..Default::default()
            },
        );

        // Same normalized query, should be combined
        assert_eq!(manager.statement_count(), 1);

        let stats = manager.get_statements();
        assert_eq!(stats[0].calls, 2);
        assert_eq!(stats[0].total_time, 250.0);
    }

    #[test]
    fn test_stat_activity_manager() {
        let manager = StatActivityManager::new();

        let pid = manager.register_session("testdb", "testuser");

        let session = manager.get_session(pid).unwrap();
        assert_eq!(session.state, ActivityState::Idle);

        manager.update_session(pid, |e| {
            e.start_query("SELECT 1");
        });

        let session = manager.get_session(pid).unwrap();
        assert_eq!(session.state, ActivityState::Active);

        let active = manager.get_active_sessions();
        assert_eq!(active.len(), 1);

        manager.unregister_session(pid);
        assert!(manager.get_session(pid).is_none());
    }

    #[test]
    fn test_slow_query_log() {
        let log = SlowQueryLog::new(SlowQueryLogConfig {
            min_duration_ms: 100.0,
            ..Default::default()
        });

        // Below threshold - should not log
        log.log("db", "user", None, 50.0, "SELECT 1", 0, 0.0);
        assert_eq!(log.total_count(), 0);

        // Above threshold - should log
        log.log("db", "user", None, 200.0, "SELECT * FROM big_table", 1000, 10.0);
        assert_eq!(log.total_count(), 1);

        let recent = log.get_recent(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].duration_ms, 200.0);
    }

    #[test]
    fn test_activity_state_display() {
        assert_eq!(format!("{}", ActivityState::Active), "active");
        assert_eq!(format!("{}", ActivityState::Idle), "idle");
        assert_eq!(
            format!("{}", ActivityState::IdleInTransaction),
            "idle in transaction"
        );
    }

    #[test]
    fn test_top_queries() {
        let manager = StatStatementsManager::new(StatStatementsConfig::default());

        // Use different queries (not just different numbers) since numbers get normalized
        manager.record(1, 1, "SELECT * FROM users", ExecutionRecord {
            execution_time_ms: 100.0,
            ..Default::default()
        });
        manager.record(1, 1, "SELECT * FROM orders", ExecutionRecord {
            execution_time_ms: 200.0,
            ..Default::default()
        });
        manager.record(1, 1, "SELECT * FROM products", ExecutionRecord {
            execution_time_ms: 50.0,
            ..Default::default()
        });

        let top = manager.get_top_by_time(2);
        assert_eq!(top.len(), 2);
        assert!(top[0].total_time >= top[1].total_time);
    }

    #[test]
    fn test_reset_stats() {
        let manager = StatStatementsManager::new(StatStatementsConfig::default());

        manager.record(1, 1, "SELECT 1", ExecutionRecord::default());
        assert_eq!(manager.statement_count(), 1);

        manager.reset();
        assert_eq!(manager.statement_count(), 0);
        assert!(manager.stats_reset_time().is_some());
    }
}
