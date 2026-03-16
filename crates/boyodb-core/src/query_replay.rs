//! Query Replay/Shadowing
//!
//! Replay production traffic to test clusters and compare query results.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Captured Query
// ============================================================================

/// Captured query for replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedQuery {
    /// Unique query ID
    pub query_id: String,
    /// SQL statement
    pub sql: String,
    /// Query parameters
    pub parameters: Vec<QueryParam>,
    /// Database context
    pub database: String,
    /// User who executed the query
    pub user: String,
    /// Client application
    pub application: Option<String>,
    /// Capture timestamp
    pub captured_at: u64,
    /// Original execution time (microseconds)
    pub execution_time_us: u64,
    /// Original row count
    pub row_count: u64,
    /// Original result hash
    pub result_hash: Option<String>,
    /// Query metadata
    pub metadata: HashMap<String, String>,
}

/// Query parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
    pub position: usize,
    pub value: ParamValue,
}

/// Parameter value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Timestamp(String),
}

// ============================================================================
// Query Capture Configuration
// ============================================================================

/// Query capture configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureConfig {
    /// Enable capturing
    pub enabled: bool,
    /// Sample rate (0.0 - 1.0)
    pub sample_rate: f64,
    /// Minimum execution time to capture (microseconds)
    pub min_execution_time_us: u64,
    /// Maximum query length to capture
    pub max_query_length: usize,
    /// Capture filters
    pub filters: CaptureFilters,
    /// Storage settings
    pub storage: CaptureStorage,
    /// Maximum capture buffer size
    pub max_buffer_size: usize,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_rate: 0.1,  // 10%
            min_execution_time_us: 1000, // 1ms
            max_query_length: 65536,
            filters: CaptureFilters::default(),
            storage: CaptureStorage::Memory { max_queries: 10000 },
            max_buffer_size: 100000,
        }
    }
}

/// Capture filters
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CaptureFilters {
    /// Include only these databases
    pub databases: Option<Vec<String>>,
    /// Exclude these databases
    pub exclude_databases: Vec<String>,
    /// Include only these users
    pub users: Option<Vec<String>>,
    /// Exclude these users
    pub exclude_users: Vec<String>,
    /// Include queries matching patterns
    pub include_patterns: Vec<String>,
    /// Exclude queries matching patterns
    pub exclude_patterns: Vec<String>,
    /// Only capture SELECT queries
    pub select_only: bool,
}

/// Capture storage options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CaptureStorage {
    Memory { max_queries: usize },
    File { path: String, rotate_size_mb: usize },
    S3 { bucket: String, prefix: String },
}

// ============================================================================
// Query Capture Service
// ============================================================================

/// Query capture service
pub struct QueryCaptureService {
    config: RwLock<CaptureConfig>,
    /// Captured queries buffer
    buffer: RwLock<Vec<CapturedQuery>>,
    /// Capture statistics
    stats: CaptureStats,
    /// Running state
    running: AtomicBool,
}

#[derive(Default)]
struct CaptureStats {
    queries_seen: AtomicU64,
    queries_captured: AtomicU64,
    queries_filtered: AtomicU64,
    bytes_captured: AtomicU64,
}

impl QueryCaptureService {
    pub fn new(config: CaptureConfig) -> Self {
        Self {
            config: RwLock::new(config),
            buffer: RwLock::new(Vec::new()),
            stats: CaptureStats::default(),
            running: AtomicBool::new(false),
        }
    }

    /// Start capturing
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop capturing
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Capture a query (called after execution)
    pub fn capture(
        &self,
        sql: &str,
        database: &str,
        user: &str,
        execution_time_us: u64,
        row_count: u64,
        result_hash: Option<&str>,
    ) {
        if !self.running.load(Ordering::Relaxed) {
            return;
        }

        self.stats.queries_seen.fetch_add(1, Ordering::Relaxed);

        let config = self.config.read();

        if !config.enabled {
            return;
        }

        // Check sample rate
        if !should_sample(config.sample_rate) {
            return;
        }

        // Check minimum execution time
        if execution_time_us < config.min_execution_time_us {
            return;
        }

        // Check query length
        if sql.len() > config.max_query_length {
            return;
        }

        // Apply filters
        if !self.passes_filters(sql, database, user, &config.filters) {
            self.stats.queries_filtered.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let query = CapturedQuery {
            query_id: generate_query_id(),
            sql: sql.to_string(),
            parameters: vec![],
            database: database.to_string(),
            user: user.to_string(),
            application: None,
            captured_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            execution_time_us,
            row_count,
            result_hash: result_hash.map(|s| s.to_string()),
            metadata: HashMap::new(),
        };

        let mut buffer = self.buffer.write();
        if buffer.len() < config.max_buffer_size {
            self.stats.bytes_captured.fetch_add(sql.len() as u64, Ordering::Relaxed);
            buffer.push(query);
            self.stats.queries_captured.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn passes_filters(&self, sql: &str, database: &str, user: &str, filters: &CaptureFilters) -> bool {
        // Check database filters
        if let Some(ref dbs) = filters.databases {
            if !dbs.iter().any(|d| d == database) {
                return false;
            }
        }
        if filters.exclude_databases.iter().any(|d| d == database) {
            return false;
        }

        // Check user filters
        if let Some(ref users) = filters.users {
            if !users.iter().any(|u| u == user) {
                return false;
            }
        }
        if filters.exclude_users.iter().any(|u| u == user) {
            return false;
        }

        // Check SELECT only
        if filters.select_only {
            let upper = sql.trim().to_uppercase();
            if !upper.starts_with("SELECT") {
                return false;
            }
        }

        // Check patterns
        if !filters.include_patterns.is_empty() {
            let matches = filters.include_patterns.iter().any(|p| sql.contains(p));
            if !matches {
                return false;
            }
        }
        for pattern in &filters.exclude_patterns {
            if sql.contains(pattern) {
                return false;
            }
        }

        true
    }

    /// Get captured queries
    pub fn get_queries(&self) -> Vec<CapturedQuery> {
        self.buffer.read().clone()
    }

    /// Drain captured queries
    pub fn drain_queries(&self) -> Vec<CapturedQuery> {
        self.buffer.write().drain(..).collect()
    }

    /// Get statistics
    pub fn stats(&self) -> CaptureStatsSnapshot {
        CaptureStatsSnapshot {
            queries_seen: self.stats.queries_seen.load(Ordering::Relaxed),
            queries_captured: self.stats.queries_captured.load(Ordering::Relaxed),
            queries_filtered: self.stats.queries_filtered.load(Ordering::Relaxed),
            bytes_captured: self.stats.bytes_captured.load(Ordering::Relaxed),
            buffer_size: self.buffer.read().len(),
        }
    }

    /// Clear buffer
    pub fn clear(&self) {
        self.buffer.write().clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureStatsSnapshot {
    pub queries_seen: u64,
    pub queries_captured: u64,
    pub queries_filtered: u64,
    pub bytes_captured: u64,
    pub buffer_size: usize,
}

// ============================================================================
// Replay Configuration
// ============================================================================

/// Replay configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Target cluster endpoint
    pub target_endpoint: String,
    /// Replay mode
    pub mode: ReplayMode,
    /// Concurrency level
    pub concurrency: usize,
    /// Rate limiting (queries per second, 0 = unlimited)
    pub rate_limit_qps: f64,
    /// Timeout per query (milliseconds)
    pub query_timeout_ms: u64,
    /// Compare results with original
    pub compare_results: bool,
    /// Continue on error
    pub continue_on_error: bool,
    /// User override (replay as this user)
    pub user_override: Option<String>,
    /// Database override
    pub database_override: Option<String>,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            target_endpoint: String::new(),
            mode: ReplayMode::Sequential,
            concurrency: 4,
            rate_limit_qps: 0.0,
            query_timeout_ms: 30000,
            compare_results: true,
            continue_on_error: true,
            user_override: None,
            database_override: None,
        }
    }
}

/// Replay mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayMode {
    /// Execute queries sequentially
    Sequential,
    /// Execute queries in parallel
    Parallel,
    /// Replay at original timing
    TimeBased,
    /// Replay as fast as possible
    Stress,
}

// ============================================================================
// Replay Result
// ============================================================================

/// Result of replaying a single query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryReplayResult {
    /// Original query ID
    pub query_id: String,
    /// Replay status
    pub status: ReplayStatus,
    /// Replay execution time (microseconds)
    pub execution_time_us: u64,
    /// Original execution time (microseconds)
    pub original_execution_time_us: u64,
    /// Row count from replay
    pub row_count: u64,
    /// Original row count
    pub original_row_count: u64,
    /// Result comparison
    pub comparison: Option<ResultComparison>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Replay timestamp
    pub replayed_at: u64,
}

/// Replay status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayStatus {
    Success,
    Failed,
    Timeout,
    Skipped,
}

/// Result comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultComparison {
    /// Results match exactly
    pub exact_match: bool,
    /// Row count matches
    pub row_count_match: bool,
    /// Schema matches
    pub schema_match: bool,
    /// Number of differing rows
    pub diff_count: usize,
    /// Sample differences
    pub sample_diffs: Vec<RowDiff>,
}

/// Row difference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowDiff {
    pub row_index: usize,
    pub column: String,
    pub original_value: String,
    pub replay_value: String,
}

// ============================================================================
// Query Replay Service
// ============================================================================

/// Query replay service
pub struct QueryReplayService {
    config: RwLock<ReplayConfig>,
    /// Replay results
    results: RwLock<Vec<QueryReplayResult>>,
    /// Current replay job
    current_job: RwLock<Option<ReplayJob>>,
    /// Statistics
    stats: ReplayStats,
}

#[derive(Default)]
struct ReplayStats {
    total_replayed: AtomicU64,
    successful: AtomicU64,
    failed: AtomicU64,
    timeouts: AtomicU64,
    total_time_us: AtomicU64,
}

#[derive(Debug)]
struct ReplayJob {
    job_id: String,
    started_at: Instant,
    total_queries: usize,
    completed_queries: AtomicU64,
    status: JobStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl QueryReplayService {
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config: RwLock::new(config),
            results: RwLock::new(Vec::new()),
            current_job: RwLock::new(None),
            stats: ReplayStats::default(),
        }
    }

    /// Update configuration
    pub fn configure(&self, config: ReplayConfig) {
        *self.config.write() = config;
    }

    /// Start replay job
    pub fn start_replay(&self, queries: Vec<CapturedQuery>) -> String {
        let job_id = generate_job_id();

        let job = ReplayJob {
            job_id: job_id.clone(),
            started_at: Instant::now(),
            total_queries: queries.len(),
            completed_queries: AtomicU64::new(0),
            status: JobStatus::Running,
        };

        *self.current_job.write() = Some(job);
        self.results.write().clear();

        // Execute replay (simplified - in production would be async)
        self.execute_replay(queries);

        job_id
    }

    fn execute_replay(&self, queries: Vec<CapturedQuery>) {
        let config = self.config.read().clone();

        for query in queries {
            let start = Instant::now();

            // Execute query on target (simplified)
            let (status, row_count, error) = self.execute_on_target(&query, &config);

            let execution_time_us = start.elapsed().as_micros() as u64;

            let comparison = if config.compare_results && status == ReplayStatus::Success {
                Some(self.compare_results(&query, row_count))
            } else {
                None
            };

            let result = QueryReplayResult {
                query_id: query.query_id.clone(),
                status,
                execution_time_us,
                original_execution_time_us: query.execution_time_us,
                row_count,
                original_row_count: query.row_count,
                comparison,
                error,
                replayed_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            self.record_result(result);

            // Rate limiting
            if config.rate_limit_qps > 0.0 {
                let delay = Duration::from_secs_f64(1.0 / config.rate_limit_qps);
                std::thread::sleep(delay);
            }
        }

        // Mark job as completed
        if let Some(ref mut job) = *self.current_job.write() {
            job.status = JobStatus::Completed;
        }
    }

    fn execute_on_target(
        &self,
        _query: &CapturedQuery,
        _config: &ReplayConfig,
    ) -> (ReplayStatus, u64, Option<String>) {
        // Simplified - in production would execute against target cluster
        (ReplayStatus::Success, 100, None)
    }

    fn compare_results(&self, query: &CapturedQuery, replay_row_count: u64) -> ResultComparison {
        // Simplified comparison
        let row_count_match = query.row_count == replay_row_count;

        ResultComparison {
            exact_match: row_count_match, // Simplified
            row_count_match,
            schema_match: true,
            diff_count: if row_count_match { 0 } else { 1 },
            sample_diffs: vec![],
        }
    }

    fn record_result(&self, result: QueryReplayResult) {
        self.stats.total_replayed.fetch_add(1, Ordering::Relaxed);
        self.stats.total_time_us.fetch_add(result.execution_time_us, Ordering::Relaxed);

        match result.status {
            ReplayStatus::Success => {
                self.stats.successful.fetch_add(1, Ordering::Relaxed);
            }
            ReplayStatus::Failed => {
                self.stats.failed.fetch_add(1, Ordering::Relaxed);
            }
            ReplayStatus::Timeout => {
                self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            ReplayStatus::Skipped => {}
        }

        if let Some(ref job) = *self.current_job.read() {
            job.completed_queries.fetch_add(1, Ordering::Relaxed);
        }

        self.results.write().push(result);
    }

    /// Get replay progress
    pub fn progress(&self) -> Option<ReplayProgress> {
        let job = self.current_job.read();
        job.as_ref().map(|j| ReplayProgress {
            job_id: j.job_id.clone(),
            total_queries: j.total_queries,
            completed_queries: j.completed_queries.load(Ordering::Relaxed) as usize,
            elapsed_secs: j.started_at.elapsed().as_secs(),
            status: match j.status {
                JobStatus::Running => "running".to_string(),
                JobStatus::Completed => "completed".to_string(),
                JobStatus::Cancelled => "cancelled".to_string(),
                JobStatus::Failed => "failed".to_string(),
            },
        })
    }

    /// Get replay results
    pub fn get_results(&self) -> Vec<QueryReplayResult> {
        self.results.read().clone()
    }

    /// Get replay statistics
    pub fn stats(&self) -> ReplayStatsSnapshot {
        let total = self.stats.total_replayed.load(Ordering::Relaxed);
        let total_time = self.stats.total_time_us.load(Ordering::Relaxed);

        ReplayStatsSnapshot {
            total_replayed: total,
            successful: self.stats.successful.load(Ordering::Relaxed),
            failed: self.stats.failed.load(Ordering::Relaxed),
            timeouts: self.stats.timeouts.load(Ordering::Relaxed),
            avg_execution_time_us: if total > 0 { total_time / total } else { 0 },
            total_execution_time_us: total_time,
        }
    }

    /// Generate replay report
    pub fn generate_report(&self) -> ReplayReport {
        let results = self.results.read();
        let stats = self.stats();

        // Calculate performance comparison
        let mut slower_queries = 0;
        let mut faster_queries = 0;
        let mut total_regression_us: i64 = 0;

        for result in results.iter() {
            if result.status == ReplayStatus::Success {
                let diff = result.execution_time_us as i64 - result.original_execution_time_us as i64;
                if diff > (result.original_execution_time_us as i64 / 10) {
                    // >10% slower
                    slower_queries += 1;
                    total_regression_us += diff;
                } else if diff < -(result.original_execution_time_us as i64 / 10) {
                    // >10% faster
                    faster_queries += 1;
                }
            }
        }

        // Find queries with result mismatches
        let mismatched: Vec<_> = results
            .iter()
            .filter(|r| {
                r.comparison
                    .as_ref()
                    .map(|c| !c.exact_match)
                    .unwrap_or(false)
            })
            .map(|r| r.query_id.clone())
            .collect();

        ReplayReport {
            stats,
            performance: PerformanceComparison {
                slower_queries,
                faster_queries,
                total_regression_us,
            },
            mismatched_queries: mismatched,
            generated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

/// Replay progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProgress {
    pub job_id: String,
    pub total_queries: usize,
    pub completed_queries: usize,
    pub elapsed_secs: u64,
    pub status: String,
}

/// Replay statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStatsSnapshot {
    pub total_replayed: u64,
    pub successful: u64,
    pub failed: u64,
    pub timeouts: u64,
    pub avg_execution_time_us: u64,
    pub total_execution_time_us: u64,
}

/// Performance comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceComparison {
    pub slower_queries: usize,
    pub faster_queries: usize,
    pub total_regression_us: i64,
}

/// Replay report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    pub stats: ReplayStatsSnapshot,
    pub performance: PerformanceComparison,
    pub mismatched_queries: Vec<String>,
    pub generated_at: u64,
}

// ============================================================================
// Shadow Traffic Service
// ============================================================================

/// Shadow traffic service (live traffic duplication)
pub struct ShadowTrafficService {
    /// Shadow targets
    targets: RwLock<Vec<ShadowTarget>>,
    /// Running state
    running: AtomicBool,
    /// Statistics
    stats: ShadowStats,
}

/// Shadow target configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowTarget {
    pub name: String,
    pub endpoint: String,
    pub enabled: bool,
    pub sample_rate: f64,
}

#[derive(Default)]
struct ShadowStats {
    queries_shadowed: AtomicU64,
    queries_failed: AtomicU64,
}

impl ShadowTrafficService {
    pub fn new() -> Self {
        Self {
            targets: RwLock::new(Vec::new()),
            running: AtomicBool::new(false),
            stats: ShadowStats::default(),
        }
    }

    /// Add shadow target
    pub fn add_target(&self, target: ShadowTarget) {
        self.targets.write().push(target);
    }

    /// Remove shadow target
    pub fn remove_target(&self, name: &str) {
        self.targets.write().retain(|t| t.name != name);
    }

    /// Start shadowing
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop shadowing
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Shadow a query (non-blocking)
    pub fn shadow(&self, sql: &str, database: &str) {
        if !self.running.load(Ordering::Relaxed) {
            return;
        }

        let targets = self.targets.read().clone();

        for target in targets {
            if !target.enabled {
                continue;
            }

            if !should_sample(target.sample_rate) {
                continue;
            }

            // Execute asynchronously (simplified - would use async runtime)
            let _sql = sql.to_string();
            let _database = database.to_string();
            let _endpoint = target.endpoint.clone();

            // In production, spawn async task to execute on target
            self.stats.queries_shadowed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get statistics
    pub fn stats(&self) -> ShadowStatsSnapshot {
        ShadowStatsSnapshot {
            queries_shadowed: self.stats.queries_shadowed.load(Ordering::Relaxed),
            queries_failed: self.stats.queries_failed.load(Ordering::Relaxed),
            targets: self.targets.read().len(),
        }
    }

    /// List targets
    pub fn list_targets(&self) -> Vec<ShadowTarget> {
        self.targets.read().clone()
    }
}

impl Default for ShadowTrafficService {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowStatsSnapshot {
    pub queries_shadowed: u64,
    pub queries_failed: u64,
    pub targets: usize,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn should_sample(rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }

    // Simple random sampling using time-based hash
    let hash = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    (hash % 10000) < (rate * 10000.0) as u64
}

fn generate_query_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("q_{:016x}", hasher.finish())
}

fn generate_job_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("job_{:016x}", hasher.finish())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_capture() {
        let config = CaptureConfig {
            enabled: true,
            sample_rate: 1.0, // Capture all
            min_execution_time_us: 0,
            ..Default::default()
        };

        let service = QueryCaptureService::new(config);
        service.start();

        service.capture(
            "SELECT * FROM users",
            "test_db",
            "test_user",
            1000,
            100,
            Some("abc123"),
        );

        let queries = service.get_queries();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].sql, "SELECT * FROM users");
    }

    #[test]
    fn test_capture_filters() {
        let mut config = CaptureConfig {
            enabled: true,
            sample_rate: 1.0,
            min_execution_time_us: 0,
            ..Default::default()
        };
        config.filters.select_only = true;

        let service = QueryCaptureService::new(config);
        service.start();

        // This should be captured
        service.capture("SELECT * FROM users", "db", "user", 1000, 100, None);
        // This should NOT be captured
        service.capture("INSERT INTO users VALUES (1)", "db", "user", 1000, 1, None);

        let queries = service.get_queries();
        assert_eq!(queries.len(), 1);
    }

    #[test]
    fn test_query_replay() {
        let config = ReplayConfig {
            compare_results: true,
            ..Default::default()
        };

        let service = QueryReplayService::new(config);

        let queries = vec![CapturedQuery {
            query_id: "q1".to_string(),
            sql: "SELECT 1".to_string(),
            parameters: vec![],
            database: "test".to_string(),
            user: "user".to_string(),
            application: None,
            captured_at: 1000,
            execution_time_us: 500,
            row_count: 1,
            result_hash: None,
            metadata: HashMap::new(),
        }];

        let job_id = service.start_replay(queries);
        assert!(!job_id.is_empty());

        let results = service.get_results();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_shadow_traffic() {
        let service = ShadowTrafficService::new();

        service.add_target(ShadowTarget {
            name: "test".to_string(),
            endpoint: "localhost:5432".to_string(),
            enabled: true,
            sample_rate: 1.0,
        });

        service.start();
        service.shadow("SELECT 1", "test_db");

        let stats = service.stats();
        assert_eq!(stats.targets, 1);
        // Note: actual shadow count depends on sampling
    }
}
