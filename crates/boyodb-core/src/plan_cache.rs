//! Query Plan Cache and Slow Query Log
//!
//! Provides query optimization features:
//! - Prepared statement plan caching with LRU eviction
//! - Query fingerprinting for plan reuse
//! - Slow query detection and logging
//! - Query performance analysis

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Query Fingerprinting
// ============================================================================

/// Query fingerprint for identifying similar queries
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryFingerprint {
    /// Normalized SQL hash
    pub hash: u64,
    /// SQL template (with literals replaced by ?)
    pub template: String,
    /// Database context
    pub database: Option<String>,
}

impl QueryFingerprint {
    /// Create fingerprint from SQL
    pub fn from_sql(sql: &str, database: Option<&str>) -> Self {
        let template = normalize_sql(sql);
        let hash = hash_string(&template);

        Self {
            hash,
            template,
            database: database.map(String::from),
        }
    }
}

/// Normalize SQL by replacing literals with placeholders
fn normalize_sql(sql: &str) -> String {
    let mut result = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut in_number = false;

    while let Some(c) = chars.next() {
        if in_string {
            if c == string_char && chars.peek() != Some(&string_char) {
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

        if c.is_ascii_digit() && !in_number {
            in_number = true;
            result.push('?');
            // Skip remaining digits
            while chars.peek().map(|c| c.is_ascii_digit() || *c == '.').unwrap_or(false) {
                chars.next();
            }
            in_number = false;
            continue;
        }

        // Normalize whitespace
        if c.is_whitespace() {
            if !result.ends_with(' ') && !result.is_empty() {
                result.push(' ');
            }
            continue;
        }

        result.push(c.to_ascii_uppercase());
    }

    result.trim().to_string()
}

fn hash_string(s: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ============================================================================
// Query Plan
// ============================================================================

/// Cached query plan
#[derive(Clone, Debug)]
pub struct CachedPlan {
    /// Plan identifier
    pub plan_id: u64,
    /// Query fingerprint
    pub fingerprint: QueryFingerprint,
    /// Original SQL
    pub sql: String,
    /// Compiled plan (serialized)
    pub plan_data: Vec<u8>,
    /// Parameter types
    pub param_types: Vec<PlanParamType>,
    /// Result column types
    pub result_types: Vec<PlanColumnType>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Created timestamp
    pub created_at: Instant,
    /// Last used timestamp
    pub last_used: Instant,
    /// Execution count
    pub execution_count: u64,
    /// Total execution time
    pub total_execution_time: Duration,
    /// Plan version (for invalidation)
    pub version: u64,
}

/// Parameter type in plan
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanParamType {
    pub name: Option<String>,
    pub data_type: String,
    pub nullable: bool,
}

/// Column type in result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanColumnType {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

// ============================================================================
// Plan Cache
// ============================================================================

/// Plan cache configuration
#[derive(Clone, Debug)]
pub struct PlanCacheConfig {
    /// Maximum number of cached plans
    pub max_entries: usize,
    /// Maximum memory usage in bytes
    pub max_memory_bytes: usize,
    /// Entry TTL
    pub entry_ttl: Duration,
    /// Enable cache statistics
    pub enable_stats: bool,
    /// Minimum executions before caching
    pub min_executions_to_cache: u64,
    /// Enable adaptive caching
    pub adaptive: bool,
}

impl Default for PlanCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            entry_ttl: Duration::from_secs(3600),
            enable_stats: true,
            min_executions_to_cache: 2,
            adaptive: true,
        }
    }
}

/// LRU cache entry
struct CacheEntry {
    plan: CachedPlan,
    prev: Option<u64>,
    next: Option<u64>,
}

/// Plan cache with LRU eviction
pub struct PlanCache {
    config: PlanCacheConfig,
    /// Plans by fingerprint hash
    plans: RwLock<HashMap<u64, CacheEntry>>,
    /// LRU list head (most recent)
    head: RwLock<Option<u64>>,
    /// LRU list tail (least recent)
    tail: RwLock<Option<u64>>,
    /// Current memory usage
    memory_usage: AtomicU64,
    /// Statistics
    stats: RwLock<PlanCacheStats>,
    /// Plan ID counter
    plan_id_counter: AtomicU64,
    /// Global version for invalidation
    global_version: AtomicU64,
}

/// Plan cache statistics
#[derive(Clone, Debug, Default)]
pub struct PlanCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub total_plans: usize,
    pub memory_bytes: u64,
    pub avg_plan_size: f64,
    pub hit_rate: f64,
}

impl PlanCache {
    pub fn new(config: PlanCacheConfig) -> Self {
        Self {
            config,
            plans: RwLock::new(HashMap::new()),
            head: RwLock::new(None),
            tail: RwLock::new(None),
            memory_usage: AtomicU64::new(0),
            stats: RwLock::new(PlanCacheStats::default()),
            plan_id_counter: AtomicU64::new(0),
            global_version: AtomicU64::new(0),
        }
    }

    /// Get cached plan for query
    pub fn get(&self, fingerprint: &QueryFingerprint) -> Option<CachedPlan> {
        let mut plans = self.plans.write().unwrap();

        if let Some(entry) = plans.get_mut(&fingerprint.hash) {
            // Check TTL
            if entry.plan.created_at.elapsed() > self.config.entry_ttl {
                self.remove_entry(&mut plans, fingerprint.hash);
                self.stats.write().unwrap().misses += 1;
                return None;
            }

            // Check version
            if entry.plan.version < self.global_version.load(Ordering::SeqCst) {
                self.remove_entry(&mut plans, fingerprint.hash);
                self.stats.write().unwrap().invalidations += 1;
                return None;
            }

            // Update LRU
            entry.plan.last_used = Instant::now();
            entry.plan.execution_count += 1;

            // Move to head of LRU list
            let hash = fingerprint.hash;
            drop(plans);
            self.move_to_head(hash);

            let plans = self.plans.read().unwrap();
            let plan = plans.get(&hash).map(|e| e.plan.clone());

            if self.config.enable_stats {
                self.stats.write().unwrap().hits += 1;
            }

            plan
        } else {
            if self.config.enable_stats {
                self.stats.write().unwrap().misses += 1;
            }
            None
        }
    }

    /// Cache a query plan
    pub fn put(&self, fingerprint: QueryFingerprint, sql: String, plan_data: Vec<u8>, param_types: Vec<PlanParamType>, result_types: Vec<PlanColumnType>, estimated_cost: f64) {
        let plan_size = plan_data.len() + sql.len() + fingerprint.template.len();

        // Check memory limit
        while self.memory_usage.load(Ordering::SeqCst) + plan_size as u64 > self.config.max_memory_bytes as u64 {
            if !self.evict_one() {
                return; // Cannot make room
            }
        }

        let plan_id = self.plan_id_counter.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();

        let plan = CachedPlan {
            plan_id,
            fingerprint: fingerprint.clone(),
            sql,
            plan_data,
            param_types,
            result_types,
            estimated_cost,
            created_at: now,
            last_used: now,
            execution_count: 1,
            total_execution_time: Duration::ZERO,
            version: self.global_version.load(Ordering::SeqCst),
        };

        let hash = fingerprint.hash;

        // Check entry limit
        {
            let plans = self.plans.read().unwrap();
            if plans.len() >= self.config.max_entries && !plans.contains_key(&hash) {
                drop(plans);
                if !self.evict_one() {
                    return;
                }
            }
        }

        // Insert
        let mut plans = self.plans.write().unwrap();
        let old_head = *self.head.read().unwrap();

        let entry = CacheEntry {
            plan,
            prev: None,
            next: old_head,
        };

        if let Some(old_head_hash) = old_head {
            if let Some(old_head_entry) = plans.get_mut(&old_head_hash) {
                old_head_entry.prev = Some(hash);
            }
        }

        plans.insert(hash, entry);
        *self.head.write().unwrap() = Some(hash);

        if self.tail.read().unwrap().is_none() {
            *self.tail.write().unwrap() = Some(hash);
        }

        self.memory_usage.fetch_add(plan_size as u64, Ordering::SeqCst);

        // Update stats
        if self.config.enable_stats {
            let mut stats = self.stats.write().unwrap();
            stats.total_plans = plans.len();
            stats.memory_bytes = self.memory_usage.load(Ordering::SeqCst);
        }
    }

    /// Record execution time for a plan
    pub fn record_execution(&self, fingerprint: &QueryFingerprint, execution_time: Duration) {
        let mut plans = self.plans.write().unwrap();
        if let Some(entry) = plans.get_mut(&fingerprint.hash) {
            entry.plan.execution_count += 1;
            entry.plan.total_execution_time += execution_time;
        }
    }

    /// Invalidate all plans
    pub fn invalidate_all(&self) {
        self.global_version.fetch_add(1, Ordering::SeqCst);

        if self.config.enable_stats {
            let plans = self.plans.read().unwrap();
            self.stats.write().unwrap().invalidations += plans.len() as u64;
        }
    }

    /// Invalidate plans for a specific table
    pub fn invalidate_table(&self, _table: &str) {
        // Would search plans that reference the table
        // For now, invalidate all
        self.invalidate_all();
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut plans = self.plans.write().unwrap();
        plans.clear();
        *self.head.write().unwrap() = None;
        *self.tail.write().unwrap() = None;
        self.memory_usage.store(0, Ordering::SeqCst);

        if self.config.enable_stats {
            let mut stats = self.stats.write().unwrap();
            stats.total_plans = 0;
            stats.memory_bytes = 0;
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> PlanCacheStats {
        let mut stats = self.stats.read().unwrap().clone();

        let total = stats.hits + stats.misses;
        stats.hit_rate = if total > 0 {
            stats.hits as f64 / total as f64
        } else {
            0.0
        };

        if stats.total_plans > 0 {
            stats.avg_plan_size = stats.memory_bytes as f64 / stats.total_plans as f64;
        }

        stats
    }

    fn evict_one(&self) -> bool {
        let tail = *self.tail.read().unwrap();

        if let Some(tail_hash) = tail {
            let mut plans = self.plans.write().unwrap();
            self.remove_entry(&mut plans, tail_hash);

            if self.config.enable_stats {
                self.stats.write().unwrap().evictions += 1;
            }

            true
        } else {
            false
        }
    }

    fn remove_entry(&self, plans: &mut HashMap<u64, CacheEntry>, hash: u64) {
        if let Some(entry) = plans.remove(&hash) {
            let plan_size = entry.plan.plan_data.len() + entry.plan.sql.len() +
                entry.plan.fingerprint.template.len();

            self.memory_usage.fetch_sub(plan_size as u64, Ordering::SeqCst);

            // Update LRU links
            if let Some(prev) = entry.prev {
                if let Some(prev_entry) = plans.get_mut(&prev) {
                    prev_entry.next = entry.next;
                }
            } else {
                *self.head.write().unwrap() = entry.next;
            }

            if let Some(next) = entry.next {
                if let Some(next_entry) = plans.get_mut(&next) {
                    next_entry.prev = entry.prev;
                }
            } else {
                *self.tail.write().unwrap() = entry.prev;
            }
        }
    }

    fn move_to_head(&self, hash: u64) {
        let mut plans = self.plans.write().unwrap();

        let (prev, next) = if let Some(entry) = plans.get(&hash) {
            (entry.prev, entry.next)
        } else {
            return;
        };

        // Already at head
        if prev.is_none() {
            return;
        }

        // Remove from current position
        if let Some(prev_hash) = prev {
            if let Some(prev_entry) = plans.get_mut(&prev_hash) {
                prev_entry.next = next;
            }
        }

        if let Some(next_hash) = next {
            if let Some(next_entry) = plans.get_mut(&next_hash) {
                next_entry.prev = prev;
            }
        } else {
            *self.tail.write().unwrap() = prev;
        }

        // Move to head
        let old_head = *self.head.read().unwrap();

        if let Some(entry) = plans.get_mut(&hash) {
            entry.prev = None;
            entry.next = old_head;
        }

        if let Some(old_head_hash) = old_head {
            if let Some(old_head_entry) = plans.get_mut(&old_head_hash) {
                old_head_entry.prev = Some(hash);
            }
        }

        *self.head.write().unwrap() = Some(hash);
    }

    /// Get top N most executed plans
    pub fn top_plans(&self, n: usize) -> Vec<CachedPlan> {
        let plans = self.plans.read().unwrap();
        let mut sorted: Vec<_> = plans.values().map(|e| &e.plan).collect();
        sorted.sort_by(|a, b| b.execution_count.cmp(&a.execution_count));
        sorted.into_iter().take(n).cloned().collect()
    }
}

// ============================================================================
// Slow Query Log
// ============================================================================

/// Slow query log configuration
#[derive(Clone, Debug)]
pub struct SlowQueryLogConfig {
    /// Enable slow query logging
    pub enabled: bool,
    /// Threshold for slow queries
    pub threshold: Duration,
    /// Maximum entries to keep in memory
    pub max_entries: usize,
    /// Log to file
    pub log_to_file: bool,
    /// Log file path
    pub log_file_path: Option<String>,
    /// Include query plan
    pub include_plan: bool,
    /// Include bind parameters
    pub include_params: bool,
    /// Capture stack traces
    pub capture_stack_trace: bool,
    /// Alert threshold (trigger alert if exceeded)
    pub alert_threshold: Option<Duration>,
}

impl Default for SlowQueryLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: Duration::from_secs(1),
            max_entries: 1000,
            log_to_file: false,
            log_file_path: None,
            include_plan: true,
            include_params: false,
            capture_stack_trace: false,
            alert_threshold: Some(Duration::from_secs(10)),
        }
    }
}

/// Slow query entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    /// Entry ID
    pub id: u64,
    /// Query SQL
    pub sql: String,
    /// Query fingerprint
    pub fingerprint_hash: u64,
    /// Execution time
    pub execution_time: Duration,
    /// Timestamp
    pub timestamp: u64,
    /// Database
    pub database: Option<String>,
    /// User
    pub user: Option<String>,
    /// Client address
    pub client_addr: Option<String>,
    /// Rows examined
    pub rows_examined: u64,
    /// Rows returned
    pub rows_returned: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Query plan (if captured)
    pub plan: Option<String>,
    /// Parameters (if captured)
    pub params: Option<Vec<String>>,
    /// Lock time
    pub lock_time: Option<Duration>,
    /// Temp tables created
    pub temp_tables: u32,
    /// Full scan performed
    pub full_scan: bool,
    /// Index used
    pub index_used: bool,
}

/// Slow query alert
#[derive(Clone, Debug)]
pub struct SlowQueryAlert {
    pub entry: SlowQueryEntry,
    pub alert_type: AlertType,
    pub message: String,
}

/// Alert type
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlertType {
    SlowQuery,
    VerySlowQuery,
    FullTableScan,
    NoIndexUsed,
    ExcessiveLocking,
}

/// Slow query log
pub struct SlowQueryLog {
    config: SlowQueryLogConfig,
    /// Log entries
    entries: RwLock<VecDeque<SlowQueryEntry>>,
    /// Entry counter
    entry_counter: AtomicU64,
    /// Statistics
    stats: RwLock<SlowQueryStats>,
    /// Alert handlers
    alert_handlers: RwLock<Vec<Arc<dyn Fn(SlowQueryAlert) + Send + Sync>>>,
}

/// Slow query statistics
#[derive(Clone, Debug, Default)]
pub struct SlowQueryStats {
    pub total_slow_queries: u64,
    pub total_execution_time: Duration,
    pub avg_execution_time: Duration,
    pub max_execution_time: Duration,
    pub queries_by_fingerprint: HashMap<u64, u64>,
    pub full_scans: u64,
    pub no_index_queries: u64,
}

impl SlowQueryLog {
    pub fn new(config: SlowQueryLogConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(VecDeque::new()),
            entry_counter: AtomicU64::new(0),
            stats: RwLock::new(SlowQueryStats::default()),
            alert_handlers: RwLock::new(Vec::new()),
        }
    }

    /// Log a query execution
    pub fn log_query(
        &self,
        sql: &str,
        execution_time: Duration,
        metadata: QueryExecutionMetadata,
    ) {
        if !self.config.enabled {
            return;
        }

        // Check if slow
        if execution_time < self.config.threshold {
            return;
        }

        let fingerprint = QueryFingerprint::from_sql(sql, metadata.database.as_deref());
        let id = self.entry_counter.fetch_add(1, Ordering::SeqCst);

        let entry = SlowQueryEntry {
            id,
            sql: sql.to_string(),
            fingerprint_hash: fingerprint.hash,
            execution_time,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            database: metadata.database,
            user: metadata.user,
            client_addr: metadata.client_addr,
            rows_examined: metadata.rows_examined,
            rows_returned: metadata.rows_returned,
            bytes_sent: metadata.bytes_sent,
            plan: if self.config.include_plan { metadata.plan } else { None },
            params: if self.config.include_params { metadata.params } else { None },
            lock_time: metadata.lock_time,
            temp_tables: metadata.temp_tables,
            full_scan: metadata.full_scan,
            index_used: metadata.index_used,
        };

        // Add to log
        {
            let mut entries = self.entries.write().unwrap();
            entries.push_back(entry.clone());

            // Enforce max entries
            while entries.len() > self.config.max_entries {
                entries.pop_front();
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_slow_queries += 1;
            stats.total_execution_time += execution_time;

            if execution_time > stats.max_execution_time {
                stats.max_execution_time = execution_time;
            }

            stats.avg_execution_time = stats.total_execution_time / stats.total_slow_queries as u32;

            *stats.queries_by_fingerprint
                .entry(fingerprint.hash)
                .or_insert(0) += 1;

            if entry.full_scan {
                stats.full_scans += 1;
            }

            if !entry.index_used {
                stats.no_index_queries += 1;
            }
        }

        // Check alerts
        self.check_alerts(&entry);
    }

    fn check_alerts(&self, entry: &SlowQueryEntry) {
        let mut alerts = Vec::new();

        // Very slow query alert
        if let Some(alert_threshold) = self.config.alert_threshold {
            if entry.execution_time > alert_threshold {
                alerts.push(SlowQueryAlert {
                    entry: entry.clone(),
                    alert_type: AlertType::VerySlowQuery,
                    message: format!(
                        "Query exceeded alert threshold: {:?} > {:?}",
                        entry.execution_time, alert_threshold
                    ),
                });
            }
        }

        // Full table scan alert
        if entry.full_scan && entry.rows_examined > 10000 {
            alerts.push(SlowQueryAlert {
                entry: entry.clone(),
                alert_type: AlertType::FullTableScan,
                message: format!(
                    "Full table scan on {} rows",
                    entry.rows_examined
                ),
            });
        }

        // No index used alert
        if !entry.index_used && entry.rows_examined > 1000 {
            alerts.push(SlowQueryAlert {
                entry: entry.clone(),
                alert_type: AlertType::NoIndexUsed,
                message: "Query did not use an index".to_string(),
            });
        }

        // Fire alerts
        let handlers = self.alert_handlers.read().unwrap();
        for alert in alerts {
            for handler in handlers.iter() {
                handler(alert.clone());
            }
        }
    }

    /// Register alert handler
    pub fn on_alert<F>(&self, handler: F)
    where
        F: Fn(SlowQueryAlert) + Send + Sync + 'static,
    {
        self.alert_handlers.write().unwrap().push(Arc::new(handler));
    }

    /// Get recent slow queries
    pub fn get_recent(&self, limit: usize) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read().unwrap();
        entries.iter().rev().take(limit).cloned().collect()
    }

    /// Get slow queries by fingerprint
    pub fn get_by_fingerprint(&self, fingerprint_hash: u64) -> Vec<SlowQueryEntry> {
        let entries = self.entries.read().unwrap();
        entries.iter()
            .filter(|e| e.fingerprint_hash == fingerprint_hash)
            .cloned()
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> SlowQueryStats {
        self.stats.read().unwrap().clone()
    }

    /// Get top slow query patterns
    pub fn top_patterns(&self, n: usize) -> Vec<(u64, u64, Duration)> {
        let entries = self.entries.read().unwrap();
        let mut patterns: HashMap<u64, (u64, Duration)> = HashMap::new();

        for entry in entries.iter() {
            let (count, time) = patterns
                .entry(entry.fingerprint_hash)
                .or_insert((0, Duration::ZERO));
            *count += 1;
            *time += entry.execution_time;
        }

        let mut sorted: Vec<_> = patterns.into_iter()
            .map(|(hash, (count, time))| (hash, count, time))
            .collect();

        sorted.sort_by(|a, b| b.2.cmp(&a.2)); // Sort by total time
        sorted.truncate(n);
        sorted
    }

    /// Clear log
    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
    }

    /// Export to JSON
    pub fn export_json(&self) -> String {
        let entries = self.entries.read().unwrap();
        serde_json::to_string_pretty(&*entries).unwrap_or_default()
    }
}

/// Query execution metadata
#[derive(Clone, Debug, Default)]
pub struct QueryExecutionMetadata {
    pub database: Option<String>,
    pub user: Option<String>,
    pub client_addr: Option<String>,
    pub rows_examined: u64,
    pub rows_returned: u64,
    pub bytes_sent: u64,
    pub plan: Option<String>,
    pub params: Option<Vec<String>>,
    pub lock_time: Option<Duration>,
    pub temp_tables: u32,
    pub full_scan: bool,
    pub index_used: bool,
}

// ============================================================================
// Query Performance Analyzer
// ============================================================================

/// Query performance analyzer
pub struct QueryPerformanceAnalyzer {
    plan_cache: Arc<PlanCache>,
    slow_query_log: Arc<SlowQueryLog>,
    /// Query execution history
    execution_history: RwLock<HashMap<u64, QueryExecutionHistory>>,
}

/// Execution history for a query pattern
#[derive(Clone, Debug, Default)]
pub struct QueryExecutionHistory {
    pub fingerprint_hash: u64,
    pub sample_sql: String,
    pub execution_count: u64,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub p50_time: Duration,
    pub p95_time: Duration,
    pub p99_time: Duration,
    /// Recent execution times for percentile calculation
    recent_times: VecDeque<Duration>,
}

impl QueryExecutionHistory {
    fn record(&mut self, execution_time: Duration) {
        self.execution_count += 1;
        self.total_time += execution_time;
        self.avg_time = self.total_time / self.execution_count as u32;

        if execution_time < self.min_time || self.min_time == Duration::ZERO {
            self.min_time = execution_time;
        }

        if execution_time > self.max_time {
            self.max_time = execution_time;
        }

        // Keep recent times for percentiles
        self.recent_times.push_back(execution_time);
        while self.recent_times.len() > 1000 {
            self.recent_times.pop_front();
        }

        // Calculate percentiles
        if !self.recent_times.is_empty() {
            let mut sorted: Vec<_> = self.recent_times.iter().copied().collect();
            sorted.sort();

            let len = sorted.len();
            self.p50_time = sorted[len / 2];
            self.p95_time = sorted[(len as f64 * 0.95) as usize];
            self.p99_time = sorted[(len as f64 * 0.99) as usize];
        }
    }
}

/// Performance recommendation
#[derive(Clone, Debug)]
pub struct PerformanceRecommendation {
    pub fingerprint_hash: u64,
    pub sample_sql: String,
    pub recommendation_type: RecommendationType,
    pub description: String,
    pub impact: ImpactLevel,
    pub suggested_action: String,
}

/// Recommendation type
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecommendationType {
    AddIndex,
    RewriteQuery,
    AddCaching,
    IncreaseResources,
    PartitionTable,
    OptimizeJoin,
}

/// Impact level
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImpactLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl QueryPerformanceAnalyzer {
    pub fn new(plan_cache: Arc<PlanCache>, slow_query_log: Arc<SlowQueryLog>) -> Self {
        Self {
            plan_cache,
            slow_query_log,
            execution_history: RwLock::new(HashMap::new()),
        }
    }

    /// Record query execution
    pub fn record_execution(
        &self,
        sql: &str,
        execution_time: Duration,
        metadata: QueryExecutionMetadata,
    ) {
        let fingerprint = QueryFingerprint::from_sql(sql, metadata.database.as_deref());

        // Update execution history
        {
            let mut history = self.execution_history.write().unwrap();
            let entry = history.entry(fingerprint.hash).or_insert_with(|| {
                QueryExecutionHistory {
                    fingerprint_hash: fingerprint.hash,
                    sample_sql: sql.to_string(),
                    ..Default::default()
                }
            });
            entry.record(execution_time);
        }

        // Update plan cache
        self.plan_cache.record_execution(&fingerprint, execution_time);

        // Log slow queries
        self.slow_query_log.log_query(sql, execution_time, metadata);
    }

    /// Analyze query patterns and generate recommendations
    pub fn analyze(&self) -> Vec<PerformanceRecommendation> {
        let mut recommendations = Vec::new();

        let history = self.execution_history.read().unwrap();
        let slow_stats = self.slow_query_log.stats();

        // Analyze slow query patterns
        for (hash, entry) in history.iter() {
            // Check for consistently slow queries
            if entry.p95_time > Duration::from_millis(500) {
                recommendations.push(PerformanceRecommendation {
                    fingerprint_hash: *hash,
                    sample_sql: entry.sample_sql.clone(),
                    recommendation_type: RecommendationType::RewriteQuery,
                    description: format!(
                        "Query has P95 latency of {:?}",
                        entry.p95_time
                    ),
                    impact: if entry.p95_time > Duration::from_secs(1) {
                        ImpactLevel::High
                    } else {
                        ImpactLevel::Medium
                    },
                    suggested_action: "Consider query optimization or adding indexes".to_string(),
                });
            }

            // Check for high execution count
            if entry.execution_count > 10000 && entry.avg_time > Duration::from_millis(10) {
                recommendations.push(PerformanceRecommendation {
                    fingerprint_hash: *hash,
                    sample_sql: entry.sample_sql.clone(),
                    recommendation_type: RecommendationType::AddCaching,
                    description: format!(
                        "Frequently executed query ({} times) with avg latency {:?}",
                        entry.execution_count, entry.avg_time
                    ),
                    impact: ImpactLevel::Medium,
                    suggested_action: "Consider caching query results".to_string(),
                });
            }

            // Check for high variance
            if entry.max_time > entry.avg_time * 10 {
                recommendations.push(PerformanceRecommendation {
                    fingerprint_hash: *hash,
                    sample_sql: entry.sample_sql.clone(),
                    recommendation_type: RecommendationType::OptimizeJoin,
                    description: format!(
                        "Query has high latency variance (max {:?} vs avg {:?})",
                        entry.max_time, entry.avg_time
                    ),
                    impact: ImpactLevel::Low,
                    suggested_action: "Investigate query plan stability".to_string(),
                });
            }
        }

        // Check for full scans
        if slow_stats.full_scans > 0 {
            let ratio = slow_stats.full_scans as f64 / slow_stats.total_slow_queries.max(1) as f64;
            if ratio > 0.1 {
                recommendations.push(PerformanceRecommendation {
                    fingerprint_hash: 0,
                    sample_sql: String::new(),
                    recommendation_type: RecommendationType::AddIndex,
                    description: format!(
                        "{:.0}% of slow queries perform full table scans",
                        ratio * 100.0
                    ),
                    impact: ImpactLevel::High,
                    suggested_action: "Review queries and add appropriate indexes".to_string(),
                });
            }
        }

        // Sort by impact
        recommendations.sort_by(|a, b| {
            let impact_order = |i: &ImpactLevel| match i {
                ImpactLevel::Critical => 0,
                ImpactLevel::High => 1,
                ImpactLevel::Medium => 2,
                ImpactLevel::Low => 3,
            };
            impact_order(&a.impact).cmp(&impact_order(&b.impact))
        });

        recommendations
    }

    /// Get execution history for a query pattern
    pub fn get_history(&self, fingerprint_hash: u64) -> Option<QueryExecutionHistory> {
        self.execution_history.read().unwrap()
            .get(&fingerprint_hash)
            .cloned()
    }

    /// Get top queries by total time
    pub fn top_queries_by_time(&self, n: usize) -> Vec<QueryExecutionHistory> {
        let history = self.execution_history.read().unwrap();
        let mut entries: Vec<_> = history.values().cloned().collect();
        entries.sort_by(|a, b| b.total_time.cmp(&a.total_time));
        entries.truncate(n);
        entries
    }

    /// Get top queries by execution count
    pub fn top_queries_by_count(&self, n: usize) -> Vec<QueryExecutionHistory> {
        let history = self.execution_history.read().unwrap();
        let mut entries: Vec<_> = history.values().cloned().collect();
        entries.sort_by(|a, b| b.execution_count.cmp(&a.execution_count));
        entries.truncate(n);
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_fingerprint() {
        let fp1 = QueryFingerprint::from_sql(
            "SELECT * FROM users WHERE id = 123",
            Some("test_db"),
        );
        let fp2 = QueryFingerprint::from_sql(
            "SELECT * FROM users WHERE id = 456",
            Some("test_db"),
        );

        // Same template, different literals
        assert_eq!(fp1.hash, fp2.hash);
        assert!(fp1.template.contains("?"));
    }

    #[test]
    fn test_plan_cache() {
        let cache = PlanCache::new(PlanCacheConfig {
            max_entries: 10,
            ..Default::default()
        });

        let fingerprint = QueryFingerprint::from_sql("SELECT 1", None);

        // Cache miss
        assert!(cache.get(&fingerprint).is_none());

        // Cache put
        cache.put(
            fingerprint.clone(),
            "SELECT 1".to_string(),
            vec![1, 2, 3],
            vec![],
            vec![],
            1.0,
        );

        // Cache hit
        let plan = cache.get(&fingerprint);
        assert!(plan.is_some());
        assert_eq!(plan.unwrap().execution_count, 2);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_slow_query_log() {
        let log = SlowQueryLog::new(SlowQueryLogConfig {
            threshold: Duration::from_millis(100),
            ..Default::default()
        });

        // Fast query - not logged
        log.log_query(
            "SELECT 1",
            Duration::from_millis(10),
            QueryExecutionMetadata::default(),
        );

        // Slow query - logged
        log.log_query(
            "SELECT * FROM big_table",
            Duration::from_millis(500),
            QueryExecutionMetadata {
                rows_examined: 100000,
                full_scan: true,
                ..Default::default()
            },
        );

        let entries = log.get_recent(10);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].full_scan);

        let stats = log.stats();
        assert_eq!(stats.total_slow_queries, 1);
        assert_eq!(stats.full_scans, 1);
    }

    #[test]
    fn test_query_performance_analyzer() {
        let cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));
        let log = Arc::new(SlowQueryLog::new(SlowQueryLogConfig::default()));
        let analyzer = QueryPerformanceAnalyzer::new(cache, log);

        // Record some executions
        for i in 0..100 {
            analyzer.record_execution(
                "SELECT * FROM users WHERE id = ?",
                Duration::from_millis(50 + (i % 100)),
                QueryExecutionMetadata::default(),
            );
        }

        let history = analyzer.get_history(
            QueryFingerprint::from_sql("SELECT * FROM users WHERE id = 1", None).hash
        );
        assert!(history.is_some());
        assert_eq!(history.unwrap().execution_count, 100);

        let top = analyzer.top_queries_by_count(5);
        assert!(!top.is_empty());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = PlanCache::new(PlanCacheConfig {
            max_entries: 3,
            max_memory_bytes: 1024 * 1024,
            ..Default::default()
        });

        // Add 5 entries with different table names (not just numbers, which normalize to same query)
        // Note: normalize_sql replaces numbers with ?, so SELECT 0, SELECT 1 etc all become SELECT ?
        let tables = ["users", "orders", "products", "customers", "inventory"];
        for table in &tables {
            let sql = format!("SELECT * FROM {}", table);
            let fp = QueryFingerprint::from_sql(&sql, None);
            cache.put(fp, sql, vec![], vec![], vec![], 1.0);
        }

        let stats = cache.stats();
        assert_eq!(stats.total_plans, 3);
        assert_eq!(stats.evictions, 2);

        // First entries (users, orders) should be evicted
        let fp_users = QueryFingerprint::from_sql("SELECT * FROM users", None);
        assert!(cache.get(&fp_users).is_none());

        // Last entry (inventory) should exist
        let fp_inv = QueryFingerprint::from_sql("SELECT * FROM inventory", None);
        assert!(cache.get(&fp_inv).is_some());
    }
}
