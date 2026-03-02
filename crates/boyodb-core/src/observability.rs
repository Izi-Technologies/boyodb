// Phase 10: Observability
//
// System Tables, Query Cache, and Prometheus Metrics for production monitoring
// and debugging via system tables.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

// ============================================================================
// System Tables
// ============================================================================

/// Types of system tables available
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemTable {
    QueryLog,
    QueryThreadLog,
    PartLog,
    ProcessList,
    Metrics,
    Events,
    AsynchronousMetrics,
    Merges,
    Mutations,
    Replicas,
    ReplicationQueue,
    Databases,
    Tables,
    Columns,
    Parts,
    Disks,
    StoragePolicies,
    Users,
    Roles,
    Grants,
    Settings,
    Clusters,
    Functions,
    Errors,
}

impl SystemTable {
    pub fn name(&self) -> &'static str {
        match self {
            Self::QueryLog => "query_log",
            Self::QueryThreadLog => "query_thread_log",
            Self::PartLog => "part_log",
            Self::ProcessList => "processes",
            Self::Metrics => "metrics",
            Self::Events => "events",
            Self::AsynchronousMetrics => "asynchronous_metrics",
            Self::Merges => "merges",
            Self::Mutations => "mutations",
            Self::Replicas => "replicas",
            Self::ReplicationQueue => "replication_queue",
            Self::Databases => "databases",
            Self::Tables => "tables",
            Self::Columns => "columns",
            Self::Parts => "parts",
            Self::Disks => "disks",
            Self::StoragePolicies => "storage_policies",
            Self::Users => "users",
            Self::Roles => "roles",
            Self::Grants => "grants",
            Self::Settings => "settings",
            Self::Clusters => "clusters",
            Self::Functions => "functions",
            Self::Errors => "errors",
        }
    }

    pub fn all() -> &'static [SystemTable] {
        &[
            Self::QueryLog, Self::QueryThreadLog, Self::PartLog, Self::ProcessList,
            Self::Metrics, Self::Events, Self::AsynchronousMetrics, Self::Merges,
            Self::Mutations, Self::Replicas, Self::ReplicationQueue, Self::Databases,
            Self::Tables, Self::Columns, Self::Parts, Self::Disks, Self::StoragePolicies,
            Self::Users, Self::Roles, Self::Grants, Self::Settings, Self::Clusters,
            Self::Functions, Self::Errors,
        ]
    }
}

/// Query log entry
#[derive(Debug, Clone)]
pub struct QueryLogEntry {
    pub query_id: String,
    pub query: String,
    pub query_kind: QueryKind,
    pub event_time: i64,
    pub event_time_microseconds: i64,
    pub query_start_time: i64,
    pub query_duration_ms: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub memory_usage: u64,
    pub peak_memory_usage: u64,
    pub thread_ids: Vec<u64>,
    pub profile_counters: HashMap<String, u64>,
    pub settings: HashMap<String, String>,
    pub user: String,
    pub client_hostname: String,
    pub client_name: String,
    pub database: String,
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub exception_code: Option<i32>,
    pub exception: Option<String>,
    pub stack_trace: Option<String>,
    pub is_initial_query: bool,
    pub query_type: QueryType,
    pub normalized_query_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    System,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Initial,
    Secondary,
}

/// Part log entry (for MergeTree operations)
#[derive(Debug, Clone)]
pub struct PartLogEntry {
    pub event_type: PartEventType,
    pub event_time: i64,
    pub duration_ms: u64,
    pub database: String,
    pub table: String,
    pub part_name: String,
    pub partition_id: String,
    pub rows: u64,
    pub size_in_bytes: u64,
    pub merged_from: Vec<String>,
    pub bytes_uncompressed: u64,
    pub bytes_compressed: u64,
    pub compression_ratio: f64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartEventType {
    NewPart,
    MergeParts,
    MutatePart,
    MovePart,
    RemovePart,
    DownloadPart,
}

/// Process list entry (running queries)
#[derive(Debug, Clone)]
pub struct ProcessEntry {
    pub query_id: String,
    pub user: String,
    pub address: String,
    pub port: u16,
    pub elapsed_ms: u64,
    pub is_cancelled: bool,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub total_rows_approx: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub memory_usage: u64,
    pub peak_memory_usage: u64,
    pub query: String,
    pub thread_ids: Vec<u64>,
    pub os_thread_ids: Vec<u64>,
    pub query_kind: QueryKind,
}

/// Metric definition
#[derive(Debug, Clone)]
pub struct MetricDefinition {
    pub name: String,
    pub value: i64,
    pub description: String,
}

/// Async metric (gauges updated periodically)
#[derive(Debug, Clone)]
pub struct AsyncMetric {
    pub metric: String,
    pub value: f64,
    pub description: String,
}

/// Error entry
#[derive(Debug, Clone)]
pub struct ErrorEntry {
    pub name: String,
    pub code: i32,
    pub value: u64,
    pub last_error_time: Option<i64>,
    pub last_error_message: Option<String>,
    pub last_error_trace: Option<String>,
    pub remote: bool,
}

/// System tables manager
pub struct SystemTablesManager {
    query_log: RwLock<VecDeque<QueryLogEntry>>,
    part_log: RwLock<VecDeque<PartLogEntry>>,
    process_list: RwLock<HashMap<String, ProcessEntry>>,
    metrics: RwLock<HashMap<String, AtomicI64Wrapper>>,
    events: RwLock<HashMap<String, AtomicU64>>,
    async_metrics: RwLock<HashMap<String, f64>>,
    errors: RwLock<HashMap<String, ErrorEntry>>,
    max_log_entries: usize,
    metric_descriptions: RwLock<HashMap<String, String>>,
}

// Wrapper to make AtomicI64 work with RwLock
struct AtomicI64Wrapper(std::sync::atomic::AtomicI64);

impl AtomicI64Wrapper {
    fn new(val: i64) -> Self {
        Self(std::sync::atomic::AtomicI64::new(val))
    }

    fn load(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }

    fn store(&self, val: i64) {
        self.0.store(val, Ordering::Relaxed);
    }

    fn fetch_add(&self, val: i64) -> i64 {
        self.0.fetch_add(val, Ordering::Relaxed)
    }
}

impl SystemTablesManager {
    pub fn new(max_log_entries: usize) -> Self {
        let manager = Self {
            query_log: RwLock::new(VecDeque::with_capacity(max_log_entries)),
            part_log: RwLock::new(VecDeque::with_capacity(max_log_entries)),
            process_list: RwLock::new(HashMap::new()),
            metrics: RwLock::new(HashMap::new()),
            events: RwLock::new(HashMap::new()),
            async_metrics: RwLock::new(HashMap::new()),
            errors: RwLock::new(HashMap::new()),
            max_log_entries,
            metric_descriptions: RwLock::new(HashMap::new()),
        };

        // Initialize standard metrics
        manager.init_standard_metrics();
        manager
    }

    fn init_standard_metrics(&self) {
        let metrics = [
            ("Query", "Number of executing queries"),
            ("Merge", "Number of executing background merges"),
            ("PartMutation", "Number of mutations in progress"),
            ("ReplicatedFetch", "Number of data parts being fetched"),
            ("ReplicatedSend", "Number of data parts being sent"),
            ("ReplicatedChecks", "Number of data parts checking for consistency"),
            ("BackgroundPoolTask", "Number of active background pool tasks"),
            ("BackgroundMovePoolTask", "Number of active background move tasks"),
            ("BackgroundSchedulePoolTask", "Number of active scheduled tasks"),
            ("DiskSpaceReservedForMerge", "Disk space reserved for running merges"),
            ("DistributedSend", "Number of connections sending data to remote servers"),
            ("QueryThread", "Number of query processing threads"),
            ("ReadonlyReplica", "Number of replicas in readonly state"),
            ("MemoryTracking", "Total memory allocated for queries"),
            ("ContextLockWait", "Number of threads waiting for context lock"),
            ("StorageBufferRows", "Number of rows in Buffer tables"),
            ("StorageBufferBytes", "Number of bytes in Buffer tables"),
            ("DictCacheRequests", "Number of requests to dictionary cache"),
            ("Revision", "Server revision number"),
            ("VersionInteger", "Server version as integer"),
            ("MaxPartCountForPartition", "Maximum active parts per partition"),
            ("TCPConnection", "Number of TCP connections"),
            ("HTTPConnection", "Number of HTTP connections"),
            ("OpenFileForRead", "Number of files open for reading"),
            ("OpenFileForWrite", "Number of files open for writing"),
        ];

        let mut descs = self.metric_descriptions.write();
        let mut m = self.metrics.write();
        for (name, desc) in metrics {
            m.insert(name.to_string(), AtomicI64Wrapper::new(0));
            descs.insert(name.to_string(), desc.to_string());
        }
    }

    /// Log a completed query
    pub fn log_query(&self, entry: QueryLogEntry) {
        let mut log = self.query_log.write();
        if log.len() >= self.max_log_entries {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Log a part event
    pub fn log_part(&self, entry: PartLogEntry) {
        let mut log = self.part_log.write();
        if log.len() >= self.max_log_entries {
            log.pop_front();
        }
        log.push_back(entry);
    }

    /// Register a running query in process list
    pub fn register_query(&self, entry: ProcessEntry) {
        self.process_list.write().insert(entry.query_id.clone(), entry);
    }

    /// Update a running query's progress
    pub fn update_query_progress(
        &self,
        query_id: &str,
        read_rows: u64,
        read_bytes: u64,
        memory_usage: u64,
    ) {
        if let Some(entry) = self.process_list.write().get_mut(query_id) {
            entry.read_rows = read_rows;
            entry.read_bytes = read_bytes;
            entry.memory_usage = memory_usage;
            if memory_usage > entry.peak_memory_usage {
                entry.peak_memory_usage = memory_usage;
            }
        }
    }

    /// Remove a query from process list
    pub fn unregister_query(&self, query_id: &str) {
        self.process_list.write().remove(query_id);
    }

    /// Cancel a running query
    pub fn cancel_query(&self, query_id: &str) -> bool {
        if let Some(entry) = self.process_list.write().get_mut(query_id) {
            entry.is_cancelled = true;
            true
        } else {
            false
        }
    }

    /// Check if a query is cancelled
    pub fn is_query_cancelled(&self, query_id: &str) -> bool {
        self.process_list.read()
            .get(query_id)
            .map(|e| e.is_cancelled)
            .unwrap_or(false)
    }

    /// Set a metric value
    pub fn set_metric(&self, name: &str, value: i64) {
        let metrics = self.metrics.read();
        if let Some(m) = metrics.get(name) {
            m.store(value);
        } else {
            drop(metrics);
            self.metrics.write().insert(name.to_string(), AtomicI64Wrapper::new(value));
        }
    }

    /// Increment a metric
    pub fn increment_metric(&self, name: &str, delta: i64) {
        let metrics = self.metrics.read();
        if let Some(m) = metrics.get(name) {
            m.fetch_add(delta);
        } else {
            drop(metrics);
            self.metrics.write().insert(name.to_string(), AtomicI64Wrapper::new(delta));
        }
    }

    /// Get a metric value
    pub fn get_metric(&self, name: &str) -> Option<i64> {
        self.metrics.read().get(name).map(|m| m.load())
    }

    /// Increment an event counter
    pub fn increment_event(&self, name: &str, count: u64) {
        let events = self.events.read();
        if let Some(e) = events.get(name) {
            e.fetch_add(count, Ordering::Relaxed);
        } else {
            drop(events);
            self.events.write().insert(name.to_string(), AtomicU64::new(count));
        }
    }

    /// Get event count
    pub fn get_event(&self, name: &str) -> u64 {
        self.events.read()
            .get(name)
            .map(|e| e.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Set async metric (called periodically)
    pub fn set_async_metric(&self, name: &str, value: f64) {
        self.async_metrics.write().insert(name.to_string(), value);
    }

    /// Record an error
    pub fn record_error(&self, name: &str, code: i32, message: &str, trace: Option<&str>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let mut errors = self.errors.write();
        let entry = errors.entry(name.to_string()).or_insert(ErrorEntry {
            name: name.to_string(),
            code,
            value: 0,
            last_error_time: None,
            last_error_message: None,
            last_error_trace: None,
            remote: false,
        });

        entry.value += 1;
        entry.last_error_time = Some(now);
        entry.last_error_message = Some(message.to_string());
        entry.last_error_trace = trace.map(|s| s.to_string());
    }

    /// Get query log entries
    pub fn get_query_log(&self, limit: usize) -> Vec<QueryLogEntry> {
        self.query_log.read().iter().rev().take(limit).cloned().collect()
    }

    /// Get part log entries
    pub fn get_part_log(&self, limit: usize) -> Vec<PartLogEntry> {
        self.part_log.read().iter().rev().take(limit).cloned().collect()
    }

    /// Get current process list
    pub fn get_process_list(&self) -> Vec<ProcessEntry> {
        self.process_list.read().values().cloned().collect()
    }

    /// Get all metrics
    pub fn get_metrics(&self) -> Vec<MetricDefinition> {
        let metrics = self.metrics.read();
        let descs = self.metric_descriptions.read();

        metrics.iter().map(|(name, val)| {
            MetricDefinition {
                name: name.clone(),
                value: val.load(),
                description: descs.get(name).cloned().unwrap_or_default(),
            }
        }).collect()
    }

    /// Get all async metrics
    pub fn get_async_metrics(&self) -> Vec<AsyncMetric> {
        self.async_metrics.read().iter().map(|(name, &value)| {
            AsyncMetric {
                metric: name.clone(),
                value,
                description: String::new(),
            }
        }).collect()
    }

    /// Get all events
    pub fn get_events(&self) -> Vec<(String, u64)> {
        self.events.read().iter().map(|(k, v)| {
            (k.clone(), v.load(Ordering::Relaxed))
        }).collect()
    }

    /// Get all errors
    pub fn get_errors(&self) -> Vec<ErrorEntry> {
        self.errors.read().values().cloned().collect()
    }

    /// Clear old log entries
    pub fn cleanup_logs(&self, max_age_secs: i64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let cutoff = now - max_age_secs;

        self.query_log.write().retain(|e| e.event_time >= cutoff);
        self.part_log.write().retain(|e| e.event_time >= cutoff);
    }
}

impl Default for SystemTablesManager {
    fn default() -> Self {
        Self::new(100_000)
    }
}

// ============================================================================
// Query Cache
// ============================================================================

/// Cached query result
#[derive(Debug)]
pub struct CachedResult {
    pub query_hash: u64,
    pub result: Vec<u8>, // Serialized result
    pub row_count: u64,
    pub byte_size: u64,
    pub created_at: Instant,
    pub last_accessed: RwLock<Instant>,
    pub access_count: AtomicU64,
    pub ttl: Duration,
}

impl CachedResult {
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }

    pub fn touch(&self) {
        *self.last_accessed.write() = Instant::now();
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Query cache configuration
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: u64,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Default TTL for cache entries
    pub default_ttl: Duration,
    /// Minimum query execution time to cache (ms)
    pub min_query_duration_ms: u64,
    /// Minimum result rows to cache
    pub min_query_rows: u64,
    /// Maximum result size to cache
    pub max_result_bytes: u64,
    /// Enable compression for cached results
    pub compress: bool,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024, // 1GB
            max_entries: 10000,
            default_ttl: Duration::from_secs(60),
            min_query_duration_ms: 100,
            min_query_rows: 1,
            max_result_bytes: 10 * 1024 * 1024, // 10MB per result
            compress: true,
        }
    }
}

/// Query cache statistics
#[derive(Debug, Clone, Default)]
pub struct QueryCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub size_bytes: u64,
    pub evictions: u64,
    pub inserts: u64,
    pub expired_evictions: u64,
}

/// Query cache for caching query results
pub struct QueryCache {
    cache: RwLock<HashMap<u64, Arc<CachedResult>>>,
    config: QueryCacheConfig,
    stats: RwLock<QueryCacheStats>,
    current_size: AtomicU64,
}

impl QueryCache {
    pub fn new(config: QueryCacheConfig) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(QueryCacheStats::default()),
            current_size: AtomicU64::new(0),
        }
    }

    /// Hash a query for cache lookup
    pub fn hash_query(query: &str, settings: &HashMap<String, String>) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        query.hash(&mut hasher);
        // Include relevant settings in hash
        for (k, v) in settings {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Try to get a cached result
    pub fn get(&self, query_hash: u64) -> Option<Arc<CachedResult>> {
        let cache = self.cache.read();
        if let Some(entry) = cache.get(&query_hash) {
            if entry.is_expired() {
                drop(cache);
                self.evict(query_hash);
                self.stats.write().misses += 1;
                self.stats.write().expired_evictions += 1;
                return None;
            }
            entry.touch();
            self.stats.write().hits += 1;
            return Some(Arc::clone(entry));
        }
        drop(cache);
        self.stats.write().misses += 1;
        None
    }

    /// Check if result is cacheable
    pub fn is_cacheable(&self, duration_ms: u64, row_count: u64, byte_size: u64) -> bool {
        duration_ms >= self.config.min_query_duration_ms
            && row_count >= self.config.min_query_rows
            && byte_size <= self.config.max_result_bytes
    }

    /// Insert a result into the cache
    pub fn insert(
        &self,
        query_hash: u64,
        result: Vec<u8>,
        row_count: u64,
        ttl: Option<Duration>,
    ) -> bool {
        let byte_size = result.len() as u64;

        if byte_size > self.config.max_result_bytes {
            return false;
        }

        // Evict if necessary
        self.evict_if_needed(byte_size);

        let entry = Arc::new(CachedResult {
            query_hash,
            result,
            row_count,
            byte_size,
            created_at: Instant::now(),
            last_accessed: RwLock::new(Instant::now()),
            access_count: AtomicU64::new(1),
            ttl: ttl.unwrap_or(self.config.default_ttl),
        });

        let mut cache = self.cache.write();

        // Update stats for replacement
        if let Some(old) = cache.get(&query_hash) {
            self.current_size.fetch_sub(old.byte_size, Ordering::Relaxed);
        }

        cache.insert(query_hash, entry);
        self.current_size.fetch_add(byte_size, Ordering::Relaxed);

        let mut stats = self.stats.write();
        stats.inserts += 1;
        stats.entries = cache.len();
        stats.size_bytes = self.current_size.load(Ordering::Relaxed);

        true
    }

    /// Evict a specific entry
    pub fn evict(&self, query_hash: u64) -> bool {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.remove(&query_hash) {
            self.current_size.fetch_sub(entry.byte_size, Ordering::Relaxed);
            self.stats.write().evictions += 1;
            true
        } else {
            false
        }
    }

    /// Evict entries if cache is too large
    fn evict_if_needed(&self, needed_bytes: u64) {
        let current = self.current_size.load(Ordering::Relaxed);
        let max_size = self.config.max_size_bytes;

        if current + needed_bytes <= max_size {
            let cache_len = self.cache.read().len();
            if cache_len < self.config.max_entries {
                return;
            }
        }

        // Evict expired entries first
        self.evict_expired();

        // If still over, evict LRU
        let mut cache = self.cache.write();
        while self.current_size.load(Ordering::Relaxed) + needed_bytes > max_size
            || cache.len() >= self.config.max_entries
        {
            // Find LRU entry
            let lru_hash = cache.iter()
                .min_by_key(|(_, v)| *v.last_accessed.read())
                .map(|(h, _)| *h);

            if let Some(hash) = lru_hash {
                if let Some(entry) = cache.remove(&hash) {
                    self.current_size.fetch_sub(entry.byte_size, Ordering::Relaxed);
                    self.stats.write().evictions += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Evict all expired entries
    pub fn evict_expired(&self) {
        let mut cache = self.cache.write();
        let expired: Vec<u64> = cache.iter()
            .filter(|(_, v)| v.is_expired())
            .map(|(k, _)| *k)
            .collect();

        for hash in expired {
            if let Some(entry) = cache.remove(&hash) {
                self.current_size.fetch_sub(entry.byte_size, Ordering::Relaxed);
                self.stats.write().expired_evictions += 1;
            }
        }
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        self.cache.write().clear();
        self.current_size.store(0, Ordering::Relaxed);
        let mut stats = self.stats.write();
        stats.entries = 0;
        stats.size_bytes = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> QueryCacheStats {
        let mut stats = self.stats.read().clone();
        stats.entries = self.cache.read().len();
        stats.size_bytes = self.current_size.load(Ordering::Relaxed);
        stats
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let stats = self.stats.read();
        let total = stats.hits + stats.misses;
        if total == 0 {
            0.0
        } else {
            stats.hits as f64 / total as f64
        }
    }

    /// Invalidate cache entries for a table
    pub fn invalidate_table(&self, _database: &str, _table: &str) {
        // In a real impl, we'd track which queries touch which tables
        // For now, this is a placeholder
        // TODO: Implement dependency tracking
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new(QueryCacheConfig::default())
    }
}

// ============================================================================
// Prometheus Metrics
// ============================================================================

/// Prometheus metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrometheusMetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// A Prometheus metric
#[derive(Debug, Clone)]
pub struct PrometheusMetric {
    pub name: String,
    pub metric_type: PrometheusMetricType,
    pub help: String,
    pub labels: HashMap<String, String>,
    pub value: f64,
    pub timestamp: Option<i64>,
}

/// Histogram bucket
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub le: f64, // Upper bound
    pub count: u64,
}

/// Prometheus histogram metric
#[derive(Debug, Clone)]
pub struct PrometheusHistogram {
    pub name: String,
    pub help: String,
    pub labels: HashMap<String, String>,
    pub buckets: Vec<HistogramBucket>,
    pub sum: f64,
    pub count: u64,
}

impl PrometheusHistogram {
    pub fn new(name: &str, help: &str, bucket_bounds: &[f64]) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            labels: HashMap::new(),
            buckets: bucket_bounds.iter().map(|&le| HistogramBucket { le, count: 0 }).collect(),
            sum: 0.0,
            count: 0,
        }
    }

    pub fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        for bucket in &mut self.buckets {
            if value <= bucket.le {
                bucket.count += 1;
            }
        }
    }

    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::new();
        let full_name = format!("{}_{}", prefix, self.name);

        output.push_str(&format!("# HELP {} {}\n", full_name, self.help));
        output.push_str(&format!("# TYPE {} histogram\n", full_name));

        let labels_str = self.format_labels();

        for bucket in &self.buckets {
            if labels_str.is_empty() {
                output.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n",
                    full_name, format_le(bucket.le), bucket.count));
            } else {
                output.push_str(&format!("{}_bucket{{{},le=\"{}\"}} {}\n",
                    full_name, labels_str, format_le(bucket.le), bucket.count));
            }
        }

        // +Inf bucket
        if labels_str.is_empty() {
            output.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", full_name, self.count));
        } else {
            output.push_str(&format!("{}_bucket{{{},le=\"+Inf\"}} {}\n", full_name, labels_str, self.count));
        }

        if labels_str.is_empty() {
            output.push_str(&format!("{}_sum {}\n", full_name, self.sum));
            output.push_str(&format!("{}_count {}\n", full_name, self.count));
        } else {
            output.push_str(&format!("{}_sum{{{}}} {}\n", full_name, labels_str, self.sum));
            output.push_str(&format!("{}_count{{{}}} {}\n", full_name, labels_str, self.count));
        }

        output
    }

    fn format_labels(&self) -> String {
        self.labels.iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn format_le(v: f64) -> String {
    if v == f64::INFINITY {
        "+Inf".to_string()
    } else {
        v.to_string()
    }
}

/// Prometheus metrics exporter
pub struct PrometheusExporter {
    prefix: String,
    counters: RwLock<HashMap<String, (f64, String, HashMap<String, String>)>>,
    gauges: RwLock<HashMap<String, (f64, String, HashMap<String, String>)>>,
    histograms: RwLock<HashMap<String, PrometheusHistogram>>,
    default_labels: HashMap<String, String>,
}

impl PrometheusExporter {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            default_labels: HashMap::new(),
        }
    }

    pub fn with_default_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.default_labels = labels;
        self
    }

    /// Register a counter
    pub fn register_counter(&self, name: &str, help: &str) {
        self.counters.write().insert(
            name.to_string(),
            (0.0, help.to_string(), HashMap::new()),
        );
    }

    /// Register a gauge
    pub fn register_gauge(&self, name: &str, help: &str) {
        self.gauges.write().insert(
            name.to_string(),
            (0.0, help.to_string(), HashMap::new()),
        );
    }

    /// Register a histogram
    pub fn register_histogram(&self, name: &str, help: &str, buckets: &[f64]) {
        self.histograms.write().insert(
            name.to_string(),
            PrometheusHistogram::new(name, help, buckets),
        );
    }

    /// Increment a counter
    pub fn inc_counter(&self, name: &str, value: f64) {
        if let Some(entry) = self.counters.write().get_mut(name) {
            entry.0 += value;
        }
    }

    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: f64) {
        if let Some(entry) = self.gauges.write().get_mut(name) {
            entry.0 = value;
        }
    }

    /// Observe a histogram value
    pub fn observe_histogram(&self, name: &str, value: f64) {
        if let Some(hist) = self.histograms.write().get_mut(name) {
            hist.observe(value);
        }
    }

    /// Export all metrics in Prometheus text format
    pub fn export(&self) -> String {
        let mut output = String::new();

        // Export counters
        for (name, (value, help, labels)) in self.counters.read().iter() {
            let full_name = format!("{}_{}", self.prefix, name);
            output.push_str(&format!("# HELP {} {}\n", full_name, help));
            output.push_str(&format!("# TYPE {} counter\n", full_name));

            let all_labels = self.merge_labels(labels);
            if all_labels.is_empty() {
                output.push_str(&format!("{} {}\n", full_name, value));
            } else {
                output.push_str(&format!("{}{{{}}} {}\n", full_name, all_labels, value));
            }
        }

        // Export gauges
        for (name, (value, help, labels)) in self.gauges.read().iter() {
            let full_name = format!("{}_{}", self.prefix, name);
            output.push_str(&format!("# HELP {} {}\n", full_name, help));
            output.push_str(&format!("# TYPE {} gauge\n", full_name));

            let all_labels = self.merge_labels(labels);
            if all_labels.is_empty() {
                output.push_str(&format!("{} {}\n", full_name, value));
            } else {
                output.push_str(&format!("{}{{{}}} {}\n", full_name, all_labels, value));
            }
        }

        // Export histograms
        for hist in self.histograms.read().values() {
            output.push_str(&hist.to_prometheus_format(&self.prefix));
        }

        output
    }

    fn merge_labels(&self, extra: &HashMap<String, String>) -> String {
        let mut all = self.default_labels.clone();
        all.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        all.iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Sync metrics from SystemTablesManager
    pub fn sync_from_system_tables(&self, system: &SystemTablesManager) {
        // Sync current metrics as gauges
        for metric in system.get_metrics() {
            self.gauges.write().insert(
                metric.name.clone(),
                (metric.value as f64, metric.description.clone(), HashMap::new()),
            );
        }

        // Sync events as counters
        for (name, count) in system.get_events() {
            let counter_name = format!("events_{}", name.to_lowercase());
            self.counters.write().insert(
                counter_name,
                (count as f64, format!("Event counter: {}", name), HashMap::new()),
            );
        }
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        let exporter = Self::new("boyodb");

        // Register standard metrics
        exporter.register_counter("queries_total", "Total number of queries executed");
        exporter.register_counter("queries_failed_total", "Total number of failed queries");
        exporter.register_counter("inserted_rows_total", "Total number of inserted rows");
        exporter.register_counter("inserted_bytes_total", "Total bytes of inserted data");
        exporter.register_counter("selected_rows_total", "Total number of selected rows");
        exporter.register_counter("selected_bytes_total", "Total bytes of selected data");
        exporter.register_counter("merge_total", "Total number of merges");
        exporter.register_counter("mutations_total", "Total number of mutations");

        exporter.register_gauge("running_queries", "Number of currently running queries");
        exporter.register_gauge("running_merges", "Number of currently running merges");
        exporter.register_gauge("memory_usage_bytes", "Current memory usage in bytes");
        exporter.register_gauge("active_connections", "Number of active connections");
        exporter.register_gauge("tables_count", "Number of tables");
        exporter.register_gauge("databases_count", "Number of databases");
        exporter.register_gauge("parts_count", "Number of active parts");
        exporter.register_gauge("replication_lag_seconds", "Replication lag in seconds");

        exporter.register_histogram(
            "query_duration_seconds",
            "Query execution time in seconds",
            &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
        );
        exporter.register_histogram(
            "query_result_rows",
            "Number of rows in query results",
            &[1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0],
        );
        exporter.register_histogram(
            "insert_batch_size",
            "Size of insert batches",
            &[1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0],
        );

        exporter
    }
}

// ============================================================================
// Observability Manager (combines all three)
// ============================================================================

/// Combined observability manager
pub struct ObservabilityManager {
    pub system_tables: Arc<SystemTablesManager>,
    pub query_cache: Arc<QueryCache>,
    pub prometheus: Arc<PrometheusExporter>,
}

impl ObservabilityManager {
    pub fn new(
        max_log_entries: usize,
        cache_config: QueryCacheConfig,
        prometheus_prefix: &str,
    ) -> Self {
        Self {
            system_tables: Arc::new(SystemTablesManager::new(max_log_entries)),
            query_cache: Arc::new(QueryCache::new(cache_config)),
            prometheus: Arc::new(PrometheusExporter::new(prometheus_prefix)),
        }
    }

    /// Record a query start
    pub fn query_started(&self, query_id: &str, query: &str, user: &str) {
        self.system_tables.increment_metric("Query", 1);
        self.prometheus.set_gauge("running_queries",
            self.system_tables.get_metric("Query").unwrap_or(0) as f64);

        let entry = ProcessEntry {
            query_id: query_id.to_string(),
            user: user.to_string(),
            address: String::new(),
            port: 0,
            elapsed_ms: 0,
            is_cancelled: false,
            read_rows: 0,
            read_bytes: 0,
            total_rows_approx: 0,
            written_rows: 0,
            written_bytes: 0,
            memory_usage: 0,
            peak_memory_usage: 0,
            query: query.to_string(),
            thread_ids: vec![],
            os_thread_ids: vec![],
            query_kind: QueryKind::Other,
        };

        self.system_tables.register_query(entry);
    }

    /// Record a query completion
    pub fn query_completed(&self, entry: QueryLogEntry) {
        self.system_tables.increment_metric("Query", -1);
        self.prometheus.set_gauge("running_queries",
            self.system_tables.get_metric("Query").unwrap_or(0) as f64);

        self.prometheus.inc_counter("queries_total", 1.0);
        self.prometheus.observe_histogram(
            "query_duration_seconds",
            entry.query_duration_ms as f64 / 1000.0,
        );
        self.prometheus.observe_histogram("query_result_rows", entry.result_rows as f64);

        if entry.exception.is_some() {
            self.prometheus.inc_counter("queries_failed_total", 1.0);
        }

        self.system_tables.unregister_query(&entry.query_id);
        self.system_tables.log_query(entry);
    }

    /// Export Prometheus metrics
    pub fn export_prometheus(&self) -> String {
        self.prometheus.sync_from_system_tables(&self.system_tables);
        self.prometheus.export()
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> QueryCacheStats {
        self.query_cache.stats()
    }
}

impl Default for ObservabilityManager {
    fn default() -> Self {
        Self {
            system_tables: Arc::new(SystemTablesManager::new(100_000)),
            query_cache: Arc::new(QueryCache::new(QueryCacheConfig::default())),
            prometheus: Arc::new(PrometheusExporter::default()),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // System Tables Tests

    #[test]
    fn test_system_table_names() {
        assert_eq!(SystemTable::QueryLog.name(), "query_log");
        assert_eq!(SystemTable::Parts.name(), "parts");
        assert_eq!(SystemTable::Metrics.name(), "metrics");
    }

    #[test]
    fn test_query_log() {
        let manager = SystemTablesManager::new(100);

        let entry = QueryLogEntry {
            query_id: "q1".to_string(),
            query: "SELECT 1".to_string(),
            query_kind: QueryKind::Select,
            event_time: 1000,
            event_time_microseconds: 1000000,
            query_start_time: 900,
            query_duration_ms: 100,
            read_rows: 0,
            read_bytes: 0,
            written_rows: 0,
            written_bytes: 0,
            result_rows: 1,
            result_bytes: 8,
            memory_usage: 1024,
            peak_memory_usage: 2048,
            thread_ids: vec![1],
            profile_counters: HashMap::new(),
            settings: HashMap::new(),
            user: "default".to_string(),
            client_hostname: "localhost".to_string(),
            client_name: "test".to_string(),
            database: "default".to_string(),
            tables: vec![],
            columns: vec![],
            exception_code: None,
            exception: None,
            stack_trace: None,
            is_initial_query: true,
            query_type: QueryType::Initial,
            normalized_query_hash: 12345,
        };

        manager.log_query(entry);

        let logs = manager.get_query_log(10);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].query_id, "q1");
    }

    #[test]
    fn test_part_log() {
        let manager = SystemTablesManager::new(100);

        let entry = PartLogEntry {
            event_type: PartEventType::NewPart,
            event_time: 1000,
            duration_ms: 50,
            database: "default".to_string(),
            table: "events".to_string(),
            part_name: "all_1_1_0".to_string(),
            partition_id: "all".to_string(),
            rows: 1000,
            size_in_bytes: 10000,
            merged_from: vec![],
            bytes_uncompressed: 50000,
            bytes_compressed: 10000,
            compression_ratio: 5.0,
            error: None,
        };

        manager.log_part(entry);

        let logs = manager.get_part_log(10);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].part_name, "all_1_1_0");
    }

    #[test]
    fn test_process_list() {
        let manager = SystemTablesManager::new(100);

        let entry = ProcessEntry {
            query_id: "q1".to_string(),
            user: "default".to_string(),
            address: "127.0.0.1".to_string(),
            port: 9000,
            elapsed_ms: 0,
            is_cancelled: false,
            read_rows: 0,
            read_bytes: 0,
            total_rows_approx: 1000,
            written_rows: 0,
            written_bytes: 0,
            memory_usage: 1024,
            peak_memory_usage: 1024,
            query: "SELECT * FROM events".to_string(),
            thread_ids: vec![1],
            os_thread_ids: vec![1001],
            query_kind: QueryKind::Select,
        };

        manager.register_query(entry);

        let processes = manager.get_process_list();
        assert_eq!(processes.len(), 1);
        assert_eq!(processes[0].query_id, "q1");

        manager.update_query_progress("q1", 500, 5000, 2048);
        let processes = manager.get_process_list();
        assert_eq!(processes[0].read_rows, 500);

        manager.cancel_query("q1");
        assert!(manager.is_query_cancelled("q1"));

        manager.unregister_query("q1");
        let processes = manager.get_process_list();
        assert!(processes.is_empty());
    }

    #[test]
    fn test_metrics() {
        let manager = SystemTablesManager::new(100);

        manager.set_metric("TestMetric", 42);
        assert_eq!(manager.get_metric("TestMetric"), Some(42));

        manager.increment_metric("TestMetric", 8);
        assert_eq!(manager.get_metric("TestMetric"), Some(50));

        let metrics = manager.get_metrics();
        assert!(metrics.iter().any(|m| m.name == "TestMetric" && m.value == 50));
    }

    #[test]
    fn test_events() {
        let manager = SystemTablesManager::new(100);

        manager.increment_event("SelectQuery", 5);
        manager.increment_event("SelectQuery", 3);

        assert_eq!(manager.get_event("SelectQuery"), 8);
        assert_eq!(manager.get_event("NonExistent"), 0);
    }

    #[test]
    fn test_errors() {
        let manager = SystemTablesManager::new(100);

        manager.record_error("SYNTAX_ERROR", 62, "Unexpected token", Some("at line 1"));
        manager.record_error("SYNTAX_ERROR", 62, "Another error", None);

        let errors = manager.get_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].value, 2); // Two errors recorded
    }

    // Query Cache Tests

    #[test]
    fn test_query_cache_basic() {
        let cache = QueryCache::new(QueryCacheConfig::default());

        let query = "SELECT * FROM events";
        let settings = HashMap::new();
        let hash = QueryCache::hash_query(query, &settings);

        // Insert
        let result = vec![1, 2, 3, 4, 5];
        assert!(cache.insert(hash, result.clone(), 1, None));

        // Get
        let cached = cache.get(hash).unwrap();
        assert_eq!(cached.result, result);
        assert_eq!(cached.row_count, 1);
    }

    #[test]
    fn test_query_cache_miss() {
        let cache = QueryCache::new(QueryCacheConfig::default());

        let result = cache.get(12345);
        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_query_cache_expiration() {
        let config = QueryCacheConfig {
            default_ttl: Duration::from_millis(10),
            ..Default::default()
        };
        let cache = QueryCache::new(config);

        let hash = 12345;
        cache.insert(hash, vec![1, 2, 3], 1, None);

        // Should hit before expiration
        assert!(cache.get(hash).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(20));

        // Should miss after expiration
        assert!(cache.get(hash).is_none());
    }

    #[test]
    fn test_query_cache_eviction() {
        let config = QueryCacheConfig {
            max_size_bytes: 100,
            max_entries: 3,
            ..Default::default()
        };
        let cache = QueryCache::new(config);

        cache.insert(1, vec![0; 30], 1, None);
        cache.insert(2, vec![0; 30], 1, None);
        cache.insert(3, vec![0; 30], 1, None);

        // This should trigger eviction
        cache.insert(4, vec![0; 30], 1, None);

        let stats = cache.stats();
        assert!(stats.entries <= 3);
    }

    #[test]
    fn test_query_cache_is_cacheable() {
        let cache = QueryCache::new(QueryCacheConfig {
            min_query_duration_ms: 100,
            min_query_rows: 10,
            max_result_bytes: 1000,
            ..Default::default()
        });

        // Too fast
        assert!(!cache.is_cacheable(50, 100, 500));

        // Too few rows
        assert!(!cache.is_cacheable(200, 5, 500));

        // Too large
        assert!(!cache.is_cacheable(200, 100, 2000));

        // OK
        assert!(cache.is_cacheable(200, 100, 500));
    }

    #[test]
    fn test_query_cache_stats() {
        let cache = QueryCache::new(QueryCacheConfig::default());

        cache.insert(1, vec![1, 2, 3], 1, None);
        cache.get(1); // Hit
        cache.get(1); // Hit
        cache.get(2); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn test_query_cache_hit_rate() {
        let cache = QueryCache::new(QueryCacheConfig::default());

        cache.insert(1, vec![1], 1, None);
        cache.get(1); // Hit
        cache.get(1); // Hit
        cache.get(2); // Miss
        cache.get(3); // Miss

        let hit_rate = cache.hit_rate();
        assert!((hit_rate - 0.5).abs() < 0.001);
    }

    // Prometheus Tests

    #[test]
    fn test_prometheus_counter() {
        let exporter = PrometheusExporter::new("test");

        exporter.register_counter("requests_total", "Total requests");
        exporter.inc_counter("requests_total", 5.0);
        exporter.inc_counter("requests_total", 3.0);

        let output = exporter.export();
        assert!(output.contains("test_requests_total 8"));
    }

    #[test]
    fn test_prometheus_gauge() {
        let exporter = PrometheusExporter::new("test");

        exporter.register_gauge("temperature", "Current temperature");
        exporter.set_gauge("temperature", 23.5);

        let output = exporter.export();
        assert!(output.contains("test_temperature 23.5"));
    }

    #[test]
    fn test_prometheus_histogram() {
        let exporter = PrometheusExporter::new("test");

        exporter.register_histogram("latency_seconds", "Request latency", &[0.1, 0.5, 1.0]);
        exporter.observe_histogram("latency_seconds", 0.3);
        exporter.observe_histogram("latency_seconds", 0.7);
        exporter.observe_histogram("latency_seconds", 0.05);

        let output = exporter.export();
        assert!(output.contains("test_latency_seconds_bucket"));
        assert!(output.contains("test_latency_seconds_sum"));
        assert!(output.contains("test_latency_seconds_count 3"));
    }

    #[test]
    fn test_prometheus_histogram_buckets() {
        let mut hist = PrometheusHistogram::new("test", "Test histogram", &[1.0, 5.0, 10.0]);

        hist.observe(0.5);  // <= 1, 5, 10
        hist.observe(3.0);  // <= 5, 10
        hist.observe(7.0);  // <= 10
        hist.observe(15.0); // > 10

        assert_eq!(hist.buckets[0].count, 1); // <= 1
        assert_eq!(hist.buckets[1].count, 2); // <= 5
        assert_eq!(hist.buckets[2].count, 3); // <= 10
        assert_eq!(hist.count, 4);
        assert!((hist.sum - 25.5).abs() < 0.001);
    }

    #[test]
    fn test_prometheus_labels() {
        let mut labels = HashMap::new();
        labels.insert("instance".to_string(), "localhost:9000".to_string());

        let exporter = PrometheusExporter::new("test").with_default_labels(labels);
        exporter.register_gauge("connections", "Active connections");
        exporter.set_gauge("connections", 42.0);

        let output = exporter.export();
        assert!(output.contains("instance=\"localhost:9000\""));
    }

    #[test]
    fn test_prometheus_default_exporter() {
        let exporter = PrometheusExporter::default();

        exporter.inc_counter("queries_total", 10.0);
        exporter.set_gauge("running_queries", 5.0);
        exporter.observe_histogram("query_duration_seconds", 0.1);

        let output = exporter.export();
        assert!(output.contains("boyodb_queries_total"));
        assert!(output.contains("boyodb_running_queries"));
        assert!(output.contains("boyodb_query_duration_seconds"));
    }

    // Observability Manager Tests

    #[test]
    fn test_observability_manager() {
        let manager = ObservabilityManager::default();

        manager.query_started("q1", "SELECT 1", "default");

        let processes = manager.system_tables.get_process_list();
        assert_eq!(processes.len(), 1);

        let entry = QueryLogEntry {
            query_id: "q1".to_string(),
            query: "SELECT 1".to_string(),
            query_kind: QueryKind::Select,
            event_time: 1000,
            event_time_microseconds: 1000000,
            query_start_time: 900,
            query_duration_ms: 100,
            read_rows: 0,
            read_bytes: 0,
            written_rows: 0,
            written_bytes: 0,
            result_rows: 1,
            result_bytes: 8,
            memory_usage: 1024,
            peak_memory_usage: 2048,
            thread_ids: vec![1],
            profile_counters: HashMap::new(),
            settings: HashMap::new(),
            user: "default".to_string(),
            client_hostname: "localhost".to_string(),
            client_name: "test".to_string(),
            database: "default".to_string(),
            tables: vec![],
            columns: vec![],
            exception_code: None,
            exception: None,
            stack_trace: None,
            is_initial_query: true,
            query_type: QueryType::Initial,
            normalized_query_hash: 12345,
        };

        manager.query_completed(entry);

        let processes = manager.system_tables.get_process_list();
        assert!(processes.is_empty());

        let logs = manager.system_tables.get_query_log(10);
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn test_prometheus_export() {
        let manager = ObservabilityManager::default();

        manager.system_tables.increment_event("SelectQuery", 100);
        manager.prometheus.inc_counter("queries_total", 50.0);

        let output = manager.export_prometheus();
        assert!(output.contains("boyodb_queries_total"));
    }

    #[test]
    fn test_log_rotation() {
        let manager = SystemTablesManager::new(5);

        for i in 0..10 {
            let entry = QueryLogEntry {
                query_id: format!("q{}", i),
                query: format!("SELECT {}", i),
                query_kind: QueryKind::Select,
                event_time: i as i64,
                event_time_microseconds: (i * 1000) as i64,
                query_start_time: i as i64,
                query_duration_ms: 10,
                read_rows: 0,
                read_bytes: 0,
                written_rows: 0,
                written_bytes: 0,
                result_rows: 1,
                result_bytes: 8,
                memory_usage: 0,
                peak_memory_usage: 0,
                thread_ids: vec![],
                profile_counters: HashMap::new(),
                settings: HashMap::new(),
                user: "default".to_string(),
                client_hostname: String::new(),
                client_name: String::new(),
                database: "default".to_string(),
                tables: vec![],
                columns: vec![],
                exception_code: None,
                exception: None,
                stack_trace: None,
                is_initial_query: true,
                query_type: QueryType::Initial,
                normalized_query_hash: i as u64,
            };
            manager.log_query(entry);
        }

        let logs = manager.get_query_log(100);
        assert_eq!(logs.len(), 5); // Max 5 entries
        assert_eq!(logs[0].query_id, "q9"); // Most recent first
    }

    #[test]
    fn test_async_metrics() {
        let manager = SystemTablesManager::new(100);

        manager.set_async_metric("cpu_usage", 45.5);
        manager.set_async_metric("memory_usage", 1024.0);

        let metrics = manager.get_async_metrics();
        assert_eq!(metrics.len(), 2);
        assert!(metrics.iter().any(|m| m.metric == "cpu_usage" && (m.value - 45.5).abs() < 0.001));
    }
}
