//! Query Result Caching
//!
//! Distributed cache layer with Redis-compatible protocol,
//! time-based and event-based invalidation.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// Cache Key and Entry
// ============================================================================

/// Cache key derived from query fingerprint
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    /// Query fingerprint (normalized SQL hash)
    pub fingerprint: u64,
    /// Database name
    pub database: String,
    /// Parameter hash (for parameterized queries)
    pub param_hash: u64,
    /// User/tenant ID for isolation
    pub tenant_id: Option<String>,
}

impl CacheKey {
    pub fn new(sql: &str, database: &str, params: &[CacheValue], tenant_id: Option<&str>) -> Self {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        normalize_sql(sql).hash(&mut hasher);
        let fingerprint = hasher.finish();

        let mut param_hasher = DefaultHasher::new();
        for p in params {
            p.hash(&mut param_hasher);
        }
        let param_hash = param_hasher.finish();

        Self {
            fingerprint,
            database: database.to_string(),
            param_hash,
            tenant_id: tenant_id.map(|s| s.to_string()),
        }
    }
}

/// Normalize SQL for fingerprinting (remove literals, whitespace)
fn normalize_sql(sql: &str) -> String {
    let mut result = String::new();
    let mut in_string = false;
    let mut in_number = false;
    let mut quote_char = ' ';

    for c in sql.chars() {
        if in_string {
            if c == quote_char {
                in_string = false;
                result.push('?');
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_string = true;
            quote_char = c;
            continue;
        }

        if c.is_ascii_digit() || (c == '.' && in_number) {
            if !in_number {
                in_number = true;
                result.push('?');
            }
            continue;
        }

        in_number = false;

        if c.is_whitespace() {
            if !result.ends_with(' ') && !result.is_empty() {
                result.push(' ');
            }
        } else {
            result.push(c.to_ascii_uppercase());
        }
    }

    result.trim().to_string()
}

/// Cached value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<CacheValue>),
}

impl Hash for CacheValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            CacheValue::Null => 0u8.hash(state),
            CacheValue::Bool(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            CacheValue::Int(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            CacheValue::Float(f) => {
                3u8.hash(state);
                f.to_bits().hash(state);
            }
            CacheValue::String(s) => {
                4u8.hash(state);
                s.hash(state);
            }
            CacheValue::Bytes(b) => {
                5u8.hash(state);
                b.hash(state);
            }
            CacheValue::Array(a) => {
                6u8.hash(state);
                for v in a {
                    v.hash(state);
                }
            }
        }
    }
}

/// Cached query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data
    pub rows: Vec<Vec<CacheValue>>,
    /// Execution time of original query (microseconds)
    pub original_exec_time_us: u64,
    /// Tables referenced by this query
    pub referenced_tables: Vec<String>,
}

/// Cache entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// The cached result
    pub result: CachedResult,
    /// When the entry was created
    pub created_at: Instant,
    /// Time-to-live
    pub ttl: Duration,
    /// Number of times this entry was accessed
    pub hit_count: u64,
    /// Size in bytes (approximate)
    pub size_bytes: usize,
    /// Last access time
    pub last_accessed: Instant,
}

impl CacheEntry {
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

// ============================================================================
// Cache Configuration
// ============================================================================

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    pub max_size_bytes: usize,
    /// Default TTL for entries
    pub default_ttl_secs: u64,
    /// Maximum TTL allowed
    pub max_ttl_secs: u64,
    /// Minimum query execution time to cache (microseconds)
    pub min_exec_time_us: u64,
    /// Maximum result size to cache (bytes)
    pub max_result_size_bytes: usize,
    /// Enable event-based invalidation
    pub enable_event_invalidation: bool,
    /// Number of cache shards for concurrency
    pub num_shards: usize,
    /// Enable distributed caching
    pub distributed: bool,
    /// Redis-compatible server address (for distributed mode)
    pub redis_addr: Option<String>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024,      // 1GB
            default_ttl_secs: 300,                   // 5 minutes
            max_ttl_secs: 3600,                      // 1 hour
            min_exec_time_us: 1000,                  // 1ms minimum
            max_result_size_bytes: 10 * 1024 * 1024, // 10MB max per result
            enable_event_invalidation: true,
            num_shards: 16,
            distributed: false,
            redis_addr: None,
        }
    }
}

// ============================================================================
// Cache Shard
// ============================================================================

/// A single cache shard
struct CacheShard {
    entries: HashMap<CacheKey, CacheEntry>,
    /// LRU queue (front = oldest)
    lru_queue: VecDeque<CacheKey>,
    /// Current size in bytes
    current_size: usize,
}

impl CacheShard {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru_queue: VecDeque::new(),
            current_size: 0,
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<&CacheEntry> {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.is_expired() {
                self.remove(key);
                return None;
            }
            entry.hit_count += 1;
            entry.last_accessed = Instant::now();
            // Move to back of LRU
            self.lru_queue.retain(|k| k != key);
            self.lru_queue.push_back(key.clone());
            return self.entries.get(key);
        }
        None
    }

    fn insert(&mut self, key: CacheKey, entry: CacheEntry, max_size: usize) {
        let entry_size = entry.size_bytes;

        // Evict if necessary
        while self.current_size + entry_size > max_size && !self.lru_queue.is_empty() {
            if let Some(oldest_key) = self.lru_queue.pop_front() {
                self.remove(&oldest_key);
            }
        }

        if entry_size <= max_size {
            self.current_size += entry_size;
            self.lru_queue.push_back(key.clone());
            self.entries.insert(key, entry);
        }
    }

    fn remove(&mut self, key: &CacheKey) -> Option<CacheEntry> {
        if let Some(entry) = self.entries.remove(key) {
            self.current_size = self.current_size.saturating_sub(entry.size_bytes);
            self.lru_queue.retain(|k| k != key);
            Some(entry)
        } else {
            None
        }
    }

    fn invalidate_by_table(&mut self, table: &str) {
        let keys_to_remove: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| e.result.referenced_tables.iter().any(|t| t == table))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            self.remove(&key);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.lru_queue.clear();
        self.current_size = 0;
    }
}

// ============================================================================
// Query Result Cache
// ============================================================================

/// Distributed query result cache
pub struct QueryResultCache {
    config: CacheConfig,
    shards: Vec<RwLock<CacheShard>>,
    /// Table -> set of dependent cache keys (for invalidation)
    table_dependencies: RwLock<HashMap<String, HashSet<CacheKey>>>,
    /// Statistics
    stats: CacheStats,
}

impl QueryResultCache {
    pub fn new(config: CacheConfig) -> Self {
        let num_shards = config.num_shards.max(1);
        let shards = (0..num_shards)
            .map(|_| RwLock::new(CacheShard::new()))
            .collect();

        Self {
            config,
            shards,
            table_dependencies: RwLock::new(HashMap::new()),
            stats: CacheStats::default(),
        }
    }

    fn get_shard_idx(&self, key: &CacheKey) -> usize {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    /// Get cached result
    pub fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        let shard_idx = self.get_shard_idx(key);
        let mut shard = self.shards[shard_idx].write();

        if let Some(entry) = shard.get(key) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.result.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Put result in cache
    pub fn put(&self, key: CacheKey, result: CachedResult, ttl: Option<Duration>) -> bool {
        // Check if result is too large
        let size = estimate_result_size(&result);
        if size > self.config.max_result_size_bytes {
            return false;
        }

        // Check minimum execution time
        if result.original_exec_time_us < self.config.min_exec_time_us {
            return false;
        }

        let ttl = ttl.unwrap_or(Duration::from_secs(self.config.default_ttl_secs));
        let ttl = ttl.min(Duration::from_secs(self.config.max_ttl_secs));

        let entry = CacheEntry {
            result: result.clone(),
            created_at: Instant::now(),
            ttl,
            hit_count: 0,
            size_bytes: size,
            last_accessed: Instant::now(),
        };

        // Update table dependencies for event-based invalidation
        if self.config.enable_event_invalidation {
            let mut deps = self.table_dependencies.write();
            for table in &result.referenced_tables {
                deps.entry(table.clone())
                    .or_insert_with(HashSet::new)
                    .insert(key.clone());
            }
        }

        let shard_idx = self.get_shard_idx(&key);
        let max_shard_size = self.config.max_size_bytes / self.shards.len();
        self.shards[shard_idx]
            .write()
            .insert(key, entry, max_shard_size);

        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Invalidate cache entry
    pub fn invalidate(&self, key: &CacheKey) {
        let shard_idx = self.get_shard_idx(key);
        self.shards[shard_idx].write().remove(key);
        self.stats.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate all entries referencing a table (event-based invalidation)
    pub fn invalidate_table(&self, table: &str) {
        // Remove from table dependencies
        let keys_to_invalidate = {
            let mut deps = self.table_dependencies.write();
            deps.remove(table).unwrap_or_default()
        };

        // Invalidate each key
        for key in &keys_to_invalidate {
            let shard_idx = self.get_shard_idx(key);
            self.shards[shard_idx].write().remove(key);
        }

        // Also scan all shards for any missed entries
        for shard in &self.shards {
            shard.write().invalidate_by_table(table);
        }

        self.stats
            .invalidations
            .fetch_add(keys_to_invalidate.len() as u64, Ordering::Relaxed);
    }

    /// Invalidate all entries for a database
    pub fn invalidate_database(&self, database: &str) {
        for shard in &self.shards {
            let mut shard = shard.write();
            let keys: Vec<_> = shard
                .entries
                .keys()
                .filter(|k| k.database == database)
                .cloned()
                .collect();
            for key in keys {
                shard.remove(&key);
                self.stats.invalidations.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Clear entire cache
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
        self.table_dependencies.write().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStatsSnapshot {
        let mut total_entries = 0;
        let mut total_size = 0;

        for shard in &self.shards {
            let shard = shard.read();
            total_entries += shard.entries.len();
            total_size += shard.current_size;
        }

        CacheStatsSnapshot {
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            inserts: self.stats.inserts.load(Ordering::Relaxed),
            invalidations: self.stats.invalidations.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            total_entries,
            total_size_bytes: total_size,
            hit_rate: {
                let hits = self.stats.hits.load(Ordering::Relaxed);
                let misses = self.stats.misses.load(Ordering::Relaxed);
                if hits + misses > 0 {
                    hits as f64 / (hits + misses) as f64
                } else {
                    0.0
                }
            },
        }
    }

    /// Prune expired entries
    pub fn prune_expired(&self) {
        for shard in &self.shards {
            let mut shard = shard.write();
            let expired_keys: Vec<_> = shard
                .entries
                .iter()
                .filter(|(_, e)| e.is_expired())
                .map(|(k, _)| k.clone())
                .collect();

            for key in expired_keys {
                shard.remove(&key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Cleanup stale table dependencies (tables with no active cache entries)
        self.cleanup_stale_dependencies();
    }

    /// Remove table dependency entries for tables that no longer have any cached queries
    fn cleanup_stale_dependencies(&self) {
        // Collect all tables referenced by current cache entries
        let mut active_tables: HashSet<String> = HashSet::new();
        for shard in &self.shards {
            let shard = shard.read();
            for entry in shard.entries.values() {
                for table in &entry.result.referenced_tables {
                    active_tables.insert(table.clone());
                }
            }
        }

        // Remove dependency entries for tables not in active set
        let mut deps = self.table_dependencies.write();
        deps.retain(|table, _| active_tables.contains(table));
    }
}

fn estimate_result_size(result: &CachedResult) -> usize {
    let mut size = 0;

    // Column names
    for col in &result.columns {
        size += col.len();
    }

    // Rows
    for row in &result.rows {
        for val in row {
            size += match val {
                CacheValue::Null => 1,
                CacheValue::Bool(_) => 1,
                CacheValue::Int(_) => 8,
                CacheValue::Float(_) => 8,
                CacheValue::String(s) => s.len(),
                CacheValue::Bytes(b) => b.len(),
                CacheValue::Array(a) => a.len() * 16, // Rough estimate
            };
        }
    }

    // Referenced tables
    for table in &result.referenced_tables {
        size += table.len();
    }

    size
}

// ============================================================================
// Cache Statistics
// ============================================================================

#[derive(Default)]
struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    invalidations: AtomicU64,
    evictions: AtomicU64,
}

/// Cache statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub invalidations: u64,
    pub evictions: u64,
    pub total_entries: usize,
    pub total_size_bytes: usize,
    pub hit_rate: f64,
}

// ============================================================================
// Redis-Compatible Protocol Handler
// ============================================================================

/// Redis command for cache operations
#[derive(Debug, Clone)]
pub enum RedisCacheCommand {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        ttl: Option<u64>,
    },
    Del {
        keys: Vec<String>,
    },
    Exists {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    FlushDb,
    Info,
    Ping,
}

/// Redis response
#[derive(Debug, Clone)]
pub enum RedisResponse {
    Ok,
    Pong,
    Nil,
    Integer(i64),
    Bulk(String),
    Array(Vec<RedisResponse>),
    Error(String),
}

impl RedisResponse {
    pub fn to_resp(&self) -> String {
        match self {
            RedisResponse::Ok => "+OK\r\n".to_string(),
            RedisResponse::Pong => "+PONG\r\n".to_string(),
            RedisResponse::Nil => "$-1\r\n".to_string(),
            RedisResponse::Integer(i) => format!(":{}\r\n", i),
            RedisResponse::Bulk(s) => format!("${}\r\n{}\r\n", s.len(), s),
            RedisResponse::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len());
                for item in arr {
                    result.push_str(&item.to_resp());
                }
                result
            }
            RedisResponse::Error(e) => format!("-ERR {}\r\n", e),
        }
    }
}

/// Redis-compatible cache server
pub struct RedisCacheServer {
    cache: Arc<QueryResultCache>,
    /// Simple key-value store for raw Redis operations
    kv_store: RwLock<HashMap<String, (String, Option<Instant>, Duration)>>,
}

impl RedisCacheServer {
    pub fn new(cache: Arc<QueryResultCache>) -> Self {
        Self {
            cache,
            kv_store: RwLock::new(HashMap::new()),
        }
    }

    /// Parse Redis RESP protocol command
    pub fn parse_command(input: &str) -> Result<RedisCacheCommand, String> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".to_string());
        }

        match parts[0].to_uppercase().as_str() {
            "GET" => {
                if parts.len() < 2 {
                    return Err("GET requires key".to_string());
                }
                Ok(RedisCacheCommand::Get {
                    key: parts[1].to_string(),
                })
            }
            "SET" => {
                if parts.len() < 3 {
                    return Err("SET requires key and value".to_string());
                }
                let ttl = if parts.len() >= 5 && parts[3].to_uppercase() == "EX" {
                    parts[4].parse().ok()
                } else {
                    None
                };
                Ok(RedisCacheCommand::Set {
                    key: parts[1].to_string(),
                    value: parts[2].to_string(),
                    ttl,
                })
            }
            "DEL" => {
                if parts.len() < 2 {
                    return Err("DEL requires keys".to_string());
                }
                Ok(RedisCacheCommand::Del {
                    keys: parts[1..].iter().map(|s| s.to_string()).collect(),
                })
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    return Err("EXISTS requires key".to_string());
                }
                Ok(RedisCacheCommand::Exists {
                    key: parts[1].to_string(),
                })
            }
            "EXPIRE" => {
                if parts.len() < 3 {
                    return Err("EXPIRE requires key and seconds".to_string());
                }
                let seconds = parts[2]
                    .parse()
                    .map_err(|_| "Invalid seconds".to_string())?;
                Ok(RedisCacheCommand::Expire {
                    key: parts[1].to_string(),
                    seconds,
                })
            }
            "TTL" => {
                if parts.len() < 2 {
                    return Err("TTL requires key".to_string());
                }
                Ok(RedisCacheCommand::Ttl {
                    key: parts[1].to_string(),
                })
            }
            "FLUSHDB" | "FLUSHALL" => Ok(RedisCacheCommand::FlushDb),
            "INFO" => Ok(RedisCacheCommand::Info),
            "PING" => Ok(RedisCacheCommand::Ping),
            _ => Err(format!("Unknown command: {}", parts[0])),
        }
    }

    /// Execute a Redis command
    pub fn execute(&self, cmd: RedisCacheCommand) -> RedisResponse {
        match cmd {
            RedisCacheCommand::Get { key } => {
                let store = self.kv_store.read();
                if let Some((value, created, ttl)) = store.get(&key) {
                    if let Some(created) = created {
                        if created.elapsed() > *ttl {
                            drop(store);
                            self.kv_store.write().remove(&key);
                            return RedisResponse::Nil;
                        }
                    }
                    RedisResponse::Bulk(value.clone())
                } else {
                    RedisResponse::Nil
                }
            }
            RedisCacheCommand::Set { key, value, ttl } => {
                let ttl_duration = Duration::from_secs(ttl.unwrap_or(3600));
                self.kv_store
                    .write()
                    .insert(key, (value, Some(Instant::now()), ttl_duration));
                RedisResponse::Ok
            }
            RedisCacheCommand::Del { keys } => {
                let mut store = self.kv_store.write();
                let mut deleted = 0i64;
                for key in keys {
                    if store.remove(&key).is_some() {
                        deleted += 1;
                    }
                }
                RedisResponse::Integer(deleted)
            }
            RedisCacheCommand::Exists { key } => {
                let store = self.kv_store.read();
                RedisResponse::Integer(if store.contains_key(&key) { 1 } else { 0 })
            }
            RedisCacheCommand::Expire { key, seconds } => {
                let mut store = self.kv_store.write();
                if let Some((value, _, _)) = store.remove(&key) {
                    store.insert(
                        key,
                        (value, Some(Instant::now()), Duration::from_secs(seconds)),
                    );
                    RedisResponse::Integer(1)
                } else {
                    RedisResponse::Integer(0)
                }
            }
            RedisCacheCommand::Ttl { key } => {
                let store = self.kv_store.read();
                if let Some((_, created, ttl)) = store.get(&key) {
                    if let Some(created) = created {
                        let elapsed = created.elapsed();
                        if elapsed > *ttl {
                            RedisResponse::Integer(-2)
                        } else {
                            RedisResponse::Integer((*ttl - elapsed).as_secs() as i64)
                        }
                    } else {
                        RedisResponse::Integer(-1) // No expiry
                    }
                } else {
                    RedisResponse::Integer(-2) // Key doesn't exist
                }
            }
            RedisCacheCommand::FlushDb => {
                self.kv_store.write().clear();
                self.cache.clear();
                RedisResponse::Ok
            }
            RedisCacheCommand::Info => {
                let stats = self.cache.stats();
                let info = format!(
                    "# Cache\r\nhits:{}\r\nmisses:{}\r\nentries:{}\r\nsize_bytes:{}\r\nhit_rate:{:.2}\r\n",
                    stats.hits,
                    stats.misses,
                    stats.total_entries,
                    stats.total_size_bytes,
                    stats.hit_rate
                );
                RedisResponse::Bulk(info)
            }
            RedisCacheCommand::Ping => RedisResponse::Pong,
        }
    }
}

// ============================================================================
// Cache Invalidation Events
// ============================================================================

/// Event types that trigger cache invalidation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InvalidationEvent {
    /// Table was modified (INSERT, UPDATE, DELETE)
    TableModified { database: String, table: String },
    /// Table schema changed
    SchemaChanged { database: String, table: String },
    /// Table was dropped
    TableDropped { database: String, table: String },
    /// Database was dropped
    DatabaseDropped { database: String },
    /// Manual invalidation by key pattern
    PatternInvalidation { pattern: String },
    /// Full cache flush
    FullFlush,
}

/// Event-based cache invalidator
pub struct CacheInvalidator {
    cache: Arc<QueryResultCache>,
    /// Pending events to process
    pending_events: RwLock<Vec<InvalidationEvent>>,
}

impl CacheInvalidator {
    pub fn new(cache: Arc<QueryResultCache>) -> Self {
        Self {
            cache,
            pending_events: RwLock::new(Vec::new()),
        }
    }

    /// Queue an invalidation event
    pub fn queue_event(&self, event: InvalidationEvent) {
        self.pending_events.write().push(event);
    }

    /// Process a single event immediately
    pub fn process_event(&self, event: InvalidationEvent) {
        match event {
            InvalidationEvent::TableModified { database, table } => {
                let full_name = format!("{}.{}", database, table);
                self.cache.invalidate_table(&full_name);
                self.cache.invalidate_table(&table);
            }
            InvalidationEvent::SchemaChanged { database, table } => {
                let full_name = format!("{}.{}", database, table);
                self.cache.invalidate_table(&full_name);
                self.cache.invalidate_table(&table);
            }
            InvalidationEvent::TableDropped { database, table } => {
                let full_name = format!("{}.{}", database, table);
                self.cache.invalidate_table(&full_name);
                self.cache.invalidate_table(&table);
            }
            InvalidationEvent::DatabaseDropped { database } => {
                self.cache.invalidate_database(&database);
            }
            InvalidationEvent::PatternInvalidation { pattern: _ } => {
                // Pattern-based invalidation would require regex matching
                // For simplicity, we'll do a full flush
                self.cache.clear();
            }
            InvalidationEvent::FullFlush => {
                self.cache.clear();
            }
        }
    }

    /// Process all pending events
    pub fn process_pending(&self) {
        let events: Vec<_> = self.pending_events.write().drain(..).collect();
        for event in events {
            self.process_event(event);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_normalization() {
        let sql1 = "SELECT * FROM users WHERE id = 123";
        let sql2 = "SELECT * FROM users WHERE id = 456";
        let sql3 = "select * from users where id = 789";

        let key1 = CacheKey::new(sql1, "test", &[], None);
        let key2 = CacheKey::new(sql2, "test", &[], None);
        let key3 = CacheKey::new(sql3, "test", &[], None);

        // All should have same fingerprint (normalized)
        assert_eq!(key1.fingerprint, key2.fingerprint);
        assert_eq!(key2.fingerprint, key3.fingerprint);
    }

    #[test]
    fn test_cache_put_get() {
        let config = CacheConfig::default();
        let cache = QueryResultCache::new(config);

        let key = CacheKey::new("SELECT 1", "test", &[], None);
        let result = CachedResult {
            columns: vec!["col1".to_string()],
            rows: vec![vec![CacheValue::Int(1)]],
            original_exec_time_us: 5000,
            referenced_tables: vec!["table1".to_string()],
        };

        assert!(cache.put(key.clone(), result.clone(), None));

        let cached = cache.get(&key);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().columns, vec!["col1"]);
    }

    #[test]
    fn test_table_invalidation() {
        let config = CacheConfig::default();
        let cache = QueryResultCache::new(config);

        let key = CacheKey::new("SELECT 1", "test", &[], None);
        let result = CachedResult {
            columns: vec!["col1".to_string()],
            rows: vec![],
            original_exec_time_us: 5000,
            referenced_tables: vec!["users".to_string()],
        };

        cache.put(key.clone(), result, None);
        assert!(cache.get(&key).is_some());

        cache.invalidate_table("users");
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_redis_commands() {
        let config = CacheConfig::default();
        let cache = Arc::new(QueryResultCache::new(config));
        let server = RedisCacheServer::new(cache);

        // Test SET and GET
        let set_cmd = RedisCacheServer::parse_command("SET mykey myvalue").unwrap();
        let response = server.execute(set_cmd);
        assert!(matches!(response, RedisResponse::Ok));

        let get_cmd = RedisCacheServer::parse_command("GET mykey").unwrap();
        let response = server.execute(get_cmd);
        if let RedisResponse::Bulk(val) = response {
            assert_eq!(val, "myvalue");
        } else {
            panic!("Expected Bulk response");
        }

        // Test PING
        let ping_cmd = RedisCacheServer::parse_command("PING").unwrap();
        let response = server.execute(ping_cmd);
        assert!(matches!(response, RedisResponse::Pong));
    }
}
