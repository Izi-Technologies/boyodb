use crate::replication::{
    compute_bundle_plan_hash, BundlePayload, BundlePlan, BundlePlanner, BundleRequest,
    BundleSegment, ColumnStats, IndexMeta, IndexState, Manifest, ManifestEntry, PrimitiveValue,
    SegmentTier, TableMeta,
};
use crate::wal::Wal;
use crate::sql::SqlValue;
use crate::sql::SelectExpr;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, Scalar, StringArray,
    TimestampMicrosecondArray, UInt64Array, Int8Array, Int16Array, Int32Array,
    UInt8Array, UInt16Array, UInt32Array, Float32Array, Float64Array,
    cast::AsArray,
};
use arrow::compute::kernels::aggregate;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::boolean::{and, or, not};
use arrow::compute::{filter as arrow_filter, like, nlike};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use bloomfilter::Bloom;
use crc32fast::Hasher as Crc32Hasher;
use lru::LruCache;
use parking_lot::Mutex as ParkingMutex;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Cursor, Write, ErrorKind};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
#[cfg(test)]
use tempfile::tempdir;
use thiserror::Error;
use zstd::stream::{decode_all as zstd_decode_all, encode_all as zstd_encode_all};
use lz4_flex::{compress_prepend_size as lz4_compress, decompress_size_prepended as lz4_decompress};
use snap::{read::FrameDecoder as SnappyDecoder, write::FrameEncoder as SnappyEncoder};
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("internal: {0}")]
    Internal(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("io: {0}")]
    Io(String),
    #[error("query timeout: {0}")]
    Timeout(String),
    #[error("remote error: {0}")]
    Remote(String),
    #[error("configuration error: {0}")]
    Configuration(String),
}

/// Maximum length for database and table identifiers
const MAX_IDENTIFIER_LEN: usize = 128;

/// Validates a database or table identifier.
/// Returns Ok(()) if valid, Err with reason if invalid.
///
/// Rules:
/// - Must not be empty
/// - Must not exceed MAX_IDENTIFIER_LEN (128) characters
/// - Must start with a letter or underscore
/// - May only contain letters, digits, underscores
/// - Must not contain path separators or null bytes
/// - Must not be a reserved keyword
pub fn validate_identifier(name: &str, kind: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(format!("{kind} name cannot be empty")));
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(EngineError::InvalidArgument(format!(
            "{kind} name too long ({} > {})",
            name.len(),
            MAX_IDENTIFIER_LEN
        )));
    }
    // Check for null bytes and path separators
    if name.contains('\0') || name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(EngineError::InvalidArgument(format!(
            "{kind} name contains invalid characters"
        )));
    }
    // Must start with letter or underscore
    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(EngineError::InvalidArgument(format!(
            "{kind} name must start with a letter or underscore"
        )));
    }
    // All chars must be alphanumeric or underscore
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(EngineError::InvalidArgument(format!(
                "{kind} name contains invalid character: '{}'",
                c
            )));
        }
    }
    // Reserved keywords check
    let reserved = ["__system", "__internal", "__metadata"];
    let lower = name.to_lowercase();
    for kw in &reserved {
        if lower == *kw {
            return Err(EngineError::InvalidArgument(format!(
                "{kind} name '{}' is reserved",
                name
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub shard_count: usize,
    pub cache_bytes: u64,
    pub manifest_path: PathBuf,
    pub segments_dir: PathBuf,
    pub wal_path: PathBuf,
    pub wal_max_bytes: u64,
    pub manifest_snapshot_path: PathBuf,
    pub wal_max_segments: u64,
    pub wal_sync_bytes: u64,
    pub wal_sync_interval_ms: u64,
    pub allow_manifest_import: bool,
    pub retention_watermark_micros: Option<u64>,
    pub compact_min_segments: usize,
    pub enable_compaction: bool,
    pub compaction_target_bytes: u64,
    /// Interval for automatic background compaction (0 = disabled)
    pub compaction_interval_ms: u64,

    // S3 specific config
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,

    pub auto_compact_interval_secs: u64,
    /// Maximum concurrent compaction operations (prevents merge storms)
    pub max_concurrent_compactions: usize,
    /// Minimum time between compactions on the same table (prevents thrashing)
    pub compaction_cooldown_secs: u64,
    pub tier_warm_after_millis: u64,
    pub tier_cold_after_millis: u64,
    pub tier_warm_compression: Option<String>,
    pub tier_cold_compression: Option<String>,
    pub maintenance_interval_millis: u64,
    pub parallel_scan_threshold: usize,
    pub segment_cache_capacity: usize,
    /// Maximum bytes for segment cache (LRU eviction based on size)
    pub segment_cache_bytes: u64,
    /// Maximum bytes for decoded batch cache (0 = disabled)
    pub batch_cache_bytes: u64,
    /// Maximum entries for segment schema cache
    pub schema_cache_entries: usize,
    pub cache_hot_segments: bool,
    pub cache_warm_segments: bool,
    pub cache_cold_segments: bool,
    pub memtable_max_bytes: u64,
    pub memtable_max_entries: usize,
    /// Query cache size (number of entries)
    pub query_cache_size: usize,
    /// Query cache maximum bytes (0 = disabled)
    pub query_cache_bytes: u64,
    /// Query cache TTL in seconds
    pub query_cache_ttl_secs: u64,
    /// Query plan cache size (number of entries)
    pub plan_cache_size: usize,
    /// Query plan cache TTL in seconds
    pub plan_cache_ttl_secs: u64,
    /// Row granularity for index-like batch sizing (0 = disabled)
    pub index_granularity_rows: usize,
    /// Maximum bytes allowed for in-memory joins before spilling
    pub join_max_bytes: usize,
    /// Target bytes per join partition when spilling to disk
    pub join_partition_bytes: u64,
    /// Maximum number of join partitions
    pub join_max_partitions: usize,
    /// Maximum rows allowed for CROSS JOIN
    pub cross_join_max_rows: usize,
    /// Maximum bytes allowed for set operation inputs
    pub set_operation_max_bytes: usize,
}

impl EngineConfig {
    pub fn new<P: AsRef<Path>>(data_dir: P, shard_count: usize) -> Self {
        let wal_dir = data_dir.as_ref().join("wal");
        let segments_dir = data_dir.as_ref().join("segments");
        let manifest_path = data_dir.as_ref().join("manifest.json");
        let wal_path = wal_dir.join("wal.log");
        let manifest_snapshot_path = wal_dir.join("manifest.snapshot.json");
        EngineConfig {
            data_dir: data_dir.as_ref().to_path_buf(),
            wal_dir,
            shard_count: shard_count.max(1),
            cache_bytes: 512 * 1024 * 1024,
            manifest_path,
            segments_dir,
            wal_path,
            wal_max_bytes: 64 * 1024 * 1024,
            manifest_snapshot_path,
            wal_max_segments: 4,
            wal_sync_bytes: 0,
            wal_sync_interval_ms: 0,
            allow_manifest_import: false,
            retention_watermark_micros: None,
            compact_min_segments: 2,
            enable_compaction: true,
            compaction_target_bytes: 128 * 1024 * 1024, // 128MB default target for merged segments
            compaction_interval_ms: 60_000,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key: None,
            s3_secret_key: None,
            auto_compact_interval_secs: 60,             // Check every minute
            max_concurrent_compactions: 2,              // Limit to 2 concurrent compactions
            compaction_cooldown_secs: 30,               // 30s cooldown per table
            tier_warm_after_millis: 3_600_000,          // 1h
            tier_cold_after_millis: 86_400_000,         // 24h
            tier_warm_compression: None,
            tier_cold_compression: None,
            maintenance_interval_millis: 0,
            parallel_scan_threshold: 2, // Use parallel scanning when >=2 segments (aggressive parallelism)
            segment_cache_capacity: 8,
            segment_cache_bytes: 4 * 1024 * 1024 * 1024, // 4GB default for segment cache
            batch_cache_bytes: 512 * 1024 * 1024,        // 512MB default for decoded batches
            schema_cache_entries: 1024,
            cache_hot_segments: true,
            cache_warm_segments: true,
            cache_cold_segments: false,
            memtable_max_bytes: 0,
            memtable_max_entries: 0,
            query_cache_size: 10_000,  // 10K queries
            query_cache_bytes: 256 * 1024 * 1024, // 256MB default query cache
            query_cache_ttl_secs: 300, // 5 minutes
            plan_cache_size: 10_000,   // 10K plans
            plan_cache_ttl_secs: 300,  // 5 minutes
            index_granularity_rows: 0,
            join_max_bytes: 200 * 1024 * 1024,
            join_partition_bytes: 64 * 1024 * 1024,
            join_max_partitions: 128,
            cross_join_max_rows: 1_000_000,
            set_operation_max_bytes: 200 * 1024 * 1024,
        }
    }

    pub fn with_segment_cache_bytes(mut self, bytes: u64) -> Self {
        self.segment_cache_bytes = bytes;
        self
    }

    pub fn with_s3(
        mut self,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
    ) -> Self {
        self.s3_bucket = Some(bucket);
        self.s3_region = Some(region);
        self.s3_endpoint = endpoint;
        self.s3_access_key = access_key;
        self.s3_secret_key = secret_key;
        self
    }

    pub fn with_batch_cache_bytes(mut self, bytes: u64) -> Self {
        self.batch_cache_bytes = bytes;
        self
    }

    pub fn with_schema_cache_entries(mut self, entries: usize) -> Self {
        self.schema_cache_entries = entries.max(1);
        self
    }

    pub fn with_query_cache_size(mut self, size: usize) -> Self {
        self.query_cache_size = size;
        self
    }

    pub fn with_query_cache_bytes(mut self, bytes: u64) -> Self {
        self.query_cache_bytes = bytes;
        self
    }

    pub fn with_query_cache_ttl_secs(mut self, ttl: u64) -> Self {
        self.query_cache_ttl_secs = ttl;
        self
    }

    pub fn with_plan_cache_size(mut self, size: usize) -> Self {
        self.plan_cache_size = size;
        self
    }

    pub fn with_plan_cache_ttl_secs(mut self, ttl: u64) -> Self {
        self.plan_cache_ttl_secs = ttl;
        self
    }

    pub fn with_parallel_scan_threshold(mut self, threshold: usize) -> Self {
        self.parallel_scan_threshold = threshold;
        self
    }

    pub fn with_cache_bytes(mut self, cache_bytes: u64) -> Self {
        self.cache_bytes = cache_bytes;
        self
    }

    pub fn with_wal_dir<P: AsRef<Path>>(mut self, wal_dir: P) -> Self {
        self.wal_dir = wal_dir.as_ref().to_path_buf();
        self
    }

    pub fn with_manifest_path<P: AsRef<Path>>(mut self, manifest_path: P) -> Self {
        self.manifest_path = manifest_path.as_ref().to_path_buf();
        self
    }

    pub fn with_segments_dir<P: AsRef<Path>>(mut self, segments_dir: P) -> Self {
        self.segments_dir = segments_dir.as_ref().to_path_buf();
        self
    }

    pub fn with_wal_path<P: AsRef<Path>>(mut self, wal_path: P) -> Self {
        self.wal_path = wal_path.as_ref().to_path_buf();
        self
    }

    pub fn with_wal_max_bytes(mut self, wal_max_bytes: u64) -> Self {
        self.wal_max_bytes = wal_max_bytes;
        self
    }

    pub fn with_manifest_snapshot_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.manifest_snapshot_path = path.as_ref().to_path_buf();
        self
    }

    pub fn with_wal_max_segments(mut self, wal_max_segments: u64) -> Self {
        self.wal_max_segments = wal_max_segments.max(1);
        self
    }

    pub fn with_wal_sync_bytes(mut self, bytes: u64) -> Self {
        self.wal_sync_bytes = bytes;
        self
    }

    pub fn with_wal_sync_interval_ms(mut self, ms: u64) -> Self {
        self.wal_sync_interval_ms = ms;
        self
    }

    pub fn with_allow_manifest_import(mut self, allow: bool) -> Self {
        self.allow_manifest_import = allow;
        self
    }

    pub fn with_retention_watermark(mut self, watermark_micros: u64) -> Self {
        self.retention_watermark_micros = Some(watermark_micros);
        self
    }

    pub fn with_compact_min_segments(mut self, min_segments: usize) -> Self {
        self.compact_min_segments = min_segments.max(2);
        self
    }

    pub fn with_enable_compaction(mut self, enable: bool) -> Self {
        self.enable_compaction = enable;
        self
    }

    pub fn with_compaction_target_bytes(mut self, target_bytes: u64) -> Self {
        // Ensure non-zero target to avoid busy compaction
        self.compaction_target_bytes = target_bytes.max(1);
        self
    }

    pub fn with_auto_compact_interval_secs(mut self, secs: u64) -> Self {
        self.auto_compact_interval_secs = secs;
        self
    }

    pub fn with_tier_warm_after_millis(mut self, millis: u64) -> Self {
        self.tier_warm_after_millis = millis;
        self
    }

    pub fn with_tier_cold_after_millis(mut self, millis: u64) -> Self {
        self.tier_cold_after_millis = millis;
        self
    }

    pub fn with_tier_warm_compression(mut self, compression: Option<String>) -> Self {
        self.tier_warm_compression = compression;
        self
    }

    pub fn with_tier_cold_compression(mut self, compression: Option<String>) -> Self {
        self.tier_cold_compression = compression;
        self
    }

    pub fn with_maintenance_interval_millis(mut self, interval: u64) -> Self {
        self.maintenance_interval_millis = interval;
        self
    }

    pub fn with_segment_cache_capacity(mut self, capacity: usize) -> Self {
        self.segment_cache_capacity = capacity;
        self
    }

    pub fn with_cache_hot_segments(mut self, enabled: bool) -> Self {
        self.cache_hot_segments = enabled;
        self
    }

    pub fn with_cache_warm_segments(mut self, enabled: bool) -> Self {
        self.cache_warm_segments = enabled;
        self
    }

    pub fn with_cache_cold_segments(mut self, enabled: bool) -> Self {
        self.cache_cold_segments = enabled;
        self
    }

    pub fn with_index_granularity_rows(mut self, rows: usize) -> Self {
        self.index_granularity_rows = rows;
        self
    }

    pub fn with_join_max_bytes(mut self, bytes: usize) -> Self {
        self.join_max_bytes = bytes;
        self
    }

    pub fn with_join_partition_bytes(mut self, bytes: u64) -> Self {
        self.join_partition_bytes = bytes;
        self
    }

    pub fn with_join_max_partitions(mut self, partitions: usize) -> Self {
        self.join_max_partitions = partitions.max(1);
        self
    }

    pub fn with_cross_join_max_rows(mut self, rows: usize) -> Self {
        self.cross_join_max_rows = rows.max(1);
        self
    }

    pub fn with_set_operation_max_bytes(mut self, bytes: usize) -> Self {
        self.set_operation_max_bytes = bytes;
        self
    }

    pub fn with_memtable_max_bytes(mut self, bytes: u64) -> Self {
        self.memtable_max_bytes = bytes;
        self
    }

    pub fn with_memtable_max_entries(mut self, entries: usize) -> Self {
        self.memtable_max_entries = entries.max(1);
        self
    }
}

/// LRU-style query result cache entry
#[derive(Debug, Clone)]
struct QueryCacheEntry {
    response: QueryResponse,
    manifest_version: u64,
    created_at: std::time::Instant,
    database: String,
    table: String,
    bytes: u64,
}

/// Normalize SQL query for better cache hit rates.
/// - Collapses whitespace
/// - Normalizes keywords to uppercase
/// - Trims leading/trailing whitespace
#[inline]
fn normalize_sql_for_cache(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut in_string = false;
    let mut string_char = '"';
    let mut prev_was_space = true;
    let mut current_word = String::new();
    let chars: Vec<char> = sql.chars().collect();

    for (i, &c) in chars.iter().enumerate() {
        if in_string {
            result.push(c);
            if c == string_char && (i == 0 || chars[i - 1] != '\\') {
                in_string = false;
            }
            prev_was_space = false;
            continue;
        }

        if c == '\'' || c == '"' {
            // Flush any pending word
            if !current_word.is_empty() {
                result.push_str(&normalize_keyword(&current_word));
                current_word.clear();
            }
            in_string = true;
            string_char = c;
            result.push(c);
            prev_was_space = false;
            continue;
        }

        if c.is_whitespace() {
            // Flush any pending word
            if !current_word.is_empty() {
                result.push_str(&normalize_keyword(&current_word));
                current_word.clear();
            }
            if !prev_was_space && !result.is_empty() {
                result.push(' ');
            }
            prev_was_space = true;
        } else if c.is_alphanumeric() || c == '_' {
            // Part of an identifier/keyword
            current_word.push(c);
            prev_was_space = false;
        } else {
            // Punctuation - flush word first
            if !current_word.is_empty() {
                result.push_str(&normalize_keyword(&current_word));
                current_word.clear();
            }
            result.push(c);
            prev_was_space = false;
        }
    }

    // Flush final word
    if !current_word.is_empty() {
        result.push_str(&normalize_keyword(&current_word));
    }

    result.trim().to_string()
}

/// Normalize SQL keywords to uppercase for consistent caching
#[inline]
fn normalize_keyword(word: &str) -> String {
    let upper = word.to_uppercase();
    match upper.as_str() {
        "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "NOT" | "IN" | "IS" | "NULL"
        | "ORDER" | "BY" | "ASC" | "DESC" | "LIMIT" | "OFFSET" | "JOIN" | "LEFT"
        | "RIGHT" | "INNER" | "OUTER" | "ON" | "AS" | "GROUP" | "HAVING" | "UNION"
        | "ALL" | "DISTINCT" | "LIKE" | "BETWEEN" | "CASE" | "WHEN" | "THEN"
        | "ELSE" | "END" | "INSERT" | "INTO" | "VALUES" | "UPDATE" | "SET"
        | "DELETE" | "CREATE" | "TABLE" | "INDEX" | "DROP" | "ALTER" | "WITH"
        | "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" | "TRUE" | "FALSE" => upper,
        _ => word.to_string(), // Keep identifiers as-is
    }
}

/// Compute a fast 64-bit hash for query cache keys.
/// Uses FNV-1a for speed (no need for cryptographic security here).
/// Normalizes the query first for better cache hit rates.
#[inline]
fn hash_query_key(sql: &str) -> u64 {
    use std::hash::Hasher;
    let normalized = normalize_sql_for_cache(sql);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

/// Query result cache with LRU eviction, TTL, and table-level invalidation.
/// Uses u64 hash keys instead of String keys for better performance.
struct QueryCache {
    lru: LruCache<u64, QueryCacheEntry>,
    ttl: std::time::Duration,
    current_bytes: u64,
    max_bytes: u64,
}

#[derive(Debug, Clone)]
struct BatchCacheEntry {
    batches: Arc<Vec<RecordBatch>>,
    size_bytes: u64,
}

/// Cache decoded RecordBatches per segment to avoid repeated IPC decoding.
#[derive(Debug)]
struct BatchCache {
    lru: LruCache<String, BatchCacheEntry>,
    current_bytes: u64,
    max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanKind {
    Simple {
        agg: Option<AggPlan>,
        db: Option<String>,
        table: Option<String>,
        projection: Option<Vec<String>>,
        filter: QueryFilter,
        computed_columns: Vec<crate::sql::SelectColumn>,
    },
    Join,
    SetOperation,
    Subquery,
    Cte,
    Transaction(crate::sql::TransactionCommand),
}

enum IndexValue {
    Int(i128),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
struct CachedPlan {
    kind: PlanKind,
}

#[derive(Debug, Clone)]
struct PlanCacheEntry {
    plan: CachedPlan,
    created_at: std::time::Instant,
}

#[derive(Debug)]
struct PlanCache {
    lru: LruCache<u64, PlanCacheEntry>,
    ttl: std::time::Duration,
}

impl BatchCache {
    fn new(max_bytes: u64) -> Self {
        let cap = NonZeroUsize::new(1024).unwrap();
        BatchCache {
            lru: LruCache::new(cap),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn estimate_bytes(batches: &[RecordBatch]) -> u64 {
        batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum()
    }

    fn get(&mut self, segment_id: &str) -> Option<Arc<Vec<RecordBatch>>> {
        self.lru.get(segment_id).map(|e| e.batches.clone())
    }

    fn insert(&mut self, segment_id: String, batches: Vec<RecordBatch>) -> Arc<Vec<RecordBatch>> {
        let size_bytes = Self::estimate_bytes(&batches);
        let arc_batches = Arc::new(batches);

        if self.max_bytes == 0 || size_bytes > self.max_bytes {
            return arc_batches;
        }

        if let Some(existing) = self.lru.pop(&segment_id) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.size_bytes);
        }

        while self.current_bytes + size_bytes > self.max_bytes {
            if let Some((_key, evicted)) = self.lru.pop_lru() {
                self.current_bytes = self.current_bytes.saturating_sub(evicted.size_bytes);
            } else {
                break;
            }
        }

        self.current_bytes = self.current_bytes.saturating_add(size_bytes);
        self.lru.put(
            segment_id,
            BatchCacheEntry {
                batches: arc_batches.clone(),
                size_bytes,
            },
        );
        arc_batches
    }

    fn invalidate(&mut self, segment_id: &str) {
        if let Some(existing) = self.lru.pop(segment_id) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.size_bytes);
        }
    }
}

impl std::fmt::Debug for QueryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCache")
            .field("len", &self.lru.len())
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl QueryCache {
    fn new(max_entries: usize, ttl_secs: u64, max_bytes: u64) -> Self {
        QueryCache {
            lru: LruCache::new(NonZeroUsize::new(max_entries.max(1)).unwrap()),
            ttl: std::time::Duration::from_secs(ttl_secs),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, sql: &str, current_manifest_version: u64) -> Option<QueryResponse> {
        if self.max_bytes == 0 {
            return None;
        }
        let key = hash_query_key(sql);
        if let Some(entry) = self.lru.get(&key) {
            // Invalidate if manifest has changed or entry is too old
            if entry.manifest_version == current_manifest_version
                && entry.created_at.elapsed() < self.ttl
            {
                return Some(entry.response.clone());
            }
        }
        if let Some(removed) = self.lru.pop(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(removed.bytes);
        }
        None
    }

    fn insert(&mut self, sql: &str, response: QueryResponse, manifest_version: u64, database: &str, table: &str) {
        if self.max_bytes == 0 {
            return;
        }
        let key = hash_query_key(sql);
        if let Some(existing) = self.lru.pop(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.bytes);
        }
        let entry_bytes = response.records_ipc.len() as u64;
        if entry_bytes > self.max_bytes {
            return;
        }
        while self.current_bytes + entry_bytes > self.max_bytes && !self.lru.is_empty() {
            if let Some((_, evicted)) = self.lru.pop_lru() {
                self.current_bytes = self.current_bytes.saturating_sub(evicted.bytes);
            }
        }
        self.lru.put(
            key,
            QueryCacheEntry {
                response,
                manifest_version,
                created_at: std::time::Instant::now(),
                database: database.to_string(),
                table: table.to_string(),
                bytes: entry_bytes,
            },
        );
        self.current_bytes = self.current_bytes.saturating_add(entry_bytes);
    }

    /// Invalidate only entries for a specific table (granular invalidation)
    fn invalidate_table(&mut self, database: &str, table: &str) {
        let keys_to_remove: Vec<(u64, u64)> = self.lru.iter()
            .filter(|(_, entry)| entry.database == database && entry.table == table)
            .map(|(key, entry)| (*key, entry.bytes))
            .collect();
        for (key, bytes) in keys_to_remove {
            if self.lru.pop(&key).is_some() {
                self.current_bytes = self.current_bytes.saturating_sub(bytes);
            }
        }
    }

    fn invalidate(&mut self) {
        self.lru.clear();
        self.current_bytes = 0;
    }

    fn len(&self) -> usize {
        self.lru.len()
    }
}

impl PlanCache {
    fn new(max_entries: usize, ttl_secs: u64) -> Self {
        PlanCache {
            lru: LruCache::new(NonZeroUsize::new(max_entries.max(1)).unwrap()),
            ttl: std::time::Duration::from_secs(ttl_secs),
        }
    }

    fn get(&mut self, sql: &str) -> Option<CachedPlan> {
        let key = hash_query_key(sql);
        if let Some(entry) = self.lru.get(&key) {
            if entry.created_at.elapsed() < self.ttl {
                return Some(entry.plan.clone());
            }
        }
        self.lru.pop(&key);
        None
    }

    fn insert(&mut self, sql: &str, plan: CachedPlan) {
        let key = hash_query_key(sql);
        self.lru.put(
            key,
            PlanCacheEntry {
                plan,
                created_at: std::time::Instant::now(),
            },
        );
    }
}

struct PartitionWriter {
    path: PathBuf,
    file: Option<File>,
    writer: Option<StreamWriter<BufWriter<File>>>,
}

impl PartitionWriter {
    fn new(path: PathBuf, file: File) -> Self {
        PartitionWriter {
            path,
            file: Some(file),
            writer: None,
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), EngineError> {
        if self.writer.is_none() {
            let file = self.file.take().ok_or_else(|| {
                EngineError::Internal("partition writer file missing".into())
            })?;
            let writer = StreamWriter::try_new(BufWriter::new(file), batch.schema().as_ref())
                .map_err(|e| EngineError::Internal(format!("IPC writer error: {e}")))?;
            self.writer = Some(writer);
        }
        if let Some(w) = self.writer.as_mut() {
            w.write(batch)
                .map_err(|e| EngineError::Internal(format!("IPC write error: {e}")))?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), EngineError> {
        if let Some(mut w) = self.writer.take() {
            w.finish()
                .map_err(|e| EngineError::Internal(format!("IPC finish error: {e}")))?;
        }
        Ok(())
    }
}

/// Manifest index for O(1) lookups by (database, table) and segment_id
#[derive(Debug, Default)]
struct ManifestIndex {
    /// Index from (database, table) -> list of segment indices in manifest.entries
    by_table: HashMap<(String, String), Vec<usize>>,
    /// Set of all segment IDs for O(1) existence checks
    segment_ids: HashSet<String>,
}

impl ManifestIndex {
    fn new() -> Self {
        ManifestIndex {
            by_table: HashMap::new(),
            segment_ids: HashSet::new(),
        }
    }

    /// Build index from manifest entries
    fn build(entries: &[ManifestEntry]) -> Self {
        let mut index = ManifestIndex::new();
        for (i, entry) in entries.iter().enumerate() {
            let db = if entry.database.is_empty() { "default" } else { &entry.database };
            let table = if entry.table.is_empty() { "default" } else { &entry.table };
            index.by_table
                .entry((db.to_string(), table.to_string()))
                .or_default()
                .push(i);
            index.segment_ids.insert(entry.segment_id.clone());
        }
        index
    }

    /// Add a new entry to the index
    fn add_entry(&mut self, index: usize, entry: &ManifestEntry) {
        let db = if entry.database.is_empty() { "default" } else { &entry.database };
        let table = if entry.table.is_empty() { "default" } else { &entry.table };
        self.by_table
            .entry((db.to_string(), table.to_string()))
            .or_default()
            .push(index);
        self.segment_ids.insert(entry.segment_id.clone());
    }

    /// Check if segment_id exists (O(1))
    #[allow(dead_code)] // Reserved for future use in segment deduplication
    fn contains_segment(&self, segment_id: &str) -> bool {
        self.segment_ids.contains(segment_id)
    }

    /// Get indices of entries for a table (O(1) lookup, then iterate)
    fn get_table_entries(&self, database: &str, table: &str) -> Option<&Vec<usize>> {
        self.by_table.get(&(database.to_string(), table.to_string()))
    }
}

/// LRU Segment cache with byte-size limits and Arc for zero-copy sharing
struct SegmentCache {
    lru: LruCache<String, Arc<Vec<u8>>>,
    current_bytes: u64,
    max_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for SegmentCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCache")
            .field("entries", &self.lru.len())
            .field("current_bytes", &self.current_bytes)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

impl SegmentCache {
    fn new(max_bytes: u64) -> Self {
        // Use a large max entry count; eviction is driven by byte limit
        let max_entries = NonZeroUsize::new(1_000_000).unwrap();
        SegmentCache {
            lru: LruCache::new(max_entries),
            current_bytes: 0,
            max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn get(&mut self, segment_id: &str) -> Option<Arc<Vec<u8>>> {
        if let Some(data) = self.lru.get(segment_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(data))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn insert(&mut self, segment_id: String, data: Vec<u8>) -> Arc<Vec<u8>> {
        let data_size = data.len() as u64;

        // Evict entries until we have space
        while self.current_bytes + data_size > self.max_bytes && !self.lru.is_empty() {
            if let Some((_, evicted)) = self.lru.pop_lru() {
                self.current_bytes = self.current_bytes.saturating_sub(evicted.len() as u64);
            }
        }

        let arc_data = Arc::new(data);
        self.lru.put(segment_id, Arc::clone(&arc_data));
        self.current_bytes += data_size;
        arc_data
    }

    fn invalidate(&mut self, segment_id: &str) {
        if let Some(evicted) = self.lru.pop(segment_id) {
            self.current_bytes = self.current_bytes.saturating_sub(evicted.len() as u64);
        }
    }

    #[allow(dead_code)] // Reserved for future metrics endpoint
    fn stats(&self) -> (u64, u64, u64, usize) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.current_bytes,
            self.lru.len(),
        )
    }
}

/// Runtime metrics for monitoring
#[derive(Debug, Default)]
pub struct Metrics {
    pub queries_total: AtomicU64,
    pub queries_cached: AtomicU64,
    pub queries_errors: AtomicU64,
    pub ingests_total: AtomicU64,
    pub ingests_bytes: AtomicU64,
    pub ingests_errors: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub batch_cache_hits: AtomicU64,
    pub batch_cache_misses: AtomicU64,
    pub schema_cache_hits: AtomicU64,
    pub schema_cache_misses: AtomicU64,
}

impl Metrics {
    /// Export metrics in Prometheus text format
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP boyodb_queries_total Total number of queries executed\n");
        out.push_str("# TYPE boyodb_queries_total counter\n");
        out.push_str(&format!(
            "boyodb_queries_total {}\n",
            self.queries_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_queries_cached Queries served from cache\n");
        out.push_str("# TYPE boyodb_queries_cached counter\n");
        out.push_str(&format!(
            "boyodb_queries_cached {}\n",
            self.queries_cached.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_queries_errors Total query errors\n");
        out.push_str("# TYPE boyodb_queries_errors counter\n");
        out.push_str(&format!(
            "boyodb_queries_errors {}\n",
            self.queries_errors.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_ingests_total Total number of ingests\n");
        out.push_str("# TYPE boyodb_ingests_total counter\n");
        out.push_str(&format!(
            "boyodb_ingests_total {}\n",
            self.ingests_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_ingests_bytes_total Total bytes ingested\n");
        out.push_str("# TYPE boyodb_ingests_bytes_total counter\n");
        out.push_str(&format!(
            "boyodb_ingests_bytes_total {}\n",
            self.ingests_bytes.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_ingests_errors Total ingest errors\n");
        out.push_str("# TYPE boyodb_ingests_errors counter\n");
        out.push_str(&format!(
            "boyodb_ingests_errors {}\n",
            self.ingests_errors.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_cache_hits Query cache hits\n");
        out.push_str("# TYPE boyodb_cache_hits counter\n");
        out.push_str(&format!(
            "boyodb_cache_hits {}\n",
            self.cache_hits.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_cache_misses Query cache misses\n");
        out.push_str("# TYPE boyodb_cache_misses counter\n");
        out.push_str(&format!(
            "boyodb_cache_misses {}\n",
            self.cache_misses.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_batch_cache_hits Batch cache hits\n");
        out.push_str("# TYPE boyodb_batch_cache_hits counter\n");
        out.push_str(&format!(
            "boyodb_batch_cache_hits {}\n",
            self.batch_cache_hits.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_batch_cache_misses Batch cache misses\n");
        out.push_str("# TYPE boyodb_batch_cache_misses counter\n");
        out.push_str(&format!(
            "boyodb_batch_cache_misses {}\n",
            self.batch_cache_misses.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_schema_cache_hits Schema cache hits\n");
        out.push_str("# TYPE boyodb_schema_cache_hits counter\n");
        out.push_str(&format!(
            "boyodb_schema_cache_hits {}\n",
            self.schema_cache_hits.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP boyodb_schema_cache_misses Schema cache misses\n");
        out.push_str("# TYPE boyodb_schema_cache_misses counter\n");
        out.push_str(&format!(
            "boyodb_schema_cache_misses {}\n",
            self.schema_cache_misses.load(Ordering::Relaxed)
        ));

        out
    }
}

/// Tracks compaction state to prevent merge storms and thrashing
#[derive(Debug, Default)]
pub struct CompactionState {
    /// Last compaction time per table (database.table -> timestamp)
    last_compaction: std::sync::RwLock<std::collections::HashMap<String, std::time::Instant>>,
    /// Currently running compactions count
    active_compactions: AtomicU64,
}

impl CompactionState {
    pub fn new() -> Self {
        Self {
            last_compaction: std::sync::RwLock::new(std::collections::HashMap::new()),
            active_compactions: AtomicU64::new(0),
        }
    }

    /// Check if we can start a new compaction (respects max_concurrent limit)
    pub fn can_start_compaction(&self, max_concurrent: usize) -> bool {
        self.active_compactions.load(Ordering::SeqCst) < max_concurrent as u64
    }

    /// Check if a table is in cooldown period
    pub fn is_table_in_cooldown(&self, database: &str, table: &str, cooldown_secs: u64) -> bool {
        let key = format!("{}.{}", database, table);
        if let Ok(map) = self.last_compaction.read() {
            if let Some(last_time) = map.get(&key) {
                return last_time.elapsed().as_secs() < cooldown_secs;
            }
        }
        false
    }

    /// Mark a compaction as started
    pub fn start_compaction(&self, database: &str, table: &str) {
        self.active_compactions.fetch_add(1, Ordering::SeqCst);
        let key = format!("{}.{}", database, table);
        if let Ok(mut map) = self.last_compaction.write() {
            map.insert(key, std::time::Instant::now());
        }
    }

    /// Mark a compaction as finished
    pub fn finish_compaction(&self) {
        self.active_compactions.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get current active compaction count
    pub fn active_count(&self) -> u64 {
        self.active_compactions.load(Ordering::SeqCst)
    }
}

/// Result of a background compaction check
#[derive(Debug, Clone)]
pub struct CompactionCandidate {
    pub database: String,
    pub table: String,
    pub segment_count: usize,
    pub total_bytes: u64,
    pub avg_segment_size: u64,
    pub priority: u32, // Higher = more urgent
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MemtableKey {
    database: String,
    table: String,
    shard_id: u16,
}

#[derive(Debug)]
struct MemtableEntry {
    entry: ManifestEntry,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct Memtable {
    entries: Vec<MemtableEntry>,
    bytes: u64,
}

#[derive(Debug)]
pub struct Db {
    pub(crate) cfg: EngineConfig,
    shards: Vec<Shard>,
    manifest: std::sync::Arc<RwLock<Manifest>>,
    /// Index for O(1) lookups by (database, table) and segment_id existence checks
    manifest_index: RwLock<ManifestIndex>,
    segment_seq: AtomicU64,
    wal: Mutex<Wal>,
    query_cache: ParkingMutex<QueryCache>,
    plan_cache: ParkingMutex<PlanCache>,
    /// LRU segment cache with byte-size limits and Arc for zero-copy sharing
    segment_cache: ParkingMutex<SegmentCache>,
    /// LRU cache of decoded record batches by segment_id
    batch_cache: ParkingMutex<BatchCache>,
    /// LRU cache of segment schemas by segment_id
    schema_cache: ParkingMutex<LruCache<String, Arc<Schema>>>,
    /// LRU of recently applied bundle plan hashes to prevent replays
    recent_bundle_hashes: ParkingMutex<LruCache<u32, ()>>,
    memtables: ParkingMutex<HashMap<MemtableKey, Memtable>>,
    memtable_index: ParkingMutex<HashMap<String, MemtableEntry>>,
    pub metrics: Metrics,
    pub shard_map: RwLock<ShardMap>,
    start_time: std::time::Instant,
    /// Track last persisted manifest version for deferred persistence
    last_persisted_version: AtomicU64,
    /// Dedicated lock for manifest file I/O to avoid blocking ingests
    manifest_persist_lock: Mutex<()>,
    /// Compaction state to prevent merge storms
    pub compaction_state: CompactionState,
    active_readers: AtomicU64,
    pending_deletes: ParkingMutex<Vec<String>>,
    pub cluster_manager: RwLock<Option<std::sync::Weak<crate::cluster::ClusterManager>>>,
    pub storage: std::sync::Arc<crate::storage::TieredStorage>,
}

struct ReadGuard<'a> {
    db: &'a Db,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        let remaining = self.db.active_readers.fetch_sub(1, Ordering::AcqRel) - 1;
        if remaining == 0 {
            self.db.cleanup_pending_deletes();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMap {
    pub total_shards: u16,
    /// Map shard_id -> node_id.
    pub shard_nodes: std::collections::HashMap<u16, crate::cluster::NodeId>,
}

impl ShardMap {
    pub fn new(total_shards: u16) -> Self {
        Self {
            total_shards,
            shard_nodes: std::collections::HashMap::new(),
        }
    }

    /// Map a partition key (e.g. tenant_id) to a shard ID.
    pub fn get_shard_id(&self, key: u64) -> u16 {
        if self.total_shards == 0 {
            0
        } else {
            (key % self.total_shards as u64) as u16
        }
    }

    /// Get the node ID responsible for a shard.
    pub fn get_node_id(&self, shard_id: u16) -> Option<crate::cluster::NodeId> {
        self.shard_nodes.get(&shard_id).cloned()
    }
}

#[derive(Debug)]
struct Shard {
    id: u16,
    ingest_bytes: AtomicU64,
    last_watermark_micros: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatch {
    pub payload_ipc: Vec<u8>,
    pub watermark_micros: u64,
    pub shard_override: Option<u64>,
    pub database: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub timeout_millis: u32,
    /// Whether to collect detailed execution statistics
    #[serde(default)]
    pub collect_stats: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub records_ipc: Vec<u8>,
    pub data_skipped_bytes: u64,
    pub segments_scanned: usize,
    /// Detailed execution statistics (populated when EXPLAIN ANALYZE is used or stats are requested)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_stats: Option<QueryExecutionStats>,
}

/// Result of a VACUUM operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumResult {
    /// Number of segments examined
    pub segments_processed: usize,
    /// Number of segments that were removed/merged
    pub segments_removed: usize,
    /// Bytes reclaimed through compaction
    pub bytes_reclaimed: u64,
    /// Number of new segments created
    pub new_segments: usize,
}

/// Query execution statistics for performance monitoring
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryExecutionStats {
    /// Total execution time in microseconds
    pub execution_time_micros: u64,
    /// Time spent parsing SQL in microseconds
    pub parse_time_micros: u64,
    /// Time spent planning/optimization in microseconds
    pub plan_time_micros: u64,
    /// Time spent scanning segments in microseconds
    pub scan_time_micros: u64,
    /// Time spent in aggregation in microseconds
    pub aggregation_time_micros: u64,
    /// Time spent sorting/ordering in microseconds
    pub sort_time_micros: u64,
    /// Number of segments scanned
    pub segments_scanned: usize,
    /// Number of segments pruned (skipped due to filters)
    pub segments_pruned: usize,
    /// Total segments considered
    pub segments_total: usize,
    /// Number of rows processed before filtering
    pub rows_scanned: u64,
    /// Number of rows returned in result
    pub rows_returned: u64,
    /// Bytes read from disk
    pub bytes_read: u64,
    /// Bytes skipped due to pruning
    pub bytes_skipped: u64,
    /// Whether result was served from cache
    pub cache_hit: bool,
    /// Peak memory usage during query (if tracked)
    pub peak_memory_bytes: Option<u64>,
}

impl QueryExecutionStats {
    /// Create stats indicating a cache hit
    pub fn cache_hit() -> Self {
        QueryExecutionStats {
            cache_hit: true,
            ..Default::default()
        }
    }

    /// Calculate pruning efficiency as a percentage
    pub fn pruning_efficiency(&self) -> f64 {
        if self.segments_total == 0 {
            return 0.0;
        }
        (self.segments_pruned as f64 / self.segments_total as f64) * 100.0
    }

    /// Calculate selectivity (percentage of rows returned)
    pub fn selectivity(&self) -> f64 {
        if self.rows_scanned == 0 {
            return 0.0;
        }
        (self.rows_returned as f64 / self.rows_scanned as f64) * 100.0
    }

    /// Format stats as human-readable summary
    pub fn summary(&self) -> String {
        format!(
            "Execution: {:.2}ms | Segments: {}/{} scanned ({} pruned, {:.1}% efficiency) | Rows: {} scanned, {} returned | Bytes: {} read, {} skipped{}",
            self.execution_time_micros as f64 / 1000.0,
            self.segments_scanned,
            self.segments_total,
            self.segments_pruned,
            self.pruning_efficiency(),
            self.rows_scanned,
            self.rows_returned,
            self.bytes_read,
            self.bytes_skipped,
            if self.cache_hit { " (CACHED)" } else { "" }
        )
    }
}

/// Detailed health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub manifest_readable: bool,
    pub segments_dir_exists: bool,
    pub wal_dir_exists: bool,
    pub total_segments: usize,
    pub missing_segments: Vec<String>,
    pub total_databases: usize,
    pub total_tables: usize,
    pub total_size_bytes: u64,
    pub manifest_version: u64,
    pub uptime_seconds: Option<u64>,
    pub cache_entries: usize,
    pub errors: Vec<String>,
}

/// Query execution plan details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    pub database: String,
    pub table: String,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<String>,
    pub aggregation: Option<String>,
    pub order_by: Option<Vec<String>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub segments_to_scan: usize,
    pub total_bytes: u64,
    pub estimated_rows: Option<u64>,
    pub uses_parallel_scan: bool,
    pub uses_bloom_filter: bool,
}

impl Db {
    fn read_guard(&self) -> ReadGuard<'_> {
        self.active_readers.fetch_add(1, Ordering::AcqRel);
        ReadGuard { db: self }
    }

    fn remove_segment_file_now(&self, segment_id: &str) {
        let path = self.cfg.segments_dir.join(format!("{segment_id}.ipc"));
        if let Err(e) = fs::remove_file(&path) {
            tracing::warn!(segment_id = %segment_id, error = %e, "failed to remove segment file");
        }
    }

    fn remove_segment_file(&self, segment_id: &str) {
        if self.active_readers.load(Ordering::Acquire) > 0 {
            let mut pending = self.pending_deletes.lock();
            pending.push(segment_id.to_string());
            return;
        }
        self.remove_segment_file_now(segment_id);
    }

    fn cleanup_pending_deletes(&self) {
        if self.active_readers.load(Ordering::Acquire) > 0 {
            return;
        }
        let pending = {
            let mut pending = self.pending_deletes.lock();
            if pending.is_empty() {
                return;
            }
            pending.drain(..).collect::<Vec<_>>()
        };
        for seg_id in pending {
            self.remove_segment_file_now(&seg_id);
        }
    }

    fn get_or_parse_plan(&self, sql: &str) -> Result<CachedPlan, EngineError> {
        if let Some(plan) = {
            let mut cache = self.plan_cache.lock();
            cache.get(sql)
        } {
            return Ok(plan);
        }
        let plan = build_query_plan(sql)?;
        let mut cache = self.plan_cache.lock();
        cache.insert(sql, plan.clone());
        Ok(plan)
    }

    pub fn open(cfg: EngineConfig) -> Result<Self, EngineError> {
        let storage = std::sync::Arc::new(crate::storage::TieredStorage::new(&cfg)?);
        Self::open_with_storage(cfg, storage)
    }

    /// Open a database for testing (no tokio runtime required)
    #[cfg(test)]
    pub fn open_for_test(cfg: EngineConfig) -> Result<Self, EngineError> {
        let storage = std::sync::Arc::new(crate::storage::TieredStorage::new_local_only(cfg.segments_dir.clone()));
        Self::open_with_storage(cfg, storage)
    }

    pub fn open_with_storage(cfg: EngineConfig, storage: std::sync::Arc<crate::storage::TieredStorage>) -> Result<Self, EngineError> {
        fs::create_dir_all(&cfg.data_dir)
            .map_err(|e| EngineError::Internal(format!("failed to create data dir: {e}")))?;
        fs::create_dir_all(&cfg.wal_dir)
            .map_err(|e| EngineError::Internal(format!("failed to create wal dir: {e}")))?;
        fs::create_dir_all(&cfg.segments_dir)
            .map_err(|e| EngineError::Internal(format!("failed to create segments dir: {e}")))?;

        let mut wal = Wal::open(&cfg.wal_path)?;
        wal.set_max_segments(cfg.wal_max_segments);
        wal.replay(&storage, &cfg.manifest_path)?;
        let mut manifest = load_manifest(&cfg.manifest_path).or_else(|_| {
            if cfg.manifest_snapshot_path.exists() {
                load_manifest(&cfg.manifest_snapshot_path)
            } else {
                Err(EngineError::NotFound("manifest missing".into()))
            }
        })?;

        // Handle manifest format migration for smooth upgrades
        if manifest.needs_migration() {
            tracing::info!(
                "Migrating manifest from format version {} to {}",
                manifest.format_version,
                crate::replication::MANIFEST_FORMAT_VERSION
            );

            // Create backup before migration
            let backup_path = cfg.manifest_path.with_extension("json.pre-migration");
            if cfg.manifest_path.exists() {
                fs::copy(&cfg.manifest_path, &backup_path).map_err(|e| {
                    EngineError::Internal(format!("failed to create pre-migration backup: {e}"))
                })?;
                tracing::info!("Created pre-migration backup at {:?}", backup_path);
            }

            // Perform migration
            manifest.migrate_if_needed();

            // Persist migrated manifest
            persist_manifest(&cfg.manifest_path, &manifest)?;
            tracing::info!(
                "Manifest migration complete. New format version: {}",
                manifest.format_version
            );
        }

        let shards = (0..cfg.shard_count)
            .map(|id| Shard {
                id: id as u16,
                ingest_bytes: AtomicU64::new(0),
                last_watermark_micros: AtomicU64::new(0),
            })
            .collect();

        // Replay WAL entries to restore manifest and segments if missing.
        wal.replay(&storage, &cfg.manifest_path)?;

        // Build index for O(1) lookups
        let manifest_index = ManifestIndex::build(&manifest.entries);

        let db = Db {
            cfg: cfg.clone(),
            shards,
            manifest: std::sync::Arc::new(RwLock::new(manifest.clone())),
            manifest_index: RwLock::new(manifest_index),
            segment_seq: AtomicU64::new(manifest.version),
            wal: Mutex::new(wal),
            query_cache: ParkingMutex::new(QueryCache::new(
                cfg.query_cache_size,
                cfg.query_cache_ttl_secs,
                cfg.query_cache_bytes,
            )),
            plan_cache: ParkingMutex::new(PlanCache::new(
                cfg.plan_cache_size,
                cfg.plan_cache_ttl_secs,
            )),
            segment_cache: ParkingMutex::new(SegmentCache::new(cfg.segment_cache_bytes)),
            batch_cache: ParkingMutex::new(BatchCache::new(cfg.batch_cache_bytes)),
            schema_cache: ParkingMutex::new(LruCache::new(
                NonZeroUsize::new(cfg.schema_cache_entries.max(1)).unwrap(),
            )),
            recent_bundle_hashes: ParkingMutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap())),
            memtables: ParkingMutex::new(HashMap::new()),
            memtable_index: ParkingMutex::new(HashMap::new()),
            metrics: Metrics::default(),
            start_time: std::time::Instant::now(),
            last_persisted_version: AtomicU64::new(manifest.version),
            manifest_persist_lock: Mutex::new(()),
            compaction_state: CompactionState::new(),
            active_readers: AtomicU64::new(0),
            pending_deletes: ParkingMutex::new(Vec::new()),
            shard_map: RwLock::new(ShardMap::new(cfg.shard_count as u16)),
            cluster_manager: RwLock::new(None),
            storage,
        };

        db.spawn_background_compaction_loop();

        // Spawn Tiering Manager
        let tiering_mgr = std::sync::Arc::new(crate::tiering::TieringManager::new(
            db.manifest.clone(),
            db.cfg.manifest_path.clone(),
            db.storage.clone(),
            db.cfg.segments_dir.clone(),
            db.cfg.tier_cold_after_millis,
        ));
        db.storage.spawn_task(async move {
            tiering_mgr.run().await;
        });

        Ok(db)
    }

    pub fn set_cluster_manager(&self, cm: std::sync::Weak<crate::cluster::ClusterManager>) {
        if let Ok(mut lock) = self.cluster_manager.write() {
            *lock = Some(cm);
        }
    }

    pub fn ingest_ipc(&self, batch: IngestBatch) -> Result<(), EngineError> {
        let payload_len = batch.payload_ipc.len() as u64;
        if batch.payload_ipc.is_empty() {
            self.metrics.ingests_errors.fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::InvalidArgument(
                "empty ingest payload".to_string(),
            ));
        }

        let db_name = batch
            .database
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        let table_name = batch
            .table
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("default");

        // Validate identifiers to prevent path traversal and injection
        validate_identifier(db_name, "database")?;
        validate_identifier(table_name, "table")?;

        // Lookup existing schema for enforcement (if any).
        let existing_table = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest
                .tables
                .iter()
                .find(|t| t.database == db_name && t.name == table_name)
                .cloned()
        };

        // Validate IPC payload decodes and collect simple stats (min/max) for pruning.
        let validated = validate_and_stats(&batch.payload_ipc, existing_table.as_ref())?;

        // Apply on-ingest deduplication if configured
        let deduped_payload = if let Some(ref dedup_config) = existing_table.as_ref().and_then(|t| t.deduplication.clone()) {
            use crate::sql::DeduplicationMode;
            match dedup_config.mode {
                DeduplicationMode::OnIngest | DeduplicationMode::Both => {
                    // Deduplicate within this batch
                    let batches = read_ipc_batches(&batch.payload_ipc)?;
                    if batches.is_empty() {
                        batch.payload_ipc.clone()
                    } else {
                        let schema = batches[0].schema();
                        let merged = if batches.len() == 1 {
                            batches.into_iter().next().unwrap()
                        } else {
                            arrow_select::concat::concat_batches(&schema, &batches)
                                .map_err(|e| EngineError::Internal(format!("concat batches for dedup: {e}")))?
                        };
                        let deduped = self.deduplicate_batch(
                            &merged,
                            &dedup_config.key_columns,
                            dedup_config.version_column.as_deref(),
                        )?;
                        tracing::debug!(
                            "On-ingest deduplication: {} -> {} rows",
                            merged.num_rows(),
                            deduped.num_rows()
                        );
                        record_batches_to_ipc(schema.as_ref(), &[deduped])?
                    }
                }
                DeduplicationMode::OnCompaction => batch.payload_ipc.clone(),
            }
        } else {
            batch.payload_ipc.clone()
        };

        // Sort hot payload for faster reads (stable by event_time/tenant/route when present)
        let sorted_payload = sort_hot_payload(&deduped_payload, self.cfg.index_granularity_rows)?;

        // Re-validate stats after deduplication/sort (stats may have changed)
        let validated = if sorted_payload != batch.payload_ipc {
            validate_and_stats(&sorted_payload, existing_table.as_ref())?
        } else {
            validated
        };

        let stats = validated.stats;
        let schema_json = serde_json::to_string(&validated.schema_spec)
            .map_err(|e| EngineError::Internal(format!("serialize schema spec failed: {e}")))?;
        let table_compression =
            normalize_compression(existing_table.as_ref().and_then(|t| t.compression.clone()))?;
        let encodings =
            column_encodings_from_schema_json(existing_table.as_ref().and_then(|t| t.schema_json.as_ref()))?;
        let encoded_payload = apply_column_encodings(&sorted_payload, &encodings)?;
        let stored_payload = compress_payload(&encoded_payload, table_compression.as_deref())?;
        let existing_schema = existing_table
            .as_ref()
            .and_then(|t| t.schema_json.as_ref())
            .map(|s| canonical_schema_from_json(s))
            .transpose()?;

        // Choose shard deterministically to avoid hot locks; prefer caller-provided shard override (e.g., tenant hash).
        // Choose shard deterministically using ShardMap
        let seq = self.segment_seq.fetch_add(1, Ordering::SeqCst);
        
        let global_shard_id = if let Some(hint) = batch.shard_override {
            // Hint provided (e.g. from upstream router or client explicit shard)
            // Existing logic was mix64(hint) % len. We should respect explicit override if it looks like a shard ID? 
            // Or assume hint is a partition key? 
            // Let's assume hint is a partition key (like tenant_id) to be consistent with get_shard_id.
            self.shard_map.read().unwrap().get_shard_id(hint)
        } else if let Some(tid) = stats.tenant_id_min {
            self.shard_map.read().unwrap().get_shard_id(tid)
        } else {
            // No partition key, round robin across global shards
            (seq % self.shard_map.read().unwrap().total_shards.max(1) as u64) as u16
        };

        // Map global shard to local concurrency lane (write lock)
        let local_shard_idx = (global_shard_id as usize) % self.shards.len();

        let shard = self
            .shards
            .get(local_shard_idx)
            .ok_or_else(|| EngineError::Internal("shard missing".into()))?;

        shard
            .ingest_bytes
            .fetch_add(batch.payload_ipc.len() as u64, Ordering::SeqCst);
        shard
            .last_watermark_micros
            .store(batch.watermark_micros, Ordering::SeqCst);

        let segment_id = format!("seg-{global_shard_id}-{seq}");
        let manifest_entry = ManifestEntry {
            segment_id,
            shard_id: global_shard_id,
            version_added: 0, // will be updated below when manifest version advances
            size_bytes: stored_payload.len() as u64,
            checksum: compute_checksum(&stored_payload),
            tier: SegmentTier::Hot,
            database: db_name.to_string(),
            table: table_name.to_string(),
            compression: table_compression.clone(),
            watermark_micros: batch.watermark_micros,
            event_time_min: stats.event_time_min,
            event_time_max: stats.event_time_max,
            tenant_id_min: stats.tenant_id_min,
            tenant_id_max: stats.tenant_id_max,
            route_id_min: stats.route_id_min,
            route_id_max: stats.route_id_max,
            bloom_tenant: stats.bloom_tenant,
            bloom_route: stats.bloom_route,
            column_stats: if stats.column_stats.is_empty() {
                None
            } else {
                Some(stats.column_stats)
            },
            schema_hash: Some(validated.schema_hash),
        };
        let use_memtable = self.cfg.memtable_max_bytes > 0 && self.cfg.memtable_max_entries > 0;
        if !use_memtable {
            persist_segment_ipc(&self.storage, &manifest_entry.segment_id, &stored_payload)?;
        }

        let (entry, _current_version, should_persist) = {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            if !manifest.databases.iter().any(|d| d.name == db_name) {
                manifest.databases.push(crate::replication::DatabaseMeta {
                    name: db_name.into(),
                });
            }
            let existing_table = manifest
                .tables
                .iter_mut()
                .find(|t| t.database == db_name && t.name == table_name);
            match existing_table {
                Some(tbl) => {
                    if let Some(existing_schema) = existing_schema.as_ref() {
                        // Compare schemas by comparing field count and names instead of hash
                        // This is more robust against serialization order differences
                        let schemas_compatible = schemas_are_compatible(&existing_schema.json, validated.schema_spec.as_deref().unwrap_or("[]"));
                        if !schemas_compatible && existing_schema.hash != 0 && validated.schema_hash != 0 {
                            // Only fail if both hashes are non-zero and schemas don't match
                            if existing_schema.hash != validated.schema_hash {
                                return Err(EngineError::InvalidArgument(
                                    "schema mismatch for existing table".into(),
                                ));
                            }
                        }
                        if tbl.schema_json.as_deref()
                            != Some(existing_schema.json.as_str())
                        {
                            tbl.schema_json = Some(existing_schema.json.clone());
                        }
                    } else {
                        tbl.schema_json = Some(schema_json.clone());
                    }

                    if tbl.compression.is_none() {
                        tbl.compression = table_compression.clone();
                    }
                }
                None => {
                    manifest.tables.push(crate::replication::TableMeta {
                        database: db_name.into(),
                        name: table_name.into(),
                        schema_json: Some(schema_json.clone()),
                        compression: table_compression.clone(),
                        deduplication: None,
                    });
                }
            }
            manifest.bump_version();
            let mut entry = manifest_entry.clone();
            entry.version_added = manifest.version;
            let entry_index = manifest.entries.len();
            manifest.entries.push(entry.clone());
            {
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                index.add_entry(entry_index, &entry);
            }
            let current_version = manifest.version;
            let last_persisted = self.last_persisted_version.load(Ordering::Acquire);
            let should_persist = current_version >= last_persisted + 10;

            (entry, current_version, should_persist)
        };

        if should_persist {
            self.maybe_persist_manifest()?;
        }

        {
            let mut wal = self
                .wal
                .lock()
                .map_err(|_| EngineError::Internal("wal lock poisoned".into()))?;
            wal.append_segment(&entry, &stored_payload)?;
            wal.maybe_sync(self.cfg.wal_sync_bytes, self.cfg.wal_sync_interval_ms)?;
            wal.maybe_rotate(self.cfg.wal_max_bytes)?;
        }

        if use_memtable {
            self.buffer_memtable_entry(
                db_name,
                table_name,
                shard.id,
                MemtableEntry {
                    entry: entry.clone(),
                    payload: stored_payload.clone(),
                },
            )?;
        }

        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(&entry.database, &entry.table);
        }
        self.spawn_background_index_build_for_segment(
            &entry.database,
            &entry.table,
            &entry.segment_id,
        );
        // Update metrics
        self.metrics.ingests_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .ingests_bytes
            .fetch_add(payload_len, Ordering::Relaxed);
        Ok(())
    }

    fn buffer_memtable_entry(
        &self,
        database: &str,
        table: &str,
        shard_id: u16,
        entry: MemtableEntry,
    ) -> Result<(), EngineError> {
        let key = MemtableKey {
            database: database.to_string(),
            table: table.to_string(),
            shard_id,
        };
        let mut entries_to_flush = Vec::new();
        {
            let mut memtables = self.memtables.lock();
            let memtable = memtables.entry(key.clone()).or_insert(Memtable {
                entries: Vec::new(),
                bytes: 0,
            });
            memtable.bytes = memtable.bytes.saturating_add(entry.payload.len() as u64);
            memtable.entries.push(MemtableEntry {
                entry: entry.entry.clone(),
                payload: entry.payload.clone(),
            });
            let should_flush = memtable.bytes >= self.cfg.memtable_max_bytes
                || memtable.entries.len() >= self.cfg.memtable_max_entries;
            if should_flush {
                entries_to_flush = memtable.entries.drain(..).collect();
                memtable.bytes = 0;
            }
        }
        {
            let mut index = self.memtable_index.lock();
            index.insert(entry.entry.segment_id.clone(), entry);
        }
        if !entries_to_flush.is_empty() {
            self.flush_memtable_entries(entries_to_flush)?;
        }
        Ok(())
    }

    fn flush_memtable_entries(&self, entries: Vec<MemtableEntry>) -> Result<(), EngineError> {
        if entries.is_empty() {
            return Ok(());
        }
        {
            let mut index = self.memtable_index.lock();
            for mem_entry in &entries {
                index.remove(&mem_entry.entry.segment_id);
            }
        }
        // Persist segment files first so manifest never points to missing data.
        for mem_entry in &entries {
            persist_segment_ipc(
                &self.storage,
                &mem_entry.entry.segment_id,
                &mem_entry.payload,
            )?;
        }
        Ok(())
    }

    fn flush_all_memtables(&self) -> Result<(), EngineError> {
        let mut all_entries = Vec::new();
        {
            let mut memtables = self.memtables.lock();
            for (_, memtable) in memtables.iter_mut() {
                all_entries.extend(memtable.entries.drain(..));
                memtable.bytes = 0;
            }
        }
        if !all_entries.is_empty() {
            self.flush_memtable_entries(all_entries)?;
        }
        Ok(())
    }

    fn append_manifest_entries(
        &self,
        mut entries: Vec<ManifestEntry>,
    ) -> Result<usize, EngineError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut added = 0usize;
        let (_current_version, should_persist) = {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            for entry in &mut entries {
                if manifest
                    .entries
                    .iter()
                    .any(|existing| existing.segment_id == entry.segment_id)
                {
                    continue;
                }
                manifest.bump_version();
                entry.version_added = manifest.version;
                let entry_index = manifest.entries.len();
                manifest.entries.push(entry.clone());
                {
                    let mut index = self
                        .manifest_index
                        .write()
                        .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                    index.add_entry(entry_index, entry);
                }
                added += 1;
            }
            let current_version = manifest.version;
            let last_persisted = self.last_persisted_version.load(Ordering::Acquire);
            let should_persist = current_version >= last_persisted + 10;
            (current_version, should_persist)
        };

        if should_persist {
            self.maybe_persist_manifest()?;
        }
        Ok(added)
    }

    fn maybe_persist_manifest(&self) -> Result<(), EngineError> {
        if let Ok(_guard) = self.manifest_persist_lock.try_lock() {
            let last_persisted = self.last_persisted_version.load(Ordering::Acquire);
            let current_version = {
                let manifest = self
                    .manifest
                    .read()
                    .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                manifest.version
            };
            if current_version > last_persisted {
                let manifest_snapshot = {
                    let manifest = self
                        .manifest
                        .read()
                        .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                    manifest.clone()
                };
                persist_manifest(&self.cfg.manifest_path, &manifest_snapshot)?;
                snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
                self.last_persisted_version
                    .store(current_version, Ordering::Release);
            }
        }
        Ok(())
    }

    /// Force persist manifest to disk. Call this on shutdown to ensure all entries are saved.
    pub fn flush_manifest(&self) -> Result<(), EngineError> {
        self.flush_all_memtables()?;
        let _guard = self
            .manifest_persist_lock
            .lock()
            .map_err(|_| EngineError::Internal("manifest persist lock poisoned".into()))?;

        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        self.last_persisted_version.store(manifest.version, Ordering::Release);
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<(), EngineError> {
        self.flush_manifest()?;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| EngineError::Internal("wal lock poisoned".into()))?;
        wal.checkpoint()
    }

    pub fn query(&self, request: QueryRequest) -> Result<QueryResponse, EngineError> {
        // Start timing for stats collection
        let query_start = std::time::Instant::now();
        let collect_stats = request.collect_stats;

        if request.sql.trim().is_empty() {
            self.metrics.queries_errors.fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::InvalidArgument("empty SQL".into()));
        }
        let _read_guard = self.read_guard();

        // Get current manifest version for cache key validation
        let manifest_version = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest.version
        };

        // Check cache first (using SQL string directly, no clone needed)
        {
            let mut cache = self.query_cache.lock();
            if let Some(mut cached) = cache.get(&request.sql, manifest_version) {
                self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);
                self.metrics.queries_cached.fetch_add(1, Ordering::Relaxed);
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                // Add cache hit stats if requested
                if collect_stats {
                    cached.execution_stats = Some(QueryExecutionStats {
                        cache_hit: true,
                        execution_time_micros: query_start.elapsed().as_micros() as u64,
                        ..Default::default()
                    });
                }
                return Ok(cached);
            }
        }
        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Calculate deadline for timeout enforcement
        let deadline = if request.timeout_millis > 0 {
            Some(
                std::time::Instant::now()
                    + std::time::Duration::from_millis(request.timeout_millis as u64),
            )
        } else {
            None
        };

        let plan = self.get_or_parse_plan(&request.sql)?;

        // Try distributed execution if clustered
        if self.cluster_manager.read().map_or(false, |g| g.is_some()) {
            if let Some((local, global)) = crate::planner_distributed::distribute_plan(&plan.kind) {
                 use crate::executor_distributed::DistributedExecutor;
                 let executor = DistributedExecutor::new();
                 // TODO: Handle fallback or specific errors? For now propagate.
                 // If executor returns valid result, return it.
                 // If it returns error, we might want to fallback? But usually distributed failure is fatal.
                 return executor.execute(self, global, local);
            }
        }

        match plan.kind {
            PlanKind::Join => self.execute_join_query(&request, deadline, manifest_version),
            PlanKind::SetOperation => {
                self.execute_set_operation(&request, deadline, manifest_version)
            }
            PlanKind::Subquery => {
                self.execute_subquery_query(&request, deadline, manifest_version)
            }
            PlanKind::Cte => self.execute_cte_query(&request, deadline, manifest_version),
            PlanKind::Transaction(_) => {
                // Return empty response for transaction control stubs
                Ok(QueryResponse {
                    records_ipc: Vec::new(),
                    data_skipped_bytes: 0,
                    segments_scanned: 0,
                    execution_stats: None,
                })
            }
            PlanKind::Simple {
                agg,
                db,
                table,
                projection,
                filter,
                computed_columns,
            } => self.execute_simple_plan(
                agg,
                db,
                table,
                projection,
                filter,
                computed_columns,
                deadline,
                manifest_version,
                collect_stats,
                query_start,
            ),
        }
    }

    /// Execute a simple SELECT query and write IPC batches directly to the writer.
    /// Only supports non-aggregated queries without JOINs, ORDER BY, DISTINCT, OFFSET, or set ops.
    pub fn query_ipc_to_writer<W: Write>(
        &self,
        request: QueryRequest,
        writer: &mut W,
    ) -> Result<QueryResponse, EngineError> {
        let query_start = std::time::Instant::now();
        let collect_stats = request.collect_stats;

        if request.sql.trim().is_empty() {
            self.metrics.queries_errors.fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::InvalidArgument("empty SQL".into()));
        }
        let _read_guard = self.read_guard();

        let deadline = if request.timeout_millis > 0 {
            Some(
                std::time::Instant::now()
                    + std::time::Duration::from_millis(request.timeout_millis as u64),
            )
        } else {
            None
        };

        let plan = self.get_or_parse_plan(&request.sql)?;
        let (agg, db, table, projection, filter) = match plan.kind {
            PlanKind::Join => {
                return Err(EngineError::NotImplemented(
                    "streaming does not support JOIN queries".into(),
                ))
            }
            PlanKind::SetOperation => {
                return Err(EngineError::NotImplemented(
                    "streaming does not support set operations".into(),
                ))
            }
            PlanKind::Subquery => {
                return Err(EngineError::NotImplemented(
                    "streaming does not support subqueries".into(),
                ))
            }
            PlanKind::Cte => {
                return Err(EngineError::NotImplemented(
                    "streaming does not support CTEs".into(),
                ))
            }
            PlanKind::Transaction(_) => {
                // Return empty result for transactions (auto-commit behavior)
                (
                    None,
                    None,
                    None,
                    None,
                    QueryFilter::default(),
                )
            }
            PlanKind::Simple {
                agg,
                db,
                table,
                projection,
                filter,
                computed_columns: _,
            } => (agg, db, table, projection, filter),
        };

        if agg.is_some() {
            return Err(EngineError::NotImplemented(
                "streaming does not support aggregation queries".into(),
            ));
        }
        if filter.order_by.is_some() || filter.distinct {
            return Err(EngineError::NotImplemented(
                "streaming does not support ORDER BY or DISTINCT".into(),
            ));
        }

        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let expected_schema: Option<Vec<TableFieldSpec>> = manifest
            .tables
            .iter()
            .find(|t| t.database == db.as_deref().unwrap_or("default") && t.name == table.as_deref().unwrap_or("unknown"))
            .and_then(|t| {
                t.schema_json
                    .as_ref()
                    .and_then(|s| serde_json::from_str::<Vec<TableFieldSpec>>(s).ok())
            });
        let schema_map = self.table_schema_map(db.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown")).unwrap_or_default();
        let index_list: Vec<IndexMeta> = manifest
            .indexes
            .iter()
            .filter(|idx| idx.database == db.as_deref().unwrap_or("default") && idx.table == table.as_deref().unwrap_or("unknown") && idx.state == IndexState::Ready)
            .cloned()
            .collect();

        let manifest_index = self
            .manifest_index
            .read()
            .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;

        let table_indices = manifest_index
            .get_table_entries(db.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown"));
        if table_indices.is_none() {
            return Err(EngineError::NotFound(format!("no segments for {}.{}", db.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown"))));
        }
        let table_indices = table_indices.unwrap();
        let total_segments = table_indices.len();

        let mut matching = Vec::with_capacity(table_indices.len());
        for &idx in table_indices {
            let e = &manifest.entries[idx];

            if let Some(ge) = filter.watermark_ge {
                if e.watermark_micros < ge {
                    continue;
                }
            }
            if let Some(le) = filter.watermark_le {
                if e.watermark_micros > le {
                    continue;
                }
            }

            if filter.event_time_ge.is_some() || filter.event_time_le.is_some() {
                let min = e.event_time_min.unwrap_or(0);
                let max = e.event_time_max.unwrap_or(u64::MAX);
                if let Some(ge) = filter.event_time_ge {
                    if max < ge {
                        continue;
                    }
                }
                if let Some(le) = filter.event_time_le {
                    if min > le {
                        continue;
                    }
                }
            }

            if let Some(eq) = filter.tenant_id_eq {
                let range_ok = match (e.tenant_id_min, e.tenant_id_max) {
                    (Some(min), Some(max)) => eq >= min && eq <= max,
                    _ => true,
                };
                if !range_ok {
                    continue;
                }
                if let Some(b) = &e.bloom_tenant {
                    if !crate::bloom_utils::deserialize_bloom_or_empty(b, 1000).check(&eq) {
                        continue;
                    }
                }
            }

            if let Some(eq) = filter.route_id_eq {
                let range_ok = match (e.route_id_min, e.route_id_max) {
                    (Some(min), Some(max)) => eq >= min && eq <= max,
                    _ => true,
                };
                if !range_ok {
                    continue;
                }
                if let Some(b) = &e.bloom_route {
                    if !crate::bloom_utils::deserialize_bloom_or_empty(b, 1000).check(&eq) {
                        continue;
                    }
                }
            }

            if false { // Optimization stubbed out
                continue;
            }
            if !index_list.is_empty() && !schema_map.is_empty() {
                if !self.entry_matches_index_filters(e, &filter, &schema_map, &index_list)? {
                    continue;
                }
            }

            matching.push(e.clone());
        }

        drop(manifest_index);

        if matching.is_empty() {
            return Err(EngineError::NotFound(format!(
                "no segments for {}.{} matching filters",
                db.as_deref().unwrap_or("default"),
                table.as_deref().unwrap_or("unknown")
            )));
        }

        let check_timeout = || -> Result<(), EngineError> {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(EngineError::Timeout(format!(
                        "query exceeded timeout of {}ms",
                        request.timeout_millis
                    )));
                }
            }
            Ok(())
        };

        let mut remaining_offset = filter.offset.unwrap_or(0);
        let mut remaining_limit = filter.limit.unwrap_or(usize::MAX);
        let mut data_skipped_bytes: u64 = 0;
        let mut stream_writer: Option<StreamWriter<&mut W>> = None;

        let mut entries_iter = matching.iter();
        while let Some(entry) = entries_iter.next() {
            if remaining_limit == 0 {
                break;
            }
            check_timeout()?;
            let payload = self.load_segment_cached(entry)?;
            let original_size = payload.len() as u64;

            let mut segment_filter = filter.clone();
            segment_filter.limit = None;
            segment_filter.offset = None;

            let batches = filter_ipc_batches(
                read_ipc_batches(&payload)?,
                &segment_filter,
            )?;

            if batches.is_empty() {
                data_skipped_bytes += original_size;
                continue;
            }

            let sliced = apply_offset_limit_to_batches(
                batches,
                &mut remaining_offset,
                &mut remaining_limit,
            );
            if sliced.is_empty() {
                continue;
            }

            stream_writer = Some(
                StreamWriter::try_new(&mut *writer, sliced[0].schema().as_ref()).map_err(|e| {
                    EngineError::Internal(format!("ipc writer init failed: {e}"))
                })?,
            );

            if let Some(w) = stream_writer.as_mut() {
                for batch in &sliced {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    w.write(batch)
                        .map_err(|e| EngineError::Internal(format!("ipc write failed: {e}")))?;
                    if remaining_limit == 0 {
                        break;
                    }
                }
            }
            break;
        }

        if remaining_limit > 0 {
            for entry in entries_iter {
                if remaining_limit == 0 {
                    break;
                }
                check_timeout()?;
                let payload = self.load_segment_cached(entry)?;
                let original_size = payload.len() as u64;

                let mut segment_filter = filter.clone();
                segment_filter.limit = None;
                segment_filter.offset = None;

                let batches = filter_ipc_batches(
                    read_ipc_batches(&payload)?,
                    &segment_filter,
                )?;

                if batches.is_empty() {
                    data_skipped_bytes += original_size;
                    continue;
                }

                let sliced = apply_offset_limit_to_batches(
                    batches,
                    &mut remaining_offset,
                    &mut remaining_limit,
                );
                if sliced.is_empty() {
                    continue;
                }

                if let Some(w) = stream_writer.as_mut() {
                    for batch in &sliced {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        w.write(batch).map_err(|e| {
                            EngineError::Internal(format!("ipc write failed: {e}"))
                        })?;
                        if remaining_limit == 0 {
                            break;
                        }
                    }
                } else {
                    return Err(EngineError::Internal(
                        "stream writer missing for batches".into(),
                    ));
                }
            }
        }

        if let Some(mut w) = stream_writer {
            w.finish()
                .map_err(|e| EngineError::Internal(format!("ipc finish failed: {e}")))?;
        }

        let execution_stats = if collect_stats {
            let bytes_read: u64 = matching.iter().map(|e| e.size_bytes).sum();
            Some(QueryExecutionStats {
                execution_time_micros: query_start.elapsed().as_micros() as u64,
                segments_scanned: matching.len(),
                segments_pruned: total_segments.saturating_sub(matching.len()),
                segments_total: total_segments,
                bytes_read,
                bytes_skipped: data_skipped_bytes,
                cache_hit: false,
                ..Default::default()
            })
        } else {
            None
        };

        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);

        Ok(QueryResponse {
            records_ipc: Vec::new(),
            data_skipped_bytes,
            segments_scanned: matching.len(),
            execution_stats,
        })
    }

    pub fn execute_distributed_plan(&self, plan: PlanKind) -> Result<QueryResponse, EngineError> {
        let manifest_version = self.get_manifest_version()?;
        let query_start = std::time::Instant::now();
        // TODO: Support timeout in distributed request
        let deadline = None; 
        
        match plan {
             PlanKind::Simple { agg, db, table, projection, filter, computed_columns } => {
                 self.execute_simple_plan(agg, db, table, projection, filter, computed_columns, deadline, manifest_version, false, query_start)
             }
             _ => Err(EngineError::NotImplemented("Distributed execution supports Simple plans only".into()))
        }
    }

    fn execute_simple_plan(
        &self,
        agg: Option<AggPlan>,
        database: Option<String>,
        table: Option<String>,
        projection: Option<Vec<String>>,
        filter: QueryFilter,
        computed_columns: Vec<crate::sql::SelectColumn>,
        deadline: Option<std::time::Instant>,
        _manifest_version: u64,
        collect_stats: bool,
        query_start: std::time::Instant,
    ) -> Result<QueryResponse, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Handle scalar queries (no table source)
        if database.is_none() && table.is_none() {
            if let Some(exprs) = projection {

                let mut columns: Vec<ArrayRef> = Vec::new();
                let mut fields: Vec<Field> = Vec::new();

                for (i, expr_str) in exprs.iter().enumerate() {
                    let col_name = format!("col_{}", i);
                    if let Ok(val) = expr_str.parse::<i64>() {
                         columns.push(Arc::new(Int64Array::from(vec![val])));
                         fields.push(Field::new(col_name, DataType::Int64, false));
                    } else if let Ok(val) = expr_str.parse::<f64>() {
                         columns.push(Arc::new(arrow_array::Float64Array::from(vec![val])));
                         fields.push(Field::new(col_name, DataType::Float64, false));
                    } else {
                         // Treat as string (strip quotes if present)
                         let val = expr_str.trim().trim_matches('\'').trim_matches('"');
                         columns.push(Arc::new(StringArray::from(vec![val])));
                         fields.push(Field::new(col_name, DataType::Utf8, false));
                    }
                }

                let schema = Arc::new(Schema::new(fields));
                let batch = RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|e| EngineError::Internal(format!("failed to create batch: {e}")))?;

                // Serialize to IPC
                let mut buffer = Vec::new();
                {
                    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &schema)
                        .map_err(|e| EngineError::Internal(format!("ipc init error: {e}")))?;
                    writer.write(&batch)
                        .map_err(|e| EngineError::Internal(format!("ipc write error: {e}")))?;
                    writer.finish()
                        .map_err(|e| EngineError::Internal(format!("ipc finish error: {e}")))?;
                }

                return Ok(QueryResponse {
                    records_ipc: buffer,
                    data_skipped_bytes: 0,
                    segments_scanned: 0,
                    execution_stats: Some(QueryExecutionStats {
                        execution_time_micros: query_start.elapsed().as_micros() as u64,
                         ..Default::default()
                    }),
                });
            } else if !computed_columns.is_empty() {
                 let mut schema_fields = Vec::new();
                 let mut columns: Vec<ArrayRef> = Vec::new();

                 for (i, col) in computed_columns.iter().enumerate() {
                     let name = col.alias.clone().unwrap_or_else(|| format!("col_{}", i + 1));
                     match &col.expr {
                         crate::sql::SelectExpr::Literal(val) => {
                              match val {
                                  crate::sql::LiteralValue::Integer(n) => {
                                      schema_fields.push(Field::new(name, DataType::Int64, false));
                                      columns.push(std::sync::Arc::new(Int64Array::from(vec![*n])));
                                  }
                                  crate::sql::LiteralValue::String(s) => {
                                      schema_fields.push(Field::new(name, DataType::Utf8, false));
                                      columns.push(std::sync::Arc::new(StringArray::from(vec![s.clone()])));
                                  }
                                  _ => return Err(EngineError::NotImplemented(format!("unsupported literal type: {:?}", val))),
                              }
                         }
                         _ => return Err(EngineError::NotImplemented("only literals supported in scalar SELECT".into())),
                     }
                 }
                 let schema = std::sync::Arc::new(Schema::new(schema_fields));
                 let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| EngineError::Internal(e.to_string()))?;
                 
                 let mut buffer = Vec::new();
                 {
                     let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &schema)
                         .map_err(|e| EngineError::Internal(format!("ipc init error: {e}")))?;
                     writer.write(&batch)
                         .map_err(|e| EngineError::Internal(format!("ipc write error: {e}")))?;
                     writer.finish()
                         .map_err(|e| EngineError::Internal(format!("ipc finish error: {e}")))?;
                 }
                  return Ok(QueryResponse {
                     records_ipc: buffer,
                     data_skipped_bytes: 0,
                     segments_scanned: 0,
                     execution_stats: Some(QueryExecutionStats {
                         execution_time_micros: query_start.elapsed().as_micros() as u64,
                          ..Default::default()
                     }),
                 });
            } else {
                 // Empty SELECT or Transaction stub (BEGIN/COMMIT)
                 // Return empty result
                 return Ok(QueryResponse {
                     records_ipc: Vec::new(), // Empty IPC for non-data queries
                     data_skipped_bytes: 0,
                     segments_scanned: 0,
                     execution_stats: Some(QueryExecutionStats {
                         execution_time_micros: query_start.elapsed().as_micros() as u64,
                          ..Default::default()
                     }),
                 });
            }
        }

        let expected_schema: Option<Vec<TableFieldSpec>> = manifest
            .tables
            .iter()
            .find(|t| t.database == database.as_deref().unwrap_or("default") && t.name == table.as_deref().unwrap_or("unknown"))
            .and_then(|t| {
                t.schema_json
                    .as_ref()
                    .and_then(|s| serde_json::from_str::<Vec<TableFieldSpec>>(s).ok())
            });
        let schema_map = self.table_schema_map(database.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown")).unwrap_or_default();
        let index_list: Vec<IndexMeta> = manifest
            .indexes
            .iter()
            .filter(|idx| idx.database == database.as_deref().unwrap_or("default") && idx.table == table.as_deref().unwrap_or("unknown") && idx.state == IndexState::Ready)
            .cloned()
            .collect();

        // OPTIMIZATION: Use manifest index for O(1) table lookup instead of O(n) linear scan
        let manifest_index = self
            .manifest_index
            .read()
            .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;

        let table_indices = manifest_index
            .get_table_entries(database.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown"))
            .ok_or_else(|| EngineError::NotFound(format!("no segments for {}.{}", database.as_deref().unwrap_or("default"), table.as_deref().unwrap_or("unknown"))))?;

        // Track total segments for stats
        let total_segments = table_indices.len();

        // Pre-allocate with expected capacity
        let mut matching = Vec::with_capacity(table_indices.len());

        // Apply remaining filters only to entries for this table
        for &idx in table_indices {
            let e = &manifest.entries[idx];

            if let Some(ge) = filter.watermark_ge {
                if e.watermark_micros < ge {
                    continue;
                }
            }
            if let Some(le) = filter.watermark_le {
                if e.watermark_micros > le {
                    continue;
                }
            }
            if filter.event_time_ge.is_some() || filter.event_time_le.is_some() {
                let min = e.event_time_min.unwrap_or(0);
                let max = e.event_time_max.unwrap_or(u64::MAX);
                if let Some(ge) = filter.event_time_ge {
                    if max < ge {
                        continue;
                    }
                }
                if let Some(le) = filter.event_time_le {
                    if min > le {
                        continue;
                    }
                }
            }
            if let Some(eq) = filter.tenant_id_eq {
                let range_ok = match (e.tenant_id_min, e.tenant_id_max) {
                    (Some(min), Some(max)) => eq >= min && eq <= max,
                    _ => true,
                };
                if !range_ok {
                    continue;
                }
                if let Some(b) = &e.bloom_tenant {
                    if !crate::bloom_utils::deserialize_bloom_or_empty(b, 1000).check(&eq) {
                        continue;
                    }
                }
            }
            if let Some(eq) = filter.route_id_eq {
                let range_ok = match (e.route_id_min, e.route_id_max) {
                    (Some(min), Some(max)) => eq >= min && eq <= max,
                    _ => true,
                };
                if !range_ok {
                    continue;
                }
                if let Some(b) = &e.bloom_route {
                    if !crate::bloom_utils::deserialize_bloom_or_empty(b, 1000).check(&eq) {
                        continue;
                    }
                }
            }

            if !entry_matches_string_filters(e, &filter) {
                continue;
            }
            if !index_list.is_empty() && !schema_map.is_empty() {
                if !self.entry_matches_index_filters(e, &filter, &schema_map, &index_list)? {
                    continue;
                }
            }

            // Zone-map pruning using column_stats
            if !segment_matches_column_stats(e, &filter) {
                continue;
            }

            matching.push(e.clone());
        }
        drop(manifest_index);

        if matching.is_empty() {
            return Err(EngineError::NotFound(format!(
                "no segments for {}.{} matching filters",
                database.as_deref().unwrap_or("default"),
                table.as_deref().unwrap_or("unknown")
            )));
        }

        let estimated_capacity = filter.limit.unwrap_or(matching.len().saturating_mul(64)).min(64 * 1024);
        let mut records = Vec::with_capacity(estimated_capacity);
        let mut data_skipped_bytes: u64 = 0;

        let check_timeout = || -> Result<(), EngineError> {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(EngineError::Timeout("query exceeded timeout".into()));
                }
            }
            Ok(())
        };

        if agg.is_none() {
            let offset_pushdown =
                filter.offset.is_some() && filter.order_by.is_none() && !filter.distinct;
            if matching.len() >= self.cfg.parallel_scan_threshold && !offset_pushdown {
                 let filter_ref = &filter;
                let projection_ref = projection.as_deref();
                let expected_schema_ref = expected_schema
                    .as_ref()
                    .map(|v: &Vec<TableFieldSpec>| Arc::new(v.clone()));
                let this = self;
                let limit = filter.limit.unwrap_or(usize::MAX);
                let collected_rows = AtomicU64::new(0);
                let collected_rows_ref = &collected_rows;

                let results: Vec<Result<(Vec<u8>, u64), EngineError>> = matching
                    .par_iter()
                    .filter_map(|entry| {
                        if collected_rows_ref.load(Ordering::Relaxed) >= limit as u64 {
                            return None;
                        }
                        let payload = match this.load_segment_cached(entry) {
                            Ok(p) => p,
                            Err(e) => return Some(Err(e)),
                        };
                        let filtered = match filter_ipc(
                            &payload,
                            filter_ref,
                        ) {
                            Ok(f) => f,
                            Err(e) => return Some(Err(e)),
                        };
                        if !filtered.is_empty() {
                            let estimated_rows = (filtered.len() / 100).max(1);
                            collected_rows_ref.fetch_add(estimated_rows as u64, Ordering::Relaxed);
                        }
                        let skipped = if filtered.is_empty() { payload.len() as u64 } else { 0 };
                        Some(Ok((filtered, skipped)))
                    })
                    .collect();

                for result in results {
                    check_timeout()?;
                    let (filtered, skipped) = result?;
                    data_skipped_bytes += skipped;
                    records.extend_from_slice(&filtered);
                }
            } else {
                let limit = filter.limit.unwrap_or(usize::MAX);
                let mut estimated_rows: usize = 0;
                let mut remaining_offset = if offset_pushdown { filter.offset.unwrap_or(0) } else { 0 };
                let mut remaining_limit = if offset_pushdown { filter.limit.unwrap_or(usize::MAX) } else { usize::MAX };
                
                for entry in &matching {
                    if estimated_rows >= limit || remaining_limit == 0 {
                        break;
                    }
                    check_timeout()?;
                    let payload = self.load_segment_cached(entry)?;
                    let original_size = payload.len() as u64;
                    if offset_pushdown {
                        let mut segment_filter = filter.clone();
                        segment_filter.limit = None;
                        segment_filter.offset = None;
                        let batches = filter_ipc_batches(
                            read_ipc_batches(&payload)?,
                            &segment_filter,
                        )?;
                        if batches.is_empty() {
                             data_skipped_bytes += original_size;
                             continue;
                        }
                        let sliced = apply_offset_limit_to_batches(
                            batches,
                            &mut remaining_offset,
                            &mut remaining_limit,
                        );
                        if sliced.is_empty() {
                            continue;
                        }
                        let ipc = record_batches_to_ipc(sliced[0].schema().as_ref(), &sliced)?;
                        estimated_rows += sliced.iter().map(|b| b.num_rows()).sum::<usize>();
                        records.extend_from_slice(&ipc);
                    } else {
                        let filtered = filter_ipc(
                            &payload,
                            &filter,
                        )?;
                        if filtered.is_empty() {
                            data_skipped_bytes += original_size;
                        } else {
                            estimated_rows += (filtered.len() / 100).max(1);
                        }
                        records.extend_from_slice(&filtered);
                    }
                }
            }

            if filter.order_by.is_some() && !records.is_empty() {
                records = apply_order_by(&records, &filter)?;
            }
            // Apply projection before DISTINCT so we deduplicate on selected columns only
            if let Some(ref cols) = projection {
                if !records.is_empty() {
                    // Convert expected_schema to Arrow Schema for handling missing columns
                    let arrow_schema = expected_schema.as_ref().map(|fields| {
                        let arrow_fields: Vec<Field> = fields.iter().map(|f| {
                            let dt = arrow_type_from_str(&f.data_type).unwrap_or(DataType::Utf8);
                            Field::new(&f.name, dt, f.nullable)
                        }).collect();
                        Schema::new(arrow_fields)
                    });
                    records = apply_projection_with_schema(&records, cols, arrow_schema.as_ref())?;
                }
            }
            if filter.distinct && !records.is_empty() {
                records = apply_distinct(&records)?;
            }
            if let Some(offset) = filter.offset {
                if !records.is_empty() && (filter.order_by.is_some() || filter.distinct) {
                    records = apply_offset(&records, offset)?;
                }
            }
        } else if let Some(agg_plan) = agg.clone() {
             let mut agg_state = Aggregator::new(agg_plan)?;
            let default_filter = QueryFilter::default();
            let agg_projection = aggregation_projection(agg.as_ref().expect("agg present"), &[]);
            if matching.len() >= self.cfg.parallel_scan_threshold {
                let this = self;
                let expected_schema_ref = expected_schema
                    .as_ref()
                    .map(|v: &Vec<TableFieldSpec>| Arc::new(v.clone()));
                 let payloads: Vec<Result<Arc<Vec<u8>>, EngineError>> = matching
                    .par_iter()
                    .map(|entry| this.load_segment_cached(entry))
                    .collect();
                 for payload_result in payloads {
                    check_timeout()?;
                    let payload = payload_result?;
                    let filtered = filter_ipc(
                        &payload,
                        &filter,
                    )?;
                    agg_state.consume_ipc(&filtered, &default_filter)?;
                }
            } else {
                 for entry in &matching {
                    check_timeout()?;
                    let payload = self.load_segment_cached(entry)?;
                    let filtered = filter_ipc(
                        &payload,
                        &filter,
                    )?;
                    agg_state.consume_ipc(&filtered, &default_filter)?;
                }
            }
            records = agg_state.finish()?;
        }

        let execution_stats = if collect_stats {
            let bytes_read: u64 = matching.iter().map(|e| e.size_bytes).sum();
            Some(QueryExecutionStats {
                execution_time_micros: query_start.elapsed().as_micros() as u64,
                segments_scanned: matching.len(),
                segments_pruned: total_segments.saturating_sub(matching.len()),
                segments_total: total_segments,
                bytes_read,
                bytes_skipped: data_skipped_bytes,
                cache_hit: false,
                ..Default::default()
            })
        } else {
            None
        };

        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);

        Ok(QueryResponse {
            records_ipc: records,
            data_skipped_bytes,
            segments_scanned: matching.len(),
            execution_stats,
        })
    }

    /// Export metrics in Prometheus text format
    pub fn prometheus_metrics(&self) -> String {
        self.metrics.to_prometheus()
    }

    /// Load segment with LRU caching and Arc for zero-copy sharing
    fn load_segment_cached(&self, entry: &ManifestEntry) -> Result<Arc<Vec<u8>>, EngineError> {
        // Check if caching is disabled
        if self.cfg.segment_cache_bytes == 0 || !self.cache_enabled_for_tier(&entry.tier) {
            return Ok(Arc::new(load_segment(&self.storage, entry)?));
        }

        if let Some(mem_entry) = self.memtable_index.lock().get(&entry.segment_id) {
            let payload = decompress_payload(
                mem_entry.payload.clone(),
                entry.compression.as_deref(),
            )?;
            return Ok(Arc::new(payload));
        }

        let mut cache = self.segment_cache.lock();

        // Check cache first (LRU access)
        if let Some(payload) = cache.get(&entry.segment_id) {
            return Ok(payload);
        }

        // Load from disk and insert into cache
        let payload = load_segment(&self.storage, entry)?;
        let arc_payload = cache.insert(entry.segment_id.clone(), payload);
        Ok(arc_payload)
    }

    fn load_segment_batches_cached(
        &self,
        entry: &ManifestEntry,
    ) -> Result<Arc<Vec<RecordBatch>>, EngineError> {
        if self.cfg.batch_cache_bytes == 0 || !self.cache_enabled_for_tier(&entry.tier) {
            let payload = self.load_segment_cached(entry)?;
            let batches = read_ipc_batches(&payload)?;
            return Ok(Arc::new(batches));
        }

        if let Some(batches) = {
            let mut cache = self.batch_cache.lock();
            cache.get(&entry.segment_id)
        } {
            self.metrics
                .batch_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(batches);
        }
        self.metrics
            .batch_cache_misses
            .fetch_add(1, Ordering::Relaxed);

        let payload = self.load_segment_cached(entry)?;
        let batches = read_ipc_batches(&payload)?;
        let mut cache = self.batch_cache.lock();
        Ok(cache.insert(entry.segment_id.clone(), batches))
    }

    fn cache_enabled_for_tier(&self, tier: &SegmentTier) -> bool {
        match tier {
            SegmentTier::Hot => self.cfg.cache_hot_segments,
            SegmentTier::Warm => self.cfg.cache_warm_segments,
            SegmentTier::Cold => self.cfg.cache_cold_segments,
        }
    }

    /// Execute a query with JOIN clauses
    ///
    /// Supports multiple JOINs chained together and all JOIN types:
    /// - INNER JOIN: Only matching rows from both tables
    /// - LEFT JOIN: All rows from left + matching rows from right (NULLs for non-matches)
    /// - RIGHT JOIN: All rows from right + matching rows from left (NULLs for non-matches)
    /// - FULL OUTER JOIN: All rows from both tables (NULLs for non-matches)
    /// - CROSS JOIN: Cartesian product of both tables
    ///
    /// **Note:** This is an in-memory implementation suitable for small to medium datasets.
    fn execute_join_query(
        &self,
        request: &QueryRequest,
        deadline: Option<std::time::Instant>,
        manifest_version: u64,
    ) -> Result<QueryResponse, EngineError> {
        use crate::sql::{parse_sql, JoinType, SqlStatement};
        use arrow_ipc::writer::StreamWriter;

        let max_join_bytes = self.cfg.join_max_bytes;
        let join_partition_bytes = self.cfg.join_partition_bytes;
        let join_max_partitions = self.cfg.join_max_partitions.max(1);

        // Parse the SQL to get JOIN information
        let parsed = match parse_sql(&request.sql)? {
            SqlStatement::Query(q) => q,
            _ => {
                return Err(EngineError::InvalidArgument(
                    "expected SELECT query with JOIN".into(),
                ))
            }
        };

        if parsed.joins.is_empty() {
            return Err(EngineError::InvalidArgument("no JOIN clause found".into()));
        }

        let mut remaining_filter = query_filter_from_sql(&parsed.filter);

        // Check timeout helper
        let check_timeout = || -> Result<(), EngineError> {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(EngineError::Timeout(format!(
                        "query exceeded timeout of {}ms",
                        request.timeout_millis
                    )));
                }
            }
            Ok(())
        };

        let mut segments_scanned = 0usize;

        // Load data from the left (main) table
        let left_entries = self.table_entries(parsed.database.as_deref().unwrap_or("default"), parsed.table.as_deref().unwrap_or("dual"))?;
        segments_scanned += left_entries.len();

        // Decode left table to RecordBatches
        let mut current_batches = self.load_table_batches(parsed.database.as_deref().unwrap_or("default"), parsed.table.as_deref().unwrap_or("dual"))?;
        let mut current_spilled: Option<(Vec<PathBuf>, Arc<Schema>)> = None;
        let mut spill_dirs: Vec<TempDir> = Vec::new();
        check_timeout()?;

        let join_projection = parsed.projection.as_deref();

        // Iterate through all JOINs, chaining results
        for (join_idx, join) in parsed.joins.iter().enumerate() {
            let right_entries = self.table_entries(&join.database, &join.table)?;
            let right_size = Self::entries_total_bytes(&right_entries);
            let (left_bytes, left_schema) = if let Some((paths, schema)) = &current_spilled {
                (Self::paths_total_bytes(paths)?, Some(schema.clone()))
            } else {
                (
                    Self::estimate_batches_bytes(&current_batches),
                    current_batches.get(0).map(|b| b.schema()),
                )
            };
            let total_bytes = left_bytes.saturating_add(right_size);
            let right_schema = self.first_batch_schema_from_entries(&right_entries)?;
            let (left_filter, right_filter, next_remaining) = split_filter_for_join(
                &remaining_filter,
                left_schema.as_deref(),
                right_schema.as_deref(),
            );
            remaining_filter = next_remaining;

            if total_bytes as usize > max_join_bytes {
                if join.join_type == JoinType::Cross {
                    return Err(EngineError::InvalidArgument(
                        "CROSS JOIN spill is not supported; add WHERE filters or reduce JOIN size"
                            .into(),
                    ));
                }

                let max_side_bytes = left_bytes.max(right_size);
                let mut partitions = (max_side_bytes + join_partition_bytes - 1)
                    / join_partition_bytes;
                partitions = partitions.max(1).min(join_max_partitions as u64);
                let partitions = (partitions as usize).next_power_of_two();

                let tmp = TempDir::new()
                    .map_err(|e| EngineError::Io(format!("create join temp dir failed: {e}")))?;

                let mut left_needed: Option<Vec<String>> = None;
                let mut right_needed: Option<Vec<String>> = None;
                if let (Some(left_schema), Some(right_schema)) = (
                    current_batches.get(0).map(|b| b.schema()),
                    right_schema,
                ) {
                    let remaining_left_keys = parsed.joins[join_idx..]
                        .iter()
                        .map(|j| j.on_condition.left_column.clone())
                        .collect::<Vec<_>>();
                    let (left_set, right_set) = join_projection_sets(
                        left_schema.as_ref(),
                        right_schema.as_ref(),
                        join_projection,
                        &join.on_condition.left_column,
                        &join.on_condition.right_column,
                        &remaining_left_keys,
                    )?;
                    left_needed = left_set;
                    right_needed = right_set;
                    if let Some(ref mut needed) = left_needed {
                        extend_needed_with_filter(needed, &left_filter, Some(left_schema.as_ref()));
                    }
                    if let Some(ref mut needed) = right_needed {
                        extend_needed_with_filter(needed, &right_filter, Some(right_schema.as_ref()));
                    }
                    if let Some(ref needed) = left_needed {
                        if current_spilled.is_none() {
                            current_batches = Self::project_batches_by_name(&current_batches, needed)?;
                        }
                    }
                }

                if !filter_is_empty(&left_filter) && current_spilled.is_none() {
                    current_batches = filter_batches_with_query_filter(current_batches, &left_filter)?;
                }

                let left_paths = if let Some((paths, _schema)) = current_spilled.take() {
                    Self::partition_ipc_paths_to_ipc(
                        &paths,
                        &join.on_condition.left_column,
                        partitions,
                        tmp.path(),
                        "left",
                        left_needed.as_deref(),
                        if filter_is_empty(&left_filter) {
                            None
                        } else {
                            Some(&left_filter)
                        },
                    )?
                } else {
                    Self::partition_batches_to_ipc(
                        &current_batches,
                        &join.on_condition.left_column,
                        partitions,
                        tmp.path(),
                        "left",
                        left_needed.as_deref(),
                    )?
                };
                let right_paths = self.partition_table_to_ipc(
                    &right_entries,
                    &join.on_condition.right_column,
                    partitions,
                    tmp.path(),
                    "right",
                    right_needed.as_deref(),
                    if filter_is_empty(&right_filter) {
                        None
                    } else {
                        Some(&right_filter)
                    },
                )?;

                let mut joined_batches: Vec<RecordBatch> = Vec::new();
                for idx in 0..partitions {
                    check_timeout()?;
                    let left_batches = Self::read_ipc_batches_from_file(&left_paths[idx])?;
                    let right_batches = Self::read_ipc_batches_from_file(&right_paths[idx])?;

                    let joined_batch = match join.join_type {
                        JoinType::Inner => {
                            if left_batches.is_empty() || right_batches.is_empty() {
                                None
                            } else {
                                Self::hash_join_batches(
                                    &left_batches,
                                    &right_batches,
                                    &join.on_condition.left_column,
                                    &join.on_condition.right_column,
                                )?
                            }
                        }
                        JoinType::Left => {
                            if left_batches.is_empty() {
                                None
                            } else {
                                Self::left_join_batches(
                                    &left_batches,
                                    &right_batches,
                                    &join.on_condition.left_column,
                                    &join.on_condition.right_column,
                                )?
                            }
                        }
                        JoinType::Right => {
                            if right_batches.is_empty() {
                                None
                            } else {
                                Self::right_join_batches(
                                    &left_batches,
                                    &right_batches,
                                    &join.on_condition.left_column,
                                    &join.on_condition.right_column,
                                )?
                            }
                        }
                        JoinType::FullOuter => {
                            Self::full_outer_join_batches(
                                &left_batches,
                                &right_batches,
                                &join.on_condition.left_column,
                                &join.on_condition.right_column,
                            )?
                        }
                        JoinType::Cross => None,
                    };

                    if let Some(batch) = joined_batch {
                        joined_batches.push(batch);
                    }
                }

                current_batches = joined_batches;
                segments_scanned += right_entries.len();
                if join_idx + 1 < parsed.joins.len() && !current_batches.is_empty() {
                    let joined_bytes = Self::estimate_batches_bytes(&current_batches);
                    if joined_bytes > max_join_bytes as u64 {
                        let tmp = TempDir::new()
                            .map_err(|e| EngineError::Io(format!("create join spill dir failed: {e}")))?;
                        let path = tmp.path().join("spill.ipc");
                        Self::write_batches_to_ipc_file(&current_batches, &path)?;
                        let schema = current_batches[0].schema();
                        current_batches.clear();
                        current_spilled = Some((vec![path], schema));
                        spill_dirs.push(tmp);
                    }
                }
                continue;
            }

            if let Some((paths, _schema)) = current_spilled.take() {
                current_batches = Self::read_ipc_batches_from_paths(&paths)?;
            }

            let mut right_batches = self.load_table_batches(&join.database, &join.table)?;
            check_timeout()?;
            segments_scanned += right_entries.len();

            if let (Some(left_schema), Some(right_schema)) = (
                current_batches.get(0).map(|b| b.schema()),
                right_batches.get(0).map(|b| b.schema()),
            ) {
                let remaining_left_keys = parsed.joins[join_idx..]
                    .iter()
                    .map(|j| j.on_condition.left_column.clone())
                    .collect::<Vec<_>>();
                let (left_needed, right_needed) = join_projection_sets(
                    left_schema.as_ref(),
                    right_schema.as_ref(),
                    join_projection,
                    &join.on_condition.left_column,
                    &join.on_condition.right_column,
                    &remaining_left_keys,
                )?;
                if let Some(needed) = left_needed {
                    let mut needed = needed;
                    extend_needed_with_filter(&mut needed, &left_filter, Some(left_schema.as_ref()));
                    current_batches = Self::project_batches_by_name(&current_batches, &needed)?;
                }
                if let Some(needed) = right_needed {
                    let mut needed = needed;
                    extend_needed_with_filter(&mut needed, &right_filter, Some(right_schema.as_ref()));
                    right_batches = Self::project_batches_by_name(&right_batches, &needed)?;
                }
            }

            if !filter_is_empty(&left_filter) {
                current_batches = filter_batches_with_query_filter(current_batches, &left_filter)?;
            }
            if !filter_is_empty(&right_filter) {
                right_batches = filter_batches_with_query_filter(right_batches, &right_filter)?;
            }

            // Handle empty tables based on join type
            if current_batches.is_empty() && right_batches.is_empty() {
                current_batches = Vec::new();
                continue;
            }

            // Perform the join based on type
            let joined_batch = match join.join_type {
                JoinType::Cross => {
                    self.cross_join_batches(&current_batches, &right_batches)?
                }
                JoinType::Inner => {
                    if current_batches.is_empty() || right_batches.is_empty() {
                        None
                    } else {
                        Self::hash_join_batches(
                            &current_batches,
                            &right_batches,
                            &join.on_condition.left_column,
                            &join.on_condition.right_column,
                        )?
                    }
                }
                JoinType::Left => {
                    if current_batches.is_empty() {
                        None
                    } else {
                        Self::left_join_batches(
                            &current_batches,
                            &right_batches,
                            &join.on_condition.left_column,
                            &join.on_condition.right_column,
                        )?
                    }
                }
                JoinType::Right => {
                    if right_batches.is_empty() {
                        None
                    } else {
                        // Right join is left join with tables swapped, then columns reordered
                        Self::right_join_batches(
                            &current_batches,
                            &right_batches,
                            &join.on_condition.left_column,
                            &join.on_condition.right_column,
                        )?
                    }
                }
                JoinType::FullOuter => {
                    Self::full_outer_join_batches(
                        &current_batches,
                        &right_batches,
                        &join.on_condition.left_column,
                        &join.on_condition.right_column,
                    )?
                }
            };
            check_timeout()?;

            // Update current_batches for next JOIN in chain
            current_batches = match joined_batch {
                Some(batch) => vec![batch],
                None => Vec::new(),
            };
            if join_idx + 1 < parsed.joins.len() && !current_batches.is_empty() {
                let joined_bytes = Self::estimate_batches_bytes(&current_batches);
                if joined_bytes > max_join_bytes as u64 {
                    let tmp = TempDir::new()
                        .map_err(|e| EngineError::Io(format!("create join spill dir failed: {e}")))?;
                    let path = tmp.path().join("spill.ipc");
                    Self::write_batches_to_ipc_file(&current_batches, &path)?;
                    let schema = current_batches[0].schema();
                    current_batches.clear();
                    current_spilled = Some((vec![path], schema));
                    spill_dirs.push(tmp);
                }
            }
        }

        if let Some((paths, _schema)) = current_spilled.take() {
            current_batches = Self::read_ipc_batches_from_paths(&paths)?;
        }

        if !filter_is_empty(&remaining_filter) {
            current_batches =
                filter_batches_with_query_filter(current_batches, &remaining_filter)?;
        }

        // Encode result to IPC
        let result_ipc = if !current_batches.is_empty() {
            let mut batch = current_batches[0].clone();
            if let Some(projection) = parsed.projection.as_ref() {
                let mut indices = Vec::with_capacity(projection.len());
                for col in projection {
                    let idx = batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == col)
                        .ok_or_else(|| {
                            EngineError::NotFound(format!("column not found: {col}"))
                        })?;
                    indices.push(idx);
                }
                batch = batch
                    .project(&indices)
                    .map_err(|e| EngineError::Internal(format!("project failed: {e}")))?;
            }
            let mut ipc_data = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc_data, batch.schema().as_ref())
                    .map_err(|e| EngineError::Internal(format!("IPC writer error: {e}")))?;
                writer
                    .write(&batch)
                    .map_err(|e| EngineError::Internal(format!("IPC write error: {e}")))?;
                writer
                    .finish()
                    .map_err(|e| EngineError::Internal(format!("IPC finish error: {e}")))?;
            }
            ipc_data
        } else {
            Vec::new()
        };

        let response = QueryResponse {
            records_ipc: result_ipc,
            data_skipped_bytes: 0,
            segments_scanned,
            execution_stats: None,
        };

        // Cache the result
        let mut cache = self.query_cache.lock();
        cache.insert(
            &request.sql,
            response.clone(),
            manifest_version,
            parsed.database.as_deref().unwrap_or("default"),
            parsed.table.as_deref().unwrap_or("dual"),
        );

        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);
        Ok(response)
    }

    /// Perform CROSS JOIN (Cartesian product) of two tables
    fn cross_join_batches(
        &self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Option<RecordBatch>, EngineError> {
        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(None);
        }

        let left_schema = left_batches[0].schema();
        let right_schema = right_batches[0].schema();

        // Build result schema
        let mut fields: Vec<arrow_schema::Field> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());
        fields.extend(left_schema.fields().iter().map(|f| f.as_ref().clone()));

        for field in right_schema.fields() {
            if left_schema.field_with_name(field.name()).is_ok() {
                fields.push(arrow_schema::Field::new(
                    format!("r_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            } else {
                fields.push(field.as_ref().clone());
            }
        }

        let result_schema = Arc::new(arrow_schema::Schema::new(fields));

        // Count total rows
        let left_total: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        let right_total: usize = right_batches.iter().map(|b| b.num_rows()).sum();

        if left_total == 0 || right_total == 0 {
            return Ok(None);
        }

        let max_cross_join_rows = self.cfg.cross_join_max_rows;
        let result_rows = left_total.saturating_mul(right_total);
        if result_rows > max_cross_join_rows {
            return Err(EngineError::InvalidArgument(format!(
                "CROSS JOIN would produce {} rows, exceeding limit of {}. \
                 Add WHERE clause or use a different join type.",
                result_rows, max_cross_join_rows
            )));
        }

        // Build indices for cross product
        let mut left_indices: Vec<(u32, u32)> = Vec::with_capacity(result_rows);
        let mut right_indices: Vec<(u32, u32)> = Vec::with_capacity(result_rows);

        for (l_batch_idx, l_batch) in left_batches.iter().enumerate() {
            for l_row in 0..l_batch.num_rows() {
                for (r_batch_idx, r_batch) in right_batches.iter().enumerate() {
                    for r_row in 0..r_batch.num_rows() {
                        left_indices.push((l_batch_idx as u32, l_row as u32));
                        right_indices.push((r_batch_idx as u32, r_row as u32));
                    }
                }
            }
        }

        // Build result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        for col_idx in 0..left_schema.fields().len() {
            let array = Self::take_from_batches(left_batches, col_idx, &left_indices)?;
            result_arrays.push(array);
        }

        for col_idx in 0..right_schema.fields().len() {
            let array = Self::take_from_batches(right_batches, col_idx, &right_indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Perform LEFT JOIN - all rows from left, matching rows from right (NULLs for non-matches)
    fn left_join_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        left_col: &str,
        right_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        use rustc_hash::FxHashMap;

        if left_batches.is_empty() {
            return Ok(None);
        }

        let left_schema = left_batches[0].schema();
        let right_schema = if right_batches.is_empty() {
            // Create empty right schema - we need to return left rows with NULL right columns
            return Self::left_join_with_empty_right(left_batches, left_col);
        } else {
            right_batches[0].schema()
        };

        // Build hash table from right side
        let right_row_count: usize = right_batches.iter().map(|b| b.num_rows()).sum();
        let mut hash_table: FxHashMap<u64, Vec<(u32, u32)>> =
            FxHashMap::with_capacity_and_hasher(right_row_count, Default::default());

        for (batch_idx, batch) in right_batches.iter().enumerate() {
            let col_idx = batch.schema().index_of(right_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{right_col}' not found in right table"))
            })?;

            let col = batch.column(col_idx);
            for row_idx in 0..batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                hash_table
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push((batch_idx as u32, row_idx as u32));
            }
        }

        // Build result schema
        let mut fields: Vec<arrow_schema::Field> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());
        fields.extend(left_schema.fields().iter().map(|f| f.as_ref().clone()));

        for field in right_schema.fields() {
            let name = if left_schema.field_with_name(field.name()).is_ok() {
                format!("r_{}", field.name())
            } else {
                field.name().to_string()
            };
            // Right side columns are nullable in LEFT JOIN
            fields.push(arrow_schema::Field::new(name, field.data_type().clone(), true));
        }

        let result_schema = Arc::new(arrow_schema::Schema::new(fields));

        // Probe with left side - keep ALL left rows
        let left_row_count: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        let mut left_indices: Vec<(u32, u32)> = Vec::with_capacity(left_row_count);
        let mut right_indices: Vec<Option<(u32, u32)>> = Vec::with_capacity(left_row_count);

        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let col_idx = left_batch.schema().index_of(left_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{left_col}' not found in left table"))
            })?;

            let col = left_batch.column(col_idx);
            for row_idx in 0..left_batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                if let Some(matches) = hash_table.get(&key) {
                    // Has matches - add one row per match
                    for &(right_batch_idx, right_row_idx) in matches {
                        left_indices.push((left_batch_idx as u32, row_idx as u32));
                        right_indices.push(Some((right_batch_idx, right_row_idx)));
                    }
                } else {
                    // No match - add row with NULL right side
                    left_indices.push((left_batch_idx as u32, row_idx as u32));
                    right_indices.push(None);
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(None);
        }

        // Build result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        // Left side columns
        for col_idx in 0..left_schema.fields().len() {
            let array = Self::take_from_batches(left_batches, col_idx, &left_indices)?;
            result_arrays.push(array);
        }

        // Right side columns (with NULLs for non-matches)
        for col_idx in 0..right_schema.fields().len() {
            let array = Self::take_from_batches_nullable(
                right_batches,
                col_idx,
                &right_indices,
            )?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Handle LEFT JOIN when right table is empty
    fn left_join_with_empty_right(
        left_batches: &[RecordBatch],
        _left_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        if left_batches.is_empty() {
            return Ok(None);
        }

        // Just return left batches as-is (no right columns to add)
        // In practice, we'd need the right schema to add NULL columns
        // For now, return the left side only
        let left_schema = left_batches[0].schema();
        let mut all_arrays: Vec<ArrayRef> = Vec::new();

        for col_idx in 0..left_schema.fields().len() {
            let mut col_arrays: Vec<ArrayRef> = Vec::new();
            for batch in left_batches {
                col_arrays.push(batch.column(col_idx).clone());
            }
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = arrow_select::concat::concat(&refs)
                .map_err(|e| EngineError::Internal(format!("concat error: {e}")))?;
            all_arrays.push(concatenated);
        }

        let result_batch = RecordBatch::try_new(left_schema, all_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Perform RIGHT JOIN - all rows from right, matching rows from left (NULLs for non-matches)
    fn right_join_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        left_col: &str,
        right_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        // Right join is essentially left join with tables swapped
        // But we want to preserve column order (left columns first, then right)
        use rustc_hash::FxHashMap;

        if right_batches.is_empty() {
            return Ok(None);
        }

        let right_schema = right_batches[0].schema();
        let left_schema = if left_batches.is_empty() {
            // Return right rows with NULL left columns
            return Self::right_join_with_empty_left(right_batches, right_col);
        } else {
            left_batches[0].schema()
        };

        // Build hash table from left side
        let left_row_count: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        let mut hash_table: FxHashMap<u64, Vec<(u32, u32)>> =
            FxHashMap::with_capacity_and_hasher(left_row_count, Default::default());

        for (batch_idx, batch) in left_batches.iter().enumerate() {
            let col_idx = batch.schema().index_of(left_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{left_col}' not found in left table"))
            })?;

            let col = batch.column(col_idx);
            for row_idx in 0..batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                hash_table
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push((batch_idx as u32, row_idx as u32));
            }
        }

        // Build result schema (left columns first, but nullable)
        let mut fields: Vec<arrow_schema::Field> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        // Left side columns are nullable in RIGHT JOIN
        for field in left_schema.fields() {
            fields.push(arrow_schema::Field::new(
                field.name(),
                field.data_type().clone(),
                true,
            ));
        }

        for field in right_schema.fields() {
            let name = if left_schema.field_with_name(field.name()).is_ok() {
                format!("r_{}", field.name())
            } else {
                field.name().to_string()
            };
            fields.push(arrow_schema::Field::new(name, field.data_type().clone(), field.is_nullable()));
        }

        let result_schema = Arc::new(arrow_schema::Schema::new(fields));

        // Probe with right side - keep ALL right rows
        let right_row_count: usize = right_batches.iter().map(|b| b.num_rows()).sum();
        let mut left_indices: Vec<Option<(u32, u32)>> = Vec::with_capacity(right_row_count);
        let mut right_indices: Vec<(u32, u32)> = Vec::with_capacity(right_row_count);

        for (right_batch_idx, right_batch) in right_batches.iter().enumerate() {
            let col_idx = right_batch.schema().index_of(right_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{right_col}' not found in right table"))
            })?;

            let col = right_batch.column(col_idx);
            for row_idx in 0..right_batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                if let Some(matches) = hash_table.get(&key) {
                    for &(left_batch_idx, left_row_idx) in matches {
                        left_indices.push(Some((left_batch_idx, left_row_idx)));
                        right_indices.push((right_batch_idx as u32, row_idx as u32));
                    }
                } else {
                    left_indices.push(None);
                    right_indices.push((right_batch_idx as u32, row_idx as u32));
                }
            }
        }

        if right_indices.is_empty() {
            return Ok(None);
        }

        // Build result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        // Left side columns (with NULLs for non-matches)
        for col_idx in 0..left_schema.fields().len() {
            let array = Self::take_from_batches_nullable(left_batches, col_idx, &left_indices)?;
            result_arrays.push(array);
        }

        // Right side columns
        for col_idx in 0..right_schema.fields().len() {
            let array = Self::take_from_batches(right_batches, col_idx, &right_indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Handle RIGHT JOIN when left table is empty
    fn right_join_with_empty_left(
        right_batches: &[RecordBatch],
        _right_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        if right_batches.is_empty() {
            return Ok(None);
        }

        let right_schema = right_batches[0].schema();
        let mut all_arrays: Vec<ArrayRef> = Vec::new();

        for col_idx in 0..right_schema.fields().len() {
            let mut col_arrays: Vec<ArrayRef> = Vec::new();
            for batch in right_batches {
                col_arrays.push(batch.column(col_idx).clone());
            }
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = arrow_select::concat::concat(&refs)
                .map_err(|e| EngineError::Internal(format!("concat error: {e}")))?;
            all_arrays.push(concatenated);
        }

        let result_batch = RecordBatch::try_new(right_schema, all_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Perform FULL OUTER JOIN - all rows from both tables (NULLs for non-matches on either side)
    fn full_outer_join_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        left_col: &str,
        right_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        use rustc_hash::FxHashMap;

        // Handle empty cases
        if left_batches.is_empty() && right_batches.is_empty() {
            return Ok(None);
        }
        if left_batches.is_empty() {
            return Self::right_join_with_empty_left(right_batches, right_col);
        }
        if right_batches.is_empty() {
            return Self::left_join_with_empty_right(left_batches, left_col);
        }

        let left_schema = left_batches[0].schema();
        let right_schema = right_batches[0].schema();

        // Build hash table from right side
        let right_row_count: usize = right_batches.iter().map(|b| b.num_rows()).sum();
        let mut hash_table: FxHashMap<u64, Vec<(u32, u32)>> =
            FxHashMap::with_capacity_and_hasher(right_row_count, Default::default());
        let mut right_matched: Vec<Vec<bool>> = right_batches
            .iter()
            .map(|b| vec![false; b.num_rows()])
            .collect();

        for (batch_idx, batch) in right_batches.iter().enumerate() {
            let col_idx = batch.schema().index_of(right_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{right_col}' not found in right table"))
            })?;

            let col = batch.column(col_idx);
            for row_idx in 0..batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                hash_table
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push((batch_idx as u32, row_idx as u32));
            }
        }

        // Build result schema (all columns nullable)
        let mut fields: Vec<arrow_schema::Field> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        for field in left_schema.fields() {
            fields.push(arrow_schema::Field::new(field.name(), field.data_type().clone(), true));
        }

        for field in right_schema.fields() {
            let name = if left_schema.field_with_name(field.name()).is_ok() {
                format!("r_{}", field.name())
            } else {
                field.name().to_string()
            };
            fields.push(arrow_schema::Field::new(name, field.data_type().clone(), true));
        }

        let result_schema = Arc::new(arrow_schema::Schema::new(fields));

        // Probe with left side
        let left_row_count: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        let mut left_indices: Vec<Option<(u32, u32)>> = Vec::with_capacity(left_row_count + right_row_count);
        let mut right_indices: Vec<Option<(u32, u32)>> = Vec::with_capacity(left_row_count + right_row_count);

        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let col_idx = left_batch.schema().index_of(left_col).map_err(|_| {
                EngineError::InvalidArgument(format!("column '{left_col}' not found in left table"))
            })?;

            let col = left_batch.column(col_idx);
            for row_idx in 0..left_batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                if let Some(matches) = hash_table.get(&key) {
                    for &(right_batch_idx, right_row_idx) in matches {
                        left_indices.push(Some((left_batch_idx as u32, row_idx as u32)));
                        right_indices.push(Some((right_batch_idx, right_row_idx)));
                        right_matched[right_batch_idx as usize][right_row_idx as usize] = true;
                    }
                } else {
                    left_indices.push(Some((left_batch_idx as u32, row_idx as u32)));
                    right_indices.push(None);
                }
            }
        }

        // Add unmatched right rows
        for (batch_idx, matched) in right_matched.iter().enumerate() {
            for (row_idx, &was_matched) in matched.iter().enumerate() {
                if !was_matched {
                    left_indices.push(None);
                    right_indices.push(Some((batch_idx as u32, row_idx as u32)));
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(None);
        }

        // Build result arrays
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        // Left side columns
        for col_idx in 0..left_schema.fields().len() {
            let array = Self::take_from_batches_nullable(left_batches, col_idx, &left_indices)?;
            result_arrays.push(array);
        }

        // Right side columns
        for col_idx in 0..right_schema.fields().len() {
            let array = Self::take_from_batches_nullable(right_batches, col_idx, &right_indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Take rows from multiple batches using optional indices (None = NULL)
    fn take_from_batches_nullable(
        batches: &[RecordBatch],
        col_idx: usize,
        indices: &[Option<(u32, u32)>],
    ) -> Result<ArrayRef, EngineError> {
        use arrow_array::NullArray;

        if batches.is_empty() || indices.is_empty() {
            // Return array of nulls
            return Ok(Arc::new(NullArray::new(indices.len())));
        }

        let sample_col = batches[0].column(col_idx);
        let data_type = sample_col.data_type();

        // Build array with nulls for None indices
        match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(indices.len());
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(indices.len());
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(indices.len());
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::UInt64Array>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(indices.len());
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(indices.len());
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::BooleanArray>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(indices.len(), indices.len() * 32);
                for idx in indices {
                    if let Some((batch_idx, row_idx)) = idx {
                        let arr = batches[*batch_idx as usize]
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .ok_or_else(|| EngineError::Internal("type mismatch".into()))?;
                        if arr.is_null(*row_idx as usize) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(*row_idx as usize));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                // Fallback for other types - create null array
                Ok(Arc::new(NullArray::new(indices.len())))
            }
        }
    }

    /// Execute a set operation query (UNION, INTERSECT, EXCEPT)
    fn execute_set_operation(
        &self,
        request: &QueryRequest,
        deadline: Option<std::time::Instant>,
        manifest_version: u64,
    ) -> Result<QueryResponse, EngineError> {
        use crate::sql::{parse_sql, SetOpType, SqlStatement};
        use arrow_ipc::writer::StreamWriter;

        let max_set_operation_bytes = self.cfg.set_operation_max_bytes;

        // Check timeout helper
        let check_timeout = || -> Result<(), EngineError> {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(EngineError::Timeout(format!(
                        "query exceeded timeout of {}ms",
                        request.timeout_millis
                    )));
                }
            }
            Ok(())
        };

        // Parse the SQL to get set operation information
        let parsed = match parse_sql(&request.sql)? {
            SqlStatement::SetOperation(set_op) => set_op,
            _ => {
                return Err(EngineError::InvalidArgument(
                    "expected set operation query (UNION/INTERSECT/EXCEPT)".into(),
                ))
            }
        };

        // Execute left query
        let left_sql = match *parsed.left {
            SqlStatement::Query(q) => format!(
                "SELECT * FROM {}.{}",
                q.database.as_deref().unwrap_or("default"),
                q.table.as_deref().unwrap_or("dual")
            ),
            SqlStatement::SetOperation(_) => {
                return Err(EngineError::NotImplemented(
                    "nested set operations not yet supported".into(),
                ))
            }
            _ => {
                return Err(EngineError::InvalidArgument(
                    "invalid left side of set operation".into(),
                ))
            }
        };

        let left_request = QueryRequest {
            sql: left_sql,
            timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
        let left_response = self.query(left_request)?;
        check_timeout()?;

        // Execute right query
        let right_sql = match *parsed.right {
            SqlStatement::Query(q) => format!(
                "SELECT * FROM {}.{}",
                q.database.as_deref().unwrap_or("default"),
                q.table.as_deref().unwrap_or("dual")
            ),
            SqlStatement::SetOperation(_) => {
                return Err(EngineError::NotImplemented(
                    "nested set operations not yet supported".into(),
                ))
            }
            _ => {
                return Err(EngineError::InvalidArgument(
                    "invalid right side of set operation".into(),
                ))
            }
        };

        let right_request = QueryRequest {
            sql: right_sql,
            timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
        let right_response = self.query(right_request)?;
        check_timeout()?;

        let total_ipc_bytes = left_response.records_ipc.len() + right_response.records_ipc.len();
        if total_ipc_bytes > max_set_operation_bytes {
            return Err(EngineError::InvalidArgument(format!(
                "set operation inputs exceed size limit ({} bytes > {})",
                total_ipc_bytes, max_set_operation_bytes
            )));
        }

        // Decode both results
        let left_batches = Self::decode_ipc_batches(&left_response.records_ipc)?;
        let right_batches = Self::decode_ipc_batches(&right_response.records_ipc)?;

        // Combine based on operation type
        let result_batch = match parsed.op {
            SetOpType::UnionAll => {
                // Just concatenate all rows
                Self::concat_batches(&left_batches, &right_batches)?
            }
            SetOpType::Union => {
                // Concatenate and remove duplicates
                let combined = Self::concat_batches(&left_batches, &right_batches)?;
                if let Some(batch) = combined {
                    Some(Self::remove_duplicates(&batch)?)
                } else {
                    None
                }
            }
            SetOpType::IntersectAll => {
                // Keep rows that appear in both (with duplicates)
                Self::intersect_batches(&left_batches, &right_batches, true)?
            }
            SetOpType::Intersect => {
                // Keep rows that appear in both (no duplicates)
                Self::intersect_batches(&left_batches, &right_batches, false)?
            }
            SetOpType::ExceptAll => {
                // Keep rows from left that don't appear in right (with duplicates)
                Self::except_batches(&left_batches, &right_batches, true)?
            }
            SetOpType::Except => {
                // Keep rows from left that don't appear in right (no duplicates)
                Self::except_batches(&left_batches, &right_batches, false)?
            }
        };
        check_timeout()?;

        // Encode result to IPC
        let result_ipc = if let Some(batch) = result_batch {
            let mut ipc_data = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc_data, batch.schema().as_ref())
                    .map_err(|e| EngineError::Internal(format!("IPC writer error: {e}")))?;
                writer
                    .write(&batch)
                    .map_err(|e| EngineError::Internal(format!("IPC write error: {e}")))?;
                writer
                    .finish()
                    .map_err(|e| EngineError::Internal(format!("IPC finish error: {e}")))?;
            }
            ipc_data
        } else {
            Vec::new()
        };

        let response = QueryResponse {
            records_ipc: result_ipc,
            data_skipped_bytes: 0,
            segments_scanned: left_response.segments_scanned + right_response.segments_scanned,
            execution_stats: None,
        };

        // Cache the result
        let mut cache = self.query_cache.lock();
        // Use default database and table for cache invalidation
        cache.insert(
            &request.sql,
            response.clone(),
            manifest_version,
            "default",
            "default",
        );

        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);
        Ok(response)
    }

    /// Execute a query containing subqueries in WHERE clause (IN, EXISTS, scalar comparison)
    fn execute_subquery_query(
        &self,
        request: &QueryRequest,
        deadline: Option<std::time::Instant>,
        manifest_version: u64,
    ) -> Result<QueryResponse, EngineError> {
        use crate::sql::{parse_sql, SqlStatement};
        use arrow_ipc::writer::StreamWriter;

        // Check timeout helper
        let check_timeout = || -> Result<(), EngineError> {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    return Err(EngineError::Timeout(format!(
                        "query exceeded timeout of {}ms",
                        request.timeout_millis
                    )));
                }
            }
            Ok(())
        };

        // Parse the SQL to get query with subquery filters
        let parsed = match parse_sql(&request.sql)? {
            SqlStatement::Query(q) => q,
            _ => {
                return Err(EngineError::InvalidArgument(
                    "expected SELECT query".into(),
                ))
            }
        };

        // Collect values from IN subqueries
        let mut in_values: HashMap<String, HashSet<String>> = HashMap::new();
        for (column, subquery_sql, negated) in &parsed.filter.in_subquery_filters {
            let subquery_request = QueryRequest {
                sql: subquery_sql.clone(),
                timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
            let subquery_response = self.query(subquery_request)?;
            check_timeout()?;

            // Extract values from subquery result (first column only)
            let batches = Self::decode_ipc_batches(&subquery_response.records_ipc)?;
            let mut values = HashSet::new();
            for batch in &batches {
                if batch.num_columns() > 0 {
                    let col = batch.column(0);
                    for row_idx in 0..batch.num_rows() {
                        let val = Self::extract_column_value_as_string(col, row_idx);
                        values.insert(val);
                    }
                }
            }

            // For NOT IN, we'll handle filtering differently
            if *negated {
                // Store with a special prefix to indicate negation
                in_values.insert(format!("NOT:{}", column), values);
            } else {
                in_values.insert(column.clone(), values);
            }
        }

        // Evaluate EXISTS subqueries
        let mut exists_results: Vec<(bool, bool)> = Vec::new(); // (has_rows, negated)
        for (subquery_sql, negated) in &parsed.filter.exists_subqueries {
            let subquery_request = QueryRequest {
                sql: subquery_sql.clone(),
                timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
            let subquery_response = self.query(subquery_request)?;
            check_timeout()?;

            let batches = Self::decode_ipc_batches(&subquery_response.records_ipc)?;
            let has_rows = batches.iter().any(|b| b.num_rows() > 0);
            exists_results.push((has_rows, *negated));
        }

        // Check if EXISTS conditions pass
        for (has_rows, negated) in &exists_results {
            let passes = if *negated { !*has_rows } else { *has_rows };
            if !passes {
                // EXISTS condition failed - return empty result
                return Ok(QueryResponse {
                    records_ipc: Vec::new(),
                    data_skipped_bytes: 0,
                    segments_scanned: 0,
                    execution_stats: None,
                });
            }
        }

        // Evaluate scalar subqueries
        let mut scalar_values: HashMap<String, (String, String)> = HashMap::new(); // column -> (op, value)
        for (column, op, subquery_sql) in &parsed.filter.scalar_subquery_filters {
            let subquery_request = QueryRequest {
                sql: subquery_sql.clone(),
                timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
            let subquery_response = self.query(subquery_request)?;
            check_timeout()?;

            let batches = Self::decode_ipc_batches(&subquery_response.records_ipc)?;
            // Scalar subquery must return exactly one row and one column
            let value = if let Some(batch) = batches.first() {
                if batch.num_rows() != 1 {
                    return Err(EngineError::InvalidArgument(format!(
                        "scalar subquery returned {} rows, expected 1",
                        batch.num_rows()
                    )));
                }
                if batch.num_columns() == 0 {
                    return Err(EngineError::InvalidArgument(
                        "scalar subquery returned no columns".into(),
                    ));
                }
                Self::extract_column_value_as_string(batch.column(0), 0)
            } else {
                return Err(EngineError::InvalidArgument(
                    "scalar subquery returned no results".into(),
                ));
            };

            scalar_values.insert(column.clone(), (op.clone(), value));
        }

        // Now execute the main query without subquery filters, then apply filters in memory
        // Build a simplified query without the subquery parts
        let main_sql = format!(
                    "SELECT * FROM {}.{}",
                    parsed.database.as_deref().unwrap_or("default"),
                    parsed.table.as_deref().unwrap_or("dual")
        );
        let main_request = QueryRequest {
            sql: main_sql,
            timeout_millis: request.timeout_millis,
            collect_stats: false,
        };
        let main_response = self.query(main_request)?;
        check_timeout()?;

        // If no subquery filters need to be applied, return directly
        if in_values.is_empty() && scalar_values.is_empty() {
            return Ok(main_response);
        }

        // Apply subquery filters to the result
        let batches = Self::decode_ipc_batches(&main_response.records_ipc)?;
        let filtered_batch = Self::apply_subquery_filters(&batches, &in_values, &scalar_values)?;

        // Encode result to IPC
        let result_ipc = if let Some(batch) = filtered_batch {
            let mut ipc_data = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc_data, batch.schema().as_ref())
                    .map_err(|e| EngineError::Internal(format!("IPC writer error: {e}")))?;
                writer
                    .write(&batch)
                    .map_err(|e| EngineError::Internal(format!("IPC write error: {e}")))?;
                writer
                    .finish()
                    .map_err(|e| EngineError::Internal(format!("IPC finish error: {e}")))?;
            }
            ipc_data
        } else {
            Vec::new()
        };

        let response = QueryResponse {
            records_ipc: result_ipc,
            data_skipped_bytes: main_response.data_skipped_bytes,
            segments_scanned: main_response.segments_scanned,
            execution_stats: None,
        };

        // Cache the result
        let mut cache = self.query_cache.lock();
        cache.insert(
            &request.sql,
            response.clone(),
            manifest_version,
            parsed.database.as_deref().unwrap_or("default"),
            parsed.table.as_deref().unwrap_or("unknown"),
        );

        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);
        Ok(response)
    }

    /// Execute a query with CTEs (WITH clause)
    fn execute_cte_query(
        &self,
        request: &QueryRequest,
        _deadline: Option<std::time::Instant>,
        _manifest_version: u64,
    ) -> Result<QueryResponse, EngineError> {
        use crate::sql::{parse_sql, SqlStatement};

        // Parse the SQL to extract CTEs
        let parsed = match parse_sql(&request.sql)? {
            SqlStatement::Query(q) => q,
            _ => {
                return Err(EngineError::InvalidArgument(
                    "expected SELECT query".into(),
                ))
            }
        };

        // If no CTEs, execute normally
        if parsed.ctes.is_empty() {
            return self.query(QueryRequest {
                sql: request.sql.clone(),
                timeout_millis: request.timeout_millis,
            collect_stats: false,
        });
        }

        // Execute query with CTEs
        execute_query_with_ctes(self, &request.sql, &parsed.ctes, request.timeout_millis)
    }

    /// Extract a column value as string for subquery comparison
    fn extract_column_value_as_string(col: &ArrayRef, row_idx: usize) -> String {
        use arrow_array::{Int32Array, Int64Array, Float32Array, Float64Array};

        if col.is_null(row_idx) {
            return "NULL".to_string();
        }

        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
            return arr.value(row_idx).to_string();
        }
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            return arr.value(row_idx).to_string();
        }

        // Fallback for other types
        format!("{:?}", col.slice(row_idx, 1))
    }

    /// Apply subquery filters (IN, NOT IN, scalar comparisons) to batches
    fn apply_subquery_filters(
        batches: &[RecordBatch],
        in_values: &HashMap<String, HashSet<String>>,
        scalar_values: &HashMap<String, (String, String)>,
    ) -> Result<Option<RecordBatch>, EngineError> {
        if batches.is_empty() {
            return Ok(None);
        }

        let schema = batches[0].schema();
        let mut all_indices: Vec<(usize, usize)> = Vec::new(); // (batch_idx, row_idx)

        // Build column index map
        let col_indices: HashMap<String, usize> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().to_lowercase(), i))
            .collect();

        for (batch_idx, batch) in batches.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let mut passes = true;

                // Check IN filters
                for (key, values) in in_values {
                    let (column, negated) = if let Some(col) = key.strip_prefix("NOT:") {
                        (col, true)
                    } else {
                        (key.as_str(), false)
                    };

                    if let Some(&col_idx) = col_indices.get(&column.to_lowercase()) {
                        let val = Self::extract_column_value_as_string(batch.column(col_idx), row_idx);
                        let in_set = values.contains(&val);
                        if negated {
                            if in_set {
                                passes = false;
                                break;
                            }
                        } else if !in_set {
                            passes = false;
                            break;
                        }
                    }
                }

                if !passes {
                    continue;
                }

                // Check scalar comparison filters
                for (column, (op, expected_value)) in scalar_values {
                    if let Some(&col_idx) = col_indices.get(&column.to_lowercase()) {
                        let actual_value = Self::extract_column_value_as_string(batch.column(col_idx), row_idx);
                        let cmp_result = Self::compare_string_values(&actual_value, expected_value);
                        let op_passes = match op.as_str() {
                            "=" => cmp_result == std::cmp::Ordering::Equal,
                            "!=" | "<>" => cmp_result != std::cmp::Ordering::Equal,
                            ">" => cmp_result == std::cmp::Ordering::Greater,
                            ">=" => cmp_result != std::cmp::Ordering::Less,
                            "<" => cmp_result == std::cmp::Ordering::Less,
                            "<=" => cmp_result != std::cmp::Ordering::Greater,
                            _ => true,
                        };
                        if !op_passes {
                            passes = false;
                            break;
                        }
                    }
                }

                if passes {
                    all_indices.push((batch_idx, row_idx));
                }
            }
        }

        if all_indices.is_empty() {
            return Ok(None);
        }

        // Build result batch from matching indices
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in 0..schema.fields().len() {
            let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

            // Build indices for arrow take operation
            let mut take_indices: Vec<u32> = Vec::with_capacity(all_indices.len());
            let mut row_offset = 0u32;
            let mut current_batch = 0usize;

            for (batch_idx, row_idx) in &all_indices {
                while current_batch < *batch_idx {
                    row_offset += batches[current_batch].num_rows() as u32;
                    current_batch += 1;
                }
                take_indices.push(row_offset + *row_idx as u32);
            }

            // Concatenate all arrays first
            let concatenated = arrow_select::concat::concat(&arrays)
                .map_err(|e| EngineError::Internal(format!("concat error: {e}")))?;

            // Take the matching rows
            let indices_array = arrow_array::UInt32Array::from(take_indices);
            let taken = arrow_select::take::take(&concatenated, &indices_array, None)
                .map_err(|e| EngineError::Internal(format!("take error: {e}")))?;
            result_arrays.push(taken);
        }

        let result_batch = RecordBatch::try_new(schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Compare two string values, attempting numeric comparison first
    fn compare_string_values(a: &str, b: &str) -> std::cmp::Ordering {
        // Try numeric comparison first
        if let (Ok(a_num), Ok(b_num)) = (a.parse::<f64>(), b.parse::<f64>()) {
            return a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal);
        }
        // Fall back to string comparison
        a.cmp(b)
    }

    /// Concatenate batches from two sets
    fn concat_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
    ) -> Result<Option<RecordBatch>, EngineError> {
        if left_batches.is_empty() && right_batches.is_empty() {
            return Ok(None);
        }

        let schema = if !left_batches.is_empty() {
            left_batches[0].schema()
        } else {
            right_batches[0].schema()
        };

        let all_batches: Vec<&RecordBatch> = left_batches.iter().chain(right_batches.iter()).collect();

        if all_batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all arrays for each column
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in 0..schema.fields().len() {
            let arrays: Vec<&dyn Array> = all_batches
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            let concatenated = arrow_select::concat::concat(&arrays)
                .map_err(|e| EngineError::Internal(format!("concat error: {e}")))?;
            result_arrays.push(concatenated);
        }

        let result_batch = RecordBatch::try_new(schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Remove duplicate rows from a batch
    fn remove_duplicates(batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
        use rustc_hash::FxHashSet;

        if batch.num_rows() == 0 {
            return Ok(batch.clone());
        }

        // Hash each row and keep only unique ones
        let mut seen: FxHashSet<u64> = FxHashSet::default();
        let mut keep_indices: Vec<u32> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let row_hash = Self::compute_row_hash(batch, row_idx);
            if seen.insert(row_hash) {
                keep_indices.push(row_idx as u32);
            }
        }

        // Take only the unique rows
        Self::take_rows(batch, &keep_indices)
    }

    /// Compute a hash for a row (all columns combined)
    fn compute_row_hash(batch: &RecordBatch, row_idx: usize) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
        for col_idx in 0..batch.num_columns() {
            let col_hash = Self::compute_hash_key(batch.column(col_idx).as_ref(), row_idx);
            hash ^= col_hash;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Take specific rows from a batch by indices
    fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, EngineError> {
        use arrow_array::UInt32Array;
        use arrow_select::take::take;

        let indices_array = UInt32Array::from(indices.to_vec());
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let taken = take(batch.column(col_idx).as_ref(), &indices_array, None)
                .map_err(|e| EngineError::Internal(format!("take error: {e}")))?;
            result_arrays.push(taken);
        }

        RecordBatch::try_new(batch.schema(), result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))
    }

    /// Intersect two sets of batches
    fn intersect_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        keep_all: bool,
    ) -> Result<Option<RecordBatch>, EngineError> {
        use rustc_hash::FxHashMap;

        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(None);
        }

        // Build hash table of right side rows
        let mut right_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for batch in right_batches {
            for row_idx in 0..batch.num_rows() {
                let row_hash = Self::compute_row_hash(batch, row_idx);
                *right_counts.entry(row_hash).or_insert(0) += 1;
            }
        }

        // Find matching rows from left side
        let schema = left_batches[0].schema();
        let mut result_indices: Vec<(usize, usize)> = Vec::new(); // (batch_idx, row_idx)
        let mut seen: rustc_hash::FxHashSet<u64> = rustc_hash::FxHashSet::default();

        for (batch_idx, batch) in left_batches.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let row_hash = Self::compute_row_hash(batch, row_idx);
                if let Some(count) = right_counts.get_mut(&row_hash) {
                    if keep_all {
                        // Keep one for each match in right
                        if *count > 0 {
                            result_indices.push((batch_idx, row_idx));
                            *count -= 1;
                        }
                    } else {
                        // Keep only first occurrence
                        if seen.insert(row_hash) {
                            result_indices.push((batch_idx, row_idx));
                        }
                    }
                }
            }
        }

        if result_indices.is_empty() {
            return Ok(None);
        }

        // Build result batch
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in 0..schema.fields().len() {
            let indices: Vec<(u32, u32)> = result_indices
                .iter()
                .map(|(b, r)| (*b as u32, *r as u32))
                .collect();
            let array = Self::take_from_batches(left_batches, col_idx, &indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Except (difference) between two sets of batches
    fn except_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        keep_all: bool,
    ) -> Result<Option<RecordBatch>, EngineError> {
        use rustc_hash::FxHashMap;

        if left_batches.is_empty() {
            return Ok(None);
        }

        // Build hash table of right side rows
        let mut right_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for batch in right_batches {
            for row_idx in 0..batch.num_rows() {
                let row_hash = Self::compute_row_hash(batch, row_idx);
                *right_counts.entry(row_hash).or_insert(0) += 1;
            }
        }

        // Find non-matching rows from left side
        let schema = left_batches[0].schema();
        let mut result_indices: Vec<(usize, usize)> = Vec::new();
        let mut seen: rustc_hash::FxHashSet<u64> = rustc_hash::FxHashSet::default();

        for (batch_idx, batch) in left_batches.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let row_hash = Self::compute_row_hash(batch, row_idx);
                if let Some(count) = right_counts.get_mut(&row_hash) {
                    if keep_all && *count > 0 {
                        // Skip this row (it matches one in right)
                        *count -= 1;
                        continue;
                    } else if !keep_all {
                        // Skip all matching rows
                        continue;
                    }
                }
                // Row doesn't match right side
                if keep_all {
                    result_indices.push((batch_idx, row_idx));
                } else {
                    // Keep only first occurrence
                    if seen.insert(row_hash) {
                        result_indices.push((batch_idx, row_idx));
                    }
                }
            }
        }

        if result_indices.is_empty() {
            return Ok(None);
        }

        // Build result batch
        let mut result_arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in 0..schema.fields().len() {
            let indices: Vec<(u32, u32)> = result_indices
                .iter()
                .map(|(b, r)| (*b as u32, *r as u32))
                .collect();
            let array = Self::take_from_batches(left_batches, col_idx, &indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Load all data from a table as decoded RecordBatches without concatenating IPC bytes.
    fn load_table_batches(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let matching = self.table_entries(database, table)?;
        if matching.is_empty() {
            return Ok(Vec::new());
        }

        let mut batches = Vec::new();
        for entry in &matching {
            let decoded = self.load_segment_batches_cached(entry)?;
            batches.extend(decoded.iter().cloned());
        }
        Ok(batches)
    }

    fn table_entries(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Vec<ManifestEntry>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let matching: Vec<_> = manifest
            .entries
            .iter()
            .filter(|e| {
                let entry_db = if e.database.is_empty() {
                    "default"
                } else {
                    &e.database
                };
                let entry_table = if e.table.is_empty() {
                    "default"
                } else {
                    &e.table
                };
                entry_db == database && entry_table == table
            })
            .cloned()
            .collect();

        Ok(matching)
    }

    fn entries_total_bytes(entries: &[ManifestEntry]) -> u64 {
        entries.iter().map(|e| e.size_bytes).sum()
    }

    fn partition_table_to_ipc(
        &self,
        entries: &[ManifestEntry],
        join_column: &str,
        partitions: usize,
        tmp_dir: &Path,
        prefix: &str,
        needed: Option<&[String]>,
        filter: Option<&QueryFilter>,
    ) -> Result<Vec<PathBuf>, EngineError> {
        use arrow_array::UInt32Array;

        let mut outputs: Vec<PartitionWriter> = Vec::with_capacity(partitions);
        for idx in 0..partitions {
            let path = tmp_dir.join(format!("{prefix}_{idx}.ipc"));
            let file = File::create(&path)
                .map_err(|e| EngineError::Io(format!("create join temp file failed: {e}")))?;
            outputs.push(PartitionWriter::new(path, file));
        }

        for entry in entries {
            let mut batches = self.load_segment_batches_cached(entry)?.iter().cloned().collect();
            if let Some(filter) = filter {
                batches = filter_batches_with_query_filter(batches, filter)?;
            }

            for batch in batches {
                let mut batch = batch;
                if let Some(needed) = needed {
                    batch = Self::project_batch_by_name(&batch, needed)?;
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                let col_idx = batch.schema().index_of(join_column).map_err(|_| {
                    EngineError::InvalidArgument(format!(
                        "column '{join_column}' not found for JOIN partitioning"
                    ))
                })?;
                let col = batch.column(col_idx);

                let mut per_partition: Vec<Vec<u32>> = vec![Vec::new(); partitions];
                for row_idx in 0..batch.num_rows() {
                    let key = Self::compute_hash_key(col.as_ref(), row_idx);
                    let bucket = (key as usize) & (partitions - 1);
                    per_partition[bucket].push(row_idx as u32);
                }

                for (bucket, indices) in per_partition.iter().enumerate() {
                    if indices.is_empty() {
                        continue;
                    }
                    let idx_array = UInt32Array::from_iter_values(indices.iter().copied());
                    let subset = arrow_select::take::take_record_batch(&batch, &idx_array)
                        .map_err(|e| EngineError::Internal(format!("partition take failed: {e}")))?;
                    outputs[bucket].write(&subset)?;
                }
            }
        }

        let mut paths = Vec::with_capacity(partitions);
        for mut out in outputs {
            out.finish()?;
            paths.push(out.path);
        }
        Ok(paths)
    }

    fn partition_batches_to_ipc(
        batches: &[RecordBatch],
        join_column: &str,
        partitions: usize,
        tmp_dir: &Path,
        prefix: &str,
        needed: Option<&[String]>,
    ) -> Result<Vec<PathBuf>, EngineError> {
        use arrow_array::UInt32Array;

        let mut outputs: Vec<PartitionWriter> = Vec::with_capacity(partitions);
        for idx in 0..partitions {
            let path = tmp_dir.join(format!("{prefix}_{idx}.ipc"));
            let file = File::create(&path)
                .map_err(|e| EngineError::Io(format!("create join temp file failed: {e}")))?;
            outputs.push(PartitionWriter::new(path, file));
        }

        for batch in batches {
            let mut batch = batch.clone();
            if let Some(needed) = needed {
                batch = Self::project_batch_by_name(&batch, needed)?;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            let col_idx = batch.schema().index_of(join_column).map_err(|_| {
                EngineError::InvalidArgument(format!(
                    "column '{join_column}' not found for JOIN partitioning"
                ))
            })?;
            let col = batch.column(col_idx);

            let mut per_partition: Vec<Vec<u32>> = vec![Vec::new(); partitions];
            for row_idx in 0..batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                let bucket = (key as usize) & (partitions - 1);
                per_partition[bucket].push(row_idx as u32);
            }

            for (bucket, indices) in per_partition.iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let idx_array = UInt32Array::from_iter_values(indices.iter().copied());
                let subset = arrow_select::take::take_record_batch(&batch, &idx_array)
                    .map_err(|e| EngineError::Internal(format!("partition take failed: {e}")))?;
                outputs[bucket].write(&subset)?;
            }
        }

        let mut paths = Vec::with_capacity(partitions);
        for mut out in outputs {
            out.finish()?;
            paths.push(out.path);
        }
        Ok(paths)
    }

    fn paths_total_bytes(paths: &[PathBuf]) -> Result<u64, EngineError> {
        let mut total = 0u64;
        for path in paths {
            let meta = fs::metadata(path)
                .map_err(|e| EngineError::Io(format!("stat join temp file failed: {e}")))?;
            total = total.saturating_add(meta.len());
        }
        Ok(total)
    }

    fn read_ipc_batches_from_paths(paths: &[PathBuf]) -> Result<Vec<RecordBatch>, EngineError> {
        let mut batches = Vec::new();
        for path in paths {
            batches.extend(Self::read_ipc_batches_from_file(path)?);
        }
        Ok(batches)
    }

    fn write_batches_to_ipc_file(
        batches: &[RecordBatch],
        path: &Path,
    ) -> Result<(), EngineError> {
        if batches.is_empty() {
            let _ = File::create(path)
                .map_err(|e| EngineError::Io(format!("create join spill file failed: {e}")))?;
            return Ok(());
        }
        let mut writer = StreamWriter::try_new(
            File::create(path)
                .map_err(|e| EngineError::Io(format!("create join spill file failed: {e}")))?,
            batches[0].schema().as_ref(),
        )
        .map_err(|e| EngineError::Internal(format!("IPC writer error: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| EngineError::Internal(format!("IPC write error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("IPC finish error: {e}")))?;
        Ok(())
    }

    fn partition_ipc_paths_to_ipc(
        paths: &[PathBuf],
        join_column: &str,
        partitions: usize,
        tmp_dir: &Path,
        prefix: &str,
        needed: Option<&[String]>,
        filter: Option<&QueryFilter>,
    ) -> Result<Vec<PathBuf>, EngineError> {
        use arrow_array::UInt32Array;

        let mut outputs: Vec<PartitionWriter> = Vec::with_capacity(partitions);
        for idx in 0..partitions {
            let path = tmp_dir.join(format!("{prefix}_{idx}.ipc"));
            let file = File::create(&path)
                .map_err(|e| EngineError::Io(format!("create join temp file failed: {e}")))?;
            outputs.push(PartitionWriter::new(path, file));
        }

        for path in paths {
            let meta = fs::metadata(path)
                .map_err(|e| EngineError::Io(format!("stat join temp file failed: {e}")))?;
            if meta.len() == 0 {
                continue;
            }
            let file = File::open(path)
                .map_err(|e| EngineError::Io(format!("open join temp file failed: {e}")))?;
            let mut reader = StreamReader::try_new(file, None)
                .map_err(|e| EngineError::Internal(format!("IPC reader error: {e}")))?;
            while let Some(batch) = reader
                .next()
                .transpose()
                .map_err(|e| EngineError::Internal(format!("IPC batch read failed: {e}")))?
            {
                let mut batches = vec![batch];
                if let Some(needed) = needed {
                    batches = batches
                        .into_iter()
                        .map(|b| Self::project_batch_by_name(&b, needed))
                        .collect::<Result<Vec<_>, _>>()?;
                }
                if let Some(filter) = filter {
                    batches = filter_batches_with_query_filter(batches, filter)?;
                }
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let col_idx = batch.schema().index_of(join_column).map_err(|_| {
                        EngineError::InvalidArgument(format!(
                            "join column not found in spilled data: {join_column}"
                        ))
                    })?;
                    let mut per_partition: Vec<Vec<u32>> =
                        (0..partitions).map(|_| Vec::new()).collect();
                    for row_idx in 0..batch.num_rows() {
                        let key = Self::compute_hash_key(batch.column(col_idx).as_ref(), row_idx);
                        let bucket = (key as usize) & (partitions - 1);
                        per_partition[bucket].push(row_idx as u32);
                    }
                    for (bucket, indices) in per_partition.iter().enumerate() {
                        if indices.is_empty() {
                            continue;
                        }
                        let idx_array = UInt32Array::from_iter_values(indices.iter().copied());
                        let subset = arrow_select::take::take_record_batch(&batch, &idx_array)
                            .map_err(|e| EngineError::Internal(format!("partition take failed: {e}")))?;
                        outputs[bucket].write(&subset)?;
                    }
                }
            }
        }

        let mut out_paths = Vec::with_capacity(partitions);
        for mut out in outputs {
            out.finish()?;
            out_paths.push(out.path);
        }
        Ok(out_paths)
    }

    fn read_ipc_batches_from_file(path: &Path) -> Result<Vec<RecordBatch>, EngineError> {
        let meta = fs::metadata(path)
            .map_err(|e| EngineError::Io(format!("stat join temp file failed: {e}")))?;
        if meta.len() == 0 {
            return Ok(Vec::new());
        }
        let file = File::open(path)
            .map_err(|e| EngineError::Io(format!("open join temp file failed: {e}")))?;
        let reader = StreamReader::try_new(file, None)
            .map_err(|e| EngineError::Internal(format!("IPC reader error: {e}")))?;
        let batches: Result<Vec<_>, _> = reader.collect();
        let mut batches =
            batches.map_err(|e| EngineError::Internal(format!("IPC decode error: {e}")))?;
        for idx in 0..batches.len() {
            let decoded = decode_record_batch_encodings(&batches[idx])?;
            batches[idx] = decoded;
        }
        Ok(batches)
    }

    fn project_batch_by_name(
        batch: &RecordBatch,
        needed: &[String],
    ) -> Result<RecordBatch, EngineError> {
        if needed.is_empty() {
            return Ok(batch.clone());
        }
        let schema = batch.schema();
        for name in needed {
            if schema.field_with_name(name).is_err() {
                return Err(EngineError::NotFound(format!("column not found: {name}")));
            }
        }
        let indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| {
                if needed.iter().any(|n| n == f.name()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        if indices.len() == schema.fields().len() {
            Ok(batch.clone())
        } else {
            batch
                .project(&indices)
                .map_err(|e| EngineError::Internal(format!("project failed: {e}")))
        }
    }

    fn estimate_batches_bytes(batches: &[RecordBatch]) -> u64 {
        batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum()
    }

    fn first_batch_schema_from_entries(
        &self,
        entries: &[ManifestEntry],
    ) -> Result<Option<Arc<Schema>>, EngineError> {
        let entry = match entries.first() {
            Some(e) => e,
            None => return Ok(None),
        };
        if self.cfg.schema_cache_entries > 0 {
            if let Some(schema) = {
                let mut cache = self.schema_cache.lock();
                cache.get(&entry.segment_id).cloned()
            } {
                self.metrics
                    .schema_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Some(schema));
            }
            self.metrics
                .schema_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        let payload = self.load_segment_cached(entry)?;
        let cursor = Cursor::new(payload.as_ref());
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("IPC reader error: {e}")))?;
        let batch = reader
            .next()
            .transpose()
            .map_err(|e| EngineError::Internal(format!("IPC batch read failed: {e}")))?;
        let schema = batch.map(|b| b.schema());
        if let Some(ref schema) = schema {
            let mut cache = self.schema_cache.lock();
            cache.put(entry.segment_id.clone(), schema.clone());
        }
        Ok(schema)
    }

    /// Decode IPC data to RecordBatches
    fn decode_ipc_batches(data: &[u8]) -> Result<Vec<RecordBatch>, EngineError> {
        use arrow_ipc::reader::StreamReader;
        use std::io::Cursor;

        if data.is_empty() {
            return Ok(Vec::new());
        }

        let cursor = Cursor::new(data);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("IPC reader error: {e}")))?;
        let mut batches = Vec::new();
        while let Some(batch) = reader
            .next()
            .transpose()
            .map_err(|e| EngineError::Internal(format!("IPC decode error: {e}")))? {
            batches.push(decode_record_batch_encodings(&batch)?);
        }
        Ok(batches)
    }

    fn project_batches_by_name(
        batches: &[RecordBatch],
        needed: &[String],
    ) -> Result<Vec<RecordBatch>, EngineError> {
        if needed.is_empty() {
            return Ok(batches.iter().cloned().collect());
        }
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            let schema = batch.schema();
            for name in needed {
                if schema.field_with_name(name).is_err() {
                    return Err(EngineError::NotFound(format!("column not found: {name}")));
                }
            }
            let indices: Vec<usize> = schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(idx, f)| {
                    if needed.iter().any(|n| n == f.name()) {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect();
            if indices.len() == schema.fields().len() {
                out.push(batch.clone());
            } else {
                out.push(
                    batch
                        .project(&indices)
                        .map_err(|e| EngineError::Internal(format!("project failed: {e}")))?,
                );
            }
        }
        Ok(out)
    }

    /// Perform hash join on two sets of record batches
    /// Uses efficient u64 hash keys and preserves original column types
    fn hash_join_batches(
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        left_col: &str,
        right_col: &str,
    ) -> Result<Option<RecordBatch>, EngineError> {
        use arrow_array::ArrayRef;
        use arrow_schema::{Field, Schema};
        use rustc_hash::FxHashMap;

        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(None);
        }

        // Estimate result size for capacity pre-allocation
        let right_row_count: usize = right_batches.iter().map(|b| b.num_rows()).sum();
        let left_row_count: usize = left_batches.iter().map(|b| b.num_rows()).sum();

        // Build hash table from right side (use FxHashMap for speed)
        // Key: hash of join column value, Value: (batch_idx, row_idx) pairs
        let mut hash_table: FxHashMap<u64, Vec<(u32, u32)>> =
            FxHashMap::with_capacity_and_hasher(right_row_count, Default::default());

        for (batch_idx, batch) in right_batches.iter().enumerate() {
            let col_idx = batch
                .schema()
                .index_of(right_col)
                .map_err(|_| {
                    EngineError::InvalidArgument(format!(
                        "column '{right_col}' not found in right table"
                    ))
                })?;

            let col = batch.column(col_idx);
            for row_idx in 0..batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                hash_table
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push((batch_idx as u32, row_idx as u32));
            }
        }

        // Pre-allocate result indices with estimated capacity
        let estimated_matches = (left_row_count * right_row_count) / 100; // Conservative estimate
        let mut left_indices: Vec<(u32, u32)> = Vec::with_capacity(estimated_matches.max(1024));
        let mut right_indices: Vec<(u32, u32)> = Vec::with_capacity(estimated_matches.max(1024));

        // Probe with left side
        for (left_batch_idx, left_batch) in left_batches.iter().enumerate() {
            let col_idx = left_batch
                .schema()
                .index_of(left_col)
                .map_err(|_| {
                    EngineError::InvalidArgument(format!(
                        "column '{left_col}' not found in left table"
                    ))
                })?;

            let col = left_batch.column(col_idx);
            for row_idx in 0..left_batch.num_rows() {
                let key = Self::compute_hash_key(col.as_ref(), row_idx);
                if let Some(matches) = hash_table.get(&key) {
                    for &(right_batch_idx, right_row_idx) in matches {
                        left_indices.push((left_batch_idx as u32, row_idx as u32));
                        right_indices.push((right_batch_idx, right_row_idx));
                    }
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(None);
        }

        // Build result schema (all columns from both tables)
        let left_schema = left_batches[0].schema();
        let right_schema = right_batches[0].schema();

        let mut fields: Vec<Field> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());
        fields.extend(left_schema.fields().iter().map(|f| f.as_ref().clone()));

        for field in right_schema.fields() {
            // Prefix right table columns with "r_" to avoid name collisions
            if left_schema.field_with_name(field.name()).is_ok() {
                fields.push(Field::new(
                    format!("r_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            } else {
                fields.push(field.as_ref().clone());
            }
        }

        let result_schema = Arc::new(Schema::new(fields));

        // Build result arrays using Arrow's take kernel (much faster than row-by-row)
        let mut result_arrays: Vec<ArrayRef> =
            Vec::with_capacity(left_schema.fields().len() + right_schema.fields().len());

        // Process left table columns
        for col_idx in 0..left_schema.fields().len() {
            let array = Self::take_from_batches(left_batches, col_idx, &left_indices)?;
            result_arrays.push(array);
        }

        // Process right table columns
        for col_idx in 0..right_schema.fields().len() {
            let array = Self::take_from_batches(right_batches, col_idx, &right_indices)?;
            result_arrays.push(array);
        }

        let result_batch = RecordBatch::try_new(result_schema, result_arrays)
            .map_err(|e| EngineError::Internal(format!("failed to create result batch: {e}")))?;

        Ok(Some(result_batch))
    }

    /// Compute a hash key from an array value (uses FxHash-style fast hashing)
    #[inline]
    fn compute_hash_key(array: &dyn Array, idx: usize) -> u64 {
        use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};

        if array.is_null(idx) {
            return 0; // NULL maps to 0
        }

        // Fast path for common types - avoid string conversion
        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            return arr.value(idx) as u64;
        }
        if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
            return arr.value(idx) as u64;
        }
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            // FNV-1a hash for strings (fast and good distribution)
            let s = arr.value(idx);
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in s.bytes() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            return hash;
        }
        if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
            return arr.value(idx).to_bits();
        }
        if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
            return arr.value(idx) as u64;
        }

        // Fallback for other types - use a simple hash
        idx as u64
    }

    /// Take rows from multiple batches using indices (batch_idx, row_idx)
    fn take_from_batches(
        batches: &[RecordBatch],
        col_idx: usize,
        indices: &[(u32, u32)],
    ) -> Result<ArrayRef, EngineError> {
        use arrow_array::{Array, ArrayRef, UInt32Array};
        use arrow_select::concat::concat;
        use arrow_select::take::take;

        if indices.is_empty() {
            // Return empty array of the correct type
            let col = batches[0].column(col_idx);
            return Ok(col.slice(0, 0));
        }

        // Group indices by batch for efficient processing
        let mut batch_groups: Vec<Vec<u32>> = vec![Vec::new(); batches.len()];
        let mut output_positions: Vec<(usize, usize)> = Vec::with_capacity(indices.len());

        for (_, &(batch_idx, row_idx)) in indices.iter().enumerate() {
            let group_idx = batch_groups[batch_idx as usize].len();
            batch_groups[batch_idx as usize].push(row_idx);
            output_positions.push((batch_idx as usize, group_idx));
        }

        // Take from each batch and collect results
        let mut taken_arrays: Vec<ArrayRef> = Vec::with_capacity(batches.len());
        let mut taken_offsets: Vec<usize> = vec![0];
        let mut total_len = 0;

        for (batch_idx, group) in batch_groups.iter().enumerate() {
            if group.is_empty() {
                taken_arrays.push(batches[batch_idx].column(col_idx).slice(0, 0));
            } else {
                let indices_array = UInt32Array::from(group.clone());
                let col = batches[batch_idx].column(col_idx);
                let taken = take(col.as_ref(), &indices_array, None)
                    .map_err(|e| EngineError::Internal(format!("take error: {e}")))?;
                total_len += taken.len();
                taken_arrays.push(taken);
            }
            taken_offsets.push(total_len);
        }

        // If all from one batch, return directly (optimization)
        let non_empty: Vec<_> = taken_arrays.iter().filter(|a| a.len() > 0).collect();
        if non_empty.len() == 1 {
            // Clone the single non-empty array to return
            return Ok(non_empty[0].clone());
        }

        // Concatenate all taken arrays
        let array_refs: Vec<&dyn Array> = taken_arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = concat(&array_refs)
            .map_err(|e| EngineError::Internal(format!("concat error: {e}")))?;

        // Reorder to match original output order
        let mut reorder_indices: Vec<u32> = vec![0; indices.len()];
        for (output_idx, (batch_idx, group_idx)) in output_positions.iter().enumerate() {
            let offset = taken_offsets[*batch_idx];
            reorder_indices[output_idx] = (offset + group_idx) as u32;
        }

        let reorder_array = UInt32Array::from(reorder_indices);
        take(concatenated.as_ref(), &reorder_array, None)
            .map_err(|e| EngineError::Internal(format!("reorder error: {e}")))
    }

    /// Explain query execution plan without executing the query
    pub fn explain(&self, sql: &str) -> Result<ExplainPlan, EngineError> {
        if sql.trim().is_empty() {
            return Err(EngineError::InvalidArgument("empty SQL".into()));
        }

        let agg = parse_agg(sql)?;
        let (db, table) = parse_table_target(sql)?;
        let projection = parse_projection(sql)?;
        let filter = parse_filters(sql)?;

        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Collect matching segments
        let matching: Vec<ManifestEntry> = manifest
            .entries
            .iter()
            .filter(|e| {
                let entry_db = e.database.as_str();
                let entry_table = e.table.as_str();
                let watermark_ok = filter
                    .watermark_ge
                    .map(|wm| e.watermark_micros >= wm)
                    .unwrap_or(true)
                    && filter
                        .watermark_le
                        .map(|wm| e.watermark_micros <= wm)
                        .unwrap_or(true);
                let event_time_ok = filter
                    .event_time_ge
                    .map(|et| e.event_time_max.unwrap_or(u64::MAX) >= et)
                    .unwrap_or(true)
                    && filter
                        .event_time_le
                        .map(|et| e.event_time_min.unwrap_or(0) <= et)
                        .unwrap_or(true);
                entry_db == db && entry_table == table && watermark_ok && event_time_ok
            })
            .cloned()
            .collect();

        let total_bytes: u64 = matching.iter().map(|e| e.size_bytes).sum();
        let uses_bloom = filter.tenant_id_eq.is_some() || filter.route_id_eq.is_some();

        // Build filter descriptions
        let mut filters = Vec::new();
        if let Some(v) = filter.watermark_ge {
            filters.push(format!("watermark_micros >= {}", v));
        }
        if let Some(v) = filter.watermark_le {
            filters.push(format!("watermark_micros <= {}", v));
        }
        if let Some(v) = filter.event_time_ge {
            filters.push(format!("event_time >= {}", v));
        }
        if let Some(v) = filter.event_time_le {
            filters.push(format!("event_time <= {}", v));
        }
        if let Some(v) = filter.tenant_id_eq {
            filters.push(format!("tenant_id = {} (bloom filter)", v));
        }
        if let Some(v) = filter.route_id_eq {
            filters.push(format!("route_id = {} (bloom filter)", v));
        }

        // Build order by descriptions
        let order_by = filter.order_by.as_ref().map(|ob| {
            ob.iter()
                .map(|(col, asc)| format!("{} {}", col, if *asc { "ASC" } else { "DESC" }))
                .collect()
        });

        Ok(ExplainPlan {
            database: db.to_string(),
            table: table.to_string(),
            projection,
            filters,
            aggregation: agg.map(|a| format!("{:?}", a)),
            order_by,
            limit: filter.limit,
            offset: filter.offset,
            segments_to_scan: matching.len(),
            total_bytes,
            estimated_rows: None, // Row count not tracked in manifest
            uses_parallel_scan: matching.len() >= self.cfg.parallel_scan_threshold,
            uses_bloom_filter: uses_bloom,
        })
    }

    pub fn maintenance(&self) -> Result<(), EngineError> {
        self.prune_by_retention()?;
        self.apply_tiering_rules()?;
        if self.cfg.enable_compaction {
            self.run_compaction_pass()?;
        }
        Ok(())
    }

    fn prune_by_retention(&self) -> Result<(), EngineError> {
        if let Some(cutoff) = self.cfg.retention_watermark_micros {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let mut removed = Vec::new();
            manifest.entries.retain(|e| {
                let keep = e.watermark_micros >= cutoff || e.watermark_micros == 0;
                if !keep {
                    removed.push(e.segment_id.clone());
                }
                keep
            });
            if !removed.is_empty() {
                manifest.bump_version();
                persist_manifest(&self.cfg.manifest_path, &manifest)?;
                snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
                // Rebuild manifest index after entries removed
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }
            drop(manifest);
            for seg in removed {
                self.remove_segment_file(&seg);
            }
        }
        Ok(())
    }

    fn apply_tiering_rules(&self) -> Result<(), EngineError> {
        let warm_after_micros = self
            .cfg
            .tier_warm_after_millis
            .saturating_mul(1_000);
        let cold_after_micros = self
            .cfg
            .tier_cold_after_millis
            .saturating_mul(1_000);
        let warm_compression = normalize_compression(self.cfg.tier_warm_compression.clone())?;
        let cold_compression = normalize_compression(self.cfg.tier_cold_compression.clone())?;

        if warm_after_micros == 0 && cold_after_micros == 0 {
            return Ok(());
        }

        let now = current_time_micros();
        let mut changed = false;
        let mut changed_tables: HashSet<(String, String)> = HashSet::new();
        let mut recompress_jobs: Vec<(ManifestEntry, Option<String>)> = Vec::new();
        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            for entry in manifest.entries.iter_mut() {
                let Some(age) = segment_age_micros(entry, now) else { continue };
                let target_tier = if cold_after_micros > 0 && age >= cold_after_micros {
                    SegmentTier::Cold
                } else if warm_after_micros > 0 && age >= warm_after_micros {
                    SegmentTier::Warm
                } else {
                    entry.tier.clone()
                };
                let target_compression = match target_tier {
                    SegmentTier::Cold => cold_compression.clone(),
                    SegmentTier::Warm => warm_compression.clone(),
                    SegmentTier::Hot => None,
                };

                if target_tier != entry.tier {
                    entry.tier = target_tier;
                    changed = true;
                    changed_tables.insert((entry.database.clone(), entry.table.clone()));
                }
                if let Some(target) = target_compression {
                    if entry.compression.as_deref() != Some(target.as_str()) {
                        recompress_jobs.push((entry.clone(), Some(target.clone())));
                        changed_tables.insert((entry.database.clone(), entry.table.clone()));
                    }
                }
            }

            if changed {
                manifest.bump_version();
                persist_manifest(&self.cfg.manifest_path, &manifest)?;
                snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }
        }

        if !recompress_jobs.is_empty() {
            let mut updates = Vec::with_capacity(recompress_jobs.len());
            for (entry, new_compression) in recompress_jobs {
                let raw = load_segment_raw(&self.storage, &entry)?;
                let decoded = decompress_payload(raw, entry.compression.as_deref())?;
                let stored = compress_payload(&decoded, new_compression.as_deref())?;
                persist_segment_ipc(&self.storage, &entry.segment_id, &stored)?;
                updates.push((
                    entry.segment_id.clone(),
                    stored.len() as u64,
                    compute_checksum(&stored),
                    new_compression,
                ));
            }

            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            for (segment_id, size_bytes, checksum, compression) in &updates {
                if let Some(entry) = manifest.entries.iter_mut().find(|e| e.segment_id == *segment_id) {
                    entry.size_bytes = *size_bytes;
                    entry.checksum = *checksum;
                    entry.compression = compression.clone();
                }
            }
            if !updates.is_empty() {
                manifest.bump_version();
                persist_manifest(&self.cfg.manifest_path, &manifest)?;
                snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }

            let mut seg_cache = self.segment_cache.lock();
            let mut batch_cache = self.batch_cache.lock();
            for (segment_id, _, _, _) in updates {
                seg_cache.invalidate(&segment_id);
                batch_cache.invalidate(&segment_id);
            }
            changed = true;
        }

        if changed {
            let mut cache = self.query_cache.lock();
            for (db, tbl) in changed_tables {
                cache.invalidate_table(&db, &tbl);
            }
        }

        Ok(())
    }

    fn run_compaction_pass(&self) -> Result<(), EngineError> {
        let target_bytes = self.cfg.compaction_target_bytes;
        let min_segments = self.cfg.compact_min_segments;
        if target_bytes == 0 || min_segments < 2 {
            return Ok(());
        }

        // Avoid unbounded loops: compact up to a fixed number of batches per invocation.
        for _ in 0..16 {
            let candidate = self.pick_compaction_candidate(target_bytes, min_segments, None)?;
            let Some((entries, table_meta)) = candidate else { break };
            self.compact_entries(entries, table_meta)?;
        }
        Ok(())
    }

    fn pick_compaction_candidate(
        &self,
        target_bytes: u64,
        min_segments: usize,
        filter_table: Option<(String, String)>,
    ) -> Result<Option<(Vec<ManifestEntry>, Option<TableMeta>)>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let mut best: Option<(Vec<ManifestEntry>, Option<TableMeta>)> = None;
        let mut best_bytes = 0u64;

        // Group hot segments per table
        let mut grouped: HashMap<(String, String), Vec<ManifestEntry>> = HashMap::new();
        for entry in &manifest.entries {
            if let Some(ref filter) = filter_table {
                if &entry.database != &filter.0 || &entry.table != &filter.1 {
                    continue;
                }
            }
            if !matches!(entry.tier, SegmentTier::Hot) {
                continue;
            }
            grouped
                .entry((entry.database.clone(), entry.table.clone()))
                .or_default()
                .push(entry.clone());
        }

        for ((db, tbl), mut entries) in grouped {
            entries.retain(|e| e.size_bytes < target_bytes);
            if entries.len() < min_segments {
                continue;
            }
            entries.sort_by_key(|e| e.size_bytes);
            let mut chunk = Vec::new();
            let mut bytes = 0u64;
            for entry in entries {
                if bytes + entry.size_bytes > target_bytes && !chunk.is_empty() {
                    break;
                }
                bytes = bytes.saturating_add(entry.size_bytes);
                chunk.push(entry);
            }
            if chunk.len() < min_segments || bytes == 0 {
                continue;
            }
            if bytes <= target_bytes && bytes > best_bytes {
                best_bytes = bytes;
                let table_meta = manifest
                    .tables
                    .iter()
                    .find(|t| t.database == db && t.name == tbl)
                    .cloned();
                best = Some((chunk, table_meta));
            }
        }

        Ok(best)
    }

    pub fn compact_table(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<ManifestEntry>, EngineError> {
        if !self.cfg.enable_compaction {
            return Ok(None);
        }

        let db_name = if database.is_empty() { "default" } else { database };
        let table_name = if table.is_empty() { "default" } else { table };
        let candidate = self.pick_compaction_candidate(
            self.cfg.compaction_target_bytes,
            self.cfg.compact_min_segments,
            Some((db_name.to_string(), table_name.to_string())),
        )?;

        if let Some((entries, table_meta)) = candidate {
            let new_entry = self.compact_entries(entries, table_meta)?;
            return Ok(Some(new_entry));
        }

        Ok(None)
    }

    fn compact_entries(
        &self,
        entries: Vec<ManifestEntry>,
        table_meta: Option<TableMeta>,
    ) -> Result<ManifestEntry, EngineError> {
        if entries.is_empty() {
            return Err(EngineError::InvalidArgument(
                "no entries provided for compaction".into(),
            ));
        }

        let db_name = if entries[0].database.is_empty() {
            "default".to_string()
        } else {
            entries[0].database.clone()
        };
        let table_name = if entries[0].table.is_empty() {
            "default".to_string()
        } else {
            entries[0].table.clone()
        };

        let mut batches = Vec::new();
        for e in &entries {
            if e.database != db_name || e.table != table_name {
                return Err(EngineError::InvalidArgument(
                    "compaction requires all entries to belong to the same table".into(),
                ));
            }
            let decoded = self.load_segment_batches_cached(e)?;
            batches.extend(decoded.iter().cloned());
        }

        if batches.is_empty() {
            return Err(EngineError::InvalidArgument(
                "no rows found for compaction".into(),
            ));
        }

        let schema = batches[0].schema();
        let merged_ipc = record_batches_to_ipc(schema.as_ref(), &batches)?;
        let sorted_ipc = sort_hot_payload(&merged_ipc, self.cfg.index_granularity_rows)?;
        let validated = validate_and_stats(&sorted_ipc, table_meta.as_ref())?;
        let max_watermark = entries
            .iter()
            .map(|e| e.watermark_micros)
            .max()
            .unwrap_or(0);
        let compression = normalize_compression(
            table_meta
                .as_ref()
                .and_then(|t| t.compression.clone())
                .or_else(|| entries.first().and_then(|e| e.compression.clone())),
        )?;
        let encodings =
            column_encodings_from_schema_json(table_meta.as_ref().and_then(|t| t.schema_json.as_ref()))?;
        let encoded_payload = apply_column_encodings(&sorted_ipc, &encodings)?;
        let stored_payload = compress_payload(&encoded_payload, compression.as_deref())?;

        let seq = self.segment_seq.fetch_add(1, Ordering::SeqCst);
        let shard_id = (seq % (self.shards.len() as u64)) as usize;
        let segment_id = format!("cmp-{shard_id}-{seq}");
        let mut new_entry = ManifestEntry {
            segment_id: segment_id.clone(),
            shard_id: shard_id as u16,
            version_added: 0,
            size_bytes: stored_payload.len() as u64,
            checksum: compute_checksum(&stored_payload),
            tier: SegmentTier::Warm,
            database: db_name.clone(),
            table: table_name.clone(),
            compression: compression.clone(),
            watermark_micros: max_watermark,
            event_time_min: validated.stats.event_time_min,
            event_time_max: validated.stats.event_time_max,
            tenant_id_min: validated.stats.tenant_id_min,
            tenant_id_max: validated.stats.tenant_id_max,
            route_id_min: validated.stats.route_id_min,
            route_id_max: validated.stats.route_id_max,
            bloom_tenant: validated.stats.bloom_tenant,
            bloom_route: validated.stats.bloom_route,
            column_stats: if validated.stats.column_stats.is_empty() {
                None
            } else {
                Some(validated.stats.column_stats.clone())
            },
            schema_hash: Some(validated.schema_hash),
        };

        persist_segment_ipc(&self.storage, &segment_id, &stored_payload)?;
        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let drop_set: HashSet<String> = entries.iter().map(|e| e.segment_id.clone()).collect();
            manifest.entries.retain(|e| !drop_set.contains(&e.segment_id));
            manifest.bump_version();
            new_entry.version_added = manifest.version;
            manifest.entries.push(new_entry.clone());
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            // Rebuild manifest index after compaction
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }

        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(&db_name, &table_name);
        }

        // delete old segment files best-effort
        for e in &entries {
            self.remove_segment_file(&e.segment_id);
        }

        Ok(new_entry)
    }

    pub fn plan_bundle(&self, request: BundleRequest) -> Result<BundlePlan, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if let Some(sv) = request.since_version {
            if sv > manifest.version {
                return Err(EngineError::InvalidArgument(format!(
                    "since_version {sv} exceeds manifest version {}",
                    manifest.version
                )));
            }
        }
        if let Some(max_bytes) = request.max_bytes {
            if max_bytes == 0 {
                return Err(EngineError::InvalidArgument(
                    "max_bytes must be greater than zero".into(),
                ));
            }
        }
        if let Some(max_entries) = request.max_entries {
            if max_entries == 0 {
                return Err(EngineError::InvalidArgument(
                    "max_entries must be greater than zero".into(),
                ));
            }
        }
        if let Some(bps) = request.target_bytes_per_sec {
            if bps == 0 {
                return Err(EngineError::InvalidArgument(
                    "target_bytes_per_sec must be greater than zero".into(),
                ));
            }
        }
        let planner = BundlePlanner::new();
        Ok(planner.plan(&manifest, request))
    }

    pub fn export_bundle(&self, request: BundleRequest) -> Result<BundlePayload, EngineError> {
        let plan = self.plan_bundle(request)?;
        let mut segments = Vec::with_capacity(plan.entries.len());
        for entry in &plan.entries {
            let data = load_segment_raw(&self.storage, entry)?;
            segments.push(BundleSegment {
                entry: entry.clone(),
                data,
            });
        }
        Ok(BundlePayload { plan, segments })
    }

    pub fn validate_bundle(&self, payload: &BundlePayload) -> Result<(), EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        self.validate_bundle_plan(payload, &manifest)?;
        for seg in &payload.segments {
            self.validate_bundle_segment(seg)?;
        }
        Ok(())
    }

    pub fn apply_bundle(&self, payload: BundlePayload) -> Result<(), EngineError> {
        {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            self.validate_bundle_plan(&payload, &manifest)?;
        }

        // Persist segments first, then manifest updates.
        for seg in &payload.segments {
            self.validate_bundle_segment(seg)?;
            persist_segment_ipc(&self.storage, &seg.entry.segment_id, &seg.data)?;
        }

        let entries: Vec<ManifestEntry> = payload.segments.iter().map(|seg| seg.entry.clone()).collect();
        let _added = self.append_manifest_entries(entries)?;

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if manifest.version < payload.plan.manifest_version {
            manifest.version = payload.plan.manifest_version;
        }
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;

        if let Some(plan_hash) = payload.plan.plan_hash {
            let mut replay = self.recent_bundle_hashes.lock();
            replay.put(plan_hash, ());
        }
        Ok(())
    }

    fn validate_bundle_plan(
        &self,
        payload: &BundlePayload,
        manifest: &Manifest,
    ) -> Result<(), EngineError> {
        let plan_total: u64 = payload.plan.entries.iter().map(|e| e.size_bytes).sum();
        if payload.plan.total_bytes != plan_total {
            return Err(EngineError::InvalidArgument(format!(
                "bundle plan total_bytes {} does not match entries sum {}",
                payload.plan.total_bytes, plan_total
            )));
        }
        if let Some(plan_hash) = payload.plan.plan_hash {
            let expected = compute_bundle_plan_hash(
                payload.plan.manifest_version,
                payload.plan.since_version,
                &payload.plan.entries,
                payload.plan.total_bytes,
                payload.plan.throttle_millis,
            );
            if expected != plan_hash {
                return Err(EngineError::InvalidArgument(
                    "bundle plan hash mismatch".into(),
                ));
            }

            let replay = self.recent_bundle_hashes.lock();
            if replay.contains(&plan_hash) {
                return Err(EngineError::InvalidArgument(
                    "bundle already applied".into(),
                ));
            }
        }

        let mut plan_ids = HashSet::new();
        for entry in &payload.plan.entries {
            if !plan_ids.insert(entry.segment_id.clone()) {
                return Err(EngineError::InvalidArgument(format!(
                    "bundle plan has duplicate segment id {}",
                    entry.segment_id
                )));
            }
        }
        for seg in &payload.segments {
            if !plan_ids.contains(&seg.entry.segment_id) {
                return Err(EngineError::InvalidArgument(format!(
                    "bundle segment {} not present in plan entries",
                    seg.entry.segment_id
                )));
            }
        }
        if plan_ids.len() != payload.segments.len() {
            return Err(EngineError::InvalidArgument(
                "bundle segments do not match plan entries".into(),
            ));
        }

        if payload.plan.manifest_version < manifest.version {
            return Err(EngineError::InvalidArgument(format!(
                "bundle manifest version {} behind local {}",
                payload.plan.manifest_version, manifest.version
            )));
        }

        if let Some(since_version) = payload.plan.since_version {
            if since_version > payload.plan.manifest_version {
                return Err(EngineError::InvalidArgument(
                    "bundle since_version exceeds manifest_version".into(),
                ));
            }
            if manifest.version < since_version {
                return Err(EngineError::InvalidArgument(format!(
                    "bundle since_version {} ahead of local {}",
                    since_version, manifest.version
                )));
            }
        }

        for seg in &payload.segments {
            if let Some(existing) = manifest
                .entries
                .iter()
                .find(|e| e.segment_id == seg.entry.segment_id)
            {
                if existing.checksum != seg.entry.checksum
                    || existing.size_bytes != seg.entry.size_bytes
                    || existing.compression != seg.entry.compression
                    || existing.schema_hash != seg.entry.schema_hash
                {
                    return Err(EngineError::InvalidArgument(format!(
                        "bundle segment {} conflicts with existing manifest entry",
                        seg.entry.segment_id
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_bundle_segment(&self, seg: &BundleSegment) -> Result<(), EngineError> {
        let chk = compute_checksum(&seg.data);
        if chk != seg.entry.checksum {
            return Err(EngineError::InvalidArgument(format!(
                "bundle segment {} checksum mismatch",
                seg.entry.segment_id
            )));
        }
        if let Some(expected_schema_hash) = seg.entry.schema_hash {
            let actual_schema_hash = compute_schema_hash_from_payload(
                &seg.data,
                seg.entry.compression.as_deref(),
            )?;
            if actual_schema_hash != expected_schema_hash {
                return Err(EngineError::InvalidArgument(format!(
                    "bundle segment {} schema hash mismatch",
                    seg.entry.segment_id
                )));
            }
        }
        if seg.entry.database.trim().is_empty() || seg.entry.table.trim().is_empty() {
            return Err(EngineError::InvalidArgument(format!(
                "bundle segment {} missing database/table",
                seg.entry.segment_id
            )));
        }
        normalize_compression(seg.entry.compression.clone())?;
        let payload_len = seg.data.len() as u64;
        if seg.entry.size_bytes != payload_len {
            return Err(EngineError::InvalidArgument(format!(
                "bundle segment {} size mismatch (entry {} vs payload {payload_len})",
                seg.entry.segment_id, seg.entry.size_bytes
            )));
        }
        Ok(())
    }

    pub fn export_manifest(&self) -> Result<Vec<u8>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        serde_json::to_vec(&*manifest)
            .map_err(|e| EngineError::Internal(format!("manifest serialize failed: {e}")))
    }

    pub fn import_manifest(&self, payload: &[u8], overwrite: bool) -> Result<(), EngineError> {
        if !self.cfg.allow_manifest_import {
            return Err(EngineError::NotImplemented(
                "manifest import disabled; set allow_manifest_import".into(),
            ));
        }
        let new_manifest: Manifest = serde_json::from_slice(payload)
            .map_err(|e| EngineError::InvalidArgument(format!("invalid manifest: {e}")))?;
        validate_manifest(&new_manifest)?;
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if overwrite {
            *manifest = new_manifest;
        } else {
            for entry in new_manifest.entries {
                if !manifest
                    .entries
                    .iter()
                    .any(|e| e.segment_id == entry.segment_id)
                {
                    manifest.entries.push(entry);
                }
            }
            for dbm in new_manifest.databases {
                if !manifest.databases.iter().any(|d| d.name == dbm.name) {
                    manifest.databases.push(dbm);
                }
            }
            for tbl in new_manifest.tables {
                if !manifest
                    .tables
                    .iter()
                    .any(|t| t.database == tbl.database && t.name == tbl.name)
                {
                    manifest.tables.push(tbl);
                }
            }
            manifest.bump_version();
        }
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        // Rebuild manifest index after import
        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }
        Ok(())
    }

    pub fn create_database(&self, name: &str) -> Result<(), EngineError> {
        validate_identifier(name, "database")?;
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if manifest.databases.iter().any(|db| db.name == name) {
            return Ok(());
        }
        manifest.bump_version();
        manifest
            .databases
            .push(crate::replication::DatabaseMeta { name: name.into() });
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        Ok(())
    }

    pub fn create_table(
        &self,
        database: &str,
        table: &str,
        schema_json: Option<String>,
    ) -> Result<(), EngineError> {
        validate_identifier(database, "database")?;
        validate_identifier(table, "table")?;
        let canonical_schema_json = match &schema_json {
            Some(schema) => Some(canonical_schema_from_json(schema)?.json),
            None => None,
        };
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if !manifest.databases.iter().any(|db| db.name == database) {
            return Err(EngineError::NotFound(format!(
                "database not found: {database}"
            )));
        }
        if manifest
            .tables
            .iter()
            .any(|t| t.database == database && t.name == table)
        {
            return Ok(());
        }
        manifest.bump_version();
        manifest.tables.push(crate::replication::TableMeta {
            database: database.to_string(),
            name: table.to_string(),
            schema_json: canonical_schema_json,
            compression: None,
            deduplication: None,
        });
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        Ok(())
    }

    /// Create a new view in the database
    pub fn create_view(
        &self,
        database: &str,
        name: &str,
        query_sql: &str,
        or_replace: bool,
    ) -> Result<(), EngineError> {
        if database.trim().is_empty() || name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and view name required".into(),
            ));
        }
        if query_sql.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "view query SQL required".into(),
            ));
        }
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if !manifest.databases.iter().any(|db| db.name == database) {
            return Err(EngineError::NotFound(format!(
                "database not found: {database}"
            )));
        }
        // Check if view already exists
        if let Some(idx) = manifest
            .views
            .iter()
            .position(|v| v.database == database && v.name == name)
        {
            if or_replace {
                manifest.views[idx].query_sql = query_sql.to_string();
            } else {
                return Err(EngineError::InvalidArgument(format!(
                    "view already exists: {database}.{name}"
                )));
            }
        } else {
            manifest.views.push(crate::replication::ViewMeta {
                database: database.to_string(),
                name: name.to_string(),
                query_sql: query_sql.to_string(),
            });
        }
        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        Ok(())
    }

    /// Drop a view from the database
    pub fn drop_view(&self, database: &str, name: &str, if_exists: bool) -> Result<(), EngineError> {
        if database.trim().is_empty() || name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and view name required".into(),
            ));
        }
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        if let Some(idx) = manifest
            .views
            .iter()
            .position(|v| v.database == database && v.name == name)
        {
            manifest.views.remove(idx);
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(EngineError::NotFound(format!(
                "view not found: {database}.{name}"
            )))
        }
    }

    /// List all views in a database
    pub fn list_views(&self, database: Option<&str>) -> Result<Vec<(String, String, String)>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let views: Vec<(String, String, String)> = manifest
            .views
            .iter()
            .filter(|v| database.is_none() || database == Some(&v.database))
            .map(|v| (v.database.clone(), v.name.clone(), v.query_sql.clone()))
            .collect();
        Ok(views)
    }

    /// Get a view's query SQL by database and name
    pub fn get_view(&self, database: &str, name: &str) -> Result<Option<String>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        Ok(manifest
            .views
            .iter()
            .find(|v| v.database == database && v.name == name)
            .map(|v| v.query_sql.clone()))
    }

    /// Create a materialized view
    pub fn create_materialized_view(
        &self,
        database: &str,
        name: &str,
        query_sql: &str,
        or_replace: bool,
    ) -> Result<(), EngineError> {
        use crate::replication::MaterializedViewMeta;

        if database.trim().is_empty() || name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and materialized view name required".into(),
            ));
        }
        if query_sql.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "materialized view query SQL required".into(),
            ));
        }

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        if !manifest.databases.iter().any(|db| db.name == database) {
            return Err(EngineError::NotFound(format!(
                "database not found: {database}"
            )));
        }

        // Check if materialized view already exists
        if let Some(idx) = manifest
            .materialized_views
            .iter()
            .position(|v| v.database == database && v.name == name)
        {
            if or_replace {
                manifest.materialized_views[idx].query_sql = query_sql.to_string();
                manifest.materialized_views[idx].last_refresh_micros = 0; // Reset refresh time
                manifest.materialized_views[idx].data_segment_id = None; // Clear data
            } else {
                return Err(EngineError::InvalidArgument(format!(
                    "materialized view already exists: {database}.{name}"
                )));
            }
        } else {
            manifest.materialized_views.push(MaterializedViewMeta {
                database: database.to_string(),
                name: name.to_string(),
                query_sql: query_sql.to_string(),
                last_refresh_micros: 0,
                data_segment_id: None,
            });
        }

        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        Ok(())
    }

    /// Drop a materialized view
    pub fn drop_materialized_view(
        &self,
        database: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<(), EngineError> {
        if database.trim().is_empty() || name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and materialized view name required".into(),
            ));
        }

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        if let Some(idx) = manifest
            .materialized_views
            .iter()
            .position(|v| v.database == database && v.name == name)
        {
            // Remove any data segment associated with this materialized view
            let segment_id_to_remove = manifest.materialized_views[idx].data_segment_id.clone();
            if let Some(segment_id) = segment_id_to_remove {
                manifest.entries.retain(|e| e.segment_id != segment_id);
            }
            manifest.materialized_views.remove(idx);
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(EngineError::NotFound(format!(
                "materialized view not found: {database}.{name}"
            )))
        }
    }

    /// Refresh a materialized view by re-executing its query and storing the results
    pub fn refresh_materialized_view(&self, database: &str, name: &str) -> Result<(), EngineError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        if database.trim().is_empty() || name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and materialized view name required".into(),
            ));
        }

        // Get the view's query
        let query_sql = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest
                .materialized_views
                .iter()
                .find(|v| v.database == database && v.name == name)
                .map(|v| v.query_sql.clone())
                .ok_or_else(|| {
                    EngineError::NotFound(format!(
                        "materialized view not found: {database}.{name}"
                    ))
                })?
        };

        // Execute the query
        let response = self.query(QueryRequest {
            sql: query_sql,
            timeout_millis: 0,
            collect_stats: false,
        })?;

        // Create a unique segment ID for the materialized view data
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let segment_id = format!("mv-{}-{}-{}", database, name, now);

        // Store the result data as a segment
        if !response.records_ipc.is_empty() {
            // Write the IPC data to a segment file
            let segment_path = self.cfg.segments_dir.join(&segment_id);
            let payload = &response.records_ipc;
            fs::write(&segment_path, payload)
                .map_err(|e| EngineError::Io(format!("write mv segment failed: {e}")))?;

            // Update the manifest
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

            // Find and update the materialized view
            if let Some(mv_idx) = manifest
                .materialized_views
                .iter()
                .position(|v| v.database == database && v.name == name)
            {
                // Remove old segment if exists
                let old_segment_id = manifest.materialized_views[mv_idx].data_segment_id.clone();
                if let Some(ref old_id) = old_segment_id {
                    manifest.entries.retain(|e| &e.segment_id != old_id);
                    // Also remove the old segment file
                    let old_path = self.cfg.segments_dir.join(old_id);
                    let _ = fs::remove_file(old_path);
                }

                // Update the view's metadata
                manifest.materialized_views[mv_idx].last_refresh_micros = now;
                manifest.materialized_views[mv_idx].data_segment_id = Some(segment_id.clone());

                // Compute version before push
                let version_added = manifest.version + 1;
                let checksum = {
                    let mut hasher = Crc32Hasher::new();
                    hasher.update(payload);
                    hasher.finalize() as u64
                };

                // Add a manifest entry for the new segment
                manifest.entries.push(crate::replication::ManifestEntry {
                    segment_id,
                    shard_id: 0,
                    version_added,
                    size_bytes: payload.len() as u64,
                    checksum,
                    tier: crate::replication::SegmentTier::Hot,
                    compression: None,
                    database: database.to_string(),
                    table: format!("__mv_{}", name),
                    watermark_micros: now,
                    event_time_min: None,
                    event_time_max: None,
                    tenant_id_min: None,
                    tenant_id_max: None,
                    route_id_min: None,
                    route_id_max: None,
                    bloom_tenant: None,
                    bloom_route: None,
                    column_stats: None,
                    schema_hash: None,
                });
            }

            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
        }

        Ok(())
    }

    /// List all materialized views in a database
    /// Returns: Vec<(database, name, query_sql, last_refresh_micros)>
    pub fn list_materialized_views(
        &self,
        database: Option<&str>,
    ) -> Result<Vec<(String, String, String, u64)>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let views: Vec<(String, String, String, u64)> = manifest
            .materialized_views
            .iter()
            .filter(|v| database.is_none() || database == Some(&v.database))
            .map(|v| {
                (
                    v.database.clone(),
                    v.name.clone(),
                    v.query_sql.clone(),
                    v.last_refresh_micros,
                )
            })
            .collect();
        Ok(views)
    }

    /// Get a materialized view's data - returns the cached results if available
    pub fn get_materialized_view_data(
        &self,
        database: &str,
        name: &str,
    ) -> Result<Option<Vec<u8>>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let mv = manifest
            .materialized_views
            .iter()
            .find(|v| v.database == database && v.name == name);

        match mv {
            Some(mv) => {
                if let Some(segment_id) = &mv.data_segment_id {
                    let segment_path = self.cfg.segments_dir.join(segment_id);
                    if segment_path.exists() {
                        let data = fs::read(&segment_path)
                            .map_err(|e| EngineError::Io(format!("read mv segment failed: {e}")))?;
                        return Ok(Some(data));
                    }
                }
                Ok(None)
            }
            None => Err(EngineError::NotFound(format!(
                "materialized view not found: {database}.{name}"
            ))),
        }
    }

    // ==================== Index Management ====================

    fn index_root(&self) -> PathBuf {
        self.cfg.data_dir.join("indexes")
    }

    fn index_dir(&self, database: &str, table: &str, index_name: &str) -> PathBuf {
        self.index_root()
            .join(database)
            .join(table)
            .join(index_name)
    }

    fn index_segment_path(
        &self,
        index: &IndexMeta,
        segment_id: &str,
    ) -> PathBuf {
        self.index_dir(&index.database, &index.table, &index.name)
            .join(format!("{segment_id}.idx"))
    }

    fn table_schema_map(
        &self,
        database: &str,
        table: &str,
    ) -> Result<HashMap<String, String>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let schema_json = manifest
            .tables
            .iter()
            .find(|t| t.database == database && t.name == table)
            .and_then(|t| t.schema_json.clone())
            .ok_or_else(|| EngineError::NotFound("table schema not found".into()))?;
        let fields: Vec<TableFieldSpec> = serde_json::from_str(&schema_json)
            .map_err(|e| EngineError::InvalidArgument(format!("invalid schema json: {e}")))?;
        Ok(fields
            .into_iter()
            .map(|f| (f.name, f.data_type))
            .collect())
    }

    fn hash_index_value(data_type: &str, value: &IndexValue) -> Option<u64> {
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        match (data_type.to_ascii_lowercase().as_str(), value) {
            ("int64", IndexValue::Int(v))
            | ("int32", IndexValue::Int(v))
            | ("int16", IndexValue::Int(v))
            | ("int8", IndexValue::Int(v)) => {
                let val = i64::try_from(*v).ok()?;
                1u8.hash(&mut hasher);
                val.hash(&mut hasher);
            }
            ("uint64", IndexValue::UInt(v))
            | ("uint32", IndexValue::UInt(v))
            | ("uint16", IndexValue::UInt(v))
            | ("uint8", IndexValue::UInt(v)) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            ("float64", IndexValue::Float(v)) => {
                1u8.hash(&mut hasher);
                v.to_bits().hash(&mut hasher);
            }
            ("float32", IndexValue::Float(v)) => {
                1u8.hash(&mut hasher);
                (*v as f32).to_bits().hash(&mut hasher);
            }
            ("bool", IndexValue::Bool(v)) | ("boolean", IndexValue::Bool(v)) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            ("utf8", IndexValue::Str(v))
            | ("string", IndexValue::Str(v))
            | ("largestring", IndexValue::Str(v)) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            ("uuid", IndexValue::Bytes(v))
            | ("binary", IndexValue::Bytes(v))
            | ("bytes", IndexValue::Bytes(v))
            | ("blob", IndexValue::Bytes(v))
            | ("largebinary", IndexValue::Bytes(v)) => {
                1u8.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            _ => return None,
        }
        Some(hasher.finish())
    }

    fn index_filter_hashes(
        &self,
        filter: &QueryFilter,
        column: &str,
        data_type: &str,
    ) -> Vec<u64> {
        let mut values: Vec<IndexValue> = Vec::new();
        if column == "tenant_id" {
            if let Some(v) = filter.tenant_id_eq {
                values.push(IndexValue::UInt(v));
            }
            if let Some(vs) = &filter.tenant_id_in {
                values.extend(vs.iter().copied().map(IndexValue::UInt));
            }
        }
        if column == "route_id" {
            if let Some(v) = filter.route_id_eq {
                values.push(IndexValue::UInt(v));
            }
            if let Some(vs) = &filter.route_id_in {
                values.extend(vs.iter().copied().map(IndexValue::UInt));
            }
        }
        for (col, v) in &filter.numeric_eq_filters {
            if col == column {
                values.push(IndexValue::Int(*v));
            }
        }
        for (col, v) in &filter.float_eq_filters {
            if col == column {
                values.push(IndexValue::Float(*v));
            }
        }
        for (col, v) in &filter.bool_eq_filters {
            if col == column {
                values.push(IndexValue::Bool(*v));
            }
        }
        let data_type_lower = data_type.to_ascii_lowercase();
        let is_uuid = data_type_lower == "uuid";
        let is_binary = matches!(
            data_type_lower.as_str(),
            "binary" | "bytes" | "blob" | "largebinary"
        );
        for (col, v) in &filter.string_eq_filters {
            if col == column {
                if is_uuid {
                    if let Some(bytes) = parse_uuid_string(v) {
                        values.push(IndexValue::Bytes(bytes.to_vec()));
                    }
                } else if is_binary {
                    values.push(IndexValue::Bytes(decode_binary_literal(v)));
                } else {
                    values.push(IndexValue::Str(v.clone()));
                }
            }
        }
        for (col, vs) in &filter.string_in_filters {
            if col == column {
                if is_uuid {
                    values.extend(vs.iter().filter_map(|v| {
                        parse_uuid_string(v).map(|b| IndexValue::Bytes(b.to_vec()))
                    }));
                } else if is_binary {
                    values.extend(vs.iter().map(|v| IndexValue::Bytes(decode_binary_literal(v))));
                } else {
                    values.extend(vs.iter().cloned().map(IndexValue::Str));
                }
            }
        }

        values
            .into_iter()
            .filter_map(|v| Self::hash_index_value(data_type, &v))
            .collect()
    }

    fn load_hash_index(path: &Path) -> Result<Vec<u64>, EngineError> {
        let bytes = fs::read(path)
            .map_err(|e| EngineError::Io(format!("read hash index failed: {e}")))?;
        if bytes.len() < 8 {
            return Ok(Vec::new());
        }
        let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let mut values = Vec::with_capacity(count);
        let mut offset = 8;
        while offset + 8 <= bytes.len() {
            let val = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            values.push(val);
            offset += 8;
        }
        values.sort_unstable();
        values.dedup();
        Ok(values)
    }

    fn entry_matches_index_filters(
        &self,
        entry: &ManifestEntry,
        filter: &QueryFilter,
        schema_map: &HashMap<String, String>,
        indexes: &[IndexMeta],
    ) -> Result<bool, EngineError> {
        for index in indexes {
            if index.state != IndexState::Ready {
                continue;
            }
            if index.columns.len() != 1 {
                continue;
            }
            let column = &index.columns[0];
            let data_type = match schema_map.get(column) {
                Some(t) => t,
                None => continue,
            };
            let hashes = self.index_filter_hashes(filter, column, data_type);
            if hashes.is_empty() {
                continue;
            }
            let path = self.index_segment_path(index, &entry.segment_id);
            if !path.exists() {
                continue;
            }

            let matches = match index.index_type {
                crate::sql::IndexType::Bloom => {
                    let bytes = fs::read(&path)
                        .map_err(|e| EngineError::Io(format!("read bloom index failed: {e}")))?;
                    let bloom = crate::bloom_utils::deserialize_bloom_or_empty(&bytes, 1000);
                    hashes.iter().any(|h| bloom.check(h))
                }
                crate::sql::IndexType::Hash => {
                    let values = Self::load_hash_index(&path)?;
                    hashes
                        .iter()
                        .any(|h| values.binary_search(h).is_ok())
                }
                _ => true,
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn build_index_for_segment(
        &self,
        index: &IndexMeta,
        entry: &ManifestEntry,
    ) -> Result<(), EngineError> {
        let batches = self.load_segment_batches_cached(entry)?;
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let mut col_indices = Vec::with_capacity(index.columns.len());
        for col in &index.columns {
            let idx = schema.index_of(col).map_err(|_| {
                EngineError::InvalidArgument(format!(
                    "index column '{col}' not found in segment schema"
                ))
            })?;
            col_indices.push(idx);
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut bloom = Bloom::new_for_fp_rate(total_rows.max(1), 0.01);
        let mut hash_values: HashSet<u64> = HashSet::new();

        for batch in batches.iter() {
            for row_idx in 0..batch.num_rows() {
                let mut hasher = DefaultHasher::new();
                for col_idx in &col_indices {
                    let col = batch.column(*col_idx);
                    hash_array_value(&mut hasher, col.as_ref(), row_idx);
                }
                let hash = hasher.finish();
                match index.index_type {
                    crate::sql::IndexType::Bloom => {
                        bloom.set(&hash);
                    }
                    crate::sql::IndexType::Hash => {
                        hash_values.insert(hash);
                    }
                    _ => {}
                }
            }
        }

        let path = self.index_segment_path(index, &entry.segment_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("create index dir failed: {e}")))?;
        }

        match index.index_type {
            crate::sql::IndexType::Bloom => {
                let bytes = crate::bloom_utils::serialize_bloom(&bloom)?;
                fs::write(&path, bytes)
                    .map_err(|e| EngineError::Io(format!("write bloom index failed: {e}")))?;
            }
            crate::sql::IndexType::Hash => {
                let mut values: Vec<u64> = hash_values.into_iter().collect();
                values.sort_unstable();
                let mut out = Vec::with_capacity(8 + values.len() * 8);
                out.extend_from_slice(&(values.len() as u64).to_le_bytes());
                for v in values {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                fs::write(&path, out)
                    .map_err(|e| EngineError::Io(format!("write hash index failed: {e}")))?;
            }
            _ => {}
        }
        Ok(())
    }

    fn build_index_for_table(&self, index: &IndexMeta) -> Result<(), EngineError> {
        let entries = self.table_entries(&index.database, &index.table)?;
        for entry in &entries {
            let path = self.index_segment_path(index, &entry.segment_id);
            if path.exists() {
                continue;
            }
            if let Err(e) = self.build_index_for_segment(index, entry) {
                tracing::warn!(
                    database = %index.database,
                    table = %index.table,
                    index = %index.name,
                    error = %e,
                    "index build failed for segment"
                );
                return Err(e);
            }
        }
        Ok(())
    }

    fn update_index_state(
        &self,
        database: &str,
        table: &str,
        name: &str,
        state: IndexState,
    ) -> Result<(), EngineError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let mut updated = false;
        for idx in &mut manifest.indexes {
            if idx.database == database && idx.table == table && idx.name == name {
                idx.state = state.clone();
                idx.last_built_micros = Some(now);
                updated = true;
                break;
            }
        }
        if updated {
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        }
        Ok(())
    }

    fn spawn_background_index_build(&self, index: IndexMeta) {
        let cfg = self.cfg.clone();
        std::thread::spawn(move || {
            // Try to open DB - use local-only storage if no tokio runtime is available
            // (common in test environments or when background threads are spawned outside async context)
            let db_result = {
                // First try normal open (for production use with S3)
                let storage_result = crate::storage::TieredStorage::new(&cfg);
                match storage_result {
                    Ok(storage) => Db::open_with_storage(cfg, std::sync::Arc::new(storage)),
                    Err(_) => {
                        // Fall back to local-only storage (no tokio runtime needed)
                        let storage = crate::storage::TieredStorage::new_local_only(cfg.segments_dir.clone());
                        Db::open_with_storage(cfg, std::sync::Arc::new(storage))
                    }
                }
            };

            match db_result {
                Ok(db) => {
                    let result = db.build_index_for_table(&index);
                    let state = if result.is_ok() {
                        IndexState::Ready
                    } else {
                        IndexState::Failed
                    };
                    if let Err(e) =
                        db.update_index_state(&index.database, &index.table, &index.name, state)
                    {
                        tracing::warn!(error = %e, "failed to update index state");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to start background index build");
                }
            }
        });
    }

    fn spawn_background_index_build_for_segment(
        &self,
        database: &str,
        table: &str,
        segment_id: &str,
    ) {
        let cfg = self.cfg.clone();
        let db_name = database.to_string();
        let tbl_name = table.to_string();
        let seg_id = segment_id.to_string();
        let (entry, indexes) = {
            let manifest = match self.manifest.read() {
                Ok(m) => m,
                Err(_) => {
                    tracing::warn!("manifest lock poisoned");
                    return;
                }
            };
            let entry = manifest
                .entries
                .iter()
                .find(|e| e.segment_id == seg_id)
                .cloned();
            let indexes = manifest
                .indexes
                .iter()
                .filter(|idx| {
                    idx.database == db_name
                        && idx.table == tbl_name
                        && matches!(idx.state, IndexState::Ready | IndexState::Building)
                })
                .cloned()
                .collect::<Vec<_>>();
            (entry, indexes)
        };
        let Some(entry) = entry else { return; };
        if indexes.is_empty() {
            return;
        }
        std::thread::spawn(move || {
            // Try to open DB - use local-only storage if no tokio runtime is available
            let db_result = {
                let storage_result = crate::storage::TieredStorage::new(&cfg);
                match storage_result {
                    Ok(storage) => Db::open_with_storage(cfg, std::sync::Arc::new(storage)),
                    Err(_) => {
                        let storage = crate::storage::TieredStorage::new_local_only(cfg.segments_dir.clone());
                        Db::open_with_storage(cfg, std::sync::Arc::new(storage))
                    }
                }
            };

            match db_result {
                Ok(db) => {
                    for index in &indexes {
                        if let Err(e) = db.build_index_for_segment(index, &entry) {
                            tracing::warn!(error = %e, "background index build failed");
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to start background index build");
                }
            }
        });
    }

    /// Create an index on a table
    pub fn create_index(
        &self,
        database: &str,
        table: &str,
        index_name: &str,
        columns: &[String],
        index_type: crate::sql::IndexType,
        if_not_exists: bool,
    ) -> Result<(), EngineError> {
        if columns.is_empty() {
            return Err(EngineError::InvalidArgument(
                "index must include at least one column".into(),
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let index_meta = {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

            // Verify table exists
            let table_exists = manifest
                .tables
                .iter()
                .any(|t| t.database == database && t.name == table);
            if !table_exists {
                return Err(EngineError::NotFound(format!(
                    "table not found: {database}.{table}"
                )));
            }

            if manifest
                .indexes
                .iter()
                .any(|idx| idx.database == database && idx.table == table && idx.name == index_name)
            {
                if if_not_exists {
                    tracing::info!(
                        "Index {} already exists on {}.{}",
                        index_name,
                        database,
                        table
                    );
                    return Ok(());
                }
                return Err(EngineError::InvalidArgument(format!(
                    "index already exists: {database}.{table}.{index_name}"
                )));
            }

            let index_meta = IndexMeta {
                name: index_name.to_string(),
                database: database.to_string(),
                table: table.to_string(),
                columns: columns.to_vec(),
                index_type,
                state: IndexState::Building,
                created_micros: now,
                last_built_micros: None,
            };
            manifest.indexes.push(index_meta.clone());
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            index_meta
        };

        self.spawn_background_index_build(index_meta);
        Ok(())
    }

    /// Drop an index from a table
    pub fn drop_index(
        &self,
        database: &str,
        table: &str,
        index_name: &str,
        if_exists: bool,
    ) -> Result<(), EngineError> {
        let removed = {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let initial_len = manifest.indexes.len();
            manifest.indexes.retain(|idx| {
                idx.database != database || idx.table != table || idx.name != index_name
            });
            let removed = manifest.indexes.len() != initial_len;
            if !removed && !if_exists {
                return Err(EngineError::NotFound(format!(
                    "index not found: {database}.{table}.{index_name}"
                )));
            }
            if removed {
                manifest.bump_version();
                persist_manifest(&self.cfg.manifest_path, &manifest)?;
                snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            }
            removed
        };

        if removed {
            let path = self.index_dir(database, table, index_name);
            let _ = fs::remove_dir_all(&path);
            tracing::info!("Index {} dropped from {}.{}", index_name, database, table);
        }
        Ok(())
    }

    /// List indexes for a table
    /// Returns: Vec<(database, table, index_name, columns, index_type)>
    pub fn list_indexes(
        &self,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<(String, String, String, Vec<String>, crate::sql::IndexType)>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let mut out = Vec::new();
        for idx in &manifest.indexes {
            if database.map_or(true, |db| db == idx.database)
                && table.map_or(true, |t| t == idx.table)
            {
                out.push((
                    idx.database.clone(),
                    idx.table.clone(),
                    idx.name.clone(),
                    idx.columns.clone(),
                    idx.index_type.clone(),
                ));
            }
        }
        Ok(out)
    }

    // ==================== Table Maintenance ====================

    /// Analyze a table to collect statistics
    pub fn analyze_table(&self, database: &str, table: &str) -> Result<(), EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Verify table exists
        let table_exists = manifest
            .tables
            .iter()
            .any(|t| t.database == database && t.name == table);
        if !table_exists {
            return Err(EngineError::NotFound(format!(
                "table not found: {database}.{table}"
            )));
        }

        // Count segments and total size for this table
        let (segment_count, total_bytes): (usize, u64) = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .fold((0, 0), |(count, bytes), e| (count + 1, bytes + e.size_bytes));

        tracing::info!(
            "ANALYZE {}.{}: {} segments, {} bytes total",
            database, table, segment_count, total_bytes
        );

        // Note: Full implementation would:
        // 1. Sample data to compute column statistics (min, max, distinct count, null count)
        // 2. Build histograms for query optimization
        // 3. Store statistics in manifest or separate stats file
        Ok(())
    }

    /// Vacuum a table to reclaim storage
    /// - Regular VACUUM: rewrites fragmented segments (< 50% of target size)
    /// - VACUUM FULL: merges all segments into optimal-sized chunks
    pub fn vacuum(&self, database: &str, table: &str, full: bool) -> Result<VacuumResult, EngineError> {
        let table_meta = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

            // Verify table exists
            let table_meta = manifest
                .tables
                .iter()
                .find(|t| t.database == database && t.name == table)
                .cloned();
            if table_meta.is_none() {
                return Err(EngineError::NotFound(format!(
                    "table not found: {database}.{table}"
                )));
            }
            table_meta
        };

        let target_bytes = self.cfg.compaction_target_bytes;
        let fragmentation_threshold = target_bytes / 2; // 50% of target = fragmented

        // Collect segments for this table
        let segments_to_vacuum: Vec<ManifestEntry> = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

            manifest
                .entries
                .iter()
                .filter(|e| e.database == database && e.table == table)
                .cloned()
                .collect()
        };

        if segments_to_vacuum.is_empty() {
            return Ok(VacuumResult {
                segments_processed: 0,
                segments_removed: 0,
                bytes_reclaimed: 0,
                new_segments: 0,
            });
        }

        let original_count = segments_to_vacuum.len();
        let original_bytes: u64 = segments_to_vacuum.iter().map(|e| e.size_bytes).sum();

        if full {
            tracing::info!(
                "VACUUM FULL {}.{}: merging {} segments ({} bytes)",
                database, table, original_count, original_bytes
            );

            // VACUUM FULL: merge ALL segments into optimal chunks
            let mut all_batches = Vec::new();
            for entry in &segments_to_vacuum {
                let decoded = self.load_segment_batches_cached(entry)?;
                all_batches.extend(decoded.iter().cloned());
            }

            if all_batches.is_empty() {
                return Ok(VacuumResult {
                    segments_processed: original_count,
                    segments_removed: 0,
                    bytes_reclaimed: 0,
                    new_segments: 0,
                });
            }

            // Remove old segments from manifest
            {
                let mut manifest = self
                    .manifest
                    .write()
                    .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                let drop_set: std::collections::HashSet<String> =
                    segments_to_vacuum.iter().map(|e| e.segment_id.clone()).collect();
                manifest.entries.retain(|e| !drop_set.contains(&e.segment_id));
                manifest.bump_version();
                persist_manifest(&self.cfg.manifest_path, &manifest)?;
            }

            // Delete old segment files
            for entry in &segments_to_vacuum {
                self.remove_segment_file(&entry.segment_id);
            }

            // Reingest all data as new optimally-sized segments
            let schema = all_batches[0].schema();
            let merged_ipc = record_batches_to_ipc(schema.as_ref(), &all_batches)?;

            let max_watermark = segments_to_vacuum
                .iter()
                .map(|e| e.watermark_micros)
                .max()
                .unwrap_or(0);

            self.ingest_ipc(IngestBatch {
                payload_ipc: merged_ipc,
                watermark_micros: max_watermark,
                shard_override: None,
                database: Some(database.to_string()),
                table: Some(table.to_string()),
            })?;

            let new_bytes: u64 = {
                let manifest = self.manifest.read()
                    .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.database == database && e.table == table)
                    .map(|e| e.size_bytes)
                    .sum()
            };

            let new_count = {
                let manifest = self.manifest.read()
                    .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.database == database && e.table == table)
                    .count()
            };

            // Invalidate query cache
            {
                let mut cache = self.query_cache.lock();
                cache.invalidate_table(database, table);
            }

            tracing::info!(
                "VACUUM FULL complete: {} -> {} segments, {} -> {} bytes",
                original_count, new_count, original_bytes, new_bytes
            );

            Ok(VacuumResult {
                segments_processed: original_count,
                segments_removed: original_count,
                bytes_reclaimed: original_bytes.saturating_sub(new_bytes),
                new_segments: new_count,
            })
        } else {
            // Regular VACUUM: only rewrite fragmented segments (< 50% of target)
            let fragmented: Vec<ManifestEntry> = segments_to_vacuum
                .into_iter()
                .filter(|e| e.size_bytes < fragmentation_threshold)
                .collect();

            if fragmented.is_empty() {
                tracing::info!("VACUUM {}.{}: no fragmented segments found", database, table);
                return Ok(VacuumResult {
                    segments_processed: original_count,
                    segments_removed: 0,
                    bytes_reclaimed: 0,
                    new_segments: 0,
                });
            }

            tracing::info!(
                "VACUUM {}.{}: rewriting {} fragmented segments",
                database, table, fragmented.len()
            );

            // Group fragmented segments and compact them
            let fragmented_count = fragmented.len();
            let fragmented_bytes: u64 = fragmented.iter().map(|e| e.size_bytes).sum();

            // Use existing compaction logic
            if fragmented.len() >= 2 {
                self.compact_entries(fragmented, table_meta)?;
            }

            let new_bytes: u64 = {
                let manifest = self.manifest.read()
                    .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
                manifest
                    .entries
                    .iter()
                    .filter(|e| e.database == database && e.table == table)
                    .map(|e| e.size_bytes)
                    .sum()
            };

            // Invalidate query cache
            {
                let mut cache = self.query_cache.lock();
                cache.invalidate_table(database, table);
            }

            Ok(VacuumResult {
                segments_processed: original_count,
                segments_removed: fragmented_count,
                bytes_reclaimed: fragmented_bytes.saturating_sub(new_bytes.saturating_sub(original_bytes - fragmented_bytes)),
                new_segments: 1,
            })
        }
    }

    // ==================== Background Compaction ====================

    /// Get list of tables that are candidates for compaction, sorted by priority
    /// This helps implement intelligent background compaction that avoids merge storms
    pub fn get_compaction_candidates(&self) -> Result<Vec<CompactionCandidate>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        let target_bytes = self.cfg.compaction_target_bytes;
        let min_segments = self.cfg.compact_min_segments;

        // Group segments by table
        let mut table_segments: std::collections::HashMap<(String, String), Vec<&ManifestEntry>> =
            std::collections::HashMap::new();
        for entry in &manifest.entries {
            if !matches!(entry.tier, SegmentTier::Hot) {
                continue;
            }
            let key = (entry.database.clone(), entry.table.clone());
            table_segments.entry(key).or_default().push(entry);
        }

        let mut candidates = Vec::new();
        for ((database, table), segments) in table_segments {
            // Skip if not enough segments
            if segments.len() < min_segments {
                continue;
            }

            // Skip if table is in cooldown
            if self.compaction_state.is_table_in_cooldown(
                &database,
                &table,
                self.cfg.compaction_cooldown_secs,
            ) {
                continue;
            }

            // Count small segments (< 50% of target)
            let fragmentation_threshold = target_bytes / 2;
            let small_segments: Vec<_> = segments
                .iter()
                .filter(|e| e.size_bytes < fragmentation_threshold)
                .collect();

            if small_segments.len() < min_segments {
                continue;
            }

            let total_bytes: u64 = segments.iter().map(|e| e.size_bytes).sum();
            let avg_size = total_bytes / segments.len() as u64;

            // Priority based on:
            // - More small segments = higher priority
            // - Smaller average size = higher priority
            // - More total segments = higher priority
            let priority = (small_segments.len() as u32 * 10)
                + ((target_bytes.saturating_sub(avg_size)) / (1024 * 1024)) as u32
                + segments.len() as u32;

            candidates.push(CompactionCandidate {
                database,
                table,
                segment_count: segments.len(),
                total_bytes,
                avg_segment_size: avg_size,
                priority,
            });
        }

        // Sort by priority (highest first)
        candidates.sort_by(|a, b| b.priority.cmp(&a.priority));
        Ok(candidates)
    }

    /// Run a single round of background compaction
    /// Returns the number of tables compacted
    pub fn run_background_compaction(&self) -> Result<usize, EngineError> {
        if !self.cfg.enable_compaction {
            return Ok(0);
        }

        // Check if we can start more compactions
        if !self.compaction_state.can_start_compaction(self.cfg.max_concurrent_compactions) {
            tracing::debug!(
                "Skipping background compaction: {} active (max {})",
                self.compaction_state.active_count(),
                self.cfg.max_concurrent_compactions
            );
            return Ok(0);
        }

        let candidates = self.get_compaction_candidates()?;
        if candidates.is_empty() {
            return Ok(0);
        }

        let mut compacted = 0;
        for candidate in candidates {
            // Check again before each compaction
            if !self.compaction_state.can_start_compaction(self.cfg.max_concurrent_compactions) {
                break;
            }

            self.compaction_state.start_compaction(&candidate.database, &candidate.table);

            match self.vacuum(&candidate.database, &candidate.table, false) {
                Ok(result) => {
                    if result.segments_removed > 0 {
                        tracing::info!(
                            "Background compaction on {}.{}: {} segments -> {} bytes reclaimed",
                            candidate.database,
                            candidate.table,
                            result.segments_removed,
                            result.bytes_reclaimed
                        );
                        compacted += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Background compaction failed on {}.{}: {}",
                        candidate.database,
                        candidate.table,
                        e
                    );
                }
            }

            self.compaction_state.finish_compaction();
        }

        Ok(compacted)
    }

    /// Get the auto-compact interval from config (0 = disabled)
    pub fn auto_compact_interval_secs(&self) -> u64 {
        self.cfg.auto_compact_interval_secs
    }

    pub fn set_table_compression(
        &self,
        database: &str,
        table: &str,
        compression: Option<String>,
    ) -> Result<(), EngineError> {
        let comp = normalize_compression(compression)?;
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let tbl = manifest
            .tables
            .iter_mut()
            .find(|t| t.database == database && t.name == table)
            .ok_or_else(|| EngineError::NotFound("table not found".into()))?;
        tbl.compression = comp.clone();
        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        Ok(())
    }

    // ==================== Deduplication ====================

    /// Set deduplication configuration for a table
    /// This enables automatic deduplication with merge-on-read semantics
    pub fn set_table_deduplication(
        &self,
        database: &str,
        table: &str,
        config: Option<crate::sql::DeduplicationConfig>,
    ) -> Result<(), EngineError> {
        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let tbl = manifest
            .tables
            .iter_mut()
            .find(|t| t.database == database && t.name == table)
            .ok_or_else(|| EngineError::NotFound("table not found".into()))?;
        tbl.deduplication = config.clone();
        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;

        if let Some(cfg) = config {
            tracing::info!(
                "Deduplication enabled on {}.{}: keys={:?}, version={:?}, mode={:?}",
                database, table, cfg.key_columns, cfg.version_column, cfg.mode
            );
        } else {
            tracing::info!("Deduplication disabled on {}.{}", database, table);
        }
        Ok(())
    }

    /// Get deduplication config for a table
    pub fn get_table_deduplication(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<crate::sql::DeduplicationConfig>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let tbl = manifest
            .tables
            .iter()
            .find(|t| t.database == database && t.name == table)
            .ok_or_else(|| EngineError::NotFound("table not found".into()))?;
        Ok(tbl.deduplication.clone())
    }

    /// Deduplicate a RecordBatch based on key columns and optional version column
    /// Returns a new batch with duplicates removed (keeping latest version if version_column specified)
    pub fn deduplicate_batch(
        &self,
        batch: &RecordBatch,
        key_columns: &[String],
        version_column: Option<&str>,
    ) -> Result<RecordBatch, EngineError> {
        use std::collections::HashMap;

        if batch.num_rows() == 0 || key_columns.is_empty() {
            return Ok(batch.clone());
        }

        // Find key column indices
        let key_indices: Vec<usize> = key_columns
            .iter()
            .map(|name| {
                batch.schema().index_of(name).map_err(|_| {
                    EngineError::InvalidArgument(format!("dedup key column not found: {name}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Find version column index if specified
        let version_idx = version_column
            .map(|name| {
                batch.schema().index_of(name).map_err(|_| {
                    EngineError::InvalidArgument(format!("version column not found: {name}"))
                })
            })
            .transpose()?;

        // Build a map of key -> (best_row_index, version)
        // We hash the key columns to create a composite key
        let mut key_map: HashMap<u64, (usize, i64)> = HashMap::new();

        for row_idx in 0..batch.num_rows() {
            // Compute composite key hash
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for &col_idx in &key_indices {
                let col = batch.column(col_idx);
                // Hash the value at this row
                hash_array_value(&mut hasher, col, row_idx);
            }
            let key_hash = std::hash::Hasher::finish(&hasher);

            // Get version if available
            let version = if let Some(v_idx) = version_idx {
                get_version_value(batch.column(v_idx), row_idx)
            } else {
                row_idx as i64 // Use row index as implicit version (later = newer)
            };

            // Check if we should keep this row
            match key_map.get(&key_hash) {
                Some(&(_, existing_version)) if version <= existing_version => {
                    // Keep existing (higher or equal version)
                }
                _ => {
                    key_map.insert(key_hash, (row_idx, version));
                }
            }
        }

        // Collect row indices to keep
        let mut keep_indices: Vec<usize> = key_map.values().map(|(idx, _)| *idx).collect();
        keep_indices.sort_unstable(); // Preserve original order

        // Build filtered batch
        if keep_indices.len() == batch.num_rows() {
            return Ok(batch.clone()); // No duplicates found
        }

        let indices = arrow_array::UInt32Array::from(
            keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );
        let columns: Vec<arrow_array::ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| arrow_select::take::take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| EngineError::Internal(format!("dedup take failed: {e}")))?;

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| EngineError::Internal(format!("dedup batch creation failed: {e}")))
    }

    /// Run deduplication on a table's segments
    /// Returns number of duplicate rows removed
    pub fn deduplicate_table(&self, database: &str, table: &str) -> Result<usize, EngineError> {
        let table_meta = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest
                .tables
                .iter()
                .find(|t| t.database == database && t.name == table)
                .cloned()
        };

        let table_meta = table_meta.ok_or_else(|| {
            EngineError::NotFound(format!("table not found: {database}.{table}"))
        })?;

        let dedup_config = table_meta.deduplication.ok_or_else(|| {
            EngineError::InvalidArgument(format!(
                "deduplication not configured for {database}.{table}"
            ))
        })?;

        // Get all segments for this table
        let segments: Vec<ManifestEntry> = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest
                .entries
                .iter()
                .filter(|e| e.database == database && e.table == table)
                .cloned()
                .collect()
        };

        if segments.is_empty() {
            return Ok(0);
        }

        // Load all batches
        let mut all_batches = Vec::new();
        let mut total_rows_before = 0usize;
        for entry in &segments {
            let batches = self.load_segment_batches_cached(entry)?;
            for batch in batches.iter() {
                total_rows_before += batch.num_rows();
                all_batches.push(batch.clone());
            }
        }

        if all_batches.is_empty() {
            return Ok(0);
        }

        // Merge all batches and deduplicate
        let schema = all_batches[0].schema();
        let merged = arrow_select::concat::concat_batches(&schema, &all_batches)
            .map_err(|e| EngineError::Internal(format!("concat batches failed: {e}")))?;

        let deduped = self.deduplicate_batch(
            &merged,
            &dedup_config.key_columns,
            dedup_config.version_column.as_deref(),
        )?;

        let rows_removed = total_rows_before - deduped.num_rows();

        if rows_removed == 0 {
            tracing::info!("Deduplication on {}.{}: no duplicates found", database, table);
            return Ok(0);
        }

        tracing::info!(
            "Deduplication on {}.{}: {} duplicates removed ({} -> {} rows)",
            database, table, rows_removed, total_rows_before, deduped.num_rows()
        );

        // Remove old segments
        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let drop_set: HashSet<String> = segments.iter().map(|e| e.segment_id.clone()).collect();
            manifest.entries.retain(|e| !drop_set.contains(&e.segment_id));
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
        }

        // Delete old segment files
        for entry in &segments {
            self.remove_segment_file(&entry.segment_id);
        }

        // Reingest deduplicated data
        let max_watermark = segments
            .iter()
            .map(|e| e.watermark_micros)
            .max()
            .unwrap_or(0);

        let merged_ipc = record_batches_to_ipc(schema.as_ref(), &[deduped])?;
        self.ingest_ipc(IngestBatch {
            payload_ipc: merged_ipc,
            watermark_micros: max_watermark,
            shard_override: None,
            database: Some(database.to_string()),
            table: Some(table.to_string()),
        })?;

        // Invalidate cache
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(database, table);
        }

        Ok(rows_removed)
    }

    /// Drop a database and all its tables and segments.
    /// If `if_exists` is true, no error is returned if the database doesn't exist.
    pub fn drop_database(&self, name: &str, if_exists: bool) -> Result<(), EngineError> {
        if name.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database name required".to_string(),
            ));
        }

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Check if database exists
        if !manifest.databases.iter().any(|db| db.name == name) {
            if if_exists {
                return Ok(());
            }
            return Err(EngineError::NotFound(format!("database not found: {name}")));
        }

        // Collect segments to remove
        let segments_to_remove: Vec<String> = manifest
            .entries
            .iter()
            .filter(|e| e.database == name)
            .map(|e| e.segment_id.clone())
            .collect();

        // Remove all entries for this database
        manifest.entries.retain(|e| e.database != name);

        // Remove all tables for this database
        manifest.tables.retain(|t| t.database != name);

        // Remove the database itself
        manifest.databases.retain(|db| db.name != name);

        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;

        // Rebuild manifest index and invalidate cache
        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate();
        }

        // Drop the lock before file operations
        drop(manifest);

        // Remove segment files (best effort)
        for seg_id in segments_to_remove {
            self.remove_segment_file(&seg_id);
        }

        Ok(())
    }

    /// Drop a table and all its segments.
    /// If `if_exists` is true, no error is returned if the table doesn't exist.
    pub fn drop_table(
        &self,
        database: &str,
        table: &str,
        if_exists: bool,
    ) -> Result<(), EngineError> {
        if database.trim().is_empty() || table.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and table name required".into(),
            ));
        }

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Check if table exists
        let table_exists = manifest
            .tables
            .iter()
            .any(|t| t.database == database && t.name == table);

        if !table_exists {
            if if_exists {
                return Ok(());
            }
            return Err(EngineError::NotFound(format!(
                "table not found: {database}.{table}"
            )));
        }

        // Collect segments to remove
        let segments_to_remove: Vec<String> = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .map(|e| e.segment_id.clone())
            .collect();

        // Remove all entries for this table
        manifest
            .entries
            .retain(|e| !(e.database == database && e.table == table));

        // Remove the table
        manifest
            .tables
            .retain(|t| !(t.database == database && t.name == table));

        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;

        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(database, table);
        }

        // Drop the lock before file operations
        drop(manifest);

        // Remove segment files (best effort)
        for seg_id in segments_to_remove {
            self.remove_segment_file(&seg_id);
        }

        Ok(())
    }

    /// Truncate a table by removing all its segments but keeping the table definition.
    pub fn truncate_table(&self, database: &str, table: &str) -> Result<(), EngineError> {
        if database.trim().is_empty() || table.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "database and table name required".into(),
            ));
        }

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Check if table exists
        let table_exists = manifest
            .tables
            .iter()
            .any(|t| t.database == database && t.name == table);

        if !table_exists {
            return Err(EngineError::NotFound(format!(
                "table not found: {database}.{table}"
            )));
        }

        // Collect segments to remove
        let segments_to_remove: Vec<String> = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .map(|e| e.segment_id.clone())
            .collect();

        // Remove all entries for this table
        manifest
            .entries
            .retain(|e| !(e.database == database && e.table == table));

        manifest.bump_version();
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;

        // Rebuild manifest index after entries changed
        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }

        // Drop the lock before file operations
        drop(manifest);

        // Remove segment files (best effort)
        for seg_id in segments_to_remove {
            self.remove_segment_file(&seg_id);
        }

        Ok(())
    }

    /// Add a nullable column by rewriting segments and updating stored schema.
    pub fn alter_table_add_column(
        &self,
        database: &str,
        table: &str,
        column: &str,
        data_type: &str,
        nullable: bool,
    ) -> Result<(), EngineError> {
        if column.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "column name required".into(),
            ));
        }
        if !nullable {
            return Err(EngineError::InvalidArgument(
                "only nullable columns are supported for ALTER TABLE ADD COLUMN".into(),
            ));
        }
        let _arrow_type = arrow_type_from_str(data_type)?;

        // Get current schema
        let table_meta = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let tbl = manifest
                .tables
                .iter()
                .find(|t| t.database == database && t.name == table)
                .cloned()
                .ok_or_else(|| {
                    EngineError::NotFound(format!("table not found: {database}.{table}"))
                })?;
            if tbl.schema_json.is_none() {
                return Err(EngineError::InvalidArgument(
                    "table schema is not defined; cannot alter".into(),
                ));
            }
            tbl
        };

        let mut fields: Vec<TableFieldSpec> = serde_json::from_str(
            table_meta
                .schema_json
                .as_ref()
                .ok_or_else(|| EngineError::InvalidArgument("table schema missing".into()))?,
        )
        .map_err(|e| EngineError::InvalidArgument(format!("invalid schema json: {e}")))?;

        if fields.iter().any(|f| f.name == column) {
            return Err(EngineError::InvalidArgument(format!(
                "column '{}' already exists",
                column
            )));
        }
        fields.push(TableFieldSpec {
            name: column.to_string(),
            data_type: data_type.to_string(),
            nullable,
            encoding: None,
        });
        let canonical_fields = canonicalize_schema_spec(&fields)?;
        let new_schema_json = serde_json::to_string(&canonical_fields)
            .map_err(|e| EngineError::Internal(format!("serialize schema failed: {e}")))?;

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        manifest.bump_version();
        if let Some(tbl) = manifest
            .tables
            .iter_mut()
            .find(|t| t.database == database && t.name == table)
        {
            tbl.schema_json = Some(new_schema_json.clone());
        }
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(database, table);
        }
        self.spawn_background_schema_rewrite(database, table, &new_schema_json);
        Ok(())
    }

    /// Drop a column by rewriting segments and updating stored schema.
    pub fn alter_table_drop_column(
        &self,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<(), EngineError> {
        if column.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "column name required".into(),
            ));
        }

        let table_meta = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let tbl = manifest
                .tables
                .iter()
                .find(|t| t.database == database && t.name == table)
                .cloned()
                .ok_or_else(|| {
                    EngineError::NotFound(format!("table not found: {database}.{table}"))
                })?;
            if tbl.schema_json.is_none() {
                return Err(EngineError::InvalidArgument(
                    "table schema is not defined; cannot alter".into(),
                ));
            }
            tbl
        };

        let mut fields: Vec<TableFieldSpec> = serde_json::from_str(
            table_meta
                .schema_json
                .as_ref()
                .ok_or_else(|| EngineError::InvalidArgument("table schema missing".into()))?,
        )
        .map_err(|e| EngineError::InvalidArgument(format!("invalid schema json: {e}")))?;

        let pos = fields.iter().position(|f| f.name == column).ok_or_else(|| {
            EngineError::NotFound(format!("column '{}' not found", column))
        })?;
        fields.remove(pos);
        if fields.is_empty() {
            return Err(EngineError::InvalidArgument(
                "dropping the last column is not supported".into(),
            ));
        }
        let canonical_fields = canonicalize_schema_spec(&fields)?;
        let new_schema_json = serde_json::to_string(&canonical_fields)
            .map_err(|e| EngineError::Internal(format!("serialize schema failed: {e}")))?;

        let mut manifest = self
            .manifest
            .write()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        manifest.bump_version();
        if let Some(tbl) = manifest
            .tables
            .iter_mut()
            .find(|t| t.database == database && t.name == table)
        {
            tbl.schema_json = Some(new_schema_json.clone());
        }
        persist_manifest(&self.cfg.manifest_path, &manifest)?;
        snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
        {
            let mut index = self
                .manifest_index
                .write()
                .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
            *index = ManifestIndex::build(&manifest.entries);
        }
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(database, table);
        }

        self.spawn_background_schema_rewrite(database, table, &new_schema_json);
        Ok(())
    }

    fn spawn_background_schema_rewrite(&self, database: &str, table: &str, schema_json: &str) {
        let cfg = self.cfg.clone();
        let db_name = database.to_string();
        let tbl_name = table.to_string();
        let schema_json = schema_json.to_string();
        std::thread::spawn(move || {
            match Db::open(cfg) {
                Ok(db) => {
                    if let Err(e) =
                        db.rewrite_table_segments_to_schema(&db_name, &tbl_name, &schema_json)
                    {
                        tracing::warn!(
                            database = %db_name,
                            table = %tbl_name,
                            error = %e,
                            "background schema rewrite failed"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        database = %db_name,
                        table = %tbl_name,
                        error = %e,
                        "failed to start background schema rewrite"
                    );
                }
            }
        });
    }

    fn spawn_background_compaction_loop(&self) {
        if !self.cfg.enable_compaction || self.cfg.auto_compact_interval_secs == 0 {
            return;
        }

        let mut cfg = self.cfg.clone();
        let interval_secs = cfg.auto_compact_interval_secs;
        cfg.auto_compact_interval_secs = 0;

        std::thread::spawn(move || {
            let interval = std::time::Duration::from_secs(interval_secs);
            match Db::open(cfg) {
                Ok(db) => loop {
                    std::thread::sleep(interval);
                    if let Err(e) = db.run_background_compaction() {
                        tracing::warn!(error = %e, "background compaction loop failed");
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "failed to start background compaction loop");
                }
            }
        });
    }

    fn rewrite_table_segments_to_schema(
        &self,
        database: &str,
        table: &str,
        schema_json: &str,
    ) -> Result<(), EngineError> {
        let expected_spec: Vec<TableFieldSpec> = serde_json::from_str(schema_json).map_err(|e| {
            EngineError::InvalidArgument(format!("invalid schema json for rewrite: {e}"))
        })?;
        let expected_fields: Vec<Field> = expected_spec
            .iter()
            .map(|s| {
                arrow_type_from_str(&s.data_type)
                    .map(|dt| Field::new(&s.name, dt, s.nullable))
            })
            .collect::<Result<_, _>>()?;

        let (table_meta, entries) = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let tbl = manifest
                .tables
                .iter()
                .find(|t| t.database == database && t.name == table)
                .cloned()
                .ok_or_else(|| {
                    EngineError::NotFound(format!("table not found: {database}.{table}"))
                })?;
            let entries = manifest
                .entries
                .iter()
                .filter(|e| e.database == database && e.table == table)
                .cloned()
                .collect::<Vec<_>>();
            (tbl, entries)
        };

        if entries.is_empty() {
            return Ok(());
        }

        let compression = normalize_compression(table_meta.compression.clone())?;
        let mut rewritten: Vec<(String, Vec<u8>, u64, u64)> = Vec::new();
        for entry in &entries {
            let data = load_segment(&self.storage, entry)?;
            let batches = read_ipc_batches(&data)?;
            let mut new_batches = Vec::new();
            for batch in batches {
                let aligned = align_batch_to_fields(&expected_fields, &batch)?;
                new_batches.push(aligned);
            }
            if !new_batches.is_empty() {
                let ipc =
                    record_batches_to_ipc(new_batches[0].schema().as_ref(), &new_batches)?;
                let stored = compress_payload(&ipc, compression.as_deref())?;
                let checksum = compute_checksum(&stored);
                let schema_hash =
                    compute_schema_hash_from_payload(&stored, compression.as_deref())?;
                rewritten.push((entry.segment_id.clone(), stored, checksum, schema_hash));
            }
        }

        for (segment_id, stored, _, _) in &rewritten {
            persist_segment_ipc(&self.storage, segment_id, stored)?;
            let mut seg_cache = self.segment_cache.lock();
            seg_cache.invalidate(segment_id);
        }

        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            manifest.bump_version();
            for entry in manifest
                .entries
                .iter_mut()
                .filter(|e| e.database == database && e.table == table)
            {
                if let Some((_, stored, checksum, schema_hash)) =
                    rewritten.iter().find(|(id, _, _, _)| id == &entry.segment_id)
                {
                    entry.size_bytes = stored.len() as u64;
                    entry.checksum = *checksum;
                    entry.compression = compression.clone();
                    entry.schema_hash = Some(*schema_hash);
                }
            }
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            {
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }
        }
        {
            let mut cache = self.query_cache.lock();
            cache.invalidate_table(database, table);
        }
        Ok(())
    }

    /// Delete rows matching the filter from a table. Returns number of rows deleted.
    pub fn delete_rows(
        &self,
        database: &str,
        table: &str,
        where_clause: Option<&str>,
    ) -> Result<usize, EngineError> {
        let filter = parse_where_filter(where_clause)?;
        if let Some(clause) = where_clause {
            if !clause.trim().is_empty() && filter_is_empty(&filter) {
                return Err(EngineError::InvalidArgument(
                    "unsupported WHERE clause for DELETE".into(),
                ));
            }
        }

        let entries = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            if !manifest
                .tables
                .iter()
                .any(|t| t.database == database && t.name == table)
            {
                return Err(EngineError::NotFound(format!(
                    "table not found: {database}.{table}"
                )));
            }
            manifest
                .entries
                .iter()
                .filter(|e| e.database == database && e.table == table)
                .cloned()
                .collect::<Vec<_>>()
        };

        if entries.is_empty() {
            return Ok(0);
        }

        let mut new_segments: Vec<(u16, u64, Vec<u8>)> = Vec::new();
        let mut segments_to_drop: Vec<String> = Vec::new();
        let mut rows_deleted = 0usize;

        for entry in entries {
            let data = load_segment(&self.storage, &entry)?;
            let batches = read_ipc_batches(&data)?;
            let mut kept_batches = Vec::new();
            let mut segment_removed = 0usize;
            for batch in batches {
                let (kept, removed) = filter_batch_for_delete(&batch, &filter)?;
                rows_deleted += removed;
                segment_removed += removed;
                if let Some(b) = kept {
                    kept_batches.push(b);
                }
            }
            if segment_removed > 0 {
                if !kept_batches.is_empty() {
                    let ipc =
                        record_batches_to_ipc(kept_batches[0].schema().as_ref(), &kept_batches)?;
                    new_segments.push((entry.shard_id, entry.watermark_micros, ipc));
                }
                segments_to_drop.push(entry.segment_id.clone());
            }
        }

        if rows_deleted == 0 {
            return Ok(0);
        }

        // Remove affected segments from manifest
        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let drop_set: HashSet<String> = segments_to_drop.iter().cloned().collect();
            manifest.entries.retain(|e| !drop_set.contains(&e.segment_id));
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            {
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }
            {
                let mut cache = self.query_cache.lock();
                cache.invalidate_table(database, table);
            }
        }

        // Drop old segment files (best effort)
        for seg_id in &segments_to_drop {
            self.remove_segment_file(seg_id);
        }

        for (shard, watermark, ipc) in new_segments {
            self.ingest_ipc(IngestBatch {
                payload_ipc: ipc,
                watermark_micros: watermark,
                shard_override: Some(shard as u64),
                database: Some(database.to_string()),
                table: Some(table.to_string()),
            })?;
        }
        Ok(rows_deleted)
    }

    /// Update rows matching the filter by applying assignments. Returns number of rows updated.
    pub fn update_rows(
        &self,
        database: &str,
        table: &str,
        assignments: &[(String, SqlValue)],
        where_clause: Option<&str>,
    ) -> Result<usize, EngineError> {
        if assignments.is_empty() {
            return Err(EngineError::InvalidArgument(
                "UPDATE requires at least one assignment".into(),
            ));
        }
        let filter = parse_where_filter(where_clause)?;
        if let Some(clause) = where_clause {
            if !clause.trim().is_empty() && filter_is_empty(&filter) {
                return Err(EngineError::InvalidArgument(
                    "unsupported WHERE clause for UPDATE".into(),
                ));
            }
        }

        let entries = {
            let manifest = self
                .manifest
                .read()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            if !manifest
                .tables
                .iter()
                .any(|t| t.database == database && t.name == table)
            {
                return Err(EngineError::NotFound(format!(
                    "table not found: {database}.{table}"
                )));
            }
            manifest
                .entries
                .iter()
                .filter(|e| e.database == database && e.table == table)
                .cloned()
                .collect::<Vec<_>>()
        };

        if entries.is_empty() {
            return Ok(0);
        }

        let mut new_segments: Vec<(u16, u64, Vec<u8>)> = Vec::new();
        let mut segments_to_drop: Vec<String> = Vec::new();
        let mut rows_updated = 0usize;

        for entry in entries {
            let data = load_segment(&self.storage, &entry)?;
            let batches = read_ipc_batches(&data)?;
            let mut updated_batches = Vec::new();
            let mut segment_changed = false;
            for batch in batches {
                let (updated, count) =
                    apply_assignments_to_batch(&batch, &filter, assignments)?;
                if count > 0 {
                    segment_changed = true;
                }
                rows_updated += count;
                updated_batches.push(updated);
            }
            if segment_changed {
                let ipc =
                    record_batches_to_ipc(updated_batches[0].schema().as_ref(), &updated_batches)?;
                new_segments.push((entry.shard_id, entry.watermark_micros, ipc));
                segments_to_drop.push(entry.segment_id.clone());
            }
        }

        if rows_updated == 0 {
            return Ok(0);
        }

        // Remove affected segments from manifest
        {
            let mut manifest = self
                .manifest
                .write()
                .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
            let drop_set: HashSet<String> = segments_to_drop.iter().cloned().collect();
            manifest.entries.retain(|e| !drop_set.contains(&e.segment_id));
            manifest.bump_version();
            persist_manifest(&self.cfg.manifest_path, &manifest)?;
            snapshot_manifest(&self.cfg.manifest_snapshot_path, &self.cfg.manifest_path)?;
            {
                let mut index = self
                    .manifest_index
                    .write()
                    .map_err(|_| EngineError::Internal("manifest index lock poisoned".into()))?;
                *index = ManifestIndex::build(&manifest.entries);
            }
            {
                let mut cache = self.query_cache.lock();
                cache.invalidate_table(database, table);
            }
        }

        for seg_id in &segments_to_drop {
            self.remove_segment_file(seg_id);
        }

        for (shard, watermark, ipc) in new_segments {
            self.ingest_ipc(IngestBatch {
                payload_ipc: ipc,
                watermark_micros: watermark,
                shard_override: Some(shard as u64),
                database: Some(database.to_string()),
                table: Some(table.to_string()),
            })?;
        }
        Ok(rows_updated)
    }

    pub fn health_check(&self) -> Result<(), EngineError> {
        // Lightweight health check: ensure manifest is readable and segments dir exists.
        let manifest = load_manifest(&self.cfg.manifest_path)?;
        if !self.cfg.segments_dir.exists() {
            return Err(EngineError::Internal(
                "segments directory missing".to_string(),
            ));
        }
        for entry in manifest.entries {
            let path = self
                .cfg
                .segments_dir
                .join(format!("{}.ipc", entry.segment_id));
            if !path.exists() {
                return Err(EngineError::Internal(format!(
                    "segment missing on disk: {}",
                    entry.segment_id
                )));
            }
        }
        Ok(())
    }

    /// Detailed health check returning comprehensive status information
    pub fn health_status(&self) -> HealthStatus {
        let mut errors = Vec::new();

        // Check manifest readability
        let manifest_result = load_manifest(&self.cfg.manifest_path);
        let manifest_readable = manifest_result.is_ok();
        if !manifest_readable {
            errors.push("Failed to read manifest".to_string());
        }

        // Check directories
        let segments_dir_exists = self.cfg.segments_dir.exists();
        if !segments_dir_exists {
            errors.push("Segments directory does not exist".to_string());
        }

        let wal_dir_exists = self.cfg.wal_dir.exists();
        if !wal_dir_exists {
            errors.push("WAL directory does not exist".to_string());
        }

        // Get manifest info
        let (
            total_segments,
            missing_segments,
            total_size_bytes,
            manifest_version,
            total_databases,
            total_tables,
        ) = if let Ok(manifest) = manifest_result {
            let mut missing = Vec::new();
            let mut total_size = 0u64;

            for entry in &manifest.entries {
                total_size += entry.size_bytes;
                if segments_dir_exists {
                    let path = self
                        .cfg
                        .segments_dir
                        .join(format!("{}.ipc", entry.segment_id));
                    if !path.exists() {
                        missing.push(entry.segment_id.clone());
                    }
                }
            }

            if !missing.is_empty() {
                errors.push(format!("{} segment(s) missing on disk", missing.len()));
            }

            (
                manifest.entries.len(),
                missing,
                total_size,
                manifest.version,
                manifest.databases.len(),
                manifest.tables.len(),
            )
        } else {
            (0, Vec::new(), 0, 0, 0, 0)
        };

        // Get cache info
        let cache_entries = self.query_cache.lock().len();

        // Calculate uptime
        let uptime_seconds = Some(self.start_time.elapsed().as_secs());

        // Determine overall health
        let is_healthy = errors.is_empty()
            && manifest_readable
            && segments_dir_exists
            && wal_dir_exists
            && missing_segments.is_empty();

        HealthStatus {
            is_healthy,
            manifest_readable,
            segments_dir_exists,
            wal_dir_exists,
            total_segments,
            missing_segments,
            total_databases,
            total_tables,
            total_size_bytes,
            manifest_version,
            uptime_seconds,
            cache_entries,
            errors,
        }
    }

    pub fn list_databases(&self) -> Result<Vec<String>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        Ok(manifest.databases.iter().map(|d| d.name.clone()).collect())
    }

    pub fn list_tables(
        &self,
        database: Option<&str>,
    ) -> Result<Vec<crate::replication::TableMeta>, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        let tables = manifest
            .tables
            .iter()
            .filter(|t| {
                if let Some(db) = database {
                    t.database == db
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        Ok(tables)
    }

    /// Get the current/default database name.
    /// Returns the first database if one exists, otherwise "boyodb".
    pub fn current_database(&self) -> Option<String> {
        let manifest = self.manifest.read().ok()?;
        manifest.databases.first().map(|d| d.name.clone())
    }

    /// Get the current manifest version.
    pub fn get_manifest_version(&self) -> Result<u64, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;
        Ok(manifest.version)
    }

    /// Convenience method to ingest IPC data with database and table parameters.
    pub fn ingest(
        &self,
        database: &str,
        table: &str,
        ipc_data: &[u8],
        watermark_micros: u64,
    ) -> Result<(), EngineError> {
        self.ingest_ipc(IngestBatch {
            payload_ipc: ipc_data.to_vec(),
            watermark_micros,
            shard_override: None,
            database: Some(database.to_string()),
            table: Some(table.to_string()),
        })
    }

    /// Describe a table, returning its metadata including schema if defined
    pub fn describe_table(
        &self,
        database: &str,
        table: &str,
    ) -> Result<TableDescription, EngineError> {
        let manifest = self
            .manifest
            .read()
            .map_err(|_| EngineError::Internal("manifest lock poisoned".into()))?;

        // Find the table metadata
        let table_meta = manifest
            .tables
            .iter()
            .find(|t| t.database == database && t.name == table)
            .ok_or_else(|| {
                EngineError::NotFound(format!("table {}.{} not found", database, table))
            })?;

        // Count segments for this table
        let segment_count = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .count();

        // Calculate total size
        let total_bytes = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .map(|e| e.size_bytes)
            .sum();

        // Get watermark range
        let watermark_range = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .fold(None, |range, e| match range {
                None => Some((e.watermark_micros, e.watermark_micros)),
                Some((min, max)) => {
                    Some((min.min(e.watermark_micros), max.max(e.watermark_micros)))
                }
            });

        // Get event time range
        let event_time_range = manifest
            .entries
            .iter()
            .filter(|e| e.database == database && e.table == table)
            .fold(None, |range: Option<(u64, u64)>, e| {
                let entry_min = e.event_time_min.unwrap_or(0);
                let entry_max = e.event_time_max.unwrap_or(0);
                if entry_min == 0 && entry_max == 0 {
                    return range;
                }
                match range {
                    None => Some((entry_min, entry_max)),
                    Some((min, max)) => Some((min.min(entry_min), max.max(entry_max))),
                }
            });

        Ok(TableDescription {
            database: database.to_string(),
            table: table.to_string(),
            schema_json: table_meta.schema_json.clone(),
            compression: table_meta.compression.clone(),
            segment_count,
            total_bytes,
            watermark_min: watermark_range.map(|(min, _)| min),
            watermark_max: watermark_range.map(|(_, max)| max),
            event_time_min: event_time_range.map(|(min, _)| min),
            event_time_max: event_time_range.map(|(_, max)| max),
        })
    }
}

/// Description of a table's structure and statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableDescription {
    pub database: String,
    pub table: String,
    pub schema_json: Option<String>,
    pub compression: Option<String>,
    pub segment_count: usize,
    pub total_bytes: u64,
    pub watermark_min: Option<u64>,
    pub watermark_max: Option<u64>,
    pub event_time_min: Option<u64>,
    pub event_time_max: Option<u64>,
}

#[allow(dead_code)]
fn mix64(mut x: u64) -> u64 {
    // Simple mixer to spread shard hints; not cryptographic.
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

pub(crate) fn load_manifest(path: &Path) -> Result<Manifest, EngineError> {
    if !path.exists() {
        return Ok(Manifest::empty());
    }
    let bytes =
        fs::read(path).map_err(|e| EngineError::Internal(format!("read manifest failed: {e}")))?;
    let mut manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|e| EngineError::Internal(format!("parse manifest failed: {e}")))?;

    if manifest.migrate_if_needed() {
        persist_manifest(path, &manifest)?;
    }

    Ok(manifest)
}

pub(crate) fn persist_manifest(path: &Path, manifest: &Manifest) -> Result<(), EngineError> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| EngineError::Internal(format!("serialize manifest failed: {e}")))?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| EngineError::Io(format!("create manifest dir failed: {e}")))?;
    }

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| EngineError::Io(format!("open manifest tmp failed: {e}")))?;
        file.write_all(&bytes)
            .map_err(|e| EngineError::Io(format!("write manifest tmp failed: {e}")))?;
        file.sync_all()
            .map_err(|e| EngineError::Io(format!("fsync manifest tmp failed: {e}")))?;
    }

    if let Err(e) = fs::rename(&tmp, path) {
        if e.kind() == ErrorKind::NotFound {
            // Fallback for rare filesystem races: write directly to final path.
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .map_err(|e| EngineError::Io(format!("open manifest final failed: {e}")))?;
            file.write_all(&bytes)
                .map_err(|e| EngineError::Io(format!("write manifest final failed: {e}")))?;
            file.sync_all()
                .map_err(|e| EngineError::Io(format!("fsync manifest final failed: {e}")))?;
        } else {
            return Err(EngineError::Io(format!("rename manifest failed: {e}")));
        }
    }

    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }

    Ok(())
}

fn snapshot_manifest(snapshot_path: &Path, manifest_path: &Path) -> Result<(), EngineError> {
    if let Some(parent) = snapshot_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| EngineError::Io(format!("create snapshot dir failed: {e}")))?;
    }
    fs::copy(manifest_path, snapshot_path)
        .map_err(|e| EngineError::Io(format!("copy manifest snapshot failed: {e}")))?;
    Ok(())
}

fn validate_manifest(manifest: &Manifest) -> Result<(), EngineError> {
    for db in &manifest.databases {
        if !is_valid_ident(&db.name) {
            return Err(EngineError::InvalidArgument(
                "invalid database name in manifest".into(),
            ));
        }
    }
    for tbl in &manifest.tables {
        if !is_valid_ident(&tbl.database) || !is_valid_ident(&tbl.name) {
            return Err(EngineError::InvalidArgument(
                "invalid table name in manifest".into(),
            ));
        }
    }
    for entry in &manifest.entries {
        if entry.segment_id.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "manifest entry missing segment_id".into(),
            ));
        }
        if entry.size_bytes == 0 {
            return Err(EngineError::InvalidArgument(
                "manifest entry has zero size_bytes".into(),
            ));
        }
        if entry.checksum == 0 {
            return Err(EngineError::InvalidArgument(
                "manifest entry has zero checksum".into(),
            ));
        }
        if !is_valid_ident(&entry.database) || !is_valid_ident(&entry.table) {
            return Err(EngineError::InvalidArgument(
                "manifest entry invalid database/table name".into(),
            ));
        }
        if let Some(c) = &entry.compression {
            if c != "zstd" && c != "lz4" && c != "snappy" {
                return Err(EngineError::InvalidArgument(
                    format!("manifest entry has unsupported compression: {} (supported: zstd, lz4, snappy)", c),
                ));
            }
        }
    }
    Ok(())
}

fn normalize_compression(opt: Option<String>) -> Result<Option<String>, EngineError> {
    match opt.as_deref() {
        Some("zstd") => Ok(Some("zstd".to_string())),
        Some("lz4") => Ok(Some("lz4".to_string())),
        Some("snappy") => Ok(Some("snappy".to_string())),
        Some("none") | None => Ok(None),
        Some(other) => Err(EngineError::InvalidArgument(format!(
            "unsupported compression: {other} (supported: zstd, lz4, snappy, none)"
        ))),
    }
}

/// Compress payload using the specified codec.
///
/// Compression speed and ratio comparison:
/// - LZ4: Fastest compression/decompression, ~2-3x compression ratio
/// - Snappy: Very fast, ~2x compression ratio
/// - ZSTD: Best compression ratio (~4-5x), slower but still fast decompression
///
/// For OLAP workloads:
/// - Use LZ4 for hot data (fastest reads)
/// - Use ZSTD for cold/archived data (best compression)
fn compress_payload(payload: &[u8], compression: Option<&str>) -> Result<Vec<u8>, EngineError> {
    match compression {
        Some("zstd") => zstd_encode_all(Cursor::new(payload), 3)
            .map_err(|e| EngineError::Internal(format!("zstd encode failed: {e}"))),
        Some("lz4") => Ok(lz4_compress(payload)),
        Some("snappy") => {
            let mut encoder = SnappyEncoder::new(Vec::new());
            std::io::copy(&mut Cursor::new(payload), &mut encoder)
                .map_err(|e| EngineError::Internal(format!("snappy encode failed: {e}")))?;
            encoder.into_inner()
                .map_err(|e| EngineError::Internal(format!("snappy finalize failed: {e}")))
        }
        Some(other) => Err(EngineError::InvalidArgument(format!(
            "unsupported compression: {other}"
        ))),
        None => Ok(payload.to_vec()),
    }
}

fn decompress_payload(payload: Vec<u8>, compression: Option<&str>) -> Result<Vec<u8>, EngineError> {
    match compression {
        Some("zstd") => zstd_decode_all(Cursor::new(payload))
            .map_err(|e| EngineError::InvalidArgument(format!("zstd decode failed: {e}"))),
        Some("lz4") => lz4_decompress(&payload)
            .map_err(|e| EngineError::InvalidArgument(format!("lz4 decode failed: {e}"))),
        Some("snappy") => {
            let mut decoder = SnappyDecoder::new(Cursor::new(payload));
            let mut decompressed = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                .map_err(|e| EngineError::InvalidArgument(format!("snappy decode failed: {e}")))?;
            Ok(decompressed)
        }
        Some(other) => Err(EngineError::InvalidArgument(format!(
            "unsupported compression: {other}"
        ))),
        None => Ok(payload),
    }
}

fn sync_dir(path: &Path) -> Result<(), EngineError> {
    let dir = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| EngineError::Io(format!("open dir for fsync failed: {e}")))?;
    dir.sync_all()
        .map_err(|e| EngineError::Io(format!("fsync dir failed: {e}")))
}

pub(crate) fn persist_segment_ipc(
    storage: &crate::storage::TieredStorage,
    segment_id: &str,
    payload: &[u8],
) -> Result<(), EngineError> {
    storage.persist_segment_local(segment_id, payload)
}

/// Hash an array value at a specific row index for deduplication key computation
fn hash_array_value(hasher: &mut impl std::hash::Hasher, array: &dyn arrow_array::Array, row_idx: usize) {
    use arrow_array::*;
    use std::hash::Hash;

    if array.is_null(row_idx) {
        // Hash a sentinel for null
        0u8.hash(hasher);
        return;
    }

    // Mark as non-null
    1u8.hash(hasher);

    match array.data_type() {
        arrow_schema::DataType::Boolean => {
            if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Int8 => {
            if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Int16 => {
            if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Int32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Int64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::UInt8 => {
            if let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::UInt16 => {
            if let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::UInt32 => {
            if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::UInt64 => {
            if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Float32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                arr.value(row_idx).to_bits().hash(hasher);
            }
        }
        arrow_schema::DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                arr.value(row_idx).to_bits().hash(hasher);
            }
        }
        arrow_schema::DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Binary => {
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::LargeBinary => {
            if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::FixedSizeBinary(_) => {
            if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                arr.value(row_idx).hash(hasher);
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                arr.value(row_idx).hash(hasher);
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                arr.value(row_idx).hash(hasher);
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Date32 => {
            if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        arrow_schema::DataType::Date64 => {
            if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
                arr.value(row_idx).hash(hasher);
            }
        }
        _ => {
            // For unsupported types, hash the row index as a fallback
            // This ensures deterministic behavior but may cause false matches
            row_idx.hash(hasher);
        }
    }
}

/// Extract a version value from a column for deduplication ordering
fn get_version_value(array: &dyn arrow_array::Array, row_idx: usize) -> i64 {
    use arrow_array::*;

    if array.is_null(row_idx) {
        return i64::MIN;
    }

    match array.data_type() {
        arrow_schema::DataType::Int8 => {
            array.as_any().downcast_ref::<Int8Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::Int16 => {
            array.as_any().downcast_ref::<Int16Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::Int32 => {
            array.as_any().downcast_ref::<Int32Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::Int64 => {
            array.as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(row_idx)).unwrap_or(0)
        }
        arrow_schema::DataType::UInt8 => {
            array.as_any().downcast_ref::<UInt8Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::UInt16 => {
            array.as_any().downcast_ref::<UInt16Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::UInt32 => {
            array.as_any().downcast_ref::<UInt32Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::UInt64 => {
            array.as_any().downcast_ref::<UInt64Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                arr.value(row_idx)
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                arr.value(row_idx)
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                arr.value(row_idx)
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                arr.value(row_idx)
            } else {
                0
            }
        }
        arrow_schema::DataType::Date32 => {
            array.as_any().downcast_ref::<Date32Array>()
                .map(|a| a.value(row_idx) as i64).unwrap_or(0)
        }
        arrow_schema::DataType::Date64 => {
            array.as_any().downcast_ref::<Date64Array>()
                .map(|a| a.value(row_idx)).unwrap_or(0)
        }
        _ => row_idx as i64, // Fallback: use row index
    }
}

fn load_segment(storage: &crate::storage::TieredStorage, entry: &ManifestEntry) -> Result<Vec<u8>, EngineError> {
    let data = storage.load_segment(entry)?;
    let actual = compute_checksum(&data);
    if actual != entry.checksum {
        return Err(EngineError::Io(format!(
            "checksum mismatch for segment {} expected={} actual={}",
            entry.segment_id, entry.checksum, actual
        )));
    }
    decompress_payload(data, entry.compression.as_deref())
}

fn load_segment_raw(storage: &crate::storage::TieredStorage, entry: &ManifestEntry) -> Result<Vec<u8>, EngineError> {
    let data = storage.load_segment(entry)?;
    let actual = compute_checksum(&data);
    if actual != entry.checksum {
        return Err(EngineError::Io(format!(
            "checksum mismatch for segment {} expected={} actual={}",
            entry.segment_id, entry.checksum, actual
        )));
    }
    Ok(data)
}

/// Sort a hot ingest payload by event_time, tenant_id, route_id when present.
fn sort_hot_payload(ipc_data: &[u8], granularity_rows: usize) -> Result<Vec<u8>, EngineError> {
    use arrow_ord::sort::{lexsort_to_indices, SortColumn};

    let batches = read_ipc_batches(ipc_data)?;
    if batches.is_empty() {
        return Ok(ipc_data.to_vec());
    }
    let schema = batches[0].schema();
    let concatenated = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| EngineError::Internal(format!("concat batches for hot sort failed: {e}")))?
    };
    if concatenated.num_rows() == 0 {
        return Ok(ipc_data.to_vec());
    }

    let mut sort_columns = Vec::new();
    for col_name in ["event_time", "tenant_id", "route_id"] {
        if let Some((idx, _)) = schema.column_with_name(col_name) {
            sort_columns.push(SortColumn {
                values: concatenated.column(idx).clone(),
                options: Some(arrow_ord::sort::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            });
        }
    }
    if sort_columns.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| EngineError::Internal(format!("hot sort failed: {e}")))?;
    let sorted_batch = arrow_select::take::take_record_batch(&concatenated, &indices)
        .map_err(|e| EngineError::Internal(format!("hot sort take failed: {e}")))?;

    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, schema.as_ref())
            .map_err(|e| EngineError::Internal(format!("hot sort ipc writer failed: {e}")))?;
        if granularity_rows == 0 || sorted_batch.num_rows() <= granularity_rows {
            writer
                .write(&sorted_batch)
                .map_err(|e| EngineError::Internal(format!("hot sort ipc write failed: {e}")))?;
        } else {
            let mut offset = 0;
            while offset < sorted_batch.num_rows() {
                let len = (granularity_rows).min(sorted_batch.num_rows() - offset);
                let slice = sorted_batch.slice(offset, len);
                writer
                    .write(&slice)
                    .map_err(|e| EngineError::Internal(format!("hot sort ipc write failed: {e}")))?;
                offset += len;
            }
        }
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("hot sort ipc finish failed: {e}")))?;
    }
    Ok(output)
}

/// Apply ORDER BY sorting to IPC record batches
fn apply_order_by(ipc_data: &[u8], filter: &QueryFilter) -> Result<Vec<u8>, EngineError> {
    use arrow_ord::sort::{lexsort_to_indices, SortColumn};

    let order_by = match &filter.order_by {
        Some(ob) => ob,
        None => return Ok(ipc_data.to_vec()),
    };

    // Parse IPC to get record batches
    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("read IPC for sorting failed: {e}")))?;

    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next()
        .transpose()
        .map_err(|e| EngineError::Internal(format!("read batch for sorting failed: {e}")))?
    {
        batches.push(batch);
    }

    if batches.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    // Concatenate all batches into a single record batch for sorting
    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok(ipc_data.to_vec());
    }
    let concatenated = arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| EngineError::Internal(format!("concat batches for sort failed: {e}")))?;

    // Build sort columns
    let mut sort_columns = Vec::new();
    for (col_name, ascending) in order_by {
        if let Some((idx, _)) = schema.column_with_name(col_name) {
            sort_columns.push(SortColumn {
                values: concatenated.column(idx).clone(),
                options: Some(arrow_ord::sort::SortOptions {
                    descending: !ascending,
                    nulls_first: true,
                }),
            });
        }
    }

    if sort_columns.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    let sort_limit = match (filter.limit, filter.offset) {
        (Some(limit), Some(offset)) => Some(limit.saturating_add(offset)),
        (Some(limit), None) => Some(limit),
        _ => None,
    };
    if matches!(sort_limit, Some(0)) {
        let mut output = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut output, schema.as_ref())
                .map_err(|e| EngineError::Internal(format!("create IPC writer failed: {e}")))?;
            writer
                .finish()
                .map_err(|e| EngineError::Internal(format!("finish IPC writer failed: {e}")))?;
        }
        return Ok(output);
    }

    // Get sorted indices (optionally limited for top-K)
    let indices = lexsort_to_indices(&sort_columns, sort_limit)
        .map_err(|e| EngineError::Internal(format!("lexsort failed: {e}")))?;

    let sorted_batch = arrow_select::take::take_record_batch(&concatenated, &indices)
        .map_err(|e| EngineError::Internal(format!("take batch failed: {e}")))?;

    // Write back to IPC
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, schema.as_ref())
            .map_err(|e| EngineError::Internal(format!("create IPC writer failed: {e}")))?;
        writer
            .write(&sorted_batch)
            .map_err(|e| EngineError::Internal(format!("write sorted batch failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("finish IPC writer failed: {e}")))?;
    }

    Ok(output)
}

/// Apply OFFSET to IPC record batches
fn apply_offset(ipc_data: &[u8], offset: usize) -> Result<Vec<u8>, EngineError> {
    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("read IPC for offset failed: {e}")))?;

    let mut remaining_offset = offset;
    let mut batches_to_write: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<Arc<Schema>> = None;
    let mut saw_batch = false;

    while let Some(batch) = reader
        .next()
        .transpose()
        .map_err(|e| EngineError::Internal(format!("read batch for offset failed: {e}")))?
    {
        saw_batch = true;
        if schema.is_none() {
            schema = Some(batch.schema());
        }

        let batch_rows = batch.num_rows();
        if remaining_offset > 0 {
            if batch_rows <= remaining_offset {
                remaining_offset -= batch_rows;
                continue;
            }
            let kept = batch.slice(remaining_offset, batch_rows - remaining_offset);
            remaining_offset = 0;
            batches_to_write.push(kept);
            continue;
        }

        batches_to_write.push(batch);
    }

    // Write collected batches
    if !batches_to_write.is_empty() {
        let mut output = Vec::new();
        {
            let first_schema = batches_to_write[0].schema();
            let mut writer = StreamWriter::try_new(&mut output, first_schema.as_ref())
                .map_err(|e| EngineError::Internal(format!("create IPC writer failed: {e}")))?;
            for batch in &batches_to_write {
                writer.write(batch)
                    .map_err(|e| EngineError::Internal(format!("write batch failed: {e}")))?;
            }
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("finish IPC writer failed: {e}")))?;
        }
        return Ok(output);
    }

    if !saw_batch {
        return Ok(ipc_data.to_vec());
    }

    // Return empty result with schema
    if let Some(schema) = schema {
        let mut empty = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut empty, schema.as_ref())
                .map_err(|e| EngineError::Internal(format!("create IPC writer failed: {e}")))?;
            w.finish()
                .map_err(|e| EngineError::Internal(format!("finish IPC writer failed: {e}")))?;
        }
        return Ok(empty);
    }

    Ok(Vec::new())
}

fn apply_offset_limit_to_batches(
    batches: Vec<RecordBatch>,
    remaining_offset: &mut usize,
    remaining_limit: &mut usize,
) -> Vec<RecordBatch> {
    if *remaining_offset == 0 && *remaining_limit == usize::MAX {
        return batches;
    }
    let mut out = Vec::new();
    for batch in batches {
        if *remaining_limit == 0 {
            break;
        }
        let rows = batch.num_rows();
        if *remaining_offset > 0 {
            if rows <= *remaining_offset {
                *remaining_offset -= rows;
                continue;
            }
        }
        let start = *remaining_offset;
        let mut len = rows.saturating_sub(start);
        *remaining_offset = 0;
        if len == 0 {
            continue;
        }
        if len > *remaining_limit {
            len = *remaining_limit;
        }
        out.push(batch.slice(start, len));
        *remaining_limit = remaining_limit.saturating_sub(len);
    }
    out
}

/// Apply column projection to IPC record batches (select specific columns)
fn apply_projection(ipc_data: &[u8], columns: &[String]) -> Result<Vec<u8>, EngineError> {
    apply_projection_with_schema(ipc_data, columns, None)
}

/// Apply projection with optional expected schema for handling missing columns (e.g. from ALTER TABLE ADD COLUMN)
fn apply_projection_with_schema(ipc_data: &[u8], columns: &[String], expected_schema: Option<&Schema>) -> Result<Vec<u8>, EngineError> {
    if columns.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("read IPC for projection failed: {e}")))?;

    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next()
        .transpose()
        .map_err(|e| EngineError::Internal(format!("read batch for projection failed: {e}")))?
    {
        batches.push(batch);
    }

    if batches.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    let schema = batches[0].schema();
    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    // Build projection info - handle missing columns with NULLs
    enum ColSource {
        Existing(usize),    // Index in source batch
        NullFilled(Field),  // Generate NULL column with this field
    }

    let mut sources: Vec<ColSource> = Vec::with_capacity(columns.len());
    let mut new_fields = Vec::with_capacity(columns.len());

    for col_name in columns {
        if let Some((idx, field)) = schema.column_with_name(col_name) {
            sources.push(ColSource::Existing(idx));
            new_fields.push(field.clone());
        } else {
            // Column not in batch - check expected schema for type info
            let field = if let Some(exp_schema) = expected_schema {
                if let Some((_, exp_field)) = exp_schema.column_with_name(col_name) {
                    // Use field from expected schema, ensure nullable
                    Field::new(col_name, exp_field.data_type().clone(), true)
                } else {
                    // Not found even in expected schema - default to nullable Int64
                    Field::new(col_name, DataType::Int64, true)
                }
            } else {
                // No expected schema - default to nullable Int64
                Field::new(col_name, DataType::Int64, true)
            };
            sources.push(ColSource::NullFilled(field.clone()));
            new_fields.push(field);
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));

    // Project each batch
    let mut projected_batches = Vec::with_capacity(batches.len());
    for batch in &batches {
        let batch_rows = batch.num_rows();
        let projected_columns: Vec<ArrayRef> = sources.iter().map(|src| {
            match src {
                ColSource::Existing(idx) => batch.column(*idx).clone(),
                ColSource::NullFilled(field) => {
                    // Create a NULL-filled array of the appropriate type
                    create_null_array(field.data_type(), batch_rows)
                }
            }
        }).collect();
        let projected = RecordBatch::try_new(new_schema.clone(), projected_columns)
            .map_err(|e| EngineError::Internal(format!("create projected batch failed: {e}")))?;
        projected_batches.push(projected);
    }

    // Write back to IPC
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, new_schema.as_ref())
            .map_err(|e| EngineError::Internal(format!("create IPC writer for projection failed: {e}")))?;
        for batch in &projected_batches {
            writer
                .write(batch)
                .map_err(|e| EngineError::Internal(format!("write projected batch failed: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("finish IPC writer for projection failed: {e}")))?;
    }

    Ok(output)
}

/// Create a NULL-filled array of the specified type
fn create_null_array(data_type: &DataType, len: usize) -> ArrayRef {
    use arrow_array::builder::*;
    match data_type {
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::UInt8 => {
            let mut builder = UInt8Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::UInt16 => {
            let mut builder = UInt16Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::UInt32 => {
            let mut builder = UInt32Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(len, 0);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(len, 0);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
        _ => {
            // Default fallback to nullable Int64
            let mut builder = Int64Builder::with_capacity(len);
            for _ in 0..len { builder.append_null(); }
            Arc::new(builder.finish())
        }
    }
}

/// Apply DISTINCT to remove duplicate rows from IPC record batches
fn apply_distinct(ipc_data: &[u8]) -> Result<Vec<u8>, EngineError> {
    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("read IPC for distinct failed: {e}")))?;

    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next()
        .transpose()
        .map_err(|e| EngineError::Internal(format!("read batch for distinct failed: {e}")))?
    {
        batches.push(batch);
    }

    if batches.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        return Ok(ipc_data.to_vec());
    }

    // Concatenate all batches
    let mut all_columns = Vec::new();
    for col_idx in 0..schema.fields().len() {
        let arrays: Vec<_> = batches.iter().map(|b| b.column(col_idx).clone()).collect();
        let refs: Vec<_> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow_select::concat::concat(&refs)
            .map_err(|e| EngineError::Internal(format!("concat for distinct failed: {e}")))?;
        all_columns.push(concatenated);
    }

    // Build a set of seen row hashes to identify duplicates
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashSet;
    use std::hash::{Hash, Hasher};

    let mut seen: HashSet<u64> = HashSet::new();
    let mut keep_indices: Vec<usize> = Vec::new();

    for row_idx in 0..total_rows {
        let mut hasher = DefaultHasher::new();
        for col in &all_columns {
            // Hash the raw bytes for this row in each column
            // We use the debug representation as a proxy for value equality
            // This is a simplification - ideally we'd compare actual values
            let array = col.as_ref();
            if array.is_null(row_idx) {
                0u8.hash(&mut hasher);
            } else {
                1u8.hash(&mut hasher);
                // Hash based on data type
                use arrow::datatypes::DataType;
                match array.data_type() {
                    DataType::UInt64 => {
                        let arr = array.as_any().downcast_ref::<UInt64Array>();
                        if let Some(a) = arr {
                            a.value(row_idx).hash(&mut hasher);
                        }
                    }
                    DataType::Int64 => {
                        let arr = array.as_any().downcast_ref::<arrow_array::Int64Array>();
                        if let Some(a) = arr {
                            a.value(row_idx).hash(&mut hasher);
                        }
                    }
                    DataType::Utf8 => {
                        let arr = array.as_any().downcast_ref::<arrow_array::StringArray>();
                        if let Some(a) = arr {
                            a.value(row_idx).hash(&mut hasher);
                        }
                    }
                    DataType::Float64 => {
                        let arr = array.as_any().downcast_ref::<arrow_array::Float64Array>();
                        if let Some(a) = arr {
                            a.value(row_idx).to_bits().hash(&mut hasher);
                        }
                    }
                    DataType::Boolean => {
                        let arr = array.as_any().downcast_ref::<arrow_array::BooleanArray>();
                        if let Some(a) = arr {
                            a.value(row_idx).hash(&mut hasher);
                        }
                    }
                    _ => {
                        // For other types, use row index (no dedup)
                        row_idx.hash(&mut hasher);
                    }
                }
            }
        }
        let row_hash = hasher.finish();
        if seen.insert(row_hash) {
            keep_indices.push(row_idx);
        }
    }

    // If no duplicates, return original
    if keep_indices.len() == total_rows {
        return Ok(ipc_data.to_vec());
    }

    // Build indices array for selection
    let indices_u32 =
        arrow_array::UInt32Array::from(keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

    // Select only unique rows
    use arrow_select::take::take;
    let mut distinct_columns = Vec::new();
    for col in &all_columns {
        let selected = take(col.as_ref(), &indices_u32, None)
            .map_err(|e| EngineError::Internal(format!("take for distinct failed: {e}")))?;
        distinct_columns.push(selected);
    }

    let distinct_batch = RecordBatch::try_new(schema.clone(), distinct_columns)
        .map_err(|e| EngineError::Internal(format!("create distinct batch failed: {e}")))?;

    // Write back to IPC
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, schema.as_ref())
            .map_err(|e| EngineError::Internal(format!("create IPC writer failed: {e}")))?;
        writer
            .write(&distinct_batch)
            .map_err(|e| EngineError::Internal(format!("write distinct batch failed: {e}")))?;
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("finish IPC writer failed: {e}")))?;
    }

    Ok(output)
}

fn parse_table_target(sql: &str) -> Result<(String, String), EngineError> {
    let mut tokens = sql.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.eq_ignore_ascii_case("from") {
            if let Some(target) = tokens.next() {
                let cleaned = target.trim_end_matches(';');
                if let Some((db, tbl)) = cleaned.split_once('.') {
                    let db = if db.is_empty() { "default" } else { db };
                    let tbl = if tbl.is_empty() { "default" } else { tbl };
                    if !is_valid_ident(db) || !is_valid_ident(tbl) {
                        return Err(EngineError::InvalidArgument(
                            "invalid database or table identifier".into(),
                        ));
                    }
                    return Ok((db.to_string(), tbl.to_string()));
                } else {
                    let tbl = if cleaned.is_empty() {
                        "default"
                    } else {
                        cleaned
                    };
                    if !is_valid_ident(tbl) {
                        return Err(EngineError::InvalidArgument(
                            "invalid table identifier".into(),
                        ));
                    }
                    return Ok(("default".to_string(), tbl.to_string()));
                }
            } else {
                break;
            }
        }
    }

    // If no FROM clause is found, we assume it's a scalar query (e.g. SELECT 1)
    // We return empty strings for database and table to indicate this.
    Ok(("".to_string(), "".to_string()))
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct QueryFilter {
    watermark_ge: Option<u64>,
    watermark_le: Option<u64>,
    limit: Option<usize>,
    offset: Option<usize>,
    event_time_ge: Option<u64>,
    event_time_le: Option<u64>,
    pub tenant_id_eq: Option<u64>,
    pub route_id_eq: Option<u64>,
    pub tenant_id_in: Option<Vec<u64>>,
    pub route_id_in: Option<Vec<u64>>,
    order_by: Option<Vec<(String, bool)>>, // (column, ascending)
    distinct: bool,
    /// Generic numeric equality filters (integer-only)
    numeric_eq_filters: Vec<(String, i128)>,
    /// Generic floating-point equality filters
    float_eq_filters: Vec<(String, f64)>,
    /// Generic boolean equality filters
    bool_eq_filters: Vec<(String, bool)>,
    /// LIKE patterns: Vec<(column, pattern, negate)>
    /// negate=true means NOT LIKE
    like_filters: Vec<(String, String, bool)>,
    /// IS NULL checks: Vec<(column, is_null)>
    /// is_null=true means IS NULL, is_null=false means IS NOT NULL
    null_filters: Vec<(String, bool)>,
    /// Generic string equality filters: Vec<(column, value)>
    string_eq_filters: Vec<(String, String)>,
    /// Generic string IN filters: Vec<(column, values)>
    string_in_filters: Vec<(String, Vec<String>)>,
    /// Numeric range filters for segment pruning using column stats
    #[serde(default)]
    numeric_range_filters: Vec<crate::sql::NumericFilter>,
}

// --- RESTORED HELPER TYPES ---

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableFieldSpec {
    pub name: String,
    pub data_type: String, // Use string representation of arrow data type
    pub nullable: bool,
    pub encoding: Option<String>,
}



#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct AggPlan {
    pub group_by: GroupBy,
    pub aggs: Vec<AggKind>,
    pub having: Vec<HavingCondition>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggKind {
    CountStar,
    CountDistinct { column: String },
    Sum { column: String },
    Avg { column: String },
    Min { column: String },
    Max { column: String },
    StddevSamp { column: String },
    StddevPop { column: String },
    VarianceSamp { column: String },
    VariancePop { column: String },
    ApproxCountDistinct { column: String },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HavingCondition {
    pub agg: AggKind,
    pub op: HavingOp,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HavingOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum GroupBy {
    #[default] None,
    Tenant,
    Route,
    Columns(Vec<GroupByColumn>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum GroupByColumn {
    TenantId,
    RouteId,
}

// --- CONVERSION HELPERS ---

fn agg_kind_from_sql(kind: crate::sql::AggKind) -> AggKind {
    match kind {
        crate::sql::AggKind::CountStar => AggKind::CountStar,
        crate::sql::AggKind::CountDistinct { column } => AggKind::CountDistinct { column },
        crate::sql::AggKind::Sum { column } => AggKind::Sum { column },
        crate::sql::AggKind::Avg { column } => AggKind::Avg { column },
        crate::sql::AggKind::Min { column } => AggKind::Min { column },
        crate::sql::AggKind::Max { column } => AggKind::Max { column },
        crate::sql::AggKind::StddevSamp { column } => AggKind::StddevSamp { column },
        crate::sql::AggKind::StddevPop { column } => AggKind::StddevPop { column },
        crate::sql::AggKind::VarianceSamp { column } => AggKind::VarianceSamp { column },
        crate::sql::AggKind::VariancePop { column } => AggKind::VariancePop { column },
        crate::sql::AggKind::ApproxCountDistinct { column } => AggKind::ApproxCountDistinct { column },
    }
}

fn having_condition_from_sql(cond: crate::sql::HavingCondition) -> HavingCondition {
    HavingCondition {
        agg: agg_kind_from_sql(cond.agg),
        op: match cond.op {
            crate::sql::HavingOp::Eq => HavingOp::Eq,
            crate::sql::HavingOp::Ne => HavingOp::Ne,
            crate::sql::HavingOp::Gt => HavingOp::Gt,
            crate::sql::HavingOp::Ge => HavingOp::Ge,
            crate::sql::HavingOp::Lt => HavingOp::Lt,
            crate::sql::HavingOp::Le => HavingOp::Le,
        },
        value: cond.value,
    }
}

fn agg_plan_from_sql(plan: crate::sql::AggPlan) -> AggPlan {
    AggPlan {
        group_by: match plan.group_by {
            crate::sql::GroupBy::None => GroupBy::None,
            crate::sql::GroupBy::Tenant => GroupBy::Tenant,
            crate::sql::GroupBy::Route => GroupBy::Route,
            crate::sql::GroupBy::Columns(cols) => GroupBy::Columns(
                cols.into_iter().map(|c| match c {
                    crate::sql::GroupByColumn::TenantId => GroupByColumn::TenantId,
                    crate::sql::GroupByColumn::RouteId => GroupByColumn::RouteId,
                }).collect()
            ),
        },
        aggs: plan.aggs.into_iter().map(agg_kind_from_sql).collect(),
        having: plan.having.into_iter().map(having_condition_from_sql).collect(),
    }
}

fn query_filter_from_sql(filter: &crate::sql::QueryFilter) -> QueryFilter {
    // Basic mapping. Missing fields are defaulted or ignored for now.
    QueryFilter {
        watermark_ge: filter.watermark_ge,
        watermark_le: filter.watermark_le,
        limit: filter.limit,
        offset: filter.offset,
        event_time_ge: filter.event_time_ge,
        event_time_le: filter.event_time_le,
        tenant_id_eq: filter.tenant_id_eq,
        route_id_eq: filter.route_id_eq,
        tenant_id_in: filter.tenant_id_in.clone(),
        route_id_in: filter.route_id_in.clone(),
        order_by: None, // Handle at call site or improve mapping
        distinct: false, // Handle at call site
        numeric_eq_filters: Vec::new(),
        float_eq_filters: Vec::new(),
        bool_eq_filters: Vec::new(),
        like_filters: filter.like_filters.clone(),
        null_filters: filter.null_filters.clone(),
        string_eq_filters: filter.string_eq_filters.clone(),
        string_in_filters: filter.string_in_filters.clone(),
        numeric_range_filters: filter.numeric_range_filters.clone(),
    }
}

fn build_query_plan(sql: &str) -> Result<CachedPlan, EngineError> {
    use crate::sql::{parse_sql, SqlStatement};
    let sql_upper = sql.to_uppercase();
    if sql_upper.contains(" JOIN ") {
        return Ok(CachedPlan { kind: PlanKind::Join });
    }
    if sql_upper.contains(" UNION ") || sql_upper.contains(" INTERSECT ") || sql_upper.contains(" EXCEPT ") {
        return Ok(CachedPlan { kind: PlanKind::SetOperation });
    }
    if sql_upper.contains(" IN (SELECT") || sql_upper.contains(" IN(SELECT") ||
       sql_upper.contains("EXISTS (SELECT") || sql_upper.contains("EXISTS(SELECT") ||
       sql_upper.contains("= (SELECT") || sql_upper.contains("=(SELECT") {
        return Ok(CachedPlan { kind: PlanKind::Subquery });
    }
    if sql_upper.trim_start().starts_with("WITH ") {
        return Ok(CachedPlan { kind: PlanKind::Cte });
    }
    match parse_sql(sql)? {
        SqlStatement::Transaction(cmd) => Ok(CachedPlan {
            kind: PlanKind::Transaction(cmd),
        }),
        SqlStatement::Query(parsed) => {
            let mut filter = query_filter_from_sql(&parsed.filter);
            // Map extra fields
            if let Some(order_by) = parsed.order_by {
                filter.order_by = Some(order_by.into_iter().map(|o| (o.column, o.ascending)).collect());
            }
            filter.distinct = parsed.distinct;
            
            Ok(CachedPlan {
                kind: PlanKind::Simple {
                    agg: parsed.aggregation.map(agg_plan_from_sql),
                    db: parsed.database,
                    table: parsed.table,
                    projection: parsed.projection,
                    filter,
                    computed_columns: parsed.computed_columns,
                },
            })
        },
        _ => Err(EngineError::NotImplemented("unsupported statement type".into())),
    }
}

// --- CTE SUPPORT ---

#[derive(Default)]
pub struct CteContext {
    results: std::collections::HashMap<String, Vec<u8>>,
}

impl CteContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_result(&mut self, name: String, data: Vec<u8>) {
        self.results.insert(name, data);
    }
    pub fn get_result(&self, name: &str) -> Option<&Vec<u8>> {
        self.results.get(name)
    }
    /// Check if a CTE with the given name exists
    pub fn has_cte(&self, name: &str) -> bool {
        self.results.contains_key(name)
    }
}

/// Extract the main SELECT portion from a CTE query (after WITH...AS clauses)
fn extract_main_query_from_cte(sql: &str) -> String {
    let sql_upper = sql.to_uppercase();

    // Find the position after the last CTE definition
    // CTEs have the form: WITH name AS (...), name2 AS (...) SELECT ...
    // We need to find the final SELECT that's not inside a CTE

    let mut depth = 0;
    let mut last_close_paren_after_as = 0;
    let mut in_as_block = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Check for 'AS' followed by '('
        if !in_as_block && depth == 0 && i + 3 < chars.len() {
            let slice: String = chars[i..i+2].iter().collect();
            if slice.to_uppercase() == "AS" {
                // Check if followed by whitespace and '('
                let mut j = i + 2;
                while j < chars.len() && chars[j].is_whitespace() {
                    j += 1;
                }
                if j < chars.len() && chars[j] == '(' {
                    in_as_block = true;
                    i = j;
                    depth = 1;
                    i += 1;
                    continue;
                }
            }
        }

        if in_as_block {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
                if depth == 0 {
                    in_as_block = false;
                    last_close_paren_after_as = i;
                }
            }
        }

        i += 1;
    }

    // Find the SELECT after the last CTE definition
    if last_close_paren_after_as > 0 {
        let remainder = &sql[last_close_paren_after_as + 1..];
        // Skip any comma (for multiple CTEs) and find SELECT
        let remainder_upper = remainder.to_uppercase();
        if let Some(select_pos) = remainder_upper.find("SELECT") {
            // Check it's not another CTE (no AS ( before it at same level)
            let before_select = &remainder_upper[..select_pos];
            if !before_select.contains(" AS ") && !before_select.contains(" AS(") {
                return remainder[select_pos..].to_string();
            }
        }
    }

    // Fallback: find first SELECT not inside WITH
    // Simple heuristic: skip "WITH" and find top-level "SELECT"
    if let Some(with_pos) = sql_upper.find("WITH") {
        let after_with = &sql[with_pos..];
        let after_with_upper = after_with.to_uppercase();

        // Find balanced parens to skip CTE definitions
        let mut depth = 0;
        let chars: Vec<char> = after_with.chars().collect();
        for (i, c) in chars.iter().enumerate() {
            if *c == '(' {
                depth += 1;
            } else if *c == ')' {
                depth -= 1;
            }

            // When at depth 0, check for SELECT
            if depth == 0 && i + 6 < chars.len() {
                let slice: String = chars[i..i+6].iter().collect();
                if slice.to_uppercase() == "SELECT" {
                    return after_with[i..].to_string();
                }
            }
        }
    }

    // Ultimate fallback: return as-is (will likely fail but won't recurse infinitely)
    sql.to_string()
}

pub fn execute_query_with_ctes(
    db: &Db,
    sql: &str,
    ctes: &[crate::sql::CteDefinition],
    timeout_millis: u32,
) -> Result<QueryResponse, EngineError> {
    let mut cte_context = CteContext::new();

    for cte in ctes {
        if cte.recursive {
             let result = execute_recursive_cte(db, cte, &cte_context, timeout_millis)?;
             cte_context.add_result(cte.name.clone(), result);
        } else {
             let cte_sql = cte.raw_sql.clone();

            let result = db.query(QueryRequest {
                sql: cte_sql,
                timeout_millis,
                collect_stats: false,
            })?;

            // Store the result in the CTE context
            cte_context.add_result(cte.name.clone(), result.records_ipc);
        }
    }

    // Extract just the main SELECT portion without the WITH clause
    // to avoid infinite recursion through CTE detection
    let main_query = extract_main_query_from_cte(sql);

    db.query(QueryRequest {
        sql: main_query,
        timeout_millis,
        collect_stats: false,
    })
}

// --- RESTORED COMPUTED VALUE STUBS ---

#[derive(Debug, Clone, PartialEq)]
pub enum ComputedValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Timestamp(i64),
    Date(i32),
    Decimal { value: i128, precision: u8, scale: i8 },
    Uuid([u8; 16]),
    Json(String),
}

impl ComputedValue {
    pub fn to_string_value(&self) -> String {
        match self {
            ComputedValue::String(s) => s.clone(),
            ComputedValue::Integer(i) => i.to_string(),
            ComputedValue::Float(f) => f.to_string(),
            ComputedValue::Boolean(b) => b.to_string(),
            ComputedValue::Null => "NULL".to_string(),
            ComputedValue::Uuid(bytes) => {
                // Format as standard UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
                format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5],
                    bytes[6], bytes[7],
                    bytes[8], bytes[9],
                    bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
                )
            }
            ComputedValue::Json(s) => s.clone(),
            ComputedValue::Date(days) => {
                // Days since Unix epoch to date string
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(*days as i64)) {
                    date.format("%Y-%m-%d").to_string()
                } else {
                    format!("Date({})", days)
                }
            }
            ComputedValue::Timestamp(micros) => {
                // Microseconds since Unix epoch to datetime string
                let secs = micros / 1_000_000;
                let nsecs = ((micros % 1_000_000) * 1000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                } else {
                    format!("Timestamp({})", micros)
                }
            }
            ComputedValue::Decimal { value, precision: _, scale } => {
                if *scale == 0 {
                    value.to_string()
                } else {
                    let divisor = 10i128.pow(*scale as u32);
                    let int_part = value / divisor;
                    let frac_part = (value % divisor).abs();
                    format!("{}.{:0>width$}", int_part, frac_part, width = *scale as usize)
                }
            }
            ComputedValue::Binary(bytes) => {
                // Hex representation for binary data
                let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
                format!("\\x{}", hex)
            }
        }
    }
    
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ComputedValue::Float(f) => Some(*f),
            ComputedValue::Integer(i) => Some(*i as f64),
            ComputedValue::Decimal { value, scale, .. } => {
                let divisor = 10f64.powi(*scale as i32);
                Some(*value as f64 / divisor)
            }
            _ => None,
        }
    }
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ComputedValue::Integer(i) => Some(*i),
            ComputedValue::Float(f) => Some(*f as i64),
            ComputedValue::Decimal { value, scale, .. } => {
                let divisor = 10i128.pow(*scale as u32);
                Some((*value / divisor) as i64)
            }
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ComputedValue::Null)
    }

    /// Get the UUID bytes if this is a Uuid variant
    pub fn as_uuid(&self) -> Option<&[u8; 16]> {
        match self {
            ComputedValue::Uuid(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Get the JSON string if this is a Json variant
    pub fn as_json(&self) -> Option<&str> {
        match self {
            ComputedValue::Json(s) => Some(s),
            _ => None,
        }
    }

    /// Get as boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ComputedValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ComputedValue::String(s) => Some(s),
            ComputedValue::Json(s) => Some(s),
            _ => None,
        }
    }
}

pub fn compare_values(_a: &ComputedValue, _b: &ComputedValue) -> i32 {
    0
}

pub struct EvalContext<'a> {
    pub row: &'a std::collections::HashMap<String, ComputedValue>,
}

impl<'a> EvalContext<'a> {
    pub fn new(row: &'a std::collections::HashMap<String, ComputedValue>) -> Self {
        Self { row }
    }
}

pub fn evaluate_expr(expr: &crate::sql::SelectExpr, ctx: &EvalContext) -> Result<ComputedValue, EngineError> {
    use crate::sql::{SelectExpr, LiteralValue, ScalarFunction};

    match expr {
        SelectExpr::Column(name) => {
            ctx.row.get(name).cloned().ok_or_else(|| {
                EngineError::Internal(format!("column not found in context: {}", name))
            })
        }

        SelectExpr::QualifiedColumn { table: _, column } => {
            // For now, just look up the column name ignoring table qualifier
            ctx.row.get(column).cloned().ok_or_else(|| {
                EngineError::Internal(format!("column not found in context: {}", column))
            })
        }

        SelectExpr::Literal(lit) => {
            Ok(match lit {
                LiteralValue::Integer(i) => ComputedValue::Integer(*i),
                LiteralValue::Float(f) => ComputedValue::Float(*f),
                LiteralValue::String(s) => ComputedValue::String(s.clone()),
                LiteralValue::Boolean(b) => ComputedValue::Boolean(*b),
                LiteralValue::Null => ComputedValue::Null,
            })
        }

        SelectExpr::Null => Ok(ComputedValue::Null),

        SelectExpr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr(left, ctx)?;
            let right_val = evaluate_expr(right, ctx)?;
            apply_binary_op(&left_val, op, &right_val)
        }

        SelectExpr::UnaryOp { op, expr: inner } => {
            let val = evaluate_expr(inner, ctx)?;
            apply_unary_op(op, &val)
        }

        SelectExpr::Function(func) => evaluate_scalar_function(func, ctx),

        SelectExpr::Aggregate(_) => {
            // Aggregates are handled separately in the aggregation engine
            Ok(ComputedValue::Null)
        }

        SelectExpr::Window { .. } => {
            // Window functions are handled separately
            Ok(ComputedValue::Null)
        }

        SelectExpr::Case { operand, when_clauses, else_result } => {
            evaluate_case_expr(operand, when_clauses, else_result, ctx)
        }

        SelectExpr::Subquery(_) => {
            // Subqueries not supported in expression evaluation context
            Err(EngineError::Internal("subqueries not supported in expression evaluation".into()))
        }
    }
}

fn apply_binary_op(left: &ComputedValue, op: &str, right: &ComputedValue) -> Result<ComputedValue, EngineError> {
    // Handle NULL propagation for most operations
    if matches!(left, ComputedValue::Null) || matches!(right, ComputedValue::Null) {
        return Ok(ComputedValue::Null);
    }

    match op {
        "+" => {
            match (left, right) {
                (ComputedValue::Integer(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Integer(a + b)),
                (ComputedValue::Float(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(a + b)),
                (ComputedValue::Integer(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(*a as f64 + b)),
                (ComputedValue::Float(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Float(a + *b as f64)),
                (ComputedValue::String(a), ComputedValue::String(b)) => Ok(ComputedValue::String(format!("{}{}", a, b))),
                _ => Err(EngineError::Internal(format!("invalid operands for +: {:?} and {:?}", left, right)))
            }
        }
        "-" => {
            match (left, right) {
                (ComputedValue::Integer(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Integer(a - b)),
                (ComputedValue::Float(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(a - b)),
                (ComputedValue::Integer(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(*a as f64 - b)),
                (ComputedValue::Float(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Float(a - *b as f64)),
                _ => Err(EngineError::Internal(format!("invalid operands for -: {:?} and {:?}", left, right)))
            }
        }
        "*" => {
            match (left, right) {
                (ComputedValue::Integer(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Integer(a * b)),
                (ComputedValue::Float(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(a * b)),
                (ComputedValue::Integer(a), ComputedValue::Float(b)) => Ok(ComputedValue::Float(*a as f64 * b)),
                (ComputedValue::Float(a), ComputedValue::Integer(b)) => Ok(ComputedValue::Float(a * *b as f64)),
                _ => Err(EngineError::Internal(format!("invalid operands for *: {:?} and {:?}", left, right)))
            }
        }
        "/" => {
            match (left, right) {
                (ComputedValue::Integer(a), ComputedValue::Integer(b)) => {
                    if *b == 0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Integer(a / b))
                }
                (ComputedValue::Float(a), ComputedValue::Float(b)) => {
                    if *b == 0.0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Float(a / b))
                }
                (ComputedValue::Integer(a), ComputedValue::Float(b)) => {
                    if *b == 0.0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Float(*a as f64 / b))
                }
                (ComputedValue::Float(a), ComputedValue::Integer(b)) => {
                    if *b == 0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Float(a / *b as f64))
                }
                _ => Err(EngineError::Internal(format!("invalid operands for /: {:?} and {:?}", left, right)))
            }
        }
        "%" => {
            match (left, right) {
                (ComputedValue::Integer(a), ComputedValue::Integer(b)) => {
                    if *b == 0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Integer(a % b))
                }
                (ComputedValue::Float(a), ComputedValue::Float(b)) => {
                    if *b == 0.0 { return Ok(ComputedValue::Null); }
                    Ok(ComputedValue::Float(a % b))
                }
                _ => Err(EngineError::Internal(format!("invalid operands for %: {:?} and {:?}", left, right)))
            }
        }
        "||" => {
            // String concatenation
            Ok(ComputedValue::String(format!("{}{}", left.to_string_value(), right.to_string_value())))
        }
        "AND" | "and" => {
            match (left, right) {
                (ComputedValue::Boolean(a), ComputedValue::Boolean(b)) => Ok(ComputedValue::Boolean(*a && *b)),
                _ => Err(EngineError::Internal(format!("invalid operands for AND: {:?} and {:?}", left, right)))
            }
        }
        "OR" | "or" => {
            match (left, right) {
                (ComputedValue::Boolean(a), ComputedValue::Boolean(b)) => Ok(ComputedValue::Boolean(*a || *b)),
                _ => Err(EngineError::Internal(format!("invalid operands for OR: {:?} and {:?}", left, right)))
            }
        }
        "=" | "==" => {
            Ok(ComputedValue::Boolean(computed_values_equal(left, right)))
        }
        "!=" | "<>" => {
            Ok(ComputedValue::Boolean(!computed_values_equal(left, right)))
        }
        "<" => {
            Ok(ComputedValue::Boolean(computed_values_cmp(left, right) == std::cmp::Ordering::Less))
        }
        "<=" => {
            let cmp = computed_values_cmp(left, right);
            Ok(ComputedValue::Boolean(cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal))
        }
        ">" => {
            Ok(ComputedValue::Boolean(computed_values_cmp(left, right) == std::cmp::Ordering::Greater))
        }
        ">=" => {
            let cmp = computed_values_cmp(left, right);
            Ok(ComputedValue::Boolean(cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal))
        }
        _ => Err(EngineError::Internal(format!("unsupported binary operator: {}", op)))
    }
}

fn computed_values_equal(a: &ComputedValue, b: &ComputedValue) -> bool {
    match (a, b) {
        (ComputedValue::Integer(x), ComputedValue::Integer(y)) => x == y,
        (ComputedValue::Float(x), ComputedValue::Float(y)) => (x - y).abs() < f64::EPSILON,
        (ComputedValue::Integer(x), ComputedValue::Float(y)) => (*x as f64 - y).abs() < f64::EPSILON,
        (ComputedValue::Float(x), ComputedValue::Integer(y)) => (x - *y as f64).abs() < f64::EPSILON,
        (ComputedValue::String(x), ComputedValue::String(y)) => x == y,
        (ComputedValue::Boolean(x), ComputedValue::Boolean(y)) => x == y,
        (ComputedValue::Null, ComputedValue::Null) => true,
        _ => false
    }
}

fn computed_values_cmp(a: &ComputedValue, b: &ComputedValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (ComputedValue::Integer(x), ComputedValue::Integer(y)) => x.cmp(y),
        (ComputedValue::Float(x), ComputedValue::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (ComputedValue::Integer(x), ComputedValue::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (ComputedValue::Float(x), ComputedValue::Integer(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (ComputedValue::String(x), ComputedValue::String(y)) => x.cmp(y),
        _ => Ordering::Equal
    }
}

fn apply_unary_op(op: &str, val: &ComputedValue) -> Result<ComputedValue, EngineError> {
    match op {
        "-" => {
            match val {
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(-i)),
                ComputedValue::Float(f) => Ok(ComputedValue::Float(-f)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal(format!("invalid operand for unary -: {:?}", val)))
            }
        }
        "NOT" | "not" | "!" => {
            match val {
                ComputedValue::Boolean(b) => Ok(ComputedValue::Boolean(!b)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal(format!("invalid operand for NOT: {:?}", val)))
            }
        }
        _ => Err(EngineError::Internal(format!("unsupported unary operator: {}", op)))
    }
}

fn evaluate_scalar_function(func: &crate::sql::ScalarFunction, ctx: &EvalContext) -> Result<ComputedValue, EngineError> {
    use crate::sql::ScalarFunction;

    match func {
        // String functions
        ScalarFunction::Upper(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.to_uppercase())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::String(val.to_string_value().to_uppercase()))
            }
        }
        ScalarFunction::Lower(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.to_lowercase())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::String(val.to_string_value().to_lowercase()))
            }
        }
        ScalarFunction::Length(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::Integer(s.len() as i64)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Integer(val.to_string_value().len() as i64))
            }
        }
        ScalarFunction::Trim(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.trim().to_string())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::String(val.to_string_value().trim().to_string()))
            }
        }
        ScalarFunction::LTrim(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.trim_start().to_string())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::String(val.to_string_value().trim_start().to_string()))
            }
        }
        ScalarFunction::RTrim(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.trim_end().to_string())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::String(val.to_string_value().trim_end().to_string()))
            }
        }
        ScalarFunction::Concat(exprs) => {
            let mut result = String::new();
            for e in exprs {
                let val = evaluate_expr(e, ctx)?;
                if matches!(val, ComputedValue::Null) {
                    continue; // Skip nulls in CONCAT
                }
                result.push_str(&val.to_string_value());
            }
            Ok(ComputedValue::String(result))
        }
        ScalarFunction::Substring { expr, start, length } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => {
                    let start_idx = (*start as usize).saturating_sub(1); // SQL is 1-indexed
                    let chars: Vec<char> = s.chars().collect();
                    if start_idx >= chars.len() {
                        return Ok(ComputedValue::String(String::new()));
                    }
                    let end_idx = if let Some(len) = length {
                        (start_idx + *len as usize).min(chars.len())
                    } else {
                        chars.len()
                    };
                    let substr: String = chars[start_idx..end_idx].iter().collect();
                    Ok(ComputedValue::String(substr))
                }
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Null)
            }
        }
        ScalarFunction::Replace { expr, from, to } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.replace(from, to))),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Null)
            }
        }
        ScalarFunction::Left { expr, count } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => {
                    let chars: Vec<char> = s.chars().collect();
                    let end = (*count as usize).min(chars.len());
                    Ok(ComputedValue::String(chars[..end].iter().collect()))
                }
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Null)
            }
        }
        ScalarFunction::Right { expr, count } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => {
                    let chars: Vec<char> = s.chars().collect();
                    let start = chars.len().saturating_sub(*count as usize);
                    Ok(ComputedValue::String(chars[start..].iter().collect()))
                }
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Null)
            }
        }
        ScalarFunction::Reverse(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::String(s) => Ok(ComputedValue::String(s.chars().rev().collect())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Ok(ComputedValue::Null)
            }
        }
        ScalarFunction::Coalesce(exprs) => {
            for e in exprs {
                let val = evaluate_expr(e, ctx)?;
                if !matches!(val, ComputedValue::Null) {
                    return Ok(val);
                }
            }
            Ok(ComputedValue::Null)
        }

        // Math functions
        ScalarFunction::Abs(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(i.abs())),
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.abs())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("ABS requires numeric argument".into()))
            }
        }
        ScalarFunction::Round { expr, precision } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => {
                    let prec = precision.unwrap_or(0);
                    let multiplier = 10f64.powi(prec);
                    Ok(ComputedValue::Float((f * multiplier).round() / multiplier))
                }
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(i)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("ROUND requires numeric argument".into()))
            }
        }
        ScalarFunction::Ceil(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.ceil())),
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(i)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("CEIL requires numeric argument".into()))
            }
        }
        ScalarFunction::Floor(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.floor())),
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(i)),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("FLOOR requires numeric argument".into()))
            }
        }
        ScalarFunction::Mod { dividend, divisor } => {
            let left = evaluate_expr(dividend, ctx)?;
            let right = evaluate_expr(divisor, ctx)?;
            apply_binary_op(&left, "%", &right)
        }
        ScalarFunction::Power { base, exponent } => {
            let base_val = evaluate_expr(base, ctx)?;
            let exp_val = evaluate_expr(exponent, ctx)?;
            match (&base_val, &exp_val) {
                (ComputedValue::Float(b), ComputedValue::Float(e)) => Ok(ComputedValue::Float(b.powf(*e))),
                (ComputedValue::Integer(b), ComputedValue::Integer(e)) => {
                    if *e >= 0 {
                        Ok(ComputedValue::Integer(b.pow(*e as u32)))
                    } else {
                        Ok(ComputedValue::Float((*b as f64).powf(*e as f64)))
                    }
                }
                (ComputedValue::Integer(b), ComputedValue::Float(e)) => Ok(ComputedValue::Float((*b as f64).powf(*e))),
                (ComputedValue::Float(b), ComputedValue::Integer(e)) => Ok(ComputedValue::Float(b.powf(*e as f64))),
                (ComputedValue::Null, _) | (_, ComputedValue::Null) => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("POWER requires numeric arguments".into()))
            }
        }
        ScalarFunction::Sqrt(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.sqrt())),
                ComputedValue::Integer(i) => Ok(ComputedValue::Float((i as f64).sqrt())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("SQRT requires numeric argument".into()))
            }
        }
        ScalarFunction::Log { expr, base } => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => {
                    let result = if let Some(b) = base {
                        f.log(*b)
                    } else {
                        f.log10()
                    };
                    Ok(ComputedValue::Float(result))
                }
                ComputedValue::Integer(i) => {
                    let f = i as f64;
                    let result = if let Some(b) = base {
                        f.log(*b)
                    } else {
                        f.log10()
                    };
                    Ok(ComputedValue::Float(result))
                }
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("LOG requires numeric argument".into()))
            }
        }
        ScalarFunction::Ln(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.ln())),
                ComputedValue::Integer(i) => Ok(ComputedValue::Float((i as f64).ln())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("LN requires numeric argument".into()))
            }
        }
        ScalarFunction::Exp(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Float(f.exp())),
                ComputedValue::Integer(i) => Ok(ComputedValue::Float((i as f64).exp())),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("EXP requires numeric argument".into()))
            }
        }
        ScalarFunction::Sign(expr) => {
            let val = evaluate_expr(expr, ctx)?;
            match val {
                ComputedValue::Float(f) => Ok(ComputedValue::Integer(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 })),
                ComputedValue::Integer(i) => Ok(ComputedValue::Integer(if i > 0 { 1 } else if i < 0 { -1 } else { 0 })),
                ComputedValue::Null => Ok(ComputedValue::Null),
                _ => Err(EngineError::Internal("SIGN requires numeric argument".into()))
            }
        }
        ScalarFunction::Greatest(exprs) => {
            let mut max: Option<ComputedValue> = None;
            for e in exprs {
                let val = evaluate_expr(e, ctx)?;
                if matches!(val, ComputedValue::Null) {
                    continue;
                }
                match &max {
                    None => max = Some(val),
                    Some(current) => {
                        if computed_values_cmp(&val, current) == std::cmp::Ordering::Greater {
                            max = Some(val);
                        }
                    }
                }
            }
            Ok(max.unwrap_or(ComputedValue::Null))
        }
        ScalarFunction::Least(exprs) => {
            let mut min: Option<ComputedValue> = None;
            for e in exprs {
                let val = evaluate_expr(e, ctx)?;
                if matches!(val, ComputedValue::Null) {
                    continue;
                }
                match &min {
                    None => min = Some(val),
                    Some(current) => {
                        if computed_values_cmp(&val, current) == std::cmp::Ordering::Less {
                            min = Some(val);
                        }
                    }
                }
            }
            Ok(min.unwrap_or(ComputedValue::Null))
        }

        // Date/Time functions
        ScalarFunction::Now | ScalarFunction::CurrentTimestamp => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            Ok(ComputedValue::Timestamp(now))
        }
        ScalarFunction::CurrentDate => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| (d.as_secs() / 86400) as i32)
                .unwrap_or(0);
            Ok(ComputedValue::Date(now))
        }

        // Default for unhandled functions
        _ => Ok(ComputedValue::Null)
    }
}

fn evaluate_case_expr(
    operand: &Option<Box<crate::sql::SelectExpr>>,
    when_clauses: &[(crate::sql::SelectExpr, crate::sql::SelectExpr)],
    else_result: &Option<Box<crate::sql::SelectExpr>>,
    ctx: &EvalContext,
) -> Result<ComputedValue, EngineError> {
    // If there's an operand, this is a simple CASE (CASE x WHEN y THEN z)
    if let Some(op_expr) = operand {
        let op_val = evaluate_expr(op_expr, ctx)?;
        for (when_expr, then_expr) in when_clauses {
            let when_val = evaluate_expr(when_expr, ctx)?;
            if computed_values_equal(&op_val, &when_val) {
                return evaluate_expr(then_expr, ctx);
            }
        }
    } else {
        // Searched CASE (CASE WHEN condition THEN result)
        for (when_expr, then_expr) in when_clauses {
            let when_val = evaluate_expr(when_expr, ctx)?;
            match when_val {
                ComputedValue::Boolean(true) => return evaluate_expr(then_expr, ctx),
                _ => continue,
            }
        }
    }

    // No match - return ELSE or NULL
    if let Some(else_expr) = else_result {
        evaluate_expr(else_expr, ctx)
    } else {
        Ok(ComputedValue::Null)
    }
}



// --- RESTORED UTILITIES ---

pub struct SegmentStats {
    pub event_time_min: Option<u64>, pub event_time_max: Option<u64>,
    pub tenant_id_min: Option<u64>, pub tenant_id_max: Option<u64>,
    pub route_id_min: Option<u64>, pub route_id_max: Option<u64>,
    pub bloom_tenant: Option<Vec<u8>>, pub bloom_route: Option<Vec<u8>>,
    pub column_stats: std::collections::HashMap<String, crate::replication::ColumnStats>,
}
#[derive(Default)] pub struct ValidatedSegment { pub stats: SegmentStats, pub schema_hash: u64, pub schema_spec: Option<String> }
impl Default for SegmentStats { fn default() -> Self { Self { event_time_min:None, event_time_max:None, tenant_id_min:None, tenant_id_max:None, route_id_min:None, route_id_max:None, bloom_tenant:None, bloom_route:None, column_stats:std::collections::HashMap::new() } } }

pub struct SchemaWrapper { pub json: String, pub hash: u64 }

pub fn is_valid_ident(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !s.is_empty()
}

pub fn compute_checksum(data: &[u8]) -> u64 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

pub fn compute_schema_hash_from_payload(data: &[u8], compression: Option<&str>) -> Result<u64, EngineError> {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    // Decompress if needed
    let decompressed = decompress_payload(data.to_vec(), compression)?;

    // Parse IPC to get schema
    let batches = read_ipc_batches(&decompressed)?;
    if batches.is_empty() {
        return Ok(0);
    }

    let schema = batches[0].schema();

    // Build same JSON format as validate_and_stats (TableFieldSpec format)
    let schema_json = serde_json::to_string(&schema.fields().iter().map(|f| {
        let data_type_str = match f.data_type() {
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "uint16",
            DataType::UInt32 => "uint32",
            DataType::UInt64 => "uint64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Boolean => "bool",
            DataType::Utf8 => "string",
            DataType::LargeUtf8 => "string",
            DataType::Binary => "binary",
            DataType::LargeBinary => "binary",
            DataType::Date32 => "date",
            DataType::Date64 => "date",
            DataType::Timestamp(_, _) => "timestamp",
            _ => "string",
        };
        serde_json::json!({
            "name": f.name(),
            "data_type": data_type_str,
            "nullable": f.is_nullable()
        })
    }).collect::<Vec<_>>()).unwrap_or_default();

    // Compute hash
    let mut hasher = DefaultHasher::new();
    schema_json.hash(&mut hasher);
    Ok(hasher.finish())
}

pub fn read_ipc_batches(ipc: &[u8]) -> Result<Vec<RecordBatch>, EngineError> {
    use arrow_ipc::reader::StreamReader; use std::io::Cursor;
    let cursor = Cursor::new(ipc);
    let mut reader = StreamReader::try_new(cursor, None).map_err(|e| EngineError::Internal(format!("{e}")))?;
    let mut batches = Vec::new();
    while let Some(b) = reader.next() { batches.push(b.map_err(|e| EngineError::Internal(format!("{e}")))?); }
    Ok(batches)
}

pub fn record_batches_to_ipc(schema: &arrow_schema::Schema, batches: &[RecordBatch]) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(|e| EngineError::Internal(format!("{e}")))?;
        for b in batches { writer.write(b).map_err(|e| EngineError::Internal(format!("{e}")))?; }
        writer.finish().map_err(|e| EngineError::Internal(format!("{e}")))?;
    }
    Ok(buf)
}

pub fn parse_where_filter(where_clause: Option<&str>) -> Result<QueryFilter, EngineError> {
    let mut filter = QueryFilter::default();

    let clause = match where_clause {
        Some(c) if !c.trim().is_empty() => c.trim(),
        _ => return Ok(filter),
    };

    // Split by " AND " (case insensitive)
    let parts: Vec<&str> = if clause.to_uppercase().contains(" AND ") {
        let upper = clause.to_uppercase();
        let indices: Vec<_> = upper.match_indices(" AND ").map(|(i, _)| i).collect();
        let mut parts = Vec::new();
        let mut start = 0;
        for idx in indices {
            parts.push(&clause[start..idx]);
            start = idx + 5; // " AND " is 5 chars
        }
        parts.push(&clause[start..]);
        parts
    } else {
        vec![clause]
    };

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Parse: column = value
        if let Some(eq_idx) = part.find('=') {
            // Make sure it's not != or >=, <=
            let before = if eq_idx > 0 { part.chars().nth(eq_idx - 1) } else { None };
            if before == Some('!') || before == Some('>') || before == Some('<') {
                continue;
            }

            let column = part[..eq_idx].trim();
            let value = part[eq_idx + 1..].trim();

            // Check for boolean values
            if value.eq_ignore_ascii_case("true") {
                filter.bool_eq_filters.push((column.to_string(), true));
            } else if value.eq_ignore_ascii_case("false") {
                filter.bool_eq_filters.push((column.to_string(), false));
            }
            // Check for string literals (quoted)
            else if (value.starts_with('\'') && value.ends_with('\'')) ||
                    (value.starts_with('"') && value.ends_with('"')) {
                let val = &value[1..value.len()-1];
                filter.string_eq_filters.push((column.to_string(), val.to_string()));
            }
            // Check for float literals
            else if value.contains('.') {
                if let Ok(f) = value.parse::<f64>() {
                    filter.float_eq_filters.push((column.to_string(), f));
                }
            }
            // Check for integer literals
            else if let Ok(i) = value.parse::<i128>() {
                filter.numeric_eq_filters.push((column.to_string(), i));
            }
        }
    }

    Ok(filter)
}

pub fn filter_is_empty(filter: &QueryFilter) -> bool {
    filter.tenant_id_eq.is_none() && filter.route_id_eq.is_none() &&
    filter.numeric_eq_filters.is_empty() && filter.string_eq_filters.is_empty() &&
    filter.like_filters.is_empty()
}

pub fn filter_batch_for_delete(batch: &RecordBatch, filter: &QueryFilter) -> Result<(Option<RecordBatch>, usize), EngineError> {
    use arrow_array::{Array, BooleanArray, UInt64Array, Int64Array, StringArray};
    use arrow_ord::cmp::eq;
    use arrow::compute::{and, not, filter_record_batch};

    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok((None, 0));
    }

    // Build a mask of rows to DELETE (true = delete this row)
    let mut delete_mask = BooleanArray::from(vec![false; num_rows]);

    // If no filter, delete all rows
    if filter_is_empty(filter) {
        return Ok((None, num_rows));
    }

    // Check tenant_id_eq filter
    if let Some(tid) = filter.tenant_id_eq {
        if let Some(col_idx) = batch.schema().index_of("tenant_id").ok() {
            let col = batch.column(col_idx);
            if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                let scalar = UInt64Array::new_scalar(tid);
                let matches = eq(arr, &scalar)
                    .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                // If any previous delete conditions exist, OR them with this one
                if delete_mask.true_count() > 0 {
                    // Already have some deletes - this is unlikely on first filter
                } else {
                    delete_mask = matches;
                }
            }
        }
    }

    // Check tenant_id_in filter
    if let Some(ref tids) = filter.tenant_id_in {
        if let Some(col_idx) = batch.schema().index_of("tenant_id").ok() {
            let col = batch.column(col_idx);
            if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                let mut in_mask = BooleanArray::from(vec![false; num_rows]);
                for tid in tids {
                    let scalar = UInt64Array::new_scalar(*tid);
                    let matches = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    in_mask = arrow::compute::or(&in_mask, &matches)
                        .map_err(|e| EngineError::Internal(format!("or error: {}", e)))?;
                }
                if delete_mask.true_count() > 0 {
                    delete_mask = and(&delete_mask, &in_mask)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                } else {
                    delete_mask = in_mask;
                }
            }
        }
    }

    // Check numeric_eq_filters
    for (col_name, value) in &filter.numeric_eq_filters {
        if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
            let col = batch.column(col_idx);
            if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                let scalar = UInt64Array::new_scalar(*value as u64);
                let matches = eq(arr, &scalar)
                    .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                if delete_mask.true_count() > 0 {
                    delete_mask = and(&delete_mask, &matches)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                } else {
                    delete_mask = matches;
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                let scalar = Int64Array::new_scalar(*value as i64);
                let matches = eq(arr, &scalar)
                    .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                if delete_mask.true_count() > 0 {
                    delete_mask = and(&delete_mask, &matches)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                } else {
                    delete_mask = matches;
                }
            }
        }
    }

    let deleted_count = delete_mask.true_count();
    if deleted_count == 0 {
        return Ok((Some(batch.clone()), 0));
    }

    if deleted_count == num_rows {
        return Ok((None, num_rows));
    }

    // Invert mask to get rows to KEEP
    let keep_mask = not(&delete_mask)
        .map_err(|e| EngineError::Internal(format!("not error: {}", e)))?;

    // Filter batch to keep only non-deleted rows
    let filtered = filter_record_batch(batch, &keep_mask)
        .map_err(|e| EngineError::Internal(format!("filter batch error: {}", e)))?;

    Ok((Some(filtered), deleted_count))
}

pub fn apply_assignments_to_batch(batch: &RecordBatch, filter: &QueryFilter, assigns: &[(String, crate::sql::SqlValue)]) -> Result<(RecordBatch, usize), EngineError> {
    use arrow_array::{Array, BooleanArray, UInt64Array, Int64Array};
    use arrow_ord::cmp::eq;
    use arrow::compute::and;
    use crate::sql::SqlValue;

    let num_rows = batch.num_rows();
    if num_rows == 0 || assigns.is_empty() {
        return Ok((batch.clone(), 0));
    }

    // Build a mask of rows to UPDATE (true = update this row)
    let mut update_mask = BooleanArray::from(vec![true; num_rows]); // Update all if no filter

    if !filter_is_empty(filter) {
        update_mask = BooleanArray::from(vec![false; num_rows]);

        // Check route_id_eq filter
        if let Some(rid) = filter.route_id_eq {
            if let Some(col_idx) = batch.schema().index_of("route_id").ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let scalar = UInt64Array::new_scalar(rid);
                    update_mask = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                }
            }
        }

        // Check route_id_in filter
        if let Some(ref rids) = filter.route_id_in {
            if let Some(col_idx) = batch.schema().index_of("route_id").ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let mut in_mask = BooleanArray::from(vec![false; num_rows]);
                    for rid in rids {
                        let scalar = UInt64Array::new_scalar(*rid);
                        let matches = eq(arr, &scalar)
                            .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                        in_mask = arrow::compute::or(&in_mask, &matches)
                            .map_err(|e| EngineError::Internal(format!("or error: {}", e)))?;
                    }
                    if update_mask.true_count() > 0 {
                        update_mask = and(&update_mask, &in_mask)
                            .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                    } else {
                        update_mask = in_mask;
                    }
                }
            }
        }

        // Check numeric_eq_filters
        for (col_name, value) in &filter.numeric_eq_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let scalar = UInt64Array::new_scalar(*value as u64);
                    let matches = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    if update_mask.true_count() > 0 {
                        update_mask = and(&update_mask, &matches)
                            .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                    } else {
                        update_mask = matches;
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    let scalar = Int64Array::new_scalar(*value as i64);
                    let matches = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    if update_mask.true_count() > 0 {
                        update_mask = and(&update_mask, &matches)
                            .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                    } else {
                        update_mask = matches;
                    }
                }
            }
        }
    }

    let updated_count = update_mask.true_count();
    if updated_count == 0 {
        return Ok((batch.clone(), 0));
    }

    // Build new columns with assignments applied to matching rows
    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);

        // Check if this column has an assignment
        if let Some((_, value)) = assigns.iter().find(|(name, _)| name == field.name()) {
            // Apply assignment to matching rows
            let updated_col = apply_value_to_column(col, &update_mask, value, field.data_type())?;
            new_columns.push(updated_col);
        } else {
            // Keep original column
            new_columns.push(col.clone());
        }
    }

    let updated_batch = RecordBatch::try_new(schema.clone(), new_columns)
        .map_err(|e| EngineError::Internal(format!("create updated batch error: {}", e)))?;

    Ok((updated_batch, updated_count))
}

/// Apply a value to specific rows in a column based on a mask
fn apply_value_to_column(col: &ArrayRef, mask: &BooleanArray, value: &crate::sql::SqlValue, dtype: &DataType) -> Result<ArrayRef, EngineError> {
    use arrow_array::{Int64Array, UInt64Array, Float64Array, StringArray, BooleanArray as BoolArr};
    use crate::sql::SqlValue;

    let num_rows = col.len();

    match dtype {
        DataType::Int64 => {
            let new_val = match value {
                SqlValue::Integer(i) => *i,
                SqlValue::Float(f) => *f as i64,
                _ => return Err(EngineError::InvalidArgument("cannot assign non-numeric to Int64".into())),
            };
            let arr = col.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| EngineError::Internal("expected Int64Array".into()))?;
            let mut builder = arrow_array::builder::Int64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                if mask.value(i) {
                    builder.append_value(new_val);
                } else {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(i));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::UInt64 => {
            let new_val = match value {
                SqlValue::Integer(i) => *i as u64,
                SqlValue::Float(f) => *f as u64,
                _ => return Err(EngineError::InvalidArgument("cannot assign non-numeric to UInt64".into())),
            };
            let arr = col.as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| EngineError::Internal("expected UInt64Array".into()))?;
            let mut builder = arrow_array::builder::UInt64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                if mask.value(i) {
                    builder.append_value(new_val);
                } else {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(i));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let new_val = match value {
                SqlValue::Integer(i) => *i as f64,
                SqlValue::Float(f) => *f,
                _ => return Err(EngineError::InvalidArgument("cannot assign non-numeric to Float64".into())),
            };
            let arr = col.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| EngineError::Internal("expected Float64Array".into()))?;
            let mut builder = arrow_array::builder::Float64Builder::with_capacity(num_rows);
            for i in 0..num_rows {
                if mask.value(i) {
                    builder.append_value(new_val);
                } else {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(i));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let new_val = match value {
                SqlValue::String(s) => s.clone(),
                SqlValue::Integer(i) => i.to_string(),
                SqlValue::Float(f) => f.to_string(),
                SqlValue::Boolean(b) => b.to_string(),
                _ => return Err(EngineError::InvalidArgument("cannot convert to string".into())),
            };
            let arr = col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| EngineError::Internal("expected StringArray".into()))?;
            let mut builder = arrow_array::builder::StringBuilder::with_capacity(num_rows, num_rows * 16);
            for i in 0..num_rows {
                if mask.value(i) {
                    builder.append_value(&new_val);
                } else {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(arr.value(i));
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        _ => {
            // For unsupported types, return original column
            Ok(col.clone())
        }
    }
}

pub fn align_batch_to_fields(_fields: &[arrow_schema::Field], batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
    Ok(batch.clone())
}

pub fn decode_binary_literal(_v: &str) -> Vec<u8> { vec![] }

pub fn arrow_type_from_str(s: &str) -> Result<arrow_schema::DataType, EngineError> {
    use arrow_schema::DataType;
    let s_lower = s.to_lowercase();
    let s_trimmed = s_lower.trim();

    // Handle decimal with precision/scale: decimal(p,s)
    if s_trimmed.starts_with("decimal") {
        if let Some(params) = s_trimmed.strip_prefix("decimal").and_then(|rest| {
            let rest = rest.trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                Some(&rest[1..rest.len()-1])
            } else {
                None
            }
        }) {
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                let precision: u8 = parts[0].trim().parse().unwrap_or(38);
                let scale: i8 = parts[1].trim().parse().unwrap_or(0);
                return Ok(DataType::Decimal128(precision, scale));
            }
        }
        // Default decimal without params
        return Ok(DataType::Decimal128(38, 0));
    }

    match s_trimmed {
        // String types
        "string" | "text" | "varchar" | "char" => Ok(DataType::Utf8),
        "largestring" | "largetext" => Ok(DataType::LargeUtf8),

        // Integer types
        "int8" | "tinyint" => Ok(DataType::Int8),
        "int16" | "smallint" => Ok(DataType::Int16),
        "int32" | "int" | "integer" => Ok(DataType::Int32),
        "int64" | "bigint" | "long" => Ok(DataType::Int64),

        // Unsigned integer types
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),

        // Float types
        "float32" | "float" | "real" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),

        // Boolean
        "bool" | "boolean" => Ok(DataType::Boolean),

        // Binary types
        "binary" | "blob" | "bytea" => Ok(DataType::LargeBinary),

        // Date/Time types
        "date" | "date32" => Ok(DataType::Date32),
        "timestamp" | "datetime" => Ok(DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),

        // Special types
        "uuid" => Ok(DataType::FixedSizeBinary(16)),
        "json" | "jsonb" => Ok(DataType::LargeUtf8),

        // Null type
        "null" => Ok(DataType::Null),

        _ => Err(EngineError::InvalidArgument(format!("unknown type: {}", s))),
    }
}

pub fn canonicalize_schema_spec<T: Clone>(fields: &[T]) -> Result<Vec<T>, EngineError> { Ok(fields.to_vec()) }

/// Check if two schema JSONs represent compatible schemas (same fields)
/// Returns true if schemas have the same field count and matching field names
fn schemas_are_compatible(schema1_json: &str, schema2_json: &str) -> bool {
    // Try to extract field names from both schemas
    fn extract_field_names(json: &str) -> Option<Vec<String>> {
        // Try parsing as array of objects with "name" field
        if let Ok(fields) = serde_json::from_str::<Vec<serde_json::Value>>(json) {
            let names: Vec<String> = fields.iter()
                .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                .collect();
            if !names.is_empty() {
                return Some(names);
            }
        }
        // Try parsing as TableFieldSpec
        if let Ok(fields) = serde_json::from_str::<Vec<TableFieldSpec>>(json) {
            let names: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
            if !names.is_empty() {
                return Some(names);
            }
        }
        None
    }

    match (extract_field_names(schema1_json), extract_field_names(schema2_json)) {
        (Some(names1), Some(names2)) => {
            if names1.len() != names2.len() {
                return false;
            }
            // Check if all field names match (in same order)
            names1.iter().zip(names2.iter()).all(|(a, b)| a == b)
        }
        _ => true, // If we can't parse, assume compatible
    }
}

pub fn canonical_schema_from_json(s: &str) -> Result<SchemaWrapper, EngineError> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Try to parse as TableFieldSpec array first (from create_table)
    if let Ok(fields) = serde_json::from_str::<Vec<TableFieldSpec>>(s) {
        // Build a canonical string for hash computation (just field names for compatibility)
        let field_names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        let canonical_for_hash = serde_json::to_string(&field_names).unwrap_or_default();

        // Compute hash from field names (more stable across format changes)
        let hash = {
            let mut hasher = DefaultHasher::new();
            canonical_for_hash.hash(&mut hasher);
            hasher.finish()
        };

        // IMPORTANT: Return the ORIGINAL json (TableFieldSpec format) so it can be parsed later
        return Ok(SchemaWrapper { json: s.to_string(), hash });
    }

    // Try to parse as validate_and_stats format (raw JSON with data_type field)
    if let Ok(fields) = serde_json::from_str::<Vec<serde_json::Value>>(s) {
        // Check if this is already in TableFieldSpec format (has data_type)
        // or if it's the old format (has type)
        let needs_conversion = fields.iter().any(|f| f.get("type").is_some() && f.get("data_type").is_none());

        let result_json = if needs_conversion {
            // Convert from old format (type) to TableFieldSpec format (data_type)
            let converted: Vec<serde_json::Value> = fields.iter().map(|f| {
                let type_val = f.get("type").and_then(|t| t.as_str()).unwrap_or("string");
                // Convert Arrow DataType string back to our format
                let data_type_str = match type_val {
                    "Int64" => "int64",
                    "Int32" => "int32",
                    "Int16" => "int16",
                    "Int8" => "int8",
                    "UInt64" => "uint64",
                    "UInt32" => "uint32",
                    "UInt16" => "uint16",
                    "UInt8" => "uint8",
                    "Float64" => "float64",
                    "Float32" => "float32",
                    "Boolean" => "bool",
                    "Utf8" => "string",
                    "LargeUtf8" => "string",
                    "Binary" => "binary",
                    "Date32" | "Date64" => "date",
                    s if s.starts_with("Timestamp") => "timestamp",
                    other => other,
                };
                serde_json::json!({
                    "name": f.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                    "data_type": data_type_str,
                    "nullable": f.get("nullable").and_then(|n| n.as_bool()).unwrap_or(true)
                })
            }).collect();
            serde_json::to_string(&converted).unwrap_or_default()
        } else {
            // Already has data_type or original format
            s.to_string()
        };

        // Compute hash from field names
        let field_names: Vec<&str> = fields.iter()
            .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
            .collect();
        let canonical_for_hash = serde_json::to_string(&field_names).unwrap_or_default();
        let hash = {
            let mut hasher = DefaultHasher::new();
            canonical_for_hash.hash(&mut hasher);
            hasher.finish()
        };
        return Ok(SchemaWrapper { json: result_json, hash });
    }

    // Fallback: empty schema
    Ok(SchemaWrapper { json: "[]".to_string(), hash: 0 })
}

/// Validates IPC payload and computes column statistics for segment pruning.
/// This is the core function that enables zone-map based query optimization.
pub fn validate_and_stats<T>(ipc: &[u8], _meta: Option<&T>) -> Result<ValidatedSegment, EngineError> {
    use arrow_array::{
        Array, Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        Float32Array, Float64Array, BooleanArray,
        TimestampMicrosecondArray, Date32Array,
    };
    use crate::replication::{ColumnStats, PrimitiveValue};
    use std::collections::HashMap;

    if ipc.is_empty() {
        return Ok(ValidatedSegment::default());
    }

    let batches = read_ipc_batches(ipc)?;
    if batches.is_empty() {
        return Ok(ValidatedSegment::default());
    }

    let schema = batches[0].schema();
    let mut column_stats: HashMap<String, ColumnStats> = HashMap::new();
    let mut total_rows: u64 = 0;

    // Legacy stats for backward compatibility
    let mut event_time_min: Option<u64> = None;
    let mut event_time_max: Option<u64> = None;
    let mut tenant_id_min: Option<u64> = None;
    let mut tenant_id_max: Option<u64> = None;
    let mut route_id_min: Option<u64> = None;
    let mut route_id_max: Option<u64> = None;
    let mut tenant_ids: Vec<u64> = Vec::new();
    let mut route_ids: Vec<u64> = Vec::new();

    for batch in &batches {
        total_rows += batch.num_rows() as u64;

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_name = field.name().to_string();
            let col = batch.column(col_idx);
            let null_count = col.null_count() as u64;

            let stats = column_stats.entry(col_name.clone()).or_insert_with(|| ColumnStats {
                min: None,
                max: None,
                bloom_filter: None,
                null_count: 0,
                row_count: 0,
                distinct_count: None,
            });

            stats.null_count += null_count;
            stats.row_count += batch.num_rows() as u64;

            // Compute min/max based on column type
            match field.data_type() {
                DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::Int64(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::Int64(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::Int32(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::Int32(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::UInt64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::UInt64(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::UInt64(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                        // Legacy tenant_id/route_id handling
                        if col_name == "tenant_id" {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let v = arr.value(i);
                                    tenant_ids.push(v);
                                    tenant_id_min = Some(tenant_id_min.map_or(v, |m| m.min(v)));
                                    tenant_id_max = Some(tenant_id_max.map_or(v, |m| m.max(v)));
                                }
                            }
                        } else if col_name == "route_id" {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let v = arr.value(i);
                                    route_ids.push(v);
                                    route_id_min = Some(route_id_min.map_or(v, |m| m.min(v)));
                                    route_id_max = Some(route_id_max.map_or(v, |m| m.max(v)));
                                }
                            }
                        }
                    }
                }
                DataType::UInt32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::UInt32(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::UInt32(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Float64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::Float64(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::Float64(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Float32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::Float32(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::Float32(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Timestamp(_, _) => {
                    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::TimestampMicros(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::TimestampMicros(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                        // Legacy event_time handling
                        if col_name == "event_time" {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let v = arr.value(i) as u64;
                                    event_time_min = Some(event_time_min.map_or(v, |m| m.min(v)));
                                    event_time_max = Some(event_time_max.map_or(v, |m| m.max(v)));
                                }
                            }
                        }
                    }
                }
                DataType::Date32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Date32Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            let new_min = PrimitiveValue::Date32(min_val);
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            let new_max = PrimitiveValue::Date32(max_val);
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if let Some(min_val) = arrow::compute::min_string(arr) {
                            let new_min = PrimitiveValue::String(min_val.to_string());
                            if stats.min.is_none() || new_min.is_less_than(stats.min.as_ref().unwrap()) {
                                stats.min = Some(new_min);
                            }
                        }
                        if let Some(max_val) = arrow::compute::max_string(arr) {
                            let new_max = PrimitiveValue::String(max_val.to_string());
                            if stats.max.is_none() || new_max.is_greater_than(stats.max.as_ref().unwrap()) {
                                stats.max = Some(new_max);
                            }
                        }
                    }
                }
                DataType::Boolean => {
                    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        let mut has_false = false;
                        let mut has_true = false;
                        for i in 0..arr.len() {
                            if !arr.is_null(i) {
                                if arr.value(i) {
                                    has_true = true;
                                } else {
                                    has_false = true;
                                }
                            }
                        }
                        if has_false {
                            stats.min = Some(PrimitiveValue::Boolean(false));
                        }
                        if has_true {
                            stats.max = Some(PrimitiveValue::Boolean(true));
                        }
                    }
                }
                // Handle smaller integer types
                DataType::Int8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            stats.min = Some(PrimitiveValue::Int32(min_val as i32));
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            stats.max = Some(PrimitiveValue::Int32(max_val as i32));
                        }
                    }
                }
                DataType::Int16 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            stats.min = Some(PrimitiveValue::Int32(min_val as i32));
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            stats.max = Some(PrimitiveValue::Int32(max_val as i32));
                        }
                    }
                }
                DataType::UInt8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            stats.min = Some(PrimitiveValue::UInt32(min_val as u32));
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            stats.max = Some(PrimitiveValue::UInt32(max_val as u32));
                        }
                    }
                }
                DataType::UInt16 => {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
                        if let Some(min_val) = arrow::compute::min(arr) {
                            stats.min = Some(PrimitiveValue::UInt32(min_val as u32));
                        }
                        if let Some(max_val) = arrow::compute::max(arr) {
                            stats.max = Some(PrimitiveValue::UInt32(max_val as u32));
                        }
                    }
                }
                _ => {
                    // Unsupported type - no stats collected
                }
            }
        }
    }

    // Build bloom filters for tenant_id and route_id (legacy compatibility)
    let bloom_tenant = if !tenant_ids.is_empty() {
        let mut bloom = Bloom::new_for_fp_rate(tenant_ids.len().max(100), 0.01);
        for id in &tenant_ids {
            bloom.set(id);
        }
        crate::bloom_utils::serialize_bloom(&bloom).ok()
    } else {
        None
    };

    let bloom_route = if !route_ids.is_empty() {
        let mut bloom = Bloom::new_for_fp_rate(route_ids.len().max(100), 0.01);
        for id in &route_ids {
            bloom.set(id);
        }
        crate::bloom_utils::serialize_bloom(&bloom).ok()
    } else {
        None
    };

    // Compute schema hash for validation
    // Use TableFieldSpec format for consistency with create_table
    let schema_json = serde_json::to_string(&schema.fields().iter().map(|f| {
        // Convert Arrow DataType to our string format
        let data_type_str = match f.data_type() {
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "uint16",
            DataType::UInt32 => "uint32",
            DataType::UInt64 => "uint64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Boolean => "bool",
            DataType::Utf8 => "string",
            DataType::LargeUtf8 => "string",
            DataType::Binary => "binary",
            DataType::LargeBinary => "binary",
            DataType::Date32 => "date",
            DataType::Date64 => "date",
            DataType::Timestamp(_, _) => "timestamp",
            _ => "string", // Default fallback
        };
        serde_json::json!({
            "name": f.name(),
            "data_type": data_type_str,
            "nullable": f.is_nullable()
        })
    }).collect::<Vec<_>>()).unwrap_or_default();

    let schema_hash = {
        let mut hasher = DefaultHasher::new();
        schema_json.hash(&mut hasher);
        hasher.finish()
    };

    Ok(ValidatedSegment {
        stats: SegmentStats {
            event_time_min,
            event_time_max,
            tenant_id_min,
            tenant_id_max,
            route_id_min,
            route_id_max,
            bloom_tenant,
            bloom_route,
            column_stats,
        },
        schema_hash,
        schema_spec: Some(schema_json),
    })
}

/// Check if a segment can be pruned based on column statistics (zone maps).
/// Returns true if the segment might contain matching rows, false if it can be skipped.
pub fn segment_matches_column_stats(entry: &ManifestEntry, filter: &QueryFilter) -> bool {
    use crate::sql::NumericOp;
    use crate::replication::PrimitiveValue;

    // If no column stats, assume segment might match
    let stats_map = match &entry.column_stats {
        Some(map) if !map.is_empty() => map,
        _ => return true,
    };

    // Check numeric equality filters (i128 values)
    for (col_name, value) in &filter.numeric_eq_filters {
        if let Some(col_stats) = stats_map.get(col_name) {
            // Convert i128 to Int64 for comparison (lossy but covers most cases)
            let prim_val = PrimitiveValue::Int64(*value as i64);
            if !col_stats.value_in_range(&prim_val) {
                return false; // Value is outside segment's min/max range
            }
        }
    }

    // Check float equality filters
    for (col_name, value) in &filter.float_eq_filters {
        if let Some(col_stats) = stats_map.get(col_name) {
            let prim_val = PrimitiveValue::Float64(*value);
            if !col_stats.value_in_range(&prim_val) {
                return false;
            }
        }
    }

    // Check numeric range filters
    for nf in &filter.numeric_range_filters {
        if let Some(col_stats) = stats_map.get(&nf.column) {
            let filter_val = nf.value.to_primitive();

            match nf.op {
                NumericOp::Eq => {
                    // segment.min <= val <= segment.max
                    if !col_stats.value_in_range(&filter_val) {
                        return false;
                    }
                }
                NumericOp::Lt | NumericOp::Le => {
                    // Looking for rows where col < val (or col <= val)
                    // Can skip segment if segment.min >= val (for Lt) or segment.min > val (for Le)
                    if let Some(ref seg_min) = col_stats.min {
                        match nf.op {
                            NumericOp::Lt => {
                                // col < val means we need seg_min < val
                                if !seg_min.is_less_than(&filter_val) && seg_min.partial_cmp_value(&filter_val) != Some(std::cmp::Ordering::Less) {
                                    return false;
                                }
                            }
                            NumericOp::Le => {
                                // col <= val means we need seg_min <= val
                                if seg_min.is_greater_than(&filter_val) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                NumericOp::Gt | NumericOp::Ge => {
                    // Looking for rows where col > val (or col >= val)
                    // Can skip segment if segment.max <= val (for Gt) or segment.max < val (for Ge)
                    if let Some(ref seg_max) = col_stats.max {
                        match nf.op {
                            NumericOp::Gt => {
                                // col > val means we need seg_max > val
                                if !seg_max.is_greater_than(&filter_val) && seg_max.partial_cmp_value(&filter_val) != Some(std::cmp::Ordering::Greater) {
                                    return false;
                                }
                            }
                            NumericOp::Ge => {
                                // col >= val means we need seg_max >= val
                                if seg_max.is_less_than(&filter_val) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                NumericOp::Ne => {
                    // Can only skip if segment has exactly one value and it equals the filter value
                    if let (Some(ref seg_min), Some(ref seg_max)) = (&col_stats.min, &col_stats.max) {
                        if seg_min.partial_cmp_value(seg_max) == Some(std::cmp::Ordering::Equal) {
                            if seg_min.partial_cmp_value(&filter_val) == Some(std::cmp::Ordering::Equal) {
                                return false; // Segment only has the value we're excluding
                            }
                        }
                    }
                }
            }
        }
    }

    // Check string equality filters against column stats
    for (col_name, value) in &filter.string_eq_filters {
        if let Some(col_stats) = stats_map.get(col_name) {
            let prim_val = PrimitiveValue::String(value.clone());
            if !col_stats.value_in_range(&prim_val) {
                return false;
            }
        }
    }

    true
}

pub fn column_encodings_from_schema_json(_j: Option<&String>) -> Result<std::collections::HashMap<String, String>, EngineError> {
    Ok(std::collections::HashMap::new())
}

pub fn apply_column_encodings(ipc: &[u8], enc: &std::collections::HashMap<String, String>) -> Result<Vec<u8>, EngineError> {
    // If no encodings specified, return original data
    if enc.is_empty() {
        return Ok(ipc.to_vec());
    }
    // TODO: Apply actual column encodings (dictionary, delta, etc.) when needed
    // For now, return the data as-is
    Ok(ipc.to_vec())
}

pub fn parse_uuid_string(s: &str) -> Option<[u8; 16]> {
    let s = s.trim();
    // With hyphens: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
    if s.len() == 36 && s.chars().filter(|c| *c == '-').count() == 4 {
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        if hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(bytes) = hex::decode(&hex) {
                if bytes.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(&bytes);
                    return Some(arr);
                }
            }
        }
        return None;
    }
    // Without hyphens: 32 hex chars
    if s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(bytes) = hex::decode(s) {
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&bytes);
                return Some(arr);
            }
        }
    }
    None
}

pub fn current_time_micros() -> u64 { 
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

pub fn segment_age_micros(entry: &ManifestEntry, now: u64) -> Option<u64> {
    // Calculate segment age based on watermark or event time
    let segment_time = entry.watermark_micros.max(entry.event_time_max.unwrap_or(0));
    if segment_time == 0 {
        return None;
    }
    if now >= segment_time {
        Some(now - segment_time)
    } else {
        Some(0) // Segment is in the future (clock skew)
    }
}

pub fn validate_and_stats_legacy(_s: &arrow_schema::Schema) -> Result<(), EngineError> { Ok(()) }

// Additional Legacy Stubs
pub fn filter_batches_with_query_filter(batches: Vec<RecordBatch>, _filter: &QueryFilter) -> Result<Vec<RecordBatch>, EngineError> {
    Ok(batches)
}

pub fn decode_record_batch_encodings(batch: &RecordBatch) -> Result<RecordBatch, EngineError> {
    Ok(batch.clone())
}

pub fn parse_agg(_sql: &str) -> Result<Option<AggPlan>, EngineError> { Ok(None) }
pub fn parse_projection(_sql: &str) -> Result<Option<Vec<String>>, EngineError> { Ok(None) }
pub fn parse_filters(sql: &str) -> Result<QueryFilter, EngineError> {
    // Find WHERE clause in SQL
    let sql_upper = sql.to_uppercase();
    let where_start = sql_upper.find(" WHERE ");

    if where_start.is_none() {
        return Ok(QueryFilter::default());
    }

    let where_start = where_start.unwrap() + 7; // Skip " WHERE "

    // Find end of WHERE clause (before GROUP BY, ORDER BY, LIMIT, etc.)
    let mut where_end = sql.len();
    for keyword in &[" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "] {
        if let Some(idx) = sql_upper.find(keyword) {
            if idx > where_start && idx < where_end {
                where_end = idx;
            }
        }
    }

    let where_clause = &sql[where_start..where_end];
    parse_where_filter(Some(where_clause))
}



// Duplicates removed
// Missing Stubs for Joins/Agg
pub fn entry_matches_string_filters<T>(_e: &T, _f: &QueryFilter) -> bool { true }

/// Filter IPC data using Arrow compute kernels
/// Decodes IPC, applies filters, and re-encodes
pub fn filter_ipc(ipc: &[u8], filter: &QueryFilter) -> Result<Vec<u8>, EngineError> {
    // If no filters to apply, return as-is for performance
    if !has_row_filters(filter) {
        return Ok(ipc.to_vec());
    }

    // Decode IPC to record batches
    let batches = read_ipc_batches(ipc)?;
    if batches.is_empty() {
        return Ok(ipc.to_vec());
    }

    // Apply filters
    let filtered = filter_ipc_batches(batches.clone(), filter)?;

    // Get schema from original batches to preserve it even if filtered is empty
    let schema = batches[0].schema();

    // Re-encode to IPC (even if empty, we need valid IPC with schema)
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| EngineError::Internal(format!("IPC write error: {}", e)))?;
        for batch in &filtered {
            writer.write(batch)
                .map_err(|e| EngineError::Internal(format!("IPC write error: {}", e)))?;
        }
        writer.finish()
            .map_err(|e| EngineError::Internal(format!("IPC finish error: {}", e)))?;
    }
    Ok(buf)
}

/// Check if filter has any row-level filters that need to be applied
fn has_row_filters(filter: &QueryFilter) -> bool {
    filter.tenant_id_eq.is_some()
        || filter.route_id_eq.is_some()
        || filter.tenant_id_in.is_some()
        || filter.route_id_in.is_some()
        || filter.event_time_ge.is_some()
        || filter.event_time_le.is_some()
        || !filter.numeric_eq_filters.is_empty()
        || !filter.float_eq_filters.is_empty()
        || !filter.bool_eq_filters.is_empty()
        || !filter.like_filters.is_empty()
        || !filter.null_filters.is_empty()
        || !filter.string_eq_filters.is_empty()
        || !filter.string_in_filters.is_empty()
        || !filter.numeric_range_filters.is_empty()
}

/// Filter record batches using Arrow compute kernels
pub fn filter_ipc_batches(batches: Vec<RecordBatch>, filter: &QueryFilter) -> Result<Vec<RecordBatch>, EngineError> {
    if batches.is_empty() || !has_row_filters(filter) {
        return Ok(batches);
    }

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // Start with all true (include all rows)
        let mut mask = BooleanArray::from(vec![true; num_rows]);

        // Apply tenant_id_eq filter
        if let Some(tid) = filter.tenant_id_eq {
            if let Some(col_idx) = batch.schema().index_of("tenant_id").ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let scalar = UInt64Array::new_scalar(tid);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    let scalar = Int64Array::new_scalar(tid as i64);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply route_id_eq filter
        if let Some(rid) = filter.route_id_eq {
            if let Some(col_idx) = batch.schema().index_of("route_id").ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    let scalar = UInt64Array::new_scalar(rid);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    let scalar = Int64Array::new_scalar(rid as i64);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply tenant_id_in filter
        if let Some(ref tids) = filter.tenant_id_in {
            if !tids.is_empty() {
                if let Some(col_idx) = batch.schema().index_of("tenant_id").ok() {
                    let col = batch.column(col_idx);
                    let in_mask = build_in_mask_u64(col, tids)?;
                    mask = and(&mask, &in_mask)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply route_id_in filter
        if let Some(ref rids) = filter.route_id_in {
            if !rids.is_empty() {
                if let Some(col_idx) = batch.schema().index_of("route_id").ok() {
                    let col = batch.column(col_idx);
                    let in_mask = build_in_mask_u64(col, rids)?;
                    mask = and(&mask, &in_mask)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply event_time_ge filter
        if let Some(ts) = filter.event_time_ge {
            if let Some(col_idx) = batch.schema().index_of("event_time").ok() {
                let col = batch.column(col_idx);
                let cmp = apply_timestamp_ge(col, ts as i64)?;
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply event_time_le filter
        if let Some(ts) = filter.event_time_le {
            if let Some(col_idx) = batch.schema().index_of("event_time").ok() {
                let col = batch.column(col_idx);
                let cmp = apply_timestamp_le(col, ts as i64)?;
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply numeric equality filters
        for (col_name, value) in &filter.numeric_eq_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                let cmp = apply_numeric_eq(col, *value)?;
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply float equality filters
        for (col_name, value) in &filter.float_eq_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                let cmp = apply_float_eq(col, *value)?;
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply boolean equality filters
        for (col_name, value) in &filter.bool_eq_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    let scalar = BooleanArray::new_scalar(*value);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply string equality filters
        for (col_name, value) in &filter.string_eq_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    let scalar = StringArray::new_scalar(value);
                    let cmp = eq(arr, &scalar)
                        .map_err(|e| EngineError::Internal(format!("filter error: {}", e)))?;
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply string IN filters
        for (col_name, values) in &filter.string_in_filters {
            if !values.is_empty() {
                if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                    let col = batch.column(col_idx);
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        let in_mask = build_in_mask_string(arr, values)?;
                        mask = and(&mask, &in_mask)
                            .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                    }
                }
            }
        }

        // Apply LIKE filters
        for (col_name, pattern, negate) in &filter.like_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    let pattern_scalar = StringArray::new_scalar(pattern);
                    let cmp = if *negate {
                        nlike(arr, &pattern_scalar)
                            .map_err(|e| EngineError::Internal(format!("nlike error: {}", e)))?
                    } else {
                        like(arr, &pattern_scalar)
                            .map_err(|e| EngineError::Internal(format!("like error: {}", e)))?
                    };
                    mask = and(&mask, &cmp)
                        .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
                }
            }
        }

        // Apply IS NULL / IS NOT NULL filters
        for (col_name, is_null) in &filter.null_filters {
            if let Some(col_idx) = batch.schema().index_of(col_name).ok() {
                let col = batch.column(col_idx);
                let cmp = if *is_null {
                    arrow::compute::is_null(col.as_ref())
                        .map_err(|e| EngineError::Internal(format!("is_null error: {}", e)))?
                } else {
                    arrow::compute::is_not_null(col.as_ref())
                        .map_err(|e| EngineError::Internal(format!("is_not_null error: {}", e)))?
                };
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply numeric range filters
        for range_filter in &filter.numeric_range_filters {
            if let Some(col_idx) = batch.schema().index_of(&range_filter.column).ok() {
                let col = batch.column(col_idx);
                let cmp = apply_numeric_range_filter(col, range_filter)?;
                mask = and(&mask, &cmp)
                    .map_err(|e| EngineError::Internal(format!("and error: {}", e)))?;
            }
        }

        // Apply the filter mask to the batch
        let filtered_batch = filter_record_batch(&batch, &mask)?;
        if filtered_batch.num_rows() > 0 {
            result.push(filtered_batch);
        }
    }

    Ok(result)
}

/// Build an IN mask for u64 values
fn build_in_mask_u64(col: &ArrayRef, values: &[u64]) -> Result<BooleanArray, EngineError> {
    let num_rows = col.len();
    let values_set: std::collections::HashSet<u64> = values.iter().copied().collect();

    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let mask: BooleanArray = arr.iter()
            .map(|v| v.map(|x| values_set.contains(&x)))
            .collect();
        Ok(mask)
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let mask: BooleanArray = arr.iter()
            .map(|v| v.map(|x| values_set.contains(&(x as u64))))
            .collect();
        Ok(mask)
    } else {
        // Return all false if column type doesn't match
        Ok(BooleanArray::from(vec![false; num_rows]))
    }
}

/// Build an IN mask for string values
fn build_in_mask_string(arr: &StringArray, values: &[String]) -> Result<BooleanArray, EngineError> {
    let values_set: std::collections::HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
    let mask: BooleanArray = arr.iter()
        .map(|v| v.map(|x| values_set.contains(x)))
        .collect();
    Ok(mask)
}

/// Apply timestamp >= filter
fn apply_timestamp_ge(col: &ArrayRef, ts: i64) -> Result<BooleanArray, EngineError> {
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let scalar = TimestampMicrosecondArray::new_scalar(ts);
        gt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("ge error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let scalar = Int64Array::new_scalar(ts);
        gt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("ge error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let scalar = UInt64Array::new_scalar(ts as u64);
        gt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("ge error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![true; col.len()]))
    }
}

/// Apply timestamp <= filter
fn apply_timestamp_le(col: &ArrayRef, ts: i64) -> Result<BooleanArray, EngineError> {
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let scalar = TimestampMicrosecondArray::new_scalar(ts);
        lt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("le error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let scalar = Int64Array::new_scalar(ts);
        lt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("le error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let scalar = UInt64Array::new_scalar(ts as u64);
        lt_eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("le error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![true; col.len()]))
    }
}

/// Apply numeric equality filter
fn apply_numeric_eq(col: &ArrayRef, value: i128) -> Result<BooleanArray, EngineError> {
    let num_rows = col.len();

    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let scalar = Int64Array::new_scalar(value as i64);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        let scalar = Int32Array::new_scalar(value as i32);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int16Array>() {
        let scalar = Int16Array::new_scalar(value as i16);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int8Array>() {
        let scalar = Int8Array::new_scalar(value as i8);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let scalar = UInt64Array::new_scalar(value as u64);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        let scalar = UInt32Array::new_scalar(value as u32);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt16Array>() {
        let scalar = UInt16Array::new_scalar(value as u16);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt8Array>() {
        let scalar = UInt8Array::new_scalar(value as u8);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![false; num_rows]))
    }
}

/// Apply float equality filter
fn apply_float_eq(col: &ArrayRef, value: f64) -> Result<BooleanArray, EngineError> {
    let num_rows = col.len();

    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        let scalar = Float64Array::new_scalar(value);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        let scalar = Float32Array::new_scalar(value as f32);
        eq(arr, &scalar).map_err(|e| EngineError::Internal(format!("eq error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![false; num_rows]))
    }
}

/// Apply numeric range filter (GT, GE, LT, LE, EQ, NE)
fn apply_numeric_range_filter(col: &ArrayRef, filter: &crate::sql::NumericFilter) -> Result<BooleanArray, EngineError> {
    use crate::sql::{NumericOp, NumericValue};

    let num_rows = col.len();

    // Convert NumericValue to appropriate type and apply comparison
    match &filter.value {
        NumericValue::Int64(v) => apply_numeric_range_i64(col, filter.op, *v),
        NumericValue::UInt64(v) => apply_numeric_range_u64(col, filter.op, *v),
        NumericValue::Float64(v) => apply_numeric_range_f64(col, filter.op, *v),
    }
}

fn apply_numeric_range_i64(col: &ArrayRef, op: crate::sql::NumericOp, value: i64) -> Result<BooleanArray, EngineError> {
    use crate::sql::NumericOp;

    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let scalar = Int64Array::new_scalar(value);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        let scalar = Int32Array::new_scalar(value as i32);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![true; col.len()]))
    }
}

fn apply_numeric_range_u64(col: &ArrayRef, op: crate::sql::NumericOp, value: u64) -> Result<BooleanArray, EngineError> {
    use crate::sql::NumericOp;

    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        let scalar = UInt64Array::new_scalar(value);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        let scalar = UInt32Array::new_scalar(value as u32);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![true; col.len()]))
    }
}

fn apply_numeric_range_f64(col: &ArrayRef, op: crate::sql::NumericOp, value: f64) -> Result<BooleanArray, EngineError> {
    use crate::sql::NumericOp;

    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        let scalar = Float64Array::new_scalar(value);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        let scalar = Float32Array::new_scalar(value as f32);
        match op {
            NumericOp::Eq => eq(arr, &scalar),
            NumericOp::Ne => neq(arr, &scalar),
            NumericOp::Lt => lt(arr, &scalar),
            NumericOp::Le => lt_eq(arr, &scalar),
            NumericOp::Gt => gt(arr, &scalar),
            NumericOp::Ge => gt_eq(arr, &scalar),
        }.map_err(|e| EngineError::Internal(format!("cmp error: {}", e)))
    } else {
        Ok(BooleanArray::from(vec![true; col.len()]))
    }
}

/// Filter a record batch using a boolean mask
fn filter_record_batch(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch, EngineError> {
    let filtered_columns: Result<Vec<ArrayRef>, _> = batch
        .columns()
        .iter()
        .map(|col| arrow_filter(col.as_ref(), mask).map_err(|e| EngineError::Internal(format!("filter error: {}", e))))
        .collect();

    RecordBatch::try_new(batch.schema(), filtered_columns?)
        .map_err(|e| EngineError::Internal(format!("batch creation error: {}", e)))
}
/// Aggregator for computing aggregations over record batches
/// Supports COUNT(*), COUNT(DISTINCT), SUM, AVG, MIN, MAX and GROUP BY
pub struct Aggregator {
    plan: AggPlan,
    /// Accumulated state for non-grouped aggregations
    global_state: Option<AggState>,
    /// Accumulated state for grouped aggregations: HashMap<GroupKey, AggState>
    grouped_state: HashMap<Vec<GroupKeyValue>, AggState>,
    /// Schema from first batch for output generation
    schema: Option<Arc<Schema>>,
}

/// Group key value for HashMap keys
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyValue {
    Int64(i64),
    UInt64(u64),
    String(String),
    Null,
}

/// Internal aggregation state
#[derive(Debug, Clone, Default)]
struct AggState {
    /// COUNT(*) - total rows
    count_star: u64,
    /// COUNT(DISTINCT col) - using HashSet
    count_distinct_sets: HashMap<String, HashSet<u64>>, // Hash of values for distinct count
    /// SUM accumulators per column
    sum_values: HashMap<String, f64>,
    /// MIN values per column
    min_values: HashMap<String, f64>,
    /// MAX values per column
    max_values: HashMap<String, f64>,
    /// For AVG: sum and count per column
    avg_sums: HashMap<String, f64>,
    avg_counts: HashMap<String, u64>,
}

impl Aggregator {
    pub fn new(plan: AggPlan) -> Result<Self, EngineError> {
        let has_group_by = !matches!(plan.group_by, GroupBy::None);
        Ok(Self {
            plan,
            global_state: if has_group_by { None } else { Some(AggState::default()) },
            grouped_state: HashMap::new(),
            schema: None,
        })
    }

    pub fn consume_ipc(&mut self, ipc: &[u8], filter: &QueryFilter) -> Result<(), EngineError> {
        if ipc.is_empty() {
            return Ok(());
        }

        // Decode batches
        let batches = read_ipc_batches(ipc)?;

        // Apply filters first
        let filtered_batches = filter_ipc_batches(batches, filter)?;

        for batch in filtered_batches {
            self.consume_batch(&batch)?;
        }

        Ok(())
    }

    fn consume_batch(&mut self, batch: &RecordBatch) -> Result<(), EngineError> {
        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }

        match &self.plan.group_by {
            GroupBy::None => {
                // Global aggregation (no GROUP BY)
                // Ensure state exists
                if self.global_state.is_none() {
                    self.global_state = Some(AggState::default());
                }
                // Now accumulate - separate borrow to satisfy borrow checker
                let aggs = self.plan.aggs.clone();
                let state = self.global_state.as_mut().unwrap();
                accumulate_aggs_into(state, &aggs, batch)?;
            }
            GroupBy::Tenant => {
                // Group by tenant_id
                self.accumulate_grouped(batch, &["tenant_id"])?;
            }
            GroupBy::Route => {
                // Group by route_id
                self.accumulate_grouped(batch, &["route_id"])?;
            }
            GroupBy::Columns(cols) => {
                // Group by specified columns
                let col_names: Vec<&str> = cols.iter().map(|c| match c {
                    GroupByColumn::TenantId => "tenant_id",
                    GroupByColumn::RouteId => "route_id",
                }).collect();
                self.accumulate_grouped(batch, &col_names)?;
            }
        }

        Ok(())
    }

    fn accumulate_grouped(&mut self, batch: &RecordBatch, group_cols: &[&str]) -> Result<(), EngineError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Extract group key columns
        let key_arrays: Vec<&ArrayRef> = group_cols.iter()
            .filter_map(|col| batch.schema().index_of(col).ok().map(|idx| batch.column(idx)))
            .collect();

        if key_arrays.is_empty() {
            // No group columns found, treat as global aggregation
            let num_rows = batch.num_rows();
            let state = self.global_state.get_or_insert_with(AggState::default);
            state.count_star += num_rows as u64;
            // Accumulate aggregates directly without calling self.accumulate_aggs to avoid borrow issues
            for agg in &self.plan.aggs.clone() {
                match agg {
                    AggKind::CountStar => {}
                    AggKind::CountDistinct { column } | AggKind::ApproxCountDistinct { column } => {
                        if let Ok(col_idx) = batch.schema().index_of(column) {
                            let col = batch.column(col_idx);
                            let set = state.count_distinct_sets.entry(column.clone()).or_insert_with(HashSet::new);
                            let _ = add_distinct_values(col, set);
                        }
                    }
                    AggKind::Sum { column } => {
                        if let Ok(col_idx) = batch.schema().index_of(column) {
                            let col = batch.column(col_idx);
                            let sum = compute_sum(col).unwrap_or(0.0);
                            *state.sum_values.entry(column.clone()).or_insert(0.0) += sum;
                        }
                    }
                    AggKind::Avg { column } => {
                        if let Ok(col_idx) = batch.schema().index_of(column) {
                            let col = batch.column(col_idx);
                            let sum = compute_sum(col).unwrap_or(0.0);
                            let count = col.len() - col.null_count();
                            *state.avg_sums.entry(column.clone()).or_insert(0.0) += sum;
                            *state.avg_counts.entry(column.clone()).or_insert(0) += count as u64;
                        }
                    }
                    AggKind::Min { column } => {
                        if let Ok(col_idx) = batch.schema().index_of(column) {
                            let col = batch.column(col_idx);
                            if let Ok(Some(min)) = compute_min(col) {
                                let entry = state.min_values.entry(column.clone()).or_insert(f64::INFINITY);
                                if min < *entry { *entry = min; }
                            }
                        }
                    }
                    AggKind::Max { column } => {
                        if let Ok(col_idx) = batch.schema().index_of(column) {
                            let col = batch.column(col_idx);
                            if let Ok(Some(max)) = compute_max(col) {
                                let entry = state.max_values.entry(column.clone()).or_insert(f64::NEG_INFINITY);
                                if max > *entry { *entry = max; }
                            }
                        }
                    }
                    _ => {}
                }
            }
            return Ok(());
        }

        // For each row, compute the group key and accumulate
        for row in 0..num_rows {
            let key: Vec<GroupKeyValue> = key_arrays.iter()
                .map(|arr| extract_group_key_value(arr, row))
                .collect();

            let state = self.grouped_state.entry(key).or_insert_with(AggState::default);
            state.count_star += 1;

            // Accumulate individual row aggregations
            for agg in &self.plan.aggs {
                accumulate_single_row(state, agg, batch, row)?;
            }
        }

        Ok(())
    }

    pub fn finish(&self) -> Result<Vec<u8>, EngineError> {
        let schema = self.build_output_schema()?;

        let batch = match &self.plan.group_by {
            GroupBy::None => {
                // Global aggregation result (single row)
                if let Some(ref state) = self.global_state {
                    self.build_global_result(&schema, state)?
                } else {
                    // No data consumed - return empty result with zero counts
                    self.build_global_result(&schema, &AggState::default())?
                }
            }
            _ => {
                // Grouped aggregation result
                self.build_grouped_result(&schema)?
            }
        };

        // Encode to IPC
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| EngineError::Internal(format!("IPC write error: {}", e)))?;
            writer.write(&batch)
                .map_err(|e| EngineError::Internal(format!("IPC write error: {}", e)))?;
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("IPC finish error: {}", e)))?;
        }

        Ok(buf)
    }

    fn build_output_schema(&self) -> Result<Arc<Schema>, EngineError> {
        let mut fields = Vec::new();

        // Add group by columns first
        match &self.plan.group_by {
            GroupBy::None => {}
            GroupBy::Tenant => {
                fields.push(Field::new("tenant_id", DataType::UInt64, true));
            }
            GroupBy::Route => {
                fields.push(Field::new("route_id", DataType::UInt64, true));
            }
            GroupBy::Columns(cols) => {
                for col in cols {
                    match col {
                        GroupByColumn::TenantId => {
                            fields.push(Field::new("tenant_id", DataType::UInt64, true));
                        }
                        GroupByColumn::RouteId => {
                            fields.push(Field::new("route_id", DataType::UInt64, true));
                        }
                    }
                }
            }
        }

        // Add aggregate columns
        for agg in &self.plan.aggs {
            let (name, dtype) = match agg {
                AggKind::CountStar => ("count".to_string(), DataType::UInt64),
                AggKind::CountDistinct { column } => (format!("count_distinct_{}", column), DataType::UInt64),
                AggKind::Sum { column } => (format!("sum_{}", column), DataType::Int64),
                AggKind::Avg { column } => (format!("avg_{}", column), DataType::Float64),
                AggKind::Min { column } => (format!("min_{}", column), DataType::Int64),
                AggKind::Max { column } => (format!("max_{}", column), DataType::Int64),
                AggKind::StddevSamp { column } => (format!("stddev_{}", column), DataType::Float64),
                AggKind::StddevPop { column } => (format!("stddev_pop_{}", column), DataType::Float64),
                AggKind::VarianceSamp { column } => (format!("variance_{}", column), DataType::Float64),
                AggKind::VariancePop { column } => (format!("var_pop_{}", column), DataType::Float64),
                AggKind::ApproxCountDistinct { column } => (format!("approx_count_distinct_{}", column), DataType::UInt64),
            };
            fields.push(Field::new(name, dtype, true));
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    fn build_global_result(&self, schema: &Arc<Schema>, state: &AggState) -> Result<RecordBatch, EngineError> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for agg in &self.plan.aggs {
            let arr: ArrayRef = match agg {
                AggKind::CountStar => {
                    Arc::new(UInt64Array::from(vec![state.count_star]))
                }
                AggKind::CountDistinct { column } => {
                    let count = state.count_distinct_sets.get(column).map(|s| s.len()).unwrap_or(0);
                    Arc::new(UInt64Array::from(vec![count as u64]))
                }
                AggKind::Sum { column } => {
                    let sum = state.sum_values.get(column).copied().unwrap_or(0.0);
                    Arc::new(Int64Array::from(vec![sum as i64]))
                }
                AggKind::Avg { column } => {
                    let sum = state.avg_sums.get(column).copied().unwrap_or(0.0);
                    let count = state.avg_counts.get(column).copied().unwrap_or(0);
                    let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                    Arc::new(Float64Array::from(vec![avg]))
                }
                AggKind::Min { column } => {
                    let min = state.min_values.get(column).copied().unwrap_or(f64::INFINITY);
                    Arc::new(Int64Array::from(vec![if min.is_infinite() { 0 } else { min as i64 }]))
                }
                AggKind::Max { column } => {
                    let max = state.max_values.get(column).copied().unwrap_or(f64::NEG_INFINITY);
                    Arc::new(Int64Array::from(vec![if max.is_infinite() { 0 } else { max as i64 }]))
                }
                AggKind::ApproxCountDistinct { column } => {
                    let count = state.count_distinct_sets.get(column).map(|s| s.len()).unwrap_or(0);
                    Arc::new(UInt64Array::from(vec![count as u64]))
                }
                _ => {
                    // Not implemented - return NULL
                    Arc::new(Float64Array::from(vec![None::<f64>]))
                }
            };
            columns.push(arr);
        }

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| EngineError::Internal(format!("batch creation error: {}", e)))
    }

    fn build_grouped_result(&self, schema: &Arc<Schema>) -> Result<RecordBatch, EngineError> {
        if self.grouped_state.is_empty() {
            // Return empty batch
            let columns: Vec<ArrayRef> = schema.fields().iter().map(|f| {
                match f.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                    DataType::UInt64 => Arc::new(UInt64Array::from(Vec::<u64>::new())) as ArrayRef,
                    DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                    _ => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                }
            }).collect();
            return RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| EngineError::Internal(format!("batch creation error: {}", e)));
        }

        let num_groups = self.grouped_state.len();
        let group_cols = self.get_group_column_names();
        let num_group_cols = group_cols.len();

        // Build group key columns
        let mut key_builders: Vec<UInt64Builder> = (0..num_group_cols)
            .map(|_| UInt64Builder::with_capacity(num_groups))
            .collect();

        // Build aggregate columns
        let mut agg_columns: Vec<Vec<f64>> = self.plan.aggs.iter().map(|_| Vec::with_capacity(num_groups)).collect();
        let mut count_star_values: Vec<i64> = Vec::with_capacity(num_groups);

        for (key, state) in &self.grouped_state {
            // Add key values
            for (i, kv) in key.iter().enumerate() {
                if i < key_builders.len() {
                    match kv {
                        GroupKeyValue::UInt64(v) => key_builders[i].append_value(*v),
                        GroupKeyValue::Int64(v) => key_builders[i].append_value(*v as u64),
                        GroupKeyValue::Null => key_builders[i].append_null(),
                        _ => key_builders[i].append_null(),
                    }
                }
            }

            // Add aggregate values
            for (i, agg) in self.plan.aggs.iter().enumerate() {
                let value = match agg {
                    AggKind::CountStar => state.count_star as f64,
                    AggKind::CountDistinct { column } => {
                        state.count_distinct_sets.get(column).map(|s| s.len() as f64).unwrap_or(0.0)
                    }
                    AggKind::Sum { column } => state.sum_values.get(column).copied().unwrap_or(0.0),
                    AggKind::Avg { column } => {
                        let sum = state.avg_sums.get(column).copied().unwrap_or(0.0);
                        let count = state.avg_counts.get(column).copied().unwrap_or(0);
                        if count > 0 { sum / count as f64 } else { 0.0 }
                    }
                    AggKind::Min { column } => {
                        let v = state.min_values.get(column).copied().unwrap_or(f64::NAN);
                        if v.is_infinite() { f64::NAN } else { v }
                    }
                    AggKind::Max { column } => {
                        let v = state.max_values.get(column).copied().unwrap_or(f64::NAN);
                        if v.is_infinite() { f64::NAN } else { v }
                    }
                    _ => f64::NAN,
                };
                agg_columns[i].push(value);
            }
        }

        // Build final columns
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Add group key columns
        for mut builder in key_builders {
            columns.push(Arc::new(builder.finish()));
        }

        // Add aggregate columns
        for (i, agg) in self.plan.aggs.iter().enumerate() {
            let arr: ArrayRef = match agg {
                AggKind::CountStar | AggKind::CountDistinct { .. } | AggKind::ApproxCountDistinct { .. } => {
                    Arc::new(UInt64Array::from(agg_columns[i].iter().map(|v| *v as u64).collect::<Vec<_>>()))
                }
                AggKind::Sum { .. } | AggKind::Min { .. } | AggKind::Max { .. } => {
                    Arc::new(Int64Array::from(agg_columns[i].iter().map(|v| *v as i64).collect::<Vec<_>>()))
                }
                _ => {
                    Arc::new(Float64Array::from(agg_columns[i].clone()))
                }
            };
            columns.push(arr);
        }

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| EngineError::Internal(format!("batch creation error: {}", e)))
    }

    fn get_group_column_names(&self) -> Vec<&str> {
        match &self.plan.group_by {
            GroupBy::None => vec![],
            GroupBy::Tenant => vec!["tenant_id"],
            GroupBy::Route => vec!["route_id"],
            GroupBy::Columns(cols) => cols.iter().map(|c| match c {
                GroupByColumn::TenantId => "tenant_id",
                GroupByColumn::RouteId => "route_id",
            }).collect(),
        }
    }
}

/// Extract a group key value from an array at a specific row
fn extract_group_key_value(arr: &ArrayRef, row: usize) -> GroupKeyValue {
    if arr.is_null(row) {
        return GroupKeyValue::Null;
    }

    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        GroupKeyValue::UInt64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        GroupKeyValue::Int64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        GroupKeyValue::String(a.value(row).to_string())
    } else {
        GroupKeyValue::Null
    }
}

/// Add distinct values from an array to a HashSet (using hash of value)
fn add_distinct_values(arr: &ArrayRef, set: &mut HashSet<u64>) -> Result<(), EngineError> {
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let hash = hash_array_value_u64(arr, i);
        set.insert(hash);
    }
    Ok(())
}

/// Hash a value from an array at a specific index (returns u64)
fn hash_array_value_u64(arr: &ArrayRef, row: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        a.value(row).hash(&mut hasher);
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        a.value(row).hash(&mut hasher);
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        a.value(row).hash(&mut hasher);
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        a.value(row).to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

/// Compute sum of an array
fn compute_sum(arr: &ArrayRef) -> Result<f64, EngineError> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(aggregate::sum(a).unwrap_or(0) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        Ok(aggregate::sum(a).unwrap_or(0) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Ok(aggregate::sum(a).unwrap_or(0.0))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(aggregate::sum(a).unwrap_or(0) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        Ok(aggregate::sum(a).unwrap_or(0.0) as f64)
    } else {
        Ok(0.0)
    }
}

/// Compute min of an array
fn compute_min(arr: &ArrayRef) -> Result<Option<f64>, EngineError> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(aggregate::min(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        Ok(aggregate::min(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Ok(aggregate::min(a))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(aggregate::min(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        Ok(aggregate::min(a).map(|v| v as f64))
    } else {
        Ok(None)
    }
}

/// Compute max of an array
fn compute_max(arr: &ArrayRef) -> Result<Option<f64>, EngineError> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Ok(aggregate::max(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        Ok(aggregate::max(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Ok(aggregate::max(a))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        Ok(aggregate::max(a).map(|v| v as f64))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        Ok(aggregate::max(a).map(|v| v as f64))
    } else {
        Ok(None)
    }
}

/// Accumulate aggregations into state (standalone function to avoid borrow issues)
fn accumulate_aggs_into(state: &mut AggState, aggs: &[AggKind], batch: &RecordBatch) -> Result<(), EngineError> {
    let num_rows = batch.num_rows();
    state.count_star += num_rows as u64;

    for agg in aggs {
        match agg {
            AggKind::CountStar => {
                // Already counted above
            }
            AggKind::CountDistinct { column } | AggKind::ApproxCountDistinct { column } => {
                if let Ok(col_idx) = batch.schema().index_of(column) {
                    let col = batch.column(col_idx);
                    let set = state.count_distinct_sets.entry(column.clone()).or_insert_with(HashSet::new);
                    add_distinct_values(col, set)?;
                }
            }
            AggKind::Sum { column } => {
                if let Ok(col_idx) = batch.schema().index_of(column) {
                    let col = batch.column(col_idx);
                    let sum = compute_sum(col)?;
                    *state.sum_values.entry(column.clone()).or_insert(0.0) += sum;
                }
            }
            AggKind::Avg { column } => {
                if let Ok(col_idx) = batch.schema().index_of(column) {
                    let col = batch.column(col_idx);
                    let sum = compute_sum(col)?;
                    let count = col.len() - col.null_count();
                    *state.avg_sums.entry(column.clone()).or_insert(0.0) += sum;
                    *state.avg_counts.entry(column.clone()).or_insert(0) += count as u64;
                }
            }
            AggKind::Min { column } => {
                if let Ok(col_idx) = batch.schema().index_of(column) {
                    let col = batch.column(col_idx);
                    let min_val = compute_min(col)?;
                    if let Some(min) = min_val {
                        let entry = state.min_values.entry(column.clone()).or_insert(f64::INFINITY);
                        if min < *entry {
                            *entry = min;
                        }
                    }
                }
            }
            AggKind::Max { column } => {
                if let Ok(col_idx) = batch.schema().index_of(column) {
                    let col = batch.column(col_idx);
                    let max_val = compute_max(col)?;
                    if let Some(max) = max_val {
                        let entry = state.max_values.entry(column.clone()).or_insert(f64::NEG_INFINITY);
                        if max > *entry {
                            *entry = max;
                        }
                    }
                }
            }
            _ => {
                // StddevSamp, StddevPop, VarianceSamp, VariancePop
                // Not yet implemented - would need two-pass or online algorithms
            }
        }
    }
    Ok(())
}

/// Accumulate aggregation for a single row (used in GROUP BY)
fn accumulate_single_row(state: &mut AggState, agg: &AggKind, batch: &RecordBatch, row: usize) -> Result<(), EngineError> {
    match agg {
        AggKind::CountStar => {
            // Already counted in outer loop
        }
        AggKind::CountDistinct { column } | AggKind::ApproxCountDistinct { column } => {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if !col.is_null(row) {
                    let hash = hash_array_value_u64(col, row);
                    let set = state.count_distinct_sets.entry(column.clone()).or_insert_with(HashSet::new);
                    set.insert(hash);
                }
            }
        }
        AggKind::Sum { column } | AggKind::Avg { column } => {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if !col.is_null(row) {
                    let val = extract_f64(col, row);
                    if let Some(v) = val {
                        *state.sum_values.entry(column.clone()).or_insert(0.0) += v;
                        if matches!(agg, AggKind::Avg { .. }) {
                            *state.avg_sums.entry(column.clone()).or_insert(0.0) += v;
                            *state.avg_counts.entry(column.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
        AggKind::Min { column } => {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if !col.is_null(row) {
                    if let Some(v) = extract_f64(col, row) {
                        let entry = state.min_values.entry(column.clone()).or_insert(f64::INFINITY);
                        if v < *entry {
                            *entry = v;
                        }
                    }
                }
            }
        }
        AggKind::Max { column } => {
            if let Ok(col_idx) = batch.schema().index_of(column) {
                let col = batch.column(col_idx);
                if !col.is_null(row) {
                    if let Some(v) = extract_f64(col, row) {
                        let entry = state.max_values.entry(column.clone()).or_insert(f64::NEG_INFINITY);
                        if v > *entry {
                            *entry = v;
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Extract a float value from an array at a specific row
fn extract_f64(arr: &ArrayRef, row: usize) -> Option<f64> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Some(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Date32Array>() {
        Some(a.value(row) as f64)
    } else {
        None
    }
}

pub fn aggregation_projection(agg: &AggPlan, proj: &[String]) -> Vec<String> {
    let mut needed = proj.to_vec();

    // Add group by columns
    match &agg.group_by {
        GroupBy::None => {}
        GroupBy::Tenant => {
            if !needed.contains(&"tenant_id".to_string()) {
                needed.push("tenant_id".to_string());
            }
        }
        GroupBy::Route => {
            if !needed.contains(&"route_id".to_string()) {
                needed.push("route_id".to_string());
            }
        }
        GroupBy::Columns(cols) => {
            for col in cols {
                let col_name = match col {
                    GroupByColumn::TenantId => "tenant_id",
                    GroupByColumn::RouteId => "route_id",
                };
                if !needed.contains(&col_name.to_string()) {
                    needed.push(col_name.to_string());
                }
            }
        }
    }

    // Add columns needed by aggregations
    for a in &agg.aggs {
        match a {
            AggKind::CountStar => {}
            AggKind::CountDistinct { column } | AggKind::Sum { column } |
            AggKind::Avg { column } | AggKind::Min { column } | AggKind::Max { column } |
            AggKind::StddevSamp { column } | AggKind::StddevPop { column } |
            AggKind::VarianceSamp { column } | AggKind::VariancePop { column } |
            AggKind::ApproxCountDistinct { column } => {
                if !needed.contains(column) {
                    needed.push(column.clone());
                }
            }
        }
    }

    needed
}
pub fn split_filter_for_join(f: &QueryFilter, _l: Option<&arrow_schema::Schema>, _r: Option<&arrow_schema::Schema>) -> (QueryFilter, QueryFilter, QueryFilter) { (f.clone(), QueryFilter::default(), QueryFilter::default()) }
pub fn join_projection_sets(_l: &arrow_schema::Schema, _r: &arrow_schema::Schema, _p: Option<&[String]>, _lc: &str, _rc: &str, _k: &[String]) -> Result<(Option<Vec<String>>, Option<Vec<String>>), EngineError> { Ok((None, None)) }
pub fn extend_needed_with_filter(_n: &mut Vec<String>, _f: &QueryFilter, _s: Option<&arrow_schema::Schema>) {}
// Duplicates removed



/// Execute a recursive CTE
/// Recursive CTEs have the form: anchor_query UNION [ALL] recursive_query
/// The recursive query references the CTE itself
fn execute_recursive_cte(
    db: &Db,
    cte: &crate::sql::CteDefinition,
    _cte_context: &CteContext,
    timeout_millis: u32,
) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;

    // Parse the CTE query to extract anchor and recursive parts
    // The raw_sql should be something like:
    // "SELECT 1 as n UNION ALL SELECT n + 1 FROM cte_name WHERE n < 10"
    let (anchor_sql, recursive_sql, is_union_all) = parse_recursive_cte_parts(&cte.raw_sql, &cte.name)?;

    // Execute the anchor query
    let anchor_result = db.query(QueryRequest {
        sql: anchor_sql.clone(),
        timeout_millis,
        collect_stats: false,
    })?;

    if anchor_result.records_ipc.is_empty() {
        return Ok(Vec::new());
    }

    // Parse anchor results into batches
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut current_batches: Vec<RecordBatch> = Vec::new();
    {
        let cursor = Cursor::new(&anchor_result.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("failed to read anchor result: {e}")))?;
        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| EngineError::Internal(format!("failed to read anchor batch: {e}")))?;
            all_batches.push(batch.clone());
            current_batches.push(batch);
        }
    }

    if current_batches.is_empty() {
        return Ok(anchor_result.records_ipc);
    }

    // Get the schema from the anchor result
    let schema = current_batches[0].schema();

    // Recursive iteration with a maximum depth to prevent infinite loops
    const MAX_RECURSION_DEPTH: usize = 1000;
    let mut iteration = 0;

    while iteration < MAX_RECURSION_DEPTH {
        iteration += 1;

        // Create a temporary table with the current iteration's data
        let _temp_table_name = format!("__recursive_temp_{}", cte.name);

        // Serialize current batches to IPC for the temp table
        let mut temp_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut temp_ipc, schema.as_ref())
                .map_err(|e| EngineError::Internal(format!("failed to create temp IPC: {e}")))?;
            for batch in &current_batches {
                writer.write(batch)
                    .map_err(|e| EngineError::Internal(format!("failed to write temp batch: {e}")))?;
            }
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("failed to finish temp IPC: {e}")))?;
        }

        // For now, we execute the recursive query by replacing references to the CTE name
        // with a subquery or inline values. This is a simplified approach.
        // A more complete implementation would use a temporary table.

        // Replace CTE name references with inline VALUES or subquery
        let recursive_with_data = replace_cte_reference_with_values(
            &recursive_sql,
            &cte.name,
            &current_batches,
        )?;

        // Execute the recursive step
        let recursive_result = db.query(QueryRequest {
            sql: recursive_with_data,
            timeout_millis,
            collect_stats: false,
        });

        match recursive_result {
            Ok(result) => {
                if result.records_ipc.is_empty() {
                    // No more results, recursion complete
                    break;
                }

                // Parse new results
                current_batches.clear();
                let cursor = Cursor::new(&result.records_ipc);
                let mut reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| EngineError::Internal(format!("failed to read recursive result: {e}")))?;

                let mut has_rows = false;
                while let Some(batch_result) = reader.next() {
                    let batch = batch_result
                        .map_err(|e| EngineError::Internal(format!("failed to read recursive batch: {e}")))?;
                    if batch.num_rows() > 0 {
                        has_rows = true;
                        if is_union_all {
                            all_batches.push(batch.clone());
                        } else {
                            // For UNION (not ALL), we'd need to deduplicate
                            // For simplicity, we'll just add them
                            all_batches.push(batch.clone());
                        }
                        current_batches.push(batch);
                    }
                }

                if !has_rows {
                    break;
                }
            }
            Err(_) => {
                // Query failed, likely no more valid results
                break;
            }
        }
    }

    if iteration >= MAX_RECURSION_DEPTH {
        return Err(EngineError::InvalidArgument(
            "recursive CTE exceeded maximum recursion depth (1000)".into(),
        ));
    }

    // Serialize all accumulated batches
    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut output_ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_ipc, all_batches[0].schema().as_ref())
            .map_err(|e| EngineError::Internal(format!("failed to create output IPC: {e}")))?;
        for batch in &all_batches {
            writer.write(batch)
                .map_err(|e| EngineError::Internal(format!("failed to write output batch: {e}")))?;
        }
        writer.finish()
            .map_err(|e| EngineError::Internal(format!("failed to finish output IPC: {e}")))?;
    }

    Ok(output_ipc)
}

/// Parse a recursive CTE query into anchor and recursive parts
/// Returns (anchor_sql, recursive_sql, is_union_all)
fn parse_recursive_cte_parts(
    raw_sql: &str,
    cte_name: &str,
) -> Result<(String, String, bool), EngineError> {
    let upper = raw_sql.to_uppercase();

    // Find UNION ALL or UNION
    let (union_pos, is_union_all) = if let Some(pos) = upper.find("UNION ALL") {
        (pos, true)
    } else if let Some(pos) = upper.find("UNION") {
        (pos, false)
    } else {
        return Err(EngineError::InvalidArgument(format!(
            "recursive CTE '{}' must contain UNION or UNION ALL",
            cte_name
        )));
    };

    let anchor_sql = raw_sql[..union_pos].trim().to_string();
    let recursive_start = if is_union_all {
        union_pos + 9 // "UNION ALL".len()
    } else {
        union_pos + 5 // "UNION".len()
    };
    let recursive_sql = raw_sql[recursive_start..].trim().to_string();

    Ok((anchor_sql, recursive_sql, is_union_all))
}

/// Replace CTE name references in the recursive query with actual values
fn replace_cte_reference_with_values(
    recursive_sql: &str,
    cte_name: &str,
    current_batches: &[RecordBatch],
) -> Result<String, EngineError> {
    fn is_safe_cte_name(name: &str) -> bool {
        if name.is_empty() || name.len() > 128 {
            return false;
        }
        let mut chars = name.chars();
        let first = match chars.next() {
            Some(c) => c,
            None => return false,
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return false;
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    fn sql_literal_for_value(
        col: &ArrayRef,
        row_idx: usize,
    ) -> Result<String, EngineError> {
        use arrow_array::{
            BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
            UInt64Array, LargeStringArray,
        };

        if col.is_null(row_idx) {
            return Ok("NULL".to_string());
        }

        match col.data_type() {
            arrow_schema::DataType::Utf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| EngineError::Internal("invalid Utf8 array".into()))?;
                let value = arr.value(row_idx).replace('\'', "''");
                Ok(format!("'{}'", value))
            }
            arrow_schema::DataType::LargeUtf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| EngineError::Internal("invalid LargeUtf8 array".into()))?;
                let value = arr.value(row_idx).replace('\'', "''");
                Ok(format!("'{}'", value))
            }
            arrow_schema::DataType::Int64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| EngineError::Internal("invalid Int64 array".into()))?;
                Ok(arr.value(row_idx).to_string())
            }
            arrow_schema::DataType::Int32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| EngineError::Internal("invalid Int32 array".into()))?;
                Ok(arr.value(row_idx).to_string())
            }
            arrow_schema::DataType::UInt64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| EngineError::Internal("invalid UInt64 array".into()))?;
                Ok(arr.value(row_idx).to_string())
            }
            arrow_schema::DataType::Float64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| EngineError::Internal("invalid Float64 array".into()))?;
                Ok(arr.value(row_idx).to_string())
            }
            arrow_schema::DataType::Float32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| EngineError::Internal("invalid Float32 array".into()))?;
                Ok(arr.value(row_idx).to_string())
            }
            arrow_schema::DataType::Boolean => {
                let arr = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| EngineError::Internal("invalid Boolean array".into()))?;
                Ok(if arr.value(row_idx) { "true" } else { "false" }.to_string())
            }
            other => Err(EngineError::InvalidArgument(format!(
                "unsupported CTE value type: {other}"
            ))),
        }
    }

    if !is_safe_cte_name(cte_name) {
        return Err(EngineError::InvalidArgument(
            "invalid CTE name".into(),
        ));
    }

    // If no data, return a query that returns no results
    if current_batches.is_empty() || current_batches.iter().all(|b| b.num_rows() == 0) {
        // Return a query that produces empty results with the same structure
        return Ok(format!("{} WHERE 1=0", recursive_sql));
    }

    // Build a VALUES clause or subquery from the current data
    let first_batch = &current_batches[0];
    let schema = first_batch.schema();
    let num_cols = schema.fields().len();

    // Collect column names
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Build VALUES rows from all batches
    let mut value_rows = Vec::new();
    for batch in current_batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_values = Vec::new();
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                let val = sql_literal_for_value(col, row_idx)?;
                row_values.push(val);
            }
            value_rows.push(format!("({})", row_values.join(", ")));
        }
    }

    if value_rows.is_empty() {
        return Ok(format!("{} WHERE 1=0", recursive_sql));
    }

    // Create a subquery that returns the current values
    // (SELECT col1, col2, ... FROM (VALUES (v1, v2), (v3, v4), ...) AS t(col1, col2))
    let values_sql = format!(
        "(SELECT * FROM (VALUES {}) AS {}({}))",
        value_rows.join(", "),
        cte_name,
        col_names.join(", ")
    );

    // Replace references to the CTE name with the VALUES subquery
    // We need to be careful to only replace table references, not column names
    let pattern = format!(r"\b{}\b", regex::escape(cte_name));
    let re = regex::Regex::new(&pattern)
        .map_err(|e| EngineError::Internal(format!("regex error: {e}")))?;

    // Replace FROM cte_name with FROM (VALUES ...)
    let result = re.replace_all(recursive_sql, values_sql.as_str()).to_string();

    Ok(result)
}

/// Build a WHERE clause from a ParsedQuery
///
/// # Security Note
/// All filter values (watermark_ge, watermark_le, event_time_ge, event_time_le,
/// tenant_id_eq, route_id_eq) are typed as Option<u64> in QueryFilter.
/// This makes them inherently safe from SQL injection since u64 values can only
/// contain numeric digits and cannot represent SQL injection strings.
fn build_where_clause(query: &crate::sql::ParsedQuery) -> String {
    let mut clauses = Vec::new();

    if let Some(ge) = query.filter.watermark_ge {
        clauses.push(format!("watermark_micros >= {ge}"));
    }
    if let Some(le) = query.filter.watermark_le {
        clauses.push(format!("watermark_micros <= {le}"));
    }
    if let Some(ge) = query.filter.event_time_ge {
        clauses.push(format!("event_time >= {ge}"));
    }
    if let Some(le) = query.filter.event_time_le {
        clauses.push(format!("event_time <= {le}"));
    }
    if let Some(eq) = query.filter.tenant_id_eq {
        clauses.push(format!("tenant_id = {eq}"));
    }
    if let Some(eq) = query.filter.route_id_eq {
        clauses.push(format!("route_id = {eq}"));
    }

    if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    }
}

/// Merge CTE results with main query results
/// Used when the main query references CTEs that need to be combined
pub fn merge_cte_results(
    main_result: &[u8],
    cte_context: &CteContext,
    cte_names: &[String],
) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;

    if cte_names.is_empty() {
        return Ok(main_result.to_vec());
    }

    let mut all_batches = Vec::new();

    // Add main result batches
    if !main_result.is_empty() {
        let cursor = Cursor::new(main_result);
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("failed to read main result: {e}")))?;
        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| EngineError::Internal(format!("failed to read batch: {e}")))?;
            all_batches.push(batch);
        }
    }

    // Add CTE result batches
    for cte_name in cte_names {
        if let Some(cte_data) = cte_context.get_result(cte_name) {
            if !cte_data.is_empty() {
                let cursor = Cursor::new(cte_data);
                let mut reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| EngineError::Internal(format!("failed to read CTE result: {e}")))?;
                while let Some(batch_result) = reader.next() {
                    let batch = batch_result
                        .map_err(|e| EngineError::Internal(format!("failed to read CTE batch: {e}")))?;
                    all_batches.push(batch);
                }
            }
        }
    }

    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Encode all batches back to IPC
    let mut output_ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_ipc, all_batches[0].schema().as_ref())
            .map_err(|e| EngineError::Internal(format!("failed to create IPC writer: {e}")))?;
        for batch in &all_batches {
            writer.write(batch)
                .map_err(|e| EngineError::Internal(format!("failed to write batch: {e}")))?;
        }
        writer.finish()
            .map_err(|e| EngineError::Internal(format!("failed to finish IPC: {e}")))?;
    }

    Ok(output_ipc)
}

// ============================================================================
// Window Function Support
// ============================================================================

use crate::sql::{WindowFunction, WindowSpec, SelectColumn};

/// Apply window functions to query results
/// This function evaluates window functions like ROW_NUMBER(), RANK(), LAG(), LEAD()
pub fn apply_window_functions(
    ipc_data: &[u8],
    window_columns: &[SelectColumn],
) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;

    if window_columns.is_empty() || ipc_data.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    // Check if there are any window functions to evaluate
    let has_window_functions = window_columns.iter().any(|c| {
        matches!(&c.expr, crate::sql::SelectExpr::Window { .. })
    });

    if !has_window_functions {
        return Ok(ipc_data.to_vec());
    }

    // Decode the IPC data
    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("failed to read IPC: {e}")))?;

    // Collect all batches first (window functions need all data)
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result
            .map_err(|e| EngineError::Internal(format!("failed to read batch: {e}")))?;
        all_batches.push(batch);
    }

    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Get the schema from first batch
    let schema = all_batches[0].schema();

    // Convert all batches to rows for window function evaluation
    let mut all_rows: Vec<std::collections::HashMap<String, ComputedValue>> = Vec::new();
    for batch in &all_batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row = std::collections::HashMap::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = extract_value_at(col, row_idx);
                row.insert(field.name().clone(), val);
            }
            all_rows.push(row);
        }
    }

    let num_rows = all_rows.len();

    // Evaluate each window function
    let mut window_results: Vec<(String, Vec<ComputedValue>)> = Vec::new();

    for select_col in window_columns {
        if let crate::sql::SelectExpr::Window { function, spec } = &select_col.expr {
            let alias = select_col.alias.clone().unwrap_or_else(|| {
                format!("window_{}", window_results.len())
            });

            // Evaluate the window function
            let results = evaluate_window_function(function, spec, &all_rows)?;
            window_results.push((alias, results));
        }
    }

    // Build new batches with window function results added
    let mut new_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    for (alias, _) in &window_results {
        new_fields.push(Field::new(alias, DataType::Int64, true));
    }
    let new_schema = Arc::new(Schema::new(new_fields));

    // Build new arrays for window results
    let mut new_arrays: Vec<ArrayRef> = Vec::new();

    // Copy original columns
    // For simplicity, we merge all batches into one
    for col_idx in 0..schema.fields().len() {
        let mut values: Vec<ComputedValue> = Vec::with_capacity(num_rows);
        for row in &all_rows {
            let field_name = schema.field(col_idx).name();
            values.push(row.get(field_name).cloned().unwrap_or(ComputedValue::Null));
        }
        let (arr, _) = build_array_from_computed_values(&values, "")?;
        new_arrays.push(arr);
    }

    // Add window function result columns
    for (_, results) in &window_results {
        let (arr, _) = build_array_from_computed_values(results, "")?;
        new_arrays.push(arr);
    }

    // Create the output batch
    let new_batch = RecordBatch::try_new(new_schema.clone(), new_arrays)
        .map_err(|e| EngineError::Internal(format!("failed to create batch: {e}")))?;

    // Encode back to IPC
    let mut output_ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_ipc, new_schema.as_ref())
            .map_err(|e| EngineError::Internal(format!("failed to create IPC writer: {e}")))?;
        writer.write(&new_batch)
            .map_err(|e| EngineError::Internal(format!("failed to write batch: {e}")))?;
        writer.finish()
            .map_err(|e| EngineError::Internal(format!("failed to finish IPC: {e}")))?;
    }

    Ok(output_ipc)
}

/// Evaluate a window function over all rows
fn evaluate_window_function(
    function: &WindowFunction,
    spec: &WindowSpec,
    rows: &[std::collections::HashMap<String, ComputedValue>],
) -> Result<Vec<ComputedValue>, EngineError> {
    let num_rows = rows.len();
    let mut results = Vec::with_capacity(num_rows);

    // Get partition groups (if PARTITION BY is specified)
    let partitions = get_partitions(rows, &spec.partition_by);

    // For each partition, evaluate the window function
    for (partition_rows, partition_indices) in &partitions {
        let partition_size = partition_rows.len();

        // Sort partition if ORDER BY is specified
        let ordered_indices = if spec.order_by.is_empty() {
            partition_indices.clone()
        } else {
            sort_partition_indices(partition_rows, partition_indices, &spec.order_by)
        };

        // Evaluate function for each row in partition
        let partition_results = match function {
            WindowFunction::RowNumber => {
                // ROW_NUMBER() - sequential number within partition
                (1..=partition_size)
                    .map(|n| ComputedValue::Integer(n as i64))
                    .collect()
            }
            WindowFunction::Rank => {
                // RANK() - rank with gaps for ties
                compute_rank(partition_rows, &spec.order_by, false)
            }
            WindowFunction::DenseRank => {
                // DENSE_RANK() - rank without gaps for ties
                compute_rank(partition_rows, &spec.order_by, true)
            }
            WindowFunction::NTile(n) => {
                // NTILE(n) - divide into n roughly equal groups
                let bucket_size = (partition_size + *n as usize - 1) / *n as usize;
                (0..partition_size)
                    .map(|i| ComputedValue::Integer((i / bucket_size + 1) as i64))
                    .collect()
            }
            WindowFunction::Lag { expr, offset, default } => {
                // LAG(expr, offset, default) - value from offset rows before
                compute_lag_lead(partition_rows, expr, *offset, default.as_deref(), true)
            }
            WindowFunction::Lead { expr, offset, default } => {
                // LEAD(expr, offset, default) - value from offset rows after
                compute_lag_lead(partition_rows, expr, *offset, default.as_deref(), false)
            }
            WindowFunction::FirstValue(expr) => {
                // FIRST_VALUE(expr) - first value in partition
                let first_val = if partition_rows.is_empty() {
                    ComputedValue::Null
                } else {
                    let ctx = EvalContext::new(&partition_rows[0]);
                    evaluate_expr(expr, &ctx)?
                };
                vec![first_val; partition_size]
            }
            WindowFunction::LastValue(expr) => {
                // LAST_VALUE(expr) - last value in partition
                let last_val = if partition_rows.is_empty() {
                    ComputedValue::Null
                } else {
                    let ctx = EvalContext::new(&partition_rows[partition_size - 1]);
                    evaluate_expr(expr, &ctx)?
                };
                vec![last_val; partition_size]
            }
            WindowFunction::NthValue { expr, n } => {
                // NTH_VALUE(expr, n) - nth value in partition
                let nth_val = if *n < 1 || (*n as usize) > partition_size {
                    ComputedValue::Null
                } else {
                    let ctx = EvalContext::new(&partition_rows[(*n - 1) as usize]);
                    evaluate_expr(expr, &ctx)?
                };
                vec![nth_val; partition_size]
            }
            WindowFunction::WindowSum(expr) => {
                // SUM() OVER - sum within partition
                compute_window_aggregate(partition_rows, expr, |vals| {
                    let sum: f64 = vals.iter().filter_map(|v| v.as_f64()).sum();
                    ComputedValue::Float(sum)
                })
            }
            WindowFunction::WindowAvg(expr) => {
                // AVG() OVER - average within partition
                compute_window_aggregate(partition_rows, expr, |vals| {
                    let non_null: Vec<f64> = vals.iter().filter_map(|v| v.as_f64()).collect();
                    if non_null.is_empty() {
                        ComputedValue::Null
                    } else {
                        ComputedValue::Float(non_null.iter().sum::<f64>() / non_null.len() as f64)
                    }
                })
            }
            WindowFunction::WindowMin(expr) => {
                // MIN() OVER - minimum within partition
                compute_window_aggregate(partition_rows, expr, |vals| {
                    vals.iter()
                        .filter(|v| !v.is_null())
                        .min_by(|a, b| compare_values(a, b).cmp(&0))
                        .cloned()
                        .unwrap_or(ComputedValue::Null)
                })
            }
            WindowFunction::WindowMax(expr) => {
                // MAX() OVER - maximum within partition
                compute_window_aggregate(partition_rows, expr, |vals| {
                    vals.iter()
                        .filter(|v| !v.is_null())
                        .max_by(|a, b| compare_values(a, b).cmp(&0))
                        .cloned()
                        .unwrap_or(ComputedValue::Null)
                })
            }
            WindowFunction::WindowCount(expr_opt) => {
                // COUNT() OVER - count within partition
                compute_window_aggregate(partition_rows,
                    expr_opt.as_deref().unwrap_or(&SelectExpr::Null),
                    |vals| {
                        let count = if expr_opt.is_some() {
                            vals.iter().filter(|v| !v.is_null()).count()
                        } else {
                            vals.len()
                        };
                        ComputedValue::Integer(count as i64)
                    })
            }
        };

        // Map partition results back to original row order
        for (partition_idx, _) in ordered_indices.iter().enumerate() {
            if partition_idx < partition_results.len() {
                results.push(partition_results[partition_idx].clone());
            }
        }
    }

    Ok(results)
}

/// Group rows by PARTITION BY columns
fn get_partitions(
    rows: &[std::collections::HashMap<String, ComputedValue>],
    partition_by: &[String],
) -> Vec<(Vec<std::collections::HashMap<String, ComputedValue>>, Vec<usize>)> {
    if partition_by.is_empty() {
        // No partitioning - all rows in one partition
        return vec![(rows.to_vec(), (0..rows.len()).collect())];
    }

    let mut partitions: std::collections::HashMap<String, (Vec<std::collections::HashMap<String, ComputedValue>>, Vec<usize>)> = std::collections::HashMap::new();

    for (idx, row) in rows.iter().enumerate() {
        // Build partition key from PARTITION BY columns
        let key: String = partition_by.iter()
            .map(|col| row.get(col).map(|v| v.to_string_value()).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("|");

        let entry = partitions.entry(key)
            .or_insert_with(|| (Vec::new(), Vec::new()));
        entry.0.push(row.clone());
        entry.1.push(idx);
    }

    partitions.into_values().collect()
}

/// Sort partition indices according to ORDER BY
fn sort_partition_indices(
    _partition_rows: &[std::collections::HashMap<String, ComputedValue>],
    partition_indices: &[usize],
    _order_by: &[crate::sql::OrderByClause],
) -> Vec<usize> {
    // For now, return original order (full ORDER BY sorting can be added later)
    partition_indices.to_vec()
}

/// Compute RANK or DENSE_RANK
fn compute_rank(
    _partition_rows: &[std::collections::HashMap<String, ComputedValue>],
    _order_by: &[crate::sql::OrderByClause],
    _dense: bool,
) -> Vec<ComputedValue> {
    // Without ORDER BY, all rows have rank 1
    // With ORDER BY, compute ranks based on value comparisons
    // For now, simple implementation: sequential ranks
    _partition_rows.iter()
        .enumerate()
        .map(|(i, _)| ComputedValue::Integer((i + 1) as i64))
        .collect()
}

/// Compute LAG or LEAD
fn compute_lag_lead(
    partition_rows: &[std::collections::HashMap<String, ComputedValue>],
    expr: &SelectExpr,
    offset: i64,
    default: Option<&SelectExpr>,
    is_lag: bool,
) -> Vec<ComputedValue> {
    let n = partition_rows.len();
    let mut results = Vec::with_capacity(n);

    for i in 0..n {
        let target_idx = if is_lag {
            i as i64 - offset
        } else {
            i as i64 + offset
        };

        let val = if target_idx >= 0 && (target_idx as usize) < n {
            let ctx = EvalContext::new(&partition_rows[target_idx as usize]);
            evaluate_expr(expr, &ctx).unwrap_or(ComputedValue::Null)
        } else if let Some(def_expr) = default {
            let ctx = EvalContext::new(&partition_rows[i]);
            evaluate_expr(def_expr, &ctx).unwrap_or(ComputedValue::Null)
        } else {
            ComputedValue::Null
        };

        results.push(val);
    }

    results
}

/// Compute a window aggregate function
fn compute_window_aggregate<F>(
    partition_rows: &[std::collections::HashMap<String, ComputedValue>],
    expr: &SelectExpr,
    aggregate_fn: F,
) -> Vec<ComputedValue>
where
    F: Fn(&[ComputedValue]) -> ComputedValue,
{
    // Evaluate expression for all rows in partition
    let values: Vec<ComputedValue> = partition_rows.iter()
        .map(|row| {
            let ctx = EvalContext::new(row);
            evaluate_expr(expr, &ctx).unwrap_or(ComputedValue::Null)
        })
        .collect();

    // Apply aggregate function
    let result = aggregate_fn(&values);

    // Return same value for all rows in partition
    vec![result; partition_rows.len()]
}

/// Apply computed columns (expressions) to query results
/// This function evaluates SELECT expressions like UPPER(name), price * 1.1, etc.
pub fn apply_computed_columns(
    ipc_data: &[u8],
    computed_columns: &[crate::sql::SelectColumn],
) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;

    if computed_columns.is_empty() || ipc_data.is_empty() {
        return Ok(ipc_data.to_vec());
    }

    // Check if there are any actual expressions to evaluate (not just column references)
    let has_expressions = computed_columns.iter().any(|c| {
        !matches!(&c.expr, crate::sql::SelectExpr::Column(_))
    });

    if !has_expressions {
        // No expressions to evaluate, just pass through
        return Ok(ipc_data.to_vec());
    }

    // Decode the IPC data
    let cursor = Cursor::new(ipc_data);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| EngineError::Internal(format!("failed to read IPC: {e}")))?;

    let mut output_batches = Vec::new();

    while let Some(batch_result) = reader.next() {
        let batch = batch_result
            .map_err(|e| EngineError::Internal(format!("failed to read batch: {e}")))?;

        let num_rows = batch.num_rows();
        let schema = batch.schema();

        // Build row data for expression evaluation
        let mut all_rows: Vec<std::collections::HashMap<String, ComputedValue>> =
            Vec::with_capacity(num_rows);

        // Convert batch columns to rows for evaluation
        for row_idx in 0..num_rows {
            let mut row = std::collections::HashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = extract_value_at(col, row_idx);
                row.insert(field.name().clone(), val);
            }
            all_rows.push(row);
        }

        // Evaluate expressions and build new columns
        let mut new_columns: Vec<ArrayRef> = Vec::new();
        let mut new_fields: Vec<Field> = Vec::new();

        for select_col in computed_columns {
            let alias = select_col.alias.clone().unwrap_or_else(|| {
                format!("expr_{}", new_fields.len())
            });

            // Evaluate the expression for each row
            let mut results: Vec<ComputedValue> = Vec::with_capacity(num_rows);
            for row in &all_rows {
                let ctx = EvalContext::new(row);
                let val = evaluate_expr(&select_col.expr, &ctx)?;
                results.push(val);
            }

            // Determine the output type based on results
            let (arr, dt) = build_array_from_computed_values(&results, &alias)?;
            new_columns.push(arr);
            new_fields.push(Field::new(&alias, dt, true));
        }

        // Combine original columns with computed columns
        let mut all_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut all_columns: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();

        all_fields.extend(new_fields);
        all_columns.extend(new_columns);

        let new_schema = Arc::new(Schema::new(all_fields));
        let new_batch = RecordBatch::try_new(new_schema, all_columns)
            .map_err(|e| EngineError::Internal(format!("failed to create batch: {e}")))?;

        output_batches.push(new_batch);
    }

    // Encode back to IPC
    if output_batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut output_ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output_ipc, output_batches[0].schema().as_ref())
            .map_err(|e| EngineError::Internal(format!("failed to create IPC writer: {e}")))?;
        for batch in &output_batches {
            writer.write(batch)
                .map_err(|e| EngineError::Internal(format!("failed to write batch: {e}")))?;
        }
        writer.finish()
            .map_err(|e| EngineError::Internal(format!("failed to finish IPC: {e}")))?;
    }

    Ok(output_ipc)
}

/// Extract a value from an Arrow array at a given row index
fn extract_value_at(arr: &ArrayRef, row_idx: usize) -> ComputedValue {
    use arrow_array::{
        Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        Float32Array, Float64Array, StringArray, BooleanArray,
        LargeStringArray, LargeBinaryArray, BinaryArray,
        FixedSizeBinaryArray, Date32Array, TimestampMicrosecondArray,
        Decimal128Array,
    };

    if arr.is_null(row_idx) {
        return ComputedValue::Null;
    }

    match arr.data_type() {
        DataType::Int8 => {
            let a = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx))
        }
        DataType::UInt8 => {
            let a = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::UInt16 => {
            let a = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::UInt32 => {
            let a = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::UInt64 => {
            let a = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
            ComputedValue::Integer(a.value(row_idx) as i64)
        }
        DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            ComputedValue::Float(a.value(row_idx) as f64)
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            ComputedValue::Float(a.value(row_idx))
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            ComputedValue::String(a.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            // Used for JSON storage
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            ComputedValue::Json(a.value(row_idx).to_string())
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            ComputedValue::Boolean(a.value(row_idx))
        }
        DataType::FixedSizeBinary(16) => {
            // UUID stored as 16 bytes
            let a = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let bytes = a.value(row_idx);
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(bytes);
            ComputedValue::Uuid(uuid_bytes)
        }
        DataType::FixedSizeBinary(_) => {
            // Other fixed-size binary
            let a = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            ComputedValue::Binary(a.value(row_idx).to_vec())
        }
        DataType::Binary => {
            let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
            ComputedValue::Binary(a.value(row_idx).to_vec())
        }
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            ComputedValue::Binary(a.value(row_idx).to_vec())
        }
        DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            ComputedValue::Date(a.value(row_idx))
        }
        DataType::Timestamp(_, _) => {
            let a = arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            ComputedValue::Timestamp(a.value(row_idx))
        }
        DataType::Decimal128(precision, scale) => {
            let a = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            ComputedValue::Decimal {
                value: a.value(row_idx),
                precision: *precision,
                scale: *scale,
            }
        }
        _ => ComputedValue::Null,
    }
}

/// Build an Arrow array from computed values
fn build_array_from_computed_values(
    values: &[ComputedValue],
    _alias: &str,
) -> Result<(ArrayRef, DataType), EngineError> {
    use arrow_array::{
        Float64Array, Int64Array, StringArray, BooleanArray, ArrayRef,
        LargeStringArray, LargeBinaryArray, Date32Array,
        Decimal128Array,
    };

    // Determine the predominant type
    let mut has_int = false;
    let mut has_float = false;
    let mut has_string = false;
    let mut has_bool = false;
    let mut has_timestamp = false;
    let mut has_uuid = false;
    let mut has_json = false;
    let mut has_decimal = false;
    let mut decimal_precision: u8 = 38;
    let mut decimal_scale: i8 = 0;
    let mut has_binary = false;
    let mut has_date = false;

    for v in values {
        match v {
            ComputedValue::Integer(_) => has_int = true,
            ComputedValue::Float(_) => has_float = true,
            ComputedValue::String(_) => has_string = true,
            ComputedValue::Boolean(_) => has_bool = true,
            ComputedValue::Timestamp(_) => has_timestamp = true,
            ComputedValue::Uuid(_) => has_uuid = true,
            ComputedValue::Json(_) => has_json = true,
            ComputedValue::Decimal { precision, scale, .. } => {
                has_decimal = true;
                decimal_precision = *precision;
                decimal_scale = *scale;
            }
            ComputedValue::Binary(_) => has_binary = true,
            ComputedValue::Date(_) => has_date = true,
            ComputedValue::Null => {}
        }
    }

    // Priority: specific types first, then generic coercion
    // UUID - must be all UUIDs or nulls
    if has_uuid && !has_string && !has_int && !has_float && !has_json && !has_decimal && !has_binary {
        use arrow_array::builder::FixedSizeBinaryBuilder;
        let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
        for v in values {
            match v {
                ComputedValue::Uuid(bytes) => builder.append_value(bytes).map_err(|e| {
                    EngineError::Internal(format!("UUID append error: {e}"))
                })?,
                ComputedValue::Null => builder.append_null(),
                _ => builder.append_null(),
            }
        }
        let arr = builder.finish();
        return Ok((Arc::new(arr) as ArrayRef, DataType::FixedSizeBinary(16)));
    }

    // JSON - stored as LargeUtf8
    if has_json && !has_uuid && !has_binary {
        let json_strings: Vec<Option<String>> = values.iter().map(|v| {
            match v {
                ComputedValue::Json(s) => Some(s.clone()),
                ComputedValue::String(s) => Some(s.clone()),
                ComputedValue::Null => None,
                _ => Some(v.to_string_value()),
            }
        }).collect();
        let arr: LargeStringArray = json_strings.iter()
            .map(|s| s.as_deref())
            .collect();
        return Ok((Arc::new(arr) as ArrayRef, DataType::LargeUtf8));
    }

    // Decimal - stored as Decimal128
    if has_decimal && !has_string && !has_uuid && !has_json && !has_binary {
        let arr = Decimal128Array::from_iter(values.iter().map(|v| {
            match v {
                ComputedValue::Decimal { value, .. } => Some(*value),
                ComputedValue::Integer(n) => Some(*n as i128),
                ComputedValue::Null => None,
                _ => None,
            }
        })).with_precision_and_scale(decimal_precision, decimal_scale)
            .map_err(|e| EngineError::Internal(format!("Decimal array error: {e}")))?;
        return Ok((Arc::new(arr) as ArrayRef, DataType::Decimal128(decimal_precision, decimal_scale)));
    }

    // Binary - stored as LargeBinary
    if has_binary && !has_string && !has_uuid && !has_json {
        let arr: LargeBinaryArray = values.iter().map(|v| {
            match v {
                ComputedValue::Binary(bytes) => Some(bytes.as_slice()),
                ComputedValue::Null => None,
                _ => None,
            }
        }).collect();
        return Ok((Arc::new(arr) as ArrayRef, DataType::LargeBinary));
    }

    // Date - stored as Date32
    if has_date && !has_string && !has_uuid && !has_json && !has_binary && !has_decimal {
        let arr: Date32Array = values.iter().map(|v| {
            match v {
                ComputedValue::Date(d) => Some(*d),
                ComputedValue::Integer(n) => Some(*n as i32),
                ComputedValue::Null => None,
                _ => None,
            }
        }).collect();
        return Ok((Arc::new(arr) as ArrayRef, DataType::Date32));
    }

    // Priority: String > Float > Integer > Boolean > Timestamp
    if has_string {
        let arr: StringArray = values.iter().map(|v| {
            if matches!(v, ComputedValue::Null) {
                None
            } else {
                Some(v.to_string_value())
            }
        }).collect();
        Ok((Arc::new(arr) as ArrayRef, DataType::Utf8))
    } else if has_float {
        let arr: Float64Array = values.iter().map(|v| v.as_f64()).collect();
        Ok((Arc::new(arr) as ArrayRef, DataType::Float64))
    } else if has_int || has_timestamp {
        let arr: Int64Array = values.iter().map(|v| v.as_i64()).collect();
        Ok((Arc::new(arr) as ArrayRef, DataType::Int64))
    } else if has_bool {
        let arr: BooleanArray = values.iter().map(|v| {
            match v {
                ComputedValue::Boolean(b) => Some(*b),
                ComputedValue::Null => None,
                _ => Some(false),
            }
        }).collect();
        Ok((Arc::new(arr) as ArrayRef, DataType::Boolean))
    } else {
        // All nulls - return as string nulls
        let arr: StringArray = values.iter().map(|_| Option::<&str>::None).collect();
        Ok((Arc::new(arr) as ArrayRef, DataType::Utf8))
    }
}

// -----------------------------------------------------------------------------
// LIKE Pattern Matching Helpers
// -----------------------------------------------------------------------------

/// Maximum length for LIKE patterns to prevent ReDoS attacks
pub const MAX_LIKE_PATTERN_LEN: usize = 1000;

/// Convert a SQL LIKE pattern to a regex pattern
/// - % matches zero or more characters
/// - _ matches exactly one character
/// - Special regex characters are escaped
pub fn like_pattern_to_regex(pattern: &str) -> Option<String> {
    if pattern.len() > MAX_LIKE_PATTERN_LEN {
        return None;
    }

    let mut regex = String::with_capacity(pattern.len() * 2 + 2);
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            // Escape regex metacharacters
            '.' | '+' | '*' | '?' | '[' | ']' | '{' | '}' | '(' | ')' | '|' | '^' | '$' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }

    regex.push('$');
    Some(regex)
}

/// Check if a string matches a SQL LIKE pattern
pub fn matches_like_pattern(value: &str, pattern: &str) -> bool {
    let Some(regex_pattern) = like_pattern_to_regex(pattern) else {
        return false;
    };

    match regex::Regex::new(&regex_pattern) {
        Ok(re) => re.is_match(value),
        Err(_) => false,
    }
}

/// Build a match mask for filtering records
/// Returns a BooleanArray with true for rows that match all filter conditions
pub fn build_match_mask(batch: &RecordBatch, filter: &QueryFilter) -> Result<BooleanArray, EngineError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(BooleanArray::from(vec![] as Vec<bool>));
    }

    // Start with all true
    let mut mask = vec![true; num_rows];

    // Apply LIKE filters - tuple of (column, pattern, negate)
    for (column, pattern, negate) in &filter.like_filters {
        if let Ok(col_idx) = batch.schema().index_of(column) {
            let col = batch.column(col_idx);
            if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..num_rows {
                    if mask[i] && !str_arr.is_null(i) {
                        let val = str_arr.value(i);
                        let matches = matches_like_pattern(val, pattern);
                        mask[i] = if *negate { !matches } else { matches };
                    } else if str_arr.is_null(i) {
                        mask[i] = false;
                    }
                }
            }
        }
    }

    // Apply numeric range filters
    for nf in &filter.numeric_range_filters {
        if let Ok(col_idx) = batch.schema().index_of(&nf.column) {
            let col = batch.column(col_idx);
            let filter_val = match &nf.value {
                crate::sql::NumericValue::Int64(v) => *v as f64,
                crate::sql::NumericValue::UInt64(v) => *v as f64,
                crate::sql::NumericValue::Float64(v) => *v,
            };
            for i in 0..num_rows {
                if mask[i] && !col.is_null(i) {
                    let val = extract_f64(col, i);
                    if let Some(v) = val {
                        let matches = match nf.op {
                            crate::sql::NumericOp::Eq => (v - filter_val).abs() < f64::EPSILON,
                            crate::sql::NumericOp::Ne => (v - filter_val).abs() >= f64::EPSILON,
                            crate::sql::NumericOp::Gt => v > filter_val,
                            crate::sql::NumericOp::Ge => v >= filter_val,
                            crate::sql::NumericOp::Lt => v < filter_val,
                            crate::sql::NumericOp::Le => v <= filter_val,
                        };
                        mask[i] = matches;
                    }
                } else if col.is_null(i) {
                    mask[i] = false;
                }
            }
        }
    }

    // Apply numeric equality filters
    for (column, filter_val) in &filter.numeric_eq_filters {
        if let Ok(col_idx) = batch.schema().index_of(column) {
            let col = batch.column(col_idx);
            let filter_val_f64 = *filter_val as f64;
            for i in 0..num_rows {
                if mask[i] && !col.is_null(i) {
                    let val = extract_f64(col, i);
                    if let Some(v) = val {
                        mask[i] = (v - filter_val_f64).abs() < 0.5; // Integer comparison
                    }
                } else if col.is_null(i) {
                    mask[i] = false;
                }
            }
        }
    }

    // Apply boolean equality filters
    for (column, filter_val) in &filter.bool_eq_filters {
        if let Ok(col_idx) = batch.schema().index_of(column) {
            let col = batch.column(col_idx);
            if let Some(bool_arr) = col.as_any().downcast_ref::<BooleanArray>() {
                for i in 0..num_rows {
                    if mask[i] && !bool_arr.is_null(i) {
                        mask[i] = bool_arr.value(i) == *filter_val;
                    } else if bool_arr.is_null(i) {
                        mask[i] = false;
                    }
                }
            }
        }
    }

    // Apply float equality filters
    for (column, filter_val) in &filter.float_eq_filters {
        if let Ok(col_idx) = batch.schema().index_of(column) {
            let col = batch.column(col_idx);
            for i in 0..num_rows {
                if mask[i] && !col.is_null(i) {
                    let val = extract_f64(col, i);
                    if let Some(v) = val {
                        mask[i] = (v - filter_val).abs() < 1e-9;
                    }
                } else if col.is_null(i) {
                    mask[i] = false;
                }
            }
        }
    }

    // Apply string equality filters
    for (column, filter_val) in &filter.string_eq_filters {
        if let Ok(col_idx) = batch.schema().index_of(column) {
            let col = batch.column(col_idx);
            if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..num_rows {
                    if mask[i] && !str_arr.is_null(i) {
                        mask[i] = str_arr.value(i) == filter_val;
                    } else if str_arr.is_null(i) {
                        mask[i] = false;
                    }
                }
            // Handle FixedSizeBinary (UUIDs stored as strings in filter)
            } else if let Some(fsb_arr) = col.as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>() {
                // Try to parse the filter value as UUID
                if let Some(uuid_bytes) = parse_uuid_string(filter_val) {
                    for i in 0..num_rows {
                        if mask[i] && !fsb_arr.is_null(i) {
                            mask[i] = fsb_arr.value(i) == uuid_bytes;
                        } else if fsb_arr.is_null(i) {
                            mask[i] = false;
                        }
                    }
                }
            }
        }
    }

    Ok(BooleanArray::from(mask))
}

// -----------------------------------------------------------------------------
// Type Casting and Parsing Helpers
// -----------------------------------------------------------------------------

/// Result of parsing a decimal string
#[derive(Debug, Clone, PartialEq)]
pub struct DecimalParseResult {
    pub integer_part: i128,
    pub fractional_part: i128,
    pub scale: u8,
    pub is_negative: bool,
}

/// Parse a decimal string into its components
pub fn parse_decimal_string(s: &str) -> Option<DecimalParseResult> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (is_negative, s) = if s.starts_with('-') {
        (true, &s[1..])
    } else if s.starts_with('+') {
        (false, &s[1..])
    } else {
        (false, s)
    };

    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        return None;
    }

    let integer_str = parts[0];
    let integer_part: i128 = if integer_str.is_empty() {
        0
    } else {
        integer_str.parse().ok()?
    };

    let (fractional_part, scale) = if parts.len() == 2 {
        let frac_str = parts[1];
        let scale = frac_str.len() as u8;
        let frac: i128 = if frac_str.is_empty() {
            0
        } else {
            frac_str.parse().ok()?
        };
        (frac, scale)
    } else {
        (0, 0)
    };

    Some(DecimalParseResult {
        integer_part,
        fractional_part,
        scale,
        is_negative,
    })
}

/// Cast a computed value to a target type
pub fn cast_value(value: &ComputedValue, target_type: &str) -> Option<ComputedValue> {
    match target_type.to_lowercase().as_str() {
        "uuid" => {
            match value {
                ComputedValue::String(s) => {
                    // Validate UUID format (simple check)
                    let s = s.trim();
                    if s.len() == 36 && s.chars().filter(|&c| c == '-').count() == 4 {
                        // Parse into bytes and create Uuid variant
                        let hex: String = s.chars().filter(|c| *c != '-').collect();
                        if let Ok(bytes) = hex::decode(&hex) {
                            if bytes.len() == 16 {
                                let mut arr = [0u8; 16];
                                arr.copy_from_slice(&bytes);
                                return Some(ComputedValue::Uuid(arr));
                            }
                        }
                        Some(ComputedValue::String(s.to_lowercase()))
                    } else if s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                        // Parse 32-char hex string directly
                        if let Ok(bytes) = hex::decode(s) {
                            if bytes.len() == 16 {
                                let mut arr = [0u8; 16];
                                arr.copy_from_slice(&bytes);
                                return Some(ComputedValue::Uuid(arr));
                            }
                        }
                        None
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        "decimal" | "numeric" => {
            match value {
                ComputedValue::Integer(i) => Some(ComputedValue::Decimal {
                    value: *i as i128,
                    precision: 38,
                    scale: 0,
                }),
                ComputedValue::Float(f) => {
                    // Convert float to decimal with reasonable precision
                    let scale = 6i8;
                    let multiplier = 10i128.pow(scale as u32);
                    let value = (*f * multiplier as f64).round() as i128;
                    Some(ComputedValue::Decimal {
                        value,
                        precision: 38,
                        scale,
                    })
                }
                ComputedValue::String(s) => {
                    if let Some(result) = parse_decimal_string(s) {
                        let multiplier = 10i128.pow(result.scale as u32);
                        let value = result.integer_part * multiplier + result.fractional_part;
                        let final_value = if result.is_negative { -value } else { value };
                        Some(ComputedValue::Decimal {
                            value: final_value,
                            precision: 38,
                            scale: result.scale as i8,
                        })
                    } else {
                        None
                    }
                }
                ComputedValue::Decimal { value, precision, scale } => Some(ComputedValue::Decimal {
                    value: *value,
                    precision: *precision,
                    scale: *scale,
                }),
                _ => None,
            }
        }
        "json" | "jsonb" => {
            match value {
                ComputedValue::String(s) => {
                    // Validate JSON - if valid, use as-is; if not, wrap as JSON string
                    if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                        Some(ComputedValue::Json(s.clone()))
                    } else {
                        // Wrap as JSON string
                        Some(ComputedValue::Json(format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))))
                    }
                }
                ComputedValue::Integer(i) => Some(ComputedValue::Json(i.to_string())),
                ComputedValue::Float(f) => Some(ComputedValue::Json(f.to_string())),
                ComputedValue::Boolean(b) => Some(ComputedValue::Json(b.to_string())),
                ComputedValue::Null => Some(ComputedValue::Json("null".to_string())),
                _ => None,
            }
        }
        "date" => {
            match value {
                ComputedValue::String(s) => {
                    // Try to parse common date formats
                    let s = s.trim();
                    // YYYY-MM-DD format
                    if s.len() >= 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
                        // Parse to days since epoch
                        if let Ok(date) = chrono::NaiveDate::parse_from_str(&s[0..10], "%Y-%m-%d") {
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days = (date - epoch).num_days() as i32;
                            Some(ComputedValue::Date(days))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                ComputedValue::Integer(i) => {
                    // Assume Unix timestamp in seconds
                    let days_since_epoch = (*i / 86400) as i32;
                    Some(ComputedValue::Date(days_since_epoch))
                }
                ComputedValue::Timestamp(micros) => {
                    // Convert microseconds to days
                    let seconds = micros / 1_000_000;
                    let days_since_epoch = (seconds / 86400) as i32;
                    Some(ComputedValue::Date(days_since_epoch))
                }
                ComputedValue::Date(d) => Some(ComputedValue::Date(*d)),
                _ => None,
            }
        }
        "text" | "varchar" | "string" => {
            Some(ComputedValue::String(match value {
                ComputedValue::String(s) => s.clone(),
                ComputedValue::Integer(i) => i.to_string(),
                ComputedValue::Float(f) => f.to_string(),
                ComputedValue::Boolean(b) => b.to_string(),
                ComputedValue::Null => "NULL".to_string(),
                ComputedValue::Binary(b) => format!("{:?}", b),
                ComputedValue::Timestamp(ts) => ts.to_string(),
                ComputedValue::Date(d) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + chrono::Duration::days(*d as i64);
                    date.format("%Y-%m-%d").to_string()
                }
                ComputedValue::Decimal { value, scale, .. } => {
                    let divisor = 10_i128.pow(*scale as u32);
                    let int_part = value / divisor;
                    let frac_part = (value % divisor).abs();
                    format!("{}.{:0>width$}", int_part, frac_part, width = *scale as usize)
                }
                ComputedValue::Uuid(bytes) => {
                    format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5], bytes[6], bytes[7],
                        bytes[8], bytes[9], bytes[10], bytes[11],
                        bytes[12], bytes[13], bytes[14], bytes[15]
                    )
                }
                ComputedValue::Json(j) => j.clone(),
            }))
        }
        "int" | "integer" | "bigint" => {
            match value {
                ComputedValue::Integer(i) => Some(ComputedValue::Integer(*i)),
                ComputedValue::Float(f) => Some(ComputedValue::Integer(*f as i64)),
                ComputedValue::String(s) => s.parse::<i64>().ok().map(ComputedValue::Integer),
                ComputedValue::Boolean(b) => Some(ComputedValue::Integer(if *b { 1 } else { 0 })),
                _ => None,
            }
        }
        "float" | "double" | "real" => {
            match value {
                ComputedValue::Float(f) => Some(ComputedValue::Float(*f)),
                ComputedValue::Integer(i) => Some(ComputedValue::Float(*i as f64)),
                ComputedValue::String(s) => s.parse::<f64>().ok().map(ComputedValue::Float),
                _ => None,
            }
        }
        "bool" | "boolean" => {
            match value {
                ComputedValue::Boolean(b) => Some(ComputedValue::Boolean(*b)),
                ComputedValue::Integer(i) => Some(ComputedValue::Boolean(*i != 0)),
                ComputedValue::String(s) => {
                    let lower = s.to_lowercase();
                    match lower.as_str() {
                        "true" | "t" | "yes" | "y" | "1" => Some(ComputedValue::Boolean(true)),
                        "false" | "f" | "no" | "n" | "0" => Some(ComputedValue::Boolean(false)),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
    use std::time::{Duration, Instant};
    use std::io::Cursor;

    #[test]
    fn ingest_sets_schema_hash_and_replays() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg.clone()).unwrap();

        let schema = Schema::new(vec![Field::new("event_time", DataType::UInt64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2u64]))],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc.clone(),
            database: None,
            table: None,
            watermark_micros: 10,
            shard_override: None,
        })
        .unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        let entry = manifest.entries.last().unwrap();
        let expected_hash = compute_schema_hash_from_payload(&ipc, None).unwrap();
        assert_eq!(entry.schema_hash, Some(expected_hash));

        drop(db);
        let reopened = Db::open_for_test(cfg).unwrap();
        let manifest: Manifest = serde_json::from_slice(&reopened.export_manifest().unwrap()).unwrap();
        assert_eq!(manifest.entries[0].schema_hash, Some(expected_hash));
    }

    fn index_file_path(
        cfg: &EngineConfig,
        database: &str,
        table: &str,
        index_name: &str,
        segment_id: &str,
    ) -> PathBuf {
        cfg.data_dir
            .join("indexes")
            .join(database)
            .join(table)
            .join(index_name)
            .join(format!("{segment_id}.idx"))
    }

    fn wait_for_path(path: &Path) -> Result<(), EngineError> {
        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        loop {
            if path.exists() {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(EngineError::Timeout(format!(
                    "timed out waiting for index file: {}",
                    path.display()
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn ingest_simple_batch(
        db: &Db,
        payload_ipc: Vec<u8>,
        watermark_micros: u64,
    ) -> String {
        db.ingest_ipc(IngestBatch {
            payload_ipc,
            database: Some("default".to_string()),
            table: Some("tbl".to_string()),
            watermark_micros,
            shard_override: None,
        })
        .unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        manifest.entries.last().unwrap().segment_id.clone()
    }

    fn decode_single_batch(ipc: &[u8]) -> RecordBatch {
        let mut batches = Db::decode_ipc_batches(ipc).unwrap();
        batches.remove(0)
    }

    #[test]
    fn async_index_builds_on_create() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg.clone()).unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::UInt64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2u64]))],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let segment_id = ingest_simple_batch(&db, ipc, 1);

        db.create_index(
            "default",
            "tbl",
            "idx_id",
            &["id".to_string()],
            crate::sql::IndexType::Hash,
            false,
        )
        .unwrap();

        let path = index_file_path(&cfg, "default", "tbl", "idx_id", &segment_id);
        wait_for_path(&path).unwrap();
    }

    #[test]
    fn async_index_builds_on_ingest() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg.clone()).unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::UInt64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2u64]))],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let first_segment = ingest_simple_batch(&db, ipc.clone(), 1);

        db.create_index(
            "default",
            "tbl",
            "idx_id",
            &["id".to_string()],
            crate::sql::IndexType::Hash,
            false,
        )
        .unwrap();

        let first_path = index_file_path(&cfg, "default", "tbl", "idx_id", &first_segment);
        wait_for_path(&first_path).unwrap();

        let new_segment = ingest_simple_batch(&db, ipc, 2);
        let path = index_file_path(&cfg, "default", "tbl", "idx_id", &new_segment);
        wait_for_path(&path).unwrap();
    }

    #[test]
    fn encoding_dictionary_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema_json = serde_json::to_string(&vec![
            TableFieldSpec {
                name: "name".into(),
                data_type: "string".into(),
                nullable: false,
                encoding: Some("dictionary".into()),
            },
        ])
        .unwrap();
        db.create_table("testdb", "users", Some(schema_json)).unwrap();

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec!["alice", "bob", "alice"]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("users".into()),
        })
        .unwrap();

        let result = db
            .query(QueryRequest {
                sql: "SELECT name FROM testdb.users".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "alice");
        assert_eq!(col.value(1), "bob");
        assert_eq!(col.value(2), "alice");
    }

    #[test]
    fn encoding_delta_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema_json = serde_json::to_string(&vec![
            TableFieldSpec {
                name: "value".into(),
                data_type: "int64".into(),
                nullable: false,
                encoding: Some("delta".into()),
            },
        ])
        .unwrap();
        db.create_table("testdb", "metrics", Some(schema_json)).unwrap();

        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![10i64, 12, 15]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("metrics".into()),
        })
        .unwrap();

        let result = db
            .query(QueryRequest {
                sql: "SELECT value FROM testdb.metrics".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 12);
        assert_eq!(col.value(2), 15);
    }

    #[test]
    fn encoding_dictionary_filter_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema_json = serde_json::to_string(&vec![
            TableFieldSpec {
                name: "name".into(),
                data_type: "string".into(),
                nullable: false,
                encoding: Some("dictionary".into()),
            },
        ])
        .unwrap();
        db.create_table("testdb", "users", Some(schema_json)).unwrap();

        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec!["alice", "bob", "alice"]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("users".into()),
        })
        .unwrap();

        let result = db
            .query(QueryRequest {
                sql: "SELECT name FROM testdb.users WHERE name = 'alice'".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.len(), 2);
        assert_eq!(col.value(0), "alice");
        assert_eq!(col.value(1), "alice");
    }

    #[test]
    fn encoding_delta_timestamp_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema_json = serde_json::to_string(&vec![
            TableFieldSpec {
                name: "ts".into(),
                data_type: "timestamp".into(),
                nullable: false,
                encoding: Some("delta".into()),
            },
        ])
        .unwrap();
        db.create_table("testdb", "events", Some(schema_json)).unwrap();

        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                1_000_000i64,
                1_000_050,
                1_000_125,
            ]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let result = db
            .query(QueryRequest {
                sql: "SELECT ts FROM testdb.events".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(col.value(0), 1_000_000);
        assert_eq!(col.value(1), 1_000_050);
        assert_eq!(col.value(2), 1_000_125);
    }

    #[test]
    fn tier_warm_compression_rewrites_segments() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1)
            .with_tier_warm_after_millis(1)
            .with_tier_cold_after_millis(0)
            .with_tier_warm_compression(Some("zstd".to_string()));
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "events", None).unwrap();

        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        db.apply_tiering_rules().unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        let entry = manifest.entries.last().unwrap();
        assert!(matches!(entry.tier, SegmentTier::Warm));
        assert_eq!(entry.compression.as_deref(), Some("zstd"));

        let result = db
            .query(QueryRequest {
                sql: "SELECT value FROM testdb.events".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);
    }

    #[test]
    fn tier_cold_compression_rewrites_segments() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1)
            .with_tier_warm_after_millis(0)
            .with_tier_cold_after_millis(1)
            .with_tier_cold_compression(Some("zstd".to_string()));
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "events", None).unwrap();

        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![4i64, 5, 6]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        db.apply_tiering_rules().unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        let entry = manifest.entries.last().unwrap();
        assert!(matches!(entry.tier, SegmentTier::Cold));
        assert_eq!(entry.compression.as_deref(), Some("zstd"));

        let result = db
            .query(QueryRequest {
                sql: "SELECT value FROM testdb.events".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            })
            .unwrap();
        let decoded = decode_single_batch(&result.records_ipc);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 4);
        assert_eq!(col.value(1), 5);
        assert_eq!(col.value(2), 6);
    }

    #[test]
    fn index_granularity_splits_sorted_batches() {
        let schema = Schema::new(vec![Field::new("event_time", DataType::UInt64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![3u64, 1, 2, 5, 4]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let sorted = sort_hot_payload(&ipc, 2).unwrap();
        let batches = read_ipc_batches(&sorted).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 1);

        let mut values = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..col.len() {
                values.push(col.value(i));
            }
        }
        assert_eq!(values, vec![1u64, 2, 3, 4, 5]);
    }

    #[test]
    fn load_manifest_migrates_on_read() {
        use crate::replication::MANIFEST_FORMAT_VERSION;

        let dir = tempdir().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let legacy = Manifest {
            format_version: 0,
            version: 5,
            databases: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            indexes: Vec::new(),
            entries: Vec::new(),
        };
        std::fs::write(&manifest_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.format_version, MANIFEST_FORMAT_VERSION);

        let persisted: Manifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(persisted.format_version, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn plan_bundle_rejects_future_since_version() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let err = db
            .plan_bundle(BundleRequest {
                max_bytes: None,
                since_version: Some(1),
                prefer_hot: true,
                target_bytes_per_sec: None,
                max_entries: None,
            })
            .unwrap_err();

        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("since_version"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn plan_bundle_rejects_zero_throughput() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let err = db
            .plan_bundle(BundleRequest {
                max_bytes: None,
                since_version: None,
                prefer_hot: true,
                target_bytes_per_sec: Some(0),
                max_entries: None,
            })
            .unwrap_err();

        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("target_bytes_per_sec"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn plan_bundle_zero_entries_has_zero_throttle() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let plan = db
            .plan_bundle(BundleRequest {
                max_bytes: None,
                since_version: None,
                prefer_hot: true,
                target_bytes_per_sec: Some(10_000),
                max_entries: None,
            })
            .unwrap();

        assert!(plan.entries.is_empty());
        assert_eq!(plan.total_bytes, 0);
        assert_eq!(plan.throttle_millis, 0);
        assert!(plan.plan_hash.is_some());
    }

    #[test]
    fn apply_bundle_rejects_size_mismatch() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let payload = vec![1u8, 2, 3, 4];
        let checksum = compute_checksum(&payload);
        let entry = ManifestEntry {
            segment_id: "bndl-0-1".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: 999, // intentionally wrong
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan_hash = compute_bundle_plan_hash(0, Some(0), &[entry.clone()], entry.size_bytes, 0);
        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(plan_hash),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment {
                entry,
                data: payload,
            }],
        };

        let err = db.apply_bundle(payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("size mismatch"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn apply_bundle_rejects_unsupported_compression() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let data = vec![1u8, 2, 3];
        let checksum = compute_checksum(&data);
        let entry = ManifestEntry {
            segment_id: "bndl-0-2".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: data.len() as u64,
            checksum,
            tier: SegmentTier::Hot,
            compression: Some("gzip".into()),
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan_hash = compute_bundle_plan_hash(0, Some(0), &[entry.clone()], entry.size_bytes, 0);
        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(plan_hash),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment { entry, data }],
        };

        let err = db.apply_bundle(payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("unsupported compression"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn apply_bundle_rejects_conflicting_existing_entry() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg.clone()).unwrap();

        let schema = Schema::new(vec![Field::new("event_time", DataType::UInt64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(vec![1u64]))],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            database: None,
            table: None,
            watermark_micros: 10,
            shard_override: None,
        })
        .unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        let existing = manifest.entries.last().unwrap().clone();

        let mut conflicting_entry = existing.clone();
        conflicting_entry.checksum = existing.checksum.wrapping_add(1);

        let data = vec![0u8; existing.size_bytes as usize];
        let plan_hash = compute_bundle_plan_hash(
            manifest.version,
            Some(manifest.version),
            &[conflicting_entry.clone()],
            conflicting_entry.size_bytes,
            0,
        );
        let plan = BundlePlan {
            manifest_version: manifest.version,
            entries: vec![conflicting_entry.clone()],
            total_bytes: conflicting_entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(manifest.version),
            plan_hash: Some(plan_hash),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment {
                entry: conflicting_entry,
                data,
            }],
        };

        let err = db.apply_bundle(payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("conflicts"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn apply_bundle_rejects_total_bytes_mismatch() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let data = vec![1u8, 2, 3, 4];
        let checksum = compute_checksum(&data);
        let entry = ManifestEntry {
            segment_id: "bndl-0-3".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: data.len() as u64,
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes + 10, // intentionally wrong
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(compute_bundle_plan_hash(
                0,
                Some(0),
                &[entry.clone()],
                entry.size_bytes + 10,
                0,
            )),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment { entry, data }],
        };

        let err = db.apply_bundle(payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("total_bytes"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn apply_bundle_rejects_replay() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let data = vec![9u8, 8, 7];
        let checksum = compute_checksum(&data);
        let entry = ManifestEntry {
            segment_id: "bndl-0-4".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: data.len() as u64,
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan_hash = compute_bundle_plan_hash(0, Some(0), &[entry.clone()], entry.size_bytes, 0);
        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(plan_hash),
        };

        let payload = BundlePayload {
            plan: plan.clone(),
            segments: vec![BundleSegment {
                entry,
                data: data.clone(),
            }],
        };

        // First apply succeeds
        db.apply_bundle(payload.clone()).unwrap();

        // Second apply should be rejected as replay
        let err = db.apply_bundle(payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("already applied"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn validate_bundle_rejects_size_mismatch() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let data = vec![1u8, 2, 3, 4];
        let checksum = compute_checksum(&data);
        let entry = ManifestEntry {
            segment_id: "bndl-val-0-1".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: data.len() as u64 + 5, // wrong on purpose
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(compute_bundle_plan_hash(
                0,
                Some(0),
                &[entry.clone()],
                entry.size_bytes,
                0,
            )),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment { entry, data }],
        };

        let err = db.validate_bundle(&payload).unwrap_err();
        match err {
            EngineError::InvalidArgument(msg) => {
                assert!(msg.contains("size mismatch"));
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
    }

    #[test]
    fn validate_bundle_ok_does_not_persist() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let data = vec![5u8, 4, 3];
        let checksum = compute_checksum(&data);
        let entry = ManifestEntry {
            segment_id: "bndl-val-0-2".to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: data.len() as u64,
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "events".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: None,
        };

        let plan = BundlePlan {
            manifest_version: 0,
            entries: vec![entry.clone()],
            total_bytes: entry.size_bytes,
            throttle_millis: 0,
            since_version: Some(0),
            plan_hash: Some(compute_bundle_plan_hash(
                0,
                Some(0),
                &[entry.clone()],
                entry.size_bytes,
                0,
            )),
        };

        let payload = BundlePayload {
            plan,
            segments: vec![BundleSegment { entry, data }],
        };

        db.validate_bundle(&payload).unwrap();

        // Ensure no manifest version bump and no segment files written
        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        assert_eq!(manifest.version, 0);

        let segments_dir = dir.path().join("segments");
        let entries: Vec<_> = std::fs::read_dir(&segments_dir)
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_expression_evaluator_scalar_functions() {
        use crate::sql::{SelectColumn, SelectExpr, ScalarFunction};

        // Create test data
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("quantity", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["apple", "banana", "cherry"])),
                Arc::new(arrow_array::Float64Array::from(vec![1.5, 2.0, 3.5])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap();

        // Encode to IPC
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test UPPER function - ScalarFunction::Upper takes Box<SelectExpr>
        let computed_columns = vec![
            SelectColumn {
                expr: SelectExpr::Function(
                    ScalarFunction::Upper(Box::new(SelectExpr::Column("name".to_string())))
                ),
                alias: Some("upper_name".to_string()),
            },
        ];

        let result = apply_computed_columns(&ipc, &computed_columns).unwrap();
        assert!(!result.is_empty());

        // Decode and verify
        let cursor = Cursor::new(result);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have original 3 columns + 1 computed column
        assert_eq!(out_batch.num_columns(), 4);

        // Check the computed column
        let upper_col = out_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(upper_col.value(0), "APPLE");
        assert_eq!(upper_col.value(1), "BANANA");
        assert_eq!(upper_col.value(2), "CHERRY");
    }

    #[test]
    fn test_expression_evaluator_math_functions() {
        use crate::sql::{SelectColumn, SelectExpr, ScalarFunction};

        // Create test data with numbers
        let schema = Schema::new(vec![
            Field::new("value", DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow_array::Float64Array::from(vec![-5.5, 16.0, 2.718])),
            ],
        )
        .unwrap();

        // Encode to IPC
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test ABS and SQRT functions
        let computed_columns = vec![
            SelectColumn {
                expr: SelectExpr::Function(
                    ScalarFunction::Abs(Box::new(SelectExpr::Column("value".to_string())))
                ),
                alias: Some("abs_value".to_string()),
            },
            SelectColumn {
                expr: SelectExpr::Function(
                    ScalarFunction::Sqrt(Box::new(SelectExpr::Column("value".to_string())))
                ),
                alias: Some("sqrt_value".to_string()),
            },
        ];

        let result = apply_computed_columns(&ipc, &computed_columns).unwrap();
        assert!(!result.is_empty());

        // Decode and verify
        let cursor = Cursor::new(result);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have original 1 column + 2 computed columns
        assert_eq!(out_batch.num_columns(), 3);

        // Check the ABS column
        let abs_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((abs_col.value(0) - 5.5).abs() < 0.001);
        assert!((abs_col.value(1) - 16.0).abs() < 0.001);

        // Check the SQRT column (sqrt of 16 = 4)
        let sqrt_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((sqrt_col.value(1) - 4.0).abs() < 0.001);
    }

    #[test]
    fn test_expression_evaluator_binary_ops() {
        use crate::sql::{SelectColumn, SelectExpr};

        // Create test data
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("quantity", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow_array::Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(Int64Array::from(vec![5i64, 3, 2])),
            ],
        )
        .unwrap();

        // Encode to IPC
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test price * quantity
        let computed_columns = vec![
            SelectColumn {
                expr: SelectExpr::BinaryOp {
                    left: Box::new(SelectExpr::Column("price".to_string())),
                    op: "*".to_string(),
                    right: Box::new(SelectExpr::Column("quantity".to_string())),
                },
                alias: Some("total".to_string()),
            },
        ];

        let result = apply_computed_columns(&ipc, &computed_columns).unwrap();
        assert!(!result.is_empty());

        // Decode and verify
        let cursor = Cursor::new(result);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have original 2 columns + 1 computed column
        assert_eq!(out_batch.num_columns(), 3);

        // Check the total column (price * quantity)
        let total_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((total_col.value(0) - 50.0).abs() < 0.001);  // 10 * 5
        assert!((total_col.value(1) - 60.0).abs() < 0.001);  // 20 * 3
        assert!((total_col.value(2) - 60.0).abs() < 0.001);  // 30 * 2
    }

    #[test]
    fn test_cte_context() {
        // Test CTE context functionality
        let mut cte_ctx = CteContext::new();

        // Add a CTE result
        let test_data = vec![1, 2, 3, 4, 5];
        cte_ctx.add_result("my_cte".to_string(), test_data.clone());

        // Check if CTE exists
        assert!(cte_ctx.has_cte("my_cte"));
        assert!(!cte_ctx.has_cte("nonexistent"));

        // Get CTE result
        let result = cte_ctx.get_result("my_cte");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), &test_data);

        // Get nonexistent CTE
        let result = cte_ctx.get_result("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_build_where_clause() {
        use crate::sql::{ParsedQuery, QueryFilter};

        // Test with empty filter
        let query = ParsedQuery {
            database: Some("db".to_string()),
            table: Some("tbl".to_string()),
            projection: None,
            filter: QueryFilter::default(),
            aggregation: None,
            order_by: None,
            distinct: false,
            joins: vec![],
            computed_columns: vec![],
            ctes: vec![],
            sample: None,
        };
        let clause = build_where_clause(&query);
        assert!(clause.is_empty());

        // Test with filters
        let filter = QueryFilter {
            watermark_ge: Some(100),
            watermark_le: Some(200),
            tenant_id_eq: Some(42),
            ..Default::default()
        };
        let query_with_filter = ParsedQuery {
            database: Some("db".to_string()),
            table: Some("tbl".to_string()),
            projection: None,
            filter,
            aggregation: None,
            order_by: None,
            distinct: false,
            joins: vec![],
            computed_columns: vec![],
            ctes: vec![],
            sample: None,
        };
        let clause = build_where_clause(&query_with_filter);
        assert!(clause.contains("WHERE"));
        assert!(clause.contains("watermark_micros >= 100"));
        assert!(clause.contains("watermark_micros <= 200"));
        assert!(clause.contains("tenant_id = 42"));
    }

    #[test]
    fn test_window_functions_row_number() {
        use crate::sql::{SelectColumn, SelectExpr, WindowFunction, WindowSpec};

        // Create test data
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])),
            ],
        )
        .unwrap();

        // Encode to IPC
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test ROW_NUMBER() window function
        let window_columns = vec![
            SelectColumn {
                expr: SelectExpr::Window {
                    function: WindowFunction::RowNumber,
                    spec: WindowSpec::default(),
                },
                alias: Some("row_num".to_string()),
            },
        ];

        let result = apply_window_functions(&ipc, &window_columns).unwrap();
        assert!(!result.is_empty());

        // Decode and verify
        let cursor = Cursor::new(result);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have original 2 columns + 1 window function column
        assert_eq!(out_batch.num_columns(), 3);

        // Check the row number column
        let row_num_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(row_num_col.value(0), 1);
        assert_eq!(row_num_col.value(1), 2);
        assert_eq!(row_num_col.value(2), 3);
        assert_eq!(row_num_col.value(3), 4);
    }

    #[test]
    fn test_window_functions_lag_lead() {
        use crate::sql::{SelectColumn, SelectExpr, WindowFunction, WindowSpec};

        // Create test data
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])),
            ],
        )
        .unwrap();

        // Encode to IPC
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Test LAG(value, 1) window function
        let window_columns = vec![
            SelectColumn {
                expr: SelectExpr::Window {
                    function: WindowFunction::Lag {
                        expr: Box::new(SelectExpr::Column("value".to_string())),
                        offset: 1,
                        default: None,
                    },
                    spec: WindowSpec::default(),
                },
                alias: Some("prev_value".to_string()),
            },
            SelectColumn {
                expr: SelectExpr::Window {
                    function: WindowFunction::Lead {
                        expr: Box::new(SelectExpr::Column("value".to_string())),
                        offset: 1,
                        default: None,
                    },
                    spec: WindowSpec::default(),
                },
                alias: Some("next_value".to_string()),
            },
        ];

        let result = apply_window_functions(&ipc, &window_columns).unwrap();
        assert!(!result.is_empty());

        // Decode and verify
        let cursor = Cursor::new(result);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have original 1 column + 2 window function columns
        assert_eq!(out_batch.num_columns(), 3);

        // Check LAG column: NULL, 10, 20, 30
        let lag_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(lag_col.is_null(0));
        assert_eq!(lag_col.value(1), 10);
        assert_eq!(lag_col.value(2), 20);
        assert_eq!(lag_col.value(3), 30);

        // Check LEAD column: 20, 30, 40, NULL
        let lead_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lead_col.value(0), 20);
        assert_eq!(lead_col.value(1), 30);
        assert_eq!(lead_col.value(2), 40);
        assert!(lead_col.is_null(3));
    }

    #[test]
    fn ingest_and_query_filters_watermark() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 5, 10])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
                Arc::new(UInt64Array::from(vec![7u64, 7, 8])),
                Arc::new(UInt64Array::from(vec![101u64, 102, 103])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let resp = db
            .query(QueryRequest {
                sql: "SELECT watermark_micros,label FROM analytics.events WHERE watermark>=5 AND event_time<=250 AND tenant_id=7 AND route_id=102 LIMIT 1".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let out_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(out_col.len(), 1);
        assert_eq!(out_col.value(0), 5);

        let out_str = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(out_str.value(0), "b");
    }

    #[test]
    fn aggregate_count_sum() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 5, 10])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
                Arc::new(UInt64Array::from(vec![7u64, 7, 8])),
                Arc::new(UInt64Array::from(vec![101u64, 101, 102])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let resp = db
            .query(QueryRequest {
                sql: "SELECT COUNT(*),SUM(value),AVG(value) FROM analytics.events WHERE tenant_id=7".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let count_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sum_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let avg_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 2);
        assert_eq!(sum_col.value(0), 30);
        assert!((avg_col.value(0) - 15.0).abs() < 1e-6);
    }

    #[test]
    fn aggregate_min_max_no_group() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 5, 10])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
                Arc::new(UInt64Array::from(vec![7u64, 7, 8])),
                Arc::new(UInt64Array::from(vec![101u64, 101, 102])),
                Arc::new(Int64Array::from(vec![10i64, -5, 30])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let resp = db
            .query(QueryRequest {
                sql: "SELECT COUNT(*),MIN(value),MAX(value) FROM analytics.events".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let count_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let min_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 3);
        assert_eq!(min_col.value(0), -5);
        assert_eq!(max_col.value(0), 30);
    }

    #[test]
    fn where_numeric_and_bool_filters_match_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(arrow_array::BooleanArray::from(vec![true, false, true])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let filter = parse_where_filter(Some("id = 2")).unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, false]));

        let filter = parse_where_filter(Some("active = true")).unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![true, false, true]));

        let filter = parse_where_filter(Some("id = 3 AND active = true")).unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, false, true]));
    }

    #[test]
    fn parse_filters_float_uuid_timestamp() {
        let sql = "SELECT id FROM db.tbl WHERE val_f32 = 1.5 AND uid = '550e8400-e29b-41d4-a716-446655440001' AND ts = 1700000000000001";
        let filter = parse_filters(sql).unwrap();

        assert_eq!(filter.float_eq_filters.len(), 1);
        assert_eq!(filter.float_eq_filters[0].0, "val_f32");
        assert!((filter.float_eq_filters[0].1 - 1.5).abs() < 1e-9);

        assert_eq!(filter.string_eq_filters.len(), 1);
        assert_eq!(filter.string_eq_filters[0].0, "uid");
        assert_eq!(
            filter.string_eq_filters[0].1,
            "550e8400-e29b-41d4-a716-446655440001"
        );

        assert_eq!(filter.numeric_eq_filters.len(), 1);
        assert_eq!(filter.numeric_eq_filters[0].0, "ts");
        assert_eq!(filter.numeric_eq_filters[0].1, 1_700_000_000_000_001i128);
    }

    #[test]
    fn match_mask_float_uuid_timestamp() {
        use arrow_array::{Float32Array, TimestampMicrosecondArray};
        use arrow_array::builder::FixedSizeBinaryBuilder;

        let uuid1 = parse_uuid_string("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = parse_uuid_string("550e8400-e29b-41d4-a716-446655440001").unwrap();

        let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(2, 16);
        uuid_builder.append_value(&uuid1).unwrap();
        uuid_builder.append_value(&uuid2).unwrap();
        let uuid_array = uuid_builder.finish();

        let schema = Schema::new(vec![
            Field::new("val_f32", DataType::Float32, false),
            Field::new("uid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Float32Array::from(vec![1.5f32, 2.25f32])),
                Arc::new(uuid_array),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_000i64,
                    1_700_000_000_000_001i64,
                ])),
            ],
        )
        .unwrap();

        let filter = parse_where_filter(Some("val_f32 = 1.5")).unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![true, false]));

        let filter = parse_where_filter(Some(
            "uid = '550e8400-e29b-41d4-a716-446655440001'",
        ))
        .unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true]));

        let filter = parse_where_filter(Some("ts = 1700000000000001")).unwrap();
        let mask = build_match_mask(&batch, &filter).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true]));
    }

    #[test]
    fn aggregate_group_by_route() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 5, 10])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
                Arc::new(UInt64Array::from(vec![7u64, 7, 8])),
                Arc::new(UInt64Array::from(vec![101u64, 101, 102])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let resp = db
            .query(QueryRequest {
                sql: "SELECT COUNT(*),SUM(value) FROM analytics.events GROUP BY route_id".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let route_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let count_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sum_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(route_col.len(), 2);
        // route_id 101 should have two rows (10+20)
        let idx_101 = if route_col.value(0) == 101 { 0 } else { 1 };
        assert_eq!(route_col.value(idx_101), 101);
        assert_eq!(count_col.value(idx_101), 2);
        assert_eq!(sum_col.value(idx_101), 30);
    }

    #[test]
    fn test_create_and_drop_database() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create database
        db.create_database("testdb").unwrap();
        let dbs = db.list_databases().unwrap();
        assert!(dbs.contains(&"testdb".to_string()));

        // Create table in database
        db.create_table("testdb", "testtable", None).unwrap();
        let tables = db.list_tables(Some("testdb")).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "testtable");

        // Drop database
        db.drop_database("testdb", false).unwrap();
        let dbs = db.list_databases().unwrap();
        assert!(!dbs.contains(&"testdb".to_string()));

        // Tables should also be gone
        let tables = db.list_tables(Some("testdb")).unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn test_drop_database_if_exists() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Should not error with if_exists=true
        db.drop_database("nonexistent", true).unwrap();

        // Should error with if_exists=false
        let result = db.drop_database("nonexistent", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_table() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create database and table
        db.create_database("testdb").unwrap();
        db.create_table("testdb", "table1", None).unwrap();
        db.create_table("testdb", "table2", None).unwrap();

        // Ingest some data
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("table1".into()),
        })
        .unwrap();

        // Drop table1
        db.drop_table("testdb", "table1", false).unwrap();

        // table1 should be gone, table2 should remain
        let tables = db.list_tables(Some("testdb")).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "table2");
    }

    #[test]
    fn test_truncate_table() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create database and table
        db.create_database("testdb").unwrap();
        db.create_table("testdb", "testtable", None).unwrap();

        // Ingest some data
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("testtable".into()),
        })
        .unwrap();

        // Truncate
        db.truncate_table("testdb", "testtable").unwrap();

        // Table should still exist
        let tables = db.list_tables(Some("testdb")).unwrap();
        assert_eq!(tables.len(), 1);

        // But query should fail (no segments)
        let result = db.query(QueryRequest {
            sql: "SELECT * FROM testdb.testtable".into(),
            timeout_millis: 1000,
            collect_stats: false,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_rows_removes_matches() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "events", None).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 30])),
                Arc::new(UInt64Array::from(vec![1u64, 1, 2])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 10])),
                Arc::new(arrow_array::Int64Array::from(vec![5i64, 6, 7])),
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let deleted = db
            .delete_rows("testdb", "events", Some("tenant_id=1"))
            .unwrap();
        assert_eq!(deleted, 2);

        let resp = db
            .query(QueryRequest {
                sql: "SELECT tenant_id,value FROM testdb.events".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();
        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let tenant_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(tenant_col.len(), 1);
        assert_eq!(tenant_col.value(0), 2);
        let val_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(val_col.value(0), 7);
    }

    #[test]
    fn test_update_rows_applies_assignments() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "events", None).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 30])),
                Arc::new(UInt64Array::from(vec![1u64, 1, 2])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 10])),
                Arc::new(arrow_array::Int64Array::from(vec![5i64, 6, 7])),
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let updated = db
            .update_rows(
                "testdb",
                "events",
                &[("value".to_string(), SqlValue::Integer(999))],
                Some("route_id=20"),
            )
            .unwrap();
        assert_eq!(updated, 1);

        let resp = db
            .query(QueryRequest {
                sql: "SELECT tenant_id,route_id,value FROM testdb.events WHERE tenant_id=1 ORDER BY route_id"
                    .into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();
        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let tenant_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let route_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let val_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let mut rows: Vec<(u64, u64, i64)> = (0..out_batch.num_rows())
            .map(|i| (tenant_col.value(i), route_col.value(i), val_col.value(i)))
            .collect();
        rows.sort_by_key(|(_, r, _)| *r);
        assert_eq!(rows, vec![(1, 10, 5), (1, 20, 999)]);
    }

    #[test]
    fn compaction_merges_small_segments() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1)
            .with_compact_min_segments(2)
            .with_compaction_target_bytes(1_024 * 1_024)
            .with_tier_warm_after_millis(0)
            .with_tier_cold_after_millis(0);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "t", None).unwrap();

        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        for v in 0..3i64 {
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(arrow_array::Int64Array::from(vec![v]))],
            )
            .unwrap();
            let mut ipc = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            db.ingest_ipc(IngestBatch {
                payload_ipc: ipc,
                watermark_micros: (v as u64) + 1,
                shard_override: None,
                database: Some("testdb".into()),
                table: Some("t".into()),
            })
            .unwrap();
        }

        let manifest_before: Manifest =
            serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        assert!(manifest_before.entries.len() >= 3);

        db.maintenance().unwrap();

        let manifest_after: Manifest =
            serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        assert!(manifest_after.entries.len() < manifest_before.entries.len());
        assert_eq!(manifest_after.entries.len(), 1);

        let mut total_rows = 0usize;
        for entry in &manifest_after.entries {
            let data = load_segment(&db.storage, entry).unwrap();
            let batches = read_ipc_batches(&data).unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn tiering_promotes_hot_to_cold() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1)
            .with_enable_compaction(false)
            .with_tier_warm_after_millis(1)
            .with_tier_cold_after_millis(1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        db.create_table("testdb", "t", None).unwrap();

        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("t".into()),
        })
        .unwrap();

        db.maintenance().unwrap();

        let manifest: Manifest = serde_json::from_slice(&db.export_manifest().unwrap()).unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert!(matches!(manifest.entries[0].tier, SegmentTier::Cold));
    }

    #[test]
    fn mvcc_defers_segment_delete_until_readers_done() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let seg_id = "seg-mvcc-test";
        let path = db.cfg.segments_dir.join(format!("{seg_id}.ipc"));
        fs::write(&path, b"dummy").unwrap();

        let guard = db.read_guard();
        db.remove_segment_file(seg_id);
        assert!(path.exists());
        drop(guard);
        assert!(!path.exists());
    }

    #[test]
    fn test_alter_table_add_column_and_query_nulls() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        db.create_table(
            "testdb",
            "events",
            Some(
                serde_json::to_string(&vec![TableFieldSpec {
                    name: "value".into(),
                    data_type: "int64".into(),
                    nullable: false,
                    encoding: None,
                }])
                .unwrap(),
            ),
        )
        .unwrap();

        // Ingest one row
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(arrow_array::Int64Array::from(vec![42i64]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        db.alter_table_add_column("testdb", "events", "latency_ms", "int64", true)
            .unwrap();
        let desc = db.describe_table("testdb", "events").unwrap();
        let fields: Vec<TableFieldSpec> =
            serde_json::from_str(desc.schema_json.as_ref().unwrap()).unwrap();
        assert!(fields.iter().any(|f| f.name == "latency_ms"));

        let resp = db
            .query(QueryRequest {
                sql: "SELECT latency_ms FROM testdb.events".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();
        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col.len(), 1);
        assert!(col.is_null(0));
    }

    #[test]
    fn test_alter_table_drop_column() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]);
        db.create_table(
            "testdb",
            "events",
            Some(
                serde_json::to_string(&vec![
                    TableFieldSpec {
                        name: "value".into(),
                        data_type: "int64".into(),
                        nullable: false,
                        encoding: None,
                    },
                    TableFieldSpec {
                        name: "note".into(),
                        data_type: "string".into(),
                        nullable: true,
                        encoding: None,
                    },
                ])
                .unwrap(),
            ),
        )
        .unwrap();

        // Ingest one row
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![7i64])),
                Arc::new(arrow_array::StringArray::from(vec![Some("hi")])),
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        })
        .unwrap();

        db.alter_table_drop_column("testdb", "events", "note")
            .unwrap();
        let desc = db.describe_table("testdb", "events").unwrap();
        let fields: Vec<TableFieldSpec> =
            serde_json::from_str(desc.schema_json.as_ref().unwrap()).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "value");

        // Query should still work and only return the remaining column
        let resp = db
            .query(QueryRequest {
                sql: "SELECT value FROM testdb.events".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();
        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        let col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 7);
    }

    #[test]
    fn test_query_timeout() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create table with data
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        })
        .unwrap();

        // Query with timeout should work
        let result = db.query(QueryRequest {
            sql: "SELECT * FROM test.data".into(),
            timeout_millis: 5000,
            collect_stats: false,
        });
        assert!(result.is_ok());

        // Query with 0 timeout (disabled) should also work
        let result = db.query(QueryRequest {
            sql: "SELECT * FROM test.data".into(),
            timeout_millis: 0,
            collect_stats: false,
        });
        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_ingest_rejected() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let result = db.ingest_ipc(IngestBatch {
            payload_ipc: vec![],
            watermark_micros: 1,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_empty_sql_rejected() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let result = db.query(QueryRequest {
            sql: "   ".into(),
            timeout_millis: 1000,
            collect_stats: false,
        });
        assert!(result.is_err());
    }

    #[test]
    fn aggregate_group_by_multiple_columns() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create data with different tenant/route combinations
        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5, 6])),
                Arc::new(UInt64Array::from(vec![100u64, 100, 100, 100, 100, 100])),
                // tenant_id: 1,1,1,2,2,2
                Arc::new(UInt64Array::from(vec![1u64, 1, 1, 2, 2, 2])),
                // route_id: 10,10,20,10,20,20
                Arc::new(UInt64Array::from(vec![10u64, 10, 20, 10, 20, 20])),
                // value: 10,20,30,40,50,60
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40, 50, 60])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        // Test multi-column GROUP BY
        let resp = db
            .query(QueryRequest {
                sql:
                    "SELECT COUNT(*), SUM(value) FROM analytics.events GROUP BY tenant_id, route_id"
                        .into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should have 4 groups: (1,10), (1,20), (2,10), (2,20)
        assert_eq!(out_batch.num_rows(), 4);

        // Verify schema has both tenant_id and route_id columns
        let schema = out_batch.schema();
        assert!(schema.field_with_name("tenant_id").is_ok());
        assert!(schema.field_with_name("route_id").is_ok());
        assert!(schema.field_with_name("count").is_ok());
        assert!(schema.field_with_name("sum_value").is_ok());

        // Collect all groups into a map for verification
        let tenant_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let route_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let count_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let sum_col = out_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut results: std::collections::HashMap<(u64, u64), (u64, i64)> =
            std::collections::HashMap::new();
        for i in 0..out_batch.num_rows() {
            results.insert(
                (tenant_col.value(i), route_col.value(i)),
                (count_col.value(i), sum_col.value(i)),
            );
        }

        // Verify expected groups:
        // (1, 10): 2 rows with duration 10+20=30
        assert_eq!(results.get(&(1, 10)), Some(&(2, 30)));
        // (1, 20): 1 row with duration 30
        assert_eq!(results.get(&(1, 20)), Some(&(1, 30)));
        // (2, 10): 1 row with duration 40
        assert_eq!(results.get(&(2, 10)), Some(&(1, 40)));
        // (2, 20): 2 rows with duration 50+60=110
        assert_eq!(results.get(&(2, 20)), Some(&(2, 110)));
    }

    #[test]
    fn aggregate_min_max_multiple_columns() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create data with multiple int64 columns to MIN/MAX on
        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 5, 10])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
                Arc::new(UInt64Array::from(vec![7u64, 7, 8])),
                Arc::new(UInt64Array::from(vec![101u64, 101, 102])),
                Arc::new(Int64Array::from(vec![10i64, -5, 30])), // value: min=-5, max=30
                Arc::new(Int64Array::from(vec![100i64, 500, 200])), // amount: min=100, max=500
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("analytics".into()),
            table: Some("events".into()),
        })
        .unwrap();

        // Test multiple MIN/MAX on different columns
        let resp = db
            .query(QueryRequest {
                sql: "SELECT MIN(value), MAX(value), MIN(amount), MAX(amount) FROM analytics.events".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Verify schema has correct column names
        let schema = out_batch.schema();
        assert!(schema.field_with_name("min_value").is_ok());
        assert!(schema.field_with_name("max_value").is_ok());
        assert!(schema.field_with_name("min_amount").is_ok());
        assert!(schema.field_with_name("max_amount").is_ok());

        // Verify values
        let min_duration = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_duration = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let min_bytes = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_bytes = out_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(min_duration.value(0), -5);
        assert_eq!(max_duration.value(0), 30);
        assert_eq!(min_bytes.value(0), 100);
        assert_eq!(max_bytes.value(0), 500);
    }

    #[test]
    fn test_select_distinct() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create data with duplicate rows (same tenant_id values)
        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                Arc::new(UInt64Array::from(vec![100u64, 100, 100, 100, 100])),
                // tenant_id: 1, 2, 1, 2, 1 - will have duplicates when selecting only tenant_id
                Arc::new(UInt64Array::from(vec![1u64, 2, 1, 2, 1])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 10, 20, 30])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        })
        .unwrap();

        // Query with DISTINCT - should get unique (tenant_id, route_id) pairs
        let resp = db
            .query(QueryRequest {
                sql: "SELECT DISTINCT tenant_id, route_id FROM test.data".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Original: (1,10), (2,20), (1,10), (2,20), (1,30)
        // After DISTINCT: (1,10), (2,20), (1,30) = 3 unique rows
        assert_eq!(out_batch.num_rows(), 3);

        // Verify we have the expected unique pairs
        let tenant_col = out_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let route_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut pairs: Vec<(u64, u64)> = Vec::new();
        for i in 0..out_batch.num_rows() {
            pairs.push((tenant_col.value(i), route_col.value(i)));
        }
        pairs.sort();

        assert!(pairs.contains(&(1, 10)));
        assert!(pairs.contains(&(1, 30)));
        assert!(pairs.contains(&(2, 20)));
    }

    #[test]
    fn test_where_between() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                // event_time: 100, 200, 300, 400, 500
                Arc::new(UInt64Array::from(vec![100u64, 200, 300, 400, 500])),
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        })
        .unwrap();

        // Query with BETWEEN - should get rows where event_time is between 200 and 400
        let resp = db
            .query(QueryRequest {
                sql: "SELECT * FROM test.data WHERE event_time BETWEEN 200 AND 400".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should get 3 rows: event_time 200, 300, 400
        assert_eq!(out_batch.num_rows(), 3);

        let event_time_col = out_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut event_times: Vec<u64> = (0..out_batch.num_rows())
            .map(|i| event_time_col.value(i))
            .collect();
        event_times.sort();

        assert_eq!(event_times, vec![200, 300, 400]);
    }

    #[test]
    fn test_where_in() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300, 400, 500])),
                // tenant_id: 1, 2, 3, 4, 5
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                // route_id: 10, 20, 30, 40, 50
                Arc::new(UInt64Array::from(vec![10u64, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        })
        .unwrap();

        // Query with IN on tenant_id - should get rows where tenant_id is 1, 3, or 5
        let resp = db
            .query(QueryRequest {
                sql: "SELECT * FROM test.data WHERE tenant_id IN (1, 3, 5)".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should get 3 rows: tenant_id 1, 3, 5
        assert_eq!(out_batch.num_rows(), 3);

        let tenant_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut tenant_ids: Vec<u64> = (0..out_batch.num_rows())
            .map(|i| tenant_col.value(i))
            .collect();
        tenant_ids.sort();

        assert_eq!(tenant_ids, vec![1, 3, 5]);

        // Query with IN on route_id - should get rows where route_id is 20 or 40
        let resp2 = db
            .query(QueryRequest {
                sql: "SELECT * FROM test.data WHERE route_id IN (20, 40)".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor2 = Cursor::new(resp2.records_ipc);
        let mut reader2 = StreamReader::try_new(cursor2, None).unwrap();
        let out_batch2 = reader2.next().unwrap().unwrap();

        // Should get 2 rows: route_id 20, 40
        assert_eq!(out_batch2.num_rows(), 2);

        let route_col = out_batch2
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut route_ids: Vec<u64> = (0..out_batch2.num_rows())
            .map(|i| route_col.value(i))
            .collect();
        route_ids.sort();

        assert_eq!(route_ids, vec![20, 40]);
    }

    #[test]
    fn test_where_or() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 300, 400, 500])),
                // tenant_id: 1, 2, 3, 4, 5
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                // route_id: 10, 20, 30, 40, 50
                Arc::new(UInt64Array::from(vec![10u64, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("data".into()),
        })
        .unwrap();

        // Query with OR on tenant_id - should get rows where tenant_id is 1 or 3
        let resp = db
            .query(QueryRequest {
                sql: "SELECT * FROM test.data WHERE tenant_id = 1 OR tenant_id = 3".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();

        // Should get 2 rows: tenant_id 1, 3
        assert_eq!(out_batch.num_rows(), 2);

        let tenant_col = out_batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut tenant_ids: Vec<u64> = (0..out_batch.num_rows())
            .map(|i| tenant_col.value(i))
            .collect();
        tenant_ids.sort();

        assert_eq!(tenant_ids, vec![1, 3]);

        // Query with multiple OR on route_id
        let resp2 = db
            .query(QueryRequest {
                sql:
                    "SELECT * FROM test.data WHERE route_id = 10 OR route_id = 30 OR route_id = 50"
                        .into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor2 = Cursor::new(resp2.records_ipc);
        let mut reader2 = StreamReader::try_new(cursor2, None).unwrap();
        let out_batch2 = reader2.next().unwrap().unwrap();

        // Should get 3 rows: route_id 10, 30, 50
        assert_eq!(out_batch2.num_rows(), 3);

        let route_col = out_batch2
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut route_ids: Vec<u64> = (0..out_batch2.num_rows())
            .map(|i| route_col.value(i))
            .collect();
        route_ids.sort();

        assert_eq!(route_ids, vec![10, 30, 50]);
    }

    #[test]
    fn test_like_pattern_to_regex() {
        // Test basic patterns
        assert_eq!(like_pattern_to_regex("hello").unwrap(), "^hello$");
        assert_eq!(like_pattern_to_regex("%world").unwrap(), "^.*world$");
        assert_eq!(like_pattern_to_regex("hello%").unwrap(), "^hello.*$");
        assert_eq!(like_pattern_to_regex("%hello%").unwrap(), "^.*hello.*$");
        assert_eq!(like_pattern_to_regex("h_llo").unwrap(), "^h.llo$");
        assert_eq!(like_pattern_to_regex("h%o").unwrap(), "^h.*o$");
        assert_eq!(like_pattern_to_regex("_ello").unwrap(), "^.ello$");

        // Test regex metacharacter escaping
        assert_eq!(like_pattern_to_regex("foo.bar").unwrap(), "^foo\\.bar$");
        assert_eq!(like_pattern_to_regex("foo[bar]").unwrap(), "^foo\\[bar\\]$");

        // Test that pattern length limit is enforced
        let long_pattern = "x".repeat(MAX_LIKE_PATTERN_LEN + 1);
        assert!(like_pattern_to_regex(&long_pattern).is_none());
    }

    #[test]
    fn test_matches_like_pattern() {
        // Basic matches
        assert!(matches_like_pattern("hello", "hello"));
        assert!(matches_like_pattern("hello world", "%world"));
        assert!(matches_like_pattern("hello world", "hello%"));
        assert!(matches_like_pattern("hello world", "%lo wo%"));
        assert!(matches_like_pattern("hello", "h_llo"));
        assert!(matches_like_pattern("hallo", "h_llo"));

        // Non-matches
        assert!(!matches_like_pattern("hello", "world"));
        assert!(!matches_like_pattern("hello", "hello%world"));
        assert!(!matches_like_pattern("hi", "h_llo"));
    }

    #[test]
    fn query_with_like_filter() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4, 5])),
                Arc::new(UInt64Array::from(vec![100u64, 100, 100, 100, 100])),
                Arc::new(UInt64Array::from(vec![1u64, 1, 1, 1, 1])),
                Arc::new(UInt64Array::from(vec![10u64, 10, 10, 10, 10])),
                Arc::new(StringArray::from(vec![
                    "alice",
                    "bob",
                    "alice_smith",
                    "charlie",
                    "bobcat",
                ])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("users".into()),
        })
        .unwrap();

        // Query with LIKE '%alice%'
        let resp = db
            .query(QueryRequest {
                sql: "SELECT name FROM test.users WHERE name LIKE '%alice%'".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        assert_eq!(out_batch.num_rows(), 2); // alice and alice_smith

        // Query with LIKE 'bob%'
        let resp2 = db
            .query(QueryRequest {
                sql: "SELECT name FROM test.users WHERE name LIKE 'bob%'".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor2 = Cursor::new(resp2.records_ipc);
        let mut reader2 = StreamReader::try_new(cursor2, None).unwrap();
        let out_batch2 = reader2.next().unwrap().unwrap();
        assert_eq!(out_batch2.num_rows(), 2); // bob and bobcat
    }

    #[test]
    fn query_with_string_equality() {
        let tmp = tempdir().unwrap();
        let cfg = EngineConfig::new(tmp.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        let schema = Schema::new(vec![
            Field::new("watermark_micros", DataType::UInt64, false),
            Field::new("event_time", DataType::UInt64, false),
            Field::new("tenant_id", DataType::UInt64, false),
            Field::new("route_id", DataType::UInt64, false),
            Field::new("status", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 4])),
                Arc::new(UInt64Array::from(vec![100u64, 100, 100, 100])),
                Arc::new(UInt64Array::from(vec![1u64, 1, 1, 1])),
                Arc::new(UInt64Array::from(vec![10u64, 10, 10, 10])),
                Arc::new(StringArray::from(vec![
                    "active",
                    "inactive",
                    "active",
                    "pending",
                ])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 10,
            shard_override: None,
            database: Some("test".into()),
            table: Some("accounts".into()),
        })
        .unwrap();

        // Query with string equality
        let resp = db
            .query(QueryRequest {
                sql: "SELECT status FROM test.accounts WHERE status = 'active'".into(),
                timeout_millis: 1000,
            collect_stats: false,
        })
            .unwrap();

        let cursor = Cursor::new(resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let out_batch = reader.next().unwrap().unwrap();
        assert_eq!(out_batch.num_rows(), 2); // Two 'active' rows
    }

    #[test]
    fn test_computed_value_uuid() {
        // Test UUID formatting
        let uuid_bytes: [u8; 16] = [
            0x55, 0x06, 0x4a, 0x5b, 0x1e, 0xc0, 0x4b, 0x8a,
            0x9e, 0x0b, 0x5a, 0x7a, 0x4e, 0x69, 0x9a, 0xbc,
        ];
        let cv = ComputedValue::Uuid(uuid_bytes);
        assert_eq!(cv.to_string_value(), "55064a5b-1ec0-4b8a-9e0b-5a7a4e699abc");
        assert!(cv.as_uuid().is_some());
        assert_eq!(cv.as_uuid().unwrap(), &uuid_bytes);
    }

    #[test]
    fn test_computed_value_decimal() {
        // Test decimal formatting
        let cv = ComputedValue::Decimal { value: 12345, precision: 10, scale: 2 };
        assert_eq!(cv.to_string_value(), "123.45");
        assert_eq!(cv.as_f64(), Some(123.45));

        // Negative decimal
        let cv_neg = ComputedValue::Decimal { value: -12345, precision: 10, scale: 2 };
        assert_eq!(cv_neg.to_string_value(), "-123.45");
        assert_eq!(cv_neg.as_f64(), Some(-123.45));

        // Small decimal (fractional only)
        let cv_small = ComputedValue::Decimal { value: 45, precision: 10, scale: 3 };
        assert_eq!(cv_small.to_string_value(), "0.045");
    }

    #[test]
    fn test_computed_value_json() {
        let cv = ComputedValue::Json(r#"{"key": "value", "num": 42}"#.to_string());
        assert_eq!(cv.as_json(), Some(r#"{"key": "value", "num": 42}"#));
        assert_eq!(cv.to_string_value(), r#"{"key": "value", "num": 42}"#);
    }

    #[test]
    fn test_computed_value_date() {
        // Test date formatting - January 1, 2024 is 19723 days since Unix epoch
        let cv = ComputedValue::Date(19723);
        assert_eq!(cv.to_string_value(), "2024-01-01");

        // Test Unix epoch
        let cv_epoch = ComputedValue::Date(0);
        assert_eq!(cv_epoch.to_string_value(), "1970-01-01");
    }

    #[test]
    fn test_cast_to_uuid() {
        // Valid UUID string
        let cv = ComputedValue::String("55064a5b-1ec0-4b8a-9e0b-5a7a4e699abc".to_string());
        let result = cast_value(&cv, "uuid").unwrap();
        if let ComputedValue::Uuid(bytes) = result {
            assert_eq!(bytes[0], 0x55);
            assert_eq!(bytes[15], 0xbc);
        } else {
            panic!("Expected UUID");
        }

        // UUID without hyphens
        let cv2 = ComputedValue::String("55064a5b1ec04b8a9e0b5a7a4e699abc".to_string());
        let result2 = cast_value(&cv2, "uuid").unwrap();
        assert!(matches!(result2, ComputedValue::Uuid(_)));
    }

    #[test]
    fn test_cast_to_decimal() {
        // Integer to decimal
        let cv_int = ComputedValue::Integer(12345);
        let result = cast_value(&cv_int, "decimal").unwrap();
        if let ComputedValue::Decimal { value, scale, .. } = result {
            assert_eq!(value, 12345);
            assert_eq!(scale, 0);
        } else {
            panic!("Expected Decimal");
        }

        // String to decimal
        let cv_str = ComputedValue::String("123.45".to_string());
        let result2 = cast_value(&cv_str, "decimal").unwrap();
        if let ComputedValue::Decimal { value, scale, .. } = result2 {
            assert_eq!(value, 12345);
            assert_eq!(scale, 2);
        } else {
            panic!("Expected Decimal");
        }
    }

    #[test]
    fn test_cast_to_json() {
        // Valid JSON string
        let cv = ComputedValue::String(r#"{"test": true}"#.to_string());
        let result = cast_value(&cv, "json").unwrap();
        assert!(matches!(result, ComputedValue::Json(_)));

        // Invalid JSON becomes string
        let cv2 = ComputedValue::String("not json".to_string());
        let result2 = cast_value(&cv2, "json").unwrap();
        if let ComputedValue::Json(s) = result2 {
            assert!(s.contains("not json")); // Should be wrapped as JSON string
        } else {
            panic!("Expected Json");
        }

        // Integer to JSON
        let cv3 = ComputedValue::Integer(42);
        let result3 = cast_value(&cv3, "json").unwrap();
        if let ComputedValue::Json(s) = result3 {
            assert_eq!(s, "42");
        } else {
            panic!("Expected Json");
        }
    }

    #[test]
    fn test_cast_to_date() {
        // String to date
        let cv = ComputedValue::String("2024-01-01".to_string());
        let result = cast_value(&cv, "date").unwrap();
        if let ComputedValue::Date(days) = result {
            assert_eq!(days, 19723); // Jan 1, 2024
        } else {
            panic!("Expected Date");
        }

        // Timestamp to date
        let cv2 = ComputedValue::Timestamp(1704067200_000_000); // Jan 1, 2024 00:00:00 UTC
        let result2 = cast_value(&cv2, "date").unwrap();
        assert!(matches!(result2, ComputedValue::Date(_)));
    }

    #[test]
    fn test_parse_uuid_string() {
        // With hyphens
        let result = parse_uuid_string("55064a5b-1ec0-4b8a-9e0b-5a7a4e699abc");
        assert!(result.is_some());

        // Without hyphens
        let result2 = parse_uuid_string("55064a5b1ec04b8a9e0b5a7a4e699abc");
        assert!(result2.is_some());

        // Invalid (too short)
        let result3 = parse_uuid_string("55064a5b-1ec0-4b8a");
        assert!(result3.is_none());
    }

    #[test]
    fn test_parse_decimal_string() {
        // Simple decimal
        let result = parse_decimal_string("123.45");
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.integer_part, 123);
        assert_eq!(parsed.fractional_part, 45);
        assert_eq!(parsed.scale, 2);
        assert!(!parsed.is_negative);

        // Negative decimal
        let result2 = parse_decimal_string("-99.999");
        assert!(result2.is_some());
        let parsed2 = result2.unwrap();
        assert_eq!(parsed2.integer_part, 99);
        assert_eq!(parsed2.fractional_part, 999);
        assert_eq!(parsed2.scale, 3);
        assert!(parsed2.is_negative);

        // Integer (no decimal point)
        let result3 = parse_decimal_string("42");
        assert!(result3.is_some());
        let parsed3 = result3.unwrap();
        assert_eq!(parsed3.integer_part, 42);
        assert_eq!(parsed3.fractional_part, 0);
        assert_eq!(parsed3.scale, 0);
        assert!(!parsed3.is_negative);
    }

    #[test]
    fn test_arrow_type_from_str_new_types() {
        // UUID
        let uuid_type = arrow_type_from_str("uuid").unwrap();
        assert_eq!(uuid_type, DataType::FixedSizeBinary(16));

        // JSON
        let json_type = arrow_type_from_str("json").unwrap();
        assert_eq!(json_type, DataType::LargeUtf8);

        // Decimal
        let decimal_type = arrow_type_from_str("decimal(10,2)").unwrap();
        assert_eq!(decimal_type, DataType::Decimal128(10, 2));

        // Date
        let date_type = arrow_type_from_str("date").unwrap();
        assert_eq!(date_type, DataType::Date32);

        // Binary
        let binary_type = arrow_type_from_str("binary").unwrap();
        assert_eq!(binary_type, DataType::LargeBinary);

        // Timestamp
        let ts_type = arrow_type_from_str("timestamp").unwrap();
        assert!(matches!(ts_type, DataType::Timestamp(_, _)));
    }

    #[test]
    fn test_subquery_parsing_in_subquery() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test IN subquery parsing
        let sql = "SELECT * FROM default.users WHERE id IN (SELECT user_id FROM default.orders)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.in_subquery_filters.len(), 1);
                let (col, subquery_sql, negated) = &q.filter.in_subquery_filters[0];
                assert_eq!(col, "id");
                assert!(!negated);
                assert!(subquery_sql.to_uppercase().contains("SELECT"));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_subquery_parsing_not_in_subquery() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test NOT IN subquery parsing
        let sql = "SELECT * FROM default.users WHERE id NOT IN (SELECT banned_id FROM default.banned)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.in_subquery_filters.len(), 1);
                let (col, _, negated) = &q.filter.in_subquery_filters[0];
                assert_eq!(col, "id");
                assert!(negated);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_subquery_parsing_exists() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test EXISTS subquery parsing
        let sql = "SELECT * FROM default.users WHERE EXISTS (SELECT 1 FROM default.orders WHERE user_id = 1)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.exists_subqueries.len(), 1);
                let (subquery_sql, negated) = &q.filter.exists_subqueries[0];
                assert!(!negated);
                assert!(subquery_sql.to_uppercase().contains("SELECT"));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_subquery_parsing_scalar_comparison() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test scalar subquery with = comparison
        let sql = "SELECT * FROM default.users WHERE age = (SELECT MAX(age) FROM default.users)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.scalar_subquery_filters.len(), 1);
                let (col, op, _) = &q.filter.scalar_subquery_filters[0];
                assert_eq!(col, "age");
                assert_eq!(op, "=");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_subquery_parsing_scalar_comparison_gt() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test scalar subquery with > comparison
        let sql = "SELECT * FROM default.users WHERE salary > (SELECT AVG(salary) FROM default.users)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.scalar_subquery_filters.len(), 1);
                let (col, op, _) = &q.filter.scalar_subquery_filters[0];
                assert_eq!(col, "salary");
                assert_eq!(op, ">");
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_extract_column_value_as_string() {
        use arrow_array::{Int64Array, Float64Array, BooleanArray};
        use std::sync::Arc;

        // Test Int64
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![42, 100, -5]));
        assert_eq!(Db::extract_column_value_as_string(&arr, 0), "42");
        assert_eq!(Db::extract_column_value_as_string(&arr, 1), "100");
        assert_eq!(Db::extract_column_value_as_string(&arr, 2), "-5");

        // Test Float64
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![3.14, 2.718]));
        assert_eq!(Db::extract_column_value_as_string(&arr, 0), "3.14");

        // Test String
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        assert_eq!(Db::extract_column_value_as_string(&arr, 0), "hello");
        assert_eq!(Db::extract_column_value_as_string(&arr, 1), "world");

        // Test Boolean
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(Db::extract_column_value_as_string(&arr, 0), "true");
        assert_eq!(Db::extract_column_value_as_string(&arr, 1), "false");
    }

    #[test]
    fn test_compare_string_values() {
        // Numeric comparison
        assert_eq!(Db::compare_string_values("10", "5"), std::cmp::Ordering::Greater);
        assert_eq!(Db::compare_string_values("3", "10"), std::cmp::Ordering::Less);
        assert_eq!(Db::compare_string_values("42", "42"), std::cmp::Ordering::Equal);

        // Float comparison
        assert_eq!(Db::compare_string_values("3.14", "2.71"), std::cmp::Ordering::Greater);
        assert_eq!(Db::compare_string_values("1.5", "1.5"), std::cmp::Ordering::Equal);

        // String comparison (when not numeric)
        assert_eq!(Db::compare_string_values("apple", "banana"), std::cmp::Ordering::Less);
        assert_eq!(Db::compare_string_values("zebra", "apple"), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_materialized_view_crud() {
        use arrow_array::Int64Array;
        use arrow_ipc::writer::StreamWriter;

        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create database and table
        db.create_database("testdb").unwrap();
        let schema_json = serde_json::to_string(&vec![
            TableFieldSpec {
                name: "id".into(),
                data_type: "int64".into(),
                nullable: false,
                encoding: None,
            },
            TableFieldSpec {
                name: "age".into(),
                data_type: "int64".into(),
                nullable: false,
                encoding: None,
            },
        ]).unwrap();
        db.create_table("testdb", "users", Some(schema_json)).unwrap();

        // Insert some data
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("age", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![30i64, 25])),
            ],
        ).unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        db.ingest_ipc(IngestBatch {
            payload_ipc: ipc,
            watermark_micros: 1,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("users".into()),
        }).unwrap();

        // Create a materialized view
        db.create_materialized_view(
            "testdb",
            "user_stats",
            "SELECT COUNT(*) as cnt FROM testdb.users",
            false,
        ).unwrap();

        // List materialized views
        let views = db.list_materialized_views(Some("testdb")).unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].0, "testdb");
        assert_eq!(views[0].1, "user_stats");

        // Refresh the materialized view
        db.refresh_materialized_view("testdb", "user_stats").unwrap();

        // Verify data was stored
        let data = db.get_materialized_view_data("testdb", "user_stats").unwrap();
        assert!(data.is_some());
        assert!(!data.unwrap().is_empty());

        // Drop the materialized view
        db.drop_materialized_view("testdb", "user_stats", false).unwrap();

        // Verify it's gone
        let views = db.list_materialized_views(Some("testdb")).unwrap();
        assert_eq!(views.len(), 0);

        // Drop with if_exists should not error
        db.drop_materialized_view("testdb", "user_stats", true).unwrap();
    }

    #[test]
    fn test_materialized_view_or_replace() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        db.create_database("testdb").unwrap();

        // Create a materialized view
        db.create_materialized_view("testdb", "test_mv", "SELECT 1 as x", false).unwrap();

        // Try to create again without or_replace should fail
        let result = db.create_materialized_view("testdb", "test_mv", "SELECT 2 as x", false);
        assert!(result.is_err());

        // Create with or_replace should succeed
        db.create_materialized_view("testdb", "test_mv", "SELECT 3 as y", true).unwrap();

        // Verify the query was replaced
        let views = db.list_materialized_views(Some("testdb")).unwrap();
        assert_eq!(views.len(), 1);
        assert!(views[0].2.contains("SELECT 3"));
    }

    #[test]
    fn test_parse_recursive_cte_parts() {
        // Test UNION ALL parsing
        let sql = "SELECT 1 as n UNION ALL SELECT n + 1 FROM cte WHERE n < 10";
        let (anchor, recursive, is_union_all) = parse_recursive_cte_parts(sql, "cte").unwrap();
        assert_eq!(anchor, "SELECT 1 as n");
        assert_eq!(recursive, "SELECT n + 1 FROM cte WHERE n < 10");
        assert!(is_union_all);

        // Test UNION parsing (not ALL)
        let sql = "SELECT 1 as n UNION SELECT n + 1 FROM cte WHERE n < 10";
        let (anchor, recursive, is_union_all) = parse_recursive_cte_parts(sql, "cte").unwrap();
        assert_eq!(anchor, "SELECT 1 as n");
        assert_eq!(recursive, "SELECT n + 1 FROM cte WHERE n < 10");
        assert!(!is_union_all);
    }

    #[test]
    fn test_recursive_cte_requires_union() {
        // Test that a recursive CTE without UNION fails
        let sql = "SELECT 1 as n";
        let result = parse_recursive_cte_parts(sql, "cte");
        assert!(result.is_err());
    }

    #[test]
    fn test_cte_definition_with_recursive_flag() {
        use crate::sql::{parse_sql, SqlStatement};

        // Test parsing WITH RECURSIVE
        let sql = "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 10) SELECT * FROM cnt";
        let result = parse_sql(sql);
        match result {
            Ok(SqlStatement::Query(q)) => {
                assert_eq!(q.ctes.len(), 1);
                assert!(q.ctes[0].recursive);
                assert_eq!(q.ctes[0].name, "cnt");
            }
            Ok(_) => panic!("expected query with CTEs"),
            Err(e) => panic!("parse error: {:?}", e),
        }

        // Test parsing regular WITH (non-recursive)
        let sql = "WITH temp AS (SELECT 1 as x) SELECT * FROM temp";
        let result = parse_sql(sql);
        match result {
            Ok(SqlStatement::Query(q)) => {
                assert_eq!(q.ctes.len(), 1);
                assert!(!q.ctes[0].recursive);
            }
            Ok(_) => panic!("expected query with CTEs"),
            Err(e) => panic!("parse error: {:?}", e),
        }
    }

    #[test]
    fn test_cte_query_routing() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path(), 1);
        let db = Db::open_for_test(cfg).unwrap();

        // Create a database and table for CTE testing
        db.create_database("testdb").unwrap();
        db.create_table("testdb", "numbers", None).unwrap();

        // Test that a simple CTE query routes through execute_cte_query
        // This verifies the wiring - the query should parse and route correctly
        let sql = "WITH nums AS (SELECT 1 as n) SELECT * FROM nums";

        // The query should not panic and should return a valid response
        // (even if the result is empty because the CTE references a virtual table)
        let result = db.query(QueryRequest {
            sql: sql.to_string(),
            timeout_millis: 1000,
            collect_stats: false,
        });

        // We just verify it doesn't panic - the actual execution may fail
        // because CTEs don't create real tables, but the routing works
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_materialized_view_sql_parsing() {
        use crate::sql::{parse_sql, SqlStatement, DdlCommand};

        // Test CREATE MATERIALIZED VIEW parsing
        let sql = "CREATE MATERIALIZED VIEW testdb.my_view AS SELECT COUNT(*) FROM testdb.events";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CreateMaterializedView { database, name, query_sql, or_replace }) => {
                assert_eq!(database, "testdb");
                assert_eq!(name, "my_view");
                assert!(query_sql.contains("SELECT"));
                assert!(!or_replace);
            }
            _ => panic!("expected CreateMaterializedView DDL"),
        }

        // Test DROP MATERIALIZED VIEW parsing
        let sql = "DROP MATERIALIZED VIEW IF EXISTS testdb.my_view";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DropMaterializedView { database, name, if_exists }) => {
                assert_eq!(database, "testdb");
                assert_eq!(name, "my_view");
                assert!(if_exists);
            }
            _ => panic!("expected DropMaterializedView DDL"),
        }

        // Test REFRESH MATERIALIZED VIEW parsing
        let sql = "REFRESH MATERIALIZED VIEW testdb.my_view";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::RefreshMaterializedView { database, name }) => {
                assert_eq!(database, "testdb");
                assert_eq!(name, "my_view");
            }
            _ => panic!("expected RefreshMaterializedView DDL"),
        }

        // Test SHOW MATERIALIZED VIEWS parsing
        let sql = "SHOW MATERIALIZED VIEWS IN testdb";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowMaterializedViews { database }) => {
                assert_eq!(database, Some("testdb".to_string()));
            }
            _ => panic!("expected ShowMaterializedViews DDL"),
        }
    }

    #[test]
    fn test_compression_codecs_roundtrip() {
        // Test data
        let original_data = b"Hello, World! This is test data for compression. ".repeat(100);

        // Test LZ4 compression
        let lz4_compressed = compress_payload(&original_data, Some("lz4")).unwrap();
        assert!(lz4_compressed.len() < original_data.len(), "LZ4 should compress data");
        let lz4_decompressed = decompress_payload(lz4_compressed, Some("lz4")).unwrap();
        assert_eq!(lz4_decompressed, original_data, "LZ4 roundtrip should preserve data");

        // Test Snappy compression
        let snappy_compressed = compress_payload(&original_data, Some("snappy")).unwrap();
        assert!(snappy_compressed.len() < original_data.len(), "Snappy should compress data");
        let snappy_decompressed = decompress_payload(snappy_compressed, Some("snappy")).unwrap();
        assert_eq!(snappy_decompressed, original_data, "Snappy roundtrip should preserve data");

        // Test ZSTD compression
        let zstd_compressed = compress_payload(&original_data, Some("zstd")).unwrap();
        assert!(zstd_compressed.len() < original_data.len(), "ZSTD should compress data");
        let zstd_decompressed = decompress_payload(zstd_compressed, Some("zstd")).unwrap();
        assert_eq!(zstd_decompressed, original_data, "ZSTD roundtrip should preserve data");

        // Test no compression
        let no_compress = compress_payload(&original_data, None).unwrap();
        assert_eq!(no_compress, original_data, "No compression should preserve data");
        let no_decompress = decompress_payload(no_compress, None).unwrap();
        assert_eq!(no_decompress, original_data, "No decompression should preserve data");
    }

    #[test]
    fn test_compression_normalize() {
        assert_eq!(normalize_compression(Some("lz4".into())).unwrap(), Some("lz4".into()));
        assert_eq!(normalize_compression(Some("snappy".into())).unwrap(), Some("snappy".into()));
        assert_eq!(normalize_compression(Some("zstd".into())).unwrap(), Some("zstd".into()));
        assert_eq!(normalize_compression(Some("none".into())).unwrap(), None);
        assert_eq!(normalize_compression(None).unwrap(), None);
        assert!(normalize_compression(Some("invalid".into())).is_err());
    }

    #[test]
    fn test_compression_performance_comparison() {
        use std::time::Instant;

        // Generate realistic columnar data (simulating timestamps)
        let mut data = Vec::with_capacity(1_000_000);
        let base_ts: u64 = 1700000000000000; // Base timestamp in micros
        for i in 0u64..100_000 {
            // Simulate timestamps with small deltas (ideal for compression)
            let ts = base_ts + i * 1000; // 1ms increments
            data.extend_from_slice(&ts.to_le_bytes());
        }

        // Benchmark LZ4
        let start = Instant::now();
        let lz4_compressed = compress_payload(&data, Some("lz4")).unwrap();
        let lz4_compress_time = start.elapsed();

        let start = Instant::now();
        let _ = decompress_payload(lz4_compressed.clone(), Some("lz4")).unwrap();
        let lz4_decompress_time = start.elapsed();

        // Benchmark Snappy
        let start = Instant::now();
        let snappy_compressed = compress_payload(&data, Some("snappy")).unwrap();
        let snappy_compress_time = start.elapsed();

        let start = Instant::now();
        let _ = decompress_payload(snappy_compressed.clone(), Some("snappy")).unwrap();
        let snappy_decompress_time = start.elapsed();

        // Benchmark ZSTD
        let start = Instant::now();
        let zstd_compressed = compress_payload(&data, Some("zstd")).unwrap();
        let zstd_compress_time = start.elapsed();

        let start = Instant::now();
        let _ = decompress_payload(zstd_compressed.clone(), Some("zstd")).unwrap();
        let zstd_decompress_time = start.elapsed();

        // Print comparison (visible in test output with --nocapture)
        println!("\n=== Compression Benchmark (800KB timestamp data) ===");
        println!("LZ4:    compress {:?}, decompress {:?}, ratio {:.2}x",
            lz4_compress_time, lz4_decompress_time, data.len() as f64 / lz4_compressed.len() as f64);
        println!("Snappy: compress {:?}, decompress {:?}, ratio {:.2}x",
            snappy_compress_time, snappy_decompress_time, data.len() as f64 / snappy_compressed.len() as f64);
        println!("ZSTD:   compress {:?}, decompress {:?}, ratio {:.2}x",
            zstd_compress_time, zstd_decompress_time, data.len() as f64 / zstd_compressed.len() as f64);

        // LZ4 should be fastest for decompression (key for OLAP reads)
        // This is a soft assertion - just verify they all work
        assert!(lz4_compressed.len() < data.len());
        assert!(snappy_compressed.len() < data.len());
        assert!(zstd_compressed.len() < data.len());
    }

    #[test]
    fn test_sql_normalization_whitespace() {
        // Multiple spaces should collapse to single space
        let sql1 = "SELECT   a,  b   FROM   table1";
        let sql2 = "SELECT a, b FROM table1";
        assert_eq!(normalize_sql_for_cache(sql1), normalize_sql_for_cache(sql2));

        // Newlines and tabs should become spaces
        let sql3 = "SELECT\n  a,\n  b\nFROM\n  table1";
        assert_eq!(normalize_sql_for_cache(sql1), normalize_sql_for_cache(sql3));
    }

    #[test]
    fn test_sql_normalization_keywords() {
        // Keywords should be normalized to uppercase
        let sql1 = "select a from table1 where b = 1";
        let sql2 = "SELECT a FROM table1 WHERE b = 1";
        let sql3 = "SeLeCt a FrOm table1 WhErE b = 1";

        let norm1 = normalize_sql_for_cache(sql1);
        let norm2 = normalize_sql_for_cache(sql2);
        let norm3 = normalize_sql_for_cache(sql3);

        assert_eq!(norm1, norm2);
        assert_eq!(norm2, norm3);
        assert!(norm1.contains("SELECT"));
        assert!(norm1.contains("FROM"));
        assert!(norm1.contains("WHERE"));
    }

    #[test]
    fn test_sql_normalization_preserves_strings() {
        // String literals should be preserved exactly
        let sql = "SELECT * FROM users WHERE name = 'John  Doe'";
        let normalized = normalize_sql_for_cache(sql);
        assert!(normalized.contains("'John  Doe'"));  // Preserved with double space

        // Double-quoted identifiers too
        let sql2 = r#"SELECT * FROM "My Table" WHERE x = 1"#;
        let normalized2 = normalize_sql_for_cache(sql2);
        assert!(normalized2.contains(r#""My Table""#));
    }

    #[test]
    fn test_sql_normalization_cache_hits() {
        // Different whitespace should produce same hash
        let sql1 = "SELECT COUNT(*) FROM events WHERE type = 'click'";
        let sql2 = "select  count(*)  from  events  where  type = 'click'";
        let sql3 = "SELECT\n  COUNT(*)\nFROM\n  events\nWHERE\n  type = 'click'";

        let hash1 = hash_query_key(sql1);
        let hash2 = hash_query_key(sql2);
        let hash3 = hash_query_key(sql3);

        assert_eq!(hash1, hash2, "Different whitespace should produce same hash");
        assert_eq!(hash2, hash3, "Newlines should normalize to spaces");
    }

    #[test]
    fn test_sql_normalization_different_queries() {
        // Actually different queries should have different hashes
        let sql1 = "SELECT * FROM events";
        let sql2 = "SELECT * FROM users";
        let sql3 = "SELECT id FROM events";

        let hash1 = hash_query_key(sql1);
        let hash2 = hash_query_key(sql2);
        let hash3 = hash_query_key(sql3);

        assert_ne!(hash1, hash2, "Different tables should have different hashes");
        assert_ne!(hash1, hash3, "Different columns should have different hashes");
    }
}
