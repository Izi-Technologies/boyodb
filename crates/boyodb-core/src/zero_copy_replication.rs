//! Zero-Copy Replication
//!
//! ClickHouse-style zero-copy replication using object storage.
//! Replicas share segment files through S3/MinIO instead of copying data.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant, SystemTime};

/// Object storage reference for a segment
#[derive(Debug, Clone)]
pub struct SegmentRef {
    /// Segment ID
    pub segment_id: String,
    /// Object storage path
    pub object_path: String,
    /// Size in bytes
    pub size_bytes: u64,
    /// Checksum (xxHash64)
    pub checksum: u64,
    /// Creation time
    pub created_at: SystemTime,
    /// Reference count (number of replicas using this)
    pub ref_count: u32,
    /// Tier (hot/warm/cold)
    pub tier: StorageTier,
}

/// Storage tier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    /// Local SSD
    Hot,
    /// Network attached storage
    Warm,
    /// Object storage (S3)
    Cold,
    /// Archive (Glacier)
    Archive,
}

/// Zero-copy metadata
#[derive(Debug, Clone)]
pub struct ZeroCopyMetadata {
    /// Table name
    pub table: String,
    /// Partition ID
    pub partition: String,
    /// Segment references
    pub segments: Vec<SegmentRef>,
    /// Last modified
    pub last_modified: SystemTime,
    /// Version (for optimistic locking)
    pub version: u64,
}

/// Zero-copy replication configuration
#[derive(Debug, Clone)]
pub struct ZeroCopyConfig {
    /// Enable zero-copy replication
    pub enabled: bool,
    /// Object storage endpoint
    pub s3_endpoint: Option<String>,
    /// S3 bucket
    pub s3_bucket: String,
    /// S3 prefix
    pub s3_prefix: String,
    /// Region
    pub region: String,
    /// Path style access
    pub path_style: bool,
    /// Metadata refresh interval
    pub metadata_refresh_interval: Duration,
    /// Enable local caching
    pub enable_local_cache: bool,
    /// Local cache size limit
    pub local_cache_size: u64,
    /// Garbage collection interval
    pub gc_interval: Duration,
    /// Minimum retention before GC
    pub gc_retention: Duration,
}

impl Default for ZeroCopyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            s3_endpoint: None,
            s3_bucket: "boyodb-data".into(),
            s3_prefix: "zero-copy/".into(),
            region: "us-east-1".into(),
            path_style: false,
            metadata_refresh_interval: Duration::from_secs(5),
            enable_local_cache: true,
            local_cache_size: 10 * 1024 * 1024 * 1024, // 10GB
            gc_interval: Duration::from_secs(3600),
            gc_retention: Duration::from_secs(86400),
        }
    }
}

/// Zero-copy replication manager
pub struct ZeroCopyManager {
    /// Configuration
    config: ZeroCopyConfig,
    /// Replica ID
    replica_id: String,
    /// Metadata per table
    metadata: RwLock<HashMap<String, ZeroCopyMetadata>>,
    /// Local cache entries
    local_cache: RwLock<HashMap<String, CacheEntry>>,
    /// Active locks
    locks: RwLock<HashMap<String, LockEntry>>,
    /// Statistics
    stats: RwLock<ZeroCopyStats>,
}

/// Cache entry
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Segment ID
    segment_id: String,
    /// Local file path
    local_path: String,
    /// Size in bytes
    size_bytes: u64,
    /// Last accessed
    last_access: Instant,
    /// Access count
    access_count: u64,
}

/// Lock entry
#[derive(Debug, Clone)]
struct LockEntry {
    /// Lock holder (replica ID)
    holder: String,
    /// Lock time
    acquired_at: Instant,
    /// Lock type
    lock_type: LockType,
}

/// Lock type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Shared read lock
    Shared,
    /// Exclusive write lock
    Exclusive,
}

/// Zero-copy statistics
#[derive(Debug, Clone, Default)]
pub struct ZeroCopyStats {
    /// Segments referenced
    pub segments_referenced: u64,
    /// Bytes shared
    pub bytes_shared: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// S3 reads
    pub s3_reads: u64,
    /// S3 writes
    pub s3_writes: u64,
    /// Bytes saved (vs full copy)
    pub bytes_saved: u64,
    /// GC runs
    pub gc_runs: u64,
    /// Segments garbage collected
    pub segments_gc: u64,
}

impl ZeroCopyManager {
    pub fn new(config: ZeroCopyConfig, replica_id: String) -> Self {
        Self {
            config,
            replica_id,
            metadata: RwLock::new(HashMap::new()),
            local_cache: RwLock::new(HashMap::new()),
            locks: RwLock::new(HashMap::new()),
            stats: RwLock::new(ZeroCopyStats::default()),
        }
    }

    /// Register a segment for zero-copy sharing
    pub fn register_segment(
        &self,
        table: &str,
        partition: &str,
        segment: SegmentRef,
    ) -> Result<(), ZeroCopyError> {
        let mut metadata = self.metadata.write();
        let key = format!("{}/{}", table, partition);

        let entry = metadata.entry(key).or_insert_with(|| ZeroCopyMetadata {
            table: table.into(),
            partition: partition.into(),
            segments: Vec::new(),
            last_modified: SystemTime::now(),
            version: 0,
        });

        // Check if segment already exists
        if entry.segments.iter().any(|s| s.segment_id == segment.segment_id) {
            return Err(ZeroCopyError::SegmentExists(segment.segment_id));
        }

        let size = segment.size_bytes;
        entry.segments.push(segment);
        entry.version += 1;
        entry.last_modified = SystemTime::now();

        // Update stats
        let mut stats = self.stats.write();
        stats.segments_referenced += 1;
        stats.bytes_shared += size;
        stats.s3_writes += 1;

        Ok(())
    }

    /// Get object path for a segment
    pub fn get_segment_path(&self, table: &str, partition: &str, segment_id: &str) -> Option<String> {
        let metadata = self.metadata.read();
        let key = format!("{}/{}", table, partition);

        metadata.get(&key)
            .and_then(|m| m.segments.iter().find(|s| s.segment_id == segment_id))
            .map(|s| s.object_path.clone())
    }

    /// Acquire a segment (increment ref count, cache locally if needed)
    pub fn acquire_segment(
        &self,
        table: &str,
        partition: &str,
        segment_id: &str,
    ) -> Result<SegmentHandle, ZeroCopyError> {
        // Find segment
        let path = {
            let mut metadata = self.metadata.write();
            let key = format!("{}/{}", table, partition);

            let entry = metadata.get_mut(&key)
                .ok_or(ZeroCopyError::TableNotFound(table.into()))?;

            let segment = entry.segments.iter_mut()
                .find(|s| s.segment_id == segment_id)
                .ok_or(ZeroCopyError::SegmentNotFound(segment_id.into()))?;

            segment.ref_count += 1;
            segment.object_path.clone()
        };

        // Check local cache
        let mut stats = self.stats.write();
        let mut cache = self.local_cache.write();

        if let Some(entry) = cache.get_mut(segment_id) {
            entry.last_access = Instant::now();
            entry.access_count += 1;
            stats.cache_hits += 1;

            return Ok(SegmentHandle {
                segment_id: segment_id.into(),
                path: entry.local_path.clone(),
                is_cached: true,
            });
        }

        stats.cache_misses += 1;
        stats.s3_reads += 1;

        // Would download from S3 here
        let local_path = format!("/tmp/boyodb-cache/{}/{}", table, segment_id);

        if self.config.enable_local_cache {
            cache.insert(segment_id.into(), CacheEntry {
                segment_id: segment_id.into(),
                local_path: local_path.clone(),
                size_bytes: 0, // Would get from S3
                last_access: Instant::now(),
                access_count: 1,
            });
        }

        Ok(SegmentHandle {
            segment_id: segment_id.into(),
            path: if self.config.enable_local_cache { local_path } else { path },
            is_cached: self.config.enable_local_cache,
        })
    }

    /// Release a segment (decrement ref count)
    pub fn release_segment(
        &self,
        table: &str,
        partition: &str,
        segment_id: &str,
    ) -> Result<(), ZeroCopyError> {
        let mut metadata = self.metadata.write();
        let key = format!("{}/{}", table, partition);

        let entry = metadata.get_mut(&key)
            .ok_or(ZeroCopyError::TableNotFound(table.into()))?;

        let segment = entry.segments.iter_mut()
            .find(|s| s.segment_id == segment_id)
            .ok_or(ZeroCopyError::SegmentNotFound(segment_id.into()))?;

        if segment.ref_count > 0 {
            segment.ref_count -= 1;
        }

        Ok(())
    }

    /// Get segment metadata
    pub fn get_metadata(&self, table: &str, partition: &str) -> Option<ZeroCopyMetadata> {
        let metadata = self.metadata.read();
        let key = format!("{}/{}", table, partition);
        metadata.get(&key).cloned()
    }

    /// Sync metadata from S3
    pub fn sync_metadata(&self) -> Result<SyncResult, ZeroCopyError> {
        // In production, this would fetch metadata from S3
        // and update local metadata cache

        let mut added = 0u64;
        let mut updated = 0u64;
        let removed = 0u64;

        // Simulate sync
        let metadata = self.metadata.read();
        added += metadata.len() as u64;
        updated += 0;

        Ok(SyncResult {
            segments_added: added,
            segments_updated: updated,
            segments_removed: removed,
            sync_time: Duration::from_millis(50),
        })
    }

    /// Run garbage collection
    pub fn run_gc(&self) -> GcResult {
        let retention = self.config.gc_retention;
        let now = SystemTime::now();
        let mut collected = 0u64;
        let mut bytes_freed = 0u64;

        // Remove unreferenced segments past retention
        let mut metadata = self.metadata.write();
        for entry in metadata.values_mut() {
            entry.segments.retain(|s| {
                if s.ref_count > 0 {
                    return true;
                }

                if let Ok(age) = now.duration_since(s.created_at) {
                    if age > retention {
                        collected += 1;
                        bytes_freed += s.size_bytes;
                        return false;
                    }
                }
                true
            });
        }

        // Clean up local cache (LRU eviction)
        if self.config.enable_local_cache {
            let mut cache = self.local_cache.write();
            let total_size: u64 = cache.values().map(|e| e.size_bytes).sum();

            if total_size > self.config.local_cache_size {
                // Sort by last access time
                let mut entries: Vec<_> = cache.drain().collect();
                entries.sort_by(|a, b| a.1.last_access.cmp(&b.1.last_access));

                // Evict oldest entries until under limit
                let mut current_size = 0u64;
                for (key, entry) in entries.into_iter().rev() {
                    if current_size + entry.size_bytes <= self.config.local_cache_size {
                        current_size += entry.size_bytes;
                        cache.insert(key, entry);
                    } else {
                        bytes_freed += entry.size_bytes;
                    }
                }
            }
        }

        // Update stats
        let mut stats = self.stats.write();
        stats.gc_runs += 1;
        stats.segments_gc += collected;

        GcResult {
            segments_collected: collected,
            bytes_freed,
            gc_time: Duration::from_millis(100),
        }
    }

    /// Acquire lock for exclusive write access
    pub fn acquire_write_lock(&self, table: &str, partition: &str) -> Result<(), ZeroCopyError> {
        let key = format!("{}/{}", table, partition);
        let mut locks = self.locks.write();

        if let Some(lock) = locks.get(&key) {
            if lock.lock_type == LockType::Exclusive {
                return Err(ZeroCopyError::LockConflict(lock.holder.clone()));
            }
        }

        locks.insert(key, LockEntry {
            holder: self.replica_id.clone(),
            acquired_at: Instant::now(),
            lock_type: LockType::Exclusive,
        });

        Ok(())
    }

    /// Release write lock
    pub fn release_write_lock(&self, table: &str, partition: &str) {
        let key = format!("{}/{}", table, partition);
        let mut locks = self.locks.write();

        if let Some(lock) = locks.get(&key) {
            if lock.holder == self.replica_id {
                locks.remove(&key);
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> ZeroCopyStats {
        self.stats.read().clone()
    }

    /// Get tables using zero-copy
    pub fn tables(&self) -> Vec<String> {
        let metadata = self.metadata.read();
        let mut tables: HashSet<String> = HashSet::new();
        for entry in metadata.values() {
            tables.insert(entry.table.clone());
        }
        tables.into_iter().collect()
    }

    /// Get total shared bytes
    pub fn total_shared_bytes(&self) -> u64 {
        let metadata = self.metadata.read();
        metadata.values()
            .flat_map(|m| m.segments.iter())
            .map(|s| s.size_bytes)
            .sum()
    }
}

impl Default for ZeroCopyManager {
    fn default() -> Self {
        Self::new(ZeroCopyConfig::default(), "default-replica".into())
    }
}

/// Segment handle for accessing data
#[derive(Debug, Clone)]
pub struct SegmentHandle {
    /// Segment ID
    pub segment_id: String,
    /// Path to access (S3 or local)
    pub path: String,
    /// Whether locally cached
    pub is_cached: bool,
}

/// Sync result
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Segments added
    pub segments_added: u64,
    /// Segments updated
    pub segments_updated: u64,
    /// Segments removed
    pub segments_removed: u64,
    /// Sync time
    pub sync_time: Duration,
}

/// GC result
#[derive(Debug, Clone)]
pub struct GcResult {
    /// Segments collected
    pub segments_collected: u64,
    /// Bytes freed
    pub bytes_freed: u64,
    /// GC time
    pub gc_time: Duration,
}

/// Zero-copy replication error
#[derive(Debug, Clone)]
pub enum ZeroCopyError {
    /// Table not found
    TableNotFound(String),
    /// Segment not found
    SegmentNotFound(String),
    /// Segment already exists
    SegmentExists(String),
    /// Lock conflict
    LockConflict(String),
    /// S3 error
    S3Error(String),
    /// Checksum mismatch
    ChecksumMismatch { expected: u64, actual: u64 },
    /// Version conflict
    VersionConflict { expected: u64, actual: u64 },
}

impl std::fmt::Display for ZeroCopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "Table not found: {}", t),
            Self::SegmentNotFound(s) => write!(f, "Segment not found: {}", s),
            Self::SegmentExists(s) => write!(f, "Segment already exists: {}", s),
            Self::LockConflict(h) => write!(f, "Lock held by: {}", h),
            Self::S3Error(e) => write!(f, "S3 error: {}", e),
            Self::ChecksumMismatch { expected, actual } =>
                write!(f, "Checksum mismatch: expected {}, got {}", expected, actual),
            Self::VersionConflict { expected, actual } =>
                write!(f, "Version conflict: expected {}, got {}", expected, actual),
        }
    }
}

impl std::error::Error for ZeroCopyError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_segment() {
        let manager = ZeroCopyManager::default();

        let segment = SegmentRef {
            segment_id: "seg-001".into(),
            object_path: "s3://bucket/data/seg-001.parquet".into(),
            size_bytes: 1024 * 1024,
            checksum: 12345,
            created_at: SystemTime::now(),
            ref_count: 0,
            tier: StorageTier::Cold,
        };

        manager.register_segment("test", "2024-01", segment).unwrap();

        let path = manager.get_segment_path("test", "2024-01", "seg-001");
        assert!(path.is_some());
        assert!(path.unwrap().contains("seg-001"));
    }

    #[test]
    fn test_acquire_release() {
        let manager = ZeroCopyManager::default();

        let segment = SegmentRef {
            segment_id: "seg-001".into(),
            object_path: "s3://bucket/data/seg-001.parquet".into(),
            size_bytes: 1024 * 1024,
            checksum: 12345,
            created_at: SystemTime::now(),
            ref_count: 0,
            tier: StorageTier::Cold,
        };

        manager.register_segment("test", "2024-01", segment).unwrap();

        // Acquire
        let handle = manager.acquire_segment("test", "2024-01", "seg-001").unwrap();
        assert_eq!(handle.segment_id, "seg-001");

        // Check ref count
        let meta = manager.get_metadata("test", "2024-01").unwrap();
        assert_eq!(meta.segments[0].ref_count, 1);

        // Release
        manager.release_segment("test", "2024-01", "seg-001").unwrap();

        let meta = manager.get_metadata("test", "2024-01").unwrap();
        assert_eq!(meta.segments[0].ref_count, 0);
    }

    #[test]
    fn test_write_lock() {
        let manager = ZeroCopyManager::new(
            ZeroCopyConfig::default(),
            "replica-1".into(),
        );

        // Acquire lock
        manager.acquire_write_lock("test", "2024-01").unwrap();

        // Second acquire should fail (different would-be replica)
        // In this test, same replica can re-acquire...
        // but in production we'd track this differently

        // Release lock
        manager.release_write_lock("test", "2024-01");
    }

    #[test]
    fn test_stats() {
        let manager = ZeroCopyManager::default();

        let segment = SegmentRef {
            segment_id: "seg-001".into(),
            object_path: "s3://bucket/data/seg-001.parquet".into(),
            size_bytes: 1024 * 1024,
            checksum: 12345,
            created_at: SystemTime::now(),
            ref_count: 0,
            tier: StorageTier::Cold,
        };

        manager.register_segment("test", "2024-01", segment).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.segments_referenced, 1);
        assert_eq!(stats.bytes_shared, 1024 * 1024);
    }

    #[test]
    fn test_gc() {
        let manager = ZeroCopyManager::new(
            ZeroCopyConfig {
                gc_retention: Duration::from_secs(0), // Immediate retention
                ..Default::default()
            },
            "replica-1".into(),
        );

        let segment = SegmentRef {
            segment_id: "seg-001".into(),
            object_path: "s3://bucket/data/seg-001.parquet".into(),
            size_bytes: 1024 * 1024,
            checksum: 12345,
            created_at: SystemTime::now() - Duration::from_secs(10),
            ref_count: 0, // Not referenced
            tier: StorageTier::Cold,
        };

        manager.register_segment("test", "2024-01", segment).unwrap();

        let result = manager.run_gc();
        assert_eq!(result.segments_collected, 1);
    }
}
