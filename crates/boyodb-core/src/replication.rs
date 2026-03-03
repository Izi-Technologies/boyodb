use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current manifest format version. Increment when making breaking changes to the schema.
/// Version history:
/// - 0: Legacy format (pre-versioning)
/// - 1: Added format_version field, bloom filters, compression support
/// - 2: Binary format support (bincode) for 3x smaller files and 5x faster parsing
pub const MANIFEST_FORMAT_VERSION: u32 = 2;

/// Magic bytes to identify binary manifest format
pub const MANIFEST_BINARY_MAGIC: &[u8; 4] = b"BOYO";

/// Serialize manifest to binary format (bincode)
/// Format: MAGIC (4 bytes) + VERSION (4 bytes) + BINCODE_DATA
pub fn serialize_manifest_binary(manifest: &Manifest) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(1024 * 1024); // Pre-allocate 1MB
    buf.extend_from_slice(MANIFEST_BINARY_MAGIC);
    buf.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());

    let data =
        bincode::serialize(manifest).map_err(|e| format!("bincode serialize failed: {}", e))?;
    buf.extend_from_slice(&data);
    Ok(buf)
}

/// Deserialize manifest from binary format
pub fn deserialize_manifest_binary(data: &[u8]) -> Result<Manifest, String> {
    if data.len() < 8 {
        return Err("manifest too small".into());
    }

    // Check magic bytes
    if &data[0..4] != MANIFEST_BINARY_MAGIC {
        return Err("invalid manifest magic".into());
    }

    // Read version
    let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    if version > MANIFEST_FORMAT_VERSION {
        return Err(format!(
            "manifest version {} is newer than supported {}",
            version, MANIFEST_FORMAT_VERSION
        ));
    }

    // Deserialize with bincode
    let manifest: Manifest = bincode::deserialize(&data[8..])
        .map_err(|e| format!("bincode deserialize failed: {}", e))?;

    Ok(manifest)
}

/// Check if data is binary manifest format
pub fn is_binary_manifest(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == MANIFEST_BINARY_MAGIC
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DatabaseMeta {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TableMeta {
    pub database: String,
    pub name: String,
    pub schema_json: Option<String>,
    #[serde(default)]
    pub compression: Option<String>,
    /// Deduplication configuration for merge-on-read deduplication
    #[serde(default)]
    pub deduplication: Option<crate::sql::DeduplicationConfig>,
    /// Table constraints (PRIMARY KEY, UNIQUE, CHECK, NOT NULL, DEFAULT)
    #[serde(default)]
    pub constraints: Vec<crate::sql::TableConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexState {
    Building,
    Ready,
    Failed,
}

impl Default for IndexState {
    fn default() -> Self {
        IndexState::Building
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexMeta {
    pub name: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: crate::sql::IndexType,
    #[serde(default)]
    pub state: IndexState,
    #[serde(default)]
    pub created_micros: u64,
    #[serde(default)]
    pub last_built_micros: Option<u64>,
}

/// Sequence metadata for auto-incrementing values
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequenceMeta {
    pub database: String,
    pub name: String,
    pub current_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
    pub created_micros: u64,
}

impl Default for SequenceMeta {
    fn default() -> Self {
        SequenceMeta {
            database: String::new(),
            name: String::new(),
            current_value: 0,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
            created_micros: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PrimitiveValue {
    Int64(i64),
    Int32(i32),
    UInt64(u64),
    UInt32(u32),
    Float64(f64),
    Float32(f32),
    String(String),
    Boolean(bool),
    /// Timestamp in microseconds since Unix epoch
    TimestampMicros(i64),
    /// Date as days since Unix epoch
    Date32(i32),
}

impl PrimitiveValue {
    /// Compare two PrimitiveValues for ordering (returns None if incompatible types)
    pub fn partial_cmp_value(&self, other: &PrimitiveValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (PrimitiveValue::Int64(a), PrimitiveValue::Int64(b)) => Some(a.cmp(b)),
            (PrimitiveValue::Int32(a), PrimitiveValue::Int32(b)) => Some(a.cmp(b)),
            (PrimitiveValue::UInt64(a), PrimitiveValue::UInt64(b)) => Some(a.cmp(b)),
            (PrimitiveValue::UInt32(a), PrimitiveValue::UInt32(b)) => Some(a.cmp(b)),
            (PrimitiveValue::Float64(a), PrimitiveValue::Float64(b)) => a.partial_cmp(b),
            (PrimitiveValue::Float32(a), PrimitiveValue::Float32(b)) => a.partial_cmp(b),
            (PrimitiveValue::String(a), PrimitiveValue::String(b)) => Some(a.cmp(b)),
            (PrimitiveValue::Boolean(a), PrimitiveValue::Boolean(b)) => Some(a.cmp(b)),
            (PrimitiveValue::TimestampMicros(a), PrimitiveValue::TimestampMicros(b)) => {
                Some(a.cmp(b))
            }
            (PrimitiveValue::Date32(a), PrimitiveValue::Date32(b)) => Some(a.cmp(b)),
            // Cross-type numeric comparisons (widening to i128/f64)
            (PrimitiveValue::Int64(a), PrimitiveValue::Int32(b)) => Some((*a).cmp(&(*b as i64))),
            (PrimitiveValue::Int32(a), PrimitiveValue::Int64(b)) => Some((*a as i64).cmp(b)),
            (PrimitiveValue::UInt64(a), PrimitiveValue::UInt32(b)) => Some((*a).cmp(&(*b as u64))),
            (PrimitiveValue::UInt32(a), PrimitiveValue::UInt64(b)) => Some((*a as u64).cmp(b)),
            _ => None, // Incompatible types
        }
    }

    /// Check if a value is less than this (for min check)
    pub fn is_less_than(&self, other: &PrimitiveValue) -> bool {
        self.partial_cmp_value(other)
            .map_or(false, |ord| ord == std::cmp::Ordering::Less)
    }

    /// Check if a value is greater than this (for max check)
    pub fn is_greater_than(&self, other: &PrimitiveValue) -> bool {
        self.partial_cmp_value(other)
            .map_or(false, |ord| ord == std::cmp::Ordering::Greater)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ColumnStats {
    #[serde(default)]
    pub min: Option<PrimitiveValue>,
    #[serde(default)]
    pub max: Option<PrimitiveValue>,
    #[serde(default)]
    pub bloom_filter: Option<Vec<u8>>,
    #[serde(default)]
    pub null_count: u64,
    /// Total number of rows in this column (segment row count)
    #[serde(default)]
    pub row_count: u64,
    /// Estimated distinct count (for cardinality estimation)
    #[serde(default)]
    pub distinct_count: Option<u64>,
}

impl ColumnStats {
    /// Check if a filter value is within the min/max range (can skip segment if outside)
    pub fn value_in_range(&self, value: &PrimitiveValue) -> bool {
        // If no stats, assume value might be present
        if self.min.is_none() && self.max.is_none() {
            return true;
        }
        // Check if value >= min
        if let Some(ref min) = self.min {
            if value.is_less_than(min) {
                return false;
            }
        }
        // Check if value <= max
        if let Some(ref max) = self.max {
            if value.is_greater_than(max) {
                return false;
            }
        }
        true
    }

    /// Check if a range overlaps with this column's min/max
    pub fn range_overlaps(
        &self,
        range_min: Option<&PrimitiveValue>,
        range_max: Option<&PrimitiveValue>,
    ) -> bool {
        // If no stats, assume overlap
        if self.min.is_none() && self.max.is_none() {
            return true;
        }
        // If filter has max and our min > filter max, no overlap
        if let (Some(our_min), Some(filter_max)) = (&self.min, range_max) {
            if our_min.is_greater_than(filter_max) {
                return false;
            }
        }
        // If filter has min and our max < filter min, no overlap
        if let (Some(our_max), Some(filter_min)) = (&self.max, range_min) {
            if our_max.is_less_than(filter_min) {
                return false;
            }
        }
        true
    }
}

/// Metadata for a stored view definition
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ViewMeta {
    pub database: String,
    pub name: String,
    /// The SQL query that defines this view
    pub query_sql: String,
}

/// Metadata for a stored materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MaterializedViewMeta {
    pub database: String,
    pub name: String,
    /// The SQL query that defines this materialized view
    pub query_sql: String,
    /// Unix timestamp (micros) of the last refresh
    #[serde(default)]
    pub last_refresh_micros: u64,
    /// Segment ID storing the materialized data (if any)
    #[serde(default)]
    pub data_segment_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SegmentTier {
    Hot,
    Warm,
    Cold,
}

/// Lightweight segment info for fast manifest scanning (no heavy metadata)
/// Used for petabyte-scale deployments where loading full metadata is expensive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub segment_id: String,
    pub shard_id: u16,
    pub version_added: u64,
    pub size_bytes: u64,
    pub checksum: u64,
    pub tier: SegmentTier,
    pub compression: Option<String>,
    pub database: String,
    pub table: String,
    pub watermark_micros: u64,
    pub event_time_min: Option<u64>,
    pub event_time_max: Option<u64>,
    pub tenant_id_min: Option<u64>,
    pub tenant_id_max: Option<u64>,
    pub route_id_min: Option<u64>,
    pub route_id_max: Option<u64>,
    pub schema_hash: Option<u64>,
}

impl SegmentInfo {
    /// Create from full ManifestEntry (drops heavy metadata)
    pub fn from_entry(entry: &ManifestEntry) -> Self {
        SegmentInfo {
            segment_id: entry.segment_id.clone(),
            shard_id: entry.shard_id,
            version_added: entry.version_added,
            size_bytes: entry.size_bytes,
            checksum: entry.checksum,
            tier: entry.tier,
            compression: entry.compression.clone(),
            database: entry.database.clone(),
            table: entry.table.clone(),
            watermark_micros: entry.watermark_micros,
            event_time_min: entry.event_time_min,
            event_time_max: entry.event_time_max,
            tenant_id_min: entry.tenant_id_min,
            tenant_id_max: entry.tenant_id_max,
            route_id_min: entry.route_id_min,
            route_id_max: entry.route_id_max,
            schema_hash: entry.schema_hash,
        }
    }

    /// Check if segment can be pruned based on time range (fast path)
    pub fn time_range_overlaps(&self, min_time: Option<u64>, max_time: Option<u64>) -> bool {
        match (self.event_time_min, self.event_time_max, min_time, max_time) {
            (Some(seg_min), Some(seg_max), Some(q_min), Some(q_max)) => {
                // Segment overlaps if: seg_max >= q_min AND seg_min <= q_max
                seg_max >= q_min && seg_min <= q_max
            }
            (Some(seg_min), _, _, Some(q_max)) => seg_min <= q_max,
            (_, Some(seg_max), Some(q_min), _) => seg_max >= q_min,
            _ => true, // Can't prune without time info
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub segment_id: String,
    pub shard_id: u16,
    pub version_added: u64,
    pub size_bytes: u64,
    pub checksum: u64,
    pub tier: SegmentTier,
    #[serde(default)]
    pub compression: Option<String>,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub watermark_micros: u64,
    #[serde(default)]
    pub event_time_min: Option<u64>,
    #[serde(default)]
    pub event_time_max: Option<u64>,
    #[serde(default)]
    pub tenant_id_min: Option<u64>,
    #[serde(default)]
    pub tenant_id_max: Option<u64>,
    #[serde(default)]
    pub route_id_min: Option<u64>,
    #[serde(default)]
    pub route_id_max: Option<u64>,
    #[serde(default)]
    pub bloom_tenant: Option<Vec<u8>>,
    #[serde(default)]
    pub bloom_route: Option<Vec<u8>>,
    /// Heavy metadata - loaded lazily for petabyte-scale deployments
    #[serde(default)]
    pub column_stats: Option<HashMap<String, ColumnStats>>,
    /// Stable schema fingerprint to detect mismatched or corrupted segments
    #[serde(default)]
    pub schema_hash: Option<u64>,
    // --- MVCC fields for transaction visibility ---
    /// Transaction ID that created this segment (for MVCC visibility)
    #[serde(default)]
    pub created_txn: Option<u64>,
    /// Transaction ID that deleted this segment (for MVCC visibility)
    /// If set, this segment is deleted but retained for snapshot queries
    #[serde(default)]
    pub deleted_txn: Option<u64>,
    /// Version when this segment was deleted (for MVCC visibility)
    #[serde(default)]
    pub deleted_version: Option<u64>,
}

impl ManifestEntry {
    /// Convert to lightweight SegmentInfo
    pub fn to_info(&self) -> SegmentInfo {
        SegmentInfo::from_entry(self)
    }

    /// Estimated memory size of heavy metadata (bloom filters + column stats)
    pub fn heavy_metadata_size(&self) -> usize {
        let bloom_size = self.bloom_tenant.as_ref().map_or(0, |b| b.len())
            + self.bloom_route.as_ref().map_or(0, |b| b.len());
        let stats_size = self.column_stats.as_ref().map_or(0, |stats| {
            stats
                .iter()
                .map(|(k, v)| {
                    k.len()
                        + std::mem::size_of::<ColumnStats>()
                        + v.bloom_filter.as_ref().map_or(0, |b| b.len())
                })
                .sum()
        });
        bloom_size + stats_size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema format version for backward compatibility. Defaults to 0 for legacy manifests.
    #[serde(default)]
    pub format_version: u32,
    /// Transaction version - incremented on every change
    pub version: u64,
    #[serde(default)]
    pub databases: Vec<DatabaseMeta>,
    #[serde(default)]
    pub tables: Vec<TableMeta>,
    #[serde(default)]
    pub views: Vec<ViewMeta>,
    #[serde(default)]
    pub materialized_views: Vec<MaterializedViewMeta>,
    #[serde(default)]
    pub indexes: Vec<IndexMeta>,
    #[serde(default)]
    pub sequences: Vec<SequenceMeta>,
    pub entries: Vec<ManifestEntry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest::empty()
    }
}

impl Manifest {
    pub fn empty() -> Self {
        Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 0,
            databases: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            indexes: Vec::new(),
            sequences: Vec::new(),
            entries: Vec::new(),
        }
    }

    /// Check if this manifest needs migration to the current format version
    pub fn needs_migration(&self) -> bool {
        self.format_version < MANIFEST_FORMAT_VERSION
    }

    /// Migrate manifest to current format version. Returns true if migration was performed.
    pub fn migrate_if_needed(&mut self) -> bool {
        if !self.needs_migration() {
            return false;
        }

        // Migration from version 0 to 1: just update format_version
        // Future migrations can add field transformations here
        if self.format_version == 0 {
            // No data transformation needed - all new fields have serde(default)
            self.format_version = 1;
        }

        // Migration from version 1 to 2: binary format support
        if self.format_version == 1 {
            // No data transformation needed - binary format is just a different serialization
            self.format_version = 2;
        }

        true
    }

    pub fn bump_version(&mut self) {
        self.version = self.version.wrapping_add(1);
    }

    pub fn delta_since(&self, version: u64) -> Manifest {
        Manifest {
            format_version: self.format_version,
            version: self.version,
            databases: self.databases.clone(),
            tables: self.tables.clone(),
            views: self.views.clone(),
            materialized_views: self.materialized_views.clone(),
            indexes: self.indexes.clone(),
            sequences: self.sequences.clone(),
            entries: self
                .entries
                .iter()
                .filter(|e| e.version_added > version)
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRequest {
    pub max_bytes: Option<u64>,
    pub since_version: Option<u64>,
    pub prefer_hot: bool,
    pub target_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub max_entries: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundlePlan {
    pub manifest_version: u64,
    pub entries: Vec<ManifestEntry>,
    pub total_bytes: u64,
    pub throttle_millis: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleSegment {
    pub entry: ManifestEntry,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundlePayload {
    pub plan: BundlePlan,
    pub segments: Vec<BundleSegment>,
}

pub fn compute_bundle_plan_hash(
    manifest_version: u64,
    since_version: Option<u64>,
    entries: &[ManifestEntry],
    total_bytes: u64,
    throttle_millis: u64,
) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&manifest_version.to_le_bytes());
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&throttle_millis.to_le_bytes());
    match since_version {
        Some(v) => {
            hasher.update(&[1]);
            hasher.update(&v.to_le_bytes());
        }
        None => hasher.update(&[0]),
    }
    hasher.update(&(entries.len() as u64).to_le_bytes());
    for e in entries {
        hasher.update(e.segment_id.as_bytes());
        hasher.update(&e.shard_id.to_le_bytes());
        hasher.update(&e.size_bytes.to_le_bytes());
        hasher.update(&e.checksum.to_le_bytes());
        hasher.update(&[match e.tier {
            SegmentTier::Hot => 1,
            SegmentTier::Warm => 2,
            SegmentTier::Cold => 3,
        }]);
        match &e.compression {
            Some(c) => {
                hasher.update(&[1]);
                hasher.update(c.as_bytes());
            }
            None => hasher.update(&[0]),
        }
        match e.schema_hash {
            Some(h) => {
                hasher.update(&[1]);
                hasher.update(&h.to_le_bytes());
            }
            None => hasher.update(&[0]),
        }
        hasher.update(e.database.as_bytes());
        hasher.update(e.table.as_bytes());
    }
    hasher.finalize()
}

#[derive(Default)]
pub struct BundlePlanner;

impl BundlePlanner {
    pub fn new() -> Self {
        BundlePlanner
    }

    pub fn plan(&self, manifest: &Manifest, request: BundleRequest) -> BundlePlan {
        let mut entries: Vec<ManifestEntry> = match request.since_version {
            Some(v) => manifest.delta_since(v).entries,
            None => manifest.entries.clone(),
        };

        if request.prefer_hot {
            entries.sort_by_key(|e| match e.tier {
                SegmentTier::Hot => 0,
                SegmentTier::Warm => 1,
                SegmentTier::Cold => 2,
            });
        }

        let mut total_bytes = 0u64;
        if let Some(max_bytes) = request.max_bytes {
            entries.retain(|e| {
                if total_bytes + e.size_bytes > max_bytes {
                    return false;
                }
                total_bytes += e.size_bytes;
                true
            });
        } else {
            total_bytes = entries.iter().map(|e| e.size_bytes).sum();
        }

        if let Some(max_entries) = request.max_entries {
            if entries.len() > max_entries {
                entries.truncate(max_entries);
                total_bytes = entries.iter().map(|e| e.size_bytes).sum();
            }
        }

        let throttle_millis = request
            .target_bytes_per_sec
            .and_then(|bps| {
                if bps == 0 || total_bytes == 0 {
                    None
                } else {
                    let millis = total_bytes.saturating_mul(1000).div_ceil(bps);
                    Some(millis)
                }
            })
            .unwrap_or(0);

        let plan_hash = compute_bundle_plan_hash(
            manifest.version,
            request.since_version,
            &entries,
            total_bytes,
            throttle_millis,
        );

        BundlePlan {
            manifest_version: manifest.version,
            entries,
            total_bytes,
            throttle_millis,
            since_version: request.since_version,
            plan_hash: Some(plan_hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, tier: SegmentTier, size: u64, version_added: u64) -> ManifestEntry {
        ManifestEntry {
            segment_id: id.to_string(),
            shard_id: 0,
            version_added,
            size_bytes: size,
            checksum: 1,
            tier,
            compression: None,
            database: "default".into(),
            table: "default".into(),
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
            created_txn: None,
            deleted_txn: None,
            deleted_version: None,
        }
    }

    #[test]
    fn manifest_delta_filters_by_version() {
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 3,
            databases: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            indexes: Vec::new(),
            sequences: Vec::new(),
            entries: vec![
                entry("a", SegmentTier::Hot, 10, 1),
                entry("b", SegmentTier::Warm, 20, 2),
                entry("c", SegmentTier::Cold, 30, 3),
            ],
        };

        let delta = manifest.delta_since(1);
        assert_eq!(delta.entries.len(), 2);
        assert!(delta.entries.iter().any(|e| e.segment_id == "b"));
        assert!(delta.entries.iter().any(|e| e.segment_id == "c"));

        let delta_none = manifest.delta_since(5);
        assert!(delta_none.entries.is_empty());
    }

    #[test]
    fn bundle_planner_respects_size_and_hot_priority() {
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 4,
            databases: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            indexes: Vec::new(),
            sequences: Vec::new(),
            entries: vec![
                entry("cold", SegmentTier::Cold, 50, 1),
                entry("hot", SegmentTier::Hot, 30, 2),
                entry("warm", SegmentTier::Warm, 40, 3),
            ],
        };

        let planner = BundlePlanner::new();
        let plan = planner.plan(
            &manifest,
            BundleRequest {
                max_bytes: Some(70),
                since_version: None,
                prefer_hot: true,
                target_bytes_per_sec: None,
                max_entries: None,
            },
        );

        // Sorted with hot first, size-capped to fit hot+warm (30+40=70).
        assert_eq!(plan.total_bytes, 70);
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[0].segment_id, "hot");
        assert_eq!(plan.entries[1].segment_id, "warm");
    }

    #[test]
    fn bundle_planner_sets_throttle() {
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 2,
            databases: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            indexes: Vec::new(),
            sequences: Vec::new(),
            entries: vec![entry("x", SegmentTier::Hot, 5_000_000, 1)],
        };
        let planner = BundlePlanner::new();
        let plan = planner.plan(
            &manifest,
            BundleRequest {
                max_bytes: None,
                since_version: None,
                prefer_hot: false,
                target_bytes_per_sec: Some(5_000_000),
                max_entries: None,
            },
        );
        // 5 MB at 5 MB/s should suggest ~1000 ms throttle.
        assert!(plan.throttle_millis >= 1000 && plan.throttle_millis <= 1001);
    }

    #[test]
    fn backward_compatible_manifest_without_format_version() {
        // Simulate loading an old manifest without format_version field
        let old_manifest_json = r#"{
            "version": 5,
            "databases": [{"name": "testdb"}],
            "tables": [{"database": "testdb", "name": "users", "schema_json": null}],
            "entries": [{
                "segment_id": "seg-0-1",
                "shard_id": 0,
                "version_added": 1,
                "size_bytes": 1000,
                "checksum": 12345,
                "tier": "Hot"
            }]
        }"#;

        // This should deserialize successfully with format_version defaulting to 0
        let manifest: Manifest = serde_json::from_str(old_manifest_json).unwrap();
        assert_eq!(manifest.format_version, 0); // Default for old manifests
        assert_eq!(manifest.version, 5);
        assert_eq!(manifest.databases.len(), 1);
        assert_eq!(manifest.entries.len(), 1);
        assert!(manifest.needs_migration());

        // Migration should update format_version
        let mut manifest_to_migrate = manifest.clone();
        assert!(manifest_to_migrate.migrate_if_needed());
        assert_eq!(manifest_to_migrate.format_version, MANIFEST_FORMAT_VERSION);
        assert!(!manifest_to_migrate.needs_migration());
    }

    #[test]
    fn current_manifest_does_not_need_migration() {
        let manifest = Manifest::empty();
        assert_eq!(manifest.format_version, MANIFEST_FORMAT_VERSION);
        assert!(!manifest.needs_migration());

        let mut manifest_copy = manifest.clone();
        assert!(!manifest_copy.migrate_if_needed()); // Returns false if no migration needed
    }
}
