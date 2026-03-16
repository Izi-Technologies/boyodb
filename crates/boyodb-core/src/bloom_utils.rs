use crate::engine::EngineError;
use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use tracing::warn;

const BLOOM_FALLBACK_MIN_ITEMS: usize = 1024;
const BLOOM_FALLBACK_MAX_ITEMS: usize = 10_000_000;
const DEFAULT_FP_RATE: f64 = 0.01; // 1% false positive rate

/// Calculate adaptive false positive probability based on data characteristics
///
/// Adapts FPP based on:
/// - Cardinality ratio (distinct/total): High cardinality benefits from tighter FPP
/// - Segment size: Large segments benefit from tighter FPP, small segments can use looser FPP
///
/// Returns FPP clamped to [0.001, 0.05] range
pub fn calculate_adaptive_fpp(
    row_count: usize,
    distinct_count: Option<u64>,
    segment_size_bytes: u64,
) -> f64 {
    let mut fpp = DEFAULT_FP_RATE; // Start with 1% baseline

    // Adjust based on cardinality ratio
    if let Some(distinct) = distinct_count {
        let ratio = distinct as f64 / row_count.max(1) as f64;
        if ratio > 0.8 {
            // High cardinality (>80% distinct) -> tighter FPP for better pruning
            fpp = 0.005;
        } else if ratio < 0.1 {
            // Low cardinality (<10% distinct) -> looser FPP acceptable
            fpp = 0.02;
        }
    }

    // Adjust based on segment size
    if segment_size_bytes > 10_485_760 {
        // Large segments (>10MB) -> tighter FPP justified by I/O savings
        fpp = fpp.min(0.005);
    } else if segment_size_bytes < 102_400 {
        // Small segments (<100KB) -> looser FPP to save memory
        fpp = fpp.max(0.02);
    }

    // Clamp to reasonable range
    fpp.clamp(0.001, 0.05)
}

/// A generic bloom filter index for any column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnBloomFilter {
    /// Column name this filter applies to
    pub column: String,
    /// Serialized bloom filter data
    pub filter_data: Vec<u8>,
    /// Number of items in the filter
    pub item_count: usize,
    /// False positive rate used
    pub fp_rate: f64,
}

/// Builder for creating bloom filters from column values
pub struct BloomFilterBuilder {
    column: String,
    bloom: Bloom<u64>,
    item_count: usize,
    fp_rate: f64,
}

impl BloomFilterBuilder {
    /// Create a new builder for a column with estimated item count
    pub fn new(column: &str, estimated_items: usize) -> Self {
        Self::with_fp_rate(column, estimated_items, DEFAULT_FP_RATE)
    }

    /// Create a new builder with a custom false positive rate
    pub fn with_fp_rate(column: &str, estimated_items: usize, fp_rate: f64) -> Self {
        let capacity = estimated_items.clamp(BLOOM_FALLBACK_MIN_ITEMS, BLOOM_FALLBACK_MAX_ITEMS);
        let clamped_fp_rate = fp_rate.clamp(0.001, 0.05);
        Self {
            column: column.to_string(),
            bloom: Bloom::new_for_fp_rate(capacity, clamped_fp_rate),
            item_count: 0,
            fp_rate: clamped_fp_rate,
        }
    }

    /// Create a new builder with adaptive false positive rate
    pub fn new_adaptive(
        column: &str,
        estimated_items: usize,
        distinct_count: Option<u64>,
        segment_size_bytes: u64,
    ) -> Self {
        let fp_rate = calculate_adaptive_fpp(estimated_items, distinct_count, segment_size_bytes);
        Self::with_fp_rate(column, estimated_items, fp_rate)
    }

    /// Add a string value to the bloom filter
    pub fn add_string(&mut self, value: &str) {
        let hash = hash_string(value);
        self.bloom.set(&hash);
        self.item_count += 1;
    }

    /// Add an i64 value to the bloom filter
    pub fn add_i64(&mut self, value: i64) {
        self.bloom.set(&(value as u64));
        self.item_count += 1;
    }

    /// Add a u64 value to the bloom filter
    pub fn add_u64(&mut self, value: u64) {
        self.bloom.set(&value);
        self.item_count += 1;
    }

    /// Add an f64 value to the bloom filter (hashed)
    pub fn add_f64(&mut self, value: f64) {
        let hash = value.to_bits();
        self.bloom.set(&hash);
        self.item_count += 1;
    }

    /// Add a boolean value
    pub fn add_bool(&mut self, value: bool) {
        self.bloom.set(&(value as u64));
        self.item_count += 1;
    }

    /// Build the final ColumnBloomFilter
    pub fn build(self) -> Result<ColumnBloomFilter, EngineError> {
        let filter_data = bincode::serialize(&self.bloom)
            .map_err(|e| EngineError::Internal(format!("bloom serialize failed: {e}")))?;
        Ok(ColumnBloomFilter {
            column: self.column,
            filter_data,
            item_count: self.item_count,
            fp_rate: self.fp_rate,
        })
    }
}

/// Checker for querying bloom filters
pub struct BloomFilterChecker {
    bloom: Bloom<u64>,
}

impl BloomFilterChecker {
    /// Create a checker from a ColumnBloomFilter
    pub fn from_filter(filter: &ColumnBloomFilter) -> Result<Self, EngineError> {
        let bloom: Bloom<u64> = bincode::deserialize(&filter.filter_data)
            .map_err(|e| EngineError::Internal(format!("bloom deserialize failed: {e}")))?;
        Ok(Self { bloom })
    }

    /// Check if a string value might be in the set
    pub fn might_contain_string(&self, value: &str) -> bool {
        let hash = hash_string(value);
        self.bloom.check(&hash)
    }

    /// Check if an i64 value might be in the set
    pub fn might_contain_i64(&self, value: i64) -> bool {
        self.bloom.check(&(value as u64))
    }

    /// Check if a u64 value might be in the set
    pub fn might_contain_u64(&self, value: u64) -> bool {
        self.bloom.check(&value)
    }

    /// Check if an f64 value might be in the set
    pub fn might_contain_f64(&self, value: f64) -> bool {
        self.bloom.check(&value.to_bits())
    }
}

/// Hash a string to u64 for bloom filter
fn hash_string(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Collection of bloom filters for multiple columns in a segment
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegmentBloomFilters {
    /// Map of column name to bloom filter
    pub filters: HashMap<String, ColumnBloomFilter>,
}

impl SegmentBloomFilters {
    pub fn new() -> Self {
        Self {
            filters: HashMap::new(),
        }
    }

    pub fn add_filter(&mut self, filter: ColumnBloomFilter) {
        self.filters.insert(filter.column.clone(), filter);
    }

    pub fn get_filter(&self, column: &str) -> Option<&ColumnBloomFilter> {
        self.filters.get(column)
    }

    /// Check if a value might exist in the segment for a given column
    pub fn might_contain(&self, column: &str, value: &BloomValue) -> Option<bool> {
        let filter = self.filters.get(column)?;
        let checker = BloomFilterChecker::from_filter(filter).ok()?;
        Some(match value {
            BloomValue::String(s) => checker.might_contain_string(s),
            BloomValue::I64(i) => checker.might_contain_i64(*i),
            BloomValue::U64(u) => checker.might_contain_u64(*u),
            BloomValue::F64(f) => checker.might_contain_f64(*f),
        })
    }

    /// Serialize to bytes for storage
    pub fn serialize(&self) -> Result<Vec<u8>, EngineError> {
        bincode::serialize(self)
            .map_err(|e| EngineError::Internal(format!("bloom filters serialize failed: {e}")))
    }

    /// Deserialize from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, EngineError> {
        bincode::deserialize(data)
            .map_err(|e| EngineError::Internal(format!("bloom filters deserialize failed: {e}")))
    }
}

/// Value types that can be checked against bloom filters
#[derive(Debug, Clone)]
pub enum BloomValue {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
}

// Legacy functions for backward compatibility with existing tenant_id/route_id bloom filters

pub fn serialize_bloom(bloom: &Bloom<u64>) -> Result<Vec<u8>, EngineError> {
    bincode::serialize(bloom)
        .map_err(|e| EngineError::Internal(format!("bloom serialize failed: {e}")))
}

/// Deserialize a bloom filter from bytes.
/// Returns an error if deserialization fails instead of silently falling back.
pub fn deserialize_bloom(data: &[u8]) -> Result<Bloom<u64>, EngineError> {
    bincode::deserialize(data)
        .map_err(|e| EngineError::Internal(format!("bloom deserialize failed: {e}")))
}

/// Deserialize a bloom filter, falling back to an empty filter on error.
/// Use this only when a missing/corrupt bloom filter is acceptable (e.g., during queries).
/// Logs a warning when fallback is used.
pub fn deserialize_bloom_or_empty(data: &[u8], expected_items: usize) -> Bloom<u64> {
    match bincode::deserialize(data) {
        Ok(bloom) => bloom,
        Err(e) => {
            warn!(
                error = %e,
                expected_items,
                used_items = fallback_capacity(expected_items),
                "bloom filter deserialization failed, using empty filter"
            );
            Bloom::new_for_fp_rate(fallback_capacity(expected_items), 0.01)
        }
    }
}

fn fallback_capacity(expected_items: usize) -> usize {
    expected_items.clamp(BLOOM_FALLBACK_MIN_ITEMS, BLOOM_FALLBACK_MAX_ITEMS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_capacity_clamps_bounds() {
        assert_eq!(fallback_capacity(0), BLOOM_FALLBACK_MIN_ITEMS);
        assert_eq!(fallback_capacity(1), BLOOM_FALLBACK_MIN_ITEMS);
        assert_eq!(fallback_capacity(50_000), 50_000);
        assert_eq!(fallback_capacity(usize::MAX), BLOOM_FALLBACK_MAX_ITEMS);
    }

    #[test]
    fn test_bloom_filter_builder_strings() {
        let mut builder = BloomFilterBuilder::new("user_id", 1000);
        builder.add_string("alice");
        builder.add_string("bob");
        builder.add_string("charlie");

        let filter = builder.build().unwrap();
        assert_eq!(filter.column, "user_id");
        assert_eq!(filter.item_count, 3);

        let checker = BloomFilterChecker::from_filter(&filter).unwrap();
        assert!(checker.might_contain_string("alice"));
        assert!(checker.might_contain_string("bob"));
        assert!(checker.might_contain_string("charlie"));
        // May have false positives, but "definitely_not_there" is unlikely to match
    }

    #[test]
    fn test_bloom_filter_builder_integers() {
        let mut builder = BloomFilterBuilder::new("age", 1000);
        builder.add_i64(25);
        builder.add_i64(30);
        builder.add_i64(35);

        let filter = builder.build().unwrap();
        let checker = BloomFilterChecker::from_filter(&filter).unwrap();

        assert!(checker.might_contain_i64(25));
        assert!(checker.might_contain_i64(30));
        assert!(checker.might_contain_i64(35));
    }

    #[test]
    fn test_segment_bloom_filters() {
        let mut segment_filters = SegmentBloomFilters::new();

        // Add string filter
        let mut string_builder = BloomFilterBuilder::new("name", 1000);
        string_builder.add_string("test");
        segment_filters.add_filter(string_builder.build().unwrap());

        // Add int filter
        let mut int_builder = BloomFilterBuilder::new("count", 1000);
        int_builder.add_i64(42);
        segment_filters.add_filter(int_builder.build().unwrap());

        // Test lookups
        assert_eq!(
            segment_filters.might_contain("name", &BloomValue::String("test".to_string())),
            Some(true)
        );
        assert_eq!(
            segment_filters.might_contain("count", &BloomValue::I64(42)),
            Some(true)
        );
        assert_eq!(
            segment_filters.might_contain("nonexistent", &BloomValue::I64(1)),
            None
        );
    }

    #[test]
    fn test_segment_bloom_filters_serialization() {
        let mut segment_filters = SegmentBloomFilters::new();
        let mut builder = BloomFilterBuilder::new("col", 100);
        builder.add_string("value");
        segment_filters.add_filter(builder.build().unwrap());

        let serialized = segment_filters.serialize().unwrap();
        let deserialized = SegmentBloomFilters::deserialize(&serialized).unwrap();

        assert_eq!(
            deserialized.might_contain("col", &BloomValue::String("value".to_string())),
            Some(true)
        );
    }

    #[test]
    fn test_adaptive_fpp_high_cardinality() {
        // High cardinality (>80% distinct) should use tighter FPP
        let fpp = calculate_adaptive_fpp(1000, Some(900), 1_000_000);
        assert!(fpp <= 0.005, "High cardinality should use tight FPP");
    }

    #[test]
    fn test_adaptive_fpp_low_cardinality() {
        // Low cardinality (<10% distinct) can use looser FPP
        let fpp = calculate_adaptive_fpp(1000, Some(50), 100_000);
        assert!(fpp >= 0.02, "Low cardinality should use loose FPP");
    }

    #[test]
    fn test_adaptive_fpp_large_segment() {
        // Large segments (>10MB) should use tighter FPP
        let fpp = calculate_adaptive_fpp(100000, None, 20_000_000);
        assert!(fpp <= 0.005, "Large segment should use tight FPP");
    }

    #[test]
    fn test_adaptive_fpp_small_segment() {
        // Small segments (<100KB) can use looser FPP
        let fpp = calculate_adaptive_fpp(100, None, 50_000);
        assert!(fpp >= 0.02, "Small segment should use loose FPP");
    }

    #[test]
    fn test_adaptive_fpp_clamped() {
        // FPP should always be in [0.001, 0.05] range
        let fpp = calculate_adaptive_fpp(1000, Some(1000), 100_000_000);
        assert!(fpp >= 0.001 && fpp <= 0.05, "FPP should be clamped");
    }

    #[test]
    fn test_bloom_filter_builder_adaptive() {
        // Test adaptive builder creates valid filter
        let mut builder = BloomFilterBuilder::new_adaptive(
            "user_id",
            10000,
            Some(9500), // High cardinality
            5_000_000,  // 5MB segment
        );
        builder.add_string("test_user");
        let filter = builder.build().unwrap();
        assert!(
            filter.fp_rate <= 0.01,
            "Adaptive should use appropriate FPP"
        );
    }
}
