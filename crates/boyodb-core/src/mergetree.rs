//! MergeTree Engine Implementation
//!
//! Implements MergeTree storage engine features:
//! - Primary key sorting within segments
//! - Sparse primary index for fast lookups
//! - Background merge optimization
//!
//! The sparse index stores every Nth row's primary key values,
//! allowing binary search to find the approximate position of any key.

use arrow_array::{
    cast::AsArray, Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array,
    TimestampMicrosecondArray,
};
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{DataType, Schema};
use arrow_select::take::take;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

/// Default number of rows between sparse index entries (granularity)
pub const DEFAULT_INDEX_GRANULARITY: usize = 8192;

/// Primary key definition for a table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    /// Column names that form the primary key (in sort order)
    pub columns: Vec<String>,
    /// Number of rows between index marks (granularity)
    #[serde(default = "default_granularity")]
    pub index_granularity: usize,
}

fn default_granularity() -> usize {
    DEFAULT_INDEX_GRANULARITY
}

impl Default for PrimaryKey {
    fn default() -> Self {
        PrimaryKey {
            columns: vec!["event_time".to_string()],
            index_granularity: DEFAULT_INDEX_GRANULARITY,
        }
    }
}

/// A single mark in the sparse primary index
/// Stores the primary key values at a specific row offset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMark {
    /// Row offset in the segment where this mark points
    pub row_offset: u64,
    /// Primary key column values at this row (serialized)
    pub key_values: Vec<KeyValue>,
}

/// A value in the primary key (supports common types)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KeyValue {
    Int64(i64),
    UInt64(u64),
    String(String),
    Timestamp(i64), // Microseconds since epoch
    Null,
}

impl KeyValue {
    /// Compare two key values
    pub fn cmp(&self, other: &KeyValue) -> Ordering {
        match (self, other) {
            (KeyValue::Int64(a), KeyValue::Int64(b)) => a.cmp(b),
            (KeyValue::UInt64(a), KeyValue::UInt64(b)) => a.cmp(b),
            (KeyValue::String(a), KeyValue::String(b)) => a.cmp(b),
            (KeyValue::Timestamp(a), KeyValue::Timestamp(b)) => a.cmp(b),
            (KeyValue::Null, KeyValue::Null) => Ordering::Equal,
            (KeyValue::Null, _) => Ordering::Less,
            (_, KeyValue::Null) => Ordering::Greater,
            // Cross-type comparisons (shouldn't happen in practice)
            _ => Ordering::Equal,
        }
    }

    /// Extract a key value from an Arrow array at a given index
    pub fn from_array(arr: &dyn Array, idx: usize) -> KeyValue {
        if arr.is_null(idx) {
            return KeyValue::Null;
        }

        match arr.data_type() {
            DataType::Int64 => {
                let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                KeyValue::Int64(arr.value(idx))
            }
            DataType::UInt64 => {
                let arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                KeyValue::UInt64(arr.value(idx))
            }
            DataType::Utf8 => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                KeyValue::String(arr.value(idx).to_string())
            }
            DataType::Timestamp(_, _) => {
                let arr = arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                KeyValue::Timestamp(arr.value(idx))
            }
            _ => {
                // Unknown type - return null
                KeyValue::Null
            }
        }
    }
}

/// Sparse primary index for a segment
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SparseIndex {
    /// Primary key column names
    pub key_columns: Vec<String>,
    /// Index granularity (rows between marks)
    pub granularity: usize,
    /// Total rows in the segment
    pub total_rows: u64,
    /// Index marks (sorted by primary key)
    pub marks: Vec<IndexMark>,
}

impl SparseIndex {
    /// Build a sparse index from sorted record batches
    pub fn build(
        batches: &[RecordBatch],
        key_columns: &[String],
        granularity: usize,
    ) -> Self {
        let mut marks = Vec::new();
        let mut current_row: u64 = 0;

        for batch in batches {
            let num_rows = batch.num_rows();

            // Get key column arrays
            let key_arrays: Vec<&dyn Array> = key_columns
                .iter()
                .filter_map(|col| {
                    batch.schema().index_of(col).ok().map(|idx| batch.column(idx).as_ref())
                })
                .collect();

            if key_arrays.len() != key_columns.len() {
                // Missing key columns, skip indexing
                current_row += num_rows as u64;
                continue;
            }

            // Create marks at granularity intervals
            let mut row = 0;
            while row < num_rows {
                let key_values: Vec<KeyValue> = key_arrays
                    .iter()
                    .map(|arr| KeyValue::from_array(*arr, row))
                    .collect();

                marks.push(IndexMark {
                    row_offset: current_row + row as u64,
                    key_values,
                });

                row += granularity;
            }

            current_row += num_rows as u64;
        }

        SparseIndex {
            key_columns: key_columns.to_vec(),
            granularity,
            total_rows: current_row,
            marks,
        }
    }

    /// Find the range of rows that might contain the given key
    /// Returns (start_row, end_row) - the range to scan
    pub fn find_range(&self, target_key: &[KeyValue]) -> (u64, u64) {
        if self.marks.is_empty() {
            return (0, self.total_rows);
        }

        // Binary search for the first mark >= target
        let pos = self.marks.partition_point(|mark| {
            compare_keys(&mark.key_values, target_key) == Ordering::Less
        });

        let start = if pos > 0 {
            self.marks[pos - 1].row_offset
        } else {
            0
        };

        let end = if pos < self.marks.len() {
            // Include one more granule to be safe
            if pos + 1 < self.marks.len() {
                self.marks[pos + 1].row_offset
            } else {
                self.total_rows
            }
        } else {
            self.total_rows
        };

        (start, end)
    }

    /// Serialize the index to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize the index from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

/// Compare two key value vectors lexicographically
fn compare_keys(a: &[KeyValue], b: &[KeyValue]) -> Ordering {
    for (av, bv) in a.iter().zip(b.iter()) {
        match av.cmp(bv) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Sort record batches by primary key columns
pub fn sort_by_primary_key(
    batches: Vec<RecordBatch>,
    key_columns: &[String],
) -> Result<Vec<RecordBatch>, crate::engine::EngineError> {
    use crate::engine::EngineError;

    if batches.is_empty() || key_columns.is_empty() {
        return Ok(batches);
    }

    // Combine all batches into one for sorting
    let schema = batches[0].schema();
    let combined = arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| EngineError::Internal(format!("concat error: {}", e)))?;

    if combined.num_rows() == 0 {
        return Ok(vec![combined]);
    }

    // Build sort columns
    let mut sort_columns: Vec<SortColumn> = Vec::new();
    for col_name in key_columns {
        if let Ok(idx) = schema.index_of(col_name) {
            sort_columns.push(SortColumn {
                values: combined.column(idx).clone(),
                options: Some(arrow_ord::sort::SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            });
        }
    }

    if sort_columns.is_empty() {
        return Ok(vec![combined]);
    }

    // Get sort indices
    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| EngineError::Internal(format!("sort error: {}", e)))?;

    // Apply sort to all columns
    let sorted_columns: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| EngineError::Internal(format!("take error: {}", e)))?;

    let sorted_batch = RecordBatch::try_new(schema, sorted_columns)
        .map_err(|e| EngineError::Internal(format!("batch error: {}", e)))?;

    Ok(vec![sorted_batch])
}

/// Merge multiple sorted segments into one
/// Used for background compaction
pub fn merge_sorted_segments(
    segments: Vec<Vec<RecordBatch>>,
    key_columns: &[String],
    target_size: usize,
) -> Result<Vec<RecordBatch>, crate::engine::EngineError> {
    use crate::engine::EngineError;

    if segments.is_empty() {
        return Ok(Vec::new());
    }

    // Flatten and combine all batches
    let all_batches: Vec<RecordBatch> = segments.into_iter().flatten().collect();
    if all_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Sort the combined data
    let sorted = sort_by_primary_key(all_batches, key_columns)?;

    // Split into target-sized batches if needed
    if sorted.is_empty() || sorted[0].num_rows() <= target_size {
        return Ok(sorted);
    }

    let combined = &sorted[0];
    let mut result = Vec::new();
    let mut offset = 0;

    while offset < combined.num_rows() {
        let length = std::cmp::min(target_size, combined.num_rows() - offset);
        let slice = combined.slice(offset, length);
        result.push(slice);
        offset += length;
    }

    Ok(result)
}

/// MergeTree table configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeTreeConfig {
    /// Primary key definition
    pub primary_key: PrimaryKey,
    /// Partition key columns (optional)
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Settings for background merges
    #[serde(default)]
    pub merge_settings: MergeSettings,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        MergeTreeConfig {
            primary_key: PrimaryKey::default(),
            partition_by: Vec::new(),
            merge_settings: MergeSettings::default(),
        }
    }
}

/// Settings for background merge operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSettings {
    /// Minimum number of parts to merge at once
    #[serde(default = "default_min_parts")]
    pub min_parts_to_merge: usize,
    /// Maximum number of parts to merge at once
    #[serde(default = "default_max_parts")]
    pub max_parts_to_merge: usize,
    /// Target size for merged parts (bytes)
    #[serde(default = "default_target_size")]
    pub target_part_size_bytes: u64,
    /// Minimum age before a part is eligible for merge (seconds)
    #[serde(default)]
    pub min_age_to_merge_seconds: u64,
}

fn default_min_parts() -> usize { 3 }
fn default_max_parts() -> usize { 10 }
fn default_target_size() -> u64 { 150 * 1024 * 1024 } // 150MB

impl Default for MergeSettings {
    fn default() -> Self {
        MergeSettings {
            min_parts_to_merge: 3,
            max_parts_to_merge: 10,
            target_part_size_bytes: 150 * 1024 * 1024,
            min_age_to_merge_seconds: 60,
        }
    }
}

/// Statistics about a merge operation
#[derive(Debug, Clone, Default)]
pub struct MergeStats {
    pub parts_merged: usize,
    pub rows_before: u64,
    pub rows_after: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int64Builder, StringBuilder, UInt64Builder};
    use arrow_schema::Field;

    fn make_test_batch(ids: &[i64], names: &[&str], times: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("event_time", DataType::UInt64, false),
        ]));

        let id_arr = Int64Array::from(ids.to_vec());
        let name_arr = StringArray::from(names.iter().map(|s| *s).collect::<Vec<_>>());
        let time_arr = UInt64Array::from(times.to_vec());

        RecordBatch::try_new(
            schema,
            vec![Arc::new(id_arr), Arc::new(name_arr), Arc::new(time_arr)],
        )
        .unwrap()
    }

    #[test]
    fn test_sort_by_primary_key() {
        let batch = make_test_batch(
            &[3, 1, 2],
            &["c", "a", "b"],
            &[300, 100, 200],
        );

        let sorted = sort_by_primary_key(vec![batch], &["id".to_string()]).unwrap();
        assert_eq!(sorted.len(), 1);

        let sorted_ids: Vec<i64> = sorted[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(sorted_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_sort_by_composite_key() {
        let batch = make_test_batch(
            &[1, 1, 2, 2],
            &["b", "a", "d", "c"],
            &[100, 200, 300, 400],
        );

        let sorted = sort_by_primary_key(
            vec![batch],
            &["id".to_string(), "name".to_string()],
        ).unwrap();

        let sorted_names: Vec<&str> = sorted[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap())
            .collect();

        // Should be sorted by id first, then name
        assert_eq!(sorted_names, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_sparse_index_build() {
        let batch = make_test_batch(
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );

        let index = SparseIndex::build(&[batch], &["id".to_string()], 3);

        assert_eq!(index.total_rows, 10);
        assert_eq!(index.granularity, 3);
        // Marks at rows 0, 3, 6, 9
        assert_eq!(index.marks.len(), 4);

        // First mark should have id=1
        assert_eq!(index.marks[0].row_offset, 0);
        assert_eq!(index.marks[0].key_values[0], KeyValue::Int64(1));

        // Second mark should have id=4
        assert_eq!(index.marks[1].row_offset, 3);
        assert_eq!(index.marks[1].key_values[0], KeyValue::Int64(4));
    }

    #[test]
    fn test_sparse_index_find_range() {
        let batch = make_test_batch(
            &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );

        let index = SparseIndex::build(&[batch], &["id".to_string()], 3);

        // Search for id=35 (between 30 and 40)
        let (start, end) = index.find_range(&[KeyValue::Int64(35)]);
        assert!(start <= 3); // Should start at or before row 3 (id=40)
        assert!(end >= 6);   // Should end at or after row 6 (id=70)

        // Search for id=5 (before all data)
        let (start, end) = index.find_range(&[KeyValue::Int64(5)]);
        assert_eq!(start, 0);

        // Search for id=150 (after all data)
        let (start, end) = index.find_range(&[KeyValue::Int64(150)]);
        assert!(end == 10);
    }

    #[test]
    fn test_merge_sorted_segments() {
        let batch1 = make_test_batch(&[1, 3, 5], &["a", "c", "e"], &[100, 300, 500]);
        let batch2 = make_test_batch(&[2, 4, 6], &["b", "d", "f"], &[200, 400, 600]);

        let merged = merge_sorted_segments(
            vec![vec![batch1], vec![batch2]],
            &["id".to_string()],
            1000,
        ).unwrap();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].num_rows(), 6);

        let ids: Vec<i64> = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_index_serialization() {
        let batch = make_test_batch(&[1, 2, 3], &["a", "b", "c"], &[100, 200, 300]);
        let index = SparseIndex::build(&[batch], &["id".to_string()], 2);

        let bytes = index.to_bytes();
        let restored = SparseIndex::from_bytes(&bytes).unwrap();

        assert_eq!(restored.total_rows, index.total_rows);
        assert_eq!(restored.marks.len(), index.marks.len());
        assert_eq!(restored.key_columns, index.key_columns);
    }

    #[test]
    fn test_key_value_comparison() {
        assert_eq!(KeyValue::Int64(1).cmp(&KeyValue::Int64(2)), Ordering::Less);
        assert_eq!(KeyValue::Int64(2).cmp(&KeyValue::Int64(2)), Ordering::Equal);
        assert_eq!(KeyValue::Int64(3).cmp(&KeyValue::Int64(2)), Ordering::Greater);

        assert_eq!(
            KeyValue::String("abc".to_string()).cmp(&KeyValue::String("abd".to_string())),
            Ordering::Less
        );

        assert_eq!(KeyValue::Null.cmp(&KeyValue::Int64(1)), Ordering::Less);
    }
}
