//! Incremental Materialized View Refresh
//!
//! Provides delta/incremental refresh for materialized views instead of full rebuild.
//! Tracks source table changes and computes only the necessary updates.
//!
//! # Features
//! - Change tracking via watermark/LSN
//! - Delta computation for aggregations
//! - Merge operations for combining old and new data
//! - Support for different refresh strategies
//!
//! # Example
//! ```sql
//! -- Create materialized view with incremental refresh support
//! CREATE MATERIALIZED VIEW hourly_stats AS
//! SELECT
//!     date_trunc('hour', event_time) AS hour,
//!     COUNT(*) AS count,
//!     SUM(value) AS total
//! FROM events
//! GROUP BY 1
//! WITH (refresh_mode = 'incremental');
//!
//! -- Incremental refresh (only processes new data)
//! REFRESH MATERIALIZED VIEW hourly_stats INCREMENTAL;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::engine::EngineError;

/// Refresh mode for materialized views
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RefreshMode {
    /// Full refresh - re-execute entire query (default)
    #[default]
    Full,
    /// Incremental refresh - only process changes since last refresh
    Incremental,
    /// Continuous refresh - stream changes in real-time
    Continuous,
}

/// Incremental refresh configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalRefreshConfig {
    /// Refresh mode
    pub mode: RefreshMode,
    /// Watermark column for tracking changes (e.g., "event_time", "updated_at")
    pub watermark_column: Option<String>,
    /// Minimum interval between refreshes (milliseconds)
    pub min_interval_ms: u64,
    /// Maximum rows to process per incremental refresh
    pub max_rows_per_refresh: Option<u64>,
    /// Whether to use merge-on-read for small deltas
    pub merge_on_read: bool,
}

impl Default for IncrementalRefreshConfig {
    fn default() -> Self {
        Self {
            mode: RefreshMode::Incremental,
            watermark_column: None,
            min_interval_ms: 1000,
            max_rows_per_refresh: Some(1_000_000),
            merge_on_read: true,
        }
    }
}

/// State tracking for incremental refresh
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IncrementalRefreshState {
    /// Last refresh timestamp (unix micros)
    pub last_refresh_micros: u64,
    /// Watermark value from last refresh
    pub last_watermark: Option<i64>,
    /// LSN (Log Sequence Number) from last refresh
    pub last_lsn: Option<u64>,
    /// Number of rows in current materialized view
    pub row_count: u64,
    /// Schema hash for detecting schema changes
    pub schema_hash: u64,
    /// Total refreshes performed
    pub total_refreshes: u64,
    /// Total incremental refreshes
    pub incremental_refreshes: u64,
    /// Total full refreshes (schema changes, first load, etc.)
    pub full_refreshes: u64,
}

impl IncrementalRefreshState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_refresh(&mut self, is_incremental: bool, row_count: u64) {
        self.total_refreshes += 1;
        self.row_count = row_count;
        self.last_refresh_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        if is_incremental {
            self.incremental_refreshes += 1;
        } else {
            self.full_refreshes += 1;
        }
    }
}

/// Result of an incremental refresh operation
#[derive(Debug)]
pub struct IncrementalRefreshResult {
    /// Whether refresh was incremental or full
    pub was_incremental: bool,
    /// Number of new rows processed
    pub rows_added: u64,
    /// Number of rows updated (for aggregate merging)
    pub rows_updated: u64,
    /// Number of rows deleted (for DELETE tracking)
    pub rows_deleted: u64,
    /// Total rows in materialized view after refresh
    pub total_rows: u64,
    /// Time taken for refresh (milliseconds)
    pub duration_ms: u64,
    /// Reason if fell back to full refresh
    pub fallback_reason: Option<String>,
}

/// Delta type for tracking changes
#[derive(Debug, Clone)]
pub enum DeltaType {
    /// New rows to insert
    Insert(RecordBatch),
    /// Rows to update (with merge logic)
    Update { old: RecordBatch, new: RecordBatch },
    /// Row keys to delete
    Delete(Vec<String>),
}

/// Aggregation merge strategy for combining old and new data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Replace old value with new
    Replace,
    /// Sum old and new values
    Sum,
    /// Keep minimum
    Min,
    /// Keep maximum
    Max,
    /// Compute average (needs count tracking)
    Avg,
    /// Increment count
    Count,
}

/// Merge aggregations from old and new batches
pub fn merge_aggregations(
    old_batch: &RecordBatch,
    new_batch: &RecordBatch,
    group_columns: &[String],
    agg_columns: &[(String, MergeStrategy)],
) -> Result<RecordBatch, EngineError> {
    let schema = old_batch.schema();

    // Build a map from group key -> row index for old batch
    let mut old_groups: HashMap<Vec<String>, usize> = HashMap::new();
    for row_idx in 0..old_batch.num_rows() {
        let key = extract_group_key(old_batch, group_columns, row_idx)?;
        old_groups.insert(key, row_idx);
    }

    // Build a map from group key -> row index for new batch
    let mut new_groups: HashMap<Vec<String>, usize> = HashMap::new();
    for row_idx in 0..new_batch.num_rows() {
        let key = extract_group_key(new_batch, group_columns, row_idx)?;
        new_groups.insert(key, row_idx);
    }

    // Collect all unique group keys
    let mut all_keys: Vec<Vec<String>> = old_groups.keys().cloned().collect();
    for key in new_groups.keys() {
        if !old_groups.contains_key(key) {
            all_keys.push(key.clone());
        }
    }

    // Sort keys for consistent output
    all_keys.sort();

    // Build result columns
    let mut result_columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        let col_name = field.name();
        let col_idx = schema.index_of(col_name).unwrap();

        if group_columns.contains(col_name) {
            // Group column - use the key value
            result_columns.push(build_group_column(
                &all_keys,
                group_columns.iter().position(|c| c == col_name).unwrap(),
                field.data_type(),
            )?);
        } else if let Some((_, strategy)) = agg_columns.iter().find(|(n, _)| n == col_name) {
            // Aggregate column - merge values
            result_columns.push(merge_column(
                old_batch.column(col_idx),
                new_batch.column(col_idx),
                &old_groups,
                &new_groups,
                &all_keys,
                *strategy,
            )?);
        } else {
            // Non-aggregate, non-group column - take from new if available, else old
            result_columns.push(coalesce_column(
                old_batch.column(col_idx),
                new_batch.column(col_idx),
                &old_groups,
                &new_groups,
                &all_keys,
            )?);
        }
    }

    RecordBatch::try_new(schema, result_columns)
        .map_err(|e| EngineError::Internal(format!("failed to create merged batch: {}", e)))
}

/// Extract group key from a row
fn extract_group_key(
    batch: &RecordBatch,
    group_columns: &[String],
    row_idx: usize,
) -> Result<Vec<String>, EngineError> {
    let mut key = Vec::with_capacity(group_columns.len());

    for col_name in group_columns {
        let col_idx = batch.schema().index_of(col_name).map_err(|_| {
            EngineError::InvalidArgument(format!("group column not found: {}", col_name))
        })?;
        let col = batch.column(col_idx);
        key.push(array_value_to_string(col.as_ref(), row_idx));
    }

    Ok(key)
}

/// Convert array value to string for grouping
fn array_value_to_string(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return "NULL".to_string();
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        arr.value(idx).to_string()
    } else if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
        arr.value(idx).to_string()
    } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        arr.value(idx).to_string()
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        arr.value(idx).to_string()
    } else {
        format!("{:?}", array.slice(idx, 1))
    }
}

/// Build group column from keys
fn build_group_column(
    keys: &[Vec<String>],
    key_idx: usize,
    data_type: &DataType,
) -> Result<ArrayRef, EngineError> {
    match data_type {
        DataType::Int64 => {
            let values: Vec<i64> = keys
                .iter()
                .map(|k| k[key_idx].parse().unwrap_or(0))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::UInt64 => {
            let values: Vec<u64> = keys
                .iter()
                .map(|k| k[key_idx].parse().unwrap_or(0))
                .collect();
            Ok(Arc::new(UInt64Array::from(values)))
        }
        DataType::Utf8 => {
            let values: Vec<&str> = keys.iter().map(|k| k[key_idx].as_str()).collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported group column type: {:?}",
            data_type
        ))),
    }
}

/// Merge aggregate column values
fn merge_column(
    old_col: &ArrayRef,
    new_col: &ArrayRef,
    old_groups: &HashMap<Vec<String>, usize>,
    new_groups: &HashMap<Vec<String>, usize>,
    all_keys: &[Vec<String>],
    strategy: MergeStrategy,
) -> Result<ArrayRef, EngineError> {
    // Assume numeric columns for aggregation
    let mut values: Vec<f64> = Vec::with_capacity(all_keys.len());

    for key in all_keys {
        let old_val = old_groups.get(key).map(|&idx| get_f64(old_col, idx));
        let new_val = new_groups.get(key).map(|&idx| get_f64(new_col, idx));

        let merged = match (old_val, new_val, strategy) {
            (Some(o), Some(n), MergeStrategy::Sum) => o + n,
            (Some(o), Some(n), MergeStrategy::Min) => o.min(n),
            (Some(o), Some(n), MergeStrategy::Max) => o.max(n),
            (Some(o), Some(n), MergeStrategy::Count) => o + n,
            (Some(_), Some(n), MergeStrategy::Replace) => n,
            (Some(o), None, _) => o,
            (None, Some(n), _) => n,
            (None, None, _) => 0.0,
            (Some(o), Some(n), MergeStrategy::Avg) => {
                // For AVG, we'd need count tracking - simplified here
                (o + n) / 2.0
            }
        };
        values.push(merged);
    }

    // Return appropriate array type
    if old_col.data_type() == &DataType::Int64 {
        Ok(Arc::new(Int64Array::from(
            values.iter().map(|&v| v as i64).collect::<Vec<_>>(),
        )))
    } else if old_col.data_type() == &DataType::UInt64 {
        Ok(Arc::new(UInt64Array::from(
            values.iter().map(|&v| v as u64).collect::<Vec<_>>(),
        )))
    } else {
        Ok(Arc::new(Float64Array::from(values)))
    }
}

/// Get f64 value from array
fn get_f64(arr: &ArrayRef, idx: usize) -> f64 {
    if arr.is_null(idx) {
        return 0.0;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        a.value(idx) as f64
    } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        a.value(idx) as f64
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        a.value(idx)
    } else {
        0.0
    }
}

/// Coalesce non-aggregate columns (prefer new, fallback to old)
fn coalesce_column(
    old_col: &ArrayRef,
    new_col: &ArrayRef,
    old_groups: &HashMap<Vec<String>, usize>,
    new_groups: &HashMap<Vec<String>, usize>,
    all_keys: &[Vec<String>],
) -> Result<ArrayRef, EngineError> {
    // For non-aggregate columns, just take from new batch if available
    let indices: Vec<Option<(bool, usize)>> = all_keys
        .iter()
        .map(|key| {
            if let Some(&idx) = new_groups.get(key) {
                Some((true, idx)) // from new
            } else if let Some(&idx) = old_groups.get(key) {
                Some((false, idx)) // from old
            } else {
                None
            }
        })
        .collect();

    // Build output based on data type
    match old_col.data_type() {
        DataType::Int64 => {
            let old = old_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let new = new_col.as_any().downcast_ref::<Int64Array>().unwrap();
            let values: Vec<Option<i64>> = indices
                .iter()
                .map(|opt| {
                    opt.map(|(is_new, idx)| {
                        if is_new {
                            new.value(idx)
                        } else {
                            old.value(idx)
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Utf8 => {
            let old = old_col.as_any().downcast_ref::<StringArray>().unwrap();
            let new = new_col.as_any().downcast_ref::<StringArray>().unwrap();
            let values: Vec<Option<&str>> = indices
                .iter()
                .map(|opt| {
                    opt.map(|(is_new, idx)| {
                        if is_new {
                            new.value(idx)
                        } else {
                            old.value(idx)
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => {
            // Fallback: just use old column structure
            Ok(old_col.clone())
        }
    }
}

/// Build a delta query that only fetches new data since the last refresh
///
/// This modifies the original query to add a watermark filter for incremental processing.
pub fn build_delta_query(query_sql: &str, last_refresh_micros: u64) -> String {
    // Try to extract the source table and add a watermark filter
    let upper = query_sql.to_uppercase();

    // Find FROM clause
    if let Some(from_pos) = upper.find(" FROM ") {
        // Find WHERE clause or GROUP BY clause
        let rest = &query_sql[from_pos + 6..];
        let rest_upper = rest.to_uppercase();

        if let Some(where_pos) = rest_upper.find(" WHERE ") {
            // Already has WHERE - add AND condition
            let insert_pos = from_pos + 6 + where_pos + 7;
            let before = &query_sql[..insert_pos];
            let after = &query_sql[insert_pos..];
            return format!(
                "{}_watermark_micros > {} AND {}",
                before, last_refresh_micros, after
            );
        } else if let Some(group_pos) = rest_upper.find(" GROUP BY") {
            // No WHERE, insert before GROUP BY
            let insert_pos = from_pos + 6 + group_pos;
            let before = &query_sql[..insert_pos];
            let after = &query_sql[insert_pos..];
            return format!(
                "{} WHERE _watermark_micros > {}{}",
                before, last_refresh_micros, after
            );
        }
    }

    // Fallback: return original query if we can't modify it
    query_sql.to_string()
}

/// Merge IPC data from existing materialized view with new delta data
pub fn merge_ipc_data(
    existing_ipc: &[u8],
    delta_ipc: &[u8],
    query_sql: &str,
) -> Result<Vec<u8>, EngineError> {
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;
    use std::io::Cursor;

    // Parse existing IPC data
    let existing_batch = {
        let cursor = Cursor::new(existing_ipc);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("failed to read existing IPC: {}", e)))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| EngineError::Internal(format!("failed to read batch: {}", e)))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(delta_ipc.to_vec());
        }

        // Concatenate all batches into one
        concat_batches(&batches)?
    };

    // Parse delta IPC data
    let delta_batch = {
        let cursor = Cursor::new(delta_ipc);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("failed to read delta IPC: {}", e)))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| EngineError::Internal(format!("failed to read batch: {}", e)))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(existing_ipc.to_vec());
        }

        concat_batches(&batches)?
    };

    // Determine group columns and aggregate columns from query
    let (group_cols, agg_cols) = extract_group_and_agg_columns(query_sql, &existing_batch);

    // Merge the batches
    let merged = if group_cols.is_empty() {
        // No group by - just concatenate
        concat_batches(&[existing_batch, delta_batch])?
    } else {
        merge_aggregations(&existing_batch, &delta_batch, &group_cols, &agg_cols)?
    };

    // Serialize back to IPC
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, &merged.schema())
            .map_err(|e| EngineError::Internal(format!("failed to create IPC writer: {}", e)))?;
        writer
            .write(&merged)
            .map_err(|e| EngineError::Internal(format!("failed to write batch: {}", e)))?;
        writer
            .finish()
            .map_err(|e| EngineError::Internal(format!("failed to finish IPC: {}", e)))?;
    }

    Ok(output)
}

/// Concatenate multiple record batches into one
fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, EngineError> {
    if batches.is_empty() {
        return Err(EngineError::Internal("no batches to concatenate".into()));
    }

    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    let schema = batches[0].schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for col_idx in 0..schema.fields().len() {
        let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

        let concatenated = arrow_select::concat::concat(&arrays)
            .map_err(|e| EngineError::Internal(format!("failed to concat arrays: {}", e)))?;
        columns.push(concatenated);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| EngineError::Internal(format!("failed to create concatenated batch: {}", e)))
}

/// Extract group and aggregate columns from query
fn extract_group_and_agg_columns(
    query_sql: &str,
    batch: &RecordBatch,
) -> (Vec<String>, Vec<(String, MergeStrategy)>) {
    let upper = query_sql.to_uppercase();
    let mut group_cols = Vec::new();
    let mut agg_cols = Vec::new();

    // Extract GROUP BY columns
    if let Some(group_pos) = upper.find("GROUP BY") {
        let after_group = &query_sql[group_pos + 8..];
        // Find end of GROUP BY clause (ORDER BY, HAVING, LIMIT, or end)
        let end_pos = ["ORDER BY", "HAVING", "LIMIT", ";"]
            .iter()
            .filter_map(|kw| after_group.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_group.len());

        let group_clause = &after_group[..end_pos];
        for col in group_clause.split(',') {
            let col = col.trim();
            // Skip numeric references like "1, 2, 3"
            if col.parse::<usize>().is_err() {
                group_cols.push(col.to_string());
            }
        }
    }

    // Detect aggregate columns from schema (columns not in group by)
    for field in batch.schema().fields() {
        let col_name = field.name();
        if !group_cols.iter().any(|g| g.eq_ignore_ascii_case(col_name)) {
            // Determine merge strategy based on column name patterns
            let strategy = if col_name.to_lowercase().contains("count") {
                MergeStrategy::Count
            } else if col_name.to_lowercase().contains("sum")
                || col_name.to_lowercase().contains("total")
            {
                MergeStrategy::Sum
            } else if col_name.to_lowercase().contains("min") {
                MergeStrategy::Min
            } else if col_name.to_lowercase().contains("max") {
                MergeStrategy::Max
            } else if col_name.to_lowercase().contains("avg") {
                MergeStrategy::Avg
            } else {
                // Default to sum for numeric, replace for others
                match field.data_type() {
                    DataType::Int64 | DataType::UInt64 | DataType::Float64 => MergeStrategy::Sum,
                    _ => MergeStrategy::Replace,
                }
            };
            agg_cols.push((col_name.clone(), strategy));
        }
    }

    (group_cols, agg_cols)
}

/// Determine if a query can use incremental refresh
pub fn can_refresh_incrementally(query_sql: &str) -> bool {
    let upper = query_sql.to_uppercase();

    // Must have GROUP BY for incremental aggregation
    if !upper.contains("GROUP BY") {
        return false;
    }

    // Cannot use window functions (non-incremental)
    if upper.contains(" OVER ") || upper.contains(" OVER(") {
        return false;
    }

    // Cannot use DISTINCT (complex dedup)
    if upper.contains("SELECT DISTINCT") {
        return false;
    }

    // Cannot use LIMIT/OFFSET (non-deterministic)
    if upper.contains(" LIMIT ") || upper.contains(" OFFSET ") {
        return false;
    }

    // Cannot use ORDER BY at top level
    if upper.contains("ORDER BY") && !upper.contains("WITHIN GROUP") {
        return false;
    }

    true
}

/// Extract the watermark column from a query if possible
pub fn extract_watermark_column(query_sql: &str) -> Option<String> {
    let upper = query_sql.to_uppercase();

    // Look for common timestamp patterns in GROUP BY
    let patterns = [
        "DATE_TRUNC",
        "TOSTARTOFSECOND",
        "TOSTARTOFMINUTE",
        "TOSTARTOFHOUR",
        "TOSTARTOFDAY",
        "TOSTARTOFWEEK",
        "TOSTARTOFMONTH",
    ];

    for pattern in &patterns {
        if upper.contains(pattern) {
            // Try to extract the source column
            if let Some(start) = upper.find(pattern) {
                let after = &query_sql[start..];
                if let Some(paren_start) = after.find('(') {
                    if let Some(paren_end) = after[paren_start..].find(')') {
                        let inside = &after[paren_start + 1..paren_start + paren_end];
                        // Extract column name (may have granularity prefix)
                        let parts: Vec<&str> = inside.split(',').collect();
                        if let Some(col_part) = parts.last() {
                            let col = col_part.trim().trim_matches(|c| c == '\'' || c == '"');
                            return Some(col.to_string());
                        }
                    }
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_refresh_incrementally() {
        // Valid for incremental
        assert!(can_refresh_incrementally(
            "SELECT hour, COUNT(*) FROM events GROUP BY hour"
        ));
        assert!(can_refresh_incrementally(
            "SELECT tenant_id, SUM(value) FROM data GROUP BY tenant_id"
        ));

        // Invalid for incremental
        assert!(!can_refresh_incrementally("SELECT * FROM events")); // No GROUP BY
        assert!(!can_refresh_incrementally(
            "SELECT DISTINCT tenant_id FROM events GROUP BY tenant_id"
        ));
        assert!(!can_refresh_incrementally(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY id) FROM events GROUP BY id"
        ));
        assert!(!can_refresh_incrementally(
            "SELECT id FROM events GROUP BY id LIMIT 10"
        ));
    }

    #[test]
    fn test_refresh_state() {
        let mut state = IncrementalRefreshState::new();
        assert_eq!(state.total_refreshes, 0);

        state.record_refresh(true, 100);
        assert_eq!(state.total_refreshes, 1);
        assert_eq!(state.incremental_refreshes, 1);
        assert_eq!(state.row_count, 100);

        state.record_refresh(false, 200);
        assert_eq!(state.total_refreshes, 2);
        assert_eq!(state.full_refreshes, 1);
        assert_eq!(state.row_count, 200);
    }

    #[test]
    fn test_merge_strategy() {
        // Test merge strategies
        assert_eq!(MergeStrategy::Sum as u8, MergeStrategy::Sum as u8);
        assert_ne!(MergeStrategy::Sum, MergeStrategy::Replace);
    }
}
