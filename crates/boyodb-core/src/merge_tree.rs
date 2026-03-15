//! MergeTree Variants
//!
//! ClickHouse-compatible specialized merge tree engines for different use cases:
//! - ReplacingMergeTree: Deduplication by key, keeping latest version
//! - CollapsingMergeTree: Efficient updates via sign column
//! - AggregatingMergeTree: Pre-aggregation during merges
//! - SummingMergeTree: Automatic summation of numeric columns

use std::collections::HashMap;
use std::sync::Arc;

/// MergeTree engine type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTreeEngine {
    /// Standard MergeTree
    MergeTree,
    /// Replaces rows with same primary key, keeping latest version
    ReplacingMergeTree { version_column: Option<String> },
    /// Collapses rows with opposite signs
    CollapsingMergeTree { sign_column: String },
    /// Versioned collapsing with state tracking
    VersionedCollapsingMergeTree { sign_column: String, version_column: String },
    /// Pre-aggregates during merges
    AggregatingMergeTree,
    /// Automatically sums numeric columns
    SummingMergeTree { columns: Vec<String> },
}

/// Row state for collapsing operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowState {
    /// Active row (sign = 1)
    Active,
    /// Cancelled row (sign = -1)
    Cancelled,
}

impl RowState {
    pub fn from_sign(sign: i8) -> Self {
        if sign >= 0 { RowState::Active } else { RowState::Cancelled }
    }

    pub fn to_sign(&self) -> i8 {
        match self {
            RowState::Active => 1,
            RowState::Cancelled => -1,
        }
    }
}

/// Aggregate state for AggregatingMergeTree
#[derive(Debug, Clone)]
pub enum AggregateState {
    /// Sum aggregate
    Sum(f64),
    /// Count aggregate
    Count(u64),
    /// Min aggregate
    Min(f64),
    /// Max aggregate
    Max(f64),
    /// Average (sum, count)
    Avg(f64, u64),
    /// Uniq (HyperLogLog serialized state)
    Uniq(Vec<u8>),
    /// Quantile (T-Digest serialized state)
    Quantile(Vec<u8>),
    /// GroupArray
    GroupArray(Vec<String>),
}

impl AggregateState {
    /// Merge two aggregate states
    pub fn merge(&mut self, other: &AggregateState) {
        match (self, other) {
            (AggregateState::Sum(a), AggregateState::Sum(b)) => *a += b,
            (AggregateState::Count(a), AggregateState::Count(b)) => *a += b,
            (AggregateState::Min(a), AggregateState::Min(b)) => *a = a.min(*b),
            (AggregateState::Max(a), AggregateState::Max(b)) => *a = a.max(*b),
            (AggregateState::Avg(sum, count), AggregateState::Avg(sum2, count2)) => {
                *sum += sum2;
                *count += count2;
            }
            (AggregateState::GroupArray(a), AggregateState::GroupArray(b)) => {
                a.extend(b.iter().cloned());
            }
            _ => {}
        }
    }

    /// Finalize the aggregate
    pub fn finalize(&self) -> f64 {
        match self {
            AggregateState::Sum(v) => *v,
            AggregateState::Count(v) => *v as f64,
            AggregateState::Min(v) => *v,
            AggregateState::Max(v) => *v,
            AggregateState::Avg(sum, count) => {
                if *count > 0 { sum / *count as f64 } else { 0.0 }
            }
            _ => 0.0,
        }
    }
}

/// Row for merge operations
#[derive(Debug, Clone)]
pub struct MergeRow {
    /// Primary key values
    pub key: Vec<Value>,
    /// Non-key column values
    pub values: Vec<Value>,
    /// Version for ReplacingMergeTree
    pub version: u64,
    /// Sign for CollapsingMergeTree
    pub sign: i8,
    /// Aggregate states for AggregatingMergeTree
    pub aggregates: Vec<AggregateState>,
}

/// Generic value type
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Int64(v) => { 1u8.hash(state); v.hash(state); }
            Value::Float64(v) => { 2u8.hash(state); v.to_bits().hash(state); }
            Value::String(v) => { 3u8.hash(state); v.hash(state); }
            Value::Bool(v) => { 4u8.hash(state); v.hash(state); }
            Value::Bytes(v) => { 5u8.hash(state); v.hash(state); }
        }
    }
}

/// MergeTree merger implementation
pub struct MergeTreeMerger {
    /// Engine type
    engine: MergeTreeEngine,
    /// Primary key column indices
    key_columns: Vec<usize>,
    /// Aggregate column indices (for AggregatingMergeTree)
    aggregate_columns: Vec<usize>,
}

impl MergeTreeMerger {
    pub fn new(engine: MergeTreeEngine, key_columns: Vec<usize>) -> Self {
        Self {
            engine,
            key_columns,
            aggregate_columns: Vec::new(),
        }
    }

    pub fn with_aggregate_columns(mut self, columns: Vec<usize>) -> Self {
        self.aggregate_columns = columns;
        self
    }

    /// Merge rows according to engine semantics
    pub fn merge(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        match &self.engine {
            MergeTreeEngine::MergeTree => rows,
            MergeTreeEngine::ReplacingMergeTree { version_column } => {
                self.merge_replacing(rows, version_column.is_some())
            }
            MergeTreeEngine::CollapsingMergeTree { .. } => {
                self.merge_collapsing(rows)
            }
            MergeTreeEngine::VersionedCollapsingMergeTree { .. } => {
                self.merge_versioned_collapsing(rows)
            }
            MergeTreeEngine::AggregatingMergeTree => {
                self.merge_aggregating(rows)
            }
            MergeTreeEngine::SummingMergeTree { columns } => {
                self.merge_summing(rows, columns)
            }
        }
    }

    /// ReplacingMergeTree: Keep only latest version per key
    fn merge_replacing(&self, rows: Vec<MergeRow>, use_version: bool) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<Value>, MergeRow> = HashMap::new();

        for row in rows {
            let key = row.key.clone();
            
            if let Some(existing) = by_key.get(&key) {
                if use_version {
                    if row.version > existing.version {
                        by_key.insert(key, row);
                    }
                } else {
                    // Without version, last one wins
                    by_key.insert(key, row);
                }
            } else {
                by_key.insert(key, row);
            }
        }

        by_key.into_values().collect()
    }

    /// CollapsingMergeTree: Cancel rows with opposite signs
    fn merge_collapsing(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<Value>, Vec<MergeRow>> = HashMap::new();

        for row in rows {
            let key = row.key.clone();
            by_key.entry(key).or_default().push(row);
        }

        let mut result = Vec::new();

        for (_, mut group) in by_key {
            // Sort by sign (positive first)
            group.sort_by_key(|r| -r.sign);

            let mut net_sign: i32 = 0;
            let mut last_positive: Option<MergeRow> = None;
            let mut last_negative: Option<MergeRow> = None;

            for row in group {
                net_sign += row.sign as i32;
                if row.sign > 0 {
                    last_positive = Some(row);
                } else {
                    last_negative = Some(row);
                }
            }

            // Emit remaining rows
            match net_sign.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    if let Some(row) = last_positive {
                        result.push(row);
                    }
                }
                std::cmp::Ordering::Less => {
                    if let Some(row) = last_negative {
                        result.push(row);
                    }
                }
                std::cmp::Ordering::Equal => {
                    // Fully collapsed - emit nothing
                }
            }
        }

        result
    }

    /// VersionedCollapsingMergeTree: Collapse with version ordering
    fn merge_versioned_collapsing(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<Value>, Vec<MergeRow>> = HashMap::new();

        for row in rows {
            let key = row.key.clone();
            by_key.entry(key).or_default().push(row);
        }

        let mut result = Vec::new();

        for (_, mut group) in by_key {
            // Sort by version, then sign
            group.sort_by(|a, b| {
                a.version.cmp(&b.version)
                    .then_with(|| a.sign.cmp(&b.sign))
            });

            // Keep only the latest state
            let mut state: Option<MergeRow> = None;

            for row in group {
                if row.sign > 0 {
                    state = Some(row);
                } else if state.as_ref().map(|s| s.version) == Some(row.version) {
                    state = None;
                }
            }

            if let Some(row) = state {
                result.push(row);
            }
        }

        result
    }

    /// AggregatingMergeTree: Merge aggregate states
    fn merge_aggregating(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<Value>, MergeRow> = HashMap::new();

        for row in rows {
            let key = row.key.clone();
            
            if let Some(existing) = by_key.get_mut(&key) {
                // Merge aggregate states
                for (i, agg) in row.aggregates.into_iter().enumerate() {
                    if i < existing.aggregates.len() {
                        existing.aggregates[i].merge(&agg);
                    }
                }
            } else {
                by_key.insert(key, row);
            }
        }

        by_key.into_values().collect()
    }

    /// SummingMergeTree: Sum specified columns
    fn merge_summing(&self, rows: Vec<MergeRow>, sum_columns: &[String]) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<Value>, MergeRow> = HashMap::new();

        for row in rows {
            let key = row.key.clone();
            
            if let Some(existing) = by_key.get_mut(&key) {
                // Sum the values
                for (i, value) in row.values.iter().enumerate() {
                    match (&mut existing.values[i], value) {
                        (Value::Int64(a), Value::Int64(b)) => *a += b,
                        (Value::Float64(a), Value::Float64(b)) => *a += b,
                        _ => {}
                    }
                }
            } else {
                by_key.insert(key, row);
            }
        }

        // Filter out rows with all zeros in summing columns
        by_key.into_values()
            .filter(|row| {
                row.values.iter().any(|v| match v {
                    Value::Int64(n) => *n != 0,
                    Value::Float64(n) => *n != 0.0,
                    _ => true,
                })
            })
            .collect()
    }
}

/// MergeTree table configuration
#[derive(Debug, Clone)]
pub struct MergeTreeConfig {
    /// Engine type
    pub engine: MergeTreeEngine,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Order by columns (sorting key)
    pub order_by: Vec<String>,
    /// Partition by expression
    pub partition_by: Option<String>,
    /// Sample by expression (for approximate queries)
    pub sample_by: Option<String>,
    /// TTL expression
    pub ttl: Option<String>,
    /// Index granularity
    pub index_granularity: usize,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        Self {
            engine: MergeTreeEngine::MergeTree,
            primary_key: Vec::new(),
            order_by: Vec::new(),
            partition_by: None,
            sample_by: None,
            ttl: None,
            index_granularity: 8192,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(key: i64, value: i64, version: u64, sign: i8) -> MergeRow {
        MergeRow {
            key: vec![Value::Int64(key)],
            values: vec![Value::Int64(value)],
            version,
            sign,
            aggregates: Vec::new(),
        }
    }

    #[test]
    fn test_replacing_merge_tree() {
        let merger = MergeTreeMerger::new(
            MergeTreeEngine::ReplacingMergeTree { version_column: Some("version".into()) },
            vec![0],
        );

        let rows = vec![
            make_row(1, 100, 1, 1),
            make_row(1, 200, 2, 1),  // Should keep this (higher version)
            make_row(2, 300, 1, 1),
        ];

        let result = merger.merge(rows);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_collapsing_merge_tree() {
        let merger = MergeTreeMerger::new(
            MergeTreeEngine::CollapsingMergeTree { sign_column: "sign".into() },
            vec![0],
        );

        let rows = vec![
            make_row(1, 100, 0, 1),   // Insert
            make_row(1, 100, 0, -1),  // Cancel
            make_row(2, 200, 0, 1),   // Insert (not cancelled)
        ];

        let result = merger.merge(rows);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key[0], Value::Int64(2));
    }

    #[test]
    fn test_summing_merge_tree() {
        let merger = MergeTreeMerger::new(
            MergeTreeEngine::SummingMergeTree { columns: vec!["value".into()] },
            vec![0],
        );

        let rows = vec![
            make_row(1, 100, 0, 1),
            make_row(1, 50, 0, 1),
            make_row(2, 200, 0, 1),
        ];

        let result = merger.merge(rows);
        assert_eq!(result.len(), 2);
        
        // Find row with key 1 and check sum
        let row1 = result.iter().find(|r| r.key[0] == Value::Int64(1)).unwrap();
        assert_eq!(row1.values[0], Value::Int64(150));
    }

    #[test]
    fn test_aggregating_merge_tree() {
        let merger = MergeTreeMerger::new(
            MergeTreeEngine::AggregatingMergeTree,
            vec![0],
        );

        let rows = vec![
            MergeRow {
                key: vec![Value::Int64(1)],
                values: vec![],
                version: 0,
                sign: 1,
                aggregates: vec![AggregateState::Sum(100.0)],
            },
            MergeRow {
                key: vec![Value::Int64(1)],
                values: vec![],
                version: 0,
                sign: 1,
                aggregates: vec![AggregateState::Sum(50.0)],
            },
        ];

        let result = merger.merge(rows);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].aggregates[0].finalize(), 150.0);
    }
}
