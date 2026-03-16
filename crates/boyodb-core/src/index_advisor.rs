//! Index Advisor Module
//!
//! Analyzes query workloads and recommends indexes:
//! - Identifies missing indexes from slow queries
//! - Suggests composite indexes for multi-column filters
//! - Detects unused indexes for removal
//! - Estimates index impact on query performance
//! - Provides cost/benefit analysis

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Index advisor configuration
#[derive(Clone, Debug)]
pub struct IndexAdvisorConfig {
    /// Enable index advisor
    pub enabled: bool,
    /// Minimum query count before making recommendations
    pub min_query_samples: usize,
    /// Minimum query duration (ms) to consider for indexing
    pub slow_query_threshold_ms: u64,
    /// Maximum recommendations per table
    pub max_recommendations_per_table: usize,
    /// Analysis window (seconds)
    pub analysis_window_secs: u64,
    /// Minimum selectivity for index recommendation
    pub min_selectivity: f64,
    /// Consider covering indexes
    pub suggest_covering_indexes: bool,
}

impl Default for IndexAdvisorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_query_samples: 10,
            slow_query_threshold_ms: 100,
            max_recommendations_per_table: 5,
            analysis_window_secs: 3600, // 1 hour
            min_selectivity: 0.1,       // 10% selectivity
            suggest_covering_indexes: true,
        }
    }
}

/// Column access pattern from queries
#[derive(Clone, Debug, Default)]
pub struct ColumnAccessPattern {
    /// Column name
    pub column: String,
    /// Number of equality filters
    pub equality_count: u64,
    /// Number of range filters
    pub range_count: u64,
    /// Number of ORDER BY uses
    pub order_by_count: u64,
    /// Number of GROUP BY uses
    pub group_by_count: u64,
    /// Number of JOIN conditions
    pub join_count: u64,
    /// Number of SELECT uses
    pub select_count: u64,
    /// Estimated cardinality (distinct values)
    pub cardinality: Option<u64>,
    /// Total rows scanned when this column is filtered
    pub total_rows_scanned: u64,
}

impl ColumnAccessPattern {
    /// Calculate index benefit score
    pub fn index_score(&self) -> f64 {
        let filter_score = (self.equality_count as f64 * 2.0) + (self.range_count as f64 * 1.5);
        let sort_score = self.order_by_count as f64 * 1.0;
        let group_score = self.group_by_count as f64 * 1.0;
        let join_score = self.join_count as f64 * 2.5;

        filter_score + sort_score + group_score + join_score
    }
}

/// Table access statistics
#[derive(Clone, Debug, Default)]
pub struct TableAccessStats {
    /// Table name (database.table)
    pub table: String,
    /// Total queries accessing this table
    pub query_count: u64,
    /// Total rows scanned
    pub rows_scanned: u64,
    /// Total duration (ms)
    pub total_duration_ms: u64,
    /// Column access patterns
    pub columns: HashMap<String, ColumnAccessPattern>,
    /// Multi-column filter combinations
    pub filter_combinations: HashMap<Vec<String>, u64>,
    /// Existing indexes
    pub existing_indexes: HashSet<String>,
}

/// Query pattern for analysis
#[derive(Clone, Debug)]
pub struct QueryPattern {
    /// Query hash for deduplication
    pub query_hash: u64,
    /// Normalized SQL pattern
    pub pattern: String,
    /// Tables accessed
    pub tables: Vec<String>,
    /// Columns in WHERE clause
    pub filter_columns: Vec<(String, String, FilterType)>, // (table, column, type)
    /// Columns in ORDER BY
    pub order_columns: Vec<(String, String)>,
    /// Columns in GROUP BY
    pub group_columns: Vec<(String, String)>,
    /// Columns in SELECT
    pub select_columns: Vec<(String, String)>,
    /// Join conditions
    pub join_columns: Vec<(String, String, String, String)>, // (t1, c1, t2, c2)
    /// Execution count
    pub execution_count: u64,
    /// Total duration (ms)
    pub total_duration_ms: u64,
    /// Average rows scanned
    pub avg_rows_scanned: u64,
    /// First seen
    pub first_seen: Instant,
    /// Last seen
    pub last_seen: Instant,
}

/// Filter type in WHERE clause
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterType {
    Equality,
    Range,
    Like,
    In,
    IsNull,
    Between,
}

/// Index recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexRecommendation {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Recommended index columns (in order)
    pub columns: Vec<String>,
    /// Index type
    pub index_type: RecommendedIndexType,
    /// Estimated benefit score
    pub benefit_score: f64,
    /// Estimated query speedup factor
    pub speedup_estimate: f64,
    /// Affected query count
    pub affected_queries: usize,
    /// Estimated index size (bytes)
    pub estimated_size_bytes: u64,
    /// Reason for recommendation
    pub reason: String,
    /// SQL to create the index
    pub create_sql: String,
}

/// Recommended index type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendedIndexType {
    /// B-tree index (default)
    BTree,
    /// Hash index (equality only)
    Hash,
    /// Bloom filter (membership test)
    Bloom,
    /// Composite index
    Composite,
    /// Covering index (includes all selected columns)
    Covering,
    /// Partial index (filtered)
    Partial,
}

/// Unused index detection
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnusedIndex {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Index name
    pub index_name: String,
    /// Days since last use
    pub days_unused: u64,
    /// Index size (bytes)
    pub size_bytes: u64,
    /// Drop SQL
    pub drop_sql: String,
}

/// Index advisor for workload analysis
pub struct IndexAdvisor {
    config: IndexAdvisorConfig,
    /// Query patterns observed
    patterns: RwLock<HashMap<u64, QueryPattern>>,
    /// Table statistics
    table_stats: RwLock<HashMap<String, TableAccessStats>>,
    /// Generated recommendations
    recommendations: RwLock<Vec<IndexRecommendation>>,
    /// Last analysis time
    last_analysis: RwLock<Instant>,
    /// Total queries analyzed
    queries_analyzed: AtomicU64,
}

impl IndexAdvisor {
    pub fn new(config: IndexAdvisorConfig) -> Self {
        Self {
            config,
            patterns: RwLock::new(HashMap::new()),
            table_stats: RwLock::new(HashMap::new()),
            recommendations: RwLock::new(Vec::new()),
            last_analysis: RwLock::new(Instant::now()),
            queries_analyzed: AtomicU64::new(0),
        }
    }

    /// Record a query execution for analysis
    pub fn record_query(
        &self,
        sql: &str,
        tables: Vec<String>,
        filter_columns: Vec<(String, String, FilterType)>,
        order_columns: Vec<(String, String)>,
        group_columns: Vec<(String, String)>,
        select_columns: Vec<(String, String)>,
        join_columns: Vec<(String, String, String, String)>,
        duration_ms: u64,
        rows_scanned: u64,
    ) {
        if !self.config.enabled {
            return;
        }

        self.queries_analyzed.fetch_add(1, Ordering::Relaxed);

        let query_hash = self.hash_query(sql);
        let now = Instant::now();

        // Update query pattern
        {
            let mut patterns = self.patterns.write();
            let pattern = patterns.entry(query_hash).or_insert_with(|| QueryPattern {
                query_hash,
                pattern: Self::normalize_sql(sql),
                tables: tables.clone(),
                filter_columns: filter_columns.clone(),
                order_columns: order_columns.clone(),
                group_columns: group_columns.clone(),
                select_columns: select_columns.clone(),
                join_columns: join_columns.clone(),
                execution_count: 0,
                total_duration_ms: 0,
                avg_rows_scanned: 0,
                first_seen: now,
                last_seen: now,
            });

            pattern.execution_count += 1;
            pattern.total_duration_ms += duration_ms;
            pattern.avg_rows_scanned = (pattern.avg_rows_scanned * (pattern.execution_count - 1)
                + rows_scanned)
                / pattern.execution_count;
            pattern.last_seen = now;
        }

        // Update table statistics
        {
            let mut stats = self.table_stats.write();

            for table in &tables {
                let table_stat = stats
                    .entry(table.clone())
                    .or_insert_with(|| TableAccessStats {
                        table: table.clone(),
                        ..Default::default()
                    });

                table_stat.query_count += 1;
                table_stat.rows_scanned += rows_scanned;
                table_stat.total_duration_ms += duration_ms;

                // Update column patterns
                for (t, col, filter_type) in &filter_columns {
                    if t == table {
                        let col_stat = table_stat.columns.entry(col.clone()).or_insert_with(|| {
                            ColumnAccessPattern {
                                column: col.clone(),
                                ..Default::default()
                            }
                        });

                        match filter_type {
                            FilterType::Equality | FilterType::In => col_stat.equality_count += 1,
                            FilterType::Range | FilterType::Between => col_stat.range_count += 1,
                            _ => {}
                        }
                        col_stat.total_rows_scanned += rows_scanned;
                    }
                }

                for (t, col) in &order_columns {
                    if t == table {
                        let col_stat = table_stat.columns.entry(col.clone()).or_default();
                        col_stat.column = col.clone();
                        col_stat.order_by_count += 1;
                    }
                }

                for (t, col) in &group_columns {
                    if t == table {
                        let col_stat = table_stat.columns.entry(col.clone()).or_default();
                        col_stat.column = col.clone();
                        col_stat.group_by_count += 1;
                    }
                }

                for (t, col) in &select_columns {
                    if t == table {
                        let col_stat = table_stat.columns.entry(col.clone()).or_default();
                        col_stat.column = col.clone();
                        col_stat.select_count += 1;
                    }
                }

                // Track filter combinations
                let table_filters: Vec<String> = filter_columns
                    .iter()
                    .filter(|(t, _, _)| t == table)
                    .map(|(_, c, _)| c.clone())
                    .collect();

                if table_filters.len() > 1 {
                    let mut sorted = table_filters.clone();
                    sorted.sort();
                    *table_stat.filter_combinations.entry(sorted).or_insert(0) += 1;
                }
            }

            // Track join columns
            for (t1, c1, t2, c2) in &join_columns {
                for (table, col) in [(t1, c1), (t2, c2)] {
                    if let Some(table_stat) = stats.get_mut(table) {
                        let col_stat = table_stat.columns.entry(col.clone()).or_default();
                        col_stat.column = col.clone();
                        col_stat.join_count += 1;
                    }
                }
            }
        }
    }

    /// Analyze workload and generate recommendations
    pub fn analyze(&self) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();
        let stats = self.table_stats.read();

        for (table_name, table_stat) in stats.iter() {
            if table_stat.query_count < self.config.min_query_samples as u64 {
                continue;
            }

            let parts: Vec<&str> = table_name.split('.').collect();
            let (database, table) = if parts.len() == 2 {
                (parts[0].to_string(), parts[1].to_string())
            } else {
                ("default".to_string(), table_name.clone())
            };

            // Single-column index recommendations
            let mut column_scores: Vec<(&String, f64)> = table_stat
                .columns
                .iter()
                .map(|(col, pattern)| (col, pattern.index_score()))
                .filter(|(_, score)| *score > 1.0)
                .collect();

            column_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            for (col, score) in column_scores
                .iter()
                .take(self.config.max_recommendations_per_table)
            {
                if table_stat.existing_indexes.contains(*col) {
                    continue;
                }

                let pattern = &table_stat.columns[*col];
                let index_type = if pattern.equality_count > pattern.range_count * 2 {
                    RecommendedIndexType::Hash
                } else {
                    RecommendedIndexType::BTree
                };

                let speedup = self.estimate_speedup(pattern, table_stat.rows_scanned);

                recommendations.push(IndexRecommendation {
                    database: database.clone(),
                    table: table.clone(),
                    columns: vec![(*col).clone()],
                    index_type,
                    benefit_score: *score,
                    speedup_estimate: speedup,
                    affected_queries: pattern.equality_count as usize
                        + pattern.range_count as usize,
                    estimated_size_bytes: self.estimate_index_size(table_stat.rows_scanned, 1),
                    reason: format!(
                        "Column '{}' used in {} equality and {} range filters",
                        col, pattern.equality_count, pattern.range_count
                    ),
                    create_sql: format!(
                        "CREATE INDEX idx_{}_{} ON {}.{} ({})",
                        table, col, database, table, col
                    ),
                });
            }

            // Composite index recommendations
            for (columns, count) in &table_stat.filter_combinations {
                if *count < self.config.min_query_samples as u64 / 2 {
                    continue;
                }

                if columns.len() > 3 {
                    continue; // Don't recommend indexes with too many columns
                }

                let combined_score: f64 = columns
                    .iter()
                    .filter_map(|c| table_stat.columns.get(c))
                    .map(|p| p.index_score())
                    .sum();

                if combined_score > 3.0 {
                    let cols_str = columns.join("_");
                    if table_stat.existing_indexes.contains(&cols_str) {
                        continue;
                    }

                    recommendations.push(IndexRecommendation {
                        database: database.clone(),
                        table: table.clone(),
                        columns: columns.clone(),
                        index_type: RecommendedIndexType::Composite,
                        benefit_score: combined_score * 1.5, // Boost composite indexes
                        speedup_estimate: 2.0 + (columns.len() as f64 * 0.5),
                        affected_queries: *count as usize,
                        estimated_size_bytes: self
                            .estimate_index_size(table_stat.rows_scanned, columns.len()),
                        reason: format!(
                            "Columns ({}) frequently used together in {} queries",
                            columns.join(", "),
                            count
                        ),
                        create_sql: format!(
                            "CREATE INDEX idx_{}_{} ON {}.{} ({})",
                            table,
                            columns.join("_"),
                            database,
                            table,
                            columns.join(", ")
                        ),
                    });
                }
            }
        }

        // Sort by benefit score
        recommendations.sort_by(|a, b| b.benefit_score.partial_cmp(&a.benefit_score).unwrap());

        // Store recommendations
        *self.recommendations.write() = recommendations.clone();
        *self.last_analysis.write() = Instant::now();

        recommendations
    }

    /// Get current recommendations
    pub fn get_recommendations(&self) -> Vec<IndexRecommendation> {
        self.recommendations.read().clone()
    }

    /// Get recommendations for a specific table
    pub fn get_table_recommendations(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<IndexRecommendation> {
        self.recommendations
            .read()
            .iter()
            .filter(|r| r.database == database && r.table == table)
            .cloned()
            .collect()
    }

    /// Register an existing index
    pub fn register_index(&self, table: &str, index_name: &str) {
        let mut stats = self.table_stats.write();
        if let Some(table_stat) = stats.get_mut(table) {
            table_stat.existing_indexes.insert(index_name.to_string());
        }
    }

    /// Get statistics
    pub fn stats(&self) -> IndexAdvisorStats {
        IndexAdvisorStats {
            queries_analyzed: self.queries_analyzed.load(Ordering::Relaxed),
            tables_tracked: self.table_stats.read().len(),
            patterns_tracked: self.patterns.read().len(),
            recommendations_count: self.recommendations.read().len(),
            last_analysis_secs_ago: self.last_analysis.read().elapsed().as_secs(),
        }
    }

    fn hash_query(&self, sql: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let normalized = Self::normalize_sql(sql);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }

    fn normalize_sql(sql: &str) -> String {
        // Simple normalization: replace literals with ?
        let mut result = String::new();
        let mut in_string = false;
        let mut in_number = false;

        for c in sql.chars() {
            match c {
                '\'' if !in_string => {
                    in_string = true;
                    result.push('?');
                }
                '\'' if in_string => {
                    in_string = false;
                }
                '0'..='9' if !in_string && !in_number => {
                    in_number = true;
                    result.push('?');
                }
                '0'..='9' if in_number => {}
                _ if in_string => {}
                _ => {
                    in_number = false;
                    result.push(c);
                }
            }
        }

        result
    }

    fn estimate_speedup(&self, pattern: &ColumnAccessPattern, total_rows: u64) -> f64 {
        if total_rows == 0 {
            return 1.0;
        }

        // Estimate based on access patterns
        let filter_selectivity = if pattern.equality_count > 0 {
            0.01 // Assume 1% selectivity for equality
        } else if pattern.range_count > 0 {
            0.1 // 10% for range
        } else {
            1.0
        };

        let speedup: f64 = 1.0 / filter_selectivity;
        speedup.min(100.0).max(1.0)
    }

    fn estimate_index_size(&self, row_count: u64, column_count: usize) -> u64 {
        // Rough estimate: 8 bytes per row per column + overhead
        let bytes_per_row = (8 * column_count + 16) as u64;
        row_count * bytes_per_row
    }
}

/// Index advisor statistics
#[derive(Clone, Debug)]
pub struct IndexAdvisorStats {
    pub queries_analyzed: u64,
    pub tables_tracked: usize,
    pub patterns_tracked: usize,
    pub recommendations_count: usize,
    pub last_analysis_secs_ago: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_advisor_basic() {
        let config = IndexAdvisorConfig {
            min_query_samples: 2,
            ..Default::default()
        };
        let advisor = IndexAdvisor::new(config);

        // Record some queries
        for _ in 0..5 {
            advisor.record_query(
                "SELECT * FROM users WHERE email = 'test@example.com'",
                vec!["users".to_string()],
                vec![(
                    "users".to_string(),
                    "email".to_string(),
                    FilterType::Equality,
                )],
                vec![],
                vec![],
                vec![("users".to_string(), "email".to_string())],
                vec![],
                100,
                10000,
            );
        }

        let recommendations = advisor.analyze();
        assert!(!recommendations.is_empty());
        assert_eq!(recommendations[0].columns, vec!["email"]);
    }

    #[test]
    fn test_composite_index_recommendation() {
        let config = IndexAdvisorConfig {
            min_query_samples: 2,
            ..Default::default()
        };
        let advisor = IndexAdvisor::new(config);

        // Record queries with multiple filter columns
        for _ in 0..10 {
            advisor.record_query(
                "SELECT * FROM orders WHERE customer_id = 1 AND status = 'pending'",
                vec!["orders".to_string()],
                vec![
                    (
                        "orders".to_string(),
                        "customer_id".to_string(),
                        FilterType::Equality,
                    ),
                    (
                        "orders".to_string(),
                        "status".to_string(),
                        FilterType::Equality,
                    ),
                ],
                vec![],
                vec![],
                vec![],
                vec![],
                200,
                50000,
            );
        }

        let recommendations = advisor.analyze();
        let composite = recommendations
            .iter()
            .find(|r| r.index_type == RecommendedIndexType::Composite);
        assert!(composite.is_some());
    }

    #[test]
    fn test_column_access_pattern() {
        let mut pattern = ColumnAccessPattern::default();
        pattern.equality_count = 10;
        pattern.range_count = 5;
        pattern.join_count = 3;

        let score = pattern.index_score();
        assert!(score > 0.0);
    }
}
