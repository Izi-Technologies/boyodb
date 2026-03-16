// Query Optimizer Module
// Provides cost-based optimization, predicate pushdown, and parallel execution planning

use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Table and Column Statistics
// ============================================================================

/// Statistics about a table for cost estimation
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// Total number of rows in the table
    pub row_count: u64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Number of segments
    pub segment_count: u64,
    /// Column-level statistics
    pub columns: HashMap<String, ColumnStatistics>,
    /// Timestamp of last stats update
    pub last_updated: u64,
    /// Column correlations for multi-column predicate estimation
    /// Key format: "col1:col2" (alphabetically ordered), Value: Pearson correlation coefficient (-1.0 to 1.0)
    pub correlations: HashMap<String, f64>,
}

/// Statistics about a column for cardinality estimation
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of distinct values (approximate)
    pub distinct_count: u64,
    /// Null count
    pub null_count: u64,
    /// Minimum value (for numeric/string columns)
    pub min_value: Option<StatValue>,
    /// Maximum value
    pub max_value: Option<StatValue>,
    /// Average value length (for strings)
    pub avg_length: Option<f64>,
    /// Histogram buckets for value distribution
    pub histogram: Option<Histogram>,
    /// Most Common Values for better equality selectivity estimation
    pub most_common_values: Option<MostCommonValues>,
}

/// Most Common Values (MCV) list for low-cardinality columns
/// Stores the top N most frequent values and their frequencies
#[derive(Debug, Clone)]
pub struct MostCommonValues {
    /// Top values sorted by frequency (descending)
    pub values: Vec<McvEntry>,
    /// Total row count used to calculate frequencies
    pub total_rows: u64,
    /// Cached sum of all MCV frequencies (avoids recomputing on every query)
    pub total_mcv_frequency: f64,
}

impl Default for MostCommonValues {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            total_rows: 0,
            total_mcv_frequency: 0.0,
        }
    }
}

impl MostCommonValues {
    /// Create a new MostCommonValues with precomputed total frequency
    pub fn new(values: Vec<McvEntry>, total_rows: u64) -> Self {
        let total_mcv_frequency = values.iter().map(|e| e.frequency).sum();
        Self {
            values,
            total_rows,
            total_mcv_frequency,
        }
    }

    /// Find an entry by value (linear scan, but MCV is typically small)
    #[inline]
    pub fn find(&self, value: &StatValue) -> Option<&McvEntry> {
        self.values.iter().find(|e| &e.value == value)
    }
}

/// Single entry in the Most Common Values list
#[derive(Debug, Clone)]
pub struct McvEntry {
    /// The value
    pub value: StatValue,
    /// Number of occurrences
    pub count: u64,
    /// Frequency as fraction of total rows (0.0 - 1.0)
    pub frequency: f64,
}

/// Histogram for value distribution
#[derive(Debug, Clone)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub lower_bound: StatValue,
    pub upper_bound: StatValue,
    pub count: u64,
    pub distinct_count: u64,
}

/// Statistical value for min/max/histogram bounds
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Null,
}

impl StatValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            StatValue::Int64(v) => Some(*v as f64),
            StatValue::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

impl PartialOrd for StatValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (StatValue::Int64(a), StatValue::Int64(b)) => a.partial_cmp(b),
            (StatValue::Float64(a), StatValue::Float64(b)) => a.partial_cmp(b),
            (StatValue::String(a), StatValue::String(b)) => a.partial_cmp(b),
            (StatValue::Bool(a), StatValue::Bool(b)) => a.partial_cmp(b),
            (StatValue::Int64(a), StatValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (StatValue::Float64(a), StatValue::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (StatValue::Null, StatValue::Null) => Some(std::cmp::Ordering::Equal),
            (StatValue::Null, _) => Some(std::cmp::Ordering::Less),
            (_, StatValue::Null) => Some(std::cmp::Ordering::Greater),
            _ => None,
        }
    }
}

// ============================================================================
// Cost Model
// ============================================================================

/// Cost estimates for query execution
#[derive(Debug, Clone, Default)]
pub struct QueryCost {
    /// Estimated CPU cost (in abstract units)
    pub cpu_cost: f64,
    /// Estimated I/O cost (pages to read)
    pub io_cost: f64,
    /// Estimated memory cost (bytes)
    pub memory_cost: f64,
    /// Estimated network cost (for distributed queries)
    pub network_cost: f64,
    /// Estimated output rows
    pub output_rows: f64,
    /// Estimated output size in bytes
    pub output_bytes: f64,
}

impl QueryCost {
    pub fn total_cost(&self) -> f64 {
        // Weighted cost combining all factors
        // IO is typically most expensive, followed by network
        self.cpu_cost + (self.io_cost * 10.0) + (self.network_cost * 5.0) + (self.memory_cost * 0.1)
    }

    pub fn add(&self, other: &QueryCost) -> QueryCost {
        QueryCost {
            cpu_cost: self.cpu_cost + other.cpu_cost,
            io_cost: self.io_cost + other.io_cost,
            memory_cost: self.memory_cost.max(other.memory_cost), // Pipeline = max
            network_cost: self.network_cost + other.network_cost,
            output_rows: other.output_rows,
            output_bytes: other.output_bytes,
        }
    }
}

/// Cost model parameters (tunable)
#[derive(Debug, Clone)]
pub struct CostModelParams {
    /// Cost per row for CPU operations
    pub cpu_tuple_cost: f64,
    /// Cost per operator invocation
    pub cpu_operator_cost: f64,
    /// Cost per index tuple
    pub cpu_index_tuple_cost: f64,
    /// Cost per page read from disk
    pub seq_page_cost: f64,
    /// Cost per random page read
    pub random_page_cost: f64,
    /// Cost per byte transferred over network
    pub network_byte_cost: f64,
    /// Network latency factor
    pub network_latency_factor: f64,
    /// Network bandwidth factor
    pub network_bandwidth_factor: f64,
    /// Page size in bytes
    pub page_size: u64,
    /// Effective cache size for cost estimation
    pub effective_cache_size: u64,
    /// Work memory for sorts/hashes (bytes)
    pub work_mem: u64,
    /// Parallel query overhead factor
    pub parallel_setup_cost: f64,
    /// Parallel tuple transfer cost
    pub parallel_tuple_cost: f64,
}

impl Default for CostModelParams {
    fn default() -> Self {
        Self {
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            cpu_index_tuple_cost: 0.005,
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            network_byte_cost: 0.001,
            network_latency_factor: 0.1,
            network_bandwidth_factor: 0.001,
            page_size: 8192,
            effective_cache_size: 4 * 1024 * 1024 * 1024, // 4GB
            work_mem: 256 * 1024 * 1024,                  // 256MB
            parallel_setup_cost: 1000.0,
            parallel_tuple_cost: 0.1,
        }
    }
}

// ============================================================================
// Logical Plan Representation
// ============================================================================

/// Logical query plan for optimization
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan
    Scan {
        table: String,
        database: String,
        columns: Vec<String>,
        filter: Option<Predicate>,
    },
    /// Filter operation
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },
    /// Projection
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<ProjectExpr>,
    },
    /// Aggregation
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Join two tables
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: JoinCondition,
    },
    /// Sort
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },
    /// Union
    Union { inputs: Vec<LogicalPlan>, all: bool },
    /// Subquery
    Subquery {
        input: Box<LogicalPlan>,
        alias: String,
    },
}

#[derive(Debug, Clone)]
pub enum ProjectExpr {
    Column(String),
    Alias {
        expr: Box<ProjectExpr>,
        alias: String,
    },
    Function {
        name: String,
        args: Vec<ProjectExpr>,
    },
    Literal(StatValue),
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggFunction,
    pub column: Option<String>,
    pub distinct: bool,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Predicate),
    Using(Vec<String>),
    Natural,
    None,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
}

// ============================================================================
// Predicate Representation
// ============================================================================

/// Predicate for filtering
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Column comparison: col op value
    Comparison {
        column: String,
        op: CompareOp,
        value: StatValue,
    },
    /// Column to column comparison
    ColumnCompare {
        left: String,
        op: CompareOp,
        right: String,
    },
    /// IN list
    In {
        column: String,
        values: Vec<StatValue>,
        negated: bool,
    },
    /// BETWEEN
    Between {
        column: String,
        low: StatValue,
        high: StatValue,
        negated: bool,
    },
    /// LIKE pattern
    Like {
        column: String,
        pattern: String,
        negated: bool,
    },
    /// IS NULL
    IsNull { column: String, negated: bool },
    /// AND combination
    And(Vec<Predicate>),
    /// OR combination
    Or(Vec<Predicate>),
    /// NOT
    Not(Box<Predicate>),
    /// Always true
    True,
    /// Always false
    False,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Predicate {
    /// Extract columns referenced by this predicate
    pub fn columns(&self) -> Vec<String> {
        match self {
            Predicate::Comparison { column, .. } => vec![column.clone()],
            Predicate::ColumnCompare { left, right, .. } => vec![left.clone(), right.clone()],
            Predicate::In { column, .. } => vec![column.clone()],
            Predicate::Between { column, .. } => vec![column.clone()],
            Predicate::Like { column, .. } => vec![column.clone()],
            Predicate::IsNull { column, .. } => vec![column.clone()],
            Predicate::And(preds) | Predicate::Or(preds) => {
                preds.iter().flat_map(|p| p.columns()).collect()
            }
            Predicate::Not(p) => p.columns(),
            Predicate::True | Predicate::False => vec![],
        }
    }

    /// Check if predicate references only the given columns
    pub fn references_only(&self, columns: &[String]) -> bool {
        self.columns().iter().all(|c| columns.contains(c))
    }

    /// Simplify predicate by removing redundant parts
    pub fn simplify(self) -> Predicate {
        match self {
            Predicate::And(mut preds) => {
                preds = preds
                    .into_iter()
                    .map(|p| p.simplify())
                    .filter(|p| !matches!(p, Predicate::True))
                    .collect();
                if preds.iter().any(|p| matches!(p, Predicate::False)) {
                    return Predicate::False;
                }
                match preds.len() {
                    0 => Predicate::True,
                    1 => preds.remove(0),
                    _ => Predicate::And(preds),
                }
            }
            Predicate::Or(mut preds) => {
                preds = preds
                    .into_iter()
                    .map(|p| p.simplify())
                    .filter(|p| !matches!(p, Predicate::False))
                    .collect();
                if preds.iter().any(|p| matches!(p, Predicate::True)) {
                    return Predicate::True;
                }
                match preds.len() {
                    0 => Predicate::False,
                    1 => preds.remove(0),
                    _ => Predicate::Or(preds),
                }
            }
            Predicate::Not(p) => match p.simplify() {
                Predicate::True => Predicate::False,
                Predicate::False => Predicate::True,
                Predicate::Not(inner) => *inner,
                other => Predicate::Not(Box::new(other)),
            },
            other => other,
        }
    }
}

// ============================================================================
// Physical Plan Representation
// ============================================================================

/// Physical execution plan
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential table scan
    SeqScan {
        table: String,
        database: String,
        columns: Vec<String>,
        filter: Option<Predicate>,
        parallel_degree: usize,
    },
    /// Index scan
    IndexScan {
        table: String,
        database: String,
        index: String,
        columns: Vec<String>,
        filter: Option<Predicate>,
        range: Option<IndexRange>,
    },
    /// Filter on input
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Predicate,
    },
    /// Project columns/expressions
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ProjectExpr>,
    },
    /// Hash aggregation
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Sorted aggregation (streaming)
    SortedAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Hash join
    HashJoin {
        build: Box<PhysicalPlan>,
        probe: Box<PhysicalPlan>,
        join_type: JoinType,
        build_keys: Vec<String>,
        probe_keys: Vec<String>,
        parallel_build: bool,
    },
    /// Sort-merge join
    MergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_keys: Vec<String>,
        right_keys: Vec<String>,
    },
    /// Nested loop join (for small tables or non-equi joins)
    NestedLoopJoin {
        outer: Box<PhysicalPlan>,
        inner: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<Predicate>,
    },
    /// Sort
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<SortExpr>,
        limit: Option<usize>,
    },
    /// Top-N sort (more efficient for LIMIT)
    TopN {
        input: Box<PhysicalPlan>,
        order_by: Vec<SortExpr>,
        limit: usize,
    },
    /// Limit
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: usize,
    },
    /// Exchange for parallelism
    Exchange {
        input: Box<PhysicalPlan>,
        partitioning: Partitioning,
    },
    /// Gather parallel streams
    Gather { inputs: Vec<PhysicalPlan> },
    /// Union all
    UnionAll { inputs: Vec<PhysicalPlan> },
}

#[derive(Debug, Clone)]
pub struct IndexRange {
    pub start: Option<StatValue>,
    pub start_inclusive: bool,
    pub end: Option<StatValue>,
    pub end_inclusive: bool,
}

#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Round-robin distribution
    RoundRobin(usize),
    /// Hash partitioning on columns
    Hash {
        columns: Vec<String>,
        num_partitions: usize,
    },
    /// Range partitioning
    Range {
        column: String,
        boundaries: Vec<StatValue>,
    },
    /// Single partition (no parallelism)
    Single,
}

// ============================================================================
// Selectivity Estimation
// ============================================================================

/// Selectivity estimator for predicates
pub struct SelectivityEstimator {
    /// Default selectivity for unknown predicates
    pub default_selectivity: f64,
    /// Default selectivity for equality
    pub eq_selectivity: f64,
    /// Default selectivity for range
    pub range_selectivity: f64,
}

impl Default for SelectivityEstimator {
    fn default() -> Self {
        Self {
            default_selectivity: 0.25,
            eq_selectivity: 0.01,    // 1% match for equality
            range_selectivity: 0.33, // 33% for range predicates
        }
    }
}

impl SelectivityEstimator {
    /// Estimate selectivity of a predicate given column statistics
    pub fn estimate(
        &self,
        predicate: &Predicate,
        stats: &HashMap<String, ColumnStatistics>,
    ) -> f64 {
        match predicate {
            Predicate::Comparison { column, op, value } => {
                if let Some(col_stats) = stats.get(column) {
                    self.estimate_comparison(col_stats, *op, value)
                } else {
                    match op {
                        CompareOp::Eq => self.eq_selectivity,
                        _ => self.range_selectivity,
                    }
                }
            }
            Predicate::In {
                column,
                values,
                negated,
            } => {
                let base = if let Some(col_stats) = stats.get(column) {
                    let distinct = col_stats.distinct_count.max(1) as f64;
                    (values.len() as f64 / distinct).min(1.0)
                } else {
                    (values.len() as f64 * self.eq_selectivity).min(1.0)
                };
                if *negated {
                    1.0 - base
                } else {
                    base
                }
            }
            Predicate::Between {
                column,
                low,
                high,
                negated,
            } => {
                let base = if let Some(col_stats) = stats.get(column) {
                    self.estimate_range(col_stats, low, high)
                } else {
                    self.range_selectivity
                };
                if *negated {
                    1.0 - base
                } else {
                    base
                }
            }
            Predicate::IsNull { column, negated } => {
                let base = if let Some(col_stats) = stats.get(column) {
                    let total = col_stats.null_count + col_stats.distinct_count;
                    if total > 0 {
                        col_stats.null_count as f64 / total as f64
                    } else {
                        0.01
                    }
                } else {
                    0.01
                };
                if *negated {
                    1.0 - base
                } else {
                    base
                }
            }
            Predicate::Like { negated, .. } => {
                // LIKE selectivity depends on pattern, use default
                let base = self.default_selectivity;
                if *negated {
                    1.0 - base
                } else {
                    base
                }
            }
            Predicate::And(preds) => {
                // Without correlations, assume independence
                preds.iter().map(|p| self.estimate(p, stats)).product()
            }
            Predicate::Or(preds) => {
                // P(A or B) = P(A) + P(B) - P(A and B)
                // Simplified: assume independence
                let individual: Vec<f64> = preds.iter().map(|p| self.estimate(p, stats)).collect();
                1.0 - individual.iter().map(|s| 1.0 - s).product::<f64>()
            }
            Predicate::Not(p) => 1.0 - self.estimate(p, stats),
            Predicate::True => 1.0,
            Predicate::False => 0.0,
            Predicate::ColumnCompare { .. } => self.default_selectivity,
        }
    }

    fn estimate_comparison(
        &self,
        stats: &ColumnStatistics,
        op: CompareOp,
        value: &StatValue,
    ) -> f64 {
        match op {
            CompareOp::Eq => {
                // First, check if value is in Most Common Values list
                if let Some(ref mcv) = stats.most_common_values {
                    if let Some(entry) = mcv.find(value) {
                        // Found in MCV - use actual frequency
                        return entry.frequency;
                    }
                    // Not in MCV - estimate from remaining values
                    // Use cached total_mcv_frequency to avoid recomputing
                    let remaining_fraction = 1.0 - mcv.total_mcv_frequency;
                    // Remaining distinct = total distinct - MCV count
                    let remaining_distinct =
                        stats.distinct_count.saturating_sub(mcv.values.len() as u64);
                    if remaining_distinct > 0 {
                        return remaining_fraction / remaining_distinct as f64;
                    }
                }
                // Fall back to uniform distribution assumption
                1.0 / stats.distinct_count.max(1) as f64
            }
            CompareOp::Ne => {
                // For inequality, check if value is in MCV
                if let Some(ref mcv) = stats.most_common_values {
                    if let Some(entry) = mcv.find(value) {
                        // Found in MCV - selectivity is 1 - frequency
                        return 1.0 - entry.frequency;
                    }
                }
                // Fall back to uniform distribution
                1.0 - (1.0 / stats.distinct_count.max(1) as f64)
            }
            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                // Use histogram if available
                if let Some(ref histogram) = stats.histogram {
                    self.estimate_from_histogram(histogram, op, value)
                } else if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
                    // Linear interpolation
                    self.estimate_linear(min, max, op, value)
                } else {
                    self.range_selectivity
                }
            }
        }
    }

    fn estimate_from_histogram(
        &self,
        histogram: &Histogram,
        op: CompareOp,
        value: &StatValue,
    ) -> f64 {
        let total_count: u64 = histogram.buckets.iter().map(|b| b.count).sum();
        if total_count == 0 {
            return self.range_selectivity;
        }

        let mut matching_count = 0u64;
        for bucket in &histogram.buckets {
            let bucket_matches = match op {
                CompareOp::Lt => value > &bucket.lower_bound,
                CompareOp::Le => value >= &bucket.lower_bound,
                CompareOp::Gt => value < &bucket.upper_bound,
                CompareOp::Ge => value <= &bucket.upper_bound,
                _ => false,
            };
            if bucket_matches {
                matching_count += bucket.count;
            }
        }

        matching_count as f64 / total_count as f64
    }

    fn estimate_linear(
        &self,
        min: &StatValue,
        max: &StatValue,
        op: CompareOp,
        value: &StatValue,
    ) -> f64 {
        let (min_f, max_f, val_f) = match (min.as_f64(), max.as_f64(), value.as_f64()) {
            (Some(mn), Some(mx), Some(v)) => (mn, mx, v),
            _ => return self.range_selectivity,
        };

        if max_f <= min_f {
            return self.range_selectivity;
        }

        let fraction = (val_f - min_f) / (max_f - min_f);
        let fraction = fraction.max(0.0).min(1.0);

        match op {
            CompareOp::Lt => fraction,
            CompareOp::Le => fraction,
            CompareOp::Gt => 1.0 - fraction,
            CompareOp::Ge => 1.0 - fraction,
            _ => self.range_selectivity,
        }
    }

    fn estimate_range(&self, stats: &ColumnStatistics, low: &StatValue, high: &StatValue) -> f64 {
        if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
            let (min_f, max_f, low_f, high_f) =
                match (min.as_f64(), max.as_f64(), low.as_f64(), high.as_f64()) {
                    (Some(mn), Some(mx), Some(lo), Some(hi)) => (mn, mx, lo, hi),
                    _ => return self.range_selectivity,
                };

            if max_f <= min_f {
                return self.range_selectivity;
            }

            let range_start = (low_f - min_f) / (max_f - min_f);
            let range_end = (high_f - min_f) / (max_f - min_f);

            (range_end - range_start).max(0.0).min(1.0)
        } else {
            self.range_selectivity
        }
    }

    /// Estimate selectivity using full table stats including correlations
    /// This provides more accurate estimates for multi-column predicates
    pub fn estimate_with_correlations(
        &self,
        predicate: &Predicate,
        table_stats: &TableStats,
    ) -> f64 {
        self.estimate_with_correlations_impl(
            predicate,
            &table_stats.columns,
            &table_stats.correlations,
        )
    }

    fn estimate_with_correlations_impl(
        &self,
        predicate: &Predicate,
        stats: &HashMap<String, ColumnStatistics>,
        correlations: &HashMap<String, f64>,
    ) -> f64 {
        match predicate {
            Predicate::And(preds) => {
                // Use correlations to adjust for non-independence
                self.estimate_and_with_correlations(preds, stats, correlations)
            }
            Predicate::Or(preds) => {
                // Use correlations for OR as well
                self.estimate_or_with_correlations(preds, stats, correlations)
            }
            // For other predicates, delegate to the basic estimate
            _ => self.estimate(predicate, stats),
        }
    }

    /// Estimate AND predicate selectivity using column correlations
    fn estimate_and_with_correlations(
        &self,
        preds: &[Predicate],
        stats: &HashMap<String, ColumnStatistics>,
        correlations: &HashMap<String, f64>,
    ) -> f64 {
        if preds.is_empty() {
            return 1.0;
        }
        if preds.len() == 1 {
            return self.estimate_with_correlations_impl(&preds[0], stats, correlations);
        }

        // Get individual selectivities
        let selectivities: Vec<f64> = preds
            .iter()
            .map(|p| self.estimate_with_correlations_impl(p, stats, correlations))
            .collect();

        // Extract columns involved in each predicate
        let columns: Vec<Option<String>> = preds.iter().map(extract_predicate_column).collect();

        // Start with independence assumption
        let mut result = selectivities[0];

        for i in 1..selectivities.len() {
            let s_i = selectivities[i];

            // Check if we have correlation info between this column and previous ones
            let mut max_correlation = 0.0_f64;

            if let Some(ref col_i) = columns[i] {
                for j in 0..i {
                    if let Some(ref col_j) = columns[j] {
                        if let Some(corr) = lookup_correlation(correlations, col_i, col_j) {
                            max_correlation = max_correlation.max(corr.abs());
                        }
                    }
                }
            }

            // Adjust selectivity based on correlation
            // High correlation -> predicates are dependent -> combined selectivity is higher
            // Formula: adjusted = s1 * (s2 + (1-s2) * correlation * s1)
            // When correlation = 0: result = s1 * s2 (independence)
            // When correlation = 1: result approaches max(s1, s2) (full dependence)
            if max_correlation > 0.01 {
                // Correlation exists - adjust for dependence
                // Use the formula: P(A AND B) = min(P(A), P(B)) * (1-c) + max(P(A), P(B)) * c * min(P(A), P(B))
                // Simplified: blend between independent and dependent estimates
                let independent = result * s_i;
                let dependent = result.min(s_i); // Fully correlated = min of selectivities
                result = independent * (1.0 - max_correlation) + dependent * max_correlation;
            } else {
                // No correlation - assume independence
                result *= s_i;
            }
        }

        result.max(0.0).min(1.0)
    }

    /// Estimate OR predicate selectivity using column correlations
    fn estimate_or_with_correlations(
        &self,
        preds: &[Predicate],
        stats: &HashMap<String, ColumnStatistics>,
        correlations: &HashMap<String, f64>,
    ) -> f64 {
        if preds.is_empty() {
            return 0.0;
        }
        if preds.len() == 1 {
            return self.estimate_with_correlations_impl(&preds[0], stats, correlations);
        }

        // Get individual selectivities
        let selectivities: Vec<f64> = preds
            .iter()
            .map(|p| self.estimate_with_correlations_impl(p, stats, correlations))
            .collect();

        // Extract columns
        let columns: Vec<Option<String>> = preds.iter().map(extract_predicate_column).collect();

        // P(A OR B) = P(A) + P(B) - P(A AND B)
        // Start with first selectivity
        let mut result = selectivities[0];

        for i in 1..selectivities.len() {
            let s_i = selectivities[i];

            // Check correlation
            let mut max_correlation = 0.0_f64;
            if let Some(ref col_i) = columns[i] {
                for j in 0..i {
                    if let Some(ref col_j) = columns[j] {
                        if let Some(corr) = lookup_correlation(correlations, col_i, col_j) {
                            max_correlation = max_correlation.max(corr.abs());
                        }
                    }
                }
            }

            // Adjust P(A AND B) based on correlation
            let and_independent = result * s_i;
            let and_dependent = result.min(s_i);
            let and_estimate =
                and_independent * (1.0 - max_correlation) + and_dependent * max_correlation;

            // P(A OR B) = P(A) + P(B) - P(A AND B)
            result = result + s_i - and_estimate;
        }

        result.max(0.0).min(1.0)
    }
}

/// Extract the column name from a predicate if it's a simple column predicate
fn extract_predicate_column(predicate: &Predicate) -> Option<String> {
    match predicate {
        Predicate::Comparison { column, .. } => Some(column.clone()),
        Predicate::In { column, .. } => Some(column.clone()),
        Predicate::Between { column, .. } => Some(column.clone()),
        Predicate::IsNull { column, .. } => Some(column.clone()),
        Predicate::Like { column, .. } => Some(column.clone()),
        _ => None,
    }
}

/// Create a canonical correlation key from two column names (alphabetically ordered)
/// Look up correlation between two columns without allocating a key string
/// Returns None if no correlation exists
#[inline]
fn lookup_correlation(correlations: &HashMap<String, f64>, col1: &str, col2: &str) -> Option<f64> {
    // Keys are stored as "smaller:larger" alphabetically
    // Build key in stack buffer to avoid heap allocation
    let (first, second) = if col1 < col2 {
        (col1, col2)
    } else {
        (col2, col1)
    };

    // Use a stack-allocated buffer for small keys (most column names < 64 chars)
    let key_len = first.len() + 1 + second.len();
    if key_len <= 128 {
        let mut buf = [0u8; 128];
        buf[..first.len()].copy_from_slice(first.as_bytes());
        buf[first.len()] = b':';
        buf[first.len() + 1..key_len].copy_from_slice(second.as_bytes());
        // SAFETY: we're constructing valid UTF-8 from two valid UTF-8 strings and ':'
        let key = unsafe { std::str::from_utf8_unchecked(&buf[..key_len]) };
        correlations.get(key).copied()
    } else {
        // Fallback for very long column names (rare)
        let key = format!("{}:{}", first, second);
        correlations.get(&key).copied()
    }
}

// Keep for backwards compatibility with existing code that generates keys
fn make_correlation_key(col1: &str, col2: &str) -> String {
    if col1 < col2 {
        format!("{}:{}", col1, col2)
    } else {
        format!("{}:{}", col2, col1)
    }
}

// ============================================================================
// Cost-Based Optimizer
// ============================================================================

/// Query optimizer that uses cost-based decisions
pub struct CostBasedOptimizer {
    /// Cost model parameters
    pub cost_params: CostModelParams,
    /// Selectivity estimator
    pub selectivity: SelectivityEstimator,
    /// Available table statistics
    pub table_stats: HashMap<String, TableStats>,
    /// Available indexes
    pub indexes: HashMap<String, Vec<IndexInfo>>,
    /// Parallelism degree
    pub parallelism: usize,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

impl CostBasedOptimizer {
    pub fn new(parallelism: usize) -> Self {
        Self {
            cost_params: CostModelParams::default(),
            selectivity: SelectivityEstimator::default(),
            table_stats: HashMap::new(),
            indexes: HashMap::new(),
            parallelism,
        }
    }

    /// Update statistics for a table
    pub fn update_stats(&mut self, table: &str, stats: TableStats) {
        self.table_stats.insert(table.to_string(), stats);
    }

    /// Add index information
    pub fn add_index(&mut self, table: &str, index: IndexInfo) {
        self.indexes
            .entry(table.to_string())
            .or_default()
            .push(index);
    }

    /// Optimize a logical plan into a physical plan
    pub fn optimize(&self, plan: LogicalPlan) -> PhysicalPlan {
        // Apply optimization rules
        let plan = self.push_down_predicates(plan);
        let plan = self.optimize_joins(plan);

        // Convert to physical plan
        self.to_physical(plan)
    }

    /// Push predicates down to table scans
    fn push_down_predicates(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                match *input {
                    LogicalPlan::Scan {
                        table,
                        database,
                        columns,
                        filter,
                    } => {
                        // Merge predicate into scan filter
                        let merged = match filter {
                            Some(existing) => Predicate::And(vec![existing, predicate]),
                            None => predicate,
                        };
                        LogicalPlan::Scan {
                            table,
                            database,
                            columns,
                            filter: Some(merged.simplify()),
                        }
                    }
                    LogicalPlan::Join {
                        left,
                        right,
                        join_type,
                        condition,
                    } => {
                        // Try to push predicate to appropriate side
                        let (left_cols, right_cols) = self.get_join_columns(&left, &right);
                        let (left_pred, right_pred, remaining) =
                            self.split_predicate(&predicate, &left_cols, &right_cols);

                        let new_left = if let Some(p) = left_pred {
                            Box::new(self.push_down_predicates(LogicalPlan::Filter {
                                input: left,
                                predicate: p,
                            }))
                        } else {
                            Box::new(self.push_down_predicates(*left))
                        };

                        let new_right = if let Some(p) = right_pred {
                            Box::new(self.push_down_predicates(LogicalPlan::Filter {
                                input: right,
                                predicate: p,
                            }))
                        } else {
                            Box::new(self.push_down_predicates(*right))
                        };

                        let join = LogicalPlan::Join {
                            left: new_left,
                            right: new_right,
                            join_type,
                            condition,
                        };

                        if let Some(p) = remaining {
                            LogicalPlan::Filter {
                                input: Box::new(join),
                                predicate: p,
                            }
                        } else {
                            join
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(self.push_down_predicates(other)),
                        predicate,
                    },
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => LogicalPlan::Join {
                left: Box::new(self.push_down_predicates(*left)),
                right: Box::new(self.push_down_predicates(*right)),
                join_type,
                condition,
            },
            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.push_down_predicates(*input)),
                columns,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.push_down_predicates(*input)),
                group_by,
                aggregates,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.push_down_predicates(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.push_down_predicates(*input)),
                limit,
                offset,
            },
            other => other,
        }
    }

    fn get_join_columns(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> (Vec<String>, Vec<String>) {
        fn get_output_columns(plan: &LogicalPlan) -> Vec<String> {
            match plan {
                LogicalPlan::Scan { columns, .. } => columns.clone(),
                LogicalPlan::Project { columns, .. } => columns
                    .iter()
                    .filter_map(|c| match c {
                        ProjectExpr::Column(name) => Some(name.clone()),
                        ProjectExpr::Alias { alias, .. } => Some(alias.clone()),
                        _ => None,
                    })
                    .collect(),
                LogicalPlan::Filter { input, .. } => get_output_columns(input),
                LogicalPlan::Aggregate {
                    group_by,
                    aggregates,
                    ..
                } => {
                    let mut cols = group_by.clone();
                    for agg in aggregates {
                        if let Some(ref alias) = agg.alias {
                            cols.push(alias.clone());
                        }
                    }
                    cols
                }
                _ => vec![],
            }
        }
        (get_output_columns(left), get_output_columns(right))
    }

    fn split_predicate(
        &self,
        predicate: &Predicate,
        left_cols: &[String],
        right_cols: &[String],
    ) -> (Option<Predicate>, Option<Predicate>, Option<Predicate>) {
        match predicate {
            Predicate::And(preds) => {
                let mut left_preds = vec![];
                let mut right_preds = vec![];
                let mut remaining = vec![];

                for p in preds {
                    if p.references_only(left_cols) {
                        left_preds.push(p.clone());
                    } else if p.references_only(right_cols) {
                        right_preds.push(p.clone());
                    } else {
                        remaining.push(p.clone());
                    }
                }

                let left = if left_preds.is_empty() {
                    None
                } else if left_preds.len() == 1 {
                    Some(left_preds.remove(0))
                } else {
                    Some(Predicate::And(left_preds))
                };

                let right = if right_preds.is_empty() {
                    None
                } else if right_preds.len() == 1 {
                    Some(right_preds.remove(0))
                } else {
                    Some(Predicate::And(right_preds))
                };

                let remain = if remaining.is_empty() {
                    None
                } else if remaining.len() == 1 {
                    Some(remaining.remove(0))
                } else {
                    Some(Predicate::And(remaining))
                };

                (left, right, remain)
            }
            p if p.references_only(left_cols) => (Some(p.clone()), None, None),
            p if p.references_only(right_cols) => (None, Some(p.clone()), None),
            p => (None, None, Some(p.clone())),
        }
    }

    /// Optimize join order for multi-table joins
    fn optimize_joins(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                // For two-table joins, decide order based on estimated sizes
                let left_cost = self.estimate_plan_cost(&left);
                let right_cost = self.estimate_plan_cost(&right);

                // Build on smaller table (for hash join)
                if right_cost.output_rows < left_cost.output_rows && join_type == JoinType::Inner {
                    // Swap for better hash join performance
                    LogicalPlan::Join {
                        left: Box::new(self.optimize_joins(*right)),
                        right: Box::new(self.optimize_joins(*left)),
                        join_type,
                        condition: self.swap_join_condition(condition),
                    }
                } else {
                    LogicalPlan::Join {
                        left: Box::new(self.optimize_joins(*left)),
                        right: Box::new(self.optimize_joins(*right)),
                        join_type,
                        condition,
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(self.optimize_joins(*input)),
                predicate,
            },
            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.optimize_joins(*input)),
                columns,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.optimize_joins(*input)),
                group_by,
                aggregates,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.optimize_joins(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.optimize_joins(*input)),
                limit,
                offset,
            },
            other => other,
        }
    }

    fn swap_join_condition(&self, condition: JoinCondition) -> JoinCondition {
        match condition {
            JoinCondition::On(pred) => JoinCondition::On(self.swap_predicate_columns(pred)),
            other => other,
        }
    }

    fn swap_predicate_columns(&self, pred: Predicate) -> Predicate {
        match pred {
            Predicate::ColumnCompare { left, op, right } => {
                let swapped_op = match op {
                    CompareOp::Lt => CompareOp::Gt,
                    CompareOp::Le => CompareOp::Ge,
                    CompareOp::Gt => CompareOp::Lt,
                    CompareOp::Ge => CompareOp::Le,
                    other => other,
                };
                Predicate::ColumnCompare {
                    left: right,
                    op: swapped_op,
                    right: left,
                }
            }
            Predicate::And(preds) => Predicate::And(
                preds
                    .into_iter()
                    .map(|p| self.swap_predicate_columns(p))
                    .collect(),
            ),
            other => other,
        }
    }

    /// Estimate cost of a logical plan
    fn estimate_plan_cost(&self, plan: &LogicalPlan) -> QueryCost {
        match plan {
            LogicalPlan::Scan {
                table,
                filter,
                columns,
                ..
            } => {
                let stats = self.table_stats.get(table);
                let base_rows = stats.map(|s| s.row_count).unwrap_or(1000) as f64;
                let base_bytes = stats.map(|s| s.size_bytes).unwrap_or(10000) as f64;

                let selectivity = filter
                    .as_ref()
                    .map(|f| {
                        let col_stats = stats.map(|s| &s.columns).cloned().unwrap_or_default();
                        self.selectivity.estimate(f, &col_stats)
                    })
                    .unwrap_or(1.0);

                let output_rows = base_rows * selectivity;
                let col_fraction = if columns.is_empty() {
                    1.0
                } else {
                    stats
                        .map(|s| columns.len() as f64 / s.columns.len().max(1) as f64)
                        .unwrap_or(1.0)
                };

                QueryCost {
                    cpu_cost: output_rows * self.cost_params.cpu_tuple_cost,
                    io_cost: (base_bytes / self.cost_params.page_size as f64).ceil(),
                    memory_cost: 0.0, // Streaming scan
                    network_cost: 0.0,
                    output_rows,
                    output_bytes: output_rows * col_fraction * 100.0, // Assume 100 bytes per row
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                let input_cost = self.estimate_plan_cost(input);
                let col_stats = HashMap::new(); // Would need to propagate stats
                let selectivity = self.selectivity.estimate(predicate, &col_stats);

                QueryCost {
                    cpu_cost: input_cost.cpu_cost
                        + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    io_cost: input_cost.io_cost,
                    memory_cost: input_cost.memory_cost,
                    network_cost: input_cost.network_cost,
                    output_rows: input_cost.output_rows * selectivity,
                    output_bytes: input_cost.output_bytes * selectivity,
                }
            }
            LogicalPlan::Join { left, right, .. } => {
                let left_cost = self.estimate_plan_cost(left);
                let right_cost = self.estimate_plan_cost(right);

                // Hash join cost
                let build_cost = right_cost.output_rows * self.cost_params.cpu_tuple_cost * 2.0;
                let probe_cost = left_cost.output_rows * self.cost_params.cpu_tuple_cost;
                let hash_table_mem = right_cost.output_bytes;

                // Assume 10% match rate for join
                let output_rows = (left_cost.output_rows * right_cost.output_rows * 0.1).max(1.0);

                QueryCost {
                    cpu_cost: left_cost.cpu_cost + right_cost.cpu_cost + build_cost + probe_cost,
                    io_cost: left_cost.io_cost + right_cost.io_cost,
                    memory_cost: hash_table_mem,
                    network_cost: left_cost.network_cost + right_cost.network_cost,
                    output_rows,
                    output_bytes: output_rows * 200.0, // Combined row size
                }
            }
            LogicalPlan::Aggregate {
                input, group_by, ..
            } => {
                let input_cost = self.estimate_plan_cost(input);

                // Estimate number of groups
                let num_groups = if group_by.is_empty() {
                    1.0
                } else {
                    // Assume each group by column reduces rows by 10x
                    (input_cost.output_rows / (10.0_f64.powi(group_by.len() as i32))).max(1.0)
                };

                QueryCost {
                    cpu_cost: input_cost.cpu_cost
                        + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    io_cost: input_cost.io_cost,
                    memory_cost: num_groups * 100.0, // Hash table for groups
                    network_cost: input_cost.network_cost,
                    output_rows: num_groups,
                    output_bytes: num_groups * 50.0 * (group_by.len() + 1) as f64,
                }
            }
            LogicalPlan::Sort { input, .. } => {
                let input_cost = self.estimate_plan_cost(input);
                let sort_cost = input_cost.output_rows
                    * (input_cost.output_rows.log2().max(1.0))
                    * self.cost_params.cpu_operator_cost;

                QueryCost {
                    cpu_cost: input_cost.cpu_cost + sort_cost,
                    io_cost: input_cost.io_cost,
                    memory_cost: input_cost.output_bytes, // Full materialization
                    network_cost: input_cost.network_cost,
                    output_rows: input_cost.output_rows,
                    output_bytes: input_cost.output_bytes,
                }
            }
            LogicalPlan::Limit { input, limit, .. } => {
                let input_cost = self.estimate_plan_cost(input);
                let output_rows = (input_cost.output_rows).min(*limit as f64);

                QueryCost {
                    cpu_cost: input_cost.cpu_cost, // Still process all input
                    io_cost: input_cost.io_cost,
                    memory_cost: 0.0, // Streaming
                    network_cost: input_cost.network_cost,
                    output_rows,
                    output_bytes: output_rows
                        * (input_cost.output_bytes / input_cost.output_rows.max(1.0)),
                }
            }
            LogicalPlan::Project { input, .. } => {
                let input_cost = self.estimate_plan_cost(input);
                QueryCost {
                    cpu_cost: input_cost.cpu_cost
                        + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    ..input_cost
                }
            }
            LogicalPlan::Union { inputs, .. } => inputs
                .iter()
                .map(|p| self.estimate_plan_cost(p))
                .fold(QueryCost::default(), |acc, c| QueryCost {
                    cpu_cost: acc.cpu_cost + c.cpu_cost,
                    io_cost: acc.io_cost + c.io_cost,
                    memory_cost: acc.memory_cost.max(c.memory_cost),
                    network_cost: acc.network_cost + c.network_cost,
                    output_rows: acc.output_rows + c.output_rows,
                    output_bytes: acc.output_bytes + c.output_bytes,
                }),
            LogicalPlan::Subquery { input, .. } => self.estimate_plan_cost(input),
        }
    }

    /// Convert logical plan to physical plan
    fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        match plan {
            LogicalPlan::Scan {
                table,
                database,
                columns,
                filter,
            } => {
                // Decide between seq scan and index scan
                let use_index = self.should_use_index(&table, &filter);
                let parallel = self.should_parallelize(&table, &filter);

                if let Some((index_name, range)) = use_index {
                    PhysicalPlan::IndexScan {
                        table,
                        database,
                        index: index_name,
                        columns,
                        filter,
                        range: Some(range),
                    }
                } else {
                    PhysicalPlan::SeqScan {
                        table,
                        database,
                        columns,
                        filter,
                        parallel_degree: if parallel { self.parallelism } else { 1 },
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(self.to_physical(*input)),
                predicate,
            },
            LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.to_physical(*input)),
                columns,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_physical = self.to_physical(*input);

                // Use sorted aggregate if input is already sorted by group keys
                let use_sorted = self.is_sorted_by(&input_physical, &group_by);

                if use_sorted {
                    PhysicalPlan::SortedAggregate {
                        input: Box::new(input_physical),
                        group_by,
                        aggregates,
                    }
                } else {
                    PhysicalPlan::HashAggregate {
                        input: Box::new(input_physical),
                        group_by,
                        aggregates,
                    }
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_physical = self.to_physical(*left);
                let right_physical = self.to_physical(*right);

                // Extract join keys if equi-join
                if let Some((left_keys, right_keys)) = self.extract_equi_join_keys(&condition) {
                    let left_cost = self.estimate_physical_cost(&left_physical);
                    let right_cost = self.estimate_physical_cost(&right_physical);

                    // Use hash join for equi-joins, smaller table as build side
                    if right_cost.output_rows <= left_cost.output_rows {
                        PhysicalPlan::HashJoin {
                            build: Box::new(right_physical),
                            probe: Box::new(left_physical),
                            join_type,
                            build_keys: right_keys,
                            probe_keys: left_keys,
                            parallel_build: right_cost.output_rows > 10000.0,
                        }
                    } else {
                        PhysicalPlan::HashJoin {
                            build: Box::new(left_physical),
                            probe: Box::new(right_physical),
                            join_type,
                            build_keys: left_keys,
                            probe_keys: right_keys,
                            parallel_build: left_cost.output_rows > 10000.0,
                        }
                    }
                } else {
                    // Nested loop for non-equi joins
                    PhysicalPlan::NestedLoopJoin {
                        outer: Box::new(left_physical),
                        inner: Box::new(right_physical),
                        join_type,
                        condition: match condition {
                            JoinCondition::On(p) => Some(p),
                            _ => None,
                        },
                    }
                }
            }
            LogicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
                input: Box::new(self.to_physical(*input)),
                order_by,
                limit: None,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_physical = self.to_physical(*input);

                // Check if we can use TopN optimization
                if let PhysicalPlan::Sort {
                    input: sort_input,
                    order_by,
                    ..
                } = input_physical
                {
                    if offset == 0 {
                        return PhysicalPlan::TopN {
                            input: sort_input,
                            order_by,
                            limit,
                        };
                    }
                    return PhysicalPlan::Limit {
                        input: Box::new(PhysicalPlan::Sort {
                            input: sort_input,
                            order_by,
                            limit: Some(limit + offset),
                        }),
                        limit,
                        offset,
                    };
                }

                PhysicalPlan::Limit {
                    input: Box::new(input_physical),
                    limit,
                    offset,
                }
            }
            LogicalPlan::Union { inputs, all } => {
                let physical_inputs: Vec<_> =
                    inputs.into_iter().map(|p| self.to_physical(p)).collect();

                if all {
                    PhysicalPlan::UnionAll {
                        inputs: physical_inputs,
                    }
                } else {
                    // UNION requires deduplication
                    PhysicalPlan::HashAggregate {
                        input: Box::new(PhysicalPlan::UnionAll {
                            inputs: physical_inputs,
                        }),
                        group_by: vec![], // All columns
                        aggregates: vec![],
                    }
                }
            }
            LogicalPlan::Subquery { input, .. } => self.to_physical(*input),
        }
    }

    fn should_use_index(
        &self,
        table: &str,
        filter: &Option<Predicate>,
    ) -> Option<(String, IndexRange)> {
        let filter = filter.as_ref()?;
        let indexes = self.indexes.get(table)?;

        for index in indexes {
            if let Some(range) = self.predicate_to_index_range(filter, &index.columns) {
                return Some((index.name.clone(), range));
            }
        }
        None
    }

    fn predicate_to_index_range(
        &self,
        pred: &Predicate,
        index_cols: &[String],
    ) -> Option<IndexRange> {
        if index_cols.is_empty() {
            return None;
        }
        let first_col = &index_cols[0];

        match pred {
            Predicate::Comparison { column, op, value } if column == first_col => Some(match op {
                CompareOp::Eq => IndexRange {
                    start: Some(value.clone()),
                    start_inclusive: true,
                    end: Some(value.clone()),
                    end_inclusive: true,
                },
                CompareOp::Lt => IndexRange {
                    start: None,
                    start_inclusive: true,
                    end: Some(value.clone()),
                    end_inclusive: false,
                },
                CompareOp::Le => IndexRange {
                    start: None,
                    start_inclusive: true,
                    end: Some(value.clone()),
                    end_inclusive: true,
                },
                CompareOp::Gt => IndexRange {
                    start: Some(value.clone()),
                    start_inclusive: false,
                    end: None,
                    end_inclusive: true,
                },
                CompareOp::Ge => IndexRange {
                    start: Some(value.clone()),
                    start_inclusive: true,
                    end: None,
                    end_inclusive: true,
                },
                CompareOp::Ne => return None,
            }),
            Predicate::Between {
                column,
                low,
                high,
                negated: false,
            } if column == first_col => Some(IndexRange {
                start: Some(low.clone()),
                start_inclusive: true,
                end: Some(high.clone()),
                end_inclusive: true,
            }),
            Predicate::And(preds) => {
                // Try to combine multiple predicates on index column
                let mut start = None;
                let mut start_inclusive = true;
                let mut end = None;
                let mut end_inclusive = true;

                for p in preds {
                    if let Some(range) = self.predicate_to_index_range(p, index_cols) {
                        if range.start.is_some() && start.is_none() {
                            start = range.start;
                            start_inclusive = range.start_inclusive;
                        }
                        if range.end.is_some() && end.is_none() {
                            end = range.end;
                            end_inclusive = range.end_inclusive;
                        }
                    }
                }

                if start.is_some() || end.is_some() {
                    Some(IndexRange {
                        start,
                        start_inclusive,
                        end,
                        end_inclusive,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn should_parallelize(&self, table: &str, _filter: &Option<Predicate>) -> bool {
        if let Some(stats) = self.table_stats.get(table) {
            stats.row_count > 10000 || stats.size_bytes > 10 * 1024 * 1024
        } else {
            true // Default to parallel for unknown tables
        }
    }

    fn is_sorted_by(&self, plan: &PhysicalPlan, columns: &[String]) -> bool {
        if columns.is_empty() {
            return true;
        }

        match plan {
            // Sort explicitly produces sorted output
            PhysicalPlan::Sort { order_by, .. } | PhysicalPlan::TopN { order_by, .. } => {
                // Check if the sort order matches the required columns
                if order_by.len() < columns.len() {
                    return false;
                }
                for (i, col) in columns.iter().enumerate() {
                    if &order_by[i].column != col {
                        return false;
                    }
                }
                true
            }

            // MergeJoin produces sorted output on join keys
            PhysicalPlan::MergeJoin { left_keys, .. } => {
                // Output is sorted on left keys
                if left_keys.len() < columns.len() {
                    return false;
                }
                columns.iter().zip(left_keys.iter()).all(|(c, k)| c == k)
            }

            // SortedAggregate produces output sorted on group_by columns
            PhysicalPlan::SortedAggregate { group_by, .. } => {
                if group_by.len() < columns.len() {
                    return false;
                }
                columns.iter().zip(group_by.iter()).all(|(c, g)| c == g)
            }

            // IndexScan may produce sorted output if using a B-tree index
            PhysicalPlan::IndexScan {
                index,
                columns: scan_cols,
                ..
            } => {
                // Check if index provides the required sort order
                // For simplicity, assume index name contains column name for sorted access
                if columns.len() == 1 && index.contains(&columns[0]) {
                    return true;
                }
                false
            }

            // These operators preserve input sort order
            PhysicalPlan::Filter { input, .. } => self.is_sorted_by(input, columns),
            PhysicalPlan::Project { input, .. } => {
                // Project preserves order if columns are still present
                self.is_sorted_by(input, columns)
            }
            PhysicalPlan::Limit { input, .. } => self.is_sorted_by(input, columns),

            // These operators don't preserve sort order
            PhysicalPlan::SeqScan { .. } => false,
            PhysicalPlan::HashAggregate { .. } => false,
            PhysicalPlan::HashJoin { .. } => false,
            PhysicalPlan::NestedLoopJoin { .. } => false,
            PhysicalPlan::Exchange { .. } => false, // Repartitioning destroys order
            PhysicalPlan::Gather { .. } => false,   // Gathering parallel streams destroys order
            PhysicalPlan::UnionAll { .. } => false, // Union doesn't preserve order
        }
    }

    fn extract_equi_join_keys(
        &self,
        condition: &JoinCondition,
    ) -> Option<(Vec<String>, Vec<String>)> {
        match condition {
            JoinCondition::On(pred) => self.extract_equi_keys_from_predicate(pred),
            JoinCondition::Using(cols) => Some((cols.clone(), cols.clone())),
            JoinCondition::Natural => None, // Would need schema info
            JoinCondition::None => None,
        }
    }

    fn extract_equi_keys_from_predicate(
        &self,
        pred: &Predicate,
    ) -> Option<(Vec<String>, Vec<String>)> {
        match pred {
            Predicate::ColumnCompare {
                left,
                op: CompareOp::Eq,
                right,
            } => Some((vec![left.clone()], vec![right.clone()])),
            Predicate::And(preds) => {
                let mut left_keys = vec![];
                let mut right_keys = vec![];
                for p in preds {
                    if let Some((l, r)) = self.extract_equi_keys_from_predicate(p) {
                        left_keys.extend(l);
                        right_keys.extend(r);
                    }
                }
                if left_keys.is_empty() {
                    None
                } else {
                    Some((left_keys, right_keys))
                }
            }
            _ => None,
        }
    }

    fn estimate_physical_cost(&self, plan: &PhysicalPlan) -> QueryCost {
        match plan {
            PhysicalPlan::SeqScan {
                table,
                parallel_degree,
                ..
            } => {
                let stats = self.table_stats.get(table);
                let rows = stats.map(|s| s.row_count).unwrap_or(1000) as f64;
                let bytes = stats.map(|s| s.size_bytes).unwrap_or(10000) as f64;
                let parallel = *parallel_degree as f64;

                QueryCost {
                    cpu_cost: rows * self.cost_params.cpu_tuple_cost / parallel,
                    io_cost: bytes / self.cost_params.page_size as f64 / parallel,
                    memory_cost: 0.0,
                    network_cost: 0.0,
                    output_rows: rows,
                    output_bytes: bytes,
                }
            }
            PhysicalPlan::IndexScan { table, .. } => {
                let stats = self.table_stats.get(table);
                let rows = stats.map(|s| s.row_count).unwrap_or(1000) as f64 * 0.1;

                QueryCost {
                    cpu_cost: rows * self.cost_params.cpu_tuple_cost,
                    io_cost: rows * self.cost_params.random_page_cost / 100.0,
                    memory_cost: 0.0,
                    network_cost: 0.0,
                    output_rows: rows,
                    output_bytes: rows * 100.0,
                }
            }
            PhysicalPlan::Filter { input, .. } => {
                let input_cost = self.estimate_physical_cost(input);
                QueryCost {
                    cpu_cost: input_cost.cpu_cost
                        + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    output_rows: input_cost.output_rows * 0.5, // Assume 50% selectivity
                    ..input_cost
                }
            }
            PhysicalPlan::HashJoin { build, probe, .. } => {
                let build_cost = self.estimate_physical_cost(build);
                let probe_cost = self.estimate_physical_cost(probe);

                QueryCost {
                    cpu_cost: build_cost.cpu_cost
                        + probe_cost.cpu_cost
                        + build_cost.output_rows * 2.0 * self.cost_params.cpu_tuple_cost
                        + probe_cost.output_rows * self.cost_params.cpu_tuple_cost,
                    io_cost: build_cost.io_cost + probe_cost.io_cost,
                    memory_cost: build_cost.output_bytes,
                    network_cost: 0.0,
                    output_rows: (build_cost.output_rows * probe_cost.output_rows * 0.1).max(1.0),
                    output_bytes: 0.0,
                }
            }
            _ => QueryCost::default(),
        }
    }
}

// ============================================================================
// Parallel Execution Planning
// ============================================================================

/// Planner for parallel query execution
pub struct ParallelPlanner {
    /// Number of worker threads
    pub workers: usize,
    /// Minimum rows per partition
    pub min_partition_rows: usize,
    /// Minimum bytes per partition
    pub min_partition_bytes: u64,
}

impl Default for ParallelPlanner {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4),
            min_partition_rows: 10000,
            min_partition_bytes: 10 * 1024 * 1024,
        }
    }
}

impl ParallelPlanner {
    pub fn new(workers: usize) -> Self {
        Self {
            workers,
            ..Default::default()
        }
    }

    /// Add parallelism to a physical plan
    pub fn parallelize(
        &self,
        plan: PhysicalPlan,
        stats: &HashMap<String, TableStats>,
    ) -> PhysicalPlan {
        match plan {
            PhysicalPlan::SeqScan {
                table,
                database,
                columns,
                filter,
                parallel_degree: _,
            } => {
                let degree = self.compute_parallelism(&table, stats);
                if degree > 1 {
                    PhysicalPlan::SeqScan {
                        table,
                        database,
                        columns,
                        filter,
                        parallel_degree: degree,
                    }
                } else {
                    PhysicalPlan::SeqScan {
                        table,
                        database,
                        columns,
                        filter,
                        parallel_degree: 1,
                    }
                }
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let parallel_input = self.parallelize(*input, stats);

                // For aggregation, we might need a two-phase approach
                if self.should_two_phase_aggregate(&parallel_input) {
                    // Partial aggregate in parallel, then final aggregate
                    PhysicalPlan::HashAggregate {
                        input: Box::new(PhysicalPlan::Gather {
                            inputs: vec![PhysicalPlan::HashAggregate {
                                input: Box::new(parallel_input),
                                group_by: group_by.clone(),
                                aggregates: aggregates.clone(),
                            }],
                        }),
                        group_by,
                        aggregates,
                    }
                } else {
                    PhysicalPlan::HashAggregate {
                        input: Box::new(parallel_input),
                        group_by,
                        aggregates,
                    }
                }
            }
            PhysicalPlan::HashJoin {
                build,
                probe,
                join_type,
                build_keys,
                probe_keys,
                ..
            } => {
                let parallel_build = self.parallelize(*build, stats);
                let parallel_probe = self.parallelize(*probe, stats);

                PhysicalPlan::HashJoin {
                    build: Box::new(parallel_build),
                    probe: Box::new(parallel_probe),
                    join_type,
                    build_keys,
                    probe_keys,
                    parallel_build: true,
                }
            }
            PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(self.parallelize(*input, stats)),
                predicate,
            },
            PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.parallelize(*input, stats)),
                columns,
            },
            PhysicalPlan::Sort {
                input,
                order_by,
                limit,
            } => {
                // Sort requires gathering all data
                PhysicalPlan::Sort {
                    input: Box::new(self.parallelize(*input, stats)),
                    order_by,
                    limit,
                }
            }
            other => other,
        }
    }

    fn compute_parallelism(&self, table: &str, stats: &HashMap<String, TableStats>) -> usize {
        if let Some(table_stats) = stats.get(table) {
            let by_rows = table_stats.row_count / self.min_partition_rows as u64;
            let by_bytes = table_stats.size_bytes / self.min_partition_bytes;
            let suggested = by_rows.max(by_bytes) as usize;
            suggested.min(self.workers).max(1)
        } else {
            self.workers
        }
    }

    fn should_two_phase_aggregate(&self, plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::SeqScan {
                parallel_degree, ..
            } => *parallel_degree > 1,
            PhysicalPlan::Filter { input, .. } => self.should_two_phase_aggregate(input),
            PhysicalPlan::Project { input, .. } => self.should_two_phase_aggregate(input),
            _ => false,
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
    fn test_predicate_simplify() {
        let pred = Predicate::And(vec![
            Predicate::True,
            Predicate::Comparison {
                column: "x".to_string(),
                op: CompareOp::Eq,
                value: StatValue::Int64(5),
            },
        ]);

        let simplified = pred.simplify();
        assert!(matches!(simplified, Predicate::Comparison { .. }));
    }

    #[test]
    fn test_predicate_false_short_circuit() {
        let pred = Predicate::And(vec![
            Predicate::False,
            Predicate::Comparison {
                column: "x".to_string(),
                op: CompareOp::Eq,
                value: StatValue::Int64(5),
            },
        ]);

        let simplified = pred.simplify();
        assert!(matches!(simplified, Predicate::False));
    }

    #[test]
    fn test_selectivity_estimation() {
        let estimator = SelectivityEstimator::default();
        let mut stats = HashMap::new();
        stats.insert(
            "id".to_string(),
            ColumnStatistics {
                distinct_count: 1000,
                null_count: 0,
                min_value: Some(StatValue::Int64(1)),
                max_value: Some(StatValue::Int64(1000)),
                avg_length: None,
                histogram: None,
                most_common_values: None,
            },
        );

        // Equality on high cardinality column
        let eq_pred = Predicate::Comparison {
            column: "id".to_string(),
            op: CompareOp::Eq,
            value: StatValue::Int64(500),
        };
        let sel = estimator.estimate(&eq_pred, &stats);
        assert!(sel < 0.01); // Should be ~0.001

        // Range predicate
        let range_pred = Predicate::Between {
            column: "id".to_string(),
            low: StatValue::Int64(100),
            high: StatValue::Int64(200),
            negated: false,
        };
        let sel = estimator.estimate(&range_pred, &stats);
        assert!(sel > 0.05 && sel < 0.15); // ~10%
    }

    #[test]
    fn test_cost_model() {
        let optimizer = CostBasedOptimizer::new(4);

        let scan = LogicalPlan::Scan {
            table: "users".to_string(),
            database: "test".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            filter: None,
        };

        let cost = optimizer.estimate_plan_cost(&scan);
        assert!(cost.total_cost() > 0.0);
    }

    #[test]
    fn test_predicate_pushdown() {
        let optimizer = CostBasedOptimizer::new(4);

        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".to_string(),
                database: "test".to_string(),
                columns: vec!["id".to_string()],
                filter: None,
            }),
            predicate: Predicate::Comparison {
                column: "id".to_string(),
                op: CompareOp::Gt,
                value: StatValue::Int64(100),
            },
        };

        let optimized = optimizer.push_down_predicates(plan);

        // Filter should be pushed into scan
        match optimized {
            LogicalPlan::Scan { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected filter to be pushed into scan"),
        }
    }

    #[test]
    fn test_join_optimization() {
        let mut optimizer = CostBasedOptimizer::new(4);

        // Add stats - small table and large table
        optimizer.update_stats(
            "small",
            TableStats {
                row_count: 100,
                size_bytes: 10000,
                ..Default::default()
            },
        );
        optimizer.update_stats(
            "large",
            TableStats {
                row_count: 1000000,
                size_bytes: 100000000,
                ..Default::default()
            },
        );

        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: "large".to_string(),
                database: "test".to_string(),
                columns: vec!["id".to_string()],
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table: "small".to_string(),
                database: "test".to_string(),
                columns: vec!["id".to_string()],
                filter: None,
            }),
            join_type: JoinType::Inner,
            condition: JoinCondition::On(Predicate::ColumnCompare {
                left: "large.id".to_string(),
                op: CompareOp::Eq,
                right: "small.id".to_string(),
            }),
        };

        let optimized = optimizer.optimize_joins(plan);

        // Should swap to build on smaller table
        match optimized {
            LogicalPlan::Join { left, .. } => match *left {
                LogicalPlan::Scan { table, .. } => {
                    assert_eq!(table, "small");
                }
                _ => panic!("Expected scan"),
            },
            _ => panic!("Expected join"),
        }
    }

    #[test]
    fn test_physical_plan_generation() {
        let mut optimizer = CostBasedOptimizer::new(4);
        optimizer.update_stats(
            "users",
            TableStats {
                row_count: 100000,
                size_bytes: 10000000,
                ..Default::default()
            },
        );

        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table: "users".to_string(),
                    database: "test".to_string(),
                    columns: vec!["id".to_string()],
                    filter: None,
                }),
                order_by: vec![SortExpr {
                    column: "id".to_string(),
                    descending: false,
                    nulls_first: false,
                }],
            }),
            limit: 10,
            offset: 0,
        };

        let physical = optimizer.optimize(logical);

        // Should use TopN optimization
        assert!(matches!(physical, PhysicalPlan::TopN { .. }));
    }

    #[test]
    fn test_parallel_planner() {
        let planner = ParallelPlanner::new(8);
        let mut stats = HashMap::new();
        stats.insert(
            "large_table".to_string(),
            TableStats {
                row_count: 1000000,
                size_bytes: 100 * 1024 * 1024,
                ..Default::default()
            },
        );

        let plan = PhysicalPlan::SeqScan {
            table: "large_table".to_string(),
            database: "test".to_string(),
            columns: vec!["id".to_string()],
            filter: None,
            parallel_degree: 1,
        };

        let parallel_plan = planner.parallelize(plan, &stats);

        match parallel_plan {
            PhysicalPlan::SeqScan {
                parallel_degree, ..
            } => {
                assert!(parallel_degree > 1);
            }
            _ => panic!("Expected seq scan"),
        }
    }

    #[test]
    fn test_index_range_extraction() {
        let optimizer = CostBasedOptimizer::new(4);

        let pred = Predicate::And(vec![
            Predicate::Comparison {
                column: "id".to_string(),
                op: CompareOp::Ge,
                value: StatValue::Int64(100),
            },
            Predicate::Comparison {
                column: "id".to_string(),
                op: CompareOp::Lt,
                value: StatValue::Int64(200),
            },
        ]);

        let range = optimizer.predicate_to_index_range(&pred, &["id".to_string()]);
        assert!(range.is_some());

        let range = range.unwrap();
        assert_eq!(range.start, Some(StatValue::Int64(100)));
        assert!(range.start_inclusive);
        assert_eq!(range.end, Some(StatValue::Int64(200)));
        assert!(!range.end_inclusive);
    }

    #[test]
    fn test_query_cost_total() {
        let cost = QueryCost {
            cpu_cost: 10.0,
            io_cost: 5.0,
            memory_cost: 1000.0,
            network_cost: 2.0,
            output_rows: 100.0,
            output_bytes: 10000.0,
        };

        let total = cost.total_cost();
        // cpu (10) + io*10 (50) + network*5 (10) + memory*0.1 (100) = 170
        assert!((total - 170.0).abs() < 0.01);
    }
}

// ============================================================================
// Index Advisor - Automatic Index Recommendations
// ============================================================================

use parking_lot::RwLock;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Types of index recommendations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecommendedIndexType {
    /// B-tree for range queries and sorting
    BTree,
    /// Hash for equality lookups
    Hash,
    /// Bloom filter for membership testing
    BloomFilter,
    /// Bitmap for low-cardinality columns
    Bitmap,
    /// Composite index on multiple columns
    Composite(Vec<String>),
}

impl std::fmt::Display for RecommendedIndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecommendedIndexType::BTree => write!(f, "BTREE"),
            RecommendedIndexType::Hash => write!(f, "HASH"),
            RecommendedIndexType::BloomFilter => write!(f, "BLOOM"),
            RecommendedIndexType::Bitmap => write!(f, "BITMAP"),
            RecommendedIndexType::Composite(cols) => write!(f, "COMPOSITE({})", cols.join(", ")),
        }
    }
}

/// An index recommendation
#[derive(Debug, Clone)]
pub struct IndexRecommendation {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Column(s) to index
    pub columns: Vec<String>,
    /// Recommended index type
    pub index_type: RecommendedIndexType,
    /// Reason for recommendation
    pub reason: String,
    /// Estimated improvement (percentage reduction in query time)
    pub estimated_improvement: f64,
    /// Number of queries that would benefit
    pub benefiting_queries: u64,
    /// Priority score (higher = more important)
    pub priority: f64,
    /// SQL to create the index
    pub create_sql: String,
}

/// Column access pattern tracking
#[derive(Debug, Clone, Default)]
pub struct ColumnAccessPattern {
    /// Number of equality filters (WHERE col = value)
    pub equality_count: u64,
    /// Number of range filters (WHERE col > value, col BETWEEN, etc.)
    pub range_count: u64,
    /// Number of IN clause usages
    pub in_clause_count: u64,
    /// Number of JOIN conditions
    pub join_count: u64,
    /// Number of ORDER BY usages
    pub order_by_count: u64,
    /// Number of GROUP BY usages
    pub group_by_count: u64,
    /// Total query count involving this column
    pub total_queries: u64,
    /// Average selectivity when this column is filtered
    pub avg_selectivity: f64,
    /// Columns frequently used together in filters
    pub co_occurring_columns: HashMap<String, u64>,
}

/// Table access pattern tracking
#[derive(Debug, Clone, Default)]
pub struct TableAccessPattern {
    /// Column-level patterns
    pub columns: HashMap<String, ColumnAccessPattern>,
    /// Total queries on this table
    pub total_queries: u64,
    /// Full table scans
    pub full_scans: u64,
    /// Index scans
    pub index_scans: u64,
    /// Average rows returned per query
    pub avg_rows_returned: f64,
    /// Total execution time (ms)
    pub total_execution_time_ms: u64,
}

/// Configuration for the index advisor
#[derive(Debug, Clone)]
pub struct IndexAdvisorConfig {
    /// Minimum queries to consider a pattern significant
    pub min_query_count: u64,
    /// Minimum improvement percentage to recommend
    pub min_improvement_pct: f64,
    /// Maximum recommendations per table
    pub max_recommendations_per_table: usize,
    /// Consider cardinality threshold for hash vs btree
    pub high_cardinality_threshold: f64,
    /// Low cardinality threshold for bitmap indexes
    pub low_cardinality_threshold: f64,
    /// Retention period for access patterns
    pub pattern_retention: Duration,
}

impl Default for IndexAdvisorConfig {
    fn default() -> Self {
        Self {
            min_query_count: 10,
            min_improvement_pct: 5.0,
            max_recommendations_per_table: 5,
            high_cardinality_threshold: 0.9, // >90% unique values
            low_cardinality_threshold: 0.01, // <1% unique values
            pattern_retention: Duration::from_secs(24 * 3600), // 24 hours
        }
    }
}

/// Index Advisor - analyzes query patterns and recommends indexes
pub struct IndexAdvisor {
    config: IndexAdvisorConfig,
    /// Access patterns per database.table
    patterns: RwLock<HashMap<String, TableAccessPattern>>,
    /// Existing indexes
    existing_indexes: RwLock<HashMap<String, Vec<ExistingIndex>>>,
    /// Table statistics
    table_stats: RwLock<HashMap<String, TableStats>>,
    /// Last cleanup time
    last_cleanup: RwLock<Instant>,
}

/// Information about an existing index
#[derive(Debug, Clone)]
pub struct ExistingIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub index_type: String,
}

impl IndexAdvisor {
    pub fn new(config: IndexAdvisorConfig) -> Self {
        Self {
            config,
            patterns: RwLock::new(HashMap::new()),
            existing_indexes: RwLock::new(HashMap::new()),
            table_stats: RwLock::new(HashMap::new()),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    /// Record a query's access patterns
    pub fn record_query(
        &self,
        database: &str,
        table: &str,
        filter_columns: &[(String, FilterType)],
        join_columns: &[String],
        order_by_columns: &[String],
        group_by_columns: &[String],
        rows_scanned: u64,
        rows_returned: u64,
        execution_time_ms: u64,
        used_index: bool,
    ) {
        let key = format!("{}.{}", database, table);
        let mut patterns = self.patterns.write();

        let pattern = patterns
            .entry(key)
            .or_insert_with(TableAccessPattern::default);
        pattern.total_queries += 1;
        pattern.total_execution_time_ms += execution_time_ms;

        if used_index {
            pattern.index_scans += 1;
        } else {
            pattern.full_scans += 1;
        }

        // Update average rows returned
        let n = pattern.total_queries as f64;
        pattern.avg_rows_returned =
            (pattern.avg_rows_returned * (n - 1.0) + rows_returned as f64) / n;

        // Track filter column patterns
        for (col, filter_type) in filter_columns {
            let col_pattern = pattern
                .columns
                .entry(col.clone())
                .or_insert_with(ColumnAccessPattern::default);
            col_pattern.total_queries += 1;

            match filter_type {
                FilterType::Equality => col_pattern.equality_count += 1,
                FilterType::Range => col_pattern.range_count += 1,
                FilterType::InClause => col_pattern.in_clause_count += 1,
                FilterType::Like => {} // Less indexable
            }

            // Update selectivity estimate
            if rows_scanned > 0 {
                let selectivity = rows_returned as f64 / rows_scanned as f64;
                let m = col_pattern.total_queries as f64;
                col_pattern.avg_selectivity =
                    (col_pattern.avg_selectivity * (m - 1.0) + selectivity) / m;
            }

            // Track co-occurring columns
            for (other_col, _) in filter_columns {
                if other_col != col {
                    *col_pattern
                        .co_occurring_columns
                        .entry(other_col.clone())
                        .or_insert(0) += 1;
                }
            }
        }

        // Track join columns
        for col in join_columns {
            let col_pattern = pattern
                .columns
                .entry(col.clone())
                .or_insert_with(ColumnAccessPattern::default);
            col_pattern.join_count += 1;
            col_pattern.total_queries += 1;
        }

        // Track ORDER BY columns
        for col in order_by_columns {
            let col_pattern = pattern
                .columns
                .entry(col.clone())
                .or_insert_with(ColumnAccessPattern::default);
            col_pattern.order_by_count += 1;
        }

        // Track GROUP BY columns
        for col in group_by_columns {
            let col_pattern = pattern
                .columns
                .entry(col.clone())
                .or_insert_with(ColumnAccessPattern::default);
            col_pattern.group_by_count += 1;
        }
    }

    /// Update table statistics
    pub fn update_stats(&self, database: &str, table: &str, stats: TableStats) {
        let key = format!("{}.{}", database, table);
        self.table_stats.write().insert(key, stats);
    }

    /// Register an existing index
    pub fn register_index(&self, database: &str, table: &str, index: ExistingIndex) {
        let key = format!("{}.{}", database, table);
        self.existing_indexes
            .write()
            .entry(key)
            .or_insert_with(Vec::new)
            .push(index);
    }

    /// Generate index recommendations
    pub fn recommend(&self) -> Vec<IndexRecommendation> {
        let patterns = self.patterns.read();
        let existing = self.existing_indexes.read();
        let stats = self.table_stats.read();

        let mut recommendations = Vec::new();

        for (table_key, pattern) in patterns.iter() {
            if pattern.total_queries < self.config.min_query_count {
                continue;
            }

            let parts: Vec<&str> = table_key.split('.').collect();
            if parts.len() != 2 {
                continue;
            }
            let (database, table) = (parts[0], parts[1]);

            let table_stats = stats.get(table_key);
            let table_indexes = existing.get(table_key);

            let mut table_recs =
                self.analyze_table(database, table, pattern, table_stats, table_indexes);

            // Sort by priority and take top N
            table_recs.sort_by(|a, b| b.priority.partial_cmp(&a.priority).unwrap());
            table_recs.truncate(self.config.max_recommendations_per_table);

            recommendations.extend(table_recs);
        }

        // Sort all recommendations by priority
        recommendations.sort_by(|a, b| b.priority.partial_cmp(&a.priority).unwrap());
        recommendations
    }

    fn analyze_table(
        &self,
        database: &str,
        table: &str,
        pattern: &TableAccessPattern,
        stats: Option<&TableStats>,
        existing: Option<&Vec<ExistingIndex>>,
    ) -> Vec<IndexRecommendation> {
        let mut recs = Vec::new();
        let existing_cols: HashSet<Vec<String>> = existing
            .map(|indexes| indexes.iter().map(|i| i.columns.clone()).collect())
            .unwrap_or_default();

        for (col, col_pattern) in &pattern.columns {
            if col_pattern.total_queries < self.config.min_query_count {
                continue;
            }

            // Skip if index already exists on this column
            if existing_cols.iter().any(|cols| cols.first() == Some(col)) {
                continue;
            }

            // Determine cardinality ratio if stats available
            let cardinality_ratio = stats
                .and_then(|s| s.columns.get(col))
                .map(|c| {
                    if stats.unwrap().row_count > 0 {
                        c.distinct_count as f64 / stats.unwrap().row_count as f64
                    } else {
                        0.5 // Default assumption
                    }
                })
                .unwrap_or(0.5);

            // Analyze and generate recommendations
            if let Some(rec) = self.recommend_for_column(
                database,
                table,
                col,
                col_pattern,
                cardinality_ratio,
                pattern,
            ) {
                recs.push(rec);
            }
        }

        // Check for composite index opportunities
        if let Some(composite_rec) =
            self.recommend_composite_index(database, table, pattern, &existing_cols)
        {
            recs.push(composite_rec);
        }

        recs
    }

    fn recommend_for_column(
        &self,
        database: &str,
        table: &str,
        column: &str,
        pattern: &ColumnAccessPattern,
        cardinality_ratio: f64,
        table_pattern: &TableAccessPattern,
    ) -> Option<IndexRecommendation> {
        let total = pattern.equality_count
            + pattern.range_count
            + pattern.in_clause_count
            + pattern.join_count;

        if total == 0 {
            return None;
        }

        // Determine best index type
        let (index_type, reason) = if cardinality_ratio < self.config.low_cardinality_threshold {
            // Low cardinality -> bitmap
            (
                RecommendedIndexType::Bitmap,
                format!(
                    "Low cardinality column ({:.1}% unique) with {} filter operations",
                    cardinality_ratio * 100.0,
                    total
                ),
            )
        } else if pattern.equality_count > pattern.range_count * 2
            && cardinality_ratio > self.config.high_cardinality_threshold
        {
            // High cardinality with mostly equality -> hash
            (
                RecommendedIndexType::Hash,
                format!(
                    "High cardinality ({:.1}% unique) with {} equality lookups",
                    cardinality_ratio * 100.0,
                    pattern.equality_count
                ),
            )
        } else if pattern.in_clause_count > pattern.equality_count {
            // Many IN clauses -> bloom filter
            (
                RecommendedIndexType::BloomFilter,
                format!(
                    "{} IN clause operations benefit from bloom filter",
                    pattern.in_clause_count
                ),
            )
        } else {
            // Default to B-tree (works for range and equality)
            (
                RecommendedIndexType::BTree,
                format!(
                    "{} equality + {} range queries, {} ORDER BY uses",
                    pattern.equality_count, pattern.range_count, pattern.order_by_count
                ),
            )
        };

        // Estimate improvement based on full scan ratio
        let full_scan_ratio =
            table_pattern.full_scans as f64 / table_pattern.total_queries.max(1) as f64;
        let column_query_ratio =
            pattern.total_queries as f64 / table_pattern.total_queries.max(1) as f64;

        // Estimated improvement: assumes index can eliminate ~90% of scan time
        // for queries on this column
        let estimated_improvement =
            full_scan_ratio * column_query_ratio * 90.0 * (1.0 - pattern.avg_selectivity.min(1.0));

        if estimated_improvement < self.config.min_improvement_pct {
            return None;
        }

        // Calculate priority score
        let priority = (pattern.total_queries as f64).log2() * estimated_improvement;

        let index_name = format!("idx_{}_{}", table, column);
        let create_sql = format!(
            "CREATE INDEX {} ON {}.{} ({}) USING {};",
            index_name, database, table, column, index_type
        );

        Some(IndexRecommendation {
            database: database.to_string(),
            table: table.to_string(),
            columns: vec![column.to_string()],
            index_type,
            reason,
            estimated_improvement,
            benefiting_queries: pattern.total_queries,
            priority,
            create_sql,
        })
    }

    fn recommend_composite_index(
        &self,
        database: &str,
        table: &str,
        pattern: &TableAccessPattern,
        existing: &HashSet<Vec<String>>,
    ) -> Option<IndexRecommendation> {
        // Find columns that frequently appear together
        let mut co_occurrence_scores: HashMap<(String, String), u64> = HashMap::new();

        for (col, col_pattern) in &pattern.columns {
            for (other_col, count) in &col_pattern.co_occurring_columns {
                if col < other_col {
                    *co_occurrence_scores
                        .entry((col.clone(), other_col.clone()))
                        .or_insert(0) += count;
                }
            }
        }

        // Find best pair
        let best_pair = co_occurrence_scores
            .iter()
            .filter(|(_, count)| **count >= self.config.min_query_count)
            .max_by_key(|(_, count)| *count)?;

        let (col1, col2) = &best_pair.0;
        let count = *best_pair.1;

        // Check if composite index already exists
        let cols = vec![col1.clone(), col2.clone()];
        if existing.contains(&cols) {
            return None;
        }

        // Determine column order based on selectivity
        let col1_pattern = pattern.columns.get(col1)?;
        let col2_pattern = pattern.columns.get(col2)?;

        let (first, second) = if col1_pattern.avg_selectivity < col2_pattern.avg_selectivity {
            (col1.clone(), col2.clone())
        } else {
            (col2.clone(), col1.clone())
        };

        let estimated_improvement = 15.0; // Conservative estimate for composite
        let priority = (count as f64).log2() * estimated_improvement;

        let index_name = format!("idx_{}_{}_{}", table, first, second);
        let create_sql = format!(
            "CREATE INDEX {} ON {}.{} ({}, {}) USING BTREE;",
            index_name, database, table, first, second
        );

        Some(IndexRecommendation {
            database: database.to_string(),
            table: table.to_string(),
            columns: vec![first.clone(), second.clone()],
            index_type: RecommendedIndexType::Composite(vec![first, second]),
            reason: format!("{} queries filter on both columns together", count),
            estimated_improvement,
            benefiting_queries: count,
            priority,
            create_sql,
        })
    }

    /// Get summary statistics
    pub fn stats(&self) -> IndexAdvisorStats {
        let patterns = self.patterns.read();

        let mut total_tables = 0;
        let mut total_queries = 0;
        let mut total_full_scans = 0;

        for pattern in patterns.values() {
            total_tables += 1;
            total_queries += pattern.total_queries;
            total_full_scans += pattern.full_scans;
        }

        IndexAdvisorStats {
            tracked_tables: total_tables,
            total_queries,
            full_scan_queries: total_full_scans,
            full_scan_ratio: if total_queries > 0 {
                total_full_scans as f64 / total_queries as f64
            } else {
                0.0
            },
        }
    }

    /// Clear old patterns
    pub fn cleanup(&self) {
        let mut last = self.last_cleanup.write();
        if last.elapsed() < Duration::from_secs(3600) {
            return; // Only cleanup once per hour
        }
        *last = Instant::now();

        // For now, just clear patterns older than retention
        // In production, you'd track timestamps per pattern
        let mut patterns = self.patterns.write();
        patterns.retain(|_, p| p.total_queries > self.config.min_query_count);
    }
}

/// Filter type for tracking
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterType {
    Equality,
    Range,
    InClause,
    Like,
}

/// Index advisor statistics
#[derive(Debug, Clone)]
pub struct IndexAdvisorStats {
    pub tracked_tables: usize,
    pub total_queries: u64,
    pub full_scan_queries: u64,
    pub full_scan_ratio: f64,
}

// ============================================================================
// Query Store - Historical Query Performance Analysis
// ============================================================================

use std::collections::VecDeque;

/// Query fingerprint for grouping similar queries
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct QueryFingerprint {
    /// Normalized SQL (parameters replaced with ?)
    pub normalized_sql: String,
    /// Database
    pub database: String,
}

/// Execution statistics for a query
#[derive(Debug, Clone, Default)]
pub struct QueryExecutionStats {
    /// Total executions
    pub execution_count: u64,
    /// Total execution time (microseconds)
    pub total_time_us: u64,
    /// Min execution time
    pub min_time_us: u64,
    /// Max execution time
    pub max_time_us: u64,
    /// Total rows returned
    pub total_rows: u64,
    /// Total rows scanned
    pub total_rows_scanned: u64,
    /// Number of times index was used
    pub index_usage_count: u64,
    /// Number of full scans
    pub full_scan_count: u64,
    /// Memory used (bytes)
    pub total_memory_bytes: u64,
    /// Last execution timestamp
    pub last_execution: u64,
    /// First seen timestamp
    pub first_seen: u64,
}

impl QueryExecutionStats {
    pub fn avg_time_us(&self) -> f64 {
        if self.execution_count > 0 {
            self.total_time_us as f64 / self.execution_count as f64
        } else {
            0.0
        }
    }

    pub fn avg_rows(&self) -> f64 {
        if self.execution_count > 0 {
            self.total_rows as f64 / self.execution_count as f64
        } else {
            0.0
        }
    }
}

/// Query plan snapshot
#[derive(Debug, Clone)]
pub struct QueryPlanSnapshot {
    /// Plan hash
    pub plan_hash: u64,
    /// Plan text (simplified)
    pub plan_text: String,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Actual cost (if executed)
    pub actual_cost: Option<f64>,
    /// When this plan was first seen
    pub first_seen: u64,
    /// When this plan was last seen
    pub last_seen: u64,
    /// Execution count with this plan
    pub execution_count: u64,
}

/// Stored query with all its metadata
#[derive(Debug, Clone)]
pub struct StoredQuery {
    pub fingerprint: QueryFingerprint,
    pub stats: QueryExecutionStats,
    pub plans: Vec<QueryPlanSnapshot>,
    /// Recent execution times for trend analysis
    pub recent_times: VecDeque<(u64, u64)>, // (timestamp, time_us)
}

impl StoredQuery {
    pub fn new(fingerprint: QueryFingerprint, timestamp: u64) -> Self {
        Self {
            fingerprint,
            stats: QueryExecutionStats {
                first_seen: timestamp,
                last_execution: timestamp,
                min_time_us: u64::MAX,
                ..Default::default()
            },
            plans: Vec::new(),
            recent_times: VecDeque::with_capacity(100),
        }
    }

    pub fn record_execution(
        &mut self,
        time_us: u64,
        rows: u64,
        rows_scanned: u64,
        used_index: bool,
        memory_bytes: u64,
        timestamp: u64,
    ) {
        self.stats.execution_count += 1;
        self.stats.total_time_us += time_us;
        self.stats.min_time_us = self.stats.min_time_us.min(time_us);
        self.stats.max_time_us = self.stats.max_time_us.max(time_us);
        self.stats.total_rows += rows;
        self.stats.total_rows_scanned += rows_scanned;
        self.stats.total_memory_bytes += memory_bytes;
        self.stats.last_execution = timestamp;

        if used_index {
            self.stats.index_usage_count += 1;
        } else {
            self.stats.full_scan_count += 1;
        }

        // Keep recent times for trend analysis
        self.recent_times.push_back((timestamp, time_us));
        if self.recent_times.len() > 100 {
            self.recent_times.pop_front();
        }
    }

    /// Detect if query performance is regressing
    pub fn detect_regression(&self, threshold_pct: f64) -> Option<f64> {
        if self.recent_times.len() < 20 {
            return None;
        }

        let mid = self.recent_times.len() / 2;
        let first_half: f64 = self
            .recent_times
            .iter()
            .take(mid)
            .map(|(_, t)| *t as f64)
            .sum::<f64>()
            / mid as f64;
        let second_half: f64 = self
            .recent_times
            .iter()
            .skip(mid)
            .map(|(_, t)| *t as f64)
            .sum::<f64>()
            / (self.recent_times.len() - mid) as f64;

        if first_half > 0.0 {
            let change_pct = ((second_half - first_half) / first_half) * 100.0;
            if change_pct > threshold_pct {
                return Some(change_pct);
            }
        }
        None
    }
}

/// Query Store configuration
#[derive(Debug, Clone)]
pub struct QueryStoreConfig {
    /// Maximum queries to store
    pub max_queries: usize,
    /// Maximum plans per query
    pub max_plans_per_query: usize,
    /// Retention period in seconds
    pub retention_secs: u64,
    /// Capture threshold (minimum time to capture)
    pub capture_threshold_us: u64,
    /// Regression detection threshold (percentage)
    pub regression_threshold_pct: f64,
}

impl Default for QueryStoreConfig {
    fn default() -> Self {
        Self {
            max_queries: 10_000,
            max_plans_per_query: 10,
            retention_secs: 7 * 24 * 3600,  // 7 days
            capture_threshold_us: 1000,     // 1ms
            regression_threshold_pct: 50.0, // 50% slower
        }
    }
}

/// Query Store - stores historical query performance data
pub struct QueryStore {
    config: QueryStoreConfig,
    queries: RwLock<HashMap<QueryFingerprint, StoredQuery>>,
    /// Top queries by total time
    top_by_time: RwLock<Vec<QueryFingerprint>>,
    /// Queries with detected regressions
    regressions: RwLock<Vec<(QueryFingerprint, f64)>>,
}

impl QueryStore {
    pub fn new(config: QueryStoreConfig) -> Self {
        Self {
            config,
            queries: RwLock::new(HashMap::new()),
            top_by_time: RwLock::new(Vec::new()),
            regressions: RwLock::new(Vec::new()),
        }
    }

    /// Normalize SQL for fingerprinting (replace literals with ?)
    pub fn normalize_sql(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut in_string = false;
        let mut in_number = false;
        let mut string_char = ' ';

        for c in sql.chars() {
            if in_string {
                if c == string_char {
                    in_string = false;
                    result.push('?');
                }
                continue;
            }

            if c == '\'' || c == '"' {
                in_string = true;
                string_char = c;
                continue;
            }

            if c.is_ascii_digit() || (c == '.' && in_number) {
                if !in_number {
                    in_number = true;
                    result.push('?');
                }
                continue;
            } else {
                in_number = false;
            }

            result.push(c);
        }

        // Normalize whitespace
        result
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase()
    }

    /// Record a query execution
    pub fn record(
        &self,
        sql: &str,
        database: &str,
        time_us: u64,
        rows: u64,
        rows_scanned: u64,
        used_index: bool,
        memory_bytes: u64,
        plan_hash: Option<u64>,
        plan_text: Option<&str>,
    ) {
        if time_us < self.config.capture_threshold_us {
            return;
        }

        let fingerprint = QueryFingerprint {
            normalized_sql: Self::normalize_sql(sql),
            database: database.to_string(),
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut queries = self.queries.write();

        // Enforce size limit
        if queries.len() >= self.config.max_queries && !queries.contains_key(&fingerprint) {
            // Remove oldest query
            if let Some(oldest) = queries
                .values()
                .min_by_key(|q| q.stats.last_execution)
                .map(|q| q.fingerprint.clone())
            {
                queries.remove(&oldest);
            }
        }

        let query = queries
            .entry(fingerprint.clone())
            .or_insert_with(|| StoredQuery::new(fingerprint.clone(), now));

        query.record_execution(time_us, rows, rows_scanned, used_index, memory_bytes, now);

        // Record plan if provided
        if let (Some(hash), Some(text)) = (plan_hash, plan_text) {
            if let Some(plan) = query.plans.iter_mut().find(|p| p.plan_hash == hash) {
                plan.last_seen = now;
                plan.execution_count += 1;
            } else if query.plans.len() < self.config.max_plans_per_query {
                query.plans.push(QueryPlanSnapshot {
                    plan_hash: hash,
                    plan_text: text.to_string(),
                    estimated_cost: 0.0,
                    actual_cost: Some(time_us as f64),
                    first_seen: now,
                    last_seen: now,
                    execution_count: 1,
                });
            }
        }
    }

    /// Get top queries by total time
    pub fn top_by_total_time(&self, limit: usize) -> Vec<StoredQuery> {
        let queries = self.queries.read();
        let mut sorted: Vec<_> = queries.values().cloned().collect();
        sorted.sort_by(|a, b| b.stats.total_time_us.cmp(&a.stats.total_time_us));
        sorted.truncate(limit);
        sorted
    }

    /// Get top queries by average time
    pub fn top_by_avg_time(&self, limit: usize) -> Vec<StoredQuery> {
        let queries = self.queries.read();
        let mut sorted: Vec<_> = queries.values().cloned().collect();
        sorted.sort_by(|a, b| {
            b.stats
                .avg_time_us()
                .partial_cmp(&a.stats.avg_time_us())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.truncate(limit);
        sorted
    }

    /// Get queries with detected regressions
    pub fn detect_regressions(&self) -> Vec<(StoredQuery, f64)> {
        let queries = self.queries.read();
        queries
            .values()
            .filter_map(|q| {
                q.detect_regression(self.config.regression_threshold_pct)
                    .map(|pct| (q.clone(), pct))
            })
            .collect()
    }

    /// Get query by fingerprint
    pub fn get(&self, fingerprint: &QueryFingerprint) -> Option<StoredQuery> {
        self.queries.read().get(fingerprint).cloned()
    }

    /// Get summary statistics
    pub fn summary(&self) -> QueryStoreSummary {
        let queries = self.queries.read();

        let total_queries = queries.len();
        let total_executions: u64 = queries.values().map(|q| q.stats.execution_count).sum();
        let total_time_us: u64 = queries.values().map(|q| q.stats.total_time_us).sum();

        QueryStoreSummary {
            total_queries,
            total_executions,
            total_time_us,
            avg_time_us: if total_executions > 0 {
                total_time_us as f64 / total_executions as f64
            } else {
                0.0
            },
        }
    }

    /// Cleanup old entries
    pub fn cleanup(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let cutoff = now.saturating_sub(self.config.retention_secs);

        let mut queries = self.queries.write();
        queries.retain(|_, q| q.stats.last_execution >= cutoff);
    }
}

/// Query store summary
#[derive(Debug, Clone)]
pub struct QueryStoreSummary {
    pub total_queries: usize,
    pub total_executions: u64,
    pub total_time_us: u64,
    pub avg_time_us: f64,
}

// ============================================================================
// Query Complexity Scoring
// ============================================================================

/// Query complexity factors
#[derive(Debug, Clone, Default)]
pub struct QueryComplexity {
    /// Number of tables in the query
    pub table_count: u32,
    /// Number of joins
    pub join_count: u32,
    /// Number of subqueries
    pub subquery_count: u32,
    /// Number of aggregations
    pub aggregation_count: u32,
    /// Has DISTINCT
    pub has_distinct: bool,
    /// Has ORDER BY
    pub has_order_by: bool,
    /// Has GROUP BY
    pub has_group_by: bool,
    /// Has window functions
    pub has_window_functions: bool,
    /// Estimated rows to process
    pub estimated_rows: u64,
    /// Number of columns selected
    pub column_count: u32,
    /// Has UNION/INTERSECT/EXCEPT
    pub has_set_operations: bool,
    /// Has CTEs (WITH clause)
    pub has_ctes: bool,
}

impl QueryComplexity {
    /// Calculate complexity score (0-100)
    pub fn score(&self) -> f64 {
        let mut score = 0.0;

        // Base score from structure
        score += (self.table_count as f64 - 1.0).max(0.0) * 5.0; // Each join adds 5
        score += self.join_count as f64 * 10.0;
        score += self.subquery_count as f64 * 15.0;
        score += self.aggregation_count as f64 * 5.0;

        // Modifiers
        if self.has_distinct {
            score += 10.0;
        }
        if self.has_order_by {
            score += 5.0;
        }
        if self.has_group_by {
            score += 10.0;
        }
        if self.has_window_functions {
            score += 20.0;
        }
        if self.has_set_operations {
            score += 15.0;
        }
        if self.has_ctes {
            score += 10.0;
        }

        // Data volume factor
        let row_factor = (self.estimated_rows as f64).log10().max(0.0) * 5.0;
        score += row_factor;

        score.min(100.0)
    }

    /// Estimate resource cost
    pub fn estimated_cost(&self) -> QueryResourceCost {
        let score = self.score();

        QueryResourceCost {
            cpu_weight: 1.0 + score / 20.0,
            memory_weight: 1.0
                + (self.has_group_by as u32 + self.has_distinct as u32 + self.join_count) as f64
                    * 0.5,
            io_weight: 1.0 + (self.estimated_rows as f64).log10().max(0.0) * 0.3,
            overall_weight: 1.0 + score / 25.0,
        }
    }
}

/// Resource cost weights for a query
#[derive(Debug, Clone)]
pub struct QueryResourceCost {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub io_weight: f64,
    pub overall_weight: f64,
}

/// Complexity-based query admission controller
pub struct ComplexityBasedAdmission {
    /// Maximum total complexity score of concurrent queries
    pub max_concurrent_complexity: f64,
    /// Current complexity
    current_complexity: std::sync::atomic::AtomicU64,
    /// Queries waiting
    waiting_queries: std::sync::atomic::AtomicU64,
}

impl ComplexityBasedAdmission {
    pub fn new(max_concurrent_complexity: f64) -> Self {
        Self {
            max_concurrent_complexity,
            current_complexity: std::sync::atomic::AtomicU64::new(0),
            waiting_queries: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Try to admit a query
    pub fn try_admit(&self, complexity: &QueryComplexity) -> bool {
        let score = complexity.score();
        let score_bits = (score * 100.0) as u64; // Store as fixed point

        let current = self
            .current_complexity
            .load(std::sync::atomic::Ordering::SeqCst);
        let current_score = current as f64 / 100.0;

        if current_score + score <= self.max_concurrent_complexity {
            self.current_complexity
                .fetch_add(score_bits, std::sync::atomic::Ordering::SeqCst);
            true
        } else {
            self.waiting_queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }
    }

    /// Release admission for a completed query
    pub fn release(&self, complexity: &QueryComplexity) {
        let score_bits = (complexity.score() * 100.0) as u64;
        self.current_complexity
            .fetch_sub(score_bits, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get current stats
    pub fn stats(&self) -> (f64, u64) {
        let current = self
            .current_complexity
            .load(std::sync::atomic::Ordering::SeqCst) as f64
            / 100.0;
        let waiting = self
            .waiting_queries
            .load(std::sync::atomic::Ordering::SeqCst);
        (current, waiting)
    }
}

// ============================================================================
// Cost Model Tuning
// ============================================================================

/// Cost model tuner with runtime adjustment
pub struct CostModelTuner {
    params: RwLock<CostModelParams>,
    /// Observed actual vs estimated ratios for calibration
    calibration_data: RwLock<VecDeque<(f64, f64)>>, // (estimated, actual)
}

impl CostModelTuner {
    pub fn new(params: CostModelParams) -> Self {
        Self {
            params: RwLock::new(params),
            calibration_data: RwLock::new(VecDeque::with_capacity(1000)),
        }
    }

    /// Get current parameters
    pub fn params(&self) -> CostModelParams {
        self.params.read().clone()
    }

    /// Update a specific parameter
    pub fn set_param(&self, name: &str, value: f64) -> Result<(), String> {
        let mut params = self.params.write();
        match name {
            "seq_page_cost" => params.seq_page_cost = value,
            "random_page_cost" => params.random_page_cost = value,
            "cpu_tuple_cost" => params.cpu_tuple_cost = value,
            "cpu_operator_cost" => params.cpu_operator_cost = value,
            "cpu_index_tuple_cost" => params.cpu_index_tuple_cost = value,
            "effective_cache_size" => params.effective_cache_size = value as u64,
            "work_mem" => params.work_mem = value as u64,
            "network_latency_factor" => params.network_latency_factor = value,
            "network_bandwidth_factor" => params.network_bandwidth_factor = value,
            "parallel_setup_cost" => params.parallel_setup_cost = value,
            "parallel_tuple_cost" => params.parallel_tuple_cost = value,
            _ => return Err(format!("Unknown parameter: {}", name)),
        }
        Ok(())
    }

    /// Record calibration data point
    pub fn record_calibration(&self, estimated: f64, actual: f64) {
        let mut data = self.calibration_data.write();
        data.push_back((estimated, actual));
        if data.len() > 1000 {
            data.pop_front();
        }
    }

    /// Get calibration statistics
    pub fn calibration_stats(&self) -> CostCalibrationStats {
        let data = self.calibration_data.read();

        if data.is_empty() {
            return CostCalibrationStats::default();
        }

        let mut total_ratio = 0.0;
        let mut underestimates = 0u64;
        let mut overestimates = 0u64;
        let mut accurate = 0u64;

        for (estimated, actual) in data.iter() {
            if *estimated > 0.0 {
                let ratio = actual / estimated;
                total_ratio += ratio;

                if ratio > 1.5 {
                    underestimates += 1;
                } else if ratio < 0.67 {
                    overestimates += 1;
                } else {
                    accurate += 1;
                }
            }
        }

        CostCalibrationStats {
            samples: data.len() as u64,
            avg_ratio: total_ratio / data.len() as f64,
            underestimate_count: underestimates,
            overestimate_count: overestimates,
            accurate_count: accurate,
            accuracy_pct: accurate as f64 / data.len() as f64 * 100.0,
        }
    }

    /// Auto-tune based on calibration data
    pub fn auto_tune(&self) {
        let stats = self.calibration_stats();

        if stats.samples < 100 {
            return; // Not enough data
        }

        let mut params = self.params.write();

        // If consistently underestimating, increase costs
        if stats.avg_ratio > 1.3 {
            params.cpu_tuple_cost *= 1.1;
            params.seq_page_cost *= 1.1;
        } else if stats.avg_ratio < 0.7 {
            params.cpu_tuple_cost *= 0.9;
            params.seq_page_cost *= 0.9;
        }
    }
}

/// Cost calibration statistics
#[derive(Debug, Clone, Default)]
pub struct CostCalibrationStats {
    pub samples: u64,
    pub avg_ratio: f64,
    pub underestimate_count: u64,
    pub overestimate_count: u64,
    pub accurate_count: u64,
    pub accuracy_pct: f64,
}

// ============================================================================
// Adaptive Query Execution
// ============================================================================

/// Adaptive execution decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecision {
    /// Continue with current plan
    Continue,
    /// Switch join strategy
    SwitchJoinStrategy(JoinStrategy),
    /// Increase parallelism
    IncreaseParallelism(u32),
    /// Decrease parallelism (memory pressure)
    DecreaseParallelism(u32),
    /// Enable spill to disk
    EnableSpill,
    /// Switch to hash aggregation
    SwitchToHashAggregation,
    /// Switch to sort aggregation
    SwitchToSortAggregation,
    /// Abort query (too expensive)
    Abort(String),
}

/// Join strategy options
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    NestedLoop,
    HashJoin,
    SortMergeJoin,
    BroadcastHashJoin,
}

/// Runtime statistics collected during execution
#[derive(Debug, Clone, Default)]
pub struct RuntimeStats {
    /// Actual rows processed so far
    pub rows_processed: u64,
    /// Estimated rows (from planner)
    pub estimated_rows: u64,
    /// Memory used (bytes)
    pub memory_used: u64,
    /// Memory limit (bytes)
    pub memory_limit: u64,
    /// Elapsed time (microseconds)
    pub elapsed_us: u64,
    /// Number of partitions/parallel tasks
    pub parallelism: u32,
    /// Build side size for hash join (rows)
    pub build_side_rows: u64,
    /// Probe side rows seen
    pub probe_side_rows: u64,
    /// Skew detected (ratio of max to avg partition size)
    pub skew_ratio: f64,
    /// Spill occurred
    pub spilled: bool,
}

impl RuntimeStats {
    /// Calculate cardinality estimation error ratio
    pub fn estimation_error(&self) -> f64 {
        if self.estimated_rows > 0 {
            self.rows_processed as f64 / self.estimated_rows as f64
        } else {
            1.0
        }
    }

    /// Check if memory pressure is high
    pub fn memory_pressure(&self) -> f64 {
        if self.memory_limit > 0 {
            self.memory_used as f64 / self.memory_limit as f64
        } else {
            0.0
        }
    }

    /// Check for data skew
    pub fn has_significant_skew(&self) -> bool {
        self.skew_ratio > 3.0
    }
}

/// Adaptive execution checkpoint
#[derive(Debug, Clone)]
pub struct AdaptiveCheckpoint {
    pub operator_id: String,
    pub operator_type: String,
    pub stats: RuntimeStats,
    pub timestamp: u64,
}

/// Configuration for adaptive execution
#[derive(Debug, Clone)]
pub struct AdaptiveExecutionConfig {
    pub enabled: bool,
    pub check_interval_rows: u64,
    pub cardinality_error_threshold: f64,
    pub memory_pressure_threshold: f64,
    pub skew_threshold: f64,
    pub min_rows_for_adaptation: u64,
    pub max_adaptations: u32,
}

impl Default for AdaptiveExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_rows: 10_000,
            cardinality_error_threshold: 10.0,
            memory_pressure_threshold: 0.8,
            skew_threshold: 3.0,
            min_rows_for_adaptation: 1000,
            max_adaptations: 3,
        }
    }
}

/// Adaptive Query Executor
pub struct AdaptiveExecutor {
    config: AdaptiveExecutionConfig,
    checkpoints: RwLock<Vec<AdaptiveCheckpoint>>,
    decisions: RwLock<Vec<(AdaptiveCheckpoint, AdaptiveDecision)>>,
    adaptation_count: std::sync::atomic::AtomicU32,
}

impl AdaptiveExecutor {
    pub fn new(config: AdaptiveExecutionConfig) -> Self {
        Self {
            config,
            checkpoints: RwLock::new(Vec::new()),
            decisions: RwLock::new(Vec::new()),
            adaptation_count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub fn checkpoint(&self, checkpoint: AdaptiveCheckpoint) -> AdaptiveDecision {
        if !self.config.enabled {
            return AdaptiveDecision::Continue;
        }

        let count = self
            .adaptation_count
            .load(std::sync::atomic::Ordering::SeqCst);
        if count >= self.config.max_adaptations {
            self.checkpoints.write().push(checkpoint);
            return AdaptiveDecision::Continue;
        }

        if checkpoint.stats.rows_processed < self.config.min_rows_for_adaptation {
            self.checkpoints.write().push(checkpoint);
            return AdaptiveDecision::Continue;
        }

        let decision = self.evaluate(&checkpoint);

        if decision != AdaptiveDecision::Continue {
            self.adaptation_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.decisions
                .write()
                .push((checkpoint.clone(), decision.clone()));
        }

        self.checkpoints.write().push(checkpoint);
        decision
    }

    fn evaluate(&self, checkpoint: &AdaptiveCheckpoint) -> AdaptiveDecision {
        let stats = &checkpoint.stats;

        // Memory pressure check
        if stats.memory_pressure() > self.config.memory_pressure_threshold {
            if !stats.spilled {
                return AdaptiveDecision::EnableSpill;
            }
            if stats.parallelism > 1 {
                return AdaptiveDecision::DecreaseParallelism(stats.parallelism / 2);
            }
        }

        // Cardinality check
        let error = stats.estimation_error();
        if error > self.config.cardinality_error_threshold {
            if checkpoint.operator_type.contains("Join")
                && stats.build_side_rows > stats.estimated_rows * 10
            {
                return AdaptiveDecision::SwitchJoinStrategy(JoinStrategy::HashJoin);
            }
            if checkpoint.operator_type.contains("Aggregate") {
                return AdaptiveDecision::SwitchToHashAggregation;
            }
        }

        // Skew check
        if stats.has_significant_skew()
            && stats.parallelism > 1
            && checkpoint.operator_type.contains("Join")
        {
            return AdaptiveDecision::SwitchJoinStrategy(JoinStrategy::BroadcastHashJoin);
        }

        AdaptiveDecision::Continue
    }

    pub fn summary(&self) -> AdaptiveExecutionSummary {
        let checkpoints = self.checkpoints.read();
        let decisions = self.decisions.read();
        AdaptiveExecutionSummary {
            total_checkpoints: checkpoints.len(),
            adaptations_made: decisions.len(),
            decisions: decisions.iter().map(|(_, d)| format!("{:?}", d)).collect(),
        }
    }

    pub fn reset(&self) {
        self.checkpoints.write().clear();
        self.decisions.write().clear();
        self.adaptation_count
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveExecutionSummary {
    pub total_checkpoints: usize,
    pub adaptations_made: usize,
    pub decisions: Vec<String>,
}

// ============================================================================
// Per-Tenant Resource Quotas
// ============================================================================

/// Tenant quota definition
#[derive(Debug, Clone)]
pub struct TenantQuota {
    pub tenant_id: String,
    pub max_concurrent_queries: u32,
    pub max_memory_per_query: u64,
    pub max_total_memory: u64,
    pub max_cpu_time_us: u64,
    pub max_iops: u64,
    pub max_rows_per_query: u64,
    pub max_query_time_secs: u64,
    pub priority: u32,
    pub enabled: bool,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            tenant_id: String::new(),
            max_concurrent_queries: 10,
            max_memory_per_query: 1024 * 1024 * 1024,
            max_total_memory: 4 * 1024 * 1024 * 1024,
            max_cpu_time_us: 60_000_000,
            max_iops: 10_000,
            max_rows_per_query: 10_000_000,
            max_query_time_secs: 300,
            priority: 5,
            enabled: true,
        }
    }
}

/// Tenant usage tracking
#[derive(Debug, Clone, Default)]
pub struct TenantUsage {
    pub concurrent_queries: u32,
    pub memory_used: u64,
    pub total_queries: u64,
    pub throttled_queries: u64,
    pub rejected_queries: u64,
    pub total_cpu_time_us: u64,
    pub total_rows: u64,
}

/// Tenant resource manager
pub struct TenantResourceManager {
    quotas: RwLock<HashMap<String, TenantQuota>>,
    usage: RwLock<HashMap<String, TenantUsage>>,
    default_quota: TenantQuota,
}

impl TenantResourceManager {
    pub fn new(default_quota: TenantQuota) -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            default_quota,
        }
    }

    pub fn set_quota(&self, tenant_id: &str, quota: TenantQuota) {
        self.quotas.write().insert(tenant_id.to_string(), quota);
    }

    pub fn get_quota(&self, tenant_id: &str) -> TenantQuota {
        self.quotas
            .read()
            .get(tenant_id)
            .cloned()
            .unwrap_or_else(|| {
                let mut q = self.default_quota.clone();
                q.tenant_id = tenant_id.to_string();
                q
            })
    }

    pub fn try_acquire(
        &self,
        tenant_id: &str,
        memory_estimate: u64,
    ) -> Result<TenantQueryToken, TenantQuotaError> {
        let quota = self.get_quota(tenant_id);

        if !quota.enabled {
            return Err(TenantQuotaError::TenantDisabled);
        }

        let mut usage = self.usage.write();
        let tenant_usage = usage.entry(tenant_id.to_string()).or_default();

        if tenant_usage.concurrent_queries >= quota.max_concurrent_queries {
            tenant_usage.throttled_queries += 1;
            return Err(TenantQuotaError::ConcurrencyLimitExceeded {
                current: tenant_usage.concurrent_queries,
                limit: quota.max_concurrent_queries,
            });
        }

        if tenant_usage.memory_used + memory_estimate > quota.max_total_memory {
            tenant_usage.rejected_queries += 1;
            return Err(TenantQuotaError::MemoryLimitExceeded {
                current: tenant_usage.memory_used,
                requested: memory_estimate,
                limit: quota.max_total_memory,
            });
        }

        tenant_usage.concurrent_queries += 1;
        tenant_usage.memory_used += memory_estimate;
        tenant_usage.total_queries += 1;

        Ok(TenantQueryToken {
            tenant_id: tenant_id.to_string(),
            memory_reserved: memory_estimate,
            start_time: Instant::now(),
        })
    }

    pub fn release(&self, token: TenantQueryToken, rows_processed: u64, cpu_time_us: u64) {
        let mut usage = self.usage.write();
        if let Some(tenant_usage) = usage.get_mut(&token.tenant_id) {
            tenant_usage.concurrent_queries = tenant_usage.concurrent_queries.saturating_sub(1);
            tenant_usage.memory_used = tenant_usage
                .memory_used
                .saturating_sub(token.memory_reserved);
            tenant_usage.total_cpu_time_us += cpu_time_us;
            tenant_usage.total_rows += rows_processed;
        }
    }

    pub fn get_usage(&self, tenant_id: &str) -> TenantUsage {
        self.usage
            .read()
            .get(tenant_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn all_stats(&self) -> Vec<(String, TenantQuota, TenantUsage)> {
        let quotas = self.quotas.read();
        let usage = self.usage.read();
        quotas
            .iter()
            .map(|(id, quota)| {
                let u = usage.get(id).cloned().unwrap_or_default();
                (id.clone(), quota.clone(), u)
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct TenantQueryToken {
    pub tenant_id: String,
    pub memory_reserved: u64,
    pub start_time: Instant,
}

#[derive(Debug, Clone)]
pub enum TenantQuotaError {
    TenantDisabled,
    ConcurrencyLimitExceeded {
        current: u32,
        limit: u32,
    },
    MemoryLimitExceeded {
        current: u64,
        requested: u64,
        limit: u64,
    },
}

impl std::fmt::Display for TenantQuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TenantDisabled => write!(f, "Tenant disabled"),
            Self::ConcurrencyLimitExceeded { current, limit } => {
                write!(f, "Concurrency limit: {} / {}", current, limit)
            }
            Self::MemoryLimitExceeded {
                current,
                requested,
                limit,
            } => {
                write!(f, "Memory limit: {} + {} > {}", current, requested, limit)
            }
        }
    }
}

impl std::error::Error for TenantQuotaError {}
