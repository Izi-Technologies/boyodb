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
    /// Cost per page read from disk
    pub seq_page_cost: f64,
    /// Cost per random page read
    pub random_page_cost: f64,
    /// Cost per byte transferred over network
    pub network_byte_cost: f64,
    /// Page size in bytes
    pub page_size: u64,
    /// Effective cache size for cost estimation
    pub effective_cache_size: u64,
}

impl Default for CostModelParams {
    fn default() -> Self {
        Self {
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            network_byte_cost: 0.001,
            page_size: 8192,
            effective_cache_size: 4 * 1024 * 1024 * 1024, // 4GB
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
    Union {
        inputs: Vec<LogicalPlan>,
        all: bool,
    },
    /// Subquery
    Subquery {
        input: Box<LogicalPlan>,
        alias: String,
    },
}

#[derive(Debug, Clone)]
pub enum ProjectExpr {
    Column(String),
    Alias { expr: Box<ProjectExpr>, alias: String },
    Function { name: String, args: Vec<ProjectExpr> },
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
    IsNull {
        column: String,
        negated: bool,
    },
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
                preds = preds.into_iter()
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
                preds = preds.into_iter()
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
            Predicate::Not(p) => {
                match p.simplify() {
                    Predicate::True => Predicate::False,
                    Predicate::False => Predicate::True,
                    Predicate::Not(inner) => *inner,
                    other => Predicate::Not(Box::new(other)),
                }
            }
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
    Gather {
        inputs: Vec<PhysicalPlan>,
    },
    /// Union all
    UnionAll {
        inputs: Vec<PhysicalPlan>,
    },
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
    Hash { columns: Vec<String>, num_partitions: usize },
    /// Range partitioning
    Range { column: String, boundaries: Vec<StatValue> },
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
            eq_selectivity: 0.01,     // 1% match for equality
            range_selectivity: 0.33,   // 33% for range predicates
        }
    }
}

impl SelectivityEstimator {
    /// Estimate selectivity of a predicate given column statistics
    pub fn estimate(&self, predicate: &Predicate, stats: &HashMap<String, ColumnStatistics>) -> f64 {
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
            Predicate::In { column, values, negated } => {
                let base = if let Some(col_stats) = stats.get(column) {
                    let distinct = col_stats.distinct_count.max(1) as f64;
                    (values.len() as f64 / distinct).min(1.0)
                } else {
                    (values.len() as f64 * self.eq_selectivity).min(1.0)
                };
                if *negated { 1.0 - base } else { base }
            }
            Predicate::Between { column, low, high, negated } => {
                let base = if let Some(col_stats) = stats.get(column) {
                    self.estimate_range(col_stats, low, high)
                } else {
                    self.range_selectivity
                };
                if *negated { 1.0 - base } else { base }
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
                if *negated { 1.0 - base } else { base }
            }
            Predicate::Like { negated, .. } => {
                // LIKE selectivity depends on pattern, use default
                let base = self.default_selectivity;
                if *negated { 1.0 - base } else { base }
            }
            Predicate::And(preds) => {
                // Assume independence (may overestimate selectivity)
                preds.iter()
                    .map(|p| self.estimate(p, stats))
                    .product()
            }
            Predicate::Or(preds) => {
                // P(A or B) = P(A) + P(B) - P(A and B)
                // Simplified: assume independence
                let individual: Vec<f64> = preds.iter()
                    .map(|p| self.estimate(p, stats))
                    .collect();
                1.0 - individual.iter().map(|s| 1.0 - s).product::<f64>()
            }
            Predicate::Not(p) => 1.0 - self.estimate(p, stats),
            Predicate::True => 1.0,
            Predicate::False => 0.0,
            Predicate::ColumnCompare { .. } => self.default_selectivity,
        }
    }

    fn estimate_comparison(&self, stats: &ColumnStatistics, op: CompareOp, value: &StatValue) -> f64 {
        match op {
            CompareOp::Eq => {
                // Uniform distribution assumption
                1.0 / stats.distinct_count.max(1) as f64
            }
            CompareOp::Ne => {
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

    fn estimate_from_histogram(&self, histogram: &Histogram, op: CompareOp, value: &StatValue) -> f64 {
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

    fn estimate_linear(&self, min: &StatValue, max: &StatValue, op: CompareOp, value: &StatValue) -> f64 {
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
            let (min_f, max_f, low_f, high_f) = match (min.as_f64(), max.as_f64(), low.as_f64(), high.as_f64()) {
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
        self.indexes.entry(table.to_string()).or_default().push(index);
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
                    LogicalPlan::Scan { table, database, columns, filter } => {
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
                    LogicalPlan::Join { left, right, join_type, condition } => {
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
                    other => {
                        LogicalPlan::Filter {
                            input: Box::new(self.push_down_predicates(other)),
                            predicate,
                        }
                    }
                }
            }
            LogicalPlan::Join { left, right, join_type, condition } => {
                LogicalPlan::Join {
                    left: Box::new(self.push_down_predicates(*left)),
                    right: Box::new(self.push_down_predicates(*right)),
                    join_type,
                    condition,
                }
            }
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(self.push_down_predicates(*input)),
                    columns,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
                LogicalPlan::Aggregate {
                    input: Box::new(self.push_down_predicates(*input)),
                    group_by,
                    aggregates,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                LogicalPlan::Sort {
                    input: Box::new(self.push_down_predicates(*input)),
                    order_by,
                }
            }
            LogicalPlan::Limit { input, limit, offset } => {
                LogicalPlan::Limit {
                    input: Box::new(self.push_down_predicates(*input)),
                    limit,
                    offset,
                }
            }
            other => other,
        }
    }

    fn get_join_columns(&self, left: &LogicalPlan, right: &LogicalPlan) -> (Vec<String>, Vec<String>) {
        fn get_output_columns(plan: &LogicalPlan) -> Vec<String> {
            match plan {
                LogicalPlan::Scan { columns, .. } => columns.clone(),
                LogicalPlan::Project { columns, .. } => {
                    columns.iter().filter_map(|c| {
                        match c {
                            ProjectExpr::Column(name) => Some(name.clone()),
                            ProjectExpr::Alias { alias, .. } => Some(alias.clone()),
                            _ => None,
                        }
                    }).collect()
                }
                LogicalPlan::Filter { input, .. } => get_output_columns(input),
                LogicalPlan::Aggregate { group_by, aggregates, .. } => {
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
            LogicalPlan::Join { left, right, join_type, condition } => {
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
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.optimize_joins(*input)),
                    predicate,
                }
            }
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(self.optimize_joins(*input)),
                    columns,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
                LogicalPlan::Aggregate {
                    input: Box::new(self.optimize_joins(*input)),
                    group_by,
                    aggregates,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                LogicalPlan::Sort {
                    input: Box::new(self.optimize_joins(*input)),
                    order_by,
                }
            }
            LogicalPlan::Limit { input, limit, offset } => {
                LogicalPlan::Limit {
                    input: Box::new(self.optimize_joins(*input)),
                    limit,
                    offset,
                }
            }
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
                Predicate::ColumnCompare { left: right, op: swapped_op, right: left }
            }
            Predicate::And(preds) => {
                Predicate::And(preds.into_iter().map(|p| self.swap_predicate_columns(p)).collect())
            }
            other => other,
        }
    }

    /// Estimate cost of a logical plan
    fn estimate_plan_cost(&self, plan: &LogicalPlan) -> QueryCost {
        match plan {
            LogicalPlan::Scan { table, filter, columns, .. } => {
                let stats = self.table_stats.get(table);
                let base_rows = stats.map(|s| s.row_count).unwrap_or(1000) as f64;
                let base_bytes = stats.map(|s| s.size_bytes).unwrap_or(10000) as f64;

                let selectivity = filter.as_ref()
                    .map(|f| {
                        let col_stats = stats.map(|s| &s.columns).cloned().unwrap_or_default();
                        self.selectivity.estimate(f, &col_stats)
                    })
                    .unwrap_or(1.0);

                let output_rows = base_rows * selectivity;
                let col_fraction = if columns.is_empty() { 1.0 } else {
                    stats.map(|s| columns.len() as f64 / s.columns.len().max(1) as f64)
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
                    cpu_cost: input_cost.cpu_cost + input_cost.output_rows * self.cost_params.cpu_operator_cost,
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
            LogicalPlan::Aggregate { input, group_by, .. } => {
                let input_cost = self.estimate_plan_cost(input);

                // Estimate number of groups
                let num_groups = if group_by.is_empty() {
                    1.0
                } else {
                    // Assume each group by column reduces rows by 10x
                    (input_cost.output_rows / (10.0_f64.powi(group_by.len() as i32))).max(1.0)
                };

                QueryCost {
                    cpu_cost: input_cost.cpu_cost + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    io_cost: input_cost.io_cost,
                    memory_cost: num_groups * 100.0, // Hash table for groups
                    network_cost: input_cost.network_cost,
                    output_rows: num_groups,
                    output_bytes: num_groups * 50.0 * (group_by.len() + 1) as f64,
                }
            }
            LogicalPlan::Sort { input, .. } => {
                let input_cost = self.estimate_plan_cost(input);
                let sort_cost = input_cost.output_rows * (input_cost.output_rows.log2().max(1.0))
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
                    output_bytes: output_rows * (input_cost.output_bytes / input_cost.output_rows.max(1.0)),
                }
            }
            LogicalPlan::Project { input, .. } => {
                let input_cost = self.estimate_plan_cost(input);
                QueryCost {
                    cpu_cost: input_cost.cpu_cost + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    ..input_cost
                }
            }
            LogicalPlan::Union { inputs, .. } => {
                inputs.iter()
                    .map(|p| self.estimate_plan_cost(p))
                    .fold(QueryCost::default(), |acc, c| QueryCost {
                        cpu_cost: acc.cpu_cost + c.cpu_cost,
                        io_cost: acc.io_cost + c.io_cost,
                        memory_cost: acc.memory_cost.max(c.memory_cost),
                        network_cost: acc.network_cost + c.network_cost,
                        output_rows: acc.output_rows + c.output_rows,
                        output_bytes: acc.output_bytes + c.output_bytes,
                    })
            }
            LogicalPlan::Subquery { input, .. } => self.estimate_plan_cost(input),
        }
    }

    /// Convert logical plan to physical plan
    fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        match plan {
            LogicalPlan::Scan { table, database, columns, filter } => {
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
            LogicalPlan::Filter { input, predicate } => {
                PhysicalPlan::Filter {
                    input: Box::new(self.to_physical(*input)),
                    predicate,
                }
            }
            LogicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.to_physical(*input)),
                    columns,
                }
            }
            LogicalPlan::Aggregate { input, group_by, aggregates } => {
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
            LogicalPlan::Join { left, right, join_type, condition } => {
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
            LogicalPlan::Sort { input, order_by } => {
                PhysicalPlan::Sort {
                    input: Box::new(self.to_physical(*input)),
                    order_by,
                    limit: None,
                }
            }
            LogicalPlan::Limit { input, limit, offset } => {
                let input_physical = self.to_physical(*input);

                // Check if we can use TopN optimization
                if let PhysicalPlan::Sort { input: sort_input, order_by, .. } = input_physical {
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
                let physical_inputs: Vec<_> = inputs.into_iter()
                    .map(|p| self.to_physical(p))
                    .collect();

                if all {
                    PhysicalPlan::UnionAll { inputs: physical_inputs }
                } else {
                    // UNION requires deduplication
                    PhysicalPlan::HashAggregate {
                        input: Box::new(PhysicalPlan::UnionAll { inputs: physical_inputs }),
                        group_by: vec![], // All columns
                        aggregates: vec![],
                    }
                }
            }
            LogicalPlan::Subquery { input, .. } => {
                self.to_physical(*input)
            }
        }
    }

    fn should_use_index(&self, table: &str, filter: &Option<Predicate>) -> Option<(String, IndexRange)> {
        let filter = filter.as_ref()?;
        let indexes = self.indexes.get(table)?;

        for index in indexes {
            if let Some(range) = self.predicate_to_index_range(filter, &index.columns) {
                return Some((index.name.clone(), range));
            }
        }
        None
    }

    fn predicate_to_index_range(&self, pred: &Predicate, index_cols: &[String]) -> Option<IndexRange> {
        if index_cols.is_empty() {
            return None;
        }
        let first_col = &index_cols[0];

        match pred {
            Predicate::Comparison { column, op, value } if column == first_col => {
                Some(match op {
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
                })
            }
            Predicate::Between { column, low, high, negated: false } if column == first_col => {
                Some(IndexRange {
                    start: Some(low.clone()),
                    start_inclusive: true,
                    end: Some(high.clone()),
                    end_inclusive: true,
                })
            }
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
                    Some(IndexRange { start, start_inclusive, end, end_inclusive })
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

    fn is_sorted_by(&self, _plan: &PhysicalPlan, _columns: &[String]) -> bool {
        // TODO: Track sort order through physical plan
        false
    }

    fn extract_equi_join_keys(&self, condition: &JoinCondition) -> Option<(Vec<String>, Vec<String>)> {
        match condition {
            JoinCondition::On(pred) => self.extract_equi_keys_from_predicate(pred),
            JoinCondition::Using(cols) => Some((cols.clone(), cols.clone())),
            JoinCondition::Natural => None, // Would need schema info
            JoinCondition::None => None,
        }
    }

    fn extract_equi_keys_from_predicate(&self, pred: &Predicate) -> Option<(Vec<String>, Vec<String>)> {
        match pred {
            Predicate::ColumnCompare { left, op: CompareOp::Eq, right } => {
                Some((vec![left.clone()], vec![right.clone()]))
            }
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
            PhysicalPlan::SeqScan { table, parallel_degree, .. } => {
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
                    cpu_cost: input_cost.cpu_cost + input_cost.output_rows * self.cost_params.cpu_operator_cost,
                    output_rows: input_cost.output_rows * 0.5, // Assume 50% selectivity
                    ..input_cost
                }
            }
            PhysicalPlan::HashJoin { build, probe, .. } => {
                let build_cost = self.estimate_physical_cost(build);
                let probe_cost = self.estimate_physical_cost(probe);

                QueryCost {
                    cpu_cost: build_cost.cpu_cost + probe_cost.cpu_cost
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
    pub fn parallelize(&self, plan: PhysicalPlan, stats: &HashMap<String, TableStats>) -> PhysicalPlan {
        match plan {
            PhysicalPlan::SeqScan { table, database, columns, filter, parallel_degree: _ } => {
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
            PhysicalPlan::HashAggregate { input, group_by, aggregates } => {
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
            PhysicalPlan::HashJoin { build, probe, join_type, build_keys, probe_keys, .. } => {
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
            PhysicalPlan::Filter { input, predicate } => {
                PhysicalPlan::Filter {
                    input: Box::new(self.parallelize(*input, stats)),
                    predicate,
                }
            }
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.parallelize(*input, stats)),
                    columns,
                }
            }
            PhysicalPlan::Sort { input, order_by, limit } => {
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
            PhysicalPlan::SeqScan { parallel_degree, .. } => *parallel_degree > 1,
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
        stats.insert("id".to_string(), ColumnStatistics {
            distinct_count: 1000,
            null_count: 0,
            min_value: Some(StatValue::Int64(1)),
            max_value: Some(StatValue::Int64(1000)),
            avg_length: None,
            histogram: None,
        });

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
        optimizer.update_stats("small", TableStats {
            row_count: 100,
            size_bytes: 10000,
            ..Default::default()
        });
        optimizer.update_stats("large", TableStats {
            row_count: 1000000,
            size_bytes: 100000000,
            ..Default::default()
        });

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
            LogicalPlan::Join { left, .. } => {
                match *left {
                    LogicalPlan::Scan { table, .. } => {
                        assert_eq!(table, "small");
                    }
                    _ => panic!("Expected scan"),
                }
            }
            _ => panic!("Expected join"),
        }
    }

    #[test]
    fn test_physical_plan_generation() {
        let mut optimizer = CostBasedOptimizer::new(4);
        optimizer.update_stats("users", TableStats {
            row_count: 100000,
            size_bytes: 10000000,
            ..Default::default()
        });

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
        stats.insert("large_table".to_string(), TableStats {
            row_count: 1000000,
            size_bytes: 100 * 1024 * 1024,
            ..Default::default()
        });

        let plan = PhysicalPlan::SeqScan {
            table: "large_table".to_string(),
            database: "test".to_string(),
            columns: vec!["id".to_string()],
            filter: None,
            parallel_degree: 1,
        };

        let parallel_plan = planner.parallelize(plan, &stats);

        match parallel_plan {
            PhysicalPlan::SeqScan { parallel_degree, .. } => {
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
