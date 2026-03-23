use crate::engine::EngineError;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Join,
    JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, UnaryOperator, Value, With,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Parsed SQL query representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub database: Option<String>,
    pub table: Option<String>,
    pub projection: Option<Vec<String>>,
    pub filter: QueryFilter,
    pub aggregation: Option<AggPlan>,
    pub order_by: Option<Vec<OrderByClause>>,
    pub distinct: bool,
    /// DISTINCT ON (columns) - PostgreSQL-style first row per group
    /// When set, returns first row for each unique combination of these columns
    #[serde(default)]
    pub distinct_on: Option<Vec<String>>,
    pub joins: Vec<JoinClause>,
    /// Computed columns with expressions (SELECT expr AS alias, ...)
    pub computed_columns: Vec<SelectColumn>,
    /// CTEs defined in WITH clause
    pub ctes: Vec<CteDefinition>,
    /// TABLESAMPLE clause for statistical sampling
    pub sample: Option<SampleClause>,
}

/// TABLESAMPLE clause for statistical sampling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleClause {
    /// Sampling method
    pub method: SampleMethod,
    /// Sample size (percentage or row count)
    pub size: f64,
    /// Optional seed for reproducible sampling
    pub seed: Option<u64>,
}

/// Sampling methods supported
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleMethod {
    /// BERNOULLI - row-level random sampling
    Bernoulli,
    /// SYSTEM - block-level random sampling (faster)
    System,
    /// RESERVOIR - reservoir sampling for exact row counts
    Reservoir,
}

/// Extract TABLESAMPLE clause from SQL string and return parsed clause + cleaned SQL
/// Syntax: TABLESAMPLE [BERNOULLI|SYSTEM|RESERVOIR] (percentage) [REPEATABLE (seed)]
pub fn extract_tablesample(sql: &str) -> (Option<SampleClause>, String) {
    let upper = sql.to_uppercase();

    // Find TABLESAMPLE keyword
    if let Some(start) = upper.find(" TABLESAMPLE ") {
        let after_tablesample = &sql[start + " TABLESAMPLE ".len()..];
        let upper_after = &upper[start + " TABLESAMPLE ".len()..];

        // Parse method (default to BERNOULLI)
        let (method, rest) = if upper_after.starts_with("BERNOULLI") {
            (
                SampleMethod::Bernoulli,
                after_tablesample["BERNOULLI".len()..].trim_start(),
            )
        } else if upper_after.starts_with("SYSTEM") {
            (
                SampleMethod::System,
                after_tablesample["SYSTEM".len()..].trim_start(),
            )
        } else if upper_after.starts_with("RESERVOIR") {
            (
                SampleMethod::Reservoir,
                after_tablesample["RESERVOIR".len()..].trim_start(),
            )
        } else {
            // Default to BERNOULLI if no method specified
            (SampleMethod::Bernoulli, after_tablesample.trim_start())
        };

        // Parse percentage in parentheses
        if rest.starts_with('(') {
            if let Some(close_paren) = rest.find(')') {
                let size_str = rest[1..close_paren].trim();
                if let Ok(size) = size_str.parse::<f64>() {
                    let rest_after_paren = &rest[close_paren + 1..];
                    let upper_rest = rest_after_paren.to_uppercase();

                    // Parse optional REPEATABLE (seed)
                    let seed = if let Some(rep_pos) = upper_rest.find("REPEATABLE") {
                        let after_rep =
                            &rest_after_paren[rep_pos + "REPEATABLE".len()..].trim_start();
                        if after_rep.starts_with('(') {
                            if let Some(seed_close) = after_rep.find(')') {
                                let seed_str = after_rep[1..seed_close].trim();
                                seed_str.parse::<u64>().ok()
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Find where the TABLESAMPLE clause ends
                    let end_offset = if let Some(rep_pos) = upper_rest.find("REPEATABLE") {
                        let after_rep =
                            &rest_after_paren[rep_pos + "REPEATABLE".len()..].trim_start();
                        if let Some(seed_close) = after_rep.find(')') {
                            start
                                + " TABLESAMPLE ".len()
                                + (rest.len() - after_rep.len())
                                + rep_pos
                                + "REPEATABLE".len()
                                + (after_rep.len() - after_rep[seed_close..].len())
                                + 1
                        } else {
                            start + " TABLESAMPLE ".len() + close_paren + 1
                        }
                    } else {
                        start + " TABLESAMPLE ".len() + close_paren + 1
                    };

                    // Remove TABLESAMPLE clause from SQL
                    let cleaned_sql = format!("{}{}", &sql[..start], &sql[end_offset..]);

                    return (Some(SampleClause { method, size, seed }), cleaned_sql);
                }
            }
        }
    }

    (None, sql.to_string())
}

/// JOIN clause representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub database: String,
    pub alias: Option<String>,
    pub on_condition: JoinCondition,
}

/// Supported JOIN types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
    /// ASOF join - match on closest timestamp/value (for time-series)
    AsOf,
    /// ASOF LEFT join - like LEFT JOIN but matches on closest value
    AsOfLeft,
    /// Semi join - exists in right table (for IN subqueries)
    Semi,
    /// Anti join - not exists in right table (for NOT IN subqueries)
    Anti,
}

/// JOIN ON condition with support for non-equi joins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_column: String,
    pub right_column: String,
    /// Comparison operator (=, <, >, <=, >=, <>)
    #[serde(default = "default_join_op")]
    pub operator: JoinComparisonOp,
}

fn default_join_op() -> JoinComparisonOp {
    JoinComparisonOp::Equal
}

/// Join comparison operators (for non-equi joins)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum JoinComparisonOp {
    #[default]
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    NotEqual,
}

/// ASOF join configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsOfJoinConfig {
    /// Column to match on (usually timestamp)
    pub match_column: String,
    /// Tolerance window (e.g., "5 seconds", "1 hour")
    pub tolerance: Option<String>,
    /// Direction: 'backward' (default), 'forward', 'nearest'
    pub direction: AsOfDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AsOfDirection {
    /// Match the closest value <= the probe value (default)
    #[default]
    Backward,
    /// Match the closest value >= the probe value
    Forward,
    /// Match the nearest value in either direction
    Nearest,
}

/// ORDER BY clause representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByClause {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

/// Numeric comparison operator for range filters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumericOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A numeric filter on a column (for segment pruning)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericFilter {
    pub column: String,
    pub op: NumericOp,
    pub value: NumericValue,
}

/// Numeric value types for filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumericValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}

impl NumericValue {
    /// Convert to PrimitiveValue for comparison with column stats
    pub fn to_primitive(&self) -> crate::replication::PrimitiveValue {
        match self {
            NumericValue::Int64(v) => crate::replication::PrimitiveValue::Int64(*v),
            NumericValue::UInt64(v) => crate::replication::PrimitiveValue::UInt64(*v),
            NumericValue::Float64(v) => crate::replication::PrimitiveValue::Float64(*v),
        }
    }
}

/// Filter conditions extracted from WHERE clause
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryFilter {
    pub watermark_ge: Option<u64>,
    pub watermark_le: Option<u64>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub event_time_ge: Option<u64>,
    pub event_time_le: Option<u64>,
    pub tenant_id_eq: Option<u64>,
    pub route_id_eq: Option<u64>,
    /// tenant_id IN (values) filter
    #[serde(default)]
    pub tenant_id_in: Option<Vec<u64>>,
    /// route_id IN (values) filter
    #[serde(default)]
    pub route_id_in: Option<Vec<u64>>,
    /// Generic numeric IN filters: Vec<(column, values)>
    #[serde(default)]
    pub numeric_in_filters: Vec<(String, Vec<i64>)>,
    /// LIKE patterns: Vec<(column, pattern, negate)>
    pub like_filters: Vec<(String, String, bool)>,
    /// ILIKE patterns (case-insensitive): Vec<(column, pattern, negate)>
    #[serde(default)]
    pub ilike_filters: Vec<(String, String, bool)>,
    /// IS NULL checks: Vec<(column, is_null)> - is_null=true means IS NULL, false means IS NOT NULL
    pub null_filters: Vec<(String, bool)>,
    /// Generic string equality filters: Vec<(column, value)>
    pub string_eq_filters: Vec<(String, String)>,
    /// Generic string IN filters: Vec<(column, values)>
    pub string_in_filters: Vec<(String, Vec<String>)>,
    /// IN subquery filters: Vec<(column, subquery SQL, negated)>
    pub in_subquery_filters: Vec<(String, String, bool)>,
    /// EXISTS subquery filters: Vec<(subquery SQL, negated)>
    pub exists_subqueries: Vec<(String, bool)>,
    /// Scalar subquery comparisons: Vec<(column, op, subquery SQL)>
    pub scalar_subquery_filters: Vec<(String, String, String)>,
    /// Numeric equality filters: Vec<(column, value)> for segment pruning
    #[serde(default)]
    pub numeric_eq_filters: Vec<(String, NumericValue)>,
    /// Numeric range filters for segment pruning (column, op, value)
    #[serde(default)]
    pub numeric_range_filters: Vec<NumericFilter>,
    /// ORDER BY clause (moved from separate field for consistency)
    #[serde(default)]
    pub order_by: Option<Vec<OrderByClause>>,
    /// DISTINCT flag
    #[serde(default)]
    pub distinct: bool,
    /// DISTINCT ON (columns) - PostgreSQL-style first row per group
    #[serde(default)]
    pub distinct_on: Option<Vec<String>>,
}

/// Aggregation function type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggKind {
    CountStar,
    /// COUNT(DISTINCT column)
    CountDistinct {
        column: String,
    },
    Sum {
        column: String,
    },
    Avg {
        column: String,
    },
    Min {
        column: String,
    },
    Max {
        column: String,
    },
    /// STDDEV/STDDEV_SAMP - sample standard deviation
    StddevSamp {
        column: String,
    },
    /// STDDEV_POP - population standard deviation
    StddevPop {
        column: String,
    },
    /// VARIANCE/VAR_SAMP - sample variance
    VarianceSamp {
        column: String,
    },
    /// VAR_POP - population variance
    VariancePop {
        column: String,
    },
    /// APPROX_COUNT_DISTINCT - HyperLogLog distinct count
    ApproxCountDistinct {
        column: String,
    },
    /// APPROX_PERCENTILE - T-Digest based approximate percentile (faster for large datasets)
    ApproxPercentile {
        column: String,
        percentile: f64,
    },
    /// APPROX_MEDIAN - T-Digest based approximate median
    ApproxMedian {
        column: String,
    },
    /// MEDIAN - 50th percentile (equivalent to PERCENTILE_CONT(0.5))
    Median {
        column: String,
    },
    /// PERCENTILE_CONT - continuous percentile (interpolated)
    PercentileCont {
        column: String,
        percentile: f64,
    },
    /// PERCENTILE_DISC - discrete percentile (actual value from data)
    PercentileDisc {
        column: String,
        percentile: f64,
    },
    /// ARRAY_AGG - collect values into array
    ArrayAgg {
        column: String,
        distinct: bool,
    },
    /// STRING_AGG / GROUP_CONCAT - concatenate strings with delimiter
    StringAgg {
        column: String,
        delimiter: String,
        distinct: bool,
    },
    /// MODE - most frequent value (WITHIN GROUP ordered aggregate)
    Mode {
        column: String,
    },
    /// STRING_AGG with WITHIN GROUP ordering
    StringAggOrdered {
        column: String,
        delimiter: String,
        order_by: String,
        order_desc: bool,
    },
    /// ARRAY_AGG with WITHIN GROUP ordering
    ArrayAggOrdered {
        column: String,
        order_by: String,
        order_desc: bool,
    },
    /// NTH_VALUE - get nth value in ordered group
    NthValue {
        column: String,
        n: usize,
    },
    /// FIRST_VALUE - first value in ordered group
    FirstValue {
        column: String,
    },
    /// LAST_VALUE - last value in ordered group
    LastValue {
        column: String,
    },
}

/// Aggregate with optional alias and FILTER clause for result column naming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub kind: AggKind,
    pub alias: Option<String>,
    /// Optional FILTER clause: only include rows where this condition is true
    /// Example: COUNT(*) FILTER (WHERE status = 'active')
    #[serde(default)]
    pub filter: Option<AggregateFilter>,
}

/// Filter condition for FILTER (WHERE ...) clause in aggregates
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateFilter {
    /// Column to filter on
    pub column: String,
    /// Comparison operator
    pub op: FilterOp,
    /// Value to compare against
    pub value: FilterValue,
}

/// Filter comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    IsNull,
    IsNotNull,
    Like,
    In,
}

/// Filter value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    /// For IN operator: multiple values
    List(Vec<FilterValue>),
}

impl PartialEq for FilterValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FilterValue::Null, FilterValue::Null) => true,
            (FilterValue::Bool(a), FilterValue::Bool(b)) => a == b,
            (FilterValue::Int(a), FilterValue::Int(b)) => a == b,
            (FilterValue::Float(a), FilterValue::Float(b)) => {
                (a - b).abs() < f64::EPSILON || (a.is_nan() && b.is_nan())
            }
            (FilterValue::String(a), FilterValue::String(b)) => a == b,
            (FilterValue::List(a), FilterValue::List(b)) => a == b,
            _ => false,
        }
    }
}

impl AggregateFilter {
    pub fn new(column: String, op: FilterOp, value: FilterValue) -> Self {
        Self { column, op, value }
    }

    /// Check if a row passes this filter
    pub fn matches(&self, row_value: Option<&serde_json::Value>) -> bool {
        match (&self.op, row_value, &self.value) {
            (FilterOp::IsNull, None, _) => true,
            (FilterOp::IsNull, Some(serde_json::Value::Null), _) => true,
            (FilterOp::IsNull, Some(_), _) => false,
            (FilterOp::IsNotNull, None, _) => false,
            (FilterOp::IsNotNull, Some(serde_json::Value::Null), _) => false,
            (FilterOp::IsNotNull, Some(_), _) => true,
            (_, None, _) => false,
            (_, Some(serde_json::Value::Null), _) => false,
            (FilterOp::Eq, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x == *i).unwrap_or(false)
            }
            (FilterOp::Eq, Some(v), FilterValue::Float(f)) => v
                .as_f64()
                .map(|x| (x - f).abs() < f64::EPSILON)
                .unwrap_or(false),
            (FilterOp::Eq, Some(v), FilterValue::String(s)) => {
                v.as_str().map(|x| x == s).unwrap_or(false)
            }
            (FilterOp::Eq, Some(v), FilterValue::Bool(b)) => {
                v.as_bool().map(|x| x == *b).unwrap_or(false)
            }
            (FilterOp::Ne, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x != *i).unwrap_or(true)
            }
            (FilterOp::Ne, Some(v), FilterValue::Float(f)) => v
                .as_f64()
                .map(|x| (x - f).abs() >= f64::EPSILON)
                .unwrap_or(true),
            (FilterOp::Ne, Some(v), FilterValue::String(s)) => {
                v.as_str().map(|x| x != s).unwrap_or(true)
            }
            (FilterOp::Ne, Some(v), FilterValue::Bool(b)) => {
                v.as_bool().map(|x| x != *b).unwrap_or(true)
            }
            (FilterOp::Gt, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x > *i).unwrap_or(false)
            }
            (FilterOp::Gt, Some(v), FilterValue::Float(f)) => {
                v.as_f64().map(|x| x > *f).unwrap_or(false)
            }
            (FilterOp::Ge, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x >= *i).unwrap_or(false)
            }
            (FilterOp::Ge, Some(v), FilterValue::Float(f)) => {
                v.as_f64().map(|x| x >= *f).unwrap_or(false)
            }
            (FilterOp::Lt, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x < *i).unwrap_or(false)
            }
            (FilterOp::Lt, Some(v), FilterValue::Float(f)) => {
                v.as_f64().map(|x| x < *f).unwrap_or(false)
            }
            (FilterOp::Le, Some(v), FilterValue::Int(i)) => {
                v.as_i64().map(|x| x <= *i).unwrap_or(false)
            }
            (FilterOp::Le, Some(v), FilterValue::Float(f)) => {
                v.as_f64().map(|x| x <= *f).unwrap_or(false)
            }
            (FilterOp::Like, Some(v), FilterValue::String(pattern)) => {
                if let Some(s) = v.as_str() {
                    // Simple LIKE matching: % = any chars, _ = single char
                    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
                    regex::Regex::new(&format!("^{}$", regex_pattern))
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            (FilterOp::In, Some(v), FilterValue::List(vals)) => {
                vals.iter().any(|fv| match (v, fv) {
                    (serde_json::Value::Number(n), FilterValue::Int(i)) => {
                        n.as_i64().map(|x| x == *i).unwrap_or(false)
                    }
                    (serde_json::Value::Number(n), FilterValue::Float(f)) => n
                        .as_f64()
                        .map(|x| (x - f).abs() < f64::EPSILON)
                        .unwrap_or(false),
                    (serde_json::Value::String(s), FilterValue::String(fs)) => s == fs,
                    (serde_json::Value::Bool(b), FilterValue::Bool(fb)) => b == fb,
                    _ => false,
                })
            }
            _ => false,
        }
    }
}

impl AggregateExpr {
    pub fn new(kind: AggKind) -> Self {
        Self {
            kind,
            alias: None,
            filter: None,
        }
    }

    pub fn with_alias(kind: AggKind, alias: String) -> Self {
        Self {
            kind,
            alias: Some(alias),
            filter: None,
        }
    }

    /// Create an aggregate with a FILTER clause
    pub fn with_filter(kind: AggKind, filter: AggregateFilter) -> Self {
        Self {
            kind,
            alias: None,
            filter: Some(filter),
        }
    }

    /// Create an aggregate with both alias and filter
    pub fn with_alias_and_filter(kind: AggKind, alias: String, filter: AggregateFilter) -> Self {
        Self {
            kind,
            alias: Some(alias),
            filter: Some(filter),
        }
    }

    /// Check if this aggregate has a filter condition
    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// Get the output column name for this aggregate
    pub fn output_name(&self) -> String {
        if let Some(ref alias) = self.alias {
            return alias.clone();
        }
        // Default names based on aggregate type
        match &self.kind {
            AggKind::CountStar => "count".to_string(),
            AggKind::CountDistinct { column } => format!("count_distinct_{}", column),
            AggKind::Sum { column } => format!("sum_{}", column),
            AggKind::Avg { column } => format!("avg_{}", column),
            AggKind::Min { column } => format!("min_{}", column),
            AggKind::Max { column } => format!("max_{}", column),
            AggKind::StddevSamp { column } => format!("stddev_{}", column),
            AggKind::StddevPop { column } => format!("stddev_pop_{}", column),
            AggKind::VarianceSamp { column } => format!("variance_{}", column),
            AggKind::VariancePop { column } => format!("var_pop_{}", column),
            AggKind::ApproxCountDistinct { column } => format!("approx_count_distinct_{}", column),
            AggKind::ApproxPercentile { column, percentile } => {
                format!(
                    "approx_percentile_{}_{}",
                    (percentile * 100.0) as i32,
                    column
                )
            }
            AggKind::ApproxMedian { column } => format!("approx_median_{}", column),
            AggKind::Median { column } => format!("median_{}", column),
            AggKind::PercentileCont { column, percentile } => {
                format!("percentile_cont_{}_{}", (percentile * 100.0) as i32, column)
            }
            AggKind::PercentileDisc { column, percentile } => {
                format!("percentile_disc_{}_{}", (percentile * 100.0) as i32, column)
            }
            AggKind::ArrayAgg { column, .. } => format!("array_agg_{}", column),
            AggKind::StringAgg { column, .. } => format!("string_agg_{}", column),
            AggKind::Mode { column } => format!("mode_{}", column),
            AggKind::StringAggOrdered { column, .. } => format!("string_agg_ordered_{}", column),
            AggKind::ArrayAggOrdered { column, .. } => format!("array_agg_ordered_{}", column),
            AggKind::NthValue { column, n } => format!("nth_value_{}_{}", n, column),
            AggKind::FirstValue { column } => format!("first_value_{}", column),
            AggKind::LastValue { column } => format!("last_value_{}", column),
        }
    }
}

/// HAVING clause condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HavingCondition {
    pub agg: AggKind,
    pub op: HavingOp,
    pub value: f64,
}

/// Comparison operators for HAVING
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HavingOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Aggregation plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggPlan {
    pub group_by: GroupBy,
    pub aggs: Vec<AggregateExpr>,
    /// HAVING clause conditions (all must be satisfied - AND logic)
    pub having: Vec<HavingCondition>,
}

/// Supported GROUP BY column identifiers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupByColumn {
    TenantId,
    RouteId,
    /// Dynamic column name for flexible grouping
    Named(String),
}

/// Group by clause type - supports single or multiple columns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupBy {
    None,
    /// Legacy single-column groupings
    Tenant,
    Route,
    /// Multiple columns grouped together (e.g., GROUP BY tenant_id, route_id)
    Columns(Vec<GroupByColumn>),
    /// GROUP BY ALL - group by all non-aggregate columns
    All,
    /// GROUPING SETS - explicit list of grouping combinations
    /// e.g., GROUPING SETS ((a, b), (a), ())
    GroupingSets(Vec<Vec<GroupByColumn>>),
    /// ROLLUP - hierarchical grouping with progressive subtotals
    /// e.g., ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
    Rollup(Vec<GroupByColumn>),
    /// CUBE - all possible grouping combinations
    /// e.g., CUBE(a, b) = GROUPING SETS ((a,b), (a), (b), ())
    Cube(Vec<GroupByColumn>),
}

/// Set operation type (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

/// A compound query with set operations (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone)]
pub struct SetOperationQuery {
    /// The type of set operation
    pub op: SetOpType,
    /// Left side query
    pub left: Box<SqlStatement>,
    /// Right side query
    pub right: Box<SqlStatement>,
}

/// Scalar function types for computed columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalarFunction {
    // String functions
    Upper(Box<SelectExpr>),
    Lower(Box<SelectExpr>),
    Length(Box<SelectExpr>),
    Trim(Box<SelectExpr>),
    LTrim(Box<SelectExpr>),
    RTrim(Box<SelectExpr>),
    Concat(Vec<SelectExpr>),
    Substring {
        expr: Box<SelectExpr>,
        start: i64,
        length: Option<i64>,
    },
    Replace {
        expr: Box<SelectExpr>,
        from: String,
        to: String,
    },
    Left {
        expr: Box<SelectExpr>,
        count: i64,
    },
    Right {
        expr: Box<SelectExpr>,
        count: i64,
    },
    Reverse(Box<SelectExpr>),
    Coalesce(Vec<SelectExpr>),

    // Regex functions
    RegexpReplace {
        expr: Box<SelectExpr>,
        pattern: String,
        replacement: String,
        flags: Option<String>,
    },
    RegexpMatch {
        expr: Box<SelectExpr>,
        pattern: String,
        flags: Option<String>,
    },
    RegexpExtract {
        expr: Box<SelectExpr>,
        pattern: String,
        group_index: Option<i64>,
    },

    // Math functions
    Abs(Box<SelectExpr>),
    Round {
        expr: Box<SelectExpr>,
        precision: Option<i32>,
    },
    Ceil(Box<SelectExpr>),
    Floor(Box<SelectExpr>),
    Mod {
        dividend: Box<SelectExpr>,
        divisor: Box<SelectExpr>,
    },
    Power {
        base: Box<SelectExpr>,
        exponent: Box<SelectExpr>,
    },
    Sqrt(Box<SelectExpr>),
    Log {
        expr: Box<SelectExpr>,
        base: Option<f64>,
    },
    Ln(Box<SelectExpr>),
    Exp(Box<SelectExpr>),
    Sign(Box<SelectExpr>),
    Greatest(Vec<SelectExpr>),
    Least(Vec<SelectExpr>),

    // Date/Time functions
    Now,
    CurrentTimestamp,
    CurrentDate,
    DateTrunc {
        unit: String,
        expr: Box<SelectExpr>,
    },
    Extract {
        field: String,
        expr: Box<SelectExpr>,
    },
    DateAdd {
        expr: Box<SelectExpr>,
        interval: i64,
        unit: String,
    },
    DateSub {
        expr: Box<SelectExpr>,
        interval: i64,
        unit: String,
    },
    DateDiff {
        unit: String,
        start: Box<SelectExpr>,
        end: Box<SelectExpr>,
    },
    ToTimestamp(Box<SelectExpr>),
    FromUnixtime(Box<SelectExpr>),
    /// Extract date portion from timestamp
    Date(Box<SelectExpr>),
    /// Convert value to string representation
    ToString(Box<SelectExpr>),

    // Type casting
    Cast {
        expr: Box<SelectExpr>,
        target_type: String,
    },

    // JSON functions
    /// Extract a value from a JSON string using a path (e.g., JSON_EXTRACT(col, '$.key'))
    JsonExtract {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Extract a scalar value from JSON as text
    JsonExtractScalar {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Extract all matching values from JSON using JSONPath (wildcards, recursive descent)
    /// Returns array of matches for paths like $.users[*].name or $..id
    JsonExtractAll {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Create a JSON array from values
    JsonArray(Vec<SelectExpr>),
    /// Create a JSON object from key-value pairs
    JsonObject(Vec<(String, SelectExpr)>),
    /// Get the type of a JSON value
    JsonType(Box<SelectExpr>),
    /// Check if a path exists in JSON
    JsonContainsPath {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Get array length for JSON arrays
    JsonArrayLength(Box<SelectExpr>),
    /// Get object keys as array
    JsonKeys(Box<SelectExpr>),
    /// Check if valid JSON
    JsonValid(Box<SelectExpr>),
    /// Pretty print JSON
    JsonPretty(Box<SelectExpr>),
    /// Check if left JSON contains right JSON (@>)
    JsonContains {
        left: Box<SelectExpr>,
        right: Box<SelectExpr>,
    },
    /// Remove a path from JSON (#-)
    JsonRemove {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Check if JSON path exists (@?)
    JsonPathExists {
        expr: Box<SelectExpr>,
        path: Box<SelectExpr>,
    },
    /// JSON path predicate match (@@)
    JsonPathMatch {
        expr: Box<SelectExpr>,
        path: Box<SelectExpr>,
    },

    // Array functions
    /// Create an array from values
    Array(Vec<SelectExpr>),
    /// Get array length
    ArrayLength(Box<SelectExpr>),
    /// Check if array contains value
    ArrayContains {
        array: Box<SelectExpr>,
        value: Box<SelectExpr>,
    },
    /// Get element at index
    ArrayElement {
        array: Box<SelectExpr>,
        index: i64,
    },
    /// Append element to array
    ArrayAppend {
        array: Box<SelectExpr>,
        element: Box<SelectExpr>,
    },
    /// Concatenate arrays
    ArrayConcat(Vec<SelectExpr>),
    /// Get distinct elements
    ArrayDistinct(Box<SelectExpr>),
    /// Join array elements into string
    ArrayJoin {
        array: Box<SelectExpr>,
        delimiter: String,
    },

    // Full-Text Search functions
    /// MATCH(column, 'search query') - full-text search with relevance scoring
    Match {
        column: Box<SelectExpr>,
        query: String,
        mode: Option<String>, // 'boolean', 'natural', 'phrase'
    },
    /// CONTAINS(column, 'word') - check if text contains term
    Contains {
        expr: Box<SelectExpr>,
        search: String,
    },
    /// FTS_RANK(column, 'query') - get relevance score for search
    FtsRank {
        column: Box<SelectExpr>,
        query: String,
    },
    /// HIGHLIGHT(column, 'query', '<b>', '</b>') - highlight matching terms
    Highlight {
        column: Box<SelectExpr>,
        query: String,
        start_tag: String,
        end_tag: String,
    },
    /// SNIPPET(column, 'query', max_length) - extract snippet with context
    Snippet {
        column: Box<SelectExpr>,
        query: String,
        max_length: i64,
    },
    /// TO_TSVECTOR(text) - convert text to searchable vector
    ToTsvector(Box<SelectExpr>),
    /// TO_TSQUERY(query) - convert query string to search query
    ToTsquery(Box<SelectExpr>),
    /// PLAINTO_TSQUERY(text) - convert plain text to search query
    PlainToTsquery(Box<SelectExpr>),

    // Geospatial functions
    /// ST_POINT(lon, lat) - create a point geometry
    StPoint {
        longitude: Box<SelectExpr>,
        latitude: Box<SelectExpr>,
    },
    /// ST_DISTANCE(geom1, geom2) - calculate distance between geometries
    StDistance {
        geom1: Box<SelectExpr>,
        geom2: Box<SelectExpr>,
    },
    /// ST_DISTANCESPHERE(point1, point2) - distance on earth in meters
    StDistanceSphere {
        point1: Box<SelectExpr>,
        point2: Box<SelectExpr>,
    },
    /// ST_CONTAINS(geom1, geom2) - check if geom1 contains geom2
    StContains {
        geom1: Box<SelectExpr>,
        geom2: Box<SelectExpr>,
    },
    /// ST_WITHIN(geom1, geom2) - check if geom1 is within geom2
    StWithin {
        geom1: Box<SelectExpr>,
        geom2: Box<SelectExpr>,
    },
    /// ST_INTERSECTS(geom1, geom2) - check if geometries intersect
    StIntersects {
        geom1: Box<SelectExpr>,
        geom2: Box<SelectExpr>,
    },
    /// ST_BUFFER(geom, distance) - create buffer around geometry
    StBuffer {
        geom: Box<SelectExpr>,
        distance: Box<SelectExpr>,
    },
    /// ST_AREA(geom) - calculate area of polygon
    StArea(Box<SelectExpr>),
    /// ST_LENGTH(geom) - calculate length of line
    StLength(Box<SelectExpr>),
    /// ST_CENTROID(geom) - calculate centroid
    StCentroid(Box<SelectExpr>),
    /// ST_ASTEXT(geom) - convert geometry to WKT string
    StAsText(Box<SelectExpr>),
    /// ST_GEOMFROMTEXT(wkt) - parse WKT string to geometry
    StGeomFromText(Box<SelectExpr>),
    /// ST_SETSRID(geom, srid) - set spatial reference ID
    StSetSrid {
        geom: Box<SelectExpr>,
        srid: i32,
    },
    /// ST_MAKEENVELOPE(xmin, ymin, xmax, ymax, srid) - create bounding box
    StMakeEnvelope {
        xmin: Box<SelectExpr>,
        ymin: Box<SelectExpr>,
        xmax: Box<SelectExpr>,
        ymax: Box<SelectExpr>,
        srid: Option<i32>,
    },

    // Vector/Similarity functions for AI/ML workloads
    /// VECTOR_DISTANCE(vec1, vec2, metric) - calculate vector distance
    VectorDistance {
        vec1: Box<SelectExpr>,
        vec2: Box<SelectExpr>,
        metric: String, // 'cosine', 'euclidean', 'dot', 'manhattan'
    },
    /// VECTOR_SIMILARITY(vec1, vec2) - cosine similarity (1 = identical, 0 = orthogonal)
    VectorSimilarity {
        vec1: Box<SelectExpr>,
        vec2: Box<SelectExpr>,
    },
    /// VECTOR_DIMS(vec) - get vector dimensions
    VectorDims(Box<SelectExpr>),
    /// VECTOR_NORM(vec) - get vector L2 norm
    VectorNorm(Box<SelectExpr>),
    /// VECTOR_NORMALIZE(vec) - normalize to unit vector
    VectorNormalize(Box<SelectExpr>),
    /// INNER_PRODUCT(vec1, vec2) - dot product
    InnerProduct {
        vec1: Box<SelectExpr>,
        vec2: Box<SelectExpr>,
    },
    /// EUCLIDEAN_DISTANCE(vec1, vec2) - L2 distance
    EuclideanDistance {
        vec1: Box<SelectExpr>,
        vec2: Box<SelectExpr>,
    },
    /// MANHATTAN_DISTANCE(vec1, vec2) - L1 distance
    ManhattanDistance {
        vec1: Box<SelectExpr>,
        vec2: Box<SelectExpr>,
    },
}

/// Window function types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    NTile(i64),
    Lag {
        expr: Box<SelectExpr>,
        offset: i64,
        default: Option<Box<SelectExpr>>,
    },
    Lead {
        expr: Box<SelectExpr>,
        offset: i64,
        default: Option<Box<SelectExpr>>,
    },
    FirstValue(Box<SelectExpr>),
    LastValue(Box<SelectExpr>),
    NthValue {
        expr: Box<SelectExpr>,
        n: i64,
    },
    // Window versions of aggregate functions
    WindowSum(Box<SelectExpr>),
    WindowAvg(Box<SelectExpr>),
    WindowMin(Box<SelectExpr>),
    WindowMax(Box<SelectExpr>),
    WindowCount(Option<Box<SelectExpr>>),
}

/// Window specification (OVER clause)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByClause>,
    pub frame: Option<WindowFrame>,
}

/// Window frame specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// Represents an expression in SELECT clause (column, function, or computed value)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectExpr {
    /// Simple column reference
    Column(String),
    /// Qualified column reference (table.column)
    QualifiedColumn { table: String, column: String },
    /// Literal value
    Literal(LiteralValue),
    /// Scalar function call
    Function(ScalarFunction),
    /// Aggregate function (for use in expressions)
    Aggregate(AggKind),
    /// Window function with OVER clause
    Window {
        function: WindowFunction,
        spec: WindowSpec,
    },
    /// Binary operation (a + b, a - b, etc.)
    BinaryOp {
        left: Box<SelectExpr>,
        op: String,
        right: Box<SelectExpr>,
    },
    /// Unary operation (-a, NOT a)
    UnaryOp { op: String, expr: Box<SelectExpr> },
    /// CASE expression
    Case {
        operand: Option<Box<SelectExpr>>,
        when_clauses: Vec<(SelectExpr, SelectExpr)>,
        else_result: Option<Box<SelectExpr>>,
    },
    /// NULL literal
    Null,
    /// Subquery (for scalar subqueries)
    Subquery(Box<ParsedQuery>),
}

/// Literal value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// Common Table Expression (CTE) definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CteDefinition {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: ParsedQuery,
    /// Whether this is a recursive CTE (WITH RECURSIVE)
    pub recursive: bool,
    /// Raw SQL query for the CTE (used for recursive execution)
    pub raw_sql: String,
}

/// Extended parsed query with CTE support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQueryWithCtes {
    pub ctes: Vec<CteDefinition>,
    pub query: ParsedQuery,
}

/// SELECT item with expression and optional alias
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectColumn {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

/// DDL command types
#[derive(Debug, Clone)]
pub enum DdlCommand {
    CreateDatabase {
        name: String,
    },
    CreateTable {
        database: String,
        table: String,
        schema_json: Option<String>,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    DropTable {
        database: String,
        table: String,
        if_exists: bool,
    },
    TruncateTable {
        database: String,
        table: String,
    },
    AlterTableAddColumn {
        database: String,
        table: String,
        column: String,
        data_type: String,
        nullable: bool,
    },
    AlterTableDropColumn {
        database: String,
        table: String,
        column: String,
    },
    /// SHOW DATABASES
    ShowDatabases,
    /// SHOW TABLES [IN database]
    ShowTables {
        database: Option<String>,
    },
    /// DESCRIBE TABLE database.table
    DescribeTable {
        database: String,
        table: String,
    },
    /// CREATE VIEW [database.]view AS SELECT ...
    CreateView {
        database: String,
        name: String,
        /// The SQL query that defines the view
        query_sql: String,
        /// If true, replace existing view with same name
        or_replace: bool,
    },
    /// DROP VIEW [IF EXISTS] [database.]view
    DropView {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// SHOW VIEWS [IN database]
    ShowViews {
        database: Option<String>,
    },
    /// DESCRIBE VIEW [database.]view
    DescribeView {
        database: String,
        view: String,
    },
    /// CREATE MATERIALIZED VIEW [database.]view AS SELECT ...
    CreateMaterializedView {
        database: String,
        name: String,
        /// The SQL query that defines the materialized view
        query_sql: String,
        /// If true, replace existing materialized view with same name
        or_replace: bool,
    },
    /// DROP MATERIALIZED VIEW [IF EXISTS] [database.]view
    DropMaterializedView {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// REFRESH MATERIALIZED VIEW [database.]view [INCREMENTAL]
    RefreshMaterializedView {
        database: String,
        name: String,
        /// If true, attempt incremental refresh; if false, full refresh
        incremental: bool,
    },
    /// SHOW MATERIALIZED VIEWS [IN database]
    ShowMaterializedViews {
        database: Option<String>,
    },
    /// CREATE INDEX [IF NOT EXISTS] index_name ON table [USING method] (columns)
    CreateIndex {
        database: String,
        table: String,
        index_name: String,
        columns: Vec<String>,
        index_type: IndexType,
        if_not_exists: bool,
    },
    /// DROP INDEX [IF EXISTS] index_name ON table
    DropIndex {
        database: String,
        table: String,
        index_name: String,
        if_exists: bool,
    },
    /// SHOW INDEXES [IN database.table]
    ShowIndexes {
        database: Option<String>,
        table: Option<String>,
    },
    /// ANALYZE TABLE database.table [COLUMNS (col1, col2, ...)] - Collect statistics
    AnalyzeTable {
        database: String,
        table: String,
        columns: Option<Vec<String>>,
    },
    /// VACUUM [FULL] [FORCE] database.table - Reclaim storage
    /// FORCE option skips missing/corrupted segments instead of failing
    Vacuum {
        database: String,
        table: String,
        full: bool,
        force: bool,
    },
    /// DEDUPLICATE database.table - Remove duplicate rows based on configured keys
    Deduplicate {
        database: String,
        table: String,
    },
    /// ALTER TABLE ... SET DEDUPLICATION (key_columns) [VERSION version_col] [MODE mode]
    SetDeduplication {
        database: String,
        table: String,
        config: Option<DeduplicationConfig>,
    },
    /// ALTER TABLE ... RENAME TO new_name
    AlterTableRename {
        database: String,
        old_table: String,
        new_table: String,
    },
    /// ALTER TABLE ... RENAME COLUMN old_name TO new_name
    AlterTableRenameColumn {
        database: String,
        table: String,
        old_column: String,
        new_column: String,
    },
    /// ALTER TABLE ... SET RETENTION retention_period [ON time_column]
    SetRetention {
        database: String,
        table: String,
        retention_seconds: u64,
        time_column: Option<String>,
    },
    /// ALTER TABLE ... DROP RETENTION
    DropRetention {
        database: String,
        table: String,
    },
    /// SHOW RETENTION [FOR database.table]
    ShowRetention {
        database: Option<String>,
        table: Option<String>,
    },
    /// ALTER TABLE ... PARTITION BY granularity ON time_column
    SetPartition {
        database: String,
        table: String,
        granularity: String,
        time_column: String,
    },
    /// ALTER TABLE ... DROP PARTITION
    DropPartition {
        database: String,
        table: String,
    },
    /// SHOW PARTITIONS [FOR database.table]
    ShowPartitions {
        database: Option<String>,
        table: Option<String>,
    },
    /// SHOW STATISTICS FOR database.table [COLUMN column_name]
    ShowStatistics {
        database: String,
        table: String,
        column: Option<String>,
    },
    /// CREATE TABLE ... AS SELECT ...
    CreateTableAs {
        database: String,
        table: String,
        query_sql: String,
        if_not_exists: bool,
    },
    /// CREATE SEQUENCE [IF NOT EXISTS] name [START WITH n] [INCREMENT BY n]
    CreateSequence {
        database: String,
        name: String,
        start: i64,
        increment: i64,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
        if_not_exists: bool,
    },
    /// DROP SEQUENCE [IF EXISTS] name
    DropSequence {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// ALTER SEQUENCE name [RESTART WITH n]
    AlterSequence {
        database: String,
        name: String,
        restart_with: Option<i64>,
        increment: Option<i64>,
    },
    /// SHOW SEQUENCES [IN database]
    ShowSequences {
        database: Option<String>,
    },
    /// COPY table FROM 'file' [WITH options]
    CopyFrom {
        database: String,
        table: String,
        source: CopySource,
        format: CopyFormat,
        options: CopyOptions,
    },
    /// COPY table TO 'file' [WITH options]
    CopyTo {
        database: String,
        table: String,
        destination: String,
        format: CopyFormat,
        options: CopyOptions,
    },
    /// ADD CONSTRAINT to table
    AddConstraint {
        database: String,
        table: String,
        constraint: TableConstraint,
    },
    /// DROP CONSTRAINT from table
    DropConstraint {
        database: String,
        table: String,
        constraint_name: String,
    },
    // --- Point-in-Time Recovery Commands ---
    /// RECOVER TO TIMESTAMP 'timestamp' - restore database to a specific point in time
    RecoverToTimestamp {
        /// Target timestamp (ISO 8601 format or Unix timestamp)
        timestamp: String,
    },
    /// RECOVER TO LSN lsn_number - restore database to a specific log sequence number
    RecoverToLsn {
        /// Target log sequence number
        lsn: u64,
    },
    /// CREATE BACKUP [label] - create a base backup for PITR
    CreateBackup {
        /// Optional label for the backup
        label: Option<String>,
    },
    /// SHOW BACKUPS - list available backups
    ShowBackups,
    /// SHOW WAL STATUS - show WAL archiving status
    ShowWalStatus,
    /// DELETE BACKUP backup_id - remove a backup
    DeleteBackup {
        /// Backup ID to delete
        backup_id: String,
    },
    /// SHOW SERVER INFO - Display server version and status
    ShowServerInfo,
    /// SHOW MISSING SEGMENTS [FROM database.table] - Find segments missing from disk
    ShowMissingSegments {
        database: Option<String>,
        table: Option<String>,
    },
    /// SHOW CORRUPTED SEGMENTS [FROM database.table] - Find segments with checksum mismatches
    ShowCorruptedSegments {
        database: Option<String>,
        table: Option<String>,
    },
    /// SHOW DAMAGED SEGMENTS [FROM database.table] - Find all damaged segments (missing + corrupted)
    ShowDamagedSegments {
        database: Option<String>,
        table: Option<String>,
    },
    /// REPAIR SEGMENTS database.table | database.* | ALL - Remove missing and corrupted segments from manifest
    RepairSegments {
        /// None = all databases, Some = specific database
        database: Option<String>,
        /// None = all tables in database(s), Some = specific table
        table: Option<String>,
    },
    /// CHECK MANIFEST - Verify manifest integrity and report issues
    CheckManifest,
    /// COMPACT TABLE database.table - Manually trigger compaction on a table
    CompactTable {
        database: String,
        table: String,
    },
    /// COMPACT ALL - Manually trigger compaction on all tables
    CompactAll,
    /// BEGIN [TRANSACTION] - Start a new transaction
    BeginTransaction,
    /// COMMIT [TRANSACTION] - Commit the current transaction
    CommitTransaction,
    /// ROLLBACK [TRANSACTION] - Rollback the current transaction
    RollbackTransaction,
    /// SAVEPOINT name - Create a savepoint within a transaction
    Savepoint {
        name: String,
    },
    /// RELEASE SAVEPOINT name - Release a savepoint
    ReleaseSavepoint {
        name: String,
    },
    /// ROLLBACK TO SAVEPOINT name - Rollback to a savepoint
    RollbackToSavepoint {
        name: String,
    },

    // User-Defined Functions (UDFs)
    /// CREATE FUNCTION name(args) RETURNS type AS 'body'
    CreateFunction {
        name: String,
        parameters: Vec<FunctionParameter>,
        return_type: String,
        body: FunctionBody,
        or_replace: bool,
        language: Option<String>, // SQL, expression
    },
    /// DROP FUNCTION [IF EXISTS] name
    DropFunction {
        name: String,
        if_exists: bool,
    },
    /// SHOW FUNCTIONS [LIKE pattern]
    ShowFunctions {
        pattern: Option<String>,
    },

    /// CREATE [OR REPLACE] PROCEDURE name(params) LANGUAGE lang AS $$ body $$
    CreateProcedure {
        name: String,
        parameters: Vec<FunctionParameter>,
        body: String,
        or_replace: bool,
        language: Option<String>,
    },
    /// DROP PROCEDURE [IF EXISTS] name
    DropProcedure {
        name: String,
        if_exists: bool,
    },
    /// CALL procedure_name(args)
    CallProcedure {
        name: String,
        arguments: Vec<String>,
    },
    /// SHOW PROCEDURES [LIKE pattern]
    ShowProcedures {
        pattern: Option<String>,
    },

    // Foreign Data Wrapper commands
    /// CREATE FOREIGN DATA WRAPPER name OPTIONS (...)
    CreateForeignDataWrapper {
        name: String,
        handler: Option<String>,
        validator: Option<String>,
        options: Vec<(String, String)>,
    },
    /// DROP FOREIGN DATA WRAPPER [IF EXISTS] name
    DropForeignDataWrapper {
        name: String,
        if_exists: bool,
    },
    /// CREATE SERVER name FOREIGN DATA WRAPPER wrapper OPTIONS (...)
    CreateForeignServer {
        name: String,
        wrapper_name: String,
        server_type: Option<String>,
        version: Option<String>,
        options: Vec<(String, String)>,
    },
    /// DROP SERVER [IF EXISTS] name
    DropForeignServer {
        name: String,
        if_exists: bool,
    },
    /// CREATE USER MAPPING FOR user SERVER server OPTIONS (...)
    CreateUserMapping {
        local_user: String,
        server_name: String,
        remote_user: Option<String>,
        options: Vec<(String, String)>,
    },
    /// DROP USER MAPPING [IF EXISTS] FOR user SERVER server
    DropUserMapping {
        local_user: String,
        server_name: String,
        if_exists: bool,
    },
    /// CREATE FOREIGN TABLE name (...) SERVER server
    CreateForeignTable {
        name: String,
        server_name: String,
        columns: Vec<ForeignTableColumn>,
        options: Vec<(String, String)>,
    },
    /// DROP FOREIGN TABLE [IF EXISTS] name
    DropForeignTable {
        name: String,
        if_exists: bool,
    },
    /// IMPORT FOREIGN SCHEMA schema FROM SERVER server INTO database
    ImportForeignSchema {
        remote_schema: String,
        server_name: String,
        local_database: String,
        limit_to: Option<Vec<String>>,
    },
    /// SHOW FOREIGN DATA WRAPPERS
    ShowForeignDataWrappers,
    /// SHOW FOREIGN SERVERS
    ShowForeignServers,
    /// SHOW FOREIGN TABLES
    ShowForeignTables,

    // Streaming/CDC commands
    /// CREATE STREAM name FROM KAFKA 'topic' WITH (options)
    CreateStream {
        name: String,
        source_type: StreamSourceType,
        source_config: String, // JSON config
        target_table: Option<String>,
        format: Option<String>,
    },
    /// DROP STREAM [IF EXISTS] name
    DropStream {
        name: String,
        if_exists: bool,
    },
    /// SHOW STREAMS
    ShowStreams,
    /// START STREAM name
    StartStream {
        name: String,
    },
    /// STOP STREAM name
    StopStream {
        name: String,
    },
    /// SHOW STREAM STATUS name
    ShowStreamStatus {
        name: String,
    },
    /// SHOW REPAIR STATUS - Display auto-repair status and statistics
    ShowRepairStatus,

    // ============================================================================
    // Column-Level Encryption Commands
    // ============================================================================
    /// CREATE ENCRYPTION KEY key_name [ALGORITHM AES256GCM|CHACHA20] [EXPIRES timestamp]
    CreateEncryptionKey {
        key_name: String,
        algorithm: EncryptionAlgorithmType,
        expires_at: Option<u64>,
    },
    /// DROP ENCRYPTION KEY [IF EXISTS] key_name
    DropEncryptionKey {
        key_name: String,
        if_exists: bool,
    },
    /// ROTATE ENCRYPTION KEY key_name
    RotateEncryptionKey {
        key_name: String,
    },
    /// SHOW ENCRYPTION KEYS
    ShowEncryptionKeys,
    /// ALTER TABLE ... ENCRYPT COLUMN column_name WITH KEY key_name
    EncryptColumn {
        database: String,
        table: String,
        column: String,
        key_name: String,
        algorithm: Option<EncryptionAlgorithmType>,
    },
    /// ALTER TABLE ... DECRYPT COLUMN column_name
    DecryptColumn {
        database: String,
        table: String,
        column: String,
    },
    /// SHOW ENCRYPTED COLUMNS [FROM database.table]
    ShowEncryptedColumns {
        database: Option<String>,
        table: Option<String>,
    },

    // ============================================================================
    // Change Data Capture (CDC) Commands
    // ============================================================================
    /// CREATE CDC SUBSCRIPTION name ON database.table [TO target] [WITH options]
    CreateCdcSubscription {
        name: String,
        database: String,
        table: String,
        target_type: CdcTargetType,
        target_config: Option<String>,
        include_before: bool,
    },
    /// DROP CDC SUBSCRIPTION [IF EXISTS] name
    DropCdcSubscription {
        name: String,
        if_exists: bool,
    },
    /// START CDC SUBSCRIPTION name
    StartCdcSubscription {
        name: String,
    },
    /// STOP CDC SUBSCRIPTION name
    StopCdcSubscription {
        name: String,
    },
    /// SHOW CDC SUBSCRIPTIONS
    ShowCdcSubscriptions,
    /// SHOW CDC STATUS name
    ShowCdcStatus {
        name: String,
    },
    /// GET CHANGES FROM database.table [SINCE sequence] [LIMIT n]
    GetChanges {
        database: String,
        table: String,
        since_sequence: Option<u64>,
        limit: Option<usize>,
    },
    /// SET CDC CHECKPOINT name TO sequence
    SetCdcCheckpoint {
        name: String,
        sequence: u64,
    },

    // ============================================================================
    // Session & Server Management Commands (PostgreSQL/MySQL compatible)
    // ============================================================================
    /// SET variable = value - Set session variable
    SetVariable {
        name: String,
        value: String,
        /// LOCAL = transaction-scoped, SESSION = session-scoped (default)
        scope: VariableScope,
    },
    /// SHOW variable - Show a session variable value
    ShowVariable {
        name: String,
    },
    /// SHOW VARIABLES [LIKE pattern] - Show all session variables
    ShowVariables {
        pattern: Option<String>,
    },
    /// SHOW STATUS [LIKE pattern] - Show server status counters
    ShowStatus {
        pattern: Option<String>,
    },
    /// SHOW PROCESSLIST / SHOW FULL PROCESSLIST - Show running queries
    ShowProcesslist {
        full: bool,
    },
    /// KILL connection_id - Terminate a connection
    KillConnection {
        connection_id: u64,
    },
    /// KILL QUERY query_id - Terminate a specific query
    KillQuery {
        query_id: u64,
    },
    /// COMMENT ON TABLE database.table IS 'comment'
    CommentOnTable {
        database: String,
        table: String,
        comment: Option<String>,
    },
    /// COMMENT ON COLUMN database.table.column IS 'comment'
    CommentOnColumn {
        database: String,
        table: String,
        column: String,
        comment: Option<String>,
    },
    /// COMMENT ON DATABASE database IS 'comment'
    CommentOnDatabase {
        database: String,
        comment: Option<String>,
    },
    /// CLUSTER [VERBOSE] table USING index_name - Reorder table by index
    ClusterTable {
        database: String,
        table: String,
        index_name: String,
        verbose: bool,
    },
    /// CLUSTER - Cluster all previously-clustered tables
    ClusterAll,
    /// REINDEX TABLE database.table - Rebuild all indexes on a table
    ReindexTable {
        database: String,
        table: String,
    },
    /// REINDEX INDEX index_name - Rebuild a specific index
    ReindexIndex {
        database: String,
        table: String,
        index_name: String,
    },
    /// REINDEX DATABASE database_name - Rebuild all indexes in a database
    ReindexDatabase {
        database: String,
    },
    /// LOCK TABLES table1 [READ|WRITE], table2 [READ|WRITE], ...
    LockTables {
        locks: Vec<TableLock>,
    },
    /// UNLOCK TABLES - Release all table locks
    UnlockTables,
    /// OPTIMIZE TABLE database.table - Reclaim space and defragment
    OptimizeTable {
        database: String,
        table: String,
    },
    /// FLUSH TABLES - Close all open tables
    FlushTables,
    /// FLUSH PRIVILEGES - Reload privilege tables
    FlushPrivileges,
    /// RESET QUERY CACHE - Clear the query cache
    ResetQueryCache,
    /// SHOW TABLE STATUS [FROM database] [LIKE pattern]
    ShowTableStatus {
        database: Option<String>,
        pattern: Option<String>,
    },
    /// SHOW CREATE TABLE database.table - Show CREATE TABLE statement
    ShowCreateTable {
        database: String,
        table: String,
    },
    /// SHOW CREATE VIEW database.view - Show CREATE VIEW statement
    ShowCreateView {
        database: String,
        view: String,
    },
    /// SHOW CREATE DATABASE database_name
    ShowCreateDatabase {
        database: String,
    },
    /// SHOW COLUMNS FROM database.table [LIKE pattern]
    ShowColumns {
        database: String,
        table: String,
        pattern: Option<String>,
    },
    /// SHOW WARNINGS - Show warnings from last statement
    ShowWarnings,
    /// SHOW ERRORS - Show errors from last statement
    ShowErrors,
    /// SHOW ENGINE STATUS - Show storage engine status
    ShowEngineStatus,
    /// CHECKSUM TABLE database.table - Calculate table checksum
    ChecksumTable {
        database: String,
        table: String,
    },
    /// CHECK TABLE database.table - Check table for errors
    CheckTable {
        database: String,
        table: String,
    },
    /// REPAIR TABLE database.table - Repair a corrupted table
    RepairTable {
        database: String,
        table: String,
    },
    // ============================================================================
    // Trigger Commands
    // ============================================================================
    /// CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE ON table FOR EACH ROW EXECUTE ...
    CreateTrigger {
        name: String,
        database: String,
        table: String,
        timing: TriggerTiming,
        events: Vec<TriggerEventType>,
        for_each_row: bool,
        when_condition: Option<String>,
        function_name: Option<String>,
        function_body: Option<String>,
        or_replace: bool,
    },
    /// DROP TRIGGER [IF EXISTS] name ON table
    DropTrigger {
        name: String,
        database: String,
        table: String,
        if_exists: bool,
    },
    /// ALTER TRIGGER name ON table ENABLE|DISABLE
    AlterTrigger {
        name: String,
        database: String,
        table: String,
        enable: bool,
    },
    /// SHOW TRIGGERS [FROM database.table]
    ShowTriggers {
        database: Option<String>,
        table: Option<String>,
    },

    // ============================================================================
    // ENUM Type Commands
    // ============================================================================
    /// CREATE TYPE name AS ENUM ('val1', 'val2', ...)
    CreateEnumType {
        database: String,
        name: String,
        values: Vec<String>,
        if_not_exists: bool,
    },
    /// DROP TYPE [IF EXISTS] name
    DropEnumType {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// ALTER TYPE name ADD VALUE 'new_value' [BEFORE|AFTER 'existing']
    AlterEnumType {
        database: String,
        name: String,
        new_value: String,
        position: Option<EnumValuePosition>,
    },
    /// SHOW TYPES [IN database]
    ShowTypes {
        database: Option<String>,
    },
}

/// Trigger timing
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event type
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerEventType {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Variable scope for SET commands
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum VariableScope {
    /// Session-scoped (default)
    #[default]
    Session,
    /// Transaction-scoped (PostgreSQL LOCAL)
    Local,
    /// Global (affects all sessions)
    Global,
}

/// Position for adding ENUM values
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EnumValuePosition {
    /// Add before an existing value
    Before(String),
    /// Add after an existing value
    After(String),
}

/// Table lock mode for LOCK TABLES
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableLock {
    pub database: String,
    pub table: String,
    pub mode: LockMode,
}

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum LockMode {
    /// READ lock - allows concurrent reads
    #[default]
    Read,
    /// WRITE lock - exclusive lock
    Write,
    /// READ LOCAL - allows concurrent inserts (MySQL-specific)
    ReadLocal,
    /// LOW PRIORITY WRITE (MySQL-specific)
    LowPriorityWrite,
}

/// Encryption algorithm types for column encryption
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum EncryptionAlgorithmType {
    /// AES-256-GCM (default, authenticated encryption)
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305
    ChaCha20Poly1305,
    /// Deterministic AES-256 (allows equality search but less secure)
    DeterministicAes256,
}

/// CDC target types
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum CdcTargetType {
    /// In-memory buffer (for polling via GET CHANGES)
    #[default]
    Buffer,
    /// Kafka topic
    Kafka,
    /// Webhook URL
    Webhook,
    /// Another BoyoDB table
    Table,
}

/// Function parameter definition
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FunctionParameter {
    pub name: String,
    pub data_type: String,
    pub default_value: Option<String>,
}

/// Foreign table column definition
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForeignTableColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub options: Vec<(String, String)>,
}

/// Function body - can be SQL expression or reference to external code
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FunctionBody {
    /// SQL expression body
    SqlExpression(String),
    /// SQL query body (for table functions)
    SqlQuery(String),
    /// External reference (for future extension)
    External { library: String, symbol: String },
}

/// Stream source types for CDC/streaming
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StreamSourceType {
    Kafka,
    Pulsar,
    Kinesis,
    /// File-based streaming (for testing)
    File,
}

/// Index type for CREATE INDEX
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexType {
    /// B-tree index (default) - good for range queries
    BTree,
    /// Hash index - good for equality queries
    Hash,
    /// Bloom filter index - good for membership testing
    Bloom,
    /// Bitmap index - good for low-cardinality columns
    Bitmap,
    /// Full-text search index - good for text search with tokenization
    Fulltext,
    /// HNSW index - approximate nearest neighbor for vector search
    Hnsw,
    /// GiST index - generalized search tree for spatial data
    Gist,
}

/// Deduplication configuration for a table
/// Supports merge-on-read deduplication with immediate dedup option
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeduplicationConfig {
    /// Columns that form the deduplication key (like a primary key)
    pub key_columns: Vec<String>,
    /// Optional version column - when duplicates found, keep row with highest version
    pub version_column: Option<String>,
    /// When to perform deduplication
    pub mode: DeduplicationMode,
}

/// When to perform deduplication
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeduplicationMode {
    /// Deduplicate immediately on ingest (slower ingest, faster queries)
    OnIngest,
    /// Deduplicate during compaction only (faster ingest, may have temp duplicates)
    OnCompaction,
    /// Deduplicate both on ingest and compaction (balanced)
    Both,
}

impl Default for DeduplicationMode {
    fn default() -> Self {
        DeduplicationMode::OnCompaction
    }
}

/// Source for COPY FROM command
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CopySource {
    /// File path
    File(String),
    /// STDIN (for streaming data)
    Stdin,
    /// Program to execute (e.g., PROGRAM 'gzip -d < file.gz')
    Program(String),
}

/// Format for COPY command
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CopyFormat {
    /// CSV format
    Csv,
    /// Tab-separated values
    Text,
    /// Binary format
    Binary,
    /// JSON lines format
    Json,
    /// Parquet format
    Parquet,
    /// Arrow IPC format
    Arrow,
}

impl Default for CopyFormat {
    fn default() -> Self {
        CopyFormat::Csv
    }
}

/// Options for COPY command
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CopyOptions {
    /// Column delimiter (default: comma for CSV, tab for TEXT)
    pub delimiter: Option<char>,
    /// Quote character (default: double quote)
    pub quote: Option<char>,
    /// Escape character
    pub escape: Option<char>,
    /// Whether the file has a header row
    pub header: bool,
    /// NULL string representation (default: empty string)
    pub null_string: Option<String>,
    /// Encoding (default: UTF8)
    pub encoding: Option<String>,
    /// Columns to copy (if not all)
    pub columns: Option<Vec<String>>,
    /// Compression type (gzip, zstd, none)
    pub compression: Option<String>,
}

/// Authentication/Authorization command types
#[derive(Debug, Clone)]
pub enum AuthCommand {
    /// CREATE USER username WITH PASSWORD 'password' [options]
    CreateUser {
        username: String,
        password: String,
        options: UserOptions,
    },
    /// DROP USER username
    DropUser { username: String },
    /// ALTER USER username SET PASSWORD 'newpassword'
    AlterUserPassword {
        username: String,
        new_password: String,
    },
    /// ALTER USER username SET SUPERUSER [TRUE|FALSE]
    AlterUserSuperuser {
        username: String,
        is_superuser: bool,
    },
    /// ALTER USER username SET DEFAULT DATABASE 'dbname'
    AlterUserDefaultDb {
        username: String,
        database: Option<String>,
    },
    /// LOCK USER username
    LockUser { username: String },
    /// UNLOCK USER username
    UnlockUser { username: String },
    /// GRANT privilege ON target TO user/role [WITH GRANT OPTION]
    Grant {
        privileges: Vec<String>,
        target_type: GrantTargetType,
        target_name: Option<String>,
        /// Column-level grant: list of columns for column-level privileges
        columns: Option<Vec<String>>,
        grantee: String,
        grantee_is_role: bool,
        with_grant_option: bool,
    },
    /// REVOKE privilege ON target FROM user/role
    Revoke {
        privileges: Vec<String>,
        target_type: GrantTargetType,
        target_name: Option<String>,
        /// Column-level revoke: list of columns for column-level privileges
        columns: Option<Vec<String>>,
        grantee: String,
        grantee_is_role: bool,
    },
    /// GRANT role TO user
    GrantRole { role: String, username: String },
    /// REVOKE role FROM user
    RevokeRole { role: String, username: String },
    /// CREATE ROLE rolename [WITH description]
    CreateRole {
        name: String,
        description: Option<String>,
    },
    /// DROP ROLE rolename
    DropRole { name: String },
    /// SHOW USERS
    ShowUsers,
    /// SHOW ROLES
    ShowRoles,
    /// SHOW GRANTS FOR user
    ShowGrants { username: String },
}

/// User creation options
#[derive(Debug, Clone, Default)]
pub struct UserOptions {
    pub superuser: bool,
    pub default_database: Option<String>,
    pub connection_limit: Option<u32>,
}

/// Target type for GRANT/REVOKE
#[derive(Debug, Clone)]
pub enum GrantTargetType {
    /// All objects (no specific target)
    Global,
    /// DATABASE dbname
    Database,
    /// TABLE dbname.tablename or ALL TABLES IN DATABASE dbname
    Table,
    /// ALL TABLES IN DATABASE dbname
    AllTablesInDatabase,
}

/// Parse a SQL statement and return a parsed query or DDL command
pub fn parse_sql(sql: &str) -> Result<SqlStatement, EngineError> {
    // First, try to parse as an auth command (custom syntax)
    if let Some(auth_cmd) = try_parse_auth_command(sql)? {
        return Ok(SqlStatement::Auth(auth_cmd));
    }

    // Try to parse pub/sub commands (LISTEN, UNLISTEN, NOTIFY)
    if let Some(pubsub_cmd) = try_parse_pubsub_command(sql)? {
        return Ok(SqlStatement::PubSub(pubsub_cmd));
    }

    // Try to parse SHOW commands (SHOW DATABASES, SHOW TABLES)
    if let Some(ddl_cmd) = try_parse_show_command(sql)? {
        return Ok(SqlStatement::Ddl(ddl_cmd));
    }

    // Extract TABLESAMPLE clause before parsing (sqlparser doesn't support it)
    let (sample_clause, cleaned_sql) = extract_tablesample(sql);

    let dialect = PostgreSqlDialect {};
    let statements = match Parser::parse_sql(&dialect, &cleaned_sql) {
        Ok(s) => s,
        Err(e) => {
            if let Some(stmt) = parse_drop_database_fallback(&cleaned_sql) {
                return Ok(stmt);
            }
            return Err(EngineError::InvalidArgument(format!(
                "SQL parse error: {e}"
            )));
        }
    };

    if statements.is_empty() {
        return Err(EngineError::InvalidArgument("empty SQL statement".into()));
    }

    if statements.len() > 1 {
        return Err(EngineError::InvalidArgument(
            "multiple SQL statements not supported".into(),
        ));
    }

    let mut result = parse_statement(&statements[0])?;

    // Apply TABLESAMPLE clause to parsed query if present
    if let Some(sample) = sample_clause {
        if let SqlStatement::Query(ref mut query) = result {
            query.sample = Some(sample);
        }
    }

    Ok(result)
}

/// Try to parse SHOW DATABASES, SHOW TABLES, or DESCRIBE commands
fn try_parse_show_command(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let upper_trimmed = upper.trim_end_matches(';');

    // SHOW DATABASES
    if upper_trimmed == "SHOW DATABASES" {
        return Ok(Some(DdlCommand::ShowDatabases));
    }

    // SHOW TABLES [IN database]
    if upper_trimmed == "SHOW TABLES" {
        return Ok(Some(DdlCommand::ShowTables { database: None }));
    }

    // SHOW TABLES IN database_name
    if upper_trimmed.starts_with("SHOW TABLES IN ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowTables {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // SHOW TABLES FROM database_name (MySQL-style alias)
    if upper_trimmed.starts_with("SHOW TABLES FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowTables {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // DESCRIBE VIEW database.view or DESC VIEW database.view
    if upper_trimmed.starts_with("DESCRIBE VIEW ") || upper_trimmed.starts_with("DESC VIEW ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3 {
            let view_name = tokens[2].trim_end_matches(';');
            let (database, view) = parse_table_name(view_name)?;
            return Ok(Some(DdlCommand::DescribeView { database, view }));
        }
    }

    // DESCRIBE TABLE database.table or DESC database.table
    if upper_trimmed.starts_with("DESCRIBE TABLE ") || upper_trimmed.starts_with("DESC TABLE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3 {
            let table_name = tokens[2].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // DESCRIBE database.table (without TABLE keyword) - but not VIEW
    if upper_trimmed.starts_with("DESCRIBE ")
        && !upper_trimmed.starts_with("DESCRIBE TABLE ")
        && !upper_trimmed.starts_with("DESCRIBE VIEW ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 2 {
            let table_name = tokens[1].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // DESC database.table (shorthand without TABLE keyword) - but not VIEW
    if upper_trimmed.starts_with("DESC ")
        && !upper_trimmed.starts_with("DESC TABLE ")
        && !upper_trimmed.starts_with("DESC VIEW ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 2 {
            let table_name = tokens[1].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // SHOW VIEWS [IN database]
    if upper_trimmed == "SHOW VIEWS" {
        return Ok(Some(DdlCommand::ShowViews { database: None }));
    }

    if upper_trimmed.starts_with("SHOW VIEWS IN ") || upper_trimmed.starts_with("SHOW VIEWS FROM ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowViews {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // SHOW MATERIALIZED VIEWS [IN database]
    if upper_trimmed == "SHOW MATERIALIZED VIEWS" {
        return Ok(Some(DdlCommand::ShowMaterializedViews { database: None }));
    }

    if upper_trimmed.starts_with("SHOW MATERIALIZED VIEWS IN ")
        || upper_trimmed.starts_with("SHOW MATERIALIZED VIEWS FROM ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 5 {
            let db_name = tokens[4].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowMaterializedViews {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // CREATE MATERIALIZED VIEW [OR REPLACE] [database.]name AS SELECT ...
    if upper_trimmed.starts_with("CREATE MATERIALIZED VIEW ")
        || upper_trimmed.starts_with("CREATE OR REPLACE MATERIALIZED VIEW ")
    {
        return parse_create_materialized_view(sql);
    }

    // DROP MATERIALIZED VIEW [IF EXISTS] [database.]name
    if upper_trimmed.starts_with("DROP MATERIALIZED VIEW ") {
        return parse_drop_materialized_view(sql);
    }

    // REFRESH MATERIALIZED VIEW [database.]name
    if upper_trimmed.starts_with("REFRESH MATERIALIZED VIEW ") {
        return parse_refresh_materialized_view(sql);
    }

    // SHOW INDEXES [IN database.table]
    if upper_trimmed == "SHOW INDEXES" || upper_trimmed == "SHOW INDEX" {
        return Ok(Some(DdlCommand::ShowIndexes {
            database: None,
            table: None,
        }));
    }

    if upper_trimmed.starts_with("SHOW INDEXES IN ")
        || upper_trimmed.starts_with("SHOW INDEX IN ")
        || upper_trimmed.starts_with("SHOW INDEXES FROM ")
        || upper_trimmed.starts_with("SHOW INDEX FROM ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let name = tokens[3].trim_end_matches(';');
            let (database, table) = parse_table_name(name)?;
            return Ok(Some(DdlCommand::ShowIndexes {
                database: Some(database),
                table: Some(table),
            }));
        }
    }

    // VACUUM [FULL] [FORCE] database.table
    if upper_trimmed.starts_with("VACUUM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(EngineError::InvalidArgument(
                "VACUUM requires table name".into(),
            ));
        }

        // Parse FULL and FORCE options (can appear in any order)
        let mut full = false;
        let mut force = false;
        let mut table_idx = 1;

        while table_idx < tokens.len() {
            let token_upper = tokens[table_idx].to_uppercase();
            if token_upper == "FULL" {
                full = true;
                table_idx += 1;
            } else if token_upper == "FORCE" {
                force = true;
                table_idx += 1;
            } else {
                break;
            }
        }

        if table_idx >= tokens.len() {
            return Err(EngineError::InvalidArgument(
                "VACUUM requires table name".into(),
            ));
        }

        let table_name = tokens[table_idx].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::Vacuum {
            database,
            table,
            full,
            force,
        }));
    }

    // DEDUPLICATE database.table - Run deduplication on a table
    if upper_trimmed.starts_with("DEDUPLICATE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(EngineError::InvalidArgument(
                "DEDUPLICATE requires table name".into(),
            ));
        }
        let table_name = tokens[1].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::Deduplicate { database, table }));
    }

    // ALTER TABLE ... SET DEDUPLICATION (key_columns) [VERSION col] [MODE mode]
    // or ALTER TABLE ... DROP DEDUPLICATION
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains("DEDUPLICATION") {
        return parse_set_deduplication(sql);
    }

    // ALTER TABLE ... SET RETENTION <duration> [ON column]
    // or ALTER TABLE ... DROP RETENTION
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains("RETENTION") {
        return parse_set_retention(sql);
    }

    // SHOW RETENTION [FOR database.table]
    if upper_trimmed.starts_with("SHOW RETENTION") {
        return parse_show_retention(sql);
    }

    // ALTER TABLE ... PARTITION BY granularity ON time_column
    // or ALTER TABLE ... DROP PARTITION
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains("PARTITION") {
        return parse_partition_command(sql);
    }

    // SHOW PARTITIONS [FOR database.table]
    if upper_trimmed.starts_with("SHOW PARTITIONS") {
        return parse_show_partitions(sql);
    }

    // ANALYZE TABLE database.table [COLUMNS (col1, col2, ...)]
    if upper_trimmed.starts_with("ANALYZE TABLE") || upper_trimmed.starts_with("ANALYZE ") {
        return parse_analyze_table(sql);
    }

    // SHOW STATISTICS FOR database.table [COLUMN column_name]
    if upper_trimmed.starts_with("SHOW STATISTICS") || upper_trimmed.starts_with("SHOW STATS") {
        return parse_show_statistics(sql);
    }

    // CREATE BACKUP ['label'] - Create an online backup
    if upper_trimmed.starts_with("CREATE BACKUP") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let label = if tokens.len() > 2 {
            // Label is the third token, optionally quoted
            Some(
                tokens[2]
                    .trim_end_matches(';')
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string(),
            )
        } else {
            None
        };
        return Ok(Some(DdlCommand::CreateBackup { label }));
    }

    // SHOW BACKUPS - List all available backups
    if upper_trimmed == "SHOW BACKUPS" || upper_trimmed == "SHOW BACKUPS;" {
        return Ok(Some(DdlCommand::ShowBackups));
    }

    // SHOW WAL STATUS - Show WAL archiving status
    if upper_trimmed == "SHOW WAL STATUS" || upper_trimmed == "SHOW WAL STATUS;" {
        return Ok(Some(DdlCommand::ShowWalStatus));
    }

    // DELETE BACKUP backup_id - Delete a specific backup
    if upper_trimmed.starts_with("DELETE BACKUP ") || upper_trimmed.starts_with("DROP BACKUP ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 3 {
            return Err(EngineError::InvalidArgument(
                "DELETE BACKUP requires backup_id".into(),
            ));
        }
        let backup_id = tokens[2]
            .trim_end_matches(';')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        return Ok(Some(DdlCommand::DeleteBackup { backup_id }));
    }

    // RECOVER TO TIMESTAMP 'timestamp' - Point-in-time recovery
    if upper_trimmed.starts_with("RECOVER TO TIMESTAMP ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "RECOVER TO TIMESTAMP requires timestamp value".into(),
            ));
        }
        // Join remaining tokens for timestamp (may contain spaces)
        let timestamp = tokens[3..]
            .join(" ")
            .trim_end_matches(';')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        return Ok(Some(DdlCommand::RecoverToTimestamp { timestamp }));
    }

    // RECOVER TO LSN lsn_number - Recover to specific log sequence number
    if upper_trimmed.starts_with("RECOVER TO LSN ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "RECOVER TO LSN requires LSN value".into(),
            ));
        }
        let lsn: u64 = tokens[3]
            .trim_end_matches(';')
            .parse()
            .map_err(|_| EngineError::InvalidArgument("Invalid LSN value".into()))?;
        return Ok(Some(DdlCommand::RecoverToLsn { lsn }));
    }

    // SHOW SERVER INFO - Display server version and status
    if upper_trimmed == "SHOW SERVER INFO" {
        return Ok(Some(DdlCommand::ShowServerInfo));
    }

    // SHOW MISSING SEGMENTS [FROM database.table] - Find segments missing from disk
    if upper_trimmed == "SHOW MISSING SEGMENTS" {
        return Ok(Some(DdlCommand::ShowMissingSegments {
            database: None,
            table: None,
        }));
    }
    if upper_trimmed.starts_with("SHOW MISSING SEGMENTS FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 5 {
            let table_name = tokens[4].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::ShowMissingSegments {
                database: Some(database),
                table: Some(table),
            }));
        }
    }

    // SHOW CORRUPTED SEGMENTS [FROM database.table] - Find segments with checksum mismatches
    if upper_trimmed == "SHOW CORRUPTED SEGMENTS" {
        return Ok(Some(DdlCommand::ShowCorruptedSegments {
            database: None,
            table: None,
        }));
    }
    if upper_trimmed.starts_with("SHOW CORRUPTED SEGMENTS FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 5 {
            let table_name = tokens[4].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::ShowCorruptedSegments {
                database: Some(database),
                table: Some(table),
            }));
        }
    }

    // SHOW DAMAGED SEGMENTS [FROM database.table] - Find all damaged segments (missing + corrupted)
    if upper_trimmed == "SHOW DAMAGED SEGMENTS" {
        return Ok(Some(DdlCommand::ShowDamagedSegments {
            database: None,
            table: None,
        }));
    }
    if upper_trimmed.starts_with("SHOW DAMAGED SEGMENTS FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 5 {
            let table_name = tokens[4].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::ShowDamagedSegments {
                database: Some(database),
                table: Some(table),
            }));
        }
    }

    // SHOW REPAIR STATUS - Display auto-repair status and statistics
    if upper_trimmed == "SHOW REPAIR STATUS" {
        return Ok(Some(DdlCommand::ShowRepairStatus));
    }

    // REPAIR ALL SEGMENTS - Repair all segments in all databases
    if upper_trimmed == "REPAIR ALL SEGMENTS" {
        return Ok(Some(DdlCommand::RepairSegments {
            database: None,
            table: None,
        }));
    }

    // REPAIR SEGMENTS [FROM] database.* or database.table - Remove missing and corrupted segments
    if upper_trimmed.starts_with("REPAIR SEGMENTS ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        // Support both "REPAIR SEGMENTS db.table" and "REPAIR SEGMENTS FROM db.table"
        let table_idx = if tokens.len() >= 4 && tokens[2].to_uppercase() == "FROM" {
            3
        } else {
            2
        };
        if tokens.len() > table_idx {
            let table_name = tokens[table_idx].trim_end_matches(';');
            // Check for wildcard: database.*
            if table_name.ends_with(".*") {
                let db = table_name.trim_end_matches(".*");
                let db = db.trim_matches('"').trim_matches('`');
                return Ok(Some(DdlCommand::RepairSegments {
                    database: Some(db.to_string()),
                    table: None,
                }));
            }
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::RepairSegments {
                database: Some(database),
                table: Some(table),
            }));
        }
        return Err(EngineError::InvalidArgument(
            "REPAIR SEGMENTS requires database.table, database.*, or use REPAIR ALL SEGMENTS"
                .into(),
        ));
    }

    // CHECK MANIFEST - Verify manifest integrity
    if upper_trimmed == "CHECK MANIFEST" {
        return Ok(Some(DdlCommand::CheckManifest));
    }

    // COMPACT ALL - Compact all tables
    if upper_trimmed == "COMPACT ALL" {
        return Ok(Some(DdlCommand::CompactAll));
    }

    // COMPACT TABLE database.table - Manually trigger compaction
    if upper_trimmed.starts_with("COMPACT TABLE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3 {
            let table_name = tokens[2].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::CompactTable { database, table }));
        }
        return Err(EngineError::InvalidArgument(
            "COMPACT TABLE requires database.table".into(),
        ));
    }

    // BEGIN [TRANSACTION] - Start a transaction
    if upper_trimmed == "BEGIN"
        || upper_trimmed == "BEGIN TRANSACTION"
        || upper_trimmed == "START TRANSACTION"
    {
        return Ok(Some(DdlCommand::BeginTransaction));
    }

    // COMMIT [TRANSACTION] - Commit a transaction
    if upper_trimmed == "COMMIT"
        || upper_trimmed == "COMMIT TRANSACTION"
        || upper_trimmed == "END"
        || upper_trimmed == "END TRANSACTION"
    {
        return Ok(Some(DdlCommand::CommitTransaction));
    }

    // ROLLBACK [TRANSACTION] - Rollback a transaction
    if upper_trimmed == "ROLLBACK"
        || upper_trimmed == "ROLLBACK TRANSACTION"
        || upper_trimmed == "ABORT"
    {
        return Ok(Some(DdlCommand::RollbackTransaction));
    }

    // SAVEPOINT name - Create a savepoint
    if upper_trimmed.starts_with("SAVEPOINT ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 2 {
            let name = tokens[1].trim_end_matches(';').to_string();
            return Ok(Some(DdlCommand::Savepoint { name }));
        }
        return Err(EngineError::InvalidArgument(
            "SAVEPOINT requires a name".into(),
        ));
    }

    // RELEASE SAVEPOINT name - Release a savepoint
    if upper_trimmed.starts_with("RELEASE SAVEPOINT ") || upper_trimmed.starts_with("RELEASE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let name_idx = if upper_trimmed.starts_with("RELEASE SAVEPOINT ") {
            2
        } else {
            1
        };
        if tokens.len() > name_idx {
            let name = tokens[name_idx].trim_end_matches(';').to_string();
            return Ok(Some(DdlCommand::ReleaseSavepoint { name }));
        }
        return Err(EngineError::InvalidArgument(
            "RELEASE requires a savepoint name".into(),
        ));
    }

    // ROLLBACK TO [SAVEPOINT] name - Rollback to a savepoint
    if upper_trimmed.starts_with("ROLLBACK TO SAVEPOINT ")
        || upper_trimmed.starts_with("ROLLBACK TO ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let name_idx = if upper_trimmed.starts_with("ROLLBACK TO SAVEPOINT ") {
            3
        } else {
            2
        };
        if tokens.len() > name_idx {
            let name = tokens[name_idx].trim_end_matches(';').to_string();
            return Ok(Some(DdlCommand::RollbackToSavepoint { name }));
        }
        return Err(EngineError::InvalidArgument(
            "ROLLBACK TO requires a savepoint name".into(),
        ));
    }

    // SHOW FUNCTIONS [LIKE pattern]
    if upper_trimmed == "SHOW FUNCTIONS" {
        return Ok(Some(DdlCommand::ShowFunctions { pattern: None }));
    }
    if upper_trimmed.starts_with("SHOW FUNCTIONS LIKE ") {
        let pattern_part = &sql["SHOW FUNCTIONS LIKE ".len()..];
        let pattern = pattern_part
            .trim()
            .trim_end_matches(';')
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        return Ok(Some(DdlCommand::ShowFunctions {
            pattern: Some(pattern),
        }));
    }

    // DROP FUNCTION [IF EXISTS] name
    if upper_trimmed.starts_with("DROP FUNCTION ") {
        let rest = &upper_trimmed["DROP FUNCTION ".len()..];
        let if_exists = rest.starts_with("IF EXISTS ");
        let name_part = if if_exists {
            &rest["IF EXISTS ".len()..]
        } else {
            rest
        };
        let name = name_part.trim().trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "DROP FUNCTION requires a function name".into(),
            ));
        }
        return Ok(Some(DdlCommand::DropFunction {
            name: name.to_string(),
            if_exists,
        }));
    }

    // CREATE [OR REPLACE] FUNCTION name(params) RETURNS type AS 'body' [LANGUAGE lang]
    if upper_trimmed.starts_with("CREATE FUNCTION ")
        || upper_trimmed.starts_with("CREATE OR REPLACE FUNCTION ")
    {
        return parse_create_function(sql);
    }

    // CREATE [OR REPLACE] PROCEDURE name(params) LANGUAGE lang AS $$ body $$
    if upper_trimmed.starts_with("CREATE PROCEDURE ")
        || upper_trimmed.starts_with("CREATE OR REPLACE PROCEDURE ")
    {
        return parse_create_procedure(sql);
    }

    // DROP PROCEDURE [IF EXISTS] name
    if upper_trimmed.starts_with("DROP PROCEDURE ") {
        let or_replace = upper_trimmed.starts_with("DROP PROCEDURE IF EXISTS ");
        let rest = if or_replace {
            upper_trimmed["DROP PROCEDURE IF EXISTS ".len()..].trim()
        } else {
            upper_trimmed["DROP PROCEDURE ".len()..].trim()
        };
        let name = rest.trim_end_matches(';').to_string();
        return Ok(Some(DdlCommand::DropProcedure {
            name,
            if_exists: or_replace,
        }));
    }

    // CALL procedure_name(args)
    if upper_trimmed.starts_with("CALL ") {
        return parse_call_procedure(sql);
    }

    // SHOW PROCEDURES [LIKE pattern]
    if upper_trimmed == "SHOW PROCEDURES" || upper_trimmed.starts_with("SHOW PROCEDURES ") {
        let pattern = if upper_trimmed.starts_with("SHOW PROCEDURES LIKE ") {
            let rest = sql["SHOW PROCEDURES LIKE ".len()..].trim();
            let pat = rest.trim_matches('\'').trim_end_matches(';').to_string();
            Some(pat)
        } else {
            None
        };
        return Ok(Some(DdlCommand::ShowProcedures { pattern }));
    }

    // Foreign Data Wrapper commands

    // SHOW FOREIGN DATA WRAPPERS
    if upper_trimmed == "SHOW FOREIGN DATA WRAPPERS"
        || upper_trimmed == "SHOW FDW"
        || upper_trimmed == "SHOW FDWS"
    {
        return Ok(Some(DdlCommand::ShowForeignDataWrappers));
    }

    // SHOW FOREIGN SERVERS
    if upper_trimmed == "SHOW FOREIGN SERVERS" {
        return Ok(Some(DdlCommand::ShowForeignServers));
    }

    // SHOW FOREIGN TABLES
    if upper_trimmed == "SHOW FOREIGN TABLES" {
        return Ok(Some(DdlCommand::ShowForeignTables));
    }

    // CREATE FOREIGN DATA WRAPPER name [HANDLER handler] [VALIDATOR validator] [OPTIONS (...)]
    if upper_trimmed.starts_with("CREATE FOREIGN DATA WRAPPER ") {
        return parse_create_foreign_data_wrapper(sql);
    }

    // DROP FOREIGN DATA WRAPPER [IF EXISTS] name
    if upper_trimmed.starts_with("DROP FOREIGN DATA WRAPPER ") {
        let rest = upper_trimmed["DROP FOREIGN DATA WRAPPER ".len()..].trim();
        let (if_exists, name) = if rest.starts_with("IF EXISTS ") {
            (
                true,
                rest["IF EXISTS ".len()..].trim().trim_end_matches(';'),
            )
        } else {
            (false, rest.trim_end_matches(';'))
        };
        return Ok(Some(DdlCommand::DropForeignDataWrapper {
            name: name.to_string(),
            if_exists,
        }));
    }

    // CREATE SERVER name FOREIGN DATA WRAPPER wrapper [TYPE type] [VERSION version] [OPTIONS (...)]
    if upper_trimmed.starts_with("CREATE SERVER ") {
        return parse_create_foreign_server(sql);
    }

    // DROP SERVER [IF EXISTS] name
    if upper_trimmed.starts_with("DROP SERVER ") {
        let rest = upper_trimmed["DROP SERVER ".len()..].trim();
        let (if_exists, name) = if rest.starts_with("IF EXISTS ") {
            (
                true,
                rest["IF EXISTS ".len()..].trim().trim_end_matches(';'),
            )
        } else {
            (false, rest.trim_end_matches(';'))
        };
        return Ok(Some(DdlCommand::DropForeignServer {
            name: name.to_string(),
            if_exists,
        }));
    }

    // CREATE USER MAPPING FOR user SERVER server [OPTIONS (...)]
    if upper_trimmed.starts_with("CREATE USER MAPPING ") {
        return parse_create_user_mapping(sql);
    }

    // DROP USER MAPPING [IF EXISTS] FOR user SERVER server
    if upper_trimmed.starts_with("DROP USER MAPPING ") {
        return parse_drop_user_mapping(sql);
    }

    // CREATE FOREIGN TABLE name (...) SERVER server
    if upper_trimmed.starts_with("CREATE FOREIGN TABLE ") {
        return parse_create_foreign_table(sql);
    }

    // DROP FOREIGN TABLE [IF EXISTS] name
    if upper_trimmed.starts_with("DROP FOREIGN TABLE ") {
        let rest = upper_trimmed["DROP FOREIGN TABLE ".len()..].trim();
        let (if_exists, name) = if rest.starts_with("IF EXISTS ") {
            (
                true,
                rest["IF EXISTS ".len()..].trim().trim_end_matches(';'),
            )
        } else {
            (false, rest.trim_end_matches(';'))
        };
        return Ok(Some(DdlCommand::DropForeignTable {
            name: name.to_string(),
            if_exists,
        }));
    }

    // IMPORT FOREIGN SCHEMA schema FROM SERVER server INTO database
    if upper_trimmed.starts_with("IMPORT FOREIGN SCHEMA ") {
        return parse_import_foreign_schema(sql);
    }

    // SHOW STREAMS
    if upper_trimmed == "SHOW STREAMS" {
        return Ok(Some(DdlCommand::ShowStreams));
    }

    // START STREAM name
    if upper_trimmed.starts_with("START STREAM ") {
        let name = upper_trimmed["START STREAM ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "START STREAM requires a stream name".into(),
            ));
        }
        return Ok(Some(DdlCommand::StartStream {
            name: name.to_string(),
        }));
    }

    // STOP STREAM name
    if upper_trimmed.starts_with("STOP STREAM ") {
        let name = upper_trimmed["STOP STREAM ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "STOP STREAM requires a stream name".into(),
            ));
        }
        return Ok(Some(DdlCommand::StopStream {
            name: name.to_string(),
        }));
    }

    // SHOW STREAM STATUS name
    if upper_trimmed.starts_with("SHOW STREAM STATUS ") {
        let name = upper_trimmed["SHOW STREAM STATUS ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "SHOW STREAM STATUS requires a stream name".into(),
            ));
        }
        return Ok(Some(DdlCommand::ShowStreamStatus {
            name: name.to_string(),
        }));
    }

    // DROP STREAM [IF EXISTS] name
    if upper_trimmed.starts_with("DROP STREAM ") {
        let rest = &upper_trimmed["DROP STREAM ".len()..];
        let if_exists = rest.starts_with("IF EXISTS ");
        let name_part = if if_exists {
            &rest["IF EXISTS ".len()..]
        } else {
            rest
        };
        let name = name_part.trim().trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "DROP STREAM requires a stream name".into(),
            ));
        }
        return Ok(Some(DdlCommand::DropStream {
            name: name.to_string(),
            if_exists,
        }));
    }

    // CREATE STREAM name FROM source INTO table [FORMAT format] [WITH (options)]
    if upper_trimmed.starts_with("CREATE STREAM ") {
        return parse_create_stream(sql);
    }

    // ========================================================================
    // Column-Level Encryption Commands
    // ========================================================================

    // CREATE ENCRYPTION KEY key_name [ALGORITHM AES256GCM|CHACHA20] [EXPIRES timestamp]
    if upper_trimmed.starts_with("CREATE ENCRYPTION KEY ") {
        return parse_create_encryption_key(sql);
    }

    // DROP ENCRYPTION KEY [IF EXISTS] key_name
    if upper_trimmed.starts_with("DROP ENCRYPTION KEY ") {
        let rest = &upper_trimmed["DROP ENCRYPTION KEY ".len()..];
        let if_exists = rest.starts_with("IF EXISTS ");
        let name_part = if if_exists {
            &rest["IF EXISTS ".len()..]
        } else {
            rest
        };
        let name = name_part.trim().trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "DROP ENCRYPTION KEY requires a key name".into(),
            ));
        }
        return Ok(Some(DdlCommand::DropEncryptionKey {
            key_name: name.to_string(),
            if_exists,
        }));
    }

    // ROTATE ENCRYPTION KEY key_name
    if upper_trimmed.starts_with("ROTATE ENCRYPTION KEY ") {
        let name = upper_trimmed["ROTATE ENCRYPTION KEY ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "ROTATE ENCRYPTION KEY requires a key name".into(),
            ));
        }
        return Ok(Some(DdlCommand::RotateEncryptionKey {
            key_name: name.to_string(),
        }));
    }

    // SHOW ENCRYPTION KEYS
    if upper_trimmed == "SHOW ENCRYPTION KEYS" {
        return Ok(Some(DdlCommand::ShowEncryptionKeys));
    }

    // SHOW ENCRYPTED COLUMNS [FROM database.table]
    if upper_trimmed == "SHOW ENCRYPTED COLUMNS" {
        return Ok(Some(DdlCommand::ShowEncryptedColumns {
            database: None,
            table: None,
        }));
    }
    if upper_trimmed.starts_with("SHOW ENCRYPTED COLUMNS FROM ") {
        let table_name = upper_trimmed["SHOW ENCRYPTED COLUMNS FROM ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ShowEncryptedColumns {
            database: Some(database),
            table: Some(table),
        }));
    }

    // ALTER TABLE ... ENCRYPT COLUMN column_name WITH KEY key_name
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains(" ENCRYPT COLUMN ") {
        return parse_encrypt_column(sql);
    }

    // ALTER TABLE ... DECRYPT COLUMN column_name
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains(" DECRYPT COLUMN ") {
        return parse_decrypt_column(sql);
    }

    // ========================================================================
    // Change Data Capture (CDC) Commands
    // ========================================================================

    // CREATE CDC SUBSCRIPTION name ON database.table [TO target] [WITH options]
    if upper_trimmed.starts_with("CREATE CDC SUBSCRIPTION ") {
        return parse_create_cdc_subscription(sql);
    }

    // DROP CDC SUBSCRIPTION [IF EXISTS] name
    if upper_trimmed.starts_with("DROP CDC SUBSCRIPTION ") {
        let rest = &upper_trimmed["DROP CDC SUBSCRIPTION ".len()..];
        let if_exists = rest.starts_with("IF EXISTS ");
        let name_part = if if_exists {
            &rest["IF EXISTS ".len()..]
        } else {
            rest
        };
        let name = name_part.trim().trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "DROP CDC SUBSCRIPTION requires a name".into(),
            ));
        }
        return Ok(Some(DdlCommand::DropCdcSubscription {
            name: name.to_string(),
            if_exists,
        }));
    }

    // START CDC SUBSCRIPTION name
    if upper_trimmed.starts_with("START CDC SUBSCRIPTION ") {
        let name = upper_trimmed["START CDC SUBSCRIPTION ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "START CDC SUBSCRIPTION requires a name".into(),
            ));
        }
        return Ok(Some(DdlCommand::StartCdcSubscription {
            name: name.to_string(),
        }));
    }

    // STOP CDC SUBSCRIPTION name
    if upper_trimmed.starts_with("STOP CDC SUBSCRIPTION ") {
        let name = upper_trimmed["STOP CDC SUBSCRIPTION ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "STOP CDC SUBSCRIPTION requires a name".into(),
            ));
        }
        return Ok(Some(DdlCommand::StopCdcSubscription {
            name: name.to_string(),
        }));
    }

    // SHOW CDC SUBSCRIPTIONS
    if upper_trimmed == "SHOW CDC SUBSCRIPTIONS" {
        return Ok(Some(DdlCommand::ShowCdcSubscriptions));
    }

    // SHOW CDC STATUS name
    if upper_trimmed.starts_with("SHOW CDC STATUS ") {
        let name = upper_trimmed["SHOW CDC STATUS ".len()..]
            .trim()
            .trim_end_matches(';');
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "SHOW CDC STATUS requires a subscription name".into(),
            ));
        }
        return Ok(Some(DdlCommand::ShowCdcStatus {
            name: name.to_string(),
        }));
    }

    // GET CHANGES FROM database.table [SINCE sequence] [LIMIT n]
    if upper_trimmed.starts_with("GET CHANGES FROM ") {
        return parse_get_changes(sql);
    }

    // SET CDC CHECKPOINT name TO sequence
    if upper_trimmed.starts_with("SET CDC CHECKPOINT ") {
        return parse_set_cdc_checkpoint(sql);
    }

    // ========================================================================
    // Session & Server Management Commands (PostgreSQL/MySQL compatible)
    // ========================================================================

    // SET [SESSION|LOCAL|GLOBAL] variable = value
    if upper_trimmed.starts_with("SET ") && !upper_trimmed.starts_with("SET CDC ") {
        return parse_set_variable(sql);
    }

    // SHOW variable (single variable)
    // Note: This must come after other SHOW commands to avoid conflicts
    if upper_trimmed.starts_with("SHOW ") && !upper_trimmed.contains(' ') && upper_trimmed != "SHOW"
    {
        let var_name = upper_trimmed["SHOW ".len()..].trim().trim_end_matches(';');
        if !var_name.is_empty() {
            return Ok(Some(DdlCommand::ShowVariable {
                name: var_name.to_string(),
            }));
        }
    }

    // SHOW VARIABLES [LIKE pattern]
    if upper_trimmed == "SHOW VARIABLES" || upper_trimmed == "SHOW ALL" {
        return Ok(Some(DdlCommand::ShowVariables { pattern: None }));
    }
    if upper_trimmed.starts_with("SHOW VARIABLES LIKE ") {
        let pattern = extract_like_pattern(&sql["SHOW VARIABLES LIKE ".len()..]);
        return Ok(Some(DdlCommand::ShowVariables {
            pattern: Some(pattern),
        }));
    }

    // SHOW STATUS [LIKE pattern] / SHOW SESSION STATUS / SHOW GLOBAL STATUS
    if upper_trimmed == "SHOW STATUS"
        || upper_trimmed == "SHOW SESSION STATUS"
        || upper_trimmed == "SHOW GLOBAL STATUS"
    {
        return Ok(Some(DdlCommand::ShowStatus { pattern: None }));
    }
    if upper_trimmed.starts_with("SHOW STATUS LIKE ") {
        let pattern = extract_like_pattern(&sql["SHOW STATUS LIKE ".len()..]);
        return Ok(Some(DdlCommand::ShowStatus {
            pattern: Some(pattern),
        }));
    }

    // SHOW PROCESSLIST / SHOW FULL PROCESSLIST
    if upper_trimmed == "SHOW PROCESSLIST" {
        return Ok(Some(DdlCommand::ShowProcesslist { full: false }));
    }
    if upper_trimmed == "SHOW FULL PROCESSLIST" {
        return Ok(Some(DdlCommand::ShowProcesslist { full: true }));
    }

    // KILL connection_id / KILL CONNECTION connection_id
    if upper_trimmed.starts_with("KILL CONNECTION ") {
        let id_str = upper_trimmed["KILL CONNECTION ".len()..]
            .trim()
            .trim_end_matches(';');
        let connection_id = id_str.parse::<u64>().map_err(|_| {
            EngineError::InvalidArgument(format!("Invalid connection ID: {}", id_str))
        })?;
        return Ok(Some(DdlCommand::KillConnection { connection_id }));
    }
    if upper_trimmed.starts_with("KILL QUERY ") {
        let id_str = upper_trimmed["KILL QUERY ".len()..]
            .trim()
            .trim_end_matches(';');
        let query_id = id_str
            .parse::<u64>()
            .map_err(|_| EngineError::InvalidArgument(format!("Invalid query ID: {}", id_str)))?;
        return Ok(Some(DdlCommand::KillQuery { query_id }));
    }
    if upper_trimmed.starts_with("KILL ")
        && !upper_trimmed.starts_with("KILL CONNECTION ")
        && !upper_trimmed.starts_with("KILL QUERY ")
    {
        let id_str = upper_trimmed["KILL ".len()..].trim().trim_end_matches(';');
        let connection_id = id_str.parse::<u64>().map_err(|_| {
            EngineError::InvalidArgument(format!("Invalid connection ID: {}", id_str))
        })?;
        return Ok(Some(DdlCommand::KillConnection { connection_id }));
    }

    // COMMENT ON TABLE database.table IS 'comment'
    if upper_trimmed.starts_with("COMMENT ON TABLE ") {
        return parse_comment_on_table(sql);
    }

    // COMMENT ON COLUMN database.table.column IS 'comment'
    if upper_trimmed.starts_with("COMMENT ON COLUMN ") {
        return parse_comment_on_column(sql);
    }

    // COMMENT ON DATABASE database IS 'comment'
    if upper_trimmed.starts_with("COMMENT ON DATABASE ") {
        return parse_comment_on_database(sql);
    }

    // CLUSTER [VERBOSE] table USING index_name
    if upper_trimmed.starts_with("CLUSTER ") && upper_trimmed.contains(" USING ") {
        return parse_cluster_table(sql);
    }
    // CLUSTER (all tables)
    if upper_trimmed == "CLUSTER" {
        return Ok(Some(DdlCommand::ClusterAll));
    }

    // REINDEX TABLE database.table
    if upper_trimmed.starts_with("REINDEX TABLE ") {
        let table_name = upper_trimmed["REINDEX TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ReindexTable { database, table }));
    }

    // REINDEX INDEX database.table.index_name
    if upper_trimmed.starts_with("REINDEX INDEX ") {
        return parse_reindex_index(sql);
    }

    // REINDEX DATABASE database_name
    if upper_trimmed.starts_with("REINDEX DATABASE ") {
        let database = upper_trimmed["REINDEX DATABASE ".len()..]
            .trim()
            .trim_end_matches(';');
        return Ok(Some(DdlCommand::ReindexDatabase {
            database: database.to_string(),
        }));
    }

    // LOCK TABLES table1 [READ|WRITE], table2 [READ|WRITE], ...
    if upper_trimmed.starts_with("LOCK TABLES ") || upper_trimmed.starts_with("LOCK TABLE ") {
        return parse_lock_tables(sql);
    }

    // UNLOCK TABLES
    if upper_trimmed == "UNLOCK TABLES" || upper_trimmed == "UNLOCK TABLE" {
        return Ok(Some(DdlCommand::UnlockTables));
    }

    // OPTIMIZE TABLE database.table
    if upper_trimmed.starts_with("OPTIMIZE TABLE ") {
        let table_name = upper_trimmed["OPTIMIZE TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::OptimizeTable { database, table }));
    }

    // FLUSH TABLES
    if upper_trimmed == "FLUSH TABLES" {
        return Ok(Some(DdlCommand::FlushTables));
    }

    // FLUSH PRIVILEGES
    if upper_trimmed == "FLUSH PRIVILEGES" {
        return Ok(Some(DdlCommand::FlushPrivileges));
    }

    // RESET QUERY CACHE
    if upper_trimmed == "RESET QUERY CACHE" {
        return Ok(Some(DdlCommand::ResetQueryCache));
    }

    // SHOW TABLE STATUS [FROM database] [LIKE pattern]
    if upper_trimmed == "SHOW TABLE STATUS" {
        return Ok(Some(DdlCommand::ShowTableStatus {
            database: None,
            pattern: None,
        }));
    }
    if upper_trimmed.starts_with("SHOW TABLE STATUS ") {
        return parse_show_table_status(sql);
    }

    // SHOW CREATE TABLE database.table
    if upper_trimmed.starts_with("SHOW CREATE TABLE ") {
        let table_name = upper_trimmed["SHOW CREATE TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ShowCreateTable { database, table }));
    }

    // SHOW CREATE VIEW database.view
    if upper_trimmed.starts_with("SHOW CREATE VIEW ") {
        let view_name = upper_trimmed["SHOW CREATE VIEW ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, view) = parse_table_name(view_name)?;
        return Ok(Some(DdlCommand::ShowCreateView { database, view }));
    }

    // SHOW CREATE DATABASE database_name
    if upper_trimmed.starts_with("SHOW CREATE DATABASE ") {
        let database = upper_trimmed["SHOW CREATE DATABASE ".len()..]
            .trim()
            .trim_end_matches(';');
        return Ok(Some(DdlCommand::ShowCreateDatabase {
            database: database.to_string(),
        }));
    }

    // SHOW COLUMNS FROM database.table [LIKE pattern]
    if upper_trimmed.starts_with("SHOW COLUMNS FROM ")
        || upper_trimmed.starts_with("SHOW FIELDS FROM ")
    {
        return parse_show_columns(sql);
    }

    // SHOW WARNINGS
    if upper_trimmed == "SHOW WARNINGS" {
        return Ok(Some(DdlCommand::ShowWarnings));
    }

    // SHOW ERRORS
    if upper_trimmed == "SHOW ERRORS" {
        return Ok(Some(DdlCommand::ShowErrors));
    }

    // SHOW ENGINE STATUS
    if upper_trimmed == "SHOW ENGINE STATUS" || upper_trimmed == "SHOW ENGINE BOYODB STATUS" {
        return Ok(Some(DdlCommand::ShowEngineStatus));
    }

    // CHECKSUM TABLE database.table
    if upper_trimmed.starts_with("CHECKSUM TABLE ") {
        let table_name = upper_trimmed["CHECKSUM TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ChecksumTable { database, table }));
    }

    // CHECK TABLE database.table
    if upper_trimmed.starts_with("CHECK TABLE ") {
        let table_name = upper_trimmed["CHECK TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::CheckTable { database, table }));
    }

    // REPAIR TABLE database.table (not the same as REPAIR SEGMENTS)
    if upper_trimmed.starts_with("REPAIR TABLE ") {
        let table_name = upper_trimmed["REPAIR TABLE ".len()..]
            .trim()
            .trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::RepairTable { database, table }));
    }

    // ========================================================================
    // ENUM Type Commands
    // ========================================================================

    // CREATE TYPE name AS ENUM ('val1', 'val2', ...)
    if upper_trimmed.starts_with("CREATE TYPE ") && upper_trimmed.contains(" AS ENUM ") {
        return parse_create_enum_type(sql);
    }

    // DROP TYPE [IF EXISTS] name
    if upper_trimmed.starts_with("DROP TYPE ") {
        return parse_drop_enum_type(sql);
    }

    // ALTER TYPE name ADD VALUE 'new_value' [BEFORE|AFTER 'existing']
    if upper_trimmed.starts_with("ALTER TYPE ") && upper_trimmed.contains(" ADD VALUE ") {
        return parse_alter_enum_type(sql);
    }

    // SHOW TYPES [IN database]
    if upper_trimmed == "SHOW TYPES" {
        return Ok(Some(DdlCommand::ShowTypes { database: None }));
    }
    if upper_trimmed.starts_with("SHOW TYPES IN ") || upper_trimmed.starts_with("SHOW TYPES FROM ")
    {
        let keyword_len = if upper_trimmed.starts_with("SHOW TYPES IN ") {
            "SHOW TYPES IN ".len()
        } else {
            "SHOW TYPES FROM ".len()
        };
        let database = sql[keyword_len..].trim().trim_end_matches(';').to_string();
        return Ok(Some(DdlCommand::ShowTypes {
            database: Some(database),
        }));
    }

    Ok(None)
}

/// Parse CREATE ENCRYPTION KEY command
fn parse_create_encryption_key(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["CREATE ENCRYPTION KEY ".len()..].trim_end_matches(';');

    // Parse key name
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    if tokens.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE ENCRYPTION KEY requires a key name".into(),
        ));
    }
    let key_name = tokens[0].to_string();

    // Parse optional ALGORITHM
    let mut algorithm = EncryptionAlgorithmType::Aes256Gcm;
    if let Some(algo_pos) = upper.find(" ALGORITHM ") {
        let algo_rest = &upper[algo_pos + " ALGORITHM ".len()..];
        let algo_token = algo_rest.split_whitespace().next().unwrap_or("");
        algorithm = match algo_token.trim_end_matches(';') {
            "AES256GCM" | "AES-256-GCM" => EncryptionAlgorithmType::Aes256Gcm,
            "CHACHA20" | "CHACHA20POLY1305" => EncryptionAlgorithmType::ChaCha20Poly1305,
            "DETERMINISTIC" | "DETERMINISTIC_AES256" => {
                EncryptionAlgorithmType::DeterministicAes256
            }
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "Unknown encryption algorithm: {}. Use AES256GCM, CHACHA20, or DETERMINISTIC",
                    other
                )))
            }
        };
    }

    // Parse optional EXPIRES
    let expires_at = if let Some(exp_pos) = upper.find(" EXPIRES ") {
        let exp_rest = &upper[exp_pos + " EXPIRES ".len()..];
        let exp_token = exp_rest.split_whitespace().next().unwrap_or("");
        exp_token.trim_end_matches(';').parse::<u64>().ok()
    } else {
        None
    };

    Ok(Some(DdlCommand::CreateEncryptionKey {
        key_name,
        algorithm,
        expires_at,
    }))
}

/// Parse ALTER TABLE ... ENCRYPT COLUMN command
fn parse_encrypt_column(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Extract table name (after ALTER TABLE, before ENCRYPT)
    let after_alter = &upper["ALTER TABLE ".len()..];
    let encrypt_pos = after_alter
        .find(" ENCRYPT COLUMN ")
        .ok_or_else(|| EngineError::InvalidArgument("Expected ENCRYPT COLUMN clause".into()))?;
    let table_name = &sql["ALTER TABLE ".len()..]["ALTER TABLE ".len() + encrypt_pos..];
    let table_name = sql["ALTER TABLE ".len()..encrypt_pos + "ALTER TABLE ".len()].trim();
    let (database, table) = parse_table_name(table_name)?;

    // Extract column name (after ENCRYPT COLUMN, before WITH KEY)
    let after_encrypt = &upper["ALTER TABLE ".len() + encrypt_pos + " ENCRYPT COLUMN ".len()..];
    let with_key_pos = after_encrypt.find(" WITH KEY ").ok_or_else(|| {
        EngineError::InvalidArgument("ENCRYPT COLUMN requires WITH KEY clause".into())
    })?;
    let column = after_encrypt[..with_key_pos].trim().to_string();

    // Extract key name
    let after_with_key = &after_encrypt[with_key_pos + " WITH KEY ".len()..];
    let key_end = after_with_key.find(' ').unwrap_or(after_with_key.len());
    let key_name = after_with_key[..key_end]
        .trim()
        .trim_end_matches(';')
        .to_string();

    // Parse optional ALGORITHM
    let algorithm = if let Some(algo_pos) = upper.find(" ALGORITHM ") {
        let algo_rest = &upper[algo_pos + " ALGORITHM ".len()..];
        let algo_token = algo_rest.split_whitespace().next().unwrap_or("");
        Some(match algo_token.trim_end_matches(';') {
            "AES256GCM" | "AES-256-GCM" => EncryptionAlgorithmType::Aes256Gcm,
            "CHACHA20" | "CHACHA20POLY1305" => EncryptionAlgorithmType::ChaCha20Poly1305,
            "DETERMINISTIC" | "DETERMINISTIC_AES256" => {
                EncryptionAlgorithmType::DeterministicAes256
            }
            _ => EncryptionAlgorithmType::Aes256Gcm,
        })
    } else {
        None
    };

    Ok(Some(DdlCommand::EncryptColumn {
        database,
        table,
        column,
        key_name,
        algorithm,
    }))
}

/// Parse ALTER TABLE ... DECRYPT COLUMN command
fn parse_decrypt_column(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Extract table name
    let after_alter = &upper["ALTER TABLE ".len()..];
    let decrypt_pos = after_alter
        .find(" DECRYPT COLUMN ")
        .ok_or_else(|| EngineError::InvalidArgument("Expected DECRYPT COLUMN clause".into()))?;
    let table_name = sql["ALTER TABLE ".len()..decrypt_pos + "ALTER TABLE ".len()].trim();
    let (database, table) = parse_table_name(table_name)?;

    // Extract column name
    let after_decrypt = &after_alter[decrypt_pos + " DECRYPT COLUMN ".len()..];
    let column = after_decrypt
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(';')
        .to_string();

    if column.is_empty() {
        return Err(EngineError::InvalidArgument(
            "DECRYPT COLUMN requires a column name".into(),
        ));
    }

    Ok(Some(DdlCommand::DecryptColumn {
        database,
        table,
        column,
    }))
}

/// Parse CREATE CDC SUBSCRIPTION command
fn parse_create_cdc_subscription(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["CREATE CDC SUBSCRIPTION ".len()..];

    // Extract subscription name (before ON)
    let on_pos = rest.find(" ON ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE CDC SUBSCRIPTION requires ON clause".into())
    })?;
    let name = rest[..on_pos].trim().to_string();

    // Extract table name (after ON)
    let after_on = &rest[on_pos + " ON ".len()..];
    let table_end = after_on
        .find(" TO ")
        .or_else(|| after_on.find(" WITH "))
        .or_else(|| after_on.find(" INCLUDE "))
        .unwrap_or(after_on.trim_end_matches(';').len());
    let table_name = after_on[..table_end].trim().trim_end_matches(';');
    let (database, table) = parse_table_name(table_name)?;

    // Parse optional TO target
    let mut target_type = CdcTargetType::Buffer;
    let mut target_config = None;
    if let Some(to_pos) = upper.find(" TO ") {
        let after_to = &upper[to_pos + " TO ".len()..];
        let target_end = after_to.find(' ').unwrap_or(after_to.len());
        let target = after_to[..target_end].trim().trim_end_matches(';');
        target_type = match target {
            "KAFKA" => CdcTargetType::Kafka,
            "WEBHOOK" => CdcTargetType::Webhook,
            "TABLE" => CdcTargetType::Table,
            _ => CdcTargetType::Buffer,
        };

        // Extract config if present (quoted string after target)
        let after_target = &sql[to_pos + " TO ".len() + target_end..];
        if after_target.trim().starts_with('\'') || after_target.trim().starts_with('"') {
            let quote = after_target.trim().chars().next().unwrap();
            let start = after_target.find(quote).unwrap() + 1;
            let end = after_target[start..].find(quote).map(|p| start + p);
            if let Some(e) = end {
                target_config = Some(after_target[start..e].to_string());
            }
        }
    }

    // Parse optional INCLUDE BEFORE
    let include_before = upper.contains(" INCLUDE BEFORE");

    Ok(Some(DdlCommand::CreateCdcSubscription {
        name,
        database,
        table,
        target_type,
        target_config,
        include_before,
    }))
}

/// Parse GET CHANGES FROM database.table command
fn parse_get_changes(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["GET CHANGES FROM ".len()..];

    // Extract table name
    let table_end = rest
        .find(" SINCE ")
        .or_else(|| rest.find(" LIMIT "))
        .unwrap_or(rest.trim_end_matches(';').len());
    let table_name = rest[..table_end].trim().trim_end_matches(';');
    let (database, table) = parse_table_name(table_name)?;

    // Parse optional SINCE sequence
    let since_sequence = if let Some(since_pos) = upper.find(" SINCE ") {
        let after_since = &upper[since_pos + " SINCE ".len()..];
        let seq_end = after_since.find(' ').unwrap_or(after_since.len());
        after_since[..seq_end]
            .trim()
            .trim_end_matches(';')
            .parse::<u64>()
            .ok()
    } else {
        None
    };

    // Parse optional LIMIT n
    let limit = if let Some(limit_pos) = upper.find(" LIMIT ") {
        let after_limit = &upper[limit_pos + " LIMIT ".len()..];
        let lim_end = after_limit.find(' ').unwrap_or(after_limit.len());
        after_limit[..lim_end]
            .trim()
            .trim_end_matches(';')
            .parse::<usize>()
            .ok()
    } else {
        None
    };

    Ok(Some(DdlCommand::GetChanges {
        database,
        table,
        since_sequence,
        limit,
    }))
}

/// Parse SET CDC CHECKPOINT command
fn parse_set_cdc_checkpoint(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["SET CDC CHECKPOINT ".len()..];

    // Extract subscription name (before TO)
    let to_pos = rest.find(" TO ").ok_or_else(|| {
        EngineError::InvalidArgument("SET CDC CHECKPOINT requires TO clause".into())
    })?;
    let name = rest[..to_pos].trim().to_string();

    // Extract sequence number
    let after_to = &rest[to_pos + " TO ".len()..];
    let sequence = after_to
        .trim()
        .trim_end_matches(';')
        .parse::<u64>()
        .map_err(|_| {
            EngineError::InvalidArgument(
                "SET CDC CHECKPOINT requires a valid sequence number".into(),
            )
        })?;

    Ok(Some(DdlCommand::SetCdcCheckpoint { name, sequence }))
}

// ============================================================================
// Helper functions for PostgreSQL/MySQL compatible commands
// ============================================================================

/// Extract a LIKE pattern from a string (handles quoted patterns)
fn extract_like_pattern(s: &str) -> String {
    let s = s.trim().trim_end_matches(';');
    // Handle quoted patterns
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Parse SET [SESSION|LOCAL|GLOBAL] variable = value
fn parse_set_variable(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["SET ".len()..].trim_end_matches(';');

    // Parse scope
    let (scope, var_start) = if rest.starts_with("SESSION ") {
        (VariableScope::Session, "SESSION ".len())
    } else if rest.starts_with("LOCAL ") {
        (VariableScope::Local, "LOCAL ".len())
    } else if rest.starts_with("GLOBAL ") {
        (VariableScope::Global, "GLOBAL ".len())
    } else {
        (VariableScope::Session, 0)
    };

    let rest = &rest[var_start..];

    // Find = or TO
    let eq_pos = rest.find('=').or_else(|| {
        // PostgreSQL uses TO
        rest.find(" TO ").map(|p| p + 1)
    });

    let (name, value) = if let Some(pos) = eq_pos {
        let name = rest[..pos].trim();
        let value_start = if rest[pos..].starts_with('=') {
            pos + 1
        } else {
            pos + 3 // " TO " - skip space before TO
        };
        let value = rest[value_start..].trim();
        (name.to_string(), value.to_string())
    } else {
        return Err(EngineError::InvalidArgument(
            "SET requires variable = value syntax".into(),
        ));
    };

    // Handle quoted values (preserve original case)
    let original_sql = &sql["SET ".len() + var_start..];
    let value = if let Some(eq_pos) = original_sql.find('=') {
        original_sql[eq_pos + 1..]
            .trim()
            .trim_end_matches(';')
            .to_string()
    } else if let Some(to_pos) = original_sql.to_uppercase().find(" TO ") {
        original_sql[to_pos + 4..]
            .trim()
            .trim_end_matches(';')
            .to_string()
    } else {
        value
    };

    Ok(Some(DdlCommand::SetVariable { name, value, scope }))
}

/// Parse COMMENT ON TABLE database.table IS 'comment'
fn parse_comment_on_table(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &sql["COMMENT ON TABLE ".len()..];

    // Find IS keyword
    let is_pos = upper.find(" IS ").ok_or_else(|| {
        EngineError::InvalidArgument("COMMENT ON TABLE requires IS clause".into())
    })?;

    let table_part = &rest[..is_pos - "COMMENT ON TABLE ".len()];
    let (database, table) = parse_table_name(table_part.trim())?;

    // Extract comment (may be NULL or quoted string)
    let comment_part = &rest[is_pos - "COMMENT ON TABLE ".len() + " IS ".len()..].trim();
    let comment_part = comment_part.trim_end_matches(';');

    let comment = if comment_part.to_uppercase() == "NULL" {
        None
    } else if (comment_part.starts_with('\'') && comment_part.ends_with('\''))
        || (comment_part.starts_with('"') && comment_part.ends_with('"'))
    {
        Some(comment_part[1..comment_part.len() - 1].to_string())
    } else {
        Some(comment_part.to_string())
    };

    Ok(Some(DdlCommand::CommentOnTable {
        database,
        table,
        comment,
    }))
}

/// Parse COMMENT ON COLUMN database.table.column IS 'comment'
fn parse_comment_on_column(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &sql["COMMENT ON COLUMN ".len()..];

    // Find IS keyword
    let is_pos = upper.find(" IS ").ok_or_else(|| {
        EngineError::InvalidArgument("COMMENT ON COLUMN requires IS clause".into())
    })?;

    let col_part = &rest[..is_pos - "COMMENT ON COLUMN ".len()];

    // Parse database.table.column
    let parts: Vec<&str> = col_part.trim().split('.').collect();
    let (database, table, column) = match parts.len() {
        3 => (
            parts[0]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string(),
            parts[1]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string(),
            parts[2]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string(),
        ),
        2 => (
            "default".to_string(),
            parts[0]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string(),
            parts[1]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string(),
        ),
        _ => {
            return Err(EngineError::InvalidArgument(
                "COMMENT ON COLUMN requires table.column or database.table.column".into(),
            ))
        }
    };

    // Extract comment
    let comment_part = &rest[is_pos - "COMMENT ON COLUMN ".len() + " IS ".len()..].trim();
    let comment_part = comment_part.trim_end_matches(';');

    let comment = if comment_part.to_uppercase() == "NULL" {
        None
    } else if (comment_part.starts_with('\'') && comment_part.ends_with('\''))
        || (comment_part.starts_with('"') && comment_part.ends_with('"'))
    {
        Some(comment_part[1..comment_part.len() - 1].to_string())
    } else {
        Some(comment_part.to_string())
    };

    Ok(Some(DdlCommand::CommentOnColumn {
        database,
        table,
        column,
        comment,
    }))
}

/// Parse COMMENT ON DATABASE database IS 'comment'
fn parse_comment_on_database(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &sql["COMMENT ON DATABASE ".len()..];

    // Find IS keyword
    let is_pos = upper.find(" IS ").ok_or_else(|| {
        EngineError::InvalidArgument("COMMENT ON DATABASE requires IS clause".into())
    })?;

    let database = rest[..is_pos - "COMMENT ON DATABASE ".len()]
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .to_string();

    // Extract comment
    let comment_part = &rest[is_pos - "COMMENT ON DATABASE ".len() + " IS ".len()..].trim();
    let comment_part = comment_part.trim_end_matches(';');

    let comment = if comment_part.to_uppercase() == "NULL" {
        None
    } else if (comment_part.starts_with('\'') && comment_part.ends_with('\''))
        || (comment_part.starts_with('"') && comment_part.ends_with('"'))
    {
        Some(comment_part[1..comment_part.len() - 1].to_string())
    } else {
        Some(comment_part.to_string())
    };

    Ok(Some(DdlCommand::CommentOnDatabase { database, comment }))
}

/// Parse CLUSTER [VERBOSE] table USING index_name
fn parse_cluster_table(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let binding = upper["CLUSTER ".len()..].trim_end_matches(';').to_string();
    let rest: &str = &binding;

    // Check for VERBOSE
    let (verbose, rest): (bool, &str) = if rest.starts_with("VERBOSE ") {
        (true, &rest["VERBOSE ".len()..])
    } else {
        (false, rest)
    };

    // Find USING keyword
    let using_pos = rest
        .find(" USING ")
        .ok_or_else(|| EngineError::InvalidArgument("CLUSTER requires USING clause".into()))?;

    let table_name = rest[..using_pos].trim();
    let (database, table) = parse_table_name(table_name)?;

    let index_name = rest[using_pos + " USING ".len()..].trim().to_string();

    Ok(Some(DdlCommand::ClusterTable {
        database,
        table,
        index_name,
        verbose,
    }))
}

/// Parse REINDEX INDEX database.table.index_name
fn parse_reindex_index(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let binding = upper["REINDEX INDEX ".len()..]
        .trim_end_matches(';')
        .to_string();
    let rest: &str = &binding;

    // Parse database.table.index_name
    let parts: Vec<&str> = rest.split('.').collect();
    let (database, table, index_name) = match parts.len() {
        3 => (
            parts[0].trim().to_string(),
            parts[1].trim().to_string(),
            parts[2].trim().to_string(),
        ),
        2 => (
            "default".to_string(),
            parts[0].trim().to_string(),
            parts[1].trim().to_string(),
        ),
        _ => {
            return Err(EngineError::InvalidArgument(
                "REINDEX INDEX requires table.index_name or database.table.index_name".into(),
            ))
        }
    };

    Ok(Some(DdlCommand::ReindexIndex {
        database,
        table,
        index_name,
    }))
}

/// Parse LOCK TABLES table1 [READ|WRITE], table2 [READ|WRITE], ...
fn parse_lock_tables(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = if upper.starts_with("LOCK TABLES ") {
        &sql["LOCK TABLES ".len()..]
    } else {
        &sql["LOCK TABLE ".len()..]
    };
    let rest = rest.trim_end_matches(';');

    let mut locks = Vec::new();

    // Split by comma and parse each table lock
    for part in rest.split(',') {
        let part = part.trim();
        let part_upper = part.to_uppercase();

        // Find lock mode
        let (table_part, mode) = if part_upper.ends_with(" READ LOCAL") {
            (
                &part[..part.len() - " READ LOCAL".len()],
                LockMode::ReadLocal,
            )
        } else if part_upper.ends_with(" LOW_PRIORITY WRITE") {
            (
                &part[..part.len() - " LOW_PRIORITY WRITE".len()],
                LockMode::LowPriorityWrite,
            )
        } else if part_upper.ends_with(" WRITE") {
            (&part[..part.len() - " WRITE".len()], LockMode::Write)
        } else if part_upper.ends_with(" READ") {
            (&part[..part.len() - " READ".len()], LockMode::Read)
        } else {
            // Default to READ lock
            (part, LockMode::Read)
        };

        let (database, table) = parse_table_name(table_part.trim())?;
        locks.push(TableLock {
            database,
            table,
            mode,
        });
    }

    if locks.is_empty() {
        return Err(EngineError::InvalidArgument(
            "LOCK TABLES requires at least one table".into(),
        ));
    }

    Ok(Some(DdlCommand::LockTables { locks }))
}

/// Parse SHOW TABLE STATUS [FROM database] [LIKE pattern]
fn parse_show_table_status(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = &upper["SHOW TABLE STATUS ".len()..].trim_end_matches(';');

    let mut database = None;
    let mut pattern = None;

    // Parse FROM database
    if rest.starts_with("FROM ") || rest.starts_with("IN ") {
        let keyword_len = if rest.starts_with("FROM ") { 5 } else { 3 };
        let after_from = &rest[keyword_len..];
        let db_end = after_from.find(" LIKE ").unwrap_or(after_from.len());
        database = Some(after_from[..db_end].trim().to_string());

        // Check for LIKE pattern after database
        if let Some(like_pos) = after_from.find(" LIKE ") {
            let pattern_str =
                &sql["SHOW TABLE STATUS ".len() + keyword_len + like_pos + " LIKE ".len()..];
            pattern = Some(extract_like_pattern(pattern_str));
        }
    } else if rest.starts_with("LIKE ") {
        let pattern_str = &sql["SHOW TABLE STATUS LIKE ".len()..];
        pattern = Some(extract_like_pattern(pattern_str));
    }

    Ok(Some(DdlCommand::ShowTableStatus { database, pattern }))
}

/// Parse SHOW COLUMNS FROM database.table [LIKE pattern]
fn parse_show_columns(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let keyword = if upper.starts_with("SHOW COLUMNS FROM ") {
        "SHOW COLUMNS FROM "
    } else {
        "SHOW FIELDS FROM "
    };
    let binding = sql[keyword.len()..].trim_end_matches(';').to_string();
    let rest: &str = &binding;
    let rest_upper = rest.to_uppercase();

    // Find LIKE if present
    let (table_part, pattern): (&str, Option<String>) =
        if let Some(like_pos) = rest_upper.find(" LIKE ") {
            let table_part = &rest[..like_pos];
            let pattern_str = &rest[like_pos + " LIKE ".len()..];
            (table_part, Some(extract_like_pattern(pattern_str)))
        } else {
            (rest, None)
        };

    let (database, table) = parse_table_name(table_part.trim())?;

    Ok(Some(DdlCommand::ShowColumns {
        database,
        table,
        pattern,
    }))
}

/// Parse CREATE TYPE name AS ENUM ('val1', 'val2', ...)
fn parse_create_enum_type(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Extract type name (after CREATE TYPE, before AS ENUM)
    let after_create = &sql["CREATE TYPE ".len()..];
    let as_enum_pos = upper.find(" AS ENUM ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE TYPE requires AS ENUM clause".into())
    })?;

    let type_part = after_create[..as_enum_pos - "CREATE TYPE ".len()].trim();

    // Check for IF NOT EXISTS
    let (if_not_exists, type_name) = if type_part.to_uppercase().starts_with("IF NOT EXISTS ") {
        (true, type_part["IF NOT EXISTS ".len()..].trim())
    } else {
        (false, type_part)
    };

    // Parse database.name or just name
    let (database, name) = parse_table_name(type_name)?;

    // Extract values from parentheses
    let after_enum = &sql[as_enum_pos + " AS ENUM ".len()..];
    let values = parse_enum_values(after_enum)?;

    if values.is_empty() {
        return Err(EngineError::InvalidArgument(
            "ENUM type requires at least one value".into(),
        ));
    }

    Ok(Some(DdlCommand::CreateEnumType {
        database,
        name,
        values,
        if_not_exists,
    }))
}

/// Parse ENUM values from parentheses: ('val1', 'val2', ...)
fn parse_enum_values(s: &str) -> Result<Vec<String>, EngineError> {
    let s = s.trim().trim_end_matches(';');
    if !s.starts_with('(') || !s.ends_with(')') {
        return Err(EngineError::InvalidArgument(
            "ENUM values must be enclosed in parentheses".into(),
        ));
    }

    let inner = &s[1..s.len() - 1];
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';

    for ch in inner.chars() {
        match ch {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
            }
            c if c == quote_char && in_quotes => {
                in_quotes = false;
                values.push(current.clone());
                current.clear();
            }
            ',' if !in_quotes => {
                // Skip commas between values
            }
            _ if in_quotes => {
                current.push(ch);
            }
            _ => {
                // Skip whitespace outside quotes
            }
        }
    }

    Ok(values)
}

/// Parse DROP TYPE [IF EXISTS] name
fn parse_drop_enum_type(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let rest = upper["DROP TYPE ".len()..]
        .trim_end_matches(';')
        .to_string();

    let (if_exists, type_name) = if rest.starts_with("IF EXISTS ") {
        (true, &rest["IF EXISTS ".len()..])
    } else {
        (false, rest.as_str())
    };

    let (database, name) = parse_table_name(type_name.trim())?;

    Ok(Some(DdlCommand::DropEnumType {
        database,
        name,
        if_exists,
    }))
}

/// Parse ALTER TYPE name ADD VALUE 'new_value' [BEFORE|AFTER 'existing']
fn parse_alter_enum_type(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let original_sql = sql.trim().trim_end_matches(';');

    // Extract type name (after ALTER TYPE, before ADD VALUE)
    let after_alter = &original_sql["ALTER TYPE ".len()..];
    let add_value_pos = upper.find(" ADD VALUE ").ok_or_else(|| {
        EngineError::InvalidArgument("ALTER TYPE requires ADD VALUE clause".into())
    })?;

    let type_name = after_alter[..add_value_pos - "ALTER TYPE ".len()].trim();
    let (database, name) = parse_table_name(type_name)?;

    // Extract the new value
    let after_add = &original_sql[add_value_pos + " ADD VALUE ".len()..];
    let upper_after_add = after_add.to_uppercase();

    // Find the quoted value
    let (new_value, position_part) = if after_add.starts_with('\'') || after_add.starts_with('"') {
        let quote = after_add.chars().next().unwrap();
        let end = after_add[1..]
            .find(quote)
            .ok_or_else(|| EngineError::InvalidArgument("Unclosed quote in ADD VALUE".into()))?
            + 1;
        let value = after_add[1..end].to_string();
        let rest = &after_add[end + 1..].trim();
        (value, rest.to_string())
    } else {
        return Err(EngineError::InvalidArgument(
            "ADD VALUE requires a quoted string".into(),
        ));
    };

    // Parse optional BEFORE/AFTER
    let position = if upper_after_add.contains(" BEFORE ") {
        let before_pos = upper_after_add.find(" BEFORE ").unwrap();
        let after_before = &position_part
            [before_pos - (position_part.len() - upper_after_add.len()) + " BEFORE ".len()..];
        if let Some(existing) = extract_quoted_string(after_before) {
            Some(EnumValuePosition::Before(existing))
        } else {
            None
        }
    } else if upper_after_add.contains(" AFTER ") {
        let after_pos = upper_after_add.find(" AFTER ").unwrap();
        let after_after = &position_part
            [after_pos - (position_part.len() - upper_after_add.len()) + " AFTER ".len()..];
        if let Some(existing) = extract_quoted_string(after_after) {
            Some(EnumValuePosition::After(existing))
        } else {
            None
        }
    } else {
        None
    };

    Ok(Some(DdlCommand::AlterEnumType {
        database,
        name,
        new_value,
        position,
    }))
}

/// Parse CREATE STREAM command
/// Syntax: CREATE STREAM name FROM KAFKA|PULSAR 'config' INTO database.table [FORMAT json|csv]
fn parse_create_stream(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Extract stream name (after CREATE STREAM, before FROM)
    let after_create = &sql["CREATE STREAM ".len()..];
    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| EngineError::InvalidArgument("CREATE STREAM requires FROM clause".into()))?;
    let name = after_create[..from_pos - "CREATE STREAM ".len()]
        .trim()
        .to_string();

    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE STREAM requires a stream name".into(),
        ));
    }

    // Extract source type and config
    let after_from = &sql[from_pos + " FROM ".len()..];
    let upper_after_from = after_from.to_uppercase();

    let (source_type, config_start) = if upper_after_from.starts_with("KAFKA ") {
        (StreamSourceType::Kafka, "KAFKA ".len())
    } else if upper_after_from.starts_with("PULSAR ") {
        (StreamSourceType::Pulsar, "PULSAR ".len())
    } else if upper_after_from.starts_with("KINESIS ") {
        (StreamSourceType::Kinesis, "KINESIS ".len())
    } else if upper_after_from.starts_with("FILE ") {
        (StreamSourceType::File, "FILE ".len())
    } else {
        return Err(EngineError::InvalidArgument(
            "CREATE STREAM source must be KAFKA, PULSAR, KINESIS, or FILE".into(),
        ));
    };

    let after_source = &after_from[config_start..];

    // Extract config (quoted string)
    let (source_config, after_config) =
        if after_source.starts_with('\'') || after_source.starts_with('"') {
            let quote_char = after_source.chars().next().unwrap();
            let end_quote = after_source[1..]
                .find(quote_char)
                .ok_or_else(|| EngineError::InvalidArgument("Unterminated config string".into()))?;
            let config = after_source[1..1 + end_quote].to_string();
            let remaining = &after_source[2 + end_quote..];
            (config, remaining)
        } else {
            // No quotes - take until whitespace
            let end = after_source
                .find(char::is_whitespace)
                .unwrap_or(after_source.len());
            let config = after_source[..end].to_string();
            let remaining = &after_source[end..];
            (config, remaining)
        };

    let upper_after_config = after_config.to_uppercase();

    // Extract target table (after INTO)
    let into_pos = upper_after_config.find(" INTO ");
    let target_table = if let Some(pos) = into_pos {
        let after_into = &after_config[pos + " INTO ".len()..];
        // Find end of table name (space, semicolon, or FORMAT)
        let end = after_into
            .to_uppercase()
            .find(" FORMAT ")
            .or_else(|| after_into.find(';'))
            .or_else(|| after_into.find(char::is_whitespace))
            .unwrap_or(after_into.len());
        Some(after_into[..end].trim().to_string())
    } else {
        None
    };

    // Extract format (after FORMAT)
    let format_pos = upper_after_config.find(" FORMAT ");
    let format = if let Some(pos) = format_pos {
        let after_format = &after_config[pos + " FORMAT ".len()..];
        let end = after_format
            .find(char::is_whitespace)
            .or_else(|| after_format.find(';'))
            .unwrap_or(after_format.len());
        Some(after_format[..end].trim().to_lowercase())
    } else {
        None
    };

    Ok(Some(DdlCommand::CreateStream {
        name,
        source_type,
        source_config,
        target_table,
        format,
    }))
}

/// Parse CREATE FUNCTION command
/// Syntax: CREATE [OR REPLACE] FUNCTION name(param1 type1 [DEFAULT val1], ...) RETURNS type AS 'body' [LANGUAGE lang]
fn parse_create_function(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    // Find the function name and parameters
    let prefix = if or_replace {
        "CREATE OR REPLACE FUNCTION "
    } else {
        "CREATE FUNCTION "
    };
    let prefix_upper = prefix.to_uppercase();

    let start_idx = upper
        .find(&prefix_upper)
        .ok_or_else(|| EngineError::InvalidArgument("Invalid CREATE FUNCTION syntax".into()))?
        + prefix.len();

    // Find the opening parenthesis for parameters
    let paren_open = sql[start_idx..].find('(').ok_or_else(|| {
        EngineError::InvalidArgument("CREATE FUNCTION requires parameter list".into())
    })? + start_idx;

    let name = sql[start_idx..paren_open].trim().to_string();
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE FUNCTION requires a function name".into(),
        ));
    }

    // Find the closing parenthesis
    let paren_close = sql[paren_open..].find(')').ok_or_else(|| {
        EngineError::InvalidArgument("CREATE FUNCTION requires closing parenthesis".into())
    })? + paren_open;

    // Parse parameters
    let params_str = &sql[paren_open + 1..paren_close];
    let parameters = parse_function_parameters(params_str)?;

    // Find RETURNS keyword
    let after_params = &sql[paren_close + 1..];
    let upper_after = after_params.to_uppercase();
    let returns_pos = upper_after.find("RETURNS ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE FUNCTION requires RETURNS clause".into())
    })?;

    // Find AS keyword
    let as_pos = upper_after
        .find(" AS ")
        .ok_or_else(|| EngineError::InvalidArgument("CREATE FUNCTION requires AS clause".into()))?;

    let return_type = after_params[returns_pos + 8..as_pos].trim().to_string();

    // Extract body (quoted string)
    let after_as = after_params[as_pos + 4..].trim();
    let (body_str, remaining) = extract_function_body_string(after_as)?;

    // Check for LANGUAGE clause
    let upper_remaining = remaining.to_uppercase();
    let language = if upper_remaining.trim_start().starts_with("LANGUAGE ") {
        let lang_start = remaining.to_uppercase().find("LANGUAGE ").unwrap() + 9;
        let lang_end = remaining[lang_start..]
            .find(|c: char| c.is_whitespace() || c == ';')
            .map(|i| i + lang_start)
            .unwrap_or(remaining.len());
        Some(remaining[lang_start..lang_end].trim().to_string())
    } else {
        None
    };

    // Determine body type based on content
    let body = if body_str.to_uppercase().starts_with("SELECT ")
        || body_str.to_uppercase().starts_with("WITH ")
    {
        FunctionBody::SqlQuery(body_str)
    } else {
        FunctionBody::SqlExpression(body_str)
    };

    Ok(Some(DdlCommand::CreateFunction {
        name,
        parameters,
        return_type,
        body,
        or_replace,
        language,
    }))
}

/// Parse CREATE [OR REPLACE] PROCEDURE name(params) LANGUAGE lang AS $$ body $$
fn parse_create_procedure(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    // Find the procedure name and parameters
    let prefix = if or_replace {
        "CREATE OR REPLACE PROCEDURE "
    } else {
        "CREATE PROCEDURE "
    };
    let prefix_upper = prefix.to_uppercase();

    let start_idx = upper
        .find(&prefix_upper)
        .ok_or_else(|| EngineError::InvalidArgument("Invalid CREATE PROCEDURE syntax".into()))?
        + prefix.len();

    // Find the opening parenthesis for parameters
    let paren_open = sql[start_idx..].find('(').ok_or_else(|| {
        EngineError::InvalidArgument("CREATE PROCEDURE requires parameter list".into())
    })? + start_idx;

    let name = sql[start_idx..paren_open].trim().to_string();
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE PROCEDURE requires a procedure name".into(),
        ));
    }

    // Find the closing parenthesis
    let paren_close = sql[paren_open..].find(')').ok_or_else(|| {
        EngineError::InvalidArgument("CREATE PROCEDURE requires closing parenthesis".into())
    })? + paren_open;

    // Parse parameters
    let params_str = &sql[paren_open + 1..paren_close];
    let parameters = parse_function_parameters(params_str)?;

    // Find AS or LANGUAGE keyword after params
    let after_params = &sql[paren_close + 1..];
    let upper_after = after_params.to_uppercase();

    // Check for LANGUAGE clause
    let language = if let Some(lang_pos) = upper_after.find("LANGUAGE ") {
        let lang_start = lang_pos + 9;
        let lang_end = after_params[lang_start..]
            .find(|c: char| c.is_whitespace() || c == ';')
            .map(|i| i + lang_start)
            .unwrap_or(after_params.len());
        Some(after_params[lang_start..lang_end].trim().to_string())
    } else {
        None
    };

    // Find AS keyword and extract body
    let as_pos = upper_after.find(" AS ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE PROCEDURE requires AS clause".into())
    })?;

    let after_as = after_params[as_pos + 4..].trim();
    let (body_str, _remaining) = extract_function_body_string(after_as)?;

    Ok(Some(DdlCommand::CreateProcedure {
        name,
        parameters,
        body: body_str,
        or_replace,
        language,
    }))
}

/// Parse CALL procedure_name(args)
fn parse_call_procedure(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("CALL ") {
        return Err(EngineError::InvalidArgument("Invalid CALL syntax".into()));
    }

    let rest = trimmed["CALL ".len()..].trim();

    // Find procedure name and optional arguments
    if let Some(paren_open) = rest.find('(') {
        let name = rest[..paren_open].trim().to_string();
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "CALL requires a procedure name".into(),
            ));
        }

        // Find closing parenthesis
        let paren_close = rest.rfind(')').ok_or_else(|| {
            EngineError::InvalidArgument("CALL requires closing parenthesis".into())
        })?;

        // Parse arguments
        let args_str = &rest[paren_open + 1..paren_close];
        let arguments: Vec<String> = if args_str.trim().is_empty() {
            Vec::new()
        } else {
            // Simple split by comma (doesn't handle nested parens or quoted strings)
            args_str.split(',').map(|a| a.trim().to_string()).collect()
        };

        Ok(Some(DdlCommand::CallProcedure { name, arguments }))
    } else {
        // No parentheses - just the procedure name
        let name = rest.to_string();
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "CALL requires a procedure name".into(),
            ));
        }
        Ok(Some(DdlCommand::CallProcedure {
            name,
            arguments: Vec::new(),
        }))
    }
}

// ============================================================================
// Foreign Data Wrapper parsing functions
// ============================================================================

/// Parse OPTIONS (key 'value', key2 'value2')
fn parse_fdw_options(sql: &str) -> Vec<(String, String)> {
    let mut options = Vec::new();
    let upper = sql.to_uppercase();

    if let Some(opt_start) = upper.find("OPTIONS") {
        if let Some(paren_start) = sql[opt_start..].find('(') {
            let start = opt_start + paren_start + 1;
            if let Some(paren_end) = sql[start..].find(')') {
                let opts_str = &sql[start..start + paren_end];
                // Simple parsing: split by comma, handle key 'value' pairs
                for pair in opts_str.split(',') {
                    let trimmed = pair.trim();
                    if let Some(space_pos) = trimmed.find(char::is_whitespace) {
                        let key = trimmed[..space_pos].trim().to_string();
                        let value = trimmed[space_pos..]
                            .trim()
                            .trim_matches('\'')
                            .trim_matches('"')
                            .to_string();
                        options.push((key, value));
                    }
                }
            }
        }
    }

    options
}

/// Parse CREATE FOREIGN DATA WRAPPER name [HANDLER handler] [VALIDATOR validator] [OPTIONS (...)]
fn parse_create_foreign_data_wrapper(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let start = upper
        .find("CREATE FOREIGN DATA WRAPPER ")
        .ok_or_else(|| EngineError::InvalidArgument("Invalid CREATE FDW syntax".into()))?
        + "CREATE FOREIGN DATA WRAPPER ".len();

    // Find the name (until HANDLER, VALIDATOR, OPTIONS, or end)
    let rest = &sql[start..];
    let name_end = rest
        .to_uppercase()
        .find(" HANDLER")
        .or_else(|| rest.to_uppercase().find(" VALIDATOR"))
        .or_else(|| rest.to_uppercase().find(" OPTIONS"))
        .unwrap_or(rest.len());

    let name = rest[..name_end].trim().trim_end_matches(';').to_string();
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE FOREIGN DATA WRAPPER requires a name".into(),
        ));
    }

    // Parse HANDLER
    let handler = if let Some(h_pos) = upper.find(" HANDLER ") {
        let h_start = h_pos + " HANDLER ".len();
        let h_rest = &sql[h_start..];
        let h_end = h_rest
            .to_uppercase()
            .find(" VALIDATOR")
            .or_else(|| h_rest.to_uppercase().find(" OPTIONS"))
            .unwrap_or(h_rest.len());
        Some(h_rest[..h_end].trim().trim_end_matches(';').to_string())
    } else {
        None
    };

    // Parse VALIDATOR
    let validator = if let Some(v_pos) = upper.find(" VALIDATOR ") {
        let v_start = v_pos + " VALIDATOR ".len();
        let v_rest = &sql[v_start..];
        let v_end = v_rest
            .to_uppercase()
            .find(" OPTIONS")
            .unwrap_or(v_rest.len());
        Some(v_rest[..v_end].trim().trim_end_matches(';').to_string())
    } else {
        None
    };

    let options = parse_fdw_options(sql);

    Ok(Some(DdlCommand::CreateForeignDataWrapper {
        name,
        handler,
        validator,
        options,
    }))
}

/// Parse CREATE SERVER name FOREIGN DATA WRAPPER wrapper [TYPE type] [VERSION version] [OPTIONS (...)]
fn parse_create_foreign_server(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Get server name
    let start = upper
        .find("CREATE SERVER ")
        .ok_or_else(|| EngineError::InvalidArgument("Invalid CREATE SERVER syntax".into()))?
        + "CREATE SERVER ".len();

    let rest = &sql[start..];
    let fdw_pos = rest
        .to_uppercase()
        .find(" FOREIGN DATA WRAPPER ")
        .ok_or_else(|| {
            EngineError::InvalidArgument(
                "CREATE SERVER requires FOREIGN DATA WRAPPER clause".into(),
            )
        })?;

    let name = rest[..fdw_pos].trim().to_string();
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE SERVER requires a server name".into(),
        ));
    }

    // Get wrapper name
    let wrapper_start = fdw_pos + " FOREIGN DATA WRAPPER ".len();
    let wrapper_rest = &rest[wrapper_start..];
    let wrapper_end = wrapper_rest
        .to_uppercase()
        .find(" TYPE")
        .or_else(|| wrapper_rest.to_uppercase().find(" VERSION"))
        .or_else(|| wrapper_rest.to_uppercase().find(" OPTIONS"))
        .unwrap_or(wrapper_rest.len());

    let wrapper_name = wrapper_rest[..wrapper_end]
        .trim()
        .trim_end_matches(';')
        .to_string();

    // Parse TYPE
    let server_type = if let Some(t_pos) = upper.find(" TYPE ") {
        let t_start = t_pos + " TYPE ".len();
        let t_rest = &sql[t_start..];
        let t_end = t_rest
            .to_uppercase()
            .find(" VERSION")
            .or_else(|| t_rest.to_uppercase().find(" OPTIONS"))
            .unwrap_or(t_rest.len());
        Some(
            t_rest[..t_end]
                .trim()
                .trim_end_matches(';')
                .trim_matches('\'')
                .to_string(),
        )
    } else {
        None
    };

    // Parse VERSION
    let version = if let Some(v_pos) = upper.find(" VERSION ") {
        let v_start = v_pos + " VERSION ".len();
        let v_rest = &sql[v_start..];
        let v_end = v_rest
            .to_uppercase()
            .find(" OPTIONS")
            .unwrap_or(v_rest.len());
        Some(
            v_rest[..v_end]
                .trim()
                .trim_end_matches(';')
                .trim_matches('\'')
                .to_string(),
        )
    } else {
        None
    };

    let options = parse_fdw_options(sql);

    Ok(Some(DdlCommand::CreateForeignServer {
        name,
        wrapper_name,
        server_type,
        version,
        options,
    }))
}

/// Parse CREATE USER MAPPING FOR user SERVER server [OPTIONS (...)]
fn parse_create_user_mapping(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Get local user
    let for_pos = upper.find(" FOR ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE USER MAPPING requires FOR clause".into())
    })?;

    let user_start = for_pos + " FOR ".len();
    let server_pos = upper[user_start..].find(" SERVER ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE USER MAPPING requires SERVER clause".into())
    })?;

    let local_user = sql[user_start..user_start + server_pos].trim().to_string();

    // Get server name
    let srv_start = user_start + server_pos + " SERVER ".len();
    let srv_rest = &sql[srv_start..];
    let srv_end = srv_rest
        .to_uppercase()
        .find(" OPTIONS")
        .unwrap_or(srv_rest.len());

    let server_name = srv_rest[..srv_end].trim().trim_end_matches(';').to_string();

    let options = parse_fdw_options(sql);

    // Look for remote user in options
    let remote_user = options
        .iter()
        .find(|(k, _)| k.to_uppercase() == "USER")
        .map(|(_, v)| v.clone());

    Ok(Some(DdlCommand::CreateUserMapping {
        local_user,
        server_name,
        remote_user,
        options,
    }))
}

/// Parse DROP USER MAPPING [IF EXISTS] FOR user SERVER server
fn parse_drop_user_mapping(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    let if_exists = upper.contains(" IF EXISTS ");

    // Get local user
    let for_pos = upper.find(" FOR ").ok_or_else(|| {
        EngineError::InvalidArgument("DROP USER MAPPING requires FOR clause".into())
    })?;

    let user_start = for_pos + " FOR ".len();
    let server_pos = upper[user_start..].find(" SERVER ").ok_or_else(|| {
        EngineError::InvalidArgument("DROP USER MAPPING requires SERVER clause".into())
    })?;

    let local_user = sql[user_start..user_start + server_pos].trim().to_string();

    // Get server name
    let srv_start = user_start + server_pos + " SERVER ".len();
    let server_name = sql[srv_start..].trim().trim_end_matches(';').to_string();

    Ok(Some(DdlCommand::DropUserMapping {
        local_user,
        server_name,
        if_exists,
    }))
}

/// Parse CREATE FOREIGN TABLE name (...) SERVER server [OPTIONS (...)]
fn parse_create_foreign_table(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Get table name
    let start = upper.find("CREATE FOREIGN TABLE ").ok_or_else(|| {
        EngineError::InvalidArgument("Invalid CREATE FOREIGN TABLE syntax".into())
    })? + "CREATE FOREIGN TABLE ".len();

    let rest = &sql[start..];
    let paren_pos = rest.find('(').ok_or_else(|| {
        EngineError::InvalidArgument("CREATE FOREIGN TABLE requires columns".into())
    })?;

    let name = rest[..paren_pos].trim().to_string();
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(
            "CREATE FOREIGN TABLE requires a table name".into(),
        ));
    }

    // Find matching closing parenthesis for columns
    let mut depth = 0;
    let mut col_end = paren_pos;
    for (i, c) in rest[paren_pos..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    col_end = paren_pos + i;
                    break;
                }
            }
            _ => {}
        }
    }

    let columns_str = &rest[paren_pos + 1..col_end];
    let columns = parse_foreign_table_columns(columns_str)?;

    // Find SERVER clause
    let after_cols = &rest[col_end + 1..];
    let upper_after = after_cols.to_uppercase();
    let server_pos = upper_after
        .find(" SERVER ")
        .or_else(|| upper_after.find("SERVER "));

    let server_name = if let Some(sp) = server_pos {
        let srv_start = sp + " SERVER ".len().min(sp + "SERVER ".len());
        let srv_rest = &after_cols[srv_start..];
        let srv_end = srv_rest
            .to_uppercase()
            .find(" OPTIONS")
            .unwrap_or(srv_rest.len());
        srv_rest[..srv_end].trim().trim_end_matches(';').to_string()
    } else {
        return Err(EngineError::InvalidArgument(
            "CREATE FOREIGN TABLE requires SERVER clause".into(),
        ));
    };

    let options = parse_fdw_options(sql);

    Ok(Some(DdlCommand::CreateForeignTable {
        name,
        server_name,
        columns,
        options,
    }))
}

/// Parse foreign table columns
fn parse_foreign_table_columns(cols_str: &str) -> Result<Vec<ForeignTableColumn>, EngineError> {
    let mut columns = Vec::new();

    for col_def in cols_str.split(',') {
        let trimmed = col_def.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }

        let name = parts[0].to_string();
        let data_type = parts[1].to_string();
        let nullable = !trimmed.to_uppercase().contains("NOT NULL");

        // Parse column options if present
        let options = if trimmed.to_uppercase().contains("OPTIONS") {
            parse_fdw_options(trimmed)
        } else {
            Vec::new()
        };

        columns.push(ForeignTableColumn {
            name,
            data_type,
            nullable,
            options,
        });
    }

    Ok(columns)
}

/// Parse IMPORT FOREIGN SCHEMA schema FROM SERVER server INTO database [LIMIT TO (tables)]
fn parse_import_foreign_schema(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();

    // Get remote schema
    let start = upper.find("IMPORT FOREIGN SCHEMA ").ok_or_else(|| {
        EngineError::InvalidArgument("Invalid IMPORT FOREIGN SCHEMA syntax".into())
    })? + "IMPORT FOREIGN SCHEMA ".len();

    let rest = &sql[start..];
    let from_pos = rest.to_uppercase().find(" FROM SERVER ").ok_or_else(|| {
        EngineError::InvalidArgument("IMPORT FOREIGN SCHEMA requires FROM SERVER clause".into())
    })?;

    let remote_schema = rest[..from_pos].trim().to_string();

    // Get server name
    let srv_start = from_pos + " FROM SERVER ".len();
    let srv_rest = &rest[srv_start..];
    let into_pos = srv_rest.to_uppercase().find(" INTO ").ok_or_else(|| {
        EngineError::InvalidArgument("IMPORT FOREIGN SCHEMA requires INTO clause".into())
    })?;

    let server_name = srv_rest[..into_pos].trim().to_string();

    // Get local database
    let db_start = srv_start + into_pos + " INTO ".len();
    let db_rest = &rest[db_start..];
    let db_end = db_rest
        .to_uppercase()
        .find(" LIMIT")
        .unwrap_or(db_rest.len());

    let local_database = db_rest[..db_end].trim().trim_end_matches(';').to_string();

    // Parse LIMIT TO if present
    let limit_to = if upper.contains(" LIMIT TO ") {
        if let Some(lt_pos) = upper.find(" LIMIT TO (") {
            let lt_start = lt_pos + " LIMIT TO (".len();
            if let Some(lt_end) = sql[lt_start..].find(')') {
                let tables_str = &sql[lt_start..lt_start + lt_end];
                Some(
                    tables_str
                        .split(',')
                        .map(|t| t.trim().to_string())
                        .collect(),
                )
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(Some(DdlCommand::ImportForeignSchema {
        remote_schema,
        server_name,
        local_database,
        limit_to,
    }))
}

/// Parse function parameters: name type [DEFAULT value], ...
fn parse_function_parameters(params_str: &str) -> Result<Vec<FunctionParameter>, EngineError> {
    let trimmed = params_str.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut parameters = Vec::new();
    let mut current_param = String::new();
    let mut in_quotes = false;
    let mut paren_depth = 0;

    for c in trimmed.chars() {
        match c {
            '\'' | '"' => {
                in_quotes = !in_quotes;
                current_param.push(c);
            }
            '(' => {
                paren_depth += 1;
                current_param.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current_param.push(c);
            }
            ',' if !in_quotes && paren_depth == 0 => {
                let param = parse_single_parameter(current_param.trim())?;
                parameters.push(param);
                current_param.clear();
            }
            _ => {
                current_param.push(c);
            }
        }
    }

    // Don't forget the last parameter
    if !current_param.trim().is_empty() {
        let param = parse_single_parameter(current_param.trim())?;
        parameters.push(param);
    }

    Ok(parameters)
}

/// Parse a single parameter: name type [DEFAULT value]
fn parse_single_parameter(param_str: &str) -> Result<FunctionParameter, EngineError> {
    let upper = param_str.to_uppercase();

    // Check for DEFAULT keyword
    let default_pos = upper.find(" DEFAULT ");
    let (name_type_part, default_value) = if let Some(pos) = default_pos {
        let default_val = param_str[pos + 9..].trim();
        (&param_str[..pos], Some(default_val.to_string()))
    } else {
        (param_str, None)
    };

    // Split name and type
    let parts: Vec<&str> = name_type_part.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(EngineError::InvalidArgument(format!(
            "Invalid parameter syntax: '{}'",
            param_str
        )));
    }

    let name = parts[0].to_string();
    let data_type = parts[1..].join(" ");

    Ok(FunctionParameter {
        name,
        data_type,
        default_value,
    })
}

/// Extract a quoted function body string and return (content, remaining)
fn extract_function_body_string(s: &str) -> Result<(String, &str), EngineError> {
    let trimmed = s.trim();
    let quote_char = if trimmed.starts_with('\'') {
        '\''
    } else if trimmed.starts_with('"') {
        '"'
    } else if trimmed.starts_with("$$") {
        // Dollar-quoted string
        let end = trimmed[2..]
            .find("$$")
            .ok_or_else(|| EngineError::InvalidArgument("Unterminated $$ string".into()))?;
        let content = trimmed[2..2 + end].to_string();
        let remaining = &trimmed[2 + end + 2..];
        return Ok((content, remaining));
    } else {
        return Err(EngineError::InvalidArgument(
            "Function body must be a quoted string".into(),
        ));
    };

    // Find the matching closing quote (handle escaped quotes)
    let mut chars = trimmed[1..].chars().peekable();
    let mut content = String::new();
    let mut end_idx = 1;

    while let Some(c) = chars.next() {
        end_idx += c.len_utf8();
        if c == quote_char {
            // Check for escaped quote (doubled)
            if chars.peek() == Some(&quote_char) {
                content.push(quote_char);
                chars.next();
                end_idx += quote_char.len_utf8();
            } else {
                // End of string
                let remaining = &trimmed[end_idx..];
                return Ok((content, remaining));
            }
        } else {
            content.push(c);
        }
    }

    Err(EngineError::InvalidArgument(
        "Unterminated quoted string".into(),
    ))
}

/// Parse CREATE MATERIALIZED VIEW command
fn parse_create_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    // Find the AS keyword to split view name from query
    let as_pos = upper.find(" AS ").ok_or_else(|| {
        EngineError::InvalidArgument("CREATE MATERIALIZED VIEW requires AS keyword".into())
    })?;

    // Extract the view name part
    let prefix = if or_replace {
        "CREATE OR REPLACE MATERIALIZED VIEW"
    } else {
        "CREATE MATERIALIZED VIEW"
    };
    let after_prefix = sql[prefix.len()..as_pos].trim();
    let view_name = after_prefix.trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    // Extract the query SQL
    let query_sql = sql[as_pos + 4..].trim().trim_end_matches(';').to_string();

    Ok(Some(DdlCommand::CreateMaterializedView {
        database,
        name,
        query_sql,
        or_replace,
    }))
}

/// Parse DROP MATERIALIZED VIEW command
fn parse_drop_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // DROP MATERIALIZED VIEW [IF EXISTS] name

    let mut idx = 3; // Start after "DROP MATERIALIZED VIEW"
    let mut if_exists = false;

    if tokens.len() > idx + 1
        && tokens[idx].eq_ignore_ascii_case("IF")
        && tokens[idx + 1].eq_ignore_ascii_case("EXISTS")
    {
        if_exists = true;
        idx += 2;
    }

    if idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "DROP MATERIALIZED VIEW requires view name".into(),
        ));
    }

    let view_name = tokens[idx].trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    Ok(Some(DdlCommand::DropMaterializedView {
        database,
        name,
        if_exists,
    }))
}

/// Parse REFRESH MATERIALIZED VIEW command
fn parse_refresh_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // REFRESH MATERIALIZED VIEW name [INCREMENTAL]

    if tokens.len() < 4 {
        return Err(EngineError::InvalidArgument(
            "REFRESH MATERIALIZED VIEW requires view name".into(),
        ));
    }

    let view_name = tokens[3].trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    // Check for INCREMENTAL keyword
    let incremental = tokens.len() > 4
        && tokens[4]
            .trim_end_matches(';')
            .eq_ignore_ascii_case("INCREMENTAL");

    Ok(Some(DdlCommand::RefreshMaterializedView {
        database,
        name,
        incremental,
    }))
}

/// Parse ALTER TABLE ... SET DEDUPLICATION or DROP DEDUPLICATION
fn parse_set_deduplication(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // ALTER TABLE table_name DROP DEDUPLICATION
    if upper.contains("DROP DEDUPLICATION") {
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "ALTER TABLE DROP DEDUPLICATION requires table name".into(),
            ));
        }
        let table_name = tokens[2].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::SetDeduplication {
            database,
            table,
            config: None,
        }));
    }

    // ALTER TABLE table_name SET DEDUPLICATION (col1, col2) [VERSION version_col] [MODE mode]
    if !upper.contains("SET DEDUPLICATION") {
        return Ok(None);
    }

    if tokens.len() < 5 {
        return Err(EngineError::InvalidArgument(
            "ALTER TABLE SET DEDUPLICATION requires table name and key columns".into(),
        ));
    }

    let table_name = tokens[2];
    let (database, table) = parse_table_name(table_name)?;

    // Find opening parenthesis for key columns
    let sql_after_set = if let Some(pos) = upper.find("SET DEDUPLICATION") {
        &sql[pos + "SET DEDUPLICATION".len()..]
    } else {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires key columns in parentheses".into(),
        ));
    };

    let trimmed = sql_after_set.trim();
    if !trimmed.starts_with('(') {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires key columns in parentheses".into(),
        ));
    }

    // Find closing parenthesis
    let close_paren = trimmed.find(')').ok_or_else(|| {
        EngineError::InvalidArgument("SET DEDUPLICATION: missing closing parenthesis".into())
    })?;

    let cols_str = &trimmed[1..close_paren];
    let key_columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if key_columns.is_empty() {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires at least one key column".into(),
        ));
    }

    let after_cols = &trimmed[close_paren + 1..];
    let after_tokens: Vec<&str> = after_cols.split_whitespace().collect();

    // Parse optional VERSION column
    let mut version_column = None;
    let mut mode = DeduplicationMode::OnCompaction;

    let mut i = 0;
    while i < after_tokens.len() {
        let token_upper = after_tokens[i].to_uppercase();
        if token_upper == "VERSION" && i + 1 < after_tokens.len() {
            version_column = Some(after_tokens[i + 1].trim_end_matches(';').to_string());
            i += 2;
        } else if token_upper == "MODE" && i + 1 < after_tokens.len() {
            let mode_str = after_tokens[i + 1].to_uppercase();
            let mode_str = mode_str.trim_end_matches(';');
            mode = match mode_str {
                "ONINGEST" | "ON_INGEST" => DeduplicationMode::OnIngest,
                "ONCOMPACTION" | "ON_COMPACTION" => DeduplicationMode::OnCompaction,
                "BOTH" => DeduplicationMode::Both,
                _ => {
                    return Err(EngineError::InvalidArgument(format!(
                        "invalid deduplication mode: {}. Use ONINGEST, ONCOMPACTION, or BOTH",
                        after_tokens[i + 1]
                    )));
                }
            };
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(Some(DdlCommand::SetDeduplication {
        database,
        table,
        config: Some(DeduplicationConfig {
            key_columns,
            version_column,
            mode,
        }),
    }))
}

/// Parse duration string into seconds
/// Supports: 1d, 7d, 30d, 1h, 24h, 1w, 1m (month), 1y
fn parse_duration_to_seconds(s: &str) -> Result<u64, EngineError> {
    let s = s.trim().to_lowercase();
    let s = s.trim_end_matches(';');

    // Try to split into number and unit
    let (num_str, unit) = if s.ends_with("days") || s.ends_with("day") {
        let num_str = s.trim_end_matches("days").trim_end_matches("day").trim();
        (num_str, "d")
    } else if s.ends_with("hours") || s.ends_with("hour") {
        let num_str = s.trim_end_matches("hours").trim_end_matches("hour").trim();
        (num_str, "h")
    } else if s.ends_with("weeks") || s.ends_with("week") {
        let num_str = s.trim_end_matches("weeks").trim_end_matches("week").trim();
        (num_str, "w")
    } else if s.ends_with("months") || s.ends_with("month") {
        let num_str = s
            .trim_end_matches("months")
            .trim_end_matches("month")
            .trim();
        (num_str, "m")
    } else if s.ends_with("years") || s.ends_with("year") {
        let num_str = s.trim_end_matches("years").trim_end_matches("year").trim();
        (num_str, "y")
    } else if s.ends_with('d') {
        (&s[..s.len() - 1], "d")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else if s.ends_with('w') {
        (&s[..s.len() - 1], "w")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('y') {
        (&s[..s.len() - 1], "y")
    } else if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else {
        // Assume seconds if no unit
        (s, "s")
    };

    let num: u64 = num_str.parse::<u64>().map_err(|_| {
        EngineError::InvalidArgument(format!("invalid retention duration number: {}", num_str))
    })?;

    let seconds = match unit {
        "s" => num,
        "h" => num * 3600,
        "d" => num * 86400,
        "w" => num * 7 * 86400,
        "m" => num * 30 * 86400,  // Approximate month
        "y" => num * 365 * 86400, // Approximate year
        _ => {
            return Err(EngineError::InvalidArgument(format!(
                "invalid duration unit: {}. Use s, h, d, w, m, or y",
                unit
            )))
        }
    };

    Ok(seconds)
}

/// Parse ALTER TABLE ... SET RETENTION or DROP RETENTION
fn parse_set_retention(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // ALTER TABLE table_name DROP RETENTION
    if upper.contains("DROP RETENTION") {
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "ALTER TABLE DROP RETENTION requires table name".into(),
            ));
        }
        let table_name = tokens[2].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::DropRetention { database, table }));
    }

    // ALTER TABLE table_name SET RETENTION <duration> [ON column]
    if !upper.contains("SET RETENTION") {
        return Ok(None);
    }

    if tokens.len() < 5 {
        return Err(EngineError::InvalidArgument(
            "ALTER TABLE SET RETENTION requires table name and duration".into(),
        ));
    }

    let table_name = tokens[2];
    let (database, table) = parse_table_name(table_name)?;

    // Find "SET RETENTION" position and get duration
    let set_idx = tokens
        .iter()
        .position(|t| t.to_uppercase() == "SET")
        .unwrap_or(0);
    let retention_idx = set_idx + 1; // "RETENTION"
    let duration_idx = retention_idx + 1;

    if duration_idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "SET RETENTION requires a duration (e.g., 30d, 1y, 24h)".into(),
        ));
    }

    let duration_str = tokens[duration_idx];
    let retention_seconds = parse_duration_to_seconds(duration_str)?;

    // Check for optional ON column
    let mut time_column = None;
    if duration_idx + 2 < tokens.len() && tokens[duration_idx + 1].to_uppercase() == "ON" {
        time_column = Some(tokens[duration_idx + 2].trim_end_matches(';').to_string());
    }

    Ok(Some(DdlCommand::SetRetention {
        database,
        table,
        retention_seconds,
        time_column,
    }))
}

/// Parse SHOW RETENTION [FOR database.table]
fn parse_show_retention(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // SHOW RETENTION - show all retention policies
    if tokens.len() == 2 {
        return Ok(Some(DdlCommand::ShowRetention {
            database: None,
            table: None,
        }));
    }

    // SHOW RETENTION FOR database.table
    if tokens.len() >= 4 && tokens[2].to_uppercase() == "FOR" {
        let table_name = tokens[3].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ShowRetention {
            database: Some(database),
            table: Some(table),
        }));
    }

    Ok(Some(DdlCommand::ShowRetention {
        database: None,
        table: None,
    }))
}

/// Parse ALTER TABLE ... PARTITION BY or DROP PARTITION
fn parse_partition_command(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // ALTER TABLE table_name DROP PARTITION
    if upper.contains("DROP PARTITION") {
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "ALTER TABLE DROP PARTITION requires table name".into(),
            ));
        }
        let table_name = tokens[2].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::DropPartition { database, table }));
    }

    // ALTER TABLE table_name PARTITION BY granularity ON time_column
    if !upper.contains("PARTITION BY") {
        return Ok(None);
    }

    if tokens.len() < 7 {
        return Err(EngineError::InvalidArgument(
            "ALTER TABLE PARTITION BY requires table name, granularity, and time column".into(),
        ));
    }

    let table_name = tokens[2];
    let (database, table) = parse_table_name(table_name)?;

    // Find "PARTITION BY" position
    let partition_idx = tokens
        .iter()
        .position(|t| t.to_uppercase() == "PARTITION")
        .unwrap_or(0);
    let by_idx = partition_idx + 1; // "BY"
    let granularity_idx = by_idx + 1;

    if granularity_idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "PARTITION BY requires a granularity (hour, day, week, month, year)".into(),
        ));
    }

    let granularity = tokens[granularity_idx].trim_end_matches(';').to_lowercase();

    // Check for optional ON column
    let time_column = if granularity_idx + 2 < tokens.len()
        && tokens[granularity_idx + 1].to_uppercase() == "ON"
    {
        tokens[granularity_idx + 2]
            .trim_end_matches(';')
            .to_string()
    } else {
        "event_time".to_string()
    };

    Ok(Some(DdlCommand::SetPartition {
        database,
        table,
        granularity,
        time_column,
    }))
}

/// Parse SHOW PARTITIONS [FOR database.table]
fn parse_show_partitions(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // SHOW PARTITIONS - show all partition configs
    if tokens.len() == 2 {
        return Ok(Some(DdlCommand::ShowPartitions {
            database: None,
            table: None,
        }));
    }

    // SHOW PARTITIONS FOR database.table
    if tokens.len() >= 4 && tokens[2].to_uppercase() == "FOR" {
        let table_name = tokens[3].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::ShowPartitions {
            database: Some(database),
            table: Some(table),
        }));
    }

    Ok(Some(DdlCommand::ShowPartitions {
        database: None,
        table: None,
    }))
}

/// Parse ANALYZE TABLE database.table [COLUMNS (col1, col2, ...)]
fn parse_analyze_table(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // ANALYZE TABLE table_name
    // or ANALYZE table_name
    let table_idx = if tokens.len() >= 3 && tokens[1].to_uppercase() == "TABLE" {
        2
    } else if tokens.len() >= 2 {
        1
    } else {
        return Err(EngineError::InvalidArgument(
            "ANALYZE requires table name".into(),
        ));
    };

    if table_idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "ANALYZE requires table name".into(),
        ));
    }

    let table_name = tokens[table_idx].trim_end_matches(';');
    let (database, table) = parse_table_name(table_name)?;

    // Check for COLUMNS clause
    let columns = if upper.contains("COLUMNS") || upper.contains("COLUMN") {
        // Find the opening parenthesis
        if let Some(paren_start) = sql.find('(') {
            if let Some(paren_end) = sql.find(')') {
                let cols_str = &sql[paren_start + 1..paren_end];
                let cols: Vec<String> = cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !cols.is_empty() {
                    Some(cols)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(Some(DdlCommand::AnalyzeTable {
        database,
        table,
        columns,
    }))
}

/// Parse SHOW STATISTICS FOR database.table [COLUMN column_name]
fn parse_show_statistics(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // SHOW STATISTICS FOR table_name or SHOW STATS FOR table_name
    if tokens.len() < 4 {
        return Err(EngineError::InvalidArgument(
            "SHOW STATISTICS requires FOR table_name".into(),
        ));
    }

    // Handle both "SHOW STATISTICS" and "SHOW STATS"
    let for_idx = if tokens[1].to_uppercase() == "STATISTICS" || tokens[1].to_uppercase() == "STATS"
    {
        if tokens[2].to_uppercase() == "FOR" {
            3
        } else {
            return Err(EngineError::InvalidArgument(
                "SHOW STATISTICS requires FOR keyword".into(),
            ));
        }
    } else {
        return Err(EngineError::InvalidArgument(
            "Invalid SHOW STATISTICS syntax".into(),
        ));
    };

    if for_idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "SHOW STATISTICS FOR requires table name".into(),
        ));
    }

    let table_name = tokens[for_idx].trim_end_matches(';');
    let (database, table) = parse_table_name(table_name)?;

    // Check for COLUMN clause
    let column = if upper.contains(" COLUMN ") {
        let col_idx = tokens
            .iter()
            .position(|t| t.to_uppercase() == "COLUMN")
            .unwrap_or(0);
        if col_idx + 1 < tokens.len() {
            Some(tokens[col_idx + 1].trim_end_matches(';').to_string())
        } else {
            None
        }
    } else {
        None
    };

    Ok(Some(DdlCommand::ShowStatistics {
        database,
        table,
        column,
    }))
}

fn parse_drop_database_fallback(sql: &str) -> Option<SqlStatement> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 3 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("drop") || !tokens[1].eq_ignore_ascii_case("database") {
        return None;
    }
    let mut idx = 2;
    let mut if_exists = false;
    if tokens.len() >= 4
        && tokens[2].eq_ignore_ascii_case("if")
        && tokens[3].eq_ignore_ascii_case("exists")
    {
        if_exists = true;
        idx = 4;
    }
    let name = tokens.get(idx)?;
    let name_clean = name.trim_end_matches(';');
    Some(SqlStatement::Ddl(DdlCommand::DropDatabase {
        name: name_clean.to_string(),
        if_exists,
    }))
}

/// Try to parse a SQL string as a pub/sub command (LISTEN, UNLISTEN, NOTIFY)
/// Returns None if not a pub/sub command, Some(cmd) if parsed successfully, Err on parse error
fn try_parse_pubsub_command(sql: &str) -> Result<Option<PubSubCommand>, EngineError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let upper_trimmed = upper.trim_end_matches(';');

    // LISTEN channel_name
    if upper_trimmed.starts_with("LISTEN ") {
        let channel = sql[7..].trim().trim_end_matches(';');
        let channel = channel.trim_matches('"').trim_matches('\'');
        if channel.is_empty() {
            return Err(EngineError::InvalidArgument(
                "LISTEN requires a channel name".into(),
            ));
        }
        return Ok(Some(PubSubCommand::Listen {
            channel: channel.to_string(),
        }));
    }

    // UNLISTEN channel_name | UNLISTEN *
    if upper_trimmed.starts_with("UNLISTEN ") {
        let channel = sql[9..].trim().trim_end_matches(';');
        let channel = channel.trim_matches('"').trim_matches('\'');
        if channel.is_empty() {
            return Err(EngineError::InvalidArgument(
                "UNLISTEN requires a channel name or *".into(),
            ));
        }
        return Ok(Some(PubSubCommand::Unlisten {
            channel: channel.to_string(),
        }));
    }

    // NOTIFY channel_name [, 'payload']
    if upper_trimmed.starts_with("NOTIFY ") {
        let rest = sql[7..].trim().trim_end_matches(';');

        // Check for payload (comma-separated)
        if let Some(comma_pos) = rest.find(',') {
            let channel = rest[..comma_pos]
                .trim()
                .trim_matches('"')
                .trim_matches('\'');
            let payload = rest[comma_pos + 1..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"');

            if channel.is_empty() {
                return Err(EngineError::InvalidArgument(
                    "NOTIFY requires a channel name".into(),
                ));
            }

            return Ok(Some(PubSubCommand::Notify {
                channel: channel.to_string(),
                payload: Some(payload.to_string()),
            }));
        } else {
            let channel = rest.trim_matches('"').trim_matches('\'');
            if channel.is_empty() {
                return Err(EngineError::InvalidArgument(
                    "NOTIFY requires a channel name".into(),
                ));
            }

            return Ok(Some(PubSubCommand::Notify {
                channel: channel.to_string(),
                payload: None,
            }));
        }
    }

    Ok(None)
}

/// Try to parse a SQL string as an auth command
/// Returns None if not an auth command, Some(cmd) if parsed successfully, Err on parse error
fn try_parse_auth_command(sql: &str) -> Result<Option<AuthCommand>, EngineError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    if tokens.is_empty() {
        return Ok(None);
    }

    // CREATE USER username WITH PASSWORD 'password' [SUPERUSER] [DEFAULT DATABASE 'db'] [CONNECTION LIMIT n]
    if upper.starts_with("CREATE USER ") {
        return parse_create_user(sql).map(Some);
    }

    // DROP USER username
    if upper.starts_with("DROP USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("DROP USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::DropUser { username }));
    }

    // ALTER USER username SET PASSWORD 'newpass'
    // ALTER USER username SET SUPERUSER TRUE/FALSE
    // ALTER USER username SET DEFAULT DATABASE 'dbname'
    if upper.starts_with("ALTER USER ") {
        return parse_alter_user(sql).map(Some);
    }

    // LOCK USER username
    if upper.starts_with("LOCK USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("LOCK USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::LockUser { username }));
    }

    // UNLOCK USER username
    if upper.starts_with("UNLOCK USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("UNLOCK USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::UnlockUser { username }));
    }

    // CREATE ROLE rolename [WITH DESCRIPTION 'desc']
    if upper.starts_with("CREATE ROLE ") {
        return parse_create_role(sql).map(Some);
    }

    // DROP ROLE rolename
    if upper.starts_with("DROP ROLE ") {
        let name = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("DROP ROLE requires role name".into()))?;
        let name = unquote(name);
        validate_ident(&name, false, "role name")?;
        return Ok(Some(AuthCommand::DropRole { name }));
    }

    // GRANT ... TO ...
    if upper.starts_with("GRANT ") {
        return parse_grant(sql).map(Some);
    }

    // REVOKE ... FROM ...
    if upper.starts_with("REVOKE ") {
        return parse_revoke(sql).map(Some);
    }

    // SHOW USERS
    if upper == "SHOW USERS" || upper == "SHOW USERS;" {
        return Ok(Some(AuthCommand::ShowUsers));
    }

    // SHOW ROLES
    if upper == "SHOW ROLES" || upper == "SHOW ROLES;" {
        return Ok(Some(AuthCommand::ShowRoles));
    }

    // SHOW GRANTS FOR username
    if upper.starts_with("SHOW GRANTS FOR ") {
        let username = tokens.get(3).ok_or_else(|| {
            EngineError::InvalidArgument("SHOW GRANTS FOR requires username".into())
        })?;
        let username = unquote(username).trim_end_matches(';').to_string();
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::ShowGrants { username }));
    }

    Ok(None)
}

/// Parse CREATE USER command
fn parse_create_user(sql: &str) -> Result<AuthCommand, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let username = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("CREATE USER requires username".into()))?;
    let username = unquote(username);
    validate_ident(&username, false, "username")?;

    // Locate the PASSWORD keyword by token to avoid substring collisions
    let password_token_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("PASSWORD"))
        .ok_or_else(|| EngineError::InvalidArgument("CREATE USER requires PASSWORD".into()))?;

    // Extract password (look for quoted string after PASSWORD)
    let after_password =
        tokens
            .iter()
            .skip(password_token_idx + 1)
            .fold(String::new(), |mut acc, t| {
                if !acc.is_empty() {
                    acc.push(' ');
                }
                acc.push_str(t);
                acc
            });

    let password = extract_quoted_string(&after_password).ok_or_else(|| {
        EngineError::InvalidArgument("PASSWORD must be followed by a quoted string".into())
    })?;

    let mut options = UserOptions::default();

    // Parse options from remaining tokens, ignoring quoted strings
    let mut i = password_token_idx + 1;
    while i < tokens.len() {
        let tok = tokens[i];
        let upper = tok.to_uppercase();

        if upper == "SUPERUSER" {
            options.superuser = true;
            i += 1;
            continue;
        }

        if upper == "DEFAULT" {
            if let (Some(db_kw), Some(db_token)) = (tokens.get(i + 1), tokens.get(i + 2)) {
                if db_kw.eq_ignore_ascii_case("DATABASE") {
                    let database = unquote(db_token.trim_end_matches(';'));
                    validate_ident(&database, false, "database")?;
                    options.default_database.replace(database);
                    i += 3;
                    continue;
                }
            }
        }

        if upper == "CONNECTION" {
            if let (Some(limit_kw), Some(limit_token)) = (tokens.get(i + 1), tokens.get(i + 2)) {
                if limit_kw.eq_ignore_ascii_case("LIMIT") {
                    let numeric = limit_token
                        .trim_end_matches(';')
                        .trim_matches(|c: char| !c.is_ascii_digit());
                    if let Ok(limit) = numeric.parse() {
                        options.connection_limit = Some(limit);
                    }
                    i += 3;
                    continue;
                }
            }
        }

        i += 1;
    }

    Ok(AuthCommand::CreateUser {
        username,
        password,
        options,
    })
}

/// Parse ALTER USER command
fn parse_alter_user(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let username = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("ALTER USER requires username".into()))?;
    let username = unquote(username);
    validate_ident(&username, false, "username")?;

    // ALTER USER username SET PASSWORD 'newpass'
    if upper.contains("SET PASSWORD") {
        if let Some(password_idx) = upper.find("PASSWORD") {
            let after_password = &sql[password_idx + 8..];
            let new_password = extract_quoted_string(after_password).ok_or_else(|| {
                EngineError::InvalidArgument("PASSWORD must be followed by a quoted string".into())
            })?;
            return Ok(AuthCommand::AlterUserPassword {
                username,
                new_password,
            });
        }
    }

    // ALTER USER username SET SUPERUSER TRUE/FALSE
    if upper.contains("SET SUPERUSER") {
        let is_superuser = upper.contains("TRUE") || upper.contains("YES") || upper.contains(" 1");
        return Ok(AuthCommand::AlterUserSuperuser {
            username,
            is_superuser,
        });
    }

    // ALTER USER username SET DEFAULT DATABASE 'dbname'
    if upper.contains("SET DEFAULT DATABASE") {
        if let Some(idx) = upper.find("DATABASE") {
            let after_db = &sql[idx + 8..];
            let database = extract_quoted_string(after_db.trim());
            let database = database.ok_or_else(|| {
                EngineError::InvalidArgument(
                    "DEFAULT DATABASE must be followed by a quoted string".into(),
                )
            })?;
            validate_ident(&database, false, "database")?;
            return Ok(AuthCommand::AlterUserDefaultDb {
                username,
                database: Some(database),
            });
        }
    }

    Err(EngineError::InvalidArgument(
        "Unrecognized ALTER USER command".into(),
    ))
}

/// Parse CREATE ROLE command
fn parse_create_role(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let name = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("CREATE ROLE requires role name".into()))?;
    let name = unquote(name);
    validate_ident(&name, false, "role name")?;

    let mut description = None;

    // Check for DESCRIPTION or WITH DESCRIPTION
    if let Some(idx) = upper.find("DESCRIPTION") {
        let after_desc = &sql[idx + 11..];
        description = extract_quoted_string(after_desc.trim());
    }

    Ok(AuthCommand::CreateRole { name, description })
}

/// Parse GRANT command
fn parse_grant(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();

    // Check if this is GRANT role TO user (no ON keyword)
    if !upper.contains(" ON ") {
        if let Some(to_idx) = upper.find(" TO ") {
            // GRANT rolename TO username
            let role = sql[6..to_idx].trim();
            let after_to = &sql[to_idx + 4..];
            let username = after_to.trim().trim_end_matches(';');
            let role = unquote(role);
            let username = unquote(username);
            validate_ident(&role, false, "role name")?;
            validate_ident(&username, false, "username")?;

            return Ok(AuthCommand::GrantRole { role, username });
        }
    }

    // GRANT privilege(s) ON target TO grantee [WITH GRANT OPTION]
    // Also support column-level: GRANT SELECT (col1, col2) ON table TO user
    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| EngineError::InvalidArgument("GRANT requires ON clause".into()))?;
    let to_idx = upper
        .find(" TO ")
        .ok_or_else(|| EngineError::InvalidArgument("GRANT requires TO clause".into()))?;

    let privileges_str = &sql[6..on_idx];

    // Check for column-level grant: SELECT (col1, col2)
    let (privileges, columns) = if let (Some(paren_start), Some(paren_end)) =
        (privileges_str.find('('), privileges_str.find(')'))
    {
        // Extract privilege before parentheses and columns within
        let priv_part = privileges_str[..paren_start].trim();
        let cols_part = &privileges_str[paren_start + 1..paren_end];
        let cols: Vec<String> = cols_part.split(',').map(|c| c.trim().to_string()).collect();
        (
            vec![priv_part.to_uppercase()],
            if cols.is_empty() { None } else { Some(cols) },
        )
    } else {
        // Regular privilege list
        let privs: Vec<String> = privileges_str
            .split(',')
            .map(|p| p.trim().to_uppercase())
            .collect();
        (privs, None)
    };

    let target_str = &sql[on_idx + 4..to_idx];
    let (target_type, target_name) = parse_grant_target(target_str)?;

    let after_to = &sql[to_idx + 4..];
    let with_grant = upper.contains("WITH GRANT OPTION");

    let grantee_end = if with_grant {
        upper
            .find("WITH GRANT")
            .map(|idx| idx - to_idx - 4)
            .unwrap_or(after_to.len())
    } else {
        after_to.len()
    };

    let grantee = after_to[..grantee_end].trim().trim_end_matches(';');
    let grantee_is_role = upper.contains("TO ROLE ");
    let grantee = unquote(
        grantee
            .trim_start_matches("ROLE ")
            .trim_start_matches("role "),
    );
    validate_ident(&grantee, false, "grantee")?;

    Ok(AuthCommand::Grant {
        privileges,
        target_type,
        target_name,
        columns,
        grantee,
        grantee_is_role,
        with_grant_option: with_grant,
    })
}

/// Parse REVOKE command
fn parse_revoke(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();

    // Check if this is REVOKE role FROM user (no ON keyword)
    if !upper.contains(" ON ") {
        if let Some(from_idx) = upper.find(" FROM ") {
            // REVOKE rolename FROM username
            let role = sql[7..from_idx].trim();
            let after_from = &sql[from_idx + 6..];
            let username = after_from.trim().trim_end_matches(';');
            let role = unquote(role);
            let username = unquote(username);
            validate_ident(&role, false, "role name")?;
            validate_ident(&username, false, "username")?;

            return Ok(AuthCommand::RevokeRole { role, username });
        }
    }

    // REVOKE privilege(s) ON target FROM grantee
    // Also support column-level: REVOKE SELECT (col1, col2) ON table FROM user
    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| EngineError::InvalidArgument("REVOKE requires ON clause".into()))?;
    let from_idx = upper
        .find(" FROM ")
        .ok_or_else(|| EngineError::InvalidArgument("REVOKE requires FROM clause".into()))?;

    let privileges_str = &sql[7..on_idx];

    // Check for column-level revoke: SELECT (col1, col2)
    let (privileges, columns) = if let (Some(paren_start), Some(paren_end)) =
        (privileges_str.find('('), privileges_str.find(')'))
    {
        // Extract privilege before parentheses and columns within
        let priv_part = privileges_str[..paren_start].trim();
        let cols_part = &privileges_str[paren_start + 1..paren_end];
        let cols: Vec<String> = cols_part.split(',').map(|c| c.trim().to_string()).collect();
        (
            vec![priv_part.to_uppercase()],
            if cols.is_empty() { None } else { Some(cols) },
        )
    } else {
        // Regular privilege list
        let privs: Vec<String> = privileges_str
            .split(',')
            .map(|p| p.trim().to_uppercase())
            .collect();
        (privs, None)
    };

    let target_str = &sql[on_idx + 4..from_idx];
    let (target_type, target_name) = parse_grant_target(target_str)?;

    let after_from = &sql[from_idx + 6..];
    let grantee = after_from.trim().trim_end_matches(';');
    let grantee_is_role = upper.contains("FROM ROLE ");
    let grantee = unquote(
        grantee
            .trim_start_matches("ROLE ")
            .trim_start_matches("role "),
    );
    validate_ident(&grantee, false, "grantee")?;

    Ok(AuthCommand::Revoke {
        privileges,
        target_type,
        target_name,
        columns,
        grantee,
        grantee_is_role,
    })
}

/// Parse the target of a GRANT/REVOKE (the part after ON)
fn parse_grant_target(target_str: &str) -> Result<(GrantTargetType, Option<String>), EngineError> {
    let upper = target_str.trim().to_uppercase();

    if upper == "*" || upper == "ALL" {
        return Ok((GrantTargetType::Global, None));
    }

    if upper.starts_with("DATABASE ") {
        let name = target_str.trim()[9..].trim();
        let name = unquote(name);
        validate_ident(&name, false, "database")?;
        return Ok((GrantTargetType::Database, Some(name)));
    }

    if upper.starts_with("ALL TABLES IN DATABASE ") {
        let name = target_str.trim()[23..].trim();
        let name = unquote(name);
        validate_ident(&name, false, "database")?;
        return Ok((GrantTargetType::AllTablesInDatabase, Some(name)));
    }

    if upper.starts_with("TABLE ") {
        let name = target_str.trim()[6..].trim();
        let name = unquote(name);
        validate_ident(&name, true, "table")?;
        return Ok((GrantTargetType::Table, Some(name)));
    }

    // Default: treat as table name
    let name = unquote(target_str.trim());
    validate_ident(&name, true, "table")?;
    Ok((GrantTargetType::Table, Some(name)))
}

/// Extract a quoted string from the beginning of text
fn extract_quoted_string(text: &str) -> Option<String> {
    let text = text.trim();

    // Single quotes
    if let Some(stripped) = text.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        return Some(stripped[..end].to_string());
    }

    // Double quotes
    if let Some(stripped) = text.strip_prefix('"') {
        let end = stripped.find('"')?;
        return Some(stripped[..end].to_string());
    }
    None
}

/// Remove quotes from a string
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn validate_ident(name: &str, allow_dot: bool, context: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(format!(
            "{} cannot be empty",
            context
        )));
    }
    let parts: Vec<&str> = if allow_dot {
        name.split('.').collect()
    } else {
        vec![name]
    };
    for part in parts {
        let mut chars = part.chars();
        let first = chars
            .next()
            .ok_or_else(|| EngineError::InvalidArgument(format!("{} cannot be empty", context)))?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(EngineError::InvalidArgument(format!(
                "{} has invalid start character",
                context
            )));
        }
        if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(EngineError::InvalidArgument(format!(
                "{} has invalid characters",
                context
            )));
        }
    }
    Ok(())
}

/// Conflict action for UPSERT operations
#[derive(Debug, Clone)]
pub enum OnConflictAction {
    /// DO NOTHING - ignore conflicts
    DoNothing,
    /// DO UPDATE SET ... - update conflicting rows
    DoUpdate {
        /// Column assignments for update: (column_name, new_value_expr)
        assignments: Vec<(String, String)>,
        /// Optional WHERE clause for conditional update
        where_clause: Option<String>,
    },
}

/// ON CONFLICT clause for UPSERT
#[derive(Debug, Clone)]
pub struct OnConflict {
    /// Conflict target columns (optional)
    pub columns: Option<Vec<String>>,
    /// Action to take on conflict
    pub action: OnConflictAction,
}

/// Parsed INSERT command
#[derive(Debug, Clone)]
pub struct InsertCommand {
    pub database: String,
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<SqlValue>>,
    /// ON CONFLICT clause for UPSERT operations
    pub on_conflict: Option<OnConflict>,
    /// RETURNING clause - columns to return after insert
    pub returning: Option<Vec<String>>,
}

/// A SQL value from INSERT/UPDATE statements
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

/// Table reference in FROM/USING/JOIN clauses
#[derive(Debug, Clone)]
pub struct JoinTable {
    pub database: String,
    pub table: String,
    pub alias: Option<String>,
}

/// UPDATE command parsed from SQL
#[derive(Debug, Clone)]
pub struct UpdateCommand {
    pub database: String,
    pub table: String,
    /// Alias for the target table (optional)
    pub alias: Option<String>,
    /// Column assignments: (column_name, new_value)
    pub assignments: Vec<(String, SqlValue)>,
    /// FROM clause tables for join-based updates (PostgreSQL style)
    pub from_clause: Option<Vec<JoinTable>>,
    /// WHERE clause filter (simplified for now)
    pub where_clause: Option<String>,
    /// RETURNING clause - columns to return after update
    pub returning: Option<Vec<String>>,
}

/// DELETE command parsed from SQL
#[derive(Debug, Clone)]
pub struct DeleteCommand {
    pub database: String,
    pub table: String,
    /// Alias for the target table (optional)
    pub alias: Option<String>,
    /// USING clause tables for join-based deletes (PostgreSQL style)
    pub using_clause: Option<Vec<JoinTable>>,
    /// WHERE clause filter (simplified for now)
    pub where_clause: Option<String>,
    /// RETURNING clause - columns to return after delete
    pub returning: Option<Vec<String>>,
}

/// MERGE command action for WHEN MATCHED clause
#[derive(Debug, Clone)]
pub enum MergeWhenMatched {
    /// UPDATE SET col = val, ...
    Update {
        assignments: Vec<(String, SqlValue)>,
        condition: Option<String>,
    },
    /// DELETE
    Delete { condition: Option<String> },
}

/// MERGE command action for WHEN NOT MATCHED clause
#[derive(Debug, Clone)]
pub struct MergeWhenNotMatched {
    pub columns: Vec<String>,
    pub values: Vec<SqlValue>,
    pub condition: Option<String>,
}

/// MERGE command parsed from SQL (UPSERT with WHEN MATCHED/NOT MATCHED)
#[derive(Debug, Clone)]
pub struct MergeCommand {
    /// Target table database
    pub database: String,
    /// Target table name
    pub table: String,
    /// Source table or subquery (for now, just table name)
    pub source_database: String,
    pub source_table: String,
    /// Join condition (ON clause)
    pub on_condition: String,
    /// Actions for WHEN MATCHED
    pub when_matched: Vec<MergeWhenMatched>,
    /// Actions for WHEN NOT MATCHED
    pub when_not_matched: Vec<MergeWhenNotMatched>,
}

/// Foreign key referential action
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForeignKeyAction {
    /// No action - error if referenced row exists
    NoAction,
    /// Restrict - same as NoAction for now
    Restrict,
    /// Cascade - delete/update referencing rows
    Cascade,
    /// Set NULL - set referencing columns to NULL
    SetNull,
    /// Set DEFAULT - set referencing columns to their default values
    SetDefault,
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        ForeignKeyAction::NoAction
    }
}

impl std::str::FromStr for ForeignKeyAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().replace(' ', "_").as_str() {
            "NO_ACTION" | "NOACTION" => Ok(ForeignKeyAction::NoAction),
            "RESTRICT" => Ok(ForeignKeyAction::Restrict),
            "CASCADE" => Ok(ForeignKeyAction::Cascade),
            "SET_NULL" | "SETNULL" => Ok(ForeignKeyAction::SetNull),
            "SET_DEFAULT" | "SETDEFAULT" => Ok(ForeignKeyAction::SetDefault),
            _ => Err(format!("Unknown foreign key action: {}", s)),
        }
    }
}

/// Table constraint types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableConstraint {
    /// PRIMARY KEY constraint
    PrimaryKey {
        columns: Vec<String>,
        name: Option<String>,
    },
    /// UNIQUE constraint
    Unique {
        columns: Vec<String>,
        name: Option<String>,
    },
    /// CHECK constraint
    Check { expr: String, name: Option<String> },
    /// NOT NULL constraint (column-level)
    NotNull { column: String },
    /// DEFAULT value constraint
    Default { column: String, value: String },
    /// FOREIGN KEY constraint for referential integrity
    ForeignKey {
        /// Columns in this table that reference another table
        columns: Vec<String>,
        /// Name of the referenced table
        referenced_table: String,
        /// Columns in the referenced table
        referenced_columns: Vec<String>,
        /// Optional constraint name
        name: Option<String>,
        /// Action to take when referenced row is deleted
        on_delete: ForeignKeyAction,
        /// Action to take when referenced row is updated
        on_update: ForeignKeyAction,
    },
}

/// Column definition for CREATE TABLE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
}

/// Sequence definition for auto-increment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDef {
    pub name: String,
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
}

/// Isolation level for transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlIsolationLevel {
    /// Read Committed - see committed data only
    ReadCommitted,
    /// Repeatable Read - consistent snapshot from start
    RepeatableRead,
    /// Serializable - full isolation
    Serializable,
}

impl std::str::FromStr for SqlIsolationLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().replace('-', " ").as_str() {
            "READ COMMITTED" | "READCOMMITTED" => Ok(SqlIsolationLevel::ReadCommitted),
            "REPEATABLE READ" | "REPEATABLEREAD" => Ok(SqlIsolationLevel::RepeatableRead),
            "SERIALIZABLE" => Ok(SqlIsolationLevel::Serializable),
            _ => Err(format!("Unknown isolation level: {}", s)),
        }
    }
}

/// Transaction control commands
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TransactionCommand {
    /// BEGIN/START TRANSACTION
    Start {
        /// Optional isolation level
        isolation_level: Option<SqlIsolationLevel>,
        /// Whether this is a read-only transaction
        read_only: bool,
    },
    /// COMMIT transaction
    Commit,
    /// ROLLBACK transaction (optionally to a savepoint)
    Rollback {
        /// Savepoint name for partial rollback
        savepoint: Option<String>,
    },
    /// Create a savepoint within a transaction
    Savepoint { name: String },
    /// Release (remove) a savepoint
    ReleaseSavepoint { name: String },
}

/// Prepared statement command
#[derive(Debug, Clone)]
pub enum PreparedStatementCommand {
    /// PREPARE name AS statement
    Prepare {
        name: String,
        /// The SQL statement to prepare (as string for now)
        statement: String,
        /// Parameter types if specified
        param_types: Vec<String>,
    },
    /// EXECUTE name [(params)]
    Execute {
        name: String,
        /// Parameter values
        parameters: Vec<SqlValue>,
    },
    /// DEALLOCATE [PREPARE] name | ALL
    Deallocate {
        name: Option<String>, // None means ALL
    },
}

/// PIVOT clause for transforming rows to columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PivotClause {
    /// Aggregate function to apply (e.g., SUM, COUNT, AVG)
    pub agg_function: AggKind,
    /// Column to aggregate
    pub value_column: String,
    /// Column whose values become new column names
    pub pivot_column: String,
    /// Specific values to pivot on (IN clause)
    pub pivot_values: Vec<String>,
    /// Aliases for the pivoted columns (optional)
    pub column_aliases: Option<Vec<String>>,
}

/// UNPIVOT clause for transforming columns to rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnpivotClause {
    /// Name for the new value column
    pub value_column: String,
    /// Name for the new name column (holds original column names)
    pub name_column: String,
    /// Columns to unpivot
    pub columns: Vec<String>,
    /// Include nulls in output (default: false)
    pub include_nulls: bool,
}

/// Parsed SQL statement - either a query, DDL, Insert, Update, Delete, auth, or set operation
#[derive(Debug, Clone)]
pub enum SqlStatement {
    Query(ParsedQuery),
    SetOperation(SetOperationQuery),
    Ddl(DdlCommand),
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Merge(MergeCommand),
    Auth(AuthCommand),
    Transaction(TransactionCommand),
    /// Prepared statement commands (PREPARE, EXECUTE, DEALLOCATE)
    PreparedStatement(PreparedStatementCommand),
    /// EXPLAIN [ANALYZE] <statement>
    Explain {
        analyze: bool,
        statement: Box<SqlStatement>,
    },
    /// Pub/Sub commands (LISTEN, UNLISTEN, NOTIFY)
    PubSub(PubSubCommand),
    /// PIVOT query - transform rows to columns
    Pivot {
        source: Box<SqlStatement>,
        pivot: PivotClause,
    },
    /// UNPIVOT query - transform columns to rows
    Unpivot {
        source: Box<SqlStatement>,
        unpivot: UnpivotClause,
    },
}

/// Pub/Sub commands for LISTEN/NOTIFY
#[derive(Debug, Clone)]
pub enum PubSubCommand {
    /// LISTEN channel_name
    Listen { channel: String },
    /// UNLISTEN channel_name | UNLISTEN *
    Unlisten { channel: String },
    /// NOTIFY channel_name [, 'payload']
    Notify {
        channel: String,
        payload: Option<String>,
    },
}

/// Convert SQL data types to boyodb schema types
fn sql_type_to_boyodb_type(sql_type: &sqlparser::ast::DataType) -> String {
    use sqlparser::ast::DataType;
    match sql_type {
        DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::SmallInt(_)
        | DataType::TinyInt(_) => "int64".to_string(),
        DataType::Float(_) | DataType::Real | DataType::Double => "float64".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Custom(name, _) => {
            let n = name.to_string().to_lowercase();
            match n.as_str() {
                "int64" | "bigint" | "int" | "integer" => "int64".to_string(),
                "uint64" | "u64" => "uint64".to_string(),
                "float64" | "double" => "float64".to_string(),
                "string" | "utf8" | "text" => "string".to_string(),
                "bool" | "boolean" => "bool".to_string(),
                _ => "string".to_string(),
            }
        }
        DataType::Varchar(_) | DataType::Char(_) | DataType::Text | DataType::String(_) => {
            "string".to_string()
        }
        DataType::Timestamp(_, _) | DataType::Datetime(_) => "timestamp".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time(_, _) => "time".to_string(),
        DataType::Binary(_) | DataType::Blob(_) | DataType::Bytes(_) => "binary".to_string(),
        other => other.to_string().to_lowercase(), // Default: bubble through string form
    }
}

fn parse_statement(stmt: &Statement) -> Result<SqlStatement, EngineError> {
    match stmt {
        Statement::Query(query) => {
            // Check if this is a set operation (UNION, INTERSECT, EXCEPT)
            if let SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } = query.body.as_ref()
            {
                let op_type = match (op, set_quantifier) {
                    (SetOperator::Union, SetQuantifier::All) => SetOpType::UnionAll,
                    (SetOperator::Union, _) => SetOpType::Union,
                    (SetOperator::Intersect, SetQuantifier::All) => SetOpType::IntersectAll,
                    (SetOperator::Intersect, _) => SetOpType::Intersect,
                    (SetOperator::Except, SetQuantifier::All) => SetOpType::ExceptAll,
                    (SetOperator::Except, _) => SetOpType::Except,
                };

                // Parse left and right sides
                let left_query = Query {
                    body: left.clone(),
                    with: None,
                    order_by: Vec::new(),
                    limit: None,
                    limit_by: Vec::new(),
                    offset: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                };
                let right_query = Query {
                    body: right.clone(),
                    with: None,
                    order_by: Vec::new(),
                    limit: None,
                    limit_by: Vec::new(),
                    offset: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                };

                let left_parsed = parse_query_or_setop(&left_query)?;
                let right_parsed = parse_query_or_setop(&right_query)?;

                Ok(SqlStatement::SetOperation(SetOperationQuery {
                    op: op_type,
                    left: Box::new(left_parsed),
                    right: Box::new(right_parsed),
                }))
            } else {
                let parsed = parse_query(query)?;
                Ok(SqlStatement::Query(parsed))
            }
        }
        Statement::Insert {
            table_name,
            columns,
            source,
            returning,
            on,
            ..
        } => parse_insert(table_name, columns, source, returning, on),
        Statement::StartTransaction { .. } => {
            Ok(SqlStatement::Transaction(TransactionCommand::Start {
                isolation_level: None,
                read_only: false,
            }))
        }
        Statement::Commit { .. } => Ok(SqlStatement::Transaction(TransactionCommand::Commit)),
        Statement::Rollback { savepoint, .. } => {
            Ok(SqlStatement::Transaction(TransactionCommand::Rollback {
                savepoint: savepoint.as_ref().map(|s| s.to_string()),
            }))
        }
        Statement::Savepoint { name } => {
            Ok(SqlStatement::Transaction(TransactionCommand::Savepoint {
                name: name.value.clone(),
            }))
        }
        Statement::ReleaseSavepoint { name } => Ok(SqlStatement::Transaction(
            TransactionCommand::ReleaseSavepoint {
                name: name.value.clone(),
            },
        )),
        Statement::Prepare {
            name,
            data_types,
            statement,
            ..
        } => {
            let param_types: Vec<String> = data_types
                .iter()
                .map(|dt| sql_type_to_boyodb_type(dt))
                .collect();
            Ok(SqlStatement::PreparedStatement(
                PreparedStatementCommand::Prepare {
                    name: name.value.clone(),
                    statement: statement.to_string(),
                    param_types,
                },
            ))
        }
        Statement::Execute { name, parameters } => {
            let mut params = Vec::new();
            for param in parameters {
                params.push(expr_to_sql_value(param)?);
            }
            Ok(SqlStatement::PreparedStatement(
                PreparedStatementCommand::Execute {
                    name: name.value.clone(),
                    parameters: params,
                },
            ))
        }
        Statement::Deallocate { name, .. } => {
            let deallocate_name = if name.value.to_uppercase() == "ALL" {
                None
            } else {
                Some(name.value.clone())
            };
            Ok(SqlStatement::PreparedStatement(
                PreparedStatementCommand::Deallocate {
                    name: deallocate_name,
                },
            ))
        }
        Statement::CreateDatabase { db_name, .. } => {
            Ok(SqlStatement::Ddl(DdlCommand::CreateDatabase {
                name: db_name.to_string(),
            }))
        }
        Statement::CreateTable { name, columns, .. } => {
            let full_name = name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            // Convert SQL column definitions to schema JSON
            let schema_json = if columns.is_empty() {
                None
            } else {
                let schema_fields: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|col| {
                        let data_type = sql_type_to_boyodb_type(&col.data_type);
                        let nullable = !col
                            .options
                            .iter()
                            .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                        serde_json::json!({
                            "name": col.name.value,
                            "type": data_type,
                            "nullable": nullable
                        })
                    })
                    .collect();
                Some(serde_json::to_string(&schema_fields).unwrap_or_default())
            };
            Ok(SqlStatement::Ddl(DdlCommand::CreateTable {
                database,
                table,
                schema_json,
            }))
        }
        Statement::CreateView {
            name,
            query,
            or_replace,
            ..
        } => {
            let full_name = name.to_string();
            let (database, view_name) = parse_table_name(&full_name)?;
            // Store the query SQL as a string
            let query_sql = query.to_string();
            Ok(SqlStatement::Ddl(DdlCommand::CreateView {
                database,
                name: view_name,
                query_sql,
                or_replace: *or_replace,
            }))
        }
        Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => {
            use sqlparser::ast::ObjectType;
            match object_type {
                ObjectType::Schema => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP DATABASE requires exactly one name".into(),
                        ));
                    }
                    let name = names[0].to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::DropDatabase {
                        name,
                        if_exists: *if_exists,
                    }))
                }
                ObjectType::Table => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP TABLE requires exactly one table name".into(),
                        ));
                    }
                    let full_name = names[0].to_string();
                    let (database, table) = parse_table_name(&full_name)?;
                    Ok(SqlStatement::Ddl(DdlCommand::DropTable {
                        database,
                        table,
                        if_exists: *if_exists,
                    }))
                }
                ObjectType::View => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP VIEW requires exactly one view name".into(),
                        ));
                    }
                    let full_name = names[0].to_string();
                    let (database, name) = parse_table_name(&full_name)?;
                    Ok(SqlStatement::Ddl(DdlCommand::DropView {
                        database,
                        name,
                        if_exists: *if_exists,
                    }))
                }
                _ => Err(EngineError::NotImplemented(format!(
                    "DROP {object_type:?} not supported"
                ))),
            }
        }
        Statement::Truncate { table_name, .. } => {
            // Handle TRUNCATE TABLE database.table_name
            let full_name = table_name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            Ok(SqlStatement::Ddl(DdlCommand::TruncateTable {
                database,
                table,
            }))
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            let full_name = name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            use sqlparser::ast::AlterTableOperation;
            let Some(operation) = operations.first() else {
                return Err(EngineError::InvalidArgument(
                    "ALTER TABLE requires at least one operation".into(),
                ));
            };
            match operation {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.to_string();
                    let data_type = sql_type_to_boyodb_type(&column_def.data_type);
                    let nullable = !column_def
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableAddColumn {
                        database,
                        table,
                        column: col_name,
                        data_type,
                        nullable,
                    }))
                }
                AlterTableOperation::DropColumn { column_name, .. } => {
                    let col_name = column_name.to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableDropColumn {
                        database,
                        table,
                        column: col_name,
                    }))
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let old_col = old_column_name.to_string();
                    let new_col = new_column_name.to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableRenameColumn {
                        database,
                        table,
                        old_column: old_col,
                        new_column: new_col,
                    }))
                }
                AlterTableOperation::RenameTable { table_name } => {
                    let new_table = table_name.to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableRename {
                        database,
                        old_table: table,
                        new_table,
                    }))
                }
                _ => Err(EngineError::NotImplemented(
                    "ALTER TABLE operation not supported".into(),
                )),
            }
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            ..
        } => parse_update(table, assignments, from, selection, returning),
        Statement::Delete {
            from,
            using,
            selection,
            returning,
            ..
        } => parse_delete(from, using, selection, returning),
        Statement::Merge {
            table,
            source,
            on,
            clauses,
            ..
        } => parse_merge(table, source, on, clauses),
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            if_not_exists,
            using,
            ..
        } => {
            let full_table = table_name.to_string();
            let (database, table) = parse_table_name(&full_table)?;
            let index_name = name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| format!("idx_{}_{}", table, columns.len()));
            let column_names: Vec<String> = columns.iter().map(|c| c.expr.to_string()).collect();
            let index_type = match using
                .as_ref()
                .map(|i| i.to_string().to_uppercase())
                .as_deref()
            {
                Some("BTREE") | None => IndexType::BTree,
                Some("HASH") => IndexType::Hash,
                Some("BLOOM") => IndexType::Bloom,
                Some("BITMAP") => IndexType::Bitmap,
                Some("FULLTEXT") => IndexType::Fulltext,
                Some(other) => {
                    return Err(EngineError::InvalidArgument(format!(
                        "unsupported index type: {other}"
                    )))
                }
            };
            Ok(SqlStatement::Ddl(DdlCommand::CreateIndex {
                database,
                table,
                index_name,
                columns: column_names,
                index_type,
                if_not_exists: *if_not_exists,
            }))
        }
        Statement::Explain {
            analyze, statement, ..
        } => {
            // Parse EXPLAIN [ANALYZE] SELECT ...
            let explained = parse_statement(statement)?;
            Ok(SqlStatement::Explain {
                analyze: *analyze,
                statement: Box::new(explained),
            })
        }
        Statement::Analyze { table_name, .. } => {
            let full_name = table_name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            Ok(SqlStatement::Ddl(DdlCommand::AnalyzeTable {
                database,
                table,
                columns: None,
            }))
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported SQL statement type: {stmt}"
        ))),
    }
}

/// Parse a query that might be a simple SELECT or a set operation
fn parse_query_or_setop(query: &Query) -> Result<SqlStatement, EngineError> {
    if let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = query.body.as_ref()
    {
        let op_type = match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => SetOpType::UnionAll,
            (SetOperator::Union, _) => SetOpType::Union,
            (SetOperator::Intersect, SetQuantifier::All) => SetOpType::IntersectAll,
            (SetOperator::Intersect, _) => SetOpType::Intersect,
            (SetOperator::Except, SetQuantifier::All) => SetOpType::ExceptAll,
            (SetOperator::Except, _) => SetOpType::Except,
        };

        let left_query = Query {
            body: left.clone(),
            with: None,
            order_by: Vec::new(),
            limit: None,
            limit_by: Vec::new(),
            offset: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
        };
        let right_query = Query {
            body: right.clone(),
            with: None,
            order_by: Vec::new(),
            limit: None,
            limit_by: Vec::new(),
            offset: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
        };

        let left_parsed = parse_query_or_setop(&left_query)?;
        let right_parsed = parse_query_or_setop(&right_query)?;

        Ok(SqlStatement::SetOperation(SetOperationQuery {
            op: op_type,
            left: Box::new(left_parsed),
            right: Box::new(right_parsed),
        }))
    } else {
        parse_query(query).map(SqlStatement::Query)
    }
}

fn parse_query(query: &Query) -> Result<ParsedQuery, EngineError> {
    // Extract the SELECT body
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => {
            return Err(EngineError::NotImplemented(
                "only SELECT queries are supported".into(),
            ))
        }
    };

    // Parse table reference and JOINs
    let (database, table, joins) = parse_from_clause(select)?;

    // Parse projection and detect aggregations
    // Parse projection and detect aggregations
    let (projection, aggregation, computed_columns) =
        parse_select_items(&select.selection, &select.projection)?;

    // Parse WHERE clause
    let mut filter = QueryFilter::default();
    if let Some(where_expr) = &select.selection {
        parse_where_expr(where_expr, &mut filter)?;
    }

    // Parse LIMIT
    if let Some(Expr::Value(Value::Number(n, _))) = &query.limit {
        filter.limit = n.parse().ok();
    }

    // Parse FETCH FIRST n ROWS ONLY (ANSI SQL standard)
    if let Some(fetch) = &query.fetch {
        // Only set limit if not already set by LIMIT clause
        if filter.limit.is_none() {
            if let Some(Expr::Value(Value::Number(n, _))) = &fetch.quantity {
                filter.limit = n.parse().ok();
            }
        }
    }

    // Parse OFFSET
    if let Some(offset_expr) = &query.offset {
        if let Expr::Value(Value::Number(n, _)) = &offset_expr.value {
            filter.offset = n.parse().ok();
        }
    }

    // Parse ORDER BY
    let order_by = parse_order_by(&query.order_by)?;

    // Parse GROUP BY
    let group_by = parse_group_by(&select.group_by)?;

    // Parse HAVING clause
    let having_conditions = if let Some(having_expr) = &select.having {
        parse_having_clause(having_expr)?
    } else {
        Vec::new()
    };

    // If we have aggregation, set the group by and having
    let aggregation = aggregation.map(|mut agg| {
        agg.group_by = group_by;
        agg.having = having_conditions;
        agg
    });

    // Parse DISTINCT and DISTINCT ON
    let (distinct, distinct_on) = match &select.distinct {
        Some(Distinct::Distinct) => (true, None),
        Some(Distinct::On(exprs)) => {
            // Extract column names from DISTINCT ON expressions
            let columns: Result<Vec<String>, EngineError> = exprs
                .iter()
                .map(|expr| match expr {
                    Expr::Identifier(ident) => Ok(ident.value.clone()),
                    Expr::CompoundIdentifier(parts) => Ok(parts
                        .iter()
                        .map(|p| p.value.clone())
                        .collect::<Vec<_>>()
                        .join(".")),
                    _ => Err(EngineError::InvalidArgument(format!(
                        "DISTINCT ON only supports column references, got: {:?}",
                        expr
                    ))),
                })
                .collect();
            (false, Some(columns?))
        }
        None => (false, None),
    };

    // Parse CTEs (WITH clause)
    let ctes = if let Some(with) = &query.with {
        parse_ctes(with)?
    } else {
        Vec::new()
    };

    Ok(ParsedQuery {
        database,
        table,
        projection,
        filter,
        aggregation,
        order_by,
        distinct,
        distinct_on,
        joins,
        computed_columns,
        ctes,
        sample: None,
    })
}

fn parse_order_by(order_by: &[OrderByExpr]) -> Result<Option<Vec<OrderByClause>>, EngineError> {
    if order_by.is_empty() {
        return Ok(None);
    }

    let mut clauses = Vec::new();
    for expr in order_by {
        let column = match &expr.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => {
                return Err(EngineError::NotImplemented(
                    "only simple column references supported in ORDER BY".into(),
                ))
            }
        };
        clauses.push(OrderByClause {
            column,
            ascending: expr.asc.unwrap_or(true),
            nulls_first: expr.nulls_first,
        });
    }
    Ok(Some(clauses))
}

fn parse_table_name(full_name: &str) -> Result<(String, String), EngineError> {
    let cleaned = full_name.trim();
    if let Some((db, tbl)) = cleaned.split_once('.') {
        let db = db.trim().trim_matches('"').trim_matches('`');
        let tbl = tbl.trim().trim_matches('"').trim_matches('`');
        if db.is_empty() {
            return Ok(("default".to_string(), tbl.to_string()));
        }
        Ok((db.to_string(), tbl.to_string()))
    } else {
        let tbl = cleaned.trim_matches('"').trim_matches('`');
        Ok(("default".to_string(), tbl.to_string()))
    }
}

fn parse_from_clause(
    select: &Select,
) -> Result<(Option<String>, Option<String>, Vec<JoinClause>), EngineError> {
    if select.from.is_empty() {
        return Ok((None, None, Vec::new()));
    }

    let table_with_joins = &select.from[0];
    let (db, table) = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => {
            let full_name = name.to_string();
            parse_table_name(&full_name)?
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "only simple table references supported".into(),
            ))
        }
    };

    // Parse JOIN clauses
    let joins = parse_joins(&table_with_joins.joins)?;

    Ok((Some(db), Some(table), joins))
}

fn parse_joins(joins: &[Join]) -> Result<Vec<JoinClause>, EngineError> {
    let mut result = Vec::new();

    for join in joins {
        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::FullOuter,
            JoinOperator::CrossJoin => JoinType::Cross,
            _ => return Err(EngineError::NotImplemented("unsupported JOIN type".into())),
        };

        // Extract the joined table name and alias
        let (join_db, join_table, alias) = match &join.relation {
            TableFactor::Table { name, alias, .. } => {
                let full_name = name.to_string();
                let (db, tbl) = parse_table_name(&full_name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                (db, tbl, alias_name)
            }
            _ => {
                return Err(EngineError::NotImplemented(
                    "only simple table references in JOINs are supported".into(),
                ))
            }
        };

        // Extract the ON condition (CROSS JOIN has no ON condition)
        let on_condition = match &join.join_operator {
            JoinOperator::CrossJoin => {
                // Cross join has no ON condition - use empty placeholder
                JoinCondition {
                    left_column: String::new(),
                    right_column: String::new(),
                    operator: JoinComparisonOp::Equal,
                }
            }
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint) => match constraint {
                JoinConstraint::On(expr) => parse_join_condition(expr)?,
                _ => {
                    return Err(EngineError::NotImplemented(
                        "only ON conditions are supported in JOINs".into(),
                    ))
                }
            },
            _ => {
                return Err(EngineError::NotImplemented(
                    "unsupported join operator".into(),
                ))
            }
        };

        result.push(JoinClause {
            join_type,
            table: join_table,
            database: join_db,
            alias,
            on_condition,
        });
    }

    Ok(result)
}

fn parse_join_condition(expr: &Expr) -> Result<JoinCondition, EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op != BinaryOperator::Eq {
                return Err(EngineError::NotImplemented(
                    "only equality conditions (=) are supported in JOINs".into(),
                ));
            }

            let left_col = extract_column_name(left)?;
            let right_col = extract_column_name(right)?;

            Ok(JoinCondition {
                left_column: left_col,
                right_column: right_col,
                operator: JoinComparisonOp::Equal,
            })
        }
        _ => Err(EngineError::NotImplemented(
            "only simple equality conditions are supported in JOINs".into(),
        )),
    }
}

fn extract_column_name(expr: &Expr) -> Result<String, EngineError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // Handle table.column format - return just the column part
            if let Some(last) = parts.last() {
                Ok(last.value.clone())
            } else {
                Err(EngineError::InvalidArgument(
                    "empty compound identifier".into(),
                ))
            }
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression in JOIN condition: {expr}"
        ))),
    }
}

fn parse_select_items(
    _where_clause: &Option<Expr>,
    items: &[SelectItem],
) -> Result<(Option<Vec<String>>, Option<AggPlan>, Vec<SelectColumn>), EngineError> {
    let mut columns = Vec::new();
    let mut aggs = Vec::new();
    let mut computed_columns = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                return Ok((None, None, Vec::new())); // SELECT * - no projection, no aggregation
            }
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    columns.push(ident.value.clone());
                    computed_columns.push(SelectColumn {
                        expr: SelectExpr::Column(ident.value.clone()),
                        alias: None,
                    });
                }
                Expr::Value(val) => {
                    let se = match val {
                        Value::Number(n, _) => {
                            SelectExpr::Literal(LiteralValue::Integer(n.parse().unwrap_or(0)))
                        }
                        Value::SingleQuotedString(s) => {
                            SelectExpr::Literal(LiteralValue::String(s.clone()))
                        }
                        _ => {
                            return Err(EngineError::NotImplemented(format!(
                                "unsupported value type: {val}"
                            )))
                        }
                    };
                    computed_columns.push(SelectColumn {
                        expr: se,
                        alias: None,
                    });
                }
                Expr::Function(func) => {
                    if let Some((agg_kind, filter)) = parse_aggregate_function(func)? {
                        let mut agg_expr = AggregateExpr::new(agg_kind);
                        agg_expr.filter = filter;
                        aggs.push(agg_expr);
                    } else {
                        // Not an aggregate, try parsing as scalar scalar function
                        extract_ast_columns(expr, &mut columns);
                        let se = parse_expr(expr)?;
                        computed_columns.push(SelectColumn {
                            expr: se,
                            alias: None,
                        });
                    }
                }
                _ => {
                    extract_ast_columns(expr, &mut columns);
                    let se = parse_expr(expr)?;
                    computed_columns.push(SelectColumn {
                        expr: se,
                        alias: None,
                    });
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => match expr {
                Expr::Identifier(ident) => {
                    columns.push(ident.value.clone());
                    computed_columns.push(SelectColumn {
                        expr: SelectExpr::Column(ident.value.clone()),
                        alias: Some(alias.value.clone()),
                    });
                }
                Expr::Value(val) => {
                    let se = match val {
                        Value::Number(n, _) => {
                            SelectExpr::Literal(LiteralValue::Integer(n.parse().unwrap_or(0)))
                        }
                        Value::SingleQuotedString(s) => {
                            SelectExpr::Literal(LiteralValue::String(s.clone()))
                        }
                        _ => {
                            return Err(EngineError::NotImplemented(format!(
                                "unsupported value type: {val}"
                            )))
                        }
                    };
                    computed_columns.push(SelectColumn {
                        expr: se,
                        alias: Some(alias.value.clone()),
                    });
                }
                Expr::Function(func) => {
                    if let Some((agg_kind, filter)) = parse_aggregate_function(func)? {
                        let mut agg_expr = AggregateExpr::with_alias(agg_kind, alias.value.clone());
                        agg_expr.filter = filter;
                        aggs.push(agg_expr);
                    } else {
                        extract_ast_columns(expr, &mut columns);
                        let se = parse_expr(expr)?;
                        computed_columns.push(SelectColumn {
                            expr: se,
                            alias: Some(alias.value.clone()),
                        });
                    }
                }
                _ => {
                    extract_ast_columns(expr, &mut columns);
                    let se = parse_expr(expr)?;
                    computed_columns.push(SelectColumn {
                        expr: se,
                        alias: Some(alias.value.clone()),
                    });
                }
            },
            _ => {
                return Err(EngineError::NotImplemented(format!(
                    "unsupported SELECT item: {item}"
                )));
            }
        }
    }

    if !aggs.is_empty() {
        // If we have columns but also aggs, they must be part of GROUP BY or valid
        // For now we return both
        Ok((
            if columns.is_empty() {
                None
            } else {
                Some(columns)
            },
            Some(AggPlan {
                group_by: GroupBy::None,
                aggs,
                having: Vec::new(),
            }),
            computed_columns,
        ))
    } else {
        Ok((
            if columns.is_empty() {
                None
            } else {
                Some(columns)
            },
            None,
            computed_columns,
        ))
    }
}

/// Recursively extract physical columns needed for an AST expression
fn extract_ast_columns(expr: &Expr, cols: &mut Vec<String>) {
    match expr {
        Expr::Identifier(ident) => {
            if !cols.contains(&ident.value) {
                cols.push(ident.value.clone());
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                if !cols.contains(&last.value) {
                    cols.push(last.value.clone());
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_ast_columns(left, cols);
            extract_ast_columns(right, cols);
        }
        Expr::UnaryOp { expr: inner, .. } => extract_ast_columns(inner, cols),
        Expr::Nested(inner) => extract_ast_columns(inner, cols),
        Expr::Function(func) => {
            for arg in &func.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = arg {
                    extract_ast_columns(inner, cols);
                }
            }
        }
        Expr::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand { extract_ast_columns(op, cols); }
            for cond in conditions { extract_ast_columns(cond, cols); }
            for res in results { extract_ast_columns(res, cols); }
            if let Some(er) = else_result { extract_ast_columns(er, cols); }
        }
        Expr::Cast { expr: inner, .. } => extract_ast_columns(inner, cols),
        Expr::InList { expr: inner, list, .. } => {
            extract_ast_columns(inner, cols);
            for e in list { extract_ast_columns(e, cols); }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => extract_ast_columns(inner, cols),
        Expr::Like { expr: inner, pattern, .. } => {
            extract_ast_columns(inner, cols);
            extract_ast_columns(pattern, cols);
        }
        Expr::Between { expr: inner, low, high, .. } => {
            extract_ast_columns(inner, cols);
            extract_ast_columns(low, cols);
            extract_ast_columns(high, cols);
        }
        _ => {}
    }
}

/// Parse a FILTER clause expression into AggregateFilter
fn parse_aggregate_filter_expr(expr: &Expr) -> Result<AggregateFilter, EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Extract column name from left side
            let column = match left.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(EngineError::NotImplemented(
                        "FILTER clause left side must be a column reference".into(),
                    ))
                }
            };

            // Parse operator
            let filter_op = match op {
                BinaryOperator::Eq => FilterOp::Eq,
                BinaryOperator::NotEq => FilterOp::Ne,
                BinaryOperator::Gt => FilterOp::Gt,
                BinaryOperator::GtEq => FilterOp::Ge,
                BinaryOperator::Lt => FilterOp::Lt,
                BinaryOperator::LtEq => FilterOp::Le,
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported operator in FILTER clause: {:?}",
                        op
                    )))
                }
            };

            // Parse value from right side
            let value = parse_filter_value(right)?;

            Ok(AggregateFilter {
                column,
                op: filter_op,
                value,
            })
        }
        Expr::IsNull(inner) => {
            let column = match inner.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(EngineError::NotImplemented(
                        "IS NULL must reference a column".into(),
                    ))
                }
            };
            Ok(AggregateFilter {
                column,
                op: FilterOp::IsNull,
                value: FilterValue::Null,
            })
        }
        Expr::IsNotNull(inner) => {
            let column = match inner.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(EngineError::NotImplemented(
                        "IS NOT NULL must reference a column".into(),
                    ))
                }
            };
            Ok(AggregateFilter {
                column,
                op: FilterOp::IsNotNull,
                value: FilterValue::Null,
            })
        }
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            if *negated {
                return Err(EngineError::NotImplemented(
                    "NOT LIKE not supported in FILTER clause".into(),
                ));
            }
            let column = match inner.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(EngineError::NotImplemented(
                        "LIKE must reference a column".into(),
                    ))
                }
            };
            let value = parse_filter_value(pattern)?;
            Ok(AggregateFilter {
                column,
                op: FilterOp::Like,
                value,
            })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            if *negated {
                return Err(EngineError::NotImplemented(
                    "NOT IN not supported in FILTER clause".into(),
                ));
            }
            let column = match inner.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join("."),
                _ => {
                    return Err(EngineError::NotImplemented(
                        "IN clause must reference a column".into(),
                    ))
                }
            };
            let values: Result<Vec<FilterValue>, _> =
                list.iter().map(|e| parse_filter_value(e)).collect();
            Ok(AggregateFilter {
                column,
                op: FilterOp::In,
                value: FilterValue::List(values?),
            })
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported FILTER clause expression: {:?}",
            expr
        ))),
    }
}

/// Parse a value expression into FilterValue
fn parse_filter_value(expr: &Expr) -> Result<FilterValue, EngineError> {
    match expr {
        Expr::Value(Value::Number(n, _)) => {
            if n.contains('.') {
                n.parse::<f64>()
                    .map(FilterValue::Float)
                    .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {}", n)))
            } else {
                n.parse::<i64>()
                    .map(FilterValue::Int)
                    .map_err(|_| EngineError::InvalidArgument(format!("invalid integer: {}", n)))
            }
        }
        Expr::Value(Value::SingleQuotedString(s)) => Ok(FilterValue::String(s.clone())),
        Expr::Value(Value::DoubleQuotedString(s)) => Ok(FilterValue::String(s.clone())),
        Expr::Value(Value::Boolean(b)) => Ok(FilterValue::Bool(*b)),
        Expr::Value(Value::Null) => Ok(FilterValue::Null),
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported value in FILTER clause: {:?}",
            expr
        ))),
    }
}

/// Parse an aggregate function from SQL AST
/// Returns the aggregate kind and optional filter clause
fn parse_aggregate_function(
    func: &sqlparser::ast::Function,
) -> Result<Option<(AggKind, Option<AggregateFilter>)>, EngineError> {
    let func_name = func.name.to_string().to_lowercase();

    // Parse the optional FILTER clause
    let filter = if let Some(filter_expr) = &func.filter {
        Some(parse_aggregate_filter_expr(filter_expr)?)
    } else {
        None
    };

    let agg_kind = match func_name.as_str() {
        "count" => {
            // Check for COUNT(DISTINCT column)
            if func.distinct {
                let col = extract_function_column(func)?;
                Some(AggKind::CountDistinct { column: col })
            } else {
                // Check if it's COUNT(*) or COUNT(column)
                if func.args.is_empty() {
                    Some(AggKind::CountStar)
                } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) =
                    func.args.first()
                {
                    Some(AggKind::CountStar)
                } else {
                    // COUNT(column) - treat as COUNT(*)
                    Some(AggKind::CountStar)
                }
            }
        }
        "approx_count_distinct" => {
            let col = extract_function_column(func)?;
            Some(AggKind::ApproxCountDistinct { column: col })
        }
        "sum" => {
            let col = extract_function_column(func)?;
            Some(AggKind::Sum { column: col })
        }
        "avg" => {
            let col = extract_function_column(func)?;
            Some(AggKind::Avg { column: col })
        }
        "min" => {
            let col = extract_function_column(func)?;
            Some(AggKind::Min { column: col })
        }
        "max" => {
            let col = extract_function_column(func)?;
            Some(AggKind::Max { column: col })
        }
        "stddev" | "stddev_samp" => {
            let col = extract_function_column(func)?;
            Some(AggKind::StddevSamp { column: col })
        }
        "stddev_pop" => {
            let col = extract_function_column(func)?;
            Some(AggKind::StddevPop { column: col })
        }
        "variance" | "var_samp" => {
            let col = extract_function_column(func)?;
            Some(AggKind::VarianceSamp { column: col })
        }
        "var_pop" => {
            let col = extract_function_column(func)?;
            Some(AggKind::VariancePop { column: col })
        }
        "median" => {
            let col = extract_function_column(func)?;
            Some(AggKind::Median { column: col })
        }
        "percentile_cont" => {
            // PERCENTILE_CONT(percentile) WITHIN GROUP (ORDER BY column)
            // For now, support simplified form: PERCENTILE_CONT(column, percentile)
            if func.args.len() < 2 {
                return Err(EngineError::InvalidArgument(
                    "PERCENTILE_CONT requires column and percentile arguments".into(),
                ));
            }
            let col = extract_function_column(func)?;
            let percentile = extract_function_percentile(func, 1)?;
            Some(AggKind::PercentileCont {
                column: col,
                percentile,
            })
        }
        "percentile_disc" => {
            if func.args.len() < 2 {
                return Err(EngineError::InvalidArgument(
                    "PERCENTILE_DISC requires column and percentile arguments".into(),
                ));
            }
            let col = extract_function_column(func)?;
            let percentile = extract_function_percentile(func, 1)?;
            Some(AggKind::PercentileDisc {
                column: col,
                percentile,
            })
        }
        "array_agg" => {
            let col = extract_function_column(func)?;
            Some(AggKind::ArrayAgg {
                column: col,
                distinct: func.distinct,
            })
        }
        "string_agg" | "group_concat" | "listagg" => {
            let col = extract_function_column(func)?;
            // Extract delimiter if provided (default to comma)
            let delimiter = if func.args.len() >= 2 {
                extract_function_string_arg(func, 1).unwrap_or_else(|_| ",".to_string())
            } else {
                ",".to_string()
            };
            // Check for optional order_by column (3rd argument)
            if func.args.len() >= 3 {
                let order_by = extract_function_string_arg(func, 2).unwrap_or_else(|_| col.clone());
                let order_desc = if func.args.len() >= 4 {
                    matches!(
                        extract_function_string_arg(func, 3).as_deref(),
                        Ok("DESC" | "desc")
                    )
                } else {
                    false
                };
                Some(AggKind::StringAggOrdered {
                    column: col,
                    delimiter,
                    order_by,
                    order_desc,
                })
            } else {
                Some(AggKind::StringAgg {
                    column: col,
                    delimiter,
                    distinct: func.distinct,
                })
            }
        }
        "mode" => {
            // MODE() - most frequent value
            let col = extract_function_column(func)?;
            Some(AggKind::Mode { column: col })
        }
        "first_value" | "first" => {
            let col = extract_function_column(func)?;
            Some(AggKind::FirstValue { column: col })
        }
        "last_value" | "last" => {
            let col = extract_function_column(func)?;
            Some(AggKind::LastValue { column: col })
        }
        "nth_value" => {
            // NTH_VALUE(column, n)
            if func.args.len() < 2 {
                return Err(EngineError::InvalidArgument(
                    "NTH_VALUE requires column and n arguments".into(),
                ));
            }
            let col = extract_function_column(func)?;
            let n = extract_function_percentile(func, 1)? as usize;
            Some(AggKind::NthValue { column: col, n })
        }
        "array_agg_ordered" => {
            // ARRAY_AGG with ordering: ARRAY_AGG_ORDERED(column, order_column, [DESC])
            if func.args.len() < 2 {
                return Err(EngineError::InvalidArgument(
                    "ARRAY_AGG_ORDERED requires column and order_by arguments".into(),
                ));
            }
            let col = extract_function_column(func)?;
            let order_by = extract_function_string_arg(func, 1)?;
            let order_desc = if func.args.len() >= 3 {
                matches!(
                    extract_function_string_arg(func, 2).as_deref(),
                    Ok("DESC" | "desc")
                )
            } else {
                false
            };
            Some(AggKind::ArrayAggOrdered {
                column: col,
                order_by,
                order_desc,
            })
        }
        _ => None,
    };

    // Return the aggregate kind with its optional filter
    Ok(agg_kind.map(|kind| (kind, filter)))
}

fn extract_function_column(func: &sqlparser::ast::Function) -> Result<String, EngineError> {
    // In sqlparser 0.40, args is Vec<FunctionArg>
    if func.args.is_empty() {
        return Err(EngineError::InvalidArgument(
            "function requires column argument".into(),
        ));
    }

    match &func.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Ok(ident.value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok("*".to_string()),
        _ => Err(EngineError::NotImplemented(
            "only simple column references supported in aggregate functions".into(),
        )),
    }
}

/// Extract a percentile value (0.0 to 1.0) from function argument at given index
fn extract_function_percentile(
    func: &sqlparser::ast::Function,
    arg_index: usize,
) -> Result<f64, EngineError> {
    if func.args.len() <= arg_index {
        return Err(EngineError::InvalidArgument(
            "percentile argument required".into(),
        ));
    }

    match &func.args[arg_index] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::Number(n, _)))) => {
            let percentile: f64 = n.parse().map_err(|_| {
                EngineError::InvalidArgument(format!("invalid percentile value: {}", n))
            })?;
            if !(0.0..=1.0).contains(&percentile) {
                return Err(EngineError::InvalidArgument(
                    "percentile must be between 0.0 and 1.0".into(),
                ));
            }
            Ok(percentile)
        }
        _ => Err(EngineError::InvalidArgument(
            "percentile must be a numeric literal between 0.0 and 1.0".into(),
        )),
    }
}

/// Extract a string argument from function at given index
fn extract_function_string_arg(
    func: &sqlparser::ast::Function,
    arg_index: usize,
) -> Result<String, EngineError> {
    if func.args.len() <= arg_index {
        return Err(EngineError::InvalidArgument(
            "string argument required".into(),
        ));
    }

    match &func.args[arg_index] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(s)))) => {
            Ok(s.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::DoubleQuotedString(s)))) => {
            Ok(s.clone())
        }
        _ => Err(EngineError::InvalidArgument(
            "expected string literal argument".into(),
        )),
    }
}

fn parse_where_expr(expr: &Expr, filter: &mut QueryFilter) -> Result<(), EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    parse_where_expr(left, filter)?;
                    parse_where_expr(right, filter)?;
                }
                BinaryOperator::Or => {
                    // Handle OR by collecting equality conditions into IN filters
                    // For example: tenant_id = 1 OR tenant_id = 3 becomes tenant_id IN (1, 3)
                    let mut or_values: std::collections::HashMap<String, Vec<u64>> =
                        std::collections::HashMap::new();
                    collect_or_equalities(expr, &mut or_values);

                    // Convert collected OR equalities into IN filters
                    for (col_name, values) in or_values {
                        let col_lower = col_name.to_lowercase();
                        if col_lower == "tenant_id" {
                            match &mut filter.tenant_id_in {
                                Some(existing) => existing.extend(values),
                                None => filter.tenant_id_in = Some(values),
                            }
                        } else if col_lower == "route_id" {
                            match &mut filter.route_id_in {
                                Some(existing) => existing.extend(values),
                                None => filter.route_id_in = Some(values),
                            }
                        } else {
                            // For other columns, add to numeric_in_filters
                            let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
                            filter.numeric_in_filters.push((col_name, i64_values));
                        }
                    }
                }
                BinaryOperator::Eq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            let col_lower = col_name.to_lowercase();
                            // Handle special columns for backward compatibility
                            if col_lower == "tenant_id" || col_lower == "route_id" {
                                if let Ok(value) = extract_u64_value(right) {
                                    match col_lower.as_str() {
                                        "tenant_id" => filter.tenant_id_eq = Some(value),
                                        "route_id" => filter.route_id_eq = Some(value),
                                        _ => {}
                                    }
                                }
                            }
                            // Try numeric value for segment pruning (all columns)
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Eq,
                                    value: num_val,
                                });
                                // Also handle legacy special columns
                                if let Ok(value) = extract_u64_value(right) {
                                    match col_lower.as_str() {
                                        "watermark" | "watermark_micros" => {
                                            filter.watermark_ge = Some(value);
                                            filter.watermark_le = Some(value);
                                        }
                                        "event_time" => {
                                            filter.event_time_ge = Some(value);
                                            filter.event_time_le = Some(value);
                                        }
                                        _ => {}
                                    }
                                }
                            } else if let Some(string_val) = extract_string_value(right) {
                                // String equality for other columns
                                filter
                                    .string_eq_filters
                                    .push((col_name.clone(), string_val));
                            }
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                ">=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Ge,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_ge = Some(value)
                                    }
                                    "event_time" => filter.event_time_ge = Some(value),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "<=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Le,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_le = Some(value)
                                    }
                                    "event_time" => filter.event_time_le = Some(value),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::Gt => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                ">".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Gt,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_ge = Some(value.saturating_add(1))
                                    }
                                    "event_time" => {
                                        filter.event_time_ge = Some(value.saturating_add(1))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::Lt => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "<".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Lt,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_le = Some(value.saturating_sub(1))
                                    }
                                    "event_time" => {
                                        filter.event_time_le = Some(value.saturating_sub(1))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Handle scalar subquery comparison
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "!=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Ne,
                                    value: num_val,
                                });
                            }
                        }
                    }
                }
                _ => {} // Ignore other operators for now
            }
        }
        // LIKE expression: column LIKE 'pattern'
        Expr::Like {
            negated,
            expr: like_expr,
            pattern,
            ..
        } => {
            let col_name = match like_expr.as_ref() {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
                _ => None,
            };
            if let (Some(col), Some(pattern_str)) = (col_name, extract_string_value(pattern)) {
                filter.like_filters.push((col, pattern_str, *negated));
            }
        }
        // ILike expression: column ILIKE 'pattern' (case-insensitive)
        Expr::ILike {
            negated,
            expr: like_expr,
            pattern,
            ..
        } => {
            let col_name = match like_expr.as_ref() {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
                _ => None,
            };
            if let (Some(col), Some(pattern_str)) = (col_name, extract_string_value(pattern)) {
                // For ILIKE, we convert pattern to lowercase for case-insensitive match
                filter
                    .ilike_filters
                    .push((col, pattern_str.to_lowercase(), *negated));
            }
        }
        // IN expression: column IN ('a', 'b', 'c') or column IN (1, 2, 3)
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => {
            if !negated {
                if let Expr::Identifier(ident) = in_expr.as_ref() {
                    let col_name = ident.value.to_lowercase();

                    // Try to extract as numeric list first
                    let numeric_values: Vec<i64> = list
                        .iter()
                        .filter_map(|e| match e {
                            Expr::Value(Value::Number(n, _)) => n.parse::<i64>().ok(),
                            Expr::UnaryOp {
                                op: UnaryOperator::Minus,
                                expr,
                            } => {
                                if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                                    n.parse::<i64>().ok().map(|v| -v)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        })
                        .collect();

                    if numeric_values.len() == list.len() && !numeric_values.is_empty() {
                        // Handle special columns tenant_id and route_id
                        if col_name == "tenant_id" {
                            filter.tenant_id_in =
                                Some(numeric_values.iter().map(|&v| v as u64).collect());
                        } else if col_name == "route_id" {
                            filter.route_id_in =
                                Some(numeric_values.iter().map(|&v| v as u64).collect());
                        } else {
                            filter
                                .numeric_in_filters
                                .push((ident.value.clone(), numeric_values));
                        }
                    } else {
                        // Try to extract as string list
                        let string_values: Vec<String> = list
                            .iter()
                            .filter_map(|e| extract_string_value(e))
                            .collect();
                        if string_values.len() == list.len() && !string_values.is_empty() {
                            filter
                                .string_in_filters
                                .push((ident.value.clone(), string_values));
                        }
                    }
                }
            }
        }
        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            if let Expr::Identifier(ident) = inner.as_ref() {
                filter.null_filters.push((ident.value.clone(), true));
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Identifier(ident) = inner.as_ref() {
                filter.null_filters.push((ident.value.clone(), false));
            }
        }
        // BETWEEN expression: column BETWEEN low AND high
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => {
            if !negated {
                if let Some(col_name) = try_extract_column_name(between_expr) {
                    // Add >= low filter
                    if let Some(low_val) = try_extract_numeric_value(low) {
                        filter.numeric_range_filters.push(NumericFilter {
                            column: col_name.clone(),
                            op: NumericOp::Ge,
                            value: low_val,
                        });
                    }
                    // Add <= high filter
                    if let Some(high_val) = try_extract_numeric_value(high) {
                        filter.numeric_range_filters.push(NumericFilter {
                            column: col_name.clone(),
                            op: NumericOp::Le,
                            value: high_val,
                        });
                    }
                    // Legacy handling for special columns
                    if let (Ok(low_u64), Ok(high_u64)) =
                        (extract_u64_value(low), extract_u64_value(high))
                    {
                        match col_name.to_lowercase().as_str() {
                            "event_time" => {
                                filter.event_time_ge = Some(low_u64);
                                filter.event_time_le = Some(high_u64);
                            }
                            "watermark" | "watermark_micros" => {
                                filter.watermark_ge = Some(low_u64);
                                filter.watermark_le = Some(high_u64);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Expr::Nested(inner) => {
            parse_where_expr(inner, filter)?;
        }
        // IN subquery: column IN (SELECT ...)
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => {
            if let Expr::Identifier(ident) = in_expr.as_ref() {
                // Convert the subquery back to SQL string for execution
                let subquery_sql = subquery.to_string();
                filter
                    .in_subquery_filters
                    .push((ident.value.clone(), subquery_sql, *negated));
            }
        }
        // EXISTS (SELECT ...)
        Expr::Exists { subquery, negated } => {
            let subquery_sql = subquery.to_string();
            filter.exists_subqueries.push((subquery_sql, *negated));
        }
        // Scalar subquery comparison: column = (SELECT ...), column > (SELECT ...), etc.
        Expr::Subquery(subquery) => {
            // Standalone subquery - handled when part of a comparison
            let _ = subquery; // Suppress unused warning
        }
        _ => {} // Ignore other expression types
    }
    Ok(())
}

fn extract_u64_value(expr: &Expr) -> Result<u64, EngineError> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {n}"))),
        _ => Err(EngineError::InvalidArgument(
            "expected numeric value".into(),
        )),
    }
}

/// Extract a numeric value from an expression for use in segment pruning.
/// Tries to parse as i64 first (most common), then f64 for floats.
fn try_extract_numeric_value(expr: &Expr) -> Option<NumericValue> {
    match expr {
        Expr::Value(Value::Number(n, _)) => {
            // Try parsing as i64 first (handles both positive and negative integers)
            if let Ok(v) = n.parse::<i64>() {
                return Some(NumericValue::Int64(v));
            }
            // Try parsing as f64 for floating point numbers
            if let Ok(v) = n.parse::<f64>() {
                return Some(NumericValue::Float64(v));
            }
            None
        }
        Expr::UnaryOp { op, expr: inner } => {
            // Handle negative numbers: -123
            if let sqlparser::ast::UnaryOperator::Minus = op {
                if let Expr::Value(Value::Number(n, _)) = inner.as_ref() {
                    if let Ok(v) = n.parse::<i64>() {
                        return Some(NumericValue::Int64(-v));
                    }
                    if let Ok(v) = n.parse::<f64>() {
                        return Some(NumericValue::Float64(-v));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract column name from an expression (handles both simple identifiers and compound identifiers)
/// Returns Option instead of Result for use in pattern matching
fn try_extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // For table.column, return just the column name
            parts.last().map(|p| p.value.clone())
        }
        _ => None,
    }
}

/// Collect all equality conditions from an OR expression tree.
/// For example: `a = 1 OR a = 2 OR b = 3` collects {a: [1, 2], b: [3]}
fn collect_or_equalities(expr: &Expr, values: &mut std::collections::HashMap<String, Vec<u64>>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            // Recursively collect from both sides of OR
            collect_or_equalities(left, values);
            collect_or_equalities(right, values);
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            // Extract column = value equality
            if let Some(col_name) = try_extract_column_name(left) {
                if let Some(val) = try_extract_u64_value(right) {
                    values.entry(col_name).or_insert_with(Vec::new).push(val);
                }
            } else if let Some(col_name) = try_extract_column_name(right) {
                // Handle reversed: value = column
                if let Some(val) = try_extract_u64_value(left) {
                    values.entry(col_name).or_insert_with(Vec::new).push(val);
                }
            }
        }
        Expr::Nested(inner) => {
            // Handle parenthesized expressions
            collect_or_equalities(inner, values);
        }
        _ => {}
    }
}

/// Try to extract a u64 value from an expression
fn try_extract_u64_value(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => try_extract_u64_value(expr),
        _ => None,
    }
}

fn extract_string_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}

fn parse_group_by_column(expr: &Expr) -> Result<GroupByColumn, EngineError> {
    match expr {
        Expr::Identifier(ident) => match ident.value.to_lowercase().as_str() {
            "tenant_id" => Ok(GroupByColumn::TenantId),
            "route_id" => Ok(GroupByColumn::RouteId),
            _ => Ok(GroupByColumn::Named(ident.value.clone())),
        },
        Expr::CompoundIdentifier(parts) => {
            // Handle table.column syntax - use the last part as the column name
            if let Some(last) = parts.last() {
                match last.value.to_lowercase().as_str() {
                    "tenant_id" => Ok(GroupByColumn::TenantId),
                    "route_id" => Ok(GroupByColumn::RouteId),
                    _ => Ok(GroupByColumn::Named(last.value.clone())),
                }
            } else {
                Err(EngineError::InvalidArgument(
                    "empty compound identifier in GROUP BY".into(),
                ))
            }
        }
        _ => Err(EngineError::NotImplemented(
            "only simple column references supported in GROUP BY".into(),
        )),
    }
}

fn parse_group_by(group_by: &GroupByExpr) -> Result<GroupBy, EngineError> {
    match group_by {
        GroupByExpr::Expressions(exprs) => {
            if exprs.is_empty() {
                return Ok(GroupBy::None);
            }

            // Check for ROLLUP, CUBE, or GROUPING SETS
            for expr in exprs {
                if let Expr::Rollup(rollup_exprs) = expr {
                    // ROLLUP(a, b, c) = GROUPING SETS ((a,b,c), (a,b), (a), ())
                    let columns = parse_nested_group_by_columns(rollup_exprs)?;
                    return Ok(GroupBy::Rollup(columns));
                }
                if let Expr::Cube(cube_exprs) = expr {
                    // CUBE(a, b) = GROUPING SETS ((a,b), (a), (b), ())
                    let columns = parse_nested_group_by_columns(cube_exprs)?;
                    return Ok(GroupBy::Cube(columns));
                }
                if let Expr::GroupingSets(sets) = expr {
                    // GROUPING SETS ((a, b), (a), ())
                    let mut grouping_sets = Vec::with_capacity(sets.len());
                    for set in sets {
                        let cols = parse_group_by_columns(set)?;
                        grouping_sets.push(cols);
                    }
                    return Ok(GroupBy::GroupingSets(grouping_sets));
                }
            }

            if exprs.len() == 1 {
                // Single column - use legacy types for backwards compatibility if possible
                match &exprs[0] {
                    Expr::Identifier(ident) => match ident.value.to_lowercase().as_str() {
                        "tenant_id" => Ok(GroupBy::Tenant),
                        "route_id" => Ok(GroupBy::Route),
                        _ => Ok(GroupBy::Columns(vec![GroupByColumn::Named(
                            ident.value.clone(),
                        )])),
                    },
                    Expr::CompoundIdentifier(parts) => {
                        // Handle table.column syntax
                        if let Some(last) = parts.last() {
                            match last.value.to_lowercase().as_str() {
                                "tenant_id" => Ok(GroupBy::Tenant),
                                "route_id" => Ok(GroupBy::Route),
                                _ => Ok(GroupBy::Columns(vec![GroupByColumn::Named(
                                    last.value.clone(),
                                )])),
                            }
                        } else {
                            Err(EngineError::InvalidArgument(
                                "empty compound identifier in GROUP BY".into(),
                            ))
                        }
                    }
                    _ => Err(EngineError::NotImplemented(
                        "only simple column references supported in GROUP BY".into(),
                    )),
                }
            } else {
                // Multiple columns - parse each and return Columns variant
                let mut columns = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    columns.push(parse_group_by_column(expr)?);
                }
                // Check for duplicates
                let mut seen = std::collections::HashSet::new();
                for col in &columns {
                    if !seen.insert(col) {
                        return Err(EngineError::InvalidArgument(
                            "duplicate column in GROUP BY".into(),
                        ));
                    }
                }
                Ok(GroupBy::Columns(columns))
            }
        }
        GroupByExpr::All => Ok(GroupBy::All),
    }
}

/// Parse a list of expressions into GroupByColumns
fn parse_group_by_columns(exprs: &[Expr]) -> Result<Vec<GroupByColumn>, EngineError> {
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        columns.push(parse_group_by_column(expr)?);
    }
    Ok(columns)
}

/// Parse nested group by columns (for ROLLUP and CUBE which use Vec<Vec<Expr>>)
fn parse_nested_group_by_columns(
    nested_exprs: &[Vec<Expr>],
) -> Result<Vec<GroupByColumn>, EngineError> {
    let mut columns = Vec::with_capacity(nested_exprs.len());
    for inner in nested_exprs {
        if inner.len() == 1 {
            // Simple column reference
            columns.push(parse_group_by_column(&inner[0])?);
        } else if inner.is_empty() {
            // Skip empty groups (represents grand total)
            continue;
        } else {
            // Composite grouping columns like (a, b) - not currently supported
            return Err(EngineError::NotImplemented(
                "composite grouping columns in ROLLUP/CUBE are not supported".into(),
            ));
        }
    }
    Ok(columns)
}

/// Parse HAVING clause expression
fn parse_having_clause(expr: &Expr) -> Result<Vec<HavingCondition>, EngineError> {
    let mut conditions = Vec::new();
    parse_having_expr(expr, &mut conditions)?;
    Ok(conditions)
}

/// Recursively parse HAVING expression (handles AND/OR)
fn parse_having_expr(
    expr: &Expr,
    conditions: &mut Vec<HavingCondition>,
) -> Result<(), EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    // Parse both sides of AND
                    parse_having_expr(left, conditions)?;
                    parse_having_expr(right, conditions)?;
                }
                BinaryOperator::Or => {
                    // OR is more complex - for now, just parse both sides
                    // (this is a simplification; full OR support would need expression trees)
                    parse_having_expr(left, conditions)?;
                    parse_having_expr(right, conditions)?;
                }
                // Comparison operators
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq => {
                    // Left side should be an aggregate function
                    if let Expr::Function(func) = left.as_ref() {
                        if let Some((agg_kind, _filter)) = parse_aggregate_function(func)? {
                            let having_op = match op {
                                BinaryOperator::Eq => HavingOp::Eq,
                                BinaryOperator::NotEq => HavingOp::Ne,
                                BinaryOperator::Gt => HavingOp::Gt,
                                BinaryOperator::GtEq => HavingOp::Ge,
                                BinaryOperator::Lt => HavingOp::Lt,
                                BinaryOperator::LtEq => HavingOp::Le,
                                _ => unreachable!(),
                            };
                            let value = extract_numeric_value(right)?;
                            conditions.push(HavingCondition {
                                agg: agg_kind,
                                op: having_op,
                                value,
                            });
                        } else {
                            return Err(EngineError::InvalidArgument(
                                "HAVING clause requires aggregate function".into(),
                            ));
                        }
                    } else {
                        return Err(EngineError::InvalidArgument(
                            "HAVING clause requires aggregate function on left side".into(),
                        ));
                    }
                }
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported operator in HAVING: {:?}",
                        op
                    )));
                }
            }
        }
        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported expression in HAVING: {}",
                expr
            )));
        }
    }
    Ok(())
}

/// Extract a numeric value from an expression
fn extract_numeric_value(expr: &Expr) -> Result<f64, EngineError> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n
            .parse::<f64>()
            .map_err(|_| EngineError::InvalidArgument(format!("invalid number in HAVING: {}", n))),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let val = extract_numeric_value(expr)?;
            Ok(-val)
        }
        _ => Err(EngineError::InvalidArgument(
            "HAVING clause requires numeric literal for comparison".into(),
        )),
    }
}

/// Parse INSERT statement
fn parse_insert(
    table_name: &ObjectName,
    columns: &[Ident],
    source: &Option<Box<Query>>,
    returning: &Option<Vec<sqlparser::ast::SelectItem>>,
    on: &Option<sqlparser::ast::OnInsert>,
) -> Result<SqlStatement, EngineError> {
    use sqlparser::ast::{ConflictTarget, OnConflictAction as SqlOnConflictAction, OnInsert};

    let full_name = table_name.to_string();
    let (database, table) = parse_table_name(&full_name)?;

    // Extract column names if specified
    let cols = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|c| c.value.clone()).collect())
    };

    // Parse VALUES clause
    let source = source
        .as_ref()
        .ok_or_else(|| EngineError::InvalidArgument("INSERT requires VALUES clause".into()))?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => {
            let mut all_rows = Vec::new();
            for row in &values.rows {
                let mut row_values = Vec::new();
                for expr in row {
                    row_values.push(expr_to_sql_value(expr)?);
                }
                all_rows.push(row_values);
            }
            all_rows
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "INSERT only supports VALUES clause, not SELECT".into(),
            ))
        }
    };

    if values.is_empty() {
        return Err(EngineError::InvalidArgument(
            "INSERT requires at least one row".into(),
        ));
    }

    // Parse RETURNING clause
    let returning_cols = parse_returning_clause(returning)?;

    // Parse ON CONFLICT / ON DUPLICATE KEY clause
    let on_conflict = match on {
        None => None,
        Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
            // MySQL syntax: ON DUPLICATE KEY UPDATE
            let parsed_assignments: Vec<(String, String)> = assignments
                .iter()
                .map(|a| {
                    let col =
                        a.id.iter()
                            .map(|i| i.value.clone())
                            .collect::<Vec<_>>()
                            .join(".");
                    let value = a.value.to_string();
                    (col, value)
                })
                .collect();
            Some(OnConflict {
                columns: None, // MySQL syntax doesn't specify conflict columns
                action: OnConflictAction::DoUpdate {
                    assignments: parsed_assignments,
                    where_clause: None,
                },
            })
        }
        Some(OnInsert::OnConflict(conflict)) => {
            // PostgreSQL syntax: ON CONFLICT
            let columns = match &conflict.conflict_target {
                None => None,
                Some(ConflictTarget::Columns(cols)) => {
                    Some(cols.iter().map(|c| c.value.clone()).collect())
                }
                Some(ConflictTarget::OnConstraint(name)) => {
                    // For ON CONSTRAINT, store the constraint name
                    Some(vec![format!("CONSTRAINT:{}", name)])
                }
            };

            let action = match &conflict.action {
                SqlOnConflictAction::DoNothing => OnConflictAction::DoNothing,
                SqlOnConflictAction::DoUpdate(update) => {
                    let assignments: Vec<(String, String)> = update
                        .assignments
                        .iter()
                        .map(|a| {
                            let col =
                                a.id.iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".");
                            let value = a.value.to_string();
                            (col, value)
                        })
                        .collect();
                    let where_clause = update.selection.as_ref().map(|e| e.to_string());
                    OnConflictAction::DoUpdate {
                        assignments,
                        where_clause,
                    }
                }
            };

            Some(OnConflict { columns, action })
        }
        Some(_) => {
            // Handle any future OnInsert variants gracefully
            return Err(EngineError::NotImplemented(
                "Unsupported ON INSERT variant".into(),
            ));
        }
    };

    Ok(SqlStatement::Insert(InsertCommand {
        database,
        table,
        columns: cols,
        values,
        on_conflict,
        returning: returning_cols,
    }))
}

/// Parse RETURNING clause columns
fn parse_returning_clause(
    returning: &Option<Vec<sqlparser::ast::SelectItem>>,
) -> Result<Option<Vec<String>>, EngineError> {
    match returning {
        None => Ok(None),
        Some(items) => {
            let mut cols = Vec::new();
            for item in items {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        cols.push(ident.value.clone());
                    }
                    sqlparser::ast::SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                        // Take the last part (column name)
                        if let Some(last) = parts.last() {
                            cols.push(last.value.clone());
                        }
                    }
                    sqlparser::ast::SelectItem::Wildcard(_) => {
                        cols.push("*".to_string());
                    }
                    sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                        // Use alias if provided, otherwise extract column name
                        match expr {
                            Expr::Identifier(ident) => cols.push(alias.value.clone()),
                            _ => cols.push(alias.value.clone()),
                        }
                    }
                    _ => {
                        return Err(EngineError::NotImplemented(format!(
                            "unsupported RETURNING item: {item}"
                        )))
                    }
                }
            }
            Ok(Some(cols))
        }
    }
}

/// Convert an Expr to SqlValue
fn expr_to_sql_value(expr: &Expr) -> Result<SqlValue, EngineError> {
    match expr {
        Expr::Value(v) => match v {
            Value::Null => Ok(SqlValue::Null),
            Value::Number(n, _) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(SqlValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(SqlValue::Float(f))
                } else {
                    Err(EngineError::InvalidArgument(format!("invalid number: {n}")))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(SqlValue::String(s.clone()))
            }
            Value::Boolean(b) => Ok(SqlValue::Boolean(*b)),
            _ => Err(EngineError::NotImplemented(format!(
                "unsupported value type: {v}"
            ))),
        },
        Expr::UnaryOp { op, expr } => {
            use sqlparser::ast::UnaryOperator;
            match op {
                UnaryOperator::Minus => {
                    let inner = expr_to_sql_value(expr)?;
                    match inner {
                        SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                        SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                        _ => Err(EngineError::InvalidArgument(
                            "unary minus only applies to numbers".into(),
                        )),
                    }
                }
                UnaryOperator::Plus => expr_to_sql_value(expr),
                _ => Err(EngineError::NotImplemented(format!(
                    "unsupported unary operator: {op}"
                ))),
            }
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression in INSERT: {expr}"
        ))),
    }
}

/// Parse a TableWithJoins into JoinTable
fn parse_join_table(twj: &sqlparser::ast::TableWithJoins) -> Result<JoinTable, EngineError> {
    match &twj.relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            let (database, table) = parse_table_name(&table_name)?;
            let table_alias = alias.as_ref().map(|a| a.name.value.clone());
            Ok(JoinTable {
                database,
                table,
                alias: table_alias,
            })
        }
        _ => Err(EngineError::NotImplemented(
            "only simple table references are supported in FROM/USING".into(),
        )),
    }
}

/// Parse UPDATE statement
fn parse_update(
    table: &sqlparser::ast::TableWithJoins,
    assignments: &[sqlparser::ast::Assignment],
    from: &Option<sqlparser::ast::TableWithJoins>,
    selection: &Option<Expr>,
    returning: &Option<Vec<sqlparser::ast::SelectItem>>,
) -> Result<SqlStatement, EngineError> {
    // Extract table name and alias from the UPDATE clause
    let (table_name, table_alias) = match &table.relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        _ => {
            return Err(EngineError::NotImplemented(
                "UPDATE only supports simple table references".into(),
            ))
        }
    };
    let (database, table) = parse_table_name(&table_name)?;

    // Parse SET assignments
    let mut parsed_assignments = Vec::new();
    for assignment in assignments {
        // Get column name - handle both single and compound identifiers
        let column_name = if assignment.id.len() == 1 {
            assignment.id[0].value.clone()
        } else {
            // For compound identifiers like table.column, take the last part
            assignment
                .id
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| {
                    EngineError::InvalidArgument("empty column identifier in SET".into())
                })?
        };
        let value = expr_to_sql_value(&assignment.value)?;
        parsed_assignments.push((column_name, value));
    }

    // Parse FROM clause for join-based updates
    let from_clause = if let Some(from_twj) = from {
        let mut tables = vec![parse_join_table(from_twj)?];
        // Also handle any JOINs in the FROM clause
        for join in &from_twj.joins {
            if let TableFactor::Table { name, alias, .. } = &join.relation {
                let join_table_name = name.to_string();
                let (join_db, join_tbl) = parse_table_name(&join_table_name)?;
                tables.push(JoinTable {
                    database: join_db,
                    table: join_tbl,
                    alias: alias.as_ref().map(|a| a.name.value.clone()),
                });
            }
        }
        Some(tables)
    } else {
        None
    };

    // Convert WHERE clause to string representation (simplified for now)
    let where_clause = selection.as_ref().map(|expr| expr.to_string());

    // Parse RETURNING clause
    let returning_cols = parse_returning_clause(returning)?;

    Ok(SqlStatement::Update(UpdateCommand {
        database,
        table,
        alias: table_alias,
        assignments: parsed_assignments,
        from_clause,
        where_clause,
        returning: returning_cols,
    }))
}

/// Parse DELETE statement
fn parse_delete(
    from: &[sqlparser::ast::TableWithJoins],
    using: &Option<Vec<sqlparser::ast::TableWithJoins>>,
    selection: &Option<Expr>,
    returning: &Option<Vec<sqlparser::ast::SelectItem>>,
) -> Result<SqlStatement, EngineError> {
    // Extract table name and alias from the FROM clause
    let from_table = from
        .first()
        .ok_or_else(|| EngineError::InvalidArgument("DELETE requires FROM clause".into()))?;

    let (table_name, table_alias) = match &from_table.relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        _ => {
            return Err(EngineError::NotImplemented(
                "DELETE only supports simple table references".into(),
            ))
        }
    };
    let (database, table) = parse_table_name(&table_name)?;

    // Parse USING clause for join-based deletes
    let using_clause = if let Some(using_tables) = using {
        let mut tables = Vec::new();
        for twj in using_tables {
            tables.push(parse_join_table(twj)?);
            // Also handle any JOINs in the USING clause
            for join in &twj.joins {
                if let TableFactor::Table { name, alias, .. } = &join.relation {
                    let join_table_name = name.to_string();
                    let (join_db, join_tbl) = parse_table_name(&join_table_name)?;
                    tables.push(JoinTable {
                        database: join_db,
                        table: join_tbl,
                        alias: alias.as_ref().map(|a| a.name.value.clone()),
                    });
                }
            }
        }
        Some(tables)
    } else {
        None
    };

    // Convert WHERE clause to string representation (simplified for now)
    let where_clause = selection.as_ref().map(|expr| expr.to_string());

    // Parse RETURNING clause
    let returning_cols = parse_returning_clause(returning)?;

    Ok(SqlStatement::Delete(DeleteCommand {
        database,
        table,
        alias: table_alias,
        using_clause,
        where_clause,
        returning: returning_cols,
    }))
}

/// Parse a MERGE statement
fn parse_merge(
    table: &TableFactor,
    source: &TableFactor,
    on: &Expr,
    clauses: &[sqlparser::ast::MergeClause],
) -> Result<SqlStatement, EngineError> {
    use sqlparser::ast::MergeClause;

    // Extract target table
    let target_name = match table {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(EngineError::NotImplemented(
                "MERGE only supports simple table references as target".into(),
            ))
        }
    };
    let (database, table_name) = parse_table_name(&target_name)?;

    // Extract source table
    let source_name = match source {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(EngineError::NotImplemented(
                "MERGE only supports simple table references as source".into(),
            ))
        }
    };
    let (source_db, source_table) = parse_table_name(&source_name)?;

    // ON condition
    let on_condition = on.to_string();

    // Parse WHEN clauses
    let mut when_matched = Vec::new();
    let mut when_not_matched = Vec::new();

    for clause in clauses {
        match clause {
            MergeClause::MatchedUpdate {
                predicate,
                assignments,
            } => {
                let mut assigns: Vec<(String, SqlValue)> = Vec::new();
                for a in assignments {
                    let col =
                        a.id.iter()
                            .map(|i| i.value.clone())
                            .collect::<Vec<_>>()
                            .join(".");
                    let val = expr_to_sql_value(&a.value)?;
                    assigns.push((col, val));
                }
                when_matched.push(MergeWhenMatched::Update {
                    assignments: assigns,
                    condition: predicate.as_ref().map(|e| e.to_string()),
                });
            }
            MergeClause::MatchedDelete(predicate) => {
                when_matched.push(MergeWhenMatched::Delete {
                    condition: predicate.as_ref().map(|e| e.to_string()),
                });
            }
            MergeClause::NotMatched {
                predicate,
                columns,
                values,
            } => {
                let cols: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();
                // Get first row of values
                let mut vals: Vec<SqlValue> = Vec::new();
                if let Some(row) = values.rows.first() {
                    for e in row {
                        vals.push(expr_to_sql_value(e)?);
                    }
                }
                when_not_matched.push(MergeWhenNotMatched {
                    columns: cols,
                    values: vals,
                    condition: predicate.as_ref().map(|e| e.to_string()),
                });
            }
        }
    }

    Ok(SqlStatement::Merge(MergeCommand {
        database,
        table: table_name,
        source_database: source_db,
        source_table,
        on_condition,
        when_matched,
        when_not_matched,
    }))
}

// ============================================================================
// Expression Parsing for Scalar Functions, Window Functions, and CTEs
// ============================================================================

/// Parse a sqlparser Expr into our SelectExpr representation
pub fn parse_expr(expr: &Expr) -> Result<SelectExpr, EngineError> {
    match expr {
        Expr::Identifier(ident) => Ok(SelectExpr::Column(ident.value.clone())),

        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(SelectExpr::QualifiedColumn {
                    table: parts[0].value.clone(),
                    column: parts[1].value.clone(),
                })
            } else if parts.len() == 1 {
                Ok(SelectExpr::Column(parts[0].value.clone()))
            } else {
                Err(EngineError::NotImplemented(
                    "compound identifiers with more than 2 parts not supported".into(),
                ))
            }
        }

        Expr::Value(val) => match val {
            Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(SelectExpr::Literal(LiteralValue::Float(
                        n.parse().map_err(|_| {
                            EngineError::InvalidArgument(format!("invalid float: {n}"))
                        })?,
                    )))
                } else {
                    Ok(SelectExpr::Literal(LiteralValue::Integer(
                        n.parse().map_err(|_| {
                            EngineError::InvalidArgument(format!("invalid integer: {n}"))
                        })?,
                    )))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(SelectExpr::Literal(LiteralValue::String(s.clone())))
            }
            Value::Boolean(b) => Ok(SelectExpr::Literal(LiteralValue::Boolean(*b))),
            Value::Null => Ok(SelectExpr::Null),
            _ => Err(EngineError::NotImplemented(format!(
                "unsupported literal value: {val}"
            ))),
        },

        Expr::Function(func) => parse_function_expr(func),

        Expr::BinaryOp { left, op, right } => {
            // Check for datetime +/- INTERVAL arithmetic
            if let Expr::Interval(interval) = right.as_ref() {
                let left_expr = parse_expr(left)?;
                let (interval_value, unit) = parse_interval(interval)?;
                return match op {
                    BinaryOperator::Plus => Ok(SelectExpr::Function(ScalarFunction::DateAdd {
                        expr: Box::new(left_expr),
                        interval: interval_value,
                        unit,
                    })),
                    BinaryOperator::Minus => Ok(SelectExpr::Function(ScalarFunction::DateSub {
                        expr: Box::new(left_expr),
                        interval: interval_value,
                        unit,
                    })),
                    _ => Err(EngineError::InvalidArgument(format!(
                        "unsupported operator with INTERVAL: {op}"
                    ))),
                };
            }

            let left_expr = parse_expr(left)?;
            let right_expr = parse_expr(right)?;
            let op_str = match op {
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
                BinaryOperator::StringConcat => "||",
                BinaryOperator::Gt => ">",
                BinaryOperator::Lt => "<",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported binary operator: {op}"
                    )))
                }
            };
            Ok(SelectExpr::BinaryOp {
                left: Box::new(left_expr),
                op: op_str.to_string(),
                right: Box::new(right_expr),
            })
        }

        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = parse_expr(inner)?;
            let op_str = match op {
                sqlparser::ast::UnaryOperator::Minus => "-",
                sqlparser::ast::UnaryOperator::Plus => "+",
                sqlparser::ast::UnaryOperator::Not => "NOT",
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported unary operator: {op}"
                    )))
                }
            };
            Ok(SelectExpr::UnaryOp {
                op: op_str.to_string(),
                expr: Box::new(inner_expr),
            })
        }

        Expr::Nested(inner) => parse_expr(inner),

        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand_expr = operand.as_ref().map(|e| parse_expr(e)).transpose()?;
            let mut when_clauses = Vec::new();
            for (cond, res) in conditions.iter().zip(results.iter()) {
                when_clauses.push((parse_expr(cond)?, parse_expr(res)?));
            }
            let else_expr = else_result.as_ref().map(|e| parse_expr(e)).transpose()?;
            Ok(SelectExpr::Case {
                operand: operand_expr.map(Box::new),
                when_clauses,
                else_result: else_expr.map(Box::new),
            })
        }

        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let inner_expr = parse_expr(inner)?;
            Ok(SelectExpr::Function(ScalarFunction::Cast {
                expr: Box::new(inner_expr),
                target_type: data_type.to_string(),
            }))
        }

        // PostgreSQL JSON operators: ->, ->>, #>, #>>
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => {
            use sqlparser::ast::JsonOperator;
            let left_expr = parse_expr(left)?;
            match operator {
                // -> : Extract JSON object field by key or array element by index (returns JSON)
                JsonOperator::Arrow => {
                    let path = extract_json_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonExtract {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // ->> : Extract JSON object field as text (returns TEXT)
                JsonOperator::LongArrow => {
                    let path = extract_json_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonExtractScalar {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // #> : Extract JSON sub-object at specified path (returns JSON)
                JsonOperator::HashArrow => {
                    let path = extract_json_array_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonExtract {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // #>> : Extract JSON sub-object at specified path as text (returns TEXT)
                JsonOperator::HashLongArrow => {
                    let path = extract_json_array_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonExtractScalar {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // : Colon (Snowflake variant, similar to ->>)
                JsonOperator::Colon => {
                    let path = extract_json_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonExtractScalar {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // @> : JSON contains
                JsonOperator::AtArrow => Ok(SelectExpr::Function(ScalarFunction::JsonContains {
                    left: Box::new(left_expr),
                    right: Box::new(parse_expr(right)?),
                })),
                // <@ : JSON contained by
                JsonOperator::ArrowAt => {
                    let right_expr = parse_expr(right)?;
                    // Swap left and right for "contained by" semantics
                    Ok(SelectExpr::Function(ScalarFunction::JsonContains {
                        left: Box::new(right_expr),
                        right: Box::new(left_expr),
                    }))
                }
                // #- : Delete path from JSON
                JsonOperator::HashMinus => {
                    let path = extract_json_array_path(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonRemove {
                        expr: Box::new(left_expr),
                        path,
                    }))
                }
                // @? : JSON path exists
                JsonOperator::AtQuestion => {
                    let path_expr = parse_expr(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonPathExists {
                        expr: Box::new(left_expr),
                        path: Box::new(path_expr),
                    }))
                }
                // @@ : JSON path match
                JsonOperator::AtAt => {
                    let path_expr = parse_expr(right)?;
                    Ok(SelectExpr::Function(ScalarFunction::JsonPathMatch {
                        expr: Box::new(left_expr),
                        path: Box::new(path_expr),
                    }))
                }
            }
        }

        Expr::Extract { field, expr } => {
            let parsed_expr = parse_expr(expr)?;
            Ok(SelectExpr::Function(ScalarFunction::Extract {
                field: field.to_string(),
                expr: Box::new(parsed_expr),
            }))
        }

        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression: {expr}"
        ))),
    }
}

/// Extract a JSON path from the right side of -> or ->> operator
/// For simple key access like `col -> 'key'`, returns `$.key`
/// For array index access like `col -> 0`, returns `$[0]`
fn extract_json_path(expr: &Expr) -> Result<String, EngineError> {
    match expr {
        // String literal key: col -> 'key' becomes $.key
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s))
        | Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => Ok(format!("$.{}", s)),
        // Numeric index: col -> 0 becomes $[0]
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => Ok(format!("$[{}]", n)),
        // Identifier (unquoted): col -> key becomes $.key
        Expr::Identifier(ident) => Ok(format!("$.{}", ident.value)),
        _ => Err(EngineError::InvalidArgument(format!(
            "JSON operator expects string key or numeric index, got: {}",
            expr
        ))),
    }
}

/// Extract a JSON path from the right side of #> or #>> operator
/// For path array like `col #> '{a,b,c}'`, returns `$.a.b.c`
fn extract_json_array_path(expr: &Expr) -> Result<String, EngineError> {
    match expr {
        // PostgreSQL path array: '{a,b,c}' becomes $.a.b.c
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s))
        | Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => {
            // Parse PostgreSQL array literal format: {a,b,c}
            let s = s.trim();
            if s.starts_with('{') && s.ends_with('}') {
                let inner = &s[1..s.len() - 1];
                let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
                if parts.is_empty() {
                    return Ok("$".to_string());
                }
                // Build JSONPath: $.a.b.c or handle numeric indices
                let mut path = String::from("$");
                for part in parts {
                    if part.parse::<i64>().is_ok() {
                        path.push_str(&format!("[{}]", part));
                    } else {
                        path.push_str(&format!(".{}", part));
                    }
                }
                Ok(path)
            } else {
                // Plain path string
                Ok(format!("$.{}", s))
            }
        }
        // Array constructor: ARRAY['a', 'b', 'c']
        Expr::Array(arr) => {
            let mut path = String::from("$");
            for elem in &arr.elem {
                match elem {
                    Expr::Value(sqlparser::ast::Value::SingleQuotedString(s))
                    | Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => {
                        if s.parse::<i64>().is_ok() {
                            path.push_str(&format!("[{}]", s));
                        } else {
                            path.push_str(&format!(".{}", s));
                        }
                    }
                    Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        path.push_str(&format!("[{}]", n));
                    }
                    _ => {
                        return Err(EngineError::InvalidArgument(format!(
                            "JSON path array element must be string or number, got: {}",
                            elem
                        )))
                    }
                }
            }
            Ok(path)
        }
        _ => Err(EngineError::InvalidArgument(format!(
            "JSON #> operator expects path array like '{{a,b,c}}', got: {}",
            expr
        ))),
    }
}

/// Parse a sqlparser Interval struct into (value, unit)
fn parse_interval(interval: &sqlparser::ast::Interval) -> Result<(i64, String), EngineError> {
    // Extract the interval value expression
    let value_str = interval.value.to_string();
    // Remove quotes if present
    let value_str = value_str.trim_matches('\'').trim_matches('"');

    // Parse formats like "24 hours", "7 days", "1 month"
    let parts: Vec<&str> = value_str.split_whitespace().collect();
    if parts.len() >= 2 {
        // Format: "24 hours" or "7 days"
        let value = parts[0].parse::<i64>().map_err(|_| {
            EngineError::InvalidArgument(format!("invalid interval value: {}", parts[0]))
        })?;
        let unit = parts[1].to_lowercase();
        return Ok((value, unit));
    } else if parts.len() == 1 {
        // Try to parse as a number, use leading_field for unit
        if let Ok(value) = parts[0].parse::<i64>() {
            let unit = interval
                .leading_field
                .as_ref()
                .map(|f| f.to_string().to_lowercase())
                .unwrap_or_else(|| "day".to_string());
            return Ok((value, unit));
        }
    }

    // Try to extract from leading_field if value is just a number
    if let Ok(value) = value_str.parse::<i64>() {
        let unit = interval
            .leading_field
            .as_ref()
            .map(|f| f.to_string().to_lowercase())
            .unwrap_or_else(|| "day".to_string());
        return Ok((value, unit));
    }

    Err(EngineError::InvalidArgument(format!(
        "unable to parse interval: {}",
        value_str
    )))
}

/// Parse a function call into SelectExpr
fn parse_function_expr(func: &sqlparser::ast::Function) -> Result<SelectExpr, EngineError> {
    let func_name = func.name.to_string().to_lowercase();
    let args: Vec<SelectExpr> = func
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(parse_expr(e)),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Some(Ok(SelectExpr::Column("*".to_string())))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Check for window function (OVER clause)
    if func.over.is_some() {
        return parse_window_function(&func_name, &args, func);
    }

    // Aggregate functions
    match func_name.as_str() {
        "count" => {
            return Ok(SelectExpr::Aggregate(AggKind::CountStar));
        }
        "sum" => {
            let col = get_single_column_arg(&args, "SUM")?;
            return Ok(SelectExpr::Aggregate(AggKind::Sum { column: col }));
        }
        "avg" => {
            let col = get_single_column_arg(&args, "AVG")?;
            return Ok(SelectExpr::Aggregate(AggKind::Avg { column: col }));
        }
        "min" => {
            let col = get_single_column_arg(&args, "MIN")?;
            return Ok(SelectExpr::Aggregate(AggKind::Min { column: col }));
        }
        "max" => {
            let col = get_single_column_arg(&args, "MAX")?;
            return Ok(SelectExpr::Aggregate(AggKind::Max { column: col }));
        }
        _ => {}
    }

    // Scalar functions
    let scalar_func = match func_name.as_str() {
        // String functions
        "upper" => ScalarFunction::Upper(Box::new(get_arg(&args, 0, "UPPER")?)),
        "lower" => ScalarFunction::Lower(Box::new(get_arg(&args, 0, "LOWER")?)),
        "length" | "len" | "char_length" | "character_length" => {
            ScalarFunction::Length(Box::new(get_arg(&args, 0, "LENGTH")?))
        }
        "trim" => ScalarFunction::Trim(Box::new(get_arg(&args, 0, "TRIM")?)),
        "ltrim" => ScalarFunction::LTrim(Box::new(get_arg(&args, 0, "LTRIM")?)),
        "rtrim" => ScalarFunction::RTrim(Box::new(get_arg(&args, 0, "RTRIM")?)),
        "concat" => ScalarFunction::Concat(args),
        "substring" | "substr" => {
            let expr = get_arg(&args, 0, "SUBSTRING")?;
            let start = get_int_arg(&args, 1, "SUBSTRING")?;
            let length = if args.len() > 2 {
                Some(get_int_arg(&args, 2, "SUBSTRING")?)
            } else {
                None
            };
            ScalarFunction::Substring {
                expr: Box::new(expr),
                start,
                length,
            }
        }
        "replace" => {
            let expr = get_arg(&args, 0, "REPLACE")?;
            let from = get_string_arg(&args, 1, "REPLACE")?;
            let to = get_string_arg(&args, 2, "REPLACE")?;
            ScalarFunction::Replace {
                expr: Box::new(expr),
                from,
                to,
            }
        }
        "left" => {
            let expr = get_arg(&args, 0, "LEFT")?;
            let count = get_int_arg(&args, 1, "LEFT")?;
            ScalarFunction::Left {
                expr: Box::new(expr),
                count,
            }
        }
        "right" => {
            let expr = get_arg(&args, 0, "RIGHT")?;
            let count = get_int_arg(&args, 1, "RIGHT")?;
            ScalarFunction::Right {
                expr: Box::new(expr),
                count,
            }
        }
        "reverse" => ScalarFunction::Reverse(Box::new(get_arg(&args, 0, "REVERSE")?)),
        "coalesce" => ScalarFunction::Coalesce(args),

        // Regex functions
        "regexp_replace" => {
            let expr = get_arg(&args, 0, "REGEXP_REPLACE")?;
            let pattern = get_string_arg(&args, 1, "REGEXP_REPLACE")?;
            let replacement = get_string_arg(&args, 2, "REGEXP_REPLACE")?;
            let flags = if args.len() > 3 {
                Some(get_string_arg(&args, 3, "REGEXP_REPLACE")?)
            } else {
                None
            };
            ScalarFunction::RegexpReplace {
                expr: Box::new(expr),
                pattern,
                replacement,
                flags,
            }
        }
        "regexp_match" | "regexp_matches" => {
            let expr = get_arg(&args, 0, "REGEXP_MATCH")?;
            let pattern = get_string_arg(&args, 1, "REGEXP_MATCH")?;
            let flags = if args.len() > 2 {
                Some(get_string_arg(&args, 2, "REGEXP_MATCH")?)
            } else {
                None
            };
            ScalarFunction::RegexpMatch {
                expr: Box::new(expr),
                pattern,
                flags,
            }
        }
        "regexp_extract" | "regexp_substr" => {
            let expr = get_arg(&args, 0, "REGEXP_EXTRACT")?;
            let pattern = get_string_arg(&args, 1, "REGEXP_EXTRACT")?;
            let group_index = if args.len() > 2 {
                Some(get_int_arg(&args, 2, "REGEXP_EXTRACT")?)
            } else {
                None
            };
            ScalarFunction::RegexpExtract {
                expr: Box::new(expr),
                pattern,
                group_index,
            }
        }

        // Math functions
        "abs" => ScalarFunction::Abs(Box::new(get_arg(&args, 0, "ABS")?)),
        "round" => {
            let expr = get_arg(&args, 0, "ROUND")?;
            let precision = if args.len() > 1 {
                Some(get_int_arg(&args, 1, "ROUND")? as i32)
            } else {
                None
            };
            ScalarFunction::Round {
                expr: Box::new(expr),
                precision,
            }
        }
        "ceil" | "ceiling" => ScalarFunction::Ceil(Box::new(get_arg(&args, 0, "CEIL")?)),
        "floor" => ScalarFunction::Floor(Box::new(get_arg(&args, 0, "FLOOR")?)),
        "mod" => {
            let dividend = get_arg(&args, 0, "MOD")?;
            let divisor = get_arg(&args, 1, "MOD")?;
            ScalarFunction::Mod {
                dividend: Box::new(dividend),
                divisor: Box::new(divisor),
            }
        }
        "power" | "pow" => {
            let base = get_arg(&args, 0, "POWER")?;
            let exponent = get_arg(&args, 1, "POWER")?;
            ScalarFunction::Power {
                base: Box::new(base),
                exponent: Box::new(exponent),
            }
        }
        "sqrt" => ScalarFunction::Sqrt(Box::new(get_arg(&args, 0, "SQRT")?)),
        "log" => {
            let expr = get_arg(&args, 0, "LOG")?;
            let base = if args.len() > 1 {
                Some(get_float_arg(&args, 1, "LOG")?)
            } else {
                None
            };
            ScalarFunction::Log {
                expr: Box::new(expr),
                base,
            }
        }
        "ln" => ScalarFunction::Ln(Box::new(get_arg(&args, 0, "LN")?)),
        "exp" => ScalarFunction::Exp(Box::new(get_arg(&args, 0, "EXP")?)),
        "sign" => ScalarFunction::Sign(Box::new(get_arg(&args, 0, "SIGN")?)),
        "greatest" => ScalarFunction::Greatest(args),
        "least" => ScalarFunction::Least(args),

        // Date/Time functions
        "now" | "current_timestamp" => ScalarFunction::Now,
        "current_date" => ScalarFunction::CurrentDate,
        "date_trunc" => {
            let unit = get_string_arg(&args, 0, "DATE_TRUNC")?;
            let expr = get_arg(&args, 1, "DATE_TRUNC")?;
            ScalarFunction::DateTrunc {
                unit,
                expr: Box::new(expr),
            }
        }
        "extract" => {
            // EXTRACT is usually parsed specially by sqlparser, but handle function form too
            let field = get_string_arg(&args, 0, "EXTRACT")?;
            let expr = get_arg(&args, 1, "EXTRACT")?;
            ScalarFunction::Extract {
                field,
                expr: Box::new(expr),
            }
        }
        "date_add" | "dateadd" => {
            let expr = get_arg(&args, 0, "DATE_ADD")?;
            let interval = get_int_arg(&args, 1, "DATE_ADD")?;
            let unit = get_string_arg(&args, 2, "DATE_ADD")?;
            ScalarFunction::DateAdd {
                expr: Box::new(expr),
                interval,
                unit,
            }
        }
        "date_sub" | "datesub" => {
            let expr = get_arg(&args, 0, "DATE_SUB")?;
            let interval = get_int_arg(&args, 1, "DATE_SUB")?;
            let unit = get_string_arg(&args, 2, "DATE_SUB")?;
            ScalarFunction::DateSub {
                expr: Box::new(expr),
                interval,
                unit,
            }
        }
        "datediff" => {
            let unit = get_string_arg(&args, 0, "DATEDIFF")?;
            let start = get_arg(&args, 1, "DATEDIFF")?;
            let end = get_arg(&args, 2, "DATEDIFF")?;
            ScalarFunction::DateDiff {
                unit,
                start: Box::new(start),
                end: Box::new(end),
            }
        }
        "to_timestamp" => ScalarFunction::ToTimestamp(Box::new(get_arg(&args, 0, "TO_TIMESTAMP")?)),
        "from_unixtime" => {
            ScalarFunction::FromUnixtime(Box::new(get_arg(&args, 0, "FROM_UNIXTIME")?))
        }
        "date" => ScalarFunction::Date(Box::new(get_arg(&args, 0, "DATE")?)),
        "tostring" | "to_string" | "tostr" => {
            ScalarFunction::ToString(Box::new(get_arg(&args, 0, "TOSTRING")?))
        }

        // JSON functions
        "json_extract" | "json_value" => {
            let expr = get_arg(&args, 0, "JSON_EXTRACT")?;
            let path = get_string_arg(&args, 1, "JSON_EXTRACT")?;
            ScalarFunction::JsonExtract {
                expr: Box::new(expr),
                path,
            }
        }
        "json_extract_scalar" | "json_unquote" => {
            let expr = get_arg(&args, 0, "JSON_EXTRACT_SCALAR")?;
            let path = get_string_arg(&args, 1, "JSON_EXTRACT_SCALAR")?;
            ScalarFunction::JsonExtractScalar {
                expr: Box::new(expr),
                path,
            }
        }
        "json_extract_all" | "jsonpath_query" => {
            let expr = get_arg(&args, 0, "JSON_EXTRACT_ALL")?;
            let path = get_string_arg(&args, 1, "JSON_EXTRACT_ALL")?;
            ScalarFunction::JsonExtractAll {
                expr: Box::new(expr),
                path,
            }
        }
        "json_array" => ScalarFunction::JsonArray(args),
        "json_object" => {
            // Pairs of key, value
            if args.len() % 2 != 0 {
                return Err(EngineError::InvalidArgument(
                    "JSON_OBJECT requires an even number of arguments (key-value pairs)".into(),
                ));
            }
            let mut pairs = Vec::new();
            for i in (0..args.len()).step_by(2) {
                let key = match &args[i] {
                    SelectExpr::Literal(LiteralValue::String(s)) => s.clone(),
                    SelectExpr::Column(c) => c.clone(),
                    _ => {
                        return Err(EngineError::InvalidArgument(
                            "JSON_OBJECT keys must be string literals or column names".into(),
                        ));
                    }
                };
                pairs.push((key, args[i + 1].clone()));
            }
            ScalarFunction::JsonObject(pairs)
        }
        "json_type" | "json_typeof" => {
            ScalarFunction::JsonType(Box::new(get_arg(&args, 0, "JSON_TYPE")?))
        }
        "json_contains_path" | "json_exists" => {
            let expr = get_arg(&args, 0, "JSON_CONTAINS_PATH")?;
            let path = get_string_arg(&args, 1, "JSON_CONTAINS_PATH")?;
            ScalarFunction::JsonContainsPath {
                expr: Box::new(expr),
                path,
            }
        }
        "json_array_length" | "json_length" => {
            ScalarFunction::JsonArrayLength(Box::new(get_arg(&args, 0, "JSON_ARRAY_LENGTH")?))
        }
        "json_keys" => ScalarFunction::JsonKeys(Box::new(get_arg(&args, 0, "JSON_KEYS")?)),
        "json_valid" | "is_json" => {
            ScalarFunction::JsonValid(Box::new(get_arg(&args, 0, "JSON_VALID")?))
        }
        "json_pretty" => ScalarFunction::JsonPretty(Box::new(get_arg(&args, 0, "JSON_PRETTY")?)),

        // Array functions
        "array" => ScalarFunction::Array(args),
        "array_length" | "cardinality" => {
            ScalarFunction::ArrayLength(Box::new(get_arg(&args, 0, "ARRAY_LENGTH")?))
        }
        "array_contains" | "array_has" => {
            let array = get_arg(&args, 0, "ARRAY_CONTAINS")?;
            let value = get_arg(&args, 1, "ARRAY_CONTAINS")?;
            ScalarFunction::ArrayContains {
                array: Box::new(array),
                value: Box::new(value),
            }
        }
        "array_element" | "element_at" => {
            let array = get_arg(&args, 0, "ARRAY_ELEMENT")?;
            let index = get_int_arg(&args, 1, "ARRAY_ELEMENT")?;
            ScalarFunction::ArrayElement {
                array: Box::new(array),
                index,
            }
        }
        "array_append" | "array_push" => {
            let array = get_arg(&args, 0, "ARRAY_APPEND")?;
            let element = get_arg(&args, 1, "ARRAY_APPEND")?;
            ScalarFunction::ArrayAppend {
                array: Box::new(array),
                element: Box::new(element),
            }
        }
        "array_concat" | "array_cat" => ScalarFunction::ArrayConcat(args),
        "array_distinct" | "array_unique" => {
            ScalarFunction::ArrayDistinct(Box::new(get_arg(&args, 0, "ARRAY_DISTINCT")?))
        }
        "array_join" | "array_to_string" => {
            let array = get_arg(&args, 0, "ARRAY_JOIN")?;
            let delimiter = if args.len() > 1 {
                get_string_arg(&args, 1, "ARRAY_JOIN")?
            } else {
                ",".to_string()
            };
            ScalarFunction::ArrayJoin {
                array: Box::new(array),
                delimiter,
            }
        }

        // ================================================================
        // Vector/AI functions for embedding search and similarity
        // ================================================================
        "vector_distance" | "vec_distance" => {
            let vec1 = get_arg(&args, 0, "VECTOR_DISTANCE")?;
            let vec2 = get_arg(&args, 1, "VECTOR_DISTANCE")?;
            let metric = if args.len() > 2 {
                get_string_arg(&args, 2, "VECTOR_DISTANCE")?
            } else {
                "cosine".to_string()
            };
            ScalarFunction::VectorDistance {
                vec1: Box::new(vec1),
                vec2: Box::new(vec2),
                metric,
            }
        }
        "vector_similarity" | "vec_similarity" | "cosine_similarity" | "similarity" => {
            // Convenience function that returns similarity (1 - distance for cosine)
            let vec1 = get_arg(&args, 0, "VECTOR_SIMILARITY")?;
            let vec2 = get_arg(&args, 1, "VECTOR_SIMILARITY")?;
            ScalarFunction::VectorSimilarity {
                vec1: Box::new(vec1),
                vec2: Box::new(vec2),
            }
        }
        "vector_dims" | "vec_dims" | "array_dims" => {
            ScalarFunction::VectorDims(Box::new(get_arg(&args, 0, "VECTOR_DIMS")?))
        }
        "vector_norm" | "vec_norm" | "l2_norm" => {
            ScalarFunction::VectorNorm(Box::new(get_arg(&args, 0, "VECTOR_NORM")?))
        }
        "vector_normalize" | "vec_normalize" | "l2_normalize" => {
            ScalarFunction::VectorNormalize(Box::new(get_arg(&args, 0, "VECTOR_NORMALIZE")?))
        }
        "inner_product" | "dot_product" => {
            let vec1 = get_arg(&args, 0, "INNER_PRODUCT")?;
            let vec2 = get_arg(&args, 1, "INNER_PRODUCT")?;
            ScalarFunction::InnerProduct {
                vec1: Box::new(vec1),
                vec2: Box::new(vec2),
            }
        }
        "euclidean_distance" | "l2_distance" => {
            let vec1 = get_arg(&args, 0, "EUCLIDEAN_DISTANCE")?;
            let vec2 = get_arg(&args, 1, "EUCLIDEAN_DISTANCE")?;
            ScalarFunction::EuclideanDistance {
                vec1: Box::new(vec1),
                vec2: Box::new(vec2),
            }
        }
        "manhattan_distance" | "l1_distance" => {
            let vec1 = get_arg(&args, 0, "MANHATTAN_DISTANCE")?;
            let vec2 = get_arg(&args, 1, "MANHATTAN_DISTANCE")?;
            ScalarFunction::ManhattanDistance {
                vec1: Box::new(vec1),
                vec2: Box::new(vec2),
            }
        }

        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported function: {func_name}"
            )));
        }
    };

    Ok(SelectExpr::Function(scalar_func))
}

/// Parse a window function
fn parse_window_function(
    func_name: &str,
    args: &[SelectExpr],
    func: &sqlparser::ast::Function,
) -> Result<SelectExpr, EngineError> {
    let window_func = match func_name {
        "row_number" => WindowFunction::RowNumber,
        "rank" => WindowFunction::Rank,
        "dense_rank" => WindowFunction::DenseRank,
        "percent_rank" => WindowFunction::PercentRank,
        "cume_dist" => WindowFunction::CumeDist,
        "ntile" => {
            let n = get_int_arg(args, 0, "NTILE")?;
            WindowFunction::NTile(n)
        }
        "lag" => {
            let expr = get_arg(args, 0, "LAG")?;
            let offset = if args.len() > 1 {
                get_int_arg(args, 1, "LAG")?
            } else {
                1
            };
            let default = if args.len() > 2 {
                Some(Box::new(args[2].clone()))
            } else {
                None
            };
            WindowFunction::Lag {
                expr: Box::new(expr),
                offset,
                default,
            }
        }
        "lead" => {
            let expr = get_arg(args, 0, "LEAD")?;
            let offset = if args.len() > 1 {
                get_int_arg(args, 1, "LEAD")?
            } else {
                1
            };
            let default = if args.len() > 2 {
                Some(Box::new(args[2].clone()))
            } else {
                None
            };
            WindowFunction::Lead {
                expr: Box::new(expr),
                offset,
                default,
            }
        }
        "first_value" => WindowFunction::FirstValue(Box::new(get_arg(args, 0, "FIRST_VALUE")?)),
        "last_value" => WindowFunction::LastValue(Box::new(get_arg(args, 0, "LAST_VALUE")?)),
        "nth_value" => {
            let expr = get_arg(args, 0, "NTH_VALUE")?;
            let n = get_int_arg(args, 1, "NTH_VALUE")?;
            WindowFunction::NthValue {
                expr: Box::new(expr),
                n,
            }
        }
        // Window versions of aggregate functions
        "sum" => WindowFunction::WindowSum(Box::new(get_arg(args, 0, "SUM")?)),
        "avg" => WindowFunction::WindowAvg(Box::new(get_arg(args, 0, "AVG")?)),
        "min" => WindowFunction::WindowMin(Box::new(get_arg(args, 0, "MIN")?)),
        "max" => WindowFunction::WindowMax(Box::new(get_arg(args, 0, "MAX")?)),
        "count" => {
            let arg = if args.is_empty() {
                None
            } else {
                Some(Box::new(args[0].clone()))
            };
            WindowFunction::WindowCount(arg)
        }
        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported window function: {func_name}"
            )));
        }
    };

    // Parse OVER clause
    let spec = if let Some(over) = &func.over {
        parse_window_spec(over)?
    } else {
        WindowSpec::default()
    };

    Ok(SelectExpr::Window {
        function: window_func,
        spec,
    })
}

/// Parse window specification from OVER clause
fn parse_window_spec(over: &sqlparser::ast::WindowType) -> Result<WindowSpec, EngineError> {
    match over {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            let partition_by = spec
                .partition_by
                .iter()
                .filter_map(|e| match e {
                    Expr::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();

            let order_by = spec
                .order_by
                .iter()
                .filter_map(|ob| {
                    if let Expr::Identifier(ident) = &ob.expr {
                        Some(OrderByClause {
                            column: ident.value.clone(),
                            ascending: ob.asc.unwrap_or(true),
                            nulls_first: ob.nulls_first,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            let frame = spec.window_frame.as_ref().map(|f| {
                let unit = match f.units {
                    sqlparser::ast::WindowFrameUnits::Rows => WindowFrameUnit::Rows,
                    sqlparser::ast::WindowFrameUnits::Range => WindowFrameUnit::Range,
                    sqlparser::ast::WindowFrameUnits::Groups => WindowFrameUnit::Groups,
                };
                let start = parse_window_frame_bound(&f.start_bound);
                let end = f.end_bound.as_ref().map(|b| parse_window_frame_bound(b));
                WindowFrame { unit, start, end }
            });

            Ok(WindowSpec {
                partition_by,
                order_by,
                frame,
            })
        }
        sqlparser::ast::WindowType::NamedWindow(_) => Err(EngineError::NotImplemented(
            "named windows not supported".into(),
        )),
    }
}

fn parse_window_frame_bound(bound: &sqlparser::ast::WindowFrameBound) -> WindowFrameBound {
    match bound {
        sqlparser::ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        sqlparser::ast::WindowFrameBound::Preceding(None) => WindowFrameBound::UnboundedPreceding,
        sqlparser::ast::WindowFrameBound::Preceding(Some(n)) => {
            WindowFrameBound::Preceding(expr_to_u64(n).unwrap_or(1))
        }
        sqlparser::ast::WindowFrameBound::Following(None) => WindowFrameBound::UnboundedFollowing,
        sqlparser::ast::WindowFrameBound::Following(Some(n)) => {
            WindowFrameBound::Following(expr_to_u64(n).unwrap_or(1))
        }
    }
}

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}

// Helper functions for argument extraction
fn get_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<SelectExpr, EngineError> {
    args.get(idx).cloned().ok_or_else(|| {
        EngineError::InvalidArgument(format!("{func_name} requires argument at position {idx}"))
    })
}

fn get_single_column_arg(args: &[SelectExpr], func_name: &str) -> Result<String, EngineError> {
    match args.first() {
        Some(SelectExpr::Column(col)) => Ok(col.clone()),
        Some(SelectExpr::QualifiedColumn { column, .. }) => Ok(column.clone()),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires a column argument"
        ))),
    }
}

fn get_int_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<i64, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::Integer(n))) => Ok(*n),
        Some(SelectExpr::Literal(LiteralValue::Float(f))) => Ok(*f as i64),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires integer argument at position {idx}"
        ))),
    }
}

fn get_float_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<f64, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::Float(f))) => Ok(*f),
        Some(SelectExpr::Literal(LiteralValue::Integer(n))) => Ok(*n as f64),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires numeric argument at position {idx}"
        ))),
    }
}

fn get_string_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<String, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::String(s))) => Ok(s.clone()),
        Some(SelectExpr::Column(c)) => Ok(c.clone()), // Allow column name as string
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires string argument at position {idx}"
        ))),
    }
}

/// Parse SELECT items with full expression support
pub fn parse_select_items_extended(items: &[SelectItem]) -> Result<Vec<SelectColumn>, EngineError> {
    let mut columns = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                columns.push(SelectColumn {
                    expr: SelectExpr::Column("*".to_string()),
                    alias: None,
                });
            }
            SelectItem::UnnamedExpr(expr) => {
                let parsed = parse_expr(expr)?;
                columns.push(SelectColumn {
                    expr: parsed,
                    alias: None,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let parsed = parse_expr(expr)?;
                columns.push(SelectColumn {
                    expr: parsed,
                    alias: Some(alias.value.clone()),
                });
            }
            SelectItem::QualifiedWildcard(name, _) => {
                columns.push(SelectColumn {
                    expr: SelectExpr::Column(format!("{}.*", name)),
                    alias: None,
                });
            }
        }
    }

    Ok(columns)
}

/// Parse CTEs from WITH clause
pub fn parse_ctes(with: &With) -> Result<Vec<CteDefinition>, EngineError> {
    let mut ctes = Vec::new();
    let is_recursive = with.recursive;

    for cte in &with.cte_tables {
        let columns = if cte.alias.columns.is_empty() {
            None
        } else {
            Some(cte.alias.columns.iter().map(|c| c.value.clone()).collect())
        };

        // Parse the CTE query
        let query = parse_query_from_sqlparser(&cte.query)?;

        // Convert the query back to SQL string for recursive execution
        let raw_sql = cte.query.to_string();

        ctes.push(CteDefinition {
            name: cte.alias.name.value.clone(),
            columns,
            query,
            recursive: is_recursive,
            raw_sql,
        });
    }

    Ok(ctes)
}

/// Extract the first SELECT from a SetExpr (handles nested set operations)
fn extract_first_select(set_expr: &SetExpr) -> Result<&Select, EngineError> {
    match set_expr {
        SetExpr::Select(s) => Ok(s.as_ref()),
        SetExpr::SetOperation { left, .. } => extract_first_select(left),
        _ => Err(EngineError::NotImplemented(
            "only SELECT queries supported in CTEs".into(),
        )),
    }
}

/// Parse a sqlparser Query into our ParsedQuery (internal helper)
fn parse_query_from_sqlparser(query: &Query) -> Result<ParsedQuery, EngineError> {
    // Get the main SELECT body, handling both SELECT and SET operations (UNION, etc.)
    let select = match &*query.body {
        SetExpr::Select(s) => s.as_ref(),
        SetExpr::SetOperation { left, .. } => {
            // For UNION/INTERSECT/EXCEPT, extract the left-most SELECT for metadata
            extract_first_select(left)?
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "only SELECT queries supported in CTEs".into(),
            ))
        }
    };

    // For CTEs, we use a simplified parsing - just get basic info
    let (database, table) = if select.from.is_empty() {
        ("default".to_string(), "dual".to_string())
    } else {
        match &select.from[0].relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                parse_table_name(&table_name)?
            }
            _ => ("default".to_string(), "unknown".to_string()),
        }
    };

    Ok(ParsedQuery {
        database: Some(database),
        table: Some(table),
        projection: None,
        filter: QueryFilter::default(),
        aggregation: None,
        order_by: None,
        distinct: false,
        distinct_on: None,
        joins: Vec::new(),
        computed_columns: Vec::new(),
        ctes: Vec::new(),
        sample: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("mydb".to_string()));
                assert_eq!(q.table, Some("mytable".to_string()));
                assert!(q.projection.is_none());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT col1, col2 FROM test WHERE tenant_id = 42 AND event_time >= 1000";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("default".to_string()));
                assert_eq!(q.table, Some("test".to_string()));
                assert_eq!(q.filter.tenant_id_eq, Some(42));
                assert_eq!(q.filter.event_time_ge, Some(1000));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_aggregation() {
        let sql = "SELECT COUNT(*), SUM(duration_ms) FROM analytics.events GROUP BY tenant_id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("analytics".to_string()));
                assert_eq!(q.table, Some("events".to_string()));
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.group_by, GroupBy::Tenant);
                assert_eq!(agg.aggs.len(), 2);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let sql = "DROP TABLE IF EXISTS mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DropTable {
                database,
                table,
                if_exists,
            }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "mytable");
                assert!(if_exists);
            }
            _ => panic!("expected drop table"),
        }
    }

    #[test]
    fn test_parse_drop_database() {
        let sql = "DROP DATABASE IF EXISTS analytics";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DropDatabase { name, if_exists }) => {
                assert_eq!(name, "analytics");
                assert!(if_exists);
            }
            _ => panic!("expected drop database"),
        }
    }

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE mydb.events ADD COLUMN latency int64";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::AlterTableAddColumn {
                database,
                table,
                column,
                data_type,
                nullable,
            }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "events");
                assert_eq!(column, "latency");
                assert_eq!(data_type, "int64");
                assert!(nullable);
            }
            _ => panic!("expected alter table add column"),
        }
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let sql = "ALTER TABLE events DROP COLUMN status";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::AlterTableDropColumn {
                database,
                table,
                column,
            }) => {
                assert_eq!(database, "default");
                assert_eq!(table, "events");
                assert_eq!(column, "status");
            }
            _ => panic!("expected alter table drop column"),
        }
    }

    #[test]
    fn test_parse_truncate_table() {
        let sql = "TRUNCATE TABLE mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::TruncateTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "mytable");
            }
            _ => panic!("expected truncate table"),
        }
    }

    #[test]
    fn test_parse_limit() {
        let sql = "SELECT * FROM test LIMIT 100";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.limit, Some(100));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_create_user() {
        let sql = "CREATE USER appuser WITH PASSWORD 'SecurePass123'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser {
                username,
                password,
                options,
            }) => {
                assert_eq!(username, "appuser");
                assert_eq!(password, "SecurePass123");
                assert!(!options.superuser);
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_create_user_password_text_does_not_grant_superuser() {
        let sql = "CREATE USER appuser WITH PASSWORD 'SUPERUSERpw'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser { options, .. }) => {
                assert!(!options.superuser);
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_parse_create_user_with_options() {
        let sql = "CREATE USER admin WITH PASSWORD 'AdminPass123' SUPERUSER DEFAULT DATABASE 'mydb' CONNECTION LIMIT 10";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser {
                username,
                password,
                options,
            }) => {
                assert_eq!(username, "admin");
                assert_eq!(password, "AdminPass123");
                assert!(options.superuser);
                assert_eq!(options.default_database, Some("mydb".to_string()));
                assert_eq!(options.connection_limit, Some(10));
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_parse_drop_user() {
        let sql = "DROP USER olduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::DropUser { username }) => {
                assert_eq!(username, "olduser");
            }
            _ => panic!("expected DropUser"),
        }
    }

    #[test]
    fn test_parse_alter_user_password() {
        let sql = "ALTER USER appuser SET PASSWORD 'NewSecurePass456'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::AlterUserPassword {
                username,
                new_password,
            }) => {
                assert_eq!(username, "appuser");
                assert_eq!(new_password, "NewSecurePass456");
            }
            _ => panic!("expected AlterUserPassword"),
        }
    }

    #[test]
    fn test_parse_grant_privilege() {
        let sql = "GRANT SELECT, INSERT ON DATABASE mydb TO appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::Grant {
                privileges,
                target_type,
                target_name,
                grantee,
                ..
            }) => {
                assert_eq!(privileges, vec!["SELECT", "INSERT"]);
                assert!(matches!(target_type, GrantTargetType::Database));
                assert_eq!(target_name, Some("mydb".to_string()));
                assert_eq!(grantee, "appuser");
            }
            _ => panic!("expected Grant"),
        }
    }

    #[test]
    fn test_parse_grant_role() {
        let sql = "GRANT readonly TO appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::GrantRole { role, username }) => {
                assert_eq!(role, "readonly");
                assert_eq!(username, "appuser");
            }
            _ => panic!("expected GrantRole"),
        }
    }

    #[test]
    fn test_parse_revoke_privilege() {
        let sql = "REVOKE INSERT ON DATABASE mydb FROM appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::Revoke {
                privileges,
                target_type,
                target_name,
                grantee,
                ..
            }) => {
                assert_eq!(privileges, vec!["INSERT"]);
                assert!(matches!(target_type, GrantTargetType::Database));
                assert_eq!(target_name, Some("mydb".to_string()));
                assert_eq!(grantee, "appuser");
            }
            _ => panic!("expected Revoke"),
        }
    }

    #[test]
    fn test_parse_show_users() {
        let sql = "SHOW USERS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::ShowUsers) => {}
            _ => panic!("expected ShowUsers"),
        }
    }

    #[test]
    fn test_parse_create_role() {
        let sql = "CREATE ROLE myrole WITH DESCRIPTION 'A custom role'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateRole { name, description }) => {
                assert_eq!(name, "myrole");
                assert_eq!(description, Some("A custom role".to_string()));
            }
            _ => panic!("expected CreateRole"),
        }
    }

    #[test]
    fn test_parse_lock_unlock_user() {
        let sql = "LOCK USER baduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::LockUser { username }) => {
                assert_eq!(username, "baduser");
            }
            _ => panic!("expected LockUser"),
        }

        let sql = "UNLOCK USER baduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::UnlockUser { username }) => {
                assert_eq!(username, "baduser");
            }
            _ => panic!("expected UnlockUser"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql =
            "INSERT INTO mydb.events (tenant_id, duration_ms, status) VALUES (1, 500, 'completed')";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "events");
                assert_eq!(
                    cmd.columns,
                    Some(vec![
                        "tenant_id".to_string(),
                        "duration_ms".to_string(),
                        "status".to_string()
                    ])
                );
                assert_eq!(cmd.values.len(), 1);
                assert_eq!(cmd.values[0].len(), 3);
                assert!(matches!(cmd.values[0][0], SqlValue::Integer(1)));
                assert!(matches!(cmd.values[0][1], SqlValue::Integer(500)));
                assert!(matches!(cmd.values[0][2], SqlValue::String(ref s) if s == "completed"));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let sql = "INSERT INTO events VALUES (1, 100), (2, 200), (3, 300)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "events");
                assert!(cmd.columns.is_none());
                assert_eq!(cmd.values.len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_negative_numbers() {
        let sql = "INSERT INTO test VALUES (-42, -3.14)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.values.len(), 1);
                assert!(matches!(cmd.values[0][0], SqlValue::Integer(-42)));
                assert!(matches!(cmd.values[0][1], SqlValue::Float(f) if (f + 3.14).abs() < 0.001));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_update() {
        let sql = "UPDATE mydb.users SET name = 'John', age = 30 WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "users");
                assert_eq!(cmd.assignments.len(), 2);
                assert_eq!(cmd.assignments[0].0, "name");
                assert!(matches!(&cmd.assignments[0].1, SqlValue::String(s) if s == "John"));
                assert_eq!(cmd.assignments[1].0, "age");
                assert!(matches!(cmd.assignments[1].1, SqlValue::Integer(30)));
                assert!(cmd.where_clause.is_some());
                assert!(cmd.where_clause.as_ref().unwrap().contains("id"));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_update_no_where() {
        let sql = "UPDATE users SET status = 'inactive'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "users");
                assert_eq!(cmd.assignments.len(), 1);
                assert!(cmd.where_clause.is_none());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let sql = "DELETE FROM mydb.users WHERE id = 5";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "users");
                assert!(cmd.where_clause.is_some());
                assert!(cmd.where_clause.as_ref().unwrap().contains("id"));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_no_where() {
        let sql = "DELETE FROM users";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "users");
                assert!(cmd.where_clause.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_inner_join() {
        let sql = "SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("default".to_string()));
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].join_type, JoinType::Inner);
                assert_eq!(q.joins[0].table, "customers");
                assert_eq!(q.joins[0].on_condition.left_column, "customer_id");
                assert_eq!(q.joins[0].on_condition.right_column, "id");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_left_join() {
        let sql = "SELECT * FROM mydb.orders LEFT JOIN mydb.customers ON orders.customer_id = customers.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("mydb".to_string()));
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].join_type, JoinType::Left);
                assert_eq!(q.joins[0].database, "mydb");
                assert_eq!(q.joins[0].table, "customers");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_join_with_alias() {
        let sql = "SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].alias, Some("c".to_string()));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_show_databases() {
        let sql = "SHOW DATABASES";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDatabases) => {}
            _ => panic!("expected ShowDatabases"),
        }

        // With semicolon
        let sql = "SHOW DATABASES;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDatabases) => {}
            _ => panic!("expected ShowDatabases"),
        }
    }

    #[test]
    fn test_parse_show_tables() {
        // Without database
        let sql = "SHOW TABLES";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert!(database.is_none());
            }
            _ => panic!("expected ShowTables"),
        }

        // With IN database
        let sql = "SHOW TABLES IN mydb";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert_eq!(database, Some("mydb".to_string()));
            }
            _ => panic!("expected ShowTables with database"),
        }

        // With FROM database (MySQL-style)
        let sql = "SHOW TABLES FROM testdb;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert_eq!(database, Some("testdb".to_string()));
            }
            _ => panic!("expected ShowTables with database"),
        }
    }

    #[test]
    fn test_parse_describe_table() {
        // DESCRIBE database.table
        let sql = "DESCRIBE mydb.users";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "users");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESC shorthand
        let sql = "DESC mydb.events;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "events");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESCRIBE TABLE database.table
        let sql = "DESCRIBE TABLE testdb.events";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "testdb");
                assert_eq!(table, "events");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESC TABLE with unqualified table name
        let sql = "DESC TABLE mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "default");
                assert_eq!(table, "mytable");
            }
            _ => panic!("expected DescribeTable with default database"),
        }
    }

    #[test]
    fn test_parse_insert_returning() {
        // INSERT with RETURNING *
        let sql = "INSERT INTO mydb.users (id, name) VALUES (1, 'Alice') RETURNING *";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "users");
                assert!(cmd.returning.is_some());
                assert_eq!(cmd.returning.as_ref().unwrap(), &["*".to_string()]);
            }
            _ => panic!("expected Insert"),
        }

        // INSERT with RETURNING specific columns
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING id, name";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert!(cmd.returning.is_some());
                let cols = cmd.returning.as_ref().unwrap();
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0], "id");
                assert_eq!(cols[1], "name");
            }
            _ => panic!("expected Insert"),
        }

        // INSERT without RETURNING
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert!(cmd.returning.is_none());
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_update_returning() {
        // UPDATE with RETURNING *
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING *";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert!(cmd.returning.is_some());
                assert_eq!(cmd.returning.as_ref().unwrap(), &["*".to_string()]);
            }
            _ => panic!("expected Update"),
        }

        // UPDATE with RETURNING specific columns
        let sql = "UPDATE mydb.users SET name = 'Bob' RETURNING id, name";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert!(cmd.returning.is_some());
                let cols = cmd.returning.as_ref().unwrap();
                assert_eq!(cols.len(), 2);
            }
            _ => panic!("expected Update"),
        }

        // UPDATE without RETURNING
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert!(cmd.returning.is_none());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete_returning() {
        // DELETE with RETURNING *
        let sql = "DELETE FROM users WHERE id = 1 RETURNING *";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert!(cmd.returning.is_some());
                assert_eq!(cmd.returning.as_ref().unwrap(), &["*".to_string()]);
            }
            _ => panic!("expected Delete"),
        }

        // DELETE with RETURNING specific columns
        let sql = "DELETE FROM mydb.users WHERE id = 1 RETURNING id, deleted_at";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert!(cmd.returning.is_some());
                let cols = cmd.returning.as_ref().unwrap();
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0], "id");
                assert_eq!(cols[1], "deleted_at");
            }
            _ => panic!("expected Delete"),
        }

        // DELETE without RETURNING
        let sql = "DELETE FROM users WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert!(cmd.returning.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_show_server_info() {
        let sql = "SHOW SERVER INFO";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowServerInfo) => {}
            _ => panic!("expected ShowServerInfo"),
        }
    }

    #[test]
    fn test_parse_show_missing_segments() {
        // Without FROM clause
        let sql = "SHOW MISSING SEGMENTS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowMissingSegments { database, table }) => {
                assert!(database.is_none());
                assert!(table.is_none());
            }
            _ => panic!("expected ShowMissingSegments"),
        }

        // With FROM clause
        let sql = "SHOW MISSING SEGMENTS FROM mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowMissingSegments { database, table }) => {
                assert_eq!(database, Some("mydb".to_string()));
                assert_eq!(table, Some("mytable".to_string()));
            }
            _ => panic!("expected ShowMissingSegments"),
        }
    }

    #[test]
    fn test_parse_repair_segments() {
        // Specific table
        let sql = "REPAIR SEGMENTS mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::RepairSegments { database, table }) => {
                assert_eq!(database, Some("mydb".to_string()));
                assert_eq!(table, Some("mytable".to_string()));
            }
            _ => panic!("expected RepairSegments"),
        }

        // All tables in database (wildcard)
        let sql = "REPAIR SEGMENTS mydb.*";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::RepairSegments { database, table }) => {
                assert_eq!(database, Some("mydb".to_string()));
                assert!(table.is_none());
            }
            _ => panic!("expected RepairSegments with wildcard"),
        }

        // All segments (REPAIR ALL SEGMENTS)
        let sql = "REPAIR ALL SEGMENTS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::RepairSegments { database, table }) => {
                assert!(database.is_none());
                assert!(table.is_none());
            }
            _ => panic!("expected RepairSegments for all"),
        }
    }

    #[test]
    fn test_parse_check_manifest() {
        let sql = "CHECK MANIFEST";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CheckManifest) => {}
            _ => panic!("expected CheckManifest"),
        }
    }

    #[test]
    fn test_parse_show_corrupted_segments() {
        // Without FROM clause
        let sql = "SHOW CORRUPTED SEGMENTS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowCorruptedSegments { database, table }) => {
                assert!(database.is_none());
                assert!(table.is_none());
            }
            _ => panic!("expected ShowCorruptedSegments"),
        }

        // With FROM clause
        let sql = "SHOW CORRUPTED SEGMENTS FROM mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowCorruptedSegments { database, table }) => {
                assert_eq!(database, Some("mydb".to_string()));
                assert_eq!(table, Some("mytable".to_string()));
            }
            _ => panic!("expected ShowCorruptedSegments"),
        }
    }

    #[test]
    fn test_parse_show_damaged_segments() {
        // Without FROM clause
        let sql = "SHOW DAMAGED SEGMENTS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDamagedSegments { database, table }) => {
                assert!(database.is_none());
                assert!(table.is_none());
            }
            _ => panic!("expected ShowDamagedSegments"),
        }

        // With FROM clause
        let sql = "SHOW DAMAGED SEGMENTS FROM mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDamagedSegments { database, table }) => {
                assert_eq!(database, Some("mydb".to_string()));
                assert_eq!(table, Some("mytable".to_string()));
            }
            _ => panic!("expected ShowDamagedSegments"),
        }
    }

    #[test]
    fn test_parse_create_index_using_fulltext() {
        // PostgreSQL syntax: USING comes BEFORE columns
        let sql =
            "CREATE INDEX idx_phone_ft ON airtel_cdr.voice_cdr USING FULLTEXT (calling_number)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CreateIndex {
                database,
                table,
                index_name,
                columns,
                index_type,
                if_not_exists,
            }) => {
                assert_eq!(database, "airtel_cdr");
                assert_eq!(table, "voice_cdr");
                assert_eq!(index_name, "idx_phone_ft");
                assert_eq!(columns, vec!["calling_number".to_string()]);
                assert_eq!(index_type, IndexType::Fulltext);
                assert!(!if_not_exists);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_create_index_using_btree() {
        // PostgreSQL syntax: USING comes BEFORE columns
        let sql = "CREATE INDEX idx_name ON mydb.users USING BTREE (name)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CreateIndex { index_type, .. }) => {
                assert_eq!(index_type, IndexType::BTree);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_create_index_default_btree() {
        // No USING clause defaults to BTree
        let sql = "CREATE INDEX idx_id ON mydb.users (id)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CreateIndex { index_type, .. }) => {
                assert_eq!(index_type, IndexType::BTree);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_create_index_if_not_exists() {
        // PostgreSQL syntax: USING comes BEFORE columns
        let sql = "CREATE INDEX IF NOT EXISTS idx_phone ON telecom.cdr USING FULLTEXT (phone)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::CreateIndex {
                if_not_exists,
                index_type,
                ..
            }) => {
                assert!(if_not_exists);
                assert_eq!(index_type, IndexType::Fulltext);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_update_with_from() {
        // UPDATE with FROM clause (PostgreSQL style)
        let sql = "UPDATE orders o SET status = 'shipped' FROM customers c WHERE o.customer_id = c.id AND c.premium = true";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "orders");
                assert_eq!(cmd.alias, Some("o".to_string()));
                assert!(cmd.from_clause.is_some());
                let from_tables = cmd.from_clause.unwrap();
                assert_eq!(from_tables.len(), 1);
                assert_eq!(from_tables[0].table, "customers");
                assert_eq!(from_tables[0].alias, Some("c".to_string()));
                assert!(cmd.where_clause.is_some());
                assert!(cmd.assignments.len() == 1);
                assert_eq!(cmd.assignments[0].0, "status");
            }
            _ => panic!("expected Update"),
        }

        // Simple UPDATE without FROM
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert!(cmd.from_clause.is_none());
                assert!(cmd.alias.is_none());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete_with_using() {
        // DELETE with USING clause (PostgreSQL style)
        let sql = "DELETE FROM orders o USING customers c WHERE o.customer_id = c.id AND c.status = 'inactive'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "orders");
                assert_eq!(cmd.alias, Some("o".to_string()));
                assert!(cmd.using_clause.is_some());
                let using_tables = cmd.using_clause.unwrap();
                assert_eq!(using_tables.len(), 1);
                assert_eq!(using_tables[0].table, "customers");
                assert_eq!(using_tables[0].alias, Some("c".to_string()));
                assert!(cmd.where_clause.is_some());
            }
            _ => panic!("expected Delete"),
        }

        // Simple DELETE without USING
        let sql = "DELETE FROM users WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert!(cmd.using_clause.is_none());
                assert!(cmd.alias.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }
}
