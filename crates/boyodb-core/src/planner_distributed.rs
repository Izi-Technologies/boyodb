//! Phase 18: Distributed Query Execution - Planning Module
//!
//! This module provides distributed query planning with support for:
//! - Scatter-gather execution for simple queries
//! - Distributed aggregations (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)
//! - ORDER BY with LIMIT pushdown
//! - Distributed JOINs (broadcast and hash partition)
//! - Set operations (UNION, INTERSECT, EXCEPT)
//! - Subquery distribution
//! - Cost-based distribution decisions

use crate::engine::{PlanKind, AggPlan, AggKind, QueryFilter, GroupBy};
use crate::sql::{JoinType, SelectColumn};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

// ============================================================================
// Core Plan Types
// ============================================================================

/// Execution hint for local plan execution strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionHint {
    /// Standard scatter-gather execution
    Scatter,
    /// Broadcast a smaller table to all nodes
    Broadcast { broadcast_table: String },
    /// Hash partition both sides by join key
    HashPartition { partition_key: String },
    /// Local execution only (for subqueries, CTEs)
    Local,
}

impl Default for ExecutionHint {
    fn default() -> Self {
        ExecutionHint::Scatter
    }
}

/// Extended local plan with execution strategy hints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalPlan {
    pub kind: PlanKind,
    pub shard_ids: Vec<u16>,
    #[serde(default)]
    pub execution_hint: ExecutionHint,
    /// ORDER BY columns to apply locally (for LIMIT pushdown)
    #[serde(default)]
    pub order_by: Vec<OrderBySpec>,
    /// Local LIMIT (pushed down for efficiency)
    #[serde(default)]
    pub local_limit: Option<usize>,
}

impl LocalPlan {
    pub fn new(kind: PlanKind) -> Self {
        Self {
            kind,
            shard_ids: vec![],
            execution_hint: ExecutionHint::default(),
            order_by: vec![],
            local_limit: None,
        }
    }

    pub fn with_shards(mut self, shard_ids: Vec<u16>) -> Self {
        self.shard_ids = shard_ids;
        self
    }

    pub fn with_hint(mut self, hint: ExecutionHint) -> Self {
        self.execution_hint = hint;
        self
    }

    pub fn with_order_limit(mut self, order_by: Vec<OrderBySpec>, limit: Option<usize>) -> Self {
        self.order_by = order_by;
        self.local_limit = limit;
        self
    }

    /// Get database name from plan
    pub fn database(&self) -> Option<&str> {
        match &self.kind {
            PlanKind::Simple { db, .. } => db.as_deref(),
            _ => None,
        }
    }

    /// Get table name from plan
    pub fn table(&self) -> Option<&str> {
        match &self.kind {
            PlanKind::Simple { table, .. } => table.as_deref(),
            _ => None,
        }
    }
}

/// ORDER BY specification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderBySpec {
    pub column: String,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Enhanced global plan with merge strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalPlan {
    pub kind: PlanKind,
    #[serde(default)]
    pub merge_strategy: MergeStrategy,
}

impl GlobalPlan {
    pub fn new(kind: PlanKind) -> Self {
        Self {
            kind,
            merge_strategy: MergeStrategy::default(),
        }
    }

    pub fn with_merge_strategy(mut self, strategy: MergeStrategy) -> Self {
        self.merge_strategy = strategy;
        self
    }
}

// ============================================================================
// Merge Strategies
// ============================================================================

/// Strategy for merging results from distributed execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MergeStrategy {
    /// Simple concatenation of results (default for non-aggregated queries)
    Concatenate,
    /// Merge-sort for ORDER BY queries with optional LIMIT
    MergeSort {
        order_by: Vec<OrderBySpec>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    /// Final aggregation for distributed aggregates
    FinalAggregate {
        partial_aggs: Vec<PartialAggSpec>,
        group_by: GroupBy,
    },
    /// Set operation merge (UNION, INTERSECT, EXCEPT)
    SetOperation { op: SetOpMerge },
    /// Join result merge
    JoinMerge { join_type: JoinType },
}

impl Default for MergeStrategy {
    fn default() -> Self {
        MergeStrategy::Concatenate
    }
}

/// Specification for partial aggregation that needs final computation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartialAggSpec {
    /// Source columns from partial aggregation results
    pub source_columns: Vec<String>,
    /// Type of final aggregation to perform
    pub final_agg: FinalAggKind,
    /// Output column name
    pub output_column: String,
}

/// Final aggregation kinds for distributed aggregates
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FinalAggKind {
    /// AVG = sum(partial_sums) / sum(partial_counts)
    AvgFromSumCount,
    /// STDDEV using Welford's parallel algorithm
    StddevFromPartials { population: bool },
    /// VARIANCE using Welford's parallel algorithm
    VarianceFromPartials { population: bool },
    /// Sum of partial sums
    SumOfSums,
    /// Max of partial maxes
    MaxOfMaxes,
    /// Min of partial mins
    MinOfMins,
    /// Sum of partial counts
    CountOfCounts,
    /// HyperLogLog union for approximate count distinct
    HllUnion,
}

/// Set operation type for distributed execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SetOpMerge {
    Union { all: bool },
    Intersect { all: bool },
    Except { all: bool },
}

// ============================================================================
// Distributed JOIN Types
// ============================================================================

/// Which side of the join to broadcast
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

/// Strategy for executing distributed joins
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JoinStrategy {
    /// Broadcast smaller table to all nodes
    Broadcast {
        broadcast_side: JoinSide,
        estimated_broadcast_bytes: u64,
    },
    /// Hash partition both sides by join key
    HashPartition {
        partition_key: String,
        parallelism: usize,
    },
    /// Tables already co-located by same partition key
    Colocated,
    /// Local nested loop (fallback for small tables)
    LocalNestedLoop,
}

/// Distributed join plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedJoinPlan {
    pub left: Box<DistributedPlan>,
    pub right: Box<DistributedPlan>,
    pub join_type: JoinType,
    pub join_strategy: JoinStrategy,
    pub join_keys: Vec<(String, String)>, // (left_col, right_col) pairs
}

/// Distributed set operation plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSetOpPlan {
    pub op: SetOpMerge,
    pub left: Box<DistributedPlan>,
    pub right: Box<DistributedPlan>,
}

/// Distributed subquery plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSubqueryPlan {
    /// The outer query plan
    pub outer: Box<DistributedPlan>,
    /// Materialized subqueries (column -> subquery plan)
    pub subqueries: Vec<MaterializedSubquery>,
}

/// Materialized subquery specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedSubquery {
    /// Column the subquery filters on
    pub column: String,
    /// The subquery plan to execute first
    pub plan: Box<DistributedPlan>,
    /// Whether this is a NOT IN / NOT EXISTS
    pub negated: bool,
    /// Subquery type
    pub subquery_type: SubqueryType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubqueryType {
    /// IN (subquery)
    InList,
    /// EXISTS (subquery)
    Exists,
    /// Scalar subquery (single value)
    Scalar,
}

/// Unified distributed plan wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistributedPlan {
    /// Simple scatter-gather plan
    Simple(LocalPlan, GlobalPlan),
    /// Distributed join
    Join(DistributedJoinPlan),
    /// Distributed set operation
    SetOperation(DistributedSetOpPlan),
    /// Query with materialized subqueries
    Subquery(DistributedSubqueryPlan),
    /// Execute locally only (no distribution)
    Local(PlanKind),
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Cost estimation for distribution decisions
#[derive(Debug, Clone, Default)]
pub struct DistributionCost {
    /// Network bytes to be transferred
    pub network_bytes: u64,
    /// Estimated CPU cost (arbitrary units)
    pub cpu_cost: f64,
    /// Memory required in bytes
    pub memory_bytes: u64,
    /// Estimated output rows
    pub output_rows: u64,
    /// Parallelism factor
    pub parallelism: usize,
}

impl DistributionCost {
    /// Total cost score (lower is better)
    pub fn total_cost(&self) -> f64 {
        // Weight network cost heavily
        let network_cost = self.network_bytes as f64 * 0.001; // 1ms per KB
        let cpu = self.cpu_cost;
        let memory_penalty = if self.memory_bytes > 1_000_000_000 { 100.0 } else { 0.0 };

        network_cost + cpu + memory_penalty
    }
}

/// Statistics for cost-based distribution optimization
#[derive(Debug, Clone, Default)]
pub struct DistributedStats {
    /// Table statistics by fully qualified name (db.table)
    pub table_stats: HashMap<String, TableStatsSnapshot>,
    /// Number of nodes in cluster
    pub node_count: usize,
    /// Average network bandwidth in MB/s
    pub avg_network_bandwidth_mbps: f64,
    /// Threshold for broadcast joins (bytes)
    pub broadcast_threshold_bytes: u64,
}

impl DistributedStats {
    pub fn new(node_count: usize) -> Self {
        Self {
            table_stats: HashMap::new(),
            node_count,
            avg_network_bandwidth_mbps: 100.0, // 100 MB/s default
            broadcast_threshold_bytes: 100 * 1024 * 1024, // 100MB default
        }
    }
}

/// Snapshot of table statistics for planning
#[derive(Debug, Clone, Default)]
pub struct TableStatsSnapshot {
    pub row_count: u64,
    pub size_bytes: u64,
    pub partition_key: Option<String>,
}

/// Distribution decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistributionDecision {
    /// Distribute across all nodes
    Distribute,
    /// Execute locally only
    Local,
    /// Target specific shards
    TargetShards(Vec<u16>),
}

// ============================================================================
// Distributed EXPLAIN Types
// ============================================================================

/// Distributed explain plan for showing query distribution strategy
#[derive(Debug, Clone, Serialize)]
pub struct DistributedExplainPlan {
    pub distribution_strategy: String,
    pub target_shards: Vec<u16>,
    pub target_nodes: Vec<String>,
    pub local_plan_description: String,
    pub global_plan_description: String,
    pub merge_strategy: String,
    pub estimated_costs: DistributedExplainCosts,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DistributedExplainCosts {
    pub network_bytes: u64,
    pub estimated_rows_per_node: u64,
    pub parallelism: usize,
}

// ============================================================================
// Main Distribution Functions
// ============================================================================

/// Splits a logical plan into a Scatter (Local) plan and a Gather (Global) plan.
///
/// Returns `None` if the plan cannot be distributed (e.g., must be executed locally on coordinator).
pub fn distribute_plan(kind: &PlanKind) -> Option<(LocalPlan, GlobalPlan)> {
    distribute_plan_with_options(kind, None, None)
}

/// Distribute plan with optional ORDER BY and LIMIT pushdown
pub fn distribute_plan_with_options(
    kind: &PlanKind,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Option<(LocalPlan, GlobalPlan)> {
    match kind {
        PlanKind::Simple { agg, db, table, projection, filter, computed_columns } => {
            distribute_simple_plan(
                agg.as_ref(),
                db,
                table,
                projection,
                filter,
                computed_columns,
                limit,
                offset,
            )
        }
        PlanKind::Join => {
            // Join distribution handled separately
            None
        }
        PlanKind::SetOperation => {
            // Set operation distribution handled separately
            None
        }
        PlanKind::Subquery => {
            // Subquery distribution handled separately
            None
        }
        PlanKind::Cte => {
            // CTE execution currently local only
            None
        }
        PlanKind::Transaction(_) => {
            // Transactions are local
            None
        }
    }
}

/// Distribute a simple (non-join) plan
fn distribute_simple_plan(
    agg: Option<&AggPlan>,
    db: &Option<String>,
    table: &Option<String>,
    projection: &Option<Vec<String>>,
    filter: &QueryFilter,
    computed_columns: &[SelectColumn],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Option<(LocalPlan, GlobalPlan)> {
    if let Some(agg_plan) = agg {
        // Distribute aggregation query
        distribute_aggregation(
            agg_plan, db, table, projection, filter, computed_columns,
            limit, offset,
        )
    } else {
        // Non-aggregated query - simple scatter-gather
        let local_kind = PlanKind::Simple {
            agg: None,
            db: db.clone(),
            table: table.clone(),
            projection: projection.clone(),
            filter: filter.clone(),
            computed_columns: computed_columns.to_vec(),
        };

        // Local plan gets LIMIT for pushdown (limit + offset)
        let local_limit = limit.map(|l| l + offset.unwrap_or(0));
        let local_plan = LocalPlan::new(local_kind.clone())
            .with_order_limit(vec![], local_limit);

        // Global plan merges results
        let merge_strategy = if limit.is_some() {
            MergeStrategy::MergeSort {
                order_by: vec![],
                limit,
                offset,
            }
        } else {
            MergeStrategy::Concatenate
        };

        let global_plan = GlobalPlan::new(local_kind)
            .with_merge_strategy(merge_strategy);

        Some((local_plan, global_plan))
    }
}

/// Distribute an aggregation query with proper partial/final aggregation
fn distribute_aggregation(
    agg_plan: &AggPlan,
    db: &Option<String>,
    table: &Option<String>,
    projection: &Option<Vec<String>>,
    filter: &QueryFilter,
    computed_columns: &[SelectColumn],
    _limit: Option<usize>,
    _offset: Option<usize>,
) -> Option<(LocalPlan, GlobalPlan)> {
    let mut local_aggs = Vec::new();
    let mut partial_agg_specs = Vec::new();
    let mut global_aggs = Vec::new();

    for agg_kind in &agg_plan.aggs {
        match agg_kind {
            AggKind::CountStar => {
                // Local: COUNT(*) -> produces "count" column
                local_aggs.push(AggKind::CountStar);
                // Global: SUM(count) -> final count
                global_aggs.push(AggKind::Sum { column: "count".into() });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec!["count".into()],
                    final_agg: FinalAggKind::CountOfCounts,
                    output_column: "count".into(),
                });
            }
            AggKind::CountDistinct { column } => {
                // For COUNT(DISTINCT), we need to collect all values and deduplicate
                // This is expensive - for now, fall back to approximate
                local_aggs.push(AggKind::ApproxCountDistinct { column: column.clone() });
                global_aggs.push(AggKind::Sum { column: format!("approx_count_distinct_{}", column) });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("approx_count_distinct_{}", column)],
                    final_agg: FinalAggKind::HllUnion,
                    output_column: format!("count_distinct_{}", column),
                });
            }
            AggKind::Sum { column } => {
                // Local: SUM(col) -> produces "sum_{col}" column
                local_aggs.push(AggKind::Sum { column: column.clone() });
                // Global: SUM(sum_{col}) -> final sum
                global_aggs.push(AggKind::Sum { column: format!("sum_{}", column) });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("sum_{}", column)],
                    final_agg: FinalAggKind::SumOfSums,
                    output_column: format!("sum_{}", column),
                });
            }
            AggKind::Avg { column } => {
                // AVG requires both SUM and COUNT for proper distributed computation
                // Local: SUM(col) and COUNT(*)
                local_aggs.push(AggKind::Sum { column: column.clone() });
                local_aggs.push(AggKind::CountStar);
                // Global: AVG = SUM(sums) / SUM(counts)
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("sum_{}", column), "count".into()],
                    final_agg: FinalAggKind::AvgFromSumCount,
                    output_column: format!("avg_{}", column),
                });
            }
            AggKind::Min { column } => {
                // Local: MIN(col) -> produces "min_{col}"
                local_aggs.push(AggKind::Min { column: column.clone() });
                // Global: MIN(min_{col}) -> final min
                global_aggs.push(AggKind::Min { column: format!("min_{}", column) });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("min_{}", column)],
                    final_agg: FinalAggKind::MinOfMins,
                    output_column: format!("min_{}", column),
                });
            }
            AggKind::Max { column } => {
                // Local: MAX(col) -> produces "max_{col}"
                local_aggs.push(AggKind::Max { column: column.clone() });
                // Global: MAX(max_{col}) -> final max
                global_aggs.push(AggKind::Max { column: format!("max_{}", column) });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("max_{}", column)],
                    final_agg: FinalAggKind::MaxOfMaxes,
                    output_column: format!("max_{}", column),
                });
            }
            AggKind::StddevSamp { column } | AggKind::StddevPop { column } => {
                let is_pop = matches!(agg_kind, AggKind::StddevPop { .. });
                // Welford's parallel algorithm requires: count, sum, m2 (sum of squared differences)
                // Local: compute partial stats
                local_aggs.push(AggKind::Sum { column: column.clone() });
                local_aggs.push(AggKind::CountStar);
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![
                        format!("sum_{}", column),
                        "count".into(),
                        format!("sum_sq_{}", column), // sum of squares
                    ],
                    final_agg: FinalAggKind::StddevFromPartials { population: is_pop },
                    output_column: if is_pop {
                        format!("stddev_pop_{}", column)
                    } else {
                        format!("stddev_samp_{}", column)
                    },
                });
            }
            AggKind::VarianceSamp { column } | AggKind::VariancePop { column } => {
                let is_pop = matches!(agg_kind, AggKind::VariancePop { .. });
                local_aggs.push(AggKind::Sum { column: column.clone() });
                local_aggs.push(AggKind::CountStar);
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![
                        format!("sum_{}", column),
                        "count".into(),
                        format!("sum_sq_{}", column),
                    ],
                    final_agg: FinalAggKind::VarianceFromPartials { population: is_pop },
                    output_column: if is_pop {
                        format!("var_pop_{}", column)
                    } else {
                        format!("var_samp_{}", column)
                    },
                });
            }
            AggKind::ApproxCountDistinct { column } => {
                // HyperLogLog can be unioned across nodes
                local_aggs.push(AggKind::ApproxCountDistinct { column: column.clone() });
                partial_agg_specs.push(PartialAggSpec {
                    source_columns: vec![format!("approx_count_distinct_{}", column)],
                    final_agg: FinalAggKind::HllUnion,
                    output_column: format!("approx_count_distinct_{}", column),
                });
            }
        }
    }

    // Deduplicate local aggregations
    local_aggs.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    local_aggs.dedup_by(|a, b| format!("{:?}", a) == format!("{:?}", b));

    // Build local plan
    let local_agg_plan = AggPlan {
        group_by: agg_plan.group_by.clone(),
        aggs: local_aggs,
        having: vec![], // HAVING applies after global aggregation
    };

    let local_kind = PlanKind::Simple {
        agg: Some(local_agg_plan),
        db: db.clone(),
        table: table.clone(),
        projection: projection.clone(),
        filter: filter.clone(),
        computed_columns: computed_columns.to_vec(),
    };

    let local_plan = LocalPlan::new(local_kind);

    // Build global plan
    let global_agg_plan = AggPlan {
        group_by: agg_plan.group_by.clone(),
        aggs: global_aggs,
        having: agg_plan.having.clone(),
    };

    let global_kind = PlanKind::Simple {
        agg: Some(global_agg_plan),
        db: db.clone(),
        table: table.clone(),
        projection: projection.clone(),
        filter: QueryFilter::default(), // Filter already applied locally
        computed_columns: computed_columns.to_vec(),
    };

    // Determine merge strategy
    let merge_strategy = MergeStrategy::FinalAggregate {
        partial_aggs: partial_agg_specs,
        group_by: agg_plan.group_by.clone(),
    };

    let global_plan = GlobalPlan::new(global_kind)
        .with_merge_strategy(merge_strategy);

    Some((local_plan, global_plan))
}

// ============================================================================
// JOIN Distribution
// ============================================================================

/// Create a distributed join plan
pub fn distribute_join(
    left_plan: DistributedPlan,
    right_plan: DistributedPlan,
    join_type: JoinType,
    join_keys: Vec<(String, String)>,
    stats: Option<&DistributedStats>,
) -> DistributedJoinPlan {
    let strategy = select_join_strategy(&left_plan, &right_plan, &join_keys, stats);

    DistributedJoinPlan {
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        join_type,
        join_strategy: strategy,
        join_keys,
    }
}

/// Select optimal join strategy based on statistics
fn select_join_strategy(
    left: &DistributedPlan,
    right: &DistributedPlan,
    join_keys: &[(String, String)],
    stats: Option<&DistributedStats>,
) -> JoinStrategy {
    let stats = match stats {
        Some(s) => s,
        None => {
            // No stats - default to broadcast if we have to guess
            return JoinStrategy::Broadcast {
                broadcast_side: JoinSide::Right,
                estimated_broadcast_bytes: 0,
            };
        }
    };

    // Get table sizes
    let left_bytes = estimate_plan_bytes(left, stats);
    let right_bytes = estimate_plan_bytes(right, stats);

    // Check for co-location
    if is_colocated(left, right, join_keys, stats) {
        return JoinStrategy::Colocated;
    }

    // Broadcast threshold check
    let threshold = stats.broadcast_threshold_bytes;

    if right_bytes < threshold && right_bytes < left_bytes {
        return JoinStrategy::Broadcast {
            broadcast_side: JoinSide::Right,
            estimated_broadcast_bytes: right_bytes,
        };
    }

    if left_bytes < threshold && left_bytes < right_bytes {
        return JoinStrategy::Broadcast {
            broadcast_side: JoinSide::Left,
            estimated_broadcast_bytes: left_bytes,
        };
    }

    // Default to hash partition
    let partition_key = join_keys
        .first()
        .map(|(l, _)| l.clone())
        .unwrap_or_else(|| "id".to_string());

    JoinStrategy::HashPartition {
        partition_key,
        parallelism: stats.node_count,
    }
}

/// Check if two plans are co-located by partition key
fn is_colocated(
    left: &DistributedPlan,
    right: &DistributedPlan,
    join_keys: &[(String, String)],
    stats: &DistributedStats,
) -> bool {
    // Get partition keys for both sides
    let left_partition = get_partition_key(left, stats);
    let right_partition = get_partition_key(right, stats);

    match (left_partition, right_partition) {
        (Some(lp), Some(rp)) => {
            // Check if join keys match partition keys
            join_keys.iter().any(|(lk, rk)| lk == &lp && rk == &rp)
        }
        _ => false,
    }
}

fn get_partition_key(plan: &DistributedPlan, stats: &DistributedStats) -> Option<String> {
    match plan {
        DistributedPlan::Simple(local, _) => {
            let table_key = format!(
                "{}.{}",
                local.database().unwrap_or("default"),
                local.table().unwrap_or("")
            );
            stats.table_stats
                .get(&table_key)
                .and_then(|s| s.partition_key.clone())
        }
        _ => None,
    }
}

fn estimate_plan_bytes(plan: &DistributedPlan, stats: &DistributedStats) -> u64 {
    match plan {
        DistributedPlan::Simple(local, _) => {
            let table_key = format!(
                "{}.{}",
                local.database().unwrap_or("default"),
                local.table().unwrap_or("")
            );
            stats.table_stats
                .get(&table_key)
                .map(|s| s.size_bytes)
                .unwrap_or(100 * 1024 * 1024) // Default 100MB if unknown
        }
        _ => 100 * 1024 * 1024, // Default for complex plans
    }
}

// ============================================================================
// Set Operation Distribution
// ============================================================================

/// Create a distributed set operation plan
pub fn distribute_set_operation(
    op: SetOpMerge,
    left: DistributedPlan,
    right: DistributedPlan,
) -> DistributedSetOpPlan {
    DistributedSetOpPlan {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// ============================================================================
// Subquery Distribution
// ============================================================================

/// Create a distributed subquery plan
pub fn distribute_with_subqueries(
    outer: DistributedPlan,
    subqueries: Vec<MaterializedSubquery>,
) -> DistributedSubqueryPlan {
    DistributedSubqueryPlan {
        outer: Box::new(outer),
        subqueries,
    }
}

// ============================================================================
// Cost-Based Distribution Decision
// ============================================================================

/// Decide whether to distribute a query or execute locally
pub fn should_distribute(
    kind: &PlanKind,
    stats: Option<&DistributedStats>,
) -> DistributionDecision {
    let stats = match stats {
        Some(s) if s.node_count > 1 => s,
        _ => return DistributionDecision::Local, // Single node or no stats
    };

    match kind {
        PlanKind::Simple { table, filter, .. } => {
            let table_name = table.as_deref().unwrap_or("");
            let table_key = format!("default.{}", table_name);

            if let Some(table_stats) = stats.table_stats.get(&table_key) {
                // Small table: local execution is cheaper
                if table_stats.size_bytes < 10 * 1024 * 1024 {
                    return DistributionDecision::Local;
                }

                // High selectivity + medium table: might be cheaper locally
                let selectivity = estimate_filter_selectivity(filter, table_stats);
                if selectivity < 0.01 && table_stats.size_bytes < 100 * 1024 * 1024 {
                    return DistributionDecision::Local;
                }

                // Check for tenant/shard targeting
                if filter.tenant_id_eq.is_some() {
                    // Can target specific shard
                    // The executor will determine the actual shard
                    return DistributionDecision::Distribute;
                }

                DistributionDecision::Distribute
            } else {
                // No stats: default to distribute for safety
                DistributionDecision::Distribute
            }
        }
        PlanKind::Join => DistributionDecision::Distribute,
        PlanKind::SetOperation => DistributionDecision::Distribute,
        _ => DistributionDecision::Local,
    }
}

/// Estimate filter selectivity (0.0 to 1.0)
fn estimate_filter_selectivity(filter: &QueryFilter, _stats: &TableStatsSnapshot) -> f64 {
    let mut selectivity = 1.0;

    // Point lookups are highly selective
    if filter.tenant_id_eq.is_some() {
        selectivity *= 0.001;
    }
    if filter.route_id_eq.is_some() {
        selectivity *= 0.01;
    }

    // IN lists
    if let Some(ref ids) = filter.tenant_id_in {
        selectivity *= (ids.len() as f64 * 0.001).min(0.1);
    }

    selectivity
}

// ============================================================================
// Distributed EXPLAIN
// ============================================================================

/// Generate distributed explain plan
pub fn explain_distributed(
    kind: &PlanKind,
    target_nodes: &[String],
    target_shards: &[u16],
) -> DistributedExplainPlan {
    let (local, global) = distribute_plan(kind)
        .unwrap_or_else(|| {
            (LocalPlan::new(kind.clone()), GlobalPlan::new(kind.clone()))
        });

    let distribution_strategy = match &local.execution_hint {
        ExecutionHint::Scatter => "Scatter-Gather".to_string(),
        ExecutionHint::Broadcast { broadcast_table } => {
            format!("Broadcast ({})", broadcast_table)
        }
        ExecutionHint::HashPartition { partition_key } => {
            format!("Hash Partition ({})", partition_key)
        }
        ExecutionHint::Local => "Local Only".to_string(),
    };

    let merge_strategy = match &global.merge_strategy {
        MergeStrategy::Concatenate => "Concatenate".to_string(),
        MergeStrategy::MergeSort { order_by, limit, .. } => {
            let cols: Vec<_> = order_by.iter().map(|o| o.column.as_str()).collect();
            format!("Merge-Sort ({}) LIMIT {:?}", cols.join(", "), limit)
        }
        MergeStrategy::FinalAggregate { partial_aggs, .. } => {
            let aggs: Vec<_> = partial_aggs.iter()
                .map(|a| format!("{:?}", a.final_agg))
                .collect();
            format!("Final Aggregate ({})", aggs.join(", "))
        }
        MergeStrategy::SetOperation { op } => format!("Set Op ({:?})", op),
        MergeStrategy::JoinMerge { join_type } => format!("Join Merge ({:?})", join_type),
    };

    DistributedExplainPlan {
        distribution_strategy,
        target_shards: target_shards.to_vec(),
        target_nodes: target_nodes.to_vec(),
        local_plan_description: format!("{:?}", local.kind),
        global_plan_description: format!("{:?}", global.kind),
        merge_strategy,
        estimated_costs: DistributedExplainCosts {
            network_bytes: target_nodes.len() as u64 * 1024, // Rough estimate
            estimated_rows_per_node: 1000,
            parallelism: target_nodes.len(),
        },
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_plan() -> PlanKind {
        PlanKind::Simple {
            agg: None,
            db: Some("default".to_string()),
            table: Some("events".to_string()),
            projection: Some(vec!["*".to_string()]),
            filter: QueryFilter::default(),
            computed_columns: vec![],
        }
    }

    fn make_agg_plan(aggs: Vec<AggKind>) -> PlanKind {
        PlanKind::Simple {
            agg: Some(AggPlan {
                group_by: GroupBy::None,
                aggs,
                having: vec![],
            }),
            db: Some("default".to_string()),
            table: Some("events".to_string()),
            projection: Some(vec!["*".to_string()]),
            filter: QueryFilter::default(),
            computed_columns: vec![],
        }
    }

    #[test]
    fn test_distribute_simple_plan() {
        let plan = make_simple_plan();
        let result = distribute_plan(&plan);
        assert!(result.is_some());

        let (local, global) = result.unwrap();
        assert!(local.shard_ids.is_empty());
        assert_eq!(global.merge_strategy, MergeStrategy::Concatenate);
    }

    #[test]
    fn test_distribute_count_star() {
        let plan = make_agg_plan(vec![AggKind::CountStar]);
        let result = distribute_plan(&plan);
        assert!(result.is_some());

        let (_, global) = result.unwrap();
        match global.merge_strategy {
            MergeStrategy::FinalAggregate { ref partial_aggs, .. } => {
                assert_eq!(partial_aggs.len(), 1);
                assert_eq!(partial_aggs[0].final_agg, FinalAggKind::CountOfCounts);
            }
            _ => panic!("Expected FinalAggregate merge strategy"),
        }
    }

    #[test]
    fn test_distribute_avg() {
        let plan = make_agg_plan(vec![AggKind::Avg { column: "value".to_string() }]);
        let result = distribute_plan(&plan);
        assert!(result.is_some());

        let (_, global) = result.unwrap();
        match global.merge_strategy {
            MergeStrategy::FinalAggregate { ref partial_aggs, .. } => {
                let avg_agg = partial_aggs.iter()
                    .find(|a| matches!(a.final_agg, FinalAggKind::AvgFromSumCount))
                    .expect("Should have AvgFromSumCount");
                assert_eq!(avg_agg.source_columns.len(), 2);
            }
            _ => panic!("Expected FinalAggregate merge strategy"),
        }
    }

    #[test]
    fn test_distribute_with_limit() {
        let plan = make_simple_plan();
        let result = distribute_plan_with_options(&plan, Some(100), Some(10));
        assert!(result.is_some());

        let (local, global) = result.unwrap();
        assert_eq!(local.local_limit, Some(110)); // limit + offset pushed down

        match global.merge_strategy {
            MergeStrategy::MergeSort { limit, offset, .. } => {
                assert_eq!(limit, Some(100));
                assert_eq!(offset, Some(10));
            }
            _ => panic!("Expected MergeSort merge strategy"),
        }
    }

    #[test]
    fn test_join_strategy_selection_broadcast() {
        let stats = DistributedStats {
            table_stats: {
                let mut m = HashMap::new();
                m.insert("default.small".to_string(), TableStatsSnapshot {
                    row_count: 1000,
                    size_bytes: 50 * 1024 * 1024, // 50MB
                    partition_key: None,
                });
                m.insert("default.large".to_string(), TableStatsSnapshot {
                    row_count: 10_000_000,
                    size_bytes: 5 * 1024 * 1024 * 1024, // 5GB
                    partition_key: None,
                });
                m
            },
            node_count: 3,
            avg_network_bandwidth_mbps: 100.0,
            broadcast_threshold_bytes: 100 * 1024 * 1024,
        };

        let left = DistributedPlan::Simple(
            LocalPlan::new(PlanKind::Simple {
                agg: None,
                db: Some("default".to_string()),
                table: Some("large".to_string()),
                projection: None,
                filter: QueryFilter::default(),
                computed_columns: vec![],
            }),
            GlobalPlan::new(PlanKind::Simple {
                agg: None,
                db: Some("default".to_string()),
                table: Some("large".to_string()),
                projection: None,
                filter: QueryFilter::default(),
                computed_columns: vec![],
            }),
        );

        let right = DistributedPlan::Simple(
            LocalPlan::new(PlanKind::Simple {
                agg: None,
                db: Some("default".to_string()),
                table: Some("small".to_string()),
                projection: None,
                filter: QueryFilter::default(),
                computed_columns: vec![],
            }),
            GlobalPlan::new(PlanKind::Simple {
                agg: None,
                db: Some("default".to_string()),
                table: Some("small".to_string()),
                projection: None,
                filter: QueryFilter::default(),
                computed_columns: vec![],
            }),
        );

        let join_plan = distribute_join(
            left,
            right,
            JoinType::Inner,
            vec![("id".to_string(), "id".to_string())],
            Some(&stats),
        );

        match join_plan.join_strategy {
            JoinStrategy::Broadcast { broadcast_side, .. } => {
                assert_eq!(broadcast_side, JoinSide::Right);
            }
            _ => panic!("Expected Broadcast strategy for small right table"),
        }
    }

    #[test]
    fn test_cost_based_decision_small_table() {
        let stats = DistributedStats {
            table_stats: {
                let mut m = HashMap::new();
                m.insert("default.tiny".to_string(), TableStatsSnapshot {
                    row_count: 100,
                    size_bytes: 1024 * 1024, // 1MB
                    partition_key: None,
                });
                m
            },
            node_count: 3,
            ..Default::default()
        };

        let plan = PlanKind::Simple {
            agg: None,
            db: Some("default".to_string()),
            table: Some("tiny".to_string()),
            projection: None,
            filter: QueryFilter::default(),
            computed_columns: vec![],
        };

        let decision = should_distribute(&plan, Some(&stats));
        assert_eq!(decision, DistributionDecision::Local);
    }

    #[test]
    fn test_set_op_merge() {
        let op = SetOpMerge::Union { all: false };
        assert_eq!(op, SetOpMerge::Union { all: false });

        let op2 = SetOpMerge::Intersect { all: true };
        assert_eq!(op2, SetOpMerge::Intersect { all: true });
    }
}
