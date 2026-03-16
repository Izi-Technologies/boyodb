//! Query Cost Estimation API
//!
//! Pre-flight query cost estimates and resource usage predictions
//! before execution.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// Cost Model Configuration
// ============================================================================

/// Cost model parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModelConfig {
    /// CPU cost per row scanned
    pub cpu_cost_per_row: f64,
    /// CPU cost per row filtered
    pub cpu_cost_per_filter: f64,
    /// CPU cost per aggregation
    pub cpu_cost_per_aggregate: f64,
    /// CPU cost per sort comparison
    pub cpu_cost_per_sort_compare: f64,
    /// CPU cost per hash operation
    pub cpu_cost_per_hash: f64,
    /// I/O cost per page read (sequential)
    pub io_cost_per_page_seq: f64,
    /// I/O cost per page read (random)
    pub io_cost_per_page_random: f64,
    /// Network cost per byte transferred
    pub network_cost_per_byte: f64,
    /// Memory cost per byte allocated
    pub memory_cost_per_byte: f64,
    /// Page size in bytes
    pub page_size_bytes: usize,
    /// Average row size (for estimation)
    pub avg_row_size_bytes: usize,
}

impl Default for CostModelConfig {
    fn default() -> Self {
        Self {
            cpu_cost_per_row: 0.01,
            cpu_cost_per_filter: 0.005,
            cpu_cost_per_aggregate: 0.02,
            cpu_cost_per_sort_compare: 0.03,
            cpu_cost_per_hash: 0.01,
            io_cost_per_page_seq: 1.0,
            io_cost_per_page_random: 4.0,
            memory_cost_per_byte: 0.0001,
            network_cost_per_byte: 0.00001,
            page_size_bytes: 8192,
            avg_row_size_bytes: 256,
        }
    }
}

// ============================================================================
// Table and Column Statistics
// ============================================================================

/// Statistics for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: u64,
    pub total_size_bytes: u64,
    pub page_count: u64,
    pub avg_row_size_bytes: u64,
    pub columns: HashMap<String, ColumnStatistics>,
    pub indexes: Vec<IndexStatistics>,
    pub last_analyzed: u64,
}

/// Statistics for a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub data_type: String,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub avg_size_bytes: u64,
    pub histogram: Option<Histogram>,
}

/// Histogram for selectivity estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub bucket_count: usize,
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: String,
    pub upper_bound: String,
    pub frequency: f64,
    pub distinct_count: u64,
}

/// Statistics for an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub index_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub size_bytes: u64,
    pub leaf_pages: u64,
    pub depth: u32,
}

// ============================================================================
// Query Cost Estimate
// ============================================================================

/// Estimated query cost breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCostEstimate {
    /// Total estimated cost (abstract units)
    pub total_cost: f64,
    /// Estimated CPU cost
    pub cpu_cost: f64,
    /// Estimated I/O cost
    pub io_cost: f64,
    /// Estimated network cost (for distributed queries)
    pub network_cost: f64,
    /// Estimated memory usage (bytes)
    pub memory_estimate_bytes: u64,
    /// Estimated rows to scan
    pub rows_scanned: u64,
    /// Estimated rows returned
    pub rows_returned: u64,
    /// Estimated execution time (milliseconds)
    pub estimated_time_ms: u64,
    /// Cost breakdown by operation
    pub operation_costs: Vec<OperationCost>,
    /// Resource usage predictions
    pub resource_predictions: ResourcePredictions,
    /// Warnings about potential issues
    pub warnings: Vec<CostWarning>,
    /// Confidence level (0.0 - 1.0)
    pub confidence: f64,
}

/// Cost for a single operation in the plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationCost {
    pub operation_type: OperationType,
    pub description: String,
    pub cost: f64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub memory_bytes: u64,
}

/// Types of query operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationType {
    TableScan,
    IndexScan,
    IndexSeek,
    Filter,
    Project,
    Sort,
    HashAggregate,
    StreamAggregate,
    HashJoin,
    MergeJoin,
    NestedLoopJoin,
    Limit,
    Distinct,
    Union,
    Materialize,
    Exchange,
}

/// Resource usage predictions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePredictions {
    /// Peak memory usage (bytes)
    pub peak_memory_bytes: u64,
    /// Temp disk space needed (bytes)
    pub temp_disk_bytes: u64,
    /// Estimated CPU time (milliseconds)
    pub cpu_time_ms: u64,
    /// Estimated I/O wait time (milliseconds)
    pub io_wait_ms: u64,
    /// Network bytes transferred
    pub network_bytes: u64,
    /// Number of parallel workers suggested
    pub parallel_workers: u32,
    /// Will query spill to disk?
    pub will_spill: bool,
}

/// Warning about query cost
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostWarning {
    pub severity: WarningSeverity,
    pub code: String,
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WarningSeverity {
    Info,
    Warning,
    Critical,
}

// ============================================================================
// Query Cost Estimator
// ============================================================================

/// Query cost estimator service
pub struct QueryCostEstimator {
    config: CostModelConfig,
    table_stats: RwLock<HashMap<String, TableStatistics>>,
    /// Historical query execution data for calibration
    execution_history: RwLock<Vec<ExecutionRecord>>,
    /// Calibration factor based on history
    calibration_factor: RwLock<f64>,
}

/// Historical execution record for calibration
#[derive(Debug, Clone)]
struct ExecutionRecord {
    estimated_cost: f64,
    actual_time_ms: u64,
    timestamp: Instant,
}

impl QueryCostEstimator {
    pub fn new(config: CostModelConfig) -> Self {
        Self {
            config,
            table_stats: RwLock::new(HashMap::new()),
            execution_history: RwLock::new(Vec::new()),
            calibration_factor: RwLock::new(1.0),
        }
    }

    /// Update table statistics
    pub fn update_statistics(&self, stats: TableStatistics) {
        self.table_stats
            .write()
            .insert(stats.table_name.clone(), stats);
    }

    /// Get table statistics
    pub fn get_statistics(&self, table_name: &str) -> Option<TableStatistics> {
        self.table_stats.read().get(table_name).cloned()
    }

    /// Estimate cost for a parsed query
    pub fn estimate(&self, query: &QueryPlan) -> QueryCostEstimate {
        let mut total_cost = 0.0;
        let mut cpu_cost = 0.0;
        let mut io_cost = 0.0;
        let mut network_cost = 0.0;
        let mut memory_estimate: u64 = 0;
        let mut rows_scanned: u64 = 0;
        let mut rows_returned: u64 = 0;
        let mut operation_costs = Vec::new();
        let mut warnings = Vec::new();

        // Process each operation in the plan
        for op in &query.operations {
            let op_cost = self.estimate_operation(op, &query.referenced_tables);
            total_cost += op_cost.cost;

            match op_cost.operation_type {
                OperationType::TableScan | OperationType::IndexScan | OperationType::IndexSeek => {
                    io_cost += op_cost.cost * 0.7;
                    cpu_cost += op_cost.cost * 0.3;
                    rows_scanned += op_cost.rows_in;
                }
                OperationType::Filter => {
                    cpu_cost += op_cost.cost;
                }
                OperationType::Sort | OperationType::HashAggregate => {
                    cpu_cost += op_cost.cost * 0.6;
                    memory_estimate += op_cost.memory_bytes;
                }
                OperationType::HashJoin | OperationType::MergeJoin => {
                    cpu_cost += op_cost.cost * 0.5;
                    io_cost += op_cost.cost * 0.3;
                    memory_estimate += op_cost.memory_bytes;
                }
                OperationType::Exchange => {
                    network_cost += op_cost.cost;
                }
                _ => {
                    cpu_cost += op_cost.cost;
                }
            }

            operation_costs.push(op_cost);
        }

        // Get final row count from last operation
        if let Some(last_op) = operation_costs.last() {
            rows_returned = last_op.rows_out;
        }

        // Check for warnings
        self.check_warnings(&operation_costs, memory_estimate, &mut warnings);

        // Apply calibration factor
        let calibration = *self.calibration_factor.read();
        total_cost *= calibration;

        // Estimate time based on cost and historical data
        let estimated_time_ms = (total_cost * 10.0) as u64; // Simple linear model

        // Resource predictions
        let resource_predictions = ResourcePredictions {
            peak_memory_bytes: memory_estimate,
            temp_disk_bytes: if memory_estimate > 1024 * 1024 * 1024 {
                memory_estimate - 1024 * 1024 * 1024
            } else {
                0
            },
            cpu_time_ms: (cpu_cost * 5.0) as u64,
            io_wait_ms: (io_cost * 2.0) as u64,
            network_bytes: (network_cost * 1000.0) as u64,
            parallel_workers: self.suggest_parallelism(rows_scanned),
            will_spill: memory_estimate > 512 * 1024 * 1024,
        };

        // Calculate confidence based on statistics freshness
        let confidence = self.calculate_confidence(&query.referenced_tables);

        QueryCostEstimate {
            total_cost,
            cpu_cost,
            io_cost,
            network_cost,
            memory_estimate_bytes: memory_estimate,
            rows_scanned,
            rows_returned,
            estimated_time_ms,
            operation_costs,
            resource_predictions,
            warnings,
            confidence,
        }
    }

    fn estimate_operation(
        &self,
        op: &PlanOperation,
        referenced_tables: &[String],
    ) -> OperationCost {
        let stats = self.table_stats.read();

        match op {
            PlanOperation::Scan { table, filter } => {
                let table_stats = stats.get(table);
                let row_count = table_stats.map(|s| s.row_count).unwrap_or(10000);
                let selectivity = filter
                    .as_ref()
                    .map(|f| self.estimate_selectivity(f, table_stats))
                    .unwrap_or(1.0);

                let rows_out = (row_count as f64 * selectivity) as u64;
                let pages = table_stats.map(|s| s.page_count).unwrap_or(row_count / 100);
                let cost = pages as f64 * self.config.io_cost_per_page_seq
                    + row_count as f64 * self.config.cpu_cost_per_row;

                OperationCost {
                    operation_type: OperationType::TableScan,
                    description: format!("Scan table {}", table),
                    cost,
                    rows_in: row_count,
                    rows_out,
                    memory_bytes: 0,
                }
            }
            PlanOperation::IndexScan { table, index, .. } => {
                let table_stats = stats.get(table);
                let row_count = table_stats.map(|s| s.row_count).unwrap_or(10000);
                let selectivity = 0.1; // Assume 10% selectivity for index scans

                let rows_out = (row_count as f64 * selectivity) as u64;
                let cost = rows_out as f64 * self.config.io_cost_per_page_random
                    + rows_out as f64 * self.config.cpu_cost_per_row;

                OperationCost {
                    operation_type: OperationType::IndexScan,
                    description: format!("Index scan {} on {}", index, table),
                    cost,
                    rows_in: row_count,
                    rows_out,
                    memory_bytes: 0,
                }
            }
            PlanOperation::Filter { selectivity } => {
                let input_rows = 10000u64; // Would come from child operation
                let rows_out = (input_rows as f64 * selectivity) as u64;
                let cost = input_rows as f64 * self.config.cpu_cost_per_filter;

                OperationCost {
                    operation_type: OperationType::Filter,
                    description: "Filter rows".to_string(),
                    cost,
                    rows_in: input_rows,
                    rows_out,
                    memory_bytes: 0,
                }
            }
            PlanOperation::Sort { rows, columns } => {
                let comparisons = if *rows > 0 {
                    (*rows as f64 * (*rows as f64).log2()) as u64
                } else {
                    0
                };
                let cost =
                    comparisons as f64 * self.config.cpu_cost_per_sort_compare * *columns as f64;
                let memory = *rows * self.config.avg_row_size_bytes as u64;

                OperationCost {
                    operation_type: OperationType::Sort,
                    description: format!("Sort {} rows by {} columns", rows, columns),
                    cost,
                    rows_in: *rows,
                    rows_out: *rows,
                    memory_bytes: memory,
                }
            }
            PlanOperation::Aggregate { rows, groups } => {
                let cost = *rows as f64 * self.config.cpu_cost_per_aggregate
                    + *rows as f64 * self.config.cpu_cost_per_hash;
                let memory = *groups * self.config.avg_row_size_bytes as u64 * 2;

                OperationCost {
                    operation_type: OperationType::HashAggregate,
                    description: format!("Aggregate {} rows into {} groups", rows, groups),
                    cost,
                    rows_in: *rows,
                    rows_out: *groups,
                    memory_bytes: memory,
                }
            }
            PlanOperation::HashJoin {
                left_rows,
                right_rows,
            } => {
                let build_cost = *right_rows as f64 * self.config.cpu_cost_per_hash;
                let probe_cost = *left_rows as f64 * self.config.cpu_cost_per_hash;
                let cost = build_cost + probe_cost;
                let memory = *right_rows * self.config.avg_row_size_bytes as u64;
                let rows_out = (*left_rows).min(*right_rows); // Conservative estimate

                OperationCost {
                    operation_type: OperationType::HashJoin,
                    description: format!("Hash join {}x{} rows", left_rows, right_rows),
                    cost,
                    rows_in: left_rows + right_rows,
                    rows_out,
                    memory_bytes: memory,
                }
            }
            PlanOperation::Limit { limit } => OperationCost {
                operation_type: OperationType::Limit,
                description: format!("Limit to {} rows", limit),
                cost: 1.0,
                rows_in: *limit,
                rows_out: *limit,
                memory_bytes: 0,
            },
            PlanOperation::Exchange { rows } => {
                let network_bytes = *rows * self.config.avg_row_size_bytes as u64;
                let cost = network_bytes as f64 * self.config.network_cost_per_byte;

                OperationCost {
                    operation_type: OperationType::Exchange,
                    description: format!("Exchange {} rows", rows),
                    cost,
                    rows_in: *rows,
                    rows_out: *rows,
                    memory_bytes: 0,
                }
            }
        }
    }

    fn estimate_selectivity(
        &self,
        filter: &FilterPredicate,
        _table_stats: Option<&TableStatistics>,
    ) -> f64 {
        // Simple selectivity estimation
        match filter {
            FilterPredicate::Equals { .. } => 0.01, // 1%
            FilterPredicate::Range { .. } => 0.1,   // 10%
            FilterPredicate::In { values } => (values.len() as f64 * 0.01).min(0.5),
            FilterPredicate::Like { .. } => 0.1, // 10%
            FilterPredicate::IsNull => 0.01,
            FilterPredicate::And(predicates) => predicates
                .iter()
                .map(|p| self.estimate_selectivity(p, _table_stats))
                .product(),
            FilterPredicate::Or(predicates) => {
                let sel: f64 = predicates
                    .iter()
                    .map(|p| self.estimate_selectivity(p, _table_stats))
                    .sum();
                sel.min(1.0)
            }
        }
    }

    fn check_warnings(
        &self,
        operations: &[OperationCost],
        memory_estimate: u64,
        warnings: &mut Vec<CostWarning>,
    ) {
        // Check for full table scans
        for op in operations {
            if matches!(op.operation_type, OperationType::TableScan) && op.rows_in > 1_000_000 {
                warnings.push(CostWarning {
                    severity: WarningSeverity::Warning,
                    code: "FULL_TABLE_SCAN".to_string(),
                    message: format!("Full table scan on {} rows: {}", op.rows_in, op.description),
                    suggestion: Some("Consider adding an index".to_string()),
                });
            }
        }

        // Check for high memory usage
        if memory_estimate > 1024 * 1024 * 1024 {
            warnings.push(CostWarning {
                severity: WarningSeverity::Warning,
                code: "HIGH_MEMORY".to_string(),
                message: format!(
                    "Query may use {} GB of memory",
                    memory_estimate / 1024 / 1024 / 1024
                ),
                suggestion: Some("Consider adding LIMIT or reducing result set".to_string()),
            });
        }

        // Check for nested loop joins
        for op in operations {
            if matches!(op.operation_type, OperationType::NestedLoopJoin) && op.rows_in > 100_000 {
                warnings.push(CostWarning {
                    severity: WarningSeverity::Critical,
                    code: "NESTED_LOOP_JOIN".to_string(),
                    message: format!("Nested loop join with {} rows", op.rows_in),
                    suggestion: Some("Consider adding join indexes".to_string()),
                });
            }
        }
    }

    fn suggest_parallelism(&self, rows: u64) -> u32 {
        if rows < 10_000 {
            1
        } else if rows < 100_000 {
            2
        } else if rows < 1_000_000 {
            4
        } else if rows < 10_000_000 {
            8
        } else {
            16
        }
    }

    fn calculate_confidence(&self, tables: &[String]) -> f64 {
        let stats = self.table_stats.read();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut total_confidence = 0.0;
        let mut count = 0;

        for table in tables {
            if let Some(table_stats) = stats.get(table) {
                let age_secs = now.saturating_sub(table_stats.last_analyzed);
                // Confidence decays over time
                let table_confidence = 1.0 - (age_secs as f64 / (24.0 * 3600.0)).min(0.5);
                total_confidence += table_confidence;
                count += 1;
            } else {
                // No stats - low confidence
                total_confidence += 0.3;
                count += 1;
            }
        }

        if count > 0 {
            total_confidence / count as f64
        } else {
            0.5
        }
    }

    /// Record actual execution for calibration
    pub fn record_execution(&self, estimated_cost: f64, actual_time_ms: u64) {
        let record = ExecutionRecord {
            estimated_cost,
            actual_time_ms,
            timestamp: Instant::now(),
        };

        let mut history = self.execution_history.write();
        history.push(record);

        // Keep last 1000 records
        if history.len() > 1000 {
            history.remove(0);
        }

        // Recalibrate
        self.recalibrate(&history);
    }

    fn recalibrate(&self, history: &[ExecutionRecord]) {
        if history.len() < 10 {
            return;
        }

        // Calculate ratio of actual to estimated
        let ratios: Vec<f64> = history
            .iter()
            .filter(|r| r.estimated_cost > 0.0)
            .map(|r| r.actual_time_ms as f64 / r.estimated_cost)
            .collect();

        if ratios.is_empty() {
            return;
        }

        // Use median ratio
        let mut sorted = ratios.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];

        *self.calibration_factor.write() = median;
    }
}

// ============================================================================
// Query Plan Representation
// ============================================================================

/// Simplified query plan for cost estimation
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub sql: String,
    pub operations: Vec<PlanOperation>,
    pub referenced_tables: Vec<String>,
}

/// Plan operations
#[derive(Debug, Clone)]
pub enum PlanOperation {
    Scan {
        table: String,
        filter: Option<FilterPredicate>,
    },
    IndexScan {
        table: String,
        index: String,
        filter: Option<FilterPredicate>,
    },
    Filter {
        selectivity: f64,
    },
    Sort {
        rows: u64,
        columns: usize,
    },
    Aggregate {
        rows: u64,
        groups: u64,
    },
    HashJoin {
        left_rows: u64,
        right_rows: u64,
    },
    Limit {
        limit: u64,
    },
    Exchange {
        rows: u64,
    },
}

/// Filter predicate for selectivity estimation
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    Equals {
        column: String,
        value: String,
    },
    Range {
        column: String,
        low: Option<String>,
        high: Option<String>,
    },
    In {
        values: Vec<String>,
    },
    Like {
        column: String,
        pattern: String,
    },
    IsNull,
    And(Vec<FilterPredicate>),
    Or(Vec<FilterPredicate>),
}

// ============================================================================
// Cost Estimation Service
// ============================================================================

/// Cost estimation service with caching
pub struct CostEstimationService {
    estimator: Arc<QueryCostEstimator>,
    /// Cache of recent estimates
    estimate_cache: RwLock<HashMap<String, (QueryCostEstimate, Instant)>>,
    /// Cache TTL
    cache_ttl_secs: u64,
}

impl CostEstimationService {
    pub fn new(config: CostModelConfig) -> Self {
        Self {
            estimator: Arc::new(QueryCostEstimator::new(config)),
            estimate_cache: RwLock::new(HashMap::new()),
            cache_ttl_secs: 60,
        }
    }

    /// Estimate query cost (with caching)
    pub fn estimate_cost(&self, sql: &str, plan: &QueryPlan) -> QueryCostEstimate {
        // Check cache
        {
            let cache = self.estimate_cache.read();
            if let Some((estimate, timestamp)) = cache.get(sql) {
                if timestamp.elapsed().as_secs() < self.cache_ttl_secs {
                    return estimate.clone();
                }
            }
        }

        // Compute estimate
        let estimate = self.estimator.estimate(plan);

        // Cache result
        {
            let mut cache = self.estimate_cache.write();
            cache.insert(sql.to_string(), (estimate.clone(), Instant::now()));

            // Prune old entries
            cache.retain(|_, (_, ts)| ts.elapsed().as_secs() < self.cache_ttl_secs * 2);
        }

        estimate
    }

    /// Get estimator for direct access
    pub fn estimator(&self) -> Arc<QueryCostEstimator> {
        self.estimator.clone()
    }

    /// Quick estimate without full plan analysis
    pub fn quick_estimate(
        &self,
        tables: &[&str],
        has_join: bool,
        has_aggregate: bool,
    ) -> QuickEstimate {
        let stats = self.estimator.table_stats.read();

        let total_rows: u64 = tables
            .iter()
            .filter_map(|t| stats.get(*t))
            .map(|s| s.row_count)
            .sum();

        let complexity = if has_join && has_aggregate {
            QueryComplexity::High
        } else if has_join || has_aggregate {
            QueryComplexity::Medium
        } else {
            QueryComplexity::Low
        };

        let estimated_time_ms = match complexity {
            QueryComplexity::Low => total_rows / 100_000 + 10,
            QueryComplexity::Medium => total_rows / 50_000 + 50,
            QueryComplexity::High => total_rows / 10_000 + 200,
        };

        QuickEstimate {
            estimated_rows: total_rows,
            complexity,
            estimated_time_ms,
            should_analyze: total_rows > 1_000_000 || has_join,
        }
    }
}

/// Quick estimate result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickEstimate {
    pub estimated_rows: u64,
    pub complexity: QueryComplexity,
    pub estimated_time_ms: u64,
    pub should_analyze: bool,
}

/// Query complexity level
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum QueryComplexity {
    Low,
    Medium,
    High,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_cost_estimation() {
        let config = CostModelConfig::default();
        let estimator = QueryCostEstimator::new(config);

        // Add table statistics
        let stats = TableStatistics {
            table_name: "users".to_string(),
            row_count: 100_000,
            total_size_bytes: 25_000_000,
            page_count: 3000,
            avg_row_size_bytes: 250,
            columns: HashMap::new(),
            indexes: vec![],
            last_analyzed: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        estimator.update_statistics(stats);

        let plan = QueryPlan {
            sql: "SELECT * FROM users WHERE id = 1".to_string(),
            operations: vec![PlanOperation::Scan {
                table: "users".to_string(),
                filter: Some(FilterPredicate::Equals {
                    column: "id".to_string(),
                    value: "1".to_string(),
                }),
            }],
            referenced_tables: vec!["users".to_string()],
        };

        let estimate = estimator.estimate(&plan);

        assert!(estimate.total_cost > 0.0);
        assert_eq!(estimate.rows_scanned, 100_000);
        assert!(estimate.confidence > 0.5);
    }

    #[test]
    fn test_join_cost_estimation() {
        let config = CostModelConfig::default();
        let estimator = QueryCostEstimator::new(config);

        let plan = QueryPlan {
            sql: "SELECT * FROM users u JOIN orders o ON u.id = o.user_id".to_string(),
            operations: vec![
                PlanOperation::Scan {
                    table: "users".to_string(),
                    filter: None,
                },
                PlanOperation::HashJoin {
                    left_rows: 100_000,
                    right_rows: 500_000,
                },
            ],
            referenced_tables: vec!["users".to_string(), "orders".to_string()],
        };

        let estimate = estimator.estimate(&plan);

        assert!(estimate.total_cost > 0.0);
        assert!(estimate.memory_estimate_bytes > 0);
    }

    #[test]
    fn test_warning_generation() {
        let config = CostModelConfig::default();
        let estimator = QueryCostEstimator::new(config);

        // Add large table statistics
        let stats = TableStatistics {
            table_name: "big_table".to_string(),
            row_count: 10_000_000,
            total_size_bytes: 2_500_000_000,
            page_count: 300_000,
            avg_row_size_bytes: 250,
            columns: HashMap::new(),
            indexes: vec![],
            last_analyzed: 0,
        };
        estimator.update_statistics(stats);

        let plan = QueryPlan {
            sql: "SELECT * FROM big_table".to_string(),
            operations: vec![PlanOperation::Scan {
                table: "big_table".to_string(),
                filter: None,
            }],
            referenced_tables: vec!["big_table".to_string()],
        };

        let estimate = estimator.estimate(&plan);

        assert!(!estimate.warnings.is_empty());
        assert!(estimate
            .warnings
            .iter()
            .any(|w| w.code == "FULL_TABLE_SCAN"));
    }
}
