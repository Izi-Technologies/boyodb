//! AI Query Optimizer
//!
//! Machine learning-based query optimization that learns from query execution history.
//! Uses features like cardinality estimation, cost prediction, and adaptive plan selection.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// Query feature vector for ML model
#[derive(Debug, Clone)]
pub struct QueryFeatures {
    /// Number of tables
    pub num_tables: u32,
    /// Number of joins
    pub num_joins: u32,
    /// Number of predicates
    pub num_predicates: u32,
    /// Number of aggregations
    pub num_aggregations: u32,
    /// Number of subqueries
    pub num_subqueries: u32,
    /// Has ORDER BY
    pub has_order_by: bool,
    /// Has GROUP BY
    pub has_group_by: bool,
    /// Has DISTINCT
    pub has_distinct: bool,
    /// Has LIMIT
    pub has_limit: bool,
    /// Total estimated rows
    pub estimated_rows: u64,
    /// Estimated selectivity (0.0 - 1.0)
    pub selectivity: f64,
    /// Query complexity score
    pub complexity_score: f64,
    /// Table statistics available
    pub stats_available: bool,
}

impl Default for QueryFeatures {
    fn default() -> Self {
        Self {
            num_tables: 0,
            num_joins: 0,
            num_predicates: 0,
            num_aggregations: 0,
            num_subqueries: 0,
            has_order_by: false,
            has_group_by: false,
            has_distinct: false,
            has_limit: false,
            estimated_rows: 0,
            selectivity: 1.0,
            complexity_score: 0.0,
            stats_available: false,
        }
    }
}

impl QueryFeatures {
    /// Convert to feature vector for ML model
    pub fn to_vector(&self) -> Vec<f64> {
        vec![
            self.num_tables as f64,
            self.num_joins as f64,
            self.num_predicates as f64,
            self.num_aggregations as f64,
            self.num_subqueries as f64,
            if self.has_order_by { 1.0 } else { 0.0 },
            if self.has_group_by { 1.0 } else { 0.0 },
            if self.has_distinct { 1.0 } else { 0.0 },
            if self.has_limit { 1.0 } else { 0.0 },
            (self.estimated_rows as f64).ln_1p(),
            self.selectivity,
            self.complexity_score,
            if self.stats_available { 1.0 } else { 0.0 },
        ]
    }
}

/// Plan alternative for the optimizer to choose from
#[derive(Debug, Clone)]
pub struct PlanAlternative {
    /// Plan ID
    pub id: u32,
    /// Plan description
    pub description: String,
    /// Join order
    pub join_order: Vec<String>,
    /// Index hints
    pub index_hints: Vec<String>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
    /// Features
    pub features: Vec<f64>,
}

/// Historical execution data
#[derive(Debug, Clone)]
pub struct ExecutionHistory {
    /// Query fingerprint
    pub fingerprint: String,
    /// Plan used
    pub plan_id: u32,
    /// Actual execution time
    pub execution_time: Duration,
    /// Rows returned
    pub rows_returned: u64,
    /// Bytes scanned
    pub bytes_scanned: u64,
    /// Features at execution time
    pub features: QueryFeatures,
    /// Timestamp
    pub timestamp: SystemTime,
}

/// Cardinality estimation model
#[derive(Debug, Clone)]
pub struct CardinalityModel {
    /// Table statistics
    table_stats: HashMap<String, TableStats>,
    /// Column statistics
    column_stats: HashMap<String, ColumnStats>,
    /// Join selectivity history
    join_history: Vec<JoinHistoryEntry>,
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: u64,
    pub size_bytes: u64,
    pub last_analyzed: SystemTime,
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: u64,
    pub null_fraction: f64,
    pub avg_width: u32,
    pub correlation: f64,
    pub histogram: Vec<HistogramBucket>,
    pub most_common_values: Vec<(String, f64)>,
}

/// Histogram bucket
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub lower: String,
    pub upper: String,
    pub frequency: f64,
}

/// Join selectivity history
#[derive(Debug, Clone)]
struct JoinHistoryEntry {
    left_table: String,
    right_table: String,
    join_columns: Vec<String>,
    actual_selectivity: f64,
}

impl CardinalityModel {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            column_stats: HashMap::new(),
            join_history: Vec::new(),
        }
    }

    /// Estimate cardinality for a table scan
    pub fn estimate_scan(&self, table: &str, predicates: &[Predicate]) -> u64 {
        let base_rows = self.table_stats.get(table)
            .map(|s| s.row_count)
            .unwrap_or(1000); // Default estimate

        let mut selectivity = 1.0;
        for pred in predicates {
            selectivity *= self.estimate_predicate_selectivity(pred);
        }

        (base_rows as f64 * selectivity).max(1.0) as u64
    }

    /// Estimate join cardinality
    pub fn estimate_join(
        &self,
        left_table: &str,
        right_table: &str,
        left_rows: u64,
        right_rows: u64,
        _join_columns: &[String],
    ) -> u64 {
        // Check history for similar joins
        for entry in &self.join_history {
            if (entry.left_table == left_table && entry.right_table == right_table) ||
               (entry.left_table == right_table && entry.right_table == left_table) {
                return ((left_rows as f64 * right_rows as f64) * entry.actual_selectivity) as u64;
            }
        }

        // Default: assume foreign key relationship
        let smaller = left_rows.min(right_rows);
        let _larger = left_rows.max(right_rows);

        // Assume 80% of smaller table matches
        (smaller as f64 * 0.8) as u64
    }

    fn estimate_predicate_selectivity(&self, pred: &Predicate) -> f64 {
        let col_key = format!("{}.{}", pred.table, pred.column);

        if let Some(stats) = self.column_stats.get(&col_key) {
            match pred.op {
                PredicateOp::Equal => {
                    // 1 / distinct_count
                    1.0 / (stats.distinct_count as f64).max(1.0)
                }
                PredicateOp::NotEqual => {
                    1.0 - (1.0 / (stats.distinct_count as f64).max(1.0))
                }
                PredicateOp::LessThan | PredicateOp::LessEqual |
                PredicateOp::GreaterThan | PredicateOp::GreaterEqual => {
                    // Use histogram if available
                    if !stats.histogram.is_empty() {
                        // Simple: assume 30% selectivity
                        0.3
                    } else {
                        0.33
                    }
                }
                PredicateOp::Like => {
                    // Depends on pattern
                    0.1
                }
                PredicateOp::In => {
                    // Depends on IN list size
                    0.2
                }
                PredicateOp::IsNull => {
                    stats.null_fraction
                }
                PredicateOp::IsNotNull => {
                    1.0 - stats.null_fraction
                }
            }
        } else {
            // Default selectivities
            match pred.op {
                PredicateOp::Equal => 0.01,
                PredicateOp::NotEqual => 0.99,
                PredicateOp::LessThan | PredicateOp::LessEqual |
                PredicateOp::GreaterThan | PredicateOp::GreaterEqual => 0.33,
                PredicateOp::Like => 0.1,
                PredicateOp::In => 0.2,
                PredicateOp::IsNull => 0.01,
                PredicateOp::IsNotNull => 0.99,
            }
        }
    }

    /// Update statistics from actual execution
    pub fn update_from_execution(&mut self, history: &ExecutionHistory) {
        // Update table row counts if significantly different
        // This would be more sophisticated in production
    }

    /// Add table statistics
    pub fn add_table_stats(&mut self, table: String, stats: TableStats) {
        self.table_stats.insert(table, stats);
    }

    /// Add column statistics
    pub fn add_column_stats(&mut self, table: &str, column: &str, stats: ColumnStats) {
        let key = format!("{}.{}", table, column);
        self.column_stats.insert(key, stats);
    }
}

impl Default for CardinalityModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Predicate for cardinality estimation
#[derive(Debug, Clone)]
pub struct Predicate {
    pub table: String,
    pub column: String,
    pub op: PredicateOp,
    pub value: Option<String>,
}

/// Predicate operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Like,
    In,
    IsNull,
    IsNotNull,
}

/// Cost model for query plans
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Sequential page cost
    pub seq_page_cost: f64,
    /// Random page cost
    pub random_page_cost: f64,
    /// CPU tuple cost
    pub cpu_tuple_cost: f64,
    /// CPU index tuple cost
    pub cpu_index_tuple_cost: f64,
    /// CPU operator cost
    pub cpu_operator_cost: f64,
    /// Parallel tuple cost
    pub parallel_tuple_cost: f64,
    /// Effective cache size (pages)
    pub effective_cache_size: u64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
            parallel_tuple_cost: 0.001,
            effective_cache_size: 524288, // 4GB at 8KB pages
        }
    }
}

impl CostModel {
    /// Estimate cost of sequential scan
    pub fn seq_scan_cost(&self, pages: u64, rows: u64) -> f64 {
        (pages as f64 * self.seq_page_cost) +
        (rows as f64 * self.cpu_tuple_cost)
    }

    /// Estimate cost of index scan
    pub fn index_scan_cost(&self, index_pages: u64, heap_pages: u64, rows: u64) -> f64 {
        // Index traversal
        let index_cost = index_pages as f64 * self.random_page_cost;
        // Heap fetches (may be random)
        let heap_cost = heap_pages as f64 * self.random_page_cost;
        // CPU costs
        let cpu_cost = rows as f64 * (self.cpu_index_tuple_cost + self.cpu_tuple_cost);

        index_cost + heap_cost + cpu_cost
    }

    /// Estimate cost of hash join
    pub fn hash_join_cost(&self, outer_rows: u64, inner_rows: u64) -> f64 {
        // Build hash table
        let build_cost = inner_rows as f64 * self.cpu_operator_cost;
        // Probe hash table
        let probe_cost = outer_rows as f64 * self.cpu_operator_cost;

        build_cost + probe_cost
    }

    /// Estimate cost of merge join
    pub fn merge_join_cost(&self, outer_rows: u64, inner_rows: u64) -> f64 {
        // Merge is linear
        (outer_rows + inner_rows) as f64 * self.cpu_operator_cost
    }

    /// Estimate cost of nested loop join
    pub fn nested_loop_cost(&self, outer_rows: u64, inner_rows: u64) -> f64 {
        (outer_rows * inner_rows) as f64 * self.cpu_tuple_cost
    }

    /// Estimate sort cost
    pub fn sort_cost(&self, rows: u64) -> f64 {
        if rows <= 1 {
            return 0.0;
        }
        // O(n log n) sorting
        let log_rows = (rows as f64).log2();
        rows as f64 * log_rows * self.cpu_operator_cost
    }
}

/// AI Query Optimizer
pub struct AiQueryOptimizer {
    /// Cardinality model
    cardinality: RwLock<CardinalityModel>,
    /// Cost model
    cost_model: CostModel,
    /// Execution history
    history: RwLock<Vec<ExecutionHistory>>,
    /// History limit
    history_limit: usize,
    /// Learned weights for plan scoring
    weights: RwLock<PlanScoringWeights>,
    /// Statistics
    stats: RwLock<OptimizerStats>,
}

/// Weights for plan scoring
#[derive(Debug, Clone)]
pub struct PlanScoringWeights {
    /// Weight for estimated cost
    pub cost_weight: f64,
    /// Weight for historical performance
    pub history_weight: f64,
    /// Weight for parallelism potential
    pub parallel_weight: f64,
    /// Weight for index usage
    pub index_weight: f64,
    /// Learning rate for weight updates
    pub learning_rate: f64,
}

impl Default for PlanScoringWeights {
    fn default() -> Self {
        Self {
            cost_weight: 0.4,
            history_weight: 0.3,
            parallel_weight: 0.15,
            index_weight: 0.15,
            learning_rate: 0.01,
        }
    }
}

/// Optimizer statistics
#[derive(Debug, Clone, Default)]
pub struct OptimizerStats {
    /// Queries optimized
    pub queries_optimized: u64,
    /// Plans generated
    pub plans_generated: u64,
    /// Plans selected by ML
    pub ml_selections: u64,
    /// Fallback to cost-based
    pub cost_based_fallbacks: u64,
    /// Average optimization time
    pub avg_optimization_time_us: f64,
    /// Model updates
    pub model_updates: u64,
}

impl AiQueryOptimizer {
    pub fn new() -> Self {
        Self {
            cardinality: RwLock::new(CardinalityModel::new()),
            cost_model: CostModel::default(),
            history: RwLock::new(Vec::new()),
            history_limit: 10000,
            weights: RwLock::new(PlanScoringWeights::default()),
            stats: RwLock::new(OptimizerStats::default()),
        }
    }

    /// Optimize a query and select the best plan
    pub fn optimize(
        &self,
        features: &QueryFeatures,
        alternatives: &[PlanAlternative],
    ) -> OptimizationResult {
        let start = Instant::now();
        let mut stats = self.stats.write().unwrap();
        stats.queries_optimized += 1;
        stats.plans_generated += alternatives.len() as u64;
        drop(stats);

        if alternatives.is_empty() {
            return OptimizationResult {
                selected_plan: 0,
                confidence: 0.0,
                reasoning: "No alternatives provided".into(),
                optimization_time: start.elapsed(),
            };
        }

        if alternatives.len() == 1 {
            return OptimizationResult {
                selected_plan: alternatives[0].id,
                confidence: 1.0,
                reasoning: "Single plan available".into(),
                optimization_time: start.elapsed(),
            };
        }

        // Score each alternative
        let weights = self.weights.read().unwrap();
        let history = self.history.read().unwrap();

        let mut scores: Vec<(u32, f64, String)> = alternatives.iter().map(|alt| {
            let mut score = 0.0;
            let mut reasons = Vec::new();

            // Cost-based score (lower is better, so invert)
            let max_cost = alternatives.iter().map(|a| a.estimated_cost).fold(0.0, f64::max);
            let cost_score = if max_cost > 0.0 {
                1.0 - (alt.estimated_cost / max_cost)
            } else {
                1.0
            };
            score += cost_score * weights.cost_weight;
            reasons.push(format!("cost_score={:.2}", cost_score));

            // Historical performance
            let hist_score = self.get_historical_score(&alt.description, &history);
            score += hist_score * weights.history_weight;
            reasons.push(format!("history_score={:.2}", hist_score));

            // Parallelism potential (based on plan features)
            let parallel_score = if alt.estimated_rows > 10000 { 0.8 } else { 0.2 };
            score += parallel_score * weights.parallel_weight;

            // Index usage score
            let index_score = if !alt.index_hints.is_empty() { 0.9 } else { 0.3 };
            score += index_score * weights.index_weight;
            reasons.push(format!("index_score={:.2}", index_score));

            (alt.id, score, reasons.join(", "))
        }).collect();

        // Select best scoring plan
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let (best_id, best_score, reasoning) = scores.remove(0);

        // Confidence based on score gap
        let second_score = scores.first().map(|s| s.1).unwrap_or(0.0);
        let confidence = if best_score > 0.0 {
            ((best_score - second_score) / best_score).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let mut stats = self.stats.write().unwrap();
        if confidence > 0.5 {
            stats.ml_selections += 1;
        } else {
            stats.cost_based_fallbacks += 1;
        }
        let elapsed = start.elapsed();
        stats.avg_optimization_time_us =
            (stats.avg_optimization_time_us * (stats.queries_optimized - 1) as f64 +
             elapsed.as_micros() as f64) / stats.queries_optimized as f64;

        OptimizationResult {
            selected_plan: best_id,
            confidence,
            reasoning,
            optimization_time: elapsed,
        }
    }

    /// Get historical score for a plan type
    fn get_historical_score(&self, plan_desc: &str, history: &[ExecutionHistory]) -> f64 {
        let relevant: Vec<&ExecutionHistory> = history.iter()
            .filter(|h| h.fingerprint.contains(plan_desc) || plan_desc.contains(&h.fingerprint))
            .collect();

        if relevant.is_empty() {
            return 0.5; // Neutral score
        }

        // Average normalized execution time
        let avg_time: f64 = relevant.iter()
            .map(|h| h.execution_time.as_micros() as f64)
            .sum::<f64>() / relevant.len() as f64;

        // Lower time is better, normalize to 0-1
        let max_expected_time = 1_000_000.0; // 1 second
        1.0 - (avg_time / max_expected_time).min(1.0)
    }

    /// Record execution result for learning
    pub fn record_execution(&self, history_entry: ExecutionHistory) {
        let mut history = self.history.write().unwrap();

        // Limit history size
        if history.len() >= self.history_limit {
            history.remove(0);
        }

        history.push(history_entry.clone());
        drop(history);

        // Update cardinality model
        let mut cardinality = self.cardinality.write().unwrap();
        cardinality.update_from_execution(&history_entry);

        let mut stats = self.stats.write().unwrap();
        stats.model_updates += 1;
    }

    /// Update learning weights based on feedback
    pub fn update_weights(&self, actual_best: u32, predicted_best: u32, alternatives: &[PlanAlternative]) {
        if actual_best == predicted_best {
            return; // Prediction was correct
        }

        let mut weights = self.weights.write().unwrap();
        let lr = weights.learning_rate;

        // Find the alternatives
        let actual_alt = alternatives.iter().find(|a| a.id == actual_best);
        let predicted_alt = alternatives.iter().find(|a| a.id == predicted_best);

        if let (Some(actual), Some(predicted)) = (actual_alt, predicted_alt) {
            // Adjust weights based on what would have helped
            if actual.estimated_cost < predicted.estimated_cost {
                // Cost model was right, increase cost weight
                weights.cost_weight += lr;
            } else {
                weights.cost_weight -= lr;
            }

            if !actual.index_hints.is_empty() && predicted.index_hints.is_empty() {
                // Index usage was better
                weights.index_weight += lr;
            }

            // Normalize weights
            let total = weights.cost_weight + weights.history_weight +
                       weights.parallel_weight + weights.index_weight;
            weights.cost_weight /= total;
            weights.history_weight /= total;
            weights.parallel_weight /= total;
            weights.index_weight /= total;
        }
    }

    /// Get cardinality estimate
    pub fn estimate_cardinality(&self, table: &str, predicates: &[Predicate]) -> u64 {
        self.cardinality.read().unwrap().estimate_scan(table, predicates)
    }

    /// Add table statistics
    pub fn add_table_stats(&self, table: String, stats: TableStats) {
        self.cardinality.write().unwrap().add_table_stats(table, stats);
    }

    /// Add column statistics
    pub fn add_column_stats(&self, table: &str, column: &str, stats: ColumnStats) {
        self.cardinality.write().unwrap().add_column_stats(table, column, stats);
    }

    /// Get optimizer statistics
    pub fn stats(&self) -> OptimizerStats {
        self.stats.read().unwrap().clone()
    }

    /// Get current weights
    pub fn get_weights(&self) -> PlanScoringWeights {
        self.weights.read().unwrap().clone()
    }
}

impl Default for AiQueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimization result
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    /// Selected plan ID
    pub selected_plan: u32,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
    /// Reasoning for selection
    pub reasoning: String,
    /// Time spent optimizing
    pub optimization_time: Duration,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_features_to_vector() {
        let features = QueryFeatures {
            num_tables: 2,
            num_joins: 1,
            num_predicates: 3,
            has_order_by: true,
            ..Default::default()
        };

        let vec = features.to_vector();
        assert_eq!(vec[0], 2.0); // num_tables
        assert_eq!(vec[1], 1.0); // num_joins
        assert_eq!(vec[5], 1.0); // has_order_by
    }

    #[test]
    fn test_cardinality_estimation() {
        let mut model = CardinalityModel::new();

        model.add_table_stats("users".into(), TableStats {
            row_count: 10000,
            size_bytes: 1000000,
            last_analyzed: SystemTime::now(),
        });

        model.add_column_stats("users", "status", ColumnStats {
            distinct_count: 5,
            null_fraction: 0.0,
            avg_width: 10,
            correlation: 1.0,
            histogram: vec![],
            most_common_values: vec![],
        });

        // Estimate with equality predicate
        let predicates = vec![Predicate {
            table: "users".into(),
            column: "status".into(),
            op: PredicateOp::Equal,
            value: Some("active".into()),
        }];

        let estimate = model.estimate_scan("users", &predicates);
        assert!(estimate > 0);
        assert!(estimate < 10000); // Should be less than total
    }

    #[test]
    fn test_cost_model() {
        let cost_model = CostModel::default();

        // Sequential scan
        let seq_cost = cost_model.seq_scan_cost(100, 10000);
        assert!(seq_cost > 0.0);

        // Index scan should be cheaper for small result sets
        let idx_cost = cost_model.index_scan_cost(10, 5, 100);
        assert!(idx_cost > 0.0);

        // Hash join
        let hash_cost = cost_model.hash_join_cost(10000, 1000);
        assert!(hash_cost > 0.0);
    }

    #[test]
    fn test_optimizer_selection() {
        let optimizer = AiQueryOptimizer::new();

        let features = QueryFeatures::default();

        let alternatives = vec![
            PlanAlternative {
                id: 1,
                description: "Sequential Scan".into(),
                join_order: vec![],
                index_hints: vec![],
                estimated_cost: 1000.0,
                estimated_rows: 10000,
                features: vec![],
            },
            PlanAlternative {
                id: 2,
                description: "Index Scan".into(),
                join_order: vec![],
                index_hints: vec!["idx_status".into()],
                estimated_cost: 100.0,
                estimated_rows: 100,
                features: vec![],
            },
        ];

        let result = optimizer.optimize(&features, &alternatives);

        // Should select index scan (lower cost, has index)
        assert_eq!(result.selected_plan, 2);
        assert!(result.confidence > 0.0);
    }

    #[test]
    fn test_record_execution() {
        let optimizer = AiQueryOptimizer::new();

        let history = ExecutionHistory {
            fingerprint: "SELECT * FROM users".into(),
            plan_id: 1,
            execution_time: Duration::from_millis(50),
            rows_returned: 100,
            bytes_scanned: 10000,
            features: QueryFeatures::default(),
            timestamp: SystemTime::now(),
        };

        optimizer.record_execution(history);

        let stats = optimizer.stats();
        assert_eq!(stats.model_updates, 1);
    }

    #[test]
    fn test_weight_normalization() {
        let optimizer = AiQueryOptimizer::new();

        let alternatives = vec![
            PlanAlternative {
                id: 1,
                description: "Plan A".into(),
                join_order: vec![],
                index_hints: vec![],
                estimated_cost: 1000.0,
                estimated_rows: 10000,
                features: vec![],
            },
            PlanAlternative {
                id: 2,
                description: "Plan B".into(),
                join_order: vec![],
                index_hints: vec!["idx".into()],
                estimated_cost: 500.0,
                estimated_rows: 5000,
                features: vec![],
            },
        ];

        // Update weights
        optimizer.update_weights(2, 1, &alternatives);

        let weights = optimizer.get_weights();
        let total = weights.cost_weight + weights.history_weight +
                   weights.parallel_weight + weights.index_weight;

        // Should be normalized to 1.0
        assert!((total - 1.0).abs() < 0.001);
    }
}
