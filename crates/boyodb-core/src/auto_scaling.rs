//! Auto-Scaling Policies
//!
//! Metrics-based scaling triggers and resource predictive scaling.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Scaling Configuration
// ============================================================================

/// Auto-scaling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalingConfig {
    /// Enable auto-scaling
    pub enabled: bool,
    /// Minimum number of nodes
    pub min_nodes: u32,
    /// Maximum number of nodes
    pub max_nodes: u32,
    /// Cooldown period after scale action (seconds)
    pub cooldown_secs: u64,
    /// Metrics evaluation period (seconds)
    pub evaluation_period_secs: u64,
    /// Enable predictive scaling
    pub predictive_scaling: bool,
    /// Scaling policies
    pub policies: Vec<ScalingPolicy>,
}

impl Default for AutoScalingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_nodes: 1,
            max_nodes: 10,
            cooldown_secs: 300,
            evaluation_period_secs: 60,
            predictive_scaling: false,
            policies: vec![
                ScalingPolicy::default_scale_out(),
                ScalingPolicy::default_scale_in(),
            ],
        }
    }
}

/// Scaling policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    /// Policy name
    pub name: String,
    /// Policy type
    pub policy_type: PolicyType,
    /// Metric to evaluate
    pub metric: ScalingMetric,
    /// Threshold value
    pub threshold: f64,
    /// Comparison operator
    pub operator: ComparisonOperator,
    /// Number of consecutive periods threshold must be breached
    pub datapoints_required: u32,
    /// Scaling action
    pub action: ScalingAction,
    /// Policy enabled
    pub enabled: bool,
}

impl ScalingPolicy {
    pub fn default_scale_out() -> Self {
        Self {
            name: "scale-out-cpu".to_string(),
            policy_type: PolicyType::TargetTracking,
            metric: ScalingMetric::CpuUtilization,
            threshold: 70.0,
            operator: ComparisonOperator::GreaterThan,
            datapoints_required: 3,
            action: ScalingAction::AddNodes { count: 1 },
            enabled: true,
        }
    }

    pub fn default_scale_in() -> Self {
        Self {
            name: "scale-in-cpu".to_string(),
            policy_type: PolicyType::TargetTracking,
            metric: ScalingMetric::CpuUtilization,
            threshold: 30.0,
            operator: ComparisonOperator::LessThan,
            datapoints_required: 5,
            action: ScalingAction::RemoveNodes { count: 1 },
            enabled: true,
        }
    }
}

/// Policy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Simple threshold-based
    SimpleScaling,
    /// Step scaling with multiple thresholds
    StepScaling,
    /// Target tracking (maintain target metric value)
    TargetTracking,
    /// Scheduled scaling
    Scheduled,
    /// Predictive scaling
    Predictive,
}

/// Scaling metric
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalingMetric {
    CpuUtilization,
    MemoryUtilization,
    DiskUtilization,
    NetworkIn,
    NetworkOut,
    ConnectionCount,
    QueryQueueLength,
    QueryLatencyP99,
    QueriesPerSecond,
    ActiveTransactions,
    ReplicationLag,
    CustomMetric,
}

/// Comparison operator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
}

impl ComparisonOperator {
    fn evaluate(&self, value: f64, threshold: f64) -> bool {
        match self {
            ComparisonOperator::LessThan => value < threshold,
            ComparisonOperator::LessThanOrEqual => value <= threshold,
            ComparisonOperator::GreaterThan => value > threshold,
            ComparisonOperator::GreaterThanOrEqual => value >= threshold,
            ComparisonOperator::Equal => (value - threshold).abs() < 0.001,
        }
    }
}

/// Scaling action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalingAction {
    AddNodes { count: u32 },
    RemoveNodes { count: u32 },
    SetNodeCount { count: u32 },
    AddCapacityPercent { percent: f64 },
    RemoveCapacityPercent { percent: f64 },
}

// ============================================================================
// Metrics Collection
// ============================================================================

/// Metric data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDataPoint {
    pub metric: ScalingMetric,
    pub value: f64,
    pub timestamp: u64,
    pub node_id: Option<String>,
}

/// Metrics aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricAggregation {
    Average,
    Maximum,
    Minimum,
    Sum,
    P50,
    P90,
    P99,
}

/// Metrics collector
pub struct MetricsCollector {
    /// Historical metrics per metric type
    metrics: RwLock<HashMap<ScalingMetric, VecDeque<MetricDataPoint>>>,
    /// Maximum history to keep
    max_history: usize,
    /// Collection interval
    collection_interval_secs: u64,
}

impl MetricsCollector {
    pub fn new(max_history: usize) -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
            max_history,
            collection_interval_secs: 10,
        }
    }

    /// Record a metric
    pub fn record(&self, metric: ScalingMetric, value: f64, node_id: Option<String>) {
        let data_point = MetricDataPoint {
            metric,
            value,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_id,
        };

        let mut metrics = self.metrics.write();
        let history = metrics.entry(metric).or_insert_with(VecDeque::new);

        history.push_back(data_point);

        // Prune old entries
        while history.len() > self.max_history {
            history.pop_front();
        }
    }

    /// Get recent metrics for a given metric type
    pub fn get_recent(&self, metric: ScalingMetric, duration_secs: u64) -> Vec<MetricDataPoint> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now.saturating_sub(duration_secs);

        let metrics = self.metrics.read();
        metrics
            .get(&metric)
            .map(|h| {
                h.iter()
                    .filter(|p| p.timestamp >= cutoff)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Aggregate metrics
    pub fn aggregate(
        &self,
        metric: ScalingMetric,
        duration_secs: u64,
        aggregation: MetricAggregation,
    ) -> Option<f64> {
        let points = self.get_recent(metric, duration_secs);
        if points.is_empty() {
            return None;
        }

        let values: Vec<f64> = points.iter().map(|p| p.value).collect();

        Some(match aggregation {
            MetricAggregation::Average => values.iter().sum::<f64>() / values.len() as f64,
            MetricAggregation::Maximum => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            MetricAggregation::Minimum => values.iter().cloned().fold(f64::INFINITY, f64::min),
            MetricAggregation::Sum => values.iter().sum(),
            MetricAggregation::P50 => percentile(&values, 0.50),
            MetricAggregation::P90 => percentile(&values, 0.90),
            MetricAggregation::P99 => percentile(&values, 0.99),
        })
    }

    /// Get all current metric averages
    pub fn current_metrics(&self) -> HashMap<ScalingMetric, f64> {
        let mut result = HashMap::new();
        let metrics = self.metrics.read();

        for (metric, history) in metrics.iter() {
            if !history.is_empty() {
                let avg = history.iter().map(|p| p.value).sum::<f64>() / history.len() as f64;
                result.insert(*metric, avg);
            }
        }

        result
    }
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let idx = (p * (sorted.len() - 1) as f64) as usize;
    sorted[idx]
}

// ============================================================================
// Predictive Scaling
// ============================================================================

/// Predictive scaling engine
pub struct PredictiveScaler {
    /// Historical capacity data
    capacity_history: RwLock<VecDeque<CapacityDataPoint>>,
    /// Forecast horizon (hours)
    forecast_horizon_hours: u32,
    /// Seasonality patterns
    seasonality: RwLock<SeasonalityModel>,
}

#[derive(Debug, Clone)]
struct CapacityDataPoint {
    timestamp: u64,
    node_count: u32,
    utilization: f64,
}

#[derive(Debug, Clone, Default)]
struct SeasonalityModel {
    /// Hourly patterns (24 hours)
    hourly_factors: [f64; 24],
    /// Daily patterns (7 days)
    daily_factors: [f64; 7],
    /// Base load
    base_load: f64,
}

impl PredictiveScaler {
    pub fn new() -> Self {
        Self {
            capacity_history: RwLock::new(VecDeque::new()),
            forecast_horizon_hours: 24,
            seasonality: RwLock::new(SeasonalityModel::default()),
        }
    }

    /// Record capacity data point
    pub fn record_capacity(&self, node_count: u32, utilization: f64) {
        let data_point = CapacityDataPoint {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_count,
            utilization,
        };

        let mut history = self.capacity_history.write();
        history.push_back(data_point);

        // Keep last 7 days
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 7 * 24 * 3600;

        while history
            .front()
            .map(|p| p.timestamp < cutoff)
            .unwrap_or(false)
        {
            history.pop_front();
        }
    }

    /// Forecast capacity needs
    pub fn forecast(&self, hours_ahead: u32) -> Vec<CapacityForecast> {
        let history = self.capacity_history.read();
        let seasonality = self.seasonality.read();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut forecasts = Vec::new();

        for hour in 0..hours_ahead {
            let future_time = now + hour as u64 * 3600;

            // Calculate hour of day and day of week
            let hour_of_day = ((future_time / 3600) % 24) as usize;
            let day_of_week = ((future_time / 86400) % 7) as usize;

            // Apply seasonality factors
            let hourly_factor = seasonality.hourly_factors[hour_of_day];
            let daily_factor = seasonality.daily_factors[day_of_week];

            // Base prediction on recent history
            let recent_avg = if !history.is_empty() {
                history.iter().map(|p| p.utilization).sum::<f64>() / history.len() as f64
            } else {
                50.0 // Default
            };

            let predicted_utilization = recent_avg * hourly_factor * daily_factor;
            let predicted_nodes = self.utilization_to_nodes(predicted_utilization, &history);

            forecasts.push(CapacityForecast {
                timestamp: future_time,
                predicted_utilization,
                recommended_nodes: predicted_nodes,
                confidence: self.calculate_confidence(&history),
            });
        }

        forecasts
    }

    fn utilization_to_nodes(&self, utilization: f64, history: &VecDeque<CapacityDataPoint>) -> u32 {
        // Simple calculation based on target 70% utilization
        let target_utilization = 70.0;

        let current_nodes = history.back().map(|p| p.node_count).unwrap_or(1);

        let needed = (utilization / target_utilization * current_nodes as f64).ceil() as u32;
        needed.max(1)
    }

    fn calculate_confidence(&self, history: &VecDeque<CapacityDataPoint>) -> f64 {
        // Confidence based on history size
        let min_history = 24 * 7; // 1 week of hourly data
        let confidence = (history.len() as f64 / min_history as f64).min(1.0);
        confidence * 0.95 // Max 95% confidence
    }

    /// Train seasonality model from history
    pub fn train_model(&self) {
        let history = self.capacity_history.read();
        let mut seasonality = self.seasonality.write();

        // Reset
        seasonality.hourly_factors = [1.0; 24];
        seasonality.daily_factors = [1.0; 7];

        if history.len() < 24 {
            return;
        }

        // Calculate average utilization
        let avg: f64 = history.iter().map(|p| p.utilization).sum::<f64>() / history.len() as f64;
        seasonality.base_load = avg;

        // Calculate hourly factors
        for hour in 0..24 {
            let hourly_points: Vec<_> = history
                .iter()
                .filter(|p| ((p.timestamp / 3600) % 24) as usize == hour)
                .collect();

            if !hourly_points.is_empty() {
                let hourly_avg = hourly_points.iter().map(|p| p.utilization).sum::<f64>()
                    / hourly_points.len() as f64;
                seasonality.hourly_factors[hour] = if avg > 0.0 { hourly_avg / avg } else { 1.0 };
            }
        }

        // Calculate daily factors
        for day in 0..7 {
            let daily_points: Vec<_> = history
                .iter()
                .filter(|p| ((p.timestamp / 86400) % 7) as usize == day)
                .collect();

            if !daily_points.is_empty() {
                let daily_avg = daily_points.iter().map(|p| p.utilization).sum::<f64>()
                    / daily_points.len() as f64;
                seasonality.daily_factors[day] = if avg > 0.0 { daily_avg / avg } else { 1.0 };
            }
        }
    }
}

impl Default for PredictiveScaler {
    fn default() -> Self {
        Self::new()
    }
}

/// Capacity forecast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    pub timestamp: u64,
    pub predicted_utilization: f64,
    pub recommended_nodes: u32,
    pub confidence: f64,
}

// ============================================================================
// Scaling Decision Engine
// ============================================================================

/// Scaling decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingDecision {
    pub decision_id: String,
    pub timestamp: u64,
    pub action: ScalingAction,
    pub triggered_by: String,
    pub current_nodes: u32,
    pub target_nodes: u32,
    pub metrics_snapshot: HashMap<ScalingMetric, f64>,
    pub reason: String,
}

/// Scaling event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingEvent {
    pub event_id: String,
    pub timestamp: u64,
    pub event_type: ScalingEventType,
    pub decision: Option<ScalingDecision>,
    pub previous_nodes: u32,
    pub new_nodes: u32,
    pub duration_secs: u64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalingEventType {
    ScaleOut,
    ScaleIn,
    Manual,
    Scheduled,
    Predictive,
}

/// Auto-scaling manager
pub struct AutoScalingManager {
    config: RwLock<AutoScalingConfig>,
    /// Current node count
    current_nodes: AtomicU64,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Predictive scaler
    predictive: Arc<PredictiveScaler>,
    /// Policy evaluation state (consecutive breaches)
    policy_states: RwLock<HashMap<String, u32>>,
    /// Last scaling time
    last_scaling_time: RwLock<Option<Instant>>,
    /// Scaling history
    history: RwLock<Vec<ScalingEvent>>,
    /// Running state
    running: AtomicBool,
    /// Maximum scaling events to keep in history
    max_history_size: usize,
}

impl AutoScalingManager {
    pub fn new(config: AutoScalingConfig) -> Self {
        let initial_nodes = config.min_nodes;

        Self {
            config: RwLock::new(config),
            current_nodes: AtomicU64::new(initial_nodes as u64),
            metrics: Arc::new(MetricsCollector::new(1000)),
            predictive: Arc::new(PredictiveScaler::new()),
            policy_states: RwLock::new(HashMap::new()),
            last_scaling_time: RwLock::new(None),
            history: RwLock::new(Vec::new()),
            running: AtomicBool::new(false),
            max_history_size: 1000, // Keep last 1000 scaling events
        }
    }

    /// Start auto-scaling
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop auto-scaling
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Update configuration
    pub fn configure(&self, config: AutoScalingConfig) {
        *self.config.write() = config;
    }

    /// Record a metric
    pub fn record_metric(&self, metric: ScalingMetric, value: f64) {
        self.metrics.record(metric, value, None);

        // Also feed predictive scaler
        if metric == ScalingMetric::CpuUtilization {
            self.predictive
                .record_capacity(self.current_nodes.load(Ordering::Relaxed) as u32, value);
        }
    }

    /// Evaluate policies and make scaling decision
    pub fn evaluate(&self) -> Option<ScalingDecision> {
        if !self.running.load(Ordering::Relaxed) {
            return None;
        }

        let config = self.config.read();
        if !config.enabled {
            return None;
        }

        // Check cooldown
        if self.in_cooldown(&config) {
            return None;
        }

        let current = self.current_nodes.load(Ordering::Relaxed) as u32;
        let metrics_snapshot = self.metrics.current_metrics();

        // Evaluate predictive scaling first
        if config.predictive_scaling {
            if let Some(decision) = self.evaluate_predictive(&config, current, &metrics_snapshot) {
                return Some(decision);
            }
        }

        // Evaluate each policy
        for policy in &config.policies {
            if !policy.enabled {
                continue;
            }

            if let Some(decision) = self.evaluate_policy(policy, current, &metrics_snapshot) {
                return Some(decision);
            }
        }

        None
    }

    fn in_cooldown(&self, config: &AutoScalingConfig) -> bool {
        if let Some(last_time) = *self.last_scaling_time.read() {
            return last_time.elapsed() < Duration::from_secs(config.cooldown_secs);
        }
        false
    }

    fn evaluate_policy(
        &self,
        policy: &ScalingPolicy,
        current_nodes: u32,
        metrics_snapshot: &HashMap<ScalingMetric, f64>,
    ) -> Option<ScalingDecision> {
        let config = self.config.read();

        // Get metric value
        let metric_value = self.metrics.aggregate(
            policy.metric,
            config.evaluation_period_secs,
            MetricAggregation::Average,
        )?;

        // Check threshold
        let breached = policy.operator.evaluate(metric_value, policy.threshold);

        // Update policy state
        let mut states = self.policy_states.write();
        let consecutive = states.entry(policy.name.clone()).or_insert(0);

        if breached {
            *consecutive += 1;
        } else {
            *consecutive = 0;
            return None;
        }

        // Check if we have enough consecutive breaches
        if *consecutive < policy.datapoints_required {
            return None;
        }

        // Reset counter
        *consecutive = 0;

        // Calculate target nodes
        let target_nodes = self.calculate_target_nodes(&policy.action, current_nodes, &config);

        if target_nodes == current_nodes {
            return None;
        }

        Some(ScalingDecision {
            decision_id: generate_decision_id(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            action: policy.action.clone(),
            triggered_by: policy.name.clone(),
            current_nodes,
            target_nodes,
            metrics_snapshot: metrics_snapshot.clone(),
            reason: format!(
                "Policy '{}' triggered: {} {} {}",
                policy.name,
                policy.metric.name(),
                policy.operator.symbol(),
                policy.threshold
            ),
        })
    }

    fn evaluate_predictive(
        &self,
        config: &AutoScalingConfig,
        current_nodes: u32,
        metrics_snapshot: &HashMap<ScalingMetric, f64>,
    ) -> Option<ScalingDecision> {
        let forecasts = self.predictive.forecast(2); // 2 hours ahead

        if forecasts.is_empty() {
            return None;
        }

        // Check if we need to scale for upcoming demand
        let max_recommended = forecasts
            .iter()
            .map(|f| f.recommended_nodes)
            .max()
            .unwrap_or(current_nodes);

        if max_recommended > current_nodes && max_recommended <= config.max_nodes {
            return Some(ScalingDecision {
                decision_id: generate_decision_id(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                action: ScalingAction::SetNodeCount {
                    count: max_recommended,
                },
                triggered_by: "predictive-scaling".to_string(),
                current_nodes,
                target_nodes: max_recommended,
                metrics_snapshot: metrics_snapshot.clone(),
                reason: format!(
                    "Predictive scaling: anticipated demand requires {} nodes",
                    max_recommended
                ),
            });
        }

        None
    }

    fn calculate_target_nodes(
        &self,
        action: &ScalingAction,
        current: u32,
        config: &AutoScalingConfig,
    ) -> u32 {
        let target = match action {
            ScalingAction::AddNodes { count } => current.saturating_add(*count),
            ScalingAction::RemoveNodes { count } => current.saturating_sub(*count),
            ScalingAction::SetNodeCount { count } => *count,
            ScalingAction::AddCapacityPercent { percent } => {
                (current as f64 * (1.0 + percent / 100.0)).ceil() as u32
            }
            ScalingAction::RemoveCapacityPercent { percent } => {
                (current as f64 * (1.0 - percent / 100.0)).floor() as u32
            }
        };

        target.clamp(config.min_nodes, config.max_nodes)
    }

    /// Execute scaling decision
    pub fn execute(&self, decision: ScalingDecision) -> Result<ScalingEvent, ScalingError> {
        let start = Instant::now();
        let previous_nodes = self.current_nodes.load(Ordering::Relaxed) as u32;

        // Validate
        let config = self.config.read();
        if decision.target_nodes < config.min_nodes || decision.target_nodes > config.max_nodes {
            return Err(ScalingError::NodeCountOutOfRange {
                target: decision.target_nodes,
                min: config.min_nodes,
                max: config.max_nodes,
            });
        }
        drop(config);

        // Execute scaling (simplified - in production would call cloud API)
        self.current_nodes
            .store(decision.target_nodes as u64, Ordering::Relaxed);

        // Update last scaling time
        *self.last_scaling_time.write() = Some(Instant::now());

        let event_type = if decision.target_nodes > previous_nodes {
            ScalingEventType::ScaleOut
        } else {
            ScalingEventType::ScaleIn
        };

        let event = ScalingEvent {
            event_id: generate_event_id(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            event_type,
            decision: Some(decision),
            previous_nodes,
            new_nodes: self.current_nodes.load(Ordering::Relaxed) as u32,
            duration_secs: start.elapsed().as_secs(),
            success: true,
            error: None,
        };

        // Record history with bounded size
        {
            let mut history = self.history.write();
            history.push(event.clone());
            if history.len() > self.max_history_size {
                let excess = history.len() - self.max_history_size;
                history.drain(0..excess);
            }
        }

        Ok(event)
    }

    /// Manual scale
    pub fn scale_to(&self, node_count: u32) -> Result<ScalingEvent, ScalingError> {
        let config = self.config.read();
        if node_count < config.min_nodes || node_count > config.max_nodes {
            return Err(ScalingError::NodeCountOutOfRange {
                target: node_count,
                min: config.min_nodes,
                max: config.max_nodes,
            });
        }
        drop(config);

        let previous = self.current_nodes.load(Ordering::Relaxed) as u32;
        self.current_nodes
            .store(node_count as u64, Ordering::Relaxed);

        let event = ScalingEvent {
            event_id: generate_event_id(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            event_type: ScalingEventType::Manual,
            decision: None,
            previous_nodes: previous,
            new_nodes: node_count,
            duration_secs: 0,
            success: true,
            error: None,
        };

        {
            let mut history = self.history.write();
            history.push(event.clone());
            if history.len() > self.max_history_size {
                let excess = history.len() - self.max_history_size;
                history.drain(0..excess);
            }
        }
        Ok(event)
    }

    /// Cleanup stale policy states for removed policies
    pub fn cleanup_policy_states(&self) {
        let config = self.config.read();
        let policy_names: std::collections::HashSet<_> =
            config.policies.iter().map(|p| p.name.clone()).collect();
        drop(config);

        self.policy_states
            .write()
            .retain(|name, _| policy_names.contains(name));
    }

    /// Get current node count
    pub fn current_nodes(&self) -> u32 {
        self.current_nodes.load(Ordering::Relaxed) as u32
    }

    /// Get scaling history
    pub fn history(&self) -> Vec<ScalingEvent> {
        self.history.read().clone()
    }

    /// Get capacity forecast
    pub fn forecast(&self, hours: u32) -> Vec<CapacityForecast> {
        self.predictive.forecast(hours)
    }

    /// Train predictive model
    pub fn train_predictive_model(&self) {
        self.predictive.train_model();
    }

    /// Get current status
    pub fn status(&self) -> AutoScalingStatus {
        let config = self.config.read();

        AutoScalingStatus {
            enabled: config.enabled,
            current_nodes: self.current_nodes(),
            min_nodes: config.min_nodes,
            max_nodes: config.max_nodes,
            running: self.running.load(Ordering::Relaxed),
            in_cooldown: self.in_cooldown(&config),
            last_scaling_event: self.history.read().last().cloned(),
            metrics: self.metrics.current_metrics(),
        }
    }
}

impl ScalingMetric {
    fn name(&self) -> &'static str {
        match self {
            ScalingMetric::CpuUtilization => "cpu_utilization",
            ScalingMetric::MemoryUtilization => "memory_utilization",
            ScalingMetric::DiskUtilization => "disk_utilization",
            ScalingMetric::NetworkIn => "network_in",
            ScalingMetric::NetworkOut => "network_out",
            ScalingMetric::ConnectionCount => "connection_count",
            ScalingMetric::QueryQueueLength => "query_queue_length",
            ScalingMetric::QueryLatencyP99 => "query_latency_p99",
            ScalingMetric::QueriesPerSecond => "queries_per_second",
            ScalingMetric::ActiveTransactions => "active_transactions",
            ScalingMetric::ReplicationLag => "replication_lag",
            ScalingMetric::CustomMetric => "custom",
        }
    }
}

impl ComparisonOperator {
    fn symbol(&self) -> &'static str {
        match self {
            ComparisonOperator::LessThan => "<",
            ComparisonOperator::LessThanOrEqual => "<=",
            ComparisonOperator::GreaterThan => ">",
            ComparisonOperator::GreaterThanOrEqual => ">=",
            ComparisonOperator::Equal => "==",
        }
    }
}

/// Auto-scaling status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalingStatus {
    pub enabled: bool,
    pub current_nodes: u32,
    pub min_nodes: u32,
    pub max_nodes: u32,
    pub running: bool,
    pub in_cooldown: bool,
    pub last_scaling_event: Option<ScalingEvent>,
    pub metrics: HashMap<ScalingMetric, f64>,
}

/// Scaling errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalingError {
    NodeCountOutOfRange { target: u32, min: u32, max: u32 },
    ScalingInProgress,
    InCooldown { remaining_secs: u64 },
    ProviderError(String),
}

impl std::fmt::Display for ScalingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalingError::NodeCountOutOfRange { target, min, max } => {
                write!(f, "Node count {} out of range [{}, {}]", target, min, max)
            }
            ScalingError::ScalingInProgress => write!(f, "Scaling operation already in progress"),
            ScalingError::InCooldown { remaining_secs } => {
                write!(f, "In cooldown, {} seconds remaining", remaining_secs)
            }
            ScalingError::ProviderError(e) => write!(f, "Provider error: {}", e),
        }
    }
}

impl std::error::Error for ScalingError {}

// Helper functions
fn generate_decision_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("dec_{:016x}", hasher.finish())
}

fn generate_event_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("evt_{:016x}", hasher.finish())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collection() {
        let collector = MetricsCollector::new(100);

        for i in 0..10 {
            collector.record(ScalingMetric::CpuUtilization, 50.0 + i as f64, None);
        }

        let avg = collector.aggregate(
            ScalingMetric::CpuUtilization,
            3600,
            MetricAggregation::Average,
        );
        assert!(avg.is_some());
        assert!((avg.unwrap() - 54.5).abs() < 0.1);
    }

    #[test]
    fn test_policy_evaluation() {
        let config = AutoScalingConfig {
            enabled: true,
            min_nodes: 1,
            max_nodes: 10,
            cooldown_secs: 0,
            evaluation_period_secs: 60,
            predictive_scaling: false,
            policies: vec![ScalingPolicy {
                name: "test-scale-out".to_string(),
                policy_type: PolicyType::SimpleScaling,
                metric: ScalingMetric::CpuUtilization,
                threshold: 70.0,
                operator: ComparisonOperator::GreaterThan,
                datapoints_required: 1,
                action: ScalingAction::AddNodes { count: 1 },
                enabled: true,
            }],
        };

        let manager = AutoScalingManager::new(config);
        manager.start();

        // Record high CPU
        manager.record_metric(ScalingMetric::CpuUtilization, 80.0);

        let decision = manager.evaluate();
        assert!(decision.is_some());
    }

    #[test]
    fn test_manual_scaling() {
        let config = AutoScalingConfig::default();
        let manager = AutoScalingManager::new(config);

        let result = manager.scale_to(5);
        assert!(result.is_ok());
        assert_eq!(manager.current_nodes(), 5);
    }

    #[test]
    fn test_predictive_scaler() {
        let scaler = PredictiveScaler::new();

        // Record some history
        for i in 0..100 {
            scaler.record_capacity(3, 50.0 + (i % 20) as f64);
        }

        scaler.train_model();

        let forecasts = scaler.forecast(24);
        assert_eq!(forecasts.len(), 24);
    }
}
