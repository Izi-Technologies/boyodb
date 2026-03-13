//! Model Monitoring for BoyoDB
//!
//! Tracks ML model performance and data drift:
//! - Prediction drift detection (PSI, KS test)
//! - Data drift detection (feature distributions)
//! - Model performance metrics over time
//! - Alerting on degradation

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Statistical test types for drift detection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DriftTest {
    /// Population Stability Index
    PSI,
    /// Kolmogorov-Smirnov test
    KolmogorovSmirnov,
    /// Chi-squared test (for categorical)
    ChiSquared,
    /// Jensen-Shannon divergence
    JensenShannon,
    /// Wasserstein distance
    Wasserstein,
}

/// Drift severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DriftSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
}

impl DriftSeverity {
    pub fn from_psi(psi: f64) -> Self {
        if psi < 0.1 {
            DriftSeverity::None
        } else if psi < 0.2 {
            DriftSeverity::Low
        } else if psi < 0.3 {
            DriftSeverity::Medium
        } else if psi < 0.5 {
            DriftSeverity::High
        } else {
            DriftSeverity::Critical
        }
    }
}

/// Drift detection result
#[derive(Debug, Clone)]
pub struct DriftResult {
    /// Feature name
    pub feature: String,
    /// Test used
    pub test: DriftTest,
    /// Test statistic value
    pub statistic: f64,
    /// P-value if applicable
    pub p_value: Option<f64>,
    /// Drift severity
    pub severity: DriftSeverity,
    /// Is drift detected
    pub drift_detected: bool,
    /// Reference distribution stats
    pub reference_stats: DistributionStats,
    /// Current distribution stats
    pub current_stats: DistributionStats,
    /// Timestamp
    pub timestamp: i64,
}

/// Distribution statistics
#[derive(Debug, Clone, Default)]
pub struct DistributionStats {
    pub count: u64,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub p25: f64,
    pub p75: f64,
    pub histogram: Vec<(f64, u64)>, // (bin_edge, count)
}

/// Model performance metrics
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Model name
    pub model_name: String,
    /// Model version
    pub model_version: String,
    /// Timestamp window
    pub window_start: i64,
    pub window_end: i64,
    /// Total predictions
    pub total_predictions: u64,
    /// Classification metrics (if applicable)
    pub accuracy: Option<f64>,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
    pub f1_score: Option<f64>,
    pub auc_roc: Option<f64>,
    pub log_loss: Option<f64>,
    /// Regression metrics (if applicable)
    pub mae: Option<f64>,
    pub mse: Option<f64>,
    pub rmse: Option<f64>,
    pub r2_score: Option<f64>,
    pub mape: Option<f64>,
    /// Latency metrics
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
}

/// Prediction log entry
#[derive(Debug, Clone)]
pub struct PredictionLog {
    /// Unique prediction ID
    pub prediction_id: String,
    /// Model name
    pub model_name: String,
    /// Model version
    pub model_version: String,
    /// Input features
    pub features: HashMap<String, f64>,
    /// Prediction output
    pub prediction: f64,
    /// Prediction probabilities (for classification)
    pub probabilities: Option<Vec<f64>>,
    /// Ground truth (when available)
    pub ground_truth: Option<f64>,
    /// Prediction timestamp
    pub timestamp: i64,
    /// Latency in milliseconds
    pub latency_ms: f64,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Alert name
    pub name: String,
    /// Metric to monitor
    pub metric: MonitoredMetric,
    /// Threshold value
    pub threshold: f64,
    /// Comparison operator
    pub operator: ComparisonOp,
    /// Minimum samples before alerting
    pub min_samples: u64,
    /// Cooldown period in seconds
    pub cooldown_seconds: u64,
    /// Alert channels
    pub channels: Vec<AlertChannel>,
    /// Is alert enabled
    pub enabled: bool,
}

/// Metrics that can be monitored
#[derive(Debug, Clone)]
pub enum MonitoredMetric {
    Accuracy,
    Precision,
    Recall,
    F1Score,
    AucRoc,
    Mae,
    Mse,
    Rmse,
    R2Score,
    LatencyP95,
    LatencyP99,
    DriftPsi(String),      // Feature name
    PredictionRate,
    ErrorRate,
}

/// Comparison operators
#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
    Equal,
    NotEqual,
}

impl ComparisonOp {
    pub fn evaluate(&self, value: f64, threshold: f64) -> bool {
        match self {
            ComparisonOp::LessThan => value < threshold,
            ComparisonOp::LessOrEqual => value <= threshold,
            ComparisonOp::GreaterThan => value > threshold,
            ComparisonOp::GreaterOrEqual => value >= threshold,
            ComparisonOp::Equal => (value - threshold).abs() < f64::EPSILON,
            ComparisonOp::NotEqual => (value - threshold).abs() >= f64::EPSILON,
        }
    }
}

/// Alert channels
#[derive(Debug, Clone)]
pub enum AlertChannel {
    Webhook(String),
    Email(String),
    Slack(String),
    PagerDuty(String),
    Log,
}

/// Alert event
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert ID
    pub id: u64,
    /// Alert name
    pub name: String,
    /// Model name
    pub model_name: String,
    /// Metric that triggered alert
    pub metric: String,
    /// Current value
    pub value: f64,
    /// Threshold
    pub threshold: f64,
    /// Message
    pub message: String,
    /// Severity
    pub severity: AlertSeverity,
    /// Timestamp
    pub timestamp: i64,
    /// Is acknowledged
    pub acknowledged: bool,
}

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Reference distribution for a feature
#[derive(Debug, Clone)]
pub struct ReferenceDistribution {
    /// Feature name
    pub feature: String,
    /// Histogram bins
    pub bins: Vec<f64>,
    /// Bin counts (normalized to proportions)
    pub proportions: Vec<f64>,
    /// Statistics
    pub stats: DistributionStats,
    /// Sample count used
    pub sample_count: u64,
    /// Creation timestamp
    pub created_at: i64,
}

/// Model Monitor
pub struct ModelMonitor {
    /// Model name
    model_name: String,
    /// Reference distributions per feature
    reference_distributions: RwLock<HashMap<String, ReferenceDistribution>>,
    /// Recent predictions buffer
    predictions: RwLock<Vec<PredictionLog>>,
    /// Max predictions to keep in memory
    max_predictions: usize,
    /// Performance metrics history
    metrics_history: RwLock<Vec<PerformanceMetrics>>,
    /// Alert configurations
    alerts: RwLock<Vec<AlertConfig>>,
    /// Triggered alerts
    triggered_alerts: RwLock<Vec<Alert>>,
    /// Alert counter
    alert_counter: AtomicU64,
    /// Statistics
    stats: MonitorStats,
}

struct MonitorStats {
    total_predictions: AtomicU64,
    drift_checks: AtomicU64,
    alerts_triggered: AtomicU64,
}

impl ModelMonitor {
    /// Create a new model monitor
    pub fn new(model_name: &str, max_predictions: usize) -> Self {
        Self {
            model_name: model_name.to_string(),
            reference_distributions: RwLock::new(HashMap::new()),
            predictions: RwLock::new(Vec::new()),
            max_predictions,
            metrics_history: RwLock::new(Vec::new()),
            alerts: RwLock::new(Vec::new()),
            triggered_alerts: RwLock::new(Vec::new()),
            alert_counter: AtomicU64::new(1),
            stats: MonitorStats {
                total_predictions: AtomicU64::new(0),
                drift_checks: AtomicU64::new(0),
                alerts_triggered: AtomicU64::new(0),
            },
        }
    }

    /// Set reference distribution for a feature
    pub fn set_reference(&self, feature: &str, values: &[f64], num_bins: usize) {
        let stats = self.compute_stats(values);
        let (bins, proportions) = self.compute_histogram(values, num_bins);

        let reference = ReferenceDistribution {
            feature: feature.to_string(),
            bins,
            proportions,
            stats,
            sample_count: values.len() as u64,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        };

        self.reference_distributions
            .write()
            .unwrap()
            .insert(feature.to_string(), reference);
    }

    /// Log a prediction
    pub fn log_prediction(&self, log: PredictionLog) {
        self.stats.total_predictions.fetch_add(1, Ordering::Relaxed);

        let mut predictions = self.predictions.write().unwrap();
        predictions.push(log);

        // Evict old predictions if over limit
        if predictions.len() > self.max_predictions {
            predictions.remove(0);
        }
    }

    /// Check for data drift on a feature
    pub fn check_drift(&self, feature: &str, current_values: &[f64]) -> Option<DriftResult> {
        self.stats.drift_checks.fetch_add(1, Ordering::Relaxed);

        let refs = self.reference_distributions.read().unwrap();
        let reference = refs.get(feature)?;

        let current_stats = self.compute_stats(current_values);
        let (_, current_proportions) = self.compute_histogram_with_bins(current_values, &reference.bins);

        // Calculate PSI
        let psi = self.calculate_psi(&reference.proportions, &current_proportions);
        let severity = DriftSeverity::from_psi(psi);

        Some(DriftResult {
            feature: feature.to_string(),
            test: DriftTest::PSI,
            statistic: psi,
            p_value: None,
            severity,
            drift_detected: severity >= DriftSeverity::Medium,
            reference_stats: reference.stats.clone(),
            current_stats,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        })
    }

    /// Check all features for drift
    pub fn check_all_drift(&self) -> Vec<DriftResult> {
        let predictions = self.predictions.read().unwrap();
        let refs = self.reference_distributions.read().unwrap();

        let mut results = Vec::new();

        for feature in refs.keys() {
            let values: Vec<f64> = predictions
                .iter()
                .filter_map(|p| p.features.get(feature).copied())
                .collect();

            if values.len() >= 100 {
                if let Some(result) = self.check_drift(feature, &values) {
                    results.push(result);
                }
            }
        }

        results
    }

    /// Calculate PSI (Population Stability Index)
    fn calculate_psi(&self, reference: &[f64], current: &[f64]) -> f64 {
        let epsilon = 1e-10;
        let mut psi = 0.0;

        for (r, c) in reference.iter().zip(current.iter()) {
            let r = r.max(epsilon);
            let c = c.max(epsilon);
            psi += (c - r) * (c / r).ln();
        }

        psi
    }

    /// Calculate KS statistic
    pub fn calculate_ks(&self, reference: &[f64], current: &[f64]) -> (f64, f64) {
        let mut ref_sorted = reference.to_vec();
        let mut cur_sorted = current.to_vec();
        ref_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        cur_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n1 = ref_sorted.len() as f64;
        let n2 = cur_sorted.len() as f64;

        let mut max_diff = 0.0f64;
        let mut i = 0;
        let mut j = 0;

        while i < ref_sorted.len() && j < cur_sorted.len() {
            let cdf1 = (i + 1) as f64 / n1;
            let cdf2 = (j + 1) as f64 / n2;
            let diff = (cdf1 - cdf2).abs();
            max_diff = max_diff.max(diff);

            if ref_sorted[i] <= cur_sorted[j] {
                i += 1;
            } else {
                j += 1;
            }
        }

        // Approximate p-value using asymptotic distribution
        let n = (n1 * n2) / (n1 + n2);
        let lambda = (n.sqrt() + 0.12 + 0.11 / n.sqrt()) * max_diff;
        let p_value = 2.0 * (-2.0 * lambda * lambda).exp();

        (max_diff, p_value.min(1.0))
    }

    /// Compute distribution statistics
    fn compute_stats(&self, values: &[f64]) -> DistributionStats {
        if values.is_empty() {
            return DistributionStats::default();
        }

        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let std = variance.sqrt();

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let median = sorted[sorted.len() / 2];
        let p25 = sorted[sorted.len() / 4];
        let p75 = sorted[3 * sorted.len() / 4];

        DistributionStats {
            count: values.len() as u64,
            mean,
            std,
            min,
            max,
            median,
            p25,
            p75,
            histogram: Vec::new(),
        }
    }

    /// Compute histogram
    fn compute_histogram(&self, values: &[f64], num_bins: usize) -> (Vec<f64>, Vec<f64>) {
        if values.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let bin_width = (max - min) / num_bins as f64;

        let bins: Vec<f64> = (0..=num_bins)
            .map(|i| min + i as f64 * bin_width)
            .collect();

        self.compute_histogram_with_bins(values, &bins)
    }

    /// Compute histogram with given bins
    fn compute_histogram_with_bins(&self, values: &[f64], bins: &[f64]) -> (Vec<f64>, Vec<f64>) {
        let mut counts = vec![0u64; bins.len().saturating_sub(1)];

        for &v in values {
            for i in 0..counts.len() {
                if v >= bins[i] && (i == counts.len() - 1 || v < bins[i + 1]) {
                    counts[i] += 1;
                    break;
                }
            }
        }

        let total = values.len() as f64;
        let proportions: Vec<f64> = counts.iter().map(|&c| c as f64 / total).collect();

        (bins.to_vec(), proportions)
    }

    /// Calculate performance metrics from predictions with ground truth
    pub fn calculate_metrics(&self, window_seconds: i64) -> Option<PerformanceMetrics> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let window_start = now - window_seconds * 1000;

        let predictions = self.predictions.read().unwrap();
        let window_preds: Vec<_> = predictions
            .iter()
            .filter(|p| p.timestamp >= window_start && p.ground_truth.is_some())
            .collect();

        if window_preds.is_empty() {
            return None;
        }

        let total = window_preds.len() as f64;

        // Calculate latency percentiles
        let mut latencies: Vec<f64> = window_preds.iter().map(|p| p.latency_ms).collect();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg_latency = latencies.iter().sum::<f64>() / total;
        let p50_latency = latencies[latencies.len() / 2];
        let p95_latency = latencies[(latencies.len() as f64 * 0.95) as usize];
        let p99_latency = latencies[(latencies.len() as f64 * 0.99) as usize];

        // Calculate regression metrics
        let predictions_vec: Vec<f64> = window_preds.iter().map(|p| p.prediction).collect();
        let actuals: Vec<f64> = window_preds.iter().map(|p| p.ground_truth.unwrap()).collect();

        let mae = predictions_vec
            .iter()
            .zip(actuals.iter())
            .map(|(p, a)| (p - a).abs())
            .sum::<f64>()
            / total;

        let mse = predictions_vec
            .iter()
            .zip(actuals.iter())
            .map(|(p, a)| (p - a).powi(2))
            .sum::<f64>()
            / total;

        let rmse = mse.sqrt();

        let actual_mean = actuals.iter().sum::<f64>() / total;
        let ss_tot: f64 = actuals.iter().map(|a| (a - actual_mean).powi(2)).sum();
        let ss_res: f64 = predictions_vec
            .iter()
            .zip(actuals.iter())
            .map(|(p, a)| (a - p).powi(2))
            .sum();
        let r2 = if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 };

        Some(PerformanceMetrics {
            model_name: self.model_name.clone(),
            model_version: String::new(),
            window_start,
            window_end: now,
            total_predictions: window_preds.len() as u64,
            accuracy: None,
            precision: None,
            recall: None,
            f1_score: None,
            auc_roc: None,
            log_loss: None,
            mae: Some(mae),
            mse: Some(mse),
            rmse: Some(rmse),
            r2_score: Some(r2),
            mape: None,
            avg_latency_ms: avg_latency,
            p50_latency_ms: p50_latency,
            p95_latency_ms: p95_latency,
            p99_latency_ms: p99_latency,
        })
    }

    /// Add an alert configuration
    pub fn add_alert(&self, config: AlertConfig) {
        self.alerts.write().unwrap().push(config);
    }

    /// Check alerts and trigger if needed
    pub fn check_alerts(&self, metrics: &PerformanceMetrics) -> Vec<Alert> {
        let alerts = self.alerts.read().unwrap();
        let mut triggered = Vec::new();

        for config in alerts.iter() {
            if !config.enabled {
                continue;
            }

            let value = match &config.metric {
                MonitoredMetric::Mae => metrics.mae,
                MonitoredMetric::Mse => metrics.mse,
                MonitoredMetric::Rmse => metrics.rmse,
                MonitoredMetric::R2Score => metrics.r2_score,
                MonitoredMetric::LatencyP95 => Some(metrics.p95_latency_ms),
                MonitoredMetric::LatencyP99 => Some(metrics.p99_latency_ms),
                _ => None,
            };

            if let Some(val) = value {
                if config.operator.evaluate(val, config.threshold) {
                    let alert = Alert {
                        id: self.alert_counter.fetch_add(1, Ordering::Relaxed),
                        name: config.name.clone(),
                        model_name: self.model_name.clone(),
                        metric: format!("{:?}", config.metric),
                        value: val,
                        threshold: config.threshold,
                        message: format!(
                            "Alert '{}': {} is {} (threshold: {})",
                            config.name, format!("{:?}", config.metric), val, config.threshold
                        ),
                        severity: AlertSeverity::Warning,
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64,
                        acknowledged: false,
                    };

                    self.stats.alerts_triggered.fetch_add(1, Ordering::Relaxed);
                    triggered.push(alert);
                }
            }
        }

        // Store triggered alerts
        self.triggered_alerts.write().unwrap().extend(triggered.clone());

        triggered
    }

    /// Get monitoring statistics
    pub fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_predictions.load(Ordering::Relaxed),
            self.stats.drift_checks.load(Ordering::Relaxed),
            self.stats.alerts_triggered.load(Ordering::Relaxed),
        )
    }

    /// Get triggered alerts
    pub fn get_alerts(&self, acknowledged: Option<bool>) -> Vec<Alert> {
        let alerts = self.triggered_alerts.read().unwrap();
        match acknowledged {
            Some(ack) => alerts.iter().filter(|a| a.acknowledged == ack).cloned().collect(),
            None => alerts.clone(),
        }
    }

    /// Acknowledge an alert
    pub fn acknowledge_alert(&self, alert_id: u64) -> bool {
        let mut alerts = self.triggered_alerts.write().unwrap();
        if let Some(alert) = alerts.iter_mut().find(|a| a.id == alert_id) {
            alert.acknowledged = true;
            true
        } else {
            false
        }
    }
}

/// Global monitoring registry
pub struct MonitoringRegistry {
    monitors: RwLock<HashMap<String, Arc<ModelMonitor>>>,
}

impl MonitoringRegistry {
    pub fn new() -> Self {
        Self {
            monitors: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, model_name: &str, max_predictions: usize) -> Arc<ModelMonitor> {
        let monitor = Arc::new(ModelMonitor::new(model_name, max_predictions));
        self.monitors
            .write()
            .unwrap()
            .insert(model_name.to_string(), Arc::clone(&monitor));
        monitor
    }

    pub fn get(&self, model_name: &str) -> Option<Arc<ModelMonitor>> {
        self.monitors.read().unwrap().get(model_name).cloned()
    }

    pub fn list(&self) -> Vec<String> {
        self.monitors.read().unwrap().keys().cloned().collect()
    }
}

impl Default for MonitoringRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_psi_calculation() {
        let monitor = ModelMonitor::new("test", 1000);

        // Set reference distribution
        let reference: Vec<f64> = (0..1000).map(|i| i as f64 / 100.0).collect();
        monitor.set_reference("feature1", &reference, 10);

        // Similar distribution - low drift
        let similar: Vec<f64> = (0..1000).map(|i| (i as f64 / 100.0) + 0.1).collect();
        let result = monitor.check_drift("feature1", &similar).unwrap();
        assert!(result.statistic < 0.2);

        // Different distribution - high drift
        let different: Vec<f64> = (0..1000).map(|i| (i as f64 / 50.0) + 5.0).collect();
        let result = monitor.check_drift("feature1", &different).unwrap();
        assert!(result.statistic > 0.2);
    }

    #[test]
    fn test_ks_statistic() {
        let monitor = ModelMonitor::new("test", 1000);

        let dist1: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let dist2: Vec<f64> = (0..100).map(|i| i as f64 + 10.0).collect();

        let (ks, p_value) = monitor.calculate_ks(&dist1, &dist2);
        assert!(ks > 0.0);
        assert!(p_value <= 1.0);
    }

    #[test]
    fn test_prediction_logging() {
        let monitor = ModelMonitor::new("test", 100);

        for i in 0..50 {
            let mut features = HashMap::new();
            features.insert("x".to_string(), i as f64);

            monitor.log_prediction(PredictionLog {
                prediction_id: format!("pred_{}", i),
                model_name: "test".to_string(),
                model_version: "1.0".to_string(),
                features,
                prediction: (i as f64) * 2.0,
                probabilities: None,
                ground_truth: Some((i as f64) * 2.0 + 1.0),
                timestamp: i as i64 * 1000,
                latency_ms: 10.0 + (i as f64 * 0.1),
            });
        }

        let (total, _, _) = monitor.get_stats();
        assert_eq!(total, 50);
    }

    #[test]
    fn test_alert_triggering() {
        let monitor = ModelMonitor::new("test", 100);

        monitor.add_alert(AlertConfig {
            name: "high_mae".to_string(),
            metric: MonitoredMetric::Mae,
            threshold: 1.0,
            operator: ComparisonOp::GreaterThan,
            min_samples: 10,
            cooldown_seconds: 300,
            channels: vec![AlertChannel::Log],
            enabled: true,
        });

        let metrics = PerformanceMetrics {
            model_name: "test".to_string(),
            model_version: "1.0".to_string(),
            window_start: 0,
            window_end: 1000,
            total_predictions: 100,
            accuracy: None,
            precision: None,
            recall: None,
            f1_score: None,
            auc_roc: None,
            log_loss: None,
            mae: Some(2.0), // Above threshold
            mse: None,
            rmse: None,
            r2_score: None,
            mape: None,
            avg_latency_ms: 10.0,
            p50_latency_ms: 8.0,
            p95_latency_ms: 15.0,
            p99_latency_ms: 20.0,
        };

        let alerts = monitor.check_alerts(&metrics);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].name, "high_mae");
    }

    #[test]
    fn test_drift_severity() {
        assert_eq!(DriftSeverity::from_psi(0.05), DriftSeverity::None);
        assert_eq!(DriftSeverity::from_psi(0.15), DriftSeverity::Low);
        assert_eq!(DriftSeverity::from_psi(0.25), DriftSeverity::Medium);
        assert_eq!(DriftSeverity::from_psi(0.4), DriftSeverity::High);
        assert_eq!(DriftSeverity::from_psi(0.6), DriftSeverity::Critical);
    }
}
