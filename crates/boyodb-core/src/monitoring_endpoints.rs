//! Monitoring Endpoints Module
//!
//! Provides health check endpoints, Prometheus metrics, and JSON metrics API.
//!
//! # Features
//! - /health endpoint for liveness checks
//! - /ready endpoint for readiness checks
//! - Prometheus metrics exporter
//! - JSON metrics API
//! - Custom metric registration
//!
//! # Example
//! ```text
//! GET /health -> {"status":"healthy","uptime_seconds":12345}
//! GET /ready -> {"status":"ready","connections":42,"lag_ms":100}
//! GET /metrics -> # Prometheus format
//! GET /api/v1/metrics -> JSON format
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Types
// ============================================================================

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// System is healthy
    Healthy,
    /// System is degraded but functional
    Degraded,
    /// System is unhealthy
    Unhealthy,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Readiness status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessStatus {
    /// Ready to accept traffic
    Ready,
    /// Not ready (e.g., still initializing)
    NotReady,
    /// Shutting down
    ShuttingDown,
}

impl std::fmt::Display for ReadinessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready => write!(f, "ready"),
            Self::NotReady => write!(f, "not_ready"),
            Self::ShuttingDown => write!(f, "shutting_down"),
        }
    }
}

// ============================================================================
// Health Check
// ============================================================================

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Overall status
    pub status: HealthStatus,
    /// Uptime in seconds
    pub uptime_seconds: u64,
    /// Version string
    pub version: String,
    /// Individual component checks
    pub components: HashMap<String, ComponentHealth>,
    /// Timestamp
    pub timestamp: SystemTime,
}

impl HealthCheckResult {
    /// Format as JSON
    pub fn to_json(&self) -> String {
        let components_json: Vec<String> = self
            .components
            .iter()
            .map(|(name, health)| {
                format!(
                    r#""{}":{{"status":"{}","message":"{}"}}"#,
                    name,
                    health.status,
                    health.message.as_deref().unwrap_or("")
                )
            })
            .collect();

        format!(
            r#"{{"status":"{}","uptime_seconds":{},"version":"{}","components":{{{}}}}}"#,
            self.status,
            self.uptime_seconds,
            self.version,
            components_json.join(",")
        )
    }

    /// HTTP status code for this result
    pub fn http_status_code(&self) -> u16 {
        match self.status {
            HealthStatus::Healthy => 200,
            HealthStatus::Degraded => 200,
            HealthStatus::Unhealthy => 503,
        }
    }
}

/// Component health status
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    /// Status
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
    /// Last check time
    pub last_check: SystemTime,
}

/// Health check callback type
pub type HealthCheckFn = Box<dyn Fn() -> ComponentHealth + Send + Sync>;

// ============================================================================
// Readiness Check
// ============================================================================

/// Readiness check result
#[derive(Debug, Clone)]
pub struct ReadinessCheckResult {
    /// Overall status
    pub status: ReadinessStatus,
    /// Active connections
    pub connections: u64,
    /// Replication lag in milliseconds (if replica)
    pub lag_ms: Option<u64>,
    /// Whether accepting connections
    pub accepting_connections: bool,
    /// Initialization progress (0-100)
    pub init_progress: Option<u8>,
    /// Timestamp
    pub timestamp: SystemTime,
}

impl ReadinessCheckResult {
    /// Format as JSON
    pub fn to_json(&self) -> String {
        let mut parts = vec![
            format!(r#""status":"{}""#, self.status),
            format!(r#""connections":{}"#, self.connections),
            format!(r#""accepting_connections":{}"#, self.accepting_connections),
        ];

        if let Some(lag) = self.lag_ms {
            parts.push(format!(r#""lag_ms":{}"#, lag));
        }

        if let Some(progress) = self.init_progress {
            parts.push(format!(r#""init_progress":{}"#, progress));
        }

        format!("{{{}}}", parts.join(","))
    }

    /// HTTP status code for this result
    pub fn http_status_code(&self) -> u16 {
        match self.status {
            ReadinessStatus::Ready => 200,
            ReadinessStatus::NotReady => 503,
            ReadinessStatus::ShuttingDown => 503,
        }
    }
}

// ============================================================================
// Prometheus Metrics
// ============================================================================

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Counter (monotonically increasing)
    Counter,
    /// Gauge (can go up and down)
    Gauge,
    /// Histogram (distribution of values)
    Histogram,
    /// Summary (similar to histogram)
    Summary,
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Counter => write!(f, "counter"),
            Self::Gauge => write!(f, "gauge"),
            Self::Histogram => write!(f, "histogram"),
            Self::Summary => write!(f, "summary"),
        }
    }
}

/// Metric definition
#[derive(Debug, Clone)]
pub struct MetricDef {
    /// Metric name
    pub name: String,
    /// Help text
    pub help: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Label names
    pub labels: Vec<String>,
}

/// Counter metric
pub struct Counter {
    /// Definition
    def: MetricDef,
    /// Values by label combination
    values: RwLock<HashMap<Vec<String>, AtomicU64>>,
}

impl Counter {
    /// Create a new counter
    pub fn new(name: &str, help: &str, labels: Vec<String>) -> Self {
        Self {
            def: MetricDef {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Counter,
                labels,
            },
            values: RwLock::new(HashMap::new()),
        }
    }

    /// Increment by 1
    pub fn inc(&self, labels: Vec<String>) {
        self.add(labels, 1);
    }

    /// Add a value
    pub fn add(&self, labels: Vec<String>, value: u64) {
        let values = self.values.read();
        if let Some(counter) = values.get(&labels) {
            counter.fetch_add(value, Ordering::Relaxed);
            return;
        }
        drop(values);

        let mut values = self.values.write();
        values
            .entry(labels)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(value, Ordering::Relaxed);
    }

    /// Get value
    pub fn get(&self, labels: &[String]) -> u64 {
        self.values
            .read()
            .get(labels)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Format as Prometheus
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("# HELP {} {}\n", self.def.name, self.def.help));
        output.push_str(&format!("# TYPE {} counter\n", self.def.name));

        let values = self.values.read();
        for (labels, value) in values.iter() {
            let label_str = self.format_labels(labels);
            output.push_str(&format!(
                "{}{} {}\n",
                self.def.name,
                label_str,
                value.load(Ordering::Relaxed)
            ));
        }

        output
    }

    fn format_labels(&self, labels: &[String]) -> String {
        if labels.is_empty() || self.def.labels.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = self
            .def
            .labels
            .iter()
            .zip(labels.iter())
            .map(|(name, value)| format!("{}=\"{}\"", name, value))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }
}

/// Gauge metric
pub struct Gauge {
    /// Definition
    def: MetricDef,
    /// Values by label combination
    values: RwLock<HashMap<Vec<String>, AtomicI64>>,
}

impl Gauge {
    /// Create a new gauge
    pub fn new(name: &str, help: &str, labels: Vec<String>) -> Self {
        Self {
            def: MetricDef {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Gauge,
                labels,
            },
            values: RwLock::new(HashMap::new()),
        }
    }

    /// Set value
    pub fn set(&self, labels: Vec<String>, value: i64) {
        let mut values = self.values.write();
        values
            .entry(labels)
            .or_insert_with(|| AtomicI64::new(0))
            .store(value, Ordering::Relaxed);
    }

    /// Increment
    pub fn inc(&self, labels: Vec<String>) {
        self.add(labels, 1);
    }

    /// Decrement
    pub fn dec(&self, labels: Vec<String>) {
        self.add(labels, -1);
    }

    /// Add value
    pub fn add(&self, labels: Vec<String>, value: i64) {
        let values = self.values.read();
        if let Some(gauge) = values.get(&labels) {
            gauge.fetch_add(value, Ordering::Relaxed);
            return;
        }
        drop(values);

        let mut values = self.values.write();
        values
            .entry(labels)
            .or_insert_with(|| AtomicI64::new(0))
            .fetch_add(value, Ordering::Relaxed);
    }

    /// Get value
    pub fn get(&self, labels: &[String]) -> i64 {
        self.values
            .read()
            .get(labels)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Format as Prometheus
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("# HELP {} {}\n", self.def.name, self.def.help));
        output.push_str(&format!("# TYPE {} gauge\n", self.def.name));

        let values = self.values.read();
        for (labels, value) in values.iter() {
            let label_str = self.format_labels(labels);
            output.push_str(&format!(
                "{}{} {}\n",
                self.def.name,
                label_str,
                value.load(Ordering::Relaxed)
            ));
        }

        output
    }

    fn format_labels(&self, labels: &[String]) -> String {
        if labels.is_empty() || self.def.labels.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = self
            .def
            .labels
            .iter()
            .zip(labels.iter())
            .map(|(name, value)| format!("{}=\"{}\"", name, value))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }
}

/// Histogram metric
pub struct Histogram {
    /// Definition
    def: MetricDef,
    /// Bucket boundaries
    buckets: Vec<f64>,
    /// Bucket counts by label combination
    bucket_counts: RwLock<HashMap<Vec<String>, Vec<AtomicU64>>>,
    /// Sum of observed values
    sums: RwLock<HashMap<Vec<String>, AtomicU64>>,
    /// Count of observations
    counts: RwLock<HashMap<Vec<String>, AtomicU64>>,
}

impl Histogram {
    /// Create a new histogram with default buckets
    pub fn new(name: &str, help: &str, labels: Vec<String>) -> Self {
        Self::with_buckets(
            name,
            help,
            labels,
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
    }

    /// Create with custom buckets
    pub fn with_buckets(name: &str, help: &str, labels: Vec<String>, buckets: Vec<f64>) -> Self {
        Self {
            def: MetricDef {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Histogram,
                labels,
            },
            buckets,
            bucket_counts: RwLock::new(HashMap::new()),
            sums: RwLock::new(HashMap::new()),
            counts: RwLock::new(HashMap::new()),
        }
    }

    /// Observe a value
    pub fn observe(&self, labels: Vec<String>, value: f64) {
        // Update buckets
        {
            let counts = self.bucket_counts.read();
            if let Some(bucket_vec) = counts.get(&labels) {
                for (i, &boundary) in self.buckets.iter().enumerate() {
                    if value <= boundary {
                        bucket_vec[i].fetch_add(1, Ordering::Relaxed);
                    }
                }
                // +Inf bucket
                bucket_vec.last().unwrap().fetch_add(1, Ordering::Relaxed);
            } else {
                drop(counts);
                self.init_buckets(labels.clone());
                self.observe(labels, value);
                return;
            }
        }

        // Update sum
        {
            let sums = self.sums.read();
            if let Some(sum) = sums.get(&labels) {
                sum.fetch_add((value * 1000.0) as u64, Ordering::Relaxed);
            }
        }

        // Update count
        {
            let counts = self.counts.read();
            if let Some(count) = counts.get(&labels) {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn init_buckets(&self, labels: Vec<String>) {
        let mut bucket_counts = self.bucket_counts.write();
        let bucket_vec: Vec<AtomicU64> = (0..=self.buckets.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        bucket_counts.insert(labels.clone(), bucket_vec);

        self.sums.write().insert(labels.clone(), AtomicU64::new(0));
        self.counts.write().insert(labels, AtomicU64::new(0));
    }

    /// Format as Prometheus
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("# HELP {} {}\n", self.def.name, self.def.help));
        output.push_str(&format!("# TYPE {} histogram\n", self.def.name));

        let bucket_counts = self.bucket_counts.read();
        let sums = self.sums.read();
        let counts = self.counts.read();

        for (labels, bucket_vec) in bucket_counts.iter() {
            let base_label_str = self.format_labels(labels);

            // Bucket values
            for (i, &boundary) in self.buckets.iter().enumerate() {
                let le_label = if base_label_str.is_empty() {
                    format!("{{le=\"{}\"}}", boundary)
                } else {
                    let inner = &base_label_str[1..base_label_str.len() - 1];
                    format!("{{{},le=\"{}\"}}", inner, boundary)
                };
                output.push_str(&format!(
                    "{}_bucket{} {}\n",
                    self.def.name,
                    le_label,
                    bucket_vec[i].load(Ordering::Relaxed)
                ));
            }

            // +Inf bucket
            let inf_label = if base_label_str.is_empty() {
                "{le=\"+Inf\"}".to_string()
            } else {
                let inner = &base_label_str[1..base_label_str.len() - 1];
                format!("{{{},le=\"+Inf\"}}", inner)
            };
            output.push_str(&format!(
                "{}_bucket{} {}\n",
                self.def.name,
                inf_label,
                bucket_vec.last().unwrap().load(Ordering::Relaxed)
            ));

            // Sum
            if let Some(sum) = sums.get(labels) {
                output.push_str(&format!(
                    "{}_sum{} {}\n",
                    self.def.name,
                    base_label_str,
                    sum.load(Ordering::Relaxed) as f64 / 1000.0
                ));
            }

            // Count
            if let Some(count) = counts.get(labels) {
                output.push_str(&format!(
                    "{}_count{} {}\n",
                    self.def.name,
                    base_label_str,
                    count.load(Ordering::Relaxed)
                ));
            }
        }

        output
    }

    fn format_labels(&self, labels: &[String]) -> String {
        if labels.is_empty() || self.def.labels.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = self
            .def
            .labels
            .iter()
            .zip(labels.iter())
            .map(|(name, value)| format!("{}=\"{}\"", name, value))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }
}

// ============================================================================
// Metrics Registry
// ============================================================================

/// Metrics registry
pub struct MetricsRegistry {
    /// Counters
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    /// Gauges
    gauges: RwLock<HashMap<String, Arc<Gauge>>>,
    /// Histograms
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Register a counter
    pub fn register_counter(&self, counter: Counter) -> Arc<Counter> {
        let name = counter.def.name.clone();
        let arc = Arc::new(counter);
        self.counters.write().insert(name, Arc::clone(&arc));
        arc
    }

    /// Register a gauge
    pub fn register_gauge(&self, gauge: Gauge) -> Arc<Gauge> {
        let name = gauge.def.name.clone();
        let arc = Arc::new(gauge);
        self.gauges.write().insert(name, Arc::clone(&arc));
        arc
    }

    /// Register a histogram
    pub fn register_histogram(&self, histogram: Histogram) -> Arc<Histogram> {
        let name = histogram.def.name.clone();
        let arc = Arc::new(histogram);
        self.histograms.write().insert(name, Arc::clone(&arc));
        arc
    }

    /// Get a counter
    pub fn get_counter(&self, name: &str) -> Option<Arc<Counter>> {
        self.counters.read().get(name).cloned()
    }

    /// Get a gauge
    pub fn get_gauge(&self, name: &str) -> Option<Arc<Gauge>> {
        self.gauges.read().get(name).cloned()
    }

    /// Get a histogram
    pub fn get_histogram(&self, name: &str) -> Option<Arc<Histogram>> {
        self.histograms.read().get(name).cloned()
    }

    /// Export all metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        for counter in self.counters.read().values() {
            output.push_str(&counter.to_prometheus());
        }

        for gauge in self.gauges.read().values() {
            output.push_str(&gauge.to_prometheus());
        }

        for histogram in self.histograms.read().values() {
            output.push_str(&histogram.to_prometheus());
        }

        output
    }

    /// Export metrics as JSON
    pub fn export_json(&self) -> String {
        let mut metrics = Vec::new();

        for (name, counter) in self.counters.read().iter() {
            metrics.push(format!(
                r#"{{"name":"{}","type":"counter","value":{}}}"#,
                name,
                counter.get(&[])
            ));
        }

        for (name, gauge) in self.gauges.read().iter() {
            metrics.push(format!(
                r#"{{"name":"{}","type":"gauge","value":{}}}"#,
                name,
                gauge.get(&[])
            ));
        }

        format!("{{\"metrics\":[{}]}}", metrics.join(","))
    }
}

// ============================================================================
// Monitoring Manager
// ============================================================================

/// Monitoring manager
pub struct MonitoringManager {
    /// Metrics registry
    registry: MetricsRegistry,
    /// Health check callbacks
    health_checks: RwLock<HashMap<String, HealthCheckFn>>,
    /// Start time
    start_time: Instant,
    /// Version
    version: String,
    /// Readiness flag
    ready: AtomicBool,
    /// Shutting down flag
    shutting_down: AtomicBool,
    /// Active connections gauge
    connections_gauge: Option<Arc<Gauge>>,
    /// Replication lag gauge
    lag_gauge: Option<Arc<Gauge>>,
}

impl MonitoringManager {
    /// Create a new monitoring manager
    pub fn new(version: &str) -> Self {
        let registry = MetricsRegistry::new();

        // Register default metrics
        let connections_gauge = Some(registry.register_gauge(Gauge::new(
            "boyodb_connections_active",
            "Number of active connections",
            vec![],
        )));

        let lag_gauge = Some(registry.register_gauge(Gauge::new(
            "boyodb_replication_lag_ms",
            "Replication lag in milliseconds",
            vec![],
        )));

        Self {
            registry,
            health_checks: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
            version: version.to_string(),
            ready: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            connections_gauge,
            lag_gauge,
        }
    }

    /// Get metrics registry
    pub fn registry(&self) -> &MetricsRegistry {
        &self.registry
    }

    /// Register a health check
    pub fn register_health_check(&self, name: &str, check: HealthCheckFn) {
        self.health_checks.write().insert(name.to_string(), check);
    }

    /// Mark as ready
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    /// Mark as shutting down
    pub fn set_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.ready.store(false, Ordering::SeqCst);
    }

    /// Update connection count
    pub fn set_connections(&self, count: i64) {
        if let Some(ref gauge) = self.connections_gauge {
            gauge.set(vec![], count);
        }
    }

    /// Update replication lag
    pub fn set_lag(&self, lag_ms: i64) {
        if let Some(ref gauge) = self.lag_gauge {
            gauge.set(vec![], lag_ms);
        }
    }

    /// Perform health check
    pub fn health_check(&self) -> HealthCheckResult {
        let checks = self.health_checks.read();
        let mut components = HashMap::new();
        let mut overall_status = HealthStatus::Healthy;

        for (name, check) in checks.iter() {
            let result = check();
            if result.status == HealthStatus::Unhealthy {
                overall_status = HealthStatus::Unhealthy;
            } else if result.status == HealthStatus::Degraded
                && overall_status == HealthStatus::Healthy
            {
                overall_status = HealthStatus::Degraded;
            }
            components.insert(name.clone(), result);
        }

        HealthCheckResult {
            status: overall_status,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            version: self.version.clone(),
            components,
            timestamp: SystemTime::now(),
        }
    }

    /// Perform readiness check
    pub fn readiness_check(&self) -> ReadinessCheckResult {
        let status = if self.shutting_down.load(Ordering::SeqCst) {
            ReadinessStatus::ShuttingDown
        } else if self.ready.load(Ordering::SeqCst) {
            ReadinessStatus::Ready
        } else {
            ReadinessStatus::NotReady
        };

        let connections = self
            .connections_gauge
            .as_ref()
            .map(|g| g.get(&[]) as u64)
            .unwrap_or(0);

        let lag_ms = self.lag_gauge.as_ref().and_then(|g| {
            let v = g.get(&[]);
            if v > 0 {
                Some(v as u64)
            } else {
                None
            }
        });

        ReadinessCheckResult {
            status,
            connections,
            lag_ms,
            accepting_connections: status == ReadinessStatus::Ready,
            init_progress: None,
            timestamp: SystemTime::now(),
        }
    }

    /// Export Prometheus metrics
    pub fn prometheus_metrics(&self) -> String {
        self.registry.export_prometheus()
    }

    /// Export JSON metrics
    pub fn json_metrics(&self) -> String {
        self.registry.export_json()
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

// ============================================================================
// Standard Metrics
// ============================================================================

/// Create standard database metrics
pub fn create_standard_metrics(registry: &MetricsRegistry) -> StandardMetrics {
    StandardMetrics {
        queries_total: registry.register_counter(Counter::new(
            "boyodb_queries_total",
            "Total number of queries executed",
            vec!["database".to_string(), "type".to_string()],
        )),
        query_duration: registry.register_histogram(Histogram::new(
            "boyodb_query_duration_seconds",
            "Query execution duration in seconds",
            vec!["database".to_string()],
        )),
        rows_read: registry.register_counter(Counter::new(
            "boyodb_rows_read_total",
            "Total number of rows read",
            vec!["database".to_string(), "table".to_string()],
        )),
        rows_written: registry.register_counter(Counter::new(
            "boyodb_rows_written_total",
            "Total number of rows written",
            vec!["database".to_string(), "table".to_string()],
        )),
        errors_total: registry.register_counter(Counter::new(
            "boyodb_errors_total",
            "Total number of errors",
            vec!["type".to_string()],
        )),
        connections_total: registry.register_counter(Counter::new(
            "boyodb_connections_total",
            "Total number of connections established",
            vec![],
        )),
    }
}

/// Standard database metrics
pub struct StandardMetrics {
    /// Total queries
    pub queries_total: Arc<Counter>,
    /// Query duration histogram
    pub query_duration: Arc<Histogram>,
    /// Rows read
    pub rows_read: Arc<Counter>,
    /// Rows written
    pub rows_written: Arc<Counter>,
    /// Errors
    pub errors_total: Arc<Counter>,
    /// Connections
    pub connections_total: Arc<Counter>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new("test_counter", "A test counter", vec![]);

        counter.inc(vec![]);
        counter.inc(vec![]);
        counter.add(vec![], 5);

        assert_eq!(counter.get(&[]), 7);
    }

    #[test]
    fn test_counter_with_labels() {
        let counter = Counter::new(
            "http_requests_total",
            "Total HTTP requests",
            vec!["method".to_string(), "status".to_string()],
        );

        counter.inc(vec!["GET".to_string(), "200".to_string()]);
        counter.inc(vec!["GET".to_string(), "200".to_string()]);
        counter.inc(vec!["POST".to_string(), "201".to_string()]);

        assert_eq!(counter.get(&["GET".to_string(), "200".to_string()]), 2);
        assert_eq!(counter.get(&["POST".to_string(), "201".to_string()]), 1);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new("temperature", "Current temperature", vec![]);

        gauge.set(vec![], 20);
        assert_eq!(gauge.get(&[]), 20);

        gauge.inc(vec![]);
        assert_eq!(gauge.get(&[]), 21);

        gauge.dec(vec![]);
        assert_eq!(gauge.get(&[]), 20);
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::with_buckets(
            "request_duration",
            "Request duration",
            vec![],
            vec![0.1, 0.5, 1.0],
        );

        histogram.observe(vec![], 0.05);
        histogram.observe(vec![], 0.3);
        histogram.observe(vec![], 0.8);
        histogram.observe(vec![], 1.5);

        let prometheus = histogram.to_prometheus();
        assert!(prometheus.contains("request_duration_bucket"));
        assert!(prometheus.contains("request_duration_sum"));
        assert!(prometheus.contains("request_duration_count"));
    }

    #[test]
    fn test_health_check() {
        let manager = MonitoringManager::new("1.0.0");

        manager.register_health_check(
            "database",
            Box::new(|| ComponentHealth {
                status: HealthStatus::Healthy,
                message: None,
                last_check: SystemTime::now(),
            }),
        );

        let result = manager.health_check();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.version, "1.0.0");
    }

    #[test]
    fn test_readiness_check() {
        let manager = MonitoringManager::new("1.0.0");

        // Initially not ready
        let result = manager.readiness_check();
        assert_eq!(result.status, ReadinessStatus::NotReady);

        // Mark as ready
        manager.set_ready(true);
        let result = manager.readiness_check();
        assert_eq!(result.status, ReadinessStatus::Ready);

        // Mark as shutting down
        manager.set_shutting_down();
        let result = manager.readiness_check();
        assert_eq!(result.status, ReadinessStatus::ShuttingDown);
    }

    #[test]
    fn test_health_result_json() {
        let result = HealthCheckResult {
            status: HealthStatus::Healthy,
            uptime_seconds: 12345,
            version: "1.0.0".to_string(),
            components: HashMap::new(),
            timestamp: SystemTime::now(),
        };

        let json = result.to_json();
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"uptime_seconds\":12345"));
    }

    #[test]
    fn test_metrics_registry() {
        let registry = MetricsRegistry::new();

        let counter = registry.register_counter(Counter::new("test", "Test", vec![]));
        counter.inc(vec![]);

        let retrieved = registry.get_counter("test").unwrap();
        assert_eq!(retrieved.get(&[]), 1);

        let prometheus = registry.export_prometheus();
        assert!(prometheus.contains("test"));
    }

    #[test]
    fn test_standard_metrics() {
        let registry = MetricsRegistry::new();
        let metrics = create_standard_metrics(&registry);

        metrics
            .queries_total
            .inc(vec!["mydb".to_string(), "SELECT".to_string()]);
        metrics
            .query_duration
            .observe(vec!["mydb".to_string()], 0.5);

        assert_eq!(
            metrics
                .queries_total
                .get(&["mydb".to_string(), "SELECT".to_string()]),
            1
        );
    }
}
