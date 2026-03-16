//! OpenTelemetry Integration Module
//!
//! Provides distributed tracing, metrics, and logging integration with OpenTelemetry.
//! Supports:
//! - Distributed tracing with span propagation
//! - Metrics export (Prometheus, OTLP)
//! - Structured logging
//! - Jaeger/Zipkin compatibility
//! - Context propagation across services

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Telemetry configuration
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    /// Enable telemetry
    pub enabled: bool,
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Environment (production, staging, etc.)
    pub environment: String,
    /// OTLP endpoint for traces
    pub otlp_endpoint: Option<String>,
    /// Jaeger endpoint
    pub jaeger_endpoint: Option<String>,
    /// Prometheus metrics port
    pub prometheus_port: Option<u16>,
    /// Sampling rate (0.0 - 1.0)
    pub sampling_rate: f64,
    /// Maximum spans per trace
    pub max_spans_per_trace: usize,
    /// Span buffer size
    pub span_buffer_size: usize,
    /// Metrics export interval in seconds
    pub metrics_export_interval_secs: u64,
    /// Enable trace context propagation
    pub propagation_enabled: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: "boyodb".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            environment: "development".to_string(),
            otlp_endpoint: None,
            jaeger_endpoint: None,
            prometheus_port: Some(9090),
            sampling_rate: 1.0,
            max_spans_per_trace: 1000,
            span_buffer_size: 10000,
            metrics_export_interval_secs: 15,
            propagation_enabled: true,
        }
    }
}

/// Span status
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    /// Unset status
    Unset,
    /// Operation completed successfully
    Ok,
    /// Operation failed
    Error,
}

/// Span kind
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    /// Internal span
    Internal,
    /// Server span (handling incoming request)
    Server,
    /// Client span (making outgoing request)
    Client,
    /// Producer span (sending message)
    Producer,
    /// Consumer span (receiving message)
    Consumer,
}

/// A trace span representing a unit of work
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Span {
    /// Unique trace ID (128-bit)
    pub trace_id: String,
    /// Unique span ID (64-bit)
    pub span_id: String,
    /// Parent span ID (if any)
    pub parent_span_id: Option<String>,
    /// Operation name
    pub name: String,
    /// Span kind
    pub kind: SpanKind,
    /// Start time (unix nanos)
    pub start_time_nanos: u64,
    /// End time (unix nanos)
    pub end_time_nanos: Option<u64>,
    /// Duration in microseconds
    pub duration_us: Option<u64>,
    /// Span status
    pub status: SpanStatus,
    /// Error message (if status is Error)
    pub error_message: Option<String>,
    /// Attributes (key-value pairs)
    pub attributes: HashMap<String, AttributeValue>,
    /// Events within the span
    pub events: Vec<SpanEvent>,
    /// Links to other spans
    pub links: Vec<SpanLink>,
}

/// Attribute value types
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<i64> for AttributeValue {
    fn from(v: i64) -> Self {
        AttributeValue::Int(v)
    }
}

impl From<f64> for AttributeValue {
    fn from(v: f64) -> Self {
        AttributeValue::Float(v)
    }
}

impl From<bool> for AttributeValue {
    fn from(v: bool) -> Self {
        AttributeValue::Bool(v)
    }
}

/// Event within a span
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event name
    pub name: String,
    /// Timestamp (unix nanos)
    pub timestamp_nanos: u64,
    /// Event attributes
    pub attributes: HashMap<String, AttributeValue>,
}

/// Link to another span
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanLink {
    /// Linked trace ID
    pub trace_id: String,
    /// Linked span ID
    pub span_id: String,
    /// Link attributes
    pub attributes: HashMap<String, AttributeValue>,
}

/// Trace context for propagation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceContext {
    /// Trace ID
    pub trace_id: String,
    /// Span ID
    pub span_id: String,
    /// Trace flags (sampled, etc.)
    pub trace_flags: u8,
    /// Trace state (vendor-specific)
    pub trace_state: Option<String>,
}

impl TraceContext {
    /// Parse from W3C traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() >= 4 && parts[0] == "00" {
            Some(Self {
                trace_id: parts[1].to_string(),
                span_id: parts[2].to_string(),
                trace_flags: u8::from_str_radix(parts[3], 16).unwrap_or(0),
                trace_state: None,
            })
        } else {
            None
        }
    }

    /// Format as W3C traceparent header
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Check if sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }
}

/// Metric types
#[derive(Clone, Debug)]
pub enum MetricValue {
    /// Counter (monotonically increasing)
    Counter(u64),
    /// Gauge (can go up or down)
    Gauge(f64),
    /// Histogram (distribution of values)
    Histogram(HistogramValue),
    /// Summary (quantile distribution)
    Summary(SummaryValue),
}

/// Histogram metric value
#[derive(Clone, Debug, Default)]
pub struct HistogramValue {
    /// Bucket counts
    pub buckets: Vec<(f64, u64)>, // (upper_bound, count)
    /// Sum of all values
    pub sum: f64,
    /// Count of values
    pub count: u64,
}

/// Summary metric value
#[derive(Clone, Debug, Default)]
pub struct SummaryValue {
    /// Quantiles
    pub quantiles: Vec<(f64, f64)>, // (quantile, value)
    /// Sum of all values
    pub sum: f64,
    /// Count of values
    pub count: u64,
}

/// A metric data point
#[derive(Clone, Debug)]
pub struct MetricPoint {
    /// Metric name
    pub name: String,
    /// Metric description
    pub description: String,
    /// Metric unit
    pub unit: String,
    /// Metric value
    pub value: MetricValue,
    /// Labels/attributes
    pub labels: HashMap<String, String>,
    /// Timestamp
    pub timestamp_ms: u64,
}

/// Span builder for fluent API
pub struct SpanBuilder {
    name: String,
    kind: SpanKind,
    parent_context: Option<TraceContext>,
    attributes: HashMap<String, AttributeValue>,
    links: Vec<SpanLink>,
}

impl SpanBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: SpanKind::Internal,
            parent_context: None,
            attributes: HashMap::new(),
            links: Vec::new(),
        }
    }

    pub fn kind(mut self, kind: SpanKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn parent(mut self, context: TraceContext) -> Self {
        self.parent_context = Some(context);
        self
    }

    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    pub fn link(mut self, trace_id: String, span_id: String) -> Self {
        self.links.push(SpanLink {
            trace_id,
            span_id,
            attributes: HashMap::new(),
        });
        self
    }

    pub fn start(self, tracer: &Arc<Tracer>) -> ActiveSpan {
        tracer.start_span_internal(self)
    }
}

/// Active span that can be modified and ended
pub struct ActiveSpan {
    span: Span,
    tracer: Arc<Tracer>,
    start_instant: Instant,
}

impl ActiveSpan {
    /// Get the trace context for propagation
    pub fn context(&self) -> TraceContext {
        TraceContext {
            trace_id: self.span.trace_id.clone(),
            span_id: self.span.span_id.clone(),
            trace_flags: 0x01, // sampled
            trace_state: None,
        }
    }

    /// Add an attribute to the span
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) {
        self.span.attributes.insert(key.into(), value.into());
    }

    /// Add an event to the span
    pub fn add_event(&mut self, name: impl Into<String>) {
        self.span.events.push(SpanEvent {
            name: name.into(),
            timestamp_nanos: current_time_nanos(),
            attributes: HashMap::new(),
        });
    }

    /// Add an event with attributes
    pub fn add_event_with_attributes(
        &mut self,
        name: impl Into<String>,
        attributes: HashMap<String, AttributeValue>,
    ) {
        self.span.events.push(SpanEvent {
            name: name.into(),
            timestamp_nanos: current_time_nanos(),
            attributes,
        });
    }

    /// Set the span status to OK
    pub fn set_ok(&mut self) {
        self.span.status = SpanStatus::Ok;
    }

    /// Set the span status to Error
    pub fn set_error(&mut self, message: impl Into<String>) {
        self.span.status = SpanStatus::Error;
        self.span.error_message = Some(message.into());
    }

    /// End the span
    pub fn end(mut self) {
        let end_time = current_time_nanos();
        self.span.end_time_nanos = Some(end_time);
        self.span.duration_us = Some(self.start_instant.elapsed().as_micros() as u64);

        if self.span.status == SpanStatus::Unset {
            self.span.status = SpanStatus::Ok;
        }

        self.tracer.record_span(self.span);
    }
}

/// Tracer for creating and managing spans
pub struct Tracer {
    config: TelemetryConfig,
    /// Completed spans buffer
    spans: RwLock<Vec<Span>>,
    /// Active span count
    active_spans: AtomicU64,
    /// Total spans created
    total_spans: AtomicU64,
    /// Dropped spans (buffer overflow)
    dropped_spans: AtomicU64,
    /// Random state for ID generation
    rng_state: AtomicU64,
}

impl Tracer {
    pub fn new(config: TelemetryConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            spans: RwLock::new(Vec::with_capacity(1000)),
            active_spans: AtomicU64::new(0),
            total_spans: AtomicU64::new(0),
            dropped_spans: AtomicU64::new(0),
            rng_state: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64,
            ),
        })
    }

    /// Create a new span builder
    pub fn span(&self, name: impl Into<String>) -> SpanBuilder {
        SpanBuilder::new(name)
    }

    /// Start a span directly
    pub fn start_span(self: &Arc<Self>, name: impl Into<String>) -> ActiveSpan {
        SpanBuilder::new(name).start(self)
    }

    /// Start a span with a parent context
    pub fn start_span_with_parent(
        self: &Arc<Self>,
        name: impl Into<String>,
        parent: TraceContext,
    ) -> ActiveSpan {
        SpanBuilder::new(name).parent(parent).start(self)
    }

    fn start_span_internal(self: &Arc<Self>, builder: SpanBuilder) -> ActiveSpan {
        self.total_spans.fetch_add(1, Ordering::Relaxed);
        self.active_spans.fetch_add(1, Ordering::Relaxed);

        let trace_id = if let Some(ref ctx) = builder.parent_context {
            ctx.trace_id.clone()
        } else {
            self.generate_trace_id()
        };

        let span_id = self.generate_span_id();
        let parent_span_id = builder.parent_context.map(|ctx| ctx.span_id);

        let span = Span {
            trace_id,
            span_id,
            parent_span_id,
            name: builder.name,
            kind: builder.kind,
            start_time_nanos: current_time_nanos(),
            end_time_nanos: None,
            duration_us: None,
            status: SpanStatus::Unset,
            error_message: None,
            attributes: builder.attributes,
            events: Vec::new(),
            links: builder.links,
        };

        ActiveSpan {
            span,
            tracer: Arc::clone(self),
            start_instant: Instant::now(),
        }
    }

    fn record_span(&self, span: Span) {
        self.active_spans.fetch_sub(1, Ordering::Relaxed);

        let mut spans = self.spans.write();
        if spans.len() >= self.config.span_buffer_size {
            self.dropped_spans.fetch_add(1, Ordering::Relaxed);
            spans.remove(0); // Remove oldest
        }
        spans.push(span);
    }

    /// Flush completed spans
    pub fn flush(&self) -> Vec<Span> {
        let mut spans = self.spans.write();
        std::mem::take(&mut *spans)
    }

    /// Get tracer statistics
    pub fn stats(&self) -> TracerStats {
        TracerStats {
            total_spans: self.total_spans.load(Ordering::Relaxed),
            active_spans: self.active_spans.load(Ordering::Relaxed),
            buffered_spans: self.spans.read().len() as u64,
            dropped_spans: self.dropped_spans.load(Ordering::Relaxed),
        }
    }

    fn generate_trace_id(&self) -> String {
        let r1 = self.next_random();
        let r2 = self.next_random();
        format!("{:016x}{:016x}", r1, r2)
    }

    fn generate_span_id(&self) -> String {
        format!("{:016x}", self.next_random())
    }

    fn next_random(&self) -> u64 {
        // Simple xorshift64
        let mut x = self.rng_state.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state.store(x, Ordering::Relaxed);
        x
    }
}

/// Tracer statistics
#[derive(Clone, Debug)]
pub struct TracerStats {
    pub total_spans: u64,
    pub active_spans: u64,
    pub buffered_spans: u64,
    pub dropped_spans: u64,
}

/// Metrics registry for collecting and exporting metrics
pub struct MetricsRegistry {
    config: TelemetryConfig,
    /// Registered metrics
    metrics: RwLock<HashMap<String, MetricEntry>>,
}

struct MetricEntry {
    description: String,
    unit: String,
    value: RwLock<MetricValue>,
    labels: HashMap<String, String>,
}

impl MetricsRegistry {
    pub fn new(config: TelemetryConfig) -> Self {
        Self {
            config,
            metrics: RwLock::new(HashMap::new()),
        }
    }

    /// Register a counter metric
    pub fn register_counter(&self, name: &str, description: &str, unit: &str) {
        let mut metrics = self.metrics.write();
        metrics.insert(
            name.to_string(),
            MetricEntry {
                description: description.to_string(),
                unit: unit.to_string(),
                value: RwLock::new(MetricValue::Counter(0)),
                labels: HashMap::new(),
            },
        );
    }

    /// Register a gauge metric
    pub fn register_gauge(&self, name: &str, description: &str, unit: &str) {
        let mut metrics = self.metrics.write();
        metrics.insert(
            name.to_string(),
            MetricEntry {
                description: description.to_string(),
                unit: unit.to_string(),
                value: RwLock::new(MetricValue::Gauge(0.0)),
                labels: HashMap::new(),
            },
        );
    }

    /// Increment a counter
    pub fn increment_counter(&self, name: &str, delta: u64) {
        let metrics = self.metrics.read();
        if let Some(entry) = metrics.get(name) {
            let mut value = entry.value.write();
            if let MetricValue::Counter(ref mut v) = *value {
                *v += delta;
            }
        }
    }

    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: f64) {
        let metrics = self.metrics.read();
        if let Some(entry) = metrics.get(name) {
            let mut v = entry.value.write();
            *v = MetricValue::Gauge(value);
        }
    }

    /// Get all metrics for export
    pub fn collect(&self) -> Vec<MetricPoint> {
        let metrics = self.metrics.read();
        let timestamp = current_time_ms();

        metrics
            .iter()
            .map(|(name, entry)| {
                let value = entry.value.read().clone();
                MetricPoint {
                    name: name.clone(),
                    description: entry.description.clone(),
                    unit: entry.unit.clone(),
                    value,
                    labels: entry.labels.clone(),
                    timestamp_ms: timestamp,
                }
            })
            .collect()
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let metrics = self.collect();
        let mut output = String::new();

        for metric in metrics {
            // Help line
            output.push_str(&format!("# HELP {} {}\n", metric.name, metric.description));

            // Type line
            let type_str = match metric.value {
                MetricValue::Counter(_) => "counter",
                MetricValue::Gauge(_) => "gauge",
                MetricValue::Histogram(_) => "histogram",
                MetricValue::Summary(_) => "summary",
            };
            output.push_str(&format!("# TYPE {} {}\n", metric.name, type_str));

            // Value line
            match metric.value {
                MetricValue::Counter(v) => {
                    output.push_str(&format!("{} {}\n", metric.name, v));
                }
                MetricValue::Gauge(v) => {
                    output.push_str(&format!("{} {}\n", metric.name, v));
                }
                MetricValue::Histogram(ref h) => {
                    for (bound, count) in &h.buckets {
                        output.push_str(&format!(
                            "{}_bucket{{le=\"{}\"}} {}\n",
                            metric.name, bound, count
                        ));
                    }
                    output.push_str(&format!("{}_sum {}\n", metric.name, h.sum));
                    output.push_str(&format!("{}_count {}\n", metric.name, h.count));
                }
                MetricValue::Summary(ref s) => {
                    for (q, v) in &s.quantiles {
                        output.push_str(&format!("{}{{quantile=\"{}\"}} {}\n", metric.name, q, v));
                    }
                    output.push_str(&format!("{}_sum {}\n", metric.name, s.sum));
                    output.push_str(&format!("{}_count {}\n", metric.name, s.count));
                }
            }
        }

        output
    }
}

/// Global telemetry instance
pub struct Telemetry {
    pub config: TelemetryConfig,
    pub tracer: Arc<Tracer>,
    pub metrics: Arc<MetricsRegistry>,
}

impl Telemetry {
    pub fn new(config: TelemetryConfig) -> Self {
        let tracer = Tracer::new(config.clone());
        let metrics = Arc::new(MetricsRegistry::new(config.clone()));

        // Register default metrics
        metrics.register_counter("boyodb_queries_total", "Total number of queries", "queries");
        metrics.register_counter("boyodb_errors_total", "Total number of errors", "errors");
        metrics.register_gauge(
            "boyodb_active_connections",
            "Active connections",
            "connections",
        );
        metrics.register_gauge("boyodb_memory_bytes", "Memory usage in bytes", "bytes");
        metrics.register_counter("boyodb_rows_read", "Total rows read", "rows");
        metrics.register_counter("boyodb_rows_written", "Total rows written", "rows");
        metrics.register_counter("boyodb_bytes_read", "Total bytes read", "bytes");
        metrics.register_counter("boyodb_bytes_written", "Total bytes written", "bytes");

        Self {
            config,
            tracer,
            metrics,
        }
    }

    /// Create a span for a database query
    pub fn query_span(&self, sql: &str, database: &str) -> ActiveSpan {
        self.tracer
            .span("db.query")
            .kind(SpanKind::Server)
            .attribute("db.system", "boyodb")
            .attribute("db.statement", sql.to_string())
            .attribute("db.name", database.to_string())
            .start(&self.tracer)
    }

    /// Record a successful query
    pub fn record_query_success(&self, duration_us: u64, rows: u64) {
        self.metrics.increment_counter("boyodb_queries_total", 1);
        self.metrics.increment_counter("boyodb_rows_read", rows);
    }

    /// Record a query error
    pub fn record_query_error(&self) {
        self.metrics.increment_counter("boyodb_queries_total", 1);
        self.metrics.increment_counter("boyodb_errors_total", 1);
    }

    /// Update active connections gauge
    pub fn set_active_connections(&self, count: u64) {
        self.metrics
            .set_gauge("boyodb_active_connections", count as f64);
    }

    /// Update memory usage gauge
    pub fn set_memory_usage(&self, bytes: u64) {
        self.metrics.set_gauge("boyodb_memory_bytes", bytes as f64);
    }
}

fn current_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_parsing() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::from_traceparent(header).unwrap();

        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.span_id, "b7ad6b7169203331");
        assert!(ctx.is_sampled());

        let round_trip = ctx.to_traceparent();
        assert_eq!(round_trip, header);
    }

    #[test]
    fn test_tracer_basic() {
        let config = TelemetryConfig::default();
        let tracer = Tracer::new(config);

        {
            let mut span = tracer.start_span("test_operation");
            span.set_attribute("test.key", "test_value");
            span.add_event("test_event");
            span.end();
        }

        let spans = tracer.flush();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "test_operation");
        assert_eq!(spans[0].status, SpanStatus::Ok);
    }

    #[test]
    fn test_span_hierarchy() {
        let config = TelemetryConfig::default();
        let tracer = Tracer::new(config);

        let parent = tracer.start_span("parent");
        let parent_ctx = parent.context();

        let child = tracer.start_span_with_parent("child", parent_ctx.clone());
        let child_ctx = child.context();

        child.end();
        parent.end();

        let spans = tracer.flush();
        assert_eq!(spans.len(), 2);

        let child_span = spans.iter().find(|s| s.name == "child").unwrap();
        assert_eq!(
            child_span.parent_span_id.as_deref(),
            Some(parent_ctx.span_id.as_str())
        );
        assert_eq!(child_span.trace_id, parent_ctx.trace_id);
    }

    #[test]
    fn test_metrics_registry() {
        let config = TelemetryConfig::default();
        let registry = MetricsRegistry::new(config);

        registry.register_counter("test_counter", "A test counter", "count");
        registry.register_gauge("test_gauge", "A test gauge", "value");

        registry.increment_counter("test_counter", 5);
        registry.increment_counter("test_counter", 3);
        registry.set_gauge("test_gauge", 42.5);

        let metrics = registry.collect();
        assert_eq!(metrics.len(), 2);

        let counter = metrics.iter().find(|m| m.name == "test_counter").unwrap();
        if let MetricValue::Counter(v) = counter.value {
            assert_eq!(v, 8);
        } else {
            panic!("Expected counter");
        }

        let gauge = metrics.iter().find(|m| m.name == "test_gauge").unwrap();
        if let MetricValue::Gauge(v) = gauge.value {
            assert!((v - 42.5).abs() < 0.001);
        } else {
            panic!("Expected gauge");
        }
    }

    #[test]
    fn test_prometheus_export() {
        let config = TelemetryConfig::default();
        let registry = MetricsRegistry::new(config);

        registry.register_counter("requests_total", "Total requests", "requests");
        registry.increment_counter("requests_total", 100);

        let output = registry.export_prometheus();
        assert!(output.contains("# HELP requests_total Total requests"));
        assert!(output.contains("# TYPE requests_total counter"));
        assert!(output.contains("requests_total 100"));
    }
}
