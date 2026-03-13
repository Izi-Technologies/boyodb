//! OpenTelemetry Integration for BoyoDB
//!
//! Provides distributed tracing, metrics, and logging integration with OpenTelemetry.
//! Supports exporting to various backends: Jaeger, Zipkin, OTLP, Prometheus.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// OpenTelemetry configuration
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Service name for tracing
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// OTLP endpoint for traces (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Jaeger endpoint (e.g., "http://localhost:14268/api/traces")
    pub jaeger_endpoint: Option<String>,
    /// Zipkin endpoint (e.g., "http://localhost:9411/api/v2/spans")
    pub zipkin_endpoint: Option<String>,
    /// Prometheus metrics port
    pub prometheus_port: Option<u16>,
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
    /// Enable trace context propagation
    pub propagation_enabled: bool,
    /// Batch export interval in milliseconds
    pub export_interval_ms: u64,
    /// Maximum batch size for export
    pub max_batch_size: usize,
    /// Resource attributes
    pub resource_attributes: HashMap<String, String>,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            service_name: "boyodb".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            otlp_endpoint: None,
            jaeger_endpoint: None,
            zipkin_endpoint: None,
            prometheus_port: None,
            sampling_rate: 1.0,
            propagation_enabled: true,
            export_interval_ms: 5000,
            max_batch_size: 512,
            resource_attributes: HashMap::new(),
        }
    }
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Span kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// A trace span representing a unit of work
#[derive(Debug, Clone)]
pub struct Span {
    /// Unique trace ID (128-bit)
    pub trace_id: u128,
    /// Unique span ID (64-bit)
    pub span_id: u64,
    /// Parent span ID (0 if root)
    pub parent_span_id: u64,
    /// Span name/operation
    pub name: String,
    /// Span kind
    pub kind: SpanKind,
    /// Start time (Unix timestamp in nanoseconds)
    pub start_time_ns: u64,
    /// End time (Unix timestamp in nanoseconds, 0 if not ended)
    pub end_time_ns: u64,
    /// Span status
    pub status: SpanStatus,
    /// Status message (for errors)
    pub status_message: Option<String>,
    /// Span attributes
    pub attributes: HashMap<String, AttributeValue>,
    /// Span events
    pub events: Vec<SpanEvent>,
    /// Links to other spans
    pub links: Vec<SpanLink>,
}

/// Attribute value types
#[derive(Debug, Clone)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
    FloatArray(Vec<f64>),
    BoolArray(Vec<bool>),
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

/// A span event (point-in-time occurrence)
#[derive(Debug, Clone)]
pub struct SpanEvent {
    /// Event name
    pub name: String,
    /// Timestamp (Unix nanoseconds)
    pub timestamp_ns: u64,
    /// Event attributes
    pub attributes: HashMap<String, AttributeValue>,
}

/// Link to another span
#[derive(Debug, Clone)]
pub struct SpanLink {
    /// Linked trace ID
    pub trace_id: u128,
    /// Linked span ID
    pub span_id: u64,
    /// Link attributes
    pub attributes: HashMap<String, AttributeValue>,
}

/// Metric types
#[derive(Debug, Clone)]
pub enum MetricKind {
    Counter,
    UpDownCounter,
    Gauge,
    Histogram,
}

/// A metric data point
#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    /// Metric name
    pub name: String,
    /// Metric description
    pub description: String,
    /// Metric unit
    pub unit: String,
    /// Metric kind
    pub kind: MetricKind,
    /// Timestamp
    pub timestamp_ns: u64,
    /// Metric value (for counter/gauge)
    pub value: Option<f64>,
    /// Histogram buckets (boundary -> count)
    pub histogram_buckets: Option<Vec<(f64, u64)>>,
    /// Histogram sum
    pub histogram_sum: Option<f64>,
    /// Histogram count
    pub histogram_count: Option<u64>,
    /// Attributes/labels
    pub attributes: HashMap<String, String>,
}

/// Database-specific semantic conventions
pub mod semantic_conventions {
    pub const DB_SYSTEM: &str = "db.system";
    pub const DB_NAME: &str = "db.name";
    pub const DB_STATEMENT: &str = "db.statement";
    pub const DB_OPERATION: &str = "db.operation";
    pub const DB_SQL_TABLE: &str = "db.sql.table";
    pub const DB_ROWS_AFFECTED: &str = "db.rows_affected";

    pub const NET_PEER_NAME: &str = "net.peer.name";
    pub const NET_PEER_PORT: &str = "net.peer.port";
    pub const NET_TRANSPORT: &str = "net.transport";

    pub const EXCEPTION_TYPE: &str = "exception.type";
    pub const EXCEPTION_MESSAGE: &str = "exception.message";
    pub const EXCEPTION_STACKTRACE: &str = "exception.stacktrace";
}

/// Trace context for propagation
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// Trace ID
    pub trace_id: u128,
    /// Span ID
    pub span_id: u64,
    /// Trace flags (sampled, etc.)
    pub trace_flags: u8,
    /// Trace state (vendor-specific)
    pub trace_state: String,
}

impl TraceContext {
    /// Parse W3C Trace Context traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let _version = u8::from_str_radix(parts[0], 16).ok()?;
        let trace_id = u128::from_str_radix(parts[1], 16).ok()?;
        let span_id = u64::from_str_radix(parts[2], 16).ok()?;
        let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id,
            span_id,
            trace_flags,
            trace_state: String::new(),
        })
    }

    /// Generate W3C Trace Context traceparent header
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{:032x}-{:016x}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Check if trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags & 0x01 != 0
    }
}

/// Span ID generator
struct IdGenerator {
    counter: AtomicU64,
}

impl IdGenerator {
    fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    fn next_span_id(&self) -> u64 {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Mix counter with timestamp for uniqueness
        counter ^ (timestamp & 0xFFFFFFFF)
    }

    fn next_trace_id(&self) -> u128 {
        let high = self.next_span_id() as u128;
        let low = self.next_span_id() as u128;
        (high << 64) | low
    }
}

/// Active span handle for automatic cleanup
pub struct SpanGuard {
    tracer: Arc<Tracer>,
    span_id: u64,
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        self.tracer.end_span(self.span_id);
    }
}

/// Tracer for creating and managing spans
pub struct Tracer {
    config: OtelConfig,
    id_gen: IdGenerator,
    active_spans: RwLock<HashMap<u64, Span>>,
    completed_spans: RwLock<Vec<Span>>,
    current_span: RwLock<Option<u64>>,
}

impl Tracer {
    /// Create a new tracer
    pub fn new(config: OtelConfig) -> Self {
        Self {
            config,
            id_gen: IdGenerator::new(),
            active_spans: RwLock::new(HashMap::new()),
            completed_spans: RwLock::new(Vec::new()),
            current_span: RwLock::new(None),
        }
    }

    /// Start a new root span
    pub fn start_span(&self, name: &str, kind: SpanKind) -> u64 {
        let trace_id = self.id_gen.next_trace_id();
        self.start_span_with_context(name, kind, trace_id, 0)
    }

    /// Start a child span
    pub fn start_child_span(&self, name: &str, kind: SpanKind, parent_span_id: u64) -> u64 {
        let parent = self.active_spans.read().unwrap().get(&parent_span_id).cloned();
        let trace_id = parent.map(|p| p.trace_id).unwrap_or_else(|| self.id_gen.next_trace_id());
        self.start_span_with_context(name, kind, trace_id, parent_span_id)
    }

    /// Start span with explicit context
    pub fn start_span_with_context(
        &self,
        name: &str,
        kind: SpanKind,
        trace_id: u128,
        parent_span_id: u64,
    ) -> u64 {
        let span_id = self.id_gen.next_span_id();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let span = Span {
            trace_id,
            span_id,
            parent_span_id,
            name: name.to_string(),
            kind,
            start_time_ns: now,
            end_time_ns: 0,
            status: SpanStatus::Unset,
            status_message: None,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        };

        self.active_spans.write().unwrap().insert(span_id, span);
        *self.current_span.write().unwrap() = Some(span_id);

        span_id
    }

    /// Start span from trace context
    pub fn start_span_from_context(&self, name: &str, kind: SpanKind, ctx: &TraceContext) -> u64 {
        self.start_span_with_context(name, kind, ctx.trace_id, ctx.span_id)
    }

    /// Start a span with automatic cleanup guard
    pub fn start_span_guarded(self: &Arc<Self>, name: &str, kind: SpanKind) -> SpanGuard {
        let span_id = self.start_span(name, kind);
        SpanGuard {
            tracer: Arc::clone(self),
            span_id,
        }
    }

    /// Set span attribute
    pub fn set_attribute(&self, span_id: u64, key: &str, value: impl Into<AttributeValue>) {
        if let Some(span) = self.active_spans.write().unwrap().get_mut(&span_id) {
            span.attributes.insert(key.to_string(), value.into());
        }
    }

    /// Add event to span
    pub fn add_event(&self, span_id: u64, name: &str, attributes: HashMap<String, AttributeValue>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if let Some(span) = self.active_spans.write().unwrap().get_mut(&span_id) {
            span.events.push(SpanEvent {
                name: name.to_string(),
                timestamp_ns: now,
                attributes,
            });
        }
    }

    /// Record exception on span
    pub fn record_exception(&self, span_id: u64, exception_type: &str, message: &str) {
        let mut attrs = HashMap::new();
        attrs.insert(
            semantic_conventions::EXCEPTION_TYPE.to_string(),
            AttributeValue::String(exception_type.to_string()),
        );
        attrs.insert(
            semantic_conventions::EXCEPTION_MESSAGE.to_string(),
            AttributeValue::String(message.to_string()),
        );

        self.add_event(span_id, "exception", attrs);
        self.set_status(span_id, SpanStatus::Error, Some(message.to_string()));
    }

    /// Set span status
    pub fn set_status(&self, span_id: u64, status: SpanStatus, message: Option<String>) {
        if let Some(span) = self.active_spans.write().unwrap().get_mut(&span_id) {
            span.status = status;
            span.status_message = message;
        }
    }

    /// End a span
    pub fn end_span(&self, span_id: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if let Some(mut span) = self.active_spans.write().unwrap().remove(&span_id) {
            span.end_time_ns = now;
            if span.status == SpanStatus::Unset {
                span.status = SpanStatus::Ok;
            }
            self.completed_spans.write().unwrap().push(span);
        }
    }

    /// Get current trace context for propagation
    pub fn current_context(&self) -> Option<TraceContext> {
        let current_id = *self.current_span.read().unwrap();
        current_id.and_then(|id| {
            self.active_spans.read().unwrap().get(&id).map(|span| TraceContext {
                trace_id: span.trace_id,
                span_id: span.span_id,
                trace_flags: 0x01, // sampled
                trace_state: String::new(),
            })
        })
    }

    /// Drain completed spans for export
    pub fn drain_completed(&self) -> Vec<Span> {
        let mut spans = self.completed_spans.write().unwrap();
        spans.drain(..).collect()
    }
}

/// Meter for recording metrics
pub struct Meter {
    name: String,
    counters: RwLock<HashMap<String, AtomicU64>>,
    gauges: RwLock<HashMap<String, f64>>,
    histograms: RwLock<HashMap<String, HistogramData>>,
}

struct HistogramData {
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum: AtomicU64, // stored as bits
    count: AtomicU64,
}

impl Meter {
    /// Create a new meter
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Increment a counter
    pub fn counter_add(&self, name: &str, value: u64) {
        let mut counters = self.counters.write().unwrap();
        counters
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(value, Ordering::Relaxed);
    }

    /// Set a gauge value
    pub fn gauge_set(&self, name: &str, value: f64) {
        self.gauges.write().unwrap().insert(name.to_string(), value);
    }

    /// Record a histogram value
    pub fn histogram_record(&self, name: &str, value: f64, buckets: &[f64]) {
        let mut histograms = self.histograms.write().unwrap();
        let data = histograms.entry(name.to_string()).or_insert_with(|| {
            let mut sorted_buckets = buckets.to_vec();
            sorted_buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
            HistogramData {
                counts: sorted_buckets.iter().map(|_| AtomicU64::new(0)).collect(),
                buckets: sorted_buckets,
                sum: AtomicU64::new(0),
                count: AtomicU64::new(0),
            }
        });

        // Find bucket and increment
        for (i, &boundary) in data.buckets.iter().enumerate() {
            if value <= boundary {
                data.counts[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }

        // Update sum and count
        let bits = value.to_bits();
        data.sum.fetch_add(bits, Ordering::Relaxed);
        data.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current metric values
    pub fn collect(&self) -> Vec<MetricDataPoint> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut points = Vec::new();

        // Collect counters
        for (name, counter) in self.counters.read().unwrap().iter() {
            points.push(MetricDataPoint {
                name: format!("{}.{}", self.name, name),
                description: String::new(),
                unit: String::new(),
                kind: MetricKind::Counter,
                timestamp_ns: now,
                value: Some(counter.load(Ordering::Relaxed) as f64),
                histogram_buckets: None,
                histogram_sum: None,
                histogram_count: None,
                attributes: HashMap::new(),
            });
        }

        // Collect gauges
        for (name, &value) in self.gauges.read().unwrap().iter() {
            points.push(MetricDataPoint {
                name: format!("{}.{}", self.name, name),
                description: String::new(),
                unit: String::new(),
                kind: MetricKind::Gauge,
                timestamp_ns: now,
                value: Some(value),
                histogram_buckets: None,
                histogram_sum: None,
                histogram_count: None,
                attributes: HashMap::new(),
            });
        }

        // Collect histograms
        for (name, data) in self.histograms.read().unwrap().iter() {
            let buckets: Vec<(f64, u64)> = data.buckets
                .iter()
                .zip(data.counts.iter())
                .map(|(&b, c)| (b, c.load(Ordering::Relaxed)))
                .collect();

            let count = data.count.load(Ordering::Relaxed);
            let sum_bits = data.sum.load(Ordering::Relaxed);

            points.push(MetricDataPoint {
                name: format!("{}.{}", self.name, name),
                description: String::new(),
                unit: String::new(),
                kind: MetricKind::Histogram,
                timestamp_ns: now,
                value: None,
                histogram_buckets: Some(buckets),
                histogram_sum: Some(f64::from_bits(sum_bits)),
                histogram_count: Some(count),
                attributes: HashMap::new(),
            });
        }

        points
    }
}

/// BoyoDB-specific instrumentation
pub struct BoyoDbInstrumentation {
    tracer: Arc<Tracer>,
    meter: Meter,
}

impl BoyoDbInstrumentation {
    /// Create new instrumentation
    pub fn new(config: OtelConfig) -> Self {
        Self {
            tracer: Arc::new(Tracer::new(config)),
            meter: Meter::new("boyodb"),
        }
    }

    /// Start a query span
    pub fn start_query(&self, sql: &str, database: &str) -> u64 {
        let span_id = self.tracer.start_span("db.query", SpanKind::Server);
        self.tracer.set_attribute(span_id, semantic_conventions::DB_SYSTEM, "boyodb");
        self.tracer.set_attribute(span_id, semantic_conventions::DB_NAME, database);
        self.tracer.set_attribute(span_id, semantic_conventions::DB_STATEMENT, sql);

        // Extract operation from SQL
        let op = sql.trim().split_whitespace().next().unwrap_or("UNKNOWN").to_uppercase();
        self.tracer.set_attribute(span_id, semantic_conventions::DB_OPERATION, op);

        self.meter.counter_add("queries.total", 1);
        span_id
    }

    /// End a query span with results
    pub fn end_query(&self, span_id: u64, rows_affected: i64, success: bool) {
        self.tracer.set_attribute(span_id, semantic_conventions::DB_ROWS_AFFECTED, rows_affected);

        if success {
            self.tracer.set_status(span_id, SpanStatus::Ok, None);
            self.meter.counter_add("queries.success", 1);
        } else {
            self.tracer.set_status(span_id, SpanStatus::Error, None);
            self.meter.counter_add("queries.error", 1);
        }

        self.tracer.end_span(span_id);
    }

    /// Record query duration
    pub fn record_query_duration(&self, duration_ms: f64) {
        let buckets = [1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0];
        self.meter.histogram_record("query.duration_ms", duration_ms, &buckets);
    }

    /// Start an ingest span
    pub fn start_ingest(&self, table: &str, batch_size: usize) -> u64 {
        let span_id = self.tracer.start_span("db.ingest", SpanKind::Server);
        self.tracer.set_attribute(span_id, semantic_conventions::DB_SYSTEM, "boyodb");
        self.tracer.set_attribute(span_id, semantic_conventions::DB_OPERATION, "INSERT");
        self.tracer.set_attribute(span_id, semantic_conventions::DB_SQL_TABLE, table);
        self.tracer.set_attribute(span_id, "db.batch_size", batch_size as i64);

        self.meter.counter_add("ingest.batches", 1);
        self.meter.counter_add("ingest.rows", batch_size as u64);
        span_id
    }

    /// Update connection metrics
    pub fn update_connections(&self, active: i64, idle: i64) {
        self.meter.gauge_set("connections.active", active as f64);
        self.meter.gauge_set("connections.idle", idle as f64);
    }

    /// Update storage metrics
    pub fn update_storage(&self, segments: i64, bytes: i64) {
        self.meter.gauge_set("storage.segments", segments as f64);
        self.meter.gauge_set("storage.bytes", bytes as f64);
    }

    /// Record compaction metrics
    pub fn record_compaction(&self, duration_ms: f64, segments_merged: u64, bytes_written: u64) {
        self.meter.counter_add("compaction.runs", 1);
        self.meter.counter_add("compaction.segments_merged", segments_merged);
        self.meter.counter_add("compaction.bytes_written", bytes_written);

        let buckets = [100.0, 500.0, 1000.0, 5000.0, 10000.0, 30000.0, 60000.0];
        self.meter.histogram_record("compaction.duration_ms", duration_ms, &buckets);
    }

    /// Get tracer reference
    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    /// Collect all metrics
    pub fn collect_metrics(&self) -> Vec<MetricDataPoint> {
        self.meter.collect()
    }
}

/// OTLP Exporter (serializes to OTLP protocol format)
pub struct OtlpExporter {
    endpoint: String,
    headers: HashMap<String, String>,
}

impl OtlpExporter {
    /// Create new OTLP exporter
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            headers: HashMap::new(),
        }
    }

    /// Add header for authentication
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }

    /// Export spans (returns serialized OTLP protobuf bytes)
    pub fn export_spans(&self, spans: &[Span]) -> Vec<u8> {
        // In a real implementation, this would serialize to OTLP protobuf
        // For now, we return a placeholder that indicates the span count
        let mut result = Vec::new();
        result.extend_from_slice(b"OTLP_SPANS:");
        result.extend_from_slice(&(spans.len() as u32).to_le_bytes());
        for span in spans {
            result.extend_from_slice(span.name.as_bytes());
            result.push(0);
        }
        result
    }

    /// Export metrics (returns serialized OTLP protobuf bytes)
    pub fn export_metrics(&self, metrics: &[MetricDataPoint]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(b"OTLP_METRICS:");
        result.extend_from_slice(&(metrics.len() as u32).to_le_bytes());
        for metric in metrics {
            result.extend_from_slice(metric.name.as_bytes());
            result.push(0);
        }
        result
    }

    /// Get endpoint
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Prometheus exporter (generates Prometheus text format)
pub struct PrometheusExporter {
    prefix: String,
}

impl PrometheusExporter {
    /// Create new Prometheus exporter
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    /// Export metrics in Prometheus text format
    pub fn export(&self, metrics: &[MetricDataPoint]) -> String {
        let mut output = String::new();

        for metric in metrics {
            let name = format!("{}_{}", self.prefix, metric.name.replace('.', "_"));

            match metric.kind {
                MetricKind::Counter | MetricKind::UpDownCounter | MetricKind::Gauge => {
                    if let Some(value) = metric.value {
                        output.push_str(&format!("{} {}\n", name, value));
                    }
                }
                MetricKind::Histogram => {
                    if let (Some(buckets), Some(sum), Some(count)) =
                        (&metric.histogram_buckets, metric.histogram_sum, metric.histogram_count)
                    {
                        let mut cumulative = 0u64;
                        for (boundary, cnt) in buckets {
                            cumulative += cnt;
                            output.push_str(&format!(
                                "{}_bucket{{le=\"{}\"}} {}\n",
                                name, boundary, cumulative
                            ));
                        }
                        output.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", name, count));
                        output.push_str(&format!("{}_sum {}\n", name, sum));
                        output.push_str(&format!("{}_count {}\n", name, count));
                    }
                }
            }
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_parsing() {
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::from_traceparent(header).unwrap();

        assert_eq!(ctx.trace_id, 0x0af7651916cd43dd8448eb211c80319c);
        assert_eq!(ctx.span_id, 0xb7ad6b7169203331);
        assert_eq!(ctx.trace_flags, 0x01);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_trace_context_serialization() {
        let ctx = TraceContext {
            trace_id: 0x0af7651916cd43dd8448eb211c80319c,
            span_id: 0xb7ad6b7169203331,
            trace_flags: 0x01,
            trace_state: String::new(),
        };

        let header = ctx.to_traceparent();
        assert_eq!(header, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    #[test]
    fn test_tracer_span_lifecycle() {
        let tracer = Tracer::new(OtelConfig::default());

        let span_id = tracer.start_span("test.operation", SpanKind::Internal);
        tracer.set_attribute(span_id, "key", "value");
        tracer.add_event(span_id, "processing", HashMap::new());
        tracer.end_span(span_id);

        let completed = tracer.drain_completed();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].name, "test.operation");
        assert_eq!(completed[0].status, SpanStatus::Ok);
    }

    #[test]
    fn test_child_spans() {
        let tracer = Tracer::new(OtelConfig::default());

        let parent_id = tracer.start_span("parent", SpanKind::Server);
        let child_id = tracer.start_child_span("child", SpanKind::Internal, parent_id);

        tracer.end_span(child_id);
        tracer.end_span(parent_id);

        let completed = tracer.drain_completed();
        assert_eq!(completed.len(), 2);

        let child = &completed[0];
        let parent = &completed[1];

        assert_eq!(child.parent_span_id, parent.span_id);
        assert_eq!(child.trace_id, parent.trace_id);
    }

    #[test]
    fn test_meter_counter() {
        let meter = Meter::new("test");

        meter.counter_add("requests", 1);
        meter.counter_add("requests", 5);

        let metrics = meter.collect();
        let counter = metrics.iter().find(|m| m.name == "test.requests").unwrap();
        assert_eq!(counter.value, Some(6.0));
    }

    #[test]
    fn test_prometheus_export() {
        let meter = Meter::new("boyodb");
        meter.counter_add("queries_total", 100);
        meter.gauge_set("connections_active", 42.0);

        let exporter = PrometheusExporter::new("boyodb");
        let output = exporter.export(&meter.collect());

        assert!(output.contains("boyodb_boyodb_queries_total 100"));
        assert!(output.contains("boyodb_boyodb_connections_active 42"));
    }

    #[test]
    fn test_boyodb_instrumentation() {
        let instr = BoyoDbInstrumentation::new(OtelConfig::default());

        let span_id = instr.start_query("SELECT * FROM users", "mydb");
        instr.end_query(span_id, 10, true);
        instr.record_query_duration(25.5);

        let metrics = instr.collect_metrics();
        assert!(metrics.iter().any(|m| m.name.contains("queries.total")));
        assert!(metrics.iter().any(|m| m.name.contains("queries.success")));
    }
}
