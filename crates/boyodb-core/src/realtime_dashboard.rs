//! Real-time Dashboard Module
//!
//! Provides WebSocket-based real-time metrics and data streaming.
//! Features:
//! - Live metric updates via WebSocket
//! - Subscription management
//! - Dashboard configurations
//! - Alert broadcasting
//! - Query result streaming

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Dashboard configuration
#[derive(Debug, Clone)]
pub struct Dashboard {
    /// Dashboard ID
    pub id: String,
    /// Dashboard name
    pub name: String,
    /// Description
    pub description: String,
    /// Widgets
    pub widgets: Vec<Widget>,
    /// Refresh interval in seconds
    pub refresh_interval: u64,
    /// Owner
    pub owner: String,
    /// Is public
    pub is_public: bool,
    /// Tags
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: u64,
    /// Modified timestamp
    pub modified_at: u64,
}

impl Dashboard {
    /// Create new dashboard
    pub fn new(id: &str, name: &str, owner: &str) -> Self {
        let now = current_timestamp();
        Dashboard {
            id: id.to_string(),
            name: name.to_string(),
            description: String::new(),
            widgets: Vec::new(),
            refresh_interval: 5,
            owner: owner.to_string(),
            is_public: false,
            tags: Vec::new(),
            created_at: now,
            modified_at: now,
        }
    }

    /// Add widget
    pub fn add_widget(&mut self, widget: Widget) {
        self.widgets.push(widget);
        self.modified_at = current_timestamp();
    }

    /// Remove widget
    pub fn remove_widget(&mut self, widget_id: &str) -> bool {
        let len_before = self.widgets.len();
        self.widgets.retain(|w| w.id != widget_id);
        if self.widgets.len() != len_before {
            self.modified_at = current_timestamp();
            true
        } else {
            false
        }
    }
}

/// Dashboard widget
#[derive(Debug, Clone)]
pub struct Widget {
    /// Widget ID
    pub id: String,
    /// Widget type
    pub widget_type: WidgetType,
    /// Title
    pub title: String,
    /// Data source query
    pub query: String,
    /// Position and size
    pub layout: WidgetLayout,
    /// Display options
    pub options: WidgetOptions,
}

/// Widget types
#[derive(Debug, Clone, PartialEq)]
pub enum WidgetType {
    /// Line chart
    LineChart,
    /// Bar chart
    BarChart,
    /// Pie chart
    PieChart,
    /// Gauge/meter
    Gauge,
    /// Single stat/KPI
    Stat,
    /// Table
    Table,
    /// Heatmap
    Heatmap,
    /// Text/Markdown
    Text,
    /// Alert list
    AlertList,
    /// Log viewer
    LogViewer,
}

/// Widget layout
#[derive(Debug, Clone)]
pub struct WidgetLayout {
    /// X position (grid units)
    pub x: u32,
    /// Y position (grid units)
    pub y: u32,
    /// Width (grid units)
    pub width: u32,
    /// Height (grid units)
    pub height: u32,
}

impl Default for WidgetLayout {
    fn default() -> Self {
        WidgetLayout {
            x: 0,
            y: 0,
            width: 4,
            height: 3,
        }
    }
}

/// Widget display options
#[derive(Debug, Clone, Default)]
pub struct WidgetOptions {
    /// Color scheme
    pub color_scheme: Option<String>,
    /// Show legend
    pub show_legend: bool,
    /// Thresholds for coloring
    pub thresholds: Vec<Threshold>,
    /// Unit
    pub unit: Option<String>,
    /// Decimal places
    pub decimals: Option<u8>,
    /// Custom options
    pub custom: HashMap<String, String>,
}

/// Threshold for value coloring
#[derive(Debug, Clone)]
pub struct Threshold {
    /// Threshold value
    pub value: f64,
    /// Color for values at or above this threshold
    pub color: String,
    /// Label
    pub label: Option<String>,
}

/// Metric data point
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// Timestamp (unix millis)
    pub timestamp: u64,
    /// Value
    pub value: f64,
    /// Labels/tags
    pub labels: HashMap<String, String>,
}

/// Metric series
#[derive(Debug, Clone)]
pub struct MetricSeries {
    /// Series name
    pub name: String,
    /// Data points
    pub points: Vec<MetricPoint>,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Subscription to metric/data updates
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Subscription ID
    pub id: String,
    /// Client ID
    pub client_id: String,
    /// Subscription type
    pub sub_type: SubscriptionType,
    /// Filter/query
    pub filter: String,
    /// Created at
    pub created_at: u64,
    /// Is active
    pub active: bool,
}

/// Subscription types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionType {
    /// Metric updates
    Metrics,
    /// Query results
    Query,
    /// Alerts
    Alerts,
    /// Logs
    Logs,
    /// Table changes (CDC)
    TableChanges,
    /// Dashboard updates
    Dashboard,
}

/// WebSocket message
#[derive(Debug, Clone)]
pub struct WsMessage {
    /// Message type
    pub msg_type: MessageType,
    /// Subscription ID (if applicable)
    pub subscription_id: Option<String>,
    /// Payload
    pub payload: MessagePayload,
    /// Timestamp
    pub timestamp: u64,
}

/// Message types
#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    /// Subscribe request
    Subscribe,
    /// Unsubscribe request
    Unsubscribe,
    /// Data update
    Data,
    /// Error
    Error,
    /// Ping/heartbeat
    Ping,
    /// Pong response
    Pong,
    /// Acknowledgment
    Ack,
}

/// Message payload
#[derive(Debug, Clone)]
pub enum MessagePayload {
    /// Empty payload
    Empty,
    /// Subscription request
    SubscribeRequest {
        sub_type: SubscriptionType,
        filter: String,
    },
    /// Metric data
    MetricData(Vec<MetricSeries>),
    /// Query results
    QueryResults {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Alert
    Alert(Alert),
    /// Log entries
    Logs(Vec<LogEntry>),
    /// Error message
    Error(String),
    /// Text message
    Text(String),
}

/// Alert
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert ID
    pub id: String,
    /// Alert name
    pub name: String,
    /// Severity
    pub severity: AlertSeverity,
    /// Message
    pub message: String,
    /// Source
    pub source: String,
    /// Timestamp
    pub timestamp: u64,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Is resolved
    pub resolved: bool,
}

/// Alert severity
#[derive(Debug, Clone, PartialEq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Log level
    pub level: String,
    /// Message
    pub message: String,
    /// Source
    pub source: String,
    /// Additional fields
    pub fields: HashMap<String, String>,
}

/// Real-time metrics registry
pub struct MetricsRegistry {
    /// Current metric values
    metrics: HashMap<String, MetricSeries>,
    /// Metric history (for time-series queries)
    history: HashMap<String, Vec<MetricPoint>>,
    /// History retention (number of points)
    retention: usize,
}

impl MetricsRegistry {
    /// Create new registry
    pub fn new(retention: usize) -> Self {
        MetricsRegistry {
            metrics: HashMap::new(),
            history: HashMap::new(),
            retention,
        }
    }

    /// Record metric value
    pub fn record(&mut self, name: &str, value: f64, labels: HashMap<String, String>) {
        let point = MetricPoint {
            timestamp: current_timestamp_millis(),
            value,
            labels: labels.clone(),
        };

        // Update current value
        self.metrics
            .entry(name.to_string())
            .or_insert_with(|| MetricSeries {
                name: name.to_string(),
                points: Vec::new(),
                metadata: HashMap::new(),
            })
            .points = vec![point.clone()];

        // Add to history
        let history = self.history.entry(name.to_string()).or_default();
        history.push(point);

        // Trim history
        if history.len() > self.retention {
            history.remove(0);
        }
    }

    /// Get current metric value
    pub fn get(&self, name: &str) -> Option<f64> {
        self.metrics
            .get(name)
            .and_then(|s| s.points.first())
            .map(|p| p.value)
    }

    /// Get metric history
    pub fn get_history(&self, name: &str, since: u64) -> Vec<MetricPoint> {
        self.history
            .get(name)
            .map(|h| h.iter().filter(|p| p.timestamp >= since).cloned().collect())
            .unwrap_or_default()
    }

    /// Get all current metrics
    pub fn get_all(&self) -> Vec<&MetricSeries> {
        self.metrics.values().collect()
    }

    /// List metric names
    pub fn list_names(&self) -> Vec<&String> {
        self.metrics.keys().collect()
    }
}

/// Subscription manager
pub struct SubscriptionManager {
    /// Active subscriptions
    subscriptions: HashMap<String, Subscription>,
    /// Subscriptions by client
    by_client: HashMap<String, HashSet<String>>,
    /// Subscriptions by type
    by_type: HashMap<SubscriptionType, HashSet<String>>,
}

impl SubscriptionManager {
    /// Create new manager
    pub fn new() -> Self {
        SubscriptionManager {
            subscriptions: HashMap::new(),
            by_client: HashMap::new(),
            by_type: HashMap::new(),
        }
    }

    /// Add subscription
    pub fn subscribe(
        &mut self,
        client_id: &str,
        sub_type: SubscriptionType,
        filter: &str,
    ) -> String {
        let sub_id = generate_id();

        let subscription = Subscription {
            id: sub_id.clone(),
            client_id: client_id.to_string(),
            sub_type: sub_type.clone(),
            filter: filter.to_string(),
            created_at: current_timestamp(),
            active: true,
        };

        self.subscriptions.insert(sub_id.clone(), subscription);

        self.by_client
            .entry(client_id.to_string())
            .or_default()
            .insert(sub_id.clone());

        self.by_type
            .entry(sub_type)
            .or_default()
            .insert(sub_id.clone());

        sub_id
    }

    /// Remove subscription
    pub fn unsubscribe(&mut self, sub_id: &str) -> bool {
        if let Some(sub) = self.subscriptions.remove(sub_id) {
            if let Some(client_subs) = self.by_client.get_mut(&sub.client_id) {
                client_subs.remove(sub_id);
            }
            if let Some(type_subs) = self.by_type.get_mut(&sub.sub_type) {
                type_subs.remove(sub_id);
            }
            true
        } else {
            false
        }
    }

    /// Remove all subscriptions for a client
    pub fn unsubscribe_client(&mut self, client_id: &str) {
        if let Some(sub_ids) = self.by_client.remove(client_id) {
            for sub_id in sub_ids {
                if let Some(sub) = self.subscriptions.remove(&sub_id) {
                    if let Some(type_subs) = self.by_type.get_mut(&sub.sub_type) {
                        type_subs.remove(&sub_id);
                    }
                }
            }
        }
    }

    /// Get subscriptions by type
    pub fn get_by_type(&self, sub_type: &SubscriptionType) -> Vec<&Subscription> {
        self.by_type
            .get(sub_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.subscriptions.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get subscriptions for client
    pub fn get_by_client(&self, client_id: &str) -> Vec<&Subscription> {
        self.by_client
            .get(client_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.subscriptions.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get subscription count
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Dashboard manager
pub struct DashboardManager {
    /// Dashboards
    dashboards: HashMap<String, Dashboard>,
    /// Dashboards by owner
    by_owner: HashMap<String, HashSet<String>>,
}

impl DashboardManager {
    /// Create new manager
    pub fn new() -> Self {
        DashboardManager {
            dashboards: HashMap::new(),
            by_owner: HashMap::new(),
        }
    }

    /// Create dashboard
    pub fn create(&mut self, dashboard: Dashboard) -> Result<String, String> {
        if self.dashboards.contains_key(&dashboard.id) {
            return Err(format!("Dashboard '{}' already exists", dashboard.id));
        }

        let id = dashboard.id.clone();
        let owner = dashboard.owner.clone();

        self.dashboards.insert(id.clone(), dashboard);
        self.by_owner.entry(owner).or_default().insert(id.clone());

        Ok(id)
    }

    /// Get dashboard
    pub fn get(&self, id: &str) -> Option<&Dashboard> {
        self.dashboards.get(id)
    }

    /// Update dashboard
    pub fn update(&mut self, dashboard: Dashboard) -> Result<(), String> {
        if !self.dashboards.contains_key(&dashboard.id) {
            return Err(format!("Dashboard '{}' not found", dashboard.id));
        }

        self.dashboards.insert(dashboard.id.clone(), dashboard);
        Ok(())
    }

    /// Delete dashboard
    pub fn delete(&mut self, id: &str) -> bool {
        if let Some(dashboard) = self.dashboards.remove(id) {
            if let Some(owner_dashboards) = self.by_owner.get_mut(&dashboard.owner) {
                owner_dashboards.remove(id);
            }
            true
        } else {
            false
        }
    }

    /// List dashboards for owner
    pub fn list_by_owner(&self, owner: &str) -> Vec<&Dashboard> {
        self.by_owner
            .get(owner)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.dashboards.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List public dashboards
    pub fn list_public(&self) -> Vec<&Dashboard> {
        self.dashboards.values().filter(|d| d.is_public).collect()
    }

    /// List all dashboards
    pub fn list_all(&self) -> Vec<&Dashboard> {
        self.dashboards.values().collect()
    }
}

impl Default for DashboardManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Real-time streaming hub
pub struct StreamingHub {
    /// Metrics registry
    pub metrics: Arc<RwLock<MetricsRegistry>>,
    /// Subscription manager
    pub subscriptions: Arc<RwLock<SubscriptionManager>>,
    /// Dashboard manager
    pub dashboards: Arc<RwLock<DashboardManager>>,
    /// Alert buffer
    alerts: Arc<RwLock<Vec<Alert>>>,
    /// Max alerts to keep
    max_alerts: usize,
}

impl StreamingHub {
    /// Create new hub
    pub fn new(metrics_retention: usize, max_alerts: usize) -> Self {
        StreamingHub {
            metrics: Arc::new(RwLock::new(MetricsRegistry::new(metrics_retention))),
            subscriptions: Arc::new(RwLock::new(SubscriptionManager::new())),
            dashboards: Arc::new(RwLock::new(DashboardManager::new())),
            alerts: Arc::new(RwLock::new(Vec::new())),
            max_alerts,
        }
    }

    /// Record metric
    pub fn record_metric(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let mut metrics = self.metrics.write();
        metrics.record(name, value, labels);
    }

    /// Broadcast alert
    pub fn broadcast_alert(&self, alert: Alert) {
        let mut alerts = self.alerts.write();
        alerts.push(alert);

        // Trim if needed
        if alerts.len() > self.max_alerts {
            alerts.remove(0);
        }
    }

    /// Get recent alerts
    pub fn get_alerts(&self, limit: usize) -> Vec<Alert> {
        let alerts = self.alerts.read();
        alerts.iter().rev().take(limit).cloned().collect()
    }

    /// Subscribe to updates
    pub fn subscribe(
        &self,
        client_id: &str,
        sub_type: SubscriptionType,
        filter: &str,
    ) -> Result<String, String> {
        let mut subs = self.subscriptions.write();
        Ok(subs.subscribe(client_id, sub_type, filter))
    }

    /// Unsubscribe
    pub fn unsubscribe(&self, sub_id: &str) -> Result<bool, String> {
        let mut subs = self.subscriptions.write();
        Ok(subs.unsubscribe(sub_id))
    }

    /// Get metrics for dashboard widget
    pub fn get_widget_data(&self, query: &str, since: u64) -> Vec<MetricSeries> {
        // Simplified - in production, parse and execute query
        let metrics = self.metrics.read();
        metrics
            .list_names()
            .iter()
            .filter(|name| name.contains(query) || query.is_empty())
            .map(|name| {
                let points = metrics.get_history(name, since);
                MetricSeries {
                    name: (*name).clone(),
                    points,
                    metadata: HashMap::new(),
                }
            })
            .collect()
    }

    /// Get subscription count
    pub fn subscription_count(&self) -> usize {
        let subs = self.subscriptions.read();
        subs.count()
    }
}

impl Default for StreamingHub {
    fn default() -> Self {
        Self::new(1000, 100)
    }
}

// Helper functions

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn generate_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", now.as_secs(), now.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_creation() {
        let mut dashboard = Dashboard::new("dash1", "Test Dashboard", "admin");

        let widget = Widget {
            id: "w1".to_string(),
            widget_type: WidgetType::LineChart,
            title: "CPU Usage".to_string(),
            query: "system.cpu".to_string(),
            layout: WidgetLayout::default(),
            options: WidgetOptions::default(),
        };

        dashboard.add_widget(widget);
        assert_eq!(dashboard.widgets.len(), 1);
    }

    #[test]
    fn test_metrics_registry() {
        let mut registry = MetricsRegistry::new(100);

        registry.record("cpu", 45.5, HashMap::new());
        registry.record("cpu", 50.0, HashMap::new());

        assert_eq!(registry.get("cpu"), Some(50.0));

        let history = registry.get_history("cpu", 0);
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_subscription_manager() {
        let mut manager = SubscriptionManager::new();

        let sub_id = manager.subscribe("client1", SubscriptionType::Metrics, "cpu.*");
        assert_eq!(manager.count(), 1);

        let subs = manager.get_by_type(&SubscriptionType::Metrics);
        assert_eq!(subs.len(), 1);

        manager.unsubscribe(&sub_id);
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_streaming_hub() {
        let hub = StreamingHub::new(100, 10);

        hub.record_metric("test.metric", 42.0, HashMap::new());

        let data = hub.get_widget_data("test", 0);
        assert!(!data.is_empty());
    }

    #[test]
    fn test_alert_broadcast() {
        let hub = StreamingHub::new(100, 10);

        let alert = Alert {
            id: "a1".to_string(),
            name: "High CPU".to_string(),
            severity: AlertSeverity::Warning,
            message: "CPU usage above 80%".to_string(),
            source: "system".to_string(),
            timestamp: current_timestamp(),
            labels: HashMap::new(),
            resolved: false,
        };

        hub.broadcast_alert(alert);

        let alerts = hub.get_alerts(10);
        assert_eq!(alerts.len(), 1);
    }
}
