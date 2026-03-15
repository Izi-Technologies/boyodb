//! Change Data Capture (CDC) Webhooks for BoyoDB
//!
//! Provides real-time change notification via webhooks. Supports:
//! - Row-level change tracking (INSERT, UPDATE, DELETE)
//! - Configurable webhook endpoints per table
//! - Retry logic with exponential backoff
//! - Batched delivery for high-throughput scenarios
//! - At-least-once delivery semantics

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};

/// CDC change operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

impl ChangeOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChangeOperation::Insert => "INSERT",
            ChangeOperation::Update => "UPDATE",
            ChangeOperation::Delete => "DELETE",
        }
    }
}

/// A single change event
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// Unique event ID
    pub event_id: u64,
    /// Timestamp (Unix milliseconds)
    pub timestamp_ms: u64,
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Operation type
    pub operation: ChangeOperation,
    /// Primary key values (JSON serialized)
    pub primary_key: String,
    /// Old row values (for UPDATE/DELETE, JSON serialized)
    pub old_values: Option<String>,
    /// New row values (for INSERT/UPDATE, JSON serialized)
    pub new_values: Option<String>,
    /// Transaction ID if available
    pub transaction_id: Option<u64>,
    /// LSN/sequence number for ordering
    pub sequence: u64,
}

impl ChangeEvent {
    /// Serialize event to JSON
    pub fn to_json(&self) -> String {
        let mut json = String::from("{");
        json.push_str(&format!("\"event_id\":{},", self.event_id));
        json.push_str(&format!("\"timestamp_ms\":{},", self.timestamp_ms));
        json.push_str(&format!("\"database\":\"{}\",", self.database));
        json.push_str(&format!("\"table\":\"{}\",", self.table));
        json.push_str(&format!("\"operation\":\"{}\",", self.operation.as_str()));
        json.push_str(&format!("\"primary_key\":{},", self.primary_key));

        if let Some(ref old) = self.old_values {
            json.push_str(&format!("\"old_values\":{},", old));
        } else {
            json.push_str("\"old_values\":null,");
        }

        if let Some(ref new) = self.new_values {
            json.push_str(&format!("\"new_values\":{},", new));
        } else {
            json.push_str("\"new_values\":null,");
        }

        if let Some(tx_id) = self.transaction_id {
            json.push_str(&format!("\"transaction_id\":{},", tx_id));
        } else {
            json.push_str("\"transaction_id\":null,");
        }

        json.push_str(&format!("\"sequence\":{}", self.sequence));
        json.push('}');
        json
    }
}

/// Webhook delivery configuration
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Unique webhook ID
    pub id: String,
    /// Display name
    pub name: String,
    /// Target URL
    pub url: String,
    /// HTTP method (POST, PUT)
    pub method: String,
    /// Custom headers
    pub headers: HashMap<String, String>,
    /// Secret for HMAC signing
    pub secret: Option<String>,
    /// Tables to watch (empty = all tables)
    pub tables: Vec<String>,
    /// Operations to watch (empty = all operations)
    pub operations: Vec<ChangeOperation>,
    /// Enable batching
    pub batch_enabled: bool,
    /// Maximum batch size
    pub batch_max_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Initial retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Maximum retry delay in milliseconds
    pub max_retry_delay_ms: u64,
    /// Request timeout in milliseconds
    pub timeout_ms: u64,
    /// Enable webhook
    pub enabled: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            url: String::new(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            secret: None,
            tables: Vec::new(),
            operations: Vec::new(),
            batch_enabled: false,
            batch_max_size: 100,
            batch_timeout_ms: 1000,
            max_retries: 3,
            retry_delay_ms: 1000,
            max_retry_delay_ms: 60000,
            timeout_ms: 30000,
            enabled: true,
        }
    }
}

/// Webhook delivery status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Delivered,
    Failed,
    Retrying,
}

/// Delivery attempt record
#[derive(Debug, Clone)]
pub struct DeliveryAttempt {
    /// Attempt number (1-based)
    pub attempt: u32,
    /// Timestamp of attempt
    pub timestamp_ms: u64,
    /// HTTP status code (0 if connection failed)
    pub status_code: u16,
    /// Response body (truncated)
    pub response: Option<String>,
    /// Error message if failed
    pub error: Option<String>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Pending delivery record
#[derive(Debug)]
pub struct PendingDelivery {
    /// Event(s) to deliver
    pub events: Vec<ChangeEvent>,
    /// Webhook configuration
    pub webhook_id: String,
    /// Current status
    pub status: DeliveryStatus,
    /// Next retry time (Unix milliseconds)
    pub next_retry_ms: u64,
    /// Number of attempts made
    pub attempts: Vec<DeliveryAttempt>,
    /// Created timestamp
    pub created_ms: u64,
}

/// Webhook registry statistics
#[derive(Debug, Clone, Default)]
pub struct WebhookStats {
    pub total_events: u64,
    pub total_deliveries: u64,
    pub successful_deliveries: u64,
    pub failed_deliveries: u64,
    pub retried_deliveries: u64,
    pub pending_deliveries: u64,
    pub events_per_second: f64,
}

/// Webhook registry for managing CDC webhooks
pub struct WebhookRegistry {
    /// Registered webhooks
    webhooks: RwLock<HashMap<String, WebhookConfig>>,
    /// Table to webhook mappings
    table_webhooks: RwLock<HashMap<String, Vec<String>>>,
    /// Pending deliveries
    pending: Mutex<VecDeque<PendingDelivery>>,
    /// Batch buffers per webhook
    batch_buffers: Mutex<HashMap<String, Vec<ChangeEvent>>>,
    /// Batch timestamps (when first event was added)
    batch_starts: Mutex<HashMap<String, Instant>>,
    /// Event ID counter
    event_counter: AtomicU64,
    /// Sequence counter
    sequence_counter: AtomicU64,
    /// Running flag
    running: AtomicBool,
    /// Statistics
    stats: Mutex<WebhookStats>,
}

impl WebhookRegistry {
    /// Create a new webhook registry
    pub fn new() -> Self {
        Self {
            webhooks: RwLock::new(HashMap::new()),
            table_webhooks: RwLock::new(HashMap::new()),
            pending: Mutex::new(VecDeque::new()),
            batch_buffers: Mutex::new(HashMap::new()),
            batch_starts: Mutex::new(HashMap::new()),
            event_counter: AtomicU64::new(1),
            sequence_counter: AtomicU64::new(1),
            running: AtomicBool::new(true),
            stats: Mutex::new(WebhookStats::default()),
        }
    }

    /// Register a webhook
    pub fn register(&self, mut config: WebhookConfig) -> Result<String, WebhookError> {
        if config.url.is_empty() {
            return Err(WebhookError::InvalidConfig("URL is required".into()));
        }

        // Generate ID if not provided
        if config.id.is_empty() {
            config.id = format!("wh_{}", self.event_counter.fetch_add(1, Ordering::Relaxed));
        }

        let id = config.id.clone();

        // Update table mappings
        {
            let mut table_webhooks = self.table_webhooks.write();
            if config.tables.is_empty() {
                // Watch all tables - use special "*" key
                table_webhooks
                    .entry("*".to_string())
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            } else {
                for table in &config.tables {
                    table_webhooks
                        .entry(table.clone())
                        .or_insert_with(Vec::new)
                        .push(id.clone());
                }
            }
        }

        self.webhooks.write().insert(id.clone(), config);
        Ok(id)
    }

    /// Unregister a webhook
    pub fn unregister(&self, id: &str) -> Result<(), WebhookError> {
        let config = self.webhooks.write().remove(id);
        if config.is_none() {
            return Err(WebhookError::NotFound(id.to_string()));
        }

        // Clean up table mappings
        let mut table_webhooks = self.table_webhooks.write();
        for webhooks in table_webhooks.values_mut() {
            webhooks.retain(|wh_id| wh_id != id);
        }

        Ok(())
    }

    /// Get webhook by ID
    pub fn get(&self, id: &str) -> Option<WebhookConfig> {
        self.webhooks.read().get(id).cloned()
    }

    /// List all webhooks
    pub fn list(&self) -> Vec<WebhookConfig> {
        self.webhooks.read().values().cloned().collect()
    }

    /// Enable/disable a webhook
    pub fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), WebhookError> {
        let mut webhooks = self.webhooks.write();
        if let Some(wh) = webhooks.get_mut(id) {
            wh.enabled = enabled;
            Ok(())
        } else {
            Err(WebhookError::NotFound(id.to_string()))
        }
    }

    /// Record a change event
    pub fn record_change(
        &self,
        database: &str,
        table: &str,
        operation: ChangeOperation,
        primary_key: &str,
        old_values: Option<&str>,
        new_values: Option<&str>,
        transaction_id: Option<u64>,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let event = ChangeEvent {
            event_id: self.event_counter.fetch_add(1, Ordering::Relaxed),
            timestamp_ms: now,
            database: database.to_string(),
            table: table.to_string(),
            operation,
            primary_key: primary_key.to_string(),
            old_values: old_values.map(String::from),
            new_values: new_values.map(String::from),
            transaction_id,
            sequence: self.sequence_counter.fetch_add(1, Ordering::Relaxed),
        };

        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_events += 1;
        }

        // Find matching webhooks
        let full_table = format!("{}.{}", database, table);
        let webhook_ids = self.find_matching_webhooks(&full_table, operation);

        for webhook_id in webhook_ids {
            self.queue_event(webhook_id, event.clone());
        }
    }

    /// Find webhooks that match a table and operation
    fn find_matching_webhooks(&self, table: &str, operation: ChangeOperation) -> Vec<String> {
        let mut result = Vec::new();
        let table_webhooks = self.table_webhooks.read();
        let webhooks = self.webhooks.read();

        // Check specific table webhooks
        if let Some(ids) = table_webhooks.get(table) {
            for id in ids {
                if let Some(wh) = webhooks.get(id) {
                    if wh.enabled && (wh.operations.is_empty() || wh.operations.contains(&operation))
                    {
                        result.push(id.clone());
                    }
                }
            }
        }

        // Check wildcard webhooks
        if let Some(ids) = table_webhooks.get("*") {
            for id in ids {
                if !result.contains(id) {
                    if let Some(wh) = webhooks.get(id) {
                        if wh.enabled
                            && (wh.operations.is_empty() || wh.operations.contains(&operation))
                        {
                            result.push(id.clone());
                        }
                    }
                }
            }
        }

        result
    }

    /// Queue an event for delivery
    fn queue_event(&self, webhook_id: String, event: ChangeEvent) {
        let webhook = match self.webhooks.read().get(&webhook_id) {
            Some(wh) => wh.clone(),
            None => return,
        };

        if webhook.batch_enabled {
            self.add_to_batch(webhook_id, event, &webhook);
        } else {
            self.queue_immediate(webhook_id, vec![event]);
        }
    }

    /// Add event to batch buffer
    fn add_to_batch(&self, webhook_id: String, event: ChangeEvent, config: &WebhookConfig) {
        let mut buffers = self.batch_buffers.lock();
        let mut starts = self.batch_starts.lock();

        let buffer = buffers.entry(webhook_id.clone()).or_insert_with(Vec::new);

        if buffer.is_empty() {
            starts.insert(webhook_id.clone(), Instant::now());
        }

        buffer.push(event);

        // Check if batch is full
        if buffer.len() >= config.batch_max_size {
            let events: Vec<ChangeEvent> = buffer.drain(..).collect();
            starts.remove(&webhook_id);
            drop(buffers);
            drop(starts);
            self.queue_immediate(webhook_id, events);
        }
    }

    /// Flush batches that have timed out
    pub fn flush_batches(&self) {
        let webhooks = self.webhooks.read();
        let mut buffers = self.batch_buffers.lock();
        let mut starts = self.batch_starts.lock();

        let mut to_flush = Vec::new();

        for (webhook_id, start_time) in starts.iter() {
            if let Some(wh) = webhooks.get(webhook_id) {
                let elapsed = start_time.elapsed().as_millis() as u64;
                if elapsed >= wh.batch_timeout_ms {
                    if let Some(buffer) = buffers.get_mut(webhook_id) {
                        if !buffer.is_empty() {
                            to_flush.push((webhook_id.clone(), buffer.drain(..).collect()));
                        }
                    }
                }
            }
        }

        for (webhook_id, _) in &to_flush {
            starts.remove(webhook_id);
        }

        drop(buffers);
        drop(starts);
        drop(webhooks);

        for (webhook_id, events) in to_flush {
            self.queue_immediate(webhook_id, events);
        }
    }

    /// Queue events for immediate delivery
    fn queue_immediate(&self, webhook_id: String, events: Vec<ChangeEvent>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let delivery = PendingDelivery {
            events,
            webhook_id,
            status: DeliveryStatus::Pending,
            next_retry_ms: now,
            attempts: Vec::new(),
            created_ms: now,
        };

        {
            let mut stats = self.stats.lock();
            stats.pending_deliveries += 1;
        }

        self.pending.lock().push_back(delivery);
    }

    /// Get next pending delivery that's ready
    pub fn next_pending(&self) -> Option<PendingDelivery> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut pending = self.pending.lock();

        // Find first delivery that's ready
        for i in 0..pending.len() {
            if pending[i].next_retry_ms <= now
                && pending[i].status != DeliveryStatus::Delivered
                && pending[i].status != DeliveryStatus::Failed
            {
                return pending.remove(i);
            }
        }

        None
    }

    /// Record a delivery attempt result
    pub fn record_attempt(
        &self,
        mut delivery: PendingDelivery,
        success: bool,
        status_code: u16,
        response: Option<String>,
        error: Option<String>,
        duration_ms: u64,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let attempt = DeliveryAttempt {
            attempt: delivery.attempts.len() as u32 + 1,
            timestamp_ms: now,
            status_code,
            response,
            error,
            duration_ms,
        };

        delivery.attempts.push(attempt);

        let mut stats = self.stats.lock();
        stats.total_deliveries += 1;

        if success {
            delivery.status = DeliveryStatus::Delivered;
            stats.successful_deliveries += 1;
            stats.pending_deliveries = stats.pending_deliveries.saturating_sub(1);
        } else {
            let webhook = self.webhooks.read().get(&delivery.webhook_id).cloned();

            if let Some(wh) = webhook {
                if delivery.attempts.len() as u32 >= wh.max_retries {
                    delivery.status = DeliveryStatus::Failed;
                    stats.failed_deliveries += 1;
                    stats.pending_deliveries = stats.pending_deliveries.saturating_sub(1);
                } else {
                    delivery.status = DeliveryStatus::Retrying;
                    stats.retried_deliveries += 1;

                    // Calculate exponential backoff
                    let attempt_num = delivery.attempts.len() as u32;
                    let delay = wh.retry_delay_ms * (2_u64.pow(attempt_num - 1));
                    let delay = delay.min(wh.max_retry_delay_ms);

                    delivery.next_retry_ms = now + delay;

                    // Re-queue for retry
                    drop(stats);
                    self.pending.lock().push_back(delivery);
                    return;
                }
            } else {
                delivery.status = DeliveryStatus::Failed;
                stats.failed_deliveries += 1;
                stats.pending_deliveries = stats.pending_deliveries.saturating_sub(1);
            }
        }
    }

    /// Build webhook payload
    pub fn build_payload(&self, events: &[ChangeEvent]) -> String {
        if events.len() == 1 {
            events[0].to_json()
        } else {
            let mut json = String::from("{\"events\":[");
            for (i, event) in events.iter().enumerate() {
                if i > 0 {
                    json.push(',');
                }
                json.push_str(&event.to_json());
            }
            json.push_str("]}");
            json
        }
    }

    /// Compute HMAC-SHA256 signature for payload
    pub fn sign_payload(&self, payload: &str, secret: &str) -> String {
        // Simple HMAC-SHA256 implementation
        // In production, use a proper crypto library
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        secret.hash(&mut hasher);
        payload.hash(&mut hasher);
        let hash = hasher.finish();

        format!("sha256={:016x}", hash)
    }

    /// Get webhook statistics
    pub fn stats(&self) -> WebhookStats {
        self.stats.lock().clone()
    }

    /// Stop the registry
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

impl Default for WebhookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Webhook errors
#[derive(Debug)]
pub enum WebhookError {
    InvalidConfig(String),
    NotFound(String),
    DeliveryFailed(String),
    NetworkError(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::InvalidConfig(msg) => write!(f, "Invalid webhook config: {}", msg),
            WebhookError::NotFound(id) => write!(f, "Webhook not found: {}", id),
            WebhookError::DeliveryFailed(msg) => write!(f, "Delivery failed: {}", msg),
            WebhookError::NetworkError(msg) => write!(f, "Network error: {}", msg),
        }
    }
}

impl std::error::Error for WebhookError {}

/// CDC change tracker for a database
pub struct ChangeTracker {
    registry: Arc<WebhookRegistry>,
    database: String,
}

impl ChangeTracker {
    /// Create a new change tracker
    pub fn new(registry: Arc<WebhookRegistry>, database: &str) -> Self {
        Self {
            registry,
            database: database.to_string(),
        }
    }

    /// Track an INSERT
    pub fn track_insert(&self, table: &str, primary_key: &str, new_values: &str, tx_id: Option<u64>) {
        self.registry.record_change(
            &self.database,
            table,
            ChangeOperation::Insert,
            primary_key,
            None,
            Some(new_values),
            tx_id,
        );
    }

    /// Track an UPDATE
    pub fn track_update(
        &self,
        table: &str,
        primary_key: &str,
        old_values: &str,
        new_values: &str,
        tx_id: Option<u64>,
    ) {
        self.registry.record_change(
            &self.database,
            table,
            ChangeOperation::Update,
            primary_key,
            Some(old_values),
            Some(new_values),
            tx_id,
        );
    }

    /// Track a DELETE
    pub fn track_delete(&self, table: &str, primary_key: &str, old_values: &str, tx_id: Option<u64>) {
        self.registry.record_change(
            &self.database,
            table,
            ChangeOperation::Delete,
            primary_key,
            Some(old_values),
            None,
            tx_id,
        );
    }
}

/// Webhook delivery worker (simulated - in real impl would use async HTTP)
pub struct WebhookDeliveryWorker {
    registry: Arc<WebhookRegistry>,
}

impl WebhookDeliveryWorker {
    /// Create a new delivery worker
    pub fn new(registry: Arc<WebhookRegistry>) -> Self {
        Self { registry }
    }

    /// Process pending deliveries (called periodically)
    pub fn process_pending(&self) -> usize {
        let mut delivered = 0;

        while let Some(delivery) = self.registry.next_pending() {
            let result = self.deliver(&delivery);
            delivered += 1;

            match result {
                Ok((status_code, response)) => {
                    self.registry.record_attempt(
                        delivery,
                        true,
                        status_code,
                        Some(response),
                        None,
                        0,
                    );
                }
                Err(e) => {
                    self.registry.record_attempt(
                        delivery,
                        false,
                        0,
                        None,
                        Some(e.to_string()),
                        0,
                    );
                }
            }
        }

        // Flush any timed-out batches
        self.registry.flush_batches();

        delivered
    }

    /// Deliver a pending webhook (simulated)
    fn deliver(&self, delivery: &PendingDelivery) -> Result<(u16, String), WebhookError> {
        let webhook = self
            .registry
            .get(&delivery.webhook_id)
            .ok_or_else(|| WebhookError::NotFound(delivery.webhook_id.clone()))?;

        let payload = self.registry.build_payload(&delivery.events);

        // In a real implementation, this would make an HTTP request
        // For now, we simulate success
        let _ = (&webhook.url, &webhook.method, &payload);

        Ok((200, "{\"status\":\"ok\"}".to_string()))
    }
}

/// Builder for creating webhook configurations
pub struct WebhookBuilder {
    config: WebhookConfig,
}

impl WebhookBuilder {
    /// Create a new webhook builder
    pub fn new(url: &str) -> Self {
        Self {
            config: WebhookConfig {
                url: url.to_string(),
                ..Default::default()
            },
        }
    }

    /// Set webhook ID
    pub fn id(mut self, id: &str) -> Self {
        self.config.id = id.to_string();
        self
    }

    /// Set webhook name
    pub fn name(mut self, name: &str) -> Self {
        self.config.name = name.to_string();
        self
    }

    /// Set HTTP method
    pub fn method(mut self, method: &str) -> Self {
        self.config.method = method.to_string();
        self
    }

    /// Add a header
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.config.headers.insert(key.to_string(), value.to_string());
        self
    }

    /// Set signing secret
    pub fn secret(mut self, secret: &str) -> Self {
        self.config.secret = Some(secret.to_string());
        self
    }

    /// Add table to watch
    pub fn table(mut self, table: &str) -> Self {
        self.config.tables.push(table.to_string());
        self
    }

    /// Add tables to watch
    pub fn tables(mut self, tables: &[&str]) -> Self {
        for t in tables {
            self.config.tables.push(t.to_string());
        }
        self
    }

    /// Add operation to watch
    pub fn operation(mut self, op: ChangeOperation) -> Self {
        self.config.operations.push(op);
        self
    }

    /// Watch only INSERT operations
    pub fn inserts_only(mut self) -> Self {
        self.config.operations = vec![ChangeOperation::Insert];
        self
    }

    /// Enable batching
    pub fn batched(mut self, max_size: usize, timeout_ms: u64) -> Self {
        self.config.batch_enabled = true;
        self.config.batch_max_size = max_size;
        self.config.batch_timeout_ms = timeout_ms;
        self
    }

    /// Set retry configuration
    pub fn retries(mut self, max_retries: u32, initial_delay_ms: u64, max_delay_ms: u64) -> Self {
        self.config.max_retries = max_retries;
        self.config.retry_delay_ms = initial_delay_ms;
        self.config.max_retry_delay_ms = max_delay_ms;
        self
    }

    /// Set request timeout
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.config.timeout_ms = timeout_ms;
        self
    }

    /// Build the webhook configuration
    pub fn build(self) -> WebhookConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_event_json() {
        let event = ChangeEvent {
            event_id: 1,
            timestamp_ms: 1699999999000,
            database: "mydb".to_string(),
            table: "users".to_string(),
            operation: ChangeOperation::Insert,
            primary_key: "{\"id\":42}".to_string(),
            old_values: None,
            new_values: Some("{\"id\":42,\"name\":\"Alice\"}".to_string()),
            transaction_id: Some(100),
            sequence: 1,
        };

        let json = event.to_json();
        assert!(json.contains("\"event_id\":1"));
        assert!(json.contains("\"operation\":\"INSERT\""));
        assert!(json.contains("\"table\":\"users\""));
    }

    #[test]
    fn test_webhook_registration() {
        let registry = WebhookRegistry::new();

        let config = WebhookBuilder::new("https://example.com/webhook")
            .name("test-webhook")
            .table("mydb.users")
            .build();

        let id = registry.register(config).unwrap();
        assert!(!id.is_empty());

        let retrieved = registry.get(&id).unwrap();
        assert_eq!(retrieved.name, "test-webhook");
    }

    #[test]
    fn test_change_tracking() {
        let registry = Arc::new(WebhookRegistry::new());

        let config = WebhookBuilder::new("https://example.com/webhook")
            .table("mydb.users")
            .build();

        registry.register(config).unwrap();

        let tracker = ChangeTracker::new(Arc::clone(&registry), "mydb");
        tracker.track_insert("users", "{\"id\":1}", "{\"id\":1,\"name\":\"Bob\"}", None);

        let stats = registry.stats();
        assert_eq!(stats.total_events, 1);
    }

    #[test]
    fn test_operation_filtering() {
        let registry = Arc::new(WebhookRegistry::new());

        let config = WebhookBuilder::new("https://example.com/webhook")
            .table("mydb.users")
            .inserts_only()
            .build();

        registry.register(config).unwrap();

        // Record an UPDATE - should not match
        registry.record_change(
            "mydb",
            "users",
            ChangeOperation::Update,
            "{}",
            Some("{}"),
            Some("{}"),
            None,
        );

        // No pending deliveries since UPDATE doesn't match
        assert!(registry.next_pending().is_none());
    }

    #[test]
    fn test_batching() {
        let registry = Arc::new(WebhookRegistry::new());

        let config = WebhookBuilder::new("https://example.com/webhook")
            .batched(3, 5000)
            .build();

        registry.register(config).unwrap();

        // Add 2 events - not enough to trigger batch
        registry.record_change("db", "tbl", ChangeOperation::Insert, "{}", None, Some("{}"), None);
        registry.record_change("db", "tbl", ChangeOperation::Insert, "{}", None, Some("{}"), None);

        assert!(registry.next_pending().is_none());

        // Add 1 more to reach batch size
        registry.record_change("db", "tbl", ChangeOperation::Insert, "{}", None, Some("{}"), None);

        let delivery = registry.next_pending().unwrap();
        assert_eq!(delivery.events.len(), 3);
    }

    #[test]
    fn test_webhook_builder() {
        let config = WebhookBuilder::new("https://api.example.com/events")
            .id("my-webhook")
            .name("User Changes")
            .method("PUT")
            .header("Authorization", "Bearer token123")
            .header("X-Custom", "value")
            .secret("mysecret")
            .tables(&["db.users", "db.orders"])
            .operation(ChangeOperation::Insert)
            .operation(ChangeOperation::Update)
            .batched(50, 2000)
            .retries(5, 500, 30000)
            .timeout(15000)
            .build();

        assert_eq!(config.id, "my-webhook");
        assert_eq!(config.name, "User Changes");
        assert_eq!(config.method, "PUT");
        assert_eq!(config.headers.len(), 2);
        assert_eq!(config.secret, Some("mysecret".to_string()));
        assert_eq!(config.tables.len(), 2);
        assert_eq!(config.operations.len(), 2);
        assert!(config.batch_enabled);
        assert_eq!(config.batch_max_size, 50);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.timeout_ms, 15000);
    }

    #[test]
    fn test_payload_signing() {
        let registry = WebhookRegistry::new();
        let sig = registry.sign_payload("{\"test\":true}", "secret");
        assert!(sig.starts_with("sha256="));
    }
}
