//! Logical Replication Subscriber
//!
//! This module implements a subscriber for logical replication, allowing
//! BoyoDB to subscribe to changes from PostgreSQL or other compatible
//! publishers using the logical replication protocol.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum SubscriptionError {
    /// Connection failed
    ConnectionFailed(String),
    /// Subscription not found
    SubscriptionNotFound(String),
    /// Publication not found
    PublicationNotFound(String),
    /// Slot not found
    SlotNotFound(String),
    /// Sync error
    SyncError(String),
    /// Apply error
    ApplyError(String),
    /// Already exists
    AlreadyExists(String),
    /// Invalid state
    InvalidState(String),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscriptionError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            SubscriptionError::SubscriptionNotFound(name) => {
                write!(f, "Subscription not found: {}", name)
            }
            SubscriptionError::PublicationNotFound(name) => {
                write!(f, "Publication not found: {}", name)
            }
            SubscriptionError::SlotNotFound(name) => write!(f, "Slot not found: {}", name),
            SubscriptionError::SyncError(msg) => write!(f, "Sync error: {}", msg),
            SubscriptionError::ApplyError(msg) => write!(f, "Apply error: {}", msg),
            SubscriptionError::AlreadyExists(name) => write!(f, "Already exists: {}", name),
            SubscriptionError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
        }
    }
}

impl std::error::Error for SubscriptionError {}

// ============================================================================
// Subscription State
// ============================================================================

/// State of a subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Subscription created but not started
    Created,
    /// Initial data copy in progress
    CopyingData,
    /// Catching up with publisher
    CatchingUp,
    /// Fully synchronized, streaming changes
    Streaming,
    /// Subscription paused
    Paused,
    /// Subscription disabled
    Disabled,
    /// Error state
    Error,
}

impl SubscriptionState {
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            SubscriptionState::CopyingData
                | SubscriptionState::CatchingUp
                | SubscriptionState::Streaming
        )
    }

    pub fn can_start(&self) -> bool {
        matches!(
            self,
            SubscriptionState::Created
                | SubscriptionState::Paused
                | SubscriptionState::Disabled
                | SubscriptionState::Error
        )
    }
}

// ============================================================================
// Table Sync State
// ============================================================================

/// Sync state for a table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSyncState {
    /// Not synced
    NotSynced,
    /// Initial copy in progress
    Copying,
    /// Copy done, catching up
    CatchingUp,
    /// Fully synced
    Synced,
    /// Error during sync
    Error,
}

/// Information about a synced table
#[derive(Debug, Clone)]
pub struct SubscribedTable {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Sync state
    pub state: TableSyncState,
    /// Rows copied during initial sync
    pub rows_copied: u64,
    /// Changes applied
    pub changes_applied: u64,
    /// Last sync time
    pub last_sync: Option<Instant>,
    /// Columns to replicate (None = all)
    pub columns: Option<Vec<String>>,
}

impl SubscribedTable {
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: schema.to_string(),
            table: table.to_string(),
            state: TableSyncState::NotSynced,
            rows_copied: 0,
            changes_applied: 0,
            last_sync: None,
            columns: None,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

// ============================================================================
// Replication Message
// ============================================================================

/// Type of replication operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationOp {
    Insert,
    Update,
    Delete,
    Truncate,
    Begin,
    Commit,
    Rollback,
}

/// A replication message from the publisher
#[derive(Debug, Clone)]
pub struct ReplicationMessage {
    /// Log sequence number
    pub lsn: u64,
    /// Transaction ID
    pub xid: u64,
    /// Operation type
    pub op: ReplicationOp,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Old row values (for update/delete)
    pub old_values: Option<HashMap<String, ReplicationValue>>,
    /// New row values (for insert/update)
    pub new_values: Option<HashMap<String, ReplicationValue>>,
    /// Timestamp
    pub timestamp: u64,
}

/// A value in a replication message
#[derive(Debug, Clone)]
pub enum ReplicationValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    Date(i32),
    Json(String),
}

// ============================================================================
// Subscription
// ============================================================================

/// Statistics for a subscription
#[derive(Debug, Default)]
pub struct SubscriptionStats {
    /// Messages received
    pub messages_received: AtomicU64,
    /// Transactions applied
    pub transactions_applied: AtomicU64,
    /// Inserts applied
    pub inserts_applied: AtomicU64,
    /// Updates applied
    pub updates_applied: AtomicU64,
    /// Deletes applied
    pub deletes_applied: AtomicU64,
    /// Errors encountered
    pub errors: AtomicU64,
    /// Bytes received
    pub bytes_received: AtomicU64,
    /// Lag in milliseconds
    pub lag_ms: AtomicU64,
}

impl SubscriptionStats {
    pub fn snapshot(&self) -> SubscriptionStatsSnapshot {
        SubscriptionStatsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            transactions_applied: self.transactions_applied.load(Ordering::Relaxed),
            inserts_applied: self.inserts_applied.load(Ordering::Relaxed),
            updates_applied: self.updates_applied.load(Ordering::Relaxed),
            deletes_applied: self.deletes_applied.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            lag_ms: self.lag_ms.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of subscription statistics
#[derive(Debug, Clone)]
pub struct SubscriptionStatsSnapshot {
    pub messages_received: u64,
    pub transactions_applied: u64,
    pub inserts_applied: u64,
    pub updates_applied: u64,
    pub deletes_applied: u64,
    pub errors: u64,
    pub bytes_received: u64,
    pub lag_ms: u64,
}

/// Configuration for a subscription
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Connection string to publisher
    pub connection: String,
    /// Publication name to subscribe to
    pub publication: String,
    /// Replication slot name
    pub slot_name: Option<String>,
    /// Whether to copy initial data
    pub copy_data: bool,
    /// Whether to create slot
    pub create_slot: bool,
    /// Synchronous commit level
    pub synchronous_commit: bool,
    /// Origin filtering
    pub origin: OriginFilter,
    /// Disable on error
    pub disable_on_error: bool,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            connection: String::new(),
            publication: String::new(),
            slot_name: None,
            copy_data: true,
            create_slot: true,
            synchronous_commit: false,
            origin: OriginFilter::Any,
            disable_on_error: false,
        }
    }
}

/// Filter for replication origin
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginFilter {
    /// Accept any origin
    Any,
    /// Accept only local changes
    Local,
    /// Accept no changes
    None,
}

/// A subscription to a publication
pub struct Subscription {
    /// Subscription name
    name: String,
    /// Configuration
    config: SubscriptionConfig,
    /// Current state
    state: RwLock<SubscriptionState>,
    /// Subscribed tables
    tables: RwLock<HashMap<String, SubscribedTable>>,
    /// Last received LSN
    received_lsn: AtomicU64,
    /// Last flushed LSN
    flushed_lsn: AtomicU64,
    /// Statistics
    stats: SubscriptionStats,
    /// Whether subscription is enabled
    enabled: AtomicBool,
    /// Last error
    last_error: RwLock<Option<String>>,
    /// Start time
    started_at: RwLock<Option<Instant>>,
}

impl Subscription {
    pub fn new(name: &str, config: SubscriptionConfig) -> Self {
        Self {
            name: name.to_string(),
            config,
            state: RwLock::new(SubscriptionState::Created),
            tables: RwLock::new(HashMap::new()),
            received_lsn: AtomicU64::new(0),
            flushed_lsn: AtomicU64::new(0),
            stats: SubscriptionStats::default(),
            enabled: AtomicBool::new(true),
            last_error: RwLock::new(None),
            started_at: RwLock::new(None),
        }
    }

    /// Get subscription name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get current state
    pub fn state(&self) -> SubscriptionState {
        *self.state.read()
    }

    /// Set state
    pub fn set_state(&self, state: SubscriptionState) {
        *self.state.write() = state;
    }

    /// Add a table to subscribe
    pub fn add_table(&self, schema: &str, table: &str) {
        let full_name = format!("{}.{}", schema, table);
        let mut tables = self.tables.write();
        tables.insert(full_name, SubscribedTable::new(schema, table));
    }

    /// Remove a table
    pub fn remove_table(&self, schema: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", schema, table);
        let mut tables = self.tables.write();
        tables.remove(&full_name).is_some()
    }

    /// Get table count
    pub fn table_count(&self) -> usize {
        self.tables.read().len()
    }

    /// Get table sync state
    pub fn get_table_state(&self, schema: &str, table: &str) -> Option<TableSyncState> {
        let full_name = format!("{}.{}", schema, table);
        let tables = self.tables.read();
        tables.get(&full_name).map(|t| t.state)
    }

    /// Apply a replication message
    pub fn apply_message(&self, msg: &ReplicationMessage) -> Result<(), SubscriptionError> {
        self.stats.messages_received.fetch_add(1, Ordering::Relaxed);

        match msg.op {
            ReplicationOp::Insert => {
                self.stats.inserts_applied.fetch_add(1, Ordering::Relaxed);
            }
            ReplicationOp::Update => {
                self.stats.updates_applied.fetch_add(1, Ordering::Relaxed);
            }
            ReplicationOp::Delete => {
                self.stats.deletes_applied.fetch_add(1, Ordering::Relaxed);
            }
            ReplicationOp::Commit => {
                self.stats.transactions_applied.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        // Update received LSN
        self.received_lsn.store(msg.lsn, Ordering::Relaxed);

        // Update table stats if applicable
        if let (Some(schema), Some(table)) = (&msg.schema, &msg.table) {
            let full_name = format!("{}.{}", schema, table);
            let mut tables = self.tables.write();
            if let Some(t) = tables.get_mut(&full_name) {
                t.changes_applied += 1;
                t.last_sync = Some(Instant::now());
            }
        }

        Ok(())
    }

    /// Mark LSN as flushed
    pub fn flush_lsn(&self, lsn: u64) {
        self.flushed_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Get received LSN
    pub fn received_lsn(&self) -> u64 {
        self.received_lsn.load(Ordering::Relaxed)
    }

    /// Get flushed LSN
    pub fn flushed_lsn(&self) -> u64 {
        self.flushed_lsn.load(Ordering::Relaxed)
    }

    /// Enable subscription
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Disable subscription
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        self.set_state(SubscriptionState::Disabled);
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Set error
    pub fn set_error(&self, error: &str) {
        *self.last_error.write() = Some(error.to_string());
        self.stats.errors.fetch_add(1, Ordering::Relaxed);
        if self.config.disable_on_error {
            self.disable();
        }
        self.set_state(SubscriptionState::Error);
    }

    /// Get last error
    pub fn last_error(&self) -> Option<String> {
        self.last_error.read().clone()
    }

    /// Clear error
    pub fn clear_error(&self) {
        *self.last_error.write() = None;
    }

    /// Get statistics
    pub fn stats(&self) -> SubscriptionStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get config
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    /// Start the subscription
    pub fn start(&self) -> Result<(), SubscriptionError> {
        if !self.state().can_start() {
            return Err(SubscriptionError::InvalidState(
                "Subscription cannot be started in current state".to_string(),
            ));
        }

        *self.started_at.write() = Some(Instant::now());

        if self.config.copy_data {
            self.set_state(SubscriptionState::CopyingData);
        } else {
            self.set_state(SubscriptionState::CatchingUp);
        }

        self.enable();
        Ok(())
    }

    /// Stop the subscription
    pub fn stop(&self) {
        self.set_state(SubscriptionState::Paused);
    }

    /// Get all tables
    pub fn tables(&self) -> Vec<SubscribedTable> {
        self.tables.read().values().cloned().collect()
    }

    /// Update table sync state
    pub fn set_table_state(&self, schema: &str, table: &str, state: TableSyncState) {
        let full_name = format!("{}.{}", schema, table);
        let mut tables = self.tables.write();
        if let Some(t) = tables.get_mut(&full_name) {
            t.state = state;
        }
    }
}

// ============================================================================
// Subscription Manager
// ============================================================================

/// Manager for all subscriptions
pub struct SubscriptionManager {
    /// Subscriptions by name
    subscriptions: Arc<RwLock<HashMap<String, Arc<Subscription>>>>,
    /// Global statistics
    stats: SubscriptionManagerStats,
}

/// Global statistics
#[derive(Debug, Default)]
pub struct SubscriptionManagerStats {
    /// Total subscriptions created
    pub subscriptions_created: AtomicU64,
    /// Active subscriptions
    pub active_subscriptions: AtomicU64,
    /// Total messages applied
    pub total_messages_applied: AtomicU64,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            stats: SubscriptionManagerStats::default(),
        }
    }

    /// Create a new subscription
    pub fn create_subscription(
        &self,
        name: &str,
        config: SubscriptionConfig,
    ) -> Result<Arc<Subscription>, SubscriptionError> {
        let mut subs = self.subscriptions.write();

        if subs.contains_key(name) {
            return Err(SubscriptionError::AlreadyExists(name.to_string()));
        }

        let sub = Arc::new(Subscription::new(name, config));
        subs.insert(name.to_string(), sub.clone());

        self.stats
            .subscriptions_created
            .fetch_add(1, Ordering::Relaxed);

        Ok(sub)
    }

    /// Drop a subscription
    pub fn drop_subscription(&self, name: &str) -> Result<(), SubscriptionError> {
        let mut subs = self.subscriptions.write();

        if let Some(sub) = subs.remove(name) {
            sub.disable();
            Ok(())
        } else {
            Err(SubscriptionError::SubscriptionNotFound(name.to_string()))
        }
    }

    /// Get a subscription
    pub fn get_subscription(&self, name: &str) -> Option<Arc<Subscription>> {
        let subs = self.subscriptions.read();
        subs.get(name).cloned()
    }

    /// List all subscriptions
    pub fn list_subscriptions(&self) -> Vec<(String, SubscriptionState)> {
        let subs = self.subscriptions.read();
        subs.iter()
            .map(|(name, sub)| (name.clone(), sub.state()))
            .collect()
    }

    /// Enable a subscription
    pub fn enable_subscription(&self, name: &str) -> Result<(), SubscriptionError> {
        let subs = self.subscriptions.read();
        let sub = subs
            .get(name)
            .ok_or_else(|| SubscriptionError::SubscriptionNotFound(name.to_string()))?;
        sub.start()
    }

    /// Disable a subscription
    pub fn disable_subscription(&self, name: &str) -> Result<(), SubscriptionError> {
        let subs = self.subscriptions.read();
        let sub = subs
            .get(name)
            .ok_or_else(|| SubscriptionError::SubscriptionNotFound(name.to_string()))?;
        sub.disable();
        Ok(())
    }

    /// Get active subscription count
    pub fn active_count(&self) -> usize {
        let subs = self.subscriptions.read();
        subs.values().filter(|s| s.state().is_active()).count()
    }

    /// Get total subscription count
    pub fn total_count(&self) -> usize {
        self.subscriptions.read().len()
    }

    /// Refresh tables from publication
    pub fn refresh_publication(
        &self,
        name: &str,
        tables: Vec<(String, String)>,
    ) -> Result<(), SubscriptionError> {
        let subs = self.subscriptions.read();
        let sub = subs
            .get(name)
            .ok_or_else(|| SubscriptionError::SubscriptionNotFound(name.to_string()))?;

        for (schema, table) in tables {
            sub.add_table(&schema, &table);
        }

        Ok(())
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Apply Worker
// ============================================================================

/// Worker that applies replication messages
pub struct ApplyWorker {
    /// Subscription
    subscription: Arc<Subscription>,
    /// Message queue
    message_queue: Arc<RwLock<Vec<ReplicationMessage>>>,
    /// Running flag
    running: AtomicBool,
    /// Current transaction messages
    current_tx: RwLock<Vec<ReplicationMessage>>,
}

impl ApplyWorker {
    pub fn new(subscription: Arc<Subscription>) -> Self {
        Self {
            subscription,
            message_queue: Arc::new(RwLock::new(Vec::new())),
            running: AtomicBool::new(false),
            current_tx: RwLock::new(Vec::new()),
        }
    }

    /// Start the worker
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the worker
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Queue a message for processing
    pub fn queue_message(&self, msg: ReplicationMessage) {
        let mut queue = self.message_queue.write();
        queue.push(msg);
    }

    /// Process queued messages
    pub fn process_queue(&self) -> Result<usize, SubscriptionError> {
        let mut queue = self.message_queue.write();
        let messages: Vec<_> = queue.drain(..).collect();
        drop(queue);

        let mut processed = 0;

        for msg in messages {
            self.process_message(msg)?;
            processed += 1;
        }

        Ok(processed)
    }

    fn process_message(&self, msg: ReplicationMessage) -> Result<(), SubscriptionError> {
        match msg.op {
            ReplicationOp::Begin => {
                // Start collecting transaction messages
                let mut current = self.current_tx.write();
                current.clear();
                current.push(msg);
            }
            ReplicationOp::Commit => {
                // Apply all messages in transaction
                let mut current = self.current_tx.write();
                current.push(msg.clone());

                for m in current.iter() {
                    self.subscription.apply_message(m)?;
                }

                current.clear();
                self.subscription.flush_lsn(msg.lsn);
            }
            ReplicationOp::Rollback => {
                // Discard transaction messages
                let mut current = self.current_tx.write();
                current.clear();
            }
            _ => {
                // Add to current transaction or apply immediately
                let mut current = self.current_tx.write();
                if !current.is_empty() {
                    current.push(msg);
                } else {
                    // No active transaction, apply immediately
                    drop(current);
                    self.subscription.apply_message(&msg)?;
                }
            }
        }

        Ok(())
    }

    /// Get queue size
    pub fn queue_size(&self) -> usize {
        self.message_queue.read().len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_state() {
        assert!(SubscriptionState::Streaming.is_active());
        assert!(SubscriptionState::CatchingUp.is_active());
        assert!(!SubscriptionState::Paused.is_active());

        assert!(SubscriptionState::Created.can_start());
        assert!(SubscriptionState::Paused.can_start());
        assert!(!SubscriptionState::Streaming.can_start());
    }

    #[test]
    fn test_subscription_creation() {
        let config = SubscriptionConfig {
            connection: "host=localhost".to_string(),
            publication: "pub1".to_string(),
            ..Default::default()
        };

        let sub = Subscription::new("sub1", config);

        assert_eq!(sub.name(), "sub1");
        assert_eq!(sub.state(), SubscriptionState::Created);
        assert!(sub.is_enabled());
    }

    #[test]
    fn test_add_remove_table() {
        let sub = Subscription::new("sub1", SubscriptionConfig::default());

        sub.add_table("public", "users");
        sub.add_table("public", "orders");

        assert_eq!(sub.table_count(), 2);

        assert!(sub.remove_table("public", "users"));
        assert_eq!(sub.table_count(), 1);

        assert!(!sub.remove_table("public", "nonexistent"));
    }

    #[test]
    fn test_apply_message() {
        let sub = Subscription::new("sub1", SubscriptionConfig::default());
        sub.add_table("public", "users");

        let msg = ReplicationMessage {
            lsn: 100,
            xid: 1,
            op: ReplicationOp::Insert,
            schema: Some("public".to_string()),
            table: Some("users".to_string()),
            old_values: None,
            new_values: Some(HashMap::new()),
            timestamp: 0,
        };

        sub.apply_message(&msg).unwrap();

        let stats = sub.stats();
        assert_eq!(stats.messages_received, 1);
        assert_eq!(stats.inserts_applied, 1);
        assert_eq!(sub.received_lsn(), 100);
    }

    #[test]
    fn test_subscription_start_stop() {
        let sub = Subscription::new("sub1", SubscriptionConfig::default());

        assert!(sub.start().is_ok());
        assert!(sub.state().is_active());

        sub.stop();
        assert_eq!(sub.state(), SubscriptionState::Paused);

        // Can restart after pause
        assert!(sub.start().is_ok());
    }

    #[test]
    fn test_subscription_disable() {
        let sub = Subscription::new("sub1", SubscriptionConfig::default());
        sub.start().unwrap();

        sub.disable();
        assert_eq!(sub.state(), SubscriptionState::Disabled);
        assert!(!sub.is_enabled());
    }

    #[test]
    fn test_subscription_error() {
        let config = SubscriptionConfig {
            disable_on_error: true,
            ..Default::default()
        };
        let sub = Subscription::new("sub1", config);

        sub.set_error("test error");

        assert_eq!(sub.state(), SubscriptionState::Error);
        assert!(!sub.is_enabled());
        assert_eq!(sub.last_error(), Some("test error".to_string()));

        let stats = sub.stats();
        assert_eq!(stats.errors, 1);
    }

    #[test]
    fn test_subscription_manager() {
        let manager = SubscriptionManager::new();

        let config = SubscriptionConfig {
            publication: "pub1".to_string(),
            ..Default::default()
        };

        let sub = manager.create_subscription("sub1", config.clone()).unwrap();
        assert_eq!(manager.total_count(), 1);

        // Cannot create duplicate
        let result = manager.create_subscription("sub1", config);
        assert!(matches!(result, Err(SubscriptionError::AlreadyExists(_))));

        // Can get subscription
        let retrieved = manager.get_subscription("sub1").unwrap();
        assert_eq!(retrieved.name(), "sub1");

        // List subscriptions
        let list = manager.list_subscriptions();
        assert_eq!(list.len(), 1);

        // Drop subscription
        manager.drop_subscription("sub1").unwrap();
        assert_eq!(manager.total_count(), 0);
    }

    #[test]
    fn test_apply_worker() {
        let sub = Arc::new(Subscription::new("sub1", SubscriptionConfig::default()));
        sub.add_table("public", "users");

        let worker = ApplyWorker::new(sub.clone());

        // Queue some messages
        worker.queue_message(ReplicationMessage {
            lsn: 100,
            xid: 1,
            op: ReplicationOp::Begin,
            schema: None,
            table: None,
            old_values: None,
            new_values: None,
            timestamp: 0,
        });

        worker.queue_message(ReplicationMessage {
            lsn: 101,
            xid: 1,
            op: ReplicationOp::Insert,
            schema: Some("public".to_string()),
            table: Some("users".to_string()),
            old_values: None,
            new_values: Some(HashMap::new()),
            timestamp: 0,
        });

        worker.queue_message(ReplicationMessage {
            lsn: 102,
            xid: 1,
            op: ReplicationOp::Commit,
            schema: None,
            table: None,
            old_values: None,
            new_values: None,
            timestamp: 0,
        });

        assert_eq!(worker.queue_size(), 3);

        let processed = worker.process_queue().unwrap();
        assert_eq!(processed, 3);
        assert_eq!(worker.queue_size(), 0);

        let stats = sub.stats();
        assert_eq!(stats.transactions_applied, 1);
    }

    #[test]
    fn test_table_sync_state() {
        let sub = Subscription::new("sub1", SubscriptionConfig::default());
        sub.add_table("public", "users");

        assert_eq!(
            sub.get_table_state("public", "users"),
            Some(TableSyncState::NotSynced)
        );

        sub.set_table_state("public", "users", TableSyncState::Synced);
        assert_eq!(
            sub.get_table_state("public", "users"),
            Some(TableSyncState::Synced)
        );
    }

    #[test]
    fn test_error_display() {
        let err = SubscriptionError::ConnectionFailed("timeout".to_string());
        assert!(format!("{}", err).contains("timeout"));

        let err = SubscriptionError::SubscriptionNotFound("sub1".to_string());
        assert!(format!("{}", err).contains("sub1"));
    }

    #[test]
    fn test_refresh_publication() {
        let manager = SubscriptionManager::new();
        manager
            .create_subscription("sub1", SubscriptionConfig::default())
            .unwrap();

        manager
            .refresh_publication(
                "sub1",
                vec![
                    ("public".to_string(), "users".to_string()),
                    ("public".to_string(), "orders".to_string()),
                ],
            )
            .unwrap();

        let sub = manager.get_subscription("sub1").unwrap();
        assert_eq!(sub.table_count(), 2);
    }

    #[test]
    fn test_subscribed_table() {
        let table = SubscribedTable::new("public", "users");

        assert_eq!(table.full_name(), "public.users");
        assert_eq!(table.state, TableSyncState::NotSynced);
        assert_eq!(table.rows_copied, 0);
    }

    #[test]
    fn test_enable_disable_subscription() {
        let manager = SubscriptionManager::new();
        manager
            .create_subscription("sub1", SubscriptionConfig::default())
            .unwrap();

        manager.disable_subscription("sub1").unwrap();
        let sub = manager.get_subscription("sub1").unwrap();
        assert!(!sub.is_enabled());

        manager.enable_subscription("sub1").unwrap();
        assert!(sub.state().is_active());
    }

    #[test]
    fn test_active_count() {
        let manager = SubscriptionManager::new();

        manager
            .create_subscription("sub1", SubscriptionConfig::default())
            .unwrap();
        manager
            .create_subscription("sub2", SubscriptionConfig::default())
            .unwrap();

        manager.enable_subscription("sub1").unwrap();

        assert_eq!(manager.active_count(), 1);
        assert_eq!(manager.total_count(), 2);
    }
}
