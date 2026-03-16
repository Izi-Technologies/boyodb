//! Blue-Green Deployment and Connection Draining
//!
//! Provides zero-downtime deployment support with blue-green switching,
//! graceful connection draining, and health check coordination.
//!
//! ## Features
//!
//! - Blue-green deployment switching
//! - Graceful connection draining
//! - Health check endpoints
//! - Traffic shifting (canary deployments)
//! - Rollback support
//! - Deployment history tracking

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;

// ============================================================================
// Deployment Slot
// ============================================================================

/// Deployment slot identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentSlot {
    Blue,
    Green,
}

impl DeploymentSlot {
    pub fn other(&self) -> Self {
        match self {
            DeploymentSlot::Blue => DeploymentSlot::Green,
            DeploymentSlot::Green => DeploymentSlot::Blue,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DeploymentSlot::Blue => "blue",
            DeploymentSlot::Green => "green",
        }
    }
}

/// Deployment slot status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    /// Slot is active and serving traffic
    Active,
    /// Slot is on standby (ready but not serving)
    Standby,
    /// Slot is draining connections
    Draining,
    /// Slot is being deployed to
    Deploying,
    /// Slot is unhealthy
    Unhealthy,
    /// Slot is offline
    Offline,
}

/// Information about a deployment slot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    pub slot: DeploymentSlot,
    pub status: SlotStatus,
    pub version: String,
    pub deployed_at: u64,
    pub active_connections: u64,
    pub total_requests: u64,
    pub health_check_passed: bool,
    pub last_health_check: u64,
    pub traffic_weight: u8, // 0-100%
    pub metadata: HashMap<String, String>,
}

// ============================================================================
// Connection Draining
// ============================================================================

/// Connection draining configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainConfig {
    /// Maximum time to wait for connections to drain (seconds)
    pub timeout_secs: u64,
    /// Interval between drain checks (milliseconds)
    pub check_interval_ms: u64,
    /// Force close connections after timeout
    pub force_close: bool,
    /// Stop accepting new connections immediately
    pub stop_accepting: bool,
    /// Send connection close signal to clients
    pub notify_clients: bool,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            timeout_secs: 30,
            check_interval_ms: 500,
            force_close: true,
            stop_accepting: true,
            notify_clients: true,
        }
    }
}

/// Connection tracker for draining
pub struct ConnectionTracker {
    /// Active connection count
    active_connections: AtomicU64,
    /// Total connections served
    total_connections: AtomicU64,
    /// Is accepting new connections
    accepting: AtomicBool,
    /// Is currently draining
    draining: AtomicBool,
    /// Drain started timestamp
    drain_started: AtomicU64,
    /// Per-connection metadata
    connections: RwLock<HashMap<u64, ConnectionInfo>>,
    /// Next connection ID
    next_conn_id: AtomicU64,
}

/// Information about a single connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: u64,
    pub created_at: u64,
    pub last_activity: u64,
    pub requests_served: u64,
    pub client_addr: Option<String>,
    pub user_id: Option<String>,
    pub in_transaction: bool,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            accepting: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            drain_started: AtomicU64::new(0),
            connections: RwLock::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
        }
    }

    /// Register a new connection
    pub fn register(&self, client_addr: Option<String>) -> Option<u64> {
        if !self.accepting.load(Ordering::SeqCst) {
            return None;
        }

        let id = self.next_conn_id.fetch_add(1, Ordering::SeqCst);
        let now = current_timestamp();

        let info = ConnectionInfo {
            id,
            created_at: now,
            last_activity: now,
            requests_served: 0,
            client_addr,
            user_id: None,
            in_transaction: false,
        };

        self.connections.write().insert(id, info);
        self.active_connections.fetch_add(1, Ordering::SeqCst);
        self.total_connections.fetch_add(1, Ordering::SeqCst);

        Some(id)
    }

    /// Unregister a connection
    pub fn unregister(&self, conn_id: u64) {
        if self.connections.write().remove(&conn_id).is_some() {
            self.active_connections.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Update connection activity
    pub fn update_activity(&self, conn_id: u64) {
        if let Some(info) = self.connections.write().get_mut(&conn_id) {
            info.last_activity = current_timestamp();
            info.requests_served += 1;
        }
    }

    /// Set connection transaction state
    pub fn set_in_transaction(&self, conn_id: u64, in_txn: bool) {
        if let Some(info) = self.connections.write().get_mut(&conn_id) {
            info.in_transaction = in_txn;
        }
    }

    /// Get active connection count
    pub fn active_count(&self) -> u64 {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Get total connections
    pub fn total_count(&self) -> u64 {
        self.total_connections.load(Ordering::SeqCst)
    }

    /// Is accepting connections
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    /// Is draining
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Start draining connections
    pub fn start_drain(&self) -> DrainHandle<'_> {
        self.draining.store(true, Ordering::SeqCst);
        self.accepting.store(false, Ordering::SeqCst);
        self.drain_started.store(current_timestamp(), Ordering::SeqCst);

        DrainHandle {
            tracker: self,
            start_time: std::time::Instant::now(),
        }
    }

    /// Stop draining (resume normal operation)
    pub fn stop_drain(&self) {
        self.draining.store(false, Ordering::SeqCst);
        self.accepting.store(true, Ordering::SeqCst);
    }

    /// Get connections in transaction
    pub fn connections_in_transaction(&self) -> Vec<u64> {
        self.connections.read()
            .iter()
            .filter(|(_, info)| info.in_transaction)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all connection info
    pub fn get_connections(&self) -> Vec<ConnectionInfo> {
        self.connections.read().values().cloned().collect()
    }
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for drain operation
pub struct DrainHandle<'a> {
    tracker: &'a ConnectionTracker,
    start_time: std::time::Instant,
}

impl<'a> DrainHandle<'a> {
    /// Wait for all connections to drain
    pub async fn wait(&self, config: &DrainConfig) -> DrainResult {
        let timeout = Duration::from_secs(config.timeout_secs);
        let check_interval = Duration::from_millis(config.check_interval_ms);

        loop {
            let active = self.tracker.active_count();

            if active == 0 {
                return DrainResult {
                    success: true,
                    connections_drained: self.tracker.total_count(),
                    connections_forced: 0,
                    duration_ms: self.start_time.elapsed().as_millis() as u64,
                };
            }

            if self.start_time.elapsed() >= timeout {
                if config.force_close {
                    let forced = active;
                    // Force close all connections
                    self.tracker.connections.write().clear();
                    self.tracker.active_connections.store(0, Ordering::SeqCst);

                    return DrainResult {
                        success: true,
                        connections_drained: self.tracker.total_count() - forced,
                        connections_forced: forced,
                        duration_ms: self.start_time.elapsed().as_millis() as u64,
                    };
                } else {
                    return DrainResult {
                        success: false,
                        connections_drained: self.tracker.total_count() - active,
                        connections_forced: 0,
                        duration_ms: self.start_time.elapsed().as_millis() as u64,
                    };
                }
            }

            tokio::time::sleep(check_interval).await;
        }
    }

    /// Wait synchronously
    pub fn wait_blocking(&self, config: &DrainConfig) -> DrainResult {
        let timeout = Duration::from_secs(config.timeout_secs);
        let check_interval = Duration::from_millis(config.check_interval_ms);

        loop {
            let active = self.tracker.active_count();

            if active == 0 {
                return DrainResult {
                    success: true,
                    connections_drained: self.tracker.total_count(),
                    connections_forced: 0,
                    duration_ms: self.start_time.elapsed().as_millis() as u64,
                };
            }

            if self.start_time.elapsed() >= timeout {
                if config.force_close {
                    let forced = active;
                    self.tracker.connections.write().clear();
                    self.tracker.active_connections.store(0, Ordering::SeqCst);

                    return DrainResult {
                        success: true,
                        connections_drained: self.tracker.total_count() - forced,
                        connections_forced: forced,
                        duration_ms: self.start_time.elapsed().as_millis() as u64,
                    };
                } else {
                    return DrainResult {
                        success: false,
                        connections_drained: self.tracker.total_count() - active,
                        connections_forced: 0,
                        duration_ms: self.start_time.elapsed().as_millis() as u64,
                    };
                }
            }

            std::thread::sleep(check_interval);
        }
    }
}

/// Result of drain operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainResult {
    pub success: bool,
    pub connections_drained: u64,
    pub connections_forced: u64,
    pub duration_ms: u64,
}

// ============================================================================
// Blue-Green Deployment Manager
// ============================================================================

/// Blue-green deployment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlueGreenConfig {
    /// Health check endpoint path
    pub health_check_path: String,
    /// Health check interval in milliseconds
    pub health_check_interval_ms: u64,
    /// Health check timeout in milliseconds
    pub health_check_timeout_ms: u64,
    /// Number of consecutive health checks required
    pub health_check_threshold: u32,
    /// Drain configuration
    pub drain_config: DrainConfig,
    /// Enable canary deployments
    pub enable_canary: bool,
    /// Default canary weight (percentage)
    pub default_canary_weight: u8,
}

impl Default for BlueGreenConfig {
    fn default() -> Self {
        Self {
            health_check_path: "/health".to_string(),
            health_check_interval_ms: 5000,
            health_check_timeout_ms: 3000,
            health_check_threshold: 3,
            drain_config: DrainConfig::default(),
            enable_canary: true,
            default_canary_weight: 10,
        }
    }
}

/// Blue-green deployment manager
pub struct DeploymentManager {
    config: BlueGreenConfig,
    /// Current active slot
    active_slot: RwLock<DeploymentSlot>,
    /// Slot information
    slots: RwLock<HashMap<DeploymentSlot, SlotInfo>>,
    /// Connection tracker for blue slot
    blue_tracker: Arc<ConnectionTracker>,
    /// Connection tracker for green slot
    green_tracker: Arc<ConnectionTracker>,
    /// Deployment history
    history: RwLock<Vec<DeploymentRecord>>,
    /// Is switch in progress
    switch_in_progress: AtomicBool,
}

/// Record of a deployment switch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRecord {
    pub id: u64,
    pub from_slot: DeploymentSlot,
    pub to_slot: DeploymentSlot,
    pub from_version: String,
    pub to_version: String,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub success: bool,
    pub drain_result: Option<DrainResult>,
    pub initiated_by: Option<String>,
    pub rollback_of: Option<u64>,
}

impl DeploymentManager {
    /// Create a new deployment manager
    pub fn new(config: BlueGreenConfig) -> Self {
        let now = current_timestamp();

        let mut slots = HashMap::new();
        slots.insert(DeploymentSlot::Blue, SlotInfo {
            slot: DeploymentSlot::Blue,
            status: SlotStatus::Active,
            version: "initial".to_string(),
            deployed_at: now,
            active_connections: 0,
            total_requests: 0,
            health_check_passed: true,
            last_health_check: now,
            traffic_weight: 100,
            metadata: HashMap::new(),
        });
        slots.insert(DeploymentSlot::Green, SlotInfo {
            slot: DeploymentSlot::Green,
            status: SlotStatus::Standby,
            version: "initial".to_string(),
            deployed_at: now,
            active_connections: 0,
            total_requests: 0,
            health_check_passed: true,
            last_health_check: now,
            traffic_weight: 0,
            metadata: HashMap::new(),
        });

        Self {
            config,
            active_slot: RwLock::new(DeploymentSlot::Blue),
            slots: RwLock::new(slots),
            blue_tracker: Arc::new(ConnectionTracker::new()),
            green_tracker: Arc::new(ConnectionTracker::new()),
            history: RwLock::new(Vec::new()),
            switch_in_progress: AtomicBool::new(false),
        }
    }

    /// Get current active slot
    pub fn active_slot(&self) -> DeploymentSlot {
        *self.active_slot.read()
    }

    /// Get slot info
    pub fn get_slot_info(&self, slot: DeploymentSlot) -> Option<SlotInfo> {
        self.slots.read().get(&slot).cloned()
    }

    /// Get connection tracker for slot
    pub fn get_tracker(&self, slot: DeploymentSlot) -> Arc<ConnectionTracker> {
        match slot {
            DeploymentSlot::Blue => self.blue_tracker.clone(),
            DeploymentSlot::Green => self.green_tracker.clone(),
        }
    }

    /// Get active tracker
    pub fn active_tracker(&self) -> Arc<ConnectionTracker> {
        self.get_tracker(self.active_slot())
    }

    /// Prepare a slot for deployment
    pub fn prepare_slot(&self, slot: DeploymentSlot, version: &str) -> Result<(), DeploymentError> {
        let active = self.active_slot();
        if slot == active {
            return Err(DeploymentError::SlotInUse(slot));
        }

        let mut slots = self.slots.write();
        if let Some(info) = slots.get_mut(&slot) {
            info.status = SlotStatus::Deploying;
            info.version = version.to_string();
            info.deployed_at = current_timestamp();
            info.health_check_passed = false;
        }

        Ok(())
    }

    /// Mark a slot as ready (health check passed)
    pub fn mark_ready(&self, slot: DeploymentSlot) {
        let mut slots = self.slots.write();
        if let Some(info) = slots.get_mut(&slot) {
            info.status = SlotStatus::Standby;
            info.health_check_passed = true;
            info.last_health_check = current_timestamp();
        }
    }

    /// Mark a slot as unhealthy
    pub fn mark_unhealthy(&self, slot: DeploymentSlot) {
        let mut slots = self.slots.write();
        if let Some(info) = slots.get_mut(&slot) {
            info.status = SlotStatus::Unhealthy;
            info.health_check_passed = false;
            info.last_health_check = current_timestamp();
        }
    }

    /// Switch traffic to another slot
    pub async fn switch_to(
        &self,
        target_slot: DeploymentSlot,
        initiated_by: Option<&str>,
    ) -> Result<DeploymentRecord, DeploymentError> {
        // Check if switch already in progress
        if self.switch_in_progress.swap(true, Ordering::SeqCst) {
            return Err(DeploymentError::SwitchInProgress);
        }

        let result = self.do_switch(target_slot, initiated_by).await;

        self.switch_in_progress.store(false, Ordering::SeqCst);
        result
    }

    async fn do_switch(
        &self,
        target_slot: DeploymentSlot,
        initiated_by: Option<&str>,
    ) -> Result<DeploymentRecord, DeploymentError> {
        let current_slot = self.active_slot();
        if current_slot == target_slot {
            return Err(DeploymentError::AlreadyActive(target_slot));
        }

        // Check target slot is healthy
        {
            let slots = self.slots.read();
            let target_info = slots.get(&target_slot)
                .ok_or(DeploymentError::SlotNotFound(target_slot))?;

            if !target_info.health_check_passed {
                return Err(DeploymentError::SlotUnhealthy(target_slot));
            }
        }

        let start_time = current_timestamp();
        let current_version = self.get_slot_info(current_slot)
            .map(|i| i.version)
            .unwrap_or_default();
        let target_version = self.get_slot_info(target_slot)
            .map(|i| i.version)
            .unwrap_or_default();

        let record_id = self.history.read().len() as u64 + 1;

        let mut record = DeploymentRecord {
            id: record_id,
            from_slot: current_slot,
            to_slot: target_slot,
            from_version: current_version,
            to_version: target_version,
            started_at: start_time,
            completed_at: None,
            success: false,
            drain_result: None,
            initiated_by: initiated_by.map(|s| s.to_string()),
            rollback_of: None,
        };

        // Update slot statuses
        {
            let mut slots = self.slots.write();

            // Mark current slot as draining
            if let Some(info) = slots.get_mut(&current_slot) {
                info.status = SlotStatus::Draining;
            }

            // Mark target slot as active
            if let Some(info) = slots.get_mut(&target_slot) {
                info.status = SlotStatus::Active;
                info.traffic_weight = 100;
            }
        }

        // Switch active slot
        *self.active_slot.write() = target_slot;

        // Start draining current slot
        let tracker = self.get_tracker(current_slot);
        let drain_handle = tracker.start_drain();
        let drain_result = drain_handle.wait(&self.config.drain_config).await;

        // Update slot status after drain
        {
            let mut slots = self.slots.write();
            if let Some(info) = slots.get_mut(&current_slot) {
                info.status = SlotStatus::Standby;
                info.traffic_weight = 0;
            }
        }

        record.drain_result = Some(drain_result);
        record.completed_at = Some(current_timestamp());
        record.success = true;

        // Add to history
        self.history.write().push(record.clone());

        tracing::info!(
            from = %current_slot.as_str(),
            to = %target_slot.as_str(),
            duration_ms = record.completed_at.unwrap_or(0) - record.started_at,
            "Deployment switch completed"
        );

        Ok(record)
    }

    /// Rollback to previous deployment
    pub async fn rollback(&self, initiated_by: Option<&str>) -> Result<DeploymentRecord, DeploymentError> {
        let current = self.active_slot();
        let target = current.other();

        let mut record = self.switch_to(target, initiated_by).await?;

        // Mark as rollback
        if let Some(last) = self.history.read().iter().rev().nth(1) {
            record.rollback_of = Some(last.id);
        }

        Ok(record)
    }

    /// Set canary weight for target slot
    pub fn set_canary_weight(&self, slot: DeploymentSlot, weight: u8) -> Result<(), DeploymentError> {
        if weight > 100 {
            return Err(DeploymentError::InvalidWeight(weight));
        }

        let mut slots = self.slots.write();
        let other_slot = slot.other();

        if let Some(info) = slots.get_mut(&slot) {
            info.traffic_weight = weight;
        }
        if let Some(info) = slots.get_mut(&other_slot) {
            info.traffic_weight = 100 - weight;
        }

        Ok(())
    }

    /// Route a request (returns which slot should handle it)
    pub fn route_request(&self) -> DeploymentSlot {
        let slots = self.slots.read();
        let active = self.active_slot();

        // If canary is enabled and standby has weight, do weighted routing
        let standby = active.other();
        let standby_weight = slots.get(&standby)
            .map(|i| i.traffic_weight)
            .unwrap_or(0);

        if standby_weight > 0 {
            // Simple weighted random routing
            let rand_val = (current_timestamp() % 100) as u8;
            if rand_val < standby_weight {
                return standby;
            }
        }

        active
    }

    /// Get deployment history
    pub fn get_history(&self, limit: usize) -> Vec<DeploymentRecord> {
        let history = self.history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get deployment status
    pub fn status(&self) -> DeploymentStatus {
        let slots = self.slots.read();
        DeploymentStatus {
            active_slot: self.active_slot(),
            blue: slots.get(&DeploymentSlot::Blue).cloned(),
            green: slots.get(&DeploymentSlot::Green).cloned(),
            switch_in_progress: self.switch_in_progress.load(Ordering::SeqCst),
        }
    }
}

/// Deployment status
#[derive(Debug, Clone, Serialize)]
pub struct DeploymentStatus {
    pub active_slot: DeploymentSlot,
    pub blue: Option<SlotInfo>,
    pub green: Option<SlotInfo>,
    pub switch_in_progress: bool,
}

// ============================================================================
// Resource Pools / Workload Management
// ============================================================================

/// Resource pool for workload management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePool {
    /// Pool name
    pub name: String,
    /// Maximum concurrent queries
    pub max_concurrent_queries: usize,
    /// Maximum memory per query (bytes)
    pub max_memory_per_query: u64,
    /// Maximum total memory for pool (bytes)
    pub max_total_memory: u64,
    /// Query timeout in seconds
    pub query_timeout_secs: u64,
    /// Priority (higher = more resources)
    pub priority: u32,
    /// Whether pool is enabled
    pub enabled: bool,
}

impl Default for ResourcePool {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            max_concurrent_queries: 100,
            max_memory_per_query: 1024 * 1024 * 1024, // 1GB
            max_total_memory: 10 * 1024 * 1024 * 1024, // 10GB
            query_timeout_secs: 300,
            priority: 50,
            enabled: true,
        }
    }
}

/// Resource pool manager
pub struct ResourcePoolManager {
    /// Defined pools
    pools: RwLock<HashMap<String, ResourcePool>>,
    /// User to pool mappings
    user_pools: RwLock<HashMap<String, String>>,
    /// Current usage per pool
    usage: RwLock<HashMap<String, PoolUsage>>,
}

/// Current pool usage
#[derive(Debug, Clone, Default)]
pub struct PoolUsage {
    pub active_queries: usize,
    pub memory_used: u64,
    pub queued_queries: usize,
}

impl ResourcePoolManager {
    pub fn new() -> Self {
        let mut pools = HashMap::new();
        pools.insert("default".to_string(), ResourcePool::default());

        Self {
            pools: RwLock::new(pools),
            user_pools: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
        }
    }

    /// Create a resource pool
    pub fn create_pool(&self, pool: ResourcePool) {
        let name = pool.name.clone();
        self.pools.write().insert(name.clone(), pool);
        self.usage.write().insert(name, PoolUsage::default());
    }

    /// Delete a resource pool
    pub fn delete_pool(&self, name: &str) -> bool {
        if name == "default" {
            return false; // Cannot delete default pool
        }
        self.pools.write().remove(name).is_some()
    }

    /// Assign user to pool
    pub fn assign_user(&self, user: &str, pool: &str) {
        self.user_pools.write().insert(user.to_string(), pool.to_string());
    }

    /// Get pool for user
    pub fn get_user_pool(&self, user: &str) -> String {
        self.user_pools.read()
            .get(user)
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    }

    /// Try to acquire resources for a query
    pub fn acquire(&self, user: &str, estimated_memory: u64) -> Result<ResourceGrant, ResourceError> {
        let pool_name = self.get_user_pool(user);
        let pools = self.pools.read();
        let pool = pools.get(&pool_name)
            .ok_or_else(|| ResourceError::PoolNotFound(pool_name.clone()))?;

        if !pool.enabled {
            return Err(ResourceError::PoolDisabled(pool_name));
        }

        let mut usage = self.usage.write();
        let pool_usage = usage.entry(pool_name.clone()).or_default();

        // Check limits
        if pool_usage.active_queries >= pool.max_concurrent_queries {
            return Err(ResourceError::ConcurrencyLimit(pool_name));
        }

        if estimated_memory > pool.max_memory_per_query {
            return Err(ResourceError::MemoryLimit(estimated_memory, pool.max_memory_per_query));
        }

        if pool_usage.memory_used + estimated_memory > pool.max_total_memory {
            return Err(ResourceError::PoolMemoryExhausted(pool_name));
        }

        // Grant resources
        pool_usage.active_queries += 1;
        pool_usage.memory_used += estimated_memory;

        Ok(ResourceGrant {
            pool_name: pool_name.clone(),
            memory_granted: estimated_memory,
            timeout_secs: pool.query_timeout_secs,
        })
    }

    /// Release resources
    pub fn release(&self, grant: &ResourceGrant) {
        let mut usage = self.usage.write();
        if let Some(pool_usage) = usage.get_mut(&grant.pool_name) {
            pool_usage.active_queries = pool_usage.active_queries.saturating_sub(1);
            pool_usage.memory_used = pool_usage.memory_used.saturating_sub(grant.memory_granted);
        }
    }

    /// Get pool status
    pub fn get_pool_status(&self) -> Vec<PoolStatus> {
        let pools = self.pools.read();
        let usage = self.usage.read();

        pools.values().map(|pool| {
            let pool_usage = usage.get(&pool.name).cloned().unwrap_or_default();
            PoolStatus {
                name: pool.name.clone(),
                max_concurrent_queries: pool.max_concurrent_queries,
                active_queries: pool_usage.active_queries,
                max_total_memory: pool.max_total_memory,
                memory_used: pool_usage.memory_used,
                queued_queries: pool_usage.queued_queries,
                enabled: pool.enabled,
            }
        }).collect()
    }
}

impl Default for ResourcePoolManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Resource grant handle
#[derive(Debug, Clone)]
pub struct ResourceGrant {
    pub pool_name: String,
    pub memory_granted: u64,
    pub timeout_secs: u64,
}

/// Pool status
#[derive(Debug, Clone, Serialize)]
pub struct PoolStatus {
    pub name: String,
    pub max_concurrent_queries: usize,
    pub active_queries: usize,
    pub max_total_memory: u64,
    pub memory_used: u64,
    pub queued_queries: usize,
    pub enabled: bool,
}

// ============================================================================
// Error Types
// ============================================================================

/// Deployment error types
#[derive(Debug)]
pub enum DeploymentError {
    SlotInUse(DeploymentSlot),
    SlotNotFound(DeploymentSlot),
    SlotUnhealthy(DeploymentSlot),
    AlreadyActive(DeploymentSlot),
    SwitchInProgress,
    InvalidWeight(u8),
    DrainTimeout,
}

impl std::fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentError::SlotInUse(s) => write!(f, "Slot {} is in use", s.as_str()),
            DeploymentError::SlotNotFound(s) => write!(f, "Slot {} not found", s.as_str()),
            DeploymentError::SlotUnhealthy(s) => write!(f, "Slot {} is unhealthy", s.as_str()),
            DeploymentError::AlreadyActive(s) => write!(f, "Slot {} is already active", s.as_str()),
            DeploymentError::SwitchInProgress => write!(f, "Deployment switch already in progress"),
            DeploymentError::InvalidWeight(w) => write!(f, "Invalid weight: {} (must be 0-100)", w),
            DeploymentError::DrainTimeout => write!(f, "Connection drain timed out"),
        }
    }
}

impl std::error::Error for DeploymentError {}

/// Resource error types
#[derive(Debug)]
pub enum ResourceError {
    PoolNotFound(String),
    PoolDisabled(String),
    ConcurrencyLimit(String),
    MemoryLimit(u64, u64),
    PoolMemoryExhausted(String),
}

impl std::fmt::Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceError::PoolNotFound(p) => write!(f, "Pool not found: {}", p),
            ResourceError::PoolDisabled(p) => write!(f, "Pool disabled: {}", p),
            ResourceError::ConcurrencyLimit(p) => write!(f, "Concurrency limit reached for pool: {}", p),
            ResourceError::MemoryLimit(req, max) => write!(f, "Memory limit: requested {} exceeds max {}", req, max),
            ResourceError::PoolMemoryExhausted(p) => write!(f, "Pool memory exhausted: {}", p),
        }
    }
}

impl std::error::Error for ResourceError {}

// ============================================================================
// Utilities
// ============================================================================

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_slot() {
        assert_eq!(DeploymentSlot::Blue.other(), DeploymentSlot::Green);
        assert_eq!(DeploymentSlot::Green.other(), DeploymentSlot::Blue);
    }

    #[test]
    fn test_connection_tracker() {
        let tracker = ConnectionTracker::new();

        let id = tracker.register(Some("127.0.0.1".to_string())).unwrap();
        assert_eq!(tracker.active_count(), 1);

        tracker.update_activity(id);
        tracker.unregister(id);
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn test_resource_pool() {
        let mgr = ResourcePoolManager::new();

        let grant = mgr.acquire("user1", 100).unwrap();
        assert_eq!(grant.pool_name, "default");

        mgr.release(&grant);
    }

    #[test]
    fn test_user_pool_assignment() {
        let mgr = ResourcePoolManager::new();

        mgr.create_pool(ResourcePool {
            name: "vip".to_string(),
            ..Default::default()
        });

        mgr.assign_user("alice", "vip");
        assert_eq!(mgr.get_user_pool("alice"), "vip");
        assert_eq!(mgr.get_user_pool("bob"), "default");
    }
}
