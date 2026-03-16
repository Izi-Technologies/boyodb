//! Multi-Region Disaster Recovery
//!
//! Cross-region async replication with automatic failover,
//! DNS routing integration, and RPO/RTO configuration.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Region Configuration
// ============================================================================

/// Region identifier
pub type RegionId = String;

/// Region configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConfig {
    /// Region identifier (e.g., "us-east-1", "eu-west-1")
    pub region_id: RegionId,
    /// Display name
    pub name: String,
    /// Primary endpoint
    pub endpoint: String,
    /// Backup endpoints (for failover within region)
    pub backup_endpoints: Vec<String>,
    /// Is this the primary region?
    pub is_primary: bool,
    /// Replication priority (lower = higher priority for promotion)
    pub priority: u32,
    /// Maximum replication lag before alerting (milliseconds)
    pub max_lag_ms: u64,
    /// Network latency to primary (estimated, milliseconds)
    pub latency_ms: u64,
}

/// Disaster recovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisasterRecoveryConfig {
    /// Recovery Point Objective (maximum data loss in seconds)
    pub rpo_seconds: u64,
    /// Recovery Time Objective (maximum downtime in seconds)
    pub rto_seconds: u64,
    /// Automatic failover enabled
    pub auto_failover: bool,
    /// Minimum healthy replicas before failover
    pub min_healthy_replicas: u32,
    /// Health check interval (seconds)
    pub health_check_interval_secs: u64,
    /// Number of failed health checks before marking unhealthy
    pub unhealthy_threshold: u32,
    /// Number of successful health checks before marking healthy
    pub healthy_threshold: u32,
    /// DNS TTL for failover routing (seconds)
    pub dns_ttl_secs: u64,
    /// Enable write forwarding from replicas
    pub write_forwarding: bool,
}

impl Default for DisasterRecoveryConfig {
    fn default() -> Self {
        Self {
            rpo_seconds: 60,           // 1 minute max data loss
            rto_seconds: 300,          // 5 minutes max downtime
            auto_failover: true,
            min_healthy_replicas: 1,
            health_check_interval_secs: 10,
            unhealthy_threshold: 3,
            healthy_threshold: 2,
            dns_ttl_secs: 60,
            write_forwarding: true,
        }
    }
}

// ============================================================================
// Region Status
// ============================================================================

/// Health status of a region
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Replication status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStatus {
    /// Actively replicating
    Active,
    /// Replication paused
    Paused,
    /// Catching up after lag
    CatchingUp,
    /// Replication stopped/broken
    Stopped,
}

/// Region runtime status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStatus {
    pub region_id: RegionId,
    pub health: RegionHealth,
    pub replication_status: ReplicationStatus,
    /// Current replication lag (milliseconds)
    pub replication_lag_ms: u64,
    /// Last successful replication timestamp
    pub last_replicated_at: u64,
    /// Last health check timestamp
    pub last_health_check: u64,
    /// Consecutive failed health checks
    pub failed_checks: u32,
    /// Is currently primary
    pub is_primary: bool,
    /// Write operations per second
    pub writes_per_sec: f64,
    /// Read operations per second
    pub reads_per_sec: f64,
}

// ============================================================================
// Replication Events
// ============================================================================

/// Replication event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationEvent {
    /// Write-ahead log entry
    WalEntry {
        lsn: u64,
        data: Vec<u8>,
        timestamp: u64,
    },
    /// Snapshot chunk
    SnapshotChunk {
        snapshot_id: String,
        chunk_index: u32,
        total_chunks: u32,
        data: Vec<u8>,
    },
    /// Schema change
    SchemaChange {
        database: String,
        ddl: String,
        timestamp: u64,
    },
    /// Heartbeat
    Heartbeat {
        lsn: u64,
        timestamp: u64,
    },
}

/// Replication batch for efficient transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationBatch {
    pub source_region: RegionId,
    pub batch_id: u64,
    pub events: Vec<ReplicationEvent>,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub timestamp: u64,
}

// ============================================================================
// Failover Management
// ============================================================================

/// Failover event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub event_id: String,
    pub timestamp: u64,
    pub old_primary: RegionId,
    pub new_primary: RegionId,
    pub reason: FailoverReason,
    pub duration_ms: u64,
    pub data_loss_estimate_ms: u64,
}

/// Reason for failover
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverReason {
    /// Primary region became unhealthy
    PrimaryUnhealthy,
    /// Manual failover initiated
    Manual,
    /// Planned maintenance
    Maintenance,
    /// Network partition detected
    NetworkPartition,
    /// RPO threshold exceeded
    RpoExceeded,
}

/// Failover state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailoverState {
    /// Normal operation
    Normal,
    /// Failover in progress
    InProgress,
    /// Waiting for confirmation
    Confirming,
    /// Failover completed
    Completed,
    /// Failover failed, rollback
    Failed,
}

// ============================================================================
// DNS Routing
// ============================================================================

/// DNS record type for routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DnsRecordType {
    A,
    AAAA,
    CNAME,
}

/// DNS routing record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsRecord {
    pub name: String,
    pub record_type: DnsRecordType,
    pub value: String,
    pub ttl: u64,
    pub weight: u32,
    pub health_check_id: Option<String>,
}

/// DNS routing policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingPolicy {
    /// Route to primary only
    Primary,
    /// Route reads to nearest healthy region
    Latency,
    /// Weighted routing across regions
    Weighted { weights: HashMap<RegionId, u32> },
    /// Geolocation-based routing
    Geolocation { mappings: HashMap<String, RegionId> },
    /// Failover routing (primary with backup)
    Failover { primary: RegionId, secondary: RegionId },
}

/// DNS routing manager
pub struct DnsRoutingManager {
    config: DisasterRecoveryConfig,
    records: RwLock<HashMap<String, Vec<DnsRecord>>>,
    current_policy: RwLock<RoutingPolicy>,
    primary_region: RwLock<RegionId>,
}

impl DnsRoutingManager {
    pub fn new(config: DisasterRecoveryConfig, primary_region: RegionId) -> Self {
        Self {
            config,
            records: RwLock::new(HashMap::new()),
            current_policy: RwLock::new(RoutingPolicy::Primary),
            primary_region: RwLock::new(primary_region),
        }
    }

    /// Update DNS records for failover
    pub fn update_for_failover(&self, new_primary: &RegionId, new_endpoint: &str) {
        *self.primary_region.write() = new_primary.clone();

        // Update records to point to new primary
        let mut records = self.records.write();
        for record_set in records.values_mut() {
            for record in record_set.iter_mut() {
                if record.weight > 0 {
                    record.value = new_endpoint.to_string();
                    record.ttl = self.config.dns_ttl_secs;
                }
            }
        }
    }

    /// Get current DNS records for a hostname
    pub fn get_records(&self, hostname: &str) -> Vec<DnsRecord> {
        self.records
            .read()
            .get(hostname)
            .cloned()
            .unwrap_or_default()
    }

    /// Set routing policy
    pub fn set_policy(&self, policy: RoutingPolicy) {
        *self.current_policy.write() = policy;
    }

    /// Resolve hostname based on current policy
    pub fn resolve(&self, hostname: &str, client_region: Option<&str>) -> Option<String> {
        let records = self.records.read();
        let record_set = records.get(hostname)?;

        match &*self.current_policy.read() {
            RoutingPolicy::Primary => {
                record_set.first().map(|r| r.value.clone())
            }
            RoutingPolicy::Latency => {
                // Return record matching client region if available
                if let Some(region) = client_region {
                    record_set
                        .iter()
                        .find(|r| r.value.contains(region))
                        .or_else(|| record_set.first())
                        .map(|r| r.value.clone())
                } else {
                    record_set.first().map(|r| r.value.clone())
                }
            }
            RoutingPolicy::Weighted { weights } => {
                // Simple weighted selection
                let total_weight: u32 = weights.values().sum();
                if total_weight == 0 {
                    return record_set.first().map(|r| r.value.clone());
                }
                // Just return first for simplicity
                record_set.first().map(|r| r.value.clone())
            }
            RoutingPolicy::Geolocation { mappings } => {
                if let Some(region) = client_region {
                    if let Some(target) = mappings.get(region) {
                        return record_set
                            .iter()
                            .find(|r| r.value.contains(target))
                            .map(|r| r.value.clone());
                    }
                }
                record_set.first().map(|r| r.value.clone())
            }
            RoutingPolicy::Failover { primary, secondary: _ } => {
                record_set
                    .iter()
                    .find(|r| r.value.contains(primary))
                    .or_else(|| record_set.first())
                    .map(|r| r.value.clone())
            }
        }
    }
}

// ============================================================================
// Multi-Region Manager
// ============================================================================

/// Multi-region disaster recovery manager
pub struct MultiRegionDRManager {
    config: DisasterRecoveryConfig,
    regions: RwLock<HashMap<RegionId, RegionConfig>>,
    region_status: RwLock<HashMap<RegionId, RegionStatus>>,
    primary_region: RwLock<RegionId>,
    failover_state: RwLock<FailoverState>,
    failover_history: RwLock<Vec<FailoverEvent>>,
    dns_manager: Arc<DnsRoutingManager>,
    /// Current LSN on primary
    current_lsn: AtomicU64,
    /// Is the manager running
    running: AtomicBool,
    /// Replication streams per region
    replication_cursors: RwLock<HashMap<RegionId, u64>>,
}

impl MultiRegionDRManager {
    pub fn new(config: DisasterRecoveryConfig, primary_region: RegionId) -> Self {
        let dns_manager = Arc::new(DnsRoutingManager::new(
            config.clone(),
            primary_region.clone(),
        ));

        Self {
            config,
            regions: RwLock::new(HashMap::new()),
            region_status: RwLock::new(HashMap::new()),
            primary_region: RwLock::new(primary_region),
            failover_state: RwLock::new(FailoverState::Normal),
            failover_history: RwLock::new(Vec::new()),
            dns_manager,
            current_lsn: AtomicU64::new(0),
            running: AtomicBool::new(false),
            replication_cursors: RwLock::new(HashMap::new()),
        }
    }

    /// Register a region
    pub fn register_region(&self, region: RegionConfig) {
        let region_id = region.region_id.clone();
        let is_primary = region.is_primary;

        self.regions.write().insert(region_id.clone(), region);

        // Initialize status
        let status = RegionStatus {
            region_id: region_id.clone(),
            health: RegionHealth::Unknown,
            replication_status: if is_primary {
                ReplicationStatus::Active
            } else {
                ReplicationStatus::Stopped
            },
            replication_lag_ms: 0,
            last_replicated_at: 0,
            last_health_check: 0,
            failed_checks: 0,
            is_primary,
            writes_per_sec: 0.0,
            reads_per_sec: 0.0,
        };
        self.region_status.write().insert(region_id.clone(), status);
        self.replication_cursors.write().insert(region_id, 0);
    }

    /// Get current primary region
    pub fn primary_region(&self) -> RegionId {
        self.primary_region.read().clone()
    }

    /// Get all region statuses
    pub fn get_all_status(&self) -> Vec<RegionStatus> {
        self.region_status.read().values().cloned().collect()
    }

    /// Get status for a specific region
    pub fn get_region_status(&self, region_id: &str) -> Option<RegionStatus> {
        self.region_status.read().get(region_id).cloned()
    }

    /// Update health check result
    pub fn update_health(&self, region_id: &str, healthy: bool, latency_ms: u64) {
        let mut status = self.region_status.write();
        if let Some(region_status) = status.get_mut(region_id) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            region_status.last_health_check = now;

            if healthy {
                region_status.failed_checks = 0;
                if region_status.health != RegionHealth::Healthy {
                    region_status.health = RegionHealth::Healthy;
                }
            } else {
                region_status.failed_checks += 1;
                if region_status.failed_checks >= self.config.unhealthy_threshold {
                    region_status.health = RegionHealth::Unhealthy;
                } else {
                    region_status.health = RegionHealth::Degraded;
                }
            }

            // Update latency in config
            if let Some(region_config) = self.regions.write().get_mut(region_id) {
                region_config.latency_ms = latency_ms;
            }
        }
    }

    /// Update replication progress
    pub fn update_replication_progress(&self, region_id: &str, lsn: u64) {
        let current_lsn = self.current_lsn.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.replication_cursors.write().insert(region_id.to_string(), lsn);

        let mut status = self.region_status.write();
        if let Some(region_status) = status.get_mut(region_id) {
            region_status.last_replicated_at = now;

            // Estimate lag based on LSN difference
            if current_lsn > lsn {
                // Rough estimate: assume 1000 LSN = 1 second of writes
                region_status.replication_lag_ms = ((current_lsn - lsn) / 1000) * 1000;
            } else {
                region_status.replication_lag_ms = 0;
            }

            if region_status.replication_status == ReplicationStatus::CatchingUp
                && region_status.replication_lag_ms < 1000
            {
                region_status.replication_status = ReplicationStatus::Active;
            }
        }
    }

    /// Advance the primary LSN
    pub fn advance_lsn(&self, delta: u64) -> u64 {
        self.current_lsn.fetch_add(delta, Ordering::Relaxed) + delta
    }

    /// Check if RPO is being violated
    pub fn check_rpo_violation(&self) -> Vec<(RegionId, u64)> {
        let rpo_ms = self.config.rpo_seconds * 1000;
        let status = self.region_status.read();

        status
            .values()
            .filter(|s| !s.is_primary && s.replication_lag_ms > rpo_ms)
            .map(|s| (s.region_id.clone(), s.replication_lag_ms))
            .collect()
    }

    /// Check if automatic failover should be triggered
    pub fn should_failover(&self) -> Option<FailoverReason> {
        if !self.config.auto_failover {
            return None;
        }

        let primary_id = self.primary_region.read().clone();
        let status = self.region_status.read();

        if let Some(primary_status) = status.get(&primary_id) {
            if primary_status.health == RegionHealth::Unhealthy {
                return Some(FailoverReason::PrimaryUnhealthy);
            }
        }

        // Check for network partition (all replicas unhealthy)
        let healthy_replicas = status
            .values()
            .filter(|s| !s.is_primary && s.health == RegionHealth::Healthy)
            .count();

        if healthy_replicas == 0 && status.len() > 1 {
            // Might be network partition from primary's perspective
            // Don't auto-failover in this case
            return None;
        }

        None
    }

    /// Initiate failover to a specific region
    pub fn initiate_failover(
        &self,
        new_primary: &str,
        reason: FailoverReason,
    ) -> Result<FailoverEvent, DRError> {
        // Check current state
        {
            let state = self.failover_state.read();
            if *state == FailoverState::InProgress {
                return Err(DRError::FailoverInProgress);
            }
        }

        // Verify new primary exists and is healthy
        {
            let regions = self.regions.read();
            if !regions.contains_key(new_primary) {
                return Err(DRError::RegionNotFound(new_primary.to_string()));
            }
        }

        let status = self.region_status.read();
        let new_primary_status = status
            .get(new_primary)
            .ok_or_else(|| DRError::RegionNotFound(new_primary.to_string()))?;

        if new_primary_status.health == RegionHealth::Unhealthy {
            return Err(DRError::RegionUnhealthy(new_primary.to_string()));
        }

        let old_primary = self.primary_region.read().clone();
        let start_time = Instant::now();

        // Set failover state
        *self.failover_state.write() = FailoverState::InProgress;

        // Update primary
        *self.primary_region.write() = new_primary.to_string();

        // Update region status
        drop(status);
        let mut status = self.region_status.write();
        if let Some(old_status) = status.get_mut(&old_primary) {
            old_status.is_primary = false;
            old_status.replication_status = ReplicationStatus::Stopped;
        }
        if let Some(new_status) = status.get_mut(new_primary) {
            new_status.is_primary = true;
            new_status.replication_status = ReplicationStatus::Active;
        }

        // Update DNS
        let regions = self.regions.read();
        if let Some(new_region_config) = regions.get(new_primary) {
            self.dns_manager
                .update_for_failover(&new_primary.to_string(), &new_region_config.endpoint);
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        // Estimate data loss
        let data_loss_ms = status
            .get(new_primary)
            .map(|s| s.replication_lag_ms)
            .unwrap_or(0);

        let event = FailoverEvent {
            event_id: uuid_v4(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            old_primary,
            new_primary: new_primary.to_string(),
            reason,
            duration_ms,
            data_loss_estimate_ms: data_loss_ms,
        };

        // Record event
        self.failover_history.write().push(event.clone());

        // Complete failover
        *self.failover_state.write() = FailoverState::Completed;

        Ok(event)
    }

    /// Get failover history
    pub fn failover_history(&self) -> Vec<FailoverEvent> {
        self.failover_history.read().clone()
    }

    /// Select best failover candidate
    pub fn select_failover_candidate(&self) -> Option<RegionId> {
        let regions = self.regions.read();
        let status = self.region_status.read();
        let primary = self.primary_region.read();

        // Find healthy non-primary regions sorted by priority then lag
        let mut candidates: Vec<_> = regions
            .values()
            .filter(|r| {
                r.region_id != *primary
                    && status
                        .get(&r.region_id)
                        .map(|s| s.health == RegionHealth::Healthy)
                        .unwrap_or(false)
            })
            .collect();

        candidates.sort_by(|a, b| {
            let lag_a = status
                .get(&a.region_id)
                .map(|s| s.replication_lag_ms)
                .unwrap_or(u64::MAX);
            let lag_b = status
                .get(&b.region_id)
                .map(|s| s.replication_lag_ms)
                .unwrap_or(u64::MAX);

            a.priority.cmp(&b.priority).then(lag_a.cmp(&lag_b))
        });

        candidates.first().map(|r| r.region_id.clone())
    }

    /// Create replication batch from LSN range
    pub fn create_replication_batch(
        &self,
        _from_lsn: u64,
        _to_lsn: u64,
    ) -> Result<ReplicationBatch, DRError> {
        // In a real implementation, this would read from WAL
        let batch = ReplicationBatch {
            source_region: self.primary_region.read().clone(),
            batch_id: self.current_lsn.load(Ordering::Relaxed),
            events: vec![ReplicationEvent::Heartbeat {
                lsn: self.current_lsn.load(Ordering::Relaxed),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            }],
            start_lsn: _from_lsn,
            end_lsn: _to_lsn,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        Ok(batch)
    }

    /// Apply replication batch on replica
    pub fn apply_replication_batch(&self, batch: ReplicationBatch) -> Result<(), DRError> {
        // In a real implementation, this would apply WAL entries
        for event in batch.events {
            match event {
                ReplicationEvent::WalEntry { lsn, .. } => {
                    self.update_replication_progress(&batch.source_region, lsn);
                }
                ReplicationEvent::Heartbeat { lsn, .. } => {
                    self.update_replication_progress(&batch.source_region, lsn);
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Get DR metrics
    pub fn metrics(&self) -> DRMetrics {
        let status = self.region_status.read();
        let primary_id = self.primary_region.read().clone();

        let healthy_regions = status
            .values()
            .filter(|s| s.health == RegionHealth::Healthy)
            .count();

        let max_lag = status.values().map(|s| s.replication_lag_ms).max().unwrap_or(0);

        let avg_lag = if !status.is_empty() {
            status.values().map(|s| s.replication_lag_ms).sum::<u64>() / status.len() as u64
        } else {
            0
        };

        DRMetrics {
            primary_region: primary_id,
            total_regions: status.len(),
            healthy_regions,
            max_replication_lag_ms: max_lag,
            avg_replication_lag_ms: avg_lag,
            current_lsn: self.current_lsn.load(Ordering::Relaxed),
            failover_count: self.failover_history.read().len(),
            rpo_violations: self.check_rpo_violation().len(),
        }
    }
}

/// DR metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRMetrics {
    pub primary_region: RegionId,
    pub total_regions: usize,
    pub healthy_regions: usize,
    pub max_replication_lag_ms: u64,
    pub avg_replication_lag_ms: u64,
    pub current_lsn: u64,
    pub failover_count: usize,
    pub rpo_violations: usize,
}

/// DR errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DRError {
    RegionNotFound(String),
    RegionUnhealthy(String),
    FailoverInProgress,
    ReplicationError(String),
    ConfigurationError(String),
}

impl std::fmt::Display for DRError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DRError::RegionNotFound(r) => write!(f, "Region not found: {}", r),
            DRError::RegionUnhealthy(r) => write!(f, "Region unhealthy: {}", r),
            DRError::FailoverInProgress => write!(f, "Failover already in progress"),
            DRError::ReplicationError(e) => write!(f, "Replication error: {}", e),
            DRError::ConfigurationError(e) => write!(f, "Configuration error: {}", e),
        }
    }
}

impl std::error::Error for DRError {}

/// Generate a simple UUID v4
fn uuid_v4() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    let h1 = hasher.finish();
    hasher.write_u64(h1);
    let h2 = hasher.finish();

    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (h1 >> 32) as u32,
        (h1 >> 16) as u16 & 0xffff,
        h1 as u16 & 0x0fff,
        (h2 >> 48) as u16 & 0x3fff | 0x8000,
        h2 & 0xffffffffffff
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_registration() {
        let config = DisasterRecoveryConfig::default();
        let manager = MultiRegionDRManager::new(config, "us-east-1".to_string());

        let region = RegionConfig {
            region_id: "us-east-1".to_string(),
            name: "US East".to_string(),
            endpoint: "db.us-east-1.example.com".to_string(),
            backup_endpoints: vec![],
            is_primary: true,
            priority: 1,
            max_lag_ms: 5000,
            latency_ms: 10,
        };

        manager.register_region(region);

        let status = manager.get_region_status("us-east-1");
        assert!(status.is_some());
        assert!(status.unwrap().is_primary);
    }

    #[test]
    fn test_health_updates() {
        let config = DisasterRecoveryConfig::default();
        let manager = MultiRegionDRManager::new(config, "us-east-1".to_string());

        let region = RegionConfig {
            region_id: "us-east-1".to_string(),
            name: "US East".to_string(),
            endpoint: "db.us-east-1.example.com".to_string(),
            backup_endpoints: vec![],
            is_primary: true,
            priority: 1,
            max_lag_ms: 5000,
            latency_ms: 10,
        };

        manager.register_region(region);
        manager.update_health("us-east-1", true, 5);

        let status = manager.get_region_status("us-east-1").unwrap();
        assert_eq!(status.health, RegionHealth::Healthy);
    }

    #[test]
    fn test_failover() {
        let config = DisasterRecoveryConfig::default();
        let manager = MultiRegionDRManager::new(config, "us-east-1".to_string());

        // Register primary
        manager.register_region(RegionConfig {
            region_id: "us-east-1".to_string(),
            name: "US East".to_string(),
            endpoint: "db.us-east-1.example.com".to_string(),
            backup_endpoints: vec![],
            is_primary: true,
            priority: 1,
            max_lag_ms: 5000,
            latency_ms: 10,
        });

        // Register replica
        manager.register_region(RegionConfig {
            region_id: "us-west-2".to_string(),
            name: "US West".to_string(),
            endpoint: "db.us-west-2.example.com".to_string(),
            backup_endpoints: vec![],
            is_primary: false,
            priority: 2,
            max_lag_ms: 5000,
            latency_ms: 50,
        });

        // Mark replica healthy
        manager.update_health("us-west-2", true, 50);

        // Initiate failover
        let result = manager.initiate_failover("us-west-2", FailoverReason::Manual);
        assert!(result.is_ok());

        let event = result.unwrap();
        assert_eq!(event.old_primary, "us-east-1");
        assert_eq!(event.new_primary, "us-west-2");

        assert_eq!(manager.primary_region(), "us-west-2");
    }
}
