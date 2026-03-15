//! Synchronous Replication
//!
//! This module implements synchronous commit options for replication,
//! allowing configuration of durability guarantees across replicas.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum SyncReplicationError {
    /// Timeout waiting for acknowledgment
    Timeout,
    /// Not enough replicas available
    NotEnoughReplicas { required: usize, available: usize },
    /// Replica not found
    ReplicaNotFound(String),
    /// Commit failed
    CommitFailed(String),
    /// Invalid configuration
    InvalidConfig(String),
}

impl fmt::Display for SyncReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncReplicationError::Timeout => write!(f, "Timeout waiting for acknowledgment"),
            SyncReplicationError::NotEnoughReplicas { required, available } => {
                write!(
                    f,
                    "Not enough replicas: required {}, available {}",
                    required, available
                )
            }
            SyncReplicationError::ReplicaNotFound(name) => {
                write!(f, "Replica not found: {}", name)
            }
            SyncReplicationError::CommitFailed(msg) => write!(f, "Commit failed: {}", msg),
            SyncReplicationError::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
        }
    }
}

impl std::error::Error for SyncReplicationError {}

// ============================================================================
// Synchronous Commit Levels
// ============================================================================

/// Level of synchronous commit guarantee
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousCommit {
    /// No synchronous replication (fastest, least durable)
    Off,
    /// Wait for local WAL flush only
    Local,
    /// Wait for remote WAL write (not flush)
    RemoteWrite,
    /// Wait for remote WAL flush
    RemoteFlush,
    /// Wait for remote WAL flush and apply (most durable)
    RemoteApply,
}

impl SynchronousCommit {
    pub fn durability_level(&self) -> u8 {
        match self {
            SynchronousCommit::Off => 0,
            SynchronousCommit::Local => 1,
            SynchronousCommit::RemoteWrite => 2,
            SynchronousCommit::RemoteFlush => 3,
            SynchronousCommit::RemoteApply => 4,
        }
    }
}

impl Default for SynchronousCommit {
    fn default() -> Self {
        SynchronousCommit::Local
    }
}

// ============================================================================
// Replica Status
// ============================================================================

/// Connection state of a replica
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaConnectionState {
    /// Not connected
    Disconnected,
    /// Connecting
    Connecting,
    /// Connected and streaming
    Streaming,
    /// Catching up
    CatchingUp,
    /// Error state
    Error,
}

/// Synchronous state of a replica
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Asynchronous replica
    Async,
    /// Potential synchronous (standby)
    Potential,
    /// Currently synchronous
    Sync,
    /// Quorum participant
    Quorum,
}

/// Information about a replica
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Replica name/identifier
    pub name: String,
    /// Connection state
    pub connection_state: ReplicaConnectionState,
    /// Synchronous state
    pub sync_state: SyncState,
    /// Priority for synchronous selection (lower = higher priority)
    pub priority: u32,
    /// Last received LSN
    pub received_lsn: u64,
    /// Last written LSN
    pub write_lsn: u64,
    /// Last flushed LSN
    pub flush_lsn: u64,
    /// Last applied LSN
    pub apply_lsn: u64,
    /// Lag in bytes
    pub lag_bytes: u64,
    /// Lag in time
    pub lag_duration: Duration,
    /// Last heartbeat
    pub last_heartbeat: Option<Instant>,
}

impl ReplicaInfo {
    pub fn new(name: &str, priority: u32) -> Self {
        Self {
            name: name.to_string(),
            connection_state: ReplicaConnectionState::Disconnected,
            sync_state: SyncState::Async,
            priority,
            received_lsn: 0,
            write_lsn: 0,
            flush_lsn: 0,
            apply_lsn: 0,
            lag_bytes: 0,
            lag_duration: Duration::ZERO,
            last_heartbeat: None,
        }
    }

    /// Check if replica has reached the given LSN for the commit level
    pub fn has_reached(&self, lsn: u64, level: SynchronousCommit) -> bool {
        match level {
            SynchronousCommit::Off | SynchronousCommit::Local => true,
            SynchronousCommit::RemoteWrite => self.write_lsn >= lsn,
            SynchronousCommit::RemoteFlush => self.flush_lsn >= lsn,
            SynchronousCommit::RemoteApply => self.apply_lsn >= lsn,
        }
    }

    /// Update heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    /// Check if replica is healthy
    pub fn is_healthy(&self, timeout: Duration) -> bool {
        if self.connection_state != ReplicaConnectionState::Streaming {
            return false;
        }
        if let Some(heartbeat) = self.last_heartbeat {
            heartbeat.elapsed() < timeout
        } else {
            false
        }
    }
}

// ============================================================================
// Pending Commit
// ============================================================================

/// A commit waiting for acknowledgment
#[derive(Debug)]
struct PendingCommit {
    /// Transaction ID
    xid: u64,
    /// LSN of the commit
    lsn: u64,
    /// Required commit level
    level: SynchronousCommit,
    /// Start time
    start_time: Instant,
    /// Replicas that have acknowledged
    acknowledged_by: HashSet<String>,
    /// Number of acknowledgments needed
    required_acks: usize,
    /// Completion flag
    completed: bool,
}

impl PendingCommit {
    fn new(xid: u64, lsn: u64, level: SynchronousCommit, required_acks: usize) -> Self {
        Self {
            xid,
            lsn,
            level,
            start_time: Instant::now(),
            acknowledged_by: HashSet::new(),
            required_acks,
            completed: false,
        }
    }

    fn add_ack(&mut self, replica: &str) {
        self.acknowledged_by.insert(replica.to_string());
    }

    fn is_satisfied(&self) -> bool {
        self.acknowledged_by.len() >= self.required_acks
    }
}

// ============================================================================
// Synchronous Standby Configuration
// ============================================================================

/// Configuration for synchronous standbys
#[derive(Debug, Clone)]
pub struct SyncStandbyConfig {
    /// Synchronous standby names pattern
    /// e.g., "FIRST 2 (replica1, replica2, replica3)" or "ANY 2 (replica1, replica2)"
    pub names: SyncStandbyNames,
    /// Commit timeout
    pub timeout: Duration,
    /// Whether to wait for all replicas or fail fast
    pub wait_for_all: bool,
}

impl Default for SyncStandbyConfig {
    fn default() -> Self {
        Self {
            names: SyncStandbyNames::Any(1, vec![]),
            timeout: Duration::from_secs(30),
            wait_for_all: false,
        }
    }
}

/// Synchronous standby names configuration
#[derive(Debug, Clone)]
pub enum SyncStandbyNames {
    /// First N of the listed standbys (ordered by priority)
    First(usize, Vec<String>),
    /// Any N of the listed standbys
    Any(usize, Vec<String>),
    /// All listed standbys
    All(Vec<String>),
}

impl SyncStandbyNames {
    /// Get required acknowledgment count
    pub fn required_acks(&self) -> usize {
        match self {
            SyncStandbyNames::First(n, _) => *n,
            SyncStandbyNames::Any(n, _) => *n,
            SyncStandbyNames::All(names) => names.len(),
        }
    }

    /// Check if a replica is in the sync standby list
    pub fn contains(&self, name: &str) -> bool {
        match self {
            SyncStandbyNames::First(_, names) => names.contains(&name.to_string()),
            SyncStandbyNames::Any(_, names) => names.contains(&name.to_string()),
            SyncStandbyNames::All(names) => names.contains(&name.to_string()),
        }
    }

    /// Get all standby names
    pub fn names(&self) -> &[String] {
        match self {
            SyncStandbyNames::First(_, names) => names,
            SyncStandbyNames::Any(_, names) => names,
            SyncStandbyNames::All(names) => names,
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics for synchronous replication
#[derive(Debug, Default)]
pub struct SyncReplicationStats {
    /// Commits waiting for sync
    pub commits_waiting: AtomicU64,
    /// Commits completed
    pub commits_completed: AtomicU64,
    /// Commits timed out
    pub commits_timed_out: AtomicU64,
    /// Total wait time (nanoseconds)
    pub total_wait_time_ns: AtomicU64,
    /// Acknowledgments received
    pub acks_received: AtomicU64,
    /// Current sync replicas
    pub sync_replicas: AtomicU64,
}

impl SyncReplicationStats {
    pub fn snapshot(&self) -> SyncReplicationStatsSnapshot {
        SyncReplicationStatsSnapshot {
            commits_waiting: self.commits_waiting.load(Ordering::Relaxed),
            commits_completed: self.commits_completed.load(Ordering::Relaxed),
            commits_timed_out: self.commits_timed_out.load(Ordering::Relaxed),
            total_wait_time_ns: self.total_wait_time_ns.load(Ordering::Relaxed),
            acks_received: self.acks_received.load(Ordering::Relaxed),
            sync_replicas: self.sync_replicas.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of statistics
#[derive(Debug, Clone)]
pub struct SyncReplicationStatsSnapshot {
    pub commits_waiting: u64,
    pub commits_completed: u64,
    pub commits_timed_out: u64,
    pub total_wait_time_ns: u64,
    pub acks_received: u64,
    pub sync_replicas: u64,
}

impl SyncReplicationStatsSnapshot {
    /// Average wait time per commit
    pub fn average_wait_time(&self) -> Duration {
        if self.commits_completed == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.total_wait_time_ns / self.commits_completed)
    }

    /// Success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.commits_completed + self.commits_timed_out;
        if total == 0 {
            return 1.0;
        }
        self.commits_completed as f64 / total as f64
    }
}

// ============================================================================
// Synchronous Replication Manager
// ============================================================================

/// Manages synchronous replication
pub struct SyncReplicationManager {
    /// Current commit level
    commit_level: RwLock<SynchronousCommit>,
    /// Standby configuration
    standby_config: RwLock<SyncStandbyConfig>,
    /// Known replicas
    replicas: RwLock<HashMap<String, ReplicaInfo>>,
    /// Pending commits waiting for acknowledgment
    pending_commits: Mutex<VecDeque<PendingCommit>>,
    /// Condition variable for waiting
    commit_cv: Condvar,
    /// Statistics
    stats: SyncReplicationStats,
    /// Current primary LSN
    primary_lsn: AtomicU64,
    /// Enabled flag
    enabled: AtomicBool,
}

impl SyncReplicationManager {
    pub fn new() -> Self {
        Self {
            commit_level: RwLock::new(SynchronousCommit::default()),
            standby_config: RwLock::new(SyncStandbyConfig::default()),
            replicas: RwLock::new(HashMap::new()),
            pending_commits: Mutex::new(VecDeque::new()),
            commit_cv: Condvar::new(),
            stats: SyncReplicationStats::default(),
            primary_lsn: AtomicU64::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    /// Set synchronous commit level
    pub fn set_commit_level(&self, level: SynchronousCommit) {
        *self.commit_level.write() = level;
    }

    /// Get current commit level
    pub fn commit_level(&self) -> SynchronousCommit {
        *self.commit_level.read()
    }

    /// Configure synchronous standbys
    pub fn set_standby_config(&self, config: SyncStandbyConfig) {
        *self.standby_config.write() = config;
        self.update_sync_states();
    }

    /// Get standby config
    pub fn standby_config(&self) -> SyncStandbyConfig {
        self.standby_config.read().clone()
    }

    /// Register a replica
    pub fn register_replica(&self, name: &str, priority: u32) {
        let mut replicas = self.replicas.write();
        replicas.insert(name.to_string(), ReplicaInfo::new(name, priority));
        self.update_sync_states();
    }

    /// Unregister a replica
    pub fn unregister_replica(&self, name: &str) {
        let mut replicas = self.replicas.write();
        replicas.remove(name);
        self.update_sync_states();
    }

    /// Update replica LSN progress
    pub fn update_replica_progress(
        &self,
        name: &str,
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
    ) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(name) {
            replica.write_lsn = write_lsn;
            replica.flush_lsn = flush_lsn;
            replica.apply_lsn = apply_lsn;
            replica.heartbeat();

            // Calculate lag
            let current_lsn = self.primary_lsn.load(Ordering::Relaxed);
            replica.lag_bytes = current_lsn.saturating_sub(apply_lsn);
        }
        drop(replicas);

        // Check if any pending commits can be satisfied
        self.check_pending_commits();
    }

    /// Update replica connection state
    pub fn update_replica_state(&self, name: &str, state: ReplicaConnectionState) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(name) {
            replica.connection_state = state;
        }
        drop(replicas);
        self.update_sync_states();
    }

    /// Set primary LSN
    pub fn set_primary_lsn(&self, lsn: u64) {
        self.primary_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Wait for synchronous commit
    pub fn wait_for_sync(
        &self,
        xid: u64,
        lsn: u64,
    ) -> Result<Duration, SyncReplicationError> {
        let level = self.commit_level();

        // No waiting needed for off or local
        if level == SynchronousCommit::Off || level == SynchronousCommit::Local {
            return Ok(Duration::ZERO);
        }

        if !self.enabled.load(Ordering::Relaxed) {
            return Ok(Duration::ZERO);
        }

        let config = self.standby_config.read().clone();
        let required_acks = config.names.required_acks();

        // Check if we have enough replicas
        let available = self.get_sync_replica_count();
        if available < required_acks {
            return Err(SyncReplicationError::NotEnoughReplicas {
                required: required_acks,
                available,
            });
        }

        // Create pending commit
        let pending = PendingCommit::new(xid, lsn, level, required_acks);
        let start_time = pending.start_time;

        {
            let mut commits = self.pending_commits.lock();
            commits.push_back(pending);
            self.stats.commits_waiting.fetch_add(1, Ordering::Relaxed);
        }

        // Check if already satisfied (replicas might have caught up)
        self.check_pending_commits();

        // Wait for completion
        let timeout = config.timeout;
        let mut commits = self.pending_commits.lock();

        loop {
            // Find our commit
            let pos = commits.iter().position(|c| c.xid == xid && c.lsn == lsn);

            match pos {
                Some(idx) => {
                    if commits[idx].completed || commits[idx].is_satisfied() {
                        // Complete
                        commits.remove(idx);
                        let elapsed = start_time.elapsed();
                        self.stats.commits_waiting.fetch_sub(1, Ordering::Relaxed);
                        self.stats.commits_completed.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .total_wait_time_ns
                            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
                        return Ok(elapsed);
                    }

                    // Check timeout
                    if start_time.elapsed() >= timeout {
                        commits.remove(idx);
                        self.stats.commits_waiting.fetch_sub(1, Ordering::Relaxed);
                        self.stats.commits_timed_out.fetch_add(1, Ordering::Relaxed);
                        return Err(SyncReplicationError::Timeout);
                    }

                    // Wait
                    let remaining = timeout.saturating_sub(start_time.elapsed());
                    self.commit_cv.wait_for(&mut commits, remaining);
                }
                None => {
                    // Already completed and removed
                    return Ok(start_time.elapsed());
                }
            }
        }
    }

    fn check_pending_commits(&self) {
        let replicas = self.replicas.read();
        let config = self.standby_config.read();
        let mut commits = self.pending_commits.lock();

        for commit in commits.iter_mut() {
            if commit.completed {
                continue;
            }

            for (name, replica) in replicas.iter() {
                if config.names.contains(name) && replica.has_reached(commit.lsn, commit.level) {
                    commit.add_ack(name);
                    self.stats.acks_received.fetch_add(1, Ordering::Relaxed);
                }
            }

            if commit.is_satisfied() {
                commit.completed = true;
            }
        }

        drop(commits);
        drop(config);
        drop(replicas);

        // Notify waiters
        self.commit_cv.notify_all();
    }

    fn update_sync_states(&self) {
        let config = self.standby_config.read();
        let mut replicas = self.replicas.write();
        let mut sync_count = 0u64;

        // First pass: determine which replicas are sync eligible
        // Collect as owned strings to avoid borrow conflicts
        let mut eligible: Vec<(String, u32)> = replicas
            .iter()
            .filter(|(name, r)| {
                config.names.contains(name)
                    && r.connection_state == ReplicaConnectionState::Streaming
            })
            .map(|(name, r)| (name.clone(), r.priority))
            .collect();

        // Sort by priority (lower = higher priority)
        eligible.sort_by_key(|(_, p)| *p);

        match &config.names {
            SyncStandbyNames::First(n, _) => {
                // First N by priority are sync
                for (i, (name, _)) in eligible.iter().enumerate() {
                    if let Some(r) = replicas.get_mut(name) {
                        if i < *n {
                            r.sync_state = SyncState::Sync;
                            sync_count += 1;
                        } else {
                            r.sync_state = SyncState::Potential;
                        }
                    }
                }
            }
            SyncStandbyNames::Any(n, _) => {
                // Any N are quorum
                for (i, (name, _)) in eligible.iter().enumerate() {
                    if let Some(r) = replicas.get_mut(name) {
                        if i < *n {
                            r.sync_state = SyncState::Quorum;
                            sync_count += 1;
                        } else {
                            r.sync_state = SyncState::Potential;
                        }
                    }
                }
            }
            SyncStandbyNames::All(_) => {
                // All are sync
                for (name, _) in &eligible {
                    if let Some(r) = replicas.get_mut(name) {
                        r.sync_state = SyncState::Sync;
                        sync_count += 1;
                    }
                }
            }
        }

        // Collect eligible names for quick lookup
        let eligible_names: std::collections::HashSet<&str> =
            eligible.iter().map(|(name, _)| name.as_str()).collect();

        // Mark non-eligible replicas as async (including those in config but not streaming)
        for (name, replica) in replicas.iter_mut() {
            if !eligible_names.contains(name.as_str()) {
                replica.sync_state = SyncState::Async;
            }
        }

        self.stats.sync_replicas.store(sync_count, Ordering::Relaxed);
    }

    fn get_sync_replica_count(&self) -> usize {
        let replicas = self.replicas.read();
        replicas
            .values()
            .filter(|r| {
                r.connection_state == ReplicaConnectionState::Streaming
                    && matches!(r.sync_state, SyncState::Sync | SyncState::Quorum)
            })
            .count()
    }

    /// Get replica info
    pub fn get_replica(&self, name: &str) -> Option<ReplicaInfo> {
        let replicas = self.replicas.read();
        replicas.get(name).cloned()
    }

    /// List all replicas
    pub fn list_replicas(&self) -> Vec<ReplicaInfo> {
        let replicas = self.replicas.read();
        replicas.values().cloned().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> SyncReplicationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Enable synchronous replication
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Disable synchronous replication
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        // Wake up all waiters
        self.commit_cv.notify_all();
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Get pending commit count
    pub fn pending_commit_count(&self) -> usize {
        self.pending_commits.lock().len()
    }
}

impl Default for SyncReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_commit_levels() {
        assert!(SynchronousCommit::RemoteApply.durability_level() > SynchronousCommit::RemoteFlush.durability_level());
        assert!(SynchronousCommit::RemoteFlush.durability_level() > SynchronousCommit::Local.durability_level());
    }

    #[test]
    fn test_replica_info() {
        let mut replica = ReplicaInfo::new("replica1", 1);

        assert_eq!(replica.name, "replica1");
        assert_eq!(replica.priority, 1);
        assert_eq!(replica.connection_state, ReplicaConnectionState::Disconnected);

        replica.write_lsn = 100;
        replica.flush_lsn = 90;
        replica.apply_lsn = 80;

        assert!(replica.has_reached(80, SynchronousCommit::RemoteApply));
        assert!(!replica.has_reached(100, SynchronousCommit::RemoteApply));
        assert!(replica.has_reached(90, SynchronousCommit::RemoteFlush));
    }

    #[test]
    fn test_replica_heartbeat() {
        let mut replica = ReplicaInfo::new("replica1", 1);
        replica.connection_state = ReplicaConnectionState::Streaming;

        assert!(!replica.is_healthy(Duration::from_secs(1)));

        replica.heartbeat();
        assert!(replica.is_healthy(Duration::from_secs(1)));
    }

    #[test]
    fn test_sync_standby_names() {
        let names = SyncStandbyNames::First(2, vec!["r1".to_string(), "r2".to_string(), "r3".to_string()]);

        assert_eq!(names.required_acks(), 2);
        assert!(names.contains("r1"));
        assert!(!names.contains("r4"));

        let names = SyncStandbyNames::Any(1, vec!["r1".to_string()]);
        assert_eq!(names.required_acks(), 1);

        let names = SyncStandbyNames::All(vec!["r1".to_string(), "r2".to_string()]);
        assert_eq!(names.required_acks(), 2);
    }

    #[test]
    fn test_sync_replication_manager() {
        let manager = SyncReplicationManager::new();

        // Set commit level
        manager.set_commit_level(SynchronousCommit::RemoteFlush);
        assert_eq!(manager.commit_level(), SynchronousCommit::RemoteFlush);

        // Register replicas
        manager.register_replica("replica1", 1);
        manager.register_replica("replica2", 2);

        let replicas = manager.list_replicas();
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn test_replica_progress_update() {
        let manager = SyncReplicationManager::new();

        manager.register_replica("replica1", 1);
        manager.update_replica_state("replica1", ReplicaConnectionState::Streaming);
        manager.update_replica_progress("replica1", 100, 90, 80);

        let replica = manager.get_replica("replica1").unwrap();
        assert_eq!(replica.write_lsn, 100);
        assert_eq!(replica.flush_lsn, 90);
        assert_eq!(replica.apply_lsn, 80);
    }

    #[test]
    fn test_sync_state_assignment() {
        let manager = SyncReplicationManager::new();

        // Configure FIRST 1 standby
        manager.set_standby_config(SyncStandbyConfig {
            names: SyncStandbyNames::First(
                1,
                vec!["replica1".to_string(), "replica2".to_string()],
            ),
            timeout: Duration::from_secs(30),
            wait_for_all: false,
        });

        // Register and connect replicas
        manager.register_replica("replica1", 1);
        manager.register_replica("replica2", 2);
        manager.update_replica_state("replica1", ReplicaConnectionState::Streaming);
        manager.update_replica_state("replica2", ReplicaConnectionState::Streaming);

        let r1 = manager.get_replica("replica1").unwrap();
        let r2 = manager.get_replica("replica2").unwrap();

        // replica1 should be sync (lower priority)
        assert_eq!(r1.sync_state, SyncState::Sync);
        // replica2 should be potential
        assert_eq!(r2.sync_state, SyncState::Potential);
    }

    #[test]
    fn test_wait_for_sync_off() {
        let manager = SyncReplicationManager::new();
        manager.set_commit_level(SynchronousCommit::Off);

        let result = manager.wait_for_sync(1, 100);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Duration::ZERO);
    }

    #[test]
    fn test_wait_for_sync_local() {
        let manager = SyncReplicationManager::new();
        manager.set_commit_level(SynchronousCommit::Local);

        let result = manager.wait_for_sync(1, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_not_enough_replicas() {
        let manager = SyncReplicationManager::new();
        manager.set_commit_level(SynchronousCommit::RemoteFlush);
        manager.set_standby_config(SyncStandbyConfig {
            names: SyncStandbyNames::First(2, vec!["r1".to_string(), "r2".to_string()]),
            timeout: Duration::from_millis(100),
            wait_for_all: false,
        });

        // No replicas registered
        let result = manager.wait_for_sync(1, 100);
        assert!(matches!(
            result,
            Err(SyncReplicationError::NotEnoughReplicas { .. })
        ));
    }

    #[test]
    fn test_enable_disable() {
        let manager = SyncReplicationManager::new();

        assert!(manager.is_enabled());
        manager.disable();
        assert!(!manager.is_enabled());
        manager.enable();
        assert!(manager.is_enabled());
    }

    #[test]
    fn test_statistics() {
        let manager = SyncReplicationManager::new();
        manager.set_commit_level(SynchronousCommit::Off);

        manager.wait_for_sync(1, 100).unwrap();
        manager.wait_for_sync(2, 200).unwrap();

        let stats = manager.stats();
        // For Off level, commits don't go through the waiting path
        assert_eq!(stats.commits_waiting, 0);
    }

    #[test]
    fn test_pending_commit() {
        let mut commit = PendingCommit::new(1, 100, SynchronousCommit::RemoteFlush, 2);

        assert!(!commit.is_satisfied());

        commit.add_ack("replica1");
        assert!(!commit.is_satisfied());

        commit.add_ack("replica2");
        assert!(commit.is_satisfied());
    }

    #[test]
    fn test_error_display() {
        let err = SyncReplicationError::Timeout;
        assert!(format!("{}", err).contains("Timeout"));

        let err = SyncReplicationError::NotEnoughReplicas {
            required: 3,
            available: 1,
        };
        assert!(format!("{}", err).contains("3"));
        assert!(format!("{}", err).contains("1"));
    }

    #[test]
    fn test_unregister_replica() {
        let manager = SyncReplicationManager::new();

        manager.register_replica("replica1", 1);
        assert!(manager.get_replica("replica1").is_some());

        manager.unregister_replica("replica1");
        assert!(manager.get_replica("replica1").is_none());
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = SyncReplicationStatsSnapshot {
            commits_waiting: 0,
            commits_completed: 100,
            commits_timed_out: 0,
            total_wait_time_ns: 1_000_000_000, // 1 second
            acks_received: 100,
            sync_replicas: 2,
        };

        let avg = stats.average_wait_time();
        assert_eq!(avg.as_millis(), 10);
        assert!((stats.success_rate() - 1.0).abs() < 0.01);
    }
}
