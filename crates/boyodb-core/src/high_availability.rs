// Phase 15: High Availability
//
// Automatic failover, read replicas, quorum writes, and health monitoring
// for production-grade fault tolerance.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

// ============================================================================
// Replica Configuration
// ============================================================================

/// Replica role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaRole {
    Leader,
    Follower,
    Candidate,
    Observer,  // Read-only, doesn't participate in elections
}

/// Replica state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    Healthy,
    Degraded,
    Syncing,
    Offline,
    Draining,
}

/// Replica information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub replica_id: String,
    pub address: String,
    pub role: ReplicaRole,
    pub state: ReplicaState,
    pub last_heartbeat: u64,
    pub lag_bytes: u64,
    pub lag_seconds: f64,
    pub term: u64,
    pub commit_index: u64,
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub priority: i32,  // Higher = more likely to become leader
}

impl ReplicaInfo {
    pub fn new(replica_id: String, address: String) -> Self {
        Self {
            replica_id,
            address,
            role: ReplicaRole::Follower,
            state: ReplicaState::Healthy,
            last_heartbeat: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            lag_bytes: 0,
            lag_seconds: 0.0,
            term: 0,
            commit_index: 0,
            zone: None,
            rack: None,
            priority: 0,
        }
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.state, ReplicaState::Healthy)
    }

    pub fn is_available(&self) -> bool {
        matches!(self.state, ReplicaState::Healthy | ReplicaState::Degraded)
    }
}

/// High availability configuration
#[derive(Debug, Clone)]
pub struct HaConfig {
    /// Minimum replicas for quorum
    pub min_replicas: usize,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Election timeout range
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Maximum lag before replica is considered unhealthy
    pub max_lag_bytes: u64,
    pub max_lag_seconds: f64,
    /// Automatic failover enabled
    pub auto_failover: bool,
    /// Read from followers allowed
    pub read_from_followers: bool,
    /// Sync replication required
    pub sync_replication: bool,
    /// Number of sync replicas required
    pub sync_replicas: usize,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            min_replicas: 2,
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            max_lag_bytes: 100 * 1024 * 1024, // 100MB
            max_lag_seconds: 10.0,
            auto_failover: true,
            read_from_followers: true,
            sync_replication: false,
            sync_replicas: 1,
        }
    }
}

// ============================================================================
// Leader Election (Raft-style)
// ============================================================================

/// Election state
#[derive(Debug, Clone)]
pub struct ElectionState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub vote_count: usize,
    pub votes_received: HashSet<String>,
    pub election_start: Option<Instant>,
    pub election_timeout: Duration,
}

impl Default for ElectionState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            vote_count: 0,
            votes_received: HashSet::new(),
            election_start: None,
            election_timeout: Duration::from_millis(2000),
        }
    }
}

/// Vote request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Vote response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub voter_id: String,
}

/// Append entries request (heartbeat/replication)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// Append entries response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
    pub follower_id: String,
}

/// Log entry for replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub entry_type: LogEntryType,
}

/// Log entry types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEntryType {
    Data,
    Configuration,
    NoOp,
}

/// Leader election manager
pub struct LeaderElection {
    node_id: String,
    config: HaConfig,
    state: RwLock<ElectionState>,
    role: RwLock<ReplicaRole>,
    leader_id: RwLock<Option<String>>,
    replicas: RwLock<HashMap<String, ReplicaInfo>>,
    log: RwLock<Vec<LogEntry>>,
    commit_index: AtomicU64,
    last_applied: AtomicU64,
    last_heartbeat: RwLock<Instant>,
}

impl LeaderElection {
    pub fn new(node_id: String, config: HaConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(ElectionState::default()),
            role: RwLock::new(ReplicaRole::Follower),
            leader_id: RwLock::new(None),
            replicas: RwLock::new(HashMap::new()),
            log: RwLock::new(Vec::new()),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
            last_heartbeat: RwLock::new(Instant::now()),
        }
    }

    /// Get current role
    pub fn role(&self) -> ReplicaRole {
        *self.role.read()
    }

    /// Get current leader
    pub fn leader(&self) -> Option<String> {
        self.leader_id.read().clone()
    }

    /// Get current term
    pub fn term(&self) -> u64 {
        self.state.read().current_term
    }

    /// Check if we are leader
    pub fn is_leader(&self) -> bool {
        *self.role.read() == ReplicaRole::Leader
    }

    /// Add a replica
    pub fn add_replica(&self, replica: ReplicaInfo) {
        self.replicas.write().insert(replica.replica_id.clone(), replica);
    }

    /// Remove a replica
    pub fn remove_replica(&self, replica_id: &str) {
        self.replicas.write().remove(replica_id);
    }

    /// Start election
    pub fn start_election(&self) -> Vec<VoteRequest> {
        let mut state = self.state.write();
        let mut role = self.role.write();

        state.current_term += 1;
        state.voted_for = Some(self.node_id.clone());
        state.vote_count = 1;  // Vote for self
        state.votes_received.clear();
        state.votes_received.insert(self.node_id.clone());
        state.election_start = Some(Instant::now());

        *role = ReplicaRole::Candidate;

        let log = self.log.read();
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);

        let replicas = self.replicas.read();
        replicas.keys()
            .filter(|id| *id != &self.node_id)
            .map(|_| VoteRequest {
                term: state.current_term,
                candidate_id: self.node_id.clone(),
                last_log_index,
                last_log_term,
            })
            .collect()
    }

    /// Handle vote request
    pub fn handle_vote_request(&self, request: &VoteRequest) -> VoteResponse {
        let mut state = self.state.write();

        // Update term if needed
        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            *self.role.write() = ReplicaRole::Follower;
        }

        let log = self.log.read();
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);

        // Check if we can grant vote
        let log_ok = request.last_log_term > last_log_term ||
            (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);

        let vote_granted = request.term >= state.current_term &&
            (state.voted_for.is_none() || state.voted_for.as_ref() == Some(&request.candidate_id)) &&
            log_ok;

        if vote_granted {
            state.voted_for = Some(request.candidate_id.clone());
            *self.last_heartbeat.write() = Instant::now();
        }

        VoteResponse {
            term: state.current_term,
            vote_granted,
            voter_id: self.node_id.clone(),
        }
    }

    /// Handle vote response
    pub fn handle_vote_response(&self, response: &VoteResponse) -> bool {
        let mut state = self.state.write();

        if response.term > state.current_term {
            state.current_term = response.term;
            state.voted_for = None;
            *self.role.write() = ReplicaRole::Follower;
            return false;
        }

        if *self.role.read() != ReplicaRole::Candidate {
            return false;
        }

        if response.vote_granted && !state.votes_received.contains(&response.voter_id) {
            state.votes_received.insert(response.voter_id.clone());
            state.vote_count += 1;
        }

        let replicas = self.replicas.read();
        let quorum = (replicas.len() + 1) / 2 + 1;

        if state.vote_count >= quorum {
            *self.role.write() = ReplicaRole::Leader;
            *self.leader_id.write() = Some(self.node_id.clone());
            return true;
        }

        false
    }

    /// Handle append entries (heartbeat from leader)
    pub fn handle_append_entries(&self, request: &AppendEntriesRequest) -> AppendEntriesResponse {
        let mut state = self.state.write();
        *self.last_heartbeat.write() = Instant::now();

        // Update term if needed
        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            *self.role.write() = ReplicaRole::Follower;
        }

        if request.term < state.current_term {
            return AppendEntriesResponse {
                term: state.current_term,
                success: false,
                match_index: 0,
                follower_id: self.node_id.clone(),
            };
        }

        *self.leader_id.write() = Some(request.leader_id.clone());
        *self.role.write() = ReplicaRole::Follower;

        let mut log = self.log.write();

        // Check log consistency
        if request.prev_log_index > 0 {
            if log.len() < request.prev_log_index as usize {
                return AppendEntriesResponse {
                    term: state.current_term,
                    success: false,
                    match_index: log.len() as u64,
                    follower_id: self.node_id.clone(),
                };
            }

            if let Some(entry) = log.get(request.prev_log_index as usize - 1) {
                if entry.term != request.prev_log_term {
                    // Truncate conflicting entries
                    log.truncate(request.prev_log_index as usize - 1);
                    return AppendEntriesResponse {
                        term: state.current_term,
                        success: false,
                        match_index: log.len() as u64,
                        follower_id: self.node_id.clone(),
                    };
                }
            }
        }

        // Append new entries
        for entry in &request.entries {
            if entry.index as usize > log.len() {
                log.push(entry.clone());
            }
        }

        // Update commit index
        if request.leader_commit > self.commit_index.load(Ordering::SeqCst) {
            let new_commit = request.leader_commit.min(log.len() as u64);
            self.commit_index.store(new_commit, Ordering::SeqCst);
        }

        AppendEntriesResponse {
            term: state.current_term,
            success: true,
            match_index: log.len() as u64,
            follower_id: self.node_id.clone(),
        }
    }

    /// Check if election timeout expired
    pub fn election_timeout_expired(&self) -> bool {
        if *self.role.read() == ReplicaRole::Leader {
            return false;
        }

        let last = *self.last_heartbeat.read();
        let state = self.state.read();
        last.elapsed() > state.election_timeout
    }

    /// Append entry to log (leader only)
    pub fn append(&self, data: Vec<u8>) -> Option<u64> {
        if !self.is_leader() {
            return None;
        }

        let mut log = self.log.write();
        let state = self.state.read();
        let index = log.len() as u64 + 1;

        log.push(LogEntry {
            index,
            term: state.current_term,
            data,
            entry_type: LogEntryType::Data,
        });

        Some(index)
    }
}

// ============================================================================
// Read Replicas
// ============================================================================

/// Read preference for replica selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPreference {
    /// Always read from leader
    Primary,
    /// Prefer leader but allow followers
    PrimaryPreferred,
    /// Always read from followers
    Secondary,
    /// Prefer followers but allow leader
    SecondaryPreferred,
    /// Read from nearest replica
    Nearest,
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    Random,
    LeastConnections,
    WeightedRoundRobin,
    LeastLatency,
}

/// Replica selector for read routing
pub struct ReplicaSelector {
    replicas: RwLock<Vec<ReplicaInfo>>,
    strategy: LoadBalanceStrategy,
    round_robin_index: AtomicU64,
    connections: RwLock<HashMap<String, u64>>,
    latencies: RwLock<HashMap<String, f64>>,
    local_zone: Option<String>,
}

impl ReplicaSelector {
    pub fn new(strategy: LoadBalanceStrategy) -> Self {
        Self {
            replicas: RwLock::new(Vec::new()),
            strategy,
            round_robin_index: AtomicU64::new(0),
            connections: RwLock::new(HashMap::new()),
            latencies: RwLock::new(HashMap::new()),
            local_zone: None,
        }
    }

    pub fn with_local_zone(mut self, zone: String) -> Self {
        self.local_zone = Some(zone);
        self
    }

    pub fn update_replicas(&self, replicas: Vec<ReplicaInfo>) {
        *self.replicas.write() = replicas;
    }

    pub fn update_connection_count(&self, replica_id: &str, count: u64) {
        self.connections.write().insert(replica_id.to_string(), count);
    }

    pub fn update_latency(&self, replica_id: &str, latency_ms: f64) {
        self.latencies.write().insert(replica_id.to_string(), latency_ms);
    }

    /// Select replica based on read preference
    pub fn select(&self, preference: ReadPreference) -> Option<ReplicaInfo> {
        let replicas = self.replicas.read();

        let candidates: Vec<_> = match preference {
            ReadPreference::Primary => {
                replicas.iter()
                    .filter(|r| r.role == ReplicaRole::Leader && r.is_available())
                    .cloned()
                    .collect()
            }
            ReadPreference::PrimaryPreferred => {
                let leaders: Vec<_> = replicas.iter()
                    .filter(|r| r.role == ReplicaRole::Leader && r.is_available())
                    .cloned()
                    .collect();
                if !leaders.is_empty() {
                    leaders
                } else {
                    replicas.iter()
                        .filter(|r| r.is_available())
                        .cloned()
                        .collect()
                }
            }
            ReadPreference::Secondary => {
                replicas.iter()
                    .filter(|r| r.role == ReplicaRole::Follower && r.is_available())
                    .cloned()
                    .collect()
            }
            ReadPreference::SecondaryPreferred => {
                let followers: Vec<_> = replicas.iter()
                    .filter(|r| r.role == ReplicaRole::Follower && r.is_available())
                    .cloned()
                    .collect();
                if !followers.is_empty() {
                    followers
                } else {
                    replicas.iter()
                        .filter(|r| r.is_available())
                        .cloned()
                        .collect()
                }
            }
            ReadPreference::Nearest => {
                replicas.iter()
                    .filter(|r| r.is_available())
                    .cloned()
                    .collect()
            }
        };

        if candidates.is_empty() {
            return None;
        }

        self.select_from_candidates(&candidates)
    }

    fn select_from_candidates(&self, candidates: &[ReplicaInfo]) -> Option<ReplicaInfo> {
        if candidates.is_empty() {
            return None;
        }

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                let index = self.round_robin_index.fetch_add(1, Ordering::SeqCst) as usize;
                Some(candidates[index % candidates.len()].clone())
            }
            LoadBalanceStrategy::Random => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                std::time::Instant::now().hash(&mut hasher);
                let index = hasher.finish() as usize % candidates.len();
                Some(candidates[index].clone())
            }
            LoadBalanceStrategy::LeastConnections => {
                let conns = self.connections.read();
                candidates.iter()
                    .min_by_key(|r| conns.get(&r.replica_id).copied().unwrap_or(0))
                    .cloned()
            }
            LoadBalanceStrategy::WeightedRoundRobin => {
                // Use priority as weight
                let total_weight: i32 = candidates.iter()
                    .map(|r| r.priority.max(1))
                    .sum();

                let index = self.round_robin_index.fetch_add(1, Ordering::SeqCst) as i32;
                let target = index % total_weight;

                let mut cumulative = 0;
                for replica in candidates {
                    cumulative += replica.priority.max(1);
                    if cumulative > target {
                        return Some(replica.clone());
                    }
                }
                candidates.first().cloned()
            }
            LoadBalanceStrategy::LeastLatency => {
                let latencies = self.latencies.read();
                candidates.iter()
                    .min_by(|a, b| {
                        let lat_a = latencies.get(&a.replica_id).copied().unwrap_or(f64::MAX);
                        let lat_b = latencies.get(&b.replica_id).copied().unwrap_or(f64::MAX);
                        lat_a.partial_cmp(&lat_b).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .cloned()
            }
        }
    }

    /// Select replica in same zone if available
    pub fn select_zone_aware(&self, preference: ReadPreference) -> Option<ReplicaInfo> {
        let replicas = self.replicas.read();

        if let Some(ref local_zone) = self.local_zone {
            // First try to find in local zone
            let local_candidates: Vec<_> = replicas.iter()
                .filter(|r| r.zone.as_ref() == Some(local_zone) && r.is_available())
                .filter(|r| match preference {
                    ReadPreference::Primary => r.role == ReplicaRole::Leader,
                    ReadPreference::Secondary => r.role == ReplicaRole::Follower,
                    _ => true,
                })
                .cloned()
                .collect();

            if !local_candidates.is_empty() {
                return self.select_from_candidates(&local_candidates);
            }
        }

        drop(replicas);
        self.select(preference)
    }
}

// ============================================================================
// Quorum Writes
// ============================================================================

/// Write consistency level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConsistency {
    /// Write acknowledged by one replica
    One,
    /// Write acknowledged by quorum of replicas
    Quorum,
    /// Write acknowledged by all replicas
    All,
    /// Write acknowledged by local datacenter quorum
    LocalQuorum,
    /// Write acknowledged by each datacenter quorum
    EachQuorum,
}

/// Write acknowledgment from replica
#[derive(Debug, Clone)]
pub struct WriteAck {
    pub replica_id: String,
    pub success: bool,
    pub commit_index: u64,
    pub latency_ms: f64,
    pub error: Option<String>,
}

/// Pending write tracking
#[derive(Debug)]
pub struct PendingWrite {
    pub write_id: u64,
    pub data: Vec<u8>,
    pub required_acks: usize,
    pub received_acks: Vec<WriteAck>,
    pub start_time: Instant,
    pub timeout: Duration,
    pub consistency: WriteConsistency,
}

impl PendingWrite {
    pub fn new(
        write_id: u64,
        data: Vec<u8>,
        consistency: WriteConsistency,
        replica_count: usize,
        timeout: Duration,
    ) -> Self {
        let required_acks = match consistency {
            WriteConsistency::One => 1,
            WriteConsistency::Quorum => replica_count / 2 + 1,
            WriteConsistency::All => replica_count,
            WriteConsistency::LocalQuorum => replica_count / 2 + 1,
            WriteConsistency::EachQuorum => replica_count / 2 + 1,
        };

        Self {
            write_id,
            data,
            required_acks,
            received_acks: Vec::new(),
            start_time: Instant::now(),
            timeout,
            consistency,
        }
    }

    pub fn add_ack(&mut self, ack: WriteAck) {
        self.received_acks.push(ack);
    }

    pub fn is_complete(&self) -> bool {
        let success_count = self.received_acks.iter()
            .filter(|a| a.success)
            .count();
        success_count >= self.required_acks
    }

    pub fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    pub fn success_count(&self) -> usize {
        self.received_acks.iter().filter(|a| a.success).count()
    }
}

/// Quorum write manager
pub struct QuorumWriter {
    config: HaConfig,
    replicas: RwLock<Vec<ReplicaInfo>>,
    pending_writes: RwLock<HashMap<u64, PendingWrite>>,
    next_write_id: AtomicU64,
    default_timeout: Duration,
}

impl QuorumWriter {
    pub fn new(config: HaConfig) -> Self {
        Self {
            config,
            replicas: RwLock::new(Vec::new()),
            pending_writes: RwLock::new(HashMap::new()),
            next_write_id: AtomicU64::new(1),
            default_timeout: Duration::from_secs(30),
        }
    }

    pub fn update_replicas(&self, replicas: Vec<ReplicaInfo>) {
        *self.replicas.write() = replicas;
    }

    /// Start a write with consistency requirements
    pub fn start_write(&self, data: Vec<u8>, consistency: WriteConsistency) -> u64 {
        let write_id = self.next_write_id.fetch_add(1, Ordering::SeqCst);
        let replicas = self.replicas.read();
        let replica_count = replicas.len();

        let pending = PendingWrite::new(
            write_id,
            data,
            consistency,
            replica_count,
            self.default_timeout,
        );

        self.pending_writes.write().insert(write_id, pending);
        write_id
    }

    /// Record acknowledgment from replica
    pub fn record_ack(&self, write_id: u64, ack: WriteAck) -> Option<bool> {
        let mut pending = self.pending_writes.write();

        if let Some(write) = pending.get_mut(&write_id) {
            write.add_ack(ack);

            if write.is_complete() {
                pending.remove(&write_id);
                return Some(true);
            }

            if write.is_timed_out() {
                pending.remove(&write_id);
                return Some(false);
            }

            return Some(false);
        }

        None
    }

    /// Get replicas to write to
    pub fn get_write_targets(&self, consistency: WriteConsistency) -> Vec<ReplicaInfo> {
        let replicas = self.replicas.read();

        match consistency {
            WriteConsistency::One => {
                replicas.iter()
                    .filter(|r| r.is_available())
                    .take(1)
                    .cloned()
                    .collect()
            }
            WriteConsistency::Quorum | WriteConsistency::LocalQuorum => {
                let quorum_size = replicas.len() / 2 + 1;
                replicas.iter()
                    .filter(|r| r.is_available())
                    .take(quorum_size)
                    .cloned()
                    .collect()
            }
            WriteConsistency::All | WriteConsistency::EachQuorum => {
                replicas.iter()
                    .filter(|r| r.is_available())
                    .cloned()
                    .collect()
            }
        }
    }

    /// Calculate required acks for consistency level
    pub fn required_acks(&self, consistency: WriteConsistency) -> usize {
        let replicas = self.replicas.read();
        let n = replicas.len();

        match consistency {
            WriteConsistency::One => 1,
            WriteConsistency::Quorum | WriteConsistency::LocalQuorum => n / 2 + 1,
            WriteConsistency::All | WriteConsistency::EachQuorum => n,
        }
    }

    /// Clean up timed out writes
    pub fn cleanup_timeouts(&self) -> Vec<u64> {
        let mut pending = self.pending_writes.write();
        let timed_out: Vec<u64> = pending.iter()
            .filter(|(_, w)| w.is_timed_out())
            .map(|(id, _)| *id)
            .collect();

        for id in &timed_out {
            pending.remove(id);
        }

        timed_out
    }
}

// ============================================================================
// Health Monitoring
// ============================================================================

/// Health check result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    pub replica_id: String,
    pub healthy: bool,
    pub latency_ms: f64,
    pub last_check: Instant,
    pub consecutive_failures: u32,
    pub details: HashMap<String, String>,
}

/// Health monitor for replicas
pub struct HealthMonitor {
    config: HaConfig,
    results: RwLock<HashMap<String, HealthCheckResult>>,
    failure_threshold: u32,
    recovery_threshold: u32,
}

impl HealthMonitor {
    pub fn new(config: HaConfig) -> Self {
        Self {
            config,
            results: RwLock::new(HashMap::new()),
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }

    /// Record health check result
    pub fn record_check(&self, replica_id: &str, healthy: bool, latency_ms: f64) -> ReplicaState {
        let mut results = self.results.write();

        let result = results.entry(replica_id.to_string()).or_insert_with(|| {
            HealthCheckResult {
                replica_id: replica_id.to_string(),
                healthy: true,
                latency_ms: 0.0,
                last_check: Instant::now(),
                consecutive_failures: 0,
                details: HashMap::new(),
            }
        });

        result.last_check = Instant::now();
        result.latency_ms = latency_ms;

        if healthy {
            result.consecutive_failures = 0;
            result.healthy = true;
        } else {
            result.consecutive_failures += 1;
            if result.consecutive_failures >= self.failure_threshold {
                result.healthy = false;
            }
        }

        // Determine state
        if !result.healthy {
            ReplicaState::Offline
        } else if result.consecutive_failures > 0 {
            ReplicaState::Degraded
        } else if latency_ms > self.config.max_lag_seconds * 1000.0 {
            ReplicaState::Degraded
        } else {
            ReplicaState::Healthy
        }
    }

    /// Get replica state
    pub fn get_state(&self, replica_id: &str) -> ReplicaState {
        let results = self.results.read();

        if let Some(result) = results.get(replica_id) {
            if !result.healthy {
                ReplicaState::Offline
            } else if result.consecutive_failures > 0 {
                ReplicaState::Degraded
            } else {
                ReplicaState::Healthy
            }
        } else {
            ReplicaState::Offline
        }
    }

    /// Get all unhealthy replicas
    pub fn get_unhealthy(&self) -> Vec<String> {
        let results = self.results.read();
        results.iter()
            .filter(|(_, r)| !r.healthy)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if replica should be removed from cluster
    pub fn should_remove(&self, replica_id: &str) -> bool {
        let results = self.results.read();
        if let Some(result) = results.get(replica_id) {
            result.consecutive_failures >= self.failure_threshold * 2
        } else {
            false
        }
    }
}

// ============================================================================
// Failover Manager
// ============================================================================

/// Failover event
#[derive(Debug, Clone)]
pub struct FailoverEvent {
    pub timestamp: u64,
    pub event_type: FailoverEventType,
    pub old_leader: Option<String>,
    pub new_leader: Option<String>,
    pub reason: String,
}

/// Failover event types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverEventType {
    LeaderElected,
    LeaderFailed,
    ManualFailover,
    SplitBrainDetected,
    QuorumLost,
    QuorumRestored,
}

/// Automatic failover manager
pub struct FailoverManager {
    config: HaConfig,
    election: Arc<LeaderElection>,
    health_monitor: Arc<HealthMonitor>,
    events: RwLock<VecDeque<FailoverEvent>>,
    failover_in_progress: AtomicBool,
    last_failover: RwLock<Option<Instant>>,
    min_failover_interval: Duration,
}

impl FailoverManager {
    pub fn new(
        config: HaConfig,
        election: Arc<LeaderElection>,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            config,
            election,
            health_monitor,
            events: RwLock::new(VecDeque::with_capacity(100)),
            failover_in_progress: AtomicBool::new(false),
            last_failover: RwLock::new(None),
            min_failover_interval: Duration::from_secs(30),
        }
    }

    /// Check if failover is needed
    pub fn check_failover_needed(&self) -> bool {
        if !self.config.auto_failover {
            return false;
        }

        if self.failover_in_progress.load(Ordering::SeqCst) {
            return false;
        }

        // Check minimum interval
        if let Some(last) = *self.last_failover.read() {
            if last.elapsed() < self.min_failover_interval {
                return false;
            }
        }

        // Check if leader is healthy
        if let Some(leader_id) = self.election.leader() {
            let state = self.health_monitor.get_state(&leader_id);
            if state == ReplicaState::Offline {
                return true;
            }
        }

        // Check election timeout
        self.election.election_timeout_expired()
    }

    /// Trigger failover
    pub fn trigger_failover(&self, reason: &str) -> Result<(), HaError> {
        if self.failover_in_progress.swap(true, Ordering::SeqCst) {
            return Err(HaError::FailoverInProgress);
        }

        let old_leader = self.election.leader();

        // Record event
        self.record_event(FailoverEvent {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            event_type: FailoverEventType::LeaderFailed,
            old_leader: old_leader.clone(),
            new_leader: None,
            reason: reason.to_string(),
        });

        // Start election
        let _vote_requests = self.election.start_election();

        *self.last_failover.write() = Some(Instant::now());
        self.failover_in_progress.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Manual failover to specific replica
    pub fn manual_failover(&self, target_replica: &str) -> Result<(), HaError> {
        if self.failover_in_progress.swap(true, Ordering::SeqCst) {
            return Err(HaError::FailoverInProgress);
        }

        let old_leader = self.election.leader();

        self.record_event(FailoverEvent {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            event_type: FailoverEventType::ManualFailover,
            old_leader,
            new_leader: Some(target_replica.to_string()),
            reason: "Manual failover requested".to_string(),
        });

        self.failover_in_progress.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn record_event(&self, event: FailoverEvent) {
        let mut events = self.events.write();
        events.push_back(event);
        while events.len() > 100 {
            events.pop_front();
        }
    }

    /// Get recent failover events
    pub fn get_events(&self, limit: usize) -> Vec<FailoverEvent> {
        let events = self.events.read();
        events.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Check for split brain
    pub fn detect_split_brain(&self) -> bool {
        // In a real implementation, would check for multiple leaders
        false
    }
}

// ============================================================================
// High Availability Manager
// ============================================================================

/// High availability manager combining all components
pub struct HaManager {
    pub config: HaConfig,
    pub election: Arc<LeaderElection>,
    pub selector: Arc<ReplicaSelector>,
    pub quorum_writer: Arc<QuorumWriter>,
    pub health_monitor: Arc<HealthMonitor>,
    pub failover_manager: Arc<FailoverManager>,
}

impl HaManager {
    pub fn new(node_id: String, config: HaConfig) -> Self {
        let election = Arc::new(LeaderElection::new(node_id, config.clone()));
        let health_monitor = Arc::new(HealthMonitor::new(config.clone()));
        let failover_manager = Arc::new(FailoverManager::new(
            config.clone(),
            Arc::clone(&election),
            Arc::clone(&health_monitor),
        ));

        let strategy = if config.read_from_followers {
            LoadBalanceStrategy::LeastLatency
        } else {
            LoadBalanceStrategy::RoundRobin
        };

        Self {
            config: config.clone(),
            election,
            selector: Arc::new(ReplicaSelector::new(strategy)),
            quorum_writer: Arc::new(QuorumWriter::new(config)),
            health_monitor,
            failover_manager,
        }
    }

    /// Add replica to cluster
    pub fn add_replica(&self, replica: ReplicaInfo) {
        self.election.add_replica(replica.clone());
        let mut replicas = vec![replica];
        // Would merge with existing
        self.selector.update_replicas(replicas.clone());
        self.quorum_writer.update_replicas(replicas);
    }

    /// Remove replica from cluster
    pub fn remove_replica(&self, replica_id: &str) {
        self.election.remove_replica(replica_id);
    }

    /// Get replica for read
    pub fn get_read_replica(&self, preference: ReadPreference) -> Option<ReplicaInfo> {
        if self.config.read_from_followers {
            self.selector.select(preference)
        } else {
            self.selector.select(ReadPreference::Primary)
        }
    }

    /// Write with consistency
    pub fn write(&self, data: Vec<u8>, consistency: WriteConsistency) -> Result<u64, HaError> {
        if !self.election.is_leader() {
            return Err(HaError::NotLeader);
        }

        let write_id = self.quorum_writer.start_write(data, consistency);
        Ok(write_id)
    }

    /// Run periodic maintenance
    pub fn tick(&self) {
        // Check for failover
        if self.failover_manager.check_failover_needed() {
            let _ = self.failover_manager.trigger_failover("Leader health check failed");
        }

        // Cleanup timed out writes
        self.quorum_writer.cleanup_timeouts();
    }

    /// Get cluster status
    pub fn status(&self) -> ClusterHaStatus {
        ClusterHaStatus {
            leader: self.election.leader(),
            term: self.election.term(),
            role: self.election.role(),
            healthy_replicas: 0,  // Would count from health monitor
            total_replicas: 0,
            quorum_available: true,
        }
    }
}

/// Cluster HA status
#[derive(Debug, Clone)]
pub struct ClusterHaStatus {
    pub leader: Option<String>,
    pub term: u64,
    pub role: ReplicaRole,
    pub healthy_replicas: usize,
    pub total_replicas: usize,
    pub quorum_available: bool,
}

// ============================================================================
// Errors
// ============================================================================

/// High availability errors
#[derive(Debug, Clone)]
pub enum HaError {
    NotLeader,
    NoQuorum,
    FailoverInProgress,
    ReplicaNotFound(String),
    Timeout,
    NetworkError(String),
    ConsistencyError(String),
}

impl std::fmt::Display for HaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader => write!(f, "Not the leader"),
            Self::NoQuorum => write!(f, "Quorum not available"),
            Self::FailoverInProgress => write!(f, "Failover in progress"),
            Self::ReplicaNotFound(id) => write!(f, "Replica not found: {}", id),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::NetworkError(msg) => write!(f, "Network error: {}", msg),
            Self::ConsistencyError(msg) => write!(f, "Consistency error: {}", msg),
        }
    }
}

impl std::error::Error for HaError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_info() {
        let replica = ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string());
        assert_eq!(replica.role, ReplicaRole::Follower);
        assert!(replica.is_healthy());
        assert!(replica.is_available());
    }

    #[test]
    fn test_ha_config_default() {
        let config = HaConfig::default();
        assert_eq!(config.min_replicas, 2);
        assert!(config.auto_failover);
        assert!(config.read_from_followers);
    }

    #[test]
    fn test_leader_election_start() {
        let config = HaConfig::default();
        let election = LeaderElection::new("node1".to_string(), config);

        // Add some replicas
        election.add_replica(ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()));
        election.add_replica(ReplicaInfo::new("node3".to_string(), "127.0.0.1:8082".to_string()));

        let requests = election.start_election();
        assert_eq!(requests.len(), 2);
        assert_eq!(election.role(), ReplicaRole::Candidate);
        assert_eq!(election.term(), 1);
    }

    #[test]
    fn test_vote_request_handling() {
        let config = HaConfig::default();
        let election = LeaderElection::new("node1".to_string(), config);

        let request = VoteRequest {
            term: 1,
            candidate_id: "node2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = election.handle_vote_request(&request);
        assert!(response.vote_granted);
        assert_eq!(response.term, 1);
    }

    #[test]
    fn test_vote_response_quorum() {
        let config = HaConfig::default();
        let election = LeaderElection::new("node1".to_string(), config);

        election.add_replica(ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()));
        election.add_replica(ReplicaInfo::new("node3".to_string(), "127.0.0.1:8082".to_string()));

        election.start_election();

        // Receive vote from node2
        let response1 = VoteResponse {
            term: 1,
            vote_granted: true,
            voter_id: "node2".to_string(),
        };
        let became_leader = election.handle_vote_response(&response1);
        assert!(became_leader);
        assert!(election.is_leader());
    }

    #[test]
    fn test_append_entries() {
        let config = HaConfig::default();
        let election = LeaderElection::new("node1".to_string(), config);

        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = election.handle_append_entries(&request);
        assert!(response.success);
        assert_eq!(election.leader(), Some("leader".to_string()));
    }

    #[test]
    fn test_replica_selector_round_robin() {
        let selector = ReplicaSelector::new(LoadBalanceStrategy::RoundRobin);

        let replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
            ReplicaInfo::new("node3".to_string(), "127.0.0.1:8082".to_string()),
        ];
        selector.update_replicas(replicas);

        let r1 = selector.select(ReadPreference::Secondary).unwrap();
        let r2 = selector.select(ReadPreference::Secondary).unwrap();
        let r3 = selector.select(ReadPreference::Secondary).unwrap();

        // Should cycle through replicas
        assert_ne!(r1.replica_id, r2.replica_id);
    }

    #[test]
    fn test_replica_selector_primary() {
        let selector = ReplicaSelector::new(LoadBalanceStrategy::RoundRobin);

        let mut replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
        ];
        replicas[0].role = ReplicaRole::Leader;

        selector.update_replicas(replicas);

        let selected = selector.select(ReadPreference::Primary).unwrap();
        assert_eq!(selected.role, ReplicaRole::Leader);
    }

    #[test]
    fn test_write_consistency_quorum() {
        let config = HaConfig::default();
        let writer = QuorumWriter::new(config);

        let replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
            ReplicaInfo::new("node3".to_string(), "127.0.0.1:8082".to_string()),
        ];
        writer.update_replicas(replicas);

        assert_eq!(writer.required_acks(WriteConsistency::One), 1);
        assert_eq!(writer.required_acks(WriteConsistency::Quorum), 2);
        assert_eq!(writer.required_acks(WriteConsistency::All), 3);
    }

    #[test]
    fn test_pending_write() {
        let mut write = PendingWrite::new(
            1,
            vec![1, 2, 3],
            WriteConsistency::Quorum,
            3,
            Duration::from_secs(30),
        );

        assert_eq!(write.required_acks, 2);
        assert!(!write.is_complete());

        write.add_ack(WriteAck {
            replica_id: "node1".to_string(),
            success: true,
            commit_index: 1,
            latency_ms: 5.0,
            error: None,
        });
        assert!(!write.is_complete());

        write.add_ack(WriteAck {
            replica_id: "node2".to_string(),
            success: true,
            commit_index: 1,
            latency_ms: 7.0,
            error: None,
        });
        assert!(write.is_complete());
    }

    #[test]
    fn test_health_monitor() {
        let config = HaConfig::default();
        let monitor = HealthMonitor::new(config);

        // Healthy check
        let state = monitor.record_check("node1", true, 5.0);
        assert_eq!(state, ReplicaState::Healthy);

        // Failed checks
        monitor.record_check("node1", false, 0.0);
        monitor.record_check("node1", false, 0.0);
        let state = monitor.record_check("node1", false, 0.0);
        assert_eq!(state, ReplicaState::Offline);
    }

    #[test]
    fn test_failover_manager() {
        let config = HaConfig::default();
        let election = Arc::new(LeaderElection::new("node1".to_string(), config.clone()));
        let health = Arc::new(HealthMonitor::new(config.clone()));
        let failover = FailoverManager::new(config, election, health);

        // No failover needed initially
        assert!(!failover.check_failover_needed());

        // Trigger manual failover
        let result = failover.manual_failover("node2");
        assert!(result.is_ok());

        let events = failover.get_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, FailoverEventType::ManualFailover);
    }

    #[test]
    fn test_ha_manager() {
        let config = HaConfig::default();
        let manager = HaManager::new("node1".to_string(), config);

        // Add replicas
        let replica = ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string());
        manager.add_replica(replica);

        let status = manager.status();
        assert_eq!(status.role, ReplicaRole::Follower);
    }

    #[test]
    fn test_log_entry() {
        let entry = LogEntry {
            index: 1,
            term: 1,
            data: vec![1, 2, 3],
            entry_type: LogEntryType::Data,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: LogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.index, 1);
    }

    #[test]
    fn test_zone_aware_selection() {
        let selector = ReplicaSelector::new(LoadBalanceStrategy::RoundRobin)
            .with_local_zone("us-east-1a".to_string());

        let mut replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
        ];
        replicas[0].zone = Some("us-east-1a".to_string());
        replicas[1].zone = Some("us-east-1b".to_string());

        selector.update_replicas(replicas);

        let selected = selector.select_zone_aware(ReadPreference::Secondary).unwrap();
        assert_eq!(selected.zone, Some("us-east-1a".to_string()));
    }

    #[test]
    fn test_least_connections_strategy() {
        let selector = ReplicaSelector::new(LoadBalanceStrategy::LeastConnections);

        let replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
        ];
        selector.update_replicas(replicas);

        selector.update_connection_count("node1", 100);
        selector.update_connection_count("node2", 10);

        let selected = selector.select(ReadPreference::Secondary).unwrap();
        assert_eq!(selected.replica_id, "node2");
    }

    #[test]
    fn test_least_latency_strategy() {
        let selector = ReplicaSelector::new(LoadBalanceStrategy::LeastLatency);

        let replicas = vec![
            ReplicaInfo::new("node1".to_string(), "127.0.0.1:8080".to_string()),
            ReplicaInfo::new("node2".to_string(), "127.0.0.1:8081".to_string()),
        ];
        selector.update_replicas(replicas);

        selector.update_latency("node1", 50.0);
        selector.update_latency("node2", 10.0);

        let selected = selector.select(ReadPreference::Secondary).unwrap();
        assert_eq!(selected.replica_id, "node2");
    }
}
