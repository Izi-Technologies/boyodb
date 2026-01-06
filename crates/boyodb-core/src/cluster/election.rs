//! Raft-lite leader election with lease-based leadership.
//!
//! This is a simplified Raft protocol focused on leader election,
//! without the full log replication complexity. It provides:
//! - Leader election with randomized timeouts
//! - Lease-based leadership with heartbeats
//! - Fencing tokens for split-brain prevention

use crate::cluster::messages::{current_timestamp_ms, ElectionMessage, NodeId, NodeRole};
use rand::Rng;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Election configuration.
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Leader lease duration.
    pub lease_duration: Duration,
    /// Minimum election timeout (randomized between min and max).
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    pub election_timeout_max: Duration,
    /// Heartbeat interval from leader (should be << lease_duration).
    pub heartbeat_interval: Duration,
    /// Minimum quorum size override. If set, uses this instead of (N/2)+1.
    /// For 2-node clusters, set to 1 to allow single-node quorum.
    pub min_quorum_size: Option<usize>,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        ElectionConfig {
            lease_duration: Duration::from_secs(10),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_secs(3),
            min_quorum_size: None, // Use standard majority: (N/2)+1
        }
    }
}

impl ElectionConfig {
    /// Create config optimized for local network (low latency).
    pub fn local_network() -> Self {
        ElectionConfig {
            lease_duration: Duration::from_secs(5),
            election_timeout_min: Duration::from_millis(100),
            election_timeout_max: Duration::from_millis(200),
            heartbeat_interval: Duration::from_secs(1),
            min_quorum_size: None,
        }
    }

    /// Create config for WAN/high-latency networks.
    pub fn wide_area_network() -> Self {
        ElectionConfig {
            lease_duration: Duration::from_secs(30),
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_millis(1000),
            heartbeat_interval: Duration::from_secs(10),
            min_quorum_size: None,
        }
    }

    /// Create config for 2-node cluster (primary/replica setup).
    /// Uses quorum of 1 to allow operation when only one node is available.
    pub fn two_node_cluster() -> Self {
        ElectionConfig {
            lease_duration: Duration::from_secs(10),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_secs(3),
            min_quorum_size: Some(1), // Single node can operate
        }
    }

    /// Set minimum quorum size.
    pub fn with_min_quorum(mut self, size: usize) -> Self {
        self.min_quorum_size = Some(size);
        self
    }

    /// Calculate quorum size for given total nodes.
    pub fn quorum_size(&self, total_nodes: usize) -> usize {
        if let Some(min) = self.min_quorum_size {
            // Use configured minimum, but ensure at least 1
            min.max(1)
        } else {
            // Standard majority: (N/2)+1
            (total_nodes / 2) + 1
        }
    }
}

/// Election state machine.
#[derive(Debug)]
pub struct ElectionState {
    pub config: ElectionConfig,
    /// Current election term (monotonically increasing).
    pub term: u64,
    /// Current role in the cluster.
    pub role: NodeRole,
    /// Who we voted for in this term.
    pub voted_for: Option<NodeId>,
    /// Current leader (if known).
    pub current_leader: Option<NodeId>,
    /// When the leader's lease expires.
    pub lease_expires: Option<Instant>,
    /// Votes received in current election (only valid when Candidate).
    pub votes_received: HashSet<NodeId>,
    /// Deadline for election timeout.
    pub election_deadline: Instant,
    /// This node's ID.
    pub local_node_id: NodeId,
    /// This node's manifest version (for leader eligibility).
    pub local_manifest_version: u64,
    /// When we last sent a heartbeat (leader only).
    last_heartbeat_sent: Option<Instant>,
}

impl ElectionState {
    /// Create a new election state for a node.
    pub fn new(node_id: NodeId, config: ElectionConfig) -> Self {
        let timeout = Self::random_election_timeout(&config);
        ElectionState {
            config,
            term: 0,
            role: NodeRole::Follower,
            voted_for: None,
            current_leader: None,
            lease_expires: None,
            votes_received: HashSet::new(),
            election_deadline: Instant::now() + timeout,
            local_node_id: node_id,
            local_manifest_version: 0,
            last_heartbeat_sent: None,
        }
    }

    fn random_election_timeout(config: &ElectionConfig) -> Duration {
        let min = config.election_timeout_min.as_millis() as u64;
        let max = config.election_timeout_max.as_millis() as u64;
        Duration::from_millis(rand::thread_rng().gen_range(min..=max))
    }

    /// Update the local manifest version.
    pub fn set_manifest_version(&mut self, version: u64) {
        self.local_manifest_version = version;
    }

    /// Check if we should start an election.
    pub fn should_start_election(&self) -> bool {
        self.role != NodeRole::Leader
            && Instant::now() >= self.election_deadline
            && !self.has_valid_leader()
    }

    /// Check if current leader lease is still valid.
    pub fn has_valid_leader(&self) -> bool {
        if let Some(expires) = self.lease_expires {
            Instant::now() < expires
        } else {
            false
        }
    }

    /// Check if we are the leader.
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader && self.has_valid_leader()
    }

    /// Start an election as candidate.
    pub fn start_election(&mut self) -> ElectionMessage {
        self.term += 1;
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.local_node_id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.local_node_id.clone());
        self.current_leader = None;
        self.lease_expires = None;
        self.election_deadline = Instant::now() + Self::random_election_timeout(&self.config);

        ElectionMessage::RequestVote {
            term: self.term,
            candidate_id: self.local_node_id.clone(),
            manifest_version: self.local_manifest_version,
        }
    }

    /// Handle incoming vote request.
    pub fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: &NodeId,
        candidate_manifest_version: u64,
    ) -> ElectionMessage {
        // Update term if behind
        if term > self.term {
            self.become_follower(term, None);
        }

        // Grant vote if:
        // 1. Term is current or newer
        // 2. Haven't voted in this term, or already voted for this candidate
        // 3. Candidate has at least as fresh data as us
        let vote_granted = term >= self.term
            && (self.voted_for.is_none() || self.voted_for.as_ref() == Some(candidate_id))
            && candidate_manifest_version >= self.local_manifest_version;

        if vote_granted {
            self.voted_for = Some(candidate_id.clone());
            self.election_deadline = Instant::now() + Self::random_election_timeout(&self.config);
        }

        ElectionMessage::VoteResponse {
            term: self.term,
            voter_id: self.local_node_id.clone(),
            vote_granted,
        }
    }

    /// Handle vote response.
    ///
    /// Returns a LeaderHeartbeat message if we became leader.
    pub fn handle_vote_response(
        &mut self,
        term: u64,
        voter_id: &NodeId,
        vote_granted: bool,
        total_nodes: usize,
    ) -> Option<ElectionMessage> {
        // Ignore stale responses
        if term != self.term || self.role != NodeRole::Candidate {
            return None;
        }

        if vote_granted {
            self.votes_received.insert(voter_id.clone());
        }

        // Check for quorum (uses config's min_quorum_size if set)
        let quorum = self.config.quorum_size(total_nodes);
        if self.votes_received.len() >= quorum {
            self.become_leader()
        } else {
            None
        }
    }

    /// Transition to leader.
    fn become_leader(&mut self) -> Option<ElectionMessage> {
        self.role = NodeRole::Leader;
        self.current_leader = Some(self.local_node_id.clone());
        let expires = Instant::now() + self.config.lease_duration;
        self.lease_expires = Some(expires);
        self.last_heartbeat_sent = Some(Instant::now());

        Some(ElectionMessage::LeaderHeartbeat {
            term: self.term,
            leader_id: self.local_node_id.clone(),
            lease_expires: current_timestamp_ms() + self.config.lease_duration.as_millis() as u64,
            manifest_version: self.local_manifest_version,
        })
    }

    /// Handle leader heartbeat from another node.
    pub fn handle_leader_heartbeat(
        &mut self,
        term: u64,
        leader_id: &NodeId,
        lease_expires_ts: u64,
        manifest_version: u64,
    ) {
        // If we see a newer term, or another node claims leadership in our term, follow it.
        if term > self.term
            || (term == self.term
                && self.current_leader.as_ref() != Some(leader_id)
                && &self.local_node_id != leader_id)
        {
            self.become_follower(term, Some(leader_id.clone()));
        }

        // Keep manifest version monotonic locally; do not decrease.
        if manifest_version > self.local_manifest_version {
            self.local_manifest_version = manifest_version;
        }

        if term >= self.term {
            // Convert timestamp to Instant (approximate)
            let now_ts = current_timestamp_ms();
            if lease_expires_ts > now_ts {
                let remaining = Duration::from_millis(lease_expires_ts - now_ts);
                self.lease_expires = Some(Instant::now() + remaining);
            }

            // Reset election timeout on valid heartbeat
            self.election_deadline = Instant::now() + Self::random_election_timeout(&self.config);
            self.current_leader = Some(leader_id.clone());
        }
    }

    /// Handle explicit step-down notification.
    pub fn handle_step_down(&mut self, term: u64, leader_id: &NodeId) {
        if term > self.term {
            self.term = term;
        }
        // Clear leadership and reset election deadline so a new election can begin promptly.
        self.role = NodeRole::Follower;
        self.current_leader = None;
        self.voted_for = None;
        self.lease_expires = None;
        self.last_heartbeat_sent = None;
        self.election_deadline = Instant::now() + Self::random_election_timeout(&self.config);

        // Avoid reusing the same leader_id for stale state when we are the one stepping down.
        if leader_id == &self.local_node_id {
            self.votes_received.clear();
        }
    }

    /// Transition to follower.
    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        let was_leader = self.role == NodeRole::Leader;
        self.term = term;
        self.role = NodeRole::Follower;
        self.voted_for = None;
        self.current_leader = leader;
        self.votes_received.clear();
        self.election_deadline = Instant::now() + Self::random_election_timeout(&self.config);

        if was_leader {
            self.last_heartbeat_sent = None;
        }
    }

    /// Leader: generate heartbeat for lease renewal.
    pub fn generate_heartbeat(&mut self) -> Option<ElectionMessage> {
        if self.role != NodeRole::Leader {
            return None;
        }

        // Check if it's time to send heartbeat
        if let Some(last) = self.last_heartbeat_sent {
            if last.elapsed() < self.config.heartbeat_interval {
                return None;
            }
        }

        // Renew our lease
        let expires = Instant::now() + self.config.lease_duration;
        self.lease_expires = Some(expires);
        self.last_heartbeat_sent = Some(Instant::now());

        Some(ElectionMessage::LeaderHeartbeat {
            term: self.term,
            leader_id: self.local_node_id.clone(),
            lease_expires: current_timestamp_ms() + self.config.lease_duration.as_millis() as u64,
            manifest_version: self.local_manifest_version,
        })
    }

    /// Check if we have write quorum.
    pub fn has_write_quorum(&self, connected_nodes: usize, total_nodes: usize) -> bool {
        if self.role != NodeRole::Leader {
            return false;
        }

        // Need quorum of nodes to be reachable (uses config's min_quorum_size if set)
        let quorum = self.config.quorum_size(total_nodes);
        connected_nodes >= quorum
    }

    /// Check lease expiry and step down if needed.
    ///
    /// Returns true if we stepped down.
    pub fn check_lease_expiry(&mut self) -> bool {
        if self.role == NodeRole::Leader {
            if let Some(expires) = self.lease_expires {
                if Instant::now() >= expires {
                    // Lease expired, step down
                    self.role = NodeRole::Follower;
                    self.current_leader = None;
                    self.lease_expires = None;
                    self.last_heartbeat_sent = None;
                    return true;
                }
            }
        }
        false
    }

    /// Generate fencing token for preventing stale leaders from writing.
    ///
    /// Combines term and manifest version for a monotonically increasing token.
    pub fn fencing_token(&self) -> u64 {
        (self.term << 32) | (self.local_manifest_version & 0xFFFFFFFF)
    }

    /// Get current leader ID.
    pub fn leader(&self) -> Option<&NodeId> {
        self.current_leader.as_ref()
    }

    /// Get current term.
    pub fn current_term(&self) -> u64 {
        self.term
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> ElectionConfig {
        ElectionConfig {
            lease_duration: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(100),
            min_quorum_size: None,
        }
    }

    fn make_two_node_config() -> ElectionConfig {
        ElectionConfig {
            lease_duration: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(100),
            min_quorum_size: Some(1), // Single node can form quorum
        }
    }

    #[test]
    fn test_initial_state_is_follower() {
        let state = ElectionState::new(NodeId::from_string("node1"), make_config());
        assert_eq!(state.role, NodeRole::Follower);
        assert_eq!(state.term, 0);
        assert!(state.current_leader.is_none());
    }

    #[test]
    fn test_start_election() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());

        let msg = state.start_election();

        assert_eq!(state.role, NodeRole::Candidate);
        assert_eq!(state.term, 1);
        assert!(state.votes_received.contains(&NodeId::from_string("node1")));

        match msg {
            ElectionMessage::RequestVote { term, candidate_id, .. } => {
                assert_eq!(term, 1);
                assert_eq!(candidate_id, NodeId::from_string("node1"));
            }
            _ => panic!("expected RequestVote"),
        }
    }

    #[test]
    fn test_vote_granting() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());

        let response = state.handle_request_vote(1, &NodeId::from_string("node2"), 0);

        match response {
            ElectionMessage::VoteResponse { vote_granted, .. } => {
                assert!(vote_granted);
            }
            _ => panic!("expected VoteResponse"),
        }

        // Should not vote for another candidate in same term
        let response2 = state.handle_request_vote(1, &NodeId::from_string("node3"), 0);

        match response2 {
            ElectionMessage::VoteResponse { vote_granted, .. } => {
                assert!(!vote_granted);
            }
            _ => panic!("expected VoteResponse"),
        }
    }

    #[test]
    fn test_become_leader_with_majority() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());

        state.start_election();

        // Receive votes from 2 other nodes (total 3 nodes, quorum = 2)
        let result = state.handle_vote_response(1, &NodeId::from_string("node2"), true, 3);

        assert!(result.is_some());
        assert_eq!(state.role, NodeRole::Leader);

        match result.unwrap() {
            ElectionMessage::LeaderHeartbeat { leader_id, .. } => {
                assert_eq!(leader_id, NodeId::from_string("node1"));
            }
            _ => panic!("expected LeaderHeartbeat"),
        }
    }

    #[test]
    fn test_fencing_token_increases() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());

        let token1 = state.fencing_token();

        state.term += 1;
        let token2 = state.fencing_token();

        assert!(token2 > token1);

        state.local_manifest_version += 1;
        let token3 = state.fencing_token();

        assert!(token3 > token2);
    }

    #[test]
    fn test_quorum_size_standard() {
        let config = make_config();

        // Standard majority: (N/2)+1
        assert_eq!(config.quorum_size(1), 1); // 1 node: quorum = 1
        assert_eq!(config.quorum_size(2), 2); // 2 nodes: quorum = 2 (both needed)
        assert_eq!(config.quorum_size(3), 2); // 3 nodes: quorum = 2
        assert_eq!(config.quorum_size(5), 3); // 5 nodes: quorum = 3
    }

    #[test]
    fn test_quorum_size_two_node_mode() {
        let config = make_two_node_config();

        // With min_quorum_size = 1, single node can operate
        assert_eq!(config.quorum_size(1), 1);
        assert_eq!(config.quorum_size(2), 1); // 2-node cluster: only 1 needed
        assert_eq!(config.quorum_size(3), 1); // Even 3 nodes uses configured minimum
    }

    #[test]
    fn test_two_node_cluster_election() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_two_node_config());

        // Start election - node votes for itself
        state.start_election();
        assert_eq!(state.role, NodeRole::Candidate);
        assert_eq!(state.votes_received.len(), 1); // Self-vote

        // In 2-node mode with min_quorum=1, self-vote is enough to become leader
        // Need to check if quorum is reached (1 vote >= 1 required)
        let result = state.handle_vote_response(1, &NodeId::from_string("node1"), true, 2);

        // Self-vote reaches quorum (1), should produce heartbeat and transition to leader
        assert!(matches!(result, Some(ElectionMessage::LeaderHeartbeat { .. })));
        assert_eq!(state.role, NodeRole::Leader);
        assert_eq!(state.current_leader, Some(NodeId::from_string("node1")));
        assert_eq!(state.votes_received.len(), 1);
    }

    #[test]
    fn test_handle_leader_heartbeat_updates_manifest_and_deadline() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());
        state.local_manifest_version = 5;

        // Heartbeat from another leader with higher term and manifest
        state.handle_leader_heartbeat(
            2,
            &NodeId::from_string("leader"),
            current_timestamp_ms() + 200,
            6,
        );

        assert_eq!(state.role, NodeRole::Follower);
        assert_eq!(state.current_leader, Some(NodeId::from_string("leader")));
        assert_eq!(state.local_manifest_version, 6);
        assert!(state.has_valid_leader());
    }

    #[test]
    fn test_step_down_clears_leader() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_config());
        state.role = NodeRole::Leader;
        state.current_leader = Some(NodeId::from_string("node1"));
        state.term = 3;

        state.handle_step_down(4, &NodeId::from_string("node1"));

        assert_eq!(state.role, NodeRole::Follower);
        assert!(state.current_leader.is_none());
        assert_eq!(state.term, 4);
    }

    #[test]
    fn test_two_node_cluster_write_quorum() {
        let mut state = ElectionState::new(NodeId::from_string("node1"), make_two_node_config());

        // Make this node the leader
        state.role = NodeRole::Leader;
        state.lease_expires = Some(Instant::now() + Duration::from_secs(10));

        // With min_quorum=1, single node should have write quorum
        assert!(state.has_write_quorum(1, 2)); // 1 of 2 nodes connected
        assert!(state.has_write_quorum(2, 2)); // Both nodes connected

        // Standard config would require both nodes
        let mut standard_state = ElectionState::new(NodeId::from_string("node1"), make_config());
        standard_state.role = NodeRole::Leader;
        standard_state.lease_expires = Some(Instant::now() + Duration::from_secs(10));

        assert!(!standard_state.has_write_quorum(1, 2)); // 1 of 2 not enough
        assert!(standard_state.has_write_quorum(2, 2));  // Both nodes needed
    }

    #[test]
    fn test_two_node_cluster_config() {
        let config = ElectionConfig::two_node_cluster();

        assert_eq!(config.min_quorum_size, Some(1));
        assert_eq!(config.quorum_size(2), 1);
    }

    #[test]
    fn test_with_min_quorum_builder() {
        let config = ElectionConfig::default().with_min_quorum(2);

        assert_eq!(config.min_quorum_size, Some(2));
        assert_eq!(config.quorum_size(5), 2); // Uses configured minimum, not (5/2)+1=3
    }
}
