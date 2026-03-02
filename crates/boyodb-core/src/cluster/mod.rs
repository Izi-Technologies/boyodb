//! Cluster module for High Availability with automatic failover.
//!
//! This module provides:
//! - Gossip-based node discovery (SWIM protocol)
//! - Leader election (Raft-lite with lease)
//! - Quorum-based write replication
//! - Automatic failover
//!
//! # Architecture
//!
//! ```text
//! ┌──────────┐     ┌──────────┐     ┌──────────┐
//! │  Node 1  │◄───►│  Node 2  │◄───►│  Node 3  │
//! │ (Leader) │     │(Follower)│     │(Follower)│
//! └────┬─────┘     └────┬─────┘     └────┬─────┘
//!      │                │                │
//!      └────────────────┼────────────────┘
//!                       │
//!            Gossip Protocol (UDP)
//!            Leader Election (Raft-lite)
//!            Write Replication (TCP)
//! ```
//!
//! # Usage
//!
//! ```bash
//! boyodb-server /data 0.0.0.0:8765 \
//!   --cluster \
//!   --cluster-id my-cluster \
//!   --gossip-addr 0.0.0.0:8766 \
//!   --seed-nodes "10.0.0.1:8766,10.0.0.2:8766"
//! ```

pub mod election;
pub mod failure_detector;
pub mod gossip;
pub mod messages;
pub mod replication;

// Re-export main types
pub use election::{ElectionConfig, ElectionState};
pub use failure_detector::PhiAccrualDetector;
pub use gossip::{GossipConfig, GossipProtocol, Membership};
pub use messages::{
    current_timestamp_ms, ElectionMessage, GossipMessage, NodeId, NodeMeta, NodeRole, NodeState,
    ReadConsistency, ReplicationMessage, WriteAck, WriteOperation, WritePayload,
};
pub use replication::{
    create_write_operation, start_replication_listener, ReplicationCoordinator, ReplicationHandler,
};

use crate::engine::{Db, EngineError};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// Cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Unique cluster identifier.
    pub cluster_id: String,
    /// This node's ID (auto-generated if None).
    pub node_id: Option<String>,
    /// Address for client/replication TCP connections.
    pub rpc_addr: SocketAddr,
    /// Address for gossip protocol (UDP).
    pub gossip_addr: SocketAddr,
    /// Seed nodes for initial cluster discovery.
    pub seed_nodes: Vec<SocketAddr>,
    /// Gossip protocol configuration.
    pub gossip_config: GossipConfig,
    /// Election configuration.
    pub election_config: ElectionConfig,
    /// Write timeout for quorum.
    pub write_timeout: Duration,
    /// Whether to require quorum for writes.
    pub require_quorum: bool,
    /// Enable 2-node cluster mode (primary/replica with quorum of 1).
    /// This allows the cluster to operate when only one node is available.
    pub two_node_mode: bool,
}

impl ClusterConfig {
    /// Create a new cluster configuration.
    pub fn new(cluster_id: String, rpc_addr: SocketAddr, gossip_addr: SocketAddr) -> Self {
        ClusterConfig {
            cluster_id,
            node_id: None,
            rpc_addr,
            gossip_addr,
            seed_nodes: Vec::new(),
            gossip_config: GossipConfig::default(),
            election_config: ElectionConfig::default(),
            write_timeout: Duration::from_secs(5),
            require_quorum: true,
            two_node_mode: false,
        }
    }

    /// Set seed nodes.
    pub fn with_seed_nodes(mut self, seeds: Vec<SocketAddr>) -> Self {
        self.seed_nodes = seeds;
        self
    }

    /// Set node ID.
    pub fn with_node_id(mut self, id: String) -> Self {
        self.node_id = Some(id);
        self
    }

    /// Enable 2-node cluster mode (primary/replica).
    /// This sets min_quorum to 1, allowing operation when only one node is available.
    pub fn with_two_node_mode(mut self) -> Self {
        self.two_node_mode = true;
        self.election_config = ElectionConfig::two_node_cluster();
        self
    }
}

/// Write result from cluster coordinator.
#[derive(Debug)]
pub enum WriteResult {
    /// Write succeeded with manifest version.
    Success { manifest_version: u64 },
    /// Quorum not reached.
    QuorumNotReached {
        acks_received: usize,
        required: usize,
    },
    /// Write timed out.
    Timeout,
    /// This node is not the leader.
    NotLeader { leader_addr: Option<SocketAddr> },
    /// Error during write.
    Error(String),
}

/// Pending write tracking.
#[derive(Debug)]
struct PendingWrite {
    _operation: WriteOperation,
    acks: HashMap<NodeId, WriteAck>,
    _started_at: Instant,
    required_acks: usize,
    result_tx: tokio::sync::oneshot::Sender<WriteResult>,
}

/// Cluster manager that coordinates all cluster components.
pub struct ClusterManager {
    config: ClusterConfig,
    node_id: NodeId,
    pub gossip: Arc<RwLock<GossipProtocol>>,
    election: Arc<RwLock<ElectionState>>,
    _db: Arc<Db>,
    /// Pending writes awaiting quorum.
    pending_writes: Arc<RwLock<HashMap<u64, PendingWrite>>>,
    /// Next write ID.
    _next_write_id: Arc<std::sync::atomic::AtomicU64>,
    /// Channel for outbound gossip messages.
    gossip_tx: mpsc::Sender<(SocketAddr, GossipMessage)>,
    /// Shutdown signal.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl ClusterManager {
    /// Create a new cluster manager.
    pub fn new(
        config: ClusterConfig,
        db: Arc<Db>,
    ) -> (Self, mpsc::Receiver<(SocketAddr, GossipMessage)>) {
        let node_id = config
            .node_id
            .as_ref()
            .map(|id| NodeId::from_string(id.clone()))
            .unwrap_or_else(NodeId::generate);

        let local_node = NodeMeta::new(node_id.clone(), config.rpc_addr, config.gossip_addr);

        let membership =
            Membership::new(config.cluster_id.clone(), local_node, config.seed_nodes.clone());

        let gossip = GossipProtocol::new(config.gossip_config.clone(), membership);
        let election = ElectionState::new(node_id.clone(), config.election_config.clone());

        let (gossip_tx, gossip_rx) = mpsc::channel(1000);

        let manager = ClusterManager {
            config,
            node_id,
            gossip: Arc::new(RwLock::new(gossip)),
            election: Arc::new(RwLock::new(election)),
            _db: db,
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            _next_write_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            gossip_tx,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        (manager, gossip_rx)
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.election.read().is_leader()
    }

    /// Get current leader address.
    pub fn leader_addr(&self) -> Option<SocketAddr> {
        let election = self.election.read();
        election
            .leader()
            .and_then(|id| self.gossip.read().get_node_addr(id))
    }

    /// Get current leader ID.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.election.read().leader().cloned()
    }

    /// Get count of alive nodes.
    pub fn alive_node_count(&self) -> usize {
        self.gossip.read().alive_node_count()
    }

    /// Get total node count.
    pub fn total_node_count(&self) -> usize {
        self.gossip.read().total_node_count()
    }

    /// Check if we have write quorum.
    pub fn has_write_quorum(&self) -> bool {
        let election = self.election.read();
        let alive = self.alive_node_count();
        let total = self.total_node_count();
        election.has_write_quorum(alive, total)
    }

    /// Get current term.
    pub fn current_term(&self) -> u64 {
        self.election.read().current_term()
    }

    /// Get cluster status for monitoring.
    pub fn status(&self) -> ClusterStatus {
        let election = self.election.read();
        let gossip = self.gossip.read();

        ClusterStatus {
            node_id: self.node_id.clone(),
            cluster_id: self.config.cluster_id.clone(),
            role: election.role,
            term: election.term,
            leader_id: election.current_leader.clone(),
            alive_nodes: gossip.alive_node_count(),
            total_nodes: gossip.total_node_count(),
            has_quorum: election.has_write_quorum(
                gossip.alive_node_count(),
                gossip.total_node_count(),
            ),
        }
    }

    /// Shutdown the cluster manager.
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get the shutdown flag for external tasks.
    pub fn shutdown_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.shutdown.clone()
    }

    /// Update manifest version (call after successful writes).
    pub fn set_manifest_version(&self, version: u64) {
        self.gossip.write().membership.set_manifest_version(version);
        self.election.write().set_manifest_version(version);
    }

    /// Handle incoming gossip message.
    pub async fn handle_gossip_message(
        &self,
        from_addr: SocketAddr,
        msg: GossipMessage,
    ) -> Result<(), EngineError> {
        // Handle election messages specially
        let election_msg = if let GossipMessage::Election(ref election_msg) = msg {
            Some(election_msg.clone())
        } else {
            None
        };

        if let Some(emsg) = election_msg {
            self.handle_election_message(emsg).await?;
        }

        // Let gossip protocol handle the message (sync)
        let responses: Vec<(SocketAddr, GossipMessage)> =
            self.gossip.write().handle_message(from_addr, msg);

        // Send responses (async)
        for (addr, response) in responses {
            let _ = self.gossip_tx.send((addr, response)).await;
        }

        Ok(())
    }

    /// Handle election message (sync part).
    fn handle_election_message_sync(&self, msg: ElectionMessage) -> Option<ElectionMessage> {
        let total_nodes = self.total_node_count();
        let mut election = self.election.write();

        match msg {
            ElectionMessage::RequestVote {
                term,
                ref candidate_id,
                manifest_version,
            } => Some(election.handle_request_vote(term, candidate_id, manifest_version)),

            ElectionMessage::VoteResponse {
                term,
                ref voter_id,
                vote_granted,
            } => election.handle_vote_response(term, voter_id, vote_granted, total_nodes),

            ElectionMessage::LeaderHeartbeat {
                term,
                ref leader_id,
                lease_expires,
                manifest_version,
            } => {
                election.handle_leader_heartbeat(term, leader_id, lease_expires, manifest_version);
                // Update gossip with leader info
                drop(election);
                let mut gossip = self.gossip.write();
                gossip
                    .membership
                    .set_leader(Some(leader_id.clone()), Some(lease_expires));
                None
            }

            ElectionMessage::StepDown { term, ref leader_id } => {
                election.handle_step_down(term, leader_id);
                self.gossip.write().membership.set_leader(None, None);
                None
            }
        }
    }

    async fn handle_election_message(
        &self,
        msg: ElectionMessage,
    ) -> Result<(), EngineError> {
        let response = self.handle_election_message_sync(msg);

        // Broadcast response if any
        if let Some(resp) = response {
            self.broadcast_election_message(resp).await?;
        }

        Ok(())
    }

    async fn broadcast_election_message(&self, msg: ElectionMessage) -> Result<(), EngineError> {
        let gossip_msg = GossipMessage::Election(msg);
        let members: Vec<(NodeId, SocketAddr)> = {
            let gossip = self.gossip.read();
            gossip
                .membership
                .members
                .iter()
                .filter(|(id, m)| **id != self.node_id && m.state == NodeState::Alive)
                .map(|(id, m)| (id.clone(), m.gossip_addr))
                .collect()
        };

        for (_, addr) in members {
            let _ = self.gossip_tx.send((addr, gossip_msg.clone())).await;
        }

        Ok(())
    }

    /// Run the gossip protocol tick.
    pub async fn gossip_tick(&self) -> Result<(), EngineError> {
        // Collect messages synchronously
        let messages: Vec<(SocketAddr, GossipMessage)> = self.gossip.write().tick();

        // Send messages asynchronously
        for (addr, msg) in messages {
            let _ = self.gossip_tx.send((addr, msg)).await;
        }

        Ok(())
    }

    /// Run the election protocol tick (sync part).
    fn election_tick_sync(
        &self,
    ) -> (bool, Option<ElectionMessage>, Option<ElectionMessage>, Option<ElectionMessage>) {
        // Check lease expiry
        let mut stepdown: Option<ElectionMessage> = None;
        {
            let mut election = self.election.write();
            if election.check_lease_expiry() {
                // Stepped down - clear leader info in gossip
                self.gossip.write().membership.set_leader(None, None);
                stepdown = Some(ElectionMessage::StepDown {
                    term: election.current_term(),
                    leader_id: self.node_id.clone(),
                });
            }
        }

        // Check if we should start an election
        let should_start = self.election.read().should_start_election();
        let election_msg = if should_start {
            Some(self.election.write().start_election())
        } else {
            None
        };

        // If leader, generate heartbeat
        let heartbeat = self.election.write().generate_heartbeat();

        (should_start, election_msg, heartbeat, stepdown)
    }

    /// Run the election protocol tick.
    pub async fn election_tick(&self) -> Result<(), EngineError> {
        let (_, election_msg, heartbeat, stepdown) = self.election_tick_sync();

        // Send election message if starting election
        if let Some(msg) = election_msg {
            self.broadcast_election_message(msg).await?;
        }

        // If leader, send heartbeat
        if let Some(heartbeat) = heartbeat {
            self.broadcast_election_message(heartbeat).await?;
        }

        // Broadcast step-down notice when we relinquish leadership
        if let Some(msg) = stepdown {
            self.broadcast_election_message(msg).await?;
        }

        Ok(())
    }

    /// Join the cluster by contacting seed nodes.
    pub async fn join_cluster(&self) -> Result<(), EngineError> {
        // Collect message and seeds synchronously
        let join_msg = self.gossip.read().create_join_message();
        let seeds: Vec<SocketAddr> = self.config.seed_nodes.clone();

        // Send asynchronously
        for seed in seeds {
            let _ = self.gossip_tx.send((seed, join_msg.clone())).await;
        }

        Ok(())
    }

    /// Leave the cluster gracefully.
    pub async fn leave_cluster(&self) -> Result<(), EngineError> {
        // Collect message and addresses synchronously
        let (leave_msg, addrs) = {
            let leave_msg = self.gossip.write().create_leave_message();
            let gossip = self.gossip.read();
            let addrs: Vec<SocketAddr> = gossip
                .membership
                .members
                .values()
                .filter(|m| m.node_id != self.node_id)
                .map(|m| m.gossip_addr)
                .collect();
            (leave_msg, addrs)
        };

        // Send asynchronously
        for addr in addrs {
            let _ = self.gossip_tx.send((addr, leave_msg.clone())).await;
        }

        Ok(())
    }

    /// Handle a write acknowledgment from a follower.
    pub fn handle_write_ack(&self, ack: WriteAck) {
        let mut pending = self.pending_writes.write();

        // First check if we should complete the write
        let should_complete = if let Some(pw) = pending.get_mut(&ack.write_id) {
            pw.acks.insert(ack.node_id.clone(), ack.clone());

            // Check if quorum reached
            let successful_acks = pw.acks.values().filter(|a| a.success).count() + 1; // +1 for local
            successful_acks >= pw.required_acks
        } else {
            false
        };

        // If quorum reached, remove and complete
        if should_complete {
            if let Some(pw) = pending.remove(&ack.write_id) {
                let max_version = pw
                    .acks
                    .values()
                    .map(|a| a.manifest_version)
                    .max()
                    .unwrap_or(0);

                let _ = pw
                    .result_tx
                    .send(WriteResult::Success {
                        manifest_version: max_version,
                    });
            }
        }
    }
}

/// Cluster status for monitoring.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub role: NodeRole,
    pub term: u64,
    pub leader_id: Option<NodeId>,
    pub alive_nodes: usize,
    pub total_nodes: usize,
    pub has_quorum: bool,
}

impl ClusterStatus {
    /// Convert to JSON for API responses.
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"node_id":"{}","cluster_id":"{}","role":"{:?}","term":{},"leader_id":{},"alive_nodes":{},"total_nodes":{},"has_quorum":{}}}"#,
            self.node_id,
            self.cluster_id,
            self.role,
            self.term,
            self.leader_id
                .as_ref()
                .map(|id| format!("\"{}\"", id))
                .unwrap_or_else(|| "null".to_string()),
            self.alive_nodes,
            self.total_nodes,
            self.has_quorum
        )
    }
}

/// Start the gossip UDP listener.
pub async fn start_gossip_listener(
    gossip_addr: SocketAddr,
    manager: Arc<ClusterManager>,
) -> Result<(), EngineError> {
    let socket = UdpSocket::bind(gossip_addr)
        .await
        .map_err(|e| EngineError::Io(format!("bind gossip socket: {}", e)))?;

    let mut buf = vec![0u8; 65536];

    loop {
        if manager
            .shutdown
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            break;
        }

        match socket.recv_from(&mut buf).await {
            Ok((len, from_addr)) => {
                if let Ok(msg) = serde_json::from_slice::<GossipMessage>(&buf[..len]) {
                    if let Err(e) = manager.handle_gossip_message(from_addr, msg).await {
                        tracing::warn!("error handling gossip message: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("gossip recv error: {}", e);
            }
        }
    }

    Ok(())
}

/// Start the gossip sender task.
pub async fn start_gossip_sender(
    mut rx: mpsc::Receiver<(SocketAddr, GossipMessage)>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), EngineError> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| EngineError::Io(format!("bind gossip sender: {}", e)))?;

    while let Some((addr, msg)) = rx.recv().await {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        if let Ok(data) = serde_json::to_vec(&msg) {
            let _ = socket.send_to(&data, addr).await;
        }
    }

    Ok(())
}

/// Start the cluster background tasks.
pub async fn start_cluster_tasks(manager: Arc<ClusterManager>) {
    let gossip_interval = manager.config.gossip_config.gossip_interval;
    let election_check_interval = Duration::from_millis(50);

    // Join cluster first
    if let Err(e) = manager.join_cluster().await {
        tracing::error!("failed to join cluster: {}", e);
    }

    let mut gossip_ticker = tokio::time::interval(gossip_interval);
    let mut election_ticker = tokio::time::interval(election_check_interval);

    loop {
        if manager
            .shutdown
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            break;
        }

        tokio::select! {
            _ = gossip_ticker.tick() => {
                if let Err(e) = manager.gossip_tick().await {
                    tracing::warn!("gossip tick error: {}", e);
                }
            }
            _ = election_ticker.tick() => {
                if let Err(e) = manager.election_tick().await {
                    tracing::warn!("election tick error: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config() {
        let rpc: SocketAddr = "127.0.0.1:8765".parse().unwrap();
        let gossip: SocketAddr = "127.0.0.1:8766".parse().unwrap();

        let config = ClusterConfig::new("test-cluster".into(), rpc, gossip)
            .with_node_id("node1".into())
            .with_seed_nodes(vec!["127.0.0.1:8767".parse().unwrap()]);

        assert_eq!(config.cluster_id, "test-cluster");
        assert_eq!(config.node_id, Some("node1".into()));
        assert_eq!(config.seed_nodes.len(), 1);
    }

    #[test]
    fn test_cluster_status_json() {
        let status = ClusterStatus {
            node_id: NodeId::from_string("node1"),
            cluster_id: "test".into(),
            role: NodeRole::Leader,
            term: 5,
            leader_id: Some(NodeId::from_string("node1")),
            alive_nodes: 3,
            total_nodes: 3,
            has_quorum: true,
        };

        let json = status.to_json();
        assert!(json.contains("\"node_id\":\"node1\""));
        assert!(json.contains("\"role\":\"Leader\""));
        assert!(json.contains("\"has_quorum\":true"));
    }
}
