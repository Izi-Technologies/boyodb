//! Cluster message types for gossip, election, and replication.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    /// Generate a new unique node ID.
    pub fn generate() -> Self {
        let host = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let uuid = uuid::Uuid::new_v4();
        NodeId(format!("{}-{}", host, &uuid.to_string()[..8]))
    }

    /// Create a NodeId from a string.
    pub fn from_string(s: impl Into<String>) -> Self {
        NodeId(s.into())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Node health states in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is healthy and responding.
    Alive,
    /// Node suspected of failure (missed heartbeats).
    Suspect,
    /// Node confirmed dead.
    Dead,
    /// Node is leaving the cluster gracefully.
    Leaving,
}

/// Cluster role of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// This node is the leader and coordinates writes.
    Leader,
    /// This node follows the leader.
    Follower,
    /// This node is running for leader election.
    Candidate,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Follower
    }
}

/// Metadata about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMeta {
    pub node_id: NodeId,
    /// Address for client/replication TCP connections.
    pub rpc_addr: SocketAddr,
    /// Address for gossip protocol (UDP).
    pub gossip_addr: SocketAddr,
    /// Current health state.
    pub state: NodeState,
    /// Current role in the cluster.
    pub role: NodeRole,
    /// Incarnation number for conflict resolution (higher wins).
    pub incarnation: u64,
    /// Current manifest version for data freshness.
    pub manifest_version: u64,
    /// ID of the current leader (if known).
    pub leader_id: Option<NodeId>,
    /// When the leader's lease expires (Unix timestamp ms).
    pub leader_lease_expires: Option<u64>,
    /// When this node joined the cluster (Unix timestamp ms).
    pub join_time: u64,
}

impl NodeMeta {
    /// Create metadata for a new node.
    pub fn new(node_id: NodeId, rpc_addr: SocketAddr, gossip_addr: SocketAddr) -> Self {
        NodeMeta {
            node_id,
            rpc_addr,
            gossip_addr,
            state: NodeState::Alive,
            role: NodeRole::Follower,
            incarnation: 0,
            manifest_version: 0,
            leader_id: None,
            leader_lease_expires: None,
            join_time: current_timestamp_ms(),
        }
    }
}

/// Gossip message types for cluster communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Ping for failure detection.
    Ping {
        from: NodeId,
        incarnation: u64,
        sequence: u64,
    },
    /// Ack response to ping.
    Ack {
        from: NodeId,
        incarnation: u64,
        sequence: u64,
    },
    /// Indirect ping request (ask node B to ping node C).
    PingReq {
        from: NodeId,
        target: NodeId,
        sequence: u64,
    },
    /// Membership update broadcast.
    Sync {
        from: NodeId,
        members: Vec<NodeMeta>,
        cluster_id: String,
    },
    /// Join request from new node.
    Join {
        node: NodeMeta,
        cluster_id: String,
    },
    /// Acknowledge join.
    JoinAck {
        from: NodeId,
        members: Vec<NodeMeta>,
        leader_id: Option<NodeId>,
    },
    /// Graceful leave announcement.
    Leave {
        node_id: NodeId,
        incarnation: u64,
    },
    /// Leader election messages.
    Election(ElectionMessage),
    /// Cluster metadata update.
    MetadataUpdate {
        from: NodeId,
        manifest_version: u64,
        leader_id: Option<NodeId>,
        leader_lease_expires: Option<u64>,
    },
}

/// TCP-based replication message types (sent over TCP, not UDP gossip).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// Write operation from leader to follower.
    WriteRequest {
        operation: WriteOperation,
    },
    /// Acknowledgment from follower to leader.
    WriteResponse {
        ack: WriteAck,
    },
    /// Request current manifest version (for sync).
    SyncRequest {
        from: NodeId,
        last_version: u64,
    },
    /// Response with bundle data for catch-up.
    SyncResponse {
        bundle_data: Vec<u8>,
        from_version: u64,
        to_version: u64,
    },
}

/// Election-related messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ElectionMessage {
    /// Request vote from other nodes.
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        manifest_version: u64,
    },
    /// Vote response.
    VoteResponse {
        term: u64,
        voter_id: NodeId,
        vote_granted: bool,
    },
    /// Leader heartbeat with lease renewal.
    LeaderHeartbeat {
        term: u64,
        leader_id: NodeId,
        lease_expires: u64,
        manifest_version: u64,
    },
    /// Step down notification.
    StepDown {
        term: u64,
        leader_id: NodeId,
    },
}

/// Write operation that must be replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOperation {
    pub id: u64,
    pub term: u64,
    pub fencing_token: u64,
    pub database: String,
    pub table: String,
    pub payload: WritePayload,
    pub timestamp: u64,
}

/// Types of write operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WritePayload {
    /// Ingest data in IPC format.
    Ingest {
        ipc_data: Vec<u8>,
        watermark_micros: u64,
    },
    /// Create a database.
    CreateDatabase { name: String },
    /// Create a table.
    CreateTable {
        database: String,
        table: String,
        schema_json: Option<String>,
    },
    /// Delete rows.
    Delete { sql: String },
    /// Update rows.
    Update { sql: String },
    /// Drop a table.
    DropTable { database: String, table: String },
    /// Drop a database.
    DropDatabase { name: String },
}

/// Replication acknowledgment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteAck {
    pub write_id: u64,
    pub node_id: NodeId,
    pub success: bool,
    pub error: Option<String>,
    pub manifest_version: u64,
}

/// Read consistency levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Read from any node (eventual consistency, lowest latency).
    Any,
    /// Read from leader only (strong consistency).
    Leader,
    /// Read from local node, verify with leader that data is fresh.
    LeaderVerified,
    /// Read from any node that has caught up to a specific version.
    AtLeastVersion(u64),
}

impl Default for ReadConsistency {
    fn default() -> Self {
        ReadConsistency::Any
    }
}

/// Get current timestamp in milliseconds.
pub fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_generation() {
        let id1 = NodeId::generate();
        let id2 = NodeId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_node_meta_creation() {
        let id = NodeId::from_string("test-node");
        let rpc: SocketAddr = "127.0.0.1:8765".parse().unwrap();
        let gossip: SocketAddr = "127.0.0.1:8766".parse().unwrap();

        let meta = NodeMeta::new(id.clone(), rpc, gossip);
        assert_eq!(meta.node_id, id);
        assert_eq!(meta.state, NodeState::Alive);
        assert_eq!(meta.role, NodeRole::Follower);
    }
}
