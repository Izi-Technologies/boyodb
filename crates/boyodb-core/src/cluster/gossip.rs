//! SWIM-based gossip protocol for cluster membership.
//!
//! This implements a simplified SWIM (Scalable Weakly-consistent
//! Infection-style Membership) protocol for node discovery and
//! failure detection.

use crate::cluster::failure_detector::PhiAccrualDetector;
use crate::cluster::messages::{current_timestamp_ms, GossipMessage, NodeId, NodeMeta, NodeState};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Gossip protocol configuration.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// How often to gossip with random nodes.
    pub gossip_interval: Duration,
    /// How long to wait for ping response.
    pub ping_timeout: Duration,
    /// How long a node stays in suspect state before being marked dead.
    pub suspect_timeout: Duration,
    /// How often to do full membership sync.
    pub sync_interval: Duration,
    /// Phi threshold for failure detection (8-12 typical).
    pub phi_threshold: f64,
    /// Number of nodes to ping each interval.
    pub fanout: usize,
}

impl Default for GossipConfig {
    fn default() -> Self {
        GossipConfig {
            gossip_interval: Duration::from_millis(200),
            ping_timeout: Duration::from_millis(500),
            suspect_timeout: Duration::from_secs(5),
            sync_interval: Duration::from_secs(30),
            phi_threshold: 8.0,
            fanout: 3,
        }
    }
}

/// Cluster membership state.
#[derive(Debug)]
pub struct Membership {
    pub cluster_id: String,
    pub local_node: NodeMeta,
    pub members: HashMap<NodeId, NodeMeta>,
    pub seed_nodes: Vec<SocketAddr>,
    /// Incarnation number (increases on refute).
    pub incarnation: u64,
    /// Sequence number for ping/ack correlation.
    pub sequence: u64,
}

impl Membership {
    /// Create a new membership state.
    pub fn new(cluster_id: String, local_node: NodeMeta, seed_nodes: Vec<SocketAddr>) -> Self {
        let node_id = local_node.node_id.clone();
        let mut members = HashMap::new();
        members.insert(node_id, local_node.clone());

        Membership {
            cluster_id,
            local_node,
            members,
            seed_nodes,
            incarnation: 0,
            sequence: 0,
        }
    }

    /// Get the next sequence number.
    pub fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Increment incarnation (used when refuting suspicion about self).
    pub fn increment_incarnation(&mut self) {
        self.incarnation += 1;
        self.local_node.incarnation = self.incarnation;
        if let Some(local) = self.members.get_mut(&self.local_node.node_id) {
            local.incarnation = self.incarnation;
        }
    }

    /// Get count of alive members.
    pub fn alive_count(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == NodeState::Alive)
            .count()
    }

    /// Get all alive member IDs except self.
    pub fn alive_peers(&self) -> Vec<&NodeId> {
        self.members
            .iter()
            .filter(|(id, m)| **id != self.local_node.node_id && m.state == NodeState::Alive)
            .map(|(id, _)| id)
            .collect()
    }

    /// Get a random subset of alive peers.
    pub fn random_alive_peers(&self, count: usize) -> Vec<&NodeId> {
        use rand::seq::SliceRandom;
        let mut peers = self.alive_peers();
        peers.shuffle(&mut rand::thread_rng());
        peers.into_iter().take(count).collect()
    }

    /// Update local node's leader info.
    pub fn set_leader(&mut self, leader_id: Option<NodeId>, lease_expires: Option<u64>) {
        self.local_node.leader_id = leader_id.clone();
        self.local_node.leader_lease_expires = lease_expires;
        if let Some(local) = self.members.get_mut(&self.local_node.node_id) {
            local.leader_id = leader_id;
            local.leader_lease_expires = lease_expires;
        }
    }

    /// Update local node's manifest version.
    pub fn set_manifest_version(&mut self, version: u64) {
        self.local_node.manifest_version = version;
        if let Some(local) = self.members.get_mut(&self.local_node.node_id) {
            local.manifest_version = version;
        }
    }
}

/// Pending ping state.
#[derive(Debug)]
struct PendingPing {
    target: NodeId,
    sent_at: Instant,
    /// True when this ping was initiated as an indirect probe (PINGREQ)
    indirect: bool,
    /// Gossip address for forwarding ACKs back to requester
    requester_addr: Option<SocketAddr>,
}

/// Gossip protocol implementation.
#[derive(Debug)]
pub struct GossipProtocol {
    pub config: GossipConfig,
    pub membership: Membership,
    failure_detectors: HashMap<NodeId, PhiAccrualDetector>,
    pending_pings: HashMap<u64, Vec<PendingPing>>,
    suspect_times: HashMap<NodeId, Instant>,
    last_sync: Instant,
    last_gossip: Instant,
}

impl GossipProtocol {
    /// Create a new gossip protocol instance.
    pub fn new(config: GossipConfig, membership: Membership) -> Self {
        GossipProtocol {
            config,
            membership,
            failure_detectors: HashMap::new(),
            pending_pings: HashMap::new(),
            suspect_times: HashMap::new(),
            last_sync: Instant::now(),
            last_gossip: Instant::now(),
        }
    }

    /// Main tick function - called periodically.
    ///
    /// Returns messages to send.
    pub fn tick(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let mut messages = Vec::new();
        let now = Instant::now();

        // Check if it's time to gossip
        if now.duration_since(self.last_gossip) >= self.config.gossip_interval {
            self.last_gossip = now;
            messages.extend(self.gossip_tick());
        }

        // Check pending pings for timeout
        messages.extend(self.check_ping_timeouts());

        // Promote suspects to dead
        self.check_suspect_timeouts();

        // Periodic full sync
        if now.duration_since(self.last_sync) >= self.config.sync_interval {
            self.last_sync = now;
            messages.extend(self.full_sync());
        }

        messages
    }

    fn gossip_tick(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let mut messages = Vec::new();

        // Select random nodes to ping - collect to owned Vec first
        let targets: Vec<NodeId> = self
            .membership
            .random_alive_peers(self.config.fanout)
            .into_iter()
            .cloned()
            .collect();

        for target_id in targets {
            if let Some(meta) = self.membership.members.get(&target_id) {
                let gossip_addr = meta.gossip_addr;
                let seq = self.membership.next_sequence();
                let msg = GossipMessage::Ping {
                    from: self.membership.local_node.node_id.clone(),
                    incarnation: self.membership.incarnation,
                    sequence: seq,
                };

                self.pending_pings
                    .entry(seq)
                    .or_default()
                    .push(PendingPing {
                        target: target_id.clone(),
                        sent_at: Instant::now(),
                        indirect: false,
                        requester_addr: None,
                    });

                messages.push((gossip_addr, msg));
            }
        }

        messages
    }

    fn check_ping_timeouts(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let mut messages = Vec::new();
        let now = Instant::now();
        let timeout = self.config.ping_timeout;

        let sequences: Vec<u64> = self.pending_pings.keys().cloned().collect();
        let mut suspects: Vec<NodeId> = Vec::new();

        for seq in sequences {
            if let Some(mut pings) = self.pending_pings.remove(&seq) {
                let mut survivors = Vec::new();

                for mut pending in pings.drain(..) {
                    if now.duration_since(pending.sent_at) <= timeout {
                        survivors.push(pending);
                        continue;
                    }

                    if !pending.indirect {
                        // Try indirect ping through other nodes
                        let peers = self.membership.random_alive_peers(2);
                        for peer_id in peers {
                            if peer_id != &pending.target {
                                if let Some(meta) = self.membership.members.get(peer_id) {
                                    let msg = GossipMessage::PingReq {
                                        from: self.membership.local_node.node_id.clone(),
                                        target: pending.target.clone(),
                                        sequence: seq,
                                    };
                                    messages.push((meta.gossip_addr, msg));
                                }
                            }
                        }

                        // Keep waiting for indirect ack
                        pending.indirect = true;
                        pending.sent_at = now;
                        survivors.push(pending);
                    } else {
                        // Indirect ping also failed - mark as suspect
                        suspects.push(pending.target.clone());
                    }
                }

                if !survivors.is_empty() {
                    self.pending_pings.insert(seq, survivors);
                }
            }
        }

        for node in suspects {
            self.mark_suspect(&node);
        }

        messages
    }

    fn check_suspect_timeouts(&mut self) {
        let now = Instant::now();
        let timeout = self.config.suspect_timeout;

        let to_mark_dead: Vec<NodeId> = self
            .suspect_times
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for node_id in to_mark_dead {
            self.mark_dead(&node_id);
        }
    }

    fn full_sync(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let mut messages = Vec::new();

        let members: Vec<NodeMeta> = self.membership.members.values().cloned().collect();
        let msg = GossipMessage::Sync {
            from: self.membership.local_node.node_id.clone(),
            members,
            cluster_id: self.membership.cluster_id.clone(),
        };

        // Send to all alive peers
        for (id, meta) in &self.membership.members {
            if id != &self.membership.local_node.node_id && meta.state == NodeState::Alive {
                messages.push((meta.gossip_addr, msg.clone()));
            }
        }

        messages
    }

    /// Handle incoming gossip message.
    ///
    /// Returns messages to send in response.
    pub fn handle_message(
        &mut self,
        from_addr: SocketAddr,
        msg: GossipMessage,
    ) -> Vec<(SocketAddr, GossipMessage)> {
        let mut responses = Vec::new();

        match msg {
            GossipMessage::Ping {
                from,
                incarnation,
                sequence,
            } => {
                self.update_node_seen(&from, incarnation);
                let ack = GossipMessage::Ack {
                    from: self.membership.local_node.node_id.clone(),
                    incarnation: self.membership.incarnation,
                    sequence,
                };
                responses.push((from_addr, ack));
            }

            GossipMessage::Ack {
                from,
                incarnation,
                sequence,
            } => {
                if let Some(pings) = self.pending_pings.get_mut(&sequence) {
                    if let Some(idx) = pings.iter().position(|p| p.target == from) {
                        let pending = pings.remove(idx);

                        if pings.is_empty() {
                            self.pending_pings.remove(&sequence);
                        }

                        self.mark_alive(&from, incarnation);

                        // Forward ACK back to the requester for indirect probes
                        if pending.indirect {
                            if let Some(addr) = pending.requester_addr {
                                let ack = GossipMessage::Ack {
                                    from,
                                    incarnation,
                                    sequence,
                                };
                                responses.push((addr, ack));
                            }
                        }
                    }
                }
            }

            GossipMessage::PingReq {
                from: _from,
                target,
                sequence,
            } => {
                // Forward ping to target
                if let Some(meta) = self.membership.members.get(&target) {
                    let ping = GossipMessage::Ping {
                        from: self.membership.local_node.node_id.clone(),
                        incarnation: self.membership.incarnation,
                        sequence,
                    };
                    responses.push((meta.gossip_addr, ping));

                    // Track that this is an indirect ping
                    self.pending_pings
                        .entry(sequence)
                        .or_default()
                        .push(PendingPing {
                            target: target.clone(),
                            sent_at: Instant::now(),
                            indirect: true,
                            requester_addr: Some(from_addr),
                        });
                }
            }

            GossipMessage::Sync {
                from: from_node,
                members,
                cluster_id,
            } => {
                if cluster_id == self.membership.cluster_id {
                    self.merge_membership(&from_node, members);
                }
            }

            GossipMessage::Join { node, cluster_id } => {
                if cluster_id == self.membership.cluster_id {
                    self.handle_join(node.clone());
                    // Send join ack with current membership
                    let members: Vec<NodeMeta> = self.membership.members.values().cloned().collect();
                    let ack = GossipMessage::JoinAck {
                        from: self.membership.local_node.node_id.clone(),
                        members,
                        leader_id: self.membership.local_node.leader_id.clone(),
                    };
                    responses.push((node.gossip_addr, ack));
                }
            }

            GossipMessage::JoinAck {
                from,
                members,
                leader_id,
            } => {
                self.merge_membership(&from, members);
                if let Some(leader) = leader_id {
                    // Update our knowledge of the leader
                    self.membership.local_node.leader_id = Some(leader);
                }
            }

            GossipMessage::Leave { node_id, incarnation } => {
                if let Some(meta) = self.membership.members.get_mut(&node_id) {
                    if incarnation >= meta.incarnation {
                        meta.state = NodeState::Leaving;
                        meta.incarnation = incarnation;
                    }
                }
            }

            GossipMessage::MetadataUpdate {
                from,
                manifest_version,
                leader_id,
                leader_lease_expires,
            } => {
                if let Some(meta) = self.membership.members.get_mut(&from) {
                    meta.manifest_version = manifest_version;
                    meta.leader_id = leader_id;
                    meta.leader_lease_expires = leader_lease_expires;
                }
            }

            GossipMessage::Election(_) => {
                // Election messages are handled separately by the election module
            }
        }

        responses
    }

    /// Create a join message for contacting seed nodes.
    pub fn create_join_message(&self) -> GossipMessage {
        GossipMessage::Join {
            node: self.membership.local_node.clone(),
            cluster_id: self.membership.cluster_id.clone(),
        }
    }

    /// Create a leave message for graceful shutdown.
    pub fn create_leave_message(&mut self) -> GossipMessage {
        self.membership.increment_incarnation();
        GossipMessage::Leave {
            node_id: self.membership.local_node.node_id.clone(),
            incarnation: self.membership.incarnation,
        }
    }

    fn update_node_seen(&mut self, node_id: &NodeId, incarnation: u64) {
        if let Some(meta) = self.membership.members.get_mut(node_id) {
            if incarnation >= meta.incarnation {
                meta.incarnation = incarnation;

                // Update failure detector
                self.failure_detectors
                    .entry(node_id.clone())
                    .or_insert_with(|| PhiAccrualDetector::new(self.config.phi_threshold))
                    .heartbeat();
            }
        }
    }

    fn mark_alive(&mut self, node_id: &NodeId, incarnation: u64) {
        if let Some(meta) = self.membership.members.get_mut(node_id) {
            if incarnation >= meta.incarnation {
                meta.state = NodeState::Alive;
                meta.incarnation = incarnation;
                self.suspect_times.remove(node_id);

                // Update failure detector
                self.failure_detectors
                    .entry(node_id.clone())
                    .or_insert_with(|| PhiAccrualDetector::new(self.config.phi_threshold))
                    .heartbeat();
            }
        }
    }

    fn mark_suspect(&mut self, node_id: &NodeId) {
        // Don't suspect ourselves
        if node_id == &self.membership.local_node.node_id {
            return;
        }

        if let Some(meta) = self.membership.members.get_mut(node_id) {
            if meta.state == NodeState::Alive {
                meta.state = NodeState::Suspect;
                self.suspect_times.insert(node_id.clone(), Instant::now());
            }
        }
    }

    fn mark_dead(&mut self, node_id: &NodeId) {
        if let Some(meta) = self.membership.members.get_mut(node_id) {
            meta.state = NodeState::Dead;
        }
        self.suspect_times.remove(node_id);
        self.failure_detectors.remove(node_id);
    }

    fn handle_join(&mut self, node: NodeMeta) {
        let node_id = node.node_id.clone();

        // Add new node or update existing
        self.membership.members.insert(node_id.clone(), node);

        // Initialize failure detector
        let mut detector = PhiAccrualDetector::new(self.config.phi_threshold);
        detector.heartbeat();
        self.failure_detectors.insert(node_id, detector);
    }

    fn merge_membership(&mut self, _from: &NodeId, members: Vec<NodeMeta>) {
        for member in members {
            if member.node_id == self.membership.local_node.node_id {
                // Skip ourselves
                continue;
            }

            match self.membership.members.get_mut(&member.node_id) {
                Some(existing) => {
                    // Update if incoming has higher incarnation
                    if member.incarnation > existing.incarnation {
                        *existing = member;
                    } else if member.incarnation == existing.incarnation {
                        // Same incarnation - prefer more severe state
                        if member.state as u8 > existing.state as u8 {
                            existing.state = member.state;
                        }
                    }
                }
                None => {
                    // New node
                    let node_id = member.node_id.clone();
                    self.membership.members.insert(node_id.clone(), member);

                    // Initialize failure detector
                    let mut detector = PhiAccrualDetector::new(self.config.phi_threshold);
                    detector.heartbeat();
                    self.failure_detectors.insert(node_id, detector);
                }
            }
        }
    }

    /// Get the current leader from cluster gossip.
    pub fn current_leader(&self) -> Option<&NodeId> {
        // Find the node with the latest leader info
        self.membership
            .members
            .values()
            .filter(|m| m.state == NodeState::Alive)
            .filter_map(|m| {
                m.leader_id.as_ref().and_then(|l| {
                    m.leader_lease_expires.and_then(|exp| {
                        if exp > current_timestamp_ms() {
                            Some(l)
                        } else {
                            None
                        }
                    })
                })
            })
            .next()
    }

    /// Get count of alive nodes.
    pub fn alive_node_count(&self) -> usize {
        self.membership.alive_count()
    }

    /// Get total node count (including dead/suspect).
    pub fn total_node_count(&self) -> usize {
        self.membership.members.len()
    }

    /// Get node address by ID.
    pub fn get_node_addr(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.membership.members.get(node_id).map(|m| m.rpc_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, port: u16) -> NodeMeta {
        let rpc: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let gossip: SocketAddr = format!("127.0.0.1:{}", port + 100).parse().unwrap();
        NodeMeta::new(NodeId::from_string(id), rpc, gossip)
    }

    #[test]
    fn test_membership_creation() {
        let node = make_node("node1", 8765);
        let membership = Membership::new("test-cluster".into(), node, vec![]);

        assert_eq!(membership.cluster_id, "test-cluster");
        assert_eq!(membership.alive_count(), 1);
    }

    #[test]
    fn test_join_message() {
        let node = make_node("node1", 8765);
        let membership = Membership::new("test-cluster".into(), node, vec![]);
        let gossip = GossipProtocol::new(GossipConfig::default(), membership);

        let msg = gossip.create_join_message();
        match msg {
            GossipMessage::Join { cluster_id, node } => {
                assert_eq!(cluster_id, "test-cluster");
                assert_eq!(node.node_id, NodeId::from_string("node1"));
            }
            _ => panic!("expected Join message"),
        }
    }

    #[test]
    fn test_handle_join() {
        let node1 = make_node("node1", 8765);
        let membership = Membership::new("test-cluster".into(), node1, vec![]);
        let mut gossip = GossipProtocol::new(GossipConfig::default(), membership);

        let node2 = make_node("node2", 8766);
        gossip.handle_join(node2);

        assert_eq!(gossip.membership.members.len(), 2);
        assert!(gossip
            .membership
            .members
            .contains_key(&NodeId::from_string("node2")));
    }

    #[test]
    fn pingreq_forwards_ack_to_requester() {
        let helper = make_node("helper", 8000);
        let mut membership = Membership::new("test-cluster".into(), helper.clone(), vec![]);

        let requester = make_node("requester", 8001);
        let target = make_node("target", 8002);

        membership
            .members
            .insert(requester.node_id.clone(), requester.clone());
        membership
            .members
            .insert(target.node_id.clone(), target.clone());

        let mut gossip = GossipProtocol::new(GossipConfig::default(), membership);

        let seq = 42;
        let responses = gossip.handle_message(
            requester.gossip_addr,
            GossipMessage::PingReq {
                from: requester.node_id.clone(),
                target: target.node_id.clone(),
                sequence: seq,
            },
        );

        assert!(responses.iter().any(|(addr, msg)| match msg {
            GossipMessage::Ping { sequence, .. } => *sequence == seq && *addr == target.gossip_addr,
            _ => false,
        }));

        let responses_ack = gossip.handle_message(
            target.gossip_addr,
            GossipMessage::Ack {
                from: target.node_id.clone(),
                incarnation: 1,
                sequence: seq,
            },
        );

        assert!(responses_ack.iter().any(|(addr, msg)| match msg {
            GossipMessage::Ack { from, sequence, .. } => {
                *from == target.node_id && *sequence == seq && *addr == requester.gossip_addr
            }
            _ => false,
        }));

        assert!(gossip.pending_pings.is_empty());
    }

    #[test]
    fn pingreq_sequence_collisions_do_not_clobber_existing_pings() {
        let helper = make_node("helper", 8100);
        let mut membership = Membership::new("test-cluster".into(), helper.clone(), vec![]);

        let requester = make_node("requester", 8101);
        let target_direct = make_node("direct", 8102);
        let target_indirect = make_node("indirect", 8103);

        membership
            .members
            .insert(requester.node_id.clone(), requester.clone());
        membership
            .members
            .insert(target_direct.node_id.clone(), target_direct.clone());
        membership
            .members
            .insert(target_indirect.node_id.clone(), target_indirect.clone());

        let mut gossip = GossipProtocol::new(GossipConfig::default(), membership);

        // Seed a direct ping in flight with sequence 7
        let seq = 7u64;
        gossip.pending_pings.insert(
            seq,
            vec![PendingPing {
                target: target_direct.node_id.clone(),
                sent_at: Instant::now() - gossip.config.ping_timeout - Duration::from_millis(1),
                indirect: false,
                requester_addr: None,
            }],
        );

        // Receive a PingReq reusing the same sequence for a different target
        let responses = gossip.handle_message(
            requester.gossip_addr,
            GossipMessage::PingReq {
                from: requester.node_id.clone(),
                target: target_indirect.node_id.clone(),
                sequence: seq,
            },
        );

        // Should forward ping to indirect target
        assert!(responses.iter().any(|(addr, msg)| match msg {
            GossipMessage::Ping { sequence, .. } => *sequence == seq && *addr == target_indirect.gossip_addr,
            _ => false,
        }));

        // Timeout processing should move the direct ping to indirect probes without dropping the new entry
        let _ = gossip.check_ping_timeouts();
        assert_eq!(gossip.pending_pings.len(), 1);
        let entries = gossip.pending_pings.get(&seq).unwrap();
        assert_eq!(entries.len(), 2);

        // Ack from indirect target should be forwarded to requester while keeping the direct probe entry intact
        let responses_ack = gossip.handle_message(
            target_indirect.gossip_addr,
            GossipMessage::Ack {
                from: target_indirect.node_id.clone(),
                incarnation: 1,
                sequence: seq,
            },
        );

        assert!(responses_ack.iter().any(|(addr, msg)| match msg {
            GossipMessage::Ack { from, sequence, .. } => {
                *from == target_indirect.node_id && *sequence == seq && *addr == requester.gossip_addr
            }
            _ => false,
        }));

        let remaining = gossip.pending_pings.get(&seq).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].target, target_direct.node_id);
        assert!(remaining[0].indirect);
    }
}
