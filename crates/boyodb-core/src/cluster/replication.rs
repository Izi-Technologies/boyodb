//! Write replication between cluster nodes.
//!
//! This module provides TCP-based write replication from leader to followers.

use crate::cluster::messages::{
    current_timestamp_ms, NodeId, ReplicationMessage, WriteAck, WriteOperation, WritePayload,
};
use crate::engine::{Db, EngineError};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Maximum message size for replication (64MB).
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Replication coordinator running on the leader.
pub struct ReplicationCoordinator {
    _node_id: NodeId,
    /// Connections to follower nodes.
    followers: HashMap<NodeId, FollowerConnection>,
    /// Channel to receive write acknowledgments.
    ack_rx: mpsc::Receiver<WriteAck>,
    /// Sender for write acknowledgments.
    ack_tx: mpsc::Sender<WriteAck>,
}

struct FollowerConnection {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl ReplicationCoordinator {
    /// Create a new replication coordinator.
    pub fn new(node_id: NodeId) -> Self {
        let (ack_tx, ack_rx) = mpsc::channel(1000);
        ReplicationCoordinator {
            _node_id: node_id,
            followers: HashMap::new(),
            ack_rx,
            ack_tx,
        }
    }

    /// Add a follower to replicate to.
    pub fn add_follower(&mut self, node_id: NodeId, addr: SocketAddr) {
        self.followers
            .insert(node_id, FollowerConnection { addr, stream: None });
    }

    /// Remove a follower.
    pub fn remove_follower(&mut self, node_id: &NodeId) {
        self.followers.remove(node_id);
    }

    /// Replicate a write operation to all followers.
    /// Returns the number of successful replications.
    pub async fn replicate_write(&mut self, operation: WriteOperation) -> usize {
        let msg = ReplicationMessage::WriteRequest {
            operation: operation.clone(),
        };
        let data = match serde_json::to_vec(&msg) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("failed to serialize replication message: {}", e);
                return 0;
            }
        };

        let mut success_count = 0;

        for (follower_id, conn) in self.followers.iter_mut() {
            if let Err(e) = send_to_follower(conn, &data).await {
                tracing::warn!("failed to replicate to follower {}: {}", follower_id, e);
            } else {
                success_count += 1;
            }
        }

        success_count
    }

    /// Wait for acknowledgments from followers.
    /// Returns when quorum is reached or timeout expires.
    pub async fn wait_for_acks(
        &mut self,
        write_id: u64,
        required: usize,
        timeout: Duration,
    ) -> Vec<WriteAck> {
        let mut acks = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;

        while acks.len() < required && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, self.ack_rx.recv()).await {
                Ok(Some(ack)) if ack.write_id == write_id => {
                    acks.push(ack);
                }
                Ok(Some(_)) => {
                    // Ack for different write, ignore
                }
                Ok(None) | Err(_) => break,
            }
        }

        acks
    }

    /// Get the ack sender for external use.
    pub fn ack_sender(&self) -> mpsc::Sender<WriteAck> {
        self.ack_tx.clone()
    }
}

async fn send_to_follower(conn: &mut FollowerConnection, data: &[u8]) -> Result<(), EngineError> {
    // Ensure we have a connection
    if conn.stream.is_none() {
        let stream = TcpStream::connect(conn.addr)
            .await
            .map_err(|e| EngineError::Io(format!("connect to follower: {}", e)))?;
        conn.stream = Some(stream);
    }

    let stream = conn.stream.as_mut().unwrap();

    // Send length-prefixed message
    let len = data.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| EngineError::Io(format!("write length: {}", e)))?;
    stream
        .write_all(data)
        .await
        .map_err(|e| EngineError::Io(format!("write data: {}", e)))?;
    stream
        .flush()
        .await
        .map_err(|e| EngineError::Io(format!("flush: {}", e)))?;

    Ok(())
}

/// Replication handler running on followers.
pub struct ReplicationHandler {
    node_id: NodeId,
    db: Arc<Db>,
}

impl ReplicationHandler {
    /// Create a new replication handler.
    pub fn new(node_id: NodeId, db: Arc<Db>) -> Self {
        ReplicationHandler { node_id, db }
    }

    /// Apply a replicated write operation.
    pub async fn apply_write(&self, operation: WriteOperation) -> WriteAck {
        let result = self.apply_write_internal(&operation).await;

        // Get manifest version - default to 0 if we can't read it
        let manifest_version = self.db.get_manifest_version().unwrap_or(0);

        WriteAck {
            write_id: operation.id,
            node_id: self.node_id.clone(),
            success: result.is_ok(),
            error: result.err().map(|e| e.to_string()),
            manifest_version,
        }
    }

    async fn apply_write_internal(&self, operation: &WriteOperation) -> Result<(), EngineError> {
        let db = self.db.clone();

        match &operation.payload {
            WritePayload::Ingest {
                ipc_data,
                watermark_micros,
            } => {
                let database = operation.database.clone();
                let table = operation.table.clone();
                let ipc_data = ipc_data.clone();
                let watermark = *watermark_micros;

                tokio::task::spawn_blocking(move || {
                    db.ingest(&database, &table, &ipc_data, watermark)
                })
                .await
                .map_err(|e| EngineError::Internal(format!("spawn blocking failed: {}", e)))?
            }

            WritePayload::CreateDatabase { name } => {
                let name = name.clone();
                tokio::task::spawn_blocking(move || db.create_database(&name))
                    .await
                    .map_err(|e| EngineError::Internal(format!("spawn blocking failed: {}", e)))?
            }

            WritePayload::CreateTable {
                database,
                table,
                schema_json,
            } => {
                let database = database.clone();
                let table = table.clone();
                let schema_json = schema_json.clone();
                tokio::task::spawn_blocking(move || db.create_table(&database, &table, schema_json))
                    .await
                    .map_err(|e| EngineError::Internal(format!("spawn blocking failed: {}", e)))?
            }

            WritePayload::DropTable { database, table } => {
                let database = database.clone();
                let table = table.clone();
                tokio::task::spawn_blocking(move || db.drop_table(&database, &table, false))
                    .await
                    .map_err(|e| EngineError::Internal(format!("spawn blocking failed: {}", e)))?
            }

            WritePayload::DropDatabase { name } => {
                let name = name.clone();
                tokio::task::spawn_blocking(move || db.drop_database(&name, false))
                    .await
                    .map_err(|e| EngineError::Internal(format!("spawn blocking failed: {}", e)))?
            }

            WritePayload::Delete { sql } | WritePayload::Update { sql } => {
                // These would need SQL execution - simplified for now
                tracing::warn!("replicated DELETE/UPDATE not fully implemented: {}", sql);
                Ok(())
            }
        }
    }

    /// Export a bundle for sync starting from the provided manifest version.
    async fn export_bundle_from(&self, last_version: u64) -> Result<(Vec<u8>, u64), EngineError> {
        let req = crate::BundleRequest {
            max_bytes: Some(8 * 1024 * 1024),
            since_version: Some(last_version),
            prefer_hot: true,
            target_bytes_per_sec: None,
            max_entries: None,
        };

        let payload = self.db.export_bundle(req)?;
        let to_version = payload.plan.manifest_version;
        let data = serde_json::to_vec(&payload)
            .map_err(|e| EngineError::Internal(format!("serialize bundle: {}", e)))?;
        Ok((data, to_version))
    }

    /// Apply a received bundle payload.
    async fn apply_bundle_payload(&self, data: &[u8]) -> Result<(), EngineError> {
        if data.is_empty() {
            return Ok(());
        }

        let payload: crate::BundlePayload = serde_json::from_slice(data)
            .map_err(|e| EngineError::InvalidArgument(format!("parse bundle: {}", e)))?;

        let current = self.db.get_manifest_version().unwrap_or(0);
        if current >= payload.plan.manifest_version {
            // Already at or ahead of this version; nothing to apply.
            return Ok(());
        }

        tokio::task::spawn_blocking({
            let db = self.db.clone();
            move || db.apply_bundle(payload)
        })
        .await
        .map_err(|e| EngineError::Internal(format!("apply bundle join error: {}", e)))?
    }
}

/// Start the replication TCP listener on a follower.
pub async fn start_replication_listener(
    addr: SocketAddr,
    handler: Arc<ReplicationHandler>,
    ack_tx: mpsc::Sender<WriteAck>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), EngineError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| EngineError::Io(format!("bind replication listener: {}", e)))?;

    tracing::info!("replication listener started on {}", addr);

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                tracing::debug!("replication connection from {}", peer_addr);
                let handler = handler.clone();
                let ack_tx = ack_tx.clone();
                let shutdown = shutdown.clone();

                tokio::spawn(async move {
                    if let Err(e) =
                        handle_replication_connection(stream, handler, ack_tx, shutdown).await
                    {
                        tracing::warn!("replication connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                tracing::warn!("replication accept error: {}", e);
            }
        }
    }

    Ok(())
}

async fn handle_replication_connection(
    mut stream: TcpStream,
    handler: Arc<ReplicationHandler>,
    ack_tx: mpsc::Sender<WriteAck>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), EngineError> {
    let mut len_buf = [0u8; 4];

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }

        // Read message length
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(EngineError::Io(format!("read length: {}", e))),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_MESSAGE_SIZE {
            return Err(EngineError::InvalidArgument(format!(
                "message too large: {} bytes",
                len
            )));
        }

        // Read message data
        let mut data = vec![0u8; len];
        stream
            .read_exact(&mut data)
            .await
            .map_err(|e| EngineError::Io(format!("read data: {}", e)))?;

        // Parse and handle message
        let msg: ReplicationMessage = serde_json::from_slice(&data)
            .map_err(|e| EngineError::InvalidArgument(format!("parse message: {}", e)))?;

        match msg {
            ReplicationMessage::WriteRequest { operation } => {
                let ack = handler.apply_write(operation).await;

                // Send ack back
                let ack_msg = ReplicationMessage::WriteResponse { ack: ack.clone() };
                let ack_data = serde_json::to_vec(&ack_msg)
                    .map_err(|e| EngineError::Internal(format!("serialize ack: {}", e)))?;

                let len = ack_data.len() as u32;
                stream
                    .write_all(&len.to_be_bytes())
                    .await
                    .map_err(|e| EngineError::Io(format!("write ack length: {}", e)))?;
                stream
                    .write_all(&ack_data)
                    .await
                    .map_err(|e| EngineError::Io(format!("write ack: {}", e)))?;

                // Also send to coordinator channel
                let _ = ack_tx.send(ack).await;
            }
            ReplicationMessage::WriteResponse { ack } => {
                // This shouldn't happen on the follower side
                tracing::warn!("unexpected WriteResponse on follower");
                let _ = ack_tx.send(ack).await;
            }
            ReplicationMessage::SyncRequest {
                from: _from,
                last_version,
            } => {
                let (bundle_data, to_version) = match handler.export_bundle_from(last_version).await
                {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!(error = %e, last_version, "sync request failed");
                        let current = handler.db.get_manifest_version().unwrap_or(last_version);
                        (Vec::new(), current)
                    }
                };

                let resp = ReplicationMessage::SyncResponse {
                    bundle_data,
                    from_version: last_version,
                    to_version,
                };

                let resp_data = serde_json::to_vec(&resp).map_err(|e| {
                    EngineError::Internal(format!("serialize sync response: {}", e))
                })?;

                let len = resp_data.len() as u32;
                stream
                    .write_all(&len.to_be_bytes())
                    .await
                    .map_err(|e| EngineError::Io(format!("write sync length: {}", e)))?;
                stream
                    .write_all(&resp_data)
                    .await
                    .map_err(|e| EngineError::Io(format!("write sync response: {}", e)))?;
            }
            ReplicationMessage::SyncResponse {
                bundle_data,
                from_version,
                to_version,
            } => {
                tracing::info!(
                    from_version,
                    to_version,
                    size = bundle_data.len(),
                    "received sync response"
                );
                if let Err(e) = handler.apply_bundle_payload(&bundle_data).await {
                    tracing::warn!(error = %e, "failed to apply sync response");
                }
            }
        }
    }

    Ok(())
}

/// Create a write operation for replication.
pub fn create_write_operation(
    id: u64,
    term: u64,
    fencing_token: u64,
    database: &str,
    table: &str,
    payload: WritePayload,
) -> WriteOperation {
    WriteOperation {
        id,
        term,
        fencing_token,
        database: database.to_string(),
        table: table.to_string(),
        payload,
        timestamp: current_timestamp_ms(),
    }
}

// ============================================================================
// Unified Replication State for Server Integration
// ============================================================================

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Unified replication state that manages both leader (coordinator) and follower (handler) roles.
/// This is the main integration point for server-level write replication.
pub struct ReplicationState {
    /// The replication coordinator (used when this node is the leader).
    /// Uses tokio Mutex to allow holding across await points.
    coordinator: tokio::sync::Mutex<Option<ReplicationCoordinator>>,
    /// The replication handler (used when this node is a follower).
    handler: Option<Arc<ReplicationHandler>>,
    /// Node ID for this server.
    node_id: NodeId,
    /// Database instance.
    db: Arc<Db>,
    /// Current election term for fencing.
    current_term: AtomicU64,
    /// Fencing token for write validation.
    fencing_token: AtomicU64,
    /// Write ID counter for unique operation IDs.
    write_id_counter: AtomicU64,
    /// Replication listener address.
    replication_addr: Option<SocketAddr>,
    /// Shutdown flag.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl ReplicationState {
    /// Create a new replication state.
    pub fn new(node_id: NodeId, db: Arc<Db>, replication_addr: Option<SocketAddr>) -> Self {
        let handler = if replication_addr.is_some() {
            Some(Arc::new(ReplicationHandler::new(
                node_id.clone(),
                db.clone(),
            )))
        } else {
            None
        };

        ReplicationState {
            coordinator: tokio::sync::Mutex::new(None),
            handler,
            node_id,
            db,
            current_term: AtomicU64::new(0),
            fencing_token: AtomicU64::new(0),
            write_id_counter: AtomicU64::new(0),
            replication_addr,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Called when this node becomes the leader.
    pub async fn become_leader(&self, term: u64) {
        tracing::info!(
            term,
            "node becoming leader, initializing replication coordinator"
        );
        self.current_term.store(term, AtomicOrdering::SeqCst);
        self.fencing_token.fetch_add(1, AtomicOrdering::SeqCst);

        let mut coordinator = self.coordinator.lock().await;
        *coordinator = Some(ReplicationCoordinator::new(self.node_id.clone()));
    }

    /// Called when this node becomes a follower.
    pub async fn become_follower(&self, term: u64) {
        tracing::info!(term, "node becoming follower, clearing coordinator");
        self.current_term.store(term, AtomicOrdering::SeqCst);

        let mut coordinator = self.coordinator.lock().await;
        *coordinator = None;
    }

    /// Add a follower to replicate to (leader only).
    pub async fn add_follower(&self, follower_id: NodeId, addr: SocketAddr) {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(ref mut coord) = *coordinator {
            coord.add_follower(follower_id, addr);
        }
    }

    /// Remove a follower (leader only).
    pub async fn remove_follower(&self, follower_id: &NodeId) {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(ref mut coord) = *coordinator {
            coord.remove_follower(follower_id);
        }
    }

    /// Get the next write ID.
    fn next_write_id(&self) -> u64 {
        self.write_id_counter.fetch_add(1, AtomicOrdering::SeqCst)
    }

    /// Replicate an ingest operation to followers.
    pub async fn replicate_ingest(
        &self,
        database: &str,
        table: &str,
        ipc_data: &[u8],
        watermark_micros: u64,
    ) -> Result<usize, EngineError> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(ref mut coord) = *coordinator {
            let operation = create_write_operation(
                self.next_write_id(),
                self.current_term.load(AtomicOrdering::SeqCst),
                self.fencing_token.load(AtomicOrdering::SeqCst),
                database,
                table,
                WritePayload::Ingest {
                    ipc_data: ipc_data.to_vec(),
                    watermark_micros,
                },
            );
            Ok(coord.replicate_write(operation).await)
        } else {
            // Not a leader, no replication needed
            Ok(0)
        }
    }

    /// Replicate a DDL operation (CREATE DATABASE, CREATE TABLE, etc.).
    pub async fn replicate_ddl(
        &self,
        database: &str,
        table: &str,
        payload: WritePayload,
    ) -> Result<usize, EngineError> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(ref mut coord) = *coordinator {
            let operation = create_write_operation(
                self.next_write_id(),
                self.current_term.load(AtomicOrdering::SeqCst),
                self.fencing_token.load(AtomicOrdering::SeqCst),
                database,
                table,
                payload,
            );
            Ok(coord.replicate_write(operation).await)
        } else {
            Ok(0)
        }
    }

    /// Check if this node is currently the leader (has a coordinator).
    pub async fn is_leader(&self) -> bool {
        self.coordinator.lock().await.is_some()
    }

    /// Get the replication handler for follower mode.
    pub fn handler(&self) -> Option<Arc<ReplicationHandler>> {
        self.handler.clone()
    }

    /// Get the shutdown flag.
    pub fn shutdown_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.shutdown.clone()
    }

    /// Start the replication listener (for followers).
    pub async fn start_listener(&self, ack_tx: mpsc::Sender<WriteAck>) -> Result<(), EngineError> {
        if let (Some(addr), Some(ref handler)) = (self.replication_addr, &self.handler) {
            start_replication_listener(addr, handler.clone(), ack_tx, self.shutdown.clone()).await
        } else {
            Ok(())
        }
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_write_operation() {
        let op = create_write_operation(
            1,
            5,
            100,
            "testdb",
            "testtable",
            WritePayload::CreateDatabase {
                name: "testdb".to_string(),
            },
        );

        assert_eq!(op.id, 1);
        assert_eq!(op.term, 5);
        assert_eq!(op.database, "testdb");
    }
}
