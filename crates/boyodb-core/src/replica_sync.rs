//! Read Replica Synchronization Module
//!
//! Provides manifest synchronization for read replicas. Supports two modes:
//! 1. Shared S3: Replica refreshes manifest from shared S3 bucket
//! 2. HTTP Bundle Pull: Replica fetches bundles from primary via HTTP API

use crate::engine::{Db, EngineError};
use crate::replication::{BundlePayload, Manifest};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Configuration for replica synchronization
#[derive(Clone, Debug)]
pub struct ReplicaSyncConfig {
    /// Interval between sync attempts in milliseconds
    pub sync_interval_ms: u64,
    /// Primary server address for HTTP bundle pull (e.g., "http://primary:8765")
    /// If None, uses shared S3 manifest refresh
    pub primary_addr: Option<String>,
    /// Authentication token for primary server (if required)
    pub primary_token: Option<String>,
    /// Maximum bytes to fetch per sync cycle
    pub max_bytes_per_sync: Option<u64>,
    /// Use TLS for primary connection
    pub use_tls: bool,
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Read timeout in milliseconds
    pub read_timeout_ms: u64,
}

impl Default for ReplicaSyncConfig {
    fn default() -> Self {
        Self {
            sync_interval_ms: 1000,
            primary_addr: None,
            primary_token: None,
            max_bytes_per_sync: Some(64 * 1024 * 1024), // 64MB default
            use_tls: false,
            connect_timeout_ms: 5000,
            read_timeout_ms: 30000,
        }
    }
}

/// Metrics for replica synchronization
#[derive(Debug, Default)]
pub struct ReplicaSyncMetrics {
    /// Total number of successful sync cycles
    pub syncs_completed: AtomicU64,
    /// Total number of failed sync cycles
    pub syncs_failed: AtomicU64,
    /// Total bytes synced from primary
    pub bytes_synced: AtomicU64,
    /// Total segments synced
    pub segments_synced: AtomicU64,
    /// Last successful sync timestamp (unix millis)
    pub last_sync_millis: AtomicU64,
    /// Last sync lag in milliseconds (time since primary's last update)
    pub last_sync_lag_ms: AtomicU64,
    /// Current manifest version on replica
    pub manifest_version: AtomicU64,
}

impl ReplicaSyncMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self, bytes: u64, segments: u64, manifest_version: u64) {
        self.syncs_completed.fetch_add(1, Ordering::Relaxed);
        self.bytes_synced.fetch_add(bytes, Ordering::Relaxed);
        self.segments_synced.fetch_add(segments, Ordering::Relaxed);
        self.manifest_version.store(manifest_version, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_sync_millis.store(now, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.syncs_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_lag(&self, lag_ms: u64) {
        self.last_sync_lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReplicaSyncMetricsSnapshot {
        ReplicaSyncMetricsSnapshot {
            syncs_completed: self.syncs_completed.load(Ordering::Relaxed),
            syncs_failed: self.syncs_failed.load(Ordering::Relaxed),
            bytes_synced: self.bytes_synced.load(Ordering::Relaxed),
            segments_synced: self.segments_synced.load(Ordering::Relaxed),
            last_sync_millis: self.last_sync_millis.load(Ordering::Relaxed),
            last_sync_lag_ms: self.last_sync_lag_ms.load(Ordering::Relaxed),
            manifest_version: self.manifest_version.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of replica sync metrics for reporting
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicaSyncMetricsSnapshot {
    pub syncs_completed: u64,
    pub syncs_failed: u64,
    pub bytes_synced: u64,
    pub segments_synced: u64,
    pub last_sync_millis: u64,
    pub last_sync_lag_ms: u64,
    pub manifest_version: u64,
}

/// Background worker for replica synchronization
pub struct ReplicaSyncWorker {
    db: Arc<Db>,
    cfg: ReplicaSyncConfig,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<ReplicaSyncMetrics>,
}

impl ReplicaSyncWorker {
    pub fn new(db: Arc<Db>, cfg: ReplicaSyncConfig) -> Self {
        Self {
            db,
            cfg,
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(ReplicaSyncMetrics::new()),
        }
    }

    /// Get a reference to the sync metrics
    pub fn metrics(&self) -> &ReplicaSyncMetrics {
        &self.metrics
    }

    /// Get a clone of the metrics Arc for sharing
    pub fn metrics_arc(&self) -> Arc<ReplicaSyncMetrics> {
        self.metrics.clone()
    }

    /// Signal the worker to shut down gracefully
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        info!("replica_sync: shutdown requested");
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Run the sync loop until shutdown is requested
    pub async fn run(&self) {
        info!(
            sync_interval_ms = self.cfg.sync_interval_ms,
            primary_addr = ?self.cfg.primary_addr,
            "replica_sync: starting sync worker"
        );

        let mut ticker = interval(Duration::from_millis(self.cfg.sync_interval_ms));
        loop {
            ticker.tick().await;

            if self.is_shutdown() {
                info!("replica_sync: shutting down");
                break;
            }

            self.run_sync_cycle().await;
        }
    }

    /// Run a single sync cycle
    pub async fn run_sync_cycle(&self) {
        let result = if self.cfg.primary_addr.is_some() {
            self.sync_from_primary().await
        } else {
            self.sync_from_s3().await
        };

        match result {
            Ok((bytes, segments, version)) => {
                if segments > 0 {
                    info!(
                        bytes = bytes,
                        segments = segments,
                        version = version,
                        "replica_sync: sync cycle completed"
                    );
                } else {
                    debug!("replica_sync: no new segments to sync");
                }
                self.metrics.record_success(bytes, segments, version);
            }
            Err(e) => {
                error!(error = %e, "replica_sync: sync cycle failed");
                self.metrics.record_failure();
            }
        }
    }

    /// Sync manifest from shared S3 storage
    async fn sync_from_s3(&self) -> Result<(u64, u64, u64), EngineError> {
        // Reload manifest from storage
        let (bytes, segments, version) = {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || db.reload_manifest_from_storage())
                .await
                .map_err(|e| EngineError::Internal(format!("sync task join error: {}", e)))??
        };

        Ok((bytes, segments, version))
    }

    /// Sync bundles from primary server via HTTP
    /// Note: HTTP sync requires the `tokio` and standard library networking.
    /// For production use, consider using a proper HTTP client crate.
    async fn sync_from_primary(&self) -> Result<(u64, u64, u64), EngineError> {
        let primary_addr = self.cfg.primary_addr.as_ref().ok_or_else(|| {
            EngineError::Configuration("primary_addr not configured".into())
        })?;

        // Get current manifest version to request only new entries
        let current_version = self.db.manifest_version();

        // Build request
        let mut request = serde_json::json!({
            "op": "export_bundle",
            "since_version": current_version,
            "max_bytes": self.cfg.max_bytes_per_sync,
            "prefer_hot": true
        });

        // Add auth token if configured
        if let Some(ref token) = self.cfg.primary_token {
            request["auth"] = serde_json::Value::String(token.clone());
        }

        // Parse the primary address to get host and port
        let url = primary_addr.strip_prefix("http://").unwrap_or(primary_addr);
        let url = url.strip_prefix("https://").unwrap_or(url);

        // Make HTTP request using tokio's TcpStream
        let connect_timeout = Duration::from_millis(self.cfg.connect_timeout_ms);
        let read_timeout = Duration::from_millis(self.cfg.read_timeout_ms);

        let stream = tokio::time::timeout(
            connect_timeout,
            tokio::net::TcpStream::connect(url),
        )
        .await
        .map_err(|_| EngineError::Remote("connection timeout".into()))?
        .map_err(|e| EngineError::Remote(format!("connection failed: {}", e)))?;

        // Simple HTTP/1.1 request
        let request_body = serde_json::to_string(&request)
            .map_err(|e| EngineError::Internal(format!("JSON serialization failed: {}", e)))?;

        let http_request = format!(
            "POST / HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            url, request_body.len(), request_body
        );

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = stream;
        stream.write_all(http_request.as_bytes()).await
            .map_err(|e| EngineError::Remote(format!("write failed: {}", e)))?;

        // Read response with timeout
        let mut response_buf = Vec::new();
        tokio::time::timeout(read_timeout, async {
            stream.read_to_end(&mut response_buf).await
        })
        .await
        .map_err(|_| EngineError::Remote("read timeout".into()))?
        .map_err(|e| EngineError::Remote(format!("read failed: {}", e)))?;

        let response_str = String::from_utf8_lossy(&response_buf);

        // Parse HTTP response (simple parsing)
        let body_start = response_str.find("\r\n\r\n").map(|i| i + 4)
            .ok_or_else(|| EngineError::Remote("invalid HTTP response".into()))?;
        let body_str = &response_str[body_start..];

        let body: serde_json::Value = serde_json::from_str(body_str)
            .map_err(|e| EngineError::Remote(format!("failed to parse response: {}", e)))?;

        // Check for errors
        if body.get("status").and_then(|v: &serde_json::Value| v.as_str()) == Some("error") {
            let msg = body
                .get("message")
                .and_then(|v: &serde_json::Value| v.as_str())
                .unwrap_or("unknown error");
            return Err(EngineError::Remote(format!("primary error: {}", msg)));
        }

        // Parse bundle
        let bundle_json = body.get("bundle_json");
        if bundle_json.is_none() || bundle_json == Some(&serde_json::Value::Null) {
            // No new data to sync
            return Ok((0, 0, current_version));
        }

        let payload: BundlePayload = serde_json::from_value(bundle_json.unwrap().clone())
            .map_err(|e| EngineError::Remote(format!("failed to parse bundle: {}", e)))?;

        let bytes: u64 = payload.segments.iter().map(|s| s.data.len() as u64).sum();
        let segments = payload.segments.len() as u64;
        let new_version = payload.plan.manifest_version;

        // Apply the bundle
        {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || db.apply_bundle(payload))
                .await
                .map_err(|e| EngineError::Internal(format!("apply bundle join error: {}", e)))??;
        }

        Ok((bytes, segments, new_version))
    }
}

/// Spawn a replica sync worker in the background
pub fn spawn_replica_sync_worker(
    db: Arc<Db>,
    cfg: ReplicaSyncConfig,
) -> (Arc<ReplicaSyncWorker>, tokio::task::JoinHandle<()>) {
    let worker = Arc::new(ReplicaSyncWorker::new(db, cfg));
    let worker_clone = worker.clone();
    let handle = tokio::spawn(async move {
        worker_clone.run().await;
    });
    (worker, handle)
}
