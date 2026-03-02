use crate::Db;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

/// Configuration for the replication worker
#[derive(Clone, Debug)]
pub struct ReplicatorConfig {
    /// Interval between replication attempts in milliseconds
    pub interval_millis: u64,
    /// Maximum bytes to include in a single bundle
    pub max_bytes: Option<u64>,
    /// Only replicate entries added after this manifest version
    pub since_version: Option<u64>,
    /// Target replication throughput in bytes per second
    pub target_bytes_per_sec: Option<u64>,
    /// Whether to prioritize hot tier segments
    pub prefer_hot: bool,
    /// Maximum number of entries per bundle
    pub max_entries: Option<usize>,
    /// Output directory for bundle files (if None, uses /tmp)
    pub output_dir: Option<PathBuf>,
}

impl Default for ReplicatorConfig {
    fn default() -> Self {
        Self {
            interval_millis: 30_000,
            max_bytes: Some(4 * 1024 * 1024),
            since_version: None,
            target_bytes_per_sec: Some(512 * 1024),
            prefer_hot: true,
            max_entries: None,
            output_dir: None,
        }
    }
}

/// Metrics for the replication worker
#[derive(Debug, Default)]
pub struct ReplicatorMetrics {
    /// Total number of successful replication cycles
    pub cycles_completed: AtomicU64,
    /// Total number of failed replication cycles
    pub cycles_failed: AtomicU64,
    /// Total bytes replicated
    pub bytes_replicated: AtomicU64,
    /// Total entries replicated
    pub entries_replicated: AtomicU64,
    /// Last successful replication timestamp (unix millis)
    pub last_success_millis: AtomicU64,
}

impl ReplicatorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self, bytes: u64, entries: u64) {
        self.cycles_completed.fetch_add(1, Ordering::Relaxed);
        self.bytes_replicated.fetch_add(bytes, Ordering::Relaxed);
        self.entries_replicated
            .fetch_add(entries, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_success_millis.store(now, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.cycles_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReplicatorMetricsSnapshot {
        ReplicatorMetricsSnapshot {
            cycles_completed: self.cycles_completed.load(Ordering::Relaxed),
            cycles_failed: self.cycles_failed.load(Ordering::Relaxed),
            bytes_replicated: self.bytes_replicated.load(Ordering::Relaxed),
            entries_replicated: self.entries_replicated.load(Ordering::Relaxed),
            last_success_millis: self.last_success_millis.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of replicator metrics
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicatorMetricsSnapshot {
    pub cycles_completed: u64,
    pub cycles_failed: u64,
    pub bytes_replicated: u64,
    pub entries_replicated: u64,
    pub last_success_millis: u64,
}

/// Replication worker that periodically creates bundles for shipping to replicas
#[derive(Clone)]
pub struct Replicator {
    db: Arc<Db>,
    cfg: ReplicatorConfig,
    in_progress: Arc<Mutex<()>>,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<ReplicatorMetrics>,
}

impl Replicator {
    pub fn new(db: Arc<Db>, cfg: ReplicatorConfig) -> Self {
        Self {
            db,
            cfg,
            in_progress: Arc::new(Mutex::new(())),
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(ReplicatorMetrics::new()),
        }
    }

    /// Get a reference to the replicator metrics
    pub fn metrics(&self) -> &ReplicatorMetrics {
        &self.metrics
    }

    /// Signal the replicator to shut down gracefully
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        info!("replicator: shutdown requested");
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Run the replication loop until shutdown is requested
    pub async fn run(&self) {
        info!(
            interval_ms = self.cfg.interval_millis,
            max_bytes = ?self.cfg.max_bytes,
            prefer_hot = self.cfg.prefer_hot,
            "replicator: starting"
        );

        let mut ticker = interval(Duration::from_millis(self.cfg.interval_millis));
        loop {
            ticker.tick().await;

            if self.is_shutdown() {
                info!("replicator: shutting down");
                break;
            }

            self.run_cycle().await;
        }
    }

    /// Run a single replication cycle
    pub async fn run_cycle(&self) {
        let guard = self.in_progress.clone();
        let db = self.db.clone();
        let cfg = self.cfg.clone();
        let metrics = self.metrics.clone();

        // Try to acquire lock - if already running, skip this cycle
        let lock = match guard.try_lock() {
            Ok(l) => l,
            Err(_) => {
                debug!("replicator: previous cycle still running, skipping");
                return;
            }
        };

        let req = crate::BundleRequest {
            max_bytes: cfg.max_bytes,
            since_version: cfg.since_version,
            prefer_hot: cfg.prefer_hot,
            target_bytes_per_sec: cfg.target_bytes_per_sec,
            max_entries: cfg.max_entries,
        };

        let plan = match db.plan_bundle(req) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "replicator: plan_bundle failed");
                metrics.record_failure();
                return;
            }
        };

        if plan.entries.is_empty() {
            debug!("replicator: no entries to replicate");
            return;
        }

        let entry_count = plan.entries.len() as u64;
        let total_bytes = plan.total_bytes;

        // Serialize the plan
        let payload = match serde_json::to_vec_pretty(&plan) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "replicator: failed to serialize plan");
                metrics.record_failure();
                return;
            }
        };

        // Determine output path
        let output_path = cfg
            .output_dir
            .as_ref()
            .map(|d| d.join("boyodb_last_bundle.json"))
            .unwrap_or_else(|| PathBuf::from("/tmp/boyodb_last_bundle.json"));

        if let Some(parent) = output_path.parent() {
            let parent = parent.to_path_buf();
            match task::spawn_blocking(move || std::fs::create_dir_all(parent)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!(error = %e, "replicator: failed to create output dir");
                    metrics.record_failure();
                    return;
                }
                Err(join_err) => {
                    error!(error = %join_err, "replicator: failed to create output dir (join error)");
                    metrics.record_failure();
                    return;
                }
            }
        }

        // Write bundle to disk without blocking the async runtime
        let output_path_cloned = output_path.clone();
        match task::spawn_blocking(move || std::fs::write(&output_path_cloned, &payload)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(
                    error = %e,
                    path = %output_path.display(),
                    "replicator: failed to write bundle"
                );
                metrics.record_failure();
                return;
            }
            Err(join_err) => {
                error!(
                    error = %join_err,
                    path = %output_path.display(),
                    "replicator: failed to write bundle (join error)"
                );
                metrics.record_failure();
                return;
            }
        }

        info!(
            entries = entry_count,
            bytes = total_bytes,
            path = %output_path.display(),
            throttle_ms = plan.throttle_millis,
            "replicator: bundle created"
        );

        metrics.record_success(total_bytes, entry_count);

        // Apply throttle if specified
        if plan.throttle_millis > 0 {
            debug!(
                throttle_ms = plan.throttle_millis,
                "replicator: applying throttle"
            );
            tokio::time::sleep(Duration::from_millis(plan.throttle_millis)).await;
        }

        drop(lock);
    }
}
