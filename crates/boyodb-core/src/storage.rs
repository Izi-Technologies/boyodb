use crate::engine::{EngineConfig, EngineError};
use crate::replication::{ManifestEntry, SegmentTier};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, GetResult};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone)]
pub struct TieredStorage {
    remote: Option<Arc<dyn ObjectStore>>,
    runtime: Handle,
    local_root: PathBuf,
}

impl TieredStorage {
    pub fn new(cfg: &EngineConfig) -> Result<Self, EngineError> {
        // We capture the handle of the runtime creating the DB (should be a Tokio runtime)
        let runtime = Handle::try_current()
            .map_err(|_| EngineError::Internal("TieredStorage must be initialized within a Tokio runtime".into()))?;

        let remote = if let (Some(bucket), Some(region)) = (&cfg.s3_bucket, &cfg.s3_region) {
            let mut builder = AmazonS3Builder::new()
                .with_region(region)
                .with_bucket_name(bucket);

            if let Some(endpoint) = &cfg.s3_endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if cfg.s3_access_key.is_some() ^ cfg.s3_secret_key.is_some() {
                return Err(EngineError::Configuration(
                    "s3_access_key and s3_secret_key must be provided together".into(),
                ));
            }
            if let (Some(ak), Some(sk)) = (&cfg.s3_access_key, &cfg.s3_secret_key) {
                let ak_trimmed = ak.trim();
                let sk_trimmed = sk.trim();
                if ak_trimmed.is_empty() || sk_trimmed.is_empty() {
                    return Err(EngineError::Configuration(
                        "s3_access_key and s3_secret_key must be non-empty".into(),
                    ));
                }
                // Check for common placeholder/weak credentials
                const WEAK_CREDENTIALS: &[&str] = &[
                    "changeme",
                    "password",
                    "secret",
                    "admin",
                    "test",
                    "example",
                    "your-access-key",
                    "your-secret-key",
                    "xxx",
                    "yyy",
                    "placeholder",
                    "replace-me",
                    "insert-key-here",
                    "minioadmin", // Default minio credentials
                    "minio",
                    "accesskey",
                    "secretkey",
                ];
                let ak_lower = ak_trimmed.to_lowercase();
                let sk_lower = sk_trimmed.to_lowercase();
                for weak in WEAK_CREDENTIALS {
                    if ak_lower == *weak || sk_lower == *weak {
                        return Err(EngineError::Configuration(format!(
                            "s3_access_key and s3_secret_key must not be weak/placeholder values (detected: '{}')",
                            weak
                        )));
                    }
                }
                // Also reject keys that are too short (real AWS keys are 20+ chars)
                if ak_trimmed.len() < 16 || sk_trimmed.len() < 16 {
                    return Err(EngineError::Configuration(
                        "s3_access_key and s3_secret_key appear too short to be valid credentials".into(),
                    ));
                }
                builder = builder
                    .with_access_key_id(ak_trimmed)
                    .with_secret_access_key(sk_trimmed);
            }
            // Allow http for local minio/testing
            builder = builder.with_allow_http(true);

            let s3 = builder
                .build()
                .map_err(|e| EngineError::Internal(format!("failed to build s3: {}", e)))?;
            Some(Arc::new(s3) as Arc<dyn ObjectStore>)
        } else {
            None
        };

        Ok(Self {
            remote,
            runtime,
            local_root: cfg.segments_dir.clone(),
        })
    }

    pub fn new_with_remote(cfg: &EngineConfig, remote: Arc<dyn ObjectStore>) -> Result<Self, EngineError> {
        let runtime = Handle::try_current()
            .map_err(|_| EngineError::Internal("TieredStorage must be initialized within a Tokio runtime".into()))?;
        Ok(Self {
            remote: Some(remote),
            runtime,
            local_root: cfg.segments_dir.clone(),
        })
    }

    pub fn has_remote(&self) -> bool {
        self.remote.is_some()
    }

    /// Create a TieredStorage for local-only operations
    /// This version doesn't require a tokio runtime but only supports local filesystem operations.
    /// Useful for background tasks or testing where S3 is not needed.
    pub fn new_local_only(local_root: PathBuf) -> Self {
        // Create a temporary runtime handle for the struct (won't be used for local-only ops)
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .handle()
            .clone();
        Self {
            remote: None,
            runtime,
            local_root,
        }
    }

    pub fn load_segment(&self, entry: &ManifestEntry) -> Result<Vec<u8>, EngineError> {
        match entry.tier {
            SegmentTier::Hot | SegmentTier::Warm => {
                // Fast path: synchronous local FS read
                let path = self.local_root.join(format!("{}.ipc", entry.segment_id));
                std::fs::read(&path).map_err(|e| {
                    EngineError::Io(format!(
                        "read local segment {} failed: {}",
                        entry.segment_id, e
                    ))
                })
            }
            SegmentTier::Cold => {
                // Try S3 first, fall back to local if not configured
                if let Some(remote) = self.remote.as_ref() {
                    let path = ObjPath::from(format!("{}.ipc", entry.segment_id));

                    // Block on async S3 fetch
                    // We use block_in_place to allow calling this safe sync wrapper from within an async context
                    tokio::task::block_in_place(move || {
                        self.runtime.block_on(async {
                            let get_future = remote.get(&path);
                            let result: GetResult = get_future.await
                                .map_err(|e| EngineError::Io(format!("s3 get failed: {}", e)))?;

                            let data_future = result.bytes();
                            let bytes = data_future.await
                                .map_err(|e| EngineError::Io(format!("s3 bytes failed: {}", e)))?;
                            Ok(bytes.to_vec())
                        })
                    })
                } else {
                    // Fall back to local storage if S3 not configured (e.g., in tests)
                    let path = self.local_root.join(format!("{}.ipc", entry.segment_id));
                    std::fs::read(&path).map_err(|e| {
                        EngineError::Io(format!(
                            "read local segment {} (cold tier fallback) failed: {}",
                            entry.segment_id, e
                        ))
                    })
                }
            }
        }
    }

    pub fn persist_segment_cold(&self, segment_id: &str, data: &[u8]) -> Result<(), EngineError> {
        let remote = self.remote.as_ref().ok_or_else(|| {
            EngineError::Configuration("Cold tier accessed but no S3 configured".into())
        })?;
        let path = ObjPath::from(format!("{}.ipc", segment_id));
        let data_vec = data.to_vec();

        // Use block_in_place to allow calling this safe sync wrapper from within an async context
        tokio::task::block_in_place(move || {
            self.runtime.block_on(async {
                remote
                    .put(&path, data_vec.into())
                    .await
                    .map_err(|e| EngineError::Io(format!("s3 put failed: {}", e)))?;
                Ok(())
            })
        })
    }

    /// Persist to LOCAL store only (Hot/Warm). Tiering manager handles S3 upload.
    pub fn persist_segment_local(&self, segment_id: &str, data: &[u8]) -> Result<(), EngineError> {
        use std::fs::{self, OpenOptions};
        use std::io::Write;

        let path = self.local_root.join(format!("{}.ipc", segment_id));
        let tmp_path = path.with_extension("tmp");
        
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("create segments dir failed: {}", e)))?;
        }

        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| EngineError::Io(format!("open segment tmp failed: {}", e)))?;
            
            file.write_all(data)
                .map_err(|e| EngineError::Io(format!("write segment tmp failed: {}", e)))?;
                
            file.sync_all()
                .map_err(|e| EngineError::Io(format!("fsync segment tmp failed: {}", e)))?;
        }
        
        fs::rename(&tmp_path, &path)
            .map_err(|e| EngineError::Io(format!("rename segment failed: {}", e)))?;
            
        // Sync parent directory
        if let Some(parent) = path.parent() {
            let dir = std::fs::File::open(parent)
                .map_err(|e| EngineError::Io(format!("open dir for fsync failed: {}", e)))?;
            dir.sync_all()
                .map_err(|e| EngineError::Io(format!("fsync dir failed: {}", e)))?;
        }
        
        Ok(())
    }

    pub fn spawn_task<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.runtime.spawn(future);
    }
}
