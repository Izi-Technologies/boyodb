use crate::engine::{EngineConfig, EngineError};
use crate::replication::{ManifestEntry, SegmentTier};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{GetResult, ObjectStore, ObjectStoreExt};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug, Clone)]
pub struct TieredStorage {
    remote: Option<Arc<dyn ObjectStore>>,
    /// Runtime handle for async S3 operations. None for local-only storage.
    runtime: Option<Handle>,
    local_root: PathBuf,
}

impl TieredStorage {
    pub fn new(cfg: &EngineConfig) -> Result<Self, EngineError> {
        // Only require Tokio runtime if S3 is configured
        let has_s3_config = cfg.s3_bucket.is_some() && cfg.s3_region.is_some();

        let runtime = if has_s3_config {
            // S3 operations require a Tokio runtime
            Some(Handle::try_current().map_err(|_| {
                EngineError::Internal("TieredStorage with S3 must be initialized within a Tokio runtime".into())
            })?)
        } else {
            // Local-only storage doesn't need a runtime
            Handle::try_current().ok()
        };

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
                        "s3_access_key and s3_secret_key appear too short to be valid credentials"
                            .into(),
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

    pub fn new_with_remote(
        cfg: &EngineConfig,
        remote: Arc<dyn ObjectStore>,
    ) -> Result<Self, EngineError> {
        let runtime = Some(Handle::try_current().map_err(|_| {
            EngineError::Internal("TieredStorage must be initialized within a Tokio runtime".into())
        })?);
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
        Self {
            remote: None,
            runtime: None,
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
                if let (Some(remote), Some(runtime)) = (self.remote.as_ref(), self.runtime.as_ref())
                {
                    let path = ObjPath::from(format!("{}.ipc", entry.segment_id));

                    // Block on async S3 fetch
                    // We use block_in_place to allow calling this safe sync wrapper from within an async context
                    tokio::task::block_in_place(move || {
                        runtime.block_on(async {
                            let get_future = remote.get(&path);
                            let result: GetResult = get_future
                                .await
                                .map_err(|e| EngineError::Io(format!("s3 get failed: {}", e)))?;

                            let data_future = result.bytes();
                            let bytes = data_future
                                .await
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
        self.persist_segment_cold_with_verify(segment_id, data, true)
    }

    /// Persist segment to S3 cold tier with optional verification
    pub fn persist_segment_cold_with_verify(
        &self,
        segment_id: &str,
        data: &[u8],
        verify: bool,
    ) -> Result<(), EngineError> {
        let remote = self.remote.as_ref().ok_or_else(|| {
            EngineError::Configuration("Cold tier accessed but no S3 configured".into())
        })?;
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            EngineError::Configuration("Cold tier requires tokio runtime".into())
        })?;
        let path = ObjPath::from(format!("{}.ipc", segment_id));
        let expected_checksum = crate::engine::compute_checksum(data);
        let data_vec = data.to_vec();
        let data_len = data.len();

        // Use block_in_place to allow calling this safe sync wrapper from within an async context
        tokio::task::block_in_place(move || {
            runtime.block_on(async {
                // Upload to S3
                remote
                    .put(&path, data_vec.into())
                    .await
                    .map_err(|e| EngineError::Io(format!("s3 put failed: {}", e)))?;

                // Verify upload if enabled
                if verify {
                    // Read back and verify
                    let get_result = remote
                        .get(&path)
                        .await
                        .map_err(|e| EngineError::Io(format!("s3 verify get failed: {}", e)))?;

                    let bytes = get_result
                        .bytes()
                        .await
                        .map_err(|e| EngineError::Io(format!("s3 verify bytes failed: {}", e)))?;

                    // Check size first (fast check)
                    if bytes.len() != data_len {
                        return Err(EngineError::Io(format!(
                            "s3 upload verification failed for {}: size mismatch (expected {}, got {})",
                            segment_id, data_len, bytes.len()
                        )));
                    }

                    // Verify checksum
                    let actual_checksum = crate::engine::compute_checksum(&bytes);
                    if actual_checksum != expected_checksum {
                        // Delete the corrupt upload - log error if delete fails
                        if let Err(del_err) = remote.delete(&path).await {
                            tracing::error!(
                                "CRITICAL: Failed to delete corrupt S3 segment {} after checksum mismatch: {}. \
                                 Manual cleanup required at path: {}",
                                segment_id, del_err, path
                            );
                        }
                        return Err(EngineError::Io(format!(
                            "s3 upload verification failed for {}: checksum mismatch (expected {:016x}, got {:016x})",
                            segment_id, expected_checksum, actual_checksum
                        )));
                    }

                    tracing::debug!("S3 upload verified for segment {}", segment_id);
                }

                Ok(())
            })
        })
    }

    /// Upload segment to S3 with retry logic
    pub fn persist_segment_cold_with_retry(
        &self,
        segment_id: &str,
        data: &[u8],
        max_retries: usize,
        retry_delay_ms: u64,
    ) -> Result<(), EngineError> {
        let mut last_error = None;

        for attempt in 0..=max_retries {
            match self.persist_segment_cold_with_verify(segment_id, data, true) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries {
                        tracing::warn!(
                            "S3 upload attempt {} for segment {} failed, retrying in {}ms",
                            attempt + 1,
                            segment_id,
                            retry_delay_ms
                        );
                        std::thread::sleep(std::time::Duration::from_millis(retry_delay_ms));
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            EngineError::Internal("persist_segment_cold_with_retry: no error captured".into())
        }))
    }

    /// Persist to LOCAL store only (Hot/Warm). Tiering manager handles S3 upload.
    /// Includes write verification to detect silent data corruption.
    pub fn persist_segment_local(&self, segment_id: &str, data: &[u8]) -> Result<(), EngineError> {
        self.persist_segment_local_with_verify(segment_id, data, true)
    }

    /// Persist segment with optional write verification
    pub fn persist_segment_local_with_verify(
        &self,
        segment_id: &str,
        data: &[u8],
        verify: bool,
    ) -> Result<(), EngineError> {
        use std::fs::{self, OpenOptions};
        use std::io::Write;

        let path = self.local_root.join(format!("{}.ipc", segment_id));
        let tmp_path = path.with_extension("tmp");

        // Compute expected checksum before write
        let expected_checksum = crate::engine::compute_checksum(data);

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

        // Write verification: read back and verify checksum before rename
        if verify {
            let written_data = fs::read(&tmp_path)
                .map_err(|e| EngineError::Io(format!("read-back verification failed: {}", e)))?;

            let actual_checksum = crate::engine::compute_checksum(&written_data);
            if actual_checksum != expected_checksum {
                // Delete corrupt temp file
                let _ = fs::remove_file(&tmp_path);
                return Err(EngineError::Io(format!(
                    "write verification failed for segment {}: checksum mismatch (expected {:016x}, got {:016x})",
                    segment_id, expected_checksum, actual_checksum
                )));
            }
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
        if let Some(runtime) = &self.runtime {
            runtime.spawn(future);
        }
        // If no runtime, silently drop the task (local-only mode)
    }

    /// Load multiple cold segments in parallel using async S3 operations.
    /// Returns results in the same order as input entries.
    /// Falls back to sequential loading for hot/warm segments (local I/O is fast).
    pub fn load_segments_parallel(
        &self,
        entries: &[&ManifestEntry],
    ) -> Vec<Result<Vec<u8>, EngineError>> {
        if entries.is_empty() {
            return Vec::new();
        }

        // Check if all entries are cold tier with S3 configured
        let all_cold = entries.iter().all(|e| matches!(e.tier, SegmentTier::Cold));
        let has_s3 = self.remote.is_some() && self.runtime.is_some();

        if all_cold && has_s3 && entries.len() > 1 {
            // Parallel async S3 fetch using tokio join_all
            let remote = self.remote.as_ref().unwrap();
            let runtime = self.runtime.as_ref().unwrap();

            let segment_ids: Vec<String> = entries.iter().map(|e| e.segment_id.clone()).collect();
            let expected_checksums: Vec<u64> = entries.iter().map(|e| e.checksum).collect();

            tokio::task::block_in_place(move || {
                runtime.block_on(async {
                    let mut futures = Vec::with_capacity(segment_ids.len());

                    for segment_id in &segment_ids {
                        let path = ObjPath::from(format!("{}.ipc", segment_id));
                        let remote = remote.clone();
                        futures.push(async move {
                            let result = remote.get(&path).await;
                            match result {
                                Ok(get_result) => match get_result.bytes().await {
                                    Ok(bytes) => Ok(bytes.to_vec()),
                                    Err(e) => Err(EngineError::Io(format!("s3 bytes failed: {}", e))),
                                },
                                Err(e) => Err(EngineError::Io(format!("s3 get failed: {}", e))),
                            }
                        });
                    }

                    let results = futures::future::join_all(futures).await;

                    // Verify checksums for all successful loads
                    results
                        .into_iter()
                        .zip(expected_checksums.iter())
                        .zip(segment_ids.iter())
                        .map(|((result, &expected_checksum), segment_id)| {
                            result.and_then(|data| {
                                let actual = crate::engine::compute_checksum(&data);
                                if actual != expected_checksum {
                                    Err(EngineError::Io(format!(
                                        "checksum mismatch for segment {}: expected {:016x}, got {:016x}",
                                        segment_id, expected_checksum, actual
                                    )))
                                } else {
                                    Ok(data)
                                }
                            })
                        })
                        .collect()
                })
            })
        } else {
            // Sequential loading for mixed tiers or single segment
            entries.iter().map(|entry| self.load_segment(entry)).collect()
        }
    }
}
