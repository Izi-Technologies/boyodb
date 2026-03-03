//! WAL Archiving System for Point-in-Time Recovery
//!
//! This module provides WAL segment archiving for:
//! - Continuous archiving to local filesystem or S3
//! - Retention management
//! - Archive listing and retrieval

use crate::engine::EngineError;

use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Information about an archived WAL segment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalArchiveInfo {
    /// Original filename
    pub filename: String,

    /// First LSN in this segment
    pub start_lsn: u64,

    /// Last LSN in this segment (exclusive)
    pub end_lsn: u64,

    /// First timestamp in this segment (microseconds since epoch)
    pub start_timestamp_micros: u64,

    /// Last timestamp in this segment (microseconds since epoch)
    pub end_timestamp_micros: u64,

    /// Size in bytes
    pub size_bytes: u64,

    /// When this segment was archived
    pub archived_at_micros: u64,

    /// Checksum (xxHash64)
    pub checksum: u64,

    /// Compression used (if any)
    pub compression: Option<String>,
}

/// Configuration for WAL archiving
#[derive(Debug, Clone)]
pub struct WalArchiveConfig {
    /// Local archive directory
    pub archive_path: PathBuf,

    /// Optional S3 bucket for remote archiving
    pub s3_bucket: Option<String>,

    /// S3 prefix (folder path)
    pub s3_prefix: Option<String>,

    /// S3 region
    pub s3_region: Option<String>,

    /// S3 endpoint (for compatible services)
    pub s3_endpoint: Option<String>,

    /// Retention period in days (0 = forever)
    pub retention_days: u32,

    /// Compression for archives
    pub compression: Option<String>,

    /// Whether to verify checksum on archive
    pub verify_checksum: bool,

    /// Maximum concurrent archive operations
    pub max_concurrent_archives: usize,
}

impl Default for WalArchiveConfig {
    fn default() -> Self {
        WalArchiveConfig {
            archive_path: PathBuf::from("./wal_archive"),
            s3_bucket: None,
            s3_prefix: None,
            s3_region: None,
            s3_endpoint: None,
            retention_days: 30,
            compression: Some("zstd".to_string()),
            verify_checksum: true,
            max_concurrent_archives: 4,
        }
    }
}

/// WAL archiver for managing archived WAL segments
pub struct WalArchiver {
    config: WalArchiveConfig,

    /// Index of archived segments (LSN -> info)
    archive_index: parking_lot::RwLock<Vec<WalArchiveInfo>>,
}

impl WalArchiver {
    /// Create a new WAL archiver
    pub fn new(config: WalArchiveConfig) -> Result<Self, EngineError> {
        // Create archive directory if it doesn't exist
        fs::create_dir_all(&config.archive_path).map_err(|e| {
            EngineError::Io(format!(
                "Failed to create archive directory {:?}: {}",
                config.archive_path, e
            ))
        })?;

        let archiver = WalArchiver {
            config,
            archive_index: parking_lot::RwLock::new(Vec::new()),
        };

        // Load existing archive index
        archiver.load_index()?;

        Ok(archiver)
    }

    /// Archive a WAL segment
    pub fn archive_segment(
        &self,
        wal_path: &Path,
        start_lsn: u64,
        end_lsn: u64,
        start_timestamp: u64,
        end_timestamp: u64,
    ) -> Result<WalArchiveInfo, EngineError> {
        let filename = wal_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| EngineError::InvalidArgument("Invalid WAL path".to_string()))?
            .to_string();

        // Read the WAL file
        let wal_data = fs::read(wal_path)
            .map_err(|e| EngineError::Io(format!("Failed to read WAL file: {}", e)))?;

        // Calculate checksum
        let checksum = xxhash_rust::xxh64::xxh64(&wal_data, 0);

        // Compress if configured
        let (archive_data, compression) = if let Some(ref comp) = self.config.compression {
            match comp.as_str() {
                "zstd" => {
                    let compressed = zstd::encode_all(&wal_data[..], 3)
                        .map_err(|e| EngineError::Internal(format!("Compression failed: {}", e)))?;
                    (compressed, Some("zstd".to_string()))
                }
                "lz4" => {
                    let compressed = lz4_flex::compress_prepend_size(&wal_data);
                    (compressed, Some("lz4".to_string()))
                }
                _ => (wal_data, None),
            }
        } else {
            (wal_data, None)
        };

        // Generate archive filename with LSN range
        let archive_filename = format!(
            "wal_{:016x}_{:016x}{}",
            start_lsn,
            end_lsn,
            if compression.is_some() { ".zst" } else { "" }
        );
        let archive_path = self.config.archive_path.join(&archive_filename);

        // Write to archive
        let mut file = File::create(&archive_path)
            .map_err(|e| EngineError::Io(format!("Failed to create archive file: {}", e)))?;
        file.write_all(&archive_data)
            .map_err(|e| EngineError::Io(format!("Failed to write archive file: {}", e)))?;
        file.sync_all()
            .map_err(|e| EngineError::Io(format!("Failed to sync archive file: {}", e)))?;

        let archived_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let info = WalArchiveInfo {
            filename: archive_filename,
            start_lsn,
            end_lsn,
            start_timestamp_micros: start_timestamp,
            end_timestamp_micros: end_timestamp,
            size_bytes: archive_data.len() as u64,
            archived_at_micros: archived_at,
            checksum,
            compression,
        };

        // Update index
        {
            let mut index = self.archive_index.write();
            index.push(info.clone());
            index.sort_by_key(|i| i.start_lsn);
        }

        // Save index
        self.save_index()?;

        // Upload to S3 if configured
        if self.config.s3_bucket.is_some() {
            self.upload_to_s3(&archive_path, &info.filename)?;
        }

        tracing::info!(
            "Archived WAL segment {} (LSN {}-{}, {} bytes)",
            info.filename,
            start_lsn,
            end_lsn,
            info.size_bytes
        );

        Ok(info)
    }

    /// List archived segments in a time range
    pub fn list_archives(
        &self,
        start_timestamp: Option<u64>,
        end_timestamp: Option<u64>,
    ) -> Vec<WalArchiveInfo> {
        let index = self.archive_index.read();
        index
            .iter()
            .filter(|info| {
                let after_start = start_timestamp
                    .map(|s| info.end_timestamp_micros >= s)
                    .unwrap_or(true);
                let before_end = end_timestamp
                    .map(|e| info.start_timestamp_micros <= e)
                    .unwrap_or(true);
                after_start && before_end
            })
            .cloned()
            .collect()
    }

    /// List archived segments in an LSN range
    pub fn list_archives_by_lsn(&self, start_lsn: u64, end_lsn: u64) -> Vec<WalArchiveInfo> {
        let index = self.archive_index.read();
        index
            .iter()
            .filter(|info| info.end_lsn > start_lsn && info.start_lsn < end_lsn)
            .cloned()
            .collect()
    }

    /// Get archives needed to recover to a specific timestamp
    pub fn get_archives_for_recovery(&self, target_timestamp: u64) -> Vec<WalArchiveInfo> {
        let index = self.archive_index.read();
        index
            .iter()
            .filter(|info| info.start_timestamp_micros <= target_timestamp)
            .cloned()
            .collect()
    }

    /// Get archives needed to recover to a specific LSN
    pub fn get_archives_for_lsn(&self, target_lsn: u64) -> Vec<WalArchiveInfo> {
        let index = self.archive_index.read();
        index
            .iter()
            .filter(|info| info.start_lsn <= target_lsn)
            .cloned()
            .collect()
    }

    /// Retrieve an archived segment
    pub fn retrieve_segment(&self, info: &WalArchiveInfo) -> Result<Vec<u8>, EngineError> {
        let archive_path = self.config.archive_path.join(&info.filename);

        // Check if local file exists
        let archive_data = if archive_path.exists() {
            fs::read(&archive_path)
                .map_err(|e| EngineError::Io(format!("Failed to read archive file: {}", e)))?
        } else if self.config.s3_bucket.is_some() {
            // Try to download from S3
            self.download_from_s3(&info.filename)?
        } else {
            return Err(EngineError::NotFound(format!(
                "Archive file not found: {}",
                info.filename
            )));
        };

        // Decompress if needed
        let wal_data = match &info.compression {
            Some(comp) if comp == "zstd" => zstd::decode_all(&archive_data[..])
                .map_err(|e| EngineError::Internal(format!("Decompression failed: {}", e)))?,
            Some(comp) if comp == "lz4" => lz4_flex::decompress_size_prepended(&archive_data)
                .map_err(|e| EngineError::Internal(format!("Decompression failed: {}", e)))?,
            _ => archive_data,
        };

        // Verify checksum if configured
        if self.config.verify_checksum {
            let checksum = xxhash_rust::xxh64::xxh64(&wal_data, 0);
            if checksum != info.checksum {
                return Err(EngineError::Internal(format!(
                    "Checksum mismatch for {}: expected {:x}, got {:x}",
                    info.filename, info.checksum, checksum
                )));
            }
        }

        Ok(wal_data)
    }

    /// Apply retention policy and delete old archives
    pub fn enforce_retention(&self) -> Result<usize, EngineError> {
        if self.config.retention_days == 0 {
            return Ok(0); // Retention disabled
        }

        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
            - (self.config.retention_days as u64 * 24 * 60 * 60 * 1_000_000);

        let mut to_delete = Vec::new();
        {
            let index = self.archive_index.read();
            for info in index.iter() {
                if info.archived_at_micros < cutoff {
                    to_delete.push(info.clone());
                }
            }
        }

        let count = to_delete.len();
        for info in to_delete {
            self.delete_archive(&info)?;
        }

        if count > 0 {
            self.save_index()?;
            tracing::info!("Deleted {} old archive segments", count);
        }

        Ok(count)
    }

    /// Delete a specific archive
    fn delete_archive(&self, info: &WalArchiveInfo) -> Result<(), EngineError> {
        let archive_path = self.config.archive_path.join(&info.filename);

        // Delete local file
        if archive_path.exists() {
            fs::remove_file(&archive_path)
                .map_err(|e| EngineError::Io(format!("Failed to delete archive file: {}", e)))?;
        }

        // Delete from S3 if configured
        if self.config.s3_bucket.is_some() {
            self.delete_from_s3(&info.filename)?;
        }

        // Remove from index
        {
            let mut index = self.archive_index.write();
            index.retain(|i| i.filename != info.filename);
        }

        Ok(())
    }

    /// Get archive statistics
    pub fn stats(&self) -> WalArchiveStats {
        let index = self.archive_index.read();
        WalArchiveStats {
            segment_count: index.len(),
            total_bytes: index.iter().map(|i| i.size_bytes).sum(),
            oldest_timestamp: index.iter().map(|i| i.start_timestamp_micros).min(),
            newest_timestamp: index.iter().map(|i| i.end_timestamp_micros).max(),
            oldest_lsn: index.iter().map(|i| i.start_lsn).min(),
            newest_lsn: index.iter().map(|i| i.end_lsn).max(),
        }
    }

    // Internal helper methods

    fn load_index(&self) -> Result<(), EngineError> {
        let index_path = self.config.archive_path.join("archive_index.json");
        if !index_path.exists() {
            return Ok(());
        }

        let file = File::open(&index_path)
            .map_err(|e| EngineError::Io(format!("Failed to open archive index: {}", e)))?;
        let reader = BufReader::new(file);
        let index: Vec<WalArchiveInfo> = serde_json::from_reader(reader)
            .map_err(|e| EngineError::Internal(format!("Failed to parse archive index: {}", e)))?;

        *self.archive_index.write() = index;
        Ok(())
    }

    fn save_index(&self) -> Result<(), EngineError> {
        let index_path = self.config.archive_path.join("archive_index.json");
        let index = self.archive_index.read();

        let file = File::create(&index_path)
            .map_err(|e| EngineError::Io(format!("Failed to create archive index: {}", e)))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &*index)
            .map_err(|e| EngineError::Internal(format!("Failed to write archive index: {}", e)))?;

        Ok(())
    }

    fn upload_to_s3(&self, _local_path: &Path, _remote_name: &str) -> Result<(), EngineError> {
        // TODO: Implement S3 upload using aws-sdk-s3 or similar
        // For now, just log that we would upload
        tracing::debug!("Would upload to S3: {}", _remote_name);
        Ok(())
    }

    fn download_from_s3(&self, _remote_name: &str) -> Result<Vec<u8>, EngineError> {
        // TODO: Implement S3 download
        Err(EngineError::NotImplemented(
            "S3 download not yet implemented".to_string(),
        ))
    }

    fn delete_from_s3(&self, _remote_name: &str) -> Result<(), EngineError> {
        // TODO: Implement S3 delete
        tracing::debug!("Would delete from S3: {}", _remote_name);
        Ok(())
    }
}

/// WAL archive statistics
#[derive(Debug, Clone)]
pub struct WalArchiveStats {
    /// Number of archived segments
    pub segment_count: usize,

    /// Total size in bytes
    pub total_bytes: u64,

    /// Oldest timestamp in archive
    pub oldest_timestamp: Option<u64>,

    /// Newest timestamp in archive
    pub newest_timestamp: Option<u64>,

    /// Oldest LSN in archive
    pub oldest_lsn: Option<u64>,

    /// Newest LSN in archive
    pub newest_lsn: Option<u64>,
}

impl WalArchiveStats {
    /// Get the time span covered by archives
    pub fn time_span(&self) -> Option<Duration> {
        match (self.oldest_timestamp, self.newest_timestamp) {
            (Some(oldest), Some(newest)) => {
                Some(Duration::from_micros(newest.saturating_sub(oldest)))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_archiver() -> (WalArchiver, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = WalArchiveConfig {
            archive_path: dir.path().to_path_buf(),
            compression: None, // Disable compression for tests
            ..Default::default()
        };
        let archiver = WalArchiver::new(config).unwrap();
        (archiver, dir)
    }

    #[test]
    fn test_archive_segment() {
        let (archiver, dir) = create_test_archiver();

        // Create a test WAL file
        let wal_path = dir.path().join("test.wal");
        fs::write(&wal_path, b"test wal data").unwrap();

        // Archive it
        let info = archiver
            .archive_segment(&wal_path, 1, 100, 1000000, 2000000)
            .unwrap();

        assert_eq!(info.start_lsn, 1);
        assert_eq!(info.end_lsn, 100);
        assert!(info.size_bytes > 0);
    }

    #[test]
    fn test_list_archives() {
        let (archiver, dir) = create_test_archiver();

        // Archive multiple segments
        for i in 0..5 {
            let wal_path = dir.path().join(format!("test{}.wal", i));
            fs::write(&wal_path, format!("wal data {}", i)).unwrap();
            archiver
                .archive_segment(
                    &wal_path,
                    i * 100,
                    (i + 1) * 100,
                    i * 1000000,
                    (i + 1) * 1000000,
                )
                .unwrap();
        }

        // List all
        let all = archiver.list_archives(None, None);
        assert_eq!(all.len(), 5);

        // List by timestamp range
        let range = archiver.list_archives(Some(1500000), Some(3500000));
        assert!(range.len() >= 2);

        // List by LSN range
        let lsn_range = archiver.list_archives_by_lsn(150, 350);
        assert!(lsn_range.len() >= 2);
    }

    #[test]
    fn test_retrieve_segment() {
        let (archiver, dir) = create_test_archiver();

        // Create and archive a segment
        let wal_data = b"important wal data";
        let wal_path = dir.path().join("test.wal");
        fs::write(&wal_path, wal_data).unwrap();

        let info = archiver
            .archive_segment(&wal_path, 1, 100, 1000000, 2000000)
            .unwrap();

        // Retrieve it
        let retrieved = archiver.retrieve_segment(&info).unwrap();
        assert_eq!(retrieved, wal_data);
    }

    #[test]
    fn test_archive_stats() {
        let (archiver, dir) = create_test_archiver();

        // Archive a few segments
        for i in 0..3 {
            let wal_path = dir.path().join(format!("test{}.wal", i));
            fs::write(&wal_path, format!("wal data {}", i)).unwrap();
            archiver
                .archive_segment(
                    &wal_path,
                    i * 100,
                    (i + 1) * 100,
                    i * 1000000,
                    (i + 1) * 1000000,
                )
                .unwrap();
        }

        let stats = archiver.stats();
        assert_eq!(stats.segment_count, 3);
        assert!(stats.total_bytes > 0);
        assert_eq!(stats.oldest_lsn, Some(0));
        assert_eq!(stats.newest_lsn, Some(300));
    }

    #[test]
    fn test_recovery_query() {
        let (archiver, dir) = create_test_archiver();

        // Archive segments
        for i in 0..5 {
            let wal_path = dir.path().join(format!("test{}.wal", i));
            fs::write(&wal_path, format!("wal data {}", i)).unwrap();
            archiver
                .archive_segment(
                    &wal_path,
                    i * 100,
                    (i + 1) * 100,
                    i * 1000000,
                    (i + 1) * 1000000,
                )
                .unwrap();
        }

        // Get archives for recovery to timestamp 2500000
        let archives = archiver.get_archives_for_recovery(2500000);
        assert_eq!(archives.len(), 3); // Segments 0, 1, 2

        // Get archives for recovery to LSN 250
        let archives = archiver.get_archives_for_lsn(250);
        assert_eq!(archives.len(), 3); // Segments with start_lsn <= 250
    }
}
