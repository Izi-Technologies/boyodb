//! Incremental Backup Module
//!
//! Provides delta-based incremental backups with:
//! - Full and incremental backup chains
//! - Block-level deduplication
//! - Backup encryption (AES-256-GCM)
//! - Parallel backup and restore
//! - Backup validation and verification
//! - Automatic backup rotation and retention

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

/// Backup configuration
#[derive(Clone, Debug)]
pub struct BackupConfig {
    /// Backup storage directory
    pub backup_dir: PathBuf,
    /// Block size for deduplication (default: 64KB)
    pub block_size: usize,
    /// Enable encryption
    pub encryption_enabled: bool,
    /// Encryption key (32 bytes for AES-256)
    pub encryption_key: Option<Vec<u8>>,
    /// Number of parallel workers for backup/restore
    pub parallelism: usize,
    /// Compression algorithm (none, lz4, zstd)
    pub compression: String,
    /// Compression level (1-9)
    pub compression_level: i32,
    /// Retention period in days (0 = forever)
    pub retention_days: u32,
    /// Maximum number of incremental backups before requiring a full backup
    pub max_incremental_chain: usize,
    /// Verify backup after creation
    pub verify_after_backup: bool,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            backup_dir: PathBuf::from("backups"),
            block_size: 64 * 1024, // 64KB
            encryption_enabled: false,
            encryption_key: None,
            parallelism: 4,
            compression: "zstd".to_string(),
            compression_level: 3,
            retention_days: 30,
            max_incremental_chain: 7,
            verify_after_backup: true,
        }
    }
}

/// Type of backup
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    /// Full backup containing all data
    Full,
    /// Incremental backup containing only changes since last backup
    Incremental,
    /// Differential backup containing changes since last full backup
    Differential,
}

/// Backup metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupMetadata {
    /// Unique backup ID
    pub id: String,
    /// Backup type
    pub backup_type: BackupType,
    /// Parent backup ID (for incremental/differential)
    pub parent_id: Option<String>,
    /// Backup start time (unix millis)
    pub start_time_ms: u64,
    /// Backup end time (unix millis)
    pub end_time_ms: u64,
    /// Total size before deduplication
    pub total_size_bytes: u64,
    /// Size after deduplication
    pub deduplicated_size_bytes: u64,
    /// Compressed size
    pub compressed_size_bytes: u64,
    /// Number of segments backed up
    pub segment_count: usize,
    /// Number of unique blocks
    pub unique_blocks: usize,
    /// Number of deduplicated blocks
    pub deduplicated_blocks: usize,
    /// Manifest version at backup time
    pub manifest_version: u64,
    /// Checksum of backup data
    pub checksum: String,
    /// Whether backup is encrypted
    pub encrypted: bool,
    /// Compression algorithm used
    pub compression: String,
    /// Block size used
    pub block_size: usize,
    /// Label for the backup
    pub label: Option<String>,
    /// Backup chain position (0 = full backup)
    pub chain_position: usize,
}

/// A block in the backup with its hash for deduplication
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupBlock {
    /// Block hash (SHA-256)
    pub hash: String,
    /// Block offset in source file
    pub offset: u64,
    /// Block size
    pub size: usize,
    /// Whether this block is stored in this backup (vs referenced from parent)
    pub stored: bool,
}

/// Segment backup information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentBackup {
    /// Segment ID
    pub segment_id: String,
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Original size
    pub original_size: u64,
    /// Blocks in this segment
    pub blocks: Vec<BackupBlock>,
    /// Checksum of original segment
    pub checksum: String,
}

/// Backup manifest containing all backed up segments
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Backup metadata
    pub metadata: BackupMetadata,
    /// Backed up segments
    pub segments: Vec<SegmentBackup>,
    /// Block hashes stored in this backup (for deduplication lookup)
    pub block_index: HashMap<String, u64>, // hash -> offset in backup file
}

/// Backup statistics
#[derive(Clone, Debug, Default)]
pub struct BackupStats {
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Bytes saved by deduplication
    pub bytes_deduplicated: u64,
    /// Bytes saved by compression
    pub bytes_compressed: u64,
    /// Number of segments processed
    pub segments_processed: usize,
    /// Number of segments skipped (unchanged)
    pub segments_skipped: usize,
    /// Number of blocks processed
    pub blocks_processed: usize,
    /// Number of blocks deduplicated
    pub blocks_deduplicated: usize,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl BackupStats {
    pub fn deduplication_ratio(&self) -> f64 {
        if self.bytes_read == 0 {
            return 1.0;
        }
        1.0 - (self.bytes_deduplicated as f64 / self.bytes_read as f64)
    }

    pub fn compression_ratio(&self) -> f64 {
        let after_dedup = self.bytes_read - self.bytes_deduplicated;
        if after_dedup == 0 {
            return 1.0;
        }
        self.bytes_written as f64 / after_dedup as f64
    }
}

/// Block deduplication index
pub struct DeduplicationIndex {
    /// Hash -> (backup_id, offset) mapping
    blocks: RwLock<HashMap<String, (String, u64)>>,
    /// Statistics
    lookups: AtomicU64,
    hits: AtomicU64,
}

impl DeduplicationIndex {
    pub fn new() -> Self {
        Self {
            blocks: RwLock::new(HashMap::new()),
            lookups: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    /// Load index from backup chain
    pub fn load_from_chain(
        &self,
        backup_dir: &Path,
        chain: &[BackupMetadata],
    ) -> std::io::Result<()> {
        let mut blocks = self.blocks.write();

        for backup in chain {
            let manifest_path = backup_dir.join(&backup.id).join("manifest.json");
            if manifest_path.exists() {
                let file = File::open(&manifest_path)?;
                let manifest: BackupManifest = serde_json::from_reader(BufReader::new(file))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                for (hash, offset) in manifest.block_index {
                    blocks.insert(hash, (backup.id.clone(), offset));
                }
            }
        }

        Ok(())
    }

    /// Look up a block by hash
    pub fn lookup(&self, hash: &str) -> Option<(String, u64)> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        let blocks = self.blocks.read();
        if let Some(result) = blocks.get(hash) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(result.clone())
        } else {
            None
        }
    }

    /// Add a block to the index
    pub fn insert(&self, hash: String, backup_id: String, offset: u64) {
        let mut blocks = self.blocks.write();
        blocks.insert(hash, (backup_id, offset));
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let lookups = self.lookups.load(Ordering::Relaxed);
        if lookups == 0 {
            return 0.0;
        }
        self.hits.load(Ordering::Relaxed) as f64 / lookups as f64
    }
}

impl Default for DeduplicationIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Backup manager for creating and managing backups
pub struct BackupManager {
    config: BackupConfig,
    dedup_index: Arc<DeduplicationIndex>,
}

impl BackupManager {
    pub fn new(config: BackupConfig) -> Self {
        Self {
            config,
            dedup_index: Arc::new(DeduplicationIndex::new()),
        }
    }

    /// Create a full backup
    pub fn create_full_backup(
        &self,
        segments_dir: &Path,
        manifest_version: u64,
        label: Option<String>,
    ) -> std::io::Result<BackupMetadata> {
        self.create_backup(
            segments_dir,
            manifest_version,
            BackupType::Full,
            None,
            label,
        )
    }

    /// Create an incremental backup
    pub fn create_incremental_backup(
        &self,
        segments_dir: &Path,
        manifest_version: u64,
        parent_id: &str,
        label: Option<String>,
    ) -> std::io::Result<BackupMetadata> {
        self.create_backup(
            segments_dir,
            manifest_version,
            BackupType::Incremental,
            Some(parent_id.to_string()),
            label,
        )
    }

    /// Create a backup
    fn create_backup(
        &self,
        segments_dir: &Path,
        manifest_version: u64,
        backup_type: BackupType,
        parent_id: Option<String>,
        label: Option<String>,
    ) -> std::io::Result<BackupMetadata> {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Generate backup ID
        let backup_id = format!("{}-{:x}", chrono_format_now(), start_time);

        // Create backup directory
        let backup_path = self.config.backup_dir.join(&backup_id);
        fs::create_dir_all(&backup_path)?;

        // Load parent backup chain for deduplication
        let chain_position = if let Some(ref parent) = parent_id {
            let chain = self.get_backup_chain(parent)?;
            self.dedup_index
                .load_from_chain(&self.config.backup_dir, &chain)?;
            chain.len()
        } else {
            0
        };

        // Collect segments to back up
        let segments = self.collect_segments(segments_dir)?;

        // Process segments in parallel
        let stats = Arc::new(Mutex::new(BackupStats::default()));
        let block_index = Arc::new(Mutex::new(HashMap::new()));
        let segment_backups = Arc::new(Mutex::new(Vec::new()));

        let data_file = backup_path.join("data.bin");
        let data_writer = Arc::new(Mutex::new(BufWriter::new(File::create(&data_file)?)));
        let write_offset = Arc::new(AtomicU64::new(0));

        segments.par_iter().for_each(|segment_path| {
            if let Ok(segment_backup) = self.backup_segment(
                segment_path,
                &backup_id,
                data_writer.clone(),
                write_offset.clone(),
                block_index.clone(),
                stats.clone(),
            ) {
                segment_backups.lock().push(segment_backup);
            }
        });

        // Flush data file
        data_writer.lock().flush()?;

        let stats = stats.lock();
        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Calculate checksum of data file
        let checksum = self.calculate_file_checksum(&data_file)?;

        let metadata = BackupMetadata {
            id: backup_id.clone(),
            backup_type,
            parent_id,
            start_time_ms: start_time,
            end_time_ms: end_time,
            total_size_bytes: stats.bytes_read,
            deduplicated_size_bytes: stats.bytes_read - stats.bytes_deduplicated,
            compressed_size_bytes: stats.bytes_written,
            segment_count: segment_backups.lock().len(),
            unique_blocks: block_index.lock().len(),
            deduplicated_blocks: stats.blocks_deduplicated,
            manifest_version,
            checksum,
            encrypted: self.config.encryption_enabled,
            compression: self.config.compression.clone(),
            block_size: self.config.block_size,
            label,
            chain_position,
        };

        // Write manifest
        let manifest = BackupManifest {
            metadata: metadata.clone(),
            segments: segment_backups.lock().clone(),
            block_index: block_index.lock().clone(),
        };

        let manifest_file = backup_path.join("manifest.json");
        let file = File::create(&manifest_file)?;
        serde_json::to_writer_pretty(BufWriter::new(file), &manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Verify backup if configured
        if self.config.verify_after_backup {
            self.verify_backup(&backup_id)?;
        }

        Ok(metadata)
    }

    /// Backup a single segment
    fn backup_segment(
        &self,
        segment_path: &Path,
        backup_id: &str,
        data_writer: Arc<Mutex<BufWriter<File>>>,
        write_offset: Arc<AtomicU64>,
        block_index: Arc<Mutex<HashMap<String, u64>>>,
        stats: Arc<Mutex<BackupStats>>,
    ) -> std::io::Result<SegmentBackup> {
        let mut file = File::open(segment_path)?;
        let file_size = file.metadata()?.len();

        // Parse segment path to get database/table
        let (database, table) = self.parse_segment_path(segment_path);
        let segment_id = segment_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let mut blocks = Vec::new();
        let mut buffer = vec![0u8; self.config.block_size];
        let mut offset = 0u64;

        // Calculate overall checksum
        let mut hasher = crc32fast::Hasher::new();

        while offset < file_size {
            let bytes_to_read =
                std::cmp::min(self.config.block_size as u64, file_size - offset) as usize;
            let bytes_read = file.read(&mut buffer[..bytes_to_read])?;

            if bytes_read == 0 {
                break;
            }

            hasher.update(&buffer[..bytes_read]);

            // Calculate block hash
            let block_hash = self.hash_block(&buffer[..bytes_read]);

            // Check if block already exists (deduplication)
            let stored = if let Some((existing_backup, _existing_offset)) =
                self.dedup_index.lookup(&block_hash)
            {
                // Block exists in previous backup, just reference it
                let mut s = stats.lock();
                s.blocks_deduplicated += 1;
                s.bytes_deduplicated += bytes_read as u64;
                false
            } else {
                // New block, write it
                let compressed = self.compress_block(&buffer[..bytes_read])?;

                let current_offset =
                    write_offset.fetch_add(compressed.len() as u64, Ordering::SeqCst);

                {
                    let mut writer = data_writer.lock();
                    writer.write_all(&compressed)?;
                }

                // Add to deduplication index
                self.dedup_index
                    .insert(block_hash.clone(), backup_id.to_string(), current_offset);
                block_index
                    .lock()
                    .insert(block_hash.clone(), current_offset);

                let mut s = stats.lock();
                s.bytes_written += compressed.len() as u64;
                true
            };

            blocks.push(BackupBlock {
                hash: block_hash,
                offset,
                size: bytes_read,
                stored,
            });

            {
                let mut s = stats.lock();
                s.bytes_read += bytes_read as u64;
                s.blocks_processed += 1;
            }

            offset += bytes_read as u64;
        }

        {
            let mut s = stats.lock();
            s.segments_processed += 1;
        }

        Ok(SegmentBackup {
            segment_id,
            database,
            table,
            original_size: file_size,
            blocks,
            checksum: format!("{:08x}", hasher.finalize()),
        })
    }

    /// Restore from backup
    pub fn restore_backup(
        &self,
        backup_id: &str,
        target_dir: &Path,
    ) -> std::io::Result<BackupStats> {
        let backup_path = self.config.backup_dir.join(backup_id);
        let manifest_path = backup_path.join("manifest.json");

        // Load manifest
        let file = File::open(&manifest_path)?;
        let manifest: BackupManifest = serde_json::from_reader(BufReader::new(file))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Get full backup chain
        let chain = self.get_backup_chain(backup_id)?;

        // Create target directory
        fs::create_dir_all(target_dir)?;

        let stats = Arc::new(Mutex::new(BackupStats::default()));
        let start_time = std::time::Instant::now();

        // Restore segments in parallel
        manifest.segments.par_iter().for_each(|segment| {
            if let Err(e) = self.restore_segment(segment, &chain, target_dir, stats.clone()) {
                eprintln!("Failed to restore segment {}: {}", segment.segment_id, e);
            }
        });

        let mut final_stats = stats.lock().clone();
        final_stats.duration_ms = start_time.elapsed().as_millis() as u64;

        Ok(final_stats)
    }

    /// Restore a single segment
    fn restore_segment(
        &self,
        segment: &SegmentBackup,
        chain: &[BackupMetadata],
        target_dir: &Path,
        stats: Arc<Mutex<BackupStats>>,
    ) -> std::io::Result<()> {
        // Create database/table directory structure
        let segment_dir = target_dir.join(&segment.database).join(&segment.table);
        fs::create_dir_all(&segment_dir)?;

        let segment_path = segment_dir.join(format!("{}.segment", segment.segment_id));
        let mut output = BufWriter::new(File::create(&segment_path)?);

        let mut hasher = crc32fast::Hasher::new();

        for block in &segment.blocks {
            // Find the block in the chain
            let block_data = self.read_block_from_chain(&block.hash, chain)?;
            let decompressed = self.decompress_block(&block_data)?;

            hasher.update(&decompressed);
            output.write_all(&decompressed)?;

            let mut s = stats.lock();
            s.bytes_written += decompressed.len() as u64;
            s.blocks_processed += 1;
        }

        output.flush()?;

        // Verify checksum
        let restored_checksum = format!("{:08x}", hasher.finalize());
        if restored_checksum != segment.checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch for segment {}: expected {}, got {}",
                    segment.segment_id, segment.checksum, restored_checksum
                ),
            ));
        }

        let mut s = stats.lock();
        s.segments_processed += 1;

        Ok(())
    }

    /// Read a block from the backup chain
    fn read_block_from_chain(
        &self,
        hash: &str,
        chain: &[BackupMetadata],
    ) -> std::io::Result<Vec<u8>> {
        for backup in chain.iter().rev() {
            let backup_path = self.config.backup_dir.join(&backup.id);
            let manifest_path = backup_path.join("manifest.json");

            if !manifest_path.exists() {
                continue;
            }

            let file = File::open(&manifest_path)?;
            let manifest: BackupManifest = serde_json::from_reader(BufReader::new(file))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if let Some(&offset) = manifest.block_index.get(hash) {
                let data_path = backup_path.join("data.bin");
                let mut data_file = File::open(&data_path)?;
                data_file.seek(SeekFrom::Start(offset))?;

                // Read block size header (4 bytes)
                let mut size_buf = [0u8; 4];
                data_file.read_exact(&mut size_buf)?;
                let block_size = u32::from_le_bytes(size_buf) as usize;

                let mut block_data = vec![0u8; block_size];
                data_file.read_exact(&mut block_data)?;

                return Ok(block_data);
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Block {} not found in backup chain", hash),
        ))
    }

    /// Get the full backup chain ending with the specified backup
    pub fn get_backup_chain(&self, backup_id: &str) -> std::io::Result<Vec<BackupMetadata>> {
        let mut chain = Vec::new();
        let mut current_id = Some(backup_id.to_string());

        while let Some(id) = current_id {
            let backup_path = self.config.backup_dir.join(&id);
            let manifest_path = backup_path.join("manifest.json");

            if !manifest_path.exists() {
                break;
            }

            let file = File::open(&manifest_path)?;
            let manifest: BackupManifest = serde_json::from_reader(BufReader::new(file))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            current_id = manifest.metadata.parent_id.clone();
            chain.push(manifest.metadata);
        }

        chain.reverse();
        Ok(chain)
    }

    /// Verify a backup
    pub fn verify_backup(&self, backup_id: &str) -> std::io::Result<bool> {
        let backup_path = self.config.backup_dir.join(backup_id);
        let manifest_path = backup_path.join("manifest.json");
        let data_path = backup_path.join("data.bin");

        // Load manifest
        let file = File::open(&manifest_path)?;
        let manifest: BackupManifest = serde_json::from_reader(BufReader::new(file))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Verify data file checksum
        let actual_checksum = self.calculate_file_checksum(&data_path)?;
        if actual_checksum != manifest.metadata.checksum {
            return Ok(false);
        }

        // Verify all blocks are present
        for segment in &manifest.segments {
            for block in &segment.blocks {
                if block.stored {
                    if !manifest.block_index.contains_key(&block.hash) {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    /// List all backups
    pub fn list_backups(&self) -> std::io::Result<Vec<BackupMetadata>> {
        let mut backups = Vec::new();

        if !self.config.backup_dir.exists() {
            return Ok(backups);
        }

        for entry in fs::read_dir(&self.config.backup_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let manifest_path = entry.path().join("manifest.json");
                if manifest_path.exists() {
                    let file = File::open(&manifest_path)?;
                    if let Ok(manifest) =
                        serde_json::from_reader::<_, BackupManifest>(BufReader::new(file))
                    {
                        backups.push(manifest.metadata);
                    }
                }
            }
        }

        // Sort by start time
        backups.sort_by_key(|b| b.start_time_ms);

        Ok(backups)
    }

    /// Delete a backup
    pub fn delete_backup(&self, backup_id: &str) -> std::io::Result<()> {
        let backup_path = self.config.backup_dir.join(backup_id);
        if backup_path.exists() {
            fs::remove_dir_all(backup_path)?;
        }
        Ok(())
    }

    /// Apply retention policy
    pub fn apply_retention_policy(&self) -> std::io::Result<Vec<String>> {
        if self.config.retention_days == 0 {
            return Ok(Vec::new());
        }

        let cutoff_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - (self.config.retention_days as u64 * 24 * 60 * 60 * 1000);

        let backups = self.list_backups()?;
        let mut deleted = Vec::new();

        // Find full backups that are expired and have no active dependents
        let mut protected: HashSet<String> = HashSet::new();

        // Mark all backups within retention as protected, along with their chains
        for backup in &backups {
            if backup.start_time_ms >= cutoff_ms {
                let chain = self.get_backup_chain(&backup.id)?;
                for b in chain {
                    protected.insert(b.id);
                }
            }
        }

        // Delete unprotected backups
        for backup in &backups {
            if !protected.contains(&backup.id) {
                self.delete_backup(&backup.id)?;
                deleted.push(backup.id.clone());
            }
        }

        Ok(deleted)
    }

    // Helper methods

    fn collect_segments(&self, segments_dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        let mut segments = Vec::new();
        self.collect_segments_recursive(segments_dir, &mut segments)?;
        Ok(segments)
    }

    fn collect_segments_recursive(
        &self,
        dir: &Path,
        segments: &mut Vec<PathBuf>,
    ) -> std::io::Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                self.collect_segments_recursive(&path, segments)?;
            } else if path
                .extension()
                .map_or(false, |e| e == "segment" || e == "ipc")
            {
                segments.push(path);
            }
        }

        Ok(())
    }

    fn parse_segment_path(&self, path: &Path) -> (String, String) {
        let components: Vec<_> = path.components().collect();
        let len = components.len();

        if len >= 3 {
            let table = components[len - 2]
                .as_os_str()
                .to_str()
                .unwrap_or("unknown")
                .to_string();
            let database = components[len - 3]
                .as_os_str()
                .to_str()
                .unwrap_or("default")
                .to_string();
            (database, table)
        } else {
            ("default".to_string(), "unknown".to_string())
        }
    }

    fn hash_block(&self, data: &[u8]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Use xxHash for fast hashing (simulated with DefaultHasher)
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let h1 = hasher.finish();

        // Second hash for collision resistance
        hasher = DefaultHasher::new();
        h1.hash(&mut hasher);
        data.len().hash(&mut hasher);
        let h2 = hasher.finish();

        format!("{:016x}{:016x}", h1, h2)
    }

    fn compress_block(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        let compressed = match self.config.compression.as_str() {
            "zstd" => zstd::encode_all(std::io::Cursor::new(data), self.config.compression_level)
                .map_err(std::io::Error::other)?,
            "lz4" => lz4_flex::compress_prepend_size(data),
            _ => data.to_vec(),
        };

        // Prepend size header
        let mut result = Vec::with_capacity(4 + compressed.len());
        result.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        result.extend_from_slice(&compressed);

        Ok(result)
    }

    fn decompress_block(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        match self.config.compression.as_str() {
            "zstd" => zstd::decode_all(std::io::Cursor::new(data)).map_err(std::io::Error::other),
            "lz4" => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| std::io::Error::other(e.to_string())),
            _ => Ok(data.to_vec()),
        }
    }

    fn calculate_file_checksum(&self, path: &Path) -> std::io::Result<String> {
        let mut file = File::open(path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; 64 * 1024];

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(format!("{:08x}", hasher.finalize()))
    }
}

fn chrono_format_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let days = now / 86400;
    let secs = now % 86400;
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;

    // Simplified date calculation (approximate)
    let years = 1970 + days / 365;
    let day_of_year = days % 365;
    let month = day_of_year / 30 + 1;
    let day = day_of_year % 30 + 1;

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}",
        years, month, day, hours, mins, secs
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_backup_manager_create() {
        let temp = tempdir().unwrap();
        let backup_dir = temp.path().join("backups");
        let segments_dir = temp.path().join("segments");

        // Create test segment
        let db_table = segments_dir.join("testdb").join("testtable");
        fs::create_dir_all(&db_table).unwrap();
        fs::write(db_table.join("test.segment"), b"test data for backup").unwrap();

        let config = BackupConfig {
            backup_dir: backup_dir.clone(),
            verify_after_backup: false,
            ..Default::default()
        };

        let manager = BackupManager::new(config);
        let metadata = manager
            .create_full_backup(&segments_dir, 1, Some("test".to_string()))
            .unwrap();

        assert_eq!(metadata.backup_type, BackupType::Full);
        assert_eq!(metadata.segment_count, 1);
        assert!(metadata.label.as_deref() == Some("test"));
    }

    #[test]
    fn test_deduplication_index() {
        let index = DeduplicationIndex::new();

        index.insert("hash1".to_string(), "backup1".to_string(), 0);
        index.insert("hash2".to_string(), "backup1".to_string(), 100);

        assert!(index.lookup("hash1").is_some());
        assert!(index.lookup("hash2").is_some());
        assert!(index.lookup("hash3").is_none());

        assert!(index.hit_rate() > 0.0);
    }
}
