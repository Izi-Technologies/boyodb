//! Point-in-Time Recovery (PITR) System
//!
//! This module provides:
//! - Recovery to specific timestamp
//! - Recovery to specific LSN
//! - Base backup management
//! - WAL replay for recovery

use crate::engine::EngineError;
use crate::replication::ManifestEntry;
use crate::wal_archive::{WalArchiveInfo, WalArchiver};

use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// WAL record header for PITR (mirrors wal.rs structure for deserialization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecordHeader {
    pub lsn: u64,
    pub timestamp_micros: u64,
    pub transaction_id: Option<u64>,
}

/// WAL record data for PITR recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WalRecordData {
    /// Segment data record
    Segment {
        entry: ManifestEntry,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload_path: Option<String>,
    },
    /// Transaction begin record
    TxnBegin {
        txn_id: u64,
        timestamp_micros: u64,
        isolation_level: Option<String>,
    },
    /// Transaction commit record
    TxnCommit {
        txn_id: u64,
        commit_version: u64,
        timestamp_micros: u64,
    },
    /// Transaction abort record
    TxnAbort {
        txn_id: u64,
        timestamp_micros: u64,
    },
    /// Checkpoint record
    Checkpoint {
        lsn: u64,
        timestamp_micros: u64,
        manifest_version: u64,
    },
}

/// Information about a base backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    /// Unique backup identifier
    pub id: String,

    /// Optional user-provided label
    pub label: Option<String>,

    /// LSN at the start of backup
    pub start_lsn: u64,

    /// LSN at the end of backup
    pub end_lsn: u64,

    /// Timestamp when backup started (microseconds since epoch)
    pub start_timestamp_micros: u64,

    /// Timestamp when backup completed
    pub end_timestamp_micros: u64,

    /// Size of the backup in bytes
    pub size_bytes: u64,

    /// Checksum of the backup
    pub checksum: u64,

    /// Path where backup is stored
    pub backup_path: String,

    /// Whether this backup is compressed
    pub compressed: bool,

    /// Tables included in this backup
    pub tables: Vec<(String, String)>, // (database, table)
}

/// Configuration for PITR
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Path where base backups are stored
    pub backup_path: PathBuf,

    /// Path to WAL archive
    pub wal_archive_path: PathBuf,

    /// Path where recovery will be performed
    pub recovery_path: PathBuf,

    /// Whether to verify checksums during recovery
    pub verify_checksums: bool,

    /// Whether to apply all WAL or stop at a specific point
    pub apply_all_wal: bool,

    /// Target timestamp for recovery (if not applying all)
    pub target_timestamp: Option<u64>,

    /// Target LSN for recovery (if not applying all)
    pub target_lsn: Option<u64>,

    /// Recovery target name (for named restore points)
    pub target_name: Option<String>,

    /// Whether this is a standby recovery (don't finalize)
    pub standby_mode: bool,

    /// Maximum number of parallel WAL replay workers
    pub parallel_workers: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        RecoveryConfig {
            backup_path: PathBuf::from("./backups"),
            wal_archive_path: PathBuf::from("./wal_archive"),
            recovery_path: PathBuf::from("./recovery"),
            verify_checksums: true,
            apply_all_wal: true,
            target_timestamp: None,
            target_lsn: None,
            target_name: None,
            standby_mode: false,
            parallel_workers: 4,
        }
    }
}

/// Result of a recovery operation
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Whether recovery was successful
    pub success: bool,

    /// LSN reached after recovery
    pub recovered_lsn: u64,

    /// Timestamp reached after recovery
    pub recovered_timestamp: u64,

    /// Number of WAL segments replayed
    pub wal_segments_replayed: usize,

    /// Total WAL bytes replayed
    pub wal_bytes_replayed: u64,

    /// Duration of the recovery process
    pub duration: Duration,

    /// Any warnings during recovery
    pub warnings: Vec<String>,

    /// Path where recovered data is located
    pub recovery_path: PathBuf,
}

/// Recovery manager handling all PITR operations
pub struct RecoveryManager {
    config: RecoveryConfig,

    /// Index of available backups
    backup_index: parking_lot::RwLock<Vec<BackupInfo>>,
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub fn new(config: RecoveryConfig) -> Result<Self, EngineError> {
        // Create necessary directories
        fs::create_dir_all(&config.backup_path).map_err(|e| {
            EngineError::Io(format!(
                "Failed to create backup directory {:?}: {}",
                config.backup_path, e
            ))
        })?;
        fs::create_dir_all(&config.recovery_path).map_err(|e| {
            EngineError::Io(format!(
                "Failed to create recovery directory {:?}: {}",
                config.recovery_path, e
            ))
        })?;

        let manager = RecoveryManager {
            config,
            backup_index: parking_lot::RwLock::new(Vec::new()),
        };

        // Load backup index
        manager.load_backup_index()?;

        Ok(manager)
    }

    /// Create a base backup
    pub fn create_backup(
        &self,
        data_path: &Path,
        label: Option<String>,
    ) -> Result<BackupInfo, EngineError> {
        let start_time = std::time::Instant::now();
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        // Generate backup ID
        let backup_id = format!("backup_{}", start_timestamp);
        let backup_dir = self.config.backup_path.join(&backup_id);

        fs::create_dir_all(&backup_dir)
            .map_err(|e| EngineError::Io(format!("Failed to create backup directory: {}", e)))?;

        // Copy data files
        // In a real implementation, this would:
        // 1. Request a checkpoint from the database
        // 2. Copy data files while tracking LSN
        // 3. Record start/end LSN
        let mut total_size = 0u64;
        let mut tables = Vec::new();

        // Copy manifest
        let manifest_src = data_path.join("manifest.bin");
        let manifest_dst = backup_dir.join("manifest.bin");
        if manifest_src.exists() {
            fs::copy(&manifest_src, &manifest_dst)
                .map_err(|e| EngineError::Io(format!("Failed to copy manifest: {}", e)))?;
            total_size += fs::metadata(&manifest_dst).map(|m| m.len()).unwrap_or(0);
        }

        // Copy segments directory
        let segments_src = data_path.join("segments");
        let segments_dst = backup_dir.join("segments");
        if segments_src.exists() {
            self.copy_directory(&segments_src, &segments_dst, &mut total_size, &mut tables)?;
        }

        let end_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        // Calculate checksum of manifest
        let checksum = if manifest_dst.exists() {
            let data = fs::read(&manifest_dst).unwrap_or_default();
            xxhash_rust::xxh64::xxh64(&data, 0)
        } else {
            0
        };

        // TODO: Get actual LSN from database
        let start_lsn = 0;
        let end_lsn = 0;

        let info = BackupInfo {
            id: backup_id,
            label,
            start_lsn,
            end_lsn,
            start_timestamp_micros: start_timestamp,
            end_timestamp_micros: end_timestamp,
            size_bytes: total_size,
            checksum,
            backup_path: backup_dir.to_string_lossy().to_string(),
            compressed: false,
            tables,
        };

        // Add to index
        {
            let mut index = self.backup_index.write();
            index.push(info.clone());
        }
        self.save_backup_index()?;

        tracing::info!(
            "Created backup {} ({} bytes) in {:?}",
            info.id,
            info.size_bytes,
            start_time.elapsed()
        );

        Ok(info)
    }

    /// Recover to a specific timestamp
    pub fn recover_to_time(&self, target_micros: u64) -> Result<RecoveryResult, EngineError> {
        let start_time = std::time::Instant::now();
        let mut warnings = Vec::new();

        // Find the best base backup (latest backup before target time)
        let backup = self.find_backup_for_recovery(Some(target_micros), None)?;

        tracing::info!(
            "Starting PITR to timestamp {} using backup {}",
            target_micros,
            backup.id
        );

        // Restore base backup
        self.restore_backup(&backup)?;

        // Open WAL archiver
        let wal_config = crate::wal_archive::WalArchiveConfig {
            archive_path: self.config.wal_archive_path.clone(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(wal_config)?;

        // Get WAL segments needed for recovery
        let wal_segments = archiver.get_archives_for_recovery(target_micros);

        // Replay WAL up to target time
        let (final_lsn, final_timestamp, segments_replayed, bytes_replayed) = self.replay_wal(
            &archiver,
            &wal_segments,
            Some(target_micros),
            None,
            &mut warnings,
        )?;

        // Finalize recovery
        if !self.config.standby_mode {
            self.finalize_recovery()?;
        }

        Ok(RecoveryResult {
            success: true,
            recovered_lsn: final_lsn,
            recovered_timestamp: final_timestamp,
            wal_segments_replayed: segments_replayed,
            wal_bytes_replayed: bytes_replayed,
            duration: start_time.elapsed(),
            warnings,
            recovery_path: self.config.recovery_path.clone(),
        })
    }

    /// Recover to a specific LSN
    pub fn recover_to_lsn(&self, target_lsn: u64) -> Result<RecoveryResult, EngineError> {
        let start_time = std::time::Instant::now();
        let mut warnings = Vec::new();

        // Find the best base backup
        let backup = self.find_backup_for_recovery(None, Some(target_lsn))?;

        tracing::info!(
            "Starting PITR to LSN {} using backup {}",
            target_lsn,
            backup.id
        );

        // Restore base backup
        self.restore_backup(&backup)?;

        // Open WAL archiver
        let wal_config = crate::wal_archive::WalArchiveConfig {
            archive_path: self.config.wal_archive_path.clone(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(wal_config)?;

        // Get WAL segments needed
        let wal_segments = archiver.get_archives_for_lsn(target_lsn);

        // Replay WAL up to target LSN
        let (final_lsn, final_timestamp, segments_replayed, bytes_replayed) = self.replay_wal(
            &archiver,
            &wal_segments,
            None,
            Some(target_lsn),
            &mut warnings,
        )?;

        // Finalize recovery
        if !self.config.standby_mode {
            self.finalize_recovery()?;
        }

        Ok(RecoveryResult {
            success: true,
            recovered_lsn: final_lsn,
            recovered_timestamp: final_timestamp,
            wal_segments_replayed: segments_replayed,
            wal_bytes_replayed: bytes_replayed,
            duration: start_time.elapsed(),
            warnings,
            recovery_path: self.config.recovery_path.clone(),
        })
    }

    /// Recover to the latest available state
    pub fn recover_latest(&self) -> Result<RecoveryResult, EngineError> {
        let start_time = std::time::Instant::now();
        let mut warnings = Vec::new();

        // Find the latest backup
        let backup = self
            .list_backups()
            .into_iter()
            .max_by_key(|b| b.end_timestamp_micros)
            .ok_or_else(|| EngineError::NotFound("No backups available".to_string()))?;

        tracing::info!("Starting recovery to latest using backup {}", backup.id);

        // Restore base backup
        self.restore_backup(&backup)?;

        // Open WAL archiver
        let wal_config = crate::wal_archive::WalArchiveConfig {
            archive_path: self.config.wal_archive_path.clone(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(wal_config)?;

        // Get all WAL segments after backup
        let wal_segments = archiver.list_archives(Some(backup.end_timestamp_micros), None);

        // Replay all WAL
        let (final_lsn, final_timestamp, segments_replayed, bytes_replayed) =
            self.replay_wal(&archiver, &wal_segments, None, None, &mut warnings)?;

        // Finalize recovery
        if !self.config.standby_mode {
            self.finalize_recovery()?;
        }

        Ok(RecoveryResult {
            success: true,
            recovered_lsn: final_lsn,
            recovered_timestamp: final_timestamp,
            wal_segments_replayed: segments_replayed,
            wal_bytes_replayed: bytes_replayed,
            duration: start_time.elapsed(),
            warnings,
            recovery_path: self.config.recovery_path.clone(),
        })
    }

    /// List available backups
    pub fn list_backups(&self) -> Vec<BackupInfo> {
        self.backup_index.read().clone()
    }

    /// Show WAL status
    pub fn wal_status(&self) -> Result<WalStatus, EngineError> {
        let wal_config = crate::wal_archive::WalArchiveConfig {
            archive_path: self.config.wal_archive_path.clone(),
            ..Default::default()
        };
        let archiver = WalArchiver::new(wal_config)?;
        let stats = archiver.stats();

        Ok(WalStatus {
            archive_path: self.config.wal_archive_path.clone(),
            segment_count: stats.segment_count,
            total_bytes: stats.total_bytes,
            oldest_timestamp: stats.oldest_timestamp,
            newest_timestamp: stats.newest_timestamp,
            oldest_lsn: stats.oldest_lsn,
            newest_lsn: stats.newest_lsn,
        })
    }

    /// Delete a backup
    pub fn delete_backup(&self, backup_id: &str) -> Result<(), EngineError> {
        let backup = {
            let index = self.backup_index.read();
            index
                .iter()
                .find(|b| b.id == backup_id)
                .cloned()
                .ok_or_else(|| EngineError::NotFound(format!("Backup {} not found", backup_id)))?
        };

        // Delete backup directory
        let backup_path = PathBuf::from(&backup.backup_path);
        if backup_path.exists() {
            fs::remove_dir_all(&backup_path).map_err(|e| {
                EngineError::Io(format!("Failed to delete backup directory: {}", e))
            })?;
        }

        // Remove from index
        {
            let mut index = self.backup_index.write();
            index.retain(|b| b.id != backup_id);
        }
        self.save_backup_index()?;

        tracing::info!("Deleted backup {}", backup_id);
        Ok(())
    }

    // Internal helper methods

    fn find_backup_for_recovery(
        &self,
        target_timestamp: Option<u64>,
        target_lsn: Option<u64>,
    ) -> Result<BackupInfo, EngineError> {
        let index = self.backup_index.read();

        // Filter backups that are before the target
        let candidates: Vec<&BackupInfo> = index
            .iter()
            .filter(|b| {
                let time_ok = target_timestamp
                    .map(|t| b.end_timestamp_micros <= t)
                    .unwrap_or(true);
                let lsn_ok = target_lsn.map(|l| b.end_lsn <= l).unwrap_or(true);
                time_ok && lsn_ok
            })
            .collect();

        // Find the most recent qualifying backup
        candidates
            .into_iter()
            .max_by_key(|b| b.end_timestamp_micros)
            .cloned()
            .ok_or_else(|| {
                EngineError::NotFound("No suitable backup found for recovery target".to_string())
            })
    }

    fn restore_backup(&self, backup: &BackupInfo) -> Result<(), EngineError> {
        let backup_path = PathBuf::from(&backup.backup_path);

        // Clear recovery directory
        if self.config.recovery_path.exists() {
            fs::remove_dir_all(&self.config.recovery_path).map_err(|e| {
                EngineError::Io(format!("Failed to clear recovery directory: {}", e))
            })?;
        }
        fs::create_dir_all(&self.config.recovery_path)
            .map_err(|e| EngineError::Io(format!("Failed to create recovery directory: {}", e)))?;

        // Copy backup files to recovery directory
        let mut total_size = 0u64;
        let mut tables = Vec::new();
        self.copy_directory(
            &backup_path,
            &self.config.recovery_path,
            &mut total_size,
            &mut tables,
        )?;

        // Verify checksum if configured
        if self.config.verify_checksums {
            let manifest_path = self.config.recovery_path.join("manifest.bin");
            if manifest_path.exists() {
                let data = fs::read(&manifest_path)
                    .map_err(|e| EngineError::Io(format!("Failed to read manifest: {}", e)))?;
                let checksum = xxhash_rust::xxh64::xxh64(&data, 0);
                if checksum != backup.checksum {
                    return Err(EngineError::Internal(format!(
                        "Backup checksum mismatch: expected {:x}, got {:x}",
                        backup.checksum, checksum
                    )));
                }
            }
        }

        tracing::info!(
            "Restored backup {} to {:?}",
            backup.id,
            self.config.recovery_path
        );
        Ok(())
    }

    fn replay_wal(
        &self,
        archiver: &WalArchiver,
        segments: &[WalArchiveInfo],
        target_timestamp: Option<u64>,
        target_lsn: Option<u64>,
        warnings: &mut Vec<String>,
    ) -> Result<(u64, u64, usize, u64), EngineError> {
        use std::io::{BufRead, BufReader, Cursor};

        let mut current_lsn = 0u64;
        let mut current_timestamp = 0u64;
        let mut segments_replayed = 0usize;
        let mut bytes_replayed = 0u64;
        let mut records_applied = 0usize;

        // Track active transactions during replay
        let mut active_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for segment in segments {
            // Check if we should stop based on segment boundaries
            if let Some(target_ts) = target_timestamp {
                if segment.start_timestamp_micros > target_ts {
                    tracing::debug!(
                        "Stopping WAL replay: segment {} starts after target timestamp",
                        segment.filename
                    );
                    break;
                }
            }
            if let Some(target) = target_lsn {
                if segment.start_lsn > target {
                    tracing::debug!(
                        "Stopping WAL replay: segment {} starts after target LSN",
                        segment.filename
                    );
                    break;
                }
            }

            // Retrieve WAL segment data
            let wal_data = archiver.retrieve_segment(segment)?;
            let reader = BufReader::new(Cursor::new(&wal_data));

            // Parse and apply each WAL record line
            for line_result in reader.lines() {
                let line = match line_result {
                    Ok(l) => l,
                    Err(e) => {
                        warnings.push(format!(
                            "Failed to read WAL line in {}: {}",
                            segment.filename, e
                        ));
                        continue;
                    }
                };

                if line.trim().is_empty() {
                    continue;
                }

                // Parse the WAL record (JSON format with header + record)
                let record_result = self.parse_wal_record(&line);
                let (header, record) = match record_result {
                    Ok(r) => r,
                    Err(e) => {
                        warnings.push(format!(
                            "Failed to parse WAL record in {}: {}",
                            segment.filename, e
                        ));
                        continue;
                    }
                };

                // Check if we've reached the target
                if let Some(target_ts) = target_timestamp {
                    if header.timestamp_micros > target_ts {
                        tracing::debug!(
                            "Stopping WAL replay at LSN {}: reached target timestamp",
                            header.lsn
                        );
                        break;
                    }
                }
                if let Some(target) = target_lsn {
                    if header.lsn > target {
                        tracing::debug!(
                            "Stopping WAL replay at LSN {}: reached target LSN",
                            header.lsn
                        );
                        break;
                    }
                }

                // Apply the WAL record
                match self.apply_wal_record(&header, &record, &mut active_txns, warnings) {
                    Ok(applied) => {
                        if applied {
                            records_applied += 1;
                        }
                    }
                    Err(e) => {
                        warnings.push(format!(
                            "Failed to apply WAL record LSN {}: {}",
                            header.lsn, e
                        ));
                    }
                }

                current_lsn = header.lsn;
                current_timestamp = header.timestamp_micros;
            }

            segments_replayed += 1;
            bytes_replayed += wal_data.len() as u64;

            tracing::info!(
                "Replayed WAL segment {} (LSN {}-{}, {} records applied)",
                segment.filename,
                segment.start_lsn,
                segment.end_lsn,
                records_applied
            );
        }

        // Warn about any uncommitted transactions
        if !active_txns.is_empty() {
            warnings.push(format!(
                "{} transactions were not committed at recovery point: {:?}",
                active_txns.len(),
                active_txns
            ));
        }

        Ok((
            current_lsn,
            current_timestamp,
            segments_replayed,
            bytes_replayed,
        ))
    }

    /// Parse a single WAL record from a JSON line
    fn parse_wal_record(
        &self,
        line: &str,
    ) -> Result<(WalRecordHeader, WalRecordData), EngineError> {
        // WAL records are stored as JSON with format: {"header": {...}, "record": {...}}
        #[derive(serde::Deserialize)]
        struct WalLine {
            header: WalRecordHeader,
            record: WalRecordData,
        }

        let parsed: WalLine = serde_json::from_str(line)
            .map_err(|e| EngineError::Internal(format!("Invalid WAL record format: {}", e)))?;

        Ok((parsed.header, parsed.record))
    }

    /// Apply a single WAL record during recovery
    fn apply_wal_record(
        &self,
        header: &WalRecordHeader,
        record: &WalRecordData,
        active_txns: &mut std::collections::HashSet<u64>,
        warnings: &mut Vec<String>,
    ) -> Result<bool, EngineError> {
        match record {
            WalRecordData::Segment {
                entry,
                payload_path,
            } => {
                // Reconstruct segment from WAL
                // The payload is stored in a separate file referenced by payload_path
                let payload = if let Some(path) = payload_path {
                    let full_path = self.config.recovery_path.join(path);
                    if full_path.exists() {
                        fs::read(&full_path).map_err(|e| {
                            EngineError::Io(format!("Failed to read segment payload: {}", e))
                        })?
                    } else {
                        warnings.push(format!("Segment payload not found: {:?}", full_path));
                        return Ok(false);
                    }
                } else {
                    warnings.push(format!(
                        "Segment record without payload path: {}",
                        entry.segment_id
                    ));
                    return Ok(false);
                };

                // Write segment to recovery directory
                let segments_dir = self.config.recovery_path.join("segments");
                fs::create_dir_all(&segments_dir).map_err(|e| {
                    EngineError::Io(format!("Failed to create segments dir: {}", e))
                })?;

                let segment_path = segments_dir.join(&entry.segment_id);
                fs::write(&segment_path, &payload).map_err(|e| {
                    EngineError::Io(format!("Failed to write segment: {}", e))
                })?;

                tracing::debug!(
                    "Applied segment record: {} ({} bytes)",
                    entry.segment_id,
                    payload.len()
                );
                Ok(true)
            }
            WalRecordData::TxnBegin { txn_id, .. } => {
                active_txns.insert(*txn_id);
                tracing::trace!("WAL replay: transaction {} started", txn_id);
                Ok(true)
            }
            WalRecordData::TxnCommit { txn_id, .. } => {
                active_txns.remove(txn_id);
                tracing::trace!("WAL replay: transaction {} committed", txn_id);
                Ok(true)
            }
            WalRecordData::TxnAbort { txn_id, .. } => {
                active_txns.remove(txn_id);
                tracing::trace!("WAL replay: transaction {} aborted", txn_id);
                Ok(true)
            }
            WalRecordData::Checkpoint {
                manifest_version, ..
            } => {
                tracing::debug!(
                    "WAL replay: checkpoint at LSN {}, manifest version {}",
                    header.lsn,
                    manifest_version
                );
                Ok(true)
            }
        }
    }

    fn finalize_recovery(&self) -> Result<(), EngineError> {
        // Write recovery completion marker
        let marker_path = self.config.recovery_path.join(".recovery_complete");
        let marker_data = format!(
            "Recovery completed at {}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros())
                .unwrap_or(0)
        );
        fs::write(&marker_path, marker_data)
            .map_err(|e| EngineError::Io(format!("Failed to write recovery marker: {}", e)))?;

        tracing::info!("Recovery finalized at {:?}", self.config.recovery_path);
        Ok(())
    }

    fn copy_directory(
        &self,
        src: &Path,
        dst: &Path,
        total_size: &mut u64,
        tables: &mut Vec<(String, String)>,
    ) -> Result<(), EngineError> {
        self.copy_directory_inner(src, dst, total_size, tables, None, None)
    }

    fn copy_directory_inner(
        &self,
        src: &Path,
        dst: &Path,
        total_size: &mut u64,
        tables: &mut Vec<(String, String)>,
        current_database: Option<&str>,
        current_table: Option<&str>,
    ) -> Result<(), EngineError> {
        if !src.is_dir() {
            return Ok(());
        }

        fs::create_dir_all(dst)
            .map_err(|e| EngineError::Io(format!("Failed to create directory {:?}: {}", dst, e)))?;

        for entry in fs::read_dir(src)
            .map_err(|e| EngineError::Io(format!("Failed to read directory {:?}: {}", src, e)))?
        {
            let entry = entry
                .map_err(|e| EngineError::Io(format!("Failed to read directory entry: {}", e)))?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            let entry_name = entry
                .file_name()
                .to_string_lossy()
                .to_string();

            if src_path.is_dir() {
                // Detect database and table directories based on path structure
                // Structure is typically: data/{database}/{table}/segments/
                let (new_db, new_table) = if current_database.is_none()
                    && src_path.parent().map(|p| p.ends_with("data")).unwrap_or(false)
                {
                    // This is a database directory
                    (Some(entry_name.as_str()), None)
                } else if current_database.is_some() && current_table.is_none() {
                    // This is a table directory
                    let db = current_database.unwrap().to_string();
                    let table = entry_name.clone();
                    tables.push((db, table.clone()));
                    (current_database, Some(entry_name.as_str()))
                } else {
                    (current_database, current_table)
                };

                self.copy_directory_inner(
                    &src_path,
                    &dst_path,
                    total_size,
                    tables,
                    new_db,
                    new_table,
                )?;
            } else {
                fs::copy(&src_path, &dst_path).map_err(|e| {
                    EngineError::Io(format!("Failed to copy {:?}: {}", src_path, e))
                })?;
                *total_size += fs::metadata(&dst_path).map(|m| m.len()).unwrap_or(0);
            }
        }

        Ok(())
    }

    fn load_backup_index(&self) -> Result<(), EngineError> {
        let index_path = self.config.backup_path.join("backup_index.json");
        if !index_path.exists() {
            return Ok(());
        }

        let file = File::open(&index_path)
            .map_err(|e| EngineError::Io(format!("Failed to open backup index: {}", e)))?;
        let reader = BufReader::new(file);
        let index: Vec<BackupInfo> = serde_json::from_reader(reader)
            .map_err(|e| EngineError::Internal(format!("Failed to parse backup index: {}", e)))?;

        *self.backup_index.write() = index;
        Ok(())
    }

    fn save_backup_index(&self) -> Result<(), EngineError> {
        let index_path = self.config.backup_path.join("backup_index.json");
        let index = self.backup_index.read();

        let file = File::create(&index_path)
            .map_err(|e| EngineError::Io(format!("Failed to create backup index: {}", e)))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &*index)
            .map_err(|e| EngineError::Internal(format!("Failed to write backup index: {}", e)))?;

        Ok(())
    }
}

/// WAL status information
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// Path to WAL archive
    pub archive_path: PathBuf,

    /// Number of archived segments
    pub segment_count: usize,

    /// Total size of archives
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_manager() -> (RecoveryManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let config = RecoveryConfig {
            backup_path: dir.path().join("backups"),
            wal_archive_path: dir.path().join("wal_archive"),
            recovery_path: dir.path().join("recovery"),
            ..Default::default()
        };
        let manager = RecoveryManager::new(config).unwrap();
        (manager, dir)
    }

    #[test]
    fn test_create_backup() {
        let (manager, dir) = create_test_manager();

        // Create test data directory
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        fs::write(data_dir.join("manifest.bin"), b"test manifest").unwrap();

        let segments_dir = data_dir.join("segments");
        fs::create_dir_all(&segments_dir).unwrap();
        fs::write(segments_dir.join("seg1.dat"), b"segment data").unwrap();

        // Create backup
        let backup = manager
            .create_backup(&data_dir, Some("test backup".to_string()))
            .unwrap();

        assert_eq!(backup.label, Some("test backup".to_string()));
        assert!(backup.size_bytes > 0);

        // Verify backup files exist
        let backup_path = PathBuf::from(&backup.backup_path);
        assert!(backup_path.join("manifest.bin").exists());
        assert!(backup_path.join("segments").join("seg1.dat").exists());
    }

    #[test]
    fn test_list_backups() {
        let (manager, dir) = create_test_manager();

        // Create test data
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        fs::write(data_dir.join("manifest.bin"), b"test").unwrap();

        // Create multiple backups
        manager
            .create_backup(&data_dir, Some("backup1".to_string()))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        manager
            .create_backup(&data_dir, Some("backup2".to_string()))
            .unwrap();

        let backups = manager.list_backups();
        assert_eq!(backups.len(), 2);
    }

    #[test]
    fn test_delete_backup() {
        let (manager, dir) = create_test_manager();

        // Create test data
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        fs::write(data_dir.join("manifest.bin"), b"test").unwrap();

        // Create and delete backup
        let backup = manager.create_backup(&data_dir, None).unwrap();
        let backup_path = PathBuf::from(&backup.backup_path);

        assert!(backup_path.exists());
        manager.delete_backup(&backup.id).unwrap();
        assert!(!backup_path.exists());
        assert!(manager.list_backups().is_empty());
    }

    #[test]
    fn test_restore_backup() {
        let (manager, dir) = create_test_manager();

        // Create test data
        let data_dir = dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        fs::write(data_dir.join("manifest.bin"), b"test manifest data").unwrap();

        // Create backup
        let backup = manager.create_backup(&data_dir, None).unwrap();

        // Restore it
        manager.restore_backup(&backup).unwrap();

        // Verify restored files
        let restored_manifest = manager.config.recovery_path.join("manifest.bin");
        assert!(restored_manifest.exists());
        assert_eq!(
            fs::read_to_string(&restored_manifest).unwrap(),
            "test manifest data"
        );
    }
}
