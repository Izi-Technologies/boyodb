use crate::engine::{
    compute_checksum, compute_schema_hash_from_payload, load_manifest, persist_manifest,
    persist_segment_ipc, EngineError,
};
use crate::replication::ManifestEntry;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Header for each WAL record with LSN and timestamp for PITR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecordHeader {
    /// Log Sequence Number - monotonically increasing
    pub lsn: u64,
    /// Wall clock time in microseconds since epoch
    pub timestamp_micros: u64,
    /// Transaction ID if this record is part of a transaction
    pub transaction_id: Option<u64>,
}

impl WalRecordHeader {
    pub fn new(lsn: u64, transaction_id: Option<u64>) -> Self {
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        WalRecordHeader {
            lsn,
            timestamp_micros,
            transaction_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum WalRecord {
    /// Segment data record (existing)
    Segment {
        entry: ManifestEntry,
        payload: Vec<u8>,
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
    TxnAbort { txn_id: u64, timestamp_micros: u64 },
    /// Checkpoint record (for recovery starting point)
    Checkpoint {
        lsn: u64,
        timestamp_micros: u64,
        manifest_version: u64,
    },
}

/// Callback type for WAL archiving
pub type ArchiveCallback = Box<dyn Fn(&Path, u64, u64) -> Result<(), EngineError> + Send + Sync>;

pub struct Wal {
    writer: BufWriter<File>,
    path: std::path::PathBuf,
    pending_bytes: usize,
    max_segments: u64,
    last_sync: Instant,
    /// Current Log Sequence Number
    current_lsn: std::sync::atomic::AtomicU64,
    /// First LSN in current WAL file
    file_start_lsn: u64,
    /// First timestamp in current WAL file
    file_start_timestamp: u64,
    /// Archive callback for PITR support
    archive_callback: Option<std::sync::Arc<ArchiveCallback>>,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("pending_bytes", &self.pending_bytes)
            .field("max_segments", &self.max_segments)
            .field("current_lsn", &self.current_lsn)
            .field("file_start_lsn", &self.file_start_lsn)
            .field("archive_callback", &self.archive_callback.is_some())
            .finish()
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.writer.flush();
    }
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self, EngineError> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)
                .map_err(|e| EngineError::Io(format!("create wal dir failed: {e}")))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| EngineError::Io(format!("open wal failed: {e}")))?;

        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        // Recover the highest LSN from existing WAL records
        let recovered_lsn = Self::recover_max_lsn(path)?;
        let next_lsn = recovered_lsn.map(|l| l + 1).unwrap_or(1);
        let file_start_lsn = recovered_lsn.unwrap_or(0) + 1;

        info!("WAL opened: recovered_lsn={:?}, next_lsn={}", recovered_lsn, next_lsn);

        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            pending_bytes: 0,
            max_segments: 4,
            last_sync: Instant::now(),
            current_lsn: std::sync::atomic::AtomicU64::new(next_lsn),
            file_start_lsn,
            file_start_timestamp: now_micros,
            archive_callback: None,
        })
    }

    /// Scan WAL files to recover the highest LSN
    /// Uses both metadata file and record counting for reliability
    fn recover_max_lsn(path: &Path) -> Result<Option<u64>, EngineError> {
        // First, try to read from metadata file
        let metadata_path = Self::lsn_metadata_path(path);
        if let Ok(content) = std::fs::read_to_string(&metadata_path) {
            if let Ok(lsn) = content.trim().parse::<u64>() {
                info!("Recovered LSN {} from metadata file", lsn);
                return Ok(Some(lsn));
            }
        }

        // Fall back to counting records in WAL files
        let wal_paths = wal_paths_for_replay(path)?;
        if wal_paths.is_empty() {
            return Ok(None);
        }

        let mut total_records: u64 = 0;
        let mut max_checkpoint_lsn: Option<u64> = None;

        for wal_path in wal_paths {
            if let Ok(file) = File::open(&wal_path) {
                let reader = BufReader::new(file);
                for line in reader.lines().map_while(Result::ok) {
                    total_records += 1;

                    // Also check for explicit LSN in checkpoint records
                    if let Some(lsn) = Self::extract_lsn_from_record(&line) {
                        max_checkpoint_lsn = Some(max_checkpoint_lsn.map(|m| m.max(lsn)).unwrap_or(lsn));
                    }
                }
            }
        }

        // Use the higher of: checkpoint LSN or record count
        let recovered = match max_checkpoint_lsn {
            Some(ckpt_lsn) => Some(ckpt_lsn.max(total_records)),
            None if total_records > 0 => Some(total_records),
            None => None,
        };

        info!("Recovered LSN from WAL scan: {:?} (records={}, checkpoint_lsn={:?})",
            recovered, total_records, max_checkpoint_lsn);

        Ok(recovered)
    }

    /// Extract LSN from a WAL record line (if present)
    fn extract_lsn_from_record(line: &str) -> Option<u64> {
        // Records are formatted as: "LENGTH JSON_DATA"
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return None;
        }

        // Try to parse as JSON and extract LSN from checkpoint records
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(parts[1]) {
            // Check for checkpoint LSN
            if let Some(lsn) = value.get("Checkpoint").and_then(|c| c.get("lsn")).and_then(|l| l.as_u64()) {
                return Some(lsn);
            }
        }

        None
    }

    /// Get the path to the LSN metadata file
    fn lsn_metadata_path(wal_path: &Path) -> PathBuf {
        wal_path.with_extension("lsn")
    }

    /// Persist the current LSN to metadata file
    pub fn persist_lsn(&self) -> Result<(), EngineError> {
        let metadata_path = Self::lsn_metadata_path(&self.path);
        let lsn = self.current_lsn();
        std::fs::write(&metadata_path, lsn.to_string())
            .map_err(|e| EngineError::Io(format!("Failed to persist LSN: {}", e)))?;
        Ok(())
    }

    /// Set the archive callback for PITR support
    pub fn set_archive_callback<F>(&mut self, callback: F)
    where
        F: Fn(&Path, u64, u64) -> Result<(), EngineError> + Send + Sync + 'static,
    {
        self.archive_callback = Some(std::sync::Arc::new(Box::new(callback)));
    }

    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the next LSN and increment
    fn next_lsn(&self) -> u64 {
        self.current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Append a transaction begin record
    pub fn append_txn_begin(
        &mut self,
        txn_id: u64,
        isolation_level: Option<&str>,
    ) -> Result<u64, EngineError> {
        let lsn = self.next_lsn();
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let rec = WalRecord::TxnBegin {
            txn_id,
            timestamp_micros,
            isolation_level: isolation_level.map(String::from),
        };
        self.write_record(&rec)?;
        Ok(lsn)
    }

    /// Append a transaction commit record
    pub fn append_txn_commit(
        &mut self,
        txn_id: u64,
        commit_version: u64,
    ) -> Result<u64, EngineError> {
        let lsn = self.next_lsn();
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let rec = WalRecord::TxnCommit {
            txn_id,
            commit_version,
            timestamp_micros,
        };
        self.write_record(&rec)?;
        Ok(lsn)
    }

    /// Append a transaction abort record
    pub fn append_txn_abort(&mut self, txn_id: u64) -> Result<u64, EngineError> {
        let lsn = self.next_lsn();
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let rec = WalRecord::TxnAbort {
            txn_id,
            timestamp_micros,
        };
        self.write_record(&rec)?;
        Ok(lsn)
    }

    /// Append a checkpoint record
    pub fn append_checkpoint(&mut self, manifest_version: u64) -> Result<u64, EngineError> {
        let lsn = self.next_lsn();
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let rec = WalRecord::Checkpoint {
            lsn,
            timestamp_micros,
            manifest_version,
        };
        self.write_record(&rec)?;
        Ok(lsn)
    }

    /// Write a record to the WAL
    fn write_record(&mut self, rec: &WalRecord) -> Result<(), EngineError> {
        let bytes = serde_json::to_vec(rec)
            .map_err(|e| EngineError::Internal(format!("wal encode: {e}")))?;
        let len_line = format!("{} ", bytes.len());
        self.writer
            .write_all(len_line.as_bytes())
            .and_then(|_| self.writer.write_all(&bytes))
            .and_then(|_| self.writer.write_all(b"\n"))
            .map_err(|e| EngineError::Io(format!("wal append failed: {e}")))?;

        self.pending_bytes += len_line.len() + bytes.len() + 1;
        Ok(())
    }

    pub fn append_segment(
        &mut self,
        entry: &ManifestEntry,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let rec = WalRecord::Segment {
            entry: entry.clone(),
            payload: payload.to_vec(),
        };
        let bytes = serde_json::to_vec(&rec)
            .map_err(|e| EngineError::Internal(format!("wal encode: {e}")))?;
        let len_line = format!("{} ", bytes.len());
        self.writer
            .write_all(len_line.as_bytes())
            .and_then(|_| self.writer.write_all(&bytes))
            .and_then(|_| self.writer.write_all(b"\n"))
            .map_err(|e| EngineError::Io(format!("wal append failed: {e}")))?;

        self.pending_bytes += len_line.len() + bytes.len() + 1;
        Ok(())
    }

    pub fn maybe_sync(
        &mut self,
        sync_bytes: u64,
        sync_interval_ms: u64,
    ) -> Result<(), EngineError> {
        if sync_bytes == 0 && sync_interval_ms == 0 {
            return self.flush_sync();
        }
        let mut should_sync = false;
        if sync_bytes > 0 && self.pending_bytes as u64 >= sync_bytes {
            should_sync = true;
        }
        if sync_interval_ms > 0
            && self.last_sync.elapsed() >= Duration::from_millis(sync_interval_ms)
        {
            should_sync = true;
        }
        if should_sync {
            self.flush_sync()?;
        }
        Ok(())
    }

    pub fn flush_sync(&mut self) -> Result<(), EngineError> {
        self.writer
            .flush()
            .map_err(|e| EngineError::Io(format!("wal flush failed: {e}")))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| EngineError::Io(format!("wal fsync failed: {e}")))?;
        self.pending_bytes = 0;
        self.last_sync = Instant::now();
        // Persist LSN to metadata file for recovery
        let _ = self.persist_lsn();
        Ok(())
    }

    pub fn replay(
        &mut self,
        storage: &crate::storage::TieredStorage,
        manifest_path: &Path,
    ) -> Result<(), EngineError> {
        let mut manifest = load_manifest(manifest_path)?;

        // Build HashSet of existing segment IDs for O(1) lookup during replay
        // This avoids O(n²) complexity when replaying 40K+ segments
        let existing_ids: HashSet<String> = manifest
            .entries
            .iter()
            .map(|e| e.segment_id.clone())
            .collect();

        let wal_paths = wal_paths_for_replay(&self.path)?;

        if wal_paths.is_empty() {
            return Ok(());
        }

        // For large WAL replays, process files in parallel
        // Each file collects its own new entries, then we merge at the end
        let results: Vec<Result<Vec<(ManifestEntry, Vec<u8>)>, EngineError>> = wal_paths
            .par_iter()
            .map(|path| replay_wal_file_parallel(path, &existing_ids))
            .collect();

        // Collect all new entries and persist segments
        let mut new_entries = Vec::new();
        let mut seen_in_replay: HashSet<String> = HashSet::new();

        for result in results {
            let entries = result?;
            for (entry, payload) in entries {
                // Skip if we've already seen this segment in this replay
                if seen_in_replay.contains(&entry.segment_id) {
                    continue;
                }
                seen_in_replay.insert(entry.segment_id.clone());

                // Persist segment file
                persist_segment_ipc(storage, &entry.segment_id, &payload)?;

                // Only add to manifest if not already present
                if !existing_ids.contains(&entry.segment_id) {
                    new_entries.push(entry);
                }
            }
        }

        // Update manifest with new entries
        if !new_entries.is_empty() {
            for entry in new_entries {
                manifest.bump_version();
                let mut entry = entry;
                entry.version_added = manifest.version;
                manifest.entries.push(entry);
            }
            persist_manifest(manifest_path, &manifest)?;
        }

        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), EngineError> {
        self.flush_sync()?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| EngineError::Io(format!("wal checkpoint open failed: {e}")))?;
        self.writer = BufWriter::new(file);
        self.pending_bytes = 0;
        self.last_sync = Instant::now();
        cleanup_rotated_segments(&self.path)?;
        Ok(())
    }

    pub fn set_max_segments(&mut self, max_segments: u64) {
        self.max_segments = max_segments.max(1);
    }

    pub fn maybe_rotate(&mut self, max_bytes: u64) -> Result<(), EngineError> {
        self.writer
            .flush()
            .map_err(|e| EngineError::Io(format!("wal flush failed: {e}")))?;
        let len = std::fs::metadata(&self.path)
            .map_err(|e| EngineError::Io(format!("wal metadata failed: {e}")))?
            .len();
        if len <= max_bytes {
            return Ok(());
        }

        let end_lsn = self.current_lsn();
        let start_lsn = self.file_start_lsn;

        let rotated = rotate_path(&self.path)?;
        std::fs::rename(&self.path, &rotated)
            .map_err(|e| EngineError::Io(format!("wal rotate rename failed: {e}")))?;

        // Call archive callback if set (for PITR)
        if let Some(ref callback) = self.archive_callback {
            if let Err(e) = callback(&rotated, start_lsn, end_lsn) {
                warn!("WAL archive callback failed: {:?}", e);
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| EngineError::Io(format!("wal rotate open failed: {e}")))?;
        self.writer = BufWriter::new(file);
        self.pending_bytes = 0;

        // Update file start tracking for new file
        self.file_start_lsn = end_lsn;
        self.file_start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        info!(
            "wal rotated: size={} > max_bytes={}, new_file={:?}, keep_max_segments={}, lsn_range={}..{}",
            len, max_bytes, rotated, self.max_segments, start_lsn, end_lsn
        );
        cleanup_old_segments(&self.path, self.max_segments)?;
        Ok(())
    }

    /// Get WAL status information for monitoring
    pub fn status(&self) -> WalStatus {
        WalStatus {
            current_lsn: self.current_lsn(),
            file_start_lsn: self.file_start_lsn,
            file_start_timestamp: self.file_start_timestamp,
            pending_bytes: self.pending_bytes,
            path: self.path.clone(),
        }
    }
}

/// WAL status information
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// Current LSN
    pub current_lsn: u64,
    /// First LSN in current file
    pub file_start_lsn: u64,
    /// First timestamp in current file
    pub file_start_timestamp: u64,
    /// Bytes pending sync
    pub pending_bytes: usize,
    /// WAL file path
    pub path: PathBuf,
}

fn rotate_path(path: &Path) -> Result<PathBuf, EngineError> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| EngineError::Internal(format!("time error: {e}")))?;
    let rotated = path.with_extension(format!("log.{}", ts.as_millis()));
    Ok(rotated)
}

fn cleanup_old_segments(current_path: &Path, max_segments: u64) -> Result<(), EngineError> {
    let parent = current_path
        .parent()
        .ok_or_else(|| EngineError::Internal("wal path missing parent".into()))?;
    let mut entries: Vec<_> = std::fs::read_dir(parent)
        .map_err(|e| EngineError::Io(format!("wal cleanup read_dir failed: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            if let Some(name) = e.file_name().to_str() {
                name.starts_with("wal.log.")
            } else {
                false
            }
        })
        .collect();
    entries.sort_by_key(|e| e.metadata().and_then(|m| m.modified()).ok());
    while entries.len() as u64 > max_segments {
        if let Some(entry) = entries.first() {
            let _ = std::fs::remove_file(entry.path());
        }
        entries.remove(0);
    }
    Ok(())
}

fn wal_paths_for_replay(current_path: &Path) -> Result<Vec<PathBuf>, EngineError> {
    let parent = current_path
        .parent()
        .ok_or_else(|| EngineError::Internal("wal path missing parent".into()))?;
    let mut rotated: Vec<_> = std::fs::read_dir(parent)
        .map_err(|e| EngineError::Io(format!("wal replay read_dir failed: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            if let Some(name) = e.file_name().to_str() {
                name.starts_with("wal.log.")
            } else {
                false
            }
        })
        .collect();
    rotated.sort_by_key(|e| e.metadata().and_then(|m| m.modified()).ok());
    let mut paths = rotated.into_iter().map(|e| e.path()).collect::<Vec<_>>();
    if current_path.exists() {
        paths.push(current_path.to_path_buf());
    }
    Ok(paths)
}

/// Parallel-safe WAL file replay that returns entries instead of modifying manifest.
/// This enables parallel processing of multiple WAL files.
fn replay_wal_file_parallel(
    path: &Path,
    existing_ids: &HashSet<String>,
) -> Result<Vec<(ManifestEntry, Vec<u8>)>, EngineError> {
    let file = File::open(path)
        .map_err(|e| EngineError::Io(format!("open wal for replay failed: {e}")))?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file); // 8MB buffer for NVMe throughput

    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| EngineError::Io(format!("read wal failed: {e}")))?;
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, ' ');
        let len_str = parts
            .next()
            .ok_or_else(|| EngineError::Internal("wal: missing length".into()))?;
        let rest = parts
            .next()
            .ok_or_else(|| EngineError::Internal("wal: missing payload".into()))?;
        let len: usize = len_str
            .parse()
            .map_err(|e| EngineError::Internal(format!("wal: bad length {e}")))?;
        if rest.len() < len {
            warn!(
                "wal truncated tail: expected {} bytes, got {} - stopping replay",
                len,
                rest.len()
            );
            break;
        }
        let payload = &rest.as_bytes()[..len];
        let rec: WalRecord = serde_json::from_slice(payload)
            .map_err(|e| EngineError::Internal(format!("wal decode failed: {e}")))?;

        match rec {
            WalRecord::Segment { entry, payload } => {
                // Skip segments already in manifest (O(1) lookup)
                if existing_ids.contains(&entry.segment_id) {
                    continue;
                }

                let actual = compute_checksum(&payload);
                if actual != entry.checksum {
                    return Err(EngineError::Io(format!(
                        "wal checksum mismatch for segment {} expected={} actual={}",
                        entry.segment_id, entry.checksum, actual
                    )));
                }
                if let Some(expected_schema_hash) = entry.schema_hash {
                    let actual_schema_hash =
                        compute_schema_hash_from_payload(&payload, entry.compression.as_deref())?;
                    if actual_schema_hash != expected_schema_hash {
                        return Err(EngineError::Io(format!(
                            "wal schema hash mismatch for segment {} expected={} actual={}",
                            entry.segment_id, expected_schema_hash, actual_schema_hash
                        )));
                    }
                }

                entries.push((entry, payload));
            }
            // Transaction records are processed separately during recovery
            WalRecord::TxnBegin { .. }
            | WalRecord::TxnCommit { .. }
            | WalRecord::TxnAbort { .. }
            | WalRecord::Checkpoint { .. } => {
                // Transaction records are logged but not replayed for segment recovery
                // They would be used by the transaction manager during PITR
            }
        }
    }

    Ok(entries)
}

fn cleanup_rotated_segments(current_path: &Path) -> Result<(), EngineError> {
    let parent = current_path
        .parent()
        .ok_or_else(|| EngineError::Internal("wal path missing parent".into()))?;
    let entries: Vec<_> = std::fs::read_dir(parent)
        .map_err(|e| EngineError::Io(format!("wal cleanup read_dir failed: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            if let Some(name) = e.file_name().to_str() {
                name.starts_with("wal.log.")
            } else {
                false
            }
        })
        .collect();
    for entry in entries {
        let _ = std::fs::remove_file(entry.path());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::SegmentTier;
    use arrow_array::{RecordBatch, UInt64Array};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn build_payload(values: Vec<u64>) -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("event_time", DataType::UInt64, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(UInt64Array::from(values))])
                .unwrap();

        let mut payload = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut payload, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        payload
    }

    fn build_entry(segment_id: &str, payload: &[u8]) -> ManifestEntry {
        let checksum = compute_checksum(payload);
        let schema_hash = compute_schema_hash_from_payload(payload, None).unwrap();
        ManifestEntry {
            segment_id: segment_id.to_string(),
            shard_id: 0,
            version_added: 0,
            size_bytes: payload.len() as u64,
            checksum,
            tier: SegmentTier::Hot,
            compression: None,
            database: "default".into(),
            table: "default".into(),
            watermark_micros: 0,
            event_time_min: None,
            event_time_max: None,
            tenant_id_min: None,
            tenant_id_max: None,
            route_id_min: None,
            route_id_max: None,
            bloom_tenant: None,
            bloom_route: None,
            column_stats: None,
            schema_hash: Some(schema_hash),
            created_txn: None,
            deleted_txn: None,
            deleted_version: None,
        }
    }

    #[test]
    fn replay_tolerates_truncated_tail() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let segments_dir = dir.path().join("segments");
        let manifest_path = dir.path().join("manifest.json");
        std::fs::create_dir_all(&segments_dir).unwrap();

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.set_max_segments(4);

        let payload = build_payload(vec![1u64, 2u64]);
        let entry = build_entry("seg-0-0", &payload);

        wal.append_segment(&entry, &payload).unwrap();
        wal.maybe_sync(0, 0).unwrap();

        // Append a truncated record to simulate crash during write
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(b"999 ").unwrap();
            f.flush().unwrap();
        }

        let mut wal = Wal::open(&wal_path).unwrap();
        let storage = crate::storage::TieredStorage::new_local_only(segments_dir.clone());
        wal.replay(&storage, &manifest_path).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].schema_hash, entry.schema_hash);
    }

    #[test]
    fn open_creates_parent_dirs() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("nested/a/b/wal.log");
        let mut wal = Wal::open(&wal_path).unwrap();

        let payload = build_payload(vec![42u64]);
        let entry = build_entry("seg-1", &payload);

        wal.append_segment(&entry, &payload).unwrap();
        wal.maybe_sync(0, 0).unwrap();

        assert!(wal_path.parent().unwrap().is_dir());
        let meta = std::fs::metadata(&wal_path).unwrap();
        assert!(meta.len() > 0);
    }

    #[test]
    fn replay_reads_rotated_segments() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let segments_dir = dir.path().join("segments");
        let manifest_path = dir.path().join("manifest.json");
        std::fs::create_dir_all(&segments_dir).unwrap();

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.set_max_segments(4);

        let payload1 = build_payload(vec![1u64, 2u64]);
        let entry1 = build_entry("seg-0-0", &payload1);
        wal.append_segment(&entry1, &payload1).unwrap();
        wal.maybe_sync(0, 0).unwrap();
        wal.maybe_rotate(1).unwrap();

        let payload2 = build_payload(vec![3u64, 4u64]);
        let entry2 = build_entry("seg-0-1", &payload2);
        wal.append_segment(&entry2, &payload2).unwrap();
        wal.maybe_sync(0, 0).unwrap();

        let mut wal = Wal::open(&wal_path).unwrap();
        let storage = crate::storage::TieredStorage::new_local_only(segments_dir.clone());
        wal.replay(&storage, &manifest_path).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        let mut ids = manifest
            .entries
            .iter()
            .map(|e| e.segment_id.as_str())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec!["seg-0-0", "seg-0-1"]);
    }

    #[test]
    fn checkpoint_truncates_and_cleans_rotated_segments() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.set_max_segments(4);

        let payload1 = build_payload(vec![10u64]);
        let entry1 = build_entry("seg-0-0", &payload1);
        wal.append_segment(&entry1, &payload1).unwrap();
        wal.maybe_sync(0, 0).unwrap();
        wal.maybe_rotate(1).unwrap();

        let payload2 = build_payload(vec![11u64]);
        let entry2 = build_entry("seg-0-1", &payload2);
        wal.append_segment(&entry2, &payload2).unwrap();
        wal.maybe_sync(0, 0).unwrap();

        wal.checkpoint().unwrap();

        let wal_len = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(wal_len, 0);

        let rotated_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("wal.log."))
            .count();
        assert_eq!(rotated_count, 0);
    }
}
