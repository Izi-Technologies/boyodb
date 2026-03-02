use crate::engine::{
    compute_checksum, compute_schema_hash_from_payload, load_manifest, persist_manifest,
    persist_segment_ipc, EngineError,
};
use crate::replication::ManifestEntry;
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize)]
enum WalRecord {
    Segment {
        entry: ManifestEntry,
        payload: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct Wal {
    writer: BufWriter<File>,
    path: std::path::PathBuf,
    pending_bytes: usize,
    max_segments: u64,
    last_sync: Instant,
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
        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            pending_bytes: 0,
            max_segments: 4,
            last_sync: Instant::now(),
        })
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

    fn flush_sync(&mut self) -> Result<(), EngineError> {
        self.writer
            .flush()
            .map_err(|e| EngineError::Io(format!("wal flush failed: {e}")))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| EngineError::Io(format!("wal fsync failed: {e}")))?;
        self.pending_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    pub fn replay(&mut self, storage: &crate::storage::TieredStorage, manifest_path: &Path) -> Result<(), EngineError> {
        let mut manifest = load_manifest(manifest_path)?;
        let mut manifest_changed = false;

        for path in wal_paths_for_replay(&self.path)? {
            replay_wal_file(&path, storage, &mut manifest, &mut manifest_changed)?;
        }
        if manifest_changed {
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
        let rotated = rotate_path(&self.path)?;
        std::fs::rename(&self.path, &rotated)
            .map_err(|e| EngineError::Io(format!("wal rotate rename failed: {e}")))?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| EngineError::Io(format!("wal rotate open failed: {e}")))?;
        self.writer = BufWriter::new(file);
        self.pending_bytes = 0;
        info!(
            "wal rotated: size={} > max_bytes={}, new_file={:?}, keep_max_segments={}",
            len, max_bytes, rotated, self.max_segments
        );
        cleanup_old_segments(&self.path, self.max_segments)?;
        Ok(())
    }

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

fn replay_wal_file(
    path: &Path,
    storage: &crate::storage::TieredStorage,
    manifest: &mut crate::replication::Manifest,
    manifest_changed: &mut bool,
) -> Result<(), EngineError> {
    let file = File::open(path)
        .map_err(|e| EngineError::Io(format!("open wal for replay failed: {e}")))?;
    let reader = BufReader::new(file);

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
                let actual = compute_checksum(&payload);
                if actual != entry.checksum {
                    return Err(EngineError::Io(format!(
                        "wal checksum mismatch for segment {} expected={} actual={}",
                        entry.segment_id, entry.checksum, actual
                    )));
                }
                if let Some(expected_schema_hash) = entry.schema_hash {
                    let actual_schema_hash = compute_schema_hash_from_payload(
                        &payload,
                        entry.compression.as_deref(),
                    )?;
                    if actual_schema_hash != expected_schema_hash {
                        return Err(EngineError::Io(format!(
                            "wal schema hash mismatch for segment {} expected={} actual={}",
                            entry.segment_id, expected_schema_hash, actual_schema_hash
                        )));
                    }
                }
                // Persist segment file and manifest entry if missing.
                persist_segment_ipc(storage, &entry.segment_id, &payload)?;
                if !manifest
                    .entries
                    .iter()
                    .any(|e| e.segment_id == entry.segment_id)
                {
                    manifest.bump_version();
                    let mut entry = entry;
                    entry.version_added = manifest.version;
                    manifest.entries.push(entry);
                    *manifest_changed = true;
                }
            }
        }
    }
    Ok(())
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
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(UInt64Array::from(values))],
        )
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
