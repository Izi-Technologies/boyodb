use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::{Db, EngineConfig, IngestBatch, Manifest};
use serde::Serialize;
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Serialize)]
struct FieldSpec {
    name: String,
    data_type: String,
    nullable: bool,
}

fn schema_json() -> String {
    let fields = vec![FieldSpec {
        name: "event_time".to_string(),
        data_type: "uint64".to_string(),
        nullable: false,
    }];
    serde_json::to_string(&fields).unwrap()
}

fn build_payload(values: &[u64]) -> Vec<u8> {
    let schema = Schema::new(vec![Field::new("event_time", DataType::UInt64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(UInt64Array::from(values.to_vec()))],
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

fn make_config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig::new(dir, 1)
        .with_wal_max_bytes(1)
        .with_wal_max_segments(4)
        .with_enable_compaction(false)
        .with_auto_compact_interval_secs(0)
}

#[tokio::test]
async fn db_replay_reads_rotated_wal_segments() {
    let dir = tempdir().unwrap();
    let cfg = make_config(dir.path());
    let manifest_path = cfg.manifest_path.clone();
    let manifest_snapshot_path = cfg.manifest_snapshot_path.clone();

    {
        let db = Db::open(cfg.clone()).unwrap();
        db.create_database("default").unwrap();
        db.create_table("default", "events", Some(schema_json()))
            .unwrap();

        let payload1 = build_payload(&[1, 2]);
        db.ingest_ipc(IngestBatch {
            payload_ipc: payload1,
            watermark_micros: 0,
            shard_override: None,
            database: Some("default".into()),
            table: Some("events".into()),
        })
        .unwrap();

        let payload2 = build_payload(&[3, 4]);
        db.ingest_ipc(IngestBatch {
            payload_ipc: payload2,
            watermark_micros: 0,
            shard_override: None,
            database: Some("default".into()),
            table: Some("events".into()),
        })
        .unwrap();
    }

    let _ = std::fs::remove_file(&manifest_path);
    let _ = std::fs::remove_file(&manifest_snapshot_path);

    let db = Db::open(cfg).unwrap();
    let manifest_bytes = db.export_manifest().unwrap();
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.entries.len(), 2);
}

#[tokio::test]
async fn db_checkpoint_truncates_wal() {
    let dir = tempdir().unwrap();
    let cfg = make_config(dir.path());
    let wal_dir = cfg.wal_dir.clone();
    let wal_path = cfg.wal_path.clone();

    let db = Db::open(cfg).unwrap();
    db.create_database("default").unwrap();
    db.create_table("default", "events", Some(schema_json()))
        .unwrap();

    let payload1 = build_payload(&[10, 11]);
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload1,
        watermark_micros: 0,
        shard_override: None,
        database: Some("default".into()),
        table: Some("events".into()),
    })
    .unwrap();

    let payload2 = build_payload(&[12, 13]);
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload2,
        watermark_micros: 0,
        shard_override: None,
        database: Some("default".into()),
        table: Some("events".into()),
    })
    .unwrap();

    db.checkpoint().unwrap();

    let wal_len = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(wal_len, 0);

    let rotated_count = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("wal.log."))
        .count();
    assert_eq!(rotated_count, 0);
}
