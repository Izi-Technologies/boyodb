use boyodb_core::engine::{Db, EngineConfig};
use boyodb_core::storage::TieredStorage;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::tempdir;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_manual_tiering_simulation() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let s3_dir = dir.path().join("s3");
    std::fs::create_dir_all(&s3_dir).unwrap();

    let cfg = EngineConfig::new(&data_dir, 1)
        .with_allow_manifest_import(true);
    
    // 1. Ingest data (Hot)
    {
        let db = Db::open(cfg.clone()).unwrap();
        db.create_database("default").unwrap();
        db.create_table("default", "test_tier", None).unwrap();
        
        let payload = generate_ipc_payload();
        let batch = boyodb_core::engine::IngestBatch {
            payload_ipc: payload,
            watermark_micros: 0,
            shard_override: None,
            database: Some("default".into()),
            table: Some("test_tier".into()),
        };
        db.ingest_ipc(batch).unwrap();
        db.checkpoint().unwrap();
        
        // Use export/import manifest to modify tier
        let json = db.export_manifest().unwrap();
        let mut manifest: boyodb_core::replication::Manifest = serde_json::from_slice(&json).unwrap();
        
        // Assert we have entries
        assert!(!manifest.entries.is_empty());
        
        // Modify first entry to be Cold
        let entry = &mut manifest.entries[0];
        let segment_id = entry.segment_id.clone();
        entry.tier = boyodb_core::replication::SegmentTier::Cold;
        
        // Import back
        let new_json = serde_json::to_vec(&manifest).unwrap();
        db.import_manifest(&new_json, true).unwrap();
        
        // Move file physical location to "S3"
        let segments_dir = cfg.segments_dir.clone();
        let src_path = segments_dir.join(format!("{}.ipc", segment_id));
        let dst_path = s3_dir.join(format!("{}.ipc", segment_id));
        std::fs::rename(&src_path, &dst_path).unwrap();
    } 

    // 2. Restart DB using injected "S3" storage
    {
        let remote_store = Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
        // Use the new testing constructor
        let storage = Arc::new(TieredStorage::new_with_remote(&cfg, remote_store).unwrap());
        
        // Use the new injection point
        let db = Db::open_with_storage(cfg.clone(), storage).unwrap();
        
        // Query
        let req = boyodb_core::engine::QueryRequest {
            sql: "SELECT * FROM test_tier".into(),
            timeout_millis: 1000,
            collect_stats: false,
        };
        let resp = db.query(req).unwrap();
        let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();
        
        assert!(batches.len() > 0);
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(count, 3);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_automated_tiering() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let s3_dir = dir.path().join("s3");
    std::fs::create_dir_all(&s3_dir).unwrap();

    // Set aggressive tiering policy: cold after 100ms
    let mut cfg = EngineConfig::new(&data_dir, 1);
    cfg.tier_cold_after_millis = 100;
    // We must provide an S3 config to enable tiering logic (even if we override storage later)
    // Actually, Db::open_with_storage injects storage, but TieringManager checks config.
    // TieredStorage needs remote to work.
    
    // Inject S3 storage
    let remote_store = Arc::new(LocalFileSystem::new_with_prefix(&s3_dir).unwrap());
    let storage = Arc::new(TieredStorage::new_with_remote(&cfg, remote_store).unwrap());

    // Open DB
    let db = Db::open_with_storage(cfg.clone(), storage).unwrap();
    db.create_database("default").unwrap();
    db.create_table("default", "auto_tier", None).unwrap();

    let payload = generate_ipc_payload();
    let batch = boyodb_core::engine::IngestBatch {
        payload_ipc: payload,
        watermark_micros: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as u64,
        shard_override: None,
        database: Some("default".into()),
        table: Some("auto_tier".into()),
    };
    db.ingest_ipc(batch).unwrap();
    db.checkpoint().unwrap();

    // Wait for tiering (needs > 100ms + loop interval)
    // Loop checks every 100ms if threshold < 1000ms
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify manifest updated to Cold
    let json = db.export_manifest().unwrap();
    let manifest: boyodb_core::replication::Manifest = serde_json::from_slice(&json).unwrap();
    let entry = &manifest.entries[0];
    assert_eq!(entry.tier, boyodb_core::replication::SegmentTier::Cold, "Segment should be tiered to Cold");
    
    // Verify file moved to S3
    let s3_path = s3_dir.join(format!("{}.ipc", entry.segment_id));
    assert!(s3_path.exists(), "Segment file should exist in S3 dir");
    
    // Verify local file deleted
    let local_path = cfg.segments_dir.join(format!("{}.ipc", entry.segment_id));
    assert!(!local_path.exists(), "Local segment file should be deleted");
    
    // Verify Query works
    let req = boyodb_core::engine::QueryRequest {
        sql: "SELECT * FROM auto_tier".into(),
        timeout_millis: 1000,
        collect_stats: false,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

fn generate_ipc_payload() -> Vec<u8> {
    use arrow_array::{RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use arrow_ipc::writer::StreamWriter;

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}
