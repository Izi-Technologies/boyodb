//! Hash Index Integration Tests
//!
//! Tests hash index creation, segment pruning, and query optimization.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::engine::{Db, EngineConfig, IngestBatch, QueryRequest};
use std::sync::Arc;
use tempfile::tempdir;

fn create_test_batch(id: i64, name: &str) -> Vec<u8> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    ipc
}

fn create_multi_row_batch(ids: Vec<i64>, names: Vec<&str>) -> Vec<u8> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    ipc
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_creation() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("testdb").unwrap();
    db.create_table("testdb", "users", None).unwrap();

    // Ingest data
    let ipc = create_multi_row_batch(vec![1, 2, 3], vec!["alice", "bob", "carol"]);
    let batch = IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("testdb".into()),
        table: Some("users".into()),
    };
    db.ingest_ipc(batch).unwrap();

    // Create hash index
    let result = db.create_index(
        "testdb",
        "users",
        "idx_id",
        &["id".to_string()],
        boyodb_core::sql::IndexType::Hash,
        false,
    );
    assert!(result.is_ok(), "Hash index creation should succeed");

    // Wait for index to be built
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Query using the index
    let req = QueryRequest {
        sql: "SELECT * FROM testdb.users WHERE id = 1".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_segment_pruning() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("testdb").unwrap();
    db.create_table("testdb", "events", None).unwrap();

    // Create hash index BEFORE data ingestion
    db.create_index(
        "testdb",
        "events",
        "idx_user_id",
        &["user_id".to_string()],
        boyodb_core::sql::IndexType::Hash,
        false,
    )
    .unwrap();

    // Ingest multiple segments
    for i in 0..5 {
        let schema = Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("event", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![i * 10, i * 10 + 1, i * 10 + 2])),
                Arc::new(StringArray::from(vec![
                    format!("event_a_{}", i),
                    format!("event_b_{}", i),
                    format!("event_c_{}", i),
                ])),
            ],
        )
        .unwrap();

        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let ingest = IngestBatch {
            payload_ipc: ipc,
            watermark_micros: i as u64,
            shard_override: None,
            database: Some("testdb".into()),
            table: Some("events".into()),
        };
        db.ingest_ipc(ingest).unwrap();
    }

    // Wait for async index builds
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Query for a specific user_id that only exists in one segment
    // user_id = 22 should only be in segment where i=2 (values: 20, 21, 22)
    let req = QueryRequest {
        sql: "SELECT * FROM testdb.events WHERE user_id = 22".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Should find exactly one row with user_id=22");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_multiple_equality_values() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("testdb").unwrap();
    db.create_table("testdb", "products", None).unwrap();

    // Ingest data
    let schema = Schema::new(vec![
        Field::new("category_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3, 3])),
            Arc::new(StringArray::from(vec![
                "prod_a", "prod_b", "prod_c", "prod_d", "prod_e", "prod_f",
            ])),
        ],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let ingest = IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("testdb".into()),
        table: Some("products".into()),
    };
    db.ingest_ipc(ingest).unwrap();

    // Create hash index
    db.create_index(
        "testdb",
        "products",
        "idx_category",
        &["category_id".to_string()],
        boyodb_core::sql::IndexType::Hash,
        false,
    )
    .unwrap();

    // Wait for index
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Query with equality filter
    let req = QueryRequest {
        sql: "SELECT * FROM testdb.products WHERE category_id = 2".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should find two rows with category_id=2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hash_index_on_string_column() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("testdb").unwrap();
    db.create_table("testdb", "users", None).unwrap();

    // Ingest data with string IDs
    let schema = Schema::new(vec![
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave"])),
            Arc::new(StringArray::from(vec![
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
                "dave@example.com",
            ])),
        ],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    let ingest = IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("testdb".into()),
        table: Some("users".into()),
    };
    db.ingest_ipc(ingest).unwrap();

    // Create hash index on string column
    db.create_index(
        "testdb",
        "users",
        "idx_username",
        &["username".to_string()],
        boyodb_core::sql::IndexType::Hash,
        false,
    )
    .unwrap();

    // Wait for index
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Query with string equality
    let req = QueryRequest {
        sql: "SELECT * FROM testdb.users WHERE username = 'bob'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1,
        "Should find exactly one row with username='bob'"
    );
}
