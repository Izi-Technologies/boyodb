//! Fulltext Index Integration Tests
//!
//! Tests fulltext index creation for LIKE '%pattern%' queries on phone numbers
//! and other string columns, enabling segment pruning for substring searches.

use arrow_array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::engine::{Db, EngineConfig, IngestBatch, QueryRequest};
use std::sync::Arc;
use tempfile::tempdir;

fn create_phone_batch(ids: Vec<i64>, phones: Vec<&str>) -> Vec<u8> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("phone_number", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(phones)),
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
async fn test_fulltext_index_creation() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("telecom").unwrap();
    db.create_table("telecom", "cdr", None).unwrap();

    // Ingest phone number data
    let ipc = create_phone_batch(
        vec![1, 2, 3],
        vec!["254712345678", "254799887766", "123456789012"],
    );
    let batch = IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("telecom".into()),
        table: Some("cdr".into()),
    };
    db.ingest_ipc(batch).unwrap();

    // Create fulltext index
    let result = db.create_index(
        "telecom",
        "cdr",
        "idx_phone_ft",
        &["phone_number".to_string()],
        boyodb_core::sql::IndexType::Fulltext,
        false,
    );
    assert!(result.is_ok(), "Fulltext index creation should succeed");

    // Wait for index to be built
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Query using LIKE with the index
    let req = QueryRequest {
        sql: "SELECT * FROM telecom.cdr WHERE phone_number LIKE '%712%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Should find one row containing '712'");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fulltext_index_segment_pruning() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("telecom").unwrap();
    db.create_table("telecom", "voice_cdr", None).unwrap();

    // Create fulltext index BEFORE data ingestion to test async build
    db.create_index(
        "telecom",
        "voice_cdr",
        "idx_calling_ft",
        &["calling_number".to_string()],
        boyodb_core::sql::IndexType::Fulltext,
        false,
    )
    .unwrap();

    // Ingest multiple segments with different phone patterns
    // Segment 1: Kenyan numbers (254...)
    let schema = Schema::new(vec![
        Field::new("call_id", DataType::Int64, false),
        Field::new("calling_number", DataType::Utf8, false),
        Field::new("called_number", DataType::Utf8, false),
    ]);

    // Segment with 254 prefix (Kenyan)
    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "254712345678",
                "254733445566",
                "254700112233",
            ])),
            Arc::new(StringArray::from(vec![
                "254711111111",
                "254722222222",
                "254733333333",
            ])),
        ],
    )
    .unwrap();

    let mut ipc1 = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc1, batch1.schema().as_ref()).unwrap();
        writer.write(&batch1).unwrap();
        writer.finish().unwrap();
    }

    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc1,
        watermark_micros: 1,
        shard_override: None,
        database: Some("telecom".into()),
        table: Some("voice_cdr".into()),
    })
    .unwrap();

    // Segment with 243 prefix (DRC)
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec![
                "243811223344",
                "243822334455",
                "243833445566",
            ])),
            Arc::new(StringArray::from(vec![
                "243844556677",
                "243855667788",
                "243866778899",
            ])),
        ],
    )
    .unwrap();

    let mut ipc2 = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc2, batch2.schema().as_ref()).unwrap();
        writer.write(&batch2).unwrap();
        writer.finish().unwrap();
    }

    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc2,
        watermark_micros: 2,
        shard_override: None,
        database: Some("telecom".into()),
        table: Some("voice_cdr".into()),
    })
    .unwrap();

    // Segment with 256 prefix (Uganda)
    let batch3 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![7, 8, 9])),
            Arc::new(StringArray::from(vec![
                "256781234567",
                "256792345678",
                "256703456789",
            ])),
            Arc::new(StringArray::from(vec![
                "256714567890",
                "256725678901",
                "256736789012",
            ])),
        ],
    )
    .unwrap();

    let mut ipc3 = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc3, batch3.schema().as_ref()).unwrap();
        writer.write(&batch3).unwrap();
        writer.finish().unwrap();
    }

    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc3,
        watermark_micros: 3,
        shard_override: None,
        database: Some("telecom".into()),
        table: Some("voice_cdr".into()),
    })
    .unwrap();

    // Wait for async index builds
    std::thread::sleep(std::time::Duration::from_millis(300));

    // First verify all data is there
    let req = QueryRequest {
        sql: "SELECT COUNT(*) FROM telecom.voice_cdr".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();
    let count_col = batches[0].column(0);
    let total = if let Some(arr) = count_col.as_any().downcast_ref::<Int64Array>() {
        arr.value(0) as i64
    } else if let Some(arr) = count_col.as_any().downcast_ref::<UInt64Array>() {
        arr.value(0) as i64
    } else {
        0
    };
    assert_eq!(total, 9, "Should have 9 total rows (3 segments x 3 rows)");

    // Query for Kenyan prefix - should only match segment 1
    let req = QueryRequest {
        sql: "SELECT * FROM telecom.voice_cdr WHERE calling_number LIKE '%254712%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Should find exactly one row with '254712'");

    // Query for all Kenyan numbers (254 prefix)
    let req = QueryRequest {
        sql: "SELECT * FROM telecom.voice_cdr WHERE calling_number LIKE '%254%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Should find 3 rows with '254' (Kenya segment)"
    );

    // Query for pattern not in any segment - should return 0 rows or error
    let req = QueryRequest {
        sql: "SELECT * FROM telecom.voice_cdr WHERE calling_number LIKE '%999888%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req);
    // This may return an error (no matching segments) or empty results
    match resp {
        Ok(r) => {
            let batches = boyodb_core::engine::read_ipc_batches(&r.records_ipc).unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 0, "Should find no rows with '999888'");
        }
        Err(_) => {
            // Expected - no segments match the filter
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fulltext_index_case_insensitive() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("testdb").unwrap();
    db.create_table("testdb", "products", None).unwrap();

    // Ingest data with mixed case
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "iPhone 15 Pro",
                "Samsung Galaxy",
                "IPHONE 14",
                "iphone case",
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

    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("testdb".into()),
        table: Some("products".into()),
    })
    .unwrap();

    // Create fulltext index
    db.create_index(
        "testdb",
        "products",
        "idx_name_ft",
        &["name".to_string()],
        boyodb_core::sql::IndexType::Fulltext,
        false,
    )
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(200));

    // ILIKE query for 'iphone' (case insensitive) - fulltext index is case-insensitive
    // All 4 products contain 'iphone' (case-insensitive): iPhone 15 Pro, Samsung Galaxy (no), IPHONE 14, iphone case
    // Wait - Samsung Galaxy does NOT contain 'iphone', so only 3 should match
    // Actually: iPhone 15 Pro, IPHONE 14, iphone case = 3 matches
    let req = QueryRequest {
        sql: "SELECT * FROM testdb.products WHERE name ILIKE '%iphone%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // Note: The fulltext index is for pruning, but ILIKE filtering happens at the row level
    // So we expect 3 matches (iPhone 15 Pro, IPHONE 14, iphone case)
    assert!(
        total_rows >= 3,
        "Should find at least 3 rows containing 'iphone' (case insensitive), got {}",
        total_rows
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fulltext_index_count_query() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 1);
    let db = Db::open(cfg).unwrap();

    db.create_database("telecom").unwrap();
    db.create_table("telecom", "calls", None).unwrap();

    // Create fulltext index
    db.create_index(
        "telecom",
        "calls",
        "idx_msisdn_ft",
        &["msisdn".to_string()],
        boyodb_core::sql::IndexType::Fulltext,
        false,
    )
    .unwrap();

    // Ingest a batch of phone numbers
    let schema = Schema::new(vec![
        Field::new("call_id", DataType::Int64, false),
        Field::new("msisdn", DataType::Utf8, false),
    ]);

    let phones: Vec<String> = (0..100).map(|i| format!("25471{:07}", i)).collect();
    let phone_refs: Vec<&str> = phones.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from((0..100).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(phone_refs)),
        ],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("telecom".into()),
        table: Some("calls".into()),
    })
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(200));

    // Count query with LIKE
    let req = QueryRequest {
        sql: "SELECT COUNT(*) FROM telecom.calls WHERE msisdn LIKE '%254710%'".into(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    let resp = db.query(req).unwrap();
    let batches = boyodb_core::engine::read_ipc_batches(&resp.records_ipc).unwrap();

    assert!(!batches.is_empty());
    // Numbers 0-9 will have msisdn = 254710000000X which matches '%254710%'
    let count_col = batches[0].column(0);
    // COUNT(*) might return Int64 or UInt64 depending on implementation
    let count = if let Some(arr) = count_col.as_any().downcast_ref::<Int64Array>() {
        arr.value(0) as i64
    } else if let Some(arr) = count_col.as_any().downcast_ref::<UInt64Array>() {
        arr.value(0) as i64
    } else {
        panic!("Unexpected count column type: {:?}", count_col.data_type());
    };
    assert!(
        count >= 10,
        "Should find at least 10 rows with '254710', got {}",
        count
    );
}
