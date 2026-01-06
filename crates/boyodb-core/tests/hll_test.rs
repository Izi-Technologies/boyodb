use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::{Db, EngineConfig, IngestBatch, QueryRequest};
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
        name: "val".to_string(),
        data_type: "uint64".to_string(),
        nullable: false,
    }];
    serde_json::to_string(&fields).unwrap()
}

fn build_payload(values: &[u64]) -> Vec<u8> {
    let schema = Schema::new(vec![Field::new("val", DataType::UInt64, false)]);
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
        .with_wal_max_bytes(1024 * 1024)
        .with_enable_compaction(false)
}

#[tokio::test]
async fn test_approx_count_distinct_accuracy() {
    let dir = tempdir().unwrap();
    let cfg = make_config(dir.path());

    let db = Db::open(cfg).unwrap();
    db.create_database("default").unwrap();
    db.create_table("default", "events", Some(schema_json()))
        .unwrap();

    // Generate data with known cardinality
    // 1000 items, 100 distinct values (0..100 repeating)
    let mut values = Vec::new();
    for i in 0..1000 {
        values.push(i % 100);
    }

    let payload = build_payload(&values);
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload,
        watermark_micros: 0,
        shard_override: None,
        database: Some("default".into()),
        table: Some("events".into()),
    })
    .unwrap();

    // Query 1: APPROX_COUNT_DISTINCT
    let response = db
        .query(QueryRequest {
            sql: "SELECT APPROX_COUNT_DISTINCT(val) FROM events".to_string(),
            timeout_millis: 1000,
            collect_stats: false,
        })
        .unwrap();
    
    let mut reader = StreamReader::try_new(std::io::Cursor::new(response.records_ipc), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    
    // Check schema
    // Expecting: [count, approx_count_distinct_val] (assuming global agg adds count?) 
    // Wait, global agg without group by returns count + fields.
    // Let's print schema to debug if needed.
    
    // The field name should be "approx_count_distinct_val"
    let schema = batch.schema();
    let col_idx = schema.index_of("approx_count_distinct_val").unwrap();
    
    let col = batch.column(col_idx).as_any().downcast_ref::<UInt64Array>().unwrap();
    let val = col.value(0);
    
    println!("Approximate Count Distinct: {}", val);
    
    // HLL accuracy for 100 distinct items with p=12 should be very close to 100.
    // Allow error margin.
    assert!(val >= 98 && val <= 102, "Expected ~100, got {}", val);
    
    // Query 2: Exact COUNT(DISTINCT) for comparison
    let response = db
        .query(QueryRequest {
            sql: "SELECT COUNT(DISTINCT val) FROM events".to_string(),
            timeout_millis: 1000,
            collect_stats: false,
        })
        .unwrap();
    let mut reader = StreamReader::try_new(std::io::Cursor::new(response.records_ipc), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    let schema = batch.schema();
    let col_idx = schema.index_of("count_distinct_val").unwrap();
    let col = batch.column(col_idx).as_any().downcast_ref::<UInt64Array>().unwrap();
    let val_exact = col.value(0);
    
    assert_eq!(val_exact, 100);
}
