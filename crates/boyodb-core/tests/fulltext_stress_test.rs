//! Stress tests for fulltext index - tests for race conditions and memory safety
//!
//! Run with thread sanitizer:
//!   RUSTFLAGS="-Z sanitizer=thread" cargo +nightly test -p boyodb-core --test fulltext_stress_test -- --test-threads=1
//!
//! Run with address sanitizer (memory leaks):
//!   RUSTFLAGS="-Z sanitizer=address" cargo +nightly test -p boyodb-core --test fulltext_stress_test -- --test-threads=1

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::engine::{Db, EngineConfig, IngestBatch, QueryRequest};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;
use tokio;

fn create_phone_batch(start_id: i64, count: usize) -> Vec<u8> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("phone_number", DataType::Utf8, false),
    ]);

    let ids: Vec<i64> = (start_id..start_id + count as i64).collect();
    let phones: Vec<String> = ids
        .iter()
        .map(|i| format!("254{:09}", i % 1_000_000_000))
        .collect();
    let phone_refs: Vec<&str> = phones.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
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
    ipc
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_fulltext_index_concurrent_build_and_query() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 4); // 4 workers
    let db = Arc::new(Db::open(cfg).unwrap());

    db.create_database("stress").unwrap();
    db.create_table("stress", "phones", None).unwrap();

    // Create fulltext index
    db.create_index(
        "stress",
        "phones",
        "idx_phone_ft",
        &["phone_number".to_string()],
        boyodb_core::sql::IndexType::Fulltext,
        false,
    )
    .unwrap();

    // Spawn multiple threads that ingest data concurrently
    let mut handles = vec![];
    for i in 0..4 {
        let db_clone = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for j in 0..10 {
                let batch_id = i * 1000 + j * 100;
                let ipc = create_phone_batch(batch_id, 50);
                let batch = IngestBatch {
                    payload_ipc: ipc,
                    watermark_micros: (i * 1000 + j) as u64,
                    shard_override: None,
                    database: Some("stress".into()),
                    table: Some("phones".into()),
                };
                db_clone.ingest_ipc(batch).unwrap();
            }
        }));
    }

    // Wait for all ingestion threads
    for h in handles {
        h.join().unwrap();
    }

    // Wait for index builds
    thread::sleep(std::time::Duration::from_millis(500));

    // Spawn multiple threads that query concurrently
    let mut query_handles = vec![];
    for i in 0..8 {
        let db_clone = Arc::clone(&db);
        query_handles.push(thread::spawn(move || {
            for _ in 0..5 {
                let pattern = format!("%254{}%", i);
                let sql = format!(
                    "SELECT COUNT(*) FROM stress.phones WHERE phone_number LIKE '{}'",
                    pattern
                );
                let req = QueryRequest {
                    sql,
                    timeout_millis: 5000,
                    collect_stats: false,
                    transaction_id: None,
                };
                let _ = db_clone.query(req); // May fail if no segments match, that's ok
            }
        }));
    }

    // Wait for all query threads
    for h in query_handles {
        h.join().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fulltext_index_repeated_build_drop() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path(), 2);
    let db = Db::open(cfg).unwrap();

    db.create_database("cycle").unwrap();
    db.create_table("cycle", "data", None).unwrap();

    // Ingest some data
    let ipc = create_phone_batch(0, 100);
    db.ingest_ipc(IngestBatch {
        payload_ipc: ipc,
        watermark_micros: 0,
        shard_override: None,
        database: Some("cycle".into()),
        table: Some("data".into()),
    })
    .unwrap();

    // Repeatedly create and drop index to test cleanup
    for i in 0..5 {
        let idx_name = format!("idx_ft_{}", i);

        // Create index
        db.create_index(
            "cycle",
            "data",
            &idx_name,
            &["phone_number".to_string()],
            boyodb_core::sql::IndexType::Fulltext,
            false,
        )
        .unwrap();

        // Wait for build
        thread::sleep(std::time::Duration::from_millis(100));

        // Query using index
        let req = QueryRequest {
            sql: "SELECT * FROM cycle.data WHERE phone_number LIKE '%254%'".into(),
            timeout_millis: 5000,
            collect_stats: false,
            transaction_id: None,
        };
        let _ = db.query(req);

        // Drop index
        db.drop_index("cycle", "data", &idx_name, false).unwrap();
    }
}

#[test]
fn test_fulltext_builder_memory_stress() {
    use boyodb_core::fts::{FulltextConfig, FulltextIndexBuilder, load_fulltext_index};

    // Build large index and verify it loads correctly
    for _ in 0..3 {
        let mut builder = FulltextIndexBuilder::new(FulltextConfig::default());

        // Add many values - phone numbers that DON'T contain "888"
        for i in 0..10_000 {
            // Use pattern that avoids "888" completely
            let phone = format!("254{:03}{:03}{:03}", i / 1000, (i / 10) % 100, i % 10);
            builder.add_value(i as u32, &phone);
        }

        // Serialize
        let data = builder.build().unwrap();
        assert!(!data.is_empty());

        // Deserialize and verify
        let index = load_fulltext_index(&data).unwrap();
        assert_eq!(index.doc_count(), 10_000);

        // Query the index
        assert!(index.might_contain("254"));
        // "888" should not exist in any of our generated phone numbers
        assert!(!index.might_contain("888888888"));
    }
}

#[test]
fn test_fulltext_concurrent_index_access() {
    use boyodb_core::fts::{FulltextConfig, FulltextIndexBuilder, load_fulltext_index};

    // Build index
    let mut builder = FulltextIndexBuilder::new(FulltextConfig::default());
    for i in 0..1000 {
        let phone = format!("254{:09}", i);
        builder.add_value(i as u32, &phone);
    }
    let data = Arc::new(builder.build().unwrap());

    // Spawn multiple threads reading the same index data - increased concurrency
    let mut handles = vec![];
    for t in 0..16 {
        let data_clone = Arc::clone(&data);
        handles.push(thread::spawn(move || {
            for i in 0..200 {
                let index = load_fulltext_index(&data_clone).unwrap();
                let pattern = format!("254{:03}", (t * 100 + i) % 1000);
                let _ = index.might_contain(&pattern);
                let _ = index.candidate_rows(&pattern);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_fulltext_builder_concurrent_serialization() {
    use boyodb_core::fts::{FulltextConfig, FulltextIndexBuilder, load_fulltext_index};

    // Build multiple indexes concurrently and verify correctness
    let mut handles = vec![];
    for t in 0..8 {
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let mut builder = FulltextIndexBuilder::new(FulltextConfig::default());
                for i in 0..500 {
                    let phone = format!("{:03}{:09}", t, i);
                    builder.add_value(i as u32, &phone);
                }
                let data = builder.build().unwrap();
                let index = load_fulltext_index(&data).unwrap();
                assert_eq!(index.doc_count(), 500);

                // Verify some patterns
                let pattern = format!("{:03}000", t);
                assert!(index.might_contain(&pattern));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_ngram_generation_stress() {
    use boyodb_core::fts::generate_ngrams;

    // Generate ngrams for many strings
    // Note: ngrams returns a HashSet, so duplicates are removed
    // For a string like "0123456789012..." the ngrams may have duplicates
    // when the pattern repeats (e.g., "012" appears multiple times)
    for _ in 0..100 {
        for len in 3..100 {
            let s: String = (0..len).map(|i| ((i % 10) as u8 + b'0') as char).collect();
            let ngrams = generate_ngrams(&s, 3, false);
            // Should have at most len-2 ngrams (some may be duplicates due to repeating pattern)
            assert!(ngrams.len() <= len - 2);
            assert!(ngrams.len() > 0);
        }
    }

    // Edge cases
    assert!(generate_ngrams("", 3, false).is_empty());
    assert!(generate_ngrams("ab", 3, false).is_empty());
    assert_eq!(generate_ngrams("abc", 3, false).len(), 1);

    // Non-repeating string should have exactly len-2 ngrams
    let unique = "abcdefghij";
    assert_eq!(generate_ngrams(unique, 3, false).len(), unique.len() - 2);
}
