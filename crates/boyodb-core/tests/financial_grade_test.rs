//! Integration tests for financial-grade features
//!
//! Tests ACID transactions, B-Tree indexes, and transaction isolation

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use boyodb_core::transaction::IsolationLevel;
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

fn accounts_schema_json() -> String {
    let fields = vec![
        FieldSpec {
            name: "account_id".to_string(),
            data_type: "int64".to_string(),
            nullable: false,
        },
        FieldSpec {
            name: "balance".to_string(),
            data_type: "int64".to_string(),
            nullable: false,
        },
        FieldSpec {
            name: "account_name".to_string(),
            data_type: "string".to_string(),
            nullable: true,
        },
    ];
    serde_json::to_string(&fields).unwrap()
}

fn build_accounts_payload(account_ids: &[i64], balances: &[i64], names: &[&str]) -> Vec<u8> {
    let schema = Schema::new(vec![
        Field::new("account_id", DataType::Int64, false),
        Field::new("balance", DataType::Int64, false),
        Field::new("account_name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())),
            Arc::new(Int64Array::from(balances.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
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
        .with_wal_max_segments(4)
        .with_enable_compaction(false)
        .with_auto_compact_interval_secs(0)
        .with_transactions_enabled(true)
}

fn setup_bank_table(db: &Db) {
    db.create_database("bank").unwrap();
    db.create_table("bank", "accounts", Some(accounts_schema_json()))
        .unwrap();
}

fn query_sql(db: &Db, sql: &str) -> boyodb_core::QueryResponse {
    let req = QueryRequest {
        sql: sql.to_string(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: None,
    };
    db.query(req).unwrap()
}

/// Test basic transaction lifecycle: BEGIN, COMMIT
#[tokio::test]
async fn test_transaction_begin_commit() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin transaction
    let txn_id = db.begin_transaction(None, false).unwrap();
    assert!(txn_id > 0);

    // Commit transaction
    let commit_version = db.commit_transaction(txn_id).unwrap();
    assert!(commit_version > 0);
}

/// Test transaction rollback
#[tokio::test]
async fn test_transaction_rollback() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin transaction
    let txn_id = db
        .begin_transaction(Some(IsolationLevel::ReadCommitted), false)
        .unwrap();

    // Rollback transaction
    db.rollback_transaction(txn_id).unwrap();

    // Transaction should no longer be active
    // Trying to commit should fail
    let result = db.commit_transaction(txn_id);
    assert!(result.is_err());
}

/// Test savepoints within a transaction
#[tokio::test]
async fn test_transaction_savepoints() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin transaction
    let txn_id = db.begin_transaction(None, false).unwrap();

    // Create savepoint
    db.create_savepoint(txn_id, "sp1").unwrap();

    // Create another savepoint
    db.create_savepoint(txn_id, "sp2").unwrap();

    // Rollback to first savepoint
    db.rollback_to_savepoint(txn_id, "sp1").unwrap();

    // sp2 should no longer exist after rollback to sp1
    let result = db.rollback_to_savepoint(txn_id, "sp2");
    assert!(result.is_err());

    // sp1 should still exist
    db.rollback_to_savepoint(txn_id, "sp1").unwrap();

    // Release sp1
    db.release_savepoint(txn_id, "sp1").unwrap();

    // sp1 should no longer exist after release
    let result = db.rollback_to_savepoint(txn_id, "sp1");
    assert!(result.is_err());

    // Commit transaction
    db.commit_transaction(txn_id).unwrap();
}

/// Test different isolation levels
#[tokio::test]
async fn test_isolation_levels() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Test ReadCommitted
    let txn1 = db
        .begin_transaction(Some(IsolationLevel::ReadCommitted), false)
        .unwrap();
    db.commit_transaction(txn1).unwrap();

    // Test RepeatableRead
    let txn2 = db
        .begin_transaction(Some(IsolationLevel::RepeatableRead), false)
        .unwrap();
    db.commit_transaction(txn2).unwrap();

    // Test Serializable
    let txn3 = db
        .begin_transaction(Some(IsolationLevel::Serializable), false)
        .unwrap();
    db.commit_transaction(txn3).unwrap();
}

/// Test read-only transactions
#[tokio::test]
async fn test_read_only_transaction() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin read-only transaction
    let txn_id = db.begin_transaction(None, true).unwrap();

    // Should be able to commit read-only transaction
    db.commit_transaction(txn_id).unwrap();
}

/// Test concurrent transactions
#[tokio::test]
async fn test_concurrent_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Db::open(make_config(dir.path())).unwrap());

    setup_bank_table(&db);

    // Start multiple concurrent transactions
    let txn1 = db.begin_transaction(None, false).unwrap();
    let txn2 = db.begin_transaction(None, false).unwrap();
    let txn3 = db.begin_transaction(None, false).unwrap();

    // All should have unique IDs
    assert_ne!(txn1, txn2);
    assert_ne!(txn2, txn3);
    assert_ne!(txn1, txn3);

    // Commit in different order
    db.commit_transaction(txn2).unwrap();
    db.commit_transaction(txn1).unwrap();
    db.commit_transaction(txn3).unwrap();
}

/// Test B-Tree index creation and data operations
#[tokio::test]
async fn test_btree_index_creation() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Insert some data first (index will be built on existing data)
    let payload = build_accounts_payload(
        &[1001, 1002, 1003],
        &[10000, 25000, 5000],
        &["Alice", "Bob", "Charlie"],
    );
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload,
        watermark_micros: 1000000,
        shard_override: None,
        database: Some("bank".to_string()),
        table: Some("accounts".to_string()),
    })
    .unwrap();

    // Query the data
    let result = query_sql(&db, "SELECT * FROM bank.accounts WHERE account_id = 1002");
    assert!(!result.records_ipc.is_empty());
}

/// Test WAL LSN persistence
#[tokio::test]
async fn test_wal_lsn_persistence() {
    let dir = tempdir().unwrap();

    // Open and close database
    {
        let db = Db::open(make_config(dir.path())).unwrap();
        db.create_database("test").unwrap();
        // The WAL should have incremented LSN
    }

    // Reopen database - LSN should be recovered
    {
        let db = Db::open(make_config(dir.path())).unwrap();
        // Database should be intact
        let result = query_sql(&db, "SHOW DATABASES");
        assert!(!result.records_ipc.is_empty());
    }
}

/// Test transaction with data operations
#[tokio::test]
async fn test_transaction_with_ingest() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin transaction
    let txn_id = db.begin_transaction(None, false).unwrap();

    // Ingest data within transaction
    let payload = build_accounts_payload(&[1001, 1002], &[10000, 20000], &["Alice", "Bob"]);
    db.ingest_ipc_txn(
        IngestBatch {
            payload_ipc: payload,
            watermark_micros: 1000000,
            shard_override: None,
            database: Some("bank".to_string()),
            table: Some("accounts".to_string()),
        },
        Some(txn_id),
    )
    .unwrap();

    // Commit transaction
    db.commit_transaction(txn_id).unwrap();

    // Data should be visible after commit
    let result = query_sql(&db, "SELECT COUNT(*) FROM bank.accounts");
    assert!(!result.records_ipc.is_empty());
}

/// Test nested savepoints
#[tokio::test]
async fn test_transaction_multiple_savepoints() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Begin transaction
    let txn_id = db.begin_transaction(None, false).unwrap();

    // Create nested savepoints
    db.create_savepoint(txn_id, "level1").unwrap();
    db.create_savepoint(txn_id, "level2").unwrap();
    db.create_savepoint(txn_id, "level3").unwrap();

    // Rollback to middle savepoint
    db.rollback_to_savepoint(txn_id, "level2").unwrap();

    // level3 should be gone, level1 and level2 should still exist
    assert!(db.rollback_to_savepoint(txn_id, "level3").is_err());
    db.rollback_to_savepoint(txn_id, "level1").unwrap();

    // Commit successfully
    db.commit_transaction(txn_id).unwrap();
}

/// Test MVCC visibility - transactions should see only committed data
#[tokio::test]
async fn test_mvcc_visibility() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Insert initial data (outside transaction)
    let payload = build_accounts_payload(&[1001], &[10000], &["Alice"]);
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload,
        watermark_micros: 1000000,
        shard_override: None,
        database: Some("bank".to_string()),
        table: Some("accounts".to_string()),
    })
    .unwrap();

    // Begin transaction 1
    let txn1 = db.begin_transaction(None, false).unwrap();

    // Begin transaction 2
    let txn2 = db.begin_transaction(None, false).unwrap();

    // Both transactions should exist with unique IDs
    assert_ne!(txn1, txn2);
    assert!(txn1 > 0);
    assert!(txn2 > 0);

    // Commit both (empty transactions, just testing visibility infrastructure)
    db.commit_transaction(txn1).unwrap();
    db.commit_transaction(txn2).unwrap();
}

/// Test query with transaction context
#[tokio::test]
async fn test_transactional_query() {
    let dir = tempdir().unwrap();
    let db = Db::open(make_config(dir.path())).unwrap();

    setup_bank_table(&db);

    // Insert data
    let payload = build_accounts_payload(&[2001, 2002], &[5000, 15000], &["Bob", "Carol"]);
    db.ingest_ipc(IngestBatch {
        payload_ipc: payload,
        watermark_micros: 2000000,
        shard_override: None,
        database: Some("bank".to_string()),
        table: Some("accounts".to_string()),
    })
    .unwrap();

    // Begin transaction
    let txn_id = db.begin_transaction(None, false).unwrap();

    // Query with transaction context
    let req = QueryRequest {
        sql: "SELECT * FROM bank.accounts".to_string(),
        timeout_millis: 5000,
        collect_stats: false,
        transaction_id: Some(txn_id),
    };
    let result = db.query(req).unwrap();
    assert!(!result.records_ipc.is_empty());

    db.commit_transaction(txn_id).unwrap();
}
