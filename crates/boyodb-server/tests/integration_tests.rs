//! Integration tests for boyodb-server
//!
//! These tests verify the TCP protocol and key server operations.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper to send a request and receive a response using the framed protocol
fn send_request(stream: &mut TcpStream, request: &Value) -> std::io::Result<Value> {
    let json_bytes = serde_json::to_vec(request).unwrap();
    let len = json_bytes.len() as u32;

    // Send length prefix + JSON
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&json_bytes)?;
    stream.flush()?;

    // Read response length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    // Read response
    let mut response = vec![0u8; resp_len];
    stream.read_exact(&mut response)?;

    Ok(serde_json::from_slice(&response).unwrap_or(json!({"error": "parse error"})))
}

fn read_frame_bytes(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut response = vec![0u8; resp_len];
    stream.read_exact(&mut response)?;
    Ok(response)
}

fn send_request_streaming(
    stream: &mut TcpStream,
    request: &Value,
) -> std::io::Result<(Value, Vec<u8>)> {
    let json_bytes = serde_json::to_vec(request).unwrap();
    let len = json_bytes.len() as u32;

    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&json_bytes)?;
    stream.flush()?;

    let header_bytes = read_frame_bytes(stream)?;
    let header: Value = serde_json::from_slice(&header_bytes).unwrap_or(json!({"error": "parse error"}));

    let mut payload = Vec::new();
    if header.get("ipc_streaming").and_then(|v| v.as_bool()) == Some(true) {
        loop {
            let chunk = read_frame_bytes(stream)?;
            if chunk.is_empty() {
                break;
            }
            payload.extend_from_slice(&chunk);
        }
    } else if let Some(ipc_len) = header.get("ipc_len").and_then(|v| v.as_u64()) {
        let chunk = read_frame_bytes(stream)?;
        if chunk.len() as u64 == ipc_len {
            payload = chunk;
        }
    }

    Ok((header, payload))
}

/// Create a test Arrow IPC payload
fn create_test_ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Int64, false),
        Field::new("route_id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    let tenant_ids = Int64Array::from(vec![1, 1, 2, 2, 3]);
    let route_ids = Int64Array::from(vec![100, 101, 100, 102, 100]);
    let messages = StringArray::from(vec!["hello", "world", "foo", "bar", "baz"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(route_ids),
            Arc::new(messages),
        ],
    )
    .unwrap();

    let mut ipc_data = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_data, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    ipc_data
}

/// Integration tests module
/// Note: These tests require a running server, so they are marked with #[ignore]
/// Run with: cargo test --package boyodb-server -- --ignored
mod server_tests {
    use super::*;

    /// Test that uses boyodb-core directly without a server
    #[test]
    fn test_core_database_operations() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_path = temp_dir.path().to_str().unwrap();

            // Use core directly
            let config = boyodb_core::EngineConfig::new(data_path, 1);
            let db = boyodb_core::Db::open(config).unwrap();

        // Health check
        assert!(db.health_check().is_ok());

        // Create database
        assert!(db.create_database("test_db").is_ok());

        // List databases
        let dbs = db.list_databases().unwrap();
        assert!(dbs.contains(&"test_db".to_string()));

        // Create table
        assert!(db.create_table("test_db", "events", None).is_ok());

        // List tables
        let tables = db.list_tables(Some("test_db")).unwrap();
        assert!(tables.iter().any(|t| t.name == "events"));

        // Ingest data
        let ipc_data = create_test_ipc();
        let batch = boyodb_core::IngestBatch {
            payload_ipc: ipc_data,
            watermark_micros: 1000000,
            shard_override: None,
            database: Some("test_db".to_string()),
            table: Some("events".to_string()),
        };
        assert!(db.ingest_ipc(batch).is_ok());

        // Query data
        let request = boyodb_core::QueryRequest {
            sql: "SELECT * FROM test_db.events".to_string(),
            timeout_millis: 5000,
            collect_stats: false,
        };
            let result = db.query(request).unwrap();
            assert!(!result.records_ipc.is_empty());
        });
    }

    /// Test aggregation queries
    #[test]
    fn test_core_aggregation_queries() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_path = temp_dir.path().to_str().unwrap();

            let config = boyodb_core::EngineConfig::new(data_path, 1);
            let db = boyodb_core::Db::open(config).unwrap();

        db.create_database("agg_test").unwrap();
        db.create_table("agg_test", "data", None).unwrap();

        // Ingest test data
        let ipc_data = create_test_ipc();
        let batch = boyodb_core::IngestBatch {
            payload_ipc: ipc_data,
            watermark_micros: 2000000,
            shard_override: None,
            database: Some("agg_test".to_string()),
            table: Some("data".to_string()),
        };
        db.ingest_ipc(batch).unwrap();

        // COUNT query
        let result = db
            .query(boyodb_core::QueryRequest {
                sql: "SELECT COUNT(*) FROM agg_test.data".to_string(),
                timeout_millis: 5000,
            collect_stats: false,
        })
            .unwrap();
        assert!(!result.records_ipc.is_empty());

        // GROUP BY query
        let result = db
            .query(boyodb_core::QueryRequest {
                sql: "SELECT tenant_id, COUNT(*) FROM agg_test.data GROUP BY tenant_id"
                    .to_string(),
                timeout_millis: 5000,
            collect_stats: false,
            })
                .unwrap();
            assert!(!result.records_ipc.is_empty());
        });
    }

    /// Test authentication operations
    #[test]
    fn test_core_auth_operations() {
        std::env::set_var("BOYODB_BOOTSTRAP_PASSWORD", "TestBootstr4p!");
        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("auth");

        // Create the auth directory
        std::fs::create_dir_all(&auth_path).unwrap();

        let auth = boyodb_core::AuthManager::new(&auth_path).unwrap();

        // Create user
        auth.create_user("testuser", "TestPass123!", "root").unwrap();

        // Authenticate
        let session = auth
            .authenticate("testuser", "TestPass123!", None, None)
            .unwrap();
        assert!(!session.is_empty());

        // Validate session
        assert!(auth.validate_session(&session).is_ok());

        // Create role
        auth.create_role("testrole", Some("Test role"), "root").unwrap();

        // Grant role to user
        auth.grant_role("testuser", "testrole", "root").unwrap();

        // Grant privilege to role
        auth.grant_privilege_to_role(
            "testrole",
            boyodb_core::Privilege::Select,
            boyodb_core::PrivilegeTarget::Global,
            false,
            "root",
        )
        .unwrap();

        // Revoke privilege from role
        auth.revoke_privilege_from_role(
            "testrole",
            boyodb_core::Privilege::Select,
            boyodb_core::PrivilegeTarget::Global,
            "root",
        )
        .unwrap();

        // Revoke role from user
        auth.revoke_role("testuser", "testrole", "root").unwrap();

        // Drop role
        auth.drop_role("testrole", "root").unwrap();

        // Drop user
        auth.drop_user("testuser", "root").unwrap();
    }

    /// Test DDL operations
    #[test]
    fn test_core_ddl_operations() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_path = temp_dir.path().to_str().unwrap();

            let config = boyodb_core::EngineConfig::new(data_path, 1);
            let db = boyodb_core::Db::open(config).unwrap();

            // CREATE DATABASE
            db.create_database("ddl_test").unwrap();

            // CREATE TABLE
            db.create_table("ddl_test", "test_table", None).unwrap();

            // DROP TABLE (via SQL)
            let _ = db.query(boyodb_core::QueryRequest {
                sql: "DROP TABLE IF EXISTS ddl_test.test_table".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            });

            // DROP DATABASE (via SQL)
            let _ = db.query(boyodb_core::QueryRequest {
                sql: "DROP DATABASE IF EXISTS ddl_test".to_string(),
                timeout_millis: 5000,
                collect_stats: false,
            });
        });
    }

    /// Test manifest and replication operations
    #[test]
    fn test_core_manifest_operations() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let data_path = temp_dir.path().to_str().unwrap();

            let config = boyodb_core::EngineConfig::new(data_path, 1);
            let db = boyodb_core::Db::open(config).unwrap();

            // Export manifest
            let manifest = db.export_manifest().unwrap();
            assert!(!manifest.is_empty());

            // Plan bundle
            let plan = db
                .plan_bundle(boyodb_core::BundleRequest {
                    max_bytes: Some(1024 * 1024),
                    since_version: None,
                    prefer_hot: true,
                    target_bytes_per_sec: None,
                    max_entries: None,
                })
                .unwrap();
            assert!(plan.entries.is_empty());
            assert_eq!(plan.total_bytes, 0);
            assert_eq!(plan.throttle_millis, 0);
        });
    }

    /// Test with a real TCP server connection (ignored by default)
    #[test]
    #[ignore]
    fn test_tcp_server_health() {
        let mut stream =
            TcpStream::connect("127.0.0.1:8765").expect("Failed to connect to server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let response = send_request(&mut stream, &json!({"op": "health"})).unwrap();
        assert_eq!(response.get("status").and_then(|v| v.as_str()), Some("ok"));
    }

    /// Test WAL stats endpoint (ignored by default - requires running server)
    #[test]
    #[ignore]
    fn test_tcp_wal_stats() {
        let mut stream =
            TcpStream::connect("127.0.0.1:8765").expect("Failed to connect to server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let username = env::var("BOYODB_TEST_USER")
            .expect("set BOYODB_TEST_USER for auth-enabled TCP tests");
        let password = env::var("BOYODB_TEST_PASS")
            .expect("set BOYODB_TEST_PASS for auth-enabled TCP tests");

        let login_response = send_request(
            &mut stream,
            &json!({
                "op": "login",
                "username": username,
                "password": password
            }),
        )
        .unwrap();
        let session = login_response
            .get("session_id")
            .and_then(|v| v.as_str())
            .expect("login did not return session_id")
            .to_string();

        let response = send_request(
            &mut stream,
            &json!({
                "op": "wal_stats",
                "auth": session
            }),
        )
        .unwrap();

        assert_eq!(response.get("status").and_then(|v| v.as_str()), Some("ok"));
        assert!(response.get("wal_stats").is_some());
    }

    /// Test TCP query (ignored by default - requires running server)
    #[test]
    #[ignore]
    fn test_tcp_query() {
        let mut stream =
            TcpStream::connect("127.0.0.1:8765").expect("Failed to connect to server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let username = env::var("BOYODB_TEST_USER")
            .expect("set BOYODB_TEST_USER for auth-enabled TCP tests");
        let password = env::var("BOYODB_TEST_PASS")
            .expect("set BOYODB_TEST_PASS for auth-enabled TCP tests");

        // Login to obtain session for authenticated operations
        let login_response = send_request(
            &mut stream,
            &json!({
                "op": "login",
                "username": username,
                "password": password
            }),
        )
        .unwrap();
        println!("Login response: {:?}", login_response);
        let session = login_response
            .get("session_id")
            .and_then(|v| v.as_str())
            .expect("login did not return session_id")
            .to_string();

        // Create database
        let response = send_request(
            &mut stream,
            &json!({"op": "createdatabase", "name": "integration_test", "auth": session}),
        )
        .unwrap();
        println!("Create DB response: {:?}", response);

        // Create table
        let response = send_request(
            &mut stream,
            &json!({
                "op": "createtable",
                "database": "integration_test",
                "table": "events",
                "auth": session,
                "schema": [
                    {"name": "tenant_id", "type": "int64", "nullable": false},
                    {"name": "route_id", "type": "int64", "nullable": false},
                    {"name": "message", "type": "utf8", "nullable": false}
                ]
            }),
        )
        .unwrap();
        println!("Create table response: {:?}", response);

        // Ingest data
        let ipc_data = create_test_ipc();
        let payload_base64 = STANDARD.encode(&ipc_data);
        let response = send_request(
            &mut stream,
            &json!({
                "op": "ingestipc",
                "payload_base64": payload_base64,
                "watermark_micros": 1000000u64,
                "auth": session,
                "database": "integration_test",
                "table": "events"
            }),
        )
        .unwrap();
        println!("Ingest response: {:?}", response);

        // Query
        let response = send_request(
            &mut stream,
            &json!({
                "op": "query",
                "sql": "SELECT * FROM integration_test.events LIMIT 10",
                "auth": session,
                "timeout_millis": 5000
            }),
        )
        .unwrap();
        println!("Query response: {:?}", response);
        assert_eq!(response.get("status").and_then(|v| v.as_str()), Some("ok"));
    }

    /// Test TCP streaming query (ignored by default - requires running server)
    #[test]
    #[ignore]
    fn test_tcp_query_streaming() {
        let mut stream =
            TcpStream::connect("127.0.0.1:8765").expect("Failed to connect to server");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let username = env::var("BOYODB_TEST_USER")
            .expect("set BOYODB_TEST_USER for auth-enabled TCP tests");
        let password = env::var("BOYODB_TEST_PASS")
            .expect("set BOYODB_TEST_PASS for auth-enabled TCP tests");

        let login_response = send_request(
            &mut stream,
            &json!({
                "op": "login",
                "username": username,
                "password": password
            }),
        )
        .unwrap();
        let session = login_response
            .get("session_id")
            .and_then(|v| v.as_str())
            .expect("login did not return session_id")
            .to_string();

        let _ = send_request(
            &mut stream,
            &json!({"op": "createdatabase", "name": "integration_stream", "auth": session}),
        )
        .unwrap();

        let _ = send_request(
            &mut stream,
            &json!({
                "op": "createtable",
                "database": "integration_stream",
                "table": "events",
                "auth": session,
                "schema": [
                    {"name": "tenant_id", "type": "int64", "nullable": false},
                    {"name": "route_id", "type": "int64", "nullable": false},
                    {"name": "message", "type": "utf8", "nullable": false}
                ]
            }),
        )
        .unwrap();

        let ipc_data = create_test_ipc();
        let payload_base64 = STANDARD.encode(&ipc_data);
        let _ = send_request(
            &mut stream,
            &json!({
                "op": "ingestipc",
                "payload_base64": payload_base64,
                "watermark_micros": 1000000u64,
                "auth": session,
                "database": "integration_stream",
                "table": "events"
            }),
        )
        .unwrap();

        let (header, payload) = send_request_streaming(
            &mut stream,
            &json!({
                "op": "query_binary",
                "sql": "SELECT * FROM integration_stream.events LIMIT 10",
                "auth": session,
                "timeout_millis": 5000,
                "stream": true
            }),
        )
        .unwrap();

        assert_eq!(header.get("status").and_then(|v| v.as_str()), Some("ok"));
        assert!(!payload.is_empty());
    }
}
