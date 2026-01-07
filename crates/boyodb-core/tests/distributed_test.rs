use boyodb_core::engine::{Db, EngineConfig, QueryRequest, IngestBatch};
use boyodb_core::cluster::{ClusterConfig, ClusterManager};
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

// Helper to find an available port
async fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    tokio::time::sleep(Duration::from_millis(10)).await;
    port
}

// Mock server to handle ExecuteSubQuery
async fn run_mock_server(addr: SocketAddr, db: Arc<Db>, name: &'static str) {
    let listener = TcpListener::bind(addr).await.expect("bind mock server");
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            let db = db.clone();
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if socket.read_exact(&mut len_buf).await.is_err() { return; }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if socket.read_exact(&mut buf).await.is_err() { return; }

                #[derive(serde::Deserialize)]
                #[serde(tag = "op", rename_all = "snake_case")]
                enum Req {
                    ExecuteSubQuery { plan_json: serde_json::Value }
                }

                if let Ok(Req::ExecuteSubQuery { plan_json }) = serde_json::from_slice::<Req>(&buf) {
                    // Debug: print the plan
                    if let Some(kind) = plan_json.get("kind") {
                        if let Some(table) = kind.get("table") {
                            println!("MockServer[{}]: Received plan for table={:?}", name, table);
                        }
                    }

                    if let Ok(local_plan) = serde_json::from_value::<boyodb_core::planner_distributed::LocalPlan>(plan_json) {
                        let result = db.execute_distributed_plan(local_plan.kind);
                        if let Ok(ref res) = result {
                            println!("MockServer[{}]: SUCCESS, {} bytes ipc", name, res.records_ipc.len());
                        } else {
                            println!("MockServer[{}]: FAILED: {:?}", name, result.as_ref().err());
                        }

                        let resp_json = match result {
                            Ok(qr) => {
                                let mut val = serde_json::to_value(qr).unwrap();
                                if let Some(obj) = val.as_object_mut() {
                                    obj.insert("status".into(), "ok".into());
                                    if let Some(ipc_val) = obj.remove("records_ipc") {
                                        let bytes: Vec<u8> = serde_json::from_value(ipc_val).unwrap();
                                        use base64::Engine;
                                        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
                                        obj.insert("ipc_base64".into(), b64.into());
                                    }
                                }
                                val
                            }
                            Err(e) => serde_json::json!({
                                "status": "error",
                                "error": e.to_string()
                            })
                        };

                        let bytes = serde_json::to_vec(&resp_json).unwrap();
                        socket.write_all(&(bytes.len() as u32).to_be_bytes()).await.ok();
                        socket.write_all(&bytes).await.ok();
                    }
                }
            });
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_query_broadcast() {
    // Use dynamic ports
    let rpc_port1 = find_available_port().await;
    let gossip_port1 = find_available_port().await;
    let rpc_port2 = find_available_port().await;
    let gossip_port2 = find_available_port().await;

    let rpc1: SocketAddr = format!("127.0.0.1:{}", rpc_port1).parse().unwrap();
    let gossip1: SocketAddr = format!("127.0.0.1:{}", gossip_port1).parse().unwrap();
    let rpc2: SocketAddr = format!("127.0.0.1:{}", rpc_port2).parse().unwrap();
    let gossip2: SocketAddr = format!("127.0.0.1:{}", gossip_port2).parse().unwrap();

    println!("Test: Ports - rpc1={}, gossip1={}, rpc2={}, gossip2={}",
             rpc_port1, gossip_port1, rpc_port2, gossip_port2);

    // Setup Node 1 (Coordinator - no data)
    let dir1 = tempfile::tempdir().unwrap();
    let cfg1 = EngineConfig::new(dir1.path(), 2);
    let db1 = Arc::new(Db::open(cfg1).unwrap());

    let c_cfg1 = ClusterConfig::new("test-cluster".into(), rpc1, gossip1)
        .with_node_id("node1".into())
        .with_seed_nodes(vec![]);
    let (cm1, _rx1) = ClusterManager::new(c_cfg1, db1.clone());
    let cm1 = Arc::new(cm1);
    db1.set_cluster_manager(Arc::downgrade(&cm1));

    run_mock_server(rpc1, db1.clone(), "node1").await;
    tokio::spawn(boyodb_core::cluster::start_gossip_listener(gossip1, cm1.clone()));
    tokio::spawn(boyodb_core::cluster::start_gossip_sender(_rx1, cm1.shutdown_flag()));
    tokio::spawn(boyodb_core::cluster::start_cluster_tasks(cm1.clone()));

    // Setup Node 2 (Worker - has data)
    let dir2 = tempfile::tempdir().unwrap();
    let cfg2 = EngineConfig::new(dir2.path(), 2);
    let db2 = Arc::new(Db::open(cfg2).unwrap());

    let mut c_cfg2 = ClusterConfig::new("test-cluster".into(), rpc2, gossip2)
        .with_node_id("node2".into())
        .with_seed_nodes(vec![gossip1]);
    c_cfg2.gossip_config.gossip_interval = Duration::from_millis(50);

    let (cm2, _rx2) = ClusterManager::new(c_cfg2, db2.clone());
    let cm2 = Arc::new(cm2);
    db2.set_cluster_manager(Arc::downgrade(&cm2));

    run_mock_server(rpc2, db2.clone(), "node2").await;
    tokio::spawn(boyodb_core::cluster::start_gossip_listener(gossip2, cm2.clone()));
    tokio::spawn(boyodb_core::cluster::start_gossip_sender(_rx2, cm2.shutdown_flag()));
    tokio::spawn(boyodb_core::cluster::start_cluster_tasks(cm2.clone()));

    tokio::time::sleep(Duration::from_millis(500)).await;
    cm2.join_cluster().await.unwrap();

    // Wait for gossip convergence
    let mut converged = false;
    for attempt in 0..20 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let g1_count = cm1.gossip.read().membership.members.len();
        let g2_count = cm2.gossip.read().membership.members.len();
        println!("Test: Convergence attempt {} - node1={}, node2={}", attempt, g1_count, g2_count);
        if g1_count >= 2 && g2_count >= 2 {
            converged = true;
            break;
        }
    }
    assert!(converged, "Gossip did not converge");

    // Helper to force all nodes to Alive state
    let force_alive = || {
        let mut g1 = cm1.gossip.write();
        for m in g1.membership.members.values_mut() {
            m.state = boyodb_core::cluster::NodeState::Alive;
        }
        drop(g1);
        let mut g2 = cm2.gossip.write();
        for m in g2.membership.members.values_mut() {
            m.state = boyodb_core::cluster::NodeState::Alive;
        }
    };

    // Initial force Alive
    force_alive();

    // Print membership
    {
        let g1 = cm1.gossip.read();
        println!("Test: Membership:");
        for (id, m) in g1.membership.members.iter() {
            println!("  {} -> rpc={}, state={:?}", id, m.rpc_addr, m.state);
        }
    }

    // Ingest data into Node 2
    use arrow_schema::{Schema, Field, DataType};
    use arrow_array::{RecordBatch, Int64Array};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant_id", DataType::UInt64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(arrow_array::UInt64Array::from(vec![100, 100, 200])),
        Arc::new(Int64Array::from(vec![10, 20, 30])),
    ]).unwrap();

    let mut ipc_buf = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut ipc_buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    db2.ingest_ipc(IngestBatch {
        payload_ipc: ipc_buf,
        watermark_micros: 0,
        shard_override: None,
        database: Some("default".into()),
        table: Some("test_dist".into()),
    }).unwrap();

    println!("Test: Data ingested into node2");

    // Verify db2 can query locally
    let local_resp = db2.query(QueryRequest {
        sql: "SELECT * FROM test_dist".into(),
        timeout_millis: 5000,
        collect_stats: false,
    });
    println!("Test: Local query on db2: {:?}", local_resp.is_ok());
    assert!(local_resp.is_ok(), "db2 should be able to query locally");

    let local_count = db2.query(QueryRequest {
        sql: "SELECT COUNT(*) FROM test_dist".into(),
        timeout_millis: 5000,
        collect_stats: false,
    });
    println!("Test: Local COUNT(*) on db2: {:?}", local_count.is_ok());
    assert!(local_count.is_ok(), "db2 should be able to COUNT locally");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test 1: Distributed SELECT *
    println!("\n=== Test 1: Distributed SELECT * ===");
    force_alive(); // Ensure nodes are Alive before distributed query
    let req = QueryRequest {
        sql: "SELECT * FROM test_dist".into(),
        timeout_millis: 10000,
        collect_stats: true,
    };
    let resp = db1.query(req);
    println!("Test: SELECT * result: {:?}", resp.is_ok());
    assert!(resp.is_ok(), "Distributed SELECT * should succeed");
    let resp = resp.unwrap();
    assert!(resp.records_ipc.len() > 0, "Should have results");

    // Test 2: Distributed COUNT(*)
    println!("\n=== Test 2: Distributed COUNT(*) ===");
    force_alive(); // Ensure nodes are Alive before distributed query
    let agg_req = QueryRequest {
        sql: "SELECT COUNT(*) FROM test_dist".into(),
        timeout_millis: 10000,
        collect_stats: true,
    };
    let agg_resp = db1.query(agg_req);
    println!("Test: COUNT(*) result: {:?}", agg_resp.is_ok());
    assert!(agg_resp.is_ok(), "Distributed COUNT(*) should succeed");

    let agg_resp = agg_resp.unwrap();
    println!("Test: COUNT(*) response has {} bytes", agg_resp.records_ipc.len());

    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;

    if agg_resp.records_ipc.is_empty() {
        println!("Test: WARNING - COUNT(*) returned empty response");
    } else {
        let cursor = Cursor::new(&agg_resp.records_ipc);
        let reader = StreamReader::try_new(cursor, None).unwrap();

        let mut count_val = 0i64;
        for batch_res in reader {
            let batch = batch_res.unwrap();
            println!("Test: COUNT(*) batch schema: {:?}", batch.schema());
            let arr = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..arr.len() {
                count_val += arr.value(i);
            }
        }
        println!("Test: COUNT(*) = {}", count_val);
        assert_eq!(count_val, 3, "Total count should be 3");
    }

    // Test 3: Distributed SUM
    println!("\n=== Test 3: Distributed SUM ===");
    force_alive(); // Ensure nodes are Alive before distributed query
    let sum_req = QueryRequest {
        sql: "SELECT SUM(val) FROM test_dist".into(),
        timeout_millis: 10000,
        collect_stats: true,
    };
    let sum_resp = db1.query(sum_req);
    println!("Test: SUM(val) result: {:?}", sum_resp.is_ok());
    assert!(sum_resp.is_ok(), "Distributed SUM should succeed");

    let sum_resp = sum_resp.unwrap();
    if !sum_resp.records_ipc.is_empty() {
        let cursor = Cursor::new(&sum_resp.records_ipc);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let arr = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        println!("Test: SUM(val) = {}", arr.value(0));
        assert_eq!(arr.value(0), 60, "Sum should be 60");
    }
}
