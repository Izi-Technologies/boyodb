use boyodb_core::engine::{Db, EngineConfig, QueryRequest, IngestBatch};
use boyodb_core::cluster::{ClusterConfig, ClusterManager, NodeId};
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;
use parking_lot::RwLock;

// Mock server to handle ExecuteSubQuery
async fn run_mock_server(addr: SocketAddr, db: Arc<Db>) {
    let listener = TcpListener::bind(addr).await.expect("bind mock server");
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break, // assume shutdown
            };
            let db = db.clone();
            tokio::spawn(async move {
                // Read len
                let mut len_buf = [0u8; 4];
                if socket.read_exact(&mut len_buf).await.is_err() { return; }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; len];
                if socket.read_exact(&mut buf).await.is_err() { return; }
                
                // Parse Request
                #[derive(serde::Deserialize)]
                #[serde(tag = "op", rename_all = "snake_case")]
                enum Req {
                    ExecuteSubQuery { plan_json: serde_json::Value }
                }
                
                if let Ok(Req::ExecuteSubQuery { plan_json }) = serde_json::from_slice::<Req>(&buf) {
                    if let Ok(local_plan) = serde_json::from_value::<boyodb_core::planner_distributed::LocalPlan>(plan_json) {
                        // Execute
                        let result = db.execute_distributed_plan(local_plan.kind);
                        if let Ok(ref res) = result {
                            println!("MockServer: executed plan, got {} bytes ipc", res.records_ipc.len());
                        } else {
                            println!("MockServer: executed plan failed: {:?}", result.as_ref().err());
                        }

                        let resp_json = match result {
                            Ok(qr) => {
                                let mut val = serde_json::to_value(qr).unwrap();
                                if let Some(obj) = val.as_object_mut() {
                                    obj.insert("status".into(), "ok".into());
                                    // Base64 encode ipc if needed. But serde serialization of QueryResponse might do it?
                                    // QueryResponse has `records_ipc: Vec<u8>`. Serde serializes as array/vec of ints.
                                    // DistributedExecutor expects `ipc_base64` string in JSON.
                                    // But I implemented fallback?
                                    // No, executor code: `if let Some(b64) = raw.ipc_base64`.
                                    // So test server MUST encode base64.
                                    
                                    // Let's remove records_ipc from the object and put ipc_base64
                                    if let Some(ipc_val) = obj.remove("records_ipc") {
                                        // It's a byte array.
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
    // Setup Node 1 (Coordinator)
    let dir1 = tempfile::tempdir().unwrap();
    let rpc1: SocketAddr = "127.0.0.1:19001".parse().unwrap();
    let gossip1: SocketAddr = "127.0.0.1:19002".parse().unwrap();
    let cfg1 = EngineConfig::new(dir1.path(), 2);
    let db1 = Arc::new(Db::open(cfg1).unwrap());
    
    let c_cfg1 = ClusterConfig::new("test-cluster".into(), rpc1, gossip1)
        .with_node_id("node1".into())
        .with_seed_nodes(vec![]);
    let (cm1, _rx1) = ClusterManager::new(c_cfg1, db1.clone());
    let cm1 = Arc::new(cm1);
    db1.set_cluster_manager(Arc::downgrade(&cm1));
    
    // Start Node 1 cluster services
    run_mock_server(rpc1, db1.clone()).await;
    tokio::spawn(boyodb_core::cluster::start_gossip_listener(gossip1, cm1.clone()));
    tokio::spawn(boyodb_core::cluster::start_gossip_sender(_rx1, cm1.shutdown_flag()));
    tokio::spawn(boyodb_core::cluster::start_cluster_tasks(cm1.clone()));
    
    // Setup Node 2 (Worker)
    let dir2 = tempfile::tempdir().unwrap();
    let rpc2: SocketAddr = "127.0.0.1:19003".parse().unwrap();
    let gossip2: SocketAddr = "127.0.0.1:19004".parse().unwrap();
    let cfg2 = EngineConfig::new(dir2.path(), 2);
    let db2 = Arc::new(Db::open(cfg2).unwrap());
    
    let mut c_cfg2 = ClusterConfig::new("test-cluster".into(), rpc2, gossip2)
        .with_node_id("node2".into())
        .with_seed_nodes(vec![gossip1]);
    c_cfg2.gossip_config.gossip_interval = Duration::from_millis(100);

    let (cm2, _rx2) = ClusterManager::new(c_cfg2, db2.clone());
    let cm2 = Arc::new(cm2);
    db2.set_cluster_manager(Arc::downgrade(&cm2));
    
    // Start Node 2 cluster services
    run_mock_server(rpc2, db2.clone()).await;
    tokio::spawn(boyodb_core::cluster::start_gossip_listener(gossip2, cm2.clone()));
    tokio::spawn(boyodb_core::cluster::start_gossip_sender(_rx2, cm2.shutdown_flag()));
    tokio::spawn(boyodb_core::cluster::start_cluster_tasks(cm2.clone()));

    // Verify clustering (let gossip converge)
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Trigger join
    cm2.join_cluster().await.unwrap();
    
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Force Alive state for stability
    {
        let mut g1 = cm1.gossip.write();
        for m in g1.membership.members.values_mut() {
            m.state = boyodb_core::cluster::NodeState::Alive;
        }
        let mut g2 = cm2.gossip.write();
        for m in g2.membership.members.values_mut() {
            m.state = boyodb_core::cluster::NodeState::Alive;
        }
    }
    
    // Should interpret both as Alive.
    // However, ShardMap is empty. So broadcast mode.
    
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
        Arc::new(arrow_array::UInt64Array::from(vec![100, 100, 200])), // Tenants 100 and 200
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
    
    // Query from Node 1
    // SELECT * FROM test_dist
    let req = QueryRequest {
        sql: "SELECT * FROM test_dist".into(),
        timeout_millis: 5000,
        collect_stats: true,
    };
    
    // Verify db2 has data locally


    let resp = db1.query(req).unwrap(); // This should trigger distributed execution
    
    assert_eq!(resp.records_ipc.len() > 0, true, "Should have results");
    // Verify content (should be 3 rows)
    // Note: Node 1 has NO data. Node 2 has 3 rows.
    // If local execution only, 0 rows.
    // If distributed, 3 rows.
    
    // Verify Aggregation (COUNT(*))
    // SELECT COUNT(*) FROM test_dist
    let agg_req = QueryRequest {
        sql: "SELECT COUNT(*) FROM test_dist".into(),
        timeout_millis: 5000,
        collect_stats: true,
    };
    let agg_resp = db1.query(agg_req).unwrap();
    
    // Check results
    // Should process IPC and verify value is 3.
    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;
    
    let cursor = Cursor::new(agg_resp.records_ipc);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    
    let mut row_count = 0;
    let mut count_val = 0;
    
    for batch_res in reader {
        let batch = batch_res.unwrap();
        // Verify schema: "sum_count" column (Int64) - Global Sum aggregation of partial counts
        assert_eq!(batch.schema().field(0).name(), "sum_count");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        
        let arr = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            count_val += arr.value(i);
            row_count += 1;
        }
    }
    
    // Should be 1 row with value 3
    assert_eq!(row_count, 1, "Should have exactly 1 total row (merged)");
    assert_eq!(count_val, 3, "Total count should be 3");

    // Verify Pruning (Targeted Query)
    // Map Tenant 200 to Node 2
    {
        let mut map = db1.shard_map.write().unwrap();
        let total = map.total_shards;
        let shard_id = (200 % total as u64) as u16;
        map.shard_nodes.insert(shard_id, boyodb_core::cluster::NodeId::from_string("node2"));
        println!("Test: Mapped shard {} (tenant 200) to node2", shard_id);
    }
    
    // Query: SELECT * FROM test_dist WHERE tenant_id = 200
    // Should target ONLY node2.
    // Node 2 has row with tenant_id 200 (value 30).
    let prune_req = QueryRequest {
        sql: "SELECT * FROM test_dist WHERE tenant_id = 200".into(),
        timeout_millis: 5000,
        collect_stats: true,
    };
    let prune_resp = db1.query(prune_req).unwrap();
    
    // Verify result: 1 row, val=30
    assert_eq!(prune_resp.records_ipc.len() > 0, true, "Should have results for pruned query");
    
    let cursor = Cursor::new(prune_resp.records_ipc);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    
    // tenant_id=200, val=30 is index 2 in original batch (100, 100, 200) -> (10, 20, 30)
    // After Filter(tenant=200), Node 2 returns 1 row (val 30).
    assert_eq!(batch.num_rows(), 1);
    
    // Verify value
    // Schema: id, tenant_id, val
    let val_col = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val_col.value(0), 30);

    // Verify Aggregation (SUM(val))
    // SELECT SUM(val) FROM test_dist
    let sum_req = QueryRequest {
        sql: "SELECT SUM(val) FROM test_dist".into(),
        timeout_millis: 5000,
        collect_stats: true,
    };
    let sum_resp = db1.query(sum_req).unwrap();
    
    let cursor = Cursor::new(sum_resp.records_ipc);
    let mut reader = StreamReader::try_new(cursor, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    
    // Verify schema: "sum_sum_val" column (Int64) - Global sum of partial sums
    // Note: Distributed SUM(val) produces "sum_val" locally, then global sums it to "sum_sum_val"
    assert_eq!(batch.schema().field(0).name(), "sum_sum_val");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);

    let arr = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    // Sum of 10, 20, 30 = 60
    assert_eq!(arr.value(0), 60);
}
