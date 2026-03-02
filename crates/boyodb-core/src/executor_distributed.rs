use crate::planner_distributed::{LocalPlan, GlobalPlan};
use crate::engine::{PlanKind, Db, EngineError, QueryResponse, QueryExecutionStats};

use std::net::{SocketAddr, TcpStream};
use std::io::{Read, Write};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use base64::{Engine as _, engine::general_purpose};
use arrow_ipc::reader::StreamReader;
use std::io::Cursor;
use rayon::prelude::*;

#[derive(Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RequestEnum {
    ExecuteSubQuery {
        plan_json: serde_json::Value,
        timeout_millis: u32,
        accept_compression: Option<String>,
    }
}

#[derive(Deserialize)]
struct ServerResponse {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    // The server responses for ExecuteSubQuery embed QueryResponse fields directly or inside?
    // Let's check boyodb-server/src/main.rs handle_execute_subquery.
    // It creates Response::from_query_response(result) which serializes QueryResponse fields into the JSON.
    // So ServerResponse IS QueryResponse + error/message fields?
    // Response struct in server:
    // pub struct Response {
    //    pub status: Status, // "ok" or "error"
    //    pub error: Option<String>,
    //    pub message: Option<String>,
    //    pub execution_stats: Option<QueryExecutionStats>,
    //    pub ipc_base64: Option<String>,
    //    ...
    // }
    
    // We can just Deserialize QueryResponse fields manually or use a flattened approach.
    #[serde(flatten)]
    query_response: Option<QueryResponseFields>,

    #[serde(default)]
    ipc_base64: Option<String>,
}

#[derive(Deserialize)]
struct QueryResponseFields {
     execution_stats: Option<QueryExecutionStats>,
     // other fields if meaningful, but mostly we care about ipc_base64 which is separate
}

pub struct DistributedExecutor;

impl DistributedExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn execute(&self, db: &Db, global: GlobalPlan, local: LocalPlan) -> Result<QueryResponse, EngineError> {
        // 1. Resolve nodes
        let nodes = self.resolve_nodes(db, &local)?;
        
        if nodes.is_empty() {
             // If no nodes (e.g. valid filter but no active nodes, or cluster empty), return empty response
             return Ok(QueryResponse {
                 records_ipc: vec![],
                 execution_stats: None,
                 segments_scanned: 0,
                 data_skipped_bytes: 0,
             });
        }

        // 2. Scatter to all nodes (including self if applicable? For now, assume pure distributed or self is in list)
        // Note: self.resolve_nodes should include self address if we are handling shards.
        
        let results: Vec<Result<QueryResponse, EngineError>> = nodes.par_iter().map(|addr| {
            self.send_subquery(*addr, &local)
        }).collect();

        // 3. Gather and Merge
        self.merge_results(&global, results)
    }

    fn resolve_nodes(&self, db: &Db, plan: &LocalPlan) -> Result<Vec<SocketAddr>, EngineError> {
        let cm_lock = db.cluster_manager.read().map_err(|_| EngineError::Internal("ClusterManager lock poisoned".into()))?;
        let cm_weak = cm_lock.as_ref().ok_or_else(|| {
             EngineError::Internal("ClusterManager not initialized in distributed mode".into())
        })?;
        
        let cm = cm_weak.upgrade().ok_or_else(|| {
             EngineError::Internal("ClusterManager dropped".into())
        })?;

        // Determine shards from filter
        let shard_ids = self.prune_shards(db, plan);
        
        let mut target_nodes = std::collections::HashSet::new();
        let mut broadcast = false;
        
        for shard_id in shard_ids {
            if let Some(node_id) = db.shard_map.read().unwrap().get_node_id(shard_id) {
                target_nodes.insert(node_id);
            } else {
                broadcast = true;
                break;
            }
        }
        
        let members: Vec<SocketAddr> = {
            let gossip = cm.gossip.read();
            if broadcast || target_nodes.is_empty() {
                gossip.membership.members.values()
                    .filter(|m| m.state == crate::cluster::NodeState::Alive)
                    .map(|m| m.rpc_addr)
                    .collect()
            } else {
                target_nodes.iter().filter_map(|nid| {
                    gossip.membership.members.get(nid)
                }).filter(|m| m.state == crate::cluster::NodeState::Alive)
                  .map(|m| m.rpc_addr)
                  .collect()
            }
        };
        
        Ok(members)
    }

    fn prune_shards(&self, db: &Db, plan: &LocalPlan) -> Vec<u16> {
        // Debug
        // println!("Pruning check: plan kind {:?}", plan.kind);
        
        let empty_shards: Vec<u16> = (0..db.shard_map.read().unwrap().total_shards).collect();
        match &plan.kind {
            PlanKind::Simple { filter, .. } => {
                let mut shards = Vec::new(); // Debug removed
                let mut constrained = false;
                
                if let Some(tid) = filter.tenant_id_eq {
                    shards.push(db.shard_map.read().unwrap().get_shard_id(tid));
                    constrained = true;
                }
                
                if let Some(tids) = &filter.tenant_id_in {
                   for tid in tids {
                       shards.push(db.shard_map.read().unwrap().get_shard_id(*tid));
                   }
                   constrained = true;
                }
                
                if constrained {
                    shards.sort();
                    shards.dedup();
                    shards
                } else {
                    empty_shards
                }
            }
             _ => empty_shards // Joins/etc, default to all shards
        }
    }

    fn send_subquery(&self, addr: SocketAddr, plan: &LocalPlan) -> Result<QueryResponse, EngineError> {
        let _start = Instant::now();
        // Connect
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500))
            .map_err(|e| EngineError::Io(format!("connect to {}: {}", addr, e)))?;
        stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

        // Prepare request
        let plan_json = serde_json::to_value(plan)
            .map_err(|e| EngineError::Internal(format!("serialize plan: {}", e)))?;
            
        let req = RequestEnum::ExecuteSubQuery {
            plan_json,
            timeout_millis: 10000,
            accept_compression: None, // zstd not yet enabled in core
        };

        // Frame encoding: [Length u32 BE][JSON bytes]
        let req_bytes = serde_json::to_vec(&req)
             .map_err(|e| EngineError::Internal(format!("serialize req: {}", e)))?;
        
        let len = req_bytes.len() as u32;
        stream.write_all(&len.to_be_bytes())
             .map_err(|e| EngineError::Io(format!("write len: {}", e)))?;
        stream.write_all(&req_bytes)
             .map_err(|e| EngineError::Io(format!("write payload: {}", e)))?;

        // Read response
        // Frame decoding: [Length u32 BE][JSON bytes]
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)
             .map_err(|e| EngineError::Io(format!("read len: {}", e)))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf)
             .map_err(|e| EngineError::Io(format!("read payload: {}", e)))?;
             
        // Deserialize
        #[derive(Deserialize)]
        struct RawResponse {
            status: String,
            error: Option<String>,
            message: Option<String>,
            execution_stats: Option<QueryExecutionStats>,
            ipc_base64: Option<String>,
        }
        
        let raw: RawResponse = serde_json::from_slice(&resp_buf)
             .map_err(|e| EngineError::Internal(format!("deserialize resp: {}", e)))?;

        if raw.status != "ok" {
             return Err(EngineError::Remote(raw.error.unwrap_or_else(|| "unknown remote error".into())));
        }

        // Decode IPC
        let records_ipc = if let Some(b64) = raw.ipc_base64 {
             general_purpose::STANDARD.decode(b64)
                 .map_err(|e| EngineError::Internal(format!("base64 decode: {}", e)))?
        } else {
             vec![]
        };

        Ok(QueryResponse {
            records_ipc,
            execution_stats: raw.execution_stats,
            segments_scanned: 0, // remote verified
            data_skipped_bytes: 0, 
        })
    }

    fn merge_results(&self, global: &GlobalPlan, results: Vec<Result<QueryResponse, EngineError>>) -> Result<QueryResponse, EngineError> {
        let mut total_stats = QueryExecutionStats::default();
        let mut total_scanned = 0;
        let mut total_skipped = 0;

        // Check if we need to perform Global Aggregation
        let global_aggregator = if let PlanKind::Simple { agg: Some(agg_plan), filter, .. } = &global.kind {
            Some((crate::engine::Aggregator::new(agg_plan.clone())?, filter))
        } else {
            None
        };

        let mut all_batches = Vec::new();
        let mut aggregator_in_flight = global_aggregator;

        for res in results {
            match res {
                Ok(qr) => {
                    // Update stats
                     if let Some(s) = qr.execution_stats {
                        total_stats.execution_time_micros += s.execution_time_micros; 
                        total_stats.segments_scanned += s.segments_scanned;
                        total_stats.segments_pruned += s.segments_pruned;
                        total_stats.segments_total += s.segments_total;
                        total_stats.bytes_read += s.bytes_read;
                        total_stats.bytes_skipped += s.bytes_skipped;
                     }
                     total_scanned += qr.segments_scanned;
                     total_skipped += qr.data_skipped_bytes;

                    if let Some((ref mut agg, filter)) = aggregator_in_flight {
                        // Feed IPC directly to aggregator
                        if !qr.records_ipc.is_empty() {
                            agg.consume_ipc(&qr.records_ipc, filter)?;
                        }
                    } else {
                        // Standard Collect & Merge
                        if !qr.records_ipc.is_empty() {
                            let cursor = Cursor::new(qr.records_ipc);
                            let reader = StreamReader::try_new(cursor, None)
                                .map_err(|e| EngineError::Internal(format!("ipc reader: {}", e)))?;
                            for batch in reader {
                                let b = batch.map_err(|e| EngineError::Internal(format!("ipc read batch: {}", e)))?;
                                all_batches.push(b);
                            }
                        }
                    }
                }
                Err(e) => {
                    // Tolerate "not found" errors (node has no data for this table/shard)
                    let msg = e.to_string();
                    if msg.to_lowercase().contains("not found") {
                        continue;
                    }

                    tracing::warn!("Distributed query partial failure: {}", e);
                    // Fail the whole query? Or partial results?
                    // For now, fail hard.
                    return Err(e);
                }
            }
        }



        // Finalize
        let merged_ipc = if let Some((agg, _)) = aggregator_in_flight {
             agg.finish()?
        } else {
            // Concatenate batches
            let schema = if all_batches.is_empty() {
                 return Ok(QueryResponse {
                     records_ipc: vec![],
                     execution_stats: Some(total_stats),
                     segments_scanned: total_scanned,
                     data_skipped_bytes: total_skipped,
                 });
            } else {
                 all_batches[0].schema()
            };

            let mut merged_ipc = Vec::new();
            {
                let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut merged_ipc, &schema)
                    .map_err(|e| EngineError::Internal(format!("ipc writer: {}", e)))?;
                for batch in all_batches {
                    writer.write(&batch)
                        .map_err(|e| EngineError::Internal(format!("ipc write: {}", e)))?;
                }
                writer.finish()
                    .map_err(|e| EngineError::Internal(format!("ipc finish: {}", e)))?;
            }
            merged_ipc
        };
        
        Ok(QueryResponse {
            records_ipc: merged_ipc,
            execution_stats: Some(total_stats),
            segments_scanned: total_scanned,
            data_skipped_bytes: total_skipped,
        })
    }
}
