//! Phase 18: Distributed Query Execution - Executor Module
//!
//! This module implements distributed query execution with:
//! - Scatter-gather execution to cluster nodes
//! - Retry logic with exponential backoff
//! - Partial result support
//! - Multiple merge strategies (concatenate, merge-sort, final aggregate)
//! - Execution stats aggregation

use crate::planner_distributed::{
    LocalPlan, GlobalPlan, MergeStrategy, FinalAggKind, PartialAggSpec, OrderBySpec,
};
use crate::engine::{PlanKind, Db, EngineError, QueryResponse, QueryExecutionStats};

use std::net::SocketAddr;
use std::io::{Read, Write, Cursor};
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use base64::{Engine as _, engine::general_purpose};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_array::{RecordBatch, ArrayRef, Float64Array, Int64Array, Array};
use arrow_schema::SchemaRef;
use rayon::prelude::*;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for distributed execution
#[derive(Debug, Clone)]
pub struct DistributedExecutorConfig {
    /// Maximum retries per node
    pub max_retries: u32,
    /// Retry backoff base in milliseconds
    pub retry_backoff_ms: u64,
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Read timeout in milliseconds
    pub read_timeout_ms: u64,
    /// Whether to allow partial results on node failure
    pub allow_partial_results: bool,
    /// Minimum nodes required for partial results
    pub min_nodes_for_partial: usize,
}

impl Default for DistributedExecutorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_backoff_ms: 100,
            connect_timeout_ms: 500,
            read_timeout_ms: 10000,
            allow_partial_results: false,
            min_nodes_for_partial: 1,
        }
    }
}

impl DistributedExecutorConfig {
    pub fn with_retries(mut self, max_retries: u32, backoff_ms: u64) -> Self {
        self.max_retries = max_retries;
        self.retry_backoff_ms = backoff_ms;
        self
    }

    pub fn with_timeouts(mut self, connect_ms: u64, read_ms: u64) -> Self {
        self.connect_timeout_ms = connect_ms;
        self.read_timeout_ms = read_ms;
        self
    }

    pub fn with_partial_results(mut self, allow: bool, min_nodes: usize) -> Self {
        self.allow_partial_results = allow;
        self.min_nodes_for_partial = min_nodes;
        self
    }
}

// ============================================================================
// Result Types
// ============================================================================

/// Result from a single node with metadata
#[derive(Debug)]
pub struct NodeResult {
    pub node_addr: SocketAddr,
    pub result: Result<QueryResponse, EngineError>,
    pub retry_count: u32,
    pub latency_ms: u64,
}

/// Aggregated execution result with fault info
#[derive(Debug)]
pub struct DistributedExecutionResult {
    pub response: QueryResponse,
    pub node_results: Vec<NodeResult>,
    pub partial: bool,
    pub failed_nodes: Vec<SocketAddr>,
}

// ============================================================================
// Wire Protocol
// ============================================================================

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
struct RawResponse {
    status: String,
    error: Option<String>,
    #[allow(dead_code)]
    message: Option<String>,
    execution_stats: Option<QueryExecutionStats>,
    ipc_base64: Option<String>,
}

// ============================================================================
// Executor Implementation
// ============================================================================

pub struct DistributedExecutor {
    config: DistributedExecutorConfig,
}

impl Default for DistributedExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedExecutor {
    pub fn new() -> Self {
        Self {
            config: DistributedExecutorConfig::default(),
        }
    }

    pub fn with_config(config: DistributedExecutorConfig) -> Self {
        Self { config }
    }

    /// Execute a distributed query with the given local and global plans
    pub fn execute(&self, db: &Db, global: GlobalPlan, local: LocalPlan) -> Result<QueryResponse, EngineError> {
        // 1. Resolve target nodes
        let nodes = self.resolve_nodes(db, &local)?;

        if nodes.is_empty() {
            return Ok(QueryResponse {
                records_ipc: vec![],
                execution_stats: None,
                segments_scanned: 0,
                data_skipped_bytes: 0,
            });
        }

        // 2. Scatter to all nodes in parallel with retry
        let node_results: Vec<NodeResult> = nodes.par_iter().map(|addr| {
            self.send_subquery_with_retry(*addr, &local)
        }).collect();

        // 3. Process results based on fault tolerance config
        let execution_result = self.process_node_results(node_results, &global)?;

        Ok(execution_result.response)
    }

    /// Execute and return detailed results including per-node info
    pub fn execute_detailed(&self, db: &Db, global: GlobalPlan, local: LocalPlan) -> Result<DistributedExecutionResult, EngineError> {
        let nodes = self.resolve_nodes(db, &local)?;

        if nodes.is_empty() {
            return Ok(DistributedExecutionResult {
                response: QueryResponse {
                    records_ipc: vec![],
                    execution_stats: None,
                    segments_scanned: 0,
                    data_skipped_bytes: 0,
                },
                node_results: vec![],
                partial: false,
                failed_nodes: vec![],
            });
        }

        let node_results: Vec<NodeResult> = nodes.par_iter().map(|addr| {
            self.send_subquery_with_retry(*addr, &local)
        }).collect();

        self.process_node_results(node_results, &global)
    }

    // ========================================================================
    // Node Resolution
    // ========================================================================

    fn resolve_nodes(&self, db: &Db, plan: &LocalPlan) -> Result<Vec<SocketAddr>, EngineError> {
        let cm_lock = db.cluster_manager.read()
            .map_err(|_| EngineError::Internal("ClusterManager lock poisoned".into()))?;
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
                // Try targeted lookup first
                let targeted: Vec<SocketAddr> = target_nodes.iter().filter_map(|nid| {
                    gossip.membership.members.get(nid)
                }).filter(|m| m.state == crate::cluster::NodeState::Alive)
                  .map(|m| m.rpc_addr)
                  .collect();

                // Fall back to broadcast if targeted lookup returns no nodes
                // This handles cases where node IDs don't match membership keys
                if targeted.is_empty() {
                    gossip.membership.members.values()
                        .filter(|m| m.state == crate::cluster::NodeState::Alive)
                        .map(|m| m.rpc_addr)
                        .collect()
                } else {
                    targeted
                }
            }
        };

        Ok(members)
    }

    fn prune_shards(&self, db: &Db, plan: &LocalPlan) -> Vec<u16> {
        let empty_shards: Vec<u16> = (0..db.shard_map.read().unwrap().total_shards).collect();
        match &plan.kind {
            PlanKind::Simple { filter, .. } => {
                let mut shards = Vec::new();
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
            _ => empty_shards
        }
    }

    // ========================================================================
    // Subquery Execution with Retry
    // ========================================================================

    fn send_subquery_with_retry(&self, addr: SocketAddr, plan: &LocalPlan) -> NodeResult {
        let start = Instant::now();
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            match self.send_subquery(addr, plan) {
                Ok(response) => {
                    return NodeResult {
                        node_addr: addr,
                        result: Ok(response),
                        retry_count: attempt,
                        latency_ms: start.elapsed().as_millis() as u64,
                    };
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        let backoff = self.config.retry_backoff_ms * (1 << attempt);
                        std::thread::sleep(Duration::from_millis(backoff));
                    }
                }
            }
        }

        NodeResult {
            node_addr: addr,
            result: Err(last_error.unwrap()),
            retry_count: self.config.max_retries,
            latency_ms: start.elapsed().as_millis() as u64,
        }
    }

    fn send_subquery(&self, addr: SocketAddr, plan: &LocalPlan) -> Result<QueryResponse, EngineError> {
        // Connect with timeout
        let mut stream = std::net::TcpStream::connect_timeout(
            &addr,
            Duration::from_millis(self.config.connect_timeout_ms)
        ).map_err(|e| EngineError::Io(format!("connect to {}: {}", addr, e)))?;

        stream.set_read_timeout(Some(Duration::from_millis(self.config.read_timeout_ms))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

        // Prepare request
        let plan_json = serde_json::to_value(plan)
            .map_err(|e| EngineError::Internal(format!("serialize plan: {}", e)))?;

        let req = RequestEnum::ExecuteSubQuery {
            plan_json,
            timeout_millis: self.config.read_timeout_ms as u32,
            accept_compression: None,
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
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)
            .map_err(|e| EngineError::Io(format!("read len: {}", e)))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf)
            .map_err(|e| EngineError::Io(format!("read payload: {}", e)))?;

        // Deserialize
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
            segments_scanned: 0,
            data_skipped_bytes: 0,
        })
    }

    // ========================================================================
    // Result Processing
    // ========================================================================

    fn process_node_results(
        &self,
        node_results: Vec<NodeResult>,
        global: &GlobalPlan,
    ) -> Result<DistributedExecutionResult, EngineError> {
        // Separate results into successes, "not found" (tolerable), and hard failures
        let mut successes: Vec<NodeResult> = Vec::new();
        let mut not_found_nodes: Vec<NodeResult> = Vec::new();
        let mut hard_failures: Vec<NodeResult> = Vec::new();

        for nr in node_results {
            match &nr.result {
                Ok(_) => successes.push(nr),
                Err(e) => {
                    // Tolerate "not found" errors - node simply doesn't have data for this table/shard
                    let msg = e.to_string().to_lowercase();
                    if msg.contains("not found") || msg.contains("no segments") {
                        // Convert to success with empty result
                        not_found_nodes.push(NodeResult {
                            node_addr: nr.node_addr,
                            result: Ok(QueryResponse {
                                records_ipc: vec![],
                                execution_stats: None,
                                segments_scanned: 0,
                                data_skipped_bytes: 0,
                            }),
                            retry_count: nr.retry_count,
                            latency_ms: nr.latency_ms,
                        });
                    } else {
                        hard_failures.push(nr);
                    }
                }
            }
        }

        // Combine successes with tolerated "not found" results
        successes.extend(not_found_nodes);

        let failed_nodes: Vec<SocketAddr> = hard_failures.iter()
            .map(|r| r.node_addr)
            .collect();

        // Check fault tolerance for hard failures only
        if !self.config.allow_partial_results && !hard_failures.is_empty() {
            // Return first error
            let first_failure = hard_failures.into_iter().next().unwrap();
            return Err(first_failure.result.unwrap_err());
        }

        if successes.len() < self.config.min_nodes_for_partial {
            return Err(EngineError::Internal(format!(
                "Insufficient nodes: {} succeeded, {} required",
                successes.len(), self.config.min_nodes_for_partial
            )));
        }

        // Merge successful results
        let merged = self.merge_results(global, &successes)?;

        let all_results: Vec<NodeResult> = successes.into_iter()
            .chain(hard_failures)
            .collect();

        Ok(DistributedExecutionResult {
            response: merged,
            node_results: all_results,
            partial: !failed_nodes.is_empty(),
            failed_nodes,
        })
    }

    fn merge_results(
        &self,
        global: &GlobalPlan,
        successes: &[NodeResult],
    ) -> Result<QueryResponse, EngineError> {
        // Aggregate execution stats
        let mut merged_stats = QueryExecutionStats::default();
        let mut total_scanned = 0;
        let mut total_skipped = 0;

        // Collect all IPC data
        let mut all_ipc: Vec<Vec<u8>> = Vec::new();

        for node_result in successes {
            if let Ok(ref qr) = node_result.result {
                // Merge stats
                if let Some(ref s) = qr.execution_stats {
                    merge_execution_stats(&mut merged_stats, s);
                }
                total_scanned += qr.segments_scanned;
                total_skipped += qr.data_skipped_bytes;

                if !qr.records_ipc.is_empty() {
                    all_ipc.push(qr.records_ipc.clone());
                }
            }
        }

        // Apply merge strategy
        let merged_ipc = match &global.merge_strategy {
            MergeStrategy::Concatenate => {
                self.merge_concatenate(&all_ipc)?
            }
            MergeStrategy::MergeSort { order_by, limit, offset } => {
                self.merge_sorted(&all_ipc, order_by, *limit, *offset)?
            }
            MergeStrategy::FinalAggregate { partial_aggs, group_by: _ } => {
                self.merge_final_aggregate(&all_ipc, partial_aggs, global)?
            }
            MergeStrategy::SetOperation { op: _ } => {
                // For now, just concatenate - deduplication handled separately
                self.merge_concatenate(&all_ipc)?
            }
            MergeStrategy::JoinMerge { join_type: _ } => {
                // Join results are already joined on each node, just concatenate
                self.merge_concatenate(&all_ipc)?
            }
        };

        Ok(QueryResponse {
            records_ipc: merged_ipc,
            execution_stats: Some(merged_stats),
            segments_scanned: total_scanned,
            data_skipped_bytes: total_skipped,
        })
    }

    // ========================================================================
    // Merge Strategies
    // ========================================================================

    fn merge_concatenate(&self, ipc_chunks: &[Vec<u8>]) -> Result<Vec<u8>, EngineError> {
        if ipc_chunks.is_empty() {
            return Ok(vec![]);
        }

        // Collect all batches
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        for chunk in ipc_chunks {
            if chunk.is_empty() {
                continue;
            }
            let cursor = Cursor::new(chunk);
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| EngineError::Internal(format!("ipc reader: {}", e)))?;

            if schema.is_none() {
                schema = Some(reader.schema());
            }

            for batch in reader {
                let b = batch.map_err(|e| EngineError::Internal(format!("ipc read batch: {}", e)))?;
                all_batches.push(b);
            }
        }

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // Write merged batches
        let schema = schema.unwrap();
        let mut merged_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut merged_ipc, &schema)
                .map_err(|e| EngineError::Internal(format!("ipc writer: {}", e)))?;
            for batch in all_batches {
                writer.write(&batch)
                    .map_err(|e| EngineError::Internal(format!("ipc write: {}", e)))?;
            }
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("ipc finish: {}", e)))?;
        }

        Ok(merged_ipc)
    }

    fn merge_sorted(
        &self,
        ipc_chunks: &[Vec<u8>],
        order_by: &[OrderBySpec],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<u8>, EngineError> {
        if ipc_chunks.is_empty() || order_by.is_empty() {
            // No ordering needed, just concatenate and apply limit
            let concat = self.merge_concatenate(ipc_chunks)?;
            if limit.is_none() && offset.is_none() {
                return Ok(concat);
            }
            // Apply offset/limit to concatenated result
            return self.apply_offset_limit(&concat, offset.unwrap_or(0), limit);
        }

        // Collect all batches
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<SchemaRef> = None;

        for chunk in ipc_chunks {
            if chunk.is_empty() {
                continue;
            }
            let cursor = Cursor::new(chunk);
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| EngineError::Internal(format!("ipc reader: {}", e)))?;

            if schema.is_none() {
                schema = Some(reader.schema());
            }

            for batch in reader {
                let b = batch.map_err(|e| EngineError::Internal(format!("ipc read batch: {}", e)))?;
                all_batches.push(b);
            }
        }

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // Simple approach: concatenate all rows, sort, apply limit
        // For large datasets, would use k-way merge with min-heap
        let schema = schema.unwrap();

        // Concatenate all batches into one
        let concatenated = arrow::compute::concat_batches(&schema, &all_batches)
            .map_err(|e| EngineError::Internal(format!("concat batches: {}", e)))?;

        // Sort
        let sort_columns: Vec<_> = order_by.iter().map(|o| {
            arrow::compute::SortColumn {
                values: concatenated.column_by_name(&o.column)
                    .cloned()
                    .unwrap_or_else(|| concatenated.column(0).clone()),
                options: Some(arrow::compute::SortOptions {
                    descending: o.descending,
                    nulls_first: o.nulls_first,
                }),
            }
        }).collect();

        let indices = arrow::compute::lexsort_to_indices(&sort_columns, limit.map(|l| l + offset.unwrap_or(0)))
            .map_err(|e| EngineError::Internal(format!("lexsort: {}", e)))?;

        // Take sorted rows
        let sorted_columns: Vec<ArrayRef> = concatenated.columns().iter().map(|col| {
            arrow::compute::take(col.as_ref(), &indices, None)
                .unwrap_or_else(|_| col.clone())
        }).collect();

        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)
            .map_err(|e| EngineError::Internal(format!("create sorted batch: {}", e)))?;

        // Apply offset and limit
        let offset_val = offset.unwrap_or(0);
        let final_batch = if offset_val > 0 || limit.is_some() {
            let start = offset_val.min(sorted_batch.num_rows());
            let len = limit.map(|l| l.min(sorted_batch.num_rows() - start))
                .unwrap_or(sorted_batch.num_rows() - start);
            sorted_batch.slice(start, len)
        } else {
            sorted_batch
        };

        // Write result
        let mut merged_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut merged_ipc, &schema)
                .map_err(|e| EngineError::Internal(format!("ipc writer: {}", e)))?;
            writer.write(&final_batch)
                .map_err(|e| EngineError::Internal(format!("ipc write: {}", e)))?;
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("ipc finish: {}", e)))?;
        }

        Ok(merged_ipc)
    }

    fn apply_offset_limit(
        &self,
        ipc: &[u8],
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<u8>, EngineError> {
        if ipc.is_empty() {
            return Ok(vec![]);
        }

        let cursor = Cursor::new(ipc);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| EngineError::Internal(format!("ipc reader: {}", e)))?;

        let schema = reader.schema();
        let mut all_batches = Vec::new();
        let mut rows_seen = 0;
        let mut rows_taken = 0;
        let max_rows = limit.unwrap_or(usize::MAX);

        for batch in reader {
            let b = batch.map_err(|e| EngineError::Internal(format!("ipc read: {}", e)))?;
            let batch_rows = b.num_rows();

            if rows_seen + batch_rows <= offset {
                // Skip entire batch
                rows_seen += batch_rows;
                continue;
            }

            let start_in_batch = if rows_seen < offset { offset - rows_seen } else { 0 };
            let available = batch_rows - start_in_batch;
            let to_take = available.min(max_rows - rows_taken);

            if to_take > 0 {
                all_batches.push(b.slice(start_in_batch, to_take));
                rows_taken += to_take;
            }

            rows_seen += batch_rows;

            if rows_taken >= max_rows {
                break;
            }
        }

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // Write result
        let mut result_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut result_ipc, &schema)
                .map_err(|e| EngineError::Internal(format!("ipc writer: {}", e)))?;
            for batch in all_batches {
                writer.write(&batch)
                    .map_err(|e| EngineError::Internal(format!("ipc write: {}", e)))?;
            }
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("ipc finish: {}", e)))?;
        }

        Ok(result_ipc)
    }

    fn merge_final_aggregate(
        &self,
        ipc_chunks: &[Vec<u8>],
        partial_aggs: &[PartialAggSpec],
        global: &GlobalPlan,
    ) -> Result<Vec<u8>, EngineError> {
        // For aggregations, use the engine's Aggregator
        if let PlanKind::Simple { agg: Some(agg_plan), filter, .. } = &global.kind {
            let mut aggregator = crate::engine::Aggregator::new(agg_plan.clone())?;

            for chunk in ipc_chunks {
                if !chunk.is_empty() {
                    aggregator.consume_ipc(chunk, filter)?;
                }
            }

            return aggregator.finish();
        }

        // Fallback: if not a simple aggregation plan, try to compute final aggregates manually
        self.compute_final_aggregates(ipc_chunks, partial_aggs)
    }

    fn compute_final_aggregates(
        &self,
        ipc_chunks: &[Vec<u8>],
        partial_aggs: &[PartialAggSpec],
    ) -> Result<Vec<u8>, EngineError> {
        // Collect all partial results
        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for chunk in ipc_chunks {
            if chunk.is_empty() {
                continue;
            }
            let cursor = Cursor::new(chunk);
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| EngineError::Internal(format!("ipc reader: {}", e)))?;

            for batch in reader {
                let b = batch.map_err(|e| EngineError::Internal(format!("ipc read batch: {}", e)))?;
                all_batches.push(b);
            }
        }

        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        // For each partial agg spec, compute the final result
        // This is a simplified implementation for common cases
        let _schema = all_batches[0].schema();
        let mut result_columns: Vec<ArrayRef> = Vec::new();
        let mut result_fields = Vec::new();

        for spec in partial_aggs {
            let final_value = self.compute_single_final_agg(&all_batches, spec)?;
            result_fields.push(arrow_schema::Field::new(&spec.output_column, final_value.data_type().clone(), true));
            result_columns.push(final_value);
        }

        if result_columns.is_empty() {
            // No aggregates to compute, return concatenated
            return self.merge_concatenate(ipc_chunks);
        }

        let result_schema = std::sync::Arc::new(arrow_schema::Schema::new(result_fields));
        let result_batch = RecordBatch::try_new(result_schema.clone(), result_columns)
            .map_err(|e| EngineError::Internal(format!("create result batch: {}", e)))?;

        // Write result
        let mut result_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut result_ipc, &result_schema)
                .map_err(|e| EngineError::Internal(format!("ipc writer: {}", e)))?;
            writer.write(&result_batch)
                .map_err(|e| EngineError::Internal(format!("ipc write: {}", e)))?;
            writer.finish()
                .map_err(|e| EngineError::Internal(format!("ipc finish: {}", e)))?;
        }

        Ok(result_ipc)
    }

    fn compute_single_final_agg(
        &self,
        batches: &[RecordBatch],
        spec: &PartialAggSpec,
    ) -> Result<ArrayRef, EngineError> {
        match &spec.final_agg {
            FinalAggKind::AvgFromSumCount => {
                // Compute: sum(sums) / sum(counts)
                let sum_col = &spec.source_columns[0];
                let count_col = &spec.source_columns[1];

                let mut total_sum: f64 = 0.0;
                let mut total_count: i64 = 0;

                for batch in batches {
                    if let Some(sum_arr) = batch.column_by_name(sum_col) {
                        if let Some(arr) = sum_arr.as_any().downcast_ref::<Float64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    total_sum += arr.value(i);
                                }
                            }
                        } else if let Some(arr) = sum_arr.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    total_sum += arr.value(i) as f64;
                                }
                            }
                        }
                    }
                    if let Some(count_arr) = batch.column_by_name(count_col) {
                        if let Some(arr) = count_arr.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    total_count += arr.value(i);
                                }
                            }
                        }
                    }
                }

                let avg = if total_count > 0 { total_sum / total_count as f64 } else { 0.0 };
                Ok(std::sync::Arc::new(Float64Array::from(vec![avg])))
            }
            FinalAggKind::SumOfSums | FinalAggKind::CountOfCounts => {
                let col = &spec.source_columns[0];
                let mut total: i64 = 0;

                for batch in batches {
                    if let Some(arr) = batch.column_by_name(col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..int_arr.len() {
                                if !int_arr.is_null(i) {
                                    total += int_arr.value(i);
                                }
                            }
                        }
                    }
                }

                Ok(std::sync::Arc::new(Int64Array::from(vec![total])))
            }
            FinalAggKind::MaxOfMaxes => {
                let col = &spec.source_columns[0];
                let mut max_val: Option<i64> = None;

                for batch in batches {
                    if let Some(arr) = batch.column_by_name(col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..int_arr.len() {
                                if !int_arr.is_null(i) {
                                    let v = int_arr.value(i);
                                    max_val = Some(max_val.map(|m| m.max(v)).unwrap_or(v));
                                }
                            }
                        }
                    }
                }

                Ok(std::sync::Arc::new(Int64Array::from(vec![max_val.unwrap_or(0)])))
            }
            FinalAggKind::MinOfMins => {
                let col = &spec.source_columns[0];
                let mut min_val: Option<i64> = None;

                for batch in batches {
                    if let Some(arr) = batch.column_by_name(col) {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..int_arr.len() {
                                if !int_arr.is_null(i) {
                                    let v = int_arr.value(i);
                                    min_val = Some(min_val.map(|m| m.min(v)).unwrap_or(v));
                                }
                            }
                        }
                    }
                }

                Ok(std::sync::Arc::new(Int64Array::from(vec![min_val.unwrap_or(0)])))
            }
            _ => {
                // For STDDEV, VARIANCE, HLL - return 0 as placeholder
                // Full implementation would require more complex partial state
                Ok(std::sync::Arc::new(Float64Array::from(vec![0.0])))
            }
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Merge execution stats from multiple nodes
fn merge_execution_stats(merged: &mut QueryExecutionStats, node_stats: &QueryExecutionStats) {
    // Sum cumulative metrics
    merged.segments_scanned += node_stats.segments_scanned;
    merged.segments_pruned += node_stats.segments_pruned;
    merged.segments_total += node_stats.segments_total;
    merged.bytes_read += node_stats.bytes_read;
    merged.bytes_skipped += node_stats.bytes_skipped;
    merged.rows_scanned += node_stats.rows_scanned;
    merged.rows_returned += node_stats.rows_returned;

    // Max for timing (wall clock of slowest node)
    merged.execution_time_micros = merged.execution_time_micros.max(node_stats.execution_time_micros);
    merged.scan_time_micros = merged.scan_time_micros.max(node_stats.scan_time_micros);
    merged.aggregation_time_micros = merged.aggregation_time_micros.max(node_stats.aggregation_time_micros);
    merged.sort_time_micros = merged.sort_time_micros.max(node_stats.sort_time_micros);
    merged.parse_time_micros = merged.parse_time_micros.max(node_stats.parse_time_micros);
    merged.plan_time_micros = merged.plan_time_micros.max(node_stats.plan_time_micros);

    // OR for cache hit (any hit counts)
    merged.cache_hit = merged.cache_hit || node_stats.cache_hit;

    // Sum memory usage
    if let Some(peak) = node_stats.peak_memory_bytes {
        merged.peak_memory_bytes = Some(
            merged.peak_memory_bytes.unwrap_or(0) + peak
        );
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = DistributedExecutorConfig::default()
            .with_retries(5, 200)
            .with_timeouts(1000, 30000)
            .with_partial_results(true, 2);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_backoff_ms, 200);
        assert_eq!(config.connect_timeout_ms, 1000);
        assert_eq!(config.read_timeout_ms, 30000);
        assert!(config.allow_partial_results);
        assert_eq!(config.min_nodes_for_partial, 2);
    }

    #[test]
    fn test_stats_merge() {
        let mut merged = QueryExecutionStats::default();
        let node1 = QueryExecutionStats {
            execution_time_micros: 100,
            segments_scanned: 5,
            rows_scanned: 1000,
            bytes_read: 50000,
            cache_hit: true,
            ..Default::default()
        };
        let node2 = QueryExecutionStats {
            execution_time_micros: 200,
            segments_scanned: 3,
            rows_scanned: 500,
            bytes_read: 25000,
            cache_hit: false,
            ..Default::default()
        };

        merge_execution_stats(&mut merged, &node1);
        merge_execution_stats(&mut merged, &node2);

        assert_eq!(merged.execution_time_micros, 200); // max
        assert_eq!(merged.segments_scanned, 8); // sum
        assert_eq!(merged.rows_scanned, 1500); // sum
        assert_eq!(merged.bytes_read, 75000); // sum
        assert!(merged.cache_hit); // or
    }
}
