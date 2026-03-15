//! Parallel Replicas
//!
//! ClickHouse-style parallel replica query execution for scaling read performance.
//! Distributes query parts across multiple replicas for parallel processing.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};

/// Replica endpoint
#[derive(Debug, Clone)]
pub struct ReplicaEndpoint {
    /// Replica ID
    pub id: String,
    /// Host address
    pub host: String,
    /// Port
    pub port: u16,
    /// Is healthy
    pub healthy: bool,
    /// Current load (0.0 - 1.0)
    pub load: f64,
    /// Last health check
    pub last_check: Option<Instant>,
    /// Weight for load balancing
    pub weight: u32,
}

impl ReplicaEndpoint {
    pub fn new(id: String, host: String, port: u16) -> Self {
        Self {
            id,
            host,
            port,
            healthy: true,
            load: 0.0,
            last_check: None,
            weight: 100,
        }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Parallel query part
#[derive(Debug, Clone)]
pub struct QueryPart {
    /// Part ID
    pub id: u64,
    /// Original query
    pub query: String,
    /// Data range start
    pub range_start: Option<u64>,
    /// Data range end
    pub range_end: Option<u64>,
    /// Partition filter
    pub partition_filter: Option<String>,
    /// Assigned replica
    pub assigned_replica: Option<String>,
    /// Estimated rows
    pub estimated_rows: Option<u64>,
}

/// Query part result
#[derive(Debug, Clone)]
pub struct PartResult {
    /// Part ID
    pub part_id: u64,
    /// Replica that executed
    pub replica_id: String,
    /// Result data (serialized)
    pub data: Vec<u8>,
    /// Row count
    pub row_count: u64,
    /// Execution time
    pub execution_time: Duration,
    /// Bytes scanned
    pub bytes_scanned: u64,
}

/// Parallel replica configuration
#[derive(Debug, Clone)]
pub struct ParallelReplicaConfig {
    /// Enable parallel replicas
    pub enabled: bool,
    /// Minimum rows to enable parallelism
    pub min_rows_for_parallel: u64,
    /// Maximum concurrent replicas
    pub max_parallel_replicas: usize,
    /// Part timeout
    pub part_timeout: Duration,
    /// Retry failed parts
    pub retry_failed: bool,
    /// Max retries per part
    pub max_retries: u32,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Load balancing strategy
    pub load_balance: LoadBalanceStrategy,
}

impl Default for ParallelReplicaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_rows_for_parallel: 100_000,
            max_parallel_replicas: 8,
            part_timeout: Duration::from_secs(30),
            retry_failed: true,
            max_retries: 3,
            health_check_interval: Duration::from_secs(5),
            load_balance: LoadBalanceStrategy::LeastLoaded,
        }
    }
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadBalanceStrategy {
    /// Round robin
    RoundRobin,
    /// Least loaded replica
    LeastLoaded,
    /// Random selection
    Random,
    /// Weighted random
    WeightedRandom,
    /// First available
    FirstAvailable,
}

/// Parallel replica coordinator
pub struct ParallelReplicaCoordinator {
    /// Configuration
    config: ParallelReplicaConfig,
    /// Registered replicas
    replicas: RwLock<HashMap<String, ReplicaEndpoint>>,
    /// Round robin counter
    rr_counter: RwLock<usize>,
    /// Statistics
    stats: RwLock<CoordinatorStats>,
}

/// Coordinator statistics
#[derive(Debug, Clone, Default)]
pub struct CoordinatorStats {
    /// Total queries executed
    pub queries_executed: u64,
    /// Total parts executed
    pub parts_executed: u64,
    /// Failed parts
    pub parts_failed: u64,
    /// Parts retried
    pub parts_retried: u64,
    /// Total rows processed
    pub rows_processed: u64,
    /// Total bytes scanned
    pub bytes_scanned: u64,
    /// Average part execution time
    pub avg_part_time_ms: f64,
}

impl ParallelReplicaCoordinator {
    pub fn new(config: ParallelReplicaConfig) -> Self {
        Self {
            config,
            replicas: RwLock::new(HashMap::new()),
            rr_counter: RwLock::new(0),
            stats: RwLock::new(CoordinatorStats::default()),
        }
    }

    /// Register a replica
    pub fn register_replica(&self, replica: ReplicaEndpoint) {
        self.replicas.write().insert(replica.id.clone(), replica);
    }

    /// Unregister a replica
    pub fn unregister_replica(&self, replica_id: &str) {
        self.replicas.write().remove(replica_id);
    }

    /// Get healthy replicas
    pub fn healthy_replicas(&self) -> Vec<ReplicaEndpoint> {
        self.replicas.read()
            .values()
            .filter(|r| r.healthy)
            .cloned()
            .collect()
    }

    /// Mark replica as unhealthy
    pub fn mark_unhealthy(&self, replica_id: &str) {
        if let Some(replica) = self.replicas.write().get_mut(replica_id) {
            replica.healthy = false;
        }
    }

    /// Mark replica as healthy
    pub fn mark_healthy(&self, replica_id: &str) {
        if let Some(replica) = self.replicas.write().get_mut(replica_id) {
            replica.healthy = true;
            replica.last_check = Some(Instant::now());
        }
    }

    /// Update replica load
    pub fn update_load(&self, replica_id: &str, load: f64) {
        if let Some(replica) = self.replicas.write().get_mut(replica_id) {
            replica.load = load.clamp(0.0, 1.0);
        }
    }

    /// Plan parallel query execution
    pub fn plan_parallel_query(
        &self,
        query: &str,
        estimated_rows: u64,
        partitions: &[String],
    ) -> ParallelQueryPlan {
        let healthy = self.healthy_replicas();

        if !self.config.enabled ||
           healthy.is_empty() ||
           estimated_rows < self.config.min_rows_for_parallel {
            return ParallelQueryPlan {
                parallel: false,
                parts: vec![QueryPart {
                    id: 0,
                    query: query.into(),
                    range_start: None,
                    range_end: None,
                    partition_filter: None,
                    assigned_replica: None,
                    estimated_rows: Some(estimated_rows),
                }],
                replicas_used: 0,
                estimated_speedup: 1.0,
            };
        }

        let num_replicas = healthy.len().min(self.config.max_parallel_replicas);
        let mut parts = Vec::new();

        if partitions.is_empty() {
            // Split by row ranges
            let rows_per_part = estimated_rows / num_replicas as u64;
            for i in 0..num_replicas {
                let start = i as u64 * rows_per_part;
                let end = if i == num_replicas - 1 {
                    estimated_rows
                } else {
                    (i as u64 + 1) * rows_per_part
                };

                parts.push(QueryPart {
                    id: i as u64,
                    query: query.into(),
                    range_start: Some(start),
                    range_end: Some(end),
                    partition_filter: None,
                    assigned_replica: Some(healthy[i].id.clone()),
                    estimated_rows: Some(end - start),
                });
            }
        } else {
            // Split by partitions
            let partitions_per_replica = (partitions.len() + num_replicas - 1) / num_replicas;
            for (i, chunk) in partitions.chunks(partitions_per_replica).enumerate() {
                let replica_idx = i % healthy.len();
                for (j, partition) in chunk.iter().enumerate() {
                    parts.push(QueryPart {
                        id: (i * partitions_per_replica + j) as u64,
                        query: query.into(),
                        range_start: None,
                        range_end: None,
                        partition_filter: Some(partition.clone()),
                        assigned_replica: Some(healthy[replica_idx].id.clone()),
                        estimated_rows: None,
                    });
                }
            }
        }

        ParallelQueryPlan {
            parallel: true,
            parts,
            replicas_used: num_replicas,
            estimated_speedup: num_replicas as f64 * 0.85, // Account for coordination overhead
        }
    }

    /// Select replica for a query part
    pub fn select_replica(&self) -> Option<ReplicaEndpoint> {
        let healthy = self.healthy_replicas();
        if healthy.is_empty() {
            return None;
        }

        match self.config.load_balance {
            LoadBalanceStrategy::RoundRobin => {
                let mut counter = self.rr_counter.write();
                let idx = *counter % healthy.len();
                *counter = counter.wrapping_add(1);
                Some(healthy[idx].clone())
            }
            LoadBalanceStrategy::LeastLoaded => {
                healthy.into_iter()
                    .min_by(|a, b| a.load.partial_cmp(&b.load).unwrap())
            }
            LoadBalanceStrategy::Random => {
                use std::time::SystemTime;
                let seed = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as usize;
                Some(healthy[seed % healthy.len()].clone())
            }
            LoadBalanceStrategy::WeightedRandom => {
                let total_weight: u32 = healthy.iter().map(|r| r.weight).sum();
                if total_weight == 0 {
                    return healthy.into_iter().next();
                }

                use std::time::SystemTime;
                let seed = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u32;
                let target = seed % total_weight;

                let mut cumulative = 0;
                for replica in healthy {
                    cumulative += replica.weight;
                    if cumulative > target {
                        return Some(replica);
                    }
                }
                None
            }
            LoadBalanceStrategy::FirstAvailable => {
                healthy.into_iter().next()
            }
        }
    }

    /// Merge results from parallel execution
    pub fn merge_results(&self, results: Vec<PartResult>) -> MergedResult {
        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut max_time = Duration::ZERO;
        let mut merged_data = Vec::new();

        for result in &results {
            total_rows += result.row_count;
            total_bytes += result.bytes_scanned;
            if result.execution_time > max_time {
                max_time = result.execution_time;
            }
            merged_data.extend_from_slice(&result.data);
        }

        // Update stats
        let mut stats = self.stats.write();
        stats.queries_executed += 1;
        stats.parts_executed += results.len() as u64;
        stats.rows_processed += total_rows;
        stats.bytes_scanned += total_bytes;

        MergedResult {
            data: merged_data,
            total_rows,
            total_bytes_scanned: total_bytes,
            wall_time: max_time,
            parts_executed: results.len(),
        }
    }

    /// Record part failure
    pub fn record_failure(&self, _part_id: u64, replica_id: &str) {
        let mut stats = self.stats.write();
        stats.parts_failed += 1;

        // Check if we should mark replica unhealthy
        // (In production, track failure rate per replica)
        drop(stats);

        // For now, just update load
        self.update_load(replica_id, 1.0);
    }

    /// Get statistics
    pub fn stats(&self) -> CoordinatorStats {
        self.stats.read().clone()
    }
}

impl Default for ParallelReplicaCoordinator {
    fn default() -> Self {
        Self::new(ParallelReplicaConfig::default())
    }
}

/// Parallel query plan
#[derive(Debug, Clone)]
pub struct ParallelQueryPlan {
    /// Whether to use parallel execution
    pub parallel: bool,
    /// Query parts
    pub parts: Vec<QueryPart>,
    /// Number of replicas used
    pub replicas_used: usize,
    /// Estimated speedup
    pub estimated_speedup: f64,
}

/// Merged result from parallel execution
#[derive(Debug)]
pub struct MergedResult {
    /// Merged data
    pub data: Vec<u8>,
    /// Total rows
    pub total_rows: u64,
    /// Total bytes scanned
    pub total_bytes_scanned: u64,
    /// Wall clock time
    pub wall_time: Duration,
    /// Parts executed
    pub parts_executed: usize,
}

/// Parallel replica executor trait
pub trait ParallelExecutor: Send + Sync {
    /// Execute a query part on a replica
    fn execute_part(
        &self,
        part: &QueryPart,
        replica: &ReplicaEndpoint,
    ) -> Result<PartResult, ParallelReplicaError>;
}

/// Parallel replica error
#[derive(Debug, Clone)]
pub enum ParallelReplicaError {
    /// No healthy replicas
    NoHealthyReplicas,
    /// Replica connection failed
    ConnectionFailed(String),
    /// Query execution failed
    ExecutionFailed(String),
    /// Timeout
    Timeout,
    /// Merge failed
    MergeFailed(String),
}

impl std::fmt::Display for ParallelReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHealthyReplicas => write!(f, "No healthy replicas available"),
            Self::ConnectionFailed(s) => write!(f, "Connection failed: {}", s),
            Self::ExecutionFailed(s) => write!(f, "Execution failed: {}", s),
            Self::Timeout => write!(f, "Query part timed out"),
            Self::MergeFailed(s) => write!(f, "Result merge failed: {}", s),
        }
    }
}

impl std::error::Error for ParallelReplicaError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_query_planning() {
        let coordinator = ParallelReplicaCoordinator::default();

        // Register replicas
        coordinator.register_replica(ReplicaEndpoint::new("r1".into(), "host1".into(), 8765));
        coordinator.register_replica(ReplicaEndpoint::new("r2".into(), "host2".into(), 8765));
        coordinator.register_replica(ReplicaEndpoint::new("r3".into(), "host3".into(), 8765));

        // Plan query with enough rows for parallelism
        let plan = coordinator.plan_parallel_query(
            "SELECT * FROM test",
            1_000_000,
            &[],
        );

        assert!(plan.parallel);
        assert_eq!(plan.replicas_used, 3);
        assert_eq!(plan.parts.len(), 3);
    }

    #[test]
    fn test_no_parallelism_for_small_queries() {
        let coordinator = ParallelReplicaCoordinator::default();
        coordinator.register_replica(ReplicaEndpoint::new("r1".into(), "host1".into(), 8765));

        let plan = coordinator.plan_parallel_query(
            "SELECT * FROM test",
            100, // Too few rows
            &[],
        );

        assert!(!plan.parallel);
        assert_eq!(plan.parts.len(), 1);
    }

    #[test]
    fn test_partition_based_parallelism() {
        let coordinator = ParallelReplicaCoordinator::default();
        coordinator.register_replica(ReplicaEndpoint::new("r1".into(), "host1".into(), 8765));
        coordinator.register_replica(ReplicaEndpoint::new("r2".into(), "host2".into(), 8765));

        let partitions = vec![
            "2024-01".into(),
            "2024-02".into(),
            "2024-03".into(),
            "2024-04".into(),
        ];

        let plan = coordinator.plan_parallel_query(
            "SELECT * FROM test",
            1_000_000,
            &partitions,
        );

        assert!(plan.parallel);
        assert_eq!(plan.parts.len(), 4);
    }

    #[test]
    fn test_replica_health() {
        let coordinator = ParallelReplicaCoordinator::default();
        coordinator.register_replica(ReplicaEndpoint::new("r1".into(), "host1".into(), 8765));
        coordinator.register_replica(ReplicaEndpoint::new("r2".into(), "host2".into(), 8765));

        assert_eq!(coordinator.healthy_replicas().len(), 2);

        coordinator.mark_unhealthy("r1");
        assert_eq!(coordinator.healthy_replicas().len(), 1);

        coordinator.mark_healthy("r1");
        assert_eq!(coordinator.healthy_replicas().len(), 2);
    }

    #[test]
    fn test_load_balancing() {
        let coordinator = ParallelReplicaCoordinator::new(ParallelReplicaConfig {
            load_balance: LoadBalanceStrategy::LeastLoaded,
            ..Default::default()
        });

        coordinator.register_replica(ReplicaEndpoint::new("r1".into(), "host1".into(), 8765));
        coordinator.register_replica(ReplicaEndpoint::new("r2".into(), "host2".into(), 8765));

        coordinator.update_load("r1", 0.8);
        coordinator.update_load("r2", 0.2);

        // Least loaded should select r2
        let selected = coordinator.select_replica().unwrap();
        assert_eq!(selected.id, "r2");
    }

    #[test]
    fn test_merge_results() {
        let coordinator = ParallelReplicaCoordinator::default();

        let results = vec![
            PartResult {
                part_id: 0,
                replica_id: "r1".into(),
                data: vec![1, 2, 3],
                row_count: 100,
                execution_time: Duration::from_millis(50),
                bytes_scanned: 1000,
            },
            PartResult {
                part_id: 1,
                replica_id: "r2".into(),
                data: vec![4, 5, 6],
                row_count: 150,
                execution_time: Duration::from_millis(75),
                bytes_scanned: 1500,
            },
        ];

        let merged = coordinator.merge_results(results);

        assert_eq!(merged.total_rows, 250);
        assert_eq!(merged.total_bytes_scanned, 2500);
        assert_eq!(merged.parts_executed, 2);
        assert_eq!(merged.data, vec![1, 2, 3, 4, 5, 6]);
    }
}
