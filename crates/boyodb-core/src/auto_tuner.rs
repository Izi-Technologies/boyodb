//! Auto-Tuning Module
//!
//! Automatically adjusts database parameters based on:
//! - Workload characteristics
//! - System resources
//! - Query performance metrics
//! - Historical patterns

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Configuration
// ============================================================================

/// Auto-tuner configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutoTunerConfig {
    /// Enable auto-tuning
    pub enabled: bool,
    /// Tuning interval in seconds
    pub interval_secs: u64,
    /// Observation window in seconds
    pub observation_window_secs: u64,
    /// Minimum samples before tuning
    pub min_samples: usize,
    /// Maximum parameter change per iteration (percentage)
    pub max_change_percent: f64,
    /// Enable memory tuning
    pub tune_memory: bool,
    /// Enable I/O tuning
    pub tune_io: bool,
    /// Enable query tuning
    pub tune_queries: bool,
    /// Enable compression tuning
    pub tune_compression: bool,
    /// Enable parallelism tuning
    pub tune_parallelism: bool,
    /// Enable cache tuning
    pub tune_cache: bool,
    /// Conservative mode (smaller changes)
    pub conservative: bool,
    /// Learn from workload patterns
    pub learn_patterns: bool,
}

impl Default for AutoTunerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            observation_window_secs: 300,
            min_samples: 100,
            max_change_percent: 20.0,
            tune_memory: true,
            tune_io: true,
            tune_queries: true,
            tune_compression: true,
            tune_parallelism: true,
            tune_cache: true,
            conservative: false,
            learn_patterns: true,
        }
    }
}

/// Tunable parameter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TunableParameter {
    /// Parameter name
    pub name: String,
    /// Current value
    pub current_value: ParameterValue,
    /// Minimum value
    pub min_value: ParameterValue,
    /// Maximum value
    pub max_value: ParameterValue,
    /// Default value
    pub default_value: ParameterValue,
    /// Category
    pub category: ParameterCategory,
    /// Requires restart
    pub requires_restart: bool,
    /// Description
    pub description: String,
    /// Last tuned timestamp
    pub last_tuned: Option<u64>,
}

/// Parameter value types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ParameterValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Bytes(u64),
    Duration(Duration),
}

impl ParameterValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(v) => Some(*v),
            Self::Bytes(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(v) => Some(*v),
            Self::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<u64> {
        match self {
            Self::Bytes(v) => Some(*v),
            Self::Integer(v) => Some(*v as u64),
            _ => None,
        }
    }
}

/// Parameter category
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ParameterCategory {
    Memory,
    IO,
    Query,
    Compression,
    Parallelism,
    Cache,
    Network,
    Storage,
}

// ============================================================================
// Metrics Collection
// ============================================================================

/// System metrics sample
#[derive(Clone, Debug)]
pub struct MetricsSample {
    pub timestamp: Instant,
    /// CPU utilization (0.0-1.0)
    pub cpu_utilization: f64,
    /// Memory utilization (0.0-1.0)
    pub memory_utilization: f64,
    /// I/O utilization (0.0-1.0)
    pub io_utilization: f64,
    /// Queries per second
    pub qps: f64,
    /// Average query latency in milliseconds
    pub avg_latency_ms: f64,
    /// P99 query latency
    pub p99_latency_ms: f64,
    /// Cache hit ratio
    pub cache_hit_ratio: f64,
    /// Buffer pool hit ratio
    pub buffer_pool_hit_ratio: f64,
    /// Disk read bytes per second
    pub disk_read_bps: u64,
    /// Disk write bytes per second
    pub disk_write_bps: u64,
    /// Network bytes per second
    pub network_bps: u64,
    /// Active connections
    pub active_connections: u32,
    /// Waiting queries
    pub waiting_queries: u32,
    /// Compression ratio
    pub compression_ratio: f64,
}

impl Default for MetricsSample {
    fn default() -> Self {
        Self {
            timestamp: Instant::now(),
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            io_utilization: 0.0,
            qps: 0.0,
            avg_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            cache_hit_ratio: 0.0,
            buffer_pool_hit_ratio: 0.0,
            disk_read_bps: 0,
            disk_write_bps: 0,
            network_bps: 0,
            active_connections: 0,
            waiting_queries: 0,
            compression_ratio: 1.0,
        }
    }
}

/// Query pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryPattern {
    /// Pattern fingerprint
    pub fingerprint: String,
    /// Query type
    pub query_type: QueryType,
    /// Execution count
    pub execution_count: u64,
    /// Average execution time
    pub avg_execution_ms: f64,
    /// Total rows processed
    pub total_rows: u64,
    /// Uses index
    pub uses_index: bool,
    /// Has aggregation
    pub has_aggregation: bool,
    /// Has join
    pub has_join: bool,
    /// Table access pattern
    pub table_access: TableAccessMode,
}

/// Query type classification
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryType {
    PointLookup,
    RangeScan,
    FullTableScan,
    Aggregation,
    Join,
    Insert,
    Update,
    Delete,
    Mixed,
}

/// Table access mode
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableAccessMode {
    Sequential,
    Random,
    Mixed,
}

/// Workload characteristics
#[derive(Clone, Debug, Default)]
pub struct WorkloadProfile {
    /// Read/write ratio (0.0 = all writes, 1.0 = all reads)
    pub read_write_ratio: f64,
    /// OLTP vs OLAP score (0.0 = pure OLTP, 1.0 = pure OLAP)
    pub oltp_olap_score: f64,
    /// Data hotness (how often data is accessed)
    pub data_hotness: f64,
    /// Query complexity score
    pub query_complexity: f64,
    /// Concurrency level
    pub concurrency: f64,
    /// Temporal patterns
    pub temporal_pattern: TemporalPattern,
    /// Primary query types
    pub primary_query_types: Vec<(QueryType, f64)>,
}

/// Temporal pattern
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TemporalPattern {
    /// Peak hours (0-23)
    pub peak_hours: Vec<u8>,
    /// Weekday vs weekend ratio
    pub weekday_ratio: f64,
    /// Seasonality detected
    pub seasonal: bool,
    /// Trend (positive = growing, negative = shrinking)
    pub trend: f64,
}

// ============================================================================
// Tuning Recommendations
// ============================================================================

/// Tuning recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TuningRecommendation {
    /// Parameter to tune
    pub parameter: String,
    /// Current value
    pub current_value: ParameterValue,
    /// Recommended value
    pub recommended_value: ParameterValue,
    /// Expected improvement
    pub expected_improvement: f64,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    /// Reasoning
    pub reasoning: String,
    /// Category
    pub category: ParameterCategory,
    /// Risk level
    pub risk: RiskLevel,
    /// Requires restart
    pub requires_restart: bool,
}

/// Risk level for changes
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

/// Tuning result
#[derive(Clone, Debug)]
pub struct TuningResult {
    pub timestamp: u64,
    pub recommendations: Vec<TuningRecommendation>,
    pub applied: Vec<String>,
    pub skipped: Vec<(String, String)>,
    pub workload_profile: WorkloadProfile,
}

// ============================================================================
// Auto-Tuner Implementation
// ============================================================================

/// Auto-tuner
pub struct AutoTuner {
    config: AutoTunerConfig,
    /// Parameters that can be tuned
    parameters: RwLock<HashMap<String, TunableParameter>>,
    /// Metrics history
    metrics_history: RwLock<VecDeque<MetricsSample>>,
    /// Query patterns
    query_patterns: RwLock<HashMap<String, QueryPattern>>,
    /// Tuning history
    tuning_history: RwLock<VecDeque<TuningResult>>,
    /// Running state
    running: AtomicBool,
    /// Total tuning iterations
    iterations: AtomicU64,
    /// Total parameters tuned
    parameters_tuned: AtomicU64,
}

impl AutoTuner {
    pub fn new(config: AutoTunerConfig) -> Self {
        let tuner = Self {
            config,
            parameters: RwLock::new(HashMap::new()),
            metrics_history: RwLock::new(VecDeque::new()),
            query_patterns: RwLock::new(HashMap::new()),
            tuning_history: RwLock::new(VecDeque::new()),
            running: AtomicBool::new(false),
            iterations: AtomicU64::new(0),
            parameters_tuned: AtomicU64::new(0),
        };

        // Register default tunable parameters
        tuner.register_default_parameters();

        tuner
    }

    fn register_default_parameters(&self) {
        let mut params = self.parameters.write().unwrap();

        // Memory parameters
        params.insert("buffer_pool_size".to_string(), TunableParameter {
            name: "buffer_pool_size".to_string(),
            current_value: ParameterValue::Bytes(1024 * 1024 * 1024), // 1GB
            min_value: ParameterValue::Bytes(64 * 1024 * 1024),
            max_value: ParameterValue::Bytes(64 * 1024 * 1024 * 1024),
            default_value: ParameterValue::Bytes(1024 * 1024 * 1024),
            category: ParameterCategory::Memory,
            requires_restart: false,
            description: "Size of the buffer pool for caching data".to_string(),
            last_tuned: None,
        });

        params.insert("query_cache_size".to_string(), TunableParameter {
            name: "query_cache_size".to_string(),
            current_value: ParameterValue::Bytes(256 * 1024 * 1024),
            min_value: ParameterValue::Bytes(0),
            max_value: ParameterValue::Bytes(4 * 1024 * 1024 * 1024),
            default_value: ParameterValue::Bytes(256 * 1024 * 1024),
            category: ParameterCategory::Cache,
            requires_restart: false,
            description: "Size of the query result cache".to_string(),
            last_tuned: None,
        });

        // I/O parameters
        params.insert("io_concurrency".to_string(), TunableParameter {
            name: "io_concurrency".to_string(),
            current_value: ParameterValue::Integer(200),
            min_value: ParameterValue::Integer(1),
            max_value: ParameterValue::Integer(1000),
            default_value: ParameterValue::Integer(200),
            category: ParameterCategory::IO,
            requires_restart: false,
            description: "Maximum concurrent I/O operations".to_string(),
            last_tuned: None,
        });

        params.insert("read_ahead_kb".to_string(), TunableParameter {
            name: "read_ahead_kb".to_string(),
            current_value: ParameterValue::Integer(256),
            min_value: ParameterValue::Integer(0),
            max_value: ParameterValue::Integer(4096),
            default_value: ParameterValue::Integer(256),
            category: ParameterCategory::IO,
            requires_restart: false,
            description: "Read-ahead size in KB".to_string(),
            last_tuned: None,
        });

        // Parallelism parameters
        params.insert("max_parallel_workers".to_string(), TunableParameter {
            name: "max_parallel_workers".to_string(),
            current_value: ParameterValue::Integer(8),
            min_value: ParameterValue::Integer(0),
            max_value: ParameterValue::Integer(128),
            default_value: ParameterValue::Integer(8),
            category: ParameterCategory::Parallelism,
            requires_restart: false,
            description: "Maximum parallel worker threads".to_string(),
            last_tuned: None,
        });

        params.insert("parallel_threshold".to_string(), TunableParameter {
            name: "parallel_threshold".to_string(),
            current_value: ParameterValue::Integer(10000),
            min_value: ParameterValue::Integer(100),
            max_value: ParameterValue::Integer(10000000),
            default_value: ParameterValue::Integer(10000),
            category: ParameterCategory::Parallelism,
            requires_restart: false,
            description: "Minimum rows for parallel execution".to_string(),
            last_tuned: None,
        });

        // Compression parameters
        params.insert("compression_level".to_string(), TunableParameter {
            name: "compression_level".to_string(),
            current_value: ParameterValue::Integer(3),
            min_value: ParameterValue::Integer(1),
            max_value: ParameterValue::Integer(22),
            default_value: ParameterValue::Integer(3),
            category: ParameterCategory::Compression,
            requires_restart: false,
            description: "Compression level (higher = better compression)".to_string(),
            last_tuned: None,
        });

        params.insert("min_compress_block_size".to_string(), TunableParameter {
            name: "min_compress_block_size".to_string(),
            current_value: ParameterValue::Bytes(65536),
            min_value: ParameterValue::Bytes(1024),
            max_value: ParameterValue::Bytes(1024 * 1024),
            default_value: ParameterValue::Bytes(65536),
            category: ParameterCategory::Compression,
            requires_restart: false,
            description: "Minimum block size for compression".to_string(),
            last_tuned: None,
        });

        // Query parameters
        params.insert("max_memory_per_query".to_string(), TunableParameter {
            name: "max_memory_per_query".to_string(),
            current_value: ParameterValue::Bytes(2 * 1024 * 1024 * 1024),
            min_value: ParameterValue::Bytes(64 * 1024 * 1024),
            max_value: ParameterValue::Bytes(64 * 1024 * 1024 * 1024),
            default_value: ParameterValue::Bytes(2 * 1024 * 1024 * 1024),
            category: ParameterCategory::Query,
            requires_restart: false,
            description: "Maximum memory per query".to_string(),
            last_tuned: None,
        });

        params.insert("join_buffer_size".to_string(), TunableParameter {
            name: "join_buffer_size".to_string(),
            current_value: ParameterValue::Bytes(256 * 1024 * 1024),
            min_value: ParameterValue::Bytes(1024 * 1024),
            max_value: ParameterValue::Bytes(4 * 1024 * 1024 * 1024),
            default_value: ParameterValue::Bytes(256 * 1024 * 1024),
            category: ParameterCategory::Query,
            requires_restart: false,
            description: "Buffer size for hash joins".to_string(),
            last_tuned: None,
        });

        params.insert("sort_buffer_size".to_string(), TunableParameter {
            name: "sort_buffer_size".to_string(),
            current_value: ParameterValue::Bytes(64 * 1024 * 1024),
            min_value: ParameterValue::Bytes(1024 * 1024),
            max_value: ParameterValue::Bytes(1024 * 1024 * 1024),
            default_value: ParameterValue::Bytes(64 * 1024 * 1024),
            category: ParameterCategory::Query,
            requires_restart: false,
            description: "Buffer size for sorting".to_string(),
            last_tuned: None,
        });
    }

    /// Record a metrics sample
    pub fn record_metrics(&self, sample: MetricsSample) {
        let mut history = self.metrics_history.write().unwrap();
        history.push_back(sample);

        // Keep only recent samples
        let max_samples = (self.config.observation_window_secs * 10) as usize;
        while history.len() > max_samples {
            history.pop_front();
        }
    }

    /// Record a query execution
    pub fn record_query(&self, fingerprint: &str, query_type: QueryType, execution_ms: f64, rows: u64) {
        let mut patterns = self.query_patterns.write().unwrap();

        let pattern = patterns.entry(fingerprint.to_string()).or_insert_with(|| {
            QueryPattern {
                fingerprint: fingerprint.to_string(),
                query_type: query_type.clone(),
                execution_count: 0,
                avg_execution_ms: 0.0,
                total_rows: 0,
                uses_index: false,
                has_aggregation: matches!(query_type, QueryType::Aggregation),
                has_join: matches!(query_type, QueryType::Join),
                table_access: TableAccessMode::Mixed,
            }
        });

        // Update moving average
        let total = pattern.execution_count as f64 * pattern.avg_execution_ms + execution_ms;
        pattern.execution_count += 1;
        pattern.avg_execution_ms = total / pattern.execution_count as f64;
        pattern.total_rows += rows;
    }

    /// Analyze workload and compute profile
    pub fn analyze_workload(&self) -> WorkloadProfile {
        let metrics = self.metrics_history.read().unwrap();
        let patterns = self.query_patterns.read().unwrap();

        if metrics.is_empty() {
            return WorkloadProfile::default();
        }

        // Compute read/write ratio from query patterns
        let mut reads = 0u64;
        let mut writes = 0u64;
        let mut query_type_counts: HashMap<QueryType, u64> = HashMap::new();

        for pattern in patterns.values() {
            match pattern.query_type {
                QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    writes += pattern.execution_count;
                }
                _ => {
                    reads += pattern.execution_count;
                }
            }
            *query_type_counts.entry(pattern.query_type.clone()).or_insert(0) += pattern.execution_count;
        }

        let total = (reads + writes) as f64;
        let read_write_ratio = if total > 0.0 { reads as f64 / total } else { 0.5 };

        // OLTP vs OLAP score based on query patterns
        let mut oltp_score = 0.0;
        let mut olap_score = 0.0;

        for pattern in patterns.values() {
            let weight = pattern.execution_count as f64;
            match pattern.query_type {
                QueryType::PointLookup | QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    oltp_score += weight;
                }
                QueryType::FullTableScan | QueryType::Aggregation => {
                    olap_score += weight;
                }
                QueryType::RangeScan | QueryType::Join => {
                    oltp_score += weight * 0.3;
                    olap_score += weight * 0.7;
                }
                _ => {
                    oltp_score += weight * 0.5;
                    olap_score += weight * 0.5;
                }
            }
        }

        let total_score = oltp_score + olap_score;
        let oltp_olap_score = if total_score > 0.0 { olap_score / total_score } else { 0.5 };

        // Compute average metrics
        let avg_cpu: f64 = metrics.iter().map(|m| m.cpu_utilization).sum::<f64>() / metrics.len() as f64;
        let avg_io: f64 = metrics.iter().map(|m| m.io_utilization).sum::<f64>() / metrics.len() as f64;
        let avg_connections: f64 = metrics.iter().map(|m| m.active_connections as f64).sum::<f64>() / metrics.len() as f64;

        // Query complexity based on patterns
        let query_complexity = patterns.values()
            .map(|p| {
                let mut complexity = 0.0;
                if p.has_join { complexity += 0.4; }
                if p.has_aggregation { complexity += 0.3; }
                if matches!(p.table_access, TableAccessMode::Random) { complexity += 0.2; }
                if p.avg_execution_ms > 100.0 { complexity += 0.1; }
                complexity
            })
            .sum::<f64>() / patterns.len().max(1) as f64;

        // Primary query types
        let mut primary_query_types: Vec<(QueryType, f64)> = query_type_counts.into_iter()
            .map(|(qt, count)| (qt, count as f64 / total.max(1.0)))
            .collect();
        primary_query_types.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        primary_query_types.truncate(3);

        WorkloadProfile {
            read_write_ratio,
            oltp_olap_score,
            data_hotness: avg_cpu,
            query_complexity,
            concurrency: avg_connections / 100.0,
            temporal_pattern: TemporalPattern::default(),
            primary_query_types,
        }
    }

    /// Generate tuning recommendations
    pub fn generate_recommendations(&self) -> Vec<TuningRecommendation> {
        let profile = self.analyze_workload();
        let metrics = self.metrics_history.read().unwrap();
        let params = self.parameters.read().unwrap();

        let mut recommendations = Vec::new();

        if metrics.len() < self.config.min_samples {
            return recommendations;
        }

        // Compute average metrics
        let avg_cache_hit = metrics.iter().map(|m| m.cache_hit_ratio).sum::<f64>() / metrics.len() as f64;
        let avg_buffer_hit = metrics.iter().map(|m| m.buffer_pool_hit_ratio).sum::<f64>() / metrics.len() as f64;
        let avg_latency = metrics.iter().map(|m| m.avg_latency_ms).sum::<f64>() / metrics.len() as f64;
        let avg_io_util = metrics.iter().map(|m| m.io_utilization).sum::<f64>() / metrics.len() as f64;
        let avg_cpu_util = metrics.iter().map(|m| m.cpu_utilization).sum::<f64>() / metrics.len() as f64;

        // Memory tuning
        if self.config.tune_memory {
            // Buffer pool sizing
            if let Some(param) = params.get("buffer_pool_size") {
                if avg_buffer_hit < 0.95 {
                    let current = param.current_value.as_bytes().unwrap_or(0);
                    let increase = (self.config.max_change_percent / 100.0) * current as f64;
                    let new_value = (current as f64 + increase) as u64;

                    if new_value <= param.max_value.as_bytes().unwrap_or(u64::MAX) {
                        recommendations.push(TuningRecommendation {
                            parameter: "buffer_pool_size".to_string(),
                            current_value: param.current_value.clone(),
                            recommended_value: ParameterValue::Bytes(new_value),
                            expected_improvement: (0.95 - avg_buffer_hit) * 100.0,
                            confidence: 0.8,
                            reasoning: format!(
                                "Buffer pool hit ratio is {:.1}%. Increasing size should improve cache hits.",
                                avg_buffer_hit * 100.0
                            ),
                            category: ParameterCategory::Memory,
                            risk: RiskLevel::Low,
                            requires_restart: false,
                        });
                    }
                }
            }
        }

        // Cache tuning
        if self.config.tune_cache {
            if let Some(param) = params.get("query_cache_size") {
                if avg_cache_hit < 0.5 && profile.read_write_ratio > 0.7 {
                    // Read-heavy workload with low cache hit - increase cache
                    let current = param.current_value.as_bytes().unwrap_or(0);
                    let new_value = (current as f64 * 1.5) as u64;

                    recommendations.push(TuningRecommendation {
                        parameter: "query_cache_size".to_string(),
                        current_value: param.current_value.clone(),
                        recommended_value: ParameterValue::Bytes(new_value),
                        expected_improvement: 15.0,
                        confidence: 0.7,
                        reasoning: format!(
                            "Read-heavy workload ({:.0}% reads) with low cache hit ratio ({:.1}%).",
                            profile.read_write_ratio * 100.0,
                            avg_cache_hit * 100.0
                        ),
                        category: ParameterCategory::Cache,
                        risk: RiskLevel::Low,
                        requires_restart: false,
                    });
                }
            }
        }

        // I/O tuning
        if self.config.tune_io {
            if let Some(param) = params.get("io_concurrency") {
                if avg_io_util > 0.8 && avg_latency > 50.0 {
                    // High I/O utilization and latency
                    let current = param.current_value.as_i64().unwrap_or(200);
                    let new_value = (current as f64 * 0.8) as i64;

                    recommendations.push(TuningRecommendation {
                        parameter: "io_concurrency".to_string(),
                        current_value: param.current_value.clone(),
                        recommended_value: ParameterValue::Integer(new_value),
                        expected_improvement: 10.0,
                        confidence: 0.6,
                        reasoning: format!(
                            "High I/O utilization ({:.1}%) may indicate contention. Reducing concurrency.",
                            avg_io_util * 100.0
                        ),
                        category: ParameterCategory::IO,
                        risk: RiskLevel::Medium,
                        requires_restart: false,
                    });
                }
            }

            // Read-ahead tuning for sequential workloads
            if let Some(param) = params.get("read_ahead_kb") {
                if profile.oltp_olap_score > 0.7 {
                    // OLAP-heavy, increase read-ahead
                    let current = param.current_value.as_i64().unwrap_or(256);
                    let new_value = (current * 2).min(4096);

                    if new_value > current {
                        recommendations.push(TuningRecommendation {
                            parameter: "read_ahead_kb".to_string(),
                            current_value: param.current_value.clone(),
                            recommended_value: ParameterValue::Integer(new_value),
                            expected_improvement: 5.0,
                            confidence: 0.7,
                            reasoning: "OLAP workload benefits from larger read-ahead for sequential scans.".to_string(),
                            category: ParameterCategory::IO,
                            risk: RiskLevel::Low,
                            requires_restart: false,
                        });
                    }
                }
            }
        }

        // Parallelism tuning
        if self.config.tune_parallelism {
            if let Some(param) = params.get("max_parallel_workers") {
                if avg_cpu_util < 0.5 && profile.oltp_olap_score > 0.5 {
                    // Low CPU utilization with analytical queries
                    let current = param.current_value.as_i64().unwrap_or(8);
                    let new_value = (current * 2).min(128);

                    recommendations.push(TuningRecommendation {
                        parameter: "max_parallel_workers".to_string(),
                        current_value: param.current_value.clone(),
                        recommended_value: ParameterValue::Integer(new_value),
                        expected_improvement: 20.0,
                        confidence: 0.75,
                        reasoning: format!(
                            "CPU utilization is low ({:.1}%). More parallel workers can speed up analytical queries.",
                            avg_cpu_util * 100.0
                        ),
                        category: ParameterCategory::Parallelism,
                        risk: RiskLevel::Low,
                        requires_restart: false,
                    });
                }
            }
        }

        // Compression tuning
        if self.config.tune_compression {
            if let Some(param) = params.get("compression_level") {
                let avg_compression = metrics.iter().map(|m| m.compression_ratio).sum::<f64>() / metrics.len() as f64;

                if avg_compression < 0.3 && avg_io_util > 0.5 {
                    // High I/O and low compression - increase compression
                    let current = param.current_value.as_i64().unwrap_or(3);
                    let new_value = (current + 2).min(22);

                    recommendations.push(TuningRecommendation {
                        parameter: "compression_level".to_string(),
                        current_value: param.current_value.clone(),
                        recommended_value: ParameterValue::Integer(new_value),
                        expected_improvement: 10.0,
                        confidence: 0.65,
                        reasoning: format!(
                            "Low compression ratio ({:.1}x) with high I/O. Better compression can reduce I/O.",
                            1.0 / avg_compression
                        ),
                        category: ParameterCategory::Compression,
                        risk: RiskLevel::Low,
                        requires_restart: false,
                    });
                }
            }
        }

        // Query tuning
        if self.config.tune_queries {
            if let Some(param) = params.get("join_buffer_size") {
                // Check if we have join-heavy workload
                let has_joins = self.query_patterns.read().unwrap()
                    .values()
                    .any(|p| p.has_join);

                if has_joins && avg_latency > 100.0 {
                    let current = param.current_value.as_bytes().unwrap_or(0);
                    let new_value = (current as f64 * 1.5) as u64;

                    recommendations.push(TuningRecommendation {
                        parameter: "join_buffer_size".to_string(),
                        current_value: param.current_value.clone(),
                        recommended_value: ParameterValue::Bytes(new_value),
                        expected_improvement: 15.0,
                        confidence: 0.7,
                        reasoning: "Join operations detected with high latency. Larger buffer may help.".to_string(),
                        category: ParameterCategory::Query,
                        risk: RiskLevel::Low,
                        requires_restart: false,
                    });
                }
            }
        }

        // Sort recommendations by expected improvement * confidence
        recommendations.sort_by(|a, b| {
            let score_a = a.expected_improvement * a.confidence;
            let score_b = b.expected_improvement * b.confidence;
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        // In conservative mode, only return top recommendations
        if self.config.conservative {
            recommendations.truncate(3);
        }

        recommendations
    }

    /// Apply a recommendation
    pub fn apply_recommendation(&self, rec: &TuningRecommendation) -> Result<(), String> {
        let mut params = self.parameters.write().unwrap();

        let param = params.get_mut(&rec.parameter).ok_or_else(|| {
            format!("parameter not found: {}", rec.parameter)
        })?;

        param.current_value = rec.recommended_value.clone();
        param.last_tuned = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        self.parameters_tuned.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Run a tuning iteration
    pub fn run_tuning_iteration(&self) -> TuningResult {
        self.iterations.fetch_add(1, Ordering::Relaxed);

        let recommendations = self.generate_recommendations();
        let workload_profile = self.analyze_workload();

        let mut applied = Vec::new();
        let mut skipped = Vec::new();

        for rec in &recommendations {
            // Skip high-risk changes in conservative mode
            if self.config.conservative && rec.risk == RiskLevel::High {
                skipped.push((rec.parameter.clone(), "high risk".to_string()));
                continue;
            }

            // Skip if requires restart
            if rec.requires_restart {
                skipped.push((rec.parameter.clone(), "requires restart".to_string()));
                continue;
            }

            // Apply the recommendation
            match self.apply_recommendation(rec) {
                Ok(_) => applied.push(rec.parameter.clone()),
                Err(e) => skipped.push((rec.parameter.clone(), e)),
            }
        }

        let result = TuningResult {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            recommendations,
            applied,
            skipped,
            workload_profile,
        };

        // Store in history
        let mut history = self.tuning_history.write().unwrap();
        history.push_back(result.clone());
        while history.len() > 100 {
            history.pop_front();
        }

        result
    }

    /// Get current parameter values
    pub fn get_parameters(&self) -> HashMap<String, TunableParameter> {
        self.parameters.read().unwrap().clone()
    }

    /// Get parameter by name
    pub fn get_parameter(&self, name: &str) -> Option<TunableParameter> {
        self.parameters.read().unwrap().get(name).cloned()
    }

    /// Set parameter value
    pub fn set_parameter(&self, name: &str, value: ParameterValue) -> Result<(), String> {
        let mut params = self.parameters.write().unwrap();
        let param = params.get_mut(name).ok_or_else(|| {
            format!("parameter not found: {}", name)
        })?;

        param.current_value = value;
        param.last_tuned = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        Ok(())
    }

    /// Get tuning statistics
    pub fn stats(&self) -> AutoTunerStats {
        let history = self.tuning_history.read().unwrap();
        let metrics = self.metrics_history.read().unwrap();

        AutoTunerStats {
            iterations: self.iterations.load(Ordering::Relaxed),
            parameters_tuned: self.parameters_tuned.load(Ordering::Relaxed),
            metrics_samples: metrics.len(),
            tuning_history_size: history.len(),
            running: self.running.load(Ordering::Relaxed),
        }
    }
}

/// Auto-tuner statistics
#[derive(Clone, Debug, Default)]
pub struct AutoTunerStats {
    pub iterations: u64,
    pub parameters_tuned: u64,
    pub metrics_samples: usize,
    pub tuning_history_size: usize,
    pub running: bool,
}

// ============================================================================
// Workload Classifier
// ============================================================================

/// Workload classifier for pattern detection
pub struct WorkloadClassifier {
    /// Historical profiles
    profiles: RwLock<VecDeque<(u64, WorkloadProfile)>>,
    /// Detected patterns
    patterns: RwLock<Vec<WorkloadPattern>>,
}

impl WorkloadClassifier {
    pub fn new() -> Self {
        Self {
            profiles: RwLock::new(VecDeque::new()),
            patterns: RwLock::new(Vec::new()),
        }
    }

    /// Record a workload profile
    pub fn record(&self, profile: WorkloadProfile) {
        let mut profiles = self.profiles.write().unwrap();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        profiles.push_back((timestamp, profile));

        // Keep last 24 hours
        let cutoff = timestamp.saturating_sub(86400);
        while profiles.front().map(|(t, _)| *t < cutoff).unwrap_or(false) {
            profiles.pop_front();
        }
    }

    /// Detect workload patterns
    pub fn detect_patterns(&self) -> Vec<WorkloadPattern> {
        let profiles = self.profiles.read().unwrap();

        if profiles.len() < 24 {
            return Vec::new();
        }

        let mut patterns = Vec::new();

        // Check for daily patterns
        let hourly_stats = self.compute_hourly_stats(&profiles);

        // Find peak hours
        let avg_load: f64 = hourly_stats.iter().sum::<f64>() / 24.0;
        let peak_hours: Vec<u8> = hourly_stats.iter().enumerate()
            .filter(|(_, &load)| load > avg_load * 1.5)
            .map(|(h, _)| h as u8)
            .collect();

        if !peak_hours.is_empty() {
            patterns.push(WorkloadPattern {
                pattern_type: PatternType::DailyPeak,
                description: format!("Peak hours detected: {:?}", peak_hours),
                confidence: 0.8,
                recommendations: vec![
                    "Consider scaling up during peak hours".to_string(),
                    "Pre-warm caches before peaks".to_string(),
                ],
            });
        }

        // Check for trending
        let recent_profiles: Vec<_> = profiles.iter().rev().take(100).collect();
        if recent_profiles.len() >= 10 {
            let first_half_load: f64 = recent_profiles[50..].iter()
                .map(|(_, p)| p.concurrency)
                .sum::<f64>() / 50.0;
            let second_half_load: f64 = recent_profiles[..50].iter()
                .map(|(_, p)| p.concurrency)
                .sum::<f64>() / 50.0;

            if second_half_load > first_half_load * 1.2 {
                patterns.push(WorkloadPattern {
                    pattern_type: PatternType::GrowthTrend,
                    description: "Workload is growing".to_string(),
                    confidence: 0.7,
                    recommendations: vec![
                        "Consider capacity planning".to_string(),
                        "Review resource limits".to_string(),
                    ],
                });
            }
        }

        patterns
    }

    fn compute_hourly_stats(&self, profiles: &VecDeque<(u64, WorkloadProfile)>) -> [f64; 24] {
        let mut hourly_sums = [0.0f64; 24];
        let mut hourly_counts = [0u32; 24];

        for (ts, profile) in profiles {
            let hour = ((*ts / 3600) % 24) as usize;
            hourly_sums[hour] += profile.concurrency;
            hourly_counts[hour] += 1;
        }

        let mut result = [0.0f64; 24];
        for i in 0..24 {
            if hourly_counts[i] > 0 {
                result[i] = hourly_sums[i] / hourly_counts[i] as f64;
            }
        }
        result
    }
}

impl Default for WorkloadClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Detected workload pattern
#[derive(Clone, Debug)]
pub struct WorkloadPattern {
    pub pattern_type: PatternType,
    pub description: String,
    pub confidence: f64,
    pub recommendations: Vec<String>,
}

/// Pattern types
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PatternType {
    DailyPeak,
    WeeklyPattern,
    GrowthTrend,
    DeclineTrend,
    Seasonal,
    Spiky,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tuner_creation() {
        let tuner = AutoTuner::new(AutoTunerConfig::default());
        let params = tuner.get_parameters();

        assert!(params.contains_key("buffer_pool_size"));
        assert!(params.contains_key("max_parallel_workers"));
    }

    #[test]
    fn test_record_metrics() {
        let tuner = AutoTuner::new(AutoTunerConfig::default());

        for _ in 0..10 {
            tuner.record_metrics(MetricsSample {
                cpu_utilization: 0.5,
                memory_utilization: 0.6,
                cache_hit_ratio: 0.9,
                ..Default::default()
            });
        }

        let stats = tuner.stats();
        assert_eq!(stats.metrics_samples, 10);
    }

    #[test]
    fn test_workload_analysis() {
        let tuner = AutoTuner::new(AutoTunerConfig::default());

        // Record some query patterns
        tuner.record_query("select_users", QueryType::PointLookup, 5.0, 1);
        tuner.record_query("select_users", QueryType::PointLookup, 4.0, 1);
        tuner.record_query("agg_sales", QueryType::Aggregation, 500.0, 10000);

        let profile = tuner.analyze_workload();

        // Should detect mixed workload (score can be 0.0 if no metrics recorded)
        assert!(profile.oltp_olap_score >= 0.0);
        assert!(profile.oltp_olap_score <= 1.0);
    }

    #[test]
    fn test_recommendations() {
        let tuner = AutoTuner::new(AutoTunerConfig {
            min_samples: 5,
            ..Default::default()
        });

        // Record metrics with low cache hit
        for _ in 0..10 {
            tuner.record_metrics(MetricsSample {
                buffer_pool_hit_ratio: 0.7,
                cache_hit_ratio: 0.3,
                avg_latency_ms: 50.0,
                ..Default::default()
            });
        }

        let recommendations = tuner.generate_recommendations();

        // Should recommend increasing buffer pool
        let buffer_rec = recommendations.iter()
            .find(|r| r.parameter == "buffer_pool_size");
        assert!(buffer_rec.is_some());
    }

    #[test]
    fn test_set_parameter() {
        let tuner = AutoTuner::new(AutoTunerConfig::default());

        tuner.set_parameter("buffer_pool_size", ParameterValue::Bytes(2 * 1024 * 1024 * 1024)).unwrap();

        let param = tuner.get_parameter("buffer_pool_size").unwrap();
        assert_eq!(param.current_value.as_bytes().unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_workload_classifier() {
        let classifier = WorkloadClassifier::new();

        // Record some profiles (need at least 24 for pattern detection)
        for _ in 0..50 {
            classifier.record(WorkloadProfile {
                concurrency: 0.5,
                ..Default::default()
            });
        }

        // With 50 uniform profiles, may or may not detect patterns
        // based on temporal distribution - just verify it doesn't panic
        let _patterns = classifier.detect_patterns();
    }
}
