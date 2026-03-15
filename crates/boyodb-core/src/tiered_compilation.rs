//! Tiered Compilation
//!
//! Multi-level JIT compilation strategy for query execution.
//! Starts with interpretation, then compiles hot paths progressively.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Compilation tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompilationTier {
    /// Interpreted execution (fastest startup)
    Interpreted = 0,
    /// Baseline JIT (quick compilation, moderate speed)
    Baseline = 1,
    /// Optimized JIT (slower compilation, faster execution)
    Optimized = 2,
    /// Vectorized JIT (SIMD-optimized)
    Vectorized = 3,
}

impl CompilationTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Interpreted => "interpreted",
            Self::Baseline => "baseline",
            Self::Optimized => "optimized",
            Self::Vectorized => "vectorized",
        }
    }
}

/// Compiled code entry
#[derive(Debug, Clone)]
pub struct CompiledCode {
    /// Query fingerprint
    pub fingerprint: String,
    /// Current tier
    pub tier: CompilationTier,
    /// Compiled code (bytecode or machine code pointer)
    pub code: Vec<u8>,
    /// Code size in bytes
    pub code_size: usize,
    /// Compilation time
    pub compilation_time: Duration,
    /// Last used timestamp
    pub last_used: Instant,
    /// Execution count
    pub execution_count: u64,
    /// Total execution time
    pub total_execution_time: Duration,
}

/// Compilation request
#[derive(Debug, Clone)]
pub struct CompilationRequest {
    /// Query fingerprint
    pub fingerprint: String,
    /// Target tier
    pub target_tier: CompilationTier,
    /// Query IR (intermediate representation)
    pub ir: QueryIR,
    /// Priority (higher = more urgent)
    pub priority: u32,
}

/// Query intermediate representation
#[derive(Debug, Clone)]
pub struct QueryIR {
    /// Operations
    pub ops: Vec<IROp>,
    /// Input columns
    pub inputs: Vec<String>,
    /// Output columns
    pub outputs: Vec<String>,
    /// Estimated rows
    pub estimated_rows: u64,
}

/// IR operation
#[derive(Debug, Clone)]
pub enum IROp {
    /// Scan table
    Scan { table: String, columns: Vec<String> },
    /// Filter rows
    Filter { predicate: IRPredicate },
    /// Project columns
    Project { columns: Vec<String> },
    /// Aggregate
    Aggregate { group_by: Vec<String>, aggregates: Vec<IRAggregate> },
    /// Sort
    Sort { columns: Vec<(String, bool)> },
    /// Limit
    Limit { count: u64, offset: u64 },
    /// Join
    Join { join_type: String, left_key: String, right_key: String },
    /// Hash build
    HashBuild { key: String },
    /// Hash probe
    HashProbe { key: String },
}

/// IR predicate
#[derive(Debug, Clone)]
pub struct IRPredicate {
    pub column: String,
    pub op: String,
    pub value: IRValue,
}

/// IR aggregate
#[derive(Debug, Clone)]
pub struct IRAggregate {
    pub function: String,
    pub column: String,
    pub alias: String,
}

/// IR value
#[derive(Debug, Clone)]
pub enum IRValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Column(String),
}

/// Tiered compilation configuration
#[derive(Debug, Clone)]
pub struct TieredCompilationConfig {
    /// Enable tiered compilation
    pub enabled: bool,
    /// Baseline tier threshold (executions)
    pub baseline_threshold: u64,
    /// Optimized tier threshold (executions)
    pub optimized_threshold: u64,
    /// Vectorized tier threshold (executions)
    pub vectorized_threshold: u64,
    /// Code cache size limit
    pub cache_size_limit: usize,
    /// Compilation timeout
    pub compilation_timeout: Duration,
    /// Enable background compilation
    pub background_compilation: bool,
    /// Max concurrent compilations
    pub max_concurrent_compilations: usize,
}

impl Default for TieredCompilationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            baseline_threshold: 5,
            optimized_threshold: 100,
            vectorized_threshold: 1000,
            cache_size_limit: 256 * 1024 * 1024, // 256MB
            compilation_timeout: Duration::from_secs(10),
            background_compilation: true,
            max_concurrent_compilations: 4,
        }
    }
}

/// Tiered compilation manager
pub struct TieredCompilationManager {
    /// Configuration
    config: TieredCompilationConfig,
    /// Code cache
    cache: RwLock<HashMap<String, CompiledCode>>,
    /// Execution counters
    counters: RwLock<HashMap<String, ExecutionCounter>>,
    /// Pending compilations
    pending: RwLock<Vec<CompilationRequest>>,
    /// Statistics
    stats: RwLock<CompilationStats>,
}

/// Execution counter
#[derive(Debug, Clone, Default)]
struct ExecutionCounter {
    /// Execution count
    count: u64,
    /// Total execution time
    total_time: Duration,
    /// Last execution
    last_execution: Option<Instant>,
}

/// Compilation statistics
#[derive(Debug, Clone, Default)]
pub struct CompilationStats {
    /// Total compilations
    pub total_compilations: u64,
    /// Baseline compilations
    pub baseline_compilations: u64,
    /// Optimized compilations
    pub optimized_compilations: u64,
    /// Vectorized compilations
    pub vectorized_compilations: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Cache evictions
    pub cache_evictions: u64,
    /// Total compilation time
    pub total_compilation_time: Duration,
    /// Average speedup
    pub avg_speedup: f64,
}

impl TieredCompilationManager {
    pub fn new(config: TieredCompilationConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
            counters: RwLock::new(HashMap::new()),
            pending: RwLock::new(Vec::new()),
            stats: RwLock::new(CompilationStats::default()),
        }
    }

    /// Get compiled code for a query, or return None for interpretation
    pub fn get_code(&self, fingerprint: &str) -> Option<CompiledCode> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.cache.write().unwrap();
        let mut stats = self.stats.write().unwrap();

        if let Some(code) = cache.get_mut(fingerprint) {
            code.last_used = Instant::now();
            code.execution_count += 1;
            stats.cache_hits += 1;
            return Some(code.clone());
        }

        stats.cache_misses += 1;
        None
    }

    /// Record execution for tier promotion tracking
    pub fn record_execution(&self, fingerprint: &str, execution_time: Duration) {
        let mut counters = self.counters.write().unwrap();
        let counter = counters.entry(fingerprint.to_string())
            .or_insert_with(ExecutionCounter::default);

        counter.count += 1;
        counter.total_time += execution_time;
        counter.last_execution = Some(Instant::now());

        let count = counter.count;
        drop(counters);

        // Check if we should promote
        self.maybe_promote(fingerprint, count);
    }

    /// Check if query should be promoted to a higher tier
    fn maybe_promote(&self, fingerprint: &str, count: u64) {
        let current_tier = {
            let cache = self.cache.read().unwrap();
            cache.get(fingerprint).map(|c| c.tier)
        };

        let target_tier = if count >= self.config.vectorized_threshold {
            CompilationTier::Vectorized
        } else if count >= self.config.optimized_threshold {
            CompilationTier::Optimized
        } else if count >= self.config.baseline_threshold {
            CompilationTier::Baseline
        } else {
            return; // No promotion needed
        };

        // Check if already at or above target tier
        if let Some(current) = current_tier {
            if current >= target_tier {
                return;
            }
        }

        // Queue for compilation (would be done in production)
        // For now, just track the request
        let mut pending = self.pending.write().unwrap();
        if !pending.iter().any(|p| p.fingerprint == fingerprint && p.target_tier >= target_tier) {
            pending.push(CompilationRequest {
                fingerprint: fingerprint.to_string(),
                target_tier,
                ir: QueryIR {
                    ops: vec![],
                    inputs: vec![],
                    outputs: vec![],
                    estimated_rows: 0,
                },
                priority: count as u32,
            });
        }
    }

    /// Compile a query to a specific tier
    pub fn compile(&self, request: &CompilationRequest) -> Result<CompiledCode, CompilationError> {
        let start = Instant::now();

        // Simulate compilation based on tier
        let (code, code_size) = match request.target_tier {
            CompilationTier::Interpreted => {
                // No compilation needed
                (vec![], 0)
            }
            CompilationTier::Baseline => {
                // Quick baseline compilation
                let code = self.compile_baseline(&request.ir)?;
                let size = code.len();
                (code, size)
            }
            CompilationTier::Optimized => {
                // Optimized compilation with more passes
                let code = self.compile_optimized(&request.ir)?;
                let size = code.len();
                (code, size)
            }
            CompilationTier::Vectorized => {
                // Full vectorized compilation
                let code = self.compile_vectorized(&request.ir)?;
                let size = code.len();
                (code, size)
            }
        };

        let compilation_time = start.elapsed();

        let compiled = CompiledCode {
            fingerprint: request.fingerprint.clone(),
            tier: request.target_tier,
            code,
            code_size,
            compilation_time,
            last_used: Instant::now(),
            execution_count: 0,
            total_execution_time: Duration::ZERO,
        };

        // Add to cache
        self.add_to_cache(compiled.clone())?;

        // Update stats
        let mut stats = self.stats.write().unwrap();
        stats.total_compilations += 1;
        stats.total_compilation_time += compilation_time;
        match request.target_tier {
            CompilationTier::Baseline => stats.baseline_compilations += 1,
            CompilationTier::Optimized => stats.optimized_compilations += 1,
            CompilationTier::Vectorized => stats.vectorized_compilations += 1,
            _ => {}
        }

        Ok(compiled)
    }

    /// Baseline compilation (simple bytecode)
    fn compile_baseline(&self, ir: &QueryIR) -> Result<Vec<u8>, CompilationError> {
        let mut bytecode = Vec::new();

        for op in &ir.ops {
            match op {
                IROp::Scan { table, columns: _ } => {
                    bytecode.push(0x01); // SCAN opcode
                    bytecode.extend(table.as_bytes());
                    bytecode.push(0x00); // null terminator
                }
                IROp::Filter { predicate } => {
                    bytecode.push(0x02); // FILTER opcode
                    bytecode.extend(predicate.column.as_bytes());
                    bytecode.push(0x00);
                }
                IROp::Project { columns } => {
                    bytecode.push(0x03); // PROJECT opcode
                    bytecode.push(columns.len() as u8);
                }
                IROp::Aggregate { group_by, aggregates } => {
                    bytecode.push(0x04); // AGGREGATE opcode
                    bytecode.push(group_by.len() as u8);
                    bytecode.push(aggregates.len() as u8);
                }
                IROp::Sort { columns } => {
                    bytecode.push(0x05); // SORT opcode
                    bytecode.push(columns.len() as u8);
                }
                IROp::Limit { count, offset: _ } => {
                    bytecode.push(0x06); // LIMIT opcode
                    bytecode.extend(&count.to_le_bytes());
                }
                IROp::Join { join_type: _, left_key: _, right_key: _ } => {
                    bytecode.push(0x07); // JOIN opcode
                }
                IROp::HashBuild { key: _ } => {
                    bytecode.push(0x08); // HASH_BUILD opcode
                }
                IROp::HashProbe { key: _ } => {
                    bytecode.push(0x09); // HASH_PROBE opcode
                }
            }
        }

        bytecode.push(0xFF); // END opcode
        Ok(bytecode)
    }

    /// Optimized compilation (with optimization passes)
    fn compile_optimized(&self, ir: &QueryIR) -> Result<Vec<u8>, CompilationError> {
        // Start with baseline
        let mut code = self.compile_baseline(ir)?;

        // Add optimization header
        let mut optimized = vec![0x4F, 0x50, 0x54]; // "OPT" magic
        optimized.append(&mut code);

        Ok(optimized)
    }

    /// Vectorized compilation (SIMD-optimized)
    fn compile_vectorized(&self, ir: &QueryIR) -> Result<Vec<u8>, CompilationError> {
        // Start with optimized
        let mut code = self.compile_optimized(ir)?;

        // Add vectorization header
        let mut vectorized = vec![0x56, 0x45, 0x43]; // "VEC" magic
        vectorized.append(&mut code);

        Ok(vectorized)
    }

    /// Add compiled code to cache
    fn add_to_cache(&self, code: CompiledCode) -> Result<(), CompilationError> {
        let mut cache = self.cache.write().unwrap();

        // Check cache size
        let total_size: usize = cache.values().map(|c| c.code_size).sum();
        if total_size + code.code_size > self.config.cache_size_limit {
            // Evict least recently used
            self.evict_lru(&mut cache, code.code_size);
        }

        cache.insert(code.fingerprint.clone(), code);
        Ok(())
    }

    /// Evict least recently used entries
    fn evict_lru(&self, cache: &mut HashMap<String, CompiledCode>, needed: usize) {
        let mut entries: Vec<_> = cache.iter()
            .map(|(k, v)| (k.clone(), v.last_used, v.code_size))
            .collect();

        entries.sort_by_key(|(_, last_used, _)| *last_used);

        let mut freed = 0usize;
        let mut stats = self.stats.write().unwrap();

        for (key, _, size) in entries {
            if freed >= needed {
                break;
            }
            cache.remove(&key);
            freed += size;
            stats.cache_evictions += 1;
        }
    }

    /// Get current tier for a query
    pub fn get_tier(&self, fingerprint: &str) -> CompilationTier {
        let cache = self.cache.read().unwrap();
        cache.get(fingerprint)
            .map(|c| c.tier)
            .unwrap_or(CompilationTier::Interpreted)
    }

    /// Get pending compilations
    pub fn pending_count(&self) -> usize {
        self.pending.read().unwrap().len()
    }

    /// Process pending compilations
    pub fn process_pending(&self) -> Vec<Result<CompiledCode, CompilationError>> {
        let requests: Vec<CompilationRequest> = {
            let mut pending = self.pending.write().unwrap();
            pending.drain(..).collect()
        };

        requests.iter().map(|req| self.compile(req)).collect()
    }

    /// Get statistics
    pub fn stats(&self) -> CompilationStats {
        self.stats.read().unwrap().clone()
    }

    /// Clear cache
    pub fn clear_cache(&self) {
        self.cache.write().unwrap().clear();
        self.counters.write().unwrap().clear();
    }

    /// Get cache info
    pub fn cache_info(&self) -> CacheInfo {
        let cache = self.cache.read().unwrap();
        let total_size: usize = cache.values().map(|c| c.code_size).sum();

        CacheInfo {
            entries: cache.len(),
            total_size_bytes: total_size,
            limit_bytes: self.config.cache_size_limit,
            utilization: total_size as f64 / self.config.cache_size_limit as f64,
        }
    }
}

impl Default for TieredCompilationManager {
    fn default() -> Self {
        Self::new(TieredCompilationConfig::default())
    }
}

/// Cache information
#[derive(Debug, Clone)]
pub struct CacheInfo {
    /// Number of entries
    pub entries: usize,
    /// Total size in bytes
    pub total_size_bytes: usize,
    /// Size limit in bytes
    pub limit_bytes: usize,
    /// Utilization ratio
    pub utilization: f64,
}

/// Compilation error
#[derive(Debug, Clone)]
pub enum CompilationError {
    /// Compilation timeout
    Timeout,
    /// Invalid IR
    InvalidIR(String),
    /// Code generation failed
    CodeGenFailed(String),
    /// Cache full
    CacheFull,
}

impl std::fmt::Display for CompilationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "Compilation timeout"),
            Self::InvalidIR(s) => write!(f, "Invalid IR: {}", s),
            Self::CodeGenFailed(s) => write!(f, "Code generation failed: {}", s),
            Self::CacheFull => write!(f, "Compilation cache full"),
        }
    }
}

impl std::error::Error for CompilationError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_ordering() {
        assert!(CompilationTier::Interpreted < CompilationTier::Baseline);
        assert!(CompilationTier::Baseline < CompilationTier::Optimized);
        assert!(CompilationTier::Optimized < CompilationTier::Vectorized);
    }

    #[test]
    fn test_execution_counting() {
        let manager = TieredCompilationManager::default();

        // Record several executions
        for _ in 0..10 {
            manager.record_execution("query1", Duration::from_millis(10));
        }

        // Should have pending compilation request
        assert!(manager.pending_count() > 0);
    }

    #[test]
    fn test_baseline_compilation() {
        let manager = TieredCompilationManager::default();

        let ir = QueryIR {
            ops: vec![
                IROp::Scan { table: "users".into(), columns: vec!["id".into(), "name".into()] },
                IROp::Filter { predicate: IRPredicate {
                    column: "status".into(),
                    op: "=".into(),
                    value: IRValue::String("active".into()),
                }},
                IROp::Project { columns: vec!["id".into(), "name".into()] },
            ],
            inputs: vec![],
            outputs: vec!["id".into(), "name".into()],
            estimated_rows: 1000,
        };

        let request = CompilationRequest {
            fingerprint: "test_query".into(),
            target_tier: CompilationTier::Baseline,
            ir,
            priority: 10,
        };

        let result = manager.compile(&request).unwrap();

        assert_eq!(result.tier, CompilationTier::Baseline);
        assert!(!result.code.is_empty());
        assert_eq!(result.code.last(), Some(&0xFF)); // END opcode
    }

    #[test]
    fn test_tier_promotion() {
        let config = TieredCompilationConfig {
            baseline_threshold: 2,
            optimized_threshold: 5,
            vectorized_threshold: 10,
            ..Default::default()
        };

        let manager = TieredCompilationManager::new(config);

        // Execute enough times to trigger baseline
        for _ in 0..3 {
            manager.record_execution("query1", Duration::from_millis(10));
        }

        // Should have baseline compilation pending
        let pending = manager.pending.read().unwrap();
        assert!(!pending.is_empty());
        assert!(pending.iter().any(|p| p.target_tier == CompilationTier::Baseline));
    }

    #[test]
    fn test_cache_operations() {
        let manager = TieredCompilationManager::default();

        let ir = QueryIR {
            ops: vec![IROp::Scan { table: "test".into(), columns: vec![] }],
            inputs: vec![],
            outputs: vec![],
            estimated_rows: 100,
        };

        let request = CompilationRequest {
            fingerprint: "test_query".into(),
            target_tier: CompilationTier::Baseline,
            ir,
            priority: 1,
        };

        manager.compile(&request).unwrap();

        // Should be in cache
        let code = manager.get_code("test_query");
        assert!(code.is_some());
        assert_eq!(code.unwrap().tier, CompilationTier::Baseline);
    }

    #[test]
    fn test_cache_stats() {
        let manager = TieredCompilationManager::default();

        // Miss
        let _ = manager.get_code("nonexistent");

        let stats = manager.stats();
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hits, 0);
    }

    #[test]
    fn test_cache_info() {
        let manager = TieredCompilationManager::default();

        let info = manager.cache_info();
        assert_eq!(info.entries, 0);
        assert_eq!(info.utilization, 0.0);
    }

    #[test]
    fn test_vectorized_compilation() {
        let manager = TieredCompilationManager::default();

        let ir = QueryIR {
            ops: vec![IROp::Scan { table: "data".into(), columns: vec![] }],
            inputs: vec![],
            outputs: vec![],
            estimated_rows: 1000000,
        };

        let request = CompilationRequest {
            fingerprint: "vector_query".into(),
            target_tier: CompilationTier::Vectorized,
            ir,
            priority: 100,
        };

        let result = manager.compile(&request).unwrap();

        assert_eq!(result.tier, CompilationTier::Vectorized);
        // Check for VEC magic header
        assert_eq!(&result.code[0..3], &[0x56, 0x45, 0x43]);
    }
}
