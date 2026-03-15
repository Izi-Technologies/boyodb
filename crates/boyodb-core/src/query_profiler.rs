//! Query Profiler
//!
//! Detailed query profiling with flame graphs, memory tracking, and operator statistics.
//! Provides deep insight into query execution for performance optimization.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Query profile
#[derive(Debug, Clone)]
pub struct QueryProfile {
    /// Query ID
    pub query_id: String,
    /// SQL text
    pub sql: String,
    /// Start time
    pub start_time: Instant,
    /// End time
    pub end_time: Option<Instant>,
    /// Total duration
    pub duration: Option<Duration>,
    /// Query phases
    pub phases: Vec<ProfilePhase>,
    /// Operator stats
    pub operators: Vec<OperatorProfile>,
    /// Memory stats
    pub memory: MemoryProfile,
    /// I/O stats
    pub io: IoProfile,
    /// Row counts
    pub rows: RowProfile,
    /// Wait events
    pub wait_events: Vec<WaitEvent>,
}

impl QueryProfile {
    pub fn new(query_id: String, sql: String) -> Self {
        Self {
            query_id,
            sql,
            start_time: Instant::now(),
            end_time: None,
            duration: None,
            phases: Vec::new(),
            operators: Vec::new(),
            memory: MemoryProfile::default(),
            io: IoProfile::default(),
            rows: RowProfile::default(),
            wait_events: Vec::new(),
        }
    }

    pub fn finish(&mut self) {
        self.end_time = Some(Instant::now());
        self.duration = Some(self.start_time.elapsed());
    }

    /// Generate flame graph data
    pub fn to_flame_graph(&self) -> String {
        let mut lines = Vec::new();
        
        for op in &self.operators {
            let path = op.ancestors.join(";") + ";" + &op.name;
            let time_us = op.total_time.as_micros();
            lines.push(format!("{} {}", path, time_us));
        }
        
        lines.join("\n")
    }

    /// Generate JSON profile
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "query_id": self.query_id,
            "sql": self.sql,
            "duration_ms": self.duration.map(|d| d.as_millis() as u64),
            "phases": self.phases.iter().map(|p| {
                serde_json::json!({
                    "name": p.name,
                    "duration_ms": p.duration.as_millis() as u64,
                })
            }).collect::<Vec<_>>(),
            "operators": self.operators.iter().map(|o| {
                serde_json::json!({
                    "name": o.name,
                    "total_time_ms": o.total_time.as_millis() as u64,
                    "self_time_ms": o.self_time.as_millis() as u64,
                    "calls": o.calls,
                    "rows_in": o.rows_in,
                    "rows_out": o.rows_out,
                })
            }).collect::<Vec<_>>(),
            "memory": {
                "peak_bytes": self.memory.peak_bytes,
                "allocated_bytes": self.memory.allocated_bytes,
            },
            "io": {
                "bytes_read": self.io.bytes_read,
                "bytes_written": self.io.bytes_written,
                "segments_scanned": self.io.segments_scanned,
            },
            "rows": {
                "scanned": self.rows.scanned,
                "filtered": self.rows.filtered,
                "returned": self.rows.returned,
            },
        })
    }
}

/// Profile phase
#[derive(Debug, Clone)]
pub struct ProfilePhase {
    /// Phase name
    pub name: String,
    /// Start time
    pub start: Instant,
    /// Duration
    pub duration: Duration,
}

/// Operator profile
#[derive(Debug, Clone)]
pub struct OperatorProfile {
    /// Operator ID
    pub id: u32,
    /// Operator name
    pub name: String,
    /// Parent operator ID
    pub parent_id: Option<u32>,
    /// Ancestor names (for flame graph)
    pub ancestors: Vec<String>,
    /// Total time (including children)
    pub total_time: Duration,
    /// Self time (excluding children)
    pub self_time: Duration,
    /// Number of calls
    pub calls: u64,
    /// Rows input
    pub rows_in: u64,
    /// Rows output
    pub rows_out: u64,
    /// Memory used
    pub memory_bytes: u64,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl Default for OperatorProfile {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            parent_id: None,
            ancestors: Vec::new(),
            total_time: Duration::ZERO,
            self_time: Duration::ZERO,
            calls: 0,
            rows_in: 0,
            rows_out: 0,
            memory_bytes: 0,
            attributes: HashMap::new(),
        }
    }
}

/// Memory profile
#[derive(Debug, Clone, Default)]
pub struct MemoryProfile {
    /// Peak memory usage
    pub peak_bytes: u64,
    /// Currently allocated
    pub allocated_bytes: u64,
    /// Memory allocations
    pub allocations: u64,
    /// Memory deallocations
    pub deallocations: u64,
    /// Per-operator memory
    pub operator_memory: HashMap<u32, u64>,
}

/// I/O profile
#[derive(Debug, Clone, Default)]
pub struct IoProfile {
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Segments scanned
    pub segments_scanned: u64,
    /// Segments skipped (pruning)
    pub segments_skipped: u64,
    /// Index lookups
    pub index_lookups: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Network bytes
    pub network_bytes: u64,
}

/// Row profile
#[derive(Debug, Clone, Default)]
pub struct RowProfile {
    /// Rows scanned
    pub scanned: u64,
    /// Rows filtered out
    pub filtered: u64,
    /// Rows returned
    pub returned: u64,
    /// Rows joined
    pub joined: u64,
    /// Rows sorted
    pub sorted: u64,
    /// Rows aggregated
    pub aggregated: u64,
}

/// Wait event
#[derive(Debug, Clone)]
pub struct WaitEvent {
    /// Event type
    pub event_type: WaitEventType,
    /// Duration
    pub duration: Duration,
    /// Start time
    pub start: Instant,
    /// Context
    pub context: String,
}

/// Wait event types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitEventType {
    /// Waiting for I/O
    IO,
    /// Waiting for lock
    Lock,
    /// Waiting for CPU
    CPU,
    /// Waiting for network
    Network,
    /// Waiting for buffer
    Buffer,
    /// Waiting for latch
    Latch,
    /// Waiting for client
    Client,
}

/// Query profiler
pub struct QueryProfiler {
    /// Active profiles
    profiles: RwLock<HashMap<String, QueryProfile>>,
    /// Completed profiles
    history: RwLock<Vec<QueryProfile>>,
    /// History limit
    history_limit: usize,
    /// Enabled
    enabled: RwLock<bool>,
}

impl QueryProfiler {
    pub fn new(history_limit: usize) -> Self {
        Self {
            profiles: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            history_limit,
            enabled: RwLock::new(true),
        }
    }

    /// Enable profiling
    pub fn enable(&self) {
        *self.enabled.write().unwrap() = true;
    }

    /// Disable profiling
    pub fn disable(&self) {
        *self.enabled.write().unwrap() = false;
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read().unwrap()
    }

    /// Start profiling a query
    pub fn start(&self, query_id: String, sql: String) {
        if !self.is_enabled() {
            return;
        }

        let profile = QueryProfile::new(query_id.clone(), sql);
        self.profiles.write().unwrap().insert(query_id, profile);
    }

    /// Start a phase
    pub fn start_phase(&self, query_id: &str, phase_name: &str) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.phases.push(ProfilePhase {
                name: phase_name.into(),
                start: Instant::now(),
                duration: Duration::ZERO,
            });
        }
    }

    /// End current phase
    pub fn end_phase(&self, query_id: &str) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            if let Some(phase) = profile.phases.last_mut() {
                phase.duration = phase.start.elapsed();
            }
        }
    }

    /// Add operator profile
    pub fn add_operator(&self, query_id: &str, operator: OperatorProfile) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.operators.push(operator);
        }
    }

    /// Update memory stats
    pub fn update_memory(&self, query_id: &str, allocated: u64, peak: u64) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.memory.allocated_bytes = allocated;
            if peak > profile.memory.peak_bytes {
                profile.memory.peak_bytes = peak;
            }
        }
    }

    /// Update I/O stats
    pub fn update_io(&self, query_id: &str, bytes_read: u64, segments: u64) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.io.bytes_read += bytes_read;
            profile.io.segments_scanned += segments;
        }
    }

    /// Update row stats
    pub fn update_rows(&self, query_id: &str, scanned: u64, filtered: u64, returned: u64) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.rows.scanned += scanned;
            profile.rows.filtered += filtered;
            profile.rows.returned += returned;
        }
    }

    /// Record wait event
    pub fn record_wait(&self, query_id: &str, event_type: WaitEventType, duration: Duration, context: &str) {
        if let Some(profile) = self.profiles.write().unwrap().get_mut(query_id) {
            profile.wait_events.push(WaitEvent {
                event_type,
                duration,
                start: Instant::now(),
                context: context.into(),
            });
        }
    }

    /// Finish profiling a query
    pub fn finish(&self, query_id: &str) -> Option<QueryProfile> {
        let mut profiles = self.profiles.write().unwrap();
        let mut profile = profiles.remove(query_id)?;
        profile.finish();

        let profile_clone = profile.clone();

        // Add to history
        let mut history = self.history.write().unwrap();
        if history.len() >= self.history_limit {
            history.remove(0);
        }
        history.push(profile);

        Some(profile_clone)
    }

    /// Get active profile
    pub fn get_active(&self, query_id: &str) -> Option<QueryProfile> {
        self.profiles.read().unwrap().get(query_id).cloned()
    }

    /// Get recent profiles
    pub fn get_history(&self, limit: usize) -> Vec<QueryProfile> {
        let history = self.history.read().unwrap();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Clear history
    pub fn clear_history(&self) {
        self.history.write().unwrap().clear();
    }
}

impl Default for QueryProfiler {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Profile builder for convenient profiling
pub struct ProfileBuilder {
    query_id: String,
    profiler: Arc<QueryProfiler>,
    current_operator_id: u32,
    operator_stack: Vec<(u32, String, Instant)>,
}

impl ProfileBuilder {
    pub fn new(profiler: Arc<QueryProfiler>, query_id: String, sql: String) -> Self {
        profiler.start(query_id.clone(), sql);
        Self {
            query_id,
            profiler,
            current_operator_id: 0,
            operator_stack: Vec::new(),
        }
    }

    pub fn phase(&self, name: &str) -> PhaseGuard {
        self.profiler.start_phase(&self.query_id, name);
        PhaseGuard {
            query_id: self.query_id.clone(),
            profiler: self.profiler.clone(),
        }
    }

    pub fn start_operator(&mut self, name: &str) -> u32 {
        let id = self.current_operator_id;
        self.current_operator_id += 1;
        self.operator_stack.push((id, name.into(), Instant::now()));
        id
    }

    pub fn end_operator(&mut self, rows_in: u64, rows_out: u64) {
        if let Some((id, name, start)) = self.operator_stack.pop() {
            let ancestors: Vec<String> = self.operator_stack.iter()
                .map(|(_, n, _)| n.clone())
                .collect();
            
            let parent_id = self.operator_stack.last().map(|(id, _, _)| *id);
            let total_time = start.elapsed();

            let profile = OperatorProfile {
                id,
                name,
                parent_id,
                ancestors,
                total_time,
                self_time: total_time, // Would need child tracking for accurate self_time
                calls: 1,
                rows_in,
                rows_out,
                memory_bytes: 0,
                attributes: HashMap::new(),
            };

            self.profiler.add_operator(&self.query_id, profile);
        }
    }

    pub fn finish(self) -> Option<QueryProfile> {
        self.profiler.finish(&self.query_id)
    }
}

/// Phase guard for RAII phase tracking
pub struct PhaseGuard {
    query_id: String,
    profiler: Arc<QueryProfiler>,
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        self.profiler.end_phase(&self.query_id);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_profiler() {
        let profiler = Arc::new(QueryProfiler::new(100));
        
        profiler.start("q1".into(), "SELECT * FROM test".into());
        profiler.start_phase("q1", "parse");
        profiler.end_phase("q1");
        profiler.start_phase("q1", "execute");
        profiler.update_rows("q1", 1000, 100, 900);
        profiler.end_phase("q1");
        
        let profile = profiler.finish("q1").unwrap();
        
        assert_eq!(profile.phases.len(), 2);
        assert_eq!(profile.rows.scanned, 1000);
        assert_eq!(profile.rows.returned, 900);
    }

    #[test]
    fn test_profile_builder() {
        let profiler = Arc::new(QueryProfiler::new(100));
        let mut builder = ProfileBuilder::new(profiler.clone(), "q2".into(), "SELECT 1".into());
        
        {
            let _phase = builder.phase("parse");
        }
        
        builder.start_operator("Scan");
        builder.end_operator(1000, 500);
        
        builder.start_operator("Filter");
        builder.end_operator(500, 100);
        
        let profile = builder.finish().unwrap();
        
        assert_eq!(profile.operators.len(), 2);
        assert_eq!(profile.phases.len(), 1);
    }

    #[test]
    fn test_flame_graph() {
        let mut profile = QueryProfile::new("q3".into(), "SELECT *".into());
        
        profile.operators.push(OperatorProfile {
            id: 0,
            name: "Scan".into(),
            ancestors: vec!["Query".into()],
            total_time: Duration::from_millis(100),
            ..Default::default()
        });
        
        profile.operators.push(OperatorProfile {
            id: 1,
            name: "Filter".into(),
            ancestors: vec!["Query".into(), "Scan".into()],
            total_time: Duration::from_millis(50),
            ..Default::default()
        });
        
        let flame = profile.to_flame_graph();
        assert!(flame.contains("Query;Scan 100000"));
        assert!(flame.contains("Query;Scan;Filter 50000"));
    }

    #[test]
    fn test_history() {
        let profiler = QueryProfiler::new(5);
        
        for i in 0..10 {
            profiler.start(format!("q{}", i), "SELECT 1".into());
            profiler.finish(&format!("q{}", i));
        }
        
        let history = profiler.get_history(10);
        assert_eq!(history.len(), 5);
        assert_eq!(history[0].query_id, "q9");
    }
}
