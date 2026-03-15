//! Parallel Query Execution - Parallel Scans, Aggregates, and Gather Nodes
//!
//! This module provides parallel query execution support:
//! - Parallel sequential scans
//! - Parallel index scans
//! - Parallel aggregates
//! - Gather and GatherMerge nodes
//! - Work stealing for load balancing

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

// ============================================================================
// PARALLEL EXECUTION CONFIGURATION
// ============================================================================

/// Parallel execution configuration
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum parallel workers per query
    pub max_parallel_workers_per_gather: usize,
    /// Minimum table size for parallel scan (bytes)
    pub min_parallel_table_scan_size: u64,
    /// Parallel tuple cost factor
    pub parallel_tuple_cost: f64,
    /// Parallel setup cost
    pub parallel_setup_cost: f64,
    /// Enable parallel hash joins
    pub enable_parallel_hash: bool,
    /// Enable parallel append
    pub enable_parallel_append: bool,
    /// Force parallel mode for testing
    pub force_parallel_mode: ParallelForceMode,
    /// Parallel leader participation
    pub parallel_leader_participation: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_parallel_workers_per_gather: 4,
            min_parallel_table_scan_size: 8 * 1024 * 1024, // 8MB
            parallel_tuple_cost: 0.1,
            parallel_setup_cost: 1000.0,
            enable_parallel_hash: true,
            enable_parallel_append: true,
            force_parallel_mode: ParallelForceMode::Off,
            parallel_leader_participation: true,
        }
    }
}

/// Force parallel mode setting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelForceMode {
    /// Don't force parallel execution
    Off,
    /// Force parallel execution when safe
    On,
    /// Force parallel execution for testing (ignores safety)
    Regress,
}

// ============================================================================
// WORK DISTRIBUTION
// ============================================================================

/// Work unit for parallel execution
#[derive(Debug, Clone)]
pub struct WorkUnit {
    /// Unit ID
    pub id: u64,
    /// Start offset (e.g., block number)
    pub start: u64,
    /// End offset
    pub end: u64,
    /// Estimated rows
    pub estimated_rows: u64,
}

impl WorkUnit {
    /// Create a new work unit
    pub fn new(id: u64, start: u64, end: u64) -> Self {
        Self {
            id,
            start,
            end,
            estimated_rows: end - start,
        }
    }

    /// Size of work unit
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

/// Work queue for distributing work to parallel workers
pub struct WorkQueue {
    /// Pending work units
    queue: Mutex<VecDeque<WorkUnit>>,
    /// Condition variable for waiting workers
    condvar: Condvar,
    /// Queue is closed
    closed: AtomicBool,
    /// Total work units
    total_units: AtomicU64,
    /// Completed work units
    completed_units: AtomicU64,
}

impl WorkQueue {
    /// Create a new work queue
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            closed: AtomicBool::new(false),
            total_units: AtomicU64::new(0),
            completed_units: AtomicU64::new(0),
        }
    }

    /// Add work to the queue
    pub fn push(&self, unit: WorkUnit) {
        let mut queue = self.queue.lock();
        queue.push_back(unit);
        self.total_units.fetch_add(1, Ordering::SeqCst);
        self.condvar.notify_one();
    }

    /// Add multiple work units
    pub fn push_all(&self, units: Vec<WorkUnit>) {
        let mut queue = self.queue.lock();
        let count = units.len() as u64;
        for unit in units {
            queue.push_back(unit);
        }
        self.total_units.fetch_add(count, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    /// Get next work unit (blocking)
    pub fn pop(&self) -> Option<WorkUnit> {
        let mut queue = self.queue.lock();
        loop {
            if let Some(unit) = queue.pop_front() {
                return Some(unit);
            }
            if self.closed.load(Ordering::SeqCst) {
                return None;
            }
            self.condvar.wait(&mut queue);
        }
    }

    /// Try to get next work unit (non-blocking)
    pub fn try_pop(&self) -> Option<WorkUnit> {
        let mut queue = self.queue.lock();
        queue.pop_front()
    }

    /// Steal work from end of queue (for work stealing)
    pub fn steal(&self) -> Option<WorkUnit> {
        let mut queue = self.queue.lock();
        queue.pop_back()
    }

    /// Mark a work unit as completed
    pub fn complete(&self) {
        self.completed_units.fetch_add(1, Ordering::SeqCst);
    }

    /// Close the queue
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        let queue = self.queue.lock();
        queue.is_empty()
    }

    /// Get progress
    pub fn progress(&self) -> (u64, u64) {
        (
            self.completed_units.load(Ordering::SeqCst),
            self.total_units.load(Ordering::SeqCst),
        )
    }
}

impl Default for WorkQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PARALLEL SCAN
// ============================================================================

/// Parallel scan state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanState {
    /// Not started
    NotStarted,
    /// Scanning in progress
    Scanning,
    /// Scan complete
    Complete,
    /// Scan failed
    Failed,
}

/// Block range for parallel scan
#[derive(Debug, Clone)]
pub struct BlockRange {
    /// First block
    pub start_block: u64,
    /// Last block (exclusive)
    pub end_block: u64,
}

/// Parallel scan coordinator
pub struct ParallelScanState {
    /// Current block being scanned
    next_block: AtomicU64,
    /// Total blocks
    total_blocks: u64,
    /// Block allocation size
    block_stride: u64,
    /// Scan state
    state: RwLock<ScanState>,
    /// Workers attached
    workers_attached: AtomicUsize,
    /// Workers finished
    workers_finished: AtomicUsize,
}

impl ParallelScanState {
    /// Create a new parallel scan state
    pub fn new(total_blocks: u64, block_stride: u64) -> Self {
        Self {
            next_block: AtomicU64::new(0),
            total_blocks,
            block_stride,
            state: RwLock::new(ScanState::NotStarted),
            workers_attached: AtomicUsize::new(0),
            workers_finished: AtomicUsize::new(0),
        }
    }

    /// Attach a worker
    pub fn attach_worker(&self) -> usize {
        let id = self.workers_attached.fetch_add(1, Ordering::SeqCst);
        let mut state = self.state.write();
        if *state == ScanState::NotStarted {
            *state = ScanState::Scanning;
        }
        id
    }

    /// Get next block range for scanning
    pub fn get_next_range(&self) -> Option<BlockRange> {
        loop {
            let current = self.next_block.load(Ordering::SeqCst);
            if current >= self.total_blocks {
                return None;
            }

            let end = (current + self.block_stride).min(self.total_blocks);
            if self
                .next_block
                .compare_exchange(current, end, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(BlockRange {
                    start_block: current,
                    end_block: end,
                });
            }
        }
    }

    /// Mark worker as finished
    pub fn finish_worker(&self) {
        let finished = self.workers_finished.fetch_add(1, Ordering::SeqCst) + 1;
        let attached = self.workers_attached.load(Ordering::SeqCst);

        if finished >= attached {
            let mut state = self.state.write();
            *state = ScanState::Complete;
        }
    }

    /// Get scan state
    pub fn state(&self) -> ScanState {
        *self.state.read()
    }

    /// Get progress
    pub fn progress(&self) -> f64 {
        let scanned = self.next_block.load(Ordering::SeqCst);
        if self.total_blocks == 0 {
            100.0
        } else {
            (scanned as f64 / self.total_blocks as f64) * 100.0
        }
    }
}

// ============================================================================
// PARALLEL AGGREGATE
// ============================================================================

/// Aggregate function for parallel execution
#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    ArrayAgg,
    StringAgg,
    BitAnd,
    BitOr,
}

impl AggregateFunc {
    /// Check if aggregate can be parallelized
    pub fn is_parallel_safe(&self) -> bool {
        matches!(
            self,
            AggregateFunc::Count
                | AggregateFunc::Sum
                | AggregateFunc::Min
                | AggregateFunc::Max
                | AggregateFunc::BitAnd
                | AggregateFunc::BitOr
        )
    }

    /// Check if aggregate needs finalization
    pub fn needs_finalize(&self) -> bool {
        matches!(self, AggregateFunc::Avg)
    }
}

/// Partial aggregate value
#[derive(Debug, Clone)]
pub enum PartialAggValue {
    Count(i64),
    Sum(f64),
    SumCount(f64, i64), // For AVG
    Min(f64),
    Max(f64),
    BitAnd(i64),
    BitOr(i64),
    Array(Vec<String>),
}

impl PartialAggValue {
    /// Combine two partial values
    pub fn combine(&self, other: &PartialAggValue) -> Option<PartialAggValue> {
        match (self, other) {
            (PartialAggValue::Count(a), PartialAggValue::Count(b)) => {
                Some(PartialAggValue::Count(a + b))
            }
            (PartialAggValue::Sum(a), PartialAggValue::Sum(b)) => {
                Some(PartialAggValue::Sum(a + b))
            }
            (PartialAggValue::SumCount(s1, c1), PartialAggValue::SumCount(s2, c2)) => {
                Some(PartialAggValue::SumCount(s1 + s2, c1 + c2))
            }
            (PartialAggValue::Min(a), PartialAggValue::Min(b)) => {
                Some(PartialAggValue::Min(a.min(*b)))
            }
            (PartialAggValue::Max(a), PartialAggValue::Max(b)) => {
                Some(PartialAggValue::Max(a.max(*b)))
            }
            (PartialAggValue::BitAnd(a), PartialAggValue::BitAnd(b)) => {
                Some(PartialAggValue::BitAnd(a & b))
            }
            (PartialAggValue::BitOr(a), PartialAggValue::BitOr(b)) => {
                Some(PartialAggValue::BitOr(a | b))
            }
            (PartialAggValue::Array(a), PartialAggValue::Array(b)) => {
                let mut combined = a.clone();
                combined.extend(b.iter().cloned());
                Some(PartialAggValue::Array(combined))
            }
            _ => None,
        }
    }

    /// Finalize the aggregate value
    pub fn finalize(&self) -> f64 {
        match self {
            PartialAggValue::Count(n) => *n as f64,
            PartialAggValue::Sum(s) => *s,
            PartialAggValue::SumCount(s, c) => {
                if *c == 0 {
                    0.0
                } else {
                    s / (*c as f64)
                }
            }
            PartialAggValue::Min(v) => *v,
            PartialAggValue::Max(v) => *v,
            PartialAggValue::BitAnd(v) => *v as f64,
            PartialAggValue::BitOr(v) => *v as f64,
            PartialAggValue::Array(v) => v.len() as f64,
        }
    }
}

/// Parallel aggregate state
pub struct ParallelAggregateState {
    /// Partial results from each worker
    partials: RwLock<Vec<PartialAggValue>>,
    /// Final result (after combination)
    final_result: RwLock<Option<f64>>,
    /// Number of workers
    num_workers: usize,
    /// Workers completed
    completed: AtomicUsize,
}

impl ParallelAggregateState {
    /// Create new parallel aggregate state
    pub fn new(num_workers: usize) -> Self {
        Self {
            partials: RwLock::new(Vec::with_capacity(num_workers)),
            final_result: RwLock::new(None),
            num_workers,
            completed: AtomicUsize::new(0),
        }
    }

    /// Submit partial result from a worker
    pub fn submit_partial(&self, partial: PartialAggValue) {
        let mut partials = self.partials.write();
        partials.push(partial);
        self.completed.fetch_add(1, Ordering::SeqCst);
    }

    /// Check if all workers have completed
    pub fn is_complete(&self) -> bool {
        self.completed.load(Ordering::SeqCst) >= self.num_workers
    }

    /// Combine all partial results
    pub fn combine(&self) -> Option<f64> {
        let partials = self.partials.read();
        if partials.is_empty() {
            return None;
        }

        let mut result = partials[0].clone();
        for partial in partials.iter().skip(1) {
            result = result.combine(partial)?;
        }

        let final_val = result.finalize();
        *self.final_result.write() = Some(final_val);
        Some(final_val)
    }

    /// Get the final result
    pub fn get_result(&self) -> Option<f64> {
        *self.final_result.read()
    }
}

// ============================================================================
// GATHER NODE
// ============================================================================

/// Gather node type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatherType {
    /// Regular gather (unordered)
    Gather,
    /// Gather merge (preserves sort order)
    GatherMerge,
}

/// Result tuple from a worker
#[derive(Debug, Clone)]
pub struct ResultTuple {
    /// Worker ID
    pub worker_id: usize,
    /// Tuple data
    pub data: Vec<Option<String>>,
    /// Sort key (for GatherMerge)
    pub sort_key: Option<Vec<u8>>,
}

/// Gather node for collecting results from parallel workers
pub struct GatherNode {
    /// Gather type
    gather_type: GatherType,
    /// Number of workers
    num_workers: usize,
    /// Result queues from workers
    result_queues: Vec<Mutex<VecDeque<ResultTuple>>>,
    /// Worker completion status
    worker_complete: Vec<AtomicBool>,
    /// Condition variable for result availability
    result_condvar: Condvar,
    /// Result lock for condvar
    result_lock: Mutex<()>,
    /// Total tuples received
    tuples_received: AtomicU64,
}

impl GatherNode {
    /// Create a new gather node
    pub fn new(gather_type: GatherType, num_workers: usize) -> Self {
        let mut result_queues = Vec::with_capacity(num_workers);
        let mut worker_complete = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            result_queues.push(Mutex::new(VecDeque::new()));
            worker_complete.push(AtomicBool::new(false));
        }

        Self {
            gather_type,
            num_workers,
            result_queues,
            worker_complete,
            result_condvar: Condvar::new(),
            result_lock: Mutex::new(()),
            tuples_received: AtomicU64::new(0),
        }
    }

    /// Push a result tuple from a worker
    pub fn push_result(&self, worker_id: usize, tuple: ResultTuple) {
        if worker_id < self.num_workers {
            let mut queue = self.result_queues[worker_id].lock();
            queue.push_back(tuple);
            self.tuples_received.fetch_add(1, Ordering::Relaxed);
            self.result_condvar.notify_one();
        }
    }

    /// Mark worker as complete
    pub fn complete_worker(&self, worker_id: usize) {
        if worker_id < self.num_workers {
            self.worker_complete[worker_id].store(true, Ordering::SeqCst);
            self.result_condvar.notify_one();
        }
    }

    /// Get next result tuple
    pub fn next(&self) -> Option<ResultTuple> {
        match self.gather_type {
            GatherType::Gather => self.next_unordered(),
            GatherType::GatherMerge => self.next_ordered(),
        }
    }

    fn next_unordered(&self) -> Option<ResultTuple> {
        // Round-robin through workers
        for (i, queue) in self.result_queues.iter().enumerate() {
            let mut q = queue.lock();
            if let Some(tuple) = q.pop_front() {
                return Some(tuple);
            }
            if !self.worker_complete[i].load(Ordering::SeqCst) {
                // Worker still running, might produce more
                continue;
            }
        }

        // Check if all workers are complete
        if self.all_complete() {
            None
        } else {
            // Wait for more results
            let mut lock = self.result_lock.lock();
            self.result_condvar.wait(&mut lock);
            self.next_unordered()
        }
    }

    fn next_ordered(&self) -> Option<ResultTuple> {
        // Find the tuple with the smallest sort key
        let mut best_worker: Option<usize> = None;
        let mut best_key: Option<Vec<u8>> = None;

        for (i, queue) in self.result_queues.iter().enumerate() {
            let q = queue.lock();
            if let Some(tuple) = q.front() {
                let key = tuple.sort_key.clone();
                if best_key.is_none() || key < best_key {
                    best_key = key;
                    best_worker = Some(i);
                }
            }
        }

        if let Some(worker_id) = best_worker {
            let mut q = self.result_queues[worker_id].lock();
            return q.pop_front();
        }

        if self.all_complete() {
            None
        } else {
            // Wait and retry
            let mut lock = self.result_lock.lock();
            self.result_condvar.wait(&mut lock);
            self.next_ordered()
        }
    }

    /// Check if all workers are complete
    pub fn all_complete(&self) -> bool {
        self.worker_complete
            .iter()
            .all(|c| c.load(Ordering::SeqCst))
    }

    /// Get statistics
    pub fn stats(&self) -> GatherStats {
        let queued: usize = self.result_queues
            .iter()
            .map(|q| q.lock().len())
            .sum();

        GatherStats {
            gather_type: self.gather_type,
            num_workers: self.num_workers,
            workers_complete: self.worker_complete
                .iter()
                .filter(|c| c.load(Ordering::SeqCst))
                .count(),
            tuples_received: self.tuples_received.load(Ordering::Relaxed),
            tuples_queued: queued,
        }
    }
}

/// Gather node statistics
#[derive(Debug, Clone)]
pub struct GatherStats {
    pub gather_type: GatherType,
    pub num_workers: usize,
    pub workers_complete: usize,
    pub tuples_received: u64,
    pub tuples_queued: usize,
}

// ============================================================================
// PARALLEL WORKER
// ============================================================================

/// Worker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    Running,
    Finished,
    Error,
}

/// Parallel worker
pub struct ParallelWorker {
    /// Worker ID
    pub id: usize,
    /// Worker state
    state: AtomicUsize,
    /// Tuples processed
    tuples_processed: AtomicU64,
    /// Start time
    start_time: RwLock<Option<Instant>>,
    /// End time
    end_time: RwLock<Option<Instant>>,
}

impl ParallelWorker {
    /// Create a new worker
    pub fn new(id: usize) -> Self {
        Self {
            id,
            state: AtomicUsize::new(WorkerState::Idle as usize),
            tuples_processed: AtomicU64::new(0),
            start_time: RwLock::new(None),
            end_time: RwLock::new(None),
        }
    }

    /// Start the worker
    pub fn start(&self) {
        self.state.store(WorkerState::Running as usize, Ordering::SeqCst);
        *self.start_time.write() = Some(Instant::now());
    }

    /// Finish the worker
    pub fn finish(&self) {
        self.state.store(WorkerState::Finished as usize, Ordering::SeqCst);
        *self.end_time.write() = Some(Instant::now());
    }

    /// Mark worker as error
    pub fn error(&self) {
        self.state.store(WorkerState::Error as usize, Ordering::SeqCst);
        *self.end_time.write() = Some(Instant::now());
    }

    /// Record tuples processed
    pub fn add_tuples(&self, count: u64) {
        self.tuples_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Get state
    pub fn state(&self) -> WorkerState {
        match self.state.load(Ordering::SeqCst) {
            0 => WorkerState::Idle,
            1 => WorkerState::Running,
            2 => WorkerState::Finished,
            _ => WorkerState::Error,
        }
    }

    /// Get execution time
    pub fn execution_time(&self) -> Option<Duration> {
        let start = *self.start_time.read();
        let end = *self.end_time.read();

        match (start, end) {
            (Some(s), Some(e)) => Some(e.duration_since(s)),
            (Some(s), None) => Some(s.elapsed()),
            _ => None,
        }
    }

    /// Get tuples processed
    pub fn tuples_processed(&self) -> u64 {
        self.tuples_processed.load(Ordering::Relaxed)
    }
}

// ============================================================================
// PARALLEL EXECUTOR
// ============================================================================

/// Parallel query executor
pub struct ParallelExecutor {
    /// Configuration
    config: ParallelConfig,
    /// Active workers
    workers: RwLock<Vec<Arc<ParallelWorker>>>,
    /// Queries executed
    queries_executed: AtomicU64,
    /// Total worker time
    total_worker_time: AtomicU64,
}

impl ParallelExecutor {
    /// Create a new parallel executor
    pub fn new(config: ParallelConfig) -> Self {
        Self {
            config,
            workers: RwLock::new(Vec::new()),
            queries_executed: AtomicU64::new(0),
            total_worker_time: AtomicU64::new(0),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &ParallelConfig {
        &self.config
    }

    /// Decide if a query should use parallel execution
    pub fn should_parallelize(&self, table_size: u64, estimated_rows: u64) -> bool {
        if self.config.force_parallel_mode == ParallelForceMode::On {
            return true;
        }

        // Check minimum table size
        if table_size < self.config.min_parallel_table_scan_size {
            return false;
        }

        // Estimate parallel benefit
        let serial_cost = estimated_rows as f64;
        let parallel_cost = self.config.parallel_setup_cost
            + (estimated_rows as f64 * self.config.parallel_tuple_cost);

        parallel_cost < serial_cost
    }

    /// Calculate optimal number of workers
    pub fn optimal_workers(&self, table_size: u64) -> usize {
        let mb = table_size / (1024 * 1024);
        let workers = (mb / 32).max(1) as usize; // 1 worker per 32MB
        workers.min(self.config.max_parallel_workers_per_gather)
    }

    /// Create work units for a scan
    pub fn create_work_units(&self, total_blocks: u64, num_workers: usize) -> Vec<WorkUnit> {
        let blocks_per_worker = (total_blocks / num_workers as u64).max(1);
        let mut units = Vec::new();
        let mut current = 0u64;
        let mut id = 0u64;

        while current < total_blocks {
            let end = (current + blocks_per_worker).min(total_blocks);
            units.push(WorkUnit::new(id, current, end));
            current = end;
            id += 1;
        }

        units
    }

    /// Execute a parallel scan
    pub fn execute_parallel_scan<F, T>(
        &self,
        num_workers: usize,
        work_queue: Arc<WorkQueue>,
        scan_fn: F,
    ) -> Vec<T>
    where
        F: Fn(WorkUnit) -> Vec<T> + Send + Sync + 'static,
        T: Send + 'static,
    {
        let scan_fn = Arc::new(scan_fn);
        let results: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Spawn workers
        for worker_id in 0..num_workers {
            let queue = Arc::clone(&work_queue);
            let results = Arc::clone(&results);
            let scan_fn = Arc::clone(&scan_fn);
            let worker = Arc::new(ParallelWorker::new(worker_id));

            {
                let mut workers = self.workers.write();
                workers.push(Arc::clone(&worker));
            }

            let handle = thread::spawn(move || {
                worker.start();
                while let Some(unit) = queue.pop() {
                    let partial = scan_fn(unit);
                    worker.add_tuples(partial.len() as u64);
                    let mut res = results.lock();
                    res.extend(partial);
                    queue.complete();
                }
                worker.finish();
            });

            handles.push(handle);
        }

        // Wait for all workers
        for handle in handles {
            let _ = handle.join();
        }

        self.queries_executed.fetch_add(1, Ordering::Relaxed);

        Arc::try_unwrap(results)
            .unwrap_or_else(|_| {
                // If we can't unwrap (shouldn't happen after join), return empty
                Mutex::new(Vec::new())
            })
            .into_inner()
    }

    /// Get executor statistics
    pub fn stats(&self) -> ParallelExecutorStats {
        let workers = self.workers.read();
        ParallelExecutorStats {
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            total_worker_time_ms: self.total_worker_time.load(Ordering::Relaxed),
            active_workers: workers
                .iter()
                .filter(|w| w.state() == WorkerState::Running)
                .count(),
            max_workers: self.config.max_parallel_workers_per_gather,
        }
    }
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self::new(ParallelConfig::default())
    }
}

/// Parallel executor statistics
#[derive(Debug, Clone)]
pub struct ParallelExecutorStats {
    pub queries_executed: u64,
    pub total_worker_time_ms: u64,
    pub active_workers: usize,
    pub max_workers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_default() {
        let config = ParallelConfig::default();
        assert_eq!(config.max_parallel_workers_per_gather, 4);
        assert!(config.enable_parallel_hash);
    }

    #[test]
    fn test_work_unit() {
        let unit = WorkUnit::new(1, 100, 200);
        assert_eq!(unit.size(), 100);
    }

    #[test]
    fn test_work_queue() {
        let queue = WorkQueue::new();
        queue.push(WorkUnit::new(1, 0, 10));
        queue.push(WorkUnit::new(2, 10, 20));

        let unit = queue.try_pop().unwrap();
        assert_eq!(unit.id, 1);

        let unit = queue.try_pop().unwrap();
        assert_eq!(unit.id, 2);

        assert!(queue.try_pop().is_none());
    }

    #[test]
    fn test_work_queue_steal() {
        let queue = WorkQueue::new();
        queue.push(WorkUnit::new(1, 0, 10));
        queue.push(WorkUnit::new(2, 10, 20));
        queue.push(WorkUnit::new(3, 20, 30));

        // Steal from back
        let unit = queue.steal().unwrap();
        assert_eq!(unit.id, 3);

        // Pop from front
        let unit = queue.try_pop().unwrap();
        assert_eq!(unit.id, 1);
    }

    #[test]
    fn test_parallel_scan_state() {
        let state = ParallelScanState::new(100, 10);

        let id = state.attach_worker();
        assert_eq!(id, 0);
        assert_eq!(state.state(), ScanState::Scanning);

        let range = state.get_next_range().unwrap();
        assert_eq!(range.start_block, 0);
        assert_eq!(range.end_block, 10);

        let range = state.get_next_range().unwrap();
        assert_eq!(range.start_block, 10);
        assert_eq!(range.end_block, 20);
    }

    #[test]
    fn test_parallel_scan_completion() {
        let state = ParallelScanState::new(20, 10);
        state.attach_worker();

        // Exhaust all blocks
        while state.get_next_range().is_some() {}

        state.finish_worker();
        assert_eq!(state.state(), ScanState::Complete);
    }

    #[test]
    fn test_partial_agg_combine() {
        let a = PartialAggValue::Count(10);
        let b = PartialAggValue::Count(20);
        let combined = a.combine(&b).unwrap();

        if let PartialAggValue::Count(n) = combined {
            assert_eq!(n, 30);
        } else {
            panic!("Expected Count");
        }
    }

    #[test]
    fn test_partial_agg_avg() {
        let a = PartialAggValue::SumCount(100.0, 10);
        let b = PartialAggValue::SumCount(200.0, 20);
        let combined = a.combine(&b).unwrap();

        assert_eq!(combined.finalize(), 10.0); // 300/30 = 10
    }

    #[test]
    fn test_parallel_aggregate_state() {
        let state = ParallelAggregateState::new(3);

        state.submit_partial(PartialAggValue::Sum(10.0));
        state.submit_partial(PartialAggValue::Sum(20.0));
        state.submit_partial(PartialAggValue::Sum(30.0));

        assert!(state.is_complete());

        let result = state.combine().unwrap();
        assert_eq!(result, 60.0);
    }

    #[test]
    fn test_gather_node() {
        let gather = GatherNode::new(GatherType::Gather, 2);

        gather.push_result(0, ResultTuple {
            worker_id: 0,
            data: vec![Some("row1".to_string())],
            sort_key: None,
        });

        gather.push_result(1, ResultTuple {
            worker_id: 1,
            data: vec![Some("row2".to_string())],
            sort_key: None,
        });

        gather.complete_worker(0);
        gather.complete_worker(1);

        assert!(gather.all_complete());
    }

    #[test]
    fn test_parallel_worker() {
        let worker = ParallelWorker::new(0);
        assert_eq!(worker.state(), WorkerState::Idle);

        worker.start();
        assert_eq!(worker.state(), WorkerState::Running);

        worker.add_tuples(100);
        assert_eq!(worker.tuples_processed(), 100);

        worker.finish();
        assert_eq!(worker.state(), WorkerState::Finished);
        assert!(worker.execution_time().is_some());
    }

    #[test]
    fn test_parallel_executor_should_parallelize() {
        let executor = ParallelExecutor::default();

        // Too small
        assert!(!executor.should_parallelize(1000, 100));

        // Large enough
        assert!(executor.should_parallelize(100 * 1024 * 1024, 1_000_000));
    }

    #[test]
    fn test_parallel_executor_optimal_workers() {
        let executor = ParallelExecutor::default();

        assert_eq!(executor.optimal_workers(32 * 1024 * 1024), 1);
        assert_eq!(executor.optimal_workers(128 * 1024 * 1024), 4);
        assert_eq!(executor.optimal_workers(1024 * 1024 * 1024), 4); // Capped at max
    }

    #[test]
    fn test_create_work_units() {
        let executor = ParallelExecutor::default();
        let units = executor.create_work_units(100, 4);

        assert_eq!(units.len(), 4);
        assert_eq!(units[0].start, 0);
        assert_eq!(units[0].end, 25);
        assert_eq!(units[3].start, 75);
        assert_eq!(units[3].end, 100);
    }

    #[test]
    fn test_aggregate_func_parallel_safe() {
        assert!(AggregateFunc::Count.is_parallel_safe());
        assert!(AggregateFunc::Sum.is_parallel_safe());
        assert!(AggregateFunc::Min.is_parallel_safe());
        assert!(!AggregateFunc::ArrayAgg.is_parallel_safe());
    }

    #[test]
    fn test_gather_stats() {
        let gather = GatherNode::new(GatherType::GatherMerge, 4);
        let stats = gather.stats();

        assert_eq!(stats.gather_type, GatherType::GatherMerge);
        assert_eq!(stats.num_workers, 4);
        assert_eq!(stats.workers_complete, 0);
    }
}
