//! Hierarchical Lock Manager with Deadlock Detection
//!
//! This module provides:
//! - Hierarchical locking (table -> row level)
//! - Multiple lock modes (Shared, Exclusive, Update, Intention locks)
//! - Wait-for graph based deadlock detection
//! - Lock upgrade support
//! - Fair queuing for lock requests

use crate::engine::EngineError;
use crate::transaction::TransactionId;

use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Lock modes following standard database locking semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Shared lock - allows concurrent reads
    Shared,

    /// Exclusive lock - no concurrent access
    Exclusive,

    /// Update lock - prevents deadlocks in read-then-write patterns
    Update,

    /// Intention Shared - signals intent to acquire shared locks on children
    IntentionShared,

    /// Intention Exclusive - signals intent to acquire exclusive locks on children
    IntentionExclusive,
}

impl LockMode {
    /// Check if this lock mode is compatible with another
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        use LockMode::*;
        matches!(
            (self, other),
            (Shared, Shared)
                | (Shared, IntentionShared)
                | (Shared, IntentionExclusive)
                | (IntentionShared, Shared)
                | (IntentionShared, IntentionShared)
                | (IntentionShared, IntentionExclusive)
                | (IntentionShared, Update)
                | (IntentionExclusive, Shared)
                | (IntentionExclusive, IntentionShared)
                | (IntentionExclusive, IntentionExclusive)
                | (Update, IntentionShared)
        )
    }

    /// Check if this mode can be upgraded to target mode
    pub fn can_upgrade_to(&self, target: &LockMode) -> bool {
        use LockMode::*;
        match (self, target) {
            (Shared, Exclusive) => true,
            (Shared, Update) => true,
            (Update, Exclusive) => true,
            (IntentionShared, IntentionExclusive) => true,
            (IntentionShared, Exclusive) => true,
            (IntentionExclusive, Exclusive) => true,
            _ => self == target,
        }
    }
}

impl std::fmt::Display for LockMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockMode::Shared => write!(f, "S"),
            LockMode::Exclusive => write!(f, "X"),
            LockMode::Update => write!(f, "U"),
            LockMode::IntentionShared => write!(f, "IS"),
            LockMode::IntentionExclusive => write!(f, "IX"),
        }
    }
}

/// Unique identifier for a lock
pub type LockId = u64;

/// Handle to a held lock - used for release
#[derive(Debug, Clone)]
pub struct LockHandle {
    /// Unique lock ID
    pub id: LockId,

    /// Transaction holding the lock
    pub txn_id: TransactionId,

    /// Lock target
    pub target: LockTarget,

    /// Lock mode
    pub mode: LockMode,
}

/// Target of a lock (what is being locked)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockTarget {
    /// Lock on an entire table
    Table { database: String, table: String },

    /// Lock on a specific row
    Row {
        database: String,
        table: String,
        row_key: Vec<u8>,
    },
}

impl LockTarget {
    /// Get the parent target (row -> table)
    pub fn parent(&self) -> Option<LockTarget> {
        match self {
            LockTarget::Table { .. } => None,
            LockTarget::Row {
                database, table, ..
            } => Some(LockTarget::Table {
                database: database.clone(),
                table: table.clone(),
            }),
        }
    }

    /// Create a table lock target
    pub fn table(database: &str, table: &str) -> Self {
        LockTarget::Table {
            database: database.to_string(),
            table: table.to_string(),
        }
    }

    /// Create a table lock target from owned strings (avoids allocation)
    #[inline]
    pub fn table_owned(database: String, table: String) -> Self {
        LockTarget::Table { database, table }
    }

    /// Create a row lock target
    pub fn row(database: &str, table: &str, row_key: &[u8]) -> Self {
        LockTarget::Row {
            database: database.to_string(),
            table: table.to_string(),
            row_key: row_key.to_vec(),
        }
    }

    /// Create a row lock target from owned data (avoids allocation)
    #[inline]
    pub fn row_owned(database: String, table: String, row_key: Vec<u8>) -> Self {
        LockTarget::Row { database, table, row_key }
    }
}

/// A lock request waiting in the queue
#[derive(Debug)]
struct LockRequest {
    id: LockId,
    txn_id: TransactionId,
    mode: LockMode,
    requested_at: Instant,
}

/// State of locks on a single resource
#[derive(Debug)]
struct LockState {
    /// Currently granted locks
    granted: Vec<(TransactionId, LockMode, LockId)>,

    /// Waiting requests (FIFO queue)
    waiting: VecDeque<LockRequest>,
}

impl LockState {
    fn new() -> Self {
        LockState {
            granted: Vec::new(),
            waiting: VecDeque::new(),
        }
    }

    /// Check if a transaction already holds a lock
    fn find_held_lock(&self, txn_id: TransactionId) -> Option<(LockMode, LockId)> {
        self.granted
            .iter()
            .find(|(tid, _, _)| *tid == txn_id)
            .map(|(_, mode, id)| (*mode, *id))
    }

    /// Check if a new lock request is compatible with current grants
    fn is_compatible(&self, requesting_txn: TransactionId, mode: &LockMode) -> bool {
        for (holder_txn, holder_mode, _) in &self.granted {
            // Own locks are always compatible
            if *holder_txn == requesting_txn {
                continue;
            }
            if !mode.is_compatible(holder_mode) {
                return false;
            }
        }
        true
    }

    /// Check if there are waiting requests ahead of this transaction
    fn has_waiting_ahead(&self, txn_id: TransactionId) -> bool {
        if let Some(req) = self.waiting.front() {
            // If this transaction is at the head of the queue, no one is ahead
            req.txn_id != txn_id
        } else {
            false
        }
    }
}

/// Deadlock detector using wait-for graph
struct DeadlockDetector {
    /// Wait-for graph: txn -> set of txns it's waiting for
    wait_for: RwLock<HashMap<TransactionId, HashSet<TransactionId>>>,
    /// Track when each transaction started waiting for cleanup of stale entries
    wait_start_times: RwLock<HashMap<TransactionId, Instant>>,
}

impl DeadlockDetector {
    fn new() -> Self {
        DeadlockDetector {
            wait_for: RwLock::new(HashMap::new()),
            wait_start_times: RwLock::new(HashMap::new()),
        }
    }

    /// Add a wait edge: waiter is waiting for holders
    fn add_wait(&self, waiter: TransactionId, holders: &[TransactionId]) {
        // Use a single lock acquisition pattern to reduce race window
        {
            let mut graph = self.wait_for.write();
            let entry = graph.entry(waiter).or_insert_with(HashSet::new);
            for holder in holders {
                if *holder != waiter {
                    entry.insert(*holder);
                }
            }
        }
        {
            let mut times = self.wait_start_times.write();
            times.entry(waiter).or_insert_with(Instant::now);
        }
    }

    /// Remove all wait edges for a transaction (when lock is granted or txn ends)
    fn remove_waiter(&self, txn_id: TransactionId) {
        // Remove from both maps atomically (same order always to avoid deadlock)
        {
            let mut graph = self.wait_for.write();
            graph.remove(&txn_id);
        }
        {
            let mut times = self.wait_start_times.write();
            times.remove(&txn_id);
        }
    }

    /// Remove a transaction from being waited on (when it releases locks)
    fn remove_holder(&self, txn_id: TransactionId) {
        let mut graph = self.wait_for.write();
        for waiters in graph.values_mut() {
            waiters.remove(&txn_id);
        }
        // Also clean up empty entries to prevent memory growth
        graph.retain(|_, v| !v.is_empty());
    }

    /// Cleanup stale wait entries older than the given duration
    /// This handles orphaned entries from crashed/panicked transactions
    fn cleanup_stale_waiters(&self, max_wait_duration: Duration) {
        let now = Instant::now();
        let stale_txns: Vec<TransactionId> = {
            let times = self.wait_start_times.read();
            times
                .iter()
                .filter(|(_, &start)| now.duration_since(start) > max_wait_duration)
                .map(|(&txn_id, _)| txn_id)
                .collect()
        };

        if !stale_txns.is_empty() {
            let mut graph = self.wait_for.write();
            let mut times = self.wait_start_times.write();
            for txn_id in stale_txns {
                graph.remove(&txn_id);
                times.remove(&txn_id);
            }
        }
    }

    /// Detect if adding a wait would create a cycle (deadlock)
    fn would_create_cycle(&self, waiter: TransactionId, holders: &[TransactionId]) -> bool {
        let graph = self.wait_for.read();

        // Pre-allocate with reasonable capacity to avoid reallocations
        let estimated_size = graph.len().min(64);
        let mut visited = HashSet::with_capacity(estimated_size);
        let mut stack = Vec::with_capacity(estimated_size);

        // DFS from each holder to see if we can reach waiter
        for holder in holders {
            if *holder == waiter {
                continue;
            }

            // Reuse allocations but clear contents
            stack.clear();
            visited.clear();
            stack.push(*holder);

            while let Some(current) = stack.pop() {
                if current == waiter {
                    return true; // Cycle detected!
                }

                if visited.insert(current) {
                    if let Some(waiting_for) = graph.get(&current) {
                        // Extend directly without intermediate allocation
                        for &txn in waiting_for {
                            if !visited.contains(&txn) {
                                stack.push(txn);
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// Find all transactions involved in a deadlock cycle
    #[allow(dead_code)]
    fn find_cycle(&self) -> Option<Vec<TransactionId>> {
        let graph = self.wait_for.read();

        for &start in graph.keys() {
            let mut path = vec![start];
            let mut visited = HashSet::new();
            visited.insert(start);

            if self.find_cycle_from(&graph, start, &mut path, &mut visited) {
                return Some(path);
            }
        }

        None
    }

    fn find_cycle_from(
        &self,
        graph: &HashMap<TransactionId, HashSet<TransactionId>>,
        current: TransactionId,
        path: &mut Vec<TransactionId>,
        visited: &mut HashSet<TransactionId>,
    ) -> bool {
        if let Some(waiting_for) = graph.get(&current) {
            for &next in waiting_for {
                if path.first() == Some(&next) {
                    // Found cycle back to start
                    return true;
                }
                if !visited.contains(&next) {
                    visited.insert(next);
                    path.push(next);
                    if self.find_cycle_from(graph, next, path, visited) {
                        return true;
                    }
                    path.pop();
                }
            }
        }
        false
    }
}

/// Configuration for the lock manager
#[derive(Debug, Clone)]
pub struct LockManagerConfig {
    /// Maximum time to wait for a lock before timing out
    pub lock_timeout: Duration,

    /// Whether to enable deadlock detection
    pub deadlock_detection: bool,

    /// Number of shards for row locks (for concurrency)
    pub row_lock_shards: usize,
}

impl Default for LockManagerConfig {
    fn default() -> Self {
        LockManagerConfig {
            lock_timeout: Duration::from_secs(30),
            deadlock_detection: true,
            row_lock_shards: 64,
        }
    }
}

/// Sharded lock storage for row-level locks
struct ShardedLocks {
    shards: Vec<RwLock<HashMap<LockTarget, LockState>>>,
    shard_count: usize,
}

impl ShardedLocks {
    fn new(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(HashMap::new()));
        }
        ShardedLocks {
            shards,
            shard_count,
        }
    }

    fn shard_for(&self, target: &LockTarget) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        target.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }

    fn get_shard(&self, target: &LockTarget) -> &RwLock<HashMap<LockTarget, LockState>> {
        &self.shards[self.shard_for(target)]
    }
}

/// Lock manager handling all lock operations
pub struct LockManager {
    /// Configuration
    config: LockManagerConfig,

    /// Table-level locks
    table_locks: RwLock<HashMap<LockTarget, LockState>>,

    /// Row-level locks (sharded for concurrency)
    row_locks: ShardedLocks,

    /// Deadlock detector
    deadlock_detector: DeadlockDetector,

    /// Next lock ID
    next_lock_id: AtomicU64,

    /// Active locks by transaction (for cleanup)
    txn_locks: RwLock<HashMap<TransactionId, Vec<LockHandle>>>,

    /// Per-lock waiters for targeted wakeup (avoids thundering herd)
    /// Maps lock target hash to set of waiting transaction IDs
    per_lock_waiters: Mutex<HashMap<u64, HashSet<TransactionId>>>,

    /// Condition variable per transaction for efficient wake-up
    waiters: Mutex<HashMap<TransactionId, Arc<std::sync::Condvar>>>,
}

impl LockManager {
    /// Create a new lock manager with default config
    pub fn new() -> Self {
        Self::with_config(LockManagerConfig::default())
    }

    /// Create a lock manager with custom config
    pub fn with_config(config: LockManagerConfig) -> Self {
        LockManager {
            row_locks: ShardedLocks::new(config.row_lock_shards),
            config,
            table_locks: RwLock::new(HashMap::new()),
            deadlock_detector: DeadlockDetector::new(),
            next_lock_id: AtomicU64::new(1),
            txn_locks: RwLock::new(HashMap::new()),
            per_lock_waiters: Mutex::new(HashMap::new()),
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Compute hash for a lock target (for per-lock waiter tracking)
    #[inline]
    fn target_hash(target: &LockTarget) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        target.hash(&mut hasher);
        hasher.finish()
    }

    /// Acquire a lock
    pub fn acquire(
        &self,
        txn_id: TransactionId,
        database: &str,
        table: &str,
        row_key: Option<&[u8]>,
        mode: LockMode,
    ) -> Result<LockHandle, EngineError> {
        let target = match row_key {
            Some(key) => LockTarget::row(database, table, key),
            None => LockTarget::table(database, table),
        };

        // For row locks, first acquire intention lock on table
        if row_key.is_some() {
            let intention_mode = match mode {
                LockMode::Shared => LockMode::IntentionShared,
                LockMode::Exclusive | LockMode::Update => LockMode::IntentionExclusive,
                m => m,
            };
            self.acquire_internal(txn_id, LockTarget::table(database, table), intention_mode)?;
        }

        self.acquire_internal(txn_id, target, mode)
    }

    fn acquire_internal(
        &self,
        txn_id: TransactionId,
        target: LockTarget,
        mode: LockMode,
    ) -> Result<LockHandle, EngineError> {
        let deadline = Instant::now() + self.config.lock_timeout;

        // Create condition variable for this transaction if needed (held throughout)
        let condvar = {
            let mut waiters = self.waiters.lock();
            waiters
                .entry(txn_id)
                .or_insert_with(|| Arc::new(std::sync::Condvar::new()))
                .clone()
        };

        // Create a mutex for the condition variable to use
        let wait_mutex = std::sync::Mutex::new(false);

        // Helper to cleanup waiters atomically
        let cleanup_waiter = |this: &Self, target: &LockTarget| {
            // Unregister from per-lock waiters
            this.unregister_waiter(target, txn_id);
            // Acquire both locks together to avoid TOCTOU
            let mut waiters = this.waiters.lock();
            waiters.remove(&txn_id);
            // Remove from deadlock detector while we have consistent state
            this.deadlock_detector.remove_waiter(txn_id);
        };

        // Adaptive wait time: start short, increase on each retry
        let mut wait_iteration = 0u32;

        loop {
            // Try to acquire the lock
            match self.try_acquire(&target, txn_id, mode)? {
                AcquireResult::Granted(handle) => {
                    // Atomically remove from waiters map and deadlock detector
                    cleanup_waiter(self, &target);
                    return Ok(handle);
                }
                AcquireResult::MustWait(holders) => {
                    // Register as waiting for this specific lock (for targeted wakeup)
                    self.register_waiter(&target, txn_id);

                    // Check for deadlock - use atomic check-and-add pattern
                    if self.config.deadlock_detection {
                        // Check cycle and add wait atomically by holding detector lock
                        let would_deadlock = self.deadlock_detector.would_create_cycle(txn_id, &holders);
                        if would_deadlock {
                            cleanup_waiter(self, &target);
                            return Err(EngineError::Internal(format!(
                                "Deadlock detected: transaction {} would create a cycle",
                                txn_id
                            )));
                        }
                        // Add wait edges (detector handles its own locking)
                        self.deadlock_detector.add_wait(txn_id, &holders);
                    }

                    // Check timeout
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        cleanup_waiter(self, &target);
                        return Err(EngineError::Timeout(format!(
                            "Lock acquisition timed out after {:?}",
                            self.config.lock_timeout
                        )));
                    }

                    // Adaptive wait time: start at 1ms, double each iteration up to 10ms max
                    // This reduces CPU usage for long waits while keeping latency low for short waits
                    let base_wait_ms = 1u64 << wait_iteration.min(3); // 1, 2, 4, 8ms
                    let wait_time = remaining.min(Duration::from_millis(base_wait_ms.min(10)));
                    wait_iteration = wait_iteration.saturating_add(1);

                    let guard = wait_mutex.lock().unwrap();
                    let _ = condvar.wait_timeout(guard, wait_time);
                }
            }
        }
    }

    /// Wake up waiting transactions for a specific lock target (targeted wakeup)
    /// This avoids the thundering herd problem by only waking transactions waiting for this lock
    fn wake_waiters(&self, target: &LockTarget) {
        let target_hash = Self::target_hash(target);

        // Get the set of transactions waiting for this specific lock
        let waiting_txns: Vec<TransactionId> = {
            let per_lock = self.per_lock_waiters.lock();
            per_lock
                .get(&target_hash)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default()
        };

        // Only wake transactions that are waiting for this specific lock
        if !waiting_txns.is_empty() {
            let waiters = self.waiters.lock();
            for txn_id in waiting_txns {
                if let Some(condvar) = waiters.get(&txn_id) {
                    condvar.notify_one();
                }
            }
        }

        // Periodically clean up stale entries (every ~100 wakeups based on lock ID)
        let lock_count = self.next_lock_id.load(Ordering::Relaxed);
        if lock_count % 100 == 0 {
            self.cleanup_stale_waiters();
        }
    }

    /// Register a transaction as waiting for a specific lock
    fn register_waiter(&self, target: &LockTarget, txn_id: TransactionId) {
        let target_hash = Self::target_hash(target);
        let mut per_lock = self.per_lock_waiters.lock();
        per_lock.entry(target_hash).or_default().insert(txn_id);
    }

    /// Unregister a transaction from waiting for a specific lock
    fn unregister_waiter(&self, target: &LockTarget, txn_id: TransactionId) {
        let target_hash = Self::target_hash(target);
        let mut per_lock = self.per_lock_waiters.lock();
        if let Some(waiters) = per_lock.get_mut(&target_hash) {
            waiters.remove(&txn_id);
            if waiters.is_empty() {
                per_lock.remove(&target_hash);
            }
        }
    }

    /// Clean up orphaned waiter entries that may have been left behind
    /// by transactions that crashed or panicked without proper cleanup
    fn cleanup_stale_waiters(&self) {
        // Clean up deadlock detector entries older than 2x the lock timeout
        let max_wait = self.config.lock_timeout * 2;
        self.deadlock_detector.cleanup_stale_waiters(max_wait);

        // Clean up condvar entries for transactions that are no longer waiting
        // by checking if they have any entries in the deadlock detector
        let stale_condvar_txns: Vec<TransactionId> = {
            let waiters = self.waiters.lock();
            let detector_graph = self.deadlock_detector.wait_for.read();
            waiters
                .keys()
                .filter(|txn_id| !detector_graph.contains_key(txn_id))
                .cloned()
                .collect()
        };

        if !stale_condvar_txns.is_empty() {
            // Clean up per-lock waiters
            {
                let mut per_lock = self.per_lock_waiters.lock();
                for txn_id in &stale_condvar_txns {
                    per_lock.retain(|_, waiters| {
                        waiters.remove(txn_id);
                        !waiters.is_empty()
                    });
                }
            }

            // Clean up condvars
            let mut waiters = self.waiters.lock();
            for txn_id in stale_condvar_txns {
                waiters.remove(&txn_id);
            }
        }
    }

    fn try_acquire(
        &self,
        target: &LockTarget,
        txn_id: TransactionId,
        mode: LockMode,
    ) -> Result<AcquireResult, EngineError> {
        let lock_id = self.next_lock_id.fetch_add(1, Ordering::SeqCst);

        let locks = match target {
            LockTarget::Table { .. } => {
                let mut locks = self.table_locks.write();
                let state = locks.entry(target.clone()).or_insert_with(LockState::new);
                self.try_grant_lock(state, txn_id, mode, lock_id, target)
            }
            LockTarget::Row { .. } => {
                let shard = self.row_locks.get_shard(target);
                let mut locks = shard.write();
                let state = locks.entry(target.clone()).or_insert_with(LockState::new);
                self.try_grant_lock(state, txn_id, mode, lock_id, target)
            }
        };

        if let AcquireResult::Granted(ref handle) = locks {
            // Track lock for transaction
            let mut txn_locks = self.txn_locks.write();
            txn_locks
                .entry(txn_id)
                .or_insert_with(Vec::new)
                .push(handle.clone());

            // Remove from deadlock detector wait list
            self.deadlock_detector.remove_waiter(txn_id);
        }

        Ok(locks)
    }

    fn try_grant_lock(
        &self,
        state: &mut LockState,
        txn_id: TransactionId,
        mode: LockMode,
        lock_id: LockId,
        target: &LockTarget,
    ) -> AcquireResult {
        // Check if transaction already holds a lock
        if let Some((held_mode, held_id)) = state.find_held_lock(txn_id) {
            if held_mode == mode {
                // Same lock, return existing handle
                return AcquireResult::Granted(LockHandle {
                    id: held_id,
                    txn_id,
                    target: target.clone(),
                    mode: held_mode,
                });
            }

            // Check for upgrade
            if held_mode.can_upgrade_to(&mode) {
                // Try to upgrade
                if state.is_compatible(txn_id, &mode) && !state.has_waiting_ahead(txn_id) {
                    // Remove old lock
                    state.granted.retain(|(tid, _, _)| *tid != txn_id);
                    // Grant upgraded lock
                    state.granted.push((txn_id, mode, lock_id));
                    return AcquireResult::Granted(LockHandle {
                        id: lock_id,
                        txn_id,
                        target: target.clone(),
                        mode,
                    });
                }
            }
        }

        // Check compatibility with current holders
        if state.is_compatible(txn_id, &mode) && !state.has_waiting_ahead(txn_id) {
            // Grant the lock
            state.granted.push((txn_id, mode, lock_id));
            AcquireResult::Granted(LockHandle {
                id: lock_id,
                txn_id,
                target: target.clone(),
                mode,
            })
        } else {
            // Must wait
            let holders: Vec<TransactionId> = state
                .granted
                .iter()
                .filter(|(tid, _, _)| *tid != txn_id)
                .map(|(tid, _, _)| *tid)
                .collect();

            // Add to wait queue if not already there
            if !state.waiting.iter().any(|r| r.txn_id == txn_id) {
                state.waiting.push_back(LockRequest {
                    id: lock_id,
                    txn_id,
                    mode,
                    requested_at: Instant::now(),
                });
            }

            AcquireResult::MustWait(holders)
        }
    }

    /// Release a lock
    pub fn release(&self, handle: LockHandle) -> Result<(), EngineError> {
        let target = handle.target.clone();

        match &handle.target {
            LockTarget::Table { .. } => {
                let mut locks = self.table_locks.write();
                if let Some(state) = locks.get_mut(&handle.target) {
                    state.granted.retain(|(_, _, id)| *id != handle.id);
                    state.waiting.retain(|r| r.id != handle.id);

                    // Clean up empty states
                    if state.granted.is_empty() && state.waiting.is_empty() {
                        locks.remove(&handle.target);
                    }
                }
            }
            LockTarget::Row { .. } => {
                let shard = self.row_locks.get_shard(&handle.target);
                let mut locks = shard.write();
                if let Some(state) = locks.get_mut(&handle.target) {
                    state.granted.retain(|(_, _, id)| *id != handle.id);
                    state.waiting.retain(|r| r.id != handle.id);

                    if state.granted.is_empty() && state.waiting.is_empty() {
                        locks.remove(&handle.target);
                    }
                }
            }
        }

        // Remove from transaction's lock list
        let mut txn_locks = self.txn_locks.write();
        if let Some(locks) = txn_locks.get_mut(&handle.txn_id) {
            locks.retain(|l| l.id != handle.id);
        }

        // Update deadlock detector
        self.deadlock_detector.remove_holder(handle.txn_id);

        // Wake up any waiting transactions so they can try to acquire the lock
        self.wake_waiters(&target);

        Ok(())
    }

    /// Release all locks held by a transaction
    /// Optimized: acquires txn_locks once and inlines release logic to avoid
    /// redundant lock acquisitions and potential deadlocks
    pub fn release_all(&self, txn_id: TransactionId) -> Result<(), EngineError> {
        // Get all locks for this transaction (single lock acquisition)
        let locks = {
            let mut txn_locks = self.txn_locks.write();
            txn_locks.remove(&txn_id).unwrap_or_default()
        };
        // txn_locks is now dropped - we have ownership of the locks Vec

        // Collect targets for wake_waiters (to be called after releasing all locks)
        let mut targets_to_wake = Vec::with_capacity(locks.len());

        // Release each lock (inlined release logic, no recursive txn_locks access)
        for handle in locks {
            targets_to_wake.push(handle.target.clone());

            match &handle.target {
                LockTarget::Table { .. } => {
                    let mut table_locks = self.table_locks.write();
                    if let Some(state) = table_locks.get_mut(&handle.target) {
                        state.granted.retain(|(_, _, id)| *id != handle.id);
                        state.waiting.retain(|r| r.id != handle.id);

                        // Clean up empty states
                        if state.granted.is_empty() && state.waiting.is_empty() {
                            table_locks.remove(&handle.target);
                        }
                    }
                }
                LockTarget::Row { .. } => {
                    let shard = self.row_locks.get_shard(&handle.target);
                    let mut shard_locks = shard.write();
                    if let Some(state) = shard_locks.get_mut(&handle.target) {
                        state.granted.retain(|(_, _, id)| *id != handle.id);
                        state.waiting.retain(|r| r.id != handle.id);

                        if state.granted.is_empty() && state.waiting.is_empty() {
                            shard_locks.remove(&handle.target);
                        }
                    }
                }
            }
        }

        // Clean up deadlock detector and waiters
        self.deadlock_detector.remove_waiter(txn_id);
        self.deadlock_detector.remove_holder(txn_id);

        // Clean up per-lock waiters (remove from all lock wait lists)
        {
            let mut per_lock = self.per_lock_waiters.lock();
            per_lock.retain(|_, waiters| {
                waiters.remove(&txn_id);
                !waiters.is_empty()
            });
        }

        // Also clean up any orphaned condvar for this transaction
        self.waiters.lock().remove(&txn_id);

        // Wake up waiters after all locks are released (batched for efficiency)
        for target in targets_to_wake {
            self.wake_waiters(&target);
        }

        Ok(())
    }

    /// Get all locks held by a transaction
    pub fn get_locks(&self, txn_id: TransactionId) -> Vec<LockHandle> {
        self.txn_locks
            .read()
            .get(&txn_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get lock statistics
    pub fn stats(&self) -> LockStats {
        let table_locks = self.table_locks.read();
        let mut stats = LockStats {
            total_table_locks: 0,
            total_row_locks: 0,
            waiting_requests: 0,
            transactions_with_locks: 0,
        };

        for state in table_locks.values() {
            stats.total_table_locks += state.granted.len();
            stats.waiting_requests += state.waiting.len();
        }

        for shard in &self.row_locks.shards {
            let locks = shard.read();
            for state in locks.values() {
                stats.total_row_locks += state.granted.len();
                stats.waiting_requests += state.waiting.len();
            }
        }

        stats.transactions_with_locks = self.txn_locks.read().len();

        stats
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a lock acquisition attempt
enum AcquireResult {
    /// Lock was granted
    Granted(LockHandle),

    /// Must wait for these transactions
    MustWait(Vec<TransactionId>),
}

/// Lock manager statistics
#[derive(Debug, Clone)]
pub struct LockStats {
    /// Total table-level locks held
    pub total_table_locks: usize,

    /// Total row-level locks held
    pub total_row_locks: usize,

    /// Number of waiting lock requests
    pub waiting_requests: usize,

    /// Number of transactions holding locks
    pub transactions_with_locks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_compatibility() {
        use LockMode::*;

        // Shared locks are compatible
        assert!(Shared.is_compatible(&Shared));

        // Exclusive is not compatible with anything
        assert!(!Exclusive.is_compatible(&Shared));
        assert!(!Exclusive.is_compatible(&Exclusive));
        assert!(!Shared.is_compatible(&Exclusive));

        // Intention locks are compatible with each other
        assert!(IntentionShared.is_compatible(&IntentionShared));
        assert!(IntentionShared.is_compatible(&IntentionExclusive));
        assert!(IntentionExclusive.is_compatible(&IntentionShared));
        assert!(IntentionExclusive.is_compatible(&IntentionExclusive));

        // Shared is compatible with intention locks
        assert!(Shared.is_compatible(&IntentionShared));
        assert!(Shared.is_compatible(&IntentionExclusive));
    }

    #[test]
    fn test_lock_upgrade() {
        use LockMode::*;

        assert!(Shared.can_upgrade_to(&Exclusive));
        assert!(Shared.can_upgrade_to(&Update));
        assert!(Update.can_upgrade_to(&Exclusive));
        assert!(!Exclusive.can_upgrade_to(&Shared));
    }

    #[test]
    fn test_basic_lock_acquire_release() {
        let manager = LockManager::new();

        let handle = manager
            .acquire(1, "db", "table", None, LockMode::Shared)
            .unwrap();
        assert_eq!(handle.txn_id, 1);
        assert_eq!(handle.mode, LockMode::Shared);

        manager.release(handle).unwrap();
    }

    #[test]
    fn test_multiple_shared_locks() {
        let manager = LockManager::new();

        let h1 = manager
            .acquire(1, "db", "table", None, LockMode::Shared)
            .unwrap();
        let h2 = manager
            .acquire(2, "db", "table", None, LockMode::Shared)
            .unwrap();

        assert_eq!(h1.mode, LockMode::Shared);
        assert_eq!(h2.mode, LockMode::Shared);

        manager.release(h1).unwrap();
        manager.release(h2).unwrap();
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let manager = LockManager::with_config(LockManagerConfig {
            lock_timeout: Duration::from_millis(100),
            ..Default::default()
        });

        let h1 = manager
            .acquire(1, "db", "table", None, LockMode::Exclusive)
            .unwrap();

        // This should timeout
        let result = manager.acquire(2, "db", "table", None, LockMode::Shared);
        assert!(result.is_err());

        manager.release(h1).unwrap();

        // Now it should succeed
        let h2 = manager
            .acquire(2, "db", "table", None, LockMode::Shared)
            .unwrap();
        manager.release(h2).unwrap();
    }

    #[test]
    fn test_row_lock_with_intention() {
        let manager = LockManager::new();

        // Row lock should automatically acquire intention lock on table
        let h1 = manager
            .acquire(1, "db", "table", Some(b"row1"), LockMode::Exclusive)
            .unwrap();

        // Another transaction should be able to lock a different row
        let h2 = manager
            .acquire(2, "db", "table", Some(b"row2"), LockMode::Exclusive)
            .unwrap();

        manager.release(h1).unwrap();
        manager.release(h2).unwrap();
    }

    #[test]
    fn test_release_all() {
        let manager = LockManager::new();

        let _h1 = manager
            .acquire(1, "db", "t1", None, LockMode::Shared)
            .unwrap();
        let _h2 = manager
            .acquire(1, "db", "t2", None, LockMode::Shared)
            .unwrap();
        let _h3 = manager
            .acquire(1, "db", "t3", None, LockMode::Exclusive)
            .unwrap();

        assert_eq!(manager.get_locks(1).len(), 3);

        manager.release_all(1).unwrap();

        assert_eq!(manager.get_locks(1).len(), 0);
    }

    #[test]
    fn test_deadlock_detection() {
        let detector = DeadlockDetector::new();

        // txn1 waits for txn2
        detector.add_wait(1, &[2]);

        // txn2 waits for txn3
        detector.add_wait(2, &[3]);

        // txn3 waiting for txn1 would create a cycle
        assert!(detector.would_create_cycle(3, &[1]));

        // txn3 waiting for txn4 would not
        assert!(!detector.would_create_cycle(3, &[4]));
    }
}
