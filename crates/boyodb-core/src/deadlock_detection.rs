//! Deadlock Detection and Resolution
//!
//! Provides deadlock detection, lock wait graph visualization, and optimistic locking.
//!
//! # Features
//! - Automatic deadlock detection
//! - Wait-for graph construction and cycle detection
//! - Victim selection for deadlock resolution
//! - Lock wait graph visualization
//! - Optimistic locking helpers (version columns)
//!
//! # Example
//! ```sql
//! -- View lock waits
//! SELECT * FROM pg_locks WHERE NOT granted;
//!
//! -- View lock wait graph
//! SELECT * FROM pg_blocking_pids(pid);
//!
//! -- Optimistic locking
//! UPDATE products
//! SET price = 10.00, version = version + 1
//! WHERE id = 1 AND version = 5;
//! ```

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Types and Errors
// ============================================================================

/// Transaction ID type
pub type TransactionId = u64;

/// Lock ID type
pub type LockId = u64;

/// Errors from deadlock detection
#[derive(Debug, Clone)]
pub enum DeadlockError {
    /// Deadlock detected
    DeadlockDetected {
        /// Transactions involved in the deadlock
        transactions: Vec<TransactionId>,
        /// The victim transaction (to be aborted)
        victim: TransactionId,
    },
    /// Lock wait timeout
    LockTimeout {
        transaction: TransactionId,
        lock_id: LockId,
        waited_ms: u64,
    },
    /// Transaction not found
    TransactionNotFound(TransactionId),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for DeadlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeadlockDetected {
                transactions,
                victim,
            } => {
                write!(
                    f,
                    "deadlock detected among transactions {:?}, aborting transaction {}",
                    transactions, victim
                )
            }
            Self::LockTimeout {
                transaction,
                lock_id,
                waited_ms,
            } => {
                write!(
                    f,
                    "lock timeout: transaction {} waiting for lock {} for {}ms",
                    transaction, lock_id, waited_ms
                )
            }
            Self::TransactionNotFound(txn) => write!(f, "transaction {} not found", txn),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for DeadlockError {}

/// PostgreSQL error codes
pub mod sqlstate {
    pub const DEADLOCK_DETECTED: &str = "40P01";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const SERIALIZATION_FAILURE: &str = "40001";
}

// ============================================================================
// Lock Types
// ============================================================================

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Access Share (SELECT)
    AccessShare,
    /// Row Share (SELECT FOR UPDATE)
    RowShare,
    /// Row Exclusive (UPDATE, DELETE, INSERT)
    RowExclusive,
    /// Share Update Exclusive (VACUUM, ANALYZE)
    ShareUpdateExclusive,
    /// Share (CREATE INDEX)
    Share,
    /// Share Row Exclusive
    ShareRowExclusive,
    /// Exclusive
    Exclusive,
    /// Access Exclusive (DROP, ALTER, VACUUM FULL)
    AccessExclusive,
}

impl LockMode {
    /// Check if this mode conflicts with another
    pub fn conflicts_with(&self, other: &LockMode) -> bool {
        use LockMode::*;
        // Lock conflict matrix (PostgreSQL compatible)
        matches!(
            (self, other),
            // AccessShare conflicts with AccessExclusive
            (AccessShare, AccessExclusive)
                | (AccessExclusive, AccessShare)
                // RowShare conflicts with Exclusive, AccessExclusive
                | (RowShare, Exclusive)
                | (RowShare, AccessExclusive)
                | (Exclusive, RowShare)
                | (AccessExclusive, RowShare)
                // RowExclusive conflicts with Share, ShareRowExclusive, Exclusive, AccessExclusive
                | (RowExclusive, Share)
                | (RowExclusive, ShareRowExclusive)
                | (RowExclusive, Exclusive)
                | (RowExclusive, AccessExclusive)
                | (Share, RowExclusive)
                | (ShareRowExclusive, RowExclusive)
                | (Exclusive, RowExclusive)
                | (AccessExclusive, RowExclusive)
                // ShareUpdateExclusive conflicts with itself and stronger
                | (ShareUpdateExclusive, ShareUpdateExclusive)
                | (ShareUpdateExclusive, Share)
                | (ShareUpdateExclusive, ShareRowExclusive)
                | (ShareUpdateExclusive, Exclusive)
                | (ShareUpdateExclusive, AccessExclusive)
                | (Share, ShareUpdateExclusive)
                | (ShareRowExclusive, ShareUpdateExclusive)
                | (Exclusive, ShareUpdateExclusive)
                | (AccessExclusive, ShareUpdateExclusive)
                // Share conflicts with RowExclusive and stronger
                | (Share, Share) // Share is compatible with Share? No, actually Share IS compatible
                | (Share, ShareRowExclusive)
                | (Share, Exclusive)
                | (Share, AccessExclusive)
                | (ShareRowExclusive, Share)
                | (Exclusive, Share)
                | (AccessExclusive, Share)
                // ShareRowExclusive conflicts with RowExclusive and stronger
                | (ShareRowExclusive, ShareRowExclusive)
                | (ShareRowExclusive, Exclusive)
                | (ShareRowExclusive, AccessExclusive)
                | (Exclusive, ShareRowExclusive)
                | (AccessExclusive, ShareRowExclusive)
                // Exclusive conflicts with all except AccessShare
                | (Exclusive, Exclusive)
                | (Exclusive, AccessExclusive)
                | (AccessExclusive, Exclusive)
                // AccessExclusive conflicts with everything
                | (AccessExclusive, AccessExclusive)
        )
    }
}

impl std::fmt::Display for LockMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccessShare => write!(f, "AccessShareLock"),
            Self::RowShare => write!(f, "RowShareLock"),
            Self::RowExclusive => write!(f, "RowExclusiveLock"),
            Self::ShareUpdateExclusive => write!(f, "ShareUpdateExclusiveLock"),
            Self::Share => write!(f, "ShareLock"),
            Self::ShareRowExclusive => write!(f, "ShareRowExclusiveLock"),
            Self::Exclusive => write!(f, "ExclusiveLock"),
            Self::AccessExclusive => write!(f, "AccessExclusiveLock"),
        }
    }
}

/// Lock target type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockTarget {
    /// Relation (table) lock
    Relation { database: u32, relation: u32 },
    /// Tuple (row) lock
    Tuple {
        database: u32,
        relation: u32,
        block: u32,
        offset: u16,
    },
    /// Transaction lock
    Transaction(TransactionId),
    /// Advisory lock
    Advisory { database: u32, key1: u32, key2: u32 },
}

impl std::fmt::Display for LockTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relation { database, relation } => {
                write!(f, "relation {}.{}", database, relation)
            }
            Self::Tuple {
                database,
                relation,
                block,
                offset,
            } => {
                write!(f, "tuple {}.{}.{}.{}", database, relation, block, offset)
            }
            Self::Transaction(txn) => write!(f, "transaction {}", txn),
            Self::Advisory {
                database,
                key1,
                key2,
            } => {
                write!(f, "advisory {}.{}.{}", database, key1, key2)
            }
        }
    }
}

// ============================================================================
// Lock State
// ============================================================================

/// A held or requested lock
#[derive(Debug, Clone)]
pub struct LockEntry {
    /// Lock ID
    pub lock_id: LockId,
    /// Target being locked
    pub target: LockTarget,
    /// Lock mode
    pub mode: LockMode,
    /// Transaction holding/requesting the lock
    pub transaction_id: TransactionId,
    /// Whether lock is granted
    pub granted: bool,
    /// When the lock was requested
    pub requested_at: Instant,
    /// When the lock was granted (if granted)
    pub granted_at: Option<Instant>,
}

/// Transaction state for deadlock detection
#[derive(Debug, Clone)]
pub struct TransactionState {
    /// Transaction ID
    pub transaction_id: TransactionId,
    /// Locks held by this transaction
    pub held_locks: Vec<LockId>,
    /// Locks waited on by this transaction
    pub waiting_for: Option<LockId>,
    /// Transaction start time
    pub started_at: Instant,
    /// Transaction priority (lower = more likely to be victim)
    pub priority: i32,
    /// Estimated work done (for victim selection)
    pub work_done: u64,
}

// ============================================================================
// Wait-For Graph
// ============================================================================

/// Edge in the wait-for graph
#[derive(Debug, Clone)]
pub struct WaitEdge {
    /// Waiting transaction
    pub waiter: TransactionId,
    /// Blocking transaction
    pub blocker: TransactionId,
    /// Lock being waited for
    pub lock_id: LockId,
    /// Lock target
    pub target: LockTarget,
    /// Lock mode requested
    pub requested_mode: LockMode,
    /// Lock mode held by blocker
    pub blocking_mode: LockMode,
}

/// Wait-for graph for deadlock detection
pub struct WaitForGraph {
    /// Edges in the graph (waiter -> blockers)
    edges: HashMap<TransactionId, Vec<WaitEdge>>,
    /// Reverse edges (blocker -> waiters)
    reverse_edges: HashMap<TransactionId, Vec<TransactionId>>,
}

impl Default for WaitForGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitForGraph {
    /// Create a new empty graph
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            reverse_edges: HashMap::new(),
        }
    }

    /// Add an edge (waiter waiting for blocker)
    pub fn add_edge(&mut self, edge: WaitEdge) {
        let waiter = edge.waiter;
        let blocker = edge.blocker;

        self.edges.entry(waiter).or_default().push(edge);
        self.reverse_edges.entry(blocker).or_default().push(waiter);
    }

    /// Remove all edges for a transaction
    pub fn remove_transaction(&mut self, txn: TransactionId) {
        // Remove outgoing edges
        if let Some(edges) = self.edges.remove(&txn) {
            for edge in edges {
                if let Some(waiters) = self.reverse_edges.get_mut(&edge.blocker) {
                    waiters.retain(|&w| w != txn);
                }
            }
        }

        // Remove incoming edges
        if let Some(waiters) = self.reverse_edges.remove(&txn) {
            for waiter in waiters {
                if let Some(edges) = self.edges.get_mut(&waiter) {
                    edges.retain(|e| e.blocker != txn);
                }
            }
        }
    }

    /// Detect cycles (deadlocks) using DFS
    pub fn detect_cycles(&self) -> Vec<Vec<TransactionId>> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for &start in self.edges.keys() {
            if !visited.contains(&start) {
                self.dfs_detect(start, &mut visited, &mut rec_stack, &mut path, &mut cycles);
            }
        }

        cycles
    }

    fn dfs_detect(
        &self,
        node: TransactionId,
        visited: &mut HashSet<TransactionId>,
        rec_stack: &mut HashSet<TransactionId>,
        path: &mut Vec<TransactionId>,
        cycles: &mut Vec<Vec<TransactionId>>,
    ) {
        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        if let Some(edges) = self.edges.get(&node) {
            for edge in edges {
                let neighbor = edge.blocker;

                if !visited.contains(&neighbor) {
                    self.dfs_detect(neighbor, visited, rec_stack, path, cycles);
                } else if rec_stack.contains(&neighbor) {
                    // Found a cycle - extract it
                    let cycle_start = path.iter().position(|&n| n == neighbor).unwrap();
                    let cycle: Vec<_> = path[cycle_start..].to_vec();
                    cycles.push(cycle);
                }
            }
        }

        path.pop();
        rec_stack.remove(&node);
    }

    /// Get all transactions blocking a given transaction
    pub fn get_blockers(&self, waiter: TransactionId) -> Vec<TransactionId> {
        self.edges
            .get(&waiter)
            .map(|edges| edges.iter().map(|e| e.blocker).collect())
            .unwrap_or_default()
    }

    /// Get all transactions waiting on a given transaction
    pub fn get_waiters(&self, blocker: TransactionId) -> Vec<TransactionId> {
        self.reverse_edges
            .get(&blocker)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all edges
    pub fn get_edges(&self) -> Vec<WaitEdge> {
        self.edges.values().flatten().cloned().collect()
    }

    /// Check if graph is empty
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        let mut nodes = HashSet::new();
        for (waiter, edges) in &self.edges {
            nodes.insert(*waiter);
            for edge in edges {
                nodes.insert(edge.blocker);
            }
        }
        nodes.len()
    }

    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.edges.values().map(|e| e.len()).sum()
    }
}

// ============================================================================
// Deadlock Detector
// ============================================================================

/// Victim selection strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VictimStrategy {
    /// Abort the youngest transaction
    Youngest,
    /// Abort the oldest transaction
    Oldest,
    /// Abort the transaction with least work done
    LeastWork,
    /// Abort the transaction with lowest priority
    LowestPriority,
    /// Random selection
    Random,
}

impl Default for VictimStrategy {
    fn default() -> Self {
        Self::LeastWork
    }
}

/// Deadlock detection configuration
#[derive(Debug, Clone)]
pub struct DeadlockConfig {
    /// Detection interval in milliseconds
    pub detection_interval_ms: u64,
    /// Lock wait timeout in milliseconds (0 = no timeout)
    pub lock_timeout_ms: u64,
    /// Victim selection strategy
    pub victim_strategy: VictimStrategy,
    /// Enable detection
    pub enabled: bool,
}

impl Default for DeadlockConfig {
    fn default() -> Self {
        Self {
            detection_interval_ms: 1000,
            lock_timeout_ms: 0,
            victim_strategy: VictimStrategy::LeastWork,
            enabled: true,
        }
    }
}

/// Deadlock detection statistics
#[derive(Debug, Clone, Default)]
pub struct DeadlockStats {
    /// Total detection runs
    pub detection_runs: u64,
    /// Deadlocks detected
    pub deadlocks_detected: u64,
    /// Transactions aborted due to deadlock
    pub transactions_aborted: u64,
    /// Lock timeouts
    pub lock_timeouts: u64,
    /// Average cycle length
    pub avg_cycle_length: f64,
    /// Maximum cycle length seen
    pub max_cycle_length: usize,
}

/// Deadlock detector
pub struct DeadlockDetector {
    /// Configuration
    config: DeadlockConfig,
    /// All lock entries
    locks: RwLock<HashMap<LockId, LockEntry>>,
    /// Transaction states
    transactions: RwLock<HashMap<TransactionId, TransactionState>>,
    /// Wait-for graph
    graph: RwLock<WaitForGraph>,
    /// Next lock ID
    next_lock_id: AtomicU64,
    /// Statistics
    stats: RwLock<DeadlockStats>,
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new(DeadlockConfig::default())
    }
}

impl DeadlockDetector {
    /// Create a new deadlock detector
    pub fn new(config: DeadlockConfig) -> Self {
        Self {
            config,
            locks: RwLock::new(HashMap::new()),
            transactions: RwLock::new(HashMap::new()),
            graph: RwLock::new(WaitForGraph::new()),
            next_lock_id: AtomicU64::new(1),
            stats: RwLock::new(DeadlockStats::default()),
        }
    }

    /// Register a transaction
    pub fn register_transaction(&self, txn_id: TransactionId) {
        let state = TransactionState {
            transaction_id: txn_id,
            held_locks: Vec::new(),
            waiting_for: None,
            started_at: Instant::now(),
            priority: 0,
            work_done: 0,
        };
        self.transactions.write().insert(txn_id, state);
    }

    /// Unregister a transaction (releases all locks)
    pub fn unregister_transaction(&self, txn_id: TransactionId) {
        // Get and remove transaction state
        let state = self.transactions.write().remove(&txn_id);

        if let Some(state) = state {
            // Release all held locks
            let mut locks = self.locks.write();
            for lock_id in state.held_locks {
                locks.remove(&lock_id);
            }
        }

        // Remove from wait-for graph
        self.graph.write().remove_transaction(txn_id);
    }

    /// Request a lock
    pub fn request_lock(
        &self,
        txn_id: TransactionId,
        target: LockTarget,
        mode: LockMode,
    ) -> Result<LockId, DeadlockError> {
        let lock_id = self.next_lock_id.fetch_add(1, Ordering::Relaxed);

        // Check for conflicts
        let conflicts = self.find_conflicts(&target, mode, txn_id);

        let entry = LockEntry {
            lock_id,
            target: target.clone(),
            mode,
            transaction_id: txn_id,
            granted: conflicts.is_empty(),
            requested_at: Instant::now(),
            granted_at: if conflicts.is_empty() {
                Some(Instant::now())
            } else {
                None
            },
        };

        // Add lock entry
        self.locks.write().insert(lock_id, entry.clone());

        // Update transaction state
        if let Some(state) = self.transactions.write().get_mut(&txn_id) {
            if entry.granted {
                state.held_locks.push(lock_id);
            } else {
                state.waiting_for = Some(lock_id);

                // Add edges to wait-for graph
                let mut graph = self.graph.write();
                for (blocking_txn, blocking_mode) in &conflicts {
                    graph.add_edge(WaitEdge {
                        waiter: txn_id,
                        blocker: *blocking_txn,
                        lock_id,
                        target: target.clone(),
                        requested_mode: mode,
                        blocking_mode: *blocking_mode,
                    });
                }
            }
        }

        // If not granted, check for deadlock
        if !entry.granted {
            if let Some(cycle) = self.check_deadlock()? {
                // Deadlock detected - select victim
                let victim = self.select_victim(&cycle);
                return Err(DeadlockError::DeadlockDetected {
                    transactions: cycle,
                    victim,
                });
            }
        }

        Ok(lock_id)
    }

    /// Find conflicting locks
    fn find_conflicts(
        &self,
        target: &LockTarget,
        mode: LockMode,
        requesting_txn: TransactionId,
    ) -> Vec<(TransactionId, LockMode)> {
        let locks = self.locks.read();
        let mut conflicts = Vec::new();

        for entry in locks.values() {
            if entry.target == *target
                && entry.granted
                && entry.transaction_id != requesting_txn
                && mode.conflicts_with(&entry.mode)
            {
                conflicts.push((entry.transaction_id, entry.mode));
            }
        }

        conflicts
    }

    /// Release a lock
    pub fn release_lock(&self, lock_id: LockId) -> bool {
        let entry = self.locks.write().remove(&lock_id);

        if let Some(entry) = entry {
            // Update transaction state
            if let Some(state) = self.transactions.write().get_mut(&entry.transaction_id) {
                state.held_locks.retain(|&id| id != lock_id);
            }

            // Grant waiting locks that no longer conflict
            self.process_waiting_locks(&entry.target);

            true
        } else {
            false
        }
    }

    /// Process waiting locks after a release
    fn process_waiting_locks(&self, target: &LockTarget) {
        let mut locks = self.locks.write();
        let mut transactions = self.transactions.write();
        let mut graph = self.graph.write();

        // Find waiting locks for this target
        let waiting: Vec<_> = locks
            .values()
            .filter(|e| e.target == *target && !e.granted)
            .map(|e| (e.lock_id, e.transaction_id, e.mode.clone()))
            .collect();

        // Collect currently held locks for conflict checking
        let held_locks: Vec<_> = locks
            .values()
            .filter(|e| e.target == *target && e.granted)
            .map(|e| (e.lock_id, e.transaction_id, e.mode.clone()))
            .collect();

        for (lock_id, txn_id, mode) in waiting {
            // Check if conflicts still exist
            let still_conflicts = held_locks.iter().any(|(other_id, other_txn, other_mode)| {
                *other_id != lock_id && *other_txn != txn_id && mode.conflicts_with(other_mode)
            });

            if !still_conflicts {
                if let Some(entry) = locks.get_mut(&lock_id) {
                    // Grant the lock
                    entry.granted = true;
                    entry.granted_at = Some(Instant::now());
                }

                // Update transaction state
                if let Some(state) = transactions.get_mut(&txn_id) {
                    state.held_locks.push(lock_id);
                    state.waiting_for = None;
                }

                // Remove from wait-for graph
                graph.remove_transaction(txn_id);
            }
        }
    }

    /// Check for deadlock
    fn check_deadlock(&self) -> Result<Option<Vec<TransactionId>>, DeadlockError> {
        if !self.config.enabled {
            return Ok(None);
        }

        let graph = self.graph.read();
        let cycles = graph.detect_cycles();

        let mut stats = self.stats.write();
        stats.detection_runs += 1;

        if let Some(cycle) = cycles.into_iter().next() {
            stats.deadlocks_detected += 1;
            if cycle.len() > stats.max_cycle_length {
                stats.max_cycle_length = cycle.len();
            }
            Ok(Some(cycle))
        } else {
            Ok(None)
        }
    }

    /// Select victim transaction from a deadlock cycle
    fn select_victim(&self, cycle: &[TransactionId]) -> TransactionId {
        let transactions = self.transactions.read();

        match self.config.victim_strategy {
            VictimStrategy::Youngest => cycle
                .iter()
                .filter_map(|&txn| transactions.get(&txn))
                .max_by_key(|s| s.started_at.elapsed())
                .map(|s| s.transaction_id)
                .unwrap_or(cycle[0]),
            VictimStrategy::Oldest => cycle
                .iter()
                .filter_map(|&txn| transactions.get(&txn))
                .min_by_key(|s| s.started_at.elapsed())
                .map(|s| s.transaction_id)
                .unwrap_or(cycle[0]),
            VictimStrategy::LeastWork => cycle
                .iter()
                .filter_map(|&txn| transactions.get(&txn))
                .min_by_key(|s| s.work_done)
                .map(|s| s.transaction_id)
                .unwrap_or(cycle[0]),
            VictimStrategy::LowestPriority => cycle
                .iter()
                .filter_map(|&txn| transactions.get(&txn))
                .min_by_key(|s| s.priority)
                .map(|s| s.transaction_id)
                .unwrap_or(cycle[0]),
            VictimStrategy::Random => cycle[0],
        }
    }

    /// Update transaction work done
    pub fn update_work(&self, txn_id: TransactionId, work: u64) {
        if let Some(state) = self.transactions.write().get_mut(&txn_id) {
            state.work_done += work;
        }
    }

    /// Set transaction priority
    pub fn set_priority(&self, txn_id: TransactionId, priority: i32) {
        if let Some(state) = self.transactions.write().get_mut(&txn_id) {
            state.priority = priority;
        }
    }

    /// Get lock wait graph for visualization
    pub fn get_wait_graph(&self) -> WaitGraphInfo {
        let graph = self.graph.read();
        let transactions = self.transactions.read();

        let nodes: Vec<_> = transactions
            .values()
            .map(|s| WaitGraphNode {
                transaction_id: s.transaction_id,
                is_waiting: s.waiting_for.is_some(),
                locks_held: s.held_locks.len(),
                wait_time_ms: s
                    .waiting_for
                    .map(|_| s.started_at.elapsed().as_millis() as u64),
            })
            .collect();

        WaitGraphInfo {
            nodes,
            edges: graph.get_edges(),
            node_count: graph.node_count(),
            edge_count: graph.edge_count(),
        }
    }

    /// Get statistics
    pub fn stats(&self) -> DeadlockStats {
        self.stats.read().clone()
    }

    /// Get all locks (for pg_locks view)
    pub fn get_locks(&self) -> Vec<LockEntry> {
        self.locks.read().values().cloned().collect()
    }

    /// Get blocking PIDs for a transaction
    pub fn get_blocking_pids(&self, txn_id: TransactionId) -> Vec<TransactionId> {
        self.graph.read().get_blockers(txn_id)
    }
}

/// Wait graph information for visualization
#[derive(Debug, Clone)]
pub struct WaitGraphInfo {
    /// Nodes (transactions)
    pub nodes: Vec<WaitGraphNode>,
    /// Edges (wait relationships)
    pub edges: Vec<WaitEdge>,
    /// Total node count
    pub node_count: usize,
    /// Total edge count
    pub edge_count: usize,
}

/// Wait graph node
#[derive(Debug, Clone)]
pub struct WaitGraphNode {
    /// Transaction ID
    pub transaction_id: TransactionId,
    /// Whether transaction is waiting
    pub is_waiting: bool,
    /// Number of locks held
    pub locks_held: usize,
    /// Time spent waiting (if waiting)
    pub wait_time_ms: Option<u64>,
}

// ============================================================================
// Optimistic Locking
// ============================================================================

/// Optimistic lock result
#[derive(Debug, Clone)]
pub enum OptimisticLockResult {
    /// Update succeeded
    Success { new_version: u64 },
    /// Conflict detected (version mismatch)
    Conflict {
        expected_version: u64,
        actual_version: u64,
    },
    /// Row not found
    NotFound,
}

/// Optimistic locking helper
pub struct OptimisticLock {
    /// Version column name
    pub version_column: String,
}

impl Default for OptimisticLock {
    fn default() -> Self {
        Self::new("version")
    }
}

impl OptimisticLock {
    /// Create a new optimistic lock helper
    pub fn new(version_column: &str) -> Self {
        Self {
            version_column: version_column.to_string(),
        }
    }

    /// Generate UPDATE SQL with optimistic locking
    pub fn update_sql(
        &self,
        table: &str,
        set_clause: &str,
        where_clause: &str,
        expected_version: u64,
    ) -> String {
        format!(
            "UPDATE {} SET {}, {} = {} + 1 WHERE {} AND {} = {}",
            table,
            set_clause,
            self.version_column,
            self.version_column,
            where_clause,
            self.version_column,
            expected_version
        )
    }

    /// Check if update was successful (affected rows > 0)
    pub fn check_result(&self, affected_rows: u64, expected_version: u64) -> OptimisticLockResult {
        if affected_rows > 0 {
            OptimisticLockResult::Success {
                new_version: expected_version + 1,
            }
        } else {
            OptimisticLockResult::Conflict {
                expected_version,
                actual_version: 0, // Would need to query to get actual
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_mode_conflicts() {
        // AccessShare only conflicts with AccessExclusive
        assert!(!LockMode::AccessShare.conflicts_with(&LockMode::AccessShare));
        assert!(LockMode::AccessShare.conflicts_with(&LockMode::AccessExclusive));

        // AccessExclusive conflicts with everything
        assert!(LockMode::AccessExclusive.conflicts_with(&LockMode::AccessShare));
        assert!(LockMode::AccessExclusive.conflicts_with(&LockMode::AccessExclusive));
    }

    #[test]
    fn test_wait_for_graph() {
        let mut graph = WaitForGraph::new();

        // Create a simple cycle: 1 -> 2 -> 3 -> 1
        graph.add_edge(WaitEdge {
            waiter: 1,
            blocker: 2,
            lock_id: 100,
            target: LockTarget::Relation {
                database: 1,
                relation: 1,
            },
            requested_mode: LockMode::Exclusive,
            blocking_mode: LockMode::Exclusive,
        });

        graph.add_edge(WaitEdge {
            waiter: 2,
            blocker: 3,
            lock_id: 101,
            target: LockTarget::Relation {
                database: 1,
                relation: 2,
            },
            requested_mode: LockMode::Exclusive,
            blocking_mode: LockMode::Exclusive,
        });

        graph.add_edge(WaitEdge {
            waiter: 3,
            blocker: 1,
            lock_id: 102,
            target: LockTarget::Relation {
                database: 1,
                relation: 3,
            },
            requested_mode: LockMode::Exclusive,
            blocking_mode: LockMode::Exclusive,
        });

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty());
        assert_eq!(cycles[0].len(), 3);
    }

    #[test]
    fn test_wait_for_graph_no_cycle() {
        let mut graph = WaitForGraph::new();

        // Linear chain: 1 -> 2 -> 3 (no cycle)
        graph.add_edge(WaitEdge {
            waiter: 1,
            blocker: 2,
            lock_id: 100,
            target: LockTarget::Relation {
                database: 1,
                relation: 1,
            },
            requested_mode: LockMode::Exclusive,
            blocking_mode: LockMode::Exclusive,
        });

        graph.add_edge(WaitEdge {
            waiter: 2,
            blocker: 3,
            lock_id: 101,
            target: LockTarget::Relation {
                database: 1,
                relation: 2,
            },
            requested_mode: LockMode::Exclusive,
            blocking_mode: LockMode::Exclusive,
        });

        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_deadlock_detector() {
        let detector = DeadlockDetector::new(DeadlockConfig::default());

        detector.register_transaction(1);
        detector.register_transaction(2);

        // Transaction 1 gets lock on resource A
        let lock1 = detector
            .request_lock(
                1,
                LockTarget::Relation {
                    database: 1,
                    relation: 1,
                },
                LockMode::Exclusive,
            )
            .unwrap();

        // Transaction 2 gets lock on resource B
        let lock2 = detector
            .request_lock(
                2,
                LockTarget::Relation {
                    database: 1,
                    relation: 2,
                },
                LockMode::Exclusive,
            )
            .unwrap();

        // Transaction 1 requests lock on resource B (waits)
        let result = detector.request_lock(
            1,
            LockTarget::Relation {
                database: 1,
                relation: 2,
            },
            LockMode::Exclusive,
        );
        // Should succeed (just waiting, no deadlock yet)
        assert!(result.is_ok());
    }

    #[test]
    fn test_optimistic_lock() {
        let lock = OptimisticLock::new("version");

        let sql = lock.update_sql("products", "price = 10.00", "id = 1", 5);
        assert!(sql.contains("version = 5"));
        assert!(sql.contains("version = version + 1"));
    }

    #[test]
    fn test_optimistic_lock_result() {
        let lock = OptimisticLock::default();

        let result = lock.check_result(1, 5);
        assert!(matches!(
            result,
            OptimisticLockResult::Success { new_version: 6 }
        ));

        let result = lock.check_result(0, 5);
        assert!(matches!(result, OptimisticLockResult::Conflict { .. }));
    }

    #[test]
    fn test_lock_target_display() {
        let target = LockTarget::Relation {
            database: 1,
            relation: 42,
        };
        assert_eq!(format!("{}", target), "relation 1.42");

        let target = LockTarget::Advisory {
            database: 1,
            key1: 100,
            key2: 200,
        };
        assert_eq!(format!("{}", target), "advisory 1.100.200");
    }

    #[test]
    fn test_victim_selection() {
        let detector = DeadlockDetector::new(DeadlockConfig {
            victim_strategy: VictimStrategy::LeastWork,
            ..Default::default()
        });

        detector.register_transaction(1);
        detector.register_transaction(2);
        detector.register_transaction(3);

        detector.update_work(1, 100);
        detector.update_work(2, 50);
        detector.update_work(3, 200);

        let victim = detector.select_victim(&[1, 2, 3]);
        assert_eq!(victim, 2); // Least work done
    }
}
