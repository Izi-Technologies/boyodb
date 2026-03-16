//! Transaction Manager for Financial-Grade ACID Compliance
//!
//! This module implements full ACID transaction support with:
//! - Transaction state machine (Active -> Preparing -> Committed/Aborted)
//! - Two-Phase Commit (2PC) protocol for distributed transactions
//! - Savepoint support for partial rollbacks
//! - Multiple isolation levels (Read Committed, Repeatable Read, Serializable)

use crate::engine::EngineError;
use crate::lock_manager::{LockHandle, LockManager, LockMode};
use crate::mvcc::MvccManager;
use crate::undo_log::{UndoLog, UndoRecord};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Unique identifier for a transaction
pub type TransactionId = u64;

/// Special transaction ID representing no transaction
pub const NO_TRANSACTION: TransactionId = 0;

/// Transaction isolation levels following SQL standard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Transactions see only committed data, but may see different data
    /// on repeated reads if other transactions commit in between.
    ReadCommitted,

    /// Transactions see a consistent snapshot from the start.
    /// Prevents dirty reads and non-repeatable reads.
    RepeatableRead,

    /// Full isolation - transactions appear to execute serially.
    /// Prevents all anomalies including phantom reads.
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().replace('-', " ").as_str() {
            "READ COMMITTED" | "READCOMMITTED" => Ok(IsolationLevel::ReadCommitted),
            "REPEATABLE READ" | "REPEATABLEREAD" => Ok(IsolationLevel::RepeatableRead),
            "SERIALIZABLE" => Ok(IsolationLevel::Serializable),
            _ => Err(format!("Unknown isolation level: {}", s)),
        }
    }
}

/// Transaction state machine states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction is actively processing operations
    Active,

    /// Transaction is preparing to commit (2PC phase 1)
    Preparing,

    /// Transaction has been prepared and is ready to commit
    Prepared,

    /// Transaction has been committed
    Committed,

    /// Transaction has been aborted/rolled back
    Aborted,
}

impl TransactionState {
    /// Check if transaction is still active (can accept operations)
    pub fn is_active(&self) -> bool {
        matches!(self, TransactionState::Active)
    }

    /// Check if transaction has completed (committed or aborted)
    pub fn is_completed(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed | TransactionState::Aborted
        )
    }

    /// Check if transaction can be committed
    pub fn can_commit(&self) -> bool {
        matches!(self, TransactionState::Active | TransactionState::Prepared)
    }

    /// Check if transaction can be rolled back
    pub fn can_rollback(&self) -> bool {
        matches!(
            self,
            TransactionState::Active | TransactionState::Preparing | TransactionState::Prepared
        )
    }
}

/// Savepoint within a transaction for partial rollback
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Savepoint name
    pub name: String,

    /// Number of undo records at savepoint creation
    pub undo_log_position: usize,

    /// Number of locks held at savepoint creation
    pub locks_held_count: usize,

    /// Timestamp when savepoint was created
    pub created_at: Instant,
}

/// A snapshot of transaction state for external access
#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    /// Unique transaction ID
    pub id: TransactionId,

    /// Current transaction state
    pub state: TransactionState,

    /// Isolation level for this transaction
    pub isolation_level: IsolationLevel,

    /// Version number at transaction start (for MVCC)
    pub start_version: u64,

    /// Whether this is a read-only transaction
    pub read_only: bool,

    /// Undo records for this transaction (for rollback)
    pub undo_records: Vec<UndoRecord>,
}

/// Individual transaction representation
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: TransactionId,

    /// Version number at transaction start (for MVCC)
    pub start_version: u64,

    /// Current transaction state
    pub state: TransactionState,

    /// Isolation level for this transaction
    pub isolation_level: IsolationLevel,

    /// Whether this is a read-only transaction
    pub read_only: bool,

    /// Locks currently held by this transaction
    pub locks_held: Vec<LockHandle>,

    /// Savepoints created within this transaction
    pub savepoints: Vec<Savepoint>,

    /// Undo records for this transaction
    pub undo_records: Vec<UndoRecord>,

    /// Tables modified by this transaction (for validation)
    pub modified_tables: HashSet<(String, String)>, // (database, table)

    /// Timestamp when transaction started
    pub started_at: Instant,

    /// Wall clock time when transaction started (for PITR)
    pub start_timestamp_micros: u64,

    /// Optional deadline for transaction timeout
    pub deadline: Option<Instant>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(
        id: TransactionId,
        start_version: u64,
        isolation_level: IsolationLevel,
        read_only: bool,
        timeout: Option<Duration>,
    ) -> Self {
        let now = Instant::now();
        let deadline = timeout.map(|d| now + d);

        let start_timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Transaction {
            id,
            start_version,
            state: TransactionState::Active,
            isolation_level,
            read_only,
            locks_held: Vec::new(),
            savepoints: Vec::new(),
            undo_records: Vec::new(),
            modified_tables: HashSet::new(),
            started_at: now,
            start_timestamp_micros,
            deadline,
        }
    }

    /// Check if transaction has timed out
    pub fn is_timed_out(&self) -> bool {
        self.deadline.map(|d| Instant::now() > d).unwrap_or(false)
    }

    /// Create a savepoint
    pub fn create_savepoint(&mut self, name: String) -> Result<(), EngineError> {
        if !self.state.is_active() {
            return Err(EngineError::InvalidArgument(
                "Cannot create savepoint on inactive transaction".to_string(),
            ));
        }

        // Check for duplicate savepoint names
        if self.savepoints.iter().any(|s| s.name == name) {
            // Overwrite existing savepoint (SQL standard behavior)
            self.savepoints.retain(|s| s.name != name);
        }

        self.savepoints.push(Savepoint {
            name,
            undo_log_position: self.undo_records.len(),
            locks_held_count: self.locks_held.len(),
            created_at: Instant::now(),
        });

        Ok(())
    }

    /// Release a savepoint (makes it unavailable for rollback)
    pub fn release_savepoint(&mut self, name: &str) -> Result<(), EngineError> {
        let idx = self
            .savepoints
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| EngineError::NotFound(format!("Savepoint '{}' does not exist", name)))?;

        // Remove this savepoint and all newer ones
        self.savepoints.truncate(idx);
        Ok(())
    }

    /// Get undo records since a savepoint
    pub fn get_undo_records_since_savepoint(
        &self,
        name: &str,
    ) -> Result<&[UndoRecord], EngineError> {
        let savepoint = self
            .savepoints
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| EngineError::NotFound(format!("Savepoint '{}' does not exist", name)))?;

        Ok(&self.undo_records[savepoint.undo_log_position..])
    }

    /// Record that a table was modified
    pub fn mark_table_modified(&mut self, database: &str, table: &str) {
        self.modified_tables
            .insert((database.to_string(), table.to_string()));
    }
}

/// Result of a transaction prepare operation (2PC phase 1)
#[derive(Debug)]
pub struct PrepareResult {
    /// Transaction ID
    pub txn_id: TransactionId,

    /// Whether the transaction is prepared to commit
    pub prepared: bool,

    /// Reason for failure if not prepared
    pub reason: Option<String>,
}

/// Transaction manager handling all transaction operations
pub struct TransactionManager {
    /// Active transactions by ID
    active_transactions: RwLock<HashMap<TransactionId, Transaction>>,

    /// Next transaction ID to assign
    next_txn_id: AtomicU64,

    /// Global version counter for MVCC
    global_version: AtomicU64,

    /// Lock manager for concurrency control
    lock_manager: Arc<LockManager>,

    /// Undo log for rollback support
    undo_log: Arc<UndoLog>,

    /// MVCC manager for snapshot isolation (public for query execution access)
    pub mvcc_manager: Arc<MvccManager>,

    /// Default isolation level for new transactions
    default_isolation_level: IsolationLevel,

    /// Default transaction timeout
    default_timeout: Option<Duration>,

    /// Maximum concurrent transactions
    max_transactions: usize,
}

impl std::fmt::Debug for TransactionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionManager")
            .field(
                "active_transactions",
                &self.active_transactions.read().len(),
            )
            .field("next_txn_id", &self.next_txn_id)
            .field("global_version", &self.global_version)
            .field("default_isolation_level", &self.default_isolation_level)
            .field("max_transactions", &self.max_transactions)
            .finish()
    }
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(
        lock_manager: Arc<LockManager>,
        undo_log: Arc<UndoLog>,
        mvcc_manager: Arc<MvccManager>,
    ) -> Self {
        TransactionManager {
            active_transactions: RwLock::new(HashMap::new()),
            next_txn_id: AtomicU64::new(1),
            global_version: AtomicU64::new(1),
            lock_manager,
            undo_log,
            mvcc_manager,
            default_isolation_level: IsolationLevel::ReadCommitted,
            default_timeout: Some(Duration::from_secs(60)), // 1 minute default
            max_transactions: 10000,
        }
    }

    /// Configure the transaction manager
    pub fn with_default_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.default_isolation_level = level;
        self
    }

    pub fn with_default_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.default_timeout = timeout;
        self
    }

    pub fn with_max_transactions(mut self, max: usize) -> Self {
        self.max_transactions = max;
        self
    }

    /// Get current global version
    pub fn current_version(&self) -> u64 {
        self.global_version.load(Ordering::SeqCst)
    }

    /// Begin a new transaction
    pub fn begin(
        &self,
        isolation_level: Option<IsolationLevel>,
        read_only: bool,
        timeout: Option<Duration>,
    ) -> Result<TransactionId, EngineError> {
        // Prepare transaction data before acquiring lock to minimize lock hold time
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_version = self.global_version.load(Ordering::SeqCst);
        let isolation = isolation_level.unwrap_or(self.default_isolation_level);
        let txn_timeout = timeout.or(self.default_timeout);

        let txn = Transaction::new(txn_id, start_version, isolation, read_only, txn_timeout);

        // Atomically check limit and insert to prevent TOCTOU race
        {
            let mut active = self.active_transactions.write();
            if active.len() >= self.max_transactions {
                return Err(EngineError::Internal(format!(
                    "Maximum concurrent transactions ({}) exceeded",
                    self.max_transactions
                )));
            }
            active.insert(txn_id, txn);
        }

        // Register transaction with MVCC manager (after successful insertion)
        self.mvcc_manager
            .register_transaction(txn_id, start_version, isolation);

        tracing::debug!(
            "Transaction {} started with isolation level {} (version {})",
            txn_id,
            isolation,
            start_version
        );

        Ok(txn_id)
    }

    /// Get a reference to a transaction (for read operations)
    pub fn get_transaction(&self, txn_id: TransactionId) -> Option<TransactionSnapshot> {
        self.active_transactions
            .read()
            .get(&txn_id)
            .map(|t| TransactionSnapshot {
                id: t.id,
                state: t.state,
                isolation_level: t.isolation_level,
                start_version: t.start_version,
                read_only: t.read_only,
                undo_records: t.undo_records.clone(),
            })
    }

    /// Get just the transaction state
    pub fn get_transaction_state(&self, txn_id: TransactionId) -> Option<TransactionState> {
        self.active_transactions
            .read()
            .get(&txn_id)
            .map(|t| t.state)
    }

    /// Check if a transaction exists and is active
    pub fn is_active(&self, txn_id: TransactionId) -> bool {
        self.active_transactions
            .read()
            .get(&txn_id)
            .map(|t| t.state.is_active())
            .unwrap_or(false)
    }

    /// Get transaction's start version
    pub fn get_start_version(&self, txn_id: TransactionId) -> Option<u64> {
        self.active_transactions
            .read()
            .get(&txn_id)
            .map(|t| t.start_version)
    }

    /// Get transaction's isolation level
    pub fn get_isolation_level(&self, txn_id: TransactionId) -> Option<IsolationLevel> {
        self.active_transactions
            .read()
            .get(&txn_id)
            .map(|t| t.isolation_level)
    }

    /// Record an undo operation for a transaction
    pub fn record_undo(
        &self,
        txn_id: TransactionId,
        record: UndoRecord,
    ) -> Result<(), EngineError> {
        let mut active = self.active_transactions.write();
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        if !txn.state.is_active() {
            return Err(EngineError::InvalidArgument(format!(
                "Transaction {} is not active (state: {:?})",
                txn_id, txn.state
            )));
        }

        if txn.read_only {
            return Err(EngineError::InvalidArgument(
                "Cannot modify data in read-only transaction".to_string(),
            ));
        }

        // Also record in global undo log for crash recovery
        self.undo_log.record(txn_id, record.clone())?;

        txn.undo_records.push(record);
        Ok(())
    }

    /// Create a savepoint in a transaction
    pub fn savepoint(&self, txn_id: TransactionId, name: String) -> Result<(), EngineError> {
        let mut active = self.active_transactions.write();
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        txn.create_savepoint(name)
    }

    /// Release a savepoint in a transaction
    pub fn release_savepoint(&self, txn_id: TransactionId, name: &str) -> Result<(), EngineError> {
        let mut active = self.active_transactions.write();
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        txn.release_savepoint(name)
    }

    /// Rollback to a savepoint (partial rollback)
    pub fn rollback_to_savepoint(
        &self,
        txn_id: TransactionId,
        savepoint_name: &str,
    ) -> Result<(), EngineError> {
        // Phase 1: Validate and extract data to process (holding the lock)
        let (undo_records, locks_to_release) = {
            let mut active = self.active_transactions.write();
            let txn = active.get_mut(&txn_id).ok_or_else(|| {
                EngineError::NotFound(format!("Transaction {} not found", txn_id))
            })?;

            if !txn.state.is_active() {
                return Err(EngineError::InvalidArgument(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }

            let savepoint = txn
                .savepoints
                .iter()
                .find(|s| s.name == savepoint_name)
                .ok_or_else(|| {
                    EngineError::NotFound(format!("Savepoint '{}' does not exist", savepoint_name))
                })?
                .clone();

            // Get undo records to apply
            let undo_records: Vec<UndoRecord> =
                txn.undo_records[savepoint.undo_log_position..].to_vec();

            // Get locks to release (those acquired after savepoint)
            let locks_to_release: Vec<LockHandle> =
                txn.locks_held[savepoint.locks_held_count..].to_vec();

            // Truncate transaction state to savepoint
            txn.undo_records.truncate(savepoint.undo_log_position);
            txn.locks_held.truncate(savepoint.locks_held_count);

            // Remove savepoints created after this one
            let idx = txn
                .savepoints
                .iter()
                .position(|s| s.name == savepoint_name)
                .unwrap();
            txn.savepoints.truncate(idx + 1);

            (undo_records, locks_to_release)
        };
        // active_transactions lock is now released

        // Phase 2: Apply undo records (outside the active_transactions lock)
        for record in undo_records.into_iter().rev() {
            if let Err(e) = self.undo_log.apply_undo(&record) {
                tracing::error!("Error applying undo record for txn {}: {:?}", txn_id, e);
                // Continue with rollback despite errors
            }
        }

        // Release locks acquired after savepoint
        for lock in locks_to_release {
            let _ = self.lock_manager.release(lock);
        }

        tracing::debug!(
            "Transaction {} rolled back to savepoint '{}'",
            txn_id,
            savepoint_name
        );

        Ok(())
    }

    /// Prepare transaction for commit (2PC phase 1)
    pub fn prepare(&self, txn_id: TransactionId) -> Result<PrepareResult, EngineError> {
        let mut active = self.active_transactions.write();
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        if txn.state != TransactionState::Active {
            return Err(EngineError::InvalidArgument(format!(
                "Transaction {} is not active (state: {:?})",
                txn_id, txn.state
            )));
        }

        // Check for timeout
        if txn.is_timed_out() {
            txn.state = TransactionState::Aborted;
            return Ok(PrepareResult {
                txn_id,
                prepared: false,
                reason: Some("Transaction timed out".to_string()),
            });
        }

        // Check for conflicts at serializable isolation level
        if txn.isolation_level == IsolationLevel::Serializable {
            if let Err(conflict) = self.mvcc_manager.validate_serializable(txn_id) {
                txn.state = TransactionState::Aborted;
                return Ok(PrepareResult {
                    txn_id,
                    prepared: false,
                    reason: Some(conflict),
                });
            }
        }

        txn.state = TransactionState::Preparing;

        // Validate all constraints would be satisfied
        // (This is a placeholder - actual validation happens during commit)

        txn.state = TransactionState::Prepared;

        tracing::debug!("Transaction {} prepared for commit", txn_id);

        Ok(PrepareResult {
            txn_id,
            prepared: true,
            reason: None,
        })
    }

    /// Commit a transaction
    pub fn commit(&self, txn_id: TransactionId) -> Result<u64, EngineError> {
        // Phase 1: Validate and prepare commit (holding the lock)
        let (commit_version, locks_to_release) = {
            let mut active = self.active_transactions.write();
            let txn = active.get_mut(&txn_id).ok_or_else(|| {
                EngineError::NotFound(format!("Transaction {} not found", txn_id))
            })?;

            if !txn.state.can_commit() {
                return Err(EngineError::InvalidArgument(format!(
                    "Transaction {} cannot be committed (state: {:?})",
                    txn_id, txn.state
                )));
            }

            // Check for timeout
            if txn.is_timed_out() {
                txn.state = TransactionState::Aborted;
                return Err(EngineError::Timeout(format!(
                    "Transaction {} timed out",
                    txn_id
                )));
            }

            // For unprepared transactions, do implicit prepare
            if txn.state == TransactionState::Active {
                // Check serializable conflicts
                if txn.isolation_level == IsolationLevel::Serializable {
                    if let Err(conflict) = self.mvcc_manager.validate_serializable(txn_id) {
                        txn.state = TransactionState::Aborted;
                        return Err(EngineError::Internal(format!(
                            "Serialization conflict: {}",
                            conflict
                        )));
                    }
                }
            }

            // Assign commit version
            let commit_version = self.global_version.fetch_add(1, Ordering::SeqCst) + 1;

            // Mark transaction as committed
            txn.state = TransactionState::Committed;

            // Extract locks to release (we'll release them outside the lock)
            let locks = std::mem::take(&mut txn.locks_held);

            // Remove from active transactions
            active.remove(&txn_id);

            (commit_version, locks)
        };
        // active_transactions lock is now released

        // Phase 2: Cleanup (outside the active_transactions lock to prevent deadlock)
        // Clear undo records (no longer needed after commit)
        let _ = self.undo_log.clear_transaction(txn_id);

        // Release all locks
        for lock in locks_to_release {
            let _ = self.lock_manager.release(lock);
        }

        // Notify MVCC manager
        self.mvcc_manager.commit_transaction(txn_id, commit_version);

        tracing::debug!(
            "Transaction {} committed at version {}",
            txn_id,
            commit_version
        );

        Ok(commit_version)
    }

    /// Rollback/abort a transaction
    pub fn rollback(&self, txn_id: TransactionId) -> Result<(), EngineError> {
        // Phase 1: Validate and prepare rollback (holding the lock)
        let (undo_records, locks_to_release) = {
            let mut active = self.active_transactions.write();
            let txn = active.get_mut(&txn_id).ok_or_else(|| {
                EngineError::NotFound(format!("Transaction {} not found", txn_id))
            })?;

            if !txn.state.can_rollback() {
                return Err(EngineError::InvalidArgument(format!(
                    "Transaction {} cannot be rolled back (state: {:?})",
                    txn_id, txn.state
                )));
            }

            // Extract undo records and locks
            let undo_records: Vec<UndoRecord> = std::mem::take(&mut txn.undo_records);
            let locks = std::mem::take(&mut txn.locks_held);

            // Mark as aborted
            txn.state = TransactionState::Aborted;

            // Remove from active transactions
            active.remove(&txn_id);

            (undo_records, locks)
        };
        // active_transactions lock is now released

        // Phase 2: Apply undo records (outside the active_transactions lock)
        for record in undo_records.into_iter().rev() {
            if let Err(e) = self.undo_log.apply_undo(&record) {
                tracing::error!("Error applying undo record for txn {}: {:?}", txn_id, e);
                // Continue with rollback despite errors
            }
        }

        // Clear from global undo log
        let _ = self.undo_log.clear_transaction(txn_id);

        // Release all locks
        for lock in locks_to_release {
            let _ = self.lock_manager.release(lock);
        }

        // Notify MVCC manager
        self.mvcc_manager.abort_transaction(txn_id);

        tracing::debug!("Transaction {} rolled back", txn_id);

        Ok(())
    }

    /// Acquire a lock for a transaction
    pub fn acquire_lock(
        &self,
        txn_id: TransactionId,
        database: &str,
        table: &str,
        row_key: Option<&[u8]>,
        mode: LockMode,
    ) -> Result<LockHandle, EngineError> {
        let lock_handle = self
            .lock_manager
            .acquire(txn_id, database, table, row_key, mode)?;

        let mut active = self.active_transactions.write();
        if let Some(txn) = active.get_mut(&txn_id) {
            txn.locks_held.push(lock_handle.clone());
        }

        Ok(lock_handle)
    }

    /// Mark a table as modified by a transaction
    pub fn mark_modified(
        &self,
        txn_id: TransactionId,
        database: &str,
        table: &str,
    ) -> Result<(), EngineError> {
        let mut active = self.active_transactions.write();
        let txn = active
            .get_mut(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        txn.mark_table_modified(database, table);
        Ok(())
    }

    /// Get statistics about active transactions
    pub fn stats(&self) -> TransactionStats {
        let active = self.active_transactions.read();
        let now = Instant::now();

        let mut stats = TransactionStats {
            active_count: active.len(),
            read_only_count: 0,
            oldest_age_ms: 0,
            total_locks_held: 0,
            by_isolation_level: HashMap::new(),
        };

        for txn in active.values() {
            if txn.read_only {
                stats.read_only_count += 1;
            }

            let age_ms = now.duration_since(txn.started_at).as_millis() as u64;
            if age_ms > stats.oldest_age_ms {
                stats.oldest_age_ms = age_ms;
            }

            stats.total_locks_held += txn.locks_held.len();

            *stats
                .by_isolation_level
                .entry(txn.isolation_level)
                .or_insert(0) += 1;
        }

        stats
    }

    /// Cleanup timed-out transactions
    pub fn cleanup_timed_out(&self) -> Vec<TransactionId> {
        let mut aborted = Vec::new();
        let active = self.active_transactions.read();

        let timed_out: Vec<TransactionId> = active
            .iter()
            .filter(|(_, txn)| txn.is_timed_out() && txn.state.is_active())
            .map(|(id, _)| *id)
            .collect();

        drop(active);

        for txn_id in timed_out {
            if let Ok(()) = self.rollback(txn_id) {
                aborted.push(txn_id);
                tracing::warn!("Transaction {} aborted due to timeout", txn_id);
            }
        }

        aborted
    }
}

/// Statistics about active transactions
#[derive(Debug, Clone)]
pub struct TransactionStats {
    /// Number of active transactions
    pub active_count: usize,

    /// Number of read-only transactions
    pub read_only_count: usize,

    /// Age of oldest transaction in milliseconds
    pub oldest_age_ms: u64,

    /// Total locks held by all transactions
    pub total_locks_held: usize,

    /// Count by isolation level
    pub by_isolation_level: HashMap<IsolationLevel, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> TransactionManager {
        let lock_manager = Arc::new(LockManager::new());
        let undo_log = Arc::new(UndoLog::new());
        let mvcc_manager = Arc::new(MvccManager::new());
        TransactionManager::new(lock_manager, undo_log, mvcc_manager)
    }

    #[test]
    fn test_transaction_lifecycle() {
        let manager = create_test_manager();

        // Begin transaction
        let txn_id = manager.begin(None, false, None).unwrap();
        assert!(manager.is_active(txn_id));
        assert_eq!(
            manager.get_transaction_state(txn_id),
            Some(TransactionState::Active)
        );

        // Commit transaction
        let commit_version = manager.commit(txn_id).unwrap();
        assert!(commit_version > 0);
        assert!(!manager.is_active(txn_id));
    }

    #[test]
    fn test_rollback() {
        let manager = create_test_manager();

        let txn_id = manager.begin(None, false, None).unwrap();
        assert!(manager.is_active(txn_id));

        manager.rollback(txn_id).unwrap();
        assert!(!manager.is_active(txn_id));
    }

    #[test]
    fn test_savepoints() {
        let manager = create_test_manager();

        let txn_id = manager.begin(None, false, None).unwrap();

        // Create savepoint
        manager.savepoint(txn_id, "sp1".to_string()).unwrap();

        // Create another savepoint
        manager.savepoint(txn_id, "sp2".to_string()).unwrap();

        // Rollback to first savepoint
        manager.rollback_to_savepoint(txn_id, "sp1").unwrap();

        // sp2 should no longer exist
        assert!(manager.rollback_to_savepoint(txn_id, "sp2").is_err());

        manager.commit(txn_id).unwrap();
    }

    #[test]
    fn test_isolation_levels() {
        let manager = create_test_manager();

        let txn1 = manager
            .begin(Some(IsolationLevel::ReadCommitted), false, None)
            .unwrap();
        assert_eq!(
            manager.get_isolation_level(txn1),
            Some(IsolationLevel::ReadCommitted)
        );

        let txn2 = manager
            .begin(Some(IsolationLevel::Serializable), false, None)
            .unwrap();
        assert_eq!(
            manager.get_isolation_level(txn2),
            Some(IsolationLevel::Serializable)
        );

        manager.rollback(txn1).unwrap();
        manager.rollback(txn2).unwrap();
    }

    #[test]
    fn test_read_only_transaction() {
        let manager = create_test_manager();

        let txn_id = manager.begin(None, true, None).unwrap();

        // Read-only transaction should reject write operations
        let record = UndoRecord::Insert {
            database: "test".to_string(),
            table: "users".to_string(),
            row_id: 1,
        };
        assert!(manager.record_undo(txn_id, record).is_err());

        manager.rollback(txn_id).unwrap();
    }

    #[test]
    fn test_2pc_prepare_commit() {
        let manager = create_test_manager();

        let txn_id = manager.begin(None, false, None).unwrap();

        // Prepare (phase 1)
        let result = manager.prepare(txn_id).unwrap();
        assert!(result.prepared);
        assert_eq!(
            manager.get_transaction_state(txn_id),
            Some(TransactionState::Prepared)
        );

        // Commit (phase 2)
        let commit_version = manager.commit(txn_id).unwrap();
        assert!(commit_version > 0);
    }

    #[test]
    fn test_concurrent_version_numbers() {
        let manager = create_test_manager();

        let txn1 = manager.begin(None, false, None).unwrap();
        let v1 = manager.get_start_version(txn1).unwrap();

        let txn2 = manager.begin(None, false, None).unwrap();
        let v2 = manager.get_start_version(txn2).unwrap();

        // Both should see the same version since neither committed
        assert_eq!(v1, v2);

        // Commit txn1
        let commit_v1 = manager.commit(txn1).unwrap();

        // Start new transaction
        let txn3 = manager.begin(None, false, None).unwrap();
        let v3 = manager.get_start_version(txn3).unwrap();

        // txn3 should see a higher version
        assert_eq!(v3, commit_v1);

        manager.rollback(txn2).unwrap();
        manager.rollback(txn3).unwrap();
    }
}
