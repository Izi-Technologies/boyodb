//! Multi-Version Concurrency Control (MVCC) Implementation
//!
//! This module provides snapshot isolation and visibility rules for:
//! - Consistent reads across transactions
//! - Conflict detection for serializable isolation
//! - Version management for concurrent access

use crate::engine::EngineError;
use crate::transaction::{IsolationLevel, TransactionId};

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// A snapshot of the database at a specific point in time
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Version number at snapshot creation
    pub version: u64,

    /// Transaction ID that owns this snapshot
    pub txn_id: TransactionId,

    /// Transactions that were active when snapshot was taken
    /// (their changes should not be visible)
    pub active_txn_ids: HashSet<TransactionId>,

    /// Minimum active transaction ID (for GC)
    pub min_active_txn_id: TransactionId,

    /// Isolation level of the owning transaction
    pub isolation_level: IsolationLevel,
}

impl Snapshot {
    /// Check if a version created by a transaction is visible
    pub fn is_visible(&self, created_txn: TransactionId, created_version: u64) -> bool {
        // Own changes are always visible
        if created_txn == self.txn_id {
            return true;
        }

        // Changes from active transactions at snapshot time are not visible
        if self.active_txn_ids.contains(&created_txn) {
            return false;
        }

        // Changes created after this snapshot are not visible
        if created_version > self.version {
            return false;
        }

        // All other committed changes are visible
        true
    }

    /// Check if a deleted version should be considered deleted
    pub fn is_deleted(
        &self,
        deleted_txn: Option<TransactionId>,
        deleted_version: Option<u64>,
    ) -> bool {
        match (deleted_txn, deleted_version) {
            (None, _) | (_, None) => false,
            (Some(del_txn), Some(del_ver)) => {
                // Own deletes are visible
                if del_txn == self.txn_id {
                    return true;
                }

                // Deletes from active transactions are not visible
                if self.active_txn_ids.contains(&del_txn) {
                    return false;
                }

                // Deletes after snapshot are not visible
                if del_ver > self.version {
                    return false;
                }

                // All other committed deletes are visible
                true
            }
        }
    }
}

/// Information about an active transaction
#[derive(Debug)]
struct TransactionInfo {
    /// Transaction ID
    id: TransactionId,

    /// Version when transaction started
    start_version: u64,

    /// Isolation level
    isolation_level: IsolationLevel,

    /// Set of rows read (for serializable validation)
    read_set: HashSet<RowKey>,

    /// Set of rows written
    write_set: HashSet<RowKey>,

    /// Commit version (if committed)
    commit_version: Option<u64>,
}

/// Key identifying a row for conflict detection
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowKey {
    pub database: String,
    pub table: String,
    pub row_id: Vec<u8>,
}

impl RowKey {
    pub fn new(database: &str, table: &str, row_id: &[u8]) -> Self {
        RowKey {
            database: database.to_string(),
            table: table.to_string(),
            row_id: row_id.to_vec(),
        }
    }
}

/// MVCC manager handling all version visibility and conflict detection
pub struct MvccManager {
    /// Current global version
    current_version: AtomicU64,

    /// Active transactions
    active_transactions: RwLock<HashMap<TransactionId, TransactionInfo>>,

    /// Recently committed transactions (for conflict detection)
    /// Maps txn_id -> (commit_version, write_set)
    committed_transactions: RwLock<HashMap<TransactionId, (u64, HashSet<RowKey>)>>,

    /// Minimum version to retain (for garbage collection)
    min_retained_version: AtomicU64,

    /// Maximum committed transactions to keep for conflict detection
    max_committed_history: usize,
}

impl MvccManager {
    /// Create a new MVCC manager
    pub fn new() -> Self {
        MvccManager {
            current_version: AtomicU64::new(1),
            active_transactions: RwLock::new(HashMap::new()),
            committed_transactions: RwLock::new(HashMap::new()),
            min_retained_version: AtomicU64::new(1),
            max_committed_history: 10000,
        }
    }

    /// Get the current global version
    pub fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::SeqCst)
    }

    /// Register a new transaction
    pub fn register_transaction(
        &self,
        txn_id: TransactionId,
        start_version: u64,
        isolation_level: IsolationLevel,
    ) {
        let info = TransactionInfo {
            id: txn_id,
            start_version,
            isolation_level,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            commit_version: None,
        };

        let mut active = self.active_transactions.write();
        active.insert(txn_id, info);
    }

    /// Create a snapshot for a transaction
    pub fn create_snapshot(&self, txn_id: TransactionId) -> Result<Snapshot, EngineError> {
        let active = self.active_transactions.read();
        let txn = active
            .get(&txn_id)
            .ok_or_else(|| EngineError::NotFound(format!("Transaction {} not found", txn_id)))?;

        // Collect active transaction IDs
        let active_txn_ids: HashSet<TransactionId> =
            active.keys().filter(|&&id| id != txn_id).cloned().collect();

        let min_active = active.keys().min().cloned().unwrap_or(txn_id);

        Ok(Snapshot {
            version: txn.start_version,
            txn_id,
            active_txn_ids,
            min_active_txn_id: min_active,
            isolation_level: txn.isolation_level,
        })
    }

    /// Record that a transaction read a row
    pub fn record_read(&self, txn_id: TransactionId, key: RowKey) {
        let mut active = self.active_transactions.write();
        if let Some(txn) = active.get_mut(&txn_id) {
            // Only track for serializable transactions
            if txn.isolation_level == IsolationLevel::Serializable {
                txn.read_set.insert(key);
            }
        }
    }

    /// Record that a transaction wrote a row
    pub fn record_write(&self, txn_id: TransactionId, key: RowKey) {
        let mut active = self.active_transactions.write();
        if let Some(txn) = active.get_mut(&txn_id) {
            txn.write_set.insert(key);
        }
    }

    /// Validate serializable isolation (check for conflicts)
    pub fn validate_serializable(&self, txn_id: TransactionId) -> Result<(), String> {
        let active = self.active_transactions.read();
        let txn = match active.get(&txn_id) {
            Some(t) => t,
            None => return Err(format!("Transaction {} not found", txn_id)),
        };

        if txn.isolation_level != IsolationLevel::Serializable {
            return Ok(());
        }

        // Check if any of our reads were written by concurrent transactions
        let committed = self.committed_transactions.read();

        for (other_txn_id, (commit_version, write_set)) in committed.iter() {
            // Skip if this transaction committed before we started
            if *commit_version <= txn.start_version {
                continue;
            }

            // Skip our own transaction
            if *other_txn_id == txn_id {
                continue;
            }

            // Check for read-write conflict (our reads vs their writes)
            for read_key in &txn.read_set {
                if write_set.contains(read_key) {
                    return Err(format!(
                        "Serialization conflict: transaction {} read {:?}.{} which was written by transaction {}",
                        txn_id, read_key.table, String::from_utf8_lossy(&read_key.row_id), other_txn_id
                    ));
                }
            }

            // Check for write-write conflict (our writes vs their writes)
            for write_key in &txn.write_set {
                if write_set.contains(write_key) {
                    return Err(format!(
                        "Serialization conflict: transaction {} wrote {:?}.{} which was also written by transaction {}",
                        txn_id, write_key.table, String::from_utf8_lossy(&write_key.row_id), other_txn_id
                    ));
                }
            }
        }

        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: TransactionId, commit_version: u64) {
        // Remove from active and add to committed
        let write_set = {
            let mut active = self.active_transactions.write();
            active
                .remove(&txn_id)
                .map(|t| t.write_set)
                .unwrap_or_default()
        };

        if !write_set.is_empty() {
            let mut committed = self.committed_transactions.write();
            committed.insert(txn_id, (commit_version, write_set));

            // Clean up old committed transactions - always check, not just when over limit
            // Use SeqCst ordering for proper synchronization
            let min_version = self.min_retained_version.load(Ordering::SeqCst);
            let len_before = committed.len();

            // Aggressive cleanup: remove entries older than min_version
            committed.retain(|_, (v, _)| *v >= min_version);

            // If we're still over limit after version-based cleanup, remove oldest entries
            if committed.len() > self.max_committed_history {
                // Find the median version and remove everything below it
                let mut versions: Vec<u64> = committed.values().map(|(v, _)| *v).collect();
                versions.sort_unstable();
                if let Some(&cutoff) = versions.get(versions.len() / 2) {
                    committed.retain(|_, (v, _)| *v >= cutoff);
                }
            }

            if len_before != committed.len() {
                tracing::debug!(
                    "MVCC cleanup: {} -> {} committed transactions",
                    len_before,
                    committed.len()
                );
            }
        }

        // Update minimum retained version
        self.update_min_retained_version();
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, txn_id: TransactionId) {
        let mut active = self.active_transactions.write();
        active.remove(&txn_id);

        self.update_min_retained_version();
    }

    /// Update the minimum retained version for garbage collection
    fn update_min_retained_version(&self) {
        let active = self.active_transactions.read();
        let min_version = active
            .values()
            .map(|t| t.start_version)
            .min()
            .unwrap_or_else(|| self.current_version.load(Ordering::SeqCst));

        self.min_retained_version
            .store(min_version, Ordering::SeqCst);
    }

    /// Get the minimum retained version (for garbage collection)
    pub fn min_retained_version(&self) -> u64 {
        self.min_retained_version.load(Ordering::SeqCst)
    }

    /// Check if a row version is visible to a transaction
    pub fn is_visible(
        &self,
        txn_id: TransactionId,
        created_txn: TransactionId,
        created_version: u64,
        deleted_txn: Option<TransactionId>,
        deleted_version: Option<u64>,
    ) -> bool {
        let active = self.active_transactions.read();
        let txn = match active.get(&txn_id) {
            Some(t) => t,
            None => return false,
        };

        // Build active transaction set
        let active_txn_ids: HashSet<TransactionId> =
            active.keys().filter(|&&id| id != txn_id).cloned().collect();

        // Check creation visibility
        let creation_visible = if created_txn == txn_id {
            true
        } else if active_txn_ids.contains(&created_txn) {
            false
        } else {
            created_version <= txn.start_version
        };

        if !creation_visible {
            return false;
        }

        // Check deletion visibility
        match (deleted_txn, deleted_version) {
            (None, _) | (_, None) => true, // Not deleted
            (Some(del_txn), Some(del_ver)) => {
                if del_txn == txn_id {
                    false // We deleted it
                } else if active_txn_ids.contains(&del_txn) {
                    true // Delete not yet visible
                } else {
                    del_ver > txn.start_version // Delete after our snapshot
                }
            }
        }
    }

    /// Get statistics about MVCC state
    pub fn stats(&self) -> MvccStats {
        let active = self.active_transactions.read();
        let committed = self.committed_transactions.read();

        MvccStats {
            current_version: self.current_version.load(Ordering::SeqCst),
            active_transaction_count: active.len(),
            committed_transaction_count: committed.len(),
            min_retained_version: self.min_retained_version.load(Ordering::SeqCst),
            total_read_set_size: active.values().map(|t| t.read_set.len()).sum(),
            total_write_set_size: active.values().map(|t| t.write_set.len()).sum(),
        }
    }
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::new()
    }
}

/// MVCC statistics
#[derive(Debug, Clone)]
pub struct MvccStats {
    /// Current global version
    pub current_version: u64,

    /// Number of active transactions
    pub active_transaction_count: usize,

    /// Number of committed transactions in history
    pub committed_transaction_count: usize,

    /// Minimum version retained for visibility
    pub min_retained_version: u64,

    /// Total size of read sets (for serializable txns)
    pub total_read_set_size: usize,

    /// Total size of write sets
    pub total_write_set_size: usize,
}

/// Helper trait for checking visibility of manifest entries
pub trait MvccVisibility {
    /// Get the transaction that created this entry
    fn created_txn(&self) -> Option<TransactionId>;

    /// Get the version when this entry was created
    fn created_version(&self) -> u64;

    /// Get the transaction that deleted this entry (if any)
    fn deleted_txn(&self) -> Option<TransactionId>;

    /// Get the version when this entry was deleted (if any)
    fn deleted_version(&self) -> Option<u64>;

    /// Check if this entry is visible to a snapshot
    fn is_visible_to(&self, snapshot: &Snapshot) -> bool {
        // Entries without created_txn were created outside of any transaction
        // and are automatically visible to all transactions (they're committed)
        let created_txn = match self.created_txn() {
            Some(txn) => txn,
            None => {
                // No transaction context - check if deleted
                return !snapshot.is_deleted(self.deleted_txn(), self.deleted_version());
            }
        };

        let created_version = self.created_version();

        // Check if creation is visible
        let creation_visible = snapshot.is_visible(created_txn, created_version);
        if !creation_visible {
            return false;
        }

        // Check if deletion is visible
        !snapshot.is_deleted(self.deleted_txn(), self.deleted_version())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_visibility() {
        let mut active = HashSet::new();
        active.insert(2);
        active.insert(3);

        let snapshot = Snapshot {
            version: 10,
            txn_id: 1,
            active_txn_ids: active,
            min_active_txn_id: 1,
            isolation_level: IsolationLevel::RepeatableRead,
        };

        // Own changes visible
        assert!(snapshot.is_visible(1, 5));

        // Committed changes before snapshot visible
        assert!(snapshot.is_visible(0, 5));

        // Changes from active transactions not visible
        assert!(!snapshot.is_visible(2, 5));
        assert!(!snapshot.is_visible(3, 8));

        // Changes after snapshot not visible
        assert!(!snapshot.is_visible(0, 15));
    }

    #[test]
    fn test_snapshot_deletion_visibility() {
        let active = HashSet::new();

        let snapshot = Snapshot {
            version: 10,
            txn_id: 1,
            active_txn_ids: active,
            min_active_txn_id: 1,
            isolation_level: IsolationLevel::RepeatableRead,
        };

        // No deletion
        assert!(!snapshot.is_deleted(None, None));

        // Own deletion visible
        assert!(snapshot.is_deleted(Some(1), Some(5)));

        // Deletion before snapshot visible
        assert!(snapshot.is_deleted(Some(0), Some(5)));

        // Deletion after snapshot not visible
        assert!(!snapshot.is_deleted(Some(0), Some(15)));
    }

    #[test]
    fn test_mvcc_manager_basic() {
        let manager = MvccManager::new();

        // Register transaction
        manager.register_transaction(1, 1, IsolationLevel::ReadCommitted);

        // Create snapshot
        let snapshot = manager.create_snapshot(1).unwrap();
        assert_eq!(snapshot.txn_id, 1);
        assert_eq!(snapshot.version, 1);
    }

    #[test]
    fn test_mvcc_serializable_conflict() {
        let manager = MvccManager::new();

        // Transaction 1 starts at version 1
        manager.register_transaction(1, 1, IsolationLevel::Serializable);

        // Transaction 2 starts at version 1
        manager.register_transaction(2, 1, IsolationLevel::Serializable);

        // Transaction 1 reads row A
        let key_a = RowKey::new("db", "table", b"row_a");
        manager.record_read(1, key_a.clone());

        // Transaction 2 writes row A and commits
        manager.record_write(2, key_a);
        manager.commit_transaction(2, 2);

        // Transaction 1 should fail validation (read-write conflict)
        let result = manager.validate_serializable(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_mvcc_no_conflict_different_rows() {
        let manager = MvccManager::new();

        manager.register_transaction(1, 1, IsolationLevel::Serializable);
        manager.register_transaction(2, 1, IsolationLevel::Serializable);

        // Transaction 1 reads row A
        manager.record_read(1, RowKey::new("db", "table", b"row_a"));

        // Transaction 2 writes row B and commits
        manager.record_write(2, RowKey::new("db", "table", b"row_b"));
        manager.commit_transaction(2, 2);

        // Transaction 1 should pass validation (different rows)
        let result = manager.validate_serializable(1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mvcc_visibility() {
        let manager = MvccManager::new();

        // Transaction 1 at version 1
        manager.register_transaction(1, 1, IsolationLevel::RepeatableRead);

        // Transaction 2 at version 1
        manager.register_transaction(2, 1, IsolationLevel::RepeatableRead);

        // Row created by transaction 0 at version 0 should be visible
        assert!(manager.is_visible(1, 0, 0, None, None));

        // Row created by transaction 2 should not be visible to transaction 1
        // (transaction 2 is still active)
        assert!(!manager.is_visible(1, 2, 1, None, None));

        // Commit transaction 2
        manager.commit_transaction(2, 2);

        // Now transaction 1 can see transaction 2's committed rows
        // (but only in read committed mode - for repeatable read,
        // we use the snapshot from transaction start)
    }
}
