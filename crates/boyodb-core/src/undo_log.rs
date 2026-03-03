//! Undo Log for Transaction Rollback Support
//!
//! This module provides undo logging for supporting:
//! - Transaction rollback (full or to savepoint)
//! - Crash recovery (reconstructing pre-crash state)
//! - Point-in-time recovery

use crate::engine::EngineError;
use crate::transaction::TransactionId;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Types of operations that can be undone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UndoRecord {
    /// Undo an INSERT by recording that a row was inserted
    /// (rollback will delete it)
    Insert {
        database: String,
        table: String,
        row_id: u64,
    },

    /// Undo a DELETE by recording the deleted row data
    /// (rollback will re-insert it)
    Delete {
        database: String,
        table: String,
        row_id: u64,
        /// Serialized row data (Arrow IPC format)
        row_data: Vec<u8>,
    },

    /// Undo an UPDATE by recording the old values
    /// (rollback will restore them)
    Update {
        database: String,
        table: String,
        row_id: u64,
        /// Column name -> old value (serialized)
        old_values: HashMap<String, Vec<u8>>,
    },

    /// Undo a table creation
    CreateTable { database: String, table: String },

    /// Undo a table drop by recording the table metadata
    DropTable {
        database: String,
        table: String,
        /// Serialized table metadata and schema
        metadata: Vec<u8>,
    },

    /// Undo an index creation
    CreateIndex {
        database: String,
        table: String,
        index_name: String,
    },

    /// Undo an index drop
    DropIndex {
        database: String,
        table: String,
        index_name: String,
        /// Serialized index metadata
        metadata: Vec<u8>,
    },

    /// Undo a constraint addition
    AddConstraint {
        database: String,
        table: String,
        constraint_name: String,
    },

    /// Undo a constraint removal
    DropConstraint {
        database: String,
        table: String,
        constraint_name: String,
        /// Serialized constraint definition
        definition: Vec<u8>,
    },

    /// Undo a sequence modification
    SequenceAdvance {
        database: String,
        sequence_name: String,
        old_value: i64,
    },

    /// Batch undo for multiple rows (for efficiency)
    BatchInsert {
        database: String,
        table: String,
        row_ids: Vec<u64>,
    },

    /// Batch delete undo
    BatchDelete {
        database: String,
        table: String,
        /// Serialized batch data (Arrow IPC format)
        batch_data: Vec<u8>,
    },

    /// Undo segment-level operations by storing the entire segment data
    /// Used for efficient rollback of UPDATE/DELETE on segments
    SegmentData {
        segment_id: String,
        database: String,
        table: String,
        /// Original segment data (Arrow IPC format)
        data: Vec<u8>,
    },

    /// Undo an INSERT by recording the new segment ID
    /// (rollback will delete this segment)
    SegmentDelete {
        segment_id: String,
        database: String,
        table: String,
    },
}

impl UndoRecord {
    /// Get the database this record affects
    pub fn database(&self) -> &str {
        match self {
            UndoRecord::Insert { database, .. }
            | UndoRecord::Delete { database, .. }
            | UndoRecord::Update { database, .. }
            | UndoRecord::CreateTable { database, .. }
            | UndoRecord::DropTable { database, .. }
            | UndoRecord::CreateIndex { database, .. }
            | UndoRecord::DropIndex { database, .. }
            | UndoRecord::AddConstraint { database, .. }
            | UndoRecord::DropConstraint { database, .. }
            | UndoRecord::SequenceAdvance { database, .. }
            | UndoRecord::BatchInsert { database, .. }
            | UndoRecord::BatchDelete { database, .. }
            | UndoRecord::SegmentData { database, .. }
            | UndoRecord::SegmentDelete { database, .. } => database,
        }
    }

    /// Get the table this record affects (if applicable)
    pub fn table(&self) -> Option<&str> {
        match self {
            UndoRecord::Insert { table, .. }
            | UndoRecord::Delete { table, .. }
            | UndoRecord::Update { table, .. }
            | UndoRecord::CreateTable { table, .. }
            | UndoRecord::DropTable { table, .. }
            | UndoRecord::CreateIndex { table, .. }
            | UndoRecord::DropIndex { table, .. }
            | UndoRecord::AddConstraint { table, .. }
            | UndoRecord::DropConstraint { table, .. }
            | UndoRecord::BatchInsert { table, .. }
            | UndoRecord::BatchDelete { table, .. }
            | UndoRecord::SegmentData { table, .. }
            | UndoRecord::SegmentDelete { table, .. } => Some(table),
            UndoRecord::SequenceAdvance { .. } => None,
        }
    }

    /// Estimate the memory size of this record
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                UndoRecord::Insert {
                    database, table, ..
                } => database.len() + table.len(),
                UndoRecord::Delete {
                    database,
                    table,
                    row_data,
                    ..
                } => database.len() + table.len() + row_data.len(),
                UndoRecord::Update {
                    database,
                    table,
                    old_values,
                    ..
                } => {
                    database.len()
                        + table.len()
                        + old_values
                            .iter()
                            .map(|(k, v)| k.len() + v.len())
                            .sum::<usize>()
                }
                UndoRecord::CreateTable { database, table } => database.len() + table.len(),
                UndoRecord::DropTable {
                    database,
                    table,
                    metadata,
                } => database.len() + table.len() + metadata.len(),
                UndoRecord::CreateIndex {
                    database,
                    table,
                    index_name,
                } => database.len() + table.len() + index_name.len(),
                UndoRecord::DropIndex {
                    database,
                    table,
                    index_name,
                    metadata,
                } => database.len() + table.len() + index_name.len() + metadata.len(),
                UndoRecord::AddConstraint {
                    database,
                    table,
                    constraint_name,
                } => database.len() + table.len() + constraint_name.len(),
                UndoRecord::DropConstraint {
                    database,
                    table,
                    constraint_name,
                    definition,
                } => database.len() + table.len() + constraint_name.len() + definition.len(),
                UndoRecord::SequenceAdvance {
                    database,
                    sequence_name,
                    ..
                } => database.len() + sequence_name.len(),
                UndoRecord::BatchInsert {
                    database,
                    table,
                    row_ids,
                } => database.len() + table.len() + row_ids.len() * 8,
                UndoRecord::BatchDelete {
                    database,
                    table,
                    batch_data,
                } => database.len() + table.len() + batch_data.len(),
                UndoRecord::SegmentData {
                    segment_id,
                    database,
                    table,
                    data,
                } => segment_id.len() + database.len() + table.len() + data.len(),
                UndoRecord::SegmentDelete {
                    segment_id,
                    database,
                    table,
                } => segment_id.len() + database.len() + table.len(),
            }
    }
}

/// Entry in the undo log for a single transaction
#[derive(Debug)]
struct TransactionUndoLog {
    /// Transaction ID
    txn_id: TransactionId,

    /// Undo records in order (oldest first)
    records: Vec<UndoRecord>,

    /// Total size in bytes
    size_bytes: usize,
}

impl TransactionUndoLog {
    fn new(txn_id: TransactionId) -> Self {
        TransactionUndoLog {
            txn_id,
            records: Vec::new(),
            size_bytes: 0,
        }
    }

    fn add(&mut self, record: UndoRecord) {
        self.size_bytes += record.size_bytes();
        self.records.push(record);
    }

    fn truncate_to(&mut self, position: usize) {
        if position < self.records.len() {
            // Recalculate size
            self.size_bytes = self.records[..position]
                .iter()
                .map(|r| r.size_bytes())
                .sum();
            self.records.truncate(position);
        }
    }
}

/// Configuration for the undo log
#[derive(Debug, Clone)]
pub struct UndoLogConfig {
    /// Maximum total size of undo logs in memory (bytes)
    pub max_memory_bytes: usize,

    /// Maximum records per transaction before spilling to disk
    pub max_records_per_txn: usize,

    /// Path for spilling large undo logs to disk
    pub spill_path: Option<std::path::PathBuf>,
}

impl Default for UndoLogConfig {
    fn default() -> Self {
        UndoLogConfig {
            max_memory_bytes: 256 * 1024 * 1024, // 256 MB
            max_records_per_txn: 100_000,
            spill_path: None,
        }
    }
}

/// Global undo log manager
pub struct UndoLog {
    /// Configuration
    #[allow(dead_code)]
    config: UndoLogConfig,

    /// Undo logs by transaction
    logs: RwLock<HashMap<TransactionId, TransactionUndoLog>>,

    /// Total memory used
    total_bytes: AtomicU64,

    /// Callback for applying undo operations
    /// This is set by the engine during initialization
    undo_callback:
        RwLock<Option<Box<dyn Fn(&UndoRecord) -> Result<(), EngineError> + Send + Sync>>>,
}

impl UndoLog {
    /// Create a new undo log with default config
    pub fn new() -> Self {
        Self::with_config(UndoLogConfig::default())
    }

    /// Create an undo log with custom config
    pub fn with_config(config: UndoLogConfig) -> Self {
        UndoLog {
            config,
            logs: RwLock::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
            undo_callback: RwLock::new(None),
        }
    }

    /// Set the callback for applying undo operations
    pub fn set_undo_callback<F>(&self, callback: F)
    where
        F: Fn(&UndoRecord) -> Result<(), EngineError> + Send + Sync + 'static,
    {
        *self.undo_callback.write() = Some(Box::new(callback));
    }

    /// Record an undo operation for a transaction
    pub fn record(&self, txn_id: TransactionId, record: UndoRecord) -> Result<(), EngineError> {
        let record_size = record.size_bytes();

        let mut logs = self.logs.write();
        let log = logs
            .entry(txn_id)
            .or_insert_with(|| TransactionUndoLog::new(txn_id));

        log.add(record);
        self.total_bytes
            .fetch_add(record_size as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Get the number of undo records for a transaction
    pub fn record_count(&self, txn_id: TransactionId) -> usize {
        self.logs
            .read()
            .get(&txn_id)
            .map(|l| l.records.len())
            .unwrap_or(0)
    }

    /// Get all undo records for a transaction
    pub fn get_records(&self, txn_id: TransactionId) -> Vec<UndoRecord> {
        self.logs
            .read()
            .get(&txn_id)
            .map(|l| l.records.clone())
            .unwrap_or_default()
    }

    /// Get undo records from a specific position
    pub fn get_records_from(&self, txn_id: TransactionId, position: usize) -> Vec<UndoRecord> {
        self.logs
            .read()
            .get(&txn_id)
            .map(|l| l.records[position..].to_vec())
            .unwrap_or_default()
    }

    /// Apply a single undo record
    pub fn apply_undo(&self, record: &UndoRecord) -> Result<(), EngineError> {
        let callback = self.undo_callback.read();
        if let Some(ref cb) = *callback {
            cb(record)
        } else {
            // No callback set - log and continue
            // In production, the engine sets this during initialization
            tracing::debug!("Undo callback not set, skipping undo record: {:?}", record);
            Ok(())
        }
    }

    /// Rollback all undo records for a transaction (in reverse order)
    pub fn rollback(&self, txn_id: TransactionId) -> Result<(), EngineError> {
        let records = {
            let logs = self.logs.read();
            logs.get(&txn_id)
                .map(|l| l.records.clone())
                .unwrap_or_default()
        };

        // Apply in reverse order
        for record in records.into_iter().rev() {
            self.apply_undo(&record)?;
        }

        // Clear the log
        self.clear_transaction(txn_id)?;

        Ok(())
    }

    /// Rollback to a specific position (for savepoint rollback)
    pub fn rollback_to(&self, txn_id: TransactionId, position: usize) -> Result<(), EngineError> {
        let records = {
            let logs = self.logs.read();
            logs.get(&txn_id)
                .map(|l| l.records[position..].to_vec())
                .unwrap_or_default()
        };

        // Apply in reverse order
        for record in records.into_iter().rev() {
            self.apply_undo(&record)?;
        }

        // Truncate the log
        let mut logs = self.logs.write();
        if let Some(log) = logs.get_mut(&txn_id) {
            let old_size = log.size_bytes;
            log.truncate_to(position);
            let new_size = log.size_bytes;
            self.total_bytes
                .fetch_sub((old_size - new_size) as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Clear all undo records for a transaction (after commit or full rollback)
    pub fn clear_transaction(&self, txn_id: TransactionId) -> Result<(), EngineError> {
        let mut logs = self.logs.write();
        if let Some(log) = logs.remove(&txn_id) {
            self.total_bytes
                .fetch_sub(log.size_bytes as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> UndoLogStats {
        let logs = self.logs.read();
        UndoLogStats {
            transaction_count: logs.len(),
            total_records: logs.values().map(|l| l.records.len()).sum(),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
        }
    }

    /// Get total memory usage
    pub fn memory_usage(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

impl Default for UndoLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Undo log statistics
#[derive(Debug, Clone)]
pub struct UndoLogStats {
    /// Number of transactions with undo logs
    pub transaction_count: usize,

    /// Total number of undo records
    pub total_records: usize,

    /// Total memory used in bytes
    pub total_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_record_size() {
        let record = UndoRecord::Insert {
            database: "test".to_string(),
            table: "users".to_string(),
            row_id: 1,
        };
        assert!(record.size_bytes() > 0);

        let record_with_data = UndoRecord::Delete {
            database: "test".to_string(),
            table: "users".to_string(),
            row_id: 1,
            row_data: vec![0u8; 1000],
        };
        assert!(record_with_data.size_bytes() > record.size_bytes());
    }

    #[test]
    fn test_record_and_clear() {
        let undo_log = UndoLog::new();

        let record = UndoRecord::Insert {
            database: "test".to_string(),
            table: "users".to_string(),
            row_id: 1,
        };

        undo_log.record(1, record).unwrap();
        assert_eq!(undo_log.record_count(1), 1);

        undo_log.clear_transaction(1).unwrap();
        assert_eq!(undo_log.record_count(1), 0);
    }

    #[test]
    fn test_multiple_records() {
        let undo_log = UndoLog::new();

        for i in 0..10 {
            let record = UndoRecord::Insert {
                database: "test".to_string(),
                table: "users".to_string(),
                row_id: i,
            };
            undo_log.record(1, record).unwrap();
        }

        assert_eq!(undo_log.record_count(1), 10);

        let records = undo_log.get_records(1);
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn test_rollback_to_position() {
        let undo_log = UndoLog::new();

        // Set up a simple callback that does nothing
        undo_log.set_undo_callback(|_| Ok(()));

        for i in 0..10 {
            let record = UndoRecord::Insert {
                database: "test".to_string(),
                table: "users".to_string(),
                row_id: i,
            };
            undo_log.record(1, record).unwrap();
        }

        // Rollback to position 5
        undo_log.rollback_to(1, 5).unwrap();
        assert_eq!(undo_log.record_count(1), 5);
    }

    #[test]
    fn test_multiple_transactions() {
        let undo_log = UndoLog::new();

        // Transaction 1
        undo_log
            .record(
                1,
                UndoRecord::Insert {
                    database: "test".to_string(),
                    table: "users".to_string(),
                    row_id: 1,
                },
            )
            .unwrap();

        // Transaction 2
        undo_log
            .record(
                2,
                UndoRecord::Insert {
                    database: "test".to_string(),
                    table: "orders".to_string(),
                    row_id: 1,
                },
            )
            .unwrap();

        assert_eq!(undo_log.record_count(1), 1);
        assert_eq!(undo_log.record_count(2), 1);

        let stats = undo_log.stats();
        assert_eq!(stats.transaction_count, 2);
        assert_eq!(stats.total_records, 2);
    }

    #[test]
    fn test_undo_callback() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let undo_log = UndoLog::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        undo_log.set_undo_callback(move |_| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        for i in 0..5 {
            let record = UndoRecord::Insert {
                database: "test".to_string(),
                table: "users".to_string(),
                row_id: i,
            };
            undo_log.record(1, record).unwrap();
        }

        undo_log.rollback(1).unwrap();

        // All 5 records should have been undone
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }
}
