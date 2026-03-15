//! Multi-Master Conflict Resolution
//!
//! This module implements conflict detection and resolution strategies
//! for multi-master replication scenarios. When the same row is modified
//! on multiple nodes, conflicts need to be detected and resolved.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum ConflictError {
    /// Resolution strategy failed
    ResolutionFailed(String),
    /// No resolution strategy configured
    NoStrategy,
    /// Invalid operation
    InvalidOperation(String),
    /// Conflict not found
    ConflictNotFound(u64),
}

impl fmt::Display for ConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConflictError::ResolutionFailed(msg) => write!(f, "Resolution failed: {}", msg),
            ConflictError::NoStrategy => write!(f, "No conflict resolution strategy configured"),
            ConflictError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            ConflictError::ConflictNotFound(id) => write!(f, "Conflict not found: {}", id),
        }
    }
}

impl std::error::Error for ConflictError {}

// ============================================================================
// Conflict Types
// ============================================================================

/// Type of conflict
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    /// Insert-Insert: Same primary key inserted on multiple nodes
    InsertInsert,
    /// Update-Update: Same row updated on multiple nodes
    UpdateUpdate,
    /// Update-Delete: Row updated on one node, deleted on another
    UpdateDelete,
    /// Delete-Update: Row deleted on one node, updated on another
    DeleteUpdate,
    /// Delete-Delete: Row deleted on multiple nodes (usually not a real conflict)
    DeleteDelete,
}

impl ConflictType {
    /// Whether this conflict type is critical
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            ConflictType::InsertInsert | ConflictType::UpdateUpdate | ConflictType::UpdateDelete
        )
    }
}

/// Origin of a change
#[derive(Debug, Clone)]
pub struct ChangeOrigin {
    /// Node ID
    pub node_id: String,
    /// Transaction ID
    pub xid: u64,
    /// Commit timestamp
    pub commit_time: u64,
    /// LSN
    pub lsn: u64,
    /// Origin name
    pub origin_name: Option<String>,
}

impl ChangeOrigin {
    pub fn new(node_id: &str, xid: u64, commit_time: u64, lsn: u64) -> Self {
        Self {
            node_id: node_id.to_string(),
            xid,
            commit_time,
            lsn,
            origin_name: None,
        }
    }
}

/// A row value in conflict
#[derive(Debug, Clone)]
pub struct ConflictRow {
    /// Table schema
    pub schema: String,
    /// Table name
    pub table: String,
    /// Primary key values
    pub primary_key: HashMap<String, ConflictValue>,
    /// Column values
    pub values: HashMap<String, ConflictValue>,
    /// Origin of this version
    pub origin: ChangeOrigin,
}

/// Value types for conflict data
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
}

impl ConflictValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ConflictValue::Null)
    }
}

/// A detected conflict
#[derive(Debug, Clone)]
pub struct Conflict {
    /// Unique conflict ID
    pub id: u64,
    /// Type of conflict
    pub conflict_type: ConflictType,
    /// Table schema
    pub schema: String,
    /// Table name
    pub table: String,
    /// Primary key values
    pub primary_key: HashMap<String, ConflictValue>,
    /// Local version (if exists)
    pub local_row: Option<ConflictRow>,
    /// Remote version (if exists)
    pub remote_row: Option<ConflictRow>,
    /// Detection time
    pub detected_at: Instant,
    /// Resolution status
    pub resolved: bool,
    /// Resolution details
    pub resolution: Option<ConflictResolution>,
}

impl Conflict {
    pub fn new(
        id: u64,
        conflict_type: ConflictType,
        schema: &str,
        table: &str,
        primary_key: HashMap<String, ConflictValue>,
    ) -> Self {
        Self {
            id,
            conflict_type,
            schema: schema.to_string(),
            table: table.to_string(),
            primary_key,
            local_row: None,
            remote_row: None,
            detected_at: Instant::now(),
            resolved: false,
            resolution: None,
        }
    }

    /// Set local row
    pub fn with_local(mut self, row: ConflictRow) -> Self {
        self.local_row = Some(row);
        self
    }

    /// Set remote row
    pub fn with_remote(mut self, row: ConflictRow) -> Self {
        self.remote_row = Some(row);
        self
    }
}

// ============================================================================
// Resolution Strategies
// ============================================================================

/// Strategy for resolving conflicts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionStrategy {
    /// Last write wins (based on timestamp)
    LastWriteWins,
    /// First write wins (based on timestamp)
    FirstWriteWins,
    /// Local changes always win
    LocalWins,
    /// Remote changes always win
    RemoteWins,
    /// Higher node ID wins
    HigherNodeWins,
    /// Lower node ID wins
    LowerNodeWins,
    /// Keep both versions (for insert-insert)
    KeepBoth,
    /// Skip the conflict (don't apply remote change)
    Skip,
    /// Error on conflict (require manual resolution)
    Error,
    /// Custom resolver function
    Custom,
}

impl ResolutionStrategy {
    /// Check if this strategy requires manual intervention
    pub fn requires_manual(&self) -> bool {
        matches!(self, ResolutionStrategy::Error | ResolutionStrategy::Custom)
    }
}

/// Outcome of conflict resolution
#[derive(Debug, Clone)]
pub struct ConflictResolution {
    /// Chosen strategy
    pub strategy: ResolutionStrategy,
    /// Winning row (if applicable)
    pub winner: Option<ConflictRow>,
    /// Additional actions taken
    pub actions: Vec<ResolutionAction>,
    /// Resolution time
    pub resolved_at: Instant,
    /// Reason for resolution
    pub reason: String,
}

/// Actions taken during resolution
#[derive(Debug, Clone)]
pub enum ResolutionAction {
    /// Applied the winning row
    Apply(ConflictRow),
    /// Skipped the change
    Skip,
    /// Deleted the row
    Delete,
    /// Logged to conflict table
    LogConflict,
    /// Notified administrator
    NotifyAdmin,
    /// Kept both versions with suffix
    KeepBoth { local_suffix: String, remote_suffix: String },
}

// ============================================================================
// Conflict Resolver
// ============================================================================

/// Configuration for conflict resolution
#[derive(Debug, Clone)]
pub struct ConflictConfig {
    /// Default resolution strategy
    pub default_strategy: ResolutionStrategy,
    /// Per-table strategies
    pub table_strategies: HashMap<String, ResolutionStrategy>,
    /// Whether to log conflicts
    pub log_conflicts: bool,
    /// Maximum conflicts to keep in history
    pub max_history: usize,
    /// Notify on critical conflicts
    pub notify_on_critical: bool,
}

impl Default for ConflictConfig {
    fn default() -> Self {
        Self {
            default_strategy: ResolutionStrategy::LastWriteWins,
            table_strategies: HashMap::new(),
            log_conflicts: true,
            max_history: 10000,
            notify_on_critical: true,
        }
    }
}

/// Statistics for conflict resolution
#[derive(Debug, Default)]
pub struct ConflictStats {
    /// Total conflicts detected
    pub conflicts_detected: AtomicU64,
    /// Conflicts resolved automatically
    pub auto_resolved: AtomicU64,
    /// Conflicts requiring manual resolution
    pub manual_required: AtomicU64,
    /// Conflicts skipped
    pub skipped: AtomicU64,
    /// Resolution errors
    pub errors: AtomicU64,
    /// By conflict type
    pub insert_insert: AtomicU64,
    pub update_update: AtomicU64,
    pub update_delete: AtomicU64,
    pub delete_update: AtomicU64,
}

impl ConflictStats {
    pub fn snapshot(&self) -> ConflictStatsSnapshot {
        ConflictStatsSnapshot {
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
            auto_resolved: self.auto_resolved.load(Ordering::Relaxed),
            manual_required: self.manual_required.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            insert_insert: self.insert_insert.load(Ordering::Relaxed),
            update_update: self.update_update.load(Ordering::Relaxed),
            update_delete: self.update_delete.load(Ordering::Relaxed),
            delete_update: self.delete_update.load(Ordering::Relaxed),
        }
    }

    fn record_conflict_type(&self, conflict_type: ConflictType) {
        match conflict_type {
            ConflictType::InsertInsert => self.insert_insert.fetch_add(1, Ordering::Relaxed),
            ConflictType::UpdateUpdate => self.update_update.fetch_add(1, Ordering::Relaxed),
            ConflictType::UpdateDelete => self.update_delete.fetch_add(1, Ordering::Relaxed),
            ConflictType::DeleteUpdate => self.delete_update.fetch_add(1, Ordering::Relaxed),
            ConflictType::DeleteDelete => 0, // Not typically counted
        };
    }
}

/// Snapshot of conflict statistics
#[derive(Debug, Clone)]
pub struct ConflictStatsSnapshot {
    pub conflicts_detected: u64,
    pub auto_resolved: u64,
    pub manual_required: u64,
    pub skipped: u64,
    pub errors: u64,
    pub insert_insert: u64,
    pub update_update: u64,
    pub update_delete: u64,
    pub delete_update: u64,
}

/// Manages conflict detection and resolution
pub struct ConflictResolver {
    /// Configuration
    config: RwLock<ConflictConfig>,
    /// Conflict history
    history: RwLock<Vec<Conflict>>,
    /// Statistics
    stats: ConflictStats,
    /// Local node ID
    local_node_id: String,
    /// Next conflict ID
    next_id: AtomicU64,
    /// Custom resolver callback
    custom_resolver: RwLock<Option<Arc<dyn Fn(&Conflict) -> Option<ConflictResolution> + Send + Sync>>>,
}

impl ConflictResolver {
    pub fn new(local_node_id: &str) -> Self {
        Self {
            config: RwLock::new(ConflictConfig::default()),
            history: RwLock::new(Vec::new()),
            stats: ConflictStats::default(),
            local_node_id: local_node_id.to_string(),
            next_id: AtomicU64::new(0),
            custom_resolver: RwLock::new(None),
        }
    }

    /// Set configuration
    pub fn set_config(&self, config: ConflictConfig) {
        *self.config.write() = config;
    }

    /// Get configuration
    pub fn config(&self) -> ConflictConfig {
        self.config.read().clone()
    }

    /// Set custom resolver
    pub fn set_custom_resolver<F>(&self, resolver: F)
    where
        F: Fn(&Conflict) -> Option<ConflictResolution> + Send + Sync + 'static,
    {
        *self.custom_resolver.write() = Some(Arc::new(resolver));
    }

    /// Detect conflict between local and remote changes
    pub fn detect_conflict(
        &self,
        local_row: Option<ConflictRow>,
        remote_row: Option<ConflictRow>,
        remote_op: RemoteOperation,
    ) -> Option<Conflict> {
        let conflict_type = match (&local_row, &remote_row, remote_op) {
            (Some(_), Some(_), RemoteOperation::Insert) => Some(ConflictType::InsertInsert),
            (Some(_), Some(_), RemoteOperation::Update) => Some(ConflictType::UpdateUpdate),
            (Some(_), None, RemoteOperation::Delete) => None, // Normal delete
            (None, Some(_), RemoteOperation::Update) => Some(ConflictType::DeleteUpdate),
            (Some(_), Some(_), RemoteOperation::Delete) => Some(ConflictType::UpdateDelete),
            _ => None,
        };

        conflict_type.map(|ct| {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let schema = local_row
                .as_ref()
                .or(remote_row.as_ref())
                .map(|r| r.schema.clone())
                .unwrap_or_default();
            let table = local_row
                .as_ref()
                .or(remote_row.as_ref())
                .map(|r| r.table.clone())
                .unwrap_or_default();
            let pk = local_row
                .as_ref()
                .or(remote_row.as_ref())
                .map(|r| r.primary_key.clone())
                .unwrap_or_default();

            let mut conflict = Conflict::new(id, ct, &schema, &table, pk);

            if let Some(row) = local_row {
                conflict = conflict.with_local(row);
            }
            if let Some(row) = remote_row {
                conflict = conflict.with_remote(row);
            }

            self.stats.conflicts_detected.fetch_add(1, Ordering::Relaxed);
            self.stats.record_conflict_type(ct);

            conflict
        })
    }

    /// Resolve a conflict
    pub fn resolve(&self, mut conflict: Conflict) -> Result<ConflictResolution, ConflictError> {
        let config = self.config.read();

        // Determine strategy
        let full_table_name = format!("{}.{}", conflict.schema, conflict.table);
        let strategy = config
            .table_strategies
            .get(&full_table_name)
            .copied()
            .unwrap_or(config.default_strategy);

        drop(config);

        // Try custom resolver first
        if strategy == ResolutionStrategy::Custom {
            if let Some(resolver) = self.custom_resolver.read().as_ref() {
                if let Some(resolution) = resolver(&conflict) {
                    conflict.resolution = Some(resolution.clone());
                    conflict.resolved = true;
                    self.store_conflict(conflict);
                    self.stats.auto_resolved.fetch_add(1, Ordering::Relaxed);
                    return Ok(resolution);
                }
            }
        }

        let resolution = self.apply_strategy(strategy, &conflict)?;

        conflict.resolution = Some(resolution.clone());
        conflict.resolved = true;
        self.store_conflict(conflict);

        if resolution.strategy == ResolutionStrategy::Skip {
            self.stats.skipped.fetch_add(1, Ordering::Relaxed);
        } else if resolution.strategy == ResolutionStrategy::Error {
            self.stats.manual_required.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.auto_resolved.fetch_add(1, Ordering::Relaxed);
        }

        Ok(resolution)
    }

    fn apply_strategy(
        &self,
        strategy: ResolutionStrategy,
        conflict: &Conflict,
    ) -> Result<ConflictResolution, ConflictError> {
        match strategy {
            ResolutionStrategy::LastWriteWins => self.resolve_last_write_wins(conflict),
            ResolutionStrategy::FirstWriteWins => self.resolve_first_write_wins(conflict),
            ResolutionStrategy::LocalWins => self.resolve_local_wins(conflict),
            ResolutionStrategy::RemoteWins => self.resolve_remote_wins(conflict),
            ResolutionStrategy::HigherNodeWins => self.resolve_higher_node_wins(conflict),
            ResolutionStrategy::LowerNodeWins => self.resolve_lower_node_wins(conflict),
            ResolutionStrategy::KeepBoth => self.resolve_keep_both(conflict),
            ResolutionStrategy::Skip => Ok(ConflictResolution {
                strategy: ResolutionStrategy::Skip,
                winner: None,
                actions: vec![ResolutionAction::Skip, ResolutionAction::LogConflict],
                resolved_at: Instant::now(),
                reason: "Skipped by policy".to_string(),
            }),
            ResolutionStrategy::Error => Ok(ConflictResolution {
                strategy: ResolutionStrategy::Error,
                winner: None,
                actions: vec![ResolutionAction::LogConflict, ResolutionAction::NotifyAdmin],
                resolved_at: Instant::now(),
                reason: "Requires manual resolution".to_string(),
            }),
            ResolutionStrategy::Custom => Err(ConflictError::NoStrategy),
        }
    }

    fn resolve_last_write_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        let local_time = conflict.local_row.as_ref().map(|r| r.origin.commit_time);
        let remote_time = conflict.remote_row.as_ref().map(|r| r.origin.commit_time);

        let (winner, reason) = match (local_time, remote_time) {
            (Some(lt), Some(rt)) if lt >= rt => {
                (conflict.local_row.clone(), "Local has later timestamp")
            }
            (Some(_), Some(_)) => (conflict.remote_row.clone(), "Remote has later timestamp"),
            (Some(_), None) => (conflict.local_row.clone(), "Only local exists"),
            (None, Some(_)) => (conflict.remote_row.clone(), "Only remote exists"),
            (None, None) => return Err(ConflictError::ResolutionFailed("No rows to compare".to_string())),
        };

        Ok(ConflictResolution {
            strategy: ResolutionStrategy::LastWriteWins,
            winner: winner.clone(),
            actions: if winner.is_some() {
                vec![ResolutionAction::Apply(winner.unwrap()), ResolutionAction::LogConflict]
            } else {
                vec![ResolutionAction::LogConflict]
            },
            resolved_at: Instant::now(),
            reason: reason.to_string(),
        })
    }

    fn resolve_first_write_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        let local_time = conflict.local_row.as_ref().map(|r| r.origin.commit_time);
        let remote_time = conflict.remote_row.as_ref().map(|r| r.origin.commit_time);

        let (winner, reason) = match (local_time, remote_time) {
            (Some(lt), Some(rt)) if lt <= rt => {
                (conflict.local_row.clone(), "Local has earlier timestamp")
            }
            (Some(_), Some(_)) => (conflict.remote_row.clone(), "Remote has earlier timestamp"),
            (Some(_), None) => (conflict.local_row.clone(), "Only local exists"),
            (None, Some(_)) => (conflict.remote_row.clone(), "Only remote exists"),
            (None, None) => return Err(ConflictError::ResolutionFailed("No rows to compare".to_string())),
        };

        Ok(ConflictResolution {
            strategy: ResolutionStrategy::FirstWriteWins,
            winner: winner.clone(),
            actions: if winner.is_some() {
                vec![ResolutionAction::Apply(winner.unwrap()), ResolutionAction::LogConflict]
            } else {
                vec![ResolutionAction::LogConflict]
            },
            resolved_at: Instant::now(),
            reason: reason.to_string(),
        })
    }

    fn resolve_local_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        Ok(ConflictResolution {
            strategy: ResolutionStrategy::LocalWins,
            winner: conflict.local_row.clone(),
            actions: vec![ResolutionAction::Skip, ResolutionAction::LogConflict],
            resolved_at: Instant::now(),
            reason: "Local wins by policy".to_string(),
        })
    }

    fn resolve_remote_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        let winner = conflict.remote_row.clone();
        Ok(ConflictResolution {
            strategy: ResolutionStrategy::RemoteWins,
            winner: winner.clone(),
            actions: if winner.is_some() {
                vec![ResolutionAction::Apply(winner.unwrap()), ResolutionAction::LogConflict]
            } else {
                vec![ResolutionAction::Delete, ResolutionAction::LogConflict]
            },
            resolved_at: Instant::now(),
            reason: "Remote wins by policy".to_string(),
        })
    }

    fn resolve_higher_node_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        let local_node = conflict.local_row.as_ref().map(|r| r.origin.node_id.clone());
        let remote_node = conflict.remote_row.as_ref().map(|r| r.origin.node_id.clone());

        let (winner, reason) = match (local_node, remote_node) {
            (Some(ln), Some(rn)) if ln >= rn => (conflict.local_row.clone(), "Local node ID is higher"),
            (Some(_), Some(_)) => (conflict.remote_row.clone(), "Remote node ID is higher"),
            (Some(_), None) => (conflict.local_row.clone(), "Only local exists"),
            (None, Some(_)) => (conflict.remote_row.clone(), "Only remote exists"),
            (None, None) => return Err(ConflictError::ResolutionFailed("No rows to compare".to_string())),
        };

        Ok(ConflictResolution {
            strategy: ResolutionStrategy::HigherNodeWins,
            winner: winner.clone(),
            actions: if winner.is_some() {
                vec![ResolutionAction::Apply(winner.unwrap()), ResolutionAction::LogConflict]
            } else {
                vec![ResolutionAction::LogConflict]
            },
            resolved_at: Instant::now(),
            reason: reason.to_string(),
        })
    }

    fn resolve_lower_node_wins(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        let local_node = conflict.local_row.as_ref().map(|r| r.origin.node_id.clone());
        let remote_node = conflict.remote_row.as_ref().map(|r| r.origin.node_id.clone());

        let (winner, reason) = match (local_node, remote_node) {
            (Some(ln), Some(rn)) if ln <= rn => (conflict.local_row.clone(), "Local node ID is lower"),
            (Some(_), Some(_)) => (conflict.remote_row.clone(), "Remote node ID is lower"),
            (Some(_), None) => (conflict.local_row.clone(), "Only local exists"),
            (None, Some(_)) => (conflict.remote_row.clone(), "Only remote exists"),
            (None, None) => return Err(ConflictError::ResolutionFailed("No rows to compare".to_string())),
        };

        Ok(ConflictResolution {
            strategy: ResolutionStrategy::LowerNodeWins,
            winner: winner.clone(),
            actions: if winner.is_some() {
                vec![ResolutionAction::Apply(winner.unwrap()), ResolutionAction::LogConflict]
            } else {
                vec![ResolutionAction::LogConflict]
            },
            resolved_at: Instant::now(),
            reason: reason.to_string(),
        })
    }

    fn resolve_keep_both(&self, conflict: &Conflict) -> Result<ConflictResolution, ConflictError> {
        Ok(ConflictResolution {
            strategy: ResolutionStrategy::KeepBoth,
            winner: None,
            actions: vec![
                ResolutionAction::KeepBoth {
                    local_suffix: "_local".to_string(),
                    remote_suffix: "_remote".to_string(),
                },
                ResolutionAction::LogConflict,
            ],
            resolved_at: Instant::now(),
            reason: "Keeping both versions".to_string(),
        })
    }

    fn store_conflict(&self, conflict: Conflict) {
        let config = self.config.read();
        if !config.log_conflicts {
            return;
        }
        let max_history = config.max_history;
        drop(config);

        let mut history = self.history.write();
        history.push(conflict);

        // Trim if necessary
        if history.len() > max_history {
            let drain_count = history.len() - max_history;
            history.drain(0..drain_count);
        }
    }

    /// Get conflict history
    pub fn history(&self) -> Vec<Conflict> {
        self.history.read().clone()
    }

    /// Clear history
    pub fn clear_history(&self) {
        self.history.write().clear();
    }

    /// Get statistics
    pub fn stats(&self) -> ConflictStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get pending (unresolved) conflicts
    pub fn pending_conflicts(&self) -> Vec<Conflict> {
        self.history
            .read()
            .iter()
            .filter(|c| !c.resolved)
            .cloned()
            .collect()
    }

    /// Get local node ID
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
}

/// Remote operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteOperation {
    Insert,
    Update,
    Delete,
}

// ============================================================================
// Conflict Logger
// ============================================================================

/// Logs conflicts to persistent storage
pub struct ConflictLogger {
    /// Logged conflicts
    conflicts: RwLock<Vec<LoggedConflict>>,
    /// Max log size
    max_size: usize,
}

/// A logged conflict entry
#[derive(Debug, Clone)]
pub struct LoggedConflict {
    pub id: u64,
    pub conflict_type: ConflictType,
    pub table: String,
    pub primary_key: String,
    pub local_origin: String,
    pub remote_origin: String,
    pub resolution: String,
    pub logged_at: SystemTime,
}

impl ConflictLogger {
    pub fn new(max_size: usize) -> Self {
        Self {
            conflicts: RwLock::new(Vec::new()),
            max_size,
        }
    }

    /// Log a conflict
    pub fn log(&self, conflict: &Conflict, resolution: &ConflictResolution) {
        let pk_str = conflict
            .primary_key
            .iter()
            .map(|(k, v)| format!("{}={:?}", k, v))
            .collect::<Vec<_>>()
            .join(",");

        let local_origin = conflict
            .local_row
            .as_ref()
            .map(|r| format!("{}:{}", r.origin.node_id, r.origin.xid))
            .unwrap_or_else(|| "none".to_string());

        let remote_origin = conflict
            .remote_row
            .as_ref()
            .map(|r| format!("{}:{}", r.origin.node_id, r.origin.xid))
            .unwrap_or_else(|| "none".to_string());

        let entry = LoggedConflict {
            id: conflict.id,
            conflict_type: conflict.conflict_type,
            table: format!("{}.{}", conflict.schema, conflict.table),
            primary_key: pk_str,
            local_origin,
            remote_origin,
            resolution: resolution.reason.clone(),
            logged_at: SystemTime::now(),
        };

        let mut conflicts = self.conflicts.write();
        conflicts.push(entry);

        if conflicts.len() > self.max_size {
            let drain_count = conflicts.len() - self.max_size;
            conflicts.drain(0..drain_count);
        }
    }

    /// Get recent conflicts
    pub fn recent(&self, limit: usize) -> Vec<LoggedConflict> {
        let conflicts = self.conflicts.read();
        conflicts.iter().rev().take(limit).cloned().collect()
    }

    /// Clear log
    pub fn clear(&self) {
        self.conflicts.write().clear();
    }

    /// Get log size
    pub fn size(&self) -> usize {
        self.conflicts.read().len()
    }
}

impl Default for ConflictLogger {
    fn default() -> Self {
        Self::new(10000)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(schema: &str, table: &str, node: &str, time: u64) -> ConflictRow {
        ConflictRow {
            schema: schema.to_string(),
            table: table.to_string(),
            primary_key: {
                let mut pk = HashMap::new();
                pk.insert("id".to_string(), ConflictValue::Int64(1));
                pk
            },
            values: HashMap::new(),
            origin: ChangeOrigin::new(node, 1, time, 100),
        }
    }

    #[test]
    fn test_conflict_type() {
        assert!(ConflictType::InsertInsert.is_critical());
        assert!(ConflictType::UpdateUpdate.is_critical());
        assert!(!ConflictType::DeleteDelete.is_critical());
    }

    #[test]
    fn test_detect_conflict() {
        let resolver = ConflictResolver::new("node1");

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update);

        assert!(conflict.is_some());
        let c = conflict.unwrap();
        assert_eq!(c.conflict_type, ConflictType::UpdateUpdate);
    }

    #[test]
    fn test_no_conflict() {
        let resolver = ConflictResolver::new("node1");

        // Normal delete - no conflict
        let conflict = resolver.detect_conflict(Some(make_row("public", "users", "node1", 100)), None, RemoteOperation::Delete);

        assert!(conflict.is_none());
    }

    #[test]
    fn test_last_write_wins() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::LastWriteWins,
            ..Default::default()
        });

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update).unwrap();
        let resolution = resolver.resolve(conflict).unwrap();

        assert_eq!(resolution.strategy, ResolutionStrategy::LastWriteWins);
        assert!(resolution.winner.is_some());
        // Remote wins (time 200 > 100)
        assert_eq!(resolution.winner.as_ref().unwrap().origin.node_id, "node2");
    }

    #[test]
    fn test_first_write_wins() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::FirstWriteWins,
            ..Default::default()
        });

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update).unwrap();
        let resolution = resolver.resolve(conflict).unwrap();

        // Local wins (time 100 < 200)
        assert_eq!(resolution.winner.as_ref().unwrap().origin.node_id, "node1");
    }

    #[test]
    fn test_local_wins() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::LocalWins,
            ..Default::default()
        });

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update).unwrap();
        let resolution = resolver.resolve(conflict).unwrap();

        assert_eq!(resolution.strategy, ResolutionStrategy::LocalWins);
        // Local always wins
        assert_eq!(resolution.winner.as_ref().unwrap().origin.node_id, "node1");
    }

    #[test]
    fn test_remote_wins() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::RemoteWins,
            ..Default::default()
        });

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update).unwrap();
        let resolution = resolver.resolve(conflict).unwrap();

        assert_eq!(resolution.strategy, ResolutionStrategy::RemoteWins);
        assert_eq!(resolution.winner.as_ref().unwrap().origin.node_id, "node2");
    }

    #[test]
    fn test_per_table_strategy() {
        let resolver = ConflictResolver::new("node1");

        let mut table_strategies = HashMap::new();
        table_strategies.insert("public.users".to_string(), ResolutionStrategy::RemoteWins);

        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::LocalWins,
            table_strategies,
            ..Default::default()
        });

        let local = make_row("public", "users", "node1", 100);
        let remote = make_row("public", "users", "node2", 200);

        let conflict = resolver.detect_conflict(Some(local), Some(remote), RemoteOperation::Update).unwrap();
        let resolution = resolver.resolve(conflict).unwrap();

        // Table-specific strategy should override default
        assert_eq!(resolution.strategy, ResolutionStrategy::RemoteWins);
    }

    #[test]
    fn test_skip_strategy() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::Skip,
            ..Default::default()
        });

        let conflict = resolver.detect_conflict(
            Some(make_row("public", "users", "node1", 100)),
            Some(make_row("public", "users", "node2", 200)),
            RemoteOperation::Update,
        ).unwrap();

        let resolution = resolver.resolve(conflict).unwrap();
        assert_eq!(resolution.strategy, ResolutionStrategy::Skip);
        assert!(resolution.winner.is_none());
    }

    #[test]
    fn test_statistics() {
        let resolver = ConflictResolver::new("node1");

        for _ in 0..5 {
            let conflict = resolver.detect_conflict(
                Some(make_row("public", "users", "node1", 100)),
                Some(make_row("public", "users", "node2", 200)),
                RemoteOperation::Update,
            ).unwrap();
            resolver.resolve(conflict).unwrap();
        }

        let stats = resolver.stats();
        assert_eq!(stats.conflicts_detected, 5);
        assert_eq!(stats.update_update, 5);
    }

    #[test]
    fn test_conflict_logger() {
        let logger = ConflictLogger::new(100);

        let conflict = Conflict::new(
            1,
            ConflictType::UpdateUpdate,
            "public",
            "users",
            HashMap::new(),
        );

        let resolution = ConflictResolution {
            strategy: ResolutionStrategy::LastWriteWins,
            winner: None,
            actions: vec![],
            resolved_at: Instant::now(),
            reason: "Test".to_string(),
        };

        logger.log(&conflict, &resolution);

        assert_eq!(logger.size(), 1);
        let recent = logger.recent(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, 1);
    }

    #[test]
    fn test_custom_resolver() {
        let resolver = ConflictResolver::new("node1");
        resolver.set_config(ConflictConfig {
            default_strategy: ResolutionStrategy::Custom,
            ..Default::default()
        });

        resolver.set_custom_resolver(|_conflict| {
            Some(ConflictResolution {
                strategy: ResolutionStrategy::Custom,
                winner: None,
                actions: vec![],
                resolved_at: Instant::now(),
                reason: "Custom resolution".to_string(),
            })
        });

        let conflict = resolver.detect_conflict(
            Some(make_row("public", "users", "node1", 100)),
            Some(make_row("public", "users", "node2", 200)),
            RemoteOperation::Update,
        ).unwrap();

        let resolution = resolver.resolve(conflict).unwrap();
        assert_eq!(resolution.reason, "Custom resolution");
    }

    #[test]
    fn test_error_display() {
        let err = ConflictError::ResolutionFailed("test".to_string());
        assert!(format!("{}", err).contains("test"));

        let err = ConflictError::NoStrategy;
        assert!(format!("{}", err).contains("strategy"));
    }

    #[test]
    fn test_conflict_value() {
        let null = ConflictValue::Null;
        assert!(null.is_null());

        let int = ConflictValue::Int64(42);
        assert!(!int.is_null());
    }

    #[test]
    fn test_history() {
        let resolver = ConflictResolver::new("node1");

        let conflict = resolver.detect_conflict(
            Some(make_row("public", "users", "node1", 100)),
            Some(make_row("public", "users", "node2", 200)),
            RemoteOperation::Update,
        ).unwrap();

        resolver.resolve(conflict).unwrap();

        let history = resolver.history();
        assert_eq!(history.len(), 1);
        assert!(history[0].resolved);

        resolver.clear_history();
        assert!(resolver.history().is_empty());
    }
}
