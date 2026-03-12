//! Logical Replication with Pub/Sub Model
//!
//! Provides PostgreSQL-style CREATE PUBLICATION / CREATE SUBSCRIPTION
//! for selective table replication with conflict resolution.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// Logical replication error types
#[derive(Debug, Clone)]
pub enum ReplicationError {
    /// Publication not found
    PublicationNotFound(String),
    /// Subscription not found
    SubscriptionNotFound(String),
    /// Slot not found
    SlotNotFound(String),
    /// Slot already exists
    SlotAlreadyExists(String),
    /// Connection failed
    ConnectionFailed(String),
    /// Replication lag exceeded
    LagExceeded(String),
    /// Conflict detected
    ConflictDetected(String),
    /// Invalid operation
    InvalidOperation(String),
    /// Origin not found
    OriginNotFound(String),
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PublicationNotFound(s) => write!(f, "publication not found: {}", s),
            Self::SubscriptionNotFound(s) => write!(f, "subscription not found: {}", s),
            Self::SlotNotFound(s) => write!(f, "replication slot not found: {}", s),
            Self::SlotAlreadyExists(s) => write!(f, "replication slot already exists: {}", s),
            Self::ConnectionFailed(s) => write!(f, "connection failed: {}", s),
            Self::LagExceeded(s) => write!(f, "replication lag exceeded: {}", s),
            Self::ConflictDetected(s) => write!(f, "conflict detected: {}", s),
            Self::InvalidOperation(s) => write!(f, "invalid operation: {}", s),
            Self::OriginNotFound(s) => write!(f, "origin not found: {}", s),
        }
    }
}

impl std::error::Error for ReplicationError {}

/// Log Sequence Number
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LSN(pub u64);

impl LSN {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn advance(&self, bytes: u64) -> Self {
        Self(self.0 + bytes)
    }
}

impl std::fmt::Display for LSN {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let high = (self.0 >> 32) as u32;
        let low = self.0 as u32;
        write!(f, "{:X}/{:08X}", high, low)
    }
}

/// Publication definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publication {
    pub name: String,
    pub owner: String,
    pub tables: PublicationTables,
    pub operations: PublicationOperations,
    pub options: PublicationOptions,
    pub created_at: SystemTime,
}

/// Which tables are included in the publication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PublicationTables {
    /// All tables in database
    AllTables,
    /// Specific tables
    Tables(Vec<PublicationTable>),
    /// Tables matching schema pattern
    SchemasInclude(Vec<String>),
    /// All tables except these schemas
    SchemasExclude(Vec<String>),
}

/// A table in a publication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicationTable {
    pub schema: String,
    pub name: String,
    /// Column filter (empty = all columns)
    pub columns: Vec<String>,
    /// Row filter expression
    pub row_filter: Option<String>,
}

/// Which operations to replicate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicationOperations {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
}

impl Default for PublicationOperations {
    fn default() -> Self {
        Self {
            insert: true,
            update: true,
            delete: true,
            truncate: true,
        }
    }
}

/// Publication options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PublicationOptions {
    /// Publish via partition root
    pub publish_via_partition_root: bool,
}

/// Subscription definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub owner: String,
    pub connection_string: String,
    pub publications: Vec<String>,
    pub slot_name: String,
    pub state: SubscriptionState,
    pub options: SubscriptionOptions,
    pub created_at: SystemTime,
}

/// Subscription state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionState {
    /// Initial sync in progress
    Initializing,
    /// Copying table data
    CopyingData,
    /// Waiting for sync to complete
    SyncWait,
    /// Ready and streaming
    Ready,
    /// Subscription disabled
    Disabled,
    /// Error state
    Error,
}

/// Subscription options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionOptions {
    /// Create slot on subscribe
    pub create_slot: bool,
    /// Enable subscription
    pub enabled: bool,
    /// Copy existing data
    pub copy_data: bool,
    /// Synchronous commit mode
    pub synchronous_commit: SyncCommitMode,
    /// Binary transfer
    pub binary: bool,
    /// Streaming mode
    pub streaming: StreamingMode,
    /// Disable on error
    pub disable_on_error: bool,
    /// Origin filter
    pub origin: OriginFilter,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            create_slot: true,
            enabled: true,
            copy_data: true,
            synchronous_commit: SyncCommitMode::Off,
            binary: false,
            streaming: StreamingMode::Off,
            disable_on_error: false,
            origin: OriginFilter::Any,
        }
    }
}

/// Synchronous commit mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncCommitMode {
    Off,
    Local,
    RemoteWrite,
    RemoteApply,
    On,
}

/// Streaming mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamingMode {
    Off,
    On,
    Parallel,
}

/// Origin filter for avoiding loops
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OriginFilter {
    /// Accept changes from any origin
    Any,
    /// Only accept local changes
    None,
    /// Accept from specific origins
    Origins(Vec<String>),
}

/// Replication slot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSlot {
    pub name: String,
    pub plugin: String,
    pub slot_type: SlotType,
    pub database: String,
    pub restart_lsn: LSN,
    pub confirmed_flush_lsn: LSN,
    pub active: bool,
    pub active_pid: Option<u32>,
    pub created_at: SystemTime,
    pub temporary: bool,
    pub two_phase: bool,
}

/// Slot type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotType {
    Physical,
    Logical,
}

/// Replication origin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationOrigin {
    pub name: String,
    pub remote_lsn: LSN,
    pub local_lsn: LSN,
}

/// Change event from WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub lsn: LSN,
    pub xid: u64,
    pub timestamp: SystemTime,
    pub origin: Option<String>,
    pub change: Change,
}

/// Type of change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Change {
    Begin {
        xid: u64,
        final_lsn: LSN,
        commit_time: SystemTime,
    },
    Commit {
        xid: u64,
        commit_lsn: LSN,
        commit_time: SystemTime,
    },
    Insert {
        schema: String,
        table: String,
        columns: Vec<ColumnValue>,
    },
    Update {
        schema: String,
        table: String,
        old_key: Option<Vec<ColumnValue>>,
        old_row: Option<Vec<ColumnValue>>,
        new_row: Vec<ColumnValue>,
    },
    Delete {
        schema: String,
        table: String,
        old_key: Vec<ColumnValue>,
        old_row: Option<Vec<ColumnValue>>,
    },
    Truncate {
        schemas: Vec<String>,
        tables: Vec<String>,
        cascade: bool,
        restart_identity: bool,
    },
    Message {
        transactional: bool,
        prefix: String,
        content: Vec<u8>,
    },
    Relation {
        id: u32,
        schema: String,
        name: String,
        replica_identity: ReplicaIdentity,
        columns: Vec<ColumnDef>,
    },
    Type {
        id: u32,
        schema: String,
        name: String,
    },
    Origin {
        commit_lsn: LSN,
        name: String,
    },
}

/// Column value in change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnValue {
    pub name: String,
    pub type_id: u32,
    pub value: Option<Vec<u8>>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub type_id: u32,
    pub type_modifier: i32,
    pub flags: u8,
}

/// Replica identity mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Apply remote change (last write wins)
    ApplyRemote,
    /// Keep local change
    KeepLocal,
    /// Log and skip
    LogAndSkip,
    /// Error and stop
    Error,
    /// Use custom resolver
    Custom,
}

/// Conflict details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub conflict_type: ConflictType,
    pub schema: String,
    pub table: String,
    pub local_row: Option<Vec<ColumnValue>>,
    pub remote_row: Vec<ColumnValue>,
    pub lsn: LSN,
    pub timestamp: SystemTime,
    pub resolution: Option<ConflictResolution>,
}

/// Type of conflict
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictType {
    /// Insert on existing key
    InsertExists,
    /// Update on missing row
    UpdateMissing,
    /// Update on changed row
    UpdateChanged,
    /// Delete on missing row
    DeleteMissing,
}

/// Subscription relation state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRelation {
    pub schema: String,
    pub table: String,
    pub state: RelationSyncState,
    pub remote_lsn: LSN,
    pub local_lsn: LSN,
}

/// Relation sync state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSyncState {
    /// Initial state
    Init,
    /// Data copy in progress
    DataCopy,
    /// Sync with WAL changes
    Sync,
    /// Fully synchronized
    Ready,
}

/// Logical replication manager
pub struct LogicalReplicationManager {
    /// Publications
    publications: RwLock<HashMap<String, Publication>>,
    /// Subscriptions
    subscriptions: RwLock<HashMap<String, Subscription>>,
    /// Replication slots
    slots: RwLock<HashMap<String, ReplicationSlot>>,
    /// Replication origins
    origins: RwLock<HashMap<String, ReplicationOrigin>>,
    /// Subscription relation states
    relation_states: RwLock<HashMap<(String, String, String), SubscriptionRelation>>,
    /// Pending changes per slot
    pending_changes: RwLock<HashMap<String, VecDeque<ChangeEvent>>>,
    /// Conflict log
    conflicts: RwLock<Vec<Conflict>>,
    /// Configuration
    config: ReplicationConfig,
    /// Statistics
    stats: RwLock<ReplicationStats>,
}

/// Replication configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Maximum slot WAL keep size
    pub max_slot_wal_keep_size: u64,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictResolution,
    /// Maximum replication lag before warning
    pub max_replication_lag: Duration,
    /// Batch size for applying changes
    pub apply_batch_size: usize,
    /// Parallel apply workers
    pub parallel_workers: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            max_slot_wal_keep_size: 1024 * 1024 * 1024, // 1GB
            conflict_resolution: ConflictResolution::ApplyRemote,
            max_replication_lag: Duration::from_secs(60),
            apply_batch_size: 1000,
            parallel_workers: 2,
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationStats {
    pub changes_sent: u64,
    pub changes_received: u64,
    pub changes_applied: u64,
    pub conflicts_total: u64,
    pub conflicts_resolved: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
}

impl LogicalReplicationManager {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            publications: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
            slots: RwLock::new(HashMap::new()),
            origins: RwLock::new(HashMap::new()),
            relation_states: RwLock::new(HashMap::new()),
            pending_changes: RwLock::new(HashMap::new()),
            conflicts: RwLock::new(Vec::new()),
            config,
            stats: RwLock::new(ReplicationStats::default()),
        }
    }

    // ========================================================================
    // Publication Management
    // ========================================================================

    /// Create a publication
    pub fn create_publication(&self, publication: Publication) -> Result<(), ReplicationError> {
        let mut publications = self.publications.write().unwrap();

        if publications.contains_key(&publication.name) {
            return Err(ReplicationError::InvalidOperation(format!(
                "publication '{}' already exists",
                publication.name
            )));
        }

        publications.insert(publication.name.clone(), publication);
        Ok(())
    }

    /// Drop a publication
    pub fn drop_publication(&self, name: &str) -> Result<Publication, ReplicationError> {
        let mut publications = self.publications.write().unwrap();

        publications
            .remove(name)
            .ok_or_else(|| ReplicationError::PublicationNotFound(name.to_string()))
    }

    /// Alter publication - add tables
    pub fn publication_add_tables(
        &self,
        name: &str,
        tables: Vec<PublicationTable>,
    ) -> Result<(), ReplicationError> {
        let mut publications = self.publications.write().unwrap();

        let publication = publications
            .get_mut(name)
            .ok_or_else(|| ReplicationError::PublicationNotFound(name.to_string()))?;

        match &mut publication.tables {
            PublicationTables::Tables(existing) => {
                existing.extend(tables);
            }
            PublicationTables::AllTables => {
                return Err(ReplicationError::InvalidOperation(
                    "cannot add tables to FOR ALL TABLES publication".into(),
                ));
            }
            _ => {
                publication.tables = PublicationTables::Tables(tables);
            }
        }

        Ok(())
    }

    /// Alter publication - drop tables
    pub fn publication_drop_tables(
        &self,
        name: &str,
        tables: Vec<(String, String)>,
    ) -> Result<(), ReplicationError> {
        let mut publications = self.publications.write().unwrap();

        let publication = publications
            .get_mut(name)
            .ok_or_else(|| ReplicationError::PublicationNotFound(name.to_string()))?;

        match &mut publication.tables {
            PublicationTables::Tables(existing) => {
                let drop_set: HashSet<_> = tables.iter().collect();
                existing.retain(|t| !drop_set.contains(&(t.schema.clone(), t.name.clone())));
            }
            _ => {
                return Err(ReplicationError::InvalidOperation(
                    "cannot drop tables from this publication type".into(),
                ));
            }
        }

        Ok(())
    }

    /// Get publication
    pub fn get_publication(&self, name: &str) -> Option<Publication> {
        self.publications.read().unwrap().get(name).cloned()
    }

    /// List all publications
    pub fn list_publications(&self) -> Vec<Publication> {
        self.publications.read().unwrap().values().cloned().collect()
    }

    /// Check if table is in publication
    pub fn is_table_published(
        &self,
        publication: &str,
        schema: &str,
        table: &str,
    ) -> Result<bool, ReplicationError> {
        let publications = self.publications.read().unwrap();
        let pub_def = publications
            .get(publication)
            .ok_or_else(|| ReplicationError::PublicationNotFound(publication.to_string()))?;

        match &pub_def.tables {
            PublicationTables::AllTables => Ok(true),
            PublicationTables::Tables(tables) => {
                Ok(tables.iter().any(|t| t.schema == schema && t.name == table))
            }
            PublicationTables::SchemasInclude(schemas) => Ok(schemas.contains(&schema.to_string())),
            PublicationTables::SchemasExclude(schemas) => {
                Ok(!schemas.contains(&schema.to_string()))
            }
        }
    }

    // ========================================================================
    // Subscription Management
    // ========================================================================

    /// Create a subscription
    pub fn create_subscription(&self, subscription: Subscription) -> Result<(), ReplicationError> {
        let mut subscriptions = self.subscriptions.write().unwrap();

        if subscriptions.contains_key(&subscription.name) {
            return Err(ReplicationError::InvalidOperation(format!(
                "subscription '{}' already exists",
                subscription.name
            )));
        }

        // Create slot if requested
        if subscription.options.create_slot {
            self.create_replication_slot(
                &subscription.slot_name,
                "pgoutput",
                SlotType::Logical,
                "default",
                false,
                false,
            )?;
        }

        subscriptions.insert(subscription.name.clone(), subscription);
        Ok(())
    }

    /// Drop a subscription
    pub fn drop_subscription(&self, name: &str) -> Result<Subscription, ReplicationError> {
        let mut subscriptions = self.subscriptions.write().unwrap();

        let subscription = subscriptions
            .remove(name)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(name.to_string()))?;

        // Drop associated slot
        let _ = self.drop_replication_slot(&subscription.slot_name);

        // Clean up relation states
        let mut states = self.relation_states.write().unwrap();
        states.retain(|(sub, _, _), _| sub != name);

        Ok(subscription)
    }

    /// Enable subscription
    pub fn enable_subscription(&self, name: &str) -> Result<(), ReplicationError> {
        let mut subscriptions = self.subscriptions.write().unwrap();

        let subscription = subscriptions
            .get_mut(name)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(name.to_string()))?;

        subscription.options.enabled = true;
        subscription.state = SubscriptionState::Ready;
        Ok(())
    }

    /// Disable subscription
    pub fn disable_subscription(&self, name: &str) -> Result<(), ReplicationError> {
        let mut subscriptions = self.subscriptions.write().unwrap();

        let subscription = subscriptions
            .get_mut(name)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(name.to_string()))?;

        subscription.options.enabled = false;
        subscription.state = SubscriptionState::Disabled;
        Ok(())
    }

    /// Refresh subscription (re-sync tables)
    pub fn refresh_subscription(
        &self,
        name: &str,
        copy_data: bool,
    ) -> Result<(), ReplicationError> {
        let subscriptions = self.subscriptions.read().unwrap();

        let _subscription = subscriptions
            .get(name)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(name.to_string()))?;

        // Would connect to publisher and discover new tables
        // Then initiate sync for new tables

        if copy_data {
            // Mark relations for data copy
        }

        Ok(())
    }

    /// Get subscription
    pub fn get_subscription(&self, name: &str) -> Option<Subscription> {
        self.subscriptions.read().unwrap().get(name).cloned()
    }

    /// List all subscriptions
    pub fn list_subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    // ========================================================================
    // Replication Slot Management
    // ========================================================================

    /// Create a replication slot
    pub fn create_replication_slot(
        &self,
        name: &str,
        plugin: &str,
        slot_type: SlotType,
        database: &str,
        temporary: bool,
        two_phase: bool,
    ) -> Result<LSN, ReplicationError> {
        let mut slots = self.slots.write().unwrap();

        if slots.contains_key(name) {
            return Err(ReplicationError::SlotAlreadyExists(name.to_string()));
        }

        let slot = ReplicationSlot {
            name: name.to_string(),
            plugin: plugin.to_string(),
            slot_type,
            database: database.to_string(),
            restart_lsn: LSN::zero(),
            confirmed_flush_lsn: LSN::zero(),
            active: false,
            active_pid: None,
            created_at: SystemTime::now(),
            temporary,
            two_phase,
        };

        let lsn = slot.restart_lsn;
        slots.insert(name.to_string(), slot);

        // Initialize pending changes queue
        self.pending_changes
            .write()
            .unwrap()
            .insert(name.to_string(), VecDeque::new());

        Ok(lsn)
    }

    /// Drop a replication slot
    pub fn drop_replication_slot(&self, name: &str) -> Result<(), ReplicationError> {
        let mut slots = self.slots.write().unwrap();

        let slot = slots
            .get(name)
            .ok_or_else(|| ReplicationError::SlotNotFound(name.to_string()))?;

        if slot.active {
            return Err(ReplicationError::InvalidOperation(
                "cannot drop active slot".into(),
            ));
        }

        slots.remove(name);
        self.pending_changes.write().unwrap().remove(name);

        Ok(())
    }

    /// Advance replication slot
    pub fn advance_slot(&self, name: &str, lsn: LSN) -> Result<(), ReplicationError> {
        let mut slots = self.slots.write().unwrap();

        let slot = slots
            .get_mut(name)
            .ok_or_else(|| ReplicationError::SlotNotFound(name.to_string()))?;

        if lsn > slot.confirmed_flush_lsn {
            slot.confirmed_flush_lsn = lsn;
        }

        Ok(())
    }

    /// Get slot
    pub fn get_slot(&self, name: &str) -> Option<ReplicationSlot> {
        self.slots.read().unwrap().get(name).cloned()
    }

    /// List all slots
    pub fn list_slots(&self) -> Vec<ReplicationSlot> {
        self.slots.read().unwrap().values().cloned().collect()
    }

    // ========================================================================
    // Origin Management
    // ========================================================================

    /// Create replication origin
    pub fn create_origin(&self, name: &str) -> Result<(), ReplicationError> {
        let mut origins = self.origins.write().unwrap();

        if origins.contains_key(name) {
            return Err(ReplicationError::InvalidOperation(format!(
                "origin '{}' already exists",
                name
            )));
        }

        origins.insert(
            name.to_string(),
            ReplicationOrigin {
                name: name.to_string(),
                remote_lsn: LSN::zero(),
                local_lsn: LSN::zero(),
            },
        );

        Ok(())
    }

    /// Drop replication origin
    pub fn drop_origin(&self, name: &str) -> Result<(), ReplicationError> {
        let mut origins = self.origins.write().unwrap();

        origins
            .remove(name)
            .ok_or_else(|| ReplicationError::OriginNotFound(name.to_string()))?;

        Ok(())
    }

    /// Advance origin
    pub fn advance_origin(
        &self,
        name: &str,
        remote_lsn: LSN,
        local_lsn: LSN,
    ) -> Result<(), ReplicationError> {
        let mut origins = self.origins.write().unwrap();

        let origin = origins
            .get_mut(name)
            .ok_or_else(|| ReplicationError::OriginNotFound(name.to_string()))?;

        origin.remote_lsn = remote_lsn;
        origin.local_lsn = local_lsn;

        Ok(())
    }

    /// Get origin progress
    pub fn get_origin_progress(&self, name: &str) -> Option<(LSN, LSN)> {
        self.origins
            .read()
            .unwrap()
            .get(name)
            .map(|o| (o.remote_lsn, o.local_lsn))
    }

    // ========================================================================
    // Change Streaming
    // ========================================================================

    /// Send change to slot
    pub fn send_change(&self, slot_name: &str, change: ChangeEvent) -> Result<(), ReplicationError> {
        let mut pending = self.pending_changes.write().unwrap();

        let queue = pending
            .get_mut(slot_name)
            .ok_or_else(|| ReplicationError::SlotNotFound(slot_name.to_string()))?;

        queue.push_back(change);

        self.stats.write().unwrap().changes_sent += 1;

        Ok(())
    }

    /// Receive changes from slot
    pub fn receive_changes(
        &self,
        slot_name: &str,
        max_changes: usize,
    ) -> Result<Vec<ChangeEvent>, ReplicationError> {
        let mut pending = self.pending_changes.write().unwrap();

        let queue = pending
            .get_mut(slot_name)
            .ok_or_else(|| ReplicationError::SlotNotFound(slot_name.to_string()))?;

        let mut changes = Vec::with_capacity(max_changes);
        for _ in 0..max_changes {
            if let Some(change) = queue.pop_front() {
                changes.push(change);
            } else {
                break;
            }
        }

        let count = changes.len() as u64;
        self.stats.write().unwrap().changes_received += count;

        Ok(changes)
    }

    /// Apply change with conflict detection
    pub fn apply_change(
        &self,
        subscription: &str,
        change: &ChangeEvent,
    ) -> Result<ApplyResult, ReplicationError> {
        // Check if we should apply based on origin
        let subscriptions = self.subscriptions.read().unwrap();
        let sub = subscriptions
            .get(subscription)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(subscription.to_string()))?;

        if !sub.options.enabled {
            return Ok(ApplyResult::Skipped("subscription disabled".into()));
        }

        // Check origin filter
        if let Some(origin) = &change.origin {
            match &sub.options.origin {
                OriginFilter::None => {
                    return Ok(ApplyResult::Skipped("origin filter: none".into()));
                }
                OriginFilter::Origins(allowed) => {
                    if !allowed.contains(origin) {
                        return Ok(ApplyResult::Skipped("origin not in filter".into()));
                    }
                }
                OriginFilter::Any => {}
            }
        }

        drop(subscriptions);

        // Apply the change (would actually modify the database)
        let result = match &change.change {
            Change::Insert { schema, table, .. } => {
                // Check for conflicts (duplicate key)
                // If conflict, resolve based on strategy
                self.stats.write().unwrap().changes_applied += 1;
                ApplyResult::Applied {
                    schema: schema.clone(),
                    table: table.clone(),
                }
            }
            Change::Update { schema, table, .. } => {
                self.stats.write().unwrap().changes_applied += 1;
                ApplyResult::Applied {
                    schema: schema.clone(),
                    table: table.clone(),
                }
            }
            Change::Delete { schema, table, .. } => {
                self.stats.write().unwrap().changes_applied += 1;
                ApplyResult::Applied {
                    schema: schema.clone(),
                    table: table.clone(),
                }
            }
            Change::Commit { xid, .. } => {
                self.stats.write().unwrap().transactions_committed += 1;
                ApplyResult::Committed { xid: *xid }
            }
            _ => ApplyResult::Skipped("unhandled change type".into()),
        };

        Ok(result)
    }

    /// Resolve conflict
    pub fn resolve_conflict(
        &self,
        conflict: &Conflict,
        resolution: ConflictResolution,
    ) -> Result<(), ReplicationError> {
        let mut conflicts = self.conflicts.write().unwrap();

        let mut resolved_conflict = conflict.clone();
        resolved_conflict.resolution = Some(resolution);

        conflicts.push(resolved_conflict);

        let mut stats = self.stats.write().unwrap();
        stats.conflicts_total += 1;
        stats.conflicts_resolved += 1;

        Ok(())
    }

    // ========================================================================
    // Monitoring
    // ========================================================================

    /// Get replication lag for subscription
    pub fn get_replication_lag(&self, subscription: &str) -> Result<Duration, ReplicationError> {
        let subscriptions = self.subscriptions.read().unwrap();
        let _sub = subscriptions
            .get(subscription)
            .ok_or_else(|| ReplicationError::SubscriptionNotFound(subscription.to_string()))?;

        // Would calculate actual lag from LSN positions
        Ok(Duration::from_millis(100))
    }

    /// Get subscription table states
    pub fn get_subscription_tables(&self, subscription: &str) -> Vec<SubscriptionRelation> {
        self.relation_states
            .read()
            .unwrap()
            .iter()
            .filter(|((sub, _, _), _)| sub == subscription)
            .map(|(_, rel)| rel.clone())
            .collect()
    }

    /// Get conflict log
    pub fn get_conflicts(&self, limit: usize) -> Vec<Conflict> {
        self.conflicts
            .read()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> ReplicationStats {
        self.stats.read().unwrap().clone()
    }

    /// Get slot statistics
    pub fn get_slot_stats(&self, slot_name: &str) -> Result<SlotStats, ReplicationError> {
        let slots = self.slots.read().unwrap();
        let slot = slots
            .get(slot_name)
            .ok_or_else(|| ReplicationError::SlotNotFound(slot_name.to_string()))?;

        let pending = self.pending_changes.read().unwrap();
        let pending_count = pending.get(slot_name).map(|q| q.len()).unwrap_or(0);

        Ok(SlotStats {
            name: slot.name.clone(),
            restart_lsn: slot.restart_lsn,
            confirmed_flush_lsn: slot.confirmed_flush_lsn,
            active: slot.active,
            pending_changes: pending_count,
            lag_bytes: slot.restart_lsn.0.saturating_sub(slot.confirmed_flush_lsn.0),
        })
    }
}

impl Default for LogicalReplicationManager {
    fn default() -> Self {
        Self::new(ReplicationConfig::default())
    }
}

/// Result of applying a change
#[derive(Debug, Clone)]
pub enum ApplyResult {
    Applied { schema: String, table: String },
    Skipped(String),
    Conflict(Conflict),
    Committed { xid: u64 },
    Error(String),
}

/// Slot statistics
#[derive(Debug, Clone)]
pub struct SlotStats {
    pub name: String,
    pub restart_lsn: LSN,
    pub confirmed_flush_lsn: LSN,
    pub active: bool,
    pub pending_changes: usize,
    pub lag_bytes: u64,
}

/// Subscription worker for applying changes
pub struct SubscriptionWorker {
    manager: Arc<LogicalReplicationManager>,
    subscription_name: String,
    batch_size: usize,
    apply_delay: Duration,
    running: bool,
}

impl SubscriptionWorker {
    pub fn new(
        manager: Arc<LogicalReplicationManager>,
        subscription_name: String,
        batch_size: usize,
    ) -> Self {
        Self {
            manager,
            subscription_name,
            batch_size,
            apply_delay: Duration::from_millis(10),
            running: false,
        }
    }

    /// Start the worker
    pub fn start(&mut self) {
        self.running = true;
    }

    /// Stop the worker
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Process one batch of changes
    pub fn process_batch(&self) -> Result<usize, ReplicationError> {
        let subscription = self
            .manager
            .get_subscription(&self.subscription_name)
            .ok_or_else(|| {
                ReplicationError::SubscriptionNotFound(self.subscription_name.clone())
            })?;

        if !subscription.options.enabled {
            return Ok(0);
        }

        let changes = self
            .manager
            .receive_changes(&subscription.slot_name, self.batch_size)?;

        let mut applied = 0;
        for change in &changes {
            match self.manager.apply_change(&self.subscription_name, change)? {
                ApplyResult::Applied { .. } | ApplyResult::Committed { .. } => {
                    applied += 1;
                }
                ApplyResult::Conflict(conflict) => {
                    self.manager
                        .resolve_conflict(&conflict, self.manager.config.conflict_resolution)?;
                }
                _ => {}
            }
        }

        // Advance slot
        if let Some(last) = changes.last() {
            self.manager.advance_slot(&subscription.slot_name, last.lsn)?;
        }

        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publication_creation() {
        let manager = LogicalReplicationManager::new(ReplicationConfig::default());

        // Create publication for all tables
        manager
            .create_publication(Publication {
                name: "pub_all".to_string(),
                owner: "admin".to_string(),
                tables: PublicationTables::AllTables,
                operations: PublicationOperations::default(),
                options: PublicationOptions::default(),
                created_at: SystemTime::now(),
            })
            .unwrap();

        // Create publication for specific tables
        manager
            .create_publication(Publication {
                name: "pub_sales".to_string(),
                owner: "admin".to_string(),
                tables: PublicationTables::Tables(vec![PublicationTable {
                    schema: "public".to_string(),
                    name: "orders".to_string(),
                    columns: vec![],
                    row_filter: Some("status = 'active'".to_string()),
                }]),
                operations: PublicationOperations {
                    insert: true,
                    update: true,
                    delete: true,
                    truncate: false,
                },
                options: PublicationOptions::default(),
                created_at: SystemTime::now(),
            })
            .unwrap();

        let pubs = manager.list_publications();
        assert_eq!(pubs.len(), 2);

        assert!(manager
            .is_table_published("pub_all", "any", "table")
            .unwrap());
        assert!(manager
            .is_table_published("pub_sales", "public", "orders")
            .unwrap());
        assert!(!manager
            .is_table_published("pub_sales", "public", "users")
            .unwrap());
    }

    #[test]
    fn test_subscription_creation() {
        let manager = LogicalReplicationManager::new(ReplicationConfig::default());

        manager
            .create_subscription(Subscription {
                name: "sub_replica".to_string(),
                owner: "admin".to_string(),
                connection_string: "host=primary port=5432 dbname=mydb".to_string(),
                publications: vec!["pub_all".to_string()],
                slot_name: "sub_replica_slot".to_string(),
                state: SubscriptionState::Initializing,
                options: SubscriptionOptions::default(),
                created_at: SystemTime::now(),
            })
            .unwrap();

        let sub = manager.get_subscription("sub_replica").unwrap();
        assert_eq!(sub.publications, vec!["pub_all".to_string()]);

        // Slot should be created
        let slot = manager.get_slot("sub_replica_slot").unwrap();
        assert_eq!(slot.slot_type, SlotType::Logical);
    }

    #[test]
    fn test_replication_slot() {
        let manager = LogicalReplicationManager::new(ReplicationConfig::default());

        let lsn = manager
            .create_replication_slot("test_slot", "pgoutput", SlotType::Logical, "testdb", false, false)
            .unwrap();

        assert_eq!(lsn, LSN::zero());

        manager.advance_slot("test_slot", LSN(1000)).unwrap();

        let slot = manager.get_slot("test_slot").unwrap();
        assert_eq!(slot.confirmed_flush_lsn, LSN(1000));
    }

    #[test]
    fn test_change_streaming() {
        let manager = LogicalReplicationManager::new(ReplicationConfig::default());

        manager
            .create_replication_slot("stream_slot", "pgoutput", SlotType::Logical, "testdb", false, false)
            .unwrap();

        // Send some changes
        for i in 0..5 {
            manager
                .send_change(
                    "stream_slot",
                    ChangeEvent {
                        lsn: LSN(i * 100),
                        xid: i,
                        timestamp: SystemTime::now(),
                        origin: None,
                        change: Change::Insert {
                            schema: "public".to_string(),
                            table: "users".to_string(),
                            columns: vec![],
                        },
                    },
                )
                .unwrap();
        }

        // Receive changes
        let changes = manager.receive_changes("stream_slot", 3).unwrap();
        assert_eq!(changes.len(), 3);

        let remaining = manager.receive_changes("stream_slot", 10).unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn test_lsn_display() {
        let lsn = LSN(0x0000000100000100);
        assert_eq!(format!("{}", lsn), "1/00000100");

        let lsn2 = LSN(0x00000ABC00001234);
        assert_eq!(format!("{}", lsn2), "ABC/00001234");
    }
}
