// Logical Decoding - WAL decoding and output plugins for BoyoDB
//
// Provides PostgreSQL-style logical replication support:
// - WAL change capture and decoding
// - Output plugins (test_decoding, pgoutput equivalent)
// - Replication slots for change tracking
// - Streaming changes to subscribers

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::SystemTime;

// ============================================================================
// WAL Types
// ============================================================================

/// Log Sequence Number (LSN) - position in the WAL
pub type Lsn = u64;

/// Transaction ID
pub type TransactionId = u64;

/// Operation type in the WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// A single tuple (row) in change data
#[derive(Debug, Clone)]
pub struct TupleData {
    /// Column values as strings (simplified)
    pub values: Vec<Option<String>>,
}

impl TupleData {
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }
}

/// A change to a single row
#[derive(Debug, Clone)]
pub struct RowChange {
    /// Operation type
    pub operation: OperationType,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Old tuple data (for UPDATE/DELETE)
    pub old_tuple: Option<TupleData>,
    /// New tuple data (for INSERT/UPDATE)
    pub new_tuple: Option<TupleData>,
    /// Column names
    pub columns: Vec<String>,
    /// Column types
    pub column_types: Vec<String>,
}

/// A decoded change from the WAL
#[derive(Debug, Clone)]
pub enum DecodedChange {
    /// Transaction begin
    Begin {
        xid: TransactionId,
        commit_time: u64,
        lsn: Lsn,
    },
    /// Row-level change
    Change {
        xid: TransactionId,
        lsn: Lsn,
        change: RowChange,
    },
    /// Transaction commit
    Commit {
        xid: TransactionId,
        commit_lsn: Lsn,
        commit_time: u64,
    },
    /// Truncate table
    Truncate {
        xid: TransactionId,
        lsn: Lsn,
        schema: String,
        table: String,
        cascade: bool,
        restart_identity: bool,
    },
    /// Message/keepalive
    Message {
        lsn: Lsn,
        transactional: bool,
        prefix: String,
        content: Vec<u8>,
    },
}

// ============================================================================
// Output Plugins
// ============================================================================

/// Output format trait for logical decoding
pub trait OutputPlugin: Send + Sync {
    /// Plugin name
    fn name(&self) -> &str;

    /// Initialize the plugin with options
    fn startup(&mut self, options: &HashMap<String, String>) -> Result<(), DecodingError>;

    /// Begin streaming from slot
    fn begin_streaming(&mut self) -> Result<(), DecodingError>;

    /// Format a decoded change for output
    fn format_change(&self, change: &DecodedChange) -> Result<Vec<u8>, DecodingError>;

    /// End streaming
    fn end_streaming(&mut self) -> Result<(), DecodingError>;

    /// Shutdown the plugin
    fn shutdown(&mut self) -> Result<(), DecodingError>;
}

/// test_decoding-style output (human readable)
pub struct TestDecodingPlugin {
    include_xids: bool,
    include_timestamp: bool,
    skip_empty_xacts: bool,
}

impl Default for TestDecodingPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl TestDecodingPlugin {
    pub fn new() -> Self {
        Self {
            include_xids: true,
            include_timestamp: false,
            skip_empty_xacts: true,
        }
    }
}

impl OutputPlugin for TestDecodingPlugin {
    fn name(&self) -> &str {
        "test_decoding"
    }

    fn startup(&mut self, options: &HashMap<String, String>) -> Result<(), DecodingError> {
        if let Some(v) = options.get("include-xids") {
            self.include_xids = v == "true" || v == "1";
        }
        if let Some(v) = options.get("include-timestamp") {
            self.include_timestamp = v == "true" || v == "1";
        }
        if let Some(v) = options.get("skip-empty-xacts") {
            self.skip_empty_xacts = v == "true" || v == "1";
        }
        Ok(())
    }

    fn begin_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn format_change(&self, change: &DecodedChange) -> Result<Vec<u8>, DecodingError> {
        let output = match change {
            DecodedChange::Begin { xid, commit_time, lsn } => {
                let mut s = String::from("BEGIN");
                if self.include_xids {
                    s.push_str(&format!(" {}", xid));
                }
                if self.include_timestamp {
                    s.push_str(&format!(" (at {})", commit_time));
                }
                let _ = lsn; // Available but not shown in basic format
                s
            }
            DecodedChange::Change { xid, lsn, change } => {
                let op = match change.operation {
                    OperationType::Insert => "INSERT",
                    OperationType::Update => "UPDATE",
                    OperationType::Delete => "DELETE",
                    OperationType::Truncate => "TRUNCATE",
                };

                let mut s = format!(
                    "table {}.{}: {}: ",
                    change.schema, change.table, op
                );

                match change.operation {
                    OperationType::Insert => {
                        if let Some(ref new) = change.new_tuple {
                            s.push_str(&self.format_tuple(&change.columns, new));
                        }
                    }
                    OperationType::Update => {
                        if let Some(ref old) = change.old_tuple {
                            s.push_str("old-key: ");
                            s.push_str(&self.format_tuple(&change.columns, old));
                        }
                        if let Some(ref new) = change.new_tuple {
                            s.push_str(" new-tuple: ");
                            s.push_str(&self.format_tuple(&change.columns, new));
                        }
                    }
                    OperationType::Delete => {
                        if let Some(ref old) = change.old_tuple {
                            s.push_str(&self.format_tuple(&change.columns, old));
                        }
                    }
                    OperationType::Truncate => {
                        s.push_str("(no-tuple-data)");
                    }
                }

                let _ = (xid, lsn); // Available for extensions
                s
            }
            DecodedChange::Commit { xid, commit_lsn, commit_time } => {
                let mut s = String::from("COMMIT");
                if self.include_xids {
                    s.push_str(&format!(" {}", xid));
                }
                if self.include_timestamp {
                    s.push_str(&format!(" (at {})", commit_time));
                }
                let _ = commit_lsn;
                s
            }
            DecodedChange::Truncate { xid: _, lsn: _, schema, table, .. } => {
                format!("table {}.{}: TRUNCATE", schema, table)
            }
            DecodedChange::Message { prefix, content, .. } => {
                format!("message: prefix={} content={:?}", prefix, content)
            }
        };

        Ok(output.into_bytes())
    }

    fn end_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }
}

impl TestDecodingPlugin {
    fn format_tuple(&self, columns: &[String], tuple: &TupleData) -> String {
        let mut parts = Vec::new();
        for (i, col) in columns.iter().enumerate() {
            let val = tuple.values.get(i)
                .and_then(|v| v.clone())
                .unwrap_or_else(|| "null".to_string());
            parts.push(format!("{}[text]:'{}'", col, val));
        }
        parts.join(" ")
    }
}

/// pgoutput-style output (binary protocol)
pub struct PgOutputPlugin {
    proto_version: u32,
    publication_names: Vec<String>,
    binary: bool,
}

impl Default for PgOutputPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl PgOutputPlugin {
    pub fn new() -> Self {
        Self {
            proto_version: 1,
            publication_names: Vec::new(),
            binary: false,
        }
    }
}

impl OutputPlugin for PgOutputPlugin {
    fn name(&self) -> &str {
        "pgoutput"
    }

    fn startup(&mut self, options: &HashMap<String, String>) -> Result<(), DecodingError> {
        if let Some(v) = options.get("proto_version") {
            self.proto_version = v.parse().unwrap_or(1);
        }
        if let Some(pubs) = options.get("publication_names") {
            self.publication_names = pubs.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Some(v) = options.get("binary") {
            self.binary = v == "true" || v == "1";
        }
        Ok(())
    }

    fn begin_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn format_change(&self, change: &DecodedChange) -> Result<Vec<u8>, DecodingError> {
        // Simplified pgoutput format (in real impl would be proper binary protocol)
        let mut output = Vec::new();

        match change {
            DecodedChange::Begin { xid, commit_time, lsn } => {
                output.push(b'B'); // Begin message type
                output.extend(&lsn.to_be_bytes());
                output.extend(&commit_time.to_be_bytes());
                output.extend(&xid.to_be_bytes());
            }
            DecodedChange::Change { lsn, change, .. } => {
                let msg_type = match change.operation {
                    OperationType::Insert => b'I',
                    OperationType::Update => b'U',
                    OperationType::Delete => b'D',
                    OperationType::Truncate => b'T',
                };
                output.push(msg_type);
                output.extend(&lsn.to_be_bytes());

                // Relation name
                let rel_name = format!("{}.{}", change.schema, change.table);
                output.extend((rel_name.len() as u32).to_be_bytes());
                output.extend(rel_name.as_bytes());

                // Column count
                output.extend((change.columns.len() as u16).to_be_bytes());

                // Tuple data
                if let Some(ref tuple) = change.new_tuple {
                    output.push(b'N'); // New tuple marker
                    for (i, val) in tuple.values.iter().enumerate() {
                        if let Some(v) = val {
                            output.push(b't'); // Text value
                            output.extend((v.len() as u32).to_be_bytes());
                            output.extend(v.as_bytes());
                        } else {
                            output.push(b'n'); // Null
                        }
                        let _ = i;
                    }
                }
            }
            DecodedChange::Commit { xid: _, commit_lsn, commit_time } => {
                output.push(b'C'); // Commit message type
                output.extend(&commit_lsn.to_be_bytes());
                output.extend(&commit_time.to_be_bytes());
            }
            DecodedChange::Truncate { lsn, schema, table, .. } => {
                output.push(b'T');
                output.extend(&lsn.to_be_bytes());
                let rel_name = format!("{}.{}", schema, table);
                output.extend((rel_name.len() as u32).to_be_bytes());
                output.extend(rel_name.as_bytes());
            }
            DecodedChange::Message { lsn, transactional, prefix, content } => {
                output.push(b'M');
                output.extend(&lsn.to_be_bytes());
                output.push(if *transactional { 1 } else { 0 });
                output.extend((prefix.len() as u32).to_be_bytes());
                output.extend(prefix.as_bytes());
                output.extend((content.len() as u32).to_be_bytes());
                output.extend(content);
            }
        }

        Ok(output)
    }

    fn end_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }
}

/// JSON output plugin
pub struct JsonOutputPlugin {
    pretty: bool,
}

impl Default for JsonOutputPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonOutputPlugin {
    pub fn new() -> Self {
        Self { pretty: false }
    }
}

impl OutputPlugin for JsonOutputPlugin {
    fn name(&self) -> &str {
        "wal2json"
    }

    fn startup(&mut self, options: &HashMap<String, String>) -> Result<(), DecodingError> {
        if let Some(v) = options.get("pretty") {
            self.pretty = v == "true" || v == "1";
        }
        Ok(())
    }

    fn begin_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn format_change(&self, change: &DecodedChange) -> Result<Vec<u8>, DecodingError> {
        let json = match change {
            DecodedChange::Begin { xid, commit_time, lsn } => {
                format!(
                    r#"{{"type":"begin","xid":{},"timestamp":{},"lsn":"{}"}}"#,
                    xid, commit_time, format_lsn(*lsn)
                )
            }
            DecodedChange::Change { xid, lsn, change } => {
                let op = match change.operation {
                    OperationType::Insert => "insert",
                    OperationType::Update => "update",
                    OperationType::Delete => "delete",
                    OperationType::Truncate => "truncate",
                };

                let columns_json = self.format_columns_json(&change.columns, &change.column_types);
                let old_json = change.old_tuple.as_ref()
                    .map(|t| self.format_tuple_json(&change.columns, t))
                    .unwrap_or_else(|| "null".to_string());
                let new_json = change.new_tuple.as_ref()
                    .map(|t| self.format_tuple_json(&change.columns, t))
                    .unwrap_or_else(|| "null".to_string());

                format!(
                    r#"{{"type":"{}","xid":{},"lsn":"{}","schema":"{}","table":"{}","columns":{},"old":{},"new":{}}}"#,
                    op, xid, format_lsn(*lsn), change.schema, change.table,
                    columns_json, old_json, new_json
                )
            }
            DecodedChange::Commit { xid, commit_lsn, commit_time } => {
                format!(
                    r#"{{"type":"commit","xid":{},"lsn":"{}","timestamp":{}}}"#,
                    xid, format_lsn(*commit_lsn), commit_time
                )
            }
            DecodedChange::Truncate { xid, lsn, schema, table, cascade, restart_identity } => {
                format!(
                    r#"{{"type":"truncate","xid":{},"lsn":"{}","schema":"{}","table":"{}","cascade":{},"restart_identity":{}}}"#,
                    xid, format_lsn(*lsn), schema, table, cascade, restart_identity
                )
            }
            DecodedChange::Message { lsn, transactional, prefix, content } => {
                let content_str = String::from_utf8_lossy(content);
                format!(
                    r#"{{"type":"message","lsn":"{}","transactional":{},"prefix":"{}","content":"{}"}}"#,
                    format_lsn(*lsn), transactional, prefix, escape_json(&content_str)
                )
            }
        };

        Ok(json.into_bytes())
    }

    fn end_streaming(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), DecodingError> {
        Ok(())
    }
}

impl JsonOutputPlugin {
    fn format_columns_json(&self, columns: &[String], types: &[String]) -> String {
        let cols: Vec<String> = columns.iter().zip(types.iter())
            .map(|(name, typ)| format!(r#"{{"name":"{}","type":"{}"}}"#, name, typ))
            .collect();
        format!("[{}]", cols.join(","))
    }

    fn format_tuple_json(&self, columns: &[String], tuple: &TupleData) -> String {
        let pairs: Vec<String> = columns.iter().zip(tuple.values.iter())
            .map(|(col, val)| {
                match val {
                    Some(v) => format!(r#""{}":"{}""#, col, escape_json(v)),
                    None => format!(r#""{}":null"#, col),
                }
            })
            .collect();
        format!("{{{}}}", pairs.join(","))
    }
}

fn format_lsn(lsn: Lsn) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

// ============================================================================
// Replication Slots
// ============================================================================

/// Type of replication slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotType {
    /// Physical replication (streaming replica)
    Physical,
    /// Logical replication (change data capture)
    Logical,
}

/// Replication slot state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlotState {
    #[default]
    Inactive,
    Active,
    Reserved,
}

/// A replication slot for tracking changes
#[derive(Debug)]
pub struct ReplicationSlot {
    /// Slot name
    pub name: String,
    /// Slot type
    pub slot_type: SlotType,
    /// Output plugin name (for logical slots)
    pub plugin: Option<String>,
    /// Database OID (for logical slots)
    pub database: Option<u32>,
    /// Confirmed flush LSN
    pub confirmed_flush_lsn: AtomicU64,
    /// Restart LSN
    pub restart_lsn: AtomicU64,
    /// Catalog xmin
    pub catalog_xmin: AtomicU64,
    /// Current state
    state: RwLock<SlotState>,
    /// Active PID using this slot
    active_pid: RwLock<Option<u32>>,
    /// Creation time
    pub created_at: u64,
    /// Is temporary slot
    pub temporary: bool,
}

impl ReplicationSlot {
    /// Create a new physical replication slot
    pub fn new_physical(name: &str) -> Self {
        Self {
            name: name.to_string(),
            slot_type: SlotType::Physical,
            plugin: None,
            database: None,
            confirmed_flush_lsn: AtomicU64::new(0),
            restart_lsn: AtomicU64::new(0),
            catalog_xmin: AtomicU64::new(0),
            state: RwLock::new(SlotState::Inactive),
            active_pid: RwLock::new(None),
            created_at: current_timestamp(),
            temporary: false,
        }
    }

    /// Create a new logical replication slot
    pub fn new_logical(name: &str, plugin: &str, database: u32) -> Self {
        Self {
            name: name.to_string(),
            slot_type: SlotType::Logical,
            plugin: Some(plugin.to_string()),
            database: Some(database),
            confirmed_flush_lsn: AtomicU64::new(0),
            restart_lsn: AtomicU64::new(0),
            catalog_xmin: AtomicU64::new(0),
            state: RwLock::new(SlotState::Inactive),
            active_pid: RwLock::new(None),
            created_at: current_timestamp(),
            temporary: false,
        }
    }

    /// Acquire the slot for use
    pub fn acquire(&self, pid: u32) -> Result<(), DecodingError> {
        let mut state = self.state.write();
        if *state == SlotState::Active {
            return Err(DecodingError::SlotInUse(self.name.clone()));
        }
        *state = SlotState::Active;
        *self.active_pid.write() = Some(pid);
        Ok(())
    }

    /// Release the slot
    pub fn release(&self) {
        *self.state.write() = SlotState::Inactive;
        *self.active_pid.write() = None;
    }

    /// Check if slot is active
    pub fn is_active(&self) -> bool {
        *self.state.read() == SlotState::Active
    }

    /// Get the current state
    pub fn state(&self) -> SlotState {
        *self.state.read()
    }

    /// Advance the confirmed flush LSN
    pub fn advance_flush_lsn(&self, lsn: Lsn) {
        self.confirmed_flush_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    /// Get the confirmed flush LSN
    pub fn get_flush_lsn(&self) -> Lsn {
        self.confirmed_flush_lsn.load(Ordering::SeqCst)
    }

    /// Get lag behind current WAL position
    pub fn lag(&self, current_lsn: Lsn) -> u64 {
        let flush_lsn = self.get_flush_lsn();
        current_lsn.saturating_sub(flush_lsn)
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Slot Manager
// ============================================================================

/// Manages replication slots
pub struct SlotManager {
    slots: RwLock<HashMap<String, Arc<ReplicationSlot>>>,
    max_slots: usize,
}

impl SlotManager {
    pub fn new(max_slots: usize) -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            max_slots,
        }
    }

    /// Create a physical replication slot
    pub fn create_physical_slot(&self, name: &str, temporary: bool) -> Result<Arc<ReplicationSlot>, DecodingError> {
        self.validate_slot_name(name)?;

        let mut slots = self.slots.write();
        if slots.len() >= self.max_slots {
            return Err(DecodingError::TooManySlots(self.max_slots));
        }
        if slots.contains_key(name) {
            return Err(DecodingError::SlotExists(name.to_string()));
        }

        let mut slot = ReplicationSlot::new_physical(name);
        slot.temporary = temporary;
        let slot = Arc::new(slot);
        slots.insert(name.to_string(), slot.clone());
        Ok(slot)
    }

    /// Create a logical replication slot
    pub fn create_logical_slot(
        &self,
        name: &str,
        plugin: &str,
        database: u32,
        temporary: bool,
    ) -> Result<Arc<ReplicationSlot>, DecodingError> {
        self.validate_slot_name(name)?;
        self.validate_plugin(plugin)?;

        let mut slots = self.slots.write();
        if slots.len() >= self.max_slots {
            return Err(DecodingError::TooManySlots(self.max_slots));
        }
        if slots.contains_key(name) {
            return Err(DecodingError::SlotExists(name.to_string()));
        }

        let mut slot = ReplicationSlot::new_logical(name, plugin, database);
        slot.temporary = temporary;
        let slot = Arc::new(slot);
        slots.insert(name.to_string(), slot.clone());
        Ok(slot)
    }

    /// Drop a replication slot
    pub fn drop_slot(&self, name: &str) -> Result<(), DecodingError> {
        let mut slots = self.slots.write();

        let slot = slots.get(name)
            .ok_or_else(|| DecodingError::SlotNotFound(name.to_string()))?;

        if slot.is_active() {
            return Err(DecodingError::SlotInUse(name.to_string()));
        }

        slots.remove(name);
        Ok(())
    }

    /// Get a slot by name
    pub fn get_slot(&self, name: &str) -> Option<Arc<ReplicationSlot>> {
        self.slots.read().get(name).cloned()
    }

    /// List all slots
    pub fn list_slots(&self) -> Vec<Arc<ReplicationSlot>> {
        self.slots.read().values().cloned().collect()
    }

    /// Drop all temporary slots
    pub fn drop_temporary_slots(&self) {
        let mut slots = self.slots.write();
        slots.retain(|_, slot| !slot.temporary || slot.is_active());
    }

    fn validate_slot_name(&self, name: &str) -> Result<(), DecodingError> {
        if name.is_empty() {
            return Err(DecodingError::InvalidSlotName("empty name".to_string()));
        }
        if name.len() > 63 {
            return Err(DecodingError::InvalidSlotName("name too long".to_string()));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(DecodingError::InvalidSlotName("invalid characters".to_string()));
        }
        Ok(())
    }

    fn validate_plugin(&self, plugin: &str) -> Result<(), DecodingError> {
        let valid_plugins = ["test_decoding", "pgoutput", "wal2json"];
        if !valid_plugins.contains(&plugin) {
            return Err(DecodingError::UnknownPlugin(plugin.to_string()));
        }
        Ok(())
    }
}

// ============================================================================
// Logical Decoder
// ============================================================================

/// Configuration for logical decoding
#[derive(Debug, Clone)]
pub struct DecodingConfig {
    /// Maximum number of changes to buffer
    pub max_buffer_size: usize,
    /// Include origin information
    pub include_origin: bool,
    /// Include transaction timestamps
    pub include_timestamp: bool,
    /// Stream large transactions
    pub streaming: bool,
}

impl Default for DecodingConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 10000,
            include_origin: false,
            include_timestamp: true,
            streaming: false,
        }
    }
}

/// The main logical decoder
pub struct LogicalDecoder {
    config: DecodingConfig,
    slot: Arc<ReplicationSlot>,
    plugin: RwLock<Box<dyn OutputPlugin>>,
    buffer: RwLock<VecDeque<DecodedChange>>,
    current_xid: AtomicU64,
    last_lsn: AtomicU64,
    active: AtomicBool,
}

impl LogicalDecoder {
    /// Create a new logical decoder
    pub fn new(
        slot: Arc<ReplicationSlot>,
        plugin: Box<dyn OutputPlugin>,
        config: DecodingConfig,
    ) -> Self {
        Self {
            config,
            slot,
            plugin: RwLock::new(plugin),
            buffer: RwLock::new(VecDeque::new()),
            current_xid: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
            active: AtomicBool::new(false),
        }
    }

    /// Start decoding
    pub fn start(&self, pid: u32, options: &HashMap<String, String>) -> Result<(), DecodingError> {
        self.slot.acquire(pid)?;

        {
            let mut plugin = self.plugin.write();
            plugin.startup(options)?;
            plugin.begin_streaming()?;
        }

        self.active.store(true, Ordering::SeqCst);
        self.last_lsn.store(self.slot.get_flush_lsn(), Ordering::SeqCst);

        Ok(())
    }

    /// Stop decoding
    pub fn stop(&self) -> Result<(), DecodingError> {
        self.active.store(false, Ordering::SeqCst);

        {
            let mut plugin = self.plugin.write();
            plugin.end_streaming()?;
            plugin.shutdown()?;
        }

        self.slot.release();
        Ok(())
    }

    /// Decode a change and add to buffer
    pub fn decode_change(&self, change: DecodedChange) -> Result<(), DecodingError> {
        if !self.active.load(Ordering::SeqCst) {
            return Err(DecodingError::NotActive);
        }

        // Update tracking
        match &change {
            DecodedChange::Begin { xid, lsn, .. } => {
                self.current_xid.store(*xid, Ordering::SeqCst);
                self.last_lsn.store(*lsn, Ordering::SeqCst);
            }
            DecodedChange::Change { lsn, .. } => {
                self.last_lsn.store(*lsn, Ordering::SeqCst);
            }
            DecodedChange::Commit { commit_lsn, .. } => {
                self.last_lsn.store(*commit_lsn, Ordering::SeqCst);
                self.slot.advance_flush_lsn(*commit_lsn);
            }
            DecodedChange::Truncate { lsn, .. } | DecodedChange::Message { lsn, .. } => {
                self.last_lsn.store(*lsn, Ordering::SeqCst);
            }
        }

        // Add to buffer
        let mut buffer = self.buffer.write();
        if buffer.len() >= self.config.max_buffer_size {
            return Err(DecodingError::BufferFull);
        }
        buffer.push_back(change);

        Ok(())
    }

    /// Get the next formatted change
    pub fn get_next_change(&self) -> Result<Option<Vec<u8>>, DecodingError> {
        let change = self.buffer.write().pop_front();
        match change {
            Some(c) => {
                let plugin = self.plugin.read();
                Ok(Some(plugin.format_change(&c)?))
            }
            None => Ok(None),
        }
    }

    /// Get all pending formatted changes
    pub fn get_all_changes(&self) -> Result<Vec<Vec<u8>>, DecodingError> {
        let changes: Vec<_> = self.buffer.write().drain(..).collect();
        let plugin = self.plugin.read();
        changes.into_iter()
            .map(|c| plugin.format_change(&c))
            .collect()
    }

    /// Check if decoder is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Get the last processed LSN
    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn.load(Ordering::SeqCst)
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.read().len()
    }
}

// ============================================================================
// Change Data Capture Stream
// ============================================================================

/// A stream of changes for CDC
pub struct ChangeStream {
    decoder: Arc<LogicalDecoder>,
    batch_size: usize,
}

impl ChangeStream {
    pub fn new(decoder: Arc<LogicalDecoder>, batch_size: usize) -> Self {
        Self { decoder, batch_size }
    }

    /// Read next batch of changes
    pub fn read_batch(&self) -> Result<Vec<Vec<u8>>, DecodingError> {
        let mut batch = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.decoder.get_next_change()? {
                Some(data) => batch.push(data),
                None => break,
            }
        }
        Ok(batch)
    }

    /// Check if stream has pending changes
    pub fn has_pending(&self) -> bool {
        self.decoder.buffer_size() > 0
    }

    /// Confirm flush up to LSN
    pub fn confirm_flush(&self, lsn: Lsn) {
        self.decoder.slot.advance_flush_lsn(lsn);
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum DecodingError {
    SlotNotFound(String),
    SlotExists(String),
    SlotInUse(String),
    InvalidSlotName(String),
    TooManySlots(usize),
    UnknownPlugin(String),
    PluginError(String),
    BufferFull,
    NotActive,
    WalReadError(String),
    FormatError(String),
}

impl std::fmt::Display for DecodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SlotNotFound(name) => write!(f, "Replication slot '{}' not found", name),
            Self::SlotExists(name) => write!(f, "Replication slot '{}' already exists", name),
            Self::SlotInUse(name) => write!(f, "Replication slot '{}' is in use", name),
            Self::InvalidSlotName(msg) => write!(f, "Invalid slot name: {}", msg),
            Self::TooManySlots(max) => write!(f, "Maximum number of slots ({}) reached", max),
            Self::UnknownPlugin(name) => write!(f, "Unknown output plugin: {}", name),
            Self::PluginError(msg) => write!(f, "Plugin error: {}", msg),
            Self::BufferFull => write!(f, "Change buffer is full"),
            Self::NotActive => write!(f, "Decoder is not active"),
            Self::WalReadError(msg) => write!(f, "WAL read error: {}", msg),
            Self::FormatError(msg) => write!(f, "Format error: {}", msg),
        }
    }
}

impl std::error::Error for DecodingError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_format() {
        assert_eq!(format_lsn(0x0000000100000001), "1/00000001");
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(0xFFFFFFFFFFFFFFFF), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn test_tuple_data() {
        let tuple = TupleData::new(vec![
            Some("hello".to_string()),
            None,
            Some("world".to_string()),
        ]);
        assert_eq!(tuple.values.len(), 3);
        assert_eq!(tuple.values[0], Some("hello".to_string()));
        assert_eq!(tuple.values[1], None);
    }

    #[test]
    fn test_row_change() {
        let change = RowChange {
            operation: OperationType::Insert,
            schema: "public".to_string(),
            table: "users".to_string(),
            old_tuple: None,
            new_tuple: Some(TupleData::new(vec![
                Some("1".to_string()),
                Some("Alice".to_string()),
            ])),
            columns: vec!["id".to_string(), "name".to_string()],
            column_types: vec!["integer".to_string(), "text".to_string()],
        };

        assert_eq!(change.operation, OperationType::Insert);
        assert!(change.old_tuple.is_none());
        assert!(change.new_tuple.is_some());
    }

    #[test]
    fn test_test_decoding_plugin() {
        let mut plugin = TestDecodingPlugin::new();
        plugin.startup(&HashMap::new()).unwrap();

        let change = DecodedChange::Begin {
            xid: 123,
            commit_time: 1234567890,
            lsn: 0x100001000,
        };

        let output = plugin.format_change(&change).unwrap();
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("BEGIN"));
        assert!(output_str.contains("123"));
    }

    #[test]
    fn test_test_decoding_insert() {
        let plugin = TestDecodingPlugin::new();

        let change = DecodedChange::Change {
            xid: 100,
            lsn: 0x100001000,
            change: RowChange {
                operation: OperationType::Insert,
                schema: "public".to_string(),
                table: "test".to_string(),
                old_tuple: None,
                new_tuple: Some(TupleData::new(vec![
                    Some("1".to_string()),
                    Some("Alice".to_string()),
                ])),
                columns: vec!["id".to_string(), "name".to_string()],
                column_types: vec!["integer".to_string(), "text".to_string()],
            },
        };

        let output = plugin.format_change(&change).unwrap();
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("public.test"));
        assert!(output_str.contains("INSERT"));
    }

    #[test]
    fn test_json_plugin() {
        let mut plugin = JsonOutputPlugin::new();
        plugin.startup(&HashMap::new()).unwrap();

        let change = DecodedChange::Begin {
            xid: 456,
            commit_time: 1234567890,
            lsn: 0x100001000,
        };

        let output = plugin.format_change(&change).unwrap();
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"type\":\"begin\""));
        assert!(output_str.contains("\"xid\":456"));
    }

    #[test]
    fn test_pgoutput_plugin() {
        let mut plugin = PgOutputPlugin::new();
        plugin.startup(&HashMap::new()).unwrap();

        let change = DecodedChange::Begin {
            xid: 789,
            commit_time: 1234567890,
            lsn: 0x100001000,
        };

        let output = plugin.format_change(&change).unwrap();
        assert_eq!(output[0], b'B'); // Begin message type
    }

    #[test]
    fn test_physical_slot() {
        let slot = ReplicationSlot::new_physical("test_slot");
        assert_eq!(slot.name, "test_slot");
        assert_eq!(slot.slot_type, SlotType::Physical);
        assert!(slot.plugin.is_none());
        assert!(!slot.is_active());
    }

    #[test]
    fn test_logical_slot() {
        let slot = ReplicationSlot::new_logical("cdc_slot", "test_decoding", 16384);
        assert_eq!(slot.name, "cdc_slot");
        assert_eq!(slot.slot_type, SlotType::Logical);
        assert_eq!(slot.plugin, Some("test_decoding".to_string()));
        assert_eq!(slot.database, Some(16384));
    }

    #[test]
    fn test_slot_acquire_release() {
        let slot = ReplicationSlot::new_physical("test");
        assert!(!slot.is_active());

        slot.acquire(12345).unwrap();
        assert!(slot.is_active());

        // Can't acquire again
        assert!(slot.acquire(99999).is_err());

        slot.release();
        assert!(!slot.is_active());

        // Can acquire again
        slot.acquire(11111).unwrap();
        assert!(slot.is_active());
    }

    #[test]
    fn test_slot_lsn_tracking() {
        let slot = ReplicationSlot::new_physical("lsn_test");
        assert_eq!(slot.get_flush_lsn(), 0);

        slot.advance_flush_lsn(100);
        assert_eq!(slot.get_flush_lsn(), 100);

        // Advancing to lower value has no effect
        slot.advance_flush_lsn(50);
        assert_eq!(slot.get_flush_lsn(), 100);

        slot.advance_flush_lsn(200);
        assert_eq!(slot.get_flush_lsn(), 200);
    }

    #[test]
    fn test_slot_lag() {
        let slot = ReplicationSlot::new_physical("lag_test");
        slot.advance_flush_lsn(100);

        assert_eq!(slot.lag(150), 50);
        assert_eq!(slot.lag(100), 0);
        assert_eq!(slot.lag(50), 0); // Current < flush means no lag
    }

    #[test]
    fn test_slot_manager_create() {
        let manager = SlotManager::new(10);

        let slot = manager.create_physical_slot("phys1", false).unwrap();
        assert_eq!(slot.name, "phys1");
        assert!(!slot.temporary);

        let slot = manager.create_logical_slot("log1", "test_decoding", 16384, true).unwrap();
        assert_eq!(slot.name, "log1");
        assert!(slot.temporary);
    }

    #[test]
    fn test_slot_manager_duplicate() {
        let manager = SlotManager::new(10);
        manager.create_physical_slot("dup_test", false).unwrap();

        let result = manager.create_physical_slot("dup_test", false);
        assert!(matches!(result, Err(DecodingError::SlotExists(_))));
    }

    #[test]
    fn test_slot_manager_max_slots() {
        let manager = SlotManager::new(2);
        manager.create_physical_slot("slot1", false).unwrap();
        manager.create_physical_slot("slot2", false).unwrap();

        let result = manager.create_physical_slot("slot3", false);
        assert!(matches!(result, Err(DecodingError::TooManySlots(2))));
    }

    #[test]
    fn test_slot_manager_invalid_plugin() {
        let manager = SlotManager::new(10);
        let result = manager.create_logical_slot("test", "invalid_plugin", 16384, false);
        assert!(matches!(result, Err(DecodingError::UnknownPlugin(_))));
    }

    #[test]
    fn test_slot_manager_drop() {
        let manager = SlotManager::new(10);
        manager.create_physical_slot("drop_me", false).unwrap();
        assert!(manager.get_slot("drop_me").is_some());

        manager.drop_slot("drop_me").unwrap();
        assert!(manager.get_slot("drop_me").is_none());
    }

    #[test]
    fn test_slot_manager_drop_active() {
        let manager = SlotManager::new(10);
        let slot = manager.create_physical_slot("active_slot", false).unwrap();
        slot.acquire(12345).unwrap();

        let result = manager.drop_slot("active_slot");
        assert!(matches!(result, Err(DecodingError::SlotInUse(_))));
    }

    #[test]
    fn test_slot_manager_list() {
        let manager = SlotManager::new(10);
        manager.create_physical_slot("list1", false).unwrap();
        manager.create_physical_slot("list2", false).unwrap();

        let slots = manager.list_slots();
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn test_decoding_config_default() {
        let config = DecodingConfig::default();
        assert_eq!(config.max_buffer_size, 10000);
        assert!(config.include_timestamp);
        assert!(!config.streaming);
    }

    #[test]
    fn test_logical_decoder() {
        let slot = Arc::new(ReplicationSlot::new_logical("decoder_slot", "test_decoding", 16384));
        let plugin = Box::new(TestDecodingPlugin::new());
        let config = DecodingConfig::default();

        let decoder = LogicalDecoder::new(slot, plugin, config);
        assert!(!decoder.is_active());
        assert_eq!(decoder.buffer_size(), 0);
    }

    #[test]
    fn test_decoder_start_stop() {
        let slot = Arc::new(ReplicationSlot::new_logical("start_stop", "test_decoding", 16384));
        let plugin = Box::new(TestDecodingPlugin::new());
        let config = DecodingConfig::default();

        let decoder = LogicalDecoder::new(slot.clone(), plugin, config);

        decoder.start(12345, &HashMap::new()).unwrap();
        assert!(decoder.is_active());
        assert!(slot.is_active());

        decoder.stop().unwrap();
        assert!(!decoder.is_active());
        assert!(!slot.is_active());
    }

    #[test]
    fn test_decoder_changes() {
        let slot = Arc::new(ReplicationSlot::new_logical("changes", "test_decoding", 16384));
        let plugin = Box::new(TestDecodingPlugin::new());
        let config = DecodingConfig::default();

        let decoder = LogicalDecoder::new(slot, plugin, config);
        decoder.start(12345, &HashMap::new()).unwrap();

        // Decode some changes
        decoder.decode_change(DecodedChange::Begin {
            xid: 100,
            commit_time: 123456,
            lsn: 0x100001000,
        }).unwrap();

        decoder.decode_change(DecodedChange::Commit {
            xid: 100,
            commit_lsn: 0x100001100,
            commit_time: 123457,
        }).unwrap();

        assert_eq!(decoder.buffer_size(), 2);

        let output = decoder.get_next_change().unwrap();
        assert!(output.is_some());
        assert_eq!(decoder.buffer_size(), 1);

        decoder.stop().unwrap();
    }

    #[test]
    fn test_change_stream() {
        let slot = Arc::new(ReplicationSlot::new_logical("stream", "test_decoding", 16384));
        let plugin = Box::new(TestDecodingPlugin::new());
        let config = DecodingConfig::default();

        let decoder = Arc::new(LogicalDecoder::new(slot, plugin, config));
        decoder.start(12345, &HashMap::new()).unwrap();

        // Add changes
        for i in 0..5 {
            decoder.decode_change(DecodedChange::Begin {
                xid: i,
                commit_time: 123456,
                lsn: 0x100001000 + i,
            }).unwrap();
        }

        let stream = ChangeStream::new(decoder.clone(), 3);
        assert!(stream.has_pending());

        let batch = stream.read_batch().unwrap();
        assert_eq!(batch.len(), 3);

        let batch = stream.read_batch().unwrap();
        assert_eq!(batch.len(), 2);

        decoder.stop().unwrap();
    }

    #[test]
    fn test_decoding_error_display() {
        let errors = vec![
            DecodingError::SlotNotFound("test".to_string()),
            DecodingError::SlotExists("dup".to_string()),
            DecodingError::SlotInUse("busy".to_string()),
            DecodingError::InvalidSlotName("bad".to_string()),
            DecodingError::TooManySlots(10),
            DecodingError::UnknownPlugin("fake".to_string()),
            DecodingError::PluginError("oops".to_string()),
            DecodingError::BufferFull,
            DecodingError::NotActive,
            DecodingError::WalReadError("io".to_string()),
            DecodingError::FormatError("fmt".to_string()),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_escape_json() {
        assert_eq!(escape_json("hello"), "hello");
        assert_eq!(escape_json("hello\nworld"), "hello\\nworld");
        assert_eq!(escape_json("say \"hi\""), "say \\\"hi\\\"");
        assert_eq!(escape_json("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_operation_types() {
        assert_ne!(OperationType::Insert, OperationType::Update);
        assert_ne!(OperationType::Delete, OperationType::Truncate);
    }
}
