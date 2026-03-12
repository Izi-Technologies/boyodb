//! Event Triggers - DDL Event Hooks
//!
//! This module provides event triggers for DDL operations:
//! - ddl_command_start - Before DDL execution
//! - ddl_command_end - After DDL execution
//! - table_rewrite - When table is rewritten
//! - sql_drop - When objects are dropped

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

// ============================================================================
// EVENT TYPES
// ============================================================================

/// Event trigger event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    /// Before DDL command execution
    DdlCommandStart,
    /// After DDL command execution
    DdlCommandEnd,
    /// Table rewrite events
    TableRewrite,
    /// Object drop events
    SqlDrop,
}

impl EventType {
    /// All event types
    pub fn all() -> &'static [EventType] {
        &[
            EventType::DdlCommandStart,
            EventType::DdlCommandEnd,
            EventType::TableRewrite,
            EventType::SqlDrop,
        ]
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            EventType::DdlCommandStart => "ddl_command_start",
            EventType::DdlCommandEnd => "ddl_command_end",
            EventType::TableRewrite => "table_rewrite",
            EventType::SqlDrop => "sql_drop",
        };
        write!(f, "{}", name)
    }
}

impl std::str::FromStr for EventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ddl_command_start" => Ok(EventType::DdlCommandStart),
            "ddl_command_end" => Ok(EventType::DdlCommandEnd),
            "table_rewrite" => Ok(EventType::TableRewrite),
            "sql_drop" => Ok(EventType::SqlDrop),
            _ => Err(format!("unknown event type: {}", s)),
        }
    }
}

/// DDL command tag (type of DDL operation)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DdlCommand {
    CreateTable,
    AlterTable,
    DropTable,
    CreateIndex,
    DropIndex,
    CreateView,
    DropView,
    CreateFunction,
    DropFunction,
    CreateTrigger,
    DropTrigger,
    CreateSchema,
    DropSchema,
    CreateSequence,
    DropSequence,
    CreateType,
    DropType,
    CreateDomain,
    DropDomain,
    Grant,
    Revoke,
    Comment,
    Other(String),
}

impl DdlCommand {
    /// Parse from command tag string
    pub fn from_tag(tag: &str) -> Self {
        let tag_upper = tag.to_uppercase();
        match tag_upper.as_str() {
            "CREATE TABLE" => DdlCommand::CreateTable,
            "ALTER TABLE" => DdlCommand::AlterTable,
            "DROP TABLE" => DdlCommand::DropTable,
            "CREATE INDEX" => DdlCommand::CreateIndex,
            "DROP INDEX" => DdlCommand::DropIndex,
            "CREATE VIEW" => DdlCommand::CreateView,
            "DROP VIEW" => DdlCommand::DropView,
            "CREATE FUNCTION" => DdlCommand::CreateFunction,
            "DROP FUNCTION" => DdlCommand::DropFunction,
            "CREATE TRIGGER" => DdlCommand::CreateTrigger,
            "DROP TRIGGER" => DdlCommand::DropTrigger,
            "CREATE SCHEMA" => DdlCommand::CreateSchema,
            "DROP SCHEMA" => DdlCommand::DropSchema,
            "CREATE SEQUENCE" => DdlCommand::CreateSequence,
            "DROP SEQUENCE" => DdlCommand::DropSequence,
            "CREATE TYPE" => DdlCommand::CreateType,
            "DROP TYPE" => DdlCommand::DropType,
            "CREATE DOMAIN" => DdlCommand::CreateDomain,
            "DROP DOMAIN" => DdlCommand::DropDomain,
            "GRANT" => DdlCommand::Grant,
            "REVOKE" => DdlCommand::Revoke,
            "COMMENT" => DdlCommand::Comment,
            _ => DdlCommand::Other(tag.to_string()),
        }
    }

    /// Get command tag string
    pub fn tag(&self) -> String {
        match self {
            DdlCommand::CreateTable => "CREATE TABLE".to_string(),
            DdlCommand::AlterTable => "ALTER TABLE".to_string(),
            DdlCommand::DropTable => "DROP TABLE".to_string(),
            DdlCommand::CreateIndex => "CREATE INDEX".to_string(),
            DdlCommand::DropIndex => "DROP INDEX".to_string(),
            DdlCommand::CreateView => "CREATE VIEW".to_string(),
            DdlCommand::DropView => "DROP VIEW".to_string(),
            DdlCommand::CreateFunction => "CREATE FUNCTION".to_string(),
            DdlCommand::DropFunction => "DROP FUNCTION".to_string(),
            DdlCommand::CreateTrigger => "CREATE TRIGGER".to_string(),
            DdlCommand::DropTrigger => "DROP TRIGGER".to_string(),
            DdlCommand::CreateSchema => "CREATE SCHEMA".to_string(),
            DdlCommand::DropSchema => "DROP SCHEMA".to_string(),
            DdlCommand::CreateSequence => "CREATE SEQUENCE".to_string(),
            DdlCommand::DropSequence => "DROP SEQUENCE".to_string(),
            DdlCommand::CreateType => "CREATE TYPE".to_string(),
            DdlCommand::DropType => "DROP TYPE".to_string(),
            DdlCommand::CreateDomain => "CREATE DOMAIN".to_string(),
            DdlCommand::DropDomain => "DROP DOMAIN".to_string(),
            DdlCommand::Grant => "GRANT".to_string(),
            DdlCommand::Revoke => "REVOKE".to_string(),
            DdlCommand::Comment => "COMMENT".to_string(),
            DdlCommand::Other(s) => s.clone(),
        }
    }

    /// Check if command matches a filter
    pub fn matches(&self, filter: &str) -> bool {
        let filter_upper = filter.to_uppercase();
        let tag = self.tag().to_uppercase();

        // Exact match
        if tag == filter_upper {
            return true;
        }

        // Category match (e.g., "CREATE" matches all CREATE commands)
        if filter_upper == "CREATE" && tag.starts_with("CREATE") {
            return true;
        }
        if filter_upper == "DROP" && tag.starts_with("DROP") {
            return true;
        }
        if filter_upper == "ALTER" && tag.starts_with("ALTER") {
            return true;
        }

        false
    }
}

// ============================================================================
// EVENT CONTEXT
// ============================================================================

/// Event trigger execution context
#[derive(Debug, Clone)]
pub struct EventContext {
    /// Event type
    pub event: EventType,
    /// DDL command tag
    pub command_tag: String,
    /// Parsed command
    pub command: DdlCommand,
    /// Object type (TABLE, INDEX, etc.)
    pub object_type: Option<String>,
    /// Schema name
    pub schema_name: Option<String>,
    /// Object name
    pub object_name: Option<String>,
    /// Original SQL statement
    pub original_sql: Option<String>,
    /// Current user
    pub current_user: String,
    /// Session ID
    pub session_id: u64,
    /// Transaction ID
    pub transaction_id: Option<u64>,
    /// Event timestamp
    pub timestamp: Instant,
    /// Additional info for sql_drop
    pub dropped_objects: Vec<DroppedObject>,
}

impl EventContext {
    /// Create a new event context
    pub fn new(event: EventType, command_tag: &str, user: &str) -> Self {
        Self {
            event,
            command_tag: command_tag.to_string(),
            command: DdlCommand::from_tag(command_tag),
            object_type: None,
            schema_name: None,
            object_name: None,
            original_sql: None,
            current_user: user.to_string(),
            session_id: 0,
            transaction_id: None,
            timestamp: Instant::now(),
            dropped_objects: Vec::new(),
        }
    }

    /// Set object info
    pub fn with_object(mut self, object_type: &str, schema: &str, name: &str) -> Self {
        self.object_type = Some(object_type.to_string());
        self.schema_name = Some(schema.to_string());
        self.object_name = Some(name.to_string());
        self
    }

    /// Set original SQL
    pub fn with_sql(mut self, sql: &str) -> Self {
        self.original_sql = Some(sql.to_string());
        self
    }

    /// Set session info
    pub fn with_session(mut self, session_id: u64, txn_id: Option<u64>) -> Self {
        self.session_id = session_id;
        self.transaction_id = txn_id;
        self
    }

    /// Add dropped object (for sql_drop events)
    pub fn add_dropped_object(&mut self, obj: DroppedObject) {
        self.dropped_objects.push(obj);
    }

    /// Get full object identity
    pub fn object_identity(&self) -> Option<String> {
        match (&self.schema_name, &self.object_name) {
            (Some(schema), Some(name)) => Some(format!("{}.{}", schema, name)),
            (None, Some(name)) => Some(name.clone()),
            _ => None,
        }
    }
}

/// Information about a dropped object
#[derive(Debug, Clone)]
pub struct DroppedObject {
    /// Object type
    pub object_type: String,
    /// Schema name
    pub schema_name: Option<String>,
    /// Object name
    pub object_name: String,
    /// Object identity (full path)
    pub object_identity: String,
    /// Address details
    pub address_names: Vec<String>,
    /// Is the drop cascaded
    pub cascaded: bool,
}

impl DroppedObject {
    /// Create a new dropped object
    pub fn new(object_type: &str, schema: Option<&str>, name: &str) -> Self {
        let identity = match schema {
            Some(s) => format!("{}.{}", s, name),
            None => name.to_string(),
        };

        Self {
            object_type: object_type.to_string(),
            schema_name: schema.map(|s| s.to_string()),
            object_name: name.to_string(),
            object_identity: identity,
            address_names: Vec::new(),
            cascaded: false,
        }
    }
}

// ============================================================================
// EVENT TRIGGER
// ============================================================================

/// Event trigger definition
#[derive(Debug, Clone)]
pub struct EventTrigger {
    /// Trigger name
    pub name: String,
    /// Event type
    pub event: EventType,
    /// Command filter (tags to fire on)
    pub tags: Option<Vec<String>>,
    /// Function to execute
    pub function_name: String,
    /// Is enabled
    pub enabled: EventTriggerEnabled,
    /// Owner
    pub owner: Option<String>,
    /// Comment
    pub comment: Option<String>,
}

/// Event trigger enabled state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTriggerEnabled {
    /// Always fires (except in single-user mode)
    Origin,
    /// Fires except during replication
    Replica,
    /// Always fires
    Always,
    /// Disabled
    Disabled,
}

impl Default for EventTriggerEnabled {
    fn default() -> Self {
        EventTriggerEnabled::Origin
    }
}

impl EventTrigger {
    /// Create a new event trigger
    pub fn new(name: &str, event: EventType, function: &str) -> Self {
        Self {
            name: name.to_string(),
            event,
            tags: None,
            function_name: function.to_string(),
            enabled: EventTriggerEnabled::default(),
            owner: None,
            comment: None,
        }
    }

    /// Set command filter
    pub fn when_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Enable/disable
    pub fn set_enabled(mut self, enabled: EventTriggerEnabled) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if trigger should fire for a command
    pub fn should_fire(&self, context: &EventContext) -> bool {
        // Check if enabled
        if self.enabled == EventTriggerEnabled::Disabled {
            return false;
        }

        // Check event type
        if self.event != context.event {
            return false;
        }

        // Check tag filter
        if let Some(ref tags) = self.tags {
            if !tags.iter().any(|t| context.command.matches(t)) {
                return false;
            }
        }

        true
    }

    /// Generate CREATE EVENT TRIGGER SQL
    pub fn to_sql(&self) -> String {
        let mut sql = format!(
            "CREATE EVENT TRIGGER {} ON {}",
            self.name, self.event
        );

        if let Some(ref tags) = self.tags {
            sql.push_str(&format!(
                "\n  WHEN TAG IN ({})",
                tags.iter()
                    .map(|t| format!("'{}'", t))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        sql.push_str(&format!("\n  EXECUTE FUNCTION {}()", self.function_name));

        sql
    }
}

// ============================================================================
// EVENT TRIGGER MANAGER
// ============================================================================

/// Result from event trigger execution
#[derive(Debug, Clone)]
pub struct EventTriggerResult {
    /// Trigger name
    pub trigger_name: String,
    /// Execution succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Execution time
    pub execution_time: std::time::Duration,
}

/// Event trigger function handler
pub type EventTriggerHandler = Box<dyn Fn(&EventContext) -> Result<(), String> + Send + Sync>;

/// Event trigger manager
pub struct EventTriggerManager {
    /// Registered triggers
    triggers: RwLock<HashMap<String, EventTrigger>>,
    /// Registered handlers
    handlers: RwLock<HashMap<String, Arc<EventTriggerHandler>>>,
    /// Event triggers enabled globally
    enabled: std::sync::atomic::AtomicBool,
    /// Triggers fired count
    triggers_fired: std::sync::atomic::AtomicU64,
}

impl EventTriggerManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            handlers: RwLock::new(HashMap::new()),
            enabled: std::sync::atomic::AtomicBool::new(true),
            triggers_fired: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Enable/disable all event triggers
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Create an event trigger
    pub fn create_trigger(&self, trigger: EventTrigger) -> Result<(), EventTriggerError> {
        let mut triggers = self.triggers.write().unwrap();

        if triggers.contains_key(&trigger.name) {
            return Err(EventTriggerError::AlreadyExists(trigger.name.clone()));
        }

        triggers.insert(trigger.name.clone(), trigger);
        Ok(())
    }

    /// Drop an event trigger
    pub fn drop_trigger(&self, name: &str) -> Result<(), EventTriggerError> {
        let mut triggers = self.triggers.write().unwrap();

        if triggers.remove(name).is_none() {
            return Err(EventTriggerError::NotFound(name.to_string()));
        }

        Ok(())
    }

    /// Alter trigger enabled state
    pub fn alter_trigger(&self, name: &str, enabled: EventTriggerEnabled) -> Result<(), EventTriggerError> {
        let mut triggers = self.triggers.write().unwrap();

        let trigger = triggers
            .get_mut(name)
            .ok_or_else(|| EventTriggerError::NotFound(name.to_string()))?;

        trigger.enabled = enabled;
        Ok(())
    }

    /// Register a handler function
    pub fn register_handler<F>(&self, function_name: &str, handler: F)
    where
        F: Fn(&EventContext) -> Result<(), String> + Send + Sync + 'static,
    {
        let mut handlers = self.handlers.write().unwrap();
        handlers.insert(function_name.to_string(), Arc::new(Box::new(handler)));
    }

    /// Fire event triggers
    pub fn fire(&self, context: &EventContext) -> Vec<EventTriggerResult> {
        if !self.is_enabled() {
            return Vec::new();
        }

        let mut results = Vec::new();
        let triggers = self.triggers.read().unwrap();
        let handlers = self.handlers.read().unwrap();

        for trigger in triggers.values() {
            if !trigger.should_fire(context) {
                continue;
            }

            let start = Instant::now();
            let result = if let Some(handler) = handlers.get(&trigger.function_name) {
                match handler(context) {
                    Ok(()) => EventTriggerResult {
                        trigger_name: trigger.name.clone(),
                        success: true,
                        error: None,
                        execution_time: start.elapsed(),
                    },
                    Err(e) => EventTriggerResult {
                        trigger_name: trigger.name.clone(),
                        success: false,
                        error: Some(e),
                        execution_time: start.elapsed(),
                    },
                }
            } else {
                EventTriggerResult {
                    trigger_name: trigger.name.clone(),
                    success: false,
                    error: Some(format!("handler function '{}' not found", trigger.function_name)),
                    execution_time: start.elapsed(),
                }
            };

            self.triggers_fired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            results.push(result);
        }

        results
    }

    /// Get trigger by name
    pub fn get_trigger(&self, name: &str) -> Option<EventTrigger> {
        let triggers = self.triggers.read().unwrap();
        triggers.get(name).cloned()
    }

    /// List all triggers
    pub fn list_triggers(&self) -> Vec<EventTrigger> {
        let triggers = self.triggers.read().unwrap();
        triggers.values().cloned().collect()
    }

    /// List triggers for an event
    pub fn list_triggers_for_event(&self, event: EventType) -> Vec<EventTrigger> {
        let triggers = self.triggers.read().unwrap();
        triggers
            .values()
            .filter(|t| t.event == event)
            .cloned()
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> EventTriggerStats {
        let triggers = self.triggers.read().unwrap();
        EventTriggerStats {
            total_triggers: triggers.len(),
            enabled_triggers: triggers.values().filter(|t| t.enabled != EventTriggerEnabled::Disabled).count(),
            triggers_fired: self.triggers_fired.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl Default for EventTriggerManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Event trigger statistics
#[derive(Debug, Clone)]
pub struct EventTriggerStats {
    pub total_triggers: usize,
    pub enabled_triggers: usize,
    pub triggers_fired: u64,
}

// ============================================================================
// ERRORS
// ============================================================================

/// Event trigger error
#[derive(Debug, Clone)]
pub enum EventTriggerError {
    /// Trigger not found
    NotFound(String),
    /// Trigger already exists
    AlreadyExists(String),
    /// Invalid event type
    InvalidEvent(String),
    /// Handler not found
    HandlerNotFound(String),
    /// Execution failed
    ExecutionFailed { trigger: String, error: String },
}

impl std::fmt::Display for EventTriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventTriggerError::NotFound(n) => write!(f, "event trigger \"{}\" does not exist", n),
            EventTriggerError::AlreadyExists(n) => write!(f, "event trigger \"{}\" already exists", n),
            EventTriggerError::InvalidEvent(e) => write!(f, "invalid event type: {}", e),
            EventTriggerError::HandlerNotFound(h) => write!(f, "handler function \"{}\" not found", h),
            EventTriggerError::ExecutionFailed { trigger, error } => {
                write!(f, "trigger \"{}\" failed: {}", trigger, error)
            }
        }
    }
}

impl std::error::Error for EventTriggerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_display() {
        assert_eq!(EventType::DdlCommandStart.to_string(), "ddl_command_start");
        assert_eq!(EventType::SqlDrop.to_string(), "sql_drop");
    }

    #[test]
    fn test_event_type_parse() {
        assert_eq!("ddl_command_start".parse::<EventType>().unwrap(), EventType::DdlCommandStart);
        assert_eq!("table_rewrite".parse::<EventType>().unwrap(), EventType::TableRewrite);
    }

    #[test]
    fn test_ddl_command_parse() {
        assert!(matches!(DdlCommand::from_tag("CREATE TABLE"), DdlCommand::CreateTable));
        assert!(matches!(DdlCommand::from_tag("DROP INDEX"), DdlCommand::DropIndex));
        assert!(matches!(DdlCommand::from_tag("UNKNOWN"), DdlCommand::Other(_)));
    }

    #[test]
    fn test_ddl_command_matches() {
        let cmd = DdlCommand::CreateTable;
        assert!(cmd.matches("CREATE TABLE"));
        assert!(cmd.matches("CREATE"));
        assert!(!cmd.matches("DROP"));
    }

    #[test]
    fn test_event_context() {
        let ctx = EventContext::new(EventType::DdlCommandEnd, "CREATE TABLE", "admin")
            .with_object("TABLE", "public", "users")
            .with_sql("CREATE TABLE public.users (id INT);");

        assert_eq!(ctx.object_identity(), Some("public.users".to_string()));
        assert_eq!(ctx.command_tag, "CREATE TABLE");
    }

    #[test]
    fn test_event_trigger_creation() {
        let trigger = EventTrigger::new("audit_ddl", EventType::DdlCommandEnd, "log_ddl")
            .when_tags(vec!["CREATE TABLE".to_string(), "DROP TABLE".to_string()]);

        assert_eq!(trigger.name, "audit_ddl");
        assert_eq!(trigger.event, EventType::DdlCommandEnd);
        assert!(trigger.tags.is_some());
    }

    #[test]
    fn test_event_trigger_should_fire() {
        let trigger = EventTrigger::new("test", EventType::DdlCommandEnd, "handler")
            .when_tags(vec!["CREATE TABLE".to_string()]);

        let ctx_create = EventContext::new(EventType::DdlCommandEnd, "CREATE TABLE", "user");
        let ctx_drop = EventContext::new(EventType::DdlCommandEnd, "DROP TABLE", "user");
        let ctx_start = EventContext::new(EventType::DdlCommandStart, "CREATE TABLE", "user");

        assert!(trigger.should_fire(&ctx_create));
        assert!(!trigger.should_fire(&ctx_drop));
        assert!(!trigger.should_fire(&ctx_start));
    }

    #[test]
    fn test_event_trigger_disabled() {
        let trigger = EventTrigger::new("test", EventType::DdlCommandEnd, "handler")
            .set_enabled(EventTriggerEnabled::Disabled);

        let ctx = EventContext::new(EventType::DdlCommandEnd, "CREATE TABLE", "user");
        assert!(!trigger.should_fire(&ctx));
    }

    #[test]
    fn test_event_trigger_manager() {
        let manager = EventTriggerManager::new();

        let trigger = EventTrigger::new("test", EventType::DdlCommandEnd, "handler");
        manager.create_trigger(trigger).unwrap();

        assert!(manager.get_trigger("test").is_some());
        assert!(manager.get_trigger("nonexistent").is_none());
    }

    #[test]
    fn test_event_trigger_manager_fire() {
        let manager = EventTriggerManager::new();

        let trigger = EventTrigger::new("test", EventType::DdlCommandEnd, "handler");
        manager.create_trigger(trigger).unwrap();

        manager.register_handler("handler", |_ctx| Ok(()));

        let ctx = EventContext::new(EventType::DdlCommandEnd, "CREATE TABLE", "user");
        let results = manager.fire(&ctx);

        assert_eq!(results.len(), 1);
        assert!(results[0].success);
    }

    #[test]
    fn test_event_trigger_to_sql() {
        let trigger = EventTrigger::new("audit", EventType::DdlCommandEnd, "log_changes")
            .when_tags(vec!["CREATE TABLE".to_string(), "DROP TABLE".to_string()]);

        let sql = trigger.to_sql();
        assert!(sql.contains("CREATE EVENT TRIGGER audit"));
        assert!(sql.contains("ON ddl_command_end"));
        assert!(sql.contains("WHEN TAG IN"));
    }

    #[test]
    fn test_dropped_object() {
        let obj = DroppedObject::new("TABLE", Some("public"), "users");
        assert_eq!(obj.object_identity, "public.users");
    }

    #[test]
    fn test_event_trigger_stats() {
        let manager = EventTriggerManager::new();

        manager.create_trigger(EventTrigger::new("t1", EventType::DdlCommandEnd, "h1")).unwrap();
        manager.create_trigger(
            EventTrigger::new("t2", EventType::DdlCommandEnd, "h2")
                .set_enabled(EventTriggerEnabled::Disabled)
        ).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total_triggers, 2);
        assert_eq!(stats.enabled_triggers, 1);
    }
}
