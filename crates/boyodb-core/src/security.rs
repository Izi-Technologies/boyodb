//! Security Hardening Module
//!
//! This module provides security features:
//! - Row-Level Security (RLS) for fine-grained access control
//! - Audit Logging for compliance and forensics
//! - Column Encryption for data protection at rest

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use rand::rngs::OsRng;
use rand::RngCore;

// ============================================================================
// Row-Level Security (RLS)
// ============================================================================

/// Row-level security policy
#[derive(Debug, Clone)]
pub struct RlsPolicy {
    /// Policy name
    pub name: String,
    /// Table this policy applies to
    pub table: String,
    /// Database
    pub database: String,
    /// Policy type (permissive or restrictive)
    pub policy_type: RlsPolicyType,
    /// Command types this policy applies to
    pub commands: Vec<RlsCommand>,
    /// Roles this policy applies to (empty = all roles)
    pub roles: Vec<String>,
    /// USING expression (for SELECT, UPDATE, DELETE)
    pub using_expr: Option<RlsExpression>,
    /// WITH CHECK expression (for INSERT, UPDATE)
    pub check_expr: Option<RlsExpression>,
    /// Is policy enabled
    pub enabled: bool,
    /// Created timestamp
    pub created_at: u64,
}

/// Policy type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RlsPolicyType {
    /// Permissive - rows passing ANY permissive policy are visible
    Permissive,
    /// Restrictive - rows must pass ALL restrictive policies
    Restrictive,
}

/// Commands that policies can apply to
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RlsCommand {
    Select,
    Insert,
    Update,
    Delete,
    All,
}

/// RLS expression for policy predicates
#[derive(Debug, Clone)]
pub enum RlsExpression {
    /// Column equals a value
    Eq { column: String, value: RlsValue },
    /// Column not equals a value
    Ne { column: String, value: RlsValue },
    /// Column in list of values
    In { column: String, values: Vec<RlsValue> },
    /// Column matches current user
    CurrentUser { column: String },
    /// Column matches current role
    CurrentRole { column: String },
    /// Column is in user's groups/teams
    InUserGroups { column: String },
    /// Custom SQL expression
    Custom(String),
    /// Logical AND
    And(Vec<RlsExpression>),
    /// Logical OR
    Or(Vec<RlsExpression>),
    /// Logical NOT
    Not(Box<RlsExpression>),
    /// Always true
    True,
    /// Always false
    False,
}

/// Value types for RLS expressions
#[derive(Debug, Clone, PartialEq)]
pub enum RlsValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    /// Reference to a session variable
    SessionVar(String),
}

/// Context for evaluating RLS policies
#[derive(Debug, Clone)]
pub struct RlsContext {
    /// Current user
    pub user: String,
    /// Current roles
    pub roles: Vec<String>,
    /// User's groups/teams
    pub groups: Vec<String>,
    /// Session variables
    pub session_vars: HashMap<String, RlsValue>,
    /// Current database
    pub database: String,
}

impl RlsContext {
    pub fn new(user: &str) -> Self {
        Self {
            user: user.to_string(),
            roles: Vec::new(),
            groups: Vec::new(),
            session_vars: HashMap::new(),
            database: "default".to_string(),
        }
    }

    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    pub fn with_groups(mut self, groups: Vec<String>) -> Self {
        self.groups = groups;
        self
    }

    pub fn set_var(&mut self, name: &str, value: RlsValue) {
        self.session_vars.insert(name.to_string(), value);
    }
}

/// Result of RLS policy evaluation
#[derive(Debug, Clone)]
pub struct RlsResult {
    /// Whether access is allowed
    pub allowed: bool,
    /// Filter predicate to apply (for SELECT)
    pub filter: Option<String>,
    /// Policies that were applied
    pub applied_policies: Vec<String>,
    /// Reason for denial (if not allowed)
    pub denial_reason: Option<String>,
}

/// Row-Level Security manager
pub struct RlsManager {
    /// Policies by table (database.table -> policies)
    policies: RwLock<HashMap<String, Vec<RlsPolicy>>>,
    /// Tables with RLS enabled
    enabled_tables: RwLock<HashSet<String>>,
    /// Bypass roles (superusers)
    bypass_roles: RwLock<HashSet<String>>,
}

impl RlsManager {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            enabled_tables: RwLock::new(HashSet::new()),
            bypass_roles: RwLock::new(HashSet::new()),
        }
    }

    /// Enable RLS on a table
    pub fn enable_rls(&self, database: &str, table: &str) {
        let key = format!("{}.{}", database, table);
        self.enabled_tables.write().unwrap().insert(key);
    }

    /// Disable RLS on a table
    pub fn disable_rls(&self, database: &str, table: &str) {
        let key = format!("{}.{}", database, table);
        self.enabled_tables.write().unwrap().remove(&key);
    }

    /// Check if RLS is enabled on a table
    pub fn is_rls_enabled(&self, database: &str, table: &str) -> bool {
        let key = format!("{}.{}", database, table);
        self.enabled_tables.read().unwrap().contains(&key)
    }

    /// Add a bypass role (superuser)
    pub fn add_bypass_role(&self, role: &str) {
        self.bypass_roles.write().unwrap().insert(role.to_string());
    }

    /// Remove a bypass role
    pub fn remove_bypass_role(&self, role: &str) {
        self.bypass_roles.write().unwrap().remove(role);
    }

    /// Create a policy
    pub fn create_policy(&self, policy: RlsPolicy) -> Result<(), RlsError> {
        let key = format!("{}.{}", policy.database, policy.table);

        let mut policies = self.policies.write().unwrap();
        let table_policies = policies.entry(key).or_default();

        // Check for duplicate name
        if table_policies.iter().any(|p| p.name == policy.name) {
            return Err(RlsError::PolicyExists(policy.name));
        }

        table_policies.push(policy);
        Ok(())
    }

    /// Drop a policy
    pub fn drop_policy(&self, database: &str, table: &str, name: &str) -> Result<RlsPolicy, RlsError> {
        let key = format!("{}.{}", database, table);

        let mut policies = self.policies.write().unwrap();
        let table_policies = policies.get_mut(&key)
            .ok_or_else(|| RlsError::PolicyNotFound(name.to_string()))?;

        let idx = table_policies.iter().position(|p| p.name == name)
            .ok_or_else(|| RlsError::PolicyNotFound(name.to_string()))?;

        Ok(table_policies.remove(idx))
    }

    /// Get all policies for a table
    pub fn get_policies(&self, database: &str, table: &str) -> Vec<RlsPolicy> {
        let key = format!("{}.{}", database, table);
        self.policies.read().unwrap()
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    /// Evaluate RLS for a command
    pub fn evaluate(
        &self,
        database: &str,
        table: &str,
        command: RlsCommand,
        context: &RlsContext,
    ) -> RlsResult {
        let key = format!("{}.{}", database, table);

        // Check if RLS is enabled
        if !self.enabled_tables.read().unwrap().contains(&key) {
            return RlsResult {
                allowed: true,
                filter: None,
                applied_policies: vec![],
                denial_reason: None,
            };
        }

        // Check for bypass roles
        let bypass_roles = self.bypass_roles.read().unwrap();
        for role in &context.roles {
            if bypass_roles.contains(role) {
                return RlsResult {
                    allowed: true,
                    filter: None,
                    applied_policies: vec!["BYPASS".to_string()],
                    denial_reason: None,
                };
            }
        }

        // Get applicable policies
        let policies = self.policies.read().unwrap();
        let table_policies = match policies.get(&key) {
            Some(p) => p.clone(),
            None => {
                // RLS enabled but no policies = deny all
                return RlsResult {
                    allowed: false,
                    filter: None,
                    applied_policies: vec![],
                    denial_reason: Some("RLS enabled but no policies defined".to_string()),
                };
            }
        };

        // Filter policies by command and role
        let applicable: Vec<_> = table_policies.iter()
            .filter(|p| p.enabled)
            .filter(|p| p.commands.contains(&command) || p.commands.contains(&RlsCommand::All))
            .filter(|p| {
                p.roles.is_empty() || p.roles.iter().any(|r| context.roles.contains(r))
            })
            .collect();

        if applicable.is_empty() {
            return RlsResult {
                allowed: false,
                filter: None,
                applied_policies: vec![],
                denial_reason: Some("No applicable RLS policies".to_string()),
            };
        }

        // Evaluate policies
        let mut permissive_filters = Vec::new();
        let mut restrictive_filters = Vec::new();
        let mut applied = Vec::new();

        for policy in applicable {
            if let Some(ref expr) = policy.using_expr {
                let filter = self.expr_to_sql(expr, context);

                match policy.policy_type {
                    RlsPolicyType::Permissive => permissive_filters.push(filter),
                    RlsPolicyType::Restrictive => restrictive_filters.push(filter),
                }
                applied.push(policy.name.clone());
            }
        }

        // Combine filters: (perm1 OR perm2 OR ...) AND rest1 AND rest2 AND ...
        let combined_filter = self.combine_filters(&permissive_filters, &restrictive_filters);

        RlsResult {
            allowed: true,
            filter: combined_filter,
            applied_policies: applied,
            denial_reason: None,
        }
    }

    /// Convert expression to SQL predicate
    fn expr_to_sql(&self, expr: &RlsExpression, context: &RlsContext) -> String {
        match expr {
            RlsExpression::Eq { column, value } => {
                format!("{} = {}", column, self.value_to_sql(value, context))
            }
            RlsExpression::Ne { column, value } => {
                format!("{} <> {}", column, self.value_to_sql(value, context))
            }
            RlsExpression::In { column, values } => {
                let vals: Vec<_> = values.iter()
                    .map(|v| self.value_to_sql(v, context))
                    .collect();
                format!("{} IN ({})", column, vals.join(", "))
            }
            RlsExpression::CurrentUser { column } => {
                format!("{} = '{}'", column, context.user)
            }
            RlsExpression::CurrentRole { column } => {
                if context.roles.is_empty() {
                    "FALSE".to_string()
                } else {
                    let roles: Vec<_> = context.roles.iter()
                        .map(|r| format!("'{}'", r))
                        .collect();
                    format!("{} IN ({})", column, roles.join(", "))
                }
            }
            RlsExpression::InUserGroups { column } => {
                if context.groups.is_empty() {
                    "FALSE".to_string()
                } else {
                    let groups: Vec<_> = context.groups.iter()
                        .map(|g| format!("'{}'", g))
                        .collect();
                    format!("{} IN ({})", column, groups.join(", "))
                }
            }
            RlsExpression::Custom(sql) => sql.clone(),
            RlsExpression::And(exprs) => {
                let parts: Vec<_> = exprs.iter()
                    .map(|e| format!("({})", self.expr_to_sql(e, context)))
                    .collect();
                parts.join(" AND ")
            }
            RlsExpression::Or(exprs) => {
                let parts: Vec<_> = exprs.iter()
                    .map(|e| format!("({})", self.expr_to_sql(e, context)))
                    .collect();
                parts.join(" OR ")
            }
            RlsExpression::Not(expr) => {
                format!("NOT ({})", self.expr_to_sql(expr, context))
            }
            RlsExpression::True => "TRUE".to_string(),
            RlsExpression::False => "FALSE".to_string(),
        }
    }

    fn value_to_sql(&self, value: &RlsValue, context: &RlsContext) -> String {
        match value {
            RlsValue::Null => "NULL".to_string(),
            RlsValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            RlsValue::Int64(i) => i.to_string(),
            RlsValue::Float64(f) => f.to_string(),
            RlsValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            RlsValue::SessionVar(name) => {
                context.session_vars.get(name)
                    .map(|v| self.value_to_sql(v, context))
                    .unwrap_or_else(|| "NULL".to_string())
            }
        }
    }

    fn combine_filters(&self, permissive: &[String], restrictive: &[String]) -> Option<String> {
        let mut parts = Vec::new();

        if !permissive.is_empty() {
            let perm_combined = permissive.iter()
                .map(|f| format!("({})", f))
                .collect::<Vec<_>>()
                .join(" OR ");
            parts.push(format!("({})", perm_combined));
        }

        for r in restrictive {
            parts.push(format!("({})", r));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        }
    }
}

impl Default for RlsManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum RlsError {
    PolicyExists(String),
    PolicyNotFound(String),
    InvalidExpression(String),
    EvaluationError(String),
}

impl std::fmt::Display for RlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RlsError::PolicyExists(name) => write!(f, "Policy already exists: {}", name),
            RlsError::PolicyNotFound(name) => write!(f, "Policy not found: {}", name),
            RlsError::InvalidExpression(e) => write!(f, "Invalid expression: {}", e),
            RlsError::EvaluationError(e) => write!(f, "Evaluation error: {}", e),
        }
    }
}

impl std::error::Error for RlsError {}

// ============================================================================
// Audit Logging
// ============================================================================

/// Audit event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuditEventType {
    // Authentication events
    LoginSuccess,
    LoginFailure,
    Logout,
    PasswordChange,

    // Authorization events
    PrivilegeGranted,
    PrivilegeRevoked,
    RoleAssigned,
    RoleRemoved,
    AccessDenied,

    // DDL events
    CreateDatabase,
    DropDatabase,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    CreateView,
    DropView,

    // DML events
    Select,
    Insert,
    Update,
    Delete,
    Truncate,

    // Admin events
    ConfigChange,
    BackupStarted,
    BackupCompleted,
    RestoreStarted,
    RestoreCompleted,
    ServerStart,
    ServerStop,

    // Security events
    RlsPolicyCreated,
    RlsPolicyDropped,
    EncryptionKeyRotated,
    SuspiciousActivity,
}

/// Audit log severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuditSeverity {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

/// Audit log entry
#[derive(Debug, Clone)]
pub struct AuditEntry {
    /// Unique entry ID
    pub id: u64,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp: u64,
    /// Event type
    pub event_type: AuditEventType,
    /// Severity level
    pub severity: AuditSeverity,
    /// User who triggered the event
    pub user: String,
    /// Client IP address
    pub client_ip: Option<String>,
    /// Database involved
    pub database: Option<String>,
    /// Table involved
    pub table: Option<String>,
    /// SQL query (if applicable)
    pub query: Option<String>,
    /// Query duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Rows affected
    pub rows_affected: Option<u64>,
    /// Additional details
    pub details: HashMap<String, String>,
    /// Success/failure
    pub success: bool,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Session ID
    pub session_id: Option<String>,
    /// Transaction ID
    pub transaction_id: Option<String>,
}

impl AuditEntry {
    pub fn new(event_type: AuditEventType, user: &str) -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        Self {
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            event_type,
            severity: AuditSeverity::Info,
            user: user.to_string(),
            client_ip: None,
            database: None,
            table: None,
            query: None,
            duration_ms: None,
            rows_affected: None,
            details: HashMap::new(),
            success: true,
            error: None,
            session_id: None,
            transaction_id: None,
        }
    }

    pub fn with_severity(mut self, severity: AuditSeverity) -> Self {
        self.severity = severity;
        self
    }

    pub fn with_database(mut self, database: &str) -> Self {
        self.database = Some(database.to_string());
        self
    }

    pub fn with_table(mut self, table: &str) -> Self {
        self.table = Some(table.to_string());
        self
    }

    pub fn with_query(mut self, query: &str) -> Self {
        self.query = Some(query.to_string());
        self
    }

    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    pub fn with_rows_affected(mut self, rows: u64) -> Self {
        self.rows_affected = Some(rows);
        self
    }

    pub fn with_client_ip(mut self, ip: &str) -> Self {
        self.client_ip = Some(ip.to_string());
        self
    }

    pub fn with_session(mut self, session_id: &str) -> Self {
        self.session_id = Some(session_id.to_string());
        self
    }

    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_error(mut self, error: &str) -> Self {
        self.success = false;
        self.error = Some(error.to_string());
        self
    }

    pub fn failed(mut self) -> Self {
        self.success = false;
        self
    }
}

/// Audit configuration
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Minimum severity to log
    pub min_severity: AuditSeverity,
    /// Event types to audit
    pub event_types: HashSet<AuditEventType>,
    /// Maximum entries to keep in memory
    pub max_entries: usize,
    /// Log to file path
    pub log_file: Option<String>,
    /// Include query text
    pub log_queries: bool,
    /// Truncate query text at this length
    pub max_query_length: usize,
    /// Users to exclude from auditing
    pub excluded_users: HashSet<String>,
    /// Include only these users (empty = all)
    pub included_users: HashSet<String>,
}

impl Default for AuditConfig {
    fn default() -> Self {
        let mut event_types = HashSet::new();
        // Default: audit security-relevant events
        event_types.insert(AuditEventType::LoginSuccess);
        event_types.insert(AuditEventType::LoginFailure);
        event_types.insert(AuditEventType::AccessDenied);
        event_types.insert(AuditEventType::PrivilegeGranted);
        event_types.insert(AuditEventType::PrivilegeRevoked);
        event_types.insert(AuditEventType::CreateTable);
        event_types.insert(AuditEventType::DropTable);
        event_types.insert(AuditEventType::Delete);
        event_types.insert(AuditEventType::Truncate);
        event_types.insert(AuditEventType::SuspiciousActivity);

        Self {
            enabled: true,
            min_severity: AuditSeverity::Info,
            event_types,
            max_entries: 100_000,
            log_file: None,
            log_queries: true,
            max_query_length: 10_000,
            excluded_users: HashSet::new(),
            included_users: HashSet::new(),
        }
    }
}

/// Audit log storage and query interface
pub struct AuditLogger {
    /// Configuration
    config: RwLock<AuditConfig>,
    /// In-memory log entries
    entries: RwLock<VecDeque<AuditEntry>>,
    /// Entry count by type
    counts: RwLock<HashMap<AuditEventType, u64>>,
}

impl AuditLogger {
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config: RwLock::new(config),
            entries: RwLock::new(VecDeque::new()),
            counts: RwLock::new(HashMap::new()),
        }
    }

    /// Log an audit entry
    pub fn log(&self, mut entry: AuditEntry) {
        let config = self.config.read().unwrap();

        // Check if auditing is enabled
        if !config.enabled {
            return;
        }

        // Check severity
        if entry.severity < config.min_severity {
            return;
        }

        // Check event type
        if !config.event_types.contains(&entry.event_type) {
            return;
        }

        // Check user filters
        if !config.excluded_users.is_empty() && config.excluded_users.contains(&entry.user) {
            return;
        }
        if !config.included_users.is_empty() && !config.included_users.contains(&entry.user) {
            return;
        }

        // Truncate query if needed
        if let Some(ref mut query) = entry.query {
            if query.len() > config.max_query_length {
                *query = format!("{}...[truncated]", &query[..config.max_query_length]);
            }
        }

        drop(config);

        // Update counts
        {
            let mut counts = self.counts.write().unwrap();
            *counts.entry(entry.event_type).or_insert(0) += 1;
        }

        // Add to log
        {
            let mut entries = self.entries.write().unwrap();
            let config = self.config.read().unwrap();

            entries.push_back(entry);

            // Trim if over limit
            while entries.len() > config.max_entries {
                entries.pop_front();
            }
        }
    }

    /// Query audit log
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let entries = self.entries.read().unwrap();

        entries.iter()
            .filter(|e| self.matches_filter(e, filter))
            .cloned()
            .take(filter.limit.unwrap_or(1000))
            .collect()
    }

    fn matches_filter(&self, entry: &AuditEntry, filter: &AuditFilter) -> bool {
        if let Some(ref user) = filter.user {
            if &entry.user != user {
                return false;
            }
        }

        if let Some(ref event_type) = filter.event_type {
            if &entry.event_type != event_type {
                return false;
            }
        }

        if let Some(ref severity) = filter.min_severity {
            if &entry.severity < severity {
                return false;
            }
        }

        if let Some(start) = filter.start_time {
            if entry.timestamp < start {
                return false;
            }
        }

        if let Some(end) = filter.end_time {
            if entry.timestamp > end {
                return false;
            }
        }

        if let Some(ref database) = filter.database {
            if entry.database.as_ref() != Some(database) {
                return false;
            }
        }

        if let Some(ref table) = filter.table {
            if entry.table.as_ref() != Some(table) {
                return false;
            }
        }

        if let Some(success) = filter.success {
            if entry.success != success {
                return false;
            }
        }

        true
    }

    /// Get event counts
    pub fn get_counts(&self) -> HashMap<AuditEventType, u64> {
        self.counts.read().unwrap().clone()
    }

    /// Get total entry count
    pub fn entry_count(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
        self.counts.write().unwrap().clear();
    }

    /// Update configuration
    pub fn update_config(&self, config: AuditConfig) {
        *self.config.write().unwrap() = config;
    }

    /// Get current configuration
    pub fn get_config(&self) -> AuditConfig {
        self.config.read().unwrap().clone()
    }
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new(AuditConfig::default())
    }
}

/// Filter for querying audit log
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub user: Option<String>,
    pub event_type: Option<AuditEventType>,
    pub min_severity: Option<AuditSeverity>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub database: Option<String>,
    pub table: Option<String>,
    pub success: Option<bool>,
    pub limit: Option<usize>,
}

// ============================================================================
// Column Encryption
// ============================================================================

/// Encryption algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (authenticated encryption)
    Aes256Gcm,
    /// AES-256-CBC with HMAC
    Aes256CbcHmac,
    /// ChaCha20-Poly1305
    ChaCha20Poly1305,
    /// Deterministic encryption (allows equality search)
    DeterministicAes256,
}

/// Encryption key metadata
#[derive(Debug, Clone)]
pub struct EncryptionKey {
    /// Key ID
    pub id: String,
    /// Key version
    pub version: u32,
    /// Algorithm this key is for
    pub algorithm: EncryptionAlgorithm,
    /// Created timestamp
    pub created_at: u64,
    /// Expires timestamp (None = never)
    pub expires_at: Option<u64>,
    /// Is key active
    pub active: bool,
    /// Key material (in real impl, would be in secure storage)
    key_material: Vec<u8>,
}

impl EncryptionKey {
    pub fn new(id: &str, algorithm: EncryptionAlgorithm, key_material: Vec<u8>) -> Self {
        Self {
            id: id.to_string(),
            version: 1,
            algorithm,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            expires_at: None,
            active: true,
            key_material,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now >= expires
        } else {
            false
        }
    }
}

/// Column encryption policy
#[derive(Debug, Clone)]
pub struct ColumnEncryptionPolicy {
    /// Database
    pub database: String,
    /// Table
    pub table: String,
    /// Column name
    pub column: String,
    /// Key ID to use
    pub key_id: String,
    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,
    /// Is encryption enabled
    pub enabled: bool,
}

/// Encrypted value wrapper
#[derive(Debug, Clone)]
pub struct EncryptedValue {
    /// Key ID used for encryption
    pub key_id: String,
    /// Key version
    pub key_version: u32,
    /// Algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Initialization vector / nonce
    pub iv: Vec<u8>,
    /// Ciphertext
    pub ciphertext: Vec<u8>,
    /// Authentication tag (for AEAD)
    pub tag: Option<Vec<u8>>,
}

impl EncryptedValue {
    /// Serialize to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Version byte
        result.push(1);

        // Key ID (length-prefixed)
        let key_id_bytes = self.key_id.as_bytes();
        result.push(key_id_bytes.len() as u8);
        result.extend_from_slice(key_id_bytes);

        // Key version
        result.extend_from_slice(&self.key_version.to_le_bytes());

        // Algorithm
        result.push(match self.algorithm {
            EncryptionAlgorithm::Aes256Gcm => 1,
            EncryptionAlgorithm::Aes256CbcHmac => 2,
            EncryptionAlgorithm::ChaCha20Poly1305 => 3,
            EncryptionAlgorithm::DeterministicAes256 => 4,
        });

        // IV length + IV
        result.push(self.iv.len() as u8);
        result.extend_from_slice(&self.iv);

        // Tag length + tag (if present)
        if let Some(ref tag) = self.tag {
            result.push(tag.len() as u8);
            result.extend_from_slice(tag);
        } else {
            result.push(0);
        }

        // Ciphertext
        result.extend_from_slice(&self.ciphertext);

        result
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, EncryptionError> {
        if data.is_empty() {
            return Err(EncryptionError::InvalidData("Empty data".to_string()));
        }

        fn read_u8(data: &[u8], pos: &mut usize) -> Result<u8, EncryptionError> {
            if *pos >= data.len() {
                return Err(EncryptionError::InvalidData(
                    "Unexpected end of data".to_string(),
                ));
            }
            let value = data[*pos];
            *pos += 1;
            Ok(value)
        }

        fn read_slice<'a>(
            data: &'a [u8],
            pos: &mut usize,
            len: usize,
        ) -> Result<&'a [u8], EncryptionError> {
            if *pos + len > data.len() {
                return Err(EncryptionError::InvalidData(
                    "Encrypted value is truncated".to_string(),
                ));
            }
            let slice = &data[*pos..*pos + len];
            *pos += len;
            Ok(slice)
        }

        let mut pos = 0;

        // Version
        let _version = read_u8(data, &mut pos)?;

        // Key ID
        let key_id_len = read_u8(data, &mut pos)? as usize;
        let key_id_bytes = read_slice(data, &mut pos, key_id_len)?;
        let key_id = String::from_utf8(key_id_bytes.to_vec())
            .map_err(|_| EncryptionError::InvalidData("Invalid key ID".to_string()))?;

        // Key version
        let key_version_bytes = read_slice(data, &mut pos, 4)?;
        let key_version = u32::from_le_bytes(
            key_version_bytes
                .try_into()
                .map_err(|_| EncryptionError::InvalidData("Invalid key version".to_string()))?,
        );

        // Algorithm
        let algorithm = match read_u8(data, &mut pos)? {
            1 => EncryptionAlgorithm::Aes256Gcm,
            2 => EncryptionAlgorithm::Aes256CbcHmac,
            3 => EncryptionAlgorithm::ChaCha20Poly1305,
            4 => EncryptionAlgorithm::DeterministicAes256,
            _ => return Err(EncryptionError::InvalidData("Unknown algorithm".to_string())),
        };

        // IV
        let iv_len = read_u8(data, &mut pos)? as usize;
        let iv = read_slice(data, &mut pos, iv_len)?.to_vec();

        // Tag
        let tag_len = read_u8(data, &mut pos)? as usize;
        let tag = if tag_len > 0 {
            Some(read_slice(data, &mut pos, tag_len)?.to_vec())
        } else {
            None
        };

        // Ciphertext
        if pos > data.len() {
            return Err(EncryptionError::InvalidData(
                "Encrypted value is truncated".to_string(),
            ));
        }
        let ciphertext = data[pos..].to_vec();

        Ok(Self {
            key_id,
            key_version,
            algorithm,
            iv,
            ciphertext,
            tag,
        })
    }
}

/// Column encryption manager
pub struct ColumnEncryptionManager {
    /// Encryption keys
    keys: RwLock<HashMap<String, Vec<EncryptionKey>>>,
    /// Column policies
    policies: RwLock<HashMap<String, ColumnEncryptionPolicy>>,
    /// Master key (for key encryption - DEK encryption)
    master_key: RwLock<Option<Vec<u8>>>,
}

impl ColumnEncryptionManager {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            policies: RwLock::new(HashMap::new()),
            master_key: RwLock::new(None),
        }
    }

    /// Set master key
    pub fn set_master_key(&self, key: Vec<u8>) {
        *self.master_key.write().unwrap() = Some(key);
    }

    /// Create a new encryption key
    pub fn create_key(&self, id: &str, algorithm: EncryptionAlgorithm) -> Result<EncryptionKey, EncryptionError> {
        // Generate random key material (32 bytes for AES-256)
        let mut key_material = vec![0u8; 32];
        OsRng.fill_bytes(&mut key_material);

        let key = EncryptionKey::new(id, algorithm, key_material);

        let mut keys = self.keys.write().unwrap();
        keys.entry(id.to_string()).or_default().push(key.clone());

        Ok(key)
    }

    /// Get active key by ID
    pub fn get_key(&self, id: &str) -> Option<EncryptionKey> {
        let keys = self.keys.read().unwrap();
        keys.get(id)?
            .iter()
            .filter(|k| k.active && !k.is_expired())
            .max_by_key(|k| k.version)
            .cloned()
    }

    /// Rotate a key (create new version)
    pub fn rotate_key(&self, id: &str) -> Result<EncryptionKey, EncryptionError> {
        let mut keys = self.keys.write().unwrap();
        let key_versions = keys.get_mut(id)
            .ok_or_else(|| EncryptionError::KeyNotFound(id.to_string()))?;

        let last = key_versions.last()
            .ok_or_else(|| EncryptionError::KeyNotFound(id.to_string()))?;

        let algorithm = last.algorithm;
        let new_version = last.version + 1;

        // Mark old key as inactive
        for k in key_versions.iter_mut() {
            k.active = false;
        }

        // Generate new key material
        let mut key_material = vec![0u8; 32];
        OsRng.fill_bytes(&mut key_material);

        let mut new_key = EncryptionKey::new(id, algorithm, key_material);
        new_key.version = new_version;

        key_versions.push(new_key.clone());

        Ok(new_key)
    }

    /// Create encryption policy for a column
    pub fn create_policy(&self, policy: ColumnEncryptionPolicy) -> Result<(), EncryptionError> {
        // Verify key exists
        if self.get_key(&policy.key_id).is_none() {
            return Err(EncryptionError::KeyNotFound(policy.key_id.clone()));
        }

        let key = format!("{}.{}.{}", policy.database, policy.table, policy.column);
        self.policies.write().unwrap().insert(key, policy);
        Ok(())
    }

    /// Get policy for a column
    pub fn get_policy(&self, database: &str, table: &str, column: &str) -> Option<ColumnEncryptionPolicy> {
        let key = format!("{}.{}.{}", database, table, column);
        self.policies.read().unwrap().get(&key).cloned()
    }

    /// Drop policy for a column
    pub fn drop_policy(&self, database: &str, table: &str, column: &str) -> bool {
        let key = format!("{}.{}.{}", database, table, column);
        self.policies.write().unwrap().remove(&key).is_some()
    }

    /// List all policies for a table
    pub fn list_policies(&self, database: &str, table: &str) -> Vec<ColumnEncryptionPolicy> {
        let prefix = format!("{}.{}.", database, table);
        self.policies.read().unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Encrypt a value
    pub fn encrypt(&self, plaintext: &[u8], key_id: &str) -> Result<EncryptedValue, EncryptionError> {
        let key = self.get_key(key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(key_id.to_string()))?;

        match key.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = Aes256Gcm::new_from_slice(&key.key_material)
                    .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

                let mut iv = [0u8; 12];
                OsRng.fill_bytes(&mut iv);
                let nonce = Nonce::from_slice(&iv);

                let ciphertext_and_tag = cipher
                    .encrypt(nonce, plaintext)
                    .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;
                if ciphertext_and_tag.len() < 16 {
                    return Err(EncryptionError::EncryptionFailed(
                        "ciphertext too short".to_string(),
                    ));
                }
                let tag_start = ciphertext_and_tag.len() - 16;
                let ciphertext = ciphertext_and_tag[..tag_start].to_vec();
                let tag = ciphertext_and_tag[tag_start..].to_vec();

                Ok(EncryptedValue {
                    key_id: key.id,
                    key_version: key.version,
                    algorithm: key.algorithm,
                    iv: iv.to_vec(),
                    ciphertext,
                    tag: Some(tag),
                })
            }
            _ => Err(EncryptionError::EncryptionFailed(
                "unsupported encryption algorithm".to_string(),
            )),
        }
    }

    /// Decrypt a value
    pub fn decrypt(&self, encrypted: &EncryptedValue) -> Result<Vec<u8>, EncryptionError> {
        // Find the key version used
        let keys = self.keys.read().unwrap();
        let key_versions = keys.get(&encrypted.key_id)
            .ok_or_else(|| EncryptionError::KeyNotFound(encrypted.key_id.clone()))?;

        let key = key_versions.iter()
            .find(|k| k.version == encrypted.key_version)
            .ok_or_else(|| EncryptionError::KeyVersionNotFound(encrypted.key_id.clone(), encrypted.key_version))?;

        match key.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                if encrypted.iv.len() != 12 {
                    return Err(EncryptionError::DecryptionFailed(
                        "invalid nonce length".to_string(),
                    ));
                }
                let tag = encrypted
                    .tag
                    .as_ref()
                    .ok_or_else(|| EncryptionError::DecryptionFailed("missing tag".to_string()))?;
                if tag.len() != 16 {
                    return Err(EncryptionError::DecryptionFailed(
                        "invalid tag length".to_string(),
                    ));
                }
                let mut ciphertext_and_tag = Vec::with_capacity(encrypted.ciphertext.len() + tag.len());
                ciphertext_and_tag.extend_from_slice(&encrypted.ciphertext);
                ciphertext_and_tag.extend_from_slice(tag);

                let cipher = Aes256Gcm::new_from_slice(&key.key_material)
                    .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?;
                let nonce = Nonce::from_slice(&encrypted.iv);
                cipher
                    .decrypt(nonce, ciphertext_and_tag.as_ref())
                    .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))
            }
            _ => Err(EncryptionError::DecryptionFailed(
                "unsupported encryption algorithm".to_string(),
            )),
        }
    }

    /// Encrypt with column policy
    pub fn encrypt_column(&self, database: &str, table: &str, column: &str, plaintext: &[u8]) -> Result<EncryptedValue, EncryptionError> {
        let policy = self.get_policy(database, table, column)
            .ok_or_else(|| EncryptionError::NoPolicyDefined(format!("{}.{}.{}", database, table, column)))?;

        if !policy.enabled {
            return Err(EncryptionError::PolicyDisabled);
        }

        self.encrypt(plaintext, &policy.key_id)
    }

    /// Check if a column should be encrypted
    pub fn should_encrypt(&self, database: &str, table: &str, column: &str) -> bool {
        self.get_policy(database, table, column)
            .map(|p| p.enabled)
            .unwrap_or(false)
    }
}

impl Default for ColumnEncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum EncryptionError {
    KeyNotFound(String),
    KeyVersionNotFound(String, u32),
    KeyExpired(String),
    NoPolicyDefined(String),
    PolicyDisabled,
    InvalidData(String),
    DecryptionFailed(String),
    EncryptionFailed(String),
}

impl std::fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionError::KeyNotFound(id) => write!(f, "Encryption key not found: {}", id),
            EncryptionError::KeyVersionNotFound(id, v) => write!(f, "Key version not found: {} v{}", id, v),
            EncryptionError::KeyExpired(id) => write!(f, "Encryption key expired: {}", id),
            EncryptionError::NoPolicyDefined(col) => write!(f, "No encryption policy for column: {}", col),
            EncryptionError::PolicyDisabled => write!(f, "Encryption policy is disabled"),
            EncryptionError::InvalidData(e) => write!(f, "Invalid encrypted data: {}", e),
            EncryptionError::DecryptionFailed(e) => write!(f, "Decryption failed: {}", e),
            EncryptionError::EncryptionFailed(e) => write!(f, "Encryption failed: {}", e),
        }
    }
}

impl std::error::Error for EncryptionError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // RLS tests
    #[test]
    fn test_rls_enable_disable() {
        let manager = RlsManager::new();

        manager.enable_rls("db", "users");
        assert!(manager.is_rls_enabled("db", "users"));

        manager.disable_rls("db", "users");
        assert!(!manager.is_rls_enabled("db", "users"));
    }

    #[test]
    fn test_rls_create_policy() {
        let manager = RlsManager::new();

        let policy = RlsPolicy {
            name: "tenant_isolation".to_string(),
            table: "data".to_string(),
            database: "app".to_string(),
            policy_type: RlsPolicyType::Permissive,
            commands: vec![RlsCommand::All],
            roles: vec![],
            using_expr: Some(RlsExpression::CurrentUser { column: "owner".to_string() }),
            check_expr: None,
            enabled: true,
            created_at: 0,
        };

        manager.create_policy(policy).unwrap();

        let policies = manager.get_policies("app", "data");
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "tenant_isolation");
    }

    #[test]
    fn test_rls_duplicate_policy() {
        let manager = RlsManager::new();

        let policy = RlsPolicy {
            name: "policy1".to_string(),
            table: "t".to_string(),
            database: "db".to_string(),
            policy_type: RlsPolicyType::Permissive,
            commands: vec![RlsCommand::Select],
            roles: vec![],
            using_expr: None,
            check_expr: None,
            enabled: true,
            created_at: 0,
        };

        manager.create_policy(policy.clone()).unwrap();
        let result = manager.create_policy(policy);
        assert!(matches!(result, Err(RlsError::PolicyExists(_))));
    }

    #[test]
    fn test_rls_evaluate_no_rls() {
        let manager = RlsManager::new();
        let context = RlsContext::new("user1");

        let result = manager.evaluate("db", "table", RlsCommand::Select, &context);
        assert!(result.allowed);
        assert!(result.filter.is_none());
    }

    #[test]
    fn test_rls_evaluate_bypass() {
        let manager = RlsManager::new();
        manager.enable_rls("db", "table");
        manager.add_bypass_role("superuser");

        let context = RlsContext::new("admin")
            .with_roles(vec!["superuser".to_string()]);

        let result = manager.evaluate("db", "table", RlsCommand::Select, &context);
        assert!(result.allowed);
        assert_eq!(result.applied_policies, vec!["BYPASS"]);
    }

    #[test]
    fn test_rls_evaluate_with_policy() {
        let manager = RlsManager::new();
        manager.enable_rls("db", "orders");

        let policy = RlsPolicy {
            name: "user_orders".to_string(),
            table: "orders".to_string(),
            database: "db".to_string(),
            policy_type: RlsPolicyType::Permissive,
            commands: vec![RlsCommand::Select],
            roles: vec![],
            using_expr: Some(RlsExpression::CurrentUser { column: "user_id".to_string() }),
            check_expr: None,
            enabled: true,
            created_at: 0,
        };

        manager.create_policy(policy).unwrap();

        let context = RlsContext::new("alice");
        let result = manager.evaluate("db", "orders", RlsCommand::Select, &context);

        assert!(result.allowed);
        assert!(result.filter.is_some());
        assert!(result.filter.unwrap().contains("user_id = 'alice'"));
    }

    #[test]
    fn test_rls_combined_policies() {
        let manager = RlsManager::new();
        manager.enable_rls("db", "data");

        // Permissive policy
        manager.create_policy(RlsPolicy {
            name: "own_data".to_string(),
            table: "data".to_string(),
            database: "db".to_string(),
            policy_type: RlsPolicyType::Permissive,
            commands: vec![RlsCommand::Select],
            roles: vec![],
            using_expr: Some(RlsExpression::CurrentUser { column: "owner".to_string() }),
            check_expr: None,
            enabled: true,
            created_at: 0,
        }).unwrap();

        // Restrictive policy
        manager.create_policy(RlsPolicy {
            name: "active_only".to_string(),
            table: "data".to_string(),
            database: "db".to_string(),
            policy_type: RlsPolicyType::Restrictive,
            commands: vec![RlsCommand::Select],
            roles: vec![],
            using_expr: Some(RlsExpression::Eq {
                column: "active".to_string(),
                value: RlsValue::Bool(true),
            }),
            check_expr: None,
            enabled: true,
            created_at: 0,
        }).unwrap();

        let context = RlsContext::new("bob");
        let result = manager.evaluate("db", "data", RlsCommand::Select, &context);

        assert!(result.allowed);
        let filter = result.filter.unwrap();
        assert!(filter.contains("owner = 'bob'"));
        assert!(filter.contains("active = TRUE"));
    }

    #[test]
    fn test_rls_expr_to_sql() {
        let manager = RlsManager::new();
        let context = RlsContext::new("user1")
            .with_roles(vec!["admin".to_string(), "user".to_string()]);

        let expr = RlsExpression::And(vec![
            RlsExpression::Eq {
                column: "status".to_string(),
                value: RlsValue::String("active".to_string()),
            },
            RlsExpression::CurrentRole { column: "role".to_string() },
        ]);

        let sql = manager.expr_to_sql(&expr, &context);
        assert!(sql.contains("status = 'active'"));
        assert!(sql.contains("role IN"));
    }

    // Audit tests
    #[test]
    fn test_audit_entry_builder() {
        let entry = AuditEntry::new(AuditEventType::Select, "alice")
            .with_database("mydb")
            .with_table("users")
            .with_query("SELECT * FROM users")
            .with_duration(100)
            .with_rows_affected(50);

        assert_eq!(entry.user, "alice");
        assert_eq!(entry.database, Some("mydb".to_string()));
        assert_eq!(entry.duration_ms, Some(100));
        assert!(entry.success);
    }

    #[test]
    fn test_audit_entry_failed() {
        let entry = AuditEntry::new(AuditEventType::LoginFailure, "bob")
            .with_error("Invalid password")
            .with_severity(AuditSeverity::Warning);

        assert!(!entry.success);
        assert_eq!(entry.error, Some("Invalid password".to_string()));
        assert_eq!(entry.severity, AuditSeverity::Warning);
    }

    #[test]
    fn test_audit_logger_log() {
        let mut config = AuditConfig::default();
        config.event_types.insert(AuditEventType::Select);

        let logger = AuditLogger::new(config);

        logger.log(AuditEntry::new(AuditEventType::Select, "user1"));
        logger.log(AuditEntry::new(AuditEventType::Select, "user2"));

        assert_eq!(logger.entry_count(), 2);
    }

    #[test]
    fn test_audit_logger_filter_event_type() {
        let mut config = AuditConfig::default();
        config.event_types.clear();
        config.event_types.insert(AuditEventType::LoginFailure);

        let logger = AuditLogger::new(config);

        logger.log(AuditEntry::new(AuditEventType::Select, "user1"));
        logger.log(AuditEntry::new(AuditEventType::LoginFailure, "user2"));

        // Only LoginFailure should be logged
        assert_eq!(logger.entry_count(), 1);
    }

    #[test]
    fn test_audit_logger_query() {
        let mut config = AuditConfig::default();
        config.event_types.insert(AuditEventType::Select);
        config.event_types.insert(AuditEventType::Insert);

        let logger = AuditLogger::new(config);

        logger.log(AuditEntry::new(AuditEventType::Select, "alice").with_database("db1"));
        logger.log(AuditEntry::new(AuditEventType::Select, "bob").with_database("db2"));
        logger.log(AuditEntry::new(AuditEventType::Insert, "alice").with_database("db1"));

        let filter = AuditFilter {
            user: Some("alice".to_string()),
            ..Default::default()
        };

        let results = logger.query(&filter);
        assert_eq!(results.len(), 2);

        let filter2 = AuditFilter {
            database: Some("db1".to_string()),
            ..Default::default()
        };

        let results2 = logger.query(&filter2);
        assert_eq!(results2.len(), 2);
    }

    #[test]
    fn test_audit_counts() {
        let mut config = AuditConfig::default();
        config.event_types.insert(AuditEventType::Select);
        config.event_types.insert(AuditEventType::Insert);

        let logger = AuditLogger::new(config);

        logger.log(AuditEntry::new(AuditEventType::Select, "user1"));
        logger.log(AuditEntry::new(AuditEventType::Select, "user2"));
        logger.log(AuditEntry::new(AuditEventType::Insert, "user1"));

        let counts = logger.get_counts();
        assert_eq!(counts.get(&AuditEventType::Select), Some(&2));
        assert_eq!(counts.get(&AuditEventType::Insert), Some(&1));
    }

    #[test]
    fn test_audit_excluded_users() {
        let mut config = AuditConfig::default();
        config.event_types.insert(AuditEventType::Select);
        config.excluded_users.insert("system".to_string());

        let logger = AuditLogger::new(config);

        logger.log(AuditEntry::new(AuditEventType::Select, "system"));
        logger.log(AuditEntry::new(AuditEventType::Select, "user1"));

        assert_eq!(logger.entry_count(), 1);
    }

    // Encryption tests
    #[test]
    fn test_encryption_key_create() {
        let manager = ColumnEncryptionManager::new();

        let key = manager.create_key("key1", EncryptionAlgorithm::Aes256Gcm).unwrap();

        assert_eq!(key.id, "key1");
        assert_eq!(key.version, 1);
        assert!(key.active);
    }

    #[test]
    fn test_encryption_key_rotate() {
        let manager = ColumnEncryptionManager::new();

        manager.create_key("key1", EncryptionAlgorithm::Aes256Gcm).unwrap();
        let rotated = manager.rotate_key("key1").unwrap();

        assert_eq!(rotated.version, 2);
        assert!(rotated.active);

        // Old key should be inactive
        let keys = manager.keys.read().unwrap();
        let key1_versions = keys.get("key1").unwrap();
        assert!(!key1_versions[0].active);
        assert!(key1_versions[1].active);
    }

    #[test]
    fn test_encryption_policy() {
        let manager = ColumnEncryptionManager::new();
        manager.create_key("pii_key", EncryptionAlgorithm::Aes256Gcm).unwrap();

        let policy = ColumnEncryptionPolicy {
            database: "app".to_string(),
            table: "users".to_string(),
            column: "ssn".to_string(),
            key_id: "pii_key".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            enabled: true,
        };

        manager.create_policy(policy).unwrap();

        let retrieved = manager.get_policy("app", "users", "ssn").unwrap();
        assert_eq!(retrieved.key_id, "pii_key");

        assert!(manager.should_encrypt("app", "users", "ssn"));
        assert!(!manager.should_encrypt("app", "users", "name"));
    }

    #[test]
    fn test_encrypt_decrypt() {
        let manager = ColumnEncryptionManager::new();
        manager.create_key("test_key", EncryptionAlgorithm::Aes256Gcm).unwrap();

        let plaintext = b"sensitive data";
        let encrypted = manager.encrypt(plaintext, "test_key").unwrap();

        assert_ne!(encrypted.ciphertext, plaintext);
        assert!(!encrypted.iv.is_empty());

        let decrypted = manager.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_with_policy() {
        let manager = ColumnEncryptionManager::new();
        manager.create_key("col_key", EncryptionAlgorithm::Aes256Gcm).unwrap();

        manager.create_policy(ColumnEncryptionPolicy {
            database: "db".to_string(),
            table: "tbl".to_string(),
            column: "secret".to_string(),
            key_id: "col_key".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            enabled: true,
        }).unwrap();

        let encrypted = manager.encrypt_column("db", "tbl", "secret", b"my secret").unwrap();
        let decrypted = manager.decrypt(&encrypted).unwrap();

        assert_eq!(decrypted, b"my secret");
    }

    #[test]
    fn test_encrypted_value_serialization() {
        let encrypted = EncryptedValue {
            key_id: "key1".to_string(),
            key_version: 3,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            iv: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            ciphertext: vec![100, 101, 102, 103],
            tag: Some(vec![200, 201, 202, 203]),
        };

        let bytes = encrypted.to_bytes();
        let restored = EncryptedValue::from_bytes(&bytes).unwrap();

        assert_eq!(restored.key_id, encrypted.key_id);
        assert_eq!(restored.key_version, encrypted.key_version);
        assert_eq!(restored.algorithm, encrypted.algorithm);
        assert_eq!(restored.iv, encrypted.iv);
        assert_eq!(restored.ciphertext, encrypted.ciphertext);
        assert_eq!(restored.tag, encrypted.tag);
    }

    #[test]
    fn test_encryption_key_not_found() {
        let manager = ColumnEncryptionManager::new();

        let result = manager.encrypt(b"data", "nonexistent");
        assert!(matches!(result, Err(EncryptionError::KeyNotFound(_))));
    }

    #[test]
    fn test_list_encryption_policies() {
        let manager = ColumnEncryptionManager::new();
        manager.create_key("key1", EncryptionAlgorithm::Aes256Gcm).unwrap();

        manager.create_policy(ColumnEncryptionPolicy {
            database: "db".to_string(),
            table: "users".to_string(),
            column: "ssn".to_string(),
            key_id: "key1".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            enabled: true,
        }).unwrap();

        manager.create_policy(ColumnEncryptionPolicy {
            database: "db".to_string(),
            table: "users".to_string(),
            column: "dob".to_string(),
            key_id: "key1".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            enabled: true,
        }).unwrap();

        let policies = manager.list_policies("db", "users");
        assert_eq!(policies.len(), 2);
    }

    #[test]
    fn test_drop_encryption_policy() {
        let manager = ColumnEncryptionManager::new();
        manager.create_key("key1", EncryptionAlgorithm::Aes256Gcm).unwrap();

        manager.create_policy(ColumnEncryptionPolicy {
            database: "db".to_string(),
            table: "tbl".to_string(),
            column: "col".to_string(),
            key_id: "key1".to_string(),
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            enabled: true,
        }).unwrap();

        assert!(manager.drop_policy("db", "tbl", "col"));
        assert!(!manager.should_encrypt("db", "tbl", "col"));
    }

    #[test]
    fn test_rls_context() {
        let mut context = RlsContext::new("alice")
            .with_roles(vec!["user".to_string(), "admin".to_string()])
            .with_groups(vec!["engineering".to_string()]);

        context.set_var("tenant_id", RlsValue::Int64(42));

        assert_eq!(context.user, "alice");
        assert_eq!(context.roles.len(), 2);
        assert_eq!(context.session_vars.get("tenant_id"), Some(&RlsValue::Int64(42)));
    }

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(config.enabled);
        assert!(config.event_types.contains(&AuditEventType::LoginFailure));
        assert!(config.event_types.contains(&AuditEventType::AccessDenied));
    }
}
