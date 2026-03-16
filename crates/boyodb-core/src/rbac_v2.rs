//! RBAC V2: Fine-Grained Column-Level Permissions
//!
//! Extends the basic RBAC system with column-level access control, allowing
//! administrators to restrict access to sensitive data at the column level.
//!
//! ## Features
//!
//! - Column-level SELECT permissions
//! - Column-level UPDATE permissions
//! - Row-level security (RLS) policies
//! - Dynamic data masking
//! - Hierarchical role inheritance
//! - Permission caching for performance
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                     Permission Check Flow                     │
//! │                                                              │
//! │  Query ──► Role Resolution ──► Permission Lookup ──► Decision │
//! │                │                     │                        │
//! │                ▼                     ▼                        │
//! │         Role Hierarchy         Column Grants                 │
//! │         (inheritance)          (SELECT/UPDATE)               │
//! │                                     │                        │
//! │                                     ▼                        │
//! │                              Row-Level Security              │
//! │                              (predicate filters)             │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Core Types
// ============================================================================

/// Unique identifier for a role
pub type RoleId = String;

/// Unique identifier for a user
pub type UserId = String;

/// Fully qualified column reference (database.table.column)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    pub database: String,
    pub table: String,
    pub column: String,
}

impl ColumnRef {
    pub fn new(database: &str, table: &str, column: &str) -> Self {
        Self {
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
        }
    }

    pub fn parse(fqn: &str) -> Option<Self> {
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() == 3 {
            Some(Self::new(parts[0], parts[1], parts[2]))
        } else {
            None
        }
    }

    pub fn to_fqn(&self) -> String {
        format!("{}.{}.{}", self.database, self.table, self.column)
    }

    pub fn table_ref(&self) -> TableRef {
        TableRef {
            database: self.database.clone(),
            table: self.table.clone(),
        }
    }
}

/// Fully qualified table reference (database.table)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            database: database.to_string(),
            table: table.to_string(),
        }
    }

    pub fn to_fqn(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

/// Permission type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Permission {
    /// Read access
    Select,
    /// Insert access
    Insert,
    /// Update access
    Update,
    /// Delete access
    Delete,
    /// All permissions
    All,
    /// Administrative access
    Admin,
}

impl Permission {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Permission::Select),
            "INSERT" => Some(Permission::Insert),
            "UPDATE" => Some(Permission::Update),
            "DELETE" => Some(Permission::Delete),
            "ALL" => Some(Permission::All),
            "ADMIN" => Some(Permission::Admin),
            _ => None,
        }
    }

    pub fn implies(&self, other: &Permission) -> bool {
        match self {
            Permission::All => true,
            Permission::Admin => true,
            _ => self == other,
        }
    }
}

// ============================================================================
// Role Definition
// ============================================================================

/// Role with permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role identifier
    pub id: RoleId,
    /// Human-readable name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Parent roles (for inheritance)
    pub parent_roles: Vec<RoleId>,
    /// Database-level permissions
    pub database_grants: HashMap<String, HashSet<Permission>>,
    /// Table-level permissions
    pub table_grants: HashMap<TableRef, HashSet<Permission>>,
    /// Column-level permissions (the key feature of RBAC V2)
    pub column_grants: HashMap<ColumnRef, HashSet<Permission>>,
    /// Column denials (explicit deny overrides grants)
    pub column_denials: HashMap<ColumnRef, HashSet<Permission>>,
    /// Row-level security policies
    pub row_policies: Vec<RowPolicy>,
    /// Data masking rules
    pub masking_rules: Vec<MaskingRule>,
    /// Is this a system role
    pub is_system: bool,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
}

impl Role {
    pub fn new(id: &str, name: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            parent_roles: Vec::new(),
            database_grants: HashMap::new(),
            table_grants: HashMap::new(),
            column_grants: HashMap::new(),
            column_denials: HashMap::new(),
            row_policies: Vec::new(),
            masking_rules: Vec::new(),
            is_system: false,
            created_at: now,
            modified_at: now,
        }
    }

    /// Grant database-level permission
    pub fn grant_database(&mut self, database: &str, permission: Permission) {
        self.database_grants
            .entry(database.to_string())
            .or_insert_with(HashSet::new)
            .insert(permission);
        self.touch();
    }

    /// Grant table-level permission
    pub fn grant_table(&mut self, table: TableRef, permission: Permission) {
        self.table_grants
            .entry(table)
            .or_insert_with(HashSet::new)
            .insert(permission);
        self.touch();
    }

    /// Grant column-level permission
    pub fn grant_column(&mut self, column: ColumnRef, permission: Permission) {
        self.column_grants
            .entry(column)
            .or_insert_with(HashSet::new)
            .insert(permission);
        self.touch();
    }

    /// Deny column-level permission (explicit deny)
    pub fn deny_column(&mut self, column: ColumnRef, permission: Permission) {
        self.column_denials
            .entry(column)
            .or_insert_with(HashSet::new)
            .insert(permission);
        self.touch();
    }

    /// Revoke database-level permission
    pub fn revoke_database(&mut self, database: &str, permission: Permission) {
        if let Some(perms) = self.database_grants.get_mut(database) {
            perms.remove(&permission);
        }
        self.touch();
    }

    /// Revoke table-level permission
    pub fn revoke_table(&mut self, table: &TableRef, permission: Permission) {
        if let Some(perms) = self.table_grants.get_mut(table) {
            perms.remove(&permission);
        }
        self.touch();
    }

    /// Revoke column-level permission
    pub fn revoke_column(&mut self, column: &ColumnRef, permission: Permission) {
        if let Some(perms) = self.column_grants.get_mut(column) {
            perms.remove(&permission);
        }
        self.touch();
    }

    /// Add row-level security policy
    pub fn add_row_policy(&mut self, policy: RowPolicy) {
        self.row_policies.push(policy);
        self.touch();
    }

    /// Add data masking rule
    pub fn add_masking_rule(&mut self, rule: MaskingRule) {
        self.masking_rules.push(rule);
        self.touch();
    }

    fn touch(&mut self) {
        self.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }
}

// ============================================================================
// Row-Level Security
// ============================================================================

/// Row-level security policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowPolicy {
    /// Policy name
    pub name: String,
    /// Target table
    pub table: TableRef,
    /// SQL predicate that must be satisfied (e.g., "tenant_id = current_tenant()")
    pub predicate: String,
    /// Policy type
    pub policy_type: RowPolicyType,
    /// Whether policy is enabled
    pub enabled: bool,
}

/// Row policy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RowPolicyType {
    /// Restrictive policy (AND with other policies)
    Restrictive,
    /// Permissive policy (OR with other policies)
    Permissive,
}

// ============================================================================
// Data Masking
// ============================================================================

/// Data masking rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskingRule {
    /// Rule name
    pub name: String,
    /// Target column
    pub column: ColumnRef,
    /// Masking function
    pub masking_type: MaskingType,
    /// Custom masking expression (for Custom type)
    pub custom_expression: Option<String>,
    /// Whether rule is enabled
    pub enabled: bool,
}

/// Masking type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskingType {
    /// Full masking (replace with NULL or placeholder)
    Full,
    /// Partial masking (show first/last N characters)
    Partial,
    /// Hash masking (replace with hash)
    Hash,
    /// Email masking (show domain only)
    Email,
    /// Phone masking (show last 4 digits)
    Phone,
    /// Credit card masking (show last 4 digits)
    CreditCard,
    /// SSN masking (show last 4 digits)
    Ssn,
    /// Custom masking expression
    Custom,
    /// Random substitution
    Random,
}

impl MaskingType {
    /// Apply masking to a string value
    pub fn apply(&self, value: &str) -> String {
        match self {
            MaskingType::Full => "***MASKED***".to_string(),
            MaskingType::Partial => {
                if value.len() <= 4 {
                    "****".to_string()
                } else {
                    format!("{}****{}", &value[..2], &value[value.len()-2..])
                }
            }
            MaskingType::Hash => {
                // Simple hash for demonstration
                let hash: u64 = value.bytes().fold(0u64, |acc, b| {
                    acc.wrapping_mul(31).wrapping_add(b as u64)
                });
                format!("{:016x}", hash)
            }
            MaskingType::Email => {
                if let Some(at_pos) = value.find('@') {
                    format!("****{}", &value[at_pos..])
                } else {
                    "****@****".to_string()
                }
            }
            MaskingType::Phone => {
                if value.len() >= 4 {
                    format!("***-***-{}", &value[value.len()-4..])
                } else {
                    "***-***-****".to_string()
                }
            }
            MaskingType::CreditCard => {
                if value.len() >= 4 {
                    format!("****-****-****-{}", &value[value.len()-4..])
                } else {
                    "****-****-****-****".to_string()
                }
            }
            MaskingType::Ssn => {
                if value.len() >= 4 {
                    format!("***-**-{}", &value[value.len()-4..])
                } else {
                    "***-**-****".to_string()
                }
            }
            MaskingType::Random => {
                // Random-ish substitution based on length
                "*".repeat(value.len().min(20))
            }
            MaskingType::Custom => value.to_string(), // Handled separately
        }
    }
}

// ============================================================================
// Permission Manager
// ============================================================================

/// Permission manager for RBAC V2
pub struct PermissionManager {
    /// All defined roles
    roles: RwLock<HashMap<RoleId, Role>>,
    /// User to role mappings
    user_roles: RwLock<HashMap<UserId, HashSet<RoleId>>>,
    /// Permission cache for performance
    cache: RwLock<PermissionCache>,
    /// Cache TTL in seconds
    cache_ttl_secs: u64,
}

/// Cached permission decisions
struct PermissionCache {
    /// Cache entries: (user_id, column_ref, permission) -> (allowed, expires_at)
    entries: HashMap<(UserId, ColumnRef, Permission), (bool, u64)>,
    /// Last cache clear time
    last_clear: u64,
}

impl Default for PermissionCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            last_clear: 0,
        }
    }
}

impl PermissionManager {
    /// Create a new permission manager
    pub fn new() -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
            user_roles: RwLock::new(HashMap::new()),
            cache: RwLock::new(PermissionCache::default()),
            cache_ttl_secs: 300, // 5 minute cache
        }
    }

    /// Create with custom cache TTL
    pub fn with_cache_ttl(cache_ttl_secs: u64) -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
            user_roles: RwLock::new(HashMap::new()),
            cache: RwLock::new(PermissionCache::default()),
            cache_ttl_secs,
        }
    }

    /// Register a role
    pub fn register_role(&self, role: Role) {
        let mut roles = self.roles.write();
        roles.insert(role.id.clone(), role);
        self.invalidate_cache();
    }

    /// Get a role by ID
    pub fn get_role(&self, role_id: &str) -> Option<Role> {
        let roles = self.roles.read();
        roles.get(role_id).cloned()
    }

    /// Delete a role
    pub fn delete_role(&self, role_id: &str) -> bool {
        let mut roles = self.roles.write();
        let removed = roles.remove(role_id).is_some();
        if removed {
            self.invalidate_cache();
        }
        removed
    }

    /// Assign a role to a user
    pub fn assign_role(&self, user_id: &str, role_id: &str) {
        let mut user_roles = self.user_roles.write();
        user_roles
            .entry(user_id.to_string())
            .or_insert_with(HashSet::new)
            .insert(role_id.to_string());
        self.invalidate_cache();
    }

    /// Revoke a role from a user
    pub fn revoke_role(&self, user_id: &str, role_id: &str) {
        let mut user_roles = self.user_roles.write();
        if let Some(roles) = user_roles.get_mut(user_id) {
            roles.remove(role_id);
        }
        self.invalidate_cache();
    }

    /// Get all roles for a user (including inherited roles)
    pub fn get_user_roles(&self, user_id: &str) -> HashSet<RoleId> {
        let user_roles = self.user_roles.read();
        let roles = self.roles.read();

        let direct_roles = user_roles.get(user_id).cloned().unwrap_or_default();
        let mut all_roles = HashSet::new();

        // Recursively collect inherited roles
        let mut to_process: Vec<RoleId> = direct_roles.into_iter().collect();
        while let Some(role_id) = to_process.pop() {
            if all_roles.insert(role_id.clone()) {
                if let Some(role) = roles.get(&role_id) {
                    for parent in &role.parent_roles {
                        if !all_roles.contains(parent) {
                            to_process.push(parent.clone());
                        }
                    }
                }
            }
        }

        all_roles
    }

    /// Check if user has permission on a column
    pub fn check_column_permission(
        &self,
        user_id: &str,
        column: &ColumnRef,
        permission: Permission,
    ) -> bool {
        // Check cache first
        if let Some(cached) = self.get_cached(user_id, column, permission) {
            return cached;
        }

        let allowed = self.check_column_permission_uncached(user_id, column, permission);

        // Cache the result
        self.set_cached(user_id, column, permission, allowed);

        allowed
    }

    fn check_column_permission_uncached(
        &self,
        user_id: &str,
        column: &ColumnRef,
        permission: Permission,
    ) -> bool {
        let user_roles = self.get_user_roles(user_id);
        let roles = self.roles.read();

        // First check for explicit denials (deny always wins)
        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                if let Some(denials) = role.column_denials.get(column) {
                    if denials.iter().any(|p| p.implies(&permission)) {
                        return false;
                    }
                }
            }
        }

        // Check for grants at various levels
        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                // Check column-level grants
                if let Some(grants) = role.column_grants.get(column) {
                    if grants.iter().any(|p| p.implies(&permission)) {
                        return true;
                    }
                }

                // Check table-level grants
                let table_ref = column.table_ref();
                if let Some(grants) = role.table_grants.get(&table_ref) {
                    if grants.iter().any(|p| p.implies(&permission)) {
                        return true;
                    }
                }

                // Check database-level grants
                if let Some(grants) = role.database_grants.get(&column.database) {
                    if grants.iter().any(|p| p.implies(&permission)) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Check if user has permission on a table
    pub fn check_table_permission(
        &self,
        user_id: &str,
        table: &TableRef,
        permission: Permission,
    ) -> bool {
        let user_roles = self.get_user_roles(user_id);
        let roles = self.roles.read();

        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                // Check table-level grants
                if let Some(grants) = role.table_grants.get(table) {
                    if grants.iter().any(|p| p.implies(&permission)) {
                        return true;
                    }
                }

                // Check database-level grants
                if let Some(grants) = role.database_grants.get(&table.database) {
                    if grants.iter().any(|p| p.implies(&permission)) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Get allowed columns for a user on a table
    pub fn get_allowed_columns(
        &self,
        user_id: &str,
        table: &TableRef,
        all_columns: &[String],
        permission: Permission,
    ) -> Vec<String> {
        // If user has table-level permission, all columns are allowed
        if self.check_table_permission(user_id, table, permission) {
            return all_columns.to_vec();
        }

        // Otherwise, filter to only columns with explicit grants
        all_columns
            .iter()
            .filter(|col| {
                let col_ref = ColumnRef::new(&table.database, &table.table, col);
                self.check_column_permission(user_id, &col_ref, permission)
            })
            .cloned()
            .collect()
    }

    /// Get row-level security policies for a user on a table
    pub fn get_row_policies(&self, user_id: &str, table: &TableRef) -> Vec<RowPolicy> {
        let user_roles = self.get_user_roles(user_id);
        let roles = self.roles.read();

        let mut policies = Vec::new();
        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                for policy in &role.row_policies {
                    if policy.enabled && &policy.table == table {
                        policies.push(policy.clone());
                    }
                }
            }
        }

        policies
    }

    /// Get masking rules for a user on columns
    pub fn get_masking_rules(&self, user_id: &str, columns: &[ColumnRef]) -> Vec<MaskingRule> {
        let user_roles = self.get_user_roles(user_id);
        let roles = self.roles.read();

        let mut rules = Vec::new();
        for role_id in &user_roles {
            if let Some(role) = roles.get(role_id) {
                for rule in &role.masking_rules {
                    if rule.enabled && columns.contains(&rule.column) {
                        rules.push(rule.clone());
                    }
                }
            }
        }

        rules
    }

    /// Build combined RLS predicate for a table
    pub fn build_rls_predicate(&self, user_id: &str, table: &TableRef) -> Option<String> {
        let policies = self.get_row_policies(user_id, table);
        if policies.is_empty() {
            return None;
        }

        let mut restrictive = Vec::new();
        let mut permissive = Vec::new();

        for policy in policies {
            match policy.policy_type {
                RowPolicyType::Restrictive => restrictive.push(format!("({})", policy.predicate)),
                RowPolicyType::Permissive => permissive.push(format!("({})", policy.predicate)),
            }
        }

        let mut parts = Vec::new();

        // Restrictive policies are ANDed together
        if !restrictive.is_empty() {
            parts.push(restrictive.join(" AND "));
        }

        // Permissive policies are ORed together
        if !permissive.is_empty() {
            parts.push(format!("({})", permissive.join(" OR ")));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        }
    }

    fn get_cached(&self, user_id: &str, column: &ColumnRef, permission: Permission) -> Option<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let cache = self.cache.read();
        let key = (user_id.to_string(), column.clone(), permission);

        if let Some((allowed, expires_at)) = cache.entries.get(&key) {
            if *expires_at > now {
                return Some(*allowed);
            }
        }

        None
    }

    fn set_cached(&self, user_id: &str, column: &ColumnRef, permission: Permission, allowed: bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut cache = self.cache.write();
        let key = (user_id.to_string(), column.clone(), permission);
        cache.entries.insert(key, (allowed, now + self.cache_ttl_secs));
    }

    fn invalidate_cache(&self) {
        let mut cache = self.cache.write();
        cache.entries.clear();
        cache.last_clear = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }

    /// Export all roles for persistence
    pub fn export_roles(&self) -> Vec<Role> {
        let roles = self.roles.read();
        roles.values().cloned().collect()
    }

    /// Export user-role mappings for persistence
    pub fn export_user_roles(&self) -> HashMap<UserId, HashSet<RoleId>> {
        let user_roles = self.user_roles.read();
        user_roles.clone()
    }

    /// Import roles from persistence
    pub fn import_roles(&self, roles: Vec<Role>) {
        let mut role_map = self.roles.write();
        for role in roles {
            role_map.insert(role.id.clone(), role);
        }
        self.invalidate_cache();
    }

    /// Import user-role mappings from persistence
    pub fn import_user_roles(&self, mappings: HashMap<UserId, HashSet<RoleId>>) {
        let mut user_roles = self.user_roles.write();
        *user_roles = mappings;
        self.invalidate_cache();
    }
}

impl Default for PermissionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Query Filter Builder
// ============================================================================

/// Builds filtered queries based on RBAC permissions
pub struct RbacQueryFilter {
    permission_manager: Arc<PermissionManager>,
}

impl RbacQueryFilter {
    pub fn new(permission_manager: Arc<PermissionManager>) -> Self {
        Self { permission_manager }
    }

    /// Filter SELECT columns based on permissions
    pub fn filter_select_columns(
        &self,
        user_id: &str,
        table: &TableRef,
        requested_columns: &[String],
    ) -> Result<Vec<String>, RbacError> {
        let allowed = self.permission_manager.get_allowed_columns(
            user_id,
            table,
            requested_columns,
            Permission::Select,
        );

        if allowed.is_empty() && !requested_columns.is_empty() {
            return Err(RbacError::AccessDenied(format!(
                "No SELECT permission on any columns of {}.{}",
                table.database, table.table
            )));
        }

        Ok(allowed)
    }

    /// Check if user can INSERT into table
    pub fn check_insert(&self, user_id: &str, table: &TableRef) -> Result<(), RbacError> {
        if self.permission_manager.check_table_permission(user_id, table, Permission::Insert) {
            Ok(())
        } else {
            Err(RbacError::AccessDenied(format!(
                "No INSERT permission on {}.{}",
                table.database, table.table
            )))
        }
    }

    /// Check if user can UPDATE specific columns
    pub fn check_update_columns(
        &self,
        user_id: &str,
        table: &TableRef,
        columns: &[String],
    ) -> Result<(), RbacError> {
        for col in columns {
            let col_ref = ColumnRef::new(&table.database, &table.table, col);
            if !self.permission_manager.check_column_permission(
                user_id,
                &col_ref,
                Permission::Update,
            ) {
                return Err(RbacError::AccessDenied(format!(
                    "No UPDATE permission on column {}.{}.{}",
                    table.database, table.table, col
                )));
            }
        }
        Ok(())
    }

    /// Check if user can DELETE from table
    pub fn check_delete(&self, user_id: &str, table: &TableRef) -> Result<(), RbacError> {
        if self.permission_manager.check_table_permission(user_id, table, Permission::Delete) {
            Ok(())
        } else {
            Err(RbacError::AccessDenied(format!(
                "No DELETE permission on {}.{}",
                table.database, table.table
            )))
        }
    }

    /// Get RLS predicate to append to WHERE clause
    pub fn get_rls_filter(&self, user_id: &str, table: &TableRef) -> Option<String> {
        self.permission_manager.build_rls_predicate(user_id, table)
    }
}

// ============================================================================
// Error Types
// ============================================================================

/// RBAC error types
#[derive(Debug)]
pub enum RbacError {
    /// Access denied
    AccessDenied(String),
    /// Role not found
    RoleNotFound(String),
    /// Invalid permission
    InvalidPermission(String),
    /// Circular role inheritance
    CircularInheritance(String),
}

impl std::fmt::Display for RbacError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RbacError::AccessDenied(msg) => write!(f, "Access denied: {}", msg),
            RbacError::RoleNotFound(msg) => write!(f, "Role not found: {}", msg),
            RbacError::InvalidPermission(msg) => write!(f, "Invalid permission: {}", msg),
            RbacError::CircularInheritance(msg) => write!(f, "Circular inheritance: {}", msg),
        }
    }
}

impl std::error::Error for RbacError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref_parsing() {
        let col = ColumnRef::parse("db.table.column").unwrap();
        assert_eq!(col.database, "db");
        assert_eq!(col.table, "table");
        assert_eq!(col.column, "column");
        assert_eq!(col.to_fqn(), "db.table.column");
    }

    #[test]
    fn test_permission_implies() {
        assert!(Permission::All.implies(&Permission::Select));
        assert!(Permission::All.implies(&Permission::Insert));
        assert!(Permission::Admin.implies(&Permission::All));
        assert!(!Permission::Select.implies(&Permission::Insert));
    }

    #[test]
    fn test_basic_permissions() {
        let mgr = PermissionManager::new();

        // Create a role with table-level SELECT permission
        let mut role = Role::new("reader", "Reader Role");
        role.grant_table(TableRef::new("mydb", "users"), Permission::Select);
        mgr.register_role(role);

        // Assign to user
        mgr.assign_role("alice", "reader");

        // Check permissions
        let col = ColumnRef::new("mydb", "users", "name");
        assert!(mgr.check_column_permission("alice", &col, Permission::Select));
        assert!(!mgr.check_column_permission("alice", &col, Permission::Update));
        assert!(!mgr.check_column_permission("bob", &col, Permission::Select));
    }

    #[test]
    fn test_column_level_permissions() {
        let mgr = PermissionManager::new();

        // Create a role with column-level permissions
        let mut role = Role::new("partial_reader", "Partial Reader");
        role.grant_column(ColumnRef::new("mydb", "users", "name"), Permission::Select);
        role.grant_column(ColumnRef::new("mydb", "users", "email"), Permission::Select);
        // SSN column not granted
        mgr.register_role(role);

        mgr.assign_role("alice", "partial_reader");

        // Check permissions
        assert!(mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "name"),
            Permission::Select
        ));
        assert!(mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "email"),
            Permission::Select
        ));
        assert!(!mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "ssn"),
            Permission::Select
        ));
    }

    #[test]
    fn test_explicit_deny() {
        let mgr = PermissionManager::new();

        // Create a role with table-level grant but column-level deny
        let mut role = Role::new("restricted_reader", "Restricted Reader");
        role.grant_table(TableRef::new("mydb", "users"), Permission::Select);
        role.deny_column(ColumnRef::new("mydb", "users", "ssn"), Permission::Select);
        mgr.register_role(role);

        mgr.assign_role("alice", "restricted_reader");

        // Table-level grant should work for most columns
        assert!(mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "name"),
            Permission::Select
        ));

        // But explicit deny should block SSN
        assert!(!mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "ssn"),
            Permission::Select
        ));
    }

    #[test]
    fn test_role_inheritance() {
        let mgr = PermissionManager::new();

        // Create parent role
        let mut parent = Role::new("base_reader", "Base Reader");
        parent.grant_database("mydb", Permission::Select);
        mgr.register_role(parent);

        // Create child role
        let mut child = Role::new("extended_reader", "Extended Reader");
        child.parent_roles.push("base_reader".to_string());
        child.grant_table(TableRef::new("mydb", "extra"), Permission::Insert);
        mgr.register_role(child);

        mgr.assign_role("alice", "extended_reader");

        // Should have inherited SELECT from parent
        assert!(mgr.check_column_permission(
            "alice",
            &ColumnRef::new("mydb", "users", "name"),
            Permission::Select
        ));

        // Should have direct INSERT on extra table
        assert!(mgr.check_table_permission(
            "alice",
            &TableRef::new("mydb", "extra"),
            Permission::Insert
        ));
    }

    #[test]
    fn test_masking() {
        assert_eq!(MaskingType::Full.apply("secret"), "***MASKED***");
        assert_eq!(MaskingType::Email.apply("user@example.com"), "****@example.com");
        assert_eq!(MaskingType::Phone.apply("1234567890"), "***-***-7890");
        assert_eq!(MaskingType::CreditCard.apply("4111111111111111"), "****-****-****-1111");
        assert_eq!(MaskingType::Ssn.apply("123456789"), "***-**-6789");
    }

    #[test]
    fn test_rls_predicate_building() {
        let mgr = PermissionManager::new();

        let mut role = Role::new("tenant_user", "Tenant User");
        role.add_row_policy(RowPolicy {
            name: "tenant_isolation".to_string(),
            table: TableRef::new("mydb", "data"),
            predicate: "tenant_id = current_tenant()".to_string(),
            policy_type: RowPolicyType::Restrictive,
            enabled: true,
        });
        role.add_row_policy(RowPolicy {
            name: "public_access".to_string(),
            table: TableRef::new("mydb", "data"),
            predicate: "is_public = true".to_string(),
            policy_type: RowPolicyType::Permissive,
            enabled: true,
        });
        mgr.register_role(role);

        mgr.assign_role("alice", "tenant_user");

        let predicate = mgr.build_rls_predicate("alice", &TableRef::new("mydb", "data"));
        assert!(predicate.is_some());
        let pred = predicate.unwrap();
        assert!(pred.contains("tenant_id = current_tenant()"));
        assert!(pred.contains("is_public = true"));
    }
}
