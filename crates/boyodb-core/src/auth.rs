//! Authentication and Authorization module for BOYODB
//!
//! This module provides comprehensive user management, password authentication,
//! and fine-grained permission control for database access.
//!
//! # Features
//! - User management (CREATE USER, DROP USER, ALTER USER)
//! - Secure password hashing using Argon2
//! - Role-based and object-level permissions
//! - Session management with authentication tokens
//! - Audit logging for security events

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use thiserror::Error;

/// Errors that can occur during authentication/authorization operations
#[derive(Debug, Clone, Error)]
pub enum AuthError {
    #[error("user not found: {0}")]
    UserNotFound(String),
    #[error("user already exists: {0}")]
    UserAlreadyExists(String),
    #[error("invalid password")]
    InvalidPassword,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("session expired")]
    SessionExpired,
    #[error("session not found")]
    SessionNotFound,
    #[error("invalid token")]
    InvalidToken,
    #[error("password too weak: {0}")]
    WeakPassword(String),
    #[error("cannot modify superuser")]
    CannotModifySuperuser,
    #[error("permission denied: requires {0}")]
    PermissionDenied(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("user is locked")]
    UserLocked,
    #[error("role not found: {0}")]
    RoleNotFound(String),
    #[error("role already exists: {0}")]
    RoleAlreadyExists(String),
    #[error("bootstrap password required: set BOYODB_BOOTSTRAP_PASSWORD")]
    BootstrapPasswordMissing,
}

/// Privilege types that can be granted on database objects
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Privilege {
    /// Can read data (SELECT)
    Select,
    /// Can insert data
    Insert,
    /// Can update data
    Update,
    /// Can delete data
    Delete,
    /// Can create tables
    Create,
    /// Can drop tables
    Drop,
    /// Can alter tables
    Alter,
    /// Can grant privileges to others
    Grant,
    /// Can truncate tables
    Truncate,
    /// All privileges combined
    All,
    /// Can connect to the database
    Connect,
    /// Can use the database/schema
    Usage,
    /// Can create databases
    CreateDb,
    /// Can create users
    CreateUser,
    /// Superuser privilege (all permissions)
    Superuser,
}

impl Privilege {
    /// Check if this privilege implies another privilege
    pub fn implies(&self, other: &Privilege) -> bool {
        match self {
            Privilege::All => true,
            Privilege::Superuser => true,
            _ => self == other,
        }
    }

    /// Get all privileges implied by this privilege
    pub fn expand(&self) -> Vec<Privilege> {
        match self {
            Privilege::All => vec![
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
                Privilege::Create,
                Privilege::Drop,
                Privilege::Alter,
                Privilege::Truncate,
                Privilege::Usage,
            ],
            Privilege::Superuser => vec![
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
                Privilege::Create,
                Privilege::Drop,
                Privilege::Alter,
                Privilege::Grant,
                Privilege::Truncate,
                Privilege::Connect,
                Privilege::Usage,
                Privilege::CreateDb,
                Privilege::CreateUser,
                Privilege::Superuser,
            ],
            _ => vec![*self],
        }
    }

    /// Parse a privilege from string
    pub fn parse(s: &str) -> Option<Privilege> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Privilege::Select),
            "INSERT" => Some(Privilege::Insert),
            "UPDATE" => Some(Privilege::Update),
            "DELETE" => Some(Privilege::Delete),
            "CREATE" => Some(Privilege::Create),
            "DROP" => Some(Privilege::Drop),
            "ALTER" => Some(Privilege::Alter),
            "GRANT" => Some(Privilege::Grant),
            "TRUNCATE" => Some(Privilege::Truncate),
            "ALL" | "ALL PRIVILEGES" => Some(Privilege::All),
            "CONNECT" => Some(Privilege::Connect),
            "USAGE" => Some(Privilege::Usage),
            "CREATEDB" | "CREATE DATABASE" => Some(Privilege::CreateDb),
            "CREATEUSER" | "CREATE USER" => Some(Privilege::CreateUser),
            "SUPERUSER" => Some(Privilege::Superuser),
            _ => None,
        }
    }
}

/// Target object for privileges
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PrivilegeTarget {
    /// All objects (global privilege)
    Global,
    /// Specific database
    Database(String),
    /// Specific table in a database
    Table { database: String, table: String },
    /// All tables in a database
    AllTablesInDatabase(String),
}

impl PrivilegeTarget {
    /// Check if this target covers another target
    pub fn covers(&self, other: &PrivilegeTarget) -> bool {
        match (self, other) {
            (PrivilegeTarget::Global, _) => true,
            (PrivilegeTarget::Database(d1), PrivilegeTarget::Database(d2)) => d1 == d2,
            (PrivilegeTarget::Database(d1), PrivilegeTarget::Table { database, .. }) => {
                d1 == database
            }
            (PrivilegeTarget::Database(d1), PrivilegeTarget::AllTablesInDatabase(d2)) => d1 == d2,
            (PrivilegeTarget::AllTablesInDatabase(d1), PrivilegeTarget::Table { database, .. }) => {
                d1 == database
            }
            (
                PrivilegeTarget::AllTablesInDatabase(d1),
                PrivilegeTarget::AllTablesInDatabase(d2),
            ) => d1 == d2,
            (
                PrivilegeTarget::Table {
                    database: d1,
                    table: t1,
                },
                PrivilegeTarget::Table {
                    database: d2,
                    table: t2,
                },
            ) => d1 == d2 && t1 == t2,
            _ => false,
        }
    }

    /// Parse a target from strings
    pub fn parse(database: Option<&str>, table: Option<&str>) -> PrivilegeTarget {
        match (database, table) {
            (None, None) => PrivilegeTarget::Global,
            (Some(db), None) => PrivilegeTarget::Database(db.to_string()),
            (Some(db), Some("*")) => PrivilegeTarget::AllTablesInDatabase(db.to_string()),
            (Some(db), Some(tbl)) => PrivilegeTarget::Table {
                database: db.to_string(),
                table: tbl.to_string(),
            },
            (None, Some(_)) => PrivilegeTarget::Global, // Invalid, treat as global
        }
    }
}

/// A single privilege grant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivilegeGrant {
    pub privilege: Privilege,
    pub target: PrivilegeTarget,
    pub granted_by: String,
    pub granted_at: u64, // Unix timestamp
    pub with_grant_option: bool,
}

/// User account status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus {
    Active,
    Locked,
    Expired,
}

/// Password policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordPolicy {
    /// Minimum password length
    pub min_length: usize,
    /// Require uppercase letters
    pub require_uppercase: bool,
    /// Require lowercase letters
    pub require_lowercase: bool,
    /// Require digits
    pub require_digit: bool,
    /// Require special characters
    pub require_special: bool,
    /// Password expiration in days (0 = never)
    pub expire_days: u32,
    /// Number of previous passwords to remember
    pub history_count: usize,
    /// Maximum failed login attempts before lockout
    pub max_failed_attempts: u32,
    /// Lockout duration in seconds
    pub lockout_duration_secs: u64,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: false,
            expire_days: 0,
            history_count: 3,
            max_failed_attempts: 5,
            lockout_duration_secs: 300,
        }
    }
}

/// User account information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Username (unique identifier)
    pub username: String,
    /// Argon2 hashed password
    pub password_hash: String,
    /// User status
    pub status: UserStatus,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub updated_at: u64,
    /// Last successful login
    pub last_login: Option<u64>,
    /// Password expiration timestamp
    pub password_expires_at: Option<u64>,
    /// Previous password hashes (for history check)
    #[serde(default)]
    pub password_history: Vec<String>,
    /// Failed login attempt count
    #[serde(default)]
    pub failed_attempts: u32,
    /// Lockout end timestamp
    pub locked_until: Option<u64>,
    /// Direct privileges granted to this user
    #[serde(default)]
    pub privileges: Vec<PrivilegeGrant>,
    /// Roles assigned to this user
    #[serde(default)]
    pub roles: Vec<String>,
    /// Whether this is a superuser
    #[serde(default)]
    pub is_superuser: bool,
    /// Default database for this user
    pub default_database: Option<String>,
    /// Connection limit (0 = unlimited)
    #[serde(default)]
    pub connection_limit: u32,
    /// User attributes (custom metadata)
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl User {
    /// Create a new user with hashed password
    pub fn new(username: &str, password_hash: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
            last_login: None,
            password_expires_at: None,
            password_history: Vec::new(),
            failed_attempts: 0,
            locked_until: None,
            privileges: Vec::new(),
            roles: Vec::new(),
            is_superuser: false,
            default_database: None,
            connection_limit: 0,
            attributes: HashMap::new(),
        }
    }

    /// Check if user is currently locked
    pub fn is_locked(&self) -> bool {
        if self.status == UserStatus::Locked {
            return true;
        }
        if let Some(locked_until) = self.locked_until {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return now < locked_until;
        }
        false
    }

    /// Check if password has expired
    pub fn is_password_expired(&self) -> bool {
        if let Some(expires_at) = self.password_expires_at {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return now >= expires_at;
        }
        false
    }
}

/// Role definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Privileges granted to this role
    pub privileges: Vec<PrivilegeGrant>,
    /// Roles this role inherits from
    #[serde(default)]
    pub inherits: Vec<String>,
    /// Creation timestamp
    pub created_at: u64,
    /// Description
    pub description: Option<String>,
}

impl Role {
    pub fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            name: name.to_string(),
            privileges: Vec::new(),
            inherits: Vec::new(),
            created_at: now,
            description: None,
        }
    }
}

/// Session information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session ID (token)
    pub session_id: String,
    /// Username
    pub username: String,
    /// Creation timestamp
    pub created_at: u64,
    /// Last activity timestamp
    pub last_activity: u64,
    /// Expiration timestamp
    pub expires_at: u64,
    /// Client IP address
    pub client_ip: Option<String>,
    /// Client application name
    pub application_name: Option<String>,
}

impl Session {
    /// Check if session has expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at
    }

    /// Update last activity timestamp
    pub fn touch(&mut self) {
        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub timestamp: u64,
    pub event_type: String,
    pub username: Option<String>,
    pub client_ip: Option<String>,
    pub target: Option<String>,
    pub action: String,
    pub success: bool,
    pub details: Option<String>,
}

/// Persistent storage for auth data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthStore {
    /// All users
    pub users: HashMap<String, User>,
    /// All roles
    pub roles: HashMap<String, Role>,
    /// Password policy
    pub password_policy: PasswordPolicy,
    /// Session timeout in seconds
    pub session_timeout_secs: u64,
    /// Whether authentication is required
    pub require_auth: bool,
    /// Store version for migrations
    pub version: u32,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self {
            users: HashMap::new(),
            roles: HashMap::new(),
            password_policy: PasswordPolicy::default(),
            session_timeout_secs: 3600, // 1 hour
            require_auth: true,
            version: 1,
        }
    }
}

/// Authentication and Authorization Manager
pub struct AuthManager {
    /// Persistent store
    store: RwLock<AuthStore>,
    /// Active sessions
    sessions: RwLock<HashMap<String, Session>>,
    /// Data directory for persistence
    data_dir: std::path::PathBuf,
    /// Audit log entries (in-memory buffer)
    audit_log: RwLock<Vec<AuditLogEntry>>,
    /// Path to persistent audit log file
    audit_log_path: std::path::PathBuf,
}

impl AuthManager {
    fn validate_password_with_policy(
        policy: &PasswordPolicy,
        password: &str,
    ) -> Result<(), AuthError> {
        if password.len() < policy.min_length {
            return Err(AuthError::WeakPassword(format!(
                "password must be at least {} characters",
                policy.min_length
            )));
        }

        if policy.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            return Err(AuthError::WeakPassword(
                "password must contain uppercase letters".into(),
            ));
        }

        if policy.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            return Err(AuthError::WeakPassword(
                "password must contain lowercase letters".into(),
            ));
        }

        if policy.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Err(AuthError::WeakPassword(
                "password must contain digits".into(),
            ));
        }

        if policy.require_special && !password.chars().any(|c| !c.is_alphanumeric()) {
            return Err(AuthError::WeakPassword(
                "password must contain special characters".into(),
            ));
        }

        Ok(())
    }

    /// Create a new AuthManager
    pub fn new(data_dir: &Path) -> Result<Self, AuthError> {
        let store_path = data_dir.join("auth_store.json");
        let store = if store_path.exists() {
            let content =
                std::fs::read_to_string(&store_path).map_err(|e| AuthError::Io(e.to_string()))?;
            serde_json::from_str(&content).map_err(|e| AuthError::Serialization(e.to_string()))?
        } else {
            // Create default store with root superuser
            let mut store = AuthStore::default();

            let bootstrap_password =
                env::var("BOYODB_BOOTSTRAP_PASSWORD").map_err(|_| AuthError::BootstrapPasswordMissing)?;
            Self::validate_password_with_policy(&store.password_policy, &bootstrap_password)?;

            // Create default root user with provided password
            let root_hash = Self::hash_password_static(&bootstrap_password)?;
            let mut root = User::new("root", &root_hash);
            root.is_superuser = true;
            store.users.insert("root".to_string(), root);

            // Create default roles
            let mut admin_role = Role::new("admin");
            admin_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::All,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: true,
            });
            admin_role.description = Some("Full administrative access".to_string());
            store.roles.insert("admin".to_string(), admin_role);

            let mut readonly_role = Role::new("readonly");
            readonly_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::Select,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: false,
            });
            readonly_role.description = Some("Read-only access to all databases".to_string());
            store.roles.insert("readonly".to_string(), readonly_role);

            let mut readwrite_role = Role::new("readwrite");
            readwrite_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::Select,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: false,
            });
            readwrite_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::Insert,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: false,
            });
            readwrite_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::Update,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: false,
            });
            readwrite_role.privileges.push(PrivilegeGrant {
                privilege: Privilege::Delete,
                target: PrivilegeTarget::Global,
                granted_by: "system".to_string(),
                granted_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                with_grant_option: false,
            });
            readwrite_role.description = Some("Read and write access to all databases".to_string());
            store.roles.insert("readwrite".to_string(), readwrite_role);

            store
        };

        let manager = Self {
            store: RwLock::new(store),
            sessions: RwLock::new(HashMap::new()),
            data_dir: data_dir.to_path_buf(),
            audit_log: RwLock::new(Vec::new()),
            audit_log_path: data_dir.join("audit.log"),
        };

        // Persist initial store
        manager.persist()?;

        Ok(manager)
    }

    /// Hash a password using Argon2id (industry-standard password hashing)
    fn hash_password_static(password: &str) -> Result<String, AuthError> {
        use argon2::{
            password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
            Argon2,
        };

        // Generate a cryptographically secure random salt
        let salt = SaltString::generate(&mut OsRng);

        // Hash password using Argon2id with default parameters
        // Argon2id is the recommended variant (hybrid of Argon2i and Argon2d)
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::Io(format!("password hashing failed: {}", e)))?
            .to_string();

        Ok(password_hash)
    }

    /// Verify a password against an Argon2 hash
    fn verify_password_static(password: &str, hash: &str) -> bool {
        use argon2::{
            password_hash::{PasswordHash, PasswordVerifier},
            Argon2,
        };

        // Parse the stored hash
        let parsed_hash = match PasswordHash::new(hash) {
            Ok(h) => h,
            Err(_) => return false,
        };

        // Verify the password against the hash
        // This performs constant-time comparison internally
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    /// Persist the auth store to disk
    fn persist(&self) -> Result<(), AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;
        let content = serde_json::to_string_pretty(&*store)
            .map_err(|e| AuthError::Serialization(e.to_string()))?;

        let store_path = self.data_dir.join("auth_store.json");
        let tmp_path = self.data_dir.join("auth_store.json.tmp");

        std::fs::write(&tmp_path, &content).map_err(|e| AuthError::Io(e.to_string()))?;
        std::fs::rename(&tmp_path, &store_path).map_err(|e| AuthError::Io(e.to_string()))?;

        Ok(())
    }

    /// Add an audit log entry
    fn audit(&self, entry: AuditLogEntry) {
        // Write to persistent file first (append mode, best-effort)
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.audit_log_path)
        {
            if let Ok(json) = serde_json::to_string(&entry) {
                let _ = writeln!(file, "{}", json);
            }
        }

        // Also keep in memory buffer
        if let Ok(mut log) = self.audit_log.write() {
            log.push(entry);
            // Keep only last 10000 entries in memory
            if log.len() > 10000 {
                log.drain(0..1000);
            }
        }
    }

    /// Get recent audit log entries (from memory)
    pub fn get_audit_log(&self, limit: usize) -> Vec<AuditLogEntry> {
        if let Ok(log) = self.audit_log.read() {
            let start = log.len().saturating_sub(limit);
            log[start..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Get audit log entries filtered by event type
    pub fn get_audit_log_by_type(&self, event_type: &str, limit: usize) -> Vec<AuditLogEntry> {
        if let Ok(log) = self.audit_log.read() {
            log.iter()
                .filter(|e| e.event_type == event_type)
                .rev()
                .take(limit)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Validate password against policy
    pub fn validate_password(&self, password: &str) -> Result<(), AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;
        Self::validate_password_with_policy(&store.password_policy, password)
    }

    // ========== User Management ==========

    /// Create a new user
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        created_by: &str,
    ) -> Result<(), AuthError> {
        // Validate password
        self.validate_password(password)?;

        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if store.users.contains_key(username) {
            return Err(AuthError::UserAlreadyExists(username.to_string()));
        }

        let password_hash = Self::hash_password_static(password)?;
        let mut user = User::new(username, &password_hash);

        // Set password expiration if policy requires
        if store.password_policy.expire_days > 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            user.password_expires_at =
                Some(now + (store.password_policy.expire_days as u64 * 86400));
        }

        store.users.insert(username.to_string(), user);
        drop(store);

        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "CREATE_USER".into(),
            username: Some(created_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: "create_user".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Drop a user
    pub fn drop_user(&self, username: &str, dropped_by: &str) -> Result<(), AuthError> {
        if username == "root" {
            return Err(AuthError::CannotModifySuperuser);
        }

        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if !store.users.contains_key(username) {
            return Err(AuthError::UserNotFound(username.to_string()));
        }

        store.users.remove(username);
        drop(store);

        self.persist()?;

        // Remove all sessions for this user
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.retain(|_, s| s.username != username);
        }

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "DROP_USER".into(),
            username: Some(dropped_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: "drop_user".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Alter user password
    pub fn alter_user_password(
        &self,
        username: &str,
        new_password: &str,
        altered_by: &str,
    ) -> Result<(), AuthError> {
        self.validate_password(new_password)?;

        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        // Extract policy values before mutable borrow
        let history_count = store.password_policy.history_count;
        let expire_days = store.password_policy.expire_days;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        // Check password history
        let new_hash = Self::hash_password_static(new_password)?;
        for old_hash in &user.password_history {
            if Self::verify_password_static(new_password, old_hash) {
                return Err(AuthError::WeakPassword("password was used recently".into()));
            }
        }

        // Update password history
        user.password_history.insert(0, user.password_hash.clone());
        if user.password_history.len() > history_count {
            user.password_history.truncate(history_count);
        }

        user.password_hash = new_hash;
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Reset password expiration
        if expire_days > 0 {
            user.password_expires_at = Some(user.updated_at + (expire_days as u64 * 86400));
        }

        // Reset failed attempts
        user.failed_attempts = 0;
        user.locked_until = None;

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "ALTER_USER".into(),
            username: Some(altered_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: "change_password".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Lock a user account
    pub fn lock_user(&self, username: &str, locked_by: &str) -> Result<(), AuthError> {
        if username == "root" {
            return Err(AuthError::CannotModifySuperuser);
        }

        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.status = UserStatus::Locked;
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "LOCK_USER".into(),
            username: Some(locked_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: "lock_user".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Unlock a user account
    pub fn unlock_user(&self, username: &str, unlocked_by: &str) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.status = UserStatus::Active;
        user.failed_attempts = 0;
        user.locked_until = None;
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "UNLOCK_USER".into(),
            username: Some(unlocked_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: "unlock_user".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Set user as superuser
    pub fn set_superuser(
        &self,
        username: &str,
        is_superuser: bool,
        set_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.is_superuser = is_superuser;
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "ALTER_USER".into(),
            username: Some(set_by.to_string()),
            client_ip: None,
            target: Some(username.to_string()),
            action: if is_superuser {
                "grant_superuser"
            } else {
                "revoke_superuser"
            }
            .into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// List all users
    pub fn list_users(&self) -> Result<Vec<UserInfo>, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        Ok(store
            .users
            .values()
            .map(|u| UserInfo {
                username: u.username.clone(),
                status: u.status,
                is_superuser: u.is_superuser,
                created_at: u.created_at,
                last_login: u.last_login,
                roles: u.roles.clone(),
                default_database: u.default_database.clone(),
            })
            .collect())
    }

    /// Get user info
    pub fn get_user(&self, username: &str) -> Result<UserInfo, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        Ok(UserInfo {
            username: user.username.clone(),
            status: user.status,
            is_superuser: user.is_superuser,
            created_at: user.created_at,
            last_login: user.last_login,
            roles: user.roles.clone(),
            default_database: user.default_database.clone(),
        })
    }

    /// Get user privileges
    pub fn get_user_privileges(&self, username: &str) -> Result<Vec<PrivilegeGrant>, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        Ok(user.privileges.clone())
    }

    // ========== Role Management ==========

    /// Create a new role
    pub fn create_role(
        &self,
        name: &str,
        description: Option<&str>,
        created_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if store.roles.contains_key(name) {
            return Err(AuthError::RoleAlreadyExists(name.to_string()));
        }

        let mut role = Role::new(name);
        role.description = description.map(|s| s.to_string());
        store.roles.insert(name.to_string(), role);

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "CREATE_ROLE".into(),
            username: Some(created_by.to_string()),
            client_ip: None,
            target: Some(name.to_string()),
            action: "create_role".into(),
            success: true,
            details: description.map(|s| s.to_string()),
        });

        Ok(())
    }

    /// Drop a role
    pub fn drop_role(&self, name: &str, dropped_by: &str) -> Result<(), AuthError> {
        // Don't allow dropping built-in roles
        if matches!(name, "admin" | "readonly" | "readwrite") {
            return Err(AuthError::AccessDenied("cannot drop built-in role".into()));
        }

        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if !store.roles.contains_key(name) {
            return Err(AuthError::RoleNotFound(name.to_string()));
        }

        // Remove role from all users
        for user in store.users.values_mut() {
            user.roles.retain(|r| r != name);
        }

        store.roles.remove(name);
        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "DROP_ROLE".into(),
            username: Some(dropped_by.to_string()),
            client_ip: None,
            target: Some(name.to_string()),
            action: "drop_role".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Grant role to user
    pub fn grant_role(
        &self,
        username: &str,
        role: &str,
        granted_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if !store.roles.contains_key(role) {
            return Err(AuthError::RoleNotFound(role.to_string()));
        }

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        if !user.roles.contains(&role.to_string()) {
            user.roles.push(role.to_string());
            user.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "GRANT_ROLE".into(),
            username: Some(granted_by.to_string()),
            client_ip: None,
            target: Some(format!("{} TO {}", role, username)),
            action: "grant_role".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Revoke role from user
    pub fn revoke_role(
        &self,
        username: &str,
        role: &str,
        revoked_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.roles.retain(|r| r != role);
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "REVOKE_ROLE".into(),
            username: Some(revoked_by.to_string()),
            client_ip: None,
            target: Some(format!("{} FROM {}", role, username)),
            action: "revoke_role".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// List all roles
    pub fn list_roles(&self) -> Result<Vec<RoleInfo>, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        Ok(store
            .roles
            .values()
            .map(|r| RoleInfo {
                name: r.name.clone(),
                description: r.description.clone(),
                created_at: r.created_at,
                privileges: r
                    .privileges
                    .iter()
                    .map(|p| format!("{:?} ON {:?}", p.privilege, p.target))
                    .collect(),
            })
            .collect())
    }

    // ========== Privilege Management ==========

    /// Grant privilege to user
    pub fn grant_privilege(
        &self,
        username: &str,
        privilege: Privilege,
        target: PrivilegeTarget,
        with_grant_option: bool,
        granted_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        let grant = PrivilegeGrant {
            privilege,
            target: target.clone(),
            granted_by: granted_by.to_string(),
            granted_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            with_grant_option,
        };

        // Check if privilege already exists
        let exists = user
            .privileges
            .iter()
            .any(|p| p.privilege == privilege && p.target == target);

        if !exists {
            user.privileges.push(grant);
            user.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "GRANT".into(),
            username: Some(granted_by.to_string()),
            client_ip: None,
            target: Some(format!("{:?} ON {:?} TO {}", privilege, target, username)),
            action: "grant_privilege".into(),
            success: true,
            details: if with_grant_option {
                Some("WITH GRANT OPTION".into())
            } else {
                None
            },
        });

        Ok(())
    }

    /// Revoke privilege from user
    pub fn revoke_privilege(
        &self,
        username: &str,
        privilege: Privilege,
        target: PrivilegeTarget,
        revoked_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.privileges
            .retain(|p| !(p.privilege == privilege && p.target == target));
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "REVOKE".into(),
            username: Some(revoked_by.to_string()),
            client_ip: None,
            target: Some(format!("{:?} ON {:?} FROM {}", privilege, target, username)),
            action: "revoke_privilege".into(),
            success: true,
            details: None,
        });

        Ok(())
    }

    /// Grant privilege to role
    pub fn grant_privilege_to_role(
        &self,
        role_name: &str,
        privilege: Privilege,
        target: PrivilegeTarget,
        with_grant_option: bool,
        granted_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let role = store
            .roles
            .get_mut(role_name)
            .ok_or_else(|| AuthError::RoleNotFound(role_name.to_string()))?;

        let grant = PrivilegeGrant {
            privilege,
            target: target.clone(),
            granted_by: granted_by.to_string(),
            granted_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            with_grant_option,
        };

        // Check if privilege already exists
        let exists = role
            .privileges
            .iter()
            .any(|p| p.privilege == privilege && p.target == target);

        if !exists {
            role.privileges.push(grant);
        }

        drop(store);
        self.persist()?;

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "GRANT".into(),
            username: Some(granted_by.to_string()),
            client_ip: None,
            target: Some(format!(
                "{:?} ON {:?} TO ROLE {}",
                privilege, target, role_name
            )),
            action: "grant_privilege_to_role".into(),
            success: true,
            details: if with_grant_option {
                Some("WITH GRANT OPTION".into())
            } else {
                None
            },
        });

        Ok(())
    }

    /// Revoke privilege from role
    pub fn revoke_privilege_from_role(
        &self,
        role_name: &str,
        privilege: Privilege,
        target: PrivilegeTarget,
        revoked_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let role = store
            .roles
            .get_mut(role_name)
            .ok_or_else(|| AuthError::RoleNotFound(role_name.to_string()))?;

        // Remove matching privilege(s)
        let original_len = role.privileges.len();
        role.privileges
            .retain(|p| !(p.privilege == privilege && p.target == target));

        let removed = original_len - role.privileges.len();

        drop(store);

        if removed > 0 {
            self.persist()?;
        }

        self.audit(AuditLogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type: "REVOKE".into(),
            username: Some(revoked_by.to_string()),
            client_ip: None,
            target: Some(format!(
                "{:?} ON {:?} FROM ROLE {}",
                privilege, target, role_name
            )),
            action: "revoke_privilege_from_role".into(),
            success: true,
            details: Some(format!("{} privilege(s) removed", removed)),
        });

        Ok(())
    }

    // ========== Authentication ==========

    /// Authenticate a user and create a session
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
        client_ip: Option<&str>,
        application_name: Option<&str>,
    ) -> Result<String, AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        // Extract policy values before mutable borrow
        let max_failed_attempts = store.password_policy.max_failed_attempts;
        let lockout_duration_secs = store.password_policy.lockout_duration_secs;

        let user = store
            .users
            .get_mut(username)
            .ok_or(AuthError::InvalidCredentials)?;

        // Check if user is locked
        if user.is_locked() {
            self.audit(AuditLogEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                event_type: "LOGIN_FAILED".into(),
                username: Some(username.to_string()),
                client_ip: client_ip.map(|s| s.to_string()),
                target: None,
                action: "authenticate".into(),
                success: false,
                details: Some("user is locked".into()),
            });
            return Err(AuthError::UserLocked);
        }

        // Verify password
        if !Self::verify_password_static(password, &user.password_hash) {
            user.failed_attempts += 1;

            // Check if should lock
            if user.failed_attempts >= max_failed_attempts {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                user.locked_until = Some(now + lockout_duration_secs);
            }

            drop(store);
            self.persist().ok(); // Best effort persist

            self.audit(AuditLogEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                event_type: "LOGIN_FAILED".into(),
                username: Some(username.to_string()),
                client_ip: client_ip.map(|s| s.to_string()),
                target: None,
                action: "authenticate".into(),
                success: false,
                details: Some("invalid password".into()),
            });

            return Err(AuthError::InvalidCredentials);
        }

        // Check password expiration
        if user.is_password_expired() {
            self.audit(AuditLogEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                event_type: "LOGIN_FAILED".into(),
                username: Some(username.to_string()),
                client_ip: client_ip.map(|s| s.to_string()),
                target: None,
                action: "authenticate".into(),
                success: false,
                details: Some("password expired".into()),
            });
            return Err(AuthError::InvalidCredentials);
        }

        // Reset failed attempts and update last login
        user.failed_attempts = 0;
        user.locked_until = None;
        user.last_login = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        let session_timeout = store.session_timeout_secs;
        drop(store);
        self.persist().ok();

        // Generate session token
        let session_id = Self::generate_session_id();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let session = Session {
            session_id: session_id.clone(),
            username: username.to_string(),
            created_at: now,
            last_activity: now,
            expires_at: now + session_timeout,
            client_ip: client_ip.map(|s| s.to_string()),
            application_name: application_name.map(|s| s.to_string()),
        };

        if let Ok(mut sessions) = self.sessions.write() {
            sessions.insert(session_id.clone(), session);
        }

        self.audit(AuditLogEntry {
            timestamp: now,
            event_type: "LOGIN".into(),
            username: Some(username.to_string()),
            client_ip: client_ip.map(|s| s.to_string()),
            target: None,
            action: "authenticate".into(),
            success: true,
            details: application_name.map(|s| format!("app={}", s)),
        });

        Ok(session_id)
    }

    /// Generate a random session ID
    fn generate_session_id() -> String {
        // Use cryptographically strong randomness for session tokens
        Uuid::new_v4().simple().to_string()
    }

    /// Validate a session and return the username
    pub fn validate_session(&self, session_id: &str) -> Result<String, AuthError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let session = sessions
            .get_mut(session_id)
            .ok_or(AuthError::SessionNotFound)?;

        if session.is_expired() {
            sessions.remove(session_id);
            return Err(AuthError::SessionExpired);
        }

        session.touch();
        Ok(session.username.clone())
    }

    /// Invalidate a session (logout)
    pub fn invalidate_session(&self, session_id: &str) -> Result<(), AuthError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        if let Some(session) = sessions.remove(session_id) {
            self.audit(AuditLogEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                event_type: "LOGOUT".into(),
                username: Some(session.username),
                client_ip: session.client_ip,
                target: None,
                action: "logout".into(),
                success: true,
                details: None,
            });
        }

        Ok(())
    }

    /// Clean up expired sessions
    pub fn cleanup_sessions(&self) {
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.retain(|_, s| !s.is_expired());
        }
    }

    // ========== Authorization ==========

    /// Check if a user has a specific privilege on a target
    pub fn check_privilege(
        &self,
        username: &str,
        required_privilege: Privilege,
        target: &PrivilegeTarget,
    ) -> Result<bool, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        // Superusers have all privileges
        if user.is_superuser {
            return Ok(true);
        }

        // Check direct user privileges
        for grant in &user.privileges {
            if grant.privilege.implies(&required_privilege) && grant.target.covers(target) {
                return Ok(true);
            }
            // Check expanded privileges (for ALL)
            for expanded in grant.privilege.expand() {
                if expanded == required_privilege && grant.target.covers(target) {
                    return Ok(true);
                }
            }
        }

        // Check role privileges
        for role_name in &user.roles {
            if let Some(role) = store.roles.get(role_name) {
                for grant in &role.privileges {
                    if grant.privilege.implies(&required_privilege) && grant.target.covers(target) {
                        return Ok(true);
                    }
                    for expanded in grant.privilege.expand() {
                        if expanded == required_privilege && grant.target.covers(target) {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    /// Require a privilege (check and return error if not granted)
    pub fn require_privilege(
        &self,
        username: &str,
        required_privilege: Privilege,
        target: &PrivilegeTarget,
    ) -> Result<(), AuthError> {
        if self.check_privilege(username, required_privilege, target)? {
            Ok(())
        } else {
            Err(AuthError::PermissionDenied(format!(
                "{:?} on {:?}",
                required_privilege, target
            )))
        }
    }

    /// Check if authentication is required
    pub fn is_auth_required(&self) -> bool {
        self.store.read().map(|s| s.require_auth).unwrap_or(false)
    }

    /// Enable or disable authentication requirement
    pub fn set_auth_required(&self, required: bool) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;
        store.require_auth = required;
        drop(store);
        self.persist()
    }

    /// Get active sessions
    pub fn get_active_sessions(&self) -> Vec<SessionInfo> {
        if let Ok(sessions) = self.sessions.read() {
            sessions
                .values()
                .filter(|s| !s.is_expired())
                .map(|s| SessionInfo {
                    session_id: s.session_id[..8].to_string() + "...", // Partial ID for security
                    username: s.username.clone(),
                    created_at: s.created_at,
                    last_activity: s.last_activity,
                    client_ip: s.client_ip.clone(),
                    application_name: s.application_name.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Set default database for user
    pub fn set_default_database(
        &self,
        username: &str,
        database: Option<&str>,
        _set_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.default_database = database.map(|s| s.to_string());
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()
    }

    /// Set connection limit for user
    pub fn set_connection_limit(
        &self,
        username: &str,
        limit: u32,
        _set_by: &str,
    ) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;

        let user = store
            .users
            .get_mut(username)
            .ok_or_else(|| AuthError::UserNotFound(username.to_string()))?;

        user.connection_limit = limit;
        user.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        drop(store);
        self.persist()
    }

    /// Update password policy
    pub fn set_password_policy(&self, policy: PasswordPolicy) -> Result<(), AuthError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;
        store.password_policy = policy;
        drop(store);
        self.persist()
    }

    /// Get password policy
    pub fn get_password_policy(&self) -> Result<PasswordPolicy, AuthError> {
        let store = self
            .store
            .read()
            .map_err(|_| AuthError::Io("lock poisoned".into()))?;
        Ok(store.password_policy.clone())
    }
}

/// Simplified user info for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub username: String,
    pub status: UserStatus,
    pub is_superuser: bool,
    pub created_at: u64,
    pub last_login: Option<u64>,
    pub roles: Vec<String>,
    pub default_database: Option<String>,
}

/// Simplified role info for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleInfo {
    pub name: String,
    pub description: Option<String>,
    pub created_at: u64,
    pub privileges: Vec<String>,
}

/// Simplified session info for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub username: String,
    pub created_at: u64,
    pub last_activity: u64,
    pub client_ip: Option<String>,
    pub application_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use tempfile::tempdir;

    fn ensure_bootstrap_password() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            std::env::set_var("BOYODB_BOOTSTRAP_PASSWORD", "TestBootstr4p!");
        });
    }

    #[test]
    fn test_create_and_authenticate_user() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Create a new user
        auth.create_user("testuser", "SecurePass123", "root")
            .unwrap();

        // Authenticate
        let session = auth
            .authenticate("testuser", "SecurePass123", None, None)
            .unwrap();
        assert!(!session.is_empty());

        // Validate session
        let username = auth.validate_session(&session).unwrap();
        assert_eq!(username, "testuser");

        // Wrong password should fail
        let result = auth.authenticate("testuser", "wrongpassword", None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_privilege_check() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Create a user
        auth.create_user("appuser", "SecurePass123", "root")
            .unwrap();

        // Grant SELECT on database
        auth.grant_privilege(
            "appuser",
            Privilege::Select,
            PrivilegeTarget::Database("mydb".to_string()),
            false,
            "root",
        )
        .unwrap();

        // Check privilege
        let has_select = auth
            .check_privilege(
                "appuser",
                Privilege::Select,
                &PrivilegeTarget::Table {
                    database: "mydb".to_string(),
                    table: "mytable".to_string(),
                },
            )
            .unwrap();
        assert!(has_select);

        // Check privilege on different database
        let has_select_other = auth
            .check_privilege(
                "appuser",
                Privilege::Select,
                &PrivilegeTarget::Database("otherdb".to_string()),
            )
            .unwrap();
        assert!(!has_select_other);
    }

    #[test]
    fn test_role_based_access() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Create a user
        auth.create_user("roleuser", "SecurePass123", "root")
            .unwrap();

        // Grant readonly role
        auth.grant_role("roleuser", "readonly", "root").unwrap();

        // User should have SELECT privilege via role
        let has_select = auth
            .check_privilege(
                "roleuser",
                Privilege::Select,
                &PrivilegeTarget::Database("anydb".to_string()),
            )
            .unwrap();
        assert!(has_select);

        // User should not have INSERT privilege
        let has_insert = auth
            .check_privilege(
                "roleuser",
                Privilege::Insert,
                &PrivilegeTarget::Database("anydb".to_string()),
            )
            .unwrap();
        assert!(!has_insert);
    }

    #[test]
    fn test_superuser_has_all_privileges() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Root user should have all privileges
        let has_all = auth
            .check_privilege("root", Privilege::CreateDb, &PrivilegeTarget::Global)
            .unwrap();
        assert!(has_all);
    }

    #[test]
    fn test_password_validation() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Weak password should fail
        let result = auth.create_user("weakuser", "weak", "root");
        assert!(result.is_err());

        // Strong password should succeed
        let result = auth.create_user("stronguser", "StrongPass123", "root");
        assert!(result.is_ok());
    }

    #[test]
    fn test_user_lockout() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Create user
        auth.create_user("locktest", "SecurePass123", "root")
            .unwrap();

        // Fail multiple times
        for _ in 0..5 {
            let _ = auth.authenticate("locktest", "wrongpass", None, None);
        }

        // User should be locked
        let result = auth.authenticate("locktest", "SecurePass123", None, None);
        assert!(matches!(result, Err(AuthError::UserLocked)));

        // Unlock user
        auth.unlock_user("locktest", "root").unwrap();

        // Should be able to login now
        let result = auth.authenticate("locktest", "SecurePass123", None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_session_expiration() {
        ensure_bootstrap_password();
        let tmp = tempdir().unwrap();
        let auth = AuthManager::new(tmp.path()).unwrap();

        // Create a session that's already expired
        let session_id = "testsession123";
        if let Ok(mut sessions) = auth.sessions.write() {
            sessions.insert(
                session_id.to_string(),
                Session {
                    session_id: session_id.to_string(),
                    username: "root".to_string(),
                    created_at: 0,
                    last_activity: 0,
                    expires_at: 0, // Already expired
                    client_ip: None,
                    application_name: None,
                },
            );
        }

        // Validation should fail
        let result = auth.validate_session(session_id);
        assert!(matches!(result, Err(AuthError::SessionExpired)));
    }

    #[test]
    fn session_ids_use_random_uuid_v4() {
        // Ensure tokens are non-empty, hex-only, and unlikely to collide
        let id1 = AuthManager::generate_session_id();
        let id2 = AuthManager::generate_session_id();

        assert_eq!(id1.len(), 32);
        assert_eq!(id2.len(), 32);
        assert_ne!(id1, id2);

        fn is_hex(s: &str) -> bool {
            s.chars()
                .all(|c| c.is_ascii_hexdigit())
        }

        assert!(is_hex(&id1));
        assert!(is_hex(&id2));
    }
}
