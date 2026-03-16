//! Schema Migrations and Versioning
//!
//! Provides database schema migration management with version tracking,
//! rollback support, and migration history.
//!
//! ## Features
//!
//! - Version-controlled schema changes
//! - Up/down migrations with rollback
//! - Migration history tracking
//! - Dependency resolution between migrations
//! - Dry-run mode for validation
//! - Lock-based concurrent migration protection
//!
//! ## Usage
//!
//! ```sql
//! -- Create a migration
//! CREATE MIGRATION 'add_user_email' AS $$
//!   ALTER TABLE users ADD COLUMN email VARCHAR(255);
//! $$ DOWN $$
//!   ALTER TABLE users DROP COLUMN email;
//! $$;
//!
//! -- Apply migrations
//! MIGRATE UP;
//! MIGRATE UP TO 'add_user_email';
//!
//! -- Rollback
//! MIGRATE DOWN;
//! MIGRATE DOWN TO 'initial';
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

// ============================================================================
// Migration Types
// ============================================================================

/// Unique migration identifier
pub type MigrationId = String;

/// Migration version (sequential or timestamp-based)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MigrationVersion {
    /// Version number or timestamp
    pub version: u64,
    /// Human-readable name
    pub name: String,
}

impl MigrationVersion {
    pub fn new(version: u64, name: &str) -> Self {
        Self {
            version,
            name: name.to_string(),
        }
    }

    pub fn from_timestamp(name: &str) -> Self {
        let version = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self::new(version, name)
    }
}

/// Migration status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationStatus {
    /// Migration is pending
    Pending,
    /// Migration is currently running
    Running,
    /// Migration completed successfully
    Applied,
    /// Migration failed
    Failed,
    /// Migration was rolled back
    RolledBack,
}

/// A single migration definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Migration version
    pub version: MigrationVersion,
    /// Description
    pub description: Option<String>,
    /// SQL statements for upgrading
    pub up_sql: Vec<String>,
    /// SQL statements for downgrading (rollback)
    pub down_sql: Vec<String>,
    /// Dependencies (migrations that must be applied first)
    pub dependencies: Vec<MigrationId>,
    /// Whether this migration is reversible
    pub reversible: bool,
    /// Checksum of migration content (for drift detection)
    pub checksum: String,
    /// Created timestamp
    pub created_at: u64,
}

impl Migration {
    /// Create a new migration
    pub fn new(version: MigrationVersion, up_sql: Vec<String>) -> Self {
        let checksum = Self::compute_checksum(&up_sql);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            version,
            description: None,
            up_sql,
            down_sql: Vec::new(),
            dependencies: Vec::new(),
            reversible: false,
            checksum,
            created_at,
        }
    }

    /// Add down migration (makes it reversible)
    pub fn with_down(mut self, down_sql: Vec<String>) -> Self {
        self.down_sql = down_sql;
        self.reversible = !self.down_sql.is_empty();
        self
    }

    /// Add description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Add dependencies
    pub fn with_dependencies(mut self, deps: Vec<MigrationId>) -> Self {
        self.dependencies = deps;
        self
    }

    fn compute_checksum(statements: &[String]) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for stmt in statements {
            stmt.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    /// Get migration ID
    pub fn id(&self) -> MigrationId {
        format!("{}_{}", self.version.version, self.version.name)
    }
}

/// Record of an applied migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    /// Migration ID
    pub migration_id: MigrationId,
    /// Migration version
    pub version: u64,
    /// Migration name
    pub name: String,
    /// Current status
    pub status: MigrationStatus,
    /// Checksum at time of application
    pub checksum: String,
    /// When migration was applied
    pub applied_at: Option<u64>,
    /// When migration was rolled back (if applicable)
    pub rolled_back_at: Option<u64>,
    /// Duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// User who applied the migration
    pub applied_by: Option<String>,
}

// ============================================================================
// Migration Manager
// ============================================================================

/// Configuration for migration manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Directory containing migration files
    pub migrations_dir: PathBuf,
    /// Table name for tracking migrations
    pub history_table: String,
    /// Database for migration history
    pub history_database: String,
    /// Lock timeout in seconds
    pub lock_timeout_secs: u64,
    /// Enable dry-run by default
    pub dry_run: bool,
    /// Validate checksums on apply
    pub validate_checksums: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            migrations_dir: PathBuf::from("migrations"),
            history_table: "_schema_migrations".to_string(),
            history_database: "system".to_string(),
            lock_timeout_secs: 300,
            dry_run: false,
            validate_checksums: true,
        }
    }
}

/// Migration manager
pub struct MigrationManager {
    config: MigrationConfig,
    /// Registered migrations
    migrations: RwLock<HashMap<MigrationId, Migration>>,
    /// Applied migrations history
    history: RwLock<Vec<MigrationRecord>>,
    /// Migration lock
    locked: RwLock<bool>,
    /// Lock holder
    lock_holder: RwLock<Option<String>>,
}

impl MigrationManager {
    /// Create a new migration manager
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            migrations: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
            locked: RwLock::new(false),
            lock_holder: RwLock::new(None),
        }
    }

    /// Register a migration
    pub fn register(&self, migration: Migration) {
        let mut migrations = self.migrations.write();
        migrations.insert(migration.id(), migration);
    }

    /// Load migrations from directory
    pub fn load_from_directory(&self) -> Result<usize, MigrationError> {
        let dir = &self.config.migrations_dir;
        if !dir.exists() {
            return Ok(0);
        }

        let mut count = 0;
        let entries = std::fs::read_dir(dir)
            .map_err(|e| MigrationError::Io(format!("cannot read migrations dir: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| MigrationError::Io(e.to_string()))?;
            let path = entry.path();

            if path.extension().map(|e| e == "sql").unwrap_or(false) {
                let migration = self.parse_migration_file(&path)?;
                self.register(migration);
                count += 1;
            }
        }

        Ok(count)
    }

    /// Parse a migration file
    fn parse_migration_file(&self, path: &PathBuf) -> Result<Migration, MigrationError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| MigrationError::Io(format!("cannot read {}: {}", path.display(), e)))?;

        let filename = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| MigrationError::Parse("invalid filename".into()))?;

        // Parse filename: VERSION_name.sql
        let parts: Vec<&str> = filename.splitn(2, '_').collect();
        if parts.len() != 2 {
            return Err(MigrationError::Parse(format!(
                "invalid migration filename format: {}",
                filename
            )));
        }

        let version: u64 = parts[0]
            .parse()
            .map_err(|_| MigrationError::Parse(format!("invalid version: {}", parts[0])))?;

        let name = parts[1].to_string();

        // Parse content for UP and DOWN sections
        let (up_sql, down_sql) = self.parse_migration_content(&content)?;

        let migration =
            Migration::new(MigrationVersion::new(version, &name), up_sql).with_down(down_sql);

        Ok(migration)
    }

    /// Parse migration content for UP/DOWN sections
    fn parse_migration_content(
        &self,
        content: &str,
    ) -> Result<(Vec<String>, Vec<String>), MigrationError> {
        let mut up_sql = Vec::new();
        let mut down_sql = Vec::new();
        let mut current_section = "up";
        let mut current_statement = String::new();

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.eq_ignore_ascii_case("-- UP") || trimmed.eq_ignore_ascii_case("-- @UP") {
                current_section = "up";
                continue;
            }

            if trimmed.eq_ignore_ascii_case("-- DOWN") || trimmed.eq_ignore_ascii_case("-- @DOWN") {
                // Save any pending statement
                if !current_statement.trim().is_empty() {
                    up_sql.push(current_statement.trim().to_string());
                    current_statement.clear();
                }
                current_section = "down";
                continue;
            }

            // Skip comments
            if trimmed.starts_with("--") {
                continue;
            }

            current_statement.push_str(line);
            current_statement.push('\n');

            // Check for statement end
            if trimmed.ends_with(';') {
                let stmt = current_statement.trim().to_string();
                if !stmt.is_empty() {
                    match current_section {
                        "up" => up_sql.push(stmt),
                        "down" => down_sql.push(stmt),
                        _ => {}
                    }
                }
                current_statement.clear();
            }
        }

        // Don't forget trailing statement without semicolon
        let stmt = current_statement.trim().to_string();
        if !stmt.is_empty() {
            match current_section {
                "up" => up_sql.push(stmt),
                "down" => down_sql.push(stmt),
                _ => {}
            }
        }

        Ok((up_sql, down_sql))
    }

    /// Get pending migrations (not yet applied)
    pub fn pending(&self) -> Vec<Migration> {
        let migrations = self.migrations.read();
        let history = self.history.read();

        let applied: std::collections::HashSet<_> = history
            .iter()
            .filter(|r| r.status == MigrationStatus::Applied)
            .map(|r| r.migration_id.clone())
            .collect();

        let mut pending: Vec<_> = migrations
            .values()
            .filter(|m| !applied.contains(&m.id()))
            .cloned()
            .collect();

        pending.sort_by_key(|m| m.version.version);
        pending
    }

    /// Get applied migrations
    pub fn applied(&self) -> Vec<MigrationRecord> {
        let history = self.history.read();
        history
            .iter()
            .filter(|r| r.status == MigrationStatus::Applied)
            .cloned()
            .collect()
    }

    /// Acquire migration lock
    pub fn acquire_lock(&self, holder: &str) -> Result<(), MigrationError> {
        let mut locked = self.locked.write();
        if *locked {
            let current_holder = self.lock_holder.read();
            return Err(MigrationError::Locked(
                current_holder
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
            ));
        }
        *locked = true;
        *self.lock_holder.write() = Some(holder.to_string());
        Ok(())
    }

    /// Release migration lock
    pub fn release_lock(&self) {
        *self.locked.write() = false;
        *self.lock_holder.write() = None;
    }

    /// Apply a single migration
    pub fn apply_one(
        &self,
        migration: &Migration,
        executor: &dyn MigrationExecutor,
        dry_run: bool,
    ) -> Result<MigrationRecord, MigrationError> {
        let start = std::time::Instant::now();
        let mut record = MigrationRecord {
            migration_id: migration.id(),
            version: migration.version.version,
            name: migration.version.name.clone(),
            status: MigrationStatus::Running,
            checksum: migration.checksum.clone(),
            applied_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
            rolled_back_at: None,
            duration_ms: None,
            error: None,
            applied_by: None,
        };

        if dry_run {
            tracing::info!(
                migration_id = %migration.id(),
                statements = migration.up_sql.len(),
                "[DRY RUN] Would apply migration"
            );
            record.status = MigrationStatus::Pending;
            return Ok(record);
        }

        // Execute each statement
        for (i, stmt) in migration.up_sql.iter().enumerate() {
            tracing::debug!(
                migration_id = %migration.id(),
                statement = i + 1,
                total = migration.up_sql.len(),
                "Executing migration statement"
            );

            if let Err(e) = executor.execute(stmt) {
                record.status = MigrationStatus::Failed;
                record.error = Some(e.to_string());
                record.duration_ms = Some(start.elapsed().as_millis() as u64);
                return Err(MigrationError::Execution(e.to_string()));
            }
        }

        record.status = MigrationStatus::Applied;
        record.duration_ms = Some(start.elapsed().as_millis() as u64);

        // Add to history
        self.history.write().push(record.clone());

        tracing::info!(
            migration_id = %migration.id(),
            duration_ms = record.duration_ms,
            "Migration applied successfully"
        );

        Ok(record)
    }

    /// Rollback a single migration
    pub fn rollback_one(
        &self,
        migration: &Migration,
        executor: &dyn MigrationExecutor,
        dry_run: bool,
    ) -> Result<MigrationRecord, MigrationError> {
        if !migration.reversible {
            return Err(MigrationError::NotReversible(migration.id()));
        }

        let start = std::time::Instant::now();
        let mut record = MigrationRecord {
            migration_id: migration.id(),
            version: migration.version.version,
            name: migration.version.name.clone(),
            status: MigrationStatus::Running,
            checksum: migration.checksum.clone(),
            applied_at: None,
            rolled_back_at: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            ),
            duration_ms: None,
            error: None,
            applied_by: None,
        };

        if dry_run {
            tracing::info!(
                migration_id = %migration.id(),
                statements = migration.down_sql.len(),
                "[DRY RUN] Would rollback migration"
            );
            record.status = MigrationStatus::Applied;
            return Ok(record);
        }

        // Execute down statements
        for (i, stmt) in migration.down_sql.iter().enumerate() {
            tracing::debug!(
                migration_id = %migration.id(),
                statement = i + 1,
                total = migration.down_sql.len(),
                "Executing rollback statement"
            );

            if let Err(e) = executor.execute(stmt) {
                record.status = MigrationStatus::Failed;
                record.error = Some(e.to_string());
                record.duration_ms = Some(start.elapsed().as_millis() as u64);
                return Err(MigrationError::Execution(e.to_string()));
            }
        }

        record.status = MigrationStatus::RolledBack;
        record.duration_ms = Some(start.elapsed().as_millis() as u64);

        // Update history
        {
            let mut history = self.history.write();
            if let Some(existing) = history
                .iter_mut()
                .find(|r| r.migration_id == migration.id())
            {
                existing.status = MigrationStatus::RolledBack;
                existing.rolled_back_at = record.rolled_back_at;
            }
        }

        tracing::info!(
            migration_id = %migration.id(),
            duration_ms = record.duration_ms,
            "Migration rolled back successfully"
        );

        Ok(record)
    }

    /// Apply all pending migrations
    pub fn migrate_up(
        &self,
        executor: &dyn MigrationExecutor,
        dry_run: bool,
    ) -> Result<Vec<MigrationRecord>, MigrationError> {
        let pending = self.pending();
        if pending.is_empty() {
            tracing::info!("No pending migrations");
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        for migration in &pending {
            let record = self.apply_one(migration, executor, dry_run)?;
            results.push(record);
        }

        Ok(results)
    }

    /// Rollback the last N migrations
    pub fn migrate_down(
        &self,
        count: usize,
        executor: &dyn MigrationExecutor,
        dry_run: bool,
    ) -> Result<Vec<MigrationRecord>, MigrationError> {
        let applied = self.applied();
        if applied.is_empty() {
            tracing::info!("No migrations to rollback");
            return Ok(vec![]);
        }

        let to_rollback: Vec<_> = applied.iter().rev().take(count).collect();
        let migrations = self.migrations.read();

        let mut results = Vec::new();
        for record in to_rollback {
            if let Some(migration) = migrations.get(&record.migration_id) {
                let result = self.rollback_one(migration, executor, dry_run)?;
                results.push(result);
            }
        }

        Ok(results)
    }

    /// Get migration status
    pub fn status(&self) -> MigrationStatusReport {
        let pending = self.pending();
        let applied = self.applied();

        MigrationStatusReport {
            total_migrations: self.migrations.read().len(),
            applied_count: applied.len(),
            pending_count: pending.len(),
            last_applied: applied.last().cloned(),
            pending_migrations: pending.iter().map(|m| m.id()).collect(),
        }
    }

    /// Validate migration checksums
    pub fn validate_checksums(&self) -> Vec<ChecksumMismatch> {
        let migrations = self.migrations.read();
        let history = self.history.read();
        let mut mismatches = Vec::new();

        for record in history
            .iter()
            .filter(|r| r.status == MigrationStatus::Applied)
        {
            if let Some(migration) = migrations.get(&record.migration_id) {
                if migration.checksum != record.checksum {
                    mismatches.push(ChecksumMismatch {
                        migration_id: record.migration_id.clone(),
                        expected: record.checksum.clone(),
                        actual: migration.checksum.clone(),
                    });
                }
            }
        }

        mismatches
    }
}

/// Migration status report
#[derive(Debug, Clone, Serialize)]
pub struct MigrationStatusReport {
    pub total_migrations: usize,
    pub applied_count: usize,
    pub pending_count: usize,
    pub last_applied: Option<MigrationRecord>,
    pub pending_migrations: Vec<MigrationId>,
}

/// Checksum mismatch detected
#[derive(Debug, Clone, Serialize)]
pub struct ChecksumMismatch {
    pub migration_id: MigrationId,
    pub expected: String,
    pub actual: String,
}

// ============================================================================
// Migration Executor Trait
// ============================================================================

/// Trait for executing migration SQL
pub trait MigrationExecutor: Send + Sync {
    /// Execute a SQL statement
    fn execute(&self, sql: &str) -> Result<(), Box<dyn std::error::Error>>;

    /// Begin a transaction
    fn begin_transaction(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Commit a transaction
    fn commit(&self) -> Result<(), Box<dyn std::error::Error>>;

    /// Rollback a transaction
    fn rollback(&self) -> Result<(), Box<dyn std::error::Error>>;
}

// ============================================================================
// Error Types
// ============================================================================

/// Migration error types
#[derive(Debug)]
pub enum MigrationError {
    /// I/O error
    Io(String),
    /// Parse error
    Parse(String),
    /// Execution error
    Execution(String),
    /// Migration is locked
    Locked(String),
    /// Migration not found
    NotFound(MigrationId),
    /// Migration not reversible
    NotReversible(MigrationId),
    /// Dependency not satisfied
    DependencyNotSatisfied(MigrationId, MigrationId),
    /// Checksum mismatch
    ChecksumMismatch(MigrationId),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Io(msg) => write!(f, "I/O error: {}", msg),
            MigrationError::Parse(msg) => write!(f, "Parse error: {}", msg),
            MigrationError::Execution(msg) => write!(f, "Execution error: {}", msg),
            MigrationError::Locked(holder) => write!(f, "Migrations locked by: {}", holder),
            MigrationError::NotFound(id) => write!(f, "Migration not found: {}", id),
            MigrationError::NotReversible(id) => write!(f, "Migration not reversible: {}", id),
            MigrationError::DependencyNotSatisfied(id, dep) => {
                write!(f, "Migration {} requires {} to be applied first", id, dep)
            }
            MigrationError::ChecksumMismatch(id) => {
                write!(f, "Migration checksum mismatch: {}", id)
            }
        }
    }
}

impl std::error::Error for MigrationError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_version() {
        let v = MigrationVersion::new(1, "initial");
        assert_eq!(v.version, 1);
        assert_eq!(v.name, "initial");
    }

    #[test]
    fn test_migration_creation() {
        let m = Migration::new(
            MigrationVersion::new(1, "create_users"),
            vec!["CREATE TABLE users (id INT);".to_string()],
        )
        .with_down(vec!["DROP TABLE users;".to_string()]);

        assert!(m.reversible);
        assert_eq!(m.up_sql.len(), 1);
        assert_eq!(m.down_sql.len(), 1);
        assert_eq!(m.id(), "1_create_users");
    }

    #[test]
    fn test_parse_migration_content() {
        let config = MigrationConfig::default();
        let mgr = MigrationManager::new(config);

        let content = r#"
-- UP
CREATE TABLE users (id INT);
CREATE TABLE posts (id INT);

-- DOWN
DROP TABLE posts;
DROP TABLE users;
"#;

        let (up, down) = mgr.parse_migration_content(content).unwrap();
        assert_eq!(up.len(), 2);
        assert_eq!(down.len(), 2);
    }

    #[test]
    fn test_pending_migrations() {
        let config = MigrationConfig::default();
        let mgr = MigrationManager::new(config);

        let m1 = Migration::new(
            MigrationVersion::new(1, "first"),
            vec!["SELECT 1;".to_string()],
        );
        let m2 = Migration::new(
            MigrationVersion::new(2, "second"),
            vec!["SELECT 2;".to_string()],
        );

        mgr.register(m1);
        mgr.register(m2);

        let pending = mgr.pending();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].version.version, 1);
        assert_eq!(pending[1].version.version, 2);
    }
}
