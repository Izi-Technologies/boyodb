// Tablespaces - Physical storage location management for BoyoDB
//
// Provides PostgreSQL-style tablespace support for:
// - Multiple physical storage locations
// - SSD/HDD tiering
// - Database/table placement control
// - Storage quota management

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::fs;

// ============================================================================
// Types and Configuration
// ============================================================================

/// Storage medium type for performance tiering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMedium {
    /// Solid state drive - fast random access
    Ssd,
    /// Hard disk drive - good for sequential, large storage
    Hdd,
    /// NVMe - fastest local storage
    Nvme,
    /// Network attached storage
    Nas,
    /// Cloud object storage (S3-compatible)
    ObjectStorage,
    /// RAM disk for temporary data
    RamDisk,
    /// Unknown or unspecified
    Unknown,
}

impl Default for StorageMedium {
    fn default() -> Self {
        StorageMedium::Unknown
    }
}

/// Tablespace configuration
#[derive(Debug, Clone)]
pub struct TablespaceConfig {
    /// Unique tablespace name
    pub name: String,
    /// Physical directory path
    pub location: PathBuf,
    /// Owner role
    pub owner: String,
    /// Storage medium type
    pub medium: StorageMedium,
    /// Maximum size in bytes (0 = unlimited)
    pub max_size: u64,
    /// Block size for this tablespace
    pub block_size: u32,
    /// Whether tablespace is read-only
    pub read_only: bool,
    /// Compression setting
    pub compression: CompressionType,
    /// Replication factor
    pub replication_factor: u8,
    /// Custom options
    pub options: HashMap<String, String>,
}

impl Default for TablespaceConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            location: PathBuf::new(),
            owner: "postgres".to_string(),
            medium: StorageMedium::Unknown,
            max_size: 0, // unlimited
            block_size: 8192,
            read_only: false,
            compression: CompressionType::None,
            replication_factor: 1,
            options: HashMap::new(),
        }
    }
}

/// Compression types for tablespace data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    #[default]
    None,
    Lz4,
    Zstd,
    Snappy,
    Gzip,
}

/// Runtime tablespace statistics
#[derive(Debug, Clone, Default)]
pub struct TablespaceStats {
    /// Total bytes used
    pub bytes_used: u64,
    /// Number of relations (tables/indexes)
    pub relation_count: u64,
    /// Number of files
    pub file_count: u64,
    /// Last vacuum time
    pub last_vacuum: Option<u64>,
    /// I/O statistics
    pub io_stats: IoStats,
}

/// I/O statistics for a tablespace
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub reads: u64,
    pub writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub read_time_ms: u64,
    pub write_time_ms: u64,
}

// ============================================================================
// Tablespace Definition
// ============================================================================

/// A tablespace represents a physical storage location
#[derive(Debug)]
pub struct Tablespace {
    /// Unique OID
    pub oid: u32,
    /// Configuration
    pub config: TablespaceConfig,
    /// Runtime statistics
    stats: RwLock<TablespaceStats>,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: RwLock<u64>,
}

impl Tablespace {
    /// Create a new tablespace
    pub fn new(oid: u32, config: TablespaceConfig) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            oid,
            config,
            stats: RwLock::new(TablespaceStats::default()),
            created_at: now,
            modified_at: RwLock::new(now),
        }
    }

    /// Get the tablespace name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get the physical location
    pub fn location(&self) -> &Path {
        &self.config.location
    }

    /// Check if tablespace is the default (pg_default equivalent)
    pub fn is_default(&self) -> bool {
        self.config.name == "pg_default" || self.config.name == "default"
    }

    /// Check if tablespace is for temporary objects (pg_global equivalent)
    pub fn is_global(&self) -> bool {
        self.config.name == "pg_global" || self.config.name == "global"
    }

    /// Get current statistics
    pub fn stats(&self) -> TablespaceStats {
        self.stats.read().unwrap().clone()
    }

    /// Update statistics
    pub fn update_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut TablespaceStats),
    {
        let mut stats = self.stats.write().unwrap();
        f(&mut stats);
    }

    /// Check if quota would be exceeded
    pub fn check_quota(&self, additional_bytes: u64) -> Result<(), TablespaceError> {
        if self.config.max_size == 0 {
            return Ok(()); // Unlimited
        }

        let stats = self.stats.read().unwrap();
        if stats.bytes_used + additional_bytes > self.config.max_size {
            return Err(TablespaceError::QuotaExceeded {
                tablespace: self.config.name.clone(),
                current: stats.bytes_used,
                requested: additional_bytes,
                limit: self.config.max_size,
            });
        }
        Ok(())
    }

    /// Get available space
    pub fn available_space(&self) -> u64 {
        if self.config.max_size == 0 {
            // Try to get filesystem available space
            if let Ok(metadata) = fs::metadata(&self.config.location) {
                // This is a simplification - real impl would use statvfs
                let _ = metadata;
                return u64::MAX;
            }
            return u64::MAX;
        }

        let stats = self.stats.read().unwrap();
        self.config.max_size.saturating_sub(stats.bytes_used)
    }

    /// Get path for a specific database in this tablespace
    pub fn database_path(&self, database_oid: u32) -> PathBuf {
        self.config.location.join(format!("{}", database_oid))
    }

    /// Get path for a specific relation in this tablespace
    pub fn relation_path(&self, database_oid: u32, relation_oid: u32) -> PathBuf {
        self.database_path(database_oid).join(format!("{}", relation_oid))
    }

    /// Initialize the tablespace directory structure
    pub fn initialize(&self) -> Result<(), TablespaceError> {
        if self.config.location.exists() {
            // Check if it's a directory
            if !self.config.location.is_dir() {
                return Err(TablespaceError::InvalidLocation(
                    self.config.location.display().to_string(),
                ));
            }
        } else {
            // Create the directory
            fs::create_dir_all(&self.config.location).map_err(|e| {
                TablespaceError::IoError(format!(
                    "Failed to create tablespace directory: {}",
                    e
                ))
            })?;
        }

        // Create version file
        let version_file = self.config.location.join("PG_VERSION");
        if !version_file.exists() {
            fs::write(&version_file, "15\n").map_err(|e| {
                TablespaceError::IoError(format!("Failed to write version file: {}", e))
            })?;
        }

        Ok(())
    }
}

// ============================================================================
// Tablespace Manager
// ============================================================================

/// Manages all tablespaces in the system
pub struct TablespaceManager {
    /// All tablespaces by OID
    tablespaces: RwLock<HashMap<u32, Arc<Tablespace>>>,
    /// Name to OID mapping
    name_index: RwLock<HashMap<String, u32>>,
    /// Next available OID
    next_oid: RwLock<u32>,
    /// Default data directory
    data_directory: PathBuf,
}

impl TablespaceManager {
    /// Create a new tablespace manager
    pub fn new(data_directory: PathBuf) -> Self {
        let manager = Self {
            tablespaces: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            next_oid: RwLock::new(16384), // Start after system OIDs
            data_directory,
        };

        // Initialize built-in tablespaces
        manager.initialize_builtins();
        manager
    }

    /// Initialize built-in tablespaces (pg_default, pg_global)
    fn initialize_builtins(&self) {
        // pg_default - default tablespace for user data
        let pg_default = TablespaceConfig {
            name: "pg_default".to_string(),
            location: self.data_directory.join("base"),
            owner: "postgres".to_string(),
            ..Default::default()
        };
        let _ = self.create_tablespace_internal(1663, pg_default);

        // pg_global - tablespace for shared system catalogs
        let pg_global = TablespaceConfig {
            name: "pg_global".to_string(),
            location: self.data_directory.join("global"),
            owner: "postgres".to_string(),
            ..Default::default()
        };
        let _ = self.create_tablespace_internal(1664, pg_global);
    }

    fn create_tablespace_internal(
        &self,
        oid: u32,
        config: TablespaceConfig,
    ) -> Result<Arc<Tablespace>, TablespaceError> {
        let name = config.name.clone();
        let tablespace = Arc::new(Tablespace::new(oid, config));

        // Initialize directory
        if let Err(e) = tablespace.initialize() {
            // Log but don't fail for built-in tablespaces
            eprintln!("Warning: Failed to initialize tablespace {}: {:?}", name, e);
        }

        let mut tablespaces = self.tablespaces.write().unwrap();
        let mut name_index = self.name_index.write().unwrap();

        tablespaces.insert(oid, tablespace.clone());
        name_index.insert(name, oid);

        Ok(tablespace)
    }

    /// Create a new tablespace
    pub fn create_tablespace(
        &self,
        config: TablespaceConfig,
    ) -> Result<Arc<Tablespace>, TablespaceError> {
        // Validate name
        if config.name.is_empty() {
            return Err(TablespaceError::InvalidName("empty name".to_string()));
        }
        if config.name.starts_with("pg_") {
            return Err(TablespaceError::InvalidName(
                "name cannot start with pg_".to_string(),
            ));
        }

        // Check for duplicate name
        {
            let name_index = self.name_index.read().unwrap();
            if name_index.contains_key(&config.name) {
                return Err(TablespaceError::AlreadyExists(config.name.clone()));
            }
        }

        // Validate location
        if config.location.as_os_str().is_empty() {
            return Err(TablespaceError::InvalidLocation("empty path".to_string()));
        }

        // Allocate OID
        let oid = {
            let mut next_oid = self.next_oid.write().unwrap();
            let oid = *next_oid;
            *next_oid += 1;
            oid
        };

        self.create_tablespace_internal(oid, config)
    }

    /// Drop a tablespace
    pub fn drop_tablespace(&self, name: &str, if_exists: bool) -> Result<(), TablespaceError> {
        // Check for built-in tablespaces
        if name == "pg_default" || name == "pg_global" {
            return Err(TablespaceError::CannotDropBuiltin(name.to_string()));
        }

        let oid = {
            let name_index = self.name_index.read().unwrap();
            match name_index.get(name) {
                Some(&oid) => oid,
                None => {
                    if if_exists {
                        return Ok(());
                    }
                    return Err(TablespaceError::NotFound(name.to_string()));
                }
            }
        };

        // Check if tablespace is empty
        {
            let tablespaces = self.tablespaces.read().unwrap();
            if let Some(ts) = tablespaces.get(&oid) {
                let stats = ts.stats();
                if stats.relation_count > 0 {
                    return Err(TablespaceError::NotEmpty {
                        tablespace: name.to_string(),
                        relation_count: stats.relation_count,
                    });
                }
            }
        }

        // Remove from maps
        let mut tablespaces = self.tablespaces.write().unwrap();
        let mut name_index = self.name_index.write().unwrap();

        tablespaces.remove(&oid);
        name_index.remove(name);

        Ok(())
    }

    /// Get a tablespace by name
    pub fn get_tablespace(&self, name: &str) -> Option<Arc<Tablespace>> {
        let name_index = self.name_index.read().unwrap();
        let oid = name_index.get(name)?;
        let tablespaces = self.tablespaces.read().unwrap();
        tablespaces.get(oid).cloned()
    }

    /// Get a tablespace by OID
    pub fn get_tablespace_by_oid(&self, oid: u32) -> Option<Arc<Tablespace>> {
        let tablespaces = self.tablespaces.read().unwrap();
        tablespaces.get(&oid).cloned()
    }

    /// Get the default tablespace
    pub fn default_tablespace(&self) -> Arc<Tablespace> {
        self.get_tablespace("pg_default")
            .expect("pg_default tablespace must exist")
    }

    /// Get the global tablespace
    pub fn global_tablespace(&self) -> Arc<Tablespace> {
        self.get_tablespace("pg_global")
            .expect("pg_global tablespace must exist")
    }

    /// List all tablespaces
    pub fn list_tablespaces(&self) -> Vec<Arc<Tablespace>> {
        let tablespaces = self.tablespaces.read().unwrap();
        tablespaces.values().cloned().collect()
    }

    /// Alter tablespace options
    pub fn alter_tablespace(
        &self,
        name: &str,
        options: AlterTablespaceOptions,
    ) -> Result<(), TablespaceError> {
        let tablespace = self
            .get_tablespace(name)
            .ok_or_else(|| TablespaceError::NotFound(name.to_string()))?;

        // Note: In a real implementation, we'd need interior mutability
        // for the config. For now, we just update stats.
        if let Some(new_owner) = &options.new_owner {
            let _ = new_owner; // Would update config.owner
        }

        if let Some(set_options) = &options.set_options {
            let _ = set_options; // Would update config.options
        }

        // Update modification time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        *tablespace.modified_at.write().unwrap() = now;

        Ok(())
    }

    /// Move a relation to a different tablespace
    pub fn move_relation(
        &self,
        _database_oid: u32,
        _relation_oid: u32,
        _from_tablespace: &str,
        _to_tablespace: &str,
    ) -> Result<(), TablespaceError> {
        // In a real implementation:
        // 1. Acquire exclusive lock on relation
        // 2. Copy data files to new tablespace
        // 3. Update system catalogs
        // 4. Remove old files
        // 5. Release lock
        Ok(())
    }

    /// Get total size across all tablespaces
    pub fn total_size(&self) -> u64 {
        let tablespaces = self.tablespaces.read().unwrap();
        tablespaces
            .values()
            .map(|ts| ts.stats().bytes_used)
            .sum()
    }

    /// Find optimal tablespace for new relation based on criteria
    pub fn find_optimal_tablespace(
        &self,
        criteria: &TablespaceSelectionCriteria,
    ) -> Option<Arc<Tablespace>> {
        let tablespaces = self.tablespaces.read().unwrap();

        tablespaces
            .values()
            .filter(|ts| {
                // Filter by medium if specified
                if let Some(medium) = criteria.preferred_medium {
                    if ts.config.medium != medium {
                        return false;
                    }
                }

                // Filter by available space
                if criteria.required_space > 0 {
                    if ts.available_space() < criteria.required_space {
                        return false;
                    }
                }

                // Filter out read-only if writes needed
                if criteria.needs_write && ts.config.read_only {
                    return false;
                }

                true
            })
            .max_by_key(|ts| ts.available_space())
            .cloned()
    }
}

/// Options for altering a tablespace
#[derive(Debug, Default)]
pub struct AlterTablespaceOptions {
    pub new_owner: Option<String>,
    pub new_name: Option<String>,
    pub set_options: Option<HashMap<String, String>>,
    pub reset_options: Option<Vec<String>>,
}

/// Criteria for selecting an optimal tablespace
#[derive(Debug, Default)]
pub struct TablespaceSelectionCriteria {
    pub preferred_medium: Option<StorageMedium>,
    pub required_space: u64,
    pub needs_write: bool,
    pub prefer_compression: bool,
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum TablespaceError {
    InvalidName(String),
    InvalidLocation(String),
    AlreadyExists(String),
    NotFound(String),
    NotEmpty {
        tablespace: String,
        relation_count: u64,
    },
    QuotaExceeded {
        tablespace: String,
        current: u64,
        requested: u64,
        limit: u64,
    },
    CannotDropBuiltin(String),
    IoError(String),
    PermissionDenied(String),
}

impl std::fmt::Display for TablespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName(msg) => write!(f, "Invalid tablespace name: {}", msg),
            Self::InvalidLocation(msg) => write!(f, "Invalid tablespace location: {}", msg),
            Self::AlreadyExists(name) => write!(f, "Tablespace '{}' already exists", name),
            Self::NotFound(name) => write!(f, "Tablespace '{}' not found", name),
            Self::NotEmpty { tablespace, relation_count } => {
                write!(
                    f,
                    "Tablespace '{}' is not empty ({} relations)",
                    tablespace, relation_count
                )
            }
            Self::QuotaExceeded {
                tablespace,
                current,
                requested,
                limit,
            } => {
                write!(
                    f,
                    "Tablespace '{}' quota exceeded: current={}, requested={}, limit={}",
                    tablespace, current, requested, limit
                )
            }
            Self::CannotDropBuiltin(name) => {
                write!(f, "Cannot drop built-in tablespace '{}'", name)
            }
            Self::IoError(msg) => write!(f, "I/O error: {}", msg),
            Self::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
        }
    }
}

impl std::error::Error for TablespaceError {}

// ============================================================================
// Tablespace-aware file operations
// ============================================================================

/// Tablespace-aware file handle
pub struct TablespaceFile {
    tablespace: Arc<Tablespace>,
    path: PathBuf,
    size: u64,
}

impl TablespaceFile {
    /// Create a new file in the tablespace
    pub fn create(
        tablespace: Arc<Tablespace>,
        database_oid: u32,
        relation_oid: u32,
        fork: &str,
    ) -> Result<Self, TablespaceError> {
        if tablespace.config.read_only {
            return Err(TablespaceError::PermissionDenied(
                "tablespace is read-only".to_string(),
            ));
        }

        let db_path = tablespace.database_path(database_oid);
        if !db_path.exists() {
            fs::create_dir_all(&db_path).map_err(|e| {
                TablespaceError::IoError(format!("Failed to create database directory: {}", e))
            })?;
        }

        let filename = if fork.is_empty() {
            format!("{}", relation_oid)
        } else {
            format!("{}_{}", relation_oid, fork)
        };
        let path = db_path.join(filename);

        Ok(Self {
            tablespace,
            path,
            size: 0,
        })
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Update the file size and tablespace statistics
    pub fn update_size(&mut self, new_size: u64) {
        let delta = new_size as i64 - self.size as i64;
        self.size = new_size;

        self.tablespace.update_stats(|stats| {
            if delta > 0 {
                stats.bytes_used += delta as u64;
            } else {
                stats.bytes_used = stats.bytes_used.saturating_sub((-delta) as u64);
            }
        });
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager() -> (TablespaceManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let manager = TablespaceManager::new(temp_dir.path().to_path_buf());
        (manager, temp_dir)
    }

    #[test]
    fn test_builtin_tablespaces() {
        let (manager, _dir) = create_test_manager();

        // pg_default should exist
        let pg_default = manager.get_tablespace("pg_default");
        assert!(pg_default.is_some());
        assert!(pg_default.unwrap().is_default());

        // pg_global should exist
        let pg_global = manager.get_tablespace("pg_global");
        assert!(pg_global.is_some());
        assert!(pg_global.unwrap().is_global());
    }

    #[test]
    fn test_create_tablespace() {
        let (manager, dir) = create_test_manager();

        let config = TablespaceConfig {
            name: "fast_ssd".to_string(),
            location: dir.path().join("fast_ssd"),
            owner: "admin".to_string(),
            medium: StorageMedium::Ssd,
            max_size: 1024 * 1024 * 1024, // 1GB
            ..Default::default()
        };

        let ts = manager.create_tablespace(config).unwrap();
        assert_eq!(ts.name(), "fast_ssd");
        assert_eq!(ts.config.medium, StorageMedium::Ssd);
        assert!(ts.location().exists());
    }

    #[test]
    fn test_cannot_create_pg_prefix() {
        let (manager, dir) = create_test_manager();

        let config = TablespaceConfig {
            name: "pg_custom".to_string(),
            location: dir.path().join("pg_custom"),
            ..Default::default()
        };

        let result = manager.create_tablespace(config);
        assert!(matches!(result, Err(TablespaceError::InvalidName(_))));
    }

    #[test]
    fn test_drop_tablespace() {
        let (manager, dir) = create_test_manager();

        let config = TablespaceConfig {
            name: "temp_ts".to_string(),
            location: dir.path().join("temp_ts"),
            ..Default::default()
        };

        manager.create_tablespace(config).unwrap();
        assert!(manager.get_tablespace("temp_ts").is_some());

        manager.drop_tablespace("temp_ts", false).unwrap();
        assert!(manager.get_tablespace("temp_ts").is_none());
    }

    #[test]
    fn test_cannot_drop_builtin() {
        let (manager, _dir) = create_test_manager();

        let result = manager.drop_tablespace("pg_default", false);
        assert!(matches!(result, Err(TablespaceError::CannotDropBuiltin(_))));

        let result = manager.drop_tablespace("pg_global", false);
        assert!(matches!(result, Err(TablespaceError::CannotDropBuiltin(_))));
    }

    #[test]
    fn test_quota_check() {
        let (manager, dir) = create_test_manager();

        let config = TablespaceConfig {
            name: "limited".to_string(),
            location: dir.path().join("limited"),
            max_size: 1000,
            ..Default::default()
        };

        let ts = manager.create_tablespace(config).unwrap();

        // Initially should allow
        assert!(ts.check_quota(500).is_ok());

        // Update stats to simulate usage
        ts.update_stats(|stats| {
            stats.bytes_used = 800;
        });

        // Now should fail for large allocation
        let result = ts.check_quota(300);
        assert!(matches!(result, Err(TablespaceError::QuotaExceeded { .. })));

        // Small allocation should still work
        assert!(ts.check_quota(100).is_ok());
    }

    #[test]
    fn test_find_optimal_tablespace() {
        let (manager, dir) = create_test_manager();

        // Create SSD tablespace
        let ssd_config = TablespaceConfig {
            name: "ssd_storage".to_string(),
            location: dir.path().join("ssd"),
            medium: StorageMedium::Ssd,
            max_size: 100_000,
            ..Default::default()
        };
        manager.create_tablespace(ssd_config).unwrap();

        // Create HDD tablespace
        let hdd_config = TablespaceConfig {
            name: "hdd_storage".to_string(),
            location: dir.path().join("hdd"),
            medium: StorageMedium::Hdd,
            max_size: 1_000_000,
            ..Default::default()
        };
        manager.create_tablespace(hdd_config).unwrap();

        // Find SSD tablespace
        let criteria = TablespaceSelectionCriteria {
            preferred_medium: Some(StorageMedium::Ssd),
            ..Default::default()
        };
        let result = manager.find_optimal_tablespace(&criteria);
        assert!(result.is_some());
        assert_eq!(result.unwrap().name(), "ssd_storage");

        // Find HDD tablespace
        let criteria = TablespaceSelectionCriteria {
            preferred_medium: Some(StorageMedium::Hdd),
            ..Default::default()
        };
        let result = manager.find_optimal_tablespace(&criteria);
        assert!(result.is_some());
        assert_eq!(result.unwrap().name(), "hdd_storage");
    }

    #[test]
    fn test_tablespace_paths() {
        let (manager, _dir) = create_test_manager();

        let ts = manager.default_tablespace();

        let db_path = ts.database_path(12345);
        assert!(db_path.ends_with("12345"));

        let rel_path = ts.relation_path(12345, 67890);
        assert!(rel_path.ends_with("67890"));
    }

    #[test]
    fn test_list_tablespaces() {
        let (manager, dir) = create_test_manager();

        // Should have at least pg_default and pg_global
        let initial_count = manager.list_tablespaces().len();
        assert!(initial_count >= 2);

        // Create more
        for i in 0..3 {
            let config = TablespaceConfig {
                name: format!("ts_{}", i),
                location: dir.path().join(format!("ts_{}", i)),
                ..Default::default()
            };
            manager.create_tablespace(config).unwrap();
        }

        let all = manager.list_tablespaces();
        assert_eq!(all.len(), initial_count + 3);
    }

    #[test]
    fn test_storage_medium() {
        assert_eq!(StorageMedium::default(), StorageMedium::Unknown);

        let mediums = [
            StorageMedium::Ssd,
            StorageMedium::Hdd,
            StorageMedium::Nvme,
            StorageMedium::Nas,
            StorageMedium::ObjectStorage,
            StorageMedium::RamDisk,
        ];

        for medium in mediums {
            assert_ne!(medium, StorageMedium::Unknown);
        }
    }

    #[test]
    fn test_tablespace_stats() {
        let (manager, dir) = create_test_manager();

        let config = TablespaceConfig {
            name: "stats_test".to_string(),
            location: dir.path().join("stats_test"),
            ..Default::default()
        };

        let ts = manager.create_tablespace(config).unwrap();

        // Initial stats
        let stats = ts.stats();
        assert_eq!(stats.bytes_used, 0);
        assert_eq!(stats.relation_count, 0);

        // Update stats
        ts.update_stats(|s| {
            s.bytes_used = 1024;
            s.relation_count = 5;
            s.io_stats.reads = 100;
        });

        let stats = ts.stats();
        assert_eq!(stats.bytes_used, 1024);
        assert_eq!(stats.relation_count, 5);
        assert_eq!(stats.io_stats.reads, 100);
    }

    #[test]
    fn test_available_space() {
        let (manager, dir) = create_test_manager();

        // Unlimited tablespace
        let unlimited_config = TablespaceConfig {
            name: "unlimited".to_string(),
            location: dir.path().join("unlimited"),
            max_size: 0,
            ..Default::default()
        };
        let ts = manager.create_tablespace(unlimited_config).unwrap();
        assert_eq!(ts.available_space(), u64::MAX);

        // Limited tablespace
        let limited_config = TablespaceConfig {
            name: "limited2".to_string(),
            location: dir.path().join("limited2"),
            max_size: 10000,
            ..Default::default()
        };
        let ts = manager.create_tablespace(limited_config).unwrap();

        // Initially full space available
        assert_eq!(ts.available_space(), 10000);

        // After using some space
        ts.update_stats(|s| {
            s.bytes_used = 3000;
        });
        assert_eq!(ts.available_space(), 7000);
    }

    #[test]
    fn test_tablespace_error_display() {
        let errors = vec![
            TablespaceError::InvalidName("test".to_string()),
            TablespaceError::InvalidLocation("/bad".to_string()),
            TablespaceError::AlreadyExists("dup".to_string()),
            TablespaceError::NotFound("missing".to_string()),
            TablespaceError::NotEmpty {
                tablespace: "busy".to_string(),
                relation_count: 5,
            },
            TablespaceError::QuotaExceeded {
                tablespace: "full".to_string(),
                current: 100,
                requested: 50,
                limit: 120,
            },
            TablespaceError::CannotDropBuiltin("pg_default".to_string()),
            TablespaceError::IoError("read failed".to_string()),
            TablespaceError::PermissionDenied("no access".to_string()),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_tablespace_file_create() {
        let (manager, _dir) = create_test_manager();

        let ts = manager.default_tablespace();
        let file = TablespaceFile::create(ts.clone(), 12345, 67890, "");

        assert!(file.is_ok());
        let file = file.unwrap();
        assert!(file.path().to_string_lossy().contains("67890"));
    }

    #[test]
    fn test_compression_type_default() {
        let compression = CompressionType::default();
        assert_eq!(compression, CompressionType::None);
    }

    #[test]
    fn test_io_stats() {
        let mut stats = IoStats::default();
        assert_eq!(stats.reads, 0);
        assert_eq!(stats.writes, 0);

        stats.reads = 100;
        stats.writes = 50;
        stats.bytes_read = 1024 * 100;
        stats.bytes_written = 1024 * 50;

        assert_eq!(stats.reads, 100);
        assert_eq!(stats.bytes_read, 102400);
    }
}
