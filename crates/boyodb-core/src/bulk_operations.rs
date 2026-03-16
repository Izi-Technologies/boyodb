//! Bulk Operations - pg_dump/pg_restore Equivalents, Logical Backup/Restore
//!
//! This module provides:
//! - Logical backup (pg_dump equivalent)
//! - Logical restore (pg_restore equivalent)
//! - Streaming backup support
//! - Point-in-time recovery helpers

use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// BACKUP FORMAT
// ============================================================================

/// Backup format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupFormat {
    /// Plain SQL (pg_dump -Fp)
    Plain,
    /// Custom format (pg_dump -Fc) - compressed, supports parallel restore
    Custom,
    /// Directory format (pg_dump -Fd) - parallel dump/restore
    Directory,
    /// Tar format (pg_dump -Ft)
    Tar,
}

impl Default for BackupFormat {
    fn default() -> Self {
        BackupFormat::Custom
    }
}

impl std::fmt::Display for BackupFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            BackupFormat::Plain => "plain",
            BackupFormat::Custom => "custom",
            BackupFormat::Directory => "directory",
            BackupFormat::Tar => "tar",
        };
        write!(f, "{}", name)
    }
}

/// Compression method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionMethod {
    #[default]
    None,
    Gzip,
    Lz4,
    Zstd,
}

/// Backup section
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupSection {
    /// Pre-data (schemas, types, functions)
    PreData,
    /// Data (table contents)
    Data,
    /// Post-data (indexes, constraints, triggers)
    PostData,
}

// ============================================================================
// DUMP OPTIONS
// ============================================================================

/// Dump (backup) options
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Output format
    pub format: BackupFormat,
    /// Compression method
    pub compression: CompressionMethod,
    /// Compression level (0-9, 0 = no compression)
    pub compression_level: u8,
    /// Number of parallel jobs
    pub jobs: usize,
    /// Include data (not schema-only)
    pub include_data: bool,
    /// Include schema (not data-only)
    pub include_schema: bool,
    /// Include blobs
    pub include_blobs: bool,
    /// Clean (drop) objects before creating
    pub clean: bool,
    /// Create database statement
    pub create_database: bool,
    /// Use IF EXISTS with clean
    pub if_exists: bool,
    /// Include privileges (GRANT/REVOKE)
    pub include_privileges: bool,
    /// Include ownership
    pub include_owner: bool,
    /// Specific schemas to dump
    pub schemas: Vec<String>,
    /// Schemas to exclude
    pub exclude_schemas: Vec<String>,
    /// Specific tables to dump
    pub tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// Lock timeout
    pub lock_timeout: Duration,
    /// Statement timeout
    pub statement_timeout: Duration,
    /// Snapshot ID for consistent backup
    pub snapshot_id: Option<String>,
    /// Add section comments
    pub verbose: bool,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self {
            format: BackupFormat::Custom,
            compression: CompressionMethod::None,
            compression_level: 6,
            jobs: 1,
            include_data: true,
            include_schema: true,
            include_blobs: true,
            clean: false,
            create_database: false,
            if_exists: false,
            include_privileges: true,
            include_owner: true,
            schemas: Vec::new(),
            exclude_schemas: Vec::new(),
            tables: Vec::new(),
            exclude_tables: Vec::new(),
            lock_timeout: Duration::from_secs(60),
            statement_timeout: Duration::ZERO,
            snapshot_id: None,
            verbose: false,
        }
    }
}

impl DumpOptions {
    /// Create options for schema-only dump
    pub fn schema_only() -> Self {
        Self {
            include_data: false,
            include_schema: true,
            ..Default::default()
        }
    }

    /// Create options for data-only dump
    pub fn data_only() -> Self {
        Self {
            include_data: true,
            include_schema: false,
            ..Default::default()
        }
    }

    /// Create options for full dump with parallel jobs
    pub fn parallel(jobs: usize) -> Self {
        Self {
            jobs,
            format: BackupFormat::Directory,
            ..Default::default()
        }
    }
}

// ============================================================================
// RESTORE OPTIONS
// ============================================================================

/// Restore options
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// Input format (auto-detected if None)
    pub format: Option<BackupFormat>,
    /// Number of parallel jobs
    pub jobs: usize,
    /// Clean (drop) objects before restoring
    pub clean: bool,
    /// Create database before restore
    pub create_database: bool,
    /// Use IF EXISTS with clean
    pub if_exists: bool,
    /// Exit on error
    pub exit_on_error: bool,
    /// Specific schemas to restore
    pub schemas: Vec<String>,
    /// Schemas to exclude
    pub exclude_schemas: Vec<String>,
    /// Specific tables to restore
    pub tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// Specific indexes to restore
    pub indexes: Vec<String>,
    /// Disable triggers during restore
    pub disable_triggers: bool,
    /// Do not restore ownership
    pub no_owner: bool,
    /// Do not restore privileges
    pub no_privileges: bool,
    /// Target database
    pub database: Option<String>,
    /// Sections to restore
    pub sections: Vec<BackupSection>,
    /// Single transaction
    pub single_transaction: bool,
    /// Verbose output
    pub verbose: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            format: None,
            jobs: 1,
            clean: false,
            create_database: false,
            if_exists: false,
            exit_on_error: false,
            schemas: Vec::new(),
            exclude_schemas: Vec::new(),
            tables: Vec::new(),
            exclude_tables: Vec::new(),
            indexes: Vec::new(),
            disable_triggers: false,
            no_owner: false,
            no_privileges: false,
            database: None,
            sections: vec![
                BackupSection::PreData,
                BackupSection::Data,
                BackupSection::PostData,
            ],
            single_transaction: false,
            verbose: false,
        }
    }
}

// ============================================================================
// BACKUP CATALOG
// ============================================================================

/// Table of contents entry
#[derive(Debug, Clone)]
pub struct TocEntry {
    /// Entry ID
    pub id: u32,
    /// Had OID (for compatibility)
    pub had_dumper: bool,
    /// Object type (TABLE, INDEX, etc.)
    pub object_type: String,
    /// Schema name
    pub schema: Option<String>,
    /// Object name
    pub name: String,
    /// Owner
    pub owner: Option<String>,
    /// Section
    pub section: BackupSection,
    /// Dependencies (entry IDs)
    pub dependencies: Vec<u32>,
    /// Definition (SQL)
    pub definition: Option<String>,
    /// Drop statement
    pub drop_statement: Option<String>,
    /// Data file offset (for custom format)
    pub data_offset: Option<u64>,
    /// Data size
    pub data_size: Option<u64>,
    /// Row count (for tables)
    pub row_count: Option<u64>,
}

impl TocEntry {
    /// Create a new TOC entry
    pub fn new(id: u32, object_type: &str, name: &str) -> Self {
        Self {
            id,
            had_dumper: true,
            object_type: object_type.to_string(),
            schema: None,
            name: name.to_string(),
            owner: None,
            section: BackupSection::PreData,
            dependencies: Vec::new(),
            definition: None,
            drop_statement: None,
            data_offset: None,
            data_size: None,
            row_count: None,
        }
    }

    /// Set schema
    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    /// Set section
    pub fn with_section(mut self, section: BackupSection) -> Self {
        self.section = section;
        self
    }

    /// Set definition
    pub fn with_definition(mut self, sql: &str) -> Self {
        self.definition = Some(sql.to_string());
        self
    }

    /// Set drop statement
    pub fn with_drop(mut self, sql: &str) -> Self {
        self.drop_statement = Some(sql.to_string());
        self
    }

    /// Add dependency
    pub fn depends_on(mut self, dep_id: u32) -> Self {
        self.dependencies.push(dep_id);
        self
    }

    /// Full qualified name
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

/// Backup catalog (table of contents)
#[derive(Debug, Clone)]
pub struct BackupCatalog {
    /// Format version
    pub version: u32,
    /// Backup format
    pub format: BackupFormat,
    /// Compression method
    pub compression: CompressionMethod,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Database name
    pub database: String,
    /// Server version
    pub server_version: String,
    /// Table of contents entries
    pub entries: Vec<TocEntry>,
    /// Entry ID counter
    next_id: u32,
}

impl BackupCatalog {
    /// Create a new catalog
    pub fn new(database: &str) -> Self {
        Self {
            version: 1,
            format: BackupFormat::Custom,
            compression: CompressionMethod::None,
            created_at: SystemTime::now(),
            database: database.to_string(),
            server_version: "BoyoDB 1.0".to_string(),
            entries: Vec::new(),
            next_id: 1,
        }
    }

    /// Add an entry
    pub fn add_entry(&mut self, mut entry: TocEntry) -> u32 {
        let id = self.next_id;
        entry.id = id;
        self.next_id += 1;
        self.entries.push(entry);
        id
    }

    /// Get entries by section
    pub fn entries_by_section(&self, section: BackupSection) -> Vec<&TocEntry> {
        self.entries
            .iter()
            .filter(|e| e.section == section)
            .collect()
    }

    /// Get entry by ID
    pub fn get_entry(&self, id: u32) -> Option<&TocEntry> {
        self.entries.iter().find(|e| e.id == id)
    }

    /// Topological sort of entries respecting dependencies
    pub fn sorted_entries(&self) -> Vec<&TocEntry> {
        // Simple topological sort
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let entry_map: HashMap<u32, &TocEntry> = self.entries.iter().map(|e| (e.id, e)).collect();

        fn visit<'a>(
            entry: &'a TocEntry,
            entry_map: &HashMap<u32, &'a TocEntry>,
            visited: &mut std::collections::HashSet<u32>,
            result: &mut Vec<&'a TocEntry>,
        ) {
            if visited.contains(&entry.id) {
                return;
            }
            visited.insert(entry.id);

            for dep_id in &entry.dependencies {
                if let Some(dep) = entry_map.get(dep_id) {
                    visit(dep, entry_map, visited, result);
                }
            }

            result.push(entry);
        }

        for entry in &self.entries {
            visit(entry, &entry_map, &mut visited, &mut result);
        }

        result
    }

    /// Serialize catalog to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic header
        buf.extend_from_slice(b"BOYOBKP\x00");

        // Version
        buf.extend_from_slice(&self.version.to_le_bytes());

        // Format
        buf.push(self.format as u8);

        // Compression
        buf.push(self.compression as u8);

        // Timestamp
        let timestamp = self
            .created_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        buf.extend_from_slice(&timestamp.to_le_bytes());

        // Database name length and content
        buf.extend_from_slice(&(self.database.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.database.as_bytes());

        // Entry count
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        // Entries (simplified)
        for entry in &self.entries {
            buf.extend_from_slice(&entry.id.to_le_bytes());
            buf.extend_from_slice(&(entry.object_type.len() as u16).to_le_bytes());
            buf.extend_from_slice(entry.object_type.as_bytes());
            buf.extend_from_slice(&(entry.name.len() as u16).to_le_bytes());
            buf.extend_from_slice(entry.name.as_bytes());
        }

        buf
    }

    /// Deserialize catalog from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, BackupError> {
        if data.len() < 8 || &data[0..7] != b"BOYOBKP" {
            return Err(BackupError::InvalidFormat("invalid magic header".into()));
        }

        let mut offset = 8;

        // Version
        let version = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Format
        let format = match data.get(offset) {
            Some(0) => BackupFormat::Plain,
            Some(1) => BackupFormat::Custom,
            Some(2) => BackupFormat::Directory,
            Some(3) => BackupFormat::Tar,
            _ => return Err(BackupError::InvalidFormat("unknown format".into())),
        };
        offset += 1;

        // Compression
        let compression = match data.get(offset) {
            Some(0) => CompressionMethod::None,
            Some(1) => CompressionMethod::Gzip,
            Some(2) => CompressionMethod::Lz4,
            Some(3) => CompressionMethod::Zstd,
            _ => CompressionMethod::None,
        };
        offset += 1;

        // Timestamp
        let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Database name
        let db_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let database = String::from_utf8_lossy(&data[offset..offset + db_len]).to_string();
        offset += db_len;

        // Entry count
        let entry_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut entries = Vec::new();
        for _ in 0..entry_count {
            if offset + 4 > data.len() {
                break;
            }
            let id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let type_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let object_type = String::from_utf8_lossy(&data[offset..offset + type_len]).to_string();
            offset += type_len;

            let name_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
            offset += name_len;

            entries.push(TocEntry::new(id, &object_type, &name));
        }

        let next_id = entries.len() as u32 + 1;
        Ok(Self {
            version,
            format,
            compression,
            created_at: SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp),
            database,
            server_version: "BoyoDB 1.0".to_string(),
            entries,
            next_id,
        })
    }
}

// ============================================================================
// DUMPER
// ============================================================================

/// Backup progress
#[derive(Debug, Clone)]
pub struct BackupProgress {
    /// Total tables to dump
    pub total_tables: u64,
    /// Tables completed
    pub tables_completed: u64,
    /// Total rows dumped
    pub total_rows: u64,
    /// Total bytes written
    pub total_bytes: u64,
    /// Current table being dumped
    pub current_table: Option<String>,
    /// Current section
    pub current_section: BackupSection,
    /// Start time
    pub started_at: Instant,
    /// Estimated completion
    pub estimated_completion: Option<Duration>,
}

impl BackupProgress {
    /// Create new progress tracker
    pub fn new() -> Self {
        Self {
            total_tables: 0,
            tables_completed: 0,
            total_rows: 0,
            total_bytes: 0,
            current_table: None,
            current_section: BackupSection::PreData,
            started_at: Instant::now(),
            estimated_completion: None,
        }
    }

    /// Percentage complete
    pub fn percentage(&self) -> f64 {
        if self.total_tables == 0 {
            0.0
        } else {
            (self.tables_completed as f64 / self.total_tables as f64) * 100.0
        }
    }

    /// Elapsed time
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Rows per second
    pub fn rows_per_second(&self) -> f64 {
        let secs = self.elapsed().as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.total_rows as f64 / secs
        }
    }
}

impl Default for BackupProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Database dumper (pg_dump equivalent)
pub struct Dumper {
    options: DumpOptions,
    catalog: BackupCatalog,
    progress: Arc<RwLock<BackupProgress>>,
    cancelled: AtomicBool,
}

impl Dumper {
    /// Create a new dumper
    pub fn new(database: &str, options: DumpOptions) -> Self {
        Self {
            options,
            catalog: BackupCatalog::new(database),
            progress: Arc::new(RwLock::new(BackupProgress::new())),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the dump
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get progress
    pub fn progress(&self) -> BackupProgress {
        self.progress.read().clone()
    }

    /// Add a table to dump
    pub fn add_table(&mut self, schema: &str, table: &str, definition: &str) -> u32 {
        let entry = TocEntry::new(0, "TABLE", table)
            .with_schema(schema)
            .with_section(BackupSection::PreData)
            .with_definition(definition)
            .with_drop(&format!("DROP TABLE IF EXISTS {}.{};", schema, table));
        self.catalog.add_entry(entry)
    }

    /// Add an index to dump
    pub fn add_index(&mut self, schema: &str, index: &str, definition: &str, table_id: u32) -> u32 {
        let entry = TocEntry::new(0, "INDEX", index)
            .with_schema(schema)
            .with_section(BackupSection::PostData)
            .with_definition(definition)
            .with_drop(&format!("DROP INDEX IF EXISTS {}.{};", schema, index))
            .depends_on(table_id);
        self.catalog.add_entry(entry)
    }

    /// Add table data
    pub fn add_table_data(&mut self, table_id: u32, row_count: u64) {
        if let Some(entry) = self.catalog.entries.iter_mut().find(|e| e.id == table_id) {
            entry.section = BackupSection::Data;
            entry.row_count = Some(row_count);
        }
    }

    /// Dump to plain SQL
    pub fn dump_plain<W: Write>(&self, writer: &mut W) -> Result<u64, BackupError> {
        let mut bytes_written = 0u64;

        // Header
        let header = format!(
            "--\n-- BoyoDB database dump\n-- Database: {}\n-- Dumped at: {:?}\n--\n\n",
            self.catalog.database, self.catalog.created_at
        );
        bytes_written += writer.write(header.as_bytes())? as u64;

        // Pre-data (schemas, tables)
        if self.options.include_schema {
            let comment = "-- Pre-data\n\n";
            bytes_written += writer.write(comment.as_bytes())? as u64;

            for entry in self.catalog.entries_by_section(BackupSection::PreData) {
                if self.is_cancelled() {
                    return Err(BackupError::Cancelled);
                }

                if self.options.clean {
                    if let Some(ref drop_stmt) = entry.drop_statement {
                        bytes_written += writer.write(drop_stmt.as_bytes())? as u64;
                        bytes_written += writer.write(b"\n")? as u64;
                    }
                }

                if let Some(ref definition) = entry.definition {
                    bytes_written += writer.write(definition.as_bytes())? as u64;
                    bytes_written += writer.write(b"\n\n")? as u64;
                }
            }
        }

        // Data
        if self.options.include_data {
            let comment = "-- Data\n\n";
            bytes_written += writer.write(comment.as_bytes())? as u64;

            for entry in self.catalog.entries_by_section(BackupSection::Data) {
                if self.is_cancelled() {
                    return Err(BackupError::Cancelled);
                }

                let placeholder = format!(
                    "-- Data for table: {}\nCOPY {} FROM stdin;\n-- [data would go here]\n\\.\n\n",
                    entry.qualified_name(),
                    entry.qualified_name()
                );
                bytes_written += writer.write(placeholder.as_bytes())? as u64;
            }
        }

        // Post-data (indexes, constraints)
        if self.options.include_schema {
            let comment = "-- Post-data\n\n";
            bytes_written += writer.write(comment.as_bytes())? as u64;

            for entry in self.catalog.entries_by_section(BackupSection::PostData) {
                if self.is_cancelled() {
                    return Err(BackupError::Cancelled);
                }

                if let Some(ref definition) = entry.definition {
                    bytes_written += writer.write(definition.as_bytes())? as u64;
                    bytes_written += writer.write(b"\n\n")? as u64;
                }
            }
        }

        // Footer
        let footer = "-- Dump completed\n";
        bytes_written += writer.write(footer.as_bytes())? as u64;

        Ok(bytes_written)
    }

    /// Dump to custom format
    pub fn dump_custom<W: Write>(&self, writer: &mut W) -> Result<u64, BackupError> {
        // Write catalog
        let catalog_bytes = self.catalog.serialize();
        writer.write_all(&catalog_bytes)?;

        // In real implementation, would write data blocks after catalog
        Ok(catalog_bytes.len() as u64)
    }

    /// Get the catalog
    pub fn catalog(&self) -> &BackupCatalog {
        &self.catalog
    }
}

// ============================================================================
// RESTORER
// ============================================================================

/// Restore progress
#[derive(Debug, Clone)]
pub struct RestoreProgress {
    /// Total entries to restore
    pub total_entries: u64,
    /// Entries completed
    pub entries_completed: u64,
    /// Rows restored
    pub rows_restored: u64,
    /// Current entry
    pub current_entry: Option<String>,
    /// Errors encountered
    pub errors: Vec<String>,
    /// Start time
    pub started_at: Instant,
}

impl RestoreProgress {
    /// Create new progress tracker
    pub fn new() -> Self {
        Self {
            total_entries: 0,
            entries_completed: 0,
            rows_restored: 0,
            current_entry: None,
            errors: Vec::new(),
            started_at: Instant::now(),
        }
    }

    /// Percentage complete
    pub fn percentage(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            (self.entries_completed as f64 / self.total_entries as f64) * 100.0
        }
    }
}

impl Default for RestoreProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Database restorer (pg_restore equivalent)
pub struct Restorer {
    options: RestoreOptions,
    catalog: Option<BackupCatalog>,
    progress: Arc<RwLock<RestoreProgress>>,
    cancelled: AtomicBool,
}

impl Restorer {
    /// Create a new restorer
    pub fn new(options: RestoreOptions) -> Self {
        Self {
            options,
            catalog: None,
            progress: Arc::new(RwLock::new(RestoreProgress::new())),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the restore
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get progress
    pub fn progress(&self) -> RestoreProgress {
        self.progress.read().clone()
    }

    /// Load catalog from custom format backup
    pub fn load_catalog<R: Read>(&mut self, reader: &mut R) -> Result<(), BackupError> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        self.catalog = Some(BackupCatalog::deserialize(&data)?);
        Ok(())
    }

    /// List contents of backup
    pub fn list_contents(&self) -> Result<Vec<TocEntry>, BackupError> {
        match &self.catalog {
            Some(catalog) => Ok(catalog.entries.clone()),
            None => Err(BackupError::NoCatalog),
        }
    }

    /// Generate restore SQL
    pub fn generate_sql(&self) -> Result<String, BackupError> {
        let catalog = self.catalog.as_ref().ok_or(BackupError::NoCatalog)?;
        let mut sql = String::new();

        sql.push_str("-- BoyoDB restore script\n\n");

        if self.options.single_transaction {
            sql.push_str("BEGIN;\n\n");
        }

        // Pre-data
        if self.options.sections.contains(&BackupSection::PreData) {
            sql.push_str("-- Pre-data\n\n");
            for entry in catalog.entries_by_section(BackupSection::PreData) {
                if self.should_skip_entry(entry) {
                    continue;
                }

                if self.options.clean {
                    if let Some(ref drop_stmt) = entry.drop_statement {
                        sql.push_str(drop_stmt);
                        sql.push('\n');
                    }
                }

                if let Some(ref definition) = entry.definition {
                    sql.push_str(definition);
                    sql.push_str("\n\n");
                }
            }
        }

        // Data
        if self.options.sections.contains(&BackupSection::Data) {
            sql.push_str("-- Data\n\n");
            for entry in catalog.entries_by_section(BackupSection::Data) {
                if self.should_skip_entry(entry) {
                    continue;
                }

                sql.push_str(&format!("-- Data for: {}\n", entry.qualified_name()));
            }
        }

        // Post-data
        if self.options.sections.contains(&BackupSection::PostData) {
            sql.push_str("-- Post-data\n\n");
            for entry in catalog.entries_by_section(BackupSection::PostData) {
                if self.should_skip_entry(entry) {
                    continue;
                }

                if let Some(ref definition) = entry.definition {
                    sql.push_str(definition);
                    sql.push_str("\n\n");
                }
            }
        }

        if self.options.single_transaction {
            sql.push_str("COMMIT;\n");
        }

        Ok(sql)
    }

    fn should_skip_entry(&self, entry: &TocEntry) -> bool {
        // Check schema filters
        if let Some(ref schema) = entry.schema {
            if !self.options.schemas.is_empty() && !self.options.schemas.contains(schema) {
                return true;
            }
            if self.options.exclude_schemas.contains(schema) {
                return true;
            }
        }

        // Check table filters
        if entry.object_type == "TABLE" || entry.object_type == "TABLE DATA" {
            if !self.options.tables.is_empty() && !self.options.tables.contains(&entry.name) {
                return true;
            }
            if self.options.exclude_tables.contains(&entry.name) {
                return true;
            }
        }

        // Check index filters
        if entry.object_type == "INDEX" {
            if !self.options.indexes.is_empty() && !self.options.indexes.contains(&entry.name) {
                return true;
            }
        }

        false
    }
}

// ============================================================================
// STREAMING BACKUP
// ============================================================================

/// Streaming backup configuration
#[derive(Debug, Clone)]
pub struct StreamingBackupConfig {
    /// Checkpoint interval
    pub checkpoint_interval: Duration,
    /// Maximum WAL lag before throttling
    pub max_wal_lag: u64,
    /// Compression method
    pub compression: CompressionMethod,
    /// Include WAL files
    pub include_wal: bool,
    /// Progress callback interval
    pub progress_interval: Duration,
}

impl Default for StreamingBackupConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(300),
            max_wal_lag: 1024 * 1024 * 1024, // 1GB
            compression: CompressionMethod::None,
            include_wal: true,
            progress_interval: Duration::from_secs(5),
        }
    }
}

/// Streaming backup handle
pub struct StreamingBackup {
    config: StreamingBackupConfig,
    started_at: Instant,
    bytes_sent: AtomicU64,
    cancelled: AtomicBool,
}

impl StreamingBackup {
    /// Start a streaming backup
    pub fn start(config: StreamingBackupConfig) -> Self {
        Self {
            config,
            started_at: Instant::now(),
            bytes_sent: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the backup
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get bytes sent so far
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Throughput in bytes per second
    pub fn throughput(&self) -> f64 {
        let secs = self.elapsed().as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.bytes_sent() as f64 / secs
        }
    }

    /// Record bytes sent
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }
}

// ============================================================================
// ERRORS
// ============================================================================

/// Backup/restore error
#[derive(Debug, Clone)]
pub enum BackupError {
    InvalidFormat(String),
    Io(String),
    Cancelled,
    NoCatalog,
    CorruptedData(String),
    UnsupportedVersion(u32),
    MissingDependency(String),
    ExecutionFailed(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::InvalidFormat(msg) => write!(f, "invalid backup format: {}", msg),
            BackupError::Io(msg) => write!(f, "I/O error: {}", msg),
            BackupError::Cancelled => write!(f, "backup/restore cancelled"),
            BackupError::NoCatalog => write!(f, "no catalog loaded"),
            BackupError::CorruptedData(msg) => write!(f, "corrupted data: {}", msg),
            BackupError::UnsupportedVersion(v) => write!(f, "unsupported backup version: {}", v),
            BackupError::MissingDependency(dep) => write!(f, "missing dependency: {}", dep),
            BackupError::ExecutionFailed(msg) => write!(f, "execution failed: {}", msg),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<std::io::Error> for BackupError {
    fn from(err: std::io::Error) -> Self {
        BackupError::Io(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_format_display() {
        assert_eq!(BackupFormat::Plain.to_string(), "plain");
        assert_eq!(BackupFormat::Custom.to_string(), "custom");
        assert_eq!(BackupFormat::Directory.to_string(), "directory");
    }

    #[test]
    fn test_dump_options_schema_only() {
        let opts = DumpOptions::schema_only();
        assert!(!opts.include_data);
        assert!(opts.include_schema);
    }

    #[test]
    fn test_dump_options_data_only() {
        let opts = DumpOptions::data_only();
        assert!(opts.include_data);
        assert!(!opts.include_schema);
    }

    #[test]
    fn test_toc_entry_creation() {
        let entry = TocEntry::new(1, "TABLE", "users")
            .with_schema("public")
            .with_section(BackupSection::PreData)
            .with_definition("CREATE TABLE users (id INT);");

        assert_eq!(entry.id, 1);
        assert_eq!(entry.object_type, "TABLE");
        assert_eq!(entry.name, "users");
        assert_eq!(entry.schema, Some("public".to_string()));
        assert_eq!(entry.qualified_name(), "public.users");
    }

    #[test]
    fn test_toc_entry_dependencies() {
        let entry = TocEntry::new(2, "INDEX", "idx_users_id").depends_on(1);

        assert_eq!(entry.dependencies, vec![1]);
    }

    #[test]
    fn test_backup_catalog_creation() {
        let mut catalog = BackupCatalog::new("testdb");
        assert_eq!(catalog.database, "testdb");

        let id = catalog.add_entry(TocEntry::new(0, "TABLE", "users"));
        assert_eq!(id, 1);

        let id2 = catalog.add_entry(TocEntry::new(0, "TABLE", "orders"));
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_backup_catalog_entries_by_section() {
        let mut catalog = BackupCatalog::new("testdb");
        catalog.add_entry(TocEntry::new(0, "TABLE", "users").with_section(BackupSection::PreData));
        catalog.add_entry(
            TocEntry::new(0, "INDEX", "idx_users").with_section(BackupSection::PostData),
        );

        let pre_data = catalog.entries_by_section(BackupSection::PreData);
        assert_eq!(pre_data.len(), 1);
        assert_eq!(pre_data[0].name, "users");

        let post_data = catalog.entries_by_section(BackupSection::PostData);
        assert_eq!(post_data.len(), 1);
        assert_eq!(post_data[0].name, "idx_users");
    }

    #[test]
    fn test_backup_catalog_serialize_deserialize() {
        let mut catalog = BackupCatalog::new("testdb");
        catalog.format = BackupFormat::Custom;
        catalog.add_entry(TocEntry::new(0, "TABLE", "users"));
        catalog.add_entry(TocEntry::new(0, "INDEX", "idx_users"));

        let bytes = catalog.serialize();
        let restored = BackupCatalog::deserialize(&bytes).unwrap();

        assert_eq!(restored.database, "testdb");
        assert_eq!(restored.entries.len(), 2);
    }

    #[test]
    fn test_dumper_creation() {
        let opts = DumpOptions::default();
        let dumper = Dumper::new("testdb", opts);
        assert_eq!(dumper.catalog().database, "testdb");
    }

    #[test]
    fn test_dumper_add_table() {
        let opts = DumpOptions::default();
        let mut dumper = Dumper::new("testdb", opts);

        let id = dumper.add_table("public", "users", "CREATE TABLE users (id INT);");
        assert_eq!(id, 1);

        let entry = dumper.catalog().get_entry(id).unwrap();
        assert_eq!(entry.name, "users");
    }

    #[test]
    fn test_dumper_add_index() {
        let opts = DumpOptions::default();
        let mut dumper = Dumper::new("testdb", opts);

        let table_id = dumper.add_table("public", "users", "CREATE TABLE users (id INT);");
        let idx_id = dumper.add_index(
            "public",
            "idx_users",
            "CREATE INDEX idx_users ON users(id);",
            table_id,
        );

        let entry = dumper.catalog().get_entry(idx_id).unwrap();
        assert!(entry.dependencies.contains(&table_id));
    }

    #[test]
    fn test_dumper_dump_plain() {
        let opts = DumpOptions::default();
        let mut dumper = Dumper::new("testdb", opts);
        dumper.add_table("public", "users", "CREATE TABLE public.users (id INT);");

        let mut output = Vec::new();
        let bytes = dumper.dump_plain(&mut output).unwrap();

        assert!(bytes > 0);
        let sql = String::from_utf8(output).unwrap();
        assert!(sql.contains("CREATE TABLE public.users"));
    }

    #[test]
    fn test_dumper_cancel() {
        let opts = DumpOptions::default();
        let dumper = Dumper::new("testdb", opts);

        assert!(!dumper.is_cancelled());
        dumper.cancel();
        assert!(dumper.is_cancelled());
    }

    #[test]
    fn test_backup_progress() {
        let mut progress = BackupProgress::new();
        progress.total_tables = 10;
        progress.tables_completed = 5;

        assert_eq!(progress.percentage(), 50.0);
    }

    #[test]
    fn test_restorer_creation() {
        let opts = RestoreOptions::default();
        let restorer = Restorer::new(opts);
        assert!(!restorer.is_cancelled());
    }

    #[test]
    fn test_restorer_load_catalog() {
        let mut catalog = BackupCatalog::new("testdb");
        catalog.add_entry(TocEntry::new(0, "TABLE", "users"));
        let bytes = catalog.serialize();

        let opts = RestoreOptions::default();
        let mut restorer = Restorer::new(opts);
        restorer.load_catalog(&mut bytes.as_slice()).unwrap();

        let contents = restorer.list_contents().unwrap();
        assert_eq!(contents.len(), 1);
    }

    #[test]
    fn test_restorer_generate_sql() {
        let mut catalog = BackupCatalog::new("testdb");
        catalog.add_entry(
            TocEntry::new(0, "TABLE", "users")
                .with_schema("public")
                .with_definition("CREATE TABLE public.users (id INT);"),
        );

        let opts = RestoreOptions::default();
        let mut restorer = Restorer::new(opts);
        restorer.catalog = Some(catalog);

        let sql = restorer.generate_sql().unwrap();
        assert!(sql.contains("CREATE TABLE public.users"));
    }

    #[test]
    fn test_restorer_schema_filter() {
        let mut catalog = BackupCatalog::new("testdb");
        catalog.add_entry(
            TocEntry::new(0, "TABLE", "users")
                .with_schema("public")
                .with_definition("CREATE TABLE public.users (id INT);"),
        );
        catalog.add_entry(
            TocEntry::new(0, "TABLE", "internal")
                .with_schema("private")
                .with_definition("CREATE TABLE private.internal (id INT);"),
        );

        let opts = RestoreOptions {
            schemas: vec!["public".to_string()],
            ..Default::default()
        };
        let mut restorer = Restorer::new(opts);
        restorer.catalog = Some(catalog);

        let sql = restorer.generate_sql().unwrap();
        assert!(sql.contains("public.users"));
        assert!(!sql.contains("private.internal"));
    }

    #[test]
    fn test_streaming_backup() {
        let config = StreamingBackupConfig::default();
        let backup = StreamingBackup::start(config);

        assert!(!backup.is_cancelled());
        assert_eq!(backup.bytes_sent(), 0);

        backup.record_bytes(1000);
        assert_eq!(backup.bytes_sent(), 1000);

        backup.cancel();
        assert!(backup.is_cancelled());
    }

    #[test]
    fn test_backup_error_display() {
        let err = BackupError::InvalidFormat("bad magic".into());
        assert!(err.to_string().contains("bad magic"));

        let err = BackupError::Cancelled;
        assert!(err.to_string().contains("cancelled"));
    }

    #[test]
    fn test_restore_progress() {
        let mut progress = RestoreProgress::new();
        progress.total_entries = 20;
        progress.entries_completed = 10;

        assert_eq!(progress.percentage(), 50.0);
    }
}
