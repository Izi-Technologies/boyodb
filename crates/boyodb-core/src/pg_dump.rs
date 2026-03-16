//! Logical Backup and Restore (pg_dump equivalent)
//!
//! This module implements logical backup capabilities similar to pg_dump,
//! allowing schema and data export in SQL or custom formats.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum DumpError {
    /// IO error
    IoError(String),
    /// Database error
    DatabaseError(String),
    /// Serialization error
    SerializationError(String),
    /// Object not found
    ObjectNotFound(String),
    /// Invalid format
    InvalidFormat(String),
    /// Restore error
    RestoreError(String),
}

impl fmt::Display for DumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DumpError::IoError(msg) => write!(f, "IO error: {}", msg),
            DumpError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            DumpError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            DumpError::ObjectNotFound(name) => write!(f, "Object not found: {}", name),
            DumpError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            DumpError::RestoreError(msg) => write!(f, "Restore error: {}", msg),
        }
    }
}

impl std::error::Error for DumpError {}

// ============================================================================
// Dump Format
// ============================================================================

/// Output format for dump
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpFormat {
    /// Plain SQL text
    PlainSql,
    /// Custom archive format (compressed)
    Custom,
    /// Directory format (parallel dump)
    Directory,
    /// TAR archive
    Tar,
}

impl Default for DumpFormat {
    fn default() -> Self {
        DumpFormat::PlainSql
    }
}

// ============================================================================
// Dump Options
// ============================================================================

/// Options for dump operation
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Output format
    pub format: DumpFormat,
    /// Include schema
    pub schema_only: bool,
    /// Include data
    pub data_only: bool,
    /// Schemas to include (empty = all)
    pub schemas: Vec<String>,
    /// Tables to include (empty = all)
    pub tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// Include CREATE DATABASE
    pub create_database: bool,
    /// Clean (drop) before create
    pub clean: bool,
    /// Include IF EXISTS in DROP
    pub if_exists: bool,
    /// Include privileges (GRANT/REVOKE)
    pub no_privileges: bool,
    /// Include ownership
    pub no_owner: bool,
    /// Include tablespace assignments
    pub no_tablespaces: bool,
    /// Use INSERT instead of COPY
    pub inserts: bool,
    /// Column-qualified INSERTs
    pub column_inserts: bool,
    /// Rows per INSERT (for batching)
    pub rows_per_insert: usize,
    /// Compression level (0-9)
    pub compression: u8,
    /// Number of parallel jobs
    pub jobs: usize,
    /// Verbose output
    pub verbose: bool,
    /// Quote identifiers
    pub quote_all_identifiers: bool,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self {
            format: DumpFormat::PlainSql,
            schema_only: false,
            data_only: false,
            schemas: Vec::new(),
            tables: Vec::new(),
            exclude_tables: Vec::new(),
            create_database: false,
            clean: false,
            if_exists: false,
            no_privileges: false,
            no_owner: false,
            no_tablespaces: false,
            inserts: false,
            column_inserts: false,
            rows_per_insert: 1000,
            compression: 6,
            jobs: 1,
            verbose: false,
            quote_all_identifiers: false,
        }
    }
}

// ============================================================================
// Dump Objects
// ============================================================================

/// Type of database object
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Schema,
    Extension,
    Type,
    Domain,
    Sequence,
    Table,
    View,
    MaterializedView,
    Index,
    Constraint,
    Trigger,
    Function,
    Procedure,
    Aggregate,
    Operator,
    Cast,
    Collation,
    TextSearchConfig,
    TextSearchDictionary,
    ForeignDataWrapper,
    ForeignServer,
    ForeignTable,
    Policy,
    Publication,
    Subscription,
    Rule,
    Comment,
    SecurityLabel,
    DefaultPrivilege,
    EventTrigger,
}

impl ObjectType {
    /// Get dump order (lower = earlier)
    pub fn dump_order(&self) -> u32 {
        match self {
            ObjectType::Extension => 1,
            ObjectType::Schema => 2,
            ObjectType::Type => 3,
            ObjectType::Domain => 4,
            ObjectType::Collation => 5,
            ObjectType::Sequence => 10,
            ObjectType::Table => 20,
            ObjectType::View => 30,
            ObjectType::MaterializedView => 31,
            ObjectType::Index => 40,
            ObjectType::Constraint => 50,
            ObjectType::Trigger => 60,
            ObjectType::Function => 70,
            ObjectType::Procedure => 71,
            ObjectType::Aggregate => 72,
            ObjectType::Operator => 73,
            ObjectType::Cast => 74,
            ObjectType::Rule => 80,
            ObjectType::Policy => 90,
            _ => 100,
        }
    }
}

/// A database object to dump
#[derive(Debug, Clone)]
pub struct DumpObject {
    /// Object type
    pub object_type: ObjectType,
    /// Schema name
    pub schema: String,
    /// Object name
    pub name: String,
    /// Full definition
    pub definition: String,
    /// Dependencies
    pub dependencies: Vec<String>,
    /// Owner
    pub owner: Option<String>,
    /// Privileges
    pub privileges: Vec<String>,
    /// Comments
    pub comments: Vec<String>,
}

impl DumpObject {
    pub fn new(object_type: ObjectType, schema: &str, name: &str, definition: &str) -> Self {
        Self {
            object_type,
            schema: schema.to_string(),
            name: name.to_string(),
            definition: definition.to_string(),
            dependencies: Vec::new(),
            owner: None,
            privileges: Vec::new(),
            comments: Vec::new(),
        }
    }

    /// Get fully qualified name
    pub fn qualified_name(&self) -> String {
        if self.schema.is_empty() || self.schema == "public" {
            self.name.clone()
        } else {
            format!("{}.{}", self.schema, self.name)
        }
    }

    /// Add dependency
    pub fn with_dependency(mut self, dep: &str) -> Self {
        self.dependencies.push(dep.to_string());
        self
    }

    /// Set owner
    pub fn with_owner(mut self, owner: &str) -> Self {
        self.owner = Some(owner.to_string());
        self
    }
}

/// Table data for dump
#[derive(Debug, Clone)]
pub struct TableData {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Column names
    pub columns: Vec<String>,
    /// Column types
    pub column_types: Vec<String>,
    /// Rows (as string values)
    pub rows: Vec<Vec<String>>,
}

impl TableData {
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: schema.to_string(),
            table: table.to_string(),
            columns: Vec::new(),
            column_types: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn add_column(&mut self, name: &str, col_type: &str) {
        self.columns.push(name.to_string());
        self.column_types.push(col_type.to_string());
    }

    pub fn add_row(&mut self, values: Vec<String>) {
        self.rows.push(values);
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

// ============================================================================
// Dump Result
// ============================================================================

/// Result of a dump operation
#[derive(Debug, Clone)]
pub struct DumpResult {
    /// Database name
    pub database: String,
    /// Number of objects dumped
    pub objects_dumped: usize,
    /// Number of rows dumped
    pub rows_dumped: u64,
    /// Total size in bytes
    pub size_bytes: u64,
    /// Duration
    pub duration: Duration,
    /// Warnings
    pub warnings: Vec<String>,
}

// ============================================================================
// SQL Dumper
// ============================================================================

/// Generates SQL dump output
pub struct SqlDumper {
    /// Dump options
    options: DumpOptions,
    /// Output buffer
    output: Vec<String>,
    /// Statistics
    stats: DumpStats,
}

/// Statistics for dump operation
#[derive(Debug, Default)]
pub struct DumpStats {
    pub schemas_dumped: AtomicU64,
    pub tables_dumped: AtomicU64,
    pub views_dumped: AtomicU64,
    pub indexes_dumped: AtomicU64,
    pub functions_dumped: AtomicU64,
    pub rows_dumped: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl SqlDumper {
    pub fn new(options: DumpOptions) -> Self {
        Self {
            options,
            output: Vec::new(),
            stats: DumpStats::default(),
        }
    }

    /// Generate header
    pub fn write_header(&mut self, database: &str) {
        self.output.push("--".to_string());
        self.output
            .push(format!("-- BoyoDB database dump - {}", chrono_now()));
        self.output.push("--".to_string());
        self.output.push(String::new());

        if self.options.create_database {
            if self.options.clean {
                if self.options.if_exists {
                    self.output
                        .push(format!("DROP DATABASE IF EXISTS {};", self.quote(database)));
                } else {
                    self.output
                        .push(format!("DROP DATABASE {};", self.quote(database)));
                }
            }
            self.output
                .push(format!("CREATE DATABASE {};", self.quote(database)));
            self.output.push(format!("\\connect {}", database));
        }

        self.output.push(String::new());
        self.output.push("SET statement_timeout = 0;".to_string());
        self.output.push("SET lock_timeout = 0;".to_string());
        self.output
            .push("SET client_encoding = 'UTF8';".to_string());
        self.output
            .push("SET standard_conforming_strings = on;".to_string());
        self.output.push(String::new());
    }

    /// Dump schema definition
    pub fn dump_schema(&mut self, obj: &DumpObject) {
        if self.options.data_only {
            return;
        }

        self.stats.schemas_dumped.fetch_add(1, Ordering::Relaxed);

        if self.options.clean {
            let drop_stmt = if self.options.if_exists {
                format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.quote(&obj.name))
            } else {
                format!("DROP SCHEMA {} CASCADE;", self.quote(&obj.name))
            };
            self.output.push(drop_stmt);
        }

        self.output.push(obj.definition.clone());

        if !self.options.no_owner {
            if let Some(owner) = &obj.owner {
                self.output.push(format!(
                    "ALTER SCHEMA {} OWNER TO {};",
                    self.quote(&obj.name),
                    self.quote(owner)
                ));
            }
        }

        self.output.push(String::new());
    }

    /// Dump table definition
    pub fn dump_table(&mut self, obj: &DumpObject) {
        if self.options.data_only {
            return;
        }

        self.stats.tables_dumped.fetch_add(1, Ordering::Relaxed);

        if self.options.clean {
            let drop_stmt = if self.options.if_exists {
                format!("DROP TABLE IF EXISTS {} CASCADE;", obj.qualified_name())
            } else {
                format!("DROP TABLE {} CASCADE;", obj.qualified_name())
            };
            self.output.push(drop_stmt);
        }

        self.output.push(obj.definition.clone());

        if !self.options.no_owner {
            if let Some(owner) = &obj.owner {
                self.output.push(format!(
                    "ALTER TABLE {} OWNER TO {};",
                    obj.qualified_name(),
                    self.quote(owner)
                ));
            }
        }

        // Comments
        for comment in &obj.comments {
            self.output.push(comment.clone());
        }

        // Privileges
        if !self.options.no_privileges {
            for priv_stmt in &obj.privileges {
                self.output.push(priv_stmt.clone());
            }
        }

        self.output.push(String::new());
    }

    /// Dump table data
    pub fn dump_data(&mut self, data: &TableData) {
        if self.options.schema_only {
            return;
        }

        if data.rows.is_empty() {
            return;
        }

        let full_name = if data.schema.is_empty() || data.schema == "public" {
            data.table.clone()
        } else {
            format!("{}.{}", data.schema, data.table)
        };

        if self.options.inserts || self.options.column_inserts {
            // Use INSERT statements
            let mut batch = Vec::new();

            for row in &data.rows {
                let values = row
                    .iter()
                    .map(|v| self.escape_value(v))
                    .collect::<Vec<_>>()
                    .join(", ");

                let insert = if self.options.column_inserts {
                    let cols = data
                        .columns
                        .iter()
                        .map(|c| self.quote(c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("INSERT INTO {} ({}) VALUES ({});", full_name, cols, values)
                } else {
                    format!("INSERT INTO {} VALUES ({});", full_name, values)
                };

                batch.push(insert);
                self.stats.rows_dumped.fetch_add(1, Ordering::Relaxed);

                if batch.len() >= self.options.rows_per_insert {
                    self.output.extend(batch.drain(..));
                }
            }

            if !batch.is_empty() {
                self.output.extend(batch);
            }
        } else {
            // Use COPY format
            self.output.push(format!(
                "COPY {} ({}) FROM stdin;",
                full_name,
                data.columns
                    .iter()
                    .map(|c| self.quote(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));

            for row in &data.rows {
                let line = row
                    .iter()
                    .map(|v| self.escape_copy_value(v))
                    .collect::<Vec<_>>()
                    .join("\t");
                self.output.push(line);
                self.stats.rows_dumped.fetch_add(1, Ordering::Relaxed);
            }

            self.output.push("\\.".to_string());
        }

        self.output.push(String::new());
    }

    /// Dump view
    pub fn dump_view(&mut self, obj: &DumpObject) {
        if self.options.data_only {
            return;
        }

        self.stats.views_dumped.fetch_add(1, Ordering::Relaxed);

        if self.options.clean {
            let drop_stmt = if self.options.if_exists {
                format!("DROP VIEW IF EXISTS {} CASCADE;", obj.qualified_name())
            } else {
                format!("DROP VIEW {} CASCADE;", obj.qualified_name())
            };
            self.output.push(drop_stmt);
        }

        self.output.push(obj.definition.clone());
        self.output.push(String::new());
    }

    /// Dump index
    pub fn dump_index(&mut self, obj: &DumpObject) {
        if self.options.data_only {
            return;
        }

        self.stats.indexes_dumped.fetch_add(1, Ordering::Relaxed);
        self.output.push(obj.definition.clone());
        self.output.push(String::new());
    }

    /// Dump function
    pub fn dump_function(&mut self, obj: &DumpObject) {
        if self.options.data_only {
            return;
        }

        self.stats.functions_dumped.fetch_add(1, Ordering::Relaxed);

        if self.options.clean {
            let drop_stmt = if self.options.if_exists {
                format!("DROP FUNCTION IF EXISTS {} CASCADE;", obj.qualified_name())
            } else {
                format!("DROP FUNCTION {} CASCADE;", obj.qualified_name())
            };
            self.output.push(drop_stmt);
        }

        self.output.push(obj.definition.clone());
        self.output.push(String::new());
    }

    /// Write footer
    pub fn write_footer(&mut self) {
        self.output.push(String::new());
        self.output.push("--".to_string());
        self.output.push("-- Dump completed".to_string());
        self.output.push("--".to_string());
    }

    /// Get the generated SQL
    pub fn get_output(&self) -> String {
        self.output.join("\n")
    }

    /// Get output lines
    pub fn get_lines(&self) -> &[String] {
        &self.output
    }

    /// Quote identifier
    fn quote(&self, name: &str) -> String {
        if self.options.quote_all_identifiers || needs_quoting(name) {
            format!("\"{}\"", name.replace('"', "\"\""))
        } else {
            name.to_string()
        }
    }

    /// Escape value for INSERT
    fn escape_value(&self, value: &str) -> String {
        if value == "NULL" || value == "null" {
            "NULL".to_string()
        } else if value
            .chars()
            .all(|c| c.is_numeric() || c == '-' || c == '.')
        {
            value.to_string()
        } else {
            format!("'{}'", value.replace('\'', "''"))
        }
    }

    /// Escape value for COPY
    fn escape_copy_value(&self, value: &str) -> String {
        if value == "NULL" || value == "null" {
            "\\N".to_string()
        } else {
            value
                .replace('\\', "\\\\")
                .replace('\t', "\\t")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        }
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> DumpStatsSnapshot {
        DumpStatsSnapshot {
            schemas_dumped: self.stats.schemas_dumped.load(Ordering::Relaxed),
            tables_dumped: self.stats.tables_dumped.load(Ordering::Relaxed),
            views_dumped: self.stats.views_dumped.load(Ordering::Relaxed),
            indexes_dumped: self.stats.indexes_dumped.load(Ordering::Relaxed),
            functions_dumped: self.stats.functions_dumped.load(Ordering::Relaxed),
            rows_dumped: self.stats.rows_dumped.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of dump statistics
#[derive(Debug, Clone)]
pub struct DumpStatsSnapshot {
    pub schemas_dumped: u64,
    pub tables_dumped: u64,
    pub views_dumped: u64,
    pub indexes_dumped: u64,
    pub functions_dumped: u64,
    pub rows_dumped: u64,
    pub bytes_written: u64,
}

// ============================================================================
// Restore Options
// ============================================================================

/// Options for restore operation
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    /// Input format (auto-detect if None)
    pub format: Option<DumpFormat>,
    /// Create the database
    pub create: bool,
    /// Clean (drop) before restore
    pub clean: bool,
    /// Use single transaction
    pub single_transaction: bool,
    /// Exit on error
    pub exit_on_error: bool,
    /// Number of parallel jobs
    pub jobs: usize,
    /// Tables to restore (empty = all)
    pub tables: Vec<String>,
    /// Schemas to restore (empty = all)
    pub schemas: Vec<String>,
    /// Skip data
    pub schema_only: bool,
    /// Skip schema
    pub data_only: bool,
    /// Verbose output
    pub verbose: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            format: None,
            create: false,
            clean: false,
            single_transaction: true,
            exit_on_error: false,
            jobs: 1,
            tables: Vec::new(),
            schemas: Vec::new(),
            schema_only: false,
            data_only: false,
            verbose: false,
        }
    }
}

// ============================================================================
// SQL Restorer
// ============================================================================

/// Restores from SQL dump
pub struct SqlRestorer {
    options: RestoreOptions,
    statements_executed: AtomicU64,
    rows_restored: AtomicU64,
    errors: Vec<String>,
}

impl SqlRestorer {
    pub fn new(options: RestoreOptions) -> Self {
        Self {
            options,
            statements_executed: AtomicU64::new(0),
            rows_restored: AtomicU64::new(0),
            errors: Vec::new(),
        }
    }

    /// Parse SQL dump into statements
    pub fn parse_sql(&self, sql: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut current = String::new();
        let mut in_string = false;
        let mut in_dollar_quote = false;
        let mut in_copy_data = false;
        let mut dollar_tag = String::new();

        for line in sql.lines() {
            let trimmed = line.trim();

            // Skip comments
            if trimmed.starts_with("--") {
                continue;
            }

            // Handle COPY data terminator
            if in_copy_data && trimmed == "\\." {
                current.push('\n');
                current.push_str(line);
                statements.push(current.clone());
                current.clear();
                in_copy_data = false;
                continue;
            }

            // Check for dollar quoting
            if !in_string && !in_dollar_quote && !in_copy_data {
                if line.contains("$$") {
                    in_dollar_quote = true;
                    dollar_tag = "$$".to_string();
                }
            } else if in_dollar_quote && line.contains(&dollar_tag) {
                in_dollar_quote = false;
            }

            // Append line to current statement
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);

            // Check if this is a COPY ... FROM stdin statement
            if !in_copy_data && !in_dollar_quote && !in_string {
                let upper = trimmed.to_uppercase();
                if upper.starts_with("COPY ")
                    && upper.contains(" FROM STDIN")
                    && trimmed.ends_with(';')
                {
                    in_copy_data = true;
                    continue; // Don't push yet, wait for data and \.
                }
            }

            // Check for statement end (not in string, dollar quote, or COPY data)
            if !in_dollar_quote && !in_string && !in_copy_data && trimmed.ends_with(';') {
                statements.push(current.clone());
                current.clear();
            }
        }

        // Add any remaining content
        if !current.trim().is_empty() {
            statements.push(current);
        }

        statements
    }

    /// Execute a statement (returns success)
    pub fn execute_statement<F>(&self, sql: &str, executor: &F) -> Result<(), DumpError>
    where
        F: Fn(&str) -> Result<u64, String>,
    {
        let trimmed = sql.trim();

        // Skip empty statements and psql commands
        if trimmed.is_empty() || trimmed.starts_with('\\') {
            return Ok(());
        }

        match executor(sql) {
            Ok(rows) => {
                self.statements_executed.fetch_add(1, Ordering::Relaxed);
                self.rows_restored.fetch_add(rows, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                if self.options.exit_on_error {
                    Err(DumpError::RestoreError(e))
                } else {
                    // Log error but continue
                    Ok(())
                }
            }
        }
    }

    /// Get restore statistics
    pub fn stats(&self) -> RestoreStats {
        RestoreStats {
            statements_executed: self.statements_executed.load(Ordering::Relaxed),
            rows_restored: self.rows_restored.load(Ordering::Relaxed),
            errors: self.errors.len() as u64,
        }
    }
}

/// Restore statistics
#[derive(Debug, Clone)]
pub struct RestoreStats {
    pub statements_executed: u64,
    pub rows_restored: u64,
    pub errors: u64,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if identifier needs quoting
fn needs_quoting(name: &str) -> bool {
    let keywords = [
        "user", "table", "index", "order", "group", "select", "from", "where",
    ];

    if name.is_empty() {
        return true;
    }

    // First char must be letter or underscore
    let first = name.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return true;
    }

    // Rest must be alphanumeric or underscore
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return true;
    }

    // Check reserved keywords
    if keywords.contains(&name.to_lowercase().as_str()) {
        return true;
    }

    // Check for uppercase (PostgreSQL convention)
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        return true;
    }

    false
}

/// Get current timestamp string
fn chrono_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{}", now)
}

// ============================================================================
// Dump Manager
// ============================================================================

/// Manages dump and restore operations
pub struct DumpManager {
    /// Default options
    default_options: DumpOptions,
}

impl DumpManager {
    pub fn new() -> Self {
        Self {
            default_options: DumpOptions::default(),
        }
    }

    /// Create a new dumper
    pub fn create_dumper(&self, options: Option<DumpOptions>) -> SqlDumper {
        SqlDumper::new(options.unwrap_or_else(|| self.default_options.clone()))
    }

    /// Create a new restorer
    pub fn create_restorer(&self, options: Option<RestoreOptions>) -> SqlRestorer {
        SqlRestorer::new(options.unwrap_or_default())
    }

    /// Set default options
    pub fn set_default_options(&mut self, options: DumpOptions) {
        self.default_options = options;
    }
}

impl Default for DumpManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dump_format() {
        assert_eq!(DumpFormat::default(), DumpFormat::PlainSql);
    }

    #[test]
    fn test_dump_object() {
        let obj = DumpObject::new(
            ObjectType::Table,
            "public",
            "users",
            "CREATE TABLE users ();",
        )
        .with_owner("admin")
        .with_dependency("other_table");

        assert_eq!(obj.qualified_name(), "users");
        assert_eq!(obj.owner, Some("admin".to_string()));
        assert_eq!(obj.dependencies.len(), 1);
    }

    #[test]
    fn test_dump_object_qualified_name() {
        let obj = DumpObject::new(ObjectType::Table, "myschema", "users", "");
        assert_eq!(obj.qualified_name(), "myschema.users");

        let obj = DumpObject::new(ObjectType::Table, "public", "users", "");
        assert_eq!(obj.qualified_name(), "users");
    }

    #[test]
    fn test_table_data() {
        let mut data = TableData::new("public", "users");
        data.add_column("id", "integer");
        data.add_column("name", "text");
        data.add_row(vec!["1".to_string(), "Alice".to_string()]);
        data.add_row(vec!["2".to_string(), "Bob".to_string()]);

        assert_eq!(data.columns.len(), 2);
        assert_eq!(data.row_count(), 2);
    }

    #[test]
    fn test_sql_dumper_header() {
        let mut dumper = SqlDumper::new(DumpOptions::default());
        dumper.write_header("testdb");

        let output = dumper.get_output();
        assert!(output.contains("BoyoDB database dump"));
    }

    #[test]
    fn test_sql_dumper_schema() {
        let mut dumper = SqlDumper::new(DumpOptions {
            clean: true,
            if_exists: true,
            ..Default::default()
        });

        let obj = DumpObject::new(
            ObjectType::Schema,
            "",
            "myschema",
            "CREATE SCHEMA myschema;",
        )
        .with_owner("admin");

        dumper.dump_schema(&obj);

        let output = dumper.get_output();
        assert!(output.contains("DROP SCHEMA IF EXISTS"));
        assert!(output.contains("CREATE SCHEMA"));
        assert!(output.contains("OWNER TO"));
    }

    #[test]
    fn test_sql_dumper_table() {
        let mut dumper = SqlDumper::new(DumpOptions::default());

        let obj = DumpObject::new(
            ObjectType::Table,
            "public",
            "users",
            "CREATE TABLE users (id INTEGER, name TEXT);",
        );

        dumper.dump_table(&obj);

        let output = dumper.get_output();
        assert!(output.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_sql_dumper_data_copy() {
        let mut dumper = SqlDumper::new(DumpOptions::default());

        let mut data = TableData::new("public", "users");
        data.add_column("id", "integer");
        data.add_column("name", "text");
        data.add_row(vec!["1".to_string(), "Alice".to_string()]);

        dumper.dump_data(&data);

        let output = dumper.get_output();
        assert!(output.contains("COPY"));
        assert!(output.contains("1\tAlice"));
        assert!(output.contains("\\."));
    }

    #[test]
    fn test_sql_dumper_data_inserts() {
        let mut dumper = SqlDumper::new(DumpOptions {
            inserts: true,
            ..Default::default()
        });

        let mut data = TableData::new("public", "users");
        data.add_column("id", "integer");
        data.add_column("name", "text");
        data.add_row(vec!["1".to_string(), "Alice".to_string()]);

        dumper.dump_data(&data);

        let output = dumper.get_output();
        assert!(output.contains("INSERT INTO users VALUES"));
    }

    #[test]
    fn test_sql_dumper_column_inserts() {
        let mut dumper = SqlDumper::new(DumpOptions {
            inserts: true,
            column_inserts: true,
            ..Default::default()
        });

        let mut data = TableData::new("public", "users");
        data.add_column("id", "integer");
        data.add_column("name", "text");
        data.add_row(vec!["1".to_string(), "Alice".to_string()]);

        dumper.dump_data(&data);

        let output = dumper.get_output();
        assert!(output.contains("INSERT INTO users (id, name) VALUES"));
    }

    #[test]
    fn test_needs_quoting() {
        assert!(!needs_quoting("simple"));
        assert!(!needs_quoting("with_underscore"));
        assert!(needs_quoting("user")); // keyword
        assert!(needs_quoting("123start")); // starts with number
        assert!(needs_quoting("has space"));
        assert!(needs_quoting("MixedCase"));
    }

    #[test]
    fn test_object_type_order() {
        assert!(ObjectType::Schema.dump_order() < ObjectType::Table.dump_order());
        assert!(ObjectType::Table.dump_order() < ObjectType::Index.dump_order());
        assert!(ObjectType::Index.dump_order() < ObjectType::Trigger.dump_order());
    }

    #[test]
    fn test_sql_restorer_parse() {
        let restorer = SqlRestorer::new(RestoreOptions::default());

        let sql = r#"
-- Comment
CREATE TABLE users (id INT);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
"#;

        let statements = restorer.parse_sql(sql);
        assert_eq!(statements.len(), 3);
    }

    #[test]
    fn test_sql_restorer_parse_copy() {
        let restorer = SqlRestorer::new(RestoreOptions::default());

        let sql = r#"
COPY users FROM stdin;
1	Alice
2	Bob
\.
"#;

        let statements = restorer.parse_sql(sql);
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("COPY"));
    }

    #[test]
    fn test_dump_manager() {
        let manager = DumpManager::new();
        let dumper = manager.create_dumper(None);

        assert!(dumper.get_lines().is_empty());
    }

    #[test]
    fn test_error_display() {
        let err = DumpError::IoError("test".to_string());
        assert!(format!("{}", err).contains("test"));

        let err = DumpError::ObjectNotFound("users".to_string());
        assert!(format!("{}", err).contains("users"));
    }

    #[test]
    fn test_dump_stats() {
        let dumper = SqlDumper::new(DumpOptions::default());

        let stats = dumper.stats();
        assert_eq!(stats.schemas_dumped, 0);
        assert_eq!(stats.tables_dumped, 0);
    }

    #[test]
    fn test_restore_stats() {
        let restorer = SqlRestorer::new(RestoreOptions::default());

        let stats = restorer.stats();
        assert_eq!(stats.statements_executed, 0);
        assert_eq!(stats.rows_restored, 0);
    }

    #[test]
    fn test_schema_only_option() {
        let mut dumper = SqlDumper::new(DumpOptions {
            schema_only: true,
            ..Default::default()
        });

        let mut data = TableData::new("public", "users");
        data.add_row(vec!["1".to_string()]);

        dumper.dump_data(&data);

        // Should not output any data
        assert!(dumper.get_output().is_empty());
    }

    #[test]
    fn test_data_only_option() {
        let mut dumper = SqlDumper::new(DumpOptions {
            data_only: true,
            ..Default::default()
        });

        let obj = DumpObject::new(
            ObjectType::Table,
            "public",
            "users",
            "CREATE TABLE users ();",
        );
        dumper.dump_table(&obj);

        // Should not output schema
        let output = dumper.get_output();
        assert!(!output.contains("CREATE TABLE"));
    }

    #[test]
    fn test_escape_value() {
        let dumper = SqlDumper::new(DumpOptions::default());

        assert_eq!(dumper.escape_value("NULL"), "NULL");
        assert_eq!(dumper.escape_value("123"), "123");
        assert_eq!(dumper.escape_value("test"), "'test'");
        assert_eq!(dumper.escape_value("it's"), "'it''s'");
    }

    #[test]
    fn test_escape_copy_value() {
        let dumper = SqlDumper::new(DumpOptions::default());

        assert_eq!(dumper.escape_copy_value("NULL"), "\\N");
        assert_eq!(dumper.escape_copy_value("test"), "test");
        assert_eq!(dumper.escape_copy_value("tab\there"), "tab\\there");
        assert_eq!(dumper.escape_copy_value("new\nline"), "new\\nline");
    }
}
