//! Phase 17: Tooling
//!
//! Provides command-line tooling capabilities:
//! - Standalone query execution without server
//! - Import/export utilities for various formats
//! - Data conversion between formats
//! - Schema inspection and migration tools
//! - Bulk data operations

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use parking_lot::RwLock;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Error, Debug)]
pub enum ToolingError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Format error: {0}")]
    Format(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("Import error: {0}")]
    Import(String),

    #[error("Export error: {0}")]
    Export(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

pub type ToolingResult<T> = Result<T, ToolingError>;

// ============================================================================
// Data Format Support
// ============================================================================

/// Supported data formats for import/export
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataFormat {
    /// Comma-separated values
    Csv,
    /// Tab-separated values
    Tsv,
    /// JSON lines (one JSON object per line)
    JsonLines,
    /// JSON array
    JsonArray,
    /// Apache Parquet
    Parquet,
    /// Apache ORC
    Orc,
    /// Apache Avro
    Avro,
    /// Native binary format
    Native,
    /// SQL INSERT statements
    SqlInsert,
    /// Pretty printed table
    Pretty,
    /// Vertical format (one column per line)
    Vertical,
    /// Raw values (no formatting)
    Raw,
}

impl DataFormat {
    /// Get file extension for format
    pub fn extension(&self) -> &'static str {
        match self {
            DataFormat::Csv => "csv",
            DataFormat::Tsv => "tsv",
            DataFormat::JsonLines => "jsonl",
            DataFormat::JsonArray => "json",
            DataFormat::Parquet => "parquet",
            DataFormat::Orc => "orc",
            DataFormat::Avro => "avro",
            DataFormat::Native => "bin",
            DataFormat::SqlInsert => "sql",
            DataFormat::Pretty => "txt",
            DataFormat::Vertical => "txt",
            DataFormat::Raw => "txt",
        }
    }

    /// Detect format from file extension
    pub fn from_extension(ext: &str) -> Option<DataFormat> {
        match ext.to_lowercase().as_str() {
            "csv" => Some(DataFormat::Csv),
            "tsv" | "tab" => Some(DataFormat::Tsv),
            "jsonl" | "ndjson" => Some(DataFormat::JsonLines),
            "json" => Some(DataFormat::JsonArray),
            "parquet" | "pq" => Some(DataFormat::Parquet),
            "orc" => Some(DataFormat::Orc),
            "avro" => Some(DataFormat::Avro),
            "bin" | "native" => Some(DataFormat::Native),
            "sql" => Some(DataFormat::SqlInsert),
            _ => None,
        }
    }

    /// Check if format is text-based
    pub fn is_text(&self) -> bool {
        matches!(self,
            DataFormat::Csv | DataFormat::Tsv | DataFormat::JsonLines |
            DataFormat::JsonArray | DataFormat::SqlInsert | DataFormat::Pretty |
            DataFormat::Vertical | DataFormat::Raw
        )
    }

    /// Check if format supports streaming
    pub fn supports_streaming(&self) -> bool {
        matches!(self,
            DataFormat::Csv | DataFormat::Tsv | DataFormat::JsonLines |
            DataFormat::Native | DataFormat::Raw
        )
    }
}

// ============================================================================
// Format Options
// ============================================================================

/// CSV format options
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Field delimiter
    pub delimiter: char,
    /// Quote character
    pub quote: char,
    /// Escape character
    pub escape: Option<char>,
    /// Whether first row is header
    pub has_header: bool,
    /// Null value representation
    pub null_value: String,
    /// Skip empty lines
    pub skip_empty_lines: bool,
    /// Maximum fields per row (0 = unlimited)
    pub max_fields: usize,
    /// Encoding
    pub encoding: String,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            quote: '"',
            escape: Some('\\'),
            has_header: true,
            null_value: "".to_string(),
            skip_empty_lines: true,
            max_fields: 0,
            encoding: "utf-8".to_string(),
        }
    }
}

impl CsvOptions {
    /// Create TSV options
    pub fn tsv() -> Self {
        Self {
            delimiter: '\t',
            ..Default::default()
        }
    }
}

/// JSON format options
#[derive(Debug, Clone)]
pub struct JsonOptions {
    /// Pretty print output
    pub pretty: bool,
    /// Indent string (for pretty print)
    pub indent: String,
    /// Date format
    pub date_format: String,
    /// DateTime format
    pub datetime_format: String,
    /// Output as array vs object per line
    pub as_array: bool,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            pretty: false,
            indent: "  ".to_string(),
            date_format: "%Y-%m-%d".to_string(),
            datetime_format: "%Y-%m-%d %H:%M:%S".to_string(),
            as_array: false,
        }
    }
}

/// Parquet format options
#[derive(Debug, Clone)]
pub struct ParquetOptions {
    /// Compression codec
    pub compression: ParquetCompression,
    /// Row group size
    pub row_group_size: usize,
    /// Page size
    pub page_size: usize,
    /// Enable dictionary encoding
    pub dictionary_enabled: bool,
    /// Dictionary page size limit
    pub dictionary_page_size: usize,
    /// Enable statistics
    pub statistics_enabled: bool,
    /// Bloom filter enabled
    pub bloom_filter_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Zstd,
            row_group_size: 1_000_000,
            page_size: 1_048_576,
            dictionary_enabled: true,
            dictionary_page_size: 1_048_576,
            statistics_enabled: true,
            bloom_filter_enabled: false,
        }
    }
}

// ============================================================================
// Import/Export Configuration
// ============================================================================

/// Import configuration
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Source file or directory path
    pub source: PathBuf,
    /// Target table name
    pub table: String,
    /// Target database name
    pub database: String,
    /// Input format
    pub format: DataFormat,
    /// Format-specific options
    pub format_options: FormatOptions,
    /// Number of rows to skip
    pub skip_rows: usize,
    /// Maximum rows to import (0 = unlimited)
    pub max_rows: usize,
    /// Batch size for inserts
    pub batch_size: usize,
    /// Number of parallel threads
    pub threads: usize,
    /// Whether to create table if not exists
    pub create_table: bool,
    /// Whether to truncate table before import
    pub truncate: bool,
    /// Error handling mode
    pub on_error: ErrorHandling,
    /// Transform expressions
    pub transforms: Vec<ColumnTransform>,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            source: PathBuf::new(),
            table: String::new(),
            database: "default".to_string(),
            format: DataFormat::Csv,
            format_options: FormatOptions::Csv(CsvOptions::default()),
            skip_rows: 0,
            max_rows: 0,
            batch_size: 10_000,
            threads: 1,
            create_table: false,
            truncate: false,
            on_error: ErrorHandling::Abort,
            transforms: Vec::new(),
        }
    }
}

/// Export configuration
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Query to export
    pub query: String,
    /// Target file path
    pub target: PathBuf,
    /// Output format
    pub format: DataFormat,
    /// Format-specific options
    pub format_options: FormatOptions,
    /// Whether to overwrite existing file
    pub overwrite: bool,
    /// Maximum rows to export (0 = unlimited)
    pub max_rows: usize,
    /// Compression for output
    pub compression: Option<CompressionType>,
    /// Split output into multiple files
    pub split_size: Option<usize>,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            query: String::new(),
            target: PathBuf::new(),
            format: DataFormat::Csv,
            format_options: FormatOptions::Csv(CsvOptions::default()),
            overwrite: false,
            max_rows: 0,
            compression: None,
            split_size: None,
        }
    }
}

/// Format-specific options
#[derive(Debug, Clone)]
pub enum FormatOptions {
    Csv(CsvOptions),
    Json(JsonOptions),
    Parquet(ParquetOptions),
    None,
}

/// Error handling modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorHandling {
    /// Abort on first error
    Abort,
    /// Skip bad rows
    Skip,
    /// Replace bad values with null
    ReplaceNull,
    /// Log errors and continue
    Log,
}

/// Compression types for output
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Gzip,
    Zstd,
    Lz4,
    Snappy,
    Bzip2,
    Xz,
}

impl CompressionType {
    pub fn extension(&self) -> &'static str {
        match self {
            CompressionType::Gzip => "gz",
            CompressionType::Zstd => "zst",
            CompressionType::Lz4 => "lz4",
            CompressionType::Snappy => "snappy",
            CompressionType::Bzip2 => "bz2",
            CompressionType::Xz => "xz",
        }
    }
}

/// Column transformation
#[derive(Debug, Clone)]
pub struct ColumnTransform {
    /// Source column name
    pub source_column: String,
    /// Target column name
    pub target_column: String,
    /// Transform expression
    pub expression: String,
}

// ============================================================================
// Import/Export Results
// ============================================================================

/// Import operation result
#[derive(Debug, Clone)]
pub struct ImportResult {
    /// Total rows read
    pub rows_read: u64,
    /// Rows successfully imported
    pub rows_imported: u64,
    /// Rows skipped due to errors
    pub rows_skipped: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Errors encountered
    pub errors: Vec<ImportError>,
}

impl ImportResult {
    pub fn new() -> Self {
        Self {
            rows_read: 0,
            rows_imported: 0,
            rows_skipped: 0,
            bytes_read: 0,
            duration_ms: 0,
            errors: Vec::new(),
        }
    }

    /// Calculate import rate (rows per second)
    pub fn rows_per_second(&self) -> f64 {
        if self.duration_ms == 0 {
            0.0
        } else {
            self.rows_imported as f64 / (self.duration_ms as f64 / 1000.0)
        }
    }

    /// Calculate throughput (MB per second)
    pub fn mb_per_second(&self) -> f64 {
        if self.duration_ms == 0 {
            0.0
        } else {
            let mb = self.bytes_read as f64 / (1024.0 * 1024.0);
            mb / (self.duration_ms as f64 / 1000.0)
        }
    }
}

impl Default for ImportResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Import error details
#[derive(Debug, Clone)]
pub struct ImportError {
    /// Row number where error occurred
    pub row: u64,
    /// Column name (if applicable)
    pub column: Option<String>,
    /// Error message
    pub message: String,
    /// Raw data that caused error
    pub raw_data: Option<String>,
}

/// Export operation result
#[derive(Debug, Clone)]
pub struct ExportResult {
    /// Total rows exported
    pub rows_exported: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Files created
    pub files_created: Vec<PathBuf>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl ExportResult {
    pub fn new() -> Self {
        Self {
            rows_exported: 0,
            bytes_written: 0,
            files_created: Vec::new(),
            duration_ms: 0,
        }
    }
}

impl Default for ExportResult {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Schema Types
// ============================================================================

/// Column definition for schema
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: ColumnType,
    /// Whether column is nullable
    pub nullable: bool,
    /// Default value expression
    pub default_expr: Option<String>,
    /// Column comment
    pub comment: Option<String>,
}

/// Column data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    FixedString(usize),
    Date,
    DateTime,
    DateTime64(u8), // precision
    Bool,
    Uuid,
    Decimal(u8, u8), // precision, scale
    Array(Box<ColumnType>),
    Nullable(Box<ColumnType>),
    LowCardinality(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Tuple(Vec<ColumnType>),
}

impl ColumnType {
    /// Get type name as string
    pub fn name(&self) -> String {
        match self {
            ColumnType::Int8 => "Int8".to_string(),
            ColumnType::Int16 => "Int16".to_string(),
            ColumnType::Int32 => "Int32".to_string(),
            ColumnType::Int64 => "Int64".to_string(),
            ColumnType::UInt8 => "UInt8".to_string(),
            ColumnType::UInt16 => "UInt16".to_string(),
            ColumnType::UInt32 => "UInt32".to_string(),
            ColumnType::UInt64 => "UInt64".to_string(),
            ColumnType::Float32 => "Float32".to_string(),
            ColumnType::Float64 => "Float64".to_string(),
            ColumnType::String => "String".to_string(),
            ColumnType::FixedString(n) => format!("FixedString({})", n),
            ColumnType::Date => "Date".to_string(),
            ColumnType::DateTime => "DateTime".to_string(),
            ColumnType::DateTime64(p) => format!("DateTime64({})", p),
            ColumnType::Bool => "Bool".to_string(),
            ColumnType::Uuid => "UUID".to_string(),
            ColumnType::Decimal(p, s) => format!("Decimal({}, {})", p, s),
            ColumnType::Array(inner) => format!("Array({})", inner.name()),
            ColumnType::Nullable(inner) => format!("Nullable({})", inner.name()),
            ColumnType::LowCardinality(inner) => format!("LowCardinality({})", inner.name()),
            ColumnType::Map(k, v) => format!("Map({}, {})", k.name(), v.name()),
            ColumnType::Tuple(types) => {
                let inner: Vec<_> = types.iter().map(|t| t.name()).collect();
                format!("Tuple({})", inner.join(", "))
            }
        }
    }

    /// Parse type from string
    pub fn parse(s: &str) -> Option<ColumnType> {
        let s = s.trim();
        match s.to_lowercase().as_str() {
            "int8" | "tinyint" => Some(ColumnType::Int8),
            "int16" | "smallint" => Some(ColumnType::Int16),
            "int32" | "int" | "integer" => Some(ColumnType::Int32),
            "int64" | "bigint" => Some(ColumnType::Int64),
            "uint8" => Some(ColumnType::UInt8),
            "uint16" => Some(ColumnType::UInt16),
            "uint32" => Some(ColumnType::UInt32),
            "uint64" => Some(ColumnType::UInt64),
            "float32" | "float" => Some(ColumnType::Float32),
            "float64" | "double" => Some(ColumnType::Float64),
            "string" | "text" | "varchar" => Some(ColumnType::String),
            "date" => Some(ColumnType::Date),
            "datetime" | "timestamp" => Some(ColumnType::DateTime),
            "bool" | "boolean" => Some(ColumnType::Bool),
            "uuid" => Some(ColumnType::Uuid),
            _ => None,
        }
    }
}

/// Table schema
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Database name
    pub database: String,
    /// Columns
    pub columns: Vec<ColumnDef>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Order by columns
    pub order_by: Vec<String>,
    /// Partition by expression
    pub partition_by: Option<String>,
    /// Engine type
    pub engine: String,
    /// Engine settings
    pub settings: HashMap<String, String>,
    /// Table comment
    pub comment: Option<String>,
}

impl TableSchema {
    pub fn new(name: &str, database: &str) -> Self {
        Self {
            name: name.to_string(),
            database: database.to_string(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            order_by: Vec::new(),
            partition_by: None,
            engine: "MergeTree".to_string(),
            settings: HashMap::new(),
            comment: None,
        }
    }

    /// Add a column
    pub fn add_column(&mut self, name: &str, data_type: ColumnType, nullable: bool) {
        self.columns.push(ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            default_expr: None,
            comment: None,
        });
    }

    /// Generate CREATE TABLE statement
    pub fn to_create_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {}.{} (\n", self.database, self.name);

        for (i, col) in self.columns.iter().enumerate() {
            sql.push_str(&format!("    {} {}", col.name, col.data_type.name()));
            if !col.nullable {
                sql.push_str(" NOT NULL");
            }
            if let Some(ref default) = col.default_expr {
                sql.push_str(&format!(" DEFAULT {}", default));
            }
            if let Some(ref comment) = col.comment {
                sql.push_str(&format!(" COMMENT '{}'", comment));
            }
            if i < self.columns.len() - 1 {
                sql.push(',');
            }
            sql.push('\n');
        }

        sql.push_str(&format!(") ENGINE = {}", self.engine));

        if !self.order_by.is_empty() {
            sql.push_str(&format!("\nORDER BY ({})", self.order_by.join(", ")));
        }

        if !self.primary_key.is_empty() {
            sql.push_str(&format!("\nPRIMARY KEY ({})", self.primary_key.join(", ")));
        }

        if let Some(ref partition) = self.partition_by {
            sql.push_str(&format!("\nPARTITION BY {}", partition));
        }

        for (key, value) in &self.settings {
            sql.push_str(&format!("\nSETTINGS {} = {}", key, value));
        }

        sql
    }
}

// ============================================================================
// Schema Inference
// ============================================================================

/// Schema inference from data
pub struct SchemaInferrer {
    /// Sample size for inference
    sample_size: usize,
    /// Type coercion rules
    type_coercion: TypeCoercion,
    /// Detected column types
    detected_types: HashMap<String, Vec<ColumnType>>,
}

/// Type coercion rules
#[derive(Debug, Clone, Copy)]
pub enum TypeCoercion {
    /// Use strictest type
    Strict,
    /// Use most general type
    Loose,
    /// Default to string for ambiguous
    StringDefault,
}

impl SchemaInferrer {
    pub fn new(sample_size: usize, coercion: TypeCoercion) -> Self {
        Self {
            sample_size,
            type_coercion: coercion,
            detected_types: HashMap::new(),
        }
    }

    /// Infer type from string value
    pub fn infer_type(value: &str) -> ColumnType {
        let trimmed = value.trim();

        // Check for null/empty
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
            return ColumnType::Nullable(Box::new(ColumnType::String));
        }

        // Try boolean
        if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
            return ColumnType::Bool;
        }

        // Try integer
        if let Ok(_) = trimmed.parse::<i64>() {
            let abs = trimmed.trim_start_matches('-').parse::<u64>().unwrap_or(u64::MAX);
            if abs <= i8::MAX as u64 {
                return ColumnType::Int8;
            } else if abs <= i16::MAX as u64 {
                return ColumnType::Int16;
            } else if abs <= i32::MAX as u64 {
                return ColumnType::Int32;
            } else {
                return ColumnType::Int64;
            }
        }

        // Try float
        if let Ok(_) = trimmed.parse::<f64>() {
            return ColumnType::Float64;
        }

        // Try date (YYYY-MM-DD)
        if trimmed.len() == 10 && trimmed.chars().nth(4) == Some('-') && trimmed.chars().nth(7) == Some('-') {
            return ColumnType::Date;
        }

        // Try datetime (YYYY-MM-DD HH:MM:SS)
        if trimmed.len() >= 19 && trimmed.chars().nth(10) == Some(' ') {
            return ColumnType::DateTime;
        }

        // Try UUID
        if trimmed.len() == 36 && trimmed.chars().filter(|c| *c == '-').count() == 4 {
            return ColumnType::Uuid;
        }

        // Default to string
        ColumnType::String
    }

    /// Add sample value for column
    pub fn add_sample(&mut self, column: &str, value: &str) {
        let inferred = Self::infer_type(value);
        self.detected_types
            .entry(column.to_string())
            .or_insert_with(Vec::new)
            .push(inferred);
    }

    /// Get final schema based on samples
    pub fn finalize(&self) -> Vec<ColumnDef> {
        self.detected_types
            .iter()
            .map(|(name, types)| {
                let final_type = self.merge_types(types);
                ColumnDef {
                    name: name.clone(),
                    data_type: final_type,
                    nullable: types.iter().any(|t| matches!(t, ColumnType::Nullable(_))),
                    default_expr: None,
                    comment: None,
                }
            })
            .collect()
    }

    /// Merge detected types into final type
    fn merge_types(&self, types: &[ColumnType]) -> ColumnType {
        if types.is_empty() {
            return ColumnType::String;
        }

        match self.type_coercion {
            TypeCoercion::Strict => {
                // All types must match
                let first = &types[0];
                if types.iter().all(|t| std::mem::discriminant(t) == std::mem::discriminant(first)) {
                    first.clone()
                } else {
                    ColumnType::String
                }
            }
            TypeCoercion::Loose => {
                // Use widest numeric type
                let has_string = types.iter().any(|t| matches!(t, ColumnType::String));
                let has_float = types.iter().any(|t| matches!(t, ColumnType::Float32 | ColumnType::Float64));
                let has_int = types.iter().any(|t| matches!(t,
                    ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64));

                if has_string {
                    ColumnType::String
                } else if has_float {
                    ColumnType::Float64
                } else if has_int {
                    ColumnType::Int64
                } else {
                    types[0].clone()
                }
            }
            TypeCoercion::StringDefault => ColumnType::String,
        }
    }
}

// ============================================================================
// Data Importer
// ============================================================================

/// Data importer for bulk loading
pub struct DataImporter {
    config: ImportConfig,
    result: ImportResult,
}

impl DataImporter {
    pub fn new(config: ImportConfig) -> Self {
        Self {
            config,
            result: ImportResult::new(),
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> ToolingResult<()> {
        if self.config.source.as_os_str().is_empty() {
            return Err(ToolingError::Validation("Source path is required".to_string()));
        }

        if self.config.table.is_empty() {
            return Err(ToolingError::Validation("Table name is required".to_string()));
        }

        if self.config.batch_size == 0 {
            return Err(ToolingError::Validation("Batch size must be > 0".to_string()));
        }

        Ok(())
    }

    /// Parse a CSV line into values
    pub fn parse_csv_line(line: &str, options: &CsvOptions) -> Vec<String> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if c == options.quote {
                if in_quotes {
                    // Check for escaped quote
                    if chars.peek() == Some(&options.quote) {
                        current.push(options.quote);
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            } else if c == options.delimiter && !in_quotes {
                values.push(std::mem::take(&mut current));
            } else if let Some(esc) = options.escape {
                if c == esc && chars.peek().is_some() {
                    current.push(chars.next().unwrap());
                } else {
                    current.push(c);
                }
            } else {
                current.push(c);
            }
        }

        values.push(current);
        values
    }

    /// Run import simulation (returns what would be imported)
    pub fn dry_run(&mut self, sample_data: &[Vec<String>]) -> ToolingResult<ImportResult> {
        self.validate()?;

        let mut result = ImportResult::new();
        result.rows_read = sample_data.len() as u64;
        result.rows_imported = sample_data.len() as u64;

        for (i, row) in sample_data.iter().enumerate() {
            result.bytes_read += row.iter().map(|s| s.len() as u64).sum::<u64>();

            // Validate row
            if row.is_empty() && self.config.on_error != ErrorHandling::Skip {
                result.errors.push(ImportError {
                    row: i as u64 + 1,
                    column: None,
                    message: "Empty row".to_string(),
                    raw_data: None,
                });
            }
        }

        Ok(result)
    }

    /// Get current result
    pub fn result(&self) -> &ImportResult {
        &self.result
    }
}

// ============================================================================
// Data Exporter
// ============================================================================

/// Data exporter for extracting data
pub struct DataExporter {
    config: ExportConfig,
    result: ExportResult,
}

impl DataExporter {
    pub fn new(config: ExportConfig) -> Self {
        Self {
            config,
            result: ExportResult::new(),
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> ToolingResult<()> {
        if self.config.query.is_empty() {
            return Err(ToolingError::Validation("Query is required".to_string()));
        }

        if self.config.target.as_os_str().is_empty() {
            return Err(ToolingError::Validation("Target path is required".to_string()));
        }

        Ok(())
    }

    /// Format row as CSV
    pub fn format_csv_row(values: &[String], options: &CsvOptions) -> String {
        values
            .iter()
            .map(|v| {
                if v.contains(options.delimiter) || v.contains(options.quote) || v.contains('\n') {
                    format!("{}{}{}",
                        options.quote,
                        v.replace(options.quote, &format!("{}{}", options.quote, options.quote)),
                        options.quote
                    )
                } else {
                    v.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(&options.delimiter.to_string())
    }

    /// Format row as JSON
    pub fn format_json_row(columns: &[String], values: &[String], options: &JsonOptions) -> String {
        let mut obj = String::from("{");

        for (i, (col, val)) in columns.iter().zip(values.iter()).enumerate() {
            if i > 0 {
                obj.push(',');
            }
            if options.pretty {
                obj.push_str(&format!("\n{}", options.indent));
            }
            obj.push_str(&format!("\"{}\":{}", col, Self::json_value(val)));
        }

        if options.pretty {
            obj.push('\n');
        }
        obj.push('}');
        obj
    }

    fn json_value(s: &str) -> String {
        // Try to parse as number
        if let Ok(n) = s.parse::<i64>() {
            return n.to_string();
        }
        if let Ok(n) = s.parse::<f64>() {
            return n.to_string();
        }
        // Boolean
        if s == "true" || s == "false" {
            return s.to_string();
        }
        // Null
        if s.is_empty() || s == "null" {
            return "null".to_string();
        }
        // String with escaping
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n"))
    }

    /// Get current result
    pub fn result(&self) -> &ExportResult {
        &self.result
    }
}

// ============================================================================
// Format Converter
// ============================================================================

/// Converts data between formats
pub struct FormatConverter {
    source_format: DataFormat,
    target_format: DataFormat,
    buffer_size: usize,
}

impl FormatConverter {
    pub fn new(source: DataFormat, target: DataFormat) -> Self {
        Self {
            source_format: source,
            target_format: target,
            buffer_size: 1_000_000,
        }
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Check if conversion is supported
    pub fn is_supported(&self) -> bool {
        // Most conversions are supported through intermediate representation
        true
    }

    /// Get estimated conversion complexity
    pub fn complexity(&self) -> ConversionComplexity {
        match (&self.source_format, &self.target_format) {
            (a, b) if a == b => ConversionComplexity::None,
            (DataFormat::Csv, DataFormat::Tsv) | (DataFormat::Tsv, DataFormat::Csv) => {
                ConversionComplexity::Trivial
            }
            (DataFormat::JsonLines, DataFormat::JsonArray) |
            (DataFormat::JsonArray, DataFormat::JsonLines) => {
                ConversionComplexity::Simple
            }
            (DataFormat::Csv | DataFormat::Tsv, DataFormat::Parquet | DataFormat::Orc) |
            (DataFormat::Parquet | DataFormat::Orc, DataFormat::Csv | DataFormat::Tsv) => {
                ConversionComplexity::Complex
            }
            _ => ConversionComplexity::Moderate,
        }
    }
}

/// Conversion complexity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionComplexity {
    /// No conversion needed (same format)
    None,
    /// Simple delimiter change
    Trivial,
    /// Minor restructuring
    Simple,
    /// Schema mapping needed
    Moderate,
    /// Full type conversion
    Complex,
}

// ============================================================================
// Local Query Engine (standalone query tool)
// ============================================================================

/// Standalone query engine for processing files without server
pub struct LocalEngine {
    /// Registered tables (file-backed)
    tables: Arc<RwLock<HashMap<String, LocalTable>>>,
    /// Working directory
    work_dir: PathBuf,
    /// Query result format
    output_format: DataFormat,
    /// Schema inference settings
    inference_settings: InferenceSettings,
}

/// Local table backed by file
#[derive(Debug, Clone)]
pub struct LocalTable {
    /// Table name
    pub name: String,
    /// Source file path
    pub path: PathBuf,
    /// Data format
    pub format: DataFormat,
    /// Inferred or provided schema
    pub schema: Option<TableSchema>,
    /// Format options
    pub format_options: FormatOptions,
}

/// Schema inference settings
#[derive(Debug, Clone)]
pub struct InferenceSettings {
    /// Number of rows to sample
    pub sample_rows: usize,
    /// Type coercion mode
    pub coercion: TypeCoercion,
    /// Try to detect dates
    pub detect_dates: bool,
    /// Try to detect JSON
    pub detect_json: bool,
}

impl Default for InferenceSettings {
    fn default() -> Self {
        Self {
            sample_rows: 1000,
            coercion: TypeCoercion::Loose,
            detect_dates: true,
            detect_json: true,
        }
    }
}

impl LocalEngine {
    pub fn new(work_dir: impl AsRef<Path>) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            work_dir: work_dir.as_ref().to_path_buf(),
            output_format: DataFormat::Pretty,
            inference_settings: InferenceSettings::default(),
        }
    }

    /// Set output format
    pub fn with_output_format(mut self, format: DataFormat) -> Self {
        self.output_format = format;
        self
    }

    /// Register a file as a table
    pub fn register_file(&self, name: &str, path: impl AsRef<Path>, format: DataFormat) -> ToolingResult<()> {
        let table = LocalTable {
            name: name.to_string(),
            path: path.as_ref().to_path_buf(),
            format,
            schema: None,
            format_options: FormatOptions::None,
        };

        self.tables.write().insert(name.to_string(), table);
        Ok(())
    }

    /// Register file with schema
    pub fn register_file_with_schema(
        &self,
        name: &str,
        path: impl AsRef<Path>,
        format: DataFormat,
        schema: TableSchema,
    ) -> ToolingResult<()> {
        let table = LocalTable {
            name: name.to_string(),
            path: path.as_ref().to_path_buf(),
            format,
            schema: Some(schema),
            format_options: FormatOptions::None,
        };

        self.tables.write().insert(name.to_string(), table);
        Ok(())
    }

    /// List registered tables
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Get table info
    pub fn describe_table(&self, name: &str) -> Option<LocalTable> {
        self.tables.read().get(name).cloned()
    }

    /// Unregister a table
    pub fn unregister_table(&self, name: &str) -> bool {
        self.tables.write().remove(name).is_some()
    }

    /// Execute query and get result
    pub fn execute(&self, _query: &str) -> ToolingResult<QueryResult> {
        // This would integrate with the actual SQL engine
        // For now, return empty result
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            execution_time_ms: 0,
        })
    }
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data
    pub rows: Vec<Vec<String>>,
    /// Affected rows (for INSERT/UPDATE/DELETE)
    pub affected_rows: u64,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Format as pretty table
    pub fn to_pretty(&self) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(val.len());
                }
            }
        }

        let mut output = String::new();

        // Header
        let header: Vec<String> = self.columns.iter()
            .zip(&widths)
            .map(|(c, w)| format!("{:width$}", c, width = *w))
            .collect();
        output.push_str(&header.join(" | "));
        output.push('\n');

        // Separator
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        output.push_str(&sep.join("-+-"));
        output.push('\n');

        // Rows
        for row in &self.rows {
            let formatted: Vec<String> = row.iter()
                .zip(&widths)
                .map(|(v, w)| format!("{:width$}", v, width = *w))
                .collect();
            output.push_str(&formatted.join(" | "));
            output.push('\n');
        }

        output
    }

    /// Format as vertical (psql \x style)
    pub fn to_vertical(&self) -> String {
        let mut output = String::new();

        for (row_num, row) in self.rows.iter().enumerate() {
            output.push_str(&format!("-[ RECORD {} ]-\n", row_num + 1));
            for (col, val) in self.columns.iter().zip(row.iter()) {
                output.push_str(&format!("{}: {}\n", col, val));
            }
        }

        output
    }
}

// ============================================================================
// Backup/Restore Utilities
// ============================================================================

/// Backup utility configuration
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Source database(s)
    pub databases: Vec<String>,
    /// Specific tables (empty = all)
    pub tables: Vec<String>,
    /// Target directory
    pub target_dir: PathBuf,
    /// Backup name
    pub name: String,
    /// Include schema
    pub include_schema: bool,
    /// Include data
    pub include_data: bool,
    /// Compression
    pub compression: Option<CompressionType>,
    /// Number of parallel threads
    pub threads: usize,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            databases: Vec::new(),
            tables: Vec::new(),
            target_dir: PathBuf::from("."),
            name: String::new(),
            include_schema: true,
            include_data: true,
            compression: Some(CompressionType::Zstd),
            threads: 4,
        }
    }
}

/// Restore utility configuration
#[derive(Debug, Clone)]
pub struct RestoreConfig {
    /// Backup directory
    pub source_dir: PathBuf,
    /// Target database (if different from backup)
    pub target_database: Option<String>,
    /// Specific tables to restore
    pub tables: Vec<String>,
    /// Whether to drop existing tables
    pub drop_existing: bool,
    /// Number of parallel threads
    pub threads: usize,
}

impl Default for RestoreConfig {
    fn default() -> Self {
        Self {
            source_dir: PathBuf::new(),
            target_database: None,
            tables: Vec::new(),
            drop_existing: false,
            threads: 4,
        }
    }
}

// ============================================================================
// Schema Migration
// ============================================================================

/// Schema migration operation
#[derive(Debug, Clone)]
pub struct Migration {
    /// Migration version
    pub version: u64,
    /// Migration name
    pub name: String,
    /// Up SQL
    pub up_sql: String,
    /// Down SQL (rollback)
    pub down_sql: String,
    /// Checksum for validation
    pub checksum: String,
}

/// Migration manager
pub struct MigrationManager {
    migrations: Vec<Migration>,
    applied: Vec<u64>,
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
            applied: Vec::new(),
        }
    }

    /// Add a migration
    pub fn add_migration(&mut self, migration: Migration) {
        self.migrations.push(migration);
        self.migrations.sort_by_key(|m| m.version);
    }

    /// Get pending migrations
    pub fn pending(&self) -> Vec<&Migration> {
        self.migrations
            .iter()
            .filter(|m| !self.applied.contains(&m.version))
            .collect()
    }

    /// Get applied migrations
    pub fn applied(&self) -> Vec<&Migration> {
        self.migrations
            .iter()
            .filter(|m| self.applied.contains(&m.version))
            .collect()
    }

    /// Mark migration as applied
    pub fn mark_applied(&mut self, version: u64) {
        if !self.applied.contains(&version) {
            self.applied.push(version);
            self.applied.sort();
        }
    }

    /// Mark migration as rolled back
    pub fn mark_rolled_back(&mut self, version: u64) {
        self.applied.retain(|v| *v != version);
    }

    /// Get next migration to apply
    pub fn next(&self) -> Option<&Migration> {
        self.pending().first().copied()
    }

    /// Get migration by version
    pub fn get(&self, version: u64) -> Option<&Migration> {
        self.migrations.iter().find(|m| m.version == version)
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Benchmarking Utilities
// ============================================================================

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Query to benchmark
    pub query: String,
    /// Number of iterations
    pub iterations: usize,
    /// Warmup iterations
    pub warmup: usize,
    /// Concurrent threads
    pub threads: usize,
    /// Whether to clear cache between runs
    pub clear_cache: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            query: String::new(),
            iterations: 10,
            warmup: 3,
            threads: 1,
            clear_cache: false,
        }
    }
}

/// Benchmark result
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Query text
    pub query: String,
    /// Number of iterations
    pub iterations: usize,
    /// Total time in milliseconds
    pub total_ms: u64,
    /// Average time in milliseconds
    pub avg_ms: f64,
    /// Minimum time in milliseconds
    pub min_ms: u64,
    /// Maximum time in milliseconds
    pub max_ms: u64,
    /// Median time in milliseconds
    pub median_ms: u64,
    /// P95 latency
    pub p95_ms: u64,
    /// P99 latency
    pub p99_ms: u64,
    /// Rows per second
    pub rows_per_second: f64,
    /// Individual timings
    pub timings: Vec<u64>,
}

impl BenchmarkResult {
    pub fn new(query: &str) -> Self {
        Self {
            query: query.to_string(),
            iterations: 0,
            total_ms: 0,
            avg_ms: 0.0,
            min_ms: u64::MAX,
            max_ms: 0,
            median_ms: 0,
            p95_ms: 0,
            p99_ms: 0,
            rows_per_second: 0.0,
            timings: Vec::new(),
        }
    }

    /// Add timing and update statistics
    pub fn add_timing(&mut self, ms: u64, rows: u64) {
        self.timings.push(ms);
        self.iterations = self.timings.len();
        self.total_ms += ms;
        self.min_ms = self.min_ms.min(ms);
        self.max_ms = self.max_ms.max(ms);
        self.avg_ms = self.total_ms as f64 / self.iterations as f64;

        if ms > 0 {
            self.rows_per_second = rows as f64 / (ms as f64 / 1000.0);
        }

        // Update percentiles
        let mut sorted = self.timings.clone();
        sorted.sort();

        if !sorted.is_empty() {
            self.median_ms = sorted[sorted.len() / 2];
            self.p95_ms = sorted[(sorted.len() as f64 * 0.95) as usize];
            self.p99_ms = sorted[(sorted.len() as f64 * 0.99).min(sorted.len() as f64 - 1.0) as usize];
        }
    }

    /// Format as report
    pub fn to_report(&self) -> String {
        format!(
            "Benchmark Results for:\n{}\n\n\
             Iterations: {}\n\
             Total time: {} ms\n\
             Average:    {:.2} ms\n\
             Min:        {} ms\n\
             Max:        {} ms\n\
             Median:     {} ms\n\
             P95:        {} ms\n\
             P99:        {} ms\n\
             Throughput: {:.0} rows/sec",
            self.query,
            self.iterations,
            self.total_ms,
            self.avg_ms,
            self.min_ms,
            self.max_ms,
            self.median_ms,
            self.p95_ms,
            self.p99_ms,
            self.rows_per_second
        )
    }
}

// ============================================================================
// CLI Command Parser
// ============================================================================

/// Parsed CLI command
#[derive(Debug, Clone)]
pub enum CliCommand {
    /// Query execution
    Query { sql: String, format: DataFormat },
    /// Import data
    Import(ImportConfig),
    /// Export data
    Export(ExportConfig),
    /// Convert format
    Convert { source: PathBuf, target: PathBuf, format: DataFormat },
    /// Describe table
    Describe { table: String },
    /// List tables
    ListTables,
    /// Show schema
    ShowSchema { table: String },
    /// Run benchmark
    Benchmark(BenchmarkConfig),
    /// Backup database
    Backup(BackupConfig),
    /// Restore database
    Restore(RestoreConfig),
    /// Run migrations
    Migrate { direction: MigrateDirection, target: Option<u64> },
    /// Help
    Help { topic: Option<String> },
    /// Exit
    Exit,
}

#[derive(Debug, Clone, Copy)]
pub enum MigrateDirection {
    Up,
    Down,
}

/// CLI command parser
pub struct CommandParser;

impl CommandParser {
    /// Parse command string
    pub fn parse(input: &str) -> ToolingResult<CliCommand> {
        let trimmed = input.trim();

        // Check for meta-commands
        if trimmed.starts_with('\\') {
            return Self::parse_meta_command(trimmed);
        }

        // Check for keywords
        let upper = trimmed.to_uppercase();
        if upper.starts_with("SELECT") || upper.starts_with("WITH") ||
           upper.starts_with("INSERT") || upper.starts_with("UPDATE") ||
           upper.starts_with("DELETE") || upper.starts_with("CREATE") ||
           upper.starts_with("DROP") || upper.starts_with("ALTER") {
            return Ok(CliCommand::Query {
                sql: trimmed.to_string(),
                format: DataFormat::Pretty
            });
        }

        if upper.starts_with("IMPORT") {
            // Basic import parsing
            return Ok(CliCommand::Import(ImportConfig::default()));
        }

        if upper.starts_with("EXPORT") {
            return Ok(CliCommand::Export(ExportConfig::default()));
        }

        if upper == "QUIT" || upper == "EXIT" || upper == "\\Q" {
            return Ok(CliCommand::Exit);
        }

        if upper.starts_with("HELP") || upper == "\\?" {
            let topic = if upper.len() > 5 {
                Some(trimmed[5..].trim().to_string())
            } else {
                None
            };
            return Ok(CliCommand::Help { topic });
        }

        Err(ToolingError::Parse(format!("Unknown command: {}", trimmed)))
    }

    /// Parse meta-command (backslash commands)
    fn parse_meta_command(input: &str) -> ToolingResult<CliCommand> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return Err(ToolingError::Parse("Empty command".to_string()));
        }

        match parts[0] {
            "\\d" | "\\dt" => Ok(CliCommand::ListTables),
            "\\d+" => {
                if parts.len() > 1 {
                    Ok(CliCommand::Describe { table: parts[1].to_string() })
                } else {
                    Ok(CliCommand::ListTables)
                }
            }
            "\\s" => {
                if parts.len() > 1 {
                    Ok(CliCommand::ShowSchema { table: parts[1].to_string() })
                } else {
                    Err(ToolingError::Parse("\\s requires table name".to_string()))
                }
            }
            "\\q" | "\\quit" | "\\exit" => Ok(CliCommand::Exit),
            "\\?" | "\\h" | "\\help" => Ok(CliCommand::Help {
                topic: parts.get(1).map(|s| s.to_string())
            }),
            _ => Err(ToolingError::Parse(format!("Unknown meta-command: {}", parts[0]))),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_format_extension() {
        assert_eq!(DataFormat::Csv.extension(), "csv");
        assert_eq!(DataFormat::Parquet.extension(), "parquet");
        assert_eq!(DataFormat::JsonLines.extension(), "jsonl");
    }

    #[test]
    fn test_data_format_from_extension() {
        assert_eq!(DataFormat::from_extension("csv"), Some(DataFormat::Csv));
        assert_eq!(DataFormat::from_extension("parquet"), Some(DataFormat::Parquet));
        assert_eq!(DataFormat::from_extension("pq"), Some(DataFormat::Parquet));
        assert_eq!(DataFormat::from_extension("unknown"), None);
    }

    #[test]
    fn test_csv_parsing() {
        let options = CsvOptions::default();

        let line = "hello,world,test";
        let values = DataImporter::parse_csv_line(line, &options);
        assert_eq!(values, vec!["hello", "world", "test"]);

        let quoted = "\"hello, world\",test,\"with \"\"quotes\"\"\"";
        let values = DataImporter::parse_csv_line(quoted, &options);
        assert_eq!(values, vec!["hello, world", "test", "with \"quotes\""]);
    }

    #[test]
    fn test_schema_inference() {
        assert_eq!(SchemaInferrer::infer_type("123"), ColumnType::Int8);
        assert_eq!(SchemaInferrer::infer_type("12345"), ColumnType::Int16);
        assert_eq!(SchemaInferrer::infer_type("1234567890"), ColumnType::Int32);
        assert_eq!(SchemaInferrer::infer_type("12.34"), ColumnType::Float64);
        assert_eq!(SchemaInferrer::infer_type("hello"), ColumnType::String);
        assert_eq!(SchemaInferrer::infer_type("true"), ColumnType::Bool);
        assert_eq!(SchemaInferrer::infer_type("2024-01-15"), ColumnType::Date);
    }

    #[test]
    fn test_column_type_name() {
        assert_eq!(ColumnType::Int64.name(), "Int64");
        assert_eq!(ColumnType::FixedString(32).name(), "FixedString(32)");
        assert_eq!(ColumnType::Decimal(18, 4).name(), "Decimal(18, 4)");
        assert_eq!(
            ColumnType::Array(Box::new(ColumnType::String)).name(),
            "Array(String)"
        );
    }

    #[test]
    fn test_table_schema_to_sql() {
        let mut schema = TableSchema::new("events", "default");
        schema.add_column("id", ColumnType::UInt64, false);
        schema.add_column("timestamp", ColumnType::DateTime, false);
        schema.add_column("value", ColumnType::Float64, true);
        schema.order_by = vec!["id".to_string()];

        let sql = schema.to_create_sql();
        assert!(sql.contains("CREATE TABLE default.events"));
        assert!(sql.contains("id UInt64 NOT NULL"));
        assert!(sql.contains("ORDER BY (id)"));
    }

    #[test]
    fn test_csv_row_formatting() {
        let options = CsvOptions::default();
        let values = vec!["hello".to_string(), "world, test".to_string()];
        let row = DataExporter::format_csv_row(&values, &options);
        assert_eq!(row, "hello,\"world, test\"");
    }

    #[test]
    fn test_json_row_formatting() {
        let options = JsonOptions::default();
        let columns = vec!["name".to_string(), "value".to_string()];
        let values = vec!["test".to_string(), "123".to_string()];
        let json = DataExporter::format_json_row(&columns, &values, &options);
        assert_eq!(json, "{\"name\":\"test\",\"value\":123}");
    }

    #[test]
    fn test_query_result_pretty() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
            affected_rows: 0,
            execution_time_ms: 10,
        };

        let pretty = result.to_pretty();
        assert!(pretty.contains("id"));
        assert!(pretty.contains("name"));
        assert!(pretty.contains("Alice"));
        assert!(pretty.contains("Bob"));
    }

    #[test]
    fn test_query_result_vertical() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
            ],
            affected_rows: 0,
            execution_time_ms: 10,
        };

        let vertical = result.to_vertical();
        assert!(vertical.contains("-[ RECORD 1 ]-"));
        assert!(vertical.contains("id: 1"));
        assert!(vertical.contains("name: Alice"));
    }

    #[test]
    fn test_local_engine() {
        let engine = LocalEngine::new("/tmp");

        engine.register_file("test", "/tmp/test.csv", DataFormat::Csv).unwrap();

        let tables = engine.list_tables();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"test".to_string()));

        let table = engine.describe_table("test");
        assert!(table.is_some());
        assert_eq!(table.unwrap().format, DataFormat::Csv);
    }

    #[test]
    fn test_format_converter() {
        let converter = FormatConverter::new(DataFormat::Csv, DataFormat::Parquet);
        assert!(converter.is_supported());
        assert_eq!(converter.complexity(), ConversionComplexity::Complex);

        let trivial = FormatConverter::new(DataFormat::Csv, DataFormat::Tsv);
        assert_eq!(trivial.complexity(), ConversionComplexity::Trivial);

        let same = FormatConverter::new(DataFormat::Csv, DataFormat::Csv);
        assert_eq!(same.complexity(), ConversionComplexity::None);
    }

    #[test]
    fn test_migration_manager() {
        let mut manager = MigrationManager::new();

        manager.add_migration(Migration {
            version: 1,
            name: "create_users".to_string(),
            up_sql: "CREATE TABLE users ...".to_string(),
            down_sql: "DROP TABLE users".to_string(),
            checksum: "abc123".to_string(),
        });

        manager.add_migration(Migration {
            version: 2,
            name: "create_orders".to_string(),
            up_sql: "CREATE TABLE orders ...".to_string(),
            down_sql: "DROP TABLE orders".to_string(),
            checksum: "def456".to_string(),
        });

        assert_eq!(manager.pending().len(), 2);
        assert_eq!(manager.applied().len(), 0);

        manager.mark_applied(1);
        assert_eq!(manager.pending().len(), 1);
        assert_eq!(manager.applied().len(), 1);

        let next = manager.next();
        assert!(next.is_some());
        assert_eq!(next.unwrap().version, 2);
    }

    #[test]
    fn test_benchmark_result() {
        let mut result = BenchmarkResult::new("SELECT * FROM test");

        result.add_timing(100, 1000);
        result.add_timing(90, 1000);
        result.add_timing(110, 1000);

        assert_eq!(result.iterations, 3);
        assert_eq!(result.min_ms, 90);
        assert_eq!(result.max_ms, 110);
        assert_eq!(result.total_ms, 300);
    }

    #[test]
    fn test_command_parser() {
        let cmd = CommandParser::parse("SELECT * FROM test").unwrap();
        assert!(matches!(cmd, CliCommand::Query { .. }));

        let cmd = CommandParser::parse("\\d").unwrap();
        assert!(matches!(cmd, CliCommand::ListTables));

        let cmd = CommandParser::parse("\\q").unwrap();
        assert!(matches!(cmd, CliCommand::Exit));

        let cmd = CommandParser::parse("HELP").unwrap();
        assert!(matches!(cmd, CliCommand::Help { .. }));
    }

    #[test]
    fn test_import_dry_run() {
        let config = ImportConfig {
            source: PathBuf::from("/tmp/test.csv"),
            table: "test".to_string(),
            ..Default::default()
        };

        let mut importer = DataImporter::new(config);

        let sample = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["c".to_string(), "d".to_string()],
        ];

        let result = importer.dry_run(&sample).unwrap();
        assert_eq!(result.rows_read, 2);
        assert_eq!(result.rows_imported, 2);
    }

    #[test]
    fn test_import_result_rates() {
        let mut result = ImportResult::new();
        result.rows_imported = 1_000_000;
        result.bytes_read = 100_000_000;
        result.duration_ms = 10_000;

        assert_eq!(result.rows_per_second(), 100_000.0);
        assert_eq!(result.mb_per_second(), 100_000_000.0 / (1024.0 * 1024.0) / 10.0);
    }

    #[test]
    fn test_compression_type_extension() {
        assert_eq!(CompressionType::Gzip.extension(), "gz");
        assert_eq!(CompressionType::Zstd.extension(), "zst");
        assert_eq!(CompressionType::Lz4.extension(), "lz4");
    }
}
