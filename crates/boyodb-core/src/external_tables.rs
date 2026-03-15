//! External Tables
//!
//! Query S3, URL, and HDFS directly without importing data.
//! Supports lazy loading, predicate pushdown, and parallel scanning.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// External table type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalTableType {
    /// S3/Object storage
    S3,
    /// HTTP URL
    Url,
    /// HDFS
    Hdfs,
    /// Local file
    File,
    /// ClickHouse remote
    Remote,
    /// Delta Lake
    Delta,
    /// Iceberg
    Iceberg,
}

/// External table configuration
#[derive(Debug, Clone)]
pub struct ExternalTableConfig {
    /// Table name
    pub name: String,
    /// External source type
    pub source_type: ExternalTableType,
    /// Source location (URL, path, etc.)
    pub location: String,
    /// File format
    pub format: FileFormat,
    /// Schema (column definitions)
    pub schema: Vec<ColumnDef>,
    /// Partitioning columns
    pub partition_columns: Vec<String>,
    /// Source-specific options
    pub options: HashMap<String, String>,
    /// Compression
    pub compression: Option<String>,
    /// Read-only
    pub read_only: bool,
}

/// File format
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
    Orc,
    Avro,
    Arrow,
    Text,
}

impl FileFormat {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "parquet" | "pq" => Some(FileFormat::Parquet),
            "csv" => Some(FileFormat::Csv),
            "json" | "jsonl" | "ndjson" => Some(FileFormat::Json),
            "orc" => Some(FileFormat::Orc),
            "avro" => Some(FileFormat::Avro),
            "arrow" | "ipc" => Some(FileFormat::Arrow),
            "txt" | "text" => Some(FileFormat::Text),
            _ => None,
        }
    }
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// External table registry
pub struct ExternalTableRegistry {
    /// Registered tables
    tables: RwLock<HashMap<String, ExternalTableConfig>>,
    /// S3 client options
    s3_options: RwLock<S3Options>,
}

/// S3 options
#[derive(Debug, Clone, Default)]
pub struct S3Options {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub path_style: bool,
}

impl ExternalTableRegistry {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            s3_options: RwLock::new(S3Options::default()),
        }
    }

    /// Configure S3 options
    pub fn configure_s3(&self, options: S3Options) {
        *self.s3_options.write().unwrap() = options;
    }

    /// Register an external table
    pub fn register(&self, config: ExternalTableConfig) -> Result<(), ExternalTableError> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&config.name) {
            return Err(ExternalTableError::AlreadyExists(config.name.clone()));
        }

        // Validate configuration
        self.validate_config(&config)?;

        tables.insert(config.name.clone(), config);
        Ok(())
    }

    /// Unregister an external table
    pub fn unregister(&self, name: &str) -> Result<(), ExternalTableError> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.remove(name).is_none() {
            return Err(ExternalTableError::NotFound(name.into()));
        }

        Ok(())
    }

    /// Get table config
    pub fn get(&self, name: &str) -> Option<ExternalTableConfig> {
        self.tables.read().unwrap().get(name).cloned()
    }

    /// List all external tables
    pub fn list(&self) -> Vec<ExternalTableConfig> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    /// Create table function (for s3(), url(), file() functions)
    pub fn create_table_function(
        &self,
        function_name: &str,
        args: &[String],
    ) -> Result<ExternalTableConfig, ExternalTableError> {
        match function_name.to_lowercase().as_str() {
            "s3" => self.create_s3_table(args),
            "url" => self.create_url_table(args),
            "file" => self.create_file_table(args),
            "hdfs" => self.create_hdfs_table(args),
            _ => Err(ExternalTableError::UnknownFunction(function_name.into())),
        }
    }

    fn create_s3_table(&self, args: &[String]) -> Result<ExternalTableConfig, ExternalTableError> {
        if args.is_empty() {
            return Err(ExternalTableError::InvalidArguments("s3() requires at least a path".into()));
        }

        let location = &args[0];
        let format = if args.len() > 1 {
            self.parse_format(&args[1])?
        } else {
            self.infer_format(location)?
        };

        let mut options = HashMap::new();
        let s3_opts = self.s3_options.read().unwrap();
        
        if let Some(ref region) = s3_opts.region {
            options.insert("region".into(), region.clone());
        }
        if let Some(ref endpoint) = s3_opts.endpoint {
            options.insert("endpoint".into(), endpoint.clone());
        }

        Ok(ExternalTableConfig {
            name: format!("_s3_{}", location.replace(['/', '.', '-'], "_")),
            source_type: ExternalTableType::S3,
            location: location.clone(),
            format,
            schema: Vec::new(), // Will be inferred
            partition_columns: Vec::new(),
            options,
            compression: None,
            read_only: true,
        })
    }

    fn create_url_table(&self, args: &[String]) -> Result<ExternalTableConfig, ExternalTableError> {
        if args.is_empty() {
            return Err(ExternalTableError::InvalidArguments("url() requires a URL".into()));
        }

        let location = &args[0];
        let format = if args.len() > 1 {
            self.parse_format(&args[1])?
        } else {
            self.infer_format(location)?
        };

        Ok(ExternalTableConfig {
            name: format!("_url_{}", location.len()),
            source_type: ExternalTableType::Url,
            location: location.clone(),
            format,
            schema: Vec::new(),
            partition_columns: Vec::new(),
            options: HashMap::new(),
            compression: None,
            read_only: true,
        })
    }

    fn create_file_table(&self, args: &[String]) -> Result<ExternalTableConfig, ExternalTableError> {
        if args.is_empty() {
            return Err(ExternalTableError::InvalidArguments("file() requires a path".into()));
        }

        let location = &args[0];
        let format = if args.len() > 1 {
            self.parse_format(&args[1])?
        } else {
            self.infer_format(location)?
        };

        Ok(ExternalTableConfig {
            name: format!("_file_{}", location.replace(['/', '.', '-'], "_")),
            source_type: ExternalTableType::File,
            location: location.clone(),
            format,
            schema: Vec::new(),
            partition_columns: Vec::new(),
            options: HashMap::new(),
            compression: None,
            read_only: false,
        })
    }

    fn create_hdfs_table(&self, args: &[String]) -> Result<ExternalTableConfig, ExternalTableError> {
        if args.is_empty() {
            return Err(ExternalTableError::InvalidArguments("hdfs() requires a path".into()));
        }

        let location = &args[0];
        let format = if args.len() > 1 {
            self.parse_format(&args[1])?
        } else {
            self.infer_format(location)?
        };

        Ok(ExternalTableConfig {
            name: format!("_hdfs_{}", location.replace(['/', '.', '-'], "_")),
            source_type: ExternalTableType::Hdfs,
            location: location.clone(),
            format,
            schema: Vec::new(),
            partition_columns: Vec::new(),
            options: HashMap::new(),
            compression: None,
            read_only: true,
        })
    }

    fn parse_format(&self, format: &str) -> Result<FileFormat, ExternalTableError> {
        match format.to_lowercase().as_str() {
            "parquet" => Ok(FileFormat::Parquet),
            "csv" => Ok(FileFormat::Csv),
            "json" | "jsonl" | "ndjson" => Ok(FileFormat::Json),
            "orc" => Ok(FileFormat::Orc),
            "avro" => Ok(FileFormat::Avro),
            "arrow" => Ok(FileFormat::Arrow),
            "text" => Ok(FileFormat::Text),
            _ => Err(ExternalTableError::UnsupportedFormat(format.into())),
        }
    }

    fn infer_format(&self, path: &str) -> Result<FileFormat, ExternalTableError> {
        let path_lower = path.to_lowercase();
        
        // Remove compression extensions first
        let path_clean = path_lower
            .trim_end_matches(".gz")
            .trim_end_matches(".zst")
            .trim_end_matches(".bz2")
            .trim_end_matches(".lz4");

        if let Some(ext) = path_clean.rsplit('.').next() {
            if let Some(format) = FileFormat::from_extension(ext) {
                return Ok(format);
            }
        }

        Err(ExternalTableError::CannotInferFormat(path.into()))
    }

    fn validate_config(&self, config: &ExternalTableConfig) -> Result<(), ExternalTableError> {
        // Validate location
        match config.source_type {
            ExternalTableType::S3 => {
                if !config.location.starts_with("s3://") && !config.location.starts_with("s3a://") {
                    return Err(ExternalTableError::InvalidLocation(
                        "S3 location must start with s3:// or s3a://".into()
                    ));
                }
            }
            ExternalTableType::Url => {
                if !config.location.starts_with("http://") && !config.location.starts_with("https://") {
                    return Err(ExternalTableError::InvalidLocation(
                        "URL must start with http:// or https://".into()
                    ));
                }
            }
            ExternalTableType::Hdfs => {
                if !config.location.starts_with("hdfs://") {
                    return Err(ExternalTableError::InvalidLocation(
                        "HDFS location must start with hdfs://".into()
                    ));
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl Default for ExternalTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Scan options for external tables
#[derive(Debug, Clone, Default)]
pub struct ExternalScanOptions {
    /// Columns to project
    pub projection: Option<Vec<String>>,
    /// Filter predicate (pushdown)
    pub filter: Option<String>,
    /// Limit
    pub limit: Option<usize>,
    /// Partition filter
    pub partition_filter: Option<String>,
    /// Parallel scans
    pub parallel_scans: usize,
    /// Row group/stripe filtering
    pub row_group_filter: bool,
}

/// External table scanner (trait for different implementations)
pub trait ExternalScanner: Send + Sync {
    /// Scan the external table
    fn scan(&self, options: &ExternalScanOptions) -> Result<Box<dyn Iterator<Item = ScanBatch>>, ExternalTableError>;
    
    /// Get estimated row count
    fn estimated_rows(&self) -> Option<u64>;
    
    /// Get estimated size in bytes
    fn estimated_bytes(&self) -> Option<u64>;
}

/// Scan batch result
#[derive(Debug)]
pub struct ScanBatch {
    /// Batch data (Arrow IPC)
    pub data: Vec<u8>,
    /// Row count
    pub row_count: usize,
    /// Bytes read
    pub bytes_read: usize,
}

/// External table error
#[derive(Debug, Clone)]
pub enum ExternalTableError {
    NotFound(String),
    AlreadyExists(String),
    InvalidLocation(String),
    InvalidArguments(String),
    UnsupportedFormat(String),
    CannotInferFormat(String),
    UnknownFunction(String),
    IoError(String),
    SchemaError(String),
}

impl std::fmt::Display for ExternalTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(n) => write!(f, "External table '{}' not found", n),
            Self::AlreadyExists(n) => write!(f, "External table '{}' already exists", n),
            Self::InvalidLocation(s) => write!(f, "Invalid location: {}", s),
            Self::InvalidArguments(s) => write!(f, "Invalid arguments: {}", s),
            Self::UnsupportedFormat(s) => write!(f, "Unsupported format: {}", s),
            Self::CannotInferFormat(s) => write!(f, "Cannot infer format from: {}", s),
            Self::UnknownFunction(s) => write!(f, "Unknown table function: {}", s),
            Self::IoError(s) => write!(f, "I/O error: {}", s),
            Self::SchemaError(s) => write!(f, "Schema error: {}", s),
        }
    }
}

impl std::error::Error for ExternalTableError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_table_function() {
        let registry = ExternalTableRegistry::new();
        registry.configure_s3(S3Options {
            region: Some("us-east-1".into()),
            ..Default::default()
        });

        let config = registry.create_table_function(
            "s3",
            &["s3://bucket/path/data.parquet".into()]
        ).unwrap();

        assert_eq!(config.source_type, ExternalTableType::S3);
        assert_eq!(config.format, FileFormat::Parquet);
        assert_eq!(config.options.get("region"), Some(&"us-east-1".into()));
    }

    #[test]
    fn test_url_table_function() {
        let registry = ExternalTableRegistry::new();

        let config = registry.create_table_function(
            "url",
            &["https://example.com/data.csv".into()]
        ).unwrap();

        assert_eq!(config.source_type, ExternalTableType::Url);
        assert_eq!(config.format, FileFormat::Csv);
    }

    #[test]
    fn test_format_inference() {
        let registry = ExternalTableRegistry::new();

        assert_eq!(registry.infer_format("data.parquet").unwrap(), FileFormat::Parquet);
        assert_eq!(registry.infer_format("data.csv.gz").unwrap(), FileFormat::Csv);
        assert_eq!(registry.infer_format("data.json.zst").unwrap(), FileFormat::Json);
    }

    #[test]
    fn test_register_external_table() {
        let registry = ExternalTableRegistry::new();

        let config = ExternalTableConfig {
            name: "my_s3_table".into(),
            source_type: ExternalTableType::S3,
            location: "s3://bucket/data/".into(),
            format: FileFormat::Parquet,
            schema: vec![
                ColumnDef { name: "id".into(), data_type: "INT64".into(), nullable: false },
                ColumnDef { name: "name".into(), data_type: "STRING".into(), nullable: true },
            ],
            partition_columns: vec!["date".into()],
            options: HashMap::new(),
            compression: None,
            read_only: true,
        };

        registry.register(config).unwrap();
        
        let retrieved = registry.get("my_s3_table").unwrap();
        assert_eq!(retrieved.schema.len(), 2);
    }
}
