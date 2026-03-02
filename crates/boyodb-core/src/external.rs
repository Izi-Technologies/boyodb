//! External Integrations Module
//!
//! This module provides external data source integrations:
//! - S3/GCS/Azure object storage for cloud-native storage
//! - Parquet import/export for data lake interoperability
//! - Dictionary tables for efficient dimension lookups
//! - External tables for querying external data sources

use std::collections::{HashMap, BTreeMap};
use std::io::{Read, Write, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Object Storage Abstraction (S3/GCS/Azure)
// ============================================================================

/// Object storage path
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath {
    /// Bucket/container name
    pub bucket: String,
    /// Object key/path
    pub key: String,
}

impl ObjectPath {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    }

    pub fn from_uri(uri: &str) -> Result<Self, StorageError> {
        // Parse s3://bucket/key or gs://bucket/key or az://container/blob
        let uri = uri.trim();

        let (scheme, rest) = uri.split_once("://")
            .ok_or_else(|| StorageError::InvalidPath(uri.to_string()))?;

        match scheme {
            "s3" | "gs" | "az" | "file" => {
                let (bucket, key) = rest.split_once('/')
                    .ok_or_else(|| StorageError::InvalidPath(uri.to_string()))?;
                Ok(Self::new(bucket, key))
            }
            _ => Err(StorageError::UnsupportedScheme(scheme.to_string())),
        }
    }

    pub fn to_uri(&self, scheme: &str) -> String {
        format!("{}://{}/{}", scheme, self.bucket, self.key)
    }

    pub fn join(&self, suffix: &str) -> Self {
        let key = if self.key.ends_with('/') {
            format!("{}{}", self.key, suffix)
        } else {
            format!("{}/{}", self.key, suffix)
        };
        Self::new(&self.bucket, &key)
    }
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Full path
    pub path: ObjectPath,
    /// Size in bytes
    pub size: u64,
    /// Last modified timestamp
    pub last_modified: u64,
    /// ETag/version
    pub etag: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

/// List result for paginated listing
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Objects found
    pub objects: Vec<ObjectMeta>,
    /// Common prefixes (directories)
    pub prefixes: Vec<String>,
    /// Continuation token for pagination
    pub continuation_token: Option<String>,
}

/// Object storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage provider type
    pub provider: StorageProvider,
    /// Region
    pub region: String,
    /// Endpoint URL (for S3-compatible services like MinIO)
    pub endpoint: Option<String>,
    /// Access key ID
    pub access_key_id: Option<String>,
    /// Secret access key
    pub secret_access_key: Option<String>,
    /// Session token (for temporary credentials)
    pub session_token: Option<String>,
    /// Use path-style addressing (for S3-compatible)
    pub path_style: bool,
    /// Connection timeout
    pub timeout: Duration,
    /// Max retries
    pub max_retries: u32,
    /// Enable request signing
    pub sign_requests: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProvider {
    /// Amazon S3
    S3,
    /// Google Cloud Storage
    GCS,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem (for testing)
    Local,
    /// S3-compatible (MinIO, etc.)
    S3Compatible,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            provider: StorageProvider::S3,
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            path_style: false,
            timeout: Duration::from_secs(30),
            max_retries: 3,
            sign_requests: true,
        }
    }
}

/// Multipart upload state
#[derive(Debug, Clone)]
pub struct MultipartUpload {
    /// Upload ID
    pub upload_id: String,
    /// Target path
    pub path: ObjectPath,
    /// Parts uploaded
    pub parts: Vec<UploadPart>,
    /// Part size in bytes
    pub part_size: usize,
    /// Started timestamp
    pub started: u64,
}

#[derive(Debug, Clone)]
pub struct UploadPart {
    /// Part number (1-based)
    pub part_number: u32,
    /// ETag returned from upload
    pub etag: String,
    /// Size of this part
    pub size: usize,
}

/// Abstract object storage trait
pub trait ObjectStore: Send + Sync {
    /// Get object contents
    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError>;

    /// Get object with byte range
    fn get_range(&self, path: &ObjectPath, start: u64, end: u64) -> Result<Vec<u8>, StorageError>;

    /// Put object
    fn put(&self, path: &ObjectPath, data: &[u8]) -> Result<(), StorageError>;

    /// Delete object
    fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;

    /// Check if object exists
    fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError>;

    /// Get object metadata
    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta, StorageError>;

    /// List objects with prefix
    fn list(&self, path: &ObjectPath, continuation_token: Option<&str>) -> Result<ListResult, StorageError>;

    /// Copy object
    fn copy(&self, src: &ObjectPath, dst: &ObjectPath) -> Result<(), StorageError>;

    /// Start multipart upload
    fn create_multipart(&self, path: &ObjectPath) -> Result<String, StorageError>;

    /// Upload a part
    fn upload_part(&self, path: &ObjectPath, upload_id: &str, part_number: u32, data: &[u8]) -> Result<String, StorageError>;

    /// Complete multipart upload
    fn complete_multipart(&self, path: &ObjectPath, upload_id: &str, parts: &[UploadPart]) -> Result<(), StorageError>;

    /// Abort multipart upload
    fn abort_multipart(&self, path: &ObjectPath, upload_id: &str) -> Result<(), StorageError>;
}

/// In-memory object store for testing
pub struct InMemoryObjectStore {
    objects: RwLock<HashMap<String, (Vec<u8>, ObjectMeta)>>,
    multipart_uploads: RwLock<HashMap<String, MultipartUpload>>,
}

impl InMemoryObjectStore {
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            multipart_uploads: RwLock::new(HashMap::new()),
        }
    }

    fn key(path: &ObjectPath) -> String {
        format!("{}/{}", path.bucket, path.key)
    }
}

impl Default for InMemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStore for InMemoryObjectStore {
    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        let key = Self::key(path);
        self.objects.read().unwrap()
            .get(&key)
            .map(|(data, _)| data.clone())
            .ok_or_else(|| StorageError::NotFound(path.clone()))
    }

    fn get_range(&self, path: &ObjectPath, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let data = self.get(path)?;
        let start = start as usize;
        let end = (end as usize).min(data.len());
        if start >= data.len() {
            return Ok(vec![]);
        }
        Ok(data[start..end].to_vec())
    }

    fn put(&self, path: &ObjectPath, data: &[u8]) -> Result<(), StorageError> {
        let key = Self::key(path);
        let meta = ObjectMeta {
            path: path.clone(),
            size: data.len() as u64,
            last_modified: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            etag: Some(format!("{:x}", md5_hash(data))),
            content_type: None,
            metadata: HashMap::new(),
        };
        self.objects.write().unwrap().insert(key, (data.to_vec(), meta));
        Ok(())
    }

    fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let key = Self::key(path);
        self.objects.write().unwrap().remove(&key);
        Ok(())
    }

    fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError> {
        let key = Self::key(path);
        Ok(self.objects.read().unwrap().contains_key(&key))
    }

    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta, StorageError> {
        let key = Self::key(path);
        self.objects.read().unwrap()
            .get(&key)
            .map(|(_, meta)| meta.clone())
            .ok_or_else(|| StorageError::NotFound(path.clone()))
    }

    fn list(&self, path: &ObjectPath, _continuation_token: Option<&str>) -> Result<ListResult, StorageError> {
        let prefix = Self::key(path);
        let objects = self.objects.read().unwrap();

        let matching: Vec<_> = objects.iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, (_, meta))| meta.clone())
            .collect();

        Ok(ListResult {
            objects: matching,
            prefixes: vec![],
            continuation_token: None,
        })
    }

    fn copy(&self, src: &ObjectPath, dst: &ObjectPath) -> Result<(), StorageError> {
        let data = self.get(src)?;
        self.put(dst, &data)
    }

    fn create_multipart(&self, path: &ObjectPath) -> Result<String, StorageError> {
        let upload_id = format!("upload_{}",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos());

        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            path: path.clone(),
            parts: Vec::new(),
            part_size: 5 * 1024 * 1024, // 5MB default
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        self.multipart_uploads.write().unwrap().insert(upload_id.clone(), upload);
        Ok(upload_id)
    }

    fn upload_part(&self, path: &ObjectPath, upload_id: &str, part_number: u32, data: &[u8]) -> Result<String, StorageError> {
        let etag = format!("{:x}", md5_hash(data));

        // Store part data temporarily
        let part_key = format!("{}/_multipart/{}/{}", Self::key(path), upload_id, part_number);
        self.objects.write().unwrap().insert(part_key, (data.to_vec(), ObjectMeta {
            path: path.clone(),
            size: data.len() as u64,
            last_modified: 0,
            etag: Some(etag.clone()),
            content_type: None,
            metadata: HashMap::new(),
        }));

        // Track part in upload
        let mut uploads = self.multipart_uploads.write().unwrap();
        if let Some(upload) = uploads.get_mut(upload_id) {
            upload.parts.push(UploadPart {
                part_number,
                etag: etag.clone(),
                size: data.len(),
            });
        }

        Ok(etag)
    }

    fn complete_multipart(&self, path: &ObjectPath, upload_id: &str, parts: &[UploadPart]) -> Result<(), StorageError> {
        // Combine all parts
        let mut combined = Vec::new();

        for part in parts {
            let part_key = format!("{}/_multipart/{}/{}", Self::key(path), upload_id, part.part_number);
            if let Some((data, _)) = self.objects.read().unwrap().get(&part_key) {
                combined.extend_from_slice(data);
            }
        }

        // Store combined object
        self.put(path, &combined)?;

        // Clean up parts
        for part in parts {
            let part_key = format!("{}/_multipart/{}/{}", Self::key(path), upload_id, part.part_number);
            self.objects.write().unwrap().remove(&part_key);
        }

        self.multipart_uploads.write().unwrap().remove(upload_id);
        Ok(())
    }

    fn abort_multipart(&self, path: &ObjectPath, upload_id: &str) -> Result<(), StorageError> {
        // Clean up parts
        let uploads = self.multipart_uploads.read().unwrap();
        if let Some(upload) = uploads.get(upload_id) {
            for part in &upload.parts {
                let part_key = format!("{}/_multipart/{}/{}", Self::key(path), upload_id, part.part_number);
                self.objects.write().unwrap().remove(&part_key);
            }
        }
        drop(uploads);

        self.multipart_uploads.write().unwrap().remove(upload_id);
        Ok(())
    }
}

/// Simple hash function for ETags (not cryptographic)
fn md5_hash(data: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Storage error types
#[derive(Debug)]
pub enum StorageError {
    NotFound(ObjectPath),
    AccessDenied(String),
    InvalidPath(String),
    UnsupportedScheme(String),
    NetworkError(String),
    Timeout,
    ChecksumMismatch,
    QuotaExceeded,
    ServiceUnavailable,
    Other(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound(p) => write!(f, "Object not found: {}/{}", p.bucket, p.key),
            StorageError::AccessDenied(msg) => write!(f, "Access denied: {}", msg),
            StorageError::InvalidPath(p) => write!(f, "Invalid path: {}", p),
            StorageError::UnsupportedScheme(s) => write!(f, "Unsupported scheme: {}", s),
            StorageError::NetworkError(e) => write!(f, "Network error: {}", e),
            StorageError::Timeout => write!(f, "Request timed out"),
            StorageError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            StorageError::QuotaExceeded => write!(f, "Storage quota exceeded"),
            StorageError::ServiceUnavailable => write!(f, "Service unavailable"),
            StorageError::Other(e) => write!(f, "Storage error: {}", e),
        }
    }
}

impl std::error::Error for StorageError {}

/// Cached object store wrapper
pub struct CachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: RwLock<HashMap<String, CacheEntry>>,
    max_cache_size: usize,
    ttl: Duration,
}

struct CacheEntry {
    data: Vec<u8>,
    cached_at: Instant,
    size: usize,
}

impl CachedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, max_cache_size: usize, ttl: Duration) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::new()),
            max_cache_size,
            ttl,
        }
    }

    fn cache_key(path: &ObjectPath) -> String {
        format!("{}/{}", path.bucket, path.key)
    }

    fn evict_if_needed(&self) {
        let mut cache = self.cache.write().unwrap();
        let total_size: usize = cache.values().map(|e| e.size).sum();

        if total_size > self.max_cache_size {
            // Simple LRU: remove oldest entries until under limit
            let mut entries: Vec<_> = cache.iter()
                .map(|(k, v)| (k.clone(), v.cached_at))
                .collect();
            entries.sort_by_key(|(_, t)| *t);

            let mut current_size = total_size;
            for (key, _) in entries {
                if current_size <= self.max_cache_size {
                    break;
                }
                if let Some(entry) = cache.remove(&key) {
                    current_size -= entry.size;
                }
            }
        }
    }
}

impl ObjectStore for CachedObjectStore {
    fn get(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        let key = Self::cache_key(path);

        // Check cache
        {
            let cache = self.cache.read().unwrap();
            if let Some(entry) = cache.get(&key) {
                if entry.cached_at.elapsed() < self.ttl {
                    return Ok(entry.data.clone());
                }
            }
        }

        // Fetch from inner store
        let data = self.inner.get(path)?;

        // Cache the result
        self.evict_if_needed();
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(key, CacheEntry {
                size: data.len(),
                data: data.clone(),
                cached_at: Instant::now(),
            });
        }

        Ok(data)
    }

    fn get_range(&self, path: &ObjectPath, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        // For ranges, just pass through (could optimize for cached full objects)
        self.inner.get_range(path, start, end)
    }

    fn put(&self, path: &ObjectPath, data: &[u8]) -> Result<(), StorageError> {
        // Invalidate cache on write
        let key = Self::cache_key(path);
        self.cache.write().unwrap().remove(&key);
        self.inner.put(path, data)
    }

    fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        let key = Self::cache_key(path);
        self.cache.write().unwrap().remove(&key);
        self.inner.delete(path)
    }

    fn exists(&self, path: &ObjectPath) -> Result<bool, StorageError> {
        self.inner.exists(path)
    }

    fn head(&self, path: &ObjectPath) -> Result<ObjectMeta, StorageError> {
        self.inner.head(path)
    }

    fn list(&self, path: &ObjectPath, continuation_token: Option<&str>) -> Result<ListResult, StorageError> {
        self.inner.list(path, continuation_token)
    }

    fn copy(&self, src: &ObjectPath, dst: &ObjectPath) -> Result<(), StorageError> {
        self.inner.copy(src, dst)
    }

    fn create_multipart(&self, path: &ObjectPath) -> Result<String, StorageError> {
        self.inner.create_multipart(path)
    }

    fn upload_part(&self, path: &ObjectPath, upload_id: &str, part_number: u32, data: &[u8]) -> Result<String, StorageError> {
        self.inner.upload_part(path, upload_id, part_number, data)
    }

    fn complete_multipart(&self, path: &ObjectPath, upload_id: &str, parts: &[UploadPart]) -> Result<(), StorageError> {
        let key = Self::cache_key(path);
        self.cache.write().unwrap().remove(&key);
        self.inner.complete_multipart(path, upload_id, parts)
    }

    fn abort_multipart(&self, path: &ObjectPath, upload_id: &str) -> Result<(), StorageError> {
        self.inner.abort_multipart(path, upload_id)
    }
}

// ============================================================================
// Parquet Import/Export
// ============================================================================

/// Parquet file metadata
#[derive(Debug, Clone)]
pub struct ParquetMetadata {
    /// Number of rows
    pub num_rows: u64,
    /// Number of row groups
    pub num_row_groups: usize,
    /// File size in bytes
    pub file_size: u64,
    /// Schema
    pub schema: ParquetSchema,
    /// Row group metadata
    pub row_groups: Vec<RowGroupMetadata>,
    /// Key-value metadata
    pub key_value_metadata: HashMap<String, String>,
    /// Created by
    pub created_by: Option<String>,
}

/// Parquet schema
#[derive(Debug, Clone)]
pub struct ParquetSchema {
    /// Column definitions
    pub columns: Vec<ParquetColumn>,
}

/// Parquet column definition
#[derive(Debug, Clone)]
pub struct ParquetColumn {
    /// Column name
    pub name: String,
    /// Physical type
    pub physical_type: ParquetPhysicalType,
    /// Logical type
    pub logical_type: Option<ParquetLogicalType>,
    /// Is nullable
    pub nullable: bool,
    /// Compression codec
    pub compression: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetPhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetLogicalType {
    String,
    Uuid,
    Timestamp { unit: TimeUnit, is_adjusted_to_utc: bool },
    Date,
    Time { unit: TimeUnit },
    Decimal { precision: u32, scale: u32 },
    List,
    Map,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

/// Row group metadata
#[derive(Debug, Clone)]
pub struct RowGroupMetadata {
    /// Number of rows in this group
    pub num_rows: u64,
    /// Total byte size
    pub total_byte_size: u64,
    /// Column chunks
    pub columns: Vec<ColumnChunkMetadata>,
}

/// Column chunk metadata
#[derive(Debug, Clone)]
pub struct ColumnChunkMetadata {
    /// Column name
    pub column: String,
    /// File offset
    pub file_offset: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Number of values
    pub num_values: u64,
    /// Statistics
    pub statistics: Option<ColumnStatistics>,
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Minimum value (encoded as bytes)
    pub min: Option<Vec<u8>>,
    /// Maximum value (encoded as bytes)
    pub max: Option<Vec<u8>>,
    /// Null count
    pub null_count: Option<u64>,
    /// Distinct count
    pub distinct_count: Option<u64>,
}

/// Parquet reader options
#[derive(Debug, Clone)]
pub struct ParquetReadOptions {
    /// Columns to read (None = all)
    pub columns: Option<Vec<String>>,
    /// Row groups to read (None = all)
    pub row_groups: Option<Vec<usize>>,
    /// Predicate for pushdown
    pub predicate: Option<ParquetPredicate>,
    /// Batch size for reading
    pub batch_size: usize,
    /// Enable parallel reading
    pub parallel: bool,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            columns: None,
            row_groups: None,
            predicate: None,
            batch_size: 8192,
            parallel: true,
        }
    }
}

/// Predicate for Parquet pushdown
#[derive(Debug, Clone)]
pub enum ParquetPredicate {
    /// Column equals value
    Eq { column: String, value: PredicateValue },
    /// Column not equals value
    Ne { column: String, value: PredicateValue },
    /// Column less than value
    Lt { column: String, value: PredicateValue },
    /// Column less than or equal value
    Le { column: String, value: PredicateValue },
    /// Column greater than value
    Gt { column: String, value: PredicateValue },
    /// Column greater than or equal value
    Ge { column: String, value: PredicateValue },
    /// Column is null
    IsNull { column: String },
    /// Column is not null
    IsNotNull { column: String },
    /// Column in list of values
    In { column: String, values: Vec<PredicateValue> },
    /// Logical AND
    And(Box<ParquetPredicate>, Box<ParquetPredicate>),
    /// Logical OR
    Or(Box<ParquetPredicate>, Box<ParquetPredicate>),
    /// Logical NOT
    Not(Box<ParquetPredicate>),
}

#[derive(Debug, Clone)]
pub enum PredicateValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// Parquet writer options
#[derive(Debug, Clone)]
pub struct ParquetWriteOptions {
    /// Compression codec
    pub compression: ParquetCompression,
    /// Row group size (number of rows)
    pub row_group_size: usize,
    /// Enable dictionary encoding
    pub dictionary_enabled: bool,
    /// Dictionary page size
    pub dictionary_page_size: usize,
    /// Data page size
    pub data_page_size: usize,
    /// Write statistics
    pub write_statistics: bool,
    /// Max statistics size
    pub max_statistics_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Brotli,
}

impl Default for ParquetWriteOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Snappy,
            row_group_size: 100_000,
            dictionary_enabled: true,
            dictionary_page_size: 1024 * 1024,
            data_page_size: 1024 * 1024,
            write_statistics: true,
            max_statistics_size: 4096,
        }
    }
}

/// Parquet file manager
pub struct ParquetManager {
    /// Object store for reading/writing
    store: Arc<dyn ObjectStore>,
}

impl ParquetManager {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Read Parquet metadata
    pub fn read_metadata(&self, path: &ObjectPath) -> Result<ParquetMetadata, ParquetError> {
        // In real implementation, would read footer from Parquet file
        // For now, return simulated metadata

        let meta = self.store.head(path)
            .map_err(|e| ParquetError::StorageError(e.to_string()))?;

        Ok(ParquetMetadata {
            num_rows: 1000,
            num_row_groups: 1,
            file_size: meta.size,
            schema: ParquetSchema {
                columns: vec![
                    ParquetColumn {
                        name: "id".to_string(),
                        physical_type: ParquetPhysicalType::Int64,
                        logical_type: None,
                        nullable: false,
                        compression: Some("snappy".to_string()),
                    },
                    ParquetColumn {
                        name: "name".to_string(),
                        physical_type: ParquetPhysicalType::ByteArray,
                        logical_type: Some(ParquetLogicalType::String),
                        nullable: true,
                        compression: Some("snappy".to_string()),
                    },
                ],
            },
            row_groups: vec![RowGroupMetadata {
                num_rows: 1000,
                total_byte_size: meta.size,
                columns: vec![],
            }],
            key_value_metadata: HashMap::new(),
            created_by: Some("BoyoDB".to_string()),
        })
    }

    /// Check if predicate can be pushed down based on statistics
    pub fn can_skip_row_group(
        &self,
        predicate: &ParquetPredicate,
        row_group: &RowGroupMetadata,
    ) -> bool {
        // Check if row group statistics allow skipping based on predicate
        match predicate {
            ParquetPredicate::Eq { column, value } => {
                if let Some(chunk) = row_group.columns.iter().find(|c| &c.column == column) {
                    if let Some(ref stats) = chunk.statistics {
                        // If min > value or max < value, can skip
                        return self.value_outside_range(value, &stats.min, &stats.max);
                    }
                }
                false
            }
            ParquetPredicate::Gt { column, value } => {
                if let Some(chunk) = row_group.columns.iter().find(|c| &c.column == column) {
                    if let Some(ref stats) = chunk.statistics {
                        // If max <= value, can skip
                        if let Some(ref max) = stats.max {
                            return self.compare_value_bytes(value, max) >= 0;
                        }
                    }
                }
                false
            }
            ParquetPredicate::Lt { column, value } => {
                if let Some(chunk) = row_group.columns.iter().find(|c| &c.column == column) {
                    if let Some(ref stats) = chunk.statistics {
                        // If min >= value, can skip
                        if let Some(ref min) = stats.min {
                            return self.compare_value_bytes(value, min) <= 0;
                        }
                    }
                }
                false
            }
            ParquetPredicate::And(left, right) => {
                self.can_skip_row_group(left, row_group) || self.can_skip_row_group(right, row_group)
            }
            ParquetPredicate::Or(left, right) => {
                self.can_skip_row_group(left, row_group) && self.can_skip_row_group(right, row_group)
            }
            _ => false,
        }
    }

    fn value_outside_range(&self, value: &PredicateValue, min: &Option<Vec<u8>>, max: &Option<Vec<u8>>) -> bool {
        if let (Some(min), Some(max)) = (min, max) {
            let cmp_min = self.compare_value_bytes(value, min);
            let cmp_max = self.compare_value_bytes(value, max);
            cmp_min < 0 || cmp_max > 0
        } else {
            false
        }
    }

    fn compare_value_bytes(&self, value: &PredicateValue, bytes: &[u8]) -> i32 {
        match value {
            PredicateValue::Int64(v) => {
                if bytes.len() >= 8 {
                    let b = i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8]));
                    v.cmp(&b) as i32
                } else {
                    0
                }
            }
            PredicateValue::String(s) => {
                s.as_bytes().cmp(bytes) as i32
            }
            _ => 0,
        }
    }

    /// Simulate reading data from Parquet (returns row count)
    pub fn read_data(&self, path: &ObjectPath, options: &ParquetReadOptions) -> Result<Vec<ParquetRow>, ParquetError> {
        let _data = self.store.get(path)
            .map_err(|e| ParquetError::StorageError(e.to_string()))?;

        // In real implementation, would decode Parquet format
        // Return simulated rows
        let mut rows = Vec::new();
        for i in 0..10 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), ParquetValue::Int64(i));
            values.insert("name".to_string(), ParquetValue::String(format!("row_{}", i)));
            rows.push(ParquetRow { values });
        }

        Ok(rows)
    }

    /// Write data to Parquet
    pub fn write_data(
        &self,
        path: &ObjectPath,
        schema: &ParquetSchema,
        rows: &[ParquetRow],
        options: &ParquetWriteOptions,
    ) -> Result<ParquetWriteResult, ParquetError> {
        // In real implementation, would encode to Parquet format
        // For now, simulate by writing a simple representation

        let mut data = Vec::new();

        // Write magic number
        data.extend_from_slice(b"PAR1");

        // Simulated data (in real impl, would write row groups, pages, etc.)
        for row in rows {
            for (col, value) in &row.values {
                data.extend_from_slice(col.as_bytes());
                data.push(b':');
                match value {
                    ParquetValue::Int64(v) => data.extend_from_slice(&v.to_le_bytes()),
                    ParquetValue::String(s) => data.extend_from_slice(s.as_bytes()),
                    _ => {}
                }
                data.push(b'\n');
            }
        }

        // Write footer magic
        data.extend_from_slice(b"PAR1");

        self.store.put(path, &data)
            .map_err(|e| ParquetError::StorageError(e.to_string()))?;

        Ok(ParquetWriteResult {
            rows_written: rows.len() as u64,
            bytes_written: data.len() as u64,
            row_groups_written: 1,
        })
    }
}

/// Parquet row (simplified)
#[derive(Debug, Clone)]
pub struct ParquetRow {
    pub values: HashMap<String, ParquetValue>,
}

/// Parquet value
#[derive(Debug, Clone)]
pub enum ParquetValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<ParquetValue>),
    Map(Vec<(ParquetValue, ParquetValue)>),
}

/// Result of writing Parquet
#[derive(Debug, Clone)]
pub struct ParquetWriteResult {
    pub rows_written: u64,
    pub bytes_written: u64,
    pub row_groups_written: usize,
}

#[derive(Debug)]
pub enum ParquetError {
    StorageError(String),
    SchemaError(String),
    DecodeError(String),
    EncodeError(String),
    UnsupportedType(String),
    Other(String),
}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetError::StorageError(e) => write!(f, "Storage error: {}", e),
            ParquetError::SchemaError(e) => write!(f, "Schema error: {}", e),
            ParquetError::DecodeError(e) => write!(f, "Decode error: {}", e),
            ParquetError::EncodeError(e) => write!(f, "Encode error: {}", e),
            ParquetError::UnsupportedType(t) => write!(f, "Unsupported type: {}", t),
            ParquetError::Other(e) => write!(f, "Parquet error: {}", e),
        }
    }
}

impl std::error::Error for ParquetError {}

// ============================================================================
// Dictionary Tables
// ============================================================================

/// Dictionary table for efficient dimension lookups
#[derive(Debug, Clone)]
pub struct DictionaryTable {
    /// Table name
    pub name: String,
    /// Key column name
    pub key_column: String,
    /// Value columns
    pub value_columns: Vec<String>,
    /// Data storage: key -> values
    data: HashMap<DictionaryKey, Vec<DictionaryValue>>,
    /// Source for lazy loading
    source: Option<DictionarySource>,
    /// Last refresh time
    last_refresh: u64,
    /// Refresh interval (0 = never auto-refresh)
    refresh_interval_secs: u64,
    /// Statistics
    stats: DictionaryStats,
}

/// Dictionary key type
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum DictionaryKey {
    Int64(i64),
    String(String),
    Composite(Vec<DictionaryKey>),
}

/// Dictionary value type
#[derive(Debug, Clone)]
pub enum DictionaryValue {
    Null,
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
}

/// Source for loading dictionary data
#[derive(Debug, Clone)]
pub enum DictionarySource {
    /// Load from a table
    Table {
        database: String,
        table: String,
    },
    /// Load from a file (CSV, Parquet, etc.)
    File {
        path: String,
        format: String,
    },
    /// Load from HTTP endpoint
    Http {
        url: String,
        format: String,
    },
    /// Load from object storage
    ObjectStorage {
        path: ObjectPath,
        format: String,
    },
}

/// Dictionary statistics
#[derive(Debug, Clone, Default)]
pub struct DictionaryStats {
    /// Number of entries
    pub entry_count: usize,
    /// Memory usage in bytes (estimated)
    pub memory_bytes: usize,
    /// Number of lookups
    pub lookup_count: u64,
    /// Number of hits
    pub hit_count: u64,
    /// Number of misses
    pub miss_count: u64,
}

impl DictionaryTable {
    pub fn new(name: &str, key_column: &str, value_columns: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            key_column: key_column.to_string(),
            value_columns,
            data: HashMap::new(),
            source: None,
            last_refresh: 0,
            refresh_interval_secs: 0,
            stats: DictionaryStats::default(),
        }
    }

    pub fn with_source(mut self, source: DictionarySource) -> Self {
        self.source = Some(source);
        self
    }

    pub fn with_refresh_interval(mut self, interval_secs: u64) -> Self {
        self.refresh_interval_secs = interval_secs;
        self
    }

    /// Insert a value
    pub fn insert(&mut self, key: DictionaryKey, values: Vec<DictionaryValue>) {
        self.data.insert(key, values);
        self.stats.entry_count = self.data.len();
        self.estimate_memory();
    }

    /// Lookup a value
    pub fn lookup(&mut self, key: &DictionaryKey) -> Option<&Vec<DictionaryValue>> {
        self.stats.lookup_count += 1;

        let result = self.data.get(key);
        if result.is_some() {
            self.stats.hit_count += 1;
        } else {
            self.stats.miss_count += 1;
        }
        result
    }

    /// Lookup with default
    pub fn lookup_or_default(&mut self, key: &DictionaryKey, default: Vec<DictionaryValue>) -> Vec<DictionaryValue> {
        self.lookup(key).cloned().unwrap_or(default)
    }

    /// Check if needs refresh
    pub fn needs_refresh(&self) -> bool {
        if self.refresh_interval_secs == 0 {
            return false;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now - self.last_refresh >= self.refresh_interval_secs
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.data.clear();
        self.stats.entry_count = 0;
        self.stats.memory_bytes = 0;
    }

    /// Get statistics
    pub fn stats(&self) -> &DictionaryStats {
        &self.stats
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        if self.stats.lookup_count > 0 {
            self.stats.hit_count as f64 / self.stats.lookup_count as f64
        } else {
            0.0
        }
    }

    fn estimate_memory(&mut self) {
        let mut size = 0usize;
        for (key, values) in &self.data {
            size += match key {
                DictionaryKey::Int64(_) => 8,
                DictionaryKey::String(s) => s.len() + 24,
                DictionaryKey::Composite(keys) => keys.len() * 32,
            };
            for v in values {
                size += match v {
                    DictionaryValue::Null => 1,
                    DictionaryValue::Int64(_) => 8,
                    DictionaryValue::Float64(_) => 8,
                    DictionaryValue::String(s) => s.len() + 24,
                    DictionaryValue::Bool(_) => 1,
                };
            }
        }
        self.stats.memory_bytes = size;
    }

    /// Refresh from source
    pub fn refresh(&mut self) -> Result<usize, DictionaryError> {
        let source = self.source.as_ref()
            .ok_or(DictionaryError::NoSource)?;

        // In real implementation, would load from source
        // For now, just update timestamp
        self.last_refresh = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(self.data.len())
    }
}

/// Dictionary manager for multiple dictionaries
pub struct DictionaryManager {
    dictionaries: RwLock<HashMap<String, DictionaryTable>>,
}

impl DictionaryManager {
    pub fn new() -> Self {
        Self {
            dictionaries: RwLock::new(HashMap::new()),
        }
    }

    /// Register a dictionary
    pub fn register(&self, dict: DictionaryTable) {
        self.dictionaries.write().unwrap()
            .insert(dict.name.clone(), dict);
    }

    /// Unregister a dictionary
    pub fn unregister(&self, name: &str) -> Option<DictionaryTable> {
        self.dictionaries.write().unwrap().remove(name)
    }

    /// Get a dictionary (for reading)
    pub fn get(&self, name: &str) -> Option<DictionaryTable> {
        self.dictionaries.read().unwrap().get(name).cloned()
    }

    /// Lookup in a dictionary
    pub fn lookup(&self, dict_name: &str, key: &DictionaryKey) -> Option<Vec<DictionaryValue>> {
        let mut dicts = self.dictionaries.write().unwrap();
        dicts.get_mut(dict_name)?.lookup(key).cloned()
    }

    /// List all dictionaries
    pub fn list(&self) -> Vec<String> {
        self.dictionaries.read().unwrap().keys().cloned().collect()
    }

    /// Refresh all dictionaries that need it
    pub fn refresh_all(&self) -> HashMap<String, Result<usize, DictionaryError>> {
        let mut results = HashMap::new();
        let mut dicts = self.dictionaries.write().unwrap();

        for (name, dict) in dicts.iter_mut() {
            if dict.needs_refresh() {
                results.insert(name.clone(), dict.refresh());
            }
        }

        results
    }
}

impl Default for DictionaryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum DictionaryError {
    NoSource,
    LoadError(String),
    KeyNotFound,
    TypeMismatch,
    Other(String),
}

impl std::fmt::Display for DictionaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DictionaryError::NoSource => write!(f, "Dictionary has no source"),
            DictionaryError::LoadError(e) => write!(f, "Failed to load dictionary: {}", e),
            DictionaryError::KeyNotFound => write!(f, "Key not found"),
            DictionaryError::TypeMismatch => write!(f, "Type mismatch"),
            DictionaryError::Other(e) => write!(f, "Dictionary error: {}", e),
        }
    }
}

impl std::error::Error for DictionaryError {}

// ============================================================================
// External Tables
// ============================================================================

/// External table type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalTableType {
    /// Files in object storage
    ObjectStorage,
    /// JDBC/database connection
    Database,
    /// HTTP/REST API
    Http,
    /// Kafka topic
    Kafka,
    /// Custom connector
    Custom(String),
}

/// External table definition
#[derive(Debug, Clone)]
pub struct ExternalTable {
    /// Table name
    pub name: String,
    /// Database
    pub database: String,
    /// External table type
    pub table_type: ExternalTableType,
    /// Schema
    pub schema: ExternalSchema,
    /// Connection/location settings
    pub location: ExternalLocation,
    /// Format settings
    pub format: ExternalFormat,
    /// Table options
    pub options: HashMap<String, String>,
    /// Created timestamp
    pub created_at: u64,
}

/// External table schema
#[derive(Debug, Clone)]
pub struct ExternalSchema {
    /// Columns
    pub columns: Vec<ExternalColumn>,
    /// Partition columns
    pub partition_columns: Vec<String>,
}

/// External column definition
#[derive(Debug, Clone)]
pub struct ExternalColumn {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: ExternalDataType,
    /// Is nullable
    pub nullable: bool,
    /// Comment
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalDataType {
    Boolean,
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
    Binary,
    Date,
    Timestamp,
    Decimal { precision: u8, scale: u8 },
    Array(Box<ExternalDataType>),
    Map(Box<ExternalDataType>, Box<ExternalDataType>),
    Struct(Vec<(String, ExternalDataType)>),
}

/// External location configuration
#[derive(Debug, Clone)]
pub struct ExternalLocation {
    /// URI pattern (can include wildcards)
    pub uri: String,
    /// Credential reference
    pub credential: Option<String>,
    /// Additional connection properties
    pub properties: HashMap<String, String>,
}

/// External format configuration
#[derive(Debug, Clone)]
pub struct ExternalFormat {
    /// Format type
    pub format_type: ExternalFormatType,
    /// Compression
    pub compression: Option<String>,
    /// Field delimiter (for CSV)
    pub field_delimiter: Option<char>,
    /// Quote character (for CSV)
    pub quote_char: Option<char>,
    /// Escape character
    pub escape_char: Option<char>,
    /// Skip header rows
    pub skip_header: usize,
    /// Null string representation
    pub null_string: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalFormatType {
    Parquet,
    Csv,
    Json,
    Avro,
    Orc,
    Text,
    Custom(String),
}

impl Default for ExternalFormat {
    fn default() -> Self {
        Self {
            format_type: ExternalFormatType::Parquet,
            compression: None,
            field_delimiter: Some(','),
            quote_char: Some('"'),
            escape_char: Some('\\'),
            skip_header: 0,
            null_string: None,
        }
    }
}

/// Query plan for external table
#[derive(Debug, Clone)]
pub struct ExternalTableScan {
    /// Table reference
    pub table: String,
    /// Columns to read
    pub columns: Vec<String>,
    /// Predicates to push down
    pub predicates: Vec<ExternalPredicate>,
    /// Limit
    pub limit: Option<u64>,
    /// Files/partitions to scan
    pub files: Vec<String>,
}

/// Predicate for external table pushdown
#[derive(Debug, Clone)]
pub enum ExternalPredicate {
    Eq { column: String, value: String },
    Ne { column: String, value: String },
    Lt { column: String, value: String },
    Le { column: String, value: String },
    Gt { column: String, value: String },
    Ge { column: String, value: String },
    In { column: String, values: Vec<String> },
    IsNull { column: String },
    IsNotNull { column: String },
    Like { column: String, pattern: String },
    And(Vec<ExternalPredicate>),
    Or(Vec<ExternalPredicate>),
}

/// External table manager
pub struct ExternalTableManager {
    /// Registered external tables
    tables: RwLock<HashMap<String, ExternalTable>>,
    /// Object store for accessing files
    store: Option<Arc<dyn ObjectStore>>,
}

impl ExternalTableManager {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            store: None,
        }
    }

    pub fn with_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Create an external table
    pub fn create_table(&self, table: ExternalTable) -> Result<(), ExternalTableError> {
        let key = format!("{}.{}", table.database, table.name);

        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&key) {
            return Err(ExternalTableError::AlreadyExists(key));
        }

        tables.insert(key, table);
        Ok(())
    }

    /// Drop an external table
    pub fn drop_table(&self, database: &str, name: &str) -> Result<ExternalTable, ExternalTableError> {
        let key = format!("{}.{}", database, name);

        self.tables.write().unwrap()
            .remove(&key)
            .ok_or_else(|| ExternalTableError::NotFound(key))
    }

    /// Get external table definition
    pub fn get_table(&self, database: &str, name: &str) -> Option<ExternalTable> {
        let key = format!("{}.{}", database, name);
        self.tables.read().unwrap().get(&key).cloned()
    }

    /// List external tables in a database
    pub fn list_tables(&self, database: &str) -> Vec<ExternalTable> {
        let prefix = format!("{}.", database);
        self.tables.read().unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Infer schema from external source
    pub fn infer_schema(&self, location: &ExternalLocation, format: &ExternalFormat) -> Result<ExternalSchema, ExternalTableError> {
        // In real implementation, would read sample data to infer schema
        // For now, return a placeholder schema

        Ok(ExternalSchema {
            columns: vec![
                ExternalColumn {
                    name: "id".to_string(),
                    data_type: ExternalDataType::Int64,
                    nullable: false,
                    comment: None,
                },
                ExternalColumn {
                    name: "data".to_string(),
                    data_type: ExternalDataType::String,
                    nullable: true,
                    comment: None,
                },
            ],
            partition_columns: vec![],
        })
    }

    /// List files matching external table location
    pub fn list_files(&self, table: &ExternalTable) -> Result<Vec<String>, ExternalTableError> {
        let path = ObjectPath::from_uri(&table.location.uri)
            .map_err(|e| ExternalTableError::InvalidLocation(e.to_string()))?;

        if let Some(ref store) = self.store {
            let result = store.list(&path, None)
                .map_err(|e| ExternalTableError::StorageError(e.to_string()))?;

            Ok(result.objects.iter().map(|o| o.path.to_uri("s3")).collect())
        } else {
            // Simulated file list
            Ok(vec![table.location.uri.clone()])
        }
    }

    /// Create scan plan for external table
    pub fn create_scan(
        &self,
        database: &str,
        name: &str,
        columns: Vec<String>,
        predicates: Vec<ExternalPredicate>,
        limit: Option<u64>,
    ) -> Result<ExternalTableScan, ExternalTableError> {
        let table = self.get_table(database, name)
            .ok_or_else(|| ExternalTableError::NotFound(format!("{}.{}", database, name)))?;

        let files = self.list_files(&table)?;

        Ok(ExternalTableScan {
            table: format!("{}.{}", database, name),
            columns,
            predicates,
            limit,
            files,
        })
    }

    /// Execute scan (simplified - returns row count)
    pub fn execute_scan(&self, scan: &ExternalTableScan) -> Result<Vec<HashMap<String, String>>, ExternalTableError> {
        // In real implementation, would read files and apply predicates
        // For now, return simulated results

        let mut results = Vec::new();
        for i in 0..10 {
            let mut row = HashMap::new();
            for col in &scan.columns {
                row.insert(col.clone(), format!("value_{}_{}", col, i));
            }
            results.push(row);
        }

        if let Some(limit) = scan.limit {
            results.truncate(limit as usize);
        }

        Ok(results)
    }
}

impl Default for ExternalTableManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum ExternalTableError {
    NotFound(String),
    AlreadyExists(String),
    InvalidLocation(String),
    StorageError(String),
    FormatError(String),
    SchemaError(String),
    AuthError(String),
    Other(String),
}

impl std::fmt::Display for ExternalTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExternalTableError::NotFound(t) => write!(f, "External table not found: {}", t),
            ExternalTableError::AlreadyExists(t) => write!(f, "External table already exists: {}", t),
            ExternalTableError::InvalidLocation(e) => write!(f, "Invalid location: {}", e),
            ExternalTableError::StorageError(e) => write!(f, "Storage error: {}", e),
            ExternalTableError::FormatError(e) => write!(f, "Format error: {}", e),
            ExternalTableError::SchemaError(e) => write!(f, "Schema error: {}", e),
            ExternalTableError::AuthError(e) => write!(f, "Authentication error: {}", e),
            ExternalTableError::Other(e) => write!(f, "External table error: {}", e),
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

    // Object Storage tests
    #[test]
    fn test_object_path_from_uri() {
        let path = ObjectPath::from_uri("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(path.bucket, "my-bucket");
        assert_eq!(path.key, "path/to/file.parquet");

        let path = ObjectPath::from_uri("gs://gcs-bucket/data/file.csv").unwrap();
        assert_eq!(path.bucket, "gcs-bucket");
        assert_eq!(path.key, "data/file.csv");
    }

    #[test]
    fn test_object_path_join() {
        let path = ObjectPath::new("bucket", "prefix");
        let joined = path.join("file.txt");
        assert_eq!(joined.key, "prefix/file.txt");

        let path2 = ObjectPath::new("bucket", "prefix/");
        let joined2 = path2.join("file.txt");
        assert_eq!(joined2.key, "prefix/file.txt");
    }

    #[test]
    fn test_in_memory_store_put_get() {
        let store = InMemoryObjectStore::new();
        let path = ObjectPath::new("test-bucket", "test-key");

        store.put(&path, b"hello world").unwrap();
        let data = store.get(&path).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_in_memory_store_exists() {
        let store = InMemoryObjectStore::new();
        let path = ObjectPath::new("bucket", "key");

        assert!(!store.exists(&path).unwrap());
        store.put(&path, b"data").unwrap();
        assert!(store.exists(&path).unwrap());
    }

    #[test]
    fn test_in_memory_store_delete() {
        let store = InMemoryObjectStore::new();
        let path = ObjectPath::new("bucket", "key");

        store.put(&path, b"data").unwrap();
        assert!(store.exists(&path).unwrap());

        store.delete(&path).unwrap();
        assert!(!store.exists(&path).unwrap());
    }

    #[test]
    fn test_in_memory_store_get_range() {
        let store = InMemoryObjectStore::new();
        let path = ObjectPath::new("bucket", "key");

        store.put(&path, b"hello world").unwrap();
        let data = store.get_range(&path, 0, 5).unwrap();
        assert_eq!(data, b"hello");

        let data2 = store.get_range(&path, 6, 11).unwrap();
        assert_eq!(data2, b"world");
    }

    #[test]
    fn test_in_memory_store_copy() {
        let store = InMemoryObjectStore::new();
        let src = ObjectPath::new("bucket", "src");
        let dst = ObjectPath::new("bucket", "dst");

        store.put(&src, b"original").unwrap();
        store.copy(&src, &dst).unwrap();

        assert_eq!(store.get(&dst).unwrap(), b"original");
    }

    #[test]
    fn test_in_memory_store_list() {
        let store = InMemoryObjectStore::new();

        store.put(&ObjectPath::new("bucket", "prefix/a.txt"), b"a").unwrap();
        store.put(&ObjectPath::new("bucket", "prefix/b.txt"), b"b").unwrap();
        store.put(&ObjectPath::new("bucket", "other/c.txt"), b"c").unwrap();

        let result = store.list(&ObjectPath::new("bucket", "prefix/"), None).unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[test]
    fn test_in_memory_store_multipart() {
        let store = InMemoryObjectStore::new();
        let path = ObjectPath::new("bucket", "large-file");

        let upload_id = store.create_multipart(&path).unwrap();

        let etag1 = store.upload_part(&path, &upload_id, 1, b"part1").unwrap();
        let etag2 = store.upload_part(&path, &upload_id, 2, b"part2").unwrap();

        let parts = vec![
            UploadPart { part_number: 1, etag: etag1, size: 5 },
            UploadPart { part_number: 2, etag: etag2, size: 5 },
        ];

        store.complete_multipart(&path, &upload_id, &parts).unwrap();

        let data = store.get(&path).unwrap();
        assert_eq!(data, b"part1part2");
    }

    #[test]
    fn test_cached_store() {
        let inner = Arc::new(InMemoryObjectStore::new());
        let cached = CachedObjectStore::new(inner.clone(), 1024 * 1024, Duration::from_secs(60));

        let path = ObjectPath::new("bucket", "key");
        inner.put(&path, b"cached data").unwrap();

        // First read - from inner
        let data1 = cached.get(&path).unwrap();
        assert_eq!(data1, b"cached data");

        // Second read - from cache
        let data2 = cached.get(&path).unwrap();
        assert_eq!(data2, b"cached data");
    }

    // Parquet tests
    #[test]
    fn test_parquet_write_options_default() {
        let options = ParquetWriteOptions::default();
        assert_eq!(options.compression, ParquetCompression::Snappy);
        assert!(options.dictionary_enabled);
    }

    #[test]
    fn test_parquet_manager_write_read() {
        let store = Arc::new(InMemoryObjectStore::new());
        let manager = ParquetManager::new(store);

        let path = ObjectPath::new("bucket", "test.parquet");
        let schema = ParquetSchema {
            columns: vec![
                ParquetColumn {
                    name: "id".to_string(),
                    physical_type: ParquetPhysicalType::Int64,
                    logical_type: None,
                    nullable: false,
                    compression: None,
                },
            ],
        };

        let rows = vec![
            ParquetRow {
                values: {
                    let mut m = HashMap::new();
                    m.insert("id".to_string(), ParquetValue::Int64(1));
                    m
                },
            },
        ];

        let result = manager.write_data(&path, &schema, &rows, &ParquetWriteOptions::default()).unwrap();
        assert_eq!(result.rows_written, 1);

        let read_rows = manager.read_data(&path, &ParquetReadOptions::default()).unwrap();
        assert!(!read_rows.is_empty());
    }

    #[test]
    fn test_parquet_predicate() {
        let predicate = ParquetPredicate::And(
            Box::new(ParquetPredicate::Gt {
                column: "id".to_string(),
                value: PredicateValue::Int64(0),
            }),
            Box::new(ParquetPredicate::Lt {
                column: "id".to_string(),
                value: PredicateValue::Int64(100),
            }),
        );

        match predicate {
            ParquetPredicate::And(_, _) => assert!(true),
            _ => panic!("Expected And predicate"),
        }
    }

    // Dictionary tests
    #[test]
    fn test_dictionary_table_insert_lookup() {
        let mut dict = DictionaryTable::new("countries", "code", vec!["name".to_string()]);

        dict.insert(
            DictionaryKey::String("US".to_string()),
            vec![DictionaryValue::String("United States".to_string())],
        );

        dict.insert(
            DictionaryKey::String("UK".to_string()),
            vec![DictionaryValue::String("United Kingdom".to_string())],
        );

        let result = dict.lookup(&DictionaryKey::String("US".to_string()));
        assert!(result.is_some());

        match &result.unwrap()[0] {
            DictionaryValue::String(s) => assert_eq!(s, "United States"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_dictionary_stats() {
        let mut dict = DictionaryTable::new("test", "id", vec!["value".to_string()]);

        dict.insert(DictionaryKey::Int64(1), vec![DictionaryValue::String("one".to_string())]);
        dict.insert(DictionaryKey::Int64(2), vec![DictionaryValue::String("two".to_string())]);

        // Lookup existing key
        dict.lookup(&DictionaryKey::Int64(1));
        // Lookup missing key
        dict.lookup(&DictionaryKey::Int64(99));

        let stats = dict.stats();
        assert_eq!(stats.entry_count, 2);
        assert_eq!(stats.lookup_count, 2);
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert!((dict.hit_rate() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_dictionary_manager() {
        let manager = DictionaryManager::new();

        let dict = DictionaryTable::new("users", "id", vec!["name".to_string()]);
        manager.register(dict);

        let names = manager.list();
        assert_eq!(names, vec!["users"]);

        let retrieved = manager.get("users");
        assert!(retrieved.is_some());

        manager.unregister("users");
        assert!(manager.get("users").is_none());
    }

    // External Table tests
    #[test]
    fn test_external_table_create() {
        let manager = ExternalTableManager::new();

        let table = ExternalTable {
            name: "logs".to_string(),
            database: "default".to_string(),
            table_type: ExternalTableType::ObjectStorage,
            schema: ExternalSchema {
                columns: vec![
                    ExternalColumn {
                        name: "timestamp".to_string(),
                        data_type: ExternalDataType::Timestamp,
                        nullable: false,
                        comment: None,
                    },
                    ExternalColumn {
                        name: "message".to_string(),
                        data_type: ExternalDataType::String,
                        nullable: true,
                        comment: None,
                    },
                ],
                partition_columns: vec![],
            },
            location: ExternalLocation {
                uri: "s3://logs-bucket/data/".to_string(),
                credential: None,
                properties: HashMap::new(),
            },
            format: ExternalFormat {
                format_type: ExternalFormatType::Parquet,
                ..Default::default()
            },
            options: HashMap::new(),
            created_at: 0,
        };

        manager.create_table(table).unwrap();

        let retrieved = manager.get_table("default", "logs").unwrap();
        assert_eq!(retrieved.name, "logs");
        assert_eq!(retrieved.schema.columns.len(), 2);
    }

    #[test]
    fn test_external_table_list() {
        let manager = ExternalTableManager::new();

        for i in 0..3 {
            let table = ExternalTable {
                name: format!("table_{}", i),
                database: "test_db".to_string(),
                table_type: ExternalTableType::ObjectStorage,
                schema: ExternalSchema { columns: vec![], partition_columns: vec![] },
                location: ExternalLocation {
                    uri: format!("s3://bucket/table_{}/", i),
                    credential: None,
                    properties: HashMap::new(),
                },
                format: ExternalFormat::default(),
                options: HashMap::new(),
                created_at: 0,
            };
            manager.create_table(table).unwrap();
        }

        let tables = manager.list_tables("test_db");
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn test_external_table_drop() {
        let manager = ExternalTableManager::new();

        let table = ExternalTable {
            name: "to_drop".to_string(),
            database: "db".to_string(),
            table_type: ExternalTableType::ObjectStorage,
            schema: ExternalSchema { columns: vec![], partition_columns: vec![] },
            location: ExternalLocation {
                uri: "s3://bucket/".to_string(),
                credential: None,
                properties: HashMap::new(),
            },
            format: ExternalFormat::default(),
            options: HashMap::new(),
            created_at: 0,
        };

        manager.create_table(table).unwrap();
        assert!(manager.get_table("db", "to_drop").is_some());

        manager.drop_table("db", "to_drop").unwrap();
        assert!(manager.get_table("db", "to_drop").is_none());
    }

    #[test]
    fn test_external_table_scan() {
        let store = Arc::new(InMemoryObjectStore::new());
        let manager = ExternalTableManager::new().with_store(store.clone());

        // Create a file in the store
        store.put(&ObjectPath::new("bucket", "data/file.parquet"), b"data").unwrap();

        let table = ExternalTable {
            name: "scan_test".to_string(),
            database: "db".to_string(),
            table_type: ExternalTableType::ObjectStorage,
            schema: ExternalSchema {
                columns: vec![
                    ExternalColumn {
                        name: "col1".to_string(),
                        data_type: ExternalDataType::String,
                        nullable: true,
                        comment: None,
                    },
                ],
                partition_columns: vec![],
            },
            location: ExternalLocation {
                uri: "s3://bucket/data/".to_string(),
                credential: None,
                properties: HashMap::new(),
            },
            format: ExternalFormat::default(),
            options: HashMap::new(),
            created_at: 0,
        };

        manager.create_table(table).unwrap();

        let scan = manager.create_scan(
            "db",
            "scan_test",
            vec!["col1".to_string()],
            vec![],
            Some(5),
        ).unwrap();

        assert_eq!(scan.columns, vec!["col1"]);
        assert_eq!(scan.limit, Some(5));

        let results = manager.execute_scan(&scan).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_external_data_types() {
        let array_type = ExternalDataType::Array(Box::new(ExternalDataType::Int64));
        let map_type = ExternalDataType::Map(
            Box::new(ExternalDataType::String),
            Box::new(ExternalDataType::Int64),
        );
        let struct_type = ExternalDataType::Struct(vec![
            ("field1".to_string(), ExternalDataType::Int32),
            ("field2".to_string(), ExternalDataType::String),
        ]);

        match array_type {
            ExternalDataType::Array(inner) => assert_eq!(*inner, ExternalDataType::Int64),
            _ => panic!("Expected Array type"),
        }

        match map_type {
            ExternalDataType::Map(k, v) => {
                assert_eq!(*k, ExternalDataType::String);
                assert_eq!(*v, ExternalDataType::Int64);
            }
            _ => panic!("Expected Map type"),
        }

        match struct_type {
            ExternalDataType::Struct(fields) => assert_eq!(fields.len(), 2),
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_external_predicate() {
        let pred = ExternalPredicate::And(vec![
            ExternalPredicate::Gt { column: "id".to_string(), value: "0".to_string() },
            ExternalPredicate::Lt { column: "id".to_string(), value: "100".to_string() },
        ]);

        match pred {
            ExternalPredicate::And(preds) => assert_eq!(preds.len(), 2),
            _ => panic!("Expected And predicate"),
        }
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert_eq!(config.provider, StorageProvider::S3);
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_infer_schema() {
        let manager = ExternalTableManager::new();

        let location = ExternalLocation {
            uri: "s3://bucket/data/".to_string(),
            credential: None,
            properties: HashMap::new(),
        };

        let schema = manager.infer_schema(&location, &ExternalFormat::default()).unwrap();
        assert!(!schema.columns.is_empty());
    }
}
