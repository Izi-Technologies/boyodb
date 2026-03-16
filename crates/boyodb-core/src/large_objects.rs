// Large Objects - BLOB/CLOB support for BoyoDB
//
// Provides PostgreSQL-style large object support:
// - Binary Large Objects (BLOB) for binary data
// - Character Large Objects (CLOB) for text data
// - Chunked storage for efficient memory usage
// - Random access read/write
// - Integration with transactions

use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

// ============================================================================
// Types
// ============================================================================

/// Large Object Identifier
pub type Oid = u32;

/// Special OID value for invalid objects
pub const INVALID_OID: Oid = 0;

/// Default chunk size for large objects (256KB)
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Large object type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LargeObjectType {
    /// Binary Large Object
    Blob,
    /// Character Large Object (text)
    Clob,
}

impl Default for LargeObjectType {
    fn default() -> Self {
        LargeObjectType::Blob
    }
}

/// Access mode for large objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessMode {
    pub read: bool,
    pub write: bool,
}

impl AccessMode {
    pub const READ: Self = Self {
        read: true,
        write: false,
    };
    pub const WRITE: Self = Self {
        read: false,
        write: true,
    };
    pub const READ_WRITE: Self = Self {
        read: true,
        write: true,
    };

    pub fn is_readable(&self) -> bool {
        self.read
    }

    pub fn is_writable(&self) -> bool {
        self.write
    }
}

impl Default for AccessMode {
    fn default() -> Self {
        Self::READ_WRITE
    }
}

// ============================================================================
// Large Object Chunk
// ============================================================================

/// A chunk of large object data
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Chunk number (0-indexed)
    pub number: u32,
    /// Chunk data
    pub data: Vec<u8>,
}

impl Chunk {
    pub fn new(number: u32, data: Vec<u8>) -> Self {
        Self { number, data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// ============================================================================
// Large Object
// ============================================================================

/// Metadata for a large object
#[derive(Debug, Clone)]
pub struct LargeObjectMetadata {
    /// Object ID
    pub oid: Oid,
    /// Object type
    pub object_type: LargeObjectType,
    /// Total size in bytes
    pub size: u64,
    /// Chunk size used
    pub chunk_size: usize,
    /// Number of chunks
    pub chunk_count: u32,
    /// Owner user OID
    pub owner: u32,
    /// Creation time
    pub created_at: u64,
    /// Last modified time
    pub modified_at: u64,
    /// Optional description
    pub description: Option<String>,
    /// MIME type (for HTTP serving)
    pub mime_type: Option<String>,
}

/// A large object stored in chunks
#[derive(Debug)]
pub struct LargeObject {
    /// Metadata
    pub metadata: RwLock<LargeObjectMetadata>,
    /// Chunks indexed by number
    chunks: RwLock<HashMap<u32, Chunk>>,
}

impl LargeObject {
    /// Create a new empty large object
    pub fn new(oid: Oid, object_type: LargeObjectType, owner: u32) -> Self {
        let now = current_timestamp();
        Self {
            metadata: RwLock::new(LargeObjectMetadata {
                oid,
                object_type,
                size: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                chunk_count: 0,
                owner,
                created_at: now,
                modified_at: now,
                description: None,
                mime_type: None,
            }),
            chunks: RwLock::new(HashMap::new()),
        }
    }

    /// Get the OID
    pub fn oid(&self) -> Oid {
        self.metadata.read().oid
    }

    /// Get the total size
    pub fn size(&self) -> u64 {
        self.metadata.read().size
    }

    /// Get the object type
    pub fn object_type(&self) -> LargeObjectType {
        self.metadata.read().object_type
    }

    /// Write data to the large object at the specified offset
    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize, LobError> {
        let chunk_size = self.metadata.read().chunk_size;
        let mut written = 0;

        while written < data.len() {
            let current_offset = offset + written as u64;
            let chunk_number = (current_offset / chunk_size as u64) as u32;
            let chunk_offset = (current_offset % chunk_size as u64) as usize;

            // Get or create chunk
            let mut chunks = self.chunks.write();
            let chunk = chunks
                .entry(chunk_number)
                .or_insert_with(|| Chunk::new(chunk_number, vec![0u8; chunk_size]));

            // Calculate how much we can write to this chunk
            let available = chunk_size - chunk_offset;
            let to_write = std::cmp::min(available, data.len() - written);

            // Ensure chunk is large enough
            if chunk.data.len() < chunk_offset + to_write {
                chunk.data.resize(chunk_offset + to_write, 0);
            }

            // Write data
            chunk.data[chunk_offset..chunk_offset + to_write]
                .copy_from_slice(&data[written..written + to_write]);

            written += to_write;
        }

        // Update metadata
        {
            let mut metadata = self.metadata.write();
            let new_end = offset + data.len() as u64;
            if new_end > metadata.size {
                metadata.size = new_end;
                metadata.chunk_count =
                    ((metadata.size + chunk_size as u64 - 1) / chunk_size as u64) as u32;
            }
            metadata.modified_at = current_timestamp();
        }

        Ok(written)
    }

    /// Read data from the large object at the specified offset
    pub fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, LobError> {
        let metadata = self.metadata.read();

        if offset >= metadata.size {
            return Ok(Vec::new());
        }

        let actual_length = std::cmp::min(length, (metadata.size - offset) as usize);
        let chunk_size = metadata.chunk_size;
        drop(metadata);

        let mut result = Vec::with_capacity(actual_length);
        let mut remaining = actual_length;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_number = (current_offset / chunk_size as u64) as u32;
            let chunk_offset = (current_offset % chunk_size as u64) as usize;

            let chunks = self.chunks.read();
            let chunk_data = if let Some(chunk) = chunks.get(&chunk_number) {
                let available = chunk.data.len().saturating_sub(chunk_offset);
                let to_read = std::cmp::min(available, remaining);
                chunk.data[chunk_offset..chunk_offset + to_read].to_vec()
            } else {
                // Chunk doesn't exist, return zeros
                let to_read = std::cmp::min(chunk_size - chunk_offset, remaining);
                vec![0u8; to_read]
            };

            remaining -= chunk_data.len();
            current_offset += chunk_data.len() as u64;
            result.extend(chunk_data);
        }

        Ok(result)
    }

    /// Truncate the large object to a specified length
    pub fn truncate(&self, length: u64) -> Result<(), LobError> {
        let mut metadata = self.metadata.write();
        let chunk_size = metadata.chunk_size;

        if length == 0 {
            // Clear all chunks
            self.chunks.write().clear();
        } else if length < metadata.size {
            // Remove chunks beyond the new length
            let last_chunk = (length / chunk_size as u64) as u32;
            let mut chunks = self.chunks.write();
            chunks.retain(|&num, _| num <= last_chunk);

            // Truncate the last chunk if needed
            let chunk_offset = (length % chunk_size as u64) as usize;
            if chunk_offset > 0 {
                if let Some(chunk) = chunks.get_mut(&last_chunk) {
                    chunk.data.truncate(chunk_offset);
                }
            }
        }

        metadata.size = length;
        metadata.chunk_count = if length == 0 {
            0
        } else {
            ((length + chunk_size as u64 - 1) / chunk_size as u64) as u32
        };
        metadata.modified_at = current_timestamp();

        Ok(())
    }

    /// Get all data as a single vector
    pub fn get_all_data(&self) -> Result<Vec<u8>, LobError> {
        self.read_at(0, self.size() as usize)
    }

    /// Set all data from a single vector
    pub fn set_all_data(&self, data: &[u8]) -> Result<(), LobError> {
        // Clear existing data
        self.truncate(0)?;
        // Write new data
        self.write_at(0, data)?;
        Ok(())
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Large Object Handle
// ============================================================================

/// Handle for reading/writing a large object
pub struct LargeObjectHandle {
    lob: Arc<LargeObject>,
    mode: AccessMode,
    position: u64,
    closed: bool,
}

impl LargeObjectHandle {
    /// Create a new handle
    pub fn new(lob: Arc<LargeObject>, mode: AccessMode) -> Self {
        Self {
            lob,
            mode,
            position: 0,
            closed: false,
        }
    }

    /// Check if handle is closed
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Close the handle
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Get the current position
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get the large object OID
    pub fn oid(&self) -> Oid {
        self.lob.oid()
    }

    /// Tell (get current position) - alias for position()
    pub fn tell(&self) -> u64 {
        self.position
    }
}

impl Read for LargeObjectHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::other("Handle is closed"));
        }
        if !self.mode.is_readable() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Not opened for reading",
            ));
        }

        let data = self
            .lob
            .read_at(self.position, buf.len())
            .map_err(|e| io::Error::other(e.to_string()))?;

        let len = data.len();
        buf[..len].copy_from_slice(&data);
        self.position += len as u64;

        Ok(len)
    }
}

impl Write for LargeObjectHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::other("Handle is closed"));
        }
        if !self.mode.is_writable() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Not opened for writing",
            ));
        }

        let written = self
            .lob
            .write_at(self.position, buf)
            .map_err(|e| io::Error::other(e.to_string()))?;

        self.position += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Large objects are immediately persisted
        Ok(())
    }
}

impl Seek for LargeObjectHandle {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if self.closed {
            return Err(io::Error::other("Handle is closed"));
        }

        let size = self.lob.size();
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position.saturating_add(offset as u64)
                } else {
                    self.position.saturating_sub((-offset) as u64)
                }
            }
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    size.saturating_add(offset as u64)
                } else {
                    size.saturating_sub((-offset) as u64)
                }
            }
        };

        self.position = new_pos;
        Ok(self.position)
    }
}

// ============================================================================
// Large Object Manager
// ============================================================================

/// Manages large objects
pub struct LargeObjectManager {
    /// All large objects by OID
    objects: RwLock<HashMap<Oid, Arc<LargeObject>>>,
    /// Next available OID
    next_oid: AtomicU64,
    /// Open handles by descriptor
    handles: RwLock<HashMap<i32, LargeObjectHandle>>,
    /// Next available descriptor
    next_descriptor: AtomicU64,
}

impl LargeObjectManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            next_oid: AtomicU64::new(16384), // Start after system OIDs
            handles: RwLock::new(HashMap::new()),
            next_descriptor: AtomicU64::new(0),
        }
    }

    /// Create a new large object
    pub fn create(&self, object_type: LargeObjectType, owner: u32) -> Result<Oid, LobError> {
        let oid = self.next_oid.fetch_add(1, Ordering::SeqCst) as Oid;
        let lob = Arc::new(LargeObject::new(oid, object_type, owner));

        self.objects.write().insert(oid, lob);
        Ok(oid)
    }

    /// Create a new BLOB
    pub fn create_blob(&self, owner: u32) -> Result<Oid, LobError> {
        self.create(LargeObjectType::Blob, owner)
    }

    /// Create a new CLOB
    pub fn create_clob(&self, owner: u32) -> Result<Oid, LobError> {
        self.create(LargeObjectType::Clob, owner)
    }

    /// Drop a large object
    pub fn drop(&self, oid: Oid) -> Result<(), LobError> {
        // Check if any handles are open
        let handles = self.handles.read();
        for handle in handles.values() {
            if handle.oid() == oid && !handle.is_closed() {
                return Err(LobError::ObjectInUse(oid));
            }
        }
        drop(handles);

        let mut objects = self.objects.write();
        if objects.remove(&oid).is_none() {
            return Err(LobError::NotFound(oid));
        }
        Ok(())
    }

    /// Get a large object by OID
    pub fn get(&self, oid: Oid) -> Option<Arc<LargeObject>> {
        self.objects.read().get(&oid).cloned()
    }

    /// Check if a large object exists
    pub fn exists(&self, oid: Oid) -> bool {
        self.objects.read().contains_key(&oid)
    }

    /// Open a large object for access
    pub fn open(&self, oid: Oid, mode: AccessMode) -> Result<i32, LobError> {
        let lob = self.get(oid).ok_or(LobError::NotFound(oid))?;

        let descriptor = self.next_descriptor.fetch_add(1, Ordering::SeqCst) as i32;
        let handle = LargeObjectHandle::new(lob, mode);

        self.handles.write().insert(descriptor, handle);
        Ok(descriptor)
    }

    /// Close a large object handle
    pub fn close(&self, descriptor: i32) -> Result<(), LobError> {
        let mut handles = self.handles.write();
        if let Some(mut handle) = handles.remove(&descriptor) {
            handle.close();
            Ok(())
        } else {
            Err(LobError::InvalidDescriptor(descriptor))
        }
    }

    /// Read from an open large object
    pub fn read(&self, descriptor: i32, length: usize) -> Result<Vec<u8>, LobError> {
        let mut handles = self.handles.write();
        let handle = handles
            .get_mut(&descriptor)
            .ok_or(LobError::InvalidDescriptor(descriptor))?;

        let mut buf = vec![0u8; length];
        let n = handle
            .read(&mut buf)
            .map_err(|e| LobError::IoError(e.to_string()))?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Write to an open large object
    pub fn write(&self, descriptor: i32, data: &[u8]) -> Result<usize, LobError> {
        let mut handles = self.handles.write();
        let handle = handles
            .get_mut(&descriptor)
            .ok_or(LobError::InvalidDescriptor(descriptor))?;

        handle
            .write(data)
            .map_err(|e| LobError::IoError(e.to_string()))
    }

    /// Seek in an open large object
    pub fn seek(&self, descriptor: i32, offset: i64, whence: i32) -> Result<u64, LobError> {
        let mut handles = self.handles.write();
        let handle = handles
            .get_mut(&descriptor)
            .ok_or(LobError::InvalidDescriptor(descriptor))?;

        let seek_from = match whence {
            0 => SeekFrom::Start(offset as u64),
            1 => SeekFrom::Current(offset),
            2 => SeekFrom::End(offset),
            _ => return Err(LobError::InvalidWhence(whence)),
        };

        handle
            .seek(seek_from)
            .map_err(|e| LobError::IoError(e.to_string()))
    }

    /// Get the current position in an open large object
    pub fn tell(&self, descriptor: i32) -> Result<u64, LobError> {
        let handles = self.handles.read();
        let handle = handles
            .get(&descriptor)
            .ok_or(LobError::InvalidDescriptor(descriptor))?;
        Ok(handle.tell())
    }

    /// Truncate an open large object
    pub fn truncate(&self, descriptor: i32, length: u64) -> Result<(), LobError> {
        let handles = self.handles.read();
        let handle = handles
            .get(&descriptor)
            .ok_or(LobError::InvalidDescriptor(descriptor))?;

        if !handle.mode.is_writable() {
            return Err(LobError::NotWritable);
        }

        handle.lob.truncate(length)
    }

    /// List all large objects
    pub fn list(&self) -> Vec<LargeObjectMetadata> {
        self.objects
            .read()
            .values()
            .map(|lob| lob.metadata.read().clone())
            .collect()
    }

    /// Get large object metadata
    pub fn get_metadata(&self, oid: Oid) -> Option<LargeObjectMetadata> {
        self.get(oid).map(|lob| lob.metadata.read().clone())
    }

    /// Update large object metadata
    pub fn update_metadata<F>(&self, oid: Oid, f: F) -> Result<(), LobError>
    where
        F: FnOnce(&mut LargeObjectMetadata),
    {
        let lob = self.get(oid).ok_or(LobError::NotFound(oid))?;
        let mut metadata = lob.metadata.write();
        f(&mut metadata);
        Ok(())
    }

    /// Import data from bytes into a new large object
    pub fn import(&self, data: &[u8], owner: u32) -> Result<Oid, LobError> {
        let oid = self.create_blob(owner)?;
        let lob = self.get(oid).unwrap();
        lob.set_all_data(data)?;
        Ok(oid)
    }

    /// Export large object data to bytes
    pub fn export(&self, oid: Oid) -> Result<Vec<u8>, LobError> {
        let lob = self.get(oid).ok_or(LobError::NotFound(oid))?;
        lob.get_all_data()
    }
}

impl Default for LargeObjectManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PostgreSQL-compatible Functions
// ============================================================================

/// PostgreSQL-compatible large object functions
impl LargeObjectManager {
    /// lo_create - Create a new large object with specified OID
    pub fn lo_create(&self, requested_oid: Oid) -> Result<Oid, LobError> {
        let oid = if requested_oid == 0 {
            self.next_oid.fetch_add(1, Ordering::SeqCst) as Oid
        } else {
            if self.exists(requested_oid) {
                return Err(LobError::AlreadyExists(requested_oid));
            }
            // Update next_oid if needed
            let next = self.next_oid.load(Ordering::SeqCst) as Oid;
            if requested_oid >= next {
                self.next_oid
                    .store((requested_oid + 1) as u64, Ordering::SeqCst);
            }
            requested_oid
        };

        let lob = Arc::new(LargeObject::new(oid, LargeObjectType::Blob, 0));
        self.objects.write().insert(oid, lob);
        Ok(oid)
    }

    /// lo_unlink - Delete a large object
    pub fn lo_unlink(&self, oid: Oid) -> Result<(), LobError> {
        self.drop(oid)
    }

    /// lo_open - Open a large object
    pub fn lo_open(&self, oid: Oid, mode: i32) -> Result<i32, LobError> {
        let access_mode = match mode {
            0x20000 => AccessMode::READ,  // INV_READ
            0x40000 => AccessMode::WRITE, // INV_WRITE
            0x60000 => AccessMode::READ_WRITE,
            _ => AccessMode::READ_WRITE,
        };
        self.open(oid, access_mode)
    }

    /// lo_close - Close a large object descriptor
    pub fn lo_close(&self, fd: i32) -> Result<(), LobError> {
        self.close(fd)
    }

    /// lo_read - Read from a large object
    pub fn lo_read(&self, fd: i32, len: usize) -> Result<Vec<u8>, LobError> {
        self.read(fd, len)
    }

    /// lo_write - Write to a large object
    pub fn lo_write(&self, fd: i32, data: &[u8]) -> Result<usize, LobError> {
        self.write(fd, data)
    }

    /// lo_lseek - Seek in a large object
    pub fn lo_lseek(&self, fd: i32, offset: i64, whence: i32) -> Result<u64, LobError> {
        self.seek(fd, offset, whence)
    }

    /// lo_tell - Get current position
    pub fn lo_tell(&self, fd: i32) -> Result<u64, LobError> {
        self.tell(fd)
    }

    /// lo_truncate - Truncate a large object
    pub fn lo_truncate(&self, fd: i32, len: u64) -> Result<(), LobError> {
        self.truncate(fd, len)
    }

    /// lo_import - Import file as large object (simplified - takes bytes)
    pub fn lo_import(&self, data: &[u8]) -> Result<Oid, LobError> {
        self.import(data, 0)
    }

    /// lo_export - Export large object (simplified - returns bytes)
    pub fn lo_export(&self, oid: Oid) -> Result<Vec<u8>, LobError> {
        self.export(oid)
    }

    /// lo_get - Get entire large object data
    pub fn lo_get(&self, oid: Oid) -> Result<Vec<u8>, LobError> {
        self.export(oid)
    }

    /// lo_get with offset and length
    pub fn lo_get_range(&self, oid: Oid, offset: u64, length: usize) -> Result<Vec<u8>, LobError> {
        let lob = self.get(oid).ok_or(LobError::NotFound(oid))?;
        lob.read_at(offset, length)
    }

    /// lo_put - Replace large object data
    pub fn lo_put(&self, oid: Oid, data: &[u8]) -> Result<(), LobError> {
        let lob = self.get(oid).ok_or(LobError::NotFound(oid))?;
        lob.set_all_data(data)
    }

    /// lo_from_bytea - Create large object from bytea
    pub fn lo_from_bytea(&self, data: &[u8]) -> Result<Oid, LobError> {
        self.import(data, 0)
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum LobError {
    NotFound(Oid),
    AlreadyExists(Oid),
    ObjectInUse(Oid),
    InvalidDescriptor(i32),
    InvalidWhence(i32),
    NotReadable,
    NotWritable,
    IoError(String),
    SizeExceeded { limit: u64, requested: u64 },
    InvalidData(String),
}

impl std::fmt::Display for LobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(oid) => write!(f, "Large object {} not found", oid),
            Self::AlreadyExists(oid) => write!(f, "Large object {} already exists", oid),
            Self::ObjectInUse(oid) => write!(f, "Large object {} is in use", oid),
            Self::InvalidDescriptor(fd) => write!(f, "Invalid large object descriptor: {}", fd),
            Self::InvalidWhence(w) => write!(f, "Invalid seek whence: {}", w),
            Self::NotReadable => write!(f, "Large object not opened for reading"),
            Self::NotWritable => write!(f, "Large object not opened for writing"),
            Self::IoError(msg) => write!(f, "I/O error: {}", msg),
            Self::SizeExceeded { limit, requested } => {
                write!(f, "Size exceeded: limit={}, requested={}", limit, requested)
            }
            Self::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
        }
    }
}

impl std::error::Error for LobError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_creation() {
        let chunk = Chunk::new(0, vec![1, 2, 3, 4, 5]);
        assert_eq!(chunk.number, 0);
        assert_eq!(chunk.len(), 5);
        assert!(!chunk.is_empty());

        let empty_chunk = Chunk::new(1, Vec::new());
        assert!(empty_chunk.is_empty());
    }

    #[test]
    fn test_access_modes() {
        assert!(AccessMode::READ.is_readable());
        assert!(!AccessMode::READ.is_writable());

        assert!(!AccessMode::WRITE.is_readable());
        assert!(AccessMode::WRITE.is_writable());

        assert!(AccessMode::READ_WRITE.is_readable());
        assert!(AccessMode::READ_WRITE.is_writable());
    }

    #[test]
    fn test_large_object_creation() {
        let lob = LargeObject::new(1234, LargeObjectType::Blob, 100);
        assert_eq!(lob.oid(), 1234);
        assert_eq!(lob.size(), 0);
        assert_eq!(lob.object_type(), LargeObjectType::Blob);
    }

    #[test]
    fn test_large_object_write_read() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);

        // Write some data
        let data = b"Hello, Large Objects!";
        lob.write_at(0, data).unwrap();
        assert_eq!(lob.size(), data.len() as u64);

        // Read it back
        let read_data = lob.read_at(0, data.len()).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_large_object_partial_read() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);
        lob.write_at(0, b"Hello World").unwrap();

        // Read partial
        let partial = lob.read_at(6, 5).unwrap();
        assert_eq!(partial, b"World");
    }

    #[test]
    fn test_large_object_overwrite() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);
        lob.write_at(0, b"Hello World").unwrap();

        // Overwrite middle
        lob.write_at(6, b"Rust!").unwrap();

        let data = lob.get_all_data().unwrap();
        assert_eq!(data, b"Hello Rust!");
    }

    #[test]
    fn test_large_object_truncate() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);
        lob.write_at(0, b"Hello World").unwrap();
        assert_eq!(lob.size(), 11);

        lob.truncate(5).unwrap();
        assert_eq!(lob.size(), 5);

        let data = lob.get_all_data().unwrap();
        assert_eq!(data, b"Hello");
    }

    #[test]
    fn test_large_object_truncate_zero() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);
        lob.write_at(0, b"Hello World").unwrap();

        lob.truncate(0).unwrap();
        assert_eq!(lob.size(), 0);

        let data = lob.get_all_data().unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_large_object_chunks() {
        let lob = LargeObject::new(1, LargeObjectType::Blob, 0);

        // Write more than one chunk
        let large_data = vec![42u8; DEFAULT_CHUNK_SIZE + 1000];
        lob.write_at(0, &large_data).unwrap();

        assert_eq!(lob.size(), large_data.len() as u64);

        let read_data = lob.get_all_data().unwrap();
        assert_eq!(read_data, large_data);
    }

    #[test]
    fn test_large_object_handle() {
        let lob = Arc::new(LargeObject::new(1, LargeObjectType::Blob, 0));
        lob.write_at(0, b"Test data").unwrap();

        let mut handle = LargeObjectHandle::new(lob.clone(), AccessMode::READ_WRITE);
        assert!(!handle.is_closed());
        assert_eq!(handle.position(), 0);

        // Read
        let mut buf = [0u8; 4];
        let n = handle.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"Test");
        assert_eq!(handle.position(), 4);

        // Seek
        handle.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(handle.position(), 0);

        handle.close();
        assert!(handle.is_closed());
    }

    #[test]
    fn test_manager_create_drop() {
        let manager = LargeObjectManager::new();

        let oid = manager.create_blob(0).unwrap();
        assert!(manager.exists(oid));

        manager.drop(oid).unwrap();
        assert!(!manager.exists(oid));
    }

    #[test]
    fn test_manager_open_close() {
        let manager = LargeObjectManager::new();

        let oid = manager.create_blob(0).unwrap();
        let fd = manager.open(oid, AccessMode::READ_WRITE).unwrap();

        // Write some data
        manager.write(fd, b"Hello").unwrap();

        // Seek back and read
        manager.seek(fd, 0, 0).unwrap();
        let data = manager.read(fd, 5).unwrap();
        assert_eq!(data, b"Hello");

        manager.close(fd).unwrap();
    }

    #[test]
    fn test_manager_import_export() {
        let manager = LargeObjectManager::new();

        let data = b"Imported data!";
        let oid = manager.import(data, 0).unwrap();

        let exported = manager.export(oid).unwrap();
        assert_eq!(exported, data);
    }

    #[test]
    fn test_manager_list() {
        let manager = LargeObjectManager::new();

        manager.create_blob(0).unwrap();
        manager.create_clob(0).unwrap();
        manager.create_blob(0).unwrap();

        let list = manager.list();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_manager_metadata() {
        let manager = LargeObjectManager::new();

        let oid = manager.create_blob(100).unwrap();

        let metadata = manager.get_metadata(oid).unwrap();
        assert_eq!(metadata.oid, oid);
        assert_eq!(metadata.owner, 100);
        assert_eq!(metadata.object_type, LargeObjectType::Blob);

        manager
            .update_metadata(oid, |m| {
                m.description = Some("Test object".to_string());
                m.mime_type = Some("application/octet-stream".to_string());
            })
            .unwrap();

        let metadata = manager.get_metadata(oid).unwrap();
        assert_eq!(metadata.description, Some("Test object".to_string()));
    }

    #[test]
    fn test_lo_create() {
        let manager = LargeObjectManager::new();

        // Auto OID
        let oid1 = manager.lo_create(0).unwrap();
        assert!(oid1 > 0);

        // Specific OID
        let oid2 = manager.lo_create(99999).unwrap();
        assert_eq!(oid2, 99999);

        // Duplicate should fail
        let result = manager.lo_create(99999);
        assert!(matches!(result, Err(LobError::AlreadyExists(99999))));
    }

    #[test]
    fn test_lo_open_close() {
        let manager = LargeObjectManager::new();

        let oid = manager.lo_create(0).unwrap();
        let fd = manager.lo_open(oid, 0x60000).unwrap(); // INV_READ | INV_WRITE

        assert!(manager.lo_close(fd).is_ok());
    }

    #[test]
    fn test_lo_read_write() {
        let manager = LargeObjectManager::new();

        let oid = manager.lo_create(0).unwrap();
        let fd = manager.lo_open(oid, 0x60000).unwrap();

        manager.lo_write(fd, b"PostgreSQL").unwrap();
        manager.lo_lseek(fd, 0, 0).unwrap();

        let data = manager.lo_read(fd, 10).unwrap();
        assert_eq!(data, b"PostgreSQL");

        manager.lo_close(fd).unwrap();
    }

    #[test]
    fn test_lo_truncate() {
        let manager = LargeObjectManager::new();

        let oid = manager.lo_create(0).unwrap();
        let fd = manager.lo_open(oid, 0x60000).unwrap();

        manager.lo_write(fd, b"Hello World").unwrap();
        manager.lo_truncate(fd, 5).unwrap();

        manager.lo_lseek(fd, 0, 0).unwrap();
        let data = manager.lo_read(fd, 100).unwrap();
        assert_eq!(data, b"Hello");

        manager.lo_close(fd).unwrap();
    }

    #[test]
    fn test_lo_get_put() {
        let manager = LargeObjectManager::new();

        let oid = manager.lo_create(0).unwrap();
        manager.lo_put(oid, b"Quick data").unwrap();

        let data = manager.lo_get(oid).unwrap();
        assert_eq!(data, b"Quick data");
    }

    #[test]
    fn test_lo_get_range() {
        let manager = LargeObjectManager::new();

        let oid = manager.lo_create(0).unwrap();
        manager.lo_put(oid, b"Hello World").unwrap();

        let data = manager.lo_get_range(oid, 6, 5).unwrap();
        assert_eq!(data, b"World");
    }

    #[test]
    fn test_lo_from_bytea() {
        let manager = LargeObjectManager::new();

        let data = b"Binary data from bytea";
        let oid = manager.lo_from_bytea(data).unwrap();

        let retrieved = manager.lo_get(oid).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            LobError::NotFound(123),
            LobError::AlreadyExists(456),
            LobError::ObjectInUse(789),
            LobError::InvalidDescriptor(42),
            LobError::InvalidWhence(99),
            LobError::NotReadable,
            LobError::NotWritable,
            LobError::IoError("test".to_string()),
            LobError::SizeExceeded {
                limit: 1000,
                requested: 2000,
            },
            LobError::InvalidData("bad".to_string()),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_clob_type() {
        let manager = LargeObjectManager::new();

        let oid = manager.create_clob(0).unwrap();
        let metadata = manager.get_metadata(oid).unwrap();
        assert_eq!(metadata.object_type, LargeObjectType::Clob);
    }

    #[test]
    fn test_handle_seek_modes() {
        let lob = Arc::new(LargeObject::new(1, LargeObjectType::Blob, 0));
        lob.write_at(0, b"0123456789").unwrap();

        let mut handle = LargeObjectHandle::new(lob, AccessMode::READ_WRITE);

        // Seek from start
        handle.seek(SeekFrom::Start(5)).unwrap();
        assert_eq!(handle.position(), 5);

        // Seek from current (forward)
        handle.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(handle.position(), 7);

        // Seek from current (backward)
        handle.seek(SeekFrom::Current(-3)).unwrap();
        assert_eq!(handle.position(), 4);

        // Seek from end
        handle.seek(SeekFrom::End(-2)).unwrap();
        assert_eq!(handle.position(), 8);
    }

    #[test]
    fn test_closed_handle_operations() {
        let lob = Arc::new(LargeObject::new(1, LargeObjectType::Blob, 0));
        let mut handle = LargeObjectHandle::new(lob, AccessMode::READ_WRITE);
        handle.close();

        let mut buf = [0u8; 10];
        assert!(handle.read(&mut buf).is_err());
        assert!(handle.write(b"test").is_err());
        assert!(handle.seek(SeekFrom::Start(0)).is_err());
    }

    #[test]
    fn test_read_only_handle() {
        let lob = Arc::new(LargeObject::new(1, LargeObjectType::Blob, 0));
        lob.write_at(0, b"data").unwrap();

        let mut handle = LargeObjectHandle::new(lob, AccessMode::READ);

        // Read should work
        let mut buf = [0u8; 4];
        assert!(handle.read(&mut buf).is_ok());

        // Write should fail
        assert!(handle.write(b"test").is_err());
    }

    #[test]
    fn test_write_only_handle() {
        let lob = Arc::new(LargeObject::new(1, LargeObjectType::Blob, 0));
        let mut handle = LargeObjectHandle::new(lob, AccessMode::WRITE);

        // Write should work
        assert!(handle.write(b"test").is_ok());

        // Read should fail
        let mut buf = [0u8; 4];
        assert!(handle.read(&mut buf).is_err());
    }
}
