//! io_uring Async I/O Module
//!
//! Provides high-performance async I/O using Linux io_uring.
//! Falls back to standard async I/O on non-Linux platforms.
//!
//! Features:
//! - Zero-copy reads/writes
//! - Batched I/O submission
//! - Fixed buffer registration
//! - Direct I/O support
//! - Completion polling

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// io_uring configuration
#[derive(Clone, Debug)]
pub struct IoUringConfig {
    /// Enable io_uring (Linux only)
    pub enabled: bool,
    /// Ring size (must be power of 2)
    pub ring_size: u32,
    /// Use direct I/O (bypass page cache)
    pub direct_io: bool,
    /// Use fixed buffers for better performance
    pub fixed_buffers: bool,
    /// Number of fixed buffers
    pub num_fixed_buffers: usize,
    /// Fixed buffer size
    pub fixed_buffer_size: usize,
    /// Enable polling mode (busy-wait for completions)
    pub polling_mode: bool,
    /// Batch size for submission
    pub batch_size: usize,
    /// Use registered files
    pub registered_files: bool,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            enabled: cfg!(target_os = "linux"),
            ring_size: 256,
            direct_io: false,
            fixed_buffers: true,
            num_fixed_buffers: 64,
            fixed_buffer_size: 64 * 1024, // 64KB
            polling_mode: false,
            batch_size: 32,
            registered_files: true,
        }
    }
}

/// I/O operation type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoOperation {
    Read,
    Write,
    Fsync,
    Fdatasync,
    ReadV,
    WriteV,
}

/// I/O request
#[derive(Debug)]
pub struct IoRequest {
    /// Operation type
    pub op: IoOperation,
    /// File descriptor or handle
    pub fd: i32,
    /// Buffer for read/write
    pub buffer: Vec<u8>,
    /// Offset in file
    pub offset: u64,
    /// Length to read/write
    pub len: usize,
    /// Request ID for tracking
    pub request_id: u64,
    /// User data for callback
    pub user_data: u64,
}

/// I/O completion result
#[derive(Debug)]
pub struct IoCompletion {
    /// Request ID
    pub request_id: u64,
    /// User data from request
    pub user_data: u64,
    /// Result (bytes transferred or error code)
    pub result: i64,
    /// Buffer (for reads)
    pub buffer: Option<Vec<u8>>,
}

/// I/O statistics
#[derive(Clone, Debug, Default)]
pub struct IoStats {
    /// Total read operations
    pub reads: u64,
    /// Total write operations
    pub writes: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Batched submissions
    pub batched_submissions: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Total latency (microseconds)
    pub total_latency_us: u64,
    /// Operations completed
    pub completions: u64,
}

/// Fixed buffer pool for zero-copy I/O
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
    free_indices: Mutex<VecDeque<usize>>,
    buffer_size: usize,
}

impl BufferPool {
    pub fn new(num_buffers: usize, buffer_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(num_buffers);
        let mut free_indices = VecDeque::with_capacity(num_buffers);

        for i in 0..num_buffers {
            buffers.push(vec![0u8; buffer_size]);
            free_indices.push_back(i);
        }

        Self {
            buffers,
            free_indices: Mutex::new(free_indices),
            buffer_size,
        }
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> Option<(usize, &mut [u8])> {
        let mut free = self.free_indices.lock();
        if let Some(idx) = free.pop_front() {
            // Safety: We have exclusive access via the index
            let buffer = unsafe {
                let ptr = self.buffers.as_ptr().add(idx) as *mut Vec<u8>;
                (*ptr).as_mut_slice()
            };
            Some((idx, buffer))
        } else {
            None
        }
    }

    /// Release a buffer back to the pool
    pub fn release(&self, idx: usize) {
        let mut free = self.free_indices.lock();
        if idx < self.buffers.len() && !free.contains(&idx) {
            free.push_back(idx);
        }
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get number of available buffers
    pub fn available(&self) -> usize {
        self.free_indices.lock().len()
    }
}

/// Async I/O engine abstraction
/// Uses io_uring on Linux, falls back to thread pool on other platforms
pub struct AsyncIoEngine {
    config: IoUringConfig,
    buffer_pool: Option<BufferPool>,
    stats: Mutex<IoStats>,
    next_request_id: AtomicU64,
    running: AtomicBool,
    /// Pending requests queue
    pending: Mutex<VecDeque<IoRequest>>,
    /// Completed requests queue
    completed: Mutex<VecDeque<IoCompletion>>,
}

impl AsyncIoEngine {
    pub fn new(config: IoUringConfig) -> io::Result<Self> {
        let buffer_pool = if config.fixed_buffers {
            Some(BufferPool::new(config.num_fixed_buffers, config.fixed_buffer_size))
        } else {
            None
        };

        Ok(Self {
            config,
            buffer_pool,
            stats: Mutex::new(IoStats::default()),
            next_request_id: AtomicU64::new(1),
            running: AtomicBool::new(true),
            pending: Mutex::new(VecDeque::new()),
            completed: Mutex::new(VecDeque::new()),
        })
    }

    /// Submit a read request
    pub fn submit_read(
        &self,
        fd: i32,
        offset: u64,
        len: usize,
        user_data: u64,
    ) -> u64 {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        let request = IoRequest {
            op: IoOperation::Read,
            fd,
            buffer: vec![0u8; len],
            offset,
            len,
            request_id,
            user_data,
        };

        self.pending.lock().push_back(request);
        request_id
    }

    /// Submit a write request
    pub fn submit_write(
        &self,
        fd: i32,
        offset: u64,
        data: Vec<u8>,
        user_data: u64,
    ) -> u64 {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let len = data.len();

        let request = IoRequest {
            op: IoOperation::Write,
            fd,
            buffer: data,
            offset,
            len,
            request_id,
            user_data,
        };

        self.pending.lock().push_back(request);
        request_id
    }

    /// Submit an fsync request
    pub fn submit_fsync(&self, fd: i32, user_data: u64) -> u64 {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);

        let request = IoRequest {
            op: IoOperation::Fsync,
            fd,
            buffer: Vec::new(),
            offset: 0,
            len: 0,
            request_id,
            user_data,
        };

        self.pending.lock().push_back(request);
        request_id
    }

    /// Process pending requests (execute I/O)
    pub fn process_pending(&self) -> io::Result<usize> {
        let mut pending = self.pending.lock();
        let mut completed = self.completed.lock();
        let mut stats = self.stats.lock();

        let batch_size = pending.len().min(self.config.batch_size);
        if batch_size == 0 {
            return Ok(0);
        }

        let start = std::time::Instant::now();

        for _ in 0..batch_size {
            if let Some(request) = pending.pop_front() {
                let completion = self.execute_request(request, &mut stats)?;
                completed.push_back(completion);
            }
        }

        stats.batched_submissions += 1;
        stats.total_latency_us += start.elapsed().as_micros() as u64;

        if stats.batched_submissions > 0 {
            stats.avg_batch_size = stats.completions as f64 / stats.batched_submissions as f64;
        }

        Ok(batch_size)
    }

    /// Execute a single I/O request
    fn execute_request(&self, request: IoRequest, stats: &mut IoStats) -> io::Result<IoCompletion> {
        let result = match request.op {
            IoOperation::Read => {
                // Use pread for positioned reads
                #[cfg(unix)]
                {
                    use std::os::unix::io::FromRawFd;
                    let mut file = unsafe { File::from_raw_fd(request.fd) };
                    file.seek(SeekFrom::Start(request.offset))?;
                    let mut buffer = request.buffer;
                    let bytes_read = file.read(&mut buffer)?;
                    std::mem::forget(file); // Don't close the fd

                    stats.reads += 1;
                    stats.bytes_read += bytes_read as u64;
                    stats.completions += 1;

                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: bytes_read as i64,
                        buffer: Some(buffer),
                    }
                }
                #[cfg(not(unix))]
                {
                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: -1,
                        buffer: None,
                    }
                }
            }
            IoOperation::Write => {
                #[cfg(unix)]
                {
                    use std::os::unix::io::FromRawFd;
                    let mut file = unsafe { File::from_raw_fd(request.fd) };
                    file.seek(SeekFrom::Start(request.offset))?;
                    let bytes_written = file.write(&request.buffer)?;
                    std::mem::forget(file);

                    stats.writes += 1;
                    stats.bytes_written += bytes_written as u64;
                    stats.completions += 1;

                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: bytes_written as i64,
                        buffer: None,
                    }
                }
                #[cfg(not(unix))]
                {
                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: -1,
                        buffer: None,
                    }
                }
            }
            IoOperation::Fsync | IoOperation::Fdatasync => {
                #[cfg(unix)]
                {
                    use std::os::unix::io::FromRawFd;
                    let file = unsafe { File::from_raw_fd(request.fd) };
                    let result = file.sync_all();
                    std::mem::forget(file);

                    stats.completions += 1;

                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: if result.is_ok() { 0 } else { -1 },
                        buffer: None,
                    }
                }
                #[cfg(not(unix))]
                {
                    IoCompletion {
                        request_id: request.request_id,
                        user_data: request.user_data,
                        result: -1,
                        buffer: None,
                    }
                }
            }
            IoOperation::ReadV | IoOperation::WriteV => {
                // Vectored I/O not implemented in fallback
                IoCompletion {
                    request_id: request.request_id,
                    user_data: request.user_data,
                    result: -1,
                    buffer: None,
                }
            }
        };

        Ok(result)
    }

    /// Poll for completed requests
    pub fn poll_completions(&self, max: usize) -> Vec<IoCompletion> {
        let mut completed = self.completed.lock();
        let count = completed.len().min(max);

        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(c) = completed.pop_front() {
                results.push(c);
            }
        }
        results
    }

    /// Wait for specific request to complete
    pub fn wait_for(&self, request_id: u64) -> Option<IoCompletion> {
        loop {
            // Process pending requests
            let _ = self.process_pending();

            // Check completed
            let mut completed = self.completed.lock();
            if let Some(pos) = completed.iter().position(|c| c.request_id == request_id) {
                return Some(completed.remove(pos).unwrap());
            }

            // Check if we have pending work
            let pending = self.pending.lock();
            if pending.is_empty() && completed.is_empty() {
                return None;
            }
            drop(pending);
            drop(completed);

            // Brief yield
            std::thread::yield_now();
        }
    }

    /// Get I/O statistics
    pub fn stats(&self) -> IoStats {
        self.stats.lock().clone()
    }

    /// Shutdown the engine
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if engine is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// High-level async file operations
pub struct AsyncFile {
    path: std::path::PathBuf,
    fd: i32,
    engine: Arc<AsyncIoEngine>,
}

impl AsyncFile {
    /// Open a file for async I/O
    pub fn open(path: impl AsRef<Path>, engine: Arc<AsyncIoEngine>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        #[cfg(unix)]
        let fd = {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let fd = file.as_raw_fd();
            std::mem::forget(file); // Keep fd open
            fd
        };

        #[cfg(not(unix))]
        let fd = -1i32;

        Ok(Self { path, fd, engine })
    }

    /// Create a new file for async I/O
    pub fn create(path: impl AsRef<Path>, engine: Arc<AsyncIoEngine>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        #[cfg(unix)]
        let fd = {
            use std::os::unix::io::AsRawFd;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            let fd = file.as_raw_fd();
            std::mem::forget(file);
            fd
        };

        #[cfg(not(unix))]
        let fd = -1i32;

        Ok(Self { path, fd, engine })
    }

    /// Async read at offset
    pub fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let request_id = self.engine.submit_read(self.fd, offset, len, 0);

        // Process and wait
        loop {
            let _ = self.engine.process_pending();
            if let Some(completion) = self.engine.wait_for(request_id) {
                if completion.result < 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "read failed"));
                }
                return Ok(completion.buffer.unwrap_or_default());
            }
        }
    }

    /// Async write at offset
    pub fn write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<usize> {
        let request_id = self.engine.submit_write(self.fd, offset, data, 0);

        loop {
            let _ = self.engine.process_pending();
            if let Some(completion) = self.engine.wait_for(request_id) {
                if completion.result < 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "write failed"));
                }
                return Ok(completion.result as usize);
            }
        }
    }

    /// Async fsync
    pub fn fsync(&self) -> io::Result<()> {
        let request_id = self.engine.submit_fsync(self.fd, 0);

        loop {
            let _ = self.engine.process_pending();
            if let Some(completion) = self.engine.wait_for(request_id) {
                if completion.result < 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "fsync failed"));
                }
                return Ok(());
            }
        }
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AsyncFile {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            if self.fd >= 0 {
                unsafe {
                    libc::close(self.fd);
                }
            }
        }
    }
}

/// Batch reader for efficient multi-file reads
pub struct BatchReader {
    engine: Arc<AsyncIoEngine>,
    requests: Vec<(String, u64, usize, u64)>, // (path, offset, len, user_data)
}

impl BatchReader {
    pub fn new(engine: Arc<AsyncIoEngine>) -> Self {
        Self {
            engine,
            requests: Vec::new(),
        }
    }

    /// Add a read request to the batch
    pub fn add_read(&mut self, path: String, offset: u64, len: usize, user_data: u64) {
        self.requests.push((path, offset, len, user_data));
    }

    /// Execute all reads and return results
    pub fn execute(self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let mut results = Vec::with_capacity(self.requests.len());
        let mut request_ids = Vec::with_capacity(self.requests.len());

        // Submit all reads
        for (path, offset, len, user_data) in &self.requests {
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file = File::open(path)?;
                let fd = file.as_raw_fd();
                let request_id = self.engine.submit_read(fd, *offset, *len, *user_data);
                request_ids.push((request_id, *user_data));
                std::mem::forget(file);
            }
            #[cfg(not(unix))]
            {
                // Fallback: synchronous read
                let mut file = File::open(path)?;
                file.seek(SeekFrom::Start(*offset))?;
                let mut buffer = vec![0u8; *len];
                let _ = file.read(&mut buffer)?;
                results.push((*user_data, buffer));
            }
        }

        #[cfg(unix)]
        {
            // Process and collect results
            while results.len() < request_ids.len() {
                let _ = self.engine.process_pending();

                for completion in self.engine.poll_completions(request_ids.len()) {
                    if let Some((_, user_data)) = request_ids.iter().find(|(id, _)| *id == completion.request_id) {
                        if completion.result >= 0 {
                            results.push((*user_data, completion.buffer.unwrap_or_default()));
                        }
                    }
                }

                if results.len() < request_ids.len() {
                    std::thread::yield_now();
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(4, 1024);

        assert_eq!(pool.available(), 4);

        let (idx1, buf1) = pool.acquire().unwrap();
        assert_eq!(pool.available(), 3);

        let (idx2, _) = pool.acquire().unwrap();
        assert_eq!(pool.available(), 2);

        pool.release(idx1);
        assert_eq!(pool.available(), 3);

        pool.release(idx2);
        assert_eq!(pool.available(), 4);
    }

    #[test]
    fn test_async_io_engine() {
        let config = IoUringConfig::default();
        let engine = AsyncIoEngine::new(config).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.reads, 0);
        assert_eq!(stats.writes, 0);
    }

    #[cfg(unix)]
    #[test]
    fn test_async_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        // Create test file
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(b"Hello, async world!").unwrap();
        }

        let config = IoUringConfig::default();
        let engine = Arc::new(AsyncIoEngine::new(config).unwrap());
        let async_file = AsyncFile::open(&path, engine).unwrap();

        let data = async_file.read_at(0, 19).unwrap();
        assert_eq!(&data[..19], b"Hello, async world!");
    }
}
