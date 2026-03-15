//! Asynchronous Inserts
//!
//! Buffer small inserts for batch efficiency. Reduces write amplification
//! and improves throughput for high-volume insert workloads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

/// Async insert configuration
#[derive(Debug, Clone)]
pub struct AsyncInsertConfig {
    /// Enable async inserts
    pub enabled: bool,
    /// Maximum buffer size in bytes before flush
    pub max_buffer_bytes: usize,
    /// Maximum number of rows before flush
    pub max_buffer_rows: usize,
    /// Maximum wait time before flush
    pub max_wait_ms: u64,
    /// Minimum buffer size for flush (to avoid tiny batches)
    pub min_buffer_bytes: usize,
    /// Minimum rows for flush
    pub min_buffer_rows: usize,
    /// Per-table buffer limits
    pub per_table_limits: bool,
    /// Deduplicate within buffer
    pub deduplicate: bool,
    /// Maximum concurrent flushes
    pub max_concurrent_flushes: usize,
}

impl Default for AsyncInsertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_buffer_bytes: 10 * 1024 * 1024, // 10MB
            max_buffer_rows: 100_000,
            max_wait_ms: 200,
            min_buffer_bytes: 1024 * 1024, // 1MB
            min_buffer_rows: 1000,
            per_table_limits: true,
            deduplicate: false,
            max_concurrent_flushes: 4,
        }
    }
}

/// Buffered row data
#[derive(Debug, Clone)]
pub struct BufferedRow {
    /// Row data (serialized)
    pub data: Vec<u8>,
    /// Insert timestamp
    pub timestamp: Instant,
    /// Estimated size in bytes
    pub size_bytes: usize,
    /// Optional dedup key
    pub dedup_key: Option<Vec<u8>>,
}

/// Table buffer
#[derive(Debug)]
pub struct TableBuffer {
    /// Database name
    database: String,
    /// Table name
    table: String,
    /// Buffered rows
    rows: Vec<BufferedRow>,
    /// Total size in bytes
    total_bytes: usize,
    /// First insert timestamp
    first_insert: Option<Instant>,
    /// Dedup set (for deduplicate mode)
    dedup_set: Option<std::collections::HashSet<Vec<u8>>>,
}

impl TableBuffer {
    fn new(database: String, table: String, deduplicate: bool) -> Self {
        Self {
            database,
            table,
            rows: Vec::new(),
            total_bytes: 0,
            first_insert: None,
            dedup_set: if deduplicate { Some(Default::default()) } else { None },
        }
    }

    fn add(&mut self, row: BufferedRow) -> bool {
        // Check for duplicates
        if let (Some(ref mut set), Some(ref key)) = (&mut self.dedup_set, &row.dedup_key) {
            if !set.insert(key.clone()) {
                return false; // Duplicate
            }
        }

        if self.first_insert.is_none() {
            self.first_insert = Some(row.timestamp);
        }

        self.total_bytes += row.size_bytes;
        self.rows.push(row);
        true
    }

    fn clear(&mut self) {
        self.rows.clear();
        self.total_bytes = 0;
        self.first_insert = None;
        if let Some(ref mut set) = self.dedup_set {
            set.clear();
        }
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn age_ms(&self) -> u64 {
        self.first_insert
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Flush result
#[derive(Debug, Clone)]
pub struct FlushResult {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Rows flushed
    pub rows_flushed: usize,
    /// Bytes flushed
    pub bytes_flushed: usize,
    /// Flush duration
    pub duration: Duration,
    /// Success
    pub success: bool,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Flush callback trait
pub trait FlushCallback: Send + Sync {
    fn on_flush(&self, database: &str, table: &str, rows: Vec<BufferedRow>) -> Result<(), String>;
}

/// Async insert buffer manager
pub struct AsyncInsertBuffer {
    /// Configuration
    config: AsyncInsertConfig,
    /// Per-table buffers
    buffers: RwLock<HashMap<String, Mutex<TableBuffer>>>,
    /// Flush callback
    flush_callback: Option<Arc<dyn FlushCallback>>,
    /// Background flusher running (atomic to avoid deadlock with buffers lock)
    running: AtomicBool,
    /// Mutex used only for condvar wait (not held during flush operations)
    wait_mutex: Mutex<()>,
    /// Condition variable for wakeup
    condvar: Condvar,
    /// Statistics
    stats: RwLock<AsyncInsertStats>,
}

/// Async insert statistics
#[derive(Debug, Clone, Default)]
pub struct AsyncInsertStats {
    pub total_rows_buffered: u64,
    pub total_rows_flushed: u64,
    pub total_bytes_buffered: u64,
    pub total_bytes_flushed: u64,
    pub total_flushes: u64,
    pub failed_flushes: u64,
    pub duplicate_rows_skipped: u64,
    pub current_buffer_rows: u64,
    pub current_buffer_bytes: u64,
}

impl AsyncInsertBuffer {
    pub fn new(config: AsyncInsertConfig) -> Self {
        Self {
            config,
            buffers: RwLock::new(HashMap::new()),
            flush_callback: None,
            running: AtomicBool::new(false),
            wait_mutex: Mutex::new(()),
            condvar: Condvar::new(),
            stats: RwLock::new(AsyncInsertStats::default()),
        }
    }

    /// Set flush callback
    pub fn set_flush_callback(&mut self, callback: Arc<dyn FlushCallback>) {
        self.flush_callback = Some(callback);
    }

    /// Buffer a row
    pub fn buffer_row(
        &self,
        database: &str,
        table: &str,
        data: Vec<u8>,
        dedup_key: Option<Vec<u8>>,
    ) -> BufferResult {
        if !self.config.enabled {
            return BufferResult::Disabled;
        }

        let key = format!("{}.{}", database, table);
        let size_bytes = data.len();

        let row = BufferedRow {
            data,
            timestamp: Instant::now(),
            size_bytes,
            dedup_key,
        };

        // Get or create buffer
        {
            let mut buffers = self.buffers.write();
            if !buffers.contains_key(&key) {
                buffers.insert(
                    key.clone(),
                    Mutex::new(TableBuffer::new(
                        database.into(),
                        table.into(),
                        self.config.deduplicate,
                    )),
                );
            }
        }

        // Add row to buffer
        let buffers = self.buffers.read();
        let buffer_mutex = buffers.get(&key).unwrap();
        let mut buffer = buffer_mutex.lock();

        if buffer.add(row) {
            {
                let mut stats = self.stats.write();
                stats.total_rows_buffered += 1;
                stats.total_bytes_buffered += size_bytes as u64;
                stats.current_buffer_rows += 1;
                stats.current_buffer_bytes += size_bytes as u64;
            } // stats lock released here before potential flush

            // Check if flush needed
            if self.should_flush(&buffer) {
                drop(buffer);
                drop(buffers);
                self.flush_table(&key);
                BufferResult::Flushed
            } else {
                BufferResult::Buffered
            }
        } else {
            self.stats.write().duplicate_rows_skipped += 1;
            BufferResult::Deduplicated
        }
    }

    /// Check if buffer should be flushed
    fn should_flush(&self, buffer: &TableBuffer) -> bool {
        buffer.total_bytes >= self.config.max_buffer_bytes ||
        buffer.row_count() >= self.config.max_buffer_rows ||
        buffer.age_ms() >= self.config.max_wait_ms
    }

    /// Flush a specific table buffer
    pub fn flush_table(&self, key: &str) -> Option<FlushResult> {
        let buffers = self.buffers.read();
        let buffer_mutex = buffers.get(key)?;
        let mut buffer = buffer_mutex.lock();

        if buffer.is_empty() {
            return None;
        }

        // Don't flush if below minimums (unless forced)
        if buffer.total_bytes < self.config.min_buffer_bytes &&
           buffer.row_count() < self.config.min_buffer_rows &&
           buffer.age_ms() < self.config.max_wait_ms {
            return None;
        }

        let database = buffer.database.clone();
        let table = buffer.table.clone();
        let rows_count = buffer.row_count();
        let bytes_count = buffer.total_bytes;
        let rows: Vec<BufferedRow> = buffer.rows.drain(..).collect();
        
        buffer.clear();

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.current_buffer_rows -= rows_count as u64;
            stats.current_buffer_bytes -= bytes_count as u64;
        }

        drop(buffer);
        drop(buffers);

        // Execute flush
        let start = Instant::now();
        let result = if let Some(ref callback) = self.flush_callback {
            callback.on_flush(&database, &table, rows)
        } else {
            Ok(())
        };

        let duration = start.elapsed();
        let success = result.is_ok();

        {
            let mut stats = self.stats.write();
            stats.total_flushes += 1;
            if success {
                stats.total_rows_flushed += rows_count as u64;
                stats.total_bytes_flushed += bytes_count as u64;
            } else {
                stats.failed_flushes += 1;
            }
        }

        Some(FlushResult {
            database,
            table,
            rows_flushed: rows_count,
            bytes_flushed: bytes_count,
            duration,
            success,
            error: result.err(),
        })
    }

    /// Flush all buffers
    pub fn flush_all(&self) -> Vec<FlushResult> {
        let keys: Vec<String> = {
            self.buffers.read().keys().cloned().collect()
        };

        keys.iter()
            .filter_map(|k| self.flush_table(k))
            .collect()
    }

    /// Force flush (ignore minimums)
    pub fn force_flush(&self, database: &str, table: &str) -> Option<FlushResult> {
        let key = format!("{}.{}", database, table);
        
        let buffers = self.buffers.read();
        let buffer_mutex = buffers.get(&key)?;
        let mut buffer = buffer_mutex.lock();

        if buffer.is_empty() {
            return None;
        }

        let database = buffer.database.clone();
        let table = buffer.table.clone();
        let rows_count = buffer.row_count();
        let bytes_count = buffer.total_bytes;
        let rows: Vec<BufferedRow> = buffer.rows.drain(..).collect();
        
        buffer.clear();
        drop(buffer);
        drop(buffers);

        {
            let mut stats = self.stats.write();
            stats.current_buffer_rows -= rows_count as u64;
            stats.current_buffer_bytes -= bytes_count as u64;
        }

        let start = Instant::now();
        let result = if let Some(ref callback) = self.flush_callback {
            callback.on_flush(&database, &table, rows)
        } else {
            Ok(())
        };

        let duration = start.elapsed();
        let success = result.is_ok();

        {
            let mut stats = self.stats.write();
            stats.total_flushes += 1;
            if success {
                stats.total_rows_flushed += rows_count as u64;
                stats.total_bytes_flushed += bytes_count as u64;
            } else {
                stats.failed_flushes += 1;
            }
        }

        Some(FlushResult {
            database,
            table,
            rows_flushed: rows_count,
            bytes_flushed: bytes_count,
            duration,
            success,
            error: result.err(),
        })
    }

    /// Get buffer status
    pub fn buffer_status(&self, database: &str, table: &str) -> Option<BufferStatus> {
        let key = format!("{}.{}", database, table);
        let buffers = self.buffers.read();
        let buffer_mutex = buffers.get(&key)?;
        let buffer = buffer_mutex.lock();

        Some(BufferStatus {
            database: database.into(),
            table: table.into(),
            rows: buffer.row_count(),
            bytes: buffer.total_bytes,
            age_ms: buffer.age_ms(),
        })
    }

    /// Get all buffer statuses
    pub fn all_buffer_statuses(&self) -> Vec<BufferStatus> {
        let buffers = self.buffers.read();
        buffers.values()
            .map(|m| {
                let b = m.lock();
                BufferStatus {
                    database: b.database.clone(),
                    table: b.table.clone(),
                    rows: b.row_count(),
                    bytes: b.total_bytes,
                    age_ms: b.age_ms(),
                }
            })
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> AsyncInsertStats {
        self.stats.read().clone()
    }

    /// Start background flusher
    pub fn start_background_flusher(self: Arc<Self>) {
        // Use compare_exchange to atomically check and set running flag
        // This avoids race conditions when multiple threads try to start the flusher
        if self.running.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ).is_err() {
            // Already running
            return;
        }

        let this = self.clone();
        thread::spawn(move || {
            loop {
                // Check running flag without holding any lock
                if !this.running.load(Ordering::SeqCst) {
                    break;
                }

                // Wait for timeout or notification
                // The wait_mutex is ONLY used for condvar wait, not held during flush
                {
                    let mut guard = this.wait_mutex.lock();
                    this.condvar.wait_for(&mut guard, Duration::from_millis(this.config.max_wait_ms / 2));
                }
                // wait_mutex is released here BEFORE calling flush operations

                // Check running flag again after wakeup
                if !this.running.load(Ordering::SeqCst) {
                    break;
                }

                // Check all buffers - no locks held from this method
                this.check_and_flush_aged();
            }
        });
    }

    /// Stop background flusher
    pub fn stop_background_flusher(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    fn check_and_flush_aged(&self) {
        let keys: Vec<String> = {
            let buffers = self.buffers.read();
            buffers.iter()
                .filter_map(|(k, m)| {
                    let b = m.lock();
                    if b.age_ms() >= self.config.max_wait_ms && !b.is_empty() {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        for key in keys {
            self.flush_table(&key);
        }
    }
}

/// Buffer result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferResult {
    /// Successfully buffered
    Buffered,
    /// Buffer was flushed
    Flushed,
    /// Row was deduplicated (skipped)
    Deduplicated,
    /// Async insert disabled
    Disabled,
}

/// Buffer status
#[derive(Debug, Clone)]
pub struct BufferStatus {
    pub database: String,
    pub table: String,
    pub rows: usize,
    pub bytes: usize,
    pub age_ms: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct MockCallback {
        flush_count: Mutex<usize>,
    }

    impl FlushCallback for MockCallback {
        fn on_flush(&self, _database: &str, _table: &str, rows: Vec<BufferedRow>) -> Result<(), String> {
            *self.flush_count.lock() += rows.len();
            Ok(())
        }
    }

    #[test]
    fn test_buffer_row() {
        let buffer = AsyncInsertBuffer::new(AsyncInsertConfig {
            max_buffer_rows: 5,
            ..Default::default()
        });

        for i in 0..3 {
            let result = buffer.buffer_row("db", "table", vec![i as u8], None);
            assert_eq!(result, BufferResult::Buffered);
        }

        let status = buffer.buffer_status("db", "table").unwrap();
        assert_eq!(status.rows, 3);
    }

    #[test]
    fn test_auto_flush() {
        let mut buffer = AsyncInsertBuffer::new(AsyncInsertConfig {
            max_buffer_rows: 5,
            min_buffer_rows: 1,
            ..Default::default()
        });

        let callback = Arc::new(MockCallback {
            flush_count: Mutex::new(0),
        });
        buffer.set_flush_callback(callback.clone());

        for i in 0..5 {
            buffer.buffer_row("db", "table", vec![i as u8], None);
        }

        // Should have triggered flush
        assert_eq!(*callback.flush_count.lock(), 5);
    }

    #[test]
    fn test_deduplication() {
        let buffer = AsyncInsertBuffer::new(AsyncInsertConfig {
            deduplicate: true,
            ..Default::default()
        });

        let key = vec![1, 2, 3];
        
        let r1 = buffer.buffer_row("db", "table", vec![1], Some(key.clone()));
        assert_eq!(r1, BufferResult::Buffered);
        
        let r2 = buffer.buffer_row("db", "table", vec![2], Some(key.clone()));
        assert_eq!(r2, BufferResult::Deduplicated);

        let stats = buffer.stats();
        assert_eq!(stats.duplicate_rows_skipped, 1);
    }

    #[test]
    fn test_force_flush() {
        let mut buffer = AsyncInsertBuffer::new(AsyncInsertConfig::default());
        
        let callback = Arc::new(MockCallback {
            flush_count: Mutex::new(0),
        });
        buffer.set_flush_callback(callback.clone());

        buffer.buffer_row("db", "table", vec![1, 2, 3], None);
        
        let result = buffer.force_flush("db", "table");
        assert!(result.is_some());
        assert!(result.unwrap().success);
        assert_eq!(*callback.flush_count.lock(), 1);
    }
}
