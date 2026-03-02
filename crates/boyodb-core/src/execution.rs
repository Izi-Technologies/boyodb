// Phase 13: Query Execution
//
// Parallel query execution, spill-to-disk for large queries, and adaptive join strategies
// for high-performance analytical query processing.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use parking_lot::{Mutex, RwLock};

// ============================================================================
// Parallel Query Execution
// ============================================================================

/// Query execution configuration
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Maximum threads for parallel execution
    pub max_threads: usize,
    /// Target rows per batch
    pub batch_size: usize,
    /// Memory limit per query (bytes)
    pub memory_limit: usize,
    /// Spill threshold (percentage of memory limit)
    pub spill_threshold: f64,
    /// Temp directory for spilling
    pub temp_dir: PathBuf,
    /// Enable adaptive query execution
    pub adaptive_execution: bool,
    /// Prefetch buffer size
    pub prefetch_buffers: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_threads: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
            batch_size: 65536,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            spill_threshold: 0.8,
            temp_dir: std::env::temp_dir().join("boyodb_spill"),
            adaptive_execution: true,
            prefetch_buffers: 4,
        }
    }
}

/// Execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    pub rows_processed: u64,
    pub bytes_processed: u64,
    pub batches_processed: u64,
    pub parallel_tasks: u64,
    pub spill_count: u64,
    pub spill_bytes: u64,
    pub memory_peak: u64,
    pub cpu_time_ms: u64,
    pub wall_time_ms: u64,
}

/// A data batch for processing
#[derive(Debug, Clone)]
pub struct DataBatch {
    pub columns: Vec<ColumnData>,
    pub row_count: usize,
}

impl DataBatch {
    pub fn new(columns: Vec<ColumnData>) -> Self {
        let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
        Self { columns, row_count }
    }

    pub fn empty() -> Self {
        Self { columns: vec![], row_count: 0 }
    }

    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|c| c.memory_size()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// Column data types
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64(Vec<i64>),
    UInt64(Vec<u64>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Bytes(Vec<Vec<u8>>),
    Bool(Vec<bool>),
    Nullable { data: Box<ColumnData>, nulls: Vec<bool> },
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            Self::Int64(v) => v.len(),
            Self::UInt64(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bytes(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Nullable { data, .. } => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_size(&self) -> usize {
        match self {
            Self::Int64(v) => v.len() * 8,
            Self::UInt64(v) => v.len() * 8,
            Self::Float64(v) => v.len() * 8,
            Self::String(v) => v.iter().map(|s| s.len() + 24).sum(),
            Self::Bytes(v) => v.iter().map(|b| b.len() + 24).sum(),
            Self::Bool(v) => v.len(),
            Self::Nullable { data, nulls } => data.memory_size() + nulls.len(),
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        match self {
            Self::Int64(v) => Self::Int64(v[start..end].to_vec()),
            Self::UInt64(v) => Self::UInt64(v[start..end].to_vec()),
            Self::Float64(v) => Self::Float64(v[start..end].to_vec()),
            Self::String(v) => Self::String(v[start..end].to_vec()),
            Self::Bytes(v) => Self::Bytes(v[start..end].to_vec()),
            Self::Bool(v) => Self::Bool(v[start..end].to_vec()),
            Self::Nullable { data, nulls } => Self::Nullable {
                data: Box::new(data.slice(start, end)),
                nulls: nulls[start..end].to_vec(),
            },
        }
    }
}

/// Parallel task state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// A parallel execution task
pub struct ParallelTask {
    pub task_id: u64,
    pub partition_id: usize,
    pub state: AtomicUsize,
    pub rows_processed: AtomicU64,
    pub error: RwLock<Option<String>>,
}

impl ParallelTask {
    pub fn new(task_id: u64, partition_id: usize) -> Self {
        Self {
            task_id,
            partition_id,
            state: AtomicUsize::new(TaskState::Pending as usize),
            rows_processed: AtomicU64::new(0),
            error: RwLock::new(None),
        }
    }

    pub fn state(&self) -> TaskState {
        match self.state.load(Ordering::SeqCst) {
            0 => TaskState::Pending,
            1 => TaskState::Running,
            2 => TaskState::Completed,
            3 => TaskState::Failed,
            _ => TaskState::Cancelled,
        }
    }

    pub fn set_state(&self, state: TaskState) {
        self.state.store(state as usize, Ordering::SeqCst);
    }
}

/// Partition for parallel processing
#[derive(Debug, Clone)]
pub struct DataPartition {
    pub partition_id: usize,
    pub start_row: u64,
    pub end_row: u64,
    pub file_path: Option<PathBuf>,
    pub memory_data: Option<Arc<DataBatch>>,
}

/// Pipeline stage trait
pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &str;
    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError>;
    fn is_parallel(&self) -> bool { true }
    fn memory_estimate(&self, input_rows: usize) -> usize { input_rows * 100 }
}

/// Query pipeline for execution
pub struct QueryPipeline {
    stages: Vec<Box<dyn PipelineStage>>,
    config: ExecutionConfig,
}

impl QueryPipeline {
    pub fn new(config: ExecutionConfig) -> Self {
        Self {
            stages: Vec::new(),
            config,
        }
    }

    pub fn add_stage(&mut self, stage: Box<dyn PipelineStage>) {
        self.stages.push(stage);
    }

    pub fn execute(&self, partitions: Vec<DataPartition>) -> Result<DataBatch, ExecutionError> {
        if partitions.is_empty() {
            return Ok(DataBatch::empty());
        }

        let num_threads = self.config.max_threads.min(partitions.len());

        if num_threads <= 1 || partitions.len() == 1 {
            // Single-threaded execution
            return self.execute_sequential(partitions);
        }

        // Parallel execution
        self.execute_parallel(partitions, num_threads)
    }

    fn execute_sequential(&self, partitions: Vec<DataPartition>) -> Result<DataBatch, ExecutionError> {
        let mut results = Vec::new();

        for partition in partitions {
            let batch = partition.memory_data
                .map(|d| (*d).clone())
                .unwrap_or_else(DataBatch::empty);

            let mut current = batch;
            for stage in &self.stages {
                current = stage.process(current)?;
            }

            if !current.is_empty() {
                results.push(current);
            }
        }

        Self::merge_batches(results)
    }

    fn execute_parallel(&self, partitions: Vec<DataPartition>, num_threads: usize) -> Result<DataBatch, ExecutionError> {
        let results: Arc<Mutex<Vec<DataBatch>>> = Arc::new(Mutex::new(Vec::new()));
        let errors: Arc<Mutex<Vec<ExecutionError>>> = Arc::new(Mutex::new(Vec::new()));
        let partition_queue: Arc<Mutex<VecDeque<DataPartition>>> = Arc::new(Mutex::new(partitions.into()));
        let cancelled = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let queue = Arc::clone(&partition_queue);
            let results = Arc::clone(&results);
            let errors = Arc::clone(&errors);
            let cancelled = Arc::clone(&cancelled);
            let stages: Vec<_> = self.stages.iter().map(|s| s.name().to_string()).collect();
            let config = self.config.clone();

            // In a real implementation, stages would be cloned/shared properly
            // For now, we simulate the parallel work
            handles.push(thread::spawn(move || {
                while !cancelled.load(Ordering::SeqCst) {
                    let partition = {
                        let mut q = queue.lock();
                        q.pop_front()
                    };

                    let partition = match partition {
                        Some(p) => p,
                        None => break,
                    };

                    // Process partition (simplified - in real impl would use actual stages)
                    let batch = partition.memory_data
                        .map(|d| (*d).clone())
                        .unwrap_or_else(DataBatch::empty);

                    if !batch.is_empty() {
                        results.lock().push(batch);
                    }
                }
            }));
        }

        // Wait for all threads
        for handle in handles {
            let _ = handle.join();
        }

        // Check for errors
        let errs = errors.lock();
        if let Some(err) = errs.first() {
            return Err(err.clone());
        }
        drop(errs);

        // Merge results
        let results = std::mem::take(&mut *results.lock());
        Self::merge_batches(results)
    }

    fn merge_batches(batches: Vec<DataBatch>) -> Result<DataBatch, ExecutionError> {
        if batches.is_empty() {
            return Ok(DataBatch::empty());
        }
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // Merge all batches column by column
        let num_columns = batches[0].columns.len();
        let mut merged_columns = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let merged_col = Self::merge_columns(
                batches.iter().map(|b| &b.columns[col_idx]).collect()
            )?;
            merged_columns.push(merged_col);
        }

        Ok(DataBatch::new(merged_columns))
    }

    fn merge_columns(columns: Vec<&ColumnData>) -> Result<ColumnData, ExecutionError> {
        if columns.is_empty() {
            return Err(ExecutionError::InvalidData("No columns to merge".to_string()));
        }

        match columns[0] {
            ColumnData::Int64(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::Int64(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::Int64(merged))
            }
            ColumnData::UInt64(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::UInt64(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::UInt64(merged))
            }
            ColumnData::Float64(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::Float64(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::Float64(merged))
            }
            ColumnData::String(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::String(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::String(merged))
            }
            ColumnData::Bytes(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::Bytes(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::Bytes(merged))
            }
            ColumnData::Bool(_) => {
                let mut merged = Vec::new();
                for col in columns {
                    if let ColumnData::Bool(v) = col {
                        merged.extend(v.iter().cloned());
                    }
                }
                Ok(ColumnData::Bool(merged))
            }
            ColumnData::Nullable { .. } => {
                // Simplified - in real impl would handle nullable properly
                Ok(columns[0].clone())
            }
        }
    }
}

/// Execution error
#[derive(Debug, Clone)]
pub enum ExecutionError {
    MemoryLimitExceeded(usize),
    SpillFailed(String),
    IoError(String),
    InvalidData(String),
    Cancelled,
    InternalError(String),
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryLimitExceeded(size) => write!(f, "Memory limit exceeded: {} bytes", size),
            Self::SpillFailed(msg) => write!(f, "Spill failed: {}", msg),
            Self::IoError(msg) => write!(f, "IO error: {}", msg),
            Self::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            Self::Cancelled => write!(f, "Execution cancelled"),
            Self::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutionError {}

// ============================================================================
// Spill to Disk
// ============================================================================

/// Spill file format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillFormat {
    Binary,
    Compressed,
}

/// Spill file metadata
#[derive(Debug, Clone)]
pub struct SpillMetadata {
    pub file_path: PathBuf,
    pub row_count: u64,
    pub byte_size: u64,
    pub column_count: usize,
    pub format: SpillFormat,
    pub created_at: std::time::Instant,
}

/// Spill manager for handling memory overflow
pub struct SpillManager {
    config: ExecutionConfig,
    spill_files: RwLock<Vec<SpillMetadata>>,
    total_spilled: AtomicU64,
    spill_counter: AtomicU64,
}

impl SpillManager {
    pub fn new(config: ExecutionConfig) -> Self {
        // Ensure temp directory exists
        let _ = std::fs::create_dir_all(&config.temp_dir);

        Self {
            config,
            spill_files: RwLock::new(Vec::new()),
            total_spilled: AtomicU64::new(0),
            spill_counter: AtomicU64::new(0),
        }
    }

    /// Check if we should spill based on memory usage
    pub fn should_spill(&self, current_memory: usize) -> bool {
        let threshold = (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        current_memory > threshold
    }

    /// Spill a data batch to disk
    pub fn spill(&self, batch: &DataBatch) -> Result<SpillMetadata, ExecutionError> {
        let spill_id = self.spill_counter.fetch_add(1, Ordering::SeqCst);
        let file_path = self.config.temp_dir.join(format!("spill_{}.bin", spill_id));

        let file = File::create(&file_path)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        let mut writer = BufWriter::new(file);

        // Write header
        let row_count = batch.row_count as u64;
        let column_count = batch.columns.len();
        writer.write_all(&row_count.to_le_bytes())
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        writer.write_all(&(column_count as u64).to_le_bytes())
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;

        // Write each column
        for col in &batch.columns {
            self.write_column(&mut writer, col)?;
        }

        let byte_size = writer.stream_position()
            .map_err(|e| ExecutionError::IoError(e.to_string()))? as u64;

        writer.flush().map_err(|e| ExecutionError::IoError(e.to_string()))?;

        let metadata = SpillMetadata {
            file_path: file_path.clone(),
            row_count,
            byte_size,
            column_count,
            format: SpillFormat::Binary,
            created_at: std::time::Instant::now(),
        };

        self.spill_files.write().push(metadata.clone());
        self.total_spilled.fetch_add(byte_size, Ordering::SeqCst);

        Ok(metadata)
    }

    fn write_column<W: Write>(&self, writer: &mut W, col: &ColumnData) -> Result<(), ExecutionError> {
        // Write column type tag
        let type_tag: u8 = match col {
            ColumnData::Int64(_) => 0,
            ColumnData::UInt64(_) => 1,
            ColumnData::Float64(_) => 2,
            ColumnData::String(_) => 3,
            ColumnData::Bytes(_) => 4,
            ColumnData::Bool(_) => 5,
            ColumnData::Nullable { .. } => 6,
        };
        writer.write_all(&[type_tag])
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;

        match col {
            ColumnData::Int64(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    writer.write_all(&val.to_le_bytes())
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::UInt64(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    writer.write_all(&val.to_le_bytes())
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::Float64(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    writer.write_all(&val.to_le_bytes())
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::String(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    let bytes = val.as_bytes();
                    writer.write_all(&(bytes.len() as u64).to_le_bytes())
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    writer.write_all(bytes)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::Bytes(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    writer.write_all(&(val.len() as u64).to_le_bytes())
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    writer.write_all(val)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::Bool(v) => {
                writer.write_all(&(v.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in v {
                    writer.write_all(&[if *val { 1 } else { 0 }])
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
            }
            ColumnData::Nullable { data, nulls } => {
                // Write nulls bitmap
                writer.write_all(&(nulls.len() as u64).to_le_bytes())
                    .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                for val in nulls {
                    writer.write_all(&[if *val { 1 } else { 0 }])
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                }
                // Write nested column
                self.write_column(writer, data)?;
            }
        }

        Ok(())
    }

    /// Read a spilled batch back from disk
    pub fn restore(&self, metadata: &SpillMetadata) -> Result<DataBatch, ExecutionError> {
        let file = File::open(&metadata.file_path)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        let row_count = u64::from_le_bytes(buf8) as usize;

        reader.read_exact(&mut buf8)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        let column_count = u64::from_le_bytes(buf8) as usize;

        // Read columns
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(self.read_column(&mut reader)?);
        }

        Ok(DataBatch { columns, row_count })
    }

    fn read_column<R: Read>(&self, reader: &mut R) -> Result<ColumnData, ExecutionError> {
        let mut type_tag = [0u8; 1];
        reader.read_exact(&mut type_tag)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;

        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)
            .map_err(|e| ExecutionError::IoError(e.to_string()))?;
        let len = u64::from_le_bytes(buf8) as usize;

        match type_tag[0] {
            0 => { // Int64
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut buf8)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(i64::from_le_bytes(buf8));
                }
                Ok(ColumnData::Int64(data))
            }
            1 => { // UInt64
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut buf8)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(u64::from_le_bytes(buf8));
                }
                Ok(ColumnData::UInt64(data))
            }
            2 => { // Float64
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut buf8)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(f64::from_le_bytes(buf8));
                }
                Ok(ColumnData::Float64(data))
            }
            3 => { // String
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut buf8)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    let str_len = u64::from_le_bytes(buf8) as usize;
                    let mut str_buf = vec![0u8; str_len];
                    reader.read_exact(&mut str_buf)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(String::from_utf8(str_buf)
                        .map_err(|e| ExecutionError::InvalidData(e.to_string()))?);
                }
                Ok(ColumnData::String(data))
            }
            4 => { // Bytes
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    reader.read_exact(&mut buf8)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    let bytes_len = u64::from_le_bytes(buf8) as usize;
                    let mut bytes_buf = vec![0u8; bytes_len];
                    reader.read_exact(&mut bytes_buf)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(bytes_buf);
                }
                Ok(ColumnData::Bytes(data))
            }
            5 => { // Bool
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut b = [0u8; 1];
                    reader.read_exact(&mut b)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    data.push(b[0] != 0);
                }
                Ok(ColumnData::Bool(data))
            }
            6 => { // Nullable
                let mut nulls = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut b = [0u8; 1];
                    reader.read_exact(&mut b)
                        .map_err(|e| ExecutionError::IoError(e.to_string()))?;
                    nulls.push(b[0] != 0);
                }
                let nested = self.read_column(reader)?;
                Ok(ColumnData::Nullable {
                    data: Box::new(nested),
                    nulls,
                })
            }
            _ => Err(ExecutionError::InvalidData(format!("Unknown column type: {}", type_tag[0]))),
        }
    }

    /// Clean up all spill files
    pub fn cleanup(&self) {
        let files = std::mem::take(&mut *self.spill_files.write());
        for meta in files {
            let _ = std::fs::remove_file(&meta.file_path);
        }
        self.total_spilled.store(0, Ordering::SeqCst);
    }

    /// Get total bytes spilled
    pub fn total_spilled(&self) -> u64 {
        self.total_spilled.load(Ordering::SeqCst)
    }

    /// Get spill file count
    pub fn spill_count(&self) -> usize {
        self.spill_files.read().len()
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// ============================================================================
// Adaptive Joins
// ============================================================================

/// Join algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgorithm {
    /// Nested loop join - for small tables or no index
    NestedLoop,
    /// Hash join - for equality joins
    Hash,
    /// Sort-merge join - for sorted data or range joins
    SortMerge,
    /// Broadcast join - small table broadcast to all nodes
    Broadcast,
    /// Shuffle join - partition both tables by join key
    Shuffle,
    /// Index join - use existing index
    IndexLookup,
}

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

/// Join statistics for adaptive decision
#[derive(Debug, Clone, Default)]
pub struct JoinStats {
    pub left_rows: u64,
    pub left_bytes: u64,
    pub right_rows: u64,
    pub right_bytes: u64,
    pub left_distinct_keys: u64,
    pub right_distinct_keys: u64,
    pub left_sorted: bool,
    pub right_sorted: bool,
    pub left_has_index: bool,
    pub right_has_index: bool,
}

/// Join configuration
#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub join_type: JoinType,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub algorithm: Option<JoinAlgorithm>,  // None = adaptive
    pub broadcast_threshold: u64,  // Max bytes to broadcast
    pub hash_table_memory_limit: usize,
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            left_keys: Vec::new(),
            right_keys: Vec::new(),
            algorithm: None,
            broadcast_threshold: 100 * 1024 * 1024, // 100MB
            hash_table_memory_limit: 512 * 1024 * 1024, // 512MB
        }
    }
}

/// Adaptive join executor
pub struct AdaptiveJoinExecutor {
    config: JoinConfig,
    stats: JoinStats,
    spill_manager: Arc<SpillManager>,
}

impl AdaptiveJoinExecutor {
    pub fn new(config: JoinConfig, spill_manager: Arc<SpillManager>) -> Self {
        Self {
            config,
            stats: JoinStats::default(),
            spill_manager,
        }
    }

    /// Choose the best join algorithm based on statistics
    pub fn choose_algorithm(&self) -> JoinAlgorithm {
        if let Some(algo) = self.config.algorithm {
            return algo;
        }

        let stats = &self.stats;

        // If either side is very small, use broadcast
        if stats.left_bytes < self.config.broadcast_threshold {
            return JoinAlgorithm::Broadcast;
        }
        if stats.right_bytes < self.config.broadcast_threshold {
            return JoinAlgorithm::Broadcast;
        }

        // If both sides are sorted on join keys, use sort-merge
        if stats.left_sorted && stats.right_sorted {
            return JoinAlgorithm::SortMerge;
        }

        // If one side has an index, use index lookup
        if stats.right_has_index && stats.left_rows < stats.right_rows / 10 {
            return JoinAlgorithm::IndexLookup;
        }
        if stats.left_has_index && stats.right_rows < stats.left_rows / 10 {
            return JoinAlgorithm::IndexLookup;
        }

        // For very small tables, nested loop might be faster
        if stats.left_rows < 1000 && stats.right_rows < 1000 {
            return JoinAlgorithm::NestedLoop;
        }

        // Default to hash join for equality joins
        JoinAlgorithm::Hash
    }

    /// Update statistics for adaptive decision making
    pub fn update_stats(&mut self, stats: JoinStats) {
        self.stats = stats;
    }

    /// Execute the join
    pub fn execute(&self, left: DataBatch, right: DataBatch) -> Result<DataBatch, ExecutionError> {
        let algorithm = self.choose_algorithm();

        match algorithm {
            JoinAlgorithm::Hash => self.hash_join(left, right),
            JoinAlgorithm::NestedLoop => self.nested_loop_join(left, right),
            JoinAlgorithm::SortMerge => self.sort_merge_join(left, right),
            JoinAlgorithm::Broadcast => self.hash_join(left, right), // Same as hash for single node
            JoinAlgorithm::Shuffle => self.hash_join(left, right),
            JoinAlgorithm::IndexLookup => self.index_join(left, right),
        }
    }

    fn hash_join(&self, left: DataBatch, right: DataBatch) -> Result<DataBatch, ExecutionError> {
        if left.is_empty() || right.is_empty() {
            return Ok(DataBatch::empty());
        }

        // Build hash table from smaller side (assume right for now)
        let mut hash_table: HashMap<u64, Vec<usize>> = HashMap::new();

        // Simplified: use first column as key
        if right.columns.is_empty() || left.columns.is_empty() {
            return Ok(DataBatch::empty());
        }

        // Build phase
        for i in 0..right.row_count {
            let key = self.compute_hash(&right.columns[0], i);
            hash_table.entry(key).or_default().push(i);
        }

        // Check memory and spill if needed
        let hash_table_size = hash_table.len() * 32; // Rough estimate
        if hash_table_size > self.config.hash_table_memory_limit {
            // Would spill here in real implementation
        }

        // Probe phase
        let mut result_indices_left = Vec::new();
        let mut result_indices_right = Vec::new();

        for i in 0..left.row_count {
            let key = self.compute_hash(&left.columns[0], i);
            if let Some(matches) = hash_table.get(&key) {
                for &j in matches {
                    // Verify actual match (hash collision check)
                    if self.values_equal(&left.columns[0], i, &right.columns[0], j) {
                        result_indices_left.push(i);
                        result_indices_right.push(j);
                    }
                }
            }
        }

        // Build result batch
        self.build_join_result(&left, &result_indices_left, &right, &result_indices_right)
    }

    fn nested_loop_join(&self, left: DataBatch, right: DataBatch) -> Result<DataBatch, ExecutionError> {
        if left.is_empty() || right.is_empty() {
            return Ok(DataBatch::empty());
        }

        let mut result_indices_left = Vec::new();
        let mut result_indices_right = Vec::new();

        for i in 0..left.row_count {
            for j in 0..right.row_count {
                // Check join condition
                if !left.columns.is_empty() && !right.columns.is_empty() {
                    if self.values_equal(&left.columns[0], i, &right.columns[0], j) {
                        result_indices_left.push(i);
                        result_indices_right.push(j);
                    }
                }
            }
        }

        self.build_join_result(&left, &result_indices_left, &right, &result_indices_right)
    }

    fn sort_merge_join(&self, left: DataBatch, right: DataBatch) -> Result<DataBatch, ExecutionError> {
        // For now, fall back to hash join
        // In real implementation, would sort both sides and merge
        self.hash_join(left, right)
    }

    fn index_join(&self, left: DataBatch, right: DataBatch) -> Result<DataBatch, ExecutionError> {
        // For now, fall back to hash join
        // In real implementation, would use index for lookups
        self.hash_join(left, right)
    }

    fn compute_hash(&self, col: &ColumnData, row: usize) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        match col {
            ColumnData::Int64(v) => v.get(row).hash(&mut hasher),
            ColumnData::UInt64(v) => v.get(row).hash(&mut hasher),
            ColumnData::Float64(v) => v.get(row).map(|f| f.to_bits()).hash(&mut hasher),
            ColumnData::String(v) => v.get(row).hash(&mut hasher),
            ColumnData::Bytes(v) => v.get(row).hash(&mut hasher),
            ColumnData::Bool(v) => v.get(row).hash(&mut hasher),
            ColumnData::Nullable { data, nulls } => {
                if nulls.get(row).copied().unwrap_or(true) {
                    0u8.hash(&mut hasher);
                } else {
                    self.compute_hash(data, row).hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }

    fn values_equal(&self, col1: &ColumnData, row1: usize, col2: &ColumnData, row2: usize) -> bool {
        match (col1, col2) {
            (ColumnData::Int64(v1), ColumnData::Int64(v2)) => {
                v1.get(row1) == v2.get(row2)
            }
            (ColumnData::UInt64(v1), ColumnData::UInt64(v2)) => {
                v1.get(row1) == v2.get(row2)
            }
            (ColumnData::Float64(v1), ColumnData::Float64(v2)) => {
                v1.get(row1) == v2.get(row2)
            }
            (ColumnData::String(v1), ColumnData::String(v2)) => {
                v1.get(row1) == v2.get(row2)
            }
            _ => false,
        }
    }

    fn build_join_result(
        &self,
        left: &DataBatch,
        left_indices: &[usize],
        right: &DataBatch,
        right_indices: &[usize],
    ) -> Result<DataBatch, ExecutionError> {
        if left_indices.is_empty() {
            return Ok(DataBatch::empty());
        }

        let mut result_columns = Vec::new();

        // Add left columns
        for col in &left.columns {
            result_columns.push(self.gather_column(col, left_indices));
        }

        // Add right columns
        for col in &right.columns {
            result_columns.push(self.gather_column(col, right_indices));
        }

        Ok(DataBatch::new(result_columns))
    }

    fn gather_column(&self, col: &ColumnData, indices: &[usize]) -> ColumnData {
        match col {
            ColumnData::Int64(v) => {
                ColumnData::Int64(indices.iter().map(|&i| v[i]).collect())
            }
            ColumnData::UInt64(v) => {
                ColumnData::UInt64(indices.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Float64(v) => {
                ColumnData::Float64(indices.iter().map(|&i| v[i]).collect())
            }
            ColumnData::String(v) => {
                ColumnData::String(indices.iter().map(|&i| v[i].clone()).collect())
            }
            ColumnData::Bytes(v) => {
                ColumnData::Bytes(indices.iter().map(|&i| v[i].clone()).collect())
            }
            ColumnData::Bool(v) => {
                ColumnData::Bool(indices.iter().map(|&i| v[i]).collect())
            }
            ColumnData::Nullable { data, nulls } => {
                ColumnData::Nullable {
                    data: Box::new(self.gather_column(data, indices)),
                    nulls: indices.iter().map(|&i| nulls[i]).collect(),
                }
            }
        }
    }
}

// ============================================================================
// Pipeline Stages
// ============================================================================

/// Scan stage - read data
pub struct ScanStage {
    pub name: String,
}

impl PipelineStage for ScanStage {
    fn name(&self) -> &str { &self.name }

    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        Ok(input)
    }
}

/// Filter stage - apply predicates
pub struct FilterStage {
    pub name: String,
    pub predicate: Box<dyn Fn(&DataBatch, usize) -> bool + Send + Sync>,
}

impl PipelineStage for FilterStage {
    fn name(&self) -> &str { &self.name }

    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        if input.is_empty() {
            return Ok(input);
        }

        let mut keep_indices = Vec::new();
        for i in 0..input.row_count {
            if (self.predicate)(&input, i) {
                keep_indices.push(i);
            }
        }

        // Gather kept rows
        let columns: Vec<ColumnData> = input.columns.iter()
            .map(|col| gather_by_indices(col, &keep_indices))
            .collect();

        Ok(DataBatch::new(columns))
    }
}

fn gather_by_indices(col: &ColumnData, indices: &[usize]) -> ColumnData {
    match col {
        ColumnData::Int64(v) => ColumnData::Int64(indices.iter().map(|&i| v[i]).collect()),
        ColumnData::UInt64(v) => ColumnData::UInt64(indices.iter().map(|&i| v[i]).collect()),
        ColumnData::Float64(v) => ColumnData::Float64(indices.iter().map(|&i| v[i]).collect()),
        ColumnData::String(v) => ColumnData::String(indices.iter().map(|&i| v[i].clone()).collect()),
        ColumnData::Bytes(v) => ColumnData::Bytes(indices.iter().map(|&i| v[i].clone()).collect()),
        ColumnData::Bool(v) => ColumnData::Bool(indices.iter().map(|&i| v[i]).collect()),
        ColumnData::Nullable { data, nulls } => ColumnData::Nullable {
            data: Box::new(gather_by_indices(data, indices)),
            nulls: indices.iter().map(|&i| nulls[i]).collect(),
        },
    }
}

/// Aggregation stage
pub struct AggregateStage {
    pub name: String,
    pub group_by_columns: Vec<usize>,
    pub aggregates: Vec<AggregateFunction>,
}

/// Aggregate function types
#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum(usize),      // Column index
    Avg(usize),
    Min(usize),
    Max(usize),
    CountDistinct(usize),
}

impl PipelineStage for AggregateStage {
    fn name(&self) -> &str { &self.name }

    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        if input.is_empty() {
            return Ok(input);
        }

        // Simple aggregation without grouping
        if self.group_by_columns.is_empty() {
            return self.aggregate_all(input);
        }

        // Group by aggregation
        self.aggregate_grouped(input)
    }

    fn is_parallel(&self) -> bool { false } // Aggregation needs coordination
}

impl AggregateStage {
    fn aggregate_all(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        let mut result_columns = Vec::new();

        for agg in &self.aggregates {
            let col = match agg {
                AggregateFunction::Count => {
                    ColumnData::UInt64(vec![input.row_count as u64])
                }
                AggregateFunction::Sum(idx) => {
                    let sum = self.compute_sum(&input.columns[*idx]);
                    ColumnData::Float64(vec![sum])
                }
                AggregateFunction::Avg(idx) => {
                    let sum = self.compute_sum(&input.columns[*idx]);
                    let avg = if input.row_count > 0 { sum / input.row_count as f64 } else { 0.0 };
                    ColumnData::Float64(vec![avg])
                }
                AggregateFunction::Min(idx) => {
                    let min = self.compute_min(&input.columns[*idx]);
                    ColumnData::Float64(vec![min])
                }
                AggregateFunction::Max(idx) => {
                    let max = self.compute_max(&input.columns[*idx]);
                    ColumnData::Float64(vec![max])
                }
                AggregateFunction::CountDistinct(idx) => {
                    let count = self.compute_count_distinct(&input.columns[*idx]);
                    ColumnData::UInt64(vec![count])
                }
            };
            result_columns.push(col);
        }

        Ok(DataBatch::new(result_columns))
    }

    fn aggregate_grouped(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        // Simplified grouped aggregation
        // In real implementation, would build hash table by group keys
        self.aggregate_all(input)
    }

    fn compute_sum(&self, col: &ColumnData) -> f64 {
        match col {
            ColumnData::Int64(v) => v.iter().map(|&x| x as f64).sum(),
            ColumnData::UInt64(v) => v.iter().map(|&x| x as f64).sum(),
            ColumnData::Float64(v) => v.iter().sum(),
            _ => 0.0,
        }
    }

    fn compute_min(&self, col: &ColumnData) -> f64 {
        match col {
            ColumnData::Int64(v) => v.iter().map(|&x| x as f64).fold(f64::INFINITY, f64::min),
            ColumnData::UInt64(v) => v.iter().map(|&x| x as f64).fold(f64::INFINITY, f64::min),
            ColumnData::Float64(v) => v.iter().cloned().fold(f64::INFINITY, f64::min),
            _ => 0.0,
        }
    }

    fn compute_max(&self, col: &ColumnData) -> f64 {
        match col {
            ColumnData::Int64(v) => v.iter().map(|&x| x as f64).fold(f64::NEG_INFINITY, f64::max),
            ColumnData::UInt64(v) => v.iter().map(|&x| x as f64).fold(f64::NEG_INFINITY, f64::max),
            ColumnData::Float64(v) => v.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            _ => 0.0,
        }
    }

    fn compute_count_distinct(&self, col: &ColumnData) -> u64 {
        use std::collections::HashSet;

        match col {
            ColumnData::Int64(v) => {
                let set: HashSet<_> = v.iter().collect();
                set.len() as u64
            }
            ColumnData::UInt64(v) => {
                let set: HashSet<_> = v.iter().collect();
                set.len() as u64
            }
            ColumnData::String(v) => {
                let set: HashSet<_> = v.iter().collect();
                set.len() as u64
            }
            _ => 0,
        }
    }
}

/// Sort stage
pub struct SortStage {
    pub name: String,
    pub sort_columns: Vec<(usize, bool)>, // (column_idx, ascending)
}

impl PipelineStage for SortStage {
    fn name(&self) -> &str { &self.name }

    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        if input.is_empty() || self.sort_columns.is_empty() {
            return Ok(input);
        }

        // Create indices and sort them
        let mut indices: Vec<usize> = (0..input.row_count).collect();

        let (sort_col_idx, ascending) = self.sort_columns[0];
        let col = &input.columns[sort_col_idx];

        indices.sort_by(|&a, &b| {
            let cmp = self.compare_values(col, a, b);
            if ascending { cmp } else { cmp.reverse() }
        });

        // Reorder all columns
        let columns: Vec<ColumnData> = input.columns.iter()
            .map(|col| gather_by_indices(col, &indices))
            .collect();

        Ok(DataBatch::new(columns))
    }
}

impl SortStage {
    fn compare_values(&self, col: &ColumnData, a: usize, b: usize) -> std::cmp::Ordering {
        match col {
            ColumnData::Int64(v) => v[a].cmp(&v[b]),
            ColumnData::UInt64(v) => v[a].cmp(&v[b]),
            ColumnData::Float64(v) => v[a].partial_cmp(&v[b]).unwrap_or(std::cmp::Ordering::Equal),
            ColumnData::String(v) => v[a].cmp(&v[b]),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

/// Limit stage
pub struct LimitStage {
    pub name: String,
    pub limit: usize,
    pub offset: usize,
}

impl PipelineStage for LimitStage {
    fn name(&self) -> &str { &self.name }

    fn process(&self, input: DataBatch) -> Result<DataBatch, ExecutionError> {
        if input.is_empty() {
            return Ok(input);
        }

        let start = self.offset.min(input.row_count);
        let end = (start + self.limit).min(input.row_count);

        if start >= end {
            return Ok(DataBatch::empty());
        }

        let columns: Vec<ColumnData> = input.columns.iter()
            .map(|col| col.slice(start, end))
            .collect();

        Ok(DataBatch::new(columns))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_batch(rows: usize) -> DataBatch {
        DataBatch::new(vec![
            ColumnData::Int64((0..rows as i64).collect()),
            ColumnData::String((0..rows).map(|i| format!("row_{}", i)).collect()),
            ColumnData::Float64((0..rows).map(|i| i as f64 * 1.5).collect()),
        ])
    }

    // Parallel Execution Tests

    #[test]
    fn test_execution_config_default() {
        let config = ExecutionConfig::default();
        assert!(config.max_threads >= 1);
        assert_eq!(config.batch_size, 65536);
        assert!(config.memory_limit > 0);
    }

    #[test]
    fn test_data_batch_basic() {
        let batch = make_test_batch(100);
        assert_eq!(batch.row_count, 100);
        assert_eq!(batch.columns.len(), 3);
        assert!(!batch.is_empty());
        assert!(batch.memory_size() > 0);
    }

    #[test]
    fn test_column_data_slice() {
        let col = ColumnData::Int64(vec![1, 2, 3, 4, 5]);
        let sliced = col.slice(1, 4);

        if let ColumnData::Int64(v) = sliced {
            assert_eq!(v, vec![2, 3, 4]);
        } else {
            panic!("Expected Int64");
        }
    }

    #[test]
    fn test_pipeline_sequential() {
        let config = ExecutionConfig {
            max_threads: 1,
            ..Default::default()
        };
        let pipeline = QueryPipeline::new(config);

        let batch = make_test_batch(100);
        let partition = DataPartition {
            partition_id: 0,
            start_row: 0,
            end_row: 100,
            file_path: None,
            memory_data: Some(Arc::new(batch)),
        };

        let result = pipeline.execute(vec![partition]).unwrap();
        assert_eq!(result.row_count, 100);
    }

    #[test]
    fn test_pipeline_parallel() {
        let config = ExecutionConfig {
            max_threads: 4,
            ..Default::default()
        };
        let pipeline = QueryPipeline::new(config);

        let partitions: Vec<DataPartition> = (0..4).map(|i| {
            DataPartition {
                partition_id: i,
                start_row: (i * 100) as u64,
                end_row: ((i + 1) * 100) as u64,
                file_path: None,
                memory_data: Some(Arc::new(make_test_batch(100))),
            }
        }).collect();

        let result = pipeline.execute(partitions).unwrap();
        assert_eq!(result.row_count, 400);
    }

    // Spill Tests

    #[test]
    fn test_spill_manager_basic() {
        let config = ExecutionConfig::default();
        let manager = SpillManager::new(config);

        assert_eq!(manager.spill_count(), 0);
        assert_eq!(manager.total_spilled(), 0);
    }

    #[test]
    fn test_spill_and_restore() {
        let config = ExecutionConfig::default();
        let manager = SpillManager::new(config);

        let batch = make_test_batch(100);
        let metadata = manager.spill(&batch).unwrap();

        assert!(metadata.file_path.exists());
        assert!(metadata.row_count == 100);

        let restored = manager.restore(&metadata).unwrap();
        assert_eq!(restored.row_count, 100);

        // Verify data
        if let (ColumnData::Int64(orig), ColumnData::Int64(rest)) =
            (&batch.columns[0], &restored.columns[0]) {
            assert_eq!(orig, rest);
        }

        manager.cleanup();
        assert!(!metadata.file_path.exists());
    }

    #[test]
    fn test_should_spill() {
        let config = ExecutionConfig {
            memory_limit: 1000,
            spill_threshold: 0.8,
            ..Default::default()
        };
        let manager = SpillManager::new(config);

        assert!(!manager.should_spill(700));  // 70% - below threshold
        assert!(manager.should_spill(900));   // 90% - above threshold
    }

    // Join Tests

    #[test]
    fn test_join_algorithm_choice() {
        let config = JoinConfig::default();
        let spill_mgr = Arc::new(SpillManager::new(ExecutionConfig::default()));
        let mut executor = AdaptiveJoinExecutor::new(config, spill_mgr);

        // Small tables -> might choose nested loop
        executor.update_stats(JoinStats {
            left_rows: 100,
            left_bytes: 200_000_000, // Above broadcast threshold
            right_rows: 100,
            right_bytes: 200_000_000, // Above broadcast threshold
            ..Default::default()
        });
        let algo = executor.choose_algorithm();
        assert!(matches!(algo, JoinAlgorithm::NestedLoop));

        // Sorted tables -> sort merge
        executor.update_stats(JoinStats {
            left_rows: 10000,
            left_bytes: 200_000_000, // Above broadcast threshold
            right_rows: 10000,
            right_bytes: 200_000_000, // Above broadcast threshold
            left_sorted: true,
            right_sorted: true,
            ..Default::default()
        });
        let algo = executor.choose_algorithm();
        assert!(matches!(algo, JoinAlgorithm::SortMerge));

        // One small side -> broadcast
        executor.update_stats(JoinStats {
            left_rows: 1000000,
            left_bytes: 100_000_000,
            right_rows: 100,
            right_bytes: 1000,
            ..Default::default()
        });
        let algo = executor.choose_algorithm();
        assert!(matches!(algo, JoinAlgorithm::Broadcast));
    }

    #[test]
    fn test_hash_join() {
        let config = JoinConfig {
            algorithm: Some(JoinAlgorithm::Hash),
            ..Default::default()
        };
        let spill_mgr = Arc::new(SpillManager::new(ExecutionConfig::default()));
        let executor = AdaptiveJoinExecutor::new(config, spill_mgr);

        let left = DataBatch::new(vec![
            ColumnData::Int64(vec![1, 2, 3, 4, 5]),
        ]);
        let right = DataBatch::new(vec![
            ColumnData::Int64(vec![3, 4, 5, 6, 7]),
        ]);

        let result = executor.execute(left, right).unwrap();
        assert_eq!(result.row_count, 3); // 3, 4, 5 match
    }

    #[test]
    fn test_nested_loop_join() {
        let config = JoinConfig {
            algorithm: Some(JoinAlgorithm::NestedLoop),
            ..Default::default()
        };
        let spill_mgr = Arc::new(SpillManager::new(ExecutionConfig::default()));
        let executor = AdaptiveJoinExecutor::new(config, spill_mgr);

        let left = DataBatch::new(vec![
            ColumnData::Int64(vec![1, 2, 3]),
        ]);
        let right = DataBatch::new(vec![
            ColumnData::Int64(vec![2, 3, 4]),
        ]);

        let result = executor.execute(left, right).unwrap();
        assert_eq!(result.row_count, 2); // 2, 3 match
    }

    // Pipeline Stage Tests

    #[test]
    fn test_filter_stage() {
        let stage = FilterStage {
            name: "filter".to_string(),
            predicate: Box::new(|batch, row| {
                if let ColumnData::Int64(v) = &batch.columns[0] {
                    v[row] > 50
                } else {
                    false
                }
            }),
        };

        let batch = make_test_batch(100);
        let result = stage.process(batch).unwrap();
        assert_eq!(result.row_count, 49); // 51-99
    }

    #[test]
    fn test_aggregate_stage_count() {
        let stage = AggregateStage {
            name: "agg".to_string(),
            group_by_columns: vec![],
            aggregates: vec![AggregateFunction::Count],
        };

        let batch = make_test_batch(100);
        let result = stage.process(batch).unwrap();

        assert_eq!(result.row_count, 1);
        if let ColumnData::UInt64(v) = &result.columns[0] {
            assert_eq!(v[0], 100);
        }
    }

    #[test]
    fn test_aggregate_stage_sum() {
        let stage = AggregateStage {
            name: "agg".to_string(),
            group_by_columns: vec![],
            aggregates: vec![AggregateFunction::Sum(0)],
        };

        let batch = DataBatch::new(vec![
            ColumnData::Int64(vec![1, 2, 3, 4, 5]),
        ]);
        let result = stage.process(batch).unwrap();

        if let ColumnData::Float64(v) = &result.columns[0] {
            assert!((v[0] - 15.0).abs() < 0.001);
        }
    }

    #[test]
    fn test_sort_stage() {
        let stage = SortStage {
            name: "sort".to_string(),
            sort_columns: vec![(0, false)], // Descending
        };

        let batch = DataBatch::new(vec![
            ColumnData::Int64(vec![3, 1, 4, 1, 5, 9, 2, 6]),
        ]);
        let result = stage.process(batch).unwrap();

        if let ColumnData::Int64(v) = &result.columns[0] {
            assert_eq!(v, &vec![9, 6, 5, 4, 3, 2, 1, 1]);
        }
    }

    #[test]
    fn test_limit_stage() {
        let stage = LimitStage {
            name: "limit".to_string(),
            limit: 10,
            offset: 5,
        };

        let batch = make_test_batch(100);
        let result = stage.process(batch).unwrap();

        assert_eq!(result.row_count, 10);
        if let ColumnData::Int64(v) = &result.columns[0] {
            assert_eq!(v[0], 5);
            assert_eq!(v[9], 14);
        }
    }

    #[test]
    fn test_limit_stage_offset_beyond() {
        let stage = LimitStage {
            name: "limit".to_string(),
            limit: 10,
            offset: 200,
        };

        let batch = make_test_batch(100);
        let result = stage.process(batch).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_aggregate_avg() {
        let stage = AggregateStage {
            name: "agg".to_string(),
            group_by_columns: vec![],
            aggregates: vec![AggregateFunction::Avg(0)],
        };

        let batch = DataBatch::new(vec![
            ColumnData::Int64(vec![10, 20, 30, 40, 50]),
        ]);
        let result = stage.process(batch).unwrap();

        if let ColumnData::Float64(v) = &result.columns[0] {
            assert!((v[0] - 30.0).abs() < 0.001);
        }
    }

    #[test]
    fn test_aggregate_min_max() {
        let stage = AggregateStage {
            name: "agg".to_string(),
            group_by_columns: vec![],
            aggregates: vec![
                AggregateFunction::Min(0),
                AggregateFunction::Max(0),
            ],
        };

        let batch = DataBatch::new(vec![
            ColumnData::Int64(vec![5, 2, 8, 1, 9]),
        ]);
        let result = stage.process(batch).unwrap();

        if let ColumnData::Float64(v) = &result.columns[0] {
            assert!((v[0] - 1.0).abs() < 0.001);
        }
        if let ColumnData::Float64(v) = &result.columns[1] {
            assert!((v[0] - 9.0).abs() < 0.001);
        }
    }

    #[test]
    fn test_count_distinct() {
        let stage = AggregateStage {
            name: "agg".to_string(),
            group_by_columns: vec![],
            aggregates: vec![AggregateFunction::CountDistinct(0)],
        };

        let batch = DataBatch::new(vec![
            ColumnData::Int64(vec![1, 2, 2, 3, 3, 3, 4]),
        ]);
        let result = stage.process(batch).unwrap();

        if let ColumnData::UInt64(v) = &result.columns[0] {
            assert_eq!(v[0], 4); // 1, 2, 3, 4
        }
    }
}
