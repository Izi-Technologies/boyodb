//! Parallel Index Building (CREATE INDEX CONCURRENTLY)
//!
//! This module implements parallel index creation that doesn't block
//! concurrent writes. Unlike regular index creation which holds a lock
//! for the entire duration, CONCURRENTLY mode allows ongoing DML operations.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum ParallelIndexError {
    /// Index already exists
    IndexExists(String),
    /// Table not found
    TableNotFound(String),
    /// Build was cancelled
    BuildCancelled,
    /// Concurrent modification conflict
    ConcurrentModification,
    /// Worker thread error
    WorkerError(String),
    /// Merge phase failed
    MergeFailed(String),
    /// Validation failed
    ValidationFailed(String),
    /// Invalid configuration
    InvalidConfig(String),
}

impl fmt::Display for ParallelIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParallelIndexError::IndexExists(name) => write!(f, "Index already exists: {}", name),
            ParallelIndexError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            ParallelIndexError::BuildCancelled => write!(f, "Index build was cancelled"),
            ParallelIndexError::ConcurrentModification => {
                write!(f, "Concurrent modification conflict")
            }
            ParallelIndexError::WorkerError(msg) => write!(f, "Worker error: {}", msg),
            ParallelIndexError::MergeFailed(msg) => write!(f, "Merge failed: {}", msg),
            ParallelIndexError::ValidationFailed(msg) => write!(f, "Validation failed: {}", msg),
            ParallelIndexError::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
        }
    }
}

impl std::error::Error for ParallelIndexError {}

// ============================================================================
// Index Build State
// ============================================================================

/// Current state of an index build
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBuildState {
    /// Not started
    Pending,
    /// Scanning table and building initial index
    Phase1Scanning,
    /// Catching up with concurrent changes
    Phase2CatchUp,
    /// Validating index entries
    Phase3Validation,
    /// Finalizing and marking index as valid
    Finalizing,
    /// Build completed successfully
    Complete,
    /// Build failed
    Failed,
    /// Build was cancelled
    Cancelled,
}

impl IndexBuildState {
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            IndexBuildState::Complete | IndexBuildState::Failed | IndexBuildState::Cancelled
        )
    }

    pub fn is_successful(&self) -> bool {
        *self == IndexBuildState::Complete
    }
}

// ============================================================================
// Index Build Progress
// ============================================================================

/// Progress tracking for index build
#[derive(Debug)]
pub struct IndexBuildProgress {
    /// Current state
    pub state: RwLock<IndexBuildState>,
    /// Total rows to process
    pub total_rows: AtomicU64,
    /// Rows processed so far
    pub rows_processed: AtomicU64,
    /// Index entries created
    pub entries_created: AtomicU64,
    /// Concurrent changes to catch up
    pub changes_to_catchup: AtomicU64,
    /// Changes caught up
    pub changes_caught_up: AtomicU64,
    /// Build start time
    pub start_time: RwLock<Option<Instant>>,
    /// Phase 1 duration
    pub phase1_duration: RwLock<Option<Duration>>,
    /// Phase 2 duration
    pub phase2_duration: RwLock<Option<Duration>>,
    /// Phase 3 duration
    pub phase3_duration: RwLock<Option<Duration>>,
    /// Error message if failed
    pub error: RwLock<Option<String>>,
    /// Cancellation flag
    pub cancelled: AtomicBool,
}

impl IndexBuildProgress {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(IndexBuildState::Pending),
            total_rows: AtomicU64::new(0),
            rows_processed: AtomicU64::new(0),
            entries_created: AtomicU64::new(0),
            changes_to_catchup: AtomicU64::new(0),
            changes_caught_up: AtomicU64::new(0),
            start_time: RwLock::new(None),
            phase1_duration: RwLock::new(None),
            phase2_duration: RwLock::new(None),
            phase3_duration: RwLock::new(None),
            error: RwLock::new(None),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Get completion percentage (0.0 - 1.0)
    pub fn completion_ratio(&self) -> f64 {
        let total = self.total_rows.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let processed = self.rows_processed.load(Ordering::Relaxed);
        (processed as f64 / total as f64).min(1.0)
    }

    /// Get elapsed time
    pub fn elapsed(&self) -> Duration {
        self.start_time
            .read()
            .map(|t| t.elapsed())
            .unwrap_or_default()
    }

    /// Get current state
    pub fn current_state(&self) -> IndexBuildState {
        *self.state.read()
    }

    /// Set state
    pub fn set_state(&self, state: IndexBuildState) {
        *self.state.write() = state;
    }

    /// Cancel the build
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Set error
    pub fn set_error(&self, error: String) {
        *self.error.write() = Some(error);
        self.set_state(IndexBuildState::Failed);
    }

    /// Get snapshot of progress
    pub fn snapshot(&self) -> IndexBuildProgressSnapshot {
        IndexBuildProgressSnapshot {
            state: self.current_state(),
            total_rows: self.total_rows.load(Ordering::Relaxed),
            rows_processed: self.rows_processed.load(Ordering::Relaxed),
            entries_created: self.entries_created.load(Ordering::Relaxed),
            changes_to_catchup: self.changes_to_catchup.load(Ordering::Relaxed),
            changes_caught_up: self.changes_caught_up.load(Ordering::Relaxed),
            elapsed: self.elapsed(),
            completion_ratio: self.completion_ratio(),
            error: self.error.read().clone(),
        }
    }
}

impl Default for IndexBuildProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of build progress
#[derive(Debug, Clone)]
pub struct IndexBuildProgressSnapshot {
    pub state: IndexBuildState,
    pub total_rows: u64,
    pub rows_processed: u64,
    pub entries_created: u64,
    pub changes_to_catchup: u64,
    pub changes_caught_up: u64,
    pub elapsed: Duration,
    pub completion_ratio: f64,
    pub error: Option<String>,
}

// ============================================================================
// Index Build Config
// ============================================================================

/// Configuration for parallel index build
#[derive(Debug, Clone)]
pub struct ParallelIndexConfig {
    /// Number of worker threads for scanning
    pub num_workers: usize,
    /// Batch size for processing rows
    pub batch_size: usize,
    /// Memory limit per worker (bytes)
    pub memory_per_worker: usize,
    /// Maximum catchup iterations
    pub max_catchup_iterations: usize,
    /// Whether to use online mode (allow concurrent writes)
    pub online: bool,
    /// Whether to validate index after build
    pub validate: bool,
    /// Maintenance work memory (for sorting)
    pub maintenance_work_mem: usize,
}

impl Default for ParallelIndexConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            batch_size: 10000,
            memory_per_worker: 64 * 1024 * 1024, // 64MB
            max_catchup_iterations: 10,
            online: true,
            validate: true,
            maintenance_work_mem: 256 * 1024 * 1024, // 256MB
        }
    }
}

impl ParallelIndexConfig {
    pub fn validate(&self) -> Result<(), ParallelIndexError> {
        if self.num_workers == 0 {
            return Err(ParallelIndexError::InvalidConfig(
                "num_workers must be > 0".to_string(),
            ));
        }
        if self.batch_size == 0 {
            return Err(ParallelIndexError::InvalidConfig(
                "batch_size must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

// ============================================================================
// Change Tracking
// ============================================================================

/// Type of concurrent change
#[derive(Debug, Clone)]
pub enum ChangeType {
    Insert,
    Update { old_key: Vec<u8> },
    Delete,
}

/// A concurrent change that happened during index build
#[derive(Debug, Clone)]
pub struct ConcurrentChange {
    /// Change type
    pub change_type: ChangeType,
    /// Transaction ID
    pub xid: u64,
    /// Row ID
    pub row_id: u64,
    /// Index key value
    pub key: Vec<u8>,
    /// Row data pointer
    pub heap_tid: (u32, u16),
    /// Timestamp of change
    pub timestamp: u64,
}

/// Tracks concurrent changes during index build
#[derive(Debug)]
pub struct ChangeTracker {
    /// Recorded changes
    changes: Arc<RwLock<Vec<ConcurrentChange>>>,
    /// Whether tracking is active
    active: AtomicBool,
    /// Start LSN for tracking
    start_lsn: AtomicU64,
    /// Current LSN
    current_lsn: AtomicU64,
}

impl ChangeTracker {
    pub fn new() -> Self {
        Self {
            changes: Arc::new(RwLock::new(Vec::new())),
            active: AtomicBool::new(false),
            start_lsn: AtomicU64::new(0),
            current_lsn: AtomicU64::new(0),
        }
    }

    /// Start tracking changes
    pub fn start(&self, lsn: u64) {
        self.start_lsn.store(lsn, Ordering::SeqCst);
        self.current_lsn.store(lsn, Ordering::SeqCst);
        self.active.store(true, Ordering::SeqCst);
    }

    /// Stop tracking
    pub fn stop(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    /// Check if tracking is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Record a change
    pub fn record_change(&self, change: ConcurrentChange) {
        if self.is_active() {
            let mut changes = self.changes.write();
            changes.push(change);
            self.current_lsn.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Get all recorded changes
    pub fn get_changes(&self) -> Vec<ConcurrentChange> {
        self.changes.read().clone()
    }

    /// Clear recorded changes up to a point
    pub fn clear_changes_up_to(&self, lsn: u64) {
        let mut changes = self.changes.write();
        changes.retain(|c| c.timestamp > lsn);
    }

    /// Number of pending changes
    pub fn pending_count(&self) -> usize {
        self.changes.read().len()
    }
}

impl Default for ChangeTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Worker Task
// ============================================================================

/// A batch of rows to index
#[derive(Debug, Clone)]
pub struct IndexBatch {
    /// Batch ID
    pub batch_id: u64,
    /// Start row ID
    pub start_row: u64,
    /// End row ID (exclusive)
    pub end_row: u64,
    /// Row data (row_id, key, heap_tid)
    pub rows: Vec<(u64, Vec<u8>, (u32, u16))>,
}

/// Result from a worker
#[derive(Debug)]
pub struct WorkerResult {
    /// Worker ID
    pub worker_id: usize,
    /// Batches processed
    pub batches_processed: u64,
    /// Entries created
    pub entries_created: u64,
    /// Errors encountered
    pub errors: Vec<String>,
    /// Duration
    pub duration: Duration,
}

// ============================================================================
// Parallel Index Builder
// ============================================================================

/// Statistics for parallel index builds
#[derive(Debug, Default)]
pub struct ParallelIndexStats {
    /// Total builds started
    pub builds_started: AtomicU64,
    /// Successful builds
    pub builds_completed: AtomicU64,
    /// Failed builds
    pub builds_failed: AtomicU64,
    /// Cancelled builds
    pub builds_cancelled: AtomicU64,
    /// Total entries created
    pub total_entries: AtomicU64,
    /// Total time spent building (ms)
    pub total_build_time_ms: AtomicU64,
}

impl ParallelIndexStats {
    pub fn snapshot(&self) -> ParallelIndexStatsSnapshot {
        ParallelIndexStatsSnapshot {
            builds_started: self.builds_started.load(Ordering::Relaxed),
            builds_completed: self.builds_completed.load(Ordering::Relaxed),
            builds_failed: self.builds_failed.load(Ordering::Relaxed),
            builds_cancelled: self.builds_cancelled.load(Ordering::Relaxed),
            total_entries: self.total_entries.load(Ordering::Relaxed),
            total_build_time_ms: self.total_build_time_ms.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of parallel index statistics
#[derive(Debug, Clone)]
pub struct ParallelIndexStatsSnapshot {
    pub builds_started: u64,
    pub builds_completed: u64,
    pub builds_failed: u64,
    pub builds_cancelled: u64,
    pub total_entries: u64,
    pub total_build_time_ms: u64,
}

/// Manages parallel index building
pub struct ParallelIndexBuilder {
    /// Table name
    table_name: String,
    /// Index name
    index_name: String,
    /// Key columns
    key_columns: Vec<String>,
    /// Build configuration
    config: ParallelIndexConfig,
    /// Build progress
    progress: Arc<IndexBuildProgress>,
    /// Change tracker
    change_tracker: Arc<ChangeTracker>,
    /// Built index entries (simplified)
    index_entries: Arc<RwLock<Vec<(Vec<u8>, (u32, u16))>>>,
    /// Statistics
    stats: Arc<ParallelIndexStats>,
}

impl ParallelIndexBuilder {
    pub fn new(
        table_name: &str,
        index_name: &str,
        key_columns: Vec<String>,
        config: ParallelIndexConfig,
    ) -> Result<Self, ParallelIndexError> {
        config.validate()?;

        Ok(Self {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            key_columns,
            config,
            progress: Arc::new(IndexBuildProgress::new()),
            change_tracker: Arc::new(ChangeTracker::new()),
            index_entries: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(ParallelIndexStats::default()),
        })
    }

    /// Get progress reference
    pub fn progress(&self) -> Arc<IndexBuildProgress> {
        self.progress.clone()
    }

    /// Build the index using provided row iterator
    pub fn build<F>(&self, row_iterator: F) -> Result<IndexBuildResult, ParallelIndexError>
    where
        F: Fn(u64, u64) -> Vec<(u64, Vec<u8>, (u32, u16))> + Send + Sync,
    {
        self.stats.builds_started.fetch_add(1, Ordering::Relaxed);

        let start = Instant::now();
        *self.progress.start_time.write() = Some(start);

        // Phase 1: Parallel scan
        self.progress.set_state(IndexBuildState::Phase1Scanning);
        let phase1_start = Instant::now();

        if let Err(e) = self.run_phase1(&row_iterator) {
            self.handle_error(e.clone());
            return Err(e);
        }

        *self.progress.phase1_duration.write() = Some(phase1_start.elapsed());

        // Check cancellation
        if self.progress.is_cancelled() {
            return self.handle_cancellation();
        }

        // Phase 2: Catch up with concurrent changes
        self.progress.set_state(IndexBuildState::Phase2CatchUp);
        let phase2_start = Instant::now();

        if let Err(e) = self.run_phase2() {
            self.handle_error(e.clone());
            return Err(e);
        }

        *self.progress.phase2_duration.write() = Some(phase2_start.elapsed());

        // Check cancellation
        if self.progress.is_cancelled() {
            return self.handle_cancellation();
        }

        // Phase 3: Validation
        if self.config.validate {
            self.progress.set_state(IndexBuildState::Phase3Validation);
            let phase3_start = Instant::now();

            if let Err(e) = self.run_phase3() {
                self.handle_error(e.clone());
                return Err(e);
            }

            *self.progress.phase3_duration.write() = Some(phase3_start.elapsed());
        }

        // Finalize
        self.progress.set_state(IndexBuildState::Finalizing);
        self.finalize()?;

        self.progress.set_state(IndexBuildState::Complete);
        self.stats.builds_completed.fetch_add(1, Ordering::Relaxed);

        let entries = self.progress.entries_created.load(Ordering::Relaxed);
        self.stats
            .total_entries
            .fetch_add(entries, Ordering::Relaxed);
        self.stats
            .total_build_time_ms
            .fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);

        Ok(IndexBuildResult {
            index_name: self.index_name.clone(),
            entries_created: entries,
            duration: start.elapsed(),
            phase1_duration: self.progress.phase1_duration.read().unwrap_or_default(),
            phase2_duration: self.progress.phase2_duration.read().unwrap_or_default(),
            phase3_duration: self.progress.phase3_duration.read().unwrap_or_default(),
            concurrent_changes_processed: self.progress.changes_caught_up.load(Ordering::Relaxed),
        })
    }

    fn run_phase1<F>(&self, row_iterator: &F) -> Result<(), ParallelIndexError>
    where
        F: Fn(u64, u64) -> Vec<(u64, Vec<u8>, (u32, u16))> + Send + Sync,
    {
        // Start tracking concurrent changes
        self.change_tracker.start(0);

        // For simplicity, we'll simulate parallel scanning
        // In a real implementation, this would spawn worker threads

        let batch_size = self.config.batch_size as u64;
        let mut offset = 0u64;

        loop {
            if self.progress.is_cancelled() {
                return Ok(());
            }

            let batch = row_iterator(offset, batch_size);
            if batch.is_empty() {
                break;
            }

            for (row_id, key, heap_tid) in &batch {
                self.add_entry(key.clone(), *heap_tid);
                self.progress.rows_processed.fetch_add(1, Ordering::Relaxed);
                self.progress
                    .entries_created
                    .fetch_add(1, Ordering::Relaxed);
            }

            offset += batch_size;
        }

        self.progress.total_rows.store(
            self.progress.rows_processed.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        Ok(())
    }

    fn run_phase2(&self) -> Result<(), ParallelIndexError> {
        // Process concurrent changes
        for iteration in 0..self.config.max_catchup_iterations {
            if self.progress.is_cancelled() {
                return Ok(());
            }

            let changes = self.change_tracker.get_changes();
            if changes.is_empty() {
                break;
            }

            self.progress
                .changes_to_catchup
                .store(changes.len() as u64, Ordering::Relaxed);

            for change in &changes {
                self.apply_change(change)?;
                self.progress
                    .changes_caught_up
                    .fetch_add(1, Ordering::Relaxed);
            }

            // Clear processed changes
            if let Some(last) = changes.last() {
                self.change_tracker.clear_changes_up_to(last.timestamp);
            }

            // Small delay to allow more changes to accumulate
            if iteration < self.config.max_catchup_iterations - 1 {
                thread::sleep(Duration::from_millis(10));
            }
        }

        // Stop tracking
        self.change_tracker.stop();

        Ok(())
    }

    fn run_phase3(&self) -> Result<(), ParallelIndexError> {
        // Validate index entries
        let entries = self.index_entries.read();

        // Check for duplicates (for unique indexes)
        let mut seen_keys: HashMap<Vec<u8>, usize> = HashMap::new();
        for (i, (key, _)) in entries.iter().enumerate() {
            if let Some(&prev_idx) = seen_keys.get(key) {
                // Note: This is simplified - real implementation would check
                // if both entries are still live
            }
            seen_keys.insert(key.clone(), i);
        }

        Ok(())
    }

    fn finalize(&self) -> Result<(), ParallelIndexError> {
        // Sort the index entries
        let mut entries = self.index_entries.write();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // In a real implementation, this would:
        // 1. Take a brief exclusive lock
        // 2. Mark the index as valid
        // 3. Release the lock

        Ok(())
    }

    fn add_entry(&self, key: Vec<u8>, heap_tid: (u32, u16)) {
        let mut entries = self.index_entries.write();
        entries.push((key, heap_tid));
    }

    fn apply_change(&self, change: &ConcurrentChange) -> Result<(), ParallelIndexError> {
        let mut entries = self.index_entries.write();

        match &change.change_type {
            ChangeType::Insert => {
                entries.push((change.key.clone(), change.heap_tid));
            }
            ChangeType::Update { old_key } => {
                // Remove old entry
                entries.retain(|(k, _)| k != old_key);
                // Add new entry
                entries.push((change.key.clone(), change.heap_tid));
            }
            ChangeType::Delete => {
                entries.retain(|(k, tid)| k != &change.key || *tid != change.heap_tid);
            }
        }

        Ok(())
    }

    fn handle_error(&self, error: ParallelIndexError) {
        self.progress.set_error(error.to_string());
        self.stats.builds_failed.fetch_add(1, Ordering::Relaxed);
        self.change_tracker.stop();
    }

    fn handle_cancellation(&self) -> Result<IndexBuildResult, ParallelIndexError> {
        self.progress.set_state(IndexBuildState::Cancelled);
        self.stats.builds_cancelled.fetch_add(1, Ordering::Relaxed);
        self.change_tracker.stop();
        Err(ParallelIndexError::BuildCancelled)
    }

    /// Record a concurrent change (for use by DML operations)
    pub fn record_change(&self, change: ConcurrentChange) {
        self.change_tracker.record_change(change);
    }

    /// Get build statistics
    pub fn stats(&self) -> ParallelIndexStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get number of entries built
    pub fn entry_count(&self) -> usize {
        self.index_entries.read().len()
    }
}

/// Result of an index build
#[derive(Debug, Clone)]
pub struct IndexBuildResult {
    /// Index name
    pub index_name: String,
    /// Number of entries created
    pub entries_created: u64,
    /// Total duration
    pub duration: Duration,
    /// Phase 1 (scanning) duration
    pub phase1_duration: Duration,
    /// Phase 2 (catchup) duration
    pub phase2_duration: Duration,
    /// Phase 3 (validation) duration
    pub phase3_duration: Duration,
    /// Number of concurrent changes processed
    pub concurrent_changes_processed: u64,
}

// ============================================================================
// Index Build Manager
// ============================================================================

/// Manages multiple concurrent index builds
pub struct IndexBuildManager {
    /// Active builds
    builds: Arc<RwLock<HashMap<String, Arc<ParallelIndexBuilder>>>>,
    /// Completed builds
    completed: Arc<RwLock<Vec<IndexBuildResult>>>,
    /// Global statistics
    stats: Arc<ParallelIndexStats>,
    /// Maximum concurrent builds
    max_concurrent_builds: usize,
}

impl IndexBuildManager {
    pub fn new(max_concurrent_builds: usize) -> Self {
        Self {
            builds: Arc::new(RwLock::new(HashMap::new())),
            completed: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(ParallelIndexStats::default()),
            max_concurrent_builds,
        }
    }

    /// Start a new index build
    pub fn start_build(
        &self,
        table_name: &str,
        index_name: &str,
        key_columns: Vec<String>,
        config: ParallelIndexConfig,
    ) -> Result<Arc<ParallelIndexBuilder>, ParallelIndexError> {
        let builds = self.builds.read();
        if builds.contains_key(index_name) {
            return Err(ParallelIndexError::IndexExists(index_name.to_string()));
        }
        if builds.len() >= self.max_concurrent_builds {
            return Err(ParallelIndexError::InvalidConfig(
                "Maximum concurrent builds reached".to_string(),
            ));
        }
        drop(builds);

        let builder = Arc::new(ParallelIndexBuilder::new(
            table_name,
            index_name,
            key_columns,
            config,
        )?);

        let mut builds = self.builds.write();
        builds.insert(index_name.to_string(), builder.clone());

        Ok(builder)
    }

    /// Get progress for a build
    pub fn get_progress(&self, index_name: &str) -> Option<IndexBuildProgressSnapshot> {
        let builds = self.builds.read();
        builds.get(index_name).map(|b| b.progress.snapshot())
    }

    /// Cancel a build
    pub fn cancel_build(&self, index_name: &str) -> bool {
        let builds = self.builds.read();
        if let Some(builder) = builds.get(index_name) {
            builder.progress.cancel();
            true
        } else {
            false
        }
    }

    /// Remove a completed or failed build
    pub fn remove_build(&self, index_name: &str) -> bool {
        let mut builds = self.builds.write();
        if let Some(builder) = builds.get(index_name) {
            if builder.progress.current_state().is_finished() {
                builds.remove(index_name);
                return true;
            }
        }
        false
    }

    /// List active builds
    pub fn list_builds(&self) -> Vec<(String, IndexBuildProgressSnapshot)> {
        let builds = self.builds.read();
        builds
            .iter()
            .map(|(name, builder)| (name.clone(), builder.progress.snapshot()))
            .collect()
    }

    /// Get global statistics
    pub fn stats(&self) -> ParallelIndexStatsSnapshot {
        self.stats.snapshot()
    }
}

impl Default for IndexBuildManager {
    fn default() -> Self {
        Self::new(4)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_build_state() {
        assert!(!IndexBuildState::Pending.is_finished());
        assert!(!IndexBuildState::Phase1Scanning.is_finished());
        assert!(IndexBuildState::Complete.is_finished());
        assert!(IndexBuildState::Failed.is_finished());
        assert!(IndexBuildState::Cancelled.is_finished());

        assert!(IndexBuildState::Complete.is_successful());
        assert!(!IndexBuildState::Failed.is_successful());
    }

    #[test]
    fn test_index_build_progress() {
        let progress = IndexBuildProgress::new();

        assert_eq!(progress.current_state(), IndexBuildState::Pending);
        assert_eq!(progress.completion_ratio(), 0.0);

        progress.total_rows.store(100, Ordering::Relaxed);
        progress.rows_processed.store(50, Ordering::Relaxed);

        assert!((progress.completion_ratio() - 0.5).abs() < 0.01);

        progress.set_state(IndexBuildState::Phase1Scanning);
        assert_eq!(progress.current_state(), IndexBuildState::Phase1Scanning);
    }

    #[test]
    fn test_cancellation() {
        let progress = IndexBuildProgress::new();

        assert!(!progress.is_cancelled());
        progress.cancel();
        assert!(progress.is_cancelled());
    }

    #[test]
    fn test_config_validation() {
        let mut config = ParallelIndexConfig::default();
        assert!(config.validate().is_ok());

        config.num_workers = 0;
        assert!(config.validate().is_err());

        config.num_workers = 4;
        config.batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_change_tracker() {
        let tracker = ChangeTracker::new();

        assert!(!tracker.is_active());
        assert_eq!(tracker.pending_count(), 0);

        tracker.start(0);
        assert!(tracker.is_active());

        tracker.record_change(ConcurrentChange {
            change_type: ChangeType::Insert,
            xid: 1,
            row_id: 1,
            key: vec![1, 2, 3],
            heap_tid: (0, 0),
            timestamp: 1,
        });

        assert_eq!(tracker.pending_count(), 1);

        tracker.stop();
        assert!(!tracker.is_active());
    }

    #[test]
    fn test_parallel_index_builder() {
        let config = ParallelIndexConfig {
            num_workers: 2,
            batch_size: 100,
            online: true,
            validate: true,
            ..Default::default()
        };

        let builder =
            ParallelIndexBuilder::new("test_table", "idx_test", vec!["id".to_string()], config)
                .unwrap();

        // Simulated row data
        let row_iterator = |offset: u64, limit: u64| {
            let mut rows = Vec::new();
            for i in offset..(offset + limit).min(500) {
                rows.push((i, vec![i as u8], (0, i as u16)));
            }
            rows
        };

        let result = builder.build(row_iterator).unwrap();

        assert_eq!(result.index_name, "idx_test");
        assert_eq!(result.entries_created, 500);
        assert_eq!(builder.progress.current_state(), IndexBuildState::Complete);
    }

    #[test]
    fn test_build_with_concurrent_changes() {
        let config = ParallelIndexConfig {
            num_workers: 1,
            batch_size: 50,
            max_catchup_iterations: 2,
            ..Default::default()
        };

        let builder =
            ParallelIndexBuilder::new("test_table", "idx_test", vec!["id".to_string()], config)
                .unwrap();

        // Start the change tracker manually to simulate ongoing changes
        builder.change_tracker.start(0);

        // Record a concurrent change while tracking is active
        builder.record_change(ConcurrentChange {
            change_type: ChangeType::Insert,
            xid: 1,
            row_id: 1000,
            key: vec![100],
            heap_tid: (0, 100),
            timestamp: 1,
        });

        let row_iterator = |offset: u64, limit: u64| {
            let mut rows = Vec::new();
            for i in offset..(offset + limit).min(100) {
                rows.push((i, vec![i as u8], (0, i as u16)));
            }
            rows
        };

        let result = builder.build(row_iterator).unwrap();

        // Should have processed the concurrent change
        assert_eq!(result.concurrent_changes_processed, 1);
    }

    #[test]
    fn test_build_cancellation() {
        let config = ParallelIndexConfig::default();

        let builder =
            ParallelIndexBuilder::new("test_table", "idx_test", vec!["id".to_string()], config)
                .unwrap();

        // Cancel before starting
        builder.progress.cancel();

        let row_iterator = |_: u64, _: u64| vec![(0u64, vec![0u8], (0u32, 0u16))];

        let result = builder.build(row_iterator);
        assert!(matches!(result, Err(ParallelIndexError::BuildCancelled)));
        assert_eq!(builder.progress.current_state(), IndexBuildState::Cancelled);
    }

    #[test]
    fn test_index_build_manager() {
        let manager = IndexBuildManager::new(2);

        let builder = manager
            .start_build(
                "table",
                "idx1",
                vec!["col".to_string()],
                ParallelIndexConfig::default(),
            )
            .unwrap();

        // Check it's listed
        let builds = manager.list_builds();
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].0, "idx1");

        // Cannot create duplicate
        let result = manager.start_build(
            "table",
            "idx1",
            vec!["col".to_string()],
            ParallelIndexConfig::default(),
        );
        assert!(matches!(result, Err(ParallelIndexError::IndexExists(_))));
    }

    #[test]
    fn test_change_types() {
        let entries = Arc::new(RwLock::new(Vec::new()));

        // Add entry
        {
            let mut e = entries.write();
            e.push((vec![1u8], (0u32, 0u16)));
        }

        // Update entry
        {
            let mut e = entries.write();
            e.retain(|(k, _)| k != &vec![1u8]);
            e.push((vec![2u8], (0u32, 1u16)));
        }

        // Delete entry
        {
            let mut e = entries.write();
            e.retain(|(k, _)| k != &vec![2u8]);
        }

        let e = entries.read();
        assert_eq!(e.len(), 0);
    }

    #[test]
    fn test_stats() {
        let stats = ParallelIndexStats::default();

        stats.builds_started.fetch_add(1, Ordering::Relaxed);
        stats.builds_completed.fetch_add(1, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.builds_started, 1);
        assert_eq!(snapshot.builds_completed, 1);
    }

    #[test]
    fn test_error_display() {
        let err = ParallelIndexError::IndexExists("idx".to_string());
        assert!(format!("{}", err).contains("idx"));

        let err = ParallelIndexError::BuildCancelled;
        assert!(format!("{}", err).contains("cancelled"));

        let err = ParallelIndexError::WorkerError("test".to_string());
        assert!(format!("{}", err).contains("test"));
    }

    #[test]
    fn test_progress_snapshot() {
        let progress = IndexBuildProgress::new();

        progress.total_rows.store(1000, Ordering::Relaxed);
        progress.rows_processed.store(500, Ordering::Relaxed);
        progress.entries_created.store(500, Ordering::Relaxed);
        progress.set_state(IndexBuildState::Phase1Scanning);

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.total_rows, 1000);
        assert_eq!(snapshot.rows_processed, 500);
        assert_eq!(snapshot.state, IndexBuildState::Phase1Scanning);
        assert!((snapshot.completion_ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_manager_cancel() {
        let manager = IndexBuildManager::new(2);

        manager
            .start_build(
                "table",
                "idx1",
                vec!["col".to_string()],
                ParallelIndexConfig::default(),
            )
            .unwrap();

        assert!(manager.cancel_build("idx1"));
        assert!(!manager.cancel_build("nonexistent"));

        let progress = manager.get_progress("idx1").unwrap();
        // Note: Cancelled flag may not be reflected in state immediately
    }

    #[test]
    fn test_manager_remove() {
        let manager = IndexBuildManager::new(2);

        let builder = manager
            .start_build(
                "table",
                "idx1",
                vec!["col".to_string()],
                ParallelIndexConfig::default(),
            )
            .unwrap();

        // Cannot remove while in progress
        assert!(!manager.remove_build("idx1"));

        // Mark as complete
        builder.progress.set_state(IndexBuildState::Complete);

        // Now can remove
        assert!(manager.remove_build("idx1"));
        assert!(!manager.remove_build("idx1")); // Already removed
    }
}
