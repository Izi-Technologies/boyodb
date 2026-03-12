//! VACUUM and ANALYZE - Table Maintenance and Statistics Collection
//!
//! This module provides:
//! - VACUUM for dead tuple cleanup and space reclamation
//! - ANALYZE for statistics collection
//! - Autovacuum daemon for automatic maintenance
//! - VACUUM FULL for complete table rewrite

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// VACUUM CONFIGURATION
// ============================================================================

/// VACUUM options
#[derive(Debug, Clone)]
pub struct VacuumOptions {
    /// VACUUM FULL - rewrites the entire table
    pub full: bool,
    /// FREEZE - aggressively freeze tuples
    pub freeze: bool,
    /// VERBOSE - output progress
    pub verbose: bool,
    /// ANALYZE - update statistics after vacuum
    pub analyze: bool,
    /// DISABLE_PAGE_SKIPPING - don't skip any pages
    pub disable_page_skipping: bool,
    /// SKIP_LOCKED - skip locked tables
    pub skip_locked: bool,
    /// INDEX_CLEANUP - clean up indexes
    pub index_cleanup: IndexCleanup,
    /// TRUNCATE - truncate empty pages at end
    pub truncate: bool,
    /// PARALLEL - number of parallel workers
    pub parallel: Option<usize>,
    /// BUFFER_USAGE_LIMIT - limit buffer usage
    pub buffer_usage_limit: Option<u64>,
}

impl Default for VacuumOptions {
    fn default() -> Self {
        Self {
            full: false,
            freeze: false,
            verbose: false,
            analyze: false,
            disable_page_skipping: false,
            skip_locked: false,
            index_cleanup: IndexCleanup::Auto,
            truncate: true,
            parallel: None,
            buffer_usage_limit: None,
        }
    }
}

impl VacuumOptions {
    /// Create VACUUM FULL options
    pub fn full() -> Self {
        Self {
            full: true,
            ..Default::default()
        }
    }

    /// Create VACUUM ANALYZE options
    pub fn analyze() -> Self {
        Self {
            analyze: true,
            ..Default::default()
        }
    }

    /// Create VACUUM FREEZE options
    pub fn freeze() -> Self {
        Self {
            freeze: true,
            ..Default::default()
        }
    }
}

/// Index cleanup behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCleanup {
    /// Automatic decision
    Auto,
    /// Always clean up indexes
    On,
    /// Never clean up indexes
    Off,
}

// ============================================================================
// ANALYZE CONFIGURATION
// ============================================================================

/// ANALYZE options
#[derive(Debug, Clone)]
pub struct AnalyzeOptions {
    /// VERBOSE - output progress
    pub verbose: bool,
    /// SKIP_LOCKED - skip locked tables
    pub skip_locked: bool,
    /// Specific columns to analyze (empty = all)
    pub columns: Vec<String>,
    /// Sample size (number of pages)
    pub sample_size: u32,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            skip_locked: false,
            columns: Vec::new(),
            sample_size: 30000, // default_statistics_target * 300
        }
    }
}

// ============================================================================
// TABLE STATISTICS
// ============================================================================

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name
    pub column_name: String,
    /// Null fraction (0.0 - 1.0)
    pub null_frac: f64,
    /// Average width in bytes
    pub avg_width: u32,
    /// Number of distinct values (-1 = unique)
    pub n_distinct: f64,
    /// Most common values
    pub most_common_vals: Vec<String>,
    /// Most common frequencies
    pub most_common_freqs: Vec<f64>,
    /// Histogram bounds
    pub histogram_bounds: Vec<String>,
    /// Correlation with physical order
    pub correlation: f64,
}

impl ColumnStats {
    /// Create new column statistics
    pub fn new(column_name: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
            null_frac: 0.0,
            avg_width: 0,
            n_distinct: 0.0,
            most_common_vals: Vec::new(),
            most_common_freqs: Vec::new(),
            histogram_bounds: Vec::new(),
            correlation: 0.0,
        }
    }

    /// Estimate selectivity for equality predicate
    pub fn estimate_equality_selectivity(&self, value: &str) -> f64 {
        // Check if value is in MCV list
        for (i, mcv) in self.most_common_vals.iter().enumerate() {
            if mcv == value {
                return self.most_common_freqs.get(i).copied().unwrap_or(0.0);
            }
        }

        // Estimate from n_distinct
        if self.n_distinct > 0.0 {
            let mcv_freq: f64 = self.most_common_freqs.iter().sum();
            (1.0 - mcv_freq) / self.n_distinct
        } else {
            0.01 // Default selectivity
        }
    }

    /// Estimate selectivity for range predicate
    pub fn estimate_range_selectivity(&self, low: Option<&str>, high: Option<&str>) -> f64 {
        if self.histogram_bounds.is_empty() {
            return 0.5; // Default
        }

        let num_buckets = self.histogram_bounds.len() - 1;
        if num_buckets == 0 {
            return 0.5;
        }

        let bucket_selectivity = 1.0 / num_buckets as f64;

        // Count buckets in range
        let mut count = 0;
        for i in 0..num_buckets {
            let bucket_low = &self.histogram_bounds[i];
            let bucket_high = &self.histogram_bounds[i + 1];

            let matches_low = low.map(|l| bucket_high.as_str() > l).unwrap_or(true);
            let matches_high = high.map(|h| bucket_low.as_str() < h).unwrap_or(true);

            if matches_low && matches_high {
                count += 1;
            }
        }

        count as f64 * bucket_selectivity
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Table name
    pub table_name: String,
    /// Schema name
    pub schema_name: String,
    /// Number of live tuples
    pub n_live_tup: u64,
    /// Number of dead tuples
    pub n_dead_tup: u64,
    /// Number of modifications since last analyze
    pub n_mod_since_analyze: u64,
    /// Number of inserts since last vacuum
    pub n_ins_since_vacuum: u64,
    /// Last vacuum time
    pub last_vacuum: Option<SystemTime>,
    /// Last auto vacuum time
    pub last_autovacuum: Option<SystemTime>,
    /// Last analyze time
    pub last_analyze: Option<SystemTime>,
    /// Last auto analyze time
    pub last_autoanalyze: Option<SystemTime>,
    /// Vacuum count
    pub vacuum_count: u64,
    /// Autovacuum count
    pub autovacuum_count: u64,
    /// Analyze count
    pub analyze_count: u64,
    /// Autoanalyze count
    pub autoanalyze_count: u64,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStats>,
    /// Relation size in bytes
    pub rel_size: u64,
    /// Number of pages
    pub rel_pages: u64,
}

impl TableStats {
    /// Create new table statistics
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            table_name: table.to_string(),
            schema_name: schema.to_string(),
            n_live_tup: 0,
            n_dead_tup: 0,
            n_mod_since_analyze: 0,
            n_ins_since_vacuum: 0,
            last_vacuum: None,
            last_autovacuum: None,
            last_analyze: None,
            last_autoanalyze: None,
            vacuum_count: 0,
            autovacuum_count: 0,
            analyze_count: 0,
            autoanalyze_count: 0,
            column_stats: HashMap::new(),
            rel_size: 0,
            rel_pages: 0,
        }
    }

    /// Dead tuple ratio
    pub fn dead_tuple_ratio(&self) -> f64 {
        let total = self.n_live_tup + self.n_dead_tup;
        if total == 0 {
            0.0
        } else {
            self.n_dead_tup as f64 / total as f64
        }
    }

    /// Check if table needs vacuum
    pub fn needs_vacuum(&self, threshold: f64, scale_factor: f64, min_threshold: u64) -> bool {
        let threshold = (threshold + scale_factor * self.n_live_tup as f64) as u64;
        let threshold = threshold.max(min_threshold);
        self.n_dead_tup >= threshold
    }

    /// Check if table needs analyze
    pub fn needs_analyze(&self, threshold: f64, scale_factor: f64, min_threshold: u64) -> bool {
        let threshold = (threshold + scale_factor * self.n_live_tup as f64) as u64;
        let threshold = threshold.max(min_threshold);
        self.n_mod_since_analyze >= threshold
    }
}

// ============================================================================
// VACUUM EXECUTION
// ============================================================================

/// Vacuum progress
#[derive(Debug, Clone)]
pub struct VacuumProgress {
    /// Phase of vacuum
    pub phase: VacuumPhase,
    /// Heap blocks total
    pub heap_blks_total: u64,
    /// Heap blocks scanned
    pub heap_blks_scanned: u64,
    /// Heap blocks vacuumed
    pub heap_blks_vacuumed: u64,
    /// Index vacuum count
    pub index_vacuum_count: u64,
    /// Max dead tuples
    pub max_dead_tuples: u64,
    /// Num dead tuples
    pub num_dead_tuples: u64,
}

impl VacuumProgress {
    /// Percentage complete
    pub fn percentage(&self) -> f64 {
        if self.heap_blks_total == 0 {
            0.0
        } else {
            (self.heap_blks_vacuumed as f64 / self.heap_blks_total as f64) * 100.0
        }
    }
}

impl Default for VacuumProgress {
    fn default() -> Self {
        Self {
            phase: VacuumPhase::Initializing,
            heap_blks_total: 0,
            heap_blks_scanned: 0,
            heap_blks_vacuumed: 0,
            index_vacuum_count: 0,
            max_dead_tuples: 0,
            num_dead_tuples: 0,
        }
    }
}

/// Vacuum phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumPhase {
    /// Initializing
    Initializing,
    /// Scanning heap
    ScanningHeap,
    /// Vacuuming indexes
    VacuumingIndexes,
    /// Vacuuming heap
    VacuumingHeap,
    /// Cleaning up indexes
    CleaningUpIndexes,
    /// Truncating heap
    TruncatingHeap,
    /// Performing final cleanup
    FinalCleanup,
}

impl std::fmt::Display for VacuumPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            VacuumPhase::Initializing => "initializing",
            VacuumPhase::ScanningHeap => "scanning heap",
            VacuumPhase::VacuumingIndexes => "vacuuming indexes",
            VacuumPhase::VacuumingHeap => "vacuuming heap",
            VacuumPhase::CleaningUpIndexes => "cleaning up indexes",
            VacuumPhase::TruncatingHeap => "truncating heap",
            VacuumPhase::FinalCleanup => "performing final cleanup",
        };
        write!(f, "{}", name)
    }
}

/// Vacuum result
#[derive(Debug, Clone)]
pub struct VacuumResult {
    /// Table name
    pub table_name: String,
    /// Dead tuples removed
    pub dead_tuples_removed: u64,
    /// Pages removed (VACUUM FULL or truncate)
    pub pages_removed: u64,
    /// Index entries removed
    pub index_entries_removed: u64,
    /// Duration
    pub duration: Duration,
    /// Pages scanned
    pub pages_scanned: u64,
    /// Pages vacuumed
    pub pages_vacuumed: u64,
    /// Index passes
    pub index_passes: u32,
    /// Was full vacuum
    pub was_full: bool,
}

/// VACUUM executor
pub struct VacuumExecutor {
    /// Progress
    progress: RwLock<VacuumProgress>,
    /// Cancelled flag
    cancelled: AtomicBool,
}

impl VacuumExecutor {
    /// Create a new vacuum executor
    pub fn new() -> Self {
        Self {
            progress: RwLock::new(VacuumProgress::default()),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the vacuum
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Get current progress
    pub fn progress(&self) -> VacuumProgress {
        self.progress.read().unwrap().clone()
    }

    /// Execute vacuum on a table
    pub fn vacuum(
        &self,
        table: &str,
        options: &VacuumOptions,
        stats: &mut TableStats,
    ) -> Result<VacuumResult, VacuumError> {
        let start = Instant::now();

        // Initialize
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::Initializing;
            progress.heap_blks_total = stats.rel_pages;
        }

        if self.is_cancelled() {
            return Err(VacuumError::Cancelled);
        }

        // Scanning heap
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::ScanningHeap;
        }

        let dead_tuples = stats.n_dead_tup;
        let pages = stats.rel_pages;

        // Simulate scanning
        for i in 0..pages {
            if self.is_cancelled() {
                return Err(VacuumError::Cancelled);
            }

            let mut progress = self.progress.write().unwrap();
            progress.heap_blks_scanned = i + 1;
            progress.num_dead_tuples = dead_tuples * (i + 1) / pages;
        }

        let mut index_entries_removed = 0;
        let mut index_passes = 0;

        // Vacuum indexes if needed
        if dead_tuples > 0 && options.index_cleanup != IndexCleanup::Off {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::VacuumingIndexes;
            index_entries_removed = dead_tuples * 2; // Estimate
            index_passes = 1;
            progress.index_vacuum_count = 1;
        }

        // Vacuum heap
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::VacuumingHeap;
        }

        for i in 0..pages {
            if self.is_cancelled() {
                return Err(VacuumError::Cancelled);
            }

            let mut progress = self.progress.write().unwrap();
            progress.heap_blks_vacuumed = i + 1;
        }

        let mut pages_removed = 0;

        // Truncate if requested
        if options.truncate && stats.n_dead_tup > 0 {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::TruncatingHeap;
            // Simulate truncation
            pages_removed = stats.n_dead_tup / 100; // Rough estimate
        }

        // Final cleanup
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = VacuumPhase::FinalCleanup;
        }

        // Update stats
        let dead_tuples_removed = stats.n_dead_tup;
        stats.n_dead_tup = 0;
        stats.n_ins_since_vacuum = 0;
        stats.last_vacuum = Some(SystemTime::now());
        stats.vacuum_count += 1;
        stats.rel_pages = stats.rel_pages.saturating_sub(pages_removed);

        Ok(VacuumResult {
            table_name: table.to_string(),
            dead_tuples_removed,
            pages_removed,
            index_entries_removed,
            duration: start.elapsed(),
            pages_scanned: pages,
            pages_vacuumed: pages,
            index_passes,
            was_full: options.full,
        })
    }
}

impl Default for VacuumExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ANALYZE EXECUTION
// ============================================================================

/// Analyze progress
#[derive(Debug, Clone)]
pub struct AnalyzeProgress {
    /// Current phase
    pub phase: AnalyzePhase,
    /// Sample blocks required
    pub sample_blks_total: u64,
    /// Sample blocks scanned
    pub sample_blks_scanned: u64,
    /// Rows sampled
    pub rows_sampled: u64,
    /// Current column being analyzed
    pub current_column: Option<String>,
}

impl Default for AnalyzeProgress {
    fn default() -> Self {
        Self {
            phase: AnalyzePhase::Initializing,
            sample_blks_total: 0,
            sample_blks_scanned: 0,
            rows_sampled: 0,
            current_column: None,
        }
    }
}

/// Analyze phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzePhase {
    /// Initializing
    Initializing,
    /// Acquiring sample rows
    AcquiringSample,
    /// Computing statistics
    ComputingStats,
    /// Finalizing
    Finalizing,
}

/// Analyze result
#[derive(Debug, Clone)]
pub struct AnalyzeResult {
    /// Table name
    pub table_name: String,
    /// Rows sampled
    pub rows_sampled: u64,
    /// Pages sampled
    pub pages_sampled: u64,
    /// Columns analyzed
    pub columns_analyzed: Vec<String>,
    /// Duration
    pub duration: Duration,
}

/// ANALYZE executor
pub struct AnalyzeExecutor {
    /// Progress
    progress: RwLock<AnalyzeProgress>,
    /// Cancelled flag
    cancelled: AtomicBool,
}

impl AnalyzeExecutor {
    /// Create a new analyze executor
    pub fn new() -> Self {
        Self {
            progress: RwLock::new(AnalyzeProgress::default()),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel the analyze
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Get current progress
    pub fn progress(&self) -> AnalyzeProgress {
        self.progress.read().unwrap().clone()
    }

    /// Execute analyze on a table
    pub fn analyze(
        &self,
        table: &str,
        columns: &[String],
        options: &AnalyzeOptions,
        stats: &mut TableStats,
    ) -> Result<AnalyzeResult, VacuumError> {
        let start = Instant::now();

        // Initialize
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = AnalyzePhase::Initializing;
            progress.sample_blks_total = options.sample_size as u64;
        }

        if self.cancelled.load(Ordering::SeqCst) {
            return Err(VacuumError::Cancelled);
        }

        // Acquire sample
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = AnalyzePhase::AcquiringSample;
        }

        let pages_to_sample = (options.sample_size as u64).min(stats.rel_pages);
        let rows_per_page = if stats.rel_pages > 0 {
            stats.n_live_tup / stats.rel_pages
        } else {
            0
        };
        let rows_sampled = pages_to_sample * rows_per_page;

        for i in 0..pages_to_sample {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(VacuumError::Cancelled);
            }

            let mut progress = self.progress.write().unwrap();
            progress.sample_blks_scanned = i + 1;
            progress.rows_sampled = (i + 1) * rows_per_page;
        }

        // Compute statistics
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = AnalyzePhase::ComputingStats;
        }

        let columns_to_analyze: Vec<String> = if columns.is_empty() {
            // Analyze all columns
            stats.column_stats.keys().cloned().collect()
        } else {
            columns.to_vec()
        };

        for column in &columns_to_analyze {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(VacuumError::Cancelled);
            }

            {
                let mut progress = self.progress.write().unwrap();
                progress.current_column = Some(column.clone());
            }

            // Generate mock statistics
            let col_stats = ColumnStats {
                column_name: column.clone(),
                null_frac: 0.01,
                avg_width: 8,
                n_distinct: (stats.n_live_tup as f64 * 0.1).max(1.0),
                most_common_vals: vec!["val1".to_string(), "val2".to_string()],
                most_common_freqs: vec![0.1, 0.05],
                histogram_bounds: (0..11).map(|i| i.to_string()).collect(),
                correlation: 0.9,
            };

            stats.column_stats.insert(column.clone(), col_stats);
        }

        // Finalize
        {
            let mut progress = self.progress.write().unwrap();
            progress.phase = AnalyzePhase::Finalizing;
            progress.current_column = None;
        }

        // Update stats
        stats.n_mod_since_analyze = 0;
        stats.last_analyze = Some(SystemTime::now());
        stats.analyze_count += 1;

        Ok(AnalyzeResult {
            table_name: table.to_string(),
            rows_sampled,
            pages_sampled: pages_to_sample,
            columns_analyzed: columns_to_analyze,
            duration: start.elapsed(),
        })
    }
}

impl Default for AnalyzeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// AUTOVACUUM
// ============================================================================

/// Autovacuum configuration
#[derive(Debug, Clone)]
pub struct AutovacuumConfig {
    /// Enable autovacuum
    pub enabled: bool,
    /// Naptime between runs (seconds)
    pub naptime: Duration,
    /// Maximum workers
    pub max_workers: usize,
    /// Vacuum threshold
    pub vacuum_threshold: u64,
    /// Vacuum scale factor
    pub vacuum_scale_factor: f64,
    /// Analyze threshold
    pub analyze_threshold: u64,
    /// Analyze scale factor
    pub analyze_scale_factor: f64,
    /// Freeze max age
    pub freeze_max_age: u64,
    /// Multixact freeze max age
    pub multixact_freeze_max_age: u64,
    /// Vacuum cost delay (ms)
    pub vacuum_cost_delay: Duration,
    /// Vacuum cost limit
    pub vacuum_cost_limit: u32,
}

impl Default for AutovacuumConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            naptime: Duration::from_secs(60),
            max_workers: 3,
            vacuum_threshold: 50,
            vacuum_scale_factor: 0.2,
            analyze_threshold: 50,
            analyze_scale_factor: 0.1,
            freeze_max_age: 200_000_000,
            multixact_freeze_max_age: 400_000_000,
            vacuum_cost_delay: Duration::from_millis(2),
            vacuum_cost_limit: 200,
        }
    }
}

/// Autovacuum worker status
#[derive(Debug, Clone)]
pub struct AutovacuumWorker {
    /// Worker ID
    pub id: usize,
    /// Current table
    pub current_table: Option<String>,
    /// Current operation
    pub current_op: Option<AutovacuumOp>,
    /// Start time
    pub start_time: Option<Instant>,
    /// Tables processed
    pub tables_processed: u64,
}

/// Autovacuum operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutovacuumOp {
    Vacuum,
    Analyze,
    VacuumAnalyze,
    Freeze,
}

/// Autovacuum daemon
pub struct AutovacuumDaemon {
    /// Configuration
    config: AutovacuumConfig,
    /// Running flag
    running: AtomicBool,
    /// Workers
    workers: RwLock<Vec<AutovacuumWorker>>,
    /// Tables needing vacuum
    vacuum_candidates: RwLock<HashSet<String>>,
    /// Tables needing analyze
    analyze_candidates: RwLock<HashSet<String>>,
    /// Total vacuums performed
    total_vacuums: AtomicU64,
    /// Total analyzes performed
    total_analyzes: AtomicU64,
}

impl AutovacuumDaemon {
    /// Create a new autovacuum daemon
    pub fn new(config: AutovacuumConfig) -> Self {
        let mut workers = Vec::with_capacity(config.max_workers);
        for i in 0..config.max_workers {
            workers.push(AutovacuumWorker {
                id: i,
                current_table: None,
                current_op: None,
                start_time: None,
                tables_processed: 0,
            });
        }

        Self {
            config,
            running: AtomicBool::new(false),
            workers: RwLock::new(workers),
            vacuum_candidates: RwLock::new(HashSet::new()),
            analyze_candidates: RwLock::new(HashSet::new()),
            total_vacuums: AtomicU64::new(0),
            total_analyzes: AtomicU64::new(0),
        }
    }

    /// Start the daemon
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the daemon
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Add a vacuum candidate
    pub fn add_vacuum_candidate(&self, table: &str) {
        let mut candidates = self.vacuum_candidates.write().unwrap();
        candidates.insert(table.to_string());
    }

    /// Add an analyze candidate
    pub fn add_analyze_candidate(&self, table: &str) {
        let mut candidates = self.analyze_candidates.write().unwrap();
        candidates.insert(table.to_string());
    }

    /// Check tables and add candidates
    pub fn check_tables(&self, stats: &HashMap<String, TableStats>) {
        for (table_name, table_stats) in stats {
            if table_stats.needs_vacuum(
                self.config.vacuum_threshold as f64,
                self.config.vacuum_scale_factor,
                self.config.vacuum_threshold,
            ) {
                self.add_vacuum_candidate(table_name);
            }

            if table_stats.needs_analyze(
                self.config.analyze_threshold as f64,
                self.config.analyze_scale_factor,
                self.config.analyze_threshold,
            ) {
                self.add_analyze_candidate(table_name);
            }
        }
    }

    /// Get next table to process
    pub fn get_next_table(&self) -> Option<(String, AutovacuumOp)> {
        // Check vacuum candidates first
        {
            let mut candidates = self.vacuum_candidates.write().unwrap();
            if let Some(table) = candidates.iter().next().cloned() {
                candidates.remove(&table);

                // Check if also needs analyze
                let analyze = self.analyze_candidates.read().unwrap();
                if analyze.contains(&table) {
                    drop(analyze);
                    let mut analyze = self.analyze_candidates.write().unwrap();
                    analyze.remove(&table);
                    return Some((table, AutovacuumOp::VacuumAnalyze));
                }

                return Some((table, AutovacuumOp::Vacuum));
            }
        }

        // Check analyze candidates
        {
            let mut candidates = self.analyze_candidates.write().unwrap();
            if let Some(table) = candidates.iter().next().cloned() {
                candidates.remove(&table);
                return Some((table, AutovacuumOp::Analyze));
            }
        }

        None
    }

    /// Assign work to a worker
    pub fn assign_work(&self, worker_id: usize, table: &str, op: AutovacuumOp) {
        let mut workers = self.workers.write().unwrap();
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.current_table = Some(table.to_string());
            worker.current_op = Some(op);
            worker.start_time = Some(Instant::now());
        }
    }

    /// Complete work for a worker
    pub fn complete_work(&self, worker_id: usize) {
        let mut workers = self.workers.write().unwrap();
        if let Some(worker) = workers.get_mut(worker_id) {
            if let Some(op) = worker.current_op {
                match op {
                    AutovacuumOp::Vacuum => {
                        self.total_vacuums.fetch_add(1, Ordering::Relaxed);
                    }
                    AutovacuumOp::Analyze => {
                        self.total_analyzes.fetch_add(1, Ordering::Relaxed);
                    }
                    AutovacuumOp::VacuumAnalyze => {
                        self.total_vacuums.fetch_add(1, Ordering::Relaxed);
                        self.total_analyzes.fetch_add(1, Ordering::Relaxed);
                    }
                    AutovacuumOp::Freeze => {
                        self.total_vacuums.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            worker.current_table = None;
            worker.current_op = None;
            worker.start_time = None;
            worker.tables_processed += 1;
        }
    }

    /// Get worker status
    pub fn worker_status(&self) -> Vec<AutovacuumWorker> {
        self.workers.read().unwrap().clone()
    }

    /// Get statistics
    pub fn stats(&self) -> AutovacuumStats {
        AutovacuumStats {
            is_running: self.is_running(),
            total_vacuums: self.total_vacuums.load(Ordering::Relaxed),
            total_analyzes: self.total_analyzes.load(Ordering::Relaxed),
            pending_vacuums: self.vacuum_candidates.read().unwrap().len(),
            pending_analyzes: self.analyze_candidates.read().unwrap().len(),
            active_workers: self
                .workers
                .read()
                .unwrap()
                .iter()
                .filter(|w| w.current_table.is_some())
                .count(),
            max_workers: self.config.max_workers,
        }
    }
}

/// Autovacuum statistics
#[derive(Debug, Clone)]
pub struct AutovacuumStats {
    pub is_running: bool,
    pub total_vacuums: u64,
    pub total_analyzes: u64,
    pub pending_vacuums: usize,
    pub pending_analyzes: usize,
    pub active_workers: usize,
    pub max_workers: usize,
}

// ============================================================================
// ERRORS
// ============================================================================

/// Vacuum/Analyze error
#[derive(Debug, Clone)]
pub enum VacuumError {
    /// Operation cancelled
    Cancelled,
    /// Table not found
    TableNotFound(String),
    /// Table locked
    TableLocked(String),
    /// Insufficient disk space
    InsufficientSpace,
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for VacuumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VacuumError::Cancelled => write!(f, "operation cancelled"),
            VacuumError::TableNotFound(t) => write!(f, "table \"{}\" not found", t),
            VacuumError::TableLocked(t) => write!(f, "table \"{}\" is locked", t),
            VacuumError::InsufficientSpace => write!(f, "insufficient disk space"),
            VacuumError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for VacuumError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vacuum_options() {
        let opts = VacuumOptions::default();
        assert!(!opts.full);
        assert!(!opts.analyze);
        assert!(opts.truncate);

        let opts = VacuumOptions::full();
        assert!(opts.full);

        let opts = VacuumOptions::analyze();
        assert!(opts.analyze);
    }

    #[test]
    fn test_column_stats() {
        let mut stats = ColumnStats::new("id");
        stats.n_distinct = 100.0;
        stats.most_common_vals = vec!["a".to_string(), "b".to_string()];
        stats.most_common_freqs = vec![0.1, 0.05];

        // MCV match
        assert_eq!(stats.estimate_equality_selectivity("a"), 0.1);

        // Non-MCV
        let sel = stats.estimate_equality_selectivity("x");
        assert!(sel > 0.0 && sel < 0.1);
    }

    #[test]
    fn test_table_stats() {
        let mut stats = TableStats::new("public", "users");
        stats.n_live_tup = 1000;
        stats.n_dead_tup = 200;

        assert_eq!(stats.dead_tuple_ratio(), 0.2);
    }

    #[test]
    fn test_table_needs_vacuum() {
        let mut stats = TableStats::new("public", "users");
        stats.n_live_tup = 1000;
        stats.n_dead_tup = 50;

        // threshold = 50 + 0.2 * 1000 = 250
        assert!(!stats.needs_vacuum(50.0, 0.2, 50));

        stats.n_dead_tup = 300;
        assert!(stats.needs_vacuum(50.0, 0.2, 50));
    }

    #[test]
    fn test_vacuum_executor() {
        let executor = VacuumExecutor::new();
        let options = VacuumOptions::default();
        let mut stats = TableStats::new("public", "test");
        stats.rel_pages = 10;
        stats.n_dead_tup = 100;

        let result = executor.vacuum("test", &options, &mut stats).unwrap();
        assert_eq!(result.dead_tuples_removed, 100);
        assert_eq!(stats.n_dead_tup, 0);
        assert!(stats.last_vacuum.is_some());
    }

    #[test]
    fn test_vacuum_cancel() {
        let executor = VacuumExecutor::new();
        executor.cancel();

        let options = VacuumOptions::default();
        let mut stats = TableStats::new("public", "test");

        let result = executor.vacuum("test", &options, &mut stats);
        assert!(matches!(result, Err(VacuumError::Cancelled)));
    }

    #[test]
    fn test_analyze_executor() {
        let executor = AnalyzeExecutor::new();
        let options = AnalyzeOptions::default();
        let mut stats = TableStats::new("public", "test");
        stats.rel_pages = 100;
        stats.n_live_tup = 1000;
        stats.column_stats.insert("id".to_string(), ColumnStats::new("id"));

        let result = executor
            .analyze("test", &[], &options, &mut stats)
            .unwrap();
        assert!(result.rows_sampled > 0);
        assert!(stats.last_analyze.is_some());
    }

    #[test]
    fn test_autovacuum_config() {
        let config = AutovacuumConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_workers, 3);
    }

    #[test]
    fn test_autovacuum_daemon() {
        let config = AutovacuumConfig::default();
        let daemon = AutovacuumDaemon::new(config);

        assert!(!daemon.is_running());
        daemon.start();
        assert!(daemon.is_running());
        daemon.stop();
        assert!(!daemon.is_running());
    }

    #[test]
    fn test_autovacuum_candidates() {
        let config = AutovacuumConfig::default();
        let daemon = AutovacuumDaemon::new(config);

        daemon.add_vacuum_candidate("table1");
        daemon.add_analyze_candidate("table2");

        let (table, op) = daemon.get_next_table().unwrap();
        assert_eq!(table, "table1");
        assert_eq!(op, AutovacuumOp::Vacuum);

        let (table, op) = daemon.get_next_table().unwrap();
        assert_eq!(table, "table2");
        assert_eq!(op, AutovacuumOp::Analyze);
    }

    #[test]
    fn test_autovacuum_vacuum_analyze() {
        let config = AutovacuumConfig::default();
        let daemon = AutovacuumDaemon::new(config);

        daemon.add_vacuum_candidate("table1");
        daemon.add_analyze_candidate("table1");

        let (table, op) = daemon.get_next_table().unwrap();
        assert_eq!(table, "table1");
        assert_eq!(op, AutovacuumOp::VacuumAnalyze);
    }

    #[test]
    fn test_vacuum_progress() {
        let progress = VacuumProgress {
            heap_blks_total: 100,
            heap_blks_vacuumed: 50,
            ..Default::default()
        };

        assert_eq!(progress.percentage(), 50.0);
    }

    #[test]
    fn test_vacuum_phase_display() {
        assert_eq!(VacuumPhase::ScanningHeap.to_string(), "scanning heap");
        assert_eq!(VacuumPhase::VacuumingIndexes.to_string(), "vacuuming indexes");
    }

    #[test]
    fn test_autovacuum_stats() {
        let config = AutovacuumConfig::default();
        let daemon = AutovacuumDaemon::new(config);

        daemon.add_vacuum_candidate("t1");
        daemon.add_vacuum_candidate("t2");
        daemon.add_analyze_candidate("t3");

        let stats = daemon.stats();
        assert_eq!(stats.pending_vacuums, 2);
        assert_eq!(stats.pending_analyzes, 1);
        assert_eq!(stats.max_workers, 3);
    }
}
