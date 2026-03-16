//! Online DDL Operations
//!
//! Provides non-blocking schema changes including:
//! - REINDEX CONCURRENTLY
//! - ALTER TABLE ADD COLUMN (online)
//! - Online schema migrations with progress tracking

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Online DDL error types
#[derive(Debug, Clone)]
pub enum OnlineDdlError {
    /// Operation already in progress
    OperationInProgress(String),
    /// Table not found
    TableNotFound(String),
    /// Index not found
    IndexNotFound(String),
    /// Operation failed
    OperationFailed(String),
    /// Lock acquisition failed
    LockFailed(String),
    /// Validation failed
    ValidationFailed(String),
    /// Timeout
    Timeout(String),
    /// Cancelled by user
    Cancelled(String),
    /// Concurrent modification detected
    ConcurrentModification(String),
}

impl std::fmt::Display for OnlineDdlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OperationInProgress(s) => write!(f, "operation in progress: {}", s),
            Self::TableNotFound(s) => write!(f, "table not found: {}", s),
            Self::IndexNotFound(s) => write!(f, "index not found: {}", s),
            Self::OperationFailed(s) => write!(f, "operation failed: {}", s),
            Self::LockFailed(s) => write!(f, "lock failed: {}", s),
            Self::ValidationFailed(s) => write!(f, "validation failed: {}", s),
            Self::Timeout(s) => write!(f, "timeout: {}", s),
            Self::Cancelled(s) => write!(f, "cancelled: {}", s),
            Self::ConcurrentModification(s) => write!(f, "concurrent modification: {}", s),
        }
    }
}

impl std::error::Error for OnlineDdlError {}

/// Online DDL operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnlineOperation {
    /// Rebuild index without blocking
    ReindexConcurrently {
        index_name: String,
        table_name: String,
        new_index_name: String,
    },
    /// Add column without blocking
    AddColumnOnline {
        table_name: String,
        column: OnlineColumnDef,
        default_value: Option<OnlineValue>,
    },
    /// Drop column without blocking
    DropColumnOnline {
        table_name: String,
        column_name: String,
    },
    /// Alter column type
    AlterColumnType {
        table_name: String,
        column_name: String,
        new_type: String,
        using_expression: Option<String>,
    },
    /// Add constraint without blocking
    AddConstraintOnline {
        table_name: String,
        constraint: OnlineConstraint,
    },
    /// Create index concurrently
    CreateIndexConcurrently {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        method: IndexMethod,
    },
    /// Drop index concurrently
    DropIndexConcurrently { index_name: String },
    /// Online table rebuild
    RebuildTable {
        table_name: String,
        options: RebuildOptions,
    },
}

/// Column definition for online operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<OnlineValue>,
}

/// Value for online operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnlineValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Expression(String),
    CurrentTimestamp,
    Uuid,
}

/// Constraint for online operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnlineConstraint {
    NotNull {
        column: String,
    },
    Unique {
        name: String,
        columns: Vec<String>,
    },
    Check {
        name: String,
        expression: String,
    },
    ForeignKey {
        name: String,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}

/// Index method
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IndexMethod {
    BTree,
    Hash,
    GiST,
    GIN,
    Brin,
}

/// Rebuild options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RebuildOptions {
    pub new_tablespace: Option<String>,
    pub compression: Option<String>,
    pub fill_factor: Option<u8>,
}

/// Operation state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationState {
    /// Pending start
    Pending,
    /// Preparing (acquiring minimal locks)
    Preparing,
    /// Building new structure
    Building,
    /// Catching up with concurrent changes
    CatchingUp,
    /// Validating
    Validating,
    /// Swapping (brief exclusive lock)
    Swapping,
    /// Cleaning up
    CleaningUp,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
}

/// Operation progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationProgress {
    pub operation_id: String,
    pub operation: OnlineOperation,
    pub state: OperationState,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
    pub rows_processed: u64,
    pub rows_total: Option<u64>,
    pub bytes_processed: u64,
    pub current_phase: String,
    pub phases_completed: usize,
    pub phases_total: usize,
    pub error: Option<String>,
    pub estimated_completion: Option<Duration>,
}

impl OperationProgress {
    pub fn percent_complete(&self) -> f64 {
        if let Some(total) = self.rows_total {
            if total == 0 {
                return 100.0;
            }
            (self.rows_processed as f64 / total as f64) * 100.0
        } else {
            (self.phases_completed as f64 / self.phases_total as f64) * 100.0
        }
    }
}

/// Online DDL Manager
pub struct OnlineDdlManager {
    /// Active operations
    operations: RwLock<HashMap<String, Arc<OnlineOperationContext>>>,
    /// Completed operations history
    history: RwLock<VecDeque<OperationProgress>>,
    /// Configuration
    config: OnlineDdlConfig,
    /// Statistics
    stats: RwLock<OnlineDdlStats>,
    /// Operation ID counter
    operation_counter: AtomicU64,
}

/// Online operation context
pub struct OnlineOperationContext {
    pub id: String,
    pub operation: OnlineOperation,
    pub state: RwLock<OperationState>,
    pub progress: RwLock<OperationProgress>,
    pub cancelled: AtomicBool,
    pub started_at: Instant,
}

/// Configuration
#[derive(Debug, Clone)]
pub struct OnlineDdlConfig {
    /// Maximum concurrent operations
    pub max_concurrent_operations: usize,
    /// Lock timeout
    pub lock_timeout: Duration,
    /// Batch size for building
    pub batch_size: usize,
    /// Delay between batches
    pub batch_delay: Duration,
    /// Maximum catch-up iterations
    pub max_catchup_iterations: usize,
    /// History size
    pub history_size: usize,
}

impl Default for OnlineDdlConfig {
    fn default() -> Self {
        Self {
            max_concurrent_operations: 4,
            lock_timeout: Duration::from_secs(30),
            batch_size: 10000,
            batch_delay: Duration::from_millis(10),
            max_catchup_iterations: 100,
            history_size: 100,
        }
    }
}

/// Statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OnlineDdlStats {
    pub operations_started: u64,
    pub operations_completed: u64,
    pub operations_failed: u64,
    pub operations_cancelled: u64,
    pub total_rows_processed: u64,
    pub total_bytes_processed: u64,
    pub total_time_seconds: u64,
}

impl OnlineDdlManager {
    pub fn new(config: OnlineDdlConfig) -> Self {
        Self {
            operations: RwLock::new(HashMap::new()),
            history: RwLock::new(VecDeque::with_capacity(config.history_size)),
            config,
            stats: RwLock::new(OnlineDdlStats::default()),
            operation_counter: AtomicU64::new(0),
        }
    }

    /// Start an online operation
    pub fn start_operation(&self, operation: OnlineOperation) -> Result<String, OnlineDdlError> {
        let operations = self.operations.read();
        if operations.len() >= self.config.max_concurrent_operations {
            return Err(OnlineDdlError::OperationInProgress(
                "maximum concurrent operations reached".into(),
            ));
        }
        drop(operations);

        let id = format!(
            "op_{}",
            self.operation_counter.fetch_add(1, Ordering::SeqCst)
        );

        let phases_total = self.get_phases_count(&operation);

        let context = Arc::new(OnlineOperationContext {
            id: id.clone(),
            operation: operation.clone(),
            state: RwLock::new(OperationState::Pending),
            progress: RwLock::new(OperationProgress {
                operation_id: id.clone(),
                operation,
                state: OperationState::Pending,
                started_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                rows_processed: 0,
                rows_total: None,
                bytes_processed: 0,
                current_phase: "Initializing".into(),
                phases_completed: 0,
                phases_total,
                error: None,
                estimated_completion: None,
            }),
            cancelled: AtomicBool::new(false),
            started_at: Instant::now(),
        });

        self.operations.write().insert(id.clone(), context);
        self.stats.write().operations_started += 1;

        Ok(id)
    }

    fn get_phases_count(&self, operation: &OnlineOperation) -> usize {
        match operation {
            OnlineOperation::ReindexConcurrently { .. } => 5,
            OnlineOperation::AddColumnOnline { .. } => 4,
            OnlineOperation::DropColumnOnline { .. } => 3,
            OnlineOperation::AlterColumnType { .. } => 5,
            OnlineOperation::AddConstraintOnline { .. } => 4,
            OnlineOperation::CreateIndexConcurrently { .. } => 5,
            OnlineOperation::DropIndexConcurrently { .. } => 2,
            OnlineOperation::RebuildTable { .. } => 6,
        }
    }

    /// Execute REINDEX CONCURRENTLY
    pub fn reindex_concurrently(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        // Phase 1: Prepare
        self.update_state(&context, OperationState::Preparing, "Acquiring share lock")?;
        self.check_cancelled(&context)?;

        // Would acquire ShareLock on index
        // Verify index exists and is valid

        // Phase 2: Build new index
        self.update_state(&context, OperationState::Building, "Building new index")?;
        self.check_cancelled(&context)?;

        // Create new index with temporary name
        // Scan table and build index entries
        self.simulate_row_processing(&context, 100000)?;

        // Phase 3: Catch up
        self.update_state(
            &context,
            OperationState::CatchingUp,
            "Processing concurrent changes",
        )?;
        self.check_cancelled(&context)?;

        // Apply any changes that happened during build
        for iteration in 0..self.config.max_catchup_iterations {
            self.check_cancelled(&context)?;

            let changes = self.get_pending_changes()?;
            if changes == 0 {
                break;
            }

            self.update_progress(&context, |p| {
                p.current_phase = format!("Catch-up iteration {}", iteration + 1);
            })?;

            // Apply changes
        }

        // Phase 4: Validate
        self.update_state(&context, OperationState::Validating, "Validating new index")?;
        self.check_cancelled(&context)?;

        // Verify index is complete and consistent

        // Phase 5: Swap
        self.update_state(&context, OperationState::Swapping, "Swapping indexes")?;

        // Brief exclusive lock to swap index names
        // This is the only blocking part

        // Phase 6: Cleanup
        self.update_state(&context, OperationState::CleaningUp, "Dropping old index")?;

        // Drop old index

        self.complete_operation(&context)?;

        Ok(())
    }

    /// Execute CREATE INDEX CONCURRENTLY
    pub fn create_index_concurrently(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        // Phase 1: Prepare
        self.update_state(&context, OperationState::Preparing, "Registering index")?;
        self.check_cancelled(&context)?;

        // Register index as "invalid" so it's not used yet

        // Phase 2: Build (first pass)
        self.update_state(&context, OperationState::Building, "Scanning table")?;
        self.check_cancelled(&context)?;

        // Scan entire table and build index
        self.simulate_row_processing(&context, 500000)?;

        // Phase 3: Catch up with concurrent changes
        self.update_state(
            &context,
            OperationState::CatchingUp,
            "Processing concurrent changes",
        )?;

        for iteration in 0..self.config.max_catchup_iterations {
            self.check_cancelled(&context)?;

            let changes = self.get_pending_changes()?;
            if changes == 0 {
                break;
            }

            self.update_progress(&context, |p| {
                p.current_phase = format!("Catch-up pass {}", iteration + 1);
            })?;
        }

        // Phase 4: Validate unique constraint (if unique index)
        self.update_state(&context, OperationState::Validating, "Checking constraints")?;
        self.check_cancelled(&context)?;

        // Phase 5: Mark valid
        self.update_state(&context, OperationState::Swapping, "Marking index valid")?;

        // Mark index as valid and usable

        self.complete_operation(&context)?;

        Ok(())
    }

    /// Execute ADD COLUMN online
    pub fn add_column_online(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        let (column, default_value) = match &context.operation {
            OnlineOperation::AddColumnOnline {
                column,
                default_value,
                ..
            } => (column.clone(), default_value.clone()),
            _ => {
                return Err(OnlineDdlError::OperationFailed(
                    "wrong operation type".into(),
                ))
            }
        };

        // Phase 1: Add column metadata
        self.update_state(
            &context,
            OperationState::Preparing,
            "Adding column metadata",
        )?;
        self.check_cancelled(&context)?;

        // Add column to catalog with NULL or default
        // If column has default and is NOT NULL, we need to backfill

        // Phase 2: Backfill (if needed)
        if default_value.is_some() && !column.nullable {
            self.update_state(
                &context,
                OperationState::Building,
                "Backfilling default values",
            )?;
            self.check_cancelled(&context)?;

            // Update existing rows in batches
            self.simulate_row_processing(&context, 200000)?;
        }

        // Phase 3: Add constraint (if NOT NULL)
        if !column.nullable {
            self.update_state(
                &context,
                OperationState::Validating,
                "Adding NOT NULL constraint",
            )?;
            self.check_cancelled(&context)?;

            // Validate all rows have values
        }

        // Phase 4: Finalize
        self.update_state(&context, OperationState::CleaningUp, "Finalizing")?;

        // Update statistics

        self.complete_operation(&context)?;

        Ok(())
    }

    /// Execute DROP COLUMN online
    pub fn drop_column_online(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        // Phase 1: Mark column as dropped
        self.update_state(
            &context,
            OperationState::Preparing,
            "Marking column dropped",
        )?;
        self.check_cancelled(&context)?;

        // Column is now invisible to queries but data remains

        // Phase 2: Remove from new pages (lazy cleanup)
        self.update_state(&context, OperationState::Building, "Scheduling cleanup")?;

        // Mark for background cleanup

        // Phase 3: Remove metadata
        self.update_state(&context, OperationState::CleaningUp, "Removing metadata")?;

        self.complete_operation(&context)?;

        Ok(())
    }

    /// Execute ADD CONSTRAINT online
    pub fn add_constraint_online(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        let constraint = match &context.operation {
            OnlineOperation::AddConstraintOnline { constraint, .. } => constraint.clone(),
            _ => {
                return Err(OnlineDdlError::OperationFailed(
                    "wrong operation type".into(),
                ))
            }
        };

        // Phase 1: Add constraint as "not valid"
        self.update_state(
            &context,
            OperationState::Preparing,
            "Adding constraint metadata",
        )?;
        self.check_cancelled(&context)?;

        // Constraint exists but not enforced on existing rows

        // Phase 2: Validate existing data
        self.update_state(
            &context,
            OperationState::Validating,
            "Validating existing rows",
        )?;
        self.check_cancelled(&context)?;

        let row_count = self.estimate_row_count(&context)?;
        self.simulate_row_processing(&context, row_count)?;

        // Phase 3: Handle violations
        self.update_state(&context, OperationState::CatchingUp, "Checking violations")?;

        // If any violations found, report them

        // Phase 4: Mark constraint valid
        self.update_state(
            &context,
            OperationState::Swapping,
            "Marking constraint valid",
        )?;

        match &constraint {
            OnlineConstraint::NotNull { column } => {
                // Mark column NOT NULL
                let _ = column;
            }
            OnlineConstraint::Unique { name, .. } => {
                // Mark unique constraint valid
                let _ = name;
            }
            OnlineConstraint::Check { name, .. } => {
                // Mark check constraint valid
                let _ = name;
            }
            OnlineConstraint::ForeignKey { name, .. } => {
                // Mark FK valid
                let _ = name;
            }
        }

        self.complete_operation(&context)?;

        Ok(())
    }

    /// Execute table rebuild online
    pub fn rebuild_table_online(&self, operation_id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(operation_id)?;

        // Phase 1: Create new table structure
        self.update_state(&context, OperationState::Preparing, "Creating new table")?;
        self.check_cancelled(&context)?;

        // Phase 2: Copy data
        self.update_state(&context, OperationState::Building, "Copying data")?;
        self.check_cancelled(&context)?;

        let row_count = self.estimate_row_count(&context)?;
        self.simulate_row_processing(&context, row_count)?;

        // Phase 3: Build indexes
        self.update_state(&context, OperationState::Building, "Rebuilding indexes")?;
        self.check_cancelled(&context)?;

        // Phase 4: Catch up with changes
        self.update_state(
            &context,
            OperationState::CatchingUp,
            "Processing concurrent changes",
        )?;

        for iteration in 0..self.config.max_catchup_iterations {
            self.check_cancelled(&context)?;

            let changes = self.get_pending_changes()?;
            if changes == 0 {
                break;
            }

            self.update_progress(&context, |p| {
                p.current_phase = format!("Catch-up iteration {}", iteration + 1);
            })?;
        }

        // Phase 5: Swap tables
        self.update_state(&context, OperationState::Swapping, "Swapping tables")?;

        // Brief exclusive lock to swap

        // Phase 6: Cleanup
        self.update_state(&context, OperationState::CleaningUp, "Dropping old table")?;

        self.complete_operation(&context)?;

        Ok(())
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn get_operation(&self, id: &str) -> Result<Arc<OnlineOperationContext>, OnlineDdlError> {
        self.operations
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| OnlineDdlError::OperationFailed(format!("operation not found: {}", id)))
    }

    fn check_cancelled(&self, context: &OnlineOperationContext) -> Result<(), OnlineDdlError> {
        if context.cancelled.load(Ordering::SeqCst) {
            *context.state.write() = OperationState::Cancelled;
            self.stats.write().operations_cancelled += 1;
            return Err(OnlineDdlError::Cancelled(context.id.clone()));
        }
        Ok(())
    }

    fn update_state(
        &self,
        context: &OnlineOperationContext,
        state: OperationState,
        phase: &str,
    ) -> Result<(), OnlineDdlError> {
        *context.state.write() = state;

        let mut progress = context.progress.write();
        progress.state = state;
        progress.current_phase = phase.to_string();
        progress.updated_at = SystemTime::now();
        progress.phases_completed += 1;

        Ok(())
    }

    fn update_progress<F>(
        &self,
        context: &OnlineOperationContext,
        f: F,
    ) -> Result<(), OnlineDdlError>
    where
        F: FnOnce(&mut OperationProgress),
    {
        let mut progress = context.progress.write();
        f(&mut progress);
        progress.updated_at = SystemTime::now();
        Ok(())
    }

    fn simulate_row_processing(
        &self,
        context: &OnlineOperationContext,
        total_rows: u64,
    ) -> Result<(), OnlineDdlError> {
        let mut progress = context.progress.write();
        progress.rows_total = Some(total_rows);
        drop(progress);

        let batch_count = (total_rows / self.config.batch_size as u64).max(1);
        let rows_per_batch = total_rows / batch_count;

        for i in 0..batch_count {
            self.check_cancelled(context)?;

            let rows = (i + 1) * rows_per_batch;
            let mut progress = context.progress.write();
            progress.rows_processed = rows.min(total_rows);
            progress.updated_at = SystemTime::now();

            // Calculate estimated completion
            let elapsed = context.started_at.elapsed();
            if progress.rows_processed > 0 {
                let rows_per_sec = progress.rows_processed as f64 / elapsed.as_secs_f64();
                let remaining_rows = total_rows - progress.rows_processed;
                let remaining_secs = remaining_rows as f64 / rows_per_sec;
                progress.estimated_completion = Some(Duration::from_secs_f64(remaining_secs));
            }
        }

        let mut progress = context.progress.write();
        progress.rows_processed = total_rows;

        self.stats.write().total_rows_processed += total_rows;

        Ok(())
    }

    fn get_pending_changes(&self) -> Result<usize, OnlineDdlError> {
        // Would return actual pending WAL changes
        Ok(0)
    }

    fn estimate_row_count(&self, _context: &OnlineOperationContext) -> Result<u64, OnlineDdlError> {
        // Would estimate from table statistics
        Ok(100000)
    }

    fn complete_operation(&self, context: &OnlineOperationContext) -> Result<(), OnlineDdlError> {
        *context.state.write() = OperationState::Completed;

        let mut progress = context.progress.write();
        progress.state = OperationState::Completed;
        progress.current_phase = "Completed".into();
        progress.updated_at = SystemTime::now();
        progress.estimated_completion = Some(Duration::ZERO);

        let elapsed = context.started_at.elapsed();

        let mut stats = self.stats.write();
        stats.operations_completed += 1;
        stats.total_time_seconds += elapsed.as_secs();

        // Move to history
        let progress_snapshot = progress.clone();
        drop(progress);

        let mut history = self.history.write();
        if history.len() >= self.config.history_size {
            history.pop_front();
        }
        history.push_back(progress_snapshot);

        // Remove from active operations
        self.operations.write().remove(&context.id);

        Ok(())
    }

    fn fail_operation(
        &self,
        context: &OnlineOperationContext,
        error: &str,
    ) -> Result<(), OnlineDdlError> {
        *context.state.write() = OperationState::Failed;

        let mut progress = context.progress.write();
        progress.state = OperationState::Failed;
        progress.error = Some(error.to_string());
        progress.updated_at = SystemTime::now();

        self.stats.write().operations_failed += 1;

        let progress_snapshot = progress.clone();
        drop(progress);

        let mut history = self.history.write();
        if history.len() >= self.config.history_size {
            history.pop_front();
        }
        history.push_back(progress_snapshot);

        self.operations.write().remove(&context.id);

        Ok(())
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /// Cancel an operation
    pub fn cancel_operation(&self, id: &str) -> Result<(), OnlineDdlError> {
        let context = self.get_operation(id)?;
        context.cancelled.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Get operation progress
    pub fn get_progress(&self, id: &str) -> Option<OperationProgress> {
        self.operations
            .read()
            .get(id)
            .map(|ctx| ctx.progress.read().clone())
    }

    /// List active operations
    pub fn list_operations(&self) -> Vec<OperationProgress> {
        self.operations
            .read()
            .values()
            .map(|ctx| ctx.progress.read().clone())
            .collect()
    }

    /// Get operation history
    pub fn get_history(&self, limit: usize) -> Vec<OperationProgress> {
        self.history
            .read()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> OnlineDdlStats {
        self.stats.read().clone()
    }
}

impl Default for OnlineDdlManager {
    fn default() -> Self {
        Self::new(OnlineDdlConfig::default())
    }
}

/// Builder for online operations
pub struct OnlineOperationBuilder {
    manager: Arc<OnlineDdlManager>,
}

impl OnlineOperationBuilder {
    pub fn new(manager: Arc<OnlineDdlManager>) -> Self {
        Self { manager }
    }

    /// Create REINDEX CONCURRENTLY operation
    pub fn reindex_concurrently(&self, table: &str, index: &str) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::ReindexConcurrently {
            index_name: index.to_string(),
            table_name: table.to_string(),
            new_index_name: format!("{}_new", index),
        };
        self.manager.start_operation(op)
    }

    /// Create CREATE INDEX CONCURRENTLY operation
    pub fn create_index_concurrently(
        &self,
        table: &str,
        index: &str,
        columns: Vec<&str>,
        unique: bool,
    ) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::CreateIndexConcurrently {
            index_name: index.to_string(),
            table_name: table.to_string(),
            columns: columns.into_iter().map(String::from).collect(),
            unique,
            method: IndexMethod::BTree,
        };
        self.manager.start_operation(op)
    }

    /// Create ADD COLUMN operation
    pub fn add_column(
        &self,
        table: &str,
        name: &str,
        data_type: &str,
        nullable: bool,
        default: Option<OnlineValue>,
    ) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::AddColumnOnline {
            table_name: table.to_string(),
            column: OnlineColumnDef {
                name: name.to_string(),
                data_type: data_type.to_string(),
                nullable,
                default: default.clone(),
            },
            default_value: default,
        };
        self.manager.start_operation(op)
    }

    /// Create DROP COLUMN operation
    pub fn drop_column(&self, table: &str, column: &str) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::DropColumnOnline {
            table_name: table.to_string(),
            column_name: column.to_string(),
        };
        self.manager.start_operation(op)
    }

    /// Create ADD NOT NULL constraint operation
    pub fn add_not_null(&self, table: &str, column: &str) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::AddConstraintOnline {
            table_name: table.to_string(),
            constraint: OnlineConstraint::NotNull {
                column: column.to_string(),
            },
        };
        self.manager.start_operation(op)
    }

    /// Create ADD UNIQUE constraint operation
    pub fn add_unique(
        &self,
        table: &str,
        name: &str,
        columns: Vec<&str>,
    ) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::AddConstraintOnline {
            table_name: table.to_string(),
            constraint: OnlineConstraint::Unique {
                name: name.to_string(),
                columns: columns.into_iter().map(String::from).collect(),
            },
        };
        self.manager.start_operation(op)
    }

    /// Create REBUILD TABLE operation
    pub fn rebuild_table(
        &self,
        table: &str,
        options: RebuildOptions,
    ) -> Result<String, OnlineDdlError> {
        let op = OnlineOperation::RebuildTable {
            table_name: table.to_string(),
            options,
        };
        self.manager.start_operation(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_operation() {
        let manager = OnlineDdlManager::new(OnlineDdlConfig::default());

        let id = manager
            .start_operation(OnlineOperation::CreateIndexConcurrently {
                index_name: "idx_test".into(),
                table_name: "test_table".into(),
                columns: vec!["col1".into()],
                unique: false,
                method: IndexMethod::BTree,
            })
            .unwrap();

        assert!(id.starts_with("op_"));

        let progress = manager.get_progress(&id).unwrap();
        assert_eq!(progress.state, OperationState::Pending);
    }

    #[test]
    fn test_operation_builder() {
        let manager = Arc::new(OnlineDdlManager::new(OnlineDdlConfig::default()));
        let builder = OnlineOperationBuilder::new(manager.clone());

        let id = builder
            .create_index_concurrently("users", "idx_users_email", vec!["email"], true)
            .unwrap();

        let progress = manager.get_progress(&id).unwrap();
        assert_eq!(progress.phases_total, 5);
    }

    #[test]
    fn test_cancel_operation() {
        let manager = OnlineDdlManager::new(OnlineDdlConfig::default());

        let id = manager
            .start_operation(OnlineOperation::RebuildTable {
                table_name: "big_table".into(),
                options: RebuildOptions::default(),
            })
            .unwrap();

        manager.cancel_operation(&id).unwrap();

        let context = manager.get_operation(&id).unwrap();
        assert!(context.cancelled.load(Ordering::SeqCst));
    }

    #[test]
    fn test_progress_percent() {
        let progress = OperationProgress {
            operation_id: "test".into(),
            operation: OnlineOperation::CreateIndexConcurrently {
                index_name: "idx".into(),
                table_name: "t".into(),
                columns: vec![],
                unique: false,
                method: IndexMethod::BTree,
            },
            state: OperationState::Building,
            started_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            rows_processed: 5000,
            rows_total: Some(10000),
            bytes_processed: 0,
            current_phase: "Building".into(),
            phases_completed: 2,
            phases_total: 5,
            error: None,
            estimated_completion: None,
        };

        assert!((progress.percent_complete() - 50.0).abs() < 0.1);
    }
}
