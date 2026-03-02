// Phase 9: Mutations & Specialized MergeTree Engines
//
// Provides UPDATE/DELETE mutations, specialized merge engines, and lightweight deletes
// for analytical workloads.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Mutations (UPDATE/DELETE)
// ============================================================================

/// Mutation operation type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationType {
    Update,
    Delete,
}

/// Mutation state in the pipeline
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationState {
    Pending,
    Executing,
    Completed,
    Failed(String),
    Cancelled,
}

/// Column update specification
#[derive(Debug, Clone)]
pub struct ColumnUpdate {
    pub column: String,
    pub expression: MutationExpr,
}

/// Expression for mutation updates
#[derive(Debug, Clone)]
pub enum MutationExpr {
    Literal(MutationValue),
    Column(String),
    Add(Box<MutationExpr>, Box<MutationExpr>),
    Sub(Box<MutationExpr>, Box<MutationExpr>),
    Mul(Box<MutationExpr>, Box<MutationExpr>),
    Div(Box<MutationExpr>, Box<MutationExpr>),
    Concat(Box<MutationExpr>, Box<MutationExpr>),
    If(Box<MutationPredicate>, Box<MutationExpr>, Box<MutationExpr>),
    Coalesce(Vec<MutationExpr>),
}

/// Values in mutations
#[derive(Debug, Clone, PartialEq)]
pub enum MutationValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
}

/// Predicate for WHERE clause in mutations
#[derive(Debug, Clone)]
pub enum MutationPredicate {
    Eq(String, MutationValue),
    Ne(String, MutationValue),
    Lt(String, MutationValue),
    Le(String, MutationValue),
    Gt(String, MutationValue),
    Ge(String, MutationValue),
    In(String, Vec<MutationValue>),
    NotIn(String, Vec<MutationValue>),
    IsNull(String),
    IsNotNull(String),
    Like(String, String),
    And(Box<MutationPredicate>, Box<MutationPredicate>),
    Or(Box<MutationPredicate>, Box<MutationPredicate>),
    Not(Box<MutationPredicate>),
    True,
}

/// A mutation command
#[derive(Debug, Clone)]
pub struct Mutation {
    pub id: u64,
    pub mutation_type: MutationType,
    pub table: String,
    pub database: String,
    pub predicate: MutationPredicate,
    pub updates: Vec<ColumnUpdate>, // Only for UPDATE
    pub state: MutationState,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub rows_affected: u64,
    pub parts_affected: u64,
    pub parts_completed: u64,
}

/// Progress of a mutation
#[derive(Debug, Clone)]
pub struct MutationProgress {
    pub mutation_id: u64,
    pub state: MutationState,
    pub rows_processed: u64,
    pub rows_affected: u64,
    pub parts_total: u64,
    pub parts_done: u64,
    pub elapsed_ms: u64,
}

/// Part (segment) to be mutated
#[derive(Debug, Clone)]
pub struct MutationPart {
    pub part_name: String,
    pub rows_count: u64,
    pub is_mutated: bool,
    pub new_part_name: Option<String>,
}

/// Mutation error types
#[derive(Debug, Clone)]
pub enum MutationError {
    TableNotFound(String),
    ColumnNotFound(String),
    InvalidExpression(String),
    InvalidPredicate(String),
    MutationFailed(String),
    MutationCancelled,
    ConcurrentMutation,
    ReadOnlyMode,
}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "Table not found: {}", t),
            Self::ColumnNotFound(c) => write!(f, "Column not found: {}", c),
            Self::InvalidExpression(e) => write!(f, "Invalid expression: {}", e),
            Self::InvalidPredicate(p) => write!(f, "Invalid predicate: {}", p),
            Self::MutationFailed(m) => write!(f, "Mutation failed: {}", m),
            Self::MutationCancelled => write!(f, "Mutation cancelled"),
            Self::ConcurrentMutation => write!(f, "Concurrent mutation in progress"),
            Self::ReadOnlyMode => write!(f, "Database is in read-only mode"),
        }
    }
}

impl std::error::Error for MutationError {}

/// Manages mutations for a database
pub struct MutationManager {
    mutations: RwLock<BTreeMap<u64, Mutation>>,
    next_id: AtomicU64,
    active_mutations: RwLock<HashMap<String, u64>>, // table -> mutation_id
    read_only: RwLock<bool>,
}

impl MutationManager {
    pub fn new() -> Self {
        Self {
            mutations: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            active_mutations: RwLock::new(HashMap::new()),
            read_only: RwLock::new(false),
        }
    }

    /// Submit an UPDATE mutation
    pub fn submit_update(
        &self,
        database: &str,
        table: &str,
        updates: Vec<ColumnUpdate>,
        predicate: MutationPredicate,
    ) -> Result<u64, MutationError> {
        self.submit_mutation(database, table, MutationType::Update, updates, predicate)
    }

    /// Submit a DELETE mutation
    pub fn submit_delete(
        &self,
        database: &str,
        table: &str,
        predicate: MutationPredicate,
    ) -> Result<u64, MutationError> {
        self.submit_mutation(database, table, MutationType::Delete, vec![], predicate)
    }

    fn submit_mutation(
        &self,
        database: &str,
        table: &str,
        mutation_type: MutationType,
        updates: Vec<ColumnUpdate>,
        predicate: MutationPredicate,
    ) -> Result<u64, MutationError> {
        if *self.read_only.read() {
            return Err(MutationError::ReadOnlyMode);
        }

        let table_key = format!("{}.{}", database, table);

        // Check for concurrent mutations on the same table
        {
            let active = self.active_mutations.read();
            if active.contains_key(&table_key) {
                return Err(MutationError::ConcurrentMutation);
            }
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mutation = Mutation {
            id,
            mutation_type,
            table: table.to_string(),
            database: database.to_string(),
            predicate,
            updates,
            state: MutationState::Pending,
            created_at: now,
            started_at: None,
            completed_at: None,
            rows_affected: 0,
            parts_affected: 0,
            parts_completed: 0,
        };

        self.mutations.write().insert(id, mutation);
        self.active_mutations.write().insert(table_key, id);

        Ok(id)
    }

    /// Start executing a mutation
    pub fn start_mutation(&self, mutation_id: u64) -> Result<(), MutationError> {
        let mut mutations = self.mutations.write();
        let mutation = mutations.get_mut(&mutation_id)
            .ok_or_else(|| MutationError::MutationFailed("Mutation not found".to_string()))?;

        if mutation.state != MutationState::Pending {
            return Err(MutationError::MutationFailed("Mutation not in pending state".to_string()));
        }

        mutation.state = MutationState::Executing;
        mutation.started_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        );

        Ok(())
    }

    /// Update mutation progress
    pub fn update_progress(
        &self,
        mutation_id: u64,
        rows_affected: u64,
        parts_completed: u64,
    ) -> Result<(), MutationError> {
        let mut mutations = self.mutations.write();
        let mutation = mutations.get_mut(&mutation_id)
            .ok_or_else(|| MutationError::MutationFailed("Mutation not found".to_string()))?;

        mutation.rows_affected = rows_affected;
        mutation.parts_completed = parts_completed;

        Ok(())
    }

    /// Complete a mutation
    pub fn complete_mutation(&self, mutation_id: u64, rows_affected: u64) -> Result<(), MutationError> {
        let table_key;
        {
            let mut mutations = self.mutations.write();
            let mutation = mutations.get_mut(&mutation_id)
                .ok_or_else(|| MutationError::MutationFailed("Mutation not found".to_string()))?;

            mutation.state = MutationState::Completed;
            mutation.rows_affected = rows_affected;
            mutation.completed_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            );

            table_key = format!("{}.{}", mutation.database, mutation.table);
        }

        self.active_mutations.write().remove(&table_key);
        Ok(())
    }

    /// Fail a mutation
    pub fn fail_mutation(&self, mutation_id: u64, error: &str) -> Result<(), MutationError> {
        let table_key;
        {
            let mut mutations = self.mutations.write();
            let mutation = mutations.get_mut(&mutation_id)
                .ok_or_else(|| MutationError::MutationFailed("Mutation not found".to_string()))?;

            mutation.state = MutationState::Failed(error.to_string());
            mutation.completed_at = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
            );

            table_key = format!("{}.{}", mutation.database, mutation.table);
        }

        self.active_mutations.write().remove(&table_key);
        Ok(())
    }

    /// Cancel a mutation
    pub fn cancel_mutation(&self, mutation_id: u64) -> Result<(), MutationError> {
        let table_key;
        {
            let mut mutations = self.mutations.write();
            let mutation = mutations.get_mut(&mutation_id)
                .ok_or_else(|| MutationError::MutationFailed("Mutation not found".to_string()))?;

            if mutation.state == MutationState::Completed {
                return Err(MutationError::MutationFailed("Cannot cancel completed mutation".to_string()));
            }

            mutation.state = MutationState::Cancelled;
            table_key = format!("{}.{}", mutation.database, mutation.table);
        }

        self.active_mutations.write().remove(&table_key);
        Ok(())
    }

    /// Get mutation status
    pub fn get_mutation(&self, mutation_id: u64) -> Option<Mutation> {
        self.mutations.read().get(&mutation_id).cloned()
    }

    /// Get mutation progress
    pub fn get_progress(&self, mutation_id: u64) -> Option<MutationProgress> {
        let mutations = self.mutations.read();
        let mutation = mutations.get(&mutation_id)?;

        let elapsed_ms = mutation.started_at.map(|start| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            (now - start) as u64
        }).unwrap_or(0);

        Some(MutationProgress {
            mutation_id,
            state: mutation.state.clone(),
            rows_processed: mutation.rows_affected,
            rows_affected: mutation.rows_affected,
            parts_total: mutation.parts_affected,
            parts_done: mutation.parts_completed,
            elapsed_ms,
        })
    }

    /// List all mutations for a table
    pub fn list_mutations(&self, database: &str, table: &str) -> Vec<Mutation> {
        self.mutations.read()
            .values()
            .filter(|m| m.database == database && m.table == table)
            .cloned()
            .collect()
    }

    /// List pending mutations
    pub fn list_pending(&self) -> Vec<Mutation> {
        self.mutations.read()
            .values()
            .filter(|m| m.state == MutationState::Pending)
            .cloned()
            .collect()
    }

    /// Set read-only mode
    pub fn set_read_only(&self, read_only: bool) {
        *self.read_only.write() = read_only;
    }

    /// Evaluate a predicate against a row
    pub fn evaluate_predicate(
        predicate: &MutationPredicate,
        row: &HashMap<String, MutationValue>,
    ) -> bool {
        match predicate {
            MutationPredicate::True => true,
            MutationPredicate::Eq(col, val) => row.get(col).map(|v| v == val).unwrap_or(false),
            MutationPredicate::Ne(col, val) => row.get(col).map(|v| v != val).unwrap_or(true),
            MutationPredicate::Lt(col, val) => {
                row.get(col).map(|v| compare_values(v, val) == std::cmp::Ordering::Less).unwrap_or(false)
            }
            MutationPredicate::Le(col, val) => {
                row.get(col).map(|v| compare_values(v, val) != std::cmp::Ordering::Greater).unwrap_or(false)
            }
            MutationPredicate::Gt(col, val) => {
                row.get(col).map(|v| compare_values(v, val) == std::cmp::Ordering::Greater).unwrap_or(false)
            }
            MutationPredicate::Ge(col, val) => {
                row.get(col).map(|v| compare_values(v, val) != std::cmp::Ordering::Less).unwrap_or(false)
            }
            MutationPredicate::In(col, vals) => {
                row.get(col).map(|v| vals.contains(v)).unwrap_or(false)
            }
            MutationPredicate::NotIn(col, vals) => {
                row.get(col).map(|v| !vals.contains(v)).unwrap_or(true)
            }
            MutationPredicate::IsNull(col) => {
                row.get(col).map(|v| *v == MutationValue::Null).unwrap_or(true)
            }
            MutationPredicate::IsNotNull(col) => {
                row.get(col).map(|v| *v != MutationValue::Null).unwrap_or(false)
            }
            MutationPredicate::Like(col, pattern) => {
                row.get(col).map(|v| {
                    if let MutationValue::String(s) = v {
                        like_match(s, pattern)
                    } else {
                        false
                    }
                }).unwrap_or(false)
            }
            MutationPredicate::And(a, b) => {
                Self::evaluate_predicate(a, row) && Self::evaluate_predicate(b, row)
            }
            MutationPredicate::Or(a, b) => {
                Self::evaluate_predicate(a, row) || Self::evaluate_predicate(b, row)
            }
            MutationPredicate::Not(p) => !Self::evaluate_predicate(p, row),
        }
    }

    /// Evaluate an expression to get a value
    pub fn evaluate_expr(
        expr: &MutationExpr,
        row: &HashMap<String, MutationValue>,
    ) -> MutationValue {
        match expr {
            MutationExpr::Literal(v) => v.clone(),
            MutationExpr::Column(col) => row.get(col).cloned().unwrap_or(MutationValue::Null),
            MutationExpr::Add(a, b) => {
                let va = Self::evaluate_expr(a, row);
                let vb = Self::evaluate_expr(b, row);
                match (va, vb) {
                    (MutationValue::Int(x), MutationValue::Int(y)) => MutationValue::Int(x + y),
                    (MutationValue::Float(x), MutationValue::Float(y)) => MutationValue::Float(x + y),
                    (MutationValue::Int(x), MutationValue::Float(y)) => MutationValue::Float(x as f64 + y),
                    (MutationValue::Float(x), MutationValue::Int(y)) => MutationValue::Float(x + y as f64),
                    _ => MutationValue::Null,
                }
            }
            MutationExpr::Sub(a, b) => {
                let va = Self::evaluate_expr(a, row);
                let vb = Self::evaluate_expr(b, row);
                match (va, vb) {
                    (MutationValue::Int(x), MutationValue::Int(y)) => MutationValue::Int(x - y),
                    (MutationValue::Float(x), MutationValue::Float(y)) => MutationValue::Float(x - y),
                    (MutationValue::Int(x), MutationValue::Float(y)) => MutationValue::Float(x as f64 - y),
                    (MutationValue::Float(x), MutationValue::Int(y)) => MutationValue::Float(x - y as f64),
                    _ => MutationValue::Null,
                }
            }
            MutationExpr::Mul(a, b) => {
                let va = Self::evaluate_expr(a, row);
                let vb = Self::evaluate_expr(b, row);
                match (va, vb) {
                    (MutationValue::Int(x), MutationValue::Int(y)) => MutationValue::Int(x * y),
                    (MutationValue::Float(x), MutationValue::Float(y)) => MutationValue::Float(x * y),
                    (MutationValue::Int(x), MutationValue::Float(y)) => MutationValue::Float(x as f64 * y),
                    (MutationValue::Float(x), MutationValue::Int(y)) => MutationValue::Float(x * y as f64),
                    _ => MutationValue::Null,
                }
            }
            MutationExpr::Div(a, b) => {
                let va = Self::evaluate_expr(a, row);
                let vb = Self::evaluate_expr(b, row);
                match (va, vb) {
                    (MutationValue::Int(x), MutationValue::Int(y)) if y != 0 => MutationValue::Int(x / y),
                    (MutationValue::Float(x), MutationValue::Float(y)) if y != 0.0 => MutationValue::Float(x / y),
                    (MutationValue::Int(x), MutationValue::Float(y)) if y != 0.0 => MutationValue::Float(x as f64 / y),
                    (MutationValue::Float(x), MutationValue::Int(y)) if y != 0 => MutationValue::Float(x / y as f64),
                    _ => MutationValue::Null,
                }
            }
            MutationExpr::Concat(a, b) => {
                let va = Self::evaluate_expr(a, row);
                let vb = Self::evaluate_expr(b, row);
                match (va, vb) {
                    (MutationValue::String(x), MutationValue::String(y)) => {
                        MutationValue::String(format!("{}{}", x, y))
                    }
                    _ => MutationValue::Null,
                }
            }
            MutationExpr::If(pred, then_expr, else_expr) => {
                if Self::evaluate_predicate(pred, row) {
                    Self::evaluate_expr(then_expr, row)
                } else {
                    Self::evaluate_expr(else_expr, row)
                }
            }
            MutationExpr::Coalesce(exprs) => {
                for e in exprs {
                    let v = Self::evaluate_expr(e, row);
                    if v != MutationValue::Null {
                        return v;
                    }
                }
                MutationValue::Null
            }
        }
    }
}

impl Default for MutationManager {
    fn default() -> Self {
        Self::new()
    }
}

fn compare_values(a: &MutationValue, b: &MutationValue) -> std::cmp::Ordering {
    match (a, b) {
        (MutationValue::Int(x), MutationValue::Int(y)) => x.cmp(y),
        (MutationValue::Float(x), MutationValue::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (MutationValue::String(x), MutationValue::String(y)) => x.cmp(y),
        (MutationValue::Timestamp(x), MutationValue::Timestamp(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let regex_pattern = pattern
        .replace('%', ".*")
        .replace('_', ".");
    regex::Regex::new(&format!("^{}$", regex_pattern))
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

// ============================================================================
// Specialized MergeTree Engines
// ============================================================================

/// MergeTree engine type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTreeEngine {
    /// Standard MergeTree
    MergeTree,
    /// Deduplication by sorting key, keeps latest version
    ReplacingMergeTree { version_column: Option<String> },
    /// Automatically sums numeric columns during merge
    SummingMergeTree { columns: Vec<String> },
    /// Stores intermediate aggregate states
    AggregatingMergeTree,
    /// Collapses rows with Sign column (+1/-1)
    CollapsingMergeTree { sign_column: String },
    /// Version-aware collapsing
    VersionedCollapsingMergeTree { sign_column: String, version_column: String },
}

/// Row with metadata for specialized engines
#[derive(Debug, Clone)]
pub struct MergeRow {
    pub key: Vec<MutationValue>,
    pub values: HashMap<String, MutationValue>,
    pub version: Option<i64>,
    pub sign: Option<i8>,
}

/// Aggregate state for AggregatingMergeTree
#[derive(Debug, Clone)]
pub enum AggState {
    Count(u64),
    Sum(f64),
    Min(MutationValue),
    Max(MutationValue),
    Avg { sum: f64, count: u64 },
    Uniq(HashSet<String>),
    GroupBitmap(Vec<u64>),
}

impl AggState {
    pub fn merge(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::Sum(a), AggState::Sum(b)) => *a += b,
            (AggState::Min(a), AggState::Min(b)) => {
                if compare_values(b, a) == std::cmp::Ordering::Less {
                    *a = b.clone();
                }
            }
            (AggState::Max(a), AggState::Max(b)) => {
                if compare_values(b, a) == std::cmp::Ordering::Greater {
                    *a = b.clone();
                }
            }
            (AggState::Avg { sum: s1, count: c1 }, AggState::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            (AggState::Uniq(a), AggState::Uniq(b)) => {
                a.extend(b.iter().cloned());
            }
            (AggState::GroupBitmap(a), AggState::GroupBitmap(b)) => {
                // Union of bitmaps
                for &v in b {
                    if !a.contains(&v) {
                        a.push(v);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn finalize(&self) -> MutationValue {
        match self {
            AggState::Count(c) => MutationValue::Int(*c as i64),
            AggState::Sum(s) => MutationValue::Float(*s),
            AggState::Min(v) | AggState::Max(v) => v.clone(),
            AggState::Avg { sum, count } if *count > 0 => MutationValue::Float(sum / *count as f64),
            AggState::Avg { .. } => MutationValue::Null,
            AggState::Uniq(set) => MutationValue::Int(set.len() as i64),
            AggState::GroupBitmap(bm) => MutationValue::Int(bm.len() as i64),
        }
    }
}

/// Configuration for a MergeTree table
#[derive(Debug, Clone)]
pub struct MergeTreeConfig {
    pub engine: MergeTreeEngine,
    pub order_by: Vec<String>,
    pub partition_by: Option<String>,
    pub primary_key: Option<Vec<String>>,
    pub sample_by: Option<String>,
    pub ttl: Option<String>,
    pub settings: MergeTreeSettings,
}

/// Settings for MergeTree behavior
#[derive(Debug, Clone)]
pub struct MergeTreeSettings {
    pub index_granularity: u64,
    pub index_granularity_bytes: u64,
    pub min_bytes_for_wide_part: u64,
    pub min_rows_for_wide_part: u64,
    pub enable_mixed_granularity_parts: bool,
    pub merge_max_block_size: u64,
    pub max_parts_in_total: u64,
    pub max_partitions_to_read: Option<u64>,
}

impl Default for MergeTreeSettings {
    fn default() -> Self {
        Self {
            index_granularity: 8192,
            index_granularity_bytes: 10 * 1024 * 1024, // 10MB
            min_bytes_for_wide_part: 10 * 1024 * 1024, // 10MB
            min_rows_for_wide_part: 0,
            enable_mixed_granularity_parts: true,
            merge_max_block_size: 8192,
            max_parts_in_total: 100000,
            max_partitions_to_read: None,
        }
    }
}

/// Handles merging for specialized engines
pub struct MergeTreeMerger {
    config: MergeTreeConfig,
}

impl MergeTreeMerger {
    pub fn new(config: MergeTreeConfig) -> Self {
        Self { config }
    }

    /// Merge rows according to engine type
    pub fn merge(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        match &self.config.engine {
            MergeTreeEngine::MergeTree => rows, // No special merging
            MergeTreeEngine::ReplacingMergeTree { version_column } => {
                self.merge_replacing(rows, version_column.as_deref())
            }
            MergeTreeEngine::SummingMergeTree { columns } => {
                self.merge_summing(rows, columns)
            }
            MergeTreeEngine::AggregatingMergeTree => {
                self.merge_aggregating(rows)
            }
            MergeTreeEngine::CollapsingMergeTree { sign_column } => {
                self.merge_collapsing(rows, sign_column)
            }
            MergeTreeEngine::VersionedCollapsingMergeTree { sign_column, version_column } => {
                self.merge_versioned_collapsing(rows, sign_column, version_column)
            }
        }
    }

    /// ReplacingMergeTree: Keep only the latest version for each key
    fn merge_replacing(&self, rows: Vec<MergeRow>, version_column: Option<&str>) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<u8>, MergeRow> = HashMap::new();

        for row in rows {
            let key_bytes = serialize_key(&row.key);

            let should_replace = match by_key.get(&key_bytes) {
                None => true,
                Some(existing) => {
                    if let Some(vc) = version_column {
                        let new_ver = row.values.get(vc)
                            .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                            .unwrap_or(0);
                        let old_ver = existing.values.get(vc)
                            .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                            .unwrap_or(0);
                        new_ver > old_ver
                    } else {
                        // Without version column, last one wins
                        true
                    }
                }
            };

            if should_replace {
                by_key.insert(key_bytes, row);
            }
        }

        by_key.into_values().collect()
    }

    /// SummingMergeTree: Sum numeric columns for same key
    fn merge_summing(&self, rows: Vec<MergeRow>, sum_columns: &[String]) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<u8>, MergeRow> = HashMap::new();

        for row in rows {
            let key_bytes = serialize_key(&row.key);

            match by_key.get_mut(&key_bytes) {
                None => {
                    by_key.insert(key_bytes, row);
                }
                Some(existing) => {
                    // Sum the specified columns
                    for col in sum_columns {
                        if let (Some(MutationValue::Int(a)), Some(MutationValue::Int(b))) =
                            (existing.values.get_mut(col), row.values.get(col)) {
                            *a += b;
                        } else if let (Some(MutationValue::Float(a)), Some(MutationValue::Float(b))) =
                            (existing.values.get_mut(col), row.values.get(col)) {
                            *a += b;
                        }
                    }
                }
            }
        }

        by_key.into_values().collect()
    }

    /// AggregatingMergeTree: Merge aggregate states
    fn merge_aggregating(&self, rows: Vec<MergeRow>) -> Vec<MergeRow> {
        // For simplicity, treat as replacing (real impl would merge AggState)
        self.merge_replacing(rows, None)
    }

    /// CollapsingMergeTree: Collapse rows with opposite signs
    fn merge_collapsing(&self, rows: Vec<MergeRow>, sign_column: &str) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<u8>, Vec<MergeRow>> = HashMap::new();

        for row in rows {
            let key_bytes = serialize_key(&row.key);
            by_key.entry(key_bytes).or_default().push(row);
        }

        let mut result = Vec::new();

        for (_, mut group) in by_key {
            // Sort by sign to process consistently
            group.sort_by(|a, b| {
                let sa = a.values.get(sign_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(0);
                let sb = b.values.get(sign_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(0);
                sa.cmp(&sb)
            });

            let mut balance: i64 = 0;
            let mut last_positive: Option<MergeRow> = None;
            let mut last_negative: Option<MergeRow> = None;

            for row in group {
                let sign = row.values.get(sign_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(1);

                balance += sign;
                if sign > 0 {
                    last_positive = Some(row);
                } else {
                    last_negative = Some(row);
                }
            }

            // Keep unmatched rows
            if balance > 0 {
                if let Some(row) = last_positive {
                    result.push(row);
                }
            } else if balance < 0 {
                if let Some(row) = last_negative {
                    result.push(row);
                }
            }
            // balance == 0 means fully collapsed
        }

        result
    }

    /// VersionedCollapsingMergeTree: Version-aware collapsing
    fn merge_versioned_collapsing(
        &self,
        rows: Vec<MergeRow>,
        sign_column: &str,
        version_column: &str,
    ) -> Vec<MergeRow> {
        let mut by_key: HashMap<Vec<u8>, Vec<MergeRow>> = HashMap::new();

        for row in rows {
            let key_bytes = serialize_key(&row.key);
            by_key.entry(key_bytes).or_default().push(row);
        }

        let mut result = Vec::new();

        for (_, mut group) in by_key {
            // Sort by version then sign
            group.sort_by(|a, b| {
                let va = a.values.get(version_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(0);
                let vb = b.values.get(version_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(0);

                match va.cmp(&vb) {
                    std::cmp::Ordering::Equal => {
                        let sa = a.values.get(sign_column)
                            .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                            .unwrap_or(0);
                        let sb = b.values.get(sign_column)
                            .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                            .unwrap_or(0);
                        sa.cmp(&sb)
                    }
                    other => other,
                }
            });

            // Group by version and collapse within each version
            let mut by_version: BTreeMap<i64, Vec<MergeRow>> = BTreeMap::new();
            for row in group {
                let ver = row.values.get(version_column)
                    .and_then(|v| if let MutationValue::Int(i) = v { Some(*i) } else { None })
                    .unwrap_or(0);
                by_version.entry(ver).or_default().push(row);
            }

            // Keep only latest version's unmatched rows
            if let Some((_, version_rows)) = by_version.into_iter().last() {
                let collapsed = self.merge_collapsing(version_rows, sign_column);
                result.extend(collapsed);
            }
        }

        result
    }
}

fn serialize_key(key: &[MutationValue]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for v in key {
        match v {
            MutationValue::Null => bytes.push(0),
            MutationValue::Bool(b) => {
                bytes.push(1);
                bytes.push(if *b { 1 } else { 0 });
            }
            MutationValue::Int(i) => {
                bytes.push(2);
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            MutationValue::Float(f) => {
                bytes.push(3);
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            MutationValue::String(s) => {
                bytes.push(4);
                bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
            }
            MutationValue::Bytes(b) => {
                bytes.push(5);
                bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
                bytes.extend_from_slice(b);
            }
            MutationValue::Timestamp(t) => {
                bytes.push(6);
                bytes.extend_from_slice(&t.to_le_bytes());
            }
        }
    }
    bytes
}

// ============================================================================
// Lightweight Deletes
// ============================================================================

/// Delete mask for lightweight deletes
#[derive(Debug, Clone)]
pub struct DeleteMask {
    /// Bitmap where 1 = deleted
    mask: Vec<u64>,
    /// Number of rows covered
    row_count: u64,
    /// Number of deleted rows
    deleted_count: u64,
}

impl DeleteMask {
    pub fn new(row_count: u64) -> Self {
        let words = ((row_count + 63) / 64) as usize;
        Self {
            mask: vec![0; words],
            row_count,
            deleted_count: 0,
        }
    }

    /// Mark a row as deleted
    pub fn delete(&mut self, row_index: u64) {
        if row_index >= self.row_count {
            return;
        }
        let word = (row_index / 64) as usize;
        let bit = row_index % 64;
        if self.mask[word] & (1 << bit) == 0 {
            self.mask[word] |= 1 << bit;
            self.deleted_count += 1;
        }
    }

    /// Mark multiple rows as deleted
    pub fn delete_range(&mut self, start: u64, end: u64) {
        for i in start..end.min(self.row_count) {
            self.delete(i);
        }
    }

    /// Check if a row is deleted
    pub fn is_deleted(&self, row_index: u64) -> bool {
        if row_index >= self.row_count {
            return false;
        }
        let word = (row_index / 64) as usize;
        let bit = row_index % 64;
        self.mask[word] & (1 << bit) != 0
    }

    /// Get count of deleted rows
    pub fn deleted_count(&self) -> u64 {
        self.deleted_count
    }

    /// Get count of live (non-deleted) rows
    pub fn live_count(&self) -> u64 {
        self.row_count - self.deleted_count
    }

    /// Get total row count
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Get deletion ratio
    pub fn deletion_ratio(&self) -> f64 {
        if self.row_count == 0 {
            0.0
        } else {
            self.deleted_count as f64 / self.row_count as f64
        }
    }

    /// Merge with another delete mask
    pub fn merge(&mut self, other: &DeleteMask) {
        assert_eq!(self.row_count, other.row_count);
        for (i, &word) in other.mask.iter().enumerate() {
            let new_deletes = word & !self.mask[i];
            self.deleted_count += new_deletes.count_ones() as u64;
            self.mask[i] |= word;
        }
    }

    /// Serialize the mask
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.row_count.to_le_bytes());
        bytes.extend_from_slice(&self.deleted_count.to_le_bytes());
        bytes.extend_from_slice(&(self.mask.len() as u64).to_le_bytes());
        for word in &self.mask {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes
    }

    /// Deserialize a mask
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }
        let row_count = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let deleted_count = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let mask_len = u64::from_le_bytes(data[16..24].try_into().ok()?) as usize;

        if data.len() < 24 + mask_len * 8 {
            return None;
        }

        let mut mask = Vec::with_capacity(mask_len);
        for i in 0..mask_len {
            let offset = 24 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            mask.push(word);
        }

        Some(Self {
            mask,
            row_count,
            deleted_count,
        })
    }

    /// Iterate over live row indices
    pub fn live_rows(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.row_count).filter(move |&i| !self.is_deleted(i))
    }

    /// Iterate over deleted row indices
    pub fn deleted_rows(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.row_count).filter(move |&i| self.is_deleted(i))
    }
}

/// Manages delete masks for parts
pub struct LightweightDeleteManager {
    masks: RwLock<HashMap<String, DeleteMask>>, // part_name -> mask
    compaction_threshold: f64, // Trigger compaction when deletion ratio exceeds this
}

impl LightweightDeleteManager {
    pub fn new(compaction_threshold: f64) -> Self {
        Self {
            masks: RwLock::new(HashMap::new()),
            compaction_threshold,
        }
    }

    /// Create or get a delete mask for a part
    pub fn get_or_create_mask(&self, part_name: &str, row_count: u64) -> DeleteMask {
        let masks = self.masks.read();
        if let Some(mask) = masks.get(part_name) {
            return mask.clone();
        }
        drop(masks);

        let mask = DeleteMask::new(row_count);
        self.masks.write().insert(part_name.to_string(), mask.clone());
        mask
    }

    /// Apply deletes to a part
    pub fn apply_deletes(&self, part_name: &str, row_indices: &[u64], row_count: u64) -> u64 {
        let mut masks = self.masks.write();
        let mask = masks.entry(part_name.to_string())
            .or_insert_with(|| DeleteMask::new(row_count));

        let before = mask.deleted_count();
        for &idx in row_indices {
            mask.delete(idx);
        }
        mask.deleted_count() - before
    }

    /// Check if a row is deleted
    pub fn is_deleted(&self, part_name: &str, row_index: u64) -> bool {
        self.masks.read()
            .get(part_name)
            .map(|m| m.is_deleted(row_index))
            .unwrap_or(false)
    }

    /// Get the delete mask for a part
    pub fn get_mask(&self, part_name: &str) -> Option<DeleteMask> {
        self.masks.read().get(part_name).cloned()
    }

    /// Check if part needs compaction
    pub fn needs_compaction(&self, part_name: &str) -> bool {
        self.masks.read()
            .get(part_name)
            .map(|m| m.deletion_ratio() > self.compaction_threshold)
            .unwrap_or(false)
    }

    /// List parts that need compaction
    pub fn parts_needing_compaction(&self) -> Vec<String> {
        self.masks.read()
            .iter()
            .filter(|(_, mask)| mask.deletion_ratio() > self.compaction_threshold)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Remove mask after compaction
    pub fn remove_mask(&self, part_name: &str) {
        self.masks.write().remove(part_name);
    }

    /// Get deletion stats
    pub fn stats(&self) -> DeleteStats {
        let masks = self.masks.read();
        let total_rows: u64 = masks.values().map(|m| m.row_count()).sum();
        let deleted_rows: u64 = masks.values().map(|m| m.deleted_count()).sum();
        let parts_with_deletes = masks.values().filter(|m| m.deleted_count() > 0).count();

        DeleteStats {
            total_parts: masks.len(),
            parts_with_deletes,
            total_rows,
            deleted_rows,
            live_rows: total_rows - deleted_rows,
            deletion_ratio: if total_rows > 0 { deleted_rows as f64 / total_rows as f64 } else { 0.0 },
        }
    }
}

impl Default for LightweightDeleteManager {
    fn default() -> Self {
        Self::new(0.5) // 50% deletion threshold for compaction
    }
}

/// Statistics about lightweight deletes
#[derive(Debug, Clone)]
pub struct DeleteStats {
    pub total_parts: usize,
    pub parts_with_deletes: usize,
    pub total_rows: u64,
    pub deleted_rows: u64,
    pub live_rows: u64,
    pub deletion_ratio: f64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Mutation Manager Tests

    #[test]
    fn test_mutation_submit_update() {
        let manager = MutationManager::new();

        let updates = vec![
            ColumnUpdate {
                column: "status".to_string(),
                expression: MutationExpr::Literal(MutationValue::String("archived".to_string())),
            },
        ];

        let id = manager.submit_update(
            "default",
            "events",
            updates,
            MutationPredicate::Lt("timestamp".to_string(), MutationValue::Timestamp(1000)),
        ).unwrap();

        let mutation = manager.get_mutation(id).unwrap();
        assert_eq!(mutation.mutation_type, MutationType::Update);
        assert_eq!(mutation.state, MutationState::Pending);
    }

    #[test]
    fn test_mutation_submit_delete() {
        let manager = MutationManager::new();

        let id = manager.submit_delete(
            "default",
            "events",
            MutationPredicate::Eq("user_id".to_string(), MutationValue::Int(123)),
        ).unwrap();

        let mutation = manager.get_mutation(id).unwrap();
        assert_eq!(mutation.mutation_type, MutationType::Delete);
    }

    #[test]
    fn test_mutation_lifecycle() {
        let manager = MutationManager::new();

        let id = manager.submit_delete(
            "default",
            "events",
            MutationPredicate::True,
        ).unwrap();

        manager.start_mutation(id).unwrap();
        let mutation = manager.get_mutation(id).unwrap();
        assert_eq!(mutation.state, MutationState::Executing);

        manager.update_progress(id, 500, 2).unwrap();

        manager.complete_mutation(id, 1000).unwrap();
        let mutation = manager.get_mutation(id).unwrap();
        assert_eq!(mutation.state, MutationState::Completed);
        assert_eq!(mutation.rows_affected, 1000);
    }

    #[test]
    fn test_mutation_cancel() {
        let manager = MutationManager::new();

        let id = manager.submit_delete("default", "events", MutationPredicate::True).unwrap();
        manager.cancel_mutation(id).unwrap();

        let mutation = manager.get_mutation(id).unwrap();
        assert_eq!(mutation.state, MutationState::Cancelled);
    }

    #[test]
    fn test_mutation_concurrent_blocked() {
        let manager = MutationManager::new();

        let _id1 = manager.submit_delete("default", "events", MutationPredicate::True).unwrap();
        let result = manager.submit_delete("default", "events", MutationPredicate::True);

        assert!(matches!(result, Err(MutationError::ConcurrentMutation)));
    }

    #[test]
    fn test_mutation_read_only() {
        let manager = MutationManager::new();
        manager.set_read_only(true);

        let result = manager.submit_delete("default", "events", MutationPredicate::True);
        assert!(matches!(result, Err(MutationError::ReadOnlyMode)));
    }

    #[test]
    fn test_evaluate_predicate() {
        let mut row = HashMap::new();
        row.insert("age".to_string(), MutationValue::Int(25));
        row.insert("name".to_string(), MutationValue::String("Alice".to_string()));

        assert!(MutationManager::evaluate_predicate(
            &MutationPredicate::Eq("age".to_string(), MutationValue::Int(25)),
            &row
        ));

        assert!(MutationManager::evaluate_predicate(
            &MutationPredicate::Gt("age".to_string(), MutationValue::Int(20)),
            &row
        ));

        assert!(MutationManager::evaluate_predicate(
            &MutationPredicate::Like("name".to_string(), "A%".to_string()),
            &row
        ));

        assert!(MutationManager::evaluate_predicate(
            &MutationPredicate::And(
                Box::new(MutationPredicate::Ge("age".to_string(), MutationValue::Int(18))),
                Box::new(MutationPredicate::Le("age".to_string(), MutationValue::Int(30))),
            ),
            &row
        ));
    }

    #[test]
    fn test_evaluate_expr() {
        let mut row = HashMap::new();
        row.insert("price".to_string(), MutationValue::Float(100.0));
        row.insert("quantity".to_string(), MutationValue::Int(5));

        let expr = MutationExpr::Mul(
            Box::new(MutationExpr::Column("price".to_string())),
            Box::new(MutationExpr::Literal(MutationValue::Float(1.1))),
        );

        let result = MutationManager::evaluate_expr(&expr, &row);
        if let MutationValue::Float(v) = result {
            assert!((v - 110.0).abs() < 0.001);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_evaluate_coalesce() {
        let mut row = HashMap::new();
        row.insert("a".to_string(), MutationValue::Null);
        row.insert("b".to_string(), MutationValue::Int(42));

        let expr = MutationExpr::Coalesce(vec![
            MutationExpr::Column("a".to_string()),
            MutationExpr::Column("b".to_string()),
            MutationExpr::Literal(MutationValue::Int(0)),
        ]);

        let result = MutationManager::evaluate_expr(&expr, &row);
        assert_eq!(result, MutationValue::Int(42));
    }

    // MergeTree Engine Tests

    #[test]
    fn test_replacing_merge_tree() {
        let config = MergeTreeConfig {
            engine: MergeTreeEngine::ReplacingMergeTree { version_column: Some("ver".to_string()) },
            order_by: vec!["id".to_string()],
            partition_by: None,
            primary_key: None,
            sample_by: None,
            ttl: None,
            settings: MergeTreeSettings::default(),
        };

        let merger = MergeTreeMerger::new(config);

        let rows = vec![
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("ver".to_string(), MutationValue::Int(1));
                    m.insert("data".to_string(), MutationValue::String("old".to_string()));
                    m
                },
                version: Some(1),
                sign: None,
            },
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("ver".to_string(), MutationValue::Int(2));
                    m.insert("data".to_string(), MutationValue::String("new".to_string()));
                    m
                },
                version: Some(2),
                sign: None,
            },
        ];

        let merged = merger.merge(rows);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values.get("data"), Some(&MutationValue::String("new".to_string())));
    }

    #[test]
    fn test_summing_merge_tree() {
        let config = MergeTreeConfig {
            engine: MergeTreeEngine::SummingMergeTree { columns: vec!["count".to_string(), "sum".to_string()] },
            order_by: vec!["key".to_string()],
            partition_by: None,
            primary_key: None,
            sample_by: None,
            ttl: None,
            settings: MergeTreeSettings::default(),
        };

        let merger = MergeTreeMerger::new(config);

        let rows = vec![
            MergeRow {
                key: vec![MutationValue::String("a".to_string())],
                values: {
                    let mut m = HashMap::new();
                    m.insert("count".to_string(), MutationValue::Int(10));
                    m.insert("sum".to_string(), MutationValue::Int(100));
                    m
                },
                version: None,
                sign: None,
            },
            MergeRow {
                key: vec![MutationValue::String("a".to_string())],
                values: {
                    let mut m = HashMap::new();
                    m.insert("count".to_string(), MutationValue::Int(5));
                    m.insert("sum".to_string(), MutationValue::Int(50));
                    m
                },
                version: None,
                sign: None,
            },
        ];

        let merged = merger.merge(rows);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values.get("count"), Some(&MutationValue::Int(15)));
        assert_eq!(merged[0].values.get("sum"), Some(&MutationValue::Int(150)));
    }

    #[test]
    fn test_collapsing_merge_tree() {
        let config = MergeTreeConfig {
            engine: MergeTreeEngine::CollapsingMergeTree { sign_column: "sign".to_string() },
            order_by: vec!["id".to_string()],
            partition_by: None,
            primary_key: None,
            sample_by: None,
            ttl: None,
            settings: MergeTreeSettings::default(),
        };

        let merger = MergeTreeMerger::new(config);

        // Insert (+1) then delete (-1) = collapsed
        let rows = vec![
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(1));
                    m
                },
                version: None,
                sign: Some(1),
            },
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(-1));
                    m
                },
                version: None,
                sign: Some(-1),
            },
        ];

        let merged = merger.merge(rows);
        assert_eq!(merged.len(), 0); // Fully collapsed
    }

    #[test]
    fn test_collapsing_unbalanced() {
        let config = MergeTreeConfig {
            engine: MergeTreeEngine::CollapsingMergeTree { sign_column: "sign".to_string() },
            order_by: vec!["id".to_string()],
            partition_by: None,
            primary_key: None,
            sample_by: None,
            ttl: None,
            settings: MergeTreeSettings::default(),
        };

        let merger = MergeTreeMerger::new(config);

        // Two inserts, one delete = one remaining
        let rows = vec![
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(1));
                    m
                },
                version: None,
                sign: Some(1),
            },
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(1));
                    m
                },
                version: None,
                sign: Some(1),
            },
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(-1));
                    m
                },
                version: None,
                sign: Some(-1),
            },
        ];

        let merged = merger.merge(rows);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn test_versioned_collapsing() {
        let config = MergeTreeConfig {
            engine: MergeTreeEngine::VersionedCollapsingMergeTree {
                sign_column: "sign".to_string(),
                version_column: "ver".to_string(),
            },
            order_by: vec!["id".to_string()],
            partition_by: None,
            primary_key: None,
            sample_by: None,
            ttl: None,
            settings: MergeTreeSettings::default(),
        };

        let merger = MergeTreeMerger::new(config);

        let rows = vec![
            // Old version - should be ignored
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(1));
                    m.insert("ver".to_string(), MutationValue::Int(1));
                    m
                },
                version: Some(1),
                sign: Some(1),
            },
            // New version - kept
            MergeRow {
                key: vec![MutationValue::Int(1)],
                values: {
                    let mut m = HashMap::new();
                    m.insert("sign".to_string(), MutationValue::Int(1));
                    m.insert("ver".to_string(), MutationValue::Int(2));
                    m.insert("data".to_string(), MutationValue::String("latest".to_string()));
                    m
                },
                version: Some(2),
                sign: Some(1),
            },
        ];

        let merged = merger.merge(rows);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].values.get("ver"), Some(&MutationValue::Int(2)));
    }

    #[test]
    fn test_agg_state_merge() {
        let mut sum1 = AggState::Sum(100.0);
        let sum2 = AggState::Sum(50.0);
        sum1.merge(&sum2);

        if let AggState::Sum(v) = sum1 {
            assert!((v - 150.0).abs() < 0.001);
        }

        let mut avg1 = AggState::Avg { sum: 100.0, count: 10 };
        let avg2 = AggState::Avg { sum: 50.0, count: 5 };
        avg1.merge(&avg2);

        if let AggState::Avg { sum, count } = avg1 {
            assert!((sum - 150.0).abs() < 0.001);
            assert_eq!(count, 15);
            let final_val = avg1.finalize();
            if let MutationValue::Float(v) = final_val {
                assert!((v - 10.0).abs() < 0.001);
            }
        }
    }

    // Delete Mask Tests

    #[test]
    fn test_delete_mask_basic() {
        let mut mask = DeleteMask::new(100);

        assert_eq!(mask.row_count(), 100);
        assert_eq!(mask.deleted_count(), 0);
        assert_eq!(mask.live_count(), 100);

        mask.delete(5);
        mask.delete(10);
        mask.delete(50);

        assert_eq!(mask.deleted_count(), 3);
        assert!(mask.is_deleted(5));
        assert!(mask.is_deleted(10));
        assert!(mask.is_deleted(50));
        assert!(!mask.is_deleted(0));
        assert!(!mask.is_deleted(99));
    }

    #[test]
    fn test_delete_mask_range() {
        let mut mask = DeleteMask::new(100);
        mask.delete_range(10, 20);

        assert_eq!(mask.deleted_count(), 10);
        assert!(!mask.is_deleted(9));
        assert!(mask.is_deleted(10));
        assert!(mask.is_deleted(19));
        assert!(!mask.is_deleted(20));
    }

    #[test]
    fn test_delete_mask_serialize() {
        let mut mask = DeleteMask::new(1000);
        mask.delete(100);
        mask.delete(500);
        mask.delete(999);

        let bytes = mask.serialize();
        let restored = DeleteMask::deserialize(&bytes).unwrap();

        assert_eq!(restored.row_count(), 1000);
        assert_eq!(restored.deleted_count(), 3);
        assert!(restored.is_deleted(100));
        assert!(restored.is_deleted(500));
        assert!(restored.is_deleted(999));
    }

    #[test]
    fn test_delete_mask_merge() {
        let mut mask1 = DeleteMask::new(100);
        mask1.delete(10);
        mask1.delete(20);

        let mut mask2 = DeleteMask::new(100);
        mask2.delete(20);
        mask2.delete(30);

        mask1.merge(&mask2);

        assert_eq!(mask1.deleted_count(), 3); // 10, 20, 30
        assert!(mask1.is_deleted(10));
        assert!(mask1.is_deleted(20));
        assert!(mask1.is_deleted(30));
    }

    #[test]
    fn test_delete_mask_iterators() {
        let mut mask = DeleteMask::new(10);
        mask.delete(2);
        mask.delete(5);
        mask.delete(8);

        let live: Vec<u64> = mask.live_rows().collect();
        assert_eq!(live, vec![0, 1, 3, 4, 6, 7, 9]);

        let deleted: Vec<u64> = mask.deleted_rows().collect();
        assert_eq!(deleted, vec![2, 5, 8]);
    }

    #[test]
    fn test_lightweight_delete_manager() {
        let manager = LightweightDeleteManager::new(0.3);

        // Apply deletes
        let deleted = manager.apply_deletes("part_1", &[0, 1, 2], 100);
        assert_eq!(deleted, 3);

        assert!(manager.is_deleted("part_1", 0));
        assert!(manager.is_deleted("part_1", 1));
        assert!(!manager.is_deleted("part_1", 10));

        // Not yet at threshold
        assert!(!manager.needs_compaction("part_1"));

        // Delete more to exceed threshold (30%)
        let indices: Vec<u64> = (3..35).collect();
        manager.apply_deletes("part_1", &indices, 100);

        assert!(manager.needs_compaction("part_1"));
    }

    #[test]
    fn test_delete_stats() {
        let manager = LightweightDeleteManager::default();

        manager.apply_deletes("part_1", &[0, 1, 2], 100);
        manager.apply_deletes("part_2", &[0], 50);
        manager.get_or_create_mask("part_3", 200); // No deletes

        let stats = manager.stats();
        assert_eq!(stats.total_parts, 3);
        assert_eq!(stats.parts_with_deletes, 2);
        assert_eq!(stats.total_rows, 350);
        assert_eq!(stats.deleted_rows, 4);
        assert_eq!(stats.live_rows, 346);
    }

    #[test]
    fn test_deletion_ratio() {
        let mut mask = DeleteMask::new(100);
        assert!((mask.deletion_ratio() - 0.0).abs() < 0.001);

        mask.delete_range(0, 50);
        assert!((mask.deletion_ratio() - 0.5).abs() < 0.001);

        mask.delete_range(50, 100);
        assert!((mask.deletion_ratio() - 1.0).abs() < 0.001);
    }
}
