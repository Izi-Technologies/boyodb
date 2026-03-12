//! Advanced Indexing Features
//!
//! Provides advanced index types beyond basic B-tree indexes.
//!
//! # Features
//! - Partial indexes (CREATE INDEX ... WHERE condition)
//! - Expression indexes (CREATE INDEX ... ON (expression))
//! - Covering indexes (INCLUDE clause)
//! - BRIN indexes for large tables
//!
//! # Example
//! ```sql
//! -- Partial index (only index active users)
//! CREATE INDEX idx_active_users ON users (email) WHERE status = 'active';
//!
//! -- Expression index
//! CREATE INDEX idx_lower_email ON users (LOWER(email));
//!
//! -- Covering index (include additional columns)
//! CREATE INDEX idx_orders_customer ON orders (customer_id) INCLUDE (order_date, total);
//!
//! -- BRIN index (for time-series data)
//! CREATE INDEX idx_events_time ON events USING BRIN (created_at);
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::SystemTime;

// ============================================================================
// Types and Errors
// ============================================================================

/// Index errors
#[derive(Debug, Clone)]
pub enum IndexError {
    /// Index not found
    NotFound(String),
    /// Invalid expression
    InvalidExpression(String),
    /// Invalid predicate
    InvalidPredicate(String),
    /// Index creation failed
    CreationFailed(String),
    /// Index scan failed
    ScanFailed(String),
    /// Unsupported operation
    Unsupported(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(name) => write!(f, "index '{}' not found", name),
            Self::InvalidExpression(expr) => write!(f, "invalid index expression: {}", expr),
            Self::InvalidPredicate(pred) => write!(f, "invalid index predicate: {}", pred),
            Self::CreationFailed(msg) => write!(f, "index creation failed: {}", msg),
            Self::ScanFailed(msg) => write!(f, "index scan failed: {}", msg),
            Self::Unsupported(msg) => write!(f, "unsupported: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for IndexError {}

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree index (default)
    BTree,
    /// Hash index
    Hash,
    /// GiST index
    GiST,
    /// GIN index
    GIN,
    /// BRIN index
    BRIN,
    /// SP-GiST index
    SpGiST,
}

impl Default for IndexType {
    fn default() -> Self {
        Self::BTree
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BTree => write!(f, "btree"),
            Self::Hash => write!(f, "hash"),
            Self::GiST => write!(f, "gist"),
            Self::GIN => write!(f, "gin"),
            Self::BRIN => write!(f, "brin"),
            Self::SpGiST => write!(f, "spgist"),
        }
    }
}

/// Index value for indexing
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexValue {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
    Bytes(Vec<u8>),
}

impl IndexValue {
    /// Convert to string for display
    pub fn to_string_value(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(b) => b.to_string(),
            Self::Int(i) => i.to_string(),
            Self::String(s) => s.clone(),
            Self::Bytes(b) => format!("{:?}", b),
        }
    }
}

// ============================================================================
// Index Column / Expression
// ============================================================================

/// Index column or expression
#[derive(Debug, Clone)]
pub enum IndexColumn {
    /// Simple column reference
    Column(String),
    /// Expression (e.g., LOWER(email))
    Expression {
        /// Expression text
        expr: String,
        /// Columns referenced in expression
        columns: Vec<String>,
    },
}

impl IndexColumn {
    /// Create a simple column
    pub fn column(name: &str) -> Self {
        Self::Column(name.to_string())
    }

    /// Create an expression column
    pub fn expression(expr: &str, columns: Vec<&str>) -> Self {
        Self::Expression {
            expr: expr.to_string(),
            columns: columns.into_iter().map(String::from).collect(),
        }
    }

    /// Get referenced columns
    pub fn referenced_columns(&self) -> Vec<&str> {
        match self {
            Self::Column(name) => vec![name.as_str()],
            Self::Expression { columns, .. } => columns.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Check if this is an expression
    pub fn is_expression(&self) -> bool {
        matches!(self, Self::Expression { .. })
    }
}

impl std::fmt::Display for IndexColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => write!(f, "{}", name),
            Self::Expression { expr, .. } => write!(f, "({})", expr),
        }
    }
}

// ============================================================================
// Index Predicate (for partial indexes)
// ============================================================================

/// Comparison operator for predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    IsNull,
    IsNotNull,
    In,
    NotIn,
    Like,
    ILike,
}

impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::Le => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Ge => write!(f, ">="),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
            Self::Like => write!(f, "LIKE"),
            Self::ILike => write!(f, "ILIKE"),
        }
    }
}

/// Index predicate for partial indexes
#[derive(Debug, Clone)]
pub enum IndexPredicate {
    /// Simple comparison: column op value
    Compare {
        column: String,
        op: CompareOp,
        value: IndexValue,
    },
    /// AND of predicates
    And(Vec<IndexPredicate>),
    /// OR of predicates
    Or(Vec<IndexPredicate>),
    /// NOT of predicate
    Not(Box<IndexPredicate>),
    /// IN list
    InList {
        column: String,
        values: Vec<IndexValue>,
    },
    /// Raw SQL expression
    Raw(String),
}

impl IndexPredicate {
    /// Create an equality predicate
    pub fn eq(column: &str, value: IndexValue) -> Self {
        Self::Compare {
            column: column.to_string(),
            op: CompareOp::Eq,
            value,
        }
    }

    /// Create an IS NOT NULL predicate
    pub fn is_not_null(column: &str) -> Self {
        Self::Compare {
            column: column.to_string(),
            op: CompareOp::IsNotNull,
            value: IndexValue::Null,
        }
    }

    /// Combine with AND
    pub fn and(self, other: IndexPredicate) -> Self {
        match self {
            Self::And(mut predicates) => {
                predicates.push(other);
                Self::And(predicates)
            }
            _ => Self::And(vec![self, other]),
        }
    }

    /// Combine with OR
    pub fn or(self, other: IndexPredicate) -> Self {
        match self {
            Self::Or(mut predicates) => {
                predicates.push(other);
                Self::Or(predicates)
            }
            _ => Self::Or(vec![self, other]),
        }
    }

    /// Check if a row matches the predicate
    pub fn matches(&self, row: &HashMap<String, IndexValue>) -> bool {
        match self {
            Self::Compare { column, op, value } => {
                let col_value = row.get(column);
                match op {
                    CompareOp::Eq => col_value == Some(value),
                    CompareOp::Ne => col_value != Some(value),
                    CompareOp::Lt => col_value.map(|v| v < value).unwrap_or(false),
                    CompareOp::Le => col_value.map(|v| v <= value).unwrap_or(false),
                    CompareOp::Gt => col_value.map(|v| v > value).unwrap_or(false),
                    CompareOp::Ge => col_value.map(|v| v >= value).unwrap_or(false),
                    CompareOp::IsNull => col_value.is_none() || col_value == Some(&IndexValue::Null),
                    CompareOp::IsNotNull => {
                        col_value.is_some() && col_value != Some(&IndexValue::Null)
                    }
                    _ => false,
                }
            }
            Self::And(predicates) => predicates.iter().all(|p| p.matches(row)),
            Self::Or(predicates) => predicates.iter().any(|p| p.matches(row)),
            Self::Not(predicate) => !predicate.matches(row),
            Self::InList { column, values } => {
                row.get(column).map(|v| values.contains(v)).unwrap_or(false)
            }
            Self::Raw(_) => true, // Can't evaluate raw SQL here
        }
    }

    /// Get referenced columns
    pub fn referenced_columns(&self) -> HashSet<String> {
        let mut columns = HashSet::new();
        self.collect_columns(&mut columns);
        columns
    }

    fn collect_columns(&self, columns: &mut HashSet<String>) {
        match self {
            Self::Compare { column, .. } => {
                columns.insert(column.clone());
            }
            Self::And(predicates) | Self::Or(predicates) => {
                for p in predicates {
                    p.collect_columns(columns);
                }
            }
            Self::Not(predicate) => predicate.collect_columns(columns),
            Self::InList { column, .. } => {
                columns.insert(column.clone());
            }
            Self::Raw(_) => {}
        }
    }
}

impl std::fmt::Display for IndexPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare { column, op, value } => {
                write!(f, "{} {} {}", column, op, value.to_string_value())
            }
            Self::And(predicates) => {
                let parts: Vec<_> = predicates.iter().map(|p| format!("({})", p)).collect();
                write!(f, "{}", parts.join(" AND "))
            }
            Self::Or(predicates) => {
                let parts: Vec<_> = predicates.iter().map(|p| format!("({})", p)).collect();
                write!(f, "{}", parts.join(" OR "))
            }
            Self::Not(predicate) => write!(f, "NOT ({})", predicate),
            Self::InList { column, values } => {
                let vals: Vec<_> = values.iter().map(|v| v.to_string_value()).collect();
                write!(f, "{} IN ({})", column, vals.join(", "))
            }
            Self::Raw(sql) => write!(f, "{}", sql),
        }
    }
}

// ============================================================================
// Partial Index
// ============================================================================

/// Partial index definition
#[derive(Debug, Clone)]
pub struct PartialIndexDef {
    /// Index name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Indexed columns/expressions
    pub columns: Vec<IndexColumn>,
    /// Index predicate (WHERE clause)
    pub predicate: IndexPredicate,
    /// Index type
    pub index_type: IndexType,
    /// Whether index is unique
    pub unique: bool,
    /// Created timestamp
    pub created_at: SystemTime,
}

/// Partial index implementation
pub struct PartialIndex {
    /// Definition
    pub def: PartialIndexDef,
    /// Index entries (key -> row IDs)
    entries: RwLock<BTreeMap<Vec<IndexValue>, Vec<u64>>>,
    /// Entry count
    entry_count: AtomicU64,
}

impl PartialIndex {
    /// Create a new partial index
    pub fn new(def: PartialIndexDef) -> Self {
        Self {
            def,
            entries: RwLock::new(BTreeMap::new()),
            entry_count: AtomicU64::new(0),
        }
    }

    /// Check if a row should be indexed
    pub fn should_index(&self, row: &HashMap<String, IndexValue>) -> bool {
        self.def.predicate.matches(row)
    }

    /// Insert a row into the index
    pub fn insert(&self, key: Vec<IndexValue>, row_id: u64, row: &HashMap<String, IndexValue>) {
        if !self.should_index(row) {
            return;
        }

        let mut entries = self.entries.write().unwrap();
        entries.entry(key).or_insert_with(Vec::new).push(row_id);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a row from the index
    pub fn remove(&self, key: &[IndexValue], row_id: u64) {
        let mut entries = self.entries.write().unwrap();
        if let Some(row_ids) = entries.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                entries.remove(key);
            }
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Lookup by key
    pub fn lookup(&self, key: &[IndexValue]) -> Vec<u64> {
        let entries = self.entries.read().unwrap();
        entries.get(key).cloned().unwrap_or_default()
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Covering Index (with INCLUDE)
// ============================================================================

/// Covering index definition
#[derive(Debug, Clone)]
pub struct CoveringIndexDef {
    /// Index name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Key columns (used for lookup)
    pub key_columns: Vec<IndexColumn>,
    /// Included columns (stored but not indexed)
    pub include_columns: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Whether index is unique
    pub unique: bool,
    /// Optional predicate (partial covering index)
    pub predicate: Option<IndexPredicate>,
    /// Created timestamp
    pub created_at: SystemTime,
}

/// Covering index entry
#[derive(Debug, Clone)]
pub struct CoveringIndexEntry {
    /// Row ID
    pub row_id: u64,
    /// Included column values
    pub included_values: HashMap<String, IndexValue>,
}

/// Covering index implementation
pub struct CoveringIndex {
    /// Definition
    pub def: CoveringIndexDef,
    /// Index entries (key -> entries with included values)
    entries: RwLock<BTreeMap<Vec<IndexValue>, Vec<CoveringIndexEntry>>>,
    /// Entry count
    entry_count: AtomicU64,
}

impl CoveringIndex {
    /// Create a new covering index
    pub fn new(def: CoveringIndexDef) -> Self {
        Self {
            def,
            entries: RwLock::new(BTreeMap::new()),
            entry_count: AtomicU64::new(0),
        }
    }

    /// Insert a row into the index
    pub fn insert(&self, key: Vec<IndexValue>, row_id: u64, row: &HashMap<String, IndexValue>) {
        // Check predicate if present
        if let Some(ref predicate) = self.def.predicate {
            if !predicate.matches(row) {
                return;
            }
        }

        // Extract included values
        let included_values: HashMap<String, IndexValue> = self
            .def
            .include_columns
            .iter()
            .filter_map(|col| row.get(col).map(|v| (col.clone(), v.clone())))
            .collect();

        let entry = CoveringIndexEntry {
            row_id,
            included_values,
        };

        let mut entries = self.entries.write().unwrap();
        entries.entry(key).or_insert_with(Vec::new).push(entry);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove a row from the index
    pub fn remove(&self, key: &[IndexValue], row_id: u64) {
        let mut entries = self.entries.write().unwrap();
        if let Some(entry_list) = entries.get_mut(key) {
            entry_list.retain(|e| e.row_id != row_id);
            if entry_list.is_empty() {
                entries.remove(key);
            }
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Lookup by key, returning entries with included values
    pub fn lookup(&self, key: &[IndexValue]) -> Vec<CoveringIndexEntry> {
        let entries = self.entries.read().unwrap();
        entries.get(key).cloned().unwrap_or_default()
    }

    /// Check if all requested columns are covered
    pub fn covers_columns(&self, columns: &[&str]) -> bool {
        let key_cols: HashSet<_> = self
            .def
            .key_columns
            .iter()
            .flat_map(|c| c.referenced_columns())
            .collect();
        let include_cols: HashSet<_> = self.def.include_columns.iter().map(|s| s.as_str()).collect();

        columns
            .iter()
            .all(|&col| key_cols.contains(col) || include_cols.contains(col))
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// BRIN Index (Block Range Index)
// ============================================================================

/// BRIN index summary for a block range
#[derive(Debug, Clone)]
pub struct BrinSummary {
    /// Block range start
    pub block_start: u64,
    /// Block range end
    pub block_end: u64,
    /// Minimum value in range
    pub min_value: IndexValue,
    /// Maximum value in range
    pub max_value: IndexValue,
    /// Whether the range has null values
    pub has_nulls: bool,
    /// Whether the range is all nulls
    pub all_nulls: bool,
}

impl BrinSummary {
    /// Check if a value might be in this range
    pub fn might_contain(&self, value: &IndexValue) -> bool {
        if *value == IndexValue::Null {
            return self.has_nulls;
        }
        value >= &self.min_value && value <= &self.max_value
    }

    /// Check if a range might overlap
    pub fn might_overlap(&self, min: &IndexValue, max: &IndexValue) -> bool {
        !(max < &self.min_value || min > &self.max_value)
    }

    /// Merge another summary into this one
    pub fn merge(&mut self, other: &BrinSummary) {
        if other.min_value < self.min_value {
            self.min_value = other.min_value.clone();
        }
        if other.max_value > self.max_value {
            self.max_value = other.max_value.clone();
        }
        self.has_nulls = self.has_nulls || other.has_nulls;
        self.all_nulls = self.all_nulls && other.all_nulls;
    }
}

/// BRIN index definition
#[derive(Debug, Clone)]
pub struct BrinIndexDef {
    /// Index name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Indexed column
    pub column: String,
    /// Pages per range (default 128)
    pub pages_per_range: u32,
    /// Created timestamp
    pub created_at: SystemTime,
}

/// BRIN index implementation
pub struct BrinIndex {
    /// Definition
    pub def: BrinIndexDef,
    /// Summaries by block range
    summaries: RwLock<BTreeMap<u64, BrinSummary>>,
    /// Total blocks indexed
    total_blocks: AtomicU64,
}

impl BrinIndex {
    /// Create a new BRIN index
    pub fn new(def: BrinIndexDef) -> Self {
        Self {
            def,
            summaries: RwLock::new(BTreeMap::new()),
            total_blocks: AtomicU64::new(0),
        }
    }

    /// Get the range ID for a block number
    fn range_id(&self, block: u64) -> u64 {
        block / self.def.pages_per_range as u64
    }

    /// Insert a value from a block
    pub fn insert(&self, block: u64, value: IndexValue) {
        let range_id = self.range_id(block);
        let range_start = range_id * self.def.pages_per_range as u64;
        let range_end = range_start + self.def.pages_per_range as u64 - 1;

        let mut summaries = self.summaries.write().unwrap();

        let summary = summaries.entry(range_id).or_insert_with(|| BrinSummary {
            block_start: range_start,
            block_end: range_end,
            min_value: value.clone(),
            max_value: value.clone(),
            has_nulls: value == IndexValue::Null,
            all_nulls: value == IndexValue::Null,
        });

        if value == IndexValue::Null {
            summary.has_nulls = true;
        } else {
            summary.all_nulls = false;
            if value < summary.min_value {
                summary.min_value = value.clone();
            }
            if value > summary.max_value {
                summary.max_value = value;
            }
        }
    }

    /// Scan for blocks that might contain values in range
    pub fn scan_range(&self, min: &IndexValue, max: &IndexValue) -> Vec<Range<u64>> {
        let summaries = self.summaries.read().unwrap();
        let mut ranges = Vec::new();

        for (_, summary) in summaries.iter() {
            if summary.might_overlap(min, max) {
                ranges.push(summary.block_start..summary.block_end + 1);
            }
        }

        // Merge adjacent ranges
        self.merge_ranges(ranges)
    }

    /// Scan for blocks that might contain a specific value
    pub fn scan_value(&self, value: &IndexValue) -> Vec<Range<u64>> {
        let summaries = self.summaries.read().unwrap();
        let mut ranges = Vec::new();

        for (_, summary) in summaries.iter() {
            if summary.might_contain(value) {
                ranges.push(summary.block_start..summary.block_end + 1);
            }
        }

        self.merge_ranges(ranges)
    }

    /// Merge adjacent block ranges
    fn merge_ranges(&self, mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        if ranges.is_empty() {
            return ranges;
        }

        ranges.sort_by_key(|r| r.start);

        let mut merged = Vec::new();
        let mut current = ranges[0].clone();

        for range in ranges.into_iter().skip(1) {
            if range.start <= current.end {
                current.end = current.end.max(range.end);
            } else {
                merged.push(current);
                current = range;
            }
        }
        merged.push(current);

        merged
    }

    /// Get number of summaries
    pub fn summary_count(&self) -> usize {
        self.summaries.read().unwrap().len()
    }

    /// Summarize the index for EXPLAIN
    pub fn summarize(&self) -> BrinIndexSummary {
        let summaries = self.summaries.read().unwrap();
        BrinIndexSummary {
            name: self.def.name.clone(),
            column: self.def.column.clone(),
            pages_per_range: self.def.pages_per_range,
            num_ranges: summaries.len(),
            total_blocks: self.total_blocks.load(Ordering::Relaxed),
        }
    }
}

/// BRIN index summary for EXPLAIN
#[derive(Debug, Clone)]
pub struct BrinIndexSummary {
    /// Index name
    pub name: String,
    /// Column name
    pub column: String,
    /// Pages per range
    pub pages_per_range: u32,
    /// Number of ranges
    pub num_ranges: usize,
    /// Total blocks indexed
    pub total_blocks: u64,
}

// ============================================================================
// Expression Index
// ============================================================================

/// Expression evaluator trait
pub trait ExpressionEvaluator: Send + Sync {
    /// Evaluate expression on a row
    fn evaluate(&self, row: &HashMap<String, IndexValue>) -> Result<IndexValue, IndexError>;
}

/// Simple expression evaluator for common functions
pub struct SimpleExprEvaluator {
    /// Expression text
    pub expr: String,
}

impl SimpleExprEvaluator {
    /// Create a new evaluator
    pub fn new(expr: &str) -> Self {
        Self {
            expr: expr.to_string(),
        }
    }
}

impl ExpressionEvaluator for SimpleExprEvaluator {
    fn evaluate(&self, row: &HashMap<String, IndexValue>) -> Result<IndexValue, IndexError> {
        let expr_upper = self.expr.to_uppercase();

        // Handle LOWER(column)
        if expr_upper.starts_with("LOWER(") && expr_upper.ends_with(')') {
            let col = &self.expr[6..self.expr.len() - 1].trim();
            if let Some(IndexValue::String(s)) = row.get(*col) {
                return Ok(IndexValue::String(s.to_lowercase()));
            }
        }

        // Handle UPPER(column)
        if expr_upper.starts_with("UPPER(") && expr_upper.ends_with(')') {
            let col = &self.expr[6..self.expr.len() - 1].trim();
            if let Some(IndexValue::String(s)) = row.get(*col) {
                return Ok(IndexValue::String(s.to_uppercase()));
            }
        }

        // Handle column reference
        if let Some(value) = row.get(&self.expr) {
            return Ok(value.clone());
        }

        Err(IndexError::InvalidExpression(format!(
            "cannot evaluate: {}",
            self.expr
        )))
    }
}

/// Expression index definition
#[derive(Debug, Clone)]
pub struct ExpressionIndexDef {
    /// Index name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Expression to index
    pub expression: String,
    /// Source columns used in expression
    pub source_columns: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Whether index is unique
    pub unique: bool,
    /// Created timestamp
    pub created_at: SystemTime,
}

/// Expression index implementation
pub struct ExpressionIndex {
    /// Definition
    pub def: ExpressionIndexDef,
    /// Expression evaluator
    evaluator: Box<dyn ExpressionEvaluator>,
    /// Index entries
    entries: RwLock<BTreeMap<IndexValue, Vec<u64>>>,
    /// Entry count
    entry_count: AtomicU64,
}

impl ExpressionIndex {
    /// Create a new expression index
    pub fn new(def: ExpressionIndexDef) -> Self {
        let evaluator = Box::new(SimpleExprEvaluator::new(&def.expression));
        Self {
            def,
            evaluator,
            entries: RwLock::new(BTreeMap::new()),
            entry_count: AtomicU64::new(0),
        }
    }

    /// Insert a row
    pub fn insert(&self, row_id: u64, row: &HashMap<String, IndexValue>) -> Result<(), IndexError> {
        let key = self.evaluator.evaluate(row)?;

        let mut entries = self.entries.write().unwrap();
        entries.entry(key).or_insert_with(Vec::new).push(row_id);
        self.entry_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Remove a row
    pub fn remove(&self, row_id: u64, row: &HashMap<String, IndexValue>) -> Result<(), IndexError> {
        let key = self.evaluator.evaluate(row)?;

        let mut entries = self.entries.write().unwrap();
        if let Some(row_ids) = entries.get_mut(&key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                entries.remove(&key);
            }
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Lookup by expression value
    pub fn lookup(&self, value: &IndexValue) -> Vec<u64> {
        let entries = self.entries.read().unwrap();
        entries.get(value).cloned().unwrap_or_default()
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Advanced Index Manager
// ============================================================================

/// Advanced index manager
pub struct AdvancedIndexManager {
    /// Partial indexes
    partial_indexes: RwLock<HashMap<String, PartialIndex>>,
    /// Covering indexes
    covering_indexes: RwLock<HashMap<String, CoveringIndex>>,
    /// BRIN indexes
    brin_indexes: RwLock<HashMap<String, BrinIndex>>,
    /// Expression indexes
    expression_indexes: RwLock<HashMap<String, ExpressionIndex>>,
}

impl Default for AdvancedIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvancedIndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            partial_indexes: RwLock::new(HashMap::new()),
            covering_indexes: RwLock::new(HashMap::new()),
            brin_indexes: RwLock::new(HashMap::new()),
            expression_indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create a partial index
    pub fn create_partial_index(&self, def: PartialIndexDef) -> Result<(), IndexError> {
        let name = def.name.clone();
        let index = PartialIndex::new(def);
        self.partial_indexes.write().unwrap().insert(name, index);
        Ok(())
    }

    /// Create a covering index
    pub fn create_covering_index(&self, def: CoveringIndexDef) -> Result<(), IndexError> {
        let name = def.name.clone();
        let index = CoveringIndex::new(def);
        self.covering_indexes.write().unwrap().insert(name, index);
        Ok(())
    }

    /// Create a BRIN index
    pub fn create_brin_index(&self, def: BrinIndexDef) -> Result<(), IndexError> {
        let name = def.name.clone();
        let index = BrinIndex::new(def);
        self.brin_indexes.write().unwrap().insert(name, index);
        Ok(())
    }

    /// Create an expression index
    pub fn create_expression_index(&self, def: ExpressionIndexDef) -> Result<(), IndexError> {
        let name = def.name.clone();
        let index = ExpressionIndex::new(def);
        self.expression_indexes.write().unwrap().insert(name, index);
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> bool {
        if self.partial_indexes.write().unwrap().remove(name).is_some() {
            return true;
        }
        if self.covering_indexes.write().unwrap().remove(name).is_some() {
            return true;
        }
        if self.brin_indexes.write().unwrap().remove(name).is_some() {
            return true;
        }
        if self.expression_indexes.write().unwrap().remove(name).is_some() {
            return true;
        }
        false
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        let mut indexes = Vec::new();

        for (name, idx) in self.partial_indexes.read().unwrap().iter() {
            indexes.push(IndexInfo {
                name: name.clone(),
                schema: idx.def.schema.clone(),
                table: idx.def.table.clone(),
                index_type: idx.def.index_type,
                is_partial: true,
                is_covering: false,
                is_expression: false,
                entry_count: idx.entry_count(),
            });
        }

        for (name, idx) in self.covering_indexes.read().unwrap().iter() {
            indexes.push(IndexInfo {
                name: name.clone(),
                schema: idx.def.schema.clone(),
                table: idx.def.table.clone(),
                index_type: idx.def.index_type,
                is_partial: idx.def.predicate.is_some(),
                is_covering: true,
                is_expression: false,
                entry_count: idx.entry_count(),
            });
        }

        for (name, idx) in self.brin_indexes.read().unwrap().iter() {
            indexes.push(IndexInfo {
                name: name.clone(),
                schema: idx.def.schema.clone(),
                table: idx.def.table.clone(),
                index_type: IndexType::BRIN,
                is_partial: false,
                is_covering: false,
                is_expression: false,
                entry_count: idx.summary_count() as u64,
            });
        }

        for (name, idx) in self.expression_indexes.read().unwrap().iter() {
            indexes.push(IndexInfo {
                name: name.clone(),
                schema: idx.def.schema.clone(),
                table: idx.def.table.clone(),
                index_type: idx.def.index_type,
                is_partial: false,
                is_covering: false,
                is_expression: true,
                entry_count: idx.entry_count(),
            });
        }

        indexes
    }
}

/// Index information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub name: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Index type
    pub index_type: IndexType,
    /// Whether it's a partial index
    pub is_partial: bool,
    /// Whether it's a covering index
    pub is_covering: bool,
    /// Whether it's an expression index
    pub is_expression: bool,
    /// Number of entries
    pub entry_count: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_index() {
        let def = PartialIndexDef {
            name: "idx_active_users".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            columns: vec![IndexColumn::column("email")],
            predicate: IndexPredicate::eq("status", IndexValue::String("active".to_string())),
            index_type: IndexType::BTree,
            unique: false,
            created_at: SystemTime::now(),
        };

        let index = PartialIndex::new(def);

        // Active user - should be indexed
        let mut row1 = HashMap::new();
        row1.insert("email".to_string(), IndexValue::String("a@b.com".to_string()));
        row1.insert("status".to_string(), IndexValue::String("active".to_string()));
        assert!(index.should_index(&row1));

        // Inactive user - should not be indexed
        let mut row2 = HashMap::new();
        row2.insert("email".to_string(), IndexValue::String("c@d.com".to_string()));
        row2.insert("status".to_string(), IndexValue::String("inactive".to_string()));
        assert!(!index.should_index(&row2));
    }

    #[test]
    fn test_covering_index() {
        let def = CoveringIndexDef {
            name: "idx_orders_customer".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
            key_columns: vec![IndexColumn::column("customer_id")],
            include_columns: vec!["order_date".to_string(), "total".to_string()],
            index_type: IndexType::BTree,
            unique: false,
            predicate: None,
            created_at: SystemTime::now(),
        };

        let index = CoveringIndex::new(def);

        assert!(index.covers_columns(&["customer_id", "order_date", "total"]));
        assert!(!index.covers_columns(&["customer_id", "status"]));
    }

    #[test]
    fn test_brin_index() {
        let def = BrinIndexDef {
            name: "idx_events_time".to_string(),
            schema: "public".to_string(),
            table: "events".to_string(),
            column: "created_at".to_string(),
            pages_per_range: 4,
            created_at: SystemTime::now(),
        };

        let index = BrinIndex::new(def);

        // Insert values in different blocks
        index.insert(0, IndexValue::Int(100));
        index.insert(1, IndexValue::Int(150));
        index.insert(4, IndexValue::Int(200));
        index.insert(5, IndexValue::Int(250));

        // Scan for range
        let ranges = index.scan_range(&IndexValue::Int(120), &IndexValue::Int(180));
        assert!(!ranges.is_empty());

        // Value outside all ranges
        let ranges = index.scan_value(&IndexValue::Int(50));
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_expression_index() {
        let def = ExpressionIndexDef {
            name: "idx_lower_email".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            expression: "LOWER(email)".to_string(),
            source_columns: vec!["email".to_string()],
            index_type: IndexType::BTree,
            unique: false,
            created_at: SystemTime::now(),
        };

        let index = ExpressionIndex::new(def);

        let mut row = HashMap::new();
        row.insert(
            "email".to_string(),
            IndexValue::String("Test@Example.COM".to_string()),
        );

        index.insert(1, &row).unwrap();

        let results = index.lookup(&IndexValue::String("test@example.com".to_string()));
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_predicate_matches() {
        let pred = IndexPredicate::eq("status", IndexValue::String("active".to_string()));

        let mut row1 = HashMap::new();
        row1.insert("status".to_string(), IndexValue::String("active".to_string()));
        assert!(pred.matches(&row1));

        let mut row2 = HashMap::new();
        row2.insert("status".to_string(), IndexValue::String("inactive".to_string()));
        assert!(!pred.matches(&row2));
    }

    #[test]
    fn test_predicate_and() {
        let pred = IndexPredicate::eq("status", IndexValue::String("active".to_string()))
            .and(IndexPredicate::is_not_null("email"));

        let mut row = HashMap::new();
        row.insert("status".to_string(), IndexValue::String("active".to_string()));
        row.insert("email".to_string(), IndexValue::String("a@b.com".to_string()));
        assert!(pred.matches(&row));
    }

    #[test]
    fn test_cron_field_display() {
        assert_eq!(
            format!("{}", IndexPredicate::eq("x", IndexValue::Int(1))),
            "x = 1"
        );
    }

    #[test]
    fn test_index_manager() {
        let manager = AdvancedIndexManager::new();

        manager
            .create_partial_index(PartialIndexDef {
                name: "test_partial".to_string(),
                schema: "public".to_string(),
                table: "t".to_string(),
                columns: vec![IndexColumn::column("a")],
                predicate: IndexPredicate::is_not_null("a"),
                index_type: IndexType::BTree,
                unique: false,
                created_at: SystemTime::now(),
            })
            .unwrap();

        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert!(indexes[0].is_partial);

        assert!(manager.drop_index("test_partial"));
        assert_eq!(manager.list_indexes().len(), 0);
    }
}
