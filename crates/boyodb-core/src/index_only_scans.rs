//! Index-Only Scans with Visibility Map
//!
//! This module implements visibility map support for index-only scans.
//! When all columns needed by a query are in the index (covering index),
//! and the visibility map indicates the page is all-visible, we can
//! skip the heap fetch entirely.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum IndexScanError {
    /// Index not found
    IndexNotFound(String),
    /// Column not in index
    ColumnNotInIndex(String),
    /// Page not found
    PageNotFound(u32),
    /// Visibility check failed
    VisibilityCheckFailed,
}

impl fmt::Display for IndexScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexScanError::IndexNotFound(name) => write!(f, "Index not found: {}", name),
            IndexScanError::ColumnNotInIndex(col) => write!(f, "Column not in index: {}", col),
            IndexScanError::PageNotFound(page) => write!(f, "Page not found: {}", page),
            IndexScanError::VisibilityCheckFailed => write!(f, "Visibility check failed"),
        }
    }
}

impl std::error::Error for IndexScanError {}

// ============================================================================
// Visibility Map
// ============================================================================

/// Visibility status of a heap page
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageVisibility {
    /// All tuples on this page are visible to all transactions
    AllVisible,
    /// Some tuples may not be visible (need heap check)
    NotAllVisible,
    /// All tuples on this page are frozen (very old, always visible)
    AllFrozen,
}

/// Visibility map for a table
///
/// Each bit in the visibility map represents a heap page.
/// If the bit is set, all tuples on that page are visible to all transactions.
#[derive(Debug)]
pub struct VisibilityMap {
    /// Table name
    table_name: String,
    /// Bitmap of all-visible pages (page_id -> is_all_visible)
    all_visible: Arc<RwLock<Vec<bool>>>,
    /// Bitmap of all-frozen pages
    all_frozen: Arc<RwLock<Vec<bool>>>,
    /// Number of pages tracked
    num_pages: AtomicU64,
    /// Statistics
    stats: Arc<VisibilityMapStats>,
}

/// Statistics for visibility map operations
#[derive(Debug, Default)]
pub struct VisibilityMapStats {
    /// Number of pages marked all-visible
    pub pages_all_visible: AtomicU64,
    /// Number of pages marked all-frozen
    pub pages_all_frozen: AtomicU64,
    /// Number of visibility map lookups
    pub lookups: AtomicU64,
    /// Number of heap fetches avoided
    pub heap_fetches_avoided: AtomicU64,
    /// Number of visibility clears (due to updates)
    pub visibility_clears: AtomicU64,
}

impl VisibilityMapStats {
    pub fn snapshot(&self) -> VisibilityMapStatsSnapshot {
        VisibilityMapStatsSnapshot {
            pages_all_visible: self.pages_all_visible.load(Ordering::Relaxed),
            pages_all_frozen: self.pages_all_frozen.load(Ordering::Relaxed),
            lookups: self.lookups.load(Ordering::Relaxed),
            heap_fetches_avoided: self.heap_fetches_avoided.load(Ordering::Relaxed),
            visibility_clears: self.visibility_clears.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of visibility map statistics
#[derive(Debug, Clone)]
pub struct VisibilityMapStatsSnapshot {
    pub pages_all_visible: u64,
    pub pages_all_frozen: u64,
    pub lookups: u64,
    pub heap_fetches_avoided: u64,
    pub visibility_clears: u64,
}

impl VisibilityMap {
    pub fn new(table_name: &str, initial_pages: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            all_visible: Arc::new(RwLock::new(vec![false; initial_pages])),
            all_frozen: Arc::new(RwLock::new(vec![false; initial_pages])),
            num_pages: AtomicU64::new(initial_pages as u64),
            stats: Arc::new(VisibilityMapStats::default()),
        }
    }

    /// Get the visibility status of a page
    pub fn get_visibility(&self, page_id: u32) -> PageVisibility {
        self.stats.lookups.fetch_add(1, Ordering::Relaxed);

        let all_visible = self.all_visible.read();
        let all_frozen = self.all_frozen.read();

        let idx = page_id as usize;

        if idx >= all_visible.len() {
            return PageVisibility::NotAllVisible;
        }

        if all_frozen.get(idx).copied().unwrap_or(false) {
            PageVisibility::AllFrozen
        } else if all_visible.get(idx).copied().unwrap_or(false) {
            PageVisibility::AllVisible
        } else {
            PageVisibility::NotAllVisible
        }
    }

    /// Check if a page is all-visible
    pub fn is_all_visible(&self, page_id: u32) -> bool {
        matches!(
            self.get_visibility(page_id),
            PageVisibility::AllVisible | PageVisibility::AllFrozen
        )
    }

    /// Check if a page is all-frozen
    pub fn is_all_frozen(&self, page_id: u32) -> bool {
        self.get_visibility(page_id) == PageVisibility::AllFrozen
    }

    /// Mark a page as all-visible
    pub fn set_all_visible(&self, page_id: u32) {
        let mut all_visible = self.all_visible.write();
        let idx = page_id as usize;

        if idx >= all_visible.len() {
            all_visible.resize(idx + 1, false);
            self.num_pages.store((idx + 1) as u64, Ordering::Relaxed);
        }

        if !all_visible[idx] {
            all_visible[idx] = true;
            self.stats.pages_all_visible.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark a page as all-frozen
    pub fn set_all_frozen(&self, page_id: u32) {
        self.set_all_visible(page_id);

        let mut all_frozen = self.all_frozen.write();
        let idx = page_id as usize;

        if idx >= all_frozen.len() {
            all_frozen.resize(idx + 1, false);
        }

        if !all_frozen[idx] {
            all_frozen[idx] = true;
            self.stats.pages_all_frozen.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clear visibility for a page (called on update/delete)
    pub fn clear_visibility(&self, page_id: u32) {
        let mut all_visible = self.all_visible.write();
        let mut all_frozen = self.all_frozen.write();
        let idx = page_id as usize;

        if idx < all_visible.len() && all_visible[idx] {
            all_visible[idx] = false;
            self.stats.visibility_clears.fetch_add(1, Ordering::Relaxed);
        }

        if idx < all_frozen.len() && all_frozen[idx] {
            all_frozen[idx] = false;
        }
    }

    /// Record that a heap fetch was avoided
    pub fn record_heap_fetch_avoided(&self) {
        self.stats
            .heap_fetches_avoided
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> VisibilityMapStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get number of all-visible pages
    pub fn count_all_visible(&self) -> usize {
        let all_visible = self.all_visible.read();
        all_visible.iter().filter(|&&v| v).count()
    }

    /// Get total number of pages
    pub fn num_pages(&self) -> usize {
        self.num_pages.load(Ordering::Relaxed) as usize
    }

    /// Visibility ratio (all-visible pages / total pages)
    pub fn visibility_ratio(&self) -> f64 {
        let total = self.num_pages();
        if total == 0 {
            return 0.0;
        }
        self.count_all_visible() as f64 / total as f64
    }
}

// ============================================================================
// Covering Index
// ============================================================================

/// A covering index that includes all columns needed for certain queries
#[derive(Debug, Clone)]
pub struct CoveringIndex {
    /// Index name
    pub name: String,
    /// Table name
    pub table_name: String,
    /// Indexed columns (used for searching)
    pub key_columns: Vec<String>,
    /// Included columns (stored but not indexed)
    pub included_columns: Vec<String>,
    /// All columns available from this index
    pub all_columns: Vec<String>,
}

impl CoveringIndex {
    pub fn new(
        name: &str,
        table_name: &str,
        key_columns: Vec<String>,
        included_columns: Vec<String>,
    ) -> Self {
        let mut all_columns = key_columns.clone();
        all_columns.extend(included_columns.clone());

        Self {
            name: name.to_string(),
            table_name: table_name.to_string(),
            key_columns,
            included_columns,
            all_columns,
        }
    }

    /// Check if this index covers the given columns
    pub fn covers(&self, columns: &[&str]) -> bool {
        columns
            .iter()
            .all(|col| self.all_columns.contains(&col.to_string()))
    }

    /// Check if a column is a key column (can be used for filtering)
    pub fn is_key_column(&self, column: &str) -> bool {
        self.key_columns.contains(&column.to_string())
    }
}

// ============================================================================
// Index Entry
// ============================================================================

/// An entry in a covering index
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Page ID in the heap
    pub heap_page_id: u32,
    /// Offset within the heap page
    pub heap_offset: u16,
    /// Column values stored in the index
    pub values: Vec<IndexValue>,
}

/// Value stored in an index
#[derive(Debug, Clone, PartialEq)]
pub enum IndexValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl IndexValue {
    pub fn compare(&self, other: &IndexValue) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (IndexValue::Null, IndexValue::Null) => Ordering::Equal,
            (IndexValue::Null, _) => Ordering::Less,
            (_, IndexValue::Null) => Ordering::Greater,
            (IndexValue::Bool(a), IndexValue::Bool(b)) => a.cmp(b),
            (IndexValue::Int(a), IndexValue::Int(b)) => a.cmp(b),
            (IndexValue::Float(a), IndexValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (IndexValue::String(a), IndexValue::String(b)) => a.cmp(b),
            (IndexValue::Bytes(a), IndexValue::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

// ============================================================================
// Index-Only Scan Manager
// ============================================================================

/// Statistics for index-only scans
#[derive(Debug, Default)]
pub struct IndexOnlyScanStats {
    /// Total index scans
    pub index_scans: AtomicU64,
    /// Index-only scans (no heap access)
    pub index_only_scans: AtomicU64,
    /// Index scans that required heap access
    pub index_scans_with_heap: AtomicU64,
    /// Rows returned from index-only scans
    pub rows_from_index_only: AtomicU64,
    /// Rows that needed heap fetch
    pub rows_from_heap: AtomicU64,
}

impl IndexOnlyScanStats {
    pub fn snapshot(&self) -> IndexOnlyScanStatsSnapshot {
        IndexOnlyScanStatsSnapshot {
            index_scans: self.index_scans.load(Ordering::Relaxed),
            index_only_scans: self.index_only_scans.load(Ordering::Relaxed),
            index_scans_with_heap: self.index_scans_with_heap.load(Ordering::Relaxed),
            rows_from_index_only: self.rows_from_index_only.load(Ordering::Relaxed),
            rows_from_heap: self.rows_from_heap.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of index-only scan statistics
#[derive(Debug, Clone)]
pub struct IndexOnlyScanStatsSnapshot {
    pub index_scans: u64,
    pub index_only_scans: u64,
    pub index_scans_with_heap: u64,
    pub rows_from_index_only: u64,
    pub rows_from_heap: u64,
}

impl IndexOnlyScanStatsSnapshot {
    /// Ratio of pure index-only scans
    pub fn index_only_ratio(&self) -> f64 {
        if self.index_scans == 0 {
            return 0.0;
        }
        self.index_only_scans as f64 / self.index_scans as f64
    }

    /// Ratio of rows from index vs heap
    pub fn rows_from_index_ratio(&self) -> f64 {
        let total = self.rows_from_index_only + self.rows_from_heap;
        if total == 0 {
            return 0.0;
        }
        self.rows_from_index_only as f64 / total as f64
    }
}

/// Manages index-only scans with visibility map support
pub struct IndexOnlyScanManager {
    /// Table name
    table_name: String,
    /// Covering indexes
    indexes: Arc<RwLock<HashMap<String, CoveringIndex>>>,
    /// Index entries (index_name -> entries)
    index_data: Arc<RwLock<HashMap<String, Vec<IndexEntry>>>>,
    /// Visibility map
    visibility_map: Arc<VisibilityMap>,
    /// Statistics
    stats: Arc<IndexOnlyScanStats>,
}

impl IndexOnlyScanManager {
    pub fn new(table_name: &str, initial_pages: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            index_data: Arc::new(RwLock::new(HashMap::new())),
            visibility_map: Arc::new(VisibilityMap::new(table_name, initial_pages)),
            stats: Arc::new(IndexOnlyScanStats::default()),
        }
    }

    /// Create a covering index
    pub fn create_index(
        &self,
        name: &str,
        key_columns: Vec<String>,
        included_columns: Vec<String>,
    ) {
        let index = CoveringIndex::new(name, &self.table_name, key_columns, included_columns);

        let mut indexes = self.indexes.write();
        indexes.insert(name.to_string(), index);

        let mut data = self.index_data.write();
        data.insert(name.to_string(), Vec::new());
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> bool {
        let mut indexes = self.indexes.write();
        let removed = indexes.remove(name).is_some();

        if removed {
            let mut data = self.index_data.write();
            data.remove(name);
        }

        removed
    }

    /// Add an entry to an index
    pub fn add_entry(
        &self,
        index_name: &str,
        heap_page_id: u32,
        heap_offset: u16,
        values: Vec<IndexValue>,
    ) -> Result<(), IndexScanError> {
        let data = self.index_data.read();
        if !data.contains_key(index_name) {
            return Err(IndexScanError::IndexNotFound(index_name.to_string()));
        }
        drop(data);

        let entry = IndexEntry {
            heap_page_id,
            heap_offset,
            values,
        };

        let mut data = self.index_data.write();
        if let Some(entries) = data.get_mut(index_name) {
            entries.push(entry);
        }

        Ok(())
    }

    /// Check if a query can be satisfied by index-only scan
    pub fn can_index_only_scan(&self, index_name: &str, columns: &[&str]) -> bool {
        let indexes = self.indexes.read();
        if let Some(index) = indexes.get(index_name) {
            index.covers(columns)
        } else {
            false
        }
    }

    /// Find the best index for the given columns
    pub fn find_covering_index(&self, columns: &[&str]) -> Option<String> {
        let indexes = self.indexes.read();

        for (name, index) in indexes.iter() {
            if index.covers(columns) {
                return Some(name.clone());
            }
        }

        None
    }

    /// Perform an index-only scan
    pub fn index_only_scan(
        &self,
        index_name: &str,
        predicate: Option<&IndexPredicate>,
    ) -> Result<IndexOnlyScanResult, IndexScanError> {
        self.stats.index_scans.fetch_add(1, Ordering::Relaxed);

        let data = self.index_data.read();
        let entries = data
            .get(index_name)
            .ok_or_else(|| IndexScanError::IndexNotFound(index_name.to_string()))?;

        let mut result = IndexOnlyScanResult::new();

        for entry in entries {
            // Check predicate
            if let Some(pred) = predicate {
                if !pred.matches(entry) {
                    continue;
                }
            }

            // Check visibility map
            if self.visibility_map.is_all_visible(entry.heap_page_id) {
                // Can return value directly from index
                result.index_only_rows.push(entry.clone());
                self.visibility_map.record_heap_fetch_avoided();
                self.stats
                    .rows_from_index_only
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                // Need to check heap for visibility
                result.heap_check_rows.push(entry.clone());
                self.stats.rows_from_heap.fetch_add(1, Ordering::Relaxed);
            }
        }

        if result.heap_check_rows.is_empty() {
            self.stats.index_only_scans.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .index_scans_with_heap
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(result)
    }

    /// Mark a page as all-visible (typically after VACUUM)
    pub fn set_page_visible(&self, page_id: u32) {
        self.visibility_map.set_all_visible(page_id);
    }

    /// Mark a page as all-frozen
    pub fn set_page_frozen(&self, page_id: u32) {
        self.visibility_map.set_all_frozen(page_id);
    }

    /// Clear visibility for a page (after update/delete)
    pub fn clear_page_visibility(&self, page_id: u32) {
        self.visibility_map.clear_visibility(page_id);
    }

    /// Get visibility map stats
    pub fn visibility_stats(&self) -> VisibilityMapStatsSnapshot {
        self.visibility_map.stats()
    }

    /// Get scan stats
    pub fn scan_stats(&self) -> IndexOnlyScanStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get visibility ratio
    pub fn visibility_ratio(&self) -> f64 {
        self.visibility_map.visibility_ratio()
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read();
        indexes.keys().cloned().collect()
    }

    /// Get index info
    pub fn get_index(&self, name: &str) -> Option<CoveringIndex> {
        let indexes = self.indexes.read();
        indexes.get(name).cloned()
    }
}

/// Result of an index-only scan
#[derive(Debug)]
pub struct IndexOnlyScanResult {
    /// Rows that could be returned from index only
    pub index_only_rows: Vec<IndexEntry>,
    /// Rows that need heap check for visibility
    pub heap_check_rows: Vec<IndexEntry>,
}

impl IndexOnlyScanResult {
    pub fn new() -> Self {
        Self {
            index_only_rows: Vec::new(),
            heap_check_rows: Vec::new(),
        }
    }

    /// Total rows found
    pub fn total_rows(&self) -> usize {
        self.index_only_rows.len() + self.heap_check_rows.len()
    }

    /// Whether any heap checks were needed
    pub fn needed_heap_check(&self) -> bool {
        !self.heap_check_rows.is_empty()
    }

    /// Index-only ratio for this scan
    pub fn index_only_ratio(&self) -> f64 {
        let total = self.total_rows();
        if total == 0 {
            return 1.0;
        }
        self.index_only_rows.len() as f64 / total as f64
    }
}

impl Default for IndexOnlyScanResult {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Index Predicate
// ============================================================================

/// Predicate for filtering index entries
#[derive(Debug, Clone)]
pub enum IndexPredicate {
    /// Equality check on column
    Eq(usize, IndexValue),
    /// Less than
    Lt(usize, IndexValue),
    /// Less than or equal
    Le(usize, IndexValue),
    /// Greater than
    Gt(usize, IndexValue),
    /// Greater than or equal
    Ge(usize, IndexValue),
    /// Between (inclusive)
    Between(usize, IndexValue, IndexValue),
    /// AND of multiple predicates
    And(Vec<IndexPredicate>),
    /// OR of multiple predicates
    Or(Vec<IndexPredicate>),
    /// NOT
    Not(Box<IndexPredicate>),
}

impl IndexPredicate {
    /// Check if an index entry matches this predicate
    pub fn matches(&self, entry: &IndexEntry) -> bool {
        use std::cmp::Ordering;

        match self {
            IndexPredicate::Eq(col, value) => entry
                .values
                .get(*col)
                .map(|v| v.compare(value) == Ordering::Equal)
                .unwrap_or(false),
            IndexPredicate::Lt(col, value) => entry
                .values
                .get(*col)
                .map(|v| v.compare(value) == Ordering::Less)
                .unwrap_or(false),
            IndexPredicate::Le(col, value) => entry
                .values
                .get(*col)
                .map(|v| matches!(v.compare(value), Ordering::Less | Ordering::Equal))
                .unwrap_or(false),
            IndexPredicate::Gt(col, value) => entry
                .values
                .get(*col)
                .map(|v| v.compare(value) == Ordering::Greater)
                .unwrap_or(false),
            IndexPredicate::Ge(col, value) => entry
                .values
                .get(*col)
                .map(|v| matches!(v.compare(value), Ordering::Greater | Ordering::Equal))
                .unwrap_or(false),
            IndexPredicate::Between(col, low, high) => entry
                .values
                .get(*col)
                .map(|v| {
                    matches!(v.compare(low), Ordering::Greater | Ordering::Equal)
                        && matches!(v.compare(high), Ordering::Less | Ordering::Equal)
                })
                .unwrap_or(false),
            IndexPredicate::And(preds) => preds.iter().all(|p| p.matches(entry)),
            IndexPredicate::Or(preds) => preds.iter().any(|p| p.matches(entry)),
            IndexPredicate::Not(pred) => !pred.matches(entry),
        }
    }
}

// ============================================================================
// Query Analyzer
// ============================================================================

/// Analyzes queries to determine if index-only scan is possible
pub struct IndexOnlyScanAnalyzer {
    /// Available covering indexes
    indexes: HashMap<String, CoveringIndex>,
}

impl IndexOnlyScanAnalyzer {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    /// Add a covering index to consider
    pub fn add_index(&mut self, index: CoveringIndex) {
        self.indexes.insert(index.name.clone(), index);
    }

    /// Analyze a query to find the best index
    pub fn analyze(
        &self,
        needed_columns: &[&str],
        filter_columns: &[&str],
    ) -> Option<IndexAnalysis> {
        let needed_set: HashSet<_> = needed_columns.iter().cloned().collect();
        let filter_set: HashSet<_> = filter_columns.iter().cloned().collect();

        let mut best: Option<IndexAnalysis> = None;

        for index in self.indexes.values() {
            // Check if index covers all needed columns
            if !index.covers(needed_columns) {
                continue;
            }

            // Calculate how many filter columns are key columns
            let usable_filters = filter_columns
                .iter()
                .filter(|c| index.is_key_column(c))
                .count();

            let analysis = IndexAnalysis {
                index_name: index.name.clone(),
                covers_all_columns: true,
                usable_filter_columns: usable_filters,
                can_index_only_scan: true,
            };

            // Prefer index with more usable filters
            match &best {
                None => best = Some(analysis),
                Some(current) if analysis.usable_filter_columns > current.usable_filter_columns => {
                    best = Some(analysis);
                }
                _ => {}
            }
        }

        best
    }
}

impl Default for IndexOnlyScanAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of index analysis
#[derive(Debug, Clone)]
pub struct IndexAnalysis {
    /// Name of the recommended index
    pub index_name: String,
    /// Whether index covers all needed columns
    pub covers_all_columns: bool,
    /// Number of filter columns that can use the index
    pub usable_filter_columns: usize,
    /// Whether index-only scan is possible
    pub can_index_only_scan: bool,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_map() {
        let vm = VisibilityMap::new("test_table", 100);

        assert!(!vm.is_all_visible(0));
        assert!(!vm.is_all_visible(50));

        vm.set_all_visible(10);
        assert!(vm.is_all_visible(10));
        assert!(!vm.is_all_frozen(10));

        vm.set_all_frozen(20);
        assert!(vm.is_all_visible(20));
        assert!(vm.is_all_frozen(20));
    }

    #[test]
    fn test_visibility_map_clear() {
        let vm = VisibilityMap::new("test_table", 100);

        vm.set_all_visible(5);
        assert!(vm.is_all_visible(5));

        vm.clear_visibility(5);
        assert!(!vm.is_all_visible(5));
    }

    #[test]
    fn test_visibility_map_stats() {
        let vm = VisibilityMap::new("test_table", 100);

        vm.set_all_visible(0);
        vm.set_all_visible(1);
        vm.set_all_frozen(2);

        vm.get_visibility(0);
        vm.get_visibility(1);
        vm.get_visibility(2);

        let stats = vm.stats();
        assert_eq!(stats.pages_all_visible, 3); // Including frozen
        assert_eq!(stats.pages_all_frozen, 1);
        assert_eq!(stats.lookups, 3);
    }

    #[test]
    fn test_covering_index() {
        let index = CoveringIndex::new(
            "idx_user",
            "users",
            vec!["id".to_string(), "email".to_string()],
            vec!["name".to_string(), "age".to_string()],
        );

        assert!(index.covers(&["id", "email"]));
        assert!(index.covers(&["id", "name"]));
        assert!(index.covers(&["age"]));
        assert!(!index.covers(&["address"]));

        assert!(index.is_key_column("id"));
        assert!(index.is_key_column("email"));
        assert!(!index.is_key_column("name"));
    }

    #[test]
    fn test_index_value_compare() {
        use std::cmp::Ordering;

        assert_eq!(
            IndexValue::Int(5).compare(&IndexValue::Int(3)),
            Ordering::Greater
        );
        assert_eq!(
            IndexValue::Int(5).compare(&IndexValue::Int(5)),
            Ordering::Equal
        );
        assert_eq!(
            IndexValue::String("b".to_string()).compare(&IndexValue::String("a".to_string())),
            Ordering::Greater
        );
        assert_eq!(
            IndexValue::Null.compare(&IndexValue::Int(0)),
            Ordering::Less
        );
    }

    #[test]
    fn test_index_only_scan_manager() {
        let manager = IndexOnlyScanManager::new("users", 100);

        manager.create_index(
            "idx_user_email",
            vec!["id".to_string()],
            vec!["email".to_string(), "name".to_string()],
        );

        assert!(manager.can_index_only_scan("idx_user_email", &["id", "email"]));
        assert!(!manager.can_index_only_scan("idx_user_email", &["address"]));
    }

    #[test]
    fn test_index_only_scan_with_visibility() {
        let manager = IndexOnlyScanManager::new("users", 100);

        manager.create_index("idx_user", vec!["id".to_string()], vec!["name".to_string()]);

        // Add some entries
        manager
            .add_entry(
                "idx_user",
                0,
                0,
                vec![IndexValue::Int(1), IndexValue::String("Alice".to_string())],
            )
            .unwrap();
        manager
            .add_entry(
                "idx_user",
                0,
                1,
                vec![IndexValue::Int(2), IndexValue::String("Bob".to_string())],
            )
            .unwrap();
        manager
            .add_entry(
                "idx_user",
                1,
                0,
                vec![
                    IndexValue::Int(3),
                    IndexValue::String("Charlie".to_string()),
                ],
            )
            .unwrap();

        // No pages are all-visible yet
        let result = manager.index_only_scan("idx_user", None).unwrap();
        assert_eq!(result.index_only_rows.len(), 0);
        assert_eq!(result.heap_check_rows.len(), 3);

        // Mark page 0 as all-visible
        manager.set_page_visible(0);

        let result = manager.index_only_scan("idx_user", None).unwrap();
        assert_eq!(result.index_only_rows.len(), 2);
        assert_eq!(result.heap_check_rows.len(), 1);
    }

    #[test]
    fn test_index_predicate() {
        let entry = IndexEntry {
            heap_page_id: 0,
            heap_offset: 0,
            values: vec![IndexValue::Int(5), IndexValue::String("test".to_string())],
        };

        assert!(IndexPredicate::Eq(0, IndexValue::Int(5)).matches(&entry));
        assert!(!IndexPredicate::Eq(0, IndexValue::Int(3)).matches(&entry));

        assert!(IndexPredicate::Gt(0, IndexValue::Int(3)).matches(&entry));
        assert!(!IndexPredicate::Gt(0, IndexValue::Int(5)).matches(&entry));

        assert!(
            IndexPredicate::Between(0, IndexValue::Int(1), IndexValue::Int(10)).matches(&entry)
        );
        assert!(
            !IndexPredicate::Between(0, IndexValue::Int(6), IndexValue::Int(10)).matches(&entry)
        );

        let and_pred = IndexPredicate::And(vec![
            IndexPredicate::Ge(0, IndexValue::Int(5)),
            IndexPredicate::Le(0, IndexValue::Int(5)),
        ]);
        assert!(and_pred.matches(&entry));

        let or_pred = IndexPredicate::Or(vec![
            IndexPredicate::Eq(0, IndexValue::Int(1)),
            IndexPredicate::Eq(0, IndexValue::Int(5)),
        ]);
        assert!(or_pred.matches(&entry));
    }

    #[test]
    fn test_find_covering_index() {
        let manager = IndexOnlyScanManager::new("orders", 100);

        manager.create_index(
            "idx_orders_customer",
            vec!["customer_id".to_string()],
            vec!["order_date".to_string(), "total".to_string()],
        );

        manager.create_index(
            "idx_orders_date",
            vec!["order_date".to_string()],
            vec!["customer_id".to_string()],
        );

        let idx = manager.find_covering_index(&["customer_id", "total"]);
        assert_eq!(idx, Some("idx_orders_customer".to_string()));

        let idx = manager.find_covering_index(&["order_date", "customer_id"]);
        assert!(idx.is_some()); // Either index works
    }

    #[test]
    fn test_index_analyzer() {
        let mut analyzer = IndexOnlyScanAnalyzer::new();

        analyzer.add_index(CoveringIndex::new(
            "idx1",
            "users",
            vec!["id".to_string()],
            vec!["name".to_string()],
        ));

        analyzer.add_index(CoveringIndex::new(
            "idx2",
            "users",
            vec!["id".to_string(), "email".to_string()],
            vec!["name".to_string()],
        ));

        // Query with filter on email should prefer idx2
        let analysis = analyzer.analyze(&["id", "name"], &["email"]).unwrap();
        assert_eq!(analysis.index_name, "idx2");
        assert_eq!(analysis.usable_filter_columns, 1);
    }

    #[test]
    fn test_scan_stats() {
        let manager = IndexOnlyScanManager::new("test", 100);

        manager.create_index("idx", vec!["a".to_string()], vec!["b".to_string()]);

        manager
            .add_entry("idx", 0, 0, vec![IndexValue::Int(1), IndexValue::Int(2)])
            .unwrap();

        manager.set_page_visible(0);

        manager.index_only_scan("idx", None).unwrap();

        let stats = manager.scan_stats();
        assert_eq!(stats.index_scans, 1);
        assert_eq!(stats.index_only_scans, 1);
        assert_eq!(stats.rows_from_index_only, 1);
    }

    #[test]
    fn test_drop_index() {
        let manager = IndexOnlyScanManager::new("test", 100);

        manager.create_index("idx1", vec!["a".to_string()], vec![]);
        manager.create_index("idx2", vec!["b".to_string()], vec![]);

        assert!(manager.drop_index("idx1"));
        assert!(!manager.drop_index("idx1")); // Already dropped

        let indexes = manager.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert!(indexes.contains(&"idx2".to_string()));
    }

    #[test]
    fn test_visibility_ratio() {
        let vm = VisibilityMap::new("test", 10);

        assert_eq!(vm.visibility_ratio(), 0.0);

        vm.set_all_visible(0);
        vm.set_all_visible(1);

        assert!((vm.visibility_ratio() - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_error_display() {
        let err = IndexScanError::IndexNotFound("idx".to_string());
        assert!(format!("{}", err).contains("idx"));

        let err = IndexScanError::ColumnNotInIndex("col".to_string());
        assert!(format!("{}", err).contains("col"));

        let err = IndexScanError::PageNotFound(42);
        assert!(format!("{}", err).contains("42"));
    }

    #[test]
    fn test_index_only_scan_result() {
        let mut result = IndexOnlyScanResult::new();
        assert_eq!(result.total_rows(), 0);
        assert!(!result.needed_heap_check());

        result.index_only_rows.push(IndexEntry {
            heap_page_id: 0,
            heap_offset: 0,
            values: vec![],
        });

        assert_eq!(result.total_rows(), 1);
        assert!(!result.needed_heap_check());
        assert_eq!(result.index_only_ratio(), 1.0);

        result.heap_check_rows.push(IndexEntry {
            heap_page_id: 1,
            heap_offset: 0,
            values: vec![],
        });

        assert!(result.needed_heap_check());
        assert_eq!(result.index_only_ratio(), 0.5);
    }

    #[test]
    fn test_not_predicate() {
        let entry = IndexEntry {
            heap_page_id: 0,
            heap_offset: 0,
            values: vec![IndexValue::Int(5)],
        };

        let pred = IndexPredicate::Not(Box::new(IndexPredicate::Eq(0, IndexValue::Int(3))));
        assert!(pred.matches(&entry));

        let pred = IndexPredicate::Not(Box::new(IndexPredicate::Eq(0, IndexValue::Int(5))));
        assert!(!pred.matches(&entry));
    }
}
