//! Heap-Only Tuple (HOT) Updates
//!
//! This module implements PostgreSQL-style HOT updates for minimizing index overhead
//! when updating non-indexed columns. When an update only affects columns that aren't
//! part of any index, the new tuple can be placed on the same page as the old tuple
//! without requiring index updates.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Error Types
// ============================================================================

/// Errors from HOT update operations
#[derive(Debug, Clone)]
pub enum HotError {
    /// Page is full, cannot perform HOT update
    PageFull,
    /// Column is indexed, cannot perform HOT update
    IndexedColumnModified(String),
    /// Row not found
    RowNotFound(u64),
    /// Invalid HOT chain
    InvalidChain,
    /// Tuple too large for page
    TupleTooLarge,
}

impl fmt::Display for HotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HotError::PageFull => write!(f, "Page is full"),
            HotError::IndexedColumnModified(col) => {
                write!(f, "Indexed column modified: {}", col)
            }
            HotError::RowNotFound(id) => write!(f, "Row not found: {}", id),
            HotError::InvalidChain => write!(f, "Invalid HOT chain"),
            HotError::TupleTooLarge => write!(f, "Tuple too large for page"),
        }
    }
}

impl std::error::Error for HotError {}

// ============================================================================
// HOT Tuple
// ============================================================================

/// Status of a tuple in the heap
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleStatus {
    /// Tuple is live and visible
    Live,
    /// Tuple has been HOT-updated
    HotUpdated,
    /// Tuple is dead and can be reclaimed
    Dead,
    /// Tuple is being updated (in-progress transaction)
    Updating,
}

/// A tuple header containing HOT-related information
#[derive(Debug, Clone)]
pub struct TupleHeader {
    /// Transaction ID that created this tuple
    pub xmin: u64,
    /// Transaction ID that deleted/updated this tuple
    pub xmax: u64,
    /// Tuple status
    pub status: TupleStatus,
    /// Offset to next tuple in HOT chain (0 if none)
    pub hot_next: u16,
    /// This tuple is a HOT-update successor
    pub is_heap_only: bool,
    /// Information bits
    pub info_mask: u16,
}

impl TupleHeader {
    pub fn new(xmin: u64) -> Self {
        Self {
            xmin,
            xmax: 0,
            status: TupleStatus::Live,
            hot_next: 0,
            is_heap_only: false,
            info_mask: 0,
        }
    }

    /// Check if tuple is visible to the given transaction
    pub fn is_visible(&self, current_xid: u64) -> bool {
        match self.status {
            TupleStatus::Live => self.xmin <= current_xid,
            TupleStatus::HotUpdated => {
                // Need to follow HOT chain
                self.xmin <= current_xid && self.xmax > current_xid
            }
            TupleStatus::Dead => false,
            TupleStatus::Updating => self.xmin <= current_xid,
        }
    }
}

/// A heap tuple containing data and HOT metadata
#[derive(Debug, Clone)]
pub struct HeapTuple {
    /// Tuple ID (row ID)
    pub tid: u64,
    /// Page number
    pub page_id: u32,
    /// Offset within page
    pub offset: u16,
    /// Tuple header
    pub header: TupleHeader,
    /// Column values
    pub data: Vec<Vec<u8>>,
}

impl HeapTuple {
    pub fn new(tid: u64, page_id: u32, offset: u16, xmin: u64, data: Vec<Vec<u8>>) -> Self {
        Self {
            tid,
            page_id,
            offset,
            header: TupleHeader::new(xmin),
            data,
        }
    }

    /// Estimate tuple size in bytes
    pub fn size(&self) -> usize {
        // Header size + data size
        32 + self.data.iter().map(|v| v.len() + 4).sum::<usize>()
    }
}

// ============================================================================
// Heap Page
// ============================================================================

/// A heap page containing tuples
#[derive(Debug)]
pub struct HeapPage {
    /// Page ID
    pub page_id: u32,
    /// Total page size
    pub page_size: usize,
    /// Used space
    pub used_space: usize,
    /// Tuples on this page
    pub tuples: Vec<HeapTuple>,
    /// Line pointer array (offset -> tuple index)
    pub line_pointers: Vec<u16>,
    /// Free space offset
    pub free_space_offset: u16,
}

impl HeapPage {
    pub fn new(page_id: u32, page_size: usize) -> Self {
        Self {
            page_id,
            page_size,
            used_space: 24, // Page header
            tuples: Vec::new(),
            line_pointers: Vec::new(),
            free_space_offset: 24,
        }
    }

    /// Available free space
    pub fn free_space(&self) -> usize {
        self.page_size.saturating_sub(self.used_space)
    }

    /// Check if tuple fits on this page
    pub fn can_fit(&self, tuple_size: usize) -> bool {
        self.free_space() >= tuple_size + 4 // 4 bytes for line pointer
    }

    /// Add a tuple to the page
    pub fn add_tuple(&mut self, mut tuple: HeapTuple) -> Result<u16, HotError> {
        let size = tuple.size();
        if !self.can_fit(size) {
            return Err(HotError::PageFull);
        }

        let offset = self.line_pointers.len() as u16;
        tuple.offset = offset;
        tuple.page_id = self.page_id;

        self.line_pointers.push(self.tuples.len() as u16);
        self.tuples.push(tuple);
        self.used_space += size + 4;

        Ok(offset)
    }

    /// Get tuple by offset
    pub fn get_tuple(&self, offset: u16) -> Option<&HeapTuple> {
        let idx = *self.line_pointers.get(offset as usize)?;
        self.tuples.get(idx as usize)
    }

    /// Get mutable tuple by offset
    pub fn get_tuple_mut(&mut self, offset: u16) -> Option<&mut HeapTuple> {
        let idx = *self.line_pointers.get(offset as usize)?;
        self.tuples.get_mut(idx as usize)
    }

    /// Perform HOT prune - remove dead tuples and compact chains
    pub fn hot_prune(&mut self, oldest_active_xid: u64) -> usize {
        let mut pruned = 0;

        // Mark tuples as dead if their xmax is less than oldest active
        for tuple in &mut self.tuples {
            if tuple.header.status == TupleStatus::HotUpdated
                && tuple.header.xmax < oldest_active_xid
            {
                tuple.header.status = TupleStatus::Dead;
                pruned += 1;
            }
        }

        // Compact the page by removing dead tuples at the end of chains
        self.compact_dead_tuples();

        pruned
    }

    fn compact_dead_tuples(&mut self) {
        // Simple compaction - just mark space as reclaimable
        // Full compaction would require rewriting line pointers
        let dead_space: usize = self
            .tuples
            .iter()
            .filter(|t| t.header.status == TupleStatus::Dead && t.header.hot_next == 0)
            .map(|t| t.size())
            .sum();

        self.used_space = self.used_space.saturating_sub(dead_space);
    }
}

// ============================================================================
// HOT Update Manager
// ============================================================================

/// Statistics for HOT updates
#[derive(Debug, Default)]
pub struct HotStats {
    /// Total updates attempted
    pub updates_attempted: AtomicU64,
    /// Successful HOT updates
    pub hot_updates: AtomicU64,
    /// Non-HOT updates (indexed column changed)
    pub non_hot_updates: AtomicU64,
    /// Updates that required new page
    pub page_splits: AtomicU64,
    /// HOT chains created
    pub chains_created: AtomicU64,
    /// HOT chains pruned
    pub chains_pruned: AtomicU64,
}

impl HotStats {
    pub fn snapshot(&self) -> HotStatsSnapshot {
        HotStatsSnapshot {
            updates_attempted: self.updates_attempted.load(Ordering::Relaxed),
            hot_updates: self.hot_updates.load(Ordering::Relaxed),
            non_hot_updates: self.non_hot_updates.load(Ordering::Relaxed),
            page_splits: self.page_splits.load(Ordering::Relaxed),
            chains_created: self.chains_created.load(Ordering::Relaxed),
            chains_pruned: self.chains_pruned.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of HOT statistics
#[derive(Debug, Clone)]
pub struct HotStatsSnapshot {
    pub updates_attempted: u64,
    pub hot_updates: u64,
    pub non_hot_updates: u64,
    pub page_splits: u64,
    pub chains_created: u64,
    pub chains_pruned: u64,
}

impl HotStatsSnapshot {
    /// HOT update ratio
    pub fn hot_ratio(&self) -> f64 {
        if self.updates_attempted == 0 {
            return 0.0;
        }
        self.hot_updates as f64 / self.updates_attempted as f64
    }
}

/// Manages HOT updates for a table
pub struct HotUpdateManager {
    /// Table name
    table_name: String,
    /// Indexed columns (updates to these prevent HOT)
    indexed_columns: Arc<RwLock<HashSet<String>>>,
    /// All column names in order
    columns: Vec<String>,
    /// Heap pages
    pages: Arc<RwLock<HashMap<u32, HeapPage>>>,
    /// Tuple ID to location mapping
    tuple_locations: Arc<RwLock<HashMap<u64, (u32, u16)>>>,
    /// Next page ID
    next_page_id: AtomicU64,
    /// Next tuple ID
    next_tuple_id: AtomicU64,
    /// Page size
    page_size: usize,
    /// Statistics
    stats: Arc<HotStats>,
    /// Fill factor (percentage of page to fill before using new page)
    fill_factor: f64,
}

impl HotUpdateManager {
    pub fn new(table_name: &str, columns: Vec<String>, page_size: usize) -> Self {
        Self {
            table_name: table_name.to_string(),
            indexed_columns: Arc::new(RwLock::new(HashSet::new())),
            columns,
            pages: Arc::new(RwLock::new(HashMap::new())),
            tuple_locations: Arc::new(RwLock::new(HashMap::new())),
            next_page_id: AtomicU64::new(0),
            next_tuple_id: AtomicU64::new(0),
            page_size,
            stats: Arc::new(HotStats::default()),
            fill_factor: 0.9,
        }
    }

    /// Set fill factor (0.0 - 1.0)
    pub fn set_fill_factor(&mut self, factor: f64) {
        self.fill_factor = factor.clamp(0.1, 1.0);
    }

    /// Add an indexed column
    pub fn add_indexed_column(&self, column: &str) {
        let mut indexed = self.indexed_columns.write();
        indexed.insert(column.to_string());
    }

    /// Remove an indexed column
    pub fn remove_indexed_column(&self, column: &str) {
        let mut indexed = self.indexed_columns.write();
        indexed.remove(column);
    }

    /// Get indexed columns
    pub fn indexed_columns(&self) -> Vec<String> {
        let indexed = self.indexed_columns.read();
        indexed.iter().cloned().collect()
    }

    /// Check if an update can be HOT
    pub fn can_hot_update(&self, modified_columns: &[&str]) -> Result<bool, HotError> {
        let indexed = self.indexed_columns.read();

        for col in modified_columns {
            if indexed.contains(*col) {
                return Err(HotError::IndexedColumnModified(col.to_string()));
            }
        }

        Ok(true)
    }

    /// Insert a new tuple
    pub fn insert(&self, data: Vec<Vec<u8>>, xid: u64) -> Result<u64, HotError> {
        let tid = self.next_tuple_id.fetch_add(1, Ordering::SeqCst);
        let tuple = HeapTuple::new(tid, 0, 0, xid, data);
        let tuple_size = tuple.size();

        let mut pages = self.pages.write();

        // Find a page with enough space
        let target_space = (self.page_size as f64 * self.fill_factor) as usize;
        let page_id = {
            let mut found_page = None;
            for (id, page) in pages.iter() {
                if page.can_fit(tuple_size) && page.used_space < target_space {
                    found_page = Some(*id);
                    break;
                }
            }
            found_page
        };

        let (page_id, offset) = if let Some(pid) = page_id {
            let page = pages.get_mut(&pid).unwrap();
            let offset = page.add_tuple(tuple)?;
            (pid, offset)
        } else {
            // Create new page
            let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst) as u32;
            let mut page = HeapPage::new(new_page_id, self.page_size);
            let offset = page.add_tuple(tuple)?;
            pages.insert(new_page_id, page);
            (new_page_id, offset)
        };

        let mut locations = self.tuple_locations.write();
        locations.insert(tid, (page_id, offset));

        Ok(tid)
    }

    /// Perform an update (HOT if possible)
    pub fn update(
        &self,
        tid: u64,
        new_data: Vec<Vec<u8>>,
        modified_columns: &[&str],
        xid: u64,
    ) -> Result<HotUpdateResult, HotError> {
        self.stats.updates_attempted.fetch_add(1, Ordering::Relaxed);

        // Check if we can do HOT update
        let can_hot = match self.can_hot_update(modified_columns) {
            Ok(true) => true,
            Ok(false) => false,
            Err(HotError::IndexedColumnModified(_)) => false,
            Err(e) => return Err(e),
        };

        let locations = self.tuple_locations.read();
        let (page_id, offset) = locations
            .get(&tid)
            .copied()
            .ok_or(HotError::RowNotFound(tid))?;
        drop(locations);

        let mut pages = self.pages.write();
        let page = pages.get_mut(&page_id).ok_or(HotError::RowNotFound(tid))?;

        if can_hot {
            // Try HOT update - place new tuple on same page
            let new_tuple = HeapTuple::new(tid, page_id, 0, xid, new_data.clone());
            let tuple_size = new_tuple.size();

            if page.can_fit(tuple_size) {
                // First, mark old tuple as HOT-updated
                if let Some(old_tuple) = page.get_tuple_mut(offset) {
                    old_tuple.header.status = TupleStatus::HotUpdated;
                    old_tuple.header.xmax = xid;
                }

                // Add new tuple to same page
                let new_offset = page.add_tuple(new_tuple)?;

                // Update old tuple's hot_next pointer
                if let Some(old_tuple) = page.get_tuple_mut(offset) {
                    old_tuple.header.hot_next = new_offset;
                }

                // Update new tuple to be heap-only
                if let Some(new_tuple) = page.get_tuple_mut(new_offset) {
                    new_tuple.header.is_heap_only = true;
                }

                // Update location mapping
                let mut locations = self.tuple_locations.write();
                locations.insert(tid, (page_id, new_offset));

                self.stats.hot_updates.fetch_add(1, Ordering::Relaxed);
                self.stats.chains_created.fetch_add(1, Ordering::Relaxed);

                return Ok(HotUpdateResult::Hot {
                    old_page: page_id,
                    old_offset: offset,
                    new_offset,
                });
            }
        }

        // Cannot do HOT - need regular update with index updates
        self.stats.non_hot_updates.fetch_add(1, Ordering::Relaxed);

        // Mark old tuple as updated
        if let Some(old_tuple) = page.get_tuple_mut(offset) {
            old_tuple.header.status = TupleStatus::HotUpdated;
            old_tuple.header.xmax = xid;
        }

        // Find or create a new page for the tuple
        let new_tuple = HeapTuple::new(tid, 0, 0, xid, new_data);
        let tuple_size = new_tuple.size();

        let target_space = (self.page_size as f64 * self.fill_factor) as usize;
        let target_page = pages.iter().find_map(|(id, p)| {
            if *id != page_id && p.can_fit(tuple_size) && p.used_space < target_space {
                Some(*id)
            } else {
                None
            }
        });

        let (new_page_id, new_offset) = if let Some(pid) = target_page {
            let target = pages.get_mut(&pid).unwrap();
            let offset = target.add_tuple(new_tuple)?;
            (pid, offset)
        } else {
            // Create new page
            let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst) as u32;
            let mut new_page = HeapPage::new(new_page_id, self.page_size);
            let offset = new_page.add_tuple(new_tuple)?;
            pages.insert(new_page_id, new_page);
            self.stats.page_splits.fetch_add(1, Ordering::Relaxed);
            (new_page_id, offset)
        };

        // Update location
        let mut locations = self.tuple_locations.write();
        locations.insert(tid, (new_page_id, new_offset));

        Ok(HotUpdateResult::NonHot {
            old_page: page_id,
            old_offset: offset,
            new_page: new_page_id,
            new_offset,
            requires_index_update: !can_hot,
        })
    }

    /// Delete a tuple
    pub fn delete(&self, tid: u64, xid: u64) -> Result<(), HotError> {
        let locations = self.tuple_locations.read();
        let (page_id, offset) = locations
            .get(&tid)
            .copied()
            .ok_or(HotError::RowNotFound(tid))?;
        drop(locations);

        let mut pages = self.pages.write();
        let page = pages.get_mut(&page_id).ok_or(HotError::RowNotFound(tid))?;

        if let Some(tuple) = page.get_tuple_mut(offset) {
            tuple.header.xmax = xid;
            tuple.header.status = TupleStatus::Dead;
        }

        Ok(())
    }

    /// Get a tuple by ID
    pub fn get(&self, tid: u64, xid: u64) -> Option<HeapTuple> {
        let locations = self.tuple_locations.read();
        let (page_id, offset) = locations.get(&tid).copied()?;
        drop(locations);

        let pages = self.pages.read();
        let page = pages.get(&page_id)?;

        // Follow HOT chain to find visible tuple
        let mut current_offset = offset;
        loop {
            let tuple = page.get_tuple(current_offset)?;

            if tuple.header.is_visible(xid) {
                if tuple.header.hot_next == 0 || tuple.header.status != TupleStatus::HotUpdated {
                    return Some(tuple.clone());
                }
                current_offset = tuple.header.hot_next;
            } else {
                break;
            }
        }

        None
    }

    /// Prune HOT chains on all pages
    pub fn prune_all(&self, oldest_active_xid: u64) -> usize {
        let mut pages = self.pages.write();
        let mut total_pruned = 0;

        for page in pages.values_mut() {
            total_pruned += page.hot_prune(oldest_active_xid);
        }

        self.stats
            .chains_pruned
            .fetch_add(total_pruned as u64, Ordering::Relaxed);
        total_pruned
    }

    /// Get statistics
    pub fn stats(&self) -> HotStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get number of pages
    pub fn page_count(&self) -> usize {
        self.pages.read().len()
    }

    /// Get total tuples
    pub fn tuple_count(&self) -> usize {
        self.tuple_locations.read().len()
    }
}

/// Result of a HOT update operation
#[derive(Debug, Clone)]
pub enum HotUpdateResult {
    /// Update was performed as HOT (same page)
    Hot {
        old_page: u32,
        old_offset: u16,
        new_offset: u16,
    },
    /// Update required new page placement
    NonHot {
        old_page: u32,
        old_offset: u16,
        new_page: u32,
        new_offset: u16,
        requires_index_update: bool,
    },
}

impl HotUpdateResult {
    pub fn is_hot(&self) -> bool {
        matches!(self, HotUpdateResult::Hot { .. })
    }

    pub fn requires_index_update(&self) -> bool {
        match self {
            HotUpdateResult::Hot { .. } => false,
            HotUpdateResult::NonHot {
                requires_index_update,
                ..
            } => *requires_index_update,
        }
    }
}

// ============================================================================
// HOT Chain Iterator
// ============================================================================

/// Iterator over HOT chain versions
pub struct HotChainIterator<'a> {
    page: &'a HeapPage,
    current_offset: Option<u16>,
}

impl<'a> HotChainIterator<'a> {
    pub fn new(page: &'a HeapPage, start_offset: u16) -> Self {
        Self {
            page,
            current_offset: Some(start_offset),
        }
    }
}

impl<'a> Iterator for HotChainIterator<'a> {
    type Item = &'a HeapTuple;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.current_offset?;
        let tuple = self.page.get_tuple(offset)?;

        if tuple.header.hot_next != 0 {
            self.current_offset = Some(tuple.header.hot_next);
        } else {
            self.current_offset = None;
        }

        Some(tuple)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_header() {
        let header = TupleHeader::new(1);
        assert_eq!(header.xmin, 1);
        assert_eq!(header.xmax, 0);
        assert_eq!(header.status, TupleStatus::Live);
        assert!(header.is_visible(1));
        assert!(header.is_visible(2));
    }

    #[test]
    fn test_heap_tuple() {
        let data = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let tuple = HeapTuple::new(1, 0, 0, 1, data.clone());

        assert_eq!(tuple.tid, 1);
        assert_eq!(tuple.data, data);
        assert!(tuple.size() > 0);
    }

    #[test]
    fn test_heap_page() {
        let mut page = HeapPage::new(0, 8192);
        assert_eq!(page.page_id, 0);
        assert!(page.free_space() > 0);

        let tuple = HeapTuple::new(1, 0, 0, 1, vec![vec![1, 2, 3]]);
        let offset = page.add_tuple(tuple).unwrap();
        assert_eq!(offset, 0);

        let retrieved = page.get_tuple(offset).unwrap();
        assert_eq!(retrieved.tid, 1);
    }

    #[test]
    fn test_hot_manager_insert() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "name".to_string()],
            8192,
        );

        let tid = manager.insert(vec![vec![1], vec![2, 3, 4]], 1).unwrap();
        assert_eq!(tid, 0);

        let tuple = manager.get(tid, 2).unwrap();
        assert_eq!(tuple.data, vec![vec![1], vec![2, 3, 4]]);
    }

    #[test]
    fn test_hot_update_non_indexed() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "name".to_string()],
            8192,
        );

        // Index only the 'id' column
        manager.add_indexed_column("id");

        // Insert
        let tid = manager.insert(vec![vec![1], vec![2, 3, 4]], 1).unwrap();

        // Update 'name' (not indexed) - should be HOT
        let result = manager
            .update(tid, vec![vec![1], vec![5, 6, 7]], &["name"], 2)
            .unwrap();

        assert!(result.is_hot());
        assert!(!result.requires_index_update());

        let stats = manager.stats();
        assert_eq!(stats.hot_updates, 1);
    }

    #[test]
    fn test_non_hot_update_indexed() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "name".to_string()],
            8192,
        );

        manager.add_indexed_column("id");

        let tid = manager.insert(vec![vec![1], vec![2, 3, 4]], 1).unwrap();

        // Update 'id' (indexed) - should NOT be HOT
        let result = manager
            .update(tid, vec![vec![99], vec![2, 3, 4]], &["id"], 2)
            .unwrap();

        assert!(!result.is_hot());
        assert!(result.requires_index_update());

        let stats = manager.stats();
        assert_eq!(stats.non_hot_updates, 1);
    }

    #[test]
    fn test_hot_chain() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "data".to_string()],
            8192,
        );

        manager.add_indexed_column("id");

        let tid = manager.insert(vec![vec![1], vec![1]], 1).unwrap();

        // Multiple HOT updates
        for i in 2..5u8 {
            manager
                .update(tid, vec![vec![1], vec![i]], &["data"], i as u64)
                .unwrap();
        }

        let tuple = manager.get(tid, 10).unwrap();
        assert_eq!(tuple.data[1], vec![4]);

        let stats = manager.stats();
        assert_eq!(stats.hot_updates, 3);
    }

    #[test]
    fn test_delete() {
        let manager = HotUpdateManager::new("test_table", vec!["id".to_string()], 8192);

        let tid = manager.insert(vec![vec![1]], 1).unwrap();
        assert!(manager.get(tid, 2).is_some());

        manager.delete(tid, 3).unwrap();

        // Tuple should not be visible after delete xid
        // Note: In real impl, visibility would depend on MVCC
    }

    #[test]
    fn test_prune() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "data".to_string()],
            8192,
        );

        manager.add_indexed_column("id");

        let tid = manager.insert(vec![vec![1], vec![1]], 1).unwrap();

        // Create some HOT updates
        for i in 2..5u8 {
            manager
                .update(tid, vec![vec![1], vec![i]], &["data"], i as u64)
                .unwrap();
        }

        // Prune old versions
        let pruned = manager.prune_all(10);
        assert!(pruned > 0);
    }

    #[test]
    fn test_can_hot_update() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            8192,
        );

        manager.add_indexed_column("a");
        manager.add_indexed_column("b");

        assert!(manager.can_hot_update(&["c"]).unwrap());
        assert!(manager.can_hot_update(&[]).unwrap());

        let err = manager.can_hot_update(&["a"]).unwrap_err();
        assert!(matches!(err, HotError::IndexedColumnModified(_)));
    }

    #[test]
    fn test_fill_factor() {
        let mut manager = HotUpdateManager::new("test_table", vec!["id".to_string()], 8192);

        manager.set_fill_factor(0.5);
        assert_eq!(manager.fill_factor, 0.5);

        manager.set_fill_factor(0.05);
        assert_eq!(manager.fill_factor, 0.1); // Clamped to minimum

        manager.set_fill_factor(1.5);
        assert_eq!(manager.fill_factor, 1.0); // Clamped to maximum
    }

    #[test]
    fn test_stats() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["id".to_string(), "name".to_string()],
            8192,
        );

        manager.add_indexed_column("id");

        let tid = manager.insert(vec![vec![1], vec![2]], 1).unwrap();

        // HOT update
        manager
            .update(tid, vec![vec![1], vec![3]], &["name"], 2)
            .unwrap();

        // Non-HOT update
        manager
            .update(tid, vec![vec![2], vec![3]], &["id"], 3)
            .unwrap();

        let stats = manager.stats();
        assert_eq!(stats.updates_attempted, 2);
        assert_eq!(stats.hot_updates, 1);
        assert_eq!(stats.non_hot_updates, 1);
        assert!(stats.hot_ratio() > 0.0);
    }

    #[test]
    fn test_page_overflow() {
        let manager = HotUpdateManager::new("test_table", vec!["id".to_string()], 1024); // Small page

        // Insert many tuples to force page creation
        for i in 0..100u8 {
            manager.insert(vec![vec![i; 50]], 1).unwrap();
        }

        assert!(manager.page_count() > 1);
    }

    #[test]
    fn test_indexed_columns() {
        let manager = HotUpdateManager::new(
            "test_table",
            vec!["a".to_string(), "b".to_string()],
            8192,
        );

        manager.add_indexed_column("a");
        manager.add_indexed_column("b");

        let indexed = manager.indexed_columns();
        assert!(indexed.contains(&"a".to_string()));
        assert!(indexed.contains(&"b".to_string()));

        manager.remove_indexed_column("a");
        let indexed = manager.indexed_columns();
        assert!(!indexed.contains(&"a".to_string()));
    }

    #[test]
    fn test_error_display() {
        let err = HotError::PageFull;
        assert_eq!(format!("{}", err), "Page is full");

        let err = HotError::IndexedColumnModified("id".to_string());
        assert!(format!("{}", err).contains("id"));

        let err = HotError::RowNotFound(42);
        assert!(format!("{}", err).contains("42"));
    }

    #[test]
    fn test_hot_update_result() {
        let hot = HotUpdateResult::Hot {
            old_page: 0,
            old_offset: 0,
            new_offset: 1,
        };
        assert!(hot.is_hot());
        assert!(!hot.requires_index_update());

        let non_hot = HotUpdateResult::NonHot {
            old_page: 0,
            old_offset: 0,
            new_page: 1,
            new_offset: 0,
            requires_index_update: true,
        };
        assert!(!non_hot.is_hot());
        assert!(non_hot.requires_index_update());
    }

    #[test]
    fn test_hot_chain_iterator() {
        let mut page = HeapPage::new(0, 8192);

        let mut t1 = HeapTuple::new(1, 0, 0, 1, vec![vec![1]]);
        t1.header.hot_next = 1;
        t1.header.status = TupleStatus::HotUpdated;
        page.add_tuple(t1).unwrap();

        let mut t2 = HeapTuple::new(1, 0, 1, 2, vec![vec![2]]);
        t2.header.is_heap_only = true;
        page.add_tuple(t2).unwrap();

        let chain: Vec<_> = HotChainIterator::new(&page, 0).collect();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].data[0], vec![1]);
        assert_eq!(chain[1].data[0], vec![2]);
    }
}
