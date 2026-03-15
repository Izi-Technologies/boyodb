//! GIN and GiST Indexes
//!
//! PostgreSQL-compatible Generalized Inverted Index (GIN) and
//! Generalized Search Tree (GiST) indexes for complex data types.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use std::hash::Hash;

// ============================================================================
// GIN (Generalized Inverted Index)
// ============================================================================

/// GIN index for full-text search, arrays, and JSONB
pub struct GinIndex<K: Eq + Hash + Clone> {
    /// Index name
    name: String,
    /// Inverted index: key -> set of row IDs
    entries: RwLock<HashMap<K, HashSet<u64>>>,
    /// Pending entries (for fast updates)
    pending: RwLock<HashMap<K, HashSet<u64>>>,
    /// Configuration
    config: GinConfig,
    /// Statistics
    stats: RwLock<GinStats>,
}

/// GIN configuration
#[derive(Debug, Clone)]
pub struct GinConfig {
    /// Enable fast update (pending list)
    pub fast_update: bool,
    /// Pending list size limit
    pub pending_list_limit: usize,
    /// Enable compression
    pub compression: bool,
}

impl Default for GinConfig {
    fn default() -> Self {
        Self {
            fast_update: true,
            pending_list_limit: 4096,
            compression: false,
        }
    }
}

/// GIN statistics
#[derive(Debug, Clone, Default)]
pub struct GinStats {
    /// Total entries
    pub total_entries: u64,
    /// Total keys
    pub total_keys: u64,
    /// Pending entries
    pub pending_entries: u64,
    /// Lookups performed
    pub lookups: u64,
    /// Inserts performed
    pub inserts: u64,
}

impl<K: Eq + Hash + Clone> GinIndex<K> {
    pub fn new(name: String, config: GinConfig) -> Self {
        Self {
            name,
            entries: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new()),
            config,
            stats: RwLock::new(GinStats::default()),
        }
    }

    /// Insert entry into GIN index
    pub fn insert(&self, keys: &[K], row_id: u64) {
        let mut stats = self.stats.write();
        stats.inserts += 1;

        if self.config.fast_update {
            let mut pending = self.pending.write();
            for key in keys {
                pending.entry(key.clone())
                    .or_insert_with(HashSet::new)
                    .insert(row_id);
                stats.pending_entries += 1;
            }

            // Merge if pending list is too large
            if stats.pending_entries >= self.config.pending_list_limit as u64 {
                drop(pending);
                drop(stats);
                self.merge_pending();
            }
        } else {
            let mut entries = self.entries.write();
            for key in keys {
                entries.entry(key.clone())
                    .or_insert_with(HashSet::new)
                    .insert(row_id);
                stats.total_entries += 1;
            }
            stats.total_keys = entries.len() as u64;
        }
    }

    /// Delete entry from GIN index
    pub fn delete(&self, keys: &[K], row_id: u64) {
        let mut entries = self.entries.write();
        for key in keys {
            if let Some(set) = entries.get_mut(key) {
                set.remove(&row_id);
                if set.is_empty() {
                    entries.remove(key);
                }
            }
        }

        // Also check pending
        let mut pending = self.pending.write();
        for key in keys {
            if let Some(set) = pending.get_mut(key) {
                set.remove(&row_id);
            }
        }
    }

    /// Look up a single key
    pub fn lookup(&self, key: &K) -> HashSet<u64> {
        let mut stats = self.stats.write();
        stats.lookups += 1;
        drop(stats);

        let mut result = HashSet::new();

        // Check main entries
        if let Some(set) = self.entries.read().get(key) {
            result.extend(set);
        }

        // Check pending
        if let Some(set) = self.pending.read().get(key) {
            result.extend(set);
        }

        result
    }

    /// Look up multiple keys (OR)
    pub fn lookup_any(&self, keys: &[K]) -> HashSet<u64> {
        let mut result = HashSet::new();
        for key in keys {
            result.extend(self.lookup(key));
        }
        result
    }

    /// Look up multiple keys (AND)
    pub fn lookup_all(&self, keys: &[K]) -> HashSet<u64> {
        if keys.is_empty() {
            return HashSet::new();
        }

        let mut result = self.lookup(&keys[0]);
        for key in keys.iter().skip(1) {
            let set = self.lookup(key);
            result = result.intersection(&set).copied().collect();
        }
        result
    }

    /// Merge pending entries into main index
    pub fn merge_pending(&self) {
        let mut pending = self.pending.write();
        let mut entries = self.entries.write();
        let mut stats = self.stats.write();

        for (key, row_ids) in pending.drain() {
            let entry = entries.entry(key).or_insert_with(HashSet::new);
            let count = row_ids.len() as u64;
            entry.extend(row_ids);
            stats.total_entries += count;
        }

        stats.pending_entries = 0;
        stats.total_keys = entries.len() as u64;
    }

    /// Get statistics
    pub fn stats(&self) -> GinStats {
        self.stats.read().clone()
    }

    /// Get index name
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// GiST (Generalized Search Tree)
// ============================================================================

/// GiST node
#[derive(Debug, Clone)]
pub struct GistNode<K: GistKey> {
    /// Node ID
    pub id: u64,
    /// Is leaf node
    pub is_leaf: bool,
    /// Entries
    pub entries: Vec<GistEntry<K>>,
    /// Parent node ID
    pub parent: Option<u64>,
}

/// GiST entry
#[derive(Debug, Clone)]
pub struct GistEntry<K: GistKey> {
    /// Key (bounding box, range, etc.)
    pub key: K,
    /// Child node ID (for internal nodes)
    pub child: Option<u64>,
    /// Row ID (for leaf nodes)
    pub row_id: Option<u64>,
}

/// GiST key trait
pub trait GistKey: Clone + Send + Sync {
    /// Check if keys are consistent (for search)
    fn consistent(&self, query: &Self) -> bool;
    /// Union of two keys
    fn union(&self, other: &Self) -> Self;
    /// Penalty for inserting into this subtree
    fn penalty(&self, new_key: &Self) -> f64;
    /// Pick split point
    fn pick_split(entries: &[Self]) -> (Vec<usize>, Vec<usize>);
    /// Compress key for storage
    fn compress(&self) -> Vec<u8>;
    /// Decompress key from storage
    fn decompress(data: &[u8]) -> Option<Self>;
}

/// Box key (2D rectangle) for spatial indexing
#[derive(Debug, Clone, Copy)]
pub struct BoxKey {
    pub xmin: f64,
    pub ymin: f64,
    pub xmax: f64,
    pub ymax: f64,
}

impl BoxKey {
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Self {
        Self { xmin, ymin, xmax, ymax }
    }

    pub fn point(x: f64, y: f64) -> Self {
        Self { xmin: x, ymin: y, xmax: x, ymax: y }
    }

    pub fn area(&self) -> f64 {
        (self.xmax - self.xmin) * (self.ymax - self.ymin)
    }

    pub fn intersects(&self, other: &Self) -> bool {
        self.xmin <= other.xmax && self.xmax >= other.xmin &&
        self.ymin <= other.ymax && self.ymax >= other.ymin
    }

    pub fn contains(&self, other: &Self) -> bool {
        self.xmin <= other.xmin && self.xmax >= other.xmax &&
        self.ymin <= other.ymin && self.ymax >= other.ymax
    }
}

impl GistKey for BoxKey {
    fn consistent(&self, query: &Self) -> bool {
        self.intersects(query)
    }

    fn union(&self, other: &Self) -> Self {
        BoxKey {
            xmin: self.xmin.min(other.xmin),
            ymin: self.ymin.min(other.ymin),
            xmax: self.xmax.max(other.xmax),
            ymax: self.ymax.max(other.ymax),
        }
    }

    fn penalty(&self, new_key: &Self) -> f64 {
        let union = self.union(new_key);
        union.area() - self.area()
    }

    fn pick_split(entries: &[Self]) -> (Vec<usize>, Vec<usize>) {
        // Simple split: divide by x coordinate
        let mut indices: Vec<usize> = (0..entries.len()).collect();
        indices.sort_by(|&a, &b| {
            let ca = (entries[a].xmin + entries[a].xmax) / 2.0;
            let cb = (entries[b].xmin + entries[b].xmax) / 2.0;
            ca.partial_cmp(&cb).unwrap()
        });

        let mid = indices.len() / 2;
        (indices[..mid].to_vec(), indices[mid..].to_vec())
    }

    fn compress(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&self.xmin.to_le_bytes());
        bytes.extend_from_slice(&self.ymin.to_le_bytes());
        bytes.extend_from_slice(&self.xmax.to_le_bytes());
        bytes.extend_from_slice(&self.ymax.to_le_bytes());
        bytes
    }

    fn decompress(data: &[u8]) -> Option<Self> {
        if data.len() < 32 {
            return None;
        }
        Some(BoxKey {
            xmin: f64::from_le_bytes(data[0..8].try_into().ok()?),
            ymin: f64::from_le_bytes(data[8..16].try_into().ok()?),
            xmax: f64::from_le_bytes(data[16..24].try_into().ok()?),
            ymax: f64::from_le_bytes(data[24..32].try_into().ok()?),
        })
    }
}

/// Range key for range types
#[derive(Debug, Clone, Copy)]
pub struct RangeKey {
    pub lower: i64,
    pub upper: i64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}

impl RangeKey {
    pub fn new(lower: i64, upper: i64) -> Self {
        Self {
            lower,
            upper,
            lower_inclusive: true,
            upper_inclusive: false,
        }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.lower < other.upper && other.lower < self.upper
    }
}

impl GistKey for RangeKey {
    fn consistent(&self, query: &Self) -> bool {
        self.overlaps(query)
    }

    fn union(&self, other: &Self) -> Self {
        RangeKey {
            lower: self.lower.min(other.lower),
            upper: self.upper.max(other.upper),
            lower_inclusive: true,
            upper_inclusive: false,
        }
    }

    fn penalty(&self, new_key: &Self) -> f64 {
        let union = self.union(new_key);
        (union.upper - union.lower) as f64 - (self.upper - self.lower) as f64
    }

    fn pick_split(entries: &[Self]) -> (Vec<usize>, Vec<usize>) {
        let mut indices: Vec<usize> = (0..entries.len()).collect();
        indices.sort_by_key(|&i| entries[i].lower);

        let mid = indices.len() / 2;
        (indices[..mid].to_vec(), indices[mid..].to_vec())
    }

    fn compress(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(18);
        bytes.extend_from_slice(&self.lower.to_le_bytes());
        bytes.extend_from_slice(&self.upper.to_le_bytes());
        bytes.push(if self.lower_inclusive { 1 } else { 0 });
        bytes.push(if self.upper_inclusive { 1 } else { 0 });
        bytes
    }

    fn decompress(data: &[u8]) -> Option<Self> {
        if data.len() < 18 {
            return None;
        }
        Some(RangeKey {
            lower: i64::from_le_bytes(data[0..8].try_into().ok()?),
            upper: i64::from_le_bytes(data[8..16].try_into().ok()?),
            lower_inclusive: data[16] != 0,
            upper_inclusive: data[17] != 0,
        })
    }
}

/// GiST index
pub struct GistIndex<K: GistKey> {
    /// Index name
    name: String,
    /// Root node ID
    root: RwLock<Option<u64>>,
    /// Nodes
    nodes: RwLock<HashMap<u64, GistNode<K>>>,
    /// Next node ID
    next_id: RwLock<u64>,
    /// Configuration
    config: GistConfig,
    /// Statistics
    stats: RwLock<GistStats>,
}

/// GiST configuration
#[derive(Debug, Clone)]
pub struct GistConfig {
    /// Maximum entries per node
    pub max_entries: usize,
    /// Minimum entries per node
    pub min_entries: usize,
    /// Enable buffering
    pub buffering: bool,
}

impl Default for GistConfig {
    fn default() -> Self {
        Self {
            max_entries: 128,
            min_entries: 32,
            buffering: true,
        }
    }
}

/// GiST statistics
#[derive(Debug, Clone, Default)]
pub struct GistStats {
    /// Total nodes
    pub total_nodes: u64,
    /// Total entries
    pub total_entries: u64,
    /// Tree height
    pub height: u32,
    /// Lookups performed
    pub lookups: u64,
    /// Inserts performed
    pub inserts: u64,
    /// Splits performed
    pub splits: u64,
}

impl<K: GistKey + 'static> GistIndex<K> {
    pub fn new(name: String, config: GistConfig) -> Self {
        Self {
            name,
            root: RwLock::new(None),
            nodes: RwLock::new(HashMap::new()),
            next_id: RwLock::new(1),
            config,
            stats: RwLock::new(GistStats::default()),
        }
    }

    /// Insert entry into GiST index
    pub fn insert(&self, key: K, row_id: u64) {
        let mut stats = self.stats.write();
        stats.inserts += 1;
        drop(stats);

        let root = *self.root.read();

        if root.is_none() {
            // Create root node
            let id = self.allocate_node();
            let node = GistNode {
                id,
                is_leaf: true,
                entries: vec![GistEntry {
                    key,
                    child: None,
                    row_id: Some(row_id),
                }],
                parent: None,
            };
            self.nodes.write().insert(id, node);
            *self.root.write() = Some(id);

            let mut stats = self.stats.write();
            stats.total_nodes = 1;
            stats.total_entries = 1;
            stats.height = 1;
            return;
        }

        let root_id = root.unwrap();
        self.insert_recursive(root_id, key, row_id);
    }

    fn insert_recursive(&self, node_id: u64, key: K, row_id: u64) {
        let is_leaf = {
            let nodes = self.nodes.read();
            nodes.get(&node_id).map(|n| n.is_leaf).unwrap_or(true)
        };

        if is_leaf {
            // Insert into leaf
            let mut nodes = self.nodes.write();
            if let Some(node) = nodes.get_mut(&node_id) {
                node.entries.push(GistEntry {
                    key,
                    child: None,
                    row_id: Some(row_id),
                });

                // Check if split needed
                if node.entries.len() > self.config.max_entries {
                    drop(nodes);
                    self.split_node(node_id);
                }
            }
        } else {
            // Find best child
            let child_id = {
                let nodes = self.nodes.read();
                let node = nodes.get(&node_id).unwrap();

                let mut best_idx = 0;
                let mut best_penalty = f64::MAX;

                for (i, entry) in node.entries.iter().enumerate() {
                    let penalty = entry.key.penalty(&key);
                    if penalty < best_penalty {
                        best_penalty = penalty;
                        best_idx = i;
                    }
                }

                node.entries[best_idx].child.unwrap()
            };

            self.insert_recursive(child_id, key, row_id);
        }
    }

    fn split_node(&self, node_id: u64) {
        let mut stats = self.stats.write();
        stats.splits += 1;
        drop(stats);

        // Get entries to split
        let (entries, parent, is_leaf) = {
            let nodes = self.nodes.read();
            let node = nodes.get(&node_id).unwrap();
            (node.entries.clone(), node.parent, node.is_leaf)
        };

        let keys: Vec<K> = entries.iter().map(|e| e.key.clone()).collect();
        let (left_indices, right_indices) = K::pick_split(&keys);

        // Create new node for right half
        let new_id = self.allocate_node();
        let right_entries: Vec<GistEntry<K>> = right_indices.iter()
            .map(|&i| entries[i].clone())
            .collect();

        let left_entries: Vec<GistEntry<K>> = left_indices.iter()
            .map(|&i| entries[i].clone())
            .collect();

        // Update nodes
        let mut nodes = self.nodes.write();

        nodes.insert(new_id, GistNode {
            id: new_id,
            is_leaf,
            entries: right_entries,
            parent,
        });

        if let Some(node) = nodes.get_mut(&node_id) {
            node.entries = left_entries;
        }

        // Update parent or create new root
        if let Some(parent_id) = parent {
            // Add new child to parent
            let left_key = {
                let node = nodes.get(&node_id).unwrap();
                node.entries.iter().map(|e| e.key.clone()).reduce(|a, b| a.union(&b)).unwrap()
            };
            let right_key = {
                let node = nodes.get(&new_id).unwrap();
                node.entries.iter().map(|e| e.key.clone()).reduce(|a, b| a.union(&b)).unwrap()
            };

            if let Some(parent) = nodes.get_mut(&parent_id) {
                // Update existing entry key
                if let Some(entry) = parent.entries.iter_mut().find(|e| e.child == Some(node_id)) {
                    entry.key = left_key;
                }
                // Add new entry
                parent.entries.push(GistEntry {
                    key: right_key,
                    child: Some(new_id),
                    row_id: None,
                });
            }
        } else {
            // Create new root
            let new_root_id = self.allocate_node();

            let left_key = {
                let node = nodes.get(&node_id).unwrap();
                node.entries.iter().map(|e| e.key.clone()).reduce(|a, b| a.union(&b)).unwrap()
            };
            let right_key = {
                let node = nodes.get(&new_id).unwrap();
                node.entries.iter().map(|e| e.key.clone()).reduce(|a, b| a.union(&b)).unwrap()
            };

            nodes.insert(new_root_id, GistNode {
                id: new_root_id,
                is_leaf: false,
                entries: vec![
                    GistEntry { key: left_key, child: Some(node_id), row_id: None },
                    GistEntry { key: right_key, child: Some(new_id), row_id: None },
                ],
                parent: None,
            });

            // Update children's parent
            if let Some(node) = nodes.get_mut(&node_id) {
                node.parent = Some(new_root_id);
            }
            if let Some(node) = nodes.get_mut(&new_id) {
                node.parent = Some(new_root_id);
            }

            drop(nodes);
            *self.root.write() = Some(new_root_id);

            let mut stats = self.stats.write();
            stats.height += 1;
        }
    }

    /// Search GiST index
    pub fn search(&self, query: &K) -> Vec<u64> {
        let mut stats = self.stats.write();
        stats.lookups += 1;
        drop(stats);

        let root = *self.root.read();
        if root.is_none() {
            return Vec::new();
        }

        let mut results = Vec::new();
        self.search_recursive(root.unwrap(), query, &mut results);
        results
    }

    fn search_recursive(&self, node_id: u64, query: &K, results: &mut Vec<u64>) {
        let nodes = self.nodes.read();
        let node = match nodes.get(&node_id) {
            Some(n) => n,
            None => return,
        };

        for entry in &node.entries {
            if entry.key.consistent(query) {
                if node.is_leaf {
                    if let Some(row_id) = entry.row_id {
                        results.push(row_id);
                    }
                } else if let Some(child) = entry.child {
                    drop(nodes);
                    self.search_recursive(child, query, results);
                    return; // Need to re-acquire lock
                }
            }
        }
    }

    fn allocate_node(&self) -> u64 {
        let mut next = self.next_id.write();
        let id = *next;
        *next += 1;

        let mut stats = self.stats.write();
        stats.total_nodes += 1;

        id
    }

    /// Get statistics
    pub fn stats(&self) -> GistStats {
        self.stats.read().clone()
    }

    /// Get index name
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// Index Registry
// ============================================================================

/// Index type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvancedIndexType {
    GIN,
    GiST,
    SPGiST,
    BRIN,
}

/// Index metadata
#[derive(Debug, Clone)]
pub struct AdvancedIndexMeta {
    /// Index name
    pub name: String,
    /// Table name
    pub table: String,
    /// Column name
    pub column: String,
    /// Index type
    pub index_type: AdvancedIndexType,
    /// Size in bytes
    pub size_bytes: u64,
    /// Entry count
    pub entries: u64,
}

/// Index registry
pub struct AdvancedIndexRegistry {
    /// Index metadata
    indexes: RwLock<HashMap<String, AdvancedIndexMeta>>,
}

impl AdvancedIndexRegistry {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Register an index
    pub fn register(&self, meta: AdvancedIndexMeta) {
        self.indexes.write().insert(meta.name.clone(), meta);
    }

    /// Get index metadata
    pub fn get(&self, name: &str) -> Option<AdvancedIndexMeta> {
        self.indexes.read().get(name).cloned()
    }

    /// List indexes for table
    pub fn list_for_table(&self, table: &str) -> Vec<AdvancedIndexMeta> {
        self.indexes.read()
            .values()
            .filter(|m| m.table == table)
            .cloned()
            .collect()
    }

    /// Drop index
    pub fn drop(&self, name: &str) -> bool {
        self.indexes.write().remove(name).is_some()
    }
}

impl Default for AdvancedIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gin_basic() {
        let index: GinIndex<String> = GinIndex::new("test_gin".into(), GinConfig::default());

        // Insert some entries
        index.insert(&["apple".into(), "banana".into()], 1);
        index.insert(&["banana".into(), "cherry".into()], 2);
        index.insert(&["apple".into(), "cherry".into()], 3);

        // Merge pending
        index.merge_pending();

        // Lookup
        let results = index.lookup(&"apple".into());
        assert!(results.contains(&1));
        assert!(results.contains(&3));
        assert!(!results.contains(&2));

        let results = index.lookup(&"banana".into());
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_gin_lookup_any() {
        let index: GinIndex<String> = GinIndex::new("test".into(), GinConfig::default());

        index.insert(&["a".into()], 1);
        index.insert(&["b".into()], 2);
        index.insert(&["c".into()], 3);
        index.merge_pending();

        let results = index.lookup_any(&["a".into(), "b".into()]);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_gin_lookup_all() {
        let index: GinIndex<String> = GinIndex::new("test".into(), GinConfig::default());

        index.insert(&["a".into(), "b".into()], 1);
        index.insert(&["b".into(), "c".into()], 2);
        index.insert(&["a".into(), "b".into(), "c".into()], 3);
        index.merge_pending();

        let results = index.lookup_all(&["a".into(), "b".into()]);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&3));
    }

    #[test]
    fn test_gist_box_basic() {
        let index: GistIndex<BoxKey> = GistIndex::new("test_gist".into(), GistConfig::default());

        // Insert some boxes
        index.insert(BoxKey::new(0.0, 0.0, 10.0, 10.0), 1);
        index.insert(BoxKey::new(5.0, 5.0, 15.0, 15.0), 2);
        index.insert(BoxKey::new(20.0, 20.0, 30.0, 30.0), 3);

        // Search for overlapping boxes
        let query = BoxKey::new(8.0, 8.0, 12.0, 12.0);
        let results = index.search(&query);

        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(!results.contains(&3));
    }

    #[test]
    fn test_gist_range() {
        let index: GistIndex<RangeKey> = GistIndex::new("test_range".into(), GistConfig::default());

        index.insert(RangeKey::new(0, 10), 1);
        index.insert(RangeKey::new(5, 15), 2);
        index.insert(RangeKey::new(20, 30), 3);

        let query = RangeKey::new(8, 12);
        let results = index.search(&query);

        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(!results.contains(&3));
    }

    #[test]
    fn test_box_key_operations() {
        let b1 = BoxKey::new(0.0, 0.0, 10.0, 10.0);
        let b2 = BoxKey::new(5.0, 5.0, 15.0, 15.0);
        let b3 = BoxKey::new(20.0, 20.0, 30.0, 30.0);

        assert!(b1.intersects(&b2));
        assert!(!b1.intersects(&b3));

        let union = b1.union(&b2);
        assert_eq!(union.xmin, 0.0);
        assert_eq!(union.ymin, 0.0);
        assert_eq!(union.xmax, 15.0);
        assert_eq!(union.ymax, 15.0);
    }

    #[test]
    fn test_gin_delete() {
        let index: GinIndex<String> = GinIndex::new("test".into(), GinConfig::default());

        index.insert(&["a".into(), "b".into()], 1);
        index.merge_pending();

        let results = index.lookup(&"a".into());
        assert!(results.contains(&1));

        index.delete(&["a".into(), "b".into()], 1);

        let results = index.lookup(&"a".into());
        assert!(!results.contains(&1));
    }

    #[test]
    fn test_index_registry() {
        let registry = AdvancedIndexRegistry::new();

        registry.register(AdvancedIndexMeta {
            name: "idx_content".into(),
            table: "documents".into(),
            column: "content".into(),
            index_type: AdvancedIndexType::GIN,
            size_bytes: 1024,
            entries: 100,
        });

        let meta = registry.get("idx_content").unwrap();
        assert_eq!(meta.index_type, AdvancedIndexType::GIN);

        let indexes = registry.list_for_table("documents");
        assert_eq!(indexes.len(), 1);
    }
}
