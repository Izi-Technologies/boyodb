//! B-Tree Index Implementation for Range Queries
//!
//! This module provides a disk-based B-Tree index supporting:
//! - Point lookups (equality)
//! - Range scans (>, <, >=, <=, BETWEEN)
//! - Composite keys (multiple columns)
//! - Efficient bulk loading from sorted data

use crate::engine::EngineError;

use ordered_float::OrderedFloat; // from ordered-float crate
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Magic bytes for B-Tree index files
pub const BTREE_MAGIC: &[u8; 4] = b"BTIX";

/// Current B-Tree format version
pub const BTREE_VERSION: u32 = 1;

/// Default node size (4KB to match disk page size)
pub const DEFAULT_NODE_SIZE: usize = 4096;

/// B-Tree key types that can be indexed
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BTreeKey {
    /// Null value (sorts first)
    Null,

    /// 64-bit signed integer
    Int64(i64),

    /// 64-bit unsigned integer
    UInt64(u64),

    /// 64-bit float (using OrderedFloat for total ordering)
    Float64(OrderedFloat<f64>),

    /// UTF-8 string
    String(String),

    /// Binary data
    Bytes(Vec<u8>),

    /// Timestamp in microseconds since epoch
    Timestamp(i64),

    /// Date as days since epoch
    Date(i32),

    /// Boolean
    Boolean(bool),

    /// Composite key (multiple columns)
    Composite(Vec<BTreeKey>),
}

impl BTreeKey {
    /// Create a key from a primitive value
    pub fn from_i64(v: i64) -> Self {
        BTreeKey::Int64(v)
    }

    pub fn from_u64(v: u64) -> Self {
        BTreeKey::UInt64(v)
    }

    pub fn from_f64(v: f64) -> Self {
        BTreeKey::Float64(OrderedFloat(v))
    }

    pub fn from_string(v: String) -> Self {
        BTreeKey::String(v)
    }

    pub fn from_bytes(v: Vec<u8>) -> Self {
        BTreeKey::Bytes(v)
    }

    pub fn from_timestamp(v: i64) -> Self {
        BTreeKey::Timestamp(v)
    }

    /// Create a composite key from multiple values
    pub fn composite(keys: Vec<BTreeKey>) -> Self {
        BTreeKey::Composite(keys)
    }

    /// Estimate the serialized size of this key
    pub fn serialized_size(&self) -> usize {
        1 + match self {
            BTreeKey::Null => 0,
            BTreeKey::Int64(_) => 8,
            BTreeKey::UInt64(_) => 8,
            BTreeKey::Float64(_) => 8,
            BTreeKey::String(s) => 4 + s.len(),
            BTreeKey::Bytes(b) => 4 + b.len(),
            BTreeKey::Timestamp(_) => 8,
            BTreeKey::Date(_) => 4,
            BTreeKey::Boolean(_) => 1,
            BTreeKey::Composite(keys) => {
                4 + keys.iter().map(|k| k.serialized_size()).sum::<usize>()
            }
        }
    }
}

impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        use BTreeKey::*;

        // Null always sorts first
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            // Same type comparisons
            (Int64(a), Int64(b)) => a.cmp(b),
            (UInt64(a), UInt64(b)) => a.cmp(b),
            (Float64(a), Float64(b)) => a.cmp(b),
            (String(a), String(b)) => a.cmp(b),
            (Bytes(a), Bytes(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Composite(a), Composite(b)) => a.cmp(b),

            // Cross-type comparisons (numeric types can be compared)
            (Int64(a), UInt64(b)) => {
                if *a < 0 {
                    Ordering::Less
                } else {
                    (*a as u64).cmp(b)
                }
            }
            (UInt64(a), Int64(b)) => {
                if *b < 0 {
                    Ordering::Greater
                } else {
                    a.cmp(&(*b as u64))
                }
            }
            (Int64(a), Float64(b)) => OrderedFloat(*a as f64).cmp(b),
            (Float64(a), Int64(b)) => a.cmp(&OrderedFloat(*b as f64)),
            (UInt64(a), Float64(b)) => OrderedFloat(*a as f64).cmp(b),
            (Float64(a), UInt64(b)) => a.cmp(&OrderedFloat(*b as f64)),
            (Timestamp(a), Int64(b)) => a.cmp(b),
            (Int64(a), Timestamp(b)) => a.cmp(b),

            // Different incompatible types: compare by type tag
            _ => self.type_tag().cmp(&other.type_tag()),
        }
    }
}

impl BTreeKey {
    fn type_tag(&self) -> u8 {
        match self {
            BTreeKey::Null => 0,
            BTreeKey::Boolean(_) => 1,
            BTreeKey::Int64(_) => 2,
            BTreeKey::UInt64(_) => 3,
            BTreeKey::Float64(_) => 4,
            BTreeKey::Date(_) => 5,
            BTreeKey::Timestamp(_) => 6,
            BTreeKey::String(_) => 7,
            BTreeKey::Bytes(_) => 8,
            BTreeKey::Composite(_) => 9,
        }
    }
}

/// Type of B-Tree node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Internal node with keys and child pointers
    Internal,

    /// Leaf node with keys and row IDs
    Leaf,
}

/// A node in the B-Tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeNode {
    /// Node type
    pub node_type: NodeType,

    /// Keys in this node
    pub keys: Vec<BTreeKey>,

    /// Child node offsets (for internal nodes)
    /// children[i] points to subtree with keys < keys[i]
    /// children[keys.len()] points to subtree with keys >= keys[keys.len()-1]
    pub children: Vec<u64>,

    /// Row IDs for each key (for leaf nodes)
    /// row_ids[i] contains all row IDs for keys[i]
    pub row_ids: Vec<Vec<u32>>,

    /// Offset of next leaf node (for range scans)
    pub next_leaf: Option<u64>,

    /// Offset of previous leaf node
    pub prev_leaf: Option<u64>,
}

impl BTreeNode {
    /// Create a new internal node
    pub fn new_internal() -> Self {
        BTreeNode {
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            row_ids: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }

    /// Create a new leaf node
    pub fn new_leaf() -> Self {
        BTreeNode {
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            row_ids: Vec::new(),
            next_leaf: None,
            prev_leaf: None,
        }
    }

    /// Check if this node is a leaf
    pub fn is_leaf(&self) -> bool {
        self.node_type == NodeType::Leaf
    }

    /// Get the number of keys in this node
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Find the child index for a key in an internal node
    pub fn find_child_index(&self, key: &BTreeKey) -> usize {
        self.keys
            .iter()
            .position(|k| key < k)
            .unwrap_or(self.keys.len())
    }

    /// Find the key index in a leaf node (for exact match)
    pub fn find_key_index(&self, key: &BTreeKey) -> Option<usize> {
        self.keys.iter().position(|k| k == key)
    }

    /// Find the first key >= given key in a leaf node
    pub fn find_key_gte(&self, key: &BTreeKey) -> Option<usize> {
        self.keys.iter().position(|k| k >= key)
    }

    /// Serialize the node to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, EngineError> {
        bincode::serialize(self)
            .map_err(|e| EngineError::Internal(format!("Failed to serialize B-Tree node: {}", e)))
    }

    /// Deserialize a node from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, EngineError> {
        bincode::deserialize(data)
            .map_err(|e| EngineError::Internal(format!("Failed to deserialize B-Tree node: {}", e)))
    }
}

/// B-Tree index header (stored at the beginning of the file)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeHeader {
    /// Magic bytes
    pub magic: [u8; 4],

    /// Format version
    pub version: u32,

    /// Total number of keys in the tree
    pub key_count: u64,

    /// Offset of the root node
    pub root_offset: u64,

    /// Node size in bytes
    pub node_size: u32,

    /// Height of the tree
    pub height: u32,

    /// Column names this index covers
    pub columns: Vec<String>,

    /// Whether the index enforces uniqueness
    pub unique: bool,
}

impl BTreeHeader {
    /// Create a new header
    pub fn new(columns: Vec<String>, unique: bool) -> Self {
        BTreeHeader {
            magic: *BTREE_MAGIC,
            version: BTREE_VERSION,
            key_count: 0,
            root_offset: 0,
            node_size: DEFAULT_NODE_SIZE as u32,
            height: 0,
            columns,
            unique,
        }
    }

    /// Validate the header
    pub fn validate(&self) -> Result<(), EngineError> {
        if &self.magic != BTREE_MAGIC {
            return Err(EngineError::Internal(
                "Invalid B-Tree magic bytes".to_string(),
            ));
        }
        if self.version > BTREE_VERSION {
            return Err(EngineError::Internal(format!(
                "B-Tree version {} not supported (max {})",
                self.version, BTREE_VERSION
            )));
        }
        Ok(())
    }
}

/// B-Tree index for efficient range queries
pub struct BTree {
    /// Index file path
    path: std::path::PathBuf,

    /// Header information
    header: BTreeHeader,

    /// Cached root node
    root: Option<BTreeNode>,
}

impl BTree {
    /// Open an existing B-Tree index file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .map_err(|e| EngineError::Io(format!("Failed to open B-Tree file: {}", e)))?;
        let mut reader = BufReader::new(file);

        // Read header
        let header: BTreeHeader = {
            let mut header_size_buf = [0u8; 4];
            reader
                .read_exact(&mut header_size_buf)
                .map_err(|e| EngineError::Io(format!("Failed to read header size: {}", e)))?;
            let header_size = u32::from_le_bytes(header_size_buf) as usize;

            let mut header_buf = vec![0u8; header_size];
            reader
                .read_exact(&mut header_buf)
                .map_err(|e| EngineError::Io(format!("Failed to read header: {}", e)))?;

            bincode::deserialize(&header_buf).map_err(|e| {
                EngineError::Internal(format!("Failed to deserialize header: {}", e))
            })?
        };

        header.validate()?;

        Ok(BTree {
            path,
            header,
            root: None,
        })
    }

    /// Search for an exact key match
    pub fn search(&self, key: &BTreeKey) -> Result<Option<Vec<u32>>, EngineError> {
        if self.header.root_offset == 0 {
            return Ok(None); // Empty tree
        }

        let file = File::open(&self.path)
            .map_err(|e| EngineError::Io(format!("Failed to open B-Tree file: {}", e)))?;
        let mut reader = BufReader::new(file);

        let mut offset = self.header.root_offset;

        loop {
            let node = self.read_node(&mut reader, offset)?;

            if node.is_leaf() {
                return Ok(node.find_key_index(key).map(|i| node.row_ids[i].clone()));
            }

            // Navigate to child
            let child_idx = node.find_child_index(key);
            offset = node.children[child_idx];
        }
    }

    /// Perform a range scan
    pub fn range_scan(
        &self,
        start: Option<&BTreeKey>,
        end: Option<&BTreeKey>,
        include_start: bool,
        include_end: bool,
    ) -> Result<Vec<(BTreeKey, Vec<u32>)>, EngineError> {
        if self.header.root_offset == 0 {
            return Ok(Vec::new()); // Empty tree
        }

        let file = File::open(&self.path)
            .map_err(|e| EngineError::Io(format!("Failed to open B-Tree file: {}", e)))?;
        let mut reader = BufReader::new(file);

        let mut results = Vec::new();

        // Find the starting leaf
        let start_leaf_offset = match start {
            Some(key) => self.find_leaf(&mut reader, key)?,
            None => self.find_leftmost_leaf(&mut reader)?,
        };

        let mut current_offset = Some(start_leaf_offset);

        while let Some(offset) = current_offset {
            let node = self.read_node(&mut reader, offset)?;

            let start_idx = match start {
                Some(key) => {
                    let idx = node.find_key_gte(key).unwrap_or(node.keys.len());
                    if !include_start && idx < node.keys.len() && &node.keys[idx] == key {
                        idx + 1
                    } else {
                        idx
                    }
                }
                None => 0,
            };

            for i in start_idx..node.keys.len() {
                let k = &node.keys[i];

                // Check end bound
                if let Some(end_key) = end {
                    match k.cmp(end_key) {
                        Ordering::Greater => return Ok(results),
                        Ordering::Equal if !include_end => return Ok(results),
                        _ => {}
                    }
                }

                results.push((k.clone(), node.row_ids[i].clone()));
            }

            current_offset = node.next_leaf;
        }

        Ok(results)
    }

    /// Get all keys in order
    pub fn scan_all(&self) -> Result<Vec<(BTreeKey, Vec<u32>)>, EngineError> {
        self.range_scan(None, None, true, true)
    }

    /// Get the number of keys in the index
    pub fn key_count(&self) -> u64 {
        self.header.key_count
    }

    /// Get the columns this index covers
    pub fn columns(&self) -> &[String] {
        &self.header.columns
    }

    /// Check if this is a unique index
    pub fn is_unique(&self) -> bool {
        self.header.unique
    }

    // Helper methods

    fn read_node<R: Read + Seek>(
        &self,
        reader: &mut R,
        offset: u64,
    ) -> Result<BTreeNode, EngineError> {
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| EngineError::Io(format!("Failed to seek to node: {}", e)))?;

        // Read node size
        let mut size_buf = [0u8; 4];
        reader
            .read_exact(&mut size_buf)
            .map_err(|e| EngineError::Io(format!("Failed to read node size: {}", e)))?;
        let size = u32::from_le_bytes(size_buf) as usize;

        // Read node data
        let mut node_buf = vec![0u8; size];
        reader
            .read_exact(&mut node_buf)
            .map_err(|e| EngineError::Io(format!("Failed to read node data: {}", e)))?;

        BTreeNode::deserialize(&node_buf)
    }

    fn find_leaf<R: Read + Seek>(
        &self,
        reader: &mut R,
        key: &BTreeKey,
    ) -> Result<u64, EngineError> {
        let mut offset = self.header.root_offset;

        loop {
            let node = self.read_node(reader, offset)?;

            if node.is_leaf() {
                return Ok(offset);
            }

            let child_idx = node.find_child_index(key);
            offset = node.children[child_idx];
        }
    }

    fn find_leftmost_leaf<R: Read + Seek>(&self, reader: &mut R) -> Result<u64, EngineError> {
        let mut offset = self.header.root_offset;

        loop {
            let node = self.read_node(reader, offset)?;

            if node.is_leaf() {
                return Ok(offset);
            }

            // Always go to first child
            offset = node.children[0];
        }
    }
}

/// Builder for creating B-Tree indexes from sorted data
pub struct BTreeBuilder {
    /// Output file path
    path: std::path::PathBuf,

    /// Header
    header: BTreeHeader,

    /// Writer
    writer: BufWriter<File>,

    /// Current offset in file
    current_offset: u64,

    /// Leaf nodes being built
    leaf_nodes: Vec<(u64, BTreeNode)>, // (offset, node)

    /// Keys per leaf node (determines fanout)
    keys_per_leaf: usize,

    /// Keys per internal node
    keys_per_internal: usize,
}

impl BTreeBuilder {
    /// Create a new builder
    pub fn new<P: AsRef<Path>>(
        path: P,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<Self, EngineError> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)
            .map_err(|e| EngineError::Io(format!("Failed to create B-Tree file: {}", e)))?;
        let writer = BufWriter::new(file);

        let header = BTreeHeader::new(columns, unique);

        // Reserve space for header (will be written at the end)
        let header_reserve = 1024u64; // Reserve 1KB for header

        Ok(BTreeBuilder {
            path,
            header,
            writer,
            current_offset: header_reserve,
            leaf_nodes: Vec::new(),
            keys_per_leaf: 100,     // Adjust based on key size
            keys_per_internal: 200, // Internal nodes can hold more
        })
    }

    /// Set the number of keys per leaf node
    pub fn with_keys_per_leaf(mut self, keys: usize) -> Self {
        self.keys_per_leaf = keys;
        self
    }

    /// Set the number of keys per internal node
    pub fn with_keys_per_internal(mut self, keys: usize) -> Self {
        self.keys_per_internal = keys;
        self
    }

    /// Build the index from sorted key-rowid pairs
    pub fn build_from_sorted<I>(mut self, iter: I) -> Result<BTree, EngineError>
    where
        I: Iterator<Item = (BTreeKey, Vec<u32>)>,
    {
        // First pass: write leaf nodes
        let mut current_leaf = BTreeNode::new_leaf();
        let mut leaf_offsets = Vec::new();
        let mut key_count = 0u64;

        for (key, row_ids) in iter {
            if current_leaf.keys.len() >= self.keys_per_leaf {
                // Write current leaf and start new one
                let offset = self.write_node(&current_leaf)?;
                leaf_offsets.push((offset, current_leaf.keys[0].clone()));
                self.leaf_nodes.push((offset, current_leaf));

                current_leaf = BTreeNode::new_leaf();
            }

            current_leaf.keys.push(key);
            current_leaf.row_ids.push(row_ids);
            key_count += 1;
        }

        // Write last leaf if not empty
        if !current_leaf.keys.is_empty() {
            let offset = self.write_node(&current_leaf)?;
            leaf_offsets.push((offset, current_leaf.keys[0].clone()));
            self.leaf_nodes.push((offset, current_leaf));
        }

        // Link leaves together
        self.link_leaves()?;

        // Build internal nodes bottom-up
        let root_offset = if leaf_offsets.is_empty() {
            0
        } else if leaf_offsets.len() == 1 {
            leaf_offsets[0].0
        } else {
            self.build_internal_nodes(leaf_offsets)?
        };

        // Update and write header
        self.header.key_count = key_count;
        self.header.root_offset = root_offset;
        self.write_header()?;

        // Flush and close
        self.writer
            .flush()
            .map_err(|e| EngineError::Io(format!("Failed to flush B-Tree file: {}", e)))?;

        // Open and return the built tree
        BTree::open(&self.path)
    }

    fn write_node(&mut self, node: &BTreeNode) -> Result<u64, EngineError> {
        let offset = self.current_offset;
        let data = node.serialize()?;

        // Write size + data
        let size = data.len() as u32;
        self.writer
            .seek(SeekFrom::Start(offset))
            .map_err(|e| EngineError::Io(format!("Failed to seek: {}", e)))?;
        self.writer
            .write_all(&size.to_le_bytes())
            .map_err(|e| EngineError::Io(format!("Failed to write node size: {}", e)))?;
        self.writer
            .write_all(&data)
            .map_err(|e| EngineError::Io(format!("Failed to write node data: {}", e)))?;

        self.current_offset += 4 + data.len() as u64;

        Ok(offset)
    }

    fn link_leaves(&mut self) -> Result<(), EngineError> {
        if self.leaf_nodes.len() <= 1 {
            return Ok(());
        }

        // Update next/prev pointers and rewrite nodes
        for i in 0..self.leaf_nodes.len() {
            let mut node = self.leaf_nodes[i].1.clone();
            let offset = self.leaf_nodes[i].0;

            if i > 0 {
                node.prev_leaf = Some(self.leaf_nodes[i - 1].0);
            }
            if i < self.leaf_nodes.len() - 1 {
                node.next_leaf = Some(self.leaf_nodes[i + 1].0);
            }

            // Rewrite the node (this is inefficient but keeps the code simple)
            let data = node.serialize()?;
            self.writer
                .seek(SeekFrom::Start(offset + 4))
                .map_err(|e| EngineError::Io(format!("Failed to seek: {}", e)))?;
            self.writer
                .write_all(&data)
                .map_err(|e| EngineError::Io(format!("Failed to write linked node: {}", e)))?;
        }

        Ok(())
    }

    fn build_internal_nodes(
        &mut self,
        mut level: Vec<(u64, BTreeKey)>,
    ) -> Result<u64, EngineError> {
        self.header.height = 1;

        while level.len() > 1 {
            let mut next_level = Vec::new();
            let mut current_node = BTreeNode::new_internal();

            for (i, (child_offset, separator_key)) in level.iter().enumerate() {
                current_node.children.push(*child_offset);

                if i > 0 {
                    current_node.keys.push(separator_key.clone());
                }

                if current_node.children.len() > self.keys_per_internal {
                    // Write current node and start new one
                    let offset = self.write_node(&current_node)?;
                    next_level.push((offset, current_node.keys[0].clone()));

                    current_node = BTreeNode::new_internal();
                }
            }

            // Write last node if not empty
            if !current_node.children.is_empty() {
                let offset = self.write_node(&current_node)?;
                if !current_node.keys.is_empty() {
                    next_level.push((offset, current_node.keys[0].clone()));
                } else if next_level.is_empty() {
                    next_level.push((offset, BTreeKey::Null));
                }
            }

            level = next_level;
            self.header.height += 1;
        }

        Ok(level[0].0)
    }

    fn write_header(&mut self) -> Result<(), EngineError> {
        let header_data = bincode::serialize(&self.header)
            .map_err(|e| EngineError::Internal(format!("Failed to serialize header: {}", e)))?;

        self.writer
            .seek(SeekFrom::Start(0))
            .map_err(|e| EngineError::Io(format!("Failed to seek to start: {}", e)))?;

        let size = header_data.len() as u32;
        self.writer
            .write_all(&size.to_le_bytes())
            .map_err(|e| EngineError::Io(format!("Failed to write header size: {}", e)))?;
        self.writer
            .write_all(&header_data)
            .map_err(|e| EngineError::Io(format!("Failed to write header: {}", e)))?;

        Ok(())
    }
}

/// Helper function to build a B-Tree from sorted data
pub fn build_btree_from_sorted<P, I>(
    path: P,
    columns: Vec<String>,
    unique: bool,
    iter: I,
) -> Result<BTree, EngineError>
where
    P: AsRef<Path>,
    I: Iterator<Item = (BTreeKey, Vec<u32>)>,
{
    BTreeBuilder::new(path, columns, unique)?.build_from_sorted(iter)
}

/// Range for B-Tree queries
#[derive(Debug, Clone)]
pub struct BTreeRange {
    pub start: Option<BTreeKey>,
    pub end: Option<BTreeKey>,
    pub include_start: bool,
    pub include_end: bool,
}

impl BTreeRange {
    /// Create a range for equality (key = value)
    pub fn eq(key: BTreeKey) -> Self {
        BTreeRange {
            start: Some(key.clone()),
            end: Some(key),
            include_start: true,
            include_end: true,
        }
    }

    /// Create a range for greater than (key > value)
    pub fn gt(key: BTreeKey) -> Self {
        BTreeRange {
            start: Some(key),
            end: None,
            include_start: false,
            include_end: true,
        }
    }

    /// Create a range for greater than or equal (key >= value)
    pub fn gte(key: BTreeKey) -> Self {
        BTreeRange {
            start: Some(key),
            end: None,
            include_start: true,
            include_end: true,
        }
    }

    /// Create a range for less than (key < value)
    pub fn lt(key: BTreeKey) -> Self {
        BTreeRange {
            start: None,
            end: Some(key),
            include_start: true,
            include_end: false,
        }
    }

    /// Create a range for less than or equal (key <= value)
    pub fn lte(key: BTreeKey) -> Self {
        BTreeRange {
            start: None,
            end: Some(key),
            include_start: true,
            include_end: true,
        }
    }

    /// Create a range for BETWEEN (low <= key <= high)
    pub fn between(low: BTreeKey, high: BTreeKey) -> Self {
        BTreeRange {
            start: Some(low),
            end: Some(high),
            include_start: true,
            include_end: true,
        }
    }

    /// Create a range for all keys
    pub fn all() -> Self {
        BTreeRange {
            start: None,
            end: None,
            include_start: true,
            include_end: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_btree_key_ordering() {
        let keys = vec![
            BTreeKey::Null,
            BTreeKey::Int64(-100),
            BTreeKey::Int64(0),
            BTreeKey::Int64(100),
            BTreeKey::String("a".to_string()),
            BTreeKey::String("b".to_string()),
            BTreeKey::String("z".to_string()),
        ];

        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] < keys[i + 1],
                "{:?} should be < {:?}",
                keys[i],
                keys[i + 1]
            );
        }
    }

    #[test]
    fn test_btree_key_numeric_comparison() {
        assert!(BTreeKey::Int64(-1) < BTreeKey::UInt64(0));
        assert!(BTreeKey::Int64(100) == BTreeKey::Int64(100));
        assert!(BTreeKey::Float64(OrderedFloat(1.5)) > BTreeKey::Int64(1));
    }

    #[test]
    fn test_btree_node_serialization() {
        let mut node = BTreeNode::new_leaf();
        node.keys.push(BTreeKey::Int64(1));
        node.keys.push(BTreeKey::Int64(2));
        node.row_ids.push(vec![100, 101]);
        node.row_ids.push(vec![200]);

        let serialized = node.serialize().unwrap();
        let deserialized = BTreeNode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.keys, node.keys);
        assert_eq!(deserialized.row_ids, node.row_ids);
    }

    #[test]
    fn test_btree_build_and_search() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        // Build index
        let data: Vec<(BTreeKey, Vec<u32>)> = (0..100)
            .map(|i| (BTreeKey::Int64(i), vec![i as u32]))
            .collect();

        let btree =
            build_btree_from_sorted(&path, vec!["id".to_string()], true, data.into_iter()).unwrap();

        assert_eq!(btree.key_count(), 100);

        // Search for existing key
        let result = btree.search(&BTreeKey::Int64(50)).unwrap();
        assert_eq!(result, Some(vec![50]));

        // Search for non-existing key
        let result = btree.search(&BTreeKey::Int64(200)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_btree_range_scan() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.btree");

        // Build index
        let data: Vec<(BTreeKey, Vec<u32>)> = (0..100)
            .map(|i| (BTreeKey::Int64(i), vec![i as u32]))
            .collect();

        let btree =
            build_btree_from_sorted(&path, vec!["id".to_string()], true, data.into_iter()).unwrap();

        // Range scan [10, 20)
        let results = btree
            .range_scan(
                Some(&BTreeKey::Int64(10)),
                Some(&BTreeKey::Int64(20)),
                true,
                false,
            )
            .unwrap();

        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, BTreeKey::Int64(10));
        assert_eq!(results[9].0, BTreeKey::Int64(19));
    }

    #[test]
    fn test_btree_composite_key() {
        let key1 = BTreeKey::composite(vec![
            BTreeKey::String("user1".to_string()),
            BTreeKey::Int64(100),
        ]);
        let key2 = BTreeKey::composite(vec![
            BTreeKey::String("user1".to_string()),
            BTreeKey::Int64(200),
        ]);
        let key3 = BTreeKey::composite(vec![
            BTreeKey::String("user2".to_string()),
            BTreeKey::Int64(100),
        ]);

        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_btree_range_helpers() {
        let range = BTreeRange::eq(BTreeKey::Int64(5));
        assert_eq!(range.start, Some(BTreeKey::Int64(5)));
        assert_eq!(range.end, Some(BTreeKey::Int64(5)));
        assert!(range.include_start);
        assert!(range.include_end);

        let range = BTreeRange::gt(BTreeKey::Int64(5));
        assert_eq!(range.start, Some(BTreeKey::Int64(5)));
        assert_eq!(range.end, None);
        assert!(!range.include_start);

        let range = BTreeRange::between(BTreeKey::Int64(5), BTreeKey::Int64(10));
        assert_eq!(range.start, Some(BTreeKey::Int64(5)));
        assert_eq!(range.end, Some(BTreeKey::Int64(10)));
        assert!(range.include_start);
        assert!(range.include_end);
    }
}
