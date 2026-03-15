//! HNSW (Hierarchical Navigable Small World) Index for Approximate Nearest Neighbor Search
//!
//! This module implements the HNSW algorithm for efficient similarity search on high-dimensional vectors.
//! HNSW provides logarithmic time complexity for search operations while maintaining high recall.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use rand::Rng;

/// HNSW index configuration
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Maximum number of connections per node at each layer (M)
    pub m: usize,
    /// Maximum connections at layer 0 (M0 = 2 * M)
    pub m0: usize,
    /// Size of dynamic candidate list during construction (ef_construction)
    pub ef_construction: usize,
    /// Size of dynamic candidate list during search (ef_search)
    pub ef_search: usize,
    /// Level generation factor (ml = 1 / ln(M))
    pub ml: f64,
    /// Distance metric
    pub metric: DistanceMetric,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            m,
            m0: m * 2,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (m as f64).ln(),
            metric: DistanceMetric::Cosine,
        }
    }
}

/// Distance metrics for vector similarity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance
    Euclidean,
    /// Cosine similarity (converted to distance)
    Cosine,
    /// Dot product (for normalized vectors)
    DotProduct,
    /// Manhattan (L1) distance
    Manhattan,
}

impl DistanceMetric {
    /// Calculate distance between two vectors
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::Euclidean => {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::Cosine => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    1.0
                } else {
                    1.0 - (dot / (norm_a * norm_b))
                }
            }
            DistanceMetric::DotProduct => {
                // Negative dot product (so lower is better)
                -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
            DistanceMetric::Manhattan => {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).abs())
                    .sum()
            }
        }
    }
}

/// A node in the HNSW graph
#[derive(Debug, Clone)]
struct HnswNode {
    /// Node ID (maps to document/row ID)
    id: u64,
    /// Vector data
    vector: Vec<f32>,
    /// Connections at each layer (layer -> list of neighbor IDs)
    neighbors: Vec<Vec<u64>>,
    /// Maximum layer this node exists on
    max_layer: usize,
}

/// Candidate for search with distance
#[derive(Clone)]
struct Candidate {
    id: u64,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap behavior
        other.distance.partial_cmp(&self.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// HNSW Index implementation
pub struct HnswIndex {
    config: HnswConfig,
    nodes: HashMap<u64, HnswNode>,
    entry_point: Option<u64>,
    max_layer: usize,
    dimension: usize,
}

impl HnswIndex {
    /// Create a new HNSW index
    pub fn new(config: HnswConfig, dimension: usize) -> Self {
        Self {
            config,
            nodes: HashMap::new(),
            entry_point: None,
            max_layer: 0,
            dimension,
        }
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, id: u64, vector: Vec<f32>) {
        assert_eq!(vector.len(), self.dimension, "Vector dimension mismatch");

        // Generate random layer for this node
        let node_layer = self.random_layer();

        // Create the new node
        let mut new_node = HnswNode {
            id,
            vector: vector.clone(),
            neighbors: vec![Vec::new(); node_layer + 1],
            max_layer: node_layer,
        };

        // If this is the first node, set it as entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_layer = node_layer;
            self.nodes.insert(id, new_node);
            return;
        }

        let entry_id = self.entry_point.unwrap();
        let mut current_id = entry_id;

        // Navigate from top layer down to node's max layer
        for layer in (node_layer + 1..=self.max_layer).rev() {
            current_id = self.greedy_search_layer(&vector, current_id, layer);
        }

        // Insert at each layer from node's max layer down to 0
        for layer in (0..=node_layer.min(self.max_layer)).rev() {
            // Find ef_construction nearest neighbors at this layer
            let neighbors = self.search_layer(&vector, current_id, self.config.ef_construction, layer);

            // Select M best neighbors
            let m = if layer == 0 { self.config.m0 } else { self.config.m };
            let selected: Vec<u64> = neighbors.iter().take(m).map(|c| c.id).collect();

            // Add bidirectional connections
            new_node.neighbors[layer] = selected.clone();

            for &neighbor_id in &selected {
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    if neighbor.neighbors.len() > layer {
                        neighbor.neighbors[layer].push(id);

                        // Prune if too many connections
                        let max_conn = if layer == 0 { self.config.m0 } else { self.config.m };
                        if neighbor.neighbors[layer].len() > max_conn {
                            self.prune_connections(neighbor_id, layer, max_conn, &vector);
                        }
                    }
                }
            }

            if !neighbors.is_empty() {
                current_id = neighbors[0].id;
            }
        }

        // Update entry point if new node is at higher layer
        if node_layer > self.max_layer {
            self.entry_point = Some(id);
            self.max_layer = node_layer;
        }

        self.nodes.insert(id, new_node);
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, id: u64) -> bool {
        let node = match self.nodes.remove(&id) {
            Some(n) => n,
            None => return false,
        };

        // Remove connections from neighbors
        for (layer, neighbors) in node.neighbors.iter().enumerate() {
            for &neighbor_id in neighbors {
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    if neighbor.neighbors.len() > layer {
                        neighbor.neighbors[layer].retain(|&n| n != id);
                    }
                }
            }
        }

        // Update entry point if necessary
        if self.entry_point == Some(id) {
            self.entry_point = self.nodes.keys().next().copied();
            self.max_layer = self.nodes.values()
                .map(|n| n.max_layer)
                .max()
                .unwrap_or(0);
        }

        true
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        if self.nodes.is_empty() || query.len() != self.dimension {
            return Vec::new();
        }

        let entry_id = self.entry_point.unwrap();
        let mut current_id = entry_id;

        // Navigate from top layer down to layer 1
        for layer in (1..=self.max_layer).rev() {
            current_id = self.greedy_search_layer(query, current_id, layer);
        }

        // Search at layer 0 with ef_search candidates
        let candidates = self.search_layer(query, current_id, self.config.ef_search, 0);

        // Return top k results
        candidates
            .into_iter()
            .take(k)
            .map(|c| (c.id, c.distance))
            .collect()
    }

    /// Search for all neighbors within a distance threshold
    pub fn range_search(&self, query: &[f32], max_distance: f32) -> Vec<(u64, f32)> {
        if self.nodes.is_empty() || query.len() != self.dimension {
            return Vec::new();
        }

        // Use a larger ef for range search to ensure good coverage
        let ef = self.config.ef_search * 2;
        let entry_id = self.entry_point.unwrap();
        let mut current_id = entry_id;

        for layer in (1..=self.max_layer).rev() {
            current_id = self.greedy_search_layer(query, current_id, layer);
        }

        let candidates = self.search_layer(query, current_id, ef, 0);

        candidates
            .into_iter()
            .filter(|c| c.distance <= max_distance)
            .map(|c| (c.id, c.distance))
            .collect()
    }

    /// Generate random layer based on exponential distribution
    fn random_layer(&self) -> usize {
        let mut rng = rand::thread_rng();
        let f: f64 = rng.gen();
        ((-f.ln() * self.config.ml).floor() as usize).min(32)
    }

    /// Greedy search to find nearest node at a layer
    fn greedy_search_layer(&self, query: &[f32], start_id: u64, layer: usize) -> u64 {
        let mut current_id = start_id;
        let mut current_dist = self.distance(query, current_id);

        loop {
            let mut changed = false;

            if let Some(node) = self.nodes.get(&current_id) {
                if layer < node.neighbors.len() {
                    for &neighbor_id in &node.neighbors[layer] {
                        let dist = self.distance(query, neighbor_id);
                        if dist < current_dist {
                            current_dist = dist;
                            current_id = neighbor_id;
                            changed = true;
                        }
                    }
                }
            }

            if !changed {
                break;
            }
        }

        current_id
    }

    /// Search layer with ef candidates
    fn search_layer(&self, query: &[f32], start_id: u64, ef: usize, layer: usize) -> Vec<Candidate> {
        let start_dist = self.distance(query, start_id);

        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();
        let mut visited = HashSet::new();

        candidates.push(Candidate { id: start_id, distance: start_dist });
        results.push(Candidate { id: start_id, distance: -start_dist }); // Max-heap for results
        visited.insert(start_id);

        while let Some(current) = candidates.pop() {
            // Check if we can stop early
            if let Some(furthest) = results.peek() {
                if current.distance > -furthest.distance && results.len() >= ef {
                    break;
                }
            }

            // Explore neighbors
            if let Some(node) = self.nodes.get(&current.id) {
                if layer < node.neighbors.len() {
                    for &neighbor_id in &node.neighbors[layer] {
                        if visited.insert(neighbor_id) {
                            let dist = self.distance(query, neighbor_id);

                            let should_add = results.len() < ef || {
                                if let Some(furthest) = results.peek() {
                                    dist < -furthest.distance
                                } else {
                                    true
                                }
                            };

                            if should_add {
                                candidates.push(Candidate { id: neighbor_id, distance: dist });
                                results.push(Candidate { id: neighbor_id, distance: -dist });

                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert to sorted vec
        let mut result_vec: Vec<Candidate> = results
            .into_iter()
            .map(|c| Candidate { id: c.id, distance: -c.distance })
            .collect();
        result_vec.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        result_vec
    }

    /// Prune connections to keep only the best ones
    fn prune_connections(&mut self, node_id: u64, layer: usize, max_conn: usize, _reference: &[f32]) {
        if let Some(node) = self.nodes.get(&node_id) {
            let vector = node.vector.clone();
            let neighbors: Vec<u64> = node.neighbors.get(layer).cloned().unwrap_or_default();

            // Calculate distances to all neighbors
            let mut candidates: Vec<Candidate> = neighbors
                .iter()
                .filter_map(|&n_id| {
                    self.nodes.get(&n_id).map(|n| Candidate {
                        id: n_id,
                        distance: self.config.metric.distance(&vector, &n.vector),
                    })
                })
                .collect();

            candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());

            if let Some(node) = self.nodes.get_mut(&node_id) {
                if layer < node.neighbors.len() {
                    node.neighbors[layer] = candidates.iter().take(max_conn).map(|c| c.id).collect();
                }
            }
        }
    }

    /// Calculate distance between query and a node
    fn distance(&self, query: &[f32], node_id: u64) -> f32 {
        self.nodes
            .get(&node_id)
            .map(|n| self.config.metric.distance(query, &n.vector))
            .unwrap_or(f32::MAX)
    }

    /// Get statistics about the index
    pub fn stats(&self) -> HnswStats {
        let mut layer_sizes = vec![0usize; self.max_layer + 1];
        let mut total_connections = 0usize;

        for node in self.nodes.values() {
            for (layer, neighbors) in node.neighbors.iter().enumerate() {
                if layer < layer_sizes.len() {
                    layer_sizes[layer] += 1;
                }
                total_connections += neighbors.len();
            }
        }

        HnswStats {
            node_count: self.nodes.len(),
            max_layer: self.max_layer,
            layer_sizes,
            total_connections,
            avg_connections: if self.nodes.is_empty() {
                0.0
            } else {
                total_connections as f64 / self.nodes.len() as f64
            },
            dimension: self.dimension,
        }
    }
}

/// Statistics about an HNSW index
#[derive(Debug, Clone)]
pub struct HnswStats {
    pub node_count: usize,
    pub max_layer: usize,
    pub layer_sizes: Vec<usize>,
    pub total_connections: usize,
    pub avg_connections: f64,
    pub dimension: usize,
}

/// Thread-safe HNSW index manager
pub struct HnswManager {
    indexes: RwLock<HashMap<(String, String, String), Arc<RwLock<HnswIndex>>>>,
}

impl HnswManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create or get an HNSW index for a column
    pub fn get_or_create_index(
        &self,
        database: &str,
        table: &str,
        column: &str,
        config: HnswConfig,
        dimension: usize,
    ) -> Arc<RwLock<HnswIndex>> {
        let key = (database.to_string(), table.to_string(), column.to_string());

        {
            let indexes = self.indexes.read();
            if let Some(idx) = indexes.get(&key) {
                return idx.clone();
            }
        }

        let mut indexes = self.indexes.write();
        indexes
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(HnswIndex::new(config, dimension))))
            .clone()
    }

    /// Drop an HNSW index
    pub fn drop_index(&self, database: &str, table: &str, column: &str) -> bool {
        let key = (database.to_string(), table.to_string(), column.to_string());
        let mut indexes = self.indexes.write();
        indexes.remove(&key).is_some()
    }
}

impl Default for HnswManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_insert_and_search() {
        let config = HnswConfig::default();
        let mut index = HnswIndex::new(config, 3);

        // Insert some vectors
        index.insert(1, vec![1.0, 0.0, 0.0]);
        index.insert(2, vec![0.0, 1.0, 0.0]);
        index.insert(3, vec![0.0, 0.0, 1.0]);
        index.insert(4, vec![0.5, 0.5, 0.0]);

        // Search for nearest to [1, 0, 0]
        let results = index.search(&[1.0, 0.0, 0.0], 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // Exact match should be first
    }

    #[test]
    fn test_hnsw_euclidean() {
        let config = HnswConfig {
            metric: DistanceMetric::Euclidean,
            ..Default::default()
        };
        let mut index = HnswIndex::new(config, 2);

        index.insert(1, vec![0.0, 0.0]);
        index.insert(2, vec![1.0, 0.0]);
        index.insert(3, vec![2.0, 0.0]);

        let results = index.search(&[0.5, 0.0], 2);
        assert!(!results.is_empty());
        // Nearest should be either 1 or 2
        assert!(results[0].0 == 1 || results[0].0 == 2);
    }

    #[test]
    fn test_hnsw_remove() {
        let config = HnswConfig::default();
        let mut index = HnswIndex::new(config, 3);

        index.insert(1, vec![1.0, 0.0, 0.0]);
        index.insert(2, vec![0.0, 1.0, 0.0]);

        assert_eq!(index.len(), 2);
        assert!(index.remove(1));
        assert_eq!(index.len(), 1);
        assert!(!index.remove(1)); // Already removed
    }

    #[test]
    fn test_hnsw_stats() {
        let config = HnswConfig::default();
        let mut index = HnswIndex::new(config, 3);

        for i in 0..100 {
            let v: Vec<f32> = (0..3).map(|j| (i + j) as f32 / 100.0).collect();
            index.insert(i, v);
        }

        let stats = index.stats();
        assert_eq!(stats.node_count, 100);
        assert_eq!(stats.dimension, 3);
    }
}
