//! Vector Search Module
//!
//! Provides approximate nearest neighbor (ANN) search capabilities for BoyoDB.
//! Features:
//! - HNSW (Hierarchical Navigable Small World) index
//! - Multiple distance metrics (Cosine, Euclidean, Dot Product)
//! - Batch indexing and search
//! - Filtered vector search
//! - Quantization for memory efficiency

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;
use std::sync::Arc;
use parking_lot::RwLock;

/// Vector with ID
#[derive(Debug, Clone)]
pub struct Vector {
    /// Unique identifier
    pub id: String,
    /// Vector data
    pub data: Vec<f32>,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

impl Vector {
    /// Create new vector
    pub fn new(id: &str, data: Vec<f32>) -> Self {
        Vector {
            id: id.to_string(),
            data,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Get dimension
    pub fn dim(&self) -> usize {
        self.data.len()
    }
}

/// Distance metrics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine)
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Dot product (negative for max heap)
    DotProduct,
    /// Manhattan distance (L1)
    Manhattan,
}

impl DistanceMetric {
    /// Calculate distance between two vectors
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::Cosine => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 {
                    1.0
                } else {
                    1.0 - (dot / (norm_a * norm_b))
                }
            }
            DistanceMetric::Euclidean => {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::DotProduct => {
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

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Vector ID
    pub id: String,
    /// Distance/score
    pub distance: f32,
    /// Vector data (optional)
    pub vector: Option<Vec<f32>>,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

/// HNSW index configuration
#[derive(Debug, Clone)]
pub struct HnswConfig {
    /// Maximum number of connections per node
    pub m: usize,
    /// Size of dynamic candidate list during construction
    pub ef_construction: usize,
    /// Size of dynamic candidate list during search
    pub ef_search: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Maximum level (auto-calculated if None)
    pub max_level: Option<usize>,
}

impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            metric: DistanceMetric::Cosine,
            max_level: None,
        }
    }
}

/// HNSW node
#[derive(Debug, Clone)]
struct HnswNode {
    id: String,
    vector: Vec<f32>,
    metadata: HashMap<String, String>,
    connections: Vec<Vec<String>>, // connections per level
    level: usize,
}

/// HNSW Index for approximate nearest neighbor search
pub struct HnswIndex {
    config: HnswConfig,
    nodes: HashMap<String, HnswNode>,
    entry_point: Option<String>,
    max_level: usize,
    dimension: Option<usize>,
}

impl HnswIndex {
    /// Create new HNSW index
    pub fn new(config: HnswConfig) -> Self {
        HnswIndex {
            config,
            nodes: HashMap::new(),
            entry_point: None,
            max_level: 0,
            dimension: None,
        }
    }

    /// Get random level for new node
    fn random_level(&self) -> usize {
        let ml = 1.0 / (self.config.m as f64).ln();
        let r: f64 = rand::random();
        (-r.ln() * ml).floor() as usize
    }

    /// Insert vector into index
    pub fn insert(&mut self, vector: Vector) -> Result<(), String> {
        // Check dimension
        if let Some(dim) = self.dimension {
            if vector.dim() != dim {
                return Err(format!(
                    "Dimension mismatch: expected {}, got {}",
                    dim,
                    vector.dim()
                ));
            }
        } else {
            self.dimension = Some(vector.dim());
        }

        let level = self.random_level();
        let id = vector.id.clone();

        let node = HnswNode {
            id: id.clone(),
            vector: vector.data.clone(),
            metadata: vector.metadata,
            connections: vec![Vec::new(); level + 1],
            level,
        };

        // First node becomes entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(id.clone());
            self.max_level = level;
            self.nodes.insert(id, node);
            return Ok(());
        }

        // Find entry point and navigate down
        let entry = self.entry_point.clone().unwrap();
        let mut current = entry;

        // Navigate from top level down to node's level
        for l in (level + 1..=self.max_level).rev() {
            current = self.greedy_search(&vector.data, &current, l);
        }

        // Insert at each level
        for l in (0..=level.min(self.max_level)).rev() {
            let neighbors = self.search_layer(&vector.data, &current, self.config.ef_construction, l);

            // Get next current node for navigation
            let next_current = neighbors.first().map(|r| r.id.clone());

            // Select M best neighbors
            let selected: Vec<String> = neighbors
                .into_iter()
                .take(self.config.m)
                .map(|r| r.id)
                .collect();

            // Add bidirectional connections - collect data first to avoid borrow issues
            let mut to_prune: Vec<(String, Vec<f32>, Vec<String>)> = Vec::new();

            for neighbor_id in &selected {
                if let Some(neighbor) = self.nodes.get_mut(neighbor_id) {
                    if l < neighbor.connections.len() {
                        neighbor.connections[l].push(id.clone());
                        // Check if pruning is needed
                        if neighbor.connections[l].len() > self.config.m * 2 {
                            to_prune.push((
                                neighbor_id.clone(),
                                neighbor.vector.clone(),
                                neighbor.connections[l].clone(),
                            ));
                        }
                    }
                }
            }

            // Perform pruning
            for (neighbor_id, vec, conns) in to_prune {
                let pruned = self.prune_connections(&vec, &conns, self.config.m);
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    if l < neighbor.connections.len() {
                        neighbor.connections[l] = pruned;
                    }
                }
            }

            // Store node connections
            let node_mut = self.nodes.entry(id.clone()).or_insert(node.clone());
            if l < node_mut.connections.len() {
                node_mut.connections[l] = selected;
            }

            if let Some(next) = next_current {
                current = next;
            }
        }

        // Update entry point if needed
        if level > self.max_level {
            self.entry_point = Some(id.clone());
            self.max_level = level;
        }

        self.nodes.insert(id, node);
        Ok(())
    }

    /// Greedy search at a single level
    fn greedy_search(&self, query: &[f32], start: &str, level: usize) -> String {
        let mut current = start.to_string();
        let mut current_dist = self.distance(query, start);

        loop {
            let mut changed = false;
            if let Some(node) = self.nodes.get(&current) {
                if level < node.connections.len() {
                    for neighbor_id in &node.connections[level] {
                        let dist = self.distance(query, neighbor_id);
                        if dist < current_dist {
                            current_dist = dist;
                            current = neighbor_id.clone();
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    /// Search within a layer
    fn search_layer(
        &self,
        query: &[f32],
        start: &str,
        ef: usize,
        level: usize,
    ) -> Vec<SearchResult> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut candidates: BinaryHeap<SearchResult> = BinaryHeap::new();
        let mut results: BinaryHeap<SearchResult> = BinaryHeap::new();

        let start_dist = self.distance(query, start);
        visited.insert(start.to_string());

        candidates.push(SearchResult {
            id: start.to_string(),
            distance: -start_dist, // Negative for max-heap as min-heap
            vector: None,
            metadata: HashMap::new(),
        });

        results.push(SearchResult {
            id: start.to_string(),
            distance: start_dist,
            vector: None,
            metadata: HashMap::new(),
        });

        while let Some(candidate) = candidates.pop() {
            let c_dist = -candidate.distance;

            // Get worst result distance
            let worst_dist = results.peek().map(|r| r.distance).unwrap_or(f32::MAX);

            if c_dist > worst_dist && results.len() >= ef {
                break;
            }

            if let Some(node) = self.nodes.get(&candidate.id) {
                if level < node.connections.len() {
                    for neighbor_id in &node.connections[level] {
                        if visited.insert(neighbor_id.clone()) {
                            let dist = self.distance(query, neighbor_id);

                            if dist < worst_dist || results.len() < ef {
                                candidates.push(SearchResult {
                                    id: neighbor_id.clone(),
                                    distance: -dist,
                                    vector: None,
                                    metadata: HashMap::new(),
                                });

                                results.push(SearchResult {
                                    id: neighbor_id.clone(),
                                    distance: dist,
                                    vector: None,
                                    metadata: HashMap::new(),
                                });

                                if results.len() > ef {
                                    results.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        results.into_sorted_vec()
    }

    /// Prune connections to keep best M
    fn prune_connections(&self, vector: &[f32], connections: &[String], m: usize) -> Vec<String> {
        let mut scored: Vec<(String, f32)> = connections
            .iter()
            .filter_map(|id| {
                self.nodes.get(id).map(|n| {
                    (id.clone(), self.config.metric.distance(vector, &n.vector))
                })
            })
            .collect();

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.into_iter().take(m).map(|(id, _)| id).collect()
    }

    /// Calculate distance to a node
    fn distance(&self, query: &[f32], node_id: &str) -> f32 {
        self.nodes
            .get(node_id)
            .map(|n| self.config.metric.distance(query, &n.vector))
            .unwrap_or(f32::MAX)
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<SearchResult> {
        self.search_with_filter(query, k, |_| true)
    }

    /// Search with metadata filter
    pub fn search_with_filter<F>(&self, query: &[f32], k: usize, filter: F) -> Vec<SearchResult>
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        if self.entry_point.is_none() {
            return Vec::new();
        }

        let entry = self.entry_point.clone().unwrap();
        let mut current = entry;

        // Navigate from top to level 1
        for l in (1..=self.max_level).rev() {
            current = self.greedy_search(query, &current, l);
        }

        // Search at level 0
        let candidates = self.search_layer(query, &current, self.config.ef_search, 0);

        // Apply filter and return top k
        candidates
            .into_iter()
            .filter(|r| {
                self.nodes
                    .get(&r.id)
                    .map(|n| filter(&n.metadata))
                    .unwrap_or(false)
            })
            .take(k)
            .map(|r| {
                let node = self.nodes.get(&r.id);
                SearchResult {
                    id: r.id,
                    distance: r.distance,
                    vector: node.map(|n| n.vector.clone()),
                    metadata: node.map(|n| n.metadata.clone()).unwrap_or_default(),
                }
            })
            .collect()
    }

    /// Get index statistics
    pub fn stats(&self) -> HnswStats {
        let total_connections: usize = self.nodes
            .values()
            .flat_map(|n| n.connections.iter())
            .map(|c| c.len())
            .sum();

        HnswStats {
            num_vectors: self.nodes.len(),
            dimension: self.dimension.unwrap_or(0),
            max_level: self.max_level,
            total_connections,
            avg_connections: if self.nodes.is_empty() {
                0.0
            } else {
                total_connections as f64 / self.nodes.len() as f64
            },
        }
    }

    /// Delete vector by ID
    pub fn delete(&mut self, id: &str) -> bool {
        if let Some(node) = self.nodes.remove(id) {
            // Remove from neighbors' connections
            for level_connections in &node.connections {
                for neighbor_id in level_connections {
                    if let Some(neighbor) = self.nodes.get_mut(neighbor_id) {
                        for conn in &mut neighbor.connections {
                            conn.retain(|c| c != id);
                        }
                    }
                }
            }

            // Update entry point if needed
            if self.entry_point.as_ref() == Some(&id.to_string()) {
                self.entry_point = self.nodes.keys().next().cloned();
                self.max_level = self.nodes
                    .values()
                    .map(|n| n.level)
                    .max()
                    .unwrap_or(0);
            }

            true
        } else {
            false
        }
    }

    /// Get vector by ID
    pub fn get(&self, id: &str) -> Option<Vector> {
        self.nodes.get(id).map(|n| Vector {
            id: n.id.clone(),
            data: n.vector.clone(),
            metadata: n.metadata.clone(),
        })
    }
}

/// HNSW index statistics
#[derive(Debug, Clone)]
pub struct HnswStats {
    /// Number of vectors
    pub num_vectors: usize,
    /// Vector dimension
    pub dimension: usize,
    /// Maximum level in the graph
    pub max_level: usize,
    /// Total number of connections
    pub total_connections: usize,
    /// Average connections per node
    pub avg_connections: f64,
}

/// Product Quantization for memory-efficient vector storage
pub struct ProductQuantizer {
    /// Number of subspaces
    num_subspaces: usize,
    /// Bits per subspace
    bits_per_subspace: usize,
    /// Codebooks (centroids for each subspace)
    codebooks: Vec<Vec<Vec<f32>>>,
    /// Original dimension
    dimension: usize,
}

impl ProductQuantizer {
    /// Create new product quantizer
    pub fn new(dimension: usize, num_subspaces: usize, bits_per_subspace: usize) -> Self {
        assert!(dimension % num_subspaces == 0, "Dimension must be divisible by num_subspaces");

        ProductQuantizer {
            num_subspaces,
            bits_per_subspace,
            codebooks: Vec::new(),
            dimension,
        }
    }

    /// Train quantizer on vectors
    pub fn train(&mut self, vectors: &[Vec<f32>], iterations: usize) {
        let subspace_dim = self.dimension / self.num_subspaces;
        let num_centroids = 1 << self.bits_per_subspace;

        self.codebooks = Vec::with_capacity(self.num_subspaces);

        for s in 0..self.num_subspaces {
            let start = s * subspace_dim;
            let end = start + subspace_dim;

            // Extract subvectors
            let subvectors: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            // K-means clustering
            let centroids = self.kmeans(&subvectors, num_centroids, iterations);
            self.codebooks.push(centroids);
        }
    }

    /// Simple k-means implementation
    fn kmeans(&self, vectors: &[Vec<f32>], k: usize, iterations: usize) -> Vec<Vec<f32>> {
        if vectors.is_empty() {
            return Vec::new();
        }

        let dim = vectors[0].len();

        // Initialize centroids randomly
        let mut centroids: Vec<Vec<f32>> = vectors
            .iter()
            .take(k.min(vectors.len()))
            .cloned()
            .collect();

        // Pad if needed
        while centroids.len() < k {
            centroids.push(vec![0.0; dim]);
        }

        for _ in 0..iterations {
            // Assign vectors to clusters
            let mut clusters: Vec<Vec<usize>> = vec![Vec::new(); k];

            for (i, v) in vectors.iter().enumerate() {
                let mut best_cluster = 0;
                let mut best_dist = f32::MAX;

                for (j, c) in centroids.iter().enumerate() {
                    let dist: f32 = v.iter()
                        .zip(c.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum();
                    if dist < best_dist {
                        best_dist = dist;
                        best_cluster = j;
                    }
                }

                clusters[best_cluster].push(i);
            }

            // Update centroids
            for (j, cluster) in clusters.iter().enumerate() {
                if cluster.is_empty() {
                    continue;
                }

                let mut new_centroid = vec![0.0; dim];
                for &idx in cluster {
                    for (d, val) in vectors[idx].iter().enumerate() {
                        new_centroid[d] += val;
                    }
                }

                let count = cluster.len() as f32;
                for val in &mut new_centroid {
                    *val /= count;
                }

                centroids[j] = new_centroid;
            }
        }

        centroids
    }

    /// Encode vector to compact representation
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let subspace_dim = self.dimension / self.num_subspaces;
        let mut codes = Vec::with_capacity(self.num_subspaces);

        for (s, codebook) in self.codebooks.iter().enumerate() {
            let start = s * subspace_dim;
            let end = start + subspace_dim;
            let subvector = &vector[start..end];

            // Find nearest centroid
            let mut best_idx = 0;
            let mut best_dist = f32::MAX;

            for (i, centroid) in codebook.iter().enumerate() {
                let dist: f32 = subvector
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                if dist < best_dist {
                    best_dist = dist;
                    best_idx = i;
                }
            }

            codes.push(best_idx as u8);
        }

        codes
    }

    /// Decode compact representation to approximate vector
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut vector = Vec::with_capacity(self.dimension);

        for (s, &code) in codes.iter().enumerate() {
            if s < self.codebooks.len() && (code as usize) < self.codebooks[s].len() {
                vector.extend_from_slice(&self.codebooks[s][code as usize]);
            }
        }

        vector
    }

    /// Compute approximate distance using lookup tables
    pub fn asymmetric_distance(&self, query: &[f32], codes: &[u8]) -> f32 {
        let subspace_dim = self.dimension / self.num_subspaces;
        let mut distance = 0.0;

        for (s, &code) in codes.iter().enumerate() {
            if s >= self.codebooks.len() {
                break;
            }

            let start = s * subspace_dim;
            let end = start + subspace_dim;
            let query_sub = &query[start..end];

            if (code as usize) < self.codebooks[s].len() {
                let centroid = &self.codebooks[s][code as usize];
                distance += query_sub
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>();
            }
        }

        distance.sqrt()
    }
}

/// Thread-safe vector index registry
pub struct VectorIndexRegistry {
    indexes: Arc<RwLock<HashMap<String, HnswIndex>>>,
}

impl VectorIndexRegistry {
    /// Create new registry
    pub fn new() -> Self {
        VectorIndexRegistry {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create new index
    pub fn create_index(&self, name: &str, config: HnswConfig) -> Result<(), String> {
        let mut indexes = self.indexes.write();
        if indexes.contains_key(name) {
            return Err(format!("Index '{}' already exists", name));
        }
        indexes.insert(name.to_string(), HnswIndex::new(config));
        Ok(())
    }

    /// Insert vector into index
    pub fn insert(&self, index_name: &str, vector: Vector) -> Result<(), String> {
        let mut indexes = self.indexes.write();
        let index = indexes
            .get_mut(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;
        index.insert(vector)
    }

    /// Search index
    pub fn search(&self, index_name: &str, query: &[f32], k: usize) -> Result<Vec<SearchResult>, String> {
        let indexes = self.indexes.read();
        let index = indexes
            .get(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;
        Ok(index.search(query, k))
    }

    /// Get index stats
    pub fn stats(&self, index_name: &str) -> Result<HnswStats, String> {
        let indexes = self.indexes.read();
        let index = indexes
            .get(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;
        Ok(index.stats())
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Result<Vec<String>, String> {
        let indexes = self.indexes.read();
        Ok(indexes.keys().cloned().collect())
    }

    /// Delete index
    pub fn delete_index(&self, name: &str) -> Result<bool, String> {
        let mut indexes = self.indexes.write();
        Ok(indexes.remove(name).is_some())
    }
}

impl Default for VectorIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metrics() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        let cosine = DistanceMetric::Cosine.distance(&a, &b);
        assert!((cosine - 1.0).abs() < 0.001); // Orthogonal vectors

        let euclidean = DistanceMetric::Euclidean.distance(&a, &b);
        assert!((euclidean - 1.414).abs() < 0.01); // sqrt(2)
    }

    #[test]
    fn test_hnsw_insert_search() {
        let mut index = HnswIndex::new(HnswConfig::default());

        // Insert some vectors
        for i in 0..10 {
            let v = Vector::new(&format!("v{}", i), vec![i as f32, 0.0, 0.0]);
            index.insert(v).unwrap();
        }

        // Search
        let results = index.search(&[5.0, 0.0, 0.0], 3);
        assert!(!results.is_empty());
        assert_eq!(results[0].id, "v5");
    }

    #[test]
    fn test_hnsw_with_metadata() {
        let mut index = HnswIndex::new(HnswConfig::default());

        index.insert(Vector::new("v1", vec![1.0, 0.0]).with_metadata("type", "a")).unwrap();
        index.insert(Vector::new("v2", vec![2.0, 0.0]).with_metadata("type", "b")).unwrap();
        index.insert(Vector::new("v3", vec![3.0, 0.0]).with_metadata("type", "a")).unwrap();

        // Filter search
        let results = index.search_with_filter(&[2.0, 0.0], 10, |m| {
            m.get("type").map(|t| t == "a").unwrap_or(false)
        });

        assert!(results.iter().all(|r| {
            r.metadata.get("type").map(|t| t == "a").unwrap_or(false)
        }));
    }

    #[test]
    fn test_product_quantizer() {
        let mut pq = ProductQuantizer::new(8, 2, 4);

        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![i as f32; 8])
            .collect();

        pq.train(&vectors, 10);

        let original = vec![50.0; 8];
        let codes = pq.encode(&original);
        let decoded = pq.decode(&codes);

        // Decoded should be close to original
        let error: f32 = original.iter()
            .zip(decoded.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();

        assert!(error < 100.0); // Allow some quantization error
    }

    #[test]
    fn test_registry() {
        let registry = VectorIndexRegistry::new();

        registry.create_index("test", HnswConfig::default()).unwrap();
        registry.insert("test", Vector::new("v1", vec![1.0, 2.0, 3.0])).unwrap();

        let stats = registry.stats("test").unwrap();
        assert_eq!(stats.num_vectors, 1);
    }
}
