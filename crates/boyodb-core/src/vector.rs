//! Vector Similarity Search for ML/AI Workloads
//!
//! Provides high-performance vector operations for:
//! - Embedding similarity search (semantic search)
//! - RAG (Retrieval Augmented Generation) pipelines
//! - Recommendation systems
//! - Anomaly detection
//!
//! Supports multiple distance metrics:
//! - Cosine similarity (default for text embeddings)
//! - Euclidean distance (L2)
//! - Dot product (for normalized vectors)
//! - Manhattan distance (L1)
//!
//! Example SQL:
//! ```sql
//! SELECT id, title, vector_cosine_similarity(embedding, $query_vector) AS score
//! FROM documents
//! ORDER BY score DESC
//! LIMIT 10;
//! ```

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Distance metric for vector similarity
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance)
    /// Best for: text embeddings, semantic search
    Cosine,
    /// Euclidean distance (L2 norm)
    /// Best for: image embeddings, geometric data
    Euclidean,
    /// Dot product (inner product)
    /// Best for: pre-normalized vectors
    DotProduct,
    /// Manhattan distance (L1 norm)
    /// Best for: sparse vectors, categorical data
    Manhattan,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Cosine
    }
}

/// Result of a vector search
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// Index of the vector in the dataset
    pub index: usize,
    /// Distance/similarity score
    pub score: f32,
}

impl PartialEq for VectorSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for VectorSearchResult {}

impl PartialOrd for VectorSearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VectorSearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Normal ordering: BinaryHeap is max-heap, so largest score at top
        // We want to keep k smallest scores, so pop() removes the largest
        self.score.partial_cmp(&other.score).unwrap_or(Ordering::Equal)
    }
}

/// Compute cosine similarity between two vectors
/// Returns value in range [-1, 1] where 1 = identical direction
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    // Process in batches of 8 for SIMD optimization
    let chunks = a.len() / 8;
    for chunk in 0..chunks {
        let base = chunk * 8;
        for i in 0..8 {
            let av = a[base + i];
            let bv = b[base + i];
            dot += av * bv;
            norm_a += av * av;
            norm_b += bv * bv;
        }
    }

    // Handle remainder
    for i in (chunks * 8)..a.len() {
        let av = a[i];
        let bv = b[i];
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
    }

    let denom = (norm_a * norm_b).sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

/// Compute Euclidean distance (L2) between two vectors
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;
    let chunks = a.len() / 8;

    for chunk in 0..chunks {
        let base = chunk * 8;
        for i in 0..8 {
            let diff = a[base + i] - b[base + i];
            sum += diff * diff;
        }
    }

    for i in (chunks * 8)..a.len() {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    sum.sqrt()
}

/// Compute dot product between two vectors
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;
    let chunks = a.len() / 8;

    for chunk in 0..chunks {
        let base = chunk * 8;
        sum += a[base] * b[base]
            + a[base + 1] * b[base + 1]
            + a[base + 2] * b[base + 2]
            + a[base + 3] * b[base + 3]
            + a[base + 4] * b[base + 4]
            + a[base + 5] * b[base + 5]
            + a[base + 6] * b[base + 6]
            + a[base + 7] * b[base + 7];
    }

    for i in (chunks * 8)..a.len() {
        sum += a[i] * b[i];
    }

    sum
}

/// Compute Manhattan distance (L1) between two vectors
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "Vector dimensions must match");

    let mut sum = 0.0f32;
    for (av, bv) in a.iter().zip(b.iter()) {
        sum += (av - bv).abs();
    }
    sum
}

/// Compute distance/similarity based on metric
#[inline]
pub fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => 1.0 - cosine_similarity(a, b), // Convert to distance
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::DotProduct => -dot_product(a, b), // Negate so smaller = more similar
        DistanceMetric::Manhattan => manhattan_distance(a, b),
    }
}

/// Find k nearest neighbors in a dataset
/// Returns indices and distances of the k closest vectors
pub fn knn_search(
    query: &[f32],
    vectors: &[Vec<f32>],
    k: usize,
    metric: DistanceMetric,
) -> Vec<VectorSearchResult> {
    if vectors.is_empty() || k == 0 {
        return vec![];
    }

    let k = k.min(vectors.len());

    // Use a max-heap to maintain k smallest distances
    let mut heap: BinaryHeap<VectorSearchResult> = BinaryHeap::with_capacity(k + 1);

    for (i, vec) in vectors.iter().enumerate() {
        let dist = compute_distance(query, vec, metric);
        heap.push(VectorSearchResult { index: i, score: dist });

        if heap.len() > k {
            heap.pop(); // Remove the largest
        }
    }

    // Convert to sorted vec (smallest distance first)
    let mut results: Vec<_> = heap.into_iter().collect();
    results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
    results
}

/// Parallel k-NN search for large datasets
pub fn parallel_knn_search(
    query: &[f32],
    vectors: &[Vec<f32>],
    k: usize,
    metric: DistanceMetric,
) -> Vec<VectorSearchResult> {
    if vectors.is_empty() || k == 0 {
        return vec![];
    }

    let k = k.min(vectors.len());

    // Compute all distances in parallel
    let distances: Vec<VectorSearchResult> = vectors
        .par_iter()
        .enumerate()
        .map(|(i, vec)| VectorSearchResult {
            index: i,
            score: compute_distance(query, vec, metric),
        })
        .collect();

    // Find k smallest
    let mut sorted = distances;
    sorted.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
    sorted.truncate(k);
    sorted
}

/// Batch k-NN search for multiple queries
pub fn batch_knn_search(
    queries: &[Vec<f32>],
    vectors: &[Vec<f32>],
    k: usize,
    metric: DistanceMetric,
) -> Vec<Vec<VectorSearchResult>> {
    queries
        .par_iter()
        .map(|q| parallel_knn_search(q, vectors, k, metric))
        .collect()
}

/// Normalize a vector to unit length
pub fn normalize(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vec.iter_mut() {
            *x /= norm;
        }
    }
}

/// Create a normalized copy of a vector
pub fn normalized(vec: &[f32]) -> Vec<f32> {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        vec.iter().map(|x| x / norm).collect()
    } else {
        vec.to_vec()
    }
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Dimension of vectors
    pub dimensions: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Number of clusters for IVF index (0 = flat index)
    pub num_clusters: usize,
    /// Number of probes during search
    pub num_probes: usize,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            dimensions: 768, // Common embedding size
            metric: DistanceMetric::Cosine,
            num_clusters: 0,
            num_probes: 1,
        }
    }
}

/// Simple flat vector index (brute force)
#[derive(Debug, Clone)]
pub struct FlatVectorIndex {
    config: VectorIndexConfig,
    vectors: Vec<Vec<f32>>,
    ids: Vec<u64>,
}

impl FlatVectorIndex {
    pub fn new(config: VectorIndexConfig) -> Self {
        Self {
            config,
            vectors: Vec::new(),
            ids: Vec::new(),
        }
    }

    /// Add a vector with its ID
    pub fn add(&mut self, id: u64, vector: Vec<f32>) {
        debug_assert_eq!(vector.len(), self.config.dimensions, "Vector dimension mismatch");
        self.ids.push(id);
        self.vectors.push(vector);
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        let results = parallel_knn_search(query, &self.vectors, k, self.config.metric);
        results
            .into_iter()
            .map(|r| (self.ids[r.index], r.score))
            .collect()
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Inverted file (IVF) vector index for approximate search
#[derive(Debug, Clone)]
pub struct IvfVectorIndex {
    config: VectorIndexConfig,
    centroids: Vec<Vec<f32>>,
    clusters: Vec<Vec<(u64, Vec<f32>)>>,
}

impl IvfVectorIndex {
    pub fn new(config: VectorIndexConfig) -> Self {
        Self {
            config,
            centroids: Vec::new(),
            clusters: Vec::new(),
        }
    }

    /// Build index from vectors using k-means clustering
    pub fn build(&mut self, ids: &[u64], vectors: &[Vec<f32>]) {
        if vectors.is_empty() || self.config.num_clusters == 0 {
            return;
        }

        let k = self.config.num_clusters.min(vectors.len());

        // Simple k-means initialization: take first k vectors as centroids
        self.centroids = vectors.iter().take(k).cloned().collect();
        self.clusters = vec![Vec::new(); k];

        // Assign vectors to nearest centroid
        for (id, vec) in ids.iter().zip(vectors.iter()) {
            let nearest = self.find_nearest_centroid(vec);
            self.clusters[nearest].push((*id, vec.clone()));
        }
    }

    fn find_nearest_centroid(&self, vec: &[f32]) -> usize {
        let mut best_idx = 0;
        let mut best_dist = f32::MAX;

        for (i, centroid) in self.centroids.iter().enumerate() {
            let dist = compute_distance(vec, centroid, self.config.metric);
            if dist < best_dist {
                best_dist = dist;
                best_idx = i;
            }
        }

        best_idx
    }

    /// Search using IVF (approximate)
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(u64, f32)> {
        if self.centroids.is_empty() {
            return vec![];
        }

        // Find nearest centroids
        let mut centroid_distances: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, compute_distance(query, c, self.config.metric)))
            .collect();
        centroid_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        // Search in top nprobe clusters
        let mut results: Vec<VectorSearchResult> = Vec::new();
        let nprobe = self.config.num_probes.min(self.centroids.len());

        for (cluster_idx, _) in centroid_distances.iter().take(nprobe) {
            for (id, vec) in &self.clusters[*cluster_idx] {
                let dist = compute_distance(query, vec, self.config.metric);
                results.push(VectorSearchResult {
                    index: *id as usize,
                    score: dist,
                });
            }
        }

        // Sort and take top k
        results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
        results.truncate(k);

        results
            .into_iter()
            .map(|r| (r.index as u64, r.score))
            .collect()
    }
}

/// Statistics about vector search performance
#[derive(Debug, Clone, Default)]
pub struct VectorSearchStats {
    pub total_queries: u64,
    pub total_vectors_scanned: u64,
    pub avg_query_time_us: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine_similarity(&a, &c) - 0.0).abs() < 0.001);

        let d = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &d) - (-1.0)).abs() < 0.001);
    }

    #[test]
    fn test_euclidean_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_dot_product() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        assert!((dot_product(&a, &b) - 32.0).abs() < 0.001);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((manhattan_distance(&a, &b) - 7.0).abs() < 0.001);
    }

    #[test]
    fn test_knn_search() {
        let vectors = vec![
            vec![1.0, 0.0],  // Index 0 - exact match
            vec![0.0, 1.0],  // Index 1 - distance sqrt(2)
            vec![1.0, 1.0],  // Index 2 - distance 1.0
            vec![2.0, 0.0],  // Index 3 - distance 1.0
        ];
        let query = vec![1.0, 0.0];

        let results = knn_search(&query, &vectors, 2, DistanceMetric::Euclidean);

        assert_eq!(results.len(), 2);
        // First result should have near-zero distance (the exact match at index 0)
        assert!(results[0].score < 0.001, "First result score should be ~0, got {}", results[0].score);
        // Second result should be one of the vectors at distance 1.0 (index 2 or 3)
        assert!((results[1].score - 1.0).abs() < 0.001, "Second result score should be ~1.0, got {}", results[1].score);
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        assert!((v[0] - 0.6).abs() < 0.001);
        assert!((v[1] - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_normalized() {
        let v = vec![3.0, 4.0];
        let n = normalized(&v);
        assert!((n[0] - 0.6).abs() < 0.001);
        assert!((n[1] - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_flat_vector_index() {
        let mut index = FlatVectorIndex::new(VectorIndexConfig {
            dimensions: 3,
            metric: DistanceMetric::Cosine,
            ..Default::default()
        });

        index.add(1, vec![1.0, 0.0, 0.0]);
        index.add(2, vec![0.0, 1.0, 0.0]);
        index.add(3, vec![0.0, 0.0, 1.0]);
        index.add(4, vec![1.0, 1.0, 0.0]);

        assert_eq!(index.len(), 4);

        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // Exact match
    }

    #[test]
    fn test_parallel_knn_search() {
        let vectors: Vec<Vec<f32>> = (0..1000)
            .map(|i| vec![(i as f32) / 1000.0, 1.0 - (i as f32) / 1000.0])
            .collect();

        let query = vec![0.5, 0.5];
        let results = parallel_knn_search(&query, &vectors, 10, DistanceMetric::Euclidean);

        assert_eq!(results.len(), 10);
        // Result around index 500 should be closest
        assert!((results[0].index as i32 - 500).abs() < 10);
    }

    #[test]
    fn test_batch_knn_search() {
        let vectors = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 1.0],
        ];

        let queries = vec![vec![1.0, 0.0], vec![0.0, 1.0]];

        let results = batch_knn_search(&queries, &vectors, 2, DistanceMetric::Euclidean);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0].index, 0); // First query matches first vector
        assert_eq!(results[1][0].index, 1); // Second query matches second vector
    }

    #[test]
    fn test_high_dimensional_cosine() {
        // Test with 768-dimensional vectors (common embedding size)
        let dim = 768;
        let v1: Vec<f32> = (0..dim).map(|i| (i as f32) / 1000.0).collect();
        let v2: Vec<f32> = (0..dim).map(|i| (i as f32) / 1000.0 + 0.001).collect();

        let sim = cosine_similarity(&v1, &v2);
        // Very similar vectors should have high cosine similarity
        assert!(sim > 0.99);
    }

    #[test]
    fn test_ivf_index() {
        let mut config = VectorIndexConfig {
            dimensions: 2,
            metric: DistanceMetric::Euclidean,
            num_clusters: 2,
            num_probes: 2,
        };

        let mut index = IvfVectorIndex::new(config);

        let ids: Vec<u64> = (0..100).collect();
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| vec![(i as f32) / 100.0, (i as f32) / 100.0])
            .collect();

        index.build(&ids, &vectors);

        let results = index.search(&[0.5, 0.5], 5);
        assert_eq!(results.len(), 5);
    }
}
