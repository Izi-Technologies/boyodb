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
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
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
        heap.push(VectorSearchResult {
            index: i,
            score: dist,
        });

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
        debug_assert_eq!(
            vector.len(),
            self.config.dimensions,
            "Vector dimension mismatch"
        );
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

// ============================================================================
// AI Data Support - Filtered Search, Hybrid Search, Embeddings
// ============================================================================

/// Result from filtered vector search (includes row ID for joining with metadata)
#[derive(Debug, Clone)]
pub struct FilteredSearchResult {
    /// Row ID in the source table
    pub row_id: u64,
    /// Vector similarity/distance score
    pub vector_score: f32,
    /// Optional metadata score (e.g., BM25 for hybrid search)
    pub metadata_score: Option<f32>,
    /// Combined score for ranking
    pub combined_score: f32,
}

/// Configuration for hybrid search (vector + text)
#[derive(Debug, Clone)]
pub struct HybridSearchConfig {
    /// Weight for vector similarity score (0.0-1.0)
    pub vector_weight: f32,
    /// Weight for text/BM25 score (0.0-1.0)
    pub text_weight: f32,
    /// Reciprocal Rank Fusion constant (default: 60)
    pub rrf_k: u32,
    /// Normalization method for score fusion
    pub fusion_method: ScoreFusionMethod,
}

impl Default for HybridSearchConfig {
    fn default() -> Self {
        Self {
            vector_weight: 0.5,
            text_weight: 0.5,
            rrf_k: 60,
            fusion_method: ScoreFusionMethod::RRF,
        }
    }
}

/// Method for fusing vector and text scores
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreFusionMethod {
    /// Reciprocal Rank Fusion - robust to score distribution differences
    RRF,
    /// Weighted linear combination (requires score normalization)
    Linear,
    /// Convex combination after min-max normalization
    Convex,
}

/// Filtered vector search - search vectors with metadata predicates
///
/// This is the key feature for AI workloads: find similar vectors that also
/// match certain criteria (e.g., category, date range, user permissions).
///
/// # Arguments
/// * `query` - Query vector
/// * `candidates` - Pre-filtered candidate vectors with their IDs
/// * `k` - Number of results to return
/// * `metric` - Distance metric to use
///
/// # Example
/// ```ignore
/// // Pre-filter by category, then vector search
/// let candidates: Vec<(u64, Vec<f32>)> = db
///     .query("SELECT id, embedding FROM docs WHERE category = 'AI'")
///     .collect();
/// let results = filtered_vector_search(&query_embedding, &candidates, 10, DistanceMetric::Cosine);
/// ```
pub fn filtered_vector_search(
    query: &[f32],
    candidates: &[(u64, Vec<f32>)],
    k: usize,
    metric: DistanceMetric,
) -> Vec<FilteredSearchResult> {
    if candidates.is_empty() {
        return vec![];
    }

    // Use parallel search for large candidate sets
    let results: Vec<(u64, f32)> = if candidates.len() > 1000 {
        candidates
            .par_iter()
            .map(|(id, vec)| (*id, compute_distance(query, vec, metric)))
            .collect()
    } else {
        candidates
            .iter()
            .map(|(id, vec)| (*id, compute_distance(query, vec, metric)))
            .collect()
    };

    // Sort by distance and take top k
    let mut scored: Vec<_> = results;
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);

    scored
        .into_iter()
        .map(|(row_id, vector_score)| FilteredSearchResult {
            row_id,
            vector_score,
            metadata_score: None,
            combined_score: vector_score,
        })
        .collect()
}

/// Hybrid search combining vector similarity and text relevance (BM25)
///
/// Uses Reciprocal Rank Fusion (RRF) by default for robust score combination
/// without requiring score normalization.
///
/// # Arguments
/// * `vector_results` - Results from vector search: (row_id, distance)
/// * `text_results` - Results from text search (BM25): (row_id, score)
/// * `k` - Number of final results
/// * `config` - Hybrid search configuration
///
/// # Returns
/// Combined results ranked by fused score
pub fn hybrid_search(
    vector_results: &[(u64, f32)],
    text_results: &[(u64, f32)],
    k: usize,
    config: &HybridSearchConfig,
) -> Vec<FilteredSearchResult> {
    use std::collections::HashMap;

    match config.fusion_method {
        ScoreFusionMethod::RRF => {
            // Reciprocal Rank Fusion: score = sum(1 / (k + rank))
            let mut scores: HashMap<u64, (f32, Option<f32>, f32)> = HashMap::new();
            let rrf_k = config.rrf_k as f32;

            // Add vector scores (lower distance = higher rank)
            for (rank, (id, dist)) in vector_results.iter().enumerate() {
                let rrf_score = config.vector_weight / (rrf_k + rank as f32 + 1.0);
                scores.entry(*id).or_insert((0.0, None, 0.0)).0 = *dist;
                scores.entry(*id).or_insert((0.0, None, 0.0)).2 += rrf_score;
            }

            // Add text scores (higher BM25 = higher rank, already sorted desc)
            for (rank, (id, bm25)) in text_results.iter().enumerate() {
                let rrf_score = config.text_weight / (rrf_k + rank as f32 + 1.0);
                let entry = scores.entry(*id).or_insert((f32::MAX, None, 0.0));
                entry.1 = Some(*bm25);
                entry.2 += rrf_score;
            }

            // Sort by RRF score (descending)
            let mut results: Vec<_> = scores.into_iter().collect();
            results.sort_by(|a, b| b.1 .2.partial_cmp(&a.1 .2).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(k);

            results
                .into_iter()
                .map(|(row_id, (vector_score, metadata_score, combined_score))| {
                    FilteredSearchResult {
                        row_id,
                        vector_score,
                        metadata_score,
                        combined_score,
                    }
                })
                .collect()
        }
        ScoreFusionMethod::Linear | ScoreFusionMethod::Convex => {
            // Linear/Convex combination requires normalized scores
            let mut scores: HashMap<u64, (f32, Option<f32>, f32)> = HashMap::new();

            // Normalize vector distances to 0-1 (invert so higher = better)
            let (v_min, v_max) = vector_results
                .iter()
                .fold((f32::MAX, f32::MIN), |(min, max), (_, d)| {
                    (min.min(*d), max.max(*d))
                });
            let v_range = (v_max - v_min).max(1e-6);

            for (id, dist) in vector_results {
                let normalized = 1.0 - ((*dist - v_min) / v_range);
                scores.insert(*id, (*dist, None, normalized * config.vector_weight));
            }

            // Normalize text scores to 0-1
            let (t_min, t_max) = text_results
                .iter()
                .fold((f32::MAX, f32::MIN), |(min, max), (_, s)| {
                    (min.min(*s), max.max(*s))
                });
            let t_range = (t_max - t_min).max(1e-6);

            for (id, bm25) in text_results {
                let normalized = (*bm25 - t_min) / t_range;
                let entry = scores.entry(*id).or_insert((f32::MAX, None, 0.0));
                entry.1 = Some(*bm25);
                entry.2 += normalized * config.text_weight;
            }

            let mut results: Vec<_> = scores.into_iter().collect();
            results.sort_by(|a, b| b.1 .2.partial_cmp(&a.1 .2).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(k);

            results
                .into_iter()
                .map(|(row_id, (vector_score, metadata_score, combined_score))| {
                    FilteredSearchResult {
                        row_id,
                        vector_score,
                        metadata_score,
                        combined_score,
                    }
                })
                .collect()
        }
    }
}

/// Embedding model metadata for tracking which model generated embeddings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingModelInfo {
    /// Model identifier (e.g., "text-embedding-3-small", "all-MiniLM-L6-v2")
    pub model_id: String,
    /// Model provider (e.g., "openai", "huggingface", "cohere")
    pub provider: String,
    /// Vector dimensions
    pub dimensions: u32,
    /// Maximum tokens/sequence length
    pub max_tokens: u32,
    /// Model version for tracking re-embedding needs
    pub version: String,
}

impl EmbeddingModelInfo {
    /// Create info for OpenAI embedding models
    pub fn openai(model: &str) -> Self {
        let (dimensions, max_tokens) = match model {
            "text-embedding-3-small" => (1536, 8191),
            "text-embedding-3-large" => (3072, 8191),
            "text-embedding-ada-002" => (1536, 8191),
            _ => (1536, 8191), // Default
        };
        Self {
            model_id: model.to_string(),
            provider: "openai".to_string(),
            dimensions,
            max_tokens,
            version: "1".to_string(),
        }
    }

    /// Create info for common open-source models
    pub fn open_source(model: &str) -> Self {
        let (dimensions, max_tokens) = match model {
            "all-MiniLM-L6-v2" => (384, 256),
            "all-mpnet-base-v2" => (768, 384),
            "bge-small-en-v1.5" => (384, 512),
            "bge-base-en-v1.5" => (768, 512),
            "bge-large-en-v1.5" => (1024, 512),
            "e5-small-v2" => (384, 512),
            "e5-base-v2" => (768, 512),
            "e5-large-v2" => (1024, 512),
            "gte-small" => (384, 512),
            "gte-base" => (768, 512),
            "gte-large" => (1024, 512),
            _ => (768, 512), // Default
        };
        Self {
            model_id: model.to_string(),
            provider: "huggingface".to_string(),
            dimensions,
            max_tokens,
            version: "1".to_string(),
        }
    }
}

/// Chunking strategy for splitting documents before embedding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkingStrategy {
    /// Fixed token count per chunk
    FixedTokens { size: u32, overlap: u32 },
    /// Split by sentences with max size
    Sentence { max_tokens: u32, overlap_sentences: u32 },
    /// Split by paragraphs
    Paragraph { max_tokens: u32 },
    /// Semantic chunking (split at topic boundaries)
    Semantic { max_tokens: u32 },
}

impl Default for ChunkingStrategy {
    fn default() -> Self {
        Self::FixedTokens {
            size: 512,
            overlap: 50,
        }
    }
}

/// Chunk text into segments suitable for embedding
///
/// Simple implementation - for production use, consider using a proper tokenizer
pub fn chunk_text(text: &str, strategy: ChunkingStrategy) -> Vec<String> {
    match strategy {
        ChunkingStrategy::FixedTokens { size, overlap } => {
            // Approximate tokens by splitting on whitespace
            let words: Vec<&str> = text.split_whitespace().collect();
            let size = size as usize;
            let overlap = overlap as usize;
            let step = size.saturating_sub(overlap).max(1);

            let mut chunks = Vec::new();
            let mut start = 0;

            while start < words.len() {
                let end = (start + size).min(words.len());
                chunks.push(words[start..end].join(" "));
                start += step;
            }

            chunks
        }
        ChunkingStrategy::Sentence { max_tokens, overlap_sentences } => {
            // Split by sentence-ending punctuation
            let sentences: Vec<&str> = text
                .split(|c| c == '.' || c == '!' || c == '?')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();

            let max_tokens = max_tokens as usize;
            let overlap = overlap_sentences as usize;
            let mut chunks = Vec::new();
            let mut current_chunk = Vec::new();
            let mut current_len = 0;

            for sentence in &sentences {
                let sentence_len = sentence.split_whitespace().count();

                if current_len + sentence_len > max_tokens && !current_chunk.is_empty() {
                    chunks.push(current_chunk.join(". ") + ".");
                    // Keep overlap sentences
                    let keep = current_chunk.len().saturating_sub(overlap);
                    current_chunk = current_chunk[keep..].to_vec();
                    current_len = current_chunk.iter().map(|s: &&str| s.split_whitespace().count()).sum();
                }

                current_chunk.push(*sentence);
                current_len += sentence_len;
            }

            if !current_chunk.is_empty() {
                chunks.push(current_chunk.join(". ") + ".");
            }

            chunks
        }
        ChunkingStrategy::Paragraph { max_tokens } => {
            let max_tokens = max_tokens as usize;
            let paragraphs: Vec<&str> = text
                .split("\n\n")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();

            let mut chunks = Vec::new();
            let mut current_chunk = String::new();
            let mut current_len = 0;

            for para in paragraphs {
                let para_len = para.split_whitespace().count();

                if current_len + para_len > max_tokens && !current_chunk.is_empty() {
                    chunks.push(current_chunk.trim().to_string());
                    current_chunk = String::new();
                    current_len = 0;
                }

                if !current_chunk.is_empty() {
                    current_chunk.push_str("\n\n");
                }
                current_chunk.push_str(para);
                current_len += para_len;
            }

            if !current_chunk.is_empty() {
                chunks.push(current_chunk.trim().to_string());
            }

            chunks
        }
        ChunkingStrategy::Semantic { max_tokens } => {
            // Simplified semantic chunking - split on heading patterns and paragraph breaks
            let max_tokens = max_tokens as usize;
            let mut chunks = Vec::new();
            let mut current_chunk = String::new();
            let mut current_len = 0;

            for line in text.lines() {
                let line = line.trim();
                let line_len = line.split_whitespace().count();

                // Check for heading patterns (start new chunk)
                let is_heading = line.starts_with('#')
                    || (line.len() < 100 && line.ends_with(':'))
                    || line.chars().all(|c| c.is_uppercase() || c.is_whitespace());

                if (is_heading || current_len + line_len > max_tokens) && !current_chunk.is_empty() {
                    chunks.push(current_chunk.trim().to_string());
                    current_chunk = String::new();
                    current_len = 0;
                }

                if !current_chunk.is_empty() && !line.is_empty() {
                    current_chunk.push('\n');
                }
                current_chunk.push_str(line);
                current_len += line_len;
            }

            if !current_chunk.is_empty() {
                chunks.push(current_chunk.trim().to_string());
            }

            chunks
        }
    }
}

/// Rerank results using cross-encoder style scoring
///
/// Takes initial retrieval results and reranks them based on query-document
/// relevance. This is typically more accurate than bi-encoder similarity
/// but slower, so used on top-k candidates.
///
/// # Arguments
/// * `query` - The query text
/// * `documents` - Candidate documents with their IDs
/// * `scores` - Pre-computed relevance scores from a reranker model
/// * `k` - Number of results to return
pub fn rerank_results(
    _query: &str,
    documents: &[(u64, String)],
    scores: &[f32],
    k: usize,
) -> Vec<(u64, f32)> {
    assert_eq!(documents.len(), scores.len(), "Documents and scores must have same length");

    let mut ranked: Vec<(u64, f32)> = documents
        .iter()
        .zip(scores.iter())
        .map(|((id, _), score)| (*id, *score))
        .collect();

    // Sort by score descending
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(k);
    ranked
}

/// Compute similarity matrix between two sets of vectors
/// Useful for clustering, deduplication, and batch similarity checks
pub fn similarity_matrix(
    vectors_a: &[Vec<f32>],
    vectors_b: &[Vec<f32>],
    metric: DistanceMetric,
) -> Vec<Vec<f32>> {
    vectors_a
        .par_iter()
        .map(|a| {
            vectors_b
                .iter()
                .map(|b| {
                    match metric {
                        DistanceMetric::Cosine => cosine_similarity(a, b),
                        _ => -compute_distance(a, b, metric), // Negate distance for similarity
                    }
                })
                .collect()
        })
        .collect()
}

/// Find near-duplicate vectors using similarity threshold
/// Returns pairs of indices that are above the similarity threshold
pub fn find_near_duplicates(
    vectors: &[Vec<f32>],
    threshold: f32,
    metric: DistanceMetric,
) -> Vec<(usize, usize, f32)> {
    let n = vectors.len();
    let mut duplicates = Vec::new();

    // Parallel comparison of all pairs
    let pairs: Vec<_> = (0..n)
        .flat_map(|i| (i + 1..n).map(move |j| (i, j)))
        .collect();

    let results: Vec<_> = pairs
        .par_iter()
        .filter_map(|(i, j)| {
            let sim = match metric {
                DistanceMetric::Cosine => cosine_similarity(&vectors[*i], &vectors[*j]),
                _ => {
                    let dist = compute_distance(&vectors[*i], &vectors[*j], metric);
                    1.0 / (1.0 + dist) // Convert distance to similarity
                }
            };
            if sim >= threshold {
                Some((*i, *j, sim))
            } else {
                None
            }
        })
        .collect();

    duplicates.extend(results);
    duplicates
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
            vec![1.0, 0.0], // Index 0 - exact match
            vec![0.0, 1.0], // Index 1 - distance sqrt(2)
            vec![1.0, 1.0], // Index 2 - distance 1.0
            vec![2.0, 0.0], // Index 3 - distance 1.0
        ];
        let query = vec![1.0, 0.0];

        let results = knn_search(&query, &vectors, 2, DistanceMetric::Euclidean);

        assert_eq!(results.len(), 2);
        // First result should have near-zero distance (the exact match at index 0)
        assert!(
            results[0].score < 0.001,
            "First result score should be ~0, got {}",
            results[0].score
        );
        // Second result should be one of the vectors at distance 1.0 (index 2 or 3)
        assert!(
            (results[1].score - 1.0).abs() < 0.001,
            "Second result score should be ~1.0, got {}",
            results[1].score
        );
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
        let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];

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
        let config = VectorIndexConfig {
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

    // =========================================================
    // AI Data Feature Tests
    // =========================================================

    #[test]
    fn test_filtered_vector_search() {
        // Simulate pre-filtered candidates (e.g., WHERE category = 'AI')
        let candidates: Vec<(u64, Vec<f32>)> = vec![
            (1, vec![1.0, 0.0, 0.0]),
            (5, vec![0.9, 0.1, 0.0]),
            (10, vec![0.0, 1.0, 0.0]),
            (15, vec![0.5, 0.5, 0.0]),
        ];

        let query = vec![1.0, 0.0, 0.0];
        let results = filtered_vector_search(&query, &candidates, 2, DistanceMetric::Cosine);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_id, 1); // Exact match
        assert_eq!(results[1].row_id, 5); // Second closest
    }

    #[test]
    fn test_hybrid_search_rrf() {
        // Vector results (sorted by distance, lower = better)
        let vector_results: Vec<(u64, f32)> = vec![
            (1, 0.1),  // Rank 1
            (2, 0.2),  // Rank 2
            (3, 0.5),  // Rank 3
        ];

        // Text/BM25 results (sorted by score, higher = better)
        let text_results: Vec<(u64, f32)> = vec![
            (3, 10.0), // Rank 1
            (1, 8.0),  // Rank 2
            (4, 5.0),  // Rank 3
        ];

        let config = HybridSearchConfig::default();
        let results = hybrid_search(&vector_results, &text_results, 3, &config);

        assert_eq!(results.len(), 3);
        // Doc 1 should rank high (good in both)
        // Doc 3 should also rank high (best text, ok vector)
        assert!(results.iter().any(|r| r.row_id == 1));
        assert!(results.iter().any(|r| r.row_id == 3));
    }

    #[test]
    fn test_chunk_text_fixed_tokens() {
        let text = "This is a test. It has multiple sentences. We want to chunk it properly.";
        let chunks = chunk_text(text, ChunkingStrategy::FixedTokens { size: 5, overlap: 2 });

        assert!(!chunks.is_empty());
        // Each chunk should have roughly 5 words
        for chunk in &chunks {
            let word_count = chunk.split_whitespace().count();
            assert!(word_count <= 6); // Allow some flexibility
        }
    }

    #[test]
    fn test_chunk_text_sentences() {
        let text = "First sentence here. Second sentence follows. Third one too. And a fourth.";
        let chunks = chunk_text(
            text,
            ChunkingStrategy::Sentence {
                max_tokens: 10,
                overlap_sentences: 0,
            },
        );

        assert!(!chunks.is_empty());
        // Chunks should end with periods
        for chunk in &chunks {
            assert!(chunk.ends_with('.'));
        }
    }

    #[test]
    fn test_embedding_model_info() {
        let openai = EmbeddingModelInfo::openai("text-embedding-3-small");
        assert_eq!(openai.dimensions, 1536);
        assert_eq!(openai.provider, "openai");

        let oss = EmbeddingModelInfo::open_source("all-MiniLM-L6-v2");
        assert_eq!(oss.dimensions, 384);
        assert_eq!(oss.provider, "huggingface");
    }

    #[test]
    fn test_similarity_matrix() {
        let vectors_a = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
        ];
        let vectors_b = vec![
            vec![1.0, 0.0],
            vec![0.5, 0.5],
        ];

        let matrix = similarity_matrix(&vectors_a, &vectors_b, DistanceMetric::Cosine);

        assert_eq!(matrix.len(), 2);
        assert_eq!(matrix[0].len(), 2);
        // First vector of A should match first vector of B perfectly
        assert!((matrix[0][0] - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_find_near_duplicates() {
        let vectors = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.99, 0.01, 0.0], // Near duplicate of first
            vec![0.0, 1.0, 0.0],   // Different
            vec![0.0, 0.99, 0.01], // Near duplicate of third
        ];

        let duplicates = find_near_duplicates(&vectors, 0.95, DistanceMetric::Cosine);

        // Should find pairs (0,1) and (2,3) as near duplicates
        assert!(duplicates.len() >= 2);
        assert!(duplicates.iter().any(|(i, j, _)| (*i == 0 && *j == 1)));
        assert!(duplicates.iter().any(|(i, j, _)| (*i == 2 && *j == 3)));
    }
}
