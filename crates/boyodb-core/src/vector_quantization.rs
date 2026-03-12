//! Vector Quantization for Efficient Similarity Search
//!
//! Implements various quantization techniques to compress high-dimensional vectors:
//! - Product Quantization (PQ): Divide vectors into subspaces and quantize each
//! - Scalar Quantization (SQ): Quantize each dimension independently
//! - Optimized Product Quantization (OPQ): Rotation-optimized PQ
//! - Binary Quantization: 1-bit per dimension for ultra-fast distance

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Common Types
// ============================================================================

/// Error type for quantization operations
#[derive(Debug, Clone)]
pub enum QuantizationError {
    /// Invalid dimensions
    InvalidDimensions { expected: usize, got: usize },
    /// Training failed
    TrainingFailed(String),
    /// Not trained
    NotTrained,
    /// Invalid parameters
    InvalidParameters(String),
    /// Index out of bounds
    IndexOutOfBounds(usize),
}

impl std::fmt::Display for QuantizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDimensions { expected, got } => {
                write!(f, "invalid dimensions: expected {}, got {}", expected, got)
            }
            Self::TrainingFailed(msg) => write!(f, "training failed: {}", msg),
            Self::NotTrained => write!(f, "quantizer not trained"),
            Self::InvalidParameters(msg) => write!(f, "invalid parameters: {}", msg),
            Self::IndexOutOfBounds(idx) => write!(f, "index out of bounds: {}", idx),
        }
    }
}

impl std::error::Error for QuantizationError {}

/// Distance metric
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    L2,
    InnerProduct,
    Cosine,
}

/// Quantization type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantizationType {
    ProductQuantization,
    ScalarQuantization,
    OptimizedProductQuantization,
    BinaryQuantization,
}

// ============================================================================
// Scalar Quantization (SQ)
// ============================================================================

/// Scalar quantization configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScalarQuantizerConfig {
    /// Number of dimensions
    pub dimensions: usize,
    /// Bits per dimension (4 or 8 typically)
    pub bits: u8,
    /// Distance metric
    pub metric: DistanceMetric,
}

impl Default for ScalarQuantizerConfig {
    fn default() -> Self {
        Self {
            dimensions: 128,
            bits: 8,
            metric: DistanceMetric::L2,
        }
    }
}

/// Scalar quantizer state
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScalarQuantizer {
    config: ScalarQuantizerConfig,
    /// Min value per dimension
    mins: Vec<f32>,
    /// Max value per dimension
    maxs: Vec<f32>,
    /// Scale factors
    scales: Vec<f32>,
    /// Whether trained
    trained: bool,
}

impl ScalarQuantizer {
    pub fn new(config: ScalarQuantizerConfig) -> Self {
        let dims = config.dimensions;
        Self {
            config,
            mins: vec![f32::MAX; dims],
            maxs: vec![f32::MIN; dims],
            scales: vec![1.0; dims],
            trained: false,
        }
    }

    /// Train the quantizer on sample vectors
    pub fn train(&mut self, vectors: &[Vec<f32>]) -> Result<(), QuantizationError> {
        if vectors.is_empty() {
            return Err(QuantizationError::TrainingFailed("empty training set".into()));
        }

        let dims = self.config.dimensions;

        // Reset stats
        self.mins = vec![f32::MAX; dims];
        self.maxs = vec![f32::MIN; dims];

        // Find min/max per dimension
        for vec in vectors {
            if vec.len() != dims {
                return Err(QuantizationError::InvalidDimensions {
                    expected: dims,
                    got: vec.len(),
                });
            }

            for (i, &v) in vec.iter().enumerate() {
                self.mins[i] = self.mins[i].min(v);
                self.maxs[i] = self.maxs[i].max(v);
            }
        }

        // Compute scales
        let num_levels = (1 << self.config.bits) as f32;
        for i in 0..dims {
            let range = self.maxs[i] - self.mins[i];
            self.scales[i] = if range > 0.0 {
                (num_levels - 1.0) / range
            } else {
                1.0
            };
        }

        self.trained = true;
        Ok(())
    }

    /// Encode a vector to quantized codes
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if vector.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }

        let mut codes = Vec::with_capacity(self.config.dimensions);

        for (i, &v) in vector.iter().enumerate() {
            let normalized = (v - self.mins[i]) * self.scales[i];
            let quantized = normalized.round().clamp(0.0, 255.0) as u8;
            codes.push(quantized);
        }

        Ok(codes)
    }

    /// Decode quantized codes back to approximate vector
    pub fn decode(&self, codes: &[u8]) -> Result<Vec<f32>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if codes.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: codes.len(),
            });
        }

        let mut vector = Vec::with_capacity(self.config.dimensions);

        for (i, &code) in codes.iter().enumerate() {
            let v = self.mins[i] + (code as f32) / self.scales[i];
            vector.push(v);
        }

        Ok(vector)
    }

    /// Compute distance between encoded vector and query
    pub fn asymmetric_distance(&self, codes: &[u8], query: &[f32]) -> Result<f32, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        let decoded = self.decode(codes)?;

        match self.config.metric {
            DistanceMetric::L2 => {
                let dist: f32 = decoded.iter()
                    .zip(query.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                Ok(dist)
            }
            DistanceMetric::InnerProduct => {
                let ip: f32 = decoded.iter()
                    .zip(query.iter())
                    .map(|(a, b)| a * b)
                    .sum();
                Ok(-ip) // Negate so lower is better
            }
            DistanceMetric::Cosine => {
                let mut dot = 0.0f32;
                let mut norm_a = 0.0f32;
                let mut norm_b = 0.0f32;
                for (a, b) in decoded.iter().zip(query.iter()) {
                    dot += a * b;
                    norm_a += a * a;
                    norm_b += b * b;
                }
                let denom = (norm_a * norm_b).sqrt();
                if denom > 0.0 {
                    Ok(1.0 - dot / denom)
                } else {
                    Ok(1.0)
                }
            }
        }
    }

    /// Memory usage per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        if self.config.bits == 8 {
            self.config.dimensions
        } else {
            (self.config.dimensions * self.config.bits as usize + 7) / 8
        }
    }
}

// ============================================================================
// Product Quantization (PQ)
// ============================================================================

/// Product quantization configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProductQuantizerConfig {
    /// Total dimensions
    pub dimensions: usize,
    /// Number of subspaces (M)
    pub num_subspaces: usize,
    /// Number of centroids per subspace (K, typically 256)
    pub num_centroids: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// K-means iterations for training
    pub kmeans_iterations: usize,
}

impl Default for ProductQuantizerConfig {
    fn default() -> Self {
        Self {
            dimensions: 128,
            num_subspaces: 8,
            num_centroids: 256,
            metric: DistanceMetric::L2,
            kmeans_iterations: 25,
        }
    }
}

/// Product quantizer
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProductQuantizer {
    config: ProductQuantizerConfig,
    /// Centroids for each subspace: [subspace][centroid][dim]
    centroids: Vec<Vec<Vec<f32>>>,
    /// Dimensions per subspace
    dims_per_subspace: usize,
    /// Whether trained
    trained: bool,
}

impl ProductQuantizer {
    pub fn new(config: ProductQuantizerConfig) -> Result<Self, QuantizationError> {
        if config.dimensions % config.num_subspaces != 0 {
            return Err(QuantizationError::InvalidParameters(
                "dimensions must be divisible by num_subspaces".into()
            ));
        }

        let dims_per_subspace = config.dimensions / config.num_subspaces;

        Ok(Self {
            config,
            centroids: Vec::new(),
            dims_per_subspace,
            trained: false,
        })
    }

    /// Train on sample vectors
    pub fn train(&mut self, vectors: &[Vec<f32>]) -> Result<(), QuantizationError> {
        if vectors.is_empty() {
            return Err(QuantizationError::TrainingFailed("empty training set".into()));
        }

        for vec in vectors {
            if vec.len() != self.config.dimensions {
                return Err(QuantizationError::InvalidDimensions {
                    expected: self.config.dimensions,
                    got: vec.len(),
                });
            }
        }

        self.centroids = Vec::with_capacity(self.config.num_subspaces);

        // Train each subspace independently
        for m in 0..self.config.num_subspaces {
            let start = m * self.dims_per_subspace;
            let end = start + self.dims_per_subspace;

            // Extract subvectors
            let subvectors: Vec<Vec<f32>> = vectors.iter()
                .map(|v| v[start..end].to_vec())
                .collect();

            // K-means clustering
            let centroids = self.kmeans(&subvectors, self.config.num_centroids)?;
            self.centroids.push(centroids);
        }

        self.trained = true;
        Ok(())
    }

    /// Simple k-means clustering
    fn kmeans(&self, vectors: &[Vec<f32>], k: usize) -> Result<Vec<Vec<f32>>, QuantizationError> {
        if vectors.is_empty() {
            return Err(QuantizationError::TrainingFailed("empty vectors".into()));
        }

        let dim = vectors[0].len();
        let n = vectors.len();

        // Initialize centroids (k-means++ style)
        let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
        centroids.push(vectors[0].clone());

        for _ in 1..k.min(n) {
            // Find farthest point from existing centroids
            let mut max_dist = 0.0f32;
            let mut max_idx = 0;

            for (i, v) in vectors.iter().enumerate() {
                let min_dist = centroids.iter()
                    .map(|c| Self::l2_squared(v, c))
                    .fold(f32::MAX, f32::min);

                if min_dist > max_dist {
                    max_dist = min_dist;
                    max_idx = i;
                }
            }
            centroids.push(vectors[max_idx].clone());
        }

        // Pad with random if needed
        while centroids.len() < k {
            let idx = centroids.len() % n;
            centroids.push(vectors[idx].clone());
        }

        // K-means iterations
        let mut assignments = vec![0usize; n];

        for _ in 0..self.config.kmeans_iterations {
            // Assign points to nearest centroid
            for (i, v) in vectors.iter().enumerate() {
                let mut min_dist = f32::MAX;
                let mut min_idx = 0;

                for (j, c) in centroids.iter().enumerate() {
                    let dist = Self::l2_squared(v, c);
                    if dist < min_dist {
                        min_dist = dist;
                        min_idx = j;
                    }
                }
                assignments[i] = min_idx;
            }

            // Update centroids
            let mut counts = vec![0usize; k];
            let mut sums = vec![vec![0.0f32; dim]; k];

            for (i, v) in vectors.iter().enumerate() {
                let c = assignments[i];
                counts[c] += 1;
                for (j, &val) in v.iter().enumerate() {
                    sums[c][j] += val;
                }
            }

            for c in 0..k {
                if counts[c] > 0 {
                    for j in 0..dim {
                        centroids[c][j] = sums[c][j] / counts[c] as f32;
                    }
                }
            }
        }

        Ok(centroids)
    }

    fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
    }

    /// Encode a vector
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if vector.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }

        let mut codes = Vec::with_capacity(self.config.num_subspaces);

        for m in 0..self.config.num_subspaces {
            let start = m * self.dims_per_subspace;
            let end = start + self.dims_per_subspace;
            let subvector = &vector[start..end];

            // Find nearest centroid
            let mut min_dist = f32::MAX;
            let mut min_idx = 0u8;

            for (i, centroid) in self.centroids[m].iter().enumerate() {
                let dist = Self::l2_squared(subvector, centroid);
                if dist < min_dist {
                    min_dist = dist;
                    min_idx = i as u8;
                }
            }
            codes.push(min_idx);
        }

        Ok(codes)
    }

    /// Decode codes to approximate vector
    pub fn decode(&self, codes: &[u8]) -> Result<Vec<f32>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if codes.len() != self.config.num_subspaces {
            return Err(QuantizationError::InvalidParameters(
                "codes length must equal num_subspaces".into()
            ));
        }

        let mut vector = Vec::with_capacity(self.config.dimensions);

        for (m, &code) in codes.iter().enumerate() {
            let centroid = &self.centroids[m][code as usize];
            vector.extend_from_slice(centroid);
        }

        Ok(vector)
    }

    /// Precompute distance table for asymmetric search
    pub fn compute_distance_table(&self, query: &[f32]) -> Result<Vec<Vec<f32>>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if query.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: query.len(),
            });
        }

        let mut table = Vec::with_capacity(self.config.num_subspaces);

        for m in 0..self.config.num_subspaces {
            let start = m * self.dims_per_subspace;
            let end = start + self.dims_per_subspace;
            let subquery = &query[start..end];

            let distances: Vec<f32> = self.centroids[m].iter()
                .map(|c| {
                    match self.config.metric {
                        DistanceMetric::L2 => Self::l2_squared(subquery, c),
                        DistanceMetric::InnerProduct => {
                            -subquery.iter().zip(c.iter()).map(|(a, b)| a * b).sum::<f32>()
                        }
                        DistanceMetric::Cosine => {
                            let dot: f32 = subquery.iter().zip(c.iter()).map(|(a, b)| a * b).sum();
                            let norm_q: f32 = subquery.iter().map(|x| x * x).sum::<f32>().sqrt();
                            let norm_c: f32 = c.iter().map(|x| x * x).sum::<f32>().sqrt();
                            if norm_q > 0.0 && norm_c > 0.0 {
                                1.0 - dot / (norm_q * norm_c)
                            } else {
                                1.0
                            }
                        }
                    }
                })
                .collect();

            table.push(distances);
        }

        Ok(table)
    }

    /// Fast distance lookup using precomputed table
    pub fn lookup_distance(&self, table: &[Vec<f32>], codes: &[u8]) -> f32 {
        codes.iter().enumerate()
            .map(|(m, &code)| table[m][code as usize])
            .sum()
    }

    /// Memory usage per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        self.config.num_subspaces
    }
}

// ============================================================================
// Binary Quantization
// ============================================================================

/// Binary quantization configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinaryQuantizerConfig {
    /// Number of dimensions
    pub dimensions: usize,
    /// Threshold per dimension (or use median)
    pub thresholds: Option<Vec<f32>>,
}

impl Default for BinaryQuantizerConfig {
    fn default() -> Self {
        Self {
            dimensions: 128,
            thresholds: None,
        }
    }
}

/// Binary quantizer (1-bit per dimension)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinaryQuantizer {
    config: BinaryQuantizerConfig,
    thresholds: Vec<f32>,
    trained: bool,
}

impl BinaryQuantizer {
    pub fn new(config: BinaryQuantizerConfig) -> Self {
        let trained = config.thresholds.is_some();
        let thresholds = config.thresholds.clone().unwrap_or_else(|| vec![0.0; config.dimensions]);
        Self {
            config,
            thresholds,
            trained,
        }
    }

    /// Train using median thresholds
    pub fn train(&mut self, vectors: &[Vec<f32>]) -> Result<(), QuantizationError> {
        if vectors.is_empty() {
            return Err(QuantizationError::TrainingFailed("empty training set".into()));
        }

        let dims = self.config.dimensions;
        self.thresholds = Vec::with_capacity(dims);

        for d in 0..dims {
            let mut values: Vec<f32> = vectors.iter()
                .filter_map(|v| v.get(d).copied())
                .collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let median = if values.len() % 2 == 0 {
                let mid = values.len() / 2;
                (values[mid - 1] + values[mid]) / 2.0
            } else {
                values[values.len() / 2]
            };

            self.thresholds.push(median);
        }

        self.trained = true;
        Ok(())
    }

    /// Encode vector to bits
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u64>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if vector.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }

        let num_words = (self.config.dimensions + 63) / 64;
        let mut bits = vec![0u64; num_words];

        for (i, (&v, &threshold)) in vector.iter().zip(self.thresholds.iter()).enumerate() {
            if v >= threshold {
                bits[i / 64] |= 1u64 << (i % 64);
            }
        }

        Ok(bits)
    }

    /// Hamming distance between two encoded vectors
    pub fn hamming_distance(a: &[u64], b: &[u64]) -> u32 {
        a.iter().zip(b.iter())
            .map(|(x, y)| (x ^ y).count_ones())
            .sum()
    }

    /// Memory usage per vector in bytes
    pub fn bytes_per_vector(&self) -> usize {
        (self.config.dimensions + 7) / 8
    }
}

// ============================================================================
// Optimized Product Quantization (OPQ)
// ============================================================================

/// OPQ configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpqConfig {
    /// PQ configuration
    pub pq_config: ProductQuantizerConfig,
    /// Number of OPQ training iterations
    pub opq_iterations: usize,
}

impl Default for OpqConfig {
    fn default() -> Self {
        Self {
            pq_config: ProductQuantizerConfig::default(),
            opq_iterations: 10,
        }
    }
}

/// Optimized Product Quantizer
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptimizedProductQuantizer {
    config: OpqConfig,
    /// Rotation matrix (dimensions x dimensions)
    rotation: Vec<Vec<f32>>,
    /// Underlying PQ
    pq: ProductQuantizer,
    trained: bool,
}

impl OptimizedProductQuantizer {
    pub fn new(config: OpqConfig) -> Result<Self, QuantizationError> {
        let dims = config.pq_config.dimensions;
        let pq = ProductQuantizer::new(config.pq_config.clone())?;

        // Initialize rotation as identity matrix
        let mut rotation = vec![vec![0.0f32; dims]; dims];
        for i in 0..dims {
            rotation[i][i] = 1.0;
        }

        Ok(Self {
            config,
            rotation,
            pq,
            trained: false,
        })
    }

    /// Apply rotation to vector
    fn rotate(&self, vector: &[f32]) -> Vec<f32> {
        let dims = vector.len();
        let mut rotated = vec![0.0f32; dims];

        for i in 0..dims {
            for j in 0..dims {
                rotated[i] += self.rotation[i][j] * vector[j];
            }
        }

        rotated
    }

    /// Train OPQ
    pub fn train(&mut self, vectors: &[Vec<f32>]) -> Result<(), QuantizationError> {
        if vectors.is_empty() {
            return Err(QuantizationError::TrainingFailed("empty training set".into()));
        }

        // For each OPQ iteration:
        // 1. Rotate vectors
        // 2. Train PQ on rotated vectors
        // 3. Update rotation matrix

        let mut rotated: Vec<Vec<f32>> = vectors.to_vec();

        for _ in 0..self.config.opq_iterations {
            // Train PQ on current rotated vectors
            self.pq.train(&rotated)?;

            // Encode and decode to get reconstructions
            let _reconstructed: Vec<Vec<f32>> = rotated.iter()
                .filter_map(|v| {
                    self.pq.encode(v).ok().and_then(|c| self.pq.decode(&c).ok())
                })
                .collect();

            // In a full implementation, we would compute optimal rotation
            // using SVD. For now, we keep the identity rotation.

            // Re-rotate original vectors
            rotated = vectors.iter().map(|v| self.rotate(v)).collect();
        }

        // Final PQ training
        self.pq.train(&rotated)?;
        self.trained = true;

        Ok(())
    }

    /// Encode vector
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        let rotated = self.rotate(vector);
        self.pq.encode(&rotated)
    }

    /// Decode to approximate vector
    pub fn decode(&self, codes: &[u8]) -> Result<Vec<f32>, QuantizationError> {
        // Note: In full implementation, we'd apply inverse rotation
        self.pq.decode(codes)
    }
}

// ============================================================================
// Quantized Vector Index
// ============================================================================

/// Configuration for quantized index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuantizedIndexConfig {
    /// Quantization type
    pub quantization_type: QuantizationType,
    /// Dimensions
    pub dimensions: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// Number of PQ subspaces (for PQ/OPQ)
    pub pq_subspaces: usize,
    /// Bits per dimension (for SQ)
    pub sq_bits: u8,
    /// Training sample size
    pub training_sample_size: usize,
    /// Use residual quantization
    pub use_residuals: bool,
}

impl Default for QuantizedIndexConfig {
    fn default() -> Self {
        Self {
            quantization_type: QuantizationType::ProductQuantization,
            dimensions: 128,
            metric: DistanceMetric::L2,
            pq_subspaces: 8,
            sq_bits: 8,
            training_sample_size: 10000,
            use_residuals: false,
        }
    }
}

/// Stored vector entry
#[derive(Clone, Debug)]
struct StoredVector {
    id: u64,
    codes: Vec<u8>,
    metadata: Option<HashMap<String, String>>,
}

/// Quantized vector index
pub struct QuantizedIndex {
    config: QuantizedIndexConfig,
    /// Scalar quantizer
    sq: Option<ScalarQuantizer>,
    /// Product quantizer
    pq: Option<ProductQuantizer>,
    /// Binary quantizer
    bq: Option<BinaryQuantizer>,
    /// OPQ
    opq: Option<OptimizedProductQuantizer>,
    /// Stored vectors
    vectors: Vec<StoredVector>,
    /// ID to index mapping
    id_map: HashMap<u64, usize>,
    /// Training vectors (before training)
    training_buffer: Vec<Vec<f32>>,
    /// Whether trained
    trained: bool,
}

impl QuantizedIndex {
    pub fn new(config: QuantizedIndexConfig) -> Result<Self, QuantizationError> {
        let sq = match config.quantization_type {
            QuantizationType::ScalarQuantization => Some(ScalarQuantizer::new(ScalarQuantizerConfig {
                dimensions: config.dimensions,
                bits: config.sq_bits,
                metric: config.metric,
            })),
            _ => None,
        };

        let pq = match config.quantization_type {
            QuantizationType::ProductQuantization => Some(ProductQuantizer::new(ProductQuantizerConfig {
                dimensions: config.dimensions,
                num_subspaces: config.pq_subspaces,
                num_centroids: 256,
                metric: config.metric,
                kmeans_iterations: 25,
            })?),
            _ => None,
        };

        let bq = match config.quantization_type {
            QuantizationType::BinaryQuantization => Some(BinaryQuantizer::new(BinaryQuantizerConfig {
                dimensions: config.dimensions,
                thresholds: None,
            })),
            _ => None,
        };

        let opq = match config.quantization_type {
            QuantizationType::OptimizedProductQuantization => Some(OptimizedProductQuantizer::new(OpqConfig {
                pq_config: ProductQuantizerConfig {
                    dimensions: config.dimensions,
                    num_subspaces: config.pq_subspaces,
                    num_centroids: 256,
                    metric: config.metric,
                    kmeans_iterations: 25,
                },
                opq_iterations: 10,
            })?),
            _ => None,
        };

        Ok(Self {
            config,
            sq,
            pq,
            bq,
            opq,
            vectors: Vec::new(),
            id_map: HashMap::new(),
            training_buffer: Vec::new(),
            trained: false,
        })
    }

    /// Add vector (buffers for training if not yet trained)
    pub fn add(&mut self, id: u64, vector: Vec<f32>, metadata: Option<HashMap<String, String>>) -> Result<(), QuantizationError> {
        if vector.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: vector.len(),
            });
        }

        if !self.trained {
            self.training_buffer.push(vector);

            // Auto-train when buffer is full
            if self.training_buffer.len() >= self.config.training_sample_size {
                self.train()?;
            }
            return Ok(());
        }

        let codes = self.encode(&vector)?;
        let idx = self.vectors.len();

        self.vectors.push(StoredVector {
            id,
            codes,
            metadata,
        });
        self.id_map.insert(id, idx);

        Ok(())
    }

    /// Train the quantizer
    pub fn train(&mut self) -> Result<(), QuantizationError> {
        if self.training_buffer.is_empty() {
            return Err(QuantizationError::TrainingFailed("no training data".into()));
        }

        match self.config.quantization_type {
            QuantizationType::ScalarQuantization => {
                if let Some(ref mut sq) = self.sq {
                    sq.train(&self.training_buffer)?;
                }
            }
            QuantizationType::ProductQuantization => {
                if let Some(ref mut pq) = self.pq {
                    pq.train(&self.training_buffer)?;
                }
            }
            QuantizationType::BinaryQuantization => {
                if let Some(ref mut bq) = self.bq {
                    bq.train(&self.training_buffer)?;
                }
            }
            QuantizationType::OptimizedProductQuantization => {
                if let Some(ref mut opq) = self.opq {
                    opq.train(&self.training_buffer)?;
                }
            }
        }

        self.trained = true;

        // Now encode buffered vectors
        let buffer = std::mem::take(&mut self.training_buffer);
        for (i, vec) in buffer.into_iter().enumerate() {
            let codes = self.encode(&vec)?;
            self.vectors.push(StoredVector {
                id: i as u64,
                codes,
                metadata: None,
            });
            self.id_map.insert(i as u64, i);
        }

        Ok(())
    }

    /// Encode a vector
    fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, QuantizationError> {
        match self.config.quantization_type {
            QuantizationType::ScalarQuantization => {
                self.sq.as_ref().unwrap().encode(vector)
            }
            QuantizationType::ProductQuantization => {
                self.pq.as_ref().unwrap().encode(vector)
            }
            QuantizationType::BinaryQuantization => {
                let bits = self.bq.as_ref().unwrap().encode(vector)?;
                // Convert u64 to bytes
                Ok(bits.iter().flat_map(|w| w.to_le_bytes()).collect())
            }
            QuantizationType::OptimizedProductQuantization => {
                self.opq.as_ref().unwrap().encode(vector)
            }
        }
    }

    /// Search for nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, QuantizationError> {
        if !self.trained {
            return Err(QuantizationError::NotTrained);
        }

        if query.len() != self.config.dimensions {
            return Err(QuantizationError::InvalidDimensions {
                expected: self.config.dimensions,
                got: query.len(),
            });
        }

        // Compute distances to all vectors
        let mut results: Vec<(u64, f32)> = match self.config.quantization_type {
            QuantizationType::ProductQuantization => {
                let pq = self.pq.as_ref().unwrap();
                let table = pq.compute_distance_table(query)?;

                self.vectors.iter()
                    .map(|v| (v.id, pq.lookup_distance(&table, &v.codes)))
                    .collect()
            }
            QuantizationType::ScalarQuantization => {
                let sq = self.sq.as_ref().unwrap();

                self.vectors.iter()
                    .map(|v| {
                        let dist = sq.asymmetric_distance(&v.codes, query).unwrap_or(f32::MAX);
                        (v.id, dist)
                    })
                    .collect()
            }
            QuantizationType::BinaryQuantization => {
                let bq = self.bq.as_ref().unwrap();
                let query_bits = bq.encode(query)?;

                self.vectors.iter()
                    .map(|v| {
                        // Convert bytes back to u64
                        let vec_bits: Vec<u64> = v.codes.chunks(8)
                            .map(|chunk| {
                                let mut bytes = [0u8; 8];
                                bytes[..chunk.len()].copy_from_slice(chunk);
                                u64::from_le_bytes(bytes)
                            })
                            .collect();

                        let dist = BinaryQuantizer::hamming_distance(&query_bits, &vec_bits);
                        (v.id, dist as f32)
                    })
                    .collect()
            }
            QuantizationType::OptimizedProductQuantization => {
                let opq = self.opq.as_ref().unwrap();
                // Use underlying PQ for search
                let table = opq.pq.compute_distance_table(query)?;

                self.vectors.iter()
                    .map(|v| (v.id, opq.pq.lookup_distance(&table, &v.codes)))
                    .collect()
            }
        };

        // Sort by distance and take top k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(results.into_iter()
            .take(k)
            .map(|(id, distance)| SearchResult { id, distance })
            .collect())
    }

    /// Get number of vectors
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        let bytes_per_vec = match self.config.quantization_type {
            QuantizationType::ScalarQuantization => {
                self.sq.as_ref().map(|sq| sq.bytes_per_vector()).unwrap_or(0)
            }
            QuantizationType::ProductQuantization => {
                self.pq.as_ref().map(|pq| pq.bytes_per_vector()).unwrap_or(0)
            }
            QuantizationType::BinaryQuantization => {
                self.bq.as_ref().map(|bq| bq.bytes_per_vector()).unwrap_or(0)
            }
            QuantizationType::OptimizedProductQuantization => {
                self.opq.as_ref().map(|opq| opq.pq.bytes_per_vector()).unwrap_or(0)
            }
        };

        self.vectors.len() * bytes_per_vec
    }
}

/// Search result
#[derive(Clone, Debug)]
pub struct SearchResult {
    pub id: u64,
    pub distance: f32,
}

// ============================================================================
// Quantization Statistics
// ============================================================================

/// Statistics for quantization quality
#[derive(Clone, Debug, Default)]
pub struct QuantizationStats {
    /// Total vectors
    pub vector_count: usize,
    /// Average distortion (reconstruction error)
    pub avg_distortion: f32,
    /// Max distortion
    pub max_distortion: f32,
    /// Compression ratio
    pub compression_ratio: f32,
    /// Training time in milliseconds
    pub training_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vectors(n: usize, dims: usize) -> Vec<Vec<f32>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        (0..n)
            .map(|i| {
                (0..dims)
                    .map(|j| {
                        let mut h = DefaultHasher::new();
                        (i * dims + j).hash(&mut h);
                        (h.finish() as f32 / u64::MAX as f32) * 2.0 - 1.0
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_scalar_quantizer() {
        let mut sq = ScalarQuantizer::new(ScalarQuantizerConfig {
            dimensions: 4,
            bits: 8,
            metric: DistanceMetric::L2,
        });

        let vectors = vec![
            vec![0.0, 0.5, 1.0, -1.0],
            vec![0.1, 0.6, 0.9, -0.8],
            vec![-0.1, 0.4, 1.1, -1.2],
        ];

        sq.train(&vectors).unwrap();

        let encoded = sq.encode(&vectors[0]).unwrap();
        let decoded = sq.decode(&encoded).unwrap();

        // Check reconstruction is close
        for (orig, dec) in vectors[0].iter().zip(decoded.iter()) {
            assert!((orig - dec).abs() < 0.1);
        }
    }

    #[test]
    fn test_product_quantizer() {
        let mut pq = ProductQuantizer::new(ProductQuantizerConfig {
            dimensions: 8,
            num_subspaces: 2,
            num_centroids: 4,
            metric: DistanceMetric::L2,
            kmeans_iterations: 5,
        }).unwrap();

        let vectors = random_vectors(100, 8);
        pq.train(&vectors).unwrap();

        let codes = pq.encode(&vectors[0]).unwrap();
        assert_eq!(codes.len(), 2);

        let decoded = pq.decode(&codes).unwrap();
        assert_eq!(decoded.len(), 8);
    }

    #[test]
    fn test_binary_quantizer() {
        let mut bq = BinaryQuantizer::new(BinaryQuantizerConfig {
            dimensions: 64,
            thresholds: None,
        });

        let vectors = random_vectors(100, 64);
        bq.train(&vectors).unwrap();

        let bits = bq.encode(&vectors[0]).unwrap();
        assert_eq!(bits.len(), 1); // 64 bits = 1 u64

        let dist = BinaryQuantizer::hamming_distance(&bits, &bits);
        assert_eq!(dist, 0);
    }

    #[test]
    fn test_quantized_index() {
        let mut index = QuantizedIndex::new(QuantizedIndexConfig {
            quantization_type: QuantizationType::ScalarQuantization,
            dimensions: 8,
            metric: DistanceMetric::L2,
            pq_subspaces: 2,
            sq_bits: 8,
            training_sample_size: 50,
            use_residuals: false,
        }).unwrap();

        let vectors = random_vectors(100, 8);

        for (i, v) in vectors.iter().enumerate() {
            index.add(i as u64, v.clone(), None).unwrap();
        }

        let results = index.search(&vectors[0], 5).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].id, 0); // Should find itself
    }
}
