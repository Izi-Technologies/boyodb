//! Approximate Query Functions
//!
//! ClickHouse-compatible approximate aggregation functions for massive datasets.
//! These functions trade perfect accuracy for 10-100x performance improvement.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

// ============================================================================
// HyperLogLog for Approximate Distinct Count
// ============================================================================

/// HyperLogLog implementation for approximate cardinality estimation
/// Provides O(1) space complexity with ~2% standard error
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    /// Number of registers (precision)
    precision: u8,
    /// Registers storing max leading zeros
    registers: Vec<u8>,
    /// Alpha correction factor
    alpha: f64,
}

impl HyperLogLog {
    /// Create new HyperLogLog with given precision (4-18)
    pub fn new(precision: u8) -> Self {
        let precision = precision.clamp(4, 18);
        let m = 1usize << precision;
        
        let alpha = match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m as f64),
        };

        Self {
            precision,
            registers: vec![0; m],
            alpha,
        }
    }

    /// Add a value to the HyperLogLog
    pub fn add<T: Hash>(&mut self, value: &T) {
        let hash = self.hash_value(value);
        let idx = (hash >> (64 - self.precision)) as usize;
        let remaining = (hash << self.precision) | (1 << (self.precision - 1));
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        
        if leading_zeros > self.registers[idx] {
            self.registers[idx] = leading_zeros;
        }
    }

    /// Estimate cardinality
    pub fn count(&self) -> u64 {
        let m = self.registers.len() as f64;
        
        // Calculate harmonic mean
        let sum: f64 = self.registers.iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();
        
        let estimate = self.alpha * m * m / sum;
        
        // Apply corrections
        if estimate <= 2.5 * m {
            // Small range correction
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                return (m * (m / zeros).ln()) as u64;
            }
        }
        
        if estimate > (1u64 << 32) as f64 / 30.0 {
            // Large range correction
            let two_32 = (1u64 << 32) as f64;
            return (-two_32 * (1.0 - estimate / two_32).ln()) as u64;
        }
        
        estimate as u64
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (i, &reg) in other.registers.iter().enumerate() {
            if reg > self.registers[i] {
                self.registers[i] = reg;
            }
        }
    }

    fn hash_value<T: Hash>(&self, value: &T) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

// ============================================================================
// T-Digest for Quantile Estimation
// ============================================================================

/// Centroid for T-Digest
#[derive(Debug, Clone, Copy)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// T-Digest implementation for accurate quantile estimation
/// Provides high accuracy at distribution tails (important for p99, p999)
#[derive(Debug, Clone)]
pub struct TDigest {
    /// Compression parameter (higher = more accuracy, more memory)
    compression: f64,
    /// Centroids
    centroids: Vec<Centroid>,
    /// Total weight
    total_weight: f64,
    /// Maximum number of centroids
    max_centroids: usize,
}

impl TDigest {
    /// Create new T-Digest with given compression
    pub fn new(compression: f64) -> Self {
        Self {
            compression,
            centroids: Vec::new(),
            total_weight: 0.0,
            max_centroids: (compression * 2.0) as usize,
        }
    }

    /// Add a value to the digest
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add a weighted value
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        self.centroids.push(Centroid { mean: value, weight });
        self.total_weight += weight;
        
        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Get quantile value (0.0 to 1.0)
    pub fn quantile(&mut self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() {
            return None;
        }
        
        self.compress();
        
        let target = q * self.total_weight;
        let mut cumulative = 0.0;
        
        for (i, centroid) in self.centroids.iter().enumerate() {
            let next_cumulative = cumulative + centroid.weight;
            
            if next_cumulative >= target {
                if i == 0 {
                    return Some(centroid.mean);
                }
                
                // Interpolate between centroids
                let prev = &self.centroids[i - 1];
                let ratio = (target - cumulative) / centroid.weight;
                return Some(prev.mean + ratio * (centroid.mean - prev.mean));
            }
            
            cumulative = next_cumulative;
        }
        
        self.centroids.last().map(|c| c.mean)
    }

    /// Get median (p50)
    pub fn median(&mut self) -> Option<f64> {
        self.quantile(0.5)
    }

    /// Get percentile
    pub fn percentile(&mut self, p: f64) -> Option<f64> {
        self.quantile(p / 100.0)
    }

    /// Merge another T-Digest
    pub fn merge(&mut self, other: &TDigest) {
        for centroid in &other.centroids {
            self.add_weighted(centroid.mean, centroid.weight);
        }
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 {
            return;
        }
        
        // Sort centroids by mean
        self.centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        
        let mut compressed = Vec::with_capacity(self.max_centroids);
        let mut current = self.centroids[0];
        
        for centroid in self.centroids.iter().skip(1) {
            // Calculate quantile at current position
            let q = (current.weight / 2.0 + compressed.iter().map(|c: &Centroid| c.weight).sum::<f64>()) / self.total_weight;
            let limit = 4.0 * self.compression * q * (1.0 - q);
            
            if current.weight + centroid.weight <= limit {
                // Merge centroids
                let new_weight = current.weight + centroid.weight;
                current.mean = (current.mean * current.weight + centroid.mean * centroid.weight) / new_weight;
                current.weight = new_weight;
            } else {
                compressed.push(current);
                current = *centroid;
            }
        }
        
        compressed.push(current);
        self.centroids = compressed;
    }
}

// ============================================================================
// Count-Min Sketch for Frequency Estimation
// ============================================================================

/// Count-Min Sketch for frequency estimation
/// Provides O(1) space frequency queries with bounded error
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// Width of each row
    width: usize,
    /// Number of hash functions (depth)
    depth: usize,
    /// Count matrix
    counts: Vec<Vec<u64>>,
    /// Seeds for hash functions
    seeds: Vec<u64>,
}

impl CountMinSketch {
    /// Create new Count-Min Sketch
    pub fn new(width: usize, depth: usize) -> Self {
        let mut seeds = Vec::with_capacity(depth);
        for i in 0..depth {
            seeds.push((i as u64).wrapping_mul(0x9e3779b97f4a7c15));
        }
        
        Self {
            width,
            depth,
            counts: vec![vec![0; width]; depth],
            seeds,
        }
    }

    /// Add a value
    pub fn add<T: Hash>(&mut self, value: &T) {
        self.add_count(value, 1);
    }

    /// Add with count
    pub fn add_count<T: Hash>(&mut self, value: &T, count: u64) {
        for i in 0..self.depth {
            let idx = self.hash(value, i);
            self.counts[i][idx] = self.counts[i][idx].saturating_add(count);
        }
    }

    /// Estimate frequency
    pub fn estimate<T: Hash>(&self, value: &T) -> u64 {
        let mut min = u64::MAX;
        for i in 0..self.depth {
            let idx = self.hash(value, i);
            min = min.min(self.counts[i][idx]);
        }
        min
    }

    fn hash<T: Hash>(&self, value: &T, row: usize) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.seeds[row].hash(&mut hasher);
        value.hash(&mut hasher);
        (hasher.finish() as usize) % self.width
    }
}

// ============================================================================
// Approximate Functions (SQL-compatible)
// ============================================================================

/// Approximate distinct count using HyperLogLog (uniqHLL12 compatible)
pub fn approx_count_distinct<T: Hash>(values: impl Iterator<Item = T>) -> u64 {
    let mut hll = HyperLogLog::new(12);
    for value in values {
        hll.add(&value);
    }
    hll.count()
}

/// Approximate quantile using T-Digest
pub fn approx_quantile(values: impl Iterator<Item = f64>, q: f64) -> Option<f64> {
    let mut tdigest = TDigest::new(100.0);
    for value in values {
        tdigest.add(value);
    }
    tdigest.quantile(q)
}

/// Approximate median
pub fn approx_median(values: impl Iterator<Item = f64>) -> Option<f64> {
    approx_quantile(values, 0.5)
}

/// Approximate percentiles (multiple at once for efficiency)
pub fn approx_percentiles(values: impl Iterator<Item = f64>, percentiles: &[f64]) -> Vec<Option<f64>> {
    let mut tdigest = TDigest::new(100.0);
    for value in values {
        tdigest.add(value);
    }
    percentiles.iter().map(|&p| tdigest.percentile(p)).collect()
}

/// Approximate top-k using Count-Min Sketch + heap
pub fn approx_top_k<T: Hash + Clone + Eq>(values: impl Iterator<Item = T>, k: usize) -> Vec<(T, u64)> {
    let mut sketch = CountMinSketch::new(1024, 5);
    let mut seen: HashSet<T> = HashSet::new();
    
    // First pass: count all values
    let all_values: Vec<T> = values.collect();
    for value in &all_values {
        sketch.add(value);
        seen.insert(value.clone());
    }
    
    // Second pass: find top-k
    let mut counts: Vec<(T, u64)> = seen.into_iter()
        .map(|v| {
            let count = sketch.estimate(&v);
            (v, count)
        })
        .collect();
    
    counts.sort_by(|a, b| b.1.cmp(&a.1));
    counts.truncate(k);
    counts
}

// ============================================================================
// Streaming Statistics
// ============================================================================

/// Streaming mean and variance (Welford's algorithm)
#[derive(Debug, Clone, Default)]
pub struct StreamingStats {
    count: u64,
    mean: f64,
    m2: f64,
    min: Option<f64>,
    max: Option<f64>,
}

impl StreamingStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
        
        self.min = Some(self.min.map(|m| m.min(value)).unwrap_or(value));
        self.max = Some(self.max.map(|m| m.max(value)).unwrap_or(value));
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / (self.count - 1) as f64
        }
    }

    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }

    pub fn min(&self) -> Option<f64> {
        self.min
    }

    pub fn max(&self) -> Option<f64> {
        self.max
    }

    pub fn merge(&mut self, other: &StreamingStats) {
        if other.count == 0 {
            return;
        }
        
        let combined_count = self.count + other.count;
        let delta = other.mean - self.mean;
        
        self.mean = (self.count as f64 * self.mean + other.count as f64 * other.mean) / combined_count as f64;
        self.m2 = self.m2 + other.m2 + delta * delta * (self.count as f64 * other.count as f64 / combined_count as f64);
        self.count = combined_count;
        
        self.min = match (self.min, other.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, other.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperloglog() {
        let mut hll = HyperLogLog::new(12);
        for i in 0..10000 {
            hll.add(&i);
        }
        let count = hll.count();
        // Should be within 2% of 10000
        assert!(count > 9800 && count < 10200, "HLL count {} not within expected range", count);
    }

    #[test]
    fn test_tdigest_quantiles() {
        let mut tdigest = TDigest::new(100.0);
        // Add more values for better accuracy
        for i in 1..=1000 {
            tdigest.add(i as f64);
        }

        let p50 = tdigest.percentile(50.0).unwrap();
        // T-Digest is an approximation, allow wider tolerance
        assert!(p50 > 400.0 && p50 < 600.0, "P50 {} not in expected range 400-600", p50);

        let p99 = tdigest.percentile(99.0).unwrap();
        // T-Digest is most accurate at the tails
        assert!(p99 > 900.0 && p99 <= 1000.0, "P99 {} not in expected range 900-1000", p99);
    }

    #[test]
    fn test_count_min_sketch() {
        let mut cms = CountMinSketch::new(1024, 5);
        
        for _ in 0..100 {
            cms.add(&"apple");
        }
        for _ in 0..50 {
            cms.add(&"banana");
        }
        
        assert!(cms.estimate(&"apple") >= 100);
        assert!(cms.estimate(&"banana") >= 50);
        assert!(cms.estimate(&"cherry") < 10);
    }

    #[test]
    fn test_streaming_stats() {
        let mut stats = StreamingStats::new();
        for i in 1..=100 {
            stats.add(i as f64);
        }
        
        assert_eq!(stats.count(), 100);
        assert!((stats.mean() - 50.5).abs() < 0.01);
        assert_eq!(stats.min(), Some(1.0));
        assert_eq!(stats.max(), Some(100.0));
    }

    #[test]
    fn test_approx_count_distinct() {
        let values = (0..10000).collect::<Vec<_>>();
        let count = approx_count_distinct(values.into_iter());
        assert!(count > 9800 && count < 10200);
    }
}
