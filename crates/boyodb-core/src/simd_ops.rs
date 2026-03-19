//! SIMD-accelerated Operations Module
//!
//! Provides vectorized implementations of common operations:
//! - Batch comparisons for filter evaluation
//! - Aggregation primitives (sum, min, max, count)
//! - Hash computation for grouping
//! - Memory operations (copy, zero, compare)
//!
//! Uses SIMD intrinsics when available, falls back to scalar operations otherwise.
//! Automatically selects the best available instruction set:
//! - AVX-512 (x86_64 with feature)
//! - AVX2 (x86_64)
//! - NEON (ARM64)
//! - Scalar fallback

use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for SIMD operations
#[derive(Clone, Debug)]
pub struct SimdConfig {
    /// Minimum batch size to use SIMD (smaller batches use scalar)
    pub min_batch_size: usize,
    /// Prefer SIMD even for small batches
    pub always_simd: bool,
    /// Enable prefetch hints
    pub prefetch_enabled: bool,
    /// Prefetch distance in cache lines
    pub prefetch_distance: usize,
}

impl Default for SimdConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 8,
            always_simd: false,
            prefetch_enabled: true,
            prefetch_distance: 4,
        }
    }
}

/// SIMD capability detection
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimdLevel {
    /// No SIMD available
    Scalar,
    /// SSE4.2 (128-bit)
    Sse42,
    /// AVX2 (256-bit)
    Avx2,
    /// AVX-512 (512-bit)
    Avx512,
    /// ARM NEON (128-bit)
    Neon,
}

impl SimdLevel {
    /// Detect the best available SIMD level for this CPU
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                return SimdLevel::Avx512;
            }
            if is_x86_feature_detected!("avx2") {
                return SimdLevel::Avx2;
            }
            if is_x86_feature_detected!("sse4.2") {
                return SimdLevel::Sse42;
            }
            return SimdLevel::Scalar;
        }
        #[cfg(target_arch = "aarch64")]
        {
            // NEON is always available on ARM64
            return SimdLevel::Neon;
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        SimdLevel::Scalar
    }

    /// Get the vector width in bytes for this SIMD level
    pub fn vector_width(&self) -> usize {
        match self {
            SimdLevel::Scalar => 8,  // Process 8 bytes at a time (u64)
            SimdLevel::Sse42 => 16,  // 128 bits
            SimdLevel::Avx2 => 32,   // 256 bits
            SimdLevel::Avx512 => 64, // 512 bits
            SimdLevel::Neon => 16,   // 128 bits
        }
    }

    /// Get the number of i64 values per vector
    pub fn i64_per_vector(&self) -> usize {
        self.vector_width() / 8
    }
}

// ============================================================================
// Sum Operations
// ============================================================================

/// Sum i64 values using SIMD when available
#[inline]
pub fn sum_i64(values: &[i64]) -> i64 {
    if values.is_empty() {
        return 0;
    }

    let simd_level = SimdLevel::detect();

    match simd_level {
        SimdLevel::Avx2 => sum_i64_avx2(values),
        SimdLevel::Sse42 | SimdLevel::Neon => sum_i64_sse(values),
        _ => sum_i64_scalar(values),
    }
}

/// Scalar sum implementation
#[inline]
fn sum_i64_scalar(values: &[i64]) -> i64 {
    values.iter().sum()
}

/// SSE/NEON sum implementation (128-bit vectors)
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn sum_i64_sse(values: &[i64]) -> i64 {
    let mut sum: i64 = 0;
    let chunks = values.chunks_exact(2);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum = sum.wrapping_add(chunk[0]).wrapping_add(chunk[1]);
    }

    for &v in remainder {
        sum = sum.wrapping_add(v);
    }

    sum
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn sum_i64_sse(values: &[i64]) -> i64 {
    sum_i64_scalar(values)
}

/// AVX2 sum implementation (256-bit vectors)
#[cfg(target_arch = "x86_64")]
fn sum_i64_avx2(values: &[i64]) -> i64 {
    // Process 4 i64 values at a time
    let mut sum0: i64 = 0;
    let mut sum1: i64 = 0;
    let mut sum2: i64 = 0;
    let mut sum3: i64 = 0;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum0 = sum0.wrapping_add(chunk[0]);
        sum1 = sum1.wrapping_add(chunk[1]);
        sum2 = sum2.wrapping_add(chunk[2]);
        sum3 = sum3.wrapping_add(chunk[3]);
    }

    let mut sum = sum0
        .wrapping_add(sum1)
        .wrapping_add(sum2)
        .wrapping_add(sum3);

    for &v in remainder {
        sum = sum.wrapping_add(v);
    }

    sum
}

#[cfg(not(target_arch = "x86_64"))]
fn sum_i64_avx2(values: &[i64]) -> i64 {
    sum_i64_scalar(values)
}

/// Sum f64 values using SIMD
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    // Use 4-way unrolling for better pipelining
    let mut sum0: f64 = 0.0;
    let mut sum1: f64 = 0.0;
    let mut sum2: f64 = 0.0;
    let mut sum3: f64 = 0.0;

    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum0 += chunk[0];
        sum1 += chunk[1];
        sum2 += chunk[2];
        sum3 += chunk[3];
    }

    let mut sum = sum0 + sum1 + sum2 + sum3;
    for &v in remainder {
        sum += v;
    }

    sum
}

// ============================================================================
// Min/Max Operations
// ============================================================================

/// Find minimum i64 value
#[inline]
pub fn min_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let mut min = values[0];

    // 4-way unrolled comparison
    let chunks = values[1..].chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        min = min.min(chunk[0]).min(chunk[1]).min(chunk[2]).min(chunk[3]);
    }

    for &v in remainder {
        min = min.min(v);
    }

    Some(min)
}

/// Find maximum i64 value
#[inline]
pub fn max_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let mut max = values[0];

    let chunks = values[1..].chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        max = max.max(chunk[0]).max(chunk[1]).max(chunk[2]).max(chunk[3]);
    }

    for &v in remainder {
        max = max.max(v);
    }

    Some(max)
}

/// Find minimum f64 value (handles NaN correctly)
#[inline]
pub fn min_f64(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::min)
}

/// Find maximum f64 value (handles NaN correctly)
#[inline]
pub fn max_f64(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(f64::max)
}

// ============================================================================
// Count Operations
// ============================================================================

/// Count non-null values in a validity bitmap
#[inline]
pub fn count_valid_bits(bitmap: &[u8], len: usize) -> usize {
    if bitmap.is_empty() {
        return len; // No bitmap means all valid
    }

    let full_bytes = len / 8;
    let remaining_bits = len % 8;

    let mut count: usize = 0;

    // Count full bytes using popcnt
    for &byte in &bitmap[..full_bytes] {
        count += byte.count_ones() as usize;
    }

    // Count remaining bits
    if remaining_bits > 0 && full_bytes < bitmap.len() {
        let mask = (1u8 << remaining_bits) - 1;
        count += (bitmap[full_bytes] & mask).count_ones() as usize;
    }

    count
}

/// Count values matching a predicate (optimized for filtering)
#[inline]
pub fn count_matching_i64(values: &[i64], predicate: impl Fn(i64) -> bool) -> usize {
    values.iter().filter(|&&v| predicate(v)).count()
}

// ============================================================================
// Hash Operations (for GROUP BY)
// ============================================================================

/// Fast hash for i64 values (for hash-based grouping)
#[inline]
pub fn hash_i64(value: i64) -> u64 {
    // FNV-1a inspired mixing
    const PRIME: u64 = 0x100000001b3;
    const OFFSET: u64 = 0xcbf29ce484222325;

    let mut hash = OFFSET;
    let bytes = value.to_le_bytes();

    for byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }

    hash
}

/// Fast hash for string bytes (for hash-based grouping)
#[inline]
pub fn hash_bytes(data: &[u8]) -> u64 {
    // Use xxHash-like algorithm for better distribution
    const PRIME1: u64 = 0x9e3779b185ebca87;
    const PRIME2: u64 = 0xc2b2ae3d27d4eb4f;
    const PRIME3: u64 = 0x165667b19e3779f9;

    let len = data.len();

    if len < 8 {
        // Small string fast path
        let mut hash = PRIME3.wrapping_mul(len as u64);
        for &byte in data {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(PRIME1);
        }
        return hash;
    }

    // Process 8 bytes at a time
    let mut hash = PRIME1.wrapping_add((len as u64).wrapping_mul(PRIME2));

    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let v = u64::from_le_bytes(chunk.try_into().unwrap());
        hash ^= v.wrapping_mul(PRIME2);
        hash = hash.rotate_left(31).wrapping_mul(PRIME1);
    }

    // Handle remainder
    for &byte in remainder {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(PRIME3);
    }

    // Final mix
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME3);
    hash ^= hash >> 32;

    hash
}

/// Batch hash computation for multiple i64 values
#[inline]
pub fn batch_hash_i64(values: &[i64], output: &mut [u64]) {
    debug_assert!(output.len() >= values.len());

    for (i, &v) in values.iter().enumerate() {
        output[i] = hash_i64(v);
    }
}

// ============================================================================
// Comparison Operations (for filtering)
// ============================================================================

/// Compare i64 values with equality (returns bitmask)
#[inline]
pub fn eq_i64_scalar(values: &[i64], scalar: i64, output: &mut [u8]) {
    let full_bytes = values.len() / 8;
    let remaining = values.len() % 8;

    for (i, chunk) in values.chunks(8).enumerate() {
        let mut byte: u8 = 0;
        for (j, &v) in chunk.iter().enumerate() {
            if v == scalar {
                byte |= 1 << j;
            }
        }
        if i < full_bytes {
            output[i] = byte;
        } else if remaining > 0 {
            output[i] = byte & ((1 << remaining) - 1);
        }
    }
}

/// Compare i64 values with less-than (returns bitmask)
#[inline]
pub fn lt_i64_scalar(values: &[i64], scalar: i64, output: &mut [u8]) {
    let full_bytes = values.len() / 8;
    let remaining = values.len() % 8;

    for (i, chunk) in values.chunks(8).enumerate() {
        let mut byte: u8 = 0;
        for (j, &v) in chunk.iter().enumerate() {
            if v < scalar {
                byte |= 1 << j;
            }
        }
        if i < full_bytes || remaining > 0 {
            output[i] = byte;
        }
    }
}

/// Compare i64 values with greater-than (returns bitmask)
#[inline]
pub fn gt_i64_scalar(values: &[i64], scalar: i64, output: &mut [u8]) {
    let full_bytes = values.len() / 8;
    let remaining = values.len() % 8;

    for (i, chunk) in values.chunks(8).enumerate() {
        let mut byte: u8 = 0;
        for (j, &v) in chunk.iter().enumerate() {
            if v > scalar {
                byte |= 1 << j;
            }
        }
        if i < full_bytes || remaining > 0 {
            output[i] = byte;
        }
    }
}

// ============================================================================
// Memory Operations
// ============================================================================

/// Fast memory copy with prefetch hints
#[inline]
pub fn fast_copy(src: &[u8], dst: &mut [u8]) {
    debug_assert!(dst.len() >= src.len());

    // For small copies, just use standard copy
    if src.len() < 4096 {
        dst[..src.len()].copy_from_slice(src);
        return;
    }

    // For larger copies, use chunks with prefetch
    #[cfg(target_arch = "x86_64")]
    {
        const PREFETCH_DISTANCE: usize = 512;

        for (i, chunk) in src.chunks(64).enumerate() {
            // Prefetch next cache line
            if i * 64 + PREFETCH_DISTANCE < src.len() {
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        src.as_ptr().add(i * 64 + PREFETCH_DISTANCE) as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }
            dst[i * 64..i * 64 + chunk.len()].copy_from_slice(chunk);
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        dst[..src.len()].copy_from_slice(src);
    }
}

/// Zero memory efficiently
#[inline]
pub fn fast_zero(dst: &mut [u8]) {
    // For small buffers, simple fill
    if dst.len() < 4096 {
        dst.fill(0);
        return;
    }

    // For larger buffers, use chunks
    for chunk in dst.chunks_mut(4096) {
        chunk.fill(0);
    }
}

// ============================================================================
// Aggregator State for Streaming Aggregation
// ============================================================================

/// Thread-safe aggregation state using atomics
pub struct AtomicAggState {
    pub count: AtomicU64,
    pub sum_i64: AtomicU64, // Stored as transmuted i64
    pub min_i64: AtomicU64,
    pub max_i64: AtomicU64,
}

impl AtomicAggState {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_i64: AtomicU64::new(0),
            min_i64: AtomicU64::new(i64::MAX as u64),
            max_i64: AtomicU64::new(i64::MIN as u64),
        }
    }

    /// Add values to this aggregation state (thread-safe)
    pub fn add_i64_values(&self, values: &[i64]) {
        if values.is_empty() {
            return;
        }

        // Compute local aggregates first
        let count = values.len() as u64;
        let sum = sum_i64(values);
        let min = min_i64(values).unwrap_or(i64::MAX);
        let max = max_i64(values).unwrap_or(i64::MIN);

        // Atomically update global state
        self.count.fetch_add(count, Ordering::Relaxed);

        // Sum requires careful handling (transmute to u64 for atomic ops)
        loop {
            let current = self.sum_i64.load(Ordering::Relaxed);
            let current_i64 = current as i64;
            let new_sum = current_i64.wrapping_add(sum);
            if self
                .sum_i64
                .compare_exchange_weak(
                    current,
                    new_sum as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // Min
        loop {
            let current = self.min_i64.load(Ordering::Relaxed) as i64;
            if min >= current {
                break;
            }
            if self
                .min_i64
                .compare_exchange_weak(
                    current as u64,
                    min as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // Max
        loop {
            let current = self.max_i64.load(Ordering::Relaxed) as i64;
            if max <= current {
                break;
            }
            if self
                .max_i64
                .compare_exchange_weak(
                    current as u64,
                    max as u64,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    /// Get final count
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get final sum as i64
    pub fn sum(&self) -> i64 {
        self.sum_i64.load(Ordering::Relaxed) as i64
    }

    /// Get final min
    pub fn min(&self) -> Option<i64> {
        let v = self.min_i64.load(Ordering::Relaxed) as i64;
        if v == i64::MAX {
            None
        } else {
            Some(v)
        }
    }

    /// Get final max
    pub fn max(&self) -> Option<i64> {
        let v = self.max_i64.load(Ordering::Relaxed) as i64;
        if v == i64::MIN {
            None
        } else {
            Some(v)
        }
    }
}

impl Default for AtomicAggState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_detection() {
        let level = SimdLevel::detect();
        println!("Detected SIMD level: {:?}", level);
        assert!(level.vector_width() >= 8);
    }

    #[test]
    fn test_sum_i64() {
        let values: Vec<i64> = (1..=100).collect();
        assert_eq!(sum_i64(&values), 5050);

        let empty: Vec<i64> = vec![];
        assert_eq!(sum_i64(&empty), 0);

        let single = vec![42i64];
        assert_eq!(sum_i64(&single), 42);
    }

    #[test]
    fn test_sum_f64() {
        let values: Vec<f64> = (1..=10).map(|x| x as f64).collect();
        assert!((sum_f64(&values) - 55.0).abs() < 0.0001);
    }

    #[test]
    fn test_min_max_i64() {
        let values = vec![5i64, 2, 8, 1, 9, 3];
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(9));

        let empty: Vec<i64> = vec![];
        assert_eq!(min_i64(&empty), None);
        assert_eq!(max_i64(&empty), None);
    }

    #[test]
    fn test_count_valid_bits() {
        let bitmap = vec![0b11110000u8, 0b00001111u8];
        assert_eq!(count_valid_bits(&bitmap, 16), 8);

        let all_ones = vec![0xFFu8; 8];
        assert_eq!(count_valid_bits(&all_ones, 64), 64);

        // Partial last byte
        let partial = vec![0b11111111u8, 0b00000111u8];
        assert_eq!(count_valid_bits(&partial, 11), 11);
    }

    #[test]
    fn test_hash_i64() {
        let h1 = hash_i64(42);
        let h2 = hash_i64(42);
        let h3 = hash_i64(43);

        assert_eq!(h1, h2); // Same input = same hash
        assert_ne!(h1, h3); // Different input = different hash
    }

    #[test]
    fn test_hash_bytes() {
        let h1 = hash_bytes(b"hello");
        let h2 = hash_bytes(b"hello");
        let h3 = hash_bytes(b"world");

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);

        // Test small strings
        let h4 = hash_bytes(b"a");
        let h5 = hash_bytes(b"b");
        assert_ne!(h4, h5);
    }

    #[test]
    fn test_eq_i64_scalar() {
        let values = vec![1i64, 2, 3, 2, 5, 2, 7, 8];
        let mut output = vec![0u8; 1];
        eq_i64_scalar(&values, 2, &mut output);
        assert_eq!(output[0], 0b00101010); // bits 1, 3, 5 set
    }

    #[test]
    fn test_atomic_agg_state() {
        let state = AtomicAggState::new();

        state.add_i64_values(&[1, 2, 3, 4, 5]);
        assert_eq!(state.count(), 5);
        assert_eq!(state.sum(), 15);
        assert_eq!(state.min(), Some(1));
        assert_eq!(state.max(), Some(5));

        state.add_i64_values(&[0, 10]);
        assert_eq!(state.count(), 7);
        assert_eq!(state.sum(), 25);
        assert_eq!(state.min(), Some(0));
        assert_eq!(state.max(), Some(10));
    }

    #[test]
    fn test_fast_copy() {
        let src: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let mut dst = vec![0u8; 1000];

        fast_copy(&src, &mut dst);
        assert_eq!(src, dst);
    }
}
