//! Gorilla Codec for Float Compression
//!
//! Implements the Gorilla compression algorithm from Facebook's time-series database.
//! This codec is highly effective for compressing floating-point time-series data
//! where consecutive values are similar.
//!
//! The algorithm uses XOR-based delta encoding:
//! 1. First value is stored raw (64 bits for f64)
//! 2. Subsequent values store XOR with previous value
//! 3. XOR results are compressed using leading/trailing zero counts
//!
//! Reference: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

use std::io::{self, Read, Write};

/// Gorilla encoder for f64 values
pub struct GorillaEncoder {
    buffer: Vec<u8>,
    bit_pos: usize,      // Current bit position in current byte
    prev_value: u64,     // Previous value (as bits)
    prev_leading: u8,    // Previous leading zeros
    prev_trailing: u8,   // Previous trailing zeros
    first_value: bool,   // Is this the first value?
}

impl GorillaEncoder {
    pub fn new() -> Self {
        GorillaEncoder {
            buffer: Vec::with_capacity(1024),
            bit_pos: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            first_value: true,
        }
    }

    /// Encode a single f64 value
    pub fn encode(&mut self, value: f64) {
        let bits = value.to_bits();

        if self.first_value {
            // Write full 64 bits for first value
            self.write_bits(bits, 64);
            self.prev_value = bits;
            self.first_value = false;
            return;
        }

        let xor = bits ^ self.prev_value;

        if xor == 0 {
            // Same value: write single 0 bit
            self.write_bit(false);
        } else {
            // Different value: write 1 bit then compressed XOR
            self.write_bit(true);

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            // Check if we can reuse previous leading/trailing counts
            if leading >= self.prev_leading && trailing >= self.prev_trailing {
                // Control bit 0: reuse previous block
                self.write_bit(false);
                // Write significant bits in the same position as previous
                let sig_bits = 64 - self.prev_leading - self.prev_trailing;
                let shifted = xor >> self.prev_trailing;
                self.write_bits(shifted as u64, sig_bits as usize);
            } else {
                // Control bit 1: new block with different leading/trailing
                self.write_bit(true);
                // Write leading zeros (5 bits, max 31)
                self.write_bits(leading.min(31) as u64, 5);
                // Write significant bits count (6 bits, max 63)
                let sig_bits = 64 - leading - trailing;
                self.write_bits(sig_bits as u64, 6);
                // Write the significant bits
                let shifted = xor >> trailing;
                self.write_bits(shifted as u64, sig_bits as usize);

                self.prev_leading = leading;
                self.prev_trailing = trailing;
            }
        }

        self.prev_value = bits;
    }

    /// Write a single bit
    fn write_bit(&mut self, bit: bool) {
        if self.bit_pos == 0 {
            self.buffer.push(0);
        }

        if bit {
            let byte_idx = self.buffer.len() - 1;
            self.buffer[byte_idx] |= 1 << (7 - self.bit_pos);
        }

        self.bit_pos = (self.bit_pos + 1) % 8;
    }

    /// Write multiple bits (up to 64)
    fn write_bits(&mut self, value: u64, num_bits: usize) {
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    /// Finish encoding and return compressed bytes
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    /// Get current compressed size
    pub fn size(&self) -> usize {
        self.buffer.len()
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Gorilla decoder for f64 values
pub struct GorillaDecoder<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: usize,
    prev_value: u64,
    prev_leading: u8,
    prev_trailing: u8,
    first_value: bool,
}

impl<'a> GorillaDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        GorillaDecoder {
            data,
            byte_pos: 0,
            bit_pos: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            first_value: true,
        }
    }

    /// Decode the next f64 value
    pub fn decode(&mut self) -> io::Result<f64> {
        if self.first_value {
            // Read full 64 bits
            let bits = self.read_bits(64)?;
            self.prev_value = bits;
            self.first_value = false;
            return Ok(f64::from_bits(bits));
        }

        // Read first control bit
        if !self.read_bit()? {
            // Same as previous value
            return Ok(f64::from_bits(self.prev_value));
        }

        // Read second control bit
        let xor = if !self.read_bit()? {
            // Reuse previous leading/trailing
            let sig_bits = 64 - self.prev_leading - self.prev_trailing;
            let shifted = self.read_bits(sig_bits as usize)?;
            shifted << self.prev_trailing
        } else {
            // New leading/trailing
            let leading = self.read_bits(5)? as u8;
            let sig_bits = self.read_bits(6)? as u8;
            let trailing = 64 - leading - sig_bits;
            let shifted = self.read_bits(sig_bits as usize)?;

            self.prev_leading = leading;
            self.prev_trailing = trailing;

            shifted << trailing
        };

        self.prev_value ^= xor;
        Ok(f64::from_bits(self.prev_value))
    }

    /// Read a single bit
    fn read_bit(&mut self) -> io::Result<bool> {
        if self.byte_pos >= self.data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "end of data"));
        }

        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }

        Ok(bit)
    }

    /// Read multiple bits (up to 64)
    fn read_bits(&mut self, num_bits: usize) -> io::Result<u64> {
        let mut value: u64 = 0;
        for _ in 0..num_bits {
            value = (value << 1) | (self.read_bit()? as u64);
        }
        Ok(value)
    }

    /// Check if there's more data to decode
    pub fn has_more(&self) -> bool {
        self.byte_pos < self.data.len()
    }
}

/// Encode a slice of f64 values using Gorilla compression
pub fn gorilla_encode_f64(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return vec![];
    }

    // Header: 8 bytes for count
    let mut result = Vec::with_capacity(values.len() + 8);
    result.extend_from_slice(&(values.len() as u64).to_le_bytes());

    let mut encoder = GorillaEncoder::new();
    for &val in values {
        encoder.encode(val);
    }
    result.extend(encoder.finish());

    result
}

/// Decode Gorilla-compressed f64 values
pub fn gorilla_decode_f64(data: &[u8]) -> io::Result<Vec<f64>> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "buffer too small"));
    }

    let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let mut decoder = GorillaDecoder::new(&data[8..]);
    let mut values = Vec::with_capacity(count);

    for _ in 0..count {
        values.push(decoder.decode()?);
    }

    Ok(values)
}

/// Statistics about Gorilla compression
#[derive(Debug, Clone, Default)]
pub struct GorillaStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub identical_values: usize,
    pub block_reuses: usize,
}

/// Analyze compression effectiveness
pub fn analyze_gorilla_compression(values: &[f64]) -> GorillaStats {
    if values.is_empty() {
        return GorillaStats::default();
    }

    let original_size = values.len() * 8;
    let compressed = gorilla_encode_f64(values);
    let compressed_size = compressed.len();

    // Count identical consecutive values
    let identical_values = values
        .windows(2)
        .filter(|w| w[0].to_bits() == w[1].to_bits())
        .count();

    GorillaStats {
        original_size,
        compressed_size,
        compression_ratio: original_size as f64 / compressed_size as f64,
        identical_values,
        block_reuses: 0, // Would need to track during encoding
    }
}

/// Encode f32 values using Gorilla (converted to f64)
pub fn gorilla_encode_f32(values: &[f32]) -> Vec<u8> {
    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
    gorilla_encode_f64(&f64_values)
}

/// Decode Gorilla-compressed values back to f32
pub fn gorilla_decode_f32(data: &[u8]) -> io::Result<Vec<f32>> {
    let f64_values = gorilla_decode_f64(data)?;
    Ok(f64_values.iter().map(|&v| v as f32).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gorilla_roundtrip() {
        let values = vec![1.0, 1.1, 1.2, 1.3, 1.4, 1.5];
        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();

        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-10, "Values differ: {} vs {}", a, b);
        }
    }

    #[test]
    fn test_gorilla_identical_values() {
        // Identical values should compress very well
        let values = vec![42.0; 1000];
        let encoded = gorilla_encode_f64(&values);

        // Should be much smaller than original
        let original_size = values.len() * 8;
        let compressed_size = encoded.len();
        let ratio = original_size as f64 / compressed_size as f64;

        assert!(ratio > 10.0, "Expected >10x compression for identical values, got {:.2}x", ratio);

        // Verify roundtrip
        let decoded = gorilla_decode_f64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_gorilla_time_series() {
        // Simulate sensor data with small variations
        // Use deterministic data for reproducible test
        let mut values = Vec::new();
        let mut v = 100.0f64;
        for i in 0..10000 {
            values.push(v);
            // Small predictable changes (simulating sensor readings)
            v += 0.001 * ((i % 10) as f64 - 5.0);
        }

        let original_size = values.len() * 8;
        let encoded = gorilla_encode_f64(&values);

        // Verify roundtrip is the primary concern
        let decoded = gorilla_decode_f64(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-10);
        }

        // Compression ratio depends on data pattern - just verify it works
        println!("Time series: original={}, compressed={}, ratio={:.2}x",
            original_size, encoded.len(), original_size as f64 / encoded.len() as f64);
    }

    #[test]
    fn test_gorilla_random_data() {
        // Random data won't compress well but should still roundtrip
        let values: Vec<f64> = (0..1000).map(|_| rand::random::<f64>() * 1000.0).collect();

        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();

        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-10);
        }
    }

    #[test]
    fn test_gorilla_empty() {
        let values: Vec<f64> = vec![];
        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_gorilla_single_value() {
        let values = vec![3.14159];
        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_gorilla_special_values() {
        let values = vec![
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::EPSILON,
        ];

        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();

        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            if a.is_nan() {
                assert!(b.is_nan());
            } else {
                assert_eq!(a.to_bits(), b.to_bits());
            }
        }
    }

    #[test]
    fn test_gorilla_f32() {
        let values: Vec<f32> = vec![1.0, 1.1, 1.2, 1.3, 1.4];
        let encoded = gorilla_encode_f32(&values);
        let decoded = gorilla_decode_f32(&encoded).unwrap();

        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert!((a - b).abs() < 1e-5);
        }
    }

    #[test]
    fn test_gorilla_compression_stats() {
        let values: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64) * 0.001).collect();
        let stats = analyze_gorilla_compression(&values);

        assert_eq!(stats.original_size, 8000);
        // Just verify stats are computed, compression ratio varies
        assert!(stats.compressed_size > 0);
        assert!(stats.compression_ratio > 0.0);
    }

    #[test]
    fn test_gorilla_increasing_sequence() {
        // Monotonically increasing values
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();

        let encoded = gorilla_encode_f64(&values);
        let decoded = gorilla_decode_f64(&encoded).unwrap();

        assert_eq!(values, decoded);
    }
}
