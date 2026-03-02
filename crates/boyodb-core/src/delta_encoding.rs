//! Delta encoding for timestamp columns
//!
//! Delta encoding stores differences between consecutive values rather than absolute values.
//! This is extremely effective for sorted timestamp data where consecutive values are close together.
//!
//! For example, timestamps like [1000000, 1000001, 1000003, 1000007] become deltas [1, 2, 4]
//! which compress much better with subsequent compression algorithms.

use arrow_array::{Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray};
use std::io::{self, Read, Write};

/// Delta-encodes an i64 array and writes to a byte buffer
/// Format: [base_value: i64][count: u64][delta0: varint][delta1: varint]...
pub fn delta_encode_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return vec![];
    }

    let mut buf = Vec::with_capacity(values.len() * 2); // Conservative estimate

    // Write base value (first element)
    buf.extend_from_slice(&values[0].to_le_bytes());

    // Write count
    buf.extend_from_slice(&(values.len() as u64).to_le_bytes());

    // Write deltas as varints
    for i in 1..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        write_varint_i64(&mut buf, delta);
    }

    buf
}

/// Decodes delta-encoded i64 values from a byte buffer
pub fn delta_decode_i64(data: &[u8]) -> io::Result<Vec<i64>> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    if data.len() < 16 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "buffer too small for header"));
    }

    // Read base value
    let base = i64::from_le_bytes(data[0..8].try_into().unwrap());

    // Read count
    let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;

    if count == 0 {
        return Ok(vec![]);
    }

    let mut values = Vec::with_capacity(count);
    values.push(base);

    let mut pos = 16;
    for _ in 1..count {
        let (delta, bytes_read) = read_varint_i64(&data[pos..])?;
        pos += bytes_read;
        let prev = *values.last().unwrap();
        values.push(prev.wrapping_add(delta));
    }

    Ok(values)
}

/// Delta-encodes a timestamp microsecond array
pub fn delta_encode_timestamp_micros(arr: &TimestampMicrosecondArray) -> Vec<u8> {
    let values: Vec<i64> = arr.values().iter().copied().collect();
    delta_encode_i64(&values)
}

/// Decodes delta-encoded timestamps into a TimestampMicrosecondArray
pub fn delta_decode_timestamp_micros(data: &[u8]) -> io::Result<TimestampMicrosecondArray> {
    let values = delta_decode_i64(data)?;
    Ok(TimestampMicrosecondArray::from(values))
}

/// Delta-encodes a timestamp millisecond array
pub fn delta_encode_timestamp_millis(arr: &TimestampMillisecondArray) -> Vec<u8> {
    let values: Vec<i64> = arr.values().iter().copied().collect();
    delta_encode_i64(&values)
}

/// Decodes delta-encoded timestamps into a TimestampMillisecondArray
pub fn delta_decode_timestamp_millis(data: &[u8]) -> io::Result<TimestampMillisecondArray> {
    let values = delta_decode_i64(data)?;
    Ok(TimestampMillisecondArray::from(values))
}

/// Delta-encodes an Int64Array (useful for any monotonic i64 column)
pub fn delta_encode_int64_array(arr: &Int64Array) -> Vec<u8> {
    let values: Vec<i64> = arr.values().iter().copied().collect();
    delta_encode_i64(&values)
}

/// Decodes delta-encoded data into an Int64Array
pub fn delta_decode_int64_array(data: &[u8]) -> io::Result<Int64Array> {
    let values = delta_decode_i64(data)?;
    Ok(Int64Array::from(values))
}

/// Write a signed i64 as a varint using zigzag encoding
/// Zigzag encoding maps signed integers to unsigned: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, etc.
/// This ensures small absolute values (positive or negative) use few bytes.
fn write_varint_i64(buf: &mut Vec<u8>, value: i64) {
    // Zigzag encode: (n << 1) ^ (n >> 63)
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    write_varint_u64(buf, zigzag);
}

/// Write an unsigned u64 as a varint
fn write_varint_u64(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
}

/// Read a signed i64 varint using zigzag decoding
fn read_varint_i64(data: &[u8]) -> io::Result<(i64, usize)> {
    let (zigzag, bytes_read) = read_varint_u64(data)?;
    // Zigzag decode: (n >> 1) ^ -(n & 1)
    let value = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
    Ok((value, bytes_read))
}

/// Read an unsigned u64 varint
fn read_varint_u64(data: &[u8]) -> io::Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"));
        }
    }

    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of varint"))
}

/// Double-delta encoding for highly regular timestamps (e.g., fixed intervals)
/// Stores second-order deltas: delta_of_deltas
/// Even more effective when timestamps are evenly spaced
pub fn double_delta_encode_i64(values: &[i64]) -> Vec<u8> {
    if values.len() < 2 {
        return delta_encode_i64(values);
    }

    let mut buf = Vec::with_capacity(values.len() * 2);

    // Write base value
    buf.extend_from_slice(&values[0].to_le_bytes());

    // Write count
    buf.extend_from_slice(&(values.len() as u64).to_le_bytes());

    // Write first delta
    let first_delta = values[1].wrapping_sub(values[0]);
    buf.extend_from_slice(&first_delta.to_le_bytes());

    // Write double-deltas
    let mut prev_delta = first_delta;
    for i in 2..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        let double_delta = delta.wrapping_sub(prev_delta);
        write_varint_i64(&mut buf, double_delta);
        prev_delta = delta;
    }

    buf
}

/// Decode double-delta encoded i64 values
pub fn double_delta_decode_i64(data: &[u8]) -> io::Result<Vec<i64>> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    if data.len() < 24 {
        // Fall back to regular delta decode for small buffers
        return delta_decode_i64(data);
    }

    // Read base value
    let base = i64::from_le_bytes(data[0..8].try_into().unwrap());

    // Read count
    let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;

    if count == 0 {
        return Ok(vec![]);
    }
    if count == 1 {
        return Ok(vec![base]);
    }

    // Read first delta
    let first_delta = i64::from_le_bytes(data[16..24].try_into().unwrap());

    let mut values = Vec::with_capacity(count);
    values.push(base);
    values.push(base.wrapping_add(first_delta));

    let mut prev_delta = first_delta;
    let mut pos = 24;

    for _ in 2..count {
        let (double_delta, bytes_read) = read_varint_i64(&data[pos..])?;
        pos += bytes_read;
        let delta = prev_delta.wrapping_add(double_delta);
        let prev_value = *values.last().unwrap();
        values.push(prev_value.wrapping_add(delta));
        prev_delta = delta;
    }

    Ok(values)
}

/// Statistics about delta encoding effectiveness
#[derive(Debug, Clone)]
pub struct DeltaEncodingStats {
    pub original_size: usize,
    pub encoded_size: usize,
    pub compression_ratio: f64,
    pub avg_delta: f64,
    pub max_delta: i64,
    pub min_delta: i64,
}

/// Analyze how effective delta encoding would be for a given array
pub fn analyze_delta_encoding(values: &[i64]) -> DeltaEncodingStats {
    if values.len() < 2 {
        return DeltaEncodingStats {
            original_size: values.len() * 8,
            encoded_size: values.len() * 8,
            compression_ratio: 1.0,
            avg_delta: 0.0,
            max_delta: 0,
            min_delta: 0,
        };
    }

    let mut deltas = Vec::with_capacity(values.len() - 1);
    for i in 1..values.len() {
        deltas.push(values[i].wrapping_sub(values[i - 1]));
    }

    let min_delta = *deltas.iter().min().unwrap();
    let max_delta = *deltas.iter().max().unwrap();
    let sum: i128 = deltas.iter().map(|&d| d as i128).sum();
    let avg_delta = sum as f64 / deltas.len() as f64;

    let encoded = delta_encode_i64(values);

    DeltaEncodingStats {
        original_size: values.len() * 8,
        encoded_size: encoded.len(),
        compression_ratio: (values.len() * 8) as f64 / encoded.len() as f64,
        avg_delta,
        max_delta,
        min_delta,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encode_decode_roundtrip() {
        let values = vec![1000000i64, 1000001, 1000003, 1000007, 1000015, 1000031];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_delta_encoding_compression() {
        // Simulate sorted timestamps with small deltas
        let mut values = Vec::new();
        let mut ts = 1700000000000000i64; // Microseconds since epoch
        for _ in 0..10000 {
            values.push(ts);
            ts += rand::random::<u64>() as i64 % 1000 + 1; // Small increments
        }

        let original_size = values.len() * 8;
        let encoded = delta_encode_i64(&values);
        let ratio = original_size as f64 / encoded.len() as f64;

        // Delta encoding should achieve at least 2x compression for small deltas
        assert!(ratio > 2.0, "Expected >2x compression, got {:.2}x", ratio);

        // Verify roundtrip
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_double_delta_encoding() {
        // Fixed interval timestamps (very regular)
        let values: Vec<i64> = (0..10000).map(|i| 1700000000000000i64 + i * 1000).collect();

        let single_encoded = delta_encode_i64(&values);
        let double_encoded = double_delta_encode_i64(&values);

        // Double delta should be even smaller for regular intervals
        assert!(double_encoded.len() <= single_encoded.len(),
                "Double delta should be <= single delta for regular intervals");

        // Verify roundtrip
        let decoded = double_delta_decode_i64(&double_encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_negative_deltas() {
        // Out-of-order values (negative deltas)
        let values = vec![100i64, 90, 85, 95, 80, 110];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_empty_array() {
        let values: Vec<i64> = vec![];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_single_element() {
        let values = vec![42i64];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_timestamp_micros_array() {
        let values = vec![1700000000000000i64, 1700000000001000, 1700000000002500];
        let arr = TimestampMicrosecondArray::from(values.clone());

        let encoded = delta_encode_timestamp_micros(&arr);
        let decoded = delta_decode_timestamp_micros(&encoded).unwrap();

        assert_eq!(arr.len(), decoded.len());
        for i in 0..arr.len() {
            assert_eq!(arr.value(i), decoded.value(i));
        }
    }

    #[test]
    fn test_analyze_delta_encoding() {
        let values: Vec<i64> = (0..1000).map(|i| 1000000i64 + i * 10).collect();
        let stats = analyze_delta_encoding(&values);

        assert_eq!(stats.min_delta, 10);
        assert_eq!(stats.max_delta, 10);
        assert!((stats.avg_delta - 10.0).abs() < 0.001);
        assert!(stats.compression_ratio > 1.0);
    }

    #[test]
    fn test_large_deltas() {
        // Large deltas (should still work, but less compression)
        let values = vec![0i64, i64::MAX / 2, i64::MAX];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_varint_encoding() {
        // Test various varint sizes
        let test_values = vec![
            0i64, 1, -1, 127, -128,
            16383, -16384,  // 2 bytes
            2097151, -2097152,  // 3 bytes
            i64::MAX, i64::MIN,
        ];

        for &val in &test_values {
            let mut buf = Vec::new();
            write_varint_i64(&mut buf, val);
            let (decoded, _) = read_varint_i64(&buf).unwrap();
            assert_eq!(val, decoded, "Varint roundtrip failed for {}", val);
        }
    }
}
