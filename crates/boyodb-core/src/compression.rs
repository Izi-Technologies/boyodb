//! Advanced Compression Module
//!
//! This module provides columnar compression codecs:
//! - Dictionary Encoding for string compression
//! - LZ4/ZSTD for general-purpose compression
//! - DoubleDelta for time-series integer compression
//! - Run-Length Encoding (RLE) for repeated values
//! - Bit-packing for small integers

use std::collections::HashMap;
use std::io::{Read, Write, Cursor};

// ============================================================================
// Compression Codec Abstraction
// ============================================================================

/// Compression codec type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecType {
    /// No compression
    None,
    /// LZ4 fast compression
    Lz4,
    /// ZSTD compression with level
    Zstd,
    /// Dictionary encoding
    Dictionary,
    /// Delta encoding
    Delta,
    /// Double-delta encoding (delta of deltas)
    DoubleDelta,
    /// Run-length encoding
    Rle,
    /// Bit-packing
    BitPacked,
    /// XOR encoding for floats
    Xor,
    /// Combined: Dictionary + LZ4
    DictionaryLz4,
    /// Combined: Delta + ZSTD
    DeltaZstd,
}

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Number of values
    pub num_values: usize,
    /// Compression time in microseconds
    pub compress_time_us: u64,
    /// Decompression time in microseconds
    pub decompress_time_us: u64,
}

impl CompressionStats {
    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_size > 0 {
            self.original_size as f64 / self.compressed_size as f64
        } else {
            0.0
        }
    }

    pub fn space_savings(&self) -> f64 {
        if self.original_size > 0 {
            1.0 - (self.compressed_size as f64 / self.original_size as f64)
        } else {
            0.0
        }
    }
}

/// Codec trait for compression/decompression
pub trait Codec: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;

    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;

    /// Get codec type
    fn codec_type(&self) -> CodecType;
}

#[derive(Debug)]
pub enum CompressionError {
    InvalidData(String),
    BufferTooSmall,
    DictionaryFull,
    UnsupportedCodec(String),
    IoError(String),
    Other(String),
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::InvalidData(e) => write!(f, "Invalid data: {}", e),
            CompressionError::BufferTooSmall => write!(f, "Buffer too small"),
            CompressionError::DictionaryFull => write!(f, "Dictionary is full"),
            CompressionError::UnsupportedCodec(c) => write!(f, "Unsupported codec: {}", c),
            CompressionError::IoError(e) => write!(f, "I/O error: {}", e),
            CompressionError::Other(e) => write!(f, "Compression error: {}", e),
        }
    }
}

impl std::error::Error for CompressionError {}

// ============================================================================
// Dictionary Encoding
// ============================================================================

/// Dictionary encoder for string/bytes columns
#[derive(Debug, Clone)]
pub struct DictionaryEncoder {
    /// Value to index mapping
    value_to_index: HashMap<Vec<u8>, u32>,
    /// Index to value mapping
    index_to_value: Vec<Vec<u8>>,
    /// Maximum dictionary size
    max_size: usize,
    /// Whether dictionary is sorted
    sorted: bool,
}

impl DictionaryEncoder {
    pub fn new(max_size: usize) -> Self {
        Self {
            value_to_index: HashMap::new(),
            index_to_value: Vec::new(),
            max_size,
            sorted: false,
        }
    }

    pub fn with_sorted(mut self, sorted: bool) -> Self {
        self.sorted = sorted;
        self
    }

    /// Add a value to the dictionary
    pub fn add(&mut self, value: &[u8]) -> Result<u32, CompressionError> {
        if let Some(&idx) = self.value_to_index.get(value) {
            return Ok(idx);
        }

        if self.index_to_value.len() >= self.max_size {
            return Err(CompressionError::DictionaryFull);
        }

        let idx = self.index_to_value.len() as u32;
        self.index_to_value.push(value.to_vec());
        self.value_to_index.insert(value.to_vec(), idx);

        Ok(idx)
    }

    /// Get index for a value (None if not in dictionary)
    pub fn get_index(&self, value: &[u8]) -> Option<u32> {
        self.value_to_index.get(value).copied()
    }

    /// Get value for an index
    pub fn get_value(&self, index: u32) -> Option<&[u8]> {
        self.index_to_value.get(index as usize).map(|v| v.as_slice())
    }

    /// Get dictionary size
    pub fn len(&self) -> usize {
        self.index_to_value.len()
    }

    /// Check if dictionary is empty
    pub fn is_empty(&self) -> bool {
        self.index_to_value.is_empty()
    }

    /// Build dictionary from values
    pub fn build_from_values(&mut self, values: &[&[u8]]) -> Result<(), CompressionError> {
        // Count frequencies
        let mut freq: HashMap<Vec<u8>, usize> = HashMap::new();
        for value in values {
            *freq.entry(value.to_vec()).or_insert(0) += 1;
        }

        // Sort by frequency (most frequent first) or alphabetically if sorted
        let mut entries: Vec<_> = freq.into_iter().collect();
        if self.sorted {
            entries.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            entries.sort_by(|a, b| b.1.cmp(&a.1));
        }

        // Add to dictionary (up to max_size)
        for (value, _) in entries.into_iter().take(self.max_size) {
            self.add(&value)?;
        }

        Ok(())
    }

    /// Encode values using the dictionary
    pub fn encode(&self, values: &[&[u8]]) -> Result<DictionaryEncoded, CompressionError> {
        let mut indices = Vec::with_capacity(values.len());
        let mut fallback_indices = Vec::new();
        let mut fallback_values = Vec::new();

        for (i, value) in values.iter().enumerate() {
            match self.get_index(value) {
                Some(idx) => indices.push(idx),
                None => {
                    // Use max u32 as marker for fallback
                    indices.push(u32::MAX);
                    fallback_indices.push(i as u32);
                    fallback_values.push(value.to_vec());
                }
            }
        }

        Ok(DictionaryEncoded {
            dictionary: self.index_to_value.clone(),
            indices,
            fallback_indices,
            fallback_values,
        })
    }

    /// Serialize dictionary to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Number of entries
        result.extend_from_slice(&(self.index_to_value.len() as u32).to_le_bytes());

        // Each entry: length (u32) + data
        for value in &self.index_to_value {
            result.extend_from_slice(&(value.len() as u32).to_le_bytes());
            result.extend_from_slice(value);
        }

        result
    }

    /// Deserialize dictionary from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let mut pos = 0;
        let num_entries = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut encoder = DictionaryEncoder::new(num_entries);

        for _ in 0..num_entries {
            if pos + 4 > data.len() {
                return Err(CompressionError::InvalidData("Truncated".to_string()));
            }

            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + len > data.len() {
                return Err(CompressionError::InvalidData("Truncated value".to_string()));
            }

            encoder.add(&data[pos..pos + len])?;
            pos += len;
        }

        Ok(encoder)
    }
}

/// Dictionary-encoded data
#[derive(Debug, Clone)]
pub struct DictionaryEncoded {
    /// The dictionary
    pub dictionary: Vec<Vec<u8>>,
    /// Encoded indices
    pub indices: Vec<u32>,
    /// Indices of fallback values
    pub fallback_indices: Vec<u32>,
    /// Fallback values (not in dictionary)
    pub fallback_values: Vec<Vec<u8>>,
}

impl DictionaryEncoded {
    /// Decode back to original values
    pub fn decode(&self) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(self.indices.len());
        let mut fallback_iter = self.fallback_values.iter();

        for &idx in &self.indices {
            if idx == u32::MAX {
                // Fallback value
                if let Some(value) = fallback_iter.next() {
                    result.push(value.clone());
                }
            } else {
                result.push(self.dictionary[idx as usize].clone());
            }
        }

        result
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Dictionary
        result.extend_from_slice(&(self.dictionary.len() as u32).to_le_bytes());
        for value in &self.dictionary {
            result.extend_from_slice(&(value.len() as u32).to_le_bytes());
            result.extend_from_slice(value);
        }

        // Indices
        result.extend_from_slice(&(self.indices.len() as u32).to_le_bytes());
        for &idx in &self.indices {
            result.extend_from_slice(&idx.to_le_bytes());
        }

        // Fallbacks
        result.extend_from_slice(&(self.fallback_values.len() as u32).to_le_bytes());
        for (i, value) in self.fallback_indices.iter().zip(self.fallback_values.iter()) {
            result.extend_from_slice(&i.to_le_bytes());
            result.extend_from_slice(&(value.len() as u32).to_le_bytes());
            result.extend_from_slice(value);
        }

        result
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, CompressionError> {
        let mut pos = 0;

        // Dictionary
        let dict_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut dictionary = Vec::with_capacity(dict_len);
        for _ in 0..dict_len {
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            dictionary.push(data[pos..pos + len].to_vec());
            pos += len;
        }

        // Indices
        let indices_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut indices = Vec::with_capacity(indices_len);
        for _ in 0..indices_len {
            indices.push(u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
            pos += 4;
        }

        // Fallbacks
        let fallback_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut fallback_indices = Vec::with_capacity(fallback_len);
        let mut fallback_values = Vec::with_capacity(fallback_len);
        for _ in 0..fallback_len {
            fallback_indices.push(u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()));
            pos += 4;
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            fallback_values.push(data[pos..pos + len].to_vec());
            pos += len;
        }

        Ok(Self {
            dictionary,
            indices,
            fallback_indices,
            fallback_values,
        })
    }
}

// ============================================================================
// LZ4 Compression
// ============================================================================

/// LZ4 compression codec
pub struct Lz4Codec {
    /// Acceleration factor (1-65537, higher = faster but worse ratio)
    acceleration: i32,
}

impl Lz4Codec {
    pub fn new() -> Self {
        Self { acceleration: 1 }
    }

    pub fn with_acceleration(mut self, acceleration: i32) -> Self {
        self.acceleration = acceleration.max(1).min(65537);
        self
    }
}

impl Default for Lz4Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for Lz4Codec {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // Simplified LZ4-like compression
        // Uses a simple format: [original_size:u32][blocks...]
        // Each block: [token:u8][literal_extra?][literals][offset:u16][match_extra?]
        // Token: upper 4 bits = literal length (15 = more follows), lower 4 bits = match length - 4

        let mut result = Vec::new();

        // Header: original size
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());

        if data.is_empty() {
            return Ok(result);
        }

        let mut pos = 0;

        while pos < data.len() {
            // Collect literals until we find a match
            let literal_start = pos;

            // Find next match position
            let mut match_pos = pos;
            let mut best_match_len = 0usize;
            let mut best_match_offset = 0usize;

            while match_pos < data.len() {
                let (len, offset) = self.find_match(data, match_pos);
                if len >= 4 {
                    best_match_len = len;
                    best_match_offset = offset;
                    break;
                }
                match_pos += 1;
            }

            let literal_len = match_pos - literal_start;

            // Encode token
            let lit_token = literal_len.min(15) as u8;
            let match_token = if best_match_len >= 4 {
                ((best_match_len - 4).min(15)) as u8
            } else {
                0
            };
            let token = (lit_token << 4) | match_token;
            result.push(token);

            // Extended literal length
            if literal_len >= 15 {
                let mut remaining = literal_len - 15;
                while remaining >= 255 {
                    result.push(255);
                    remaining -= 255;
                }
                result.push(remaining as u8);
            }

            // Literals
            result.extend_from_slice(&data[literal_start..match_pos]);
            pos = match_pos;

            // If we have a match
            if best_match_len >= 4 {
                // Offset
                result.extend_from_slice(&(best_match_offset as u16).to_le_bytes());

                // Extended match length
                if best_match_len - 4 >= 15 {
                    let mut remaining = best_match_len - 4 - 15;
                    while remaining >= 255 {
                        result.push(255);
                        remaining -= 255;
                    }
                    result.push(remaining as u8);
                }

                pos += best_match_len;
            }

            // Handle final literals (no match at end)
            if pos >= data.len() && best_match_len < 4 && literal_len == 0 {
                break;
            }
        }

        Ok(result)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let original_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if original_size == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(original_size);
        let mut pos = 4;

        while pos < data.len() && result.len() < original_size {
            if pos >= data.len() {
                break;
            }

            let token = data[pos];
            pos += 1;

            // Literal length
            let mut literal_len = ((token >> 4) & 0x0F) as usize;
            if literal_len == 15 {
                while pos < data.len() {
                    let extra = data[pos] as usize;
                    pos += 1;
                    literal_len += extra;
                    if extra != 255 {
                        break;
                    }
                }
            }

            // Copy literals
            if literal_len > 0 {
                let end = (pos + literal_len).min(data.len());
                let actual_len = end - pos;
                result.extend_from_slice(&data[pos..end]);
                pos = end;

                if actual_len < literal_len {
                    // Not enough data, but we've hit the original size
                    break;
                }
            }

            // Check if we're done
            if result.len() >= original_size {
                break;
            }

            // Match (if there's more data)
            if pos + 2 <= data.len() {
                let offset = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;

                // Match length
                let mut match_len = ((token & 0x0F) as usize) + 4;
                if (token & 0x0F) == 15 {
                    while pos < data.len() {
                        let extra = data[pos] as usize;
                        pos += 1;
                        match_len += extra;
                        if extra != 255 {
                            break;
                        }
                    }
                }

                // Copy match
                if offset > 0 && offset <= result.len() {
                    let match_start = result.len() - offset;
                    for i in 0..match_len {
                        if result.len() >= original_size {
                            break;
                        }
                        let byte = result[match_start + (i % offset)];
                        result.push(byte);
                    }
                }
            }
        }

        result.truncate(original_size);
        Ok(result)
    }

    fn codec_type(&self) -> CodecType {
        CodecType::Lz4
    }
}

impl Lz4Codec {
    fn find_match(&self, data: &[u8], pos: usize) -> (usize, usize) {
        if pos == 0 {
            return (0, 0);
        }

        let mut best_len = 0;
        let mut best_offset = 0;

        // Look back up to 64KB (but limit search for performance)
        let search_start = pos.saturating_sub(4096);

        for i in search_start..pos {
            let mut len = 0;
            while pos + len < data.len() && i + len < pos && data[i + len] == data[pos + len] && len < 255 {
                len += 1;
            }

            if len > best_len {
                best_len = len;
                best_offset = pos - i;
            }
        }

        (best_len, best_offset)
    }
}

// ============================================================================
// ZSTD Compression
// ============================================================================

/// ZSTD compression codec
pub struct ZstdCodec {
    /// Compression level (1-22)
    level: i32,
}

impl ZstdCodec {
    pub fn new(level: i32) -> Self {
        Self {
            level: level.max(1).min(22),
        }
    }

    pub fn fast() -> Self {
        Self::new(1)
    }

    pub fn default_level() -> Self {
        Self::new(3)
    }

    pub fn high() -> Self {
        Self::new(19)
    }
}

impl Default for ZstdCodec {
    fn default() -> Self {
        Self::default_level()
    }
}

impl Codec for ZstdCodec {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        // Simplified ZSTD-like compression
        // Real implementation would use zstd crate

        let mut result = Vec::new();

        // Magic number
        result.extend_from_slice(&[0x28, 0xB5, 0x2F, 0xFD]);

        // Frame header
        result.push(0x00); // Frame header descriptor
        result.extend_from_slice(&(data.len() as u32).to_le_bytes()); // Original size

        // Simple compression using dictionary + Huffman-like encoding
        // (This is a simplified simulation)

        // Build frequency table
        let mut freq = [0u32; 256];
        for &byte in data {
            freq[byte as usize] += 1;
        }

        // Write frequency table (simplified)
        for &f in &freq {
            result.push((f.min(255)) as u8);
        }

        // Write compressed data (simplified - just copy for demo)
        // Real ZSTD uses FSE entropy coding
        result.extend_from_slice(data);

        Ok(result)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        // Check magic
        if &data[0..4] != &[0x28, 0xB5, 0x2F, 0xFD] {
            return Err(CompressionError::InvalidData("Invalid ZSTD magic".to_string()));
        }

        // Read frame header
        let _descriptor = data[4];
        let original_size = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;

        // Skip frequency table (256 bytes)
        let data_start = 9 + 256;

        if data_start > data.len() {
            return Err(CompressionError::InvalidData("Truncated".to_string()));
        }

        // For our simplified version, data is stored uncompressed after header
        let mut result = Vec::with_capacity(original_size);
        result.extend_from_slice(&data[data_start..]);
        result.truncate(original_size);

        Ok(result)
    }

    fn codec_type(&self) -> CodecType {
        CodecType::Zstd
    }
}

// ============================================================================
// Delta Encoding
// ============================================================================

/// Delta encoder for integer sequences
pub struct DeltaEncoder;

impl DeltaEncoder {
    /// Encode i64 values as deltas
    pub fn encode_i64(values: &[i64]) -> Vec<u8> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();

        // Number of values
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // First value (full)
        result.extend_from_slice(&values[0].to_le_bytes());

        // Delta values (as variable-length integers)
        let mut prev = values[0];
        for &value in &values[1..] {
            let delta = value.wrapping_sub(prev);
            Self::write_varint(&mut result, delta);
            prev = value;
        }

        result
    }

    /// Decode deltas back to i64 values
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>, CompressionError> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let num_values = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if num_values == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(num_values);

        // First value
        let first = i64::from_le_bytes(data[4..12].try_into().unwrap());
        result.push(first);

        // Delta values
        let mut pos = 12;
        let mut prev = first;

        while result.len() < num_values && pos < data.len() {
            let (delta, bytes_read) = Self::read_varint(&data[pos..])?;
            pos += bytes_read;
            prev = prev.wrapping_add(delta);
            result.push(prev);
        }

        Ok(result)
    }

    /// Write variable-length integer (zigzag encoded)
    fn write_varint(buf: &mut Vec<u8>, value: i64) {
        // Zigzag encode to handle negative numbers efficiently
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;

        let mut v = zigzag;
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                buf.push(byte);
                break;
            } else {
                buf.push(byte | 0x80);
            }
        }
    }

    /// Read variable-length integer
    fn read_varint(data: &[u8]) -> Result<(i64, usize), CompressionError> {
        let mut result = 0u64;
        let mut shift = 0;

        for (i, &byte) in data.iter().enumerate() {
            result |= ((byte & 0x7F) as u64) << shift;
            shift += 7;

            if byte & 0x80 == 0 {
                // Zigzag decode
                let decoded = ((result >> 1) as i64) ^ (-((result & 1) as i64));
                return Ok((decoded, i + 1));
            }

            if shift >= 64 {
                return Err(CompressionError::InvalidData("Varint too long".to_string()));
            }
        }

        Err(CompressionError::InvalidData("Truncated varint".to_string()))
    }
}

// ============================================================================
// Double-Delta Encoding
// ============================================================================

/// Double-delta encoder for timestamps and monotonic sequences
pub struct DoubleDeltaEncoder;

impl DoubleDeltaEncoder {
    /// Encode i64 values as double-deltas
    pub fn encode_i64(values: &[i64]) -> Vec<u8> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();

        // Number of values
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        // First value (full)
        result.extend_from_slice(&values[0].to_le_bytes());

        if values.len() == 1 {
            return result;
        }

        // First delta (full)
        let first_delta = values[1].wrapping_sub(values[0]);
        result.extend_from_slice(&first_delta.to_le_bytes());

        // Double-delta values
        let mut prev_value = values[1];
        let mut prev_delta = first_delta;

        for &value in &values[2..] {
            let delta = value.wrapping_sub(prev_value);
            let double_delta = delta.wrapping_sub(prev_delta);

            // Encode double-delta with bit-width prefix
            Self::write_double_delta(&mut result, double_delta);

            prev_value = value;
            prev_delta = delta;
        }

        result
    }

    /// Decode double-deltas back to i64 values
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let num_values = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if num_values == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(num_values);

        // First value
        let first = i64::from_le_bytes(data[4..12].try_into().unwrap());
        result.push(first);

        if num_values == 1 {
            return Ok(result);
        }

        // First delta
        let first_delta = i64::from_le_bytes(data[12..20].try_into().unwrap());
        result.push(first.wrapping_add(first_delta));

        // Double-delta values
        let mut pos = 20;
        let mut prev_value = result[1];
        let mut prev_delta = first_delta;

        while result.len() < num_values && pos < data.len() {
            let (double_delta, bytes_read) = Self::read_double_delta(&data[pos..])?;
            pos += bytes_read;

            let delta = prev_delta.wrapping_add(double_delta);
            let value = prev_value.wrapping_add(delta);

            result.push(value);
            prev_value = value;
            prev_delta = delta;
        }

        Ok(result)
    }

    /// Write double-delta with adaptive bit-width
    fn write_double_delta(buf: &mut Vec<u8>, dd: i64) {
        // Use different encodings based on magnitude
        if dd == 0 {
            // Single bit: 0
            buf.push(0b00000000);
        } else if dd >= -64 && dd <= 63 {
            // 7 bits + 1 bit marker
            buf.push(0b10000000 | ((dd as i8) as u8 & 0x7F));
        } else if dd >= -8192 && dd <= 8191 {
            // 14 bits + 2 bit marker
            let encoded = (dd as i16) as u16 & 0x3FFF;
            buf.push(0b11000000 | ((encoded >> 8) as u8 & 0x3F));
            buf.push((encoded & 0xFF) as u8);
        } else {
            // Full 64-bit
            buf.push(0b11110000);
            buf.extend_from_slice(&dd.to_le_bytes());
        }
    }

    /// Read double-delta with adaptive bit-width
    fn read_double_delta(data: &[u8]) -> Result<(i64, usize), CompressionError> {
        if data.is_empty() {
            return Err(CompressionError::InvalidData("Empty data".to_string()));
        }

        let first = data[0];

        if first == 0b00000000 {
            // Zero
            Ok((0, 1))
        } else if first & 0b10000000 != 0 && first & 0b01000000 == 0 {
            // 7-bit value
            let value = ((first & 0x7F) as i8) as i64;
            // Sign extend from 7 bits
            let value = if value & 0x40 != 0 {
                value | !0x7F
            } else {
                value
            };
            Ok((value, 1))
        } else if first & 0b11000000 == 0b11000000 && first & 0b00110000 != 0b00110000 {
            // 14-bit value
            if data.len() < 2 {
                return Err(CompressionError::InvalidData("Truncated".to_string()));
            }
            let high = ((first & 0x3F) as u16) << 8;
            let low = data[1] as u16;
            let value = (high | low) as i16 as i64;
            Ok((value, 2))
        } else if first == 0b11110000 {
            // Full 64-bit
            if data.len() < 9 {
                return Err(CompressionError::InvalidData("Truncated".to_string()));
            }
            let value = i64::from_le_bytes(data[1..9].try_into().unwrap());
            Ok((value, 9))
        } else {
            Err(CompressionError::InvalidData("Invalid encoding".to_string()))
        }
    }
}

// ============================================================================
// Run-Length Encoding
// ============================================================================

/// Run-length encoder
pub struct RleEncoder;

impl RleEncoder {
    /// Encode bytes with run-length encoding
    pub fn encode(data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();

        // Header: original size
        result.extend_from_slice(&(data.len() as u32).to_le_bytes());

        let mut pos = 0;
        while pos < data.len() {
            let value = data[pos];
            let mut run_len = 1usize;

            // Count run length
            while pos + run_len < data.len() && data[pos + run_len] == value && run_len < 127 {
                run_len += 1;
            }

            if run_len >= 3 {
                // Encode as run: marker (0x80 | length), value
                result.push(0x80 | (run_len as u8));
                result.push(value);
                pos += run_len;
            } else {
                // Collect literals
                let literal_start = pos;
                let mut literal_len = 0;

                while pos + literal_len < data.len() && literal_len < 127 {
                    // Check if next few bytes form a run
                    let check_pos = pos + literal_len;
                    let mut run = 1;
                    while check_pos + run < data.len()
                        && data[check_pos + run] == data[check_pos]
                        && run < 3
                    {
                        run += 1;
                    }

                    if run >= 3 {
                        break;
                    }
                    literal_len += 1;
                }

                if literal_len > 0 {
                    // Encode as literals: length, values...
                    result.push(literal_len as u8);
                    result.extend_from_slice(&data[literal_start..literal_start + literal_len]);
                    pos += literal_len;
                }
            }
        }

        result
    }

    /// Decode run-length encoded data
    pub fn decode(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let original_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(original_size);
        let mut pos = 4;

        while pos < data.len() && result.len() < original_size {
            let control = data[pos];
            pos += 1;

            if control & 0x80 != 0 {
                // Run
                let run_len = (control & 0x7F) as usize;
                if pos >= data.len() {
                    return Err(CompressionError::InvalidData("Truncated run".to_string()));
                }
                let value = data[pos];
                pos += 1;

                for _ in 0..run_len {
                    if result.len() >= original_size {
                        break;
                    }
                    result.push(value);
                }
            } else {
                // Literals
                let literal_len = control as usize;
                if pos + literal_len > data.len() {
                    return Err(CompressionError::InvalidData("Truncated literals".to_string()));
                }

                for &byte in &data[pos..pos + literal_len] {
                    if result.len() >= original_size {
                        break;
                    }
                    result.push(byte);
                }
                pos += literal_len;
            }
        }

        Ok(result)
    }

    /// Encode i64 values with RLE
    pub fn encode_i64(values: &[i64]) -> Vec<u8> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();

        // Header: number of values
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        let mut pos = 0;
        while pos < values.len() {
            let value = values[pos];
            let mut run_len = 1usize;

            // Count run length
            while pos + run_len < values.len() && values[pos + run_len] == value && run_len < 65535 {
                run_len += 1;
            }

            // Write run: length (u16), value (i64)
            result.extend_from_slice(&(run_len as u16).to_le_bytes());
            result.extend_from_slice(&value.to_le_bytes());
            pos += run_len;
        }

        result
    }

    /// Decode RLE i64 values
    pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>, CompressionError> {
        if data.len() < 4 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let num_values = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(num_values);
        let mut pos = 4;

        while pos + 10 <= data.len() && result.len() < num_values {
            let run_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let value = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            for _ in 0..run_len {
                if result.len() >= num_values {
                    break;
                }
                result.push(value);
            }
        }

        Ok(result)
    }
}

// ============================================================================
// Bit-Packing
// ============================================================================

/// Bit-packing encoder for small integers
pub struct BitPackEncoder;

impl BitPackEncoder {
    /// Calculate minimum bits needed to store values
    pub fn min_bits(values: &[u64]) -> u8 {
        let max_value = values.iter().copied().max().unwrap_or(0);
        if max_value == 0 {
            return 1;
        }
        (64 - max_value.leading_zeros()) as u8
    }

    /// Encode u64 values with bit-packing
    pub fn encode(values: &[u64], bits: u8) -> Vec<u8> {
        if values.is_empty() || bits == 0 {
            return Vec::new();
        }

        let bits = bits.min(64);
        let total_bits = values.len() * bits as usize;
        let num_bytes = (total_bits + 7) / 8;

        let mut result = Vec::with_capacity(num_bytes + 5);

        // Header: number of values (u32), bits per value (u8)
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());
        result.push(bits);

        // Pack values
        let mut bit_pos = 0usize;
        let mask = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };

        // Pre-allocate packed bytes
        result.resize(result.len() + num_bytes, 0);
        let packed_start = 5;

        for &value in values {
            let value = value & mask;
            let byte_pos = bit_pos / 8;
            let bit_offset = bit_pos % 8;

            // Write value across potentially multiple bytes
            let mut v = value << bit_offset;
            let mut remaining_bits = bits as usize + bit_offset;
            let mut b = byte_pos;

            while remaining_bits > 0 && b < num_bytes {
                result[packed_start + b] |= (v & 0xFF) as u8;
                v >>= 8;
                remaining_bits = remaining_bits.saturating_sub(8);
                b += 1;
            }

            bit_pos += bits as usize;
        }

        result
    }

    /// Decode bit-packed values
    pub fn decode(data: &[u8]) -> Result<Vec<u64>, CompressionError> {
        if data.len() < 5 {
            return Err(CompressionError::InvalidData("Too short".to_string()));
        }

        let num_values = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let bits = data[4];

        if bits == 0 || bits > 64 {
            return Err(CompressionError::InvalidData("Invalid bit width".to_string()));
        }

        let mask = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
        let mut result = Vec::with_capacity(num_values);
        let packed = &data[5..];

        let mut bit_pos = 0usize;

        for _ in 0..num_values {
            let byte_pos = bit_pos / 8;
            let bit_offset = bit_pos % 8;

            // Read value across potentially multiple bytes
            let mut value = 0u64;
            let mut bits_read = 0;
            let mut b = byte_pos;

            while bits_read < bits as usize && b < packed.len() {
                let byte_val = packed[b] as u64;
                let available = 8 - if b == byte_pos { bit_offset } else { 0 };
                let to_read = (bits as usize - bits_read).min(available);

                let shift_down = if b == byte_pos { bit_offset } else { 0 };
                let extracted = (byte_val >> shift_down) & ((1u64 << to_read) - 1);

                value |= extracted << bits_read;
                bits_read += to_read;
                b += 1;
            }

            result.push(value & mask);
            bit_pos += bits as usize;
        }

        Ok(result)
    }
}

// ============================================================================
// Compression Manager
// ============================================================================

/// Compression manager for selecting and applying codecs
pub struct CompressionManager {
    /// Default codec for each column type
    default_codecs: HashMap<String, CodecType>,
}

impl CompressionManager {
    pub fn new() -> Self {
        let mut default_codecs = HashMap::new();
        default_codecs.insert("string".to_string(), CodecType::Dictionary);
        default_codecs.insert("int64".to_string(), CodecType::Delta);
        default_codecs.insert("timestamp".to_string(), CodecType::DoubleDelta);
        default_codecs.insert("float64".to_string(), CodecType::Xor);
        default_codecs.insert("bytes".to_string(), CodecType::Lz4);

        Self { default_codecs }
    }

    /// Analyze data and recommend best codec
    pub fn recommend_codec(&self, data: &[u8], data_type: &str) -> CodecType {
        // Check for high repetition (good for RLE)
        let repetition = self.measure_repetition(data);
        if repetition > 0.5 {
            return CodecType::Rle;
        }

        // Check default for type
        if let Some(&codec) = self.default_codecs.get(data_type) {
            return codec;
        }

        // Default to LZ4
        CodecType::Lz4
    }

    fn measure_repetition(&self, data: &[u8]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }

        let mut repeated = 0;
        for window in data.windows(2) {
            if window[0] == window[1] {
                repeated += 1;
            }
        }

        repeated as f64 / (data.len() - 1) as f64
    }

    /// Compress with automatic codec selection
    pub fn compress_auto(&self, data: &[u8], data_type: &str) -> Result<(CodecType, Vec<u8>), CompressionError> {
        let codec = self.recommend_codec(data, data_type);
        let compressed = self.compress(data, codec)?;
        Ok((codec, compressed))
    }

    /// Compress with specific codec
    pub fn compress(&self, data: &[u8], codec: CodecType) -> Result<Vec<u8>, CompressionError> {
        match codec {
            CodecType::None => Ok(data.to_vec()),
            CodecType::Lz4 => Lz4Codec::new().compress(data),
            CodecType::Zstd => ZstdCodec::default().compress(data),
            CodecType::Rle => Ok(RleEncoder::encode(data)),
            _ => Err(CompressionError::UnsupportedCodec(format!("{:?}", codec))),
        }
    }

    /// Decompress with specific codec
    pub fn decompress(&self, data: &[u8], codec: CodecType) -> Result<Vec<u8>, CompressionError> {
        match codec {
            CodecType::None => Ok(data.to_vec()),
            CodecType::Lz4 => Lz4Codec::new().decompress(data),
            CodecType::Zstd => ZstdCodec::default().decompress(data),
            CodecType::Rle => RleEncoder::decode(data),
            _ => Err(CompressionError::UnsupportedCodec(format!("{:?}", codec))),
        }
    }
}

impl Default for CompressionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Dictionary tests
    #[test]
    fn test_dictionary_encoder_basic() {
        let mut encoder = DictionaryEncoder::new(1000);

        let idx1 = encoder.add(b"hello").unwrap();
        let idx2 = encoder.add(b"world").unwrap();
        let idx3 = encoder.add(b"hello").unwrap(); // Duplicate

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, idx1); // Same as first

        assert_eq!(encoder.get_value(0), Some(b"hello".as_slice()));
        assert_eq!(encoder.get_value(1), Some(b"world".as_slice()));
    }

    #[test]
    fn test_dictionary_encoder_max_size() {
        let mut encoder = DictionaryEncoder::new(2);

        encoder.add(b"a").unwrap();
        encoder.add(b"b").unwrap();
        let result = encoder.add(b"c");

        assert!(matches!(result, Err(CompressionError::DictionaryFull)));
    }

    #[test]
    fn test_dictionary_encode_decode() {
        let mut encoder = DictionaryEncoder::new(100);

        let values: Vec<&[u8]> = vec![b"foo", b"bar", b"foo", b"baz", b"foo"];
        encoder.build_from_values(&values).unwrap();

        let encoded = encoder.encode(&values).unwrap();
        let decoded = encoded.decode();

        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], b"foo");
        assert_eq!(decoded[1], b"bar");
        assert_eq!(decoded[2], b"foo");
    }

    #[test]
    fn test_dictionary_serialization() {
        let mut encoder = DictionaryEncoder::new(100);
        encoder.add(b"test1").unwrap();
        encoder.add(b"test2").unwrap();
        encoder.add(b"test3").unwrap();

        let bytes = encoder.to_bytes();
        let restored = DictionaryEncoder::from_bytes(&bytes).unwrap();

        assert_eq!(restored.len(), 3);
        assert_eq!(restored.get_value(0), Some(b"test1".as_slice()));
        assert_eq!(restored.get_value(2), Some(b"test3".as_slice()));
    }

    #[test]
    fn test_dictionary_encoded_serialization() {
        let encoded = DictionaryEncoded {
            dictionary: vec![b"a".to_vec(), b"b".to_vec()],
            indices: vec![0, 1, 0, 1, 0],
            fallback_indices: vec![],
            fallback_values: vec![],
        };

        let bytes = encoded.to_bytes();
        let restored = DictionaryEncoded::from_bytes(&bytes).unwrap();

        assert_eq!(restored.indices, encoded.indices);
        assert_eq!(restored.dictionary, encoded.dictionary);
    }

    // LZ4 tests
    #[test]
    fn test_lz4_compress_decompress() {
        let codec = Lz4Codec::new();
        let data = b"hello hello hello world world";

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_empty() {
        let codec = Lz4Codec::new();

        let compressed = codec.compress(b"").unwrap();
        let decompressed = codec.decompress(&compressed).unwrap();

        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_lz4_random_data() {
        let codec = Lz4Codec::new();
        let data: Vec<u8> = (0..1000).map(|i| (i * 17 % 256) as u8).collect();

        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    // ZSTD tests
    #[test]
    fn test_zstd_compress_decompress() {
        let codec = ZstdCodec::default();
        let data = b"test data for zstd compression";

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_levels() {
        let data = b"repeated data repeated data repeated data";

        let fast = ZstdCodec::fast().compress(data).unwrap();
        let high = ZstdCodec::high().compress(data).unwrap();

        // Both should decompress correctly
        assert_eq!(ZstdCodec::fast().decompress(&fast).unwrap(), data);
        assert_eq!(ZstdCodec::high().decompress(&high).unwrap(), data);
    }

    // Delta encoding tests
    #[test]
    fn test_delta_encode_decode() {
        let values = vec![100i64, 105, 110, 115, 120, 125];

        let encoded = DeltaEncoder::encode_i64(&values);
        let decoded = DeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_negative() {
        let values = vec![100i64, 90, 80, 70, 60];

        let encoded = DeltaEncoder::encode_i64(&values);
        let decoded = DeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_empty() {
        let values: Vec<i64> = vec![];

        let encoded = DeltaEncoder::encode_i64(&values);
        let decoded = DeltaEncoder::decode_i64(&encoded).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_delta_single() {
        let values = vec![42i64];

        let encoded = DeltaEncoder::encode_i64(&values);
        let decoded = DeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    // Double-delta tests
    #[test]
    fn test_double_delta_monotonic() {
        // Monotonically increasing timestamps (constant delta)
        let values: Vec<i64> = (0..100).map(|i| 1000 + i * 10).collect();

        let encoded = DoubleDeltaEncoder::encode_i64(&values);
        let decoded = DoubleDeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);

        // Should compress very well since double-deltas are 0
        assert!(encoded.len() < values.len() * 8);
    }

    #[test]
    fn test_double_delta_varying() {
        let values = vec![100i64, 110, 125, 145, 170, 200];

        let encoded = DoubleDeltaEncoder::encode_i64(&values);
        let decoded = DoubleDeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_double_delta_timestamps() {
        // Realistic timestamp sequence (milliseconds)
        let values: Vec<i64> = vec![
            1700000000000,
            1700000001000,
            1700000002000,
            1700000003000,
            1700000004000,
        ];

        let encoded = DoubleDeltaEncoder::encode_i64(&values);
        let decoded = DoubleDeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    // RLE tests
    #[test]
    fn test_rle_bytes() {
        let data = b"aaaaaabbbbcccccccccc";

        let encoded = RleEncoder::encode(data);
        let decoded = RleEncoder::decode(&encoded).unwrap();

        assert_eq!(decoded, data);
        assert!(encoded.len() < data.len()); // Should compress
    }

    #[test]
    fn test_rle_no_runs() {
        let data = b"abcdefghij";

        let encoded = RleEncoder::encode(data);
        let decoded = RleEncoder::decode(&encoded).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_rle_i64() {
        let values = vec![1i64, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3];

        let encoded = RleEncoder::encode_i64(&values);
        let decoded = RleEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_rle_single_value() {
        let values = vec![42i64; 1000];

        let encoded = RleEncoder::encode_i64(&values);
        let decoded = RleEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
        assert!(encoded.len() < 100); // Should compress very well
    }

    // Bit-packing tests
    #[test]
    fn test_bitpack_small_values() {
        let values: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7];
        let bits = BitPackEncoder::min_bits(&values);

        assert_eq!(bits, 3); // Need 3 bits to store 7

        let encoded = BitPackEncoder::encode(&values, bits);
        let decoded = BitPackEncoder::decode(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_bitpack_various_widths() {
        for bits in [1, 4, 8, 16, 32] {
            let max_val = (1u64 << bits) - 1;
            let values: Vec<u64> = (0..100).map(|i| i % max_val).collect();

            let encoded = BitPackEncoder::encode(&values, bits);
            let decoded = BitPackEncoder::decode(&encoded).unwrap();

            assert_eq!(decoded, values, "Failed for {} bits", bits);
        }
    }

    #[test]
    fn test_bitpack_min_bits() {
        assert_eq!(BitPackEncoder::min_bits(&[0]), 1);
        assert_eq!(BitPackEncoder::min_bits(&[1]), 1);
        assert_eq!(BitPackEncoder::min_bits(&[2]), 2);
        assert_eq!(BitPackEncoder::min_bits(&[7]), 3);
        assert_eq!(BitPackEncoder::min_bits(&[255]), 8);
        assert_eq!(BitPackEncoder::min_bits(&[256]), 9);
    }

    // Compression manager tests
    #[test]
    fn test_compression_manager_auto() {
        let manager = CompressionManager::new();

        // Repetitive data should use RLE
        let repetitive = vec![0u8; 1000];
        let (codec, _) = manager.compress_auto(&repetitive, "bytes").unwrap();
        assert_eq!(codec, CodecType::Rle);
    }

    #[test]
    fn test_compression_manager_roundtrip() {
        let manager = CompressionManager::new();
        let data = b"test compression roundtrip data";

        let compressed = manager.compress(data, CodecType::Lz4).unwrap();
        let decompressed = manager.decompress(&compressed, CodecType::Lz4).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats {
            original_size: 1000,
            compressed_size: 250,
            num_values: 100,
            compress_time_us: 50,
            decompress_time_us: 20,
        };

        assert!((stats.compression_ratio() - 4.0).abs() < 0.001);
        assert!((stats.space_savings() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_varint_encoding() {
        // Test various values
        let test_values: Vec<i64> = vec![0, 1, -1, 127, -128, 10000, -10000, i64::MAX, i64::MIN];

        for &value in &test_values {
            let values = vec![0i64, value]; // Need at least 2 for delta
            let encoded = DeltaEncoder::encode_i64(&values);
            let decoded = DeltaEncoder::decode_i64(&encoded).unwrap();
            assert_eq!(decoded[1], value, "Failed for {}", value);
        }
    }

    #[test]
    fn test_double_delta_large_gaps() {
        let values = vec![0i64, 1000000, 2000000, 3000000];

        let encoded = DoubleDeltaEncoder::encode_i64(&values);
        let decoded = DoubleDeltaEncoder::decode_i64(&encoded).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_codec_type() {
        let lz4 = Lz4Codec::new();
        let zstd = ZstdCodec::default();

        assert_eq!(lz4.codec_type(), CodecType::Lz4);
        assert_eq!(zstd.codec_type(), CodecType::Zstd);
    }
}
