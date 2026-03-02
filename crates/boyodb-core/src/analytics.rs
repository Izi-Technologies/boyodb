// Advanced Analytics Module
// Quantiles, GROUPING SETS, Array/JSON, ASOF JOIN, Probabilistic Sketches

use std::collections::{HashMap, HashSet, BTreeMap};
use std::cmp::Ordering;

// ============================================================================
// Quantile/Percentile Functions
// ============================================================================

/// T-Digest for streaming quantile estimation
/// Based on Dunning's T-Digest algorithm
#[derive(Debug, Clone)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    compression: f64,
    total_weight: f64,
    max_centroids: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

impl TDigest {
    /// Create new T-Digest with given compression factor
    /// Higher compression = more accuracy but more memory
    pub fn new(compression: f64) -> Self {
        Self {
            centroids: Vec::new(),
            compression,
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
        if weight <= 0.0 || !value.is_finite() {
            return;
        }

        // Find insertion point
        let idx = self.centroids
            .binary_search_by(|c| c.mean.partial_cmp(&value).unwrap_or(Ordering::Equal))
            .unwrap_or_else(|i| i);

        // Try to merge with nearest centroid
        if !self.centroids.is_empty() {
            let nearest = if idx == 0 {
                0
            } else if idx >= self.centroids.len() {
                self.centroids.len() - 1
            } else {
                let dist_left = (self.centroids[idx - 1].mean - value).abs();
                let dist_right = (self.centroids[idx].mean - value).abs();
                if dist_left < dist_right { idx - 1 } else { idx }
            };

            let q = self.quantile_at_centroid(nearest);
            let max_weight = self.max_weight(q);

            if self.centroids[nearest].weight + weight <= max_weight {
                // Merge into existing centroid
                let c = &mut self.centroids[nearest];
                let new_weight = c.weight + weight;
                c.mean = (c.mean * c.weight + value * weight) / new_weight;
                c.weight = new_weight;
                self.total_weight += weight;
                return;
            }
        }

        // Insert new centroid
        self.centroids.insert(idx, Centroid { mean: value, weight });
        self.total_weight += weight;

        // Compress if needed
        if self.centroids.len() > self.max_centroids {
            self.compress();
        }
    }

    /// Merge another T-Digest into this one
    pub fn merge(&mut self, other: &TDigest) {
        for centroid in &other.centroids {
            self.add_weighted(centroid.mean, centroid.weight);
        }
    }

    /// Get quantile value (0.0 to 1.0)
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() || q < 0.0 || q > 1.0 {
            return None;
        }

        if q == 0.0 {
            return Some(self.centroids.first()?.mean);
        }
        if q == 1.0 {
            return Some(self.centroids.last()?.mean);
        }

        let target_weight = q * self.total_weight;
        let mut cumulative = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let next_cumulative = cumulative + c.weight;

            if target_weight <= next_cumulative {
                // Interpolate within this centroid
                if i == 0 || c.weight == 0.0 {
                    return Some(c.mean);
                }

                let prev = &self.centroids[i - 1];
                let fraction = (target_weight - cumulative) / c.weight;
                return Some(prev.mean + (c.mean - prev.mean) * fraction);
            }

            cumulative = next_cumulative;
        }

        Some(self.centroids.last()?.mean)
    }

    /// Get percentile (0 to 100)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        self.quantile(p / 100.0)
    }

    /// Get median (p50)
    pub fn median(&self) -> Option<f64> {
        self.quantile(0.5)
    }

    fn quantile_at_centroid(&self, idx: usize) -> f64 {
        if self.total_weight == 0.0 {
            return 0.5;
        }
        let mut cumulative = 0.0;
        for i in 0..idx {
            cumulative += self.centroids[i].weight;
        }
        cumulative += self.centroids[idx].weight / 2.0;
        cumulative / self.total_weight
    }

    fn max_weight(&self, q: f64) -> f64 {
        let k = 4.0 * self.compression * q * (1.0 - q);
        k.max(1.0) * self.total_weight / self.compression
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 {
            return;
        }

        // Sort by mean
        self.centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal));

        let mut new_centroids = Vec::with_capacity(self.max_centroids);
        let mut current = self.centroids[0];
        let mut cumulative = 0.0;

        for i in 1..self.centroids.len() {
            let c = &self.centroids[i];
            let q = (cumulative + current.weight / 2.0) / self.total_weight;
            let max_w = self.max_weight(q);

            if current.weight + c.weight <= max_w {
                // Merge
                let new_weight = current.weight + c.weight;
                current.mean = (current.mean * current.weight + c.mean * c.weight) / new_weight;
                current.weight = new_weight;
            } else {
                cumulative += current.weight;
                new_centroids.push(current);
                current = *c;
            }
        }
        new_centroids.push(current);

        self.centroids = new_centroids;
    }

    pub fn count(&self) -> f64 {
        self.total_weight
    }

    pub fn is_empty(&self) -> bool {
        self.centroids.is_empty()
    }
}

/// Exact quantile calculation for small datasets
pub fn exact_quantile(values: &mut [f64], q: f64) -> Option<f64> {
    if values.is_empty() || q < 0.0 || q > 1.0 {
        return None;
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let n = values.len();
    let pos = q * (n - 1) as f64;
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;
    let fraction = pos - lower as f64;

    if lower == upper || upper >= n {
        Some(values[lower.min(n - 1)])
    } else {
        Some(values[lower] * (1.0 - fraction) + values[upper] * fraction)
    }
}

/// Multiple quantiles at once (more efficient)
pub fn exact_quantiles(values: &mut [f64], quantiles: &[f64]) -> Vec<Option<f64>> {
    if values.is_empty() {
        return quantiles.iter().map(|_| None).collect();
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let n = values.len();

    quantiles.iter().map(|&q| {
        if q < 0.0 || q > 1.0 {
            return None;
        }
        let pos = q * (n - 1) as f64;
        let lower = pos.floor() as usize;
        let upper = pos.ceil() as usize;
        let fraction = pos - lower as f64;

        if lower == upper || upper >= n {
            Some(values[lower.min(n - 1)])
        } else {
            Some(values[lower] * (1.0 - fraction) + values[upper] * fraction)
        }
    }).collect()
}

// ============================================================================
// GROUPING SETS / CUBE / ROLLUP
// ============================================================================

/// Represents a grouping set for multi-dimensional aggregation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupingSet {
    /// Columns included in this grouping (indices into group_by list)
    pub columns: Vec<usize>,
}

impl GroupingSet {
    pub fn new(columns: Vec<usize>) -> Self {
        Self { columns }
    }

    pub fn all(n: usize) -> Self {
        Self { columns: (0..n).collect() }
    }

    pub fn empty() -> Self {
        Self { columns: Vec::new() }
    }
}

/// Generate grouping sets for ROLLUP
/// ROLLUP(a, b, c) = { (a,b,c), (a,b), (a), () }
pub fn rollup_sets(n: usize) -> Vec<GroupingSet> {
    let mut sets = Vec::with_capacity(n + 1);
    for i in (0..=n).rev() {
        sets.push(GroupingSet::new((0..i).collect()));
    }
    sets
}

/// Generate grouping sets for CUBE
/// CUBE(a, b) = { (a,b), (a), (b), () }
pub fn cube_sets(n: usize) -> Vec<GroupingSet> {
    let mut sets = Vec::with_capacity(1 << n);
    for mask in 0..(1 << n) {
        let columns: Vec<usize> = (0..n).filter(|&i| (mask >> i) & 1 == 1).collect();
        sets.push(GroupingSet::new(columns));
    }
    // Sort by number of columns (descending) for consistent ordering
    sets.sort_by(|a, b| b.columns.len().cmp(&a.columns.len()));
    sets
}

/// Compute GROUPING() function value for a set
/// Returns bitmask indicating which columns are aggregated (not grouped)
pub fn grouping_value(set: &GroupingSet, total_columns: usize) -> u64 {
    let mut value = 0u64;
    for i in 0..total_columns {
        if !set.columns.contains(&i) {
            value |= 1 << (total_columns - 1 - i);
        }
    }
    value
}

/// Key for grouping set aggregation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupingKey {
    /// Which grouping set this belongs to
    pub set_index: usize,
    /// Values for each grouped column (None = aggregated/NULL)
    pub values: Vec<Option<GroupValue>>,
}

/// Value that can be used as a grouping key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupValue {
    Int64(i64),
    String(String),
    Bool(bool),
    Null,
}

impl GroupValue {
    pub fn from_i64(v: i64) -> Self {
        GroupValue::Int64(v)
    }

    pub fn from_str(s: &str) -> Self {
        GroupValue::String(s.to_string())
    }
}

// ============================================================================
// Array Type Support
// ============================================================================

/// Array value type for SQL arrays
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayValue {
    Int64Array(Vec<i64>),
    Float64Array(Vec<f64>),
    StringArray(Vec<String>),
    BoolArray(Vec<bool>),
    NestedArray(Vec<ArrayValue>),
    Null,
}

impl ArrayValue {
    /// Get array length
    pub fn len(&self) -> usize {
        match self {
            ArrayValue::Int64Array(v) => v.len(),
            ArrayValue::Float64Array(v) => v.len(),
            ArrayValue::StringArray(v) => v.len(),
            ArrayValue::BoolArray(v) => v.len(),
            ArrayValue::NestedArray(v) => v.len(),
            ArrayValue::Null => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Array contains check
    pub fn contains(&self, value: &JsonValue) -> bool {
        match (self, value) {
            (ArrayValue::Int64Array(arr), JsonValue::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    arr.contains(&i)
                } else {
                    false
                }
            }
            (ArrayValue::Float64Array(arr), JsonValue::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    arr.iter().any(|&x| (x - f).abs() < f64::EPSILON)
                } else {
                    false
                }
            }
            (ArrayValue::StringArray(arr), JsonValue::String(s)) => arr.contains(s),
            (ArrayValue::BoolArray(arr), JsonValue::Bool(b)) => arr.contains(b),
            _ => false,
        }
    }

    /// Array concatenation
    pub fn concat(&self, other: &ArrayValue) -> Option<ArrayValue> {
        match (self, other) {
            (ArrayValue::Int64Array(a), ArrayValue::Int64Array(b)) => {
                let mut result = a.clone();
                result.extend(b);
                Some(ArrayValue::Int64Array(result))
            }
            (ArrayValue::Float64Array(a), ArrayValue::Float64Array(b)) => {
                let mut result = a.clone();
                result.extend(b);
                Some(ArrayValue::Float64Array(result))
            }
            (ArrayValue::StringArray(a), ArrayValue::StringArray(b)) => {
                let mut result = a.clone();
                result.extend(b.iter().cloned());
                Some(ArrayValue::StringArray(result))
            }
            (ArrayValue::BoolArray(a), ArrayValue::BoolArray(b)) => {
                let mut result = a.clone();
                result.extend(b);
                Some(ArrayValue::BoolArray(result))
            }
            _ => None,
        }
    }

    /// Get element at index (1-based for SQL compatibility)
    pub fn get(&self, index: usize) -> Option<JsonValue> {
        let idx = index.checked_sub(1)?; // Convert to 0-based
        match self {
            ArrayValue::Int64Array(v) => v.get(idx).map(|&i| JsonValue::Number(JsonNumber::from_i64(i))),
            ArrayValue::Float64Array(v) => v.get(idx).and_then(|&f| JsonNumber::from_f64(f).map(JsonValue::Number)),
            ArrayValue::StringArray(v) => v.get(idx).map(|s| JsonValue::String(s.clone())),
            ArrayValue::BoolArray(v) => v.get(idx).map(|&b| JsonValue::Bool(b)),
            ArrayValue::NestedArray(v) => v.get(idx).map(|a| array_to_json(a)),
            ArrayValue::Null => None,
        }
    }

    /// Slice array
    pub fn slice(&self, start: usize, end: usize) -> ArrayValue {
        let start_idx = start.saturating_sub(1);
        match self {
            ArrayValue::Int64Array(v) => {
                ArrayValue::Int64Array(v.get(start_idx..end.min(v.len())).unwrap_or(&[]).to_vec())
            }
            ArrayValue::Float64Array(v) => {
                ArrayValue::Float64Array(v.get(start_idx..end.min(v.len())).unwrap_or(&[]).to_vec())
            }
            ArrayValue::StringArray(v) => {
                ArrayValue::StringArray(v.get(start_idx..end.min(v.len())).unwrap_or(&[]).to_vec())
            }
            ArrayValue::BoolArray(v) => {
                ArrayValue::BoolArray(v.get(start_idx..end.min(v.len())).unwrap_or(&[]).to_vec())
            }
            ArrayValue::NestedArray(v) => {
                ArrayValue::NestedArray(v.get(start_idx..end.min(v.len())).unwrap_or(&[]).to_vec())
            }
            ArrayValue::Null => ArrayValue::Null,
        }
    }
}

fn array_to_json(arr: &ArrayValue) -> JsonValue {
    match arr {
        ArrayValue::Int64Array(v) => JsonValue::Array(
            v.iter().map(|&i| JsonValue::Number(JsonNumber::from_i64(i))).collect()
        ),
        ArrayValue::Float64Array(v) => JsonValue::Array(
            v.iter().filter_map(|&f| JsonNumber::from_f64(f).map(JsonValue::Number)).collect()
        ),
        ArrayValue::StringArray(v) => JsonValue::Array(
            v.iter().map(|s| JsonValue::String(s.clone())).collect()
        ),
        ArrayValue::BoolArray(v) => JsonValue::Array(
            v.iter().map(|&b| JsonValue::Bool(b)).collect()
        ),
        ArrayValue::NestedArray(v) => JsonValue::Array(
            v.iter().map(array_to_json).collect()
        ),
        ArrayValue::Null => JsonValue::Null,
    }
}

// ============================================================================
// JSON Type Support
// ============================================================================

/// JSON value type
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

/// JSON number (preserves integer vs float distinction)
#[derive(Debug, Clone, PartialEq)]
pub enum JsonNumber {
    Int(i64),
    Float(f64),
}

impl JsonNumber {
    pub fn from_i64(v: i64) -> Self {
        JsonNumber::Int(v)
    }

    pub fn from_f64(v: f64) -> Option<Self> {
        if v.is_finite() {
            Some(JsonNumber::Float(v))
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonNumber::Int(i) => Some(*i),
            JsonNumber::Float(f) => {
                if *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    Some(*f as i64)
                } else {
                    None
                }
            }
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JsonNumber::Int(i) => Some(*i as f64),
            JsonNumber::Float(f) => Some(*f),
        }
    }
}

impl JsonValue {
    /// Parse JSON from string
    pub fn parse(s: &str) -> Result<Self, JsonError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(JsonError::EmptyInput);
        }

        Self::parse_value(&mut s.chars().peekable())
    }

    fn parse_value(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        Self::skip_whitespace(chars);

        match chars.peek() {
            None => Err(JsonError::UnexpectedEnd),
            Some('n') => Self::parse_null(chars),
            Some('t') | Some('f') => Self::parse_bool(chars),
            Some('"') => Self::parse_string(chars),
            Some('[') => Self::parse_array(chars),
            Some('{') => Self::parse_object(chars),
            Some(c) if c.is_ascii_digit() || *c == '-' => Self::parse_number(chars),
            Some(c) => Err(JsonError::UnexpectedChar(*c)),
        }
    }

    fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars>) {
        while matches!(chars.peek(), Some(' ') | Some('\t') | Some('\n') | Some('\r')) {
            chars.next();
        }
    }

    fn parse_null(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        for expected in ['n', 'u', 'l', 'l'] {
            if chars.next() != Some(expected) {
                return Err(JsonError::InvalidValue);
            }
        }
        Ok(JsonValue::Null)
    }

    fn parse_bool(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        match chars.peek() {
            Some('t') => {
                for expected in ['t', 'r', 'u', 'e'] {
                    if chars.next() != Some(expected) {
                        return Err(JsonError::InvalidValue);
                    }
                }
                Ok(JsonValue::Bool(true))
            }
            Some('f') => {
                for expected in ['f', 'a', 'l', 's', 'e'] {
                    if chars.next() != Some(expected) {
                        return Err(JsonError::InvalidValue);
                    }
                }
                Ok(JsonValue::Bool(false))
            }
            _ => Err(JsonError::InvalidValue),
        }
    }

    fn parse_string(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        chars.next(); // consume opening quote
        let mut s = String::new();
        let mut escaped = false;

        loop {
            match chars.next() {
                None => return Err(JsonError::UnterminatedString),
                Some('\\') if !escaped => escaped = true,
                Some('"') if !escaped => return Ok(JsonValue::String(s)),
                Some(c) => {
                    if escaped {
                        match c {
                            'n' => s.push('\n'),
                            't' => s.push('\t'),
                            'r' => s.push('\r'),
                            '\\' => s.push('\\'),
                            '"' => s.push('"'),
                            '/' => s.push('/'),
                            _ => {
                                s.push('\\');
                                s.push(c);
                            }
                        }
                        escaped = false;
                    } else {
                        s.push(c);
                    }
                }
            }
        }
    }

    fn parse_number(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        let mut num_str = String::new();
        let mut is_float = false;

        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
                if c == '.' || c == 'e' || c == 'E' {
                    is_float = true;
                }
                num_str.push(c);
                chars.next();
            } else {
                break;
            }
        }

        if is_float {
            num_str.parse::<f64>()
                .map(|f| JsonValue::Number(JsonNumber::Float(f)))
                .map_err(|_| JsonError::InvalidNumber)
        } else {
            num_str.parse::<i64>()
                .map(|i| JsonValue::Number(JsonNumber::Int(i)))
                .map_err(|_| JsonError::InvalidNumber)
        }
    }

    fn parse_array(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        chars.next(); // consume '['
        let mut arr = Vec::new();

        Self::skip_whitespace(chars);
        if chars.peek() == Some(&']') {
            chars.next();
            return Ok(JsonValue::Array(arr));
        }

        loop {
            arr.push(Self::parse_value(chars)?);
            Self::skip_whitespace(chars);

            match chars.next() {
                Some(',') => Self::skip_whitespace(chars),
                Some(']') => return Ok(JsonValue::Array(arr)),
                _ => return Err(JsonError::ExpectedCommaOrBracket),
            }
        }
    }

    fn parse_object(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Self, JsonError> {
        chars.next(); // consume '{'
        let mut obj = HashMap::new();

        Self::skip_whitespace(chars);
        if chars.peek() == Some(&'}') {
            chars.next();
            return Ok(JsonValue::Object(obj));
        }

        loop {
            Self::skip_whitespace(chars);
            let key = match Self::parse_value(chars)? {
                JsonValue::String(s) => s,
                _ => return Err(JsonError::ExpectedString),
            };

            Self::skip_whitespace(chars);
            if chars.next() != Some(':') {
                return Err(JsonError::ExpectedColon);
            }

            let value = Self::parse_value(chars)?;
            obj.insert(key, value);

            Self::skip_whitespace(chars);
            match chars.next() {
                Some(',') => continue,
                Some('}') => return Ok(JsonValue::Object(obj)),
                _ => return Err(JsonError::ExpectedCommaOrBrace),
            }
        }
    }

    /// JSON path access (e.g., "$.foo.bar[0]")
    pub fn get_path(&self, path: &str) -> Option<&JsonValue> {
        let path = path.trim_start_matches('$').trim_start_matches('.');
        if path.is_empty() {
            return Some(self);
        }

        let mut current = self;
        for part in path.split('.') {
            // Handle array index
            if let Some(idx_start) = part.find('[') {
                let key = &part[..idx_start];
                if !key.is_empty() {
                    current = match current {
                        JsonValue::Object(obj) => obj.get(key)?,
                        _ => return None,
                    };
                }

                let idx_end = part.find(']')?;
                let idx: usize = part[idx_start + 1..idx_end].parse().ok()?;
                current = match current {
                    JsonValue::Array(arr) => arr.get(idx)?,
                    _ => return None,
                };
            } else {
                current = match current {
                    JsonValue::Object(obj) => obj.get(part)?,
                    _ => return None,
                };
            }
        }
        Some(current)
    }

    /// Convert to string representation
    pub fn to_json_string(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            JsonValue::Number(JsonNumber::Int(i)) => i.to_string(),
            JsonValue::Number(JsonNumber::Float(f)) => f.to_string(),
            JsonValue::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_json_string()).collect();
                format!("[{}]", items.join(","))
            }
            JsonValue::Object(obj) => {
                let items: Vec<String> = obj.iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v.to_json_string()))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
        }
    }

    /// Check if value is truthy
    pub fn is_truthy(&self) -> bool {
        match self {
            JsonValue::Null => false,
            JsonValue::Bool(b) => *b,
            JsonValue::Number(JsonNumber::Int(i)) => *i != 0,
            JsonValue::Number(JsonNumber::Float(f)) => *f != 0.0,
            JsonValue::String(s) => !s.is_empty(),
            JsonValue::Array(arr) => !arr.is_empty(),
            JsonValue::Object(obj) => !obj.is_empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JsonError {
    EmptyInput,
    UnexpectedEnd,
    UnexpectedChar(char),
    InvalidValue,
    InvalidNumber,
    UnterminatedString,
    ExpectedCommaOrBracket,
    ExpectedCommaOrBrace,
    ExpectedString,
    ExpectedColon,
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonError::EmptyInput => write!(f, "Empty JSON input"),
            JsonError::UnexpectedEnd => write!(f, "Unexpected end of JSON"),
            JsonError::UnexpectedChar(c) => write!(f, "Unexpected character: {}", c),
            JsonError::InvalidValue => write!(f, "Invalid JSON value"),
            JsonError::InvalidNumber => write!(f, "Invalid JSON number"),
            JsonError::UnterminatedString => write!(f, "Unterminated string"),
            JsonError::ExpectedCommaOrBracket => write!(f, "Expected ',' or ']'"),
            JsonError::ExpectedCommaOrBrace => write!(f, "Expected ',' or '}}'"),
            JsonError::ExpectedString => write!(f, "Expected string key"),
            JsonError::ExpectedColon => write!(f, "Expected ':'"),
        }
    }
}

impl std::error::Error for JsonError {}

// ============================================================================
// ASOF JOIN
// ============================================================================

/// ASOF JOIN for time-series data
/// Joins each row from left with the closest matching row from right
/// where right.timestamp <= left.timestamp (or nearest match)
#[derive(Debug, Clone)]
pub struct AsofJoinConfig {
    /// Left join column (typically timestamp)
    pub left_on: String,
    /// Right join column (typically timestamp)
    pub right_on: String,
    /// Additional equality columns (e.g., symbol, id)
    pub by_columns: Vec<String>,
    /// Direction: backward (<=), forward (>=), or nearest
    pub direction: AsofDirection,
    /// Maximum allowed difference (None = no limit)
    pub tolerance: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofDirection {
    /// Match right.ts <= left.ts (most recent before)
    Backward,
    /// Match right.ts >= left.ts (first after)
    Forward,
    /// Match closest (minimum |difference|)
    Nearest,
}

/// Result of ASOF JOIN matching
#[derive(Debug, Clone)]
pub struct AsofMatch {
    /// Index in left table
    pub left_idx: usize,
    /// Index in right table (None if no match)
    pub right_idx: Option<usize>,
    /// Difference between join keys (if matched)
    pub difference: Option<i64>,
}

/// Perform ASOF JOIN between two sorted arrays
/// Both arrays must be sorted by the join key
pub fn asof_join(
    left_keys: &[i64],
    right_keys: &[i64],
    left_by: Option<&[impl AsRef<str> + Eq + std::hash::Hash]>,
    right_by: Option<&[impl AsRef<str> + Eq + std::hash::Hash]>,
    config: &AsofJoinConfig,
) -> Vec<AsofMatch> {
    let mut results = Vec::with_capacity(left_keys.len());

    // Group right side by 'by' columns if present
    let right_groups: HashMap<Option<&str>, Vec<(usize, i64)>> = if let Some(by) = right_by {
        let mut groups: HashMap<Option<&str>, Vec<(usize, i64)>> = HashMap::new();
        for (i, key) in right_keys.iter().enumerate() {
            let group_key = by.get(i).map(|s| s.as_ref());
            groups.entry(group_key).or_default().push((i, *key));
        }
        groups
    } else {
        let mut groups = HashMap::new();
        groups.insert(None, right_keys.iter().enumerate().map(|(i, &k)| (i, k)).collect());
        groups
    };

    for (left_idx, &left_key) in left_keys.iter().enumerate() {
        let group_key = left_by.and_then(|by| by.get(left_idx).map(|s| s.as_ref()));

        let right_idx = if let Some(right_group) = right_groups.get(&group_key) {
            find_asof_match(left_key, right_group, config.direction, config.tolerance)
        } else {
            None
        };

        let difference = right_idx.map(|idx| left_key - right_keys[idx]);

        results.push(AsofMatch {
            left_idx,
            right_idx,
            difference,
        });
    }

    results
}

fn find_asof_match(
    target: i64,
    candidates: &[(usize, i64)],
    direction: AsofDirection,
    tolerance: Option<i64>,
) -> Option<usize> {
    if candidates.is_empty() {
        return None;
    }

    match direction {
        AsofDirection::Backward => {
            // Find largest key <= target
            let mut best: Option<(usize, i64)> = None;
            for &(idx, key) in candidates {
                if key <= target {
                    if best.is_none() || key > best.unwrap().1 {
                        best = Some((idx, key));
                    }
                }
            }
            best.filter(|(_, key)| {
                tolerance.map_or(true, |t| target - key <= t)
            }).map(|(idx, _)| idx)
        }
        AsofDirection::Forward => {
            // Find smallest key >= target
            let mut best: Option<(usize, i64)> = None;
            for &(idx, key) in candidates {
                if key >= target {
                    if best.is_none() || key < best.unwrap().1 {
                        best = Some((idx, key));
                    }
                }
            }
            best.filter(|(_, key)| {
                tolerance.map_or(true, |t| key - target <= t)
            }).map(|(idx, _)| idx)
        }
        AsofDirection::Nearest => {
            // Find closest key
            let mut best: Option<(usize, i64)> = None;
            let mut best_diff = i64::MAX;
            for &(idx, key) in candidates {
                let diff = (key - target).abs();
                if diff < best_diff {
                    best_diff = diff;
                    best = Some((idx, key));
                }
            }
            best.filter(|(_, _)| {
                tolerance.map_or(true, |t| best_diff <= t)
            }).map(|(idx, _)| idx)
        }
    }
}

// ============================================================================
// Probabilistic Sketches
// ============================================================================

/// Count-Min Sketch for frequency estimation
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// 2D array of counters
    counters: Vec<Vec<u64>>,
    /// Number of hash functions (rows)
    depth: usize,
    /// Width of each row
    width: usize,
    /// Total count
    total: u64,
}

impl CountMinSketch {
    /// Create new sketch with given dimensions
    /// Error rate ≈ e/width, probability ≈ 1 - 1/2^depth
    pub fn new(width: usize, depth: usize) -> Self {
        Self {
            counters: vec![vec![0; width]; depth],
            depth,
            width,
            total: 0,
        }
    }

    /// Create with target error rate and confidence
    pub fn with_error_rate(error_rate: f64, confidence: f64) -> Self {
        let width = (std::f64::consts::E / error_rate).ceil() as usize;
        let depth = (1.0 / (1.0 - confidence)).ln().ceil() as usize;
        Self::new(width.max(1), depth.max(1))
    }

    /// Add item to sketch
    pub fn add(&mut self, item: &[u8]) {
        self.add_count(item, 1);
    }

    /// Add item with count
    pub fn add_count(&mut self, item: &[u8], count: u64) {
        self.total += count;
        for i in 0..self.depth {
            let hash = self.hash(item, i);
            self.counters[i][hash] = self.counters[i][hash].saturating_add(count);
        }
    }

    /// Estimate count of item
    pub fn estimate(&self, item: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for i in 0..self.depth {
            let hash = self.hash(item, i);
            min = min.min(self.counters[i][hash]);
        }
        min
    }

    /// Merge another sketch into this one
    pub fn merge(&mut self, other: &CountMinSketch) {
        assert_eq!(self.width, other.width);
        assert_eq!(self.depth, other.depth);

        self.total += other.total;
        for i in 0..self.depth {
            for j in 0..self.width {
                self.counters[i][j] = self.counters[i][j].saturating_add(other.counters[i][j]);
            }
        }
    }

    fn hash(&self, item: &[u8], seed: usize) -> usize {
        // Simple hash function with different seeds
        let mut h = seed as u64;
        for &byte in item {
            h = h.wrapping_mul(31).wrapping_add(byte as u64);
        }
        h = h ^ (h >> 33);
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h = h ^ (h >> 33);
        (h as usize) % self.width
    }

    pub fn total_count(&self) -> u64 {
        self.total
    }
}

/// Bloom Filter for set membership
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<bool>,
    num_hashes: usize,
    size: usize,
}

impl BloomFilter {
    /// Create bloom filter with given size and number of hash functions
    pub fn new(size: usize, num_hashes: usize) -> Self {
        Self {
            bits: vec![false; size],
            num_hashes,
            size,
        }
    }

    /// Create with target capacity and false positive rate
    pub fn with_capacity(capacity: usize, fp_rate: f64) -> Self {
        let size = (-(capacity as f64 * fp_rate.ln()) / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes = ((size as f64 / capacity as f64) * 2.0_f64.ln()).ceil() as usize;
        Self::new(size.max(1), num_hashes.max(1))
    }

    /// Add item to filter
    pub fn add(&mut self, item: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i);
            self.bits[hash] = true;
        }
    }

    /// Check if item might be in set
    pub fn might_contain(&self, item: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i);
            if !self.bits[hash] {
                return false;
            }
        }
        true
    }

    /// Merge another bloom filter
    pub fn merge(&mut self, other: &BloomFilter) {
        assert_eq!(self.size, other.size);
        for i in 0..self.size {
            self.bits[i] = self.bits[i] || other.bits[i];
        }
    }

    fn hash(&self, item: &[u8], seed: usize) -> usize {
        let mut h = seed as u64 ^ 0x9e3779b97f4a7c15;
        for &byte in item {
            h = h.wrapping_mul(0x517cc1b727220a95).wrapping_add(byte as u64);
        }
        h = h ^ (h >> 32);
        (h as usize) % self.size
    }

    pub fn estimated_fill_ratio(&self) -> f64 {
        self.bits.iter().filter(|&&b| b).count() as f64 / self.size as f64
    }
}

/// HyperLogLog for cardinality estimation
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
    precision: u8,
    num_registers: usize,
}

impl HyperLogLog {
    /// Create HLL with given precision (4-18)
    /// Memory = 2^precision bytes
    /// Standard error ≈ 1.04 / sqrt(2^precision)
    pub fn new(precision: u8) -> Self {
        let precision = precision.clamp(4, 18);
        let num_registers = 1 << precision;
        Self {
            registers: vec![0; num_registers],
            precision,
            num_registers,
        }
    }

    /// Add item
    pub fn add(&mut self, item: &[u8]) {
        let hash = self.hash(item);
        let index = (hash >> (64 - self.precision)) as usize;
        let remaining = hash << self.precision | (1 << (self.precision - 1));
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        self.registers[index] = self.registers[index].max(leading_zeros);
    }

    /// Estimate cardinality
    pub fn estimate(&self) -> u64 {
        let m = self.num_registers as f64;

        // Calculate harmonic mean
        let sum: f64 = self.registers.iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();

        let alpha = match self.precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let raw_estimate = alpha * m * m / sum;

        // Apply corrections
        if raw_estimate <= 2.5 * m {
            // Small range correction
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                (m * (m / zeros as f64).ln()) as u64
            } else {
                raw_estimate as u64
            }
        } else if raw_estimate > (1u64 << 32) as f64 / 30.0 {
            // Large range correction
            let two_32 = (1u64 << 32) as f64;
            (-two_32 * (1.0 - raw_estimate / two_32).ln()) as u64
        } else {
            raw_estimate as u64
        }
    }

    /// Merge another HLL
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(self.precision, other.precision);
        for i in 0..self.num_registers {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    fn hash(&self, item: &[u8]) -> u64 {
        // MurmurHash3 64-bit finalizer for better distribution
        let mut h = 0xcbf29ce484222325u64;
        for &byte in item {
            h = h.wrapping_add(byte as u64);
            h = h.wrapping_mul(0x517cc1b727220a95);
            h = h ^ (h >> 47);
        }
        // Final mixing
        h = h ^ (h >> 33);
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h = h ^ (h >> 33);
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h = h ^ (h >> 33);
        h
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tdigest_basic() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }

        let p50 = td.percentile(50.0).unwrap();
        assert!((p50 - 50.0).abs() < 5.0, "p50 should be around 50, got {}", p50);

        let p99 = td.percentile(99.0).unwrap();
        assert!((p99 - 99.0).abs() < 5.0, "p99 should be around 99, got {}", p99);
    }

    #[test]
    fn test_tdigest_merge() {
        let mut td1 = TDigest::new(100.0);
        let mut td2 = TDigest::new(100.0);

        for i in 1..=50 {
            td1.add(i as f64);
        }
        for i in 51..=100 {
            td2.add(i as f64);
        }

        td1.merge(&td2);

        let p50 = td1.percentile(50.0).unwrap();
        assert!((p50 - 50.0).abs() < 5.0);
    }

    #[test]
    fn test_exact_quantile() {
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(exact_quantile(&mut values, 0.0), Some(1.0));
        assert_eq!(exact_quantile(&mut values, 0.5), Some(3.0));
        assert_eq!(exact_quantile(&mut values, 1.0), Some(5.0));
    }

    #[test]
    fn test_rollup_sets() {
        let sets = rollup_sets(3);
        assert_eq!(sets.len(), 4);
        assert_eq!(sets[0].columns, vec![0, 1, 2]);
        assert_eq!(sets[1].columns, vec![0, 1]);
        assert_eq!(sets[2].columns, vec![0]);
        assert_eq!(sets[3].columns, vec![] as Vec<usize>);
    }

    #[test]
    fn test_cube_sets() {
        let sets = cube_sets(2);
        assert_eq!(sets.len(), 4);
        // Should include: (0,1), (0), (1), ()
        let columns: Vec<_> = sets.iter().map(|s| s.columns.clone()).collect();
        assert!(columns.contains(&vec![0, 1]));
        assert!(columns.contains(&vec![0]));
        assert!(columns.contains(&vec![1]));
        assert!(columns.contains(&vec![]));
    }

    #[test]
    fn test_grouping_value() {
        let set = GroupingSet::new(vec![0]); // Only first column grouped
        let value = grouping_value(&set, 3);
        // Columns 1 and 2 are aggregated: binary 011 = 3
        assert_eq!(value, 3);
    }

    #[test]
    fn test_array_operations() {
        let arr = ArrayValue::Int64Array(vec![1, 2, 3, 4, 5]);

        assert_eq!(arr.len(), 5);
        assert_eq!(arr.get(1), Some(JsonValue::Number(JsonNumber::Int(1))));
        assert_eq!(arr.get(3), Some(JsonValue::Number(JsonNumber::Int(3))));

        // SQL slice is 1-based: slice(2,4) means elements at positions 2,3,4 (1-based)
        // So in 0-based: indices 1,2,3 = values 2,3,4 = 3 elements
        let sliced = arr.slice(2, 4);
        assert_eq!(sliced.len(), 3);
    }

    #[test]
    fn test_array_concat() {
        let arr1 = ArrayValue::Int64Array(vec![1, 2, 3]);
        let arr2 = ArrayValue::Int64Array(vec![4, 5]);

        let result = arr1.concat(&arr2).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_json_parse() {
        let json = JsonValue::parse(r#"{"name": "test", "value": 42}"#).unwrap();
        match json {
            JsonValue::Object(obj) => {
                assert_eq!(obj.get("name"), Some(&JsonValue::String("test".to_string())));
                assert_eq!(obj.get("value"), Some(&JsonValue::Number(JsonNumber::Int(42))));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_json_path() {
        let json = JsonValue::parse(r#"{"data": {"items": [1, 2, 3]}}"#).unwrap();

        let items = json.get_path("data.items").unwrap();
        match items {
            JsonValue::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }

        let first = json.get_path("data.items[0]").unwrap();
        assert_eq!(first, &JsonValue::Number(JsonNumber::Int(1)));
    }

    #[test]
    fn test_json_to_string() {
        let json = JsonValue::Object(HashMap::from([
            ("name".to_string(), JsonValue::String("test".to_string())),
            ("value".to_string(), JsonValue::Number(JsonNumber::Int(42))),
        ]));

        let s = json.to_json_string();
        assert!(s.contains("\"name\":\"test\""));
        assert!(s.contains("\"value\":42"));
    }

    #[test]
    fn test_asof_join_backward() {
        let left_keys = vec![10, 20, 30, 40];
        let right_keys = vec![5, 15, 25, 35];

        let config = AsofJoinConfig {
            left_on: "ts".to_string(),
            right_on: "ts".to_string(),
            by_columns: vec![],
            direction: AsofDirection::Backward,
            tolerance: None,
        };

        let empty: Option<&[String]> = None;
        let results = asof_join(&left_keys, &right_keys, empty, empty, &config);

        // 10 matches 5, 20 matches 15, 30 matches 25, 40 matches 35
        assert_eq!(results[0].right_idx, Some(0)); // 10 -> 5
        assert_eq!(results[1].right_idx, Some(1)); // 20 -> 15
        assert_eq!(results[2].right_idx, Some(2)); // 30 -> 25
        assert_eq!(results[3].right_idx, Some(3)); // 40 -> 35
    }

    #[test]
    fn test_asof_join_forward() {
        let left_keys = vec![10, 20, 30, 40];
        let right_keys = vec![15, 25, 35, 45];

        let config = AsofJoinConfig {
            left_on: "ts".to_string(),
            right_on: "ts".to_string(),
            by_columns: vec![],
            direction: AsofDirection::Forward,
            tolerance: None,
        };

        let empty: Option<&[String]> = None;
        let results = asof_join(&left_keys, &right_keys, empty, empty, &config);

        // 10 matches 15, 20 matches 25, 30 matches 35, 40 matches 45
        assert_eq!(results[0].right_idx, Some(0)); // 10 -> 15
        assert_eq!(results[1].right_idx, Some(1)); // 20 -> 25
        assert_eq!(results[2].right_idx, Some(2)); // 30 -> 35
        assert_eq!(results[3].right_idx, Some(3)); // 40 -> 45
    }

    #[test]
    fn test_asof_join_with_tolerance() {
        let left_keys = vec![10, 20, 100];
        let right_keys = vec![5, 15];

        let config = AsofJoinConfig {
            left_on: "ts".to_string(),
            right_on: "ts".to_string(),
            by_columns: vec![],
            direction: AsofDirection::Backward,
            tolerance: Some(10),
        };

        let empty: Option<&[String]> = None;
        let results = asof_join(&left_keys, &right_keys, empty, empty, &config);

        assert_eq!(results[0].right_idx, Some(0)); // 10 -> 5 (diff=5, within tolerance)
        assert_eq!(results[1].right_idx, Some(1)); // 20 -> 15 (diff=5, within tolerance)
        assert_eq!(results[2].right_idx, None);    // 100 -> None (diff=85, exceeds tolerance)
    }

    #[test]
    fn test_count_min_sketch() {
        let mut sketch = CountMinSketch::new(1000, 5);

        sketch.add_count(b"apple", 10);
        sketch.add_count(b"banana", 5);
        sketch.add_count(b"apple", 5);

        let apple_est = sketch.estimate(b"apple");
        let banana_est = sketch.estimate(b"banana");

        // Estimates should be >= actual (may overcount)
        assert!(apple_est >= 15);
        assert!(banana_est >= 5);
    }

    #[test]
    fn test_bloom_filter() {
        let mut filter = BloomFilter::with_capacity(1000, 0.01);

        filter.add(b"hello");
        filter.add(b"world");

        assert!(filter.might_contain(b"hello"));
        assert!(filter.might_contain(b"world"));
        // "goodbye" might false positive, but probably not
    }

    #[test]
    fn test_hyperloglog() {
        let mut hll = HyperLogLog::new(14);

        for i in 0..10000 {
            hll.add(format!("item{}", i).as_bytes());
        }

        let estimate = hll.estimate();
        // HLL with precision 14 has ~1.04/sqrt(2^14) = ~0.8% error
        // Allow wider margin for test stability
        assert!(estimate > 8000 && estimate < 12000,
            "Expected ~10000, got {}", estimate);
    }

    #[test]
    fn test_hyperloglog_merge() {
        let mut hll1 = HyperLogLog::new(14);
        let mut hll2 = HyperLogLog::new(14);

        for i in 0..5000 {
            hll1.add(format!("item{}", i).as_bytes());
        }
        for i in 5000..10000 {
            hll2.add(format!("item{}", i).as_bytes());
        }

        hll1.merge(&hll2);
        let estimate = hll1.estimate();

        // Allow wider margin for merged estimates
        assert!(estimate > 8000 && estimate < 12000,
            "Expected ~10000, got {}", estimate);
    }

    #[test]
    fn test_multiple_quantiles() {
        let mut values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let quantiles = exact_quantiles(&mut values, &[0.25, 0.5, 0.75, 0.90, 0.99]);

        assert_eq!(quantiles[0], Some(25.75)); // p25
        assert_eq!(quantiles[1], Some(50.5));  // p50
        assert_eq!(quantiles[2], Some(75.25)); // p75
    }
}
