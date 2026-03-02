// Phase 11: Advanced Functions
//
// Array, Map, JSON, Geo, URL, IP functions, Full-Text Search, and Sampling
// providing an extensive function library for analytics.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Common Types
// ============================================================================

/// Generic value type for function arguments and results
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Tuple(Vec<Value>),
    Date(i32),      // Days since epoch
    DateTime(i64),  // Microseconds since epoch
    Ipv4(u32),
    Ipv6(u128),
    Uuid(u128),
    Point(f64, f64), // Geo point (lon, lat)
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int64",
            Value::UInt(_) => "UInt64",
            Value::Float(_) => "Float64",
            Value::String(_) => "String",
            Value::Bytes(_) => "Bytes",
            Value::Array(_) => "Array",
            Value::Map(_) => "Map",
            Value::Tuple(_) => "Tuple",
            Value::Date(_) => "Date",
            Value::DateTime(_) => "DateTime",
            Value::Ipv4(_) => "IPv4",
            Value::Ipv6(_) => "IPv6",
            Value::Uuid(_) => "UUID",
            Value::Point(_, _) => "Point",
        }
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::UInt(u) => u.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => format!("{:?}", b),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_string_repr()).collect();
                format!("[{}]", items.join(", "))
            }
            Value::Map(m) => {
                let items: Vec<String> = m.iter()
                    .map(|(k, v)| format!("'{}': {}", k, v.to_string_repr()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            Value::Tuple(t) => {
                let items: Vec<String> = t.iter().map(|v| v.to_string_repr()).collect();
                format!("({})", items.join(", "))
            }
            Value::Date(d) => format!("Date({})", d),
            Value::DateTime(dt) => format!("DateTime({})", dt),
            Value::Ipv4(ip) => Ipv4Addr::from(*ip).to_string(),
            Value::Ipv6(ip) => Ipv6Addr::from(*ip).to_string(),
            Value::Uuid(u) => format!("{:032x}", u),
            Value::Point(lon, lat) => format!("POINT({} {})", lon, lat),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::UInt(u) => Some(*u as i64),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::UInt(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(arr) => Some(arr),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }
}

/// Function error type
#[derive(Debug, Clone)]
pub enum FunctionError {
    InvalidArgumentCount { expected: usize, got: usize },
    InvalidArgumentType { expected: &'static str, got: String },
    InvalidArgument(String),
    DivisionByZero,
    OutOfRange(String),
    ParseError(String),
    NotImplemented(String),
}

impl std::fmt::Display for FunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgumentCount { expected, got } => {
                write!(f, "Expected {} arguments, got {}", expected, got)
            }
            Self::InvalidArgumentType { expected, got } => {
                write!(f, "Expected {}, got {}", expected, got)
            }
            Self::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            Self::DivisionByZero => write!(f, "Division by zero"),
            Self::OutOfRange(msg) => write!(f, "Out of range: {}", msg),
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
        }
    }
}

impl std::error::Error for FunctionError {}

pub type FunctionResult = Result<Value, FunctionError>;

// ============================================================================
// Array Functions
// ============================================================================

pub struct ArrayFunctions;

impl ArrayFunctions {
    /// Create an array from arguments
    pub fn array(args: &[Value]) -> FunctionResult {
        Ok(Value::Array(args.to_vec()))
    }

    /// Get array length
    pub fn length(arr: &Value) -> FunctionResult {
        match arr {
            Value::Array(a) => Ok(Value::UInt(a.len() as u64)),
            Value::String(s) => Ok(Value::UInt(s.len() as u64)),
            _ => Err(FunctionError::InvalidArgumentType {
                expected: "Array or String",
                got: arr.type_name().to_string(),
            }),
        }
    }

    /// Check if array is empty
    pub fn empty(arr: &Value) -> FunctionResult {
        match arr {
            Value::Array(a) => Ok(Value::Bool(a.is_empty())),
            Value::String(s) => Ok(Value::Bool(s.is_empty())),
            _ => Err(FunctionError::InvalidArgumentType {
                expected: "Array or String",
                got: arr.type_name().to_string(),
            }),
        }
    }

    /// Check if array is not empty
    pub fn not_empty(arr: &Value) -> FunctionResult {
        match Self::empty(arr)? {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => unreachable!(),
        }
    }

    /// Get element at index (1-based)
    pub fn element(arr: &Value, index: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        let idx = index.as_i64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Integer",
            got: index.type_name().to_string(),
        })?;

        // 1-based indexing, negative from end
        let real_idx = if idx > 0 {
            (idx - 1) as usize
        } else if idx < 0 {
            let len = arr.len() as i64;
            if -idx > len {
                return Ok(Value::Null);
            }
            (len + idx) as usize
        } else {
            return Ok(Value::Null);
        };

        Ok(arr.get(real_idx).cloned().unwrap_or(Value::Null))
    }

    /// Check if array contains element
    pub fn has(arr: &Value, elem: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        Ok(Value::Bool(arr.contains(elem)))
    }

    /// Check if array contains all elements
    pub fn has_all(arr: &Value, elems: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        let elems = elems.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: elems.type_name().to_string(),
        })?;

        Ok(Value::Bool(elems.iter().all(|e| arr.contains(e))))
    }

    /// Check if array contains any element
    pub fn has_any(arr: &Value, elems: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        let elems = elems.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: elems.type_name().to_string(),
        })?;

        Ok(Value::Bool(elems.iter().any(|e| arr.contains(e))))
    }

    /// Find index of element (1-based, 0 if not found)
    pub fn index_of(arr: &Value, elem: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let idx = arr.iter().position(|e| e == elem);
        Ok(Value::UInt(idx.map(|i| i + 1).unwrap_or(0) as u64))
    }

    /// Count occurrences of element
    pub fn count_equal(arr: &Value, elem: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let count = arr.iter().filter(|e| *e == elem).count();
        Ok(Value::UInt(count as u64))
    }

    /// Concatenate arrays
    pub fn concat(arrays: &[Value]) -> FunctionResult {
        let mut result = Vec::new();
        for arr in arrays {
            let a = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
                expected: "Array",
                got: arr.type_name().to_string(),
            })?;
            result.extend(a.iter().cloned());
        }
        Ok(Value::Array(result))
    }

    /// Get unique elements
    pub fn distinct(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut seen = Vec::new();
        for elem in arr {
            if !seen.contains(elem) {
                seen.push(elem.clone());
            }
        }
        Ok(Value::Array(seen))
    }

    /// Flatten nested arrays
    pub fn flatten(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut result = Vec::new();
        for elem in arr {
            match elem {
                Value::Array(inner) => result.extend(inner.iter().cloned()),
                _ => result.push(elem.clone()),
            }
        }
        Ok(Value::Array(result))
    }

    /// Reverse array
    pub fn reverse(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut result = arr.clone();
        result.reverse();
        Ok(Value::Array(result))
    }

    /// Sort array
    pub fn sort(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut result = arr.clone();
        result.sort_by(|a, b| {
            match (a, b) {
                (Value::Int(x), Value::Int(y)) => x.cmp(y),
                (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
                (Value::String(x), Value::String(y)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(Value::Array(result))
    }

    /// Get slice of array
    pub fn slice(arr: &Value, offset: &Value, length: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        let offset = offset.as_i64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Integer",
            got: offset.type_name().to_string(),
        })?;
        let length = length.as_i64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Integer",
            got: length.type_name().to_string(),
        })?;

        let start = if offset > 0 {
            (offset - 1) as usize
        } else if offset < 0 {
            let len = arr.len() as i64;
            ((len + offset).max(0)) as usize
        } else {
            0
        };

        let end = (start + length as usize).min(arr.len());
        Ok(Value::Array(arr[start..end].to_vec()))
    }

    /// Sum of array elements
    pub fn array_sum(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut sum = 0.0;
        for elem in arr {
            if let Some(v) = elem.as_f64() {
                sum += v;
            }
        }
        Ok(Value::Float(sum))
    }

    /// Average of array elements
    pub fn array_avg(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        if arr.is_empty() {
            return Ok(Value::Null);
        }

        let mut sum = 0.0;
        let mut count = 0;
        for elem in arr {
            if let Some(v) = elem.as_f64() {
                sum += v;
                count += 1;
            }
        }

        if count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Float(sum / count as f64))
        }
    }

    /// Min of array elements
    pub fn array_min(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut min: Option<f64> = None;
        for elem in arr {
            if let Some(v) = elem.as_f64() {
                min = Some(min.map(|m| m.min(v)).unwrap_or(v));
            }
        }
        Ok(min.map(Value::Float).unwrap_or(Value::Null))
    }

    /// Max of array elements
    pub fn array_max(arr: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;

        let mut max: Option<f64> = None;
        for elem in arr {
            if let Some(v) = elem.as_f64() {
                max = Some(max.map(|m| m.max(v)).unwrap_or(v));
            }
        }
        Ok(max.map(Value::Float).unwrap_or(Value::Null))
    }

    /// Join array elements into string
    pub fn array_join(arr: &Value, sep: &Value) -> FunctionResult {
        let arr = arr.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: arr.type_name().to_string(),
        })?;
        let sep = sep.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: sep.type_name().to_string(),
        })?;

        let strings: Vec<String> = arr.iter().map(|v| v.to_string_repr()).collect();
        Ok(Value::String(strings.join(sep)))
    }

    /// Intersection of two arrays
    pub fn array_intersect(a: &Value, b: &Value) -> FunctionResult {
        let a = a.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: a.type_name().to_string(),
        })?;
        let b = b.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: b.type_name().to_string(),
        })?;

        let result: Vec<Value> = a.iter().filter(|e| b.contains(e)).cloned().collect();
        Ok(Value::Array(result))
    }

    /// Difference of two arrays (a - b)
    pub fn array_difference(a: &Value, b: &Value) -> FunctionResult {
        let a = a.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: a.type_name().to_string(),
        })?;
        let b = b.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: b.type_name().to_string(),
        })?;

        let result: Vec<Value> = a.iter().filter(|e| !b.contains(e)).cloned().collect();
        Ok(Value::Array(result))
    }
}

// ============================================================================
// Map Functions
// ============================================================================

pub struct MapFunctions;

impl MapFunctions {
    /// Create a map from key-value pairs
    pub fn map(keys: &Value, values: &Value) -> FunctionResult {
        let keys = keys.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: keys.type_name().to_string(),
        })?;
        let values = values.as_array().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Array",
            got: values.type_name().to_string(),
        })?;

        if keys.len() != values.len() {
            return Err(FunctionError::InvalidArgument(
                "Keys and values arrays must have same length".to_string()
            ));
        }

        let mut map = BTreeMap::new();
        for (k, v) in keys.iter().zip(values.iter()) {
            let key = k.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| k.to_string_repr());
            map.insert(key, v.clone());
        }

        Ok(Value::Map(map))
    }

    /// Get value by key
    pub fn map_get(map: &Value, key: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;
        let key = key.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: key.type_name().to_string(),
        })?;

        Ok(map.get(key).cloned().unwrap_or(Value::Null))
    }

    /// Get value by key with default
    pub fn map_get_or_default(map: &Value, key: &Value, default: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;
        let key = key.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: key.type_name().to_string(),
        })?;

        Ok(map.get(key).cloned().unwrap_or_else(|| default.clone()))
    }

    /// Check if map contains key
    pub fn map_contains(map: &Value, key: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;
        let key = key.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: key.type_name().to_string(),
        })?;

        Ok(Value::Bool(map.contains_key(key)))
    }

    /// Get all keys
    pub fn map_keys(map: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;

        Ok(Value::Array(map.keys().map(|k| Value::String(k.clone())).collect()))
    }

    /// Get all values
    pub fn map_values(map: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;

        Ok(Value::Array(map.values().cloned().collect()))
    }

    /// Get map size
    pub fn map_size(map: &Value) -> FunctionResult {
        let map = map.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: map.type_name().to_string(),
        })?;

        Ok(Value::UInt(map.len() as u64))
    }

    /// Merge two maps (second overrides first)
    pub fn map_update(base: &Value, updates: &Value) -> FunctionResult {
        let base = base.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: base.type_name().to_string(),
        })?;
        let updates = updates.as_map().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Map",
            got: updates.type_name().to_string(),
        })?;

        let mut result = base.clone();
        result.extend(updates.iter().map(|(k, v)| (k.clone(), v.clone())));
        Ok(Value::Map(result))
    }
}

// ============================================================================
// JSON Functions
// ============================================================================

pub struct JsonFunctions;

impl JsonFunctions {
    /// Parse JSON string into Value
    pub fn parse_json(json: &Value) -> FunctionResult {
        let json_str = json.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: json.type_name().to_string(),
        })?;

        Self::parse_json_str(json_str)
    }

    fn parse_json_str(s: &str) -> FunctionResult {
        let s = s.trim();

        if s == "null" {
            return Ok(Value::Null);
        }
        if s == "true" {
            return Ok(Value::Bool(true));
        }
        if s == "false" {
            return Ok(Value::Bool(false));
        }

        // Try number
        if let Ok(i) = s.parse::<i64>() {
            return Ok(Value::Int(i));
        }
        if let Ok(f) = s.parse::<f64>() {
            return Ok(Value::Float(f));
        }

        // String
        if s.starts_with('"') && s.ends_with('"') {
            let inner = &s[1..s.len()-1];
            return Ok(Value::String(Self::unescape_json_string(inner)));
        }

        // Array
        if s.starts_with('[') && s.ends_with(']') {
            let inner = &s[1..s.len()-1];
            if inner.trim().is_empty() {
                return Ok(Value::Array(vec![]));
            }
            let elements = Self::split_json_elements(inner)?;
            let mut arr = Vec::new();
            for elem in elements {
                arr.push(Self::parse_json_str(&elem)?);
            }
            return Ok(Value::Array(arr));
        }

        // Object
        if s.starts_with('{') && s.ends_with('}') {
            let inner = &s[1..s.len()-1];
            if inner.trim().is_empty() {
                return Ok(Value::Map(BTreeMap::new()));
            }
            let pairs = Self::split_json_elements(inner)?;
            let mut map = BTreeMap::new();
            for pair in pairs {
                let colon_pos = pair.find(':').ok_or_else(|| {
                    FunctionError::ParseError("Invalid JSON object".to_string())
                })?;
                let key = pair[..colon_pos].trim();
                let value = pair[colon_pos+1..].trim();

                let key = if key.starts_with('"') && key.ends_with('"') {
                    Self::unescape_json_string(&key[1..key.len()-1])
                } else {
                    key.to_string()
                };

                map.insert(key, Self::parse_json_str(value)?);
            }
            return Ok(Value::Map(map));
        }

        Err(FunctionError::ParseError(format!("Invalid JSON: {}", s)))
    }

    fn split_json_elements(s: &str) -> Result<Vec<String>, FunctionError> {
        let mut elements = Vec::new();
        let mut current = String::new();
        let mut depth = 0;
        let mut in_string = false;
        let mut escape = false;

        for c in s.chars() {
            if escape {
                current.push(c);
                escape = false;
                continue;
            }

            if c == '\\' && in_string {
                current.push(c);
                escape = true;
                continue;
            }

            if c == '"' {
                in_string = !in_string;
            }

            if !in_string {
                match c {
                    '[' | '{' => depth += 1,
                    ']' | '}' => depth -= 1,
                    ',' if depth == 0 => {
                        elements.push(current.trim().to_string());
                        current = String::new();
                        continue;
                    }
                    _ => {}
                }
            }

            current.push(c);
        }

        if !current.trim().is_empty() {
            elements.push(current.trim().to_string());
        }

        Ok(elements)
    }

    fn unescape_json_string(s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('r') => result.push('\r'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some('/') => result.push('/'),
                    Some(other) => {
                        result.push('\\');
                        result.push(other);
                    }
                    None => result.push('\\'),
                }
            } else {
                result.push(c);
            }
        }

        result
    }

    /// Convert Value to JSON string
    pub fn to_json(value: &Value) -> FunctionResult {
        Ok(Value::String(Self::value_to_json(value)))
    }

    fn value_to_json(value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::UInt(u) => u.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => format!("\"{}\"", Self::escape_json_string(s)),
            Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::value_to_json).collect();
                format!("[{}]", items.join(","))
            }
            Value::Map(m) => {
                let items: Vec<String> = m.iter()
                    .map(|(k, v)| format!("\"{}\":{}", Self::escape_json_string(k), Self::value_to_json(v)))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
            _ => format!("\"{}\"", value.to_string_repr()),
        }
    }

    fn escape_json_string(s: &str) -> String {
        s.replace('\\', "\\\\")
         .replace('"', "\\\"")
         .replace('\n', "\\n")
         .replace('\r', "\\r")
         .replace('\t', "\\t")
    }

    /// Extract value at JSON path
    pub fn json_extract(json: &Value, path: &Value) -> FunctionResult {
        let value = match json {
            Value::String(s) => Self::parse_json_str(s)?,
            Value::Map(_) | Value::Array(_) => json.clone(),
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "JSON String, Map, or Array",
                got: json.type_name().to_string(),
            }),
        };

        let path = path.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: path.type_name().to_string(),
        })?;

        Self::extract_path(&value, path)
    }

    fn extract_path(value: &Value, path: &str) -> FunctionResult {
        let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
        let mut current = value.clone();

        for part in parts {
            // Check for array index
            if let Some(bracket_pos) = part.find('[') {
                let key = &part[..bracket_pos];
                let idx_str = &part[bracket_pos+1..part.len()-1];

                if !key.is_empty() {
                    current = match &current {
                        Value::Map(m) => m.get(key).cloned().unwrap_or(Value::Null),
                        _ => return Ok(Value::Null),
                    };
                }

                if let Ok(idx) = idx_str.parse::<usize>() {
                    current = match &current {
                        Value::Array(arr) => arr.get(idx).cloned().unwrap_or(Value::Null),
                        _ => return Ok(Value::Null),
                    };
                }
            } else {
                current = match &current {
                    Value::Map(m) => m.get(part).cloned().unwrap_or(Value::Null),
                    _ => return Ok(Value::Null),
                };
            }
        }

        Ok(current)
    }

    /// Get JSON type
    pub fn json_type(json: &Value) -> FunctionResult {
        let value = match json {
            Value::String(s) => Self::parse_json_str(s)?,
            _ => json.clone(),
        };

        let type_name = match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int(_) | Value::UInt(_) | Value::Float(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Map(_) => "object",
            _ => "unknown",
        };

        Ok(Value::String(type_name.to_string()))
    }

    /// Check if JSON has key
    pub fn json_has(json: &Value, key: &Value) -> FunctionResult {
        let value = match json {
            Value::String(s) => Self::parse_json_str(s)?,
            Value::Map(_) => json.clone(),
            _ => return Ok(Value::Bool(false)),
        };

        let key = key.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: key.type_name().to_string(),
        })?;

        match value {
            Value::Map(m) => Ok(Value::Bool(m.contains_key(key))),
            _ => Ok(Value::Bool(false)),
        }
    }

    /// Get array length or object key count
    pub fn json_length(json: &Value) -> FunctionResult {
        let value = match json {
            Value::String(s) => Self::parse_json_str(s)?,
            _ => json.clone(),
        };

        match value {
            Value::Array(arr) => Ok(Value::UInt(arr.len() as u64)),
            Value::Map(m) => Ok(Value::UInt(m.len() as u64)),
            Value::String(s) => Ok(Value::UInt(s.len() as u64)),
            _ => Ok(Value::Null),
        }
    }
}

// ============================================================================
// Geo Functions
// ============================================================================

pub struct GeoFunctions;

impl GeoFunctions {
    const EARTH_RADIUS_M: f64 = 6371000.0;

    /// Create a point
    pub fn point(lon: &Value, lat: &Value) -> FunctionResult {
        let lon = lon.as_f64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Float",
            got: lon.type_name().to_string(),
        })?;
        let lat = lat.as_f64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Float",
            got: lat.type_name().to_string(),
        })?;

        Ok(Value::Point(lon, lat))
    }

    /// Get longitude from point
    pub fn geo_lon(point: &Value) -> FunctionResult {
        match point {
            Value::Point(lon, _) => Ok(Value::Float(*lon)),
            _ => Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: point.type_name().to_string(),
            }),
        }
    }

    /// Get latitude from point
    pub fn geo_lat(point: &Value) -> FunctionResult {
        match point {
            Value::Point(_, lat) => Ok(Value::Float(*lat)),
            _ => Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: point.type_name().to_string(),
            }),
        }
    }

    /// Calculate great circle distance in meters
    pub fn great_circle_distance(p1: &Value, p2: &Value) -> FunctionResult {
        let (lon1, lat1) = match p1 {
            Value::Point(lon, lat) => (*lon, *lat),
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: p1.type_name().to_string(),
            }),
        };
        let (lon2, lat2) = match p2 {
            Value::Point(lon, lat) => (*lon, *lat),
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: p2.type_name().to_string(),
            }),
        };

        let distance = Self::haversine(lat1, lon1, lat2, lon2);
        Ok(Value::Float(distance))
    }

    fn haversine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        let lat1_rad = lat1.to_radians();
        let lat2_rad = lat2.to_radians();
        let lon1_rad = lon1.to_radians();
        let lon2_rad = lon2.to_radians();

        let dlat = lat2_rad - lat1_rad;
        let dlon = lon2_rad - lon1_rad;

        let a = (dlat / 2.0).sin().powi(2) +
                lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();

        Self::EARTH_RADIUS_M * c
    }

    /// Check if point is within radius (meters) of center
    pub fn point_in_circle(point: &Value, center: &Value, radius: &Value) -> FunctionResult {
        let radius = radius.as_f64().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "Float",
            got: radius.type_name().to_string(),
        })?;

        let distance = Self::great_circle_distance(point, center)?;
        match distance {
            Value::Float(d) => Ok(Value::Bool(d <= radius)),
            _ => unreachable!(),
        }
    }

    /// Check if point is within bounding box
    pub fn point_in_bbox(point: &Value, min_lon: &Value, min_lat: &Value, max_lon: &Value, max_lat: &Value) -> FunctionResult {
        let (lon, lat) = match point {
            Value::Point(lon, lat) => (*lon, *lat),
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: point.type_name().to_string(),
            }),
        };

        let min_lon = min_lon.as_f64().unwrap_or(f64::NEG_INFINITY);
        let min_lat = min_lat.as_f64().unwrap_or(f64::NEG_INFINITY);
        let max_lon = max_lon.as_f64().unwrap_or(f64::INFINITY);
        let max_lat = max_lat.as_f64().unwrap_or(f64::INFINITY);

        let in_bbox = lon >= min_lon && lon <= max_lon && lat >= min_lat && lat <= max_lat;
        Ok(Value::Bool(in_bbox))
    }

    /// Calculate geohash for a point
    pub fn geohash_encode(point: &Value, precision: &Value) -> FunctionResult {
        let (lon, lat) = match point {
            Value::Point(lon, lat) => (*lon, *lat),
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "Point",
                got: point.type_name().to_string(),
            }),
        };

        let precision = precision.as_i64().unwrap_or(12) as usize;
        let hash = Self::encode_geohash(lat, lon, precision);
        Ok(Value::String(hash))
    }

    fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
        const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut hash = String::new();
        let mut bits = 0u8;
        let mut bit_count = 0;
        let mut is_lon = true;

        while hash.len() < precision {
            if is_lon {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if lon >= mid {
                    bits = (bits << 1) | 1;
                    lon_range.0 = mid;
                } else {
                    bits <<= 1;
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if lat >= mid {
                    bits = (bits << 1) | 1;
                    lat_range.0 = mid;
                } else {
                    bits <<= 1;
                    lat_range.1 = mid;
                }
            }

            is_lon = !is_lon;
            bit_count += 1;

            if bit_count == 5 {
                hash.push(BASE32[bits as usize] as char);
                bits = 0;
                bit_count = 0;
            }
        }

        hash
    }

    /// Decode geohash to bounding box
    pub fn geohash_decode(hash: &Value) -> FunctionResult {
        let hash = hash.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: hash.type_name().to_string(),
        })?;

        let (min_lat, min_lon, max_lat, max_lon) = Self::decode_geohash(hash)?;
        let center_lon = (min_lon + max_lon) / 2.0;
        let center_lat = (min_lat + max_lat) / 2.0;

        Ok(Value::Point(center_lon, center_lat))
    }

    fn decode_geohash(hash: &str) -> Result<(f64, f64, f64, f64), FunctionError> {
        const BASE32: &str = "0123456789bcdefghjkmnpqrstuvwxyz";

        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut is_lon = true;

        for c in hash.chars() {
            let idx = BASE32.find(c.to_ascii_lowercase())
                .ok_or_else(|| FunctionError::ParseError("Invalid geohash character".to_string()))?;

            for bit in (0..5).rev() {
                let val = (idx >> bit) & 1;
                if is_lon {
                    let mid = (lon_range.0 + lon_range.1) / 2.0;
                    if val == 1 {
                        lon_range.0 = mid;
                    } else {
                        lon_range.1 = mid;
                    }
                } else {
                    let mid = (lat_range.0 + lat_range.1) / 2.0;
                    if val == 1 {
                        lat_range.0 = mid;
                    } else {
                        lat_range.1 = mid;
                    }
                }
                is_lon = !is_lon;
            }
        }

        Ok((lat_range.0, lon_range.0, lat_range.1, lon_range.1))
    }
}

// ============================================================================
// URL Functions
// ============================================================================

pub struct UrlFunctions;

impl UrlFunctions {
    /// Extract domain from URL
    pub fn domain(url: &Value) -> FunctionResult {
        let url = url.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: url.type_name().to_string(),
        })?;

        let domain = Self::extract_domain(url);
        Ok(Value::String(domain))
    }

    fn extract_domain(url: &str) -> String {
        let url = url.trim();

        // Skip protocol
        let without_protocol = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .or_else(|| url.strip_prefix("//"))
            .unwrap_or(url);

        // Get host part (before path, query, fragment)
        let host = without_protocol
            .split(&['/', '?', '#', ':'][..])
            .next()
            .unwrap_or("");

        host.to_string()
    }

    /// Extract top-level domain
    pub fn top_level_domain(url: &Value) -> FunctionResult {
        let domain = match Self::domain(url)? {
            Value::String(d) => d,
            _ => return Ok(Value::String(String::new())),
        };

        let parts: Vec<&str> = domain.split('.').collect();
        if parts.len() >= 2 {
            Ok(Value::String(parts[parts.len() - 1].to_string()))
        } else {
            Ok(Value::String(String::new()))
        }
    }

    /// Extract domain without www
    pub fn domain_without_www(url: &Value) -> FunctionResult {
        let domain = match Self::domain(url)? {
            Value::String(d) => d,
            _ => return Ok(Value::String(String::new())),
        };

        let without_www = domain.strip_prefix("www.").unwrap_or(&domain);
        Ok(Value::String(without_www.to_string()))
    }

    /// Extract protocol
    pub fn protocol(url: &Value) -> FunctionResult {
        let url = url.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: url.type_name().to_string(),
        })?;

        let protocol = if url.starts_with("https://") {
            "https"
        } else if url.starts_with("http://") {
            "http"
        } else if url.starts_with("ftp://") {
            "ftp"
        } else {
            ""
        };

        Ok(Value::String(protocol.to_string()))
    }

    /// Extract path
    pub fn path(url: &Value) -> FunctionResult {
        let url = url.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: url.type_name().to_string(),
        })?;

        let without_protocol = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .or_else(|| url.strip_prefix("//"))
            .unwrap_or(url);

        // Find start of path
        let path_start = without_protocol.find('/').unwrap_or(without_protocol.len());
        let path_part = &without_protocol[path_start..];

        // Remove query and fragment
        let path = path_part
            .split(&['?', '#'][..])
            .next()
            .unwrap_or("/");

        Ok(Value::String(path.to_string()))
    }

    /// Extract query string
    pub fn query_string(url: &Value) -> FunctionResult {
        let url = url.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: url.type_name().to_string(),
        })?;

        let query = url.split('?')
            .nth(1)
            .map(|q| q.split('#').next().unwrap_or(""))
            .unwrap_or("");

        Ok(Value::String(query.to_string()))
    }

    /// Extract query parameter value
    pub fn extract_url_parameter(url: &Value, param: &Value) -> FunctionResult {
        let query = match Self::query_string(url)? {
            Value::String(q) => q,
            _ => return Ok(Value::String(String::new())),
        };

        let param = param.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: param.type_name().to_string(),
        })?;

        for pair in query.split('&') {
            let mut parts = pair.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                if key == param {
                    return Ok(Value::String(Self::url_decode(value)));
                }
            }
        }

        Ok(Value::String(String::new()))
    }

    /// Decode URL-encoded string
    pub fn decode_url_component(encoded: &Value) -> FunctionResult {
        let s = encoded.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: encoded.type_name().to_string(),
        })?;

        Ok(Value::String(Self::url_decode(s)))
    }

    fn url_decode(s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                let hex: String = chars.by_ref().take(2).collect();
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                } else {
                    result.push('%');
                    result.push_str(&hex);
                }
            } else if c == '+' {
                result.push(' ');
            } else {
                result.push(c);
            }
        }

        result
    }

    /// Encode URL component
    pub fn encode_url_component(s: &Value) -> FunctionResult {
        let s = s.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: s.type_name().to_string(),
        })?;

        Ok(Value::String(Self::url_encode(s)))
    }

    fn url_encode(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
                _ => {
                    for byte in c.to_string().bytes() {
                        result.push_str(&format!("%{:02X}", byte));
                    }
                }
            }
        }
        result
    }
}

// ============================================================================
// IP Functions
// ============================================================================

pub struct IpFunctions;

impl IpFunctions {
    /// Parse IPv4 address to integer
    pub fn ipv4_string_to_num(ip: &Value) -> FunctionResult {
        let ip_str = ip.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: ip.type_name().to_string(),
        })?;

        let addr: Ipv4Addr = ip_str.parse()
            .map_err(|_| FunctionError::ParseError(format!("Invalid IPv4: {}", ip_str)))?;

        Ok(Value::UInt(u32::from(addr) as u64))
    }

    /// Convert integer to IPv4 string
    pub fn ipv4_num_to_string(num: &Value) -> FunctionResult {
        let num = match num {
            Value::UInt(n) => *n as u32,
            Value::Int(n) => *n as u32,
            _ => return Err(FunctionError::InvalidArgumentType {
                expected: "Integer",
                got: num.type_name().to_string(),
            }),
        };

        let addr = Ipv4Addr::from(num);
        Ok(Value::String(addr.to_string()))
    }

    /// Parse IPv6 address to integer
    pub fn ipv6_string_to_num(ip: &Value) -> FunctionResult {
        let ip_str = ip.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: ip.type_name().to_string(),
        })?;

        let addr: Ipv6Addr = ip_str.parse()
            .map_err(|_| FunctionError::ParseError(format!("Invalid IPv6: {}", ip_str)))?;

        Ok(Value::Ipv6(u128::from(addr)))
    }

    /// Check if IP is in CIDR range
    pub fn ipv4_cidr_to_range(cidr: &Value) -> FunctionResult {
        let cidr_str = cidr.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: cidr.type_name().to_string(),
        })?;

        let parts: Vec<&str> = cidr_str.split('/').collect();
        if parts.len() != 2 {
            return Err(FunctionError::ParseError("Invalid CIDR notation".to_string()));
        }

        let addr: Ipv4Addr = parts[0].parse()
            .map_err(|_| FunctionError::ParseError("Invalid IP address".to_string()))?;
        let prefix: u8 = parts[1].parse()
            .map_err(|_| FunctionError::ParseError("Invalid prefix length".to_string()))?;

        if prefix > 32 {
            return Err(FunctionError::OutOfRange("Prefix must be <= 32".to_string()));
        }

        let ip_num = u32::from(addr);
        let mask = if prefix == 0 { 0 } else { !0u32 << (32 - prefix) };
        let start = ip_num & mask;
        let end = start | !mask;

        Ok(Value::Tuple(vec![
            Value::String(Ipv4Addr::from(start).to_string()),
            Value::String(Ipv4Addr::from(end).to_string()),
        ]))
    }

    /// Check if IPv4 is in range
    pub fn ipv4_in_range(ip: &Value, start: &Value, end: &Value) -> FunctionResult {
        let ip_num = match Self::ipv4_string_to_num(ip)? {
            Value::UInt(n) => n as u32,
            _ => return Ok(Value::Bool(false)),
        };
        let start_num = match Self::ipv4_string_to_num(start)? {
            Value::UInt(n) => n as u32,
            _ => return Ok(Value::Bool(false)),
        };
        let end_num = match Self::ipv4_string_to_num(end)? {
            Value::UInt(n) => n as u32,
            _ => return Ok(Value::Bool(false)),
        };

        Ok(Value::Bool(ip_num >= start_num && ip_num <= end_num))
    }

    /// Check if IP is private (RFC 1918)
    pub fn is_ipv4_private(ip: &Value) -> FunctionResult {
        let ip_str = ip.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: ip.type_name().to_string(),
        })?;

        let addr: Ipv4Addr = ip_str.parse()
            .map_err(|_| FunctionError::ParseError(format!("Invalid IPv4: {}", ip_str)))?;

        let is_private = addr.is_private() || addr.is_loopback() || addr.is_link_local();
        Ok(Value::Bool(is_private))
    }

    /// Get IP version
    pub fn ip_version(ip: &Value) -> FunctionResult {
        let ip_str = ip.as_str().ok_or_else(|| FunctionError::InvalidArgumentType {
            expected: "String",
            got: ip.type_name().to_string(),
        })?;

        if ip_str.parse::<Ipv4Addr>().is_ok() {
            Ok(Value::Int(4))
        } else if ip_str.parse::<Ipv6Addr>().is_ok() {
            Ok(Value::Int(6))
        } else {
            Ok(Value::Int(0))
        }
    }
}

// ============================================================================
// Full-Text Search (Inverted Index)
// ============================================================================

/// Token from text
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Token {
    pub term: String,
    pub position: u32,
    pub offset_start: u32,
    pub offset_end: u32,
}

/// Tokenizer trait
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

/// Simple whitespace tokenizer
pub struct SimpleTokenizer {
    lowercase: bool,
    min_length: usize,
    max_length: usize,
}

impl SimpleTokenizer {
    pub fn new(lowercase: bool, min_length: usize, max_length: usize) -> Self {
        Self { lowercase, min_length, max_length }
    }
}

impl Default for SimpleTokenizer {
    fn default() -> Self {
        Self::new(true, 1, 100)
    }
}

impl Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0u32;
        let mut offset = 0u32;

        for word in text.split(|c: char| c.is_whitespace() || c.is_ascii_punctuation()) {
            let word = if self.lowercase {
                word.to_lowercase()
            } else {
                word.to_string()
            };

            if word.len() >= self.min_length && word.len() <= self.max_length {
                tokens.push(Token {
                    term: word.clone(),
                    position,
                    offset_start: offset,
                    offset_end: offset + word.len() as u32,
                });
                position += 1;
            }
            offset += word.len() as u32 + 1;
        }

        tokens
    }
}

/// N-gram tokenizer
pub struct NgramTokenizer {
    min_gram: usize,
    max_gram: usize,
    lowercase: bool,
}

impl NgramTokenizer {
    pub fn new(min_gram: usize, max_gram: usize, lowercase: bool) -> Self {
        Self { min_gram, max_gram, lowercase }
    }
}

impl Tokenizer for NgramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let text = if self.lowercase {
            text.to_lowercase()
        } else {
            text.to_string()
        };

        let chars: Vec<char> = text.chars().collect();
        let mut tokens = Vec::new();
        let mut position = 0u32;

        for n in self.min_gram..=self.max_gram {
            for i in 0..=chars.len().saturating_sub(n) {
                let term: String = chars[i..i+n].iter().collect();
                tokens.push(Token {
                    term,
                    position,
                    offset_start: i as u32,
                    offset_end: (i + n) as u32,
                });
                position += 1;
            }
        }

        tokens
    }
}

/// Posting list entry
#[derive(Debug, Clone)]
pub struct Posting {
    pub doc_id: u64,
    pub positions: Vec<u32>,
    pub term_frequency: u32,
}

/// Inverted index for full-text search
pub struct InvertedIndex {
    /// term -> postings
    index: RwLock<HashMap<String, Vec<Posting>>>,
    /// doc_id -> doc length (for BM25)
    doc_lengths: RwLock<HashMap<u64, u32>>,
    /// Total documents
    doc_count: RwLock<u64>,
    /// Average document length
    avg_doc_length: RwLock<f64>,
    /// Tokenizer
    tokenizer: Arc<dyn Tokenizer>,
}

impl InvertedIndex {
    pub fn new(tokenizer: Arc<dyn Tokenizer>) -> Self {
        Self {
            index: RwLock::new(HashMap::new()),
            doc_lengths: RwLock::new(HashMap::new()),
            doc_count: RwLock::new(0),
            avg_doc_length: RwLock::new(0.0),
            tokenizer,
        }
    }

    /// Index a document
    pub fn index_document(&self, doc_id: u64, text: &str) {
        let tokens = self.tokenizer.tokenize(text);
        let doc_length = tokens.len() as u32;

        // Group tokens by term
        let mut term_positions: HashMap<String, Vec<u32>> = HashMap::new();
        for token in &tokens {
            term_positions.entry(token.term.clone())
                .or_default()
                .push(token.position);
        }

        // Update index
        let mut index = self.index.write();
        for (term, positions) in term_positions {
            let posting = Posting {
                doc_id,
                positions: positions.clone(),
                term_frequency: positions.len() as u32,
            };

            index.entry(term).or_default().push(posting);
        }

        // Update doc stats
        let mut doc_lengths = self.doc_lengths.write();
        doc_lengths.insert(doc_id, doc_length);

        let mut doc_count = self.doc_count.write();
        *doc_count += 1;

        // Update average
        let total_length: u32 = doc_lengths.values().sum();
        *self.avg_doc_length.write() = total_length as f64 / *doc_count as f64;
    }

    /// Search for documents containing term
    pub fn search(&self, query: &str) -> Vec<(u64, f64)> {
        let tokens = self.tokenizer.tokenize(query);
        let terms: Vec<String> = tokens.iter().map(|t| t.term.clone()).collect();

        if terms.is_empty() {
            return vec![];
        }

        let index = self.index.read();
        let doc_count = *self.doc_count.read();
        let avg_dl = *self.avg_doc_length.read();
        let doc_lengths = self.doc_lengths.read();

        // Calculate BM25 scores
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for term in &terms {
            if let Some(postings) = index.get(term) {
                let df = postings.len() as f64;
                let idf = ((doc_count as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();

                for posting in postings {
                    let tf = posting.term_frequency as f64;
                    let dl = *doc_lengths.get(&posting.doc_id).unwrap_or(&1) as f64;

                    // BM25 formula
                    let k1 = 1.2;
                    let b = 0.75;
                    let score = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avg_dl));

                    *scores.entry(posting.doc_id).or_insert(0.0) += score;
                }
            }
        }

        let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Search with phrase matching
    pub fn search_phrase(&self, phrase: &str) -> Vec<u64> {
        let tokens = self.tokenizer.tokenize(phrase);
        if tokens.is_empty() {
            return vec![];
        }

        let index = self.index.read();

        // Find documents containing all terms
        let mut candidate_docs: Option<HashSet<u64>> = None;
        for token in &tokens {
            if let Some(postings) = index.get(&token.term) {
                let docs: HashSet<u64> = postings.iter().map(|p| p.doc_id).collect();
                candidate_docs = Some(match candidate_docs {
                    Some(existing) => existing.intersection(&docs).cloned().collect(),
                    None => docs,
                });
            } else {
                return vec![];
            }
        }

        let candidates = match candidate_docs {
            Some(docs) => docs,
            None => return vec![],
        };

        // Check phrase order in each candidate
        let mut results = Vec::new();
        for doc_id in candidates {
            if self.check_phrase_in_doc(&index, doc_id, &tokens) {
                results.push(doc_id);
            }
        }

        results
    }

    fn check_phrase_in_doc(&self, index: &HashMap<String, Vec<Posting>>, doc_id: u64, tokens: &[Token]) -> bool {
        if tokens.is_empty() {
            return false;
        }

        // Get positions for first term
        let first_term = &tokens[0].term;
        let first_positions: Vec<u32> = index.get(first_term)
            .and_then(|postings| postings.iter().find(|p| p.doc_id == doc_id))
            .map(|p| p.positions.clone())
            .unwrap_or_default();

        // Check if remaining terms follow
        'outer: for &start_pos in &first_positions {
            for (i, token) in tokens.iter().enumerate().skip(1) {
                let expected_pos = start_pos + i as u32;

                let has_position = index.get(&token.term)
                    .and_then(|postings| postings.iter().find(|p| p.doc_id == doc_id))
                    .map(|p| p.positions.contains(&expected_pos))
                    .unwrap_or(false);

                if !has_position {
                    continue 'outer;
                }
            }
            return true;
        }

        false
    }

    /// Get term count
    pub fn term_count(&self) -> usize {
        self.index.read().len()
    }

    /// Get document count
    pub fn doc_count(&self) -> u64 {
        *self.doc_count.read()
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new(Arc::new(SimpleTokenizer::default()))
    }
}

// ============================================================================
// Sampling
// ============================================================================

/// Sampling method
#[derive(Debug, Clone)]
pub enum SamplingMethod {
    /// Random sampling with ratio (0.0 - 1.0)
    Random(f64),
    /// Deterministic sampling based on expression hash
    Deterministic { ratio: f64, seed: u64 },
    /// Block-based sampling (sample every Nth block)
    Block { n: u64, offset: u64 },
    /// Reservoir sampling for streaming
    Reservoir(usize),
}

/// Sampler for query results
pub struct Sampler {
    method: SamplingMethod,
    /// For deterministic sampling
    seed: u64,
    /// For reservoir sampling
    reservoir: RwLock<Vec<Value>>,
    reservoir_count: RwLock<u64>,
}

impl Sampler {
    pub fn new(method: SamplingMethod) -> Self {
        let seed = match &method {
            SamplingMethod::Deterministic { seed, .. } => *seed,
            _ => 0,
        };

        Self {
            method,
            seed,
            reservoir: RwLock::new(Vec::new()),
            reservoir_count: RwLock::new(0),
        }
    }

    /// Check if a row should be sampled (by row number)
    pub fn should_sample(&self, row_num: u64) -> bool {
        match &self.method {
            SamplingMethod::Random(ratio) => {
                let hash = self.hash_row(row_num);
                (hash as f64 / u64::MAX as f64) < *ratio
            }
            SamplingMethod::Deterministic { ratio, .. } => {
                let hash = self.hash_row(row_num);
                (hash as f64 / u64::MAX as f64) < *ratio
            }
            SamplingMethod::Block { n, offset } => {
                (row_num + offset) % n == 0
            }
            SamplingMethod::Reservoir(_) => {
                // Reservoir sampling handles this differently
                true
            }
        }
    }

    /// Check if a value should be sampled (deterministic based on value)
    pub fn should_sample_value(&self, value: &Value) -> bool {
        match &self.method {
            SamplingMethod::Deterministic { ratio, .. } => {
                let hash = self.hash_value(value);
                (hash as f64 / u64::MAX as f64) < *ratio
            }
            _ => self.should_sample(0),
        }
    }

    /// Add value to reservoir (for reservoir sampling)
    pub fn add_to_reservoir(&self, value: Value) {
        if let SamplingMethod::Reservoir(k) = &self.method {
            let mut reservoir = self.reservoir.write();
            let mut count = self.reservoir_count.write();
            *count += 1;

            if reservoir.len() < *k {
                reservoir.push(value);
            } else {
                // Replace with probability k/count
                let idx = self.hash_row(*count) as usize % *count as usize;
                if idx < *k {
                    reservoir[idx] = value;
                }
            }
        }
    }

    /// Get reservoir sample
    pub fn get_reservoir(&self) -> Vec<Value> {
        self.reservoir.read().clone()
    }

    fn hash_row(&self, row_num: u64) -> u64 {
        // Simple hash combining seed and row number
        let mut hash = self.seed;
        hash ^= row_num;
        hash = hash.wrapping_mul(0x517cc1b727220a95);
        hash ^= hash >> 32;
        hash
    }

    fn hash_value(&self, value: &Value) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        self.seed.hash(&mut hasher);
        value.to_string_repr().hash(&mut hasher);
        hasher.finish()
    }

    /// Get sampling ratio
    pub fn ratio(&self) -> f64 {
        match &self.method {
            SamplingMethod::Random(r) => *r,
            SamplingMethod::Deterministic { ratio, .. } => *ratio,
            SamplingMethod::Block { n, .. } => 1.0 / *n as f64,
            SamplingMethod::Reservoir(k) => {
                let count = *self.reservoir_count.read();
                if count == 0 { 1.0 } else { *k as f64 / count as f64 }
            }
        }
    }
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new(SamplingMethod::Random(0.1))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Array Function Tests

    #[test]
    fn test_array_basic() {
        let arr = ArrayFunctions::array(&[Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap();
        assert_eq!(arr, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));

        let len = ArrayFunctions::length(&arr).unwrap();
        assert_eq!(len, Value::UInt(3));

        let empty = ArrayFunctions::empty(&arr).unwrap();
        assert_eq!(empty, Value::Bool(false));
    }

    #[test]
    fn test_array_element() {
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);

        assert_eq!(ArrayFunctions::element(&arr, &Value::Int(1)).unwrap(), Value::Int(10));
        assert_eq!(ArrayFunctions::element(&arr, &Value::Int(2)).unwrap(), Value::Int(20));
        assert_eq!(ArrayFunctions::element(&arr, &Value::Int(-1)).unwrap(), Value::Int(30));
        assert_eq!(ArrayFunctions::element(&arr, &Value::Int(0)).unwrap(), Value::Null);
    }

    #[test]
    fn test_array_has() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);

        assert_eq!(ArrayFunctions::has(&arr, &Value::Int(2)).unwrap(), Value::Bool(true));
        assert_eq!(ArrayFunctions::has(&arr, &Value::Int(5)).unwrap(), Value::Bool(false));

        let search = Value::Array(vec![Value::Int(1), Value::Int(5)]);
        assert_eq!(ArrayFunctions::has_any(&arr, &search).unwrap(), Value::Bool(true));
        assert_eq!(ArrayFunctions::has_all(&arr, &search).unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_array_operations() {
        let arr1 = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let arr2 = Value::Array(vec![Value::Int(3), Value::Int(4)]);

        let concat = ArrayFunctions::concat(&[arr1.clone(), arr2]).unwrap();
        assert_eq!(ArrayFunctions::length(&concat).unwrap(), Value::UInt(4));

        let with_dups = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(1)]);
        let distinct = ArrayFunctions::distinct(&with_dups).unwrap();
        assert_eq!(ArrayFunctions::length(&distinct).unwrap(), Value::UInt(2));
    }

    #[test]
    fn test_array_math() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)]);

        assert_eq!(ArrayFunctions::array_sum(&arr).unwrap(), Value::Float(10.0));
        assert_eq!(ArrayFunctions::array_avg(&arr).unwrap(), Value::Float(2.5));
        assert_eq!(ArrayFunctions::array_min(&arr).unwrap(), Value::Float(1.0));
        assert_eq!(ArrayFunctions::array_max(&arr).unwrap(), Value::Float(4.0));
    }

    // Map Function Tests

    #[test]
    fn test_map_basic() {
        let keys = Value::Array(vec![Value::String("a".to_string()), Value::String("b".to_string())]);
        let values = Value::Array(vec![Value::Int(1), Value::Int(2)]);

        let map = MapFunctions::map(&keys, &values).unwrap();

        assert_eq!(MapFunctions::map_get(&map, &Value::String("a".to_string())).unwrap(), Value::Int(1));
        assert_eq!(MapFunctions::map_contains(&map, &Value::String("a".to_string())).unwrap(), Value::Bool(true));
        assert_eq!(MapFunctions::map_size(&map).unwrap(), Value::UInt(2));
    }

    // JSON Function Tests

    #[test]
    fn test_json_parse() {
        let json = Value::String(r#"{"name": "Alice", "age": 30}"#.to_string());
        let parsed = JsonFunctions::parse_json(&json).unwrap();

        assert!(matches!(parsed, Value::Map(_)));

        let name = JsonFunctions::json_extract(&parsed, &Value::String("name".to_string())).unwrap();
        assert_eq!(name, Value::String("Alice".to_string()));
    }

    #[test]
    fn test_json_array() {
        let json = Value::String(r#"[1, 2, 3]"#.to_string());
        let parsed = JsonFunctions::parse_json(&json).unwrap();

        assert_eq!(JsonFunctions::json_length(&parsed).unwrap(), Value::UInt(3));
    }

    #[test]
    fn test_json_extract_path() {
        let json = Value::String(r#"{"user": {"name": "Bob", "scores": [10, 20, 30]}}"#.to_string());

        let name = JsonFunctions::json_extract(&json, &Value::String("user.name".to_string())).unwrap();
        assert_eq!(name, Value::String("Bob".to_string()));

        let score = JsonFunctions::json_extract(&json, &Value::String("user.scores[1]".to_string())).unwrap();
        assert_eq!(score, Value::Int(20));
    }

    #[test]
    fn test_json_to_string() {
        let value = Value::Map({
            let mut m = BTreeMap::new();
            m.insert("a".to_string(), Value::Int(1));
            m
        });

        let json = JsonFunctions::to_json(&value).unwrap();
        assert!(matches!(json, Value::String(_)));
    }

    // Geo Function Tests

    #[test]
    fn test_geo_point() {
        let point = GeoFunctions::point(&Value::Float(-122.4194), &Value::Float(37.7749)).unwrap();
        assert!(matches!(point, Value::Point(_, _)));

        assert_eq!(GeoFunctions::geo_lon(&point).unwrap(), Value::Float(-122.4194));
        assert_eq!(GeoFunctions::geo_lat(&point).unwrap(), Value::Float(37.7749));
    }

    #[test]
    fn test_geo_distance() {
        // SF to LA
        let sf = Value::Point(-122.4194, 37.7749);
        let la = Value::Point(-118.2437, 34.0522);

        let distance = GeoFunctions::great_circle_distance(&sf, &la).unwrap();
        if let Value::Float(d) = distance {
            // Should be approximately 559 km
            assert!(d > 500_000.0 && d < 600_000.0);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_geohash() {
        let point = Value::Point(-122.4194, 37.7749);
        let hash = GeoFunctions::geohash_encode(&point, &Value::Int(6)).unwrap();

        if let Value::String(h) = hash {
            assert_eq!(h.len(), 6);

            let decoded = GeoFunctions::geohash_decode(&Value::String(h)).unwrap();
            if let Value::Point(lon, lat) = decoded {
                assert!((lon - (-122.4194)).abs() < 0.1);
                assert!((lat - 37.7749).abs() < 0.1);
            }
        }
    }

    // URL Function Tests

    #[test]
    fn test_url_parsing() {
        let url = Value::String("https://www.example.com/path/to/page?query=test#anchor".to_string());

        assert_eq!(UrlFunctions::domain(&url).unwrap(), Value::String("www.example.com".to_string()));
        assert_eq!(UrlFunctions::domain_without_www(&url).unwrap(), Value::String("example.com".to_string()));
        assert_eq!(UrlFunctions::protocol(&url).unwrap(), Value::String("https".to_string()));
        assert_eq!(UrlFunctions::path(&url).unwrap(), Value::String("/path/to/page".to_string()));
        assert_eq!(UrlFunctions::query_string(&url).unwrap(), Value::String("query=test".to_string()));
    }

    #[test]
    fn test_url_parameter() {
        let url = Value::String("https://example.com?foo=bar&baz=qux".to_string());

        assert_eq!(
            UrlFunctions::extract_url_parameter(&url, &Value::String("foo".to_string())).unwrap(),
            Value::String("bar".to_string())
        );
        assert_eq!(
            UrlFunctions::extract_url_parameter(&url, &Value::String("missing".to_string())).unwrap(),
            Value::String(String::new())
        );
    }

    // IP Function Tests

    #[test]
    fn test_ip_conversion() {
        let ip = Value::String("192.168.1.1".to_string());
        let num = IpFunctions::ipv4_string_to_num(&ip).unwrap();

        if let Value::UInt(n) = num {
            let back = IpFunctions::ipv4_num_to_string(&Value::UInt(n)).unwrap();
            assert_eq!(back, Value::String("192.168.1.1".to_string()));
        }
    }

    #[test]
    fn test_ip_cidr() {
        let cidr = Value::String("192.168.1.0/24".to_string());
        let range = IpFunctions::ipv4_cidr_to_range(&cidr).unwrap();

        if let Value::Tuple(vals) = range {
            assert_eq!(vals[0], Value::String("192.168.1.0".to_string()));
            assert_eq!(vals[1], Value::String("192.168.1.255".to_string()));
        }
    }

    #[test]
    fn test_ip_private() {
        assert_eq!(
            IpFunctions::is_ipv4_private(&Value::String("192.168.1.1".to_string())).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            IpFunctions::is_ipv4_private(&Value::String("8.8.8.8".to_string())).unwrap(),
            Value::Bool(false)
        );
    }

    // Full-Text Search Tests

    #[test]
    fn test_simple_tokenizer() {
        let tokenizer = SimpleTokenizer::default();
        let tokens = tokenizer.tokenize("Hello, World! How are you?");

        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].term, "hello");
        assert_eq!(tokens[1].term, "world");
    }

    #[test]
    fn test_ngram_tokenizer() {
        let tokenizer = NgramTokenizer::new(2, 3, true);
        let tokens = tokenizer.tokenize("abc");

        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert!(terms.contains(&"ab"));
        assert!(terms.contains(&"bc"));
        assert!(terms.contains(&"abc"));
    }

    #[test]
    fn test_inverted_index() {
        let index = InvertedIndex::default();

        index.index_document(1, "The quick brown fox");
        index.index_document(2, "The lazy dog");
        index.index_document(3, "Quick brown dogs");

        let results = index.search("quick");
        assert!(results.iter().any(|(id, _)| *id == 1));
        assert!(results.iter().any(|(id, _)| *id == 3));

        let results = index.search("dog");
        assert!(results.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn test_phrase_search() {
        let index = InvertedIndex::default();

        index.index_document(1, "the quick brown fox jumps");
        index.index_document(2, "brown quick fox");

        let results = index.search_phrase("quick brown");
        assert!(results.contains(&1));
        assert!(!results.contains(&2)); // Wrong order
    }

    // Sampling Tests

    #[test]
    fn test_random_sampling() {
        let sampler = Sampler::new(SamplingMethod::Random(0.5));

        let mut sampled = 0;
        for i in 0..1000 {
            if sampler.should_sample(i) {
                sampled += 1;
            }
        }

        // Should be roughly 50% (within 10%)
        assert!(sampled > 400 && sampled < 600);
    }

    #[test]
    fn test_block_sampling() {
        let sampler = Sampler::new(SamplingMethod::Block { n: 10, offset: 0 });

        let mut sampled = 0;
        for i in 0..100 {
            if sampler.should_sample(i) {
                sampled += 1;
            }
        }

        assert_eq!(sampled, 10); // Every 10th row
    }

    #[test]
    fn test_reservoir_sampling() {
        let sampler = Sampler::new(SamplingMethod::Reservoir(10));

        for i in 0..100 {
            sampler.add_to_reservoir(Value::Int(i));
        }

        let reservoir = sampler.get_reservoir();
        assert_eq!(reservoir.len(), 10);
    }

    #[test]
    fn test_deterministic_sampling() {
        let sampler1 = Sampler::new(SamplingMethod::Deterministic { ratio: 0.3, seed: 42 });
        let sampler2 = Sampler::new(SamplingMethod::Deterministic { ratio: 0.3, seed: 42 });

        // Same seed should give same results
        for i in 0..100 {
            assert_eq!(sampler1.should_sample(i), sampler2.should_sample(i));
        }
    }
}
