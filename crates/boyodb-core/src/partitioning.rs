//! Advanced Table Partitioning
//!
//! Provides range, list, and hash partitioning strategies for large tables.
//! Supports automatic partition management, pruning, and maintenance.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Partitioning error types
#[derive(Debug, Clone)]
pub enum PartitionError {
    /// Partition not found
    NotFound(String),
    /// Partition already exists
    AlreadyExists(String),
    /// Invalid partition specification
    InvalidSpec(String),
    /// Partition boundary violation
    BoundaryViolation(String),
    /// Cannot drop default partition with data
    DefaultPartitionNotEmpty,
    /// Value doesn't match any partition
    NoMatchingPartition(String),
    /// Partition is being modified
    PartitionBusy(String),
    /// Invalid partition key
    InvalidKey(String),
    /// Subpartition error
    SubpartitionError(String),
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(s) => write!(f, "partition not found: {}", s),
            Self::AlreadyExists(s) => write!(f, "partition already exists: {}", s),
            Self::InvalidSpec(s) => write!(f, "invalid partition spec: {}", s),
            Self::BoundaryViolation(s) => write!(f, "boundary violation: {}", s),
            Self::DefaultPartitionNotEmpty => write!(f, "default partition contains data"),
            Self::NoMatchingPartition(s) => write!(f, "no matching partition for: {}", s),
            Self::PartitionBusy(s) => write!(f, "partition is busy: {}", s),
            Self::InvalidKey(s) => write!(f, "invalid partition key: {}", s),
            Self::SubpartitionError(s) => write!(f, "subpartition error: {}", s),
        }
    }
}

impl std::error::Error for PartitionError {}

/// Partition key value for routing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(u32), // bits representation for Eq/Hash
    Float64(u64), // bits representation for Eq/Hash
    String(String),
    Bytes(Vec<u8>),
    Date(i32),      // days since epoch
    Timestamp(i64), // microseconds since epoch
}

impl PartitionValue {
    pub fn from_i64(v: i64) -> Self {
        Self::Int64(v)
    }

    pub fn from_string(v: String) -> Self {
        Self::String(v)
    }

    pub fn from_f64(v: f64) -> Self {
        Self::Float64(v.to_bits())
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int8(v) => Some(*v as i64),
            Self::Int16(v) => Some(*v as i64),
            Self::Int32(v) => Some(*v as i64),
            Self::Int64(v) => Some(*v),
            Self::UInt8(v) => Some(*v as i64),
            Self::UInt16(v) => Some(*v as i64),
            Self::UInt32(v) => Some(*v as i64),
            Self::Date(v) => Some(*v as i64),
            Self::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float32(bits) => Some(f32::from_bits(*bits) as f64),
            Self::Float64(bits) => Some(f64::from_bits(*bits)),
            Self::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Compute hash for hash partitioning
    pub fn partition_hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl PartialOrd for PartitionValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => std::cmp::Ordering::Equal,
            (Self::Null, _) => std::cmp::Ordering::Less,
            (_, Self::Null) => std::cmp::Ordering::Greater,
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            (Self::Float64(a), Self::Float64(b)) => {
                let fa = f64::from_bits(*a);
                let fb = f64::from_bits(*b);
                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
            }
            _ => std::cmp::Ordering::Equal,
        }
    }
}

/// Partitioning strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Range partitioning: values fall into ranges
    Range(RangePartitionConfig),
    /// List partitioning: explicit value lists
    List(ListPartitionConfig),
    /// Hash partitioning: hash-based distribution
    Hash(HashPartitionConfig),
    /// Composite: combination of strategies (subpartitioning)
    Composite(CompositePartitionConfig),
}

/// Range partition configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangePartitionConfig {
    /// Partition key columns
    pub key_columns: Vec<String>,
    /// Range definitions
    pub ranges: Vec<RangePartition>,
    /// Default partition for values not matching any range
    pub default_partition: Option<String>,
    /// Interval for automatic partition creation
    pub interval: Option<RangeInterval>,
}

/// A single range partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangePartition {
    pub name: String,
    pub lower_bound: Option<Vec<PartitionValue>>, // None = MINVALUE
    pub upper_bound: Option<Vec<PartitionValue>>, // None = MAXVALUE
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub tablespace: Option<String>,
}

/// Interval for automatic range partition creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeInterval {
    pub value: i64,
    pub unit: IntervalUnit,
    pub start_value: PartitionValue,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IntervalUnit {
    Number,
    Day,
    Week,
    Month,
    Year,
}

/// List partition configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPartitionConfig {
    /// Partition key columns
    pub key_columns: Vec<String>,
    /// List definitions
    pub lists: Vec<ListPartition>,
    /// Default partition for values not in any list
    pub default_partition: Option<String>,
}

/// A single list partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPartition {
    pub name: String,
    pub values: Vec<Vec<PartitionValue>>, // Multiple columns possible
    pub tablespace: Option<String>,
}

/// Hash partition configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashPartitionConfig {
    /// Partition key columns
    pub key_columns: Vec<String>,
    /// Number of partitions (modulus)
    pub modulus: u32,
    /// Partition definitions (remainder -> partition)
    pub partitions: Vec<HashPartition>,
}

/// A single hash partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashPartition {
    pub name: String,
    pub remainder: u32,
    pub tablespace: Option<String>,
}

/// Composite (subpartitioned) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositePartitionConfig {
    /// Primary partitioning strategy
    pub primary: Box<PartitionStrategy>,
    /// Secondary (subpartition) strategy template
    pub secondary: Box<PartitionStrategy>,
}

/// Partition metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub name: String,
    pub table_name: String,
    pub strategy: PartitionStrategy,
    pub parent_partition: Option<String>,
    pub created_at: SystemTime,
    pub row_count: u64,
    pub size_bytes: u64,
    pub last_analyzed: Option<SystemTime>,
    pub is_attached: bool,
    pub tablespace: Option<String>,
}

/// Partition pruning context
#[derive(Debug, Clone)]
pub struct PruningContext {
    /// Predicates on partition key columns
    pub predicates: Vec<PartitionPredicate>,
    /// Columns referenced in query
    pub referenced_columns: HashSet<String>,
}

/// Predicate for partition pruning
#[derive(Debug, Clone)]
pub enum PartitionPredicate {
    Equals(String, PartitionValue),
    NotEquals(String, PartitionValue),
    LessThan(String, PartitionValue),
    LessThanOrEqual(String, PartitionValue),
    GreaterThan(String, PartitionValue),
    GreaterThanOrEqual(String, PartitionValue),
    Between(String, PartitionValue, PartitionValue),
    In(String, Vec<PartitionValue>),
    IsNull(String),
    IsNotNull(String),
}

/// Result of partition pruning
#[derive(Debug, Clone)]
pub struct PruningResult {
    /// Partitions that must be scanned
    pub included_partitions: Vec<String>,
    /// Partitions definitely excluded
    pub excluded_partitions: Vec<String>,
    /// Whether pruning was effective
    pub pruning_applied: bool,
    /// Estimated selectivity
    pub selectivity: f64,
}

/// Partition manager for a table
pub struct PartitionManager {
    /// Table name
    table_name: String,
    /// Partitioning configuration
    config: RwLock<PartitionStrategy>,
    /// Partition metadata
    partitions: RwLock<HashMap<String, PartitionMetadata>>,
    /// Partition routing cache
    routing_cache: RwLock<RoutingCache>,
    /// Statistics
    stats: RwLock<PartitionStats>,
}

/// Routing cache for fast partition lookup
struct RoutingCache {
    /// Range partition boundaries (sorted)
    range_boundaries: BTreeMap<Vec<PartitionValue>, String>,
    /// List partition value map
    list_map: HashMap<Vec<PartitionValue>, String>,
    /// Hash partition map
    hash_map: HashMap<u32, String>,
    /// Cache validity
    valid: bool,
    /// Last rebuild time
    last_rebuild: Instant,
}

impl Default for RoutingCache {
    fn default() -> Self {
        Self {
            range_boundaries: BTreeMap::new(),
            list_map: HashMap::new(),
            hash_map: HashMap::new(),
            valid: false,
            last_rebuild: Instant::now(),
        }
    }
}

/// Partition statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionStats {
    pub total_partitions: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub routing_cache_hits: u64,
    pub routing_cache_misses: u64,
    pub pruning_operations: u64,
    pub partitions_pruned: u64,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(table_name: String, strategy: PartitionStrategy) -> Self {
        Self {
            table_name,
            config: RwLock::new(strategy),
            partitions: RwLock::new(HashMap::new()),
            routing_cache: RwLock::new(RoutingCache::default()),
            stats: RwLock::new(PartitionStats::default()),
        }
    }

    /// Get partition for a row based on key values
    pub fn route_row(&self, key_values: &[PartitionValue]) -> Result<String, PartitionError> {
        let config = self.config.read();

        // Try cache first
        {
            let cache = self.routing_cache.read();
            if cache.valid {
                if let Some(partition) = self.lookup_cache(&cache, key_values, &config) {
                    self.stats.write().routing_cache_hits += 1;
                    return Ok(partition);
                }
            }
        }

        self.stats.write().routing_cache_misses += 1;

        match &*config {
            PartitionStrategy::Range(range_config) => self.route_range(key_values, range_config),
            PartitionStrategy::List(list_config) => self.route_list(key_values, list_config),
            PartitionStrategy::Hash(hash_config) => self.route_hash(key_values, hash_config),
            PartitionStrategy::Composite(composite_config) => {
                self.route_composite(key_values, composite_config)
            }
        }
    }

    fn lookup_cache(
        &self,
        cache: &RoutingCache,
        key_values: &[PartitionValue],
        config: &PartitionStrategy,
    ) -> Option<String> {
        match config {
            PartitionStrategy::Hash(hash_config) => {
                let hash = self.compute_hash(key_values);
                let remainder = (hash % hash_config.modulus as u64) as u32;
                cache.hash_map.get(&remainder).cloned()
            }
            PartitionStrategy::List(_) => cache.list_map.get(key_values).cloned(),
            PartitionStrategy::Range(_) => {
                // Find the partition whose upper bound is > key_values
                for (bound, partition) in cache.range_boundaries.iter() {
                    if key_values < bound {
                        return Some(partition.clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn route_range(
        &self,
        key_values: &[PartitionValue],
        config: &RangePartitionConfig,
    ) -> Result<String, PartitionError> {
        for range in &config.ranges {
            let in_lower = match &range.lower_bound {
                None => true, // MINVALUE
                Some(lower) => {
                    if range.lower_inclusive {
                        key_values >= lower.as_slice()
                    } else {
                        key_values > lower.as_slice()
                    }
                }
            };

            let in_upper = match &range.upper_bound {
                None => true, // MAXVALUE
                Some(upper) => {
                    if range.upper_inclusive {
                        key_values <= upper.as_slice()
                    } else {
                        key_values < upper.as_slice()
                    }
                }
            };

            if in_lower && in_upper {
                return Ok(range.name.clone());
            }
        }

        // Check default partition
        if let Some(default) = &config.default_partition {
            return Ok(default.clone());
        }

        Err(PartitionError::NoMatchingPartition(format!(
            "no partition for key values: {:?}",
            key_values
        )))
    }

    fn route_list(
        &self,
        key_values: &[PartitionValue],
        config: &ListPartitionConfig,
    ) -> Result<String, PartitionError> {
        for list in &config.lists {
            for values in &list.values {
                if key_values == values.as_slice() {
                    return Ok(list.name.clone());
                }
            }
        }

        if let Some(default) = &config.default_partition {
            return Ok(default.clone());
        }

        Err(PartitionError::NoMatchingPartition(format!(
            "no partition for values: {:?}",
            key_values
        )))
    }

    fn route_hash(
        &self,
        key_values: &[PartitionValue],
        config: &HashPartitionConfig,
    ) -> Result<String, PartitionError> {
        let hash = self.compute_hash(key_values);
        let remainder = (hash % config.modulus as u64) as u32;

        for partition in &config.partitions {
            if partition.remainder == remainder {
                return Ok(partition.name.clone());
            }
        }

        Err(PartitionError::NoMatchingPartition(format!(
            "no partition for hash remainder {}",
            remainder
        )))
    }

    fn route_composite(
        &self,
        key_values: &[PartitionValue],
        config: &CompositePartitionConfig,
    ) -> Result<String, PartitionError> {
        // First, route using primary strategy
        let primary_key_len = match &*config.primary {
            PartitionStrategy::Range(r) => r.key_columns.len(),
            PartitionStrategy::List(l) => l.key_columns.len(),
            PartitionStrategy::Hash(h) => h.key_columns.len(),
            PartitionStrategy::Composite(_) => {
                return Err(PartitionError::InvalidSpec(
                    "nested composite not supported".into(),
                ));
            }
        };

        if key_values.len() < primary_key_len {
            return Err(PartitionError::InvalidKey(
                "insufficient key values for composite partition".into(),
            ));
        }

        let primary_keys = &key_values[..primary_key_len];
        let secondary_keys = &key_values[primary_key_len..];

        let primary_partition = match &*config.primary {
            PartitionStrategy::Range(r) => self.route_range(primary_keys, r)?,
            PartitionStrategy::List(l) => self.route_list(primary_keys, l)?,
            PartitionStrategy::Hash(h) => self.route_hash(primary_keys, h)?,
            _ => unreachable!(),
        };

        let secondary_partition = match &*config.secondary {
            PartitionStrategy::Range(r) => self.route_range(secondary_keys, r)?,
            PartitionStrategy::List(l) => self.route_list(secondary_keys, l)?,
            PartitionStrategy::Hash(h) => self.route_hash(secondary_keys, h)?,
            _ => {
                return Err(PartitionError::SubpartitionError(
                    "invalid subpartition strategy".into(),
                ));
            }
        };

        Ok(format!("{}_{}", primary_partition, secondary_partition))
    }

    fn compute_hash(&self, key_values: &[PartitionValue]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        for value in key_values {
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Apply partition pruning
    pub fn prune(&self, context: &PruningContext) -> PruningResult {
        let config = self.config.read();
        let partitions = self.partitions.read();

        let all_partitions: Vec<String> = partitions.keys().cloned().collect();

        if context.predicates.is_empty() {
            return PruningResult {
                included_partitions: all_partitions.clone(),
                excluded_partitions: vec![],
                pruning_applied: false,
                selectivity: 1.0,
            };
        }

        let mut stats = self.stats.write();
        stats.pruning_operations += 1;

        let result = match &*config {
            PartitionStrategy::Range(range_config) => {
                self.prune_range(context, range_config, &all_partitions)
            }
            PartitionStrategy::List(list_config) => {
                self.prune_list(context, list_config, &all_partitions)
            }
            PartitionStrategy::Hash(hash_config) => {
                self.prune_hash(context, hash_config, &all_partitions)
            }
            PartitionStrategy::Composite(composite_config) => {
                self.prune_composite(context, composite_config, &all_partitions)
            }
        };

        stats.partitions_pruned += result.excluded_partitions.len() as u64;
        result
    }

    fn prune_range(
        &self,
        context: &PruningContext,
        config: &RangePartitionConfig,
        all_partitions: &[String],
    ) -> PruningResult {
        let key_col = config.key_columns.first().map(|s| s.as_str()).unwrap_or("");

        let mut included = Vec::new();
        let mut excluded = Vec::new();

        for range in &config.ranges {
            let mut should_include = true;

            for predicate in &context.predicates {
                match predicate {
                    PartitionPredicate::Equals(col, val) if col == key_col => {
                        // Check if value is in this range
                        let in_range = self.value_in_range(val, range);
                        if !in_range {
                            should_include = false;
                            break;
                        }
                    }
                    PartitionPredicate::LessThan(col, val) if col == key_col => {
                        // If range lower bound >= val, exclude
                        if let Some(lower) = &range.lower_bound {
                            if let Some(lower_val) = lower.first() {
                                if lower_val >= val {
                                    should_include = false;
                                    break;
                                }
                            }
                        }
                    }
                    PartitionPredicate::GreaterThan(col, val) if col == key_col => {
                        // If range upper bound <= val, exclude
                        if let Some(upper) = &range.upper_bound {
                            if let Some(upper_val) = upper.first() {
                                if upper_val <= val {
                                    should_include = false;
                                    break;
                                }
                            }
                        }
                    }
                    PartitionPredicate::Between(col, low, high) if col == key_col => {
                        // Check range overlap
                        let overlaps = self.range_overlaps(range, low, high);
                        if !overlaps {
                            should_include = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if should_include {
                included.push(range.name.clone());
            } else {
                excluded.push(range.name.clone());
            }
        }

        // Include default partition if it exists
        if let Some(default) = &config.default_partition {
            if all_partitions.contains(default) {
                included.push(default.clone());
            }
        }

        let selectivity = if all_partitions.is_empty() {
            1.0
        } else {
            included.len() as f64 / all_partitions.len() as f64
        };

        let pruning_applied = !excluded.is_empty();
        PruningResult {
            included_partitions: included,
            excluded_partitions: excluded,
            pruning_applied,
            selectivity,
        }
    }

    fn value_in_range(&self, val: &PartitionValue, range: &RangePartition) -> bool {
        let in_lower = match &range.lower_bound {
            None => true,
            Some(lower) => {
                if let Some(lower_val) = lower.first() {
                    if range.lower_inclusive {
                        val >= lower_val
                    } else {
                        val > lower_val
                    }
                } else {
                    true
                }
            }
        };

        let in_upper = match &range.upper_bound {
            None => true,
            Some(upper) => {
                if let Some(upper_val) = upper.first() {
                    if range.upper_inclusive {
                        val <= upper_val
                    } else {
                        val < upper_val
                    }
                } else {
                    true
                }
            }
        };

        in_lower && in_upper
    }

    fn range_overlaps(
        &self,
        range: &RangePartition,
        low: &PartitionValue,
        high: &PartitionValue,
    ) -> bool {
        // Check if [low, high] overlaps with range
        let range_start = range.lower_bound.as_ref().and_then(|v| v.first());
        let range_end = range.upper_bound.as_ref().and_then(|v| v.first());

        match (range_start, range_end) {
            (Some(rs), Some(re)) => {
                // Overlap: NOT (high < rs OR low > re)
                !(high < rs || low > re)
            }
            (Some(rs), None) => high >= rs,
            (None, Some(re)) => low <= re,
            (None, None) => true,
        }
    }

    fn prune_list(
        &self,
        context: &PruningContext,
        config: &ListPartitionConfig,
        all_partitions: &[String],
    ) -> PruningResult {
        let key_col = config.key_columns.first().map(|s| s.as_str()).unwrap_or("");

        let mut included = Vec::new();
        let mut excluded = Vec::new();

        for list in &config.lists {
            let mut should_include = false;

            for predicate in &context.predicates {
                match predicate {
                    PartitionPredicate::Equals(col, val) if col == key_col => {
                        // Check if value is in this list
                        for list_vals in &list.values {
                            if let Some(list_val) = list_vals.first() {
                                if list_val == val {
                                    should_include = true;
                                    break;
                                }
                            }
                        }
                    }
                    PartitionPredicate::In(col, vals) if col == key_col => {
                        // Check if any IN value is in this list
                        for val in vals {
                            for list_vals in &list.values {
                                if let Some(list_val) = list_vals.first() {
                                    if list_val == val {
                                        should_include = true;
                                        break;
                                    }
                                }
                            }
                            if should_include {
                                break;
                            }
                        }
                    }
                    _ => {
                        // Cannot prune, include partition
                        should_include = true;
                    }
                }
            }

            if should_include {
                included.push(list.name.clone());
            } else {
                excluded.push(list.name.clone());
            }
        }

        // Include default partition
        if let Some(default) = &config.default_partition {
            if all_partitions.contains(default) {
                included.push(default.clone());
            }
        }

        let selectivity = if all_partitions.is_empty() {
            1.0
        } else {
            included.len() as f64 / all_partitions.len() as f64
        };

        let pruning_applied = !excluded.is_empty();
        PruningResult {
            included_partitions: included,
            excluded_partitions: excluded,
            pruning_applied,
            selectivity,
        }
    }

    fn prune_hash(
        &self,
        context: &PruningContext,
        config: &HashPartitionConfig,
        all_partitions: &[String],
    ) -> PruningResult {
        let key_col = config.key_columns.first().map(|s| s.as_str()).unwrap_or("");

        let mut target_remainders: Option<HashSet<u32>> = None;

        for predicate in &context.predicates {
            match predicate {
                PartitionPredicate::Equals(col, val) if col == key_col => {
                    let hash = val.partition_hash();
                    let remainder = (hash % config.modulus as u64) as u32;
                    target_remainders = Some([remainder].into_iter().collect());
                    break;
                }
                PartitionPredicate::In(col, vals) if col == key_col => {
                    let remainders: HashSet<u32> = vals
                        .iter()
                        .map(|v| (v.partition_hash() % config.modulus as u64) as u32)
                        .collect();
                    target_remainders = Some(remainders);
                    break;
                }
                _ => {}
            }
        }

        let (included, excluded) = match target_remainders {
            Some(remainders) => {
                let mut inc = Vec::new();
                let mut exc = Vec::new();
                for partition in &config.partitions {
                    if remainders.contains(&partition.remainder) {
                        inc.push(partition.name.clone());
                    } else {
                        exc.push(partition.name.clone());
                    }
                }
                (inc, exc)
            }
            None => (all_partitions.to_vec(), vec![]),
        };

        let selectivity = if all_partitions.is_empty() {
            1.0
        } else {
            included.len() as f64 / all_partitions.len() as f64
        };

        let pruning_applied = !excluded.is_empty();
        PruningResult {
            included_partitions: included,
            excluded_partitions: excluded,
            pruning_applied,
            selectivity,
        }
    }

    fn prune_composite(
        &self,
        context: &PruningContext,
        config: &CompositePartitionConfig,
        all_partitions: &[String],
    ) -> PruningResult {
        // Prune primary partitions first, then subpartitions
        // For simplicity, return primary result
        // Full implementation would also prune subpartitions
        match &*config.primary {
            PartitionStrategy::Range(r) => self.prune_range(context, r, all_partitions),
            PartitionStrategy::List(l) => self.prune_list(context, l, all_partitions),
            PartitionStrategy::Hash(h) => self.prune_hash(context, h, all_partitions),
            _ => PruningResult {
                included_partitions: all_partitions.to_vec(),
                excluded_partitions: vec![],
                pruning_applied: false,
                selectivity: 1.0,
            },
        }
    }

    /// Add a new partition
    pub fn add_partition(&self, partition: PartitionMetadata) -> Result<(), PartitionError> {
        let mut partitions = self.partitions.write();

        if partitions.contains_key(&partition.name) {
            return Err(PartitionError::AlreadyExists(partition.name));
        }

        partitions.insert(partition.name.clone(), partition);

        // Invalidate routing cache
        self.routing_cache.write().valid = false;

        self.stats.write().total_partitions = partitions.len();

        Ok(())
    }

    /// Drop a partition
    pub fn drop_partition(&self, name: &str) -> Result<PartitionMetadata, PartitionError> {
        let mut partitions = self.partitions.write();

        let partition = partitions
            .remove(name)
            .ok_or_else(|| PartitionError::NotFound(name.to_string()))?;

        // Invalidate routing cache
        self.routing_cache.write().valid = false;

        self.stats.write().total_partitions = partitions.len();

        Ok(partition)
    }

    /// Attach an existing table as a partition
    pub fn attach_partition(
        &self,
        name: &str,
        constraint: PartitionConstraint,
    ) -> Result<(), PartitionError> {
        let mut partitions = self.partitions.write();

        if let Some(partition) = partitions.get_mut(name) {
            partition.is_attached = true;
            // Validate constraint matches partition definition
            self.validate_constraint(&constraint)?;
            Ok(())
        } else {
            Err(PartitionError::NotFound(name.to_string()))
        }
    }

    /// Detach a partition (makes it a standalone table)
    pub fn detach_partition(&self, name: &str) -> Result<(), PartitionError> {
        let mut partitions = self.partitions.write();

        if let Some(partition) = partitions.get_mut(name) {
            partition.is_attached = false;
            Ok(())
        } else {
            Err(PartitionError::NotFound(name.to_string()))
        }
    }

    fn validate_constraint(&self, _constraint: &PartitionConstraint) -> Result<(), PartitionError> {
        // Validate that constraint matches partition strategy
        Ok(())
    }

    /// Split a partition into two
    pub fn split_partition(
        &self,
        name: &str,
        split_point: Vec<PartitionValue>,
        new_partition_name: String,
    ) -> Result<(), PartitionError> {
        let mut config = self.config.write();

        match &mut *config {
            PartitionStrategy::Range(range_config) => {
                let idx = range_config
                    .ranges
                    .iter()
                    .position(|r| r.name == name)
                    .ok_or_else(|| PartitionError::NotFound(name.to_string()))?;

                let original = range_config.ranges[idx].clone();

                // Create two new ranges
                let lower_range = RangePartition {
                    name: name.to_string(),
                    lower_bound: original.lower_bound,
                    upper_bound: Some(split_point.clone()),
                    lower_inclusive: original.lower_inclusive,
                    upper_inclusive: false,
                    tablespace: original.tablespace.clone(),
                };

                let upper_range = RangePartition {
                    name: new_partition_name,
                    lower_bound: Some(split_point),
                    upper_bound: original.upper_bound,
                    lower_inclusive: true,
                    upper_inclusive: original.upper_inclusive,
                    tablespace: original.tablespace,
                };

                range_config.ranges[idx] = lower_range;
                range_config.ranges.insert(idx + 1, upper_range);
            }
            _ => {
                return Err(PartitionError::InvalidSpec(
                    "split only supported for range partitions".into(),
                ));
            }
        }

        // Invalidate cache
        self.routing_cache.write().valid = false;

        Ok(())
    }

    /// Merge two adjacent partitions
    pub fn merge_partitions(
        &self,
        partition1: &str,
        partition2: &str,
        merged_name: String,
    ) -> Result<(), PartitionError> {
        let mut config = self.config.write();

        match &mut *config {
            PartitionStrategy::Range(range_config) => {
                let idx1 = range_config
                    .ranges
                    .iter()
                    .position(|r| r.name == partition1)
                    .ok_or_else(|| PartitionError::NotFound(partition1.to_string()))?;

                let idx2 = range_config
                    .ranges
                    .iter()
                    .position(|r| r.name == partition2)
                    .ok_or_else(|| PartitionError::NotFound(partition2.to_string()))?;

                // Must be adjacent
                if idx2 != idx1 + 1 {
                    return Err(PartitionError::InvalidSpec(
                        "partitions must be adjacent".into(),
                    ));
                }

                let p1 = &range_config.ranges[idx1];
                let p2 = &range_config.ranges[idx2];

                let merged = RangePartition {
                    name: merged_name,
                    lower_bound: p1.lower_bound.clone(),
                    upper_bound: p2.upper_bound.clone(),
                    lower_inclusive: p1.lower_inclusive,
                    upper_inclusive: p2.upper_inclusive,
                    tablespace: p1.tablespace.clone(),
                };

                range_config.ranges.remove(idx2);
                range_config.ranges[idx1] = merged;
            }
            _ => {
                return Err(PartitionError::InvalidSpec(
                    "merge only supported for range partitions".into(),
                ));
            }
        }

        self.routing_cache.write().valid = false;

        Ok(())
    }

    /// Rebuild routing cache
    pub fn rebuild_cache(&self) {
        let config = self.config.read();
        let mut cache = self.routing_cache.write();

        cache.range_boundaries.clear();
        cache.list_map.clear();
        cache.hash_map.clear();

        match &*config {
            PartitionStrategy::Range(range_config) => {
                for range in &range_config.ranges {
                    if let Some(upper) = &range.upper_bound {
                        cache
                            .range_boundaries
                            .insert(upper.clone(), range.name.clone());
                    }
                }
            }
            PartitionStrategy::List(list_config) => {
                for list in &list_config.lists {
                    for values in &list.values {
                        cache.list_map.insert(values.clone(), list.name.clone());
                    }
                }
            }
            PartitionStrategy::Hash(hash_config) => {
                for partition in &hash_config.partitions {
                    cache
                        .hash_map
                        .insert(partition.remainder, partition.name.clone());
                }
            }
            _ => {}
        }

        cache.valid = true;
        cache.last_rebuild = Instant::now();
    }

    /// Get partition statistics
    pub fn stats(&self) -> PartitionStats {
        self.stats.read().clone()
    }

    /// List all partitions
    pub fn list_partitions(&self) -> Vec<PartitionMetadata> {
        self.partitions.read().values().cloned().collect()
    }

    /// Get partition by name
    pub fn get_partition(&self, name: &str) -> Option<PartitionMetadata> {
        self.partitions.read().get(name).cloned()
    }

    /// Update partition statistics
    pub fn update_partition_stats(&self, name: &str, row_count: u64, size_bytes: u64) {
        let mut partitions = self.partitions.write();
        if let Some(partition) = partitions.get_mut(name) {
            partition.row_count = row_count;
            partition.size_bytes = size_bytes;
            partition.last_analyzed = Some(SystemTime::now());
        }

        // Update totals
        let mut stats = self.stats.write();
        stats.total_rows = partitions.values().map(|p| p.row_count).sum();
        stats.total_size_bytes = partitions.values().map(|p| p.size_bytes).sum();
    }
}

/// Partition constraint for ATTACH
#[derive(Debug, Clone)]
pub enum PartitionConstraint {
    Range {
        lower: Option<Vec<PartitionValue>>,
        upper: Option<Vec<PartitionValue>>,
    },
    List {
        values: Vec<Vec<PartitionValue>>,
    },
    Hash {
        modulus: u32,
        remainder: u32,
    },
}

/// Automatic partition maintenance
pub struct PartitionMaintenance {
    /// Managers by table
    managers: RwLock<HashMap<String, Arc<PartitionManager>>>,
    /// Maintenance config
    config: MaintenanceConfig,
}

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Auto-create partitions ahead of time
    pub pre_create_partitions: bool,
    /// How many partitions to pre-create
    pub pre_create_count: usize,
    /// Auto-drop old partitions
    pub auto_drop_old: bool,
    /// Retention period for auto-drop
    pub retention: Option<Duration>,
    /// Check interval
    pub check_interval: Duration,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            pre_create_partitions: true,
            pre_create_count: 3,
            auto_drop_old: false,
            retention: None,
            check_interval: Duration::from_secs(3600),
        }
    }
}

impl PartitionMaintenance {
    pub fn new(config: MaintenanceConfig) -> Self {
        Self {
            managers: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Register a partition manager
    pub fn register(&self, table: String, manager: Arc<PartitionManager>) {
        self.managers.write().insert(table, manager);
    }

    /// Run maintenance for all registered tables
    pub fn run_maintenance(&self) -> Vec<MaintenanceAction> {
        let managers = self.managers.read();
        let mut actions = Vec::new();

        for (table, manager) in managers.iter() {
            // Check for pre-creation
            if self.config.pre_create_partitions {
                if let Some(action) = self.check_pre_create(table, manager) {
                    actions.push(action);
                }
            }

            // Check for auto-drop
            if self.config.auto_drop_old {
                if let Some(action) = self.check_auto_drop(table, manager) {
                    actions.push(action);
                }
            }
        }

        actions
    }

    fn check_pre_create(
        &self,
        table: &str,
        _manager: &PartitionManager,
    ) -> Option<MaintenanceAction> {
        // Implementation would check interval partitions and pre-create
        Some(MaintenanceAction::PreCreate {
            table: table.to_string(),
            partitions: vec![],
        })
    }

    fn check_auto_drop(
        &self,
        table: &str,
        manager: &PartitionManager,
    ) -> Option<MaintenanceAction> {
        let retention = self.config.retention?;
        let now = SystemTime::now();
        let mut to_drop = Vec::new();

        for partition in manager.list_partitions() {
            if let Ok(age) = now.duration_since(partition.created_at) {
                if age > retention {
                    to_drop.push(partition.name);
                }
            }
        }

        if to_drop.is_empty() {
            None
        } else {
            Some(MaintenanceAction::Drop {
                table: table.to_string(),
                partitions: to_drop,
            })
        }
    }
}

/// Maintenance action
#[derive(Debug, Clone)]
pub enum MaintenanceAction {
    PreCreate {
        table: String,
        partitions: Vec<String>,
    },
    Drop {
        table: String,
        partitions: Vec<String>,
    },
    Merge {
        table: String,
        partition1: String,
        partition2: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_partitioning() {
        let config = PartitionStrategy::Range(RangePartitionConfig {
            key_columns: vec!["year".to_string()],
            ranges: vec![
                RangePartition {
                    name: "p2022".to_string(),
                    lower_bound: Some(vec![PartitionValue::Int64(2022)]),
                    upper_bound: Some(vec![PartitionValue::Int64(2023)]),
                    lower_inclusive: true,
                    upper_inclusive: false,
                    tablespace: None,
                },
                RangePartition {
                    name: "p2023".to_string(),
                    lower_bound: Some(vec![PartitionValue::Int64(2023)]),
                    upper_bound: Some(vec![PartitionValue::Int64(2024)]),
                    lower_inclusive: true,
                    upper_inclusive: false,
                    tablespace: None,
                },
                RangePartition {
                    name: "p2024".to_string(),
                    lower_bound: Some(vec![PartitionValue::Int64(2024)]),
                    upper_bound: Some(vec![PartitionValue::Int64(2025)]),
                    lower_inclusive: true,
                    upper_inclusive: false,
                    tablespace: None,
                },
            ],
            default_partition: Some("p_default".to_string()),
            interval: None,
        });

        let manager = PartitionManager::new("test_table".to_string(), config);

        // Route to correct partition
        let partition = manager.route_row(&[PartitionValue::Int64(2023)]).unwrap();
        assert_eq!(partition, "p2023");

        let partition = manager.route_row(&[PartitionValue::Int64(2022)]).unwrap();
        assert_eq!(partition, "p2022");

        // Route to default
        let partition = manager.route_row(&[PartitionValue::Int64(2025)]).unwrap();
        assert_eq!(partition, "p_default");
    }

    #[test]
    fn test_list_partitioning() {
        let config = PartitionStrategy::List(ListPartitionConfig {
            key_columns: vec!["region".to_string()],
            lists: vec![
                ListPartition {
                    name: "p_americas".to_string(),
                    values: vec![
                        vec![PartitionValue::String("US".to_string())],
                        vec![PartitionValue::String("CA".to_string())],
                        vec![PartitionValue::String("MX".to_string())],
                    ],
                    tablespace: None,
                },
                ListPartition {
                    name: "p_europe".to_string(),
                    values: vec![
                        vec![PartitionValue::String("UK".to_string())],
                        vec![PartitionValue::String("DE".to_string())],
                        vec![PartitionValue::String("FR".to_string())],
                    ],
                    tablespace: None,
                },
            ],
            default_partition: Some("p_other".to_string()),
        });

        let manager = PartitionManager::new("test_table".to_string(), config);

        let partition = manager
            .route_row(&[PartitionValue::String("US".to_string())])
            .unwrap();
        assert_eq!(partition, "p_americas");

        let partition = manager
            .route_row(&[PartitionValue::String("DE".to_string())])
            .unwrap();
        assert_eq!(partition, "p_europe");

        let partition = manager
            .route_row(&[PartitionValue::String("JP".to_string())])
            .unwrap();
        assert_eq!(partition, "p_other");
    }

    #[test]
    fn test_hash_partitioning() {
        let config = PartitionStrategy::Hash(HashPartitionConfig {
            key_columns: vec!["user_id".to_string()],
            modulus: 4,
            partitions: vec![
                HashPartition {
                    name: "p0".to_string(),
                    remainder: 0,
                    tablespace: None,
                },
                HashPartition {
                    name: "p1".to_string(),
                    remainder: 1,
                    tablespace: None,
                },
                HashPartition {
                    name: "p2".to_string(),
                    remainder: 2,
                    tablespace: None,
                },
                HashPartition {
                    name: "p3".to_string(),
                    remainder: 3,
                    tablespace: None,
                },
            ],
        });

        let manager = PartitionManager::new("test_table".to_string(), config);

        // All values should route to some partition
        for i in 0..100 {
            let result = manager.route_row(&[PartitionValue::Int64(i)]);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_partition_pruning() {
        let config = PartitionStrategy::Range(RangePartitionConfig {
            key_columns: vec!["year".to_string()],
            ranges: vec![
                RangePartition {
                    name: "p2022".to_string(),
                    lower_bound: Some(vec![PartitionValue::Int64(2022)]),
                    upper_bound: Some(vec![PartitionValue::Int64(2023)]),
                    lower_inclusive: true,
                    upper_inclusive: false,
                    tablespace: None,
                },
                RangePartition {
                    name: "p2023".to_string(),
                    lower_bound: Some(vec![PartitionValue::Int64(2023)]),
                    upper_bound: Some(vec![PartitionValue::Int64(2024)]),
                    lower_inclusive: true,
                    upper_inclusive: false,
                    tablespace: None,
                },
            ],
            default_partition: None,
            interval: None,
        });

        let manager = PartitionManager::new("test_table".to_string(), config);

        // Add partition metadata
        manager
            .add_partition(PartitionMetadata {
                name: "p2022".to_string(),
                table_name: "test_table".to_string(),
                strategy: PartitionStrategy::Range(RangePartitionConfig {
                    key_columns: vec![],
                    ranges: vec![],
                    default_partition: None,
                    interval: None,
                }),
                parent_partition: None,
                created_at: SystemTime::now(),
                row_count: 1000,
                size_bytes: 10000,
                last_analyzed: None,
                is_attached: true,
                tablespace: None,
            })
            .unwrap();

        manager
            .add_partition(PartitionMetadata {
                name: "p2023".to_string(),
                table_name: "test_table".to_string(),
                strategy: PartitionStrategy::Range(RangePartitionConfig {
                    key_columns: vec![],
                    ranges: vec![],
                    default_partition: None,
                    interval: None,
                }),
                parent_partition: None,
                created_at: SystemTime::now(),
                row_count: 2000,
                size_bytes: 20000,
                last_analyzed: None,
                is_attached: true,
                tablespace: None,
            })
            .unwrap();

        // Prune with equals predicate
        let context = PruningContext {
            predicates: vec![PartitionPredicate::Equals(
                "year".to_string(),
                PartitionValue::Int64(2023),
            )],
            referenced_columns: ["year".to_string()].into_iter().collect(),
        };

        let result = manager.prune(&context);
        assert!(result.pruning_applied);
        assert_eq!(result.included_partitions.len(), 1);
        assert!(result.included_partitions.contains(&"p2023".to_string()));
    }
}
