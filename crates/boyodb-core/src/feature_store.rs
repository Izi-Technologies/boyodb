//! Feature Store for BoyoDB
//!
//! Manages feature engineering pipelines with:
//! - Versioned feature sets and feature groups
//! - Point-in-time feature lookups for training data
//! - Online/offline feature serving
//! - Feature lineage and metadata tracking

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Feature data types
#[derive(Debug, Clone, PartialEq)]
pub enum FeatureDataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Boolean,
    Timestamp,
    Array(Box<FeatureDataType>),
    Embedding(usize), // dimension
}

/// Feature value
#[derive(Debug, Clone)]
pub enum FeatureValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Boolean(bool),
    Timestamp(i64),
    Array(Vec<FeatureValue>),
    Embedding(Vec<f32>),
    Null,
}

impl FeatureValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeatureValue::Int32(v) => Some(*v as f64),
            FeatureValue::Int64(v) => Some(*v as f64),
            FeatureValue::Float32(v) => Some(*v as f64),
            FeatureValue::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

/// Feature definition
#[derive(Debug, Clone)]
pub struct FeatureDefinition {
    /// Feature name
    pub name: String,
    /// Data type
    pub dtype: FeatureDataType,
    /// Description
    pub description: String,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Owner/team
    pub owner: String,
    /// Creation timestamp
    pub created_at: i64,
    /// Whether feature is deprecated
    pub deprecated: bool,
    /// Default value if missing
    pub default_value: Option<FeatureValue>,
    /// Transformation SQL or expression
    pub transformation: Option<String>,
}

/// Feature group - collection of related features
#[derive(Debug, Clone)]
pub struct FeatureGroup {
    /// Group name
    pub name: String,
    /// Version number
    pub version: u32,
    /// Description
    pub description: String,
    /// Entity key columns (e.g., user_id, product_id)
    pub entity_keys: Vec<String>,
    /// Event timestamp column for point-in-time joins
    pub event_timestamp_column: Option<String>,
    /// Features in this group
    pub features: Vec<FeatureDefinition>,
    /// Source table or query
    pub source: FeatureSource,
    /// Online serving enabled
    pub online_enabled: bool,
    /// Offline serving enabled
    pub offline_enabled: bool,
    /// TTL for online features (seconds)
    pub online_ttl_seconds: Option<u64>,
    /// Creation timestamp
    pub created_at: i64,
    /// Last updated timestamp
    pub updated_at: i64,
    /// Tags
    pub tags: Vec<String>,
}

/// Feature source definition
#[derive(Debug, Clone)]
pub enum FeatureSource {
    /// From a database table
    Table {
        database: String,
        table: String,
    },
    /// From a SQL query
    Query {
        sql: String,
    },
    /// From a streaming source
    Stream {
        topic: String,
        format: String,
    },
    /// From an external API
    External {
        endpoint: String,
        auth_type: String,
    },
}

/// Feature view - a subset of features for a specific use case
#[derive(Debug, Clone)]
pub struct FeatureView {
    /// View name
    pub name: String,
    /// Version
    pub version: u32,
    /// Description
    pub description: String,
    /// Feature references (group_name.feature_name)
    pub features: Vec<String>,
    /// Entity keys
    pub entity_keys: Vec<String>,
    /// TTL for cached features
    pub ttl_seconds: Option<u64>,
    /// Creation timestamp
    pub created_at: i64,
}

/// Point-in-time join request
#[derive(Debug, Clone)]
pub struct PointInTimeRequest {
    /// Entity DataFrame with entity keys and timestamps
    pub entities: Vec<EntityRow>,
    /// Feature view or group name
    pub feature_source: String,
    /// Features to retrieve
    pub features: Vec<String>,
    /// How to handle missing features
    pub missing_strategy: MissingStrategy,
}

/// Entity row for point-in-time join
#[derive(Debug, Clone)]
pub struct EntityRow {
    /// Entity key values
    pub keys: HashMap<String, FeatureValue>,
    /// Event timestamp for point-in-time lookup
    pub event_timestamp: i64,
}

/// Strategy for handling missing features
#[derive(Debug, Clone, Copy)]
pub enum MissingStrategy {
    /// Return null for missing features
    Null,
    /// Use default value from feature definition
    Default,
    /// Fill with last known value
    FillForward,
    /// Raise error on missing
    Error,
}

/// Feature vector result
#[derive(Debug, Clone)]
pub struct FeatureVector {
    /// Entity keys
    pub entity_keys: HashMap<String, FeatureValue>,
    /// Feature values
    pub features: HashMap<String, FeatureValue>,
    /// Timestamp of feature computation
    pub feature_timestamp: i64,
    /// Request timestamp
    pub request_timestamp: i64,
}

/// Online feature store (low-latency serving)
pub struct OnlineStore {
    /// Feature cache: entity_key -> feature_name -> (value, timestamp)
    cache: RwLock<HashMap<String, HashMap<String, (FeatureValue, i64)>>>,
    /// TTL for cached values
    default_ttl_ms: u64,
    /// Stats
    stats: OnlineStoreStats,
}

struct OnlineStoreStats {
    hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
}

impl OnlineStore {
    pub fn new(default_ttl_ms: u64) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            default_ttl_ms,
            stats: OnlineStoreStats {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                writes: AtomicU64::new(0),
            },
        }
    }

    /// Get features for an entity
    pub fn get_features(
        &self,
        entity_key: &str,
        feature_names: &[String],
    ) -> HashMap<String, Option<FeatureValue>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let cache = self.cache.read();
        let mut result = HashMap::new();

        if let Some(entity_features) = cache.get(entity_key) {
            for name in feature_names {
                if let Some((value, timestamp)) = entity_features.get(name) {
                    if now - timestamp < self.default_ttl_ms as i64 {
                        result.insert(name.clone(), Some(value.clone()));
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        result.insert(name.clone(), None);
                        self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    result.insert(name.clone(), None);
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            for name in feature_names {
                result.insert(name.clone(), None);
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    /// Write features for an entity
    pub fn write_features(&self, entity_key: &str, features: HashMap<String, FeatureValue>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut cache = self.cache.write();
        let entity_features = cache.entry(entity_key.to_string()).or_insert_with(HashMap::new);

        for (name, value) in features {
            entity_features.insert(name, (value, now));
            self.stats.writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.hits.load(Ordering::Relaxed),
            self.stats.misses.load(Ordering::Relaxed),
            self.stats.writes.load(Ordering::Relaxed),
        )
    }

    /// Clear expired entries
    pub fn evict_expired(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut cache = self.cache.write();
        for entity_features in cache.values_mut() {
            entity_features.retain(|_, (_, ts)| now - *ts < self.default_ttl_ms as i64);
        }
        cache.retain(|_, v| !v.is_empty());
    }
}

/// Feature transformation types
#[derive(Debug, Clone)]
pub enum Transformation {
    /// No transformation
    Identity,
    /// Z-score normalization
    StandardScaler { mean: f64, std: f64 },
    /// Min-max scaling
    MinMaxScaler { min: f64, max: f64 },
    /// Log transformation
    Log { base: f64 },
    /// Bucketize into bins
    Bucketize { boundaries: Vec<f64> },
    /// One-hot encoding
    OneHot { categories: Vec<String> },
    /// Hash encoding
    HashBucket { num_buckets: usize },
    /// Custom SQL expression
    SqlExpression { expression: String },
    /// Embedding lookup
    EmbeddingLookup { table: String, key_column: String },
    /// Time-based features
    TimeFeatures { extract: Vec<TimeFeature> },
}

/// Time features to extract
#[derive(Debug, Clone, Copy)]
pub enum TimeFeature {
    Year,
    Month,
    Day,
    DayOfWeek,
    Hour,
    Minute,
    Quarter,
    WeekOfYear,
    IsWeekend,
}

/// Feature engineering pipeline
#[derive(Debug, Clone)]
pub struct FeaturePipeline {
    /// Pipeline name
    pub name: String,
    /// Input features
    pub inputs: Vec<String>,
    /// Transformation steps
    pub steps: Vec<PipelineStep>,
    /// Output feature names
    pub outputs: Vec<String>,
}

/// Pipeline step
#[derive(Debug, Clone)]
pub struct PipelineStep {
    /// Step name
    pub name: String,
    /// Input columns
    pub inputs: Vec<String>,
    /// Transformation to apply
    pub transformation: Transformation,
    /// Output column name
    pub output: String,
}

/// Feature Store registry
pub struct FeatureStore {
    /// Feature groups
    groups: RwLock<HashMap<String, FeatureGroup>>,
    /// Feature views
    views: RwLock<HashMap<String, FeatureView>>,
    /// Pipelines
    pipelines: RwLock<HashMap<String, FeaturePipeline>>,
    /// Online store
    online_store: Arc<OnlineStore>,
    /// Feature lineage: feature -> source features
    lineage: RwLock<HashMap<String, Vec<String>>>,
    /// Statistics
    stats: FeatureStoreStats,
}

/// Feature store statistics
#[derive(Debug, Clone, Default)]
pub struct FeatureStoreStats {
    pub total_groups: usize,
    pub total_views: usize,
    pub total_features: usize,
    pub total_pipelines: usize,
    pub online_features_served: u64,
    pub offline_features_served: u64,
}

/// Feature store errors
#[derive(Debug)]
pub enum FeatureStoreError {
    GroupNotFound(String),
    ViewNotFound(String),
    FeatureNotFound(String),
    PipelineNotFound(String),
    DuplicateGroup(String),
    DuplicateView(String),
    InvalidTransformation(String),
    ValidationError(String),
}

impl std::fmt::Display for FeatureStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureStoreError::GroupNotFound(n) => write!(f, "Feature group not found: {}", n),
            FeatureStoreError::ViewNotFound(n) => write!(f, "Feature view not found: {}", n),
            FeatureStoreError::FeatureNotFound(n) => write!(f, "Feature not found: {}", n),
            FeatureStoreError::PipelineNotFound(n) => write!(f, "Pipeline not found: {}", n),
            FeatureStoreError::DuplicateGroup(n) => write!(f, "Duplicate feature group: {}", n),
            FeatureStoreError::DuplicateView(n) => write!(f, "Duplicate feature view: {}", n),
            FeatureStoreError::InvalidTransformation(m) => write!(f, "Invalid transformation: {}", m),
            FeatureStoreError::ValidationError(m) => write!(f, "Validation error: {}", m),
        }
    }
}

impl std::error::Error for FeatureStoreError {}

impl FeatureStore {
    /// Create a new feature store
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            views: RwLock::new(HashMap::new()),
            pipelines: RwLock::new(HashMap::new()),
            online_store: Arc::new(OnlineStore::new(3600_000)), // 1 hour default TTL
            lineage: RwLock::new(HashMap::new()),
            stats: FeatureStoreStats::default(),
        }
    }

    /// Register a feature group
    pub fn register_group(&self, group: FeatureGroup) -> Result<(), FeatureStoreError> {
        let mut groups = self.groups.write();
        if groups.contains_key(&group.name) {
            return Err(FeatureStoreError::DuplicateGroup(group.name));
        }
        groups.insert(group.name.clone(), group);
        Ok(())
    }

    /// Get a feature group
    pub fn get_group(&self, name: &str) -> Option<FeatureGroup> {
        self.groups.read().get(name).cloned()
    }

    /// List all feature groups
    pub fn list_groups(&self) -> Vec<FeatureGroup> {
        self.groups.read().values().cloned().collect()
    }

    /// Register a feature view
    pub fn register_view(&self, view: FeatureView) -> Result<(), FeatureStoreError> {
        let mut views = self.views.write();
        if views.contains_key(&view.name) {
            return Err(FeatureStoreError::DuplicateView(view.name));
        }
        views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Get a feature view
    pub fn get_view(&self, name: &str) -> Option<FeatureView> {
        self.views.read().get(name).cloned()
    }

    /// Register a transformation pipeline
    pub fn register_pipeline(&self, pipeline: FeaturePipeline) -> Result<(), FeatureStoreError> {
        // Track lineage
        {
            let mut lineage = self.lineage.write();
            for output in &pipeline.outputs {
                lineage.insert(output.clone(), pipeline.inputs.clone());
            }
        }

        self.pipelines.write().insert(pipeline.name.clone(), pipeline);
        Ok(())
    }

    /// Get online features for serving
    pub fn get_online_features(
        &self,
        entity_keys: &HashMap<String, FeatureValue>,
        feature_names: &[String],
    ) -> Result<FeatureVector, FeatureStoreError> {
        let entity_key = self.build_entity_key(entity_keys);
        let features = self.online_store.get_features(&entity_key, feature_names);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut result_features = HashMap::new();
        for (name, value) in features {
            result_features.insert(name, value.unwrap_or(FeatureValue::Null));
        }

        Ok(FeatureVector {
            entity_keys: entity_keys.clone(),
            features: result_features,
            feature_timestamp: now,
            request_timestamp: now,
        })
    }

    /// Write features to online store
    pub fn write_online_features(
        &self,
        entity_keys: &HashMap<String, FeatureValue>,
        features: HashMap<String, FeatureValue>,
    ) {
        let entity_key = self.build_entity_key(entity_keys);
        self.online_store.write_features(&entity_key, features);
    }

    /// Get historical features (point-in-time correct)
    pub fn get_historical_features(
        &self,
        request: PointInTimeRequest,
    ) -> Result<Vec<FeatureVector>, FeatureStoreError> {
        // In a real implementation, this would query the offline store
        // with point-in-time joins to ensure no data leakage
        let mut results = Vec::new();

        for entity in request.entities {
            let mut features = HashMap::new();
            for feature_name in &request.features {
                // Placeholder - would do actual point-in-time lookup
                features.insert(feature_name.clone(), FeatureValue::Null);
            }

            results.push(FeatureVector {
                entity_keys: entity.keys,
                features,
                feature_timestamp: entity.event_timestamp,
                request_timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            });
        }

        Ok(results)
    }

    /// Apply transformation to feature value
    pub fn apply_transformation(
        &self,
        value: &FeatureValue,
        transformation: &Transformation,
    ) -> Result<FeatureValue, FeatureStoreError> {
        match transformation {
            Transformation::Identity => Ok(value.clone()),

            Transformation::StandardScaler { mean, std } => {
                if let Some(v) = value.as_f64() {
                    Ok(FeatureValue::Float64((v - mean) / std))
                } else {
                    Err(FeatureStoreError::InvalidTransformation(
                        "StandardScaler requires numeric input".into(),
                    ))
                }
            }

            Transformation::MinMaxScaler { min, max } => {
                if let Some(v) = value.as_f64() {
                    Ok(FeatureValue::Float64((v - min) / (max - min)))
                } else {
                    Err(FeatureStoreError::InvalidTransformation(
                        "MinMaxScaler requires numeric input".into(),
                    ))
                }
            }

            Transformation::Log { base } => {
                if let Some(v) = value.as_f64() {
                    if v > 0.0 {
                        Ok(FeatureValue::Float64(v.log(*base)))
                    } else {
                        Err(FeatureStoreError::InvalidTransformation(
                            "Log requires positive input".into(),
                        ))
                    }
                } else {
                    Err(FeatureStoreError::InvalidTransformation(
                        "Log requires numeric input".into(),
                    ))
                }
            }

            Transformation::Bucketize { boundaries } => {
                if let Some(v) = value.as_f64() {
                    let bucket = boundaries.iter().position(|&b| v < b).unwrap_or(boundaries.len());
                    Ok(FeatureValue::Int32(bucket as i32))
                } else {
                    Err(FeatureStoreError::InvalidTransformation(
                        "Bucketize requires numeric input".into(),
                    ))
                }
            }

            Transformation::OneHot { categories } => {
                if let FeatureValue::String(s) = value {
                    let mut vec = vec![FeatureValue::Float32(0.0); categories.len()];
                    if let Some(idx) = categories.iter().position(|c| c == s) {
                        vec[idx] = FeatureValue::Float32(1.0);
                    }
                    Ok(FeatureValue::Array(vec))
                } else {
                    Err(FeatureStoreError::InvalidTransformation(
                        "OneHot requires string input".into(),
                    ))
                }
            }

            Transformation::HashBucket { num_buckets } => {
                let hash = match value {
                    FeatureValue::String(s) => {
                        let mut h: u64 = 0;
                        for byte in s.bytes() {
                            h = h.wrapping_mul(31).wrapping_add(byte as u64);
                        }
                        h
                    }
                    FeatureValue::Int64(v) => *v as u64,
                    _ => 0,
                };
                Ok(FeatureValue::Int32((hash % *num_buckets as u64) as i32))
            }

            _ => Ok(value.clone()),
        }
    }

    /// Get feature lineage
    pub fn get_lineage(&self, feature_name: &str) -> Vec<String> {
        self.lineage
            .read()
            .get(feature_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Get statistics
    pub fn get_stats(&self) -> FeatureStoreStats {
        let groups = self.groups.read();
        let views = self.views.read();
        let pipelines = self.pipelines.read();

        let total_features: usize = groups.values().map(|g| g.features.len()).sum();
        let (hits, misses, _) = self.online_store.stats();

        FeatureStoreStats {
            total_groups: groups.len(),
            total_views: views.len(),
            total_features,
            total_pipelines: pipelines.len(),
            online_features_served: hits + misses,
            offline_features_served: 0,
        }
    }

    fn build_entity_key(&self, keys: &HashMap<String, FeatureValue>) -> String {
        let mut parts: Vec<String> = keys
            .iter()
            .map(|(k, v)| format!("{}={:?}", k, v))
            .collect();
        parts.sort();
        parts.join("|")
    }
}

impl Default for FeatureStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_online_store() {
        let store = OnlineStore::new(3600_000);

        let mut features = HashMap::new();
        features.insert("age".to_string(), FeatureValue::Int32(25));
        features.insert("score".to_string(), FeatureValue::Float64(0.85));

        store.write_features("user_123", features);

        let result = store.get_features("user_123", &["age".to_string(), "score".to_string()]);
        assert!(result.get("age").unwrap().is_some());
        assert!(result.get("score").unwrap().is_some());
    }

    #[test]
    fn test_transformations() {
        let store = FeatureStore::new();

        // StandardScaler
        let result = store
            .apply_transformation(
                &FeatureValue::Float64(100.0),
                &Transformation::StandardScaler { mean: 50.0, std: 25.0 },
            )
            .unwrap();
        if let FeatureValue::Float64(v) = result {
            assert!((v - 2.0).abs() < 0.001);
        }

        // MinMaxScaler
        let result = store
            .apply_transformation(
                &FeatureValue::Float64(75.0),
                &Transformation::MinMaxScaler { min: 0.0, max: 100.0 },
            )
            .unwrap();
        if let FeatureValue::Float64(v) = result {
            assert!((v - 0.75).abs() < 0.001);
        }

        // Bucketize
        let result = store
            .apply_transformation(
                &FeatureValue::Float64(35.0),
                &Transformation::Bucketize {
                    boundaries: vec![18.0, 25.0, 35.0, 50.0, 65.0],
                },
            )
            .unwrap();
        if let FeatureValue::Int32(bucket) = result {
            assert_eq!(bucket, 2);
        }
    }

    #[test]
    fn test_feature_group_registration() {
        let store = FeatureStore::new();

        let group = FeatureGroup {
            name: "user_features".to_string(),
            version: 1,
            description: "User demographic features".to_string(),
            entity_keys: vec!["user_id".to_string()],
            event_timestamp_column: Some("created_at".to_string()),
            features: vec![FeatureDefinition {
                name: "age".to_string(),
                dtype: FeatureDataType::Int32,
                description: "User age".to_string(),
                tags: vec!["demographic".to_string()],
                owner: "data-team".to_string(),
                created_at: 0,
                deprecated: false,
                default_value: None,
                transformation: None,
            }],
            source: FeatureSource::Table {
                database: "analytics".to_string(),
                table: "users".to_string(),
            },
            online_enabled: true,
            offline_enabled: true,
            online_ttl_seconds: Some(3600),
            created_at: 0,
            updated_at: 0,
            tags: vec![],
        };

        store.register_group(group).unwrap();
        assert!(store.get_group("user_features").is_some());
    }

    #[test]
    fn test_one_hot_encoding() {
        let store = FeatureStore::new();

        let result = store
            .apply_transformation(
                &FeatureValue::String("cat".to_string()),
                &Transformation::OneHot {
                    categories: vec!["dog".to_string(), "cat".to_string(), "bird".to_string()],
                },
            )
            .unwrap();

        if let FeatureValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            if let FeatureValue::Float32(v) = &arr[1] {
                assert_eq!(*v, 1.0);
            }
        }
    }
}
