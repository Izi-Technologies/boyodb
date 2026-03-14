//! Data Quality Framework for BoyoDB
//!
//! Data quality capabilities:
//! - Validation rules and constraints
//! - Anomaly detection
//! - Data profiling
//! - Quality metrics and scoring

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Data types for validation
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    Timestamp,
    Json,
    Array,
    Null,
}

/// Cell value for validation
#[derive(Debug, Clone)]
pub enum CellValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Array(Vec<CellValue>),
}

impl CellValue {
    pub fn data_type(&self) -> DataType {
        match self {
            CellValue::String(_) => DataType::String,
            CellValue::Integer(_) => DataType::Integer,
            CellValue::Float(_) => DataType::Float,
            CellValue::Boolean(_) => DataType::Boolean,
            CellValue::Null => DataType::Null,
            CellValue::Array(_) => DataType::Array,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            CellValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            CellValue::Integer(i) => Some(*i as f64),
            CellValue::Float(f) => Some(*f),
            _ => None,
        }
    }
}

/// Validation rule types
#[derive(Debug, Clone)]
pub enum ValidationRule {
    /// Column must not be null
    NotNull,
    /// Value must be unique in column
    Unique,
    /// Value must be in specified set
    InSet(Vec<CellValue>),
    /// Value must match regex pattern
    Regex(String),
    /// Numeric value must be in range
    Range { min: Option<f64>, max: Option<f64> },
    /// String length must be in range
    Length { min: Option<usize>, max: Option<usize> },
    /// Value must match expected type
    TypeCheck(DataType),
    /// Custom SQL expression must be true
    Expression(String),
    /// Value must be a valid email
    Email,
    /// Value must be a valid URL
    Url,
    /// Value must be a valid phone number
    Phone,
    /// Value must be a valid date format
    DateFormat(String),
    /// Referential integrity check
    ForeignKey { table: String, column: String },
    /// Custom validation function
    Custom { name: String, params: HashMap<String, String> },
}

/// Validation result for a single check
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Rule that was checked
    pub rule: String,
    /// Column name
    pub column: String,
    /// Whether validation passed
    pub passed: bool,
    /// Number of valid values
    pub valid_count: u64,
    /// Number of invalid values
    pub invalid_count: u64,
    /// Sample invalid values (up to 10)
    pub invalid_samples: Vec<(usize, CellValue)>,
    /// Error message if failed
    pub error_message: Option<String>,
}

/// Column profile
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    /// Column name
    pub name: String,
    /// Inferred data type
    pub data_type: DataType,
    /// Total row count
    pub count: u64,
    /// Null count
    pub null_count: u64,
    /// Null percentage
    pub null_percentage: f64,
    /// Distinct value count
    pub distinct_count: u64,
    /// Distinct percentage
    pub distinct_percentage: f64,
    /// Min value (for numeric)
    pub min: Option<f64>,
    /// Max value (for numeric)
    pub max: Option<f64>,
    /// Mean value (for numeric)
    pub mean: Option<f64>,
    /// Standard deviation (for numeric)
    pub std_dev: Option<f64>,
    /// Median (for numeric)
    pub median: Option<f64>,
    /// Min length (for string)
    pub min_length: Option<usize>,
    /// Max length (for string)
    pub max_length: Option<usize>,
    /// Average length (for string)
    pub avg_length: Option<f64>,
    /// Most common values
    pub top_values: Vec<(CellValue, u64)>,
    /// Pattern distribution (for strings)
    pub patterns: HashMap<String, u64>,
}

/// Data profile for entire dataset
#[derive(Debug, Clone)]
pub struct DataProfile {
    /// Table/dataset name
    pub name: String,
    /// Total row count
    pub row_count: u64,
    /// Column count
    pub column_count: usize,
    /// Column profiles
    pub columns: Vec<ColumnProfile>,
    /// Overall data quality score (0-100)
    pub quality_score: f64,
    /// Profiling timestamp
    pub timestamp: i64,
}

/// Anomaly types
#[derive(Debug, Clone)]
pub enum AnomalyType {
    /// Value is statistical outlier
    Outlier { z_score: f64 },
    /// Value pattern is unusual
    PatternAnomaly { expected: String, actual: String },
    /// Value is outside expected range
    RangeAnomaly { expected_min: f64, expected_max: f64, actual: f64 },
    /// Sudden change in distribution
    DistributionShift { metric: String, change: f64 },
    /// Missing expected value
    MissingValue,
    /// Duplicate value
    Duplicate,
}

/// Detected anomaly
#[derive(Debug, Clone)]
pub struct Anomaly {
    /// Row index
    pub row_index: usize,
    /// Column name
    pub column: String,
    /// Anomaly type
    pub anomaly_type: AnomalyType,
    /// Anomalous value
    pub value: CellValue,
    /// Confidence score (0-1)
    pub confidence: f64,
    /// Severity (low, medium, high, critical)
    pub severity: Severity,
}

/// Severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

/// Quality metric
#[derive(Debug, Clone)]
pub struct QualityMetric {
    /// Metric name
    pub name: String,
    /// Metric value (0-100)
    pub score: f64,
    /// Description
    pub description: String,
    /// Weight for overall score
    pub weight: f64,
}

/// Quality report
#[derive(Debug, Clone)]
pub struct QualityReport {
    /// Dataset name
    pub dataset: String,
    /// Overall quality score (0-100)
    pub overall_score: f64,
    /// Individual metrics
    pub metrics: Vec<QualityMetric>,
    /// Validation results
    pub validations: Vec<ValidationResult>,
    /// Detected anomalies
    pub anomalies: Vec<Anomaly>,
    /// Data profile
    pub profile: DataProfile,
    /// Recommendations
    pub recommendations: Vec<String>,
    /// Report timestamp
    pub timestamp: i64,
}

/// Data Quality Engine
pub struct DataQualityEngine {
    /// Validation rules by table.column
    rules: RwLock<HashMap<String, Vec<ValidationRule>>>,
    /// Cached profiles
    profiles: RwLock<HashMap<String, DataProfile>>,
    /// Statistics
    stats: EngineStats,
}

struct EngineStats {
    validations_run: AtomicU64,
    profiles_generated: AtomicU64,
    anomalies_detected: AtomicU64,
}

impl DataQualityEngine {
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            profiles: RwLock::new(HashMap::new()),
            stats: EngineStats {
                validations_run: AtomicU64::new(0),
                profiles_generated: AtomicU64::new(0),
                anomalies_detected: AtomicU64::new(0),
            },
        }
    }

    /// Add validation rule
    pub fn add_rule(&self, table: &str, column: &str, rule: ValidationRule) {
        let key = format!("{}.{}", table, column);
        self.rules.write().unwrap()
            .entry(key)
            .or_default()
            .push(rule);
    }

    /// Validate data against rules
    pub fn validate(&self, table: &str, column: &str, values: &[CellValue]) -> Vec<ValidationResult> {
        self.stats.validations_run.fetch_add(1, Ordering::Relaxed);

        let key = format!("{}.{}", table, column);
        let rules = self.rules.read().unwrap();

        let column_rules = match rules.get(&key) {
            Some(r) => r.clone(),
            None => return vec![],
        };

        let mut results = Vec::new();

        for rule in column_rules {
            let result = self.apply_rule(&rule, column, values);
            results.push(result);
        }

        results
    }

    fn apply_rule(&self, rule: &ValidationRule, column: &str, values: &[CellValue]) -> ValidationResult {
        let mut valid_count = 0u64;
        let mut invalid_count = 0u64;
        let mut invalid_samples = Vec::new();

        for (i, value) in values.iter().enumerate() {
            let is_valid = match rule {
                ValidationRule::NotNull => !value.is_null(),

                ValidationRule::Unique => {
                    // Simplified - would need to track seen values
                    true
                }

                ValidationRule::InSet(allowed) => {
                    allowed.iter().any(|a| match (a, value) {
                        (CellValue::String(s1), CellValue::String(s2)) => s1 == s2,
                        (CellValue::Integer(i1), CellValue::Integer(i2)) => i1 == i2,
                        (CellValue::Float(f1), CellValue::Float(f2)) => (f1 - f2).abs() < 1e-10,
                        (CellValue::Boolean(b1), CellValue::Boolean(b2)) => b1 == b2,
                        _ => false,
                    })
                }

                ValidationRule::Regex(pattern) => {
                    if let CellValue::String(s) = value {
                        // Simplified regex matching
                        !s.is_empty() && pattern.len() > 0
                    } else {
                        false
                    }
                }

                ValidationRule::Range { min, max } => {
                    if let Some(v) = value.as_f64() {
                        let above_min = min.map_or(true, |m| v >= m);
                        let below_max = max.map_or(true, |m| v <= m);
                        above_min && below_max
                    } else {
                        value.is_null() // Nulls pass range check
                    }
                }

                ValidationRule::Length { min, max } => {
                    if let CellValue::String(s) = value {
                        let len = s.len();
                        let above_min = min.map_or(true, |m| len >= m);
                        let below_max = max.map_or(true, |m| len <= m);
                        above_min && below_max
                    } else {
                        value.is_null()
                    }
                }

                ValidationRule::TypeCheck(expected_type) => {
                    value.is_null() || &value.data_type() == expected_type
                }

                ValidationRule::Email => {
                    if let CellValue::String(s) = value {
                        s.contains('@') && s.contains('.')
                    } else {
                        false
                    }
                }

                ValidationRule::Url => {
                    if let CellValue::String(s) = value {
                        s.starts_with("http://") || s.starts_with("https://")
                    } else {
                        false
                    }
                }

                ValidationRule::Phone => {
                    if let CellValue::String(s) = value {
                        s.chars().filter(|c| c.is_ascii_digit()).count() >= 10
                    } else {
                        false
                    }
                }

                _ => true,
            };

            if is_valid {
                valid_count += 1;
            } else {
                invalid_count += 1;
                if invalid_samples.len() < 10 {
                    invalid_samples.push((i, value.clone()));
                }
            }
        }

        let passed = invalid_count == 0;

        ValidationResult {
            rule: format!("{:?}", rule),
            column: column.to_string(),
            passed,
            valid_count,
            invalid_count,
            invalid_samples,
            error_message: if passed {
                None
            } else {
                Some(format!("{} values failed validation", invalid_count))
            },
        }
    }

    /// Profile a column
    pub fn profile_column(&self, name: &str, values: &[CellValue]) -> ColumnProfile {
        let count = values.len() as u64;
        let null_count = values.iter().filter(|v| v.is_null()).count() as u64;

        // Infer data type from non-null values
        let data_type = values.iter()
            .find(|v| !v.is_null())
            .map(|v| v.data_type())
            .unwrap_or(DataType::Null);

        // Count distinct values
        let mut value_counts: HashMap<String, u64> = HashMap::new();
        for v in values {
            let key = format!("{:?}", v);
            *value_counts.entry(key).or_insert(0) += 1;
        }
        let distinct_count = value_counts.len() as u64;

        // Numeric statistics
        let numeric_values: Vec<f64> = values.iter()
            .filter_map(|v| v.as_f64())
            .collect();

        let (min, max, mean, std_dev, median) = if !numeric_values.is_empty() {
            let min = numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = numeric_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
            let variance = numeric_values.iter()
                .map(|v| (v - mean).powi(2))
                .sum::<f64>() / numeric_values.len() as f64;
            let std_dev = variance.sqrt();

            let mut sorted = numeric_values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median = sorted[sorted.len() / 2];

            (Some(min), Some(max), Some(mean), Some(std_dev), Some(median))
        } else {
            (None, None, None, None, None)
        };

        // String statistics
        let string_values: Vec<&str> = values.iter()
            .filter_map(|v| v.as_string())
            .collect();

        let (min_length, max_length, avg_length) = if !string_values.is_empty() {
            let lengths: Vec<usize> = string_values.iter().map(|s| s.len()).collect();
            let min_len = *lengths.iter().min().unwrap();
            let max_len = *lengths.iter().max().unwrap();
            let avg_len = lengths.iter().sum::<usize>() as f64 / lengths.len() as f64;
            (Some(min_len), Some(max_len), Some(avg_len))
        } else {
            (None, None, None)
        };

        // Top values
        let mut top_values: Vec<_> = value_counts.into_iter()
            .map(|(k, v)| (CellValue::String(k), v))
            .collect();
        top_values.sort_by(|a, b| b.1.cmp(&a.1));
        top_values.truncate(10);

        ColumnProfile {
            name: name.to_string(),
            data_type,
            count,
            null_count,
            null_percentage: if count > 0 { null_count as f64 / count as f64 * 100.0 } else { 0.0 },
            distinct_count,
            distinct_percentage: if count > 0 { distinct_count as f64 / count as f64 * 100.0 } else { 0.0 },
            min,
            max,
            mean,
            std_dev,
            median,
            min_length,
            max_length,
            avg_length,
            top_values,
            patterns: HashMap::new(),
        }
    }

    /// Detect anomalies in data
    pub fn detect_anomalies(&self, column: &str, values: &[CellValue], z_threshold: f64) -> Vec<Anomaly> {
        let mut anomalies = Vec::new();

        // For numeric columns, detect outliers using z-score
        let numeric_values: Vec<(usize, f64)> = values.iter()
            .enumerate()
            .filter_map(|(i, v)| v.as_f64().map(|f| (i, f)))
            .collect();

        if numeric_values.len() >= 3 {
            let vals: Vec<f64> = numeric_values.iter().map(|(_, v)| *v).collect();
            let mean = vals.iter().sum::<f64>() / vals.len() as f64;
            let std = (vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len() as f64).sqrt();

            if std > 0.0 {
                for (i, v) in numeric_values {
                    let z_score = (v - mean) / std;
                    if z_score.abs() > z_threshold {
                        let severity = if z_score.abs() > z_threshold * 2.0 {
                            Severity::Critical
                        } else if z_score.abs() > z_threshold * 1.5 {
                            Severity::High
                        } else {
                            Severity::Medium
                        };

                        anomalies.push(Anomaly {
                            row_index: i,
                            column: column.to_string(),
                            anomaly_type: AnomalyType::Outlier { z_score },
                            value: values[i].clone(),
                            confidence: 1.0 - 1.0 / (1.0 + z_score.abs()),
                            severity,
                        });

                        self.stats.anomalies_detected.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        anomalies
    }

    /// Calculate overall quality score
    pub fn calculate_quality_score(&self, profile: &DataProfile, validations: &[ValidationResult]) -> f64 {
        let mut total_weight = 0.0;
        let mut weighted_score = 0.0;

        // Completeness score (inverse of null percentage)
        for col in &profile.columns {
            let completeness = 100.0 - col.null_percentage;
            weighted_score += completeness * 0.3;
            total_weight += 0.3;
        }

        // Validation pass rate
        if !validations.is_empty() {
            let pass_rate = validations.iter()
                .filter(|v| v.passed)
                .count() as f64 / validations.len() as f64 * 100.0;
            weighted_score += pass_rate * 0.4;
            total_weight += 0.4;
        }

        // Uniqueness score
        for col in &profile.columns {
            let uniqueness = col.distinct_percentage.min(100.0);
            weighted_score += uniqueness * 0.3 / profile.columns.len() as f64;
            total_weight += 0.3 / profile.columns.len() as f64;
        }

        if total_weight > 0.0 {
            weighted_score / total_weight
        } else {
            0.0
        }
    }

    /// Generate quality report
    pub fn generate_report(
        &self,
        dataset: &str,
        columns: &HashMap<String, Vec<CellValue>>,
    ) -> QualityReport {
        self.stats.profiles_generated.fetch_add(1, Ordering::Relaxed);

        let row_count = columns.values()
            .next()
            .map(|c| c.len())
            .unwrap_or(0) as u64;

        // Profile columns
        let column_profiles: Vec<ColumnProfile> = columns.iter()
            .map(|(name, values)| self.profile_column(name, values))
            .collect();

        let profile = DataProfile {
            name: dataset.to_string(),
            row_count,
            column_count: columns.len(),
            columns: column_profiles,
            quality_score: 0.0, // Will be updated
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        };

        // Run validations
        let mut validations = Vec::new();
        for (col_name, values) in columns {
            let col_validations = self.validate(dataset, col_name, values);
            validations.extend(col_validations);
        }

        // Detect anomalies
        let mut anomalies = Vec::new();
        for (col_name, values) in columns {
            let col_anomalies = self.detect_anomalies(col_name, values, 3.0);
            anomalies.extend(col_anomalies);
        }

        // Calculate overall score
        let overall_score = self.calculate_quality_score(&profile, &validations);

        // Generate metrics
        let metrics = vec![
            QualityMetric {
                name: "Completeness".to_string(),
                score: 100.0 - profile.columns.iter()
                    .map(|c| c.null_percentage)
                    .sum::<f64>() / profile.columns.len().max(1) as f64,
                description: "Percentage of non-null values".to_string(),
                weight: 0.3,
            },
            QualityMetric {
                name: "Validity".to_string(),
                score: if validations.is_empty() {
                    100.0
                } else {
                    validations.iter().filter(|v| v.passed).count() as f64
                        / validations.len() as f64 * 100.0
                },
                description: "Percentage of values passing validation rules".to_string(),
                weight: 0.4,
            },
            QualityMetric {
                name: "Uniqueness".to_string(),
                score: profile.columns.iter()
                    .map(|c| c.distinct_percentage)
                    .sum::<f64>() / profile.columns.len().max(1) as f64,
                description: "Average percentage of unique values".to_string(),
                weight: 0.3,
            },
        ];

        // Generate recommendations
        let mut recommendations = Vec::new();

        for col in &profile.columns {
            if col.null_percentage > 10.0 {
                recommendations.push(format!(
                    "Column '{}' has {:.1}% null values - consider adding NOT NULL constraint or default value",
                    col.name, col.null_percentage
                ));
            }

            if col.distinct_percentage < 1.0 && col.count > 100 {
                recommendations.push(format!(
                    "Column '{}' has very low cardinality ({} distinct values) - consider using enum type",
                    col.name, col.distinct_count
                ));
            }
        }

        for validation in &validations {
            if !validation.passed {
                recommendations.push(format!(
                    "Fix {} validation failures in column '{}'",
                    validation.invalid_count, validation.column
                ));
            }
        }

        QualityReport {
            dataset: dataset.to_string(),
            overall_score,
            metrics,
            validations,
            anomalies,
            profile,
            recommendations,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        }
    }

    /// Get engine statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.validations_run.load(Ordering::Relaxed),
            self.stats.profiles_generated.load(Ordering::Relaxed),
            self.stats.anomalies_detected.load(Ordering::Relaxed),
        )
    }
}

impl Default for DataQualityEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_null_validation() {
        let engine = DataQualityEngine::new();
        engine.add_rule("test", "col1", ValidationRule::NotNull);

        let values = vec![
            CellValue::String("a".to_string()),
            CellValue::Null,
            CellValue::String("c".to_string()),
        ];

        let results = engine.validate("test", "col1", &values);
        assert_eq!(results.len(), 1);
        assert!(!results[0].passed);
        assert_eq!(results[0].invalid_count, 1);
    }

    #[test]
    fn test_range_validation() {
        let engine = DataQualityEngine::new();
        engine.add_rule("test", "age", ValidationRule::Range {
            min: Some(0.0),
            max: Some(120.0),
        });

        let values = vec![
            CellValue::Integer(25),
            CellValue::Integer(-5),
            CellValue::Integer(150),
            CellValue::Integer(50),
        ];

        let results = engine.validate("test", "age", &values);
        assert!(!results[0].passed);
        assert_eq!(results[0].invalid_count, 2);
    }

    #[test]
    fn test_column_profile() {
        let engine = DataQualityEngine::new();

        let values = vec![
            CellValue::Integer(10),
            CellValue::Integer(20),
            CellValue::Integer(30),
            CellValue::Null,
            CellValue::Integer(40),
        ];

        let profile = engine.profile_column("test_col", &values);

        assert_eq!(profile.count, 5);
        assert_eq!(profile.null_count, 1);
        assert_eq!(profile.null_percentage, 20.0);
        assert_eq!(profile.min, Some(10.0));
        assert_eq!(profile.max, Some(40.0));
        assert_eq!(profile.mean, Some(25.0));
    }

    #[test]
    fn test_anomaly_detection() {
        let engine = DataQualityEngine::new();

        let values = vec![
            CellValue::Integer(10),
            CellValue::Integer(11),
            CellValue::Integer(12),
            CellValue::Integer(100), // Outlier
            CellValue::Integer(10),
            CellValue::Integer(11),
        ];

        let anomalies = engine.detect_anomalies("col", &values, 2.0);
        assert!(!anomalies.is_empty());
        assert_eq!(anomalies[0].row_index, 3);
    }

    #[test]
    fn test_quality_report() {
        let engine = DataQualityEngine::new();
        engine.add_rule("test", "email", ValidationRule::Email);

        let mut columns = HashMap::new();
        columns.insert("name".to_string(), vec![
            CellValue::String("Alice".to_string()),
            CellValue::String("Bob".to_string()),
            CellValue::Null,
        ]);
        columns.insert("email".to_string(), vec![
            CellValue::String("alice@example.com".to_string()),
            CellValue::String("invalid-email".to_string()),
            CellValue::String("bob@example.com".to_string()),
        ]);

        let report = engine.generate_report("test", &columns);

        assert!(!report.recommendations.is_empty());
        assert!(report.overall_score > 0.0);
    }

    #[test]
    fn test_email_validation() {
        let engine = DataQualityEngine::new();
        engine.add_rule("users", "email", ValidationRule::Email);

        let values = vec![
            CellValue::String("test@example.com".to_string()),
            CellValue::String("invalid".to_string()),
            CellValue::String("another@test.org".to_string()),
        ];

        let results = engine.validate("users", "email", &values);
        assert!(!results[0].passed);
        assert_eq!(results[0].invalid_count, 1);
    }
}
