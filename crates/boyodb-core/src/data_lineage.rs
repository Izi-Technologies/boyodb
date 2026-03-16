//! Data Lineage Tracking and Data Quality Monitoring
//!
//! Provides comprehensive data lineage tracking, schema evolution management,
//! and data quality rule enforcement.
//!
//! ## Features
//!
//! - Column-level lineage tracking
//! - Data flow visualization
//! - Schema evolution with compatibility checks
//! - Data quality rules and validation
//! - Anomaly detection
//! - Impact analysis for schema changes

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Data Lineage Types
// ============================================================================

/// Unique identifier for a data asset (table, view, etc.)
pub type AssetId = String;

/// Unique identifier for a column
pub type ColumnId = String;

/// Fully qualified column reference
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    pub database: String,
    pub table: String,
    pub column: String,
}

impl ColumnRef {
    pub fn new(database: &str, table: &str, column: &str) -> Self {
        Self {
            database: database.to_string(),
            table: table.to_string(),
            column: column.to_string(),
        }
    }

    pub fn to_id(&self) -> ColumnId {
        format!("{}.{}.{}", self.database, self.table, self.column)
    }

    pub fn asset_id(&self) -> AssetId {
        format!("{}.{}", self.database, self.table)
    }
}

/// Type of data asset
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetType {
    Table,
    View,
    MaterializedView,
    ExternalTable,
    Stream,
    Function,
}

/// Type of lineage relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LineageType {
    /// Direct copy (SELECT col FROM t)
    Direct,
    /// Transformation (SELECT upper(col) FROM t)
    Transformation,
    /// Aggregation (SELECT SUM(col) FROM t)
    Aggregation,
    /// Join (derived from join condition)
    Join,
    /// Filter (used in WHERE clause)
    Filter,
    /// Expression (part of complex expression)
    Expression,
}

/// A lineage edge connecting two columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Source column
    pub source: ColumnRef,
    /// Target column
    pub target: ColumnRef,
    /// Type of relationship
    pub lineage_type: LineageType,
    /// Transformation expression (if applicable)
    pub transformation: Option<String>,
    /// Query that created this relationship
    pub query_id: Option<String>,
    /// Timestamp when relationship was created
    pub created_at: u64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Data asset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataAsset {
    pub id: AssetId,
    pub name: String,
    pub asset_type: AssetType,
    pub database: String,
    pub schema_version: u64,
    pub columns: Vec<ColumnInfo>,
    pub created_at: u64,
    pub updated_at: u64,
    pub owner: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
}

/// Column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub sensitivity: DataSensitivity,
}

/// Data sensitivity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataSensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
    Pii,
}

// ============================================================================
// Lineage Manager
// ============================================================================

/// Data lineage manager
pub struct LineageManager {
    /// Registered assets
    assets: RwLock<HashMap<AssetId, DataAsset>>,
    /// Lineage edges (target -> sources)
    edges: RwLock<HashMap<ColumnId, Vec<LineageEdge>>>,
    /// Reverse edges (source -> targets)
    reverse_edges: RwLock<HashMap<ColumnId, Vec<LineageEdge>>>,
}

impl LineageManager {
    pub fn new() -> Self {
        Self {
            assets: RwLock::new(HashMap::new()),
            edges: RwLock::new(HashMap::new()),
            reverse_edges: RwLock::new(HashMap::new()),
        }
    }

    /// Register a data asset
    pub fn register_asset(&self, asset: DataAsset) {
        self.assets.write().insert(asset.id.clone(), asset);
    }

    /// Get an asset
    pub fn get_asset(&self, id: &str) -> Option<DataAsset> {
        self.assets.read().get(id).cloned()
    }

    /// Add a lineage edge
    pub fn add_edge(&self, edge: LineageEdge) {
        let target_id = edge.target.to_id();
        let source_id = edge.source.to_id();

        self.edges.write()
            .entry(target_id)
            .or_insert_with(Vec::new)
            .push(edge.clone());

        self.reverse_edges.write()
            .entry(source_id)
            .or_insert_with(Vec::new)
            .push(edge);
    }

    /// Get upstream lineage for a column (what data feeds into this column)
    pub fn get_upstream(&self, column: &ColumnRef) -> Vec<LineageEdge> {
        let col_id = column.to_id();
        self.edges.read().get(&col_id).cloned().unwrap_or_default()
    }

    /// Get downstream lineage for a column (what columns depend on this column)
    pub fn get_downstream(&self, column: &ColumnRef) -> Vec<LineageEdge> {
        let col_id = column.to_id();
        self.reverse_edges.read().get(&col_id).cloned().unwrap_or_default()
    }

    /// Get full upstream lineage (recursive)
    pub fn get_full_upstream(&self, column: &ColumnRef) -> LineageGraph {
        let mut graph = LineageGraph::new();
        let mut visited = HashSet::new();
        self.traverse_upstream(column, &mut graph, &mut visited);
        graph
    }

    fn traverse_upstream(
        &self,
        column: &ColumnRef,
        graph: &mut LineageGraph,
        visited: &mut HashSet<ColumnId>,
    ) {
        let col_id = column.to_id();
        if visited.contains(&col_id) {
            return;
        }
        visited.insert(col_id.clone());
        graph.columns.insert(col_id.clone());

        let edges = self.get_upstream(column);
        for edge in edges {
            graph.edges.push(edge.clone());
            self.traverse_upstream(&edge.source, graph, visited);
        }
    }

    /// Get full downstream lineage (recursive)
    pub fn get_full_downstream(&self, column: &ColumnRef) -> LineageGraph {
        let mut graph = LineageGraph::new();
        let mut visited = HashSet::new();
        self.traverse_downstream(column, &mut graph, &mut visited);
        graph
    }

    fn traverse_downstream(
        &self,
        column: &ColumnRef,
        graph: &mut LineageGraph,
        visited: &mut HashSet<ColumnId>,
    ) {
        let col_id = column.to_id();
        if visited.contains(&col_id) {
            return;
        }
        visited.insert(col_id.clone());
        graph.columns.insert(col_id.clone());

        let edges = self.get_downstream(column);
        for edge in edges {
            graph.edges.push(edge.clone());
            self.traverse_downstream(&edge.target, graph, visited);
        }
    }

    /// Analyze impact of dropping a column
    pub fn impact_analysis(&self, column: &ColumnRef) -> ImpactAnalysis {
        let downstream = self.get_full_downstream(column);

        let affected_tables: HashSet<_> = downstream.edges
            .iter()
            .map(|e| e.target.asset_id())
            .collect();

        ImpactAnalysis {
            source_column: column.clone(),
            affected_columns: downstream.columns.len(),
            affected_tables: affected_tables.len(),
            downstream_graph: downstream,
        }
    }
}

impl Default for LineageManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Lineage graph representation
#[derive(Debug, Clone, Default, Serialize)]
pub struct LineageGraph {
    pub columns: HashSet<ColumnId>,
    pub edges: Vec<LineageEdge>,
}

impl LineageGraph {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Impact analysis result
#[derive(Debug, Clone, Serialize)]
pub struct ImpactAnalysis {
    pub source_column: ColumnRef,
    pub affected_columns: usize,
    pub affected_tables: usize,
    pub downstream_graph: LineageGraph,
}

// ============================================================================
// Schema Evolution
// ============================================================================

/// Schema change type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaChangeType {
    AddColumn,
    DropColumn,
    RenameColumn,
    ChangeType,
    AddNullable,
    RemoveNullable,
    AddDefault,
    RemoveDefault,
    AddConstraint,
    DropConstraint,
}

/// Schema compatibility level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    /// No constraints
    None,
    /// New schema can read old data
    Backward,
    /// Old schema can read new data
    Forward,
    /// Both backward and forward compatible
    Full,
}

/// Schema change record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    pub change_type: SchemaChangeType,
    pub column_name: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    pub is_breaking: bool,
}

/// Schema version record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub version: u64,
    pub asset_id: AssetId,
    pub changes: Vec<SchemaChange>,
    pub created_at: u64,
    pub created_by: Option<String>,
    pub compatible_with: Vec<u64>,
}

/// Schema evolution manager
pub struct SchemaEvolutionManager {
    /// Schema history per asset
    history: RwLock<HashMap<AssetId, Vec<SchemaVersion>>>,
    /// Compatibility requirements per asset
    compatibility: RwLock<HashMap<AssetId, CompatibilityLevel>>,
}

impl SchemaEvolutionManager {
    pub fn new() -> Self {
        Self {
            history: RwLock::new(HashMap::new()),
            compatibility: RwLock::new(HashMap::new()),
        }
    }

    /// Set compatibility level for an asset
    pub fn set_compatibility(&self, asset_id: &str, level: CompatibilityLevel) {
        self.compatibility.write().insert(asset_id.to_string(), level);
    }

    /// Get compatibility level for an asset
    pub fn get_compatibility(&self, asset_id: &str) -> CompatibilityLevel {
        self.compatibility.read()
            .get(asset_id)
            .copied()
            .unwrap_or(CompatibilityLevel::None)
    }

    /// Check if a schema change is compatible
    pub fn check_compatibility(
        &self,
        asset_id: &str,
        changes: &[SchemaChange],
    ) -> CompatibilityResult {
        let required_level = self.get_compatibility(asset_id);
        let mut violations = Vec::new();

        for change in changes {
            let (backward_safe, forward_safe) = self.analyze_change(change);

            let is_compatible = match required_level {
                CompatibilityLevel::None => true,
                CompatibilityLevel::Backward => backward_safe,
                CompatibilityLevel::Forward => forward_safe,
                CompatibilityLevel::Full => backward_safe && forward_safe,
            };

            if !is_compatible {
                violations.push(CompatibilityViolation {
                    change: change.clone(),
                    required_level,
                    backward_compatible: backward_safe,
                    forward_compatible: forward_safe,
                });
            }
        }

        CompatibilityResult {
            compatible: violations.is_empty(),
            violations,
        }
    }

    fn analyze_change(&self, change: &SchemaChange) -> (bool, bool) {
        match change.change_type {
            SchemaChangeType::AddColumn => {
                // Adding nullable column is backward compatible
                // Adding required column is not backward compatible
                (true, false)
            }
            SchemaChangeType::DropColumn => {
                // Dropping column is not backward compatible
                (false, true)
            }
            SchemaChangeType::RenameColumn => {
                // Renaming is neither backward nor forward compatible
                (false, false)
            }
            SchemaChangeType::ChangeType => {
                // Type change depends on the specific types
                // For simplicity, mark as incompatible
                (false, false)
            }
            SchemaChangeType::AddNullable => {
                // Making column nullable is backward compatible
                (true, false)
            }
            SchemaChangeType::RemoveNullable => {
                // Making column non-nullable is not backward compatible
                (false, true)
            }
            SchemaChangeType::AddDefault => {
                // Adding default is backward compatible
                (true, true)
            }
            SchemaChangeType::RemoveDefault => {
                // Removing default is not backward compatible
                (false, true)
            }
            SchemaChangeType::AddConstraint => {
                // Adding constraint is not backward compatible
                (false, true)
            }
            SchemaChangeType::DropConstraint => {
                // Dropping constraint is backward compatible
                (true, false)
            }
        }
    }

    /// Record a schema change
    pub fn record_change(
        &self,
        asset_id: &str,
        changes: Vec<SchemaChange>,
        created_by: Option<&str>,
    ) -> SchemaVersion {
        let mut history = self.history.write();
        let versions = history.entry(asset_id.to_string()).or_insert_with(Vec::new);

        let new_version = versions.last().map(|v| v.version + 1).unwrap_or(1);

        let version = SchemaVersion {
            version: new_version,
            asset_id: asset_id.to_string(),
            changes,
            created_at: current_timestamp(),
            created_by: created_by.map(|s| s.to_string()),
            compatible_with: vec![new_version - 1], // Simplified
        };

        versions.push(version.clone());
        version
    }

    /// Get schema history for an asset
    pub fn get_history(&self, asset_id: &str) -> Vec<SchemaVersion> {
        self.history.read()
            .get(asset_id)
            .cloned()
            .unwrap_or_default()
    }
}

impl Default for SchemaEvolutionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Compatibility check result
#[derive(Debug, Clone, Serialize)]
pub struct CompatibilityResult {
    pub compatible: bool,
    pub violations: Vec<CompatibilityViolation>,
}

/// Compatibility violation
#[derive(Debug, Clone, Serialize)]
pub struct CompatibilityViolation {
    pub change: SchemaChange,
    pub required_level: CompatibilityLevel,
    pub backward_compatible: bool,
    pub forward_compatible: bool,
}

// ============================================================================
// Data Quality Rules
// ============================================================================

/// Data quality rule type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleType {
    /// Column cannot be null
    NotNull,
    /// Values must be unique
    Unique,
    /// Value must be in range
    Range,
    /// Value must match pattern
    Pattern,
    /// Value must be in set
    InSet,
    /// Custom SQL expression
    Expression,
    /// Statistical check (mean, stddev)
    Statistical,
    /// Freshness check
    Freshness,
    /// Row count check
    RowCount,
    /// Referential integrity
    ReferentialIntegrity,
}

/// Data quality rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityRule {
    /// Rule ID
    pub id: String,
    /// Rule name
    pub name: String,
    /// Rule type
    pub rule_type: RuleType,
    /// Target column(s)
    pub columns: Vec<ColumnRef>,
    /// Rule parameters
    pub parameters: HashMap<String, String>,
    /// Severity level
    pub severity: RuleSeverity,
    /// Is rule enabled
    pub enabled: bool,
    /// Description
    pub description: Option<String>,
    /// Created timestamp
    pub created_at: u64,
}

/// Rule severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Data quality check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityCheckResult {
    pub rule_id: String,
    pub rule_name: String,
    pub passed: bool,
    pub severity: RuleSeverity,
    pub message: String,
    pub rows_checked: u64,
    pub rows_failed: u64,
    pub failure_rate: f64,
    pub checked_at: u64,
    pub duration_ms: u64,
    pub sample_failures: Vec<String>,
}

/// Data quality manager
pub struct DataQualityManager {
    /// Defined rules
    rules: RwLock<HashMap<String, DataQualityRule>>,
    /// Rule assignments (asset -> rules)
    assignments: RwLock<HashMap<AssetId, Vec<String>>>,
    /// Check history
    history: RwLock<Vec<QualityCheckResult>>,
}

impl DataQualityManager {
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
            history: RwLock::new(Vec::new()),
        }
    }

    /// Create a data quality rule
    pub fn create_rule(&self, rule: DataQualityRule) {
        self.rules.write().insert(rule.id.clone(), rule);
    }

    /// Delete a rule
    pub fn delete_rule(&self, rule_id: &str) -> bool {
        self.rules.write().remove(rule_id).is_some()
    }

    /// Assign rule to asset
    pub fn assign_rule(&self, asset_id: &str, rule_id: &str) {
        self.assignments.write()
            .entry(asset_id.to_string())
            .or_insert_with(Vec::new)
            .push(rule_id.to_string());
    }

    /// Get rules for an asset
    pub fn get_rules_for_asset(&self, asset_id: &str) -> Vec<DataQualityRule> {
        let assignments = self.assignments.read();
        let rules = self.rules.read();

        assignments.get(asset_id)
            .map(|rule_ids| {
                rule_ids.iter()
                    .filter_map(|id| rules.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Record a check result
    pub fn record_result(&self, result: QualityCheckResult) {
        self.history.write().push(result);
    }

    /// Get check history
    pub fn get_history(&self, limit: usize) -> Vec<QualityCheckResult> {
        let history = self.history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get failed checks
    pub fn get_failed_checks(&self) -> Vec<QualityCheckResult> {
        self.history.read()
            .iter()
            .filter(|r| !r.passed)
            .cloned()
            .collect()
    }

    /// Create common rules
    pub fn create_not_null_rule(name: &str, column: ColumnRef) -> DataQualityRule {
        DataQualityRule {
            id: format!("not_null_{}", column.to_id()),
            name: name.to_string(),
            rule_type: RuleType::NotNull,
            columns: vec![column],
            parameters: HashMap::new(),
            severity: RuleSeverity::Error,
            enabled: true,
            description: Some("Column must not contain null values".to_string()),
            created_at: current_timestamp(),
        }
    }

    pub fn create_unique_rule(name: &str, columns: Vec<ColumnRef>) -> DataQualityRule {
        let id = columns.iter().map(|c| c.to_id()).collect::<Vec<_>>().join("_");
        DataQualityRule {
            id: format!("unique_{}", id),
            name: name.to_string(),
            rule_type: RuleType::Unique,
            columns,
            parameters: HashMap::new(),
            severity: RuleSeverity::Error,
            enabled: true,
            description: Some("Values must be unique".to_string()),
            created_at: current_timestamp(),
        }
    }

    pub fn create_range_rule(
        name: &str,
        column: ColumnRef,
        min: Option<f64>,
        max: Option<f64>,
    ) -> DataQualityRule {
        let mut params = HashMap::new();
        if let Some(min) = min {
            params.insert("min".to_string(), min.to_string());
        }
        if let Some(max) = max {
            params.insert("max".to_string(), max.to_string());
        }

        DataQualityRule {
            id: format!("range_{}", column.to_id()),
            name: name.to_string(),
            rule_type: RuleType::Range,
            columns: vec![column],
            parameters: params,
            severity: RuleSeverity::Warning,
            enabled: true,
            description: Some("Value must be within specified range".to_string()),
            created_at: current_timestamp(),
        }
    }

    pub fn create_pattern_rule(name: &str, column: ColumnRef, pattern: &str) -> DataQualityRule {
        let mut params = HashMap::new();
        params.insert("pattern".to_string(), pattern.to_string());

        DataQualityRule {
            id: format!("pattern_{}", column.to_id()),
            name: name.to_string(),
            rule_type: RuleType::Pattern,
            columns: vec![column],
            parameters: params,
            severity: RuleSeverity::Warning,
            enabled: true,
            description: Some(format!("Value must match pattern: {}", pattern)),
            created_at: current_timestamp(),
        }
    }

    pub fn create_freshness_rule(
        name: &str,
        column: ColumnRef,
        max_age_seconds: u64,
    ) -> DataQualityRule {
        let mut params = HashMap::new();
        params.insert("max_age_seconds".to_string(), max_age_seconds.to_string());

        DataQualityRule {
            id: format!("freshness_{}", column.to_id()),
            name: name.to_string(),
            rule_type: RuleType::Freshness,
            columns: vec![column],
            parameters: params,
            severity: RuleSeverity::Warning,
            enabled: true,
            description: Some(format!("Data must be no older than {} seconds", max_age_seconds)),
            created_at: current_timestamp(),
        }
    }
}

impl Default for DataQualityManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Anomaly Detection
// ============================================================================

/// Anomaly type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyType {
    /// Sudden spike in values
    Spike,
    /// Sudden drop in values
    Drop,
    /// Value outside expected range
    Outlier,
    /// Missing data
    MissingData,
    /// Unusual pattern
    Pattern,
    /// Volume change
    VolumeChange,
}

/// Detected anomaly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub id: String,
    pub anomaly_type: AnomalyType,
    pub column: ColumnRef,
    pub detected_at: u64,
    pub value: Option<f64>,
    pub expected_value: Option<f64>,
    pub deviation: f64,
    pub severity: RuleSeverity,
    pub message: String,
    pub acknowledged: bool,
}

/// Anomaly detector
pub struct AnomalyDetector {
    /// Historical statistics per column
    stats: RwLock<HashMap<ColumnId, ColumnStats>>,
    /// Detected anomalies
    anomalies: RwLock<Vec<Anomaly>>,
    /// Configuration
    config: AnomalyConfig,
}

/// Column statistics for anomaly detection
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub count: u64,
    pub sum: f64,
    pub mean: f64,
    pub variance: f64,
    pub min: f64,
    pub max: f64,
    pub last_values: Vec<f64>,
}

impl ColumnStats {
    pub fn stddev(&self) -> f64 {
        self.variance.sqrt()
    }

    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;

        let old_mean = self.mean;
        self.mean = self.sum / self.count as f64;

        // Online variance calculation (Welford's algorithm)
        self.variance += (value - old_mean) * (value - self.mean);

        if value < self.min || self.count == 1 {
            self.min = value;
        }
        if value > self.max || self.count == 1 {
            self.max = value;
        }

        // Keep last N values for pattern detection
        self.last_values.push(value);
        if self.last_values.len() > 100 {
            self.last_values.remove(0);
        }
    }

    pub fn is_outlier(&self, value: f64, threshold: f64) -> bool {
        if self.count < 10 {
            return false; // Not enough data
        }
        let z_score = (value - self.mean).abs() / self.stddev();
        z_score > threshold
    }
}

/// Anomaly detection configuration
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    /// Z-score threshold for outliers
    pub outlier_threshold: f64,
    /// Percentage change threshold for spikes/drops
    pub change_threshold: f64,
    /// Minimum samples before detection
    pub min_samples: u64,
    /// Enable spike detection
    pub detect_spikes: bool,
    /// Enable outlier detection
    pub detect_outliers: bool,
    /// Enable volume change detection
    pub detect_volume_changes: bool,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            outlier_threshold: 3.0,
            change_threshold: 0.5, // 50% change
            min_samples: 10,
            detect_spikes: true,
            detect_outliers: true,
            detect_volume_changes: true,
        }
    }
}

impl AnomalyDetector {
    pub fn new(config: AnomalyConfig) -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
            anomalies: RwLock::new(Vec::new()),
            config,
        }
    }

    /// Record a value and check for anomalies
    pub fn record(&self, column: &ColumnRef, value: f64) -> Option<Anomaly> {
        let col_id = column.to_id();

        let anomaly = {
            let mut stats = self.stats.write();
            let col_stats = stats.entry(col_id.clone()).or_default();

            let anomaly = self.detect_anomaly(column, value, col_stats);
            col_stats.update(value);
            anomaly
        };

        if let Some(ref a) = anomaly {
            self.anomalies.write().push(a.clone());
        }

        anomaly
    }

    fn detect_anomaly(
        &self,
        column: &ColumnRef,
        value: f64,
        stats: &ColumnStats,
    ) -> Option<Anomaly> {
        if stats.count < self.config.min_samples {
            return None;
        }

        // Check for outlier
        if self.config.detect_outliers && stats.is_outlier(value, self.config.outlier_threshold) {
            let deviation = (value - stats.mean).abs() / stats.stddev();
            return Some(Anomaly {
                id: format!("anomaly_{}_{}", column.to_id(), current_timestamp()),
                anomaly_type: AnomalyType::Outlier,
                column: column.clone(),
                detected_at: current_timestamp(),
                value: Some(value),
                expected_value: Some(stats.mean),
                deviation,
                severity: if deviation > 5.0 { RuleSeverity::Critical } else { RuleSeverity::Warning },
                message: format!(
                    "Value {} is {:.1} standard deviations from mean {:.2}",
                    value, deviation, stats.mean
                ),
                acknowledged: false,
            });
        }

        // Check for spike/drop
        if self.config.detect_spikes && !stats.last_values.is_empty() {
            let last_value = *stats.last_values.last().unwrap();
            if last_value != 0.0 {
                let change_rate = (value - last_value) / last_value.abs();

                if change_rate > self.config.change_threshold {
                    return Some(Anomaly {
                        id: format!("anomaly_{}_{}", column.to_id(), current_timestamp()),
                        anomaly_type: AnomalyType::Spike,
                        column: column.clone(),
                        detected_at: current_timestamp(),
                        value: Some(value),
                        expected_value: Some(last_value),
                        deviation: change_rate,
                        severity: RuleSeverity::Warning,
                        message: format!(
                            "Value spiked {:.1}% from {} to {}",
                            change_rate * 100.0, last_value, value
                        ),
                        acknowledged: false,
                    });
                }

                if change_rate < -self.config.change_threshold {
                    return Some(Anomaly {
                        id: format!("anomaly_{}_{}", column.to_id(), current_timestamp()),
                        anomaly_type: AnomalyType::Drop,
                        column: column.clone(),
                        detected_at: current_timestamp(),
                        value: Some(value),
                        expected_value: Some(last_value),
                        deviation: change_rate.abs(),
                        severity: RuleSeverity::Warning,
                        message: format!(
                            "Value dropped {:.1}% from {} to {}",
                            change_rate.abs() * 100.0, last_value, value
                        ),
                        acknowledged: false,
                    });
                }
            }
        }

        None
    }

    /// Get detected anomalies
    pub fn get_anomalies(&self, limit: usize) -> Vec<Anomaly> {
        let anomalies = self.anomalies.read();
        anomalies.iter().rev().take(limit).cloned().collect()
    }

    /// Get unacknowledged anomalies
    pub fn get_unacknowledged(&self) -> Vec<Anomaly> {
        self.anomalies.read()
            .iter()
            .filter(|a| !a.acknowledged)
            .cloned()
            .collect()
    }

    /// Acknowledge an anomaly
    pub fn acknowledge(&self, anomaly_id: &str) -> bool {
        let mut anomalies = self.anomalies.write();
        if let Some(a) = anomalies.iter_mut().find(|a| a.id == anomaly_id) {
            a.acknowledged = true;
            return true;
        }
        false
    }

    /// Get statistics for a column
    pub fn get_stats(&self, column: &ColumnRef) -> Option<ColumnStats> {
        let col_id = column.to_id();
        self.stats.read().get(&col_id).cloned()
    }
}

impl Default for AnomalyDetector {
    fn default() -> Self {
        Self::new(AnomalyConfig::default())
    }
}

// ============================================================================
// Utilities
// ============================================================================

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref() {
        let col = ColumnRef::new("db", "table", "column");
        assert_eq!(col.to_id(), "db.table.column");
        assert_eq!(col.asset_id(), "db.table");
    }

    #[test]
    fn test_lineage() {
        let mgr = LineageManager::new();

        let source = ColumnRef::new("db", "source", "col");
        let target = ColumnRef::new("db", "target", "col");

        mgr.add_edge(LineageEdge {
            source: source.clone(),
            target: target.clone(),
            lineage_type: LineageType::Direct,
            transformation: None,
            query_id: None,
            created_at: 0,
            metadata: HashMap::new(),
        });

        let upstream = mgr.get_upstream(&target);
        assert_eq!(upstream.len(), 1);
        assert_eq!(upstream[0].source, source);

        let downstream = mgr.get_downstream(&source);
        assert_eq!(downstream.len(), 1);
        assert_eq!(downstream[0].target, target);
    }

    #[test]
    fn test_schema_compatibility() {
        let mgr = SchemaEvolutionManager::new();
        mgr.set_compatibility("test.table", CompatibilityLevel::Backward);

        // Adding column should be backward compatible
        let changes = vec![SchemaChange {
            change_type: SchemaChangeType::AddColumn,
            column_name: "new_col".to_string(),
            old_value: None,
            new_value: Some("VARCHAR(255)".to_string()),
            is_breaking: false,
        }];

        let result = mgr.check_compatibility("test.table", &changes);
        assert!(result.compatible);

        // Dropping column should not be backward compatible
        let changes = vec![SchemaChange {
            change_type: SchemaChangeType::DropColumn,
            column_name: "old_col".to_string(),
            old_value: Some("INT".to_string()),
            new_value: None,
            is_breaking: true,
        }];

        let result = mgr.check_compatibility("test.table", &changes);
        assert!(!result.compatible);
    }

    #[test]
    fn test_anomaly_detection() {
        let detector = AnomalyDetector::new(AnomalyConfig {
            min_samples: 5,
            outlier_threshold: 2.0,
            ..Default::default()
        });

        let col = ColumnRef::new("db", "table", "value");

        // Build up statistics
        for i in 0..10 {
            detector.record(&col, 100.0 + i as f64);
        }

        // Record an outlier
        let anomaly = detector.record(&col, 1000.0);
        assert!(anomaly.is_some());
        assert_eq!(anomaly.unwrap().anomaly_type, AnomalyType::Outlier);
    }

    #[test]
    fn test_data_quality_rules() {
        let mgr = DataQualityManager::new();

        let col = ColumnRef::new("db", "users", "email");
        let rule = DataQualityManager::create_pattern_rule(
            "email_format",
            col.clone(),
            r"^[\w\.-]+@[\w\.-]+\.\w+$",
        );

        mgr.create_rule(rule.clone());
        mgr.assign_rule("db.users", &rule.id);

        let rules = mgr.get_rules_for_asset("db.users");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].rule_type, RuleType::Pattern);
    }
}
