//! TTL (Time-To-Live) support for automatic data expiration.
//!
//! This module provides:
//! - TTL expression parsing (e.g., "timestamp + INTERVAL 30 DAY")
//! - TTL evaluation during reads and compaction
//! - Background worker for automatic cleanup of expired data
//!
//! # Example
//! ```sql
//! CREATE TABLE events ENGINE = MergeTree
//! ORDER BY (timestamp)
//! TTL timestamp + INTERVAL 30 DAY DELETE;
//!
//! -- Move old data to cold storage before deletion
//! CREATE TABLE logs ENGINE = MergeTree
//! ORDER BY (timestamp)
//! TTL timestamp + INTERVAL 7 DAY TO DISK 'cold'
//! TTL timestamp + INTERVAL 30 DAY DELETE;
//! ```

use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use arrow_array::{RecordBatch, ArrayRef, BooleanArray};
use arrow_array::cast::AsArray;
use tracing::{debug, info, warn};

use crate::EngineError;

/// TTL expression that defines when data expires
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TtlExpression {
    /// The column containing the timestamp
    pub column: String,
    /// Interval to add to the column value
    pub interval: TtlInterval,
    /// Action to take when data expires
    pub action: TtlAction,
}

/// Time interval for TTL calculation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TtlInterval {
    /// Number of units
    pub value: i64,
    /// Unit type (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR)
    pub unit: TtlIntervalUnit,
}

/// TTL interval unit
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlIntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TtlIntervalUnit {
    /// Convert interval to seconds
    pub fn to_seconds(&self, value: i64) -> i64 {
        match self {
            TtlIntervalUnit::Second => value,
            TtlIntervalUnit::Minute => value * 60,
            TtlIntervalUnit::Hour => value * 3600,
            TtlIntervalUnit::Day => value * 86400,
            TtlIntervalUnit::Week => value * 604800,
            TtlIntervalUnit::Month => value * 2592000, // Approximate: 30 days
            TtlIntervalUnit::Year => value * 31536000,  // Approximate: 365 days
        }
    }
}

/// Action to take when data expires
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TtlAction {
    /// Delete expired rows
    Delete,
    /// Move to a different disk/tier
    MoveToDisk(String),
    /// Move to a different volume
    MoveToVolume(String),
    /// Recompress with different codec
    Recompress(String),
    /// Aggregate expired data (for rollup)
    GroupBy {
        columns: Vec<String>,
        aggregates: HashMap<String, String>,
    },
}

impl TtlExpression {
    /// Parse a TTL expression string
    /// Formats:
    /// - "column + INTERVAL N UNIT"
    /// - "column + INTERVAL N UNIT DELETE"
    /// - "column + INTERVAL N UNIT TO DISK 'name'"
    /// - "column + INTERVAL N UNIT RECOMPRESS CODEC(name)"
    pub fn parse(expr: &str) -> Result<Self, EngineError> {
        let expr = expr.trim();

        // Split on '+' to get column and interval parts
        let parts: Vec<&str> = expr.splitn(2, '+').collect();
        if parts.len() != 2 {
            return Err(EngineError::InvalidArgument(
                format!("Invalid TTL expression: expected 'column + INTERVAL ...', got '{}'", expr)
            ));
        }

        let column = parts[0].trim().to_string();
        let remainder = parts[1].trim();
        let remainder_upper = remainder.to_uppercase();

        // Parse INTERVAL N UNIT
        if !remainder_upper.starts_with("INTERVAL") {
            return Err(EngineError::InvalidArgument(
                format!("Invalid TTL expression: expected 'INTERVAL', got '{}'", remainder)
            ));
        }

        // Use original case for value extraction (for disk/volume names)
        let after_interval_orig = &remainder[8..].trim(); // "INTERVAL" is 8 chars
        let after_interval = remainder_upper.strip_prefix("INTERVAL").unwrap().trim();
        let tokens: Vec<&str> = after_interval.split_whitespace().collect();
        let tokens_orig: Vec<&str> = after_interval_orig.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(EngineError::InvalidArgument(
                "Invalid TTL expression: expected 'INTERVAL N UNIT'".into()
            ));
        }

        let value: i64 = tokens[0].parse().map_err(|_| {
            EngineError::InvalidArgument(format!("Invalid interval value: {}", tokens[0]))
        })?;

        let unit = match tokens[1] {
            "SECOND" | "SECONDS" => TtlIntervalUnit::Second,
            "MINUTE" | "MINUTES" => TtlIntervalUnit::Minute,
            "HOUR" | "HOURS" => TtlIntervalUnit::Hour,
            "DAY" | "DAYS" => TtlIntervalUnit::Day,
            "WEEK" | "WEEKS" => TtlIntervalUnit::Week,
            "MONTH" | "MONTHS" => TtlIntervalUnit::Month,
            "YEAR" | "YEARS" => TtlIntervalUnit::Year,
            other => {
                return Err(EngineError::InvalidArgument(
                    format!("Invalid interval unit: {}", other)
                ));
            }
        };

        // Parse action (default is DELETE)
        // Use original tokens for preserving case of disk/volume names
        let action = if tokens.len() > 2 {
            match tokens[2] {
                "DELETE" => TtlAction::Delete,
                "TO" if tokens.len() > 4 && tokens[3] == "DISK" => {
                    // Use original case-preserved name
                    let disk_name = tokens_orig.get(4).unwrap_or(&"").trim_matches('\'').to_string();
                    TtlAction::MoveToDisk(disk_name)
                }
                "TO" if tokens.len() > 4 && tokens[3] == "VOLUME" => {
                    // Use original case-preserved name
                    let volume_name = tokens_orig.get(4).unwrap_or(&"").trim_matches('\'').to_string();
                    TtlAction::MoveToVolume(volume_name)
                }
                "RECOMPRESS" if tokens.len() > 3 => {
                    // Use original case for codec name
                    let codec = tokens_orig[3..].join(" ");
                    TtlAction::Recompress(codec)
                }
                _ => TtlAction::Delete,
            }
        } else {
            TtlAction::Delete
        };

        Ok(TtlExpression {
            column,
            interval: TtlInterval { value, unit },
            action,
        })
    }

    /// Calculate expiration timestamp for a given base timestamp
    pub fn expiration_time(&self, base_timestamp: i64) -> i64 {
        base_timestamp + self.interval.unit.to_seconds(self.interval.value)
    }

    /// Check if a row has expired based on its timestamp
    pub fn is_expired(&self, row_timestamp: i64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        self.expiration_time(row_timestamp) < now
    }

    /// Check if a row has expired based on a custom reference time
    pub fn is_expired_at(&self, row_timestamp: i64, reference_time: i64) -> bool {
        self.expiration_time(row_timestamp) < reference_time
    }
}

/// TTL configuration for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfig {
    /// List of TTL expressions (multiple TTLs can be defined)
    pub expressions: Vec<TtlExpression>,
    /// Minimum age before data is considered for TTL (prevents deleting just-written data)
    pub min_age_seconds: u64,
    /// How often to check for expired data (in seconds)
    pub check_interval_seconds: u64,
}

impl Default for TtlConfig {
    fn default() -> Self {
        TtlConfig {
            expressions: Vec::new(),
            min_age_seconds: 0,
            check_interval_seconds: 3600, // Check hourly by default
        }
    }
}

impl TtlConfig {
    /// Parse a TTL config from a string
    pub fn parse(ttl_str: &str) -> Result<Self, EngineError> {
        // Split by "," to get multiple TTL expressions
        let expressions: Result<Vec<_>, _> = ttl_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(TtlExpression::parse)
            .collect();

        Ok(TtlConfig {
            expressions: expressions?,
            ..Default::default()
        })
    }

    /// Check if this config has any TTL expressions
    pub fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    /// Get the primary TTL expression (first one, used for DELETE)
    pub fn primary(&self) -> Option<&TtlExpression> {
        self.expressions.first()
    }
}

/// Filter a RecordBatch to remove expired rows based on TTL
pub fn filter_expired_rows(
    batch: &RecordBatch,
    ttl: &TtlExpression,
) -> Result<RecordBatch, EngineError> {
    let schema = batch.schema();

    // Find the TTL column
    let col_idx = schema.index_of(&ttl.column).map_err(|_| {
        EngineError::InvalidArgument(format!("TTL column '{}' not found", ttl.column))
    })?;

    let col = batch.column(col_idx);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Build a boolean mask for rows that are NOT expired
    let mask = build_ttl_mask(col, ttl, now)?;

    // Apply the mask to filter out expired rows
    let filtered_columns: Result<Vec<ArrayRef>, _> = batch
        .columns()
        .iter()
        .map(|col| {
            arrow::compute::filter(col.as_ref(), &mask)
                .map_err(|e| EngineError::Internal(format!("Filter error: {}", e)))
        })
        .collect();

    RecordBatch::try_new(schema, filtered_columns?)
        .map_err(|e| EngineError::Internal(format!("Batch creation error: {}", e)))
}

/// Build a boolean mask where true = row NOT expired (should be kept)
fn build_ttl_mask(
    col: &ArrayRef,
    ttl: &TtlExpression,
    now: i64,
) -> Result<BooleanArray, EngineError> {
    use arrow_array::{Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray};

    let expiration_threshold = now - ttl.interval.unit.to_seconds(ttl.interval.value);

    // Extract timestamps and check expiration
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        // Assume Unix timestamps in seconds
        let mask: BooleanArray = arr
            .iter()
            .map(|v| v.map(|ts| ts > expiration_threshold))
            .collect();
        Ok(mask)
    } else if let Some(arr) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        let mask: BooleanArray = arr
            .iter()
            .map(|v| v.map(|ts| ts > expiration_threshold))
            .collect();
        Ok(mask)
    } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        // Convert milliseconds to seconds for comparison
        let threshold_ms = expiration_threshold * 1000;
        let mask: BooleanArray = arr
            .iter()
            .map(|v| v.map(|ts| ts > threshold_ms))
            .collect();
        Ok(mask)
    } else if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        // Convert microseconds to seconds for comparison
        let threshold_us = expiration_threshold * 1_000_000;
        let mask: BooleanArray = arr
            .iter()
            .map(|v| v.map(|ts| ts > threshold_us))
            .collect();
        Ok(mask)
    } else {
        Err(EngineError::InvalidArgument(format!(
            "TTL column '{}' must be a timestamp or integer type",
            ttl.column
        )))
    }
}

/// Statistics for TTL cleanup operations
#[derive(Debug, Default)]
pub struct TtlStats {
    /// Total rows scanned
    pub rows_scanned: AtomicU64,
    /// Total rows expired/deleted
    pub rows_expired: AtomicU64,
    /// Total segments processed
    pub segments_processed: AtomicU64,
    /// Total segments with expired data
    pub segments_with_expired: AtomicU64,
    /// Last cleanup time (Unix timestamp)
    pub last_cleanup_time: AtomicU64,
    /// Whether cleanup is currently running
    pub cleanup_running: AtomicBool,
}

impl TtlStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_scan(&self, rows: u64) {
        self.rows_scanned.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_expired(&self, rows: u64) {
        self.rows_expired.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_segment(&self, had_expired: bool) {
        self.segments_processed.fetch_add(1, Ordering::Relaxed);
        if had_expired {
            self.segments_with_expired.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn start_cleanup(&self) -> bool {
        self.cleanup_running.compare_exchange(
            false, true,
            Ordering::SeqCst,
            Ordering::Relaxed
        ).is_ok()
    }

    pub fn finish_cleanup(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_cleanup_time.store(now, Ordering::Relaxed);
        self.cleanup_running.store(false, Ordering::Relaxed);
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows_scanned": self.rows_scanned.load(Ordering::Relaxed),
            "rows_expired": self.rows_expired.load(Ordering::Relaxed),
            "segments_processed": self.segments_processed.load(Ordering::Relaxed),
            "segments_with_expired": self.segments_with_expired.load(Ordering::Relaxed),
            "last_cleanup_time": self.last_cleanup_time.load(Ordering::Relaxed),
            "cleanup_running": self.cleanup_running.load(Ordering::Relaxed),
        })
    }
}

/// TTL Manager for coordinating TTL cleanup across tables
pub struct TtlManager {
    /// TTL configurations per table (database.table -> TtlConfig)
    configs: parking_lot::RwLock<HashMap<String, TtlConfig>>,
    /// Statistics
    stats: TtlStats,
    /// Whether the manager is enabled
    enabled: AtomicBool,
}

impl TtlManager {
    pub fn new() -> Self {
        TtlManager {
            configs: parking_lot::RwLock::new(HashMap::new()),
            stats: TtlStats::new(),
            enabled: AtomicBool::new(true),
        }
    }

    /// Register a table's TTL configuration
    pub fn register_table(&self, table_name: &str, config: TtlConfig) {
        if !config.is_empty() {
            info!("Registered TTL for table '{}': {:?}", table_name, config);
            self.configs.write().insert(table_name.to_string(), config);
        }
    }

    /// Unregister a table's TTL configuration
    pub fn unregister_table(&self, table_name: &str) {
        self.configs.write().remove(table_name);
    }

    /// Get TTL configuration for a table
    pub fn get_config(&self, table_name: &str) -> Option<TtlConfig> {
        self.configs.read().get(table_name).cloned()
    }

    /// Check if a table has TTL configured
    pub fn has_ttl(&self, table_name: &str) -> bool {
        self.configs.read().contains_key(table_name)
    }

    /// Get all tables with TTL configured
    pub fn tables_with_ttl(&self) -> Vec<String> {
        self.configs.read().keys().cloned().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> &TtlStats {
        &self.stats
    }

    /// Enable/disable TTL processing
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Check if TTL processing is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

impl Default for TtlManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_ttl() {
        let ttl = TtlExpression::parse("timestamp + INTERVAL 30 DAY").unwrap();
        assert_eq!(ttl.column, "timestamp");
        assert_eq!(ttl.interval.value, 30);
        assert_eq!(ttl.interval.unit, TtlIntervalUnit::Day);
        assert_eq!(ttl.action, TtlAction::Delete);
    }

    #[test]
    fn test_parse_ttl_with_delete() {
        let ttl = TtlExpression::parse("created_at + INTERVAL 7 DAYS DELETE").unwrap();
        assert_eq!(ttl.column, "created_at");
        assert_eq!(ttl.interval.value, 7);
        assert_eq!(ttl.interval.unit, TtlIntervalUnit::Day);
        assert_eq!(ttl.action, TtlAction::Delete);
    }

    #[test]
    fn test_parse_ttl_with_disk() {
        let ttl = TtlExpression::parse("timestamp + INTERVAL 1 MONTH TO DISK 'cold'").unwrap();
        assert_eq!(ttl.column, "timestamp");
        assert_eq!(ttl.interval.value, 1);
        assert_eq!(ttl.interval.unit, TtlIntervalUnit::Month);
        assert_eq!(ttl.action, TtlAction::MoveToDisk("cold".to_string()));
    }

    #[test]
    fn test_parse_ttl_hours() {
        let ttl = TtlExpression::parse("event_time + INTERVAL 24 HOURS").unwrap();
        assert_eq!(ttl.interval.value, 24);
        assert_eq!(ttl.interval.unit, TtlIntervalUnit::Hour);
    }

    #[test]
    fn test_ttl_expiration() {
        let ttl = TtlExpression::parse("ts + INTERVAL 1 HOUR").unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Data from 2 hours ago should be expired
        assert!(ttl.is_expired(now - 7200));

        // Data from 30 minutes ago should NOT be expired
        assert!(!ttl.is_expired(now - 1800));
    }

    #[test]
    fn test_interval_to_seconds() {
        assert_eq!(TtlIntervalUnit::Second.to_seconds(1), 1);
        assert_eq!(TtlIntervalUnit::Minute.to_seconds(1), 60);
        assert_eq!(TtlIntervalUnit::Hour.to_seconds(1), 3600);
        assert_eq!(TtlIntervalUnit::Day.to_seconds(1), 86400);
        assert_eq!(TtlIntervalUnit::Week.to_seconds(1), 604800);
    }

    #[test]
    fn test_ttl_config_parse() {
        let config = TtlConfig::parse("timestamp + INTERVAL 30 DAY").unwrap();
        assert_eq!(config.expressions.len(), 1);
        assert!(!config.is_empty());
    }

    #[test]
    fn test_ttl_config_multiple() {
        let config = TtlConfig::parse(
            "timestamp + INTERVAL 7 DAY TO DISK 'cold', timestamp + INTERVAL 30 DAY DELETE"
        ).unwrap();
        assert_eq!(config.expressions.len(), 2);
    }
}
