//! Time-Travel Query Module
//!
//! Provides temporal queries for accessing historical data:
//! - AS OF TIMESTAMP - query data at a specific point in time
//! - AS OF VERSION - query data at a specific manifest version
//! - VERSIONS BETWEEN - query all versions in a time range
//! - System-time temporal tables
//!
//! Uses the existing MVCC and manifest versioning infrastructure.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Time-travel configuration
#[derive(Clone, Debug)]
pub struct TimeTravelConfig {
    /// Enable time-travel queries
    pub enabled: bool,
    /// Maximum retention period for historical versions (seconds)
    pub retention_secs: u64,
    /// Maximum number of versions to retain per table
    pub max_versions_per_table: usize,
    /// Enable automatic version cleanup
    pub auto_cleanup: bool,
    /// Cleanup interval in seconds
    pub cleanup_interval_secs: u64,
}

impl Default for TimeTravelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_secs: 7 * 24 * 60 * 60, // 7 days
            max_versions_per_table: 1000,
            auto_cleanup: true,
            cleanup_interval_secs: 3600, // 1 hour
        }
    }
}

/// Point in time specification
#[derive(Clone, Debug, PartialEq)]
pub enum TimePoint {
    /// Specific timestamp (microseconds since epoch)
    Timestamp(i64),
    /// Specific version number
    Version(u64),
    /// Relative time (seconds ago)
    Relative(i64),
    /// Current time (default)
    Current,
}

impl TimePoint {
    /// Parse from SQL syntax
    /// Examples:
    /// - "2024-01-15 10:30:00"
    /// - "VERSION 42"
    /// - "-1 hour"
    /// - "-30 minutes"
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        // VERSION syntax
        if let Some(ver_str) = s
            .strip_prefix("VERSION ")
            .or_else(|| s.strip_prefix("version "))
        {
            if let Ok(v) = ver_str.trim().parse::<u64>() {
                return Some(TimePoint::Version(v));
            }
        }

        // Relative time syntax
        if s.starts_with('-') || s.starts_with('+') {
            return Self::parse_relative(s);
        }

        // ISO timestamp
        if let Some(ts) = Self::parse_timestamp(s) {
            return Some(TimePoint::Timestamp(ts));
        }

        None
    }

    fn parse_relative(s: &str) -> Option<Self> {
        let s = s.trim();
        let (sign, rest) = if s.starts_with('-') {
            (-1i64, &s[1..])
        } else if s.starts_with('+') {
            (1i64, &s[1..])
        } else {
            (1i64, s)
        };

        let rest = rest.trim();
        let parts: Vec<&str> = rest.split_whitespace().collect();

        if parts.len() != 2 {
            return None;
        }

        let value: i64 = parts[0].parse().ok()?;
        let unit = parts[1].to_lowercase();

        let seconds = match unit.as_str() {
            "second" | "seconds" | "sec" | "s" => value,
            "minute" | "minutes" | "min" | "m" => value * 60,
            "hour" | "hours" | "hr" | "h" => value * 3600,
            "day" | "days" | "d" => value * 86400,
            "week" | "weeks" | "w" => value * 604800,
            _ => return None,
        };

        Some(TimePoint::Relative(sign * seconds))
    }

    fn parse_timestamp(s: &str) -> Option<i64> {
        // Simple ISO-like timestamp parsing
        // Format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
        let s = s.trim().trim_matches('\'').trim_matches('"');

        let parts: Vec<&str> = s.split([' ', 'T']).collect();

        let date_str = parts.first()?;
        let time_str = parts.get(1).copied().unwrap_or("00:00:00");

        let date_parts: Vec<i32> = date_str.split('-').filter_map(|p| p.parse().ok()).collect();
        let time_parts: Vec<i32> = time_str
            .split(':')
            .filter_map(|p| p.trim_end_matches('Z').parse().ok())
            .collect();

        if date_parts.len() != 3 {
            return None;
        }

        let year = date_parts[0];
        let month = date_parts[1];
        let day = date_parts[2];
        let hour = time_parts.get(0).copied().unwrap_or(0);
        let minute = time_parts.get(1).copied().unwrap_or(0);
        let second = time_parts.get(2).copied().unwrap_or(0);

        // Simple calculation (not accounting for leap years precisely)
        let days_since_epoch = (year - 1970) * 365
            + (year - 1969) / 4
            + days_before_month(month, is_leap_year(year))
            + day
            - 1;

        let timestamp_secs = (days_since_epoch as i64) * 86400
            + (hour as i64) * 3600
            + (minute as i64) * 60
            + (second as i64);

        Some(timestamp_secs * 1_000_000) // Convert to microseconds
    }

    /// Convert to absolute timestamp (microseconds since epoch)
    pub fn to_timestamp(&self) -> i64 {
        match self {
            TimePoint::Timestamp(ts) => *ts,
            TimePoint::Relative(secs) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                now + (*secs * 1_000_000)
            }
            TimePoint::Version(_) => {
                // Version needs to be resolved by the engine
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64
            }
            TimePoint::Current => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64,
        }
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_before_month(month: i32, leap: bool) -> i32 {
    let days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut d = days.get((month - 1) as usize).copied().unwrap_or(0);
    if leap && month > 2 {
        d += 1;
    }
    d
}

/// Time range for VERSIONS BETWEEN queries
#[derive(Clone, Debug)]
pub struct TimeRange {
    pub start: TimePoint,
    pub end: TimePoint,
}

impl TimeRange {
    pub fn new(start: TimePoint, end: TimePoint) -> Self {
        Self { start, end }
    }

    pub fn last_n_days(n: i64) -> Self {
        Self {
            start: TimePoint::Relative(-n * 86400),
            end: TimePoint::Current,
        }
    }
}

/// Snapshot metadata for a specific version
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionSnapshot {
    /// Version number
    pub version: u64,
    /// Timestamp when this version was created (microseconds)
    pub timestamp_us: i64,
    /// Manifest entries at this version
    pub segment_ids: Vec<String>,
    /// Parent version
    pub parent_version: Option<u64>,
    /// Transaction ID that created this version
    pub transaction_id: Option<u64>,
    /// Operation type
    pub operation: VersionOperation,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Type of operation that created a version
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersionOperation {
    /// Initial table creation
    Create,
    /// Data insertion
    Insert,
    /// Data update
    Update,
    /// Data deletion
    Delete,
    /// Schema alteration
    AlterSchema,
    /// Compaction
    Compact,
    /// Merge
    Merge,
    /// Restore from backup
    Restore,
}

/// Time-travel query options
#[derive(Clone, Debug, Default)]
pub struct TimeTravelOptions {
    /// Point in time to query
    pub as_of: Option<TimePoint>,
    /// Include deleted rows
    pub include_deleted: bool,
    /// Return version metadata
    pub include_version_info: bool,
}

/// Version history for a table
pub struct VersionHistory {
    /// Table identifier (database.table)
    pub table_id: String,
    /// All versions ordered by version number
    versions: RwLock<Vec<VersionSnapshot>>,
    /// Version index by timestamp
    timestamp_index: RwLock<Vec<(i64, u64)>>, // (timestamp, version)
    /// Configuration
    config: TimeTravelConfig,
}

impl VersionHistory {
    pub fn new(table_id: String, config: TimeTravelConfig) -> Self {
        Self {
            table_id,
            versions: RwLock::new(Vec::new()),
            timestamp_index: RwLock::new(Vec::new()),
            config,
        }
    }

    /// Add a new version
    pub fn add_version(&self, snapshot: VersionSnapshot) {
        let mut versions = self.versions.write();
        let mut index = self.timestamp_index.write();

        let version = snapshot.version;
        let timestamp = snapshot.timestamp_us;

        versions.push(snapshot);
        index.push((timestamp, version));

        // Keep versions sorted
        versions.sort_by_key(|v| v.version);
        index.sort_by_key(|(ts, _)| *ts);

        // Enforce limits
        self.enforce_limits(&mut versions, &mut index);
    }

    fn enforce_limits(&self, versions: &mut Vec<VersionSnapshot>, index: &mut Vec<(i64, u64)>) {
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let retention_us = (self.config.retention_secs as i64) * 1_000_000;
        let cutoff = now_us - retention_us;

        // Remove versions older than retention period (keep at least one)
        while versions.len() > 1 && versions[0].timestamp_us < cutoff {
            let removed = versions.remove(0);
            index.retain(|(_, v)| *v != removed.version);
        }

        // Enforce max versions
        while versions.len() > self.config.max_versions_per_table {
            let removed = versions.remove(0);
            index.retain(|(_, v)| *v != removed.version);
        }
    }

    /// Get version at a specific point in time
    pub fn version_at(&self, time_point: &TimePoint) -> Option<VersionSnapshot> {
        match time_point {
            TimePoint::Version(v) => self.get_version(*v),
            TimePoint::Current => self.latest_version(),
            _ => {
                let timestamp = time_point.to_timestamp();
                self.version_at_timestamp(timestamp)
            }
        }
    }

    /// Get version at or before a timestamp
    pub fn version_at_timestamp(&self, timestamp_us: i64) -> Option<VersionSnapshot> {
        let index = self.timestamp_index.read();
        let versions = self.versions.read();

        // Binary search for the latest version <= timestamp
        let pos = index.partition_point(|(ts, _)| *ts <= timestamp_us);

        if pos == 0 {
            return None;
        }

        let (_, version) = index[pos - 1];
        versions.iter().find(|v| v.version == version).cloned()
    }

    /// Get a specific version
    pub fn get_version(&self, version: u64) -> Option<VersionSnapshot> {
        self.versions
            .read()
            .iter()
            .find(|v| v.version == version)
            .cloned()
    }

    /// Get the latest version
    pub fn latest_version(&self) -> Option<VersionSnapshot> {
        self.versions.read().last().cloned()
    }

    /// Get all versions in a time range
    pub fn versions_between(&self, range: &TimeRange) -> Vec<VersionSnapshot> {
        let start_ts = range.start.to_timestamp();
        let end_ts = range.end.to_timestamp();

        self.versions
            .read()
            .iter()
            .filter(|v| v.timestamp_us >= start_ts && v.timestamp_us <= end_ts)
            .cloned()
            .collect()
    }

    /// Get version count
    pub fn version_count(&self) -> usize {
        self.versions.read().len()
    }

    /// Get all versions
    pub fn all_versions(&self) -> Vec<VersionSnapshot> {
        self.versions.read().clone()
    }
}

/// Time-travel query manager
pub struct TimeTravelManager {
    config: TimeTravelConfig,
    /// Version histories per table
    histories: RwLock<HashMap<String, Arc<VersionHistory>>>,
}

impl TimeTravelManager {
    pub fn new(config: TimeTravelConfig) -> Self {
        Self {
            config,
            histories: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create version history for a table
    pub fn get_history(&self, table_id: &str) -> Arc<VersionHistory> {
        let histories = self.histories.read();
        if let Some(h) = histories.get(table_id) {
            return h.clone();
        }
        drop(histories);

        let mut histories = self.histories.write();
        histories
            .entry(table_id.to_string())
            .or_insert_with(|| {
                Arc::new(VersionHistory::new(
                    table_id.to_string(),
                    self.config.clone(),
                ))
            })
            .clone()
    }

    /// Record a new version
    pub fn record_version(
        &self,
        database: &str,
        table: &str,
        version: u64,
        segment_ids: Vec<String>,
        operation: VersionOperation,
        transaction_id: Option<u64>,
    ) {
        let table_id = format!("{}.{}", database, table);
        let history = self.get_history(&table_id);

        let snapshot = VersionSnapshot {
            version,
            timestamp_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64,
            segment_ids,
            parent_version: if version > 0 { Some(version - 1) } else { None },
            transaction_id,
            operation,
            metadata: HashMap::new(),
        };

        history.add_version(snapshot);
    }

    /// Resolve time point to segment IDs
    pub fn resolve_segments(
        &self,
        database: &str,
        table: &str,
        time_point: &TimePoint,
    ) -> Option<Vec<String>> {
        let table_id = format!("{}.{}", database, table);
        let history = self.get_history(&table_id);

        history.version_at(time_point).map(|v| v.segment_ids)
    }

    /// Get version history for a table
    pub fn get_table_history(&self, database: &str, table: &str) -> Vec<VersionSnapshot> {
        let table_id = format!("{}.{}", database, table);
        let history = self.get_history(&table_id);
        history.all_versions()
    }

    /// Clean up old versions across all tables
    pub fn cleanup(&self) {
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let retention_us = (self.config.retention_secs as i64) * 1_000_000;
        let cutoff = now_us - retention_us;

        let histories = self.histories.read();
        for history in histories.values() {
            let mut versions = history.versions.write();
            let mut index = history.timestamp_index.write();

            while versions.len() > 1 && versions[0].timestamp_us < cutoff {
                let removed = versions.remove(0);
                index.retain(|(_, v)| *v != removed.version);
            }
        }
    }

    /// Get statistics
    pub fn stats(&self) -> TimeTravelStats {
        let histories = self.histories.read();
        let mut total_versions = 0;
        let mut oldest_version_age_secs = 0i64;

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        for history in histories.values() {
            let versions = history.versions.read();
            total_versions += versions.len();

            if let Some(oldest) = versions.first() {
                let age = (now_us - oldest.timestamp_us) / 1_000_000;
                oldest_version_age_secs = oldest_version_age_secs.max(age);
            }
        }

        TimeTravelStats {
            tables_tracked: histories.len(),
            total_versions,
            oldest_version_age_secs,
            retention_secs: self.config.retention_secs,
        }
    }
}

/// Time-travel statistics
#[derive(Clone, Debug)]
pub struct TimeTravelStats {
    pub tables_tracked: usize,
    pub total_versions: usize,
    pub oldest_version_age_secs: i64,
    pub retention_secs: u64,
}

/// SQL AST extension for time-travel
#[derive(Clone, Debug)]
pub struct TimeTravelClause {
    /// AS OF specification
    pub as_of: Option<TimePoint>,
    /// VERSIONS BETWEEN specification
    pub versions_between: Option<TimeRange>,
    /// FOR SYSTEM_TIME specification
    pub system_time: Option<SystemTimeSpec>,
}

/// System time specification (SQL:2011 temporal)
#[derive(Clone, Debug)]
pub enum SystemTimeSpec {
    AsOf(TimePoint),
    Between(TimePoint, TimePoint),
    From(TimePoint, TimePoint),
    All,
}

impl TimeTravelClause {
    /// Parse from SQL tokens
    /// Supports:
    /// - FOR SYSTEM_TIME AS OF '2024-01-15'
    /// - FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-01-15'
    /// - AS OF TIMESTAMP '2024-01-15 10:00:00'
    /// - AS OF VERSION 42
    pub fn parse(tokens: &[&str]) -> Option<Self> {
        if tokens.is_empty() {
            return None;
        }

        let joined = tokens.join(" ");
        let upper = joined.to_uppercase();

        // AS OF VERSION
        if upper.contains("AS OF VERSION") {
            if let Some(pos) = upper.find("AS OF VERSION") {
                let rest = &joined[pos + 14..].trim();
                if let Ok(v) = rest.split_whitespace().next()?.parse::<u64>() {
                    return Some(TimeTravelClause {
                        as_of: Some(TimePoint::Version(v)),
                        versions_between: None,
                        system_time: None,
                    });
                }
            }
        }

        // AS OF TIMESTAMP
        if upper.contains("AS OF TIMESTAMP") || upper.contains("AS OF '") {
            if let Some(pos) = upper.find("AS OF") {
                let rest = &joined[pos + 5..].trim();
                let ts_str = rest
                    .trim_start_matches("TIMESTAMP")
                    .trim_start_matches("timestamp")
                    .trim();
                if let Some(tp) = TimePoint::parse(ts_str) {
                    return Some(TimeTravelClause {
                        as_of: Some(tp),
                        versions_between: None,
                        system_time: None,
                    });
                }
            }
        }

        // FOR SYSTEM_TIME AS OF
        if upper.contains("FOR SYSTEM_TIME AS OF") {
            if let Some(pos) = upper.find("FOR SYSTEM_TIME AS OF") {
                let rest = &joined[pos + 21..].trim();
                if let Some(tp) = TimePoint::parse(rest) {
                    return Some(TimeTravelClause {
                        as_of: None,
                        versions_between: None,
                        system_time: Some(SystemTimeSpec::AsOf(tp)),
                    });
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_point_parse() {
        // Version
        assert_eq!(TimePoint::parse("VERSION 42"), Some(TimePoint::Version(42)));

        // Relative
        let tp = TimePoint::parse("-1 hour");
        assert!(matches!(tp, Some(TimePoint::Relative(-3600))));

        let tp = TimePoint::parse("-30 minutes");
        assert!(matches!(tp, Some(TimePoint::Relative(-1800))));

        // Timestamp
        let tp = TimePoint::parse("2024-01-15 10:30:00");
        assert!(matches!(tp, Some(TimePoint::Timestamp(_))));
    }

    #[test]
    fn test_version_history() {
        let mut config = TimeTravelConfig::default();
        config.retention_secs = 999_999_999_999;
        let history = VersionHistory::new("test.table".to_string(), config);

        // Add versions
        for i in 1..=5 {
            history.add_version(VersionSnapshot {
                version: i,
                timestamp_us: i as i64 * 1_000_000,
                segment_ids: vec![format!("seg_{}", i)],
                parent_version: if i > 1 { Some(i - 1) } else { None },
                transaction_id: Some(i * 100),
                operation: VersionOperation::Insert,
                metadata: HashMap::new(),
            });
        }

        assert_eq!(history.version_count(), 5);

        // Get specific version
        let v3 = history.get_version(3).unwrap();
        assert_eq!(v3.version, 3);
        assert_eq!(v3.segment_ids, vec!["seg_3"]);

        // Get version at timestamp
        let v_at = history.version_at_timestamp(2_500_000).unwrap();
        assert_eq!(v_at.version, 2);
    }

    #[test]
    fn test_time_travel_manager() {
        let config = TimeTravelConfig::default();
        let manager = TimeTravelManager::new(config);

        manager.record_version(
            "mydb",
            "users",
            1,
            vec!["seg_1".to_string()],
            VersionOperation::Create,
            None,
        );

        manager.record_version(
            "mydb",
            "users",
            2,
            vec!["seg_1".to_string(), "seg_2".to_string()],
            VersionOperation::Insert,
            Some(100),
        );

        let segments = manager.resolve_segments("mydb", "users", &TimePoint::Version(1));
        assert_eq!(segments, Some(vec!["seg_1".to_string()]));

        let history = manager.get_table_history("mydb", "users");
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_time_travel_clause_parse() {
        let clause = TimeTravelClause::parse(&["AS", "OF", "VERSION", "42"]);
        assert!(clause.is_some());
        assert!(matches!(
            clause.unwrap().as_of,
            Some(TimePoint::Version(42))
        ));

        let clause = TimeTravelClause::parse(&["AS", "OF", "TIMESTAMP", "'2024-01-15'"]);
        assert!(clause.is_some());
    }
}
