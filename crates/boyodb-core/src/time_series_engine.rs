//! Time Series Engine for BoyoDB
//!
//! Specialized time series capabilities:
//! - Time-based aggregations and windowing
//! - Gap filling and interpolation
//! - Downsampling and retention policies
//! - Forecasting and trend analysis

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Time series data point
#[derive(Debug, Clone)]
pub struct DataPoint {
    /// Timestamp in milliseconds
    pub timestamp: i64,
    /// Value
    pub value: f64,
    /// Optional tags
    pub tags: HashMap<String, String>,
}

/// Time series
#[derive(Debug, Clone)]
pub struct TimeSeries {
    /// Metric name
    pub name: String,
    /// Data points (sorted by timestamp)
    pub points: Vec<DataPoint>,
    /// Tags common to all points
    pub tags: HashMap<String, String>,
}

impl TimeSeries {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            points: Vec::new(),
            tags: HashMap::new(),
        }
    }

    pub fn add_point(&mut self, timestamp: i64, value: f64) {
        self.points.push(DataPoint {
            timestamp,
            value,
            tags: HashMap::new(),
        });
    }

    pub fn sort(&mut self) {
        self.points.sort_by_key(|p| p.timestamp);
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}

/// Time bucket/interval
#[derive(Debug, Clone, Copy)]
pub enum TimeBucket {
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
    Days(u64),
    Weeks(u64),
    Months(u64),
}

impl TimeBucket {
    /// Get bucket duration in milliseconds
    pub fn duration_ms(&self) -> i64 {
        match self {
            TimeBucket::Seconds(n) => *n as i64 * 1000,
            TimeBucket::Minutes(n) => *n as i64 * 60 * 1000,
            TimeBucket::Hours(n) => *n as i64 * 3600 * 1000,
            TimeBucket::Days(n) => *n as i64 * 86400 * 1000,
            TimeBucket::Weeks(n) => *n as i64 * 7 * 86400 * 1000,
            TimeBucket::Months(n) => *n as i64 * 30 * 86400 * 1000,
        }
    }

    /// Get bucket start for a timestamp
    pub fn bucket_start(&self, timestamp: i64) -> i64 {
        let duration = self.duration_ms();
        (timestamp / duration) * duration
    }
}

/// Aggregation functions for time series
#[derive(Debug, Clone, Copy)]
pub enum TimeAggregation {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
    StdDev,
    Variance,
    Percentile(f64),
    Rate,     // Change per second
    Increase, // Total increase (for counters)
    Delta,    // Difference from previous
}

/// Gap filling strategies
#[derive(Debug, Clone, Copy)]
pub enum GapFillStrategy {
    /// Fill with null/NaN
    Null,
    /// Fill with previous value
    Previous,
    /// Fill with next value
    Next,
    /// Linear interpolation
    Linear,
    /// Fill with constant value
    Constant(f64),
    /// Fill with average of neighbors
    Average,
}

/// Interpolation methods
#[derive(Debug, Clone, Copy)]
pub enum InterpolationMethod {
    Linear,
    Cubic,
    Spline,
    Step,
    None,
}

/// Downsampling policy
#[derive(Debug, Clone)]
pub struct DownsamplePolicy {
    /// Source retention
    pub source_retention: TimeBucket,
    /// Target bucket size
    pub target_bucket: TimeBucket,
    /// Aggregation function
    pub aggregation: TimeAggregation,
    /// Target retention period
    pub target_retention: Option<TimeBucket>,
}

/// Time range
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

impl TimeRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    pub fn duration_ms(&self) -> i64 {
        self.end - self.start
    }

    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }
}

/// Time series query
#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    /// Metric name pattern
    pub metric: String,
    /// Time range
    pub range: TimeRange,
    /// Bucket size for aggregation
    pub bucket: Option<TimeBucket>,
    /// Aggregation function
    pub aggregation: Option<TimeAggregation>,
    /// Gap fill strategy
    pub gap_fill: Option<GapFillStrategy>,
    /// Tag filters
    pub filters: HashMap<String, String>,
    /// Limit number of points
    pub limit: Option<usize>,
}

/// Aggregated bucket
#[derive(Debug, Clone)]
pub struct AggregatedBucket {
    pub timestamp: i64,
    pub value: f64,
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
}

/// Time series functions
pub struct TimeSeriesFunctions;

impl TimeSeriesFunctions {
    /// Aggregate time series into buckets
    pub fn aggregate(series: &TimeSeries, bucket: TimeBucket, agg: TimeAggregation) -> TimeSeries {
        if series.is_empty() {
            return TimeSeries::new(&series.name);
        }

        let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();

        for point in &series.points {
            let bucket_start = bucket.bucket_start(point.timestamp);
            buckets.entry(bucket_start).or_default().push(point.value);
        }

        let mut result = TimeSeries::new(&series.name);
        let mut bucket_times: Vec<i64> = buckets.keys().copied().collect();
        bucket_times.sort();

        for ts in bucket_times {
            let values = &buckets[&ts];
            let value = Self::apply_aggregation(values, agg);
            result.add_point(ts, value);
        }

        result
    }

    /// Apply aggregation function to values
    fn apply_aggregation(values: &[f64], agg: TimeAggregation) -> f64 {
        if values.is_empty() {
            return f64::NAN;
        }

        match agg {
            TimeAggregation::Sum => values.iter().sum(),
            TimeAggregation::Avg => values.iter().sum::<f64>() / values.len() as f64,
            TimeAggregation::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
            TimeAggregation::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            TimeAggregation::Count => values.len() as f64,
            TimeAggregation::First => values[0],
            TimeAggregation::Last => values[values.len() - 1],
            TimeAggregation::StdDev => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                variance.sqrt()
            }
            TimeAggregation::Variance => {
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
            }
            TimeAggregation::Percentile(p) => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
                sorted[idx]
            }
            TimeAggregation::Rate => {
                if values.len() < 2 {
                    0.0
                } else {
                    (values[values.len() - 1] - values[0]) / values.len() as f64
                }
            }
            TimeAggregation::Increase => {
                if values.len() < 2 {
                    0.0
                } else {
                    values[values.len() - 1] - values[0]
                }
            }
            TimeAggregation::Delta => {
                if values.len() < 2 {
                    0.0
                } else {
                    values[values.len() - 1] - values[values.len() - 2]
                }
            }
        }
    }

    /// Fill gaps in time series
    pub fn gap_fill(
        series: &TimeSeries,
        bucket: TimeBucket,
        strategy: GapFillStrategy,
    ) -> TimeSeries {
        if series.points.len() < 2 {
            return series.clone();
        }

        let mut result = TimeSeries::new(&series.name);
        let duration = bucket.duration_ms();

        let start = bucket.bucket_start(series.points[0].timestamp);
        let end = bucket.bucket_start(series.points[series.points.len() - 1].timestamp);

        // Create map of existing values
        let mut value_map: HashMap<i64, f64> = HashMap::new();
        for point in &series.points {
            let bucket_start = bucket.bucket_start(point.timestamp);
            value_map.insert(bucket_start, point.value);
        }

        let mut current = start;
        let mut prev_value: Option<f64> = None;

        while current <= end {
            let value = if let Some(&v) = value_map.get(&current) {
                prev_value = Some(v);
                v
            } else {
                match strategy {
                    GapFillStrategy::Null => f64::NAN,
                    GapFillStrategy::Previous => prev_value.unwrap_or(f64::NAN),
                    GapFillStrategy::Next => {
                        // Look for next value
                        let mut next = current + duration;
                        while next <= end {
                            if let Some(&v) = value_map.get(&next) {
                                break;
                            }
                            next += duration;
                        }
                        value_map.get(&next).copied().unwrap_or(f64::NAN)
                    }
                    GapFillStrategy::Linear => {
                        // Linear interpolation
                        if let Some(pv) = prev_value {
                            let mut next = current + duration;
                            while next <= end && !value_map.contains_key(&next) {
                                next += duration;
                            }
                            if let Some(&nv) = value_map.get(&next) {
                                let ratio = 0.5; // Simplified
                                pv + (nv - pv) * ratio
                            } else {
                                pv
                            }
                        } else {
                            f64::NAN
                        }
                    }
                    GapFillStrategy::Constant(c) => c,
                    GapFillStrategy::Average => {
                        let sum: f64 = value_map.values().sum();
                        sum / value_map.len() as f64
                    }
                }
            };

            result.add_point(current, value);
            current += duration;
        }

        result
    }

    /// Downsample time series
    pub fn downsample(series: &TimeSeries, policy: &DownsamplePolicy) -> TimeSeries {
        Self::aggregate(series, policy.target_bucket, policy.aggregation)
    }

    /// Calculate rate of change
    pub fn rate(series: &TimeSeries) -> TimeSeries {
        let mut result = TimeSeries::new(&format!("rate({})", series.name));

        for i in 1..series.points.len() {
            let dt = (series.points[i].timestamp - series.points[i - 1].timestamp) as f64 / 1000.0;
            let dv = series.points[i].value - series.points[i - 1].value;
            let rate = if dt > 0.0 { dv / dt } else { 0.0 };
            result.add_point(series.points[i].timestamp, rate);
        }

        result
    }

    /// Calculate derivative
    pub fn derivative(series: &TimeSeries) -> TimeSeries {
        Self::rate(series)
    }

    /// Calculate integral (cumulative sum)
    pub fn integral(series: &TimeSeries) -> TimeSeries {
        let mut result = TimeSeries::new(&format!("integral({})", series.name));
        let mut cumsum = 0.0;

        for point in &series.points {
            cumsum += point.value;
            result.add_point(point.timestamp, cumsum);
        }

        result
    }

    /// Moving average
    pub fn moving_average(series: &TimeSeries, window_size: usize) -> TimeSeries {
        let mut result = TimeSeries::new(&format!("ma{}({})", window_size, series.name));

        for i in 0..series.points.len() {
            let start = if i >= window_size {
                i - window_size + 1
            } else {
                0
            };
            let window: Vec<f64> = series.points[start..=i].iter().map(|p| p.value).collect();
            let avg = window.iter().sum::<f64>() / window.len() as f64;
            result.add_point(series.points[i].timestamp, avg);
        }

        result
    }

    /// Exponential moving average
    pub fn exponential_moving_average(series: &TimeSeries, alpha: f64) -> TimeSeries {
        let mut result = TimeSeries::new(&format!("ema({})", series.name));

        if series.is_empty() {
            return result;
        }

        let mut ema = series.points[0].value;
        result.add_point(series.points[0].timestamp, ema);

        for point in series.points.iter().skip(1) {
            ema = alpha * point.value + (1.0 - alpha) * ema;
            result.add_point(point.timestamp, ema);
        }

        result
    }

    /// Detect anomalies using z-score
    pub fn detect_anomalies(series: &TimeSeries, threshold: f64) -> Vec<(i64, f64, f64)> {
        if series.points.len() < 3 {
            return vec![];
        }

        let values: Vec<f64> = series.points.iter().map(|p| p.value).collect();
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let std =
            (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64).sqrt();

        if std == 0.0 {
            return vec![];
        }

        series
            .points
            .iter()
            .filter_map(|p| {
                let z_score = (p.value - mean) / std;
                if z_score.abs() > threshold {
                    Some((p.timestamp, p.value, z_score))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Simple forecasting using linear regression
    pub fn forecast(series: &TimeSeries, periods: usize) -> TimeSeries {
        if series.points.len() < 2 {
            return TimeSeries::new(&format!("forecast({})", series.name));
        }

        // Simple linear regression
        let n = series.points.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = series.points.iter().map(|p| p.value).sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for (i, point) in series.points.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (point.value - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        let slope = if denominator != 0.0 {
            numerator / denominator
        } else {
            0.0
        };
        let intercept = y_mean - slope * x_mean;

        // Get time interval
        let interval = if series.points.len() >= 2 {
            series.points[1].timestamp - series.points[0].timestamp
        } else {
            1000
        };

        let last_ts = series.points.last().unwrap().timestamp;
        let last_idx = series.points.len();

        let mut result = TimeSeries::new(&format!("forecast({})", series.name));

        for i in 0..periods {
            let x = (last_idx + i) as f64;
            let y = slope * x + intercept;
            let ts = last_ts + (i as i64 + 1) * interval;
            result.add_point(ts, y);
        }

        result
    }

    /// Seasonal decomposition (simplified)
    pub fn decompose(series: &TimeSeries, period: usize) -> (TimeSeries, TimeSeries, TimeSeries) {
        let trend = Self::moving_average(series, period);

        let mut seasonal = TimeSeries::new(&format!("seasonal({})", series.name));
        let mut residual = TimeSeries::new(&format!("residual({})", series.name));

        for (i, point) in series.points.iter().enumerate() {
            let trend_val = if i < trend.points.len() {
                trend.points[i].value
            } else {
                point.value
            };

            let seasonal_val = point.value - trend_val;
            let residual_val = 0.0; // Simplified

            seasonal.add_point(point.timestamp, seasonal_val);
            residual.add_point(point.timestamp, residual_val);
        }

        (trend, seasonal, residual)
    }
}

/// Time series store
pub struct TimeSeriesStore {
    series: RwLock<HashMap<String, TimeSeries>>,
    retention_policies: RwLock<HashMap<String, DownsamplePolicy>>,
}

impl TimeSeriesStore {
    pub fn new() -> Self {
        Self {
            series: RwLock::new(HashMap::new()),
            retention_policies: RwLock::new(HashMap::new()),
        }
    }

    /// Write a data point
    pub fn write(&self, metric: &str, timestamp: i64, value: f64, tags: HashMap<String, String>) {
        let mut series = self.series.write();
        let ts = series
            .entry(metric.to_string())
            .or_insert_with(|| TimeSeries::new(metric));
        ts.points.push(DataPoint {
            timestamp,
            value,
            tags,
        });
    }

    /// Query time series
    pub fn query(&self, query: &TimeSeriesQuery) -> Vec<TimeSeries> {
        let series = self.series.read();
        let mut results = Vec::new();

        for (name, ts) in series.iter() {
            if !name.contains(&query.metric) && query.metric != "*" {
                continue;
            }

            // Filter by tags
            let mut matches = true;
            for (key, value) in &query.filters {
                if ts.tags.get(key) != Some(value) {
                    matches = false;
                    break;
                }
            }

            if !matches {
                continue;
            }

            // Filter by time range
            let filtered: Vec<DataPoint> = ts
                .points
                .iter()
                .filter(|p| query.range.contains(p.timestamp))
                .cloned()
                .collect();

            let mut result = TimeSeries {
                name: name.clone(),
                points: filtered,
                tags: ts.tags.clone(),
            };

            // Apply aggregation
            if let (Some(bucket), Some(agg)) = (query.bucket, query.aggregation) {
                result = TimeSeriesFunctions::aggregate(&result, bucket, agg);
            }

            // Apply gap fill
            if let (Some(bucket), Some(strategy)) = (query.bucket, query.gap_fill) {
                result = TimeSeriesFunctions::gap_fill(&result, bucket, strategy);
            }

            // Apply limit
            if let Some(limit) = query.limit {
                result.points.truncate(limit);
            }

            results.push(result);
        }

        results
    }

    /// Set retention policy
    pub fn set_retention_policy(&self, metric: &str, policy: DownsamplePolicy) {
        self.retention_policies
            .write()
            .insert(metric.to_string(), policy);
    }

    /// Apply retention policies
    pub fn apply_retention(&self) {
        let policies = self.retention_policies.read().clone();
        let mut series = self.series.write();

        for (metric, policy) in policies {
            if let Some(ts) = series.get_mut(&metric) {
                let downsampled = TimeSeriesFunctions::downsample(ts, &policy);
                *ts = downsampled;
            }
        }
    }
}

impl Default for TimeSeriesStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_series() -> TimeSeries {
        let mut ts = TimeSeries::new("test");
        for i in 0..100 {
            ts.add_point(i * 1000, (i as f64).sin() * 100.0 + 50.0);
        }
        ts
    }

    #[test]
    fn test_aggregation() {
        let series = create_test_series();
        let result =
            TimeSeriesFunctions::aggregate(&series, TimeBucket::Seconds(10), TimeAggregation::Avg);
        assert!(result.points.len() < series.points.len());
    }

    #[test]
    fn test_gap_fill() {
        let mut series = TimeSeries::new("test");
        series.add_point(0, 10.0);
        series.add_point(3000, 40.0);

        let filled =
            TimeSeriesFunctions::gap_fill(&series, TimeBucket::Seconds(1), GapFillStrategy::Linear);
        assert!(filled.points.len() > series.points.len());
    }

    #[test]
    fn test_moving_average() {
        let series = create_test_series();
        let ma = TimeSeriesFunctions::moving_average(&series, 5);
        assert_eq!(ma.points.len(), series.points.len());
    }

    #[test]
    fn test_rate() {
        let mut series = TimeSeries::new("counter");
        series.add_point(0, 0.0);
        series.add_point(1000, 10.0);
        series.add_point(2000, 30.0);

        let rate = TimeSeriesFunctions::rate(&series);
        assert_eq!(rate.points.len(), 2);
        assert!((rate.points[0].value - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_forecast() {
        let mut series = TimeSeries::new("linear");
        for i in 0..10 {
            series.add_point(i * 1000, i as f64 * 2.0);
        }

        let forecast = TimeSeriesFunctions::forecast(&series, 5);
        assert_eq!(forecast.points.len(), 5);
        // Should continue linear trend
        assert!(forecast.points[0].value > 18.0);
    }

    #[test]
    fn test_anomaly_detection() {
        let mut series = TimeSeries::new("test");
        for i in 0..100 {
            let value = if i == 50 { 1000.0 } else { 10.0 };
            series.add_point(i * 1000, value);
        }

        let anomalies = TimeSeriesFunctions::detect_anomalies(&series, 3.0);
        assert!(!anomalies.is_empty());
    }

    #[test]
    fn test_time_bucket() {
        let bucket = TimeBucket::Minutes(5);
        assert_eq!(bucket.duration_ms(), 5 * 60 * 1000);

        let ts = 1234567890123_i64;
        let start = bucket.bucket_start(ts);
        assert!(start <= ts);
        assert!(ts - start < bucket.duration_ms());
    }
}
