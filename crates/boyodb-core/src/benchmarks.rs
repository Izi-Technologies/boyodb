//! Benchmarks Module
//!
//! Provides benchmarking utilities for BoyoDB performance testing.
//! Features:
//! - Micro-benchmarks for core operations
//! - Latency histograms
//! - Throughput measurement
//! - Memory profiling helpers

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Benchmark result
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Benchmark name
    pub name: String,
    /// Total operations
    pub operations: u64,
    /// Total duration
    pub duration: Duration,
    /// Operations per second
    pub ops_per_sec: f64,
    /// Average latency
    pub avg_latency: Duration,
    /// P50 latency
    pub p50_latency: Duration,
    /// P95 latency
    pub p95_latency: Duration,
    /// P99 latency
    pub p99_latency: Duration,
    /// Maximum latency
    pub max_latency: Duration,
    /// Minimum latency
    pub min_latency: Duration,
    /// Bytes processed (if applicable)
    pub bytes_processed: Option<u64>,
    /// Throughput in MB/s (if bytes_processed is set)
    pub throughput_mbps: Option<f64>,
}

impl BenchmarkResult {
    /// Format as human-readable string
    pub fn format(&self) -> String {
        let mut s = format!(
            "Benchmark: {}\n\
             Operations: {}\n\
             Duration: {:.2?}\n\
             Ops/sec: {:.2}\n\
             Avg latency: {:.2?}\n\
             P50 latency: {:.2?}\n\
             P95 latency: {:.2?}\n\
             P99 latency: {:.2?}\n\
             Max latency: {:.2?}\n\
             Min latency: {:.2?}",
            self.name,
            self.operations,
            self.duration,
            self.ops_per_sec,
            self.avg_latency,
            self.p50_latency,
            self.p95_latency,
            self.p99_latency,
            self.max_latency,
            self.min_latency,
        );

        if let (Some(bytes), Some(mbps)) = (self.bytes_processed, self.throughput_mbps) {
            s.push_str(&format!(
                "\nBytes processed: {}\nThroughput: {:.2} MB/s",
                format_bytes(bytes),
                mbps
            ));
        }

        s
    }

    /// Format as JSON
    pub fn to_json(&self) -> String {
        serde_json::json!({
            "name": self.name,
            "operations": self.operations,
            "duration_ms": self.duration.as_millis(),
            "ops_per_sec": self.ops_per_sec,
            "avg_latency_us": self.avg_latency.as_micros(),
            "p50_latency_us": self.p50_latency.as_micros(),
            "p95_latency_us": self.p95_latency.as_micros(),
            "p99_latency_us": self.p99_latency.as_micros(),
            "max_latency_us": self.max_latency.as_micros(),
            "min_latency_us": self.min_latency.as_micros(),
            "bytes_processed": self.bytes_processed,
            "throughput_mbps": self.throughput_mbps,
        })
        .to_string()
    }
}

/// Benchmark runner
pub struct BenchmarkRunner {
    /// Benchmark name
    name: String,
    /// Warmup iterations
    warmup_iterations: u64,
    /// Actual iterations
    iterations: u64,
    /// Collected latencies
    latencies: Vec<Duration>,
    /// Bytes processed per operation
    bytes_per_op: Option<u64>,
}

impl BenchmarkRunner {
    /// Create new benchmark runner
    pub fn new(name: &str) -> Self {
        BenchmarkRunner {
            name: name.to_string(),
            warmup_iterations: 100,
            iterations: 1000,
            latencies: Vec::new(),
            bytes_per_op: None,
        }
    }

    /// Set warmup iterations
    pub fn warmup(mut self, n: u64) -> Self {
        self.warmup_iterations = n;
        self
    }

    /// Set benchmark iterations
    pub fn iterations(mut self, n: u64) -> Self {
        self.iterations = n;
        self
    }

    /// Set bytes per operation for throughput calculation
    pub fn bytes_per_op(mut self, bytes: u64) -> Self {
        self.bytes_per_op = Some(bytes);
        self
    }

    /// Run benchmark with given function
    pub fn run<F>(&mut self, mut f: F) -> BenchmarkResult
    where
        F: FnMut(),
    {
        // Warmup
        for _ in 0..self.warmup_iterations {
            f();
        }

        // Clear any previous results
        self.latencies.clear();
        self.latencies.reserve(self.iterations as usize);

        // Run benchmark
        let start = Instant::now();
        for _ in 0..self.iterations {
            let op_start = Instant::now();
            f();
            self.latencies.push(op_start.elapsed());
        }
        let total_duration = start.elapsed();

        self.calculate_result(total_duration)
    }

    /// Run benchmark with indexed function
    pub fn run_indexed<F>(&mut self, mut f: F) -> BenchmarkResult
    where
        F: FnMut(u64),
    {
        // Warmup
        for i in 0..self.warmup_iterations {
            f(i);
        }

        // Clear any previous results
        self.latencies.clear();
        self.latencies.reserve(self.iterations as usize);

        // Run benchmark
        let start = Instant::now();
        for i in 0..self.iterations {
            let op_start = Instant::now();
            f(i);
            self.latencies.push(op_start.elapsed());
        }
        let total_duration = start.elapsed();

        self.calculate_result(total_duration)
    }

    fn calculate_result(&mut self, total_duration: Duration) -> BenchmarkResult {
        // Sort latencies for percentile calculation
        self.latencies.sort();

        let ops = self.iterations;
        let ops_per_sec = ops as f64 / total_duration.as_secs_f64();

        let total_latency: Duration = self.latencies.iter().sum();
        let avg_latency = total_latency / ops as u32;

        let p50_idx = (ops as f64 * 0.50) as usize;
        let p95_idx = (ops as f64 * 0.95) as usize;
        let p99_idx = (ops as f64 * 0.99) as usize;

        let p50_latency = self.latencies.get(p50_idx).copied().unwrap_or_default();
        let p95_latency = self.latencies.get(p95_idx).copied().unwrap_or_default();
        let p99_latency = self.latencies.get(p99_idx).copied().unwrap_or_default();

        let min_latency = self.latencies.first().copied().unwrap_or_default();
        let max_latency = self.latencies.last().copied().unwrap_or_default();

        let bytes_processed = self.bytes_per_op.map(|b| b * ops);
        let throughput_mbps =
            bytes_processed.map(|b| (b as f64 / 1024.0 / 1024.0) / total_duration.as_secs_f64());

        BenchmarkResult {
            name: self.name.clone(),
            operations: ops,
            duration: total_duration,
            ops_per_sec,
            avg_latency,
            p50_latency,
            p95_latency,
            p99_latency,
            max_latency,
            min_latency,
            bytes_processed,
            throughput_mbps,
        }
    }
}

/// Latency histogram for detailed distribution analysis
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    /// Bucket boundaries in microseconds
    buckets: Vec<u64>,
    /// Counts per bucket
    counts: Vec<u64>,
    /// Total count
    total: u64,
    /// Sum of all latencies
    sum_us: u64,
}

impl LatencyHistogram {
    /// Create histogram with default buckets (powers of 2 from 1us to 1s)
    pub fn new() -> Self {
        let buckets: Vec<u64> = (0..20).map(|i| 1u64 << i).collect();
        let counts = vec![0u64; buckets.len() + 1];
        LatencyHistogram {
            buckets,
            counts,
            total: 0,
            sum_us: 0,
        }
    }

    /// Create histogram with custom bucket boundaries
    pub fn with_buckets(buckets: Vec<u64>) -> Self {
        let counts = vec![0u64; buckets.len() + 1];
        LatencyHistogram {
            buckets,
            counts,
            total: 0,
            sum_us: 0,
        }
    }

    /// Record a latency
    pub fn record(&mut self, latency: Duration) {
        let us = latency.as_micros() as u64;
        self.sum_us += us;
        self.total += 1;

        // Find bucket
        let idx = self
            .buckets
            .iter()
            .position(|&b| us < b)
            .unwrap_or(self.buckets.len());
        self.counts[idx] += 1;
    }

    /// Get percentile value
    pub fn percentile(&self, p: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }

        let target = (self.total as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, &count) in self.counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                if i == 0 {
                    return 0;
                } else if i <= self.buckets.len() {
                    return self.buckets[i - 1];
                } else {
                    return *self.buckets.last().unwrap_or(&0);
                }
            }
        }

        *self.buckets.last().unwrap_or(&0)
    }

    /// Get average latency
    pub fn average(&self) -> u64 {
        if self.total == 0 {
            0
        } else {
            self.sum_us / self.total
        }
    }

    /// Get total count
    pub fn count(&self) -> u64 {
        self.total
    }

    /// Print histogram
    pub fn print(&self) {
        println!("Latency Histogram (total: {})", self.total);
        println!(
            "{:>12} {:>12} {:>10} {:>10}",
            "Bucket", "Count", "Percent", "Cumulative"
        );

        let mut cumulative = 0u64;
        for (i, &count) in self.counts.iter().enumerate() {
            if count == 0 {
                continue;
            }

            cumulative += count;
            let pct = (count as f64 / self.total as f64) * 100.0;
            let cum_pct = (cumulative as f64 / self.total as f64) * 100.0;

            let bucket_label = if i == 0 {
                format!("<{}", self.buckets[0])
            } else if i < self.buckets.len() {
                format!("{}-{}", self.buckets[i - 1], self.buckets[i])
            } else {
                format!(">{}", self.buckets.last().unwrap_or(&0))
            };

            println!(
                "{:>12} {:>12} {:>9.2}% {:>9.2}%",
                bucket_label, count, pct, cum_pct
            );
        }

        println!("\nPercentiles:");
        println!("  P50: {} us", self.percentile(50.0));
        println!("  P90: {} us", self.percentile(90.0));
        println!("  P95: {} us", self.percentile(95.0));
        println!("  P99: {} us", self.percentile(99.0));
        println!("  P99.9: {} us", self.percentile(99.9));
        println!("  Avg: {} us", self.average());
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Throughput tracker for sustained workloads
#[derive(Debug)]
pub struct ThroughputTracker {
    /// Start time
    start: Instant,
    /// Operations completed
    operations: u64,
    /// Bytes processed
    bytes: u64,
    /// Samples for moving average
    samples: Vec<(Instant, u64, u64)>,
    /// Window size for moving average
    window_size: usize,
}

impl ThroughputTracker {
    /// Create new tracker
    pub fn new() -> Self {
        ThroughputTracker {
            start: Instant::now(),
            operations: 0,
            bytes: 0,
            samples: Vec::new(),
            window_size: 100,
        }
    }

    /// Record an operation
    pub fn record(&mut self, bytes: u64) {
        self.operations += 1;
        self.bytes += bytes;

        // Sample for moving average
        if self.samples.len() >= self.window_size {
            self.samples.remove(0);
        }
        self.samples
            .push((Instant::now(), self.operations, self.bytes));
    }

    /// Get current ops/sec
    pub fn ops_per_sec(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.operations as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Get current throughput in MB/s
    pub fn throughput_mbps(&self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (self.bytes as f64 / 1024.0 / 1024.0) / elapsed
        } else {
            0.0
        }
    }

    /// Get moving average ops/sec
    pub fn moving_avg_ops_per_sec(&self) -> f64 {
        if self.samples.len() < 2 {
            return self.ops_per_sec();
        }

        let first = &self.samples[0];
        let last = self.samples.last().unwrap();
        let elapsed = last.0.duration_since(first.0).as_secs_f64();
        let ops = last.1 - first.1;

        if elapsed > 0.0 {
            ops as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Get summary
    pub fn summary(&self) -> ThroughputSummary {
        ThroughputSummary {
            duration: self.start.elapsed(),
            operations: self.operations,
            bytes: self.bytes,
            ops_per_sec: self.ops_per_sec(),
            throughput_mbps: self.throughput_mbps(),
        }
    }

    /// Reset tracker
    pub fn reset(&mut self) {
        self.start = Instant::now();
        self.operations = 0;
        self.bytes = 0;
        self.samples.clear();
    }
}

impl Default for ThroughputTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Throughput summary
#[derive(Debug, Clone)]
pub struct ThroughputSummary {
    pub duration: Duration,
    pub operations: u64,
    pub bytes: u64,
    pub ops_per_sec: f64,
    pub throughput_mbps: f64,
}

/// Benchmark suite for running multiple benchmarks
pub struct BenchmarkSuite {
    /// Suite name
    name: String,
    /// Results
    results: Vec<BenchmarkResult>,
}

impl BenchmarkSuite {
    /// Create new suite
    pub fn new(name: &str) -> Self {
        BenchmarkSuite {
            name: name.to_string(),
            results: Vec::new(),
        }
    }

    /// Add a benchmark result
    pub fn add(&mut self, result: BenchmarkResult) {
        self.results.push(result);
    }

    /// Run a benchmark and add to suite
    pub fn run<F>(&mut self, name: &str, iterations: u64, f: F)
    where
        F: FnMut(),
    {
        let result = BenchmarkRunner::new(name).iterations(iterations).run(f);
        self.results.push(result);
    }

    /// Print all results
    pub fn print(&self) {
        println!("=== Benchmark Suite: {} ===\n", self.name);
        for result in &self.results {
            println!("{}\n", result.format());
            println!("---");
        }
    }

    /// Get results as JSON
    pub fn to_json(&self) -> String {
        let results: Vec<serde_json::Value> = self
            .results
            .iter()
            .map(|r| serde_json::from_str(&r.to_json()).unwrap())
            .collect();

        serde_json::json!({
            "suite": self.name,
            "results": results
        })
        .to_string()
    }

    /// Compare two suites
    pub fn compare(&self, other: &BenchmarkSuite) -> HashMap<String, f64> {
        let mut comparisons = HashMap::new();

        let other_by_name: HashMap<&str, &BenchmarkResult> =
            other.results.iter().map(|r| (r.name.as_str(), r)).collect();

        for result in &self.results {
            if let Some(other_result) = other_by_name.get(result.name.as_str()) {
                let speedup = result.ops_per_sec / other_result.ops_per_sec;
                comparisons.insert(result.name.clone(), speedup);
            }
        }

        comparisons
    }
}

// Helper functions

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_runner() {
        let mut runner = BenchmarkRunner::new("test").warmup(10).iterations(100);

        let result = runner.run(|| {
            let _x: u64 = (0..1000).sum();
        });

        assert_eq!(result.operations, 100);
        assert!(result.ops_per_sec > 0.0);
    }

    #[test]
    fn test_latency_histogram() {
        let mut hist = LatencyHistogram::new();

        for i in 0..1000 {
            hist.record(Duration::from_micros(i));
        }

        assert_eq!(hist.count(), 1000);
        assert!(hist.percentile(50.0) > 0);
    }

    #[test]
    fn test_throughput_tracker() {
        let mut tracker = ThroughputTracker::new();

        for _ in 0..100 {
            tracker.record(1024);
        }

        assert_eq!(tracker.summary().operations, 100);
        assert_eq!(tracker.summary().bytes, 102400);
    }

    #[test]
    fn test_benchmark_suite() {
        let mut suite = BenchmarkSuite::new("test_suite");

        suite.run("addition", 1000, || {
            let _x = 1 + 1;
        });

        suite.run("multiplication", 1000, || {
            let _x = 2 * 3;
        });

        assert_eq!(suite.results.len(), 2);
    }
}
