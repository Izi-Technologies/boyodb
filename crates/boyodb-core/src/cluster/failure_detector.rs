//! Phi Accrual Failure Detector for accurate failure detection.
//!
//! Based on: "The Phi Accrual Failure Detector" (Hayashibara et al.)
//! This detector adapts to varying network conditions and provides
//! a probability-based suspicion level rather than binary alive/dead.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Phi Accrual Failure Detector.
///
/// Instead of binary alive/dead decisions, this detector provides
/// a continuous "phi" value that represents how suspicious we should
/// be that a node has failed. Higher phi = more likely failed.
#[derive(Debug)]
pub struct PhiAccrualDetector {
    /// Window of heartbeat intervals for statistical analysis.
    intervals: VecDeque<Duration>,
    /// Maximum samples to keep.
    max_samples: usize,
    /// Last heartbeat time.
    last_heartbeat: Option<Instant>,
    /// Threshold for failure (typically 8-12).
    phi_threshold: f64,
    /// Minimum standard deviation to prevent divide-by-zero.
    min_std_deviation: Duration,
    /// Initial heartbeat interval estimate.
    initial_interval: Duration,
}

impl PhiAccrualDetector {
    /// Create a new failure detector with the given phi threshold.
    ///
    /// - `phi_threshold`: Higher values mean more tolerance for slow heartbeats.
    ///   Typical values: 8 (aggressive) to 12 (conservative).
    pub fn new(phi_threshold: f64) -> Self {
        PhiAccrualDetector {
            intervals: VecDeque::with_capacity(1000),
            max_samples: 1000,
            last_heartbeat: None,
            phi_threshold,
            min_std_deviation: Duration::from_millis(100),
            initial_interval: Duration::from_millis(500),
        }
    }

    /// Create a detector with custom configuration.
    pub fn with_config(
        phi_threshold: f64,
        max_samples: usize,
        min_std_deviation: Duration,
        initial_interval: Duration,
    ) -> Self {
        PhiAccrualDetector {
            intervals: VecDeque::with_capacity(max_samples),
            max_samples,
            last_heartbeat: None,
            phi_threshold,
            min_std_deviation,
            initial_interval,
        }
    }

    /// Record a heartbeat at the current time.
    pub fn heartbeat(&mut self) {
        self.heartbeat_at(Instant::now());
    }

    /// Record a heartbeat at a specific time.
    pub fn heartbeat_at(&mut self, now: Instant) {
        if let Some(last) = self.last_heartbeat {
            let interval = now.duration_since(last);
            if self.intervals.len() >= self.max_samples {
                self.intervals.pop_front();
            }
            self.intervals.push_back(interval);
        }
        self.last_heartbeat = Some(now);
    }

    /// Calculate the phi value at the current time.
    ///
    /// Returns a value >= 0 where:
    /// - phi < 1: Very likely alive
    /// - phi 1-3: Probably alive
    /// - phi 3-8: Uncertain
    /// - phi > 8: Probably failed
    /// - phi > 12: Very likely failed
    pub fn phi(&self) -> f64 {
        self.phi_at(Instant::now())
    }

    /// Calculate phi at a specific time.
    pub fn phi_at(&self, now: Instant) -> f64 {
        let Some(last) = self.last_heartbeat else {
            return 0.0; // No data yet, assume alive
        };

        let elapsed = now.duration_since(last);

        // Use initial estimate if not enough samples
        if self.intervals.len() < 2 {
            let mean = self.initial_interval;
            let std_dev = self.min_std_deviation;
            return self.calculate_phi(elapsed, mean, std_dev);
        }

        let mean = self.mean();
        let std_dev = self.std_deviation().max(self.min_std_deviation);
        self.calculate_phi(elapsed, mean, std_dev)
    }

    fn calculate_phi(&self, elapsed: Duration, mean: Duration, std_dev: Duration) -> f64 {
        // Calculate probability using normal distribution
        let y = (elapsed.as_secs_f64() - mean.as_secs_f64()) / std_dev.as_secs_f64();
        let p = 1.0 - normal_cdf(y);

        if p <= 0.0 {
            f64::MAX
        } else {
            -p.log10()
        }
    }

    /// Check if the node should be considered failed.
    pub fn is_failed(&self) -> bool {
        self.phi() >= self.phi_threshold
    }

    /// Check if the node should be considered failed at a specific time.
    pub fn is_failed_at(&self, now: Instant) -> bool {
        self.phi_at(now) >= self.phi_threshold
    }

    /// Get the mean heartbeat interval.
    fn mean(&self) -> Duration {
        if self.intervals.is_empty() {
            return self.initial_interval;
        }
        let sum: Duration = self.intervals.iter().sum();
        sum / self.intervals.len() as u32
    }

    /// Get the standard deviation of heartbeat intervals.
    fn std_deviation(&self) -> Duration {
        if self.intervals.len() < 2 {
            return self.min_std_deviation;
        }

        let mean = self.mean().as_secs_f64();
        let variance: f64 = self
            .intervals
            .iter()
            .map(|d| (d.as_secs_f64() - mean).powi(2))
            .sum::<f64>()
            / self.intervals.len() as f64;
        Duration::from_secs_f64(variance.sqrt())
    }

    /// Get the time since last heartbeat.
    pub fn time_since_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.map(|last| last.elapsed())
    }

    /// Reset the detector state.
    pub fn reset(&mut self) {
        self.intervals.clear();
        self.last_heartbeat = None;
    }

    /// Get the number of samples collected.
    pub fn sample_count(&self) -> usize {
        self.intervals.len()
    }
}

/// Standard normal cumulative distribution function.
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + libm::erf(x / std::f64::consts::SQRT_2))
}

/// Simple timeout-based failure detector for comparison/fallback.
#[derive(Debug)]
pub struct TimeoutDetector {
    last_heartbeat: Option<Instant>,
    timeout: Duration,
}

impl TimeoutDetector {
    pub fn new(timeout: Duration) -> Self {
        TimeoutDetector {
            last_heartbeat: None,
            timeout,
        }
    }

    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    pub fn is_failed(&self) -> bool {
        match self.last_heartbeat {
            Some(last) => last.elapsed() > self.timeout,
            None => false,
        }
    }

    pub fn time_since_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.map(|last| last.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_phi_detector_initial_state() {
        let detector = PhiAccrualDetector::new(8.0);
        assert_eq!(detector.phi(), 0.0);
        assert!(!detector.is_failed());
    }

    #[test]
    fn test_phi_detector_after_heartbeat() {
        let mut detector = PhiAccrualDetector::new(8.0);
        detector.heartbeat();
        // Immediately after heartbeat, phi should be low
        assert!(detector.phi() < 1.0);
        assert!(!detector.is_failed());
    }

    #[test]
    fn test_phi_increases_over_time() {
        let mut detector = PhiAccrualDetector::new(8.0);

        // Record some regular heartbeats
        for _ in 0..10 {
            detector.heartbeat();
            thread::sleep(Duration::from_millis(10));
        }

        let phi_soon = detector.phi();

        // Wait longer
        thread::sleep(Duration::from_millis(100));
        let phi_later = detector.phi();

        assert!(phi_later > phi_soon);
    }

    #[test]
    fn test_timeout_detector() {
        let mut detector = TimeoutDetector::new(Duration::from_millis(50));
        assert!(!detector.is_failed());

        detector.heartbeat();
        assert!(!detector.is_failed());

        thread::sleep(Duration::from_millis(100));
        assert!(detector.is_failed());
    }
}
