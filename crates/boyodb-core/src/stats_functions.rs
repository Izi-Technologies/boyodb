//! Advanced Statistical and Analytics Functions
//!
//! Implements SQL statistical functions:
//! - Linear regression (REGR_*)
//! - Correlation (CORR)
//! - Covariance (COVAR_POP, COVAR_SAMP)
//! - Additional statistical tests

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Statistical Aggregator State
// ============================================================================

/// State for computing linear regression statistics
#[derive(Clone, Debug, Default)]
pub struct RegressionState {
    /// Number of non-null pairs
    pub count: u64,
    /// Sum of X values
    pub sum_x: f64,
    /// Sum of Y values
    pub sum_y: f64,
    /// Sum of X squared
    pub sum_x2: f64,
    /// Sum of Y squared
    pub sum_y2: f64,
    /// Sum of X * Y
    pub sum_xy: f64,
}

impl RegressionState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a data point
    pub fn add(&mut self, x: f64, y: f64) {
        self.count += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_x2 += x * x;
        self.sum_y2 += y * y;
        self.sum_xy += x * y;
    }

    /// Merge with another state
    pub fn merge(&mut self, other: &RegressionState) {
        self.count += other.count;
        self.sum_x += other.sum_x;
        self.sum_y += other.sum_y;
        self.sum_x2 += other.sum_x2;
        self.sum_y2 += other.sum_y2;
        self.sum_xy += other.sum_xy;
    }

    /// Compute the slope (REGR_SLOPE)
    pub fn slope(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let n = self.count as f64;
        let denominator = n * self.sum_x2 - self.sum_x * self.sum_x;

        if denominator.abs() < f64::EPSILON {
            return None;
        }

        Some((n * self.sum_xy - self.sum_x * self.sum_y) / denominator)
    }

    /// Compute the Y-intercept (REGR_INTERCEPT)
    pub fn intercept(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let slope = self.slope()?;
        let n = self.count as f64;

        Some((self.sum_y - slope * self.sum_x) / n)
    }

    /// Compute R-squared (REGR_R2)
    pub fn r_squared(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let corr = self.correlation()?;
        Some(corr * corr)
    }

    /// Compute the count of non-null pairs (REGR_COUNT)
    pub fn regr_count(&self) -> u64 {
        self.count
    }

    /// Compute average of X (REGR_AVGX)
    pub fn avg_x(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.sum_x / self.count as f64)
    }

    /// Compute average of Y (REGR_AVGY)
    pub fn avg_y(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.sum_y / self.count as f64)
    }

    /// Compute sum of squares for X (REGR_SXX)
    pub fn sxx(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let n = self.count as f64;
        Some(self.sum_x2 - (self.sum_x * self.sum_x) / n)
    }

    /// Compute sum of squares for Y (REGR_SYY)
    pub fn syy(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let n = self.count as f64;
        Some(self.sum_y2 - (self.sum_y * self.sum_y) / n)
    }

    /// Compute sum of products (REGR_SXY)
    pub fn sxy(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let n = self.count as f64;
        Some(self.sum_xy - (self.sum_x * self.sum_y) / n)
    }

    /// Compute correlation coefficient (CORR)
    pub fn correlation(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let n = self.count as f64;

        let var_x = self.sum_x2 - (self.sum_x * self.sum_x) / n;
        let var_y = self.sum_y2 - (self.sum_y * self.sum_y) / n;
        let cov_xy = self.sum_xy - (self.sum_x * self.sum_y) / n;

        let denominator = (var_x * var_y).sqrt();

        if denominator.abs() < f64::EPSILON {
            return None;
        }

        Some(cov_xy / denominator)
    }

    /// Compute population covariance (COVAR_POP)
    pub fn covariance_pop(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        let n = self.count as f64;
        Some((self.sum_xy - (self.sum_x * self.sum_y) / n) / n)
    }

    /// Compute sample covariance (COVAR_SAMP)
    pub fn covariance_samp(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let n = self.count as f64;
        Some((self.sum_xy - (self.sum_x * self.sum_y) / n) / (n - 1.0))
    }

    /// Population variance of X (VAR_POP)
    pub fn variance_x_pop(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let n = self.count as f64;
        Some((self.sum_x2 - (self.sum_x * self.sum_x) / n) / n)
    }

    /// Sample variance of X (VAR_SAMP)
    pub fn variance_x_samp(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as f64;
        Some((self.sum_x2 - (self.sum_x * self.sum_x) / n) / (n - 1.0))
    }

    /// Population variance of Y (VAR_POP)
    pub fn variance_y_pop(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let n = self.count as f64;
        Some((self.sum_y2 - (self.sum_y * self.sum_y) / n) / n)
    }

    /// Sample variance of Y (VAR_SAMP)
    pub fn variance_y_samp(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let n = self.count as f64;
        Some((self.sum_y2 - (self.sum_y * self.sum_y) / n) / (n - 1.0))
    }

    /// Population standard deviation of X
    pub fn stddev_x_pop(&self) -> Option<f64> {
        self.variance_x_pop().map(|v| v.sqrt())
    }

    /// Sample standard deviation of X
    pub fn stddev_x_samp(&self) -> Option<f64> {
        self.variance_x_samp().map(|v| v.sqrt())
    }

    /// Population standard deviation of Y
    pub fn stddev_y_pop(&self) -> Option<f64> {
        self.variance_y_pop().map(|v| v.sqrt())
    }

    /// Sample standard deviation of Y
    pub fn stddev_y_samp(&self) -> Option<f64> {
        self.variance_y_samp().map(|v| v.sqrt())
    }
}

// ============================================================================
// Single Variable Statistics
// ============================================================================

/// State for single-variable statistics
#[derive(Clone, Debug, Default)]
pub struct UnivariateState {
    pub count: u64,
    pub sum: f64,
    pub sum_sq: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// For kurtosis and skewness (Welford's algorithm)
    pub mean: f64,
    pub m2: f64,
    pub m3: f64,
    pub m4: f64,
}

impl UnivariateState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a value using Welford's online algorithm
    pub fn add(&mut self, x: f64) {
        let n = self.count + 1;
        self.count = n;
        self.sum += x;
        self.sum_sq += x * x;

        // Update min/max
        match self.min {
            None => self.min = Some(x),
            Some(m) if x < m => self.min = Some(x),
            _ => {}
        }
        match self.max {
            None => self.max = Some(x),
            Some(m) if x > m => self.max = Some(x),
            _ => {}
        }

        // Welford's algorithm for higher moments
        let n_f = n as f64;
        let delta = x - self.mean;
        let delta_n = delta / n_f;
        let delta_n2 = delta_n * delta_n;
        let term1 = delta * delta_n * (n_f - 1.0);

        self.mean += delta_n;
        self.m4 += term1 * delta_n2 * (n_f * n_f - 3.0 * n_f + 3.0) + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * self.m3;
        self.m3 += term1 * delta_n * (n_f - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
    }

    /// Merge with another state
    pub fn merge(&mut self, other: &UnivariateState) {
        if other.count == 0 {
            return;
        }

        if self.count == 0 {
            *self = other.clone();
            return;
        }

        let n1 = self.count as f64;
        let n2 = other.count as f64;
        let n = n1 + n2;

        let delta = other.mean - self.mean;
        let delta2 = delta * delta;
        let delta3 = delta2 * delta;
        let delta4 = delta2 * delta2;

        let new_mean = (n1 * self.mean + n2 * other.mean) / n;

        let new_m2 = self.m2 + other.m2 + delta2 * n1 * n2 / n;

        let new_m3 = self.m3
            + other.m3
            + delta3 * n1 * n2 * (n1 - n2) / (n * n)
            + 3.0 * delta * (n1 * other.m2 - n2 * self.m2) / n;

        let new_m4 = self.m4
            + other.m4
            + delta4 * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2) / (n * n * n)
            + 6.0 * delta2 * (n1 * n1 * other.m2 + n2 * n2 * self.m2) / (n * n)
            + 4.0 * delta * (n1 * other.m3 - n2 * self.m3) / n;

        self.count += other.count;
        self.sum += other.sum;
        self.sum_sq += other.sum_sq;
        self.mean = new_mean;
        self.m2 = new_m2;
        self.m3 = new_m3;
        self.m4 = new_m4;

        if let Some(om) = other.min {
            match self.min {
                None => self.min = Some(om),
                Some(m) if om < m => self.min = Some(om),
                _ => {}
            }
        }

        if let Some(om) = other.max {
            match self.max {
                None => self.max = Some(om),
                Some(m) if om > m => self.max = Some(om),
                _ => {}
            }
        }
    }

    /// Mean
    pub fn mean(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.mean)
        }
    }

    /// Population variance
    pub fn variance_pop(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.m2 / self.count as f64)
        }
    }

    /// Sample variance
    pub fn variance_samp(&self) -> Option<f64> {
        if self.count < 2 {
            None
        } else {
            Some(self.m2 / (self.count - 1) as f64)
        }
    }

    /// Population standard deviation
    pub fn stddev_pop(&self) -> Option<f64> {
        self.variance_pop().map(|v| v.sqrt())
    }

    /// Sample standard deviation
    pub fn stddev_samp(&self) -> Option<f64> {
        self.variance_samp().map(|v| v.sqrt())
    }

    /// Skewness (Fisher's definition)
    pub fn skewness(&self) -> Option<f64> {
        if self.count < 3 || self.m2.abs() < f64::EPSILON {
            return None;
        }

        let n = self.count as f64;
        let s2 = self.m2 / n;
        let s3 = s2 * s2.sqrt();

        Some((self.m3 / n) / s3)
    }

    /// Excess kurtosis (Fisher's definition)
    pub fn kurtosis(&self) -> Option<f64> {
        if self.count < 4 || self.m2.abs() < f64::EPSILON {
            return None;
        }

        let n = self.count as f64;
        let s2 = self.m2 / n;
        let s4 = s2 * s2;

        Some((self.m4 / n) / s4 - 3.0)
    }
}

// ============================================================================
// Percentile / Quantile
// ============================================================================

/// State for percentile computation (exact)
#[derive(Clone, Debug, Default)]
pub struct PercentileState {
    values: Vec<f64>,
    sorted: bool,
}

impl PercentileState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, x: f64) {
        self.values.push(x);
        self.sorted = false;
    }

    pub fn merge(&mut self, other: &PercentileState) {
        self.values.extend_from_slice(&other.values);
        self.sorted = false;
    }

    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.values
                .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            self.sorted = true;
        }
    }

    /// Compute percentile (0-100)
    pub fn percentile(&mut self, p: f64) -> Option<f64> {
        if self.values.is_empty() || p < 0.0 || p > 100.0 {
            return None;
        }

        self.ensure_sorted();

        let n = self.values.len();
        let idx = (p / 100.0) * (n - 1) as f64;
        let lower = idx.floor() as usize;
        let upper = idx.ceil() as usize;
        let frac = idx - lower as f64;

        if lower == upper || upper >= n {
            Some(self.values[lower])
        } else {
            Some(self.values[lower] * (1.0 - frac) + self.values[upper] * frac)
        }
    }

    /// Median (50th percentile)
    pub fn median(&mut self) -> Option<f64> {
        self.percentile(50.0)
    }

    /// First quartile (25th percentile)
    pub fn q1(&mut self) -> Option<f64> {
        self.percentile(25.0)
    }

    /// Third quartile (75th percentile)
    pub fn q3(&mut self) -> Option<f64> {
        self.percentile(75.0)
    }

    /// Interquartile range
    pub fn iqr(&mut self) -> Option<f64> {
        match (self.q1(), self.q3()) {
            (Some(q1), Some(q3)) => Some(q3 - q1),
            _ => None,
        }
    }

    /// Mode (most frequent value, for discrete data)
    pub fn mode(&self) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }

        let mut counts: HashMap<u64, (f64, usize)> = HashMap::new();
        for &v in &self.values {
            let key = v.to_bits();
            counts
                .entry(key)
                .and_modify(|(_, count)| *count += 1)
                .or_insert((v, 1));
        }

        counts
            .into_values()
            .max_by_key(|(_, count)| *count)
            .map(|(v, _)| v)
    }
}

// ============================================================================
// T-Test
// ============================================================================

/// Result of a T-test
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TTestResult {
    /// T statistic
    pub t_statistic: f64,
    /// Degrees of freedom
    pub degrees_of_freedom: f64,
    /// Two-tailed p-value (approximate)
    pub p_value: f64,
    /// Mean of first sample
    pub mean1: f64,
    /// Mean of second sample (or hypothesized mean)
    pub mean2: f64,
    /// Pooled standard error
    pub standard_error: f64,
    /// 95% confidence interval
    pub confidence_interval: (f64, f64),
}

/// Perform one-sample t-test
pub fn t_test_one_sample(state: &UnivariateState, mu: f64) -> Option<TTestResult> {
    if state.count < 2 {
        return None;
    }

    let n = state.count as f64;
    let mean = state.mean()?;
    let std = state.stddev_samp()?;
    let se = std / n.sqrt();

    if se.abs() < f64::EPSILON {
        return None;
    }

    let t = (mean - mu) / se;
    let df = n - 1.0;
    let p_value = 2.0 * (1.0 - t_cdf(t.abs(), df));

    // 95% CI using t-critical value approximation
    let t_crit = t_critical_approx(0.05, df);
    let margin = t_crit * se;

    Some(TTestResult {
        t_statistic: t,
        degrees_of_freedom: df,
        p_value,
        mean1: mean,
        mean2: mu,
        standard_error: se,
        confidence_interval: (mean - margin, mean + margin),
    })
}

/// Perform two-sample t-test (independent samples)
pub fn t_test_two_sample(
    state1: &UnivariateState,
    state2: &UnivariateState,
) -> Option<TTestResult> {
    if state1.count < 2 || state2.count < 2 {
        return None;
    }

    let n1 = state1.count as f64;
    let n2 = state2.count as f64;
    let mean1 = state1.mean()?;
    let mean2 = state2.mean()?;
    let var1 = state1.variance_samp()?;
    let var2 = state2.variance_samp()?;

    // Welch's t-test (unequal variances)
    let se = ((var1 / n1) + (var2 / n2)).sqrt();

    if se.abs() < f64::EPSILON {
        return None;
    }

    let t = (mean1 - mean2) / se;

    // Welch-Satterthwaite degrees of freedom
    let v1 = var1 / n1;
    let v2 = var2 / n2;
    let df = (v1 + v2).powi(2) / (v1.powi(2) / (n1 - 1.0) + v2.powi(2) / (n2 - 1.0));

    let p_value = 2.0 * (1.0 - t_cdf(t.abs(), df));

    let t_crit = t_critical_approx(0.05, df);
    let margin = t_crit * se;
    let diff = mean1 - mean2;

    Some(TTestResult {
        t_statistic: t,
        degrees_of_freedom: df,
        p_value,
        mean1,
        mean2,
        standard_error: se,
        confidence_interval: (diff - margin, diff + margin),
    })
}

/// Approximate t-distribution CDF using normal approximation for large df
fn t_cdf(t: f64, df: f64) -> f64 {
    if df > 100.0 {
        // Use normal approximation
        normal_cdf(t)
    } else {
        // Approximation using regularized incomplete beta function
        let x = df / (df + t * t);
        let a = df / 2.0;
        let b = 0.5;

        // Very rough approximation
        if t < 0.0 {
            0.5 * incomplete_beta_approx(x, a, b)
        } else {
            1.0 - 0.5 * incomplete_beta_approx(x, a, b)
        }
    }
}

/// Approximate t critical value
fn t_critical_approx(alpha: f64, df: f64) -> f64 {
    if df > 120.0 {
        // Use z-score approximation
        return normal_quantile(1.0 - alpha / 2.0);
    }

    // Rough approximation using lookup interpolation
    let z = normal_quantile(1.0 - alpha / 2.0);
    let g1 = (z.powi(3) + z) / 4.0;
    let g2 = (5.0 * z.powi(5) + 16.0 * z.powi(3) + 3.0 * z) / 96.0;

    z + g1 / df + g2 / df.powi(2)
}

/// Standard normal CDF approximation
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Standard normal quantile (inverse CDF) approximation
fn normal_quantile(p: f64) -> f64 {
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }

    // Rational approximation
    if p < 0.5 {
        let t = (-2.0 * p.ln()).sqrt();
        let c0 = 2.515517;
        let c1 = 0.802853;
        let c2 = 0.010328;
        let d1 = 1.432788;
        let d2 = 0.189269;
        let d3 = 0.001308;

        -(t - (c0 + c1 * t + c2 * t * t) / (1.0 + d1 * t + d2 * t * t + d3 * t * t * t))
    } else {
        -normal_quantile(1.0 - p)
    }
}

/// Error function approximation
fn erf(x: f64) -> f64 {
    // Abramowitz and Stegun approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

/// Very rough incomplete beta function approximation
fn incomplete_beta_approx(x: f64, a: f64, b: f64) -> f64 {
    // Use simple approximation for small values
    if x < 0.001 {
        return (x.powf(a) * (1.0 - x).powf(b)) / a;
    }
    if x > 0.999 {
        return 1.0 - incomplete_beta_approx(1.0 - x, b, a);
    }

    // Use continued fraction expansion (simplified)
    let bt = if x == 0.0 || x == 1.0 {
        0.0
    } else {
        (ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln()).exp()
    };

    if x < (a + 1.0) / (a + b + 2.0) {
        bt * beta_cf(x, a, b) / a
    } else {
        1.0 - bt * beta_cf(1.0 - x, b, a) / b
    }
}

/// Continued fraction for incomplete beta
fn beta_cf(x: f64, a: f64, b: f64) -> f64 {
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;

    if d.abs() < 1e-30 {
        d = 1e-30;
    }
    d = 1.0 / d;
    let mut h = d;

    for m in 1..=100 {
        let m = m as f64;
        let m2 = 2.0 * m;

        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < 1e-30 {
            d = 1e-30;
        }
        c = 1.0 + aa / c;
        if c.abs() < 1e-30 {
            c = 1e-30;
        }
        d = 1.0 / d;
        h *= d * c;

        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < 1e-30 {
            d = 1e-30;
        }
        c = 1.0 + aa / c;
        if c.abs() < 1e-30 {
            c = 1e-30;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;

        if (del - 1.0).abs() < 1e-10 {
            break;
        }
    }

    h
}

/// Log gamma function (Lanczos approximation)
fn ln_gamma(x: f64) -> f64 {
    let coefficients = [
        76.18009172947146,
        -86.50532032941677,
        24.01409824083091,
        -1.231739572450155,
        0.1208650973866179e-2,
        -0.5395239384953e-5,
    ];

    let x = x - 1.0;
    let tmp = x + 5.5;
    let tmp = tmp - (x + 0.5) * tmp.ln();

    let mut ser = 1.000000000190015;
    for (i, &c) in coefficients.iter().enumerate() {
        ser += c / (x + 1.0 + i as f64);
    }

    -tmp + (2.5066282746310005 * ser).ln()
}

// ============================================================================
// Chi-Square Test
// ============================================================================

/// Chi-square test result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChiSquareResult {
    /// Chi-square statistic
    pub chi_square: f64,
    /// Degrees of freedom
    pub degrees_of_freedom: u64,
    /// P-value
    pub p_value: f64,
    /// Expected frequencies
    pub expected: Vec<f64>,
}

/// Chi-square goodness of fit test
pub fn chi_square_goodness_of_fit(observed: &[f64], expected: &[f64]) -> Option<ChiSquareResult> {
    if observed.len() != expected.len() || observed.is_empty() {
        return None;
    }

    let mut chi2 = 0.0;
    for (o, e) in observed.iter().zip(expected.iter()) {
        if *e <= 0.0 {
            return None;
        }
        chi2 += (o - e).powi(2) / e;
    }

    let df = (observed.len() - 1) as u64;
    let p_value = 1.0 - chi_square_cdf(chi2, df as f64);

    Some(ChiSquareResult {
        chi_square: chi2,
        degrees_of_freedom: df,
        p_value,
        expected: expected.to_vec(),
    })
}

/// Chi-square test of independence
pub fn chi_square_independence(contingency: &[Vec<f64>]) -> Option<ChiSquareResult> {
    if contingency.is_empty() || contingency[0].is_empty() {
        return None;
    }

    let rows = contingency.len();
    let cols = contingency[0].len();

    // Calculate row and column totals
    let row_totals: Vec<f64> = contingency.iter().map(|row| row.iter().sum()).collect();

    let col_totals: Vec<f64> = (0..cols)
        .map(|j| contingency.iter().map(|row| row[j]).sum())
        .collect();

    let total: f64 = row_totals.iter().sum();

    if total <= 0.0 {
        return None;
    }

    // Calculate chi-square
    let mut chi2 = 0.0;
    let mut expected = Vec::new();

    for (i, row) in contingency.iter().enumerate() {
        for (j, &observed) in row.iter().enumerate() {
            let exp = row_totals[i] * col_totals[j] / total;
            if exp > 0.0 {
                chi2 += (observed - exp).powi(2) / exp;
            }
            expected.push(exp);
        }
    }

    let df = ((rows - 1) * (cols - 1)) as u64;
    let p_value = 1.0 - chi_square_cdf(chi2, df as f64);

    Some(ChiSquareResult {
        chi_square: chi2,
        degrees_of_freedom: df,
        p_value,
        expected,
    })
}

/// Chi-square CDF approximation
fn chi_square_cdf(x: f64, df: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }

    // Use regularized incomplete gamma function
    gamma_p(df / 2.0, x / 2.0)
}

/// Regularized incomplete gamma function P(a, x)
fn gamma_p(a: f64, x: f64) -> f64 {
    if x < 0.0 || a <= 0.0 {
        return 0.0;
    }

    if x < a + 1.0 {
        // Use series expansion
        let mut sum = 1.0 / a;
        let mut term = 1.0 / a;

        for n in 1..100 {
            term *= x / (a + n as f64);
            sum += term;
            if term.abs() < 1e-10 * sum.abs() {
                break;
            }
        }

        sum * (-x + a * x.ln() - ln_gamma(a)).exp()
    } else {
        // Use continued fraction
        1.0 - gamma_q_cf(a, x)
    }
}

/// Continued fraction for Q(a, x)
fn gamma_q_cf(a: f64, x: f64) -> f64 {
    let mut b = x + 1.0 - a;
    let mut c = 1.0 / 1e-30;
    let mut d = 1.0 / b;
    let mut h = d;

    for i in 1..100 {
        let an = -(i as f64) * (i as f64 - a);
        b += 2.0;
        d = an * d + b;
        if d.abs() < 1e-30 {
            d = 1e-30;
        }
        c = b + an / c;
        if c.abs() < 1e-30 {
            c = 1e-30;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < 1e-10 {
            break;
        }
    }

    (-x + a * x.ln() - ln_gamma(a)).exp() * h
}

// ============================================================================
// ANOVA (Analysis of Variance)
// ============================================================================

/// One-way ANOVA result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnovaResult {
    /// Between-group sum of squares
    pub ss_between: f64,
    /// Within-group sum of squares
    pub ss_within: f64,
    /// Total sum of squares
    pub ss_total: f64,
    /// Between-group degrees of freedom
    pub df_between: u64,
    /// Within-group degrees of freedom
    pub df_within: u64,
    /// Between-group mean square
    pub ms_between: f64,
    /// Within-group mean square
    pub ms_within: f64,
    /// F-statistic
    pub f_statistic: f64,
    /// P-value
    pub p_value: f64,
    /// Effect size (eta-squared)
    pub eta_squared: f64,
}

/// One-way ANOVA
pub fn anova_one_way(groups: &[&[f64]]) -> Option<AnovaResult> {
    if groups.len() < 2 {
        return None;
    }

    let k = groups.len();
    let mut n_total = 0;
    let mut grand_sum = 0.0;

    // Calculate group means and sizes
    let group_stats: Vec<(usize, f64, f64)> = groups
        .iter()
        .map(|g| {
            let n = g.len();
            let sum: f64 = g.iter().sum();
            let mean = if n > 0 { sum / n as f64 } else { 0.0 };
            n_total += n;
            grand_sum += sum;
            (n, mean, sum)
        })
        .collect();

    if n_total <= k {
        return None;
    }

    let grand_mean = grand_sum / n_total as f64;

    // Calculate sum of squares
    let mut ss_between = 0.0;
    let mut ss_within = 0.0;

    for (i, &(n, mean, _)) in group_stats.iter().enumerate() {
        ss_between += n as f64 * (mean - grand_mean).powi(2);

        for &x in groups[i] {
            ss_within += (x - mean).powi(2);
        }
    }

    let ss_total = ss_between + ss_within;
    let df_between = (k - 1) as u64;
    let df_within = (n_total - k) as u64;

    let ms_between = ss_between / df_between as f64;
    let ms_within = ss_within / df_within as f64;

    if ms_within.abs() < f64::EPSILON {
        return None;
    }

    let f_stat = ms_between / ms_within;
    let p_value = 1.0 - f_cdf(f_stat, df_between as f64, df_within as f64);
    let eta_squared = ss_between / ss_total;

    Some(AnovaResult {
        ss_between,
        ss_within,
        ss_total,
        df_between,
        df_within,
        ms_between,
        ms_within,
        f_statistic: f_stat,
        p_value,
        eta_squared,
    })
}

/// F-distribution CDF approximation
fn f_cdf(f: f64, d1: f64, d2: f64) -> f64 {
    if f <= 0.0 {
        return 0.0;
    }

    let x = d2 / (d2 + d1 * f);
    1.0 - incomplete_beta_approx(x, d2 / 2.0, d1 / 2.0)
}

// ============================================================================
// Aggregate Function Registry
// ============================================================================

/// SQL aggregate function names
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StatFunction {
    // Linear regression
    RegrSlope,
    RegrIntercept,
    RegrR2,
    RegrCount,
    RegrAvgX,
    RegrAvgY,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    // Correlation & covariance
    Corr,
    CovarPop,
    CovarSamp,
    // Variance & stddev (bivariate, for x or y)
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    // Univariate
    Skewness,
    Kurtosis,
    // Percentiles
    PercentileCont,
    PercentileDisc,
    Median,
}

impl StatFunction {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "REGR_SLOPE" => Some(Self::RegrSlope),
            "REGR_INTERCEPT" => Some(Self::RegrIntercept),
            "REGR_R2" => Some(Self::RegrR2),
            "REGR_COUNT" => Some(Self::RegrCount),
            "REGR_AVGX" => Some(Self::RegrAvgX),
            "REGR_AVGY" => Some(Self::RegrAvgY),
            "REGR_SXX" => Some(Self::RegrSxx),
            "REGR_SYY" => Some(Self::RegrSyy),
            "REGR_SXY" => Some(Self::RegrSxy),
            "CORR" => Some(Self::Corr),
            "COVAR_POP" => Some(Self::CovarPop),
            "COVAR_SAMP" => Some(Self::CovarSamp),
            "VAR_POP" | "VARIANCE_POP" => Some(Self::VarPop),
            "VAR_SAMP" | "VARIANCE_SAMP" | "VARIANCE" => Some(Self::VarSamp),
            "STDDEV_POP" => Some(Self::StddevPop),
            "STDDEV_SAMP" | "STDDEV" => Some(Self::StddevSamp),
            "SKEWNESS" => Some(Self::Skewness),
            "KURTOSIS" => Some(Self::Kurtosis),
            "PERCENTILE_CONT" => Some(Self::PercentileCont),
            "PERCENTILE_DISC" => Some(Self::PercentileDisc),
            "MEDIAN" => Some(Self::Median),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regression() {
        let mut state = RegressionState::new();

        // y = 2x + 1
        state.add(1.0, 3.0);
        state.add(2.0, 5.0);
        state.add(3.0, 7.0);
        state.add(4.0, 9.0);
        state.add(5.0, 11.0);

        let slope = state.slope().unwrap();
        let intercept = state.intercept().unwrap();
        let r2 = state.r_squared().unwrap();

        assert!((slope - 2.0).abs() < 0.001);
        assert!((intercept - 1.0).abs() < 0.001);
        assert!((r2 - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_correlation() {
        let mut state = RegressionState::new();

        // Perfect positive correlation
        state.add(1.0, 1.0);
        state.add(2.0, 2.0);
        state.add(3.0, 3.0);

        let corr = state.correlation().unwrap();
        assert!((corr - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_covariance() {
        let mut state = RegressionState::new();

        state.add(1.0, 2.0);
        state.add(2.0, 4.0);
        state.add(3.0, 6.0);

        let cov_pop = state.covariance_pop().unwrap();
        let cov_samp = state.covariance_samp().unwrap();

        // Covariance should be positive
        assert!(cov_pop > 0.0);
        assert!(cov_samp > cov_pop); // Sample covariance > population for n > 1
    }

    #[test]
    fn test_univariate() {
        let mut state = UnivariateState::new();

        // Standard normal-ish data
        for x in [1.0, 2.0, 3.0, 4.0, 5.0].iter() {
            state.add(*x);
        }

        assert_eq!(state.count, 5);
        assert!((state.mean().unwrap() - 3.0).abs() < 0.001);
        assert!(state.stddev_samp().unwrap() > 0.0);
    }

    #[test]
    fn test_percentile() {
        let mut state = PercentileState::new();

        for i in 1..=100 {
            state.add(i as f64);
        }

        let median = state.median().unwrap();
        let q1 = state.q1().unwrap();
        let q3 = state.q3().unwrap();

        assert!((median - 50.5).abs() < 1.0);
        assert!((q1 - 25.25).abs() < 1.0);
        assert!((q3 - 75.75).abs() < 1.0);
    }

    #[test]
    fn test_t_test() {
        let mut state = UnivariateState::new();

        for x in [5.0, 5.5, 6.0, 5.8, 5.2].iter() {
            state.add(*x);
        }

        let result = t_test_one_sample(&state, 5.0).unwrap();

        // Mean should be significantly different from 5.0 or not
        assert!(result.t_statistic.is_finite());
        assert!(result.p_value >= 0.0 && result.p_value <= 1.0);
    }

    #[test]
    fn test_chi_square() {
        // Test dice rolls
        let observed = vec![16.0, 18.0, 22.0, 20.0, 15.0, 29.0];
        let expected = vec![20.0, 20.0, 20.0, 20.0, 20.0, 20.0];

        let result = chi_square_goodness_of_fit(&observed, &expected).unwrap();

        assert!(result.chi_square > 0.0);
        assert_eq!(result.degrees_of_freedom, 5);
    }

    #[test]
    fn test_anova() {
        let group1 = [1.0, 2.0, 3.0];
        let group2 = [4.0, 5.0, 6.0];
        let group3 = [7.0, 8.0, 9.0];

        let result = anova_one_way(&[&group1, &group2, &group3]).unwrap();

        // Groups are clearly different, F should be significant
        assert!(result.f_statistic > 1.0);
        assert!(result.p_value < 0.05);
    }

    #[test]
    fn test_stat_function_names() {
        assert_eq!(
            StatFunction::from_name("REGR_SLOPE"),
            Some(StatFunction::RegrSlope)
        );
        assert_eq!(StatFunction::from_name("corr"), Some(StatFunction::Corr));
        assert_eq!(
            StatFunction::from_name("COVAR_POP"),
            Some(StatFunction::CovarPop)
        );
        assert_eq!(StatFunction::from_name("unknown"), None);
    }
}
