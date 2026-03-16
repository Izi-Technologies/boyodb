//! ML Explainability for BoyoDB
//!
//! Model interpretation and explanation tools:
//! - SHAP (SHapley Additive exPlanations) values
//! - LIME (Local Interpretable Model-agnostic Explanations)
//! - Feature importance analysis
//! - Partial dependence plots
//! - Counterfactual explanations

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Explanation types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExplanationType {
    /// SHAP values
    Shap,
    /// LIME explanation
    Lime,
    /// Permutation importance
    PermutationImportance,
    /// Partial dependence
    PartialDependence,
    /// Counterfactual
    Counterfactual,
}

/// Feature contribution to prediction
#[derive(Debug, Clone)]
pub struct FeatureContribution {
    /// Feature name
    pub feature: String,
    /// Feature value
    pub value: f64,
    /// Contribution to prediction
    pub contribution: f64,
    /// Confidence/uncertainty
    pub confidence: Option<f64>,
}

/// SHAP explanation
#[derive(Debug, Clone)]
pub struct ShapExplanation {
    /// Base value (expected prediction)
    pub base_value: f64,
    /// Feature SHAP values
    pub shap_values: Vec<FeatureContribution>,
    /// Final prediction
    pub prediction: f64,
    /// Interaction values (optional)
    pub interactions: Option<HashMap<(String, String), f64>>,
}

impl ShapExplanation {
    /// Get top contributing features
    pub fn top_features(&self, n: usize) -> Vec<&FeatureContribution> {
        let mut sorted: Vec<_> = self.shap_values.iter().collect();
        sorted.sort_by(|a, b| {
            b.contribution
                .abs()
                .partial_cmp(&a.contribution.abs())
                .unwrap()
        });
        sorted.truncate(n);
        sorted
    }

    /// Verify SHAP values sum to prediction - base_value
    pub fn verify(&self) -> bool {
        let sum: f64 = self.shap_values.iter().map(|s| s.contribution).sum();
        (sum - (self.prediction - self.base_value)).abs() < 1e-6
    }
}

/// LIME explanation
#[derive(Debug, Clone)]
pub struct LimeExplanation {
    /// Original prediction
    pub original_prediction: f64,
    /// Local model intercept
    pub intercept: f64,
    /// Feature weights in local model
    pub feature_weights: Vec<FeatureContribution>,
    /// Local model R² score
    pub local_fidelity: f64,
    /// Number of samples used
    pub n_samples: usize,
}

impl LimeExplanation {
    /// Get features contributing positively
    pub fn positive_contributors(&self) -> Vec<&FeatureContribution> {
        self.feature_weights
            .iter()
            .filter(|f| f.contribution > 0.0)
            .collect()
    }

    /// Get features contributing negatively
    pub fn negative_contributors(&self) -> Vec<&FeatureContribution> {
        self.feature_weights
            .iter()
            .filter(|f| f.contribution < 0.0)
            .collect()
    }
}

/// Partial dependence result
#[derive(Debug, Clone)]
pub struct PartialDependence {
    /// Feature name
    pub feature: String,
    /// Feature values
    pub values: Vec<f64>,
    /// Average predictions at each value
    pub predictions: Vec<f64>,
    /// Confidence intervals (optional)
    pub confidence_lower: Option<Vec<f64>>,
    pub confidence_upper: Option<Vec<f64>>,
}

/// Counterfactual explanation
#[derive(Debug, Clone)]
pub struct Counterfactual {
    /// Original instance
    pub original: HashMap<String, f64>,
    /// Original prediction
    pub original_prediction: f64,
    /// Counterfactual instance
    pub counterfactual: HashMap<String, f64>,
    /// Counterfactual prediction
    pub counterfactual_prediction: f64,
    /// Changes made
    pub changes: Vec<FeatureChange>,
    /// Distance from original
    pub distance: f64,
    /// Sparsity (number of features changed)
    pub sparsity: usize,
}

/// Feature change in counterfactual
#[derive(Debug, Clone)]
pub struct FeatureChange {
    pub feature: String,
    pub original_value: f64,
    pub new_value: f64,
    pub change_magnitude: f64,
}

/// Model interface for explanations
pub trait ExplainableModel {
    /// Predict for a single instance
    fn predict(&self, features: &[f64]) -> f64;

    /// Predict probabilities (for classification)
    fn predict_proba(&self, features: &[f64]) -> Option<Vec<f64>> {
        None
    }

    /// Get feature names
    fn feature_names(&self) -> Vec<String>;

    /// Get number of features
    fn n_features(&self) -> usize;
}

/// Simple linear model for explanations
pub struct LinearExplainableModel {
    weights: Vec<f64>,
    bias: f64,
    feature_names: Vec<String>,
}

impl LinearExplainableModel {
    pub fn new(weights: Vec<f64>, bias: f64, feature_names: Vec<String>) -> Self {
        Self {
            weights,
            bias,
            feature_names,
        }
    }
}

impl ExplainableModel for LinearExplainableModel {
    fn predict(&self, features: &[f64]) -> f64 {
        self.weights
            .iter()
            .zip(features.iter())
            .map(|(w, x)| w * x)
            .sum::<f64>()
            + self.bias
    }

    fn feature_names(&self) -> Vec<String> {
        self.feature_names.clone()
    }

    fn n_features(&self) -> usize {
        self.weights.len()
    }
}

/// SHAP explainer (Kernel SHAP implementation)
pub struct ShapExplainer {
    /// Background data for computing expectations
    background_data: Vec<Vec<f64>>,
    /// Feature names
    feature_names: Vec<String>,
    /// Number of samples for approximation
    n_samples: usize,
}

impl ShapExplainer {
    /// Create a new SHAP explainer
    pub fn new(background_data: Vec<Vec<f64>>, feature_names: Vec<String>) -> Self {
        Self {
            background_data,
            feature_names,
            n_samples: 100,
        }
    }

    /// Set number of samples
    pub fn with_samples(mut self, n: usize) -> Self {
        self.n_samples = n;
        self
    }

    /// Compute SHAP values for an instance
    pub fn explain<M: ExplainableModel>(&self, model: &M, instance: &[f64]) -> ShapExplanation {
        let n_features = instance.len();
        let prediction = model.predict(instance);

        // Compute base value (expected prediction over background)
        let base_value: f64 = self
            .background_data
            .iter()
            .map(|bg| model.predict(bg))
            .sum::<f64>()
            / self.background_data.len() as f64;

        // Compute SHAP values using Kernel SHAP approximation
        let mut shap_values = vec![0.0; n_features];

        // Sample coalitions and compute marginal contributions
        for sample_idx in 0..self.n_samples {
            // Generate random coalition
            let coalition = self.sample_coalition(n_features, sample_idx as u64);

            // For each feature, compute marginal contribution
            for i in 0..n_features {
                if !coalition[i] {
                    // Feature not in coalition - compute marginal contribution
                    let with_feature =
                        self.create_instance_with_coalition(instance, &coalition, Some(i));
                    let without_feature =
                        self.create_instance_with_coalition(instance, &coalition, None);

                    let pred_with = model.predict(&with_feature);
                    let pred_without = model.predict(&without_feature);

                    // Weight by coalition size
                    let coalition_size: usize = coalition.iter().filter(|&&x| x).count();
                    let weight = self.shapley_kernel_weight(n_features, coalition_size);

                    shap_values[i] += weight * (pred_with - pred_without);
                }
            }
        }

        // Normalize SHAP values
        let scale = (prediction - base_value) / shap_values.iter().sum::<f64>().max(1e-10);
        for v in &mut shap_values {
            *v *= scale;
        }

        let contributions: Vec<FeatureContribution> = self
            .feature_names
            .iter()
            .zip(instance.iter())
            .zip(shap_values.iter())
            .map(|((name, &value), &contrib)| FeatureContribution {
                feature: name.clone(),
                value,
                contribution: contrib,
                confidence: None,
            })
            .collect();

        ShapExplanation {
            base_value,
            shap_values: contributions,
            prediction,
            interactions: None,
        }
    }

    /// Sample a random coalition
    fn sample_coalition(&self, n: usize, seed: u64) -> Vec<bool> {
        (0..n)
            .map(|i| {
                let r = ((seed + i as u64) as f64 * 0.123456789).sin().abs();
                r > 0.5
            })
            .collect()
    }

    /// Create instance with coalition features from instance, others from background
    fn create_instance_with_coalition(
        &self,
        instance: &[f64],
        coalition: &[bool],
        extra_feature: Option<usize>,
    ) -> Vec<f64> {
        // Use mean of background data for non-coalition features
        let bg_mean: Vec<f64> = (0..instance.len())
            .map(|i| {
                self.background_data.iter().map(|bg| bg[i]).sum::<f64>()
                    / self.background_data.len() as f64
            })
            .collect();

        instance
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                if coalition[i] || extra_feature == Some(i) {
                    v
                } else {
                    bg_mean[i]
                }
            })
            .collect()
    }

    /// Shapley kernel weight
    fn shapley_kernel_weight(&self, n: usize, coalition_size: usize) -> f64 {
        let s = coalition_size as f64;
        let m = n as f64;

        if s == 0.0 || s == m {
            return 0.0;
        }

        // SHAP kernel weight
        (m - 1.0) / (self.binomial(m as u64 - 1, s as u64) * s * (m - s))
    }

    /// Binomial coefficient
    fn binomial(&self, n: u64, k: u64) -> f64 {
        if k > n {
            return 0.0;
        }

        let mut result = 1.0;
        for i in 0..k {
            result *= (n - i) as f64 / (i + 1) as f64;
        }
        result
    }
}

/// LIME explainer
pub struct LimeExplainer {
    /// Feature names
    feature_names: Vec<String>,
    /// Number of samples to generate
    n_samples: usize,
    /// Kernel width
    kernel_width: f64,
}

impl LimeExplainer {
    /// Create a new LIME explainer
    pub fn new(feature_names: Vec<String>) -> Self {
        Self {
            feature_names,
            n_samples: 1000,
            kernel_width: 0.75,
        }
    }

    /// Set number of samples
    pub fn with_samples(mut self, n: usize) -> Self {
        self.n_samples = n;
        self
    }

    /// Set kernel width
    pub fn with_kernel_width(mut self, width: f64) -> Self {
        self.kernel_width = width;
        self
    }

    /// Generate LIME explanation
    pub fn explain<M: ExplainableModel>(&self, model: &M, instance: &[f64]) -> LimeExplanation {
        let n_features = instance.len();
        let original_prediction = model.predict(instance);

        // Generate perturbed samples
        let mut perturbed_samples = Vec::with_capacity(self.n_samples);
        let mut predictions = Vec::with_capacity(self.n_samples);
        let mut weights = Vec::with_capacity(self.n_samples);

        for i in 0..self.n_samples {
            let (sample, distance) = self.perturb_instance(instance, i as u64);
            let pred = model.predict(&sample);
            let weight = self.kernel_fn(distance);

            perturbed_samples.push(sample);
            predictions.push(pred);
            weights.push(weight);
        }

        // Fit weighted linear model
        let (coefficients, intercept, r2) =
            self.fit_weighted_linear(&perturbed_samples, &predictions, &weights, n_features);

        let feature_weights: Vec<FeatureContribution> = self
            .feature_names
            .iter()
            .zip(instance.iter())
            .zip(coefficients.iter())
            .map(|((name, &value), &weight)| FeatureContribution {
                feature: name.clone(),
                value,
                contribution: weight,
                confidence: None,
            })
            .collect();

        LimeExplanation {
            original_prediction,
            intercept,
            feature_weights,
            local_fidelity: r2,
            n_samples: self.n_samples,
        }
    }

    /// Perturb an instance
    fn perturb_instance(&self, instance: &[f64], seed: u64) -> (Vec<f64>, f64) {
        let perturbed: Vec<f64> = instance
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                let noise = ((seed * 31 + i as u64) as f64 * 0.123456789).sin() * 0.1;
                v + noise * v.abs().max(1.0)
            })
            .collect();

        let distance: f64 = instance
            .iter()
            .zip(perturbed.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>()
            .sqrt();

        (perturbed, distance)
    }

    /// Kernel function for weighting samples
    fn kernel_fn(&self, distance: f64) -> f64 {
        (-distance.powi(2) / (2.0 * self.kernel_width.powi(2))).exp()
    }

    /// Fit weighted linear regression
    fn fit_weighted_linear(
        &self,
        samples: &[Vec<f64>],
        targets: &[f64],
        weights: &[f64],
        n_features: usize,
    ) -> (Vec<f64>, f64, f64) {
        // Simple weighted least squares
        let n = samples.len() as f64;
        let total_weight: f64 = weights.iter().sum();

        // Compute weighted means
        let mut x_means = vec![0.0; n_features];
        let mut y_mean = 0.0;

        for (i, (sample, &target)) in samples.iter().zip(targets.iter()).enumerate() {
            let w = weights[i] / total_weight;
            y_mean += w * target;
            for (j, &x) in sample.iter().enumerate() {
                x_means[j] += w * x;
            }
        }

        // Compute coefficients using gradient descent
        let mut coefficients = vec![0.0; n_features];
        let lr = 0.01;

        for _ in 0..100 {
            let mut gradients = vec![0.0; n_features];

            for (i, (sample, &target)) in samples.iter().zip(targets.iter()).enumerate() {
                let pred: f64 = sample
                    .iter()
                    .zip(coefficients.iter())
                    .map(|(x, c)| x * c)
                    .sum();
                let error = pred - target;

                for (j, &x) in sample.iter().enumerate() {
                    gradients[j] += weights[i] * error * x;
                }
            }

            for (c, g) in coefficients.iter_mut().zip(gradients.iter()) {
                *c -= lr * g / total_weight;
            }
        }

        // Compute intercept
        let intercept = y_mean
            - coefficients
                .iter()
                .zip(x_means.iter())
                .map(|(c, x)| c * x)
                .sum::<f64>();

        // Compute R²
        let ss_tot: f64 = targets
            .iter()
            .zip(weights.iter())
            .map(|(&t, &w)| w * (t - y_mean).powi(2))
            .sum();

        let ss_res: f64 = samples
            .iter()
            .zip(targets.iter())
            .zip(weights.iter())
            .map(|((s, &t), &w)| {
                let pred: f64 = s.iter().zip(coefficients.iter()).map(|(x, c)| x * c).sum();
                w * (t - pred - intercept).powi(2)
            })
            .sum();

        let r2 = if ss_tot > 0.0 {
            1.0 - ss_res / ss_tot
        } else {
            0.0
        };

        (coefficients, intercept, r2)
    }
}

/// Counterfactual explainer
pub struct CounterfactualExplainer {
    feature_names: Vec<String>,
    feature_ranges: Vec<(f64, f64)>,
    max_iterations: usize,
    target_class: Option<f64>,
}

impl CounterfactualExplainer {
    /// Create a new counterfactual explainer
    pub fn new(feature_names: Vec<String>, feature_ranges: Vec<(f64, f64)>) -> Self {
        Self {
            feature_names,
            feature_ranges,
            max_iterations: 1000,
            target_class: None,
        }
    }

    /// Set target class for classification
    pub fn with_target(mut self, target: f64) -> Self {
        self.target_class = Some(target);
        self
    }

    /// Generate counterfactual explanation
    pub fn explain<M: ExplainableModel>(
        &self,
        model: &M,
        instance: &[f64],
        desired_prediction: f64,
    ) -> Option<Counterfactual> {
        let original_prediction = model.predict(instance);
        let mut current = instance.to_vec();
        let mut best_cf: Option<Vec<f64>> = None;
        let mut best_distance = f64::MAX;

        // Gradient-free optimization to find counterfactual
        for iter in 0..self.max_iterations {
            // Try small perturbations
            for i in 0..instance.len() {
                let (min_val, max_val) = self.feature_ranges[i];
                let range = max_val - min_val;

                // Try increasing and decreasing
                for direction in &[-1.0, 1.0] {
                    let step = direction * range * 0.01 * (1.0 + (iter as f64 * 0.1).sin().abs());
                    let new_val = (current[i] + step).clamp(min_val, max_val);

                    let mut candidate = current.clone();
                    candidate[i] = new_val;

                    let pred = model.predict(&candidate);
                    let pred_diff = (pred - desired_prediction).abs();

                    // Check if this is a valid counterfactual
                    if pred_diff < (original_prediction - desired_prediction).abs() * 0.5 {
                        let distance = self.compute_distance(instance, &candidate);
                        if distance < best_distance {
                            best_distance = distance;
                            best_cf = Some(candidate.clone());
                        }
                    }

                    // Update current based on progress
                    if pred_diff < (model.predict(&current) - desired_prediction).abs() {
                        current[i] = new_val;
                    }
                }
            }
        }

        best_cf.map(|cf| {
            let changes: Vec<FeatureChange> = self
                .feature_names
                .iter()
                .zip(instance.iter())
                .zip(cf.iter())
                .filter(|((_, &orig), &new)| (orig - new).abs() > 1e-6)
                .map(|((name, &orig), &new)| FeatureChange {
                    feature: name.clone(),
                    original_value: orig,
                    new_value: new,
                    change_magnitude: (new - orig).abs(),
                })
                .collect();

            let mut original_map = HashMap::new();
            let mut cf_map = HashMap::new();

            for (i, name) in self.feature_names.iter().enumerate() {
                original_map.insert(name.clone(), instance[i]);
                cf_map.insert(name.clone(), cf[i]);
            }

            Counterfactual {
                original: original_map,
                original_prediction,
                counterfactual: cf_map,
                counterfactual_prediction: model.predict(&cf),
                changes: changes.clone(),
                distance: best_distance,
                sparsity: changes.len(),
            }
        })
    }

    fn compute_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        a.iter()
            .zip(b.iter())
            .zip(self.feature_ranges.iter())
            .map(|((&x, &y), &(min, max))| {
                let range = (max - min).max(1e-6);
                ((x - y) / range).powi(2)
            })
            .sum::<f64>()
            .sqrt()
    }
}

/// Permutation feature importance
pub struct PermutationImportance {
    n_repeats: usize,
}

impl PermutationImportance {
    pub fn new(n_repeats: usize) -> Self {
        Self { n_repeats }
    }

    /// Compute permutation importance
    pub fn compute<M: ExplainableModel>(
        &self,
        model: &M,
        data: &[Vec<f64>],
        targets: &[f64],
        feature_names: &[String],
    ) -> Vec<FeatureContribution> {
        let n_features = data[0].len();

        // Compute baseline score
        let baseline_score = self.compute_score(model, data, targets);

        // Compute importance for each feature
        let mut importances = Vec::with_capacity(n_features);

        for i in 0..n_features {
            let mut scores = Vec::with_capacity(self.n_repeats);

            for repeat in 0..self.n_repeats {
                // Permute feature i
                let permuted_data = self.permute_feature(data, i, repeat as u64);
                let score = self.compute_score(model, &permuted_data, targets);
                scores.push(baseline_score - score);
            }

            let mean_importance = scores.iter().sum::<f64>() / scores.len() as f64;
            let std_importance = (scores
                .iter()
                .map(|s| (s - mean_importance).powi(2))
                .sum::<f64>()
                / scores.len() as f64)
                .sqrt();

            importances.push(FeatureContribution {
                feature: feature_names[i].clone(),
                value: 0.0,
                contribution: mean_importance,
                confidence: Some(std_importance),
            });
        }

        // Sort by importance
        importances.sort_by(|a, b| {
            b.contribution
                .abs()
                .partial_cmp(&a.contribution.abs())
                .unwrap()
        });

        importances
    }

    fn compute_score<M: ExplainableModel>(
        &self,
        model: &M,
        data: &[Vec<f64>],
        targets: &[f64],
    ) -> f64 {
        // Use R² as score
        let predictions: Vec<f64> = data.iter().map(|x| model.predict(x)).collect();
        let mean_target = targets.iter().sum::<f64>() / targets.len() as f64;

        let ss_tot: f64 = targets.iter().map(|t| (t - mean_target).powi(2)).sum();
        let ss_res: f64 = predictions
            .iter()
            .zip(targets.iter())
            .map(|(p, t)| (t - p).powi(2))
            .sum();

        1.0 - ss_res / ss_tot.max(1e-10)
    }

    fn permute_feature(&self, data: &[Vec<f64>], feature_idx: usize, seed: u64) -> Vec<Vec<f64>> {
        let n = data.len();
        let mut result: Vec<Vec<f64>> = data.iter().cloned().collect();

        // Generate permutation
        let mut indices: Vec<usize> = (0..n).collect();
        for i in 0..n {
            let j = ((seed + i as u64) * 31) as usize % n;
            indices.swap(i, j);
        }

        // Apply permutation to feature
        for (i, &j) in indices.iter().enumerate() {
            result[i][feature_idx] = data[j][feature_idx];
        }

        result
    }
}

/// Global explainability registry
pub struct ExplainabilityRegistry {
    shap_explainers: RwLock<HashMap<String, ShapExplainer>>,
    lime_explainers: RwLock<HashMap<String, LimeExplainer>>,
    explanations_generated: AtomicU64,
}

impl ExplainabilityRegistry {
    pub fn new() -> Self {
        Self {
            shap_explainers: RwLock::new(HashMap::new()),
            lime_explainers: RwLock::new(HashMap::new()),
            explanations_generated: AtomicU64::new(0),
        }
    }

    pub fn register_shap(
        &self,
        name: &str,
        background_data: Vec<Vec<f64>>,
        feature_names: Vec<String>,
    ) {
        let explainer = ShapExplainer::new(background_data, feature_names);
        self.shap_explainers
            .write()
            .insert(name.to_string(), explainer);
    }

    pub fn register_lime(&self, name: &str, feature_names: Vec<String>) {
        let explainer = LimeExplainer::new(feature_names);
        self.lime_explainers
            .write()
            .insert(name.to_string(), explainer);
    }

    pub fn explain_shap<M: ExplainableModel>(
        &self,
        name: &str,
        model: &M,
        instance: &[f64],
    ) -> Option<ShapExplanation> {
        self.explanations_generated.fetch_add(1, Ordering::Relaxed);
        self.shap_explainers
            .read()
            .get(name)
            .map(|exp| exp.explain(model, instance))
    }

    pub fn explain_lime<M: ExplainableModel>(
        &self,
        name: &str,
        model: &M,
        instance: &[f64],
    ) -> Option<LimeExplanation> {
        self.explanations_generated.fetch_add(1, Ordering::Relaxed);
        self.lime_explainers
            .read()
            .get(name)
            .map(|exp| exp.explain(model, instance))
    }

    pub fn stats(&self) -> u64 {
        self.explanations_generated.load(Ordering::Relaxed)
    }
}

impl Default for ExplainabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_model() -> LinearExplainableModel {
        LinearExplainableModel::new(
            vec![2.0, -1.0, 0.5],
            1.0,
            vec!["x1".to_string(), "x2".to_string(), "x3".to_string()],
        )
    }

    fn create_background_data() -> Vec<Vec<f64>> {
        (0..100)
            .map(|i| {
                vec![
                    i as f64 / 10.0,
                    (i as f64 * 0.1).sin(),
                    (i as f64 * 0.05).cos(),
                ]
            })
            .collect()
    }

    #[test]
    fn test_shap_explanation() {
        let model = create_test_model();
        let background = create_background_data();
        let feature_names = model.feature_names();

        let explainer = ShapExplainer::new(background, feature_names);
        let instance = vec![1.0, 2.0, 3.0];
        let explanation = explainer.explain(&model, &instance);

        assert_eq!(explanation.shap_values.len(), 3);
        // SHAP values should roughly sum to prediction - base_value
    }

    #[test]
    fn test_lime_explanation() {
        let model = create_test_model();
        let feature_names = model.feature_names();

        let explainer = LimeExplainer::new(feature_names).with_samples(500);
        let instance = vec![1.0, 2.0, 3.0];
        let explanation = explainer.explain(&model, &instance);

        assert_eq!(explanation.feature_weights.len(), 3);
        assert!(explanation.local_fidelity >= -1.0 && explanation.local_fidelity <= 1.0);
    }

    #[test]
    fn test_top_features() {
        let explanation = ShapExplanation {
            base_value: 0.0,
            shap_values: vec![
                FeatureContribution {
                    feature: "a".to_string(),
                    value: 1.0,
                    contribution: 0.1,
                    confidence: None,
                },
                FeatureContribution {
                    feature: "b".to_string(),
                    value: 2.0,
                    contribution: 0.5,
                    confidence: None,
                },
                FeatureContribution {
                    feature: "c".to_string(),
                    value: 3.0,
                    contribution: -0.3,
                    confidence: None,
                },
            ],
            prediction: 0.3,
            interactions: None,
        };

        let top = explanation.top_features(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].feature, "b"); // highest absolute contribution
    }

    #[test]
    fn test_permutation_importance() {
        let model = create_test_model();
        let data = create_background_data();
        let targets: Vec<f64> = data.iter().map(|x| model.predict(x)).collect();
        let feature_names = model.feature_names();

        let pi = PermutationImportance::new(5);
        let importances = pi.compute(&model, &data, &targets, &feature_names);

        assert_eq!(importances.len(), 3);
        // x1 has highest weight (2.0), should have highest importance
    }

    #[test]
    fn test_counterfactual() {
        let model = create_test_model();
        let feature_names = model.feature_names();
        let feature_ranges = vec![(-10.0, 10.0), (-10.0, 10.0), (-10.0, 10.0)];

        let explainer = CounterfactualExplainer::new(feature_names, feature_ranges);
        let instance = vec![1.0, 1.0, 1.0];
        let original_pred = model.predict(&instance);

        let cf = explainer.explain(&model, &instance, original_pred + 2.0);

        if let Some(cf) = cf {
            assert!(cf.counterfactual_prediction > original_pred);
            assert!(cf.sparsity > 0);
        }
    }

    #[test]
    fn test_explainability_registry() {
        let registry = ExplainabilityRegistry::new();

        let background = create_background_data();
        let feature_names = vec!["x1".to_string(), "x2".to_string(), "x3".to_string()];

        registry.register_shap("test_model", background, feature_names.clone());
        registry.register_lime("test_model", feature_names);

        let model = create_test_model();
        let instance = vec![1.0, 2.0, 3.0];

        let shap = registry.explain_shap("test_model", &model, &instance);
        assert!(shap.is_some());

        let lime = registry.explain_lime("test_model", &model, &instance);
        assert!(lime.is_some());

        assert_eq!(registry.stats(), 2);
    }
}
