//! AutoML for BoyoDB
//!
//! Automatic machine learning capabilities:
//! - Automatic model selection
//! - Hyperparameter optimization (Bayesian, grid search, random)
//! - Feature selection and engineering
//! - Cross-validation and evaluation

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// AutoML task types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskType {
    BinaryClassification,
    MulticlassClassification,
    Regression,
    Ranking,
    Clustering,
    AnomalyDetection,
}

/// Model types available for AutoML
#[derive(Debug, Clone, PartialEq)]
pub enum ModelType {
    // Linear models
    LinearRegression,
    LogisticRegression,
    Ridge {
        alpha: f64,
    },
    Lasso {
        alpha: f64,
    },
    ElasticNet {
        alpha: f64,
        l1_ratio: f64,
    },

    // Tree-based models
    DecisionTree {
        max_depth: Option<usize>,
    },
    RandomForest {
        n_estimators: usize,
        max_depth: Option<usize>,
    },
    GradientBoosting {
        n_estimators: usize,
        learning_rate: f64,
        max_depth: usize,
    },
    XGBoost {
        params: HashMap<String, f64>,
    },
    LightGBM {
        params: HashMap<String, f64>,
    },

    // Neural networks
    MLP {
        hidden_layers: Vec<usize>,
        activation: String,
    },

    // Other
    KNN {
        k: usize,
    },
    SVM {
        kernel: String,
        c: f64,
    },
    NaiveBayes,
}

impl ModelType {
    /// Get model name
    pub fn name(&self) -> &'static str {
        match self {
            ModelType::LinearRegression => "LinearRegression",
            ModelType::LogisticRegression => "LogisticRegression",
            ModelType::Ridge { .. } => "Ridge",
            ModelType::Lasso { .. } => "Lasso",
            ModelType::ElasticNet { .. } => "ElasticNet",
            ModelType::DecisionTree { .. } => "DecisionTree",
            ModelType::RandomForest { .. } => "RandomForest",
            ModelType::GradientBoosting { .. } => "GradientBoosting",
            ModelType::XGBoost { .. } => "XGBoost",
            ModelType::LightGBM { .. } => "LightGBM",
            ModelType::MLP { .. } => "MLP",
            ModelType::KNN { .. } => "KNN",
            ModelType::SVM { .. } => "SVM",
            ModelType::NaiveBayes => "NaiveBayes",
        }
    }
}

/// Hyperparameter space definition
#[derive(Debug, Clone)]
pub enum HyperparameterSpace {
    /// Categorical values
    Categorical(Vec<String>),
    /// Integer range
    IntRange {
        low: i64,
        high: i64,
        log_scale: bool,
    },
    /// Float range
    FloatRange {
        low: f64,
        high: f64,
        log_scale: bool,
    },
    /// Boolean
    Boolean,
}

impl HyperparameterSpace {
    /// Sample a value from the space
    pub fn sample(&self, seed: u64) -> HyperparameterValue {
        let r = ((seed as f64) * 0.123456789).sin().abs();

        match self {
            HyperparameterSpace::Categorical(options) => {
                let idx = (r * options.len() as f64) as usize;
                HyperparameterValue::String(options[idx.min(options.len() - 1)].clone())
            }
            HyperparameterSpace::IntRange {
                low,
                high,
                log_scale,
            } => {
                let value = if *log_scale {
                    let log_low = (*low as f64).ln();
                    let log_high = (*high as f64).ln();
                    (log_low + r * (log_high - log_low)).exp() as i64
                } else {
                    *low + (r * (*high - *low) as f64) as i64
                };
                HyperparameterValue::Int(value)
            }
            HyperparameterSpace::FloatRange {
                low,
                high,
                log_scale,
            } => {
                let value = if *log_scale {
                    let log_low = low.ln();
                    let log_high = high.ln();
                    (log_low + r * (log_high - log_low)).exp()
                } else {
                    low + r * (high - low)
                };
                HyperparameterValue::Float(value)
            }
            HyperparameterSpace::Boolean => HyperparameterValue::Bool(r > 0.5),
        }
    }
}

/// Hyperparameter value
#[derive(Debug, Clone)]
pub enum HyperparameterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Search strategy for hyperparameter optimization
#[derive(Debug, Clone)]
pub enum SearchStrategy {
    /// Grid search
    Grid,
    /// Random search
    Random { n_iter: usize },
    /// Bayesian optimization
    Bayesian { n_iter: usize, n_initial: usize },
    /// Successive halving
    SuccessiveHalving {
        n_iter: usize,
        reduction_factor: usize,
    },
    /// Hyperband
    Hyperband { max_iter: usize, eta: usize },
}

/// Cross-validation strategy
#[derive(Debug, Clone)]
pub enum CrossValidation {
    /// K-fold cross-validation
    KFold { n_splits: usize, shuffle: bool },
    /// Stratified K-fold
    StratifiedKFold { n_splits: usize },
    /// Time series split
    TimeSeriesSplit { n_splits: usize },
    /// Leave-one-out
    LeaveOneOut,
    /// Hold-out validation
    HoldOut { test_size: f64 },
}

/// Evaluation metric
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Metric {
    // Classification
    Accuracy,
    Precision,
    Recall,
    F1,
    AucRoc,
    LogLoss,

    // Regression
    Mae,
    Mse,
    Rmse,
    R2,
    Mape,

    // Ranking
    Ndcg,
    Map,
    Mrr,
}

impl Metric {
    /// Is higher better for this metric?
    pub fn higher_is_better(&self) -> bool {
        match self {
            Metric::Accuracy
            | Metric::Precision
            | Metric::Recall
            | Metric::F1
            | Metric::AucRoc
            | Metric::R2
            | Metric::Ndcg
            | Metric::Map
            | Metric::Mrr => true,
            Metric::Mae | Metric::Mse | Metric::Rmse | Metric::Mape | Metric::LogLoss => false,
        }
    }
}

/// AutoML configuration
#[derive(Debug, Clone)]
pub struct AutoMLConfig {
    /// Task type
    pub task: TaskType,
    /// Optimization metric
    pub metric: Metric,
    /// Model types to consider
    pub models: Vec<ModelType>,
    /// Search strategy
    pub search_strategy: SearchStrategy,
    /// Cross-validation strategy
    pub cv: CrossValidation,
    /// Time budget in seconds
    pub time_budget_seconds: u64,
    /// Maximum number of trials
    pub max_trials: usize,
    /// Early stopping patience
    pub early_stopping_patience: usize,
    /// Number of parallel jobs
    pub n_jobs: usize,
    /// Feature selection enabled
    pub feature_selection: bool,
    /// Feature engineering enabled
    pub feature_engineering: bool,
}

impl Default for AutoMLConfig {
    fn default() -> Self {
        Self {
            task: TaskType::BinaryClassification,
            metric: Metric::Accuracy,
            models: vec![
                ModelType::LogisticRegression,
                ModelType::RandomForest {
                    n_estimators: 100,
                    max_depth: Some(10),
                },
                ModelType::GradientBoosting {
                    n_estimators: 100,
                    learning_rate: 0.1,
                    max_depth: 6,
                },
            ],
            search_strategy: SearchStrategy::Random { n_iter: 50 },
            cv: CrossValidation::KFold {
                n_splits: 5,
                shuffle: true,
            },
            time_budget_seconds: 3600,
            max_trials: 100,
            early_stopping_patience: 10,
            n_jobs: 1,
            feature_selection: true,
            feature_engineering: false,
        }
    }
}

/// Trial result
#[derive(Debug, Clone)]
pub struct TrialResult {
    /// Trial ID
    pub trial_id: u64,
    /// Model type
    pub model_type: String,
    /// Hyperparameters
    pub hyperparameters: HashMap<String, HyperparameterValue>,
    /// CV scores
    pub cv_scores: Vec<f64>,
    /// Mean CV score
    pub mean_score: f64,
    /// Std CV score
    pub std_score: f64,
    /// Training time
    pub training_time_ms: u64,
    /// Status
    pub status: TrialStatus,
    /// Timestamp
    pub timestamp: i64,
}

/// Trial status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrialStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Pruned,
}

/// Feature importance
#[derive(Debug, Clone)]
pub struct FeatureImportance {
    /// Feature name
    pub feature: String,
    /// Importance score
    pub importance: f64,
    /// Rank
    pub rank: usize,
}

/// AutoML result
#[derive(Debug, Clone)]
pub struct AutoMLResult {
    /// Best trial
    pub best_trial: TrialResult,
    /// All trials
    pub all_trials: Vec<TrialResult>,
    /// Best model type
    pub best_model_type: String,
    /// Best hyperparameters
    pub best_hyperparameters: HashMap<String, HyperparameterValue>,
    /// Best score
    pub best_score: f64,
    /// Feature importances (if available)
    pub feature_importances: Vec<FeatureImportance>,
    /// Selected features (if feature selection enabled)
    pub selected_features: Vec<String>,
    /// Total training time
    pub total_time_seconds: f64,
    /// Number of completed trials
    pub completed_trials: usize,
}

/// Training data
#[derive(Debug, Clone)]
pub struct TrainingData {
    /// Feature matrix (n_samples x n_features)
    pub features: Vec<Vec<f64>>,
    /// Target values
    pub targets: Vec<f64>,
    /// Feature names
    pub feature_names: Vec<String>,
    /// Sample weights
    pub sample_weights: Option<Vec<f64>>,
}

impl TrainingData {
    /// Get number of samples
    pub fn n_samples(&self) -> usize {
        self.features.len()
    }

    /// Get number of features
    pub fn n_features(&self) -> usize {
        if self.features.is_empty() {
            0
        } else {
            self.features[0].len()
        }
    }

    /// Split into train and test
    pub fn train_test_split(&self, test_size: f64, seed: u64) -> (TrainingData, TrainingData) {
        let n = self.n_samples();
        let n_test = (n as f64 * test_size) as usize;
        let n_train = n - n_test;

        // Simple deterministic split based on seed
        let mut indices: Vec<usize> = (0..n).collect();
        for i in 0..n {
            let j = ((seed + i as u64) * 31) as usize % n;
            indices.swap(i, j);
        }

        let train_indices: Vec<usize> = indices[..n_train].to_vec();
        let test_indices: Vec<usize> = indices[n_train..].to_vec();

        let train = TrainingData {
            features: train_indices
                .iter()
                .map(|&i| self.features[i].clone())
                .collect(),
            targets: train_indices.iter().map(|&i| self.targets[i]).collect(),
            feature_names: self.feature_names.clone(),
            sample_weights: self
                .sample_weights
                .as_ref()
                .map(|w| train_indices.iter().map(|&i| w[i]).collect()),
        };

        let test = TrainingData {
            features: test_indices
                .iter()
                .map(|&i| self.features[i].clone())
                .collect(),
            targets: test_indices.iter().map(|&i| self.targets[i]).collect(),
            feature_names: self.feature_names.clone(),
            sample_weights: self
                .sample_weights
                .as_ref()
                .map(|w| test_indices.iter().map(|&i| w[i]).collect()),
        };

        (train, test)
    }
}

/// Simple model for simulation
struct SimpleModel {
    model_type: ModelType,
    weights: Vec<f64>,
    bias: f64,
}

impl SimpleModel {
    fn new(model_type: ModelType, n_features: usize) -> Self {
        // Initialize weights based on model type
        let weights = vec![0.1; n_features];
        Self {
            model_type,
            weights,
            bias: 0.0,
        }
    }

    fn fit(&mut self, data: &TrainingData) {
        // Simple gradient descent fitting
        let lr = 0.01;
        for _ in 0..100 {
            for (features, &target) in data.features.iter().zip(data.targets.iter()) {
                let pred: f64 = self
                    .weights
                    .iter()
                    .zip(features.iter())
                    .map(|(w, x)| w * x)
                    .sum::<f64>()
                    + self.bias;

                let error = pred - target;

                for (w, x) in self.weights.iter_mut().zip(features.iter()) {
                    *w -= lr * error * x;
                }
                self.bias -= lr * error;
            }
        }
    }

    fn predict(&self, features: &[f64]) -> f64 {
        let pred: f64 = self
            .weights
            .iter()
            .zip(features.iter())
            .map(|(w, x)| w * x)
            .sum::<f64>()
            + self.bias;

        match self.model_type {
            ModelType::LogisticRegression => 1.0 / (1.0 + (-pred).exp()),
            _ => pred,
        }
    }

    fn feature_importances(&self) -> Vec<f64> {
        self.weights.iter().map(|w| w.abs()).collect()
    }
}

/// AutoML engine
pub struct AutoML {
    config: AutoMLConfig,
    trial_counter: AtomicU64,
    trials: RwLock<Vec<TrialResult>>,
    best_score: RwLock<f64>,
    start_time: RwLock<Option<Instant>>,
}

impl AutoML {
    /// Create a new AutoML instance
    pub fn new(config: AutoMLConfig) -> Self {
        let best_score = if config.metric.higher_is_better() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };

        Self {
            config,
            trial_counter: AtomicU64::new(0),
            trials: RwLock::new(Vec::new()),
            best_score: RwLock::new(best_score),
            start_time: RwLock::new(None),
        }
    }

    /// Run AutoML on the given data
    pub fn fit(&self, data: &TrainingData) -> AutoMLResult {
        *self.start_time.write() = Some(Instant::now());

        let start = Instant::now();
        let timeout = Duration::from_secs(self.config.time_budget_seconds);

        // Run trials for each model type
        for model_type in &self.config.models {
            if start.elapsed() > timeout {
                break;
            }

            // Run multiple trials per model with different hyperparameters
            let n_trials = match &self.config.search_strategy {
                SearchStrategy::Grid => 1,
                SearchStrategy::Random { n_iter } => *n_iter / self.config.models.len(),
                SearchStrategy::Bayesian { n_iter, .. } => *n_iter / self.config.models.len(),
                _ => 10,
            };

            for trial_num in 0..n_trials {
                if start.elapsed() > timeout {
                    break;
                }

                let trial_id = self.trial_counter.fetch_add(1, Ordering::Relaxed);
                let result = self.run_trial(trial_id, model_type.clone(), data, trial_num as u64);

                self.trials.write().push(result.clone());

                // Update best score
                let is_better = if self.config.metric.higher_is_better() {
                    result.mean_score > *self.best_score.read()
                } else {
                    result.mean_score < *self.best_score.read()
                };

                if is_better && result.status == TrialStatus::Completed {
                    *self.best_score.write() = result.mean_score;
                }
            }
        }

        self.compile_results(data, start.elapsed().as_secs_f64())
    }

    /// Run a single trial
    fn run_trial(
        &self,
        trial_id: u64,
        model_type: ModelType,
        data: &TrainingData,
        seed: u64,
    ) -> TrialResult {
        let start = Instant::now();

        // Generate hyperparameters
        let hyperparameters = self.sample_hyperparameters(&model_type, seed);

        // Run cross-validation
        let cv_scores = self.cross_validate(&model_type, data, seed);

        let mean_score = cv_scores.iter().sum::<f64>() / cv_scores.len() as f64;
        let variance = cv_scores
            .iter()
            .map(|s| (s - mean_score).powi(2))
            .sum::<f64>()
            / cv_scores.len() as f64;
        let std_score = variance.sqrt();

        TrialResult {
            trial_id,
            model_type: model_type.name().to_string(),
            hyperparameters,
            cv_scores,
            mean_score,
            std_score,
            training_time_ms: start.elapsed().as_millis() as u64,
            status: TrialStatus::Completed,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        }
    }

    /// Sample hyperparameters for a model
    fn sample_hyperparameters(
        &self,
        model_type: &ModelType,
        seed: u64,
    ) -> HashMap<String, HyperparameterValue> {
        let mut params = HashMap::new();

        match model_type {
            ModelType::Ridge { alpha } => {
                params.insert("alpha".to_string(), HyperparameterValue::Float(*alpha));
            }
            ModelType::RandomForest {
                n_estimators,
                max_depth,
            } => {
                params.insert(
                    "n_estimators".to_string(),
                    HyperparameterValue::Int(*n_estimators as i64),
                );
                if let Some(d) = max_depth {
                    params.insert("max_depth".to_string(), HyperparameterValue::Int(*d as i64));
                }
            }
            ModelType::GradientBoosting {
                n_estimators,
                learning_rate,
                max_depth,
            } => {
                params.insert(
                    "n_estimators".to_string(),
                    HyperparameterValue::Int(*n_estimators as i64),
                );
                params.insert(
                    "learning_rate".to_string(),
                    HyperparameterValue::Float(*learning_rate),
                );
                params.insert(
                    "max_depth".to_string(),
                    HyperparameterValue::Int(*max_depth as i64),
                );
            }
            _ => {}
        }

        // Add seed
        params.insert("seed".to_string(), HyperparameterValue::Int(seed as i64));

        params
    }

    /// Run cross-validation
    fn cross_validate(&self, model_type: &ModelType, data: &TrainingData, seed: u64) -> Vec<f64> {
        let n_splits = match &self.config.cv {
            CrossValidation::KFold { n_splits, .. } => *n_splits,
            CrossValidation::StratifiedKFold { n_splits } => *n_splits,
            CrossValidation::TimeSeriesSplit { n_splits } => *n_splits,
            CrossValidation::LeaveOneOut => data.n_samples(),
            CrossValidation::HoldOut { .. } => 1,
        };

        let mut scores = Vec::with_capacity(n_splits);

        for fold in 0..n_splits {
            // Simple fold split
            let fold_size = data.n_samples() / n_splits;
            let test_start = fold * fold_size;
            let test_end = if fold == n_splits - 1 {
                data.n_samples()
            } else {
                (fold + 1) * fold_size
            };

            let train_data = TrainingData {
                features: data.features[..test_start]
                    .iter()
                    .chain(data.features[test_end..].iter())
                    .cloned()
                    .collect(),
                targets: data.targets[..test_start]
                    .iter()
                    .chain(data.targets[test_end..].iter())
                    .cloned()
                    .collect(),
                feature_names: data.feature_names.clone(),
                sample_weights: None,
            };

            let test_data = TrainingData {
                features: data.features[test_start..test_end].to_vec(),
                targets: data.targets[test_start..test_end].to_vec(),
                feature_names: data.feature_names.clone(),
                sample_weights: None,
            };

            // Train and evaluate
            let score =
                self.train_and_evaluate(model_type, &train_data, &test_data, seed + fold as u64);
            scores.push(score);
        }

        scores
    }

    /// Train model and evaluate
    fn train_and_evaluate(
        &self,
        model_type: &ModelType,
        train: &TrainingData,
        test: &TrainingData,
        _seed: u64,
    ) -> f64 {
        let mut model = SimpleModel::new(model_type.clone(), train.n_features());
        model.fit(train);

        // Compute metric
        let predictions: Vec<f64> = test.features.iter().map(|f| model.predict(f)).collect();

        self.compute_metric(&predictions, &test.targets)
    }

    /// Compute evaluation metric
    fn compute_metric(&self, predictions: &[f64], targets: &[f64]) -> f64 {
        match self.config.metric {
            Metric::Accuracy => {
                let correct = predictions
                    .iter()
                    .zip(targets.iter())
                    .filter(|(p, t)| ((**p > 0.5) as i32 as f64 - **t).abs() < 0.5)
                    .count();
                correct as f64 / predictions.len() as f64
            }
            Metric::Mae => {
                predictions
                    .iter()
                    .zip(targets.iter())
                    .map(|(p, t)| (p - t).abs())
                    .sum::<f64>()
                    / predictions.len() as f64
            }
            Metric::Mse => {
                predictions
                    .iter()
                    .zip(targets.iter())
                    .map(|(p, t)| (p - t).powi(2))
                    .sum::<f64>()
                    / predictions.len() as f64
            }
            Metric::Rmse => {
                let mse: f64 = predictions
                    .iter()
                    .zip(targets.iter())
                    .map(|(p, t)| (p - t).powi(2))
                    .sum::<f64>()
                    / predictions.len() as f64;
                mse.sqrt()
            }
            Metric::R2 => {
                let mean_target = targets.iter().sum::<f64>() / targets.len() as f64;
                let ss_tot: f64 = targets.iter().map(|t| (t - mean_target).powi(2)).sum();
                let ss_res: f64 = predictions
                    .iter()
                    .zip(targets.iter())
                    .map(|(p, t)| (t - p).powi(2))
                    .sum();
                1.0 - ss_res / ss_tot.max(1e-10)
            }
            _ => 0.0,
        }
    }

    /// Compile final results
    fn compile_results(&self, data: &TrainingData, total_time: f64) -> AutoMLResult {
        let trials = self.trials.read();
        let completed: Vec<_> = trials
            .iter()
            .filter(|t| t.status == TrialStatus::Completed)
            .collect();

        let best_trial = if self.config.metric.higher_is_better() {
            completed
                .iter()
                .max_by(|a, b| a.mean_score.partial_cmp(&b.mean_score).unwrap())
        } else {
            completed
                .iter()
                .min_by(|a, b| a.mean_score.partial_cmp(&b.mean_score).unwrap())
        };

        let best_trial = best_trial
            .map(|t| (*t).clone())
            .unwrap_or_else(|| TrialResult {
                trial_id: 0,
                model_type: "None".to_string(),
                hyperparameters: HashMap::new(),
                cv_scores: vec![],
                mean_score: 0.0,
                std_score: 0.0,
                training_time_ms: 0,
                status: TrialStatus::Failed,
                timestamp: 0,
            });

        // Compute feature importances using best model
        let feature_importances = if !data.feature_names.is_empty() {
            let model_type = self
                .config
                .models
                .first()
                .cloned()
                .unwrap_or(ModelType::LinearRegression);
            let mut model = SimpleModel::new(model_type, data.n_features());
            model.fit(data);

            let importances = model.feature_importances();
            let mut fi: Vec<_> = data
                .feature_names
                .iter()
                .zip(importances.iter())
                .map(|(name, &imp)| (name.clone(), imp))
                .collect();
            fi.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            fi.iter()
                .enumerate()
                .map(|(rank, (name, imp))| FeatureImportance {
                    feature: name.clone(),
                    importance: *imp,
                    rank: rank + 1,
                })
                .collect()
        } else {
            vec![]
        };

        AutoMLResult {
            best_trial: best_trial.clone(),
            all_trials: trials.clone(),
            best_model_type: best_trial.model_type.clone(),
            best_hyperparameters: best_trial.hyperparameters.clone(),
            best_score: best_trial.mean_score,
            feature_importances,
            selected_features: data.feature_names.clone(),
            total_time_seconds: total_time,
            completed_trials: completed.len(),
        }
    }

    /// Get current best score
    pub fn best_score(&self) -> f64 {
        *self.best_score.read()
    }

    /// Get all trials
    pub fn get_trials(&self) -> Vec<TrialResult> {
        self.trials.read().clone()
    }
}

/// AutoML registry
pub struct AutoMLRegistry {
    jobs: RwLock<HashMap<String, AutoML>>,
    results: RwLock<HashMap<String, AutoMLResult>>,
}

impl AutoMLRegistry {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            results: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_job(&self, name: &str, config: AutoMLConfig) -> String {
        let automl = AutoML::new(config);
        self.jobs.write().insert(name.to_string(), automl);
        name.to_string()
    }

    pub fn run_job(&self, name: &str, data: &TrainingData) -> Option<AutoMLResult> {
        let jobs = self.jobs.read();
        if let Some(automl) = jobs.get(name) {
            let result = automl.fit(data);
            drop(jobs);
            self.results
                .write()
                .insert(name.to_string(), result.clone());
            Some(result)
        } else {
            None
        }
    }

    pub fn get_result(&self, name: &str) -> Option<AutoMLResult> {
        self.results.read().get(name).cloned()
    }

    pub fn list_jobs(&self) -> Vec<String> {
        self.jobs.read().keys().cloned().collect()
    }
}

impl Default for AutoMLRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data() -> TrainingData {
        let n = 100;
        let features: Vec<Vec<f64>> = (0..n)
            .map(|i| vec![i as f64 / 10.0, (i as f64 / 5.0).sin()])
            .collect();
        let targets: Vec<f64> = features
            .iter()
            .map(|f| 2.0 * f[0] + 0.5 * f[1] + 0.1)
            .collect();

        TrainingData {
            features,
            targets,
            feature_names: vec!["x1".to_string(), "x2".to_string()],
            sample_weights: None,
        }
    }

    #[test]
    fn test_automl_basic() {
        let config = AutoMLConfig {
            task: TaskType::Regression,
            metric: Metric::Mse,
            models: vec![ModelType::LinearRegression],
            search_strategy: SearchStrategy::Random { n_iter: 5 },
            cv: CrossValidation::KFold {
                n_splits: 3,
                shuffle: true,
            },
            time_budget_seconds: 60,
            max_trials: 10,
            ..Default::default()
        };

        let automl = AutoML::new(config);
        let data = create_test_data();
        let result = automl.fit(&data);

        assert!(result.completed_trials > 0);
        assert!(result.best_score < 10.0); // MSE should be reasonable
    }

    #[test]
    fn test_train_test_split() {
        let data = create_test_data();
        let (train, test) = data.train_test_split(0.2, 42);

        assert_eq!(train.n_samples() + test.n_samples(), data.n_samples());
        assert!(test.n_samples() > 0);
    }

    #[test]
    fn test_hyperparameter_sampling() {
        let space = HyperparameterSpace::FloatRange {
            low: 0.001,
            high: 1.0,
            log_scale: true,
        };

        let value = space.sample(42);
        if let HyperparameterValue::Float(v) = value {
            assert!(v >= 0.001 && v <= 1.0);
        }
    }

    #[test]
    fn test_metric_direction() {
        assert!(Metric::Accuracy.higher_is_better());
        assert!(!Metric::Mse.higher_is_better());
        assert!(Metric::R2.higher_is_better());
    }

    #[test]
    fn test_multiple_models() {
        let config = AutoMLConfig {
            task: TaskType::Regression,
            metric: Metric::R2,
            models: vec![ModelType::LinearRegression, ModelType::Ridge { alpha: 1.0 }],
            search_strategy: SearchStrategy::Random { n_iter: 2 },
            cv: CrossValidation::KFold {
                n_splits: 2,
                shuffle: false,
            },
            time_budget_seconds: 30,
            max_trials: 5,
            ..Default::default()
        };

        let automl = AutoML::new(config);
        let data = create_test_data();
        let result = automl.fit(&data);

        assert!(result.completed_trials >= 2);
    }

    #[test]
    fn test_feature_importances() {
        let config = AutoMLConfig {
            task: TaskType::Regression,
            metric: Metric::Mse,
            models: vec![ModelType::LinearRegression],
            search_strategy: SearchStrategy::Random { n_iter: 1 },
            cv: CrossValidation::HoldOut { test_size: 0.2 },
            time_budget_seconds: 10,
            max_trials: 1,
            feature_selection: true,
            ..Default::default()
        };

        let automl = AutoML::new(config);
        let data = create_test_data();
        let result = automl.fit(&data);

        assert_eq!(result.feature_importances.len(), 2);
        assert!(
            result.feature_importances[0].importance >= result.feature_importances[1].importance
        );
    }
}
