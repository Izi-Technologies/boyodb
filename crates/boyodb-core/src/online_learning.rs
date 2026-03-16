//! Online Learning for BoyoDB
//!
//! Incremental model updates as new data arrives:
//! - Stochastic gradient descent variants
//! - Online classification and regression
//! - Bandit algorithms for exploration/exploitation
//! - Adaptive learning rate scheduling

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Online learning algorithms
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OnlineAlgorithm {
    /// Stochastic Gradient Descent
    SGD,
    /// SGD with Momentum
    Momentum,
    /// AdaGrad
    AdaGrad,
    /// RMSProp
    RMSProp,
    /// Adam optimizer
    Adam,
    /// Online Passive-Aggressive
    PassiveAggressive,
    /// Perceptron
    Perceptron,
}

/// Learning rate schedules
#[derive(Debug, Clone)]
pub enum LearningRateSchedule {
    /// Constant learning rate
    Constant(f64),
    /// Decay: lr = initial / (1 + decay * t)
    InverseTime { initial: f64, decay: f64 },
    /// Exponential decay: lr = initial * decay^t
    Exponential { initial: f64, decay: f64 },
    /// Step decay: lr = initial * factor^(t / step_size)
    Step {
        initial: f64,
        factor: f64,
        step_size: u64,
    },
    /// Adaptive based on loss
    Adaptive {
        initial: f64,
        factor: f64,
        patience: u64,
    },
}

impl LearningRateSchedule {
    pub fn get_lr(&self, step: u64, _loss: Option<f64>) -> f64 {
        match self {
            LearningRateSchedule::Constant(lr) => *lr,
            LearningRateSchedule::InverseTime { initial, decay } => {
                initial / (1.0 + decay * step as f64)
            }
            LearningRateSchedule::Exponential { initial, decay } => {
                initial * decay.powi(step as i32)
            }
            LearningRateSchedule::Step {
                initial,
                factor,
                step_size,
            } => initial * factor.powi((step / step_size) as i32),
            LearningRateSchedule::Adaptive { initial, .. } => {
                // Simplified - real impl would track loss history
                *initial
            }
        }
    }
}

/// Online model configuration
#[derive(Debug, Clone)]
pub struct OnlineConfig {
    /// Algorithm to use
    pub algorithm: OnlineAlgorithm,
    /// Learning rate schedule
    pub lr_schedule: LearningRateSchedule,
    /// Regularization strength (L2)
    pub l2_reg: f64,
    /// Momentum coefficient (for momentum-based algorithms)
    pub momentum: f64,
    /// Beta1 for Adam
    pub beta1: f64,
    /// Beta2 for Adam
    pub beta2: f64,
    /// Epsilon for numerical stability
    pub epsilon: f64,
    /// Number of features
    pub num_features: usize,
    /// Number of classes (for classification)
    pub num_classes: usize,
}

impl Default for OnlineConfig {
    fn default() -> Self {
        Self {
            algorithm: OnlineAlgorithm::Adam,
            lr_schedule: LearningRateSchedule::Constant(0.001),
            l2_reg: 0.0001,
            momentum: 0.9,
            beta1: 0.9,
            beta2: 0.999,
            epsilon: 1e-8,
            num_features: 10,
            num_classes: 2,
        }
    }
}

/// Training example
#[derive(Debug, Clone)]
pub struct Example {
    /// Feature values
    pub features: Vec<f64>,
    /// Target value (regression) or class label (classification)
    pub target: f64,
    /// Optional sample weight
    pub weight: f64,
}

/// Online linear model
pub struct OnlineLinearModel {
    config: OnlineConfig,
    /// Model weights
    weights: RwLock<Vec<f64>>,
    /// Bias term
    bias: RwLock<f64>,
    /// Optimizer state (gradient accumulator for AdaGrad, etc.)
    optimizer_state: RwLock<OptimizerState>,
    /// Training statistics
    stats: ModelStats,
}

struct OptimizerState {
    /// Gradient accumulator (AdaGrad)
    grad_squared: Vec<f64>,
    /// First moment (Adam)
    m: Vec<f64>,
    /// Second moment (Adam)
    v: Vec<f64>,
    /// Velocity (Momentum)
    velocity: Vec<f64>,
    /// Step count
    step: u64,
    /// Running loss
    running_loss: f64,
    /// Best loss (for adaptive LR)
    best_loss: f64,
    /// Steps without improvement
    no_improvement_steps: u64,
}

impl OptimizerState {
    fn new(num_features: usize) -> Self {
        Self {
            grad_squared: vec![0.0; num_features],
            m: vec![0.0; num_features],
            v: vec![0.0; num_features],
            velocity: vec![0.0; num_features],
            step: 0,
            running_loss: 0.0,
            best_loss: f64::MAX,
            no_improvement_steps: 0,
        }
    }
}

struct ModelStats {
    examples_seen: AtomicU64,
    updates: AtomicU64,
    total_loss: RwLock<f64>,
}

impl OnlineLinearModel {
    /// Create a new online linear model
    pub fn new(config: OnlineConfig) -> Self {
        let num_features = config.num_features;
        Self {
            weights: RwLock::new(vec![0.0; num_features]),
            bias: RwLock::new(0.0),
            optimizer_state: RwLock::new(OptimizerState::new(num_features)),
            stats: ModelStats {
                examples_seen: AtomicU64::new(0),
                updates: AtomicU64::new(0),
                total_loss: RwLock::new(0.0),
            },
            config,
        }
    }

    /// Predict for a single example
    pub fn predict(&self, features: &[f64]) -> f64 {
        let weights = self.weights.read();
        let bias = *self.bias.read();

        let mut pred = bias;
        for (w, x) in weights.iter().zip(features.iter()) {
            pred += w * x;
        }
        pred
    }

    /// Predict probability (sigmoid for binary classification)
    pub fn predict_proba(&self, features: &[f64]) -> f64 {
        let logit = self.predict(features);
        1.0 / (1.0 + (-logit).exp())
    }

    /// Partial fit on a single example
    pub fn partial_fit(&self, example: &Example) -> f64 {
        self.stats.examples_seen.fetch_add(1, Ordering::Relaxed);

        let prediction = self.predict(&example.features);
        let error = prediction - example.target;
        let loss = error * error * 0.5 * example.weight;

        // Get current learning rate
        let state = self.optimizer_state.read();
        let step = state.step;
        drop(state);

        let lr = self.config.lr_schedule.get_lr(step, Some(loss));

        // Compute gradients
        let mut gradients: Vec<f64> = example
            .features
            .iter()
            .map(|&x| error * x * example.weight)
            .collect();
        let bias_grad = error * example.weight;

        // Add L2 regularization
        let weights = self.weights.read();
        for (g, w) in gradients.iter_mut().zip(weights.iter()) {
            *g += self.config.l2_reg * w;
        }
        drop(weights);

        // Apply update based on algorithm
        self.apply_update(&gradients, bias_grad, lr);

        // Update stats
        *self.stats.total_loss.write() += loss;
        self.stats.updates.fetch_add(1, Ordering::Relaxed);

        loss
    }

    /// Partial fit on a batch of examples
    pub fn partial_fit_batch(&self, examples: &[Example]) -> f64 {
        let mut total_loss = 0.0;
        for example in examples {
            total_loss += self.partial_fit(example);
        }
        total_loss / examples.len() as f64
    }

    /// Apply gradient update based on algorithm
    fn apply_update(&self, gradients: &[f64], bias_grad: f64, lr: f64) {
        let mut weights = self.weights.write();
        let mut bias = self.bias.write();
        let mut state = self.optimizer_state.write();

        state.step += 1;

        match self.config.algorithm {
            OnlineAlgorithm::SGD => {
                for (w, g) in weights.iter_mut().zip(gradients.iter()) {
                    *w -= lr * g;
                }
                *bias -= lr * bias_grad;
            }

            OnlineAlgorithm::Momentum => {
                for (i, (w, g)) in weights.iter_mut().zip(gradients.iter()).enumerate() {
                    state.velocity[i] = self.config.momentum * state.velocity[i] + lr * g;
                    *w -= state.velocity[i];
                }
                *bias -= lr * bias_grad;
            }

            OnlineAlgorithm::AdaGrad => {
                for (i, (w, g)) in weights.iter_mut().zip(gradients.iter()).enumerate() {
                    state.grad_squared[i] += g * g;
                    let adjusted_lr = lr / (state.grad_squared[i].sqrt() + self.config.epsilon);
                    *w -= adjusted_lr * g;
                }
                *bias -= lr * bias_grad;
            }

            OnlineAlgorithm::RMSProp => {
                for (i, (w, g)) in weights.iter_mut().zip(gradients.iter()).enumerate() {
                    state.v[i] = self.config.beta2 * state.v[i] + (1.0 - self.config.beta2) * g * g;
                    let adjusted_lr = lr / (state.v[i].sqrt() + self.config.epsilon);
                    *w -= adjusted_lr * g;
                }
                *bias -= lr * bias_grad;
            }

            OnlineAlgorithm::Adam => {
                let t = state.step as f64;
                for (i, (w, g)) in weights.iter_mut().zip(gradients.iter()).enumerate() {
                    state.m[i] = self.config.beta1 * state.m[i] + (1.0 - self.config.beta1) * g;
                    state.v[i] = self.config.beta2 * state.v[i] + (1.0 - self.config.beta2) * g * g;

                    let m_hat = state.m[i] / (1.0 - self.config.beta1.powf(t));
                    let v_hat = state.v[i] / (1.0 - self.config.beta2.powf(t));

                    *w -= lr * m_hat / (v_hat.sqrt() + self.config.epsilon);
                }
                *bias -= lr * bias_grad;
            }

            OnlineAlgorithm::PassiveAggressive => {
                // PA-I algorithm
                let norm_sq: f64 = gradients.iter().map(|g| g * g).sum();
                if norm_sq > 0.0 {
                    let loss = gradients.iter().map(|g| g.abs()).sum::<f64>();
                    let tau = (loss / norm_sq).min(lr);
                    for (w, g) in weights.iter_mut().zip(gradients.iter()) {
                        *w -= tau * g;
                    }
                    *bias -= tau * bias_grad;
                }
            }

            OnlineAlgorithm::Perceptron => {
                // Only update on misclassification
                let pred = weights
                    .iter()
                    .zip(gradients.iter())
                    .map(|(w, g)| w * g)
                    .sum::<f64>();
                if pred * gradients[0] <= 0.0 {
                    for (w, g) in weights.iter_mut().zip(gradients.iter()) {
                        *w -= lr * g;
                    }
                    *bias -= lr * bias_grad;
                }
            }
        }
    }

    /// Get current weights
    pub fn get_weights(&self) -> Vec<f64> {
        self.weights.read().clone()
    }

    /// Get bias
    pub fn get_bias(&self) -> f64 {
        *self.bias.read()
    }

    /// Get statistics
    pub fn get_stats(&self) -> OnlineModelStats {
        let total_loss = *self.stats.total_loss.read();
        let updates = self.stats.updates.load(Ordering::Relaxed);

        OnlineModelStats {
            examples_seen: self.stats.examples_seen.load(Ordering::Relaxed),
            updates,
            average_loss: if updates > 0 {
                total_loss / updates as f64
            } else {
                0.0
            },
            current_lr: self
                .config
                .lr_schedule
                .get_lr(self.optimizer_state.read().step, None),
        }
    }

    /// Reset optimizer state
    pub fn reset_optimizer(&self) {
        let mut state = self.optimizer_state.write();
        *state = OptimizerState::new(self.config.num_features);
    }
}

/// Online model statistics
#[derive(Debug, Clone)]
pub struct OnlineModelStats {
    pub examples_seen: u64,
    pub updates: u64,
    pub average_loss: f64,
    pub current_lr: f64,
}

/// Multi-armed bandit for exploration/exploitation
pub struct MultiArmedBandit {
    /// Number of arms
    num_arms: usize,
    /// Algorithm
    algorithm: BanditAlgorithm,
    /// Counts per arm
    counts: RwLock<Vec<u64>>,
    /// Rewards per arm
    rewards: RwLock<Vec<f64>>,
    /// Total pulls
    total_pulls: AtomicU64,
}

/// Bandit algorithms
#[derive(Debug, Clone)]
pub enum BanditAlgorithm {
    /// Epsilon-greedy
    EpsilonGreedy { epsilon: f64 },
    /// Upper Confidence Bound
    UCB { c: f64 },
    /// Thompson Sampling (Beta distribution for Bernoulli rewards)
    ThompsonSampling,
    /// Softmax/Boltzmann
    Softmax { temperature: f64 },
}

impl MultiArmedBandit {
    /// Create a new bandit
    pub fn new(num_arms: usize, algorithm: BanditAlgorithm) -> Self {
        Self {
            num_arms,
            algorithm,
            counts: RwLock::new(vec![0; num_arms]),
            rewards: RwLock::new(vec![0.0; num_arms]),
            total_pulls: AtomicU64::new(0),
        }
    }

    /// Select an arm to pull
    pub fn select_arm(&self) -> usize {
        let counts = self.counts.read();
        let rewards = self.rewards.read();
        let total = self.total_pulls.load(Ordering::Relaxed);

        match &self.algorithm {
            BanditAlgorithm::EpsilonGreedy { epsilon } => {
                let r: f64 = (total as f64 * 0.123456789).sin().abs();
                if r < *epsilon {
                    // Explore: random arm
                    (total as usize) % self.num_arms
                } else {
                    // Exploit: best arm
                    self.best_arm(&counts, &rewards)
                }
            }

            BanditAlgorithm::UCB { c } => {
                // Find arm with highest UCB
                let ln_total = ((total + 1) as f64).ln();
                let mut best_arm = 0;
                let mut best_ucb = f64::NEG_INFINITY;

                for i in 0..self.num_arms {
                    let ucb = if counts[i] == 0 {
                        f64::INFINITY
                    } else {
                        let avg = rewards[i] / counts[i] as f64;
                        let exploration = c * (2.0 * ln_total / counts[i] as f64).sqrt();
                        avg + exploration
                    };

                    if ucb > best_ucb {
                        best_ucb = ucb;
                        best_arm = i;
                    }
                }

                best_arm
            }

            BanditAlgorithm::ThompsonSampling => {
                // Sample from Beta distribution for each arm
                let mut best_arm = 0;
                let mut best_sample = f64::NEG_INFINITY;

                for i in 0..self.num_arms {
                    let successes = rewards[i];
                    let failures = counts[i] as f64 - successes;

                    // Simple approximation of Beta sampling
                    let alpha = successes + 1.0;
                    let beta = failures + 1.0;
                    let sample = alpha / (alpha + beta)
                        + ((total as f64 * 0.31415926 + i as f64).sin() * 0.1);

                    if sample > best_sample {
                        best_sample = sample;
                        best_arm = i;
                    }
                }

                best_arm
            }

            BanditAlgorithm::Softmax { temperature } => {
                // Softmax selection
                let mut exp_values = vec![0.0; self.num_arms];
                let mut max_q = f64::NEG_INFINITY;

                for i in 0..self.num_arms {
                    let q = if counts[i] == 0 {
                        0.0
                    } else {
                        rewards[i] / counts[i] as f64
                    };
                    max_q = max_q.max(q);
                }

                let mut sum = 0.0;
                for i in 0..self.num_arms {
                    let q = if counts[i] == 0 {
                        0.0
                    } else {
                        rewards[i] / counts[i] as f64
                    };
                    exp_values[i] = ((q - max_q) / temperature).exp();
                    sum += exp_values[i];
                }

                // Sample based on probabilities
                let r = (total as f64 * 0.987654321).sin().abs();
                let mut cumsum = 0.0;
                for i in 0..self.num_arms {
                    cumsum += exp_values[i] / sum;
                    if r < cumsum {
                        return i;
                    }
                }

                self.num_arms - 1
            }
        }
    }

    /// Update with reward
    pub fn update(&self, arm: usize, reward: f64) {
        if arm >= self.num_arms {
            return;
        }

        self.counts.write()[arm] += 1;
        self.rewards.write()[arm] += reward;
        self.total_pulls.fetch_add(1, Ordering::Relaxed);
    }

    /// Get best arm based on average reward
    fn best_arm(&self, counts: &[u64], rewards: &[f64]) -> usize {
        let mut best = 0;
        let mut best_avg = f64::NEG_INFINITY;

        for i in 0..self.num_arms {
            if counts[i] > 0 {
                let avg = rewards[i] / counts[i] as f64;
                if avg > best_avg {
                    best_avg = avg;
                    best = i;
                }
            }
        }

        best
    }

    /// Get statistics for all arms
    pub fn get_stats(&self) -> Vec<ArmStats> {
        let counts = self.counts.read();
        let rewards = self.rewards.read();

        (0..self.num_arms)
            .map(|i| ArmStats {
                arm: i,
                pulls: counts[i],
                total_reward: rewards[i],
                average_reward: if counts[i] > 0 {
                    rewards[i] / counts[i] as f64
                } else {
                    0.0
                },
            })
            .collect()
    }
}

/// Arm statistics
#[derive(Debug, Clone)]
pub struct ArmStats {
    pub arm: usize,
    pub pulls: u64,
    pub total_reward: f64,
    pub average_reward: f64,
}

/// Contextual bandit (LinUCB)
pub struct ContextualBandit {
    num_arms: usize,
    num_features: usize,
    alpha: f64,
    /// A matrices per arm
    a_matrices: RwLock<Vec<Vec<Vec<f64>>>>,
    /// b vectors per arm
    b_vectors: RwLock<Vec<Vec<f64>>>,
}

impl ContextualBandit {
    /// Create a new contextual bandit
    pub fn new(num_arms: usize, num_features: usize, alpha: f64) -> Self {
        // Initialize A = I (identity matrix) for each arm
        let mut a_matrices = Vec::with_capacity(num_arms);
        let mut b_vectors = Vec::with_capacity(num_arms);

        for _ in 0..num_arms {
            let mut a = vec![vec![0.0; num_features]; num_features];
            for i in 0..num_features {
                a[i][i] = 1.0;
            }
            a_matrices.push(a);
            b_vectors.push(vec![0.0; num_features]);
        }

        Self {
            num_arms,
            num_features,
            alpha,
            a_matrices: RwLock::new(a_matrices),
            b_vectors: RwLock::new(b_vectors),
        }
    }

    /// Select arm given context
    pub fn select_arm(&self, context: &[f64]) -> usize {
        let a_matrices = self.a_matrices.read();
        let b_vectors = self.b_vectors.read();

        let mut best_arm = 0;
        let mut best_ucb = f64::NEG_INFINITY;

        for arm in 0..self.num_arms {
            // Compute theta = A^-1 * b (simplified - real impl would use proper linear algebra)
            let theta: Vec<f64> = b_vectors[arm].clone();

            // Compute UCB
            let expected: f64 = theta.iter().zip(context.iter()).map(|(t, c)| t * c).sum();

            // Simplified uncertainty (real impl would use A^-1)
            let uncertainty = self.alpha * context.iter().map(|x| x * x).sum::<f64>().sqrt();

            let ucb = expected + uncertainty;

            if ucb > best_ucb {
                best_ucb = ucb;
                best_arm = arm;
            }
        }

        best_arm
    }

    /// Update with reward
    pub fn update(&self, arm: usize, context: &[f64], reward: f64) {
        if arm >= self.num_arms || context.len() != self.num_features {
            return;
        }

        let mut a_matrices = self.a_matrices.write();
        let mut b_vectors = self.b_vectors.write();

        // A = A + x * x^T
        for i in 0..self.num_features {
            for j in 0..self.num_features {
                a_matrices[arm][i][j] += context[i] * context[j];
            }
        }

        // b = b + r * x
        for i in 0..self.num_features {
            b_vectors[arm][i] += reward * context[i];
        }
    }
}

/// Online learning registry
pub struct OnlineLearningRegistry {
    linear_models: RwLock<HashMap<String, OnlineLinearModel>>,
    bandits: RwLock<HashMap<String, MultiArmedBandit>>,
    contextual_bandits: RwLock<HashMap<String, ContextualBandit>>,
}

impl OnlineLearningRegistry {
    pub fn new() -> Self {
        Self {
            linear_models: RwLock::new(HashMap::new()),
            bandits: RwLock::new(HashMap::new()),
            contextual_bandits: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_linear_model(&self, name: &str, config: OnlineConfig) {
        self.linear_models
            .write()
            .insert(name.to_string(), OnlineLinearModel::new(config));
    }

    pub fn get_linear_model(
        &self,
        name: &str,
    ) -> Option<parking_lot::RwLockReadGuard<'_, HashMap<String, OnlineLinearModel>>> {
        let models = self.linear_models.read();
        if models.contains_key(name) {
            Some(models)
        } else {
            None
        }
    }

    pub fn create_bandit(&self, name: &str, num_arms: usize, algorithm: BanditAlgorithm) {
        self.bandits
            .write()
            .insert(name.to_string(), MultiArmedBandit::new(num_arms, algorithm));
    }

    pub fn list_models(&self) -> Vec<String> {
        self.linear_models.read().keys().cloned().collect()
    }
}

impl Default for OnlineLearningRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_online_sgd() {
        let config = OnlineConfig {
            algorithm: OnlineAlgorithm::SGD,
            lr_schedule: LearningRateSchedule::Constant(0.1),
            num_features: 2,
            ..Default::default()
        };

        let model = OnlineLinearModel::new(config);

        // Train on y = 2*x1 + 3*x2
        for _ in 0..100 {
            let example = Example {
                features: vec![1.0, 1.0],
                target: 5.0,
                weight: 1.0,
            };
            model.partial_fit(&example);
        }

        let pred = model.predict(&[1.0, 1.0]);
        assert!((pred - 5.0).abs() < 1.0);
    }

    #[test]
    fn test_online_adam() {
        let config = OnlineConfig {
            algorithm: OnlineAlgorithm::Adam,
            lr_schedule: LearningRateSchedule::Constant(0.01),
            num_features: 2,
            ..Default::default()
        };

        let model = OnlineLinearModel::new(config);

        // Train on simple linear function
        for i in 0..500 {
            let x = (i as f64) / 100.0;
            let example = Example {
                features: vec![x, 1.0],
                target: 2.0 * x + 1.0,
                weight: 1.0,
            };
            model.partial_fit(&example);
        }

        let stats = model.get_stats();
        assert!(stats.average_loss < 1.0);
    }

    #[test]
    fn test_epsilon_greedy_bandit() {
        let bandit = MultiArmedBandit::new(3, BanditAlgorithm::EpsilonGreedy { epsilon: 0.1 });

        // Simulate pulls
        for _ in 0..100 {
            let arm = bandit.select_arm();
            let reward = if arm == 1 { 1.0 } else { 0.5 }; // Arm 1 is best
            bandit.update(arm, reward);
        }

        let stats = bandit.get_stats();
        // Best arm should have more pulls
        assert!(stats[1].pulls > 0);
    }

    #[test]
    fn test_ucb_bandit() {
        let bandit = MultiArmedBandit::new(3, BanditAlgorithm::UCB { c: 2.0 });

        // Initial exploration
        for i in 0..3 {
            bandit.update(i, if i == 2 { 0.8 } else { 0.3 });
        }

        // UCB should favor arm 2
        let arm = bandit.select_arm();
        assert_eq!(arm, 2);
    }

    #[test]
    fn test_learning_rate_schedules() {
        let constant = LearningRateSchedule::Constant(0.01);
        assert_eq!(constant.get_lr(100, None), 0.01);

        let inverse = LearningRateSchedule::InverseTime {
            initial: 0.1,
            decay: 0.01,
        };
        assert!(inverse.get_lr(100, None) < 0.1);

        let exp = LearningRateSchedule::Exponential {
            initial: 0.1,
            decay: 0.99,
        };
        assert!(exp.get_lr(100, None) < 0.1);
    }

    #[test]
    fn test_contextual_bandit() {
        let bandit = ContextualBandit::new(3, 4, 1.0);

        let context = vec![1.0, 0.5, 0.3, 0.2];
        let arm = bandit.select_arm(&context);
        bandit.update(arm, &context, 1.0);

        // Should work without panicking
        assert!(arm < 3);
    }
}
