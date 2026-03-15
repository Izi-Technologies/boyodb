//! Workflow Engine Module
//!
//! Provides DAG-based data pipeline orchestration for BoyoDB.
//! Features:
//! - Directed Acyclic Graph (DAG) workflow definitions
//! - Task dependencies and parallel execution
//! - Retry logic with exponential backoff
//! - Workflow state management
//! - Scheduling and triggers
//! - Workflow monitoring and logging

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Workflow definition
#[derive(Debug, Clone)]
pub struct Workflow {
    /// Unique workflow ID
    pub id: String,
    /// Workflow name
    pub name: String,
    /// Description
    pub description: String,
    /// Tasks in the workflow
    pub tasks: HashMap<String, Task>,
    /// Task dependencies (task_id -> list of upstream task_ids)
    pub dependencies: HashMap<String, Vec<String>>,
    /// Schedule configuration
    pub schedule: Option<Schedule>,
    /// Default retry policy
    pub default_retry_policy: RetryPolicy,
    /// Workflow tags
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
}

impl Workflow {
    /// Create new workflow
    pub fn new(id: &str, name: &str) -> Self {
        let now = current_timestamp();
        Workflow {
            id: id.to_string(),
            name: name.to_string(),
            description: String::new(),
            tasks: HashMap::new(),
            dependencies: HashMap::new(),
            schedule: None,
            default_retry_policy: RetryPolicy::default(),
            tags: Vec::new(),
            created_at: now,
            modified_at: now,
        }
    }

    /// Add task to workflow
    pub fn add_task(&mut self, task: Task) -> &mut Self {
        self.tasks.insert(task.id.clone(), task);
        self.modified_at = current_timestamp();
        self
    }

    /// Set task dependency
    pub fn add_dependency(&mut self, task_id: &str, depends_on: &str) -> &mut Self {
        self.dependencies
            .entry(task_id.to_string())
            .or_default()
            .push(depends_on.to_string());
        self.modified_at = current_timestamp();
        self
    }

    /// Validate workflow (check for cycles, missing tasks)
    pub fn validate(&self) -> Result<(), WorkflowError> {
        // Check all referenced tasks exist
        for (task_id, deps) in &self.dependencies {
            if !self.tasks.contains_key(task_id) {
                return Err(WorkflowError::TaskNotFound(task_id.clone()));
            }
            for dep in deps {
                if !self.tasks.contains_key(dep) {
                    return Err(WorkflowError::TaskNotFound(dep.clone()));
                }
            }
        }

        // Check for cycles using DFS
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for task_id in self.tasks.keys() {
            if self.has_cycle(task_id, &mut visited, &mut rec_stack) {
                return Err(WorkflowError::CycleDetected);
            }
        }

        Ok(())
    }

    fn has_cycle(
        &self,
        task_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        if rec_stack.contains(task_id) {
            return true;
        }
        if visited.contains(task_id) {
            return false;
        }

        visited.insert(task_id.to_string());
        rec_stack.insert(task_id.to_string());

        if let Some(deps) = self.dependencies.get(task_id) {
            for dep in deps {
                if self.has_cycle(dep, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(task_id);
        false
    }

    /// Get topological order of tasks
    pub fn topological_order(&self) -> Result<Vec<String>, WorkflowError> {
        self.validate()?;

        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

        // Initialize in-degrees
        for task_id in self.tasks.keys() {
            in_degree.insert(task_id.clone(), 0);
        }

        // Calculate in-degrees
        for (task_id, deps) in &self.dependencies {
            in_degree.insert(task_id.clone(), deps.len());
            for dep in deps {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }

        // Kahn's algorithm
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        let mut result = Vec::new();

        while let Some(task_id) = queue.pop_front() {
            result.push(task_id.clone());

            if let Some(dependents) = reverse_deps.get(&task_id) {
                for dependent in dependents {
                    if let Some(deg) = in_degree.get_mut(dependent) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(dependent.clone());
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get tasks that can run in parallel at each level
    pub fn parallel_levels(&self) -> Result<Vec<Vec<String>>, WorkflowError> {
        self.validate()?;

        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut completed: HashSet<String> = HashSet::new();
        let remaining: HashSet<String> = self.tasks.keys().cloned().collect();

        while completed.len() < self.tasks.len() {
            let mut current_level = Vec::new();

            for task_id in &remaining {
                if completed.contains(task_id) {
                    continue;
                }

                let deps = self.dependencies.get(task_id);
                let deps_satisfied = deps
                    .map(|d| d.iter().all(|dep| completed.contains(dep)))
                    .unwrap_or(true);

                if deps_satisfied {
                    current_level.push(task_id.clone());
                }
            }

            if current_level.is_empty() && completed.len() < self.tasks.len() {
                return Err(WorkflowError::CycleDetected);
            }

            for task_id in &current_level {
                completed.insert(task_id.clone());
            }

            levels.push(current_level);
        }

        Ok(levels)
    }
}

/// Task definition
#[derive(Debug, Clone)]
pub struct Task {
    /// Unique task ID within workflow
    pub id: String,
    /// Task name
    pub name: String,
    /// Task type
    pub task_type: TaskType,
    /// Task parameters
    pub params: HashMap<String, String>,
    /// Retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Timeout in seconds
    pub timeout_secs: Option<u64>,
    /// Tags
    pub tags: Vec<String>,
}

impl Task {
    /// Create new task
    pub fn new(id: &str, name: &str, task_type: TaskType) -> Self {
        Task {
            id: id.to_string(),
            name: name.to_string(),
            task_type,
            params: HashMap::new(),
            retry_policy: None,
            timeout_secs: None,
            tags: Vec::new(),
        }
    }

    /// Set parameter
    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.params.insert(key.to_string(), value.to_string());
        self
    }

    /// Set retry policy
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }
}

/// Task types
#[derive(Debug, Clone, PartialEq)]
pub enum TaskType {
    /// SQL query execution
    SqlQuery { query: String },
    /// Data transformation
    Transform { source: String, target: String },
    /// Data validation
    Validate { table: String, rules: Vec<String> },
    /// Data export
    Export { source: String, format: String, path: String },
    /// Data import
    Import { path: String, target: String, format: String },
    /// External API call
    HttpRequest { url: String, method: String },
    /// Shell command
    Shell { command: String },
    /// Python script
    Python { script: String },
    /// Wait/delay
    Delay { seconds: u64 },
    /// Conditional branch
    Branch { condition: String },
    /// Custom task
    Custom { handler: String },
}

/// Retry policy
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Initial delay between retries (ms)
    pub initial_delay_ms: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Maximum delay (ms)
    pub max_delay_ms: u64,
    /// Retryable error patterns
    pub retry_on: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 60000,
            retry_on: vec!["timeout".to_string(), "connection_error".to_string()],
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for given attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay = (self.initial_delay_ms as f64)
            * self.backoff_multiplier.powi(attempt as i32);
        let delay = delay.min(self.max_delay_ms as f64) as u64;
        Duration::from_millis(delay)
    }
}

/// Schedule configuration
#[derive(Debug, Clone)]
pub struct Schedule {
    /// Schedule type
    pub schedule_type: ScheduleType,
    /// Timezone
    pub timezone: String,
    /// Start date (optional)
    pub start_date: Option<u64>,
    /// End date (optional)
    pub end_date: Option<u64>,
    /// Is active
    pub is_active: bool,
}

/// Schedule types
#[derive(Debug, Clone, PartialEq)]
pub enum ScheduleType {
    /// Run once
    Once { at: u64 },
    /// Cron expression
    Cron { expression: String },
    /// Fixed interval
    Interval { seconds: u64 },
    /// Daily at specific time
    Daily { hour: u8, minute: u8 },
    /// Weekly on specific days
    Weekly { days: Vec<u8>, hour: u8, minute: u8 },
}

/// Workflow run instance
#[derive(Debug, Clone)]
pub struct WorkflowRun {
    /// Run ID
    pub run_id: String,
    /// Workflow ID
    pub workflow_id: String,
    /// Run status
    pub status: RunStatus,
    /// Task states
    pub task_states: HashMap<String, TaskState>,
    /// Started at
    pub started_at: u64,
    /// Ended at
    pub ended_at: Option<u64>,
    /// Trigger type
    pub trigger: TriggerType,
    /// Run parameters
    pub params: HashMap<String, String>,
    /// Logs
    pub logs: Vec<LogEntry>,
}

impl WorkflowRun {
    /// Create new workflow run
    pub fn new(workflow_id: &str, trigger: TriggerType) -> Self {
        WorkflowRun {
            run_id: generate_run_id(),
            workflow_id: workflow_id.to_string(),
            status: RunStatus::Pending,
            task_states: HashMap::new(),
            started_at: current_timestamp(),
            ended_at: None,
            trigger,
            params: HashMap::new(),
            logs: Vec::new(),
        }
    }

    /// Add log entry
    pub fn log(&mut self, level: LogLevel, message: &str) {
        self.logs.push(LogEntry {
            timestamp: current_timestamp(),
            level,
            message: message.to_string(),
            task_id: None,
        });
    }

    /// Add task log entry
    pub fn task_log(&mut self, task_id: &str, level: LogLevel, message: &str) {
        self.logs.push(LogEntry {
            timestamp: current_timestamp(),
            level,
            message: message.to_string(),
            task_id: Some(task_id.to_string()),
        });
    }

    /// Get duration in seconds
    pub fn duration_secs(&self) -> u64 {
        let end = self.ended_at.unwrap_or_else(current_timestamp);
        end.saturating_sub(self.started_at)
    }
}

/// Run status
#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    /// Pending start
    Pending,
    /// Currently running
    Running,
    /// Completed successfully
    Success,
    /// Failed
    Failed,
    /// Cancelled
    Cancelled,
    /// Timed out
    TimedOut,
}

/// Task execution state
#[derive(Debug, Clone)]
pub struct TaskState {
    /// Task ID
    pub task_id: String,
    /// Current status
    pub status: TaskStatus,
    /// Attempt count
    pub attempts: u32,
    /// Started at
    pub started_at: Option<u64>,
    /// Ended at
    pub ended_at: Option<u64>,
    /// Error message
    pub error: Option<String>,
    /// Output data
    pub output: Option<String>,
}

/// Task status
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Pending
    Pending,
    /// Queued for execution
    Queued,
    /// Running
    Running,
    /// Success
    Success,
    /// Failed
    Failed,
    /// Skipped
    Skipped,
    /// Retrying
    Retrying,
}

/// Trigger types
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerType {
    /// Manual trigger
    Manual,
    /// Scheduled trigger
    Scheduled,
    /// Event-based trigger
    Event { event_type: String },
    /// API trigger
    Api,
    /// Dependency trigger (from another workflow)
    Dependency { workflow_id: String },
}

/// Log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Log level
    pub level: LogLevel,
    /// Message
    pub message: String,
    /// Associated task ID
    pub task_id: Option<String>,
}

/// Log levels
#[derive(Debug, Clone, PartialEq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

/// Workflow errors
#[derive(Debug, Clone)]
pub enum WorkflowError {
    /// Task not found
    TaskNotFound(String),
    /// Cycle detected in DAG
    CycleDetected,
    /// Workflow not found
    WorkflowNotFound(String),
    /// Run not found
    RunNotFound(String),
    /// Validation error
    ValidationError(String),
    /// Execution error
    ExecutionError(String),
    /// Already exists
    AlreadyExists(String),
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowError::TaskNotFound(id) => write!(f, "Task not found: {}", id),
            WorkflowError::CycleDetected => write!(f, "Cycle detected in workflow DAG"),
            WorkflowError::WorkflowNotFound(id) => write!(f, "Workflow not found: {}", id),
            WorkflowError::RunNotFound(id) => write!(f, "Run not found: {}", id),
            WorkflowError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            WorkflowError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            WorkflowError::AlreadyExists(id) => write!(f, "Already exists: {}", id),
        }
    }
}

impl std::error::Error for WorkflowError {}

/// Workflow engine for managing and executing workflows
pub struct WorkflowEngine {
    /// Registered workflows
    workflows: HashMap<String, Workflow>,
    /// Workflow runs
    runs: HashMap<String, WorkflowRun>,
    /// Active runs by workflow
    active_runs: HashMap<String, Vec<String>>,
}

impl WorkflowEngine {
    /// Create new workflow engine
    pub fn new() -> Self {
        WorkflowEngine {
            workflows: HashMap::new(),
            runs: HashMap::new(),
            active_runs: HashMap::new(),
        }
    }

    /// Register workflow
    pub fn register_workflow(&mut self, workflow: Workflow) -> Result<(), WorkflowError> {
        workflow.validate()?;

        if self.workflows.contains_key(&workflow.id) {
            return Err(WorkflowError::AlreadyExists(workflow.id.clone()));
        }

        self.workflows.insert(workflow.id.clone(), workflow);
        Ok(())
    }

    /// Update workflow
    pub fn update_workflow(&mut self, workflow: Workflow) -> Result<(), WorkflowError> {
        workflow.validate()?;

        if !self.workflows.contains_key(&workflow.id) {
            return Err(WorkflowError::WorkflowNotFound(workflow.id.clone()));
        }

        self.workflows.insert(workflow.id.clone(), workflow);
        Ok(())
    }

    /// Delete workflow
    pub fn delete_workflow(&mut self, workflow_id: &str) -> Result<(), WorkflowError> {
        if !self.workflows.contains_key(workflow_id) {
            return Err(WorkflowError::WorkflowNotFound(workflow_id.to_string()));
        }

        self.workflows.remove(workflow_id);
        self.active_runs.remove(workflow_id);
        Ok(())
    }

    /// Get workflow
    pub fn get_workflow(&self, workflow_id: &str) -> Option<&Workflow> {
        self.workflows.get(workflow_id)
    }

    /// List workflows
    pub fn list_workflows(&self) -> Vec<&Workflow> {
        self.workflows.values().collect()
    }

    /// Start workflow run
    pub fn start_run(
        &mut self,
        workflow_id: &str,
        trigger: TriggerType,
        params: HashMap<String, String>,
    ) -> Result<String, WorkflowError> {
        let workflow = self
            .workflows
            .get(workflow_id)
            .ok_or_else(|| WorkflowError::WorkflowNotFound(workflow_id.to_string()))?;

        let mut run = WorkflowRun::new(workflow_id, trigger);
        run.params = params;
        run.status = RunStatus::Running;

        // Initialize task states
        for task_id in workflow.tasks.keys() {
            run.task_states.insert(
                task_id.clone(),
                TaskState {
                    task_id: task_id.clone(),
                    status: TaskStatus::Pending,
                    attempts: 0,
                    started_at: None,
                    ended_at: None,
                    error: None,
                    output: None,
                },
            );
        }

        run.log(LogLevel::Info, "Workflow run started");

        let run_id = run.run_id.clone();
        self.runs.insert(run_id.clone(), run);
        self.active_runs
            .entry(workflow_id.to_string())
            .or_default()
            .push(run_id.clone());

        Ok(run_id)
    }

    /// Get workflow run
    pub fn get_run(&self, run_id: &str) -> Option<&WorkflowRun> {
        self.runs.get(run_id)
    }

    /// Get mutable workflow run
    pub fn get_run_mut(&mut self, run_id: &str) -> Option<&mut WorkflowRun> {
        self.runs.get_mut(run_id)
    }

    /// Update task state
    pub fn update_task_state(
        &mut self,
        run_id: &str,
        task_id: &str,
        status: TaskStatus,
        output: Option<String>,
        error: Option<String>,
    ) -> Result<(), WorkflowError> {
        let run = self
            .runs
            .get_mut(run_id)
            .ok_or_else(|| WorkflowError::RunNotFound(run_id.to_string()))?;

        let task_state = run
            .task_states
            .get_mut(task_id)
            .ok_or_else(|| WorkflowError::TaskNotFound(task_id.to_string()))?;

        let now = current_timestamp();

        match status {
            TaskStatus::Running => {
                task_state.started_at = Some(now);
                task_state.attempts += 1;
            }
            TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped => {
                task_state.ended_at = Some(now);
            }
            _ => {}
        }

        task_state.status = status.clone();
        task_state.output = output;
        task_state.error = error.clone();

        // Log the state change
        let level = match &status {
            TaskStatus::Failed => LogLevel::Error,
            TaskStatus::Success => LogLevel::Info,
            _ => LogLevel::Debug,
        };
        let message = match &error {
            Some(e) => format!("Task {} status: {:?} - {}", task_id, status, e),
            None => format!("Task {} status: {:?}", task_id, status),
        };
        run.task_log(task_id, level, &message);

        Ok(())
    }

    /// Complete workflow run
    pub fn complete_run(&mut self, run_id: &str, status: RunStatus) -> Result<(), WorkflowError> {
        let run = self
            .runs
            .get_mut(run_id)
            .ok_or_else(|| WorkflowError::RunNotFound(run_id.to_string()))?;

        run.status = status;
        run.ended_at = Some(current_timestamp());
        run.log(LogLevel::Info, &format!("Workflow run completed: {:?}", run.status));

        // Remove from active runs
        if let Some(active) = self.active_runs.get_mut(&run.workflow_id) {
            active.retain(|id| id != run_id);
        }

        Ok(())
    }

    /// Get next runnable tasks for a run
    pub fn get_runnable_tasks(&self, run_id: &str) -> Result<Vec<String>, WorkflowError> {
        let run = self
            .runs
            .get(run_id)
            .ok_or_else(|| WorkflowError::RunNotFound(run_id.to_string()))?;

        let workflow = self
            .workflows
            .get(&run.workflow_id)
            .ok_or_else(|| WorkflowError::WorkflowNotFound(run.workflow_id.clone()))?;

        let mut runnable = Vec::new();

        for (task_id, state) in &run.task_states {
            if state.status != TaskStatus::Pending {
                continue;
            }

            // Check if all dependencies are completed
            let deps = workflow.dependencies.get(task_id);
            let deps_satisfied = deps
                .map(|d| {
                    d.iter().all(|dep| {
                        run.task_states
                            .get(dep)
                            .map(|s| s.status == TaskStatus::Success)
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(true);

            if deps_satisfied {
                runnable.push(task_id.clone());
            }
        }

        Ok(runnable)
    }

    /// Get run statistics
    pub fn get_run_stats(&self, run_id: &str) -> Result<RunStats, WorkflowError> {
        let run = self
            .runs
            .get(run_id)
            .ok_or_else(|| WorkflowError::RunNotFound(run_id.to_string()))?;

        let mut stats = RunStats {
            total_tasks: run.task_states.len(),
            pending: 0,
            running: 0,
            success: 0,
            failed: 0,
            skipped: 0,
            duration_secs: run.duration_secs(),
        };

        for state in run.task_states.values() {
            match state.status {
                TaskStatus::Pending | TaskStatus::Queued => stats.pending += 1,
                TaskStatus::Running | TaskStatus::Retrying => stats.running += 1,
                TaskStatus::Success => stats.success += 1,
                TaskStatus::Failed => stats.failed += 1,
                TaskStatus::Skipped => stats.skipped += 1,
            }
        }

        Ok(stats)
    }

    /// List runs for workflow
    pub fn list_runs(&self, workflow_id: &str) -> Vec<&WorkflowRun> {
        self.runs
            .values()
            .filter(|r| r.workflow_id == workflow_id)
            .collect()
    }

    /// Get active runs
    pub fn get_active_runs(&self) -> Vec<&WorkflowRun> {
        self.runs
            .values()
            .filter(|r| r.status == RunStatus::Running)
            .collect()
    }
}

impl Default for WorkflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Run statistics
#[derive(Debug, Clone)]
pub struct RunStats {
    /// Total tasks
    pub total_tasks: usize,
    /// Pending tasks
    pub pending: usize,
    /// Running tasks
    pub running: usize,
    /// Successful tasks
    pub success: usize,
    /// Failed tasks
    pub failed: usize,
    /// Skipped tasks
    pub skipped: usize,
    /// Duration in seconds
    pub duration_secs: u64,
}

/// Thread-safe workflow registry
pub struct WorkflowRegistry {
    engine: Arc<RwLock<WorkflowEngine>>,
}

impl WorkflowRegistry {
    /// Create new registry
    pub fn new() -> Self {
        WorkflowRegistry {
            engine: Arc::new(RwLock::new(WorkflowEngine::new())),
        }
    }

    /// Register workflow
    pub fn register(&self, workflow: Workflow) -> Result<(), WorkflowError> {
        self.engine
            .write()
            .register_workflow(workflow)
    }

    /// Start run
    pub fn start_run(
        &self,
        workflow_id: &str,
        trigger: TriggerType,
        params: HashMap<String, String>,
    ) -> Result<String, WorkflowError> {
        self.engine
            .write()
            .start_run(workflow_id, trigger, params)
    }

    /// Get run stats
    pub fn get_run_stats(&self, run_id: &str) -> Result<RunStats, WorkflowError> {
        self.engine
            .read()
            .get_run_stats(run_id)
    }

    /// List workflows
    pub fn list_workflows(&self) -> Result<Vec<String>, WorkflowError> {
        let engine = self
            .engine
            .read();
        Ok(engine.workflows.keys().cloned().collect())
    }
}

impl Default for WorkflowRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("run_{:x}_{:x}", now.as_secs(), now.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_creation() {
        let mut workflow = Workflow::new("etl_pipeline", "ETL Pipeline");

        workflow.add_task(Task::new("extract", "Extract Data", TaskType::SqlQuery {
            query: "SELECT * FROM source".to_string(),
        }));

        workflow.add_task(Task::new("transform", "Transform Data", TaskType::Transform {
            source: "raw_data".to_string(),
            target: "clean_data".to_string(),
        }));

        workflow.add_task(Task::new("load", "Load Data", TaskType::SqlQuery {
            query: "INSERT INTO target SELECT * FROM clean_data".to_string(),
        }));

        workflow.add_dependency("transform", "extract");
        workflow.add_dependency("load", "transform");

        assert!(workflow.validate().is_ok());
    }

    #[test]
    fn test_cycle_detection() {
        let mut workflow = Workflow::new("cyclic", "Cyclic Workflow");

        workflow.add_task(Task::new("a", "Task A", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("b", "Task B", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("c", "Task C", TaskType::Delay { seconds: 1 }));

        workflow.add_dependency("b", "a");
        workflow.add_dependency("c", "b");
        workflow.add_dependency("a", "c"); // Creates cycle

        assert!(matches!(workflow.validate(), Err(WorkflowError::CycleDetected)));
    }

    #[test]
    fn test_topological_order() {
        let mut workflow = Workflow::new("linear", "Linear Workflow");

        workflow.add_task(Task::new("a", "A", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("b", "B", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("c", "C", TaskType::Delay { seconds: 1 }));

        workflow.add_dependency("b", "a");
        workflow.add_dependency("c", "b");

        let order = workflow.topological_order().unwrap();
        let a_pos = order.iter().position(|x| x == "a").unwrap();
        let b_pos = order.iter().position(|x| x == "b").unwrap();
        let c_pos = order.iter().position(|x| x == "c").unwrap();

        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);
    }

    #[test]
    fn test_parallel_levels() {
        let mut workflow = Workflow::new("parallel", "Parallel Workflow");

        workflow.add_task(Task::new("start", "Start", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("a", "A", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("b", "B", TaskType::Delay { seconds: 1 }));
        workflow.add_task(Task::new("end", "End", TaskType::Delay { seconds: 1 }));

        workflow.add_dependency("a", "start");
        workflow.add_dependency("b", "start");
        workflow.add_dependency("end", "a");
        workflow.add_dependency("end", "b");

        let levels = workflow.parallel_levels().unwrap();
        assert_eq!(levels.len(), 3); // start, [a,b], end
        assert!(levels[1].contains(&"a".to_string()));
        assert!(levels[1].contains(&"b".to_string()));
    }

    #[test]
    fn test_workflow_engine() {
        let mut engine = WorkflowEngine::new();

        let mut workflow = Workflow::new("test", "Test Workflow");
        workflow.add_task(Task::new("task1", "Task 1", TaskType::Delay { seconds: 1 }));

        engine.register_workflow(workflow).unwrap();

        let run_id = engine.start_run("test", TriggerType::Manual, HashMap::new()).unwrap();

        let runnable = engine.get_runnable_tasks(&run_id).unwrap();
        assert!(runnable.contains(&"task1".to_string()));
    }

    #[test]
    fn test_retry_policy() {
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            backoff_multiplier: 2.0,
            max_delay_ms: 1000,
            retry_on: vec![],
        };

        assert_eq!(policy.delay_for_attempt(0).as_millis(), 100);
        assert_eq!(policy.delay_for_attempt(1).as_millis(), 200);
        assert_eq!(policy.delay_for_attempt(2).as_millis(), 400);
        assert_eq!(policy.delay_for_attempt(5).as_millis(), 1000); // Capped at max
    }
}
