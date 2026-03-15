//! Graceful Shutdown and Operational Features
//!
//! Provides graceful shutdown, configuration hot-reload, and scheduled maintenance.
//!
//! # Features
//! - Graceful shutdown with connection draining
//! - Configuration hot-reload (pg_reload_conf)
//! - Scheduled maintenance jobs (pg_cron-like)
//! - Background task management
//! - Shutdown hooks
//!
//! # Example
//! ```sql
//! -- Reload configuration
//! SELECT pg_reload_conf();
//!
//! -- Schedule a job
//! SELECT cron.schedule('vacuum_job', '0 3 * * *', 'VACUUM ANALYZE');
//!
//! -- View scheduled jobs
//! SELECT * FROM cron.job;
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::RwLock;

// ============================================================================
// Types and Errors
// ============================================================================

/// Errors from operational features
#[derive(Debug, Clone)]
pub enum OperationalError {
    /// Shutdown in progress
    ShutdownInProgress,
    /// Shutdown timeout
    ShutdownTimeout { remaining_connections: usize },
    /// Configuration error
    ConfigError(String),
    /// Job scheduling error
    JobError(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for OperationalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShutdownInProgress => write!(f, "server shutdown in progress"),
            Self::ShutdownTimeout { remaining_connections } => {
                write!(
                    f,
                    "shutdown timeout with {} connections remaining",
                    remaining_connections
                )
            }
            Self::ConfigError(msg) => write!(f, "configuration error: {}", msg),
            Self::JobError(msg) => write!(f, "job error: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for OperationalError {}

// ============================================================================
// Shutdown State
// ============================================================================

/// Shutdown phase
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    /// Normal operation
    Running,
    /// Shutdown initiated, rejecting new connections
    Initiated,
    /// Draining existing connections
    Draining,
    /// Waiting for background tasks
    WaitingTasks,
    /// Final cleanup
    Cleanup,
    /// Shutdown complete
    Complete,
}

impl std::fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Initiated => write!(f, "shutdown initiated"),
            Self::Draining => write!(f, "draining connections"),
            Self::WaitingTasks => write!(f, "waiting for tasks"),
            Self::Cleanup => write!(f, "cleanup"),
            Self::Complete => write!(f, "complete"),
        }
    }
}

/// Shutdown statistics
#[derive(Debug, Clone, Default)]
pub struct ShutdownStats {
    /// Connections drained
    pub connections_drained: u64,
    /// Connections terminated
    pub connections_terminated: u64,
    /// Background tasks completed
    pub tasks_completed: u64,
    /// Background tasks cancelled
    pub tasks_cancelled: u64,
    /// Shutdown duration
    pub duration_ms: u64,
}

// ============================================================================
// Shutdown Manager
// ============================================================================

/// Shutdown hook callback type
pub type ShutdownHook = Box<dyn Fn() + Send + Sync>;

/// Graceful shutdown manager
pub struct ShutdownManager {
    /// Current phase
    phase: RwLock<ShutdownPhase>,
    /// Shutdown requested flag
    shutdown_requested: AtomicBool,
    /// Active connections count
    active_connections: AtomicU64,
    /// Active background tasks
    active_tasks: AtomicU64,
    /// Shutdown timeout in milliseconds
    timeout_ms: u64,
    /// Drain timeout in milliseconds
    drain_timeout_ms: u64,
    /// Condition variable for waiting
    shutdown_cv: Condvar,
    /// Mutex for condition variable
    shutdown_mutex: Mutex<()>,
    /// Shutdown hooks
    hooks: RwLock<Vec<(String, ShutdownHook)>>,
    /// Shutdown start time
    shutdown_start: RwLock<Option<Instant>>,
    /// Statistics
    stats: RwLock<ShutdownStats>,
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new(30000, 10000) // 30s total, 10s drain
    }
}

impl ShutdownManager {
    /// Create a new shutdown manager
    pub fn new(timeout_ms: u64, drain_timeout_ms: u64) -> Self {
        Self {
            phase: RwLock::new(ShutdownPhase::Running),
            shutdown_requested: AtomicBool::new(false),
            active_connections: AtomicU64::new(0),
            active_tasks: AtomicU64::new(0),
            timeout_ms,
            drain_timeout_ms,
            shutdown_cv: Condvar::new(),
            shutdown_mutex: Mutex::new(()),
            hooks: RwLock::new(Vec::new()),
            shutdown_start: RwLock::new(None),
            stats: RwLock::new(ShutdownStats::default()),
        }
    }

    /// Check if shutdown is in progress
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Get current shutdown phase
    pub fn phase(&self) -> ShutdownPhase {
        *self.phase.read()
    }

    /// Check if new connections should be accepted
    pub fn accept_connections(&self) -> bool {
        !self.is_shutdown_requested()
    }

    /// Register a connection
    pub fn register_connection(&self) -> Result<ConnectionGuard, OperationalError> {
        if self.is_shutdown_requested() {
            return Err(OperationalError::ShutdownInProgress);
        }
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        Ok(ConnectionGuard {
            manager: self as *const ShutdownManager,
        })
    }

    /// Unregister a connection
    fn unregister_connection(&self) {
        let prev = self.active_connections.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 && self.is_shutdown_requested() {
            // Last connection, notify waiters
            self.shutdown_cv.notify_all();
        }

        // Update stats
        if self.is_shutdown_requested() {
            let mut stats = self.stats.write();
            stats.connections_drained += 1;
        }
    }

    /// Register a background task
    pub fn register_task(&self) -> TaskGuard {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
        TaskGuard {
            manager: self as *const ShutdownManager,
        }
    }

    /// Unregister a background task
    fn unregister_task(&self) {
        let prev = self.active_tasks.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 && self.is_shutdown_requested() {
            self.shutdown_cv.notify_all();
        }

        if self.is_shutdown_requested() {
            let mut stats = self.stats.write();
            stats.tasks_completed += 1;
        }
    }

    /// Register a shutdown hook
    pub fn register_hook(&self, name: &str, hook: ShutdownHook) {
        self.hooks
            .write()
            .push((name.to_string(), hook));
    }

    /// Get active connection count
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get active task count
    pub fn active_tasks(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed)
    }

    /// Initiate graceful shutdown
    pub fn shutdown(&self) -> Result<ShutdownStats, OperationalError> {
        // Mark shutdown requested
        if self
            .shutdown_requested
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(OperationalError::ShutdownInProgress);
        }

        *self.shutdown_start.write() = Some(Instant::now());
        *self.phase.write() = ShutdownPhase::Initiated;

        let start = Instant::now();

        // Phase 1: Stop accepting new connections (already done via flag)
        *self.phase.write() = ShutdownPhase::Draining;

        // Phase 2: Drain existing connections
        let drain_deadline = Instant::now() + Duration::from_millis(self.drain_timeout_ms);
        while self.active_connections.load(Ordering::Relaxed) > 0 {
            if Instant::now() >= drain_deadline {
                // Timeout - force terminate remaining connections
                let remaining = self.active_connections.load(Ordering::Relaxed) as usize;
                let mut stats = self.stats.write();
                stats.connections_terminated = remaining as u64;
                break;
            }

            let guard = self.shutdown_mutex.lock().unwrap();
            let _ = self
                .shutdown_cv
                .wait_timeout(guard, Duration::from_millis(100));
        }

        // Phase 3: Wait for background tasks
        *self.phase.write() = ShutdownPhase::WaitingTasks;
        let task_deadline = Instant::now()
            + Duration::from_millis(self.timeout_ms.saturating_sub(self.drain_timeout_ms));

        while self.active_tasks.load(Ordering::Relaxed) > 0 {
            if Instant::now() >= task_deadline {
                let remaining = self.active_tasks.load(Ordering::Relaxed);
                let mut stats = self.stats.write();
                stats.tasks_cancelled = remaining;
                break;
            }

            let guard = self.shutdown_mutex.lock().unwrap();
            let _ = self
                .shutdown_cv
                .wait_timeout(guard, Duration::from_millis(100));
        }

        // Phase 4: Run shutdown hooks
        *self.phase.write() = ShutdownPhase::Cleanup;
        let hooks = self.hooks.read();
        for (name, hook) in hooks.iter() {
            hook();
        }

        // Complete
        *self.phase.write() = ShutdownPhase::Complete;

        let mut stats = self.stats.write();
        stats.duration_ms = start.elapsed().as_millis() as u64;

        Ok(stats.clone())
    }

    /// Get shutdown statistics
    pub fn stats(&self) -> ShutdownStats {
        self.stats.read().clone()
    }
}

/// RAII guard for connections
pub struct ConnectionGuard {
    manager: *const ShutdownManager,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        unsafe {
            (*self.manager).unregister_connection();
        }
    }
}

// Safety: ConnectionGuard only holds a pointer that's valid for the lifetime of ShutdownManager
unsafe impl Send for ConnectionGuard {}
unsafe impl Sync for ConnectionGuard {}

/// RAII guard for background tasks
pub struct TaskGuard {
    manager: *const ShutdownManager,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        unsafe {
            (*self.manager).unregister_task();
        }
    }
}

unsafe impl Send for TaskGuard {}
unsafe impl Sync for TaskGuard {}

// ============================================================================
// Configuration Hot-Reload
// ============================================================================

/// Configuration parameter type
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// Duration in milliseconds
    Duration(u64),
    /// Size in bytes
    Size(u64),
}

impl std::fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(v) => write!(f, "{}", if *v { "on" } else { "off" }),
            Self::Int(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
            Self::String(v) => write!(f, "{}", v),
            Self::Duration(v) => write!(f, "{}ms", v),
            Self::Size(v) => write!(f, "{}", v),
        }
    }
}

/// Configuration parameter metadata
#[derive(Debug, Clone)]
pub struct ConfigParam {
    /// Parameter name
    pub name: String,
    /// Current value
    pub value: ConfigValue,
    /// Default value
    pub default: ConfigValue,
    /// Description
    pub description: String,
    /// Whether it can be changed at runtime
    pub runtime_changeable: bool,
    /// Whether a restart is required
    pub requires_restart: bool,
    /// Category
    pub category: String,
}

/// Configuration change callback
pub type ConfigChangeCallback = Box<dyn Fn(&str, &ConfigValue, &ConfigValue) + Send + Sync>;

/// Configuration manager with hot-reload support
pub struct ConfigManager {
    /// Configuration parameters
    params: RwLock<HashMap<String, ConfigParam>>,
    /// Change callbacks
    callbacks: RwLock<Vec<ConfigChangeCallback>>,
    /// Pending changes (require restart)
    pending_changes: RwLock<HashMap<String, ConfigValue>>,
    /// Last reload time
    last_reload: RwLock<Option<SystemTime>>,
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigManager {
    /// Create a new configuration manager
    pub fn new() -> Self {
        Self {
            params: RwLock::new(HashMap::new()),
            callbacks: RwLock::new(Vec::new()),
            pending_changes: RwLock::new(HashMap::new()),
            last_reload: RwLock::new(None),
        }
    }

    /// Register a configuration parameter
    pub fn register_param(&self, param: ConfigParam) {
        self.params
            .write()
            .insert(param.name.clone(), param);
    }

    /// Get a parameter value
    pub fn get(&self, name: &str) -> Option<ConfigValue> {
        self.params.read().get(name).map(|p| p.value.clone())
    }

    /// Get a parameter
    pub fn get_param(&self, name: &str) -> Option<ConfigParam> {
        self.params.read().get(name).cloned()
    }

    /// Set a parameter value
    pub fn set(&self, name: &str, value: ConfigValue) -> Result<(), OperationalError> {
        let mut params = self.params.write();

        let param = params
            .get_mut(name)
            .ok_or_else(|| OperationalError::ConfigError(format!("unknown parameter: {}", name)))?;

        if !param.runtime_changeable {
            return Err(OperationalError::ConfigError(format!(
                "parameter '{}' cannot be changed at runtime",
                name
            )));
        }

        let old_value = param.value.clone();

        if param.requires_restart {
            // Queue for restart
            self.pending_changes
                .write()
                .insert(name.to_string(), value);
            return Ok(());
        }

        param.value = value.clone();

        // Notify callbacks
        drop(params);
        let callbacks = self.callbacks.read();
        for callback in callbacks.iter() {
            callback(name, &old_value, &value);
        }

        Ok(())
    }

    /// Register a change callback
    pub fn on_change(&self, callback: ConfigChangeCallback) {
        self.callbacks.write().push(callback);
    }

    /// Reload configuration (pg_reload_conf)
    pub fn reload(&self) -> Result<Vec<String>, OperationalError> {
        let mut reloaded = Vec::new();

        // In a real implementation, this would re-read the config file
        // For now, we'll just apply any pending changes that don't require restart
        let pending = self.pending_changes.read().clone();

        for (name, value) in pending {
            if let Some(param) = self.params.read().get(&name) {
                if !param.requires_restart {
                    self.params.write().get_mut(&name).unwrap().value = value;
                    reloaded.push(name);
                }
            }
        }

        *self.last_reload.write() = Some(SystemTime::now());

        Ok(reloaded)
    }

    /// Get all parameters
    pub fn get_all(&self) -> Vec<ConfigParam> {
        self.params.read().values().cloned().collect()
    }

    /// Get pending changes
    pub fn get_pending_changes(&self) -> HashMap<String, ConfigValue> {
        self.pending_changes.read().clone()
    }

    /// Check if restart is pending
    pub fn restart_pending(&self) -> bool {
        !self.pending_changes.read().is_empty()
    }

    /// Get last reload time
    pub fn last_reload_time(&self) -> Option<SystemTime> {
        *self.last_reload.read()
    }
}

// ============================================================================
// Scheduled Jobs (pg_cron-like)
// ============================================================================

/// Job schedule using cron syntax
#[derive(Debug, Clone)]
pub struct CronSchedule {
    /// Minute (0-59)
    pub minute: CronField,
    /// Hour (0-23)
    pub hour: CronField,
    /// Day of month (1-31)
    pub day_of_month: CronField,
    /// Month (1-12)
    pub month: CronField,
    /// Day of week (0-6, Sunday = 0)
    pub day_of_week: CronField,
}

/// Cron field value
#[derive(Debug, Clone)]
pub enum CronField {
    /// Any value (*)
    Any,
    /// Specific value
    Value(u8),
    /// Range (start-end)
    Range(u8, u8),
    /// Step (*/n)
    Step(u8),
    /// List of values
    List(Vec<u8>),
}

impl CronSchedule {
    /// Parse a cron expression
    pub fn parse(expr: &str) -> Result<Self, OperationalError> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(OperationalError::JobError(
                "cron expression must have 5 fields".into(),
            ));
        }

        Ok(Self {
            minute: Self::parse_field(parts[0])?,
            hour: Self::parse_field(parts[1])?,
            day_of_month: Self::parse_field(parts[2])?,
            month: Self::parse_field(parts[3])?,
            day_of_week: Self::parse_field(parts[4])?,
        })
    }

    fn parse_field(field: &str) -> Result<CronField, OperationalError> {
        if field == "*" {
            return Ok(CronField::Any);
        }

        if let Some(step) = field.strip_prefix("*/") {
            let n: u8 = step
                .parse()
                .map_err(|_| OperationalError::JobError(format!("invalid step: {}", step)))?;
            return Ok(CronField::Step(n));
        }

        if field.contains('-') {
            let parts: Vec<&str> = field.split('-').collect();
            if parts.len() == 2 {
                let start: u8 = parts[0]
                    .parse()
                    .map_err(|_| OperationalError::JobError("invalid range start".into()))?;
                let end: u8 = parts[1]
                    .parse()
                    .map_err(|_| OperationalError::JobError("invalid range end".into()))?;
                return Ok(CronField::Range(start, end));
            }
        }

        if field.contains(',') {
            let values: Result<Vec<u8>, _> = field.split(',').map(|v| v.parse()).collect();
            let values =
                values.map_err(|_| OperationalError::JobError("invalid list value".into()))?;
            return Ok(CronField::List(values));
        }

        let value: u8 = field
            .parse()
            .map_err(|_| OperationalError::JobError(format!("invalid value: {}", field)))?;
        Ok(CronField::Value(value))
    }

    /// Check if schedule matches the given time
    pub fn matches(&self, minute: u8, hour: u8, day: u8, month: u8, weekday: u8) -> bool {
        self.field_matches(&self.minute, minute)
            && self.field_matches(&self.hour, hour)
            && self.field_matches(&self.day_of_month, day)
            && self.field_matches(&self.month, month)
            && self.field_matches(&self.day_of_week, weekday)
    }

    fn field_matches(&self, field: &CronField, value: u8) -> bool {
        match field {
            CronField::Any => true,
            CronField::Value(v) => *v == value,
            CronField::Range(start, end) => value >= *start && value <= *end,
            CronField::Step(step) => value % step == 0,
            CronField::List(values) => values.contains(&value),
        }
    }
}

impl std::fmt::Display for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {} {}",
            Self::format_field(&self.minute),
            Self::format_field(&self.hour),
            Self::format_field(&self.day_of_month),
            Self::format_field(&self.month),
            Self::format_field(&self.day_of_week)
        )
    }
}

impl CronSchedule {
    fn format_field(field: &CronField) -> String {
        match field {
            CronField::Any => "*".to_string(),
            CronField::Value(v) => v.to_string(),
            CronField::Range(s, e) => format!("{}-{}", s, e),
            CronField::Step(s) => format!("*/{}", s),
            CronField::List(l) => l
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
        }
    }
}

/// Scheduled job
#[derive(Debug, Clone)]
pub struct ScheduledJob {
    /// Job ID
    pub job_id: u64,
    /// Job name
    pub name: String,
    /// Cron schedule
    pub schedule: CronSchedule,
    /// SQL command to execute
    pub command: String,
    /// Database to run in
    pub database: String,
    /// Username to run as
    pub username: String,
    /// Whether job is enabled
    pub enabled: bool,
    /// Last run time
    pub last_run: Option<SystemTime>,
    /// Last run status
    pub last_status: Option<JobStatus>,
    /// Next scheduled run
    pub next_run: Option<SystemTime>,
    /// Run count
    pub run_count: u64,
    /// Error count
    pub error_count: u64,
}

/// Job execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    /// Job succeeded
    Success,
    /// Job failed
    Failed,
    /// Job is running
    Running,
    /// Job was skipped
    Skipped,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Running => write!(f, "running"),
            Self::Skipped => write!(f, "skipped"),
        }
    }
}

/// Job scheduler
pub struct JobScheduler {
    /// Scheduled jobs
    jobs: RwLock<HashMap<u64, ScheduledJob>>,
    /// Next job ID
    next_job_id: AtomicU64,
    /// Running flag
    running: AtomicBool,
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl JobScheduler {
    /// Create a new job scheduler
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            next_job_id: AtomicU64::new(1),
            running: AtomicBool::new(false),
        }
    }

    /// Schedule a new job
    pub fn schedule(
        &self,
        name: &str,
        schedule: &str,
        command: &str,
        database: &str,
        username: &str,
    ) -> Result<u64, OperationalError> {
        let cron = CronSchedule::parse(schedule)?;
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);

        let job = ScheduledJob {
            job_id,
            name: name.to_string(),
            schedule: cron,
            command: command.to_string(),
            database: database.to_string(),
            username: username.to_string(),
            enabled: true,
            last_run: None,
            last_status: None,
            next_run: None,
            run_count: 0,
            error_count: 0,
        };

        self.jobs.write().insert(job_id, job);

        Ok(job_id)
    }

    /// Unschedule a job
    pub fn unschedule(&self, job_id: u64) -> bool {
        self.jobs.write().remove(&job_id).is_some()
    }

    /// Enable a job
    pub fn enable(&self, job_id: u64) -> bool {
        if let Some(job) = self.jobs.write().get_mut(&job_id) {
            job.enabled = true;
            true
        } else {
            false
        }
    }

    /// Disable a job
    pub fn disable(&self, job_id: u64) -> bool {
        if let Some(job) = self.jobs.write().get_mut(&job_id) {
            job.enabled = false;
            true
        } else {
            false
        }
    }

    /// Get all jobs
    pub fn get_jobs(&self) -> Vec<ScheduledJob> {
        self.jobs.read().values().cloned().collect()
    }

    /// Get a specific job
    pub fn get_job(&self, job_id: u64) -> Option<ScheduledJob> {
        self.jobs.read().get(&job_id).cloned()
    }

    /// Record job execution
    pub fn record_execution(&self, job_id: u64, status: JobStatus) {
        if let Some(job) = self.jobs.write().get_mut(&job_id) {
            job.last_run = Some(SystemTime::now());
            job.last_status = Some(status);
            job.run_count += 1;
            if status == JobStatus::Failed {
                job.error_count += 1;
            }
        }
    }

    /// Start the scheduler
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the scheduler
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if scheduler is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_manager() {
        let manager = ShutdownManager::new(5000, 2000);

        assert!(!manager.is_shutdown_requested());
        assert_eq!(manager.phase(), ShutdownPhase::Running);
        assert!(manager.accept_connections());
    }

    #[test]
    fn test_connection_registration() {
        let manager = ShutdownManager::new(5000, 2000);

        let _guard = manager.register_connection().unwrap();
        assert_eq!(manager.active_connections(), 1);

        // Guard drops here, connection unregistered
    }

    #[test]
    fn test_shutdown_rejects_connections() {
        let manager = ShutdownManager::new(100, 50);

        // Start shutdown in background
        manager.shutdown_requested.store(true, Ordering::SeqCst);

        // New connections should be rejected
        let result = manager.register_connection();
        assert!(matches!(result, Err(OperationalError::ShutdownInProgress)));
    }

    #[test]
    fn test_config_manager() {
        let config = ConfigManager::new();

        config.register_param(ConfigParam {
            name: "max_connections".to_string(),
            value: ConfigValue::Int(100),
            default: ConfigValue::Int(100),
            description: "Maximum connections".to_string(),
            runtime_changeable: true,
            requires_restart: false,
            category: "connections".to_string(),
        });

        assert_eq!(config.get("max_connections"), Some(ConfigValue::Int(100)));

        config.set("max_connections", ConfigValue::Int(200)).unwrap();
        assert_eq!(config.get("max_connections"), Some(ConfigValue::Int(200)));
    }

    #[test]
    fn test_config_non_runtime_changeable() {
        let config = ConfigManager::new();

        config.register_param(ConfigParam {
            name: "shared_buffers".to_string(),
            value: ConfigValue::Size(128 * 1024 * 1024),
            default: ConfigValue::Size(128 * 1024 * 1024),
            description: "Shared memory buffers".to_string(),
            runtime_changeable: false,
            requires_restart: true,
            category: "memory".to_string(),
        });

        let result = config.set("shared_buffers", ConfigValue::Size(256 * 1024 * 1024));
        assert!(matches!(result, Err(OperationalError::ConfigError(_))));
    }

    #[test]
    fn test_cron_schedule_parse() {
        // Every minute
        let schedule = CronSchedule::parse("* * * * *").unwrap();
        assert!(matches!(schedule.minute, CronField::Any));

        // At 3:00 AM every day
        let schedule = CronSchedule::parse("0 3 * * *").unwrap();
        assert!(matches!(schedule.minute, CronField::Value(0)));
        assert!(matches!(schedule.hour, CronField::Value(3)));

        // Every 5 minutes
        let schedule = CronSchedule::parse("*/5 * * * *").unwrap();
        assert!(matches!(schedule.minute, CronField::Step(5)));
    }

    #[test]
    fn test_cron_schedule_matches() {
        let schedule = CronSchedule::parse("0 3 * * *").unwrap();

        // 3:00 AM should match
        assert!(schedule.matches(0, 3, 15, 6, 1));

        // 3:01 AM should not match
        assert!(!schedule.matches(1, 3, 15, 6, 1));

        // 4:00 AM should not match
        assert!(!schedule.matches(0, 4, 15, 6, 1));
    }

    #[test]
    fn test_job_scheduler() {
        let scheduler = JobScheduler::new();

        let job_id = scheduler
            .schedule("vacuum_job", "0 3 * * *", "VACUUM ANALYZE", "mydb", "admin")
            .unwrap();

        let job = scheduler.get_job(job_id).unwrap();
        assert_eq!(job.name, "vacuum_job");
        assert!(job.enabled);

        scheduler.disable(job_id);
        let job = scheduler.get_job(job_id).unwrap();
        assert!(!job.enabled);

        scheduler.unschedule(job_id);
        assert!(scheduler.get_job(job_id).is_none());
    }

    #[test]
    fn test_job_execution_recording() {
        let scheduler = JobScheduler::new();

        let job_id = scheduler
            .schedule("test_job", "* * * * *", "SELECT 1", "db", "user")
            .unwrap();

        scheduler.record_execution(job_id, JobStatus::Success);
        scheduler.record_execution(job_id, JobStatus::Failed);

        let job = scheduler.get_job(job_id).unwrap();
        assert_eq!(job.run_count, 2);
        assert_eq!(job.error_count, 1);
        assert_eq!(job.last_status, Some(JobStatus::Failed));
    }

    #[test]
    fn test_config_value_display() {
        assert_eq!(format!("{}", ConfigValue::Bool(true)), "on");
        assert_eq!(format!("{}", ConfigValue::Bool(false)), "off");
        assert_eq!(format!("{}", ConfigValue::Int(100)), "100");
        assert_eq!(format!("{}", ConfigValue::Duration(5000)), "5000ms");
    }

    #[test]
    fn test_shutdown_phase_display() {
        assert_eq!(format!("{}", ShutdownPhase::Running), "running");
        assert_eq!(format!("{}", ShutdownPhase::Draining), "draining connections");
    }
}
