// Phase 16: Resource Governance
//
// Memory pools, I/O scheduling, workload isolation, and resource quotas
// for multi-tenant and production environments.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

// ============================================================================
// Memory Pools
// ============================================================================

/// Memory pool for controlled allocation
pub struct MemoryPool {
    name: String,
    total_bytes: AtomicU64,
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    allocation_count: AtomicU64,
    parent: Option<Arc<MemoryPool>>,
    children: RwLock<Vec<Arc<MemoryPool>>>,
    reservations: RwLock<HashMap<u64, MemoryReservation>>,
    next_reservation_id: AtomicU64,
}

impl MemoryPool {
    pub fn new(name: impl Into<String>, total_bytes: u64) -> Self {
        Self {
            name: name.into(),
            total_bytes: AtomicU64::new(total_bytes),
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            allocation_count: AtomicU64::new(0),
            parent: None,
            children: RwLock::new(Vec::new()),
            reservations: RwLock::new(HashMap::new()),
            next_reservation_id: AtomicU64::new(1),
        }
    }

    pub fn with_parent(name: impl Into<String>, total_bytes: u64, parent: Arc<MemoryPool>) -> Self {
        Self {
            name: name.into(),
            total_bytes: AtomicU64::new(total_bytes),
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            allocation_count: AtomicU64::new(0),
            parent: Some(parent),
            children: RwLock::new(Vec::new()),
            reservations: RwLock::new(HashMap::new()),
            next_reservation_id: AtomicU64::new(1),
        }
    }

    /// Get pool name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get total bytes in pool
    pub fn total(&self) -> u64 {
        self.total_bytes.load(Ordering::SeqCst)
    }

    /// Get used bytes
    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::SeqCst)
    }

    /// Get available bytes
    pub fn available(&self) -> u64 {
        let total = self.total();
        let used = self.used();
        total.saturating_sub(used)
    }

    /// Get peak usage
    pub fn peak(&self) -> u64 {
        self.peak_bytes.load(Ordering::SeqCst)
    }

    /// Get allocation count
    pub fn allocation_count(&self) -> u64 {
        self.allocation_count.load(Ordering::SeqCst)
    }

    /// Try to allocate memory
    pub fn try_allocate(&self, bytes: u64) -> Result<MemoryAllocation, MemoryError> {
        // Check parent first if exists
        if let Some(ref parent) = self.parent {
            if parent.available() < bytes {
                return Err(MemoryError::ParentPoolExhausted);
            }
        }

        loop {
            let current = self.used_bytes.load(Ordering::SeqCst);
            let total = self.total_bytes.load(Ordering::SeqCst);

            if current + bytes > total {
                return Err(MemoryError::PoolExhausted {
                    requested: bytes,
                    available: total.saturating_sub(current),
                });
            }

            if self.used_bytes.compare_exchange(
                current,
                current + bytes,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                // Update peak
                let new_used = current + bytes;
                loop {
                    let peak = self.peak_bytes.load(Ordering::SeqCst);
                    if new_used <= peak {
                        break;
                    }
                    if self.peak_bytes.compare_exchange(
                        peak,
                        new_used,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ).is_ok() {
                        break;
                    }
                }

                self.allocation_count.fetch_add(1, Ordering::SeqCst);

                // Also allocate from parent
                if let Some(ref parent) = self.parent {
                    let _ = parent.try_allocate(bytes);
                }

                return Ok(MemoryAllocation {
                    bytes,
                    pool_name: self.name.clone(),
                });
            }
        }
    }

    /// Release allocated memory
    pub fn release(&self, bytes: u64) {
        self.used_bytes.fetch_sub(bytes, Ordering::SeqCst);

        // Also release from parent
        if let Some(ref parent) = self.parent {
            parent.release(bytes);
        }
    }

    /// Reserve memory for future use
    pub fn reserve(&self, bytes: u64) -> Result<u64, MemoryError> {
        // Check if reservation is possible
        if self.available() < bytes {
            return Err(MemoryError::InsufficientMemory);
        }

        let id = self.next_reservation_id.fetch_add(1, Ordering::SeqCst);
        let reservation = MemoryReservation {
            id,
            bytes,
            created: Instant::now(),
            used: AtomicU64::new(0),
        };

        self.reservations.write().insert(id, reservation);
        Ok(id)
    }

    /// Use reserved memory
    pub fn use_reservation(&self, reservation_id: u64, bytes: u64) -> Result<(), MemoryError> {
        let reservations = self.reservations.read();
        if let Some(reservation) = reservations.get(&reservation_id) {
            let used = reservation.used.fetch_add(bytes, Ordering::SeqCst);
            if used + bytes > reservation.bytes {
                reservation.used.fetch_sub(bytes, Ordering::SeqCst);
                return Err(MemoryError::ReservationExceeded);
            }
            drop(reservations);
            self.try_allocate(bytes)?;
            Ok(())
        } else {
            Err(MemoryError::ReservationNotFound)
        }
    }

    /// Release a reservation
    pub fn release_reservation(&self, reservation_id: u64) {
        if let Some(reservation) = self.reservations.write().remove(&reservation_id) {
            let used = reservation.used.load(Ordering::SeqCst);
            if used > 0 {
                self.release(used);
            }
        }
    }

    /// Get stats
    pub fn stats(&self) -> MemoryPoolStats {
        MemoryPoolStats {
            name: self.name.clone(),
            total_bytes: self.total(),
            used_bytes: self.used(),
            available_bytes: self.available(),
            peak_bytes: self.peak(),
            allocation_count: self.allocation_count(),
            utilization: self.used() as f64 / self.total() as f64,
        }
    }
}

/// Memory allocation handle
#[derive(Debug)]
pub struct MemoryAllocation {
    pub bytes: u64,
    pub pool_name: String,
}

/// Memory reservation
struct MemoryReservation {
    id: u64,
    bytes: u64,
    created: Instant,
    used: AtomicU64,
}

/// Memory pool statistics
#[derive(Debug, Clone)]
pub struct MemoryPoolStats {
    pub name: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub peak_bytes: u64,
    pub allocation_count: u64,
    pub utilization: f64,
}

/// Memory manager for multiple pools
pub struct MemoryManager {
    pools: RwLock<HashMap<String, Arc<MemoryPool>>>,
    global_limit: AtomicU64,
    global_used: AtomicU64,
}

impl MemoryManager {
    pub fn new(global_limit: u64) -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            global_limit: AtomicU64::new(global_limit),
            global_used: AtomicU64::new(0),
        }
    }

    /// Create a new memory pool
    pub fn create_pool(&self, name: &str, size: u64) -> Arc<MemoryPool> {
        let pool = Arc::new(MemoryPool::new(name, size));
        self.pools.write().insert(name.to_string(), Arc::clone(&pool));
        pool
    }

    /// Get a pool by name
    pub fn get_pool(&self, name: &str) -> Option<Arc<MemoryPool>> {
        self.pools.read().get(name).cloned()
    }

    /// Remove a pool
    pub fn remove_pool(&self, name: &str) -> Option<Arc<MemoryPool>> {
        self.pools.write().remove(name)
    }

    /// Get all pool stats
    pub fn all_stats(&self) -> Vec<MemoryPoolStats> {
        self.pools.read()
            .values()
            .map(|p| p.stats())
            .collect()
    }

    /// Get global memory usage
    pub fn global_stats(&self) -> MemoryPoolStats {
        let total = self.global_limit.load(Ordering::SeqCst);
        let used = self.global_used.load(Ordering::SeqCst);
        MemoryPoolStats {
            name: "global".to_string(),
            total_bytes: total,
            used_bytes: used,
            available_bytes: total.saturating_sub(used),
            peak_bytes: used, // Simplified
            allocation_count: 0,
            utilization: used as f64 / total as f64,
        }
    }
}

// ============================================================================
// I/O Scheduling
// ============================================================================

/// I/O priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IoPriority {
    Background = 0,
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Default for IoPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// I/O request type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoType {
    Read,
    Write,
    Sync,
    Flush,
}

/// I/O request
#[derive(Debug)]
pub struct IoRequest {
    pub id: u64,
    pub priority: IoPriority,
    pub io_type: IoType,
    pub size_bytes: u64,
    pub submitted: Instant,
    pub deadline: Option<Instant>,
}

/// I/O scheduler using multiple queues
pub struct IoScheduler {
    queues: RwLock<HashMap<IoPriority, VecDeque<IoRequest>>>,
    next_id: AtomicU64,
    total_submitted: AtomicU64,
    total_completed: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    rate_limiter: Option<RateLimiter>,
    max_concurrent: AtomicUsize,
    current_concurrent: AtomicUsize,
}

impl IoScheduler {
    pub fn new() -> Self {
        let mut queues = HashMap::new();
        queues.insert(IoPriority::Background, VecDeque::new());
        queues.insert(IoPriority::Low, VecDeque::new());
        queues.insert(IoPriority::Normal, VecDeque::new());
        queues.insert(IoPriority::High, VecDeque::new());
        queues.insert(IoPriority::Critical, VecDeque::new());

        Self {
            queues: RwLock::new(queues),
            next_id: AtomicU64::new(1),
            total_submitted: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            rate_limiter: None,
            max_concurrent: AtomicUsize::new(64),
            current_concurrent: AtomicUsize::new(0),
        }
    }

    pub fn with_rate_limit(mut self, bytes_per_second: u64) -> Self {
        self.rate_limiter = Some(RateLimiter::new(bytes_per_second));
        self
    }

    pub fn with_max_concurrent(self, max: usize) -> Self {
        self.max_concurrent.store(max, Ordering::SeqCst);
        self
    }

    /// Submit an I/O request
    pub fn submit(&self, priority: IoPriority, io_type: IoType, size_bytes: u64) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = IoRequest {
            id,
            priority,
            io_type,
            size_bytes,
            submitted: Instant::now(),
            deadline: None,
        };

        self.queues.write()
            .get_mut(&priority)
            .unwrap()
            .push_back(request);

        self.total_submitted.fetch_add(1, Ordering::SeqCst);
        id
    }

    /// Submit with deadline
    pub fn submit_with_deadline(
        &self,
        priority: IoPriority,
        io_type: IoType,
        size_bytes: u64,
        deadline: Duration,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = IoRequest {
            id,
            priority,
            io_type,
            size_bytes,
            submitted: Instant::now(),
            deadline: Some(Instant::now() + deadline),
        };

        self.queues.write()
            .get_mut(&priority)
            .unwrap()
            .push_back(request);

        self.total_submitted.fetch_add(1, Ordering::SeqCst);
        id
    }

    /// Get next request to process (priority-based)
    pub fn next(&self) -> Option<IoRequest> {
        // Check concurrency limit
        let current = self.current_concurrent.load(Ordering::SeqCst);
        let max = self.max_concurrent.load(Ordering::SeqCst);
        if current >= max {
            return None;
        }

        let mut queues = self.queues.write();

        // Process in priority order (highest first)
        for priority in [
            IoPriority::Critical,
            IoPriority::High,
            IoPriority::Normal,
            IoPriority::Low,
            IoPriority::Background,
        ] {
            if let Some(queue) = queues.get_mut(&priority) {
                // Check for expired deadlines first
                let expired_idx = queue.iter().position(|r| {
                    r.deadline.map(|d| d < Instant::now()).unwrap_or(false)
                });

                if let Some(idx) = expired_idx {
                    if let Some(request) = queue.remove(idx) {
                        self.current_concurrent.fetch_add(1, Ordering::SeqCst);
                        return Some(request);
                    }
                }

                if let Some(request) = queue.pop_front() {
                    self.current_concurrent.fetch_add(1, Ordering::SeqCst);
                    return Some(request);
                }
            }
        }

        None
    }

    /// Complete an I/O request
    pub fn complete(&self, request: &IoRequest) {
        self.current_concurrent.fetch_sub(1, Ordering::SeqCst);
        self.total_completed.fetch_add(1, Ordering::SeqCst);

        match request.io_type {
            IoType::Read => {
                self.bytes_read.fetch_add(request.size_bytes, Ordering::SeqCst);
            }
            IoType::Write => {
                self.bytes_written.fetch_add(request.size_bytes, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// Check rate limit
    pub fn check_rate_limit(&self, bytes: u64) -> bool {
        if let Some(ref limiter) = self.rate_limiter {
            limiter.try_acquire(bytes)
        } else {
            true
        }
    }

    /// Get queue depths
    pub fn queue_depths(&self) -> HashMap<IoPriority, usize> {
        self.queues.read()
            .iter()
            .map(|(p, q)| (*p, q.len()))
            .collect()
    }

    /// Get stats
    pub fn stats(&self) -> IoStats {
        IoStats {
            total_submitted: self.total_submitted.load(Ordering::SeqCst),
            total_completed: self.total_completed.load(Ordering::SeqCst),
            pending: self.total_submitted.load(Ordering::SeqCst)
                - self.total_completed.load(Ordering::SeqCst),
            bytes_read: self.bytes_read.load(Ordering::SeqCst),
            bytes_written: self.bytes_written.load(Ordering::SeqCst),
            current_concurrent: self.current_concurrent.load(Ordering::SeqCst),
        }
    }
}

impl Default for IoScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// I/O statistics
#[derive(Debug, Clone)]
pub struct IoStats {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub pending: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub current_concurrent: usize,
}

/// Rate limiter using token bucket
pub struct RateLimiter {
    tokens: AtomicU64,
    capacity: u64,
    refill_rate: u64, // tokens per second
    last_refill: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            tokens: AtomicU64::new(bytes_per_second),
            capacity: bytes_per_second,
            refill_rate: bytes_per_second,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Try to acquire tokens
    pub fn try_acquire(&self, tokens: u64) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::SeqCst);
            if current < tokens {
                return false;
            }

            if self.tokens.compare_exchange(
                current,
                current - tokens,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                return true;
            }
        }
    }

    fn refill(&self) {
        let mut last = self.last_refill.lock();
        let elapsed = last.elapsed();
        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;

        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::SeqCst);
            let new_tokens = (current + tokens_to_add).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::SeqCst);
            *last = Instant::now();
        }
    }

    /// Get available tokens
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Workload Isolation
// ============================================================================

/// Workload group for resource isolation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadGroup {
    pub name: String,
    pub priority: WorkloadPriority,
    pub memory_limit: u64,
    pub cpu_weight: u32,
    pub io_priority: IoPriority,
    pub max_concurrent_queries: usize,
    pub max_queued_queries: usize,
    pub query_timeout: Duration,
    pub enabled: bool,
}

impl WorkloadGroup {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            priority: WorkloadPriority::Normal,
            memory_limit: 1024 * 1024 * 1024, // 1GB
            cpu_weight: 100,
            io_priority: IoPriority::Normal,
            max_concurrent_queries: 100,
            max_queued_queries: 1000,
            query_timeout: Duration::from_secs(300),
            enabled: true,
        }
    }

    pub fn with_priority(mut self, priority: WorkloadPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_memory_limit(mut self, bytes: u64) -> Self {
        self.memory_limit = bytes;
        self
    }

    pub fn with_cpu_weight(mut self, weight: u32) -> Self {
        self.cpu_weight = weight;
        self
    }

    pub fn with_io_priority(mut self, priority: IoPriority) -> Self {
        self.io_priority = priority;
        self
    }
}

/// Workload priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum WorkloadPriority {
    Background = 0,
    Low = 1,
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Default for WorkloadPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Workload group state
pub struct WorkloadGroupState {
    group: WorkloadGroup,
    memory_pool: Arc<MemoryPool>,
    current_queries: AtomicUsize,
    queued_queries: AtomicUsize,
    total_queries: AtomicU64,
    total_query_time_ms: AtomicU64,
    rejected_queries: AtomicU64,
}

impl WorkloadGroupState {
    pub fn new(group: WorkloadGroup) -> Self {
        let pool = Arc::new(MemoryPool::new(&group.name, group.memory_limit));
        Self {
            group,
            memory_pool: pool,
            current_queries: AtomicUsize::new(0),
            queued_queries: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_query_time_ms: AtomicU64::new(0),
            rejected_queries: AtomicU64::new(0),
        }
    }

    /// Try to admit a query
    pub fn try_admit(&self) -> Result<QueryAdmission, WorkloadError> {
        if !self.group.enabled {
            return Err(WorkloadError::GroupDisabled);
        }

        let current = self.current_queries.load(Ordering::SeqCst);
        if current >= self.group.max_concurrent_queries {
            // Try to queue
            let queued = self.queued_queries.load(Ordering::SeqCst);
            if queued >= self.group.max_queued_queries {
                self.rejected_queries.fetch_add(1, Ordering::SeqCst);
                return Err(WorkloadError::QueueFull);
            }

            self.queued_queries.fetch_add(1, Ordering::SeqCst);
            return Err(WorkloadError::Queued);
        }

        self.current_queries.fetch_add(1, Ordering::SeqCst);
        self.total_queries.fetch_add(1, Ordering::SeqCst);

        Ok(QueryAdmission {
            group_name: self.group.name.clone(),
            admitted_at: Instant::now(),
            timeout: self.group.query_timeout,
        })
    }

    /// Release a query slot
    pub fn release(&self, duration_ms: u64) {
        self.current_queries.fetch_sub(1, Ordering::SeqCst);
        self.total_query_time_ms.fetch_add(duration_ms, Ordering::SeqCst);

        // Admit queued query if any
        let queued = self.queued_queries.load(Ordering::SeqCst);
        if queued > 0 {
            self.queued_queries.fetch_sub(1, Ordering::SeqCst);
            self.current_queries.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Get memory pool
    pub fn memory_pool(&self) -> &Arc<MemoryPool> {
        &self.memory_pool
    }

    /// Get stats
    pub fn stats(&self) -> WorkloadGroupStats {
        let total = self.total_queries.load(Ordering::SeqCst);
        let total_time = self.total_query_time_ms.load(Ordering::SeqCst);
        WorkloadGroupStats {
            name: self.group.name.clone(),
            priority: self.group.priority,
            current_queries: self.current_queries.load(Ordering::SeqCst),
            queued_queries: self.queued_queries.load(Ordering::SeqCst),
            total_queries: total,
            rejected_queries: self.rejected_queries.load(Ordering::SeqCst),
            avg_query_time_ms: if total > 0 { total_time / total } else { 0 },
            memory_used: self.memory_pool.used(),
            memory_limit: self.group.memory_limit,
        }
    }
}

/// Query admission token
#[derive(Debug)]
pub struct QueryAdmission {
    pub group_name: String,
    pub admitted_at: Instant,
    pub timeout: Duration,
}

impl QueryAdmission {
    pub fn is_timed_out(&self) -> bool {
        self.admitted_at.elapsed() > self.timeout
    }

    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.admitted_at.elapsed())
    }
}

/// Workload group statistics
#[derive(Debug, Clone)]
pub struct WorkloadGroupStats {
    pub name: String,
    pub priority: WorkloadPriority,
    pub current_queries: usize,
    pub queued_queries: usize,
    pub total_queries: u64,
    pub rejected_queries: u64,
    pub avg_query_time_ms: u64,
    pub memory_used: u64,
    pub memory_limit: u64,
}

/// Workload manager
pub struct WorkloadManager {
    groups: RwLock<HashMap<String, Arc<WorkloadGroupState>>>,
    default_group: String,
    user_mappings: RwLock<HashMap<String, String>>,
}

impl WorkloadManager {
    pub fn new() -> Self {
        let mut groups = HashMap::new();
        let default = Arc::new(WorkloadGroupState::new(
            WorkloadGroup::new("default")
        ));
        groups.insert("default".to_string(), default);

        Self {
            groups: RwLock::new(groups),
            default_group: "default".to_string(),
            user_mappings: RwLock::new(HashMap::new()),
        }
    }

    /// Create a workload group
    pub fn create_group(&self, group: WorkloadGroup) -> Result<(), WorkloadError> {
        let name = group.name.clone();
        let state = Arc::new(WorkloadGroupState::new(group));
        self.groups.write().insert(name, state);
        Ok(())
    }

    /// Remove a workload group
    pub fn remove_group(&self, name: &str) -> Result<(), WorkloadError> {
        if name == "default" {
            return Err(WorkloadError::CannotRemoveDefault);
        }
        self.groups.write().remove(name);
        Ok(())
    }

    /// Get a group
    pub fn get_group(&self, name: &str) -> Option<Arc<WorkloadGroupState>> {
        self.groups.read().get(name).cloned()
    }

    /// Map user to group
    pub fn map_user(&self, user: &str, group: &str) {
        self.user_mappings.write().insert(user.to_string(), group.to_string());
    }

    /// Get group for user
    pub fn get_group_for_user(&self, user: &str) -> Arc<WorkloadGroupState> {
        let group_name = self.user_mappings.read()
            .get(user)
            .cloned()
            .unwrap_or_else(|| self.default_group.clone());

        self.groups.read()
            .get(&group_name)
            .cloned()
            .unwrap_or_else(|| {
                self.groups.read().get(&self.default_group).unwrap().clone()
            })
    }

    /// Admit query for user
    pub fn admit(&self, user: &str) -> Result<QueryAdmission, WorkloadError> {
        let group = self.get_group_for_user(user);
        group.try_admit()
    }

    /// Get all group stats
    pub fn all_stats(&self) -> Vec<WorkloadGroupStats> {
        self.groups.read()
            .values()
            .map(|g| g.stats())
            .collect()
    }
}

impl Default for WorkloadManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Resource Quotas
// ============================================================================

/// Resource quota definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuotaDef {
    pub name: String,
    pub max_memory_bytes: Option<u64>,
    pub max_cpu_time_ms: Option<u64>,
    pub max_read_rows: Option<u64>,
    pub max_read_bytes: Option<u64>,
    pub max_result_rows: Option<u64>,
    pub max_result_bytes: Option<u64>,
    pub max_execution_time: Option<Duration>,
    pub max_queries_per_interval: Option<(u64, Duration)>,
}

impl ResourceQuotaDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            max_memory_bytes: None,
            max_cpu_time_ms: None,
            max_read_rows: None,
            max_read_bytes: None,
            max_result_rows: None,
            max_result_bytes: None,
            max_execution_time: None,
            max_queries_per_interval: None,
        }
    }

    pub fn with_memory_limit(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    pub fn with_row_limit(mut self, rows: u64) -> Self {
        self.max_read_rows = Some(rows);
        self
    }

    pub fn with_execution_time(mut self, duration: Duration) -> Self {
        self.max_execution_time = Some(duration);
        self
    }

    pub fn with_rate_limit(mut self, queries: u64, interval: Duration) -> Self {
        self.max_queries_per_interval = Some((queries, interval));
        self
    }
}

/// Resource usage tracker
pub struct ResourceTracker {
    quota: ResourceQuotaDef,
    memory_used: AtomicU64,
    cpu_time_ms: AtomicU64,
    rows_read: AtomicU64,
    bytes_read: AtomicU64,
    result_rows: AtomicU64,
    result_bytes: AtomicU64,
    start_time: Instant,
    query_count: AtomicU64,
    interval_start: Mutex<Instant>,
}

impl ResourceTracker {
    pub fn new(quota: ResourceQuotaDef) -> Self {
        Self {
            quota,
            memory_used: AtomicU64::new(0),
            cpu_time_ms: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            result_rows: AtomicU64::new(0),
            result_bytes: AtomicU64::new(0),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            interval_start: Mutex::new(Instant::now()),
        }
    }

    /// Add memory usage
    pub fn add_memory(&self, bytes: u64) -> Result<(), QuotaError> {
        let new_value = self.memory_used.fetch_add(bytes, Ordering::SeqCst) + bytes;
        if let Some(limit) = self.quota.max_memory_bytes {
            if new_value > limit {
                self.memory_used.fetch_sub(bytes, Ordering::SeqCst);
                return Err(QuotaError::MemoryExceeded { used: new_value, limit });
            }
        }
        Ok(())
    }

    /// Release memory
    pub fn release_memory(&self, bytes: u64) {
        self.memory_used.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Add rows read
    pub fn add_rows_read(&self, rows: u64) -> Result<(), QuotaError> {
        let new_value = self.rows_read.fetch_add(rows, Ordering::SeqCst) + rows;
        if let Some(limit) = self.quota.max_read_rows {
            if new_value > limit {
                return Err(QuotaError::RowsExceeded { read: new_value, limit });
            }
        }
        Ok(())
    }

    /// Add bytes read
    pub fn add_bytes_read(&self, bytes: u64) -> Result<(), QuotaError> {
        let new_value = self.bytes_read.fetch_add(bytes, Ordering::SeqCst) + bytes;
        if let Some(limit) = self.quota.max_read_bytes {
            if new_value > limit {
                return Err(QuotaError::BytesExceeded { read: new_value, limit });
            }
        }
        Ok(())
    }

    /// Check execution time
    pub fn check_execution_time(&self) -> Result<(), QuotaError> {
        if let Some(limit) = self.quota.max_execution_time {
            if self.start_time.elapsed() > limit {
                return Err(QuotaError::ExecutionTimeExceeded {
                    elapsed: self.start_time.elapsed(),
                    limit,
                });
            }
        }
        Ok(())
    }

    /// Track query for rate limiting
    pub fn track_query(&self) -> Result<(), QuotaError> {
        if let Some((limit, interval)) = self.quota.max_queries_per_interval {
            let mut start = self.interval_start.lock();
            if start.elapsed() > interval {
                *start = Instant::now();
                self.query_count.store(1, Ordering::SeqCst);
            } else {
                let count = self.query_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count > limit {
                    return Err(QuotaError::RateLimitExceeded { count, limit, interval });
                }
            }
        }
        Ok(())
    }

    /// Get current usage
    pub fn usage(&self) -> ResourceUsageInfo {
        ResourceUsageInfo {
            memory_used: self.memory_used.load(Ordering::SeqCst),
            cpu_time_ms: self.cpu_time_ms.load(Ordering::SeqCst),
            rows_read: self.rows_read.load(Ordering::SeqCst),
            bytes_read: self.bytes_read.load(Ordering::SeqCst),
            result_rows: self.result_rows.load(Ordering::SeqCst),
            result_bytes: self.result_bytes.load(Ordering::SeqCst),
            execution_time: self.start_time.elapsed(),
        }
    }
}

/// Resource usage information
#[derive(Debug, Clone)]
pub struct ResourceUsageInfo {
    pub memory_used: u64,
    pub cpu_time_ms: u64,
    pub rows_read: u64,
    pub bytes_read: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub execution_time: Duration,
}

// ============================================================================
// Query Throttling
// ============================================================================

/// Query throttler
pub struct QueryThrottler {
    max_concurrent: AtomicUsize,
    current: AtomicUsize,
    waiting: AtomicUsize,
    total_throttled: AtomicU64,
    total_admitted: AtomicU64,
}

impl QueryThrottler {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent: AtomicUsize::new(max_concurrent),
            current: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            total_throttled: AtomicU64::new(0),
            total_admitted: AtomicU64::new(0),
        }
    }

    /// Try to acquire a slot
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.current.load(Ordering::SeqCst);
            let max = self.max_concurrent.load(Ordering::SeqCst);

            if current >= max {
                self.total_throttled.fetch_add(1, Ordering::SeqCst);
                return false;
            }

            if self.current.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ).is_ok() {
                self.total_admitted.fetch_add(1, Ordering::SeqCst);
                return true;
            }
        }
    }

    /// Release a slot
    pub fn release(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get current count
    pub fn current(&self) -> usize {
        self.current.load(Ordering::SeqCst)
    }

    /// Get waiting count
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    /// Set max concurrent
    pub fn set_max(&self, max: usize) {
        self.max_concurrent.store(max, Ordering::SeqCst);
    }

    /// Get stats
    pub fn stats(&self) -> ThrottlerStats {
        ThrottlerStats {
            max_concurrent: self.max_concurrent.load(Ordering::SeqCst),
            current: self.current.load(Ordering::SeqCst),
            waiting: self.waiting.load(Ordering::SeqCst),
            total_throttled: self.total_throttled.load(Ordering::SeqCst),
            total_admitted: self.total_admitted.load(Ordering::SeqCst),
        }
    }
}

/// Throttler statistics
#[derive(Debug, Clone)]
pub struct ThrottlerStats {
    pub max_concurrent: usize,
    pub current: usize,
    pub waiting: usize,
    pub total_throttled: u64,
    pub total_admitted: u64,
}

// ============================================================================
// Resource Governor (Combines all components)
// ============================================================================

/// Resource governor configuration
#[derive(Debug, Clone)]
pub struct ResourceGovernorConfig {
    pub global_memory_limit: u64,
    pub max_concurrent_queries: usize,
    pub io_rate_limit: Option<u64>,
    pub enable_workload_groups: bool,
}

impl Default for ResourceGovernorConfig {
    fn default() -> Self {
        Self {
            global_memory_limit: 8 * 1024 * 1024 * 1024, // 8GB
            max_concurrent_queries: 100,
            io_rate_limit: None,
            enable_workload_groups: true,
        }
    }
}

/// Resource governor
pub struct ResourceGovernor {
    config: ResourceGovernorConfig,
    memory_manager: Arc<MemoryManager>,
    io_scheduler: Arc<IoScheduler>,
    workload_manager: Arc<WorkloadManager>,
    throttler: Arc<QueryThrottler>,
    quotas: RwLock<HashMap<String, Arc<ResourceTracker>>>,
}

impl ResourceGovernor {
    pub fn new(config: ResourceGovernorConfig) -> Self {
        let io_scheduler = if let Some(rate) = config.io_rate_limit {
            IoScheduler::new().with_rate_limit(rate)
        } else {
            IoScheduler::new()
        };

        Self {
            memory_manager: Arc::new(MemoryManager::new(config.global_memory_limit)),
            io_scheduler: Arc::new(io_scheduler),
            workload_manager: Arc::new(WorkloadManager::new()),
            throttler: Arc::new(QueryThrottler::new(config.max_concurrent_queries)),
            quotas: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Get memory manager
    pub fn memory(&self) -> &Arc<MemoryManager> {
        &self.memory_manager
    }

    /// Get I/O scheduler
    pub fn io(&self) -> &Arc<IoScheduler> {
        &self.io_scheduler
    }

    /// Get workload manager
    pub fn workloads(&self) -> &Arc<WorkloadManager> {
        &self.workload_manager
    }

    /// Get throttler
    pub fn throttler(&self) -> &Arc<QueryThrottler> {
        &self.throttler
    }

    /// Create quota for user
    pub fn create_quota(&self, user: &str, quota: ResourceQuotaDef) {
        let tracker = Arc::new(ResourceTracker::new(quota));
        self.quotas.write().insert(user.to_string(), tracker);
    }

    /// Get quota tracker for user
    pub fn get_quota(&self, user: &str) -> Option<Arc<ResourceTracker>> {
        self.quotas.read().get(user).cloned()
    }

    /// Admit query with all checks
    pub fn admit_query(&self, user: &str) -> Result<QueryContext, ResourceError> {
        // Check throttler
        if !self.throttler.try_acquire() {
            return Err(ResourceError::Throttled);
        }

        // Check workload group
        let admission = match self.workload_manager.admit(user) {
            Ok(a) => Some(a),
            Err(WorkloadError::Queued) => None,
            Err(e) => {
                self.throttler.release();
                return Err(ResourceError::Workload(e));
            }
        };

        // Check quota rate limit
        if let Some(tracker) = self.get_quota(user) {
            if let Err(e) = tracker.track_query() {
                self.throttler.release();
                return Err(ResourceError::Quota(e));
            }
        }

        Ok(QueryContext {
            user: user.to_string(),
            admission,
            start_time: Instant::now(),
        })
    }

    /// Release query resources
    pub fn release_query(&self, ctx: &QueryContext) {
        self.throttler.release();

        if let Some(ref admission) = ctx.admission {
            if let Some(group) = self.workload_manager.get_group(&admission.group_name) {
                group.release(ctx.start_time.elapsed().as_millis() as u64);
            }
        }
    }

    /// Get overall stats
    pub fn stats(&self) -> ResourceGovernorStats {
        ResourceGovernorStats {
            memory: self.memory_manager.global_stats(),
            io: self.io_scheduler.stats(),
            throttler: self.throttler.stats(),
            workload_groups: self.workload_manager.all_stats(),
        }
    }
}

/// Query execution context
#[derive(Debug)]
pub struct QueryContext {
    pub user: String,
    pub admission: Option<QueryAdmission>,
    pub start_time: Instant,
}

/// Resource governor statistics
#[derive(Debug)]
pub struct ResourceGovernorStats {
    pub memory: MemoryPoolStats,
    pub io: IoStats,
    pub throttler: ThrottlerStats,
    pub workload_groups: Vec<WorkloadGroupStats>,
}

// ============================================================================
// Errors
// ============================================================================

/// Memory errors
#[derive(Debug, Clone)]
pub enum MemoryError {
    PoolExhausted { requested: u64, available: u64 },
    ParentPoolExhausted,
    InsufficientMemory,
    ReservationNotFound,
    ReservationExceeded,
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PoolExhausted { requested, available } => {
                write!(f, "Pool exhausted: requested {} bytes, available {}", requested, available)
            }
            Self::ParentPoolExhausted => write!(f, "Parent pool exhausted"),
            Self::InsufficientMemory => write!(f, "Insufficient memory"),
            Self::ReservationNotFound => write!(f, "Reservation not found"),
            Self::ReservationExceeded => write!(f, "Reservation exceeded"),
        }
    }
}

impl std::error::Error for MemoryError {}

/// Workload errors
#[derive(Debug, Clone)]
pub enum WorkloadError {
    GroupDisabled,
    QueueFull,
    Queued,
    GroupNotFound,
    CannotRemoveDefault,
}

impl std::fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupDisabled => write!(f, "Workload group is disabled"),
            Self::QueueFull => write!(f, "Workload queue is full"),
            Self::Queued => write!(f, "Query queued"),
            Self::GroupNotFound => write!(f, "Workload group not found"),
            Self::CannotRemoveDefault => write!(f, "Cannot remove default group"),
        }
    }
}

impl std::error::Error for WorkloadError {}

/// Quota errors
#[derive(Debug, Clone)]
pub enum QuotaError {
    MemoryExceeded { used: u64, limit: u64 },
    RowsExceeded { read: u64, limit: u64 },
    BytesExceeded { read: u64, limit: u64 },
    ExecutionTimeExceeded { elapsed: Duration, limit: Duration },
    RateLimitExceeded { count: u64, limit: u64, interval: Duration },
}

impl std::fmt::Display for QuotaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryExceeded { used, limit } => {
                write!(f, "Memory quota exceeded: {} / {}", used, limit)
            }
            Self::RowsExceeded { read, limit } => {
                write!(f, "Row limit exceeded: {} / {}", read, limit)
            }
            Self::BytesExceeded { read, limit } => {
                write!(f, "Byte limit exceeded: {} / {}", read, limit)
            }
            Self::ExecutionTimeExceeded { elapsed, limit } => {
                write!(f, "Execution time exceeded: {:?} / {:?}", elapsed, limit)
            }
            Self::RateLimitExceeded { count, limit, interval } => {
                write!(f, "Rate limit exceeded: {} / {} per {:?}", count, limit, interval)
            }
        }
    }
}

impl std::error::Error for QuotaError {}

/// Resource errors
#[derive(Debug, Clone)]
pub enum ResourceError {
    Throttled,
    Workload(WorkloadError),
    Quota(QuotaError),
    Memory(MemoryError),
}

impl std::fmt::Display for ResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Throttled => write!(f, "Query throttled"),
            Self::Workload(e) => write!(f, "Workload error: {}", e),
            Self::Quota(e) => write!(f, "Quota error: {}", e),
            Self::Memory(e) => write!(f, "Memory error: {}", e),
        }
    }
}

impl std::error::Error for ResourceError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_basic() {
        let pool = MemoryPool::new("test", 1024);
        assert_eq!(pool.total(), 1024);
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.available(), 1024);

        let alloc = pool.try_allocate(512).unwrap();
        assert_eq!(alloc.bytes, 512);
        assert_eq!(pool.used(), 512);
        assert_eq!(pool.available(), 512);

        pool.release(512);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_memory_pool_exhausted() {
        let pool = MemoryPool::new("test", 100);
        let _ = pool.try_allocate(60).unwrap();

        let result = pool.try_allocate(50);
        assert!(matches!(result, Err(MemoryError::PoolExhausted { .. })));
    }

    #[test]
    fn test_memory_pool_peak() {
        let pool = MemoryPool::new("test", 1000);

        let _ = pool.try_allocate(100).unwrap();
        let _ = pool.try_allocate(200).unwrap();
        assert_eq!(pool.peak(), 300);

        pool.release(200);
        assert_eq!(pool.peak(), 300); // Peak unchanged
        assert_eq!(pool.used(), 100);
    }

    #[test]
    fn test_memory_reservation() {
        let pool = MemoryPool::new("test", 1000);

        let res_id = pool.reserve(500).unwrap();
        assert!(pool.use_reservation(res_id, 200).is_ok());
        assert_eq!(pool.used(), 200);

        pool.release_reservation(res_id);
    }

    #[test]
    fn test_memory_manager() {
        let manager = MemoryManager::new(10 * 1024 * 1024);

        let pool1 = manager.create_pool("pool1", 1024);
        let pool2 = manager.create_pool("pool2", 2048);

        assert!(manager.get_pool("pool1").is_some());
        assert!(manager.get_pool("pool2").is_some());
        assert!(manager.get_pool("pool3").is_none());

        let stats = manager.all_stats();
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_io_scheduler_priority() {
        let scheduler = IoScheduler::new();

        scheduler.submit(IoPriority::Low, IoType::Read, 100);
        scheduler.submit(IoPriority::High, IoType::Read, 200);
        scheduler.submit(IoPriority::Normal, IoType::Read, 150);

        // Should get high priority first
        let req = scheduler.next().unwrap();
        assert_eq!(req.priority, IoPriority::High);
        scheduler.complete(&req);

        let req = scheduler.next().unwrap();
        assert_eq!(req.priority, IoPriority::Normal);
        scheduler.complete(&req);

        let req = scheduler.next().unwrap();
        assert_eq!(req.priority, IoPriority::Low);
        scheduler.complete(&req);
    }

    #[test]
    fn test_io_scheduler_concurrency() {
        let scheduler = IoScheduler::new().with_max_concurrent(2);

        scheduler.submit(IoPriority::Normal, IoType::Read, 100);
        scheduler.submit(IoPriority::Normal, IoType::Read, 100);
        scheduler.submit(IoPriority::Normal, IoType::Read, 100);

        let _req1 = scheduler.next().unwrap();
        let _req2 = scheduler.next().unwrap();

        // Third should be blocked
        assert!(scheduler.next().is_none());
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(1000);

        assert!(limiter.try_acquire(500));
        assert!(limiter.try_acquire(500));
        assert!(!limiter.try_acquire(100)); // Should fail, no tokens left
    }

    #[test]
    fn test_workload_group() {
        let group = WorkloadGroup::new("analytics")
            .with_priority(WorkloadPriority::Low)
            .with_memory_limit(1024 * 1024)
            .with_cpu_weight(50);

        assert_eq!(group.name, "analytics");
        assert_eq!(group.priority, WorkloadPriority::Low);
        assert_eq!(group.cpu_weight, 50);
    }

    #[test]
    fn test_workload_group_state() {
        let group = WorkloadGroup::new("test");
        let state = WorkloadGroupState::new(group);

        let admission = state.try_admit().unwrap();
        assert_eq!(admission.group_name, "test");

        state.release(100);

        let stats = state.stats();
        assert_eq!(stats.total_queries, 1);
    }

    #[test]
    fn test_workload_manager() {
        let manager = WorkloadManager::new();

        let group = WorkloadGroup::new("batch")
            .with_priority(WorkloadPriority::Low);
        manager.create_group(group).unwrap();

        manager.map_user("etl_user", "batch");

        let group = manager.get_group_for_user("etl_user");
        assert_eq!(group.stats().name, "batch");

        let group = manager.get_group_for_user("unknown");
        assert_eq!(group.stats().name, "default");
    }

    #[test]
    fn test_resource_quota() {
        let quota = ResourceQuotaDef::new("test")
            .with_memory_limit(1000)
            .with_row_limit(100);

        let tracker = ResourceTracker::new(quota);

        assert!(tracker.add_memory(500).is_ok());
        assert!(tracker.add_memory(600).is_err());

        assert!(tracker.add_rows_read(50).is_ok());
        assert!(tracker.add_rows_read(60).is_err());
    }

    #[test]
    fn test_query_throttler() {
        let throttler = QueryThrottler::new(2);

        assert!(throttler.try_acquire());
        assert!(throttler.try_acquire());
        assert!(!throttler.try_acquire()); // Should be throttled

        throttler.release();
        assert!(throttler.try_acquire()); // Should work now
    }

    #[test]
    fn test_resource_governor() {
        let config = ResourceGovernorConfig {
            global_memory_limit: 1024 * 1024,
            max_concurrent_queries: 10,
            io_rate_limit: None,
            enable_workload_groups: true,
        };

        let governor = ResourceGovernor::new(config);

        let ctx = governor.admit_query("user1").unwrap();
        assert_eq!(ctx.user, "user1");

        governor.release_query(&ctx);

        let stats = governor.stats();
        assert_eq!(stats.throttler.total_admitted, 1);
    }

    #[test]
    fn test_io_stats() {
        let scheduler = IoScheduler::new();

        scheduler.submit(IoPriority::Normal, IoType::Read, 100);
        scheduler.submit(IoPriority::Normal, IoType::Write, 200);

        let stats = scheduler.stats();
        assert_eq!(stats.total_submitted, 2);
        assert_eq!(stats.pending, 2);
    }

    #[test]
    fn test_query_admission_timeout() {
        let admission = QueryAdmission {
            group_name: "test".to_string(),
            admitted_at: Instant::now() - Duration::from_secs(100),
            timeout: Duration::from_secs(60),
        };

        assert!(admission.is_timed_out());
        assert_eq!(admission.remaining(), Duration::ZERO);
    }
}
