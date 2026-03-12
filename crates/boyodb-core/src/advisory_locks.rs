//! Advisory Locks Implementation
//!
//! PostgreSQL-compatible advisory locks for application-level coordination.
//!
//! # Features
//! - Session-level advisory locks (held until session ends)
//! - Transaction-level advisory locks (released at transaction end)
//! - Shared and exclusive lock modes
//! - Try-lock variants (non-blocking)
//! - 64-bit and dual 32-bit key formats
//!
//! # Functions
//! - pg_advisory_lock(key) / pg_advisory_lock(key1, key2)
//! - pg_advisory_unlock(key) / pg_advisory_unlock(key1, key2)
//! - pg_try_advisory_lock(key) / pg_try_advisory_lock(key1, key2)
//! - pg_advisory_lock_shared(key)
//! - pg_advisory_unlock_shared(key)
//! - pg_advisory_xact_lock(key) - transaction-level
//! - pg_advisory_unlock_all()
//!
//! # Example
//! ```sql
//! -- Acquire exclusive lock on resource 12345
//! SELECT pg_advisory_lock(12345);
//!
//! -- Do work...
//!
//! -- Release lock
//! SELECT pg_advisory_unlock(12345);
//!
//! -- Or use dual-key format
//! SELECT pg_advisory_lock(1, 100);  -- classid=1, objid=100
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

// ============================================================================
// Types and Errors
// ============================================================================

/// Session identifier
pub type SessionId = u64;

/// Transaction identifier
pub type TransactionId = u64;

/// Advisory lock key (64-bit)
pub type LockKey = i64;

/// Errors from advisory lock operations
#[derive(Debug, Clone)]
pub enum AdvisoryLockError {
    /// Lock not held by this session
    LockNotHeld { key: LockKey, session_id: SessionId },
    /// Session not found
    SessionNotFound(SessionId),
    /// Lock acquisition timeout
    Timeout { key: LockKey, timeout_ms: u64 },
    /// Deadlock detected
    DeadlockDetected { key: LockKey, session_id: SessionId },
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for AdvisoryLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LockNotHeld { key, session_id } => {
                write!(f, "advisory lock {} not held by session {}", key, session_id)
            }
            Self::SessionNotFound(id) => write!(f, "session not found: {}", id),
            Self::Timeout { key, timeout_ms } => {
                write!(f, "timeout waiting for advisory lock {} ({}ms)", key, timeout_ms)
            }
            Self::DeadlockDetected { key, session_id } => {
                write!(f, "deadlock detected: session {} waiting for lock {}", session_id, key)
            }
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for AdvisoryLockError {}

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// Exclusive lock - only one holder
    Exclusive,
    /// Shared lock - multiple holders allowed
    Shared,
}

/// Lock scope
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockScope {
    /// Session-level: held until explicitly released or session ends
    Session,
    /// Transaction-level: automatically released at transaction end
    Transaction,
}

// ============================================================================
// Lock Key Utilities
// ============================================================================

/// Convert two 32-bit integers to a single 64-bit key
/// PostgreSQL uses this format: (classid << 32) | objid
pub fn make_lock_key(key1: i32, key2: i32) -> LockKey {
    ((key1 as i64) << 32) | ((key2 as i64) & 0xFFFFFFFF)
}

/// Extract two 32-bit integers from a 64-bit key
pub fn split_lock_key(key: LockKey) -> (i32, i32) {
    let key1 = (key >> 32) as i32;
    let key2 = (key & 0xFFFFFFFF) as i32;
    (key1, key2)
}

// ============================================================================
// Lock State
// ============================================================================

/// Information about a held lock
#[derive(Debug, Clone)]
pub struct HeldLock {
    /// Lock key
    pub key: LockKey,
    /// Session holding the lock
    pub session_id: SessionId,
    /// Lock mode (exclusive/shared)
    pub mode: LockMode,
    /// Lock scope (session/transaction)
    pub scope: LockScope,
    /// Transaction ID (for transaction-level locks)
    pub transaction_id: Option<TransactionId>,
    /// Time when lock was acquired
    pub acquired_at: SystemTime,
    /// Reference count (for re-entrant locks)
    pub count: u32,
}

/// State of a single advisory lock
#[derive(Debug)]
struct LockState {
    /// Lock key
    key: LockKey,
    /// Exclusive holder (if any)
    exclusive_holder: Option<(SessionId, u32)>, // (session_id, count)
    /// Shared holders (session_id -> count)
    shared_holders: HashMap<SessionId, u32>,
    /// Sessions waiting for this lock
    waiters: Vec<(SessionId, LockMode, Instant)>,
}

impl LockState {
    fn new(key: LockKey) -> Self {
        Self {
            key,
            exclusive_holder: None,
            shared_holders: HashMap::new(),
            waiters: Vec::new(),
        }
    }

    /// Check if lock can be acquired
    fn can_acquire(&self, session_id: SessionId, mode: LockMode) -> bool {
        match mode {
            LockMode::Exclusive => {
                // Can acquire if:
                // - No exclusive holder OR we already hold it
                // - No shared holders OR only we hold shared
                match &self.exclusive_holder {
                    Some((holder, _)) if *holder != session_id => false,
                    _ => {
                        // Check shared holders
                        self.shared_holders.is_empty()
                            || (self.shared_holders.len() == 1
                                && self.shared_holders.contains_key(&session_id))
                    }
                }
            }
            LockMode::Shared => {
                // Can acquire if no exclusive holder or we already hold exclusive
                match &self.exclusive_holder {
                    None => true,
                    Some((holder, _)) => *holder == session_id,
                }
            }
        }
    }

    /// Acquire the lock
    fn acquire(&mut self, session_id: SessionId, mode: LockMode) -> bool {
        if !self.can_acquire(session_id, mode) {
            return false;
        }

        match mode {
            LockMode::Exclusive => {
                if let Some((holder, count)) = &mut self.exclusive_holder {
                    if *holder == session_id {
                        *count += 1;
                    }
                } else {
                    self.exclusive_holder = Some((session_id, 1));
                }
            }
            LockMode::Shared => {
                *self.shared_holders.entry(session_id).or_insert(0) += 1;
            }
        }
        true
    }

    /// Release the lock
    fn release(&mut self, session_id: SessionId, mode: LockMode) -> bool {
        match mode {
            LockMode::Exclusive => {
                if let Some((holder, count)) = &mut self.exclusive_holder {
                    if *holder == session_id {
                        if *count > 1 {
                            *count -= 1;
                        } else {
                            self.exclusive_holder = None;
                        }
                        return true;
                    }
                }
                false
            }
            LockMode::Shared => {
                if let Some(count) = self.shared_holders.get_mut(&session_id) {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        self.shared_holders.remove(&session_id);
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Release all locks held by a session
    fn release_all(&mut self, session_id: SessionId) {
        if let Some((holder, _)) = &self.exclusive_holder {
            if *holder == session_id {
                self.exclusive_holder = None;
            }
        }
        self.shared_holders.remove(&session_id);
    }

    /// Check if lock is free
    fn is_free(&self) -> bool {
        self.exclusive_holder.is_none() && self.shared_holders.is_empty()
    }

    /// Get holders count
    fn holder_count(&self) -> usize {
        let exclusive_count = if self.exclusive_holder.is_some() { 1 } else { 0 };
        exclusive_count + self.shared_holders.len()
    }
}

// ============================================================================
// Session State
// ============================================================================

/// Session's advisory lock state
#[derive(Debug)]
pub struct SessionLockState {
    /// Session ID
    pub session_id: SessionId,
    /// Session-level locks held (key -> (mode, count))
    session_locks: HashMap<LockKey, (LockMode, u32)>,
    /// Transaction-level locks held (key -> (mode, count, txn_id))
    transaction_locks: HashMap<LockKey, (LockMode, u32, TransactionId)>,
    /// Current transaction ID
    current_transaction: Option<TransactionId>,
}

impl SessionLockState {
    fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            session_locks: HashMap::new(),
            transaction_locks: HashMap::new(),
            current_transaction: None,
        }
    }

    fn add_lock(&mut self, key: LockKey, mode: LockMode, scope: LockScope, txn_id: Option<TransactionId>) {
        match scope {
            LockScope::Session => {
                let entry = self.session_locks.entry(key).or_insert((mode, 0));
                entry.1 += 1;
            }
            LockScope::Transaction => {
                if let Some(tid) = txn_id.or(self.current_transaction) {
                    let entry = self.transaction_locks.entry(key).or_insert((mode, 0, tid));
                    entry.1 += 1;
                }
            }
        }
    }

    fn remove_lock(&mut self, key: LockKey, mode: LockMode) -> bool {
        // Try session locks first
        if let Some((m, count)) = self.session_locks.get_mut(&key) {
            if *m == mode {
                if *count > 1 {
                    *count -= 1;
                } else {
                    self.session_locks.remove(&key);
                }
                return true;
            }
        }

        // Try transaction locks
        if let Some((m, count, _)) = self.transaction_locks.get_mut(&key) {
            if *m == mode {
                if *count > 1 {
                    *count -= 1;
                } else {
                    self.transaction_locks.remove(&key);
                }
                return true;
            }
        }

        false
    }

    fn get_all_locks(&self) -> Vec<(LockKey, LockMode, LockScope)> {
        let mut locks = Vec::new();

        for (key, (mode, _)) in &self.session_locks {
            locks.push((*key, *mode, LockScope::Session));
        }

        for (key, (mode, _, _)) in &self.transaction_locks {
            locks.push((*key, *mode, LockScope::Transaction));
        }

        locks
    }
}

// ============================================================================
// Advisory Lock Manager
// ============================================================================

/// Statistics for advisory locks
#[derive(Debug, Clone, Default)]
pub struct AdvisoryLockStats {
    /// Total lock acquisitions
    pub total_acquisitions: u64,
    /// Total lock releases
    pub total_releases: u64,
    /// Total lock waits
    pub total_waits: u64,
    /// Total timeouts
    pub total_timeouts: u64,
    /// Current held locks
    pub current_locks: usize,
    /// Current waiting sessions
    pub current_waiters: usize,
}

/// Advisory Lock Manager
pub struct AdvisoryLockManager {
    /// Lock states (key -> LockState)
    locks: RwLock<HashMap<LockKey, LockState>>,
    /// Session states (session_id -> SessionLockState)
    sessions: RwLock<HashMap<SessionId, SessionLockState>>,
    /// Condition variable for lock waiting
    lock_available: Arc<(Mutex<()>, Condvar)>,
    /// Statistics
    stats: RwLock<AdvisoryLockStats>,
    /// Default lock timeout (0 = no timeout)
    default_timeout_ms: u64,
}

impl Default for AdvisoryLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvisoryLockManager {
    /// Create a new advisory lock manager
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            lock_available: Arc::new((Mutex::new(()), Condvar::new())),
            stats: RwLock::new(AdvisoryLockStats::default()),
            default_timeout_ms: 0,
        }
    }

    /// Create with custom timeout
    pub fn with_timeout(timeout_ms: u64) -> Self {
        let mut manager = Self::new();
        manager.default_timeout_ms = timeout_ms;
        manager
    }

    /// Register a session
    pub fn register_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.write().unwrap();
        sessions
            .entry(session_id)
            .or_insert_with(|| SessionLockState::new(session_id));
    }

    /// Unregister a session (release all locks)
    pub fn unregister_session(&self, session_id: SessionId) {
        // Get locks held by this session
        let locks_to_release = {
            let sessions = self.sessions.read().unwrap();
            if let Some(state) = sessions.get(&session_id) {
                state.get_all_locks()
            } else {
                return;
            }
        };

        // Release all locks
        {
            let mut locks = self.locks.write().unwrap();
            for (key, _mode, _scope) in &locks_to_release {
                if let Some(lock_state) = locks.get_mut(key) {
                    lock_state.release_all(session_id);
                    if lock_state.is_free() {
                        locks.remove(key);
                    }
                }
            }
        }

        // Remove session state
        {
            let mut sessions = self.sessions.write().unwrap();
            sessions.remove(&session_id);
        }

        // Notify waiters
        let (_, cvar) = &*self.lock_available;
        cvar.notify_all();
    }

    /// Begin a transaction for a session
    pub fn begin_transaction(&self, session_id: SessionId, transaction_id: TransactionId) {
        let mut sessions = self.sessions.write().unwrap();
        if let Some(state) = sessions.get_mut(&session_id) {
            state.current_transaction = Some(transaction_id);
        }
    }

    /// End a transaction (release transaction-level locks)
    pub fn end_transaction(&self, session_id: SessionId, transaction_id: TransactionId) {
        // Get transaction locks to release
        let locks_to_release: Vec<(LockKey, LockMode)> = {
            let mut sessions = self.sessions.write().unwrap();
            if let Some(state) = sessions.get_mut(&session_id) {
                state.current_transaction = None;
                let locks: Vec<_> = state
                    .transaction_locks
                    .iter()
                    .filter(|(_, (_, _, tid))| *tid == transaction_id)
                    .map(|(key, (mode, _, _))| (*key, *mode))
                    .collect();
                for (key, _) in &locks {
                    state.transaction_locks.remove(key);
                }
                locks
            } else {
                return;
            }
        };

        // Release locks
        {
            let mut locks = self.locks.write().unwrap();
            for (key, mode) in locks_to_release {
                if let Some(lock_state) = locks.get_mut(&key) {
                    lock_state.release(session_id, mode);
                    if lock_state.is_free() {
                        locks.remove(&key);
                    }
                }
            }
        }

        // Notify waiters
        let (_, cvar) = &*self.lock_available;
        cvar.notify_all();
    }

    /// pg_advisory_lock - Acquire exclusive session-level lock (blocking)
    pub fn pg_advisory_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
    ) -> Result<(), AdvisoryLockError> {
        self.acquire_lock(session_id, key, LockMode::Exclusive, LockScope::Session, None)
    }

    /// pg_advisory_lock with two keys
    pub fn pg_advisory_lock_2(
        &self,
        session_id: SessionId,
        key1: i32,
        key2: i32,
    ) -> Result<(), AdvisoryLockError> {
        self.pg_advisory_lock(session_id, make_lock_key(key1, key2))
    }

    /// pg_try_advisory_lock - Try to acquire exclusive lock (non-blocking)
    pub fn pg_try_advisory_lock(&self, session_id: SessionId, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, key, LockMode::Exclusive, LockScope::Session, None)
    }

    /// pg_try_advisory_lock with two keys
    pub fn pg_try_advisory_lock_2(&self, session_id: SessionId, key1: i32, key2: i32) -> bool {
        self.pg_try_advisory_lock(session_id, make_lock_key(key1, key2))
    }

    /// pg_advisory_unlock - Release exclusive session-level lock
    pub fn pg_advisory_unlock(
        &self,
        session_id: SessionId,
        key: LockKey,
    ) -> Result<bool, AdvisoryLockError> {
        self.release_lock(session_id, key, LockMode::Exclusive)
    }

    /// pg_advisory_unlock with two keys
    pub fn pg_advisory_unlock_2(
        &self,
        session_id: SessionId,
        key1: i32,
        key2: i32,
    ) -> Result<bool, AdvisoryLockError> {
        self.pg_advisory_unlock(session_id, make_lock_key(key1, key2))
    }

    /// pg_advisory_lock_shared - Acquire shared session-level lock
    pub fn pg_advisory_lock_shared(
        &self,
        session_id: SessionId,
        key: LockKey,
    ) -> Result<(), AdvisoryLockError> {
        self.acquire_lock(session_id, key, LockMode::Shared, LockScope::Session, None)
    }

    /// pg_try_advisory_lock_shared - Try to acquire shared lock
    pub fn pg_try_advisory_lock_shared(&self, session_id: SessionId, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, key, LockMode::Shared, LockScope::Session, None)
    }

    /// pg_advisory_unlock_shared - Release shared session-level lock
    pub fn pg_advisory_unlock_shared(
        &self,
        session_id: SessionId,
        key: LockKey,
    ) -> Result<bool, AdvisoryLockError> {
        self.release_lock(session_id, key, LockMode::Shared)
    }

    /// pg_advisory_xact_lock - Acquire exclusive transaction-level lock
    pub fn pg_advisory_xact_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
        transaction_id: TransactionId,
    ) -> Result<(), AdvisoryLockError> {
        self.acquire_lock(
            session_id,
            key,
            LockMode::Exclusive,
            LockScope::Transaction,
            Some(transaction_id),
        )
    }

    /// pg_try_advisory_xact_lock - Try to acquire transaction-level lock
    pub fn pg_try_advisory_xact_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
        transaction_id: TransactionId,
    ) -> bool {
        self.try_acquire_lock(
            session_id,
            key,
            LockMode::Exclusive,
            LockScope::Transaction,
            Some(transaction_id),
        )
    }

    /// pg_advisory_xact_lock_shared - Acquire shared transaction-level lock
    pub fn pg_advisory_xact_lock_shared(
        &self,
        session_id: SessionId,
        key: LockKey,
        transaction_id: TransactionId,
    ) -> Result<(), AdvisoryLockError> {
        self.acquire_lock(
            session_id,
            key,
            LockMode::Shared,
            LockScope::Transaction,
            Some(transaction_id),
        )
    }

    /// pg_advisory_unlock_all - Release all session-level locks
    pub fn pg_advisory_unlock_all(&self, session_id: SessionId) -> Result<usize, AdvisoryLockError> {
        // Get all session locks
        let locks_to_release: Vec<(LockKey, LockMode)> = {
            let mut sessions = self.sessions.write().unwrap();
            let state = sessions
                .get_mut(&session_id)
                .ok_or(AdvisoryLockError::SessionNotFound(session_id))?;
            let locks: Vec<_> = state
                .session_locks
                .iter()
                .map(|(key, (mode, _))| (*key, *mode))
                .collect();
            state.session_locks.clear();
            locks
        };

        let count = locks_to_release.len();

        // Release all locks
        {
            let mut locks = self.locks.write().unwrap();
            for (key, mode) in locks_to_release {
                if let Some(lock_state) = locks.get_mut(&key) {
                    lock_state.release(session_id, mode);
                    if lock_state.is_free() {
                        locks.remove(&key);
                    }
                }
            }
        }

        // Notify waiters
        let (_, cvar) = &*self.lock_available;
        cvar.notify_all();

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_releases += count as u64;
        }

        Ok(count)
    }

    /// Internal: Try to acquire a lock (non-blocking)
    fn try_acquire_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
        mode: LockMode,
        scope: LockScope,
        transaction_id: Option<TransactionId>,
    ) -> bool {
        // Ensure session exists
        self.register_session(session_id);

        // Try to acquire
        let acquired = {
            let mut locks = self.locks.write().unwrap();
            let lock_state = locks.entry(key).or_insert_with(|| LockState::new(key));
            lock_state.acquire(session_id, mode)
        };

        if acquired {
            // Update session state
            let mut sessions = self.sessions.write().unwrap();
            if let Some(state) = sessions.get_mut(&session_id) {
                state.add_lock(key, mode, scope, transaction_id);
            }

            // Update stats
            let mut stats = self.stats.write().unwrap();
            stats.total_acquisitions += 1;
            stats.current_locks += 1;
        }

        acquired
    }

    /// Internal: Acquire a lock (blocking with optional timeout)
    fn acquire_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
        mode: LockMode,
        scope: LockScope,
        transaction_id: Option<TransactionId>,
    ) -> Result<(), AdvisoryLockError> {
        // Try non-blocking first
        if self.try_acquire_lock(session_id, key, mode, scope, transaction_id) {
            return Ok(());
        }

        // Add to waiters
        {
            let mut locks = self.locks.write().unwrap();
            let lock_state = locks.entry(key).or_insert_with(|| LockState::new(key));
            lock_state.waiters.push((session_id, mode, Instant::now()));
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_waits += 1;
            stats.current_waiters += 1;
        }

        // Wait for lock
        let timeout = if self.default_timeout_ms > 0 {
            Some(Duration::from_millis(self.default_timeout_ms))
        } else {
            None
        };

        let start = Instant::now();
        let (lock, cvar) = &*self.lock_available;

        loop {
            // Try to acquire
            if self.try_acquire_lock(session_id, key, mode, scope, transaction_id) {
                // Remove from waiters
                {
                    let mut locks = self.locks.write().unwrap();
                    if let Some(lock_state) = locks.get_mut(&key) {
                        lock_state.waiters.retain(|(s, _, _)| *s != session_id);
                    }
                }
                // Update stats
                {
                    let mut stats = self.stats.write().unwrap();
                    stats.current_waiters = stats.current_waiters.saturating_sub(1);
                }
                return Ok(());
            }

            // Check timeout
            if let Some(t) = timeout {
                if start.elapsed() >= t {
                    // Remove from waiters
                    {
                        let mut locks = self.locks.write().unwrap();
                        if let Some(lock_state) = locks.get_mut(&key) {
                            lock_state.waiters.retain(|(s, _, _)| *s != session_id);
                        }
                    }
                    // Update stats
                    {
                        let mut stats = self.stats.write().unwrap();
                        stats.total_timeouts += 1;
                        stats.current_waiters = stats.current_waiters.saturating_sub(1);
                    }
                    return Err(AdvisoryLockError::Timeout {
                        key,
                        timeout_ms: self.default_timeout_ms,
                    });
                }
            }

            // Wait for notification
            let guard = lock.lock().unwrap();
            let wait_time = timeout
                .map(|t| t.saturating_sub(start.elapsed()))
                .unwrap_or(Duration::from_millis(100));
            let _ = cvar.wait_timeout(guard, wait_time);
        }
    }

    /// Internal: Release a lock
    fn release_lock(
        &self,
        session_id: SessionId,
        key: LockKey,
        mode: LockMode,
    ) -> Result<bool, AdvisoryLockError> {
        // Update session state
        {
            let mut sessions = self.sessions.write().unwrap();
            let state = sessions
                .get_mut(&session_id)
                .ok_or(AdvisoryLockError::SessionNotFound(session_id))?;
            if !state.remove_lock(key, mode) {
                return Ok(false);
            }
        }

        // Release lock
        let released = {
            let mut locks = self.locks.write().unwrap();
            if let Some(lock_state) = locks.get_mut(&key) {
                let released = lock_state.release(session_id, mode);
                if lock_state.is_free() {
                    locks.remove(&key);
                }
                released
            } else {
                false
            }
        };

        if released {
            // Notify waiters
            let (_, cvar) = &*self.lock_available;
            cvar.notify_all();

            // Update stats
            let mut stats = self.stats.write().unwrap();
            stats.total_releases += 1;
            stats.current_locks = stats.current_locks.saturating_sub(1);
        }

        Ok(released)
    }

    /// Get all locks held by a session
    pub fn get_session_locks(&self, session_id: SessionId) -> Result<Vec<HeldLock>, AdvisoryLockError> {
        let sessions = self.sessions.read().unwrap();
        let state = sessions
            .get(&session_id)
            .ok_or(AdvisoryLockError::SessionNotFound(session_id))?;

        let mut locks = Vec::new();

        for (key, (mode, count)) in &state.session_locks {
            locks.push(HeldLock {
                key: *key,
                session_id,
                mode: *mode,
                scope: LockScope::Session,
                transaction_id: None,
                acquired_at: SystemTime::now(), // We don't track this currently
                count: *count,
            });
        }

        for (key, (mode, count, txn_id)) in &state.transaction_locks {
            locks.push(HeldLock {
                key: *key,
                session_id,
                mode: *mode,
                scope: LockScope::Transaction,
                transaction_id: Some(*txn_id),
                acquired_at: SystemTime::now(),
                count: *count,
            });
        }

        Ok(locks)
    }

    /// Get all advisory locks (for pg_locks view)
    pub fn get_all_locks(&self) -> Vec<LockInfo> {
        let locks = self.locks.read().unwrap();
        let mut result = Vec::new();

        for (key, state) in locks.iter() {
            if let Some((session_id, count)) = &state.exclusive_holder {
                result.push(LockInfo {
                    key: *key,
                    session_id: *session_id,
                    mode: LockMode::Exclusive,
                    granted: true,
                    count: *count,
                });
            }

            for (session_id, count) in &state.shared_holders {
                result.push(LockInfo {
                    key: *key,
                    session_id: *session_id,
                    mode: LockMode::Shared,
                    granted: true,
                    count: *count,
                });
            }

            for (session_id, mode, _) in &state.waiters {
                result.push(LockInfo {
                    key: *key,
                    session_id: *session_id,
                    mode: *mode,
                    granted: false,
                    count: 0,
                });
            }
        }

        result
    }

    /// Get statistics
    pub fn stats(&self) -> AdvisoryLockStats {
        self.stats.read().unwrap().clone()
    }
}

/// Information about a lock (for pg_locks)
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Lock key
    pub key: LockKey,
    /// Session ID
    pub session_id: SessionId,
    /// Lock mode
    pub mode: LockMode,
    /// Whether lock is granted
    pub granted: bool,
    /// Reference count
    pub count: u32,
}

impl LockInfo {
    /// Get classid (high 32 bits)
    pub fn classid(&self) -> i32 {
        split_lock_key(self.key).0
    }

    /// Get objid (low 32 bits)
    pub fn objid(&self) -> i32 {
        split_lock_key(self.key).1
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_lock_unlock() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);

        // Acquire lock
        manager.pg_advisory_lock(1, 100).unwrap();

        // Verify lock is held
        let locks = manager.get_session_locks(1).unwrap();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].key, 100);
        assert_eq!(locks[0].mode, LockMode::Exclusive);

        // Release lock
        let released = manager.pg_advisory_unlock(1, 100).unwrap();
        assert!(released);

        // Verify lock is released
        let locks = manager.get_session_locks(1).unwrap();
        assert!(locks.is_empty());
    }

    #[test]
    fn test_try_lock() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);

        // Session 1 acquires lock
        assert!(manager.pg_try_advisory_lock(1, 100));

        // Session 2 cannot acquire (non-blocking)
        assert!(!manager.pg_try_advisory_lock(2, 100));

        // Session 1 releases
        manager.pg_advisory_unlock(1, 100).unwrap();

        // Now session 2 can acquire
        assert!(manager.pg_try_advisory_lock(2, 100));
    }

    #[test]
    fn test_shared_locks() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);
        manager.register_session(3);

        // Multiple sessions can hold shared locks
        assert!(manager.pg_try_advisory_lock_shared(1, 100));
        assert!(manager.pg_try_advisory_lock_shared(2, 100));

        // But exclusive lock is blocked
        assert!(!manager.pg_try_advisory_lock(3, 100));

        // Release one shared lock
        manager.pg_advisory_unlock_shared(1, 100).unwrap();

        // Still can't get exclusive (session 2 still holds shared)
        assert!(!manager.pg_try_advisory_lock(3, 100));

        // Release all shared locks
        manager.pg_advisory_unlock_shared(2, 100).unwrap();

        // Now exclusive is available
        assert!(manager.pg_try_advisory_lock(3, 100));
    }

    #[test]
    fn test_reentrant_locks() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);

        // Acquire same lock multiple times
        manager.pg_advisory_lock(1, 100).unwrap();
        manager.pg_advisory_lock(1, 100).unwrap();
        manager.pg_advisory_lock(1, 100).unwrap();

        // Need to release 3 times
        assert!(manager.pg_advisory_unlock(1, 100).unwrap());
        assert!(manager.pg_advisory_unlock(1, 100).unwrap());
        assert!(manager.pg_advisory_unlock(1, 100).unwrap());

        // Fourth release should return false (not held)
        assert!(!manager.pg_advisory_unlock(1, 100).unwrap());
    }

    #[test]
    fn test_dual_key_format() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);

        // Use dual key format
        assert!(manager.pg_try_advisory_lock_2(1, 1, 100));

        // Verify it's the same as make_lock_key - re-entrant acquisition succeeds
        let key = make_lock_key(1, 100);
        assert!(manager.pg_try_advisory_lock(1, key)); // Re-entrant succeeds

        // Need to unlock twice (once for each acquisition)
        assert!(manager.pg_advisory_unlock_2(1, 1, 100).unwrap());
        assert!(manager.pg_advisory_unlock(1, key).unwrap());

        // Third unlock should fail (not held anymore)
        assert!(!manager.pg_advisory_unlock(1, key).unwrap());
    }

    #[test]
    fn test_transaction_locks() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);

        // Acquire transaction lock
        assert!(manager.pg_try_advisory_xact_lock(1, 100, 1000));

        // Session 2 cannot acquire
        assert!(!manager.pg_try_advisory_lock(2, 100));

        // End transaction releases lock
        manager.end_transaction(1, 1000);

        // Now session 2 can acquire
        assert!(manager.pg_try_advisory_lock(2, 100));
    }

    #[test]
    fn test_unlock_all() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);

        // Acquire multiple locks
        manager.pg_advisory_lock(1, 100).unwrap();
        manager.pg_advisory_lock(1, 200).unwrap();
        manager.pg_advisory_lock(1, 300).unwrap();

        // Unlock all
        let count = manager.pg_advisory_unlock_all(1).unwrap();
        assert_eq!(count, 3);

        // All locks should be released
        let locks = manager.get_session_locks(1).unwrap();
        assert!(locks.is_empty());
    }

    #[test]
    fn test_session_cleanup() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);

        // Session 1 holds lock
        manager.pg_advisory_lock(1, 100).unwrap();

        // Session 2 cannot acquire
        assert!(!manager.pg_try_advisory_lock(2, 100));

        // Session 1 disconnects
        manager.unregister_session(1);

        // Now session 2 can acquire
        assert!(manager.pg_try_advisory_lock(2, 100));
    }

    #[test]
    fn test_key_conversion() {
        // Test key conversion
        let key = make_lock_key(12345, 67890);
        let (k1, k2) = split_lock_key(key);
        assert_eq!(k1, 12345);
        assert_eq!(k2, 67890);

        // Test negative values
        let key2 = make_lock_key(-1, -1);
        let (k1, k2) = split_lock_key(key2);
        assert_eq!(k1, -1);
        assert_eq!(k2, -1);
    }

    #[test]
    fn test_get_all_locks() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);

        manager.pg_advisory_lock(1, 100).unwrap();
        manager.pg_advisory_lock_shared(2, 200).unwrap();

        let all_locks = manager.get_all_locks();
        assert_eq!(all_locks.len(), 2);

        let lock1 = all_locks.iter().find(|l| l.key == 100).unwrap();
        assert_eq!(lock1.session_id, 1);
        assert_eq!(lock1.mode, LockMode::Exclusive);
        assert!(lock1.granted);

        let lock2 = all_locks.iter().find(|l| l.key == 200).unwrap();
        assert_eq!(lock2.session_id, 2);
        assert_eq!(lock2.mode, LockMode::Shared);
        assert!(lock2.granted);
    }

    #[test]
    fn test_stats() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);

        manager.pg_advisory_lock(1, 100).unwrap();
        manager.pg_advisory_unlock(1, 100).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total_acquisitions, 1);
        assert_eq!(stats.total_releases, 1);
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let manager = AdvisoryLockManager::new();
        manager.register_session(1);
        manager.register_session(2);

        // Session 1 holds exclusive
        manager.pg_advisory_lock(1, 100).unwrap();

        // Session 2 cannot get shared
        assert!(!manager.pg_try_advisory_lock_shared(2, 100));

        // Release exclusive
        manager.pg_advisory_unlock(1, 100).unwrap();

        // Now shared works
        assert!(manager.pg_try_advisory_lock_shared(2, 100));
    }
}
