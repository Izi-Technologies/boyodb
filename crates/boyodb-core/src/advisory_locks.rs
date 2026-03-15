//! Advisory Locks
//!
//! PostgreSQL-compatible advisory locking for application-level coordination.
//! Supports session-level and transaction-level locks with shared/exclusive modes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use std::time::{Duration, Instant};

/// Advisory lock key (64-bit or two 32-bit integers)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockKey {
    /// Single 64-bit key
    Single(i64),
    /// Two 32-bit keys (classid, objid)
    Double(i32, i32),
}

impl LockKey {
    pub fn from_single(key: i64) -> Self {
        LockKey::Single(key)
    }

    pub fn from_double(classid: i32, objid: i32) -> Self {
        LockKey::Double(classid, objid)
    }

    pub fn to_i64(&self) -> i64 {
        match self {
            LockKey::Single(k) => *k,
            LockKey::Double(c, o) => ((*c as i64) << 32) | (*o as u32 as i64),
        }
    }
}

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Exclusive lock
    Exclusive,
    /// Shared lock
    Shared,
}

/// Lock scope
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockScope {
    /// Session-level lock (held until explicitly released or session ends)
    Session,
    /// Transaction-level lock (released at transaction end)
    Transaction,
}

/// Advisory lock entry
#[derive(Debug, Clone)]
struct LockEntry {
    /// Lock mode
    mode: LockMode,
    /// Lock scope
    scope: LockScope,
    /// Session ID that holds the lock
    session_id: u64,
    /// Transaction ID (for transaction-level locks)
    transaction_id: Option<u64>,
    /// When the lock was acquired
    acquired_at: Instant,
    /// Reference count (for reentrant locks)
    ref_count: u32,
}

/// Waiting lock request
#[derive(Debug)]
struct WaitingRequest {
    session_id: u64,
    mode: LockMode,
    requested_at: Instant,
}

/// Advisory lock manager
pub struct AdvisoryLockManager {
    /// Active locks by key
    locks: RwLock<HashMap<LockKey, Vec<LockEntry>>>,
    /// Session locks (for cleanup on disconnect)
    session_locks: RwLock<HashMap<u64, HashSet<LockKey>>>,
    /// Waiting requests
    waiting: RwLock<HashMap<LockKey, Vec<WaitingRequest>>>,
    /// Lock statistics
    stats: RwLock<LockStats>,
}

/// Lock statistics
#[derive(Debug, Default, Clone)]
pub struct LockStats {
    pub locks_acquired: u64,
    pub locks_released: u64,
    pub lock_waits: u64,
    pub lock_timeouts: u64,
    pub deadlocks_detected: u64,
}

impl AdvisoryLockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            session_locks: RwLock::new(HashMap::new()),
            waiting: RwLock::new(HashMap::new()),
            stats: RwLock::new(LockStats::default()),
        }
    }

    /// Acquire exclusive session lock (blocking)
    pub fn pg_advisory_lock(&self, session_id: u64, key: LockKey) -> bool {
        self.acquire_lock(session_id, None, key, LockMode::Exclusive, LockScope::Session, None)
    }

    /// Acquire exclusive session lock with two keys
    pub fn pg_advisory_lock_double(&self, session_id: u64, classid: i32, objid: i32) -> bool {
        self.pg_advisory_lock(session_id, LockKey::Double(classid, objid))
    }

    /// Acquire shared session lock (blocking)
    pub fn pg_advisory_lock_shared(&self, session_id: u64, key: LockKey) -> bool {
        self.acquire_lock(session_id, None, key, LockMode::Shared, LockScope::Session, None)
    }

    /// Try to acquire exclusive session lock (non-blocking)
    pub fn pg_try_advisory_lock(&self, session_id: u64, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, None, key, LockMode::Exclusive, LockScope::Session)
    }

    /// Try to acquire shared session lock (non-blocking)
    pub fn pg_try_advisory_lock_shared(&self, session_id: u64, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, None, key, LockMode::Shared, LockScope::Session)
    }

    /// Release exclusive session lock
    pub fn pg_advisory_unlock(&self, session_id: u64, key: LockKey) -> bool {
        self.release_lock(session_id, key, LockMode::Exclusive)
    }

    /// Release shared session lock
    pub fn pg_advisory_unlock_shared(&self, session_id: u64, key: LockKey) -> bool {
        self.release_lock(session_id, key, LockMode::Shared)
    }

    /// Release all session locks
    pub fn pg_advisory_unlock_all(&self, session_id: u64) {
        self.release_all_session_locks(session_id);
    }

    /// Acquire transaction-level exclusive lock
    pub fn pg_advisory_xact_lock(&self, session_id: u64, transaction_id: u64, key: LockKey) -> bool {
        self.acquire_lock(session_id, Some(transaction_id), key, LockMode::Exclusive, LockScope::Transaction, None)
    }

    /// Acquire transaction-level shared lock
    pub fn pg_advisory_xact_lock_shared(&self, session_id: u64, transaction_id: u64, key: LockKey) -> bool {
        self.acquire_lock(session_id, Some(transaction_id), key, LockMode::Shared, LockScope::Transaction, None)
    }

    /// Try to acquire transaction-level exclusive lock
    pub fn pg_try_advisory_xact_lock(&self, session_id: u64, transaction_id: u64, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, Some(transaction_id), key, LockMode::Exclusive, LockScope::Transaction)
    }

    /// Try to acquire transaction-level shared lock
    pub fn pg_try_advisory_xact_lock_shared(&self, session_id: u64, transaction_id: u64, key: LockKey) -> bool {
        self.try_acquire_lock(session_id, Some(transaction_id), key, LockMode::Shared, LockScope::Transaction)
    }

    /// Release all transaction locks
    pub fn release_transaction_locks(&self, session_id: u64, transaction_id: u64) {
        let mut locks = self.locks.write();
        let mut session_locks = self.session_locks.write();
        let mut stats = self.stats.write();

        let mut keys_to_check: Vec<LockKey> = Vec::new();

        for (key, entries) in locks.iter_mut() {
            let before_len = entries.len();
            entries.retain(|e| {
                !(e.session_id == session_id && 
                  e.scope == LockScope::Transaction && 
                  e.transaction_id == Some(transaction_id))
            });
            
            if entries.len() < before_len {
                stats.locks_released += (before_len - entries.len()) as u64;
                keys_to_check.push(*key);
            }
        }

        // Clean up empty entries
        locks.retain(|_, v| !v.is_empty());

        // Update session locks
        if let Some(session_keys) = session_locks.get_mut(&session_id) {
            for key in &keys_to_check {
                if !locks.contains_key(key) || 
                   locks.get(key).map(|v| v.iter().all(|e| e.session_id != session_id)).unwrap_or(true) {
                    session_keys.remove(key);
                }
            }
        }
    }

    /// Check if lock is held
    pub fn is_locked(&self, key: LockKey) -> bool {
        let locks = self.locks.read();
        locks.get(&key).map(|v| !v.is_empty()).unwrap_or(false)
    }

    /// Get lock statistics
    pub fn stats(&self) -> LockStats {
        self.stats.read().clone()
    }

    /// List all locks for a session
    pub fn list_session_locks(&self, session_id: u64) -> Vec<(LockKey, LockMode, LockScope)> {
        let locks = self.locks.read();
        let mut result = Vec::new();

        for (key, entries) in locks.iter() {
            for entry in entries {
                if entry.session_id == session_id {
                    result.push((*key, entry.mode, entry.scope));
                }
            }
        }

        result
    }

    fn acquire_lock(
        &self,
        session_id: u64,
        transaction_id: Option<u64>,
        key: LockKey,
        mode: LockMode,
        scope: LockScope,
        timeout: Option<Duration>,
    ) -> bool {
        let start = Instant::now();
        let deadline = timeout.map(|t| start + t);

        loop {
            if self.try_acquire_lock(session_id, transaction_id, key, mode, scope) {
                return true;
            }

            // Check timeout
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    self.stats.write().lock_timeouts += 1;
                    return false;
                }
            }

            // Wait a bit before retrying
            std::thread::sleep(Duration::from_millis(10));
            self.stats.write().lock_waits += 1;
        }
    }

    fn try_acquire_lock(
        &self,
        session_id: u64,
        transaction_id: Option<u64>,
        key: LockKey,
        mode: LockMode,
        scope: LockScope,
    ) -> bool {
        let mut locks = self.locks.write();
        let mut session_locks = self.session_locks.write();

        let entries = locks.entry(key).or_default();

        // Check for conflicts
        let can_acquire = match mode {
            LockMode::Exclusive => {
                // Exclusive: no other locks, or only our own exclusive lock (reentrant)
                entries.is_empty() || 
                entries.iter().all(|e| e.session_id == session_id && e.mode == LockMode::Exclusive)
            }
            LockMode::Shared => {
                // Shared: no exclusive locks from others
                entries.iter().all(|e| e.mode == LockMode::Shared || e.session_id == session_id)
            }
        };

        if !can_acquire {
            return false;
        }

        // Check for existing lock by this session (reentrant)
        if let Some(existing) = entries.iter_mut().find(|e| {
            e.session_id == session_id && e.mode == mode && e.scope == scope
        }) {
            existing.ref_count += 1;
        } else {
            entries.push(LockEntry {
                mode,
                scope,
                session_id,
                transaction_id,
                acquired_at: Instant::now(),
                ref_count: 1,
            });
        }

        // Track session locks
        session_locks.entry(session_id).or_default().insert(key);

        self.stats.write().locks_acquired += 1;
        true
    }

    fn release_lock(&self, session_id: u64, key: LockKey, mode: LockMode) -> bool {
        let mut locks = self.locks.write();
        let mut session_locks = self.session_locks.write();

        if let Some(entries) = locks.get_mut(&key) {
            if let Some(pos) = entries.iter().position(|e| {
                e.session_id == session_id && e.mode == mode && e.scope == LockScope::Session
            }) {
                entries[pos].ref_count -= 1;
                if entries[pos].ref_count == 0 {
                    entries.remove(pos);
                    self.stats.write().locks_released += 1;

                    if entries.is_empty() {
                        locks.remove(&key);
                        if let Some(session_keys) = session_locks.get_mut(&session_id) {
                            session_keys.remove(&key);
                        }
                    }
                }
                return true;
            }
        }

        false
    }

    fn release_all_session_locks(&self, session_id: u64) {
        let mut locks = self.locks.write();
        let mut session_locks = self.session_locks.write();
        let mut stats = self.stats.write();

        if let Some(keys) = session_locks.remove(&session_id) {
            for key in keys {
                if let Some(entries) = locks.get_mut(&key) {
                    let before_len = entries.len();
                    entries.retain(|e| e.session_id != session_id);
                    stats.locks_released += (before_len - entries.len()) as u64;
                }
            }

            locks.retain(|_, v| !v.is_empty());
        }
    }
}

impl Default for AdvisoryLockManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exclusive_lock() {
        let mgr = AdvisoryLockManager::new();
        let key = LockKey::Single(12345);

        // Session 1 acquires lock
        assert!(mgr.pg_advisory_lock(1, key));
        
        // Session 2 cannot acquire (non-blocking)
        assert!(!mgr.pg_try_advisory_lock(2, key));
        
        // Session 1 releases
        assert!(mgr.pg_advisory_unlock(1, key));
        
        // Now session 2 can acquire
        assert!(mgr.pg_try_advisory_lock(2, key));
    }

    #[test]
    fn test_shared_lock() {
        let mgr = AdvisoryLockManager::new();
        let key = LockKey::Single(12345);

        // Multiple sessions can acquire shared lock
        assert!(mgr.pg_advisory_lock_shared(1, key));
        assert!(mgr.pg_advisory_lock_shared(2, key));
        
        // But not exclusive
        assert!(!mgr.pg_try_advisory_lock(3, key));
        
        // Release shared locks
        mgr.pg_advisory_unlock_shared(1, key);
        mgr.pg_advisory_unlock_shared(2, key);
        
        // Now exclusive works
        assert!(mgr.pg_try_advisory_lock(3, key));
    }

    #[test]
    fn test_reentrant_lock() {
        let mgr = AdvisoryLockManager::new();
        let key = LockKey::Single(12345);

        // Same session can acquire same lock multiple times
        assert!(mgr.pg_advisory_lock(1, key));
        assert!(mgr.pg_advisory_lock(1, key));
        
        // Need to release multiple times
        assert!(mgr.pg_advisory_unlock(1, key));
        assert!(mgr.is_locked(key));
        assert!(mgr.pg_advisory_unlock(1, key));
        assert!(!mgr.is_locked(key));
    }

    #[test]
    fn test_transaction_lock() {
        let mgr = AdvisoryLockManager::new();
        let key = LockKey::Single(12345);

        // Acquire transaction lock
        assert!(mgr.pg_advisory_xact_lock(1, 100, key));
        
        // Lock is held
        assert!(mgr.is_locked(key));
        
        // Release on transaction end
        mgr.release_transaction_locks(1, 100);
        
        // Lock is released
        assert!(!mgr.is_locked(key));
    }

    #[test]
    fn test_unlock_all() {
        let mgr = AdvisoryLockManager::new();
        
        // Acquire multiple locks
        mgr.pg_advisory_lock(1, LockKey::Single(1));
        mgr.pg_advisory_lock(1, LockKey::Single(2));
        mgr.pg_advisory_lock(1, LockKey::Single(3));
        
        // Release all
        mgr.pg_advisory_unlock_all(1);
        
        // All released
        assert!(!mgr.is_locked(LockKey::Single(1)));
        assert!(!mgr.is_locked(LockKey::Single(2)));
        assert!(!mgr.is_locked(LockKey::Single(3)));
    }

    #[test]
    fn test_double_key() {
        let mgr = AdvisoryLockManager::new();
        
        assert!(mgr.pg_advisory_lock_double(1, 1000, 1));
        assert!(!mgr.pg_try_advisory_lock(2, LockKey::Double(1000, 1)));
        
        mgr.pg_advisory_unlock(1, LockKey::Double(1000, 1));
        assert!(mgr.pg_try_advisory_lock(2, LockKey::Double(1000, 1)));
    }
}
