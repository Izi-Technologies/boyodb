//! Two-Phase Commit (2PC) for Distributed Transactions
//!
//! Implements distributed transaction coordination:
//! - Coordinator for managing global transactions
//! - Participant protocol for local transaction handling
//! - Recovery and failure handling
//! - Timeout and deadlock detection

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Types and Errors
// ============================================================================

/// Error types for 2PC operations
#[derive(Debug, Clone)]
pub enum TwoPhaseCommitError {
    /// Transaction not found
    TransactionNotFound(GlobalTransactionId),
    /// Participant not found
    ParticipantNotFound(ParticipantId),
    /// Transaction already exists
    TransactionExists(GlobalTransactionId),
    /// Invalid state transition
    InvalidStateTransition { from: TransactionState, to: TransactionState },
    /// Prepare failed
    PrepareFailed { participant: ParticipantId, reason: String },
    /// Commit failed
    CommitFailed { participant: ParticipantId, reason: String },
    /// Rollback failed
    RollbackFailed { participant: ParticipantId, reason: String },
    /// Timeout
    Timeout(Duration),
    /// Network error
    NetworkError(String),
    /// Coordinator failure
    CoordinatorFailure(String),
    /// Heuristic decision made
    HeuristicDecision(HeuristicOutcome),
    /// Recovery error
    RecoveryError(String),
}

impl std::fmt::Display for TwoPhaseCommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransactionNotFound(id) => write!(f, "transaction not found: {:?}", id),
            Self::ParticipantNotFound(id) => write!(f, "participant not found: {:?}", id),
            Self::TransactionExists(id) => write!(f, "transaction already exists: {:?}", id),
            Self::InvalidStateTransition { from, to } => {
                write!(f, "invalid state transition: {:?} -> {:?}", from, to)
            }
            Self::PrepareFailed { participant, reason } => {
                write!(f, "prepare failed for {:?}: {}", participant, reason)
            }
            Self::CommitFailed { participant, reason } => {
                write!(f, "commit failed for {:?}: {}", participant, reason)
            }
            Self::RollbackFailed { participant, reason } => {
                write!(f, "rollback failed for {:?}: {}", participant, reason)
            }
            Self::Timeout(d) => write!(f, "operation timed out after {:?}", d),
            Self::NetworkError(msg) => write!(f, "network error: {}", msg),
            Self::CoordinatorFailure(msg) => write!(f, "coordinator failure: {}", msg),
            Self::HeuristicDecision(outcome) => write!(f, "heuristic decision: {:?}", outcome),
            Self::RecoveryError(msg) => write!(f, "recovery error: {}", msg),
        }
    }
}

impl std::error::Error for TwoPhaseCommitError {}

/// Global transaction identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalTransactionId {
    /// Coordinator node ID
    pub coordinator_id: String,
    /// Local transaction sequence
    pub sequence: u64,
    /// Creation timestamp
    pub timestamp: u64,
}

impl GlobalTransactionId {
    pub fn new(coordinator_id: &str, sequence: u64) -> Self {
        Self {
            coordinator_id: coordinator_id.to_string(),
            sequence,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}

/// Participant identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParticipantId {
    pub node_id: String,
    pub shard_id: Option<u32>,
}

impl ParticipantId {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            shard_id: None,
        }
    }

    pub fn with_shard(node_id: &str, shard_id: u32) -> Self {
        Self {
            node_id: node_id.to_string(),
            shard_id: Some(shard_id),
        }
    }
}

/// Transaction state
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    /// Transaction started, not yet prepared
    Active,
    /// Preparing phase initiated
    Preparing,
    /// All participants voted to prepare
    Prepared,
    /// Committing phase initiated
    Committing,
    /// Transaction committed
    Committed,
    /// Rolling back
    Aborting,
    /// Transaction aborted
    Aborted,
    /// Unknown state (after recovery)
    Unknown,
    /// Heuristic completion
    HeuristicCommit,
    HeuristicRollback,
    HeuristicMixed,
}

/// Participant vote
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    /// Ready to commit
    Prepared,
    /// Cannot commit, must abort
    Abort(String),
    /// Read-only, no changes to commit
    ReadOnly,
}

/// Heuristic outcome
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeuristicOutcome {
    Commit,
    Rollback,
    Mixed,
}

// ============================================================================
// Protocol Messages
// ============================================================================

/// Messages from coordinator to participants
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CoordinatorMessage {
    /// Start a new distributed transaction
    Begin {
        gtid: GlobalTransactionId,
        timeout_ms: u64,
    },
    /// Request prepare vote
    Prepare {
        gtid: GlobalTransactionId,
    },
    /// Commit the transaction
    Commit {
        gtid: GlobalTransactionId,
    },
    /// Rollback the transaction
    Rollback {
        gtid: GlobalTransactionId,
    },
    /// Request transaction status (for recovery)
    QueryStatus {
        gtid: GlobalTransactionId,
    },
    /// Forget transaction (after heuristic completion)
    Forget {
        gtid: GlobalTransactionId,
    },
}

/// Messages from participants to coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ParticipantMessage {
    /// Transaction started
    BeginAck {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
    },
    /// Vote on prepare
    VoteResult {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
        vote: Vote,
    },
    /// Commit acknowledgment
    CommitAck {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
    },
    /// Rollback acknowledgment
    RollbackAck {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
    },
    /// Status response
    StatusResponse {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
        state: TransactionState,
    },
    /// Error response
    Error {
        gtid: GlobalTransactionId,
        participant: ParticipantId,
        error: String,
    },
}

// ============================================================================
// Coordinator
// ============================================================================

/// 2PC Coordinator configuration
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Node ID for this coordinator
    pub node_id: String,
    /// Default transaction timeout
    pub default_timeout: Duration,
    /// Prepare phase timeout
    pub prepare_timeout: Duration,
    /// Commit/rollback phase timeout
    pub completion_timeout: Duration,
    /// Recovery check interval
    pub recovery_interval: Duration,
    /// Maximum concurrent transactions
    pub max_transactions: usize,
    /// Enable presumed abort optimization
    pub presumed_abort: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            node_id: "coordinator-1".to_string(),
            default_timeout: Duration::from_secs(60),
            prepare_timeout: Duration::from_secs(30),
            completion_timeout: Duration::from_secs(30),
            recovery_interval: Duration::from_secs(10),
            max_transactions: 10000,
            presumed_abort: true,
        }
    }
}

/// Transaction record maintained by coordinator
#[derive(Clone, Debug)]
pub struct TransactionRecord {
    pub gtid: GlobalTransactionId,
    pub state: TransactionState,
    pub participants: HashMap<ParticipantId, ParticipantState>,
    pub created_at: Instant,
    pub timeout: Duration,
    pub last_activity: Instant,
}

/// Participant state from coordinator's view
#[derive(Clone, Debug)]
pub struct ParticipantState {
    pub id: ParticipantId,
    pub state: ParticipantPhaseState,
    pub vote: Option<Vote>,
    pub last_contact: Instant,
    pub retry_count: u32,
}

/// Participant phase state
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantPhaseState {
    Registered,
    PrepareSent,
    Prepared,
    CommitSent,
    Committed,
    RollbackSent,
    Aborted,
    Failed,
}

/// 2PC Coordinator
pub struct Coordinator {
    config: CoordinatorConfig,
    transactions: RwLock<HashMap<GlobalTransactionId, TransactionRecord>>,
    sequence: AtomicU64,
    /// Persistent transaction log
    transaction_log: RwLock<Vec<TransactionLogEntry>>,
    /// Statistics
    stats: RwLock<CoordinatorStats>,
}

/// Transaction log entry for recovery
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionLogEntry {
    pub gtid: GlobalTransactionId,
    pub action: LogAction,
    pub timestamp: u64,
    pub participants: Vec<ParticipantId>,
}

/// Log action types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogAction {
    Begin,
    Prepare,
    Commit,
    Abort,
    Complete,
}

/// Coordinator statistics
#[derive(Clone, Debug, Default)]
pub struct CoordinatorStats {
    pub total_transactions: u64,
    pub committed_transactions: u64,
    pub aborted_transactions: u64,
    pub in_progress_transactions: u64,
    pub prepare_timeouts: u64,
    pub commit_timeouts: u64,
    pub heuristic_decisions: u64,
}

impl Coordinator {
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            config,
            transactions: RwLock::new(HashMap::new()),
            sequence: AtomicU64::new(0),
            transaction_log: RwLock::new(Vec::new()),
            stats: RwLock::new(CoordinatorStats::default()),
        }
    }

    /// Begin a new distributed transaction
    pub fn begin(&self, participants: Vec<ParticipantId>) -> Result<GlobalTransactionId, TwoPhaseCommitError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let gtid = GlobalTransactionId::new(&self.config.node_id, seq);

        let mut txns = self.transactions.write();

        if txns.len() >= self.config.max_transactions {
            return Err(TwoPhaseCommitError::CoordinatorFailure(
                "max transactions exceeded".into()
            ));
        }

        let now = Instant::now();
        let mut participant_states = HashMap::new();

        for p in &participants {
            participant_states.insert(p.clone(), ParticipantState {
                id: p.clone(),
                state: ParticipantPhaseState::Registered,
                vote: None,
                last_contact: now,
                retry_count: 0,
            });
        }

        let record = TransactionRecord {
            gtid: gtid.clone(),
            state: TransactionState::Active,
            participants: participant_states,
            created_at: now,
            timeout: self.config.default_timeout,
            last_activity: now,
        };

        txns.insert(gtid.clone(), record);

        // Log begin
        self.log_action(&gtid, LogAction::Begin, &participants);

        // Update stats
        self.stats.write().total_transactions += 1;
        self.stats.write().in_progress_transactions += 1;

        Ok(gtid)
    }

    /// Prepare phase - request all participants to prepare
    pub fn prepare(&self, gtid: &GlobalTransactionId) -> Result<bool, TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state != TransactionState::Active {
            return Err(TwoPhaseCommitError::InvalidStateTransition {
                from: record.state.clone(),
                to: TransactionState::Preparing,
            });
        }

        record.state = TransactionState::Preparing;
        record.last_activity = Instant::now();

        // Mark all participants as prepare sent
        for (_, pstate) in record.participants.iter_mut() {
            pstate.state = ParticipantPhaseState::PrepareSent;
        }

        // Log prepare
        let participants: Vec<_> = record.participants.keys().cloned().collect();
        drop(txns);
        self.log_action(gtid, LogAction::Prepare, &participants);

        Ok(true)
    }

    /// Receive vote from a participant
    pub fn receive_vote(
        &self,
        gtid: &GlobalTransactionId,
        participant: &ParticipantId,
        vote: Vote,
    ) -> Result<Option<TransactionState>, TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        let pstate = record.participants.get_mut(participant)
            .ok_or_else(|| TwoPhaseCommitError::ParticipantNotFound(participant.clone()))?;

        pstate.vote = Some(vote.clone());
        pstate.last_contact = Instant::now();

        match vote {
            Vote::Prepared => {
                pstate.state = ParticipantPhaseState::Prepared;
            }
            Vote::Abort(_) => {
                pstate.state = ParticipantPhaseState::Aborted;
                // One abort means global abort
                record.state = TransactionState::Aborting;
                return Ok(Some(TransactionState::Aborting));
            }
            Vote::ReadOnly => {
                pstate.state = ParticipantPhaseState::Committed;
            }
        }

        // Check if all participants have voted
        let all_voted = record.participants.values()
            .all(|p| p.vote.is_some());

        if all_voted {
            let all_prepared = record.participants.values()
                .all(|p| matches!(p.vote, Some(Vote::Prepared) | Some(Vote::ReadOnly)));

            if all_prepared {
                record.state = TransactionState::Prepared;
                return Ok(Some(TransactionState::Prepared));
            } else {
                record.state = TransactionState::Aborting;
                return Ok(Some(TransactionState::Aborting));
            }
        }

        Ok(None)
    }

    /// Commit phase - tell all participants to commit
    pub fn commit(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state != TransactionState::Prepared {
            return Err(TwoPhaseCommitError::InvalidStateTransition {
                from: record.state.clone(),
                to: TransactionState::Committing,
            });
        }

        record.state = TransactionState::Committing;
        record.last_activity = Instant::now();

        // Mark all participants as commit sent
        for (_, pstate) in record.participants.iter_mut() {
            if pstate.state == ParticipantPhaseState::Prepared {
                pstate.state = ParticipantPhaseState::CommitSent;
            }
        }

        // Log commit decision (critical - must be durable before sending commit)
        let participants: Vec<_> = record.participants.keys().cloned().collect();
        drop(txns);
        self.log_action(gtid, LogAction::Commit, &participants);

        Ok(())
    }

    /// Receive commit acknowledgment from participant
    pub fn receive_commit_ack(
        &self,
        gtid: &GlobalTransactionId,
        participant: &ParticipantId,
    ) -> Result<Option<TransactionState>, TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        let pstate = record.participants.get_mut(participant)
            .ok_or_else(|| TwoPhaseCommitError::ParticipantNotFound(participant.clone()))?;

        pstate.state = ParticipantPhaseState::Committed;
        pstate.last_contact = Instant::now();

        // Check if all participants have committed
        let all_committed = record.participants.values()
            .all(|p| p.state == ParticipantPhaseState::Committed);

        if all_committed {
            record.state = TransactionState::Committed;

            // Update stats
            drop(txns);
            let mut stats = self.stats.write();
            stats.committed_transactions += 1;
            stats.in_progress_transactions = stats.in_progress_transactions.saturating_sub(1);

            return Ok(Some(TransactionState::Committed));
        }

        Ok(None)
    }

    /// Rollback a transaction
    pub fn rollback(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state == TransactionState::Committed {
            return Err(TwoPhaseCommitError::InvalidStateTransition {
                from: record.state.clone(),
                to: TransactionState::Aborting,
            });
        }

        record.state = TransactionState::Aborting;
        record.last_activity = Instant::now();

        // Mark all participants as rollback sent
        for (_, pstate) in record.participants.iter_mut() {
            if pstate.state != ParticipantPhaseState::Aborted {
                pstate.state = ParticipantPhaseState::RollbackSent;
            }
        }

        // Log abort decision
        let participants: Vec<_> = record.participants.keys().cloned().collect();
        drop(txns);
        self.log_action(gtid, LogAction::Abort, &participants);

        Ok(())
    }

    /// Receive rollback acknowledgment from participant
    pub fn receive_rollback_ack(
        &self,
        gtid: &GlobalTransactionId,
        participant: &ParticipantId,
    ) -> Result<Option<TransactionState>, TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        let pstate = record.participants.get_mut(participant)
            .ok_or_else(|| TwoPhaseCommitError::ParticipantNotFound(participant.clone()))?;

        pstate.state = ParticipantPhaseState::Aborted;
        pstate.last_contact = Instant::now();

        // Check if all participants have aborted
        let all_aborted = record.participants.values()
            .all(|p| p.state == ParticipantPhaseState::Aborted);

        if all_aborted {
            record.state = TransactionState::Aborted;

            // Update stats
            drop(txns);
            let mut stats = self.stats.write();
            stats.aborted_transactions += 1;
            stats.in_progress_transactions = stats.in_progress_transactions.saturating_sub(1);

            return Ok(Some(TransactionState::Aborted));
        }

        Ok(None)
    }

    /// Get transaction state
    pub fn get_state(&self, gtid: &GlobalTransactionId) -> Option<TransactionState> {
        let txns = self.transactions.read();
        txns.get(gtid).map(|r| r.state.clone())
    }

    /// Check for timed-out transactions
    pub fn check_timeouts(&self) -> Vec<GlobalTransactionId> {
        let txns = self.transactions.read();
        let mut timed_out = Vec::new();

        for (gtid, record) in txns.iter() {
            if record.created_at.elapsed() > record.timeout {
                if !matches!(record.state, TransactionState::Committed | TransactionState::Aborted) {
                    timed_out.push(gtid.clone());
                }
            }
        }

        timed_out
    }

    /// Make heuristic decision for stuck transaction
    pub fn heuristic_commit(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        record.state = TransactionState::HeuristicCommit;

        self.stats.write().heuristic_decisions += 1;

        Ok(())
    }

    /// Make heuristic rollback decision
    pub fn heuristic_rollback(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        record.state = TransactionState::HeuristicRollback;

        self.stats.write().heuristic_decisions += 1;

        Ok(())
    }

    /// Forget a completed transaction
    pub fn forget(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        txns.remove(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        self.log_action(gtid, LogAction::Complete, &[]);
        Ok(())
    }

    /// Recover transactions from log
    pub fn recover(&self) -> Result<Vec<GlobalTransactionId>, TwoPhaseCommitError> {
        let log = self.transaction_log.read();
        let mut in_doubt: HashMap<GlobalTransactionId, LogAction> = HashMap::new();

        // Replay log to find incomplete transactions
        for entry in log.iter() {
            match entry.action {
                LogAction::Begin => {
                    in_doubt.insert(entry.gtid.clone(), LogAction::Begin);
                }
                LogAction::Prepare => {
                    in_doubt.insert(entry.gtid.clone(), LogAction::Prepare);
                }
                LogAction::Commit => {
                    in_doubt.insert(entry.gtid.clone(), LogAction::Commit);
                }
                LogAction::Abort => {
                    in_doubt.insert(entry.gtid.clone(), LogAction::Abort);
                }
                LogAction::Complete => {
                    in_doubt.remove(&entry.gtid);
                }
            }
        }

        // Process in-doubt transactions
        let mut recovered = Vec::new();
        for (gtid, last_action) in in_doubt {
            match last_action {
                LogAction::Begin | LogAction::Prepare => {
                    // Presumed abort - rollback
                    if self.config.presumed_abort {
                        // Transaction aborted
                    }
                }
                LogAction::Commit => {
                    // Must re-send commit to all participants
                    recovered.push(gtid);
                }
                LogAction::Abort => {
                    // Must re-send abort to all participants
                    recovered.push(gtid);
                }
                _ => {}
            }
        }

        Ok(recovered)
    }

    fn log_action(&self, gtid: &GlobalTransactionId, action: LogAction, participants: &[ParticipantId]) {
        let entry = TransactionLogEntry {
            gtid: gtid.clone(),
            action,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            participants: participants.to_vec(),
        };

        self.transaction_log.write().push(entry);
    }

    /// Get coordinator statistics
    pub fn stats(&self) -> CoordinatorStats {
        self.stats.read().clone()
    }
}

// ============================================================================
// Participant
// ============================================================================

/// 2PC Participant configuration
#[derive(Clone, Debug)]
pub struct ParticipantConfig {
    /// Node ID
    pub node_id: String,
    /// Shard ID (if sharded)
    pub shard_id: Option<u32>,
    /// Prepare timeout
    pub prepare_timeout: Duration,
    /// Uncertainty period (for recovery)
    pub uncertainty_period: Duration,
}

impl Default for ParticipantConfig {
    fn default() -> Self {
        Self {
            node_id: "participant-1".to_string(),
            shard_id: None,
            prepare_timeout: Duration::from_secs(30),
            uncertainty_period: Duration::from_secs(60),
        }
    }
}

/// Local transaction record at participant
#[derive(Clone, Debug)]
pub struct LocalTransactionRecord {
    pub gtid: GlobalTransactionId,
    pub state: ParticipantTransactionState,
    pub prepared_at: Option<Instant>,
    pub redo_log: Vec<RedoLogEntry>,
    pub undo_log: Vec<UndoLogEntry>,
}

/// Participant transaction state
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantTransactionState {
    Active,
    Prepared,
    Committed,
    Aborted,
    InDoubt,
}

/// Redo log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedoLogEntry {
    pub sequence: u64,
    pub operation: String,
    pub data: Vec<u8>,
}

/// Undo log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UndoLogEntry {
    pub sequence: u64,
    pub operation: String,
    pub data: Vec<u8>,
}

/// 2PC Participant
pub struct Participant {
    config: ParticipantConfig,
    transactions: RwLock<HashMap<GlobalTransactionId, LocalTransactionRecord>>,
    /// Prepared transaction log (must be durable)
    prepare_log: RwLock<Vec<PrepareLogEntry>>,
}

/// Prepare log entry for recovery
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareLogEntry {
    pub gtid: GlobalTransactionId,
    pub coordinator_id: String,
    pub prepared_at: u64,
    pub redo_data: Vec<u8>,
}

impl Participant {
    pub fn new(config: ParticipantConfig) -> Self {
        Self {
            config,
            transactions: RwLock::new(HashMap::new()),
            prepare_log: RwLock::new(Vec::new()),
        }
    }

    /// Get participant ID
    pub fn id(&self) -> ParticipantId {
        ParticipantId {
            node_id: self.config.node_id.clone(),
            shard_id: self.config.shard_id,
        }
    }

    /// Start participating in a distributed transaction
    pub fn begin(&self, gtid: GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();

        if txns.contains_key(&gtid) {
            return Err(TwoPhaseCommitError::TransactionExists(gtid));
        }

        txns.insert(gtid.clone(), LocalTransactionRecord {
            gtid,
            state: ParticipantTransactionState::Active,
            prepared_at: None,
            redo_log: Vec::new(),
            undo_log: Vec::new(),
        });

        Ok(())
    }

    /// Add operation to transaction
    pub fn add_operation(
        &self,
        gtid: &GlobalTransactionId,
        operation: &str,
        redo_data: Vec<u8>,
        undo_data: Vec<u8>,
    ) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state != ParticipantTransactionState::Active {
            return Err(TwoPhaseCommitError::InvalidStateTransition {
                from: TransactionState::Active, // Simplified
                to: TransactionState::Active,
            });
        }

        let seq = record.redo_log.len() as u64;

        record.redo_log.push(RedoLogEntry {
            sequence: seq,
            operation: operation.to_string(),
            data: redo_data,
        });

        record.undo_log.push(UndoLogEntry {
            sequence: seq,
            operation: operation.to_string(),
            data: undo_data,
        });

        Ok(())
    }

    /// Prepare to commit (vote)
    pub fn prepare(&self, gtid: &GlobalTransactionId) -> Result<Vote, TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state != ParticipantTransactionState::Active {
            return Ok(Vote::Abort("transaction not active".to_string()));
        }

        // Check if read-only
        if record.redo_log.is_empty() {
            record.state = ParticipantTransactionState::Committed;
            return Ok(Vote::ReadOnly);
        }

        // Flush redo log to disk (critical for recovery)
        self.flush_redo_log(record)?;

        // Write prepare record
        let prepare_entry = PrepareLogEntry {
            gtid: gtid.clone(),
            coordinator_id: gtid.coordinator_id.clone(),
            prepared_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            redo_data: Vec::new(), // Would serialize redo log
        };

        self.prepare_log.write().push(prepare_entry);

        record.state = ParticipantTransactionState::Prepared;
        record.prepared_at = Some(Instant::now());

        Ok(Vote::Prepared)
    }

    /// Commit the transaction
    pub fn commit(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        if record.state != ParticipantTransactionState::Prepared {
            return Err(TwoPhaseCommitError::InvalidStateTransition {
                from: TransactionState::Prepared, // Simplified
                to: TransactionState::Committing,
            });
        }

        // Apply changes (redo log already flushed)
        // In real implementation, this would apply the actual changes

        record.state = ParticipantTransactionState::Committed;

        // Clean up undo log
        record.undo_log.clear();

        // Remove from prepare log
        self.prepare_log.write()
            .retain(|e| e.gtid != *gtid);

        Ok(())
    }

    /// Rollback the transaction
    pub fn rollback(&self, gtid: &GlobalTransactionId) -> Result<(), TwoPhaseCommitError> {
        let mut txns = self.transactions.write();
        let record = txns.get_mut(gtid)
            .ok_or_else(|| TwoPhaseCommitError::TransactionNotFound(gtid.clone()))?;

        // Apply undo log in reverse order
        for undo_entry in record.undo_log.iter().rev() {
            // Would apply undo operation
            let _ = undo_entry;
        }

        record.state = ParticipantTransactionState::Aborted;

        // Clean up
        record.redo_log.clear();
        record.undo_log.clear();

        // Remove from prepare log
        self.prepare_log.write()
            .retain(|e| e.gtid != *gtid);

        Ok(())
    }

    /// Get transaction state
    pub fn get_state(&self, gtid: &GlobalTransactionId) -> Option<ParticipantTransactionState> {
        let txns = self.transactions.read();
        txns.get(gtid).map(|r| r.state.clone())
    }

    /// Find in-doubt transactions for recovery
    pub fn get_in_doubt_transactions(&self) -> Vec<GlobalTransactionId> {
        let prepare_log = self.prepare_log.read();
        prepare_log.iter()
            .map(|e| e.gtid.clone())
            .collect()
    }

    /// Recovery: query coordinator for transaction status
    pub fn recover_transaction(&self, gtid: &GlobalTransactionId, coordinator_decision: TransactionState) -> Result<(), TwoPhaseCommitError> {
        match coordinator_decision {
            TransactionState::Committed => self.commit(gtid),
            TransactionState::Aborted => self.rollback(gtid),
            _ => Err(TwoPhaseCommitError::RecoveryError(
                "unknown coordinator decision".into()
            )),
        }
    }

    fn flush_redo_log(&self, _record: &LocalTransactionRecord) -> Result<(), TwoPhaseCommitError> {
        // Would sync redo log to disk
        Ok(())
    }

    /// Clean up completed transactions
    pub fn cleanup(&self, max_age: Duration) {
        let mut txns = self.transactions.write();
        let now = Instant::now();

        txns.retain(|_, record| {
            match record.state {
                ParticipantTransactionState::Committed |
                ParticipantTransactionState::Aborted => {
                    if let Some(prepared_at) = record.prepared_at {
                        now.duration_since(prepared_at) < max_age
                    } else {
                        true
                    }
                }
                _ => true,
            }
        });
    }
}

// ============================================================================
// Distributed Transaction Manager
// ============================================================================

/// High-level distributed transaction manager
pub struct DistributedTransactionManager {
    coordinator: Arc<Coordinator>,
    local_participant: Arc<Participant>,
    remote_participants: RwLock<HashMap<ParticipantId, RemoteParticipant>>,
}

/// Remote participant stub
pub struct RemoteParticipant {
    pub id: ParticipantId,
    pub address: String,
    pub connected: bool,
}

impl DistributedTransactionManager {
    pub fn new(coordinator: Arc<Coordinator>, local_participant: Arc<Participant>) -> Self {
        Self {
            coordinator,
            local_participant,
            remote_participants: RwLock::new(HashMap::new()),
        }
    }

    /// Register a remote participant
    pub fn register_participant(&self, id: ParticipantId, address: String) {
        self.remote_participants.write().insert(
            id.clone(),
            RemoteParticipant {
                id,
                address,
                connected: false,
            },
        );
    }

    /// Execute a distributed transaction
    pub fn execute_distributed<F, R>(
        &self,
        participants: Vec<ParticipantId>,
        operation: F,
    ) -> Result<R, TwoPhaseCommitError>
    where
        F: FnOnce(&GlobalTransactionId) -> Result<R, TwoPhaseCommitError>,
    {
        // Phase 0: Begin transaction
        let gtid = self.coordinator.begin(participants.clone())?;

        // Execute user operation
        let result = match operation(&gtid) {
            Ok(r) => r,
            Err(e) => {
                // Rollback on error
                let _ = self.coordinator.rollback(&gtid);
                return Err(e);
            }
        };

        // Phase 1: Prepare
        self.coordinator.prepare(&gtid)?;

        // Collect votes from all participants
        let mut all_prepared = true;
        for pid in &participants {
            if *pid == self.local_participant.id() {
                // Local participant
                match self.local_participant.prepare(&gtid)? {
                    Vote::Prepared | Vote::ReadOnly => {
                        self.coordinator.receive_vote(&gtid, pid, Vote::Prepared)?;
                    }
                    Vote::Abort(reason) => {
                        self.coordinator.receive_vote(&gtid, pid, Vote::Abort(reason))?;
                        all_prepared = false;
                        break;
                    }
                }
            } else {
                // Would send prepare message to remote participant
                // For now, assume prepared
                self.coordinator.receive_vote(&gtid, pid, Vote::Prepared)?;
            }
        }

        // Phase 2: Commit or Rollback
        if all_prepared {
            self.coordinator.commit(&gtid)?;

            // Commit all participants
            for pid in &participants {
                if *pid == self.local_participant.id() {
                    self.local_participant.commit(&gtid)?;
                }
                // Would send commit to remote participants
                self.coordinator.receive_commit_ack(&gtid, pid)?;
            }
        } else {
            self.coordinator.rollback(&gtid)?;

            // Rollback all participants
            for pid in &participants {
                if *pid == self.local_participant.id() {
                    self.local_participant.rollback(&gtid)?;
                }
                // Would send rollback to remote participants
                self.coordinator.receive_rollback_ack(&gtid, pid)?;
            }

            return Err(TwoPhaseCommitError::PrepareFailed {
                participant: participants[0].clone(),
                reason: "prepare failed".to_string(),
            });
        }

        Ok(result)
    }

    /// Get coordinator statistics
    pub fn stats(&self) -> CoordinatorStats {
        self.coordinator.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_successful_2pc() {
        let coordinator = Coordinator::new(CoordinatorConfig::default());

        let p1 = ParticipantId::new("node-1");
        let p2 = ParticipantId::new("node-2");

        // Begin
        let gtid = coordinator.begin(vec![p1.clone(), p2.clone()]).unwrap();
        assert_eq!(coordinator.get_state(&gtid), Some(TransactionState::Active));

        // Prepare
        coordinator.prepare(&gtid).unwrap();
        assert_eq!(coordinator.get_state(&gtid), Some(TransactionState::Preparing));

        // Receive votes
        coordinator.receive_vote(&gtid, &p1, Vote::Prepared).unwrap();
        let state = coordinator.receive_vote(&gtid, &p2, Vote::Prepared).unwrap();
        assert_eq!(state, Some(TransactionState::Prepared));

        // Commit
        coordinator.commit(&gtid).unwrap();
        assert_eq!(coordinator.get_state(&gtid), Some(TransactionState::Committing));

        // Receive commit acks
        coordinator.receive_commit_ack(&gtid, &p1).unwrap();
        let state = coordinator.receive_commit_ack(&gtid, &p2).unwrap();
        assert_eq!(state, Some(TransactionState::Committed));
    }

    #[test]
    fn test_abort_on_vote_no() {
        let coordinator = Coordinator::new(CoordinatorConfig::default());

        let p1 = ParticipantId::new("node-1");
        let p2 = ParticipantId::new("node-2");

        let gtid = coordinator.begin(vec![p1.clone(), p2.clone()]).unwrap();
        coordinator.prepare(&gtid).unwrap();

        // P1 votes yes, P2 votes no
        coordinator.receive_vote(&gtid, &p1, Vote::Prepared).unwrap();
        let state = coordinator.receive_vote(&gtid, &p2, Vote::Abort("conflict".into())).unwrap();

        assert_eq!(state, Some(TransactionState::Aborting));
    }

    #[test]
    fn test_participant_lifecycle() {
        let participant = Participant::new(ParticipantConfig::default());

        let gtid = GlobalTransactionId::new("coord-1", 1);

        // Begin
        participant.begin(gtid.clone()).unwrap();
        assert_eq!(
            participant.get_state(&gtid),
            Some(ParticipantTransactionState::Active)
        );

        // Add operation
        participant.add_operation(
            &gtid,
            "INSERT",
            b"redo data".to_vec(),
            b"undo data".to_vec(),
        ).unwrap();

        // Prepare
        let vote = participant.prepare(&gtid).unwrap();
        assert_eq!(vote, Vote::Prepared);
        assert_eq!(
            participant.get_state(&gtid),
            Some(ParticipantTransactionState::Prepared)
        );

        // Commit
        participant.commit(&gtid).unwrap();
        assert_eq!(
            participant.get_state(&gtid),
            Some(ParticipantTransactionState::Committed)
        );
    }

    #[test]
    fn test_read_only_optimization() {
        let participant = Participant::new(ParticipantConfig::default());

        let gtid = GlobalTransactionId::new("coord-1", 2);

        participant.begin(gtid.clone()).unwrap();
        // No operations added - read only

        let vote = participant.prepare(&gtid).unwrap();
        assert_eq!(vote, Vote::ReadOnly);
    }

    #[test]
    fn test_distributed_transaction_manager() {
        let coordinator = Arc::new(Coordinator::new(CoordinatorConfig::default()));
        let participant = Arc::new(Participant::new(ParticipantConfig::default()));

        let manager = DistributedTransactionManager::new(coordinator, participant.clone());

        let result = manager.execute_distributed(
            vec![participant.id()],
            |gtid| {
                // Execute some operation
                participant.begin(gtid.clone())?;
                participant.add_operation(
                    gtid,
                    "UPDATE",
                    b"new value".to_vec(),
                    b"old value".to_vec(),
                )?;
                Ok(42)
            },
        );

        assert_eq!(result.unwrap(), 42);
    }
}
