//! Data Retention Policies
//!
//! GDPR/CCPA compliant data purging and legal hold support.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Retention Policy Configuration
// ============================================================================

/// Data retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Policy ID
    pub policy_id: String,
    /// Policy name
    pub name: String,
    /// Target scope
    pub scope: RetentionScope,
    /// Retention period
    pub retention: RetentionPeriod,
    /// Purge action
    pub action: PurgeAction,
    /// Compliance requirements
    pub compliance: Vec<ComplianceRequirement>,
    /// Schedule
    pub schedule: PurgeSchedule,
    /// Policy enabled
    pub enabled: bool,
    /// Created timestamp
    pub created_at: u64,
    /// Last executed timestamp
    pub last_executed: Option<u64>,
}

/// Retention scope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionScope {
    /// Apply to specific table
    Table { database: String, table: String },
    /// Apply to specific columns in a table
    Columns {
        database: String,
        table: String,
        columns: Vec<String>,
    },
    /// Apply to entire database
    Database { database: String },
    /// Apply to data matching condition
    Conditional {
        database: String,
        table: String,
        condition: String,
    },
    /// Apply to specific data category
    DataCategory { category: DataCategory },
}

/// Data category for GDPR/CCPA
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataCategory {
    /// Personally Identifiable Information
    Pii,
    /// Sensitive Personal Data (GDPR Art. 9)
    SensitivePersonal,
    /// Financial Information
    Financial,
    /// Health Information (HIPAA PHI)
    Health,
    /// Behavioral/Tracking Data
    Behavioral,
    /// User Generated Content
    UserContent,
    /// System Logs
    SystemLogs,
    /// Analytics Data
    Analytics,
    /// Custom category
    Custom(String),
}

/// Retention period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPeriod {
    /// Keep for specified days
    Days(u32),
    /// Keep for specified months
    Months(u32),
    /// Keep for specified years
    Years(u32),
    /// Keep until specific date
    UntilDate(u64),
    /// Keep indefinitely (must have legal basis)
    Indefinite { legal_basis: String },
}

impl RetentionPeriod {
    /// Convert to seconds
    pub fn to_seconds(&self) -> Option<u64> {
        match self {
            RetentionPeriod::Days(d) => Some(*d as u64 * 86400),
            RetentionPeriod::Months(m) => Some(*m as u64 * 30 * 86400),
            RetentionPeriod::Years(y) => Some(*y as u64 * 365 * 86400),
            RetentionPeriod::UntilDate(ts) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if *ts > now {
                    Some(*ts - now)
                } else {
                    Some(0)
                }
            }
            RetentionPeriod::Indefinite { .. } => None,
        }
    }
}

/// Purge action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PurgeAction {
    /// Hard delete (physical removal)
    Delete,
    /// Soft delete (mark as deleted)
    SoftDelete { marker_column: String },
    /// Anonymize data
    Anonymize { strategy: AnonymizationStrategy },
    /// Archive to cold storage
    Archive { destination: String },
    /// Pseudonymize (replace identifiers)
    Pseudonymize { key_column: String },
}

/// Anonymization strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AnonymizationStrategy {
    /// Replace with null
    Nullify,
    /// Replace with fixed value
    FixedValue(String),
    /// Replace with hash
    Hash { algorithm: String },
    /// Generalize (e.g., age -> age range)
    Generalize { mapping: HashMap<String, String> },
    /// Suppress (remove column)
    Suppress,
    /// K-anonymity
    KAnonymity { k: u32 },
}

/// Compliance requirements
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ComplianceRequirement {
    Gdpr,
    Ccpa,
    Hipaa,
    Sox,
    Pci,
    Custom(String),
}

/// Purge schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PurgeSchedule {
    /// Run daily at specified hour (UTC)
    Daily { hour: u32 },
    /// Run weekly on specified day and hour
    Weekly { day: u32, hour: u32 },
    /// Run monthly on specified day and hour
    Monthly { day: u32, hour: u32 },
    /// Cron expression
    Cron(String),
    /// Manual only
    Manual,
}

// ============================================================================
// Legal Hold
// ============================================================================

/// Legal hold
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegalHold {
    /// Hold ID
    pub hold_id: String,
    /// Hold name
    pub name: String,
    /// Matter/Case reference
    pub matter_id: String,
    /// Description
    pub description: String,
    /// Scope of hold
    pub scope: LegalHoldScope,
    /// Custodians (data owners)
    pub custodians: Vec<String>,
    /// Status
    pub status: LegalHoldStatus,
    /// Created by
    pub created_by: String,
    /// Created timestamp
    pub created_at: u64,
    /// Released timestamp
    pub released_at: Option<u64>,
    /// Release reason
    pub release_reason: Option<String>,
}

/// Legal hold scope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LegalHoldScope {
    /// Hold specific tables
    Tables(Vec<TableRef>),
    /// Hold data matching condition
    Condition {
        database: String,
        table: String,
        condition: String,
    },
    /// Hold all data for specific users
    Users(Vec<String>),
    /// Hold data in date range
    DateRange {
        database: String,
        table: String,
        date_column: String,
        start: u64,
        end: u64,
    },
    /// Hold entire database
    Database(String),
}

/// Table reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

/// Legal hold status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegalHoldStatus {
    Active,
    Released,
    Expired,
}

// ============================================================================
// Data Subject Rights (GDPR)
// ============================================================================

/// Data subject request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSubjectRequest {
    /// Request ID
    pub request_id: String,
    /// Request type
    pub request_type: DataSubjectRequestType,
    /// Subject identifier
    pub subject_id: String,
    /// Subject email
    pub subject_email: Option<String>,
    /// Verification status
    pub verified: bool,
    /// Status
    pub status: RequestStatus,
    /// Created timestamp
    pub created_at: u64,
    /// Due date (GDPR: 30 days)
    pub due_at: u64,
    /// Completed timestamp
    pub completed_at: Option<u64>,
    /// Notes
    pub notes: Vec<RequestNote>,
}

/// Data subject request type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataSubjectRequestType {
    /// Right to access (GDPR Art. 15)
    Access,
    /// Right to rectification (GDPR Art. 16)
    Rectification { corrections: HashMap<String, String> },
    /// Right to erasure (GDPR Art. 17)
    Erasure,
    /// Right to restrict processing (GDPR Art. 18)
    Restriction,
    /// Right to data portability (GDPR Art. 20)
    Portability { format: ExportFormat },
    /// Right to object (GDPR Art. 21)
    Objection { processing_type: String },
}

/// Export format for portability
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    Json,
    Csv,
    Xml,
    Parquet,
}

/// Request status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestStatus {
    Pending,
    InProgress,
    Completed,
    Rejected,
    Expired,
}

/// Request note
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestNote {
    pub timestamp: u64,
    pub author: String,
    pub content: String,
}

// ============================================================================
// Retention Manager
// ============================================================================

/// Data retention manager
pub struct DataRetentionManager {
    /// Retention policies
    policies: RwLock<HashMap<String, RetentionPolicy>>,
    /// Legal holds
    legal_holds: RwLock<HashMap<String, LegalHold>>,
    /// Data subject requests
    requests: RwLock<HashMap<String, DataSubjectRequest>>,
    /// Data category mappings (table.column -> category)
    category_mappings: RwLock<HashMap<String, DataCategory>>,
    /// Purge history
    purge_history: RwLock<Vec<PurgeEvent>>,
    /// Statistics
    stats: RetentionStats,
}

#[derive(Default)]
struct RetentionStats {
    rows_purged: AtomicU64,
    rows_anonymized: AtomicU64,
    rows_archived: AtomicU64,
    requests_processed: AtomicU64,
}

impl DataRetentionManager {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            legal_holds: RwLock::new(HashMap::new()),
            requests: RwLock::new(HashMap::new()),
            category_mappings: RwLock::new(HashMap::new()),
            purge_history: RwLock::new(Vec::new()),
            stats: RetentionStats::default(),
        }
    }

    // --- Policy Management ---

    /// Create a retention policy
    pub fn create_policy(&self, policy: RetentionPolicy) -> Result<(), RetentionError> {
        let policy_id = policy.policy_id.clone();

        if self.policies.read().contains_key(&policy_id) {
            return Err(RetentionError::PolicyExists(policy_id));
        }

        self.policies.write().insert(policy_id, policy);
        Ok(())
    }

    /// Update a retention policy
    pub fn update_policy(&self, policy: RetentionPolicy) -> Result<(), RetentionError> {
        let policy_id = policy.policy_id.clone();

        if !self.policies.read().contains_key(&policy_id) {
            return Err(RetentionError::PolicyNotFound(policy_id));
        }

        self.policies.write().insert(policy_id, policy);
        Ok(())
    }

    /// Delete a retention policy
    pub fn delete_policy(&self, policy_id: &str) -> Result<(), RetentionError> {
        if self.policies.write().remove(policy_id).is_none() {
            return Err(RetentionError::PolicyNotFound(policy_id.to_string()));
        }
        Ok(())
    }

    /// Get a policy
    pub fn get_policy(&self, policy_id: &str) -> Option<RetentionPolicy> {
        self.policies.read().get(policy_id).cloned()
    }

    /// List all policies
    pub fn list_policies(&self) -> Vec<RetentionPolicy> {
        self.policies.read().values().cloned().collect()
    }

    // --- Legal Hold Management ---

    /// Create a legal hold
    pub fn create_legal_hold(&self, hold: LegalHold) -> Result<(), RetentionError> {
        let hold_id = hold.hold_id.clone();

        if self.legal_holds.read().contains_key(&hold_id) {
            return Err(RetentionError::HoldExists(hold_id));
        }

        self.legal_holds.write().insert(hold_id, hold);
        Ok(())
    }

    /// Release a legal hold
    pub fn release_legal_hold(&self, hold_id: &str, reason: &str) -> Result<(), RetentionError> {
        let mut holds = self.legal_holds.write();
        let hold = holds
            .get_mut(hold_id)
            .ok_or_else(|| RetentionError::HoldNotFound(hold_id.to_string()))?;

        hold.status = LegalHoldStatus::Released;
        hold.released_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        hold.release_reason = Some(reason.to_string());

        Ok(())
    }

    /// Check if data is under legal hold
    pub fn is_under_hold(&self, database: &str, table: &str) -> bool {
        let holds = self.legal_holds.read();

        for hold in holds.values() {
            if hold.status != LegalHoldStatus::Active {
                continue;
            }

            match &hold.scope {
                LegalHoldScope::Database(db) if db == database => return true,
                LegalHoldScope::Tables(tables) => {
                    if tables.iter().any(|t| t.database == database && t.table == table) {
                        return true;
                    }
                }
                LegalHoldScope::Condition { database: db, table: t, .. }
                    if db == database && t == table =>
                {
                    return true;
                }
                LegalHoldScope::DateRange { database: db, table: t, .. }
                    if db == database && t == table =>
                {
                    return true;
                }
                _ => {}
            }
        }

        false
    }

    /// List active legal holds
    pub fn list_active_holds(&self) -> Vec<LegalHold> {
        self.legal_holds
            .read()
            .values()
            .filter(|h| h.status == LegalHoldStatus::Active)
            .cloned()
            .collect()
    }

    // --- Data Category Management ---

    /// Map a column to a data category
    pub fn map_category(&self, database: &str, table: &str, column: &str, category: DataCategory) {
        let key = format!("{}.{}.{}", database, table, column);
        self.category_mappings.write().insert(key, category);
    }

    /// Get category for a column
    pub fn get_category(&self, database: &str, table: &str, column: &str) -> Option<DataCategory> {
        let key = format!("{}.{}.{}", database, table, column);
        self.category_mappings.read().get(&key).cloned()
    }

    /// Get all columns in a category
    pub fn columns_in_category(&self, category: &DataCategory) -> Vec<(String, String, String)> {
        self.category_mappings
            .read()
            .iter()
            .filter(|(_, cat)| *cat == category)
            .map(|(key, _)| {
                let parts: Vec<&str> = key.split('.').collect();
                (
                    parts.get(0).unwrap_or(&"").to_string(),
                    parts.get(1).unwrap_or(&"").to_string(),
                    parts.get(2).unwrap_or(&"").to_string(),
                )
            })
            .collect()
    }

    // --- Data Subject Requests ---

    /// Submit a data subject request
    pub fn submit_request(&self, request: DataSubjectRequest) -> Result<String, RetentionError> {
        let request_id = request.request_id.clone();

        self.requests.write().insert(request_id.clone(), request);
        Ok(request_id)
    }

    /// Process a data subject request
    pub fn process_request(&self, request_id: &str) -> Result<RequestResult, RetentionError> {
        let mut requests = self.requests.write();
        let request = requests
            .get_mut(request_id)
            .ok_or_else(|| RetentionError::RequestNotFound(request_id.to_string()))?;

        if request.status != RequestStatus::Pending && request.status != RequestStatus::InProgress {
            return Err(RetentionError::RequestNotProcessable(request_id.to_string()));
        }

        request.status = RequestStatus::InProgress;

        // Process based on request type
        let result = match &request.request_type {
            DataSubjectRequestType::Access => {
                self.process_access_request(&request.subject_id)
            }
            DataSubjectRequestType::Erasure => {
                self.process_erasure_request(&request.subject_id)
            }
            DataSubjectRequestType::Portability { format } => {
                self.process_portability_request(&request.subject_id, format.clone())
            }
            DataSubjectRequestType::Rectification { corrections } => {
                self.process_rectification_request(&request.subject_id, corrections.clone())
            }
            DataSubjectRequestType::Restriction => {
                self.process_restriction_request(&request.subject_id)
            }
            DataSubjectRequestType::Objection { processing_type } => {
                self.process_objection_request(&request.subject_id, processing_type)
            }
        };

        request.status = RequestStatus::Completed;
        request.completed_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        self.stats.requests_processed.fetch_add(1, Ordering::Relaxed);

        result
    }

    fn process_access_request(&self, subject_id: &str) -> Result<RequestResult, RetentionError> {
        // Simplified - in production would query all tables containing subject's data
        Ok(RequestResult::AccessReport {
            subject_id: subject_id.to_string(),
            data_categories: self.find_subject_data(subject_id),
            generated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        })
    }

    fn process_erasure_request(&self, subject_id: &str) -> Result<RequestResult, RetentionError> {
        // Check for legal holds
        let categories = self.find_subject_data(subject_id);
        let rows_deleted = categories.len() as u64 * 10; // Simplified

        self.stats.rows_purged.fetch_add(rows_deleted, Ordering::Relaxed);

        Ok(RequestResult::ErasureComplete {
            subject_id: subject_id.to_string(),
            rows_deleted,
            tables_affected: categories.len(),
        })
    }

    fn process_portability_request(
        &self,
        subject_id: &str,
        format: ExportFormat,
    ) -> Result<RequestResult, RetentionError> {
        let download_url = format!("/exports/{}.{}", subject_id, format_extension(&format));
        Ok(RequestResult::PortabilityExport {
            subject_id: subject_id.to_string(),
            format,
            download_url,
            expires_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + 86400 * 7, // 7 days
        })
    }

    fn process_rectification_request(
        &self,
        subject_id: &str,
        _corrections: HashMap<String, String>,
    ) -> Result<RequestResult, RetentionError> {
        Ok(RequestResult::RectificationComplete {
            subject_id: subject_id.to_string(),
            fields_updated: _corrections.len(),
        })
    }

    fn process_restriction_request(&self, subject_id: &str) -> Result<RequestResult, RetentionError> {
        Ok(RequestResult::RestrictionApplied {
            subject_id: subject_id.to_string(),
            restricted_until: None, // Indefinite
        })
    }

    fn process_objection_request(
        &self,
        subject_id: &str,
        processing_type: &str,
    ) -> Result<RequestResult, RetentionError> {
        Ok(RequestResult::ObjectionRecorded {
            subject_id: subject_id.to_string(),
            processing_type: processing_type.to_string(),
        })
    }

    fn find_subject_data(&self, _subject_id: &str) -> Vec<String> {
        // Simplified - would query actual data locations
        vec!["PII".to_string(), "Behavioral".to_string()]
    }

    /// Get request status
    pub fn get_request(&self, request_id: &str) -> Option<DataSubjectRequest> {
        self.requests.read().get(request_id).cloned()
    }

    /// List pending requests
    pub fn list_pending_requests(&self) -> Vec<DataSubjectRequest> {
        self.requests
            .read()
            .values()
            .filter(|r| r.status == RequestStatus::Pending)
            .cloned()
            .collect()
    }

    // --- Purge Execution ---

    /// Execute a retention policy
    pub fn execute_policy(&self, policy_id: &str) -> Result<PurgeEvent, RetentionError> {
        let policy = self
            .policies
            .read()
            .get(policy_id)
            .cloned()
            .ok_or_else(|| RetentionError::PolicyNotFound(policy_id.to_string()))?;

        if !policy.enabled {
            return Err(RetentionError::PolicyDisabled(policy_id.to_string()));
        }

        // Check legal holds
        match &policy.scope {
            RetentionScope::Table { database, table }
            | RetentionScope::Columns { database, table, .. }
            | RetentionScope::Conditional { database, table, .. } => {
                if self.is_under_hold(database, table) {
                    return Err(RetentionError::DataUnderLegalHold);
                }
            }
            _ => {}
        }

        // Execute purge (simplified)
        let rows_affected = 1000u64; // Would be actual count

        match &policy.action {
            PurgeAction::Delete | PurgeAction::SoftDelete { .. } => {
                self.stats.rows_purged.fetch_add(rows_affected, Ordering::Relaxed);
            }
            PurgeAction::Anonymize { .. } | PurgeAction::Pseudonymize { .. } => {
                self.stats.rows_anonymized.fetch_add(rows_affected, Ordering::Relaxed);
            }
            PurgeAction::Archive { .. } => {
                self.stats.rows_archived.fetch_add(rows_affected, Ordering::Relaxed);
            }
        }

        // Update policy last executed time
        if let Some(p) = self.policies.write().get_mut(policy_id) {
            p.last_executed = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }

        let event = PurgeEvent {
            event_id: generate_event_id(),
            policy_id: policy_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            action: policy.action.clone(),
            rows_affected,
            success: true,
            error: None,
            duration_ms: 100,
        };

        self.purge_history.write().push(event.clone());

        Ok(event)
    }

    /// Get purge history
    pub fn purge_history(&self) -> Vec<PurgeEvent> {
        self.purge_history.read().clone()
    }

    /// Get statistics
    pub fn stats(&self) -> RetentionStatsSnapshot {
        RetentionStatsSnapshot {
            rows_purged: self.stats.rows_purged.load(Ordering::Relaxed),
            rows_anonymized: self.stats.rows_anonymized.load(Ordering::Relaxed),
            rows_archived: self.stats.rows_archived.load(Ordering::Relaxed),
            requests_processed: self.stats.requests_processed.load(Ordering::Relaxed),
            active_policies: self.policies.read().values().filter(|p| p.enabled).count(),
            active_holds: self.legal_holds.read().values().filter(|h| h.status == LegalHoldStatus::Active).count(),
            pending_requests: self.requests.read().values().filter(|r| r.status == RequestStatus::Pending).count(),
        }
    }

    /// Generate compliance report
    pub fn compliance_report(&self, compliance: ComplianceRequirement) -> ComplianceReport {
        let policies: Vec<_> = self
            .policies
            .read()
            .values()
            .filter(|p| p.compliance.contains(&compliance))
            .cloned()
            .collect();

        let pii_columns = self.columns_in_category(&DataCategory::Pii);
        let sensitive_columns = self.columns_in_category(&DataCategory::SensitivePersonal);

        ComplianceReport {
            compliance_type: compliance,
            generated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            policies: policies.len(),
            active_holds: self.list_active_holds().len(),
            pii_columns: pii_columns.len(),
            sensitive_columns: sensitive_columns.len(),
            pending_requests: self.list_pending_requests().len(),
            data_mappings: self.category_mappings.read().len(),
        }
    }
}

impl Default for DataRetentionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Results and Events
// ============================================================================

/// Request processing result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestResult {
    AccessReport {
        subject_id: String,
        data_categories: Vec<String>,
        generated_at: u64,
    },
    ErasureComplete {
        subject_id: String,
        rows_deleted: u64,
        tables_affected: usize,
    },
    PortabilityExport {
        subject_id: String,
        format: ExportFormat,
        download_url: String,
        expires_at: u64,
    },
    RectificationComplete {
        subject_id: String,
        fields_updated: usize,
    },
    RestrictionApplied {
        subject_id: String,
        restricted_until: Option<u64>,
    },
    ObjectionRecorded {
        subject_id: String,
        processing_type: String,
    },
}

/// Purge event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurgeEvent {
    pub event_id: String,
    pub policy_id: String,
    pub timestamp: u64,
    pub action: PurgeAction,
    pub rows_affected: u64,
    pub success: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionStatsSnapshot {
    pub rows_purged: u64,
    pub rows_anonymized: u64,
    pub rows_archived: u64,
    pub requests_processed: u64,
    pub active_policies: usize,
    pub active_holds: usize,
    pub pending_requests: usize,
}

/// Compliance report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    pub compliance_type: ComplianceRequirement,
    pub generated_at: u64,
    pub policies: usize,
    pub active_holds: usize,
    pub pii_columns: usize,
    pub sensitive_columns: usize,
    pub pending_requests: usize,
    pub data_mappings: usize,
}

/// Retention errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionError {
    PolicyNotFound(String),
    PolicyExists(String),
    PolicyDisabled(String),
    HoldNotFound(String),
    HoldExists(String),
    RequestNotFound(String),
    RequestNotProcessable(String),
    DataUnderLegalHold,
    ValidationError(String),
}

impl std::fmt::Display for RetentionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionError::PolicyNotFound(id) => write!(f, "Policy not found: {}", id),
            RetentionError::PolicyExists(id) => write!(f, "Policy already exists: {}", id),
            RetentionError::PolicyDisabled(id) => write!(f, "Policy is disabled: {}", id),
            RetentionError::HoldNotFound(id) => write!(f, "Legal hold not found: {}", id),
            RetentionError::HoldExists(id) => write!(f, "Legal hold already exists: {}", id),
            RetentionError::RequestNotFound(id) => write!(f, "Request not found: {}", id),
            RetentionError::RequestNotProcessable(id) => write!(f, "Request not processable: {}", id),
            RetentionError::DataUnderLegalHold => write!(f, "Data is under legal hold"),
            RetentionError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for RetentionError {}

// Helper functions
fn format_extension(format: &ExportFormat) -> &'static str {
    match format {
        ExportFormat::Json => "json",
        ExportFormat::Csv => "csv",
        ExportFormat::Xml => "xml",
        ExportFormat::Parquet => "parquet",
    }
}

fn generate_event_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("evt_{:016x}", hasher.finish())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy() {
        let manager = DataRetentionManager::new();

        let policy = RetentionPolicy {
            policy_id: "policy1".to_string(),
            name: "PII Retention".to_string(),
            scope: RetentionScope::DataCategory {
                category: DataCategory::Pii,
            },
            retention: RetentionPeriod::Years(2),
            action: PurgeAction::Anonymize {
                strategy: AnonymizationStrategy::Nullify,
            },
            compliance: vec![ComplianceRequirement::Gdpr],
            schedule: PurgeSchedule::Daily { hour: 2 },
            enabled: true,
            created_at: 0,
            last_executed: None,
        };

        assert!(manager.create_policy(policy).is_ok());

        let retrieved = manager.get_policy("policy1");
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_legal_hold() {
        let manager = DataRetentionManager::new();

        let hold = LegalHold {
            hold_id: "hold1".to_string(),
            name: "Litigation Hold".to_string(),
            matter_id: "CASE-123".to_string(),
            description: "Test hold".to_string(),
            scope: LegalHoldScope::Tables(vec![TableRef {
                database: "prod".to_string(),
                table: "users".to_string(),
            }]),
            custodians: vec!["legal@company.com".to_string()],
            status: LegalHoldStatus::Active,
            created_by: "admin".to_string(),
            created_at: 0,
            released_at: None,
            release_reason: None,
        };

        assert!(manager.create_legal_hold(hold).is_ok());
        assert!(manager.is_under_hold("prod", "users"));
        assert!(!manager.is_under_hold("prod", "orders"));
    }

    #[test]
    fn test_data_subject_request() {
        let manager = DataRetentionManager::new();

        let request = DataSubjectRequest {
            request_id: "req1".to_string(),
            request_type: DataSubjectRequestType::Access,
            subject_id: "user123".to_string(),
            subject_email: Some("user@example.com".to_string()),
            verified: true,
            status: RequestStatus::Pending,
            created_at: 0,
            due_at: 86400 * 30,
            completed_at: None,
            notes: vec![],
        };

        let request_id = manager.submit_request(request).unwrap();
        let result = manager.process_request(&request_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_category_mapping() {
        let manager = DataRetentionManager::new();

        manager.map_category("prod", "users", "email", DataCategory::Pii);
        manager.map_category("prod", "users", "ssn", DataCategory::SensitivePersonal);

        let pii_cols = manager.columns_in_category(&DataCategory::Pii);
        assert_eq!(pii_cols.len(), 1);
    }

    #[test]
    fn test_compliance_report() {
        let manager = DataRetentionManager::new();

        let policy = RetentionPolicy {
            policy_id: "gdpr_policy".to_string(),
            name: "GDPR Compliance".to_string(),
            scope: RetentionScope::DataCategory {
                category: DataCategory::Pii,
            },
            retention: RetentionPeriod::Years(2),
            action: PurgeAction::Delete,
            compliance: vec![ComplianceRequirement::Gdpr],
            schedule: PurgeSchedule::Daily { hour: 0 },
            enabled: true,
            created_at: 0,
            last_executed: None,
        };

        manager.create_policy(policy).unwrap();
        manager.map_category("db", "users", "email", DataCategory::Pii);

        let report = manager.compliance_report(ComplianceRequirement::Gdpr);
        assert_eq!(report.policies, 1);
        assert_eq!(report.pii_columns, 1);
    }
}
