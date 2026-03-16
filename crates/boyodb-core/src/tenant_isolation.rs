//! Tenant Isolation Enhancements
//!
//! Namespace-level encryption keys and per-tenant backup/restore.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Tenant Configuration
// ============================================================================

/// Tenant identifier
pub type TenantId = String;

/// Tenant configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Unique tenant identifier
    pub tenant_id: TenantId,
    /// Display name
    pub name: String,
    /// Tenant status
    pub status: TenantStatus,
    /// Resource quotas
    pub quotas: TenantQuotas,
    /// Encryption settings
    pub encryption: TenantEncryption,
    /// Backup settings
    pub backup: TenantBackupConfig,
    /// Data residency requirements
    pub data_residency: Option<DataResidency>,
    /// Created timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
}

/// Tenant status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantStatus {
    Active,
    Suspended,
    PendingDeletion,
    Deleted,
}

/// Tenant resource quotas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuotas {
    /// Maximum storage in bytes
    pub max_storage_bytes: u64,
    /// Maximum databases
    pub max_databases: u32,
    /// Maximum tables per database
    pub max_tables_per_db: u32,
    /// Maximum concurrent connections
    pub max_connections: u32,
    /// Maximum queries per second
    pub max_qps: u32,
    /// Maximum CPU cores
    pub max_cpu_cores: f64,
    /// Maximum memory in bytes
    pub max_memory_bytes: u64,
    /// Maximum backup retention days
    pub max_backup_retention_days: u32,
}

impl Default for TenantQuotas {
    fn default() -> Self {
        Self {
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            max_databases: 10,
            max_tables_per_db: 100,
            max_connections: 100,
            max_qps: 1000,
            max_cpu_cores: 4.0,
            max_memory_bytes: 8 * 1024 * 1024 * 1024, // 8GB
            max_backup_retention_days: 30,
        }
    }
}

/// Data residency requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataResidency {
    /// Allowed regions for data storage
    pub allowed_regions: Vec<String>,
    /// Primary region
    pub primary_region: String,
    /// GDPR compliance required
    pub gdpr_compliant: bool,
    /// HIPAA compliance required
    pub hipaa_compliant: bool,
}

// ============================================================================
// Namespace-Level Encryption
// ============================================================================

/// Tenant encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantEncryption {
    /// Is encryption enabled
    pub enabled: bool,
    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,
    /// Key management method
    pub key_management: KeyManagement,
    /// Master key ID (external reference)
    pub master_key_id: Option<String>,
    /// Key rotation interval in days
    pub key_rotation_days: u32,
    /// Last key rotation timestamp
    pub last_rotation: u64,
}

impl Default for TenantEncryption {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_management: KeyManagement::Internal,
            master_key_id: None,
            key_rotation_days: 90,
            last_rotation: 0,
        }
    }
}

/// Encryption algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    Aes256Cbc,
    ChaCha20Poly1305,
}

/// Key management methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyManagement {
    /// Internal key management
    Internal,
    /// AWS KMS
    AwsKms { region: String },
    /// Azure Key Vault
    AzureKeyVault { vault_url: String },
    /// Google Cloud KMS
    GcpKms { project: String, location: String },
    /// HashiCorp Vault
    HashiCorpVault { address: String },
    /// Customer-managed keys
    CustomerManaged { endpoint: String },
}

/// Data encryption key for a namespace
#[derive(Debug, Clone)]
pub struct DataEncryptionKey {
    /// Key ID
    pub key_id: String,
    /// Encrypted key material (encrypted by master key)
    pub encrypted_key: Vec<u8>,
    /// Key version
    pub version: u32,
    /// Creation timestamp
    pub created_at: u64,
    /// Expiration timestamp (if any)
    pub expires_at: Option<u64>,
    /// Is key active
    pub active: bool,
}

/// Namespace encryption manager
pub struct NamespaceEncryptionManager {
    /// Tenant encryption configs
    tenant_configs: RwLock<HashMap<TenantId, TenantEncryption>>,
    /// Data encryption keys per tenant/namespace
    data_keys: RwLock<HashMap<(TenantId, String), Vec<DataEncryptionKey>>>,
    /// Decrypted key cache (in secure memory)
    key_cache: RwLock<HashMap<String, CachedKey>>,
    /// Cache TTL
    cache_ttl: Duration,
}

#[derive(Clone)]
struct CachedKey {
    key_material: Vec<u8>,
    cached_at: std::time::Instant,
}

impl NamespaceEncryptionManager {
    pub fn new() -> Self {
        Self {
            tenant_configs: RwLock::new(HashMap::new()),
            data_keys: RwLock::new(HashMap::new()),
            key_cache: RwLock::new(HashMap::new()),
            cache_ttl: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Configure encryption for a tenant
    pub fn configure_tenant(&self, tenant_id: &str, config: TenantEncryption) {
        self.tenant_configs
            .write()
            .insert(tenant_id.to_string(), config);
    }

    /// Generate a new data encryption key for a namespace
    pub fn generate_namespace_key(
        &self,
        tenant_id: &str,
        namespace: &str,
    ) -> Result<String, EncryptionError> {
        let config = self
            .tenant_configs
            .read()
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| EncryptionError::TenantNotFound(tenant_id.to_string()))?;

        if !config.enabled {
            return Err(EncryptionError::EncryptionDisabled);
        }

        // Generate random key material
        let key_material = generate_random_key(32); // 256 bits

        // Encrypt with master key (simplified - in production would use KMS)
        let encrypted_key = self.encrypt_with_master_key(&key_material, &config)?;

        let key_id = format!("{}:{}:{}", tenant_id, namespace, generate_key_id());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let dek = DataEncryptionKey {
            key_id: key_id.clone(),
            encrypted_key,
            version: 1,
            created_at: now,
            expires_at: Some(now + config.key_rotation_days as u64 * 86400),
            active: true,
        };

        // Store the key
        let mut keys = self.data_keys.write();
        keys.entry((tenant_id.to_string(), namespace.to_string()))
            .or_insert_with(Vec::new)
            .push(dek);

        Ok(key_id)
    }

    /// Get active key for a namespace
    pub fn get_active_key(&self, tenant_id: &str, namespace: &str) -> Option<DataEncryptionKey> {
        let keys = self.data_keys.read();
        keys.get(&(tenant_id.to_string(), namespace.to_string()))
            .and_then(|ks| ks.iter().find(|k| k.active).cloned())
    }

    /// Rotate keys for a namespace
    pub fn rotate_keys(&self, tenant_id: &str, namespace: &str) -> Result<String, EncryptionError> {
        // Deactivate current keys
        {
            let mut keys = self.data_keys.write();
            if let Some(key_list) = keys.get_mut(&(tenant_id.to_string(), namespace.to_string())) {
                for key in key_list.iter_mut() {
                    key.active = false;
                }
            }
        }

        // Generate new key
        self.generate_namespace_key(tenant_id, namespace)
    }

    /// Encrypt data with namespace key
    pub fn encrypt(
        &self,
        tenant_id: &str,
        namespace: &str,
        plaintext: &[u8],
    ) -> Result<EncryptedData, EncryptionError> {
        let key = self
            .get_active_key(tenant_id, namespace)
            .ok_or(EncryptionError::KeyNotFound)?;

        // Get decrypted key material (from cache or decrypt)
        let key_material = self.get_decrypted_key(&key)?;

        // Encrypt data (simplified - would use proper crypto library)
        let nonce = generate_random_key(12);
        let ciphertext = xor_encrypt(&plaintext, &key_material, &nonce);

        Ok(EncryptedData {
            key_id: key.key_id.clone(),
            key_version: key.version,
            nonce,
            ciphertext,
        })
    }

    /// Decrypt data with namespace key
    pub fn decrypt(
        &self,
        tenant_id: &str,
        namespace: &str,
        encrypted: &EncryptedData,
    ) -> Result<Vec<u8>, EncryptionError> {
        // Find the key used for encryption
        let keys = self.data_keys.read();
        let key = keys
            .get(&(tenant_id.to_string(), namespace.to_string()))
            .and_then(|ks| ks.iter().find(|k| k.key_id == encrypted.key_id))
            .cloned()
            .ok_or(EncryptionError::KeyNotFound)?;

        let key_material = self.get_decrypted_key(&key)?;

        // Decrypt data
        let plaintext = xor_encrypt(&encrypted.ciphertext, &key_material, &encrypted.nonce);

        Ok(plaintext)
    }

    fn encrypt_with_master_key(
        &self,
        key_material: &[u8],
        _config: &TenantEncryption,
    ) -> Result<Vec<u8>, EncryptionError> {
        // Simplified - in production would call KMS
        // Just return the key XOR'd with a fixed value for demo
        Ok(key_material.iter().map(|b| b ^ 0x5A).collect())
    }

    fn get_decrypted_key(&self, key: &DataEncryptionKey) -> Result<Vec<u8>, EncryptionError> {
        // Check cache
        {
            let cache = self.key_cache.read();
            if let Some(cached) = cache.get(&key.key_id) {
                if cached.cached_at.elapsed() < self.cache_ttl {
                    return Ok(cached.key_material.clone());
                }
            }
        }

        // Decrypt key (simplified)
        let decrypted: Vec<u8> = key.encrypted_key.iter().map(|b| b ^ 0x5A).collect();

        // Cache it
        self.key_cache.write().insert(
            key.key_id.clone(),
            CachedKey {
                key_material: decrypted.clone(),
                cached_at: std::time::Instant::now(),
            },
        );

        Ok(decrypted)
    }

    /// Clear key cache (for security)
    pub fn clear_cache(&self) {
        self.key_cache.write().clear();
    }

    /// Prune expired entries from the key cache
    pub fn prune_cache(&self) {
        let mut cache = self.key_cache.write();
        cache.retain(|_, cached| cached.cached_at.elapsed() < self.cache_ttl);
    }

    /// Remove all keys for a tenant (call when deleting tenant)
    pub fn remove_tenant_keys(&self, tenant_id: &str) {
        self.tenant_configs.write().remove(tenant_id);

        let mut data_keys = self.data_keys.write();
        data_keys.retain(|(tid, _), _| tid != tenant_id);

        // Clear related cache entries
        let mut cache = self.key_cache.write();
        cache.retain(|key_id, _| !key_id.starts_with(&format!("{}:", tenant_id)));
    }
}

impl Default for NamespaceEncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Encrypted data container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    pub key_id: String,
    pub key_version: u32,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// Encryption errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionError {
    TenantNotFound(String),
    EncryptionDisabled,
    KeyNotFound,
    KeyExpired,
    DecryptionFailed,
    KmsError(String),
}

impl std::fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionError::TenantNotFound(t) => write!(f, "Tenant not found: {}", t),
            EncryptionError::EncryptionDisabled => write!(f, "Encryption disabled for tenant"),
            EncryptionError::KeyNotFound => write!(f, "Encryption key not found"),
            EncryptionError::KeyExpired => write!(f, "Encryption key expired"),
            EncryptionError::DecryptionFailed => write!(f, "Decryption failed"),
            EncryptionError::KmsError(e) => write!(f, "KMS error: {}", e),
        }
    }
}

impl std::error::Error for EncryptionError {}

// Helper functions for demo (would use proper crypto in production)
fn generate_random_key(len: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut result = Vec::with_capacity(len);
    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );

    for i in 0..len {
        hasher.write_usize(i);
        result.push((hasher.finish() & 0xFF) as u8);
    }
    result
}

fn generate_key_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    format!("{:016x}", hasher.finish())
}

fn xor_encrypt(data: &[u8], key: &[u8], nonce: &[u8]) -> Vec<u8> {
    // Simplified XOR encryption for demo
    data.iter()
        .enumerate()
        .map(|(i, b)| b ^ key[i % key.len()] ^ nonce[i % nonce.len()])
        .collect()
}

// ============================================================================
// Per-Tenant Backup/Restore
// ============================================================================

/// Tenant backup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantBackupConfig {
    /// Automatic backup enabled
    pub auto_backup: bool,
    /// Backup frequency (cron expression)
    pub backup_schedule: String,
    /// Retention period in days
    pub retention_days: u32,
    /// Backup storage location
    pub storage_location: BackupStorageLocation,
    /// Encryption for backups
    pub encrypt_backups: bool,
    /// Point-in-time recovery enabled
    pub pitr_enabled: bool,
    /// PITR retention in hours
    pub pitr_retention_hours: u32,
}

impl Default for TenantBackupConfig {
    fn default() -> Self {
        Self {
            auto_backup: true,
            backup_schedule: "0 0 * * *".to_string(), // Daily at midnight
            retention_days: 30,
            storage_location: BackupStorageLocation::Local {
                path: "/backups".to_string(),
            },
            encrypt_backups: true,
            pitr_enabled: true,
            pitr_retention_hours: 24,
        }
    }
}

/// Backup storage location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupStorageLocation {
    Local { path: String },
    S3 { bucket: String, prefix: String },
    Gcs { bucket: String, prefix: String },
    Azure { container: String, prefix: String },
}

/// Backup metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantBackup {
    /// Backup ID
    pub backup_id: String,
    /// Tenant ID
    pub tenant_id: TenantId,
    /// Backup type
    pub backup_type: BackupType,
    /// Backup status
    pub status: BackupStatus,
    /// Start time
    pub started_at: u64,
    /// End time
    pub completed_at: Option<u64>,
    /// Size in bytes
    pub size_bytes: u64,
    /// Databases included
    pub databases: Vec<String>,
    /// Storage location
    pub location: String,
    /// Is encrypted
    pub encrypted: bool,
    /// Encryption key ID (if encrypted)
    pub encryption_key_id: Option<String>,
    /// Checksum
    pub checksum: String,
}

/// Backup type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupType {
    Full,
    Incremental,
    Differential,
    PointInTime,
}

/// Backup status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    InProgress,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

/// Restore request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    /// Backup ID to restore from
    pub backup_id: String,
    /// Target tenant ID (can be different for cloning)
    pub target_tenant_id: TenantId,
    /// Databases to restore (None = all)
    pub databases: Option<Vec<String>>,
    /// Tables to restore (None = all)
    pub tables: Option<Vec<String>>,
    /// Point-in-time to restore to (for PITR)
    pub point_in_time: Option<u64>,
    /// Restore to new names (for cloning)
    pub rename_mapping: Option<HashMap<String, String>>,
}

/// Restore status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreStatus {
    pub restore_id: String,
    pub backup_id: String,
    pub target_tenant_id: TenantId,
    pub status: RestoreState,
    pub progress_percent: u32,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub error: Option<String>,
    pub databases_restored: Vec<String>,
    pub bytes_restored: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreState {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Tenant backup manager
pub struct TenantBackupManager {
    /// Backup configurations per tenant
    configs: RwLock<HashMap<TenantId, TenantBackupConfig>>,
    /// Backup metadata
    backups: RwLock<HashMap<String, TenantBackup>>,
    /// Active restores
    restores: RwLock<HashMap<String, RestoreStatus>>,
    /// Backup counter
    backup_counter: AtomicU64,
    /// Encryption manager
    encryption_manager: Arc<NamespaceEncryptionManager>,
    /// Maximum backups to keep per tenant (prevents unbounded growth)
    max_backups_per_tenant: usize,
}

impl TenantBackupManager {
    pub fn new(encryption_manager: Arc<NamespaceEncryptionManager>) -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            backups: RwLock::new(HashMap::new()),
            restores: RwLock::new(HashMap::new()),
            backup_counter: AtomicU64::new(0),
            encryption_manager,
            max_backups_per_tenant: 100,
        }
    }

    /// Cleanup completed restores older than the given age in seconds
    pub fn cleanup_old_restores(&self, max_age_secs: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut restores = self.restores.write();
        restores.retain(|_, r| {
            // Keep pending/in-progress restores, or completed ones within max_age
            r.status == RestoreState::Pending
                || r.status == RestoreState::InProgress
                || r.completed_at
                    .map(|t| now - t < max_age_secs)
                    .unwrap_or(true)
        });
    }

    /// Enforce backup limits per tenant (keeps most recent backups)
    fn enforce_backup_limits(&self, tenant_id: &str) {
        let mut backups = self.backups.write();

        // Get all backups for this tenant sorted by creation time
        let mut tenant_backups: Vec<_> = backups
            .iter()
            .filter(|(_, b)| b.tenant_id == tenant_id && b.status == BackupStatus::Completed)
            .map(|(id, b)| (id.clone(), b.started_at))
            .collect();

        if tenant_backups.len() <= self.max_backups_per_tenant {
            return;
        }

        // Sort by time (oldest first)
        tenant_backups.sort_by_key(|(_, ts)| *ts);

        // Remove oldest backups exceeding limit
        let to_remove = tenant_backups.len() - self.max_backups_per_tenant;
        for (id, _) in tenant_backups.into_iter().take(to_remove) {
            backups.remove(&id);
        }
    }

    /// Configure backup for a tenant
    pub fn configure(&self, tenant_id: &str, config: TenantBackupConfig) {
        self.configs.write().insert(tenant_id.to_string(), config);
    }

    /// Start a backup for a tenant
    pub fn start_backup(
        &self,
        tenant_id: &str,
        backup_type: BackupType,
        databases: Option<Vec<String>>,
    ) -> Result<String, BackupError> {
        let config = self
            .configs
            .read()
            .get(tenant_id)
            .cloned()
            .unwrap_or_default();

        let backup_id = format!(
            "backup_{}_{}",
            tenant_id,
            self.backup_counter.fetch_add(1, Ordering::SeqCst)
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let backup = TenantBackup {
            backup_id: backup_id.clone(),
            tenant_id: tenant_id.to_string(),
            backup_type,
            status: BackupStatus::InProgress,
            started_at: now,
            completed_at: None,
            size_bytes: 0,
            databases: databases.unwrap_or_default(),
            location: format!("{}/{}", tenant_id, backup_id),
            encrypted: config.encrypt_backups,
            encryption_key_id: None,
            checksum: String::new(),
        };

        self.backups.write().insert(backup_id.clone(), backup);

        Ok(backup_id)
    }

    /// Complete a backup
    pub fn complete_backup(
        &self,
        backup_id: &str,
        size_bytes: u64,
        checksum: &str,
    ) -> Result<(), BackupError> {
        let tenant_id = {
            let mut backups = self.backups.write();
            let backup = backups
                .get_mut(backup_id)
                .ok_or_else(|| BackupError::BackupNotFound(backup_id.to_string()))?;

            backup.status = BackupStatus::Completed;
            backup.completed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            backup.size_bytes = size_bytes;
            backup.checksum = checksum.to_string();
            backup.tenant_id.clone()
        };

        // Enforce per-tenant backup limits
        self.enforce_backup_limits(&tenant_id);

        Ok(())
    }

    /// Fail a backup
    pub fn fail_backup(&self, backup_id: &str) {
        if let Some(backup) = self.backups.write().get_mut(backup_id) {
            backup.status = BackupStatus::Failed;
            backup.completed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }
    }

    /// List backups for a tenant
    pub fn list_backups(&self, tenant_id: &str) -> Vec<TenantBackup> {
        self.backups
            .read()
            .values()
            .filter(|b| b.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Get backup by ID
    pub fn get_backup(&self, backup_id: &str) -> Option<TenantBackup> {
        self.backups.read().get(backup_id).cloned()
    }

    /// Start a restore
    pub fn start_restore(&self, request: RestoreRequest) -> Result<String, BackupError> {
        // Verify backup exists
        let backup = self
            .backups
            .read()
            .get(&request.backup_id)
            .cloned()
            .ok_or_else(|| BackupError::BackupNotFound(request.backup_id.clone()))?;

        if backup.status != BackupStatus::Completed {
            return Err(BackupError::BackupNotComplete);
        }

        let restore_id = format!("restore_{}_{}", request.target_tenant_id, generate_key_id());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let status = RestoreStatus {
            restore_id: restore_id.clone(),
            backup_id: request.backup_id,
            target_tenant_id: request.target_tenant_id,
            status: RestoreState::Pending,
            progress_percent: 0,
            started_at: now,
            completed_at: None,
            error: None,
            databases_restored: vec![],
            bytes_restored: 0,
        };

        self.restores.write().insert(restore_id.clone(), status);

        Ok(restore_id)
    }

    /// Update restore progress
    pub fn update_restore_progress(&self, restore_id: &str, progress: u32, bytes_restored: u64) {
        if let Some(status) = self.restores.write().get_mut(restore_id) {
            status.progress_percent = progress;
            status.bytes_restored = bytes_restored;
            if status.status == RestoreState::Pending {
                status.status = RestoreState::InProgress;
            }
        }
    }

    /// Complete a restore
    pub fn complete_restore(&self, restore_id: &str, databases: Vec<String>) {
        if let Some(status) = self.restores.write().get_mut(restore_id) {
            status.status = RestoreState::Completed;
            status.progress_percent = 100;
            status.completed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            status.databases_restored = databases;
        }
    }

    /// Fail a restore
    pub fn fail_restore(&self, restore_id: &str, error: &str) {
        if let Some(status) = self.restores.write().get_mut(restore_id) {
            status.status = RestoreState::Failed;
            status.error = Some(error.to_string());
            status.completed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }
    }

    /// Get restore status
    pub fn get_restore_status(&self, restore_id: &str) -> Option<RestoreStatus> {
        self.restores.read().get(restore_id).cloned()
    }

    /// Prune expired backups
    pub fn prune_expired(&self) -> Vec<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut pruned = Vec::new();
        let configs = self.configs.read();
        let mut backups = self.backups.write();

        let expired_ids: Vec<String> = backups
            .iter()
            .filter(|(_, b)| {
                if b.status != BackupStatus::Completed {
                    return false;
                }
                let retention_secs = configs
                    .get(&b.tenant_id)
                    .map(|c| c.retention_days as u64 * 86400)
                    .unwrap_or(30 * 86400);
                b.completed_at
                    .map(|t| now - t > retention_secs)
                    .unwrap_or(false)
            })
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            if let Some(mut backup) = backups.remove(&id) {
                backup.status = BackupStatus::Expired;
                pruned.push(id);
            }
        }

        pruned
    }
}

/// Backup errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupError {
    BackupNotFound(String),
    BackupNotComplete,
    RestoreInProgress,
    StorageError(String),
    EncryptionError(String),
    ValidationError(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::BackupNotFound(id) => write!(f, "Backup not found: {}", id),
            BackupError::BackupNotComplete => write!(f, "Backup not complete"),
            BackupError::RestoreInProgress => write!(f, "Restore already in progress"),
            BackupError::StorageError(e) => write!(f, "Storage error: {}", e),
            BackupError::EncryptionError(e) => write!(f, "Encryption error: {}", e),
            BackupError::ValidationError(e) => write!(f, "Validation error: {}", e),
        }
    }
}

impl std::error::Error for BackupError {}

// ============================================================================
// Tenant Manager
// ============================================================================

/// Unified tenant management
pub struct TenantManager {
    /// Tenant configurations
    tenants: RwLock<HashMap<TenantId, TenantConfig>>,
    /// Encryption manager
    encryption_manager: Arc<NamespaceEncryptionManager>,
    /// Backup manager
    backup_manager: Arc<TenantBackupManager>,
    /// Resource usage tracking
    usage: RwLock<HashMap<TenantId, TenantUsage>>,
}

/// Tenant resource usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantUsage {
    pub tenant_id: TenantId,
    pub storage_bytes: u64,
    pub database_count: u32,
    pub table_count: u32,
    pub active_connections: u32,
    pub queries_per_second: f64,
    pub cpu_usage: f64,
    pub memory_bytes: u64,
    pub last_updated: u64,
}

impl TenantManager {
    pub fn new() -> Self {
        let encryption_manager = Arc::new(NamespaceEncryptionManager::new());
        let backup_manager = Arc::new(TenantBackupManager::new(encryption_manager.clone()));

        Self {
            tenants: RwLock::new(HashMap::new()),
            encryption_manager,
            backup_manager,
            usage: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new tenant
    pub fn create_tenant(&self, config: TenantConfig) -> Result<(), TenantError> {
        let tenant_id = config.tenant_id.clone();

        // Check if tenant already exists
        if self.tenants.read().contains_key(&tenant_id) {
            return Err(TenantError::AlreadyExists(tenant_id));
        }

        // Configure encryption
        self.encryption_manager
            .configure_tenant(&tenant_id, config.encryption.clone());

        // Configure backups
        self.backup_manager
            .configure(&tenant_id, config.backup.clone());

        // Initialize usage tracking
        let usage = TenantUsage {
            tenant_id: tenant_id.clone(),
            storage_bytes: 0,
            database_count: 0,
            table_count: 0,
            active_connections: 0,
            queries_per_second: 0.0,
            cpu_usage: 0.0,
            memory_bytes: 0,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.usage.write().insert(tenant_id.clone(), usage);

        // Store tenant config
        self.tenants.write().insert(tenant_id, config);

        Ok(())
    }

    /// Get tenant configuration
    pub fn get_tenant(&self, tenant_id: &str) -> Option<TenantConfig> {
        self.tenants.read().get(tenant_id).cloned()
    }

    /// Update tenant configuration
    pub fn update_tenant(&self, config: TenantConfig) -> Result<(), TenantError> {
        let tenant_id = config.tenant_id.clone();

        if !self.tenants.read().contains_key(&tenant_id) {
            return Err(TenantError::NotFound(tenant_id));
        }

        self.encryption_manager
            .configure_tenant(&tenant_id, config.encryption.clone());
        self.backup_manager
            .configure(&tenant_id, config.backup.clone());
        self.tenants.write().insert(tenant_id, config);

        Ok(())
    }

    /// Suspend a tenant
    pub fn suspend_tenant(&self, tenant_id: &str) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write();
        let tenant = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.status = TenantStatus::Suspended;
        tenant.modified_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }

    /// Reactivate a tenant
    pub fn reactivate_tenant(&self, tenant_id: &str) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write();
        let tenant = tenants
            .get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if tenant.status != TenantStatus::Suspended {
            return Err(TenantError::InvalidState(format!(
                "Tenant is {:?}, not Suspended",
                tenant.status
            )));
        }

        tenant.status = TenantStatus::Active;
        tenant.modified_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }

    /// Check if tenant can perform operation based on quotas
    pub fn check_quota(
        &self,
        tenant_id: &str,
        operation: QuotaCheckOperation,
    ) -> Result<(), TenantError> {
        let tenant = self
            .tenants
            .read()
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if tenant.status != TenantStatus::Active {
            return Err(TenantError::TenantSuspended);
        }

        let usage = self
            .usage
            .read()
            .get(tenant_id)
            .cloned()
            .unwrap_or_else(|| TenantUsage {
                tenant_id: tenant_id.to_string(),
                storage_bytes: 0,
                database_count: 0,
                table_count: 0,
                active_connections: 0,
                queries_per_second: 0.0,
                cpu_usage: 0.0,
                memory_bytes: 0,
                last_updated: 0,
            });

        match operation {
            QuotaCheckOperation::CreateDatabase => {
                if usage.database_count >= tenant.quotas.max_databases {
                    return Err(TenantError::QuotaExceeded("databases".to_string()));
                }
            }
            QuotaCheckOperation::CreateTable => {
                if usage.table_count >= tenant.quotas.max_tables_per_db * usage.database_count {
                    return Err(TenantError::QuotaExceeded("tables".to_string()));
                }
            }
            QuotaCheckOperation::Connect => {
                if usage.active_connections >= tenant.quotas.max_connections {
                    return Err(TenantError::QuotaExceeded("connections".to_string()));
                }
            }
            QuotaCheckOperation::Write { bytes } => {
                if usage.storage_bytes + bytes > tenant.quotas.max_storage_bytes {
                    return Err(TenantError::QuotaExceeded("storage".to_string()));
                }
            }
            QuotaCheckOperation::Query => {
                if usage.queries_per_second >= tenant.quotas.max_qps as f64 {
                    return Err(TenantError::QuotaExceeded("qps".to_string()));
                }
            }
        }

        Ok(())
    }

    /// Update usage metrics
    pub fn update_usage(&self, tenant_id: &str, usage: TenantUsage) {
        self.usage.write().insert(tenant_id.to_string(), usage);
    }

    /// Get usage metrics
    pub fn get_usage(&self, tenant_id: &str) -> Option<TenantUsage> {
        self.usage.read().get(tenant_id).cloned()
    }

    /// Get encryption manager
    pub fn encryption(&self) -> Arc<NamespaceEncryptionManager> {
        self.encryption_manager.clone()
    }

    /// Get backup manager
    pub fn backups(&self) -> Arc<TenantBackupManager> {
        self.backup_manager.clone()
    }

    /// List all tenants
    pub fn list_tenants(&self) -> Vec<TenantConfig> {
        self.tenants.read().values().cloned().collect()
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Operations to check quota for
#[derive(Debug, Clone)]
pub enum QuotaCheckOperation {
    CreateDatabase,
    CreateTable,
    Connect,
    Write { bytes: u64 },
    Query,
}

/// Tenant errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TenantError {
    NotFound(String),
    AlreadyExists(String),
    InvalidState(String),
    TenantSuspended,
    QuotaExceeded(String),
}

impl std::fmt::Display for TenantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantError::NotFound(id) => write!(f, "Tenant not found: {}", id),
            TenantError::AlreadyExists(id) => write!(f, "Tenant already exists: {}", id),
            TenantError::InvalidState(msg) => write!(f, "Invalid tenant state: {}", msg),
            TenantError::TenantSuspended => write!(f, "Tenant is suspended"),
            TenantError::QuotaExceeded(q) => write!(f, "Quota exceeded: {}", q),
        }
    }
}

impl std::error::Error for TenantError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let manager = TenantManager::new();

        let config = TenantConfig {
            tenant_id: "tenant1".to_string(),
            name: "Test Tenant".to_string(),
            status: TenantStatus::Active,
            quotas: TenantQuotas::default(),
            encryption: TenantEncryption::default(),
            backup: TenantBackupConfig::default(),
            data_residency: None,
            created_at: 0,
            modified_at: 0,
        };

        assert!(manager.create_tenant(config.clone()).is_ok());
        assert!(manager.create_tenant(config).is_err()); // Duplicate
    }

    #[test]
    fn test_namespace_encryption() {
        let enc_manager = NamespaceEncryptionManager::new();

        enc_manager.configure_tenant("tenant1", TenantEncryption::default());
        let key_id = enc_manager.generate_namespace_key("tenant1", "database1");
        assert!(key_id.is_ok());

        let plaintext = b"Hello, World!";
        let encrypted = enc_manager.encrypt("tenant1", "database1", plaintext);
        assert!(encrypted.is_ok());

        let encrypted = encrypted.unwrap();
        let decrypted = enc_manager.decrypt("tenant1", "database1", &encrypted);
        assert!(decrypted.is_ok());
        assert_eq!(decrypted.unwrap(), plaintext);
    }

    #[test]
    fn test_backup_lifecycle() {
        let enc_manager = Arc::new(NamespaceEncryptionManager::new());
        let backup_manager = TenantBackupManager::new(enc_manager);

        backup_manager.configure("tenant1", TenantBackupConfig::default());

        let backup_id = backup_manager
            .start_backup("tenant1", BackupType::Full, None)
            .unwrap();

        assert!(backup_manager
            .complete_backup(&backup_id, 1000, "abc123")
            .is_ok());

        let backup = backup_manager.get_backup(&backup_id).unwrap();
        assert_eq!(backup.status, BackupStatus::Completed);
        assert_eq!(backup.size_bytes, 1000);
    }

    #[test]
    fn test_quota_check() {
        let manager = TenantManager::new();

        let mut quotas = TenantQuotas::default();
        quotas.max_databases = 1;

        let config = TenantConfig {
            tenant_id: "tenant1".to_string(),
            name: "Test Tenant".to_string(),
            status: TenantStatus::Active,
            quotas,
            encryption: TenantEncryption::default(),
            backup: TenantBackupConfig::default(),
            data_residency: None,
            created_at: 0,
            modified_at: 0,
        };

        manager.create_tenant(config).unwrap();

        // Set usage to max
        let usage = TenantUsage {
            tenant_id: "tenant1".to_string(),
            storage_bytes: 0,
            database_count: 1,
            table_count: 0,
            active_connections: 0,
            queries_per_second: 0.0,
            cpu_usage: 0.0,
            memory_bytes: 0,
            last_updated: 0,
        };
        manager.update_usage("tenant1", usage);

        // Should fail - quota exceeded
        let result = manager.check_quota("tenant1", QuotaCheckOperation::CreateDatabase);
        assert!(result.is_err());
    }
}
