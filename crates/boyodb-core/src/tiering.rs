use crate::engine::{EngineError, persist_manifest};
use crate::replication::{Manifest, ManifestEntry, SegmentTier};
use crate::storage::TieredStorage;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

// ============================================================================
// Lifecycle Policy Configuration
// ============================================================================

/// S3 storage class for cold tier data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum S3StorageClass {
    #[default]
    Standard,
    StandardIA,         // Infrequent Access
    OneZoneIA,          // One Zone Infrequent Access
    IntelligentTiering, // AWS intelligent tiering
    Glacier,            // Glacier Flexible Retrieval
    GlacierIR,          // Glacier Instant Retrieval
    DeepArchive,        // Glacier Deep Archive
}

impl S3StorageClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            S3StorageClass::Standard => "STANDARD",
            S3StorageClass::StandardIA => "STANDARD_IA",
            S3StorageClass::OneZoneIA => "ONEZONE_IA",
            S3StorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
            S3StorageClass::Glacier => "GLACIER",
            S3StorageClass::GlacierIR => "GLACIER_IR",
            S3StorageClass::DeepArchive => "DEEP_ARCHIVE",
        }
    }

    /// Estimated retrieval time in seconds (0 = instant)
    pub fn retrieval_time_secs(&self) -> u64 {
        match self {
            S3StorageClass::Standard => 0,
            S3StorageClass::StandardIA => 0,
            S3StorageClass::OneZoneIA => 0,
            S3StorageClass::IntelligentTiering => 0,
            S3StorageClass::GlacierIR => 0,
            S3StorageClass::Glacier => 180,         // 3-5 minutes expedited
            S3StorageClass::DeepArchive => 43200,   // 12 hours standard
        }
    }
}

/// Policy for determining when segments transition between tiers
#[derive(Debug, Clone)]
pub struct LifecyclePolicy {
    /// Name identifier for this policy
    pub name: String,
    /// Optional database filter (None = all databases)
    pub database_filter: Option<String>,
    /// Optional table filter (None = all tables)
    pub table_filter: Option<String>,
    /// Rules for tier transitions
    pub rules: Vec<LifecycleRule>,
    /// Priority (higher = evaluated first)
    pub priority: i32,
    /// Whether this policy is enabled
    pub enabled: bool,
}

/// A single lifecycle rule for tier transitions
#[derive(Debug, Clone)]
pub struct LifecycleRule {
    /// From tier
    pub from_tier: SegmentTier,
    /// To tier
    pub to_tier: SegmentTier,
    /// Conditions that must ALL be met for transition
    pub conditions: Vec<TransitionCondition>,
    /// S3 storage class to use when transitioning to Cold
    pub storage_class: S3StorageClass,
}

/// Conditions for triggering a tier transition
#[derive(Debug, Clone)]
pub enum TransitionCondition {
    /// Segment age exceeds threshold (milliseconds since last write)
    AgeExceeds(u64),
    /// Segment size exceeds threshold (bytes)
    SizeExceeds(u64),
    /// Segment size is below threshold (bytes) - for compaction targets
    SizeBelow(u64),
    /// Access count below threshold in last N milliseconds
    AccessCountBelow { count: u64, window_millis: u64 },
    /// Row count exceeds threshold
    RowCountExceeds(u64),
    /// Row count below threshold
    RowCountBelow(u64),
    /// Always true (for unconditional rules)
    Always,
}

impl TransitionCondition {
    pub fn evaluate(&self, segment: &SegmentInfo, stats: &TieringStats) -> bool {
        match self {
            TransitionCondition::AgeExceeds(threshold) => {
                segment.age_millis >= *threshold
            }
            TransitionCondition::SizeExceeds(threshold) => {
                segment.size_bytes >= *threshold
            }
            TransitionCondition::SizeBelow(threshold) => {
                segment.size_bytes < *threshold
            }
            TransitionCondition::AccessCountBelow { count, window_millis } => {
                let recent_accesses = stats.get_recent_accesses(&segment.segment_id, *window_millis);
                recent_accesses < *count
            }
            TransitionCondition::RowCountExceeds(threshold) => {
                segment.row_count >= *threshold
            }
            TransitionCondition::RowCountBelow(threshold) => {
                segment.row_count < *threshold
            }
            TransitionCondition::Always => true,
        }
    }
}

/// Information about a segment for policy evaluation
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub segment_id: String,
    pub database: String,
    pub table: String,
    pub tier: SegmentTier,
    pub size_bytes: u64,
    pub row_count: u64,
    pub age_millis: u64,
    pub watermark_micros: u64,
}

impl SegmentInfo {
    pub fn from_manifest_entry(entry: &ManifestEntry, now_micros: u64) -> Self {
        let age_millis = now_micros.saturating_sub(entry.watermark_micros) / 1000;
        Self {
            segment_id: entry.segment_id.clone(),
            database: entry.database.clone(),
            table: entry.table.clone(),
            tier: entry.tier,
            size_bytes: entry.size_bytes,
            row_count: 0, // Not stored in manifest, would need to read segment
            age_millis,
            watermark_micros: entry.watermark_micros,
        }
    }
}

// ============================================================================
// Tiering Statistics
// ============================================================================

/// Statistics and metrics for tiering operations
#[derive(Debug, Default)]
pub struct TieringStats {
    /// Total bytes moved to cold tier
    pub bytes_tiered_to_cold: AtomicU64,
    /// Total bytes moved to warm tier
    pub bytes_tiered_to_warm: AtomicU64,
    /// Total segments tiered
    pub segments_tiered: AtomicU64,
    /// Failed tiering operations
    pub tiering_failures: AtomicU64,
    /// Bytes retrieved from cold storage
    pub bytes_retrieved_cold: AtomicU64,
    /// Cache hits for cold data
    pub cold_cache_hits: AtomicU64,
    /// Cache misses for cold data
    pub cold_cache_misses: AtomicU64,
    /// Access tracking: segment_id -> (access_count, last_access_micros)
    access_log: RwLock<HashMap<String, (u64, u64)>>,
}

impl TieringStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_access(&self, segment_id: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut log = self.access_log.write().unwrap();
        let entry = log.entry(segment_id.to_string()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 = now;
    }

    pub fn get_recent_accesses(&self, segment_id: &str, window_millis: u64) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let window_micros = window_millis * 1000;
        let log = self.access_log.read().unwrap();

        if let Some((count, last_access)) = log.get(segment_id) {
            if now.saturating_sub(*last_access) < window_micros {
                return *count;
            }
        }
        0
    }

    pub fn record_tier_transition(&self, bytes: u64, to_tier: SegmentTier) {
        match to_tier {
            SegmentTier::Cold => {
                self.bytes_tiered_to_cold.fetch_add(bytes, Ordering::Relaxed);
            }
            SegmentTier::Warm => {
                self.bytes_tiered_to_warm.fetch_add(bytes, Ordering::Relaxed);
            }
            _ => {}
        }
        self.segments_tiered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.tiering_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cold_retrieval(&self, bytes: u64) {
        self.bytes_retrieved_cold.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TieringStatsSnapshot {
        TieringStatsSnapshot {
            bytes_tiered_to_cold: self.bytes_tiered_to_cold.load(Ordering::Relaxed),
            bytes_tiered_to_warm: self.bytes_tiered_to_warm.load(Ordering::Relaxed),
            segments_tiered: self.segments_tiered.load(Ordering::Relaxed),
            tiering_failures: self.tiering_failures.load(Ordering::Relaxed),
            bytes_retrieved_cold: self.bytes_retrieved_cold.load(Ordering::Relaxed),
            cold_cache_hits: self.cold_cache_hits.load(Ordering::Relaxed),
            cold_cache_misses: self.cold_cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of tiering stats
#[derive(Debug, Clone, Default)]
pub struct TieringStatsSnapshot {
    pub bytes_tiered_to_cold: u64,
    pub bytes_tiered_to_warm: u64,
    pub segments_tiered: u64,
    pub tiering_failures: u64,
    pub bytes_retrieved_cold: u64,
    pub cold_cache_hits: u64,
    pub cold_cache_misses: u64,
}

// ============================================================================
// Cold Data Cache
// ============================================================================

/// LRU cache for frequently accessed cold data
pub struct ColdDataCache {
    cache: RwLock<HashMap<String, CacheEntry>>,
    max_bytes: u64,
    current_bytes: AtomicU64,
}

struct CacheEntry {
    data: Vec<u8>,
    last_access: u64,
    access_count: u64,
}

impl ColdDataCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_bytes,
            current_bytes: AtomicU64::new(0),
        }
    }

    pub fn get(&self, segment_id: &str) -> Option<Vec<u8>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut cache = self.cache.write().unwrap();
        if let Some(entry) = cache.get_mut(segment_id) {
            entry.last_access = now;
            entry.access_count += 1;
            return Some(entry.data.clone());
        }
        None
    }

    pub fn put(&self, segment_id: &str, data: Vec<u8>) {
        let data_len = data.len() as u64;

        // Don't cache if single item exceeds max
        if data_len > self.max_bytes {
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut cache = self.cache.write().unwrap();

        // Evict until we have space
        while self.current_bytes.load(Ordering::Relaxed) + data_len > self.max_bytes {
            // Find LRU entry
            let lru_key = cache
                .iter()
                .min_by_key(|(_, v)| v.last_access)
                .map(|(k, _)| k.clone());

            if let Some(key) = lru_key {
                if let Some(removed) = cache.remove(&key) {
                    self.current_bytes.fetch_sub(removed.data.len() as u64, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }

        // Insert new entry
        if let Some(existing) = cache.insert(segment_id.to_string(), CacheEntry {
            data,
            last_access: now,
            access_count: 1,
        }) {
            self.current_bytes.fetch_sub(existing.data.len() as u64, Ordering::Relaxed);
        }
        self.current_bytes.fetch_add(data_len, Ordering::Relaxed);
    }

    pub fn invalidate(&self, segment_id: &str) {
        let mut cache = self.cache.write().unwrap();
        if let Some(removed) = cache.remove(segment_id) {
            self.current_bytes.fetch_sub(removed.data.len() as u64, Ordering::Relaxed);
        }
    }

    pub fn current_size(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    pub fn entry_count(&self) -> usize {
        self.cache.read().unwrap().len()
    }
}

// ============================================================================
// Default Lifecycle Policies
// ============================================================================

impl LifecyclePolicy {
    /// Default policy: tier to warm after 1 hour, cold after 24 hours
    pub fn default_policy() -> Self {
        Self {
            name: "default".to_string(),
            database_filter: None,
            table_filter: None,
            rules: vec![
                LifecycleRule {
                    from_tier: SegmentTier::Hot,
                    to_tier: SegmentTier::Warm,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(3600_000), // 1 hour
                    ],
                    storage_class: S3StorageClass::Standard,
                },
                LifecycleRule {
                    from_tier: SegmentTier::Warm,
                    to_tier: SegmentTier::Cold,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(86400_000), // 24 hours
                    ],
                    storage_class: S3StorageClass::StandardIA,
                },
            ],
            priority: 0,
            enabled: true,
        }
    }

    /// Aggressive policy: cold after 1 hour, good for cost savings
    pub fn cost_optimized() -> Self {
        Self {
            name: "cost_optimized".to_string(),
            database_filter: None,
            table_filter: None,
            rules: vec![
                LifecycleRule {
                    from_tier: SegmentTier::Hot,
                    to_tier: SegmentTier::Cold,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(3600_000), // 1 hour
                    ],
                    storage_class: S3StorageClass::OneZoneIA,
                },
            ],
            priority: 0,
            enabled: true,
        }
    }

    /// Performance policy: keep hot longer, use faster cold storage
    pub fn performance_optimized() -> Self {
        Self {
            name: "performance_optimized".to_string(),
            database_filter: None,
            table_filter: None,
            rules: vec![
                LifecycleRule {
                    from_tier: SegmentTier::Hot,
                    to_tier: SegmentTier::Warm,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(86400_000), // 24 hours
                    ],
                    storage_class: S3StorageClass::Standard,
                },
                LifecycleRule {
                    from_tier: SegmentTier::Warm,
                    to_tier: SegmentTier::Cold,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(604800_000), // 7 days
                    ],
                    storage_class: S3StorageClass::GlacierIR, // Instant retrieval
                },
            ],
            priority: 0,
            enabled: true,
        }
    }

    /// Archive policy for compliance/audit data
    pub fn archive_policy() -> Self {
        Self {
            name: "archive".to_string(),
            database_filter: None,
            table_filter: None,
            rules: vec![
                LifecycleRule {
                    from_tier: SegmentTier::Hot,
                    to_tier: SegmentTier::Cold,
                    conditions: vec![
                        TransitionCondition::AgeExceeds(86400_000), // 24 hours
                    ],
                    storage_class: S3StorageClass::DeepArchive,
                },
            ],
            priority: 0,
            enabled: true,
        }
    }

    /// Check if policy matches a segment
    pub fn matches(&self, segment: &SegmentInfo) -> bool {
        if let Some(ref db) = self.database_filter {
            if &segment.database != db {
                return false;
            }
        }
        if let Some(ref table) = self.table_filter {
            if &segment.table != table {
                return false;
            }
        }
        true
    }

    /// Find applicable rule for a segment
    pub fn find_applicable_rule(&self, segment: &SegmentInfo, stats: &TieringStats) -> Option<&LifecycleRule> {
        if !self.enabled {
            return None;
        }

        for rule in &self.rules {
            if rule.from_tier != segment.tier {
                continue;
            }

            let all_conditions_met = rule.conditions.iter()
                .all(|c| c.evaluate(segment, stats));

            if all_conditions_met {
                return Some(rule);
            }
        }
        None
    }
}

// ============================================================================
// Enhanced Tiering Manager
// ============================================================================

pub struct TieringManager {
    manifest: Arc<RwLock<Manifest>>,
    manifest_path: PathBuf,
    storage: Arc<TieredStorage>,
    segments_dir: PathBuf,
    tier_cold_after_millis: u64,
    /// Lifecycle policies (sorted by priority descending)
    policies: RwLock<Vec<LifecyclePolicy>>,
    /// Tiering statistics
    pub stats: Arc<TieringStats>,
    /// Cache for cold data
    pub cold_cache: Arc<ColdDataCache>,
    /// Whether to use lifecycle policies (vs simple age-based)
    use_lifecycle_policies: bool,
}

impl TieringManager {
    pub fn new(
        manifest: Arc<RwLock<Manifest>>,
        manifest_path: PathBuf,
        storage: Arc<TieredStorage>,
        segments_dir: PathBuf,
        tier_cold_after_millis: u64,
    ) -> Self {
        Self {
            manifest,
            manifest_path,
            storage,
            segments_dir,
            tier_cold_after_millis,
            policies: RwLock::new(vec![]),
            stats: Arc::new(TieringStats::new()),
            cold_cache: Arc::new(ColdDataCache::new(256 * 1024 * 1024)), // 256MB default
            use_lifecycle_policies: false,
        }
    }

    /// Create with lifecycle policies enabled
    pub fn with_lifecycle_policies(
        manifest: Arc<RwLock<Manifest>>,
        manifest_path: PathBuf,
        storage: Arc<TieredStorage>,
        segments_dir: PathBuf,
        policies: Vec<LifecyclePolicy>,
        cold_cache_bytes: u64,
    ) -> Self {
        let mut sorted_policies = policies;
        sorted_policies.sort_by(|a, b| b.priority.cmp(&a.priority));

        Self {
            manifest,
            manifest_path,
            storage,
            segments_dir,
            tier_cold_after_millis: 0, // Not used with lifecycle policies
            policies: RwLock::new(sorted_policies),
            stats: Arc::new(TieringStats::new()),
            cold_cache: Arc::new(ColdDataCache::new(cold_cache_bytes)),
            use_lifecycle_policies: true,
        }
    }

    /// Add a lifecycle policy
    pub fn add_policy(&self, policy: LifecyclePolicy) {
        let mut policies = self.policies.write().unwrap();
        policies.push(policy);
        policies.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a policy by name
    pub fn remove_policy(&self, name: &str) -> bool {
        let mut policies = self.policies.write().unwrap();
        let len_before = policies.len();
        policies.retain(|p| p.name != name);
        policies.len() != len_before
    }

    /// List all policies
    pub fn list_policies(&self) -> Vec<LifecyclePolicy> {
        self.policies.read().unwrap().clone()
    }

    /// Get tiering statistics
    pub fn get_stats(&self) -> TieringStatsSnapshot {
        self.stats.snapshot()
    }

    pub async fn run(self: Arc<Self>) {
        if !self.use_lifecycle_policies && self.tier_cold_after_millis == 0 {
            tracing::info!("Tiering disabled (tier_cold_after_millis = 0)");
            return;
        }

        if !self.storage.has_remote() {
            tracing::info!("Tiering disabled: no S3 configured");
            return;
        }

        if self.use_lifecycle_policies {
            tracing::info!("Starting TieringManager with lifecycle policies");
        } else {
            tracing::info!(
                "Starting TieringManager (cold after {}ms)",
                self.tier_cold_after_millis
            );
        }

        loop {
            // Check every minute (or faster if threshold is very low during testing)
            let sleep_duration = if self.tier_cold_after_millis < 1000 && !self.use_lifecycle_policies {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(60)
            };
            sleep(sleep_duration).await;

            if self.use_lifecycle_policies {
                if let Err(e) = self.run_lifecycle_cycle() {
                    tracing::error!("Lifecycle tiering cycle failed: {}", e);
                }
            } else {
                if let Err(e) = self.run_tiering_cycle() {
                    tracing::error!("Tiering cycle failed: {}", e);
                }
            }
        }
    }

    /// Run a tiering cycle using lifecycle policies
    fn run_lifecycle_cycle(&self) -> Result<(), EngineError> {
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Collect segment info
        let segments: Vec<SegmentInfo> = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .entries
                .iter()
                .filter(|e| e.tier != SegmentTier::Cold)
                .map(|e| SegmentInfo::from_manifest_entry(e, now_micros))
                .collect()
        };

        if segments.is_empty() {
            return Ok(());
        }

        // Evaluate policies for each segment
        let policies = self.policies.read().unwrap();
        let mut transitions: Vec<(String, SegmentTier, S3StorageClass)> = Vec::new();

        for segment in &segments {
            // Find first matching policy with applicable rule
            for policy in policies.iter() {
                if !policy.matches(segment) {
                    continue;
                }

                if let Some(rule) = policy.find_applicable_rule(segment, &self.stats) {
                    transitions.push((
                        segment.segment_id.clone(),
                        rule.to_tier,
                        rule.storage_class,
                    ));
                    break; // Only apply first matching policy
                }
            }
        }

        drop(policies); // Release lock before I/O

        if transitions.is_empty() {
            return Ok(());
        }

        tracing::info!("Found {} segments for tier transition", transitions.len());

        // Execute transitions
        for (segment_id, to_tier, storage_class) in transitions {
            if let Err(e) = self.tier_segment_with_class(&segment_id, to_tier, storage_class) {
                tracing::warn!(
                    segment_id = %segment_id,
                    to_tier = ?to_tier,
                    error = %e,
                    "Failed to tier segment"
                );
                self.stats.record_failure();
            }
        }

        Ok(())
    }

    fn run_tiering_cycle(&self) -> Result<(), EngineError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Threshold in micros
        let threshold = now.saturating_sub(self.tier_cold_after_millis * 1000);

        // 1. Identify candidates (holding READ lock)
        let candidates: Vec<String> = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .entries
                .iter()
                .filter(|e| {
                    (e.tier == SegmentTier::Hot || e.tier == SegmentTier::Warm)
                        && e.watermark_micros < threshold
                })
                .map(|e| e.segment_id.clone())
                .collect()
        };

        if candidates.is_empty() {
            return Ok(());
        }

        tracing::info!("Found {} candidates for cold tiering", candidates.len());

        // 2. Process each candidate
        for segment_id in candidates {
            if let Err(e) = self.tier_segment(&segment_id) {
                tracing::warn!(segment_id = %segment_id, error = %e, "Failed to tier segment");
                self.stats.record_failure();
            }
        }

        Ok(())
    }

    fn tier_segment(&self, segment_id: &str) -> Result<(), EngineError> {
        self.tier_segment_with_class(segment_id, SegmentTier::Cold, S3StorageClass::Standard)
    }

    fn tier_segment_with_class(
        &self,
        segment_id: &str,
        to_tier: SegmentTier,
        _storage_class: S3StorageClass,
    ) -> Result<(), EngineError> {
        // Double check state and get fresh metadata (race condition protection)
        // Also helps avoid holding lock during I/O
        let entry_clone = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .entries
                .iter()
                .find(|e| e.segment_id == segment_id)
                .cloned()
                .ok_or_else(|| EngineError::NotFound(format!("segment {} missing", segment_id)))?
        };

        // If validation fails (e.g. already at target tier), skip
        if entry_clone.tier == to_tier {
            return Ok(());
        }

        let data_size = entry_clone.size_bytes;

        // Handle different tier transitions
        match to_tier {
            SegmentTier::Cold => {
                // Load data from LOCAL storage
                let data = self.storage.load_segment(&entry_clone)?;

                // Write to COLD storage (S3)
                // TODO: Use storage_class when object_store supports it
                self.storage.persist_segment_cold(segment_id, &data)?;

                // Update Manifest and Delete Local File
                {
                    let mut manifest = self.manifest.write().unwrap();
                    if let Some(entry) = manifest
                        .entries
                        .iter_mut()
                        .find(|e| e.segment_id == segment_id)
                    {
                        entry.tier = SegmentTier::Cold;
                        persist_manifest(&self.manifest_path, &manifest)?;

                        // Delete local file
                        let local_path = self.segments_dir.join(format!("{}.ipc", segment_id));
                        if let Err(e) = std::fs::remove_file(&local_path) {
                            tracing::warn!(path = ?local_path, error = %e, "Failed to remove tiered local segment");
                        }

                        tracing::info!(segment_id = %segment_id, "Successfully tiered to Cold storage");
                    } else {
                        tracing::warn!(segment_id = %segment_id, "Segment removed during tiering");
                    }
                }

                self.stats.record_tier_transition(data_size, SegmentTier::Cold);
            }
            SegmentTier::Warm => {
                // Hot -> Warm transition (just update manifest, data stays local)
                let mut manifest = self.manifest.write().unwrap();
                if let Some(entry) = manifest
                    .entries
                    .iter_mut()
                    .find(|e| e.segment_id == segment_id)
                {
                    entry.tier = SegmentTier::Warm;
                    persist_manifest(&self.manifest_path, &manifest)?;
                    tracing::info!(segment_id = %segment_id, "Transitioned to Warm tier");
                }

                self.stats.record_tier_transition(data_size, SegmentTier::Warm);
            }
            SegmentTier::Hot => {
                // Promoting cold data back to hot (prefetch)
                self.prefetch_to_hot(segment_id)?;
            }
        }

        Ok(())
    }

    /// Prefetch cold data back to hot tier
    pub fn prefetch_to_hot(&self, segment_id: &str) -> Result<(), EngineError> {
        let entry_clone = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .entries
                .iter()
                .find(|e| e.segment_id == segment_id)
                .cloned()
                .ok_or_else(|| EngineError::NotFound(format!("segment {} missing", segment_id)))?
        };

        if entry_clone.tier != SegmentTier::Cold {
            return Ok(()); // Already hot/warm
        }

        // Load from cold storage
        let data = self.storage.load_segment(&entry_clone)?;

        // Write to local storage
        self.storage.persist_segment_local(segment_id, &data)?;

        // Update manifest
        {
            let mut manifest = self.manifest.write().unwrap();
            if let Some(entry) = manifest
                .entries
                .iter_mut()
                .find(|e| e.segment_id == segment_id)
            {
                entry.tier = SegmentTier::Hot;
                persist_manifest(&self.manifest_path, &manifest)?;
                tracing::info!(segment_id = %segment_id, "Prefetched from Cold to Hot tier");
            }
        }

        self.stats.record_cold_retrieval(data.len() as u64);
        Ok(())
    }

    /// Load segment with caching for cold data
    pub fn load_segment_cached(&self, entry: &ManifestEntry) -> Result<Vec<u8>, EngineError> {
        if entry.tier == SegmentTier::Cold {
            // Check cache first
            if let Some(data) = self.cold_cache.get(&entry.segment_id) {
                self.stats.cold_cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(data);
            }

            self.stats.cold_cache_misses.fetch_add(1, Ordering::Relaxed);

            // Load from cold storage
            let data = self.storage.load_segment(entry)?;
            self.stats.record_cold_retrieval(data.len() as u64);

            // Cache for future access
            self.cold_cache.put(&entry.segment_id, data.clone());

            Ok(data)
        } else {
            self.storage.load_segment(entry)
        }
    }

    /// Record segment access for access-based tiering policies
    pub fn record_access(&self, segment_id: &str) {
        self.stats.record_access(segment_id);
    }

    /// Get tier distribution summary
    pub fn get_tier_distribution(&self) -> TierDistribution {
        let manifest = self.manifest.read().unwrap();
        let mut dist = TierDistribution::default();

        for entry in &manifest.entries {
            match entry.tier {
                SegmentTier::Hot => {
                    dist.hot_segments += 1;
                    dist.hot_bytes += entry.size_bytes;
                }
                SegmentTier::Warm => {
                    dist.warm_segments += 1;
                    dist.warm_bytes += entry.size_bytes;
                }
                SegmentTier::Cold => {
                    dist.cold_segments += 1;
                    dist.cold_bytes += entry.size_bytes;
                }
            }
        }

        dist
    }
}

/// Summary of data distribution across tiers
#[derive(Debug, Clone, Default)]
pub struct TierDistribution {
    pub hot_segments: u64,
    pub hot_bytes: u64,
    pub warm_segments: u64,
    pub warm_bytes: u64,
    pub cold_segments: u64,
    pub cold_bytes: u64,
}

impl TierDistribution {
    pub fn total_segments(&self) -> u64 {
        self.hot_segments + self.warm_segments + self.cold_segments
    }

    pub fn total_bytes(&self) -> u64 {
        self.hot_bytes + self.warm_bytes + self.cold_bytes
    }

    pub fn cold_percentage(&self) -> f64 {
        let total = self.total_bytes();
        if total == 0 {
            0.0
        } else {
            (self.cold_bytes as f64 / total as f64) * 100.0
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_storage_class() {
        assert_eq!(S3StorageClass::Standard.as_str(), "STANDARD");
        assert_eq!(S3StorageClass::StandardIA.as_str(), "STANDARD_IA");
        assert_eq!(S3StorageClass::Glacier.as_str(), "GLACIER");
        assert_eq!(S3StorageClass::DeepArchive.as_str(), "DEEP_ARCHIVE");

        assert_eq!(S3StorageClass::Standard.retrieval_time_secs(), 0);
        assert_eq!(S3StorageClass::Glacier.retrieval_time_secs(), 180);
        assert_eq!(S3StorageClass::DeepArchive.retrieval_time_secs(), 43200);
    }

    #[test]
    fn test_transition_condition_age() {
        let segment = SegmentInfo {
            segment_id: "seg1".to_string(),
            database: "db".to_string(),
            table: "tbl".to_string(),
            tier: SegmentTier::Hot,
            size_bytes: 1000,
            row_count: 100,
            age_millis: 5000,
            watermark_micros: 0,
        };
        let stats = TieringStats::new();

        let cond_below = TransitionCondition::AgeExceeds(10000);
        let cond_above = TransitionCondition::AgeExceeds(1000);

        assert!(!cond_below.evaluate(&segment, &stats));
        assert!(cond_above.evaluate(&segment, &stats));
    }

    #[test]
    fn test_transition_condition_size() {
        let segment = SegmentInfo {
            segment_id: "seg1".to_string(),
            database: "db".to_string(),
            table: "tbl".to_string(),
            tier: SegmentTier::Hot,
            size_bytes: 5000,
            row_count: 100,
            age_millis: 1000,
            watermark_micros: 0,
        };
        let stats = TieringStats::new();

        let exceeds = TransitionCondition::SizeExceeds(3000);
        let below = TransitionCondition::SizeBelow(10000);

        assert!(exceeds.evaluate(&segment, &stats));
        assert!(below.evaluate(&segment, &stats));
    }

    #[test]
    fn test_lifecycle_policy_matching() {
        let policy = LifecyclePolicy {
            name: "test".to_string(),
            database_filter: Some("analytics".to_string()),
            table_filter: None,
            rules: vec![],
            priority: 0,
            enabled: true,
        };

        let matching_segment = SegmentInfo {
            segment_id: "seg1".to_string(),
            database: "analytics".to_string(),
            table: "events".to_string(),
            tier: SegmentTier::Hot,
            size_bytes: 1000,
            row_count: 100,
            age_millis: 1000,
            watermark_micros: 0,
        };

        let non_matching_segment = SegmentInfo {
            segment_id: "seg2".to_string(),
            database: "production".to_string(),
            table: "users".to_string(),
            tier: SegmentTier::Hot,
            size_bytes: 1000,
            row_count: 100,
            age_millis: 1000,
            watermark_micros: 0,
        };

        assert!(policy.matches(&matching_segment));
        assert!(!policy.matches(&non_matching_segment));
    }

    #[test]
    fn test_lifecycle_policy_rule_finding() {
        let stats = TieringStats::new();
        let policy = LifecyclePolicy {
            name: "test".to_string(),
            database_filter: None,
            table_filter: None,
            rules: vec![
                LifecycleRule {
                    from_tier: SegmentTier::Hot,
                    to_tier: SegmentTier::Warm,
                    conditions: vec![TransitionCondition::AgeExceeds(1000)],
                    storage_class: S3StorageClass::Standard,
                },
                LifecycleRule {
                    from_tier: SegmentTier::Warm,
                    to_tier: SegmentTier::Cold,
                    conditions: vec![TransitionCondition::AgeExceeds(5000)],
                    storage_class: S3StorageClass::StandardIA,
                },
            ],
            priority: 0,
            enabled: true,
        };

        let hot_old = SegmentInfo {
            segment_id: "seg1".to_string(),
            database: "db".to_string(),
            table: "tbl".to_string(),
            tier: SegmentTier::Hot,
            size_bytes: 1000,
            row_count: 100,
            age_millis: 2000,
            watermark_micros: 0,
        };

        let warm_old = SegmentInfo {
            segment_id: "seg2".to_string(),
            database: "db".to_string(),
            table: "tbl".to_string(),
            tier: SegmentTier::Warm,
            size_bytes: 1000,
            row_count: 100,
            age_millis: 6000,
            watermark_micros: 0,
        };

        let rule1 = policy.find_applicable_rule(&hot_old, &stats);
        assert!(rule1.is_some());
        assert_eq!(rule1.unwrap().to_tier, SegmentTier::Warm);

        let rule2 = policy.find_applicable_rule(&warm_old, &stats);
        assert!(rule2.is_some());
        assert_eq!(rule2.unwrap().to_tier, SegmentTier::Cold);
    }

    #[test]
    fn test_cold_data_cache() {
        let cache = ColdDataCache::new(1000);

        cache.put("seg1", vec![1, 2, 3, 4, 5]);
        cache.put("seg2", vec![6, 7, 8, 9, 10]);

        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.current_size(), 10);

        let data = cache.get("seg1");
        assert!(data.is_some());
        assert_eq!(data.unwrap(), vec![1, 2, 3, 4, 5]);

        cache.invalidate("seg1");
        assert_eq!(cache.entry_count(), 1);
        assert!(cache.get("seg1").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = ColdDataCache::new(100);

        // Fill cache
        for i in 0..10 {
            cache.put(&format!("seg{}", i), vec![0; 15]);
        }

        // Cache should have evicted older entries
        assert!(cache.current_size() <= 100);
        assert!(cache.entry_count() <= 7); // ~100/15 = 6.6
    }

    #[test]
    fn test_tiering_stats() {
        let stats = TieringStats::new();

        stats.record_tier_transition(1000, SegmentTier::Cold);
        stats.record_tier_transition(500, SegmentTier::Warm);
        stats.record_failure();
        stats.record_cold_retrieval(200);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.bytes_tiered_to_cold, 1000);
        assert_eq!(snapshot.bytes_tiered_to_warm, 500);
        assert_eq!(snapshot.segments_tiered, 2);
        assert_eq!(snapshot.tiering_failures, 1);
        assert_eq!(snapshot.bytes_retrieved_cold, 200);
    }

    #[test]
    fn test_access_tracking() {
        let stats = TieringStats::new();

        stats.record_access("seg1");
        stats.record_access("seg1");
        stats.record_access("seg1");

        // Should have 3 recent accesses
        let count = stats.get_recent_accesses("seg1", 60000); // 1 minute window
        assert_eq!(count, 3);

        // Unknown segment should have 0
        let count2 = stats.get_recent_accesses("unknown", 60000);
        assert_eq!(count2, 0);
    }

    #[test]
    fn test_default_policies() {
        let default = LifecyclePolicy::default_policy();
        assert_eq!(default.name, "default");
        assert_eq!(default.rules.len(), 2);

        let cost = LifecyclePolicy::cost_optimized();
        assert_eq!(cost.name, "cost_optimized");
        assert_eq!(cost.rules.len(), 1);
        assert_eq!(cost.rules[0].storage_class, S3StorageClass::OneZoneIA);

        let perf = LifecyclePolicy::performance_optimized();
        assert_eq!(perf.name, "performance_optimized");
        assert_eq!(perf.rules.len(), 2);

        let archive = LifecyclePolicy::archive_policy();
        assert_eq!(archive.rules[0].storage_class, S3StorageClass::DeepArchive);
    }

    #[test]
    fn test_tier_distribution() {
        let dist = TierDistribution {
            hot_segments: 10,
            hot_bytes: 1_000_000,
            warm_segments: 5,
            warm_bytes: 500_000,
            cold_segments: 20,
            cold_bytes: 5_000_000,
        };

        assert_eq!(dist.total_segments(), 35);
        assert_eq!(dist.total_bytes(), 6_500_000);

        // Cold percentage should be ~76.9%
        let cold_pct = dist.cold_percentage();
        assert!(cold_pct > 76.0 && cold_pct < 77.0);
    }
}
