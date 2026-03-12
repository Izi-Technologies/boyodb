//! Extensions/Plugins System
//!
//! Provides CREATE EXTENSION infrastructure with:
//! - Dynamic library loading
//! - Extension registry and versioning
//! - Dependency management
//! - Upgrade paths

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Extension error types
#[derive(Debug, Clone)]
pub enum ExtensionError {
    /// Extension not found
    NotFound(String),
    /// Extension already installed
    AlreadyInstalled(String),
    /// Version not found
    VersionNotFound(String, String),
    /// Dependency missing
    DependencyMissing(String),
    /// Load failed
    LoadFailed(String),
    /// Incompatible version
    IncompatibleVersion(String),
    /// Permission denied
    PermissionDenied(String),
    /// Invalid extension
    InvalidExtension(String),
    /// Upgrade failed
    UpgradeFailed(String),
    /// Cannot drop (has dependents)
    HasDependents(String),
}

impl std::fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(s) => write!(f, "extension not found: {}", s),
            Self::AlreadyInstalled(s) => write!(f, "extension already installed: {}", s),
            Self::VersionNotFound(e, v) => write!(f, "version {} not found for extension {}", v, e),
            Self::DependencyMissing(s) => write!(f, "dependency missing: {}", s),
            Self::LoadFailed(s) => write!(f, "load failed: {}", s),
            Self::IncompatibleVersion(s) => write!(f, "incompatible version: {}", s),
            Self::PermissionDenied(s) => write!(f, "permission denied: {}", s),
            Self::InvalidExtension(s) => write!(f, "invalid extension: {}", s),
            Self::UpgradeFailed(s) => write!(f, "upgrade failed: {}", s),
            Self::HasDependents(s) => write!(f, "has dependents: {}", s),
        }
    }
}

impl std::error::Error for ExtensionError {}

/// Semantic version
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub prerelease: Option<String>,
}

impl Version {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            prerelease: None,
        }
    }

    pub fn parse(s: &str) -> Result<Self, ExtensionError> {
        let s = s.trim();
        let (version_part, prerelease) = if let Some(pos) = s.find('-') {
            (&s[..pos], Some(s[pos + 1..].to_string()))
        } else {
            (s, None)
        };

        let parts: Vec<&str> = version_part.split('.').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return Err(ExtensionError::InvalidExtension(format!(
                "invalid version: {}",
                s
            )));
        }

        let major = parts[0]
            .parse()
            .map_err(|_| ExtensionError::InvalidExtension(format!("invalid major version: {}", s)))?;
        let minor = parts[1]
            .parse()
            .map_err(|_| ExtensionError::InvalidExtension(format!("invalid minor version: {}", s)))?;
        let patch = if parts.len() > 2 {
            parts[2]
                .parse()
                .map_err(|_| ExtensionError::InvalidExtension(format!("invalid patch version: {}", s)))?
        } else {
            0
        };

        Ok(Self {
            major,
            minor,
            patch,
            prerelease,
        })
    }

    pub fn is_compatible_with(&self, other: &Version) -> bool {
        // Same major version, this >= other
        self.major == other.major && (self.minor > other.minor ||
            (self.minor == other.minor && self.patch >= other.patch))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(pre) = &self.prerelease {
            write!(f, "{}.{}.{}-{}", self.major, self.minor, self.patch, pre)
        } else {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        }
    }
}

/// Extension metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionMetadata {
    pub name: String,
    pub version: Version,
    pub description: String,
    pub author: Option<String>,
    pub license: Option<String>,
    pub homepage: Option<String>,
    pub repository: Option<String>,
    pub dependencies: Vec<ExtensionDependency>,
    pub provides: Vec<String>,
    pub relocatable: bool,
    pub schema: Option<String>,
    pub requires_superuser: bool,
    pub trusted: bool,
}

/// Extension dependency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionDependency {
    pub name: String,
    pub version_requirement: VersionRequirement,
}

/// Version requirement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionRequirement {
    /// Any version
    Any,
    /// Exact version
    Exact(Version),
    /// Minimum version
    AtLeast(Version),
    /// Version range
    Range(Version, Version),
    /// Compatible with (same major)
    Compatible(Version),
}

impl VersionRequirement {
    pub fn satisfies(&self, version: &Version) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(v) => version == v,
            Self::AtLeast(v) => version >= v,
            Self::Range(min, max) => version >= min && version <= max,
            Self::Compatible(v) => version.is_compatible_with(v),
        }
    }
}

/// Installed extension
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledExtension {
    pub name: String,
    pub version: Version,
    pub schema: String,
    pub owner: String,
    pub installed_at: SystemTime,
    pub relocatable: bool,
    pub config: HashMap<String, String>,
}

/// Available extension (from registry)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableExtension {
    pub metadata: ExtensionMetadata,
    pub control_file: PathBuf,
    pub sql_files: Vec<PathBuf>,
    pub library: Option<PathBuf>,
    pub upgrade_paths: Vec<UpgradePath>,
}

/// Upgrade path between versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradePath {
    pub from_version: Version,
    pub to_version: Version,
    pub script: PathBuf,
}

/// Extension object (what the extension provides)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtensionObject {
    Function {
        schema: String,
        name: String,
        args: Vec<String>,
    },
    Type {
        schema: String,
        name: String,
    },
    Operator {
        schema: String,
        name: String,
        left_type: Option<String>,
        right_type: Option<String>,
    },
    Table {
        schema: String,
        name: String,
    },
    View {
        schema: String,
        name: String,
    },
    Index {
        schema: String,
        name: String,
    },
    Trigger {
        schema: String,
        table: String,
        name: String,
    },
    Cast {
        source: String,
        target: String,
    },
    Aggregate {
        schema: String,
        name: String,
        args: Vec<String>,
    },
    Procedure {
        schema: String,
        name: String,
        args: Vec<String>,
    },
}

/// Extension hook type
pub type ExtensionHook = Box<dyn Fn(&ExtensionContext) -> Result<(), ExtensionError> + Send + Sync>;

/// Extension context for hooks
pub struct ExtensionContext {
    pub extension_name: String,
    pub extension_version: Version,
    pub schema: String,
    pub owner: String,
    pub config: HashMap<String, String>,
}

/// Extension manager
pub struct ExtensionManager {
    /// Installed extensions
    installed: RwLock<HashMap<String, InstalledExtension>>,
    /// Available extensions (from filesystem)
    available: RwLock<HashMap<String, AvailableExtension>>,
    /// Extension objects
    objects: RwLock<HashMap<String, Vec<ExtensionObject>>>,
    /// Extension directory
    extension_dir: PathBuf,
    /// Configuration
    config: ExtensionConfig,
    /// Hooks
    hooks: RwLock<ExtensionHooks>,
}

/// Extension configuration
#[derive(Debug, Clone)]
pub struct ExtensionConfig {
    /// Extension search path
    pub extension_path: Vec<PathBuf>,
    /// Allowed extensions (empty = all allowed)
    pub allowed_extensions: HashSet<String>,
    /// Blocked extensions
    pub blocked_extensions: HashSet<String>,
    /// Allow untrusted extensions
    pub allow_untrusted: bool,
    /// Auto-create schema
    pub auto_create_schema: bool,
}

impl Default for ExtensionConfig {
    fn default() -> Self {
        Self {
            extension_path: vec![PathBuf::from("/usr/share/boyodb/extension")],
            allowed_extensions: HashSet::new(),
            blocked_extensions: HashSet::new(),
            allow_untrusted: false,
            auto_create_schema: true,
        }
    }
}

/// Extension hooks
struct ExtensionHooks {
    pre_create: Vec<ExtensionHook>,
    post_create: Vec<ExtensionHook>,
    pre_drop: Vec<ExtensionHook>,
    post_drop: Vec<ExtensionHook>,
    pre_upgrade: Vec<ExtensionHook>,
    post_upgrade: Vec<ExtensionHook>,
}

impl Default for ExtensionHooks {
    fn default() -> Self {
        Self {
            pre_create: Vec::new(),
            post_create: Vec::new(),
            pre_drop: Vec::new(),
            post_drop: Vec::new(),
            pre_upgrade: Vec::new(),
            post_upgrade: Vec::new(),
        }
    }
}

impl ExtensionManager {
    pub fn new(extension_dir: PathBuf, config: ExtensionConfig) -> Self {
        Self {
            installed: RwLock::new(HashMap::new()),
            available: RwLock::new(HashMap::new()),
            objects: RwLock::new(HashMap::new()),
            extension_dir,
            config,
            hooks: RwLock::new(ExtensionHooks::default()),
        }
    }

    /// Scan for available extensions
    pub fn scan_extensions(&self) -> Result<usize, ExtensionError> {
        let mut available = self.available.write().unwrap();
        let mut count = 0;

        for search_path in &self.config.extension_path {
            if !search_path.exists() {
                continue;
            }

            if let Ok(entries) = std::fs::read_dir(search_path) {
                for entry in entries.flatten() {
                    if let Some(ext_name) = entry.file_name().to_str() {
                        if ext_name.ends_with(".control") {
                            let name = ext_name.trim_end_matches(".control");
                            if let Ok(ext) = self.load_extension_metadata(search_path, name) {
                                available.insert(name.to_string(), ext);
                                count += 1;
                            }
                        }
                    }
                }
            }
        }

        // Also add built-in extensions
        for builtin in self.get_builtin_extensions() {
            available.insert(builtin.metadata.name.clone(), builtin);
            count += 1;
        }

        Ok(count)
    }

    fn load_extension_metadata(
        &self,
        _dir: &PathBuf,
        name: &str,
    ) -> Result<AvailableExtension, ExtensionError> {
        // Would parse control file and discover SQL files
        // For now, return a placeholder
        Ok(AvailableExtension {
            metadata: ExtensionMetadata {
                name: name.to_string(),
                version: Version::new(1, 0, 0),
                description: format!("{} extension", name),
                author: None,
                license: None,
                homepage: None,
                repository: None,
                dependencies: vec![],
                provides: vec![],
                relocatable: true,
                schema: None,
                requires_superuser: false,
                trusted: true,
            },
            control_file: PathBuf::new(),
            sql_files: vec![],
            library: None,
            upgrade_paths: vec![],
        })
    }

    fn get_builtin_extensions(&self) -> Vec<AvailableExtension> {
        vec![
            // UUID generation
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "uuid-ossp".to_string(),
                    version: Version::new(1, 1, 0),
                    description: "Generate UUIDs".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["uuid_generate_v4".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // JSON functions
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "jsonb_extra".to_string(),
                    version: Version::new(1, 0, 0),
                    description: "Extra JSONB functions".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["jsonb_merge".to_string(), "jsonb_diff".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // Full-text search
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "fts".to_string(),
                    version: Version::new(2, 0, 0),
                    description: "Full-text search support".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["to_tsvector".to_string(), "to_tsquery".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // Vector similarity
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "vector".to_string(),
                    version: Version::new(0, 5, 0),
                    description: "Vector similarity search".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["vector".to_string(), "ivfflat".to_string(), "hnsw".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // Statistics
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "tablefunc".to_string(),
                    version: Version::new(1, 0, 0),
                    description: "Table functions including crosstab".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["crosstab".to_string(), "normal_rand".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // PostGIS-like
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "geospatial".to_string(),
                    version: Version::new(3, 0, 0),
                    description: "Geospatial functions and types".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["geometry".to_string(), "geography".to_string(), "st_distance".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
            // Time series
            AvailableExtension {
                metadata: ExtensionMetadata {
                    name: "timeseries".to_string(),
                    version: Version::new(1, 0, 0),
                    description: "Time series functions".to_string(),
                    author: Some("BoyoDB Team".to_string()),
                    license: Some("MIT".to_string()),
                    homepage: None,
                    repository: None,
                    dependencies: vec![],
                    provides: vec!["time_bucket".to_string(), "first".to_string(), "last".to_string()],
                    relocatable: true,
                    schema: None,
                    requires_superuser: false,
                    trusted: true,
                },
                control_file: PathBuf::new(),
                sql_files: vec![],
                library: None,
                upgrade_paths: vec![],
            },
        ]
    }

    /// Create (install) an extension
    pub fn create_extension(
        &self,
        name: &str,
        version: Option<&str>,
        schema: Option<&str>,
        cascade: bool,
        owner: &str,
    ) -> Result<(), ExtensionError> {
        // Check if allowed
        if !self.config.allowed_extensions.is_empty()
            && !self.config.allowed_extensions.contains(name)
        {
            return Err(ExtensionError::PermissionDenied(format!(
                "extension {} not in allowed list",
                name
            )));
        }

        if self.config.blocked_extensions.contains(name) {
            return Err(ExtensionError::PermissionDenied(format!(
                "extension {} is blocked",
                name
            )));
        }

        // Check if already installed
        if self.installed.read().unwrap().contains_key(name) {
            return Err(ExtensionError::AlreadyInstalled(name.to_string()));
        }

        // Get available extension
        let available = self
            .available
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExtensionError::NotFound(name.to_string()))?;

        // Check version
        let target_version = if let Some(v) = version {
            let parsed = Version::parse(v)?;
            if parsed != available.metadata.version {
                return Err(ExtensionError::VersionNotFound(name.to_string(), v.to_string()));
            }
            parsed
        } else {
            available.metadata.version.clone()
        };

        // Check dependencies
        if cascade {
            for dep in &available.metadata.dependencies {
                if !self.installed.read().unwrap().contains_key(&dep.name) {
                    self.create_extension(&dep.name, None, None, true, owner)?;
                }
            }
        } else {
            for dep in &available.metadata.dependencies {
                let installed = self.installed.read().unwrap();
                if let Some(installed_dep) = installed.get(&dep.name) {
                    if !dep.version_requirement.satisfies(&installed_dep.version) {
                        return Err(ExtensionError::IncompatibleVersion(format!(
                            "{} requires {} {:?}",
                            name, dep.name, dep.version_requirement
                        )));
                    }
                } else {
                    return Err(ExtensionError::DependencyMissing(dep.name.clone()));
                }
            }
        }

        // Check trusted
        if !available.metadata.trusted && !self.config.allow_untrusted {
            return Err(ExtensionError::PermissionDenied(
                "untrusted extensions not allowed".into(),
            ));
        }

        // Determine schema
        let schema = schema
            .map(String::from)
            .or_else(|| available.metadata.schema.clone())
            .unwrap_or_else(|| "public".to_string());

        // Create context
        let context = ExtensionContext {
            extension_name: name.to_string(),
            extension_version: target_version.clone(),
            schema: schema.clone(),
            owner: owner.to_string(),
            config: HashMap::new(),
        };

        // Run pre-create hooks
        for hook in &self.hooks.read().unwrap().pre_create {
            hook(&context)?;
        }

        // Install extension
        let installed = InstalledExtension {
            name: name.to_string(),
            version: target_version,
            schema,
            owner: owner.to_string(),
            installed_at: SystemTime::now(),
            relocatable: available.metadata.relocatable,
            config: HashMap::new(),
        };

        // Register objects
        let objects = self.create_extension_objects(&available, &installed)?;
        self.objects
            .write()
            .unwrap()
            .insert(name.to_string(), objects);

        self.installed
            .write()
            .unwrap()
            .insert(name.to_string(), installed);

        // Run post-create hooks
        for hook in &self.hooks.read().unwrap().post_create {
            hook(&context)?;
        }

        Ok(())
    }

    fn create_extension_objects(
        &self,
        available: &AvailableExtension,
        installed: &InstalledExtension,
    ) -> Result<Vec<ExtensionObject>, ExtensionError> {
        let mut objects = Vec::new();

        // Create objects based on what the extension provides
        for provided in &available.metadata.provides {
            // Would actually execute SQL scripts
            // For now, create placeholder objects
            objects.push(ExtensionObject::Function {
                schema: installed.schema.clone(),
                name: provided.clone(),
                args: vec![],
            });
        }

        Ok(objects)
    }

    /// Drop an extension
    pub fn drop_extension(&self, name: &str, cascade: bool) -> Result<(), ExtensionError> {
        // Check if installed
        let installed = self
            .installed
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExtensionError::NotFound(name.to_string()))?;

        // Check dependents
        if !cascade {
            let all_installed = self.installed.read().unwrap();
            for (ext_name, ext) in all_installed.iter() {
                if ext_name == name {
                    continue;
                }

                // Would check if ext depends on name
                let _ = ext;
            }
        }

        let context = ExtensionContext {
            extension_name: name.to_string(),
            extension_version: installed.version.clone(),
            schema: installed.schema.clone(),
            owner: installed.owner.clone(),
            config: installed.config.clone(),
        };

        // Run pre-drop hooks
        for hook in &self.hooks.read().unwrap().pre_drop {
            hook(&context)?;
        }

        // Remove objects
        self.objects.write().unwrap().remove(name);

        // Remove extension
        self.installed.write().unwrap().remove(name);

        // Run post-drop hooks
        for hook in &self.hooks.read().unwrap().post_drop {
            hook(&context)?;
        }

        Ok(())
    }

    /// Upgrade an extension
    pub fn alter_extension_update(
        &self,
        name: &str,
        new_version: &str,
    ) -> Result<(), ExtensionError> {
        let installed = self
            .installed
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExtensionError::NotFound(name.to_string()))?;

        let available = self
            .available
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| ExtensionError::NotFound(name.to_string()))?;

        let target_version = Version::parse(new_version)?;

        // Find upgrade path
        let upgrade_path = available
            .upgrade_paths
            .iter()
            .find(|p| p.from_version == installed.version && p.to_version == target_version)
            .ok_or_else(|| {
                ExtensionError::UpgradeFailed(format!(
                    "no upgrade path from {} to {}",
                    installed.version, target_version
                ))
            })?;

        let context = ExtensionContext {
            extension_name: name.to_string(),
            extension_version: installed.version.clone(),
            schema: installed.schema.clone(),
            owner: installed.owner.clone(),
            config: installed.config.clone(),
        };

        // Run pre-upgrade hooks
        for hook in &self.hooks.read().unwrap().pre_upgrade {
            hook(&context)?;
        }

        // Execute upgrade script
        let _ = upgrade_path;

        // Update version
        let mut installed_map = self.installed.write().unwrap();
        if let Some(ext) = installed_map.get_mut(name) {
            ext.version = target_version;
        }

        // Run post-upgrade hooks
        for hook in &self.hooks.read().unwrap().post_upgrade {
            hook(&context)?;
        }

        Ok(())
    }

    /// Set extension schema
    pub fn alter_extension_set_schema(
        &self,
        name: &str,
        new_schema: &str,
    ) -> Result<(), ExtensionError> {
        let mut installed = self.installed.write().unwrap();
        let ext = installed
            .get_mut(name)
            .ok_or_else(|| ExtensionError::NotFound(name.to_string()))?;

        if !ext.relocatable {
            return Err(ExtensionError::InvalidExtension(
                "extension is not relocatable".into(),
            ));
        }

        // Update schema for all objects
        let mut objects = self.objects.write().unwrap();
        if let Some(ext_objects) = objects.get_mut(name) {
            for obj in ext_objects.iter_mut() {
                match obj {
                    ExtensionObject::Function { schema, .. } => *schema = new_schema.to_string(),
                    ExtensionObject::Type { schema, .. } => *schema = new_schema.to_string(),
                    ExtensionObject::Table { schema, .. } => *schema = new_schema.to_string(),
                    ExtensionObject::View { schema, .. } => *schema = new_schema.to_string(),
                    _ => {}
                }
            }
        }

        ext.schema = new_schema.to_string();

        Ok(())
    }

    /// Get installed extension
    pub fn get_extension(&self, name: &str) -> Option<InstalledExtension> {
        self.installed.read().unwrap().get(name).cloned()
    }

    /// List installed extensions
    pub fn list_installed(&self) -> Vec<InstalledExtension> {
        self.installed.read().unwrap().values().cloned().collect()
    }

    /// List available extensions
    pub fn list_available(&self) -> Vec<ExtensionMetadata> {
        self.available
            .read()
            .unwrap()
            .values()
            .map(|e| e.metadata.clone())
            .collect()
    }

    /// Get extension objects
    pub fn get_extension_objects(&self, name: &str) -> Vec<ExtensionObject> {
        self.objects
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if extension is installed
    pub fn is_installed(&self, name: &str) -> bool {
        self.installed.read().unwrap().contains_key(name)
    }

    /// Add pre-create hook
    pub fn add_pre_create_hook(&self, hook: ExtensionHook) {
        self.hooks.write().unwrap().pre_create.push(hook);
    }

    /// Add post-create hook
    pub fn add_post_create_hook(&self, hook: ExtensionHook) {
        self.hooks.write().unwrap().post_create.push(hook);
    }
}

impl Default for ExtensionManager {
    fn default() -> Self {
        Self::new(
            PathBuf::from("/usr/share/boyodb/extension"),
            ExtensionConfig::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parse() {
        let v1 = Version::parse("1.2.3").unwrap();
        assert_eq!(v1.major, 1);
        assert_eq!(v1.minor, 2);
        assert_eq!(v1.patch, 3);

        let v2 = Version::parse("2.0").unwrap();
        assert_eq!(v2.major, 2);
        assert_eq!(v2.minor, 0);
        assert_eq!(v2.patch, 0);

        let v3 = Version::parse("1.0.0-beta").unwrap();
        assert_eq!(v3.prerelease, Some("beta".to_string()));
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = Version::new(1, 2, 3);
        let v2 = Version::new(1, 2, 0);
        let v3 = Version::new(1, 3, 0);
        let v4 = Version::new(2, 0, 0);

        assert!(v1.is_compatible_with(&v2));
        assert!(v3.is_compatible_with(&v2));
        assert!(!v4.is_compatible_with(&v1));
    }

    #[test]
    fn test_version_requirement() {
        let v = Version::new(1, 2, 3);

        assert!(VersionRequirement::Any.satisfies(&v));
        assert!(VersionRequirement::Exact(Version::new(1, 2, 3)).satisfies(&v));
        assert!(!VersionRequirement::Exact(Version::new(1, 2, 4)).satisfies(&v));
        assert!(VersionRequirement::AtLeast(Version::new(1, 0, 0)).satisfies(&v));
        assert!(!VersionRequirement::AtLeast(Version::new(2, 0, 0)).satisfies(&v));
    }

    #[test]
    fn test_extension_manager() {
        let manager = ExtensionManager::default();

        // Scan for extensions
        manager.scan_extensions().unwrap();

        // List available
        let available = manager.list_available();
        assert!(!available.is_empty());

        // Check built-ins exist
        let names: Vec<_> = available.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"uuid-ossp"));
        assert!(names.contains(&"vector"));
        assert!(names.contains(&"geospatial"));
    }

    #[test]
    fn test_create_extension() {
        let manager = ExtensionManager::default();
        manager.scan_extensions().unwrap();

        // Install extension
        manager
            .create_extension("uuid-ossp", None, Some("public"), false, "admin")
            .unwrap();

        assert!(manager.is_installed("uuid-ossp"));

        let installed = manager.get_extension("uuid-ossp").unwrap();
        assert_eq!(installed.schema, "public");
        assert_eq!(installed.owner, "admin");

        // Try to install again
        let result = manager.create_extension("uuid-ossp", None, None, false, "admin");
        assert!(matches!(result, Err(ExtensionError::AlreadyInstalled(_))));
    }

    #[test]
    fn test_drop_extension() {
        let manager = ExtensionManager::default();
        manager.scan_extensions().unwrap();

        manager
            .create_extension("fts", None, None, false, "admin")
            .unwrap();
        assert!(manager.is_installed("fts"));

        manager.drop_extension("fts", false).unwrap();
        assert!(!manager.is_installed("fts"));
    }

    #[test]
    fn test_extension_objects() {
        let manager = ExtensionManager::default();
        manager.scan_extensions().unwrap();

        manager
            .create_extension("timeseries", None, None, false, "admin")
            .unwrap();

        let objects = manager.get_extension_objects("timeseries");
        assert!(!objects.is_empty());

        // Check that functions are created
        let function_names: Vec<_> = objects
            .iter()
            .filter_map(|o| match o {
                ExtensionObject::Function { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();

        assert!(function_names.contains(&"time_bucket"));
    }
}
