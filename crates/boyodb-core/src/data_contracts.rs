//! Data Contracts Module
//!
//! Provides schema evolution and compatibility checking for BoyoDB.
//! Features:
//! - Schema versioning
//! - Backward/Forward compatibility checks
//! - Schema migration planning
//! - Contract validation
//! - Breaking change detection

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Data contract definition
#[derive(Debug, Clone)]
pub struct DataContract {
    /// Contract ID
    pub id: String,
    /// Contract name
    pub name: String,
    /// Description
    pub description: String,
    /// Schema definition
    pub schema: SchemaDefinition,
    /// Version
    pub version: SemanticVersion,
    /// Compatibility mode
    pub compatibility: CompatibilityMode,
    /// Validation rules
    pub validations: Vec<ValidationRule>,
    /// Owner
    pub owner: String,
    /// Tags
    pub tags: Vec<String>,
    /// Created timestamp
    pub created_at: u64,
    /// Modified timestamp
    pub modified_at: u64,
}

impl DataContract {
    /// Create new contract
    pub fn new(id: &str, name: &str, schema: SchemaDefinition) -> Self {
        let now = current_timestamp();
        DataContract {
            id: id.to_string(),
            name: name.to_string(),
            description: String::new(),
            schema,
            version: SemanticVersion::new(1, 0, 0),
            compatibility: CompatibilityMode::Backward,
            validations: Vec::new(),
            owner: String::new(),
            tags: Vec::new(),
            created_at: now,
            modified_at: now,
        }
    }

    /// Add validation rule
    pub fn with_validation(mut self, rule: ValidationRule) -> Self {
        self.validations.push(rule);
        self
    }

    /// Set compatibility mode
    pub fn with_compatibility(mut self, mode: CompatibilityMode) -> Self {
        self.compatibility = mode;
        self
    }
}

/// Semantic version
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SemanticVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SemanticVersion {
    /// Create new version
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        SemanticVersion { major, minor, patch }
    }

    /// Parse version string
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err("Invalid version format".to_string());
        }

        Ok(SemanticVersion {
            major: parts[0].parse().map_err(|_| "Invalid major version")?,
            minor: parts[1].parse().map_err(|_| "Invalid minor version")?,
            patch: parts[2].parse().map_err(|_| "Invalid patch version")?,
        })
    }

    /// Increment major version
    pub fn bump_major(&self) -> Self {
        SemanticVersion {
            major: self.major + 1,
            minor: 0,
            patch: 0,
        }
    }

    /// Increment minor version
    pub fn bump_minor(&self) -> Self {
        SemanticVersion {
            major: self.major,
            minor: self.minor + 1,
            patch: 0,
        }
    }

    /// Increment patch version
    pub fn bump_patch(&self) -> Self {
        SemanticVersion {
            major: self.major,
            minor: self.minor,
            patch: self.patch + 1,
        }
    }

    /// Check if this version is compatible with another
    pub fn is_compatible(&self, other: &SemanticVersion) -> bool {
        self.major == other.major
    }
}

impl std::fmt::Display for SemanticVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Schema definition
#[derive(Debug, Clone)]
pub struct SchemaDefinition {
    /// Schema name
    pub name: String,
    /// Namespace
    pub namespace: Option<String>,
    /// Fields
    pub fields: Vec<FieldDefinition>,
    /// Primary key columns
    pub primary_key: Vec<String>,
    /// Indexes
    pub indexes: Vec<IndexDefinition>,
}

impl SchemaDefinition {
    /// Create new schema
    pub fn new(name: &str) -> Self {
        SchemaDefinition {
            name: name.to_string(),
            namespace: None,
            fields: Vec::new(),
            primary_key: Vec::new(),
            indexes: Vec::new(),
        }
    }

    /// Add field
    pub fn add_field(&mut self, field: FieldDefinition) {
        self.fields.push(field);
    }

    /// Get field by name
    pub fn get_field(&self, name: &str) -> Option<&FieldDefinition> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get field names
    pub fn field_names(&self) -> Vec<&String> {
        self.fields.iter().map(|f| &f.name).collect()
    }
}

/// Field definition
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDefinition {
    /// Field name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default: Option<String>,
    /// Description
    pub description: Option<String>,
    /// Is deprecated
    pub deprecated: bool,
    /// Deprecation message
    pub deprecation_message: Option<String>,
}

impl FieldDefinition {
    /// Create new field
    pub fn new(name: &str, data_type: DataType) -> Self {
        FieldDefinition {
            name: name.to_string(),
            data_type,
            nullable: true,
            default: None,
            description: None,
            deprecated: false,
            deprecation_message: None,
        }
    }

    /// Set as required
    pub fn required(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set default value
    pub fn with_default(mut self, value: &str) -> Self {
        self.default = Some(value.to_string());
        self
    }

    /// Mark as deprecated
    pub fn deprecate(mut self, message: &str) -> Self {
        self.deprecated = true;
        self.deprecation_message = Some(message.to_string());
        self
    }
}

/// Data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// Boolean
    Boolean,
    /// Integer (8-bit)
    Int8,
    /// Integer (16-bit)
    Int16,
    /// Integer (32-bit)
    Int32,
    /// Integer (64-bit)
    Int64,
    /// Float (32-bit)
    Float32,
    /// Float (64-bit)
    Float64,
    /// Decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// String
    String,
    /// Binary
    Binary,
    /// Date
    Date,
    /// Time
    Time,
    /// Timestamp
    Timestamp,
    /// UUID
    Uuid,
    /// JSON
    Json,
    /// Array
    Array(Box<DataType>),
    /// Map
    Map { key: Box<DataType>, value: Box<DataType> },
    /// Struct
    Struct(Vec<FieldDefinition>),
    /// Union of types
    Union(Vec<DataType>),
}

impl DataType {
    /// Check if types are promotable (widening)
    pub fn can_promote_to(&self, target: &DataType) -> bool {
        match (self, target) {
            (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => true,
            (DataType::Int16, DataType::Int32 | DataType::Int64) => true,
            (DataType::Int32, DataType::Int64) => true,
            (DataType::Float32, DataType::Float64) => true,
            (DataType::String, DataType::String) => true,
            _ => self == target,
        }
    }

    /// Get type name
    pub fn name(&self) -> String {
        match self {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int8 => "INT8".to_string(),
            DataType::Int16 => "INT16".to_string(),
            DataType::Int32 => "INT32".to_string(),
            DataType::Int64 => "INT64".to_string(),
            DataType::Float32 => "FLOAT32".to_string(),
            DataType::Float64 => "FLOAT64".to_string(),
            DataType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
            DataType::String => "STRING".to_string(),
            DataType::Binary => "BINARY".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Uuid => "UUID".to_string(),
            DataType::Json => "JSON".to_string(),
            DataType::Array(inner) => format!("ARRAY<{}>", inner.name()),
            DataType::Map { key, value } => format!("MAP<{},{}>", key.name(), value.name()),
            DataType::Struct(_) => "STRUCT".to_string(),
            DataType::Union(_) => "UNION".to_string(),
        }
    }
}

/// Index definition
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,
    /// Columns
    pub columns: Vec<String>,
    /// Is unique
    pub unique: bool,
}

/// Compatibility modes
#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityMode {
    /// New schema can read data written by old schema
    Backward,
    /// Old schema can read data written by new schema
    Forward,
    /// Both backward and forward compatible
    Full,
    /// No compatibility guarantees
    None,
}

/// Validation rule
#[derive(Debug, Clone)]
pub struct ValidationRule {
    /// Rule name
    pub name: String,
    /// Rule type
    pub rule_type: ValidationRuleType,
    /// Is required
    pub required: bool,
    /// Error message
    pub message: Option<String>,
}

/// Validation rule types
#[derive(Debug, Clone)]
pub enum ValidationRuleType {
    /// Field must not be null
    NotNull { field: String },
    /// Field must be unique
    Unique { fields: Vec<String> },
    /// Field must be in range
    Range { field: String, min: Option<f64>, max: Option<f64> },
    /// Field must match pattern
    Pattern { field: String, regex: String },
    /// Field length constraint
    Length { field: String, min: Option<usize>, max: Option<usize> },
    /// Custom SQL expression
    Expression { sql: String },
    /// Foreign key reference
    ForeignKey { field: String, references: String },
}

/// Schema change
#[derive(Debug, Clone)]
pub struct SchemaChange {
    /// Change type
    pub change_type: ChangeType,
    /// Path to changed element
    pub path: String,
    /// Old value (if applicable)
    pub old_value: Option<String>,
    /// New value (if applicable)
    pub new_value: Option<String>,
    /// Is breaking change
    pub is_breaking: bool,
    /// Suggested migration
    pub migration: Option<String>,
}

/// Change types
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeType {
    /// Field added
    FieldAdded,
    /// Field removed
    FieldRemoved,
    /// Field type changed
    FieldTypeChanged,
    /// Field made nullable
    FieldMadeNullable,
    /// Field made required
    FieldMadeRequired,
    /// Default value changed
    DefaultChanged,
    /// Index added
    IndexAdded,
    /// Index removed
    IndexRemoved,
    /// Primary key changed
    PrimaryKeyChanged,
    /// Field renamed
    FieldRenamed,
    /// Field deprecated
    FieldDeprecated,
}

/// Compatibility check result
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Is compatible
    pub compatible: bool,
    /// Compatibility mode checked
    pub mode: CompatibilityMode,
    /// Changes detected
    pub changes: Vec<SchemaChange>,
    /// Breaking changes
    pub breaking_changes: Vec<SchemaChange>,
    /// Warnings
    pub warnings: Vec<String>,
    /// Suggested version bump
    pub suggested_version_bump: VersionBump,
}

/// Version bump type
#[derive(Debug, Clone, PartialEq)]
pub enum VersionBump {
    /// No version change needed
    None,
    /// Patch version bump (bug fixes, no schema changes)
    Patch,
    /// Minor version bump (backward compatible changes)
    Minor,
    /// Major version bump (breaking changes)
    Major,
}

/// Schema compatibility checker
pub struct CompatibilityChecker;

impl CompatibilityChecker {
    /// Check compatibility between two schemas
    pub fn check(
        old_schema: &SchemaDefinition,
        new_schema: &SchemaDefinition,
        mode: &CompatibilityMode,
    ) -> CompatibilityResult {
        let mut changes = Vec::new();
        let mut warnings = Vec::new();

        let old_fields: HashMap<&String, &FieldDefinition> = old_schema
            .fields
            .iter()
            .map(|f| (&f.name, f))
            .collect();

        let new_fields: HashMap<&String, &FieldDefinition> = new_schema
            .fields
            .iter()
            .map(|f| (&f.name, f))
            .collect();

        // Check for removed fields
        for (name, old_field) in &old_fields {
            if !new_fields.contains_key(name) {
                changes.push(SchemaChange {
                    change_type: ChangeType::FieldRemoved,
                    path: (*name).clone(),
                    old_value: Some(old_field.data_type.name()),
                    new_value: None,
                    is_breaking: matches!(mode, CompatibilityMode::Backward | CompatibilityMode::Full),
                    migration: Some(format!("ALTER TABLE DROP COLUMN {}", name)),
                });
            }
        }

        // Check for added fields
        for (name, new_field) in &new_fields {
            if !old_fields.contains_key(name) {
                let is_breaking = !new_field.nullable && new_field.default.is_none() &&
                    matches!(mode, CompatibilityMode::Forward | CompatibilityMode::Full);

                changes.push(SchemaChange {
                    change_type: ChangeType::FieldAdded,
                    path: (*name).clone(),
                    old_value: None,
                    new_value: Some(new_field.data_type.name()),
                    is_breaking,
                    migration: Some(format!(
                        "ALTER TABLE ADD COLUMN {} {}",
                        name,
                        new_field.data_type.name()
                    )),
                });

                if is_breaking {
                    warnings.push(format!(
                        "Adding non-nullable field '{}' without default is a breaking change",
                        name
                    ));
                }
            }
        }

        // Check for field changes
        for (name, old_field) in &old_fields {
            if let Some(new_field) = new_fields.get(name) {
                // Type change
                if old_field.data_type != new_field.data_type {
                    let can_promote = old_field.data_type.can_promote_to(&new_field.data_type);

                    changes.push(SchemaChange {
                        change_type: ChangeType::FieldTypeChanged,
                        path: (*name).clone(),
                        old_value: Some(old_field.data_type.name()),
                        new_value: Some(new_field.data_type.name()),
                        is_breaking: !can_promote,
                        migration: Some(format!(
                            "ALTER TABLE ALTER COLUMN {} TYPE {}",
                            name,
                            new_field.data_type.name()
                        )),
                    });
                }

                // Nullability change
                if old_field.nullable && !new_field.nullable {
                    changes.push(SchemaChange {
                        change_type: ChangeType::FieldMadeRequired,
                        path: (*name).clone(),
                        old_value: Some("nullable".to_string()),
                        new_value: Some("required".to_string()),
                        is_breaking: matches!(mode, CompatibilityMode::Forward | CompatibilityMode::Full),
                        migration: Some(format!(
                            "ALTER TABLE ALTER COLUMN {} SET NOT NULL",
                            name
                        )),
                    });
                } else if !old_field.nullable && new_field.nullable {
                    changes.push(SchemaChange {
                        change_type: ChangeType::FieldMadeNullable,
                        path: (*name).clone(),
                        old_value: Some("required".to_string()),
                        new_value: Some("nullable".to_string()),
                        is_breaking: matches!(mode, CompatibilityMode::Backward | CompatibilityMode::Full),
                        migration: Some(format!(
                            "ALTER TABLE ALTER COLUMN {} DROP NOT NULL",
                            name
                        )),
                    });
                }

                // Deprecation
                if !old_field.deprecated && new_field.deprecated {
                    changes.push(SchemaChange {
                        change_type: ChangeType::FieldDeprecated,
                        path: (*name).clone(),
                        old_value: None,
                        new_value: new_field.deprecation_message.clone(),
                        is_breaking: false,
                        migration: None,
                    });

                    warnings.push(format!(
                        "Field '{}' has been deprecated: {}",
                        name,
                        new_field.deprecation_message.as_deref().unwrap_or("No message")
                    ));
                }
            }
        }

        // Check primary key changes
        if old_schema.primary_key != new_schema.primary_key {
            changes.push(SchemaChange {
                change_type: ChangeType::PrimaryKeyChanged,
                path: "primary_key".to_string(),
                old_value: Some(old_schema.primary_key.join(", ")),
                new_value: Some(new_schema.primary_key.join(", ")),
                is_breaking: true,
                migration: None,
            });
        }

        let breaking_changes: Vec<SchemaChange> = changes
            .iter()
            .filter(|c| c.is_breaking)
            .cloned()
            .collect();

        let compatible = breaking_changes.is_empty();

        let suggested_version_bump = if !breaking_changes.is_empty() {
            VersionBump::Major
        } else if !changes.is_empty() {
            VersionBump::Minor
        } else {
            VersionBump::None
        };

        CompatibilityResult {
            compatible,
            mode: mode.clone(),
            changes,
            breaking_changes,
            warnings,
            suggested_version_bump,
        }
    }
}

/// Contract registry
pub struct ContractRegistry {
    /// Contracts by ID
    contracts: HashMap<String, DataContract>,
    /// Contract versions
    versions: HashMap<String, Vec<SemanticVersion>>,
}

impl ContractRegistry {
    /// Create new registry
    pub fn new() -> Self {
        ContractRegistry {
            contracts: HashMap::new(),
            versions: HashMap::new(),
        }
    }

    /// Register contract
    pub fn register(&mut self, contract: DataContract) -> Result<(), String> {
        let key = format!("{}:{}", contract.id, contract.version);

        if self.contracts.contains_key(&key) {
            return Err(format!("Contract {} version {} already exists", contract.id, contract.version));
        }

        // Check compatibility with previous version
        if let Some(versions) = self.versions.get(&contract.id) {
            if let Some(prev_version) = versions.iter().max() {
                let prev_key = format!("{}:{}", contract.id, prev_version);
                if let Some(prev_contract) = self.contracts.get(&prev_key) {
                    let result = CompatibilityChecker::check(
                        &prev_contract.schema,
                        &contract.schema,
                        &contract.compatibility,
                    );

                    if !result.compatible {
                        return Err(format!(
                            "Contract {} version {} is not compatible with version {}: {:?}",
                            contract.id,
                            contract.version,
                            prev_version,
                            result.breaking_changes.iter().map(|c| &c.path).collect::<Vec<_>>()
                        ));
                    }
                }
            }
        }

        // Add version
        self.versions
            .entry(contract.id.clone())
            .or_default()
            .push(contract.version.clone());

        self.contracts.insert(key, contract);
        Ok(())
    }

    /// Get contract by ID and version
    pub fn get(&self, id: &str, version: &SemanticVersion) -> Option<&DataContract> {
        let key = format!("{}:{}", id, version);
        self.contracts.get(&key)
    }

    /// Get latest version of contract
    pub fn get_latest(&self, id: &str) -> Option<&DataContract> {
        self.versions
            .get(id)
            .and_then(|versions| versions.iter().max())
            .and_then(|version| {
                let key = format!("{}:{}", id, version);
                self.contracts.get(&key)
            })
    }

    /// List all versions for contract
    pub fn list_versions(&self, id: &str) -> Vec<&SemanticVersion> {
        self.versions
            .get(id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// List all contracts
    pub fn list_contracts(&self) -> Vec<&str> {
        self.versions.keys().map(|s| s.as_str()).collect()
    }

    /// Check if data matches contract
    pub fn validate(&self, contract_id: &str, version: &SemanticVersion, data: &HashMap<String, String>) -> ValidationResult {
        let mut errors = Vec::new();

        if let Some(contract) = self.get(contract_id, version) {
            for field in &contract.schema.fields {
                let value = data.get(&field.name);

                // Check required fields
                if !field.nullable && value.is_none() && field.default.is_none() {
                    errors.push(format!("Missing required field: {}", field.name));
                }
            }

            // Check for unknown fields
            let known_fields: HashSet<&String> = contract.schema.fields.iter().map(|f| &f.name).collect();
            for key in data.keys() {
                if !known_fields.contains(key) {
                    errors.push(format!("Unknown field: {}", key));
                }
            }
        } else {
            errors.push(format!("Contract {} version {} not found", contract_id, version));
        }

        ValidationResult {
            valid: errors.is_empty(),
            errors,
        }
    }
}

impl Default for ContractRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Is valid
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
}

/// Thread-safe contract registry
pub struct ContractStore {
    registry: Arc<RwLock<ContractRegistry>>,
}

impl ContractStore {
    /// Create new store
    pub fn new() -> Self {
        ContractStore {
            registry: Arc::new(RwLock::new(ContractRegistry::new())),
        }
    }

    /// Register contract
    pub fn register(&self, contract: DataContract) -> Result<(), String> {
        self.registry
            .write()
            .map_err(|e| e.to_string())?
            .register(contract)
    }

    /// Get latest contract
    pub fn get_latest(&self, id: &str) -> Result<Option<DataContract>, String> {
        let registry = self.registry.read().map_err(|e| e.to_string())?;
        Ok(registry.get_latest(id).cloned())
    }

    /// Validate data
    pub fn validate(&self, contract_id: &str, version: &SemanticVersion, data: &HashMap<String, String>) -> Result<ValidationResult, String> {
        let registry = self.registry.read().map_err(|e| e.to_string())?;
        Ok(registry.validate(contract_id, version, data))
    }
}

impl Default for ContractStore {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semantic_version() {
        let v1 = SemanticVersion::new(1, 0, 0);
        let v2 = SemanticVersion::new(1, 1, 0);
        let v3 = SemanticVersion::new(2, 0, 0);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1.is_compatible(&v2));
        assert!(!v1.is_compatible(&v3));
    }

    #[test]
    fn test_schema_compatibility() {
        let mut old_schema = SchemaDefinition::new("users");
        old_schema.add_field(FieldDefinition::new("id", DataType::Int64).required());
        old_schema.add_field(FieldDefinition::new("name", DataType::String));

        let mut new_schema = SchemaDefinition::new("users");
        new_schema.add_field(FieldDefinition::new("id", DataType::Int64).required());
        new_schema.add_field(FieldDefinition::new("name", DataType::String));
        new_schema.add_field(FieldDefinition::new("email", DataType::String)); // New nullable field

        let result = CompatibilityChecker::check(
            &old_schema,
            &new_schema,
            &CompatibilityMode::Backward,
        );

        assert!(result.compatible);
        assert_eq!(result.suggested_version_bump, VersionBump::Minor);
    }

    #[test]
    fn test_breaking_change_detection() {
        let mut old_schema = SchemaDefinition::new("users");
        old_schema.add_field(FieldDefinition::new("id", DataType::Int64));

        let mut new_schema = SchemaDefinition::new("users");
        // Type changed from Int64 to String - breaking
        new_schema.add_field(FieldDefinition::new("id", DataType::String));

        let result = CompatibilityChecker::check(
            &old_schema,
            &new_schema,
            &CompatibilityMode::Backward,
        );

        assert!(!result.compatible);
        assert_eq!(result.suggested_version_bump, VersionBump::Major);
    }

    #[test]
    fn test_type_promotion() {
        assert!(DataType::Int8.can_promote_to(&DataType::Int16));
        assert!(DataType::Int16.can_promote_to(&DataType::Int32));
        assert!(DataType::Int32.can_promote_to(&DataType::Int64));
        assert!(DataType::Float32.can_promote_to(&DataType::Float64));
        assert!(!DataType::Int64.can_promote_to(&DataType::Int32)); // Can't narrow
    }

    #[test]
    fn test_contract_registry() {
        let mut registry = ContractRegistry::new();

        let mut schema = SchemaDefinition::new("users");
        schema.add_field(FieldDefinition::new("id", DataType::Int64).required());
        schema.add_field(FieldDefinition::new("name", DataType::String));

        let contract = DataContract::new("users-contract", "Users Contract", schema);
        registry.register(contract).unwrap();

        let latest = registry.get_latest("users-contract");
        assert!(latest.is_some());
    }

    #[test]
    fn test_validation() {
        let mut registry = ContractRegistry::new();

        let mut schema = SchemaDefinition::new("users");
        schema.add_field(FieldDefinition::new("id", DataType::Int64).required());
        schema.add_field(FieldDefinition::new("name", DataType::String));

        let contract = DataContract::new("test", "Test", schema);
        let version = contract.version.clone();
        registry.register(contract).unwrap();

        // Valid data
        let mut data = HashMap::new();
        data.insert("id".to_string(), "123".to_string());
        data.insert("name".to_string(), "Test".to_string());

        let result = registry.validate("test", &version, &data);
        assert!(result.valid);

        // Missing required field
        let mut invalid_data = HashMap::new();
        invalid_data.insert("name".to_string(), "Test".to_string());

        let result = registry.validate("test", &version, &invalid_data);
        assert!(!result.valid);
    }
}
