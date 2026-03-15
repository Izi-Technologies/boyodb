//! User-Defined Composite Types
//!
//! Provides CREATE TYPE support for:
//! - Composite types (structs)
//! - Enum types
//! - Domain types (constrained base types)
//! - Range types
//! - Array types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::SystemTime;

/// User type error
#[derive(Debug, Clone)]
pub enum UserTypeError {
    /// Type not found
    TypeNotFound(String),
    /// Type already exists
    TypeAlreadyExists(String),
    /// Invalid type definition
    InvalidDefinition(String),
    /// Constraint violation
    ConstraintViolation(String),
    /// Circular dependency
    CircularDependency(String),
    /// Cannot drop type (has dependents)
    HasDependents(String),
    /// Cast not found
    CastNotFound(String, String),
    /// Invalid value for type
    InvalidValue(String),
}

impl std::fmt::Display for UserTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeNotFound(s) => write!(f, "type not found: {}", s),
            Self::TypeAlreadyExists(s) => write!(f, "type already exists: {}", s),
            Self::InvalidDefinition(s) => write!(f, "invalid type definition: {}", s),
            Self::ConstraintViolation(s) => write!(f, "constraint violation: {}", s),
            Self::CircularDependency(s) => write!(f, "circular dependency: {}", s),
            Self::HasDependents(s) => write!(f, "type has dependents: {}", s),
            Self::CastNotFound(a, b) => write!(f, "no cast from {} to {}", a, b),
            Self::InvalidValue(s) => write!(f, "invalid value: {}", s),
        }
    }
}

impl std::error::Error for UserTypeError {}

/// Type identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TypeId {
    pub schema: String,
    pub name: String,
}

impl TypeId {
    pub fn new(schema: &str, name: &str) -> Self {
        Self {
            schema: schema.to_string(),
            name: name.to_string(),
        }
    }

    pub fn simple(name: &str) -> Self {
        Self::new("public", name)
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

impl std::fmt::Display for TypeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

/// User-defined type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserType {
    pub id: TypeId,
    pub owner: String,
    pub type_kind: TypeKind,
    pub created_at: SystemTime,
    pub comment: Option<String>,
}

/// Kind of user type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeKind {
    /// Composite type (struct)
    Composite(CompositeType),
    /// Enum type
    Enum(EnumType),
    /// Domain type (constrained base type)
    Domain(DomainType),
    /// Range type
    Range(RangeType),
    /// Array type (explicit)
    Array(ArrayType),
}

/// Composite type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeType {
    pub fields: Vec<CompositeField>,
}

/// Field in composite type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeField {
    pub name: String,
    pub data_type: FieldType,
    pub nullable: bool,
    pub default: Option<FieldValue>,
    pub comment: Option<String>,
}

/// Field type reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    /// Built-in scalar type
    Builtin(BuiltinType),
    /// Reference to another user type
    UserType(TypeId),
    /// Array of type
    Array(Box<FieldType>),
    /// Nullable wrapper
    Nullable(Box<FieldType>),
}

/// Built-in types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuiltinType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal,
    String,
    Bytes,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Jsonb,
}

impl BuiltinType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::UInt8 => "UINT8",
            Self::UInt16 => "UINT16",
            Self::UInt32 => "UINT32",
            Self::UInt64 => "UINT64",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Decimal => "DECIMAL",
            Self::String => "STRING",
            Self::Bytes => "BYTES",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::TimestampTz => "TIMESTAMPTZ",
            Self::Interval => "INTERVAL",
            Self::Uuid => "UUID",
            Self::Json => "JSON",
            Self::Jsonb => "JSONB",
        }
    }
}

/// Field value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Composite(HashMap<String, FieldValue>),
    Array(Vec<FieldValue>),
    Enum(String),
    Expression(String),
}

/// Enum type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumType {
    pub labels: Vec<String>,
}

impl EnumType {
    pub fn new(labels: Vec<String>) -> Self {
        Self { labels }
    }

    pub fn contains(&self, label: &str) -> bool {
        self.labels.contains(&label.to_string())
    }

    pub fn ordinal(&self, label: &str) -> Option<usize> {
        self.labels.iter().position(|l| l == label)
    }
}

/// Domain type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainType {
    pub base_type: FieldType,
    pub constraints: Vec<DomainConstraint>,
    pub default: Option<FieldValue>,
    pub not_null: bool,
}

/// Domain constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainConstraint {
    pub name: Option<String>,
    pub check: String,
}

/// Range type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeType {
    pub subtype: FieldType,
    pub subtype_diff: Option<String>,
    pub canonical: Option<String>,
}

/// Array type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayType {
    pub element_type: FieldType,
    pub dimensions: Option<Vec<i32>>,
}

/// Type cast definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeCast {
    pub source: TypeId,
    pub target: TypeId,
    pub function: Option<String>,
    pub context: CastContext,
    pub method: CastMethod,
}

/// Cast context
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CastContext {
    /// Implicit (automatic)
    Implicit,
    /// Requires explicit CAST
    Assignment,
    /// Only with explicit CAST
    Explicit,
}

/// Cast method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CastMethod {
    /// Binary compatible
    Binary,
    /// Function call
    Function(String),
    /// I/O conversion
    InOut,
}

/// User type manager
pub struct UserTypeManager {
    /// Type definitions
    types: RwLock<HashMap<TypeId, UserType>>,
    /// Type casts
    casts: RwLock<HashMap<(TypeId, TypeId), TypeCast>>,
    /// Type dependencies (type -> dependents)
    dependencies: RwLock<HashMap<TypeId, Vec<TypeId>>>,
    /// Built-in type registry
    builtins: HashMap<String, BuiltinType>,
}

impl UserTypeManager {
    pub fn new() -> Self {
        let mut builtins = HashMap::new();
        builtins.insert("BOOLEAN".to_string(), BuiltinType::Boolean);
        builtins.insert("BOOL".to_string(), BuiltinType::Boolean);
        builtins.insert("INT8".to_string(), BuiltinType::Int8);
        builtins.insert("TINYINT".to_string(), BuiltinType::Int8);
        builtins.insert("INT16".to_string(), BuiltinType::Int16);
        builtins.insert("SMALLINT".to_string(), BuiltinType::Int16);
        builtins.insert("INT32".to_string(), BuiltinType::Int32);
        builtins.insert("INT".to_string(), BuiltinType::Int32);
        builtins.insert("INTEGER".to_string(), BuiltinType::Int32);
        builtins.insert("INT64".to_string(), BuiltinType::Int64);
        builtins.insert("BIGINT".to_string(), BuiltinType::Int64);
        builtins.insert("UINT8".to_string(), BuiltinType::UInt8);
        builtins.insert("UINT16".to_string(), BuiltinType::UInt16);
        builtins.insert("UINT32".to_string(), BuiltinType::UInt32);
        builtins.insert("UINT64".to_string(), BuiltinType::UInt64);
        builtins.insert("FLOAT32".to_string(), BuiltinType::Float32);
        builtins.insert("FLOAT".to_string(), BuiltinType::Float32);
        builtins.insert("REAL".to_string(), BuiltinType::Float32);
        builtins.insert("FLOAT64".to_string(), BuiltinType::Float64);
        builtins.insert("DOUBLE".to_string(), BuiltinType::Float64);
        builtins.insert("DECIMAL".to_string(), BuiltinType::Decimal);
        builtins.insert("NUMERIC".to_string(), BuiltinType::Decimal);
        builtins.insert("STRING".to_string(), BuiltinType::String);
        builtins.insert("TEXT".to_string(), BuiltinType::String);
        builtins.insert("VARCHAR".to_string(), BuiltinType::String);
        builtins.insert("BYTES".to_string(), BuiltinType::Bytes);
        builtins.insert("BYTEA".to_string(), BuiltinType::Bytes);
        builtins.insert("BLOB".to_string(), BuiltinType::Bytes);
        builtins.insert("DATE".to_string(), BuiltinType::Date);
        builtins.insert("TIME".to_string(), BuiltinType::Time);
        builtins.insert("TIMESTAMP".to_string(), BuiltinType::Timestamp);
        builtins.insert("TIMESTAMPTZ".to_string(), BuiltinType::TimestampTz);
        builtins.insert("INTERVAL".to_string(), BuiltinType::Interval);
        builtins.insert("UUID".to_string(), BuiltinType::Uuid);
        builtins.insert("JSON".to_string(), BuiltinType::Json);
        builtins.insert("JSONB".to_string(), BuiltinType::Jsonb);

        Self {
            types: RwLock::new(HashMap::new()),
            casts: RwLock::new(HashMap::new()),
            dependencies: RwLock::new(HashMap::new()),
            builtins,
        }
    }

    /// Create a composite type
    pub fn create_composite_type(
        &self,
        id: TypeId,
        fields: Vec<CompositeField>,
        owner: &str,
    ) -> Result<(), UserTypeError> {
        // Validate fields
        for field in &fields {
            self.validate_field_type(&field.data_type)?;
        }

        let user_type = UserType {
            id: id.clone(),
            owner: owner.to_string(),
            type_kind: TypeKind::Composite(CompositeType { fields }),
            created_at: SystemTime::now(),
            comment: None,
        };

        self.register_type(user_type)?;

        // Track dependencies
        self.update_dependencies(&id)?;

        Ok(())
    }

    /// Create an enum type
    pub fn create_enum_type(
        &self,
        id: TypeId,
        labels: Vec<String>,
        owner: &str,
    ) -> Result<(), UserTypeError> {
        if labels.is_empty() {
            return Err(UserTypeError::InvalidDefinition(
                "enum must have at least one label".into(),
            ));
        }

        // Check for duplicates
        let mut seen = std::collections::HashSet::new();
        for label in &labels {
            if !seen.insert(label) {
                return Err(UserTypeError::InvalidDefinition(format!(
                    "duplicate enum label: {}",
                    label
                )));
            }
        }

        let user_type = UserType {
            id: id.clone(),
            owner: owner.to_string(),
            type_kind: TypeKind::Enum(EnumType::new(labels)),
            created_at: SystemTime::now(),
            comment: None,
        };

        self.register_type(user_type)
    }

    /// Add label to enum type
    pub fn alter_enum_add_value(
        &self,
        id: &TypeId,
        new_value: &str,
        before: Option<&str>,
        after: Option<&str>,
    ) -> Result<(), UserTypeError> {
        let mut types = self.types.write();
        let user_type = types
            .get_mut(id)
            .ok_or_else(|| UserTypeError::TypeNotFound(id.full_name()))?;

        match &mut user_type.type_kind {
            TypeKind::Enum(enum_type) => {
                if enum_type.contains(new_value) {
                    return Err(UserTypeError::InvalidDefinition(format!(
                        "enum label already exists: {}",
                        new_value
                    )));
                }

                if let Some(before_val) = before {
                    let pos = enum_type
                        .ordinal(before_val)
                        .ok_or_else(|| UserTypeError::InvalidValue(before_val.to_string()))?;
                    enum_type.labels.insert(pos, new_value.to_string());
                } else if let Some(after_val) = after {
                    let pos = enum_type
                        .ordinal(after_val)
                        .ok_or_else(|| UserTypeError::InvalidValue(after_val.to_string()))?;
                    enum_type.labels.insert(pos + 1, new_value.to_string());
                } else {
                    enum_type.labels.push(new_value.to_string());
                }

                Ok(())
            }
            _ => Err(UserTypeError::InvalidDefinition(
                "not an enum type".into(),
            )),
        }
    }

    /// Create a domain type
    pub fn create_domain(
        &self,
        id: TypeId,
        base_type: FieldType,
        constraints: Vec<DomainConstraint>,
        default: Option<FieldValue>,
        not_null: bool,
        owner: &str,
    ) -> Result<(), UserTypeError> {
        self.validate_field_type(&base_type)?;

        let user_type = UserType {
            id: id.clone(),
            owner: owner.to_string(),
            type_kind: TypeKind::Domain(DomainType {
                base_type,
                constraints,
                default,
                not_null,
            }),
            created_at: SystemTime::now(),
            comment: None,
        };

        self.register_type(user_type)?;
        self.update_dependencies(&id)?;

        Ok(())
    }

    /// Create a range type
    pub fn create_range_type(
        &self,
        id: TypeId,
        subtype: FieldType,
        owner: &str,
    ) -> Result<(), UserTypeError> {
        self.validate_field_type(&subtype)?;

        let user_type = UserType {
            id: id.clone(),
            owner: owner.to_string(),
            type_kind: TypeKind::Range(RangeType {
                subtype,
                subtype_diff: None,
                canonical: None,
            }),
            created_at: SystemTime::now(),
            comment: None,
        };

        self.register_type(user_type)?;
        self.update_dependencies(&id)?;

        Ok(())
    }

    /// Drop a user type
    pub fn drop_type(&self, id: &TypeId, cascade: bool) -> Result<(), UserTypeError> {
        // Check dependents
        let deps = self.dependencies.read();
        if let Some(dependents) = deps.get(id) {
            if !dependents.is_empty() && !cascade {
                return Err(UserTypeError::HasDependents(format!(
                    "{} depends on {}",
                    dependents
                        .iter()
                        .map(|d| d.full_name())
                        .collect::<Vec<_>>()
                        .join(", "),
                    id.full_name()
                )));
            }
        }
        drop(deps);

        // Drop dependents if cascade
        if cascade {
            let dependents = self
                .dependencies
                .read()
                .get(id)
                .cloned()
                .unwrap_or_default();

            for dep in dependents {
                self.drop_type(&dep, true)?;
            }
        }

        // Remove type
        let mut types = self.types.write();
        types
            .remove(id)
            .ok_or_else(|| UserTypeError::TypeNotFound(id.full_name()))?;

        // Clean up dependencies
        let mut deps = self.dependencies.write();
        deps.remove(id);
        for dependents in deps.values_mut() {
            dependents.retain(|d| d != id);
        }

        Ok(())
    }

    /// Get a type
    pub fn get_type(&self, id: &TypeId) -> Option<UserType> {
        self.types.read().get(id).cloned()
    }

    /// List all types in schema
    pub fn list_types(&self, schema: &str) -> Vec<UserType> {
        self.types
            .read()
            .values()
            .filter(|t| t.id.schema == schema)
            .cloned()
            .collect()
    }

    /// Check if a type exists
    pub fn type_exists(&self, id: &TypeId) -> bool {
        self.types.read().contains_key(id)
    }

    /// Create a type cast
    pub fn create_cast(&self, cast: TypeCast) -> Result<(), UserTypeError> {
        // Validate source and target types exist
        if !self.is_valid_type(&cast.source) {
            return Err(UserTypeError::TypeNotFound(cast.source.full_name()));
        }
        if !self.is_valid_type(&cast.target) {
            return Err(UserTypeError::TypeNotFound(cast.target.full_name()));
        }

        let mut casts = self.casts.write();
        casts.insert((cast.source.clone(), cast.target.clone()), cast);

        Ok(())
    }

    /// Drop a cast
    pub fn drop_cast(&self, source: &TypeId, target: &TypeId) -> Result<(), UserTypeError> {
        let mut casts = self.casts.write();
        casts
            .remove(&(source.clone(), target.clone()))
            .ok_or_else(|| UserTypeError::CastNotFound(source.full_name(), target.full_name()))?;
        Ok(())
    }

    /// Get a cast
    pub fn get_cast(&self, source: &TypeId, target: &TypeId) -> Option<TypeCast> {
        self.casts
            .read()
            .get(&(source.clone(), target.clone()))
            .cloned()
    }

    /// Check if cast exists
    pub fn can_cast(&self, source: &TypeId, target: &TypeId, context: CastContext) -> bool {
        if let Some(cast) = self.get_cast(source, target) {
            match (cast.context, context) {
                (CastContext::Implicit, _) => true,
                (CastContext::Assignment, CastContext::Explicit) => true,
                (CastContext::Assignment, CastContext::Assignment) => true,
                (CastContext::Explicit, CastContext::Explicit) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Validate a value against a type
    pub fn validate_value(&self, value: &FieldValue, field_type: &FieldType) -> Result<(), UserTypeError> {
        match (value, field_type) {
            (FieldValue::Null, FieldType::Nullable(_)) => Ok(()),
            (FieldValue::Null, _) => Err(UserTypeError::ConstraintViolation(
                "NULL value for non-nullable type".into(),
            )),
            (FieldValue::Boolean(_), FieldType::Builtin(BuiltinType::Boolean)) => Ok(()),
            (FieldValue::Int64(_), FieldType::Builtin(BuiltinType::Int64)) => Ok(()),
            (FieldValue::Float64(_), FieldType::Builtin(BuiltinType::Float64)) => Ok(()),
            (FieldValue::String(_), FieldType::Builtin(BuiltinType::String)) => Ok(()),
            (FieldValue::Bytes(_), FieldType::Builtin(BuiltinType::Bytes)) => Ok(()),
            (FieldValue::Composite(fields), FieldType::UserType(type_id)) => {
                let user_type = self
                    .get_type(type_id)
                    .ok_or_else(|| UserTypeError::TypeNotFound(type_id.full_name()))?;

                match &user_type.type_kind {
                    TypeKind::Composite(composite) => {
                        for field in &composite.fields {
                            if let Some(field_value) = fields.get(&field.name) {
                                self.validate_value(field_value, &field.data_type)?;
                            } else if !field.nullable && field.default.is_none() {
                                return Err(UserTypeError::ConstraintViolation(format!(
                                    "missing required field: {}",
                                    field.name
                                )));
                            }
                        }
                        Ok(())
                    }
                    _ => Err(UserTypeError::InvalidValue("not a composite type".into())),
                }
            }
            (FieldValue::Enum(label), FieldType::UserType(type_id)) => {
                let user_type = self
                    .get_type(type_id)
                    .ok_or_else(|| UserTypeError::TypeNotFound(type_id.full_name()))?;

                match &user_type.type_kind {
                    TypeKind::Enum(enum_type) => {
                        if enum_type.contains(label) {
                            Ok(())
                        } else {
                            Err(UserTypeError::InvalidValue(format!(
                                "invalid enum value: {}",
                                label
                            )))
                        }
                    }
                    _ => Err(UserTypeError::InvalidValue("not an enum type".into())),
                }
            }
            (FieldValue::Array(elements), FieldType::Array(element_type)) => {
                for element in elements {
                    self.validate_value(element, element_type)?;
                }
                Ok(())
            }
            _ => Err(UserTypeError::InvalidValue(format!(
                "type mismatch: {:?} vs {:?}",
                value, field_type
            ))),
        }
    }

    // Private helpers

    fn register_type(&self, user_type: UserType) -> Result<(), UserTypeError> {
        let mut types = self.types.write();

        if types.contains_key(&user_type.id) {
            return Err(UserTypeError::TypeAlreadyExists(user_type.id.full_name()));
        }

        types.insert(user_type.id.clone(), user_type);
        Ok(())
    }

    fn validate_field_type(&self, field_type: &FieldType) -> Result<(), UserTypeError> {
        match field_type {
            FieldType::Builtin(_) => Ok(()),
            FieldType::UserType(id) => {
                if self.type_exists(id) || self.is_builtin_by_id(id) {
                    Ok(())
                } else {
                    Err(UserTypeError::TypeNotFound(id.full_name()))
                }
            }
            FieldType::Array(element_type) => self.validate_field_type(element_type),
            FieldType::Nullable(inner_type) => self.validate_field_type(inner_type),
        }
    }

    fn is_valid_type(&self, id: &TypeId) -> bool {
        self.type_exists(id) || self.is_builtin_by_id(id)
    }

    fn is_builtin_by_id(&self, id: &TypeId) -> bool {
        self.builtins.contains_key(&id.name.to_uppercase())
    }

    fn update_dependencies(&self, _type_id: &TypeId) -> Result<(), UserTypeError> {
        // Would traverse type definition and update dependency graph
        Ok(())
    }

    /// Parse a type name to FieldType
    pub fn parse_type(&self, type_name: &str) -> Result<FieldType, UserTypeError> {
        let type_name_trimmed = type_name.trim();
        let type_name_upper = type_name_trimmed.to_uppercase();

        // Check for array syntax
        if type_name_upper.ends_with("[]") {
            let element_type = self.parse_type(&type_name_trimmed[..type_name_trimmed.len() - 2])?;
            return Ok(FieldType::Array(Box::new(element_type)));
        }

        // Check built-in types (case-insensitive)
        if let Some(builtin) = self.builtins.get(&type_name_upper) {
            return Ok(FieldType::Builtin(*builtin));
        }

        // Check user types (preserve original case for user types)
        let id = if type_name_trimmed.contains('.') {
            let parts: Vec<&str> = type_name_trimmed.splitn(2, '.').collect();
            TypeId::new(parts[0], parts[1])
        } else {
            TypeId::simple(type_name_trimmed)
        };

        if self.type_exists(&id) {
            Ok(FieldType::UserType(id))
        } else {
            Err(UserTypeError::TypeNotFound(type_name_trimmed.to_string()))
        }
    }
}

impl Default for UserTypeManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Type definition builder
pub struct TypeBuilder {
    manager: Arc<UserTypeManager>,
}

impl TypeBuilder {
    pub fn new(manager: Arc<UserTypeManager>) -> Self {
        Self { manager }
    }

    /// Create a composite type builder
    pub fn composite(&self, schema: &str, name: &str) -> CompositeTypeBuilder {
        CompositeTypeBuilder {
            manager: self.manager.clone(),
            id: TypeId::new(schema, name),
            fields: Vec::new(),
            owner: "system".to_string(),
        }
    }

    /// Create an enum type
    pub fn enum_type(
        &self,
        schema: &str,
        name: &str,
        labels: Vec<&str>,
    ) -> Result<(), UserTypeError> {
        self.manager.create_enum_type(
            TypeId::new(schema, name),
            labels.into_iter().map(String::from).collect(),
            "system",
        )
    }

    /// Create a domain type
    pub fn domain(&self, schema: &str, name: &str) -> DomainBuilder {
        DomainBuilder {
            manager: self.manager.clone(),
            id: TypeId::new(schema, name),
            base_type: FieldType::Builtin(BuiltinType::String),
            constraints: Vec::new(),
            default: None,
            not_null: false,
            owner: "system".to_string(),
        }
    }
}

/// Builder for composite types
pub struct CompositeTypeBuilder {
    manager: Arc<UserTypeManager>,
    id: TypeId,
    fields: Vec<CompositeField>,
    owner: String,
}

impl CompositeTypeBuilder {
    pub fn field(mut self, name: &str, data_type: &str, nullable: bool) -> Self {
        if let Ok(field_type) = self.manager.parse_type(data_type) {
            self.fields.push(CompositeField {
                name: name.to_string(),
                data_type: field_type,
                nullable,
                default: None,
                comment: None,
            });
        }
        self
    }

    pub fn field_with_default(
        mut self,
        name: &str,
        data_type: &str,
        nullable: bool,
        default: FieldValue,
    ) -> Self {
        if let Ok(field_type) = self.manager.parse_type(data_type) {
            self.fields.push(CompositeField {
                name: name.to_string(),
                data_type: field_type,
                nullable,
                default: Some(default),
                comment: None,
            });
        }
        self
    }

    pub fn owner(mut self, owner: &str) -> Self {
        self.owner = owner.to_string();
        self
    }

    pub fn build(self) -> Result<(), UserTypeError> {
        self.manager
            .create_composite_type(self.id, self.fields, &self.owner)
    }
}

/// Builder for domain types
pub struct DomainBuilder {
    manager: Arc<UserTypeManager>,
    id: TypeId,
    base_type: FieldType,
    constraints: Vec<DomainConstraint>,
    default: Option<FieldValue>,
    not_null: bool,
    owner: String,
}

impl DomainBuilder {
    pub fn base_type(mut self, type_name: &str) -> Self {
        if let Ok(field_type) = self.manager.parse_type(type_name) {
            self.base_type = field_type;
        }
        self
    }

    pub fn check(mut self, name: &str, expression: &str) -> Self {
        self.constraints.push(DomainConstraint {
            name: Some(name.to_string()),
            check: expression.to_string(),
        });
        self
    }

    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn default(mut self, value: FieldValue) -> Self {
        self.default = Some(value);
        self
    }

    pub fn owner(mut self, owner: &str) -> Self {
        self.owner = owner.to_string();
        self
    }

    pub fn build(self) -> Result<(), UserTypeError> {
        self.manager.create_domain(
            self.id,
            self.base_type,
            self.constraints,
            self.default,
            self.not_null,
            &self.owner,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_type() {
        let manager = Arc::new(UserTypeManager::new());
        let builder = TypeBuilder::new(manager.clone());

        builder
            .composite("public", "address")
            .field("street", "STRING", false)
            .field("city", "STRING", false)
            .field("zip", "STRING", true)
            .field("country", "STRING", false)
            .build()
            .unwrap();

        let address_type = manager.get_type(&TypeId::simple("address")).unwrap();
        assert!(matches!(address_type.type_kind, TypeKind::Composite(_)));

        if let TypeKind::Composite(comp) = &address_type.type_kind {
            assert_eq!(comp.fields.len(), 4);
            assert_eq!(comp.fields[0].name, "street");
        }
    }

    #[test]
    fn test_enum_type() {
        let manager = Arc::new(UserTypeManager::new());
        let builder = TypeBuilder::new(manager.clone());

        builder
            .enum_type("public", "status", vec!["pending", "active", "cancelled"])
            .unwrap();

        let status_type = manager.get_type(&TypeId::simple("status")).unwrap();
        if let TypeKind::Enum(enum_type) = &status_type.type_kind {
            assert_eq!(enum_type.labels.len(), 3);
            assert!(enum_type.contains("active"));
            assert_eq!(enum_type.ordinal("pending"), Some(0));
        }

        // Add new value
        manager
            .alter_enum_add_value(
                &TypeId::simple("status"),
                "completed",
                None,
                Some("active"),
            )
            .unwrap();

        let updated_type = manager.get_type(&TypeId::simple("status")).unwrap();
        if let TypeKind::Enum(enum_type) = &updated_type.type_kind {
            assert_eq!(enum_type.labels.len(), 4);
            assert_eq!(enum_type.ordinal("completed"), Some(2)); // After active
        }
    }

    #[test]
    fn test_domain_type() {
        let manager = Arc::new(UserTypeManager::new());
        let builder = TypeBuilder::new(manager.clone());

        builder
            .domain("public", "email")
            .base_type("STRING")
            .check("valid_email", "VALUE LIKE '%@%'")
            .not_null()
            .build()
            .unwrap();

        let email_type = manager.get_type(&TypeId::simple("email")).unwrap();
        if let TypeKind::Domain(domain) = &email_type.type_kind {
            assert!(domain.not_null);
            assert_eq!(domain.constraints.len(), 1);
        }
    }

    #[test]
    fn test_type_dependencies() {
        let manager = Arc::new(UserTypeManager::new());
        let builder = TypeBuilder::new(manager.clone());

        // Create address type first
        builder
            .composite("public", "address")
            .field("street", "STRING", false)
            .field("city", "STRING", false)
            .build()
            .unwrap();

        // Create person type that uses address
        builder
            .composite("public", "person")
            .field("name", "STRING", false)
            .field("home_address", "public.address", true)
            .build()
            .unwrap();

        let person_type = manager.get_type(&TypeId::simple("person")).unwrap();
        if let TypeKind::Composite(comp) = &person_type.type_kind {
            assert_eq!(comp.fields.len(), 2);
            assert!(matches!(
                &comp.fields[1].data_type,
                FieldType::UserType(_)
            ));
        }
    }

    #[test]
    fn test_parse_type() {
        let manager = UserTypeManager::new();

        // Built-in types
        assert!(matches!(
            manager.parse_type("INT64").unwrap(),
            FieldType::Builtin(BuiltinType::Int64)
        ));
        assert!(matches!(
            manager.parse_type("string").unwrap(),
            FieldType::Builtin(BuiltinType::String)
        ));
        assert!(matches!(
            manager.parse_type("BOOLEAN").unwrap(),
            FieldType::Builtin(BuiltinType::Boolean)
        ));

        // Array types
        let arr_type = manager.parse_type("INT64[]").unwrap();
        assert!(matches!(arr_type, FieldType::Array(_)));
    }

    #[test]
    fn test_validate_value() {
        let manager = Arc::new(UserTypeManager::new());
        let builder = TypeBuilder::new(manager.clone());

        builder
            .enum_type("public", "color", vec!["red", "green", "blue"])
            .unwrap();

        // Valid enum value
        let result = manager.validate_value(
            &FieldValue::Enum("red".to_string()),
            &FieldType::UserType(TypeId::simple("color")),
        );
        assert!(result.is_ok());

        // Invalid enum value
        let result = manager.validate_value(
            &FieldValue::Enum("yellow".to_string()),
            &FieldType::UserType(TypeId::simple("color")),
        );
        assert!(result.is_err());
    }
}
