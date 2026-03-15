//! Extensions API
//!
//! PostgreSQL-compatible extension system for loadable modules.
//! Supports CREATE EXTENSION syntax and extension management.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Extension metadata
#[derive(Debug, Clone)]
pub struct Extension {
    /// Extension name
    pub name: String,
    /// Version
    pub version: String,
    /// Description
    pub description: String,
    /// Schema to install into
    pub schema: String,
    /// Whether extension is relocatable
    pub relocatable: bool,
    /// Required extensions
    pub requires: Vec<String>,
    /// Extension state
    pub state: ExtensionState,
    /// Functions provided
    pub functions: Vec<ExtensionFunction>,
    /// Types provided
    pub types: Vec<ExtensionType>,
    /// Operators provided
    pub operators: Vec<ExtensionOperator>,
}

/// Extension state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionState {
    /// Available but not installed
    Available,
    /// Installed and active
    Installed,
    /// Disabled
    Disabled,
}

/// Function provided by extension
#[derive(Debug, Clone)]
pub struct ExtensionFunction {
    pub name: String,
    pub args: Vec<FunctionArg>,
    pub return_type: String,
    pub volatility: Volatility,
    pub is_aggregate: bool,
    pub description: String,
}

/// Function argument
#[derive(Debug, Clone)]
pub struct FunctionArg {
    pub name: String,
    pub data_type: String,
    pub default: Option<String>,
}

/// Function volatility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

/// Type provided by extension
#[derive(Debug, Clone)]
pub struct ExtensionType {
    pub name: String,
    pub category: TypeCategory,
    pub input_function: String,
    pub output_function: String,
    pub storage: String,
}

/// Type category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeCategory {
    Boolean,
    Numeric,
    String,
    DateTime,
    Geometric,
    Network,
    User,
    Array,
    Composite,
}

/// Operator provided by extension
#[derive(Debug, Clone)]
pub struct ExtensionOperator {
    pub name: String,
    pub left_type: String,
    pub right_type: String,
    pub result_type: String,
    pub function: String,
    pub commutator: Option<String>,
    pub negator: Option<String>,
}

/// Extension registry
pub struct ExtensionRegistry {
    /// Installed extensions
    extensions: RwLock<HashMap<String, Extension>>,
    /// Available extensions (from control files)
    available: RwLock<HashMap<String, ExtensionMetadata>>,
}

/// Extension control file metadata
#[derive(Debug, Clone)]
pub struct ExtensionMetadata {
    pub name: String,
    pub default_version: String,
    pub comment: String,
    pub requires: Vec<String>,
    pub relocatable: bool,
    pub schema: Option<String>,
}

impl ExtensionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            extensions: RwLock::new(HashMap::new()),
            available: RwLock::new(HashMap::new()),
        };
        
        // Register built-in extensions
        registry.register_builtin_extensions();
        registry
    }

    fn register_builtin_extensions(&mut self) {
        // pgcrypto equivalent
        self.register_available(ExtensionMetadata {
            name: "boyocrypto".into(),
            default_version: "1.0".into(),
            comment: "Cryptographic functions".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // uuid-ossp equivalent
        self.register_available(ExtensionMetadata {
            name: "uuid_ossp".into(),
            default_version: "1.0".into(),
            comment: "UUID generation functions".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // PostGIS equivalent
        self.register_available(ExtensionMetadata {
            name: "boyogis".into(),
            default_version: "1.0".into(),
            comment: "Geographic/spatial functions".into(),
            requires: vec![],
            relocatable: false,
            schema: Some("boyogis".into()),
        });

        // pg_stat_statements equivalent
        self.register_available(ExtensionMetadata {
            name: "boyo_stat_statements".into(),
            default_version: "1.0".into(),
            comment: "Query statistics tracking".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // pg_trgm equivalent
        self.register_available(ExtensionMetadata {
            name: "boyo_trgm".into(),
            default_version: "1.0".into(),
            comment: "Trigram text similarity".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // hstore equivalent
        self.register_available(ExtensionMetadata {
            name: "hstore".into(),
            default_version: "1.0".into(),
            comment: "Key-value pair storage".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // vector/pgvector equivalent
        self.register_available(ExtensionMetadata {
            name: "vector".into(),
            default_version: "1.0".into(),
            comment: "Vector similarity search".into(),
            requires: vec![],
            relocatable: true,
            schema: None,
        });

        // timescaledb equivalent
        self.register_available(ExtensionMetadata {
            name: "boyoscale".into(),
            default_version: "1.0".into(),
            comment: "Time-series optimizations".into(),
            requires: vec![],
            relocatable: false,
            schema: Some("boyoscale".into()),
        });
    }

    fn register_available(&mut self, metadata: ExtensionMetadata) {
        self.available.write().insert(metadata.name.clone(), metadata);
    }

    /// Create/install an extension
    pub fn create_extension(
        &self,
        name: &str,
        version: Option<&str>,
        schema: Option<&str>,
        cascade: bool,
    ) -> Result<(), ExtensionError> {
        let available = self.available.read();
        let metadata = available.get(name)
            .ok_or_else(|| ExtensionError::NotFound(name.into()))?;

        // Check dependencies
        if cascade {
            for req in &metadata.requires {
                if !self.extensions.read().contains_key(req) {
                    self.create_extension(req, None, None, true)?;
                }
            }
        } else {
            for req in &metadata.requires {
                if !self.extensions.read().contains_key(req) {
                    return Err(ExtensionError::MissingDependency(req.clone()));
                }
            }
        }

        let version = version.unwrap_or(&metadata.default_version);
        let schema = schema.unwrap_or(metadata.schema.as_deref().unwrap_or("public"));

        let extension = self.build_extension(name, version, schema)?;
        self.extensions.write().insert(name.into(), extension);

        Ok(())
    }

    /// Drop an extension
    pub fn drop_extension(&self, name: &str, cascade: bool) -> Result<(), ExtensionError> {
        let mut extensions = self.extensions.write();
        
        if !extensions.contains_key(name) {
            return Err(ExtensionError::NotInstalled(name.into()));
        }

        // Check reverse dependencies
        if !cascade {
            for (other_name, ext) in extensions.iter() {
                if ext.requires.contains(&name.to_string()) {
                    return Err(ExtensionError::DependentExists(other_name.clone()));
                }
            }
        } else {
            // Cascade drop dependencies
            let dependents: Vec<String> = extensions.iter()
                .filter(|(_, ext)| ext.requires.contains(&name.to_string()))
                .map(|(n, _)| n.clone())
                .collect();
            
            for dependent in dependents {
                extensions.remove(&dependent);
            }
        }

        extensions.remove(name);
        Ok(())
    }

    /// Alter extension
    pub fn alter_extension(
        &self,
        name: &str,
        action: AlterExtensionAction,
    ) -> Result<(), ExtensionError> {
        let mut extensions = self.extensions.write();
        let extension = extensions.get_mut(name)
            .ok_or_else(|| ExtensionError::NotInstalled(name.into()))?;

        match action {
            AlterExtensionAction::UpdateTo(version) => {
                extension.version = version;
            }
            AlterExtensionAction::SetSchema(schema) => {
                if !extension.relocatable {
                    return Err(ExtensionError::NotRelocatable(name.into()));
                }
                extension.schema = schema;
            }
        }

        Ok(())
    }

    /// List installed extensions
    pub fn list_installed(&self) -> Vec<Extension> {
        self.extensions.read().values().cloned().collect()
    }

    /// List available extensions
    pub fn list_available(&self) -> Vec<ExtensionMetadata> {
        self.available.read().values().cloned().collect()
    }

    /// Get extension
    pub fn get(&self, name: &str) -> Option<Extension> {
        self.extensions.read().get(name).cloned()
    }

    /// Check if extension is installed
    pub fn is_installed(&self, name: &str) -> bool {
        self.extensions.read().contains_key(name)
    }

    fn build_extension(&self, name: &str, version: &str, schema: &str) -> Result<Extension, ExtensionError> {
        // Build extension based on name
        match name {
            "uuid_ossp" => Ok(self.build_uuid_ossp(version, schema)),
            "boyocrypto" => Ok(self.build_boyocrypto(version, schema)),
            "boyogis" => Ok(self.build_boyogis(version, schema)),
            "boyo_stat_statements" => Ok(self.build_boyo_stat_statements(version, schema)),
            "boyo_trgm" => Ok(self.build_boyo_trgm(version, schema)),
            "hstore" => Ok(self.build_hstore(version, schema)),
            "vector" => Ok(self.build_vector(version, schema)),
            "boyoscale" => Ok(self.build_boyoscale(version, schema)),
            _ => Err(ExtensionError::NotFound(name.into())),
        }
    }

    fn build_uuid_ossp(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "uuid_ossp".into(),
            version: version.into(),
            description: "UUID generation functions".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "uuid_generate_v1".into(),
                    args: vec![],
                    return_type: "uuid".into(),
                    volatility: Volatility::Volatile,
                    is_aggregate: false,
                    description: "Generate time-based UUID".into(),
                },
                ExtensionFunction {
                    name: "uuid_generate_v4".into(),
                    args: vec![],
                    return_type: "uuid".into(),
                    volatility: Volatility::Volatile,
                    is_aggregate: false,
                    description: "Generate random UUID".into(),
                },
                ExtensionFunction {
                    name: "uuid_nil".into(),
                    args: vec![],
                    return_type: "uuid".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Return nil UUID".into(),
                },
            ],
            types: vec![],
            operators: vec![],
        }
    }

    fn build_boyocrypto(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "boyocrypto".into(),
            version: version.into(),
            description: "Cryptographic functions".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "digest".into(),
                    args: vec![
                        FunctionArg { name: "data".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "algorithm".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "bytea".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Compute hash digest".into(),
                },
                ExtensionFunction {
                    name: "hmac".into(),
                    args: vec![
                        FunctionArg { name: "data".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "key".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "algorithm".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "bytea".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Compute HMAC".into(),
                },
                ExtensionFunction {
                    name: "encrypt".into(),
                    args: vec![
                        FunctionArg { name: "data".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "key".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "algorithm".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "bytea".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Encrypt data".into(),
                },
                ExtensionFunction {
                    name: "decrypt".into(),
                    args: vec![
                        FunctionArg { name: "data".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "key".into(), data_type: "bytea".into(), default: None },
                        FunctionArg { name: "algorithm".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "bytea".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Decrypt data".into(),
                },
                ExtensionFunction {
                    name: "gen_random_bytes".into(),
                    args: vec![
                        FunctionArg { name: "count".into(), data_type: "integer".into(), default: None },
                    ],
                    return_type: "bytea".into(),
                    volatility: Volatility::Volatile,
                    is_aggregate: false,
                    description: "Generate random bytes".into(),
                },
            ],
            types: vec![],
            operators: vec![],
        }
    }

    fn build_boyogis(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "boyogis".into(),
            version: version.into(),
            description: "Geographic/spatial functions".into(),
            schema: schema.into(),
            relocatable: false,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "ST_Distance".into(),
                    args: vec![
                        FunctionArg { name: "geom1".into(), data_type: "geometry".into(), default: None },
                        FunctionArg { name: "geom2".into(), data_type: "geometry".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate distance between geometries".into(),
                },
                ExtensionFunction {
                    name: "ST_Contains".into(),
                    args: vec![
                        FunctionArg { name: "geom1".into(), data_type: "geometry".into(), default: None },
                        FunctionArg { name: "geom2".into(), data_type: "geometry".into(), default: None },
                    ],
                    return_type: "boolean".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Check if geometry contains another".into(),
                },
                ExtensionFunction {
                    name: "ST_Within".into(),
                    args: vec![
                        FunctionArg { name: "geom1".into(), data_type: "geometry".into(), default: None },
                        FunctionArg { name: "geom2".into(), data_type: "geometry".into(), default: None },
                    ],
                    return_type: "boolean".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Check if geometry is within another".into(),
                },
                ExtensionFunction {
                    name: "ST_Area".into(),
                    args: vec![
                        FunctionArg { name: "geom".into(), data_type: "geometry".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate area of geometry".into(),
                },
            ],
            types: vec![
                ExtensionType {
                    name: "geometry".into(),
                    category: TypeCategory::Geometric,
                    input_function: "geometry_in".into(),
                    output_function: "geometry_out".into(),
                    storage: "external".into(),
                },
                ExtensionType {
                    name: "geography".into(),
                    category: TypeCategory::Geometric,
                    input_function: "geography_in".into(),
                    output_function: "geography_out".into(),
                    storage: "external".into(),
                },
            ],
            operators: vec![],
        }
    }

    fn build_boyo_stat_statements(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "boyo_stat_statements".into(),
            version: version.into(),
            description: "Query statistics tracking".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "boyo_stat_statements_reset".into(),
                    args: vec![],
                    return_type: "void".into(),
                    volatility: Volatility::Volatile,
                    is_aggregate: false,
                    description: "Reset query statistics".into(),
                },
            ],
            types: vec![],
            operators: vec![],
        }
    }

    fn build_boyo_trgm(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "boyo_trgm".into(),
            version: version.into(),
            description: "Trigram text similarity".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "similarity".into(),
                    args: vec![
                        FunctionArg { name: "text1".into(), data_type: "text".into(), default: None },
                        FunctionArg { name: "text2".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "real".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate trigram similarity".into(),
                },
                ExtensionFunction {
                    name: "show_trgm".into(),
                    args: vec![
                        FunctionArg { name: "text".into(), data_type: "text".into(), default: None },
                    ],
                    return_type: "text[]".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Show trigrams".into(),
                },
            ],
            types: vec![],
            operators: vec![
                ExtensionOperator {
                    name: "%".into(),
                    left_type: "text".into(),
                    right_type: "text".into(),
                    result_type: "boolean".into(),
                    function: "similarity_op".into(),
                    commutator: Some("%".into()),
                    negator: None,
                },
            ],
        }
    }

    fn build_hstore(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "hstore".into(),
            version: version.into(),
            description: "Key-value pair storage".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "hstore".into(),
                    args: vec![
                        FunctionArg { name: "keys".into(), data_type: "text[]".into(), default: None },
                        FunctionArg { name: "values".into(), data_type: "text[]".into(), default: None },
                    ],
                    return_type: "hstore".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Create hstore from arrays".into(),
                },
                ExtensionFunction {
                    name: "akeys".into(),
                    args: vec![
                        FunctionArg { name: "hs".into(), data_type: "hstore".into(), default: None },
                    ],
                    return_type: "text[]".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Get all keys".into(),
                },
                ExtensionFunction {
                    name: "avals".into(),
                    args: vec![
                        FunctionArg { name: "hs".into(), data_type: "hstore".into(), default: None },
                    ],
                    return_type: "text[]".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Get all values".into(),
                },
            ],
            types: vec![
                ExtensionType {
                    name: "hstore".into(),
                    category: TypeCategory::User,
                    input_function: "hstore_in".into(),
                    output_function: "hstore_out".into(),
                    storage: "extended".into(),
                },
            ],
            operators: vec![
                ExtensionOperator {
                    name: "->".into(),
                    left_type: "hstore".into(),
                    right_type: "text".into(),
                    result_type: "text".into(),
                    function: "hstore_fetchval".into(),
                    commutator: None,
                    negator: None,
                },
                ExtensionOperator {
                    name: "?".into(),
                    left_type: "hstore".into(),
                    right_type: "text".into(),
                    result_type: "boolean".into(),
                    function: "hstore_exists".into(),
                    commutator: None,
                    negator: None,
                },
            ],
        }
    }

    fn build_vector(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "vector".into(),
            version: version.into(),
            description: "Vector similarity search".into(),
            schema: schema.into(),
            relocatable: true,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "vector".into(),
                    args: vec![
                        FunctionArg { name: "arr".into(), data_type: "real[]".into(), default: None },
                    ],
                    return_type: "vector".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Create vector from array".into(),
                },
                ExtensionFunction {
                    name: "vector_dims".into(),
                    args: vec![
                        FunctionArg { name: "vec".into(), data_type: "vector".into(), default: None },
                    ],
                    return_type: "integer".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Get vector dimensions".into(),
                },
                ExtensionFunction {
                    name: "vector_norm".into(),
                    args: vec![
                        FunctionArg { name: "vec".into(), data_type: "vector".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate L2 norm".into(),
                },
                ExtensionFunction {
                    name: "cosine_distance".into(),
                    args: vec![
                        FunctionArg { name: "vec1".into(), data_type: "vector".into(), default: None },
                        FunctionArg { name: "vec2".into(), data_type: "vector".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate cosine distance".into(),
                },
                ExtensionFunction {
                    name: "l2_distance".into(),
                    args: vec![
                        FunctionArg { name: "vec1".into(), data_type: "vector".into(), default: None },
                        FunctionArg { name: "vec2".into(), data_type: "vector".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate L2/Euclidean distance".into(),
                },
                ExtensionFunction {
                    name: "inner_product".into(),
                    args: vec![
                        FunctionArg { name: "vec1".into(), data_type: "vector".into(), default: None },
                        FunctionArg { name: "vec2".into(), data_type: "vector".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Calculate inner/dot product".into(),
                },
            ],
            types: vec![
                ExtensionType {
                    name: "vector".into(),
                    category: TypeCategory::User,
                    input_function: "vector_in".into(),
                    output_function: "vector_out".into(),
                    storage: "extended".into(),
                },
            ],
            operators: vec![
                ExtensionOperator {
                    name: "<->".into(),
                    left_type: "vector".into(),
                    right_type: "vector".into(),
                    result_type: "double precision".into(),
                    function: "l2_distance".into(),
                    commutator: Some("<->".into()),
                    negator: None,
                },
                ExtensionOperator {
                    name: "<=>".into(),
                    left_type: "vector".into(),
                    right_type: "vector".into(),
                    result_type: "double precision".into(),
                    function: "cosine_distance".into(),
                    commutator: Some("<=>".into()),
                    negator: None,
                },
                ExtensionOperator {
                    name: "<#>".into(),
                    left_type: "vector".into(),
                    right_type: "vector".into(),
                    result_type: "double precision".into(),
                    function: "inner_product".into(),
                    commutator: Some("<#>".into()),
                    negator: None,
                },
            ],
        }
    }

    fn build_boyoscale(&self, version: &str, schema: &str) -> Extension {
        Extension {
            name: "boyoscale".into(),
            version: version.into(),
            description: "Time-series optimizations".into(),
            schema: schema.into(),
            relocatable: false,
            requires: vec![],
            state: ExtensionState::Installed,
            functions: vec![
                ExtensionFunction {
                    name: "time_bucket".into(),
                    args: vec![
                        FunctionArg { name: "bucket_width".into(), data_type: "interval".into(), default: None },
                        FunctionArg { name: "ts".into(), data_type: "timestamp".into(), default: None },
                    ],
                    return_type: "timestamp".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Bucket timestamps".into(),
                },
                ExtensionFunction {
                    name: "first".into(),
                    args: vec![
                        FunctionArg { name: "value".into(), data_type: "anyelement".into(), default: None },
                        FunctionArg { name: "time".into(), data_type: "timestamp".into(), default: None },
                    ],
                    return_type: "anyelement".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: true,
                    description: "First value by time".into(),
                },
                ExtensionFunction {
                    name: "last".into(),
                    args: vec![
                        FunctionArg { name: "value".into(), data_type: "anyelement".into(), default: None },
                        FunctionArg { name: "time".into(), data_type: "timestamp".into(), default: None },
                    ],
                    return_type: "anyelement".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: true,
                    description: "Last value by time".into(),
                },
                ExtensionFunction {
                    name: "interpolate".into(),
                    args: vec![
                        FunctionArg { name: "value".into(), data_type: "double precision".into(), default: None },
                    ],
                    return_type: "double precision".into(),
                    volatility: Volatility::Immutable,
                    is_aggregate: false,
                    description: "Interpolate missing values".into(),
                },
            ],
            types: vec![],
            operators: vec![],
        }
    }
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Alter extension action
#[derive(Debug, Clone)]
pub enum AlterExtensionAction {
    /// Update to new version
    UpdateTo(String),
    /// Set schema
    SetSchema(String),
}

/// Extension error
#[derive(Debug, Clone)]
pub enum ExtensionError {
    NotFound(String),
    NotInstalled(String),
    AlreadyInstalled(String),
    MissingDependency(String),
    DependentExists(String),
    NotRelocatable(String),
    VersionNotFound(String),
}

impl std::fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(n) => write!(f, "Extension '{}' not found", n),
            Self::NotInstalled(n) => write!(f, "Extension '{}' is not installed", n),
            Self::AlreadyInstalled(n) => write!(f, "Extension '{}' is already installed", n),
            Self::MissingDependency(n) => write!(f, "Missing required extension '{}'", n),
            Self::DependentExists(n) => write!(f, "Extension '{}' depends on this extension", n),
            Self::NotRelocatable(n) => write!(f, "Extension '{}' is not relocatable", n),
            Self::VersionNotFound(v) => write!(f, "Version '{}' not found", v),
        }
    }
}

impl std::error::Error for ExtensionError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_extension() {
        let registry = ExtensionRegistry::new();
        
        assert!(!registry.is_installed("uuid_ossp"));
        
        registry.create_extension("uuid_ossp", None, None, false).unwrap();
        
        assert!(registry.is_installed("uuid_ossp"));
        
        let ext = registry.get("uuid_ossp").unwrap();
        assert_eq!(ext.functions.len(), 3);
    }

    #[test]
    fn test_drop_extension() {
        let registry = ExtensionRegistry::new();
        
        registry.create_extension("hstore", None, None, false).unwrap();
        assert!(registry.is_installed("hstore"));
        
        registry.drop_extension("hstore", false).unwrap();
        assert!(!registry.is_installed("hstore"));
    }

    #[test]
    fn test_list_available() {
        let registry = ExtensionRegistry::new();
        let available = registry.list_available();
        
        assert!(available.len() >= 8);
        assert!(available.iter().any(|e| e.name == "uuid_ossp"));
        assert!(available.iter().any(|e| e.name == "vector"));
    }
}
