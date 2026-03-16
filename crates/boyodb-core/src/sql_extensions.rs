//! SQL Extensions Module
//!
//! Provides SQL function bindings for the new analytical modules.
//! Features:
//! - Graph query functions (GRAPH_TRAVERSE, SHORTEST_PATH, etc.)
//! - Time series functions (DOWNSAMPLE, GAP_FILL, etc.)
//! - Vector search functions (VECTOR_SEARCH, COSINE_DISTANCE, etc.)
//! - NL to SQL integration
//! - Data quality functions

use std::collections::HashMap;

/// SQL function registry for analytical extensions
pub struct SqlExtensionRegistry {
    /// Registered functions
    functions: HashMap<String, SqlExtension>,
}

impl SqlExtensionRegistry {
    /// Create new registry with default functions
    pub fn new() -> Self {
        let mut registry = SqlExtensionRegistry {
            functions: HashMap::new(),
        };
        registry.register_defaults();
        registry
    }

    /// Register default analytical functions
    fn register_defaults(&mut self) {
        // Graph functions
        self.register(SqlExtension::new(
            "GRAPH_TRAVERSE",
            ExtensionCategory::Graph,
            vec![
                Param::new("graph", ParamType::String),
                Param::new("start_node", ParamType::String),
                Param::new("direction", ParamType::String).optional(),
                Param::new("max_depth", ParamType::Integer).optional(),
            ],
            ParamType::Table,
            "Traverse graph from start node, returns visited nodes",
        ));

        self.register(SqlExtension::new(
            "SHORTEST_PATH",
            ExtensionCategory::Graph,
            vec![
                Param::new("graph", ParamType::String),
                Param::new("start_node", ParamType::String),
                Param::new("end_node", ParamType::String),
            ],
            ParamType::Table,
            "Find shortest path between two nodes",
        ));

        self.register(SqlExtension::new(
            "PAGERANK",
            ExtensionCategory::Graph,
            vec![
                Param::new("graph", ParamType::String),
                Param::new("damping", ParamType::Float).optional(),
                Param::new("iterations", ParamType::Integer).optional(),
            ],
            ParamType::Table,
            "Calculate PageRank scores for all nodes",
        ));

        self.register(SqlExtension::new(
            "COMMUNITY_DETECT",
            ExtensionCategory::Graph,
            vec![
                Param::new("graph", ParamType::String),
                Param::new("algorithm", ParamType::String).optional(),
            ],
            ParamType::Table,
            "Detect communities in graph",
        ));

        // Time series functions
        self.register(SqlExtension::new(
            "DOWNSAMPLE",
            ExtensionCategory::TimeSeries,
            vec![
                Param::new("series", ParamType::Table),
                Param::new("bucket", ParamType::String),
                Param::new("aggregation", ParamType::String),
            ],
            ParamType::Table,
            "Downsample time series to larger buckets",
        ));

        self.register(SqlExtension::new(
            "GAP_FILL",
            ExtensionCategory::TimeSeries,
            vec![
                Param::new("series", ParamType::Table),
                Param::new("interval", ParamType::String),
                Param::new("method", ParamType::String).optional(),
            ],
            ParamType::Table,
            "Fill gaps in time series data",
        ));

        self.register(SqlExtension::new(
            "MOVING_AVERAGE",
            ExtensionCategory::TimeSeries,
            vec![
                Param::new("series", ParamType::Table),
                Param::new("window", ParamType::Integer),
            ],
            ParamType::Table,
            "Calculate moving average",
        ));

        self.register(SqlExtension::new(
            "FORECAST",
            ExtensionCategory::TimeSeries,
            vec![
                Param::new("series", ParamType::Table),
                Param::new("periods", ParamType::Integer),
                Param::new("method", ParamType::String).optional(),
            ],
            ParamType::Table,
            "Forecast future values",
        ));

        self.register(SqlExtension::new(
            "DETECT_ANOMALIES",
            ExtensionCategory::TimeSeries,
            vec![
                Param::new("series", ParamType::Table),
                Param::new("sensitivity", ParamType::Float).optional(),
            ],
            ParamType::Table,
            "Detect anomalies in time series",
        ));

        // Vector search functions
        self.register(SqlExtension::new(
            "VECTOR_SEARCH",
            ExtensionCategory::Vector,
            vec![
                Param::new("index", ParamType::String),
                Param::new("query_vector", ParamType::Array),
                Param::new("k", ParamType::Integer),
            ],
            ParamType::Table,
            "Search for k nearest vectors",
        ));

        self.register(SqlExtension::new(
            "COSINE_DISTANCE",
            ExtensionCategory::Vector,
            vec![
                Param::new("vector1", ParamType::Array),
                Param::new("vector2", ParamType::Array),
            ],
            ParamType::Float,
            "Calculate cosine distance between vectors",
        ));

        self.register(SqlExtension::new(
            "EUCLIDEAN_DISTANCE",
            ExtensionCategory::Vector,
            vec![
                Param::new("vector1", ParamType::Array),
                Param::new("vector2", ParamType::Array),
            ],
            ParamType::Float,
            "Calculate Euclidean distance between vectors",
        ));

        self.register(SqlExtension::new(
            "EMBEDDING",
            ExtensionCategory::Vector,
            vec![
                Param::new("text", ParamType::String),
                Param::new("model", ParamType::String).optional(),
            ],
            ParamType::Array,
            "Generate embedding for text",
        ));

        // Data quality functions
        self.register(SqlExtension::new(
            "VALIDATE",
            ExtensionCategory::DataQuality,
            vec![
                Param::new("table", ParamType::Table),
                Param::new("rules", ParamType::String),
            ],
            ParamType::Table,
            "Validate data against rules",
        ));

        self.register(SqlExtension::new(
            "PROFILE",
            ExtensionCategory::DataQuality,
            vec![Param::new("table", ParamType::Table)],
            ParamType::Table,
            "Generate data quality profile",
        ));

        self.register(SqlExtension::new(
            "QUALITY_SCORE",
            ExtensionCategory::DataQuality,
            vec![Param::new("table", ParamType::Table)],
            ParamType::Float,
            "Calculate overall data quality score",
        ));

        // NL to SQL
        self.register(SqlExtension::new(
            "NL_QUERY",
            ExtensionCategory::NaturalLanguage,
            vec![
                Param::new("question", ParamType::String),
                Param::new("schema", ParamType::String).optional(),
            ],
            ParamType::String,
            "Convert natural language to SQL",
        ));

        // Blockchain/Audit functions
        self.register(SqlExtension::new(
            "AUDIT_LOG",
            ExtensionCategory::Audit,
            vec![
                Param::new("table", ParamType::String),
                Param::new("since", ParamType::Timestamp).optional(),
            ],
            ParamType::Table,
            "Get audit log for table",
        ));

        self.register(SqlExtension::new(
            "VERIFY_CHAIN",
            ExtensionCategory::Audit,
            vec![Param::new("ledger", ParamType::String)],
            ParamType::Boolean,
            "Verify blockchain integrity",
        ));

        // Workflow functions
        self.register(SqlExtension::new(
            "RUN_WORKFLOW",
            ExtensionCategory::Workflow,
            vec![
                Param::new("workflow_id", ParamType::String),
                Param::new("params", ParamType::Json).optional(),
            ],
            ParamType::String,
            "Execute workflow and return run ID",
        ));

        self.register(SqlExtension::new(
            "WORKFLOW_STATUS",
            ExtensionCategory::Workflow,
            vec![Param::new("run_id", ParamType::String)],
            ParamType::Table,
            "Get workflow run status",
        ));

        // Federated query
        self.register(SqlExtension::new(
            "FEDERATED_QUERY",
            ExtensionCategory::Federation,
            vec![
                Param::new("sources", ParamType::Array),
                Param::new("query", ParamType::String),
            ],
            ParamType::Table,
            "Execute query across multiple sources",
        ));

        // ML functions
        self.register(SqlExtension::new(
            "PREDICT",
            ExtensionCategory::ML,
            vec![
                Param::new("model", ParamType::String),
                Param::new("features", ParamType::Table),
            ],
            ParamType::Table,
            "Run ML prediction",
        ));

        self.register(SqlExtension::new(
            "EXPLAIN_PREDICTION",
            ExtensionCategory::ML,
            vec![
                Param::new("model", ParamType::String),
                Param::new("features", ParamType::Table),
            ],
            ParamType::Table,
            "Explain ML prediction with feature importance",
        ));

        // Catalog functions
        self.register(SqlExtension::new(
            "SEARCH_CATALOG",
            ExtensionCategory::Catalog,
            vec![
                Param::new("query", ParamType::String),
                Param::new("type", ParamType::String).optional(),
            ],
            ParamType::Table,
            "Search data catalog",
        ));

        self.register(SqlExtension::new(
            "DATA_LINEAGE",
            ExtensionCategory::Catalog,
            vec![
                Param::new("table", ParamType::String),
                Param::new("direction", ParamType::String).optional(),
            ],
            ParamType::Table,
            "Get data lineage for table",
        ));
    }

    /// Register function
    pub fn register(&mut self, ext: SqlExtension) {
        self.functions.insert(ext.name.clone(), ext);
    }

    /// Get function
    pub fn get(&self, name: &str) -> Option<&SqlExtension> {
        self.functions.get(&name.to_uppercase())
    }

    /// List functions
    pub fn list(&self) -> Vec<&SqlExtension> {
        self.functions.values().collect()
    }

    /// List by category
    pub fn list_by_category(&self, category: ExtensionCategory) -> Vec<&SqlExtension> {
        self.functions
            .values()
            .filter(|f| f.category == category)
            .collect()
    }

    /// Generate help text
    pub fn help(&self, name: &str) -> Option<String> {
        self.get(name).map(|ext| {
            let params: Vec<String> = ext
                .parameters
                .iter()
                .map(|p| {
                    if p.optional {
                        format!("[{}: {}]", p.name, p.param_type.name())
                    } else {
                        format!("{}: {}", p.name, p.param_type.name())
                    }
                })
                .collect();

            format!(
                "{}\n\nSyntax: {}({})\nReturns: {}\nCategory: {:?}\n\nDescription: {}",
                ext.name,
                ext.name,
                params.join(", "),
                ext.return_type.name(),
                ext.category,
                ext.description
            )
        })
    }
}

impl Default for SqlExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL extension definition
#[derive(Debug, Clone)]
pub struct SqlExtension {
    /// Function name
    pub name: String,
    /// Category
    pub category: ExtensionCategory,
    /// Parameters
    pub parameters: Vec<Param>,
    /// Return type
    pub return_type: ParamType,
    /// Description
    pub description: String,
}

impl SqlExtension {
    /// Create new extension
    pub fn new(
        name: &str,
        category: ExtensionCategory,
        parameters: Vec<Param>,
        return_type: ParamType,
        description: &str,
    ) -> Self {
        SqlExtension {
            name: name.to_uppercase(),
            category,
            parameters,
            return_type,
            description: description.to_string(),
        }
    }

    /// Validate arguments
    pub fn validate_args(&self, args: &[SqlValue]) -> Result<(), String> {
        let required_count = self.parameters.iter().filter(|p| !p.optional).count();

        if args.len() < required_count {
            return Err(format!(
                "{} requires at least {} arguments, got {}",
                self.name,
                required_count,
                args.len()
            ));
        }

        if args.len() > self.parameters.len() {
            return Err(format!(
                "{} takes at most {} arguments, got {}",
                self.name,
                self.parameters.len(),
                args.len()
            ));
        }

        for (i, arg) in args.iter().enumerate() {
            let param = &self.parameters[i];
            if !param.param_type.is_compatible(arg) {
                return Err(format!(
                    "Argument {} ({}) expects {}, got {:?}",
                    i + 1,
                    param.name,
                    param.param_type.name(),
                    arg
                ));
            }
        }

        Ok(())
    }
}

/// Extension categories
#[derive(Debug, Clone, PartialEq)]
pub enum ExtensionCategory {
    Graph,
    TimeSeries,
    Vector,
    DataQuality,
    NaturalLanguage,
    Audit,
    Workflow,
    Federation,
    ML,
    Catalog,
}

/// Function parameter
#[derive(Debug, Clone)]
pub struct Param {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: ParamType,
    /// Is optional
    pub optional: bool,
    /// Default value
    pub default: Option<SqlValue>,
}

impl Param {
    /// Create new parameter
    pub fn new(name: &str, param_type: ParamType) -> Self {
        Param {
            name: name.to_string(),
            param_type,
            optional: false,
            default: None,
        }
    }

    /// Mark as optional
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    /// Set default value
    pub fn with_default(mut self, value: SqlValue) -> Self {
        self.optional = true;
        self.default = Some(value);
        self
    }
}

/// Parameter types
#[derive(Debug, Clone, PartialEq)]
pub enum ParamType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Table,
    Json,
    Timestamp,
    Any,
}

impl ParamType {
    /// Get type name
    pub fn name(&self) -> &str {
        match self {
            ParamType::String => "STRING",
            ParamType::Integer => "INTEGER",
            ParamType::Float => "FLOAT",
            ParamType::Boolean => "BOOLEAN",
            ParamType::Array => "ARRAY",
            ParamType::Table => "TABLE",
            ParamType::Json => "JSON",
            ParamType::Timestamp => "TIMESTAMP",
            ParamType::Any => "ANY",
        }
    }

    /// Check if value is compatible
    pub fn is_compatible(&self, value: &SqlValue) -> bool {
        match (self, value) {
            (ParamType::Any, _) => true,
            (ParamType::String, SqlValue::String(_)) => true,
            (ParamType::Integer, SqlValue::Integer(_)) => true,
            (ParamType::Float, SqlValue::Float(_)) => true,
            (ParamType::Float, SqlValue::Integer(_)) => true, // Allow int for float
            (ParamType::Boolean, SqlValue::Boolean(_)) => true,
            (ParamType::Array, SqlValue::Array(_)) => true,
            (ParamType::Json, SqlValue::Json(_)) => true,
            (ParamType::Timestamp, SqlValue::Timestamp(_)) => true,
            (ParamType::Table, SqlValue::Table(_)) => true,
            _ => false,
        }
    }
}

/// SQL value for function arguments
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<SqlValue>),
    Json(String),
    Timestamp(u64),
    Table(String), // Table name/reference
}

/// SQL query rewriter for extensions
pub struct QueryRewriter;

impl QueryRewriter {
    /// Rewrite query with extensions to standard SQL + function calls
    pub fn rewrite(sql: &str, registry: &SqlExtensionRegistry) -> Result<RewriteResult, String> {
        // Simple pattern matching - in production, use proper SQL parser
        let mut rewritten = sql.to_string();
        let mut function_calls = Vec::new();

        for func in registry.list() {
            let pattern = format!("{}(", func.name);
            if sql.to_uppercase().contains(&pattern) {
                // Extract function call
                if let Some(start) = sql.to_uppercase().find(&pattern) {
                    // Find matching parenthesis
                    let after_name = start + func.name.len();
                    if let Some(end) = find_matching_paren(&sql[after_name..]) {
                        let call = &sql[start..after_name + end + 1];
                        function_calls.push(FunctionCall {
                            name: func.name.clone(),
                            call_text: call.to_string(),
                            category: func.category.clone(),
                        });
                    }
                }
            }
        }

        // Replace function calls with placeholders or CTEs
        for (i, call) in function_calls.iter().enumerate() {
            let placeholder = format!("__ext_result_{}", i);
            rewritten = rewritten.replace(&call.call_text, &placeholder);
        }

        Ok(RewriteResult {
            rewritten_sql: rewritten,
            function_calls,
        })
    }
}

/// Find matching closing parenthesis
fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Query rewrite result
#[derive(Debug)]
pub struct RewriteResult {
    /// Rewritten SQL
    pub rewritten_sql: String,
    /// Function calls found
    pub function_calls: Vec<FunctionCall>,
}

/// Function call in query
#[derive(Debug)]
pub struct FunctionCall {
    /// Function name
    pub name: String,
    /// Full call text
    pub call_text: String,
    /// Category
    pub category: ExtensionCategory,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = SqlExtensionRegistry::new();

        assert!(registry.get("VECTOR_SEARCH").is_some());
        assert!(registry.get("GRAPH_TRAVERSE").is_some());
        assert!(registry.get("DOWNSAMPLE").is_some());
    }

    #[test]
    fn test_function_categories() {
        let registry = SqlExtensionRegistry::new();

        let graph_funcs = registry.list_by_category(ExtensionCategory::Graph);
        assert!(!graph_funcs.is_empty());

        let ts_funcs = registry.list_by_category(ExtensionCategory::TimeSeries);
        assert!(!ts_funcs.is_empty());
    }

    #[test]
    fn test_validate_args() {
        let registry = SqlExtensionRegistry::new();
        let vector_search = registry.get("VECTOR_SEARCH").unwrap();

        // Valid args
        let args = vec![
            SqlValue::String("my_index".to_string()),
            SqlValue::Array(vec![SqlValue::Float(1.0), SqlValue::Float(2.0)]),
            SqlValue::Integer(10),
        ];
        assert!(vector_search.validate_args(&args).is_ok());

        // Missing required arg
        let args = vec![SqlValue::String("my_index".to_string())];
        assert!(vector_search.validate_args(&args).is_err());
    }

    #[test]
    fn test_help_generation() {
        let registry = SqlExtensionRegistry::new();

        let help = registry.help("VECTOR_SEARCH");
        assert!(help.is_some());
        assert!(help.unwrap().contains("VECTOR_SEARCH"));
    }

    #[test]
    fn test_query_rewrite() {
        let registry = SqlExtensionRegistry::new();

        let sql = "SELECT * FROM VECTOR_SEARCH('idx', [1,2,3], 10)";
        let result = QueryRewriter::rewrite(sql, &registry).unwrap();

        assert!(!result.function_calls.is_empty());
        assert_eq!(result.function_calls[0].name, "VECTOR_SEARCH");
    }
}
