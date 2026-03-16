//! LATERAL Joins Implementation
//!
//! Support for LATERAL subqueries that can reference columns from preceding tables.
//!
//! # Features
//! - LATERAL subqueries in FROM clause
//! - Cross-lateral joins (implicit CROSS JOIN LATERAL)
//! - Left lateral joins (LEFT JOIN LATERAL)
//! - Correlated lateral references
//! - Set-returning function calls with LATERAL
//!
//! # Example
//! ```sql
//! -- Get top 3 orders for each customer
//! SELECT c.name, o.order_id, o.total
//! FROM customers c
//! CROSS JOIN LATERAL (
//!     SELECT order_id, total
//!     FROM orders
//!     WHERE customer_id = c.id
//!     ORDER BY total DESC
//!     LIMIT 3
//! ) o;
//!
//! -- Using LATERAL with set-returning functions
//! SELECT * FROM t, LATERAL unnest(t.arr) AS elem;
//! ```

use std::collections::{HashMap, HashSet};

// ============================================================================
// Types and Errors
// ============================================================================

/// Errors from lateral join processing
#[derive(Debug, Clone)]
pub enum LateralJoinError {
    /// Invalid lateral reference (column not available)
    InvalidReference {
        column: String,
        available_tables: Vec<String>,
    },
    /// Lateral keyword without subquery
    LateralWithoutSubquery,
    /// Circular lateral reference
    CircularReference { tables: Vec<String> },
    /// Subquery returns wrong number of columns
    ColumnCountMismatch { expected: usize, actual: usize },
    /// Parse error
    ParseError(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for LateralJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidReference {
                column,
                available_tables,
            } => {
                write!(
                    f,
                    "invalid lateral reference to '{}', available tables: {:?}",
                    column, available_tables
                )
            }
            Self::LateralWithoutSubquery => {
                write!(f, "LATERAL keyword must be followed by a subquery")
            }
            Self::CircularReference { tables } => {
                write!(f, "circular lateral reference: {:?}", tables)
            }
            Self::ColumnCountMismatch { expected, actual } => {
                write!(
                    f,
                    "subquery returns {} columns, expected {}",
                    actual, expected
                )
            }
            Self::ParseError(msg) => write!(f, "parse error: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for LateralJoinError {}

/// Join type for lateral joins
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateralJoinType {
    /// CROSS JOIN LATERAL (or just , LATERAL)
    Cross,
    /// INNER JOIN LATERAL
    Inner,
    /// LEFT JOIN LATERAL (LEFT OUTER JOIN LATERAL)
    Left,
}

impl std::fmt::Display for LateralJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cross => write!(f, "CROSS JOIN LATERAL"),
            Self::Inner => write!(f, "INNER JOIN LATERAL"),
            Self::Left => write!(f, "LEFT JOIN LATERAL"),
        }
    }
}

// ============================================================================
// Column Reference
// ============================================================================

/// A column reference in a lateral subquery
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Table alias (if specified)
    pub table_alias: Option<String>,
    /// Column name
    pub column_name: String,
}

impl ColumnRef {
    /// Create a new column reference
    pub fn new(table_alias: Option<String>, column_name: String) -> Self {
        Self {
            table_alias,
            column_name,
        }
    }

    /// Parse a column reference from "table.column" or just "column"
    pub fn parse(s: &str) -> Self {
        if let Some(dot_pos) = s.find('.') {
            let table = &s[..dot_pos];
            let column = &s[dot_pos + 1..];
            Self::new(Some(table.to_string()), column.to_string())
        } else {
            Self::new(None, s.to_string())
        }
    }

    /// Format as string
    pub fn to_sql(&self) -> String {
        match &self.table_alias {
            Some(t) => format!("{}.{}", t, self.column_name),
            None => self.column_name.clone(),
        }
    }
}

// ============================================================================
// Lateral Subquery
// ============================================================================

/// A LATERAL subquery definition
#[derive(Debug, Clone)]
pub struct LateralSubquery {
    /// The subquery SQL (without LATERAL keyword)
    pub subquery_sql: String,
    /// Alias for the subquery result
    pub alias: String,
    /// Column aliases (if specified)
    pub column_aliases: Vec<String>,
    /// Lateral references (columns from outer tables used in subquery)
    pub lateral_refs: Vec<ColumnRef>,
    /// Tables that this subquery depends on
    pub depends_on: HashSet<String>,
}

impl LateralSubquery {
    /// Create a new lateral subquery
    pub fn new(subquery_sql: String, alias: String) -> Self {
        Self {
            subquery_sql,
            alias,
            column_aliases: Vec::new(),
            lateral_refs: Vec::new(),
            depends_on: HashSet::new(),
        }
    }

    /// Set column aliases
    pub fn with_column_aliases(mut self, aliases: Vec<String>) -> Self {
        self.column_aliases = aliases;
        self
    }

    /// Add a lateral reference
    pub fn add_lateral_ref(&mut self, ref_col: ColumnRef) {
        if let Some(table) = &ref_col.table_alias {
            self.depends_on.insert(table.clone());
        }
        self.lateral_refs.push(ref_col);
    }

    /// Get all lateral references
    pub fn get_lateral_refs(&self) -> &[ColumnRef] {
        &self.lateral_refs
    }
}

// ============================================================================
// Lateral Join
// ============================================================================

/// A lateral join in a query
#[derive(Debug, Clone)]
pub struct LateralJoin {
    /// Join type
    pub join_type: LateralJoinType,
    /// The lateral subquery
    pub subquery: LateralSubquery,
    /// ON condition (for INNER/LEFT joins)
    pub on_condition: Option<String>,
}

impl LateralJoin {
    /// Create a CROSS JOIN LATERAL
    pub fn cross(subquery: LateralSubquery) -> Self {
        Self {
            join_type: LateralJoinType::Cross,
            subquery,
            on_condition: None,
        }
    }

    /// Create an INNER JOIN LATERAL
    pub fn inner(subquery: LateralSubquery, on_condition: String) -> Self {
        Self {
            join_type: LateralJoinType::Inner,
            subquery,
            on_condition: Some(on_condition),
        }
    }

    /// Create a LEFT JOIN LATERAL
    pub fn left(subquery: LateralSubquery, on_condition: Option<String>) -> Self {
        Self {
            join_type: LateralJoinType::Left,
            subquery,
            on_condition,
        }
    }

    /// Generate SQL for this lateral join
    pub fn to_sql(&self) -> String {
        let subquery_sql = &self.subquery.subquery_sql;
        let alias = &self.subquery.alias;

        let column_list = if self.subquery.column_aliases.is_empty() {
            String::new()
        } else {
            format!("({})", self.subquery.column_aliases.join(", "))
        };

        match self.join_type {
            LateralJoinType::Cross => {
                format!(
                    "CROSS JOIN LATERAL ({}) AS {}{}",
                    subquery_sql, alias, column_list
                )
            }
            LateralJoinType::Inner => {
                let on_clause = self
                    .on_condition
                    .as_ref()
                    .map(|c| format!(" ON {}", c))
                    .unwrap_or_default();
                format!(
                    "INNER JOIN LATERAL ({}) AS {}{} {}",
                    subquery_sql, alias, column_list, on_clause
                )
            }
            LateralJoinType::Left => {
                let on_clause = self
                    .on_condition
                    .as_ref()
                    .map(|c| format!(" ON {}", c))
                    .unwrap_or_else(|| " ON true".to_string());
                format!(
                    "LEFT JOIN LATERAL ({}) AS {}{}{}",
                    subquery_sql, alias, column_list, on_clause
                )
            }
        }
    }
}

// ============================================================================
// Table Source (for FROM clause)
// ============================================================================

/// A table source in the FROM clause
#[derive(Debug, Clone)]
pub enum TableSource {
    /// Simple table reference
    Table {
        schema: Option<String>,
        name: String,
        alias: Option<String>,
    },
    /// Subquery (non-lateral)
    Subquery {
        sql: String,
        alias: String,
        column_aliases: Vec<String>,
    },
    /// Lateral subquery
    Lateral(LateralSubquery),
    /// Set-returning function
    Function {
        name: String,
        args: Vec<String>,
        alias: Option<String>,
        is_lateral: bool,
    },
}

impl TableSource {
    /// Get the alias for this source
    pub fn alias(&self) -> Option<&str> {
        match self {
            Self::Table { alias, name, .. } => alias.as_deref().or(Some(name.as_str())),
            Self::Subquery { alias, .. } => Some(alias.as_str()),
            Self::Lateral(sub) => Some(sub.alias.as_str()),
            Self::Function { alias, name, .. } => alias.as_deref().or(Some(name.as_str())),
        }
    }

    /// Check if this is a lateral source
    pub fn is_lateral(&self) -> bool {
        matches!(
            self,
            Self::Lateral(_)
                | Self::Function {
                    is_lateral: true,
                    ..
                }
        )
    }
}

// ============================================================================
// Lateral Join Analysis
// ============================================================================

/// Analyzer for lateral joins
pub struct LateralJoinAnalyzer {
    /// Available tables in scope (alias -> columns)
    available_tables: HashMap<String, HashSet<String>>,
    /// Order in which tables appear in FROM clause
    table_order: Vec<String>,
}

impl Default for LateralJoinAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl LateralJoinAnalyzer {
    /// Create a new analyzer
    pub fn new() -> Self {
        Self {
            available_tables: HashMap::new(),
            table_order: Vec::new(),
        }
    }

    /// Add a table to the available scope
    pub fn add_table(&mut self, alias: &str, columns: HashSet<String>) {
        self.available_tables.insert(alias.to_string(), columns);
        self.table_order.push(alias.to_string());
    }

    /// Check if a column reference is valid
    pub fn validate_ref(&self, col_ref: &ColumnRef) -> Result<(), LateralJoinError> {
        if let Some(table) = &col_ref.table_alias {
            if !self.available_tables.contains_key(table) {
                return Err(LateralJoinError::InvalidReference {
                    column: col_ref.to_sql(),
                    available_tables: self.table_order.clone(),
                });
            }
        }
        Ok(())
    }

    /// Analyze a lateral subquery for valid references
    pub fn analyze_lateral_subquery(
        &self,
        subquery: &LateralSubquery,
    ) -> Result<(), LateralJoinError> {
        for ref_col in &subquery.lateral_refs {
            self.validate_ref(ref_col)?;
        }
        Ok(())
    }

    /// Get tables available for lateral reference
    pub fn get_available_tables(&self) -> Vec<String> {
        self.table_order.clone()
    }
}

// ============================================================================
// Lateral Reference Extractor
// ============================================================================

/// Extract lateral references from a subquery
pub struct LateralRefExtractor {
    /// Column references found
    refs: Vec<ColumnRef>,
    /// Tables seen in the subquery itself
    subquery_tables: HashSet<String>,
}

impl Default for LateralRefExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl LateralRefExtractor {
    /// Create a new extractor
    pub fn new() -> Self {
        Self {
            refs: Vec::new(),
            subquery_tables: HashSet::new(),
        }
    }

    /// Simple extraction of potential column references from SQL
    /// This is a basic implementation - a full parser would be more accurate
    pub fn extract_refs(&mut self, sql: &str, outer_tables: &[String]) -> Vec<ColumnRef> {
        let outer_set: HashSet<_> = outer_tables.iter().collect();
        let mut refs = Vec::new();

        // Look for qualified references like "outer_table.column"
        for outer_table in outer_tables {
            let pattern = format!("{}.", outer_table);
            let mut start = 0;
            while let Some(pos) = sql[start..].find(&pattern) {
                let abs_pos = start + pos;
                let after_dot = abs_pos + pattern.len();

                // Extract column name (alphanumeric + underscore)
                let col_end = sql[after_dot..]
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .map(|p| after_dot + p)
                    .unwrap_or(sql.len());

                if col_end > after_dot {
                    let column = &sql[after_dot..col_end];
                    refs.push(ColumnRef::new(
                        Some(outer_table.clone()),
                        column.to_string(),
                    ));
                }

                start = col_end;
            }
        }

        self.refs = refs.clone();
        refs
    }

    /// Get extracted references
    pub fn get_refs(&self) -> &[ColumnRef] {
        &self.refs
    }
}

// ============================================================================
// Query Rewriter for Lateral Joins
// ============================================================================

/// Execution plan for lateral join
#[derive(Debug, Clone)]
pub struct LateralExecutionPlan {
    /// The outer (driving) table
    pub outer_source: String,
    /// The lateral subquery
    pub lateral_subquery: String,
    /// Lateral references to bind for each outer row
    pub bindings: Vec<ColumnRef>,
    /// Join type
    pub join_type: LateralJoinType,
    /// Output columns
    pub output_columns: Vec<String>,
}

/// Rewrite a query with lateral joins into an execution plan
pub struct LateralQueryRewriter;

impl LateralQueryRewriter {
    /// Create execution plan for a lateral join
    pub fn create_plan(outer_alias: &str, lateral_join: &LateralJoin) -> LateralExecutionPlan {
        LateralExecutionPlan {
            outer_source: outer_alias.to_string(),
            lateral_subquery: lateral_join.subquery.subquery_sql.clone(),
            bindings: lateral_join.subquery.lateral_refs.clone(),
            join_type: lateral_join.join_type,
            output_columns: lateral_join.subquery.column_aliases.clone(),
        }
    }

    /// Substitute parameters in lateral subquery for a specific outer row
    pub fn substitute_params(subquery: &str, bindings: &[(ColumnRef, String)]) -> String {
        let mut result = subquery.to_string();
        for (col_ref, value) in bindings {
            let pattern = col_ref.to_sql();
            result = result.replace(&pattern, value);
        }
        result
    }
}

// ============================================================================
// Set-Returning Function Support
// ============================================================================

/// Set-returning function for LATERAL joins
#[derive(Debug, Clone)]
pub struct SetReturningFunction {
    /// Function name
    pub name: String,
    /// Function arguments
    pub args: Vec<String>,
    /// Return column names
    pub return_columns: Vec<String>,
    /// Whether the function uses outer references
    pub uses_lateral_refs: bool,
}

impl SetReturningFunction {
    /// Create a new set-returning function
    pub fn new(name: &str, args: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            args,
            return_columns: vec!["value".to_string()],
            uses_lateral_refs: false,
        }
    }

    /// Set return columns
    pub fn with_return_columns(mut self, columns: Vec<String>) -> Self {
        self.return_columns = columns;
        self
    }

    /// Mark as using lateral references
    pub fn with_lateral_refs(mut self) -> Self {
        self.uses_lateral_refs = true;
        self
    }

    /// Generate SQL for the function call
    pub fn to_sql(&self, alias: Option<&str>) -> String {
        let func_call = format!("{}({})", self.name, self.args.join(", "));

        match alias {
            Some(a) => {
                if self.return_columns.len() == 1 {
                    format!("{} AS {}", func_call, a)
                } else {
                    format!("{} AS {}({})", func_call, a, self.return_columns.join(", "))
                }
            }
            None => func_call,
        }
    }
}

/// Common set-returning functions
pub struct SetReturningFunctions;

impl SetReturningFunctions {
    /// unnest(array) - expand array to set of rows
    pub fn unnest(array_expr: &str) -> SetReturningFunction {
        SetReturningFunction::new("unnest", vec![array_expr.to_string()])
    }

    /// generate_series(start, stop, [step]) - generate a series of values
    pub fn generate_series(start: &str, stop: &str, step: Option<&str>) -> SetReturningFunction {
        let mut args = vec![start.to_string(), stop.to_string()];
        if let Some(s) = step {
            args.push(s.to_string());
        }
        SetReturningFunction::new("generate_series", args)
    }

    /// regexp_matches(string, pattern) - returns matches of a regex
    pub fn regexp_matches(string: &str, pattern: &str) -> SetReturningFunction {
        SetReturningFunction::new(
            "regexp_matches",
            vec![string.to_string(), pattern.to_string()],
        )
        .with_return_columns(vec!["match".to_string()])
    }

    /// json_array_elements(json) - expand JSON array
    pub fn json_array_elements(json_expr: &str) -> SetReturningFunction {
        SetReturningFunction::new("json_array_elements", vec![json_expr.to_string()])
            .with_return_columns(vec!["value".to_string()])
    }

    /// jsonb_array_elements(jsonb) - expand JSONB array
    pub fn jsonb_array_elements(jsonb_expr: &str) -> SetReturningFunction {
        SetReturningFunction::new("jsonb_array_elements", vec![jsonb_expr.to_string()])
            .with_return_columns(vec!["value".to_string()])
    }

    /// jsonb_each(jsonb) - expand JSONB object to key/value pairs
    pub fn jsonb_each(jsonb_expr: &str) -> SetReturningFunction {
        SetReturningFunction::new("jsonb_each", vec![jsonb_expr.to_string()])
            .with_return_columns(vec!["key".to_string(), "value".to_string()])
    }
}

// ============================================================================
// Lateral Join Builder
// ============================================================================

/// Builder for constructing lateral joins
pub struct LateralJoinBuilder {
    join_type: LateralJoinType,
    subquery: Option<String>,
    alias: Option<String>,
    column_aliases: Vec<String>,
    on_condition: Option<String>,
    lateral_refs: Vec<ColumnRef>,
}

impl Default for LateralJoinBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LateralJoinBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            join_type: LateralJoinType::Cross,
            subquery: None,
            alias: None,
            column_aliases: Vec::new(),
            on_condition: None,
            lateral_refs: Vec::new(),
        }
    }

    /// Set join type to CROSS JOIN LATERAL
    pub fn cross_join(mut self) -> Self {
        self.join_type = LateralJoinType::Cross;
        self
    }

    /// Set join type to INNER JOIN LATERAL
    pub fn inner_join(mut self) -> Self {
        self.join_type = LateralJoinType::Inner;
        self
    }

    /// Set join type to LEFT JOIN LATERAL
    pub fn left_join(mut self) -> Self {
        self.join_type = LateralJoinType::Left;
        self
    }

    /// Set the subquery
    pub fn subquery(mut self, sql: &str) -> Self {
        self.subquery = Some(sql.to_string());
        self
    }

    /// Set the alias
    pub fn alias(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }

    /// Set column aliases
    pub fn columns(mut self, aliases: Vec<&str>) -> Self {
        self.column_aliases = aliases.into_iter().map(String::from).collect();
        self
    }

    /// Set ON condition
    pub fn on(mut self, condition: &str) -> Self {
        self.on_condition = Some(condition.to_string());
        self
    }

    /// Add a lateral reference
    pub fn lateral_ref(mut self, table: &str, column: &str) -> Self {
        self.lateral_refs
            .push(ColumnRef::new(Some(table.to_string()), column.to_string()));
        self
    }

    /// Build the lateral join
    pub fn build(self) -> Result<LateralJoin, LateralJoinError> {
        let subquery_sql = self
            .subquery
            .ok_or(LateralJoinError::LateralWithoutSubquery)?;
        let alias = self.alias.unwrap_or_else(|| "lateral_subq".to_string());

        let mut subquery = LateralSubquery::new(subquery_sql, alias);
        subquery.column_aliases = self.column_aliases;
        for ref_col in self.lateral_refs {
            subquery.add_lateral_ref(ref_col);
        }

        Ok(match self.join_type {
            LateralJoinType::Cross => LateralJoin::cross(subquery),
            LateralJoinType::Inner => {
                let on_cond = self.on_condition.ok_or_else(|| {
                    LateralJoinError::ParseError("INNER JOIN requires ON condition".into())
                })?;
                LateralJoin::inner(subquery, on_cond)
            }
            LateralJoinType::Left => LateralJoin::left(subquery, self.on_condition),
        })
    }
}

// ============================================================================
// SQL Parser Helpers
// ============================================================================

/// Check if a SQL fragment contains LATERAL keyword
pub fn contains_lateral(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains(" LATERAL ") || upper.contains(",LATERAL ") || upper.contains("(LATERAL ")
}

/// Extract LATERAL subqueries from FROM clause (simplified)
pub fn extract_lateral_subqueries(from_clause: &str) -> Vec<(usize, usize, String)> {
    let upper = from_clause.to_uppercase();
    let mut results = Vec::new();

    let mut search_start = 0;
    while let Some(lateral_pos) = upper[search_start..].find("LATERAL") {
        let abs_pos = search_start + lateral_pos;

        // Find the opening parenthesis
        if let Some(paren_start) = from_clause[abs_pos..].find('(') {
            let subq_start = abs_pos + paren_start;

            // Find matching closing parenthesis
            let mut depth = 0;
            let mut subq_end = subq_start;

            for (i, c) in from_clause[subq_start..].char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            subq_end = subq_start + i + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if subq_end > subq_start {
                let subquery = from_clause[subq_start + 1..subq_end - 1].trim().to_string();
                results.push((abs_pos, subq_end, subquery));
            }

            search_start = subq_end;
        } else {
            search_start = abs_pos + 7; // len("LATERAL")
        }
    }

    results
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref_parse() {
        let ref1 = ColumnRef::parse("c.id");
        assert_eq!(ref1.table_alias, Some("c".to_string()));
        assert_eq!(ref1.column_name, "id");

        let ref2 = ColumnRef::parse("column_name");
        assert_eq!(ref2.table_alias, None);
        assert_eq!(ref2.column_name, "column_name");
    }

    #[test]
    fn test_lateral_join_builder() {
        let join = LateralJoinBuilder::new()
            .cross_join()
            .subquery("SELECT * FROM orders WHERE customer_id = c.id LIMIT 3")
            .alias("recent_orders")
            .lateral_ref("c", "id")
            .build()
            .unwrap();

        assert_eq!(join.join_type, LateralJoinType::Cross);
        assert_eq!(join.subquery.alias, "recent_orders");
        assert_eq!(join.subquery.lateral_refs.len(), 1);
    }

    #[test]
    fn test_lateral_join_to_sql() {
        let subquery = LateralSubquery::new(
            "SELECT * FROM orders WHERE customer_id = c.id".to_string(),
            "o".to_string(),
        );
        let join = LateralJoin::cross(subquery);

        let sql = join.to_sql();
        assert!(sql.contains("CROSS JOIN LATERAL"));
        assert!(sql.contains("AS o"));
    }

    #[test]
    fn test_left_lateral_join() {
        let join = LateralJoinBuilder::new()
            .left_join()
            .subquery("SELECT * FROM orders WHERE customer_id = c.id")
            .alias("o")
            .build()
            .unwrap();

        let sql = join.to_sql();
        assert!(sql.contains("LEFT JOIN LATERAL"));
        assert!(sql.contains("ON true"));
    }

    #[test]
    fn test_inner_lateral_join() {
        let join = LateralJoinBuilder::new()
            .inner_join()
            .subquery("SELECT * FROM orders WHERE customer_id = c.id")
            .alias("o")
            .on("o.total > 100")
            .build()
            .unwrap();

        let sql = join.to_sql();
        assert!(sql.contains("INNER JOIN LATERAL"));
        assert!(sql.contains("ON o.total > 100"));
    }

    #[test]
    fn test_lateral_analyzer() {
        let mut analyzer = LateralJoinAnalyzer::new();
        analyzer.add_table("customers", HashSet::from(["id".into(), "name".into()]));
        analyzer.add_table("products", HashSet::from(["id".into(), "price".into()]));

        // Valid reference
        let valid_ref = ColumnRef::new(Some("customers".to_string()), "id".to_string());
        assert!(analyzer.validate_ref(&valid_ref).is_ok());

        // Invalid reference
        let invalid_ref = ColumnRef::new(Some("unknown_table".to_string()), "id".to_string());
        assert!(analyzer.validate_ref(&invalid_ref).is_err());
    }

    #[test]
    fn test_lateral_ref_extractor() {
        let mut extractor = LateralRefExtractor::new();
        let sql = "SELECT * FROM orders WHERE customer_id = c.id AND total > c.min_order";
        let outer_tables = vec!["c".to_string()];

        let refs = extractor.extract_refs(sql, &outer_tables);

        assert_eq!(refs.len(), 2);
        assert!(refs.iter().any(|r| r.column_name == "id"));
        assert!(refs.iter().any(|r| r.column_name == "min_order"));
    }

    #[test]
    fn test_set_returning_functions() {
        let unnest = SetReturningFunctions::unnest("t.arr");
        assert_eq!(unnest.to_sql(Some("elem")), "unnest(t.arr) AS elem");

        let series = SetReturningFunctions::generate_series("1", "10", Some("2"));
        assert!(series.to_sql(None).contains("generate_series(1, 10, 2)"));

        let json = SetReturningFunctions::jsonb_each("data");
        assert_eq!(json.return_columns, vec!["key", "value"]);
    }

    #[test]
    fn test_contains_lateral() {
        assert!(contains_lateral("SELECT * FROM t, LATERAL (SELECT 1)"));
        assert!(contains_lateral(
            "SELECT * FROM t LEFT JOIN LATERAL (SELECT 1) x ON true"
        ));
        assert!(!contains_lateral("SELECT * FROM t"));
    }

    #[test]
    fn test_extract_lateral_subqueries() {
        let from_clause = "customers c, LATERAL (SELECT * FROM orders WHERE cust_id = c.id) o";
        let subqueries = extract_lateral_subqueries(from_clause);

        assert_eq!(subqueries.len(), 1);
        assert!(subqueries[0].2.contains("SELECT * FROM orders"));
    }

    #[test]
    fn test_execution_plan() {
        let subquery = LateralSubquery::new(
            "SELECT * FROM orders WHERE customer_id = c.id".to_string(),
            "o".to_string(),
        );
        let join = LateralJoin::cross(subquery);

        let plan = LateralQueryRewriter::create_plan("c", &join);

        assert_eq!(plan.outer_source, "c");
        assert_eq!(plan.join_type, LateralJoinType::Cross);
    }

    #[test]
    fn test_param_substitution() {
        let subquery = "SELECT * FROM orders WHERE customer_id = c.id";
        let bindings = vec![(
            ColumnRef::new(Some("c".to_string()), "id".to_string()),
            "42".to_string(),
        )];

        let result = LateralQueryRewriter::substitute_params(subquery, &bindings);
        assert_eq!(result, "SELECT * FROM orders WHERE customer_id = 42");
    }

    #[test]
    fn test_lateral_subquery_with_column_aliases() {
        let subquery = LateralSubquery::new(
            "SELECT order_id, total FROM orders".to_string(),
            "recent".to_string(),
        )
        .with_column_aliases(vec!["id".to_string(), "amount".to_string()]);

        let join = LateralJoin::cross(subquery);
        let sql = join.to_sql();

        assert!(sql.contains("AS recent(id, amount)"));
    }

    #[test]
    fn test_table_source() {
        let table = TableSource::Table {
            schema: Some("public".to_string()),
            name: "users".to_string(),
            alias: Some("u".to_string()),
        };
        assert_eq!(table.alias(), Some("u"));
        assert!(!table.is_lateral());

        let lateral = TableSource::Lateral(LateralSubquery::new(
            "SELECT 1".to_string(),
            "x".to_string(),
        ));
        assert_eq!(lateral.alias(), Some("x"));
        assert!(lateral.is_lateral());
    }
}
