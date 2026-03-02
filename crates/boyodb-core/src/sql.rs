use crate::engine::EngineError;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Join,
    JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, UnaryOperator, Value, With,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parsed SQL query representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub database: Option<String>,
    pub table: Option<String>,
    pub projection: Option<Vec<String>>,
    pub filter: QueryFilter,
    pub aggregation: Option<AggPlan>,
    pub order_by: Option<Vec<OrderByClause>>,
    pub distinct: bool,
    pub joins: Vec<JoinClause>,
    /// Computed columns with expressions (SELECT expr AS alias, ...)
    pub computed_columns: Vec<SelectColumn>,
    /// CTEs defined in WITH clause
    pub ctes: Vec<CteDefinition>,
    /// TABLESAMPLE clause for statistical sampling
    pub sample: Option<SampleClause>,
}

/// TABLESAMPLE clause for statistical sampling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleClause {
    /// Sampling method
    pub method: SampleMethod,
    /// Sample size (percentage or row count)
    pub size: f64,
    /// Optional seed for reproducible sampling
    pub seed: Option<u64>,
}

/// Sampling methods supported
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleMethod {
    /// BERNOULLI - row-level random sampling
    Bernoulli,
    /// SYSTEM - block-level random sampling (faster)
    System,
    /// RESERVOIR - reservoir sampling for exact row counts
    Reservoir,
}

/// JOIN clause representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub database: String,
    pub alias: Option<String>,
    pub on_condition: JoinCondition,
}

/// Supported JOIN types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

/// JOIN ON condition (equi-join only for now)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_column: String,
    pub right_column: String,
}

/// ORDER BY clause representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByClause {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

/// Numeric comparison operator for range filters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumericOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A numeric filter on a column (for segment pruning)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericFilter {
    pub column: String,
    pub op: NumericOp,
    pub value: NumericValue,
}

/// Numeric value types for filters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumericValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
}

impl NumericValue {
    /// Convert to PrimitiveValue for comparison with column stats
    pub fn to_primitive(&self) -> crate::replication::PrimitiveValue {
        match self {
            NumericValue::Int64(v) => crate::replication::PrimitiveValue::Int64(*v),
            NumericValue::UInt64(v) => crate::replication::PrimitiveValue::UInt64(*v),
            NumericValue::Float64(v) => crate::replication::PrimitiveValue::Float64(*v),
        }
    }
}

/// Filter conditions extracted from WHERE clause
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryFilter {
    pub watermark_ge: Option<u64>,
    pub watermark_le: Option<u64>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub event_time_ge: Option<u64>,
    pub event_time_le: Option<u64>,
    pub tenant_id_eq: Option<u64>,
    pub route_id_eq: Option<u64>,
    /// tenant_id IN (values) filter
    #[serde(default)]
    pub tenant_id_in: Option<Vec<u64>>,
    /// route_id IN (values) filter
    #[serde(default)]
    pub route_id_in: Option<Vec<u64>>,
    /// Generic numeric IN filters: Vec<(column, values)>
    #[serde(default)]
    pub numeric_in_filters: Vec<(String, Vec<i64>)>,
    /// LIKE patterns: Vec<(column, pattern, negate)>
    pub like_filters: Vec<(String, String, bool)>,
    /// IS NULL checks: Vec<(column, is_null)> - is_null=true means IS NULL, false means IS NOT NULL
    pub null_filters: Vec<(String, bool)>,
    /// Generic string equality filters: Vec<(column, value)>
    pub string_eq_filters: Vec<(String, String)>,
    /// Generic string IN filters: Vec<(column, values)>
    pub string_in_filters: Vec<(String, Vec<String>)>,
    /// IN subquery filters: Vec<(column, subquery SQL, negated)>
    pub in_subquery_filters: Vec<(String, String, bool)>,
    /// EXISTS subquery filters: Vec<(subquery SQL, negated)>
    pub exists_subqueries: Vec<(String, bool)>,
    /// Scalar subquery comparisons: Vec<(column, op, subquery SQL)>
    pub scalar_subquery_filters: Vec<(String, String, String)>,
    /// Numeric equality filters: Vec<(column, value)> for segment pruning
    #[serde(default)]
    pub numeric_eq_filters: Vec<(String, NumericValue)>,
    /// Numeric range filters for segment pruning (column, op, value)
    #[serde(default)]
    pub numeric_range_filters: Vec<NumericFilter>,
    /// ORDER BY clause (moved from separate field for consistency)
    #[serde(default)]
    pub order_by: Option<Vec<OrderByClause>>,
    /// DISTINCT flag
    #[serde(default)]
    pub distinct: bool,
}

/// Aggregation function type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggKind {
    CountStar,
    /// COUNT(DISTINCT column)
    CountDistinct { column: String },
    Sum { column: String },
    Avg { column: String },
    Min { column: String },
    Max { column: String },
    /// STDDEV/STDDEV_SAMP - sample standard deviation
    StddevSamp { column: String },
    /// STDDEV_POP - population standard deviation
    StddevPop { column: String },
    /// VARIANCE/VAR_SAMP - sample variance
    VarianceSamp { column: String },
    /// VAR_POP - population variance
    VariancePop { column: String },
    /// APPROX_COUNT_DISTINCT - HyperLogLog distinct count
    ApproxCountDistinct { column: String },
}

/// HAVING clause condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HavingCondition {
    pub agg: AggKind,
    pub op: HavingOp,
    pub value: f64,
}

/// Comparison operators for HAVING
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HavingOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Aggregation plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggPlan {
    pub group_by: GroupBy,
    pub aggs: Vec<AggKind>,
    /// HAVING clause conditions (all must be satisfied - AND logic)
    pub having: Vec<HavingCondition>,
}

/// Supported GROUP BY column identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupByColumn {
    TenantId,
    RouteId,
}

/// Group by clause type - supports single or multiple columns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupBy {
    None,
    /// Legacy single-column groupings
    Tenant,
    Route,
    /// Multiple columns grouped together (e.g., GROUP BY tenant_id, route_id)
    Columns(Vec<GroupByColumn>),
}

/// Set operation type (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

/// A compound query with set operations (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone)]
pub struct SetOperationQuery {
    /// The type of set operation
    pub op: SetOpType,
    /// Left side query
    pub left: Box<SqlStatement>,
    /// Right side query
    pub right: Box<SqlStatement>,
}

/// Scalar function types for computed columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalarFunction {
    // String functions
    Upper(Box<SelectExpr>),
    Lower(Box<SelectExpr>),
    Length(Box<SelectExpr>),
    Trim(Box<SelectExpr>),
    LTrim(Box<SelectExpr>),
    RTrim(Box<SelectExpr>),
    Concat(Vec<SelectExpr>),
    Substring {
        expr: Box<SelectExpr>,
        start: i64,
        length: Option<i64>,
    },
    Replace {
        expr: Box<SelectExpr>,
        from: String,
        to: String,
    },
    Left {
        expr: Box<SelectExpr>,
        count: i64,
    },
    Right {
        expr: Box<SelectExpr>,
        count: i64,
    },
    Reverse(Box<SelectExpr>),
    Coalesce(Vec<SelectExpr>),

    // Math functions
    Abs(Box<SelectExpr>),
    Round {
        expr: Box<SelectExpr>,
        precision: Option<i32>,
    },
    Ceil(Box<SelectExpr>),
    Floor(Box<SelectExpr>),
    Mod {
        dividend: Box<SelectExpr>,
        divisor: Box<SelectExpr>,
    },
    Power {
        base: Box<SelectExpr>,
        exponent: Box<SelectExpr>,
    },
    Sqrt(Box<SelectExpr>),
    Log {
        expr: Box<SelectExpr>,
        base: Option<f64>,
    },
    Ln(Box<SelectExpr>),
    Exp(Box<SelectExpr>),
    Sign(Box<SelectExpr>),
    Greatest(Vec<SelectExpr>),
    Least(Vec<SelectExpr>),

    // Date/Time functions
    Now,
    CurrentTimestamp,
    CurrentDate,
    DateTrunc {
        unit: String,
        expr: Box<SelectExpr>,
    },
    Extract {
        field: String,
        expr: Box<SelectExpr>,
    },
    DateAdd {
        expr: Box<SelectExpr>,
        interval: i64,
        unit: String,
    },
    DateSub {
        expr: Box<SelectExpr>,
        interval: i64,
        unit: String,
    },
    DateDiff {
        unit: String,
        start: Box<SelectExpr>,
        end: Box<SelectExpr>,
    },
    ToTimestamp(Box<SelectExpr>),
    FromUnixtime(Box<SelectExpr>),

    // Type casting
    Cast {
        expr: Box<SelectExpr>,
        target_type: String,
    },

    // JSON functions
    /// Extract a value from a JSON string using a path (e.g., JSON_EXTRACT(col, '$.key'))
    JsonExtract {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Extract a scalar value from JSON as text
    JsonExtractScalar {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Create a JSON array from values
    JsonArray(Vec<SelectExpr>),
    /// Create a JSON object from key-value pairs
    JsonObject(Vec<(String, SelectExpr)>),
    /// Get the type of a JSON value
    JsonType(Box<SelectExpr>),
    /// Check if a path exists in JSON
    JsonContainsPath {
        expr: Box<SelectExpr>,
        path: String,
    },
    /// Get array length for JSON arrays
    JsonArrayLength(Box<SelectExpr>),
    /// Get object keys as array
    JsonKeys(Box<SelectExpr>),
    /// Check if valid JSON
    JsonValid(Box<SelectExpr>),
    /// Pretty print JSON
    JsonPretty(Box<SelectExpr>),

    // Array functions
    /// Create an array from values
    Array(Vec<SelectExpr>),
    /// Get array length
    ArrayLength(Box<SelectExpr>),
    /// Check if array contains value
    ArrayContains {
        array: Box<SelectExpr>,
        value: Box<SelectExpr>,
    },
    /// Get element at index
    ArrayElement {
        array: Box<SelectExpr>,
        index: i64,
    },
    /// Append element to array
    ArrayAppend {
        array: Box<SelectExpr>,
        element: Box<SelectExpr>,
    },
    /// Concatenate arrays
    ArrayConcat(Vec<SelectExpr>),
    /// Get distinct elements
    ArrayDistinct(Box<SelectExpr>),
    /// Join array elements into string
    ArrayJoin {
        array: Box<SelectExpr>,
        delimiter: String,
    },
}

/// Window function types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    NTile(i64),
    Lag {
        expr: Box<SelectExpr>,
        offset: i64,
        default: Option<Box<SelectExpr>>,
    },
    Lead {
        expr: Box<SelectExpr>,
        offset: i64,
        default: Option<Box<SelectExpr>>,
    },
    FirstValue(Box<SelectExpr>),
    LastValue(Box<SelectExpr>),
    NthValue {
        expr: Box<SelectExpr>,
        n: i64,
    },
    // Window versions of aggregate functions
    WindowSum(Box<SelectExpr>),
    WindowAvg(Box<SelectExpr>),
    WindowMin(Box<SelectExpr>),
    WindowMax(Box<SelectExpr>),
    WindowCount(Option<Box<SelectExpr>>),
}

/// Window specification (OVER clause)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderByClause>,
    pub frame: Option<WindowFrame>,
}

/// Window frame specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// Represents an expression in SELECT clause (column, function, or computed value)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectExpr {
    /// Simple column reference
    Column(String),
    /// Qualified column reference (table.column)
    QualifiedColumn { table: String, column: String },
    /// Literal value
    Literal(LiteralValue),
    /// Scalar function call
    Function(ScalarFunction),
    /// Aggregate function (for use in expressions)
    Aggregate(AggKind),
    /// Window function with OVER clause
    Window {
        function: WindowFunction,
        spec: WindowSpec,
    },
    /// Binary operation (a + b, a - b, etc.)
    BinaryOp {
        left: Box<SelectExpr>,
        op: String,
        right: Box<SelectExpr>,
    },
    /// Unary operation (-a, NOT a)
    UnaryOp {
        op: String,
        expr: Box<SelectExpr>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<SelectExpr>>,
        when_clauses: Vec<(SelectExpr, SelectExpr)>,
        else_result: Option<Box<SelectExpr>>,
    },
    /// NULL literal
    Null,
    /// Subquery (for scalar subqueries)
    Subquery(Box<ParsedQuery>),
}

/// Literal value types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// Common Table Expression (CTE) definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CteDefinition {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: ParsedQuery,
    /// Whether this is a recursive CTE (WITH RECURSIVE)
    pub recursive: bool,
    /// Raw SQL query for the CTE (used for recursive execution)
    pub raw_sql: String,
}

/// Extended parsed query with CTE support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQueryWithCtes {
    pub ctes: Vec<CteDefinition>,
    pub query: ParsedQuery,
}

/// SELECT item with expression and optional alias
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectColumn {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

/// DDL command types
#[derive(Debug, Clone)]
pub enum DdlCommand {
    CreateDatabase {
        name: String,
    },
    CreateTable {
        database: String,
        table: String,
        schema_json: Option<String>,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    DropTable {
        database: String,
        table: String,
        if_exists: bool,
    },
    TruncateTable {
        database: String,
        table: String,
    },
    AlterTableAddColumn {
        database: String,
        table: String,
        column: String,
        data_type: String,
        nullable: bool,
    },
    AlterTableDropColumn {
        database: String,
        table: String,
        column: String,
    },
    /// SHOW DATABASES
    ShowDatabases,
    /// SHOW TABLES [IN database]
    ShowTables {
        database: Option<String>,
    },
    /// DESCRIBE TABLE database.table
    DescribeTable {
        database: String,
        table: String,
    },
    /// CREATE VIEW [database.]view AS SELECT ...
    CreateView {
        database: String,
        name: String,
        /// The SQL query that defines the view
        query_sql: String,
        /// If true, replace existing view with same name
        or_replace: bool,
    },
    /// DROP VIEW [IF EXISTS] [database.]view
    DropView {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// SHOW VIEWS [IN database]
    ShowViews {
        database: Option<String>,
    },
    /// CREATE MATERIALIZED VIEW [database.]view AS SELECT ...
    CreateMaterializedView {
        database: String,
        name: String,
        /// The SQL query that defines the materialized view
        query_sql: String,
        /// If true, replace existing materialized view with same name
        or_replace: bool,
    },
    /// DROP MATERIALIZED VIEW [IF EXISTS] [database.]view
    DropMaterializedView {
        database: String,
        name: String,
        if_exists: bool,
    },
    /// REFRESH MATERIALIZED VIEW [database.]view
    RefreshMaterializedView {
        database: String,
        name: String,
    },
    /// SHOW MATERIALIZED VIEWS [IN database]
    ShowMaterializedViews {
        database: Option<String>,
    },
    /// CREATE INDEX [IF NOT EXISTS] index_name ON table (columns) [USING method]
    CreateIndex {
        database: String,
        table: String,
        index_name: String,
        columns: Vec<String>,
        index_type: IndexType,
        if_not_exists: bool,
    },
    /// DROP INDEX [IF EXISTS] index_name ON table
    DropIndex {
        database: String,
        table: String,
        index_name: String,
        if_exists: bool,
    },
    /// SHOW INDEXES [IN database.table]
    ShowIndexes {
        database: Option<String>,
        table: Option<String>,
    },
    /// ANALYZE TABLE database.table - Collect statistics
    AnalyzeTable {
        database: String,
        table: String,
    },
    /// VACUUM [FULL] database.table - Reclaim storage
    Vacuum {
        database: String,
        table: String,
        full: bool,
    },
    /// DEDUPLICATE database.table - Remove duplicate rows based on configured keys
    Deduplicate {
        database: String,
        table: String,
    },
    /// ALTER TABLE ... SET DEDUPLICATION (key_columns) [VERSION version_col] [MODE mode]
    SetDeduplication {
        database: String,
        table: String,
        config: Option<DeduplicationConfig>,
    },
}

/// Index type for CREATE INDEX
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexType {
    /// B-tree index (default) - good for range queries
    BTree,
    /// Hash index - good for equality queries
    Hash,
    /// Bloom filter index - good for membership testing
    Bloom,
    /// Bitmap index - good for low-cardinality columns
    Bitmap,
}

/// Deduplication configuration for a table
/// Supports merge-on-read deduplication with immediate dedup option
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeduplicationConfig {
    /// Columns that form the deduplication key (like a primary key)
    pub key_columns: Vec<String>,
    /// Optional version column - when duplicates found, keep row with highest version
    pub version_column: Option<String>,
    /// When to perform deduplication
    pub mode: DeduplicationMode,
}

/// When to perform deduplication
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeduplicationMode {
    /// Deduplicate immediately on ingest (slower ingest, faster queries)
    OnIngest,
    /// Deduplicate during compaction only (faster ingest, may have temp duplicates)
    OnCompaction,
    /// Deduplicate both on ingest and compaction (balanced)
    Both,
}

impl Default for DeduplicationMode {
    fn default() -> Self {
        DeduplicationMode::OnCompaction
    }
}

/// Authentication/Authorization command types
#[derive(Debug, Clone)]
pub enum AuthCommand {
    /// CREATE USER username WITH PASSWORD 'password' [options]
    CreateUser {
        username: String,
        password: String,
        options: UserOptions,
    },
    /// DROP USER username
    DropUser { username: String },
    /// ALTER USER username SET PASSWORD 'newpassword'
    AlterUserPassword {
        username: String,
        new_password: String,
    },
    /// ALTER USER username SET SUPERUSER [TRUE|FALSE]
    AlterUserSuperuser {
        username: String,
        is_superuser: bool,
    },
    /// ALTER USER username SET DEFAULT DATABASE 'dbname'
    AlterUserDefaultDb {
        username: String,
        database: Option<String>,
    },
    /// LOCK USER username
    LockUser { username: String },
    /// UNLOCK USER username
    UnlockUser { username: String },
    /// GRANT privilege ON target TO user/role [WITH GRANT OPTION]
    Grant {
        privileges: Vec<String>,
        target_type: GrantTargetType,
        target_name: Option<String>,
        grantee: String,
        grantee_is_role: bool,
        with_grant_option: bool,
    },
    /// REVOKE privilege ON target FROM user/role
    Revoke {
        privileges: Vec<String>,
        target_type: GrantTargetType,
        target_name: Option<String>,
        grantee: String,
        grantee_is_role: bool,
    },
    /// GRANT role TO user
    GrantRole { role: String, username: String },
    /// REVOKE role FROM user
    RevokeRole { role: String, username: String },
    /// CREATE ROLE rolename [WITH description]
    CreateRole {
        name: String,
        description: Option<String>,
    },
    /// DROP ROLE rolename
    DropRole { name: String },
    /// SHOW USERS
    ShowUsers,
    /// SHOW ROLES
    ShowRoles,
    /// SHOW GRANTS FOR user
    ShowGrants { username: String },
}

/// User creation options
#[derive(Debug, Clone, Default)]
pub struct UserOptions {
    pub superuser: bool,
    pub default_database: Option<String>,
    pub connection_limit: Option<u32>,
}

/// Target type for GRANT/REVOKE
#[derive(Debug, Clone)]
pub enum GrantTargetType {
    /// All objects (no specific target)
    Global,
    /// DATABASE dbname
    Database,
    /// TABLE dbname.tablename or ALL TABLES IN DATABASE dbname
    Table,
    /// ALL TABLES IN DATABASE dbname
    AllTablesInDatabase,
}

/// Parse a SQL statement and return a parsed query or DDL command
pub fn parse_sql(sql: &str) -> Result<SqlStatement, EngineError> {
    // First, try to parse as an auth command (custom syntax)
    if let Some(auth_cmd) = try_parse_auth_command(sql)? {
        return Ok(SqlStatement::Auth(auth_cmd));
    }

    // Try to parse SHOW commands (SHOW DATABASES, SHOW TABLES)
    if let Some(ddl_cmd) = try_parse_show_command(sql)? {
        return Ok(SqlStatement::Ddl(ddl_cmd));
    }

    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(e) => {
            if let Some(stmt) = parse_drop_database_fallback(sql) {
                return Ok(stmt);
            }
            return Err(EngineError::InvalidArgument(format!("SQL parse error: {e}")));
        }
    };

    if statements.is_empty() {
        return Err(EngineError::InvalidArgument("empty SQL statement".into()));
    }

    if statements.len() > 1 {
        return Err(EngineError::InvalidArgument(
            "multiple SQL statements not supported".into(),
        ));
    }

    parse_statement(&statements[0])
}

/// Try to parse SHOW DATABASES, SHOW TABLES, or DESCRIBE commands
fn try_parse_show_command(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let upper_trimmed = upper.trim_end_matches(';');

    // SHOW DATABASES
    if upper_trimmed == "SHOW DATABASES" {
        return Ok(Some(DdlCommand::ShowDatabases));
    }

    // SHOW TABLES [IN database]
    if upper_trimmed == "SHOW TABLES" {
        return Ok(Some(DdlCommand::ShowTables { database: None }));
    }

    // SHOW TABLES IN database_name
    if upper_trimmed.starts_with("SHOW TABLES IN ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowTables {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // SHOW TABLES FROM database_name (MySQL-style alias)
    if upper_trimmed.starts_with("SHOW TABLES FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowTables {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // DESCRIBE TABLE database.table or DESC database.table
    if upper_trimmed.starts_with("DESCRIBE TABLE ") || upper_trimmed.starts_with("DESC TABLE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 3 {
            let table_name = tokens[2].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // DESCRIBE database.table (without TABLE keyword)
    if upper_trimmed.starts_with("DESCRIBE ") && !upper_trimmed.starts_with("DESCRIBE TABLE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 2 {
            let table_name = tokens[1].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // DESC database.table (shorthand without TABLE keyword)
    if upper_trimmed.starts_with("DESC ") && !upper_trimmed.starts_with("DESC TABLE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 2 {
            let table_name = tokens[1].trim_end_matches(';');
            let (database, table) = parse_table_name(table_name)?;
            return Ok(Some(DdlCommand::DescribeTable { database, table }));
        }
    }

    // SHOW VIEWS [IN database]
    if upper_trimmed == "SHOW VIEWS" {
        return Ok(Some(DdlCommand::ShowViews { database: None }));
    }

    if upper_trimmed.starts_with("SHOW VIEWS IN ") || upper_trimmed.starts_with("SHOW VIEWS FROM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let db_name = tokens[3].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowViews {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // SHOW MATERIALIZED VIEWS [IN database]
    if upper_trimmed == "SHOW MATERIALIZED VIEWS" {
        return Ok(Some(DdlCommand::ShowMaterializedViews { database: None }));
    }

    if upper_trimmed.starts_with("SHOW MATERIALIZED VIEWS IN ")
        || upper_trimmed.starts_with("SHOW MATERIALIZED VIEWS FROM ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 5 {
            let db_name = tokens[4].trim_end_matches(';');
            return Ok(Some(DdlCommand::ShowMaterializedViews {
                database: Some(db_name.to_string()),
            }));
        }
    }

    // CREATE MATERIALIZED VIEW [OR REPLACE] [database.]name AS SELECT ...
    if upper_trimmed.starts_with("CREATE MATERIALIZED VIEW ")
        || upper_trimmed.starts_with("CREATE OR REPLACE MATERIALIZED VIEW ")
    {
        return parse_create_materialized_view(sql);
    }

    // DROP MATERIALIZED VIEW [IF EXISTS] [database.]name
    if upper_trimmed.starts_with("DROP MATERIALIZED VIEW ") {
        return parse_drop_materialized_view(sql);
    }

    // REFRESH MATERIALIZED VIEW [database.]name
    if upper_trimmed.starts_with("REFRESH MATERIALIZED VIEW ") {
        return parse_refresh_materialized_view(sql);
    }

    // SHOW INDEXES [IN database.table]
    if upper_trimmed == "SHOW INDEXES" || upper_trimmed == "SHOW INDEX" {
        return Ok(Some(DdlCommand::ShowIndexes {
            database: None,
            table: None,
        }));
    }

    if upper_trimmed.starts_with("SHOW INDEXES IN ")
        || upper_trimmed.starts_with("SHOW INDEX IN ")
        || upper_trimmed.starts_with("SHOW INDEXES FROM ")
        || upper_trimmed.starts_with("SHOW INDEX FROM ")
    {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() >= 4 {
            let name = tokens[3].trim_end_matches(';');
            let (database, table) = parse_table_name(name)?;
            return Ok(Some(DdlCommand::ShowIndexes {
                database: Some(database),
                table: Some(table),
            }));
        }
    }

    // VACUUM [FULL] database.table
    if upper_trimmed.starts_with("VACUUM ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(EngineError::InvalidArgument(
                "VACUUM requires table name".into(),
            ));
        }

        let (full, table_idx) = if tokens.len() >= 2
            && tokens[1].eq_ignore_ascii_case("FULL")
        {
            (true, 2)
        } else {
            (false, 1)
        };

        if table_idx >= tokens.len() {
            return Err(EngineError::InvalidArgument(
                "VACUUM requires table name".into(),
            ));
        }

        let table_name = tokens[table_idx].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::Vacuum {
            database,
            table,
            full,
        }));
    }

    // DEDUPLICATE database.table - Run deduplication on a table
    if upper_trimmed.starts_with("DEDUPLICATE ") {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(EngineError::InvalidArgument(
                "DEDUPLICATE requires table name".into(),
            ));
        }
        let table_name = tokens[1].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::Deduplicate { database, table }));
    }

    // ALTER TABLE ... SET DEDUPLICATION (key_columns) [VERSION col] [MODE mode]
    // or ALTER TABLE ... DROP DEDUPLICATION
    if upper_trimmed.starts_with("ALTER TABLE ") && upper_trimmed.contains("DEDUPLICATION") {
        return parse_set_deduplication(sql);
    }

    Ok(None)
}

/// Parse CREATE MATERIALIZED VIEW command
fn parse_create_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let or_replace = upper.contains("OR REPLACE");

    // Find the AS keyword to split view name from query
    let as_pos = upper.find(" AS ").ok_or_else(|| {
        EngineError::InvalidArgument(
            "CREATE MATERIALIZED VIEW requires AS keyword".into(),
        )
    })?;

    // Extract the view name part
    let prefix = if or_replace {
        "CREATE OR REPLACE MATERIALIZED VIEW"
    } else {
        "CREATE MATERIALIZED VIEW"
    };
    let after_prefix = sql[prefix.len()..as_pos].trim();
    let view_name = after_prefix.trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    // Extract the query SQL
    let query_sql = sql[as_pos + 4..].trim().trim_end_matches(';').to_string();

    Ok(Some(DdlCommand::CreateMaterializedView {
        database,
        name,
        query_sql,
        or_replace,
    }))
}

/// Parse DROP MATERIALIZED VIEW command
fn parse_drop_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // DROP MATERIALIZED VIEW [IF EXISTS] name

    let mut idx = 3; // Start after "DROP MATERIALIZED VIEW"
    let mut if_exists = false;

    if tokens.len() > idx + 1
        && tokens[idx].eq_ignore_ascii_case("IF")
        && tokens[idx + 1].eq_ignore_ascii_case("EXISTS")
    {
        if_exists = true;
        idx += 2;
    }

    if idx >= tokens.len() {
        return Err(EngineError::InvalidArgument(
            "DROP MATERIALIZED VIEW requires view name".into(),
        ));
    }

    let view_name = tokens[idx].trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    Ok(Some(DdlCommand::DropMaterializedView {
        database,
        name,
        if_exists,
    }))
}

/// Parse REFRESH MATERIALIZED VIEW command
fn parse_refresh_materialized_view(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // REFRESH MATERIALIZED VIEW name

    if tokens.len() < 4 {
        return Err(EngineError::InvalidArgument(
            "REFRESH MATERIALIZED VIEW requires view name".into(),
        ));
    }

    let view_name = tokens[3].trim_end_matches(';');
    let (database, name) = parse_table_name(view_name)?;

    Ok(Some(DdlCommand::RefreshMaterializedView { database, name }))
}

/// Parse ALTER TABLE ... SET DEDUPLICATION or DROP DEDUPLICATION
fn parse_set_deduplication(sql: &str) -> Result<Option<DdlCommand>, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    // ALTER TABLE table_name DROP DEDUPLICATION
    if upper.contains("DROP DEDUPLICATION") {
        if tokens.len() < 4 {
            return Err(EngineError::InvalidArgument(
                "ALTER TABLE DROP DEDUPLICATION requires table name".into(),
            ));
        }
        let table_name = tokens[2].trim_end_matches(';');
        let (database, table) = parse_table_name(table_name)?;
        return Ok(Some(DdlCommand::SetDeduplication {
            database,
            table,
            config: None,
        }));
    }

    // ALTER TABLE table_name SET DEDUPLICATION (col1, col2) [VERSION version_col] [MODE mode]
    if !upper.contains("SET DEDUPLICATION") {
        return Ok(None);
    }

    if tokens.len() < 5 {
        return Err(EngineError::InvalidArgument(
            "ALTER TABLE SET DEDUPLICATION requires table name and key columns".into(),
        ));
    }

    let table_name = tokens[2];
    let (database, table) = parse_table_name(table_name)?;

    // Find opening parenthesis for key columns
    let sql_after_set = if let Some(pos) = upper.find("SET DEDUPLICATION") {
        &sql[pos + "SET DEDUPLICATION".len()..]
    } else {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires key columns in parentheses".into(),
        ));
    };

    let trimmed = sql_after_set.trim();
    if !trimmed.starts_with('(') {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires key columns in parentheses".into(),
        ));
    }

    // Find closing parenthesis
    let close_paren = trimmed.find(')').ok_or_else(|| {
        EngineError::InvalidArgument("SET DEDUPLICATION: missing closing parenthesis".into())
    })?;

    let cols_str = &trimmed[1..close_paren];
    let key_columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if key_columns.is_empty() {
        return Err(EngineError::InvalidArgument(
            "SET DEDUPLICATION requires at least one key column".into(),
        ));
    }

    let after_cols = &trimmed[close_paren + 1..];
    let after_tokens: Vec<&str> = after_cols.split_whitespace().collect();

    // Parse optional VERSION column
    let mut version_column = None;
    let mut mode = DeduplicationMode::OnCompaction;

    let mut i = 0;
    while i < after_tokens.len() {
        let token_upper = after_tokens[i].to_uppercase();
        if token_upper == "VERSION" && i + 1 < after_tokens.len() {
            version_column = Some(after_tokens[i + 1].trim_end_matches(';').to_string());
            i += 2;
        } else if token_upper == "MODE" && i + 1 < after_tokens.len() {
            let mode_str = after_tokens[i + 1].to_uppercase();
            let mode_str = mode_str.trim_end_matches(';');
            mode = match mode_str {
                "ONINGEST" | "ON_INGEST" => DeduplicationMode::OnIngest,
                "ONCOMPACTION" | "ON_COMPACTION" => DeduplicationMode::OnCompaction,
                "BOTH" => DeduplicationMode::Both,
                _ => {
                    return Err(EngineError::InvalidArgument(format!(
                        "invalid deduplication mode: {}. Use ONINGEST, ONCOMPACTION, or BOTH",
                        after_tokens[i + 1]
                    )));
                }
            };
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(Some(DdlCommand::SetDeduplication {
        database,
        table,
        config: Some(DeduplicationConfig {
            key_columns,
            version_column,
            mode,
        }),
    }))
}

fn parse_drop_database_fallback(sql: &str) -> Option<SqlStatement> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 3 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("drop") || !tokens[1].eq_ignore_ascii_case("database") {
        return None;
    }
    let mut idx = 2;
    let mut if_exists = false;
    if tokens.len() >= 4
        && tokens[2].eq_ignore_ascii_case("if")
        && tokens[3].eq_ignore_ascii_case("exists")
    {
        if_exists = true;
        idx = 4;
    }
    let name = tokens.get(idx)?;
    let name_clean = name.trim_end_matches(';');
    Some(SqlStatement::Ddl(DdlCommand::DropDatabase {
        name: name_clean.to_string(),
        if_exists,
    }))
}

/// Try to parse a SQL string as an auth command
/// Returns None if not an auth command, Some(cmd) if parsed successfully, Err on parse error
fn try_parse_auth_command(sql: &str) -> Result<Option<AuthCommand>, EngineError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    if tokens.is_empty() {
        return Ok(None);
    }

    // CREATE USER username WITH PASSWORD 'password' [SUPERUSER] [DEFAULT DATABASE 'db'] [CONNECTION LIMIT n]
    if upper.starts_with("CREATE USER ") {
        return parse_create_user(sql).map(Some);
    }

    // DROP USER username
    if upper.starts_with("DROP USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("DROP USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::DropUser {
            username,
        }));
    }

    // ALTER USER username SET PASSWORD 'newpass'
    // ALTER USER username SET SUPERUSER TRUE/FALSE
    // ALTER USER username SET DEFAULT DATABASE 'dbname'
    if upper.starts_with("ALTER USER ") {
        return parse_alter_user(sql).map(Some);
    }

    // LOCK USER username
    if upper.starts_with("LOCK USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("LOCK USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::LockUser {
            username,
        }));
    }

    // UNLOCK USER username
    if upper.starts_with("UNLOCK USER ") {
        let username = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("UNLOCK USER requires username".into()))?;
        let username = unquote(username);
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::UnlockUser {
            username,
        }));
    }

    // CREATE ROLE rolename [WITH DESCRIPTION 'desc']
    if upper.starts_with("CREATE ROLE ") {
        return parse_create_role(sql).map(Some);
    }

    // DROP ROLE rolename
    if upper.starts_with("DROP ROLE ") {
        let name = tokens
            .get(2)
            .ok_or_else(|| EngineError::InvalidArgument("DROP ROLE requires role name".into()))?;
        let name = unquote(name);
        validate_ident(&name, false, "role name")?;
        return Ok(Some(AuthCommand::DropRole {
            name,
        }));
    }

    // GRANT ... TO ...
    if upper.starts_with("GRANT ") {
        return parse_grant(sql).map(Some);
    }

    // REVOKE ... FROM ...
    if upper.starts_with("REVOKE ") {
        return parse_revoke(sql).map(Some);
    }

    // SHOW USERS
    if upper == "SHOW USERS" || upper == "SHOW USERS;" {
        return Ok(Some(AuthCommand::ShowUsers));
    }

    // SHOW ROLES
    if upper == "SHOW ROLES" || upper == "SHOW ROLES;" {
        return Ok(Some(AuthCommand::ShowRoles));
    }

    // SHOW GRANTS FOR username
    if upper.starts_with("SHOW GRANTS FOR ") {
        let username = tokens.get(3).ok_or_else(|| {
            EngineError::InvalidArgument("SHOW GRANTS FOR requires username".into())
        })?;
        let username = unquote(username).trim_end_matches(';').to_string();
        validate_ident(&username, false, "username")?;
        return Ok(Some(AuthCommand::ShowGrants {
            username,
        }));
    }

    Ok(None)
}

/// Parse CREATE USER command
fn parse_create_user(sql: &str) -> Result<AuthCommand, EngineError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let username = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("CREATE USER requires username".into()))?;
    let username = unquote(username);
    validate_ident(&username, false, "username")?;

    // Locate the PASSWORD keyword by token to avoid substring collisions
    let password_token_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("PASSWORD"))
        .ok_or_else(|| EngineError::InvalidArgument("CREATE USER requires PASSWORD".into()))?;

    // Extract password (look for quoted string after PASSWORD)
    let after_password = tokens
        .iter()
        .skip(password_token_idx + 1)
        .fold(String::new(), |mut acc, t| {
            if !acc.is_empty() {
                acc.push(' ');
            }
            acc.push_str(t);
            acc
        });

    let password = extract_quoted_string(&after_password).ok_or_else(|| {
        EngineError::InvalidArgument("PASSWORD must be followed by a quoted string".into())
    })?;

    let mut options = UserOptions::default();

    // Parse options from remaining tokens, ignoring quoted strings
    let mut i = password_token_idx + 1;
    while i < tokens.len() {
        let tok = tokens[i];
        let upper = tok.to_uppercase();

        if upper == "SUPERUSER" {
            options.superuser = true;
            i += 1;
            continue;
        }

        if upper == "DEFAULT" {
            if let (Some(db_kw), Some(db_token)) = (tokens.get(i + 1), tokens.get(i + 2)) {
                if db_kw.eq_ignore_ascii_case("DATABASE") {
                    let database = unquote(db_token.trim_end_matches(';'));
                    validate_ident(&database, false, "database")?;
                    options.default_database.replace(database);
                    i += 3;
                    continue;
                }
            }
        }

        if upper == "CONNECTION" {
            if let (Some(limit_kw), Some(limit_token)) = (tokens.get(i + 1), tokens.get(i + 2)) {
                if limit_kw.eq_ignore_ascii_case("LIMIT") {
                    let numeric = limit_token
                        .trim_end_matches(';')
                        .trim_matches(|c: char| !c.is_ascii_digit());
                    if let Ok(limit) = numeric.parse() {
                        options.connection_limit = Some(limit);
                    }
                    i += 3;
                    continue;
                }
            }
        }

        i += 1;
    }

    Ok(AuthCommand::CreateUser {
        username,
        password,
        options,
    })
}

/// Parse ALTER USER command
fn parse_alter_user(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let username = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("ALTER USER requires username".into()))?;
    let username = unquote(username);
    validate_ident(&username, false, "username")?;

    // ALTER USER username SET PASSWORD 'newpass'
    if upper.contains("SET PASSWORD") {
        if let Some(password_idx) = upper.find("PASSWORD") {
            let after_password = &sql[password_idx + 8..];
            let new_password = extract_quoted_string(after_password).ok_or_else(|| {
                EngineError::InvalidArgument("PASSWORD must be followed by a quoted string".into())
            })?;
            return Ok(AuthCommand::AlterUserPassword {
                username,
                new_password,
            });
        }
    }

    // ALTER USER username SET SUPERUSER TRUE/FALSE
    if upper.contains("SET SUPERUSER") {
        let is_superuser = upper.contains("TRUE") || upper.contains("YES") || upper.contains(" 1");
        return Ok(AuthCommand::AlterUserSuperuser {
            username,
            is_superuser,
        });
    }

    // ALTER USER username SET DEFAULT DATABASE 'dbname'
    if upper.contains("SET DEFAULT DATABASE") {
        if let Some(idx) = upper.find("DATABASE") {
            let after_db = &sql[idx + 8..];
            let database = extract_quoted_string(after_db.trim());
            let database = database.ok_or_else(|| {
                EngineError::InvalidArgument(
                    "DEFAULT DATABASE must be followed by a quoted string".into(),
                )
            })?;
            validate_ident(&database, false, "database")?;
            return Ok(AuthCommand::AlterUserDefaultDb {
                username,
                database: Some(database),
            });
        }
    }

    Err(EngineError::InvalidArgument(
        "Unrecognized ALTER USER command".into(),
    ))
}

/// Parse CREATE ROLE command
fn parse_create_role(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = sql.split_whitespace().collect();

    let name = tokens
        .get(2)
        .ok_or_else(|| EngineError::InvalidArgument("CREATE ROLE requires role name".into()))?;
    let name = unquote(name);
    validate_ident(&name, false, "role name")?;

    let mut description = None;

    // Check for DESCRIPTION or WITH DESCRIPTION
    if let Some(idx) = upper.find("DESCRIPTION") {
        let after_desc = &sql[idx + 11..];
        description = extract_quoted_string(after_desc.trim());
    }

    Ok(AuthCommand::CreateRole { name, description })
}

/// Parse GRANT command
fn parse_grant(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();

    // Check if this is GRANT role TO user (no ON keyword)
    if !upper.contains(" ON ") {
        if let Some(to_idx) = upper.find(" TO ") {
            // GRANT rolename TO username
            let role = sql[6..to_idx].trim();
            let after_to = &sql[to_idx + 4..];
            let username = after_to.trim().trim_end_matches(';');
            let role = unquote(role);
            let username = unquote(username);
            validate_ident(&role, false, "role name")?;
            validate_ident(&username, false, "username")?;

            return Ok(AuthCommand::GrantRole {
                role,
                username,
            });
        }
    }

    // GRANT privilege(s) ON target TO grantee [WITH GRANT OPTION]
    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| EngineError::InvalidArgument("GRANT requires ON clause".into()))?;
    let to_idx = upper
        .find(" TO ")
        .ok_or_else(|| EngineError::InvalidArgument("GRANT requires TO clause".into()))?;

    let privileges_str = &sql[6..on_idx];
    let privileges: Vec<String> = privileges_str
        .split(',')
        .map(|p| p.trim().to_uppercase())
        .collect();

    let target_str = &sql[on_idx + 4..to_idx];
    let (target_type, target_name) = parse_grant_target(target_str)?;

    let after_to = &sql[to_idx + 4..];
    let with_grant = upper.contains("WITH GRANT OPTION");

    let grantee_end = if with_grant {
        upper
            .find("WITH GRANT")
            .map(|idx| idx - to_idx - 4)
            .unwrap_or(after_to.len())
    } else {
        after_to.len()
    };

    let grantee = after_to[..grantee_end].trim().trim_end_matches(';');
    let grantee_is_role = upper.contains("TO ROLE ");
    let grantee = unquote(
        grantee
            .trim_start_matches("ROLE ")
            .trim_start_matches("role "),
    );
    validate_ident(&grantee, false, "grantee")?;

    Ok(AuthCommand::Grant {
        privileges,
        target_type,
        target_name,
        grantee,
        grantee_is_role,
        with_grant_option: with_grant,
    })
}

/// Parse REVOKE command
fn parse_revoke(sql: &str) -> Result<AuthCommand, EngineError> {
    let upper = sql.to_uppercase();

    // Check if this is REVOKE role FROM user (no ON keyword)
    if !upper.contains(" ON ") {
        if let Some(from_idx) = upper.find(" FROM ") {
            // REVOKE rolename FROM username
            let role = sql[7..from_idx].trim();
            let after_from = &sql[from_idx + 6..];
            let username = after_from.trim().trim_end_matches(';');
            let role = unquote(role);
            let username = unquote(username);
            validate_ident(&role, false, "role name")?;
            validate_ident(&username, false, "username")?;

            return Ok(AuthCommand::RevokeRole {
                role,
                username,
            });
        }
    }

    // REVOKE privilege(s) ON target FROM grantee
    let on_idx = upper
        .find(" ON ")
        .ok_or_else(|| EngineError::InvalidArgument("REVOKE requires ON clause".into()))?;
    let from_idx = upper
        .find(" FROM ")
        .ok_or_else(|| EngineError::InvalidArgument("REVOKE requires FROM clause".into()))?;

    let privileges_str = &sql[7..on_idx];
    let privileges: Vec<String> = privileges_str
        .split(',')
        .map(|p| p.trim().to_uppercase())
        .collect();

    let target_str = &sql[on_idx + 4..from_idx];
    let (target_type, target_name) = parse_grant_target(target_str)?;

    let after_from = &sql[from_idx + 6..];
    let grantee = after_from.trim().trim_end_matches(';');
    let grantee_is_role = upper.contains("FROM ROLE ");
    let grantee = unquote(
        grantee
            .trim_start_matches("ROLE ")
            .trim_start_matches("role "),
    );
    validate_ident(&grantee, false, "grantee")?;

    Ok(AuthCommand::Revoke {
        privileges,
        target_type,
        target_name,
        grantee,
        grantee_is_role,
    })
}

/// Parse the target of a GRANT/REVOKE (the part after ON)
fn parse_grant_target(target_str: &str) -> Result<(GrantTargetType, Option<String>), EngineError> {
    let upper = target_str.trim().to_uppercase();

    if upper == "*" || upper == "ALL" {
        return Ok((GrantTargetType::Global, None));
    }

    if upper.starts_with("DATABASE ") {
        let name = target_str.trim()[9..].trim();
        let name = unquote(name);
        validate_ident(&name, false, "database")?;
        return Ok((GrantTargetType::Database, Some(name)));
    }

    if upper.starts_with("ALL TABLES IN DATABASE ") {
        let name = target_str.trim()[23..].trim();
        let name = unquote(name);
        validate_ident(&name, false, "database")?;
        return Ok((GrantTargetType::AllTablesInDatabase, Some(name)));
    }

    if upper.starts_with("TABLE ") {
        let name = target_str.trim()[6..].trim();
        let name = unquote(name);
        validate_ident(&name, true, "table")?;
        return Ok((GrantTargetType::Table, Some(name)));
    }

    // Default: treat as table name
    let name = unquote(target_str.trim());
    validate_ident(&name, true, "table")?;
    Ok((GrantTargetType::Table, Some(name)))
}

/// Extract a quoted string from the beginning of text
fn extract_quoted_string(text: &str) -> Option<String> {
    let text = text.trim();

    // Single quotes
    if let Some(stripped) = text.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        return Some(stripped[..end].to_string());
    }

    // Double quotes
    if let Some(stripped) = text.strip_prefix('"') {
        let end = stripped.find('"')?;
        return Some(stripped[..end].to_string());
    }
    None
}

/// Remove quotes from a string
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn validate_ident(name: &str, allow_dot: bool, context: &str) -> Result<(), EngineError> {
    if name.is_empty() {
        return Err(EngineError::InvalidArgument(format!(
            "{} cannot be empty",
            context
        )));
    }
    let parts: Vec<&str> = if allow_dot {
        name.split('.').collect()
    } else {
        vec![name]
    };
    for part in parts {
        let mut chars = part.chars();
        let first = chars.next().ok_or_else(|| {
            EngineError::InvalidArgument(format!("{} cannot be empty", context))
        })?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(EngineError::InvalidArgument(format!(
                "{} has invalid start character",
                context
            )));
        }
        if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(EngineError::InvalidArgument(format!(
                "{} has invalid characters",
                context
            )));
        }
    }
    Ok(())
}

/// Conflict action for UPSERT operations
#[derive(Debug, Clone)]
pub enum OnConflictAction {
    /// DO NOTHING - ignore conflicts
    DoNothing,
    /// DO UPDATE SET ... - update conflicting rows
    DoUpdate {
        /// Column assignments for update: (column_name, new_value_expr)
        assignments: Vec<(String, String)>,
        /// Optional WHERE clause for conditional update
        where_clause: Option<String>,
    },
}

/// ON CONFLICT clause for UPSERT
#[derive(Debug, Clone)]
pub struct OnConflict {
    /// Conflict target columns (optional)
    pub columns: Option<Vec<String>>,
    /// Action to take on conflict
    pub action: OnConflictAction,
}

/// Parsed INSERT command
#[derive(Debug, Clone)]
pub struct InsertCommand {
    pub database: String,
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<SqlValue>>,
    /// ON CONFLICT clause for UPSERT operations
    pub on_conflict: Option<OnConflict>,
}

/// A SQL value from INSERT/UPDATE statements
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

/// UPDATE command parsed from SQL
#[derive(Debug, Clone)]
pub struct UpdateCommand {
    pub database: String,
    pub table: String,
    /// Column assignments: (column_name, new_value)
    pub assignments: Vec<(String, SqlValue)>,
    /// WHERE clause filter (simplified for now)
    pub where_clause: Option<String>,
}

/// DELETE command parsed from SQL
#[derive(Debug, Clone)]
pub struct DeleteCommand {
    pub database: String,
    pub table: String,
    /// WHERE clause filter (simplified for now)
    pub where_clause: Option<String>,
}

/// Transaction control commands
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TransactionCommand {
    Start,
    Commit,
    Rollback,
}

/// Parsed SQL statement - either a query, DDL, Insert, Update, Delete, auth, or set operation
#[derive(Debug, Clone)]
pub enum SqlStatement {
    Query(ParsedQuery),
    SetOperation(SetOperationQuery),
    Ddl(DdlCommand),
    Insert(InsertCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Auth(AuthCommand),
    Transaction(TransactionCommand),
    /// EXPLAIN [ANALYZE] <statement>
    Explain {
        analyze: bool,
        statement: Box<SqlStatement>,
    },
}

/// Convert SQL data types to boyodb schema types
fn sql_type_to_boyodb_type(sql_type: &sqlparser::ast::DataType) -> String {
    use sqlparser::ast::DataType;
    match sql_type {
        DataType::Int(_) | DataType::Integer(_) | DataType::BigInt(_) |
        DataType::SmallInt(_) | DataType::TinyInt(_) => "int64".to_string(),
        DataType::Float(_) | DataType::Real | DataType::Double => "float64".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Custom(name, _) => {
            let n = name.to_string().to_lowercase();
            match n.as_str() {
                "int64" | "bigint" | "int" | "integer" => "int64".to_string(),
                "uint64" | "u64" => "uint64".to_string(),
                "float64" | "double" => "float64".to_string(),
                "string" | "utf8" | "text" => "string".to_string(),
                "bool" | "boolean" => "bool".to_string(),
                _ => "string".to_string(),
            }
        }
        DataType::Varchar(_) | DataType::Char(_) | DataType::Text |
        DataType::String(_) => "string".to_string(),
        DataType::Timestamp(_, _) | DataType::Datetime(_) => "timestamp".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time(_, _) => "time".to_string(),
        DataType::Binary(_) | DataType::Blob(_) | DataType::Bytes(_) => "binary".to_string(),
        other => other.to_string().to_lowercase(), // Default: bubble through string form
    }
}

fn parse_statement(stmt: &Statement) -> Result<SqlStatement, EngineError> {
    match stmt {
        Statement::Query(query) => {
            // Check if this is a set operation (UNION, INTERSECT, EXCEPT)
            if let SetExpr::SetOperation { op, set_quantifier, left, right } = query.body.as_ref() {
                let op_type = match (op, set_quantifier) {
                    (SetOperator::Union, SetQuantifier::All) => SetOpType::UnionAll,
                    (SetOperator::Union, _) => SetOpType::Union,
                    (SetOperator::Intersect, SetQuantifier::All) => SetOpType::IntersectAll,
                    (SetOperator::Intersect, _) => SetOpType::Intersect,
                    (SetOperator::Except, SetQuantifier::All) => SetOpType::ExceptAll,
                    (SetOperator::Except, _) => SetOpType::Except,
                };

                // Parse left and right sides
                let left_query = Query {
                    body: left.clone(),
                    with: None,
                    order_by: Vec::new(),
                    limit: None,
                    limit_by: Vec::new(),
                    offset: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                };
                let right_query = Query {
                    body: right.clone(),
                    with: None,
                    order_by: Vec::new(),
                    limit: None,
                    limit_by: Vec::new(),
                    offset: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                };

                let left_parsed = parse_query_or_setop(&left_query)?;
                let right_parsed = parse_query_or_setop(&right_query)?;

                Ok(SqlStatement::SetOperation(SetOperationQuery {
                    op: op_type,
                    left: Box::new(left_parsed),
                    right: Box::new(right_parsed),
                }))
            } else {
                let parsed = parse_query(query)?;
                Ok(SqlStatement::Query(parsed))
            }
        }
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => parse_insert(table_name, columns, source),
        Statement::StartTransaction { .. } => Ok(SqlStatement::Transaction(TransactionCommand::Start)),
        Statement::Commit { .. } => Ok(SqlStatement::Transaction(TransactionCommand::Commit)),
        Statement::Rollback { .. } => Ok(SqlStatement::Transaction(TransactionCommand::Rollback)),
        Statement::CreateDatabase { db_name, .. } => {
            Ok(SqlStatement::Ddl(DdlCommand::CreateDatabase {
                name: db_name.to_string(),
            }))
        }
        Statement::CreateTable { name, columns, .. } => {
            let full_name = name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            // Convert SQL column definitions to schema JSON
            let schema_json = if columns.is_empty() {
                None
            } else {
                let schema_fields: Vec<serde_json::Value> = columns.iter().map(|col| {
                    let data_type = sql_type_to_boyodb_type(&col.data_type);
                    let nullable = !col.options.iter().any(|opt| {
                        matches!(opt.option, sqlparser::ast::ColumnOption::NotNull)
                    });
                    serde_json::json!({
                        "name": col.name.value,
                        "type": data_type,
                        "nullable": nullable
                    })
                }).collect();
                Some(serde_json::to_string(&schema_fields).unwrap_or_default())
            };
            Ok(SqlStatement::Ddl(DdlCommand::CreateTable {
                database,
                table,
                schema_json,
            }))
        }
        Statement::CreateView {
            name,
            query,
            or_replace,
            ..
        } => {
            let full_name = name.to_string();
            let (database, view_name) = parse_table_name(&full_name)?;
            // Store the query SQL as a string
            let query_sql = query.to_string();
            Ok(SqlStatement::Ddl(DdlCommand::CreateView {
                database,
                name: view_name,
                query_sql,
                or_replace: *or_replace,
            }))
        }
        Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => {
            use sqlparser::ast::ObjectType;
            match object_type {
                ObjectType::Schema => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP DATABASE requires exactly one name".into(),
                        ));
                    }
                    let name = names[0].to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::DropDatabase {
                        name,
                        if_exists: *if_exists,
                    }))
                }
                ObjectType::Table => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP TABLE requires exactly one table name".into(),
                        ));
                    }
                    let full_name = names[0].to_string();
                    let (database, table) = parse_table_name(&full_name)?;
                    Ok(SqlStatement::Ddl(DdlCommand::DropTable {
                        database,
                        table,
                        if_exists: *if_exists,
                    }))
                }
                ObjectType::View => {
                    if names.len() != 1 {
                        return Err(EngineError::InvalidArgument(
                            "DROP VIEW requires exactly one view name".into(),
                        ));
                    }
                    let full_name = names[0].to_string();
                    let (database, name) = parse_table_name(&full_name)?;
                    Ok(SqlStatement::Ddl(DdlCommand::DropView {
                        database,
                        name,
                        if_exists: *if_exists,
                    }))
                }
                _ => Err(EngineError::NotImplemented(format!(
                    "DROP {object_type:?} not supported"
                ))),
            }
        }
        Statement::Truncate { table_name, .. } => {
            // Handle TRUNCATE TABLE database.table_name
            let full_name = table_name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            Ok(SqlStatement::Ddl(DdlCommand::TruncateTable {
                database,
                table,
            }))
        }
        Statement::AlterTable {
            name,
            operations,
            ..
        } => {
            let full_name = name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            use sqlparser::ast::AlterTableOperation;
            let Some(operation) = operations.first() else {
                return Err(EngineError::InvalidArgument(
                    "ALTER TABLE requires at least one operation".into(),
                ));
            };
            match operation {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.to_string();
                    let data_type = sql_type_to_boyodb_type(&column_def.data_type);
                    let nullable = !column_def
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableAddColumn {
                        database,
                        table,
                        column: col_name,
                        data_type,
                        nullable,
                    }))
                }
                AlterTableOperation::DropColumn { column_name, .. } => {
                    let col_name = column_name.to_string();
                    Ok(SqlStatement::Ddl(DdlCommand::AlterTableDropColumn {
                        database,
                        table,
                        column: col_name,
                    }))
                }
                _ => Err(EngineError::NotImplemented(
                    "ALTER TABLE operation not supported".into(),
                )),
            }
        }
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => parse_update(table, assignments, selection),
        Statement::Delete {
            from,
            selection,
            ..
        } => parse_delete(from, selection),
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            if_not_exists,
            using,
            ..
        } => {
            let full_table = table_name.to_string();
            let (database, table) = parse_table_name(&full_table)?;
            let index_name = name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| format!("idx_{}_{}", table, columns.len()));
            let column_names: Vec<String> = columns
                .iter()
                .map(|c| c.expr.to_string())
                .collect();
            let index_type = match using.as_ref().map(|i| i.to_string().to_uppercase()).as_deref() {
                Some("BTREE") | None => IndexType::BTree,
                Some("HASH") => IndexType::Hash,
                Some("BLOOM") => IndexType::Bloom,
                Some("BITMAP") => IndexType::Bitmap,
                Some(other) => return Err(EngineError::InvalidArgument(format!(
                    "unsupported index type: {other}"
                ))),
            };
            Ok(SqlStatement::Ddl(DdlCommand::CreateIndex {
                database,
                table,
                index_name,
                columns: column_names,
                index_type,
                if_not_exists: *if_not_exists,
            }))
        }
        Statement::Explain {
            analyze,
            statement,
            ..
        } => {
            // Parse EXPLAIN [ANALYZE] SELECT ...
            let explained = parse_statement(statement)?;
            Ok(SqlStatement::Explain {
                analyze: *analyze,
                statement: Box::new(explained),
            })
        }
        Statement::Analyze { table_name, .. } => {
            let full_name = table_name.to_string();
            let (database, table) = parse_table_name(&full_name)?;
            Ok(SqlStatement::Ddl(DdlCommand::AnalyzeTable { database, table }))
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported SQL statement type: {stmt}"
        ))),
    }
}

/// Parse a query that might be a simple SELECT or a set operation
fn parse_query_or_setop(query: &Query) -> Result<SqlStatement, EngineError> {
    if let SetExpr::SetOperation { op, set_quantifier, left, right } = query.body.as_ref() {
        let op_type = match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => SetOpType::UnionAll,
            (SetOperator::Union, _) => SetOpType::Union,
            (SetOperator::Intersect, SetQuantifier::All) => SetOpType::IntersectAll,
            (SetOperator::Intersect, _) => SetOpType::Intersect,
            (SetOperator::Except, SetQuantifier::All) => SetOpType::ExceptAll,
            (SetOperator::Except, _) => SetOpType::Except,
        };

        let left_query = Query {
            body: left.clone(),
            with: None,
            order_by: Vec::new(),
            limit: None,
            limit_by: Vec::new(),
            offset: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
        };
        let right_query = Query {
            body: right.clone(),
            with: None,
            order_by: Vec::new(),
            limit: None,
            limit_by: Vec::new(),
            offset: None,
            fetch: None,
            locks: Vec::new(),
            for_clause: None,
        };

        let left_parsed = parse_query_or_setop(&left_query)?;
        let right_parsed = parse_query_or_setop(&right_query)?;

        Ok(SqlStatement::SetOperation(SetOperationQuery {
            op: op_type,
            left: Box::new(left_parsed),
            right: Box::new(right_parsed),
        }))
    } else {
        parse_query(query).map(SqlStatement::Query)
    }
}

fn parse_query(query: &Query) -> Result<ParsedQuery, EngineError> {
    // Extract the SELECT body
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => {
            return Err(EngineError::NotImplemented(
                "only SELECT queries are supported".into(),
            ))
        }
    };

    // Parse table reference and JOINs
    let (database, table, joins) = parse_from_clause(select)?;

    // Parse projection and detect aggregations
    // Parse projection and detect aggregations
    let (projection, aggregation, computed_columns) = parse_select_items(&select.selection, &select.projection)?;

    // Parse WHERE clause
    let mut filter = QueryFilter::default();
    if let Some(where_expr) = &select.selection {
        parse_where_expr(where_expr, &mut filter)?;
    }

    // Parse LIMIT
    if let Some(Expr::Value(Value::Number(n, _))) = &query.limit {
        filter.limit = n.parse().ok();
    }

    // Parse OFFSET
    if let Some(offset_expr) = &query.offset {
        if let Expr::Value(Value::Number(n, _)) = &offset_expr.value {
            filter.offset = n.parse().ok();
        }
    }

    // Parse ORDER BY
    let order_by = parse_order_by(&query.order_by)?;

    // Parse GROUP BY
    let group_by = parse_group_by(&select.group_by)?;

    // Parse HAVING clause
    let having_conditions = if let Some(having_expr) = &select.having {
        parse_having_clause(having_expr)?
    } else {
        Vec::new()
    };

    // If we have aggregation, set the group by and having
    let aggregation = aggregation.map(|mut agg| {
        agg.group_by = group_by;
        agg.having = having_conditions;
        agg
    });

    // Parse DISTINCT
    let distinct = match &select.distinct {
        Some(Distinct::Distinct) => true,
        Some(Distinct::On(_)) => {
            return Err(EngineError::NotImplemented(
                "DISTINCT ON not supported".into(),
            ));
        }
        None => false,
    };

    // Parse CTEs (WITH clause)
    let ctes = if let Some(with) = &query.with {
        parse_ctes(with)?
    } else {
        Vec::new()
    };

    Ok(ParsedQuery {
        database,
        table,
        projection,
        filter,
        aggregation,
        order_by,
        distinct,
        joins,
        computed_columns,
        ctes,
        sample: None,
    })
}

fn parse_order_by(order_by: &[OrderByExpr]) -> Result<Option<Vec<OrderByClause>>, EngineError> {
    if order_by.is_empty() {
        return Ok(None);
    }

    let mut clauses = Vec::new();
    for expr in order_by {
        let column = match &expr.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => {
                return Err(EngineError::NotImplemented(
                    "only simple column references supported in ORDER BY".into(),
                ))
            }
        };
        clauses.push(OrderByClause {
            column,
            ascending: expr.asc.unwrap_or(true),
            nulls_first: expr.nulls_first,
        });
    }
    Ok(Some(clauses))
}

fn parse_table_name(full_name: &str) -> Result<(String, String), EngineError> {
    let cleaned = full_name.trim();
    if let Some((db, tbl)) = cleaned.split_once('.') {
        let db = db.trim().trim_matches('"').trim_matches('`');
        let tbl = tbl.trim().trim_matches('"').trim_matches('`');
        if db.is_empty() {
            return Ok(("default".to_string(), tbl.to_string()));
        }
        Ok((db.to_string(), tbl.to_string()))
    } else {
        let tbl = cleaned.trim_matches('"').trim_matches('`');
        Ok(("default".to_string(), tbl.to_string()))
    }
}

fn parse_from_clause(select: &Select) -> Result<(Option<String>, Option<String>, Vec<JoinClause>), EngineError> {
    if select.from.is_empty() {
        return Ok((None, None, Vec::new()));
    }

    let table_with_joins = &select.from[0];
    let (db, table) = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => {
            let full_name = name.to_string();
            parse_table_name(&full_name)?
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "only simple table references supported".into(),
            ))
        }
    };

    // Parse JOIN clauses
    let joins = parse_joins(&table_with_joins.joins)?;

    Ok((Some(db), Some(table), joins))
}

fn parse_joins(joins: &[Join]) -> Result<Vec<JoinClause>, EngineError> {
    let mut result = Vec::new();

    for join in joins {
        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::FullOuter,
            JoinOperator::CrossJoin => JoinType::Cross,
            _ => {
                return Err(EngineError::NotImplemented(
                    "unsupported JOIN type".into(),
                ))
            }
        };

        // Extract the joined table name and alias
        let (join_db, join_table, alias) = match &join.relation {
            TableFactor::Table { name, alias, .. } => {
                let full_name = name.to_string();
                let (db, tbl) = parse_table_name(&full_name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                (db, tbl, alias_name)
            }
            _ => {
                return Err(EngineError::NotImplemented(
                    "only simple table references in JOINs are supported".into(),
                ))
            }
        };

        // Extract the ON condition (CROSS JOIN has no ON condition)
        let on_condition = match &join.join_operator {
            JoinOperator::CrossJoin => {
                // Cross join has no ON condition - use empty placeholder
                JoinCondition {
                    left_column: String::new(),
                    right_column: String::new(),
                }
            }
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint) => match constraint {
                JoinConstraint::On(expr) => parse_join_condition(expr)?,
                _ => {
                    return Err(EngineError::NotImplemented(
                        "only ON conditions are supported in JOINs".into(),
                    ))
                }
            },
            _ => {
                return Err(EngineError::NotImplemented(
                    "unsupported join operator".into(),
                ))
            }
        };

        result.push(JoinClause {
            join_type,
            table: join_table,
            database: join_db,
            alias,
            on_condition,
        });
    }

    Ok(result)
}

fn parse_join_condition(expr: &Expr) -> Result<JoinCondition, EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op != BinaryOperator::Eq {
                return Err(EngineError::NotImplemented(
                    "only equality conditions (=) are supported in JOINs".into(),
                ));
            }

            let left_col = extract_column_name(left)?;
            let right_col = extract_column_name(right)?;

            Ok(JoinCondition {
                left_column: left_col,
                right_column: right_col,
            })
        }
        _ => Err(EngineError::NotImplemented(
            "only simple equality conditions are supported in JOINs".into(),
        )),
    }
}

fn extract_column_name(expr: &Expr) -> Result<String, EngineError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // Handle table.column format - return just the column part
            if let Some(last) = parts.last() {
                Ok(last.value.clone())
            } else {
                Err(EngineError::InvalidArgument("empty compound identifier".into()))
            }
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression in JOIN condition: {expr}"
        ))),
    }
}

fn parse_select_items(
    _where_clause: &Option<Expr>,
    items: &[SelectItem],
) -> Result<(Option<Vec<String>>, Option<AggPlan>, Vec<SelectColumn>), EngineError> {
    let mut columns = Vec::new();
    let mut aggs = Vec::new();
    let mut computed_columns = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                return Ok((None, None, Vec::new())); // SELECT * - no projection, no aggregation
            }
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    columns.push(ident.value.clone());
                    computed_columns.push(SelectColumn {
                        expr: SelectExpr::Column(ident.value.clone()),
                        alias: None,
                    });
                }
                Expr::Value(val) => {
                     let se = match val {
                         Value::Number(n, _) => SelectExpr::Literal(LiteralValue::Integer(n.parse().unwrap_or(0))),
                         Value::SingleQuotedString(s) => SelectExpr::Literal(LiteralValue::String(s.clone())),
                         _ => return Err(EngineError::NotImplemented(format!("unsupported value type: {val}"))),
                     };
                     computed_columns.push(SelectColumn { expr: se, alias: None });
                }
                Expr::Function(func) => {
                    if let Some(agg) = parse_aggregate_function(func)? {
                        aggs.push(agg);
                    } else {
                        return Err(EngineError::NotImplemented(format!(
                            "unsupported function: {}", func.name
                        )));
                    }
                }
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported expression in SELECT: {expr}"
                    )));
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                match expr {
                    Expr::Identifier(ident) => {
                        columns.push(ident.value.clone());
                        computed_columns.push(SelectColumn {
                            expr: SelectExpr::Column(ident.value.clone()),
                            alias: Some(alias.value.clone()),
                        });
                    }
                    Expr::Value(val) => {
                         let se = match val {
                             Value::Number(n, _) => SelectExpr::Literal(LiteralValue::Integer(n.parse().unwrap_or(0))),
                             Value::SingleQuotedString(s) => SelectExpr::Literal(LiteralValue::String(s.clone())),
                             _ => return Err(EngineError::NotImplemented(format!("unsupported value type: {val}"))),
                         };
                         computed_columns.push(SelectColumn { expr: se, alias: Some(alias.value.clone()) });
                    }
                    Expr::Function(func) => {
                        if let Some(agg) = parse_aggregate_function(func)? {
                            aggs.push(agg);
                        } else {
                            return Err(EngineError::NotImplemented(format!(
                                "unsupported function: {}", func.name
                            )));
                        }
                    }
                    _ => {
                        return Err(EngineError::NotImplemented(format!(
                            "unsupported expression in SELECT: {expr}"
                        )));
                    }
                }
            }
            _ => {
                return Err(EngineError::NotImplemented(format!(
                    "unsupported SELECT item: {item}"
                )));
            }
        }
    }

    if !aggs.is_empty() {
        // If we have columns but also aggs, they must be part of GROUP BY or valid
        // For now we return both
        Ok((
            if columns.is_empty() { None } else { Some(columns) },
            Some(AggPlan {
                group_by: GroupBy::None,
                aggs,
                having: Vec::new(),
            }),
            computed_columns,
        ))
    } else {
        Ok((
            if columns.is_empty() { None } else { Some(columns) },
            None,
            computed_columns,
        ))
    }
}

/// Parse an aggregate function from SQL AST
fn parse_aggregate_function(func: &sqlparser::ast::Function) -> Result<Option<AggKind>, EngineError> {
    let func_name = func.name.to_string().to_lowercase();

    match func_name.as_str() {
        "count" => {
            // Check for COUNT(DISTINCT column)
            if func.distinct {
                let col = extract_function_column(func)?;
                Ok(Some(AggKind::CountDistinct { column: col }))
            } else {
                // Check if it's COUNT(*) or COUNT(column)
                if func.args.is_empty() {
                    Ok(Some(AggKind::CountStar))
                } else if let Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) = func.args.first() {
                    Ok(Some(AggKind::CountStar))
                } else {
                    // COUNT(column) - treat as COUNT(*)
                    Ok(Some(AggKind::CountStar))
                }
            }
        }
        "approx_count_distinct" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::ApproxCountDistinct { column: col }))
        }
        "sum" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::Sum { column: col }))
        }
        "avg" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::Avg { column: col }))
        }
        "min" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::Min { column: col }))
        }
        "max" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::Max { column: col }))
        }
        "stddev" | "stddev_samp" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::StddevSamp { column: col }))
        }
        "stddev_pop" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::StddevPop { column: col }))
        }
        "variance" | "var_samp" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::VarianceSamp { column: col }))
        }
        "var_pop" => {
            let col = extract_function_column(func)?;
            Ok(Some(AggKind::VariancePop { column: col }))
        }
        _ => Ok(None),
    }
}

fn extract_function_column(func: &sqlparser::ast::Function) -> Result<String, EngineError> {
    // In sqlparser 0.40, args is Vec<FunctionArg>
    if func.args.is_empty() {
        return Err(EngineError::InvalidArgument(
            "function requires column argument".into(),
        ));
    }

    match &func.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Ok(ident.value.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok("*".to_string()),
        _ => Err(EngineError::NotImplemented(
            "only simple column references supported in aggregate functions".into(),
        )),
    }
}

fn parse_where_expr(expr: &Expr, filter: &mut QueryFilter) -> Result<(), EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    parse_where_expr(left, filter)?;
                    parse_where_expr(right, filter)?;
                }
                BinaryOperator::Or => {
                    // Handle OR by collecting equality conditions into IN filters
                    // For example: tenant_id = 1 OR tenant_id = 3 becomes tenant_id IN (1, 3)
                    let mut or_values: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
                    collect_or_equalities(expr, &mut or_values);

                    // Convert collected OR equalities into IN filters
                    for (col_name, values) in or_values {
                        let col_lower = col_name.to_lowercase();
                        if col_lower == "tenant_id" {
                            match &mut filter.tenant_id_in {
                                Some(existing) => existing.extend(values),
                                None => filter.tenant_id_in = Some(values),
                            }
                        } else if col_lower == "route_id" {
                            match &mut filter.route_id_in {
                                Some(existing) => existing.extend(values),
                                None => filter.route_id_in = Some(values),
                            }
                        } else {
                            // For other columns, add to numeric_in_filters
                            let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
                            filter.numeric_in_filters.push((col_name, i64_values));
                        }
                    }
                }
                BinaryOperator::Eq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            let col_lower = col_name.to_lowercase();
                            // Handle special columns for backward compatibility
                            if col_lower == "tenant_id" || col_lower == "route_id" {
                                if let Ok(value) = extract_u64_value(right) {
                                    match col_lower.as_str() {
                                        "tenant_id" => filter.tenant_id_eq = Some(value),
                                        "route_id" => filter.route_id_eq = Some(value),
                                        _ => {}
                                    }
                                }
                            }
                            // Try numeric value for segment pruning (all columns)
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Eq,
                                    value: num_val,
                                });
                                // Also handle legacy special columns
                                if let Ok(value) = extract_u64_value(right) {
                                    match col_lower.as_str() {
                                        "watermark" | "watermark_micros" => {
                                            filter.watermark_ge = Some(value);
                                            filter.watermark_le = Some(value);
                                        }
                                        "event_time" => {
                                            filter.event_time_ge = Some(value);
                                            filter.event_time_le = Some(value);
                                        }
                                        _ => {}
                                    }
                                }
                            } else if let Some(string_val) = extract_string_value(right) {
                                // String equality for other columns
                                filter.string_eq_filters.push((col_name.clone(), string_val));
                            }
                        }
                    }
                }
                BinaryOperator::GtEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                ">=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Ge,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_ge = Some(value)
                                    }
                                    "event_time" => filter.event_time_ge = Some(value),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::LtEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "<=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Le,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_le = Some(value)
                                    }
                                    "event_time" => filter.event_time_le = Some(value),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::Gt => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                ">".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Gt,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_ge = Some(value.saturating_add(1))
                                    }
                                    "event_time" => {
                                        filter.event_time_ge = Some(value.saturating_add(1))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::Lt => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Check for scalar subquery first
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "<".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Lt,
                                    value: num_val,
                                });
                            }
                            // Legacy special column handling
                            if let Ok(value) = extract_u64_value(right) {
                                match col_name.to_lowercase().as_str() {
                                    "watermark" | "watermark_micros" => {
                                        filter.watermark_le = Some(value.saturating_sub(1))
                                    }
                                    "event_time" => {
                                        filter.event_time_le = Some(value.saturating_sub(1))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                BinaryOperator::NotEq => {
                    if let Some(col_name) = try_extract_column_name(left) {
                        // Handle scalar subquery comparison
                        if let Expr::Subquery(subquery) = right.as_ref() {
                            let subquery_sql = subquery.to_string();
                            filter.scalar_subquery_filters.push((
                                col_name.clone(),
                                "!=".to_string(),
                                subquery_sql,
                            ));
                        } else {
                            // Add to numeric_range_filters for segment pruning
                            if let Some(num_val) = try_extract_numeric_value(right) {
                                filter.numeric_range_filters.push(NumericFilter {
                                    column: col_name.clone(),
                                    op: NumericOp::Ne,
                                    value: num_val,
                                });
                            }
                        }
                    }
                }
                _ => {} // Ignore other operators for now
            }
        }
        // LIKE expression: column LIKE 'pattern'
        Expr::Like {
            negated,
            expr: like_expr,
            pattern,
            ..
        } => {
            if let Expr::Identifier(ident) = like_expr.as_ref() {
                if let Some(pattern_str) = extract_string_value(pattern) {
                    filter
                        .like_filters
                        .push((ident.value.clone(), pattern_str, *negated));
                }
            }
        }
        // IN expression: column IN ('a', 'b', 'c') or column IN (1, 2, 3)
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => {
            if !negated {
                if let Expr::Identifier(ident) = in_expr.as_ref() {
                    let col_name = ident.value.to_lowercase();

                    // Try to extract as numeric list first
                    let numeric_values: Vec<i64> = list
                        .iter()
                        .filter_map(|e| {
                            match e {
                                Expr::Value(Value::Number(n, _)) => n.parse::<i64>().ok(),
                                Expr::UnaryOp { op: UnaryOperator::Minus, expr } => {
                                    if let Expr::Value(Value::Number(n, _)) = expr.as_ref() {
                                        n.parse::<i64>().ok().map(|v| -v)
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        })
                        .collect();

                    if numeric_values.len() == list.len() && !numeric_values.is_empty() {
                        // Handle special columns tenant_id and route_id
                        if col_name == "tenant_id" {
                            filter.tenant_id_in = Some(numeric_values.iter().map(|&v| v as u64).collect());
                        } else if col_name == "route_id" {
                            filter.route_id_in = Some(numeric_values.iter().map(|&v| v as u64).collect());
                        } else {
                            filter.numeric_in_filters.push((ident.value.clone(), numeric_values));
                        }
                    } else {
                        // Try to extract as string list
                        let string_values: Vec<String> = list
                            .iter()
                            .filter_map(|e| extract_string_value(e))
                            .collect();
                        if string_values.len() == list.len() && !string_values.is_empty() {
                            filter
                                .string_in_filters
                                .push((ident.value.clone(), string_values));
                        }
                    }
                }
            }
        }
        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            if let Expr::Identifier(ident) = inner.as_ref() {
                filter.null_filters.push((ident.value.clone(), true));
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Identifier(ident) = inner.as_ref() {
                filter.null_filters.push((ident.value.clone(), false));
            }
        }
        // BETWEEN expression: column BETWEEN low AND high
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => {
            if !negated {
                if let Some(col_name) = try_extract_column_name(between_expr) {
                    // Add >= low filter
                    if let Some(low_val) = try_extract_numeric_value(low) {
                        filter.numeric_range_filters.push(NumericFilter {
                            column: col_name.clone(),
                            op: NumericOp::Ge,
                            value: low_val,
                        });
                    }
                    // Add <= high filter
                    if let Some(high_val) = try_extract_numeric_value(high) {
                        filter.numeric_range_filters.push(NumericFilter {
                            column: col_name.clone(),
                            op: NumericOp::Le,
                            value: high_val,
                        });
                    }
                    // Legacy handling for special columns
                    if let (Ok(low_u64), Ok(high_u64)) = (extract_u64_value(low), extract_u64_value(high)) {
                        match col_name.to_lowercase().as_str() {
                            "event_time" => {
                                filter.event_time_ge = Some(low_u64);
                                filter.event_time_le = Some(high_u64);
                            }
                            "watermark" | "watermark_micros" => {
                                filter.watermark_ge = Some(low_u64);
                                filter.watermark_le = Some(high_u64);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Expr::Nested(inner) => {
            parse_where_expr(inner, filter)?;
        }
        // IN subquery: column IN (SELECT ...)
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => {
            if let Expr::Identifier(ident) = in_expr.as_ref() {
                // Convert the subquery back to SQL string for execution
                let subquery_sql = subquery.to_string();
                filter
                    .in_subquery_filters
                    .push((ident.value.clone(), subquery_sql, *negated));
            }
        }
        // EXISTS (SELECT ...)
        Expr::Exists { subquery, negated } => {
            let subquery_sql = subquery.to_string();
            filter.exists_subqueries.push((subquery_sql, *negated));
        }
        // Scalar subquery comparison: column = (SELECT ...), column > (SELECT ...), etc.
        Expr::Subquery(subquery) => {
            // Standalone subquery - handled when part of a comparison
            let _ = subquery; // Suppress unused warning
        }
        _ => {} // Ignore other expression types
    }
    Ok(())
}

fn extract_u64_value(expr: &Expr) -> Result<u64, EngineError> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {n}"))),
        _ => Err(EngineError::InvalidArgument(
            "expected numeric value".into(),
        )),
    }
}

/// Extract a numeric value from an expression for use in segment pruning.
/// Tries to parse as i64 first (most common), then f64 for floats.
fn try_extract_numeric_value(expr: &Expr) -> Option<NumericValue> {
    match expr {
        Expr::Value(Value::Number(n, _)) => {
            // Try parsing as i64 first (handles both positive and negative integers)
            if let Ok(v) = n.parse::<i64>() {
                return Some(NumericValue::Int64(v));
            }
            // Try parsing as f64 for floating point numbers
            if let Ok(v) = n.parse::<f64>() {
                return Some(NumericValue::Float64(v));
            }
            None
        }
        Expr::UnaryOp { op, expr: inner } => {
            // Handle negative numbers: -123
            if let sqlparser::ast::UnaryOperator::Minus = op {
                if let Expr::Value(Value::Number(n, _)) = inner.as_ref() {
                    if let Ok(v) = n.parse::<i64>() {
                        return Some(NumericValue::Int64(-v));
                    }
                    if let Ok(v) = n.parse::<f64>() {
                        return Some(NumericValue::Float64(-v));
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract column name from an expression (handles both simple identifiers and compound identifiers)
/// Returns Option instead of Result for use in pattern matching
fn try_extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // For table.column, return just the column name
            parts.last().map(|p| p.value.clone())
        }
        _ => None,
    }
}

/// Collect all equality conditions from an OR expression tree.
/// For example: `a = 1 OR a = 2 OR b = 3` collects {a: [1, 2], b: [3]}
fn collect_or_equalities(expr: &Expr, values: &mut std::collections::HashMap<String, Vec<u64>>) {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
            // Recursively collect from both sides of OR
            collect_or_equalities(left, values);
            collect_or_equalities(right, values);
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            // Extract column = value equality
            if let Some(col_name) = try_extract_column_name(left) {
                if let Some(val) = try_extract_u64_value(right) {
                    values.entry(col_name).or_insert_with(Vec::new).push(val);
                }
            } else if let Some(col_name) = try_extract_column_name(right) {
                // Handle reversed: value = column
                if let Some(val) = try_extract_u64_value(left) {
                    values.entry(col_name).or_insert_with(Vec::new).push(val);
                }
            }
        }
        Expr::Nested(inner) => {
            // Handle parenthesized expressions
            collect_or_equalities(inner, values);
        }
        _ => {}
    }
}

/// Try to extract a u64 value from an expression
fn try_extract_u64_value(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        Expr::UnaryOp { op: UnaryOperator::Plus, expr } => try_extract_u64_value(expr),
        _ => None,
    }
}

fn extract_string_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}

fn parse_group_by_column(expr: &Expr) -> Result<GroupByColumn, EngineError> {
    match expr {
        Expr::Identifier(ident) => match ident.value.to_lowercase().as_str() {
            "tenant_id" => Ok(GroupByColumn::TenantId),
            "route_id" => Ok(GroupByColumn::RouteId),
            _ => Err(EngineError::InvalidArgument(format!(
                "unsupported GROUP BY column: {}",
                ident.value
            ))),
        },
        _ => Err(EngineError::NotImplemented(
            "only simple column references supported in GROUP BY".into(),
        )),
    }
}

fn parse_group_by(group_by: &GroupByExpr) -> Result<GroupBy, EngineError> {
    match group_by {
        GroupByExpr::Expressions(exprs) => {
            if exprs.is_empty() {
                return Ok(GroupBy::None);
            }
            if exprs.len() == 1 {
                // Single column - use legacy types for backwards compatibility
                match &exprs[0] {
                    Expr::Identifier(ident) => match ident.value.to_lowercase().as_str() {
                        "tenant_id" => Ok(GroupBy::Tenant),
                        "route_id" => Ok(GroupBy::Route),
                        _ => Err(EngineError::InvalidArgument(format!(
                            "unsupported GROUP BY column: {}",
                            ident.value
                        ))),
                    },
                    _ => Err(EngineError::NotImplemented(
                        "only simple column references supported in GROUP BY".into(),
                    )),
                }
            } else {
                // Multiple columns - parse each and return Columns variant
                let mut columns = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    columns.push(parse_group_by_column(expr)?);
                }
                // Check for duplicates
                let mut seen = std::collections::HashSet::new();
                for col in &columns {
                    if !seen.insert(col) {
                        return Err(EngineError::InvalidArgument(
                            "duplicate column in GROUP BY".into(),
                        ));
                    }
                }
                Ok(GroupBy::Columns(columns))
            }
        }
        GroupByExpr::All => Err(EngineError::NotImplemented(
            "GROUP BY ALL not supported".into(),
        )),
    }
}

/// Parse HAVING clause expression
fn parse_having_clause(expr: &Expr) -> Result<Vec<HavingCondition>, EngineError> {
    let mut conditions = Vec::new();
    parse_having_expr(expr, &mut conditions)?;
    Ok(conditions)
}

/// Recursively parse HAVING expression (handles AND/OR)
fn parse_having_expr(expr: &Expr, conditions: &mut Vec<HavingCondition>) -> Result<(), EngineError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    // Parse both sides of AND
                    parse_having_expr(left, conditions)?;
                    parse_having_expr(right, conditions)?;
                }
                BinaryOperator::Or => {
                    // OR is more complex - for now, just parse both sides
                    // (this is a simplification; full OR support would need expression trees)
                    parse_having_expr(left, conditions)?;
                    parse_having_expr(right, conditions)?;
                }
                // Comparison operators
                BinaryOperator::Eq | BinaryOperator::NotEq |
                BinaryOperator::Gt | BinaryOperator::GtEq |
                BinaryOperator::Lt | BinaryOperator::LtEq => {
                    // Left side should be an aggregate function
                    if let Expr::Function(func) = left.as_ref() {
                        if let Some(agg) = parse_aggregate_function(func)? {
                            let having_op = match op {
                                BinaryOperator::Eq => HavingOp::Eq,
                                BinaryOperator::NotEq => HavingOp::Ne,
                                BinaryOperator::Gt => HavingOp::Gt,
                                BinaryOperator::GtEq => HavingOp::Ge,
                                BinaryOperator::Lt => HavingOp::Lt,
                                BinaryOperator::LtEq => HavingOp::Le,
                                _ => unreachable!(),
                            };
                            let value = extract_numeric_value(right)?;
                            conditions.push(HavingCondition {
                                agg,
                                op: having_op,
                                value,
                            });
                        } else {
                            return Err(EngineError::InvalidArgument(
                                "HAVING clause requires aggregate function".into(),
                            ));
                        }
                    } else {
                        return Err(EngineError::InvalidArgument(
                            "HAVING clause requires aggregate function on left side".into(),
                        ));
                    }
                }
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported operator in HAVING: {:?}", op
                    )));
                }
            }
        }
        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported expression in HAVING: {}", expr
            )));
        }
    }
    Ok(())
}

/// Extract a numeric value from an expression
fn extract_numeric_value(expr: &Expr) -> Result<f64, EngineError> {
    match expr {
        Expr::Value(Value::Number(n, _)) => {
            n.parse::<f64>().map_err(|_| {
                EngineError::InvalidArgument(format!("invalid number in HAVING: {}", n))
            })
        }
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr } => {
            let val = extract_numeric_value(expr)?;
            Ok(-val)
        }
        _ => Err(EngineError::InvalidArgument(
            "HAVING clause requires numeric literal for comparison".into(),
        )),
    }
}

/// Parse INSERT statement
fn parse_insert(
    table_name: &ObjectName,
    columns: &[Ident],
    source: &Option<Box<Query>>,
) -> Result<SqlStatement, EngineError> {
    let full_name = table_name.to_string();
    let (database, table) = parse_table_name(&full_name)?;

    // Extract column names if specified
    let cols = if columns.is_empty() {
        None
    } else {
        Some(columns.iter().map(|c| c.value.clone()).collect())
    };

    // Parse VALUES clause
    let source = source.as_ref().ok_or_else(|| {
        EngineError::InvalidArgument("INSERT requires VALUES clause".into())
    })?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => {
            let mut all_rows = Vec::new();
            for row in &values.rows {
                let mut row_values = Vec::new();
                for expr in row {
                    row_values.push(expr_to_sql_value(expr)?);
                }
                all_rows.push(row_values);
            }
            all_rows
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "INSERT only supports VALUES clause, not SELECT".into(),
            ))
        }
    };

    if values.is_empty() {
        return Err(EngineError::InvalidArgument(
            "INSERT requires at least one row".into(),
        ));
    }

    Ok(SqlStatement::Insert(InsertCommand {
        database,
        table,
        columns: cols,
        values,
        on_conflict: None,
    }))
}

/// Convert an Expr to SqlValue
fn expr_to_sql_value(expr: &Expr) -> Result<SqlValue, EngineError> {
    match expr {
        Expr::Value(v) => match v {
            Value::Null => Ok(SqlValue::Null),
            Value::Number(n, _) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(SqlValue::Integer(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(SqlValue::Float(f))
                } else {
                    Err(EngineError::InvalidArgument(format!(
                        "invalid number: {n}"
                    )))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(SqlValue::String(s.clone()))
            }
            Value::Boolean(b) => Ok(SqlValue::Boolean(*b)),
            _ => Err(EngineError::NotImplemented(format!(
                "unsupported value type: {v}"
            ))),
        },
        Expr::UnaryOp { op, expr } => {
            use sqlparser::ast::UnaryOperator;
            match op {
                UnaryOperator::Minus => {
                    let inner = expr_to_sql_value(expr)?;
                    match inner {
                        SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                        SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                        _ => Err(EngineError::InvalidArgument(
                            "unary minus only applies to numbers".into(),
                        )),
                    }
                }
                UnaryOperator::Plus => expr_to_sql_value(expr),
                _ => Err(EngineError::NotImplemented(format!(
                    "unsupported unary operator: {op}"
                ))),
            }
        }
        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression in INSERT: {expr}"
        ))),
    }
}

/// Parse UPDATE statement
fn parse_update(
    table: &sqlparser::ast::TableWithJoins,
    assignments: &[sqlparser::ast::Assignment],
    selection: &Option<Expr>,
) -> Result<SqlStatement, EngineError> {
    // Extract table name from the UPDATE clause
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(EngineError::NotImplemented(
                "UPDATE only supports simple table references".into(),
            ))
        }
    };
    let (database, table) = parse_table_name(&table_name)?;

    // Parse SET assignments
    let mut parsed_assignments = Vec::new();
    for assignment in assignments {
        // Get column name - handle both single and compound identifiers
        let column_name = if assignment.id.len() == 1 {
            assignment.id[0].value.clone()
        } else {
            // For compound identifiers like table.column, take the last part
            assignment
                .id
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| {
                    EngineError::InvalidArgument("empty column identifier in SET".into())
                })?
        };
        let value = expr_to_sql_value(&assignment.value)?;
        parsed_assignments.push((column_name, value));
    }

    // Convert WHERE clause to string representation (simplified for now)
    let where_clause = selection.as_ref().map(|expr| expr.to_string());

    Ok(SqlStatement::Update(UpdateCommand {
        database,
        table,
        assignments: parsed_assignments,
        where_clause,
    }))
}

/// Parse DELETE statement
fn parse_delete(
    from: &[sqlparser::ast::TableWithJoins],
    selection: &Option<Expr>,
) -> Result<SqlStatement, EngineError> {
    // Extract table name from the FROM clause
    let from_table = from.first().ok_or_else(|| {
        EngineError::InvalidArgument("DELETE requires FROM clause".into())
    })?;

    let table_name = match &from_table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(EngineError::NotImplemented(
                "DELETE only supports simple table references".into(),
            ))
        }
    };
    let (database, table) = parse_table_name(&table_name)?;

    // Convert WHERE clause to string representation (simplified for now)
    let where_clause = selection.as_ref().map(|expr| expr.to_string());

    Ok(SqlStatement::Delete(DeleteCommand {
        database,
        table,
        where_clause,
    }))
}

// ============================================================================
// Expression Parsing for Scalar Functions, Window Functions, and CTEs
// ============================================================================

/// Parse a sqlparser Expr into our SelectExpr representation
pub fn parse_expr(expr: &Expr) -> Result<SelectExpr, EngineError> {
    match expr {
        Expr::Identifier(ident) => Ok(SelectExpr::Column(ident.value.clone())),

        Expr::CompoundIdentifier(parts) => {
            if parts.len() == 2 {
                Ok(SelectExpr::QualifiedColumn {
                    table: parts[0].value.clone(),
                    column: parts[1].value.clone(),
                })
            } else if parts.len() == 1 {
                Ok(SelectExpr::Column(parts[0].value.clone()))
            } else {
                Err(EngineError::NotImplemented(
                    "compound identifiers with more than 2 parts not supported".into(),
                ))
            }
        }

        Expr::Value(val) => match val {
            Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(SelectExpr::Literal(LiteralValue::Float(
                        n.parse().map_err(|_| {
                            EngineError::InvalidArgument(format!("invalid float: {n}"))
                        })?,
                    )))
                } else {
                    Ok(SelectExpr::Literal(LiteralValue::Integer(
                        n.parse().map_err(|_| {
                            EngineError::InvalidArgument(format!("invalid integer: {n}"))
                        })?,
                    )))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(SelectExpr::Literal(LiteralValue::String(s.clone())))
            }
            Value::Boolean(b) => Ok(SelectExpr::Literal(LiteralValue::Boolean(*b))),
            Value::Null => Ok(SelectExpr::Null),
            _ => Err(EngineError::NotImplemented(format!(
                "unsupported literal value: {val}"
            ))),
        },

        Expr::Function(func) => parse_function_expr(func),

        Expr::BinaryOp { left, op, right } => {
            let left_expr = parse_expr(left)?;
            let right_expr = parse_expr(right)?;
            let op_str = match op {
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
                BinaryOperator::StringConcat => "||",
                BinaryOperator::Gt => ">",
                BinaryOperator::Lt => "<",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported binary operator: {op}"
                    )))
                }
            };
            Ok(SelectExpr::BinaryOp {
                left: Box::new(left_expr),
                op: op_str.to_string(),
                right: Box::new(right_expr),
            })
        }

        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = parse_expr(inner)?;
            let op_str = match op {
                sqlparser::ast::UnaryOperator::Minus => "-",
                sqlparser::ast::UnaryOperator::Plus => "+",
                sqlparser::ast::UnaryOperator::Not => "NOT",
                _ => {
                    return Err(EngineError::NotImplemented(format!(
                        "unsupported unary operator: {op}"
                    )))
                }
            };
            Ok(SelectExpr::UnaryOp {
                op: op_str.to_string(),
                expr: Box::new(inner_expr),
            })
        }

        Expr::Nested(inner) => parse_expr(inner),

        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let operand_expr = operand.as_ref().map(|e| parse_expr(e)).transpose()?;
            let mut when_clauses = Vec::new();
            for (cond, res) in conditions.iter().zip(results.iter()) {
                when_clauses.push((parse_expr(cond)?, parse_expr(res)?));
            }
            let else_expr = else_result.as_ref().map(|e| parse_expr(e)).transpose()?;
            Ok(SelectExpr::Case {
                operand: operand_expr.map(Box::new),
                when_clauses,
                else_result: else_expr.map(Box::new),
            })
        }

        Expr::Cast { expr: inner, data_type, .. } => {
            let inner_expr = parse_expr(inner)?;
            Ok(SelectExpr::Function(ScalarFunction::Cast {
                expr: Box::new(inner_expr),
                target_type: data_type.to_string(),
            }))
        }

        _ => Err(EngineError::NotImplemented(format!(
            "unsupported expression: {expr}"
        ))),
    }
}

/// Parse a function call into SelectExpr
fn parse_function_expr(func: &sqlparser::ast::Function) -> Result<SelectExpr, EngineError> {
    let func_name = func.name.to_string().to_lowercase();
    let args: Vec<SelectExpr> = func
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(parse_expr(e)),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                Some(Ok(SelectExpr::Column("*".to_string())))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Check for window function (OVER clause)
    if func.over.is_some() {
        return parse_window_function(&func_name, &args, func);
    }

    // Aggregate functions
    match func_name.as_str() {
        "count" => {
            return Ok(SelectExpr::Aggregate(AggKind::CountStar));
        }
        "sum" => {
            let col = get_single_column_arg(&args, "SUM")?;
            return Ok(SelectExpr::Aggregate(AggKind::Sum { column: col }));
        }
        "avg" => {
            let col = get_single_column_arg(&args, "AVG")?;
            return Ok(SelectExpr::Aggregate(AggKind::Avg { column: col }));
        }
        "min" => {
            let col = get_single_column_arg(&args, "MIN")?;
            return Ok(SelectExpr::Aggregate(AggKind::Min { column: col }));
        }
        "max" => {
            let col = get_single_column_arg(&args, "MAX")?;
            return Ok(SelectExpr::Aggregate(AggKind::Max { column: col }));
        }
        _ => {}
    }

    // Scalar functions
    let scalar_func = match func_name.as_str() {
        // String functions
        "upper" => ScalarFunction::Upper(Box::new(get_arg(&args, 0, "UPPER")?)),
        "lower" => ScalarFunction::Lower(Box::new(get_arg(&args, 0, "LOWER")?)),
        "length" | "len" | "char_length" | "character_length" => {
            ScalarFunction::Length(Box::new(get_arg(&args, 0, "LENGTH")?))
        }
        "trim" => ScalarFunction::Trim(Box::new(get_arg(&args, 0, "TRIM")?)),
        "ltrim" => ScalarFunction::LTrim(Box::new(get_arg(&args, 0, "LTRIM")?)),
        "rtrim" => ScalarFunction::RTrim(Box::new(get_arg(&args, 0, "RTRIM")?)),
        "concat" => ScalarFunction::Concat(args),
        "substring" | "substr" => {
            let expr = get_arg(&args, 0, "SUBSTRING")?;
            let start = get_int_arg(&args, 1, "SUBSTRING")?;
            let length = if args.len() > 2 {
                Some(get_int_arg(&args, 2, "SUBSTRING")?)
            } else {
                None
            };
            ScalarFunction::Substring {
                expr: Box::new(expr),
                start,
                length,
            }
        }
        "replace" => {
            let expr = get_arg(&args, 0, "REPLACE")?;
            let from = get_string_arg(&args, 1, "REPLACE")?;
            let to = get_string_arg(&args, 2, "REPLACE")?;
            ScalarFunction::Replace {
                expr: Box::new(expr),
                from,
                to,
            }
        }
        "left" => {
            let expr = get_arg(&args, 0, "LEFT")?;
            let count = get_int_arg(&args, 1, "LEFT")?;
            ScalarFunction::Left {
                expr: Box::new(expr),
                count,
            }
        }
        "right" => {
            let expr = get_arg(&args, 0, "RIGHT")?;
            let count = get_int_arg(&args, 1, "RIGHT")?;
            ScalarFunction::Right {
                expr: Box::new(expr),
                count,
            }
        }
        "reverse" => ScalarFunction::Reverse(Box::new(get_arg(&args, 0, "REVERSE")?)),
        "coalesce" => ScalarFunction::Coalesce(args),

        // Math functions
        "abs" => ScalarFunction::Abs(Box::new(get_arg(&args, 0, "ABS")?)),
        "round" => {
            let expr = get_arg(&args, 0, "ROUND")?;
            let precision = if args.len() > 1 {
                Some(get_int_arg(&args, 1, "ROUND")? as i32)
            } else {
                None
            };
            ScalarFunction::Round {
                expr: Box::new(expr),
                precision,
            }
        }
        "ceil" | "ceiling" => ScalarFunction::Ceil(Box::new(get_arg(&args, 0, "CEIL")?)),
        "floor" => ScalarFunction::Floor(Box::new(get_arg(&args, 0, "FLOOR")?)),
        "mod" => {
            let dividend = get_arg(&args, 0, "MOD")?;
            let divisor = get_arg(&args, 1, "MOD")?;
            ScalarFunction::Mod {
                dividend: Box::new(dividend),
                divisor: Box::new(divisor),
            }
        }
        "power" | "pow" => {
            let base = get_arg(&args, 0, "POWER")?;
            let exponent = get_arg(&args, 1, "POWER")?;
            ScalarFunction::Power {
                base: Box::new(base),
                exponent: Box::new(exponent),
            }
        }
        "sqrt" => ScalarFunction::Sqrt(Box::new(get_arg(&args, 0, "SQRT")?)),
        "log" => {
            let expr = get_arg(&args, 0, "LOG")?;
            let base = if args.len() > 1 {
                Some(get_float_arg(&args, 1, "LOG")?)
            } else {
                None
            };
            ScalarFunction::Log {
                expr: Box::new(expr),
                base,
            }
        }
        "ln" => ScalarFunction::Ln(Box::new(get_arg(&args, 0, "LN")?)),
        "exp" => ScalarFunction::Exp(Box::new(get_arg(&args, 0, "EXP")?)),
        "sign" => ScalarFunction::Sign(Box::new(get_arg(&args, 0, "SIGN")?)),
        "greatest" => ScalarFunction::Greatest(args),
        "least" => ScalarFunction::Least(args),

        // Date/Time functions
        "now" | "current_timestamp" => ScalarFunction::Now,
        "current_date" => ScalarFunction::CurrentDate,
        "date_trunc" => {
            let unit = get_string_arg(&args, 0, "DATE_TRUNC")?;
            let expr = get_arg(&args, 1, "DATE_TRUNC")?;
            ScalarFunction::DateTrunc {
                unit,
                expr: Box::new(expr),
            }
        }
        "extract" => {
            // EXTRACT is usually parsed specially by sqlparser, but handle function form too
            let field = get_string_arg(&args, 0, "EXTRACT")?;
            let expr = get_arg(&args, 1, "EXTRACT")?;
            ScalarFunction::Extract {
                field,
                expr: Box::new(expr),
            }
        }
        "date_add" | "dateadd" => {
            let expr = get_arg(&args, 0, "DATE_ADD")?;
            let interval = get_int_arg(&args, 1, "DATE_ADD")?;
            let unit = get_string_arg(&args, 2, "DATE_ADD")?;
            ScalarFunction::DateAdd {
                expr: Box::new(expr),
                interval,
                unit,
            }
        }
        "date_sub" | "datesub" => {
            let expr = get_arg(&args, 0, "DATE_SUB")?;
            let interval = get_int_arg(&args, 1, "DATE_SUB")?;
            let unit = get_string_arg(&args, 2, "DATE_SUB")?;
            ScalarFunction::DateSub {
                expr: Box::new(expr),
                interval,
                unit,
            }
        }
        "datediff" => {
            let unit = get_string_arg(&args, 0, "DATEDIFF")?;
            let start = get_arg(&args, 1, "DATEDIFF")?;
            let end = get_arg(&args, 2, "DATEDIFF")?;
            ScalarFunction::DateDiff {
                unit,
                start: Box::new(start),
                end: Box::new(end),
            }
        }
        "to_timestamp" => ScalarFunction::ToTimestamp(Box::new(get_arg(&args, 0, "TO_TIMESTAMP")?)),
        "from_unixtime" => {
            ScalarFunction::FromUnixtime(Box::new(get_arg(&args, 0, "FROM_UNIXTIME")?))
        }

        // JSON functions
        "json_extract" | "json_value" => {
            let expr = get_arg(&args, 0, "JSON_EXTRACT")?;
            let path = get_string_arg(&args, 1, "JSON_EXTRACT")?;
            ScalarFunction::JsonExtract {
                expr: Box::new(expr),
                path,
            }
        }
        "json_extract_scalar" | "json_unquote" => {
            let expr = get_arg(&args, 0, "JSON_EXTRACT_SCALAR")?;
            let path = get_string_arg(&args, 1, "JSON_EXTRACT_SCALAR")?;
            ScalarFunction::JsonExtractScalar {
                expr: Box::new(expr),
                path,
            }
        }
        "json_array" => ScalarFunction::JsonArray(args),
        "json_object" => {
            // Pairs of key, value
            if args.len() % 2 != 0 {
                return Err(EngineError::InvalidArgument(
                    "JSON_OBJECT requires an even number of arguments (key-value pairs)".into(),
                ));
            }
            let mut pairs = Vec::new();
            for i in (0..args.len()).step_by(2) {
                let key = match &args[i] {
                    SelectExpr::Literal(LiteralValue::String(s)) => s.clone(),
                    SelectExpr::Column(c) => c.clone(),
                    _ => {
                        return Err(EngineError::InvalidArgument(
                            "JSON_OBJECT keys must be string literals or column names".into(),
                        ));
                    }
                };
                pairs.push((key, args[i + 1].clone()));
            }
            ScalarFunction::JsonObject(pairs)
        }
        "json_type" | "json_typeof" => {
            ScalarFunction::JsonType(Box::new(get_arg(&args, 0, "JSON_TYPE")?))
        }
        "json_contains_path" | "json_exists" => {
            let expr = get_arg(&args, 0, "JSON_CONTAINS_PATH")?;
            let path = get_string_arg(&args, 1, "JSON_CONTAINS_PATH")?;
            ScalarFunction::JsonContainsPath {
                expr: Box::new(expr),
                path,
            }
        }
        "json_array_length" | "json_length" => {
            ScalarFunction::JsonArrayLength(Box::new(get_arg(&args, 0, "JSON_ARRAY_LENGTH")?))
        }
        "json_keys" => ScalarFunction::JsonKeys(Box::new(get_arg(&args, 0, "JSON_KEYS")?)),
        "json_valid" | "is_json" => {
            ScalarFunction::JsonValid(Box::new(get_arg(&args, 0, "JSON_VALID")?))
        }
        "json_pretty" => ScalarFunction::JsonPretty(Box::new(get_arg(&args, 0, "JSON_PRETTY")?)),

        // Array functions
        "array" => ScalarFunction::Array(args),
        "array_length" | "cardinality" => {
            ScalarFunction::ArrayLength(Box::new(get_arg(&args, 0, "ARRAY_LENGTH")?))
        }
        "array_contains" | "array_has" => {
            let array = get_arg(&args, 0, "ARRAY_CONTAINS")?;
            let value = get_arg(&args, 1, "ARRAY_CONTAINS")?;
            ScalarFunction::ArrayContains {
                array: Box::new(array),
                value: Box::new(value),
            }
        }
        "array_element" | "element_at" => {
            let array = get_arg(&args, 0, "ARRAY_ELEMENT")?;
            let index = get_int_arg(&args, 1, "ARRAY_ELEMENT")?;
            ScalarFunction::ArrayElement {
                array: Box::new(array),
                index,
            }
        }
        "array_append" | "array_push" => {
            let array = get_arg(&args, 0, "ARRAY_APPEND")?;
            let element = get_arg(&args, 1, "ARRAY_APPEND")?;
            ScalarFunction::ArrayAppend {
                array: Box::new(array),
                element: Box::new(element),
            }
        }
        "array_concat" | "array_cat" => ScalarFunction::ArrayConcat(args),
        "array_distinct" | "array_unique" => {
            ScalarFunction::ArrayDistinct(Box::new(get_arg(&args, 0, "ARRAY_DISTINCT")?))
        }
        "array_join" | "array_to_string" => {
            let array = get_arg(&args, 0, "ARRAY_JOIN")?;
            let delimiter = if args.len() > 1 {
                get_string_arg(&args, 1, "ARRAY_JOIN")?
            } else {
                ",".to_string()
            };
            ScalarFunction::ArrayJoin {
                array: Box::new(array),
                delimiter,
            }
        }

        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported function: {func_name}"
            )));
        }
    };

    Ok(SelectExpr::Function(scalar_func))
}

/// Parse a window function
fn parse_window_function(
    func_name: &str,
    args: &[SelectExpr],
    func: &sqlparser::ast::Function,
) -> Result<SelectExpr, EngineError> {
    let window_func = match func_name {
        "row_number" => WindowFunction::RowNumber,
        "rank" => WindowFunction::Rank,
        "dense_rank" => WindowFunction::DenseRank,
        "ntile" => {
            let n = get_int_arg(args, 0, "NTILE")?;
            WindowFunction::NTile(n)
        }
        "lag" => {
            let expr = get_arg(args, 0, "LAG")?;
            let offset = if args.len() > 1 {
                get_int_arg(args, 1, "LAG")?
            } else {
                1
            };
            let default = if args.len() > 2 {
                Some(Box::new(args[2].clone()))
            } else {
                None
            };
            WindowFunction::Lag {
                expr: Box::new(expr),
                offset,
                default,
            }
        }
        "lead" => {
            let expr = get_arg(args, 0, "LEAD")?;
            let offset = if args.len() > 1 {
                get_int_arg(args, 1, "LEAD")?
            } else {
                1
            };
            let default = if args.len() > 2 {
                Some(Box::new(args[2].clone()))
            } else {
                None
            };
            WindowFunction::Lead {
                expr: Box::new(expr),
                offset,
                default,
            }
        }
        "first_value" => {
            WindowFunction::FirstValue(Box::new(get_arg(args, 0, "FIRST_VALUE")?))
        }
        "last_value" => {
            WindowFunction::LastValue(Box::new(get_arg(args, 0, "LAST_VALUE")?))
        }
        "nth_value" => {
            let expr = get_arg(args, 0, "NTH_VALUE")?;
            let n = get_int_arg(args, 1, "NTH_VALUE")?;
            WindowFunction::NthValue {
                expr: Box::new(expr),
                n,
            }
        }
        // Window versions of aggregate functions
        "sum" => WindowFunction::WindowSum(Box::new(get_arg(args, 0, "SUM")?)),
        "avg" => WindowFunction::WindowAvg(Box::new(get_arg(args, 0, "AVG")?)),
        "min" => WindowFunction::WindowMin(Box::new(get_arg(args, 0, "MIN")?)),
        "max" => WindowFunction::WindowMax(Box::new(get_arg(args, 0, "MAX")?)),
        "count" => {
            let arg = if args.is_empty() {
                None
            } else {
                Some(Box::new(args[0].clone()))
            };
            WindowFunction::WindowCount(arg)
        }
        _ => {
            return Err(EngineError::NotImplemented(format!(
                "unsupported window function: {func_name}"
            )));
        }
    };

    // Parse OVER clause
    let spec = if let Some(over) = &func.over {
        parse_window_spec(over)?
    } else {
        WindowSpec::default()
    };

    Ok(SelectExpr::Window {
        function: window_func,
        spec,
    })
}

/// Parse window specification from OVER clause
fn parse_window_spec(over: &sqlparser::ast::WindowType) -> Result<WindowSpec, EngineError> {
    match over {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            let partition_by = spec
                .partition_by
                .iter()
                .filter_map(|e| match e {
                    Expr::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();

            let order_by = spec
                .order_by
                .iter()
                .filter_map(|ob| {
                    if let Expr::Identifier(ident) = &ob.expr {
                        Some(OrderByClause {
                            column: ident.value.clone(),
                            ascending: ob.asc.unwrap_or(true),
                            nulls_first: ob.nulls_first,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            let frame = spec.window_frame.as_ref().map(|f| {
                let unit = match f.units {
                    sqlparser::ast::WindowFrameUnits::Rows => WindowFrameUnit::Rows,
                    sqlparser::ast::WindowFrameUnits::Range => WindowFrameUnit::Range,
                    sqlparser::ast::WindowFrameUnits::Groups => WindowFrameUnit::Groups,
                };
                let start = parse_window_frame_bound(&f.start_bound);
                let end = f.end_bound.as_ref().map(|b| parse_window_frame_bound(b));
                WindowFrame { unit, start, end }
            });

            Ok(WindowSpec {
                partition_by,
                order_by,
                frame,
            })
        }
        sqlparser::ast::WindowType::NamedWindow(_) => Err(EngineError::NotImplemented(
            "named windows not supported".into(),
        )),
    }
}

fn parse_window_frame_bound(bound: &sqlparser::ast::WindowFrameBound) -> WindowFrameBound {
    match bound {
        sqlparser::ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        sqlparser::ast::WindowFrameBound::Preceding(None) => WindowFrameBound::UnboundedPreceding,
        sqlparser::ast::WindowFrameBound::Preceding(Some(n)) => {
            WindowFrameBound::Preceding(expr_to_u64(n).unwrap_or(1))
        }
        sqlparser::ast::WindowFrameBound::Following(None) => WindowFrameBound::UnboundedFollowing,
        sqlparser::ast::WindowFrameBound::Following(Some(n)) => {
            WindowFrameBound::Following(expr_to_u64(n).unwrap_or(1))
        }
    }
}

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}

// Helper functions for argument extraction
fn get_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<SelectExpr, EngineError> {
    args.get(idx).cloned().ok_or_else(|| {
        EngineError::InvalidArgument(format!("{func_name} requires argument at position {idx}"))
    })
}

fn get_single_column_arg(args: &[SelectExpr], func_name: &str) -> Result<String, EngineError> {
    match args.first() {
        Some(SelectExpr::Column(col)) => Ok(col.clone()),
        Some(SelectExpr::QualifiedColumn { column, .. }) => Ok(column.clone()),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires a column argument"
        ))),
    }
}

fn get_int_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<i64, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::Integer(n))) => Ok(*n),
        Some(SelectExpr::Literal(LiteralValue::Float(f))) => Ok(*f as i64),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires integer argument at position {idx}"
        ))),
    }
}

fn get_float_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<f64, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::Float(f))) => Ok(*f),
        Some(SelectExpr::Literal(LiteralValue::Integer(n))) => Ok(*n as f64),
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires numeric argument at position {idx}"
        ))),
    }
}

fn get_string_arg(args: &[SelectExpr], idx: usize, func_name: &str) -> Result<String, EngineError> {
    match args.get(idx) {
        Some(SelectExpr::Literal(LiteralValue::String(s))) => Ok(s.clone()),
        Some(SelectExpr::Column(c)) => Ok(c.clone()), // Allow column name as string
        _ => Err(EngineError::InvalidArgument(format!(
            "{func_name} requires string argument at position {idx}"
        ))),
    }
}

/// Parse SELECT items with full expression support
pub fn parse_select_items_extended(
    items: &[SelectItem],
) -> Result<Vec<SelectColumn>, EngineError> {
    let mut columns = Vec::new();

    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                columns.push(SelectColumn {
                    expr: SelectExpr::Column("*".to_string()),
                    alias: None,
                });
            }
            SelectItem::UnnamedExpr(expr) => {
                let parsed = parse_expr(expr)?;
                columns.push(SelectColumn {
                    expr: parsed,
                    alias: None,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let parsed = parse_expr(expr)?;
                columns.push(SelectColumn {
                    expr: parsed,
                    alias: Some(alias.value.clone()),
                });
            }
            SelectItem::QualifiedWildcard(name, _) => {
                columns.push(SelectColumn {
                    expr: SelectExpr::Column(format!("{}.*", name)),
                    alias: None,
                });
            }
        }
    }

    Ok(columns)
}

/// Parse CTEs from WITH clause
pub fn parse_ctes(with: &With) -> Result<Vec<CteDefinition>, EngineError> {
    let mut ctes = Vec::new();
    let is_recursive = with.recursive;

    for cte in &with.cte_tables {
        let columns = if cte.alias.columns.is_empty() {
            None
        } else {
            Some(cte.alias.columns.iter().map(|c| c.value.clone()).collect())
        };

        // Parse the CTE query
        let query = parse_query_from_sqlparser(&cte.query)?;

        // Convert the query back to SQL string for recursive execution
        let raw_sql = cte.query.to_string();

        ctes.push(CteDefinition {
            name: cte.alias.name.value.clone(),
            columns,
            query,
            recursive: is_recursive,
            raw_sql,
        });
    }

    Ok(ctes)
}

/// Extract the first SELECT from a SetExpr (handles nested set operations)
fn extract_first_select(set_expr: &SetExpr) -> Result<&Select, EngineError> {
    match set_expr {
        SetExpr::Select(s) => Ok(s.as_ref()),
        SetExpr::SetOperation { left, .. } => extract_first_select(left),
        _ => Err(EngineError::NotImplemented(
            "only SELECT queries supported in CTEs".into(),
        )),
    }
}

/// Parse a sqlparser Query into our ParsedQuery (internal helper)
fn parse_query_from_sqlparser(query: &Query) -> Result<ParsedQuery, EngineError> {
    // Get the main SELECT body, handling both SELECT and SET operations (UNION, etc.)
    let select = match &*query.body {
        SetExpr::Select(s) => s.as_ref(),
        SetExpr::SetOperation { left, .. } => {
            // For UNION/INTERSECT/EXCEPT, extract the left-most SELECT for metadata
            extract_first_select(left)?
        }
        _ => {
            return Err(EngineError::NotImplemented(
                "only SELECT queries supported in CTEs".into(),
            ))
        }
    };

    // For CTEs, we use a simplified parsing - just get basic info
    let (database, table) = if select.from.is_empty() {
        ("default".to_string(), "dual".to_string())
    } else {
        match &select.from[0].relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                parse_table_name(&table_name)?
            }
            _ => ("default".to_string(), "unknown".to_string()),
        }
    };

    Ok(ParsedQuery {
        database: Some(database),
        table: Some(table),
        projection: None,
        filter: QueryFilter::default(),
        aggregation: None,
        order_by: None,
        distinct: false,
        joins: Vec::new(),
        computed_columns: Vec::new(),
        ctes: Vec::new(),
        sample: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("mydb".to_string()));
                assert_eq!(q.table, Some("mytable".to_string()));
                assert!(q.projection.is_none());
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT col1, col2 FROM test WHERE tenant_id = 42 AND event_time >= 1000";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("default".to_string()));
                assert_eq!(q.table, Some("test".to_string()));
                assert_eq!(q.filter.tenant_id_eq, Some(42));
                assert_eq!(q.filter.event_time_ge, Some(1000));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_aggregation() {
        let sql = "SELECT COUNT(*), SUM(duration_ms) FROM analytics.events GROUP BY tenant_id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("analytics".to_string()));
                assert_eq!(q.table, Some("events".to_string()));
                let agg = q.aggregation.unwrap();
                assert_eq!(agg.group_by, GroupBy::Tenant);
                assert_eq!(agg.aggs.len(), 2);
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let sql = "DROP TABLE IF EXISTS mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DropTable {
                database,
                table,
                if_exists,
            }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "mytable");
                assert!(if_exists);
            }
            _ => panic!("expected drop table"),
        }
    }

    #[test]
    fn test_parse_drop_database() {
        let sql = "DROP DATABASE IF EXISTS analytics";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DropDatabase { name, if_exists }) => {
                assert_eq!(name, "analytics");
                assert!(if_exists);
            }
            _ => panic!("expected drop database"),
        }
    }

    #[test]
    fn test_parse_alter_add_column() {
        let sql = "ALTER TABLE mydb.events ADD COLUMN latency int64";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::AlterTableAddColumn {
                database,
                table,
                column,
                data_type,
                nullable,
            }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "events");
                assert_eq!(column, "latency");
                assert_eq!(data_type, "int64");
                assert!(nullable);
            }
            _ => panic!("expected alter table add column"),
        }
    }

    #[test]
    fn test_parse_alter_drop_column() {
        let sql = "ALTER TABLE events DROP COLUMN status";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::AlterTableDropColumn {
                database,
                table,
                column,
            }) => {
                assert_eq!(database, "default");
                assert_eq!(table, "events");
                assert_eq!(column, "status");
            }
            _ => panic!("expected alter table drop column"),
        }
    }

    #[test]
    fn test_parse_truncate_table() {
        let sql = "TRUNCATE TABLE mydb.mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::TruncateTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "mytable");
            }
            _ => panic!("expected truncate table"),
        }
    }

    #[test]
    fn test_parse_limit() {
        let sql = "SELECT * FROM test LIMIT 100";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.filter.limit, Some(100));
            }
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_parse_create_user() {
        let sql = "CREATE USER appuser WITH PASSWORD 'SecurePass123'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser {
                username,
                password,
                options,
            }) => {
                assert_eq!(username, "appuser");
                assert_eq!(password, "SecurePass123");
                assert!(!options.superuser);
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_create_user_password_text_does_not_grant_superuser() {
        let sql = "CREATE USER appuser WITH PASSWORD 'SUPERUSERpw'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser { options, .. }) => {
                assert!(!options.superuser);
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_parse_create_user_with_options() {
        let sql = "CREATE USER admin WITH PASSWORD 'AdminPass123' SUPERUSER DEFAULT DATABASE 'mydb' CONNECTION LIMIT 10";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateUser {
                username,
                password,
                options,
            }) => {
                assert_eq!(username, "admin");
                assert_eq!(password, "AdminPass123");
                assert!(options.superuser);
                assert_eq!(options.default_database, Some("mydb".to_string()));
                assert_eq!(options.connection_limit, Some(10));
            }
            _ => panic!("expected CreateUser"),
        }
    }

    #[test]
    fn test_parse_drop_user() {
        let sql = "DROP USER olduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::DropUser { username }) => {
                assert_eq!(username, "olduser");
            }
            _ => panic!("expected DropUser"),
        }
    }

    #[test]
    fn test_parse_alter_user_password() {
        let sql = "ALTER USER appuser SET PASSWORD 'NewSecurePass456'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::AlterUserPassword {
                username,
                new_password,
            }) => {
                assert_eq!(username, "appuser");
                assert_eq!(new_password, "NewSecurePass456");
            }
            _ => panic!("expected AlterUserPassword"),
        }
    }

    #[test]
    fn test_parse_grant_privilege() {
        let sql = "GRANT SELECT, INSERT ON DATABASE mydb TO appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::Grant {
                privileges,
                target_type,
                target_name,
                grantee,
                ..
            }) => {
                assert_eq!(privileges, vec!["SELECT", "INSERT"]);
                assert!(matches!(target_type, GrantTargetType::Database));
                assert_eq!(target_name, Some("mydb".to_string()));
                assert_eq!(grantee, "appuser");
            }
            _ => panic!("expected Grant"),
        }
    }

    #[test]
    fn test_parse_grant_role() {
        let sql = "GRANT readonly TO appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::GrantRole { role, username }) => {
                assert_eq!(role, "readonly");
                assert_eq!(username, "appuser");
            }
            _ => panic!("expected GrantRole"),
        }
    }

    #[test]
    fn test_parse_revoke_privilege() {
        let sql = "REVOKE INSERT ON DATABASE mydb FROM appuser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::Revoke {
                privileges,
                target_type,
                target_name,
                grantee,
                ..
            }) => {
                assert_eq!(privileges, vec!["INSERT"]);
                assert!(matches!(target_type, GrantTargetType::Database));
                assert_eq!(target_name, Some("mydb".to_string()));
                assert_eq!(grantee, "appuser");
            }
            _ => panic!("expected Revoke"),
        }
    }

    #[test]
    fn test_parse_show_users() {
        let sql = "SHOW USERS";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::ShowUsers) => {}
            _ => panic!("expected ShowUsers"),
        }
    }

    #[test]
    fn test_parse_create_role() {
        let sql = "CREATE ROLE myrole WITH DESCRIPTION 'A custom role'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::CreateRole { name, description }) => {
                assert_eq!(name, "myrole");
                assert_eq!(description, Some("A custom role".to_string()));
            }
            _ => panic!("expected CreateRole"),
        }
    }

    #[test]
    fn test_parse_lock_unlock_user() {
        let sql = "LOCK USER baduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::LockUser { username }) => {
                assert_eq!(username, "baduser");
            }
            _ => panic!("expected LockUser"),
        }

        let sql = "UNLOCK USER baduser";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Auth(AuthCommand::UnlockUser { username }) => {
                assert_eq!(username, "baduser");
            }
            _ => panic!("expected UnlockUser"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO mydb.events (tenant_id, duration_ms, status) VALUES (1, 500, 'completed')";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "events");
                assert_eq!(
                    cmd.columns,
                    Some(vec![
                        "tenant_id".to_string(),
                        "duration_ms".to_string(),
                        "status".to_string()
                    ])
                );
                assert_eq!(cmd.values.len(), 1);
                assert_eq!(cmd.values[0].len(), 3);
                assert!(matches!(cmd.values[0][0], SqlValue::Integer(1)));
                assert!(matches!(cmd.values[0][1], SqlValue::Integer(500)));
                assert!(matches!(cmd.values[0][2], SqlValue::String(ref s) if s == "completed"));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let sql = "INSERT INTO events VALUES (1, 100), (2, 200), (3, 300)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "events");
                assert!(cmd.columns.is_none());
                assert_eq!(cmd.values.len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_negative_numbers() {
        let sql = "INSERT INTO test VALUES (-42, -3.14)";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Insert(cmd) => {
                assert_eq!(cmd.values.len(), 1);
                assert!(matches!(cmd.values[0][0], SqlValue::Integer(-42)));
                assert!(matches!(cmd.values[0][1], SqlValue::Float(f) if (f + 3.14).abs() < 0.001));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_update() {
        let sql = "UPDATE mydb.users SET name = 'John', age = 30 WHERE id = 1";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "users");
                assert_eq!(cmd.assignments.len(), 2);
                assert_eq!(cmd.assignments[0].0, "name");
                assert!(matches!(&cmd.assignments[0].1, SqlValue::String(s) if s == "John"));
                assert_eq!(cmd.assignments[1].0, "age");
                assert!(matches!(cmd.assignments[1].1, SqlValue::Integer(30)));
                assert!(cmd.where_clause.is_some());
                assert!(cmd.where_clause.as_ref().unwrap().contains("id"));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_update_no_where() {
        let sql = "UPDATE users SET status = 'inactive'";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Update(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "users");
                assert_eq!(cmd.assignments.len(), 1);
                assert!(cmd.where_clause.is_none());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let sql = "DELETE FROM mydb.users WHERE id = 5";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "mydb");
                assert_eq!(cmd.table, "users");
                assert!(cmd.where_clause.is_some());
                assert!(cmd.where_clause.as_ref().unwrap().contains("id"));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_no_where() {
        let sql = "DELETE FROM users";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Delete(cmd) => {
                assert_eq!(cmd.database, "default");
                assert_eq!(cmd.table, "users");
                assert!(cmd.where_clause.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_inner_join() {
        let sql = "SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("default".to_string()));
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].join_type, JoinType::Inner);
                assert_eq!(q.joins[0].table, "customers");
                assert_eq!(q.joins[0].on_condition.left_column, "customer_id");
                assert_eq!(q.joins[0].on_condition.right_column, "id");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_left_join() {
        let sql = "SELECT * FROM mydb.orders LEFT JOIN mydb.customers ON orders.customer_id = customers.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.database, Some("mydb".to_string()));
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].join_type, JoinType::Left);
                assert_eq!(q.joins[0].database, "mydb");
                assert_eq!(q.joins[0].table, "customers");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_join_with_alias() {
        let sql = "SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.id";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Query(q) => {
                assert_eq!(q.table, Some("orders".to_string()));
                assert_eq!(q.joins.len(), 1);
                assert_eq!(q.joins[0].alias, Some("c".to_string()));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_parse_show_databases() {
        let sql = "SHOW DATABASES";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDatabases) => {}
            _ => panic!("expected ShowDatabases"),
        }

        // With semicolon
        let sql = "SHOW DATABASES;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowDatabases) => {}
            _ => panic!("expected ShowDatabases"),
        }
    }

    #[test]
    fn test_parse_show_tables() {
        // Without database
        let sql = "SHOW TABLES";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert!(database.is_none());
            }
            _ => panic!("expected ShowTables"),
        }

        // With IN database
        let sql = "SHOW TABLES IN mydb";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert_eq!(database, Some("mydb".to_string()));
            }
            _ => panic!("expected ShowTables with database"),
        }

        // With FROM database (MySQL-style)
        let sql = "SHOW TABLES FROM testdb;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::ShowTables { database }) => {
                assert_eq!(database, Some("testdb".to_string()));
            }
            _ => panic!("expected ShowTables with database"),
        }
    }

    #[test]
    fn test_parse_describe_table() {
        // DESCRIBE database.table
        let sql = "DESCRIBE mydb.users";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "users");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESC shorthand
        let sql = "DESC mydb.events;";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "mydb");
                assert_eq!(table, "events");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESCRIBE TABLE database.table
        let sql = "DESCRIBE TABLE testdb.events";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "testdb");
                assert_eq!(table, "events");
            }
            _ => panic!("expected DescribeTable"),
        }

        // DESC TABLE with unqualified table name
        let sql = "DESC TABLE mytable";
        let result = parse_sql(sql).unwrap();
        match result {
            SqlStatement::Ddl(DdlCommand::DescribeTable { database, table }) => {
                assert_eq!(database, "default");
                assert_eq!(table, "mytable");
            }
            _ => panic!("expected DescribeTable with default database"),
        }
    }
}
