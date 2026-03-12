//! Foreign Data Wrappers (FDW)
//!
//! Provides pluggable connectors for querying external databases
//! including MySQL, PostgreSQL, MongoDB, Redis, and custom sources.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// FDW Error types
#[derive(Debug, Clone)]
pub enum FdwError {
    /// Wrapper not found
    WrapperNotFound(String),
    /// Server not found
    ServerNotFound(String),
    /// User mapping not found
    UserMappingNotFound(String),
    /// Connection failed
    ConnectionFailed(String),
    /// Query execution failed
    QueryFailed(String),
    /// Invalid option
    InvalidOption(String),
    /// Authentication failed
    AuthenticationFailed(String),
    /// Type conversion error
    TypeConversion(String),
    /// Feature not supported
    NotSupported(String),
    /// Table not found in foreign server
    TableNotFound(String),
}

impl fmt::Display for FdwError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrapperNotFound(s) => write!(f, "wrapper not found: {}", s),
            Self::ServerNotFound(s) => write!(f, "foreign server not found: {}", s),
            Self::UserMappingNotFound(s) => write!(f, "user mapping not found: {}", s),
            Self::ConnectionFailed(s) => write!(f, "connection failed: {}", s),
            Self::QueryFailed(s) => write!(f, "query failed: {}", s),
            Self::InvalidOption(s) => write!(f, "invalid option: {}", s),
            Self::AuthenticationFailed(s) => write!(f, "authentication failed: {}", s),
            Self::TypeConversion(s) => write!(f, "type conversion error: {}", s),
            Self::NotSupported(s) => write!(f, "not supported: {}", s),
            Self::TableNotFound(s) => write!(f, "foreign table not found: {}", s),
        }
    }
}

impl std::error::Error for FdwError {}

/// Foreign data wrapper definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub handler: String,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
    pub wrapper_type: WrapperType,
}

/// Types of foreign data wrappers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WrapperType {
    /// PostgreSQL wire protocol
    Postgres,
    /// MySQL wire protocol
    MySQL,
    /// MongoDB
    MongoDB,
    /// Redis
    Redis,
    /// SQLite
    SQLite,
    /// CSV/Parquet files
    File,
    /// HTTP/REST API
    Http,
    /// JDBC (generic)
    Jdbc,
    /// Custom/Plugin
    Custom,
}

/// Foreign server definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignServer {
    pub name: String,
    pub wrapper_name: String,
    pub server_type: Option<String>,
    pub version: Option<String>,
    pub options: HashMap<String, String>,
}

/// User mapping for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMapping {
    pub local_user: String,
    pub server_name: String,
    pub remote_user: Option<String>,
    pub options: HashMap<String, String>,
}

/// Foreign table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignTable {
    pub name: String,
    pub server_name: String,
    pub schema: Option<String>,
    pub columns: Vec<ForeignColumn>,
    pub options: HashMap<String, String>,
}

/// Foreign column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignColumn {
    pub name: String,
    pub data_type: ForeignDataType,
    pub nullable: bool,
    pub options: HashMap<String, String>,
}

/// Foreign data types with mapping hints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForeignDataType {
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
    Decimal { precision: u8, scale: u8 },
    String,
    Binary,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Json,
    Array(Box<ForeignDataType>),
    Custom(String),
}

/// Query pushdown capabilities
#[derive(Debug, Clone, Default)]
pub struct PushdownCapabilities {
    /// Can push WHERE clauses
    pub filter: bool,
    /// Can push projections (column selection)
    pub projection: bool,
    /// Can push ORDER BY
    pub sort: bool,
    /// Can push LIMIT
    pub limit: bool,
    /// Can push aggregations
    pub aggregate: bool,
    /// Can push GROUP BY
    pub group_by: bool,
    /// Can push JOINs
    pub join: bool,
    /// Supported functions
    pub functions: Vec<String>,
    /// Supported operators
    pub operators: Vec<String>,
}

/// Foreign scan state
pub struct ForeignScanState {
    pub table: ForeignTable,
    pub connection: Box<dyn FdwConnection>,
    pub cursor: Option<Box<dyn FdwCursor>>,
    pub pushdown_predicates: Vec<FdwPredicate>,
    pub projected_columns: Vec<String>,
    pub stats: ForeignScanStats,
}

/// Foreign scan statistics
#[derive(Debug, Clone, Default)]
pub struct ForeignScanStats {
    pub rows_fetched: u64,
    pub bytes_transferred: u64,
    pub remote_time_ms: u64,
    pub local_time_ms: u64,
    pub batches_fetched: u64,
}

/// Predicate for pushdown
#[derive(Debug, Clone)]
pub enum FdwPredicate {
    Equals(String, FdwValue),
    NotEquals(String, FdwValue),
    LessThan(String, FdwValue),
    LessOrEqual(String, FdwValue),
    GreaterThan(String, FdwValue),
    GreaterOrEqual(String, FdwValue),
    Between(String, FdwValue, FdwValue),
    In(String, Vec<FdwValue>),
    Like(String, String),
    IsNull(String),
    IsNotNull(String),
    And(Box<FdwPredicate>, Box<FdwPredicate>),
    Or(Box<FdwPredicate>, Box<FdwPredicate>),
    Not(Box<FdwPredicate>),
}

/// Value for predicates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FdwValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),
    Timestamp(i64),
}

/// FDW Connection trait
pub trait FdwConnection: Send + Sync {
    /// Get connection info
    fn info(&self) -> ConnectionInfo;

    /// Check if connection is valid
    fn is_valid(&self) -> bool;

    /// Ping the remote server
    fn ping(&self) -> Result<Duration, FdwError>;

    /// Close the connection
    fn close(&mut self) -> Result<(), FdwError>;

    /// Begin a remote transaction
    fn begin_transaction(&mut self) -> Result<(), FdwError>;

    /// Commit remote transaction
    fn commit(&mut self) -> Result<(), FdwError>;

    /// Rollback remote transaction
    fn rollback(&mut self) -> Result<(), FdwError>;

    /// Execute a query and return cursor
    fn execute_query(&mut self, query: &str) -> Result<Box<dyn FdwCursor>, FdwError>;

    /// Execute a modification (INSERT/UPDATE/DELETE)
    fn execute_modify(&mut self, query: &str) -> Result<u64, FdwError>;

    /// Get pushdown capabilities
    fn capabilities(&self) -> PushdownCapabilities;

    /// Import foreign schema (discover tables)
    fn import_schema(
        &self,
        remote_schema: &str,
        options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError>;

    /// Get table statistics
    fn get_stats(&self, table: &str) -> Result<ForeignTableStats, FdwError>;
}

/// Connection info
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub server_type: String,
    pub server_version: String,
    pub connected_at: Instant,
    pub remote_address: String,
    pub database: Option<String>,
}

/// FDW Cursor trait for iterating results
pub trait FdwCursor: Send + Sync {
    /// Get column names
    fn columns(&self) -> Vec<String>;

    /// Get column types
    fn column_types(&self) -> Vec<ForeignDataType>;

    /// Fetch next batch of rows
    fn fetch_batch(&mut self, batch_size: usize) -> Result<Vec<FdwRow>, FdwError>;

    /// Check if more rows available
    fn has_more(&self) -> bool;

    /// Close cursor
    fn close(&mut self) -> Result<(), FdwError>;
}

/// Row from foreign table
#[derive(Debug, Clone)]
pub struct FdwRow {
    pub values: Vec<FdwValue>,
}

/// Foreign table statistics
#[derive(Debug, Clone, Default)]
pub struct ForeignTableStats {
    pub row_count: Option<u64>,
    pub total_bytes: Option<u64>,
    pub column_stats: HashMap<String, ForeignColumnStats>,
}

/// Foreign column statistics
#[derive(Debug, Clone, Default)]
pub struct ForeignColumnStats {
    pub null_fraction: f64,
    pub distinct_count: Option<u64>,
    pub avg_width: Option<u32>,
    pub min_value: Option<FdwValue>,
    pub max_value: Option<FdwValue>,
}

/// FDW Registry
pub struct FdwRegistry {
    wrappers: RwLock<HashMap<String, ForeignDataWrapper>>,
    servers: RwLock<HashMap<String, ForeignServer>>,
    user_mappings: RwLock<HashMap<(String, String), UserMapping>>, // (user, server) -> mapping
    foreign_tables: RwLock<HashMap<String, ForeignTable>>,
    connection_pool: RwLock<HashMap<String, Vec<Box<dyn FdwConnection>>>>,
    handlers: RwLock<HashMap<String, Arc<dyn FdwHandler>>>,
    stats: RwLock<FdwStats>,
}

/// FDW Handler trait - factory for connections
pub trait FdwHandler: Send + Sync {
    /// Create a new connection
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError>;

    /// Validate server options
    fn validate_server_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError>;

    /// Validate table options
    fn validate_table_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError>;

    /// Get required server options
    fn required_server_options(&self) -> Vec<&'static str>;

    /// Get required user mapping options
    fn required_user_mapping_options(&self) -> Vec<&'static str>;
}

/// FDW Statistics
#[derive(Debug, Clone, Default)]
pub struct FdwStats {
    pub total_queries: u64,
    pub total_rows_fetched: u64,
    pub total_bytes_transferred: u64,
    pub connection_opens: u64,
    pub connection_closes: u64,
    pub connection_errors: u64,
    pub query_errors: u64,
}

impl FdwRegistry {
    pub fn new() -> Self {
        let registry = Self {
            wrappers: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            user_mappings: RwLock::new(HashMap::new()),
            foreign_tables: RwLock::new(HashMap::new()),
            connection_pool: RwLock::new(HashMap::new()),
            handlers: RwLock::new(HashMap::new()),
            stats: RwLock::new(FdwStats::default()),
        };

        // Register built-in handlers
        registry.register_builtin_handlers();

        registry
    }

    fn register_builtin_handlers(&self) {
        let mut handlers = self.handlers.write().unwrap();
        handlers.insert("postgres_fdw".to_string(), Arc::new(PostgresFdwHandler));
        handlers.insert("mysql_fdw".to_string(), Arc::new(MySqlFdwHandler));
        handlers.insert("mongo_fdw".to_string(), Arc::new(MongoFdwHandler));
        handlers.insert("redis_fdw".to_string(), Arc::new(RedisFdwHandler));
        handlers.insert("file_fdw".to_string(), Arc::new(FileFdwHandler));
        handlers.insert("http_fdw".to_string(), Arc::new(HttpFdwHandler));
    }

    /// Create a foreign data wrapper
    pub fn create_wrapper(&self, wrapper: ForeignDataWrapper) -> Result<(), FdwError> {
        let mut wrappers = self.wrappers.write().unwrap();

        if wrappers.contains_key(&wrapper.name) {
            return Err(FdwError::InvalidOption(format!(
                "wrapper '{}' already exists",
                wrapper.name
            )));
        }

        wrappers.insert(wrapper.name.clone(), wrapper);
        Ok(())
    }

    /// Drop a foreign data wrapper
    pub fn drop_wrapper(&self, name: &str, cascade: bool) -> Result<(), FdwError> {
        // Check for dependent servers
        let servers = self.servers.read().unwrap();
        let dependent_servers: Vec<_> = servers
            .values()
            .filter(|s| s.wrapper_name == name)
            .map(|s| s.name.clone())
            .collect();

        if !dependent_servers.is_empty() && !cascade {
            return Err(FdwError::InvalidOption(format!(
                "wrapper '{}' has dependent servers: {:?}",
                name, dependent_servers
            )));
        }

        drop(servers);

        // Cascade drop servers
        if cascade {
            for server in dependent_servers {
                self.drop_server(&server, true)?;
            }
        }

        let mut wrappers = self.wrappers.write().unwrap();
        wrappers
            .remove(name)
            .ok_or_else(|| FdwError::WrapperNotFound(name.to_string()))?;

        Ok(())
    }

    /// Create a foreign server
    pub fn create_server(&self, server: ForeignServer) -> Result<(), FdwError> {
        // Validate wrapper exists
        let wrappers = self.wrappers.read().unwrap();
        if !wrappers.contains_key(&server.wrapper_name) {
            return Err(FdwError::WrapperNotFound(server.wrapper_name.clone()));
        }
        drop(wrappers);

        // Validate options
        let handlers = self.handlers.read().unwrap();
        if let Some(handler) = handlers.get(&server.wrapper_name) {
            handler.validate_server_options(&server.options)?;
        }
        drop(handlers);

        let mut servers = self.servers.write().unwrap();
        if servers.contains_key(&server.name) {
            return Err(FdwError::InvalidOption(format!(
                "server '{}' already exists",
                server.name
            )));
        }

        servers.insert(server.name.clone(), server);
        Ok(())
    }

    /// Drop a foreign server
    pub fn drop_server(&self, name: &str, cascade: bool) -> Result<(), FdwError> {
        // Check for dependent foreign tables
        let tables = self.foreign_tables.read().unwrap();
        let dependent_tables: Vec<_> = tables
            .values()
            .filter(|t| t.server_name == name)
            .map(|t| t.name.clone())
            .collect();

        if !dependent_tables.is_empty() && !cascade {
            return Err(FdwError::InvalidOption(format!(
                "server '{}' has dependent tables: {:?}",
                name, dependent_tables
            )));
        }

        drop(tables);

        // Cascade drop tables
        if cascade {
            for table in dependent_tables {
                self.drop_foreign_table(&table)?;
            }
        }

        // Remove user mappings
        let mut mappings = self.user_mappings.write().unwrap();
        mappings.retain(|(_, server), _| server != name);
        drop(mappings);

        // Close connections
        let mut pool = self.connection_pool.write().unwrap();
        pool.remove(name);
        drop(pool);

        let mut servers = self.servers.write().unwrap();
        servers
            .remove(name)
            .ok_or_else(|| FdwError::ServerNotFound(name.to_string()))?;

        Ok(())
    }

    /// Create user mapping
    pub fn create_user_mapping(&self, mapping: UserMapping) -> Result<(), FdwError> {
        // Validate server exists
        let servers = self.servers.read().unwrap();
        if !servers.contains_key(&mapping.server_name) {
            return Err(FdwError::ServerNotFound(mapping.server_name.clone()));
        }
        drop(servers);

        let mut mappings = self.user_mappings.write().unwrap();
        let key = (mapping.local_user.clone(), mapping.server_name.clone());

        if mappings.contains_key(&key) {
            return Err(FdwError::InvalidOption(format!(
                "user mapping for '{}' on '{}' already exists",
                mapping.local_user, mapping.server_name
            )));
        }

        mappings.insert(key, mapping);
        Ok(())
    }

    /// Drop user mapping
    pub fn drop_user_mapping(&self, user: &str, server: &str) -> Result<(), FdwError> {
        let mut mappings = self.user_mappings.write().unwrap();
        let key = (user.to_string(), server.to_string());

        mappings
            .remove(&key)
            .ok_or_else(|| FdwError::UserMappingNotFound(format!("{}@{}", user, server)))?;

        Ok(())
    }

    /// Create foreign table
    pub fn create_foreign_table(&self, table: ForeignTable) -> Result<(), FdwError> {
        // Validate server exists
        let servers = self.servers.read().unwrap();
        if !servers.contains_key(&table.server_name) {
            return Err(FdwError::ServerNotFound(table.server_name.clone()));
        }
        drop(servers);

        let mut tables = self.foreign_tables.write().unwrap();
        if tables.contains_key(&table.name) {
            return Err(FdwError::InvalidOption(format!(
                "foreign table '{}' already exists",
                table.name
            )));
        }

        tables.insert(table.name.clone(), table);
        Ok(())
    }

    /// Drop foreign table
    pub fn drop_foreign_table(&self, name: &str) -> Result<(), FdwError> {
        let mut tables = self.foreign_tables.write().unwrap();
        tables
            .remove(name)
            .ok_or_else(|| FdwError::TableNotFound(name.to_string()))?;
        Ok(())
    }

    /// Get connection to foreign server
    pub fn get_connection(
        &self,
        server_name: &str,
        local_user: &str,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        // Try connection pool first
        {
            let mut pool = self.connection_pool.write().unwrap();
            if let Some(connections) = pool.get_mut(server_name) {
                if let Some(conn) = connections.pop() {
                    if conn.is_valid() {
                        return Ok(conn);
                    }
                }
            }
        }

        // Create new connection
        let server = self
            .servers
            .read()
            .unwrap()
            .get(server_name)
            .cloned()
            .ok_or_else(|| FdwError::ServerNotFound(server_name.to_string()))?;

        let mapping = self
            .user_mappings
            .read()
            .unwrap()
            .get(&(local_user.to_string(), server_name.to_string()))
            .cloned()
            .ok_or_else(|| {
                FdwError::UserMappingNotFound(format!("{}@{}", local_user, server_name))
            })?;

        let handler = self
            .handlers
            .read()
            .unwrap()
            .get(&server.wrapper_name)
            .cloned()
            .ok_or_else(|| FdwError::WrapperNotFound(server.wrapper_name.clone()))?;

        let conn = handler.connect(&server, &mapping)?;

        self.stats.write().unwrap().connection_opens += 1;

        Ok(conn)
    }

    /// Return connection to pool
    pub fn return_connection(&self, server_name: &str, conn: Box<dyn FdwConnection>) {
        if conn.is_valid() {
            let mut pool = self.connection_pool.write().unwrap();
            pool.entry(server_name.to_string())
                .or_insert_with(Vec::new)
                .push(conn);
        }
    }

    /// Import foreign schema
    pub fn import_foreign_schema(
        &self,
        server_name: &str,
        remote_schema: &str,
        local_schema: &str,
        local_user: &str,
        options: &HashMap<String, String>,
    ) -> Result<Vec<String>, FdwError> {
        let mut conn = self.get_connection(server_name, local_user)?;
        let tables = conn.import_schema(remote_schema, options)?;

        let mut created = Vec::new();
        for mut table in tables {
            table.name = format!("{}.{}", local_schema, table.name);
            let name = table.name.clone();
            self.create_foreign_table(table)?;
            created.push(name);
        }

        self.return_connection(server_name, conn);

        Ok(created)
    }

    /// Execute foreign query
    pub fn execute_query(
        &self,
        table_name: &str,
        predicates: Vec<FdwPredicate>,
        columns: Vec<String>,
        limit: Option<usize>,
        local_user: &str,
    ) -> Result<ForeignScanState, FdwError> {
        let table = self
            .foreign_tables
            .read()
            .unwrap()
            .get(table_name)
            .cloned()
            .ok_or_else(|| FdwError::TableNotFound(table_name.to_string()))?;

        let mut conn = self.get_connection(&table.server_name, local_user)?;

        // Build query with pushdown
        let query = self.build_pushdown_query(&table, &predicates, &columns, limit, &conn)?;

        let cursor = conn.execute_query(&query)?;

        self.stats.write().unwrap().total_queries += 1;

        Ok(ForeignScanState {
            table,
            connection: conn,
            cursor: Some(cursor),
            pushdown_predicates: predicates,
            projected_columns: columns,
            stats: ForeignScanStats::default(),
        })
    }

    fn build_pushdown_query(
        &self,
        table: &ForeignTable,
        predicates: &[FdwPredicate],
        columns: &[String],
        limit: Option<usize>,
        conn: &Box<dyn FdwConnection>,
    ) -> Result<String, FdwError> {
        let capabilities = conn.capabilities();

        let remote_table = table
            .options
            .get("table_name")
            .cloned()
            .unwrap_or_else(|| table.name.clone());

        // SELECT clause
        let select_clause = if columns.is_empty() || !capabilities.projection {
            "*".to_string()
        } else {
            columns.join(", ")
        };

        let mut query = format!("SELECT {} FROM {}", select_clause, remote_table);

        // WHERE clause
        if capabilities.filter && !predicates.is_empty() {
            let where_clause = self.predicates_to_sql(predicates)?;
            query.push_str(" WHERE ");
            query.push_str(&where_clause);
        }

        // LIMIT clause
        if capabilities.limit {
            if let Some(lim) = limit {
                query.push_str(&format!(" LIMIT {}", lim));
            }
        }

        Ok(query)
    }

    fn predicates_to_sql(&self, predicates: &[FdwPredicate]) -> Result<String, FdwError> {
        let parts: Vec<String> = predicates
            .iter()
            .map(|p| self.predicate_to_sql(p))
            .collect::<Result<_, _>>()?;
        Ok(parts.join(" AND "))
    }

    fn predicate_to_sql(&self, predicate: &FdwPredicate) -> Result<String, FdwError> {
        match predicate {
            FdwPredicate::Equals(col, val) => {
                Ok(format!("{} = {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::NotEquals(col, val) => {
                Ok(format!("{} <> {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::LessThan(col, val) => {
                Ok(format!("{} < {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::LessOrEqual(col, val) => {
                Ok(format!("{} <= {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::GreaterThan(col, val) => {
                Ok(format!("{} > {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::GreaterOrEqual(col, val) => {
                Ok(format!("{} >= {}", col, self.value_to_sql(val)?))
            }
            FdwPredicate::Between(col, lo, hi) => Ok(format!(
                "{} BETWEEN {} AND {}",
                col,
                self.value_to_sql(lo)?,
                self.value_to_sql(hi)?
            )),
            FdwPredicate::In(col, vals) => {
                let values: Vec<String> = vals
                    .iter()
                    .map(|v| self.value_to_sql(v))
                    .collect::<Result<_, _>>()?;
                Ok(format!("{} IN ({})", col, values.join(", ")))
            }
            FdwPredicate::Like(col, pattern) => Ok(format!("{} LIKE '{}'", col, pattern)),
            FdwPredicate::IsNull(col) => Ok(format!("{} IS NULL", col)),
            FdwPredicate::IsNotNull(col) => Ok(format!("{} IS NOT NULL", col)),
            FdwPredicate::And(left, right) => Ok(format!(
                "({}) AND ({})",
                self.predicate_to_sql(left)?,
                self.predicate_to_sql(right)?
            )),
            FdwPredicate::Or(left, right) => Ok(format!(
                "({}) OR ({})",
                self.predicate_to_sql(left)?,
                self.predicate_to_sql(right)?
            )),
            FdwPredicate::Not(inner) => {
                Ok(format!("NOT ({})", self.predicate_to_sql(inner)?))
            }
        }
    }

    fn value_to_sql(&self, value: &FdwValue) -> Result<String, FdwError> {
        match value {
            FdwValue::Null => Ok("NULL".to_string()),
            FdwValue::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
            FdwValue::Int64(i) => Ok(i.to_string()),
            FdwValue::Float64(f) => Ok(f.to_string()),
            FdwValue::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
            FdwValue::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                Ok(format!("X'{}'", hex))
            }
            FdwValue::Date(d) => Ok(format!("DATE '{}'", d)),
            FdwValue::Timestamp(ts) => Ok(format!("TIMESTAMP '{}'", ts)),
        }
    }

    /// Get foreign table
    pub fn get_foreign_table(&self, name: &str) -> Option<ForeignTable> {
        self.foreign_tables.read().unwrap().get(name).cloned()
    }

    /// List foreign tables
    pub fn list_foreign_tables(&self) -> Vec<ForeignTable> {
        self.foreign_tables.read().unwrap().values().cloned().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> FdwStats {
        self.stats.read().unwrap().clone()
    }
}

impl Default for FdwRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Built-in FDW Handlers
// ============================================================================

/// PostgreSQL FDW Handler
pub struct PostgresFdwHandler;

impl FdwHandler for PostgresFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let host = server
            .options
            .get("host")
            .cloned()
            .unwrap_or_else(|| "localhost".to_string());
        let port = server
            .options
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432u16);
        let dbname = server
            .options
            .get("dbname")
            .cloned()
            .unwrap_or_else(|| "postgres".to_string());

        let user = user_mapping
            .remote_user
            .clone()
            .or_else(|| user_mapping.options.get("user").cloned())
            .unwrap_or_else(|| "postgres".to_string());
        let password = user_mapping
            .options
            .get("password")
            .cloned()
            .unwrap_or_default();

        Ok(Box::new(PostgresConnection {
            host,
            port,
            database: dbname,
            user,
            password,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError> {
        if let Some(port) = options.get("port") {
            port.parse::<u16>()
                .map_err(|_| FdwError::InvalidOption("port must be a valid port number".into()))?;
        }
        Ok(())
    }

    fn validate_table_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["host"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec!["password"]
    }
}

/// PostgreSQL Connection (stub implementation)
struct PostgresConnection {
    host: String,
    port: u16,
    database: String,
    user: String,
    #[allow(dead_code)]
    password: String,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for PostgresConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "PostgreSQL".to_string(),
            server_version: "15.0".to_string(),
            connected_at: self.connected_at,
            remote_address: format!("{}:{}", self.host, self.port),
            database: Some(self.database.clone()),
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        // Would actually ping the server
        Ok(Duration::from_millis(1))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Ok(0)
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: true,
            sort: true,
            limit: true,
            aggregate: true,
            group_by: true,
            join: true,
            functions: vec!["count".into(), "sum".into(), "avg".into()],
            operators: vec!["=".into(), "<>".into(), "<".into(), ">".into()],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// MySQL FDW Handler
pub struct MySqlFdwHandler;

impl FdwHandler for MySqlFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let host = server
            .options
            .get("host")
            .cloned()
            .unwrap_or_else(|| "localhost".to_string());
        let port = server
            .options
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(3306u16);
        let database = server.options.get("database").cloned();

        let user = user_mapping
            .remote_user
            .clone()
            .unwrap_or_else(|| "root".to_string());
        let password = user_mapping
            .options
            .get("password")
            .cloned()
            .unwrap_or_default();

        Ok(Box::new(MySqlConnection {
            host,
            port,
            database,
            user,
            password,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn validate_table_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["host"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec!["password"]
    }
}

struct MySqlConnection {
    host: String,
    port: u16,
    database: Option<String>,
    user: String,
    #[allow(dead_code)]
    password: String,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for MySqlConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "MySQL".to_string(),
            server_version: "8.0".to_string(),
            connected_at: self.connected_at,
            remote_address: format!("{}:{}", self.host, self.port),
            database: self.database.clone(),
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        Ok(Duration::from_millis(1))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Ok(0)
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: true,
            sort: true,
            limit: true,
            aggregate: true,
            group_by: true,
            join: false, // Cross-server joins not supported
            functions: vec!["count".into(), "sum".into()],
            operators: vec!["=".into(), "<>".into()],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// MongoDB FDW Handler
pub struct MongoFdwHandler;

impl FdwHandler for MongoFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let uri = server
            .options
            .get("uri")
            .cloned()
            .unwrap_or_else(|| "mongodb://localhost:27017".to_string());
        let database = server
            .options
            .get("database")
            .cloned()
            .unwrap_or_else(|| "test".to_string());

        let user = user_mapping.remote_user.clone();
        let password = user_mapping.options.get("password").cloned();

        Ok(Box::new(MongoConnection {
            uri,
            database,
            user,
            password,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn validate_table_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["uri"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec![]
    }
}

struct MongoConnection {
    uri: String,
    database: String,
    #[allow(dead_code)]
    user: Option<String>,
    #[allow(dead_code)]
    password: Option<String>,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for MongoConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "MongoDB".to_string(),
            server_version: "6.0".to_string(),
            connected_at: self.connected_at,
            remote_address: self.uri.clone(),
            database: Some(self.database.clone()),
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        Ok(Duration::from_millis(1))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Ok(0)
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: true,
            sort: true,
            limit: true,
            aggregate: true,
            group_by: true,
            join: false,
            functions: vec!["count".into(), "sum".into()],
            operators: vec!["=".into(), "<>".into()],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// Redis FDW Handler
pub struct RedisFdwHandler;

impl FdwHandler for RedisFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let host = server
            .options
            .get("host")
            .cloned()
            .unwrap_or_else(|| "localhost".to_string());
        let port = server
            .options
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(6379u16);

        let password = user_mapping.options.get("password").cloned();

        Ok(Box::new(RedisConnection {
            host,
            port,
            password,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn validate_table_options(&self, _options: &HashMap<String, String>) -> Result<(), FdwError> {
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["host"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec![]
    }
}

struct RedisConnection {
    host: String,
    port: u16,
    #[allow(dead_code)]
    password: Option<String>,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for RedisConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "Redis".to_string(),
            server_version: "7.0".to_string(),
            connected_at: self.connected_at,
            remote_address: format!("{}:{}", self.host, self.port),
            database: None,
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        Ok(Duration::from_millis(1))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Ok(0)
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: false,
            sort: false,
            limit: true,
            aggregate: false,
            group_by: false,
            join: false,
            functions: vec![],
            operators: vec!["=".into()],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// File FDW Handler (CSV, Parquet, etc.)
pub struct FileFdwHandler;

impl FdwHandler for FileFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        _user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let base_path = server
            .options
            .get("base_path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        Ok(Box::new(FileConnection {
            base_path,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError> {
        if let Some(path) = options.get("base_path") {
            if !std::path::Path::new(path).exists() {
                return Err(FdwError::InvalidOption(format!(
                    "base_path '{}' does not exist",
                    path
                )));
            }
        }
        Ok(())
    }

    fn validate_table_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError> {
        if !options.contains_key("filename") {
            return Err(FdwError::InvalidOption(
                "filename option is required".into(),
            ));
        }
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["base_path"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec![]
    }
}

struct FileConnection {
    base_path: String,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for FileConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "File".to_string(),
            server_version: "1.0".to_string(),
            connected_at: self.connected_at,
            remote_address: self.base_path.clone(),
            database: None,
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        Ok(Duration::from_millis(0))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Err(FdwError::NotSupported("transactions".into()))
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Err(FdwError::NotSupported("modify operations".into()))
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: false,
            projection: true,
            sort: false,
            limit: false,
            aggregate: false,
            group_by: false,
            join: false,
            functions: vec![],
            operators: vec![],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// HTTP FDW Handler (REST APIs)
pub struct HttpFdwHandler;

impl FdwHandler for HttpFdwHandler {
    fn connect(
        &self,
        server: &ForeignServer,
        user_mapping: &UserMapping,
    ) -> Result<Box<dyn FdwConnection>, FdwError> {
        let base_url = server
            .options
            .get("base_url")
            .cloned()
            .ok_or_else(|| FdwError::InvalidOption("base_url is required".into()))?;

        let auth_header = user_mapping.options.get("auth_header").cloned();
        let api_key = user_mapping.options.get("api_key").cloned();

        Ok(Box::new(HttpConnection {
            base_url,
            auth_header,
            api_key,
            connected_at: Instant::now(),
            is_valid: true,
        }))
    }

    fn validate_server_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError> {
        if !options.contains_key("base_url") {
            return Err(FdwError::InvalidOption("base_url is required".into()));
        }
        Ok(())
    }

    fn validate_table_options(&self, options: &HashMap<String, String>) -> Result<(), FdwError> {
        if !options.contains_key("endpoint") {
            return Err(FdwError::InvalidOption("endpoint is required".into()));
        }
        Ok(())
    }

    fn required_server_options(&self) -> Vec<&'static str> {
        vec!["base_url"]
    }

    fn required_user_mapping_options(&self) -> Vec<&'static str> {
        vec![]
    }
}

struct HttpConnection {
    base_url: String,
    #[allow(dead_code)]
    auth_header: Option<String>,
    #[allow(dead_code)]
    api_key: Option<String>,
    connected_at: Instant,
    is_valid: bool,
}

impl FdwConnection for HttpConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            server_type: "HTTP".to_string(),
            server_version: "1.1".to_string(),
            connected_at: self.connected_at,
            remote_address: self.base_url.clone(),
            database: None,
        }
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn ping(&self) -> Result<Duration, FdwError> {
        Ok(Duration::from_millis(1))
    }

    fn close(&mut self) -> Result<(), FdwError> {
        self.is_valid = false;
        Ok(())
    }

    fn begin_transaction(&mut self) -> Result<(), FdwError> {
        Err(FdwError::NotSupported("transactions".into()))
    }

    fn commit(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), FdwError> {
        Ok(())
    }

    fn execute_query(&mut self, _query: &str) -> Result<Box<dyn FdwCursor>, FdwError> {
        Ok(Box::new(StubCursor::default()))
    }

    fn execute_modify(&mut self, _query: &str) -> Result<u64, FdwError> {
        Ok(0)
    }

    fn capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: true,
            sort: false,
            limit: true,
            aggregate: false,
            group_by: false,
            join: false,
            functions: vec![],
            operators: vec!["=".into()],
        }
    }

    fn import_schema(
        &self,
        _remote_schema: &str,
        _options: &HashMap<String, String>,
    ) -> Result<Vec<ForeignTable>, FdwError> {
        Ok(vec![])
    }

    fn get_stats(&self, _table: &str) -> Result<ForeignTableStats, FdwError> {
        Ok(ForeignTableStats::default())
    }
}

/// Stub cursor for testing
#[derive(Default)]
struct StubCursor {
    exhausted: bool,
}

impl FdwCursor for StubCursor {
    fn columns(&self) -> Vec<String> {
        vec![]
    }

    fn column_types(&self) -> Vec<ForeignDataType> {
        vec![]
    }

    fn fetch_batch(&mut self, _batch_size: usize) -> Result<Vec<FdwRow>, FdwError> {
        self.exhausted = true;
        Ok(vec![])
    }

    fn has_more(&self) -> bool {
        !self.exhausted
    }

    fn close(&mut self) -> Result<(), FdwError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fdw_registry() {
        let registry = FdwRegistry::new();

        // Create wrapper
        registry
            .create_wrapper(ForeignDataWrapper {
                name: "test_postgres".to_string(),
                handler: "postgres_fdw".to_string(),
                validator: None,
                options: HashMap::new(),
                wrapper_type: WrapperType::Postgres,
            })
            .unwrap();

        // Create server
        let mut server_opts = HashMap::new();
        server_opts.insert("host".to_string(), "localhost".to_string());
        server_opts.insert("port".to_string(), "5432".to_string());
        server_opts.insert("dbname".to_string(), "testdb".to_string());

        registry
            .create_server(ForeignServer {
                name: "pg_server".to_string(),
                wrapper_name: "test_postgres".to_string(),
                server_type: Some("postgresql".to_string()),
                version: Some("15".to_string()),
                options: server_opts,
            })
            .unwrap();

        // Create user mapping
        let mut mapping_opts = HashMap::new();
        mapping_opts.insert("password".to_string(), "secret".to_string());

        registry
            .create_user_mapping(UserMapping {
                local_user: "admin".to_string(),
                server_name: "pg_server".to_string(),
                remote_user: Some("postgres".to_string()),
                options: mapping_opts,
            })
            .unwrap();

        // Create foreign table
        registry
            .create_foreign_table(ForeignTable {
                name: "foreign_users".to_string(),
                server_name: "pg_server".to_string(),
                schema: Some("public".to_string()),
                columns: vec![
                    ForeignColumn {
                        name: "id".to_string(),
                        data_type: ForeignDataType::Int64,
                        nullable: false,
                        options: HashMap::new(),
                    },
                    ForeignColumn {
                        name: "name".to_string(),
                        data_type: ForeignDataType::String,
                        nullable: true,
                        options: HashMap::new(),
                    },
                ],
                options: HashMap::new(),
            })
            .unwrap();

        // List tables
        let tables = registry.list_foreign_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "foreign_users");
    }

    #[test]
    fn test_predicate_to_sql() {
        let registry = FdwRegistry::new();

        let pred = FdwPredicate::Equals("name".to_string(), FdwValue::String("test".to_string()));
        let sql = registry.predicate_to_sql(&pred).unwrap();
        assert_eq!(sql, "name = 'test'");

        let pred = FdwPredicate::Between(
            "age".to_string(),
            FdwValue::Int64(18),
            FdwValue::Int64(65),
        );
        let sql = registry.predicate_to_sql(&pred).unwrap();
        assert_eq!(sql, "age BETWEEN 18 AND 65");

        let pred = FdwPredicate::In(
            "status".to_string(),
            vec![
                FdwValue::String("active".to_string()),
                FdwValue::String("pending".to_string()),
            ],
        );
        let sql = registry.predicate_to_sql(&pred).unwrap();
        assert_eq!(sql, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_pushdown_capabilities() {
        let caps = PushdownCapabilities {
            filter: true,
            projection: true,
            sort: true,
            limit: true,
            aggregate: false,
            group_by: false,
            join: false,
            functions: vec!["count".into()],
            operators: vec!["=".into(), "<".into()],
        };

        assert!(caps.filter);
        assert!(!caps.aggregate);
        assert!(caps.functions.contains(&"count".to_string()));
    }
}
