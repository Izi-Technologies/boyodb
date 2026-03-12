//! Cross-Database Queries
//!
//! Enables querying across multiple databases with:
//! - db.schema.table syntax
//! - Database links
//! - Federated query optimization

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// Cross-database error types
#[derive(Debug, Clone)]
pub enum CrossDbError {
    /// Database not found
    DatabaseNotFound(String),
    /// Database link not found
    LinkNotFound(String),
    /// Permission denied
    PermissionDenied(String),
    /// Connection failed
    ConnectionFailed(String),
    /// Query execution failed
    QueryFailed(String),
    /// Distributed transaction error
    DistributedTxError(String),
    /// Invalid reference
    InvalidReference(String),
}

impl std::fmt::Display for CrossDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseNotFound(s) => write!(f, "database not found: {}", s),
            Self::LinkNotFound(s) => write!(f, "database link not found: {}", s),
            Self::PermissionDenied(s) => write!(f, "permission denied: {}", s),
            Self::ConnectionFailed(s) => write!(f, "connection failed: {}", s),
            Self::QueryFailed(s) => write!(f, "query failed: {}", s),
            Self::DistributedTxError(s) => write!(f, "distributed transaction error: {}", s),
            Self::InvalidReference(s) => write!(f, "invalid reference: {}", s),
        }
    }
}

impl std::error::Error for CrossDbError {}

/// Fully qualified table reference
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableReference {
    /// Database name (or link name with @)
    pub database: Option<String>,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Optional alias
    pub alias: Option<String>,
}

impl TableReference {
    pub fn new(table: &str) -> Self {
        Self {
            database: None,
            schema: None,
            table: table.to_string(),
            alias: None,
        }
    }

    pub fn with_schema(schema: &str, table: &str) -> Self {
        Self {
            database: None,
            schema: Some(schema.to_string()),
            table: table.to_string(),
            alias: None,
        }
    }

    pub fn with_database(database: &str, schema: &str, table: &str) -> Self {
        Self {
            database: Some(database.to_string()),
            schema: Some(schema.to_string()),
            table: table.to_string(),
            alias: None,
        }
    }

    /// Parse from string like "db.schema.table" or "table@link"
    pub fn parse(s: &str) -> Result<Self, CrossDbError> {
        // Check for database link syntax: table@link
        if let Some(at_pos) = s.find('@') {
            let table_part = &s[..at_pos];
            let link_name = &s[at_pos + 1..];

            let (schema, table) = if let Some(dot_pos) = table_part.rfind('.') {
                (Some(table_part[..dot_pos].to_string()), table_part[dot_pos + 1..].to_string())
            } else {
                (None, table_part.to_string())
            };

            return Ok(Self {
                database: Some(format!("@{}", link_name)),
                schema,
                table,
                alias: None,
            });
        }

        // Parse db.schema.table syntax
        let parts: Vec<&str> = s.split('.').collect();
        match parts.len() {
            1 => Ok(Self::new(parts[0])),
            2 => Ok(Self::with_schema(parts[0], parts[1])),
            3 => Ok(Self::with_database(parts[0], parts[1], parts[2])),
            _ => Err(CrossDbError::InvalidReference(s.to_string())),
        }
    }

    /// Check if this is a cross-database reference
    pub fn is_cross_database(&self) -> bool {
        self.database.is_some()
    }

    /// Check if this uses a database link
    pub fn uses_link(&self) -> bool {
        self.database
            .as_ref()
            .map(|d| d.starts_with('@'))
            .unwrap_or(false)
    }

    /// Get link name if using database link
    pub fn link_name(&self) -> Option<&str> {
        self.database
            .as_ref()
            .filter(|d| d.starts_with('@'))
            .map(|d| &d[1..])
    }

    /// Get display name
    pub fn display_name(&self) -> String {
        let mut parts = Vec::new();
        if let Some(db) = &self.database {
            parts.push(db.clone());
        }
        if let Some(schema) = &self.schema {
            parts.push(schema.clone());
        }
        parts.push(self.table.clone());
        parts.join(".")
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Database link definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseLink {
    pub name: String,
    pub connection_string: String,
    pub link_type: LinkType,
    pub owner: String,
    pub is_public: bool,
    pub options: LinkOptions,
    pub created_at: SystemTime,
}

/// Type of database link
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkType {
    /// Same database system (BoyoDB to BoyoDB)
    Internal,
    /// PostgreSQL
    Postgres,
    /// MySQL
    MySQL,
    /// ODBC connection
    Odbc,
    /// HTTP/REST
    Http,
}

/// Database link options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkOptions {
    /// Connection timeout
    pub connect_timeout: Option<Duration>,
    /// Query timeout
    pub query_timeout: Option<Duration>,
    /// Fetch size for cursors
    pub fetch_size: Option<usize>,
    /// Use connection pooling
    pub pooling: bool,
    /// Pool size
    pub pool_size: Option<usize>,
    /// SSL mode
    pub ssl_mode: Option<String>,
    /// Custom options
    pub custom: HashMap<String, String>,
}

/// Cross-database query context
#[derive(Debug, Clone)]
pub struct CrossDbContext {
    /// Current database
    pub current_database: String,
    /// Current schema
    pub current_schema: String,
    /// Current user
    pub current_user: String,
    /// Transaction ID (for distributed transactions)
    pub transaction_id: Option<String>,
    /// Query timeout
    pub timeout: Option<Duration>,
}

/// Resolved table location
#[derive(Debug, Clone)]
pub enum TableLocation {
    /// Local table in current database
    Local {
        schema: String,
        table: String,
    },
    /// Table in another local database
    OtherDatabase {
        database: String,
        schema: String,
        table: String,
    },
    /// Remote table via database link
    Remote {
        link_name: String,
        schema: Option<String>,
        table: String,
    },
}

/// Query fragment that can be pushed to remote
#[derive(Debug, Clone)]
pub struct RemoteQuery {
    pub link_name: String,
    pub sql: String,
    pub params: Vec<RemoteParam>,
    pub columns: Vec<RemoteColumn>,
}

/// Parameter for remote query
#[derive(Debug, Clone)]
pub struct RemoteParam {
    pub name: String,
    pub value: RemoteValue,
}

/// Value for remote parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// Column from remote query
#[derive(Debug, Clone)]
pub struct RemoteColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Cross-database query planner
pub struct CrossDbPlanner {
    /// Database catalog
    catalog: Arc<DatabaseCatalog>,
    /// Configuration
    config: CrossDbConfig,
}

/// Database catalog
pub struct DatabaseCatalog {
    /// Local databases
    databases: RwLock<HashMap<String, DatabaseInfo>>,
    /// Database links
    links: RwLock<HashMap<String, DatabaseLink>>,
    /// Connection pool
    connections: RwLock<HashMap<String, Vec<RemoteConnection>>>,
}

/// Database information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub owner: String,
    pub encoding: String,
    pub collation: String,
    pub schemas: Vec<String>,
    pub created_at: SystemTime,
}

/// Remote connection handle
pub struct RemoteConnection {
    pub link_name: String,
    pub connected_at: Instant,
    pub in_use: bool,
}

/// Cross-database configuration
#[derive(Debug, Clone)]
pub struct CrossDbConfig {
    /// Allow cross-database queries
    pub enabled: bool,
    /// Maximum remote connections per link
    pub max_connections_per_link: usize,
    /// Default query timeout for remote queries
    pub remote_query_timeout: Duration,
    /// Enable distributed transactions
    pub distributed_transactions: bool,
    /// Maximum pushdown complexity
    pub max_pushdown_complexity: usize,
}

impl Default for CrossDbConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_connections_per_link: 10,
            remote_query_timeout: Duration::from_secs(60),
            distributed_transactions: true,
            max_pushdown_complexity: 100,
        }
    }
}

impl DatabaseCatalog {
    pub fn new() -> Self {
        Self {
            databases: RwLock::new(HashMap::new()),
            links: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// Register a database
    pub fn register_database(&self, info: DatabaseInfo) {
        self.databases
            .write()
            .unwrap()
            .insert(info.name.clone(), info);
    }

    /// Get database info
    pub fn get_database(&self, name: &str) -> Option<DatabaseInfo> {
        self.databases.read().unwrap().get(name).cloned()
    }

    /// List all databases
    pub fn list_databases(&self) -> Vec<DatabaseInfo> {
        self.databases.read().unwrap().values().cloned().collect()
    }

    /// Create database link
    pub fn create_link(&self, link: DatabaseLink) -> Result<(), CrossDbError> {
        let mut links = self.links.write().unwrap();
        if links.contains_key(&link.name) {
            return Err(CrossDbError::InvalidReference(format!(
                "link '{}' already exists",
                link.name
            )));
        }
        links.insert(link.name.clone(), link);
        Ok(())
    }

    /// Drop database link
    pub fn drop_link(&self, name: &str) -> Result<DatabaseLink, CrossDbError> {
        self.links
            .write()
            .unwrap()
            .remove(name)
            .ok_or_else(|| CrossDbError::LinkNotFound(name.to_string()))
    }

    /// Get database link
    pub fn get_link(&self, name: &str) -> Option<DatabaseLink> {
        self.links.read().unwrap().get(name).cloned()
    }

    /// List all links
    pub fn list_links(&self) -> Vec<DatabaseLink> {
        self.links.read().unwrap().values().cloned().collect()
    }

    /// Get or create remote connection
    pub fn get_connection(&self, link_name: &str) -> Result<RemoteConnection, CrossDbError> {
        let link = self
            .get_link(link_name)
            .ok_or_else(|| CrossDbError::LinkNotFound(link_name.to_string()))?;

        // Try to get from pool
        {
            let mut conns = self.connections.write().unwrap();
            if let Some(pool) = conns.get_mut(link_name) {
                if let Some(conn) = pool.iter_mut().find(|c| !c.in_use) {
                    conn.in_use = true;
                    return Ok(RemoteConnection {
                        link_name: link_name.to_string(),
                        connected_at: conn.connected_at,
                        in_use: true,
                    });
                }
            }
        }

        // Create new connection
        let conn = self.create_connection(&link)?;

        // Add to pool
        let mut conns = self.connections.write().unwrap();
        conns
            .entry(link_name.to_string())
            .or_insert_with(Vec::new)
            .push(RemoteConnection {
                link_name: conn.link_name.clone(),
                connected_at: conn.connected_at,
                in_use: true,
            });

        Ok(conn)
    }

    fn create_connection(&self, _link: &DatabaseLink) -> Result<RemoteConnection, CrossDbError> {
        // Would actually connect to remote database
        Ok(RemoteConnection {
            link_name: _link.name.clone(),
            connected_at: Instant::now(),
            in_use: true,
        })
    }

    /// Return connection to pool
    pub fn return_connection(&self, link_name: &str) {
        let mut conns = self.connections.write().unwrap();
        if let Some(pool) = conns.get_mut(link_name) {
            if let Some(conn) = pool.iter_mut().find(|c| c.in_use) {
                conn.in_use = false;
            }
        }
    }
}

impl Default for DatabaseCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CrossDbPlanner {
    pub fn new(catalog: Arc<DatabaseCatalog>, config: CrossDbConfig) -> Self {
        Self { catalog, config }
    }

    /// Resolve table reference to location
    pub fn resolve_table(
        &self,
        reference: &TableReference,
        context: &CrossDbContext,
    ) -> Result<TableLocation, CrossDbError> {
        // Check if using database link
        if reference.uses_link() {
            let link_name = reference.link_name().unwrap();
            if self.catalog.get_link(link_name).is_none() {
                return Err(CrossDbError::LinkNotFound(link_name.to_string()));
            }
            return Ok(TableLocation::Remote {
                link_name: link_name.to_string(),
                schema: reference.schema.clone(),
                table: reference.table.clone(),
            });
        }

        // Check database name
        if let Some(db) = &reference.database {
            if db == &context.current_database {
                // Same database
                return Ok(TableLocation::Local {
                    schema: reference
                        .schema
                        .clone()
                        .unwrap_or_else(|| context.current_schema.clone()),
                    table: reference.table.clone(),
                });
            }

            // Check if database exists
            if self.catalog.get_database(db).is_none() {
                return Err(CrossDbError::DatabaseNotFound(db.clone()));
            }

            return Ok(TableLocation::OtherDatabase {
                database: db.clone(),
                schema: reference
                    .schema
                    .clone()
                    .unwrap_or_else(|| "public".to_string()),
                table: reference.table.clone(),
            });
        }

        // Local table
        Ok(TableLocation::Local {
            schema: reference
                .schema
                .clone()
                .unwrap_or_else(|| context.current_schema.clone()),
            table: reference.table.clone(),
        })
    }

    /// Check if query involves multiple databases
    pub fn is_cross_database_query(&self, references: &[TableReference]) -> bool {
        let databases: std::collections::HashSet<_> = references
            .iter()
            .filter_map(|r| r.database.as_ref())
            .collect();
        databases.len() > 1 || references.iter().any(|r| r.uses_link())
    }

    /// Plan cross-database query
    pub fn plan_cross_db_query(
        &self,
        references: &[TableReference],
        context: &CrossDbContext,
    ) -> Result<CrossDbPlan, CrossDbError> {
        if !self.config.enabled {
            return Err(CrossDbError::PermissionDenied(
                "cross-database queries disabled".into(),
            ));
        }

        let mut local_tables = Vec::new();
        let mut remote_tables = Vec::new();
        let mut other_db_tables = Vec::new();

        for reference in references {
            match self.resolve_table(reference, context)? {
                TableLocation::Local { schema, table } => {
                    local_tables.push((schema, table, reference.alias.clone()));
                }
                TableLocation::OtherDatabase {
                    database,
                    schema,
                    table,
                } => {
                    other_db_tables.push((database, schema, table, reference.alias.clone()));
                }
                TableLocation::Remote {
                    link_name,
                    schema,
                    table,
                } => {
                    remote_tables.push((link_name, schema, table, reference.alias.clone()));
                }
            }
        }

        let requires_distributed_tx = self.config.distributed_transactions
            && (!other_db_tables.is_empty() || !remote_tables.is_empty());

        Ok(CrossDbPlan {
            local_tables,
            other_db_tables,
            remote_tables,
            requires_distributed_tx,
        })
    }

    /// Generate remote query with pushdown
    pub fn generate_remote_query(
        &self,
        link_name: &str,
        tables: &[(Option<String>, String)],
        columns: &[String],
        predicates: &[String],
        limit: Option<usize>,
    ) -> Result<RemoteQuery, CrossDbError> {
        let link = self
            .catalog
            .get_link(link_name)
            .ok_or_else(|| CrossDbError::LinkNotFound(link_name.to_string()))?;

        // Build SQL based on link type
        let sql = match link.link_type {
            LinkType::Postgres | LinkType::MySQL | LinkType::Internal => {
                let cols = if columns.is_empty() {
                    "*".to_string()
                } else {
                    columns.join(", ")
                };

                let from_clause: Vec<String> = tables
                    .iter()
                    .map(|(schema, table)| {
                        if let Some(s) = schema {
                            format!("{}.{}", s, table)
                        } else {
                            table.clone()
                        }
                    })
                    .collect();

                let mut sql = format!("SELECT {} FROM {}", cols, from_clause.join(", "));

                if !predicates.is_empty() {
                    sql.push_str(" WHERE ");
                    sql.push_str(&predicates.join(" AND "));
                }

                if let Some(lim) = limit {
                    sql.push_str(&format!(" LIMIT {}", lim));
                }

                sql
            }
            LinkType::Http => {
                // REST API query format
                format!(
                    "GET /query?tables={}&columns={}&filter={}",
                    tables
                        .iter()
                        .map(|(_, t)| t.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                    columns.join(","),
                    predicates.join(",")
                )
            }
            LinkType::Odbc => {
                // Standard SQL
                let cols = if columns.is_empty() {
                    "*".to_string()
                } else {
                    columns.join(", ")
                };

                let from_clause: Vec<String> = tables
                    .iter()
                    .map(|(schema, table)| {
                        if let Some(s) = schema {
                            format!("{}.{}", s, table)
                        } else {
                            table.clone()
                        }
                    })
                    .collect();

                let mut sql = format!("SELECT {} FROM {}", cols, from_clause.join(", "));

                if !predicates.is_empty() {
                    sql.push_str(" WHERE ");
                    sql.push_str(&predicates.join(" AND "));
                }

                sql
            }
        };

        Ok(RemoteQuery {
            link_name: link_name.to_string(),
            sql,
            params: vec![],
            columns: columns
                .iter()
                .map(|c| RemoteColumn {
                    name: c.clone(),
                    data_type: "unknown".to_string(),
                    nullable: true,
                })
                .collect(),
        })
    }
}

/// Cross-database query plan
#[derive(Debug, Clone)]
pub struct CrossDbPlan {
    /// Tables from current database
    pub local_tables: Vec<(String, String, Option<String>)>, // (schema, table, alias)
    /// Tables from other local databases
    pub other_db_tables: Vec<(String, String, String, Option<String>)>, // (db, schema, table, alias)
    /// Tables from remote databases via links
    pub remote_tables: Vec<(String, Option<String>, String, Option<String>)>, // (link, schema, table, alias)
    /// Whether distributed transaction is required
    pub requires_distributed_tx: bool,
}

impl CrossDbPlan {
    /// Check if this is a purely local query
    pub fn is_local_only(&self) -> bool {
        self.other_db_tables.is_empty() && self.remote_tables.is_empty()
    }

    /// Get all involved databases
    pub fn involved_databases(&self) -> Vec<String> {
        let mut dbs = Vec::new();
        for (db, _, _, _) in &self.other_db_tables {
            if !dbs.contains(db) {
                dbs.push(db.clone());
            }
        }
        dbs
    }

    /// Get all involved links
    pub fn involved_links(&self) -> Vec<String> {
        let mut links = Vec::new();
        for (link, _, _, _) in &self.remote_tables {
            if !links.contains(link) {
                links.push(link.clone());
            }
        }
        links
    }
}

/// Cross-database query executor
pub struct CrossDbExecutor {
    catalog: Arc<DatabaseCatalog>,
    config: CrossDbConfig,
}

impl CrossDbExecutor {
    pub fn new(catalog: Arc<DatabaseCatalog>, config: CrossDbConfig) -> Self {
        Self { catalog, config }
    }

    /// Execute remote query
    pub fn execute_remote(
        &self,
        query: &RemoteQuery,
    ) -> Result<RemoteResult, CrossDbError> {
        let _conn = self.catalog.get_connection(&query.link_name)?;

        // Would execute query on remote database
        // For now, return empty result

        self.catalog.return_connection(&query.link_name);

        Ok(RemoteResult {
            columns: query.columns.clone(),
            rows: vec![],
            rows_affected: 0,
            execution_time: Duration::from_millis(10),
        })
    }

    /// Execute cross-database query
    pub fn execute_cross_db(
        &self,
        plan: &CrossDbPlan,
        _context: &CrossDbContext,
    ) -> Result<CrossDbResult, CrossDbError> {
        if plan.requires_distributed_tx && !self.config.distributed_transactions {
            return Err(CrossDbError::DistributedTxError(
                "distributed transactions disabled".into(),
            ));
        }

        // Would actually execute the query across databases
        // Using 2PC if distributed transactions are enabled

        Ok(CrossDbResult {
            rows: vec![],
            execution_time: Duration::from_millis(50),
            databases_involved: plan.involved_databases(),
            links_involved: plan.involved_links(),
        })
    }
}

/// Result from remote query
#[derive(Debug, Clone)]
pub struct RemoteResult {
    pub columns: Vec<RemoteColumn>,
    pub rows: Vec<Vec<RemoteValue>>,
    pub rows_affected: u64,
    pub execution_time: Duration,
}

/// Result from cross-database query
#[derive(Debug, Clone)]
pub struct CrossDbResult {
    pub rows: Vec<Vec<RemoteValue>>,
    pub execution_time: Duration,
    pub databases_involved: Vec<String>,
    pub links_involved: Vec<String>,
}

/// Cross-database query manager
pub struct CrossDbManager {
    pub catalog: Arc<DatabaseCatalog>,
    pub planner: CrossDbPlanner,
    pub executor: CrossDbExecutor,
    stats: RwLock<CrossDbStats>,
}

/// Cross-database statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrossDbStats {
    pub total_queries: u64,
    pub local_queries: u64,
    pub cross_db_queries: u64,
    pub remote_queries: u64,
    pub distributed_tx_count: u64,
    pub total_rows_fetched: u64,
    pub total_execution_time_ms: u64,
}

impl CrossDbManager {
    pub fn new(config: CrossDbConfig) -> Self {
        let catalog = Arc::new(DatabaseCatalog::new());
        let planner = CrossDbPlanner::new(catalog.clone(), config.clone());
        let executor = CrossDbExecutor::new(catalog.clone(), config);

        Self {
            catalog,
            planner,
            executor,
            stats: RwLock::new(CrossDbStats::default()),
        }
    }

    /// Create a database link
    pub fn create_database_link(
        &self,
        name: &str,
        connection_string: &str,
        link_type: LinkType,
        owner: &str,
        is_public: bool,
    ) -> Result<(), CrossDbError> {
        self.catalog.create_link(DatabaseLink {
            name: name.to_string(),
            connection_string: connection_string.to_string(),
            link_type,
            owner: owner.to_string(),
            is_public,
            options: LinkOptions::default(),
            created_at: SystemTime::now(),
        })
    }

    /// Drop a database link
    pub fn drop_database_link(&self, name: &str) -> Result<(), CrossDbError> {
        self.catalog.drop_link(name)?;
        Ok(())
    }

    /// Execute query with cross-database support
    pub fn execute_query(
        &self,
        references: &[TableReference],
        context: &CrossDbContext,
    ) -> Result<CrossDbResult, CrossDbError> {
        let plan = self.planner.plan_cross_db_query(references, context)?;

        let mut stats = self.stats.write().unwrap();
        stats.total_queries += 1;

        if plan.is_local_only() {
            stats.local_queries += 1;
        } else {
            stats.cross_db_queries += 1;
        }

        if plan.requires_distributed_tx {
            stats.distributed_tx_count += 1;
        }

        drop(stats);

        let result = self.executor.execute_cross_db(&plan, context)?;

        let mut stats = self.stats.write().unwrap();
        stats.total_execution_time_ms += result.execution_time.as_millis() as u64;
        stats.total_rows_fetched += result.rows.len() as u64;

        Ok(result)
    }

    /// Get statistics
    pub fn stats(&self) -> CrossDbStats {
        self.stats.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_reference_parse() {
        // Simple table
        let ref1 = TableReference::parse("users").unwrap();
        assert_eq!(ref1.table, "users");
        assert!(ref1.schema.is_none());
        assert!(ref1.database.is_none());

        // Schema.table
        let ref2 = TableReference::parse("public.users").unwrap();
        assert_eq!(ref2.table, "users");
        assert_eq!(ref2.schema.as_deref(), Some("public"));
        assert!(ref2.database.is_none());

        // Database.schema.table
        let ref3 = TableReference::parse("mydb.public.users").unwrap();
        assert_eq!(ref3.table, "users");
        assert_eq!(ref3.schema.as_deref(), Some("public"));
        assert_eq!(ref3.database.as_deref(), Some("mydb"));

        // Database link syntax
        let ref4 = TableReference::parse("users@remote_db").unwrap();
        assert_eq!(ref4.table, "users");
        assert_eq!(ref4.database.as_deref(), Some("@remote_db"));
        assert!(ref4.uses_link());
        assert_eq!(ref4.link_name(), Some("remote_db"));

        // Schema.table@link
        let ref5 = TableReference::parse("public.orders@sales_db").unwrap();
        assert_eq!(ref5.table, "orders");
        assert_eq!(ref5.schema.as_deref(), Some("public"));
        assert!(ref5.uses_link());
    }

    #[test]
    fn test_database_catalog() {
        let catalog = DatabaseCatalog::new();

        // Register database
        catalog.register_database(DatabaseInfo {
            name: "testdb".to_string(),
            owner: "admin".to_string(),
            encoding: "UTF8".to_string(),
            collation: "en_US.UTF-8".to_string(),
            schemas: vec!["public".to_string()],
            created_at: SystemTime::now(),
        });

        assert!(catalog.get_database("testdb").is_some());
        assert!(catalog.get_database("nonexistent").is_none());

        // Create link
        catalog
            .create_link(DatabaseLink {
                name: "remote".to_string(),
                connection_string: "host=remote.example.com".to_string(),
                link_type: LinkType::Postgres,
                owner: "admin".to_string(),
                is_public: true,
                options: LinkOptions::default(),
                created_at: SystemTime::now(),
            })
            .unwrap();

        assert!(catalog.get_link("remote").is_some());
    }

    #[test]
    fn test_cross_db_planner() {
        let catalog = Arc::new(DatabaseCatalog::new());

        catalog.register_database(DatabaseInfo {
            name: "main".to_string(),
            owner: "admin".to_string(),
            encoding: "UTF8".to_string(),
            collation: "en_US.UTF-8".to_string(),
            schemas: vec!["public".to_string()],
            created_at: SystemTime::now(),
        });

        catalog.register_database(DatabaseInfo {
            name: "analytics".to_string(),
            owner: "admin".to_string(),
            encoding: "UTF8".to_string(),
            collation: "en_US.UTF-8".to_string(),
            schemas: vec!["reports".to_string()],
            created_at: SystemTime::now(),
        });

        let planner = CrossDbPlanner::new(catalog, CrossDbConfig::default());

        let context = CrossDbContext {
            current_database: "main".to_string(),
            current_schema: "public".to_string(),
            current_user: "user1".to_string(),
            transaction_id: None,
            timeout: None,
        };

        // Local table
        let ref1 = TableReference::parse("users").unwrap();
        let location1 = planner.resolve_table(&ref1, &context).unwrap();
        assert!(matches!(location1, TableLocation::Local { .. }));

        // Cross-database table
        let ref2 = TableReference::parse("analytics.reports.sales").unwrap();
        let location2 = planner.resolve_table(&ref2, &context).unwrap();
        assert!(matches!(location2, TableLocation::OtherDatabase { .. }));
    }

    #[test]
    fn test_cross_db_plan() {
        let catalog = Arc::new(DatabaseCatalog::new());

        catalog.register_database(DatabaseInfo {
            name: "db1".to_string(),
            owner: "admin".to_string(),
            encoding: "UTF8".to_string(),
            collation: "en_US.UTF-8".to_string(),
            schemas: vec!["public".to_string()],
            created_at: SystemTime::now(),
        });

        catalog.register_database(DatabaseInfo {
            name: "db2".to_string(),
            owner: "admin".to_string(),
            encoding: "UTF8".to_string(),
            collation: "en_US.UTF-8".to_string(),
            schemas: vec!["public".to_string()],
            created_at: SystemTime::now(),
        });

        let planner = CrossDbPlanner::new(catalog, CrossDbConfig::default());

        let context = CrossDbContext {
            current_database: "db1".to_string(),
            current_schema: "public".to_string(),
            current_user: "user1".to_string(),
            transaction_id: None,
            timeout: None,
        };

        let refs = vec![
            TableReference::parse("users").unwrap(),
            TableReference::parse("db2.public.orders").unwrap(),
        ];

        let plan = planner.plan_cross_db_query(&refs, &context).unwrap();
        assert_eq!(plan.local_tables.len(), 1);
        assert_eq!(plan.other_db_tables.len(), 1);
        assert!(!plan.is_local_only());
        assert!(plan.requires_distributed_tx);
    }
}
