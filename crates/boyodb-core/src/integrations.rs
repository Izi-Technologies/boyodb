//! Data Platform Integrations
//!
//! Provides integration interfaces for popular data platform tools including
//! Apache Spark, Apache Flink, dbt, Apache Airflow, and Presto/Trino.
//!
//! ## Features
//!
//! - Spark DataSource V2 connector specification
//! - Flink Table Source/Sink interfaces
//! - dbt adapter protocol
//! - Airflow operator definitions
//! - Presto/Trino connector specification

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Common Types
// ============================================================================

/// Connection configuration for integrations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Host address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub username: Option<String>,
    /// Password (should be handled securely)
    pub password: Option<String>,
    /// Use SSL/TLS
    pub ssl: bool,
    /// SSL certificate path
    pub ssl_cert: Option<String>,
    /// Connection timeout in seconds
    pub timeout_secs: u64,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8765,
            database: "default".to_string(),
            username: None,
            password: None,
            ssl: false,
            ssl_cert: None,
            timeout_secs: 30,
            properties: HashMap::new(),
        }
    }
}

/// Data type mapping between systems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeMapping {
    pub boyodb_type: String,
    pub external_type: String,
    pub nullable: bool,
}

// ============================================================================
// Apache Spark Integration
// ============================================================================

/// Spark DataSource V2 connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkConnectorConfig {
    /// Base connection config
    pub connection: ConnectionConfig,
    /// Batch size for reads
    pub batch_size: usize,
    /// Number of partitions for parallel reads
    pub num_partitions: usize,
    /// Push down predicates to BoyoDB
    pub predicate_pushdown: bool,
    /// Push down projections (column pruning)
    pub projection_pushdown: bool,
    /// Push down aggregations
    pub aggregate_pushdown: bool,
    /// Enable write mode (batch/streaming)
    pub write_mode: SparkWriteMode,
    /// Checkpoint location for streaming
    pub checkpoint_location: Option<String>,
}

/// Spark write mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SparkWriteMode {
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore,
}

impl Default for SparkConnectorConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            batch_size: 10000,
            num_partitions: 4,
            predicate_pushdown: true,
            projection_pushdown: true,
            aggregate_pushdown: true,
            write_mode: SparkWriteMode::Append,
            checkpoint_location: None,
        }
    }
}

/// Spark read partition information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkPartition {
    pub partition_id: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub preferred_locations: Vec<String>,
}

/// Spark schema representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkSchema {
    pub fields: Vec<SparkField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparkField {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

/// Spark connector interface
pub struct SparkConnector {
    config: SparkConnectorConfig,
    /// Type mappings
    type_mappings: Vec<TypeMapping>,
}

impl SparkConnector {
    pub fn new(config: SparkConnectorConfig) -> Self {
        Self {
            config,
            type_mappings: Self::default_type_mappings(),
        }
    }

    fn default_type_mappings() -> Vec<TypeMapping> {
        vec![
            TypeMapping { boyodb_type: "INT8".into(), external_type: "ByteType".into(), nullable: true },
            TypeMapping { boyodb_type: "INT16".into(), external_type: "ShortType".into(), nullable: true },
            TypeMapping { boyodb_type: "INT32".into(), external_type: "IntegerType".into(), nullable: true },
            TypeMapping { boyodb_type: "INT64".into(), external_type: "LongType".into(), nullable: true },
            TypeMapping { boyodb_type: "FLOAT32".into(), external_type: "FloatType".into(), nullable: true },
            TypeMapping { boyodb_type: "FLOAT64".into(), external_type: "DoubleType".into(), nullable: true },
            TypeMapping { boyodb_type: "VARCHAR".into(), external_type: "StringType".into(), nullable: true },
            TypeMapping { boyodb_type: "BOOLEAN".into(), external_type: "BooleanType".into(), nullable: true },
            TypeMapping { boyodb_type: "DATE".into(), external_type: "DateType".into(), nullable: true },
            TypeMapping { boyodb_type: "TIMESTAMP".into(), external_type: "TimestampType".into(), nullable: true },
            TypeMapping { boyodb_type: "BINARY".into(), external_type: "BinaryType".into(), nullable: true },
        ]
    }

    /// Get partitions for a table read
    pub fn get_partitions(&self, table: &str, num_partitions: Option<usize>) -> Vec<SparkPartition> {
        let n = num_partitions.unwrap_or(self.config.num_partitions);
        (0..n).map(|i| SparkPartition {
            partition_id: i as i32,
            start_offset: 0,
            end_offset: -1, // Unknown
            preferred_locations: vec![],
        }).collect()
    }

    /// Generate JDBC-compatible connection URL
    pub fn jdbc_url(&self) -> String {
        let c = &self.config.connection;
        format!(
            "jdbc:boyodb://{}:{}/{}",
            c.host, c.port, c.database
        )
    }

    /// Generate Spark DataSource options
    pub fn datasource_options(&self, table: &str) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        opts.insert("url".to_string(), self.jdbc_url());
        opts.insert("dbtable".to_string(), table.to_string());
        opts.insert("batchsize".to_string(), self.config.batch_size.to_string());
        opts.insert("numPartitions".to_string(), self.config.num_partitions.to_string());
        opts.insert("pushDownPredicate".to_string(), self.config.predicate_pushdown.to_string());
        opts.insert("pushDownAggregate".to_string(), self.config.aggregate_pushdown.to_string());

        if let Some(ref user) = self.config.connection.username {
            opts.insert("user".to_string(), user.clone());
        }

        opts
    }
}

// ============================================================================
// Apache Flink Integration
// ============================================================================

/// Flink connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkConnectorConfig {
    /// Base connection config
    pub connection: ConnectionConfig,
    /// Parallelism for source/sink
    pub parallelism: usize,
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Flush interval for streaming writes (ms)
    pub flush_interval_ms: u64,
    /// Maximum buffered rows before flush
    pub buffer_size: usize,
    /// Enable changelog mode for CDC
    pub changelog_mode: bool,
    /// Lookup cache configuration
    pub lookup_cache: Option<FlinkLookupCache>,
}

/// Flink lookup cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkLookupCache {
    pub max_rows: usize,
    pub ttl_seconds: u64,
}

impl Default for FlinkConnectorConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            parallelism: 4,
            batch_size: 1000,
            flush_interval_ms: 1000,
            buffer_size: 10000,
            changelog_mode: false,
            lookup_cache: None,
        }
    }
}

/// Flink table DDL generator
pub struct FlinkDdlGenerator {
    config: FlinkConnectorConfig,
}

impl FlinkDdlGenerator {
    pub fn new(config: FlinkConnectorConfig) -> Self {
        Self { config }
    }

    /// Generate CREATE TABLE DDL for Flink
    pub fn create_table_ddl(
        &self,
        table_name: &str,
        columns: &[(String, String)],
        primary_key: Option<&[String]>,
    ) -> String {
        let mut ddl = format!("CREATE TABLE {} (\n", table_name);

        for (i, (name, dtype)) in columns.iter().enumerate() {
            ddl.push_str(&format!("  {} {}", name, self.map_type(dtype)));
            if i < columns.len() - 1 || primary_key.is_some() {
                ddl.push(',');
            }
            ddl.push('\n');
        }

        if let Some(pk) = primary_key {
            ddl.push_str(&format!("  PRIMARY KEY ({}) NOT ENFORCED\n", pk.join(", ")));
        }

        ddl.push_str(") WITH (\n");
        ddl.push_str("  'connector' = 'boyodb',\n");
        ddl.push_str(&format!("  'hostname' = '{}',\n", self.config.connection.host));
        ddl.push_str(&format!("  'port' = '{}',\n", self.config.connection.port));
        ddl.push_str(&format!("  'database' = '{}',\n", self.config.connection.database));
        ddl.push_str(&format!("  'table-name' = '{}',\n", table_name));
        ddl.push_str(&format!("  'sink.buffer-flush.max-rows' = '{}',\n", self.config.buffer_size));
        ddl.push_str(&format!("  'sink.buffer-flush.interval' = '{}ms'\n", self.config.flush_interval_ms));
        ddl.push_str(")");

        ddl
    }

    fn map_type(&self, boyodb_type: &str) -> &'static str {
        match boyodb_type.to_uppercase().as_str() {
            "INT8" | "TINYINT" => "TINYINT",
            "INT16" | "SMALLINT" => "SMALLINT",
            "INT32" | "INT" | "INTEGER" => "INT",
            "INT64" | "BIGINT" => "BIGINT",
            "FLOAT32" | "FLOAT" => "FLOAT",
            "FLOAT64" | "DOUBLE" => "DOUBLE",
            "BOOLEAN" | "BOOL" => "BOOLEAN",
            "DATE" => "DATE",
            "TIMESTAMP" => "TIMESTAMP",
            "TIME" => "TIME",
            "BINARY" | "BLOB" => "BYTES",
            _ => "STRING",
        }
    }
}

// ============================================================================
// dbt Integration
// ============================================================================

/// dbt adapter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtAdapterConfig {
    /// Connection config
    pub connection: ConnectionConfig,
    /// Schema for dbt models
    pub schema: String,
    /// Threads for parallel execution
    pub threads: usize,
    /// Retry count for failed queries
    pub retries: usize,
    /// Query timeout
    pub query_timeout_secs: u64,
}

impl Default for DbtAdapterConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            schema: "dbt".to_string(),
            threads: 4,
            retries: 1,
            query_timeout_secs: 300,
        }
    }
}

/// dbt profile generator
pub struct DbtProfileGenerator {
    config: DbtAdapterConfig,
}

impl DbtProfileGenerator {
    pub fn new(config: DbtAdapterConfig) -> Self {
        Self { config }
    }

    /// Generate profiles.yml content
    pub fn generate_profile(&self, profile_name: &str, target_name: &str) -> String {
        format!(
            r#"{profile_name}:
  target: {target_name}
  outputs:
    {target_name}:
      type: boyodb
      host: {host}
      port: {port}
      database: {database}
      schema: {schema}
      threads: {threads}
      retries: {retries}
      timeout_seconds: {timeout}
"#,
            profile_name = profile_name,
            target_name = target_name,
            host = self.config.connection.host,
            port = self.config.connection.port,
            database = self.config.connection.database,
            schema = self.config.schema,
            threads = self.config.threads,
            retries = self.config.retries,
            timeout = self.config.query_timeout_secs,
        )
    }

    /// Generate adapter macro definitions
    pub fn generate_adapter_macros(&self) -> String {
        r#"{% macro boyodb__create_schema(relation) %}
  CREATE DATABASE IF NOT EXISTS {{ relation.database }};
{% endmacro %}

{% macro boyodb__drop_schema(relation) %}
  DROP DATABASE IF EXISTS {{ relation.database }};
{% endmacro %}

{% macro boyodb__create_table_as(temporary, relation, sql) %}
  CREATE TABLE {{ relation }} AS {{ sql }};
{% endmacro %}

{% macro boyodb__create_view_as(relation, sql) %}
  CREATE VIEW {{ relation }} AS {{ sql }};
{% endmacro %}

{% macro boyodb__drop_relation(relation) %}
  DROP {{ relation.type }} IF EXISTS {{ relation }};
{% endmacro %}

{% macro boyodb__truncate_relation(relation) %}
  TRUNCATE TABLE {{ relation }};
{% endmacro %}

{% macro boyodb__rename_relation(from_relation, to_relation) %}
  ALTER TABLE {{ from_relation }} RENAME TO {{ to_relation }};
{% endmacro %}

{% macro boyodb__list_schemas(database) %}
  SELECT database_name FROM system.databases;
{% endmacro %}

{% macro boyodb__list_relations_without_caching(schema_relation) %}
  SELECT
    table_name AS name,
    database_name AS schema,
    CASE table_type WHEN 'BASE TABLE' THEN 'table' ELSE 'view' END AS type
  FROM system.tables
  WHERE database_name = '{{ schema_relation.schema }}';
{% endmacro %}

{% macro boyodb__get_columns_in_relation(relation) %}
  SELECT
    column_name AS name,
    data_type AS type,
    CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable
  FROM system.columns
  WHERE database_name = '{{ relation.database }}'
    AND table_name = '{{ relation.identifier }}';
{% endmacro %}
"#.to_string()
    }
}

/// dbt model metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtModel {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub materialized: DbtMaterialization,
    pub depends_on: Vec<String>,
    pub columns: Vec<DbtColumn>,
    pub tags: Vec<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DbtMaterialization {
    View,
    Table,
    Incremental,
    Ephemeral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtColumn {
    pub name: String,
    pub description: Option<String>,
    pub tests: Vec<String>,
}

// ============================================================================
// Apache Airflow Integration
// ============================================================================

/// Airflow operator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirflowOperatorConfig {
    /// Connection config
    pub connection: ConnectionConfig,
    /// Airflow connection ID
    pub conn_id: String,
}

impl Default for AirflowOperatorConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            conn_id: "boyodb_default".to_string(),
        }
    }
}

/// Airflow operator generator
pub struct AirflowOperatorGenerator {
    config: AirflowOperatorConfig,
}

impl AirflowOperatorGenerator {
    pub fn new(config: AirflowOperatorConfig) -> Self {
        Self { config }
    }

    /// Generate Python operator code
    pub fn generate_operator(&self) -> String {
        format!(r#"from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
import socket
import json


class BoyoDBHook(BaseHook):
    """Hook for connecting to BoyoDB."""

    conn_type = 'boyodb'
    conn_name_attr = 'boyodb_conn_id'
    default_conn_name = '{conn_id}'
    hook_name = 'BoyoDB'

    def __init__(self, boyodb_conn_id: str = '{conn_id}'):
        super().__init__()
        self.boyodb_conn_id = boyodb_conn_id
        self.connection = None
        self._socket = None

    def get_conn(self):
        if self.connection is None:
            self.connection = self.get_connection(self.boyodb_conn_id)
        return self.connection

    def connect(self):
        conn = self.get_conn()
        host = conn.host or 'localhost'
        port = conn.port or 8765

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        return self._socket

    def execute(self, sql: str) -> dict:
        sock = self.connect()
        try:
            request = json.dumps({{"op": "query", "sql": sql}})
            sock.sendall((request + "\\n").encode())

            response = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
                if b"\\n" in response:
                    break

            return json.loads(response.decode().strip())
        finally:
            sock.close()


class BoyoDBOperator(BaseOperator):
    """Execute SQL on BoyoDB."""

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        boyodb_conn_id: str = '{conn_id}',
        parameters: dict = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.boyodb_conn_id = boyodb_conn_id
        self.parameters = parameters or {{}}

    def execute(self, context):
        hook = BoyoDBHook(boyodb_conn_id=self.boyodb_conn_id)
        self.log.info(f"Executing SQL: {{self.sql}}")
        result = hook.execute(self.sql)
        self.log.info(f"Result: {{result}}")
        return result


class BoyoDBSensor(BaseOperator):
    """Wait for a condition in BoyoDB."""

    template_fields = ('sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        boyodb_conn_id: str = '{conn_id}',
        poke_interval: int = 60,
        timeout: int = 3600,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.boyodb_conn_id = boyodb_conn_id
        self.poke_interval = poke_interval
        self.timeout = timeout

    def poke(self, context):
        hook = BoyoDBHook(boyodb_conn_id=self.boyodb_conn_id)
        result = hook.execute(self.sql)
        if result.get('status') == 'ok' and result.get('rows'):
            return bool(result['rows'][0][0])
        return False


class BoyoDBToFileOperator(BaseOperator):
    """Export BoyoDB query results to a file."""

    template_fields = ('sql', 'file_path')
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(
        self,
        sql: str,
        file_path: str,
        file_format: str = 'csv',
        boyodb_conn_id: str = '{conn_id}',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.file_path = file_path
        self.file_format = file_format
        self.boyodb_conn_id = boyodb_conn_id

    def execute(self, context):
        import csv
        import json as json_lib

        hook = BoyoDBHook(boyodb_conn_id=self.boyodb_conn_id)
        result = hook.execute(self.sql)

        if result.get('status') != 'ok':
            raise Exception(f"Query failed: {{result.get('error')}}")

        columns = result.get('columns', [])
        rows = result.get('rows', [])

        if self.file_format == 'csv':
            with open(self.file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        elif self.file_format == 'json':
            data = [dict(zip(columns, row)) for row in rows]
            with open(self.file_path, 'w') as f:
                json_lib.dump(data, f, indent=2)
        else:
            raise ValueError(f"Unsupported format: {{self.file_format}}")

        return self.file_path
"#, conn_id = self.config.conn_id)
    }

    /// Generate Airflow connection configuration
    pub fn generate_connection(&self) -> HashMap<String, String> {
        let mut conn = HashMap::new();
        conn.insert("conn_id".to_string(), self.config.conn_id.clone());
        conn.insert("conn_type".to_string(), "boyodb".to_string());
        conn.insert("host".to_string(), self.config.connection.host.clone());
        conn.insert("port".to_string(), self.config.connection.port.to_string());
        conn.insert("schema".to_string(), self.config.connection.database.clone());

        if let Some(ref user) = self.config.connection.username {
            conn.insert("login".to_string(), user.clone());
        }

        conn
    }
}

// ============================================================================
// Presto/Trino Integration
// ============================================================================

/// Presto/Trino connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrestoConnectorConfig {
    /// Connection config
    pub connection: ConnectionConfig,
    /// Catalog name in Presto/Trino
    pub catalog_name: String,
    /// Enable query pushdown
    pub query_pushdown: bool,
    /// Metadata cache TTL in seconds
    pub metadata_cache_ttl_secs: u64,
}

impl Default for PrestoConnectorConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            catalog_name: "boyodb".to_string(),
            query_pushdown: true,
            metadata_cache_ttl_secs: 60,
        }
    }
}

/// Presto/Trino connector properties generator
pub struct PrestoConnectorGenerator {
    config: PrestoConnectorConfig,
}

impl PrestoConnectorGenerator {
    pub fn new(config: PrestoConnectorConfig) -> Self {
        Self { config }
    }

    /// Generate catalog properties file content
    pub fn generate_catalog_properties(&self) -> String {
        format!(
            r#"connector.name=boyodb
boyodb.host={host}
boyodb.port={port}
boyodb.database={database}
boyodb.query-pushdown-enabled={pushdown}
boyodb.metadata-cache-ttl={cache_ttl}s
"#,
            host = self.config.connection.host,
            port = self.config.connection.port,
            database = self.config.connection.database,
            pushdown = self.config.query_pushdown,
            cache_ttl = self.config.metadata_cache_ttl_secs,
        )
    }

    /// Generate connector JAR plugin structure info
    pub fn get_plugin_structure(&self) -> PrestoPluginInfo {
        PrestoPluginInfo {
            plugin_name: "boyodb".to_string(),
            connector_class: "com.boyodb.presto.BoyoDBConnector".to_string(),
            metadata_class: "com.boyodb.presto.BoyoDBMetadata".to_string(),
            split_manager_class: "com.boyodb.presto.BoyoDBSplitManager".to_string(),
            record_set_provider_class: "com.boyodb.presto.BoyoDBRecordSetProvider".to_string(),
            page_sink_provider_class: "com.boyodb.presto.BoyoDBPageSinkProvider".to_string(),
        }
    }

    /// Generate type mappings for Presto/Trino
    pub fn get_type_mappings(&self) -> Vec<TypeMapping> {
        vec![
            TypeMapping { boyodb_type: "INT8".into(), external_type: "TINYINT".into(), nullable: true },
            TypeMapping { boyodb_type: "INT16".into(), external_type: "SMALLINT".into(), nullable: true },
            TypeMapping { boyodb_type: "INT32".into(), external_type: "INTEGER".into(), nullable: true },
            TypeMapping { boyodb_type: "INT64".into(), external_type: "BIGINT".into(), nullable: true },
            TypeMapping { boyodb_type: "FLOAT32".into(), external_type: "REAL".into(), nullable: true },
            TypeMapping { boyodb_type: "FLOAT64".into(), external_type: "DOUBLE".into(), nullable: true },
            TypeMapping { boyodb_type: "VARCHAR".into(), external_type: "VARCHAR".into(), nullable: true },
            TypeMapping { boyodb_type: "BOOLEAN".into(), external_type: "BOOLEAN".into(), nullable: true },
            TypeMapping { boyodb_type: "DATE".into(), external_type: "DATE".into(), nullable: true },
            TypeMapping { boyodb_type: "TIMESTAMP".into(), external_type: "TIMESTAMP".into(), nullable: true },
            TypeMapping { boyodb_type: "BINARY".into(), external_type: "VARBINARY".into(), nullable: true },
        ]
    }
}

/// Presto plugin information
#[derive(Debug, Clone, Serialize)]
pub struct PrestoPluginInfo {
    pub plugin_name: String,
    pub connector_class: String,
    pub metadata_class: String,
    pub split_manager_class: String,
    pub record_set_provider_class: String,
    pub page_sink_provider_class: String,
}

// ============================================================================
// Integration Manager
// ============================================================================

/// Central manager for all integrations
pub struct IntegrationManager {
    /// Registered Spark configs
    spark_configs: RwLock<HashMap<String, SparkConnectorConfig>>,
    /// Registered Flink configs
    flink_configs: RwLock<HashMap<String, FlinkConnectorConfig>>,
    /// Registered dbt configs
    dbt_configs: RwLock<HashMap<String, DbtAdapterConfig>>,
    /// Registered Airflow configs
    airflow_configs: RwLock<HashMap<String, AirflowOperatorConfig>>,
    /// Registered Presto configs
    presto_configs: RwLock<HashMap<String, PrestoConnectorConfig>>,
}

impl IntegrationManager {
    pub fn new() -> Self {
        Self {
            spark_configs: RwLock::new(HashMap::new()),
            flink_configs: RwLock::new(HashMap::new()),
            dbt_configs: RwLock::new(HashMap::new()),
            airflow_configs: RwLock::new(HashMap::new()),
            presto_configs: RwLock::new(HashMap::new()),
        }
    }

    // Spark methods
    pub fn register_spark(&self, name: &str, config: SparkConnectorConfig) {
        self.spark_configs.write().insert(name.to_string(), config);
    }

    pub fn get_spark(&self, name: &str) -> Option<SparkConnector> {
        self.spark_configs.read().get(name).map(|c| SparkConnector::new(c.clone()))
    }

    // Flink methods
    pub fn register_flink(&self, name: &str, config: FlinkConnectorConfig) {
        self.flink_configs.write().insert(name.to_string(), config);
    }

    pub fn get_flink_ddl_generator(&self, name: &str) -> Option<FlinkDdlGenerator> {
        self.flink_configs.read().get(name).map(|c| FlinkDdlGenerator::new(c.clone()))
    }

    // dbt methods
    pub fn register_dbt(&self, name: &str, config: DbtAdapterConfig) {
        self.dbt_configs.write().insert(name.to_string(), config);
    }

    pub fn get_dbt_generator(&self, name: &str) -> Option<DbtProfileGenerator> {
        self.dbt_configs.read().get(name).map(|c| DbtProfileGenerator::new(c.clone()))
    }

    // Airflow methods
    pub fn register_airflow(&self, name: &str, config: AirflowOperatorConfig) {
        self.airflow_configs.write().insert(name.to_string(), config);
    }

    pub fn get_airflow_generator(&self, name: &str) -> Option<AirflowOperatorGenerator> {
        self.airflow_configs.read().get(name).map(|c| AirflowOperatorGenerator::new(c.clone()))
    }

    // Presto methods
    pub fn register_presto(&self, name: &str, config: PrestoConnectorConfig) {
        self.presto_configs.write().insert(name.to_string(), config);
    }

    pub fn get_presto_generator(&self, name: &str) -> Option<PrestoConnectorGenerator> {
        self.presto_configs.read().get(name).map(|c| PrestoConnectorGenerator::new(c.clone()))
    }
}

impl Default for IntegrationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_jdbc_url() {
        let config = SparkConnectorConfig {
            connection: ConnectionConfig {
                host: "db.example.com".to_string(),
                port: 8765,
                database: "mydb".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let connector = SparkConnector::new(config);
        assert_eq!(connector.jdbc_url(), "jdbc:boyodb://db.example.com:8765/mydb");
    }

    #[test]
    fn test_flink_ddl_generation() {
        let config = FlinkConnectorConfig::default();
        let generator = FlinkDdlGenerator::new(config);

        let ddl = generator.create_table_ddl(
            "users",
            &[
                ("id".to_string(), "INT64".to_string()),
                ("name".to_string(), "VARCHAR".to_string()),
            ],
            Some(&["id".to_string()]),
        );

        assert!(ddl.contains("CREATE TABLE users"));
        assert!(ddl.contains("'connector' = 'boyodb'"));
        assert!(ddl.contains("PRIMARY KEY (id) NOT ENFORCED"));
    }

    #[test]
    fn test_dbt_profile_generation() {
        let config = DbtAdapterConfig::default();
        let generator = DbtProfileGenerator::new(config);

        let profile = generator.generate_profile("my_project", "dev");
        assert!(profile.contains("my_project:"));
        assert!(profile.contains("type: boyodb"));
    }

    #[test]
    fn test_presto_catalog_properties() {
        let config = PrestoConnectorConfig::default();
        let generator = PrestoConnectorGenerator::new(config);

        let props = generator.generate_catalog_properties();
        assert!(props.contains("connector.name=boyodb"));
        assert!(props.contains("boyodb.host="));
    }
}
