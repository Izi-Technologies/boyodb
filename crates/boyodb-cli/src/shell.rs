//! Interactive SQL shell for boyodb-server
//!
//! Provides a mysql/psql-like interactive experience for executing SQL queries
//! and administrative commands against a running boyodb-server.

use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use base64::{engine::general_purpose, Engine as _};
use comfy_table::{Cell, Table};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Config, Context, EditMode, Editor, Helper};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use rustls::pki_types::ServerName;

/// Configuration loaded from ~/.boyodbrc
#[derive(Debug, Default, Deserialize)]
pub struct ShellConfig {
    /// Default host:port to connect to
    #[serde(default)]
    pub host: Option<String>,
    /// Default authentication token
    #[serde(default)]
    pub token: Option<String>,
    /// Default database to use
    #[serde(default)]
    pub database: Option<String>,
    /// Enable TLS by default
    #[serde(default)]
    pub tls: Option<bool>,
    /// Path to CA certificate
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Default output format (table, csv, json)
    #[serde(default)]
    pub format: Option<String>,
    /// Query timeout in milliseconds
    #[serde(default)]
    pub timeout_ms: Option<u32>,
    /// Username for auto-login
    #[serde(default)]
    pub user: Option<String>,
}

impl ShellConfig {
    /// Load configuration from ~/.boyodbrc (TOML format)
    ///
    /// Security: The config file may contain sensitive data (tokens).
    /// On Unix, this function warns if the file has overly permissive permissions.
    pub fn load() -> Self {
        let config_path = Self::config_path();
        if let Some(path) = config_path {
            if path.exists() {
                // Security check: warn if config file has insecure permissions on Unix
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        let mode = metadata.mode();
                        // Check if group or others have read/write access
                        if mode & 0o077 != 0 {
                            eprintln!(
                                "WARNING: {} has insecure permissions ({:o}). \
                                 Config may contain sensitive data. \
                                 Run: chmod 600 {}",
                                path.display(),
                                mode & 0o777,
                                path.display()
                            );
                        }
                    }
                }

                if let Ok(contents) = std::fs::read_to_string(&path) {
                    // Try TOML parsing
                    if let Ok(config) = toml_parse(&contents) {
                        return config;
                    }
                    eprintln!("Warning: Failed to parse {}, using defaults", path.display());
                }
            }
        }
        Self::default()
    }

    /// Get the config file path
    fn config_path() -> Option<PathBuf> {
        dirs_next::home_dir().map(|h| h.join(".boyodbrc"))
    }

    /// Parse output format string
    pub fn output_format(&self) -> OutputFormat {
        match self.format.as_deref() {
            Some("csv") => OutputFormat::Csv,
            Some("json") => OutputFormat::Json,
            _ => OutputFormat::Table,
        }
    }
}

/// Simple TOML-like parser for config file
fn toml_parse(contents: &str) -> Result<ShellConfig, ()> {
    let mut config = ShellConfig::default();

    for line in contents.lines() {
        let line = line.trim();
        // Skip comments and empty lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim().trim_matches('"').trim_matches('\'');

            match key {
                "host" => config.host = Some(value.to_string()),
                "token" => config.token = Some(value.to_string()),
                "database" => config.database = Some(value.to_string()),
                "tls" => config.tls = Some(value == "true" || value == "1"),
                "ca_cert" => config.ca_cert = Some(value.to_string()),
                "format" => config.format = Some(value.to_string()),
                "timeout_ms" => config.timeout_ms = value.parse().ok(),
                "user" => config.user = Some(value.to_string()),
                _ => {} // Ignore unknown keys
            }
        }
    }

    Ok(config)
}

/// TLS configuration for secure connections
#[derive(Clone)]
pub struct TlsConfig {
    /// Path to CA certificate file for verification
    pub ca_path: Option<String>,
    /// Skip certificate verification (insecure, for testing)
    pub skip_verify: bool,
}

/// Output format for query results
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum OutputFormat {
    #[default]
    Table,
    Csv,
    Json,
    /// Vertical format (like psql's \x) - one column per line
    Vertical,
}

/// Display options for query output
struct DisplayOptions<'a> {
    format: OutputFormat,
    output_file: Option<&'a PathBuf>,
    use_pager: bool,
}

/// SQL keywords for tab completion
const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
    "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET", "GROUP", "HAVING",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON", "AS",
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "TRUNCATE",
    "CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "INDEX", "VIEW",
    "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "NULL", "DEFAULT",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT",
    "UNION", "INTERSECT", "EXCEPT", "ALL",
    "CASE", "WHEN", "THEN", "ELSE", "END",
    "CAST", "COALESCE", "NULLIF",
    "TRUE", "FALSE", "IS",
    "EXPLAIN", "ANALYZE", "DESCRIBE", "SHOW", "USE",
    "GRANT", "REVOKE", "TO", "WITH", "OPTION",
    "VACUUM", "DEDUPLICATE", "DEDUPLICATION",
];

/// Meta-commands for tab completion
const META_COMMANDS: &[&str] = &[
    "\\q", "\\quit", "\\h", "\\help", "\\?",
    "\\l", "\\dt", "\\d", "\\du", "\\di", "\\dv",
    "\\c", "\\x", "\\o", "\\i",
    "\\timing", "\\format", "\\copy", "\\ps", "\\views",
    "\\history", "\\pset", "\\pager",
];

/// Tab completion helper for the shell
#[derive(Default)]
struct BoyodbHelper {
    /// Cached list of databases
    databases: Vec<String>,
    /// Cached list of tables (database.table format)
    tables: Vec<String>,
    /// Cached list of columns per table (for future column completion)
    #[allow(dead_code)]
    columns: std::collections::HashMap<String, Vec<String>>,
}

impl BoyodbHelper {
    fn new() -> Self {
        Self::default()
    }

    /// Update cached metadata (call periodically)
    fn update_databases(&mut self, dbs: Vec<String>) {
        self.databases = dbs;
    }

    fn update_tables(&mut self, tables: Vec<(String, String)>) {
        self.tables = tables.into_iter()
            .map(|(db, tbl)| format!("{}.{}", db, tbl))
            .collect();
    }

    /// Get completions for the current input
    fn get_completions(&self, line: &str, pos: usize) -> Vec<Pair> {
        let mut completions = Vec::new();
        let line_to_cursor = &line[..pos];

        // Find the word being completed
        let word_start = line_to_cursor.rfind(|c: char| c.is_whitespace() || c == ',' || c == '(')
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &line_to_cursor[word_start..];
        let word_upper = word.to_uppercase();

        // Meta-command completion
        if line_to_cursor.trim().starts_with('\\') {
            for cmd in META_COMMANDS {
                if cmd.starts_with(&line_to_cursor.trim().to_lowercase()) {
                    completions.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
            return completions;
        }

        // SQL keyword completion
        for kw in SQL_KEYWORDS {
            if kw.starts_with(&word_upper) && !word.is_empty() {
                // Match case with user input
                let replacement = if word.chars().next().map(|c| c.is_lowercase()).unwrap_or(false) {
                    kw.to_lowercase()
                } else {
                    kw.to_string()
                };
                completions.push(Pair {
                    display: kw.to_string(),
                    replacement,
                });
            }
        }

        // Table name completion (after FROM, JOIN, INTO, UPDATE, TABLE)
        let lower_line = line_to_cursor.to_lowercase();
        if lower_line.contains("from ") || lower_line.contains("join ")
            || lower_line.contains("into ") || lower_line.contains("update ")
            || lower_line.contains("table ") || lower_line.contains("describe ")
        {
            for table in &self.tables {
                if table.to_lowercase().starts_with(&word.to_lowercase()) {
                    completions.push(Pair {
                        display: table.clone(),
                        replacement: table.clone(),
                    });
                }
            }
            // Also suggest just table names without database prefix
            for table in &self.tables {
                if let Some(tbl_name) = table.split('.').nth(1) {
                    if tbl_name.to_lowercase().starts_with(&word.to_lowercase()) {
                        completions.push(Pair {
                            display: tbl_name.to_string(),
                            replacement: tbl_name.to_string(),
                        });
                    }
                }
            }
        }

        // Database name completion (after USE, DATABASE, \c)
        if lower_line.contains("use ") || lower_line.ends_with("\\c ")
            || lower_line.contains("database ")
        {
            for db in &self.databases {
                if db.to_lowercase().starts_with(&word.to_lowercase()) {
                    completions.push(Pair {
                        display: db.clone(),
                        replacement: db.clone(),
                    });
                }
            }
        }

        // Remove duplicates
        completions.sort_by(|a, b| a.display.cmp(&b.display));
        completions.dedup_by(|a, b| a.display == b.display);

        completions
    }
}

impl Completer for BoyodbHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let completions = self.get_completions(line, pos);

        // Find word start for replacement position
        let line_to_cursor = &line[..pos];
        let word_start = line_to_cursor.rfind(|c: char| c.is_whitespace() || c == ',' || c == '(')
            .map(|i| i + 1)
            .unwrap_or(0);

        Ok((word_start, completions))
    }
}

impl Hinter for BoyodbHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None // No inline hints for now
    }
}

impl Highlighter for BoyodbHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        Cow::Borrowed(line) // No syntax highlighting for now
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        false
    }
}

impl Validator for BoyodbHelper {}

impl Helper for BoyodbHelper {}

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Request envelope sent to the server
#[derive(Serialize)]
struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    #[serde(flatten)]
    op: Operation,
}

/// Server operations
#[derive(Serialize, Clone)]
#[serde(tag = "op", rename_all = "lowercase")]
#[allow(dead_code)] // Some variants reserved for future server operations
enum Operation {
    Query {
        sql: String,
        timeout_millis: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
    },
    Prepare {
        sql: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
    },
    #[serde(rename = "execute_prepared_binary")]
    ExecutePreparedBinary {
        id: String,
        timeout_millis: u32,
        #[serde(default)]
        stream: bool,
    },
    #[serde(rename = "query_binary")]
    QueryBinary {
        sql: String,
        timeout_millis: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        #[serde(default)]
        stream: bool,
    },
    Explain {
        sql: String,
    },
    Health,
    #[serde(rename = "list_databases")]
    ListDatabases,
    #[serde(rename = "list_tables")]
    ListTables {
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
    },
    #[serde(rename = "create_database")]
    CreateDatabase { name: String },
    #[serde(rename = "create_table")]
    CreateTable {
        database: String,
        table: String,
    },
    Metrics,
    #[serde(rename = "wal_stats")]
    WalStats,
    Login {
        username: String,
        password: String,
    },
    Logout,
    /// Ingest CSV data into a table
    #[serde(rename = "ingest_csv")]
    IngestCsv {
        payload_base64: String,
        database: String,
        table: String,
        has_header: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        delimiter: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        infer_schema_rows: Option<usize>,
    },
}

/// Response from the server
#[derive(Deserialize, Debug)]
#[allow(dead_code)] // Some fields reserved for future server responses
struct Response {
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    ipc_base64: Option<String>,
    #[serde(default)]
    ipc_len: Option<u64>,
    #[serde(default)]
    ipc_streaming: Option<bool>,
    #[serde(skip)]
    ipc_bytes: Option<Vec<u8>>,
    #[serde(default)]
    prepared_id: Option<String>,
    #[serde(default)]
    segments_scanned: Option<usize>,
    #[serde(default)]
    data_skipped_bytes: Option<u64>,
    #[serde(default)]
    databases: Option<Vec<String>>,
    #[serde(default)]
    tables: Option<Vec<TableInfo>>,
    #[serde(default)]
    explain_plan: Option<serde_json::Value>,
    #[serde(default)]
    metrics: Option<serde_json::Value>,
    #[serde(default)]
    wal_stats: Option<serde_json::Value>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    table_description: Option<TableDescription>,
}

fn is_select_like(sql: &str) -> bool {
    let trimmed = sql.trim_start().to_ascii_lowercase();
    trimmed.starts_with("select ") || trimmed.starts_with("with ")
}

fn build_query_operation(sql: String, timeout_millis: u32, database: Option<String>) -> Operation {
    if is_select_like(&sql) {
        Operation::QueryBinary {
            sql,
            timeout_millis,
            database,
            stream: true,
        }
    } else {
        Operation::Query {
            sql,
            timeout_millis,
            database,
        }
    }
}

fn response_ipc_bytes(resp: &Response) -> Result<Option<Vec<u8>>> {
    if let Some(bytes) = &resp.ipc_bytes {
        return Ok(Some(bytes.clone()));
    }
    if let Some(ref ipc_base64) = resp.ipc_base64 {
        let ipc_bytes = general_purpose::STANDARD.decode(ipc_base64)?;
        return Ok(Some(ipc_bytes));
    }
    Ok(None)
}

/// Table description returned by DESCRIBE TABLE
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct TableDescription {
    database: String,
    table: String,
    #[serde(default)]
    schema_json: Option<String>,
    #[serde(default)]
    compression: Option<String>,
    #[serde(default)]
    segment_count: Option<usize>,
    #[serde(default)]
    total_bytes: Option<u64>,
    #[serde(default)]
    watermark_min: Option<u64>,
    #[serde(default)]
    watermark_max: Option<u64>,
    #[serde(default)]
    event_time_min: Option<u64>,
    #[serde(default)]
    event_time_max: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)] // Fields used for deserialization from server
struct TableInfo {
    database: String,
    name: String,
    schema_json: Option<String>,
}

/// A prepared statement stored client-side
#[allow(dead_code)]
struct PreparedStatement {
    name: String,
    sql: String,
    param_count: usize,
    server_id: Option<String>,
}

/// A client-side view (stored locally, expanded at query time)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientView {
    name: String,
    database: Option<String>,
    sql: String,
}

/// Shell state
struct ShellState {
    host: String,
    token: Option<String>,
    current_db: Option<String>,
    timeout_millis: u32,
    tls_config: Option<TlsConfig>,
    tls_connector: Option<TlsConnector>,
    /// Session ID from server after login
    session_id: Option<String>,
    /// Currently logged-in username
    current_user: Option<String>,
    /// Output format for query results
    output_format: OutputFormat,
    /// Client-side prepared statements
    prepared_statements: std::collections::HashMap<String, PreparedStatement>,
    /// Client-side views (stored locally, persisted to disk)
    client_views: std::collections::HashMap<String, ClientView>,
    /// Output file for \o command (None = stdout)
    output_file: Option<PathBuf>,
    /// Whether to use pager for large output
    use_pager: bool,
}

impl ShellState {
    fn new(host: String, token: Option<String>, database: Option<String>, tls_config: Option<TlsConfig>) -> Self {
        let tls_connector = tls_config.as_ref().map(|config| {
            build_tls_connector(config).expect("Failed to build TLS connector")
        });

        // Load saved views from disk
        let client_views = load_client_views().unwrap_or_default();

        Self {
            host,
            token,
            current_db: database,
            timeout_millis: 30_000,
            tls_config,
            tls_connector,
            session_id: None,
            current_user: None,
            output_format: OutputFormat::default(),
            prepared_statements: std::collections::HashMap::new(),
            client_views,
            output_file: None,
            use_pager: true, // Enable pager by default
        }
    }

    fn prompt(&self) -> String {
        let tls_indicator = if self.tls_config.is_some() { "[TLS]" } else { "" };
        let user_indicator = match &self.current_user {
            Some(user) => format!("{}@", user),
            None => String::new(),
        };
        match &self.current_db {
            Some(db) => format!("{}boyodb{}[{}]> ", user_indicator, tls_indicator, db),
            None => format!("{}boyodb{}> ", user_indicator, tls_indicator),
        }
    }

    /// Get the current auth token (session_id takes precedence over static token)
    fn auth_token(&self) -> Option<&str> {
        self.session_id.as_deref().or(self.token.as_deref())
    }
}

/// Run the interactive shell with config file support
///
/// Arguments provided explicitly override config file settings.
pub fn run_shell(
    host: Option<&str>,
    token: Option<String>,
    database: Option<String>,
    tls_config: Option<TlsConfig>,
    auth: Option<(String, String)>,
    command: Option<String>,
) -> Result<()> {
    // Load config file
    let config = ShellConfig::load();

    // Extract all values from config before any moves
    let effective_format = config.output_format();
    let effective_timeout = config.timeout_ms.unwrap_or(30_000);
    let config_tls_enabled = config.tls == Some(true);

    // CLI auth takes precedence over config file
    let auto_user = if auth.is_some() {
        None  // Don't use config user if CLI auth is provided
    } else {
        config.user
    };

    // Merge CLI args with config (CLI takes precedence)
    let effective_host = host
        .map(|s| s.to_string())
        .or(config.host)
        .unwrap_or_else(|| "localhost:8765".to_string());

    let effective_token = token.or(config.token);
    let effective_database = database.or(config.database);

    // TLS config from CLI or config file
    let effective_tls = if tls_config.is_some() {
        tls_config
    } else if config_tls_enabled {
        Some(TlsConfig {
            ca_path: config.ca_cert,
            skip_verify: false,
        })
    } else {
        None
    };

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_run_shell(
        &effective_host,
        effective_token,
        effective_database,
        effective_tls,
        effective_format,
        effective_timeout,
        auto_user,
        auth,
        command,
    ))
}

pub fn fetch_wal_stats(
    host: Option<&str>,
    token: Option<String>,
    tls_config: Option<TlsConfig>,
    auth: Option<(String, String)>,
) -> Result<serde_json::Value> {
    let config = ShellConfig::load();
    let config_tls_enabled = config.tls == Some(true);

    let auto_user = if auth.is_some() { None } else { config.user };

    let effective_host = host
        .map(|s| s.to_string())
        .or(config.host)
        .unwrap_or_else(|| "localhost:8765".to_string());

    let effective_token = token.or(config.token);
    let effective_tls = if tls_config.is_some() {
        tls_config
    } else if config_tls_enabled {
        Some(TlsConfig {
            ca_path: config.ca_cert,
            skip_verify: false,
        })
    } else {
        None
    };

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_fetch_wal_stats(
        &effective_host,
        effective_token,
        effective_tls,
        auto_user,
        auth,
    ))
}

async fn async_fetch_wal_stats(
    host: &str,
    token: Option<String>,
    tls_config: Option<TlsConfig>,
    auto_user: Option<String>,
    cli_auth: Option<(String, String)>,
) -> Result<serde_json::Value> {
    let mut state = ShellState::new(host.to_string(), token, None, tls_config);

    if let Some((username, password)) = cli_auth {
        let resp = send_request(
            &state,
            Operation::Login {
                username: username.clone(),
                password,
            },
        )
        .await?;
        if resp.status != "ok" {
            let msg = resp.message.unwrap_or_else(|| "login failed".into());
            return Err(anyhow!("Login failed: {}", msg));
        }
        state.session_id = resp.session_id;
        state.current_user = Some(username);
    } else if let Some(username) = auto_user {
        let password = rpassword::prompt_password("Password: ")
            .unwrap_or_else(|_| String::new());
        if password.is_empty() {
            return Err(anyhow!("Password required for wal_stats"));
        }
        let resp = send_request(
            &state,
            Operation::Login {
                username: username.clone(),
                password,
            },
        )
        .await?;
        if resp.status != "ok" {
            let msg = resp.message.unwrap_or_else(|| "login failed".into());
            return Err(anyhow!("Login failed: {}", msg));
        }
        state.session_id = resp.session_id;
        state.current_user = Some(username);
    }

    let resp = send_request(&state, Operation::WalStats).await?;
    if resp.status != "ok" {
        let msg = resp.message.unwrap_or_else(|| "wal_stats failed".into());
        return Err(anyhow!("wal_stats failed: {}", msg));
    }
    resp.wal_stats
        .ok_or_else(|| anyhow!("wal_stats missing from response"))
}

async fn async_run_shell(
    host: &str,
    token: Option<String>,
    database: Option<String>,
    tls_config: Option<TlsConfig>,
    output_format: OutputFormat,
    timeout_millis: u32,
    auto_user: Option<String>,
    cli_auth: Option<(String, String)>,
    command: Option<String>,
) -> Result<()> {
    let mut state = ShellState::new(host.to_string(), token, database, tls_config);
    state.output_format = output_format;
    state.timeout_millis = timeout_millis;

    // Non-interactive mode: suppress some output
    let interactive = command.is_none();

    // Warn about insecure TLS mode
    if let Some(ref tls_config) = state.tls_config {
        if tls_config.skip_verify && interactive {
            eprintln!("WARNING: TLS certificate verification is DISABLED. This is insecure and should only be used for testing!");
            eprintln!("         Connections are vulnerable to man-in-the-middle attacks.");
        }
    }

    // Test connection
    let tls_info = if state.tls_config.is_some() { " (TLS)" } else { "" };
    if interactive {
        print!("Connecting to {}{}... ", host, tls_info);
    }
    match send_request(&state, Operation::Health).await {
        Ok(_) => {
            if interactive {
                println!("OK");
            }
        }
        Err(e) => {
            if interactive {
                println!("FAILED");
            }
            return Err(anyhow!("Failed to connect: {}", e));
        }
    }

    if interactive {
        println!("boyodb shell v{}", VERSION);
        println!("Type 'help' for available commands, 'quit' to exit.\n");
        println!("History search: Ctrl+R (reverse), Ctrl+S (forward), Up/Down to navigate\n");
    }

    // CLI auth takes precedence (for non-interactive use)
    if let Some((username, password)) = cli_auth {
        let resp = send_request(&state, Operation::Login {
            username: username.clone(),
            password,
        }).await;

        match resp {
            Ok(r) if r.status == "ok" => {
                state.session_id = r.session_id;
                state.current_user = Some(username.clone());
                if interactive {
                    println!("Logged in as {}\n", username);
                }
            }
            Ok(r) => {
                let msg = r.message.unwrap_or_else(|| "unknown error".into());
                if interactive {
                    eprintln!("Login failed: {}\n", msg);
                }
                return Err(anyhow!("Login failed: {}", msg));
            }
            Err(e) => {
                if interactive {
                    eprintln!("Login failed: {}\n", e);
                }
                return Err(anyhow!("Login failed: {}", e));
            }
        }
    } else if let Some(username) = auto_user {
        // Auto-login if user is configured in config file
        if interactive {
            println!("Auto-login as '{}' (from config)...", username);
        }
        let password = rpassword::prompt_password("Password: ")
            .unwrap_or_else(|_| String::new());

        if !password.is_empty() {
            let resp = send_request(&state, Operation::Login {
                username: username.clone(),
                password,
            }).await;

            match resp {
                Ok(r) if r.status == "ok" => {
                    state.session_id = r.session_id;
                    state.current_user = Some(username.clone());
                    if interactive {
                        println!("Logged in as {}\n", username);
                    }
                }
                Ok(r) => {
                    if interactive {
                        eprintln!("Auto-login failed: {}\n", r.message.unwrap_or_else(|| "unknown error".into()));
                    }
                }
                Err(e) => {
                    if interactive {
                        eprintln!("Auto-login failed: {}\n", e);
                    }
                }
            }
        } else if interactive {
            eprintln!("No password provided, skipping auto-login\n");
        }
    }

    // Non-interactive command execution mode
    if let Some(sql) = command {
        // Execute the command and exit
        match process_input(&mut state, &sql).await {
            Ok(true) => return Ok(()), // quit requested
            Ok(false) => return Ok(()),
            Err(e) => return Err(anyhow!("Command failed: {}", e)),
        }
    }

    // Configure rustyline with proper history search support
    // Emacs mode (default) supports Ctrl+R for reverse history search
    let config = Config::builder()
        .max_history_size(10000)            // Store up to 10k history entries
        .unwrap()
        .history_ignore_dups(true)          // Don't store duplicate consecutive entries
        .unwrap()
        .history_ignore_space(true)         // Don't store entries starting with space (returns Self, no unwrap)
        .edit_mode(EditMode::Emacs)         // Emacs mode has Ctrl+R search built-in
        .auto_add_history(false)            // We add history manually for better control
        .build();

    let mut rl: Editor<BoyodbHelper, DefaultHistory> = Editor::with_config(config)?;
    rl.set_helper(Some(BoyodbHelper::new()));
    let history_path = dirs_next::home_dir()
        .map(|h| h.join(".boyodb_history"))
        .unwrap_or_else(|| ".boyodb_history".into());
    let _ = rl.load_history(&history_path);

    // Fetch initial metadata for tab completion
    if let Ok(resp) = send_request(&state, Operation::ListDatabases).await {
        if let Some(dbs) = resp.databases {
            if let Some(helper) = rl.helper_mut() {
                helper.update_databases(dbs);
            }
        }
    }
    if let Ok(resp) = send_request(&state, Operation::ListTables { database: None }).await {
        if let Some(tables) = resp.tables {
            let table_list: Vec<(String, String)> = tables.iter()
                .map(|t| (t.database.clone(), t.name.clone()))
                .collect();
            if let Some(helper) = rl.helper_mut() {
                helper.update_tables(table_list);
            }
        }
    }

    // Print keyboard shortcuts info
    println!("History search: Ctrl+R (reverse), Ctrl+S (forward), Up/Down to navigate\n");

    // Multi-line input buffer
    let mut input_buffer = String::new();

    loop {
        // Use continuation prompt if we're in multi-line mode
        let prompt = if input_buffer.is_empty() {
            state.prompt()
        } else {
            "   -> ".to_string()
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();

                // Empty line in single-line mode: skip
                // Empty line in multi-line mode: continue accumulating
                if line.is_empty() && input_buffer.is_empty() {
                    continue;
                }

                // Check if this is a shell command (starts with \) - execute immediately
                if input_buffer.is_empty() && line.starts_with('\\') {
                    let _ = rl.add_history_entry(line);

                    // Handle history command locally (needs access to rl)
                    let lower = line.to_lowercase();
                    if lower.starts_with("\\history") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        handle_history_command(&rl, &parts);
                        continue;
                    }

                    match process_input(&mut state, line).await {
                        Ok(true) => break, // quit requested
                        Ok(false) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    }
                    continue;
                }

                // Check for single-line commands that don't need semicolon
                if input_buffer.is_empty() && is_immediate_command(line) {
                    let _ = rl.add_history_entry(line);
                    match process_input(&mut state, line).await {
                        Ok(true) => break, // quit requested
                        Ok(false) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    }
                    continue;
                }

                // Accumulate multi-line input
                if !input_buffer.is_empty() {
                    input_buffer.push(' ');
                }
                input_buffer.push_str(line);

                // Check if the statement is complete (ends with semicolon, not in string)
                if is_statement_complete(&input_buffer) {
                    let full_input = input_buffer.trim().to_string();
                    input_buffer.clear();

                    if full_input.is_empty() {
                        continue;
                    }

                    let _ = rl.add_history_entry(&full_input);

                    // Handle history command locally (needs access to rl)
                    let lower = full_input.to_lowercase();
                    if lower.starts_with("history ") || lower == "history" {
                        let parts: Vec<&str> = full_input.split_whitespace().collect();
                        handle_history_command(&rl, &parts);
                        continue;
                    }

                    match process_input(&mut state, &full_input).await {
                        Ok(true) => break, // quit requested
                        Ok(false) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
                // If not complete, loop continues with continuation prompt
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C: cancel multi-line input if active, otherwise just print ^C
                if !input_buffer.is_empty() {
                    input_buffer.clear();
                    println!("^C (input cancelled)");
                } else {
                    println!("^C");
                }
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

/// Check if a command should be executed immediately without requiring semicolon
/// These are shell commands like help, quit, status, login, etc.
fn is_immediate_command(input: &str) -> bool {
    let lower = input.to_lowercase();
    let first_word = lower.split_whitespace().next().unwrap_or("");

    matches!(
        first_word,
        "help" | "quit" | "exit" | "status" | "login" | "logout" | "whoami" | "clear" | "use" | "history"
    )
}

/// Check if a SQL statement is complete (ends with semicolon outside of quotes)
/// This handles string literals to avoid false positives like SELECT 'foo;'
fn is_statement_complete(input: &str) -> bool {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return false;
    }

    // Track whether we're inside a string literal
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_char = ' ';
    let mut escape_next = false;

    for c in trimmed.chars() {
        if escape_next {
            escape_next = false;
            last_char = c;
            continue;
        }

        match c {
            '\\' => {
                escape_next = true;
            }
            '\'' if !in_double_quote => {
                // Handle escaped quotes within strings (e.g., 'it''s')
                if in_single_quote && last_char == '\'' {
                    // This might be an escaped quote, handled by doubling
                } else {
                    in_single_quote = !in_single_quote;
                }
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            _ => {}
        }
        last_char = c;
    }

    // Statement is complete if it ends with semicolon and we're not inside a string
    !in_single_quote && !in_double_quote && trimmed.ends_with(';')
}

/// Handle the history command
/// Syntax:
///   \history           - Show last 20 entries
///   \history N         - Show last N entries
///   \history search X  - Search history for pattern X
///   history clear      - Clear history (not implemented to prevent accidents)
fn handle_history_command(rl: &Editor<BoyodbHelper, DefaultHistory>, parts: &[&str]) {
    use rustyline::history::History;

    let history = rl.history();
    let len = history.len();

    if len == 0 {
        println!("History is empty");
        return;
    }

    // Check for search mode: \history search <pattern>
    if parts.len() >= 3 && parts[1].to_lowercase() == "search" {
        let pattern = parts[2..].join(" ").to_lowercase();
        println!("Searching history for '{}':", pattern);
        let mut found = 0;
        for (idx, entry) in history.iter().enumerate() {
            if entry.to_lowercase().contains(&pattern) {
                println!("  {:>4}  {}", idx + 1, entry);
                found += 1;
            }
        }
        if found == 0 {
            println!("  (no matches)");
        } else {
            println!("\n{} match(es) found", found);
        }
        return;
    }

    // Check for count: \history N
    let count = if parts.len() > 1 {
        parts[1].parse::<usize>().unwrap_or(20)
    } else {
        20
    };

    let start = len.saturating_sub(count);

    println!("History (last {} of {}):", count.min(len), len);
    for (idx, entry) in history.iter().enumerate().skip(start) {
        println!("  {:>4}  {}", idx + 1, entry);
    }
    println!("\nTip: Use Ctrl+R to search history interactively");
}

/// Process user input. Returns Ok(true) if quit was requested.
async fn process_input(state: &mut ShellState, input: &str) -> Result<bool> {
    let lower = input.to_lowercase();
    let parts: Vec<&str> = input.split_whitespace().collect();

    // Handle shell commands
    match parts.first().map(|s| s.to_lowercase()).as_deref() {
        Some("quit") | Some("exit") | Some("\\q") => return Ok(true),
        Some("help") | Some("\\h") | Some("\\?") => {
            print_help();
            return Ok(false);
        }
        Some("\\c") | Some("use") => {
            if parts.len() > 1 {
                // Strip trailing semicolon if present
                let db_name = parts[1].trim_end_matches(';');
                state.current_db = Some(db_name.to_string());
                println!("Switched to database: {}", db_name);
            } else {
                state.current_db = None;
                println!("Cleared current database");
            }
            return Ok(false);
        }
        Some("\\l") => {
            // List databases
            let resp = send_request(state, Operation::ListDatabases).await?;
            if let Some(dbs) = resp.databases {
                println!("Databases:");
                for db in dbs {
                    println!("  {}", db);
                }
            }
            return Ok(false);
        }
        Some("\\dt") => {
            // List tables
            let db = if parts.len() > 1 {
                Some(parts[1].to_string())
            } else {
                state.current_db.clone()
            };
            let resp = send_request(state, Operation::ListTables { database: db }).await?;
            if let Some(tables) = resp.tables {
                println!("Tables:");
                for t in tables {
                    println!("  {}.{}", t.database, t.name);
                }
            }
            return Ok(false);
        }
        Some("\\d") => {
            // Describe table - show schema
            if parts.len() < 2 {
                eprintln!("Usage: \\d <table> or \\d <database>.<table>");
                return Ok(false);
            }
            let table_ref = parts[1];
            // Try to describe the table using DESCRIBE TABLE SQL
            let sql = format!("DESCRIBE TABLE {};", table_ref);
            if let Ok(resp) = send_request(
                state,
                build_query_operation(sql, state.timeout_millis, state.current_db.clone()),
            )
            .await
            {
                if let Some(desc) = resp.table_description {
                    println!("Table: {}.{}", desc.database, desc.table);
                    if let Some(schema_json) = &desc.schema_json {
                        if let Ok(schema) = serde_json::from_str::<serde_json::Value>(schema_json) {
                            if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
                                println!("\nColumns:");
                                println!("{:<20} {:<15} {:<10}", "Name", "Type", "Nullable");
                                println!("{}", "-".repeat(50));
                                for field in fields {
                                    let name = field.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                                    let dtype = field.get("data_type").map(|d| format!("{:?}", d)).unwrap_or_else(|| "?".to_string());
                                    let nullable = field.get("nullable").and_then(|n| n.as_bool()).unwrap_or(true);
                                    println!("{:<20} {:<15} {:<10}", name, dtype, if nullable { "YES" } else { "NO" });
                                }
                            }
                        }
                    }
                    if let Some(seg_count) = desc.segment_count {
                        println!("\nSegments: {}", seg_count);
                    }
                    if let Some(total_bytes) = desc.total_bytes {
                        println!("Total size: {} bytes", total_bytes);
                    }
                    if let Some(compression) = &desc.compression {
                        println!("Compression: {}", compression);
                    }
                } else {
                    println!("Table: {}", table_ref);
                    println!("(Use DESCRIBE TABLE {} for detailed schema)", table_ref);
                }
            }
            return Ok(false);
        }
        Some("\\du") => {
            // List users/roles (like psql \du)
            // Execute SHOW USERS query
            let sql = "SHOW USERS;".to_string();
            let start = Instant::now();
            match send_request(
                state,
                build_query_operation(sql, state.timeout_millis, state.current_db.clone()),
            )
            .await
            {
                Ok(resp) => {
                    if let Ok(Some(ipc_bytes)) = response_ipc_bytes(&resp) {
                            let opts = DisplayOptions {
                                format: state.output_format,
                                output_file: state.output_file.as_ref(),
                                use_pager: state.use_pager,
                            };
                            let _ = display_arrow_results(&ipc_bytes, &resp, start.elapsed(), &opts);
                    } else if resp.status == "ok" {
                        println!("No users found (or SHOW USERS not supported)");
                    } else {
                        eprintln!("Error: {}", resp.message.unwrap_or_else(|| "unknown error".to_string()));
                    }
                }
                Err(e) => eprintln!("Error listing users: {}", e),
            }
            return Ok(false);
        }
        Some("\\di") => {
            // List indexes (like psql \di)
            // For now, show that indexes would be listed here
            println!("Index listing:");
            println!("(Index information not yet available - boyodb uses implicit indexing)");
            return Ok(false);
        }
        Some("\\o") => {
            // Output to file (like psql \o)
            if parts.len() > 1 {
                let file_path = parts[1].trim_end_matches(';');
                if file_path.is_empty() || file_path == "-" {
                    // Reset to stdout
                    if let Some(ref path) = state.output_file {
                        println!("Output reset to stdout (was: {})", path.display());
                    }
                    state.output_file = None;
                } else {
                    let path = PathBuf::from(file_path);
                    // Test that we can write to the file
                    match std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&path)
                    {
                        Ok(_) => {
                            println!("Query output will be written to: {}", path.display());
                            state.output_file = Some(path);
                        }
                        Err(e) => {
                            eprintln!("Cannot open file '{}': {}", file_path, e);
                        }
                    }
                }
            } else {
                match &state.output_file {
                    Some(path) => println!("Output going to: {}", path.display()),
                    None => println!("Output going to: stdout"),
                }
                println!("Usage: \\o <filename> to redirect, \\o - to reset to stdout");
            }
            return Ok(false);
        }
        Some("\\i") => {
            // Execute SQL from file (like psql \i)
            if parts.len() < 2 {
                eprintln!("Usage: \\i <filename>");
                return Ok(false);
            }
            let file_path = parts[1].trim_end_matches(';');
            let path = PathBuf::from(file_path);

            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    println!("Executing commands from: {}", path.display());
                    let mut success_count = 0;
                    let mut error_count = 0;

                    // Split on semicolons and execute each statement
                    for stmt in contents.split(';') {
                        let stmt = stmt.trim();
                        if stmt.is_empty() || stmt.starts_with("--") {
                            continue;
                        }

                        // Execute the statement via the query operation
                        let sql = format!("{};", stmt);
                        let start = Instant::now();
                        match send_request(
                            state,
                            build_query_operation(sql.clone(), state.timeout_millis, state.current_db.clone()),
                        )
                        .await {
                            Ok(resp) => {
                                if resp.status == "ok" {
                                    if let Ok(Some(ipc_bytes)) = response_ipc_bytes(&resp) {
                                            let opts = DisplayOptions {
                                                format: state.output_format,
                                                output_file: state.output_file.as_ref(),
                                                use_pager: state.use_pager,
                                            };
                                            let _ = display_arrow_results(&ipc_bytes, &resp, start.elapsed(), &opts);
                                    }
                                    success_count += 1;
                                } else {
                                    eprintln!("Error in '{}': {}", truncate_sql(&sql, 40), resp.message.unwrap_or_else(|| "unknown".to_string()));
                                    error_count += 1;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error in '{}': {}", truncate_sql(&sql, 40), e);
                                error_count += 1;
                            }
                        }
                    }
                    println!("\nExecuted {} statement(s), {} error(s)", success_count, error_count);
                }
                Err(e) => {
                    eprintln!("Cannot read file '{}': {}", file_path, e);
                }
            }
            return Ok(false);
        }
        Some("\\timing") => {
            println!("Query timing is always enabled");
            return Ok(false);
        }
        Some("\\pager") => {
            // Toggle pager for large output (like psql \pset pager)
            if parts.len() > 1 {
                let arg = parts[1].to_lowercase();
                let arg = arg.trim_end_matches(';');
                match arg {
                    "on" | "always" => {
                        state.use_pager = true;
                        println!("Pager is on (output will use less/more for large results)");
                    }
                    "off" | "never" => {
                        state.use_pager = false;
                        println!("Pager is off (output directly to terminal)");
                    }
                    _ => {
                        eprintln!("Usage: \\pager [on|off]");
                    }
                }
            } else {
                state.use_pager = !state.use_pager;
                if state.use_pager {
                    println!("Pager is on");
                } else {
                    println!("Pager is off");
                }
            }
            return Ok(false);
        }
        Some("\\copy") => {
            // COPY command for CSV import
            // Syntax: \copy <table> FROM '<file>' [WITH (options)]
            // Options: HEADER, DELIMITER '<char>'
            execute_copy(state, input).await?;
            return Ok(false);
        }
        Some("status") => {
            let resp = send_request(state, Operation::Health).await?;
            println!("Server: {} - {}", state.host, resp.status);
            if let Some(msg) = resp.message {
                println!("Message: {}", msg);
            }
            if let Some(ref user) = state.current_user {
                println!("Logged in as: {}", user);
            } else {
                println!("Not logged in");
            }
            return Ok(false);
        }
        Some("login") => {
            // LOGIN [username] [password] - prompts for missing fields
            // Supports: login, login user, login user password
            let username = if parts.len() > 1 {
                parts[1].trim_end_matches(';').to_string()
            } else {
                // Prompt for username
                print!("Username: ");
                std::io::Write::flush(&mut std::io::stdout())?;
                let mut username = String::new();
                std::io::stdin().read_line(&mut username)?;
                username.trim().to_string()
            };

            if username.is_empty() {
                eprintln!("Username required");
                return Ok(false);
            }

            // Check if password was provided as second argument (for non-interactive use)
            let password = if parts.len() > 2 {
                // Password provided on command line (for scripting/automation)
                parts[2].trim_end_matches(';').to_string()
            } else {
                // Prompt for password (hidden input) - interactive mode
                rpassword::prompt_password("Password: ")
                    .unwrap_or_else(|_| String::new())
            };

            if password.is_empty() {
                eprintln!("Password required");
                return Ok(false);
            }

            let resp = send_request(state, Operation::Login {
                username: username.clone(),
                password,
            }).await?;

            if resp.status == "ok" {
                state.session_id = resp.session_id;
                state.current_user = Some(username.clone());
                println!("Logged in as {}", username);
            } else {
                eprintln!("Login failed: {}", resp.message.unwrap_or_else(|| "unknown error".into()));
            }
            return Ok(false);
        }
        Some("logout") => {
            if state.current_user.is_none() {
                println!("Not logged in");
                return Ok(false);
            }

            let resp = send_request(state, Operation::Logout).await?;
            if resp.status == "ok" {
                let user = state.current_user.take();
                state.session_id = None;
                println!("Logged out{}", user.map(|u| format!(" (was {})", u)).unwrap_or_default());
            } else {
                eprintln!("Logout failed: {}", resp.message.unwrap_or_else(|| "unknown error".into()));
            }
            return Ok(false);
        }
        Some("whoami") => {
            match &state.current_user {
                Some(user) => println!("{}", user),
                None => println!("Not logged in"),
            }
            return Ok(false);
        }
        Some("clear") | Some("\\!") => {
            print!("\x1B[2J\x1B[1;1H");
            return Ok(false);
        }
        Some("\\ps") => {
            // List prepared statements
            if state.prepared_statements.is_empty() {
                println!("No prepared statements");
            } else {
                println!("Prepared statements:");
                for (name, stmt) in &state.prepared_statements {
                    println!("  {} ({} params): {}", name, stmt.param_count, truncate_sql(&stmt.sql, 60));
                }
            }
            return Ok(false);
        }
        Some("\\x") => {
            // Toggle expanded/vertical output (like psql \x)
            if state.output_format == OutputFormat::Vertical {
                state.output_format = OutputFormat::Table;
                println!("Expanded display is off.");
            } else {
                state.output_format = OutputFormat::Vertical;
                println!("Expanded display is on.");
            }
            return Ok(false);
        }
        Some("\\format") | Some("format") => {
            if parts.len() > 1 {
                let fmt = parts[1].to_lowercase();
                let fmt = fmt.trim_end_matches(';');
                match fmt {
                    "table" => {
                        state.output_format = OutputFormat::Table;
                        println!("Output format: table");
                    }
                    "csv" => {
                        state.output_format = OutputFormat::Csv;
                        println!("Output format: csv");
                    }
                    "json" => {
                        state.output_format = OutputFormat::Json;
                        println!("Output format: json");
                    }
                    "vertical" | "expanded" => {
                        state.output_format = OutputFormat::Vertical;
                        println!("Output format: vertical (expanded)");
                    }
                    _ => {
                        eprintln!("Unknown format '{}'. Available: table, csv, json, vertical", fmt);
                    }
                }
            } else {
                let current = match state.output_format {
                    OutputFormat::Table => "table",
                    OutputFormat::Csv => "csv",
                    OutputFormat::Json => "json",
                    OutputFormat::Vertical => "vertical (expanded)",
                };
                println!("Current output format: {}", current);
                println!("Usage: \\format <table|csv|json|vertical>");
            }
            return Ok(false);
        }
        Some("\\views") | Some("\\dv") => {
            // List client-side views
            list_views(state);
            return Ok(false);
        }
        _ => {}
    }

    // Handle PREPARE statement
    if lower.starts_with("prepare ") {
        handle_prepare(state, input).await?;
        return Ok(false);
    }

    // Handle EXECUTE statement
    if lower.starts_with("execute ") {
        execute_prepared(state, input).await?;
        return Ok(false);
    }

    // Handle DEALLOCATE statement
    if lower.starts_with("deallocate ") {
        handle_deallocate(state, input)?;
        return Ok(false);
    }

    // Handle CREATE VIEW (client-side)
    if lower.starts_with("create view ") || lower.starts_with("create or replace view ") {
        handle_create_view(state, input)?;
        return Ok(false);
    }

    // Handle DROP VIEW (client-side)
    if lower.starts_with("drop view ") {
        handle_drop_view(state, input)?;
        return Ok(false);
    }

    // Handle SQL statements
    if lower.starts_with("select")
        || lower.starts_with("show")
        || lower.starts_with("create")
        || lower.starts_with("drop")
        || lower.starts_with("grant")
        || lower.starts_with("revoke")
        || lower.starts_with("alter")
        || lower.starts_with("lock")
        || lower.starts_with("unlock")
        || lower.starts_with("truncate")
        || lower.starts_with("insert")
        || lower.starts_with("update")
        || lower.starts_with("delete")
        || lower.starts_with("describe")
        || lower.starts_with("desc ")
    {
        execute_sql(state, input).await?;
    } else if lower.starts_with("explain") {
        execute_explain(state, input).await?;
    } else {
        eprintln!("Unknown command. Type 'help' for available commands.");
    }

    Ok(false)
}

/// Execute a SQL query
async fn execute_sql(state: &ShellState, sql: &str) -> Result<()> {
    let start = Instant::now();

    // Expand any client-side views in the SQL
    let expanded_sql = if !state.client_views.is_empty() {
        expand_views_in_sql(sql, &state.client_views)
    } else {
        sql.to_string()
    };

    let resp = send_request(
        state,
        build_query_operation(expanded_sql, state.timeout_millis, state.current_db.clone()),
    )
    .await?;

    let elapsed = start.elapsed();

    if resp.status != "ok" {
        eprintln!("Error: {}", resp.message.unwrap_or_else(|| "unknown error".into()));
        return Ok(());
    }

    // Handle different response types
    if let Some(ipc_bytes) = response_ipc_bytes(&resp)? {
        let opts = DisplayOptions {
            format: state.output_format,
            output_file: state.output_file.as_ref(),
            use_pager: state.use_pager,
        };
        display_arrow_results(&ipc_bytes, &resp, elapsed, &opts)?;
    } else if let Some(msg) = resp.message {
        println!("{}", msg);
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    } else if let Some(tables) = resp.tables {
        // SHOW TABLES response
        let mut table = Table::new();
        table.set_header(vec!["Database", "Table"]);
        for t in tables {
            table.add_row(vec![t.database, t.name]);
        }
        println!("{}", table);
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    } else if let Some(dbs) = resp.databases {
        // SHOW DATABASES response
        let mut table = Table::new();
        table.set_header(vec!["Database"]);
        for db in dbs {
            table.add_row(vec![db]);
        }
        println!("{}", table);
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    } else if let Some(desc) = resp.table_description {
        // DESCRIBE TABLE response
        println!("Table: {}.{}", desc.database, desc.table);
        println!();

        // Display schema if available
        if let Some(schema_json) = desc.schema_json {
            if let Ok(fields) = serde_json::from_str::<Vec<serde_json::Value>>(&schema_json) {
                let mut table = Table::new();
                table.set_header(vec!["Column", "Type", "Nullable"]);
                for field in fields {
                    let name = field.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                    let dtype = field.get("type").and_then(|v| v.as_str())
                        .or_else(|| field.get("data_type").and_then(|v| v.as_str()))
                        .unwrap_or("?");
                    let nullable = field.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                    table.add_row(vec![
                        name.to_string(),
                        dtype.to_string(),
                        if nullable { "YES" } else { "NO" }.to_string(),
                    ]);
                }
                println!("{}", table);
                println!();
            }
        }

        // Display statistics
        println!("Statistics:");
        if let Some(count) = desc.segment_count {
            println!("  Segments: {}", count);
        }
        if let Some(bytes) = desc.total_bytes {
            println!("  Total Size: {} bytes", bytes);
        }
        if let Some(compression) = desc.compression {
            println!("  Compression: {}", compression);
        }
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    } else {
        println!("OK");
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    }

    Ok(())
}

/// Execute EXPLAIN
async fn execute_explain(state: &ShellState, sql: &str) -> Result<()> {
    // Strip "EXPLAIN " prefix
    let query_sql = sql.strip_prefix("EXPLAIN ")
        .or_else(|| sql.strip_prefix("explain "))
        .unwrap_or(sql);

    let start = Instant::now();
    let resp = send_request(state, Operation::Explain { sql: query_sql.to_string() }).await?;
    let elapsed = start.elapsed();

    if resp.status != "ok" {
        eprintln!("Error: {}", resp.message.unwrap_or_else(|| "unknown error".into()));
        return Ok(());
    }

    if let Some(plan) = resp.explain_plan {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    }
    println!("Time: {:.3}s", elapsed.as_secs_f64());

    Ok(())
}

/// Execute COPY command for bulk CSV import
/// Syntax: \copy <table> FROM '<file>' [WITH (options)]
/// Options: HEADER (default true), DELIMITER '<char>' (default ','), NO HEADER
///
/// Examples:
///   \copy users FROM '/path/to/users.csv'
///   \copy db.users FROM 'data.csv' WITH (DELIMITER '|')
///   \copy users FROM 'data.csv' WITH (NO HEADER)
async fn execute_copy(state: &ShellState, input: &str) -> Result<()> {
    // Parse the COPY command
    let input = input.trim();

    // Skip "\copy " prefix
    let rest = input.strip_prefix("\\copy ")
        .or_else(|| input.strip_prefix("\\COPY "))
        .ok_or_else(|| anyhow!("Invalid \\copy syntax"))?;

    // Parse: <table> FROM '<file>' [WITH (options)]
    let parts: Vec<&str> = rest.splitn(2, " FROM ").collect();
    if parts.len() != 2 {
        return Err(anyhow!("\\copy syntax: \\copy <table> FROM '<file>' [WITH (options)]"));
    }

    let table_ref = parts[0].trim();
    let rest = parts[1].trim();

    // Extract file path (in quotes)
    let (file_path, options_str) = extract_quoted_path(rest)?;

    // Parse table reference (database.table or just table)
    let (database, table) = if table_ref.contains('.') {
        let mut parts = table_ref.splitn(2, '.');
        let db = parts.next().unwrap().to_string();
        let tbl = parts.next().unwrap().to_string();
        (db, tbl)
    } else {
        let db = state.current_db.clone()
            .ok_or_else(|| anyhow!("No database selected. Use \\c <database> or specify as db.table"))?;
        (db, table_ref.to_string())
    };

    // Parse options
    let mut has_header = true;
    let mut delimiter: Option<String> = None;

    if let Some(opts) = options_str {
        let opts_upper = opts.to_uppercase();
        if opts_upper.contains("NO HEADER") || opts_upper.contains("NOHEADER") {
            has_header = false;
        }
        // Parse DELIMITER
        if let Some(delim_start) = opts_upper.find("DELIMITER") {
            let after_delim = &opts[delim_start + 9..];
            // Find the delimiter character in quotes
            if let Some(quote_start) = after_delim.find('\'') {
                let after_quote = &after_delim[quote_start + 1..];
                if let Some(quote_end) = after_quote.find('\'') {
                    delimiter = Some(after_quote[..quote_end].to_string());
                }
            }
        }
    }

    // Read the file
    let file_path_expanded = expand_path(&file_path);
    let csv_data = std::fs::read(&file_path_expanded)
        .map_err(|e| anyhow!("Failed to read file '{}': {}", file_path, e))?;

    let file_size = csv_data.len();
    println!("Loading {} bytes from '{}'...", file_size, file_path);

    // Encode as base64
    let payload_base64 = general_purpose::STANDARD.encode(&csv_data);

    // Send to server
    let start = Instant::now();
    let resp = send_request(
        state,
        Operation::IngestCsv {
            payload_base64,
            database: database.clone(),
            table: table.clone(),
            has_header,
            delimiter,
            infer_schema_rows: Some(100), // Use first 100 rows for schema inference
        },
    )
    .await?;

    let elapsed = start.elapsed();

    if resp.status == "ok" {
        println!(
            "COPY: {} bytes loaded into {}.{} (Time: {:.3}s)",
            file_size, database, table, elapsed.as_secs_f64()
        );
    } else {
        eprintln!(
            "COPY failed: {}",
            resp.message.unwrap_or_else(|| "unknown error".into())
        );
    }

    Ok(())
}

/// Extract a quoted path from input, returning (path, remaining_options)
fn extract_quoted_path(input: &str) -> Result<(String, Option<String>)> {
    let input = input.trim();

    /// Helper to extract options from "WITH (...)" clause
    fn extract_options(rest: &str) -> Option<String> {
        if rest.to_uppercase().starts_with("WITH") {
            rest.find('(').and_then(|paren_start| {
                rest.rfind(')').map(|paren_end| rest[paren_start + 1..paren_end].to_string())
            })
        } else {
            None
        }
    }

    // Handle single-quoted path
    if let Some(stripped) = input.strip_prefix('\'') {
        if let Some(end_quote) = stripped.find('\'') {
            let path = stripped[..end_quote].to_string();
            let rest = stripped[end_quote + 1..].trim();
            let options = extract_options(rest);
            return Ok((path, options));
        }
    }

    // Handle double-quoted path
    if let Some(stripped) = input.strip_prefix('"') {
        if let Some(end_quote) = stripped.find('"') {
            let path = stripped[..end_quote].to_string();
            let rest = stripped[end_quote + 1..].trim();
            let options = extract_options(rest);
            return Ok((path, options));
        }
    }

    // Unquoted path (take until whitespace or end)
    let path_end = input.find(char::is_whitespace).unwrap_or(input.len());
    let path = input[..path_end].to_string();
    let rest = input[path_end..].trim();
    let options = extract_options(rest);

    Ok((path, options))
}

/// Expand ~ to home directory in file paths
fn expand_path(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = dirs_next::home_dir() {
            return home.join(stripped).to_string_lossy().to_string();
        }
    }
    path.to_string()
}

/// Get path to views file
fn views_file_path() -> Option<PathBuf> {
    dirs_next::home_dir().map(|h| h.join(".boyodb_views"))
}

/// Load client-side views from disk
fn load_client_views() -> Result<std::collections::HashMap<String, ClientView>> {
    let path = views_file_path().ok_or_else(|| anyhow!("Cannot determine home directory"))?;
    if !path.exists() {
        return Ok(std::collections::HashMap::new());
    }
    let content = std::fs::read_to_string(&path)?;
    let views: Vec<ClientView> = serde_json::from_str(&content)?;
    let map = views.into_iter().map(|v| (v.name.clone(), v)).collect();
    Ok(map)
}

/// Save client-side views to disk
fn save_client_views(views: &std::collections::HashMap<String, ClientView>) -> Result<()> {
    let path = views_file_path().ok_or_else(|| anyhow!("Cannot determine home directory"))?;
    let views_vec: Vec<&ClientView> = views.values().collect();
    let content = serde_json::to_string_pretty(&views_vec)?;
    std::fs::write(&path, content)?;
    Ok(())
}

/// Handle CREATE VIEW statement
/// Syntax: CREATE VIEW name AS SELECT ...
fn handle_create_view(state: &mut ShellState, input: &str) -> Result<()> {
    let input = input.trim().trim_end_matches(';');

    // Parse: CREATE VIEW <name> AS <sql>
    // Also handle: CREATE OR REPLACE VIEW <name> AS <sql>
    let rest = input
        .strip_prefix("CREATE OR REPLACE VIEW ")
        .or_else(|| input.strip_prefix("create or replace view "))
        .or_else(|| input.strip_prefix("CREATE VIEW "))
        .or_else(|| input.strip_prefix("create view "))
        .ok_or_else(|| anyhow!("Invalid CREATE VIEW syntax"))?;

    // Find " AS " separator
    let as_pos = rest.to_uppercase().find(" AS ");
    let as_pos = as_pos.ok_or_else(|| anyhow!("CREATE VIEW syntax: CREATE VIEW <name> AS <select>"))?;

    let name = rest[..as_pos].trim().to_string();
    let sql = rest[as_pos + 4..].trim().to_string();

    if name.is_empty() {
        return Err(anyhow!("View name cannot be empty"));
    }
    if sql.is_empty() || !sql.to_uppercase().starts_with("SELECT") {
        return Err(anyhow!("View definition must be a SELECT statement"));
    }

    let view = ClientView {
        name: name.clone(),
        database: state.current_db.clone(),
        sql,
    };

    state.client_views.insert(name.clone(), view);

    // Persist to disk
    if let Err(e) = save_client_views(&state.client_views) {
        eprintln!("Warning: Failed to save views: {}", e);
    }

    println!("VIEW '{}' created (client-side)", name);
    Ok(())
}

/// Handle DROP VIEW statement
fn handle_drop_view(state: &mut ShellState, input: &str) -> Result<()> {
    let input = input.trim().trim_end_matches(';');

    // Parse: DROP VIEW [IF EXISTS] <name>
    let rest = input
        .strip_prefix("DROP VIEW IF EXISTS ")
        .or_else(|| input.strip_prefix("drop view if exists "))
        .map(|s| (s, true))
        .or_else(|| {
            input
                .strip_prefix("DROP VIEW ")
                .or_else(|| input.strip_prefix("drop view "))
                .map(|s| (s, false))
        })
        .ok_or_else(|| anyhow!("Invalid DROP VIEW syntax"))?;

    let (name, if_exists) = rest;
    let name = name.trim();

    if state.client_views.remove(name).is_some() {
        // Persist to disk
        if let Err(e) = save_client_views(&state.client_views) {
            eprintln!("Warning: Failed to save views: {}", e);
        }
        println!("VIEW '{}' dropped", name);
    } else if if_exists {
        println!("View '{}' does not exist (nothing to drop)", name);
    } else {
        return Err(anyhow!("View '{}' does not exist", name));
    }

    Ok(())
}

/// List all client-side views
fn list_views(state: &ShellState) {
    if state.client_views.is_empty() {
        println!("No client-side views defined");
        println!("Create with: CREATE VIEW name AS SELECT ...");
        return;
    }

    println!("Client-side views:");
    for (name, view) in &state.client_views {
        let db_info = view.database.as_deref().unwrap_or("(any)");
        let sql_preview = if view.sql.len() > 50 {
            format!("{}...", &view.sql[..50])
        } else {
            view.sql.clone()
        };
        println!("  {} [{}]: {}", name, db_info, sql_preview);
    }
}

/// Expand view references in SQL
/// Replaces "FROM view_name" with "FROM (view_sql) AS view_name"
fn expand_views_in_sql(sql: &str, views: &std::collections::HashMap<String, ClientView>) -> String {
    let mut result = sql.to_string();

    for (name, view) in views {
        // Simple replacement: FROM view_name (word boundary)
        // This is a basic implementation; a full SQL parser would be better
        let patterns = [
            format!(" FROM {} ", name),
            format!(" FROM {}\n", name),
            format!(" FROM {};", name),
            format!(" from {} ", name),
            format!(" from {}\n", name),
            format!(" from {};", name),
            format!(" JOIN {} ", name),
            format!(" join {} ", name),
        ];

        for pattern in patterns {
            if result.contains(&pattern) {
                let replacement = pattern.replace(
                    name,
                    &format!("({}) AS {}", view.sql, name)
                );
                result = result.replace(&pattern, &replacement);
            }
        }
    }

    result
}

/// Handle PREPARE statement
/// Syntax: PREPARE name AS sql_statement
/// Parameters are marked with $1, $2, etc.
async fn handle_prepare(state: &mut ShellState, input: &str) -> Result<()> {
    // Parse: PREPARE <name> AS <sql>
    let input = input.trim().trim_end_matches(';');

    // Skip "PREPARE "
    let rest = input.strip_prefix("PREPARE ")
        .or_else(|| input.strip_prefix("prepare "))
        .ok_or_else(|| anyhow!("Invalid PREPARE syntax"))?;

    // Find " AS " separator
    let as_pos = rest.to_uppercase().find(" AS ");
    let as_pos = as_pos.ok_or_else(|| anyhow!("PREPARE syntax: PREPARE <name> AS <sql>"))?;

    let name = rest[..as_pos].trim().to_string();
    let sql = rest[as_pos + 4..].trim().to_string();

    if name.is_empty() {
        return Err(anyhow!("Statement name cannot be empty"));
    }
    if sql.is_empty() {
        return Err(anyhow!("SQL statement cannot be empty"));
    }

    // Count parameters ($1, $2, etc.)
    let param_count = count_parameters(&sql);

    let mut server_id = None;
    if param_count == 0 {
        let resp = send_request(
            state,
            Operation::Prepare {
                sql: sql.clone(),
                database: state.current_db.clone(),
            },
        )
        .await?;
        if resp.status == "ok" {
            server_id = resp.prepared_id.clone();
        } else if let Some(msg) = resp.message.as_ref() {
            eprintln!("Server prepare failed, using local statement: {}", msg);
        }
    }

    let stmt = PreparedStatement {
        name: name.clone(),
        sql,
        param_count,
        server_id,
    };

    state.prepared_statements.insert(name.clone(), stmt);
    if let Some(id) = state.prepared_statements.get(&name).and_then(|s| s.server_id.as_ref()) {
        println!(
            "PREPARE: statement '{}' created (server id {})",
            name, id
        );
    } else {
        println!(
            "PREPARE: statement '{}' created with {} parameter(s)",
            name, param_count
        );
    }

    Ok(())
}

/// Count the number of parameter placeholders ($1, $2, etc.) in SQL
fn count_parameters(sql: &str) -> usize {
    let mut max_param = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let mut num_str = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_ascii_digit() {
                    num_str.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(n) = num_str.parse::<usize>() {
                max_param = max_param.max(n);
            }
        }
    }

    max_param
}

/// Execute a prepared statement
/// Syntax: EXECUTE name (param1, param2, ...)
async fn execute_prepared(state: &ShellState, input: &str) -> Result<()> {
    let input = input.trim().trim_end_matches(';');

    // Skip "EXECUTE "
    let rest = input.strip_prefix("EXECUTE ")
        .or_else(|| input.strip_prefix("execute "))
        .ok_or_else(|| anyhow!("Invalid EXECUTE syntax"))?;

    // Parse name and optional parameters
    let (name, params) = if let Some(paren_pos) = rest.find('(') {
        let name = rest[..paren_pos].trim();
        let params_str = rest[paren_pos..].trim();

        // Parse parameters: (val1, val2, ...)
        let params = parse_execute_params(params_str)?;
        (name.to_string(), params)
    } else {
        (rest.trim().to_string(), vec![])
    };

    // Find the prepared statement
    let stmt = state.prepared_statements.get(&name)
        .ok_or_else(|| anyhow!("Prepared statement '{}' not found", name))?;

    // Validate parameter count
    if params.len() != stmt.param_count {
        return Err(anyhow!(
            "Statement '{}' requires {} parameter(s), but {} provided",
            name, stmt.param_count, params.len()
        ));
    }

    let start = Instant::now();
    let resp = if stmt.param_count == 0 {
        if let Some(id) = stmt.server_id.as_ref() {
            send_request(
                state,
                Operation::ExecutePreparedBinary {
                    id: id.clone(),
                    timeout_millis: state.timeout_millis,
                    stream: true,
                },
            )
            .await?
        } else {
            send_request(
                state,
                build_query_operation(
                    stmt.sql.clone(),
                    state.timeout_millis,
                    state.current_db.clone(),
                ),
            )
            .await?
        }
    } else {
        // Substitute parameters client-side
        let sql = substitute_parameters(&stmt.sql, &params);
        send_request(
            state,
            build_query_operation(sql, state.timeout_millis, state.current_db.clone()),
        )
        .await?
    };

    let elapsed = start.elapsed();

    if resp.status != "ok" {
        eprintln!("Error: {}", resp.message.unwrap_or_else(|| "unknown error".into()));
        return Ok(());
    }

    // Handle different response types
    if let Some(ipc_bytes) = response_ipc_bytes(&resp)? {
        let opts = DisplayOptions {
            format: state.output_format,
            output_file: state.output_file.as_ref(),
            use_pager: state.use_pager,
        };
        display_arrow_results(&ipc_bytes, &resp, elapsed, &opts)?;
    } else if let Some(msg) = resp.message {
        println!("{}", msg);
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    } else {
        println!("OK");
        println!("Time: {:.3}s", elapsed.as_secs_f64());
    }

    Ok(())
}

/// Parse parameters from EXECUTE (val1, val2, ...)
fn parse_execute_params(params_str: &str) -> Result<Vec<String>> {
    let params_str = params_str.trim();
    if !params_str.starts_with('(') || !params_str.ends_with(')') {
        return Err(anyhow!("Parameters must be enclosed in parentheses"));
    }

    let inner = &params_str[1..params_str.len()-1];
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }

    // Simple comma-separated parsing (handles basic cases)
    // For more complex cases with quoted strings containing commas, would need more sophisticated parsing
    let mut params = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = ' ';

    for c in inner.chars() {
        match c {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = c;
                current.push(c);
            }
            c if c == quote_char && in_quotes => {
                in_quotes = false;
                current.push(c);
            }
            ',' if !in_quotes => {
                params.push(current.trim().to_string());
                current = String::new();
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.trim().is_empty() {
        params.push(current.trim().to_string());
    }

    Ok(params)
}

/// Substitute parameters ($1, $2, etc.) with actual values
fn substitute_parameters(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();

    // Replace from highest to lowest to avoid $1 replacing part of $10
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        result = result.replace(&placeholder, param);
    }

    result
}

/// Handle DEALLOCATE statement
/// Syntax: DEALLOCATE name or DEALLOCATE ALL
fn handle_deallocate(state: &mut ShellState, input: &str) -> Result<()> {
    let input = input.trim().trim_end_matches(';');

    let name = input.strip_prefix("DEALLOCATE ")
        .or_else(|| input.strip_prefix("deallocate "))
        .ok_or_else(|| anyhow!("Invalid DEALLOCATE syntax"))?
        .trim();

    if name.eq_ignore_ascii_case("all") {
        let count = state.prepared_statements.len();
        state.prepared_statements.clear();
        println!("DEALLOCATE: {} statement(s) removed", count);
    } else if state.prepared_statements.remove(name).is_some() {
        println!("DEALLOCATE: statement '{}' removed", name);
    } else {
        eprintln!("Prepared statement '{}' not found", name);
    }

    Ok(())
}

/// Display Arrow IPC results in the specified format
/// Display output with optional paging for large results
fn display_with_pager(output: &str, use_pager: bool) {
    // Only use pager if output is large enough and we're in a terminal
    let lines: Vec<&str> = output.lines().collect();
    let terminal_height = terminal_size::terminal_size()
        .map(|(_, h)| h.0 as usize)
        .unwrap_or(24);

    if use_pager && lines.len() > terminal_height.saturating_sub(3) {
        // Try to use less, then more, then just print
        use std::process::{Command, Stdio};
        use std::io::Write;

        let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
        let pager_cmd = if pager.contains("less") { "less" } else { &pager };

        match Command::new(pager_cmd)
            .arg("-R") // Enable color output in less
            .stdin(Stdio::piped())
            .spawn()
        {
            Ok(mut child) => {
                if let Some(stdin) = child.stdin.as_mut() {
                    let _ = stdin.write_all(output.as_bytes());
                }
                let _ = child.wait();
            }
            Err(_) => {
                // Fallback: try 'more'
                match Command::new("more")
                    .stdin(Stdio::piped())
                    .spawn()
                {
                    Ok(mut child) => {
                        if let Some(stdin) = child.stdin.as_mut() {
                            let _ = stdin.write_all(output.as_bytes());
                        }
                        let _ = child.wait();
                    }
                    Err(_) => {
                        // Just print directly
                        print!("{}", output);
                    }
                }
            }
        }
    } else {
        print!("{}", output);
    }
}

fn display_arrow_results(ipc_bytes: &[u8], resp: &Response, elapsed: Duration, opts: &DisplayOptions) -> Result<()> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)?;

    let mut total_rows = 0;
    let mut all_rows: Vec<Vec<String>> = Vec::new();
    let mut headers: Vec<String> = Vec::new();

    for batch_result in reader {
        let batch: RecordBatch = batch_result?;

        // Get headers from schema
        if headers.is_empty() {
            headers = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
        }

        // Collect rows
        let num_rows = batch.num_rows();
        total_rows += num_rows;

        for row_idx in 0..num_rows {
            let mut row: Vec<String> = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let value = format_array_value(col, row_idx);
                row.push(value);
            }
            all_rows.push(row);
        }
    }

    // Build output string
    let mut output = String::new();
    use std::fmt::Write;

    // Output based on format
    match opts.format {
        OutputFormat::Table => {
            if !headers.is_empty() {
                let mut table = Table::new();
                table.set_header(headers);
                for row in all_rows {
                    table.add_row(row.iter().map(Cell::new).collect::<Vec<_>>());
                }
                writeln!(output, "{}", table)?;
            }
        }
        OutputFormat::Csv => {
            // Print CSV header
            writeln!(output, "{}", headers.join(","))?;
            // Print rows
            for row in all_rows {
                let csv_row: Vec<String> = row.iter().map(|v| escape_csv(v)).collect();
                writeln!(output, "{}", csv_row.join(","))?;
            }
        }
        OutputFormat::Json => {
            let json_rows: Vec<serde_json::Value> = all_rows
                .iter()
                .map(|row| {
                    let mut obj = serde_json::Map::new();
                    for (i, val) in row.iter().enumerate() {
                        if let Some(header) = headers.get(i) {
                            // Try to parse as number, otherwise use string
                            let json_val = if val == "NULL" {
                                serde_json::Value::Null
                            } else if let Ok(n) = val.parse::<i64>() {
                                serde_json::Value::Number(n.into())
                            } else if let Ok(f) = val.parse::<f64>() {
                                serde_json::Number::from_f64(f)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::String(val.clone()))
                            } else if val == "true" {
                                serde_json::Value::Bool(true)
                            } else if val == "false" {
                                serde_json::Value::Bool(false)
                            } else {
                                serde_json::Value::String(val.clone())
                            };
                            obj.insert(header.clone(), json_val);
                        }
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
            writeln!(output, "{}", serde_json::to_string_pretty(&json_rows)?)?;
        }
        OutputFormat::Vertical => {
            // Vertical format: one column per line, like psql's \x mode
            // Find max header length for alignment
            let max_header_len = headers.iter().map(|h| h.len()).max().unwrap_or(0);

            for (row_num, row) in all_rows.iter().enumerate() {
                writeln!(output, "-[ RECORD {} ]{}",
                    row_num + 1,
                    "-".repeat(50_usize.saturating_sub(13 + row_num.to_string().len())))?;
                for (i, val) in row.iter().enumerate() {
                    if let Some(header) = headers.get(i) {
                        writeln!(output, "{:>width$} | {}", header, val, width = max_header_len)?;
                    }
                }
            }
        }
    }

    // Print stats
    let mut stats = format!("{} row(s)", total_rows);
    if let Some(segs) = resp.segments_scanned {
        stats.push_str(&format!(", {} segment(s) scanned", segs));
    }
    if let Some(skipped) = resp.data_skipped_bytes {
        if skipped > 0 {
            stats.push_str(&format!(", {} bytes skipped", skipped));
        }
    }
    writeln!(output, "{}", stats)?;
    writeln!(output, "Time: {:.3}s", elapsed.as_secs_f64())?;

    // Output to file, pager, or stdout
    if let Some(file_path) = opts.output_file {
        // Write to file
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)?;
        file.write_all(output.as_bytes())?;
    } else {
        // Output to stdout, optionally through pager
        display_with_pager(&output, opts.use_pager);
    }

    Ok(())
}

/// Escape a value for CSV output
fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Truncate SQL for display
fn truncate_sql(sql: &str, max_len: usize) -> String {
    let sql = sql.replace('\n', " ").replace("  ", " ");
    if sql.len() > max_len {
        format!("{}...", &sql[..max_len])
    } else {
        sql
    }
}

/// Format a single value from an Arrow array
fn format_array_value(array: &dyn arrow_array::Array, idx: usize) -> String {
    use arrow_array::*;
    use arrow_schema::DataType;

    if array.is_null(idx) {
        return "NULL".to_string();
    }

    macro_rules! format_primitive {
        ($array_type:ty) => {
            if let Some(arr) = array.as_any().downcast_ref::<$array_type>() {
                return arr.value(idx).to_string();
            }
        };
    }

    format_primitive!(Int8Array);
    format_primitive!(Int16Array);
    format_primitive!(Int32Array);
    format_primitive!(Int64Array);
    format_primitive!(UInt8Array);
    format_primitive!(UInt16Array);
    format_primitive!(UInt32Array);
    format_primitive!(UInt64Array);
    format_primitive!(Float32Array);
    format_primitive!(Float64Array);
    format_primitive!(StringArray);
    format_primitive!(LargeStringArray);
    format_primitive!(BooleanArray);

    // UUID stored as FixedSizeBinary(16)
    if let DataType::FixedSizeBinary(16) = array.data_type() {
        if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            let bytes = arr.value(idx);
            return format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            );
        }
    }

    // Other FixedSizeBinary as hex
    if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let bytes = arr.value(idx);
        return format!("0x{}", hex::encode(bytes));
    }

    // Binary and LargeBinary as hex
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return format!("0x{}", hex::encode(arr.value(idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return format!("0x{}", hex::encode(arr.value(idx)));
    }

    // Decimal128
    if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
        let value = arr.value(idx);
        let scale = arr.scale();
        return format_decimal_value(value, scale);
    }

    // Date32 - format as YYYY-MM-DD
    if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
        let days = arr.value(idx);
        return format_date_value(days);
    }

    // Timestamp (various time units)
    if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return arr.value(idx).to_string();
    }

    // Fallback
    format!("{:?}", array.slice(idx, 1))
}

/// Format a decimal value with scale
fn format_decimal_value(value: i128, scale: i8) -> String {
    if scale <= 0 {
        format!("{}", value * 10_i128.pow((-scale) as u32))
    } else {
        let divisor = 10_i128.pow(scale as u32);
        let integer_part = value / divisor;
        let fractional_part = (value % divisor).abs();
        let sign = if value < 0 && integer_part == 0 { "-" } else { "" };
        format!("{}{}.{:0>width$}", sign, integer_part, fractional_part, width = scale as usize)
    }
}

/// Format date (days since Unix epoch) as YYYY-MM-DD
fn format_date_value(days: i32) -> String {
    let epoch = 719468; // Days from year 0 to 1970-01-01
    let z = days as i64 + epoch;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", year, m, d)
}

/// Build a TLS connector from configuration
fn build_tls_connector(config: &TlsConfig) -> Result<TlsConnector> {
    use rustls::ClientConfig;
    use rustls::pki_types::CertificateDer;

    let root_store = if let Some(ca_path) = &config.ca_path {
        // Load custom CA certificate
        let ca_file = std::fs::File::open(ca_path)
            .map_err(|e| anyhow!("Failed to open CA file {}: {}", ca_path, e))?;
        let mut ca_reader = std::io::BufReader::new(ca_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
            .filter_map(|r| r.ok())
            .collect();

        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store.add(cert)
                .map_err(|e| anyhow!("Failed to add CA certificate: {}", e))?;
        }
        root_store
    } else {
        // Use system/webpki root certificates
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        root_store
    };

    let client_config = if config.skip_verify {
        // Insecure: skip certificate verification
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    } else {
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    Ok(TlsConnector::from(Arc::new(client_config)))
}

/// Certificate verifier that accepts any certificate (insecure, for testing)
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Send a request to the server with retry logic
async fn send_request(state: &ShellState, op: Operation) -> Result<Response> {
    let mut last_error = None;

    for attempt in 1..=MAX_RETRY_ATTEMPTS {
        match send_request_once(state, &op).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                // Check if it's a connection error worth retrying
                let err_str = e.to_string();
                let is_connection_error = err_str.contains("Connection refused")
                    || err_str.contains("Connection reset")
                    || err_str.contains("Connection failed")
                    || err_str.contains("timed out")
                    || err_str.contains("broken pipe");

                if is_connection_error && attempt < MAX_RETRY_ATTEMPTS {
                    eprintln!(
                        "Connection error (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt, MAX_RETRY_ATTEMPTS, e, RETRY_DELAY_MS
                    );
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    last_error = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Connection failed after {} attempts", MAX_RETRY_ATTEMPTS)))
}

/// Send a single request to the server (without retry)
async fn send_request_once(state: &ShellState, op: &Operation) -> Result<Response> {
    let req = Request {
        auth: state.auth_token().map(|s| s.to_string()),
        op: op.clone(),
    };

    let json = serde_json::to_vec(&req)?;

    // Connect with timeout
    let connect_timeout = Duration::from_secs(10);
    let tcp_stream = tokio::time::timeout(
        connect_timeout,
        TcpStream::connect(&state.host)
    )
    .await
    .map_err(|_| anyhow!("Connection timed out after {:?}", connect_timeout))?
    .map_err(|e| anyhow!("Connection failed: {}", e))?;

    // If TLS is configured, wrap the connection
    if let Some(ref connector) = state.tls_connector {
        // Extract hostname from host:port
        let hostname = state.host.split(':').next().unwrap_or(&state.host);
        let server_name = ServerName::try_from(hostname.to_string())
            .map_err(|_| anyhow!("Invalid server name: {}", hostname))?;

        let mut tls_stream = connector.connect(server_name, tcp_stream).await
            .map_err(|e| anyhow!("TLS handshake failed: {}", e))?;

        // Send length-prefixed frame
        let len = json.len() as u32;
        tls_stream.write_all(&len.to_be_bytes()).await?;
        tls_stream.write_all(&json).await?;
        tls_stream.flush().await?;

        let resp_bytes = read_frame_bytes(&mut tls_stream).await?;
        let mut resp: Response = serde_json::from_slice(&resp_bytes)
            .map_err(|e| anyhow!("Invalid response: {} - body: {}", e, String::from_utf8_lossy(&resp_bytes)))?;
        if resp.ipc_streaming.unwrap_or(false) {
            let payload = read_stream_bytes(&mut tls_stream).await?;
            resp.ipc_bytes = Some(payload);
        } else if let Some(ipc_len) = resp.ipc_len {
            let payload = read_frame_bytes(&mut tls_stream).await?;
            if payload.len() as u64 != ipc_len {
                return Err(anyhow!(
                    "IPC length mismatch: expected {}, got {}",
                    ipc_len,
                    payload.len()
                ));
            }
            resp.ipc_bytes = Some(payload);
        }
        Ok(resp)
    } else {
        // Plain TCP connection
        let mut stream = tcp_stream;

        // Send length-prefixed frame
        let len = json.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&json).await?;
        stream.flush().await?;

        let resp_bytes = read_frame_bytes(&mut stream).await?;
        let mut resp: Response = serde_json::from_slice(&resp_bytes)
            .map_err(|e| anyhow!("Invalid response: {} - body: {}", e, String::from_utf8_lossy(&resp_bytes)))?;
        if resp.ipc_streaming.unwrap_or(false) {
            let payload = read_stream_bytes(&mut stream).await?;
            resp.ipc_bytes = Some(payload);
        } else if let Some(ipc_len) = resp.ipc_len {
            let payload = read_frame_bytes(&mut stream).await?;
            if payload.len() as u64 != ipc_len {
                return Err(anyhow!(
                    "IPC length mismatch: expected {}, got {}",
                    ipc_len,
                    payload.len()
                ));
            }
            resp.ipc_bytes = Some(payload);
        }
        Ok(resp)
    }
}

async fn read_frame_bytes<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    if resp_len > 100 * 1024 * 1024 {
        return Err(anyhow!("Response too large: {} bytes", resp_len));
    }
    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await?;
    Ok(resp_buf)
}

async fn read_stream_bytes<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    loop {
        let chunk = read_frame_bytes(stream).await?;
        if chunk.is_empty() {
            break;
        }
        payload.extend_from_slice(&chunk);
    }
    Ok(payload)
}

/// Connection retry configuration
const MAX_RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY_MS: u64 = 1000;

fn print_help() {
    println!(
        r#"
boyodb shell commands:

  Authentication:
    login [username]    Log in to the server (prompts for password)
    logout              Log out from the server
    whoami              Show current user

  SQL Commands:
    SELECT ...          Execute a query
    EXPLAIN SELECT ...  Show query execution plan
    CREATE DATABASE x   Create a new database
    CREATE TABLE x.y    Create a new table
    DROP DATABASE x     Drop a database
    DROP TABLE x.y      Drop a table
    TRUNCATE TABLE x.y  Truncate a table
    ALTER TABLE x.y ADD COLUMN col type [NULL]   Add a nullable column
    ALTER TABLE x.y DROP COLUMN col              Drop a column
    INSERT INTO ...     Insert rows into a table
    UPDATE ...          Update rows
    DELETE FROM ...     Delete rows
    SHOW USERS          List users (requires auth)
    SHOW ROLES          List roles (requires auth)
    SHOW GRANTS FOR x   Show grants for a user
    GRANT ...           Grant privileges
    REVOKE ...          Revoke privileges

  Prepared Statements:
    PREPARE name AS sql   Create a prepared statement with $1, $2, ... params
    EXECUTE name (args)   Execute a prepared statement with arguments
    DEALLOCATE name       Remove a prepared statement
    DEALLOCATE ALL        Remove all prepared statements

  Client-Side Views:
    CREATE VIEW name AS SELECT ...    Create a local view (stored in ~/.boyodb_views)
    CREATE OR REPLACE VIEW name AS... Replace or create a view
    DROP VIEW name                    Remove a view
    DROP VIEW IF EXISTS name          Remove a view if it exists
    \views, \dv                       List all client-side views

  Shell Commands:
    \l                  List databases
    \dt [database]      List tables
    \d <table>          Describe a table (show schema, segments, size)
    \du                 List users and roles
    \di                 List indexes
    \dv, \views         List client-side views
    \c <database>       Switch to a database (USE <database>)
    \x                  Toggle expanded/vertical output (like psql)
    \o [file]           Redirect output to file (\o - to reset to stdout)
    \i <file>           Execute SQL from file
    \ps                 List prepared statements
    \format [fmt]       Set output format: table, csv, json, vertical
    \pager [on|off]     Toggle pager for large output
    \history [N]        Show last N history entries (default: 20)
    \history search X   Search history for pattern X
    \copy <tbl> FROM 'file' [WITH (opts)]  Import CSV file into table
    \q                  Quit (also: quit, exit)
    \?                  Show this help (also: help, \h)
    clear               Clear screen
    status              Show server status and login info

  Keyboard Shortcuts:
    Ctrl+R              Reverse history search (like bash)
    Ctrl+S              Forward history search
    Up/Down             Navigate history
    Ctrl+C              Cancel current line / multi-line input
    Ctrl+D              Exit shell

  Multi-line Input:
    SQL statements can span multiple lines. Press Enter to continue
    on a new line (shown with '   -> ' prompt). The statement executes
    when you type a semicolon (;) at the end. Example:
      boyodb> SELECT id, name
         -> FROM users
         -> WHERE active = true;

  CSV Import (\copy):
    \copy <table> FROM '<file>' [WITH (options)]
    Options:
      DELIMITER '<char>'  - Field delimiter (default: ',')
      NO HEADER           - File has no header row (default: has header)
    Examples:
      \copy users FROM '/data/users.csv'
      \copy mydb.events FROM '~/events.csv' WITH (DELIMITER '|')
      \copy logs FROM 'data.csv' WITH (NO HEADER)

  Examples:
    login admin
    SELECT * FROM mydb.calls LIMIT 10;
    SELECT COUNT(*) FROM mydb.calls WHERE tenant_id = 42;
    CREATE DATABASE analytics;
    \c analytics
    SELECT * FROM calls;
    \copy calls FROM 'calls.csv'
    PREPARE q1 AS SELECT * FROM calls WHERE tenant_id = $1;
    EXECUTE q1 (42);
    \format json
    EXECUTE q1 (100);
    CREATE VIEW recent_calls AS SELECT * FROM calls WHERE ts > now() - 3600;
    SELECT * FROM recent_calls LIMIT 10;
    \views
    DROP VIEW recent_calls;
    logout
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_statement_complete() {
        // Complete statements
        assert!(is_statement_complete("SELECT * FROM users;"));
        assert!(is_statement_complete("SELECT * FROM users WHERE id = 1;"));
        assert!(is_statement_complete("  SELECT * FROM users;  "));

        // Incomplete statements (no semicolon)
        assert!(!is_statement_complete("SELECT * FROM users"));
        assert!(!is_statement_complete("SELECT * FROM users WHERE id = 1"));

        // Semicolon inside string literal should not count
        assert!(!is_statement_complete("SELECT 'foo;' FROM users"));
        assert!(!is_statement_complete("SELECT \"bar;\" FROM users"));

        // Complete with semicolon after string literal
        assert!(is_statement_complete("SELECT 'foo;' FROM users;"));
        assert!(is_statement_complete("SELECT \"bar;\" FROM users;"));

        // Empty input
        assert!(!is_statement_complete(""));
        assert!(!is_statement_complete("   "));
    }

    #[test]
    fn test_is_immediate_command() {
        // Immediate commands
        assert!(is_immediate_command("help"));
        assert!(is_immediate_command("quit"));
        assert!(is_immediate_command("exit"));
        assert!(is_immediate_command("status"));
        assert!(is_immediate_command("login"));
        assert!(is_immediate_command("logout"));
        assert!(is_immediate_command("whoami"));
        assert!(is_immediate_command("clear"));
        assert!(is_immediate_command("use testdb"));
        assert!(is_immediate_command("history"));

        // Non-immediate commands (need semicolon)
        assert!(!is_immediate_command("SELECT * FROM users"));
        assert!(!is_immediate_command("CREATE DATABASE test"));
        assert!(!is_immediate_command("INSERT INTO users VALUES (1)"));
    }

    #[test]
    fn test_count_parameters() {
        assert_eq!(count_parameters("SELECT * FROM users"), 0);
        assert_eq!(count_parameters("SELECT * FROM users WHERE id = $1"), 1);
        assert_eq!(count_parameters("SELECT * FROM users WHERE id = $1 AND name = $2"), 2);
        assert_eq!(count_parameters("SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3"), 3);
        assert_eq!(count_parameters("SELECT $10 + $1"), 10); // Max parameter number
    }

    #[test]
    fn test_substitute_parameters() {
        assert_eq!(
            substitute_parameters("SELECT * FROM users WHERE id = $1", &["42".to_string()]),
            "SELECT * FROM users WHERE id = 42"
        );
        assert_eq!(
            substitute_parameters(
                "SELECT * FROM users WHERE id = $1 AND name = $2",
                &["42".to_string(), "'Alice'".to_string()]
            ),
            "SELECT * FROM users WHERE id = 42 AND name = 'Alice'"
        );
        // Test that $10 doesn't get partially replaced by $1
        assert_eq!(
            substitute_parameters(
                "SELECT $1, $10",
                &["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string(),
                  "e".to_string(), "f".to_string(), "g".to_string(), "h".to_string(),
                  "i".to_string(), "j".to_string()]
            ),
            "SELECT a, j"
        );
    }

    #[test]
    fn test_escape_csv() {
        assert_eq!(escape_csv("hello"), "hello");
        assert_eq!(escape_csv("hello,world"), "\"hello,world\"");
        assert_eq!(escape_csv("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(escape_csv("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn test_expand_path() {
        // Can't test ~ expansion without knowing home dir, but test passthrough
        assert_eq!(expand_path("/absolute/path"), "/absolute/path");
        assert_eq!(expand_path("relative/path"), "relative/path");
    }

    #[test]
    fn test_expand_views_in_sql() {
        let mut views = std::collections::HashMap::new();
        views.insert(
            "active_users".to_string(),
            ClientView {
                name: "active_users".to_string(),
                database: None,
                sql: "SELECT * FROM users WHERE active = true".to_string(),
            },
        );

        // Test FROM expansion
        let sql = "SELECT * FROM active_users WHERE id = 1";
        let expanded = expand_views_in_sql(sql, &views);
        assert!(expanded.contains("(SELECT * FROM users WHERE active = true) AS active_users"));

        // Test that non-matching tables are not affected
        let sql2 = "SELECT * FROM other_table WHERE id = 1";
        let expanded2 = expand_views_in_sql(sql2, &views);
        assert_eq!(sql2, expanded2);
    }

    #[test]
    fn test_toml_parse() {
        let config_str = r#"
            host = "localhost:9999"
            database = "mydb"
            tls = true
            timeout_ms = 5000
            # This is a comment
            format = "json"
        "#;

        let config = toml_parse(config_str).unwrap();
        assert_eq!(config.host, Some("localhost:9999".to_string()));
        assert_eq!(config.database, Some("mydb".to_string()));
        assert_eq!(config.tls, Some(true));
        assert_eq!(config.timeout_ms, Some(5000));
        assert_eq!(config.format, Some("json".to_string()));
    }

    #[test]
    fn test_output_format() {
        let mut config = ShellConfig::default();
        assert_eq!(config.output_format(), OutputFormat::Table);

        config.format = Some("csv".to_string());
        assert_eq!(config.output_format(), OutputFormat::Csv);

        config.format = Some("json".to_string());
        assert_eq!(config.output_format(), OutputFormat::Json);

        config.format = Some("invalid".to_string());
        assert_eq!(config.output_format(), OutputFormat::Table); // Falls back to table
    }

    #[test]
    fn test_extract_quoted_path() {
        // Single quoted path
        let (path, opts) = extract_quoted_path("'/path/to/file.csv'").unwrap();
        assert_eq!(path, "/path/to/file.csv");
        assert!(opts.is_none());

        // With options
        let (path, opts) = extract_quoted_path("'/data/file.csv' WITH (DELIMITER '|')").unwrap();
        assert_eq!(path, "/data/file.csv");
        assert_eq!(opts, Some("DELIMITER '|'".to_string()));

        // Double quoted path
        let (path, opts) = extract_quoted_path("\"/path/to/file.csv\"").unwrap();
        assert_eq!(path, "/path/to/file.csv");
        assert!(opts.is_none());

        // Unquoted path
        let (path, opts) = extract_quoted_path("/simple/path.csv WITH (NO HEADER)").unwrap();
        assert_eq!(path, "/simple/path.csv");
        assert_eq!(opts, Some("NO HEADER".to_string()));
    }

    #[test]
    fn test_parse_execute_params() {
        // Empty params
        let params = parse_execute_params("()").unwrap();
        assert!(params.is_empty());

        // Single param
        let params = parse_execute_params("(42)").unwrap();
        assert_eq!(params, vec!["42"]);

        // Multiple params
        let params = parse_execute_params("(1, 'hello', 3.14)").unwrap();
        assert_eq!(params, vec!["1", "'hello'", "3.14"]);

        // Quoted strings with commas inside
        let params = parse_execute_params("('hello, world', 42)").unwrap();
        assert_eq!(params, vec!["'hello, world'", "42"]);
    }

    #[test]
    fn test_truncate_sql() {
        assert_eq!(truncate_sql("short", 10), "short");
        assert_eq!(truncate_sql("this is a very long sql statement", 10), "this is a ...");
        assert_eq!(truncate_sql("line1\nline2", 20), "line1 line2");
    }
}
