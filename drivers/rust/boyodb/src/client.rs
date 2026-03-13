use crate::config::Config;
use crate::error::Error;
use crate::result::{parse_arrow_ipc, QueryResult, Response, TableInfo};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;

#[cfg(feature = "tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "tls")]
use std::fs::File;
#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;

/// A stream that can be either plain TCP or TLS-encrypted.
enum StreamKind {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl StreamKind {
    async fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            StreamKind::Plain(s) => s.shutdown().await,
            #[cfg(feature = "tls")]
            StreamKind::Tls(s) => {
                use tokio::io::AsyncWriteExt;
                s.shutdown().await
            }
        }
    }
}

/// boyodb client for Rust.
pub struct Client {
    stream: Arc<Mutex<StreamKind>>,
    config: Config,
    session_id: Arc<Mutex<Option<String>>>,
}

impl Client {
    /// Connect to the boyodb server.
    pub async fn connect(host: &str, config: Config) -> Result<Self, Error> {
        let client = Self::connect_with_retry(host, &config).await?;

        // Verify with health check
        client.health().await.map_err(|e| {
            Error::Connection(format!("Health check failed: {}", e))
        })?;

        Ok(client)
    }

    async fn connect_with_retry(host: &str, config: &Config) -> Result<Self, Error> {
        let mut last_error = None;

        for attempt in 0..config.max_retries {
            match Self::connect_once(host, config).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < config.max_retries - 1 {
                        tokio::time::sleep(config.retry_delay).await;
                    }
                }
            }
        }

        Err(Error::Connection(format!(
            "Failed to connect after {} attempts: {:?}",
            config.max_retries, last_error
        )))
    }

    async fn connect_once(host: &str, config: &Config) -> Result<Self, Error> {
        let (hostname, port) = parse_host(host);

        let tcp_stream = timeout(
            config.connect_timeout,
            TcpStream::connect(format!("{}:{}", hostname, port)),
        )
        .await
        .map_err(|_| Error::Timeout("Connection timeout".into()))?
        .map_err(|e| Error::Connection(format!("Failed to connect: {}", e)))?;

        let stream = if config.tls {
            #[cfg(feature = "tls")]
            {
                Self::wrap_tls(tcp_stream, hostname, config).await?
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Error::Connection(
                    "TLS requested but 'tls' feature is not enabled. \
                     Add `features = [\"tls\"]` to your Cargo.toml dependency.".into()
                ));
            }
        } else {
            StreamKind::Plain(tcp_stream)
        };

        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            config: config.clone(),
            session_id: Arc::new(Mutex::new(None)),
        })
    }

    #[cfg(feature = "tls")]
    async fn wrap_tls(tcp_stream: TcpStream, hostname: &str, config: &Config) -> Result<StreamKind, Error> {
        use rustls::RootCertStore;

        let mut root_store = RootCertStore::empty();

        // Load custom CA certificate if provided
        if let Some(ref ca_file) = config.ca_file {
            let file = File::open(ca_file)
                .map_err(|e| Error::Connection(format!("Failed to open CA file: {}", e)))?;
            let mut reader = BufReader::new(file);

            let certs = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Error::Connection(format!("Failed to parse CA certificates: {}", e)))?;

            for cert in certs {
                root_store.add(cert)
                    .map_err(|e| Error::Connection(format!("Failed to add CA certificate: {}", e)))?;
            }
        } else {
            // Use webpki roots (Mozilla's root certificates)
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let tls_config = if config.insecure_skip_verify {
            eprintln!(
                "WARNING: TLS certificate verification is DISABLED. \
                 This is insecure and vulnerable to MITM attacks. \
                 Only use for testing with self-signed certificates."
            );

            // Create a custom verifier that accepts any certificate
            let verifier = Arc::new(InsecureServerCertVerifier);

            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth()
        } else {
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        let connector = TlsConnector::from(Arc::new(tls_config));

        let server_name = ServerName::try_from(hostname.to_string())
            .map_err(|_| Error::Connection(format!("Invalid hostname for TLS: {}", hostname)))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| Error::Connection(format!("TLS handshake failed: {}", e)))?;

        Ok(StreamKind::Tls(tls_stream))
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<(), Error> {
        let mut stream = self.stream.lock().await;
        stream.shutdown().await?;
        Ok(())
    }

    async fn send_request(&self, mut request: Value) -> Result<Response, Error> {
        let mut stream = self.stream.lock().await;

        // Add auth if available
        let session_id = self.session_id.lock().await;
        if let Some(ref sid) = *session_id {
            request["auth"] = json!(sid);
        } else if let Some(ref token) = self.config.token {
            request["auth"] = json!(token);
        }
        drop(session_id);

        // Serialize request
        let json_bytes = serde_json::to_vec(&request)?;

        // Write length prefix (big-endian) and payload
        let len = json_bytes.len() as u32;
        let len_bytes = len.to_be_bytes();

        match &mut *stream {
            StreamKind::Plain(s) => {
                s.write_all(&len_bytes).await?;
                s.write_all(&json_bytes).await?;
                s.flush().await?;
            }
            #[cfg(feature = "tls")]
            StreamKind::Tls(s) => {
                s.write_all(&len_bytes).await?;
                s.write_all(&json_bytes).await?;
                s.flush().await?;
            }
        }

        let resp_buf = read_frame(&mut *stream).await?;
        let mut response: Response = serde_json::from_slice(&resp_buf)?;
        if response.ipc_streaming.unwrap_or(false) {
            let payload = read_stream_frames(&mut *stream).await?;
            response.ipc_bytes = Some(payload);
        } else if let Some(ipc_len) = response.ipc_len {
            let payload = read_frame(&mut *stream).await?;
            if payload.len() as u64 != ipc_len {
                return Err(Error::Query(format!(
                    "IPC length mismatch: expected {} got {}",
                    ipc_len,
                    payload.len()
                )));
            }
            response.ipc_bytes = Some(payload);
        }
        Ok(response)
    }

    async fn send_request_binary(&self, mut request: Value, payload: &[u8]) -> Result<(), Error> {
        let mut stream = self.stream.lock().await;

        let session_id = self.session_id.lock().await;
        if let Some(ref sid) = *session_id {
            request["auth"] = json!(sid);
        } else if let Some(ref token) = self.config.token {
            request["auth"] = json!(token);
        }
        drop(session_id);

        let json_bytes = serde_json::to_vec(&request)?;
        let len = json_bytes.len() as u32;
        let len_bytes = len.to_be_bytes();

        match &mut *stream {
            StreamKind::Plain(s) => {
                s.write_all(&len_bytes).await?;
                s.write_all(&json_bytes).await?;
            }
            #[cfg(feature = "tls")]
            StreamKind::Tls(s) => {
                s.write_all(&len_bytes).await?;
                s.write_all(&json_bytes).await?;
            }
        }

        let payload_len = payload.len() as u32;
        let payload_len_bytes = payload_len.to_be_bytes();
        match &mut *stream {
            StreamKind::Plain(s) => {
                s.write_all(&payload_len_bytes).await?;
                s.write_all(payload).await?;
                s.flush().await?;
            }
            #[cfg(feature = "tls")]
            StreamKind::Tls(s) => {
                s.write_all(&payload_len_bytes).await?;
                s.write_all(payload).await?;
                s.flush().await?;
            }
        }

        let resp_buf = read_frame(&mut *stream).await?;
        let response: Response = serde_json::from_slice(&resp_buf)?;
        Self::check_status(&response, "Ingest IPC failed")?;
        Ok(())
    }

    fn check_status(response: &Response, error_prefix: &str) -> Result<(), Error> {
        if response.status != "ok" {
            let msg = response.message.as_deref().unwrap_or("Unknown error");
            return Err(Error::Query(format!("{}: {}", error_prefix, msg)));
        }
        Ok(())
    }

    /// Check server health.
    pub async fn health(&self) -> Result<(), Error> {
        let response = self.send_request(json!({"op": "health"})).await?;
        Self::check_status(&response, "Health check failed")
    }

    /// Login with username and password.
    pub async fn login(&self, username: &str, password: &str) -> Result<(), Error> {
        let response = self
            .send_request(json!({
                "op": "login",
                "username": username,
                "password": password
            }))
            .await?;

        Self::check_status(&response, "Login failed")?;

        if let Some(sid) = response.session_id {
            let mut session = self.session_id.lock().await;
            *session = Some(sid);
        }

        Ok(())
    }

    /// Logout from the server.
    pub async fn logout(&self) -> Result<(), Error> {
        let response = self.send_request(json!({"op": "logout"})).await?;
        Self::check_status(&response, "Logout failed")?;

        let mut session = self.session_id.lock().await;
        *session = None;

        Ok(())
    }

    /// Execute a SQL query.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, Error> {
        self.query_with_options(sql, None, None).await
    }

    /// Execute a SQL query with options.
    pub async fn query_with_options(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout_millis: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let mut request = json!({
            "op": if Self::is_select_like(sql) { "query_binary" } else { "query" },
            "sql": sql,
            "timeout_millis": timeout_millis.unwrap_or(self.config.query_timeout)
        });
        if Self::is_select_like(sql) {
            request["stream"] = json!(true);
        }

        let db = database.or(self.config.database.as_deref());
        if let Some(db) = db {
            request["database"] = json!(db);
        }

        let response = self.send_request(request).await?;
        Self::check_status(&response, "Query failed")?;

        let mut result = QueryResult {
            segments_scanned: response.segments_scanned.unwrap_or(0),
            data_skipped_bytes: response.data_skipped_bytes.unwrap_or(0),
            ..Default::default()
        };

        if let Some(ipc_bytes) = response.ipc_bytes {
            parse_arrow_ipc(&ipc_bytes, &mut result);
        } else if let Some(ipc_base64) = response.ipc_base64 {
            let ipc_data = BASE64.decode(&ipc_base64)?;
            parse_arrow_ipc(&ipc_data, &mut result);
        }

        Ok(result)
    }

    /// Execute a SQL statement that doesn't return rows.
    pub async fn exec(&self, sql: &str) -> Result<(), Error> {
        self.query(sql).await?;
        Ok(())
    }

    /// Execute a SQL statement with options.
    pub async fn exec_with_options(
        &self,
        sql: &str,
        database: Option<&str>,
        timeout_millis: Option<u32>,
    ) -> Result<(), Error> {
        self.query_with_options(sql, database, timeout_millis).await?;
        Ok(())
    }

    /// Prepare a SELECT query on the server and return a prepared id.
    pub async fn prepare(&self, sql: &str, database: Option<&str>) -> Result<String, Error> {
        let mut request = json!({
            "op": "prepare",
            "sql": sql
        });
        let db = database.or(self.config.database.as_deref());
        if let Some(db) = db {
            request["database"] = json!(db);
        }
        let response = self.send_request(request).await?;
        Self::check_status(&response, "Prepare failed")?;
        response
            .prepared_id
            .ok_or_else(|| Error::Query("missing prepared_id in response".into()))
    }

    /// Execute a prepared statement using the binary response path.
    pub async fn execute_prepared_binary(
        &self,
        prepared_id: &str,
        timeout_millis: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let response = self
            .send_request(json!({
                "op": "execute_prepared_binary",
                "id": prepared_id,
                "timeout_millis": timeout_millis.unwrap_or(self.config.query_timeout),
                "stream": true
            }))
            .await?;
        Self::check_status(&response, "Execute prepared failed")?;

        let mut result = QueryResult {
            segments_scanned: response.segments_scanned.unwrap_or(0),
            data_skipped_bytes: response.data_skipped_bytes.unwrap_or(0),
            ..Default::default()
        };

        if let Some(ipc_bytes) = response.ipc_bytes {
            parse_arrow_ipc(&ipc_bytes, &mut result);
        } else if let Some(ipc_base64) = response.ipc_base64 {
            let ipc_data = BASE64.decode(&ipc_base64)?;
            parse_arrow_ipc(&ipc_data, &mut result);
        }

        Ok(result)
    }

    /// Create a new database.
    pub async fn create_database(&self, name: &str) -> Result<(), Error> {
        let response = self
            .send_request(json!({
                "op": "createdatabase",
                "name": name
            }))
            .await?;
        Self::check_status(&response, "Create database failed")
    }

    /// Create a new table.
    pub async fn create_table(&self, database: &str, table: &str) -> Result<(), Error> {
        let response = self
            .send_request(json!({
                "op": "createtable",
                "database": database,
                "table": table
            }))
            .await?;
        Self::check_status(&response, "Create table failed")
    }

    /// List all databases.
    pub async fn list_databases(&self) -> Result<Vec<String>, Error> {
        let response = self.send_request(json!({"op": "listdatabases"})).await?;
        Self::check_status(&response, "List databases failed")?;
        Ok(response.databases.unwrap_or_default())
    }

    /// List tables, optionally filtered by database.
    pub async fn list_tables(&self, database: Option<&str>) -> Result<Vec<TableInfo>, Error> {
        let mut request = json!({"op": "listtables"});
        if let Some(db) = database {
            request["database"] = json!(db);
        }

        let response = self.send_request(request).await?;
        Self::check_status(&response, "List tables failed")?;
        Ok(response.tables.unwrap_or_default())
    }

    /// Get query execution plan.
    pub async fn explain(&self, sql: &str) -> Result<Value, Error> {
        let response = self
            .send_request(json!({
                "op": "explain",
                "sql": sql
            }))
            .await?;
        Self::check_status(&response, "Explain failed")?;
        Ok(response.explain_plan.unwrap_or(json!({})))
    }

    /// Get server metrics.
    pub async fn metrics(&self) -> Result<Value, Error> {
        let response = self.send_request(json!({"op": "metrics"})).await?;
        Self::check_status(&response, "Metrics failed")?;
        Ok(response.metrics.unwrap_or(json!({})))
    }

    /// Ingest CSV data into a table.
    pub async fn ingest_csv(
        &self,
        database: &str,
        table: &str,
        csv_data: &[u8],
        has_header: bool,
        delimiter: Option<&str>,
    ) -> Result<(), Error> {
        let mut request = json!({
            "op": "ingestcsv",
            "database": database,
            "table": table,
            "payload_base64": BASE64.encode(csv_data),
            "has_header": has_header
        });

        if let Some(delim) = delimiter {
            request["delimiter"] = json!(delim);
        }

        let response = self.send_request(request).await?;
        Self::check_status(&response, "Ingest CSV failed")
    }

    /// Ingest CSV data from a string.
    pub async fn ingest_csv_str(
        &self,
        database: &str,
        table: &str,
        csv_data: &str,
        has_header: bool,
        delimiter: Option<&str>,
    ) -> Result<(), Error> {
        self.ingest_csv(database, table, csv_data.as_bytes(), has_header, delimiter)
            .await
    }

    /// Ingest Arrow IPC data into a table.
    pub async fn ingest_ipc(
        &self,
        database: &str,
        table: &str,
        ipc_data: &[u8],
    ) -> Result<(), Error> {
        if let Ok(()) = self
            .send_request_binary(
                json!({
                    "op": "ingest_ipc_binary",
                    "database": database,
                    "table": table
                }),
                ipc_data,
            )
            .await
        {
            return Ok(());
        }
        let response = self
            .send_request(json!({
                "op": "ingestipc",
                "database": database,
                "table": table,
                "payload_base64": BASE64.encode(ipc_data)
            }))
            .await?;
        Self::check_status(&response, "Ingest IPC failed")
    }

    /// Set the default database.
    pub fn set_database(&mut self, database: impl Into<String>) {
        self.config.database = Some(database.into());
    }

    /// Set the authentication token.
    pub fn set_token(&mut self, token: impl Into<String>) {
        self.config.token = Some(token.into());
    }

    // ========================================================================
    // Pub/Sub Support
    // ========================================================================

    /// Start listening on a notification channel.
    pub async fn listen(&self, channel: &str) -> Result<(), Error> {
        self.exec(&format!("LISTEN {}", Self::escape_identifier(channel))).await
    }

    /// Stop listening on a notification channel.
    /// Use "*" to unlisten all channels.
    pub async fn unlisten(&self, channel: &str) -> Result<(), Error> {
        if channel == "*" {
            self.exec("UNLISTEN *").await
        } else {
            self.exec(&format!("UNLISTEN {}", Self::escape_identifier(channel))).await
        }
    }

    /// Send a notification on a channel.
    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<(), Error> {
        if let Some(p) = payload {
            self.exec(&format!(
                "NOTIFY {}, '{}'",
                Self::escape_identifier(channel),
                Self::escape_string(p)
            )).await
        } else {
            self.exec(&format!("NOTIFY {}", Self::escape_identifier(channel))).await
        }
    }

    // ========================================================================
    // Trigger Management
    // ========================================================================

    /// Create a database trigger.
    pub async fn create_trigger(
        &self,
        name: &str,
        table: &str,
        timing: &str,
        events: &[&str],
        function: &str,
        arguments: Option<&[Value]>,
        for_each_row: bool,
        when_clause: Option<&str>,
        or_replace: bool,
    ) -> Result<(), Error> {
        let mut sql = if or_replace {
            format!("CREATE OR REPLACE TRIGGER {} ", Self::escape_identifier(name))
        } else {
            format!("CREATE TRIGGER {} ", Self::escape_identifier(name))
        };

        sql.push_str(&timing.to_uppercase());
        sql.push(' ');
        sql.push_str(&events.iter().map(|e| e.to_uppercase()).collect::<Vec<_>>().join(" OR "));
        sql.push_str(" ON ");
        sql.push_str(&Self::escape_identifier(table));
        sql.push(' ');

        sql.push_str(if for_each_row { "FOR EACH ROW " } else { "FOR EACH STATEMENT " });

        if let Some(when) = when_clause {
            sql.push_str(&format!("WHEN ({}) ", when));
        }

        sql.push_str(&format!("EXECUTE FUNCTION {}(", Self::escape_identifier(function)));
        if let Some(args) = arguments {
            sql.push_str(
                &args
                    .iter()
                    .map(|v| Self::format_value(v))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql.push(')');

        self.exec(&sql).await
    }

    /// Drop a trigger.
    pub async fn drop_trigger(&self, name: &str, table: &str, if_exists: bool) -> Result<(), Error> {
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        self.exec(&format!(
            "DROP TRIGGER {}{} ON {}",
            if_exists_clause,
            Self::escape_identifier(name),
            Self::escape_identifier(table)
        )).await
    }

    /// Enable or disable a trigger.
    pub async fn alter_trigger(&self, name: &str, table: &str, enable: bool) -> Result<(), Error> {
        let action = if enable { "ENABLE" } else { "DISABLE" };
        self.exec(&format!(
            "ALTER TRIGGER {} ON {} {}",
            Self::escape_identifier(name),
            Self::escape_identifier(table),
            action
        )).await
    }

    /// List triggers on a table or in a database.
    pub async fn list_triggers(
        &self,
        table: Option<&str>,
        database: Option<&str>,
    ) -> Result<QueryResult, Error> {
        let mut sql = "SHOW TRIGGERS".to_string();
        if let Some(t) = table {
            sql.push_str(&format!(" ON {}", Self::escape_identifier(t)));
        } else if let Some(db) = database {
            sql.push_str(&format!(" IN {}", Self::escape_identifier(db)));
        }
        self.query(&sql).await
    }

    // ========================================================================
    // Stored Procedures and Functions
    // ========================================================================

    /// Call a stored procedure.
    pub async fn call(&self, procedure: &str, args: &[Value]) -> Result<QueryResult, Error> {
        let formatted_args = args
            .iter()
            .map(|v| Self::format_value(v))
            .collect::<Vec<_>>()
            .join(", ");
        self.query(&format!("CALL {}({})", Self::escape_identifier(procedure), formatted_args)).await
    }

    /// Create a stored function.
    pub async fn create_function(
        &self,
        name: &str,
        parameters: &[(&str, &str)],
        return_type: &str,
        body: &str,
        language: Option<&str>,
        or_replace: bool,
        volatility: Option<&str>,
    ) -> Result<(), Error> {
        let lang = language.unwrap_or("plpgsql");
        let params = parameters
            .iter()
            .map(|(n, t)| format!("{} {}", Self::escape_identifier(n), t))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = if or_replace {
            format!("CREATE OR REPLACE FUNCTION {}({}) ", Self::escape_identifier(name), params)
        } else {
            format!("CREATE FUNCTION {}({}) ", Self::escape_identifier(name), params)
        };

        sql.push_str(&format!("RETURNS {} LANGUAGE {} ", return_type, lang));

        if let Some(vol) = volatility {
            sql.push_str(&vol.to_uppercase());
            sql.push(' ');
        }

        sql.push_str(&format!("AS $$ {} $$", body));

        self.exec(&sql).await
    }

    /// Create a stored procedure.
    pub async fn create_procedure(
        &self,
        name: &str,
        parameters: &[(&str, &str, Option<&str>)], // (name, type, mode)
        body: &str,
        language: Option<&str>,
        or_replace: bool,
    ) -> Result<(), Error> {
        let lang = language.unwrap_or("plpgsql");
        let params = parameters
            .iter()
            .map(|(n, t, m)| {
                let mode = m.map(|mo| format!("{} ", mo.to_uppercase())).unwrap_or_default();
                format!("{}{} {}", mode, Self::escape_identifier(n), t)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = if or_replace {
            format!(
                "CREATE OR REPLACE PROCEDURE {}({}) LANGUAGE {} AS $$ {} $$",
                Self::escape_identifier(name),
                params,
                lang,
                body
            )
        } else {
            format!(
                "CREATE PROCEDURE {}({}) LANGUAGE {} AS $$ {} $$",
                Self::escape_identifier(name),
                params,
                lang,
                body
            )
        };

        self.exec(&sql).await
    }

    /// Drop a function.
    pub async fn drop_function(
        &self,
        name: &str,
        param_types: Option<&[&str]>,
        if_exists: bool,
    ) -> Result<(), Error> {
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let signature = param_types
            .map(|types| format!("({})", types.join(", ")))
            .unwrap_or_default();
        self.exec(&format!(
            "DROP FUNCTION {}{}{}",
            if_exists_clause,
            Self::escape_identifier(name),
            signature
        )).await
    }

    /// Drop a procedure.
    pub async fn drop_procedure(
        &self,
        name: &str,
        param_types: Option<&[&str]>,
        if_exists: bool,
    ) -> Result<(), Error> {
        let if_exists_clause = if if_exists { "IF EXISTS " } else { "" };
        let signature = param_types
            .map(|types| format!("({})", types.join(", ")))
            .unwrap_or_default();
        self.exec(&format!(
            "DROP PROCEDURE {}{}{}",
            if_exists_clause,
            Self::escape_identifier(name),
            signature
        )).await
    }

    // ========================================================================
    // JSON Operations
    // ========================================================================

    /// Extract a JSON value using the -> operator.
    pub async fn json_extract(
        &self,
        column: &str,
        path: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let path_expr = if path.parse::<i32>().is_ok() {
            path.to_string()
        } else {
            format!("'{}'", Self::escape_string(path))
        };

        let mut sql = format!(
            "SELECT {} -> {} AS value FROM {}",
            Self::escape_identifier(column),
            path_expr,
            Self::escape_identifier(table)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" WHERE {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Extract a JSON value as text using the ->> operator.
    pub async fn json_extract_text(
        &self,
        column: &str,
        path: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let path_expr = if path.parse::<i32>().is_ok() {
            path.to_string()
        } else {
            format!("'{}'", Self::escape_string(path))
        };

        let mut sql = format!(
            "SELECT {} ->> {} AS value FROM {}",
            Self::escape_identifier(column),
            path_expr,
            Self::escape_identifier(table)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" WHERE {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Extract a nested JSON value using the #> operator.
    pub async fn json_extract_path(
        &self,
        column: &str,
        path: &[&str],
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let path_array = format!(
            "'{{{}}}'",
            path.iter().map(|p| Self::escape_string(p)).collect::<Vec<_>>().join(",")
        );

        let mut sql = format!(
            "SELECT {} #> {} AS value FROM {}",
            Self::escape_identifier(column),
            path_array,
            Self::escape_identifier(table)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" WHERE {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if JSON contains a value using the @> operator.
    pub async fn json_contains(
        &self,
        column: &str,
        value: &Value,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let json_value = serde_json::to_string(value)?;

        let mut sql = format!(
            "SELECT * FROM {} WHERE {} @> '{}'::jsonb",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            Self::escape_string(&json_value)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if JSON is contained in a value using the <@ operator.
    pub async fn json_contained_by(
        &self,
        column: &str,
        value: &Value,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let json_value = serde_json::to_string(value)?;

        let mut sql = format!(
            "SELECT * FROM {} WHERE {} <@ '{}'::jsonb",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            Self::escape_string(&json_value)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if a JSON path exists using the @? operator.
    pub async fn json_path_exists(
        &self,
        column: &str,
        json_path: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let mut sql = format!(
            "SELECT * FROM {} WHERE {} @? '{}'",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            Self::escape_string(json_path)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if a JSON path predicate matches using the @@ operator.
    pub async fn json_path_match(
        &self,
        column: &str,
        json_path: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let mut sql = format!(
            "SELECT * FROM {} WHERE {} @@ '{}'",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            Self::escape_string(json_path)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    // ========================================================================
    // Cursor Support
    // ========================================================================

    /// Declare a server-side cursor for a query.
    pub async fn declare_cursor(
        &self,
        name: &str,
        sql: &str,
        with_hold: bool,
        scrollable: bool,
    ) -> Result<(), Error> {
        let mut cursor_sql = String::from("DECLARE ");
        cursor_sql.push_str(&Self::escape_identifier(name));

        if scrollable {
            cursor_sql.push_str(" SCROLL");
        } else {
            cursor_sql.push_str(" NO SCROLL");
        }

        cursor_sql.push_str(" CURSOR ");

        if with_hold {
            cursor_sql.push_str("WITH HOLD ");
        }

        cursor_sql.push_str("FOR ");
        cursor_sql.push_str(sql);

        self.exec(&cursor_sql).await
    }

    /// Fetch rows from a cursor.
    pub async fn fetch_cursor(
        &self,
        name: &str,
        direction: Option<&str>,
        count: Option<i32>,
    ) -> Result<QueryResult, Error> {
        let dir = direction.unwrap_or("NEXT");
        let cnt = count.map(|c| c.to_string()).unwrap_or_default();

        let sql = format!(
            "FETCH {} {} FROM {}",
            dir.to_uppercase(),
            cnt,
            Self::escape_identifier(name)
        );

        self.query(&sql).await
    }

    /// Move the cursor position without fetching data.
    pub async fn move_cursor(
        &self,
        name: &str,
        direction: &str,
        count: Option<i32>,
    ) -> Result<(), Error> {
        let cnt = count.map(|c| c.to_string()).unwrap_or_default();

        let sql = format!(
            "MOVE {} {} IN {}",
            direction.to_uppercase(),
            cnt,
            Self::escape_identifier(name)
        );

        self.exec(&sql).await
    }

    /// Close a cursor.
    pub async fn close_cursor(&self, name: &str) -> Result<(), Error> {
        if name == "*" {
            self.exec("CLOSE ALL").await
        } else {
            self.exec(&format!("CLOSE {}", Self::escape_identifier(name))).await
        }
    }

    // ========================================================================
    // Large Object Support
    // ========================================================================

    /// Create a new large object.
    pub async fn lo_create(&self, oid: Option<u32>) -> Result<u32, Error> {
        let sql = match oid {
            Some(o) => format!("SELECT lo_create({}) AS oid", o),
            None => "SELECT lo_create(0) AS oid".to_string(),
        };

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Number(n)) = result.rows[0].get("oid") {
                return Ok(n.as_u64().unwrap_or(0) as u32);
            }
        }
        Err(Error::Query("Failed to create large object".into()))
    }

    /// Open a large object for read/write.
    pub async fn lo_open(&self, oid: u32, mode: Option<u32>) -> Result<i32, Error> {
        // mode: 0x20000 = INV_READ, 0x40000 = INV_WRITE, 0x60000 = INV_READ | INV_WRITE
        let m = mode.unwrap_or(0x60000);
        let sql = format!("SELECT lo_open({}, {}) AS fd", oid, m);

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Number(n)) = result.rows[0].get("fd") {
                return Ok(n.as_i64().unwrap_or(-1) as i32);
            }
        }
        Err(Error::Query("Failed to open large object".into()))
    }

    /// Write data to a large object.
    pub async fn lo_write(&self, fd: i32, data: &[u8]) -> Result<i32, Error> {
        let encoded = BASE64.encode(data);
        let sql = format!(
            "SELECT lo_write({}, decode('{}', 'base64')) AS bytes_written",
            fd, encoded
        );

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Number(n)) = result.rows[0].get("bytes_written") {
                return Ok(n.as_i64().unwrap_or(0) as i32);
            }
        }
        Err(Error::Query("Failed to write to large object".into()))
    }

    /// Read data from a large object.
    pub async fn lo_read(&self, fd: i32, length: i32) -> Result<Vec<u8>, Error> {
        let sql = format!(
            "SELECT encode(lo_read({}, {}), 'base64') AS data",
            fd, length
        );

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::String(s)) = result.rows[0].get("data") {
                return BASE64.decode(s).map_err(|e| Error::Query(format!("Base64 decode error: {}", e)));
            }
        }
        Ok(Vec::new())
    }

    /// Seek within a large object.
    pub async fn lo_seek(&self, fd: i32, offset: i64, whence: i32) -> Result<i64, Error> {
        let sql = format!("SELECT lo_lseek64({}, {}, {}) AS position", fd, offset, whence);

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Number(n)) = result.rows[0].get("position") {
                return Ok(n.as_i64().unwrap_or(0));
            }
        }
        Err(Error::Query("Failed to seek in large object".into()))
    }

    /// Close a large object file descriptor.
    pub async fn lo_close(&self, fd: i32) -> Result<(), Error> {
        let sql = format!("SELECT lo_close({}) AS result", fd);
        self.query(&sql).await?;
        Ok(())
    }

    /// Delete a large object.
    pub async fn lo_unlink(&self, oid: u32) -> Result<(), Error> {
        let sql = format!("SELECT lo_unlink({}) AS result", oid);
        self.query(&sql).await?;
        Ok(())
    }

    /// Import a file into a large object.
    pub async fn lo_import(&self, path: &str, oid: Option<u32>) -> Result<u32, Error> {
        let sql = match oid {
            Some(o) => format!("SELECT lo_import('{}', {}) AS oid", Self::escape_string(path), o),
            None => format!("SELECT lo_import('{}') AS oid", Self::escape_string(path)),
        };

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Number(n)) = result.rows[0].get("oid") {
                return Ok(n.as_u64().unwrap_or(0) as u32);
            }
        }
        Err(Error::Query("Failed to import large object".into()))
    }

    /// Export a large object to a file.
    pub async fn lo_export(&self, oid: u32, path: &str) -> Result<(), Error> {
        let sql = format!("SELECT lo_export({}, '{}') AS result", oid, Self::escape_string(path));
        self.query(&sql).await?;
        Ok(())
    }

    // ========================================================================
    // Advisory Locks
    // ========================================================================

    /// Acquire a session-level advisory lock (blocking).
    pub async fn advisory_lock(&self, key: i64, key2: Option<i64>) -> Result<(), Error> {
        let sql = match key2 {
            Some(k2) => format!("SELECT pg_advisory_lock({}, {})", key, k2),
            None => format!("SELECT pg_advisory_lock({})", key),
        };
        self.query(&sql).await?;
        Ok(())
    }

    /// Try to acquire a session-level advisory lock (non-blocking).
    pub async fn advisory_lock_try(&self, key: i64, key2: Option<i64>) -> Result<bool, Error> {
        let sql = match key2 {
            Some(k2) => format!("SELECT pg_try_advisory_lock({}, {}) AS acquired", key, k2),
            None => format!("SELECT pg_try_advisory_lock({}) AS acquired", key),
        };

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Bool(b)) = result.rows[0].get("acquired") {
                return Ok(*b);
            }
        }
        Ok(false)
    }

    /// Release a session-level advisory lock.
    pub async fn advisory_unlock(&self, key: i64, key2: Option<i64>) -> Result<bool, Error> {
        let sql = match key2 {
            Some(k2) => format!("SELECT pg_advisory_unlock({}, {}) AS released", key, k2),
            None => format!("SELECT pg_advisory_unlock({}) AS released", key),
        };

        let result = self.query(&sql).await?;
        if !result.rows.is_empty() {
            if let Some(Value::Bool(b)) = result.rows[0].get("released") {
                return Ok(*b);
            }
        }
        Ok(false)
    }

    /// Release all session-level advisory locks.
    pub async fn advisory_unlock_all(&self) -> Result<(), Error> {
        self.query("SELECT pg_advisory_unlock_all()").await?;
        Ok(())
    }

    /// Acquire a transaction-level advisory lock (auto-released at commit/rollback).
    pub async fn advisory_xact_lock(&self, key: i64, key2: Option<i64>, try_lock: bool) -> Result<bool, Error> {
        let sql = if try_lock {
            match key2 {
                Some(k2) => format!("SELECT pg_try_advisory_xact_lock({}, {}) AS acquired", key, k2),
                None => format!("SELECT pg_try_advisory_xact_lock({}) AS acquired", key),
            }
        } else {
            match key2 {
                Some(k2) => format!("SELECT pg_advisory_xact_lock({}, {})", key, k2),
                None => format!("SELECT pg_advisory_xact_lock({})", key),
            }
        };

        let result = self.query(&sql).await?;
        if try_lock && !result.rows.is_empty() {
            if let Some(Value::Bool(b)) = result.rows[0].get("acquired") {
                return Ok(*b);
            }
        }
        Ok(!try_lock)
    }

    // ========================================================================
    // Full-Text Search
    // ========================================================================

    /// Search for documents using full-text search.
    pub async fn fts_search(
        &self,
        table: &str,
        columns: &[&str],
        query: &str,
        language: Option<&str>,
        limit: Option<u32>,
        rank_normalization: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let lang = language.unwrap_or("english");
        let norm = rank_normalization.unwrap_or(0);

        let col_expr = if columns.len() == 1 {
            format!("to_tsvector('{}', {})", lang, Self::escape_identifier(columns[0]))
        } else {
            columns
                .iter()
                .map(|c| format!("to_tsvector('{}', COALESCE({}, ''))", lang, Self::escape_identifier(c)))
                .collect::<Vec<_>>()
                .join(" || ")
        };

        let mut sql = format!(
            "SELECT *, ts_rank({}, plainto_tsquery('{}', '{}'), {}) AS rank FROM {} WHERE {} @@ plainto_tsquery('{}', '{}') ORDER BY rank DESC",
            col_expr,
            lang,
            Self::escape_string(query),
            norm,
            Self::escape_identifier(table),
            col_expr,
            lang,
            Self::escape_string(query)
        );

        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Get highlighted excerpts from full-text search results.
    pub async fn fts_highlight(
        &self,
        table: &str,
        column: &str,
        query: &str,
        language: Option<&str>,
        start_sel: Option<&str>,
        stop_sel: Option<&str>,
        max_words: Option<u32>,
        min_words: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let lang = language.unwrap_or("english");
        let start = start_sel.unwrap_or("<b>");
        let stop = stop_sel.unwrap_or("</b>");
        let max_w = max_words.unwrap_or(35);
        let min_w = min_words.unwrap_or(15);

        let sql = format!(
            "SELECT *, ts_headline('{}', {}, plainto_tsquery('{}', '{}'), 'StartSel={}, StopSel={}, MaxWords={}, MinWords={}') AS headline FROM {} WHERE to_tsvector('{}', {}) @@ plainto_tsquery('{}', '{}')",
            lang,
            Self::escape_identifier(column),
            lang,
            Self::escape_string(query),
            Self::escape_string(start),
            Self::escape_string(stop),
            max_w,
            min_w,
            Self::escape_identifier(table),
            lang,
            Self::escape_identifier(column),
            lang,
            Self::escape_string(query)
        );

        self.query(&sql).await
    }

    // ========================================================================
    // Geospatial Operations
    // ========================================================================

    /// Calculate distance between two points using ST_Distance.
    pub async fn geo_distance(
        &self,
        table: &str,
        geometry_column: &str,
        lat: f64,
        lon: f64,
        srid: Option<i32>,
        use_spheroid: bool,
    ) -> Result<QueryResult, Error> {
        let s = srid.unwrap_or(4326);
        let point = format!("ST_SetSRID(ST_MakePoint({}, {}), {})", lon, lat, s);

        let distance_func = if use_spheroid {
            format!("ST_Distance({}::geography, {}::geography)", Self::escape_identifier(geometry_column), point)
        } else {
            format!("ST_Distance({}, {})", Self::escape_identifier(geometry_column), point)
        };

        let sql = format!(
            "SELECT *, {} AS distance FROM {} ORDER BY distance",
            distance_func,
            Self::escape_identifier(table)
        );

        self.query(&sql).await
    }

    /// Check if geometries contain a point.
    pub async fn geo_contains(
        &self,
        table: &str,
        geometry_column: &str,
        lat: f64,
        lon: f64,
        srid: Option<i32>,
    ) -> Result<QueryResult, Error> {
        let s = srid.unwrap_or(4326);
        let point = format!("ST_SetSRID(ST_MakePoint({}, {}), {})", lon, lat, s);

        let sql = format!(
            "SELECT * FROM {} WHERE ST_Contains({}, {})",
            Self::escape_identifier(table),
            Self::escape_identifier(geometry_column),
            point
        );

        self.query(&sql).await
    }

    /// Find K nearest neighbors using KNN index.
    pub async fn geo_nearest(
        &self,
        table: &str,
        geometry_column: &str,
        lat: f64,
        lon: f64,
        k: u32,
        srid: Option<i32>,
        max_distance: Option<f64>,
    ) -> Result<QueryResult, Error> {
        let s = srid.unwrap_or(4326);
        let point = format!("ST_SetSRID(ST_MakePoint({}, {}), {})", lon, lat, s);

        let mut sql = format!(
            "SELECT *, {} <-> {} AS distance FROM {}",
            Self::escape_identifier(geometry_column),
            point,
            Self::escape_identifier(table)
        );

        if let Some(max_dist) = max_distance {
            sql.push_str(&format!(
                " WHERE ST_DWithin({}, {}, {})",
                Self::escape_identifier(geometry_column),
                point,
                max_dist
            ));
        }

        sql.push_str(&format!(" ORDER BY {} <-> {} LIMIT {}", Self::escape_identifier(geometry_column), point, k));

        self.query(&sql).await
    }

    // ========================================================================
    // Time Travel Queries
    // ========================================================================

    /// Query data as of a specific timestamp.
    pub async fn query_as_of(&self, sql: &str, timestamp: &str) -> Result<QueryResult, Error> {
        let time_travel_sql = format!("{} AS OF TIMESTAMP '{}'", sql, Self::escape_string(timestamp));
        self.query(&time_travel_sql).await
    }

    /// Query data between two timestamps.
    pub async fn query_between(
        &self,
        sql: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<QueryResult, Error> {
        let time_travel_sql = format!(
            "{} FOR SYSTEM_TIME BETWEEN '{}' AND '{}'",
            sql,
            Self::escape_string(start_time),
            Self::escape_string(end_time)
        );
        self.query(&time_travel_sql).await
    }

    /// Get history of changes for a table.
    pub async fn get_history(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &Value,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> Result<QueryResult, Error> {
        let mut sql = format!(
            "SELECT * FROM {} FOR SYSTEM_TIME ALL WHERE {} = {}",
            Self::escape_identifier(table),
            Self::escape_identifier(pk_column),
            Self::format_value(pk_value)
        );

        if let Some(start) = start_time {
            sql.push_str(&format!(" AND system_time_start >= '{}'", Self::escape_string(start)));
        }
        if let Some(end) = end_time {
            sql.push_str(&format!(" AND system_time_end <= '{}'", Self::escape_string(end)));
        }

        sql.push_str(" ORDER BY system_time_start");

        self.query(&sql).await
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /// Batch insert multiple rows.
    pub async fn batch_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
        chunk_size: Option<usize>,
    ) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }

        let chunk_sz = chunk_size.unwrap_or(1000);
        let mut total_inserted = 0u64;

        let cols_escaped: Vec<String> = columns.iter().map(|c| Self::escape_identifier(c)).collect();
        let cols_joined = cols_escaped.join(", ");

        for chunk in rows.chunks(chunk_sz) {
            let values_strs: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let formatted: Vec<String> = row.iter().map(|v| Self::format_value(v)).collect();
                    format!("({})", formatted.join(", "))
                })
                .collect();

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                Self::escape_identifier(table),
                cols_joined,
                values_strs.join(", ")
            );

            self.exec(&sql).await?;
            total_inserted += chunk.len() as u64;
        }

        Ok(total_inserted)
    }

    /// Batch update multiple rows.
    pub async fn batch_update(
        &self,
        table: &str,
        updates: &[(&str, Value)],
        where_clause: &str,
    ) -> Result<(), Error> {
        if updates.is_empty() {
            return Ok(());
        }

        let set_clause: Vec<String> = updates
            .iter()
            .map(|(col, val)| format!("{} = {}", Self::escape_identifier(col), Self::format_value(val)))
            .collect();

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            Self::escape_identifier(table),
            set_clause.join(", "),
            where_clause
        );

        self.exec(&sql).await
    }

    /// Batch upsert (insert or update on conflict).
    pub async fn batch_upsert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
        conflict_columns: &[&str],
        update_columns: &[&str],
        chunk_size: Option<usize>,
    ) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }

        let chunk_sz = chunk_size.unwrap_or(1000);
        let mut total_upserted = 0u64;

        let cols_escaped: Vec<String> = columns.iter().map(|c| Self::escape_identifier(c)).collect();
        let cols_joined = cols_escaped.join(", ");

        let conflict_escaped: Vec<String> = conflict_columns.iter().map(|c| Self::escape_identifier(c)).collect();
        let conflict_joined = conflict_escaped.join(", ");

        let update_clause: Vec<String> = update_columns
            .iter()
            .map(|c| format!("{} = EXCLUDED.{}", Self::escape_identifier(c), Self::escape_identifier(c)))
            .collect();
        let update_joined = update_clause.join(", ");

        for chunk in rows.chunks(chunk_sz) {
            let values_strs: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let formatted: Vec<String> = row.iter().map(|v| Self::format_value(v)).collect();
                    format!("({})", formatted.join(", "))
                })
                .collect();

            let sql = format!(
                "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {}",
                Self::escape_identifier(table),
                cols_joined,
                values_strs.join(", "),
                conflict_joined,
                update_joined
            );

            self.exec(&sql).await?;
            total_upserted += chunk.len() as u64;
        }

        Ok(total_upserted)
    }

    /// Batch delete rows matching conditions.
    pub async fn batch_delete(
        &self,
        table: &str,
        pk_column: &str,
        pk_values: &[Value],
        chunk_size: Option<usize>,
    ) -> Result<u64, Error> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let chunk_sz = chunk_size.unwrap_or(1000);
        let mut total_deleted = 0u64;

        for chunk in pk_values.chunks(chunk_sz) {
            let values_formatted: Vec<String> = chunk.iter().map(|v| Self::format_value(v)).collect();

            let sql = format!(
                "DELETE FROM {} WHERE {} IN ({})",
                Self::escape_identifier(table),
                Self::escape_identifier(pk_column),
                values_formatted.join(", ")
            );

            self.exec(&sql).await?;
            total_deleted += chunk.len() as u64;
        }

        Ok(total_deleted)
    }

    // ========================================================================
    // Schema Introspection
    // ========================================================================

    /// Get column information for a table.
    pub async fn get_columns(&self, table: &str) -> Result<QueryResult, Error> {
        self.query(&format!("DESCRIBE {}", Self::escape_identifier(table))).await
    }

    /// Get index information for a table.
    pub async fn get_indexes(&self, table: &str) -> Result<QueryResult, Error> {
        self.query(&format!("SHOW INDEXES ON {}", Self::escape_identifier(table))).await
    }

    /// Get constraint information for a table.
    pub async fn get_constraints(&self, table: &str) -> Result<QueryResult, Error> {
        self.query(&format!("SHOW CONSTRAINTS ON {}", Self::escape_identifier(table))).await
    }

    /// Get complete schema information for a table.
    pub async fn get_schema(&self, table: &str) -> Result<Value, Error> {
        let columns = self.get_columns(table).await?;
        let indexes = self.get_indexes(table).await?;
        let constraints = self.get_constraints(table).await?;

        Ok(json!({
            "table": table,
            "columns": columns.rows,
            "indexes": indexes.rows,
            "constraints": constraints.rows
        }))
    }

    // ========================================================================
    // Array Operations
    // ========================================================================

    /// Check if an array column contains a value.
    pub async fn array_contains(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let mut sql = format!(
            "SELECT * FROM {} WHERE {} @> ARRAY[{}]",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            Self::format_value(value)
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if arrays overlap (have common elements).
    pub async fn array_overlap(
        &self,
        table: &str,
        column: &str,
        values: &[Value],
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let values_formatted: Vec<String> = values.iter().map(|v| Self::format_value(v)).collect();

        let mut sql = format!(
            "SELECT * FROM {} WHERE {} && ARRAY[{}]",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            values_formatted.join(", ")
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    /// Check if array contains all specified values.
    pub async fn array_contains_all(
        &self,
        table: &str,
        column: &str,
        values: &[Value],
        where_clause: Option<&str>,
        limit: Option<u32>,
    ) -> Result<QueryResult, Error> {
        let values_formatted: Vec<String> = values.iter().map(|v| Self::format_value(v)).collect();

        let mut sql = format!(
            "SELECT * FROM {} WHERE {} @> ARRAY[{}]",
            Self::escape_identifier(table),
            Self::escape_identifier(column),
            values_formatted.join(", ")
        );

        if let Some(w) = where_clause {
            sql.push_str(&format!(" AND {}", w));
        }
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {}", l));
        }

        self.query(&sql).await
    }

    // ========================================================================
    // Async Pub/Sub Polling
    // ========================================================================

    /// Poll for notifications on channels we're listening to.
    pub async fn poll_notifications(&self, timeout_ms: Option<u32>) -> Result<Vec<(String, String)>, Error> {
        let tm = timeout_ms.unwrap_or(0);
        let response = self
            .send_request(json!({
                "op": "poll_notifications",
                "timeout_ms": tm
            }))
            .await?;

        Self::check_status(&response, "Poll notifications failed")?;

        let mut notifications = Vec::new();
        if let Some(notifs) = response.notifications {
            for notif in notifs {
                if let (Some(channel), Some(payload)) = (
                    notif.get("channel").and_then(|v| v.as_str()),
                    notif.get("payload").and_then(|v| v.as_str()),
                ) {
                    notifications.push((channel.to_string(), payload.to_string()));
                }
            }
        }

        Ok(notifications)
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /// Escape an identifier for SQL.
    fn escape_identifier(identifier: &str) -> String {
        if identifier.contains('.') {
            identifier
                .split('.')
                .map(Self::escape_identifier)
                .collect::<Vec<_>>()
                .join(".")
        } else {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
    }

    /// Escape a string value for SQL.
    fn escape_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    /// Format a value for SQL.
    fn format_value(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("'{}'", Self::escape_string(s)),
            _ => format!("'{}'", Self::escape_string(&value.to_string())),
        }
    }

    fn is_select_like(sql: &str) -> bool {
        let trimmed = sql.trim_start().to_ascii_lowercase();
        trimmed.starts_with("select ") || trimmed.starts_with("with ")
    }
}

async fn read_frame(stream: &mut StreamKind) -> Result<Vec<u8>, Error> {
    let mut len_buf = [0u8; 4];
    match stream {
        StreamKind::Plain(s) => { s.read_exact(&mut len_buf).await?; }
        #[cfg(feature = "tls")]
        StreamKind::Tls(s) => { s.read_exact(&mut len_buf).await?; }
    }
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    if resp_len > 100 * 1024 * 1024 {
        return Err(Error::Query(format!("Response too large: {} bytes", resp_len)));
    }
    let mut buf = vec![0u8; resp_len];
    match stream {
        StreamKind::Plain(s) => { s.read_exact(&mut buf).await?; }
        #[cfg(feature = "tls")]
        StreamKind::Tls(s) => { s.read_exact(&mut buf).await?; }
    }
    Ok(buf)
}

async fn read_stream_frames(stream: &mut StreamKind) -> Result<Vec<u8>, Error> {
    let mut payload = Vec::new();
    loop {
        let chunk = read_frame(stream).await?;
        if chunk.is_empty() {
            break;
        }
        payload.extend_from_slice(&chunk);
    }
    Ok(payload)
}

/// Custom certificate verifier that accepts any certificate (INSECURE).
/// Only used when insecure_skip_verify is enabled.
#[cfg(feature = "tls")]
#[derive(Debug)]
struct InsecureServerCertVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Accept any certificate - THIS IS INSECURE
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
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

fn parse_host(host: &str) -> (&str, u16) {
    let parts: Vec<&str> = host.split(':').collect();
    let hostname = if parts.is_empty() { "localhost" } else { parts[0] };
    let port = if parts.len() > 1 {
        parts[1].parse().unwrap_or(8765)
    } else {
        8765
    };
    (hostname, port)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host() {
        assert_eq!(parse_host("localhost:8765"), ("localhost", 8765));
        assert_eq!(parse_host("127.0.0.1:9000"), ("127.0.0.1", 9000));
        assert_eq!(parse_host("localhost"), ("localhost", 8765));
    }
}
