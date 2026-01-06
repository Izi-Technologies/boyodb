use crate::config::Config;
use crate::error::Error;
use crate::result::{QueryResult, Response, TableInfo};
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

/// Parse Arrow IPC data (simplified parser).
/// For production use, consider using the arrow crate.
fn parse_arrow_ipc(data: &[u8], result: &mut QueryResult) {
    let mut offset = 0;

    while offset < data.len() {
        if offset + 4 > data.len() {
            break;
        }

        let msg_len = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        if msg_len == 0 {
            break;
        }

        // Handle continuation marker
        let msg_len = if msg_len == -1 {
            if offset + 4 > data.len() {
                break;
            }
            let len = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            if len == 0 {
                break;
            }
            len
        } else {
            msg_len
        };

        let msg_len = msg_len as usize;
        if offset + msg_len > data.len() {
            break;
        }

        let _metadata = &data[offset..offset + msg_len];
        offset += msg_len;

        // Pad to 8-byte boundary
        let padding = (8 - (msg_len % 8)) % 8;
        offset += padding;

        // For now, just skip the body - full Arrow parsing is complex
        // In production, use the arrow crate
    }

    // Return empty result - full parsing would require arrow crate
    let _ = result;
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
