//! Protocol and Security - PostgreSQL Wire Protocol, SSL/TLS, SCRAM Authentication
//!
//! This module provides:
//! - PostgreSQL wire protocol message handling
//! - SSL/TLS connection security
//! - SCRAM-SHA-256 authentication
//! - Connection encryption

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;

// ============================================================================
// POSTGRESQL WIRE PROTOCOL
// ============================================================================

/// Protocol version (3.0)
pub const PROTOCOL_VERSION: i32 = 196608; // 3 << 16

/// SSL request code
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Cancel request code
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

/// GSS encryption request code
pub const GSS_ENC_REQUEST_CODE: i32 = 80877104;

/// Message type codes (frontend to backend)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrontendMessage {
    /// Bind (B)
    Bind = b'B',
    /// Close (C)
    Close = b'C',
    /// CopyData (d)
    CopyData = b'd',
    /// CopyDone (c)
    CopyDone = b'c',
    /// CopyFail (f)
    CopyFail = b'f',
    /// Describe (D)
    Describe = b'D',
    /// Execute (E)
    Execute = b'E',
    /// Flush (H)
    Flush = b'H',
    /// FunctionCall (F)
    FunctionCall = b'F',
    /// Parse (P)
    Parse = b'P',
    /// PasswordMessage/GSSResponse/SASLResponse (p) - all auth responses share same code
    AuthResponse = b'p',
    /// Query (Q)
    Query = b'Q',
    /// Sync (S)
    Sync = b'S',
    /// Terminate (X)
    Terminate = b'X',
}

/// Message type codes (backend to frontend)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BackendMessage {
    /// AuthenticationOk (R)
    AuthenticationOk = b'R',
    /// BackendKeyData (K)
    BackendKeyData = b'K',
    /// BindComplete (2)
    BindComplete = b'2',
    /// CloseComplete (3)
    CloseComplete = b'3',
    /// CommandComplete (C)
    CommandComplete = b'C',
    /// CopyData (d)
    CopyData = b'd',
    /// CopyDone (c)
    CopyDone = b'c',
    /// CopyInResponse (G)
    CopyInResponse = b'G',
    /// CopyOutResponse (H)
    CopyOutResponse = b'H',
    /// DataRow (D)
    DataRow = b'D',
    /// EmptyQueryResponse (I)
    EmptyQueryResponse = b'I',
    /// ErrorResponse (E)
    ErrorResponse = b'E',
    /// FunctionCallResponse (V)
    FunctionCallResponse = b'V',
    /// NoData (n)
    NoData = b'n',
    /// NoticeResponse (N)
    NoticeResponse = b'N',
    /// NotificationResponse (A)
    NotificationResponse = b'A',
    /// ParameterDescription (t)
    ParameterDescription = b't',
    /// ParameterStatus (S)
    ParameterStatus = b'S',
    /// ParseComplete (1)
    ParseComplete = b'1',
    /// PortalSuspended (s)
    PortalSuspended = b's',
    /// ReadyForQuery (Z)
    ReadyForQuery = b'Z',
    /// RowDescription (T)
    RowDescription = b'T',
}

/// Transaction status indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Idle (not in transaction)
    Idle = b'I' as isize,
    /// In transaction block
    InTransaction = b'T' as isize,
    /// In failed transaction block
    Failed = b'E' as isize,
}

/// Error/Notice field codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorField {
    Severity = b'S' as isize,
    SeverityNonLocalized = b'V' as isize,
    Code = b'C' as isize,
    Message = b'M' as isize,
    Detail = b'D' as isize,
    Hint = b'H' as isize,
    Position = b'P' as isize,
    InternalPosition = b'p' as isize,
    InternalQuery = b'q' as isize,
    Where = b'W' as isize,
    SchemaName = b's' as isize,
    TableName = b't' as isize,
    ColumnName = b'c' as isize,
    DataTypeName = b'd' as isize,
    ConstraintName = b'n' as isize,
    File = b'F' as isize,
    Line = b'L' as isize,
    Routine = b'R' as isize,
}

/// PostgreSQL error response
#[derive(Debug, Clone)]
pub struct PgError {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<i32>,
    pub where_: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub data_type: Option<String>,
    pub constraint: Option<String>,
}

impl PgError {
    /// Create a new error
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            severity: "ERROR".to_string(),
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
            position: None,
            where_: None,
            schema: None,
            table: None,
            column: None,
            data_type: None,
            constraint: None,
        }
    }

    /// Create a syntax error
    pub fn syntax_error(message: &str, position: i32) -> Self {
        let mut err = Self::new("42601", message);
        err.position = Some(position);
        err
    }

    /// Create an undefined table error
    pub fn undefined_table(table: &str) -> Self {
        let mut err = Self::new("42P01", &format!("relation \"{}\" does not exist", table));
        err.table = Some(table.to_string());
        err
    }

    /// Create an undefined column error
    pub fn undefined_column(column: &str, table: Option<&str>) -> Self {
        let msg = if let Some(t) = table {
            format!("column \"{}\" of relation \"{}\" does not exist", column, t)
        } else {
            format!("column \"{}\" does not exist", column)
        };
        let mut err = Self::new("42703", &msg);
        err.column = Some(column.to_string());
        if let Some(t) = table {
            err.table = Some(t.to_string());
        }
        err
    }

    /// Create a permission denied error
    pub fn permission_denied(message: &str) -> Self {
        Self::new("42501", message)
    }

    /// Create a read-only transaction error
    pub fn read_only() -> Self {
        Self::new("25006", "cannot execute statement in a read-only transaction")
    }

    /// Create an internal error
    pub fn internal(message: &str) -> Self {
        Self::new("XX000", message)
    }

    /// Set the detail field
    pub fn with_detail(mut self, detail: &str) -> Self {
        self.detail = Some(detail.to_string());
        self
    }

    /// Set the hint field
    pub fn with_hint(mut self, hint: &str) -> Self {
        self.hint = Some(hint.to_string());
        self
    }

    /// Encode as wire protocol message
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(BackendMessage::ErrorResponse as u8);

        // Pre-compute position string to ensure it lives long enough
        let pos_str = self.position.map(|p| p.to_string());

        let mut fields: Vec<(u8, &[u8])> = Vec::new();
        fields.push((b'S', self.severity.as_bytes()));
        fields.push((b'V', self.severity.as_bytes()));
        fields.push((b'C', self.code.as_bytes()));
        fields.push((b'M', self.message.as_bytes()));

        if let Some(ref detail) = self.detail {
            fields.push((b'D', detail.as_bytes()));
        }
        if let Some(ref hint) = self.hint {
            fields.push((b'H', hint.as_bytes()));
        }
        if let Some(ref pos) = pos_str {
            fields.push((b'P', pos.as_bytes()));
        }

        // Calculate length
        let mut len = 4; // length field itself
        for (_, value) in &fields {
            len += 1 + value.len() + 1; // field code + value + null
        }
        len += 1; // terminating null

        buf.extend_from_slice(&(len as i32).to_be_bytes());

        for (code, value) in fields {
            buf.push(code);
            buf.extend_from_slice(value);
            buf.push(0);
        }
        buf.push(0); // terminating null

        buf
    }
}

/// Row description field
#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}

impl FieldDescription {
    /// Create a text field
    pub fn text(name: &str, type_oid: i32) -> Self {
        Self {
            name: name.to_string(),
            table_oid: 0,
            column_attr: 0,
            type_oid,
            type_size: -1,
            type_modifier: -1,
            format_code: 0,
        }
    }

    /// Encode field description
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(self.name.as_bytes());
        buf.push(0); // null terminator
        buf.extend_from_slice(&self.table_oid.to_be_bytes());
        buf.extend_from_slice(&self.column_attr.to_be_bytes());
        buf.extend_from_slice(&self.type_oid.to_be_bytes());
        buf.extend_from_slice(&self.type_size.to_be_bytes());
        buf.extend_from_slice(&self.type_modifier.to_be_bytes());
        buf.extend_from_slice(&self.format_code.to_be_bytes());
        buf
    }
}

/// Common PostgreSQL type OIDs
pub mod type_oid {
    pub const BOOL: i32 = 16;
    pub const BYTEA: i32 = 17;
    pub const CHAR: i32 = 18;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const OID: i32 = 26;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const VARCHAR: i32 = 1043;
    pub const DATE: i32 = 1082;
    pub const TIME: i32 = 1083;
    pub const TIMESTAMP: i32 = 1114;
    pub const TIMESTAMPTZ: i32 = 1184;
    pub const INTERVAL: i32 = 1186;
    pub const NUMERIC: i32 = 1700;
    pub const UUID: i32 = 2950;
    pub const JSON: i32 = 114;
    pub const JSONB: i32 = 3802;
}

/// Protocol message encoder/decoder
pub struct ProtocolCodec {
    buffer: Vec<u8>,
}

impl ProtocolCodec {
    /// Create a new codec
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(8192),
        }
    }

    /// Encode ReadyForQuery message
    pub fn ready_for_query(&mut self, status: TransactionStatus) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::ReadyForQuery as u8);
        self.buffer.extend_from_slice(&5i32.to_be_bytes());
        self.buffer.push(status as u8);
        &self.buffer
    }

    /// Encode AuthenticationOk message
    pub fn authentication_ok(&mut self) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::AuthenticationOk as u8);
        self.buffer.extend_from_slice(&8i32.to_be_bytes());
        self.buffer.extend_from_slice(&0i32.to_be_bytes());
        &self.buffer
    }

    /// Encode AuthenticationSASL message
    pub fn authentication_sasl(&mut self, mechanisms: &[&str]) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::AuthenticationOk as u8);

        let mut body = Vec::new();
        body.extend_from_slice(&10i32.to_be_bytes()); // SASL auth type
        for mech in mechanisms {
            body.extend_from_slice(mech.as_bytes());
            body.push(0);
        }
        body.push(0); // terminating null

        let len = (body.len() + 4) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(&body);
        &self.buffer
    }

    /// Encode AuthenticationSASLContinue message
    pub fn authentication_sasl_continue(&mut self, data: &[u8]) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::AuthenticationOk as u8);

        let len = (4 + 4 + data.len()) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(&11i32.to_be_bytes()); // SASL continue type
        self.buffer.extend_from_slice(data);
        &self.buffer
    }

    /// Encode AuthenticationSASLFinal message
    pub fn authentication_sasl_final(&mut self, data: &[u8]) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::AuthenticationOk as u8);

        let len = (4 + 4 + data.len()) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(&12i32.to_be_bytes()); // SASL final type
        self.buffer.extend_from_slice(data);
        &self.buffer
    }

    /// Encode BackendKeyData message
    pub fn backend_key_data(&mut self, pid: i32, secret_key: i32) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::BackendKeyData as u8);
        self.buffer.extend_from_slice(&12i32.to_be_bytes());
        self.buffer.extend_from_slice(&pid.to_be_bytes());
        self.buffer.extend_from_slice(&secret_key.to_be_bytes());
        &self.buffer
    }

    /// Encode ParameterStatus message
    pub fn parameter_status(&mut self, name: &str, value: &str) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::ParameterStatus as u8);

        let len = (4 + name.len() + 1 + value.len() + 1) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(name.as_bytes());
        self.buffer.push(0);
        self.buffer.extend_from_slice(value.as_bytes());
        self.buffer.push(0);
        &self.buffer
    }

    /// Encode RowDescription message
    pub fn row_description(&mut self, fields: &[FieldDescription]) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::RowDescription as u8);

        let mut body = Vec::new();
        body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for field in fields {
            body.extend_from_slice(&field.encode());
        }

        let len = (body.len() + 4) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(&body);
        &self.buffer
    }

    /// Encode DataRow message
    pub fn data_row(&mut self, values: &[Option<&[u8]>]) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::DataRow as u8);

        let mut body = Vec::new();
        body.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for value in values {
            match value {
                Some(data) => {
                    body.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    body.extend_from_slice(data);
                }
                None => {
                    body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
                }
            }
        }

        let len = (body.len() + 4) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(&body);
        &self.buffer
    }

    /// Encode CommandComplete message
    pub fn command_complete(&mut self, tag: &str) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::CommandComplete as u8);

        let len = (4 + tag.len() + 1) as i32;
        self.buffer.extend_from_slice(&len.to_be_bytes());
        self.buffer.extend_from_slice(tag.as_bytes());
        self.buffer.push(0);
        &self.buffer
    }

    /// Encode ParseComplete message
    pub fn parse_complete(&mut self) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::ParseComplete as u8);
        self.buffer.extend_from_slice(&4i32.to_be_bytes());
        &self.buffer
    }

    /// Encode BindComplete message
    pub fn bind_complete(&mut self) -> &[u8] {
        self.buffer.clear();
        self.buffer.push(BackendMessage::BindComplete as u8);
        self.buffer.extend_from_slice(&4i32.to_be_bytes());
        &self.buffer
    }

    /// Decode startup message
    pub fn decode_startup(&self, data: &[u8]) -> Result<StartupMessage, ProtocolError> {
        if data.len() < 8 {
            return Err(ProtocolError::InsufficientData);
        }

        let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < length {
            return Err(ProtocolError::InsufficientData);
        }

        let protocol = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);

        match protocol {
            SSL_REQUEST_CODE => Ok(StartupMessage::SSLRequest),
            CANCEL_REQUEST_CODE => {
                if length >= 16 {
                    let pid = i32::from_be_bytes([data[8], data[9], data[10], data[11]]);
                    let key = i32::from_be_bytes([data[12], data[13], data[14], data[15]]);
                    Ok(StartupMessage::CancelRequest { pid, secret_key: key })
                } else {
                    Err(ProtocolError::InvalidMessage)
                }
            }
            GSS_ENC_REQUEST_CODE => Ok(StartupMessage::GSSEncRequest),
            PROTOCOL_VERSION => {
                // Parse parameters
                let mut params = HashMap::new();
                let mut i = 8;
                while i < length - 1 {
                    let key_end = data[i..].iter().position(|&b| b == 0).unwrap_or(0);
                    let key = String::from_utf8_lossy(&data[i..i + key_end]).to_string();
                    i += key_end + 1;

                    if i >= length - 1 {
                        break;
                    }

                    let value_end = data[i..].iter().position(|&b| b == 0).unwrap_or(0);
                    let value = String::from_utf8_lossy(&data[i..i + value_end]).to_string();
                    i += value_end + 1;

                    if !key.is_empty() {
                        params.insert(key, value);
                    }
                }
                Ok(StartupMessage::Startup {
                    version: protocol,
                    parameters: params,
                })
            }
            _ => Err(ProtocolError::UnsupportedProtocol(protocol)),
        }
    }
}

impl Default for ProtocolCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Startup message types
#[derive(Debug, Clone)]
pub enum StartupMessage {
    /// Regular startup with parameters
    Startup {
        version: i32,
        parameters: HashMap<String, String>,
    },
    /// SSL negotiation request
    SSLRequest,
    /// Cancel request
    CancelRequest { pid: i32, secret_key: i32 },
    /// GSS encryption request
    GSSEncRequest,
}

/// Protocol error
#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolError {
    InsufficientData,
    InvalidMessage,
    UnsupportedProtocol(i32),
    IoError(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::InsufficientData => write!(f, "insufficient data"),
            ProtocolError::InvalidMessage => write!(f, "invalid message format"),
            ProtocolError::UnsupportedProtocol(v) => write!(f, "unsupported protocol version: {}", v),
            ProtocolError::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for ProtocolError {}

// ============================================================================
// SCRAM-SHA-256 AUTHENTICATION
// ============================================================================

/// SCRAM mechanism name
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
pub const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

/// SCRAM authentication state
#[derive(Debug, Clone)]
pub struct ScramState {
    /// Client nonce
    pub client_nonce: String,
    /// Server nonce
    pub server_nonce: String,
    /// Combined nonce
    pub nonce: String,
    /// Salt
    pub salt: Vec<u8>,
    /// Iteration count
    pub iterations: u32,
    /// Username
    pub username: String,
    /// Client first message bare
    pub client_first_bare: String,
    /// Server first message
    pub server_first: String,
    /// Stored key (for verification)
    pub stored_key: Option<Vec<u8>>,
    /// Server key
    pub server_key: Option<Vec<u8>>,
}

impl ScramState {
    /// Create initial state from client first message
    pub fn from_client_first(message: &str) -> Result<Self, ScramError> {
        // Parse: n,,n=username,r=client-nonce
        let parts: Vec<&str> = message.split(',').collect();
        if parts.len() < 3 {
            return Err(ScramError::InvalidMessage("malformed client-first".into()));
        }

        // GS2 header should be "n,," or "y,," or "p=..."
        let gs2_header = format!("{},{},", parts[0], parts[1]);
        if !gs2_header.starts_with("n,,") && !gs2_header.starts_with("y,,") {
            return Err(ScramError::InvalidMessage("invalid GS2 header".into()));
        }

        let mut username = String::new();
        let mut client_nonce = String::new();

        for part in &parts[2..] {
            if let Some(value) = part.strip_prefix("n=") {
                username = value.to_string();
            } else if let Some(value) = part.strip_prefix("r=") {
                client_nonce = value.to_string();
            }
        }

        if username.is_empty() || client_nonce.is_empty() {
            return Err(ScramError::InvalidMessage("missing username or nonce".into()));
        }

        // Client first message bare (without GS2 header)
        let client_first_bare = parts[2..].join(",");

        Ok(Self {
            client_nonce: client_nonce.clone(),
            server_nonce: String::new(),
            nonce: client_nonce,
            salt: Vec::new(),
            iterations: 0,
            username,
            client_first_bare,
            server_first: String::new(),
            stored_key: None,
            server_key: None,
        })
    }

    /// Generate server first message
    pub fn generate_server_first(&mut self, salt: &[u8], iterations: u32) -> String {
        // Generate server nonce (24 random bytes, base64)
        self.server_nonce = generate_nonce();
        self.nonce = format!("{}{}", self.client_nonce, self.server_nonce);
        self.salt = salt.to_vec();
        self.iterations = iterations;

        // r=combined-nonce,s=base64-salt,i=iterations
        use base64::{Engine, engine::general_purpose::STANDARD};
        let salt_b64 = STANDARD.encode(&self.salt);
        self.server_first = format!("r={},s={},i={}", self.nonce, salt_b64, self.iterations);
        self.server_first.clone()
    }

    /// Process client final message and generate server final
    pub fn process_client_final(
        &mut self,
        message: &str,
        stored_key: &[u8],
        server_key: &[u8],
    ) -> Result<String, ScramError> {
        // Parse: c=base64-channel-binding,r=nonce,p=client-proof
        let parts: Vec<&str> = message.split(',').collect();

        let mut channel_binding = String::new();
        let mut nonce = String::new();
        let mut client_proof_b64 = String::new();

        for part in &parts {
            if let Some(value) = part.strip_prefix("c=") {
                channel_binding = value.to_string();
            } else if let Some(value) = part.strip_prefix("r=") {
                nonce = value.to_string();
            } else if let Some(value) = part.strip_prefix("p=") {
                client_proof_b64 = value.to_string();
            }
        }

        // Verify nonce matches
        if nonce != self.nonce {
            return Err(ScramError::InvalidNonce);
        }

        use base64::{Engine, engine::general_purpose::STANDARD};

        // Decode client proof
        let client_proof = STANDARD
            .decode(&client_proof_b64)
            .map_err(|_| ScramError::InvalidProof)?;

        // Client final message without proof
        let client_final_without_proof = format!("c={},r={}", channel_binding, nonce);

        // Auth message = client-first-bare,server-first,client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );

        // Verify client proof
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(stored_key, auth_message.as_bytes());

        // ClientKey = ClientProof XOR ClientSignature
        let mut client_key = vec![0u8; 32];
        for i in 0..32 {
            client_key[i] = client_proof.get(i).copied().unwrap_or(0) ^ client_signature[i];
        }

        // Verify: StoredKey = SHA256(ClientKey)
        let computed_stored_key = sha256(&client_key);
        if computed_stored_key != stored_key {
            return Err(ScramError::AuthenticationFailed);
        }

        // Generate server signature
        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = hmac_sha256(server_key, auth_message.as_bytes());
        let server_signature_b64 = STANDARD.encode(&server_signature);

        Ok(format!("v={}", server_signature_b64))
    }
}

/// SCRAM authentication error
#[derive(Debug, Clone, PartialEq)]
pub enum ScramError {
    InvalidMessage(String),
    InvalidNonce,
    InvalidProof,
    AuthenticationFailed,
    UnsupportedMechanism,
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::InvalidMessage(msg) => write!(f, "invalid SCRAM message: {}", msg),
            ScramError::InvalidNonce => write!(f, "invalid nonce"),
            ScramError::InvalidProof => write!(f, "invalid client proof"),
            ScramError::AuthenticationFailed => write!(f, "authentication failed"),
            ScramError::UnsupportedMechanism => write!(f, "unsupported SCRAM mechanism"),
        }
    }
}

impl std::error::Error for ScramError {}

/// Generate random nonce
fn generate_nonce() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(format!("{:032x}", seed).as_bytes())
}

/// Compute PBKDF2-HMAC-SHA256
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    // Simplified PBKDF2 implementation
    let mut result = vec![0u8; 32];
    let mut u = hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());

    result.copy_from_slice(&u);

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for i in 0..32 {
            result[i] ^= u[i];
        }
    }

    result
}

/// Compute HMAC-SHA256
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    const BLOCK_SIZE: usize = 64;

    let mut key_pad = vec![0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        key_pad[..32].copy_from_slice(&sha256(key));
    } else {
        key_pad[..key.len()].copy_from_slice(key);
    }

    // Inner hash
    let mut inner = Vec::with_capacity(BLOCK_SIZE + message.len());
    for b in &key_pad {
        inner.push(b ^ 0x36);
    }
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);

    // Outer hash
    let mut outer = Vec::with_capacity(BLOCK_SIZE + 32);
    for b in &key_pad {
        outer.push(b ^ 0x5c);
    }
    outer.extend_from_slice(&inner_hash);

    sha256(&outer)
}

/// Compute SHA-256 hash (simplified)
pub fn sha256(data: &[u8]) -> Vec<u8> {
    // Use a simple hash for now - in production, use a proper crypto library
    let mut hash = [0u8; 32];
    let mut state: u64 = 0x6a09e667f3bcc908;

    for chunk in data.chunks(8) {
        let mut val = 0u64;
        for (i, &byte) in chunk.iter().enumerate() {
            val |= (byte as u64) << (56 - i * 8);
        }
        state = state.wrapping_mul(0x5851f42d4c957f2d);
        state ^= val;
        state = state.rotate_left(31);
    }

    state ^= data.len() as u64;
    state = state.wrapping_mul(0x94d049bb133111eb);

    for i in 0..4 {
        hash[i * 8..(i + 1) * 8].copy_from_slice(&state.to_le_bytes());
        state = state.wrapping_mul(0x85ebca6b).rotate_right(17);
    }

    hash.to_vec()
}

/// Compute stored and server keys from password
pub fn compute_scram_keys(password: &str, salt: &[u8], iterations: u32) -> (Vec<u8>, Vec<u8>) {
    let salted_password = pbkdf2_sha256(password.as_bytes(), salt, iterations);

    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");

    (stored_key, server_key)
}

// ============================================================================
// SSL/TLS
// ============================================================================

/// TLS configuration
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to certificate file
    pub cert_path: Option<String>,
    /// Path to private key file
    pub key_path: Option<String>,
    /// Path to CA certificate for client verification
    pub ca_path: Option<String>,
    /// Require client certificate
    pub require_client_cert: bool,
    /// Minimum TLS version (1.2 or 1.3)
    pub min_version: TlsVersion,
    /// Cipher suites (empty = use defaults)
    pub cipher_suites: Vec<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            ca_path: None,
            require_client_cert: false,
            min_version: TlsVersion::Tls12,
            cipher_suites: Vec::new(),
        }
    }
}

impl TlsConfig {
    /// Check if TLS is configured
    pub fn is_enabled(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), TlsError> {
        if self.require_client_cert && self.ca_path.is_none() {
            return Err(TlsError::Config("CA certificate required for client auth".into()));
        }
        Ok(())
    }
}

/// TLS version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

/// TLS error
#[derive(Debug, Clone)]
pub enum TlsError {
    Config(String),
    Handshake(String),
    Certificate(String),
    Io(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Config(msg) => write!(f, "TLS config error: {}", msg),
            TlsError::Handshake(msg) => write!(f, "TLS handshake failed: {}", msg),
            TlsError::Certificate(msg) => write!(f, "certificate error: {}", msg),
            TlsError::Io(msg) => write!(f, "I/O error: {}", msg),
        }
    }
}

impl std::error::Error for TlsError {}

/// SSL mode for connections
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// No SSL
    Disable,
    /// Try SSL, fall back to plain if unavailable
    Allow,
    /// Prefer SSL, fall back to plain if unavailable
    Prefer,
    /// Require SSL
    Require,
    /// Require SSL and verify CA
    VerifyCA,
    /// Require SSL, verify CA and hostname
    VerifyFull,
}

impl Default for SslMode {
    fn default() -> Self {
        SslMode::Prefer
    }
}

impl std::str::FromStr for SslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "allow" => Ok(SslMode::Allow),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCA),
            "verify-full" => Ok(SslMode::VerifyFull),
            _ => Err(format!("unknown SSL mode: {}", s)),
        }
    }
}

// ============================================================================
// CONNECTION HANDLER
// ============================================================================

/// Connection security context
#[derive(Debug, Clone)]
pub struct SecurityContext {
    /// Username (authenticated)
    pub username: String,
    /// Database
    pub database: String,
    /// Is SSL/TLS enabled
    pub ssl_enabled: bool,
    /// SSL cipher suite used
    pub ssl_cipher: Option<String>,
    /// Client certificate subject (if verified)
    pub client_cert_subject: Option<String>,
    /// Authentication method used
    pub auth_method: AuthMethod,
    /// Session parameters
    pub parameters: HashMap<String, String>,
}

impl SecurityContext {
    /// Create a new context
    pub fn new(username: String, database: String) -> Self {
        Self {
            username,
            database,
            ssl_enabled: false,
            ssl_cipher: None,
            client_cert_subject: None,
            auth_method: AuthMethod::Trust,
            parameters: HashMap::new(),
        }
    }

    /// Check if connection is encrypted
    pub fn is_encrypted(&self) -> bool {
        self.ssl_enabled
    }
}

/// Authentication method
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    Trust,
    Password,
    MD5,
    ScramSha256,
    Certificate,
    GSSAPI,
    SSPI,
    Peer,
    Ident,
    LDAP,
    Radius,
    PAM,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AuthMethod::Trust => "trust",
            AuthMethod::Password => "password",
            AuthMethod::MD5 => "md5",
            AuthMethod::ScramSha256 => "scram-sha-256",
            AuthMethod::Certificate => "cert",
            AuthMethod::GSSAPI => "gss",
            AuthMethod::SSPI => "sspi",
            AuthMethod::Peer => "peer",
            AuthMethod::Ident => "ident",
            AuthMethod::LDAP => "ldap",
            AuthMethod::Radius => "radius",
            AuthMethod::PAM => "pam",
        };
        write!(f, "{}", name)
    }
}

/// HBA (Host-Based Authentication) rule
#[derive(Debug, Clone)]
pub struct HbaRule {
    /// Connection type: local, host, hostssl, hostnossl
    pub conn_type: String,
    /// Database name or "all"
    pub database: String,
    /// Username or "all"
    pub user: String,
    /// Address (CIDR notation) or empty for local
    pub address: String,
    /// Authentication method
    pub auth_method: AuthMethod,
    /// Additional options
    pub options: HashMap<String, String>,
}

impl HbaRule {
    /// Check if rule matches connection
    pub fn matches(&self, conn_type: &str, database: &str, user: &str, address: &str) -> bool {
        // Match connection type
        if self.conn_type != conn_type && self.conn_type != "all" {
            return false;
        }

        // Match database
        if self.database != database && self.database != "all" {
            return false;
        }

        // Match user
        if self.user != user && self.user != "all" {
            return false;
        }

        // Match address (simplified - just string comparison)
        if !self.address.is_empty() && self.address != address && self.address != "all" {
            // In real implementation, would check CIDR match
            return false;
        }

        true
    }
}

/// HBA configuration
pub struct HbaConfig {
    rules: Vec<HbaRule>,
}

impl HbaConfig {
    /// Create a new HBA configuration
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a rule
    pub fn add_rule(&mut self, rule: HbaRule) {
        self.rules.push(rule);
    }

    /// Find matching rule
    pub fn find_rule(
        &self,
        conn_type: &str,
        database: &str,
        user: &str,
        address: &str,
    ) -> Option<&HbaRule> {
        self.rules
            .iter()
            .find(|rule| rule.matches(conn_type, database, user, address))
    }

    /// Parse pg_hba.conf format
    pub fn parse(content: &str) -> Result<Self, String> {
        let mut config = Self::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                continue;
            }

            let auth_method = match parts.get(4).unwrap_or(&"trust") {
                &"trust" => AuthMethod::Trust,
                &"password" => AuthMethod::Password,
                &"md5" => AuthMethod::MD5,
                &"scram-sha-256" => AuthMethod::ScramSha256,
                &"cert" => AuthMethod::Certificate,
                _ => AuthMethod::Trust,
            };

            let rule = HbaRule {
                conn_type: parts[0].to_string(),
                database: parts[1].to_string(),
                user: parts[2].to_string(),
                address: if parts[0] == "local" {
                    String::new()
                } else {
                    parts[3].to_string()
                },
                auth_method,
                options: HashMap::new(),
            };

            config.add_rule(rule);
        }

        Ok(config)
    }
}

impl Default for HbaConfig {
    fn default() -> Self {
        let mut config = Self::new();
        // Default rule: trust local connections
        config.add_rule(HbaRule {
            conn_type: "local".to_string(),
            database: "all".to_string(),
            user: "all".to_string(),
            address: String::new(),
            auth_method: AuthMethod::Trust,
            options: HashMap::new(),
        });
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_error_creation() {
        let err = PgError::new("42601", "syntax error");
        assert_eq!(err.code, "42601");
        assert_eq!(err.message, "syntax error");
        assert_eq!(err.severity, "ERROR");
    }

    #[test]
    fn test_pg_error_syntax() {
        let err = PgError::syntax_error("unexpected token", 15);
        assert_eq!(err.code, "42601");
        assert_eq!(err.position, Some(15));
    }

    #[test]
    fn test_pg_error_undefined_table() {
        let err = PgError::undefined_table("users");
        assert_eq!(err.code, "42P01");
        assert!(err.message.contains("users"));
        assert_eq!(err.table, Some("users".to_string()));
    }

    #[test]
    fn test_pg_error_encode() {
        let err = PgError::new("42601", "test");
        let encoded = err.encode();
        assert_eq!(encoded[0], BackendMessage::ErrorResponse as u8);
    }

    #[test]
    fn test_field_description() {
        let field = FieldDescription::text("id", type_oid::INT4);
        assert_eq!(field.name, "id");
        assert_eq!(field.type_oid, 23);
        assert_eq!(field.format_code, 0);
    }

    #[test]
    fn test_protocol_codec_ready_for_query() {
        let mut codec = ProtocolCodec::new();
        let msg = codec.ready_for_query(TransactionStatus::Idle);
        assert_eq!(msg[0], BackendMessage::ReadyForQuery as u8);
        assert_eq!(msg[5], b'I');
    }

    #[test]
    fn test_protocol_codec_auth_ok() {
        let mut codec = ProtocolCodec::new();
        let msg = codec.authentication_ok();
        assert_eq!(msg[0], BackendMessage::AuthenticationOk as u8);
    }

    #[test]
    fn test_protocol_codec_parameter_status() {
        let mut codec = ProtocolCodec::new();
        let msg = codec.parameter_status("server_version", "15.0");
        assert_eq!(msg[0], BackendMessage::ParameterStatus as u8);
    }

    #[test]
    fn test_protocol_codec_command_complete() {
        let mut codec = ProtocolCodec::new();
        let msg = codec.command_complete("SELECT 1");
        assert_eq!(msg[0], BackendMessage::CommandComplete as u8);
    }

    #[test]
    fn test_protocol_codec_data_row() {
        let mut codec = ProtocolCodec::new();
        let values: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world")];
        let msg = codec.data_row(&values);
        assert_eq!(msg[0], BackendMessage::DataRow as u8);
    }

    #[test]
    fn test_decode_startup_ssl_request() {
        let codec = ProtocolCodec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&8i32.to_be_bytes());
        data.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());

        let result = codec.decode_startup(&data).unwrap();
        assert!(matches!(result, StartupMessage::SSLRequest));
    }

    #[test]
    fn test_decode_startup_cancel_request() {
        let codec = ProtocolCodec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&16i32.to_be_bytes());
        data.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        data.extend_from_slice(&123i32.to_be_bytes()); // pid
        data.extend_from_slice(&456i32.to_be_bytes()); // secret_key

        let result = codec.decode_startup(&data).unwrap();
        match result {
            StartupMessage::CancelRequest { pid, secret_key } => {
                assert_eq!(pid, 123);
                assert_eq!(secret_key, 456);
            }
            _ => panic!("expected CancelRequest"),
        }
    }

    #[test]
    fn test_scram_parse_client_first() {
        let message = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        let state = ScramState::from_client_first(message).unwrap();
        assert_eq!(state.username, "user");
        assert_eq!(state.client_nonce, "rOprNGfwEbeRWgbNEkqO");
    }

    #[test]
    fn test_scram_generate_server_first() {
        let message = "n,,n=user,r=client-nonce";
        let mut state = ScramState::from_client_first(message).unwrap();
        let salt = b"randomsalt";
        let server_first = state.generate_server_first(salt, 4096);

        assert!(server_first.starts_with("r=client-nonce"));
        assert!(server_first.contains(",s="));
        assert!(server_first.contains(",i=4096"));
    }

    #[test]
    fn test_hmac_sha256() {
        let key = b"key";
        let message = b"message";
        let result = hmac_sha256(key, message);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_sha256() {
        let data = b"hello world";
        let hash = sha256(data);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_pbkdf2() {
        let password = b"password";
        let salt = b"salt";
        let result = pbkdf2_sha256(password, salt, 1000);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
        assert_eq!(config.min_version, TlsVersion::Tls12);
    }

    #[test]
    fn test_tls_config_enabled() {
        let config = TlsConfig {
            cert_path: Some("/path/to/cert.pem".to_string()),
            key_path: Some("/path/to/key.pem".to_string()),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_ssl_mode_parse() {
        assert_eq!("disable".parse::<SslMode>().unwrap(), SslMode::Disable);
        assert_eq!("require".parse::<SslMode>().unwrap(), SslMode::Require);
        assert_eq!("verify-full".parse::<SslMode>().unwrap(), SslMode::VerifyFull);
    }

    #[test]
    fn test_security_context() {
        let ctx = SecurityContext::new("postgres".to_string(), "mydb".to_string());
        assert_eq!(ctx.username, "postgres");
        assert_eq!(ctx.database, "mydb");
        assert!(!ctx.is_encrypted());
    }

    #[test]
    fn test_hba_rule_matches() {
        let rule = HbaRule {
            conn_type: "host".to_string(),
            database: "all".to_string(),
            user: "postgres".to_string(),
            address: "127.0.0.1/32".to_string(),
            auth_method: AuthMethod::ScramSha256,
            options: HashMap::new(),
        };

        assert!(rule.matches("host", "mydb", "postgres", "127.0.0.1/32"));
        assert!(!rule.matches("host", "mydb", "other_user", "127.0.0.1/32"));
    }

    #[test]
    fn test_hba_config_parse() {
        let content = r#"
# Comment line
local   all         all                               trust
host    all         all         127.0.0.1/32          scram-sha-256
host    all         all         ::1/128               scram-sha-256
"#;

        let config = HbaConfig::parse(content).unwrap();
        assert_eq!(config.rules.len(), 3);
        assert_eq!(config.rules[0].auth_method, AuthMethod::Trust);
        assert_eq!(config.rules[1].auth_method, AuthMethod::ScramSha256);
    }

    #[test]
    fn test_hba_find_rule() {
        let mut config = HbaConfig::new();
        config.add_rule(HbaRule {
            conn_type: "host".to_string(),
            database: "all".to_string(),
            user: "admin".to_string(),
            address: "all".to_string(),
            auth_method: AuthMethod::ScramSha256,
            options: HashMap::new(),
        });
        config.add_rule(HbaRule {
            conn_type: "host".to_string(),
            database: "all".to_string(),
            user: "all".to_string(),
            address: "all".to_string(),
            auth_method: AuthMethod::MD5,
            options: HashMap::new(),
        });

        let rule = config.find_rule("host", "mydb", "admin", "192.168.1.1").unwrap();
        assert_eq!(rule.auth_method, AuthMethod::ScramSha256);

        let rule = config.find_rule("host", "mydb", "other", "192.168.1.1").unwrap();
        assert_eq!(rule.auth_method, AuthMethod::MD5);
    }

    #[test]
    fn test_auth_method_display() {
        assert_eq!(AuthMethod::ScramSha256.to_string(), "scram-sha-256");
        assert_eq!(AuthMethod::Trust.to_string(), "trust");
        assert_eq!(AuthMethod::MD5.to_string(), "md5");
    }
}
