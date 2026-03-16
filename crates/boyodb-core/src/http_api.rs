//! HTTP/REST API Module
//!
//! Provides HTTP/REST API alongside the PostgreSQL wire protocol.
//! Features:
//! - JSON request/response format
//! - Streaming query results
//! - OpenAPI specification
//! - CORS support
//! - Authentication (API keys, JWT)
//! - Rate limiting
//! - Request logging

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// HTTP API configuration
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// HTTP port
    pub port: u16,
    /// Enable HTTPS
    pub tls_enabled: bool,
    /// TLS certificate path
    pub tls_cert_path: Option<String>,
    /// TLS key path
    pub tls_key_path: Option<String>,
    /// Enable CORS
    pub cors_enabled: bool,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
    /// Enable rate limiting
    pub rate_limit_enabled: bool,
    /// Rate limit (requests per second)
    pub rate_limit_rps: u32,
    /// Maximum request body size (bytes)
    pub max_body_size: usize,
    /// Request timeout (seconds)
    pub request_timeout_secs: u64,
    /// Enable request logging
    pub request_logging: bool,
    /// Enable API key authentication
    pub api_key_auth_enabled: bool,
    /// Enable JWT authentication
    pub jwt_auth_enabled: bool,
    /// JWT secret
    pub jwt_secret: Option<String>,
    /// Streaming batch size
    pub streaming_batch_size: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            cors_enabled: true,
            cors_origins: vec!["*".to_string()],
            rate_limit_enabled: true,
            rate_limit_rps: 1000,
            max_body_size: 10 * 1024 * 1024, // 10MB
            request_timeout_secs: 30,
            request_logging: true,
            api_key_auth_enabled: false,
            jwt_auth_enabled: false,
            jwt_secret: None,
            streaming_batch_size: 10000,
        }
    }
}

/// HTTP method
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Options,
    Head,
}

impl HttpMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PUT" => Some(HttpMethod::Put),
            "DELETE" => Some(HttpMethod::Delete),
            "PATCH" => Some(HttpMethod::Patch),
            "OPTIONS" => Some(HttpMethod::Options),
            "HEAD" => Some(HttpMethod::Head),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Options => "OPTIONS",
            HttpMethod::Head => "HEAD",
        }
    }
}

/// HTTP status codes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpStatus {
    Ok = 200,
    Created = 201,
    Accepted = 202,
    NoContent = 204,
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    MethodNotAllowed = 405,
    Conflict = 409,
    PayloadTooLarge = 413,
    UnprocessableEntity = 422,
    TooManyRequests = 429,
    InternalServerError = 500,
    ServiceUnavailable = 503,
}

impl HttpStatus {
    pub fn reason_phrase(&self) -> &'static str {
        match self {
            HttpStatus::Ok => "OK",
            HttpStatus::Created => "Created",
            HttpStatus::Accepted => "Accepted",
            HttpStatus::NoContent => "No Content",
            HttpStatus::BadRequest => "Bad Request",
            HttpStatus::Unauthorized => "Unauthorized",
            HttpStatus::Forbidden => "Forbidden",
            HttpStatus::NotFound => "Not Found",
            HttpStatus::MethodNotAllowed => "Method Not Allowed",
            HttpStatus::Conflict => "Conflict",
            HttpStatus::PayloadTooLarge => "Payload Too Large",
            HttpStatus::UnprocessableEntity => "Unprocessable Entity",
            HttpStatus::TooManyRequests => "Too Many Requests",
            HttpStatus::InternalServerError => "Internal Server Error",
            HttpStatus::ServiceUnavailable => "Service Unavailable",
        }
    }
}

/// HTTP request
#[derive(Clone, Debug)]
pub struct HttpRequest {
    /// HTTP method
    pub method: HttpMethod,
    /// Request path
    pub path: String,
    /// Query parameters
    pub query_params: HashMap<String, String>,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Request body
    pub body: Vec<u8>,
    /// Remote IP address
    pub remote_ip: Option<String>,
    /// Request ID for tracing
    pub request_id: String,
    /// Request start time
    pub start_time: Instant,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            query_params: HashMap::new(),
            headers: HashMap::new(),
            body: Vec::new(),
            remote_ip: None,
            request_id: generate_request_id(),
            start_time: Instant::now(),
        }
    }

    /// Get a header value
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_lowercase()).map(|s| s.as_str())
    }

    /// Get content type
    pub fn content_type(&self) -> Option<&str> {
        self.header("content-type")
    }

    /// Check if request accepts JSON
    pub fn accepts_json(&self) -> bool {
        self.header("accept")
            .map(|a| a.contains("application/json") || a.contains("*/*"))
            .unwrap_or(true)
    }

    /// Parse body as JSON
    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, ApiError> {
        serde_json::from_slice(&self.body).map_err(|e| ApiError::InvalidJson(e.to_string()))
    }

    /// Get authorization token
    pub fn auth_token(&self) -> Option<&str> {
        self.header("authorization")
            .and_then(|h| h.strip_prefix("Bearer "))
    }

    /// Get API key
    pub fn api_key(&self) -> Option<&str> {
        self.header("x-api-key")
    }
}

/// HTTP response
#[derive(Clone, Debug)]
pub struct HttpResponse {
    /// Status code
    pub status: HttpStatus,
    /// Response headers
    pub headers: HashMap<String, String>,
    /// Response body
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: HttpStatus) -> Self {
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());

        Self {
            status,
            headers,
            body: Vec::new(),
        }
    }

    pub fn ok() -> Self {
        Self::new(HttpStatus::Ok)
    }

    pub fn created() -> Self {
        Self::new(HttpStatus::Created)
    }

    pub fn no_content() -> Self {
        Self::new(HttpStatus::NoContent)
    }

    pub fn bad_request(message: &str) -> Self {
        Self::error(HttpStatus::BadRequest, message)
    }

    pub fn unauthorized(message: &str) -> Self {
        Self::error(HttpStatus::Unauthorized, message)
    }

    pub fn not_found(message: &str) -> Self {
        Self::error(HttpStatus::NotFound, message)
    }

    pub fn internal_error(message: &str) -> Self {
        Self::error(HttpStatus::InternalServerError, message)
    }

    pub fn error(status: HttpStatus, message: &str) -> Self {
        let mut resp = Self::new(status);
        resp.body = serde_json::to_vec(&ErrorResponse {
            error: message.to_string(),
            code: status as u16,
        })
        .unwrap_or_default();
        resp
    }

    pub fn json<T: Serialize>(data: &T) -> Result<Self, ApiError> {
        let mut resp = Self::ok();
        resp.body =
            serde_json::to_vec(data).map_err(|e| ApiError::SerializationError(e.to_string()))?;
        Ok(resp)
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn with_cors(mut self, origin: &str) -> Self {
        self.headers.insert(
            "access-control-allow-origin".to_string(),
            origin.to_string(),
        );
        self.headers.insert(
            "access-control-allow-methods".to_string(),
            "GET, POST, PUT, DELETE, OPTIONS".to_string(),
        );
        self.headers.insert(
            "access-control-allow-headers".to_string(),
            "Content-Type, Authorization, X-API-Key".to_string(),
        );
        self
    }
}

/// Error response format
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    code: u16,
}

/// API error types
#[derive(Debug, Clone)]
pub enum ApiError {
    /// Invalid JSON in request body
    InvalidJson(String),
    /// Missing required parameter
    MissingParameter(String),
    /// Invalid parameter value
    InvalidParameter(String, String),
    /// Authentication failed
    Unauthorized(String),
    /// Permission denied
    Forbidden(String),
    /// Resource not found
    NotFound(String),
    /// Database error
    DatabaseError(String),
    /// Serialization error
    SerializationError(String),
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Request timeout
    Timeout,
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::InvalidJson(msg) => write!(f, "Invalid JSON: {}", msg),
            ApiError::MissingParameter(param) => write!(f, "Missing parameter: {}", param),
            ApiError::InvalidParameter(param, msg) => {
                write!(f, "Invalid parameter '{}': {}", param, msg)
            }
            ApiError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ApiError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            ApiError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ApiError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            ApiError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ApiError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            ApiError::Timeout => write!(f, "Request timeout"),
            ApiError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn to_response(&self) -> HttpResponse {
        match self {
            ApiError::InvalidJson(_)
            | ApiError::MissingParameter(_)
            | ApiError::InvalidParameter(_, _) => HttpResponse::bad_request(&self.to_string()),
            ApiError::Unauthorized(_) => HttpResponse::unauthorized(&self.to_string()),
            ApiError::Forbidden(_) => HttpResponse::error(HttpStatus::Forbidden, &self.to_string()),
            ApiError::NotFound(_) => HttpResponse::not_found(&self.to_string()),
            ApiError::RateLimitExceeded => {
                HttpResponse::error(HttpStatus::TooManyRequests, &self.to_string())
            }
            ApiError::Timeout => {
                HttpResponse::error(HttpStatus::ServiceUnavailable, &self.to_string())
            }
            _ => HttpResponse::internal_error(&self.to_string()),
        }
    }
}

// ============ API Request/Response Types ============

/// Query request
#[derive(Deserialize)]
pub struct QueryRequest {
    /// SQL query
    pub sql: String,
    /// Default database
    #[serde(default)]
    pub database: Option<String>,
    /// Query timeout in milliseconds
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Maximum rows to return
    #[serde(default)]
    pub limit: Option<usize>,
    /// Output format (json, csv, arrow)
    #[serde(default = "default_format")]
    pub format: String,
    /// Enable streaming response
    #[serde(default)]
    pub stream: bool,
}

fn default_timeout() -> u64 {
    30000
}

fn default_format() -> String {
    "json".to_string()
}

/// Query response
#[derive(Serialize)]
pub struct QueryResponse {
    /// Query status
    pub status: String,
    /// Number of rows
    pub rows_count: usize,
    /// Column names
    pub columns: Vec<String>,
    /// Column types
    pub types: Vec<String>,
    /// Row data
    pub data: Vec<Vec<serde_json::Value>>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Rows affected (for DML)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
}

/// Ingest request
#[derive(Deserialize)]
pub struct IngestRequest {
    /// Database name
    pub database: String,
    /// Table name
    pub table: String,
    /// Data format (json, csv, ndjson)
    #[serde(default = "default_ingest_format")]
    pub format: String,
    /// Column names (for CSV without header)
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Create table if not exists
    #[serde(default)]
    pub create_table: bool,
}

fn default_ingest_format() -> String {
    "json".to_string()
}

/// Ingest response
#[derive(Serialize)]
pub struct IngestResponse {
    pub status: String,
    pub rows_ingested: u64,
    pub bytes_ingested: u64,
    pub duration_ms: u64,
}

/// Database info response
#[derive(Serialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub tables_count: usize,
    pub size_bytes: u64,
}

/// Table info response
#[derive(Serialize)]
pub struct TableInfo {
    pub name: String,
    pub database: String,
    pub columns: Vec<ColumnInfo>,
    pub rows_count: u64,
    pub size_bytes: u64,
    pub segments_count: usize,
}

/// Column info
#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub databases_count: usize,
    pub tables_count: usize,
    pub segments_count: usize,
    pub memory_bytes: u64,
}

/// Metrics response
#[derive(Serialize)]
pub struct MetricsResponse {
    pub queries_total: u64,
    pub queries_active: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub errors_total: u64,
    pub connections_active: u64,
    pub cache_hit_rate: f64,
}

// ============ Route Handler Types ============

/// Route handler function type
pub type RouteHandler = Box<dyn Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync>;

/// Route definition
pub struct Route {
    pub method: HttpMethod,
    pub path: String,
    pub handler: RouteHandler,
}

/// HTTP router
pub struct Router {
    routes: Vec<Route>,
    not_found_handler: Option<RouteHandler>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            not_found_handler: None,
        }
    }

    /// Add a route
    pub fn route(
        &mut self,
        method: HttpMethod,
        path: impl Into<String>,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.routes.push(Route {
            method,
            path: path.into(),
            handler: Box::new(handler),
        });
    }

    /// Add GET route
    pub fn get(
        &mut self,
        path: impl Into<String>,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.route(HttpMethod::Get, path, handler);
    }

    /// Add POST route
    pub fn post(
        &mut self,
        path: impl Into<String>,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.route(HttpMethod::Post, path, handler);
    }

    /// Add PUT route
    pub fn put(
        &mut self,
        path: impl Into<String>,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.route(HttpMethod::Put, path, handler);
    }

    /// Add DELETE route
    pub fn delete(
        &mut self,
        path: impl Into<String>,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.route(HttpMethod::Delete, path, handler);
    }

    /// Set 404 handler
    pub fn not_found(
        &mut self,
        handler: impl Fn(&HttpRequest) -> Result<HttpResponse, ApiError> + Send + Sync + 'static,
    ) {
        self.not_found_handler = Some(Box::new(handler));
    }

    /// Match a request to a route
    pub fn match_route(&self, req: &HttpRequest) -> Option<&Route> {
        self.routes
            .iter()
            .find(|route| route.method == req.method && self.path_matches(&route.path, &req.path))
    }

    /// Handle a request
    pub fn handle(&self, req: &HttpRequest) -> HttpResponse {
        // Handle OPTIONS for CORS preflight
        if req.method == HttpMethod::Options {
            return HttpResponse::no_content().with_cors("*");
        }

        match self.match_route(req) {
            Some(route) => match (route.handler)(req) {
                Ok(resp) => resp,
                Err(e) => e.to_response(),
            },
            None => {
                if let Some(ref handler) = self.not_found_handler {
                    match handler(req) {
                        Ok(resp) => resp,
                        Err(e) => e.to_response(),
                    }
                } else {
                    HttpResponse::not_found("Route not found")
                }
            }
        }
    }

    fn path_matches(&self, pattern: &str, path: &str) -> bool {
        // Simple path matching (supports :param syntax)
        let pattern_parts: Vec<&str> = pattern.split('/').collect();
        let path_parts: Vec<&str> = path.split('/').collect();

        if pattern_parts.len() != path_parts.len() {
            return false;
        }

        for (p, a) in pattern_parts.iter().zip(path_parts.iter()) {
            if p.starts_with(':') {
                continue; // Parameter - matches anything
            }
            if p != a {
                return false;
            }
        }

        true
    }

    /// Extract path parameters
    pub fn extract_params(&self, pattern: &str, path: &str) -> HashMap<String, String> {
        let mut params = HashMap::new();
        let pattern_parts: Vec<&str> = pattern.split('/').collect();
        let path_parts: Vec<&str> = path.split('/').collect();

        for (p, a) in pattern_parts.iter().zip(path_parts.iter()) {
            if let Some(param_name) = p.strip_prefix(':') {
                params.insert(param_name.to_string(), a.to_string());
            }
        }

        params
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

// ============ Rate Limiter ============

/// Token bucket rate limiter
pub struct RateLimiter {
    /// Requests per second limit
    rps: u32,
    /// Bucket capacity
    capacity: u32,
    /// Current tokens per IP
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Total requests
    total_requests: AtomicU64,
    /// Rejected requests
    rejected_requests: AtomicU64,
}

struct TokenBucket {
    tokens: f64,
    last_update: Instant,
}

impl RateLimiter {
    pub fn new(rps: u32) -> Self {
        Self {
            rps,
            capacity: rps * 2, // Allow burst
            buckets: RwLock::new(HashMap::new()),
            total_requests: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
        }
    }

    /// Check if a request is allowed
    pub fn check(&self, ip: &str) -> bool {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        let mut buckets = self.buckets.write();
        let now = Instant::now();

        let bucket = buckets.entry(ip.to_string()).or_insert(TokenBucket {
            tokens: self.capacity as f64,
            last_update: now,
        });

        // Refill tokens based on time elapsed
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rps as f64).min(self.capacity as f64);
        bucket.last_update = now;

        // Try to consume a token
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            self.rejected_requests.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Clean up old buckets
    pub fn cleanup(&self, max_age: Duration) {
        let mut buckets = self.buckets.write();
        let now = Instant::now();
        buckets.retain(|_, bucket| now.duration_since(bucket.last_update) < max_age);
    }

    /// Get statistics
    pub fn stats(&self) -> (u64, u64) {
        (
            self.total_requests.load(Ordering::Relaxed),
            self.rejected_requests.load(Ordering::Relaxed),
        )
    }
}

// ============ OpenAPI Spec Generation ============

/// OpenAPI specification generator
pub struct OpenApiGenerator {
    title: String,
    version: String,
    description: String,
}

impl OpenApiGenerator {
    pub fn new(title: &str, version: &str, description: &str) -> Self {
        Self {
            title: title.to_string(),
            version: version.to_string(),
            description: description.to_string(),
        }
    }

    /// Generate OpenAPI 3.0 specification
    pub fn generate(&self) -> serde_json::Value {
        serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": self.title,
                "version": self.version,
                "description": self.description
            },
            "servers": [
                {"url": "/api/v1", "description": "API v1"}
            ],
            "paths": {
                "/query": {
                    "post": {
                        "summary": "Execute SQL query",
                        "requestBody": {
                            "required": true,
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/QueryRequest"}
                                }
                            }
                        },
                        "responses": {
                            "200": {
                                "description": "Query executed successfully",
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": "#/components/schemas/QueryResponse"}
                                    }
                                }
                            }
                        }
                    }
                },
                "/databases": {
                    "get": {
                        "summary": "List all databases",
                        "responses": {
                            "200": {
                                "description": "List of databases",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/DatabaseInfo"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "/databases/{database}/tables": {
                    "get": {
                        "summary": "List tables in a database",
                        "parameters": [
                            {"name": "database", "in": "path", "required": true, "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {
                                "description": "List of tables"
                            }
                        }
                    }
                },
                "/ingest": {
                    "post": {
                        "summary": "Ingest data into a table",
                        "requestBody": {
                            "required": true,
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/IngestRequest"}
                                }
                            }
                        },
                        "responses": {
                            "200": {
                                "description": "Data ingested successfully"
                            }
                        }
                    }
                },
                "/health": {
                    "get": {
                        "summary": "Health check",
                        "responses": {
                            "200": {
                                "description": "Server is healthy",
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": "#/components/schemas/HealthResponse"}
                                    }
                                }
                            }
                        }
                    }
                },
                "/metrics": {
                    "get": {
                        "summary": "Server metrics",
                        "responses": {
                            "200": {
                                "description": "Server metrics"
                            }
                        }
                    }
                }
            },
            "components": {
                "schemas": {
                    "QueryRequest": {
                        "type": "object",
                        "required": ["sql"],
                        "properties": {
                            "sql": {"type": "string"},
                            "database": {"type": "string"},
                            "timeout_ms": {"type": "integer", "default": 30000},
                            "limit": {"type": "integer"},
                            "format": {"type": "string", "enum": ["json", "csv", "arrow"]},
                            "stream": {"type": "boolean", "default": false}
                        }
                    },
                    "QueryResponse": {
                        "type": "object",
                        "properties": {
                            "status": {"type": "string"},
                            "rows_count": {"type": "integer"},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "types": {"type": "array", "items": {"type": "string"}},
                            "data": {"type": "array"},
                            "execution_time_ms": {"type": "integer"}
                        }
                    },
                    "DatabaseInfo": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "tables_count": {"type": "integer"},
                            "size_bytes": {"type": "integer"}
                        }
                    },
                    "TableInfo": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "database": {"type": "string"},
                            "columns": {"type": "array"},
                            "rows_count": {"type": "integer"},
                            "size_bytes": {"type": "integer"}
                        }
                    },
                    "HealthResponse": {
                        "type": "object",
                        "properties": {
                            "status": {"type": "string"},
                            "version": {"type": "string"},
                            "uptime_secs": {"type": "integer"}
                        }
                    },
                    "IngestRequest": {
                        "type": "object",
                        "required": ["database", "table"],
                        "properties": {
                            "database": {"type": "string"},
                            "table": {"type": "string"},
                            "format": {"type": "string", "enum": ["json", "csv", "ndjson"]},
                            "columns": {"type": "array", "items": {"type": "string"}},
                            "create_table": {"type": "boolean"}
                        }
                    }
                },
                "securitySchemes": {
                    "ApiKeyAuth": {
                        "type": "apiKey",
                        "in": "header",
                        "name": "X-API-Key"
                    },
                    "BearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    }
                }
            }
        })
    }
}

// ============ Helper Functions ============

fn generate_request_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:016x}", now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_matching() {
        let mut router = Router::new();
        router.get("/health", |_| Ok(HttpResponse::ok()));
        router.get("/databases/:name", |_| Ok(HttpResponse::ok()));
        router.post("/query", |_| Ok(HttpResponse::ok()));

        let req = HttpRequest::new(HttpMethod::Get, "/health");
        assert!(router.match_route(&req).is_some());

        let req = HttpRequest::new(HttpMethod::Get, "/databases/test");
        assert!(router.match_route(&req).is_some());

        let req = HttpRequest::new(HttpMethod::Get, "/notfound");
        assert!(router.match_route(&req).is_none());
    }

    #[test]
    fn test_path_params() {
        let router = Router::new();
        let params = router.extract_params(
            "/databases/:name/tables/:table",
            "/databases/mydb/tables/users",
        );

        assert_eq!(params.get("name"), Some(&"mydb".to_string()));
        assert_eq!(params.get("table"), Some(&"users".to_string()));
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(10);

        // First requests should pass
        for _ in 0..10 {
            assert!(limiter.check("127.0.0.1"));
        }

        // Should be rate limited now (burst allows some extra)
        let mut rejected = 0;
        for _ in 0..30 {
            if !limiter.check("127.0.0.1") {
                rejected += 1;
            }
        }
        assert!(rejected > 0);
    }

    #[test]
    fn test_http_response() {
        let resp = HttpResponse::json(&serde_json::json!({"test": "value"})).unwrap();
        assert_eq!(resp.status, HttpStatus::Ok);
        assert!(!resp.body.is_empty());

        let resp = HttpResponse::bad_request("test error");
        assert_eq!(resp.status, HttpStatus::BadRequest);
    }

    #[test]
    fn test_openapi_generation() {
        let generator = OpenApiGenerator::new("BoyoDB API", "1.0.0", "Test API");
        let spec = generator.generate();

        assert_eq!(spec["openapi"], "3.0.0");
        assert_eq!(spec["info"]["title"], "BoyoDB API");
        assert!(spec["paths"]["/query"].is_object());
    }
}
