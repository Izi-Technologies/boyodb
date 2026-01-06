use thiserror::Error;

/// Error type for boyodb operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Connection error.
    #[error("Connection error: {0}")]
    Connection(String),

    /// Query error.
    #[error("Query error: {0}")]
    Query(String),

    /// Authentication error.
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Timeout error.
    #[error("Timeout: {0}")]
    Timeout(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Base64 decode error.
    #[error("Base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}
