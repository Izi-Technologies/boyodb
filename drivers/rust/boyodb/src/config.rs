use std::time::Duration;

/// Configuration options for the boyodb client.
#[derive(Clone, Debug)]
pub struct Config {
    /// Enable TLS encryption.
    pub tls: bool,

    /// Path to CA certificate file.
    pub ca_file: Option<String>,

    /// Skip TLS certificate verification.
    /// WARNING: SECURITY RISK - Disables certificate validation, making connections
    /// vulnerable to man-in-the-middle (MITM) attacks. NEVER use in production.
    pub insecure_skip_verify: bool,

    /// Connection timeout.
    pub connect_timeout: Duration,

    /// Read timeout.
    pub read_timeout: Duration,

    /// Write timeout.
    pub write_timeout: Duration,

    /// Authentication token.
    pub token: Option<String>,

    /// Maximum connection retry attempts.
    pub max_retries: usize,

    /// Delay between retries.
    pub retry_delay: Duration,

    /// Default database for queries.
    pub database: Option<String>,

    /// Default query timeout in milliseconds.
    pub query_timeout: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tls: false,
            ca_file: None,
            insecure_skip_verify: false,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            token: None,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            database: None,
            query_timeout: 30000,
        }
    }
}

impl Config {
    /// Create a new Config builder.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

/// Builder for Config.
#[derive(Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Enable TLS.
    pub fn tls(mut self, enabled: bool) -> Self {
        self.config.tls = enabled;
        self
    }

    /// Set CA file path.
    pub fn ca_file(mut self, path: impl Into<String>) -> Self {
        self.config.ca_file = Some(path.into());
        self
    }

    /// Set insecure skip verify (NOT RECOMMENDED).
    pub fn insecure_skip_verify(mut self, skip: bool) -> Self {
        self.config.insecure_skip_verify = skip;
        self
    }

    /// Set connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set read timeout.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = timeout;
        self
    }

    /// Set write timeout.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.config.write_timeout = timeout;
        self
    }

    /// Set authentication token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// Set max retries.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set retry delay.
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.config.retry_delay = delay;
        self
    }

    /// Set default database.
    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.config.database = Some(db.into());
        self
    }

    /// Set query timeout in milliseconds.
    pub fn query_timeout(mut self, timeout: u32) -> Self {
        self.config.query_timeout = timeout;
        self
    }

    /// Build the Config.
    pub fn build(self) -> Config {
        self.config
    }
}
