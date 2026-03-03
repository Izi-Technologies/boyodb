//! Connection pool for BoyoDB client.
//!
//! Provides efficient connection reuse for high-throughput applications.

use crate::client::Client;
use crate::config::Config;
use crate::error::Error;

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to keep in the pool.
    pub min_connections: usize,
    /// Maximum number of connections allowed.
    pub max_connections: usize,
    /// How long to wait for a connection before timing out.
    pub acquire_timeout: Duration,
    /// How long a connection can be idle before being closed.
    pub idle_timeout: Duration,
    /// How long a connection can live before being recycled.
    pub max_lifetime: Duration,
    /// Client configuration for new connections.
    pub client_config: Config,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(3600),
            client_config: Config::default(),
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration with the given max connections.
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections,
            ..Default::default()
        }
    }

    /// Set the client configuration.
    pub fn with_client_config(mut self, config: Config) -> Self {
        self.client_config = config;
        self
    }

    /// Set the minimum connections.
    pub fn with_min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set the acquire timeout.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set the idle timeout.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }
}

/// A pooled connection wrapper.
struct PooledConnection {
    client: Client,
    created_at: Instant,
    last_used: Instant,
}

impl PooledConnection {
    fn new(client: Client) -> Self {
        let now = Instant::now();
        Self {
            client,
            created_at: now,
            last_used: now,
        }
    }

    fn is_expired(&self, config: &PoolConfig) -> bool {
        self.created_at.elapsed() > config.max_lifetime
    }

    fn is_idle_expired(&self, config: &PoolConfig) -> bool {
        self.last_used.elapsed() > config.idle_timeout
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

/// A connection pool for BoyoDB.
pub struct Pool {
    host: String,
    config: PoolConfig,
    connections: Mutex<VecDeque<PooledConnection>>,
    semaphore: Arc<Semaphore>,
    total_connections: AtomicUsize,
    waiting: AtomicUsize,
}

impl Pool {
    /// Create a new connection pool.
    pub async fn new(host: &str, config: PoolConfig) -> Result<Arc<Self>, Error> {
        let pool = Arc::new(Self {
            host: host.to_string(),
            semaphore: Arc::new(Semaphore::new(config.max_connections)),
            config,
            connections: Mutex::new(VecDeque::new()),
            total_connections: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
        });

        // Pre-create minimum connections
        for _ in 0..pool.config.min_connections {
            let client = Client::connect(&pool.host, pool.config.client_config.clone()).await?;
            let mut conns = pool.connections.lock();
            conns.push_back(PooledConnection::new(client));
            pool.total_connections.fetch_add(1, Ordering::SeqCst);
        }

        Ok(pool)
    }

    /// Get a connection from the pool.
    pub async fn get(&self) -> Result<PooledClient<'_>, Error> {
        self.waiting.fetch_add(1, Ordering::SeqCst);

        // Try to acquire a permit with timeout
        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            Error::Pool("Connection acquire timeout".into())
        })?
        .map_err(|_| {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            Error::Pool("Semaphore closed".into())
        })?;

        self.waiting.fetch_sub(1, Ordering::SeqCst);

        // Try to get an existing connection
        loop {
            let maybe_conn = {
                let mut conns = self.connections.lock();
                conns.pop_front()
            };

            match maybe_conn {
                Some(mut conn) => {
                    // Check if connection is still valid
                    if conn.is_expired(&self.config) || conn.is_idle_expired(&self.config) {
                        // Connection expired, close it and try again
                        let _ = conn.client.close().await;
                        self.total_connections.fetch_sub(1, Ordering::SeqCst);
                        continue;
                    }

                    // Verify connection is alive
                    match conn.client.health().await {
                        Ok(_) => {
                            conn.touch();
                            return Ok(PooledClient {
                                client: Some(conn.client),
                                pool: self,
                                _permit: permit,
                            });
                        }
                        Err(_) => {
                            // Connection dead, close and try again
                            let _ = conn.client.close().await;
                            self.total_connections.fetch_sub(1, Ordering::SeqCst);
                            continue;
                        }
                    }
                }
                None => {
                    // No available connection, create a new one
                    break;
                }
            }
        }

        // Create a new connection
        let client = Client::connect(&self.host, self.config.client_config.clone()).await?;
        self.total_connections.fetch_add(1, Ordering::SeqCst);

        Ok(PooledClient {
            client: Some(client),
            pool: self,
            _permit: permit,
        })
    }

    /// Return a connection to the pool.
    fn return_connection(&self, client: Client) {
        let mut conns = self.connections.lock();
        conns.push_back(PooledConnection::new(client));
    }

    /// Get pool statistics.
    pub fn stats(&self) -> PoolStats {
        let conns = self.connections.lock();
        PoolStats {
            total_connections: self.total_connections.load(Ordering::SeqCst),
            idle_connections: conns.len(),
            waiting: self.waiting.load(Ordering::SeqCst),
            max_connections: self.config.max_connections,
        }
    }

    /// Close all connections in the pool.
    pub async fn close(&self) {
        let connections: Vec<_> = {
            let mut conns = self.connections.lock();
            conns.drain(..).collect()
        };

        for conn in connections {
            let _ = conn.client.close().await;
            self.total_connections.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Pool statistics.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total number of connections (in use + idle).
    pub total_connections: usize,
    /// Number of idle connections available.
    pub idle_connections: usize,
    /// Number of waiters for a connection.
    pub waiting: usize,
    /// Maximum allowed connections.
    pub max_connections: usize,
}

/// A connection borrowed from the pool.
///
/// The connection is automatically returned to the pool when dropped.
pub struct PooledClient<'a> {
    client: Option<Client>,
    pool: &'a Pool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a> PooledClient<'a> {
    /// Get a reference to the underlying client.
    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }

    /// Get a mutable reference to the underlying client.
    pub fn client_mut(&mut self) -> &mut Client {
        self.client.as_mut().unwrap()
    }
}

impl<'a> std::ops::Deref for PooledClient<'a> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}

impl<'a> std::ops::DerefMut for PooledClient<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}

impl<'a> Drop for PooledClient<'a> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.return_connection(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 10);
    }

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new(20)
            .with_min_connections(5)
            .with_acquire_timeout(Duration::from_secs(60));

        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 5);
        assert_eq!(config.acquire_timeout, Duration::from_secs(60));
    }
}
