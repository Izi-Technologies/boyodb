// Package boyodb provides connection pooling for high-performance concurrent access.
package boyodb

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// PoolConfig holds configuration for the connection pool.
type PoolConfig struct {
	// Host is the server host
	Host string
	// Port is the server port
	Port int

	// PoolSize is the number of connections in the pool
	PoolSize int
	// PoolTimeout is the timeout for acquiring a connection
	PoolTimeout time.Duration

	// TLS enables TLS encryption
	TLS bool
	// TLSConfig is the optional TLS configuration
	TLSConfig *tls.Config
	// CAFile is the path to a CA certificate file
	CAFile string
	// InsecureSkipVerify skips TLS verification (DANGEROUS)
	InsecureSkipVerify bool

	// ConnectTimeout is the timeout for establishing a connection
	ConnectTimeout time.Duration
	// ReadTimeout is the timeout for reading responses
	ReadTimeout time.Duration
	// WriteTimeout is the timeout for writing requests
	WriteTimeout time.Duration

	// Token is the authentication token
	Token string
	// MaxRetries is the number of connection retries
	MaxRetries int
	// RetryDelay is the delay between retries
	RetryDelay time.Duration

	// Database is the default database
	Database string
	// QueryTimeout is the default query timeout in milliseconds
	QueryTimeout uint32

	// --- Enhanced pooling options ---

	// HealthCheckInterval is how often to check connection health (0 = disabled)
	HealthCheckInterval time.Duration
	// MaxConnLifetime is the maximum lifetime of a connection (0 = unlimited)
	MaxConnLifetime time.Duration
	// MaxConnIdleTime is the maximum idle time before closing (0 = unlimited)
	MaxConnIdleTime time.Duration
	// MinPoolSize is the minimum connections to maintain (for warm pool)
	MinPoolSize int

	// --- Circuit breaker options ---

	// CircuitBreakerEnabled enables the circuit breaker pattern
	CircuitBreakerEnabled bool
	// CircuitBreakerThreshold is failures before opening circuit
	CircuitBreakerThreshold int
	// CircuitBreakerTimeout is how long circuit stays open
	CircuitBreakerTimeout time.Duration
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Host:                    "localhost",
		Port:                    8765,
		PoolSize:                10,
		PoolTimeout:             30 * time.Second,
		ConnectTimeout:          10 * time.Second,
		ReadTimeout:             30 * time.Second,
		WriteTimeout:            10 * time.Second,
		MaxRetries:              3,
		RetryDelay:              time.Second,
		QueryTimeout:            30000,
		HealthCheckInterval:     30 * time.Second,
		MaxConnLifetime:         30 * time.Minute,
		MaxConnIdleTime:         5 * time.Minute,
		MinPoolSize:             2,
		CircuitBreakerEnabled:   true,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
	}
}

// CircuitState represents the state of the circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota // Normal operation
	CircuitOpen                       // Failing, reject requests
	CircuitHalfOpen                   // Testing if service recovered
)

// circuitBreaker implements the circuit breaker pattern.
type circuitBreaker struct {
	mu              sync.RWMutex
	state           CircuitState
	failures        int
	threshold       int
	timeout         time.Duration
	lastFailure     time.Time
	lastStateChange time.Time
	successesNeeded int
	successes       int
}

func newCircuitBreaker(threshold int, timeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:           CircuitClosed,
		threshold:       threshold,
		timeout:         timeout,
		successesNeeded: 2,
	}
}

func (cb *circuitBreaker) canExecute() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
			cb.lastStateChange = time.Now()
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return false
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.successes++
		if cb.successes >= cb.successesNeeded {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.lastStateChange = time.Now()
		}
	} else if cb.state == CircuitClosed {
		cb.failures = 0
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailure = time.Now()
	cb.failures++

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastStateChange = time.Now()
	} else if cb.state == CircuitClosed && cb.failures >= cb.threshold {
		cb.state = CircuitOpen
		cb.lastStateChange = time.Now()
	}
}

func (cb *circuitBreaker) getState() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// pooledConn represents a connection in the pool.
type pooledConn struct {
	conn       net.Conn
	valid      bool
	createdAt  time.Time
	lastUsedAt time.Time
}

func (p *pooledConn) isValid() bool {
	return p.valid && p.conn != nil
}

func (p *pooledConn) invalidate() {
	p.valid = false
}

func (p *pooledConn) close() {
	p.valid = false
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

func (p *pooledConn) touch() {
	p.lastUsedAt = time.Now()
}

func (p *pooledConn) isExpired(maxLifetime, maxIdleTime time.Duration) bool {
	now := time.Now()
	if maxLifetime > 0 && now.Sub(p.createdAt) > maxLifetime {
		return true
	}
	if maxIdleTime > 0 && now.Sub(p.lastUsedAt) > maxIdleTime {
		return true
	}
	return false
}

// ConnectionPool manages a pool of connections to BoyoDB.
type ConnectionPool struct {
	config         *PoolConfig
	pool           chan *pooledConn
	mu             sync.Mutex
	closed         bool
	sessionID      string
	circuitBreaker *circuitBreaker
	stopHealthCh   chan struct{}

	// Statistics
	stats struct {
		sync.RWMutex
		totalConnections   int64
		activeConnections  int64
		idleConnections    int64
		waitCount          int64
		waitDuration       time.Duration
		maxIdleTimeClosed  int64
		maxLifetimeClosed  int64
		successfulRequests int64
		failedRequests     int64
		circuitBreakerTrips int64
	}
}

// NewConnectionPool creates a new connection pool.
func NewConnectionPool(config *PoolConfig) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}
	if config.PoolSize <= 0 {
		config.PoolSize = 10
	}
	if config.MinPoolSize <= 0 {
		config.MinPoolSize = 2
	}
	if config.MinPoolSize > config.PoolSize {
		config.MinPoolSize = config.PoolSize
	}

	pool := &ConnectionPool{
		config:       config,
		pool:         make(chan *pooledConn, config.PoolSize),
		stopHealthCh: make(chan struct{}),
	}

	// Initialize circuit breaker
	if config.CircuitBreakerEnabled {
		threshold := config.CircuitBreakerThreshold
		if threshold <= 0 {
			threshold = 5
		}
		timeout := config.CircuitBreakerTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		pool.circuitBreaker = newCircuitBreaker(threshold, timeout)
	}

	// Initialize pool with minimum connections
	initialSize := config.MinPoolSize
	for i := 0; i < initialSize; i++ {
		conn, err := pool.createConnectionWithRetry()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to initialize pool: %w", err)
		}
		pool.pool <- conn
		pool.stats.Lock()
		pool.stats.totalConnections++
		pool.stats.idleConnections++
		pool.stats.Unlock()
	}

	// Health check
	if err := pool.Health(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("health check failed: %w", err)
	}

	// Start background health checker
	if config.HealthCheckInterval > 0 {
		go pool.healthCheckLoop()
	}

	return pool, nil
}

// healthCheckLoop runs periodic health checks and connection maintenance.
func (p *ConnectionPool) healthCheckLoop() {
	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopHealthCh:
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck checks and maintains pool health.
func (p *ConnectionPool) performHealthCheck() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	// Check pool size and replenish if needed
	currentSize := len(p.pool)
	if currentSize < p.config.MinPoolSize {
		for i := currentSize; i < p.config.MinPoolSize; i++ {
			go func() {
				conn, err := p.createConnectionWithRetry()
				if err != nil {
					return
				}
				p.mu.Lock()
				closed := p.closed
				p.mu.Unlock()
				if closed {
					conn.close()
					return
				}
				select {
				case p.pool <- conn:
					p.stats.Lock()
					p.stats.totalConnections++
					p.stats.idleConnections++
					p.stats.Unlock()
				default:
					conn.close()
				}
			}()
		}
	}

	// Test a connection from the pool
	select {
	case conn := <-p.pool:
		if conn.isExpired(p.config.MaxConnLifetime, p.config.MaxConnIdleTime) {
			conn.close()
			p.stats.Lock()
			if p.config.MaxConnLifetime > 0 && time.Since(conn.createdAt) > p.config.MaxConnLifetime {
				p.stats.maxLifetimeClosed++
			} else {
				p.stats.maxIdleTimeClosed++
			}
			p.stats.Unlock()
			// Replace with fresh connection
			newConn, err := p.createConnectionWithRetry()
			if err == nil {
				p.pool <- newConn
			}
		} else {
			// Return healthy connection
			p.pool <- conn
		}
	default:
		// Pool is empty, nothing to check
	}
}

// createConnectionWithRetry creates a connection with exponential backoff retry.
func (p *ConnectionPool) createConnectionWithRetry() (*pooledConn, error) {
	var lastErr error
	maxRetries := p.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		conn, err := p.createConnection()
		if err == nil {
			return conn, nil
		}
		lastErr = err

		if attempt < maxRetries {
			// Exponential backoff: 1s, 2s, 4s, ...
			backoff := p.config.RetryDelay * time.Duration(1<<uint(attempt))
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// createConnection establishes a new connection.
func (p *ConnectionPool) createConnection() (*pooledConn, error) {
	address := fmt.Sprintf("%s:%d", p.config.Host, p.config.Port)

	timeout := p.config.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	var conn net.Conn
	var err error

	if p.config.TLS {
		tlsConfig := p.config.TLSConfig
		if tlsConfig == nil {
			tlsConfig, err = p.buildTLSConfig()
			if err != nil {
				return nil, err
			}
		}
		dialer := &net.Dialer{Timeout: timeout}
		conn, err = tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", address, timeout)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	now := time.Now()
	return &pooledConn{
		conn:       conn,
		valid:      true,
		createdAt:  now,
		lastUsedAt: now,
	}, nil
}

func (p *ConnectionPool) buildTLSConfig() (*tls.Config, error) {
	if p.config.InsecureSkipVerify {
		fmt.Fprintln(os.Stderr, "WARNING: TLS certificate verification is DISABLED.")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: p.config.InsecureSkipVerify,
	}

	if p.config.CAFile != "" {
		caCert, err := os.ReadFile(p.config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// borrow gets a connection from the pool.
func (p *ConnectionPool) borrow() (*pooledConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("pool is closed")
	}
	p.mu.Unlock()

	startWait := time.Now()
	timeout := p.config.PoolTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	p.stats.Lock()
	p.stats.waitCount++
	p.stats.Unlock()

	select {
	case conn := <-p.pool:
		p.stats.Lock()
		p.stats.waitDuration += time.Since(startWait)
		p.stats.idleConnections--
		p.stats.activeConnections++
		p.stats.Unlock()

		// Check if connection is expired or invalid
		if !conn.isValid() || conn.isExpired(p.config.MaxConnLifetime, p.config.MaxConnIdleTime) {
			conn.close()
			if conn.isExpired(p.config.MaxConnLifetime, p.config.MaxConnIdleTime) {
				p.stats.Lock()
				p.stats.maxLifetimeClosed++
				p.stats.Unlock()
			}
			newConn, err := p.createConnectionWithRetry()
			if err != nil {
				p.stats.Lock()
				p.stats.activeConnections--
				p.stats.Unlock()
				return nil, err
			}
			return newConn, nil
		}
		conn.touch()
		return conn, nil
	case <-time.After(timeout):
		return nil, errors.New("connection pool exhausted")
	}
}

// return puts a connection back in the pool.
func (p *ConnectionPool) returnConn(conn *pooledConn) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	p.stats.Lock()
	p.stats.activeConnections--
	p.stats.Unlock()

	if conn.isValid() && !closed {
		conn.touch()
		select {
		case p.pool <- conn:
			p.stats.Lock()
			p.stats.idleConnections++
			p.stats.Unlock()
			return
		default:
			conn.close()
		}
	} else {
		conn.close()
		if !closed {
			// Replace with new connection asynchronously
			go func() {
				newConn, err := p.createConnectionWithRetry()
				if err != nil {
					return
				}
				p.mu.Lock()
				stillOpen := !p.closed
				p.mu.Unlock()
				if !stillOpen {
					newConn.close()
					return
				}
				select {
				case p.pool <- newConn:
					p.stats.Lock()
					p.stats.totalConnections++
					p.stats.idleConnections++
					p.stats.Unlock()
				default:
					newConn.close()
				}
			}()
		}
	}
}

// sendRequest sends a request using a pooled connection.
func (p *ConnectionPool) sendRequest(req map[string]interface{}) (*response, error) {
	// Check circuit breaker
	if p.circuitBreaker != nil && !p.circuitBreaker.canExecute() {
		p.stats.Lock()
		p.stats.circuitBreakerTrips++
		p.stats.Unlock()
		return nil, errors.New("circuit breaker is open - server appears unavailable")
	}

	// Add auth
	p.mu.Lock()
	if p.sessionID != "" {
		req["auth"] = p.sessionID
	} else if p.config.Token != "" {
		req["auth"] = p.config.Token
	}
	p.mu.Unlock()

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	conn, err := p.borrow()
	if err != nil {
		if p.circuitBreaker != nil {
			p.circuitBreaker.recordFailure()
		}
		p.stats.Lock()
		p.stats.failedRequests++
		p.stats.Unlock()
		return nil, err
	}

	resp, err := p.sendOnConnection(conn, data)
	if err != nil {
		conn.invalidate()
		p.returnConn(conn)
		if p.circuitBreaker != nil {
			p.circuitBreaker.recordFailure()
		}
		p.stats.Lock()
		p.stats.failedRequests++
		p.stats.Unlock()
		return nil, err
	}

	// Success
	if p.circuitBreaker != nil {
		p.circuitBreaker.recordSuccess()
	}
	p.stats.Lock()
	p.stats.successfulRequests++
	p.stats.Unlock()

	p.returnConn(conn)
	return resp, nil
}

func (p *ConnectionPool) sendOnConnection(pc *pooledConn, data []byte) (*response, error) {
	conn := pc.conn

	// Set write timeout
	if p.config.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
	}

	// Write length prefix
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("failed to write length: %w", err)
	}

	// Write payload
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write payload: %w", err)
	}

	// Set read timeout
	if p.config.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
	}

	// Read response length
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	if respLen > 100*1024*1024 {
		return nil, fmt.Errorf("response too large: %d bytes", respLen)
	}

	// Read response body
	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var resp response
	if err := json.Unmarshal(respBuf, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Handle IPC streaming
	if resp.IPCStreaming {
		ipcBuf, err := readStreamFrames(conn)
		if err != nil {
			return nil, err
		}
		resp.IPCBytes = ipcBuf
	} else if resp.IPCLen > 0 {
		if resp.IPCLen > 100*1024*1024 {
			return nil, fmt.Errorf("response too large: %d bytes", resp.IPCLen)
		}
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return nil, fmt.Errorf("failed to read IPC length: %w", err)
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf)
		if uint64(payloadLen) != resp.IPCLen {
			return nil, fmt.Errorf("IPC length mismatch")
		}
		ipcBuf := make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, ipcBuf); err != nil {
			return nil, fmt.Errorf("failed to read IPC payload: %w", err)
		}
		resp.IPCBytes = ipcBuf
	}

	return &resp, nil
}

// Health checks server health.
func (p *ConnectionPool) Health() error {
	resp, err := p.sendRequest(map[string]interface{}{"op": "health"})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("health check failed: %s", resp.Message)
	}
	return nil
}

// Login authenticates with username and password.
func (p *ConnectionPool) Login(username, password string) error {
	resp, err := p.sendRequest(map[string]interface{}{
		"op":       "login",
		"username": username,
		"password": password,
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("login failed: %s", resp.Message)
	}
	p.mu.Lock()
	p.sessionID = resp.SessionID
	p.mu.Unlock()
	return nil
}

// Logout logs out from the server.
func (p *ConnectionPool) Logout() error {
	resp, err := p.sendRequest(map[string]interface{}{"op": "logout"})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("logout failed: %s", resp.Message)
	}
	p.mu.Lock()
	p.sessionID = ""
	p.mu.Unlock()
	return nil
}

// Close closes all connections in the pool.
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	// Stop health check goroutine
	if p.stopHealthCh != nil {
		close(p.stopHealthCh)
	}

	close(p.pool)
	for conn := range p.pool {
		conn.close()
	}
}

// PooledClient is a high-performance pooled BoyoDB client.
type PooledClient struct {
	config *PoolConfig
	pool   *ConnectionPool
}

// NewPooledClient creates a new pooled client.
func NewPooledClient(config *PoolConfig) (*PooledClient, error) {
	pool, err := NewConnectionPool(config)
	if err != nil {
		return nil, err
	}
	return &PooledClient{
		config: config,
		pool:   pool,
	}, nil
}

// Query executes a SQL query and returns results.
func (c *PooledClient) Query(sql string) (*Result, error) {
	return c.QueryContext(sql, c.config.Database, c.config.QueryTimeout)
}

// QueryContext executes a SQL query with specific database and timeout.
func (c *PooledClient) QueryContext(sql, database string, timeoutMillis uint32) (*Result, error) {
	op := "query"
	if isSelectLike(sql) {
		op = "query_binary"
	}
	req := map[string]interface{}{
		"op":             op,
		"sql":            sql,
		"timeout_millis": timeoutMillis,
	}
	if op == "query_binary" {
		req["stream"] = true
	}
	if database != "" {
		req["database"] = database
	}

	resp, err := c.pool.sendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("query failed: %s", resp.Message)
	}

	result := &Result{
		segmentsScanned:  resp.SegmentsScanned,
		dataSkippedBytes: resp.DataSkippedBytes,
	}

	if len(resp.IPCBytes) > 0 {
		result.ipcData = resp.IPCBytes
		if err := result.parseIPC(); err != nil {
			return nil, fmt.Errorf("failed to parse IPC data: %w", err)
		}
	} else if resp.IPCBase64 != "" {
		ipcData, err := decodeBase64(resp.IPCBase64)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IPC data: %w", err)
		}
		result.ipcData = ipcData
		if err := result.parseIPC(); err != nil {
			return nil, fmt.Errorf("failed to parse IPC data: %w", err)
		}
	}

	return result, nil
}

// Exec executes a SQL statement that doesn't return rows.
func (c *PooledClient) Exec(sql string) error {
	return c.ExecContext(sql, c.config.Database, c.config.QueryTimeout)
}

// ExecContext executes a SQL statement with specific database and timeout.
func (c *PooledClient) ExecContext(sql, database string, timeoutMillis uint32) error {
	req := map[string]interface{}{
		"op":             "query",
		"sql":            sql,
		"timeout_millis": timeoutMillis,
	}
	if database != "" {
		req["database"] = database
	}

	resp, err := c.pool.sendRequest(req)
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("exec failed: %s", resp.Message)
	}
	return nil
}

// Login authenticates with username and password.
func (c *PooledClient) Login(username, password string) error {
	return c.pool.Login(username, password)
}

// Logout logs out from the server.
func (c *PooledClient) Logout() error {
	return c.pool.Logout()
}

// Close closes the client and all connections.
func (c *PooledClient) Close() error {
	c.pool.Close()
	return nil
}

// SetDatabase sets the default database.
func (c *PooledClient) SetDatabase(database string) {
	c.config.Database = database
}

// Health checks if the server is healthy.
func (c *PooledClient) Health() error {
	return c.pool.Health()
}

// CreateDatabase creates a new database.
func (c *PooledClient) CreateDatabase(name string) error {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op":   "createdatabase",
		"name": name,
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("create database failed: %s", resp.Message)
	}
	return nil
}

// CreateTable creates a new table in the specified database.
func (c *PooledClient) CreateTable(database, table string) error {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op":       "createtable",
		"database": database,
		"table":    table,
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("create table failed: %s", resp.Message)
	}
	return nil
}

// ListDatabases returns a list of all databases.
func (c *PooledClient) ListDatabases() ([]string, error) {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op": "listdatabases",
	})
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("list databases failed: %s", resp.Message)
	}
	return resp.Databases, nil
}

// ListTables returns a list of all tables, optionally filtered by database.
func (c *PooledClient) ListTables(database string) ([]TableInfo, error) {
	req := map[string]interface{}{
		"op": "listtables",
	}
	if database != "" {
		req["database"] = database
	}

	resp, err := c.pool.sendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("list tables failed: %s", resp.Message)
	}
	return resp.Tables, nil
}

// IngestCSV ingests CSV data into a table.
func (c *PooledClient) IngestCSV(database, table string, csvData []byte, hasHeader bool, delimiter string) error {
	req := map[string]interface{}{
		"op":             "ingestcsv",
		"database":       database,
		"table":          table,
		"payload_base64": base64.StdEncoding.EncodeToString(csvData),
		"has_header":     hasHeader,
	}
	if delimiter != "" {
		req["delimiter"] = delimiter
	}

	resp, err := c.pool.sendRequest(req)
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("ingest CSV failed: %s", resp.Message)
	}
	return nil
}

// IngestIPC ingests Arrow IPC data into a table.
func (c *PooledClient) IngestIPC(database, table string, ipcData []byte) error {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op":             "ingestipc",
		"database":       database,
		"table":          table,
		"payload_base64": base64.StdEncoding.EncodeToString(ipcData),
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("ingest IPC failed: %s", resp.Message)
	}
	return nil
}

// Explain returns the query execution plan.
func (c *PooledClient) Explain(sql string) (string, error) {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op":  "explain",
		"sql": sql,
	})
	if err != nil {
		return "", err
	}
	if resp.Status != "ok" {
		return "", fmt.Errorf("explain failed: %s", resp.Message)
	}
	return string(resp.ExplainPlan), nil
}

// Metrics returns server metrics as JSON.
func (c *PooledClient) Metrics() (string, error) {
	resp, err := c.pool.sendRequest(map[string]interface{}{
		"op": "metrics",
	})
	if err != nil {
		return "", err
	}
	if resp.Status != "ok" {
		return "", fmt.Errorf("metrics failed: %s", resp.Message)
	}
	return string(resp.Metrics), nil
}

// Transaction Support

// Begin starts a new transaction.
func (c *PooledClient) Begin() error {
	return c.Exec("BEGIN")
}

// Commit commits the current transaction.
func (c *PooledClient) Commit() error {
	return c.Exec("COMMIT")
}

// Rollback aborts the current transaction.
func (c *PooledClient) Rollback() error {
	return c.Exec("ROLLBACK")
}

// InTransaction executes a function within a transaction.
func (c *PooledClient) InTransaction(fn func() error) error {
	if err := c.Begin(); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := fn(); err != nil {
		if rbErr := c.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed after error (%v): %w", err, rbErr)
		}
		return err
	}

	if err := c.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// PoolStats returns statistics about the connection pool.
func (c *PooledClient) PoolStats() PoolStats {
	c.pool.mu.Lock()
	closed := c.pool.closed
	c.pool.mu.Unlock()

	c.pool.stats.RLock()
	defer c.pool.stats.RUnlock()

	var circuitState string
	if c.pool.circuitBreaker != nil {
		switch c.pool.circuitBreaker.getState() {
		case CircuitClosed:
			circuitState = "closed"
		case CircuitOpen:
			circuitState = "open"
		case CircuitHalfOpen:
			circuitState = "half-open"
		}
	} else {
		circuitState = "disabled"
	}

	return PoolStats{
		PoolSize:            c.config.PoolSize,
		MinPoolSize:         c.config.MinPoolSize,
		Available:           len(c.pool.pool),
		InUse:               int(c.pool.stats.activeConnections),
		IsClosed:            closed,
		TotalConnections:    c.pool.stats.totalConnections,
		WaitCount:           c.pool.stats.waitCount,
		WaitDuration:        c.pool.stats.waitDuration,
		MaxIdleTimeClosed:   c.pool.stats.maxIdleTimeClosed,
		MaxLifetimeClosed:   c.pool.stats.maxLifetimeClosed,
		SuccessfulRequests:  c.pool.stats.successfulRequests,
		FailedRequests:      c.pool.stats.failedRequests,
		CircuitBreakerTrips: c.pool.stats.circuitBreakerTrips,
		CircuitBreakerState: circuitState,
	}
}

// PoolStats contains connection pool statistics.
type PoolStats struct {
	// PoolSize is the maximum number of connections
	PoolSize int
	// MinPoolSize is the minimum connections to maintain
	MinPoolSize int
	// Available is the number of idle connections
	Available int
	// InUse is the number of connections currently in use
	InUse int
	// IsClosed indicates if the pool is closed
	IsClosed bool
	// TotalConnections is the total connections created over lifetime
	TotalConnections int64
	// WaitCount is total times a goroutine waited for a connection
	WaitCount int64
	// WaitDuration is total time spent waiting for connections
	WaitDuration time.Duration
	// MaxIdleTimeClosed is connections closed due to idle timeout
	MaxIdleTimeClosed int64
	// MaxLifetimeClosed is connections closed due to max lifetime
	MaxLifetimeClosed int64
	// SuccessfulRequests is total successful requests
	SuccessfulRequests int64
	// FailedRequests is total failed requests
	FailedRequests int64
	// CircuitBreakerTrips is times circuit breaker blocked requests
	CircuitBreakerTrips int64
	// CircuitBreakerState is current circuit breaker state
	CircuitBreakerState string
}

// helper function for base64 decoding
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
