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
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		Host:           "localhost",
		Port:           8765,
		PoolSize:       10,
		PoolTimeout:    30 * time.Second,
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxRetries:     3,
		RetryDelay:     time.Second,
		QueryTimeout:   30000,
	}
}

// pooledConn represents a connection in the pool.
type pooledConn struct {
	conn      net.Conn
	valid     bool
	createdAt time.Time
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

// ConnectionPool manages a pool of connections to BoyoDB.
type ConnectionPool struct {
	config    *PoolConfig
	pool      chan *pooledConn
	mu        sync.Mutex
	closed    bool
	sessionID string
}

// NewConnectionPool creates a new connection pool.
func NewConnectionPool(config *PoolConfig) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}
	if config.PoolSize <= 0 {
		config.PoolSize = 10
	}

	pool := &ConnectionPool{
		config: config,
		pool:   make(chan *pooledConn, config.PoolSize),
	}

	// Initialize pool with connections
	for i := 0; i < config.PoolSize; i++ {
		conn, err := pool.createConnection()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to initialize pool: %w", err)
		}
		pool.pool <- conn
	}

	// Health check
	if err := pool.Health(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("health check failed: %w", err)
	}

	return pool, nil
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

	return &pooledConn{
		conn:      conn,
		valid:     true,
		createdAt: time.Now(),
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

	timeout := p.config.PoolTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	select {
	case conn := <-p.pool:
		if !conn.isValid() {
			conn.close()
			newConn, err := p.createConnection()
			if err != nil {
				return nil, err
			}
			return newConn, nil
		}
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

	if conn.isValid() && !closed {
		select {
		case p.pool <- conn:
			return
		default:
			conn.close()
		}
	} else {
		conn.close()
		if !closed {
			// Replace with new connection
			go func() {
				newConn, err := p.createConnection()
				if err != nil {
					return
				}
				select {
				case p.pool <- newConn:
				default:
					newConn.close()
				}
			}()
		}
	}
}

// sendRequest sends a request using a pooled connection.
func (p *ConnectionPool) sendRequest(req map[string]interface{}) (*response, error) {
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
		return nil, err
	}

	resp, err := p.sendOnConnection(conn, data)
	if err != nil {
		conn.invalidate()
		p.returnConn(conn)
		return nil, err
	}

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
	p.closed = true
	p.mu.Unlock()

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

// helper function for base64 decoding
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
