// Package boyodb provides a Go driver for connecting to boyodb-server.
//
// Basic usage:
//
//	client, err := boyodb.NewClient("localhost:8765", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	result, err := client.Query("SELECT * FROM mydb.users LIMIT 10")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer result.Close()
//
//	for result.Next() {
//	    var id int64
//	    var name string
//	    if err := result.Scan(&id, &name); err != nil {
//	        log.Fatal(err)
//	    }
//	    fmt.Printf("id=%d, name=%s\n", id, name)
//	}
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
	"strings"
)

// Security note: This driver includes an InsecureSkipVerify option for TLS.
// This option should NEVER be used in production as it disables certificate
// validation, making connections vulnerable to man-in-the-middle attacks.

// Client represents a connection to a boyodb server.
type Client struct {
	host    string
	config  *Config
	token   string
	session string

	mu   sync.Mutex
	conn net.Conn
}

// Config holds client configuration options.
type Config struct {
	// TLS enables TLS encryption
	TLS bool

	// TLSConfig is the optional TLS configuration. If nil and TLS is true,
	// a default config is used with system root CAs.
	TLSConfig *tls.Config

	// CAFile is the path to a CA certificate file for TLS verification.
	// Only used if TLSConfig is nil and TLS is true.
	CAFile string

	// InsecureSkipVerify skips TLS certificate verification.
	// WARNING: SECURITY RISK - This option disables certificate validation and makes
	// connections vulnerable to man-in-the-middle (MITM) attacks. An attacker can
	// intercept and modify all traffic between client and server.
	// NEVER use this option in production environments.
	// Only use for local development or testing with self-signed certificates.
	InsecureSkipVerify bool

	// ConnectTimeout is the timeout for establishing a connection.
	// Default: 10 seconds
	ConnectTimeout time.Duration

	// ReadTimeout is the timeout for reading responses.
	// Default: 30 seconds
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for writing requests.
	// Default: 10 seconds
	WriteTimeout time.Duration

	// Token is an optional authentication token.
	Token string

	// MaxRetries is the number of times to retry failed connections.
	// Default: 3
	MaxRetries int

	// RetryDelay is the delay between retries.
	// Default: 1 second
	RetryDelay time.Duration

	// Database is the default database to use for queries.
	Database string

	// QueryTimeout is the default query timeout in milliseconds.
	// Default: 30000 (30 seconds)
	QueryTimeout uint32
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxRetries:     3,
		RetryDelay:     1 * time.Second,
		QueryTimeout:   30000,
	}
}

// NewClient creates a new client connection to the boyodb server.
// If config is nil, DefaultConfig() is used.
func NewClient(host string, config *Config) (*Client, error) {
	if config == nil {
		config = DefaultConfig()
	}

	client := &Client{
		host:   host,
		config: config,
		token:  config.Token,
	}

	// Test connection with health check
	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Verify connection with health check
	if err := client.Health(); err != nil {
		client.Close()
		return nil, fmt.Errorf("health check failed: %w", err)
	}

	return client, nil
}

// connect establishes the underlying network connection.
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	var conn net.Conn
	var err error

	timeout := c.config.ConnectTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	if c.config.TLS {
		tlsConfig := c.config.TLSConfig
		if tlsConfig == nil {
			tlsConfig, err = c.buildTLSConfig()
			if err != nil {
				return fmt.Errorf("failed to build TLS config: %w", err)
			}
		}

		dialer := &net.Dialer{Timeout: timeout}
		conn, err = tls.DialWithDialer(dialer, "tcp", c.host, tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.host, timeout)
	}

	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

// buildTLSConfig creates a TLS configuration from Config options.
func (c *Client) buildTLSConfig() (*tls.Config, error) {
	if c.config.InsecureSkipVerify {
		// Log a warning when insecure mode is enabled
		fmt.Fprintln(os.Stderr, "WARNING: TLS certificate verification is DISABLED. "+
			"This is insecure and vulnerable to MITM attacks. "+
			"Only use for testing with self-signed certificates.")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.config.InsecureSkipVerify,
	}

	if c.config.CAFile != "" {
		caCert, err := os.ReadFile(c.config.CAFile)
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

// Close closes the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// request is the internal request envelope.
type request struct {
	Auth     string          `json:"auth,omitempty"`
	Op       string          `json:"op"`
	SQL      string          `json:"sql,omitempty"`
	Timeout  uint32          `json:"timeout_millis,omitempty"`
	Database string          `json:"database,omitempty"`
	Name     string          `json:"name,omitempty"`
	Table    string          `json:"table,omitempty"`
	Username string          `json:"username,omitempty"`
	Password string          `json:"password,omitempty"`
	Data     json.RawMessage `json:"-"` // For raw request building
}

// response is the internal response envelope.
type response struct {
	Status          string          `json:"status"`
	Message         string          `json:"message,omitempty"`
	IPCBase64       string          `json:"ipc_base64,omitempty"`
	IPCLen          uint64          `json:"ipc_len,omitempty"`
	IPCStreaming    bool            `json:"ipc_streaming,omitempty"`
	IPCBytes        []byte          `json:"-"`
	SegmentsScanned int             `json:"segments_scanned,omitempty"`
	DataSkippedBytes uint64         `json:"data_skipped_bytes,omitempty"`
	Databases       []string        `json:"databases,omitempty"`
	Tables          []TableInfo     `json:"tables,omitempty"`
	SessionID       string          `json:"session_id,omitempty"`
	ExplainPlan     json.RawMessage `json:"explain_plan,omitempty"`
	Metrics         json.RawMessage `json:"metrics,omitempty"`
	PreparedID     string          `json:"prepared_id,omitempty"`
}

// TableInfo represents table metadata.
type TableInfo struct {
	Database   string `json:"database"`
	Name       string `json:"name"`
	SchemaJSON string `json:"schema_json,omitempty"`
}

// sendRequest sends a request to the server and returns the response.
func (c *Client) sendRequest(req map[string]interface{}) (*response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add auth token if available
	if c.session != "" {
		req["auth"] = c.session
	} else if c.token != "" {
		req["auth"] = c.token
	}

	// Serialize request
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Retry logic
	maxRetries := c.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Reconnect on retry
			c.mu.Unlock()
			if err := c.connect(); err != nil {
				c.mu.Lock()
				lastErr = err
				time.Sleep(c.config.RetryDelay)
				continue
			}
			c.mu.Lock()
		}

		resp, err := c.sendRequestOnce(data)
		if err != nil {
			lastErr = err
			// Check if it's a connection error worth retrying
			if isConnectionError(err) && attempt < maxRetries-1 {
				time.Sleep(c.config.RetryDelay)
				continue
			}
			return nil, err
		}
		return resp, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// sendRequestOnce sends a single request without retry logic.
func (c *Client) sendRequestOnce(data []byte) (*response, error) {
	if c.conn == nil {
		return nil, errors.New("not connected")
	}

	// Set write timeout
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	// Write length prefix (4 bytes, big-endian)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return nil, fmt.Errorf("failed to write length: %w", err)
	}

	// Write payload
	if _, err := c.conn.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write payload: %w", err)
	}

	// Set read timeout
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}

	// Read response length
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)

	// Sanity check response size
	if respLen > 100*1024*1024 { // 100MB max
		return nil, fmt.Errorf("response too large: %d bytes", respLen)
	}

	// Read response body
	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response
	var resp response
	if err := json.Unmarshal(respBuf, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.IPCStreaming {
		ipcBuf, err := readStreamFrames(c.conn)
		if err != nil {
			return nil, err
		}
		resp.IPCBytes = ipcBuf
	} else if resp.IPCLen > 0 {
		if resp.IPCLen > 100*1024*1024 {
			return nil, fmt.Errorf("response too large: %d bytes", resp.IPCLen)
		}
		if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
			return nil, fmt.Errorf("failed to read IPC length: %w", err)
		}
		payloadLen := binary.BigEndian.Uint32(lenBuf)
		if uint64(payloadLen) != resp.IPCLen {
			return nil, fmt.Errorf("IPC length mismatch: expected %d got %d", resp.IPCLen, payloadLen)
		}
		ipcBuf := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.conn, ipcBuf); err != nil {
			return nil, fmt.Errorf("failed to read IPC payload: %w", err)
		}
		resp.IPCBytes = ipcBuf
	}

	return &resp, nil
}

func isSelectLike(sql string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(sql))
	return strings.HasPrefix(trimmed, "select ") || strings.HasPrefix(trimmed, "with ")
}

// isConnectionError checks if an error is a connection-related error.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "connection refused") ||
		contains(errStr, "connection reset") ||
		contains(errStr, "broken pipe") ||
		contains(errStr, "timeout") ||
		contains(errStr, "EOF")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func readStreamFrames(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 0, 1024)
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return nil, fmt.Errorf("failed to read frame length: %w", err)
		}
		chunkLen := binary.BigEndian.Uint32(lenBuf)
		if chunkLen == 0 {
			break
		}
		if chunkLen > 100*1024*1024 {
			return nil, fmt.Errorf("response too large: %d bytes", chunkLen)
		}
		chunk := make([]byte, chunkLen)
		if _, err := io.ReadFull(conn, chunk); err != nil {
			return nil, fmt.Errorf("failed to read frame payload: %w", err)
		}
		buf = append(buf, chunk...)
	}
	return buf, nil
}

// Health checks if the server is healthy.
func (c *Client) Health() error {
	resp, err := c.sendRequest(map[string]interface{}{
		"op": "health",
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("health check failed: %s", resp.Message)
	}
	return nil
}

// Login authenticates with the server using username and password.
func (c *Client) Login(username, password string) error {
	resp, err := c.sendRequest(map[string]interface{}{
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
	c.session = resp.SessionID
	return nil
}

// Logout logs out from the server.
func (c *Client) Logout() error {
	resp, err := c.sendRequest(map[string]interface{}{
		"op": "logout",
	})
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("logout failed: %s", resp.Message)
	}
	c.session = ""
	return nil
}

// Query executes a SQL query and returns the results.
func (c *Client) Query(sql string) (*Result, error) {
	return c.QueryContext(sql, c.config.Database, c.config.QueryTimeout)
}

// QueryContext executes a SQL query with specific database and timeout.
func (c *Client) QueryContext(sql, database string, timeoutMillis uint32) (*Result, error) {
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

	resp, err := c.sendRequest(req)
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

	// Decode Arrow IPC data if present
	if len(resp.IPCBytes) > 0 {
		result.ipcData = resp.IPCBytes
		if err := result.parseIPC(); err != nil {
			return nil, fmt.Errorf("failed to parse IPC data: %w", err)
		}
	} else if resp.IPCBase64 != "" {
		ipcData, err := base64.StdEncoding.DecodeString(resp.IPCBase64)
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

// Exec executes a SQL statement that doesn't return rows (CREATE, DROP, etc.).
func (c *Client) Exec(sql string) error {
	return c.ExecContext(sql, c.config.Database, c.config.QueryTimeout)
}

// ExecContext executes a SQL statement with specific database and timeout.
func (c *Client) ExecContext(sql, database string, timeoutMillis uint32) error {
	req := map[string]interface{}{
		"op":             "query",
		"sql":            sql,
		"timeout_millis": timeoutMillis,
	}
	if database != "" {
		req["database"] = database
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("exec failed: %s", resp.Message)
	}
	return nil
}

// Prepare registers a server-side prepared statement and returns its id.
func (c *Client) Prepare(sql, database string) (string, error) {
	req := map[string]interface{}{
		"op":  "prepare",
		"sql": sql,
	}
	if database != "" {
		req["database"] = database
	}
	resp, err := c.sendRequest(req)
	if err != nil {
		return "", err
	}
	if resp.Status != "ok" {
		return "", fmt.Errorf("prepare failed: %s", resp.Message)
	}
	if resp.PreparedID == "" {
		return "", errors.New("missing prepared_id in response")
	}
	return resp.PreparedID, nil
}

// ExecutePreparedBinary executes a prepared statement and returns results using binary IPC.
func (c *Client) ExecutePreparedBinary(preparedID string, timeoutMillis uint32) (*Result, error) {
	req := map[string]interface{}{
		"op":             "execute_prepared_binary",
		"id":             preparedID,
		"timeout_millis": timeoutMillis,
	}
	req["stream"] = true
	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("execute prepared failed: %s", resp.Message)
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
		ipcData, err := base64.StdEncoding.DecodeString(resp.IPCBase64)
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

// CreateDatabase creates a new database.
func (c *Client) CreateDatabase(name string) error {
	resp, err := c.sendRequest(map[string]interface{}{
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
func (c *Client) CreateTable(database, table string) error {
	resp, err := c.sendRequest(map[string]interface{}{
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

// CreateTableWithSchema creates a new table with a schema definition.
func (c *Client) CreateTableWithSchema(database, table string, schema []map[string]interface{}) error {
	resp, err := c.sendRequest(map[string]interface{}{
		"op":       "createtable",
		"database": database,
		"table":    table,
		"schema":   schema,
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
func (c *Client) ListDatabases() ([]string, error) {
	resp, err := c.sendRequest(map[string]interface{}{
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
func (c *Client) ListTables(database string) ([]TableInfo, error) {
	req := map[string]interface{}{
		"op": "listtables",
	}
	if database != "" {
		req["database"] = database
	}

	resp, err := c.sendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("list tables failed: %s", resp.Message)
	}
	return resp.Tables, nil
}

// Explain returns the query execution plan.
func (c *Client) Explain(sql string) (string, error) {
	resp, err := c.sendRequest(map[string]interface{}{
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
func (c *Client) Metrics() (string, error) {
	resp, err := c.sendRequest(map[string]interface{}{
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

// IngestCSV ingests CSV data into a table.
func (c *Client) IngestCSV(database, table string, csvData []byte, hasHeader bool, delimiter string) error {
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

	resp, err := c.sendRequest(req)
	if err != nil {
		return err
	}
	if resp.Status != "ok" {
		return fmt.Errorf("ingest CSV failed: %s", resp.Message)
	}
	return nil
}

// IngestIPC ingests Arrow IPC data into a table.
func (c *Client) IngestIPC(database, table string, ipcData []byte) error {
	if err := c.sendRequestBinary(map[string]interface{}{
		"op":       "ingest_ipc_binary",
		"database": database,
		"table":    table,
	}, ipcData); err == nil {
		return nil
	}
	resp, err := c.sendRequest(map[string]interface{}{
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

func (c *Client) sendRequestBinary(req map[string]interface{}, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return errors.New("not connected")
	}

	if c.session != "" {
		req["auth"] = c.session
	} else if c.token != "" {
		req["auth"] = c.token
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}
	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := c.conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write IPC length: %w", err)
	}
	if _, err := c.conn.Write(payload); err != nil {
		return fmt.Errorf("failed to write IPC payload: %w", err)
	}

	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}
	if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}
	respLen := binary.BigEndian.Uint32(lenBuf)
	if respLen > 100*1024*1024 {
		return fmt.Errorf("response too large: %d bytes", respLen)
	}
	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(c.conn, respBuf); err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	var resp response
	if err := json.Unmarshal(respBuf, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("ingest IPC failed: %s", resp.Message)
	}
	return nil
}

// SetDatabase sets the default database for queries.
func (c *Client) SetDatabase(database string) {
	c.config.Database = database
}

// SetToken sets the authentication token.
func (c *Client) SetToken(token string) {
	c.token = token
}
