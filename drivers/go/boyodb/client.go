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
	"strings"
	"sync"
	"time"
)

// Version is the current version of the boyodb Go driver.
const Version = "0.9.6"

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

// Transaction Support

// Begin starts a new transaction.
func (c *Client) Begin() error {
	return c.BeginWithOptions("", false)
}

// BeginWithOptions starts a new transaction with the specified isolation level.
// isolationLevel can be: "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"
// If empty, the default isolation level is used.
func (c *Client) BeginWithOptions(isolationLevel string, readOnly bool) error {
	sql := "BEGIN"
	if isolationLevel != "" {
		sql = "START TRANSACTION ISOLATION LEVEL " + isolationLevel
	}
	if readOnly {
		if isolationLevel != "" {
			sql += " READ ONLY"
		} else {
			sql = "START TRANSACTION READ ONLY"
		}
	}
	return c.Exec(sql)
}

// Commit commits the current transaction.
func (c *Client) Commit() error {
	return c.Exec("COMMIT")
}

// Rollback aborts the current transaction.
func (c *Client) Rollback() error {
	return c.Exec("ROLLBACK")
}

// Savepoint creates a savepoint with the given name.
func (c *Client) Savepoint(name string) error {
	return c.Exec("SAVEPOINT " + name)
}

// RollbackToSavepoint rolls back to the specified savepoint.
func (c *Client) RollbackToSavepoint(name string) error {
	return c.Exec("ROLLBACK TO SAVEPOINT " + name)
}

// ReleaseSavepoint releases the specified savepoint.
func (c *Client) ReleaseSavepoint(name string) error {
	return c.Exec("RELEASE SAVEPOINT " + name)
}

// ============================================================================
// Pub/Sub Support
// ============================================================================

// Listen starts listening on a notification channel.
func (c *Client) Listen(channel string) error {
	return c.Exec(fmt.Sprintf("LISTEN %s", escapeIdentifier(channel)))
}

// Unlisten stops listening on a notification channel.
// Use "*" to stop listening on all channels.
func (c *Client) Unlisten(channel string) error {
	if channel == "*" {
		return c.Exec("UNLISTEN *")
	}
	return c.Exec(fmt.Sprintf("UNLISTEN %s", escapeIdentifier(channel)))
}

// Notify sends a notification on a channel.
func (c *Client) Notify(channel string, payload string) error {
	if payload != "" {
		return c.Exec(fmt.Sprintf("NOTIFY %s, '%s'", escapeIdentifier(channel), escapeString(payload)))
	}
	return c.Exec(fmt.Sprintf("NOTIFY %s", escapeIdentifier(channel)))
}

// ============================================================================
// Trigger Management
// ============================================================================

// TriggerOptions configures trigger creation.
type TriggerOptions struct {
	// Timing is when the trigger fires: "BEFORE", "AFTER", or "INSTEAD OF"
	Timing string
	// Events are the triggering events: "INSERT", "UPDATE", "DELETE", or combinations
	Events []string
	// Function is the function name to execute
	Function string
	// Arguments are optional arguments to pass to the function
	Arguments []interface{}
	// ForEachRow indicates row-level trigger (vs statement-level)
	ForEachRow bool
	// When is an optional WHEN clause condition
	When string
	// OrReplace indicates whether to replace an existing trigger
	OrReplace bool
}

// CreateTrigger creates a database trigger.
func (c *Client) CreateTrigger(name, table string, opts *TriggerOptions) error {
	if opts == nil {
		return errors.New("TriggerOptions is required")
	}

	timing := opts.Timing
	if timing == "" {
		timing = "AFTER"
	}

	events := strings.Join(opts.Events, " OR ")

	var sql strings.Builder
	if opts.OrReplace {
		sql.WriteString("CREATE OR REPLACE TRIGGER ")
	} else {
		sql.WriteString("CREATE TRIGGER ")
	}
	sql.WriteString(escapeIdentifier(name))
	sql.WriteString(" ")
	sql.WriteString(strings.ToUpper(timing))
	sql.WriteString(" ")
	sql.WriteString(strings.ToUpper(events))
	sql.WriteString(" ON ")
	sql.WriteString(escapeIdentifier(table))
	sql.WriteString(" ")

	if opts.ForEachRow {
		sql.WriteString("FOR EACH ROW ")
	} else {
		sql.WriteString("FOR EACH STATEMENT ")
	}

	if opts.When != "" {
		sql.WriteString("WHEN (")
		sql.WriteString(opts.When)
		sql.WriteString(") ")
	}

	sql.WriteString("EXECUTE FUNCTION ")
	sql.WriteString(escapeIdentifier(opts.Function))
	sql.WriteString("(")
	for i, arg := range opts.Arguments {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(formatValue(arg))
	}
	sql.WriteString(")")

	return c.Exec(sql.String())
}

// DropTrigger drops a trigger.
func (c *Client) DropTrigger(name, table string, ifExists bool) error {
	var sql strings.Builder
	sql.WriteString("DROP TRIGGER ")
	if ifExists {
		sql.WriteString("IF EXISTS ")
	}
	sql.WriteString(escapeIdentifier(name))
	sql.WriteString(" ON ")
	sql.WriteString(escapeIdentifier(table))
	return c.Exec(sql.String())
}

// AlterTrigger enables or disables a trigger.
func (c *Client) AlterTrigger(name, table string, enable bool) error {
	action := "ENABLE"
	if !enable {
		action = "DISABLE"
	}
	sql := fmt.Sprintf("ALTER TRIGGER %s ON %s %s",
		escapeIdentifier(name), escapeIdentifier(table), action)
	return c.Exec(sql)
}

// ListTriggers lists triggers, optionally filtered by table or database.
func (c *Client) ListTriggers(table, database string) (*Result, error) {
	sql := "SHOW TRIGGERS"
	if table != "" {
		sql += " ON " + escapeIdentifier(table)
	} else if database != "" {
		sql += " IN " + escapeIdentifier(database)
	}
	return c.Query(sql)
}

// ============================================================================
// Stored Procedures and Functions
// ============================================================================

// Call executes a stored procedure.
func (c *Client) Call(procedure string, args ...interface{}) (*Result, error) {
	var sql strings.Builder
	sql.WriteString("CALL ")
	sql.WriteString(escapeIdentifier(procedure))
	sql.WriteString("(")
	for i, arg := range args {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(formatValue(arg))
	}
	sql.WriteString(")")
	return c.Query(sql.String())
}

// FunctionParameter represents a function/procedure parameter.
type FunctionParameter struct {
	Name string
	Type string
	Mode string // IN, OUT, INOUT (for procedures)
}

// CreateFunctionOptions configures function creation.
type CreateFunctionOptions struct {
	Language   string // Default: "plpgsql"
	OrReplace  bool
	Volatility string // IMMUTABLE, STABLE, VOLATILE
}

// CreateFunction creates a stored function.
func (c *Client) CreateFunction(name string, params []FunctionParameter, returnType, body string, opts *CreateFunctionOptions) error {
	if opts == nil {
		opts = &CreateFunctionOptions{}
	}
	language := opts.Language
	if language == "" {
		language = "plpgsql"
	}

	var sql strings.Builder
	if opts.OrReplace {
		sql.WriteString("CREATE OR REPLACE FUNCTION ")
	} else {
		sql.WriteString("CREATE FUNCTION ")
	}
	sql.WriteString(escapeIdentifier(name))
	sql.WriteString("(")
	for i, p := range params {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(escapeIdentifier(p.Name))
		sql.WriteString(" ")
		sql.WriteString(p.Type)
	}
	sql.WriteString(") RETURNS ")
	sql.WriteString(returnType)
	sql.WriteString(" LANGUAGE ")
	sql.WriteString(language)
	sql.WriteString(" ")

	if opts.Volatility != "" {
		sql.WriteString(strings.ToUpper(opts.Volatility))
		sql.WriteString(" ")
	}

	sql.WriteString("AS $$ ")
	sql.WriteString(body)
	sql.WriteString(" $$")

	return c.Exec(sql.String())
}

// CreateProcedureOptions configures procedure creation.
type CreateProcedureOptions struct {
	Language  string // Default: "plpgsql"
	OrReplace bool
}

// CreateProcedure creates a stored procedure.
func (c *Client) CreateProcedure(name string, params []FunctionParameter, body string, opts *CreateProcedureOptions) error {
	if opts == nil {
		opts = &CreateProcedureOptions{}
	}
	language := opts.Language
	if language == "" {
		language = "plpgsql"
	}

	var sql strings.Builder
	if opts.OrReplace {
		sql.WriteString("CREATE OR REPLACE PROCEDURE ")
	} else {
		sql.WriteString("CREATE PROCEDURE ")
	}
	sql.WriteString(escapeIdentifier(name))
	sql.WriteString("(")
	for i, p := range params {
		if i > 0 {
			sql.WriteString(", ")
		}
		if p.Mode != "" {
			sql.WriteString(strings.ToUpper(p.Mode))
			sql.WriteString(" ")
		}
		sql.WriteString(escapeIdentifier(p.Name))
		sql.WriteString(" ")
		sql.WriteString(p.Type)
	}
	sql.WriteString(") LANGUAGE ")
	sql.WriteString(language)
	sql.WriteString(" AS $$ ")
	sql.WriteString(body)
	sql.WriteString(" $$")

	return c.Exec(sql.String())
}

// DropFunction drops a function.
func (c *Client) DropFunction(name string, paramTypes []string, ifExists bool) error {
	var sql strings.Builder
	sql.WriteString("DROP FUNCTION ")
	if ifExists {
		sql.WriteString("IF EXISTS ")
	}
	sql.WriteString(escapeIdentifier(name))
	if len(paramTypes) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(paramTypes, ", "))
		sql.WriteString(")")
	}
	return c.Exec(sql.String())
}

// DropProcedure drops a procedure.
func (c *Client) DropProcedure(name string, paramTypes []string, ifExists bool) error {
	var sql strings.Builder
	sql.WriteString("DROP PROCEDURE ")
	if ifExists {
		sql.WriteString("IF EXISTS ")
	}
	sql.WriteString(escapeIdentifier(name))
	if len(paramTypes) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(paramTypes, ", "))
		sql.WriteString(")")
	}
	return c.Exec(sql.String())
}

// ============================================================================
// JSON Operations
// ============================================================================

// JsonExtract extracts a JSON value using the -> operator.
func (c *Client) JsonExtract(column, path, table, where string, limit int) (*Result, error) {
	var pathExpr string
	if _, err := fmt.Sscanf(path, "%d", new(int)); err == nil {
		pathExpr = path // numeric index
	} else {
		pathExpr = fmt.Sprintf("'%s'", escapeString(path))
	}

	sql := fmt.Sprintf("SELECT %s -> %s AS value FROM %s",
		escapeIdentifier(column), pathExpr, escapeIdentifier(table))
	if where != "" {
		sql += " WHERE " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonExtractText extracts a JSON value as text using the ->> operator.
func (c *Client) JsonExtractText(column, path, table, where string, limit int) (*Result, error) {
	var pathExpr string
	if _, err := fmt.Sscanf(path, "%d", new(int)); err == nil {
		pathExpr = path
	} else {
		pathExpr = fmt.Sprintf("'%s'", escapeString(path))
	}

	sql := fmt.Sprintf("SELECT %s ->> %s AS value FROM %s",
		escapeIdentifier(column), pathExpr, escapeIdentifier(table))
	if where != "" {
		sql += " WHERE " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonExtractPath extracts a nested JSON value using the #> operator.
func (c *Client) JsonExtractPath(column string, path []string, table, where string, limit int) (*Result, error) {
	var escapedPath []string
	for _, p := range path {
		escapedPath = append(escapedPath, escapeString(p))
	}
	pathArray := fmt.Sprintf("'{%s}'", strings.Join(escapedPath, ","))

	sql := fmt.Sprintf("SELECT %s #> %s AS value FROM %s",
		escapeIdentifier(column), pathArray, escapeIdentifier(table))
	if where != "" {
		sql += " WHERE " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonExtractPathText extracts a nested JSON value as text using the #>> operator.
func (c *Client) JsonExtractPathText(column string, path []string, table, where string, limit int) (*Result, error) {
	var escapedPath []string
	for _, p := range path {
		escapedPath = append(escapedPath, escapeString(p))
	}
	pathArray := fmt.Sprintf("'{%s}'", strings.Join(escapedPath, ","))

	sql := fmt.Sprintf("SELECT %s #>> %s AS value FROM %s",
		escapeIdentifier(column), pathArray, escapeIdentifier(table))
	if where != "" {
		sql += " WHERE " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonContains checks if JSON contains a value using the @> operator.
func (c *Client) JsonContains(column string, value interface{}, table, where string, limit int) (*Result, error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON value: %w", err)
	}

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s @> '%s'::jsonb",
		escapeIdentifier(table), escapeIdentifier(column), escapeString(string(jsonValue)))
	if where != "" {
		sql += " AND " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonContainedBy checks if JSON is contained in a value using the <@ operator.
func (c *Client) JsonContainedBy(column string, value interface{}, table, where string, limit int) (*Result, error) {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON value: %w", err)
	}

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s <@ '%s'::jsonb",
		escapeIdentifier(table), escapeIdentifier(column), escapeString(string(jsonValue)))
	if where != "" {
		sql += " AND " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonPathExists checks if a JSON path exists using the @? operator.
func (c *Client) JsonPathExists(column, jsonPath, table, where string, limit int) (*Result, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s @? '%s'",
		escapeIdentifier(table), escapeIdentifier(column), escapeString(jsonPath))
	if where != "" {
		sql += " AND " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// JsonPathMatch checks if a JSON path predicate matches using the @@ operator.
func (c *Client) JsonPathMatch(column, jsonPath, table, where string, limit int) (*Result, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s @@ '%s'",
		escapeIdentifier(table), escapeIdentifier(column), escapeString(jsonPath))
	if where != "" {
		sql += " AND " + where
	}
	if limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", limit)
	}
	return c.Query(sql)
}

// escapeIdentifier escapes an identifier for SQL (handles qualified names).
func escapeIdentifier(identifier string) string {
	if strings.Contains(identifier, ".") {
		parts := strings.Split(identifier, ".")
		var escaped []string
		for _, part := range parts {
			escaped = append(escaped, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
		}
		return strings.Join(escaped, ".")
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// formatValue formats a value for SQL.
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%g", val)
	case string:
		return fmt.Sprintf("'%s'", escapeString(val))
	default:
		// Try JSON marshaling for complex types
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("'%s'", escapeString(fmt.Sprintf("%v", v)))
		}
		return fmt.Sprintf("'%s'", escapeString(string(jsonBytes)))
	}
}

// InTransaction executes a function within a transaction.
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
func (c *Client) InTransaction(fn func() error) error {
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

// ============================================================================
// AI/Vector Search Support
// ============================================================================

// VectorSearchResult represents a single result from vector similarity search.
type VectorSearchResult struct {
	ID            int64   `json:"id"`
	Score         float64 `json:"score"`
	Distance      float64 `json:"distance"`
	VectorScore   float64 `json:"vector_score"`
	MetadataScore float64 `json:"metadata_score,omitempty"`
}

// VectorSearchOptions configures vector similarity search.
type VectorSearchOptions struct {
	// Table is the table to search (required)
	Table string
	// VectorColumn is the column containing embeddings (default: "embedding")
	VectorColumn string
	// IDColumn is the column containing row IDs (default: "id")
	IDColumn string
	// Metric is the distance metric: "cosine", "euclidean", "dot", "manhattan"
	Metric string
	// Limit is the maximum number of results (default: 10)
	Limit int
	// Filter is an optional SQL WHERE clause for metadata filtering
	Filter string
	// SelectColumns are additional columns to return
	SelectColumns []string
}

// VectorSearch performs similarity search on vector embeddings.
//
// Example:
//
//	results, err := client.VectorSearch(ctx, queryVector, &VectorSearchOptions{
//	    Table:        "documents",
//	    VectorColumn: "embedding",
//	    Metric:       "cosine",
//	    Limit:        10,
//	    Filter:       "category = 'AI'",
//	})
func (c *Client) VectorSearch(queryVector []float32, opts *VectorSearchOptions) (*Result, error) {
	if opts == nil {
		return nil, errors.New("VectorSearchOptions is required")
	}
	if opts.Table == "" {
		return nil, errors.New("Table is required")
	}

	// Set defaults
	vectorCol := opts.VectorColumn
	if vectorCol == "" {
		vectorCol = "embedding"
	}
	idCol := opts.IDColumn
	if idCol == "" {
		idCol = "id"
	}
	metric := opts.Metric
	if metric == "" {
		metric = "cosine"
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}

	// Build the query
	selectCols := idCol
	if len(opts.SelectColumns) > 0 {
		selectCols = idCol + ", " + strings.Join(opts.SelectColumns, ", ")
	}

	// Format vector as array literal
	vectorStr := formatVector(queryVector)

	var sql string
	if metric == "cosine" {
		sql = fmt.Sprintf(
			"SELECT %s, vector_similarity(%s, %s) AS score FROM %s",
			selectCols, vectorCol, vectorStr, opts.Table,
		)
	} else {
		sql = fmt.Sprintf(
			"SELECT %s, vector_distance(%s, %s, '%s') AS score FROM %s",
			selectCols, vectorCol, vectorStr, metric, opts.Table,
		)
	}

	if opts.Filter != "" {
		sql += " WHERE " + opts.Filter
	}

	if metric == "cosine" {
		sql += " ORDER BY score DESC"
	} else {
		sql += " ORDER BY score ASC"
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// HybridSearchOptions configures hybrid search (vector + text).
type HybridSearchOptions struct {
	// Table is the table to search
	Table string
	// VectorColumn is the embedding column
	VectorColumn string
	// TextColumn is the text column for full-text search
	TextColumn string
	// IDColumn is the ID column
	IDColumn string
	// VectorWeight is the weight for vector similarity (0.0-1.0)
	VectorWeight float64
	// TextWeight is the weight for text relevance (0.0-1.0)
	TextWeight float64
	// Limit is the maximum results
	Limit int
	// Filter is an optional WHERE clause
	Filter string
	// SelectColumns are additional columns to return
	SelectColumns []string
}

// HybridSearch performs combined vector similarity and full-text search.
//
// This uses Reciprocal Rank Fusion (RRF) to combine results from both
// vector similarity and BM25 text search.
func (c *Client) HybridSearch(queryVector []float32, textQuery string, opts *HybridSearchOptions) (*Result, error) {
	if opts == nil {
		return nil, errors.New("HybridSearchOptions is required")
	}
	if opts.Table == "" {
		return nil, errors.New("Table is required")
	}

	// Set defaults
	vectorCol := opts.VectorColumn
	if vectorCol == "" {
		vectorCol = "embedding"
	}
	textCol := opts.TextColumn
	if textCol == "" {
		textCol = "content"
	}
	idCol := opts.IDColumn
	if idCol == "" {
		idCol = "id"
	}
	vectorWeight := opts.VectorWeight
	if vectorWeight <= 0 {
		vectorWeight = 0.5
	}
	textWeight := opts.TextWeight
	if textWeight <= 0 {
		textWeight = 0.5
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}

	selectCols := idCol
	if len(opts.SelectColumns) > 0 {
		selectCols = idCol + ", " + strings.Join(opts.SelectColumns, ", ")
	}

	vectorStr := formatVector(queryVector)

	// Build hybrid search query using weighted combination
	sql := fmt.Sprintf(`
		SELECT %s,
		       vector_similarity(%s, %s) * %.2f AS vector_score,
		       COALESCE(match_score(%s, '%s'), 0) * %.2f AS text_score,
		       vector_similarity(%s, %s) * %.2f + COALESCE(match_score(%s, '%s'), 0) * %.2f AS combined_score
		FROM %s
	`, selectCols,
		vectorCol, vectorStr, vectorWeight,
		textCol, escapeString(textQuery), textWeight,
		vectorCol, vectorStr, vectorWeight,
		textCol, escapeString(textQuery), textWeight,
		opts.Table,
	)

	if opts.Filter != "" {
		sql += " WHERE " + opts.Filter
	}

	sql += " ORDER BY combined_score DESC"
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// EmbeddingModel represents an embedding model configuration.
type EmbeddingModel struct {
	ID         string `json:"id"`
	Provider   string `json:"provider"`
	Dimensions int    `json:"dimensions"`
	MaxTokens  int    `json:"max_tokens"`
}

// CommonEmbeddingModels returns configurations for popular embedding models.
func CommonEmbeddingModels() map[string]EmbeddingModel {
	return map[string]EmbeddingModel{
		// OpenAI
		"text-embedding-3-small": {ID: "text-embedding-3-small", Provider: "openai", Dimensions: 1536, MaxTokens: 8191},
		"text-embedding-3-large": {ID: "text-embedding-3-large", Provider: "openai", Dimensions: 3072, MaxTokens: 8191},
		"text-embedding-ada-002": {ID: "text-embedding-ada-002", Provider: "openai", Dimensions: 1536, MaxTokens: 8191},
		// Open source
		"all-MiniLM-L6-v2":  {ID: "all-MiniLM-L6-v2", Provider: "huggingface", Dimensions: 384, MaxTokens: 256},
		"all-mpnet-base-v2": {ID: "all-mpnet-base-v2", Provider: "huggingface", Dimensions: 768, MaxTokens: 384},
		"bge-small-en-v1.5": {ID: "bge-small-en-v1.5", Provider: "huggingface", Dimensions: 384, MaxTokens: 512},
		"bge-base-en-v1.5":  {ID: "bge-base-en-v1.5", Provider: "huggingface", Dimensions: 768, MaxTokens: 512},
		"bge-large-en-v1.5": {ID: "bge-large-en-v1.5", Provider: "huggingface", Dimensions: 1024, MaxTokens: 512},
	}
}

// ChunkText splits text into chunks suitable for embedding.
func ChunkText(text string, chunkSize, overlap int) []string {
	if chunkSize <= 0 {
		chunkSize = 512
	}
	if overlap < 0 {
		overlap = 0
	}

	words := strings.Fields(text)
	if len(words) <= chunkSize {
		return []string{text}
	}

	var chunks []string
	step := chunkSize - overlap
	if step <= 0 {
		step = 1
	}

	for i := 0; i < len(words); i += step {
		end := i + chunkSize
		if end > len(words) {
			end = len(words)
		}
		chunk := strings.Join(words[i:end], " ")
		chunks = append(chunks, chunk)
		if end == len(words) {
			break
		}
	}

	return chunks
}

// CosineSimilarity computes cosine similarity between two vectors.
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dot / (sqrt(normA) * sqrt(normB))
}

// EuclideanDistance computes Euclidean (L2) distance between two vectors.
func EuclideanDistance(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return sqrt(sum)
}

// DotProduct computes the dot product of two vectors.
func DotProduct(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}

	return sum
}

// NormalizeVector returns a unit-length version of the vector.
func NormalizeVector(v []float32) []float32 {
	var norm float64
	for _, val := range v {
		norm += float64(val) * float64(val)
	}
	norm = sqrt(norm)

	if norm == 0 {
		return v
	}

	result := make([]float32, len(v))
	for i, val := range v {
		result[i] = float32(float64(val) / norm)
	}

	return result
}

// formatVector formats a float32 slice as a SQL array literal.
func formatVector(v []float32) string {
	var sb strings.Builder
	sb.WriteString("ARRAY[")
	for i, val := range v {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%g", val))
	}
	sb.WriteString("]")
	return sb.String()
}

// escapeString escapes a string for SQL.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// sqrt is a simple square root implementation.
func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x / 2
	for i := 0; i < 10; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}

// ============================================================================
// Cursor Support
// ============================================================================

// CursorOptions configures cursor declaration.
type CursorOptions struct {
	Scroll bool // Allow backward movement
	Hold   bool // Keep cursor open after commit
}

// DeclareCursor declares a server-side cursor.
func (c *Client) DeclareCursor(name, query string, opts *CursorOptions) error {
	sql := "DECLARE " + escapeIdentifier(name)
	if opts != nil {
		if opts.Scroll {
			sql += " SCROLL"
		}
		if opts.Hold {
			sql += " WITH HOLD"
		}
	}
	sql += " CURSOR FOR " + query
	return c.Exec(sql)
}

// FetchCursor fetches rows from a cursor.
func (c *Client) FetchCursor(name string, count int, direction string) (*Result, error) {
	safeName := escapeIdentifier(name)
	if direction == "" {
		direction = "NEXT"
	}
	dir := strings.ToUpper(direction)

	var sql string
	switch dir {
	case "NEXT", "PRIOR", "FIRST", "LAST":
		sql = fmt.Sprintf("FETCH %s %d FROM %s", dir, count, safeName)
	default:
		sql = fmt.Sprintf("FETCH %s FROM %s", dir, safeName)
	}
	return c.Query(sql)
}

// MoveCursor moves the cursor position without returning data.
func (c *Client) MoveCursor(name string, count int, direction string) error {
	safeName := escapeIdentifier(name)
	if direction == "" {
		direction = "NEXT"
	}
	dir := strings.ToUpper(direction)

	var sql string
	switch dir {
	case "NEXT", "PRIOR", "FIRST", "LAST":
		sql = fmt.Sprintf("MOVE %s %d IN %s", dir, count, safeName)
	default:
		sql = fmt.Sprintf("MOVE %s IN %s", dir, safeName)
	}
	return c.Exec(sql)
}

// CloseCursor closes a cursor. Use "*" or "ALL" to close all cursors.
func (c *Client) CloseCursor(name string) error {
	if name == "*" || strings.ToUpper(name) == "ALL" {
		return c.Exec("CLOSE ALL")
	}
	return c.Exec("CLOSE " + escapeIdentifier(name))
}

// ============================================================================
// Large Objects (LOB) Support
// ============================================================================

// LoCreate creates a new large object and returns its OID.
func (c *Client) LoCreate() (int64, error) {
	result, err := c.Query("SELECT lo_create(0) AS oid")
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if !result.Next() {
		return 0, errors.New("no result from lo_create")
	}
	var oid int64
	if err := result.Scan(&oid); err != nil {
		return 0, err
	}
	return oid, nil
}

// LoOpen opens a large object and returns a file descriptor.
// mode: "r" for read, "w" for write, "rw" for read-write.
func (c *Client) LoOpen(oid int64, mode string) (int, error) {
	// INV_READ = 0x40000, INV_WRITE = 0x20000
	modeFlag := 0x40000 // read
	if strings.Contains(mode, "w") {
		modeFlag |= 0x20000
	}
	result, err := c.Query(fmt.Sprintf("SELECT lo_open(%d, %d) AS fd", oid, modeFlag))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if !result.Next() {
		return 0, errors.New("no result from lo_open")
	}
	var fd int
	if err := result.Scan(&fd); err != nil {
		return 0, err
	}
	return fd, nil
}

// LoWrite writes data to a large object.
func (c *Client) LoWrite(fd int, data []byte) (int, error) {
	b64 := base64.StdEncoding.EncodeToString(data)
	result, err := c.Query(fmt.Sprintf("SELECT lowrite(%d, decode('%s', 'base64')) AS written", fd, b64))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if !result.Next() {
		return 0, errors.New("no result from lowrite")
	}
	var written int
	if err := result.Scan(&written); err != nil {
		return 0, err
	}
	return written, nil
}

// LoRead reads data from a large object.
func (c *Client) LoRead(fd int, length int) ([]byte, error) {
	result, err := c.Query(fmt.Sprintf("SELECT encode(loread(%d, %d), 'base64') AS data", fd, length))
	if err != nil {
		return nil, err
	}
	defer result.Close()
	if !result.Next() {
		return nil, errors.New("no result from loread")
	}
	var b64 string
	if err := result.Scan(&b64); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(b64)
}

// LoSeek seeks within a large object.
// whence: 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END
func (c *Client) LoSeek(fd int, offset int64, whence int) (int64, error) {
	result, err := c.Query(fmt.Sprintf("SELECT lo_lseek(%d, %d, %d) AS pos", fd, offset, whence))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if !result.Next() {
		return 0, errors.New("no result from lo_lseek")
	}
	var pos int64
	if err := result.Scan(&pos); err != nil {
		return 0, err
	}
	return pos, nil
}

// LoTell returns the current position in a large object.
func (c *Client) LoTell(fd int) (int64, error) {
	result, err := c.Query(fmt.Sprintf("SELECT lo_tell(%d) AS pos", fd))
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if !result.Next() {
		return 0, errors.New("no result from lo_tell")
	}
	var pos int64
	if err := result.Scan(&pos); err != nil {
		return 0, err
	}
	return pos, nil
}

// LoClose closes a large object file descriptor.
func (c *Client) LoClose(fd int) error {
	_, err := c.Query(fmt.Sprintf("SELECT lo_close(%d)", fd))
	return err
}

// LoUnlink deletes a large object.
func (c *Client) LoUnlink(oid int64) error {
	_, err := c.Query(fmt.Sprintf("SELECT lo_unlink(%d)", oid))
	return err
}

// LoImport imports data as a large object.
func (c *Client) LoImport(data []byte) (int64, error) {
	oid, err := c.LoCreate()
	if err != nil {
		return 0, err
	}
	fd, err := c.LoOpen(oid, "w")
	if err != nil {
		return 0, err
	}
	defer c.LoClose(fd)
	if _, err := c.LoWrite(fd, data); err != nil {
		return 0, err
	}
	return oid, nil
}

// LoExport exports a large object as bytes.
func (c *Client) LoExport(oid int64) ([]byte, error) {
	fd, err := c.LoOpen(oid, "r")
	if err != nil {
		return nil, err
	}
	defer c.LoClose(fd)

	var chunks [][]byte
	for {
		chunk, err := c.LoRead(fd, 8192)
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}
		chunks = append(chunks, chunk)
	}

	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	result := make([]byte, 0, total)
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}
	return result, nil
}

// ============================================================================
// Advisory Locks
// ============================================================================

// AdvisoryLock acquires a session-level advisory lock.
func (c *Client) AdvisoryLock(key int64, shared bool) error {
	fn := "pg_advisory_lock"
	if shared {
		fn = "pg_advisory_lock_shared"
	}
	_, err := c.Query(fmt.Sprintf("SELECT %s(%d)", fn, key))
	return err
}

// AdvisoryLockTry tries to acquire an advisory lock without blocking.
func (c *Client) AdvisoryLockTry(key int64, shared bool) (bool, error) {
	fn := "pg_try_advisory_lock"
	if shared {
		fn = "pg_try_advisory_lock_shared"
	}
	result, err := c.Query(fmt.Sprintf("SELECT %s(%d) AS acquired", fn, key))
	if err != nil {
		return false, err
	}
	defer result.Close()
	if !result.Next() {
		return false, errors.New("no result from advisory lock try")
	}
	var acquired bool
	if err := result.Scan(&acquired); err != nil {
		return false, err
	}
	return acquired, nil
}

// AdvisoryUnlock releases a session-level advisory lock.
func (c *Client) AdvisoryUnlock(key int64, shared bool) (bool, error) {
	fn := "pg_advisory_unlock"
	if shared {
		fn = "pg_advisory_unlock_shared"
	}
	result, err := c.Query(fmt.Sprintf("SELECT %s(%d) AS released", fn, key))
	if err != nil {
		return false, err
	}
	defer result.Close()
	if !result.Next() {
		return false, errors.New("no result from advisory unlock")
	}
	var released bool
	if err := result.Scan(&released); err != nil {
		return false, err
	}
	return released, nil
}

// AdvisoryUnlockAll releases all session-level advisory locks.
func (c *Client) AdvisoryUnlockAll() error {
	_, err := c.Query("SELECT pg_advisory_unlock_all()")
	return err
}

// AdvisoryXactLock acquires a transaction-level advisory lock.
func (c *Client) AdvisoryXactLock(key int64, shared bool) error {
	fn := "pg_advisory_xact_lock"
	if shared {
		fn = "pg_advisory_xact_lock_shared"
	}
	_, err := c.Query(fmt.Sprintf("SELECT %s(%d)", fn, key))
	return err
}

// AdvisoryXactLockTry tries to acquire a transaction-level advisory lock.
func (c *Client) AdvisoryXactLockTry(key int64, shared bool) (bool, error) {
	fn := "pg_try_advisory_xact_lock"
	if shared {
		fn = "pg_try_advisory_xact_lock_shared"
	}
	result, err := c.Query(fmt.Sprintf("SELECT %s(%d) AS acquired", fn, key))
	if err != nil {
		return false, err
	}
	defer result.Close()
	if !result.Next() {
		return false, errors.New("no result from advisory xact lock try")
	}
	var acquired bool
	if err := result.Scan(&acquired); err != nil {
		return false, err
	}
	return acquired, nil
}

// ============================================================================
// Full-Text Search
// ============================================================================

// FTSSearchOptions configures full-text search.
type FTSSearchOptions struct {
	Config string // Text search configuration (default: "english")
	Limit  int    // Maximum results (default: 100)
	Rank   bool   // Include ranking (default: true)
	Where  string // Additional WHERE clause
}

// FTSSearch performs a full-text search.
func (c *Client) FTSSearch(table, textColumn, query string, opts *FTSSearchOptions) (*Result, error) {
	if opts == nil {
		opts = &FTSSearchOptions{}
	}
	config := opts.Config
	if config == "" {
		config = "english"
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(textColumn)
	safeQuery := escapeString(query)

	var sql string
	if opts.Rank {
		sql = fmt.Sprintf(
			"SELECT *, ts_rank(to_tsvector('%s', %s), plainto_tsquery('%s', '%s')) AS rank "+
				"FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s')",
			config, safeColumn, config, safeQuery,
			safeTable, config, safeColumn, config, safeQuery,
		)
	} else {
		sql = fmt.Sprintf(
			"SELECT * FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s')",
			safeTable, config, safeColumn, config, safeQuery,
		)
	}

	if opts.Where != "" {
		sql += " AND " + opts.Where
	}
	if opts.Rank {
		sql += " ORDER BY rank DESC"
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// FTSHighlightOptions configures highlighted search results.
type FTSHighlightOptions struct {
	Config   string // Text search configuration (default: "english")
	StartTag string // Tag before match (default: "<b>")
	StopTag  string // Tag after match (default: "</b>")
	Limit    int    // Maximum results (default: 100)
}

// FTSHighlight performs a full-text search with highlighted results.
func (c *Client) FTSHighlight(table, textColumn, query string, opts *FTSHighlightOptions) (*Result, error) {
	if opts == nil {
		opts = &FTSHighlightOptions{}
	}
	config := opts.Config
	if config == "" {
		config = "english"
	}
	startTag := opts.StartTag
	if startTag == "" {
		startTag = "<b>"
	}
	stopTag := opts.StopTag
	if stopTag == "" {
		stopTag = "</b>"
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(textColumn)
	safeQuery := escapeString(query)

	sql := fmt.Sprintf(
		"SELECT *, ts_headline('%s', %s, plainto_tsquery('%s', '%s'), 'StartSel=%s, StopSel=%s') AS headline "+
			"FROM %s WHERE to_tsvector('%s', %s) @@ plainto_tsquery('%s', '%s') LIMIT %d",
		config, safeColumn, config, safeQuery, startTag, stopTag,
		safeTable, config, safeColumn, config, safeQuery, limit,
	)

	return c.Query(sql)
}

// ============================================================================
// Geospatial Support
// ============================================================================

// GeoDistanceOptions configures geospatial distance queries.
type GeoDistanceOptions struct {
	Limit int    // Maximum results (default: 100)
	Where string // Additional WHERE clause
}

// GeoDistance finds records within a radius of a point.
func (c *Client) GeoDistance(table, geoColumn string, lat, lon, radiusMeters float64, opts *GeoDistanceOptions) (*Result, error) {
	if opts == nil {
		opts = &GeoDistanceOptions{}
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(geoColumn)

	sql := fmt.Sprintf(
		"SELECT *, ST_Distance(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography) AS distance "+
			"FROM %s WHERE ST_DWithin(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography, %f)",
		safeColumn, lon, lat, safeTable, safeColumn, lon, lat, radiusMeters,
	)

	if opts.Where != "" {
		sql += " AND " + opts.Where
	}
	sql += fmt.Sprintf(" ORDER BY distance ASC LIMIT %d", limit)

	return c.Query(sql)
}

// GeoContains finds records where geometry contains a shape.
func (c *Client) GeoContains(table, geoColumn, wkt string, limit int) (*Result, error) {
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(geoColumn)

	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE ST_Contains(%s, ST_GeomFromText('%s', 4326)) LIMIT %d",
		safeTable, safeColumn, escapeString(wkt), limit,
	)

	return c.Query(sql)
}

// GeoNearest finds K nearest neighbors to a point.
func (c *Client) GeoNearest(table, geoColumn string, lat, lon float64, k int, where string) (*Result, error) {
	if k <= 0 {
		k = 10
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(geoColumn)

	sql := fmt.Sprintf(
		"SELECT *, ST_Distance(%s, ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geography) AS distance FROM %s",
		safeColumn, lon, lat, safeTable,
	)

	if where != "" {
		sql += " WHERE " + where
	}
	sql += fmt.Sprintf(" ORDER BY %s <-> ST_SetSRID(ST_MakePoint(%f, %f), 4326)::geometry LIMIT %d",
		safeColumn, lon, lat, k)

	return c.Query(sql)
}

// GeoIntersects finds records that intersect with a geometry.
func (c *Client) GeoIntersects(table, geoColumn, wkt string, limit int) (*Result, error) {
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(geoColumn)

	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE ST_Intersects(%s, ST_GeomFromText('%s', 4326)) LIMIT %d",
		safeTable, safeColumn, escapeString(wkt), limit,
	)

	return c.Query(sql)
}

// ============================================================================
// Time Travel Queries
// ============================================================================

// QueryAsOf queries data as of a specific timestamp.
func (c *Client) QueryAsOf(sql string, timestamp time.Time) (*Result, error) {
	ts := timestamp.Format(time.RFC3339)
	modifiedSQL := sql + fmt.Sprintf(" AS OF TIMESTAMP '%s'", escapeString(ts))
	return c.Query(modifiedSQL)
}

// QueryBetween queries data changes between two timestamps.
func (c *Client) QueryBetween(sql string, startTime, endTime time.Time) (*Result, error) {
	start := startTime.Format(time.RFC3339)
	end := endTime.Format(time.RFC3339)
	modifiedSQL := sql + fmt.Sprintf(" FOR SYSTEM_TIME FROM '%s' TO '%s'", escapeString(start), escapeString(end))
	return c.Query(modifiedSQL)
}

// GetHistoryOptions configures history queries.
type GetHistoryOptions struct {
	Since time.Time
	Until time.Time
	Limit int
}

// GetHistory retrieves the history of a table.
func (c *Client) GetHistory(table string, opts *GetHistoryOptions) (*Result, error) {
	if opts == nil {
		opts = &GetHistoryOptions{}
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	safeTable := escapeIdentifier(table)
	sql := "SELECT * FROM " + safeTable

	if !opts.Since.IsZero() && !opts.Until.IsZero() {
		since := opts.Since.Format(time.RFC3339)
		until := opts.Until.Format(time.RFC3339)
		sql += fmt.Sprintf(" FOR SYSTEM_TIME FROM '%s' TO '%s'", escapeString(since), escapeString(until))
	} else if !opts.Since.IsZero() {
		since := opts.Since.Format(time.RFC3339)
		sql += fmt.Sprintf(" FOR SYSTEM_TIME FROM '%s' TO NOW()", escapeString(since))
	}

	sql += fmt.Sprintf(" LIMIT %d", limit)
	return c.Query(sql)
}

// ============================================================================
// Batch Operations
// ============================================================================

// BatchInsert inserts multiple rows in batches.
func (c *Client) BatchInsert(table string, rows []map[string]interface{}, chunkSize int) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	safeTable := escapeIdentifier(table)

	// Get column names from first row
	var columns []string
	for col := range rows[0] {
		columns = append(columns, col)
	}

	var safeColumns []string
	for _, col := range columns {
		safeColumns = append(safeColumns, escapeIdentifier(col))
	}
	colList := strings.Join(safeColumns, ", ")

	totalInserted := 0
	for i := 0; i < len(rows); i += chunkSize {
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]

		var valueRows []string
		for _, row := range chunk {
			var vals []string
			for _, col := range columns {
				vals = append(vals, formatValue(row[col]))
			}
			valueRows = append(valueRows, "("+strings.Join(vals, ", ")+")")
		}

		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", safeTable, colList, strings.Join(valueRows, ", "))
		if err := c.Exec(sql); err != nil {
			return totalInserted, err
		}
		totalInserted += len(chunk)
	}

	return totalInserted, nil
}

// BatchUpdate updates multiple rows by key.
func (c *Client) BatchUpdate(table string, updates []map[string]interface{}, keyColumn string) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	safeTable := escapeIdentifier(table)
	safeKeyColumn := escapeIdentifier(keyColumn)
	totalUpdated := 0

	for _, update := range updates {
		keyValue := update[keyColumn]
		var setClauses []string
		for col, val := range update {
			if col != keyColumn {
				setClauses = append(setClauses, fmt.Sprintf("%s = %s", escapeIdentifier(col), formatValue(val)))
			}
		}
		if len(setClauses) == 0 {
			continue
		}

		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
			safeTable, strings.Join(setClauses, ", "), safeKeyColumn, formatValue(keyValue))
		if err := c.Exec(sql); err != nil {
			return totalUpdated, err
		}
		totalUpdated++
	}

	return totalUpdated, nil
}

// BatchUpsert performs bulk upsert (INSERT ... ON CONFLICT).
func (c *Client) BatchUpsert(table string, rows []map[string]interface{}, conflictColumns []string, updateColumns []string, chunkSize int) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	safeTable := escapeIdentifier(table)

	// Get all column names
	var columns []string
	for col := range rows[0] {
		columns = append(columns, col)
	}

	var safeColumns []string
	for _, col := range columns {
		safeColumns = append(safeColumns, escapeIdentifier(col))
	}
	colList := strings.Join(safeColumns, ", ")

	// Build conflict clause
	var safeConflict []string
	for _, col := range conflictColumns {
		safeConflict = append(safeConflict, escapeIdentifier(col))
	}
	conflictClause := strings.Join(safeConflict, ", ")

	// Build update clause
	if len(updateColumns) == 0 {
		// Update all non-conflict columns
		conflictSet := make(map[string]bool)
		for _, col := range conflictColumns {
			conflictSet[col] = true
		}
		for _, col := range columns {
			if !conflictSet[col] {
				updateColumns = append(updateColumns, col)
			}
		}
	}

	var updateClauses []string
	for _, col := range updateColumns {
		safeName := escapeIdentifier(col)
		updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", safeName, safeName))
	}
	updateClause := strings.Join(updateClauses, ", ")

	totalAffected := 0
	for i := 0; i < len(rows); i += chunkSize {
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[i:end]

		var valueRows []string
		for _, row := range chunk {
			var vals []string
			for _, col := range columns {
				vals = append(vals, formatValue(row[col]))
			}
			valueRows = append(valueRows, "("+strings.Join(vals, ", ")+")")
		}

		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s)",
			safeTable, colList, strings.Join(valueRows, ", "), conflictClause)
		if updateClause != "" {
			sql += " DO UPDATE SET " + updateClause
		} else {
			sql += " DO NOTHING"
		}

		if err := c.Exec(sql); err != nil {
			return totalAffected, err
		}
		totalAffected += len(chunk)
	}

	return totalAffected, nil
}

// BatchDelete deletes multiple rows by key values.
func (c *Client) BatchDelete(table string, keys []interface{}, keyColumn string, chunkSize int) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	safeTable := escapeIdentifier(table)
	safeKeyColumn := escapeIdentifier(keyColumn)
	totalDeleted := 0

	for i := 0; i < len(keys); i += chunkSize {
		end := i + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[i:end]

		var values []string
		for _, k := range chunk {
			values = append(values, formatValue(k))
		}

		sql := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			safeTable, safeKeyColumn, strings.Join(values, ", "))
		if err := c.Exec(sql); err != nil {
			return totalDeleted, err
		}
		totalDeleted += len(chunk)
	}

	return totalDeleted, nil
}

// ============================================================================
// Schema Introspection
// ============================================================================

// GetColumns returns column information for a table.
func (c *Client) GetColumns(table, database string) (*Result, error) {
	safeTable := escapeIdentifier(table)
	var sql string
	if database != "" {
		sql = fmt.Sprintf("DESCRIBE %s.%s", escapeIdentifier(database), safeTable)
	} else {
		sql = "DESCRIBE " + safeTable
	}
	return c.Query(sql)
}

// GetIndexes returns index information for a table.
func (c *Client) GetIndexes(table, database string) (*Result, error) {
	safeTable := escapeIdentifier(table)
	var sql string
	if database != "" {
		sql = fmt.Sprintf("SHOW INDEXES ON %s.%s", escapeIdentifier(database), safeTable)
	} else {
		sql = "SHOW INDEXES ON " + safeTable
	}
	return c.Query(sql)
}

// GetConstraints returns constraint information for a table.
func (c *Client) GetConstraints(table, database string) (*Result, error) {
	safeTable := escapeIdentifier(table)
	var sql string
	if database != "" {
		sql = fmt.Sprintf("SHOW CONSTRAINTS ON %s.%s", escapeIdentifier(database), safeTable)
	} else {
		sql = "SHOW CONSTRAINTS ON " + safeTable
	}
	return c.Query(sql)
}

// GetTableStats returns statistics for a table.
func (c *Client) GetTableStats(table, database string) (*Result, error) {
	safeTable := escapeIdentifier(table)
	var sql string
	if database != "" {
		sql = fmt.Sprintf("SHOW STATS FOR %s.%s", escapeIdentifier(database), safeTable)
	} else {
		sql = "SHOW STATS FOR " + safeTable
	}
	return c.Query(sql)
}

// SchemaInfo represents schema information for a database.
type SchemaInfo struct {
	Tables      []TableSchema      `json:"tables"`
	Indexes     []IndexInfo        `json:"indexes"`
	Constraints []ConstraintInfo   `json:"constraints"`
}

// TableSchema represents a table's schema.
type TableSchema struct {
	Name    string        `json:"name"`
	Columns []ColumnInfo  `json:"columns"`
	Error   string        `json:"error,omitempty"`
}

// ColumnInfo represents column information.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// IndexInfo represents index information.
type IndexInfo struct {
	Table  string `json:"table"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Unique bool   `json:"unique"`
}

// ConstraintInfo represents constraint information.
type ConstraintInfo struct {
	Table string `json:"table"`
	Name  string `json:"name"`
	Type  string `json:"type"`
}

// GetSchema returns full schema information for a database.
func (c *Client) GetSchema(database string) (*SchemaInfo, error) {
	tables, err := c.ListTables(database)
	if err != nil {
		return nil, err
	}

	schema := &SchemaInfo{
		Tables:      make([]TableSchema, 0),
		Indexes:     make([]IndexInfo, 0),
		Constraints: make([]ConstraintInfo, 0),
	}

	for _, tableInfo := range tables {
		tableName := tableInfo.Name
		ts := TableSchema{Name: tableName}

		// Get columns
		columns, err := c.GetColumns(tableName, database)
		if err != nil {
			ts.Error = err.Error()
		} else {
			defer columns.Close()
			for columns.Next() {
				// Parse column info from result
				// This depends on the exact format returned by DESCRIBE
				ts.Columns = append(ts.Columns, ColumnInfo{})
			}
		}
		schema.Tables = append(schema.Tables, ts)
	}

	return schema, nil
}

// ============================================================================
// Array Operations
// ============================================================================

// ArrayContains finds rows where an array column contains a value.
func (c *Client) ArrayContains(table, arrayColumn string, value interface{}, limit int, where string) (*Result, error) {
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(arrayColumn)

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s = ANY(%s)",
		safeTable, formatValue(value), safeColumn)
	if where != "" {
		sql += " AND " + where
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// ArrayOverlap finds rows where array columns have overlapping elements.
func (c *Client) ArrayOverlap(table, arrayColumn string, values []interface{}, limit int, where string) (*Result, error) {
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(arrayColumn)

	var formattedVals []string
	for _, v := range values {
		formattedVals = append(formattedVals, formatValue(v))
	}
	arrayLiteral := "ARRAY[" + strings.Join(formattedVals, ", ") + "]"

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s && %s",
		safeTable, safeColumn, arrayLiteral)
	if where != "" {
		sql += " AND " + where
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// ArrayContainsAll finds rows where array contains all specified values.
func (c *Client) ArrayContainsAll(table, arrayColumn string, values []interface{}, limit int, where string) (*Result, error) {
	if limit <= 0 {
		limit = 100
	}

	safeTable := escapeIdentifier(table)
	safeColumn := escapeIdentifier(arrayColumn)

	var formattedVals []string
	for _, v := range values {
		formattedVals = append(formattedVals, formatValue(v))
	}
	arrayLiteral := "ARRAY[" + strings.Join(formattedVals, ", ") + "]"

	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s @> %s",
		safeTable, safeColumn, arrayLiteral)
	if where != "" {
		sql += " AND " + where
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	return c.Query(sql)
}

// ============================================================================
// Async Pub/Sub Polling
// ============================================================================

// Notification represents a pub/sub notification.
type Notification struct {
	Channel string `json:"channel"`
	Payload string `json:"payload"`
}

// PollNotifications polls for pending notifications.
func (c *Client) PollNotifications(timeoutMs int) ([]Notification, error) {
	result, err := c.Query(fmt.Sprintf("SELECT * FROM pg_notification_queue(%d)", timeoutMs))
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var notifications []Notification
	for result.Next() {
		var channel, payload string
		if err := result.Scan(&channel, &payload); err != nil {
			continue
		}
		notifications = append(notifications, Notification{Channel: channel, Payload: payload})
	}
	return notifications, nil
}
