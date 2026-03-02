# boyodb Go Driver

A Go client library for connecting to boyodb-server.

## Installation

```bash
go get github.com/loreste/boyodb/drivers/go/boyodb
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/loreste/boyodb/drivers/go/boyodb"
)

func main() {
    client, err := boyodb.NewClient("localhost:8765", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    result, err := client.Query("SELECT * FROM mydb.users LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }
    defer result.Close()

    for result.Next() {
        row := result.RowMap()
        fmt.Printf("Name: %v\n", row["name"])
    }
}
```

## Configuration

```go
package main

import (
    "time"

    "github.com/loreste/boyodb/drivers/go/boyodb"
)

func main() {
    config := &boyodb.Config{
        TLS:            true,
        CAFile:         "/path/to/ca.pem",
        Token:          "your-auth-token",
        Database:       "mydb",
        QueryTimeout:   60000, // 60 seconds
        ConnectTimeout: 10 * time.Second,
        ReadTimeout:    30 * time.Second,
        WriteTimeout:   10 * time.Second,
        MaxRetries:     3,
        RetryDelay:     1 * time.Second,
    }

    client, err := boyodb.NewClient("localhost:8765", config)
    if err != nil {
        panic(err)
    }
    defer client.Close()
}
```

## Authentication

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Login with username and password
if err := client.Login("admin", "password"); err != nil {
    log.Fatal(err)
}

// Execute authenticated queries
result, err := client.Query("SELECT * FROM sensitive_data")
if err != nil {
    log.Fatal(err)
}
defer result.Close()

// Logout
if err := client.Logout(); err != nil {
    log.Fatal(err)
}
```

## Prepared Statements

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

preparedId, err := client.Prepare("SELECT * FROM calls WHERE tenant_id = 42", "")
if err != nil {
    log.Fatal(err)
}

result, err := client.ExecutePreparedBinary(preparedId, 30000)
if err != nil {
    log.Fatal(err)
}
defer result.Close()

log.Printf("Rows: %d\n", result.RowCount())
```

## Database Operations

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Create a database
if err := client.CreateDatabase("analytics"); err != nil {
    log.Fatal(err)
}

// Create a table
if err := client.CreateTable("analytics", "events"); err != nil {
    log.Fatal(err)
}

// List databases
databases, err := client.ListDatabases()
if err != nil {
    log.Fatal(err)
}
fmt.Println("Databases:", databases)

// List tables
tables, err := client.ListTables("analytics")
if err != nil {
    log.Fatal(err)
}
for _, table := range tables {
    fmt.Printf("Table: %s.%s\n", table.Database, table.Name)
}
```

## CSV Ingestion

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

csvData := []byte(`id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com`)

if err := client.IngestCSV("mydb", "users", csvData, true, ""); err != nil {
    log.Fatal(err)
}
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | Go Type | Description |
|----------|---------|-------------|
| `INT64`, `BIGINT` | `int64` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `int32` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `int16` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `string` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `bool` | True/False |
| `FLOAT64`, `DOUBLE` | `float64` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `float32` | 32-bit floating point |

### Advanced Types

| SQL Type | Go Type | Description |
|----------|---------|-------------|
| `UUID` | `string` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `string` | JSON document (stored as string) |
| `DECIMAL(p,s)` | `string` | Decimal number with precision and scale |
| `DATE` | `string` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `string` | Binary data (returned as hex string) |
| `TIMESTAMP` | `int64` | Unix timestamp in microseconds |

### Example Usage

```go
// Create a table with various types
err := client.Exec(`
    CREATE TABLE mydb.products (
        id UUID,
        name STRING,
        price DECIMAL(10,2),
        metadata JSON,
        created_at DATE
    )
`)
if err != nil {
    log.Fatal(err)
}

// Insert data
err = client.Exec(`
    INSERT INTO mydb.products VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        'Widget',
        '19.99',
        '{"color": "blue"}',
        '2024-01-15'
    )
`)
if err != nil {
    log.Fatal(err)
}

// Query with type casting
result, err := client.Query(`
    SELECT id, name, CAST(price AS FLOAT64) as price_float
    FROM mydb.products
`)
if err != nil {
    log.Fatal(err)
}
defer result.Close()
```

## Query Execution Plan

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

plan, err := client.Explain("SELECT * FROM users WHERE id > 100")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Query Plan:", plan)
```

## Error Handling

```go
import (
    "errors"
    "fmt"

    "github.com/loreste/boyodb/drivers/go/boyodb"
)

client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    fmt.Printf("Connection failed: %v\n", err)
    return
}
defer client.Close()

result, err := client.Query("SELECT * FROM nonexistent_table")
if err != nil {
    fmt.Printf("Query failed: %v\n", err)
    return
}
defer result.Close()
```

## API Reference

### Client

- `NewClient(host string, config *Config) (*Client, error)` - Create and connect
- `Close() error` - Close the connection
- `Health() error` - Check server health
- `Login(username, password string) error` - Login with credentials
- `Logout() error` - Logout from server
- `Query(sql string) (*Result, error)` - Execute query
- `QueryContext(sql, database string, timeout uint32) (*Result, error)` - Execute query with options
- `Exec(sql string) error` - Execute statement
- `ExecContext(sql, database string, timeout uint32) error` - Execute statement with options
- `CreateDatabase(name string) error` - Create database
- `CreateTable(database, table string) error` - Create table
- `CreateTableWithSchema(database, table string, schema []map[string]interface{}) error` - Create table with schema
- `ListDatabases() ([]string, error)` - List all databases
- `ListTables(database string) ([]TableInfo, error)` - List tables
- `Explain(sql string) (string, error)` - Get query plan
- `Metrics() (string, error)` - Get server metrics
- `IngestCSV(database, table string, csvData []byte, hasHeader bool, delimiter string) error` - Ingest CSV
- `IngestIPC(database, table string, ipcData []byte) error` - Ingest Arrow IPC
- `SetDatabase(database string)` - Set default database
- `SetToken(token string)` - Set auth token

### Config

- `TLS bool` - Enable TLS (default: false)
- `TLSConfig *tls.Config` - Custom TLS configuration
- `CAFile string` - Path to CA certificate
- `InsecureSkipVerify bool` - Skip TLS verification (default: false)
- `ConnectTimeout time.Duration` - Connection timeout (default: 10s)
- `ReadTimeout time.Duration` - Read timeout (default: 30s)
- `WriteTimeout time.Duration` - Write timeout (default: 10s)
- `Token string` - Authentication token
- `MaxRetries int` - Max connection retries (default: 3)
- `RetryDelay time.Duration` - Retry delay (default: 1s)
- `Database string` - Default database
- `QueryTimeout uint32` - Query timeout in milliseconds (default: 30000)

### Result

- `Next() bool` - Advance to next row
- `Close() error` - Close result set
- `Scan(dest ...interface{}) error` - Scan current row into variables
- `RowMap() map[string]interface{}` - Get current row as map
- `Columns() []string` - Get column names
- `RowCount() int` - Get total row count

### TableInfo

- `Database string` - Database name
- `Name string` - Table name
- `SchemaJSON string` - Schema JSON (optional)

## TLS Support

### TLS with System Root Certificates

```go
config := &boyodb.Config{
    TLS: true,
}

client, err := boyodb.NewClient("localhost:8765", config)
```

### TLS with Custom CA Certificate

```go
config := &boyodb.Config{
    TLS:    true,
    CAFile: "/path/to/ca.pem",
}

client, err := boyodb.NewClient("localhost:8765", config)
```

### TLS with Self-Signed Certificates (Development Only)

```go
// WARNING: This disables certificate verification - NEVER use in production!
config := &boyodb.Config{
    TLS:                true,
    InsecureSkipVerify: true,
}

client, err := boyodb.NewClient("localhost:8765", config)
```

## License

Apache-2.0
