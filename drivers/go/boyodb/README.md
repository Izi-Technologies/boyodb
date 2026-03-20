# boyodb Go Driver

[![Version](https://img.shields.io/badge/version-0.9.8-green.svg)](../../../CHANGELOG.md)
[![Go](https://img.shields.io/badge/go-1.21+-00ADD8.svg)](https://go.dev/)

A Go client library for connecting to boyodb-server with support for transactions, analytics, external tables, and vector search.

## Features

- Connection pooling for concurrent access
- Binary protocol with Arrow IPC support
- TLS encryption with certificate verification
- ACID transactions with savepoints and isolation levels
- Approximate analytics (HyperLogLog, T-Digest)
- Time series and graph analytics
- External tables (S3, URL, HDFS, File)
- Vector similarity search

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

## Connection Pooling (High Performance)

For concurrent access and better performance, use the pooled client:

```go
package main

import (
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/loreste/boyodb/drivers/go/boyodb"
)

func main() {
    // Configure connection pool
    config := &boyodb.PoolConfig{
        Host:         "localhost",
        Port:         8765,
        PoolSize:     20,
        PoolTimeout:  30 * time.Second,
        Database:     "analytics",
        QueryTimeout: 60000,
    }

    // Create pooled client
    client, err := boyodb.NewPooledClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Thread-safe concurrent queries
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            result, err := client.Query(fmt.Sprintf("SELECT * FROM events WHERE id = %d", id))
            if err != nil {
                log.Printf("Query %d failed: %v", id, err)
                return
            }
            log.Printf("Query %d: %d rows", id, result.RowCount())
            result.Close()
        }(i)
    }
    wg.Wait()
}
```

### Pool Configuration

```go
config := &boyodb.PoolConfig{
    Host:           "localhost",     // Server host
    Port:           8765,            // Server port
    PoolSize:       20,              // Connections in pool
    PoolTimeout:    30 * time.Second,// Timeout for acquiring connection
    TLS:            true,            // Enable TLS
    CAFile:         "/path/to/ca.pem",
    Token:          "auth-token",    // Authentication token
    Database:       "mydb",          // Default database
    QueryTimeout:   60000,           // Query timeout (ms)
    ConnectTimeout: 10 * time.Second,
    ReadTimeout:    30 * time.Second,
    WriteTimeout:   10 * time.Second,
    MaxRetries:     3,
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

## Transactions

ACID transactions with savepoints:

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Manual transaction
tx := client.Transaction(&boyodb.TxOptions{
    IsolationLevel: boyodb.Serializable,
})
if err := tx.Begin(); err != nil {
    log.Fatal(err)
}

if err := tx.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')"); err != nil {
    tx.Rollback()
    log.Fatal(err)
}
if err := tx.Commit(); err != nil {
    log.Fatal(err)
}

// Auto-managed transaction
err = client.WithTransaction(func(tx *boyodb.Transaction) error {
    if err := tx.Exec("INSERT INTO orders (id, user_id) VALUES (1, 1)"); err != nil {
        return err
    }
    tx.Savepoint("sp1")
    if err := tx.Exec("INSERT INTO order_items (order_id) VALUES (1)"); err != nil {
        tx.RollbackTo("sp1")
    }
    return nil
}, nil)
```

## Approximate Analytics

Fast approximate aggregations:

```go
client, err := boyodb.NewClient("localhost:8765", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// HyperLogLog distinct count
approx := client.Approximate()
estimate, err := approx.CountDistinct("events", "user_id", "", 14)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Unique users: ~%d (±%.1f%%)\n", estimate.Estimate, estimate.RelativeError*100)

// T-Digest percentiles
percentiles, err := approx.Percentile("orders", "amount", []float64{50, 90, 99}, "", 0)
for _, p := range percentiles {
    fmt.Printf("P%.0f: %.2f\n", p.Quantile, p.Value)
}

// Top-K items
topEvents, err := approx.TopK("events", "event_type", 5, "")
for _, item := range topEvents {
    fmt.Printf("%s: %d\n", item.Item, item.Count)
}
```

## Time Series Analytics

```go
ts := client.TimeSeries()

// Aggregate by time buckets
hourly, err := ts.AggregateByTime(
    "metrics", "timestamp", "cpu_usage", "1 hour", "avg", "")

// Moving average
ma, err := ts.MovingAverage("metrics", "timestamp", "value", 7, "")

// Anomaly detection
anomalies, err := ts.DetectAnomalies("metrics", "timestamp", "value", 3.0, "")
```

## Graph Analytics

```go
graph := client.Graph()

// Shortest path
path, err := graph.ShortestPath("edges", "from_node", "to_node", "weight", 1, 10)

// PageRank
rankings, err := graph.PageRank("edges", "from_node", "to_node", 0.85, 20, 10)

// Connected components
components, err := graph.ConnectedComponents("edges", "from_node", "to_node")
```

## External Tables

Query external data without importing:

```go
ext := client.External()

// Query S3
result, err := ext.QueryS3(
    "s3://my-bucket/data/events.parquet",
    "SELECT event_type, COUNT(*) FROM data GROUP BY event_type",
    &boyodb.QueryS3Options{Format: "parquet"})

// Query URL
result, err = ext.QueryURL(
    "https://example.com/data.csv",
    "SELECT * FROM data WHERE value > 100",
    nil)

// Query local file
result, err = ext.QueryFile("/data/logs.json", "SELECT *", nil)

// Create external table
err = ext.CreateExternalTable("events_s3", boyodb.CreateExternalTableOptions{
    SourceType: "s3",
    Location:   "bucket/events/",
    Format:     "parquet",
    Columns: []boyodb.ExternalTableColumn{
        {Name: "id", Type: "INT64"},
        {Name: "event", Type: "STRING"},
        {Name: "ts", Type: "TIMESTAMP"},
    },
    PartitionColumns: []string{"date"},
})
```

## Vector Search

```go
vec := client.Vector()

// Create vector index
err := vec.CreateIndex("documents", "embedding", &boyodb.VectorIndexOptions{
    DistanceMetric: "cosine",
    M:              16,
    EfConstruction: 200,
})

// Search for similar vectors
queryVec := []float64{0.1, 0.2, 0.3}
result, err := vec.Search("documents", "embedding", queryVec, &boyodb.VectorSearchOptions{
    K:     10,
    Where: "category = 'tech'",
})

// Hybrid search (vector + full-text)
result, err = vec.HybridSearch(
    "documents", "embedding", "content", queryVec, "machine learning tutorial",
    &boyodb.HybridSearchOptions{K: 10, VectorWeight: 0.7, TextWeight: 0.3})
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

### PooledClient

- `NewPooledClient(config *PoolConfig) (*PooledClient, error)` - Create pooled client
- `Query(sql string) (*Result, error)` - Execute query
- `QueryContext(sql, database string, timeout uint32) (*Result, error)` - Execute with options
- `Exec(sql string) error` - Execute statement
- `ExecContext(sql, database string, timeout uint32) error` - Execute with options
- `Login(username, password string) error` - Login
- `Logout() error` - Logout
- `Close() error` - Close pool
- `SetDatabase(database string)` - Set default database

### PoolConfig

- `Host string` - Server host (default: localhost)
- `Port int` - Server port (default: 8765)
- `PoolSize int` - Number of connections (default: 10)
- `PoolTimeout time.Duration` - Acquire timeout (default: 30s)
- `TLS bool` - Enable TLS
- `CAFile string` - CA certificate file
- `Token string` - Authentication token
- `Database string` - Default database
- `QueryTimeout uint32` - Query timeout in ms

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
