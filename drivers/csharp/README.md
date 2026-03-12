# boyodb C#/.NET Driver

A C#/.NET client library for connecting to boyodb-server.

## Features

- Connection pooling for concurrent access
- Binary protocol with Arrow IPC support
- TLS encryption with certificate verification
- Async/await throughout
- Full cancellation token support

## Installation

```bash
dotnet add package Boyodb
```

Or add to your project file:

```xml
<PackageReference Include="Boyodb" Version="0.9.4" />
```

## Quick Start

```csharp
using Boyodb;

var client = new Client("localhost:8765");
await client.ConnectAsync();

var result = await client.QueryAsync("SELECT * FROM mydb.users LIMIT 10");
foreach (var row in result.Rows)
{
    Console.WriteLine(row["name"]);
}

client.Close();
```

## Connection Pooling (High Performance)

For concurrent access and better performance, use the pooled client:

```csharp
using Boyodb;

// Configure connection pool
var config = new PoolConfig
{
    Host = "localhost",
    Port = 8765,
    PoolSize = 20,
    Database = "analytics",
    QueryTimeout = 60000,
};

// Create pooled client
var client = new PooledClient(config);
await client.ConnectAsync();

// Thread-safe concurrent queries
var result = await client.QueryAsync("SELECT COUNT(*) FROM events");
Console.WriteLine(result.Rows[0]["count"]);

// Multiple concurrent queries
var tasks = Enumerable.Range(0, 100).Select(async i =>
{
    var r = await client.QueryAsync($"SELECT * FROM events WHERE id = {i}");
    return r.Rows.Count;
});
var results = await Task.WhenAll(tasks);

client.Dispose();
```

### Pool Configuration

```csharp
var config = new PoolConfig
{
    Host = "localhost",              // Server host
    Port = 8765,                     // Server port
    PoolSize = 20,                   // Connections in pool
    PoolTimeout = TimeSpan.FromSeconds(30), // Acquire timeout
    Tls = true,                      // Enable TLS
    CaFile = "/path/to/ca.pem",      // CA certificate
    Token = "auth-token",            // Authentication token
    Database = "mydb",               // Default database
    QueryTimeout = 60000,            // Query timeout (ms)
    ConnectTimeout = TimeSpan.FromSeconds(10),
    ReadTimeout = TimeSpan.FromSeconds(30),
    WriteTimeout = TimeSpan.FromSeconds(10),
    MaxRetries = 3,
};
```

## Configuration

```csharp
using Boyodb;

var config = new Config
{
    Tls = true,
    CaFile = "/path/to/ca.pem",
    Token = "your-auth-token",
    Database = "mydb",
    QueryTimeout = 60000, // 60 seconds
};

var client = new Client("localhost:8765", config);
await client.ConnectAsync();
```

## Authentication

```csharp
var client = new Client("localhost:8765");
await client.ConnectAsync();

// Login with username and password
await client.LoginAsync("admin", "password");

// Execute authenticated queries
var result = await client.QueryAsync("SELECT * FROM sensitive_data");

// Logout
await client.LogoutAsync();
client.Close();
```

## Prepared Statements

```csharp
using Boyodb;

var client = new Client("localhost:8765");
await client.ConnectAsync();

var preparedId = await client.PrepareAsync("SELECT * FROM calls WHERE tenant_id = 42");
var result = await client.ExecutePreparedBinaryAsync(preparedId);
Console.WriteLine($"Rows: {result.Rows.Count}");

client.Close();
```

## Database Operations

```csharp
var client = new Client("localhost:8765");
await client.ConnectAsync();

// Create a database
await client.CreateDatabaseAsync("analytics");

// Create a table
await client.CreateTableAsync("analytics", "events");

// List databases
var databases = await client.ListDatabasesAsync();
Console.WriteLine($"Databases: {string.Join(", ", databases)}");

// List tables
var tables = await client.ListTablesAsync("analytics");
foreach (var table in tables)
{
    Console.WriteLine($"Table: {table.Database}.{table.Name}");
}

client.Close();
```

## CSV Ingestion

```csharp
var client = new Client("localhost:8765");
await client.ConnectAsync();

var csvData = @"id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com";

await client.IngestCsvAsync("mydb", "users", csvData, hasHeader: true);

client.Close();
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | C# Type | Description |
|----------|---------|-------------|
| `INT64`, `BIGINT` | `long` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `int` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `short` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `string` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `bool` | True/False |
| `FLOAT64`, `DOUBLE` | `double` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `float` | 32-bit floating point |

### Advanced Types

| SQL Type | C# Type | Description |
|----------|---------|-------------|
| `UUID` | `string` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `string` | JSON document (use `JsonSerializer` to parse) |
| `DECIMAL(p,s)` | `string` | Decimal number with precision and scale |
| `DATE` | `string` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `string` | Binary data (returned as hex string) |
| `TIMESTAMP` | `long` | Unix timestamp in microseconds |

### Example Usage

```csharp
// Create a table with various types
await client.ExecAsync(@"
    CREATE TABLE mydb.products (
        id UUID,
        name STRING,
        price DECIMAL(10,2),
        metadata JSON,
        created_at DATE
    )
");

// Insert data
await client.ExecAsync(@"
    INSERT INTO mydb.products VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        'Widget',
        '19.99',
        '{""color"": ""blue""}',
        '2024-01-15'
    )
");

// Query with type casting
var result = await client.QueryAsync(@"
    SELECT id, name, CAST(price AS FLOAT64) as price_float
    FROM mydb.products
");

// Parse JSON field
using System.Text.Json;
foreach (var row in result.Rows)
{
    var metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(row["metadata"].ToString());
    Console.WriteLine($"Color: {metadata["color"]}");
}
```

## Query Execution Plan

```csharp
var client = new Client("localhost:8765");
await client.ConnectAsync();

var plan = await client.ExplainAsync("SELECT * FROM users WHERE id > 100");
Console.WriteLine($"Query Plan: {plan}");

client.Close();
```

## Error Handling

```csharp
using Boyodb;

try
{
    var client = new Client("localhost:8765");
    await client.ConnectAsync();

    var result = await client.QueryAsync("SELECT * FROM nonexistent_table");
}
catch (ConnectionException e)
{
    Console.WriteLine($"Connection failed: {e.Message}");
}
catch (QueryException e)
{
    Console.WriteLine($"Query failed: {e.Message}");
}
catch (AuthException e)
{
    Console.WriteLine($"Authentication failed: {e.Message}");
}
```

## API Reference

### Client

- `Client(string host, Config? config = null)` - Create a new client
- `ConnectAsync(CancellationToken ct = default)` - Connect to the server
- `Close()` - Close the connection
- `HealthAsync(CancellationToken ct = default)` - Check server health
- `LoginAsync(string username, string password, CancellationToken ct = default)` - Login
- `LogoutAsync(CancellationToken ct = default)` - Logout
- `QueryAsync(string sql, string? database = null, int? timeout = null, CancellationToken ct = default)` - Execute query
- `ExecAsync(string sql, string? database = null, int? timeout = null, CancellationToken ct = default)` - Execute statement
- `CreateDatabaseAsync(string name, CancellationToken ct = default)` - Create database
- `CreateTableAsync(string database, string table, CancellationToken ct = default)` - Create table
- `ListDatabasesAsync(CancellationToken ct = default)` - List all databases
- `ListTablesAsync(string? database = null, CancellationToken ct = default)` - List tables
- `ExplainAsync(string sql, CancellationToken ct = default)` - Get query plan
- `MetricsAsync(CancellationToken ct = default)` - Get server metrics
- `IngestCsvAsync(...)` - Ingest CSV data
- `IngestIpcAsync(...)` - Ingest Arrow IPC data

### Config

- `Tls` - Enable TLS (default: false)
- `CaFile` - Path to CA certificate
- `InsecureSkipVerify` - Skip TLS verification (default: false)
- `ConnectTimeout` - Connection timeout (default: 10s)
- `ReadTimeout` - Read timeout (default: 30s)
- `WriteTimeout` - Write timeout (default: 10s)
- `Token` - Authentication token
- `MaxRetries` - Max connection retries (default: 3)
- `RetryDelay` - Retry delay (default: 1s)
- `Database` - Default database
- `QueryTimeout` - Query timeout in milliseconds (default: 30000)

### QueryResult

- `Rows` - List of row dictionaries
- `Columns` - Column names
- `RowCount` - Number of rows
- `SegmentsScanned` - Segments scanned
- `DataSkippedBytes` - Bytes skipped by pruning

### PooledClient

- `PooledClient(PoolConfig config)` - Create pooled client
- `ConnectAsync(CancellationToken ct = default)` - Initialize pool
- `QueryAsync(string sql, string? database = null, int? timeout = null, CancellationToken ct = default)` - Execute query
- `ExecAsync(string sql, string? database = null, int? timeout = null, CancellationToken ct = default)` - Execute statement
- `LoginAsync(string username, string password, CancellationToken ct = default)` - Login
- `LogoutAsync(CancellationToken ct = default)` - Logout
- `SetDatabase(string database)` - Set default database
- `Dispose()` - Close pool

### PoolConfig

- `Host` - Server host (default: localhost)
- `Port` - Server port (default: 8765)
- `PoolSize` - Connections in pool (default: 10)
- `PoolTimeout` - Acquire timeout (default: 30s)
- `Tls` - Enable TLS
- `CaFile` - CA certificate file
- `Token` - Authentication token
- `Database` - Default database
- `QueryTimeout` - Query timeout in ms

## License

Apache-2.0
