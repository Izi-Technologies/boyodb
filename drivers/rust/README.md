# boyodb Rust Driver

A Rust client library for connecting to boyodb-server.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
boyodb = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let result = client.query("SELECT * FROM mydb.users LIMIT 10").await?;
    for row in result.rows {
        println!("{:?}", row);
    }

    client.close().await?;
    Ok(())
}
```

## Configuration

```rust
use boyodb::{Client, Config};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let config = Config::builder()
        .tls(true)
        .ca_file("/path/to/ca.pem")
        .token("your-auth-token")
        .database("mydb")
        .query_timeout(60000) // 60 seconds
        .connect_timeout(Duration::from_secs(10))
        .build();

    let client = Client::connect("localhost:8765", config).await?;
    // ...
    Ok(())
}
```

## Authentication

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Login with username and password
    client.login("admin", "password").await?;

    // Execute authenticated queries
    let result = client.query("SELECT * FROM sensitive_data").await?;

    // Logout
    client.logout().await?;
client.close().await?;
Ok(())
}
```

## Prepared Statements

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let prepared_id = client.prepare(
        "SELECT * FROM calls WHERE tenant_id = 42",
        None,
    ).await?;
    let result = client.execute_prepared_binary(&prepared_id, None).await?;
    println!("Rows: {}", result.row_count());

    client.close().await?;
    Ok(())
}
```

## Database Operations

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Create a database
    client.create_database("analytics").await?;

    // Create a table
    client.create_table("analytics", "events").await?;

    // List databases
    let databases = client.list_databases().await?;
    println!("Databases: {:?}", databases);

    // List tables
    let tables = client.list_tables(Some("analytics")).await?;
    for table in tables {
        println!("Table: {}.{}", table.database, table.name);
    }

    client.close().await?;
    Ok(())
}
```

## CSV Ingestion

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let csv_data = "id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com";

    client.ingest_csv_str("mydb", "users", csv_data, true, None).await?;

    client.close().await?;
    Ok(())
}
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | Rust Type | Description |
|----------|-----------|-------------|
| `INT64`, `BIGINT` | `i64` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `i32` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `i16` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `String` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `bool` | true/false |
| `FLOAT64`, `DOUBLE` | `f64` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `f32` | 32-bit floating point |

### Advanced Types

| SQL Type | Rust Type | Description |
|----------|-----------|-------------|
| `UUID` | `String` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `String` | JSON document (use `serde_json` to parse) |
| `DECIMAL(p,s)` | `String` | Decimal number with precision and scale |
| `DATE` | `String` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `String` | Binary data (returned as hex string) |
| `TIMESTAMP` | `i64` | Unix timestamp in microseconds |

### Example Usage

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Create a table with various types
    client.exec(r#"
        CREATE TABLE mydb.products (
            id UUID,
            name STRING,
            price DECIMAL(10,2),
            metadata JSON,
            created_at DATE
        )
    "#).await?;

    // Insert data
    client.exec(r#"
        INSERT INTO mydb.products VALUES (
            '550e8400-e29b-41d4-a716-446655440000',
            'Widget',
            '19.99',
            '{"color": "blue"}',
            '2024-01-15'
        )
    "#).await?;

    // Query with type casting
    let result = client.query(r#"
        SELECT id, name, CAST(price AS FLOAT64) as price_float
        FROM mydb.products
    "#).await?;

    // Parse JSON field using serde_json
    for row in &result.rows {
        if let Some(serde_json::Value::String(metadata_str)) = row.get("metadata") {
            let metadata: serde_json::Value = serde_json::from_str(metadata_str)?;
            println!("Color: {}", metadata["color"]);
        }
    }

    client.close().await?;
    Ok(())
}
```

## Query Execution Plan

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let plan = client.explain("SELECT * FROM users WHERE id > 100").await?;
    println!("Query Plan: {}", plan);

    client.close().await?;
    Ok(())
}
```

## Error Handling

```rust
use boyodb::{Client, Config, Error};

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => println!("Success"),
        Err(Error::Connection(msg)) => eprintln!("Connection failed: {}", msg),
        Err(Error::Query(msg)) => eprintln!("Query failed: {}", msg),
        Err(Error::Auth(msg)) => eprintln!("Authentication failed: {}", msg),
        Err(e) => eprintln!("Error: {}", e),
    }
}

async fn run() -> Result<(), Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;
    let result = client.query("SELECT * FROM nonexistent_table").await?;
    client.close().await?;
    Ok(())
}
```

## API Reference

### Client

- `Client::connect(host, config)` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health
- `login(username, password)` - Login with credentials
- `logout()` - Logout from server
- `query(sql)` - Execute query
- `query_with_options(sql, database, timeout)` - Execute query with options
- `exec(sql)` - Execute statement
- `create_database(name)` - Create database
- `create_table(database, table)` - Create table
- `list_databases()` - List all databases
- `list_tables(database)` - List tables
- `explain(sql)` - Get query plan
- `metrics()` - Get server metrics
- `ingest_csv(database, table, data, has_header, delimiter)` - Ingest CSV bytes
- `ingest_csv_str(database, table, data, has_header, delimiter)` - Ingest CSV string
- `ingest_ipc(database, table, data)` - Ingest Arrow IPC

### Config

Use `Config::builder()` for a fluent API:

- `.tls(bool)` - Enable TLS
- `.ca_file(path)` - Set CA certificate path
- `.insecure_skip_verify(bool)` - Skip TLS verification
- `.connect_timeout(duration)` - Connection timeout
- `.read_timeout(duration)` - Read timeout
- `.write_timeout(duration)` - Write timeout
- `.token(string)` - Authentication token
- `.max_retries(usize)` - Max retries
- `.retry_delay(duration)` - Retry delay
- `.database(string)` - Default database
- `.query_timeout(u32)` - Query timeout in milliseconds
- `.build()` - Build the config

### QueryResult

- `rows: Vec<HashMap<String, Value>>` - Row data
- `columns: Vec<String>` - Column names
- `row_count()` - Number of rows
- `is_empty()` - Check if empty
- `segments_scanned` - Segments scanned
- `data_skipped_bytes` - Bytes skipped by pruning

## TLS Support

Enable TLS by adding the `tls` feature to your `Cargo.toml`:

```toml
[dependencies]
boyodb = { version = "0.1", features = ["tls"] }
```

### TLS with System Root Certificates

```rust
use boyodb::{Client, Config};

let config = Config::builder()
    .tls(true)
    .build();

let client = Client::connect("localhost:8765", config).await?;
```

### TLS with Custom CA Certificate

```rust
use boyodb::{Client, Config};

let config = Config::builder()
    .tls(true)
    .ca_file("/path/to/ca.pem")
    .build();

let client = Client::connect("localhost:8765", config).await?;
```

### TLS with Self-Signed Certificates (Development Only)

```rust
use boyodb::{Client, Config};

// WARNING: This disables certificate verification - NEVER use in production!
let config = Config::builder()
    .tls(true)
    .insecure_skip_verify(true)
    .build();

let client = Client::connect("localhost:8765", config).await?;
```

## Features

- `tls` - Enable TLS support (uses `rustls` for pure-Rust TLS)

## License

Apache-2.0
