# boyodb Rust Driver

[![Version](https://img.shields.io/badge/version-0.9.6-green.svg)](../../CHANGELOG.md)
[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org/)

A high-performance native Rust client for BoyoDB with connection pooling, batch inserts, transactions, and native Arrow support.

## Features

- **Connection Pooling**: Efficient connection reuse with health checks and automatic recovery
- **Batch Inserts**: High-throughput data ingestion using Arrow IPC format
- **Transactions**: ACID transaction support with isolation levels and savepoints
- **Native Arrow**: Direct RecordBatch support for zero-copy data transfer
- **TLS Support**: Secure connections with rustls
- **Async/Await**: Built on Tokio for async I/O

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
boyodb = "0.2"
tokio = { version = "1", features = ["full"] }
```

For TLS support:

```toml
[dependencies]
boyodb = { version = "0.2", features = ["tls"] }
```

## Quick Start

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let result = client.query("SELECT * FROM mydb.users LIMIT 10").await?;
    println!("Got {} rows", result.row_count);

    // Access Arrow RecordBatches directly
    for batch in result.record_batches() {
        println!("Batch with {} rows", batch.num_rows());
    }

    client.close().await?;
    Ok(())
}
```

## Connection Pooling

For high-throughput applications, use the connection pool to efficiently manage connections:

```rust
use boyodb::{Pool, PoolConfig, Config};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    // Create a pool with up to 20 connections
    let pool_config = PoolConfig::new(20)
        .with_min_connections(5)
        .with_acquire_timeout(Duration::from_secs(30))
        .with_idle_timeout(Duration::from_secs(600))
        .with_client_config(Config::default());

    let pool = Pool::new("localhost:8765", pool_config).await?;

    // Get a connection from the pool
    let conn = pool.get().await?;

    // Use the connection (automatically returned to pool when dropped)
    let result = conn.query("SELECT COUNT(*) FROM analytics.events").await?;
    println!("Count: {}", result.row_count);

    // Check pool statistics
    let stats = pool.stats();
    println!("Total connections: {}", stats.total_connections);
    println!("Idle connections: {}", stats.idle_connections);

    // Gracefully close all connections
    pool.close().await;
    Ok(())
}
```

### Pool Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `min_connections` | 1 | Minimum idle connections to maintain |
| `max_connections` | 10 | Maximum total connections |
| `acquire_timeout` | 30s | Time to wait for an available connection |
| `idle_timeout` | 600s | Close idle connections after this duration |
| `max_lifetime` | 3600s | Maximum lifetime of a connection |

## Batch Inserts

For high-throughput data ingestion, use the `BatchInserter`:

```rust
use boyodb::{Client, Config};
use boyodb::batch::{BatchInserter, Value};
use arrow_schema::{Schema, Field, DataType};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
    ]));

    // Create a batch inserter with batch size of 10,000 rows
    let mut inserter = BatchInserter::new(&client, "analytics", "users", schema, 10_000);

    // Insert rows - automatically flushes when batch size is reached
    for i in 0..100_000 {
        inserter.add_row_and_maybe_flush(vec![
            Value::Int64(i),
            Value::String(format!("user_{}", i)),
            Value::Float64(i as f64 * 0.1),
            Value::Bool(i % 2 == 0),
        ]).await?;
    }

    // Flush remaining rows
    let remaining = inserter.flush().await?;
    println!("Flushed {} remaining rows", remaining);

    client.close().await?;
    Ok(())
}
```

### Inserting RecordBatches Directly

For maximum performance with pre-built Arrow data:

```rust
use boyodb::{Client, Config};
use boyodb::batch::{insert_record_batch, record_batch_to_ipc};
use arrow_array::{Int64Array, StringArray, RecordBatch};
use arrow_schema::{Schema, Field, DataType};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Create a RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    ).unwrap();

    // Insert the batch directly
    let rows_inserted = insert_record_batch(&client, "mydb", "users", &batch).await?;
    println!("Inserted {} rows", rows_inserted);

    client.close().await?;
    Ok(())
}
```

### Value Types

| Value Variant | Arrow Type | Description |
|---------------|------------|-------------|
| `Value::Null` | Any nullable | Null value |
| `Value::Bool(bool)` | Boolean | Boolean |
| `Value::Int64(i64)` | Int64 | 64-bit integer |
| `Value::Float64(f64)` | Float64 | 64-bit float |
| `Value::String(String)` | Utf8 | UTF-8 string |
| `Value::TimestampMicros(i64)` | Timestamp | Microsecond timestamp |
| `Value::Bytes(Vec<u8>)` | Binary | Binary data |

## Transactions

Execute multiple operations atomically with transaction support:

```rust
use boyodb::{Client, Config};
use boyodb::transaction::{Transaction, IsolationLevel};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Begin a transaction with serializable isolation
    let txn = Transaction::begin_with_isolation(
        &client,
        IsolationLevel::Serializable
    ).await?;

    // Execute statements within the transaction
    txn.exec("INSERT INTO bank.accounts (id, balance) VALUES (1, 1000)").await?;
    txn.exec("INSERT INTO bank.accounts (id, balance) VALUES (2, 500)").await?;

    // Create a savepoint
    let savepoint = txn.savepoint("before_transfer").await?;

    txn.exec("UPDATE bank.accounts SET balance = balance - 100 WHERE id = 1").await?;
    txn.exec("UPDATE bank.accounts SET balance = balance + 100 WHERE id = 2").await?;

    // Rollback to savepoint if needed
    // savepoint.rollback().await?;

    // Or release the savepoint to continue
    savepoint.release().await?;

    // Commit the transaction
    txn.commit().await?;

    client.close().await?;
    Ok(())
}
```

### Using with_transaction Helper

For automatic commit/rollback handling:

```rust
use boyodb::{Client, Config};
use boyodb::transaction::TransactionExt;

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Transaction is automatically committed on success, rolled back on error
    client.with_transaction(|client| Box::pin(async move {
        client.exec("INSERT INTO logs.events (msg) VALUES ('start')").await?;
        client.exec("INSERT INTO logs.events (msg) VALUES ('end')").await?;
        Ok(())
    })).await?;

    client.close().await?;
    Ok(())
}
```

### Isolation Levels

| Level | Description |
|-------|-------------|
| `ReadCommitted` | See committed data only (default) |
| `RepeatableRead` | Consistent reads within transaction |
| `Serializable` | Full isolation, prevents phantoms |

## Native Arrow Support

Query results include native Arrow RecordBatches for efficient data processing:

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    let result = client.query("SELECT id, name, score FROM analytics.users").await?;

    // Access schema
    if let Some(schema) = result.schema() {
        for field in schema.fields() {
            println!("Column: {} ({})", field.name(), field.data_type());
        }
    }

    // Process RecordBatches directly
    for batch in result.record_batches() {
        // Access columns by name
        let id_col = batch.column_by_name("id").unwrap();
        let name_col = batch.column_by_name("name").unwrap();

        println!("Processing batch with {} rows", batch.num_rows());
    }

    // Or concatenate all batches into one
    if let Some(combined) = result.concat_batches() {
        println!("Total rows: {}", combined.num_rows());
    }

    // Legacy row-based access still available
    for row in result.iter() {
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
        .query_timeout(60000)
        .connect_timeout(Duration::from_secs(10))
        .max_retries(3)
        .retry_delay(Duration::from_secs(1))
        .build();

    let client = Client::connect("localhost:8765", config).await?;
    Ok(())
}
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `tls` | false | Enable TLS encryption |
| `ca_file` | None | Custom CA certificate path |
| `insecure_skip_verify` | false | Skip TLS verification (dev only) |
| `connect_timeout` | 10s | Connection timeout |
| `read_timeout` | 30s | Read timeout |
| `write_timeout` | 10s | Write timeout |
| `token` | None | Authentication token |
| `max_retries` | 3 | Connection retry attempts |
| `retry_delay` | 1s | Delay between retries |
| `database` | None | Default database |
| `query_timeout` | 30000 | Query timeout (ms) |

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

## Database Operations

```rust
use boyodb::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), boyodb::Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;

    // Create database and table
    client.create_database("analytics").await?;
    client.exec("CREATE TABLE analytics.events (id INT64, name STRING)").await?;

    // List databases
    let databases = client.list_databases().await?;
    println!("Databases: {:?}", databases);

    // List tables
    let tables = client.list_tables(Some("analytics")).await?;
    for table in tables {
        println!("Table: {}.{}", table.database, table.name);
    }

    // Get query plan
    let plan = client.explain("SELECT * FROM analytics.events WHERE id > 100").await?;
    println!("Query Plan: {}", plan);

    // Get server metrics
    let metrics = client.metrics().await?;
    println!("Metrics: {}", metrics);

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

    // Or with custom delimiter
    let tsv_data = "id\tname\temail
1\tAlice\talice@example.com";
    client.ingest_csv_str("mydb", "users", tsv_data, true, Some("\t")).await?;

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
        Err(Error::Pool(msg)) => eprintln!("Pool error: {}", msg),
        Err(Error::Batch(msg)) => eprintln!("Batch insert error: {}", msg),
        Err(Error::Transaction(msg)) => eprintln!("Transaction error: {}", msg),
        Err(Error::Timeout(msg)) => eprintln!("Timeout: {}", msg),
        Err(e) => eprintln!("Error: {}", e),
    }
}

async fn run() -> Result<(), Error> {
    let client = Client::connect("localhost:8765", Config::default()).await?;
    client.query("SELECT * FROM mydb.table").await?;
    client.close().await?;
    Ok(())
}
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `connect(host, config)` | Connect to server |
| `close()` | Close connection |
| `health()` | Check server health |
| `login(user, pass)` | Authenticate |
| `logout()` | End session |
| `query(sql)` | Execute query |
| `query_with_options(sql, db, timeout)` | Query with options |
| `exec(sql)` | Execute statement |
| `prepare(sql, db)` | Prepare statement |
| `execute_prepared_binary(id, timeout)` | Execute prepared |
| `create_database(name)` | Create database |
| `create_table(db, table)` | Create table |
| `list_databases()` | List databases |
| `list_tables(db)` | List tables |
| `explain(sql)` | Get query plan |
| `metrics()` | Get metrics |
| `ingest_csv(db, table, data, header, delim)` | Ingest CSV |
| `ingest_ipc(db, table, data)` | Ingest Arrow IPC |

### QueryResult

| Field/Method | Description |
|--------------|-------------|
| `rows` | Legacy row access (Vec<HashMap>) |
| `columns` | Column names |
| `batches` | Arrow RecordBatches |
| `schema` | Arrow schema |
| `row_count` | Total rows |
| `is_empty()` | Check if empty |
| `iter()` | Iterate rows |
| `record_batches()` | Get batches |
| `concat_batches()` | Merge batches |
| `column(name)` | Get column array |

### Pool

| Method | Description |
|--------|-------------|
| `new(host, config)` | Create pool |
| `get()` | Acquire connection |
| `stats()` | Get statistics |
| `close()` | Close all connections |

### Transaction

| Method | Description |
|--------|-------------|
| `begin(client)` | Start transaction |
| `begin_with_isolation(client, level)` | Start with isolation |
| `query(sql)` | Query in transaction |
| `exec(sql)` | Execute in transaction |
| `savepoint(name)` | Create savepoint |
| `commit()` | Commit transaction |
| `rollback()` | Rollback transaction |
| `is_active()` | Check if active |

## Features

| Feature | Description |
|---------|-------------|
| `default` | Arrow support |
| `tls` | TLS with rustls |

## Performance Tips

1. **Use Connection Pooling**: Reuse connections for multiple queries
2. **Batch Inserts**: Use `BatchInserter` for bulk data loading
3. **Native Arrow**: Work with RecordBatches directly instead of row iteration
4. **Prepared Statements**: Prepare frequently-used queries
5. **Streaming**: Use `query_binary` with `stream: true` for large results

## License

Apache-2.0
