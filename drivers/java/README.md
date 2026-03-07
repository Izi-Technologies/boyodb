# BoyoDB Java Client

High-performance Java client for BoyoDB analytical database.

## Features

- Connection pooling for concurrent access
- Binary protocol with Arrow IPC support
- TLS encryption with certificate verification
- Automatic retry on connection failures
- Transaction support with savepoints
- Full JDBC-like API

## Installation

### Maven

```xml
<dependency>
    <groupId>io.boyodb</groupId>
    <artifactId>boyodb-client</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'io.boyodb:boyodb-client:0.1.0'
```

## Quick Start

```java
import io.boyodb.client.*;

// Create client with connection pool
BoyoDBConfig config = BoyoDBConfig.builder()
    .host("localhost")
    .port(8765)
    .poolSize(10)
    .build();

try (BoyoDBClient client = new BoyoDBClient(config)) {
    // Execute a query
    QueryResult result = client.query("SELECT * FROM mydb.users LIMIT 10");

    for (Map<String, Object> row : result) {
        System.out.println(row.get("id") + ": " + row.get("name"));
    }
}
```

## Configuration

```java
BoyoDBConfig config = BoyoDBConfig.builder()
    .host("localhost")           // Server host
    .port(8765)                  // Server port
    .tls(true)                   // Enable TLS
    .caFile("/path/to/ca.crt")   // CA certificate
    .token("auth-token")         // Authentication token
    .database("mydb")            // Default database
    .poolSize(20)                // Connection pool size
    .connectTimeout(Duration.ofSeconds(10))
    .readTimeout(Duration.ofSeconds(30))
    .queryTimeout(60000)         // Query timeout in ms
    .maxRetries(3)               // Retry attempts
    .build();
```

## Usage Examples

### Authentication

```java
// Token-based auth (set in config)
BoyoDBConfig config = BoyoDBConfig.builder()
    .host("localhost")
    .token("my-secret-token")
    .build();

// User-based auth
try (BoyoDBClient client = new BoyoDBClient(config)) {
    client.login("admin", "password");
    // ... do work
    client.logout();
}
```

### Queries

```java
// Simple query
QueryResult result = client.query("SELECT * FROM users WHERE active = true");

// With database and timeout
QueryResult result = client.query(
    "SELECT COUNT(*) as cnt FROM events",
    "analytics",  // database
    60000         // timeout ms
);

// Get scalar value
Long count = result.getScalar("cnt");

// Get column as list
List<String> names = result.getColumn("name");
```

### Data Manipulation

```java
// Create database and table
client.createDatabase("mydb");
client.exec("CREATE TABLE mydb.users (id INT64, name STRING, email STRING)");

// Insert data
client.exec("INSERT INTO mydb.users VALUES (1, 'Alice', 'alice@example.com')");

// Update
client.exec("UPDATE mydb.users SET name = 'Alice Smith' WHERE id = 1");

// Delete
client.exec("DELETE FROM mydb.users WHERE id = 1");
```

### Transactions

```java
// Simple transaction
client.begin();
try {
    client.exec("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
    client.exec("INSERT INTO accounts (id, balance) VALUES (2, 500)");
    client.commit();
} catch (Exception e) {
    client.rollback();
    throw e;
}

// With isolation level
client.begin("SERIALIZABLE", false);
client.exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
client.exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
client.commit();

// Using transaction helper
String result = client.inTransaction(() -> {
    client.exec("INSERT INTO logs (msg) VALUES ('started')");
    QueryResult r = client.query("SELECT COUNT(*) as c FROM logs");
    return r.getScalar("c").toString();
});

// Savepoints
client.begin();
client.exec("INSERT INTO orders (id) VALUES (1)");
client.savepoint("sp1");
client.exec("INSERT INTO items (order_id) VALUES (1)");
client.rollbackToSavepoint("sp1");  // Undo items insert
client.commit();  // Commits only orders insert
```

### Bulk Data Ingestion

```java
// CSV ingestion
String csvData = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com";
client.ingestCSV("mydb", "users", csvData.getBytes(), true, ",");

// Arrow IPC ingestion (efficient for large datasets)
byte[] ipcData = createArrowIPC();  // Your Arrow IPC data
client.ingestIPC("mydb", "events", ipcData);
```

### Metadata Operations

```java
// List databases
List<String> databases = client.listDatabases();

// List tables
List<TableInfo> tables = client.listTables("mydb");
for (TableInfo table : tables) {
    System.out.println(table.getFullName());
}

// Explain query plan
String plan = client.explain("SELECT * FROM mydb.users WHERE id = 1");
System.out.println(plan);

// Get server metrics
String metrics = client.metrics();
```

## Thread Safety

The `BoyoDBClient` is thread-safe. The connection pool allows multiple threads to execute queries concurrently.

```java
// Safe to share across threads
BoyoDBClient client = new BoyoDBClient(config);

ExecutorService executor = Executors.newFixedThreadPool(10);
for (int i = 0; i < 100; i++) {
    executor.submit(() -> {
        QueryResult result = client.query("SELECT COUNT(*) FROM events");
        System.out.println(result.getScalar("count"));
    });
}
```

## Error Handling

```java
try {
    QueryResult result = client.query("SELECT * FROM nonexistent");
} catch (BoyoDBException e) {
    System.err.println("Query failed: " + e.getMessage());
}
```

## Performance Tips

1. **Use connection pooling**: Set `poolSize` based on concurrent query needs
2. **Use prepared statements**: For repeated queries (coming soon)
3. **Batch inserts**: Use `ingestIPC` for bulk data loading
4. **Set appropriate timeouts**: Avoid long-running queries blocking connections

## Requirements

- Java 11 or higher
- Apache Arrow 15.0.0+
- Jackson 2.16.1+

## Building from Source

```bash
cd drivers/java
mvn clean package
```

## License

Apache License 2.0
