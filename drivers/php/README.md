# boyodb PHP Driver

[![Version](https://img.shields.io/badge/version-0.9.8-green.svg)](../../CHANGELOG.md)
[![PHP](https://img.shields.io/badge/php-8.0+-777BB4.svg)](https://www.php.net/)

A PHP client library for connecting to boyodb-server.

## Requirements

- PHP 8.0 or higher

## Installation

```bash
composer require boyodb/boyodb
```

## Quick Start

```php
<?php

require 'vendor/autoload.php';

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$result = $client->query('SELECT * FROM mydb.users LIMIT 10');
foreach ($result as $row) {
    echo $row['name'] . PHP_EOL;
}

$client->close();
```

## Configuration

```php
<?php

use Boyodb\Client;
use Boyodb\Config;

$config = new Config([
    'tls' => true,
    'caFile' => '/path/to/ca.pem',
    'token' => 'your-auth-token',
    'database' => 'mydb',
    'queryTimeout' => 60000, // 60 seconds
]);

$client = new Client('localhost:8765', $config);
$client->connect();
```

## Authentication

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

// Login with username and password
$client->login('admin', 'password');

// Execute authenticated queries
$result = $client->query('SELECT * FROM sensitive_data');

// Logout
$client->logout();
$client->close();
```

## Connection Pooling (High Performance)

For high-throughput applications, use `PooledClient` which maintains a pool of reusable connections:

```php
<?php

use Boyodb\PooledClient;
use Boyodb\PoolConfig;

$config = new PoolConfig([
    'host' => 'localhost',
    'port' => 8765,
    'poolSize' => 20,
    'database' => 'analytics',
]);

$client = new PooledClient($config);

// Execute queries - connections are automatically managed
$result = $client->query("SELECT COUNT(*) FROM events");

// Check pool statistics
$stats = $client->poolStats();
echo "Pool size: {$stats['pool_size']}\n";
echo "Active: {$stats['active']}\n";
echo "Available: {$stats['available']}\n";

$client->close();
```

### Pool Configuration Options

```php
<?php

use Boyodb\PoolConfig;

$config = new PoolConfig([
    // Connection target
    'host' => 'localhost',
    'port' => 8765,

    // Pool settings
    'poolSize' => 10,           // Maximum connections in pool
    'poolTimeout' => 30.0,      // Timeout waiting for available connection

    // Connection settings
    'tls' => false,
    'caFile' => null,
    'insecureSkipVerify' => false,
    'connectTimeout' => 10.0,
    'readTimeout' => 30.0,
    'writeTimeout' => 10.0,

    // Authentication
    'token' => null,

    // Retry settings
    'maxRetries' => 3,
    'retryDelay' => 1.0,

    // Query defaults
    'database' => 'mydb',
    'queryTimeout' => 30000,    // milliseconds
]);
```

### Data Ingestion with Pooled Client

```php
<?php

use Boyodb\PooledClient;
use Boyodb\PoolConfig;

$client = new PooledClient(new PoolConfig(['host' => 'localhost']));

// CSV ingestion
$csvData = "id,name,value\n1,foo,100\n2,bar,200";
$client->ingestCsv('mydb', 'events', $csvData, hasHeader: true);

// Zero-copy Arrow IPC ingestion (highest performance)
$ipcData = file_get_contents('data.arrow');
$client->ingestIpc('mydb', 'events', $ipcData);

$client->close();
```

### Manual Connection Management

For fine-grained control, you can borrow and return connections directly:

```php
<?php

use Boyodb\ConnectionPool;
use Boyodb\PoolConfig;

$pool = new ConnectionPool(new PoolConfig(['host' => 'localhost']));

$conn = $pool->borrow();
try {
    $response = $pool->sendRequest($conn, [
        'op' => 'query',
        'sql' => 'SELECT * FROM mydb.users',
    ]);
    // Process response...
} finally {
    $pool->return($conn);
}

$pool->close();
```

## Prepared Statements

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$preparedId = $client->prepare('SELECT * FROM calls WHERE tenant_id = 42');
$result = $client->executePreparedBinary($preparedId);
echo "Rows: " . count($result->rows) . PHP_EOL;

$client->close();
```

## Database Operations

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

// Create a database
$client->createDatabase('analytics');

// Create a table
$client->createTable('analytics', 'events');

// List databases
$databases = $client->listDatabases();
echo 'Databases: ' . implode(', ', $databases) . PHP_EOL;

// List tables
$tables = $client->listTables('analytics');
foreach ($tables as $table) {
    echo "Table: {$table->database}.{$table->name}" . PHP_EOL;
}

$client->close();
```

## CSV Ingestion

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$csvData = <<<CSV
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
CSV;

$client->ingestCsv('mydb', 'users', $csvData, hasHeader: true);

$client->close();
```

## Zero-Copy Arrow IPC Ingestion

For ultra-high performance, BoyoDB supports native binary Zero-Copy streaming of Apache Arrow IPC buffers. This allows you to skip all JSON/CSV string parsing and pipe raw columnar analytics data natively over the TCP socket.

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

// Provide raw binary bytes representing an Arrow IPC stream
$rawArrowBytes = file_get_contents('data.ipc');

// Native Zero-Copy ingestion straight to the BoyoDB engine
$client->ingestIpc('mydb', 'users', $rawArrowBytes);

$client->close();
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | PHP Type | Description |
|----------|----------|-------------|
| `INT64`, `BIGINT` | `int` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `int` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `int` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `string` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `bool` | true/false |
| `FLOAT64`, `DOUBLE` | `float` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `float` | 32-bit floating point |

### Advanced Types

| SQL Type | PHP Type | Description |
|----------|----------|-------------|
| `UUID` | `string` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `string` | JSON document (use `json_decode()` to parse) |
| `DECIMAL(p,s)` | `string` | Decimal number with precision and scale |
| `DATE` | `string` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `string` | Binary data (returned as hex string) |
| `TIMESTAMP` | `int` | Unix timestamp in microseconds |

### Example Usage

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

// Create a table with various types
$client->exec('
    CREATE TABLE mydb.products (
        id UUID,
        name STRING,
        price DECIMAL(10,2),
        metadata JSON,
        created_at DATE
    )
');

// Insert data
$client->exec("
    INSERT INTO mydb.products VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        'Widget',
        '19.99',
        '{\"color\": \"blue\"}',
        '2024-01-15'
    )
");

// Query with type casting
$result = $client->query('
    SELECT id, name, CAST(price AS FLOAT64) as price_float
    FROM mydb.products
');

// Parse JSON field
foreach ($result as $row) {
    $metadata = json_decode($row['metadata'], true);
    echo "Color: {$metadata['color']}" . PHP_EOL;
}

$client->close();
```

## Query Execution Plan

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$plan = $client->explain('SELECT * FROM users WHERE id > 100');
print_r($plan);

$client->close();
```

## Server Metrics

```php
<?php

use Boyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$metrics = $client->metrics();
print_r($metrics);

$client->close();
```

## Error Handling

```php
<?php

use Boyodb\Client;
use Boyodb\ConnectionException;
use Boyodb\QueryException;
use Boyodb\AuthException;

try {
    $client = new Client('localhost:8765');
    $client->connect();

    $result = $client->query('SELECT * FROM nonexistent_table');
} catch (ConnectionException $e) {
    echo "Connection failed: {$e->getMessage()}" . PHP_EOL;
} catch (QueryException $e) {
    echo "Query failed: {$e->getMessage()}" . PHP_EOL;
} catch (AuthException $e) {
    echo "Authentication failed: {$e->getMessage()}" . PHP_EOL;
} finally {
    if (isset($client)) {
        $client->close();
    }
}
```

## API Reference

### Client

- `__construct(string $host, ?Config $config = null)` - Create a new client
- `connect()` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health
- `login(string $username, string $password)` - Login with credentials
- `logout()` - Logout from server
- `query(string $sql, ?string $database = null, ?int $timeout = null)` - Execute query
- `exec(string $sql, ?string $database = null, ?int $timeout = null)` - Execute statement
- `createDatabase(string $name)` - Create database
- `createTable(string $database, string $table)` - Create table
- `listDatabases()` - List all databases
- `listTables(?string $database = null)` - List tables
- `explain(string $sql)` - Get query plan
- `metrics()` - Get server metrics
- `ingestCsv(string $database, string $table, string $csvData, bool $hasHeader = true, ?string $delimiter = null)` - Ingest CSV
- `ingestIpc(string $database, string $table, string $ipcData)` - Ingest Arrow IPC

### Config

Properties (set via constructor array or directly):

- `tls` - Enable TLS (default: false)
- `caFile` - Path to CA certificate
- `insecureSkipVerify` - Skip TLS verification (default: false)
- `connectTimeout` - Connection timeout in seconds (default: 10.0)
- `readTimeout` - Read timeout in seconds (default: 30.0)
- `writeTimeout` - Write timeout in seconds (default: 10.0)
- `token` - Authentication token
- `maxRetries` - Max connection retries (default: 3)
- `retryDelay` - Retry delay in seconds (default: 1.0)
- `database` - Default database
- `queryTimeout` - Query timeout in milliseconds (default: 30000)

### QueryResult

Implements `Countable` and `IteratorAggregate`.

- `$rows` - Array of row associative arrays
- `$columns` - Column names
- `count()` / `rowCount()` - Number of rows
- `isEmpty()` - Check if empty
- `getRow(int $index)` - Get row by index
- `getRows()` - Get all rows
- `getColumns()` - Get column names
- `$segmentsScanned` - Segments scanned
- `$dataSkippedBytes` - Bytes skipped by pruning

### TableInfo

- `$database` - Database name
- `$name` - Table name
- `$schemaJson` - Schema JSON (optional)

### PooledClient

High-performance client with connection pooling.

- `__construct(PoolConfig $config)` - Create pooled client
- `query(string $sql, ?string $database = null, ?int $timeout = null)` - Execute query
- `exec(string $sql, ?string $database = null, ?int $timeout = null)` - Execute statement
- `login(string $username, string $password)` - Login with credentials
- `logout()` - Logout from server
- `health()` - Check server health
- `createDatabase(string $name)` - Create database
- `createTable(string $database, string $table)` - Create table
- `listDatabases()` - List all databases
- `listTables(?string $database = null)` - List tables
- `ingestCsv(string $database, string $table, string $csvData, bool $hasHeader = true, ?string $delimiter = null)` - Ingest CSV
- `ingestIpc(string $database, string $table, string $ipcData)` - Ingest Arrow IPC (zero-copy)
- `poolStats()` - Get pool statistics
- `setDatabase(string $database)` - Set default database
- `close()` - Close client and release connections

### PoolConfig

Configuration for connection pool (set via constructor array):

- `host` - Server hostname (default: 'localhost')
- `port` - Server port (default: 8765)
- `poolSize` - Maximum connections (default: 10)
- `poolTimeout` - Timeout waiting for connection (default: 30.0)
- `tls` - Enable TLS (default: false)
- `caFile` - Path to CA certificate
- `insecureSkipVerify` - Skip TLS verification (default: false)
- `connectTimeout` - Connection timeout in seconds (default: 10.0)
- `readTimeout` - Read timeout in seconds (default: 30.0)
- `writeTimeout` - Write timeout in seconds (default: 10.0)
- `token` - Authentication token
- `maxRetries` - Max connection retries (default: 3)
- `retryDelay` - Retry delay in seconds (default: 1.0)
- `database` - Default database
- `queryTimeout` - Query timeout in milliseconds (default: 30000)

### ConnectionPool

Low-level connection pool for manual management.

- `__construct(PoolConfig $config)` - Create pool
- `borrow()` - Borrow a connection from pool
- `return(PooledConnection $conn)` - Return connection to pool
- `sendRequest(PooledConnection $conn, array $request)` - Send request on connection
- `sendBinaryRequest(PooledConnection $conn, array $request, string $payload)` - Send binary request
- `health()` - Check server health
- `setSessionId(?string $sessionId)` - Set session ID for auth
- `stats()` - Get pool statistics
- `close()` - Close pool and all connections

## License

Apache-2.0
