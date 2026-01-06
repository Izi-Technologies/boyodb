# boyodb PHP Driver

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

use Cboyodb\Client;

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

use Cboyodb\Client;
use Cboyodb\Config;

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

use Cboyodb\Client;

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

## Prepared Statements

```php
<?php

use Cboyodb\Client;

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

use Cboyodb\Client;

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

use Cboyodb\Client;

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

use Cboyodb\Client;

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

use Cboyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$plan = $client->explain('SELECT * FROM users WHERE id > 100');
print_r($plan);

$client->close();
```

## Server Metrics

```php
<?php

use Cboyodb\Client;

$client = new Client('localhost:8765');
$client->connect();

$metrics = $client->metrics();
print_r($metrics);

$client->close();
```

## Error Handling

```php
<?php

use Cboyodb\Client;
use Cboyodb\ConnectionException;
use Cboyodb\QueryException;
use Cboyodb\AuthException;

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

## License

Apache-2.0
