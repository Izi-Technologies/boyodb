# boyodb Node.js Driver

A Node.js client library for connecting to boyodb-server.

## Installation

```bash
npm install boyodb
```

## Quick Start

```javascript
const { Client } = require('boyodb');

async function main() {
    const client = new Client('localhost:8765');
    await client.connect();

    const result = await client.query('SELECT * FROM mydb.users LIMIT 10');
    for (const row of result.rows) {
        console.log(row);
    }

    await client.close();
}

main().catch(console.error);
```

## Configuration

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765', {
    tls: true,
    caFile: '/path/to/ca.pem',
    token: 'your-auth-token',
    database: 'mydb',
    queryTimeout: 60000, // 60 seconds
    connectTimeout: 10000,
    readTimeout: 30000,
    writeTimeout: 10000,
    maxRetries: 3,
    retryDelay: 1000,
});

await client.connect();
```

## Authentication

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Login with username and password
await client.login('admin', 'password');

// Execute authenticated queries
const result = await client.query('SELECT * FROM sensitive_data');

// Logout
await client.logout();
await client.close();
```

## Prepared Statements

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const preparedId = await client.prepare('SELECT * FROM calls WHERE tenant_id = 42');
const result = await client.executePreparedBinary(preparedId);
console.log(`Rows: ${result.rowCount}`);

await client.close();
```

## Database Operations

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Create a database
await client.createDatabase('analytics');

// Create a table
await client.createTable('analytics', 'events');

// List databases
const databases = await client.listDatabases();
console.log('Databases:', databases);

// List tables
const tables = await client.listTables('analytics');
for (const table of tables) {
    console.log(`Table: ${table.database}.${table.name}`);
}

await client.close();
```

## CSV Ingestion

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const csvData = `id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com`;

await client.ingestCSV('mydb', 'users', csvData, { hasHeader: true });

await client.close();
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | JavaScript Type | Description |
|----------|-----------------|-------------|
| `INT64`, `BIGINT` | `number` / `BigInt` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `number` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `number` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `string` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `boolean` | true/false |
| `FLOAT64`, `DOUBLE` | `number` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `number` | 32-bit floating point |

### Advanced Types

| SQL Type | JavaScript Type | Description |
|----------|-----------------|-------------|
| `UUID` | `string` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `string` | JSON document (stored as string, use `JSON.parse()` to access) |
| `DECIMAL(p,s)` | `string` | Decimal number with precision and scale |
| `DATE` | `string` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `string` | Binary data (returned as hex string) |
| `TIMESTAMP` | `number` | Unix timestamp in microseconds |

### Example Usage

```javascript
// Create a table with various types
await client.exec(`
    CREATE TABLE mydb.products (
        id UUID,
        name STRING,
        price DECIMAL(10,2),
        metadata JSON,
        created_at DATE
    )
`);

// Insert data
await client.exec(`
    INSERT INTO mydb.products VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        'Widget',
        '19.99',
        '{"color": "blue"}',
        '2024-01-15'
    )
`);

// Query with type casting
const result = await client.query(`
    SELECT id, name, CAST(price AS FLOAT64) as price_float
    FROM mydb.products
`);

// Parse JSON field
for (const row of result.rows) {
    const metadata = JSON.parse(row.metadata);
    console.log(`Color: ${metadata.color}`);
}
```

## Query Execution Plan

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const plan = await client.explain('SELECT * FROM users WHERE id > 100');
console.log('Query Plan:', plan);

await client.close();
```

## Server Metrics

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const metrics = await client.metrics();
console.log('Server Metrics:', metrics);

await client.close();
```

## Error Handling

```javascript
const { Client } = require('boyodb');

async function main() {
    let client;
    try {
        client = new Client('localhost:8765');
        await client.connect();

        const result = await client.query('SELECT * FROM nonexistent_table');
    } catch (err) {
        if (err.message.includes('Connection')) {
            console.error('Connection failed:', err.message);
        } else if (err.message.includes('Query')) {
            console.error('Query failed:', err.message);
        } else if (err.message.includes('Auth')) {
            console.error('Authentication failed:', err.message);
        } else {
            console.error('Error:', err.message);
        }
    } finally {
        if (client) {
            await client.close();
        }
    }
}

main();
```

## API Reference

### Client

#### Constructor

```javascript
new Client(host, config)
```

- `host` (string) - Server address in `host:port` format
- `config` (object, optional) - Configuration options

#### Methods

- `connect()` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health
- `login(username, password)` - Login with credentials
- `logout()` - Logout from server
- `query(sql, options)` - Execute query
- `exec(sql, options)` - Execute statement
- `createDatabase(name)` - Create database
- `createTable(database, table)` - Create table
- `listDatabases()` - List all databases
- `listTables(database)` - List tables
- `explain(sql)` - Get query plan
- `metrics()` - Get server metrics
- `ingestCSV(database, table, csvData, options)` - Ingest CSV data
- `ingestIPC(database, table, ipcData)` - Ingest Arrow IPC data
- `setDatabase(database)` - Set default database
- `setToken(token)` - Set auth token

### Config Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tls` | boolean | false | Enable TLS encryption |
| `caFile` | string | null | Path to CA certificate file |
| `insecureSkipVerify` | boolean | false | Skip TLS verification (insecure) |
| `connectTimeout` | number | 10000 | Connection timeout in ms |
| `readTimeout` | number | 30000 | Read timeout in ms |
| `writeTimeout` | number | 10000 | Write timeout in ms |
| `token` | string | null | Authentication token |
| `maxRetries` | number | 3 | Max connection retries |
| `retryDelay` | number | 1000 | Retry delay in ms |
| `database` | string | null | Default database |
| `queryTimeout` | number | 30000 | Query timeout in ms |

### QueryResult

| Property | Type | Description |
|----------|------|-------------|
| `rows` | Array | Array of row objects |
| `columns` | Array | Column names |
| `rowCount` | number | Number of rows |
| `segmentsScanned` | number | Segments scanned |
| `dataSkippedBytes` | number | Bytes skipped by pruning |

## TLS Support

### TLS with System Root Certificates

```javascript
const client = new Client('localhost:8765', {
    tls: true,
});

await client.connect();
```

### TLS with Custom CA Certificate

```javascript
const client = new Client('localhost:8765', {
    tls: true,
    caFile: '/path/to/ca.pem',
});

await client.connect();
```

### TLS with Self-Signed Certificates (Development Only)

```javascript
// WARNING: This disables certificate verification - NEVER use in production!
const client = new Client('localhost:8765', {
    tls: true,
    insecureSkipVerify: true,
});

await client.connect();
```

## License

Apache-2.0
