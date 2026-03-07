# BoyoDB Node.js Driver

High-performance Node.js client for BoyoDB analytical database.

## Features

- Connection pooling for concurrent access
- Binary protocol with Arrow IPC support
- TLS encryption with certificate verification
- Automatic retry on connection failures
- Transaction support with savepoints
- Prepared statements

## Installation

```bash
npm install boyodb
```

## Quick Start

```javascript
const { Client } = require('boyodb');

// Create and connect client
const client = new Client('localhost:8765');
await client.connect();

// Execute a query
const result = await client.query('SELECT * FROM mydb.users LIMIT 10');
for (const row of result.rows) {
  console.log(row.id, row.name);
}

// Close connection
await client.close();
```

## Connection Pooling (High Performance)

For concurrent access and better performance, use the pooled client:

```javascript
const { PooledClient, PoolConfig } = require('boyodb');

// Configure connection pool
const config = new PoolConfig({
  host: 'localhost',
  port: 8765,
  poolSize: 20,
  database: 'analytics',
  queryTimeout: 60000,
});

// Create pooled client
const client = new PooledClient(config);
await client.connect();

// Thread-safe concurrent queries
const result = await client.query('SELECT COUNT(*) FROM events');
console.log(result.rows);

// Multiple concurrent queries
const promises = [];
for (let i = 0; i < 100; i++) {
  promises.push(client.query(`SELECT * FROM events WHERE id = ${i}`));
}
const results = await Promise.all(promises);

await client.close();
```

### Pool Configuration

```javascript
const config = new PoolConfig({
  host: 'localhost',         // Server host
  port: 8765,                // Server port
  poolSize: 20,              // Connections in pool
  poolTimeout: 30000,        // Timeout waiting for connection (ms)
  tls: true,                 // Enable TLS
  caFile: '/path/to/ca.pem', // CA certificate
  token: 'auth-token',       // Authentication token
  database: 'mydb',          // Default database
  queryTimeout: 60000,       // Query timeout (ms)
  maxRetries: 3,             // Connection retries
});
```

## Configuration

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765', {
  tls: true,
  caFile: '/path/to/ca.pem',
  token: 'your-auth-token',
  database: 'mydb',
  queryTimeout: 60000,
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

// Ingest CSV data
const csvData = `id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
`;

await client.ingestCSV('mydb', 'users', csvData, { hasHeader: true });

await client.close();
```

## Transactions

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Simple transaction
await client.begin();
try {
  await client.exec('INSERT INTO accounts (id, balance) VALUES (1, 1000)');
  await client.exec('INSERT INTO accounts (id, balance) VALUES (2, 500)');
  await client.commit();
} catch (err) {
  await client.rollback();
  throw err;
}

// Using transaction helper
const result = await client.inTransaction(async () => {
  await client.exec("INSERT INTO logs (msg) VALUES ('started')");
  const r = await client.query('SELECT COUNT(*) as c FROM logs');
  return r.rows[0].c;
});

// Savepoints
await client.begin();
await client.exec('INSERT INTO orders (id) VALUES (1)');
await client.savepoint('sp1');
await client.exec('INSERT INTO items (order_id) VALUES (1)');
await client.rollback('sp1');  // Undo items insert
await client.commit();  // Commits only orders insert

await client.close();
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

try {
  const client = new Client('localhost:8765');
  await client.connect();

  const result = await client.query('SELECT * FROM nonexistent_table');
} catch (err) {
  console.error('Error:', err.message);
}
```

## API Reference

### Client

- `new Client(host, config)` - Create a new client
- `connect()` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health
- `login(username, password)` - Login with credentials
- `logout()` - Logout from server
- `query(sql, options)` - Execute query
- `exec(sql, options)` - Execute statement
- `prepare(sql, options)` - Prepare statement
- `executePreparedBinary(preparedId, options)` - Execute prepared statement
- `createDatabase(name)` - Create database
- `createTable(database, table)` - Create table
- `listDatabases()` - List all databases
- `listTables(database)` - List tables
- `explain(sql)` - Get query plan
- `metrics()` - Get server metrics
- `ingestCSV(database, table, csvData, options)` - Ingest CSV
- `ingestIPC(database, table, ipcData)` - Ingest Arrow IPC
- `begin(options)` - Start transaction
- `commit()` - Commit transaction
- `rollback(savepoint)` - Rollback transaction
- `savepoint(name)` - Create savepoint
- `releaseSavepoint(name)` - Release savepoint
- `inTransaction(fn, options)` - Execute in transaction

### PooledClient

- `new PooledClient(config)` - Create pooled client
- `connect()` - Initialize pool
- `query(sql, options)` - Execute query
- `exec(sql, options)` - Execute statement
- `login(username, password)` - Login
- `logout()` - Logout
- `close()` - Close pool

### QueryResult

- `rows` - Array of row objects
- `columns` - Column names
- `rowCount` - Number of rows
- `segmentsScanned` - Segments scanned
- `dataSkippedBytes` - Bytes skipped by pruning

## Requirements

- Node.js 14 or higher
- apache-arrow package

## License

Apache-2.0
