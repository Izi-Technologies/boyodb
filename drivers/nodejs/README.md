# BoyoDB Node.js Driver

[![Version](https://img.shields.io/badge/version-0.9.8-green.svg)](../../CHANGELOG.md)
[![Node.js](https://img.shields.io/badge/node.js-14+-blue.svg)](https://nodejs.org/)

High-performance Node.js client for BoyoDB analytical database with support for transactions, async inserts, pub/sub, CDC, analytics, and more.

## Features

- Connection pooling for concurrent access
- Binary protocol with Arrow IPC support
- TLS encryption with certificate verification
- ACID transactions with savepoints and isolation levels
- Async insert buffering for high-throughput ingestion
- Pub/Sub messaging and CDC (Change Data Capture)
- Approximate analytics (HyperLogLog, T-Digest)
- Time series and graph analytics
- External tables (S3, URL, HDFS, File)
- Vector similarity search

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

## Connection Pooling

For concurrent access and better performance:

```javascript
const { PooledClient, PoolConfig } = require('boyodb');

const config = new PoolConfig({
  host: 'localhost',
  port: 8765,
  poolSize: 20,
  database: 'analytics',
});

const client = new PooledClient(config);
await client.connect();

// Concurrent queries
const promises = [];
for (let i = 0; i < 100; i++) {
  promises.push(client.query(`SELECT * FROM events WHERE id = ${i}`));
}
const results = await Promise.all(promises);

await client.close();
```

## Transactions

ACID transactions with savepoints:

```javascript
const { Client, IsolationLevel } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Manual transaction management
const tx = client.transaction({ isolationLevel: IsolationLevel.SERIALIZABLE });
try {
  await tx.begin();
  await tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
  await tx.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
  await tx.commit();
} catch (err) {
  await tx.rollback();
  throw err;
}

// Auto-managed transaction
await client.withTransaction(async (tx) => {
  await tx.execute("INSERT INTO orders (id, user_id) VALUES (1, 1)");
  await tx.savepoint('sp1');
  try {
    await tx.execute("INSERT INTO order_items (order_id) VALUES (1)");
  } catch (err) {
    await tx.rollbackTo('sp1');
  }
  // Transaction continues
});

await client.close();
```

## Async Inserts (High-Throughput)

Buffer inserts for batch efficiency:

```javascript
const { Client, AsyncInsertBuffer } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const buffer = new AsyncInsertBuffer(client, 'mydb.events', {
  maxRows: 10000,
  maxBytes: 10 * 1024 * 1024, // 10MB
  maxWaitMs: 200,
  deduplicate: true,
  dedupColumns: ['id'],
});

buffer.start();

// Add rows - automatically batched
for (let i = 0; i < 100000; i++) {
  buffer.insert({ id: i, event: 'click', value: i * 10 });
}

// Flush remaining and stop
await buffer.stop();
console.log(buffer.stats);
// { totalRows: 100000, totalFlushes: 10, ... }
```

## Zero-Copy Arrow IPC Ingestion

With Native Zero-Copy Binary streaming, applications can skip translating objects into JSON completely. By serializing data to an Apache Arrow IPC memory buffer, the Node.js SDK streams raw bytes asynchronously to BoyoDB for the absolute highest ingestion speed.

```javascript
const { Client } = require('boyodb');
const fs = require('fs');

const client = new Client('localhost:8765');
await client.connect();

// rawArrowBuffer should be a Node.js Buffer containing a valid Arrow IPC stream
const rawArrowBuffer = fs.readFileSync('data.ipc');

// Native Zero-Copy ingestion straight to the BoyoDB engine
await client.ingestIPC('mydb', 'users', rawArrowBuffer);

await client.close();
```

## Pub/Sub Messaging

Real-time messaging:

```javascript
const { Client, Subscriber, Publisher } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Subscribe to channels
const subscriber = new Subscriber(client, ['events', 'alerts'], (msg) => {
  console.log(`[${msg.channel}] ${JSON.stringify(msg.payload)}`);
});
subscriber.start();

// Publish messages
const publisher = client.publisher;
await publisher.publish('events', { type: 'user_signup', userId: 123 });
await publisher.publish('alerts', { level: 'warning', message: 'High CPU' });

// Cleanup
subscriber.stop();
```

## CDC (Change Data Capture)

Subscribe to table changes:

```javascript
const { Client, CDCSubscriber, ChangeType } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const cdc = new CDCSubscriber(client, 'mydb.users', (event) => {
  console.log(`${event.changeType}: ${JSON.stringify(event.after)}`);
}, {
  includeBefore: true,
  changeTypes: [ChangeType.INSERT, ChangeType.UPDATE],
});

cdc.start();

// Make changes in another session...
// INSERT INTO mydb.users VALUES (1, 'Alice')
// -> INSERT: { id: 1, name: 'Alice' }

cdc.stop();
```

## Approximate Analytics

Fast approximate aggregations:

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// HyperLogLog distinct count
const approx = client.approximate;
const estimate = await approx.countDistinct('events', 'user_id');
console.log(`Unique users: ~${estimate.estimate} (±${(estimate.relativeError * 100).toFixed(1)}%)`);

// T-Digest percentiles
const percentiles = await approx.percentile('orders', 'amount', [50, 90, 99]);
for (const p of percentiles) {
  console.log(`P${p.quantile}: ${p.value}`);
}

// Top-K items
const topEvents = await approx.topK('events', 'event_type', 5);
for (const item of topEvents) {
  console.log(`${item.item}: ${item.count}`);
}
```

## Time Series Analytics

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const ts = client.timeseries;

// Aggregate by time buckets
const hourly = await ts.aggregateByTime(
  'metrics',
  'timestamp',
  'cpu_usage',
  '1 hour',
  'avg'
);

// Moving average
const ma = await ts.movingAverage(
  'metrics',
  'timestamp',
  'value',
  7 // window size
);

// Anomaly detection
const anomalies = await ts.detectAnomalies(
  'metrics',
  'timestamp',
  'value',
  3.0 // threshold stddev
);
```

## Graph Analytics

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const graph = client.graph;

// Shortest path
const path = await graph.shortestPath(
  'edges',
  'from_node',
  'to_node',
  'weight',
  1,  // start
  10  // end
);

// PageRank
const rankings = await graph.pagerank('edges', 'from_node', 'to_node', {
  topK: 10,
  damping: 0.85,
});

// Connected components
const components = await graph.connectedComponents(
  'edges',
  'from_node',
  'to_node'
);
```

## External Tables (S3, URLs, Files)

Query external data without importing:

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const ext = client.external;

// Query S3
const results = await ext.queryS3(
  's3://my-bucket/data/events.parquet',
  'SELECT event_type, COUNT(*) as cnt FROM data GROUP BY event_type'
);

// Query HTTP URL
const csvResults = await ext.queryUrl(
  'https://example.com/data.csv',
  'SELECT * FROM data WHERE value > 100'
);

// Query local file
const fileResults = await ext.queryFile('/data/logs.json', { limit: 1000 });

// Create named external table
await ext.createExternalTable('events_s3', {
  sourceType: 's3',
  location: 'bucket/events/',
  format: 'parquet',
  columns: [
    { name: 'id', type: 'INT64' },
    { name: 'event', type: 'STRING' },
    { name: 'ts', type: 'TIMESTAMP' },
  ],
  partitionColumns: ['date'],
});
```

## Vector Similarity Search

```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

const vec = client.vector;

// Create vector index
await vec.createIndex('documents', 'embedding', {
  distanceMetric: 'cosine',
  m: 16,
  efConstruction: 200,
});

// Search for similar vectors
const queryEmbedding = [0.1, 0.2, 0.3, /* ... */];
const results = await vec.search('documents', 'embedding', queryEmbedding, {
  k: 10,
  where: "category = 'tech'",
});

// Hybrid search (vector + full-text)
const hybridResults = await vec.hybridSearch(
  'documents',
  'embedding',
  'content',
  queryEmbedding,
  'machine learning tutorial',
  {
    k: 10,
    vectorWeight: 0.7,
    textWeight: 0.3,
  }
);
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
  connectTimeout: 10000,
  readTimeout: 30000,
  writeTimeout: 10000,
  maxRetries: 3,
  retryDelay: 1000,
});

await client.connect();
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

#### Connection
- `new Client(host, config)` - Create a new client
- `connect()` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health

#### Authentication
- `login(username, password)` - Login with credentials
- `logout()` - Logout from server

#### Queries
- `query(sql, options)` - Execute query and return results
- `exec(sql, options)` - Execute statement (no results)
- `prepare(sql, options)` - Prepare statement
- `executePreparedBinary(preparedId, options)` - Execute prepared statement
- `explain(sql)` - Get query plan

#### Transactions
- `transaction(options)` - Create transaction wrapper
- `withTransaction(fn, options)` - Execute in auto-managed transaction
- `begin(options)` - Start transaction
- `commit()` - Commit transaction
- `rollback(savepoint)` - Rollback transaction
- `savepoint(name)` - Create savepoint
- `releaseSavepoint(name)` - Release savepoint

#### Database Operations
- `createDatabase(name)` - Create database
- `createTable(database, table)` - Create table
- `listDatabases()` - List all databases
- `listTables(database)` - List tables

#### Data Ingestion
- `ingestCSV(database, table, csvData, options)` - Ingest CSV
- `ingestIPC(database, table, ipcData)` - Ingest Arrow IPC

#### Analytics Properties
- `approximate` - Approximate analytics (HyperLogLog, T-Digest)
- `timeseries` - Time series analytics
- `graph` - Graph analytics
- `external` - External tables helper
- `vector` - Vector search helper
- `publisher` - Pub/Sub publisher

### Transaction

- `begin()` - Start the transaction
- `commit()` - Commit the transaction
- `rollback()` - Rollback the transaction
- `execute(sql, database)` - Execute SQL within transaction
- `savepoint(name)` - Create savepoint
- `rollbackTo(name)` - Rollback to savepoint
- `release(name)` - Release savepoint
- `isActive` - Check if transaction is active

### AsyncInsertBuffer

- `new AsyncInsertBuffer(client, table, config)` - Create buffer
- `start()` - Start the buffer
- `stop()` - Stop and flush remaining
- `insert(row)` - Insert a row
- `insertMany(rows)` - Insert multiple rows
- `flush()` - Force flush
- `stats` - Get statistics
- `bufferSize` - Current buffer size
- `isRunning` - Check if running

### Subscriber / CDCSubscriber

- `start()` - Start subscribing
- `stop()` - Stop subscribing
- `addChannel(channel)` - Add channel (Subscriber only)
- `removeChannel(channel)` - Remove channel (Subscriber only)

### Publisher

- `publish(channel, payload)` - Publish message
- `publishMany(channel, payloads)` - Publish multiple messages

### ApproximateFunctions

- `countDistinct(table, column, options)` - HyperLogLog count
- `percentile(table, column, percentiles, options)` - T-Digest percentiles
- `median(table, column, where)` - Approximate median
- `topK(table, column, k, where)` - Top-K items

### TimeSeriesAnalytics

- `aggregateByTime(table, timeColumn, valueColumn, bucket, aggregation, where)` - Time bucket aggregation
- `movingAverage(table, timeColumn, valueColumn, windowSize, where)` - Moving average
- `detectAnomalies(table, timeColumn, valueColumn, thresholdStddev, where)` - Anomaly detection
- `rateOfChange(table, timeColumn, valueColumn, where)` - Rate of change

### GraphAnalytics

- `shortestPath(edgesTable, sourceColumn, targetColumn, weightColumn, startNode, endNode)` - Shortest path
- `pagerank(edgesTable, sourceColumn, targetColumn, options)` - PageRank
- `connectedComponents(edgesTable, sourceColumn, targetColumn)` - Connected components
- `degrees(edgesTable, sourceColumn, targetColumn)` - Node degrees

### ExternalTables

- `queryS3(path, sql, options)` - Query S3 data
- `queryUrl(url, sql, options)` - Query URL data
- `queryFile(path, sql, options)` - Query local file
- `queryHdfs(path, sql, options)` - Query HDFS data
- `createExternalTable(name, config)` - Create external table
- `dropExternalTable(name)` - Drop external table

### VectorSearch

- `createIndex(table, column, options)` - Create vector index
- `search(table, column, queryVector, options)` - Search similar vectors
- `hybridSearch(table, vectorColumn, textColumn, queryVector, queryText, options)` - Hybrid search
- `dropIndex(indexName)` - Drop vector index

## Requirements

- Node.js 14 or higher
- apache-arrow package

## License

Apache-2.0
