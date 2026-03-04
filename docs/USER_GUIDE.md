# BoyoDB User Guide

A comprehensive guide to installing, configuring, and using BoyoDB - a high-performance analytical database.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [Server Configuration](#server-configuration)
4. [CLI Usage](#cli-usage)
5. [SQL Reference](#sql-reference)
6. [Clustering & High Availability](#clustering--high-availability)
7. [Security & Authentication](#security--authentication)
8. [Backup & Recovery](#backup--recovery)
9. [Performance Tuning](#performance-tuning)
10. [Monitoring & Observability](#monitoring--observability)

---

## Quick Start

### Start a Server

```bash
# Build the project
cargo build --release

# Start a single-node server
./target/release/boyodb-server /var/lib/boyodb 0.0.0.0:8765
```

### Connect with CLI

```bash
# Interactive shell
./target/release/boyodb-cli shell --host localhost:8765

# Or run a single command
./target/release/boyodb-cli shell -H localhost:8765 -c "SHOW DATABASES"
```

### Create Your First Database

```sql
-- Create a database
CREATE DATABASE analytics;

-- Create a table
CREATE TABLE analytics.events (
    event_id INT64,
    timestamp TIMESTAMP,
    user_id INT64,
    event_type STRING,
    properties JSON
);

-- Insert data
INSERT INTO analytics.events VALUES
    (1, NOW(), 100, 'page_view', '{"page": "/home"}'),
    (2, NOW(), 101, 'click', '{"button": "signup"}'),
    (3, NOW(), 100, 'purchase', '{"amount": 99.99}');

-- Query data
SELECT user_id, COUNT(*) as event_count
FROM analytics.events
GROUP BY user_id
ORDER BY event_count DESC;
```

---

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/boyodb.git
cd boyodb

# Build release binaries
cargo build --release

# Binaries are located at:
# ./target/release/boyodb-server
# ./target/release/boyodb-cli
```

### Directory Structure

```
/var/lib/boyodb/           # Data directory (configurable)
├── wal/                   # Write-ahead log
├── segments/              # Data segments
├── indexes/               # Index files
└── backups/               # Backup files
```

---

## Server Configuration

### Basic Server Startup

```bash
boyodb-server <data_dir> [bind_addr] [options]
```

### Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--max-connections <n>` | Maximum concurrent connections | 64 |
| `--workers <n>` | Number of worker threads | CPU cores |
| `--io-timeout-ms <ms>` | I/O operation timeout | 30000 |
| `--idle-timeout <secs>` | Idle connection timeout | 300 |
| `--log-requests` | Log all incoming requests | disabled |

### Storage & WAL Options

| Option | Description | Default |
|--------|-------------|---------|
| `--wal-dir <path>` | Write-ahead log directory | `<data_dir>/wal` |
| `--wal-max-bytes <bytes>` | Maximum WAL file size | 256MB |
| `--wal-max-segments <n>` | Maximum WAL segments | 4 |
| `--wal-sync-interval-ms <ms>` | WAL sync interval | 100 |

### Caching Options

| Option | Description | Default |
|--------|-------------|---------|
| `--query-cache-bytes <bytes>` | Query result cache size | 64MB |
| `--plan-cache-size <n>` | Query plan cache entries | 1000 |
| `--segment-cache <n>` | Segment cache capacity | 100 |
| `--schema-cache-entries <n>` | Schema metadata cache | 500 |

### Example: Production Server

```bash
boyodb-server /var/lib/boyodb 0.0.0.0:8765 \
    --max-connections 256 \
    --workers 16 \
    --wal-max-bytes 536870912 \
    --query-cache-bytes 134217728 \
    --segment-cache 200 \
    --log-requests
```

---

## CLI Usage

### Connection Options

```bash
boyodb-cli shell [options]
```

| Option | Description |
|--------|-------------|
| `-H, --host <addr:port>` | Server address (default: localhost:8765) |
| `-u, --user <username>` | Username for authentication |
| `-P, --password <password>` | Password for authentication |
| `--token <token>` | Authentication token |
| `-d, --database <name>` | Default database |
| `-c, --command <sql>` | Execute command and exit |
| `--tls` | Enable TLS |
| `--tls-ca <path>` | CA certificate path |

### Configuration File (~/.boyodbrc)

Create a config file for persistent settings:

```toml
host = "localhost:8765"
user = "admin"
database = "analytics"
tls = false
format = "table"
timeout_ms = 30000
```

### Shell Meta-Commands

| Command | Description |
|---------|-------------|
| `\q`, `\quit` | Exit shell |
| `\h`, `\help` | Show help |
| `\l` | List databases |
| `\dt` | List tables in current database |
| `\d <table>` | Describe table structure |
| `\du` | List users |
| `\di` | List indexes |
| `\c <database>` | Connect to database |
| `\conninfo` | Show connection info |
| `\x` | Toggle expanded display |
| `\timing` | Toggle query timing |
| `\i <file>` | Execute commands from file |
| `\o <file>` | Send output to file |
| `\! <cmd>` | Execute shell command |

### Non-Interactive Commands

```bash
# Execute a query
boyodb-cli shell -H localhost:8765 -c "SELECT * FROM mydb.users LIMIT 10"

# Import data
boyodb-cli import --host localhost:8765 \
    --table mydb.events \
    --input data.csv \
    --format csv

# Export data
boyodb-cli export --host localhost:8765 \
    --query "SELECT * FROM mydb.events WHERE date > '2024-01-01'" \
    --output results.parquet \
    --format parquet

# Check cluster status
boyodb-cli cluster-status --host localhost:8765

# View metrics
boyodb-cli metrics --host localhost:8765 --format json
```

---

## SQL Reference

### Data Types

| Type | Description | Example |
|------|-------------|---------|
| `INT8`, `INT16`, `INT32`, `INT64` | Signed integers | `42` |
| `UINT8`, `UINT16`, `UINT32`, `UINT64` | Unsigned integers | `255` |
| `FLOAT32`, `FLOAT64` | Floating point | `3.14159` |
| `DECIMAL(p, s)` | Fixed-point decimal | `123.45` |
| `BOOLEAN` | True/false | `true` |
| `STRING`, `VARCHAR`, `TEXT` | UTF-8 strings | `'hello'` |
| `DATE` | Calendar date | `'2024-01-15'` |
| `TIMESTAMP` | Date and time | `'2024-01-15T14:30:00'` |
| `UUID` | Universally unique ID | `uuid()` |
| `JSON` | JSON documents | `'{"key": "value"}'` |
| `BINARY`, `BLOB` | Binary data | `x'deadbeef'` |
| `ARRAY<T>` | Typed arrays | `ARRAY(1, 2, 3)` |

### Database Operations

```sql
-- Create database
CREATE DATABASE mydb;

-- Drop database
DROP DATABASE IF EXISTS mydb;

-- List databases
SHOW DATABASES;
```

### Table Operations

```sql
-- Create table with constraints
CREATE TABLE mydb.users (
    id INT64 PRIMARY KEY,
    email STRING NOT NULL UNIQUE,
    name STRING,
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSON
);

-- Create table from query
CREATE TABLE mydb.active_users AS
SELECT * FROM mydb.users WHERE last_login > DATE_SUB(NOW(), 30, 'day');

-- Alter table
ALTER TABLE mydb.users ADD COLUMN phone STRING;
ALTER TABLE mydb.users DROP COLUMN phone;
ALTER TABLE mydb.users RENAME COLUMN name TO full_name;

-- Drop table
DROP TABLE IF EXISTS mydb.users;

-- Describe table
DESCRIBE mydb.users;

-- List tables
SHOW TABLES IN mydb;
```

### SELECT Queries

```sql
-- Basic select
SELECT id, name, email FROM mydb.users;

-- With filtering
SELECT * FROM mydb.users
WHERE created_at > '2024-01-01'
  AND status = 'active';

-- With aggregation
SELECT
    DATE_TRUNC('day', created_at) as day,
    COUNT(*) as signups,
    COUNT(DISTINCT country) as countries
FROM mydb.users
GROUP BY DATE_TRUNC('day', created_at)
HAVING COUNT(*) > 100
ORDER BY day DESC
LIMIT 30;

-- With joins
SELECT
    u.name,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM mydb.users u
LEFT JOIN mydb.orders o ON u.id = o.user_id
GROUP BY u.id, u.name
ORDER BY total_spent DESC;

-- Window functions
SELECT
    name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM mydb.employees;

-- Common Table Expressions (CTEs)
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date) as month,
        SUM(total) as revenue
    FROM mydb.orders
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month,
    revenue - LAG(revenue) OVER (ORDER BY month) as growth
FROM monthly_sales;

-- Subqueries
SELECT * FROM mydb.users
WHERE id IN (
    SELECT user_id FROM mydb.orders
    WHERE total > 1000
);

-- Table sampling
SELECT * FROM mydb.large_table
TABLESAMPLE BERNOULLI(1.0) SEED 42;
```

### INSERT Operations

```sql
-- Single insert
INSERT INTO mydb.users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice');

-- Multi-row insert
INSERT INTO mydb.users (id, email, name) VALUES
    (2, 'bob@example.com', 'Bob'),
    (3, 'carol@example.com', 'Carol'),
    (4, 'dave@example.com', 'Dave');

-- Upsert (insert or update)
INSERT INTO mydb.users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice Smith')
ON CONFLICT (id) DO UPDATE SET name = 'Alice Smith';

-- Insert with returning
INSERT INTO mydb.users (email, name)
VALUES ('eve@example.com', 'Eve')
RETURNING id, email;
```

### UPDATE and DELETE

```sql
-- Update
UPDATE mydb.users
SET name = 'Alice Johnson', updated_at = NOW()
WHERE id = 1;

-- Delete
DELETE FROM mydb.users
WHERE last_login < DATE_SUB(NOW(), 365, 'day');

-- Truncate (fast delete all)
TRUNCATE TABLE mydb.users;
```

### Indexes

```sql
-- Create index
CREATE INDEX idx_users_email ON mydb.users (email);

-- Create unique index
CREATE UNIQUE INDEX idx_users_email_unique ON mydb.users (email);

-- Create hash index (for equality lookups)
CREATE INDEX idx_users_id ON mydb.users (id) USING HASH;

-- Create composite index
CREATE INDEX idx_orders_user_date ON mydb.orders (user_id, order_date);

-- Drop index
DROP INDEX idx_users_email ON mydb.users;

-- Show indexes
SHOW INDEXES IN mydb.users;

-- Analyze table (update statistics)
ANALYZE TABLE mydb.users;
```

### Views

```sql
-- Create view
CREATE VIEW mydb.active_users AS
SELECT * FROM mydb.users WHERE status = 'active';

-- Create or replace view
CREATE OR REPLACE VIEW mydb.active_users AS
SELECT id, name, email FROM mydb.users WHERE status = 'active';

-- Materialized view
CREATE MATERIALIZED VIEW mydb.daily_stats AS
SELECT
    DATE_TRUNC('day', timestamp) as day,
    COUNT(*) as events
FROM mydb.events
GROUP BY DATE_TRUNC('day', timestamp);

-- Refresh materialized view
REFRESH MATERIALIZED VIEW mydb.daily_stats;

-- Drop view
DROP VIEW IF EXISTS mydb.active_users;
```

### Transactions

```sql
-- Start transaction
BEGIN;
-- or
BEGIN TRANSACTION;
-- or with isolation level
BEGIN ISOLATION LEVEL SERIALIZABLE;

-- Savepoints
SAVEPOINT my_savepoint;
ROLLBACK TO SAVEPOINT my_savepoint;
RELEASE SAVEPOINT my_savepoint;

-- Commit
COMMIT;

-- Rollback
ROLLBACK;
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count distinct values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average value |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |
| `STDDEV(column)` | Sample standard deviation |
| `VARIANCE(column)` | Sample variance |
| `APPROX_COUNT_DISTINCT(column)` | Approximate distinct count (HyperLogLog) |

### Scalar Functions

**String Functions:**
```sql
SELECT
    UPPER('hello'),                    -- 'HELLO'
    LOWER('HELLO'),                    -- 'hello'
    LENGTH('hello'),                   -- 5
    TRIM('  hello  '),                 -- 'hello'
    CONCAT('hello', ' ', 'world'),     -- 'hello world'
    SUBSTRING('hello', 2, 3),          -- 'ell'
    REPLACE('hello', 'l', 'L'),        -- 'heLLo'
    LEFT('hello', 2),                  -- 'he'
    RIGHT('hello', 2),                 -- 'lo'
    REVERSE('hello');                  -- 'olleh'
```

**Math Functions:**
```sql
SELECT
    ABS(-5),                           -- 5
    ROUND(3.14159, 2),                 -- 3.14
    CEIL(3.2),                         -- 4
    FLOOR(3.8),                        -- 3
    MOD(10, 3),                        -- 1
    POWER(2, 10),                      -- 1024
    SQRT(16),                          -- 4
    LOG(100, 10),                      -- 2
    GREATEST(1, 5, 3),                 -- 5
    LEAST(1, 5, 3);                    -- 1
```

**Date/Time Functions:**
```sql
SELECT
    NOW(),                             -- Current timestamp
    CURRENT_DATE(),                    -- Current date
    DATE_TRUNC('hour', NOW()),         -- Truncate to hour
    EXTRACT(YEAR FROM NOW()),          -- Extract year
    DATE_ADD(NOW(), 7, 'day'),         -- Add 7 days
    DATE_SUB(NOW(), 1, 'month'),       -- Subtract 1 month
    DATE_DIFF('day', '2024-01-01', '2024-01-15');  -- 14
```

**JSON Functions:**
```sql
SELECT
    JSON_EXTRACT('{"name": "Alice"}', '$.name'),       -- "Alice"
    JSON_EXTRACT_SCALAR('{"age": 30}', '$.age'),       -- '30'
    JSON_OBJECT('name', 'Alice', 'age', 30),           -- {"name":"Alice","age":30}
    JSON_ARRAY(1, 2, 3),                               -- [1,2,3]
    JSON_ARRAY_LENGTH('[1,2,3]'),                      -- 3
    JSON_KEYS('{"a":1,"b":2}');                        -- ["a","b"]
```

### COPY (Bulk Data Loading)

```sql
-- Import from CSV
COPY mydb.events FROM '/path/to/events.csv'
WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Import from JSON
COPY mydb.events FROM '/path/to/events.json'
WITH (FORMAT JSON);

-- Import from Parquet
COPY mydb.events FROM '/path/to/events.parquet'
WITH (FORMAT PARQUET);

-- Import from stdin
COPY mydb.events FROM STDIN WITH (FORMAT CSV, HEADER true);

-- Export to file
COPY mydb.events TO '/path/to/export.csv'
WITH (FORMAT CSV, HEADER true);
```

### EXPLAIN (Query Analysis)

```sql
-- Show query plan
EXPLAIN SELECT * FROM mydb.users WHERE id = 1;

-- Show query plan with execution stats
EXPLAIN ANALYZE SELECT * FROM mydb.users WHERE id = 1;
```

---

## Clustering & High Availability

### Cluster Architecture

BoyoDB supports multi-node clustering with:
- **Gossip Protocol**: SWIM-based membership and failure detection
- **Leader Election**: Raft-lite consensus for leader selection
- **Write Replication**: Quorum-based writes for durability
- **Two-Node Mode**: Simplified HA for paired deployments

### Three-Node Cluster Setup

```bash
# Node 1 (seed node)
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node1 \
    --gossip-addr 0.0.0.0:8766

# Node 2
boyodb-server /data/node2 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node2 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1-ip:8766"

# Node 3
boyodb-server /data/node3 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node3 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1-ip:8766"
```

### Two-Node Mode (Primary/Replica)

For simpler deployments with two nodes:

```bash
# Primary node
boyodb-server /data/primary 0.0.0.0:8765 \
    --cluster \
    --cluster-id myapp \
    --two-node-mode \
    --gossip-addr 0.0.0.0:8766

# Replica node
boyodb-server /data/replica 0.0.0.0:8765 \
    --cluster \
    --cluster-id myapp \
    --two-node-mode \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "primary-ip:8766"
```

Two-node mode features:
- Quorum of 1 (can operate with one node down)
- Automatic failover when primary fails
- Replica promotes to primary automatically

### Cluster Options

| Option | Description |
|--------|-------------|
| `--cluster` | Enable cluster mode |
| `--cluster-id <id>` | Unique cluster identifier |
| `--node-id <id>` | Node identifier (auto-generated if omitted) |
| `--gossip-addr <addr:port>` | UDP address for gossip protocol |
| `--rpc-addr <addr:port>` | TCP address for cluster RPC |
| `--seed-nodes <nodes>` | Comma-separated seed node addresses |
| `--two-node-mode` | Enable two-node mode |

### Checking Cluster Status

```sql
-- From SQL
SHOW CLUSTER STATUS;
```

```bash
# From CLI
boyodb-cli cluster-status --host localhost:8765
```

Example output:
```json
{
  "cluster_id": "production",
  "node_id": "node1",
  "role": "Leader",
  "term": 15,
  "alive_nodes": 3,
  "total_nodes": 3,
  "has_quorum": true,
  "leader_id": "node1"
}
```

### Read Replicas

For scaling reads without full clustering:

```bash
# Primary server
boyodb-server /data/primary 0.0.0.0:8765

# Read replica
boyodb-server /data/replica 0.0.0.0:8765 \
    --replicate-from primary-ip:8765 \
    --replicate-interval-ms 100
```

---

## Security & Authentication

### Token Authentication

Simple token-based authentication:

```bash
# Server
boyodb-server /data 0.0.0.0:8765 --token my-secret-token

# Client
boyodb-cli shell --host localhost:8765 --token my-secret-token
```

### User Authentication

Full user management with roles:

```bash
# Server with auth enabled
boyodb-server /data 0.0.0.0:8765 --auth
```

```sql
-- Create a user
CREATE USER alice WITH PASSWORD 'secure-password';

-- Create user with options
CREATE USER bob WITH PASSWORD 'password'
    WITH DEFAULT DATABASE analytics;

-- Change password
ALTER USER alice SET PASSWORD 'new-password';

-- Lock/unlock user
LOCK USER alice;
UNLOCK USER alice;

-- Drop user
DROP USER alice;

-- List users
SHOW USERS;
```

### Role-Based Access Control

```sql
-- Create a role
CREATE ROLE analyst WITH DESCRIPTION 'Data analysts';

-- Grant privileges to role
GRANT SELECT ON DATABASE analytics TO analyst;
GRANT SELECT, INSERT ON TABLE analytics.reports TO analyst;

-- Grant role to user
GRANT analyst TO alice;

-- Revoke privileges
REVOKE INSERT ON TABLE analytics.reports FROM analyst;

-- Revoke role from user
REVOKE analyst FROM alice;

-- Show grants
SHOW GRANTS FOR alice;
SHOW ROLES;
```

### Privilege Types

| Privilege | Description |
|-----------|-------------|
| `SELECT` | Read data |
| `INSERT` | Insert data |
| `UPDATE` | Update data |
| `DELETE` | Delete data |
| `CREATE` | Create tables/views |
| `DROP` | Drop tables/views |
| `ALTER` | Alter tables |
| `TRUNCATE` | Truncate tables |
| `GRANT` | Grant privileges to others |
| `CREATE_DB` | Create databases |
| `CREATE_USER` | Create users |
| `SUPERUSER` | All privileges |
| `ALL` | All privileges on target |

### TLS Encryption

```bash
# Server with TLS
boyodb-server /data 0.0.0.0:8765 \
    --tls-cert /path/to/server.crt \
    --tls-key /path/to/server.key

# Client with TLS
boyodb-cli shell --host localhost:8765 \
    --tls \
    --tls-ca /path/to/ca.crt
```

### Mutual TLS (mTLS)

```bash
# Server requiring client certificates
boyodb-server /data 0.0.0.0:8765 \
    --tls-cert /path/to/server.crt \
    --tls-key /path/to/server.key \
    --tls-ca /path/to/ca.crt
```

---

## Backup & Recovery

### Creating Backups

```sql
-- Create a labeled backup
CREATE BACKUP 'daily-backup-2024-01-15';

-- List backups
SHOW BACKUPS;
```

### Point-in-Time Recovery (PITR)

```sql
-- Check WAL status
SHOW WAL STATUS;

-- Recover to specific timestamp
RECOVER TO TIMESTAMP '2024-01-15T14:30:00';

-- Recover to specific LSN
RECOVER TO LSN 1234567890;
```

### Managing Backups

```sql
-- Delete old backup
DELETE BACKUP 'old-backup-id';
```

### CLI Backup Commands

```bash
# Create backup
boyodb-cli create-backup --host localhost:8765 --label daily

# List backups
boyodb-cli list-backups --host localhost:8765

# Check WAL stats
boyodb-cli wal-stats --host localhost:8765

# Recover to timestamp
boyodb-cli recover-to --host localhost:8765 \
    --timestamp '2024-01-15T14:30:00'
```

---

## Performance Tuning

### Storage Optimization

```sql
-- Compact table segments
VACUUM analytics.events;

-- Full vacuum (reclaims more space)
VACUUM FULL analytics.events;

-- Deduplicate data
DEDUPLICATE analytics.events;

-- Configure deduplication
ALTER TABLE analytics.events
SET DEDUPLICATION (user_id, event_type)
VERSION timestamp
MODE OnCompaction;
```

### Index Strategies

```sql
-- Hash index for equality lookups
CREATE INDEX idx_user_id ON analytics.events (user_id) USING HASH;

-- B-tree index for range queries
CREATE INDEX idx_timestamp ON analytics.events (timestamp) USING BTREE;

-- Bloom filter for existence checks
CREATE INDEX idx_email ON analytics.users (email) USING BLOOM;

-- Composite index for common query patterns
CREATE INDEX idx_user_time ON analytics.events (user_id, timestamp);
```

### Caching Configuration

```bash
boyodb-server /data 0.0.0.0:8765 \
    --query-cache-bytes 268435456 \     # 256MB query cache
    --plan-cache-size 2000 \            # 2000 cached plans
    --segment-cache 500 \               # 500 cached segments
    --batch-cache-bytes 134217728       # 128MB batch cache
```

### Connection Pool Settings

```bash
boyodb-server /data 0.0.0.0:8765 \
    --max-connections 512 \
    --workers 32 \
    --idle-timeout 300 \
    --conn-max-lifetime 3600
```

---

## Monitoring & Observability

### Prometheus Metrics

```bash
# View metrics
boyodb-cli metrics --host localhost:8765

# JSON format
boyodb-cli metrics --host localhost:8765 --format json
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| `queries_total` | Total queries executed |
| `query_latency_seconds` | Query latency histogram |
| `connections_current` | Current active connections |
| `connections_max` | Maximum allowed connections |
| `segments_total` | Total data segments |
| `bytes_ingested_total` | Total bytes ingested |
| `wal_bytes` | WAL size |
| `throttler_admitted_total` | Admitted queries |
| `throttler_rejected_total` | Throttled queries |

### Health Checks

```bash
# Basic health
boyodb-cli info --host localhost:8765

# Detailed health
boyodb-cli shell -H localhost:8765 -c "SELECT 1"
```

### Query Analysis

```sql
-- Show query plan
EXPLAIN SELECT * FROM analytics.events WHERE user_id = 100;

-- Show plan with execution stats
EXPLAIN ANALYZE
SELECT user_id, COUNT(*)
FROM analytics.events
GROUP BY user_id;
```

---

## Example Use Cases

### Analytics Pipeline

```sql
-- Create events table
CREATE TABLE analytics.events (
    event_id INT64,
    timestamp TIMESTAMP,
    user_id INT64,
    event_type STRING,
    properties JSON
);

-- Create hash index for user lookups
CREATE INDEX idx_user ON analytics.events (user_id) USING HASH;

-- Create index for time-range queries
CREATE INDEX idx_time ON analytics.events (timestamp);

-- Daily aggregation view
CREATE MATERIALIZED VIEW analytics.daily_stats AS
SELECT
    DATE_TRUNC('day', timestamp) as day,
    event_type,
    COUNT(*) as count,
    COUNT(DISTINCT user_id) as unique_users
FROM analytics.events
GROUP BY DATE_TRUNC('day', timestamp), event_type;

-- Refresh daily
REFRESH MATERIALIZED VIEW analytics.daily_stats;
```

### Time Series Data

```sql
-- Create metrics table
CREATE TABLE metrics.cpu (
    host STRING,
    timestamp TIMESTAMP,
    cpu_percent FLOAT64,
    memory_percent FLOAT64
);

-- Query last hour with downsampling
SELECT
    host,
    DATE_TRUNC('minute', timestamp) as minute,
    AVG(cpu_percent) as avg_cpu,
    MAX(cpu_percent) as max_cpu
FROM metrics.cpu
WHERE timestamp > DATE_SUB(NOW(), 1, 'hour')
GROUP BY host, DATE_TRUNC('minute', timestamp)
ORDER BY minute DESC;
```

### E-Commerce Example

```sql
-- Products table
CREATE TABLE shop.products (
    id INT64 PRIMARY KEY,
    name STRING NOT NULL,
    category STRING,
    price DECIMAL(10, 2),
    inventory INT32
);

-- Orders table
CREATE TABLE shop.orders (
    id INT64 PRIMARY KEY,
    user_id INT64,
    product_id INT64,
    quantity INT32,
    total DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Top selling products
SELECT
    p.name,
    p.category,
    SUM(o.quantity) as units_sold,
    SUM(o.total) as revenue
FROM shop.orders o
JOIN shop.products p ON o.product_id = p.id
WHERE o.created_at > DATE_SUB(NOW(), 30, 'day')
GROUP BY p.id, p.name, p.category
ORDER BY revenue DESC
LIMIT 10;
```

---

## Troubleshooting

### Common Issues

**Connection refused:**
```bash
# Check server is running
ps aux | grep boyodb-server

# Check port is listening
netstat -an | grep 8765
```

**Authentication failed:**
```bash
# Verify token
boyodb-cli shell --host localhost:8765 --token your-token

# Check user credentials
boyodb-cli shell --host localhost:8765 -u admin -P password
```

**Query timeout:**
```sql
-- Check for long-running queries
SHOW PROCESSLIST;

-- Increase timeout
SET query_timeout = 60000;  -- 60 seconds
```

**Disk space:**
```bash
# Check WAL size
boyodb-cli wal-stats --host localhost:8765

# Run vacuum to reclaim space
boyodb-cli shell -H localhost:8765 -c "VACUUM FULL mydb.large_table"
```

### Logs

Server logs are written to stderr by default. Redirect to a file:

```bash
boyodb-server /data 0.0.0.0:8765 2>&1 | tee /var/log/boyodb.log
```

Enable request logging for debugging:

```bash
boyodb-server /data 0.0.0.0:8765 --log-requests
```

---

## Additional Resources

- [GitHub Repository](https://github.com/your-org/boyodb)
- [Issue Tracker](https://github.com/your-org/boyodb/issues)
- [API Documentation](./API.md)

---

*BoyoDB - High-performance analytical database*
