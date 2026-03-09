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
9. [Data Integrity & Corruption Prevention](#data-integrity--corruption-prevention)
10. [Performance Tuning](#performance-tuning)
11. [Monitoring & Observability](#monitoring--observability)

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
git clone https://github.com/Izi-Technologies/boyodb.git
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
CREATE INDEX idx_users_id ON mydb.users USING HASH (id);

-- Create fulltext index (for LIKE '%pattern%' searches)
CREATE INDEX idx_phone ON mydb.users USING FULLTEXT (phone_number);

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

### User-Defined Functions

Create custom scalar functions for data transformations:

```sql
-- Create a function
CREATE FUNCTION calculate_tax(amount FLOAT64, rate FLOAT64)
RETURNS FLOAT64
AS amount * rate;

-- Use the function
SELECT product, price, calculate_tax(price, 0.08) as tax
FROM products;

-- List all functions
SHOW FUNCTIONS;

-- Drop a function
DROP FUNCTION calculate_tax;
```

**Supported operations in function body:**
- Arithmetic: `+`, `-`, `*`, `/`
- String: `CONCAT()`, `UPPER()`, `LOWER()`
- Null handling: `COALESCE()`, `NULLIF()`
- Conditional: `IF(condition, then, else)`

### Stream Connectors

Ingest data from streaming platforms:

```sql
-- Create a Kafka stream
CREATE STREAM user_events
FROM KAFKA 'bootstrap.servers=kafka:9092;topic=events;group.id=boyodb'
INTO analytics.events
FORMAT json;

-- Start consuming
START STREAM user_events;

-- Check status
SHOW STREAM STATUS user_events;

-- Stop stream
STOP STREAM user_events;

-- List all streams
SHOW STREAMS;

-- Remove stream
DROP STREAM user_events;
```

**Supported sources:**
- Kafka
- Pulsar

**Supported formats:**
- JSON
- CSV

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

### Multi-Region Replication

BoyoDB supports cross-region replication for global deployments:

```bash
# Primary region (US-East)
boyodb-server /data 0.0.0.0:8765 \
    --cluster \
    --cluster-id global-app \
    --region us-east \
    --region-mode primary \
    --replicate-to us-west,eu-west

# Secondary region (US-West)
boyodb-server /data 0.0.0.0:8765 \
    --cluster \
    --cluster-id global-app \
    --region us-west \
    --region-mode secondary \
    --primary-region us-east

# Secondary region (EU-West)
boyodb-server /data 0.0.0.0:8765 \
    --cluster \
    --cluster-id global-app \
    --region eu-west \
    --region-mode secondary \
    --primary-region us-east
```

#### Region Status and Monitoring

```sql
-- Check region status
SHOW REGION STATUS;
-- +----------+----------+-----------+--------+
-- | region   | mode     | lag_ms    | status |
-- +----------+----------+-----------+--------+
-- | us-east  | primary  | 0         | active |
-- | us-west  | secondary| 45        | active |
-- | eu-west  | secondary| 120       | active |
-- +----------+----------+-----------+--------+

-- View replication details
SELECT * FROM system.region_replication;
```

#### Conflict Resolution

When concurrent writes occur in multiple regions:

```sql
-- Configure conflict resolution strategy
SET conflict_resolution = 'last_writer_wins';

-- Available strategies:
-- 'last_writer_wins'  - Latest timestamp wins (default)
-- 'first_writer_wins' - First write preserved
-- 'region_priority'   - Primary region wins conflicts
-- 'custom'            - User-defined resolution function

-- View recent conflicts
SELECT * FROM system.replication_conflicts
ORDER BY detected_at DESC
LIMIT 10;
```

#### Region Options

| Option | Description |
|--------|-------------|
| `--region <name>` | Region identifier |
| `--region-mode <mode>` | `primary` or `secondary` |
| `--primary-region <name>` | Primary region to replicate from |
| `--replicate-to <regions>` | Comma-separated target regions |
| `--region-write-concern` | `local`, `majority`, or `all` |

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

### Per-Tenant Resource Quotas

For multi-tenant deployments, enforce resource limits per tenant:

```sql
-- Create a quota definition
CREATE QUOTA enterprise_tier
    MAX_CPU_TIME_MS = 60000       -- 60 seconds CPU per query
    MAX_MEMORY_MB = 8192          -- 8GB memory per query
    MAX_CONCURRENT_QUERIES = 20   -- 20 concurrent queries
    MAX_QPM = 5000;               -- 5000 queries per minute

CREATE QUOTA free_tier
    MAX_CPU_TIME_MS = 5000
    MAX_MEMORY_MB = 512
    MAX_CONCURRENT_QUERIES = 2
    MAX_QPM = 100;

-- Assign quota to user
ALTER USER alice SET QUOTA enterprise_tier;
ALTER USER bob SET QUOTA free_tier;

-- View quota definitions
SHOW QUOTAS;

-- View current usage per tenant
SELECT tenant, cpu_used_ms, memory_used_mb,
       concurrent_queries, queries_this_minute
FROM system.tenant_usage;
```

#### Quota Enforcement

When a quota is exceeded:

| Limit | Behavior |
|-------|----------|
| `MAX_CPU_TIME_MS` | Query is cancelled after limit |
| `MAX_MEMORY_MB` | Query fails with out-of-memory error |
| `MAX_CONCURRENT_QUERIES` | New queries wait in queue |
| `MAX_QPM` | New queries are rejected (429) |

#### Fair Scheduling

BoyoDB implements fair scheduling across tenants:

```sql
-- View scheduling statistics
SELECT tenant, queries_executed, total_wait_ms, avg_wait_ms
FROM system.tenant_scheduling
ORDER BY queries_executed DESC;
```

This ensures no single tenant can monopolize cluster resources, even under heavy load.
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

## Data Integrity & Corruption Prevention

BoyoDB includes multiple layers of protection against data corruption, ensuring your data remains safe even under hardware failures, power outages, or filesystem issues.

### Checksums

BoyoDB uses xxHash64 checksums to detect data corruption at multiple levels:

**Segment Checksums:**
Every data segment is checksummed when written and verified when read:
```sql
-- Verify all segments in a table
SELECT COUNT(*) FROM analytics.events;  -- Automatically verifies segments

-- Check for corrupted segments
SHOW CORRUPTED SEGMENTS FROM analytics.events;
```

**WAL Checksums:**
Every WAL record includes a checksum for corruption detection:
```sql
-- Check WAL health
SHOW WAL STATUS;
```

**Manifest Checksums:**
The manifest file (which tracks all segments) includes a checksum to detect corruption during reads.

### Write Verification

BoyoDB can verify writes by reading data back after writing and comparing checksums. This catches silent data corruption caused by faulty disks or filesystem bugs.

```bash
# Enable write verification (default: on)
boyodb-server /data 0.0.0.0:8765 --verify-writes
```

### Disk Space Checking

Pre-flight checks prevent writes when disk space is low, avoiding partial writes:

```bash
# Set minimum free disk space (default: 1GB)
boyodb-server /data 0.0.0.0:8765 --min-free-disk-bytes 5368709120  # 5GB
```

### Manifest Backups

Multiple manifest backups are maintained for recovery:

```bash
# Keep 5 manifest backups (default: 3)
boyodb-server /data 0.0.0.0:8765 --manifest-backup-count 5
```

### VACUUM FORCE

When segments are corrupted or missing, normal VACUUM fails. Use VACUUM FORCE to compact despite missing segments:

```sql
-- Normal vacuum (fails on missing segments)
VACUUM analytics.events;

-- Force vacuum (skips missing segments)
VACUUM FORCE analytics.events;

-- Full vacuum with force
VACUUM FULL FORCE analytics.events;
```

### WAL Integrity

WAL records include checksums and can be repaired if corrupted:

```sql
-- Check WAL health
SHOW WAL STATUS;

-- View corrupted records (if any)
SHOW WAL ERRORS;
```

**WAL Configuration:**
```bash
# Force sync after every WAL write (safest, slower)
boyodb-server /data 0.0.0.0:8765 --wal-sync-every-write

# Standard sync interval (default)
boyodb-server /data 0.0.0.0:8765 --wal-sync-interval-ms 100
```

### Repairing Corrupted Data

```sql
-- Find missing/corrupted segments
SHOW MISSING SEGMENTS;
SHOW MISSING SEGMENTS FROM analytics.events;

-- Repair segments (removes corrupt entries from manifest)
REPAIR SEGMENTS analytics.events;

-- Repair all tables in database
REPAIR SEGMENTS analytics;
```

### Background Scrubbing

BoyoDB periodically scans segments in the background to detect corruption before it causes query failures:

```bash
# Enable background scrubbing (default: on)
boyodb-server /data 0.0.0.0:8765 --background-scrubbing

# Set scrub interval in seconds (default: 3600 = 1 hour)
boyodb-server /data 0.0.0.0:8765 --scrub-interval-secs 1800
```

### Deep Scrub with IPC Validation

Deep scrub validates not just checksums but also the internal Arrow IPC format:

```bash
# Enable deep scrub with IPC validation
boyodb-server /data 0.0.0.0:8765 \
    --deep-scrub-validate-ipc \
    --validate-schema-on-load
```

Deep scrub detects:
- Corrupted IPC headers or footers
- Invalid Arrow record batches
- Schema mismatches between manifest and segment data
- Truncated or malformed segment files

### Segment Checksum Journal

For critical deployments, enable redundant checksum tracking:

```bash
# Enable checksum journal
boyodb-server /data 0.0.0.0:8765 \
    --segment-checksum-journal \
    --segment-checksum-journal-path /var/lib/boyodb/checksums
```

The checksum journal:
- Maintains independent checksums separate from the manifest
- Enables cross-validation of segment integrity
- Survives manifest corruption
- Can be used for forensic analysis

### Auto-Repair on Corruption

Enable automatic repair when corruption is detected:

```bash
# Enable auto-repair
boyodb-server /data 0.0.0.0:8765 --auto-repair-on-corruption
```

When corruption is detected during scrub:
1. Corrupted segments are logged
2. If auto-repair is enabled, segments are removed from manifest
3. Data can be recovered from replicas or backups
4. Alerts are generated for operator review

### S3 Upload Verification

For cold storage, enable read-back verification:

```bash
# Verify S3 uploads by reading back and comparing checksums
boyodb-server /data 0.0.0.0:8765 --verify-s3-uploads
```

This catches:
- Silent write corruption
- S3 eventual consistency issues
- Network transmission errors

### Atomic File Operations

BoyoDB uses atomic file operations to prevent corruption during writes:

**Temp-File + Rename Pattern:**
All critical files (manifest, WAL LSN, segments) are written using this pattern:
1. Write to temporary file (.tmp)
2. Sync to disk (fsync)
3. Atomic rename to final location
4. Sync parent directory

This ensures that files are either fully written or not present at all - no partial writes.

**Protected Operations:**
- **Manifest updates**: Never truncate live manifest; always atomic rename
- **WAL LSN persistence**: Atomic temp file + rename with directory creation
- **Segment persistence**: Write-verify-rename pattern with checksum validation
- **S3 cold tier uploads**: Upload, verify via read-back, then update manifest

**Benefits:**
- No partial files after crash or power failure
- Manifest is always consistent
- WAL LSN recovery is reliable
- No orphaned or corrupt segments

### Retry Configuration

Configure retry behavior for transient failures:

```bash
# Set retry parameters
boyodb-server /data 0.0.0.0:8765 \
    --segment-operation-max-retries 5 \
    --segment-operation-retry-delay-ms 500
```

### Best Practices for Data Integrity

1. **Use ECC RAM**: Prevents memory bit-flips from corrupting data
2. **Enable write verification**: Catches silent disk corruption
3. **Keep manifest backups**: Allows recovery from manifest corruption
4. **Monitor disk health**: Replace failing disks promptly
5. **Regular backups**: Create periodic backups with `CREATE BACKUP`
6. **Test recovery**: Periodically test `RECOVER TO TIMESTAMP` works

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
CREATE INDEX idx_user_id ON analytics.events USING HASH (user_id);

-- B-tree index for range queries
CREATE INDEX idx_timestamp ON analytics.events USING BTREE (timestamp);

-- Bloom filter for existence checks
CREATE INDEX idx_email ON analytics.users USING BLOOM (email);

-- Fulltext index for substring searches (LIKE '%pattern%')
CREATE INDEX idx_phone ON telecom.cdr USING FULLTEXT (calling_number);

-- Composite index for common query patterns
CREATE INDEX idx_user_time ON analytics.events (user_id, timestamp);
```

**Index Types:**

| Type | Best For | Example Query |
|------|----------|---------------|
| `BTREE` | Range queries, ORDER BY | `WHERE timestamp > '2024-01-01'` |
| `HASH` | Equality lookups | `WHERE user_id = 123` |
| `BLOOM` | Existence checks, high-cardinality | `WHERE email = 'user@example.com'` |
| `BITMAP` | Low-cardinality columns | `WHERE status IN ('active', 'pending')` |
| `FULLTEXT` | Substring search | `WHERE phone LIKE '%254712%'` |

### Automatic Index Recommendations

BoyoDB includes an Index Advisor that analyzes query patterns and recommends optimal indexes:

```sql
-- View index recommendations
SHOW INDEX RECOMMENDATIONS;

-- Example output:
-- +----------------+------------+--------+-----------+--------+
-- | table          | columns    | type   | frequency | impact |
-- +----------------+------------+--------+-----------+--------+
-- | analytics.events | user_id  | HASH   | 1523      | 0.85   |
-- | analytics.events | timestamp| BTREE  | 892       | 0.72   |
-- +----------------+------------+--------+-----------+--------+

-- Recommendations are based on:
-- - Columns in WHERE clauses (equality, range, IN)
-- - JOIN columns
-- - ORDER BY and GROUP BY columns
-- - Query frequency and estimated selectivity
```

### Query Store

Enable query performance tracking for historical analysis:

```sql
-- Enable query store
SET query_store = ON;

-- View top queries by total execution time
SELECT query_fingerprint, call_count, total_time_ms, avg_time_ms
FROM system.query_store
ORDER BY total_time_ms DESC
LIMIT 10;

-- Find slow queries
SELECT query_fingerprint, avg_time_ms, rows_examined
FROM system.query_store
WHERE avg_time_ms > 1000
ORDER BY avg_time_ms DESC;

-- Detect plan regressions
SELECT query_fingerprint, baseline_time_ms, latest_time_ms
FROM system.query_store
WHERE plan_changed = true
  AND latest_time_ms > baseline_time_ms * 2;
```

### Adaptive Query Execution

Enable runtime query plan optimization:

```sql
-- Enable adaptive execution
SET adaptive_execution = ON;

-- The optimizer automatically adjusts:
-- - Join algorithms (hash vs. merge) based on actual row counts
-- - Parallelism based on data distribution
-- - Memory budgets based on observed usage
-- - Partition strategies based on data skew

-- View adaptive decisions for a query
EXPLAIN ANALYZE SELECT ...;
-- Output includes adaptive checkpoint decisions
```

### Cost Model Tuning

Calibrate the query optimizer for your hardware:

```sql
-- Run automatic calibration
ANALYZE COST MODEL;

-- View current cost parameters
SHOW COST MODEL;
-- +---------------------+-------+
-- | parameter           | value |
-- +---------------------+-------+
-- | cpu_tuple_cost      | 0.01  |
-- | cpu_operator_cost   | 0.0025|
-- | random_page_cost    | 4.0   |
-- | seq_page_cost       | 1.0   |
-- | effective_cache_size| 4GB   |
-- +---------------------+-------+

-- Manually adjust parameters
SET cpu_tuple_cost = 0.015;
SET random_page_cost = 3.0;
SET effective_cache_size = '8GB';
```

### GPU Acceleration

Enable hardware-accelerated query processing:

```bash
# Start server with GPU support
boyodb-server /data 0.0.0.0:8765 \
    --gpu-acceleration \
    --gpu-memory-limit 8GB \
    --gpu-min-batch-size 10000
```

```sql
-- Check GPU status
SHOW GPU STATUS;
-- +--------+------------------+--------+-----------+
-- | device | name             | memory | available |
-- +--------+------------------+--------+-----------+
-- | 0      | NVIDIA RTX 4090  | 24GB   | true      |
-- +--------+------------------+--------+-----------+

-- Enable/disable per session
SET gpu_acceleration = ON;

-- GPU-accelerated operations:
-- - Aggregations (SUM, COUNT, AVG, MIN, MAX)
-- - Filter evaluation
-- - Hash joins
-- - Vector similarity search
```

Supported platforms:
- **NVIDIA CUDA**: Linux, Windows
- **Apple Metal**: macOS (M1/M2/M3)
- Automatic CPU fallback when GPU unavailable

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
CREATE INDEX idx_user ON analytics.events USING HASH (user_id);

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

**Missing segments:**
```sql
-- Check server info
SHOW SERVER INFO;

-- Find missing segments
SHOW MISSING SEGMENTS;
SHOW MISSING SEGMENTS FROM mydb.events;

-- Repair missing segments (removes from manifest)
REPAIR SEGMENTS mydb.events;
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

- [GitHub Repository](https://github.com/Izi-Technologies/boyodb)
- [Issue Tracker](https://github.com/Izi-Technologies/boyodb/issues)
- [API Documentation](./API.md)

---

*BoyoDB - High-performance analytical database*
