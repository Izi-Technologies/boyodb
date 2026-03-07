# BoyoDB Quick Reference

A cheat sheet for common BoyoDB operations.

---

## Server Commands

```bash
# Start single-node server
boyodb-server /data 0.0.0.0:8765

# Start with authentication
boyodb-server /data 0.0.0.0:8765 --auth

# Start with TLS
boyodb-server /data 0.0.0.0:8765 \
    --tls-cert server.crt --tls-key server.key

# Start cluster node
boyodb-server /data 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node1 \
    --gossip-addr 0.0.0.0:8766

# Join cluster
boyodb-server /data 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node2 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1:8766"
```

---

## CLI Connection

```bash
# Interactive shell
boyodb-cli shell -H localhost:8765

# With authentication
boyodb-cli shell -H localhost:8765 -u admin -P password

# Execute single command
boyodb-cli shell -H localhost:8765 -c "SHOW DATABASES"

# With database
boyodb-cli shell -H localhost:8765 -d mydb -c "SHOW TABLES"
```

---

## Shell Meta-Commands

| Command | Description |
|---------|-------------|
| `\q` | Quit |
| `\l` | List databases |
| `\dt` | List tables |
| `\d table` | Describe table |
| `\du` | List users |
| `\di` | List indexes |
| `\c db` | Connect to database |
| `\x` | Toggle expanded display |
| `\timing` | Toggle timing |
| `\i file` | Execute file |
| `\o file` | Output to file |

---

## Database Operations

```sql
-- Create database
CREATE DATABASE mydb;

-- Drop database
DROP DATABASE IF EXISTS mydb;

-- List databases
SHOW DATABASES;
```

---

## Table Operations

```sql
-- Create table
CREATE TABLE mydb.users (
    id INT64 PRIMARY KEY,
    email STRING NOT NULL,
    name STRING,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create from query
CREATE TABLE mydb.backup AS SELECT * FROM mydb.users;

-- Alter table
ALTER TABLE mydb.users ADD COLUMN phone STRING;
ALTER TABLE mydb.users DROP COLUMN phone;

-- Drop table
DROP TABLE IF EXISTS mydb.users;

-- Describe
DESCRIBE mydb.users;

-- List tables
SHOW TABLES IN mydb;
```

---

## Data Operations

```sql
-- Insert
INSERT INTO mydb.users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice');

-- Multi-row insert
INSERT INTO mydb.users VALUES
    (2, 'bob@example.com', 'Bob'),
    (3, 'carol@example.com', 'Carol');

-- Upsert
INSERT INTO mydb.users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice Smith')
ON CONFLICT (id) DO UPDATE SET name = 'Alice Smith';

-- Update
UPDATE mydb.users SET name = 'Alice Johnson' WHERE id = 1;

-- Delete
DELETE FROM mydb.users WHERE id = 1;

-- Truncate
TRUNCATE TABLE mydb.users;
```

---

## SELECT Queries

```sql
-- Basic select
SELECT * FROM mydb.users WHERE id = 1;

-- With ordering and limit
SELECT * FROM mydb.users
ORDER BY created_at DESC
LIMIT 10 OFFSET 20;

-- Aggregation
SELECT COUNT(*), AVG(age) FROM mydb.users;

-- Group by
SELECT country, COUNT(*) as cnt
FROM mydb.users
GROUP BY country
HAVING COUNT(*) > 10;

-- Join
SELECT u.name, o.total
FROM mydb.users u
JOIN mydb.orders o ON u.id = o.user_id;

-- Window function
SELECT name, salary,
    RANK() OVER (ORDER BY salary DESC) as rank
FROM mydb.employees;

-- CTE
WITH top_users AS (
    SELECT user_id, SUM(total) as spent
    FROM mydb.orders
    GROUP BY user_id
)
SELECT * FROM top_users WHERE spent > 1000;
```

---

## Indexes

```sql
-- Create index
CREATE INDEX idx_email ON mydb.users (email);

-- Hash index (equality lookups)
CREATE INDEX idx_id ON mydb.users (id) USING HASH;

-- Unique index
CREATE UNIQUE INDEX idx_email ON mydb.users (email);

-- Drop index
DROP INDEX idx_email ON mydb.users;

-- Show indexes
SHOW INDEXES IN mydb.users;
```

---

## Views

```sql
-- Create view
CREATE VIEW mydb.active_users AS
SELECT * FROM mydb.users WHERE status = 'active';

-- Materialized view
CREATE MATERIALIZED VIEW mydb.daily_stats AS
SELECT date, COUNT(*) FROM mydb.events GROUP BY date;

-- Refresh
REFRESH MATERIALIZED VIEW mydb.daily_stats;
```

---

## Transactions

```sql
BEGIN;
INSERT INTO mydb.accounts (id, balance) VALUES (1, 100);
UPDATE mydb.accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;

-- Rollback
BEGIN;
DELETE FROM mydb.users WHERE id = 1;
ROLLBACK;

-- Savepoints
BEGIN;
SAVEPOINT sp1;
UPDATE mydb.users SET name = 'test';
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

---

## User Management

```sql
-- Create user
CREATE USER alice WITH PASSWORD 'secret';

-- Alter user
ALTER USER alice SET PASSWORD 'newsecret';

-- Lock/unlock
LOCK USER alice;
UNLOCK USER alice;

-- Drop user
DROP USER alice;

-- List users
SHOW USERS;
```

---

## Roles & Permissions

```sql
-- Create role
CREATE ROLE analyst;

-- Grant to role
GRANT SELECT ON DATABASE mydb TO analyst;
GRANT SELECT, INSERT ON TABLE mydb.reports TO analyst;

-- Grant role to user
GRANT analyst TO alice;

-- Revoke
REVOKE analyst FROM alice;

-- Show grants
SHOW GRANTS FOR alice;
```

---

## Backup & Recovery

```sql
-- Create backup
CREATE BACKUP 'daily-backup';

-- List backups
SHOW BACKUPS;

-- WAL status
SHOW WAL STATUS;

-- Point-in-time recovery
RECOVER TO TIMESTAMP '2024-01-15T14:30:00';
RECOVER TO LSN 1234567890;
```

---

## Maintenance

```sql
-- Vacuum (reclaim space)
VACUUM mydb.users;
VACUUM FULL mydb.users;

-- Analyze (update stats)
ANALYZE TABLE mydb.users;

-- Deduplicate
DEDUPLICATE mydb.events;

-- Check for missing segments
SHOW MISSING SEGMENTS;
SHOW MISSING SEGMENTS FROM mydb.events;

-- Repair missing segments
REPAIR SEGMENTS mydb.events;

-- Server info
SHOW SERVER INFO;
```

---

## User-Defined Functions

```sql
-- Create function
CREATE FUNCTION double_val(x INT64)
RETURNS INT64
AS x * 2;

-- Use function
SELECT double_val(amount) FROM orders;

-- List functions
SHOW FUNCTIONS;

-- Drop function
DROP FUNCTION double_val;
```

---

## Stream Connectors

```sql
-- Create Kafka stream
CREATE STREAM events_stream
FROM KAFKA 'bootstrap.servers=localhost:9092;topic=events'
INTO analytics.events
FORMAT json;

-- Manage streams
START STREAM events_stream;
STOP STREAM events_stream;
SHOW STREAM STATUS events_stream;

-- List streams
SHOW STREAMS;

-- Drop stream
DROP STREAM events_stream;
```

---

## Data Import/Export

```sql
-- Import CSV
COPY mydb.events FROM '/path/to/file.csv'
WITH (FORMAT CSV, HEADER true);

-- Import JSON
COPY mydb.events FROM '/path/to/file.json'
WITH (FORMAT JSON);

-- Export
COPY mydb.events TO '/path/to/export.csv'
WITH (FORMAT CSV, HEADER true);
```

```bash
# CLI import
boyodb-cli import -H localhost:8765 \
    --table mydb.events \
    --input data.csv \
    --format csv

# CLI export
boyodb-cli export -H localhost:8765 \
    --query "SELECT * FROM mydb.events" \
    --output data.parquet \
    --format parquet
```

---

## Monitoring

```bash
# Cluster status
boyodb-cli cluster-status -H localhost:8765

# Metrics
boyodb-cli metrics -H localhost:8765

# Info
boyodb-cli info -H localhost:8765
```

```sql
-- Query plan
EXPLAIN SELECT * FROM mydb.users WHERE id = 1;

-- With execution stats
EXPLAIN ANALYZE SELECT * FROM mydb.users WHERE id = 1;
```

---

## Common Functions

**String:**
```sql
UPPER('hello')          -- 'HELLO'
LOWER('HELLO')          -- 'hello'
LENGTH('hello')         -- 5
CONCAT('a', 'b', 'c')   -- 'abc'
SUBSTRING('hello', 2, 3) -- 'ell'
TRIM('  hello  ')       -- 'hello'
```

**Math:**
```sql
ABS(-5)                 -- 5
ROUND(3.14159, 2)       -- 3.14
CEIL(3.2)               -- 4
FLOOR(3.8)              -- 3
POWER(2, 10)            -- 1024
```

**Date/Time:**
```sql
NOW()                   -- Current timestamp
CURRENT_DATE()          -- Current date
DATE_TRUNC('day', ts)   -- Truncate to day
EXTRACT(YEAR FROM ts)   -- Extract year
DATE_ADD(ts, 7, 'day')  -- Add 7 days
```

**JSON:**
```sql
JSON_EXTRACT(j, '$.key')        -- Extract value
JSON_OBJECT('a', 1, 'b', 2)     -- Create object
JSON_ARRAY(1, 2, 3)             -- Create array
```

**Aggregates:**
```sql
COUNT(*)                -- Count rows
COUNT(DISTINCT col)     -- Distinct count
SUM(col)                -- Sum
AVG(col)                -- Average
MIN(col), MAX(col)      -- Min/max
APPROX_COUNT_DISTINCT(col) -- HLL estimate
```

---

## Data Types

| Type | Example |
|------|---------|
| `INT64` | `42` |
| `FLOAT64` | `3.14` |
| `DECIMAL(10,2)` | `123.45` |
| `BOOLEAN` | `true` |
| `STRING` | `'hello'` |
| `DATE` | `'2024-01-15'` |
| `TIMESTAMP` | `'2024-01-15T14:30:00'` |
| `UUID` | `uuid()` |
| `JSON` | `'{"key": "value"}'` |
| `ARRAY<INT64>` | `ARRAY(1, 2, 3)` |

---

## Configuration File (~/.boyodbrc)

```toml
host = "localhost:8765"
user = "admin"
database = "mydb"
tls = false
format = "table"
timeout_ms = 30000
```

---

*Full documentation: [USER_GUIDE.md](USER_GUIDE.md) | [CLUSTERING.md](CLUSTERING.md)*
