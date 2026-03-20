# SQL Reference

Complete SQL reference for BoyoDB.

## Table of Contents

- [Data Types](#data-types)
- [DDL Statements](#ddl-statements)
- [DML Statements](#dml-statements)
- [Transaction Control](#transaction-control)
- [Query Statements](#query-statements)
- [Operators](#operators)
- [Functions](#functions)
- [JSON Functions](#json-functions)
- [Window Functions](#window-functions)
- [Common Table Expressions](#common-table-expressions)
- [Prepared Statements](#prepared-statements)
- [Views](#views)
- [Materialized Views](#materialized-views)
- [Foreign Data Wrappers](#foreign-data-wrappers)
- [Recovery Statements](#recovery-statements)
- [Analytical Functions](#analytical-functions)

---

## Data Types

### Numeric Types

| Type | Aliases | Size | Range |
|------|---------|------|-------|
| `INT8` | `tinyint` | 1 byte | -128 to 127 |
| `INT16` | `smallint` | 2 bytes | -32,768 to 32,767 |
| `INT32` | `int32` | 4 bytes | -2B to 2B |
| `INT64` | `integer`, `bigint`, `int` | 8 bytes | -9E18 to 9E18 |
| `UINT8` | | 1 byte | 0 to 255 |
| `UINT16` | | 2 bytes | 0 to 65,535 |
| `UINT32` | | 4 bytes | 0 to 4B |
| `UINT64` | | 8 bytes | 0 to 18E18 |
| `FLOAT32` | `float32` | 4 bytes | IEEE 754 single |
| `FLOAT64` | `double`, `float`, `real` | 8 bytes | IEEE 754 double |
| `DECIMAL(p,s)` | `numeric` | 16 bytes | Precision p, scale s |

### String Types

| Type | Aliases | Description |
|------|---------|-------------|
| `STRING` | `varchar`, `text`, `utf8` | Variable-length UTF-8 |

### Binary Types

| Type | Aliases | Description |
|------|---------|-------------|
| `BINARY` | `bytes`, `blob` | Variable-length binary |

### Date/Time Types

| Type | Aliases | Description |
|------|---------|-------------|
| `DATE` | | Calendar date (days since epoch) |
| `TIMESTAMP` | `datetime` | Microseconds since epoch |

### Special Types

| Type | Description | Storage |
|------|-------------|---------|
| `BOOLEAN` | True/false | 1 bit |
| `UUID` | 128-bit identifier | 16 bytes |
| `JSON` | JSON document | Variable |

---

## DDL Statements

### CREATE DATABASE

```sql
CREATE DATABASE database_name;
```

**Example:**
```sql
CREATE DATABASE analytics;
CREATE DATABASE production;
```

### DROP DATABASE

```sql
DROP DATABASE database_name;
DROP DATABASE IF EXISTS database_name;
```

**Example:**
```sql
DROP DATABASE test_db;
DROP DATABASE IF EXISTS old_data;
```

### CREATE TABLE

```sql
CREATE TABLE [database.]table_name (
    column_name data_type [NOT NULL] [DEFAULT value],
    ...
);
```

**Examples:**
```sql
-- Simple table
CREATE TABLE users (
    id INT64,
    name STRING,
    email STRING
);

-- With database qualifier
CREATE TABLE analytics.events (
    event_id UUID,
    user_id INT64,
    event_type STRING,
    event_time TIMESTAMP,
    metadata JSON
);

-- With NOT NULL constraints
CREATE TABLE orders (
    order_id UUID NOT NULL,
    customer_id INT64 NOT NULL,
    total DECIMAL(12,2),
    created_at TIMESTAMP NOT NULL
);
```

### DROP TABLE

```sql
DROP TABLE [database.]table_name;
DROP TABLE IF EXISTS [database.]table_name;
```

**Example:**
```sql
DROP TABLE analytics.old_events;
DROP TABLE IF EXISTS temp_data;
```

### TRUNCATE TABLE

```sql
TRUNCATE TABLE [database.]table_name;
```

**Example:**
```sql
TRUNCATE TABLE staging.imports;
```

### ALTER TABLE

```sql
-- Add column
ALTER TABLE [database.]table_name ADD COLUMN column_name data_type;

-- Drop column
ALTER TABLE [database.]table_name DROP COLUMN column_name;
```

**Examples:**
```sql
ALTER TABLE users ADD COLUMN phone STRING;
ALTER TABLE users ADD COLUMN verified BOOLEAN;
ALTER TABLE users DROP COLUMN legacy_field;
```

### CREATE INDEX

```sql
CREATE INDEX index_name ON [database.]table_name [USING type] (column1, column2, ...);
CREATE UNIQUE INDEX index_name ON [database.]table_name (column);
```

**Index Types:**

| Type | Description | Best For |
|------|-------------|----------|
| `BTREE` | B-tree index (default) | Range queries, ORDER BY |
| `HASH` | Hash index | Equality lookups |
| `BLOOM` | Bloom filter | Existence checks, high-cardinality columns |
| `BITMAP` | Bitmap index | Low-cardinality columns |
| `FULLTEXT` | N-gram based fulltext index | Substring search (`LIKE '%pattern%'`) |

**Examples:**
```sql
-- B-tree index (default) for range queries
CREATE INDEX idx_timestamp ON events (timestamp);

-- Hash index for equality lookups
CREATE INDEX idx_user_id ON events USING HASH (user_id);

-- Bloom filter for existence checks
CREATE INDEX idx_email ON users USING BLOOM (email);

-- Fulltext index for substring searches
CREATE INDEX idx_phone ON cdr USING FULLTEXT (calling_number);

-- Unique constraint
CREATE UNIQUE INDEX idx_email ON users (email);

-- Composite index
CREATE INDEX idx_user_time ON events (user_id, timestamp);
```

**Fulltext Index Details:**

The fulltext index uses n-gram tokenization (default: 3-grams) to enable efficient segment pruning for `LIKE '%substring%'` queries:

```sql
-- Create fulltext index on phone numbers
CREATE INDEX idx_calling_ft ON telecom.voice_cdr USING FULLTEXT (calling_number);

-- Query uses index to skip segments that cannot contain matches
SELECT * FROM telecom.voice_cdr WHERE calling_number LIKE '%254712%';
```

- Segments without matching n-grams are skipped entirely (100% pruned)
- Case-insensitive by default
- Ideal for phone number searches, partial string matching

### DROP INDEX

```sql
DROP INDEX index_name ON [database.]table_name;
DROP INDEX IF EXISTS index_name ON [database.]table_name;
```

**Example:**
```sql
DROP INDEX idx_email ON mydb.users;
DROP INDEX IF EXISTS idx_old ON mydb.events;
```

### CREATE VIEW

Views are virtual tables defined by a SQL query. They don't store data but execute the underlying query when accessed.

```sql
CREATE VIEW view_name AS
SELECT ...;

CREATE OR REPLACE VIEW view_name AS
SELECT ...;
```

**Examples:**
```sql
-- Simple view for active users
CREATE VIEW mydb.active_users AS
SELECT user_id, username, email
FROM mydb.users
WHERE status = 'active';

-- View with aggregation
CREATE VIEW mydb.order_totals AS
SELECT customer_id, COUNT(*) as order_count, SUM(total) as total_spent
FROM mydb.orders
GROUP BY customer_id;

-- Replace existing view
CREATE OR REPLACE VIEW mydb.active_users AS
SELECT user_id, username, email, last_login
FROM mydb.users
WHERE status = 'active' AND last_login > NOW() - INTERVAL 30 DAY;
```

### DROP VIEW

```sql
DROP VIEW view_name;
DROP VIEW IF EXISTS view_name;
```

### SHOW VIEWS

```sql
-- Show all views in all databases
SHOW VIEWS;

-- Show views in a specific database
SHOW VIEWS FROM mydb;
```

### DESCRIBE VIEW

```sql
DESCRIBE VIEW mydb.view_name;
```

### CREATE MATERIALIZED VIEW

```sql
CREATE MATERIALIZED VIEW view_name AS
SELECT ...;
```

**Examples:**
```sql
-- Daily aggregations
CREATE MATERIALIZED VIEW daily_stats AS
SELECT DATE(event_time) as day, COUNT(*) as events, SUM(amount) as total
FROM events
GROUP BY DATE(event_time);

-- Customer summaries
CREATE MATERIALIZED VIEW customer_summary AS
SELECT customer_id, COUNT(*) as orders, SUM(total) as lifetime_value
FROM orders
GROUP BY customer_id;
```

### REFRESH MATERIALIZED VIEW

```sql
-- Full refresh (recompute entire view)
REFRESH MATERIALIZED VIEW view_name;

-- Incremental refresh (delta-based, faster for append-heavy tables)
REFRESH MATERIALIZED VIEW view_name INCREMENTAL;
```

**Incremental Refresh:**

Incremental refresh uses watermark tracking to identify rows added since the last refresh, computing only the delta and merging with existing data. This is much faster for append-heavy workloads.

**Examples:**
```sql
-- Full refresh (recomputes everything)
REFRESH MATERIALIZED VIEW daily_stats;

-- Incremental refresh (only processes new data)
REFRESH MATERIALIZED VIEW daily_stats INCREMENTAL;
```

**Best Practices:**
- Use incremental refresh for append-only or append-heavy tables
- Use full refresh after DELETE/UPDATE operations or periodic full reconciliation
- Schedule refreshes during low-traffic periods for large views

### DROP MATERIALIZED VIEW

```sql
DROP MATERIALIZED VIEW view_name;
DROP MATERIALIZED VIEW IF EXISTS view_name;
```

### SHOW INDEXES

```sql
SHOW INDEXES IN [database.]table_name;
```

**Example:**
```sql
SHOW INDEXES IN mydb.users;
```

---

## DML Statements

### INSERT

```sql
INSERT INTO [database.]table_name (column1, column2, ...)
VALUES (value1, value2, ...);

INSERT INTO [database.]table_name (column1, column2, ...)
VALUES
    (value1, value2, ...),
    (value1, value2, ...);
```

**Examples:**
```sql
-- Single row
INSERT INTO users (id, name, email)
VALUES (1, 'John Doe', 'john@example.com');

-- Multiple rows
INSERT INTO events (event_id, user_id, event_type)
VALUES
    ('550e8400-e29b-41d4-a716-446655440000', 1, 'login'),
    ('550e8400-e29b-41d4-a716-446655440001', 2, 'purchase');

-- With JSON
INSERT INTO logs (id, data)
VALUES (1, '{"level": "info", "message": "Started"}');

-- With binary data (hex format)
INSERT INTO files (id, content)
VALUES (1, '0x48656c6c6f');
```

### INSERT ... ON CONFLICT (Upsert)

```sql
-- PostgreSQL syntax: DO NOTHING
INSERT INTO [database.]table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT (conflict_columns) DO NOTHING;

-- PostgreSQL syntax: DO UPDATE
INSERT INTO [database.]table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON CONFLICT (conflict_columns) DO UPDATE SET column = value;

-- MySQL syntax
INSERT INTO [database.]table_name (column1, column2, ...)
VALUES (value1, value2, ...)
ON DUPLICATE KEY UPDATE column = value;
```

**Examples:**
```sql
-- Insert or ignore if key exists
INSERT INTO users (id, name, email)
VALUES (1, 'John Doe', 'john@example.com')
ON CONFLICT (id) DO NOTHING;

-- Insert or update if key exists
INSERT INTO users (id, name, email, login_count)
VALUES (1, 'John Doe', 'john@example.com', 1)
ON CONFLICT (id) DO UPDATE SET
    login_count = EXCLUDED.login_count + 1,
    last_login = NOW();

-- MySQL-style upsert
INSERT INTO counters (id, count)
VALUES (1, 1)
ON DUPLICATE KEY UPDATE count = count + 1;

-- Multi-row upsert
INSERT INTO inventory (product_id, quantity)
VALUES
    (100, 50),
    (101, 30),
    (102, 20)
ON CONFLICT (product_id) DO UPDATE SET quantity = EXCLUDED.quantity;
```

**Notes:**
- `EXCLUDED` refers to the values that would have been inserted
- Conflict columns should have a PRIMARY KEY or UNIQUE constraint
- Returns count of inserted and updated rows

### UPDATE

```sql
UPDATE [database.]table_name
SET column1 = value1, column2 = value2, ...
WHERE condition;
```

**Examples:**
```sql
UPDATE users SET email = 'new@example.com' WHERE id = 1;
UPDATE orders SET status = 'shipped' WHERE order_date < '2024-01-01';
UPDATE products SET price = price * 1.1 WHERE category = 'electronics';
```

### DELETE

```sql
DELETE FROM [database.]table_name
WHERE condition;
```

**Examples:**
```sql
DELETE FROM users WHERE status = 'inactive';
DELETE FROM logs WHERE event_time < 1700000000;
DELETE FROM temp_data WHERE processed = true;
```

---

## Transaction Control

BoyoDB supports full ACID transactions with multiple isolation levels.

### BEGIN/START TRANSACTION

```sql
BEGIN;
BEGIN TRANSACTION;
START TRANSACTION;
START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
START TRANSACTION READ ONLY;
```

**Isolation Levels:**

| Level | Description |
|-------|-------------|
| `READ UNCOMMITTED` | Allows dirty reads (not recommended) |
| `READ COMMITTED` | Default; sees only committed data |
| `REPEATABLE READ` | Snapshot at transaction start |
| `SERIALIZABLE` | Full isolation, may abort on conflicts |

**Examples:**
```sql
-- Default isolation
BEGIN;
INSERT INTO accounts (id, balance) VALUES (1, 1000);
COMMIT;

-- Serializable for financial operations
START TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Read-only transaction
START TRANSACTION READ ONLY;
SELECT SUM(balance) FROM accounts;
COMMIT;
```

### COMMIT

```sql
COMMIT;
COMMIT TRANSACTION;
```

Commits all changes made in the current transaction.

### ROLLBACK

```sql
ROLLBACK;
ROLLBACK TRANSACTION;
ROLLBACK TO SAVEPOINT savepoint_name;
```

Aborts the current transaction or rolls back to a savepoint.

### SAVEPOINT

```sql
SAVEPOINT savepoint_name;
RELEASE SAVEPOINT savepoint_name;
ROLLBACK TO SAVEPOINT savepoint_name;
```

**Example:**
```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (1, 100);
SAVEPOINT order_created;

INSERT INTO order_items (order_id, product_id) VALUES (1, 999);
-- Oops, product 999 doesn't exist
ROLLBACK TO SAVEPOINT order_created;

INSERT INTO order_items (order_id, product_id) VALUES (1, 42);
COMMIT;
```

---

## Query Statements

### SELECT

```sql
SELECT [DISTINCT] columns
FROM [database.]table_name
[WHERE conditions]
[GROUP BY columns]
[HAVING conditions]
[ORDER BY columns [ASC|DESC]]
[LIMIT n]
[OFFSET n];
```

### Column Selection

```sql
-- All columns
SELECT * FROM users;

-- Specific columns
SELECT id, name, email FROM users;

-- With aliases
SELECT id AS user_id, name AS full_name FROM users;

-- With expressions
SELECT id, price * quantity AS total FROM orders;

-- DISTINCT
SELECT DISTINCT category FROM products;

-- DISTINCT ON (PostgreSQL-style)
SELECT DISTINCT ON (category) * FROM products ORDER BY category, price ASC;
```

### DISTINCT ON

PostgreSQL-style first-row-per-group selection:

```sql
SELECT DISTINCT ON (column1, column2, ...) columns
FROM table
ORDER BY column1, column2, ... [, additional_columns];
```

Returns the first row for each unique combination of the DISTINCT ON columns. The ORDER BY clause determines which row is "first".

**Examples:**
```sql
-- Get cheapest product in each category
SELECT DISTINCT ON (category) id, category, name, price
FROM products
ORDER BY category, price ASC;

-- Get most recent order per customer
SELECT DISTINCT ON (customer_id) *
FROM orders
ORDER BY customer_id, order_date DESC;

-- Multiple DISTINCT ON columns
SELECT DISTINCT ON (region, category) *
FROM sales
ORDER BY region, category, revenue DESC;
```

**Note:** The DISTINCT ON columns must match the leading columns in ORDER BY.

### WHERE Clause

```sql
-- Comparison operators
SELECT * FROM users WHERE age > 18;
SELECT * FROM users WHERE status = 'active';
SELECT * FROM users WHERE created_at >= 1700000000;

-- Multiple conditions
SELECT * FROM orders WHERE status = 'pending' AND total > 100;
SELECT * FROM users WHERE role = 'admin' OR role = 'moderator';

-- NULL checks
SELECT * FROM users WHERE phone IS NULL;
SELECT * FROM orders WHERE shipped_at IS NOT NULL;

-- IN lists
SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5);
SELECT * FROM products WHERE category IN ('electronics', 'books');

-- LIKE patterns
SELECT * FROM users WHERE name LIKE 'John%';
SELECT * FROM logs WHERE message LIKE '%error%';
SELECT * FROM codes WHERE code LIKE 'A___';  -- 4 characters starting with A

-- NOT LIKE
SELECT * FROM logs WHERE message NOT LIKE '%debug%';

-- BETWEEN
SELECT * FROM orders WHERE total BETWEEN 100 AND 500;
SELECT * FROM events WHERE event_time BETWEEN 1700000000 AND 1800000000;
```

### GROUP BY

```sql
-- Single column
SELECT category, COUNT(*) FROM products GROUP BY category;

-- Multiple columns
SELECT category, status, COUNT(*) FROM products GROUP BY category, status;

-- GROUPING SETS - multiple groupings in one query
SELECT category, region, SUM(sales)
FROM sales_data
GROUP BY GROUPING SETS ((category, region), (category), (region), ());

-- ROLLUP - hierarchical subtotals
SELECT category, region, SUM(sales)
FROM sales_data
GROUP BY ROLLUP (category, region);
-- Produces: (category, region), (category), ()

-- CUBE - all possible combinations
SELECT category, region, SUM(sales)
FROM sales_data
GROUP BY CUBE (category, region);
-- Produces: (category, region), (category), (region), ()

-- With aggregations
SELECT
    tenant_id,
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average
FROM transactions
GROUP BY tenant_id;

-- With HAVING
SELECT category, COUNT(*) as cnt
FROM products
GROUP BY category
HAVING COUNT(*) > 10;
```

### ORDER BY

```sql
-- Ascending (default)
SELECT * FROM users ORDER BY name;
SELECT * FROM users ORDER BY name ASC;

-- Descending
SELECT * FROM users ORDER BY created_at DESC;

-- Multiple columns
SELECT * FROM orders ORDER BY status ASC, created_at DESC;

-- With LIMIT
SELECT * FROM products ORDER BY price DESC LIMIT 10;
```

### LIMIT and OFFSET

```sql
-- First 10 rows
SELECT * FROM users LIMIT 10;

-- Skip first 20, get next 10
SELECT * FROM users LIMIT 10 OFFSET 20;

-- Pagination
SELECT * FROM products ORDER BY id LIMIT 25 OFFSET 50;
```

### JOIN

```sql
-- INNER JOIN
SELECT a.*, b.name
FROM orders a
INNER JOIN customers b ON a.customer_id = b.id;

-- LEFT JOIN
SELECT a.*, b.name
FROM orders a
LEFT JOIN customers b ON a.customer_id = b.id;

-- RIGHT JOIN
SELECT a.*, b.name
FROM orders a
RIGHT JOIN customers b ON a.customer_id = b.id;

-- FULL OUTER JOIN
SELECT a.*, b.name
FROM orders a
FULL OUTER JOIN customers b ON a.customer_id = b.id;

-- With aliases
SELECT o.id, o.total, c.name, c.email
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id
WHERE o.status = 'completed';

-- Multiple joins
SELECT o.id, c.name, p.name as product
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id;
```

---

## Operators

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal to |
| `<>`, `!=` | Not equal to |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |

### Logical Operators

| Operator | Description |
|----------|-------------|
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical NOT |

### Special Operators

| Operator | Description |
|----------|-------------|
| `IS NULL` | Check for NULL |
| `IS NOT NULL` | Check for non-NULL |
| `IN (...)` | Match any in list |
| `NOT IN (...)` | Match none in list |
| `BETWEEN a AND b` | Range check |
| `LIKE` | Pattern match |
| `NOT LIKE` | Negated pattern match |

### Arithmetic Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

---

## Functions

### Aggregate Functions

```sql
COUNT(*)              -- Count all rows
COUNT(column)         -- Count non-NULL values
COUNT(DISTINCT col)   -- Count unique values
SUM(column)           -- Sum of values
AVG(column)           -- Average of values
MIN(column)           -- Minimum value
MAX(column)           -- Maximum value
MEDIAN(column)        -- Median value
STDDEV(column)        -- Standard deviation
VARIANCE(column)      -- Variance

-- Percentile functions
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY column)  -- Continuous percentile (interpolated)
PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY column)  -- Discrete percentile (actual value)

-- Collection aggregates
ARRAY_AGG(column)                    -- Collect values into array
ARRAY_AGG(DISTINCT column)           -- Collect unique values into array
STRING_AGG(column, ',')              -- Concatenate strings with delimiter
STRING_AGG(DISTINCT column, ',')     -- Concatenate unique strings

-- Ordered aggregates (WITHIN GROUP)
MODE() WITHIN GROUP (ORDER BY column)                    -- Most frequent value
STRING_AGG_ORDERED(column, ',' ORDER BY col)             -- Ordered string concatenation
ARRAY_AGG_ORDERED(column ORDER BY col)                   -- Ordered array collection
FIRST_VALUE(column) WITHIN GROUP (ORDER BY col)          -- First value in order
LAST_VALUE(column) WITHIN GROUP (ORDER BY col)           -- Last value in order
NTH_VALUE(column, n) WITHIN GROUP (ORDER BY col)         -- Nth value in order

-- Approximate aggregates (faster for large datasets)
APPROX_COUNT_DISTINCT(column)                            -- HyperLogLog-based distinct count
APPROX_PERCENTILE(column, 0.95)                          -- T-Digest approximate percentile
APPROX_MEDIAN(column)                                    -- T-Digest approximate median
```

**Examples:**
```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(amount), AVG(amount) FROM transactions;
SELECT MIN(price), MAX(price) FROM products WHERE category = 'electronics';

-- Statistical analysis
SELECT MEDIAN(response_time_ms) FROM api_requests;
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency) as p50,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency) as p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency) as p99
FROM requests;

-- Collection aggregates
SELECT user_id, ARRAY_AGG(product_id) as purchased_products
FROM orders GROUP BY user_id;

SELECT department, STRING_AGG(name, ', ') as team_members
FROM employees GROUP BY department;

-- Ordered aggregates (WITHIN GROUP)
SELECT category, MODE() WITHIN GROUP (ORDER BY brand) as most_common_brand
FROM products GROUP BY category;

SELECT user_id, STRING_AGG_ORDERED(event_type, ' -> ' ORDER BY event_time) as journey
FROM events GROUP BY user_id;

SELECT department, FIRST_VALUE(name) WITHIN GROUP (ORDER BY hire_date) as first_hire
FROM employees GROUP BY department;

SELECT department, NTH_VALUE(salary, 2) WITHIN GROUP (ORDER BY salary DESC) as second_highest
FROM employees GROUP BY department;
```

### String Functions

```sql
UPPER(string)                    -- Convert to uppercase
LOWER(string)                    -- Convert to lowercase
LENGTH(string)                   -- String length
TRIM(string)                     -- Remove whitespace
LTRIM(string)                    -- Remove leading whitespace
RTRIM(string)                    -- Remove trailing whitespace
SUBSTR(string, start, length)    -- Extract substring
CONCAT(str1, str2, ...)          -- Concatenate strings
REPLACE(string, from, to)        -- Replace occurrences
COALESCE(val1, val2, ...)        -- First non-NULL value

-- Regular expression functions
REGEXP_REPLACE(string, pattern, replacement)           -- Replace regex matches
REGEXP_REPLACE(string, pattern, replacement, flags)    -- With flags (i=case-insensitive, g=global)
REGEXP_MATCH(string, pattern)                          -- Check if pattern matches
REGEXP_EXTRACT(string, pattern)                        -- Extract first match
REGEXP_EXTRACT(string, pattern, group)                 -- Extract specific capture group
```

**Examples:**
```sql
SELECT UPPER(name) FROM users;
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;
SELECT SUBSTR(phone, 1, 3) AS area_code FROM users;
SELECT COALESCE(nickname, name) AS display_name FROM users;

-- Regex examples
SELECT REGEXP_REPLACE(email, '@.*', '@redacted.com') AS masked_email FROM users;
SELECT REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS digits_only FROM users;
SELECT * FROM logs WHERE REGEXP_MATCH(message, 'error|warning', 'i');
SELECT REGEXP_EXTRACT(url, 'https?://([^/]+)', 1) AS domain FROM requests;
```

### JSON Functions

```sql
-- Basic extraction
JSON_EXTRACT(json, '$.path')           -- Extract value at path
JSON_EXTRACT(json, '$.nested.field')   -- Nested field access
JSON_EXTRACT(json, '$.array[0]')       -- Array index access

-- Advanced JSON Path expressions
JSON_EXTRACT(json, '$.items[*].name')          -- Wildcard: all array elements
JSON_EXTRACT(json, '$.items[0:5]')             -- Slicing: elements 0-4
JSON_EXTRACT(json, '$..field')                 -- Recursive descent: find at any depth
JSON_EXTRACT(json, '$.items[?(@.price > 100)]') -- Filter: conditional selection

-- Multi-value extraction
JSON_EXTRACT_ALL(json, '$.items[*].id')        -- Returns array of all matches
```

**JSON Path Syntax:**

| Syntax | Description | Example |
|--------|-------------|---------|
| `$.field` | Root field access | `$.name` |
| `$.a.b` | Nested field | `$.user.email` |
| `$[n]` | Array index | `$[0]`, `$[2]` |
| `$[*]` | All array elements | `$.items[*]` |
| `$[start:end]` | Array slice | `$[0:5]`, `$[2:10]` |
| `$..field` | Recursive descent | `$..id` (find id at any level) |
| `$[?(@.x > y)]` | Filter expression | `$[?(@.price > 100)]` |

**Examples:**
```sql
-- Extract nested field
SELECT JSON_EXTRACT(metadata, '$.user.preferences.theme') FROM settings;

-- Get all product names from array
SELECT JSON_EXTRACT(order_data, '$.items[*].name') FROM orders;

-- Slice first 3 items
SELECT JSON_EXTRACT(data, '$.results[0:3]') FROM api_responses;

-- Find all IDs at any depth
SELECT JSON_EXTRACT(document, '$..id') FROM documents;

-- Filter products over $100
SELECT JSON_EXTRACT(cart, '$.items[?(@.price > 100)]') FROM shopping_carts;

-- Extract all matching values as array
SELECT JSON_EXTRACT_ALL(log_entry, '$.events[*].timestamp') FROM logs;
```

### Math Functions

```sql
ABS(number)                 -- Absolute value
CEIL(number)                -- Round up
FLOOR(number)               -- Round down
ROUND(number, decimals)     -- Round to decimals
SQRT(number)                -- Square root
POWER(base, exponent)       -- Exponentiation
MOD(dividend, divisor)      -- Modulo
```

**Examples:**
```sql
SELECT ABS(balance) FROM accounts;
SELECT ROUND(price, 2) FROM products;
SELECT POWER(2, 10) AS result;
```

### Date/Time Functions

```sql
-- Current date/time
NOW()                           -- Current timestamp (microseconds since epoch)
CURRENT_DATE                    -- Current date (days since epoch)
CURRENT_TIMESTAMP               -- Alias for NOW()

-- Date extraction
DATE(timestamp)                 -- Extract date from timestamp
EXTRACT(field FROM timestamp)   -- Extract specific field

-- Date arithmetic with INTERVAL
timestamp + INTERVAL 'n unit'   -- Add interval to timestamp
timestamp - INTERVAL 'n unit'   -- Subtract interval from timestamp
DATE_ADD(timestamp, n, 'unit')  -- Add n units to timestamp
DATE_SUB(timestamp, n, 'unit')  -- Subtract n units from timestamp
DATEDIFF('unit', start, end)    -- Difference between timestamps

-- Conversion
TO_TIMESTAMP(epoch_seconds)     -- Convert Unix timestamp to timestamp
FROM_UNIXTIME(epoch_seconds)    -- Alias for TO_TIMESTAMP
TOSTRING(value)                 -- Convert any value to string
```

**EXTRACT Fields:**

| Field | Description |
|-------|-------------|
| `YEAR` | Year (e.g., 2024) |
| `MONTH` | Month (1-12) |
| `DAY` | Day of month (1-31) |
| `HOUR` | Hour (0-23) |
| `MINUTE` | Minute (0-59) |
| `SECOND` | Second (0-59) |
| `MILLISECOND` | Milliseconds (0-999) |
| `MICROSECOND` | Microseconds (0-999999) |
| `DOW` / `DAYOFWEEK` | Day of week (0=Sunday) |
| `DOY` / `DAYOFYEAR` | Day of year (1-366) |
| `WEEK` | ISO week number (1-53) |
| `QUARTER` | Quarter (1-4) |
| `EPOCH` | Seconds since Unix epoch |

**INTERVAL Units:**

| Unit | Aliases |
|------|---------|
| `MICROSECOND` | `MICROSECONDS` |
| `MILLISECOND` | `MILLISECONDS` |
| `SECOND` | `SECONDS` |
| `MINUTE` | `MINUTES` |
| `HOUR` | `HOURS` |
| `DAY` | `DAYS` |
| `WEEK` | `WEEKS` |
| `MONTH` | `MONTHS` (approximate: 30 days) |
| `YEAR` | `YEARS` (approximate: 365 days) |

**Examples:**
```sql
-- Current date and time
SELECT NOW();
SELECT CURRENT_DATE;
SELECT CURRENT_TIMESTAMP;

-- Extract date components
SELECT EXTRACT(YEAR FROM created_at) as year FROM events;
SELECT EXTRACT(HOUR FROM event_time) as hour, COUNT(*) FROM logs GROUP BY hour;
SELECT EXTRACT(DOW FROM order_date) as day_of_week FROM orders;

-- Date arithmetic with INTERVAL
SELECT * FROM events WHERE event_time >= NOW() - INTERVAL '24 hours';
SELECT * FROM logs WHERE created_at >= CURRENT_DATE - INTERVAL '7 days';
SELECT * FROM orders WHERE order_date > NOW() - INTERVAL '1 month';

-- Using DATE_ADD/DATE_SUB functions
SELECT DATE_ADD(NOW(), 30, 'days') as due_date;
SELECT DATE_SUB(created_at, 1, 'hour') as adjusted_time FROM events;

-- Calculate time differences
SELECT DATEDIFF('days', created_at, NOW()) as days_old FROM users;
SELECT DATEDIFF('hours', start_time, end_time) as duration FROM sessions;

-- Convert timestamps
SELECT TO_TIMESTAMP(1700000000) as ts;
SELECT FROM_UNIXTIME(epoch_seconds) FROM raw_data;

-- Convert date to string
SELECT TOSTRING(created_at) as date_string FROM events;
SELECT TOSTRING(NOW()) as current_time;

-- Filter by date
SELECT * FROM orders WHERE DATE(created_at) = CURRENT_DATE;
SELECT * FROM logs WHERE DATE(event_time) = DATE(NOW() - INTERVAL '1 day');

-- Aggregate by time periods
SELECT
    DATE(event_time) as day,
    COUNT(*) as event_count
FROM events
WHERE event_time >= NOW() - INTERVAL '30 days'
GROUP BY DATE(event_time)
ORDER BY day;

-- Hourly breakdown
SELECT
    EXTRACT(HOUR FROM event_time) as hour,
    COUNT(*) as total
FROM events
WHERE event_time >= NOW() - INTERVAL '24 hours'
GROUP BY EXTRACT(HOUR FROM event_time)
ORDER BY hour;
```

### Type Conversion

```sql
CAST(value AS type)
```

**Examples:**
```sql
SELECT CAST('2024-01-15' AS DATE);
SELECT CAST(123 AS STRING);
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
SELECT CAST(99.95 AS DECIMAL(10,2));
SELECT CAST('{"key": "value"}' AS JSON);
```

### Machine Learning Functions

```sql
PREDICT(model_name, column_data) -- Invoke an ONNX Inference model natively
```

**Examples:**
```sql
-- Predict sentiment based on a registered model
SELECT PREDICT('sentiment-model-v2', comment_text) FROM interactions;
```
*Note: Models must be registered via `REGISTER MODEL 'name' FROM '/path/to/model.onnx' FORMAT ONNX` before `PREDICT` is called. Native inference uses the integrated zero-copy `ort` runtime.*

---

## Window Functions

### Syntax

```sql
function_name() OVER (
    [PARTITION BY column, ...]
    [ORDER BY column [ASC|DESC], ...]
    [frame_clause]
)
```

### Ranking Functions

```sql
-- Row number (unique sequential)
ROW_NUMBER() OVER (ORDER BY column)

-- Rank (same rank for ties, gaps after)
RANK() OVER (ORDER BY column)

-- Dense rank (same rank for ties, no gaps)
DENSE_RANK() OVER (ORDER BY column)
```

**Examples:**
```sql
SELECT
    name,
    score,
    ROW_NUMBER() OVER (ORDER BY score DESC) as position,
    RANK() OVER (ORDER BY score DESC) as rank
FROM players;

SELECT
    department,
    name,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM employees;
```

### Value Functions

```sql
-- Previous row value
LAG(column, offset, default) OVER (ORDER BY column)

-- Next row value
LEAD(column, offset, default) OVER (ORDER BY column)

-- First value in partition
FIRST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)

-- Last value in partition
LAST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)
```

**Examples:**
```sql
SELECT
    date,
    price,
    LAG(price, 1) OVER (ORDER BY date) as prev_price,
    price - LAG(price, 1) OVER (ORDER BY date) as change
FROM stock_prices;

SELECT
    user_id,
    event_time,
    LEAD(event_time, 1) OVER (PARTITION BY user_id ORDER BY event_time) as next_event
FROM events;
```

### Aggregate Window Functions

```sql
SUM(column) OVER (PARTITION BY ... ORDER BY ...)
AVG(column) OVER (PARTITION BY ... ORDER BY ...)
COUNT(*) OVER (PARTITION BY ...)
MIN(column) OVER (PARTITION BY ...)
MAX(column) OVER (PARTITION BY ...)
```

**Examples:**
```sql
SELECT
    date,
    sales,
    SUM(sales) OVER (ORDER BY date) as cumulative_sales,
    AVG(sales) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg
FROM daily_sales;

SELECT
    department,
    name,
    salary,
    SUM(salary) OVER (PARTITION BY department) as dept_total,
    salary / SUM(salary) OVER (PARTITION BY department) as pct_of_dept
FROM employees;
```

---

## Common Table Expressions

### Basic CTE

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

**Example:**
```sql
WITH active_users AS (
    SELECT id, name, email
    FROM users
    WHERE status = 'active'
)
SELECT * FROM active_users WHERE name LIKE 'J%';
```

### Multiple CTEs

```sql
WITH
    cte1 AS (SELECT ...),
    cte2 AS (SELECT ...)
SELECT * FROM cte1 JOIN cte2 ON ...;
```

**Example:**
```sql
WITH
    recent_orders AS (
        SELECT * FROM orders WHERE order_date > '2024-01-01'
    ),
    high_value AS (
        SELECT * FROM recent_orders WHERE total > 1000
    )
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(total) as total_value
FROM high_value
GROUP BY customer_id
ORDER BY total_value DESC;
```

---

## Prepared Statements

### Client-Side (Parameterized)

```sql
-- Create prepared statement
PREPARE statement_name AS SELECT ... WHERE column = $1;

-- Execute with parameters
EXECUTE statement_name (value1);
EXECUTE statement_name (value1, value2);

-- Remove prepared statement
DEALLOCATE statement_name;
DEALLOCATE ALL;
```

**Examples:**
```sql
-- Simple query
PREPARE get_user AS SELECT * FROM users WHERE id = $1;
EXECUTE get_user (42);

-- Multiple parameters
PREPARE get_orders AS
    SELECT * FROM orders
    WHERE customer_id = $1 AND order_date > $2;
EXECUTE get_orders (123, '2024-01-01');

-- Clean up
DEALLOCATE get_user;
DEALLOCATE ALL;
```

### Server-Side (Binary Protocol)

```sql
-- Create server-side prepared statement
PREPARE statement_name AS SELECT ...;

-- Execute (returns binary IPC)
EXECUTE statement_name;
```

---

## Maintenance Statements

### VACUUM

Compact table segments to reduce segment count and reclaim space.

```sql
VACUUM [database.]table_name;
VACUUM FULL [database.]table_name;
VACUUM FORCE [database.]table_name;
VACUUM FULL FORCE [database.]table_name;
```

**Modes:**

| Mode | Description |
|------|-------------|
| `VACUUM` | Rewrites fragmented segments (< 50% of target size) |
| `VACUUM FULL` | Merges ALL segments into optimally-sized chunks |
| `VACUUM FORCE` | Skips missing/corrupted segments during compaction |
| `VACUUM FULL FORCE` | Full compaction, skipping any damaged segments |

**Examples:**

```sql
-- Basic vacuum (compact fragmented segments)
VACUUM analytics.events;

-- Full vacuum (merge all segments, slower but thorough)
VACUUM FULL analytics.events;

-- Force vacuum when segments are missing/corrupted
VACUUM FORCE analytics.events;

-- Full vacuum that skips damaged segments
VACUUM FULL FORCE analytics.events;
```

**Note:** Use `VACUUM FORCE` when normal VACUUM fails due to missing or corrupted segments. This mode removes damaged segment references from the manifest while compacting the remaining healthy segments.

**Returns:**
```
Vacuum complete: processed=1500 removed=1450 reclaimed=524288000 bytes new=12
```

**Best Practices:**

- Run `VACUUM` periodically after heavy write/delete workloads
- Use `VACUUM FULL` when segment count grows excessively (e.g., >10,000 segments per table)
- Schedule during low-traffic periods as it temporarily increases I/O

### COMPACT TABLE

Merge small segments into larger ones without full rewrite.

```sql
COMPACT TABLE [database.]table_name;
```

**Example:**
```sql
-- Compact a specific table
COMPACT TABLE analytics.events;
```

**Returns:**
```
Compaction complete: merged 150 segments into 12, reclaimed 52428800 bytes
```

**Difference from VACUUM:**
- `COMPACT TABLE` is faster and merges adjacent small segments
- `VACUUM` rewrites all fragmented segments
- `VACUUM FULL` rewrites the entire table

---

## Utility Statements

### SHOW Statements

```sql
SHOW DATABASES;
SHOW TABLES;
SHOW TABLES IN database_name;
SHOW USERS;
SHOW ROLES;
SHOW GRANTS FOR username;
SHOW SERVER INFO;              -- Server version and statistics
SHOW MISSING SEGMENTS;         -- Find segments missing from disk
SHOW MISSING SEGMENTS FROM database.table;  -- For specific table
SHOW REPAIR STATUS;            -- Auto-repair background task status
```

### Server Information

```sql
SHOW SERVER INFO;
```

Returns server status including:
- Server version
- Database count
- Table count
- Segment count
- Manifest version
- WAL LSN

### Segment Integrity

```sql
-- Find segments referenced in manifest but missing from disk
SHOW MISSING SEGMENTS;
SHOW MISSING SEGMENTS FROM analytics.events;

-- Remove missing segment entries from manifest (repairs metadata)
REPAIR SEGMENTS database.table;
REPAIR SEGMENTS *.*;           -- Repair all tables
```

**Example workflow:**
```sql
-- Check for missing segments
SHOW MISSING SEGMENTS;

-- If segments are missing, repair the manifest
REPAIR SEGMENTS analytics.events;

-- Verify repair
SHOW MISSING SEGMENTS FROM analytics.events;
```

### DESCRIBE

```sql
DESCRIBE [database.]table_name;
DESC [database.]table_name;
```

### EXPLAIN

```sql
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;
```

**Example:**
```sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 42;
-- Returns: segments to scan, bytes, bloom filter usage, parallel scan info
```

---

## Reserved Words

The following are reserved words and cannot be used as identifiers without quoting:

```
SELECT, FROM, WHERE, AND, OR, NOT, IN, LIKE, BETWEEN, IS, NULL,
TRUE, FALSE, AS, ON, JOIN, LEFT, RIGHT, INNER, OUTER, FULL, CROSS,
GROUP, BY, HAVING, ORDER, ASC, DESC, LIMIT, OFFSET, DISTINCT,
INSERT, INTO, VALUES, UPDATE, SET, DELETE, CREATE, DROP, ALTER,
DATABASE, TABLE, COLUMN, ADD, IF, EXISTS, TRUNCATE,
WITH, UNION, INTERSECT, EXCEPT, ALL, ANY, SOME,
CASE, WHEN, THEN, ELSE, END, CAST, OVER, PARTITION, ROWS, RANGE,
PREPARE, EXECUTE, DEALLOCATE, EXPLAIN, ANALYZE, DESCRIBE, SHOW,
GRANT, REVOKE, TO, FOR, ROLE, USER, PASSWORD, SUPERUSER, ADMIN
```

---

## User-Defined Functions

BoyoDB supports user-defined scalar functions for custom data transformations.

### CREATE FUNCTION

```sql
CREATE FUNCTION function_name(param1 type, param2 type, ...)
RETURNS return_type
AS expression;
```

**Examples:**

```sql
-- Simple calculation
CREATE FUNCTION double_value(x INT64)
RETURNS INT64
AS x * 2;

-- String manipulation
CREATE FUNCTION full_name(first STRING, last STRING)
RETURNS STRING
AS CONCAT(first, ' ', last);

-- Conditional logic
CREATE FUNCTION discount_price(price FLOAT64, discount FLOAT64)
RETURNS FLOAT64
AS price * (1 - discount);

-- With COALESCE for null handling
CREATE FUNCTION safe_divide(a FLOAT64, b FLOAT64)
RETURNS FLOAT64
AS COALESCE(a / NULLIF(b, 0), 0);
```

### DROP FUNCTION

```sql
DROP FUNCTION function_name;
DROP FUNCTION IF EXISTS function_name;
```

### SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Lists all user-defined functions with their signatures and return types.

### Using Functions

```sql
-- Use in SELECT
SELECT id, double_value(amount) as doubled FROM orders;

-- Use in WHERE
SELECT * FROM products WHERE discount_price(price, 0.1) < 100;

-- Combine with built-in functions
SELECT full_name(first_name, last_name) as name FROM users;
```

---

## Stream Connectors

BoyoDB supports streaming data ingestion from Kafka and Pulsar.

### CREATE STREAM

```sql
CREATE STREAM stream_name
FROM KAFKA|PULSAR 'connection_config'
INTO database.table
[FORMAT json|csv];
```

**Examples:**

```sql
-- Kafka stream
CREATE STREAM events_stream
FROM KAFKA 'bootstrap.servers=localhost:9092;topic=events;group.id=boyodb'
INTO analytics.events
FORMAT json;

-- Pulsar stream
CREATE STREAM logs_stream
FROM PULSAR 'service_url=pulsar://localhost:6650;topic=logs'
INTO logging.entries
FORMAT json;

-- CSV format
CREATE STREAM csv_stream
FROM KAFKA 'bootstrap.servers=kafka:9092;topic=data'
INTO mydb.records
FORMAT csv;
```

### DROP STREAM

```sql
DROP STREAM stream_name;
DROP STREAM IF EXISTS stream_name;
```

### SHOW STREAMS

```sql
SHOW STREAMS;
```

Lists all defined streams with their source, target table, and status.

### START STREAM

```sql
START STREAM stream_name;
```

Starts consuming messages from the configured source.

### STOP STREAM

```sql
STOP STREAM stream_name;
```

Stops consuming messages (can be resumed with START STREAM).

### SHOW STREAM STATUS

```sql
SHOW STREAM STATUS stream_name;
```

Shows detailed status including:
- Connection state
- Messages consumed
- Last message timestamp
- Error count

**Example workflow:**

```sql
-- Create and start a stream
CREATE STREAM user_events
FROM KAFKA 'bootstrap.servers=kafka:9092;topic=user-events;group.id=analytics'
INTO analytics.user_events
FORMAT json;

START STREAM user_events;

-- Check status
SHOW STREAM STATUS user_events;

-- Stop when needed
STOP STREAM user_events;

-- Remove stream definition
DROP STREAM user_events;
```

---

## PIVOT and UNPIVOT

### PIVOT

Transform rows to columns based on aggregate values:

```sql
SELECT *
FROM sales_data
PIVOT (
    SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
);
```

**Example:**
```sql
-- Source data:
-- | product | quarter | amount |
-- | A       | Q1      | 100    |
-- | A       | Q2      | 150    |
-- | B       | Q1      | 200    |

-- After PIVOT:
-- | product | Q1  | Q2  | Q3   | Q4   |
-- | A       | 100 | 150 | NULL | NULL |
-- | B       | 200 | NULL| NULL | NULL |
```

### UNPIVOT

Transform columns to rows:

```sql
SELECT *
FROM quarterly_sales
UNPIVOT (
    amount FOR quarter IN (Q1, Q2, Q3, Q4)
);
```

**Example:**
```sql
-- Source data:
-- | product | Q1  | Q2  | Q3  | Q4  |
-- | A       | 100 | 150 | 200 | 175 |

-- After UNPIVOT:
-- | product | quarter | amount |
-- | A       | Q1      | 100    |
-- | A       | Q2      | 150    |
-- | A       | Q3      | 200    |
-- | A       | Q4      | 175    |
```

---

## Views

Views are virtual tables defined by SQL queries. They don't store data but execute the underlying query when accessed, making them useful for:

- Simplifying complex queries
- Providing a consistent interface to data
- Restricting access to specific columns or rows
- Abstracting underlying table structure changes

See [CREATE VIEW](#create-view), [DROP VIEW](#drop-view), [SHOW VIEWS](#show-views), and [DESCRIBE VIEW](#describe-view) in DDL Statements for syntax.

**Example Usage:**
```sql
-- Create a view
CREATE VIEW mydb.high_value_orders AS
SELECT order_id, customer_id, total, order_date
FROM mydb.orders
WHERE total > 1000;

-- Query the view like a regular table
SELECT * FROM mydb.high_value_orders WHERE order_date > '2024-01-01';

-- Views are automatically expanded in queries
-- The above query is equivalent to:
SELECT * FROM (
    SELECT order_id, customer_id, total, order_date
    FROM mydb.orders
    WHERE total > 1000
) WHERE order_date > '2024-01-01';
```

---

## Materialized Views

See [CREATE MATERIALIZED VIEW](#create-materialized-view) in DDL Statements for syntax.

Materialized views store precomputed query results for fast access. BoyoDB supports both full and incremental refresh strategies.

**Key Features:**
- Full refresh: Recomputes entire view from base tables
- Incremental refresh: Uses watermark tracking to process only new data
- Automatic query rewriting to use materialized views (when applicable)

---

## Foreign Data Wrappers

BoyoDB supports querying external databases through Foreign Data Wrappers (FDW).

### CREATE SERVER

```sql
CREATE SERVER server_name
FOREIGN DATA WRAPPER postgres
OPTIONS (host 'hostname', port '5432', dbname 'database');
```

### CREATE FOREIGN TABLE

```sql
CREATE FOREIGN TABLE local_name (
    column1 type,
    column2 type,
    ...
)
SERVER server_name
OPTIONS (schema_name 'public', table_name 'remote_table');
```

### Query Federation

BoyoDB can push down operations to foreign servers for improved performance:

```sql
-- Aggregations are pushed to remote server
SELECT category, SUM(amount) FROM foreign_sales GROUP BY category;

-- Sorts are pushed down
SELECT * FROM foreign_orders ORDER BY created_at DESC LIMIT 100;

-- Filters are pushed down
SELECT * FROM foreign_users WHERE status = 'active';
```

**Push-down Capabilities:**
- WHERE clause filters
- GROUP BY with aggregations (SUM, COUNT, AVG, MIN, MAX)
- ORDER BY with LIMIT
- Column projection (only fetch needed columns)

**Example Setup:**
```sql
-- Create connection to PostgreSQL
CREATE SERVER pg_warehouse
FOREIGN DATA WRAPPER postgres
OPTIONS (host 'warehouse.example.com', port '5432', dbname 'analytics');

-- Create foreign table
CREATE FOREIGN TABLE remote_sales (
    id INT64,
    product_id INT64,
    amount DECIMAL(10,2),
    sale_date TIMESTAMP
)
SERVER pg_warehouse
OPTIONS (schema_name 'public', table_name 'sales');

-- Query with push-down
SELECT product_id, SUM(amount) as total
FROM remote_sales
WHERE sale_date >= '2024-01-01'
GROUP BY product_id
ORDER BY total DESC
LIMIT 10;
```

---

## Recovery Statements

BoyoDB supports Point-in-Time Recovery (PITR) for disaster recovery.

### CREATE BACKUP

```sql
CREATE BACKUP;
CREATE BACKUP 'weekly-backup';
```

Creates a base backup with optional label.

### SHOW BACKUPS

```sql
SHOW BACKUPS;
```

Lists all available backups with timestamps and LSN.

### SHOW WAL STATUS

```sql
SHOW WAL STATUS;
```

Shows current WAL position and archiving status.

### RECOVER

```sql
-- Recover to specific timestamp
RECOVER TO TIMESTAMP '2024-01-15 14:30:00';

-- Recover to specific LSN
RECOVER TO LSN 12345678;
```

**Example Recovery Workflow:**
```sql
-- Check available backups
SHOW BACKUPS;

-- Check WAL status
SHOW WAL STATUS;

-- Recover to point before data corruption
RECOVER TO TIMESTAMP '2024-01-15 14:30:00';
```

---

## Analytical Functions

BoyoDB provides specialized SQL functions for advanced analytics.

### Graph Functions

```sql
-- Traverse graph from start node
SELECT * FROM GRAPH_TRAVERSE('graph_name', 'start_node', 'outgoing', 3);

-- Find shortest path between nodes
SELECT * FROM SHORTEST_PATH('graph_name', 'node_a', 'node_b');

-- Calculate PageRank scores
SELECT * FROM PAGERANK('graph_name', 0.85, 20);

-- Detect communities
SELECT * FROM COMMUNITY_DETECT('graph_name', 'label_propagation');
```

### Time Series Functions

```sql
-- Downsample time series data
SELECT * FROM DOWNSAMPLE(
    (SELECT timestamp, value FROM metrics),
    '1 hour',
    'avg'
);

-- Fill gaps in time series
SELECT * FROM GAP_FILL(
    (SELECT timestamp, value FROM metrics),
    '5 minutes',
    'linear'
);

-- Calculate moving average
SELECT * FROM MOVING_AVERAGE(
    (SELECT timestamp, value FROM metrics),
    10
);

-- Forecast future values
SELECT * FROM FORECAST(
    (SELECT timestamp, value FROM metrics),
    24
);

-- Detect anomalies
SELECT * FROM DETECT_ANOMALIES(
    (SELECT timestamp, value FROM metrics),
    3.0
);
```

### Vector Search Functions

```sql
-- Search for k nearest vectors
SELECT * FROM VECTOR_SEARCH('index_name', [0.1, 0.2, 0.3], 10);

-- Calculate cosine distance
SELECT COSINE_DISTANCE([1.0, 2.0], [2.0, 3.0]);

-- Calculate Euclidean distance
SELECT EUCLIDEAN_DISTANCE([1.0, 2.0], [2.0, 3.0]);

-- Generate text embedding
SELECT EMBEDDING('Hello world', 'default');
```

### Data Quality Functions

```sql
-- Validate data against rules
SELECT * FROM VALIDATE('table_name', 'not_null:col1,range:col2:0:100');

-- Profile table data
SELECT * FROM PROFILE('table_name');

-- Get quality score
SELECT QUALITY_SCORE('table_name');
```

### Natural Language Functions

```sql
-- Convert natural language to SQL
SELECT * FROM NL_QUERY('Show top 10 customers by revenue');

-- With schema context
SELECT * FROM NL_QUERY('Count orders per day', 'analytics.orders');
```

### Catalog Functions

```sql
-- Search data catalog
SELECT * FROM SEARCH_CATALOG('customer');

-- Get data lineage
SELECT * FROM DATA_LINEAGE('table_name', 'upstream');
```

### Audit Functions

```sql
-- Get audit log for table
SELECT * FROM AUDIT_LOG('table_name');

-- Verify blockchain integrity
SELECT VERIFY_CHAIN('ledger_name');
```

### Workflow Functions

```sql
-- Execute workflow
SELECT RUN_WORKFLOW('workflow_id', '{"param": "value"}');

-- Get workflow status
SELECT * FROM WORKFLOW_STATUS('run_id');
```

### ML Functions

```sql
-- Run prediction
SELECT * FROM PREDICT('model_name', features_table);

-- Explain prediction
SELECT * FROM EXPLAIN_PREDICTION('model_name', features_table);
```

---

## Notes

- SQL keywords are case-insensitive
- Identifiers (table/column names) are case-sensitive
- String literals use single quotes: `'hello'`
- Use double quotes for identifiers with special characters: `"my-table"`
- Comments: `--` for single line, `/* */` for multi-line
