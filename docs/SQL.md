# SQL Reference

Complete SQL reference for BoyoDB.

## Table of Contents

- [Data Types](#data-types)
- [DDL Statements](#ddl-statements)
- [DML Statements](#dml-statements)
- [Query Statements](#query-statements)
- [Operators](#operators)
- [Functions](#functions)
- [Window Functions](#window-functions)
- [Common Table Expressions](#common-table-expressions)
- [Prepared Statements](#prepared-statements)

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
```

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
```

**Examples:**
```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(amount), AVG(amount) FROM transactions;
SELECT MIN(price), MAX(price) FROM products WHERE category = 'electronics';
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
```

**Examples:**
```sql
SELECT UPPER(name) FROM users;
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;
SELECT SUBSTR(phone, 1, 3) AS area_code FROM users;
SELECT COALESCE(nickname, name) AS display_name FROM users;
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
NOW()               -- Current timestamp (microseconds)
CURRENT_DATE        -- Current date
CURRENT_TIMESTAMP   -- Current timestamp
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

## Utility Statements

### SHOW Statements

```sql
SHOW DATABASES;
SHOW TABLES;
SHOW TABLES IN database_name;
SHOW USERS;
SHOW ROLES;
SHOW GRANTS FOR username;
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

## Notes

- SQL keywords are case-insensitive
- Identifiers (table/column names) are case-sensitive
- String literals use single quotes: `'hello'`
- Use double quotes for identifiers with special characters: `"my-table"`
- Comments: `--` for single line, `/* */` for multi-line
