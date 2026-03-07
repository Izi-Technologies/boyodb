# boyodb Python Driver

A Python client library for connecting to boyodb-server.

## Installation

```bash
pip install boyodb
```

## Quick Start

```python
from boyodb import Client

# Connect to the server
client = Client("localhost:8765")
client.connect()

# Execute a query
result = client.query("SELECT * FROM mydb.users LIMIT 10")
for row in result:
    print(row)

# Close the connection
client.close()
```

## Using Context Manager

```python
from boyodb import Client

with Client("localhost:8765") as client:
    result = client.query("SELECT COUNT(*) FROM events")
    print(f"Count: {result[0]['count']}")
```

## Connection Pooling (High Performance)

For concurrent access and better performance, use the pooled client:

```python
from boyodb import PooledClient, PoolConfig

# Configure connection pool
config = PoolConfig(
    host="localhost",
    port=8765,
    pool_size=20,        # Number of connections in pool
    database="analytics",
    query_timeout=60000,
)

# Create pooled client
client = PooledClient(config)

# Thread-safe concurrent queries
result = client.query("SELECT COUNT(*) FROM events")
print(result)

# Multiple threads can use the same client
import concurrent.futures

def run_query(query_id):
    result = client.query(f"SELECT * FROM events WHERE id = {query_id}")
    return len(result)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(run_query, i) for i in range(100)]
    for future in concurrent.futures.as_completed(futures):
        print(f"Rows: {future.result()}")

client.close()
```

### Pool Configuration

```python
from boyodb import PoolConfig

config = PoolConfig(
    host="localhost",
    port=8765,
    pool_size=20,              # Connections in pool
    pool_timeout=30.0,         # Timeout waiting for connection
    tls=True,                  # Enable TLS
    ca_file="/path/to/ca.pem", # CA certificate
    token="auth-token",        # Authentication token
    database="mydb",           # Default database
    query_timeout=60000,       # Query timeout (ms)
    max_retries=3,             # Connection retries
)
```

## Configuration

```python
from boyodb import Client, Config

config = Config(
    tls=True,
    ca_file="/path/to/ca.pem",
    token="your-auth-token",
    database="mydb",
    query_timeout=60000,  # 60 seconds
)

client = Client("localhost:8765", config)
client.connect()
```

## Authentication

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

# Login with username and password
client.login("admin", "password")

# Execute authenticated queries
result = client.query("SELECT * FROM sensitive_data")

# Logout
client.logout()
client.close()
```

## Prepared Statements

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

prepared_id = client.prepare("SELECT * FROM calls WHERE tenant_id = 42")
result = client.execute_prepared_binary(prepared_id)
print(f"Rows: {len(result.rows)}")

client.close()
```

## Database Operations

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

# Create a database
client.create_database("analytics")

# Create a table
client.create_table("analytics", "events")

# List databases
databases = client.list_databases()
print("Databases:", databases)

# List tables
tables = client.list_tables("analytics")
for table in tables:
    print(f"Table: {table.database}.{table.name}")

client.close()
```

## CSV Ingestion

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

# Ingest CSV data
csv_data = """id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
"""

client.ingest_csv("mydb", "users", csv_data, has_header=True)

client.close()
```

## Supported Data Types

BoyoDB supports the following data types in SQL queries:

### Basic Types

| SQL Type | Python Type | Description |
|----------|-------------|-------------|
| `INT64`, `BIGINT` | `int` | 64-bit signed integer |
| `INT32`, `INT`, `INTEGER` | `int` | 32-bit signed integer |
| `INT16`, `SMALLINT` | `int` | 16-bit signed integer |
| `STRING`, `VARCHAR`, `TEXT` | `str` | UTF-8 string |
| `BOOLEAN`, `BOOL` | `bool` | True/False |
| `FLOAT64`, `DOUBLE` | `float` | 64-bit floating point |
| `FLOAT32`, `FLOAT` | `float` | 32-bit floating point |

### Advanced Types

| SQL Type | Python Type | Description |
|----------|-------------|-------------|
| `UUID` | `str` | UUID in standard format (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `JSON` | `str` | JSON document (stored as string) |
| `DECIMAL(p,s)` | `str` | Decimal number with precision and scale |
| `DATE` | `str` | Date in `YYYY-MM-DD` format |
| `BINARY`, `BLOB` | `str` | Binary data (returned as hex string) |
| `TIMESTAMP` | `int` | Unix timestamp in microseconds |

### Example Usage

```python
# Create a table with various types
client.exec("""
    CREATE TABLE mydb.products (
        id UUID,
        name STRING,
        price DECIMAL(10,2),
        metadata JSON,
        created_at DATE
    )
""")

# Insert data
client.exec("""
    INSERT INTO mydb.products VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        'Widget',
        '19.99',
        '{"color": "blue"}',
        '2024-01-15'
    )
""")

# Query with type casting
result = client.query("""
    SELECT id, name, CAST(price AS FLOAT64) as price_float
    FROM mydb.products
""")
```

## Query Execution Plan

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

plan = client.explain("SELECT * FROM users WHERE id > 100")
print("Query Plan:", plan)

client.close()
```

## Server Metrics

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

metrics = client.metrics()
print("Server Metrics:", metrics)

client.close()
```

## Error Handling

```python
from boyodb import Client, ConnectionError, QueryError, AuthError

try:
    client = Client("localhost:8765")
    client.connect()

    result = client.query("SELECT * FROM nonexistent_table")

except ConnectionError as e:
    print(f"Connection failed: {e}")
except QueryError as e:
    print(f"Query failed: {e}")
except AuthError as e:
    print(f"Authentication failed: {e}")
finally:
    client.close()
```

## API Reference

### Client

- `Client(host: str, config: Config = None)` - Create a new client
- `connect()` - Connect to the server
- `close()` - Close the connection
- `health()` - Check server health
- `login(username: str, password: str)` - Login with credentials
- `logout()` - Logout from server
- `query(sql: str, database: str = None, timeout: int = None)` - Execute query
- `exec(sql: str, database: str = None, timeout: int = None)` - Execute statement
- `create_database(name: str)` - Create database
- `create_table(database: str, table: str)` - Create table
- `list_databases()` - List all databases
- `list_tables(database: str = None)` - List tables
- `explain(sql: str)` - Get query plan
- `metrics()` - Get server metrics
- `ingest_csv(database, table, csv_data, has_header, delimiter)` - Ingest CSV
- `ingest_ipc(database, table, ipc_data)` - Ingest Arrow IPC

### Config

- `tls: bool` - Enable TLS (default: False)
- `ca_file: str` - Path to CA certificate
- `insecure_skip_verify: bool` - Skip TLS verification (default: False)
- `connect_timeout: float` - Connection timeout in seconds (default: 10)
- `read_timeout: float` - Read timeout in seconds (default: 30)
- `write_timeout: float` - Write timeout in seconds (default: 10)
- `token: str` - Authentication token
- `max_retries: int` - Max connection retries (default: 3)
- `retry_delay: float` - Retry delay in seconds (default: 1)
- `database: str` - Default database
- `query_timeout: int` - Query timeout in milliseconds (default: 30000)

### QueryResult

- `rows: List[Dict[str, Any]]` - List of row dictionaries
- `columns: List[str]` - Column names
- `row_count: int` - Number of rows
- `segments_scanned: int` - Segments scanned
- `data_skipped_bytes: int` - Bytes skipped by pruning

## License

Apache-2.0
