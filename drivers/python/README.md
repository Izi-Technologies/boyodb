# boyodb Python Driver

[![Version](https://img.shields.io/badge/version-0.9.6-green.svg)](../../CHANGELOG.md)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

A comprehensive Python client for BoyoDB with support for transactions, async inserts, pub/sub, CDC, analytics, and more.

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

## Connection Pooling

For concurrent access and better performance:

```python
from boyodb import PooledClient, PoolConfig

config = PoolConfig(
    host="localhost",
    port=8765,
    pool_size=20,
    database="analytics",
)

client = PooledClient(config)

# Thread-safe concurrent queries
import concurrent.futures

def run_query(query_id):
    return client.query(f"SELECT * FROM events WHERE id = {query_id}")

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(run_query, i) for i in range(100)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())

client.close()
```

## Transactions

ACID transactions with savepoints:

```python
from boyodb import Client, IsolationLevel

with Client("localhost:8765") as client:
    # Basic transaction
    with client.transaction() as tx:
        tx.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        tx.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
        # Auto-commits on success, rolls back on exception

    # With isolation level and savepoints
    with client.transaction(isolation_level=IsolationLevel.SERIALIZABLE) as tx:
        tx.execute("INSERT INTO orders (id, user_id) VALUES (1, 1)")
        tx.savepoint("sp1")
        try:
            tx.execute("INSERT INTO order_items (order_id) VALUES (1)")
        except:
            tx.rollback_to("sp1")
        # Transaction continues
```

## Async Inserts (High-Throughput)

Buffer inserts for batch efficiency:

```python
from boyodb import Client, AsyncInsertBuffer, AsyncInsertConfig

client = Client("localhost:8765")
client.connect()

config = AsyncInsertConfig(
    max_rows=10000,
    max_bytes=10 * 1024 * 1024,  # 10MB
    max_wait_seconds=0.2,
    deduplicate=True,
    dedup_columns=["id"],
)

buffer = AsyncInsertBuffer(client, "mydb.events", config)
buffer.start()

# Add rows - automatically batched
for i in range(100000):
    buffer.insert({"id": i, "event": "click", "value": i * 10})

# Flush remaining and stop
buffer.stop()
print(buffer.stats)  # {'total_rows': 100000, 'total_flushes': 10, ...}
```

## Pub/Sub Messaging

Real-time messaging:

```python
from boyodb import Client, Subscriber, Publisher

client = Client("localhost:8765")
client.connect()

# Subscribe to channels
def on_message(msg):
    print(f"[{msg.channel}] {msg.payload}")

sub = Subscriber(client, ["events", "alerts"], on_message)
sub.start()

# Publish messages
pub = client.publisher
pub.publish("events", {"type": "user_signup", "user_id": 123})
pub.publish("alerts", {"level": "warning", "message": "High CPU usage"})

# Cleanup
sub.stop()
```

## CDC (Change Data Capture)

Subscribe to table changes:

```python
from boyodb import Client, CDCSubscriber, ChangeType

client = Client("localhost:8765")
client.connect()

def on_change(event):
    print(f"{event.change_type.value}: {event.after}")

cdc = CDCSubscriber(
    client,
    "mydb.users",
    on_change,
    include_before=True,
    change_types=[ChangeType.INSERT, ChangeType.UPDATE],
)
cdc.start()

# Make changes in another session...
# INSERT INTO mydb.users VALUES (1, 'Alice')
# -> INSERT: {'id': 1, 'name': 'Alice'}

cdc.stop()
```

## Approximate Analytics

Fast approximate aggregations:

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

# HyperLogLog distinct count
approx = client.approximate
estimate = approx.count_distinct("events", "user_id")
print(f"Unique users: ~{estimate.estimate} (±{estimate.relative_error*100:.1f}%)")

# T-Digest percentiles
percentiles = approx.percentile("orders", "amount", [50, 90, 99])
for p in percentiles:
    print(f"P{int(p.quantile)}: {p.value}")

# Top-K items
top_events = approx.top_k("events", "event_type", k=5)
for item in top_events:
    print(f"{item.item}: {item.count}")
```

## Time Series Analytics

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

ts = client.timeseries

# Aggregate by time buckets
hourly = ts.aggregate_by_time(
    "metrics",
    time_column="timestamp",
    value_column="cpu_usage",
    bucket="1 hour",
    aggregation="avg",
)

# Moving average
ma = ts.moving_average(
    "metrics",
    time_column="timestamp",
    value_column="value",
    window_size=7,
)

# Anomaly detection
anomalies = ts.detect_anomalies(
    "metrics",
    time_column="timestamp",
    value_column="value",
    threshold_stddev=3.0,
)
```

## Graph Analytics

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

graph = client.graph

# Shortest path
path = graph.shortest_path(
    "edges",
    source_column="from_node",
    target_column="to_node",
    weight_column="weight",
    start_node=1,
    end_node=10,
)

# PageRank
rankings = graph.pagerank(
    "edges",
    source_column="from_node",
    target_column="to_node",
    top_k=10,
)

# Connected components
components = graph.connected_components(
    "edges",
    source_column="from_node",
    target_column="to_node",
)
```

## External Tables (S3, URLs, Files)

Query external data without importing:

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

ext = client.external

# Query S3
results = ext.query_s3(
    "s3://my-bucket/data/events.parquet",
    "SELECT event_type, COUNT(*) as cnt FROM data GROUP BY event_type",
)

# Query HTTP URL
results = ext.query_url(
    "https://example.com/data.csv",
    "SELECT * FROM data WHERE value > 100",
)

# Query local file
results = ext.query_file("/data/logs.json", limit=1000)

# Create named external table
ext.create_external_table(
    "events_s3",
    source_type="s3",
    location="s3://bucket/events/",
    format="parquet",
    columns=[
        {"name": "id", "type": "INT64"},
        {"name": "event", "type": "STRING"},
        {"name": "ts", "type": "TIMESTAMP"},
    ],
    partition_columns=["date"],
)
```

## Vector Similarity Search

```python
from boyodb import Client

client = Client("localhost:8765")
client.connect()

vec = client.vector

# Create vector index
vec.create_index(
    "documents",
    column="embedding",
    distance_metric="cosine",
    m=16,
    ef_construction=200,
)

# Search for similar vectors
query_embedding = [0.1, 0.2, 0.3, ...]  # Your query vector
results = vec.search(
    "documents",
    column="embedding",
    query_vector=query_embedding,
    k=10,
    where="category = 'tech'",
)

# Hybrid search (vector + full-text)
results = vec.hybrid_search(
    "documents",
    vector_column="embedding",
    text_column="content",
    query_vector=query_embedding,
    query_text="machine learning tutorial",
    k=10,
    vector_weight=0.7,
    text_weight=0.3,
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
    query_timeout=60000,
    connect_timeout=10.0,
    read_timeout=30.0,
    write_timeout=10.0,
    max_retries=3,
    retry_delay=1.0,
)

client = Client("localhost:8765", config)
```

## Error Handling

```python
from boyodb import Client, ConnectionError, AuthError, QueryError, TimeoutError

try:
    with Client("localhost:8765") as client:
        client.login("user", "password")
        result = client.query("SELECT * FROM table")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except AuthError as e:
    print(f"Authentication failed: {e}")
except QueryError as e:
    print(f"Query failed: {e}")
except TimeoutError as e:
    print(f"Operation timed out: {e}")
```

## License

Apache 2.0
