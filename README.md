# BoyoDB

[![Build Status](https://github.com/Izi-Technologies/boyodb/workflows/CI/badge.svg)](https://github.com/Izi-Technologies/boyodb/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A high-performance columnar database engine built in Rust for real-time analytics, time-series data, and high-throughput OLAP workloads.

## Why BoyoDB?

- **Blazing Fast**: Vectorized query execution with SIMD optimizations via Apache Arrow
- **Real-Time Analytics**: Sub-second queries on billions of rows
- **Horizontally Scalable**: Built-in clustering with automatic failover
- **Cloud-Native**: S3/object storage integration, tiered storage, data lake formats
- **Production Ready**: RBAC, TLS, audit logging, resource governance
- **Developer Friendly**: PostgreSQL-compatible CLI, multiple language drivers

## Quick Start

```bash
# Build from source
cargo build --release

# Start the server
./target/release/boyodb-server /data 0.0.0.0:8765

# Connect with the interactive shell
./target/release/boyodb-cli shell --host localhost:8765
```

```sql
-- Create a database and table
CREATE DATABASE analytics;
CREATE TABLE analytics.events (
    event_id INT64,
    timestamp TIMESTAMP,
    user_id INT64,
    event_type STRING,
    properties JSON
);

-- Insert data
INSERT INTO analytics.events VALUES
    (1, NOW(), 42, 'page_view', '{"page": "/home"}'),
    (2, NOW(), 43, 'click', '{"button": "signup"}');

-- Query with aggregations
SELECT event_type, COUNT(*) as cnt
FROM analytics.events
GROUP BY event_type
ORDER BY cnt DESC;
```

## Documentation

| Document | Description |
|----------|-------------|
| [User Guide](docs/USER_GUIDE.md) | Comprehensive guide to all features |
| [Quick Reference](docs/QUICK_REFERENCE.md) | Cheat sheet for common operations |
| [Clustering Guide](docs/CLUSTERING.md) | High availability and cluster setup |
| [SQL Reference](docs/SQL.md) | Complete SQL syntax and examples |
| [CLI Reference](docs/CLI.md) | Command-line interface guide |
| [Security Guide](docs/SECURITY.md) | Authentication, authorization, TLS |
| [API Reference](docs/API.md) | Server protocol and operations |

## Features

### SQL Support

```sql
-- Full SELECT with JOINs, subqueries, CTEs, window functions
SELECT
    u.name,
    COUNT(o.id) as orders,
    SUM(o.total) as revenue,
    RANK() OVER (ORDER BY SUM(o.total) DESC) as rank
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY revenue DESC
LIMIT 100;

-- Upserts with ON CONFLICT
INSERT INTO users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice')
ON CONFLICT (id) DO UPDATE SET name = 'Alice Smith';

-- Transactions with savepoints
BEGIN;
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- Materialized views
CREATE MATERIALIZED VIEW daily_stats AS
SELECT DATE_TRUNC('day', ts) as day, COUNT(*)
FROM events GROUP BY 1;
REFRESH MATERIALIZED VIEW daily_stats;
```

**Supported Operations:**
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `UPSERT`
- DDL: `CREATE/DROP/ALTER` for databases, tables, indexes, views
- Joins: `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`
- Window Functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`
- CTEs (Common Table Expressions)
- Subqueries (scalar, IN, EXISTS, correlated)
- Set Operations: `UNION`, `INTERSECT`, `EXCEPT`
- Table Sampling: `TABLESAMPLE BERNOULLI/SYSTEM/RESERVOIR`
- Prepared Statements

### Data Types

| Type | Description |
|------|-------------|
| `INT8/16/32/64` | Signed integers |
| `UINT8/16/32/64` | Unsigned integers |
| `FLOAT32/64` | Floating point |
| `DECIMAL(p,s)` | Fixed-point decimal |
| `STRING/VARCHAR/TEXT` | UTF-8 strings |
| `BOOLEAN` | True/false |
| `DATE` | Calendar date |
| `TIMESTAMP` | Microsecond precision |
| `UUID` | 128-bit identifier |
| `JSON/JSONB` | JSON documents |
| `BINARY/BLOB` | Binary data |
| `ARRAY<T>` | Typed arrays |
| `MAP<K,V>` | Key-value maps |

### Indexes

```sql
-- B-tree index for range queries
CREATE INDEX idx_timestamp ON events (timestamp);

-- Hash index for equality lookups
CREATE INDEX idx_user_id ON events (user_id) USING HASH;

-- Unique constraint
CREATE UNIQUE INDEX idx_email ON users (email);

-- Composite index
CREATE INDEX idx_user_time ON events (user_id, timestamp);
```

Index types: `BTREE`, `HASH`, `BLOOM`, `BITMAP`

### Interactive CLI Shell

PostgreSQL/MySQL-compatible shell with full editing support:

```bash
boyodb-cli shell --host localhost:8765 --user admin --database mydb
```

**Shell Features:**
- Tab completion for SQL keywords, tables, columns
- Multi-line query editing
- Command history with search (Ctrl+R)
- Output formats: table, CSV, JSON, vertical
- Meta-commands: `\l`, `\dt`, `\d`, `\du`, `\di`, `\x`, `\timing`
- Query timing and statistics
- Execute from file: `\i script.sql`
- Export results: `\o output.csv`

### High Availability & Clustering

```bash
# Three-node cluster
# Node 1 (seed)
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node1 \
    --gossip-addr 0.0.0.0:8766

# Node 2
boyodb-server /data/node2 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node2 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1:8766"

# Node 3
boyodb-server /data/node3 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node3 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1:8766"
```

**Cluster Features:**
- SWIM-based gossip protocol for node discovery
- Raft-lite leader election with lease-based leadership
- Automatic failover (sub-second leader election)
- Two-node mode for simplified HA deployments
- Quorum-based writes with configurable consistency
- Read replicas for horizontal read scaling

### Security

```sql
-- User management
CREATE USER alice WITH PASSWORD 'secure-password';
ALTER USER alice SET DEFAULT DATABASE analytics;

-- Role-based access control
CREATE ROLE analyst;
GRANT SELECT ON DATABASE analytics TO analyst;
GRANT SELECT, INSERT ON TABLE analytics.reports TO analyst;
GRANT analyst TO alice;

-- View permissions
SHOW GRANTS FOR alice;
```

**Security Features:**
- Username/password authentication (Argon2id hashing)
- Token-based authentication
- Role-based access control (RBAC)
- TLS/mTLS encryption
- Row-level security policies
- Audit logging
- Rate limiting and brute-force protection
- User locking/unlocking

### Backup & Point-in-Time Recovery

```sql
-- Create backup
CREATE BACKUP 'daily-2024-01-15';

-- List backups
SHOW BACKUPS;

-- WAL status
SHOW WAL STATUS;

-- Recover to specific point
RECOVER TO TIMESTAMP '2024-01-15T14:30:00';
RECOVER TO LSN 1234567890;
```

### Data Import/Export

```sql
-- Import CSV
COPY events FROM '/path/to/data.csv'
WITH (FORMAT CSV, HEADER true);

-- Import Parquet
COPY events FROM '/path/to/data.parquet'
WITH (FORMAT PARQUET);

-- Export to JSON
COPY (SELECT * FROM events WHERE date > '2024-01-01')
TO '/path/to/export.json'
WITH (FORMAT JSON);
```

```bash
# CLI import
boyodb-cli import --host localhost:8765 \
    --table mydb.events \
    --input data.csv \
    --format csv

# CLI export
boyodb-cli export --host localhost:8765 \
    --query "SELECT * FROM events" \
    --output results.parquet \
    --format parquet
```

Supported formats: CSV, JSON, Parquet, Arrow IPC

### Storage Engine

- **Columnar Storage**: Apache Arrow-based format for cache-efficient analytics
- **Compression**: Zstd, LZ4, Snappy with dictionary encoding
- **Write-Ahead Log**: Durability with configurable fsync policies
- **Tiered Storage**: Hot (SSD) → Warm (HDD) → Cold (S3)
- **Bloom Filters**: Fast segment pruning
- **Deduplication**: Configurable key-based deduplication

### Resource Governance

- Memory pools with hierarchical limits
- I/O scheduling with priority
- Concurrent query throttling
- Connection pooling with idle timeout
- Query admission control

### Observability

```bash
# Prometheus metrics
boyodb-cli metrics --host localhost:8765 --format json

# Cluster status
boyodb-cli cluster-status --host localhost:8765

# Query analysis
EXPLAIN ANALYZE SELECT * FROM events WHERE user_id = 100;
```

**Metrics:**
- Query throughput and latency histograms
- Connection counts and pool utilization
- Segment and cache statistics
- Replication lag
- Resource usage (memory, I/O)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                           │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │   Go    │ │  Python │ │  Rust   │ │  Node   │  ...      │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘           │
└───────┼──────────┼──────────┼──────────┼───────────────────┘
        └──────────┴──────────┴──────────┘
                       │
              ┌────────▼────────┐
              │   TCP/TLS API   │
              │  (JSON + Arrow) │
              └────────┬────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    BoyoDB Server                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Parser    │──│  Optimizer  │──│  Executor   │         │
│  │   (SQL)     │  │  (CBO)      │  │ (Vectorized)│         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │    Auth     │  │   Cluster   │  │  Resource   │         │
│  │  Manager    │  │   Manager   │  │  Governor   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Storage Engine                           │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │    WAL    │  │  Segments │  │  Indexes  │               │
│  │  (Append) │  │  (Arrow)  │  │ (B-tree)  │               │
│  └───────────┘  └───────────┘  └───────────┘               │
│                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │   Hot     │  │   Warm    │  │   Cold    │               │
│  │  (SSD)    │  │  (Zstd)   │  │   (S3)    │               │
│  └───────────┘  └───────────┘  └───────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Client Drivers

Native drivers available for multiple languages:

| Driver | Connection Pool | Batch Insert | Transactions | TLS |
|--------|----------------|--------------|--------------|-----|
| **Rust** | ✓ | ✓ | ✓ | ✓ |
| **Go** | ✓ | ✓ | ✓ | ✓ |
| **Python** | ✓ | ✓ | ✓ | ✓ |
| **Node.js** | ✓ | ✓ | ✓ | ✓ |
| **C#** | ✓ | - | ✓ | ✓ |
| **PHP** | - | - | ✓ | ✓ |

### Go
```go
import "github.com/Izi-Technologies/boyodb/drivers/go/boyodb"

client, _ := boyodb.NewClient("localhost:8765", nil)
defer client.Close()

// Query
result, _ := client.Query("SELECT * FROM events LIMIT 10")
for result.Next() {
    var id int64
    var name string
    result.Scan(&id, &name)
}

// Transaction
tx, _ := client.Begin()
tx.Execute("INSERT INTO events VALUES (1, 'test')")
tx.Commit()
```

### Python
```python
from boyodb import Client

with Client("localhost:8765") as client:
    # Query
    result = client.query("SELECT * FROM events LIMIT 10")
    for row in result:
        print(row)

    # Transaction
    with client.transaction() as tx:
        tx.execute("INSERT INTO events VALUES (1, 'test')")
```

### Rust
```rust
use boyodb::{Client, Config, Pool, PoolConfig};

// Connection pooling
let pool = Pool::new("localhost:8765", PoolConfig::new(20)).await?;
let conn = pool.get().await?;

// Query
let result = conn.query("SELECT * FROM events").await?;

// Transaction
let tx = conn.begin().await?;
tx.execute("INSERT INTO events VALUES (1, 'test')").await?;
tx.commit().await?;
```

### Node.js
```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();

// Query
const result = await client.query('SELECT * FROM events LIMIT 10');

// Transaction
const tx = await client.begin();
await tx.execute("INSERT INTO events VALUES (1, 'test')");
await tx.commit();
```

## Server Configuration

### Command Line Options

```bash
boyodb-server <data_dir> [bind_addr] [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--max-connections` | 64 | Maximum concurrent connections |
| `--workers` | CPU cores | Worker threads |
| `--idle-timeout` | 300 | Idle connection timeout (seconds) |
| `--wal-max-bytes` | 256MB | Maximum WAL file size |
| `--query-cache-bytes` | 64MB | Query result cache size |
| `--segment-cache` | 100 | Cached segments |
| `--tls-cert` / `--tls-key` | - | TLS certificate and key |
| `--auth` | - | Enable user authentication |
| `--cluster` | - | Enable cluster mode |
| `--cluster-id` | - | Cluster identifier |
| `--gossip-addr` | - | Gossip protocol address |
| `--seed-nodes` | - | Initial cluster seed nodes |
| `--two-node-mode` | - | Enable two-node HA mode |

### Configuration File (~/.boyodbrc)

```toml
host = "localhost:8765"
user = "admin"
database = "analytics"
tls = true
format = "table"
timeout_ms = 30000
```

## Performance

| Metric | Performance |
|--------|-------------|
| Scan throughput | 1+ GB/s per core |
| Aggregation | 100M+ rows/sec |
| Compression ratio | 5-10x typical |
| Query latency | Sub-second on TB datasets |
| Ingestion | 500K+ rows/sec |

## Building from Source

### Prerequisites
- Rust 1.75+ (stable)
- Cargo

### Build Commands

```bash
# Build release binaries
cargo build --release

# Run tests
cargo test

# Build documentation
cargo doc --open
```

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Run tests
cargo test

# Run lints
cargo clippy

# Format code
cargo fmt
```

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Community

- [GitHub Issues](https://github.com/Izi-Technologies/boyodb/issues) - Bug reports and feature requests
- [Discussions](https://github.com/Izi-Technologies/boyodb/discussions) - Questions and community chat

---

Built with Rust and Apache Arrow
