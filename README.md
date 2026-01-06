# BoyoDB

[![Build Status](https://github.com/loreste/boyodb/workflows/CI/badge.svg)](https://github.com/loreste/boyodb/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A high-performance columnar database engine built in Rust for real-time analytics, time-series data, and high-throughput OLAP workloads.

## Why BoyoDB?

- **Blazing Fast**: Vectorized query execution with SIMD optimizations via Apache Arrow
- **Real-Time Analytics**: Sub-second queries on billions of rows
- **Horizontally Scalable**: Built-in clustering with automatic failover
- **Cloud-Native**: S3/object storage integration, tiered storage, data lake formats
- **Production Ready**: RBAC, TLS, audit logging, resource governance
- **Developer Friendly**: SQL interface, multiple language drivers, interactive shell

## Quick Start

```bash
# Build from source
cargo build --release

# Start the server
./target/release/boyodb-server /data 0.0.0.0:8765

# Connect with the interactive shell
./target/release/boyodb-cli shell --host localhost:8765

# Or use the bsql convenience script
./scripts/bsql -H localhost:8765
```

```sql
-- Create a database and table
CREATE DATABASE analytics;
CREATE TABLE analytics.events (
    event_id UUID,
    timestamp TIMESTAMP,
    user_id INT64,
    event_type STRING,
    properties JSON
);

-- Insert data
INSERT INTO analytics.events VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    1705312800000000,
    42,
    'page_view',
    '{"page": "/home", "duration": 1200}'
);

-- Query with aggregations
SELECT event_type, COUNT(*) as cnt, AVG(user_id) as avg_user
FROM analytics.events
WHERE timestamp > 1705000000000000
GROUP BY event_type
ORDER BY cnt DESC
LIMIT 10;
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/GETTING_STARTED.md) | Installation and basic usage tutorial |
| [Architecture](ARCHITECTURE.md) | System design, components, and data flow |
| [SQL Reference](docs/SQL.md) | Complete SQL syntax and examples |
| [CLI Reference](docs/CLI.md) | Command-line interface and shell guide |
| [API Reference](docs/API.md) | Server protocol and operations |
| [Security Guide](docs/SECURITY.md) | Authentication, authorization, encryption |
| [Contributing](CONTRIBUTING.md) | Development setup and PR process |

## Features

### Core Engine
- **Columnar Storage**: Apache Arrow-based columnar format for cache-efficient analytics
- **Vectorized Execution**: SIMD-optimized query processing
- **Compression**: Zstd, LZ4, Snappy with dictionary and delta encoding
- **Bloom Filters**: Probabilistic filtering for fast segment pruning
- **Write-Ahead Log**: Durability with configurable fsync policies

### SQL Support
- Full DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- DDL: `CREATE/DROP/ALTER DATABASE/TABLE`
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT DISTINCT`
- `GROUP BY` with multiple columns
- `ORDER BY`, `LIMIT`, `OFFSET`
- `JOIN` (INNER, LEFT, RIGHT, FULL, CROSS)
- Window Functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`
- Common Table Expressions (CTEs)
- Subqueries and correlated subqueries
- `UNION`, `INTERSECT`, `EXCEPT`
- Prepared statements

### Data Types
| Type | Description |
|------|-------------|
| `INT8/16/32/64` | Signed integers |
| `UINT8/16/32/64` | Unsigned integers |
| `FLOAT32/64` | Floating point |
| `DECIMAL(p,s)` | Fixed-point decimal |
| `STRING/VARCHAR` | UTF-8 strings |
| `BOOLEAN` | True/false |
| `DATE` | Calendar date |
| `TIMESTAMP` | Microsecond precision |
| `UUID` | 128-bit identifier |
| `JSON/JSONB` | JSON documents |
| `BINARY/BLOB` | Binary data |
| `ARRAY<T>` | Typed arrays |
| `MAP<K,V>` | Key-value maps |

### High Availability
- **Gossip Protocol**: SWIM-based node discovery and failure detection
- **Leader Election**: Raft-lite with lease-based leadership
- **Automatic Failover**: Sub-second leader election on failure
- **Quorum Writes**: Configurable consistency levels
- **Read Replicas**: Scale reads horizontally

```bash
# Start a 3-node cluster
boyodb-server /data 0.0.0.0:8765 \
  --cluster \
  --cluster-id prod \
  --gossip-addr 0.0.0.0:8766 \
  --seed-nodes "node2:8766,node3:8766"
```

### Security
- **Authentication**: Username/password with Argon2id hashing
- **Authorization**: Role-based access control (RBAC)
- **Row-Level Security**: Fine-grained data access policies
- **Column Encryption**: Transparent column-level encryption
- **TLS/mTLS**: Encrypted connections with client certificates
- **Audit Logging**: Comprehensive security event tracking
- **Rate Limiting**: Protection against brute force attacks

### Resource Governance
- **Memory Pools**: Hierarchical memory allocation with limits
- **I/O Scheduling**: Priority-based I/O with rate limiting
- **Workload Isolation**: Resource groups with CPU/memory quotas
- **Query Throttling**: Concurrent query limits per user/group
- **Admission Control**: Query queue management

### Advanced Features
- **Materialized Views**: Automatic refresh with CDC support
- **Streaming Ingestion**: Kafka and Pulsar consumers
- **Data Lake Integration**: Delta Lake-style transactions
- **Native Parquet/ORC**: Read and write industry formats
- **External Tables**: Query S3, GCS, Azure Blob directly
- **Full-Text Search**: Inverted indexes with BM25 ranking
- **Geospatial**: Point-in-polygon, distance calculations
- **Time-Series**: Downsampling, gap filling, interpolation

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                           │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │   Go    │ │  Python │ │  Rust   │ │  Node   │  ...      │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘           │
└───────┼──────────┼──────────┼──────────┼───────────────────┘
        │          │          │          │
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
│  │   Parser    │  │  Optimizer  │  │  Executor   │         │
│  │   (SQL)     │──│  (Query)    │──│ (Vectorized)│         │
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
│  │    WAL    │  │  Segments │  │  Manifest │               │
│  │  (Append) │  │  (Arrow)  │  │  (Index)  │               │
│  └───────────┘  └───────────┘  └───────────┘               │
│                                                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │   Hot     │  │   Warm    │  │   Cold    │               │
│  │  (SSD)    │  │  (HDD)    │  │   (S3)    │               │
│  └───────────┘  └───────────┘  └───────────┘               │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
boyodb/
├── crates/
│   ├── boyodb-core/          # Core engine library
│   │   ├── src/
│   │   │   ├── engine.rs     # Main database engine
│   │   │   ├── sql.rs        # SQL parser
│   │   │   ├── auth.rs       # Authentication/authorization
│   │   │   ├── cluster/      # HA clustering
│   │   │   ├── compression.rs
│   │   │   ├── execution.rs  # Query execution
│   │   │   ├── high_availability.rs
│   │   │   ├── resource_governance.rs
│   │   │   └── ...
│   │   └── include/
│   │       └── boyodb.h      # C ABI header
│   ├── boyodb-server/        # TCP/TLS server
│   ├── boyodb-cli/           # Command-line interface
│   ├── boyodb-bench/         # Benchmarking suite
│   └── boyodb-ingestor/      # Bulk data ingestion tool
├── bindings/
│   ├── go/                   # Go bindings (CGO)
│   └── node/                 # Node.js bindings (NAPI)
├── drivers/
│   ├── go/                   # Pure Go driver
│   ├── python/               # Python driver
│   ├── rust/                 # Rust driver
│   ├── nodejs/               # Node.js driver
│   ├── csharp/               # C# driver
│   └── php/                  # PHP driver
└── docs/                     # Documentation
```

## Client Drivers

Native drivers for all major languages:

### Go
```go
import "github.com/loreste/boyodb/drivers/go/boyodb"

client, _ := boyodb.NewClient("localhost:8765", nil)
defer client.Close()

result, _ := client.Query("SELECT * FROM events LIMIT 10")
for result.Next() {
    var id int64
    var name string
    result.Scan(&id, &name)
}
```

### Python
```python
from boyodb import Client

with Client("localhost:8765") as client:
    result = client.query("SELECT * FROM events LIMIT 10")
    for row in result:
        print(row)
```

### Rust
```rust
use boyodb::{Client, Config};

let client = Client::connect("localhost:8765", Config::default()).await?;
let result = client.query("SELECT * FROM events LIMIT 10").await?;
```

### Node.js
```javascript
const { Client } = require('boyodb');

const client = new Client('localhost:8765');
await client.connect();
const result = await client.query('SELECT * FROM events LIMIT 10');
```

## Performance

BoyoDB is designed for high-performance analytics workloads:

| Metric | Performance |
|--------|-------------|
| Scan throughput | 1+ GB/s per core |
| Aggregation | 100M+ rows/sec |
| Compression ratio | 5-10x typical |
| Query latency | Sub-second on TB datasets |
| Ingestion | 500K+ rows/sec |

### Optimizations
- O(1) manifest index for segment lookup
- Parallel segment scanning with Rayon
- Predicate pushdown to storage layer
- Bloom filter-based segment pruning
- LRU caches for segments, batches, schemas
- Vectorized Arrow compute kernels
- SIMD string comparisons

## Configuration

### Server Options

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | 8 | Worker threads |
| `--max-conns` | 64 | Max connections |
| `--max-ipc-bytes` | 32MB | Max Arrow payload |
| `--segment-cache` | 8 | Cached segments |
| `--batch-cache-bytes` | 512MB | Batch cache size |
| `--tls-cert/--tls-key` | - | TLS certificate |
| `--token` | - | Auth token |

### Engine Configuration (Rust API)

```rust
let config = EngineConfig::default()
    .with_segment_cache_capacity(16)
    .with_segment_cache_bytes(8 * 1024 * 1024 * 1024)  // 8GB
    .with_enable_compaction(true)
    .with_auto_compact_interval_secs(300);

let db = Db::open_with_config("/data", config)?;
```

## Building from Source

### Prerequisites
- Rust 1.75+ (stable)
- Cargo
- (Optional) Node.js 18+ for Node bindings
- (Optional) Go 1.21+ for Go bindings

### Build Commands

```bash
# Build all crates
cargo build --release

# Run tests (601 tests)
cargo test

# Build with all features
cargo build --release --all-features

# Build documentation
cargo doc --open

# Build Node.js binding
cd bindings/node && npm install && npm run build

# Build Go binding
cd bindings/go && go build ./...
```

## Roadmap

- [x] Phase 1-12: Core engine, SQL, Auth, Streaming
- [x] Phase 13: Parallel query execution, spill-to-disk
- [x] Phase 14: Native Parquet/ORC, Data Lake integration
- [x] Phase 15: High availability, automatic failover
- [x] Phase 16: Resource governance, workload isolation
- [x] Phase 17: Tooling (boyodb-local, import/export)
- [ ] Phase 18: Distributed query execution
- [ ] Phase 19: GPU acceleration (DataFusion integration)
- [ ] Phase 20: Kubernetes operator

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

- [GitHub Issues](https://github.com/loreste/boyodb/issues) - Bug reports and feature requests
- [Discussions](https://github.com/loreste/boyodb/discussions) - Questions and community chat

---

Built with Rust and Apache Arrow
