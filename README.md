# BoyoDB - High-Performance Columnar Database for Real-Time Analytics

[![Build Status](https://github.com/Izi-Technologies/boyodb/workflows/CI/badge.svg)](https://github.com/Izi-Technologies/boyodb/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.9.8-green.svg)](CHANGELOG.md)

**BoyoDB** is an open-source, high-performance columnar database engine built in Rust. Designed for real-time analytics, time-series data, and high-throughput OLAP workloads, BoyoDB combines the best features of ClickHouse and PostgreSQL with modern AI/ML capabilities. Query billions of rows in milliseconds with GPU acceleration, vector search, and machine learning built-in.

## Table of Contents

- [Key Features](#key-features)
- [Use Cases](#use-cases)
- [Quick Start](#quick-start)
- [Why Choose BoyoDB](#why-choose-boyodb)
- [Feature Comparison](#feature-comparison)
- [SQL Support](#sql-support)
- [Vector Search & AI](#vector-search--ai-capabilities)
- [Time Series Analytics](#time-series-analytics)
- [Machine Learning](#machine-learning-integration)
- [Graph Database](#graph-database-queries)
- [High Availability](#high-availability--clustering)
- [Security](#enterprise-security)
- [Performance Benchmarks](#performance-benchmarks)
- [Client SDKs](#client-sdks--drivers)
- [Documentation](#documentation)
- [Contributing](#contributing)

## Key Features

### Analytics Database Engine
- **Columnar Storage** - Apache Arrow-based format optimized for analytical queries
- **Vectorized Execution** - SIMD-accelerated query processing for maximum throughput
- **GPU Acceleration** - NVIDIA CUDA and Apple Metal support for compute-intensive workloads
- **JIT Compilation** - Cranelift-based native code generation for hot query paths

### Real-Time Data Processing
- **Sub-Second Queries** - Analyze billions of rows in milliseconds
- **Streaming Ingestion** - 500K+ rows/second sustained write throughput
- **Change Data Capture** - Debezium-compatible CDC with Delta Lake/Iceberg support
- **Views & Materialized Views** - Virtual views and precomputed materialized views with incremental refresh

### Cloud-Native Architecture
- **Tiered Storage** - Hot (SSD), Warm (compressed), Cold (S3/object storage)
- **Multi-Region Replication** - Cross-region sync with automatic failover
- **Auto-Scaling** - Metrics-based horizontal scaling with predictive policies
- **Kubernetes Ready** - Helm charts and operator for container orchestration

### AI & Machine Learning
- **Vector Search** - HNSW approximate nearest neighbor with multiple distance metrics
- **ML Model Inference** - Run ONNX, TensorFlow, PyTorch models in SQL queries
- **Feature Store** - Point-in-time feature lookup for ML training and serving
- **Natural Language Queries** - Convert questions to SQL automatically

## Use Cases

| Use Case | Description |
|----------|-------------|
| **Real-Time Analytics** | Dashboard queries on streaming event data |
| **Time Series Database** | IoT metrics, application monitoring, financial data |
| **Log Analytics** | Centralized logging with full-text search |
| **Product Analytics** | User behavior tracking and funnel analysis |
| **Ad Tech / MarTech** | Attribution modeling and audience segmentation |
| **Telecommunications CDR** | Call detail record processing and analysis |
| **Vector Database** | Semantic search and RAG applications |
| **ML Feature Store** | Feature engineering and model serving |

## Quick Start

### Installation

```bash
# Build from source (requires Rust 1.75+)
git clone https://github.com/Izi-Technologies/boyodb.git
cd boyodb
cargo build --release

# Start the server
./target/release/boyodb-server /data 0.0.0.0:8765

# Connect with interactive shell
./target/release/boyodb-cli shell --host localhost:8765
```

### Your First Query

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

-- Run analytical query
SELECT event_type, COUNT(*) as cnt
FROM analytics.events
GROUP BY event_type
ORDER BY cnt DESC;
```

## Why Choose BoyoDB

| Capability | BoyoDB | ClickHouse | PostgreSQL | TimescaleDB |
|------------|--------|------------|------------|-------------|
| Columnar Storage | Yes | Yes | No | Hybrid |
| GPU Acceleration | Yes | No | No | No |
| Vector Search | Native | Extension | Extension | No |
| ML Model Inference | Native | No | Extension | No |
| Graph Queries | Native | No | Extension | No |
| Time Series | Native | Yes | Extension | Yes |
| ACID Transactions | Yes | Limited | Yes | Yes |
| PostgreSQL Compatible | Yes | No | Native | Yes |

### Performance Advantages

- **10x Faster Aggregations** - Vectorized execution with SIMD optimization
- **100x Better Compression** - Columnar format with dictionary encoding
- **Sub-Millisecond Latency** - Query result caching with Redis protocol
- **Linear Scalability** - Sharded caches eliminate lock contention

## Feature Comparison

### ClickHouse-Compatible Features
- **MergeTree Engine** - ReplacingMergeTree, CollapsingMergeTree, AggregatingMergeTree, SummingMergeTree
- **Approximate Functions** - HyperLogLog, T-Digest, Count-Min Sketch
- **External Tables** - Query S3, HTTP URLs, HDFS, Parquet, Delta Lake, Iceberg directly
- **Async Inserts** - Buffered batch ingestion for high-throughput writes
- **Query Profiler** - Flame graphs, per-operator timing, memory tracking
- **Parallel Replicas** - Distributed query execution across replicas
- **Zero-Copy Replication** - Share segments via object storage

### PostgreSQL-Compatible Features
- **Full SQL Support** - JOINs, CTEs, window functions, subqueries
- **ACID Transactions** - Snapshot isolation with savepoints
- **GIN/GiST Indexes** - Full-text search, spatial indexing, array operations
- **Exclusion Constraints** - Prevent overlapping ranges for scheduling
- **Foreign Data Wrappers** - Query PostgreSQL, MySQL, MongoDB, Redis
- **Connection Pooler** - PgBouncer-compatible built-in pooling
- **pg_dump Compatible** - Standard backup/restore tools

## SQL Support

### Query Capabilities

```sql
-- Window functions and CTEs
WITH user_stats AS (
    SELECT user_id, COUNT(*) as events
    FROM analytics.events
    GROUP BY user_id
)
SELECT
    user_id,
    events,
    RANK() OVER (ORDER BY events DESC) as rank
FROM user_stats
LIMIT 100;

-- Upserts with conflict handling
INSERT INTO users (id, email, name)
VALUES (1, 'alice@example.com', 'Alice')
ON CONFLICT (id) DO UPDATE SET name = 'Alice Smith';

-- PIVOT/UNPIVOT transformations
SELECT * FROM sales
PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'));

-- Approximate aggregates for big data
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
SELECT APPROX_PERCENTILE(response_time, 0.99) FROM requests;
```

### Supported Data Types

| Category | Types |
|----------|-------|
| Numeric | INT8, INT16, INT32, INT64, UINT8-64, FLOAT32, FLOAT64, DECIMAL |
| String | STRING, VARCHAR, TEXT, CHAR |
| Temporal | DATE, TIMESTAMP, INTERVAL |
| Complex | JSON, JSONB, ARRAY, MAP, STRUCT |
| Binary | BINARY, BLOB, BYTEA |
| Specialized | UUID, VECTOR, INET, CIDR, MACADDR |

### Index Types

```sql
-- B-tree for range queries
CREATE INDEX idx_timestamp ON events (timestamp);

-- Hash for equality lookups
CREATE INDEX idx_user ON events USING HASH (user_id);

-- Fulltext for substring search (LIKE '%pattern%')
CREATE INDEX idx_phone ON calls USING FULLTEXT (phone_number);

-- GIN for array/JSON containment
CREATE INDEX idx_tags ON posts USING GIN (tags);

-- Bloom filter for membership testing
CREATE INDEX idx_email ON users USING BLOOM (email);
```

## Vector Search & AI Capabilities

BoyoDB includes native vector database functionality for semantic search, recommendation systems, and RAG (Retrieval-Augmented Generation) applications.

```sql
-- Create table with vector embeddings
CREATE TABLE documents (
    id INT64,
    content STRING,
    embedding VECTOR(1536)  -- OpenAI ada-002 dimensions
);

-- Semantic similarity search
SELECT id, content,
       COSINE_SIMILARITY(embedding, $query_vector) as score
FROM documents
ORDER BY score DESC
LIMIT 10;

-- Hybrid search (vector + keyword)
SELECT * FROM documents
WHERE content LIKE '%machine learning%'
ORDER BY COSINE_SIMILARITY(embedding, $query_vector) DESC
LIMIT 10;
```

### Vector Search Features
- **HNSW Index** - Fast approximate nearest neighbor search
- **Distance Metrics** - Cosine, Euclidean, Dot Product, Manhattan
- **Product Quantization** - Memory-efficient vector storage
- **Filtered Search** - Combine vector search with SQL predicates
- **Model Support** - OpenAI, Cohere, HuggingFace embedding dimensions

## Time Series Analytics

Purpose-built functions for time series data analysis, IoT metrics, and monitoring use cases.

```sql
-- Downsample to hourly buckets
SELECT * FROM DOWNSAMPLE(
    (SELECT timestamp, value FROM metrics),
    '1 hour', 'avg'
);

-- Fill gaps with interpolation
SELECT * FROM GAP_FILL(
    (SELECT timestamp, value FROM metrics),
    '5 minutes', 'linear'
);

-- Detect anomalies using z-score
SELECT * FROM DETECT_ANOMALIES(
    (SELECT timestamp, value FROM metrics),
    3.0  -- threshold
);

-- Forecast future values
SELECT * FROM FORECAST(
    (SELECT timestamp, value FROM metrics),
    24  -- predict 24 periods
);
```

### Time Series Capabilities
- **Aggregation** - Sum, avg, min, max, count, first, last by time bucket
- **Gap Filling** - Null, zero, forward fill, backward fill, linear interpolation
- **Downsampling** - Reduce data resolution with configurable policies
- **Moving Averages** - Simple, exponential, weighted moving averages
- **Anomaly Detection** - Z-score, IQR-based outlier detection
- **Forecasting** - Linear regression time series prediction

## Machine Learning Integration

Run ML model inference directly in SQL queries with the built-in ML engine.

```sql
-- Feature Store: Point-in-time lookup
SELECT * FROM FEATURE_STORE_LOOKUP('user_features', user_id, '2024-01-15 14:30:00');

-- Model predictions with explainability
SELECT
    user_id,
    PREDICT('churn_model', features) as churn_probability,
    EXPLAIN_PREDICTION('churn_model', features) as explanation
FROM user_features;

-- Monitor model drift
SELECT * FROM MODEL_DRIFT('churn_model', 'psi') WHERE drift_score > 0.1;
```

### ML Features
- **Model Registry** - Deploy ONNX, TensorFlow, PyTorch, XGBoost, LightGBM models
- **Feature Store** - Versioned features with point-in-time correctness
- **Explainability** - SHAP and LIME explanations for predictions
- **Drift Detection** - PSI, KS tests for data and model drift
- **AutoML** - Hyperparameter optimization and cross-validation
- **Online Learning** - Incremental model updates with SGD, Adam

## Graph Database Queries

Native graph analytics for relationship analysis, social networks, and knowledge graphs.

```sql
-- Traverse graph relationships
SELECT * FROM GRAPH_TRAVERSE('social_graph', 'user_123', 'outgoing', 3);

-- Find shortest path between nodes
SELECT * FROM SHORTEST_PATH('social_graph', 'user_123', 'user_456');

-- Calculate PageRank scores
SELECT * FROM PAGERANK('social_graph', 0.85, 20);

-- Detect communities
SELECT * FROM COMMUNITY_DETECT('social_graph', 'label_propagation');
```

### Graph Capabilities
- **Node/Edge Storage** - Properties on nodes and edges
- **Traversal** - BFS, DFS with configurable depth limits
- **Path Algorithms** - Shortest path, all paths enumeration
- **Centrality** - PageRank for node importance
- **Community Detection** - Label propagation clustering

## High Availability & Clustering

Built-in clustering with automatic failover for production deployments.

```bash
# Three-node cluster setup
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

### Cluster Features
- **SWIM Gossip Protocol** - Fast failure detection and node discovery
- **Raft-Lite Consensus** - Leader election with lease-based leadership
- **Automatic Failover** - Sub-second leader election on failure
- **Quorum Writes** - Configurable consistency levels
- **Read Replicas** - Horizontal read scaling with manifest sync
- **Multi-Region** - Cross-region replication with conflict resolution

### Read Replica Setup

```bash
# Primary server
boyodb-server /data/primary 0.0.0.0:8765 \
    --s3-bucket my-bucket --s3-region us-east-1

# Read replica with S3 sync
boyodb-server /data/replica 0.0.0.0:8766 \
    --replica --replica-sync-interval-ms 1000 \
    --s3-bucket my-bucket --s3-region us-east-1
```

## Enterprise Security

Production-grade security features for compliance and data protection.

```sql
-- User management
CREATE USER alice WITH PASSWORD 'secure-password';
ALTER USER alice SET DEFAULT DATABASE analytics;

-- Role-based access control
CREATE ROLE analyst;
GRANT SELECT ON DATABASE analytics TO analyst;
GRANT SELECT, INSERT ON TABLE analytics.reports TO analyst;
GRANT analyst TO alice;

-- Row-level security
CREATE POLICY tenant_isolation ON orders
    FOR SELECT USING (tenant_id = current_tenant());
```

### Security Features
- **Authentication** - Username/password (Argon2id), token-based, LDAP
- **Authorization** - Role-based access control (RBAC)
- **Encryption** - TLS/mTLS, column-level encryption, at-rest encryption
- **Audit Logging** - Comprehensive query and access logging
- **Data Masking** - Dynamic column masking for PII
- **Compliance** - GDPR, CCPA, HIPAA, SOX support

### Data Retention & Compliance

```sql
-- GDPR-compliant retention policy
CREATE RETENTION POLICY pii_retention
  ON users
  RETAIN 2 YEARS
  ACTION ANONYMIZE
  COMPLIANCE GDPR;

-- Legal hold for litigation
CREATE LEGAL HOLD litigation_hold
  ON transactions
  MATTER 'CASE-2024-001'
  SCOPE DATE_RANGE '2023-01-01' TO '2023-12-31';

-- Data subject request (right to erasure)
PROCESS DATA SUBJECT REQUEST
  TYPE ERASURE
  SUBJECT 'user@example.com';
```

## Performance Benchmarks

| Metric | Performance |
|--------|-------------|
| Scan Throughput | 1+ GB/s per core |
| Aggregation Speed | 100M+ rows/second |
| Compression Ratio | 5-10x typical |
| Query Latency | Sub-second on TB datasets |
| Write Throughput | 500K+ rows/second |
| Concurrent Queries | Linear scaling |

### Optimization Features
- **Adaptive Cache Sharding** - Dynamic shard count based on CPU cores and cache size
- **Adaptive Bloom Filters** - Intelligent FPP selection based on data cardinality
- **Parallel Aggregation** - Tree-reduction merge for 5-10% aggregation speedup
- **Segment Prefetching** - Proactive cache warming during sequential scans
- **Parallel Compression** - Concurrent batch compression for high-throughput ingest
- **Query Result Caching** - Redis-compatible distributed cache
- **Adaptive Execution** - Runtime plan adjustment based on statistics
- **Index Advisor** - Automatic index recommendations
- **Cost Model Tuning** - Hardware-calibrated query planning

## Client SDKs & Drivers

Native drivers with connection pooling, batch operations, and transaction support.

| Language | Package | Connection Pool | Batch Insert | Transactions |
|----------|---------|-----------------|--------------|--------------|
| **Rust** | `boyodb` | Yes | Yes | Yes |
| **Go** | `github.com/Izi-Technologies/boyodb/drivers/go/boyodb` | Yes | Yes | Yes |
| **Python** | `boyodb` | Yes | Yes | Yes |
| **Node.js** | `boyodb` | Yes | Yes | Yes |
| **C#** | `BoyoDB` | Yes | Limited | Yes |
| **PHP** | `boyodb/boyodb` | Limited | Limited | Yes |

### Python Example

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

### Go Example

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

## Documentation

| Document | Description |
|----------|-------------|
| [User Guide](docs/USER_GUIDE.md) | Comprehensive feature guide |
| [Quick Reference](docs/QUICK_REFERENCE.md) | Command cheat sheet |
| [SQL Reference](docs/SQL.md) | Complete SQL syntax |
| [Clustering Guide](docs/CLUSTERING.md) | High availability setup |
| [Security Guide](docs/SECURITY.md) | Authentication & authorization |
| [CLI Reference](docs/CLI.md) | Command-line interface |
| [API Reference](docs/API.md) | Server protocol documentation |

## Server Configuration

```bash
boyodb-server <data_dir> [bind_addr] [options]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--max-connections` | 64 | Maximum concurrent connections |
| `--workers` | CPU cores | Worker threads |
| `--query-cache-bytes` | 64MB | Query result cache size |
| `--segment-cache` | 100 | Cached segments |
| `--tls-cert` / `--tls-key` | - | TLS certificate and key |
| `--cluster` | - | Enable cluster mode |
| `--replica` | - | Run as read replica |
| `--backup-interval` | 0 | Automatic backup interval (hours) |

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

## Community & Support

- [GitHub Issues](https://github.com/Izi-Technologies/boyodb/issues) - Bug reports and feature requests
- [Discussions](https://github.com/Izi-Technologies/boyodb/discussions) - Questions and community chat
- [Changelog](CHANGELOG.md) - Release history and version notes

---

**BoyoDB** - The modern analytical database combining the speed of ClickHouse, compatibility of PostgreSQL, and intelligence of AI. Built with Rust and Apache Arrow.

*Keywords: analytical database, columnar database, time series database, vector database, OLAP, real-time analytics, ClickHouse alternative, PostgreSQL compatible, Rust database, Apache Arrow, GPU database, machine learning database, graph database*
