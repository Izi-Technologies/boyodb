# BoyoDB Documentation - High-Performance Analytical Database

Welcome to the official documentation for **BoyoDB**, an open-source columnar database engine built in Rust for real-time analytics, time-series data, and OLAP workloads.

**Current Version: 0.9.8** | [View Changelog](../CHANGELOG.md)

## Documentation Index

### Getting Started
- **[Getting Started Guide](GETTING_STARTED.md)** - Installation, setup, and first queries
- **[Quick Reference](QUICK_REFERENCE.md)** - Cheat sheet for common SQL operations

### Core Documentation
- **[User Guide](USER_GUIDE.md)** - Comprehensive guide to all features
- **[SQL Reference](SQL.md)** - Complete SQL syntax with examples
- **[CLI Reference](CLI.md)** - Command-line interface documentation
- **[Shell Commands](SHELL.md)** - Interactive shell meta-commands

### Operations & Administration
- **[Clustering Guide](CLUSTERING.md)** - High availability and distributed setup
- **[Security Guide](SECURITY.md)** - Authentication, authorization, TLS, RBAC
- **[Storage Tiers](STORAGE_PHASES.md)** - Hot, warm, cold storage configuration

### API & Integration
- **[API Reference](API.md)** - Server protocol and client integration
- **[Roadmap](ROADMAP.md)** - Upcoming features and planned improvements

---

## Feature Overview

### Enterprise Analytics (v0.9.8)
| Feature | Description |
|---------|-------------|
| Query Result Caching | Distributed cache with Redis-compatible protocol |
| Multi-Region DR | Cross-region replication with automatic failover |
| Query Cost API | Pre-flight cost estimates for query planning |
| Tenant Isolation | Namespace encryption, per-tenant backup/restore |
| CDC to Data Lakes | Stream changes to Delta Lake and Apache Iceberg |
| Query Replay | Traffic capture and shadow testing for migrations |
| Auto-Scaling | Metrics-based scaling with predictive policies |
| Data Retention | GDPR/CCPA compliant purging and legal holds |

### ClickHouse-Compatible Features
| Feature | Description |
|---------|-------------|
| Approximate Functions | HyperLogLog, T-Digest, Count-Min Sketch |
| MergeTree Variants | Replacing, Collapsing, Aggregating, Summing |
| External Tables | Query S3, HTTP, HDFS, Delta Lake, Iceberg |
| Async Inserts | Buffered batch ingestion for high throughput |
| Query Profiler | Flame graphs and per-operator timing |
| Parallel Replicas | Distributed query execution |

### PostgreSQL-Compatible Features
| Feature | Description |
|---------|-------------|
| Full SQL | JOINs, CTEs, window functions, subqueries |
| ACID Transactions | Snapshot isolation with savepoints |
| GIN/GiST Indexes | Full-text search, spatial, array containment |
| Exclusion Constraints | Prevent overlapping ranges |
| Connection Pooler | PgBouncer-compatible built-in pooling |

### AI & Machine Learning
| Feature | Description |
|---------|-------------|
| Vector Search | HNSW index with cosine, euclidean, dot product |
| ML Inference | ONNX, TensorFlow, PyTorch model execution |
| Feature Store | Point-in-time feature lookup |
| Explainability | SHAP/LIME prediction explanations |
| AutoML | Hyperparameter optimization |

### Time Series & Analytics
| Feature | Description |
|---------|-------------|
| Time Series Engine | Aggregation, gap filling, downsampling |
| Graph Database | Path queries, PageRank, community detection |
| Data Quality | Validation rules, profiling, anomaly detection |
| Natural Language | Convert questions to SQL queries |

### Performance
| Feature | Description |
|---------|-------------|
| GPU Acceleration | NVIDIA CUDA and Apple Metal support |
| JIT Compilation | Cranelift native code generation |
| Vectorized Execution | SIMD-optimized query processing |
| Group Commit | Batched WAL writes for high throughput |
| Adaptive Cache Sharding | Dynamic shard count based on CPU cores and cache size |
| Adaptive Bloom Filters | Intelligent FPP selection based on data cardinality |
| Parallel Aggregation | Tree-reduction merge for aggregation speedup |
| Segment Prefetching | Proactive cache warming during sequential scans |
| Parallel Compression | Concurrent batch compression for high-throughput ingest |

---

## Quick Start Examples

### Start a Server

```bash
# Single-node server
boyodb-server /var/lib/boyodb 0.0.0.0:8765

# With TLS and authentication
boyodb-server /var/lib/boyodb 0.0.0.0:8765 \
    --tls-cert server.crt --tls-key server.key \
    --auth
```

### Connect with CLI

```bash
# Interactive shell
boyodb-cli shell --host localhost:8765

# Execute single query
boyodb-cli shell -H localhost:8765 -c "SHOW DATABASES"
```

### Create Your First Table

```sql
CREATE DATABASE analytics;

CREATE TABLE analytics.events (
    event_id INT64,
    timestamp TIMESTAMP,
    user_id INT64,
    event_type STRING,
    properties JSON
);

INSERT INTO analytics.events VALUES
    (1, NOW(), 100, 'page_view', '{"page": "/home"}'),
    (2, NOW(), 101, 'click', '{"button": "signup"}');

SELECT event_type, COUNT(*) as cnt
FROM analytics.events
GROUP BY event_type
ORDER BY cnt DESC;
```

### Set Up High Availability Cluster

```bash
# Node 1 (seed node)
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster --cluster-id prod --node-id node1 \
    --gossip-addr 0.0.0.0:8766

# Node 2
boyodb-server /data/node2 0.0.0.0:8767 \
    --cluster --cluster-id prod --node-id node2 \
    --gossip-addr 0.0.0.0:8768 \
    --seed-nodes "node1:8766"

# Node 3
boyodb-server /data/node3 0.0.0.0:8769 \
    --cluster --cluster-id prod --node-id node3 \
    --gossip-addr 0.0.0.0:8770 \
    --seed-nodes "node1:8766"
```

### Start Read Replica

```bash
# Primary server with S3 storage
boyodb-server /data/primary 0.0.0.0:8765 \
    --s3-bucket my-bucket --s3-region us-east-1

# Read replica syncing from S3
boyodb-server /data/replica 0.0.0.0:8766 \
    --replica --replica-sync-interval-ms 1000 \
    --s3-bucket my-bucket --s3-region us-east-1
```

### Vector Similarity Search

```sql
-- Create table with vector embeddings
CREATE TABLE documents (
    id INT64,
    content STRING,
    embedding VECTOR(1536)
);

-- Semantic search using cosine similarity
SELECT id, content,
       COSINE_SIMILARITY(embedding, $query_vector) as score
FROM documents
ORDER BY score DESC
LIMIT 10;
```

---

## Client SDKs

| Language | Package/Repository |
|----------|-------------------|
| **Rust** | `drivers/rust/boyodb` |
| **Go** | `drivers/go/boyodb` |
| **Python** | `drivers/python/boyodb` |
| **Node.js** | `bindings/node` |
| **C#** | `drivers/csharp/BoyoDB` |
| **PHP** | `drivers/php/boyodb` |

---

## Performance Characteristics

| Metric | Performance |
|--------|-------------|
| Scan Throughput | 1+ GB/s per core |
| Aggregation Speed | 100M+ rows/second |
| Compression Ratio | 5-10x typical |
| Query Latency | Sub-second on TB datasets |
| Write Throughput | 500K+ rows/second |

---

## Support & Community

- [GitHub Issues](https://github.com/Izi-Technologies/boyodb/issues) - Bug reports and feature requests
- [Discussions](https://github.com/Izi-Technologies/boyodb/discussions) - Questions and community chat
- [Changelog](../CHANGELOG.md) - Release history and version notes

---

*BoyoDB - Open-source analytical database for real-time analytics, time series, and AI workloads. Built with Rust and Apache Arrow.*
