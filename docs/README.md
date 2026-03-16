# BoyoDB Documentation

Welcome to the BoyoDB documentation. This directory contains comprehensive guides for installing, configuring, and using BoyoDB.

**Current Version: 0.9.6**

## Documentation Index

### Getting Started
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Quick introduction and first steps
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Cheat sheet for common operations

### Core Documentation
- **[USER_GUIDE.md](USER_GUIDE.md)** - Comprehensive user guide covering all features
- **[SQL.md](SQL.md)** - Complete SQL syntax reference
- **[CLI.md](CLI.md)** - Command-line interface reference
- **[SHELL.md](SHELL.md)** - Interactive shell commands

### Operations
- **[CLUSTERING.md](CLUSTERING.md)** - High availability and clustering guide
- **[SECURITY.md](SECURITY.md)** - Authentication, authorization, and TLS
- **[STORAGE_PHASES.md](STORAGE_PHASES.md)** - Storage tiers and data lifecycle

### Development
- **[API.md](API.md)** - Protocol and API documentation
- **[ROADMAP.md](ROADMAP.md)** - Feature roadmap and planned improvements

## Feature Highlights

### Enterprise Operations (v0.9.6)
- **Query Result Caching** - Distributed cache with Redis-compatible protocol
- **Multi-Region DR** - Cross-region replication with automatic failover
- **Query Cost API** - Pre-flight cost estimates for query planning
- **Tenant Isolation** - Namespace encryption, per-tenant backup/restore
- **CDC to Data Lakes** - Direct CDC to Delta Lake/Iceberg
- **Query Replay** - Traffic capture and replay for testing
- **Auto-Scaling** - Metrics-based scaling with predictive policies
- **Data Retention** - GDPR/CCPA compliant purging and legal holds

### Analytics & ML
- **Vector Search** - HNSW approximate nearest neighbor with multiple distance metrics
- **Machine Learning** - Feature store, model registry, SHAP/LIME explainability
- **Time Series Engine** - Aggregation, gap filling, downsampling, forecasting
- **Graph Database** - Node/edge storage, path queries, PageRank, community detection
- **Data Quality** - Validation rules, profiling, anomaly detection

### ClickHouse Parity
- **Approximate Functions** - HyperLogLog, T-Digest, Count-Min Sketch
- **MergeTree Variants** - Replacing, Collapsing, Aggregating, Summing
- **External Tables** - Query S3, HTTP, HDFS, Delta Lake, Iceberg
- **Async Inserts** - Buffered batch ingestion
- **Query Profiler** - Flame graphs, per-operator timing
- **Parallel Replicas** - Distributed query execution

### PostgreSQL Parity
- **Exclusion Constraints** - Prevent overlapping ranges
- **GIN/GiST Indexes** - Full-text search, spatial indexing
- **CDC (Debezium-compatible)** - Change data capture
- **WebAssembly UDFs** - Sandboxed user-defined functions
- **Connection Pooler** - PgBouncer-compatible pooling

### Performance
- **GPU Acceleration** - CUDA (Linux/Windows) and Metal (macOS)
- **JIT Compilation** - Cranelift-based native code generation
- **Vectorized Execution** - SIMD-optimized query processing
- **Group Commit** - Batched WAL writes for high throughput

## Quick Links

### Start a Server
```bash
boyodb-server /var/lib/boyodb 0.0.0.0:8765
```

### Connect with CLI
```bash
boyodb-cli shell --host localhost:8765
```

### Create Your First Table
```sql
CREATE DATABASE mydb;
CREATE TABLE mydb.events (id INT64, name STRING, ts TIMESTAMP);
INSERT INTO mydb.events VALUES (1, 'test', NOW());
SELECT * FROM mydb.events;
```

### Set Up a Cluster
```bash
# Node 1
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster --cluster-id prod --gossip-addr 0.0.0.0:8766

# Node 2
boyodb-server /data/node2 0.0.0.0:8767 \
    --cluster --cluster-id prod --gossip-addr 0.0.0.0:8768 \
    --seed-nodes "node1:8766"
```

### Start a Read Replica
```bash
# Primary
boyodb-server /data/primary 0.0.0.0:8765 \
    --s3-bucket my-bucket --s3-region us-east-1

# Replica
boyodb-server /data/replica 0.0.0.0:8766 \
    --replica --replica-sync-interval-ms 1000 \
    --s3-bucket my-bucket --s3-region us-east-1
```

## Client Drivers

| Driver | Repository |
|--------|------------|
| Rust | `drivers/rust/boyodb` |
| Go | `drivers/go/boyodb` |
| Python | `drivers/python/boyodb` |
| Node.js | `bindings/node` |
| C# | `drivers/csharp/BoyoDB` |
| PHP | `drivers/php/boyodb` |

## Support

- [GitHub Issues](https://github.com/Izi-Technologies/boyodb/issues)
- [Discussions](https://github.com/Izi-Technologies/boyodb/discussions)
- [Changelog](../CHANGELOG.md) - Release history and version changes
