# BoyoDB

[![Build Status](https://github.com/Izi-Technologies/boyodb/workflows/CI/badge.svg)](https://github.com/Izi-Technologies/boyodb/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.9.6-green.svg)](CHANGELOG.md)

A high-performance columnar database engine built in Rust for real-time analytics, time-series data, and high-throughput OLAP workloads.

## What's New in v0.9.6

### Enterprise Operations
- **Query Result Caching**: Distributed cache with Redis-compatible protocol and event-based invalidation
- **Multi-Region DR**: Cross-region replication with automatic failover, RPO/RTO controls
- **Query Cost API**: Pre-flight cost estimates for query planning and resource prediction
- **Tenant Isolation**: Namespace-level encryption, per-tenant backup/restore, quota enforcement
- **CDC to Data Lakes**: Direct CDC to Delta Lake/Iceberg with schema evolution
- **Query Replay**: Traffic capture and replay to test clusters with result comparison
- **Auto-Scaling**: Metrics-based scaling triggers with predictive scaling
- **Data Retention**: GDPR/CCPA compliant purging, legal holds, data subject requests

### ClickHouse Parity
- **Approximate Functions**: HyperLogLog, T-Digest, Count-Min Sketch for fast approximate analytics
- **MergeTree Variants**: Replacing, Collapsing, Aggregating, Summing for specialized workloads
- **External Tables**: Query S3, HTTP URLs, HDFS, Delta Lake, Iceberg without importing
- **Async Inserts**: Buffered batch ingestion for high-throughput writes
- **Query Profiler**: Flame graphs, per-operator timing, memory and I/O tracking
- **Parallel Replicas**: Distribute query execution across multiple replicas
- **Zero-Copy Replication**: Share segments via object storage without copying

### PostgreSQL Parity
- **Exclusion Constraints**: Prevent overlapping ranges (scheduling, bookings)
- **GIN/GiST Indexes**: Full-text search, spatial indexing, array containment
- **CDC (Debezium-compatible)**: Change data capture with before/after images
- **WebAssembly UDFs**: Sandboxed user-defined functions in WASM

### AI-Powered Optimization
- **AI Query Optimizer**: ML-based cardinality estimation and plan selection
- **Tiered JIT Compilation**: Interpreted → Baseline → Optimized → Vectorized

See the [CHANGELOG](CHANGELOG.md) for full release history.

## Why BoyoDB?

- **Blazing Fast**: Vectorized query execution with SIMD and GPU acceleration
- **Real-Time Analytics**: Sub-second queries on billions of rows
- **Horizontally Scalable**: Built-in clustering with automatic failover
- **Cloud-Native**: S3/object storage integration, tiered storage, data lake formats
- **Multi-Region**: Cross-region replication with conflict resolution
- **Self-Tuning**: Automatic index recommendations and adaptive query execution
- **Multi-Tenant**: Per-tenant resource quotas and fair scheduling
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
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, `MEDIAN`, `PERCENTILE_CONT`, `PERCENTILE_DISC`, `ARRAY_AGG`, `STRING_AGG`, `MODE`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`
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
CREATE INDEX idx_user_id ON events USING HASH (user_id);

-- Fulltext index for substring searches (LIKE '%pattern%')
CREATE INDEX idx_phone ON calls USING FULLTEXT (phone_number);

-- Unique constraint
CREATE UNIQUE INDEX idx_email ON users (email);

-- Composite index
CREATE INDEX idx_user_time ON events (user_id, timestamp);
```

Index types: `BTREE`, `HASH`, `BLOOM`, `BITMAP`, `FULLTEXT`

**Fulltext Index:**
- Uses n-gram tokenization for efficient `LIKE '%substring%'` queries
- Enables segment pruning - segments without matching n-grams are skipped entirely
- Ideal for phone number searches, partial string matching, and substring lookups
- Case-insensitive by default

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

**Automatic Scheduled Backups:**

```bash
# Enable automatic backups every 6 hours, keep last 10
boyodb-server /data 0.0.0.0:8765 \
    --backup-interval 6 \
    --backup-max-count 10
```

Automatic backups are labeled `auto-YYYYMMDD-HHMMSS` and old backups are automatically pruned when the limit is reached.

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
# CLI import with batching for high performance
boyodb-cli import --host localhost:8765 \
    --table mydb.events \
    --input data.csv \
    --format csv \
    --batch-size 5000 \
    --progress

# CLI export
boyodb-cli export --host localhost:8765 \
    --query "SELECT * FROM events" \
    --output results.parquet \
    --format parquet
```

Supported formats: CSV, JSON, Parquet, Arrow IPC

Import options:
- `--batch-size <rows>` - Rows per batch (default: 1000)
- `--progress` - Show import progress

### AI/Vector Search

Built-in vector similarity search for AI and machine learning workloads:

```sql
-- Create table with vector column
CREATE TABLE embeddings (
    id INT64,
    content STRING,
    embedding VECTOR(1536)  -- OpenAI ada-002 dimensions
);

-- Vector similarity search using HNSW index
SELECT id, content,
       COSINE_SIMILARITY(embedding, $query_vector) as score
FROM embeddings
ORDER BY score DESC
LIMIT 10;

-- Hybrid search (vector + text)
SELECT * FROM embeddings
WHERE content LIKE '%machine learning%'
ORDER BY COSINE_SIMILARITY(embedding, $query_vector) DESC
LIMIT 10;

-- Use SQL extension functions
SELECT * FROM VECTOR_SEARCH('embeddings_idx', [0.1, 0.2, ...], 10);
```

**Vector Features:**
- HNSW index for approximate nearest neighbor search
- Distance metrics: Cosine, Euclidean, Dot Product, Manhattan
- Product quantization for memory-efficient storage
- Filtered vector search with metadata predicates
- Pre-configured dimensions for OpenAI, Cohere, HuggingFace models
- Hybrid search with RRF and linear fusion

### Machine Learning

Integrated ML capabilities for feature engineering, model deployment, and monitoring:

```sql
-- Feature Store: Point-in-time feature lookup
SELECT * FROM FEATURE_STORE_LOOKUP('user_features', user_id, '2024-01-15 14:30:00');

-- ML Predictions with explainability
SELECT
    user_id,
    PREDICT('churn_model', features) as churn_prob,
    EXPLAIN_PREDICTION('churn_model', features) as explanation
FROM user_features;

-- Model drift monitoring
SELECT * FROM MODEL_DRIFT('churn_model', 'psi') WHERE drift_score > 0.1;
```

**ML Features:**
- **Feature Store**: Versioned features, transformations, online serving
- **Model Registry**: Deploy and version ML models
- **Predictions**: Run inference in SQL queries
- **Explainability**: SHAP/LIME explanations for predictions
- **Drift Detection**: PSI, KS tests for data/model drift
- **Online Learning**: Incremental model updates
- **AutoML**: Hyperparameter optimization, cross-validation

### Graph Queries

Native graph database capabilities for relationship analysis:

```sql
-- Traverse graph from a node
SELECT * FROM GRAPH_TRAVERSE('social_graph', 'user_123', 'outgoing', 3);

-- Find shortest path
SELECT * FROM SHORTEST_PATH('social_graph', 'user_123', 'user_456');

-- PageRank for influence scoring
SELECT * FROM PAGERANK('social_graph', 0.85, 20);

-- Community detection
SELECT * FROM COMMUNITY_DETECT('social_graph', 'label_propagation');
```

**Graph Features:**
- Node and edge storage with properties
- BFS/DFS traversal with depth limits
- Dijkstra's shortest path algorithm
- PageRank for node importance
- Label propagation for community detection
- All paths enumeration

### Time Series Analytics

Specialized functions for time series data:

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

-- Anomaly detection
SELECT * FROM DETECT_ANOMALIES(
    (SELECT timestamp, value FROM metrics),
    3.0  -- z-score threshold
);

-- Forecasting
SELECT * FROM FORECAST(
    (SELECT timestamp, value FROM metrics),
    24  -- predict next 24 periods
);
```

**Time Series Features:**
- Aggregation: sum, avg, min, max, count, first, last
- Gap filling: null, zero, forward fill, backward fill, linear interpolation
- Downsampling with multiple aggregation methods
- Moving averages and exponential smoothing
- Seasonal decomposition
- Anomaly detection (z-score, IQR)
- Linear regression forecasting

### Data Quality

Built-in data quality validation and profiling:

```sql
-- Profile a table
SELECT * FROM PROFILE('analytics.events');

-- Validate against rules
SELECT * FROM VALIDATE('analytics.events', 'not_null:user_id,range:amount:0:10000');

-- Get quality score
SELECT QUALITY_SCORE('analytics.events');
```

**Data Quality Features:**
- Column profiling (nulls, distinct, min, max, mean)
- Validation rules: not null, unique, range, regex, email, URL
- Anomaly detection for outliers
- Quality scoring with recommendations
- Data type inference

### Natural Language to SQL

Query your data using natural language:

```sql
-- Convert question to SQL
SELECT * FROM NL_QUERY('Show me top 10 customers by revenue last month');

-- With schema context
SELECT * FROM NL_QUERY('How many orders per day?', 'analytics.orders');
```

**NL Features:**
- Intent recognition (select, aggregate, filter, sort)
- Entity extraction for tables and columns
- Schema-aware query generation
- Support for aggregations, filters, ordering

### Query Federation

Query across multiple data sources:

```sql
-- Register external sources
CREATE FOREIGN DATA WRAPPER postgres_fdw;
CREATE SERVER pg_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'pg.example.com', dbname 'prod');

-- Federated query
SELECT * FROM FEDERATED_QUERY(
    ['local.events', 'pg_server.orders'],
    'SELECT e.*, o.total FROM events e JOIN orders o ON e.order_id = o.id'
);
```

**Federation Features:**
- PostgreSQL, MySQL, S3, HTTP API connectors
- Push-down optimization (filters, projections, aggregations)
- Connection pooling
- Result merging and caching

### Read Replicas

Scale read workloads with read replicas:

```bash
# Start primary server
boyodb-server /data/primary 0.0.0.0:8765 \
    --s3-bucket my-bucket --s3-region us-east-1

# Start read replica
boyodb-server /data/replica 0.0.0.0:8766 \
    --replica \
    --replica-sync-interval-ms 1000 \
    --s3-bucket my-bucket --s3-region us-east-1

# Or sync from primary via HTTP
boyodb-server /data/replica 0.0.0.0:8766 \
    --replica \
    --replica-primary-addr http://primary:8765
```

**Replica Features:**
- Read-only mode (all writes rejected)
- Manifest sync from shared S3
- HTTP bundle pull from primary
- Sub-second sync lag
- Automatic failover support

### Data Catalog

Metadata management and data discovery:

```sql
-- Search catalog
SELECT * FROM SEARCH_CATALOG('customer');

-- View data lineage
SELECT * FROM DATA_LINEAGE('analytics.daily_stats', 'upstream');

-- Business glossary
SELECT * FROM system.glossary WHERE term LIKE '%revenue%';
```

**Catalog Features:**
- Automatic metadata collection
- Data lineage tracking (upstream/downstream)
- Business glossary with definitions
- Classification and tagging
- Full-text search

### Blockchain Audit Log

Immutable audit logging with cryptographic verification:

```sql
-- View audit trail
SELECT * FROM AUDIT_LOG('analytics.transactions');

-- Verify chain integrity
SELECT VERIFY_CHAIN('audit_ledger');
```

**Audit Features:**
- Append-only blockchain ledger
- SHA-256 hash chains
- Merkle tree verification
- Transaction signing
- Tamper detection

### Storage Engine

- **Columnar Storage**: Apache Arrow-based format for cache-efficient analytics
- **Compression**: Zstd, LZ4, Snappy with dictionary encoding
- **Write-Ahead Log**: Durability with configurable fsync policies
- **Tiered Storage**: Hot (SSD) → Warm (HDD) → Cold (S3)
- **Bloom Filters**: Fast segment pruning
- **Deduplication**: Configurable key-based deduplication
- **Sharded Caches**: 64-shard segment cache, 32-shard batch cache for parallel access

### Data Integrity & Corruption Prevention

BoyoDB implements multiple layers of protection against data corruption:

- **Atomic File Operations**: All critical writes use temp-file + rename pattern
- **Write Verification**: Read-back verification with checksum validation
- **Manifest Protection**: Atomic manifest updates prevent corruption on crash
- **WAL Integrity**: Atomic LSN persistence with directory-safe temp files
- **S3 Upload Verification**: Cold tier uploads verified with checksum comparison
- **Segment Checksums**: xxHash64 checksums for all segment data
- **IPC Format Validation**: Deep validation of Arrow IPC format during loads
- **Schema Hash Validation**: Schema verification during segment loads

```sql
-- Check segment integrity
CHECK TABLE mydb.mytable;

-- Deep scrub with IPC validation
SHOW DAMAGED SEGMENTS FROM mydb.mytable;

-- Repair corrupted segments
REPAIR TABLE mydb.mytable;
```

### Resource Governance

- Memory pools with hierarchical limits
- I/O scheduling with priority
- Concurrent query throttling
- Connection pooling with idle timeout
- Query admission control

### Query Optimizer

BoyoDB includes an advanced query optimizer with automatic tuning capabilities:

#### Index Advisor

Automatically analyzes query patterns and recommends optimal indexes:

```sql
-- View index recommendations
SHOW INDEX RECOMMENDATIONS;

-- Recommendations include:
-- - Columns frequently used in WHERE clauses (equality, range, IN)
-- - Join columns
-- - ORDER BY and GROUP BY columns
-- - Estimated impact score
```

The Index Advisor tracks access patterns and provides recommendations with estimated benefit scores based on query frequency and selectivity.

#### Query Store

Historical query analysis for performance tracking and regression detection:

```sql
-- Enable query store
SET query_store = ON;

-- View top queries by execution time
SELECT * FROM system.query_store
ORDER BY total_time_ms DESC
LIMIT 10;

-- Find queries with plan regressions
SELECT * FROM system.query_store
WHERE plan_changed = true
  AND latest_time_ms > baseline_time_ms * 2;
```

Query Store captures:
- Query fingerprints (normalized SQL)
- Execution statistics (time, rows, I/O)
- Plan snapshots for regression analysis
- Historical trends

#### Adaptive Query Execution

Runtime query plan adjustment based on actual data statistics:

```sql
-- Enable adaptive execution
SET adaptive_execution = ON;

-- The optimizer will automatically:
-- - Switch join algorithms based on actual row counts
-- - Adjust parallelism based on data distribution
-- - Resize memory budgets based on observed usage
```

Adaptive checkpoints monitor execution and trigger plan changes when:
- Actual cardinality differs significantly from estimates
- Memory pressure requires spilling strategy changes
- Data skew suggests different partitioning

#### Cost Model Tuning

Calibrate the optimizer's cost model for your hardware:

```sql
-- Run calibration workload
ANALYZE COST MODEL;

-- View current cost parameters
SHOW COST MODEL;

-- Adjust specific parameters
SET cpu_tuple_cost = 0.01;
SET random_page_cost = 4.0;
SET effective_cache_size = '4GB';
```

### Per-Tenant Resource Quotas

Fair resource allocation for multi-tenant deployments:

```sql
-- Create tenant quota
CREATE QUOTA tenant_acme
  MAX_CPU_TIME_MS = 30000
  MAX_MEMORY_MB = 4096
  MAX_CONCURRENT_QUERIES = 10
  MAX_QPM = 1000;

-- Assign quota to user
ALTER USER alice SET QUOTA tenant_acme;

-- View tenant usage
SELECT * FROM system.tenant_usage;
```

Quota enforcement includes:
- CPU time limits per query and per minute
- Memory allocation limits
- Concurrent query limits
- Queries per minute (QPM) rate limiting
- Fair scheduling across tenants

### Enterprise Operations

#### Query Result Caching

Distributed cache layer for query results with Redis-compatible protocol:

```sql
-- Configure cache TTL for a session
SET query_cache_ttl = 300;  -- 5 minutes

-- Query results are automatically cached
SELECT * FROM analytics.daily_stats WHERE date > '2024-01-01';

-- Invalidate cache for a table after writes
INVALIDATE CACHE FOR analytics.daily_stats;
```

**Cache Features:**
- Sharded LRU cache with configurable size limits
- Time-based TTL and event-based invalidation
- Redis RESP protocol server for external access
- Per-tenant isolation
- Table dependency tracking

#### Auto-Scaling Policies

Metrics-based scaling triggers with predictive scaling:

```sql
-- Create scaling policy
CREATE SCALING POLICY scale_out_cpu
  METRIC cpu_utilization
  THRESHOLD 70
  ACTION ADD_NODES 1
  COOLDOWN 300;

-- View scaling status
SHOW SCALING STATUS;

-- Manual scale
SCALE TO 5 NODES;
```

**Scaling Features:**
- Policy types: threshold, step, target tracking, predictive
- Metrics: CPU, memory, connections, QPS, replication lag
- Predictive scaling with hourly/daily seasonality
- Cooldown periods between scale actions

#### Data Retention Policies

GDPR/CCPA compliant data lifecycle management:

```sql
-- Create retention policy
CREATE RETENTION POLICY pii_retention
  ON users
  RETAIN 2 YEARS
  ACTION ANONYMIZE
  COMPLIANCE GDPR;

-- Create legal hold
CREATE LEGAL HOLD litigation_hold
  ON transactions
  MATTER 'CASE-2024-001'
  SCOPE DATE_RANGE '2023-01-01' TO '2023-12-31';

-- Process data subject request
PROCESS DATA SUBJECT REQUEST
  TYPE ERASURE
  SUBJECT 'user@example.com';
```

**Retention Features:**
- Purge actions: delete, soft-delete, anonymize, archive
- Legal holds with scope control
- Data subject requests (access, erasure, portability)
- Compliance reporting (GDPR, CCPA, HIPAA)

#### Query Replay/Shadowing

Capture and replay production traffic for testing:

```sql
-- Start query capture
START QUERY CAPTURE
  SAMPLE_RATE 0.1
  MIN_EXEC_TIME_MS 100;

-- Replay to test cluster
REPLAY QUERIES TO 'test-cluster:8765'
  COMPARE_RESULTS true;

-- View replay report
SHOW REPLAY REPORT;
```

**Replay Features:**
- Configurable sampling rate
- Result comparison with diff reporting
- Shadow traffic to test clusters
- Performance regression detection

### Multi-Region Replication

Cross-region data synchronization for global deployments:

```bash
# Primary region (US-East)
boyodb-server /data 0.0.0.0:8765 \
    --region us-east \
    --region-mode primary \
    --replicate-to us-west,eu-west

# Secondary region (US-West)
boyodb-server /data 0.0.0.0:8765 \
    --region us-west \
    --region-mode secondary \
    --primary-region us-east
```

```sql
-- Check replication status
SHOW REGION STATUS;

-- Configure conflict resolution
SET conflict_resolution = 'last_writer_wins';  -- or 'region_priority'

-- View replication lag
SELECT * FROM system.region_replication;
```

**Conflict Resolution Strategies:**
- `last_writer_wins` - Latest timestamp wins
- `first_writer_wins` - First write preserved
- `region_priority` - Priority-based (primary wins)
- `custom` - User-defined resolution function

### GPU Acceleration

Hardware-accelerated analytics for compute-intensive queries:

```sql
-- Check GPU availability
SHOW GPU STATUS;

-- Enable GPU acceleration
SET gpu_acceleration = ON;

-- GPU-accelerated operations:
-- - Aggregations (SUM, COUNT, AVG, MIN, MAX)
-- - Filters with complex predicates
-- - Hash joins
-- - Vector similarity search
```

**Supported Platforms:**
- NVIDIA CUDA (Linux, Windows)
- Apple Metal (macOS)
- Automatic CPU fallback when GPU unavailable

**GPU Memory Management:**
- Automatic memory pooling
- Configurable memory limits
- Spill-to-host for large datasets

```bash
# Configure GPU settings
boyodb-server /data 0.0.0.0:8765 \
    --gpu-memory-limit 8GB \
    --gpu-min-batch-size 10000
```

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
- Replication lag (local and cross-region)
- Resource usage (memory, I/O, GPU)
- Per-tenant quota usage
- Query store statistics
- Index advisor recommendations
- Adaptive execution decisions

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
│  │   (SQL)     │  │  (Adaptive) │  │ (Vectorized)│         │
│  └─────────────┘  └──────┬──────┘  └──────┬──────┘         │
│                          │                │                 │
│                   ┌──────▼──────┐  ┌──────▼──────┐         │
│                   │ Query Store │  │     GPU     │         │
│                   │Index Advisor│  │  Executor   │         │
│                   └─────────────┘  └─────────────┘         │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │    Auth     │  │   Cluster   │  │  Resource   │         │
│  │  Manager    │  │   Manager   │  │  Governor   │         │
│  └─────────────┘  └─────────────┘  └──────┬──────┘         │
│                          │                │                 │
│                   ┌──────▼──────┐  ┌──────▼──────┐         │
│                   │Multi-Region │  │   Tenant    │         │
│                   │ Replication │  │   Quotas    │         │
│                   └─────────────┘  └─────────────┘         │
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
| `--workers` | CPU cores | Worker threads (auto-detected) |
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
| `--compact-on-start` | - | Compact segments on startup if threshold exceeded |
| `--compact-on-start-threshold` | 10000 | Segment count threshold for startup compaction |
| `--backup-interval` | 0 | Automatic backup interval in hours (0=disabled) |
| `--backup-max-count` | 0 | Max automatic backups to retain (0=unlimited) |
| `--backup-dir` | data_dir/backups | Directory for backup storage |

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
| Concurrent queries | Linear scaling with sharded caches |

### Concurrency Optimizations

BoyoDB is engineered for high-concurrency workloads:

- **Sharded Segment Cache**: 64 independent shards eliminate lock contention
- **Sharded Batch Cache**: 32 shards for parallel IPC decoding
- **Targeted Lock Wakeups**: Per-lock waiter tracking prevents thundering herd
- **MVCC Row Write Index**: O(R+W) conflict detection for fast transaction validation
- **Adaptive Wait Timeouts**: Exponential backoff reduces CPU spinning
- **Manifest Early Release**: Lock released before segment I/O
- **Parallel S3 I/O**: Concurrent cold segment loading

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
